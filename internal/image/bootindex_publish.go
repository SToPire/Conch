package image

import (
	"context"
	"fmt"
	"strings"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	containerdclient "github.com/openeuler/Conch/internal/adapters/containerd/client"
)

func PublishBootIndex(ctx context.Context, client *containerdclient.Client, req PublishBootIndexOptions) (PublishBootIndexResult, error) {
	if client == nil || client.Client == nil {
		return PublishBootIndexResult{}, fmt.Errorf("containerd client is required")
	}
	if req.RootfsImageName == "" {
		return PublishBootIndexResult{}, fmt.Errorf("%w: rootfs_image_name is required", ErrInvalidRequest)
	}
	if req.KernelPath == "" {
		return PublishBootIndexResult{}, fmt.Errorf("%w: kernel_path is required", ErrInvalidRequest)
	}
	if req.InitrdPath == "" {
		return PublishBootIndexResult{}, fmt.Errorf("%w: initrd_path is required", ErrInvalidRequest)
	}
	namespaceCtx := containerdclient.NewNamespaceContext(ctx)
	publishCtx, done, err := client.WithLease(namespaceCtx)
	if err != nil {
		return PublishBootIndexResult{}, fmt.Errorf("create content lease: %w", err)
	}
	defer done(publishCtx)

	rootfsImage, err := client.ImageService().Get(publishCtx, req.RootfsImageName)
	if err != nil {
		return PublishBootIndexResult{}, fmt.Errorf("lookup rootfs image %s: %w", req.RootfsImageName, err)
	}
	indexDesc, err := BuildBootIndexInContent(publishCtx, client.ContentStore(), BootIndexContentOptions{
		RootfsDescriptor: rootfsImage.Target,
		KernelPath:       req.KernelPath,
		InitrdPath:       req.InitrdPath,
	})
	if err != nil {
		return PublishBootIndexResult{}, fmt.Errorf("build boot index content: %w", err)
	}

	imageName, err := BootIndexRecordName(indexDesc.Digest.String())
	if err != nil {
		return PublishBootIndexResult{}, err
	}
	if err := publishBootIndexRecord(publishCtx, client, imageName, indexDesc, ImageKindBootIndexCold); err != nil {
		return PublishBootIndexResult{}, err
	}

	return PublishBootIndexResult{
		BootIndexDigest: indexDesc.Digest.String(),
		ImageName:       imageName,
	}, nil
}

// PushBootIndex pushes the exact descriptor closure selected by an immutable
// digest. Unlike a regular image push, it does not resolve through a mutable
// local image name.
func PushBootIndex(ctx context.Context, client *containerdclient.Client, req PushBootIndexOptions) error {
	if client == nil || client.Client == nil {
		return fmt.Errorf("containerd client is required")
	}
	if strings.TrimSpace(req.BootIndexDigest) == "" {
		return fmt.Errorf("%w: boot_index_digest is required", ErrInvalidRequest)
	}
	req.RemoteReference = strings.TrimSpace(req.RemoteReference)
	if req.RemoteReference == "" {
		return fmt.Errorf("%w: remote_reference is required", ErrInvalidRequest)
	}
	pushCtx := containerdclient.NewNamespaceContext(ctx)
	desc, _, err := inspectBootIndexByDigest(pushCtx, client.ContentStore(), req.BootIndexDigest)
	if err != nil {
		return fmt.Errorf("validate boot index %s before push: %w", req.BootIndexDigest, err)
	}
	resolver := docker.NewResolver(docker.ResolverOptions{
		PlainHTTP: req.PlainHTTP,
		Credentials: func(string) (string, string, error) {
			return req.Username, req.Password, nil
		},
	})
	if err := client.Push(pushCtx, req.RemoteReference, desc, containerd.WithResolver(resolver), containerd.WithMaxConcurrentUploadedLayers(1)); err != nil {
		return fmt.Errorf("push boot index %s -> %s: %w", desc.Digest, req.RemoteReference, err)
	}
	return nil
}

// PublishCheckpointBootIndex packages captured memory and VMM state into OCI
// content, reuses the source Boot Index's immutable rootfs and sandbox
// components, and publishes a new Boot Index. It intentionally does not unpack
// the index: checkpoint publication may add content and metadata, but it must
// not create checkpoint snapshots.
//
// The current implementation takes a VMM-specific MemRoot staging directory as
// its mutable checkpoint input. A future, more containerd-native implementation
// should integrate checkpoint publication with containerd's snapshot commit
// mechanism and publish the committed snapshot as the memory component.
func PublishCheckpointBootIndex(
	ctx context.Context,
	client *containerdclient.Client,
	req PublishCheckpointBootIndexOptions,
) (PublishCheckpointBootIndexResult, error) {
	if client == nil || client.Client == nil {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("containerd client is required")
	}
	if strings.TrimSpace(req.SourceBootIndexDigest) == "" {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("%w: source_boot_index_digest is required", ErrInvalidRequest)
	}
	req.MemRoot = strings.TrimSpace(req.MemRoot)
	if req.MemRoot == "" {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("%w: mem_root is required", ErrInvalidRequest)
	}
	req.VMMName = strings.TrimSpace(req.VMMName)
	if req.VMMName == "" {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("%w: vmm_name is required", ErrInvalidRequest)
	}
	if req.MemorySizeMB <= 0 {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("%w: memory_size_mb must be positive", ErrInvalidRequest)
	}

	namespaceCtx := containerdclient.NewNamespaceContext(ctx)
	publishCtx, done, err := client.WithLease(namespaceCtx)
	if err != nil {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("create content lease: %w", err)
	}
	defer done(publishCtx)

	_, sourceInfo, err := inspectBootIndexByDigest(publishCtx, client.ContentStore(), req.SourceBootIndexDigest)
	if err != nil {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("inspect source boot index: %w", err)
	}
	if sourceInfo.VMMName != "" && sourceInfo.VMMName != req.VMMName {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("source boot index VMM %q does not match capture VMM %q", sourceInfo.VMMName, req.VMMName)
	}

	memDesc, err := BuildNativeComponentInContent(publishCtx, client.ContentStore(), []string{req.MemRoot}, KindMemSnapshot)
	if err != nil {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("publish captured mem component: %w", err)
	}
	indexDesc, err := BuildBootIndexInContent(publishCtx, client.ContentStore(), BootIndexContentOptions{
		RootfsDescriptor:  sourceInfo.RootfsDescriptor,
		MemDescriptor:     memDesc,
		SandboxDescriptor: sourceInfo.SandboxDescriptor,
		VMMName:           req.VMMName,
		MemorySizeMB:      req.MemorySizeMB,
	})
	if err != nil {
		return PublishCheckpointBootIndexResult{}, fmt.Errorf("build checkpoint boot index: %w", err)
	}
	imageName, err := BootIndexRecordName(indexDesc.Digest.String())
	if err != nil {
		return PublishCheckpointBootIndexResult{}, err
	}
	if err := publishBootIndexRecord(publishCtx, client, imageName, indexDesc, ImageKindBootIndexResume); err != nil {
		return PublishCheckpointBootIndexResult{}, err
	}
	return PublishCheckpointBootIndexResult{
		BootIndexDigest: indexDesc.Digest.String(),
		ImageName:       imageName,
	}, nil
}

func publishBootIndexRecord(ctx context.Context, client *containerdclient.Client, imageName string, indexDesc ocispec.Descriptor, kind string) error {
	labelHandler := images.SetChildrenLabels(client.ContentStore(), images.ChildrenHandler(client.ContentStore()))
	if err := images.WalkNotEmpty(ctx, labelHandler, indexDesc); err != nil {
		return fmt.Errorf("label boot index content: %w", err)
	}
	imageRecord := images.Image{
		Name:   imageName,
		Target: indexDesc,
		Labels: map[string]string{
			ImageKindLabel: kind,
		},
	}
	existing, err := client.ImageService().Get(ctx, imageName)
	if errdefs.IsNotFound(err) {
		if _, createErr := client.ImageService().Create(ctx, imageRecord); createErr == nil {
			return nil
		} else if !errdefs.IsAlreadyExists(createErr) {
			return fmt.Errorf("create boot image record %s: %w", imageName, createErr)
		}
		// Another publisher may have installed the same digest-derived record
		// between Get and Create. Re-read it and verify immutable identity below.
		existing, err = client.ImageService().Get(ctx, imageName)
	}
	if err != nil {
		return fmt.Errorf("lookup boot image record %s: %w", imageName, err)
	}
	if existing.Target.Digest != indexDesc.Digest {
		return fmt.Errorf("boot image record %s targets %s, want %s", imageName, existing.Target.Digest, indexDesc.Digest)
	}
	if _, err := client.ImageService().Update(ctx, imageRecord, "labels."+ImageKindLabel); err != nil {
		return fmt.Errorf("label boot image record %s: %w", imageName, err)
	}
	return nil
}

// BootIndexRecordName returns the canonical local image name owned by the
// Template whose identity is the supplied immutable Boot Index digest.
func BootIndexRecordName(bootIndexDigest string) (string, error) {
	dgst, err := digest.Parse(strings.TrimSpace(bootIndexDigest))
	if err != nil {
		return "", fmt.Errorf("invalid boot index digest %q: %w", bootIndexDigest, err)
	}
	return "localhost/conch/template:" + dgst.Algorithm().String() + "-" + dgst.Encoded(), nil
}

// RemoveBootIndexRecord deletes one image record only if it still targets the
// expected immutable Boot Index digest. A missing record is already clean.
func RemoveBootIndexRecord(ctx context.Context, client *containerdclient.Client, name, expectedDigest string, synchronous bool) error {
	if client == nil || client.Client == nil {
		return fmt.Errorf("containerd client is required")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("%w: boot index image name is required", ErrInvalidRequest)
	}
	targetDigest, err := digest.Parse(strings.TrimSpace(expectedDigest))
	if err != nil {
		return fmt.Errorf("remove boot index image %s: invalid target digest %q: %w", name, expectedDigest, err)
	}
	cleanupCtx := containerdclient.NewNamespaceContext(ctx)
	opts := []images.DeleteOpt{images.DeleteTarget(&ocispec.Descriptor{Digest: targetDigest})}
	if synchronous {
		opts = append(opts, images.SynchronousDelete())
	}
	err = client.ImageService().Delete(cleanupCtx, name, opts...)
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove boot index image %s: %w", name, err)
	}
	return nil
}
