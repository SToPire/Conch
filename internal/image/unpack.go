package image

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	containerdclient "github.com/openeuler/Conch/internal/adapters/containerd/client"
	"github.com/openeuler/Conch/internal/image/erofsconvert"
	"github.com/openeuler/Conch/pkg/ulog"
)

const (
	KindRootfs      = "rootfs"
	KindSandbox     = "sandbox"
	KindMemSnapshot = "mem-snapshot"
	KindUnknown     = "unknown"
)

var ErrMissingSandbox = errors.New("missing required sandbox component")

// UnpackBootIndex validates a Boot Index by its immutable digest and unpacks
// all component manifests.
//
// The Boot Index must be fully available locally before calling: all content
// (manifests, configs, and layers) must exist in the content store.
func UnpackBootIndex(ctx context.Context, client *containerdclient.Client, bootIndexDigest string) error {
	_, _, err := unpackBootIndexByDigest(ctx, client, bootIndexDigest)
	return err
}

func unpackBootIndexByDigest(
	ctx context.Context,
	client *containerdclient.Client,
	bootIndexDigest string,
) (info BootIndexInfo, snapshotMap map[string]string, retErr error) {
	unpackCtx, release, err := beginBootIndexOperation(ctx, client, bootIndexDigest)
	if err != nil {
		return BootIndexInfo{}, nil, err
	}
	defer func() {
		if err := release(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("release boot index operation lease: %w", err))
		}
	}()

	_, info, err = inspectBootIndexByDigest(unpackCtx, client.ContentStore(), bootIndexDigest)
	if err != nil {
		return BootIndexInfo{}, nil, err
	}
	snapshotMap, err = unpackBootIndexComponents(unpackCtx, client.Client, info)
	if err != nil {
		return BootIndexInfo{}, nil, fmt.Errorf("unpack boot index %s: %w", info.BootIndexDigest, err)
	}
	return info, snapshotMap, nil
}

// beginBootIndexOperation deliberately overrides any caller lease. Component
// snapshots are build products of this operation, not permanent members of the
// process-wide runtime lease. Once unpack has linked each snapshot from its OCI
// config, Template/image ownership keeps it reachable through the content GC
// graph and this temporary lease can be released.
func beginBootIndexOperation(
	ctx context.Context,
	client *containerdclient.Client,
	bootIndexDigest string,
) (context.Context, func() error, error) {
	if client == nil || client.Client == nil {
		return nil, nil, fmt.Errorf("containerd client is required")
	}
	dgst, err := digest.Parse(strings.TrimSpace(bootIndexDigest))
	if err != nil {
		return nil, nil, fmt.Errorf("invalid boot index digest %q: %w", bootIndexDigest, err)
	}

	namespaceCtx := containerdclient.NewNamespaceContext(ctx)
	manager := client.LeasesService()
	lease, err := manager.Create(
		namespaceCtx,
		leases.WithRandomID(),
		leases.WithExpiration(24*time.Hour),
		leases.WithLabel("io.conch.lease.kind", "operation"),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create boot index operation lease: %w", err)
	}
	release := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(namespaceCtx), 10*time.Second)
		defer cancel()
		err := manager.Delete(cleanupCtx, lease)
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := manager.AddResource(namespaceCtx, lease, leases.Resource{
		Type: "content",
		ID:   dgst.String(),
	}); err != nil && !errdefs.IsAlreadyExists(err) {
		return nil, nil, errors.Join(
			fmt.Errorf("retain boot index %s for unpack: %w", dgst, err),
			release(),
		)
	}
	return leases.WithLease(namespaceCtx, lease.ID), release, nil
}

func unpackBootIndexComponents(ctx context.Context, client *containerd.Client, info BootIndexInfo) (map[string]string, error) {
	if client == nil {
		return nil, fmt.Errorf("containerd client is required")
	}

	snapshotMap := make(map[string]string)

	components := []ocispec.Descriptor{info.RootfsDescriptor}
	if info.MemDescriptor.Digest != "" {
		components = append(components, info.MemDescriptor)
	}
	components = append(components, info.SandboxDescriptor)
	ulog.Info("Found components in Boot Index, starting unpack",
		ulog.F("count", len(components)))

	for _, manifestDesc := range components {
		kind := getKind(manifestDesc)
		unpackName := componentUnpackName(kind, manifestDesc)
		if err := validateNativeComponentManifest(ctx, client, kind, manifestDesc); err != nil {
			return nil, err
		}
		snapshotID, err := unpackOneSubImage(ctx, client, "erofs", manifestDesc, kind, unpackName)
		if err != nil {
			return nil, err
		}
		snapshotMap[kind] = snapshotID
		ulog.Info("Generated SnapshotID",
			ulog.F("kind", kind),
			ulog.F("snapshot_id", snapshotID))
	}

	return snapshotMap, nil
}

func getKind(manifestDesc ocispec.Descriptor) string {
	if kind := manifestDesc.Annotations["io.conch.kind"]; kind != "" {
		return kind
	}
	return KindUnknown
}

func componentUnpackName(kind string, manifestDesc ocispec.Descriptor) string {
	return fmt.Sprintf("localhost/conch/%s-component:%s", kind, manifestDesc.Digest.Encoded())
}

func unpackOneSubImage(ctx context.Context, client *containerd.Client, snapshotterName string, manifestDesc ocispec.Descriptor, kind string, imageName string) (string, error) {
	subImg := containerd.NewImage(client, images.Image{
		Name:   imageName,
		Target: manifestDesc,
	})

	diffIDs, err := subImg.RootFS(ctx)
	if err != nil {
		return "", fmt.Errorf("get RootFS for %s: %w", kind, err)
	}
	if err := subImg.Unpack(ctx, snapshotterName); err != nil {
		return "", fmt.Errorf("unpack sub-image %s (kind: %s): %w", manifestDesc.Digest, kind, err)
	}
	return identity.ChainID(diffIDs).String(), nil
}

func isNativeErofsKind(kind string) bool {
	return kind == KindRootfs || kind == KindMemSnapshot || kind == KindSandbox
}

func validateNativeComponentManifest(ctx context.Context, client *containerd.Client, kind string, manifestDesc ocispec.Descriptor) error {
	manifest, err := images.Manifest(ctx, client.ContentStore(), manifestDesc, platforms.DefaultStrict())
	if err != nil {
		return fmt.Errorf("resolve native %s manifest: %w", kind, err)
	}
	if kind == KindRootfs {
		if _, err := erofsconvert.ValidateNativeLayers(manifest.Layers, erofsconvert.DefaultAlignBytes); err != nil {
			return fmt.Errorf("%s component is not native erofs: %w", kind, err)
		}
		return nil
	}
	if len(manifest.Layers) == 0 {
		return fmt.Errorf("%s component is not native erofs: manifest has no layers", kind)
	}
	for _, layer := range manifest.Layers {
		if layer.MediaType != erofsconvert.NativeLayerMediaType {
			return fmt.Errorf("%s component is not native erofs: layer %s media type %s is not %s", kind, layer.Digest, layer.MediaType, erofsconvert.NativeLayerMediaType)
		}
		if layer.Size <= 0 {
			return fmt.Errorf("%s component is not native erofs: layer %s size %d is invalid", kind, layer.Digest, layer.Size)
		}
	}
	return nil
}
