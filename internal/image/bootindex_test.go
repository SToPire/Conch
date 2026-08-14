package image

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/metadata"
	localcontent "github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"

	containerdclient "github.com/openeuler/Conch/internal/adapters/containerd/client"
)

func TestBuildBootIndexInContentWritesBootIndexBlobs(t *testing.T) {
	requireMkfsErofs(t)

	ctx := context.Background()
	dir := t.TempDir()
	store, err := localcontent.NewStore(filepath.Join(dir, "content"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	rootfs := filepath.Join(dir, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootfs, "bin"), []byte("rootfs"), 0o644); err != nil {
		t.Fatal(err)
	}
	rootfsDesc, err := BuildNativeComponentInContent(ctx, store, []string{rootfs}, KindRootfs)
	if err != nil {
		t.Fatalf("BuildNativeComponentInContent rootfs: %v", err)
	}

	kernel := filepath.Join(dir, "bzImage")
	initrd := filepath.Join(dir, "conch.initrd")
	if err := os.WriteFile(kernel, []byte("kernel"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(initrd, []byte("initrd"), 0o644); err != nil {
		t.Fatal(err)
	}

	indexDesc, err := BuildBootIndexInContent(ctx, store, BootIndexContentOptions{
		RootfsDescriptor: rootfsDesc,
		KernelPath:       kernel,
		InitrdPath:       initrd,
	})
	if err != nil {
		t.Fatalf("BuildBootIndexInContent: %v", err)
	}
	raw, err := content.ReadBlob(ctx, store, indexDesc)
	if err != nil {
		t.Fatalf("read boot index blob: %v", err)
	}
	var index ocispec.Index
	if err := json.Unmarshal(raw, &index); err != nil {
		t.Fatalf("unmarshal boot index: %v", err)
	}
	if len(index.Manifests) != 2 {
		t.Fatalf("manifest count = %d, want 2", len(index.Manifests))
	}
	kinds := map[string]bool{}
	for _, desc := range index.Manifests {
		kinds[desc.Annotations["io.conch.kind"]] = true
		if _, err := content.ReadBlob(ctx, store, desc); err != nil {
			t.Fatalf("manifest blob %s missing: %v", desc.Digest, err)
		}
	}
	if !kinds[KindRootfs] || !kinds[KindSandbox] {
		t.Fatalf("kinds = %#v", kinds)
	}
}

func TestBuildBootIndexInContentUsesPreparedCheckpointComponentsInStableOrder(t *testing.T) {
	requireMkfsErofs(t)

	ctx := context.Background()
	dir := t.TempDir()
	store, err := localcontent.NewStore(filepath.Join(dir, "content"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	build := func(kind, name string) ocispec.Descriptor {
		t.Helper()
		desc, err := BuildNativeComponentInContent(ctx, store, []string{writeComponentRoot(t, dir, name)}, kind)
		if err != nil {
			t.Fatalf("BuildNativeComponentInContent(%s): %v", kind, err)
		}
		return desc
	}
	rootfsDesc := build(KindRootfs, "rootfs-capture")
	memDesc := build(KindMemSnapshot, "mem-capture")
	sandboxDesc := build(KindSandbox, "source-sandbox")

	indexDesc, err := BuildBootIndexInContent(ctx, store, BootIndexContentOptions{
		RootfsDescriptor:  rootfsDesc,
		MemDescriptor:     memDesc,
		SandboxDescriptor: sandboxDesc,
		VMMName:           "cloud-hypervisor",
		MemorySizeMB:      512,
	})
	if err != nil {
		t.Fatalf("BuildBootIndexInContent: %v", err)
	}
	raw, err := content.ReadBlob(ctx, store, indexDesc)
	if err != nil {
		t.Fatalf("read boot index: %v", err)
	}
	var index ocispec.Index
	if err := json.Unmarshal(raw, &index); err != nil {
		t.Fatalf("unmarshal boot index: %v", err)
	}
	wantKinds := []string{KindRootfs, KindMemSnapshot, KindSandbox}
	if len(index.Manifests) != len(wantKinds) {
		t.Fatalf("manifest count = %d, want %d", len(index.Manifests), len(wantKinds))
	}
	for i, want := range wantKinds {
		if got := getKind(index.Manifests[i]); got != want {
			t.Fatalf("manifest %d kind = %q, want %q", i, got, want)
		}
	}
	if got := index.Manifests[0].Digest; got != rootfsDesc.Digest {
		t.Fatalf("rootfs digest = %s, want reused %s", got, rootfsDesc.Digest)
	}
	if got := index.Manifests[2].Digest; got != sandboxDesc.Digest {
		t.Fatalf("sandbox digest = %s, want reused %s", got, sandboxDesc.Digest)
	}
	if got := index.Annotations[AnnotationVMM]; got != "cloud-hypervisor" {
		t.Fatalf("index VMM = %q", got)
	}
	if got := index.Manifests[1].Annotations[AnnotationVMM]; got != "cloud-hypervisor" {
		t.Fatalf("mem component VMM = %q", got)
	}
	if got := index.Annotations[AnnotationMemorySizeMB]; got != "512" {
		t.Fatalf("index memory size = %q", got)
	}
	if got := index.Manifests[1].Annotations[AnnotationMemorySizeMB]; got != "512" {
		t.Fatalf("mem component memory size = %q", got)
	}

	resolved, info, err := inspectBootIndexByDigest(ctx, store, indexDesc.Digest.String())
	if err != nil {
		t.Fatalf("inspectBootIndexByDigest: %v", err)
	}
	if resolved.Digest != indexDesc.Digest || resolved.Size != indexDesc.Size || resolved.MediaType != indexDesc.MediaType {
		t.Fatalf("resolved descriptor = %#v, want %#v", resolved, indexDesc)
	}
	if !info.Resume || info.VMMName != "cloud-hypervisor" || info.MemorySizeMB != 512 {
		t.Fatalf("boot index info = %#v", info)
	}
	if info.RootfsDescriptor.Digest != rootfsDesc.Digest {
		t.Fatalf("inspected rootfs digest = %s, want %s", info.RootfsDescriptor.Digest, rootfsDesc.Digest)
	}
	if info.SandboxDescriptor.Digest != sandboxDesc.Digest {
		t.Fatalf("inspected sandbox digest = %s, want %s", info.SandboxDescriptor.Digest, sandboxDesc.Digest)
	}
}

func TestBuildBootIndexInContentRejectsInvalidSandboxAndVMMCombinations(t *testing.T) {
	valid := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("component"),
		Size:      1,
	}
	cases := []struct {
		name string
		opts BootIndexContentOptions
		want string
	}{
		{
			name: "missing sandbox source",
			opts: BootIndexContentOptions{RootfsDescriptor: valid},
			want: "exactly one sandbox source",
		},
		{
			name: "prepared and kernel assets",
			opts: BootIndexContentOptions{RootfsDescriptor: valid, SandboxDescriptor: valid, KernelPath: "kernel", InitrdPath: "initrd"},
			want: "exactly one sandbox source",
		},
		{
			name: "half kernel assets",
			opts: BootIndexContentOptions{RootfsDescriptor: valid, KernelPath: "kernel"},
			want: "provided together",
		},
		{
			name: "mem without VMM",
			opts: BootIndexContentOptions{RootfsDescriptor: valid, MemDescriptor: valid, SandboxDescriptor: valid},
			want: "VMM name is required",
		},
		{
			name: "mem without memory size",
			opts: BootIndexContentOptions{RootfsDescriptor: valid, MemDescriptor: valid, SandboxDescriptor: valid, VMMName: "stratovirt"},
			want: "positive memory size",
		},
		{
			name: "VMM without mem",
			opts: BootIndexContentOptions{RootfsDescriptor: valid, SandboxDescriptor: valid, VMMName: "stratovirt"},
			want: "requires a mem-snapshot",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := localcontent.NewStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			_, err = BuildBootIndexInContent(context.Background(), store, tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("BuildBootIndexInContent() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestBuildBootIndexInContentRejectsWrongPreparedComponentKind(t *testing.T) {
	requireMkfsErofs(t)

	ctx := context.Background()
	dir := t.TempDir()
	store, err := localcontent.NewStore(filepath.Join(dir, "content"))
	if err != nil {
		t.Fatal(err)
	}
	rootfsDesc, err := BuildNativeComponentInContent(ctx, store, []string{writeComponentRoot(t, dir, "rootfs-kind")}, KindRootfs)
	if err != nil {
		t.Fatal(err)
	}
	sandboxDesc, err := BuildNativeComponentInContent(ctx, store, []string{writeComponentRoot(t, dir, "sandbox-kind")}, KindSandbox)
	if err != nil {
		t.Fatal(err)
	}
	sandboxDesc.Annotations["io.conch.kind"] = KindMemSnapshot

	_, err = BuildBootIndexInContent(ctx, store, BootIndexContentOptions{
		RootfsDescriptor:  rootfsDesc,
		SandboxDescriptor: sandboxDesc,
	})
	if err == nil || !strings.Contains(err.Error(), `sandbox descriptor has component kind "mem-snapshot"`) {
		t.Fatalf("BuildBootIndexInContent() error = %v", err)
	}
}

func TestBuildNativeComponentInContentRejectsUnsafeInputs(t *testing.T) {
	store, err := localcontent.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, err = BuildNativeComponentInContent(context.Background(), store, []string{"unused"}, "unknown")
	if err == nil || !strings.Contains(err.Error(), "unsupported native component kind") {
		t.Fatalf("unknown kind error = %v", err)
	}

	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	_, err = BuildNativeComponentInContent(context.Background(), store, []string{link}, KindMemSnapshot)
	if err == nil || !strings.Contains(err.Error(), "is a symlink") {
		t.Fatalf("symlink error = %v", err)
	}
}

func TestInspectBootIndexContentRejectsMissingClosure(t *testing.T) {
	ctx := context.Background()
	store, err := localcontent.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	component := func(kind, value string) ocispec.Descriptor {
		return ocispec.Descriptor{
			MediaType:   ocispec.MediaTypeImageManifest,
			Digest:      digest.FromString(value),
			Size:        1,
			Annotations: map[string]string{"io.conch.kind": kind},
		}
	}
	indexDesc, err := writeIndexToContent(ctx, store, []ocispec.Descriptor{
		component(KindRootfs, "missing-rootfs"),
		component(KindSandbox, "missing-sandbox"),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = InspectBootIndexContent(ctx, store, indexDesc)
	if err == nil || !strings.Contains(err.Error(), "closure") {
		t.Fatalf("InspectBootIndexContent() error = %v, want closure error", err)
	}
}

func TestInspectBootIndexContentRejectsMismatchedVMMCapability(t *testing.T) {
	requireMkfsErofs(t)

	ctx := context.Background()
	dir := t.TempDir()
	store, err := localcontent.NewStore(filepath.Join(dir, "content"))
	if err != nil {
		t.Fatal(err)
	}
	build := func(kind, name string) ocispec.Descriptor {
		t.Helper()
		desc, err := BuildNativeComponentInContent(ctx, store, []string{writeComponentRoot(t, dir, name)}, kind)
		if err != nil {
			t.Fatal(err)
		}
		return desc
	}
	rootfsDesc := build(KindRootfs, "rootfs-vmm")
	memDesc := build(KindMemSnapshot, "mem-vmm")
	memDesc.Annotations[AnnotationVMM] = "stratovirt"
	sandboxDesc := build(KindSandbox, "sandbox-vmm")
	indexDesc, err := writeIndexToContent(ctx, store, []ocispec.Descriptor{rootfsDesc, memDesc, sandboxDesc}, map[string]string{
		AnnotationVMM: "cloud-hypervisor",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = InspectBootIndexContent(ctx, store, indexDesc)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("InspectBootIndexContent() error = %v", err)
	}
}

func TestInspectBootIndexMemorySizeCompatibilityIsVMMSpecific(t *testing.T) {
	requireMkfsErofs(t)

	ctx := context.Background()
	dir := t.TempDir()
	store, err := localcontent.NewStore(filepath.Join(dir, "content"))
	if err != nil {
		t.Fatal(err)
	}
	build := func(kind, name string) ocispec.Descriptor {
		t.Helper()
		desc, err := BuildNativeComponentInContent(ctx, store, []string{writeComponentRoot(t, dir, name)}, kind)
		if err != nil {
			t.Fatal(err)
		}
		return desc
	}
	rootfsDesc := build(KindRootfs, "rootfs-memory-compat")
	memDesc := build(KindMemSnapshot, "mem-memory-compat")
	sandboxDesc := build(KindSandbox, "sandbox-memory-compat")

	for _, tt := range []struct {
		vmm       string
		wantError bool
	}{
		{vmm: "cloud-hypervisor"},
		{vmm: "stratovirt", wantError: true},
	} {
		t.Run(tt.vmm, func(t *testing.T) {
			candidateMem := memDesc
			candidateMem.Annotations = mergeAnnotations(memDesc.Annotations, map[string]string{AnnotationVMM: tt.vmm})
			indexDesc, err := writeIndexToContent(ctx, store, []ocispec.Descriptor{rootfsDesc, candidateMem, sandboxDesc}, map[string]string{
				AnnotationVMM: tt.vmm,
			})
			if err != nil {
				t.Fatal(err)
			}
			info, err := InspectBootIndexContent(ctx, store, indexDesc)
			if tt.wantError {
				if err == nil || !strings.Contains(err.Error(), AnnotationMemorySizeMB) {
					t.Fatalf("InspectBootIndexContent() error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("InspectBootIndexContent() error = %v", err)
			}
			if info.MemorySizeMB != 0 {
				t.Fatalf("legacy CLH memory size = %d", info.MemorySizeMB)
			}
		})
	}
}

func TestBootIndexRecordNameIsDerivedFromTemplateDigest(t *testing.T) {
	dgst := digest.FromString("template identity")
	got, err := BootIndexRecordName(dgst.String())
	if err != nil {
		t.Fatal(err)
	}
	want := "localhost/conch/template:" + dgst.Algorithm().String() + "-" + dgst.Encoded()
	if got != want {
		t.Fatalf("BootIndexRecordName() = %q, want %q", got, want)
	}
}

func TestRemoveBootIndexRecordDoesNotDeleteRetargetedRecord(t *testing.T) {
	ctx := containerdclient.NewNamespaceContext(context.Background())
	client := newMetadataClient(t, ctx)
	store := client.ContentStore()
	name := "localhost/conch/template:retargeted-during-cleanup"

	original := writeTestBootIndex(t, ctx, store, "original")
	if _, err := client.ImageService().Create(ctx, images.Image{Name: name, Target: original}); err != nil {
		t.Fatal(err)
	}
	replacement := writeTestBootIndex(t, ctx, store, "replacement")
	if _, err := client.ImageService().Update(ctx, images.Image{Name: name, Target: replacement}, "target"); err != nil {
		t.Fatal(err)
	}
	if err := RemoveBootIndexRecord(ctx, client, name, original.Digest.String(), false); err != nil {
		t.Fatalf("remove Boot Index record after retarget: %v", err)
	}
	got, err := client.ImageService().Get(ctx, name)
	if err != nil {
		t.Fatalf("retargeted image record was deleted: %v", err)
	}
	if got.Target.Digest != replacement.Digest {
		t.Fatalf("retargeted image digest = %s, want %s", got.Target.Digest, replacement.Digest)
	}
}

func TestUnpackBootIndexReleasesOperationLeaseAfterInspectFailure(t *testing.T) {
	ctx := containerdclient.NewNamespaceContext(context.Background())
	client := newMetadataClient(t, ctx)
	missing := digest.FromString("missing-boot-index")

	if err := UnpackBootIndex(ctx, client, missing.String()); err == nil {
		t.Fatal("UnpackBootIndex() succeeded for missing content")
	}
	items, err := client.LeasesService().List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.Labels["io.conch.lease.kind"] == "operation" {
			t.Fatalf("operation lease %s remained after failed unpack", item.ID)
		}
	}
}

func newMetadataClient(t *testing.T, ctx context.Context) *containerdclient.Client {
	t.Helper()
	root := t.TempDir()
	localStore, err := localcontent.NewStore(filepath.Join(root, "content"))
	if err != nil {
		t.Fatal(err)
	}
	boltDB, err := bolt.Open(filepath.Join(root, "metadata.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boltDB.Close() })
	db := metadata.NewDB(boltDB, localStore, nil)
	if err := db.Init(ctx); err != nil {
		t.Fatal(err)
	}
	rawClient, err := containerd.New("", containerd.WithServices(
		containerd.WithContentStore(db.ContentStore()),
		containerd.WithImageStore(metadata.NewImageStore(db)),
		containerd.WithLeasesService(metadata.NewLeaseManager(db)),
	))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rawClient.Close() })
	return &containerdclient.Client{Client: rawClient}
}

func writeTestBootIndex(t *testing.T, ctx context.Context, store content.Store, marker string) ocispec.Descriptor {
	t.Helper()
	rootfs := writeTestComponent(t, ctx, store, KindRootfs, marker+"-rootfs")
	sandbox := writeTestComponent(t, ctx, store, KindSandbox, marker+"-sandbox")
	desc, err := BuildBootIndexInContent(ctx, store, BootIndexContentOptions{
		RootfsDescriptor: rootfs, SandboxDescriptor: sandbox,
	})
	if err != nil {
		t.Fatal(err)
	}
	return desc
}

func writeTestComponent(t *testing.T, ctx context.Context, store content.Store, kind, marker string) ocispec.Descriptor {
	t.Helper()
	layer := writeTestBlob(t, ctx, store, []byte(marker), ocispec.MediaTypeImageLayer)
	config := ocispec.Image{
		RootFS: ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{layer.Digest}},
		Config: ocispec.ImageConfig{Labels: map[string]string{"io.conch.component.type": kind}},
	}
	configDesc := writeTestJSON(t, ctx, store, config, ocispec.MediaTypeImageConfig)
	manifest := ocispec.Manifest{
		Versioned: ispec.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDesc,
		Layers:    []ocispec.Descriptor{layer},
	}
	manifestDesc := writeTestJSON(t, ctx, store, manifest, ocispec.MediaTypeImageManifest)
	manifestDesc.Annotations = map[string]string{"io.conch.kind": kind}
	return manifestDesc
}

func writeTestJSON(t *testing.T, ctx context.Context, store content.Store, value any, mediaType string) ocispec.Descriptor {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return writeTestBlob(t, ctx, store, raw, mediaType)
}

func writeTestBlob(t *testing.T, ctx context.Context, store content.Store, raw []byte, mediaType string) ocispec.Descriptor {
	t.Helper()
	desc := ocispec.Descriptor{MediaType: mediaType, Digest: digest.FromBytes(raw), Size: int64(len(raw))}
	if err := content.WriteBlob(ctx, store, "test-"+desc.Digest.Encoded(), bytes.NewReader(raw), desc); err != nil {
		t.Fatal(err)
	}
	return desc
}

func writeComponentRoot(t *testing.T, dir, name string) string {
	t.Helper()
	root := filepath.Join(dir, name+"-root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte(name), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func requireMkfsErofs(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		t.Skip("mkfs.erofs not available")
	}
}
