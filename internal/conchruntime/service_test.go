package conchruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	containerdclient "github.com/openeuler/Conch/internal/adapters/containerd/client"
	containerdhost "github.com/openeuler/Conch/internal/adapters/containerd/host"
	"github.com/openeuler/Conch/internal/daemon/state"
	conchimage "github.com/openeuler/Conch/internal/image"
	"github.com/openeuler/Conch/internal/netstack"
	"github.com/openeuler/Conch/internal/runtimeapi"
	"github.com/openeuler/Conch/internal/sandbox"
	conchtemplate "github.com/openeuler/Conch/internal/template"
)

type fakeSandboxOps struct {
	req                sandbox.CreateRequest
	checkpointRequests []sandbox.CheckpointRequest
	checkpointResults  []sandbox.CheckpointResult
	checkpointErr      error
	createResult       sandbox.CreateResult
	createErr          error
	createCalls        int
	deleteErr          error
	updateReq          sandbox.NetworkUpdateRequest
	updateErr          error
}

type serializedDeleteOps struct {
	fakeSandboxOps
	firstEntered chan struct{}
	releaseFirst chan struct{}
	calls        atomic.Int32
}

func (f *serializedDeleteOps) Delete(sandbox.DeleteRequest) error {
	if f.calls.Add(1) == 1 {
		close(f.firstEntered)
		<-f.releaseFirst
		return nil
	}
	return errors.New("sandbox not found")
}

func (f *fakeSandboxOps) Create(req sandbox.CreateRequest) (sandbox.CreateResult, error) {
	f.createCalls++
	f.req = req
	result := f.createResult
	if result.SandboxID == "" {
		result.SandboxID = req.SandboxID
	}
	if result.IP == "" {
		result.IP = "192.0.2.10"
	}
	if result.AgentToken == "" {
		result.AgentToken = req.AgentToken
	}
	return result, f.createErr
}

func (f *fakeSandboxOps) Delete(sandbox.DeleteRequest) error {
	return f.deleteErr
}

func (f *fakeSandboxOps) Suspend(sandbox.LifecycleRequest) error {
	return nil
}

func (f *fakeSandboxOps) Resume(sandbox.LifecycleRequest) error {
	return nil
}

func (f *fakeSandboxOps) UpdateNetwork(_ context.Context, req sandbox.NetworkUpdateRequest) error {
	f.updateReq = req
	return f.updateErr
}

func (f *fakeSandboxOps) Checkpoint(req sandbox.CheckpointRequest) (sandbox.CheckpointResult, error) {
	call := len(f.checkpointRequests)
	f.checkpointRequests = append(f.checkpointRequests, req)
	if f.checkpointErr != nil {
		return sandbox.CheckpointResult{}, f.checkpointErr
	}
	if call < len(f.checkpointResults) {
		return f.checkpointResults[call], nil
	}
	return sandbox.CheckpointResult{}, nil
}

func TestCheckpointSandboxPublishesCaptureAndAtomicallyAdvancesHead(t *testing.T) {
	ctx := context.Background()
	host := newRuntimeImageHost(t)
	t0Digest := buildColdBootIndex(t, host, "checkpoint-t0")
	memRoot := t.TempDir()
	captured := sandbox.CapturedBootComponents{
		MemRootPath:  memRoot,
		VMMName:      "cloud-hypervisor",
		MemorySizeMB: 512,
	}
	sandboxOps := &fakeSandboxOps{checkpointResults: []sandbox.CheckpointResult{captured}}
	store := newTestStore(t)
	svc := New(sandboxOps, host.Client(), store)
	seedTemplate(t, ctx, svc.Templates, t0Digest, conchtemplate.BootModeCold)

	before := state.SandboxRecord{
		SandboxID:                     "sandbox-a",
		CheckpointHeadTemplateID:      t0Digest,
		CheckpointHeadBootIndexDigest: t0Digest,
	}
	if err := store.UpsertSandbox(ctx, before); err != nil {
		t.Fatalf("UpsertSandbox() error = %v", err)
	}

	result, err := svc.CheckpointSandbox(ctx, SandboxCheckpointOptions{
		SandboxID: "sandbox-a",
		Labels:    map[string]string{"generation": "t1"},
	})
	if err != nil {
		t.Fatalf("CheckpointSandbox() error = %v", err)
	}
	if result.TemplateID == "" || result.TemplateID != result.BootIndexDigest || result.BootIndexDigest == t0Digest {
		t.Fatalf("CheckpointSandbox() = %#v", result)
	}
	if len(sandboxOps.checkpointRequests) != 1 {
		t.Fatalf("checkpoint requests = %#v, want one", sandboxOps.checkpointRequests)
	}
	// Generation identity and parent snapshot IDs are deliberately absent from
	// the runtime capture seam; it receives only the sandbox identity.
	if got, want := sandboxOps.checkpointRequests[0], (sandbox.CheckpointRequest{
		SandboxID: "sandbox-a",
	}); got != want {
		t.Fatalf("checkpoint request = %#v, want %#v", got, want)
	}
	if _, err := os.Stat(memRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("captured memory root still exists after publication: %v", err)
	}

	t1, err := store.GetTemplate(ctx, result.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate(t1) error = %v", err)
	}
	if t1.ID != result.BootIndexDigest || t1.BootMode != conchtemplate.BootModeResume {
		t.Fatalf("t1 entry = %#v", t1)
	}
	if t1.ParentTemplateID != t0Digest || t1.SourceSandboxID != "sandbox-a" || t1.Labels["generation"] != "t1" {
		t.Fatalf("t1 lineage = %#v", t1)
	}

	after, err := store.GetSandbox(ctx, "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	wantAfter := before
	wantAfter.CheckpointHeadTemplateID = result.TemplateID
	wantAfter.CheckpointHeadBootIndexDigest = result.BootIndexDigest
	if !reflect.DeepEqual(after, wantAfter) {
		t.Fatalf("sandbox record after checkpoint = %#v, want only checkpoint head changed from %#v", after, before)
	}
}

func TestCheckpointSandboxDoesNotPersistBeforeValidationSucceeds(t *testing.T) {
	ctx := context.Background()
	host := newRuntimeImageHost(t)
	sourceDigest := digest.FromString("checkpoint-source").String()
	memRoot := t.TempDir()
	sandboxOps := &fakeSandboxOps{checkpointResults: []sandbox.CheckpointResult{{
		MemRootPath:  memRoot,
		VMMName:      "cloud-hypervisor",
		MemorySizeMB: 512,
	}}}
	store := newTestStore(t)
	svc := New(sandboxOps, host.Client(), store)
	seedTemplate(t, ctx, svc.Templates, sourceDigest, conchtemplate.BootModeCold)
	before := state.SandboxRecord{
		SandboxID:                     "sandbox-validation-failure",
		CheckpointHeadTemplateID:      sourceDigest,
		CheckpointHeadBootIndexDigest: sourceDigest,
	}
	if err := store.UpsertSandbox(ctx, before); err != nil {
		t.Fatalf("UpsertSandbox() error = %v", err)
	}

	if _, err := svc.CheckpointSandbox(ctx, SandboxCheckpointOptions{
		SandboxID: before.SandboxID,
	}); err == nil {
		t.Fatal("CheckpointSandbox() error = nil, want validation failure")
	}
	templates, err := store.ListTemplates(ctx)
	if err != nil {
		t.Fatalf("ListTemplates() error = %v", err)
	}
	if len(templates) != 1 || templates[0].ID != sourceDigest {
		t.Fatalf("templates after failed validation = %#v, want only source template", templates)
	}
	after, err := store.GetSandbox(ctx, before.SandboxID)
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("sandbox changed after failed validation: got %#v, want %#v", after, before)
	}
}

func TestCheckpointSandboxBuildsConsecutiveTemplateLineage(t *testing.T) {
	ctx := context.Background()
	host := newRuntimeImageHost(t)
	t0Digest := buildColdBootIndex(t, host, "lineage-t0")
	memRoot1 := t.TempDir()
	memRoot2 := t.TempDir()
	sandboxOps := &fakeSandboxOps{checkpointResults: []sandbox.CheckpointResult{
		{MemRootPath: memRoot1, VMMName: "stratovirt", MemorySizeMB: 256},
		{MemRootPath: memRoot2, VMMName: "stratovirt", MemorySizeMB: 256},
	}}
	store := newTestStore(t)
	svc := New(sandboxOps, host.Client(), store)
	seedTemplate(t, ctx, svc.Templates, t0Digest, conchtemplate.BootModeCold)
	if err := store.UpsertSandbox(ctx, state.SandboxRecord{
		SandboxID:                     "sandbox-lineage",
		CheckpointHeadTemplateID:      t0Digest,
		CheckpointHeadBootIndexDigest: t0Digest,
	}); err != nil {
		t.Fatalf("UpsertSandbox() error = %v", err)
	}

	t1Result, err := svc.CheckpointSandbox(ctx, SandboxCheckpointOptions{SandboxID: "sandbox-lineage"})
	if err != nil {
		t.Fatalf("CheckpointSandbox(t1) error = %v", err)
	}
	t2Result, err := svc.CheckpointSandbox(ctx, SandboxCheckpointOptions{SandboxID: "sandbox-lineage"})
	if err != nil {
		t.Fatalf("CheckpointSandbox(t2) error = %v", err)
	}
	if t1Result.TemplateID == "" || t2Result.TemplateID == "" || t1Result.TemplateID == t2Result.TemplateID {
		t.Fatalf("checkpoint template ids = (%q, %q)", t1Result.TemplateID, t2Result.TemplateID)
	}
	if t1Result.BootIndexDigest == "" || t2Result.BootIndexDigest == "" || t1Result.BootIndexDigest == t2Result.BootIndexDigest {
		t.Fatalf("checkpoint digests = (%q, %q)", t1Result.BootIndexDigest, t2Result.BootIndexDigest)
	}

	t1, err := store.GetTemplate(ctx, t1Result.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate(t1) error = %v", err)
	}
	t2, err := store.GetTemplate(ctx, t2Result.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate(t2) error = %v", err)
	}
	if t1.ParentTemplateID != t0Digest || t2.ParentTemplateID != t1.ID {
		t.Fatalf("template lineage: t1 parent = %q, t2 parent = %q", t1.ParentTemplateID, t2.ParentTemplateID)
	}
	if t1.ID != t1Result.BootIndexDigest || t2.ID != t2Result.BootIndexDigest {
		t.Fatalf("checkpoint template entries = (%#v, %#v)", t1, t2)
	}

	rec, err := store.GetSandbox(ctx, "sandbox-lineage")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if rec.CheckpointHeadTemplateID != t2.ID || rec.CheckpointHeadBootIndexDigest != t2Result.BootIndexDigest {
		t.Fatalf("sandbox checkpoint head = (%q, %q), want (%q, %q)",
			rec.CheckpointHeadTemplateID, rec.CheckpointHeadBootIndexDigest, t2.ID, t2Result.BootIndexDigest)
	}
}

func TestRemoveSandboxKeepsRecordWhenCleanupFails(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	before := state.SandboxRecord{
		SandboxID:                     "sandbox-1",
		CheckpointHeadTemplateID:      "tmpl-1",
		CheckpointHeadBootIndexDigest: digest.FromString("sandbox-1-head").String(),
	}
	if err := store.UpsertSandbox(ctx, before); err != nil {
		t.Fatalf("UpsertSandbox() error = %v", err)
	}

	sandboxOps := &fakeSandboxOps{deleteErr: errors.New("cleanup failed")}
	svc := New(sandboxOps, nil, store)
	if err := svc.RemoveSandbox(ctx, "sandbox-1"); err == nil {
		t.Fatalf("RemoveSandbox() error = nil, want cleanup error")
	}
	rec, err := store.GetSandbox(ctx, "sandbox-1")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if rec != before {
		t.Fatalf("sandbox record = %#v, want unchanged %#v", rec, before)
	}
}

func TestRemoveSandboxDoesNotCreateStateForUnknownRuntime(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	sandboxOps := &fakeSandboxOps{deleteErr: errors.New("sandbox not found")}
	svc := New(sandboxOps, nil, store)
	if err := svc.RemoveSandbox(ctx, "missing-sandbox"); err != nil {
		t.Fatalf("RemoveSandbox() error = %v", err)
	}

	if _, err := store.GetSandbox(ctx, "missing-sandbox"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetSandbox() error = %v, want ErrNotFound", err)
	}
}

func TestConcurrentRemoveSandboxCallsAreSerialized(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	rec := state.SandboxRecord{
		SandboxID:                     "sandbox-serialized",
		CheckpointHeadTemplateID:      "tmpl-1",
		CheckpointHeadBootIndexDigest: digest.FromString("sandbox-serialized-head").String(),
	}
	if err := store.UpsertSandbox(ctx, rec); err != nil {
		t.Fatal(err)
	}
	ops := &serializedDeleteOps{
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	svc := New(ops, nil, store)

	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() { firstDone <- svc.RemoveSandbox(ctx, rec.SandboxID) }()
	<-ops.firstEntered
	go func() { secondDone <- svc.RemoveSandbox(ctx, rec.SandboxID) }()
	select {
	case err := <-secondDone:
		t.Fatalf("second RemoveSandbox returned before first completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(ops.releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first RemoveSandbox() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second RemoveSandbox() error = %v", err)
	}
	if got := ops.calls.Load(); got != 2 {
		t.Fatalf("Delete() calls = %d, want 2 serialized calls", got)
	}
	if _, err := store.GetSandbox(ctx, rec.SandboxID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("sandbox record remains after Remove: %v", err)
	}
}

func TestCreateSandboxStoresAPIAndCheckpointMetadata(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	bootIndexDigest := digest.FromString("sandbox-1-boot-index").String()

	sandboxOps := &fakeSandboxOps{
		createResult: sandbox.CreateResult{
			BootIndexDigest: bootIndexDigest,
		},
	}
	svc := New(sandboxOps, nil, store)

	result, err := svc.CreateSandbox(ctx, SandboxCreateOptions{
		SandboxID:  "sandbox-1",
		TemplateID: "tmpl-1",
	})
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}

	rec, err := store.GetSandbox(ctx, "sandbox-1")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	want := state.SandboxRecord{
		SandboxID:                     "sandbox-1",
		State:                         state.SandboxReady,
		CreatedAt:                     result.CreatedAt,
		SourceTemplateID:              "tmpl-1",
		CheckpointHeadTemplateID:      "tmpl-1",
		CheckpointHeadBootIndexDigest: bootIndexDigest,
		IP:                            result.IP,
	}
	if rec != want {
		t.Fatalf("sandbox record = %#v, want %#v", rec, want)
	}
}

func TestCreateSandboxFailureDoesNotPersistRecord(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	svc := New(&fakeSandboxOps{createErr: errors.New("create failed")}, nil, store)

	if _, err := svc.CreateSandbox(ctx, SandboxCreateOptions{
		SandboxID:  "sandbox-1",
		TemplateID: "tmpl-1",
	}); err == nil {
		t.Fatal("CreateSandbox() error = nil, want create failure")
	}
	if _, err := store.GetSandbox(ctx, "sandbox-1"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetSandbox() error = %v, want ErrNotFound", err)
	}
}

func TestCreateSandboxRejectsExistingGlobalIDBeforeRuntimeCreate(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.UpsertSandbox(ctx, state.SandboxRecord{
		SandboxID:                     "sandbox-1",
		CheckpointHeadTemplateID:      "tmpl-existing",
		CheckpointHeadBootIndexDigest: digest.FromString("existing-sandbox").String(),
	}); err != nil {
		t.Fatalf("UpsertSandbox() seed error = %v", err)
	}
	ops := &fakeSandboxOps{}
	svc := New(ops, nil, store)

	_, err := svc.CreateSandbox(ctx, SandboxCreateOptions{
		SandboxID:  "sandbox-1",
		TemplateID: "tmpl-new",
	})
	if !errors.Is(err, ErrSandboxAlreadyExists) {
		t.Fatalf("CreateSandbox() error = %v, want ErrSandboxAlreadyExists", err)
	}
	if ops.createCalls != 0 {
		t.Fatalf("runtime Create() calls = %d, want 0", ops.createCalls)
	}
}

func TestCreateSandboxAppliesDefaults(t *testing.T) {
	sandboxOps := &fakeSandboxOps{}
	svc := New(sandboxOps, nil, nil)
	svc.SetSandboxDefaults(SandboxDefaults{
		TemplateID: "tmpl_default",
		VMMName:    "cloud-hypervisor",
		VCPUNum:    2,
		VCPUMax:    4,
		RamMB:      4096,
	})

	result, err := svc.CreateSandbox(context.Background(), SandboxCreateOptions{
		SandboxID: "sandbox-1",
		Env:       map[string]string{"SOME_RANDOM_KEY": "key123"},
	})
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}

	if sandboxOps.req.TemplateID != "tmpl_default" {
		t.Fatalf("TemplateID = %q", sandboxOps.req.TemplateID)
	}
	if sandboxOps.req.VMMName != "cloud-hypervisor" {
		t.Fatalf("VmmName = %q", sandboxOps.req.VMMName)
	}
	if sandboxOps.req.VCPUNum != 2 || sandboxOps.req.VCPUMax != 4 || sandboxOps.req.RAMMB != 4096 {
		t.Fatalf("resources = vcpu:%d max:%d ram:%d", sandboxOps.req.VCPUNum, sandboxOps.req.VCPUMax, sandboxOps.req.RAMMB)
	}
	if sandboxOps.req.AgentToken == "" {
		t.Fatal("AgentToken is empty")
	}
	if got := sandboxOps.req.Env["SOME_RANDOM_KEY"]; got != "key123" {
		t.Fatalf("Env[SOME_RANDOM_KEY] = %q, want key123", got)
	}
	if result.AgentToken != sandboxOps.req.AgentToken {
		t.Fatalf("result.AgentToken = %q, want generated token", result.AgentToken)
	}
	if result.SandboxID != "sandbox-1" || sandboxOps.req.SandboxID != "sandbox-1" {
		t.Fatalf("sandbox identity = result:%q request:%q", result.SandboxID, sandboxOps.req.SandboxID)
	}
}

func TestCreateSandboxRejectsMissingOrWhitespaceDefaultTemplate(t *testing.T) {
	for _, defaultTemplate := range []string{"", " \t "} {
		t.Run(fmt.Sprintf("default %q", defaultTemplate), func(t *testing.T) {
			sandboxOps := &fakeSandboxOps{}
			svc := New(sandboxOps, nil, nil)
			svc.SetSandboxDefaults(SandboxDefaults{TemplateID: defaultTemplate})

			_, err := svc.CreateSandbox(context.Background(), SandboxCreateOptions{TemplateID: " \n "})
			if !errors.Is(err, ErrTemplateIDRequired) {
				t.Fatalf("CreateSandbox() error = %v, want ErrTemplateIDRequired", err)
			}
			if sandboxOps.req.TemplateID != "" {
				t.Fatalf("sandbox create was called with %#v", sandboxOps.req)
			}
		})
	}
}

func TestCreateSandboxGeneratesSingleSandboxID(t *testing.T) {
	store := newTestStore(t)
	sandboxOps := &fakeSandboxOps{createResult: sandbox.CreateResult{
		BootIndexDigest: digest.FromString("generated-sandbox-boot-index").String(),
	}}
	svc := New(sandboxOps, nil, store)
	svc.SetSandboxDefaults(SandboxDefaults{TemplateID: "tmpl-default"})

	result, err := svc.CreateSandbox(context.Background(), SandboxCreateOptions{TemplateID: "tmpl-1"})
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	if result.SandboxID == "" || sandboxOps.req.SandboxID != result.SandboxID {
		t.Fatalf("sandbox identity = result:%q request:%q", result.SandboxID, sandboxOps.req.SandboxID)
	}
	record, err := store.GetSandbox(context.Background(), result.SandboxID)
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if record.SandboxID != result.SandboxID {
		t.Fatalf("record.SandboxID = %q, want %q", record.SandboxID, result.SandboxID)
	}
}

func TestCreateSandboxKeepsExplicitOptions(t *testing.T) {
	sandboxOps := &fakeSandboxOps{}
	svc := New(sandboxOps, nil, nil)
	svc.SetSandboxDefaults(SandboxDefaults{
		TemplateID: "tmpl_default",
		VMMName:    "default-vmm",
		VCPUNum:    2,
		VCPUMax:    2,
		RamMB:      4096,
	})

	_, err := svc.CreateSandbox(context.Background(), SandboxCreateOptions{
		SandboxID:  "sandbox-1",
		TemplateID: "tmpl_resume_explicit",
		VMMName:    "explicit-vmm",
		VCPUNum:    6,
		VCPUMax:    8,
		RamMB:      8192,
	})
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}

	if sandboxOps.req.TemplateID != "tmpl_resume_explicit" || sandboxOps.req.VMMName != "explicit-vmm" {
		t.Fatalf("request = %#v", sandboxOps.req)
	}
	if sandboxOps.req.VCPUNum != 6 || sandboxOps.req.VCPUMax != 8 || sandboxOps.req.RAMMB != 8192 {
		t.Fatalf("resources = vcpu:%d max:%d ram:%d", sandboxOps.req.VCPUNum, sandboxOps.req.VCPUMax, sandboxOps.req.RAMMB)
	}
}

func TestCreateSandboxPersistsNetworkPolicy(t *testing.T) {
	store := newTestStore(t)
	sandboxOps := &fakeSandboxOps{createResult: sandbox.CreateResult{
		BootIndexDigest: digest.FromString("sandbox-network").String(),
	}}
	svc := New(sandboxOps, nil, store)
	svc.SetSandboxDefaults(SandboxDefaults{TemplateID: "tmpl-default"})
	policy := &netstack.SandboxNetworkConfig{DenyOut: []string{"192.0.2.10"}}

	if _, err := svc.CreateSandbox(context.Background(), SandboxCreateOptions{
		SandboxID: "sandbox-network",
		Network:   policy,
	}); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	if sandboxOps.req.Network != policy {
		t.Fatalf("runtime network = %#v, want %#v", sandboxOps.req.Network, policy)
	}
	record, err := store.GetSandbox(context.Background(), "sandbox-network")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if !reflect.DeepEqual(record.Network, policy) {
		t.Fatalf("stored network = %#v, want %#v", record.Network, policy)
	}
}

func TestUpdateSandboxNetworkConfigReplacesPolicy(t *testing.T) {
	store := newTestStore(t)
	sandboxOps := &fakeSandboxOps{}
	svc := New(sandboxOps, nil, store)
	if err := store.UpsertSandbox(context.Background(), state.SandboxRecord{
		SandboxID:                     "sandbox-network",
		State:                         state.SandboxReady,
		CheckpointHeadTemplateID:      "tmpl-network",
		CheckpointHeadBootIndexDigest: digest.FromString("sandbox-network").String(),
		Network:                       &netstack.SandboxNetworkConfig{AllowOut: []string{"192.0.2.10"}},
	}); err != nil {
		t.Fatalf("seed sandbox: %v", err)
	}
	replacement := &netstack.SandboxNetworkConfig{DenyOut: []string{"198.51.100.10"}}

	if err := svc.UpdateSandboxNetworkConfig(context.Background(), SandboxNetworkUpdateOptions{
		SandboxID: "sandbox-network",
		Network:   replacement,
	}); err != nil {
		t.Fatalf("UpdateSandboxNetworkConfig() error = %v", err)
	}
	if sandboxOps.updateReq.Network != replacement {
		t.Fatalf("runtime network = %#v, want %#v", sandboxOps.updateReq.Network, replacement)
	}
	record, err := store.GetSandbox(context.Background(), "sandbox-network")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if !reflect.DeepEqual(record.Network, replacement) {
		t.Fatalf("stored network = %#v, want %#v", record.Network, replacement)
	}
}

func TestUpdateSandboxNetworkConfigAllowsSuspendedSandbox(t *testing.T) {
	store := newTestStore(t)
	sandboxOps := &fakeSandboxOps{}
	svc := New(sandboxOps, nil, store)
	if err := store.UpsertSandbox(context.Background(), state.SandboxRecord{
		SandboxID:                     "sandbox-suspended",
		State:                         state.SandboxSuspended,
		CheckpointHeadTemplateID:      "tmpl-suspended",
		CheckpointHeadBootIndexDigest: digest.FromString("sandbox-suspended").String(),
	}); err != nil {
		t.Fatalf("seed sandbox: %v", err)
	}
	policy := &netstack.SandboxNetworkConfig{DenyIn: []string{"192.0.2.10"}}

	if err := svc.UpdateSandboxNetworkConfig(context.Background(), SandboxNetworkUpdateOptions{
		SandboxID: "sandbox-suspended",
		Network:   policy,
	}); err != nil {
		t.Fatalf("UpdateSandboxNetworkConfig() error = %v", err)
	}
	if sandboxOps.updateReq.Network != policy {
		t.Fatalf("runtime network = %#v, want %#v", sandboxOps.updateReq.Network, policy)
	}
}

func TestCreateTemplateRequiresContainerdClient(t *testing.T) {
	store := newTestStore(t)
	svc := New(nil, nil, store)

	if _, err := svc.CreateTemplate(context.Background(), TemplateCreateOptions{}); err == nil || err.Error() != "containerd client is required" {
		t.Fatalf("CreateTemplate() error = %v, want containerd client is required", err)
	}
}

func TestUnpackTemplateResolvesBootIndexByDigest(t *testing.T) {
	ctx := context.Background()
	host := newRuntimeImageHost(t)
	bootIndexDigest := buildColdBootIndex(t, host, "explicit-unpack")
	store := newTestStore(t)
	svc := New(nil, host.Client(), store)

	if _, err := svc.Templates.Create(ctx, conchtemplate.Entry{
		ID:        bootIndexDigest,
		Origin:    conchtemplate.OriginImage,
		BootMode:  conchtemplate.BootModeCold,
		ImageName: "not-the-boot-index:latest",
	}); err != nil {
		t.Fatalf("create template: %v", err)
	}

	if err := svc.UnpackTemplate(ctx, TemplateUnpackOptions{TemplateID: bootIndexDigest}); err != nil {
		t.Fatalf("UnpackTemplate() error = %v", err)
	}
}

func TestRemoveTemplateRejectsSandboxReference(t *testing.T) {
	ctx := context.Background()
	host := newRuntimeImageHost(t)
	store := newTestStore(t)
	svc := New(nil, host.Client(), store)
	bootDigest := buildColdBootIndex(t, host, "in-use-template")
	seedTemplate(t, ctx, svc.Templates, bootDigest, conchtemplate.BootModeCold)
	if err := store.UpsertSandbox(ctx, state.SandboxRecord{
		SandboxID:                     "sandbox-in-use",
		SourceTemplateID:              bootDigest,
		CheckpointHeadTemplateID:      bootDigest,
		CheckpointHeadBootIndexDigest: bootDigest,
	}); err != nil {
		t.Fatal(err)
	}

	if err := svc.RemoveTemplate(ctx, bootDigest); !errors.Is(err, conchtemplate.ErrInUse) {
		t.Fatalf("RemoveTemplate() error = %v, want ErrInUse", err)
	}
	if _, err := svc.Templates.Get(ctx, bootDigest); err != nil {
		t.Fatalf("template was removed despite sandbox reference: %v", err)
	}
}

func TestRemoveImageProtectsTemplateArtifactsAndDeletesOCIImage(t *testing.T) {
	ctx := containerdclient.NewNamespaceContext(context.Background())
	host := newRuntimeImageHost(t)
	store := newTestStore(t)
	svc := New(nil, host.Client(), store)

	bootDigest := buildColdBootIndex(t, host, "owned-boot-index")
	bootInfo, err := host.Client().ContentStore().Info(ctx, digest.Digest(bootDigest))
	if err != nil {
		t.Fatal(err)
	}
	bootName, err := conchimage.BootIndexRecordName(bootDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.Client().ImageService().Create(ctx, images.Image{
		Name: bootName,
		Target: ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageIndex,
			Digest:    digest.Digest(bootDigest),
			Size:      bootInfo.Size,
		},
		Labels: map[string]string{conchimage.ImageKindLabel: conchimage.ImageKindBootIndexCold},
	}); err != nil {
		t.Fatal(err)
	}
	seedTemplate(t, ctx, svc.Templates, bootDigest, conchtemplate.BootModeCold)
	if err := svc.RemoveImage(ctx, runtimeapi.RemoveImageOptions{ImageName: bootName}); !errors.Is(err, conchimage.ErrTemplateManaged) {
		t.Fatalf("RemoveImage(owned boot index) error = %v, want ErrTemplateManaged", err)
	}
	if _, err := host.Client().ImageService().Get(ctx, bootName); err != nil {
		t.Fatalf("owned boot index was removed: %v", err)
	}

	ociName := "localhost/conch/rootfs:ordinary"
	ociTarget := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("ordinary-oci-image"),
		Size:      1,
	}
	if _, err := host.Client().ImageService().Create(ctx, images.Image{
		Name: ociName, Target: ociTarget,
		Labels: map[string]string{conchimage.ImageKindLabel: conchimage.ImageKindOCIImage},
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RemoveImage(ctx, runtimeapi.RemoveImageOptions{ImageName: ociName}); err != nil {
		t.Fatalf("RemoveImage(OCI) error = %v", err)
	}
	if _, err := host.Client().ImageService().Get(ctx, ociName); err == nil {
		t.Fatal("ordinary OCI image still exists after removal")
	}
}

func TestRemoveTemplateKeepsSharedComponentsAndLetsGCRemoveLastSnapshots(t *testing.T) {
	host := newRuntimeImageHost(t)
	ctx := containerdclient.NewNamespaceContext(context.Background())
	buildCtx, done, err := host.Client().WithLease(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rootfsDesc, err := conchimage.BuildNativeComponentInContent(buildCtx, host.Client().ContentStore(), []string{writeTemplateCaptureRoot(t, "cleanup-rootfs")}, conchimage.KindRootfs)
	if err != nil {
		t.Fatal(err)
	}
	sandboxDesc, err := conchimage.BuildNativeComponentInContent(buildCtx, host.Client().ContentStore(), []string{writeTemplateCaptureRoot(t, "cleanup-sandbox")}, conchimage.KindSandbox)
	if err != nil {
		t.Fatal(err)
	}
	memDesc, err := conchimage.BuildNativeComponentInContent(buildCtx, host.Client().ContentStore(), []string{writeTemplateCaptureRoot(t, "cleanup-memory")}, conchimage.KindMemSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	coldIndex, err := conchimage.BuildBootIndexInContent(buildCtx, host.Client().ContentStore(), conchimage.BootIndexContentOptions{
		RootfsDescriptor: rootfsDesc, SandboxDescriptor: sandboxDesc,
	})
	if err != nil {
		t.Fatal(err)
	}
	resumeIndex, err := conchimage.BuildBootIndexInContent(buildCtx, host.Client().ContentStore(), conchimage.BootIndexContentOptions{
		RootfsDescriptor: rootfsDesc, MemDescriptor: memDesc, SandboxDescriptor: sandboxDesc,
		VMMName: "cloud-hypervisor", MemorySizeMB: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	coldRef, err := conchimage.BootIndexRecordName(coldIndex.Digest.String())
	if err != nil {
		t.Fatal(err)
	}
	resumeRef, err := conchimage.BootIndexRecordName(resumeIndex.Digest.String())
	if err != nil {
		t.Fatal(err)
	}
	labelHandler := images.SetChildrenLabels(host.Client().ContentStore(), images.ChildrenHandler(host.Client().ContentStore()))
	for name, target := range map[string]images.Image{
		coldRef: {
			Name:   coldRef,
			Target: coldIndex,
			Labels: map[string]string{conchimage.ImageKindLabel: conchimage.ImageKindBootIndexCold},
		},
		resumeRef: {
			Name:   resumeRef,
			Target: resumeIndex,
			Labels: map[string]string{conchimage.ImageKindLabel: conchimage.ImageKindBootIndexResume},
		},
	} {
		if err := images.WalkNotEmpty(buildCtx, labelHandler, target.Target); err != nil {
			t.Fatalf("label Boot Index %s content: %v", name, err)
		}
		if _, err := host.Client().ImageService().Create(buildCtx, target); err != nil {
			t.Fatalf("create Boot Index image %s: %v", name, err)
		}
	}
	if err := done(buildCtx); err != nil {
		t.Fatalf("release build lease: %v", err)
	}

	store := newTestStore(t)
	svc := New(nil, host.Client(), store)
	seedTemplate(t, ctx, svc.Templates, coldIndex.Digest.String(), conchtemplate.BootModeCold)
	seedTemplate(t, ctx, svc.Templates, resumeIndex.Digest.String(), conchtemplate.BootModeResume)
	runtimeCtx, _, err := host.Client().WithRuntimeLease(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := conchimage.UnpackBootIndex(runtimeCtx, host.Client(), coldIndex.Digest.String()); err != nil {
		t.Fatalf("unpack cold template: %v", err)
	}
	if err := conchimage.UnpackBootIndex(runtimeCtx, host.Client(), resumeIndex.Digest.String()); err != nil {
		t.Fatalf("unpack resume template: %v", err)
	}
	assertNoTemplateOperationLeases(t, ctx, host.Client().LeasesService())
	for kind, component := range map[string]string{
		conchimage.KindRootfs:      rootfsDesc.Digest.Encoded(),
		conchimage.KindSandbox:     sandboxDesc.Digest.Encoded(),
		conchimage.KindMemSnapshot: memDesc.Digest.Encoded(),
	} {
		name := fmt.Sprintf("localhost/conch/%s-component:%s", kind, component)
		if _, err := host.Client().ImageService().Get(ctx, name); !errdefs.IsNotFound(err) {
			t.Fatalf("component image %s was persisted or lookup failed: %v", name, err)
		}
	}
	before := templateSnapshotCount(t, ctx, host.Client().SnapshotService("erofs"))
	if before == 0 {
		t.Fatal("unpack created no snapshots")
	}

	if err := svc.RemoveTemplate(ctx, coldIndex.Digest.String()); err != nil {
		t.Fatalf("remove cold template: %v", err)
	}
	if _, err := host.Client().ImageService().Get(ctx, coldRef); !errdefs.IsNotFound(err) {
		t.Fatalf("cold Boot Index still exists or lookup failed: %v", err)
	}
	assertTemplateContentMissing(t, ctx, host.Client(), "cold Boot Index", coldIndex.Digest.String())
	assertTemplateContentPresent(t, ctx, host.Client(), "shared rootfs", rootfsDesc.Digest.String())
	assertTemplateContentPresent(t, ctx, host.Client(), "shared sandbox", sandboxDesc.Digest.String())
	if got := templateSnapshotCount(t, ctx, host.Client().SnapshotService("erofs")); got != before {
		t.Fatalf("shared snapshots changed from %d to %d", before, got)
	}
	assertNoTemplateSnapshotLeaseRefs(t, ctx, host.Client().LeasesService(), containerdclient.RuntimeLeaseID())

	if err := svc.RemoveTemplate(ctx, resumeIndex.Digest.String()); err != nil {
		t.Fatalf("remove resume template: %v", err)
	}
	if got := templateSnapshotCount(t, ctx, host.Client().SnapshotService("erofs")); got != 0 {
		t.Fatalf("snapshot count after last template removal = %d, want 0", got)
	}
	for name, dgst := range map[string]string{
		"resume Boot Index": resumeIndex.Digest.String(),
		"rootfs component":  rootfsDesc.Digest.String(),
		"sandbox component": sandboxDesc.Digest.String(),
		"memory component":  memDesc.Digest.String(),
	} {
		assertTemplateContentMissing(t, ctx, host.Client(), name, dgst)
	}
}

func writeTemplateCaptureRoot(t *testing.T, value string) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), value)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "payload"), []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

func templateSnapshotCount(t *testing.T, ctx context.Context, snapshotter snapshots.Snapshotter) int {
	t.Helper()
	count := 0
	if err := snapshotter.Walk(ctx, func(context.Context, snapshots.Info) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("walk snapshots: %v", err)
	}
	return count
}

func assertTemplateContentPresent(t *testing.T, ctx context.Context, client *containerdclient.Client, name, rawDigest string) {
	t.Helper()
	if _, err := client.ContentStore().Info(ctx, digest.Digest(rawDigest)); err != nil {
		t.Fatalf("%s content %s is missing: %v", name, rawDigest, err)
	}
}

func assertTemplateContentMissing(t *testing.T, ctx context.Context, client *containerdclient.Client, name, rawDigest string) {
	t.Helper()
	if _, err := client.ContentStore().Info(ctx, digest.Digest(rawDigest)); !errdefs.IsNotFound(err) {
		t.Fatalf("%s content %s still exists or lookup failed: %v", name, rawDigest, err)
	}
}

func assertNoTemplateOperationLeases(t *testing.T, ctx context.Context, manager leases.Manager) {
	t.Helper()
	items, err := manager.List(ctx)
	if err != nil {
		t.Fatalf("list leases: %v", err)
	}
	for _, item := range items {
		if item.Labels["io.conch.lease.kind"] == "operation" {
			t.Fatalf("operation lease %s remained after unpack", item.ID)
		}
	}
}

func assertNoTemplateSnapshotLeaseRefs(t *testing.T, ctx context.Context, manager leases.Manager, leaseID string) {
	t.Helper()
	resources, err := manager.ListResources(ctx, leases.Lease{ID: leaseID})
	if err != nil {
		t.Fatalf("list runtime lease resources: %v", err)
	}
	for _, resource := range resources {
		if resource.Type == "snapshots/erofs" {
			t.Fatalf("snapshot %s was attached to the runtime lease", resource.ID)
		}
	}
}

func newRuntimeImageHost(t *testing.T) *containerdhost.Host {
	t.Helper()
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		t.Skip("mkfs.erofs is required")
	}
	host, err := containerdhost.Start(context.Background(), containerdhost.Config{
		RootDir:  t.TempDir(),
		StateDir: t.TempDir(),
		Snapshot: containerdhost.SnapshotConfig{
			WorkDir: t.TempDir(),
		},
	})
	if err != nil {
		t.Skipf("embedded containerd host unavailable: %v", err)
	}
	t.Cleanup(func() {
		if err := host.Close(); err != nil {
			t.Errorf("close containerd host: %v", err)
		}
	})
	return host
}

func buildColdBootIndex(t *testing.T, host *containerdhost.Host, name string) string {
	t.Helper()
	ctx := containerdclient.NewNamespaceContext(context.Background())
	leaseCtx, done, err := host.Client().WithLease(ctx)
	if err != nil {
		t.Fatalf("create source boot index lease: %v", err)
	}
	t.Cleanup(func() { done(leaseCtx) })
	ctx = leaseCtx
	store := host.Client().ContentStore()
	rootfsDir := filepath.Join(t.TempDir(), "rootfs")
	sandboxDir := filepath.Join(t.TempDir(), "sandbox")
	for _, dir := range []string{rootfsDir, sandboxDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "payload"), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	rootfsDesc, err := conchimage.BuildNativeComponentInContent(
		ctx, store, []string{rootfsDir}, conchimage.KindRootfs,
	)
	if err != nil {
		t.Fatalf("build rootfs component: %v", err)
	}
	sandboxDesc, err := conchimage.BuildNativeComponentInContent(
		ctx, store, []string{sandboxDir}, conchimage.KindSandbox,
	)
	if err != nil {
		t.Fatalf("build sandbox component: %v", err)
	}
	indexDesc, err := conchimage.BuildBootIndexInContent(ctx, store, conchimage.BootIndexContentOptions{
		RootfsDescriptor:  rootfsDesc,
		SandboxDescriptor: sandboxDesc,
	})
	if err != nil {
		t.Fatalf("build cold boot index: %v", err)
	}
	return indexDesc.Digest.String()
}

func seedTemplate(
	t *testing.T,
	ctx context.Context,
	templates conchtemplate.Store,
	id string,
	bootMode conchtemplate.BootMode,
) {
	t.Helper()
	if _, err := templates.Create(ctx, conchtemplate.Entry{
		ID:       id,
		Origin:   conchtemplate.OriginImage,
		BootMode: bootMode,
	}); err != nil {
		t.Fatalf("CreateTemplate(%s) error = %v", id, err)
	}
}

func newTestStore(t *testing.T) *state.BoltStore {
	t.Helper()
	store, err := state.OpenBolt(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatalf("OpenBolt() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return store
}
