package conchruntime

import (
	"context"
	"errors"
	"testing"

	containerdclient "github.com/openeuler/Conch/internal/adapters/containerd/client"
	conchimage "github.com/openeuler/Conch/internal/image"
	"github.com/openeuler/Conch/internal/sandbox"
	conchtemplate "github.com/openeuler/Conch/internal/template"
)

func TestResumeTemplateReservesCapturedResourcesBeforeCreate(t *testing.T) {
	ctx := containerdclient.NewNamespaceContext(context.Background())
	host := newRuntimeImageHost(t)
	cold := buildColdBootIndex(t, host, "capacity-cold")
	published, err := conchimage.PublishCheckpointBootIndex(ctx, host.Client(), conchimage.PublishCheckpointBootIndexOptions{
		SourceBootIndexDigest: cold, MemRoot: t.TempDir(), VMMName: "cloud-hypervisor", MemorySizeMB: 512, CPUCount: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	const name = "capacity-resume"
	seedTemplate(t, ctx, host, name, published.BootIndexDigest, conchtemplate.BootModeResume)
	ops := &fakeSandboxOps{createResult: sandbox.CreateResult{BootIndexDigest: published.BootIndexDigest}}
	svc := New(ops, host.Client(), host.SandboxStore())
	svc.Templates = host.TemplateStore()
	svc.SetSandboxDefaults(SandboxDefaults{VMMName: "cloud-hypervisor", VCPUNum: 2, VCPUMax: 2, RamMB: 128})
	svc.Capacity, err = NewCapacity(CapacityLimits{MaxSandboxes: 2, MaxCPUs: 4, MaxMemoryMB: 1024})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.CreateSandbox(ctx, SandboxCreateOptions{TemplateName: name})
	if !errors.Is(err, sandbox.ErrResourceExhausted) || ops.createCalls != 0 {
		t.Fatalf("admission: err=%v createCalls=%d", err, ops.createCalls)
	}
	svc.Capacity, err = NewCapacity(CapacityLimits{MaxSandboxes: 2, MaxCPUs: 8, MaxMemoryMB: 1024})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.CreateSandbox(ctx, SandboxCreateOptions{TemplateName: name})
	if err != nil {
		t.Fatal(err)
	}
	if ops.req.VCPUNum != 8 || ops.req.VCPUMax < 8 || ops.req.RAMMB != 512 {
		t.Fatalf("runtime allocation=%+v", ops.req)
	}
	rec, err := svc.GetSandbox(ctx, result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if rec.VCPUNum != 8 || rec.RamMB != 512 {
		t.Fatalf("reported resources=%+v", rec)
	}
	if _, err := svc.CreateSandbox(ctx, SandboxCreateOptions{TemplateName: name}); !errors.Is(err, sandbox.ErrResourceExhausted) {
		t.Fatalf("second admission=%v", err)
	}
	if err := svc.RemoveSandbox(ctx, result.SandboxID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateSandbox(ctx, SandboxCreateOptions{TemplateName: name}); err != nil {
		t.Fatalf("capacity not released: %v", err)
	}
}
