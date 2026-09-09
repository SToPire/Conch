package conchruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/openeuler/Conch/internal/envd"
	"github.com/openeuler/Conch/internal/sandbox"
	"github.com/openeuler/Conch/internal/sandboxproxy"
	conchtemplate "github.com/openeuler/Conch/internal/template"
)

func TestManagerCreateErrorPreservesUnconfirmedBootstrapOwner(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unconfirmed", true: "released"}[confirmed], func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			root := digest.FromString("bootstrap-template").String()
			cause := errors.New("conch-init readiness failed")
			ops := &fakeSandboxOps{createResult: sandbox.CreateResult{VMMPID: 12345, BootIndexDigest: root}, createErr: &sandbox.CleanupError{Err: cause, ResourcesReleased: confirmed}}
			svc := New(ops, nil, store)
			setFakeTemplate(svc, testTemplateName, root, conchtemplate.BootModeCold)
			svc.Envd = envd.NewClient()
			svc.ProxyRoutes = sandboxproxy.NewRegistry()
			var err error
			svc.Capacity, err = NewCapacity(CapacityLimits{MaxSandboxes: 1, MaxCPUs: 2, MaxMemoryMB: 512})
			if err != nil {
				t.Fatal(err)
			}
			const sandboxID = "a541d234-baf9-4d67-a3ca-5696e49e39db"
			_, err = svc.CreateSandbox(ctx, SandboxCreateOptions{SandboxID: sandboxID, TemplateName: testTemplateName, VCPUNum: 2, VCPUMax: 2, RamMB: 512, E2B: true})
			if !errors.Is(err, cause) {
				t.Fatalf("bootstrap cause lost: %v", err)
			}
			if _, ok := svc.ProxyRoutes.CurrentGeneration(sandboxID); ok {
				t.Fatal("failed create left a proxy generation")
			}
			record, err := store.Get(ctx, sandboxID)
			if confirmed {
				if !errors.Is(err, sandbox.ErrNotFound) {
					t.Fatalf("released bootstrap retained record: %v", err)
				}
			} else {
				if err != nil || record.State != sandbox.StateUnknown || !record.CleanupPending || record.ResourcesReleased || record.VMMPID != 12345 || record.CheckpointHeadTemplateID != root {
					t.Fatalf("lost bootstrap owner: %+v %v", record, err)
				}
				if err := svc.Capacity.reserve("next", 2, 512); !errors.Is(err, sandbox.ErrResourceExhausted) {
					t.Fatalf("unconfirmed VMM was refunded: %v", err)
				}
				// The production controller later confirms release on deletion.
				if err := svc.RemoveSandbox(ctx, sandboxID); err != nil {
					t.Fatal(err)
				}
			}
			if err := svc.Capacity.reserve("next", 2, 512); err != nil {
				t.Fatalf("confirmed bootstrap cleanup stranded capacity: %v", err)
			}
		})
	}
}
