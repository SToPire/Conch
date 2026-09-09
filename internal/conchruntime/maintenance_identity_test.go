package conchruntime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/openeuler/Conch/internal/sandbox"
	conchtemplate "github.com/openeuler/Conch/internal/template"
)

func TestMaintenanceObservationCannotDeleteReplacement(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "expiry", true: "cleanup"}[pending], func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			templateID := digest.FromString("maintenance-identity-template").String()
			ops := &fakeSandboxOps{createResult: sandbox.CreateResult{BootIndexDigest: templateID}}
			svc := New(ops, nil, store)
			setFakeTemplate(svc, testTemplateName, templateID, conchtemplate.BootModeCold)
			var err error
			svc.Capacity, err = NewCapacity(CapacityLimits{MaxSandboxes: 1, MaxCPUs: 2, MaxMemoryMB: 512})
			if err != nil {
				t.Fatal(err)
			}
			opts := SandboxCreateOptions{SandboxID: "maintenance-id", TemplateName: testTemplateName, VCPUNum: 2, VCPUMax: 2, RamMB: 512}
			if _, err := svc.CreateSandbox(ctx, opts); err != nil {
				t.Fatal(err)
			}
			old, err := svc.GetSandbox(ctx, opts.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			old.CleanupPending = pending
			old.E2B = !pending
			old.ExpiresAt = now.Add(-time.Second).UnixNano()
			if _, err := store.Update(ctx, old); err != nil {
				t.Fatal(err)
			}
			// The worker retains this observation while explicit deletion and
			// native creation replace the runtime behind the same public ID.
			if err := svc.RemoveSandbox(ctx, old.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.CreateSandbox(ctx, opts); err != nil {
				t.Fatal(err)
			}
			replacement, err := svc.GetSandbox(ctx, opts.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			if err := svc.ReconcileSandbox(ctx, old.ID, old.RuntimeID, now); err != nil {
				t.Fatal(err)
			}
			after, err := svc.GetSandbox(ctx, replacement.ID)
			if err != nil || !reflect.DeepEqual(after, replacement) {
				t.Fatalf("stale maintenance deleted/changed replacement: %+v %v", after, err)
			}
			if err := svc.Capacity.reserve("extra", 2, 512); !errors.Is(err, sandbox.ErrResourceExhausted) {
				t.Fatalf("stale maintenance refunded capacity: %v", err)
			}
			// Even a matching identity must still pass the current eligibility
			// check; an observation is not authorization to delete a live VM.
			replacement.E2B = true
			replacement.ExpiresAt = now.Add(time.Hour).UnixNano()
			if _, err := store.Update(ctx, replacement); err != nil {
				t.Fatal(err)
			}
			if err := svc.ReconcileSandbox(ctx, replacement.ID, replacement.RuntimeID, now); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.GetSandbox(ctx, replacement.ID); err != nil {
				t.Fatal("maintenance ignored current deadline")
			}
			replacement.CleanupPending = true
			if _, err := store.Update(ctx, replacement); err != nil {
				t.Fatal(err)
			}
			if err := svc.ReconcileSandbox(ctx, replacement.ID, replacement.RuntimeID, now); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.GetSandbox(ctx, replacement.ID); !errors.Is(err, sandbox.ErrNotFound) {
				t.Fatalf("eligible cleanup did not run: %v", err)
			}
			if err := svc.Capacity.reserve("extra", 2, 512); err != nil {
				t.Fatalf("eligible cleanup retained capacity: %v", err)
			}
		})
	}
}
