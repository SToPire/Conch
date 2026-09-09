package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/leases"

	"github.com/openeuler/Conch/internal/config"
	"github.com/openeuler/Conch/internal/netstack"
	"github.com/openeuler/Conch/internal/sandboxproxy"
	"github.com/openeuler/Conch/internal/vmm"
	"github.com/openeuler/Conch/internal/volume"
)

func TestDurationOrDefault(t *testing.T) {
	const fallback = 10 * time.Millisecond
	if got := durationOrDefault(0, fallback); got != fallback {
		t.Fatalf("durationOrDefault(0) = %s, want %s", got, fallback)
	}
	if got := durationOrDefault(time.Second, fallback); got != time.Second {
		t.Fatalf("durationOrDefault(1s) = %s, want 1s", got)
	}
}

func TestReserveSandboxEntryDoesNotBlockDifferentSandbox(t *testing.T) {
	m := &Manager{}

	key, entry, err := m.reserveSandboxEntry("sandbox-a", "runtime-a")
	if err != nil {
		t.Fatalf("reserveSandboxEntry() error = %v", err)
	}
	defer m.sandboxes.CompareAndDelete(key, entry)
	defer entry.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		otherKey, otherEntry, err := m.reserveSandboxEntry("sandbox-b", "runtime-b")
		if err == nil {
			m.sandboxes.CompareAndDelete(otherKey, otherEntry)
			otherEntry.mu.Unlock()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reserveSandboxEntry() for different sandbox error = %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("reserveSandboxEntry() for different sandbox blocked behind another sandbox entry")
	}
}

func TestHandleSandboxExitCleansSuspendedSandbox(t *testing.T) {
	boot := &recordingBootPreparer{}
	m := &Manager{
		boot:         boot,
		cidAllocator: NewCIDAllocator(),
	}
	sbx := &Sandbox{
		cleanup:   NewCleanup(),
		sandboxID: "sandbox-a",
	}
	entry := &sandboxEntry{state: sandboxSuspended, sbx: sbx}
	mapKey := "sandbox-a"
	m.sandboxes.Store(mapKey, entry)

	m.handleSandboxExit(mapKey, entry, "sandbox-a", sbx)

	if _, ok := m.sandboxes.Load(mapKey); ok {
		t.Fatal("suspended sandbox entry remains after VMM exit")
	}
	if len(boot.released) != 1 || boot.released[0].SandboxID != "sandbox-a" {
		t.Fatalf("released boot layouts = %#v", boot.released)
	}
}

func TestHandleSandboxExitCallsUnexpectedHandlerOnce(t *testing.T) {
	m, entry, sbx := newExitTestSandbox(func(context.Context) error { return nil })
	type exitResult struct {
		id  string
		err error
	}
	called := make(chan exitResult, 2)
	m.UnexpectedExitHandler = func(id, _ string, err error) { called <- exitResult{id: id, err: err} }
	m.handleSandboxExit("sandbox-a", entry, "sandbox-a", sbx)
	m.handleSandboxExit("sandbox-a", entry, "sandbox-a", sbx)
	select {
	case result := <-called:
		if result.id != "sandbox-a" || result.err != nil {
			t.Fatalf("handler result = %#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected-exit handler was not called")
	}
	select {
	case result := <-called:
		t.Fatalf("handler called more than once for %#v", result)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDelayedExitCallbackRetainsOriginalRuntimeIdentity(t *testing.T) {
	m := &Manager{boot: &recordingBootPreparer{}, cidAllocator: NewCIDAllocator()}
	key, old, err := m.reserveSandboxEntry("sandbox-a", "runtime-old")
	if err != nil {
		t.Fatal(err)
	}
	sbx := &Sandbox{cleanup: NewCleanup(), sandboxID: "sandbox-a"}
	old.sbx = sbx
	old.mu.Unlock()
	callbackEntered := make(chan struct{})
	continueCallback := make(chan struct{})
	type notification struct{ sandboxID, runtimeID string }
	notifications := make(chan notification, 1)
	m.UnexpectedExitHandler = func(sandboxID, runtimeID string, err error) {
		if err != nil {
			t.Errorf("cleanup error: %v", err)
		}
		close(callbackEntered)
		select {
		case <-continueCallback:
		case <-t.Context().Done():
			return
		}
		notifications <- notification{sandboxID, runtimeID}
	}
	go m.handleSandboxExit(key, old, "sandbox-a", sbx)
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("old runtime did not reach exit notification")
	}
	// The old entry has been removed, while its callback is still waiting for
	// the runtime service. Reuse the public ID with a distinct allocation ID.
	_, replacement, err := m.reserveSandboxEntry("sandbox-a", "runtime-new")
	if err != nil {
		t.Fatal(err)
	}
	replacement.sbx = &Sandbox{cleanup: NewCleanup(), sandboxID: "sandbox-a"}
	replacement.mu.Unlock()
	close(continueCallback)
	select {
	case got := <-notifications:
		if got.sandboxID != "sandbox-a" || got.runtimeID != "runtime-old" {
			t.Fatalf("old exit changed identity after ID reuse: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("old runtime callback did not finish")
	}
	if !m.isCurrentSandboxEntry(key, replacement) || replacement.runtimeID != "runtime-new" {
		t.Fatal("old callback affected replacement runtime ownership")
	}
}

func TestRuntimeReleaseRevokesRouteBeforeResourceCleanup(t *testing.T) {
	for _, operation := range []string{"unexpected exit", "delete"} {
		t.Run(operation, func(t *testing.T) {
			routes := sandboxproxy.NewRegistry()
			defer routes.Close()
			generation := routes.Begin("sandbox-a")
			if err := routes.Publish("sandbox-a", generation, "127.0.0.1"); err != nil {
				t.Fatal(err)
			}
			bootstrap, ok := routes.GenerationContext("sandbox-a", generation)
			if !ok {
				t.Fatal("missing generation context")
			}
			cleanupRan := false
			m, entry, sbx := newExitTestSandbox(func(context.Context) error {
				// A real network slot is released by this same Cleanup stack.
				// Once any resource cleanup runs, neither a new proxy lookup
				// nor the old bootstrap may still target the released guest IP.
				if _, ok := routes.Lookup("sandbox-a"); ok || bootstrap.Err() == nil {
					t.Error("resource cleanup began while old generation remained usable")
				}
				cleanupRan = true
				return nil
			})
			callbackCount := 0
			m.BeforeRuntimeRelease = func(id string) {
				callbackCount++
				if current, ok := routes.CurrentGeneration(id); ok {
					routes.Remove(id, current)
				}
			}
			if operation == "delete" {
				if err := m.Delete(DeleteRequest{SandboxID: "sandbox-a"}); err != nil {
					t.Fatal(err)
				}
			} else {
				m.handleSandboxExit("sandbox-a", entry, "sandbox-a", sbx)
			}
			// A delayed waiter for the removed runtime must not revoke the
			// next runtime's route or call resource cleanup a second time.
			m.handleSandboxExit("sandbox-a", entry, "sandbox-a", sbx)
			if !cleanupRan || callbackCount != 1 {
				t.Fatalf("cleanup=%v callback count=%d", cleanupRan, callbackCount)
			}
		})
	}
}

func TestStaleRuntimeExitDoesNotRevokeReplacementRoute(t *testing.T) {
	m, entry, sbx := newExitTestSandbox(func(context.Context) error {
		t.Error("stale runtime ran resource cleanup")
		return nil
	})
	routes := sandboxproxy.NewRegistry()
	defer routes.Close()
	generation := routes.Begin("sandbox-a")
	if err := routes.Publish("sandbox-a", generation, "127.0.0.2"); err != nil {
		t.Fatal(err)
	}
	m.BeforeRuntimeRelease = func(string) {
		t.Error("stale runtime invoked route-revocation callback")
	}
	replacement := &sandboxEntry{state: sandboxReady, sbx: &Sandbox{cleanup: NewCleanup()}}
	m.sandboxes.Store("sandbox-a", replacement)
	m.handleSandboxExit("sandbox-a", entry, "sandbox-a", sbx)
	if route, ok := routes.Lookup("sandbox-a"); !ok || route.Generation != generation {
		t.Fatal("stale exit removed replacement route")
	}
}

func TestCreatePropagatesCallerCancellationToBootPreparation(t *testing.T) {
	oldWorkDir := config.WorkDir
	workDir, err := os.MkdirTemp("/tmp", "csb-")
	if err != nil {
		t.Fatal(err)
	}
	config.WorkDir = workDir
	t.Cleanup(func() {
		config.WorkDir = oldWorkDir
		_ = os.RemoveAll(workDir)
	})
	boot := &blockingBootPreparer{entered: make(chan struct{})}
	m := &Manager{
		boot:           boot,
		cidAllocator:   NewCIDAllocator(),
		requestTimeout: time.Hour,
		vmmBinaries:    map[string]string{"cloud-hypervisor": "/unused"},
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := leases.WithLease(baseCtx, "operation-lease")
	done := make(chan error, 1)
	go func() {
		_, err := m.Create(ctx, CreateRequest{
			TemplateID: "sha256:template", VMMName: "cloud-hypervisor", SandboxID: "sandbox-a",
			VCPUNum: 1, VCPUMax: 1, RAMMB: 128, AgentToken: "token",
		})
		done <- err
	}()
	select {
	case <-boot.entered:
	case err := <-done:
		t.Fatalf("Create() returned before boot preparation: %v", err)
	case <-time.After(time.Second):
		t.Fatal("Create() did not reach boot preparation")
	}
	if boot.leaseID != "operation-lease" {
		t.Fatalf("boot preparation lease = %q, want operation-lease", boot.leaseID)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Create() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Create() did not observe caller cancellation")
	}
}

func TestCreateTreatsBootCleanupFailureAsBestEffort(t *testing.T) {
	oldWorkDir := config.WorkDir
	config.WorkDir = t.TempDir()
	t.Cleanup(func() { config.WorkDir = oldWorkDir })
	wantErr := errors.New("release boot failed")
	boot := &recordingBootPreparer{
		prepared:   PreparedBoot{Runtime: BootRuntime{Resume: true}},
		releaseErr: wantErr,
	}
	m := &Manager{
		boot:           boot,
		cidAllocator:   NewCIDAllocator(),
		requestTimeout: time.Second,
		vmmBinaries:    map[string]string{"cloud-hypervisor": "/unused"},
	}
	_, err := m.Create(context.Background(), CreateRequest{
		TemplateID: "sha256:template", VMMName: "cloud-hypervisor", SandboxID: "sandbox-a",
		VCPUNum: 1, VCPUMax: 1, RAMMB: 128, AgentToken: "token", VolumeMounts: []volume.Mount{{}},
	})
	if err == nil {
		t.Fatal("Create() error = nil")
	}
	if errors.Is(err, wantErr) {
		t.Fatalf("Create() error = %v, want primary create error only", err)
	}
	if len(boot.released) != 1 || boot.released[0].SandboxID != "sandbox-a" {
		t.Fatalf("released boot layouts = %#v", boot.released)
	}
}

func TestCreateUsesLiveBoundedContextForBootCleanupAfterCancellation(t *testing.T) {
	oldWorkDir := config.WorkDir
	config.WorkDir = t.TempDir()
	t.Cleanup(func() { config.WorkDir = oldWorkDir })

	ctx, cancel := context.WithCancel(context.Background())
	boot := &recordingBootPreparer{
		prepared:    PreparedBoot{Runtime: BootRuntime{Resume: true}},
		prepareHook: cancel,
	}
	m := &Manager{
		boot:           boot,
		cidAllocator:   NewCIDAllocator(),
		requestTimeout: time.Second,
		vmmBinaries:    map[string]string{"cloud-hypervisor": "/unused"},
	}

	_, err := m.Create(ctx, CreateRequest{
		TemplateID: "sha256:template", VMMName: "cloud-hypervisor", SandboxID: "sandbox-a",
		VCPUNum: 1, VCPUMax: 1, RAMMB: 128, AgentToken: "token",
	})
	if err == nil {
		t.Fatal("Create() error = nil")
	}
	if boot.releaseContextErr != nil {
		t.Fatalf("boot cleanup context error = %v, want nil", boot.releaseContextErr)
	}
	if !boot.releaseHasDeadline {
		t.Fatal("boot cleanup context has no deadline")
	}
}

func TestDeleteRemovesEntryAfterCleanupFailure(t *testing.T) {
	wantErr := errors.New("runtime cleanup failed")
	boot := &recordingBootPreparer{}
	m := &Manager{boot: boot, cidAllocator: NewCIDAllocator()}
	cleanup := NewCleanup()
	cleanup.Add(func(context.Context) error { return wantErr })
	sbx := &Sandbox{cleanup: cleanup, sandboxID: "sandbox-a"}
	entry := &sandboxEntry{state: sandboxReady, sbx: sbx}
	m.sandboxes.Store("sandbox-a", entry)

	err := m.Delete(DeleteRequest{SandboxID: "sandbox-a"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Delete() error = %v, want %v", err, wantErr)
	}
	var cleanupErr *CleanupError
	if !errors.As(err, &cleanupErr) || !cleanupErr.ResourcesReleased {
		t.Fatalf("no-VM cleanup failure lost confirmed release: %v", err)
	}
	if _, ok := m.sandboxes.Load("sandbox-a"); ok {
		t.Fatal("sandbox entry remains after one-shot cleanup failure")
	}
}

func TestUnexpectedExitRemovesEntryAndReportsCleanupError(t *testing.T) {
	wantErr := errors.New("release boot failed")
	boot := &recordingBootPreparer{releaseErrors: []error{wantErr}}
	m := &Manager{boot: boot, cidAllocator: NewCIDAllocator()}
	sbx := &Sandbox{cleanup: NewCleanup(), sandboxID: "sandbox-a"}
	entry := &sandboxEntry{state: sandboxReady, sbx: sbx}
	m.sandboxes.Store("sandbox-a", entry)
	called := make(chan error, 1)
	m.UnexpectedExitHandler = func(_, _ string, err error) { called <- err }

	m.handleSandboxExit("sandbox-a", entry, "sandbox-a", sbx)
	if _, ok := m.sandboxes.Load("sandbox-a"); ok {
		t.Fatal("sandbox entry remains after one-shot unexpected-exit cleanup")
	}
	select {
	case err := <-called:
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected-exit cleanup error = %v, want %v", err, wantErr)
		}
		var cleanupErr *CleanupError
		if !errors.As(err, &cleanupErr) || !cleanupErr.ResourcesReleased {
			t.Fatalf("boot cleanup failure lost confirmed VM release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected-exit handler was not called")
	}
}

func TestCleanupErrorPreservesAllCausesAndReleaseEvidence(t *testing.T) {
	first := errors.New("socket cleanup failed")
	second := errors.New("boot release failed")
	m, _, sbx := newExitTestSandbox(func(context.Context) error { return first })
	m.boot = &recordingBootPreparer{releaseErr: second}
	// This concrete Process has never been started; no constructor or fake
	// process injection is needed to establish that it owns no VM allocation.
	sbx.process = &vmm.Process{}
	if !sbx.ComputeResourcesReleased() {
		t.Fatal("unstarted process incorrectly retains CPU/RAM allocation")
	}
	err := m.Delete(DeleteRequest{SandboxID: "sandbox-a"})
	var cleanupErr *CleanupError
	if !errors.As(err, &cleanupErr) || !cleanupErr.ResourcesReleased {
		t.Fatalf("unstarted process cleanup = %v", err)
	}
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("typed cleanup error lost cause: %v", err)
	}
	if !strings.Contains(err.Error(), first.Error()) || !strings.Contains(err.Error(), second.Error()) {
		t.Fatalf("typed cleanup error lost diagnostics: %v", err)
	}
}

func TestEntryCleanupConfirmationDoesNotRepeatResourceRelease(t *testing.T) {
	wantErr := errors.New("host cleanup incomplete")
	runtimeReleases := 0
	m, entry, _ := newExitTestSandbox(func(context.Context) error {
		runtimeReleases++
		return wantErr
	})
	boot := &recordingBootPreparer{}
	m.boot = boot
	routeRevocations := 0
	m.BeforeRuntimeRelease = func(string) { routeRevocations++ }
	// Run the same entry-owned teardown used by unexpected exit, then leave
	// the entry available for deletion's confirmation pass. No live VMM is
	// needed to prove that host teardown is not executed a second time.
	entry.mu.Lock()
	first := m.cleanupSandboxEntry(t.Context(), entry, "sandbox-a")
	entry.mu.Unlock()
	if !errors.Is(first, wantErr) {
		t.Fatalf("first cleanup error = %v", first)
	}
	// A prior inconclusive observation may become confirmed before CREATE's
	// deferred ownership decision. Its cached result must agree with the
	// confirmation returned to the runtime service.
	entry.mu.Lock()
	entry.cleanupErr = &CleanupError{Err: wantErr, ResourcesReleased: false}
	confirmed := m.cleanupSandboxEntry(t.Context(), entry, "sandbox-a")
	agrees := cleanupConfirmsResourceRelease(confirmed) && cleanupConfirmsResourceRelease(entry.cleanupErr)
	entry.mu.Unlock()
	if !agrees {
		t.Fatal("returned and cached cleanup confirmation disagree")
	}
	err := m.Delete(DeleteRequest{SandboxID: "sandbox-a"})
	var cleanupErr *CleanupError
	if !errors.Is(err, wantErr) || !errors.As(err, &cleanupErr) || !cleanupErr.ResourcesReleased {
		t.Fatalf("confirmation lost release evidence or diagnostic: %v", err)
	}
	if _, ok := m.sandboxes.Load("sandbox-a"); ok {
		t.Fatal("confirmed entry retained after deletion")
	}
	if err := m.Delete(DeleteRequest{SandboxID: "sandbox-a"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("repeated delete = %v", err)
	}
	if runtimeReleases != 1 || len(boot.released) != 1 || routeRevocations != 1 {
		t.Fatalf("cleanup repeated: runtime=%d boot=%d routes=%d", runtimeReleases, len(boot.released), routeRevocations)
	}
}

func TestCleanupCreateFailureTransfersRuntimeAndReturnsPartialMetadata(t *testing.T) {
	for _, cleanupFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("cleanup-error-%v", cleanupFails), func(t *testing.T) {
			var wantErr error
			if cleanupFails {
				wantErr = errors.New("socket teardown diagnostic")
			}
			routes := sandboxproxy.NewRegistry()
			defer routes.Close()
			generation := routes.Begin("sandbox-a")
			bootstrap, _ := routes.GenerationContext("sandbox-a", generation)
			released := 0
			m, entry, sbx := newExitTestSandbox(func(context.Context) error {
				if bootstrap.Err() == nil {
					t.Error("create failure released guest resources before bootstrap invalidation")
				}
				released++
				return wantErr
			})
			// Use concrete production objects with no started VMM. The result
			// still carries every acquired host-resource reference needed by
			// the runtime when handling a partial create failure.
			sbx.process = &vmm.Process{SandboxId: "sandbox-a", VmmSocketPath: "/vm-api.sock", VsockSocketPath: "/vsock.sock"}
			sbx.slot = &netstack.Slot{}
			cid, err := m.AllocateUniqueCID("sandbox-a")
			if err != nil {
				t.Fatal(err)
			}
			m.BeforeRuntimeRelease = func(id string) { routes.Remove(id, generation) }
			req := CreateRequest{SandboxID: "sandbox-a", AgentToken: "token"}
			refs := []SnapshotRef{{Key: "memory-active", Role: "memory", Snapshotter: "erofs"}}
			boot := PreparedBoot{
				Spec: BootSpec{PmemPaths: []string{"/rootfs.erofs"}}, RuntimeSnapshots: refs,
				Runtime: BootRuntime{BootIndexDigest: "sha256:captured", RootfsKey: "rootfs-active", MemKey: "memory-active", RootfsMount: "/rootfs", MemMount: "/memory", MemSize: 512},
			}
			ids := createRuntimeIDs{key: "sandbox-a", vsockCID: cid, vsockSocketPath: "/vsock.sock"}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			entry.mu.Lock()
			partial, cleanupErr := m.cleanupCreateFailure(ctx, entry, req, sbx, boot, ids, nil)
			entry.mu.Unlock()
			if !errors.Is(cleanupErr, wantErr) {
				t.Fatalf("cleanup error=%v want=%v", cleanupErr, wantErr)
			}
			if cleanupFails {
				var confirmed *CleanupError
				if !errors.As(cleanupErr, &confirmed) || !confirmed.ResourcesReleased {
					t.Fatalf("confirmed cleanup failure lost termination evidence: %v", cleanupErr)
				}
			}
			if partial.SandboxID != req.SandboxID || partial.AgentToken != req.AgentToken || partial.BootIndexDigest != "sha256:captured" || partial.VMMSocketPath != "/vm-api.sock" || partial.RootfsKey != "rootfs-active" || partial.MemKey != "memory-active" || partial.VsockCID != cid || !reflect.DeepEqual(partial.RuntimeSnapshots, refs) {
				t.Fatalf("partial create metadata incomplete: %+v", partial)
			}
			if !entry.cleanupAttempted || entry.sbx != sbx || entry.state != sandboxCleanupPending {
				t.Fatalf("runtime ownership was not transferred: %+v", entry)
			}
			bootPreparer := m.boot.(*recordingBootPreparer)
			if bootPreparer.releaseContextErr != nil || !bootPreparer.releaseHasDeadline || len(bootPreparer.released) != 1 || m.cidAllocator.GetActiveCount() != 0 || released != 1 {
				t.Fatalf("create teardown incomplete or repeated: boot=%+v cid=%d runtime=%d", bootPreparer, m.cidAllocator.GetActiveCount(), released)
			}
			// Pending-create entries use the same confirmation path as normal
			// deletion. Even with cached errors, resources are released once.
			if err := m.Delete(DeleteRequest{SandboxID: req.SandboxID}); !errors.Is(err, wantErr) {
				t.Fatalf("delete pending create: %v", err)
			}
			if _, ok := m.sandboxes.Load(req.SandboxID); ok || released != 1 || len(bootPreparer.released) != 1 {
				t.Fatal("confirmed create failure kept its entry or repeated cleanup")
			}
		})
	}
}

func TestWaitForSandboxExitCleansSandboxOnVirtiofsExit(t *testing.T) {
	cleanupDone := make(chan struct{})
	m, entry, sbx := newExitTestSandbox(func(context.Context) error {
		close(cleanupDone)
		return nil
	})

	vmmExit := make(chan struct{})
	virtiofsExit := make(chan struct{})
	go func() {
		select {
		case <-vmmExit:
		case <-virtiofsExit:
		}
		m.handleSandboxExit("sandbox-a", entry, "sandbox-a", sbx)
	}()
	close(virtiofsExit)

	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("sandbox cleanup was not triggered after virtiofsd exit")
	}
	entry.mu.Lock()
	entry.mu.Unlock()
	if _, ok := m.sandboxes.Load("sandbox-a"); ok {
		t.Fatal("sandbox entry remains after virtiofsd exit")
	}
}

func TestWaitForSandboxExitDoesNotDuplicateDeleteCleanup(t *testing.T) {
	cleanupCalls := 0
	cleanupStarted := make(chan struct{})
	continueCleanup := make(chan struct{})
	m, entry, sbx := newExitTestSandbox(func(context.Context) error {
		cleanupCalls++
		close(cleanupStarted)
		<-continueCleanup
		return nil
	})

	vmmExit := make(chan struct{})
	virtiofsExit := make(chan struct{})
	go func() {
		select {
		case <-vmmExit:
		case <-virtiofsExit:
		}
		m.handleSandboxExit("sandbox-a", entry, "sandbox-a", sbx)
	}()
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- m.Delete(DeleteRequest{SandboxID: "sandbox-a"})
	}()

	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("Delete did not start sandbox cleanup")
	}
	close(virtiofsExit)
	close(continueCleanup)

	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Delete blocked after virtiofsd exit")
	}
	if cleanupCalls != 1 {
		t.Fatalf("sandbox cleanup calls = %d, want 1", cleanupCalls)
	}
	if _, ok := m.sandboxes.Load("sandbox-a"); ok {
		t.Fatal("sandbox entry remains after Delete")
	}
}

func newExitTestSandbox(cleanup func(context.Context) error) (*Manager, *sandboxEntry, *Sandbox) {
	m := &Manager{boot: &recordingBootPreparer{}, cidAllocator: NewCIDAllocator()}
	sbx := &Sandbox{cleanup: NewCleanup(), sandboxID: "sandbox-a"}
	sbx.cleanup.Add(cleanup)
	entry := &sandboxEntry{state: sandboxReady, sbx: sbx}
	m.sandboxes.Store("sandbox-a", entry)
	return m, entry, sbx
}

func TestCheckpointCapturesRunningAndSuspendedSandbox(t *testing.T) {
	tests := []struct {
		name            string
		initialState    sandboxLifecycleState
		wantPauseBefore bool
	}{
		{name: "running", initialState: sandboxReady, wantPauseBefore: true},
		{name: "suspended", initialState: sandboxSuspended, wantPauseBefore: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := CapturedBootComponents{
				MemRootPath:  "/capture/mem",
				VMMName:      "cloud-hypervisor",
				MemorySizeMB: 512,
			}
			capture := &recordingCheckpointCapture{result: want}
			m, entry, sbx := checkpointTestManager(tt.initialState, capture)

			got, err := m.Checkpoint(CheckpointRequest{SandboxID: "sandbox-a"})
			if err != nil {
				t.Fatalf("Checkpoint() error = %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Checkpoint() result = %#v, want %#v", got, want)
			}
			if entry.state != tt.initialState {
				t.Fatalf("entry state after checkpoint = %s, want %s", entry.state, tt.initialState)
			}
			if len(capture.requests) != 1 {
				t.Fatalf("capture requests = %d, want 1", len(capture.requests))
			}
			if capture.requests[0].Source != sbx {
				t.Fatalf("capture source = %T %p, want sandbox %p", capture.requests[0].Source, capture.requests[0].Source, sbx)
			}
			if capture.requests[0].PauseBefore != tt.wantPauseBefore {
				t.Fatalf("PauseBefore = %v, want %v", capture.requests[0].PauseBefore, tt.wantPauseBefore)
			}
		})
	}
}

func TestCheckpointCaptureErrorRestoresPreviousLifecycleState(t *testing.T) {
	errCapture := errors.New("capture failed")
	tests := []struct {
		name         string
		initialState sandboxLifecycleState
	}{
		{name: "running", initialState: sandboxReady},
		{name: "suspended", initialState: sandboxSuspended},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capture := &recordingCheckpointCapture{err: errCapture}
			m, entry, _ := checkpointTestManager(tt.initialState, capture)

			_, err := m.Checkpoint(CheckpointRequest{SandboxID: "sandbox-a"})
			if !errors.Is(err, errCapture) {
				t.Fatalf("Checkpoint() error = %v, want errors.Is(capture error)", err)
			}
			if entry.state != tt.initialState {
				t.Fatalf("entry state after capture error = %s, want %s", entry.state, tt.initialState)
			}
		})
	}
}

func TestCheckpointResumeFailureLeavesSandboxSuspended(t *testing.T) {
	errResume := errors.New("resume failed")
	capture := &recordingCheckpointCapture{err: errors.Join(ErrCheckpointResume, errResume)}
	m, entry, _ := checkpointTestManager(sandboxReady, capture)

	_, err := m.Checkpoint(CheckpointRequest{SandboxID: "sandbox-a"})
	if !errors.Is(err, ErrCheckpointResume) || !errors.Is(err, errResume) {
		t.Fatalf("Checkpoint() error = %v, want joined resume failure", err)
	}
	if entry.state != sandboxSuspended {
		t.Fatalf("entry state after resume failure = %s, want %s", entry.state, sandboxSuspended)
	}
}

type recordingCheckpointCapture struct {
	requests []RuntimeCaptureRequest
	result   CapturedBootComponents
	err      error
}

func (r *recordingCheckpointCapture) Capture(_ context.Context, req RuntimeCaptureRequest) (CapturedBootComponents, error) {
	r.requests = append(r.requests, req)
	return r.result, r.err
}

func checkpointTestManager(initialState sandboxLifecycleState, capture CheckpointCapture) (*Manager, *sandboxEntry, *Sandbox) {
	m := &Manager{checkpointCapture: capture, requestTimeout: time.Second}
	sbx := &Sandbox{
		sandboxID: "sandbox-a",
	}
	entry := &sandboxEntry{state: initialState, sbx: sbx}
	m.sandboxes.Store("sandbox-a", entry)
	return m, entry, sbx
}

type recordingBootPreparer struct {
	released           []ReleaseBootRequest
	releaseErr         error
	releaseErrors      []error
	prepared           PreparedBoot
	prepareHook        func()
	releaseContextErr  error
	releaseHasDeadline bool
}

func (r *recordingBootPreparer) Prepare(context.Context, PrepareBootRequest) (PreparedBoot, error) {
	if r.prepareHook != nil {
		r.prepareHook()
	}
	return r.prepared, nil
}

func (r *recordingBootPreparer) Release(ctx context.Context, req ReleaseBootRequest) error {
	r.released = append(r.released, req)
	r.releaseContextErr = ctx.Err()
	_, r.releaseHasDeadline = ctx.Deadline()
	if len(r.releaseErrors) > 0 {
		err := r.releaseErrors[0]
		r.releaseErrors = r.releaseErrors[1:]
		return err
	}
	return r.releaseErr
}

type blockingBootPreparer struct {
	entered chan struct{}
	leaseID string
}

func (b *blockingBootPreparer) Prepare(ctx context.Context, _ PrepareBootRequest) (PreparedBoot, error) {
	b.leaseID, _ = leases.FromContext(ctx)
	close(b.entered)
	<-ctx.Done()
	return PreparedBoot{}, ctx.Err()
}

func (b *blockingBootPreparer) Release(context.Context, ReleaseBootRequest) error { return nil }

func TestDeleteMissingSandboxReturnsNotFoundWithoutReleasingBootLayout(t *testing.T) {
	boot := &recordingBootPreparer{}
	m := &Manager{boot: boot}

	if err := m.Delete(DeleteRequest{SandboxID: "sandbox-a"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete() error = %v, want sandbox not found", err)
	}
	if len(boot.released) != 0 {
		t.Fatalf("released boot layouts = %#v, want none", boot.released)
	}
}

func TestCleanupStaleBootResourcesReleasesBootLayout(t *testing.T) {
	boot := &recordingBootPreparer{}
	m := &Manager{boot: boot}

	if err := m.cleanupStaleBootResources(context.Background(), []string{"sandbox-a"}); err != nil {
		t.Fatalf("cleanupStaleBootResources() error = %v", err)
	}
	if len(boot.released) != 1 || boot.released[0].SandboxID != "sandbox-a" {
		t.Fatalf("released boot layouts = %#v", boot.released)
	}
}

func TestCleanupStaleBootResourcesReturnsBootReleaseError(t *testing.T) {
	wantErr := errors.New("release failed")
	boot := &recordingBootPreparer{releaseErr: wantErr}
	m := &Manager{boot: boot}

	if err := m.cleanupStaleBootResources(context.Background(), []string{"sandbox-a"}); !errors.Is(err, wantErr) {
		t.Fatalf("cleanupStaleBootResources() error = %v, want %v", err, wantErr)
	}
}

func TestReserveSandboxEntryRejectsSameSandboxWithoutWaiting(t *testing.T) {
	m := &Manager{}

	key, entry, err := m.reserveSandboxEntry("sandbox-a", "runtime-a")
	if err != nil {
		t.Fatalf("reserveSandboxEntry() error = %v", err)
	}
	defer m.sandboxes.CompareAndDelete(key, entry)
	defer entry.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, _, err := m.reserveSandboxEntry("sandbox-a", "runtime-replacement")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("same sandbox reserve error = %v, want already exists", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("same sandbox reserve blocked behind the existing entry lock")
	}
}
