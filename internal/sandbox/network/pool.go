/*
Copyright the e2b-dev Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[MODIFIED] - Changes made on 2025-12-24 by Team conch: Add bridge interface
*/
package network

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/openeuler/Conch/pkg/ulog"
)

const (
	defaultPoolSize         = 250
	prefillWorkers          = 16
	hostLocalIPAMNetworkDir = "/var/lib/cni/networks"
)

func getLogger() ulog.Logger {
	return ulog.GetLogger()
}

type Pool struct {
	slotStorage        Storage
	newSlots           chan *Slot
	done               chan struct{}
	stopOnce           sync.Once
	closeOnce          sync.Once
	opMu               sync.Mutex
	wg                 sync.WaitGroup
	dynamicReservation bool
	cniManager         *CNIManager
	inUse              map[string]*Slot
	inUseMu            sync.Mutex
}

func normalizeAndValidatePoolSize(poolSize int) (int, error) {
	if poolSize <= 0 {
		poolSize = defaultPoolSize
	}
	if !bridgeLayoutReady || maxVrtSlotsSize == invaildSlotSize {
		return 0, fmt.Errorf("bridge layout capacity is not initialized")
	}
	if poolSize > maxVrtSlotsSize {
		return 0, fmt.Errorf("invalid network.pool_size=%d, exceeds max available slots=%d", poolSize, maxVrtSlotsSize)
	}
	return poolSize, nil
}

func NewPool(poolSize int, dynamicReservation bool, bridgeCount int, tapIP string, tapMask int, cniCfg CNIManagerConfig) (*Pool, error) {
	if err := initConfigureBridgeLayout(bridgeCount); err != nil {
		return nil, fmt.Errorf("invalid bridge layout: %w", err)
	}
	poolSize, err := normalizeAndValidatePoolSize(poolSize)
	if err != nil {
		return nil, err
	}
	if err := configureTapNetwork(tapIP, tapMask); err != nil {
		return nil, fmt.Errorf("invalid tap network config: %w", err)
	}
	newSlots := make(chan *Slot, poolSize)

	slotStorage, err := NewStorage(maxVrtSlotsSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create new storage: %w", err)
	}

	cniManager, err := NewCNIManager(cniCfg)
	if err != nil {
		return nil, err
	}

	p := &Pool{
		slotStorage:        slotStorage,
		newSlots:           newSlots,
		done:               make(chan struct{}),
		dynamicReservation: dynamicReservation,
		cniManager:         cniManager,
		inUse:              make(map[string]*Slot),
	}

	return p, nil
}

func (p *Pool) createNetworkSlot(ctx context.Context) (*Slot, error) {
	slot, err := p.slotStorage.Acquire(ctx)
	if err != nil {
		if isExpectedShutdownError(ctx, err) {
			return nil, context.Canceled
		}
		getLogger().Error("failed to acquire network slot", ulog.F("error", err))
		return nil, fmt.Errorf("failed to acquire network slot: %w", err)
	}

	err = slot.CreateNetwork()
	if err != nil {
		releaseErr := p.slotStorage.Release(slot)
		err = errors.Join(err, releaseErr)
		if isExpectedShutdownError(ctx, err) {
			getLogger().Debug("network creation interrupted during shutdown", ulog.F("error", err))
			return nil, context.Canceled
		}
		getLogger().Error("failed to create network", ulog.F("slot_index", slot.Idx), ulog.F("error", err))
		return nil, fmt.Errorf("failed to create network for slot index %d: %w", slot.Idx, err)
	}
	if err := p.setupSlotNetwork(ctx, slot); err != nil {
		teardownErr := p.teardownSlotNetwork(context.WithoutCancel(ctx), slot)
		releaseErr := p.slotStorage.Release(slot)
		err = errors.Join(err, teardownErr, releaseErr)
		if isExpectedShutdownError(ctx, err) {
			getLogger().Debug("network creation interrupted during shutdown", ulog.F("error", err))
			return nil, context.Canceled
		}
		getLogger().Error("failed to setup slot network", ulog.F("slot_index", slot.Idx), ulog.F("error", err))
		return nil, fmt.Errorf("failed to setup slot network for slot index %d: %w", slot.Idx, err)
	}
	return slot, nil
}

func isExpectedShutdownError(ctx context.Context, err error) bool {
	// Without an error or an active context cancellation, this is not shutdown-related noise.
	if err == nil || ctx.Err() == nil {
		return false
	}
	// Prefer typed context errors when the lower layers preserve them.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// Some external command failures during Ctrl+C only surface as stderr text.
	msg := err.Error()
	return strings.Contains(msg, "exit status -1") || strings.Contains(msg, "signal: interrupt")
}

func (p *Pool) Populate(ctx context.Context) {
	if !p.beginOp() {
		p.closeNewSlots()
		return
	}
	defer p.wg.Done()
	defer p.closeNewSlots()

	if !p.dynamicReservation {
		if err := p.populateStatic(ctx); err != nil {
			getLogger().Warn("pool: static reservation exited with error", ulog.F("error", err))
		}
		select {
		case <-p.done:
			return
		case <-ctx.Done():
			return
		}
	}

	for {
		select {
		case <-p.done:
			return
		case <-ctx.Done():
			return
		default:
			slot, err := p.createNetworkSlot(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				getLogger().Debug("pool: failed to create network", ulog.F("error", err))
				continue
			}
			if !p.enqueueCreatedSlot(ctx, slot) {
				return
			}
		}
	}
}

func (p *Pool) beginOp() bool {
	p.opMu.Lock()
	defer p.opMu.Unlock()

	select {
	case <-p.done:
		return false
	default:
		p.wg.Add(1)
		return true
	}
}

func (p *Pool) stopOp() {
	p.stopOnce.Do(func() {
		p.opMu.Lock()
		defer p.opMu.Unlock()
		close(p.done)
	})
}

func (p *Pool) closeNewSlots() {
	p.closeOnce.Do(func() {
		close(p.newSlots)
	})
}

func (p *Pool) isStopping() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *Pool) enqueueCreatedSlot(ctx context.Context, slot *Slot) bool {
	select {
	case <-p.done:
		p.discardCreatedSlot(slot)
		return false
	case <-ctx.Done():
		p.discardCreatedSlot(slot)
		return false
	case p.newSlots <- slot:
		return true
	}
}

func (p *Pool) discardCreatedSlot(slot *Slot) {
	if slot == nil {
		return
	}
	err := errors.Join(p.teardownSlotNetwork(context.Background(), slot), p.slotStorage.Release(slot))
	if err != nil {
		getLogger().Warn("failed to discard network slot during pool stop", ulog.F("slot_index", slot.Idx), ulog.F("error", err))
	}
}

func (p *Pool) populateStatic(ctx context.Context) error {
	target := cap(p.newSlots)

	type job struct{}
	type result struct {
		slot *Slot
		err  error
	}

	// jobs fan out slot-creation work; results fan in worker outputs.
	jobs := make(chan job, target)
	results := make(chan result, target)

	workers := prefillWorkers
	if target < workers {
		workers = target
	}

	var workerWg sync.WaitGroup
	// Start a bounded worker pool to avoid overwhelming netns/iptables operations.
	for w := 0; w < workers; w++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-p.done:
					return
				case _, ok := <-jobs:
					if !ok {
						return
					}
					slot, err := p.createNetworkSlot(ctx)

					select {
					case <-ctx.Done():
						p.discardCreatedSlot(slot)
						return
					case <-p.done:
						p.discardCreatedSlot(slot)
						return
					case results <- result{slot: slot, err: err}:
					}
				}
			}
		}()
	}
	go func() {
		workerWg.Wait()
		close(results)
	}()

	// Submit one prefill task per target slot; static reservation is one-shot.
	for i := 0; i < target; i++ {
		jobs <- job{}
	}
	close(jobs)

	var firstErr error
	stopping := false
	for r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		if stopping || p.isStopping() {
			stopping = true
			p.discardCreatedSlot(r.slot)
			continue
		}
		if !p.enqueueCreatedSlot(ctx, r.slot) {
			stopping = true
		}
	}

	if p.isStopping() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if firstErr != nil {
		return fmt.Errorf(
			"static reservation stopped before reaching target: current=%d target=%d: %w",
			len(p.newSlots), target, firstErr,
		)
	}

	getLogger().Info("pool: static reservation completed", ulog.F("acquired_total", len(p.newSlots)), ulog.F("in_pool", len(p.newSlots)), ulog.F("target", target))
	return nil
}

func (p *Pool) Get(ctx context.Context, sandboxID string) (*Slot, error) {
	if sandboxID == "" {
		return nil, fmt.Errorf("sandboxID is required")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s, ok := <-p.newSlots:
		if !ok {
			return nil, fmt.Errorf("network channel has been closed")
		}
		if s != nil {
			s.assignSandbox(sandboxID)
			p.trackInUse(s)
		}
		return s, nil
	}
}

func (p *Pool) setupSlotNetwork(ctx context.Context, slot *Slot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || p.cniManager == nil {
		return fmt.Errorf("cni config not initialized")
	}
	if slot == nil {
		return fmt.Errorf("slot is nil")
	}
	netnsPath := slot.NetNSPath()
	if _, _, err := p.cniManager.SelectCNIPluginAndConfig(slot); err != nil {
		return err
	}
	cniID := slot.CNIContainerID()
	opts, err := buildCNIOpts(slot, cniID, netnsPath)
	if err != nil {
		return err
	}

	cniResult, err := p.cniManager.SetupPodNetwork(ctx, cniID, netnsPath, opts...)
	if err != nil {
		return fmt.Errorf("failed to setup cni network: %w", err)
	}
	slot.setSlotNetwork(cniID, cniResult, opts)

	if err := SetupGuestTapNetwork(ctx, slot, netnsPath, cniResult); err != nil {
		return fmt.Errorf("failed to setup guest tap network: %w", err)
	}

	return nil
}

func (p *Pool) enqueueReplacement(ctx context.Context, slot *Slot) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Populate closes newSlots during shutdown; Release may race with that close.
			err = fmt.Errorf("network channel has been closed")
		}
	}()

	select {
	case <-p.done:
		return fmt.Errorf("pool is stopping")
	case <-ctx.Done():
		return ctx.Err()
	case p.newSlots <- slot:
		return nil
	}
}

func (p *Pool) Release(ctx context.Context, slot *Slot) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if slot != nil {
			slot.clearSandboxAssignment()
			slotHealthErr := p.slotHealth(ctx, slot)
			if slotHealthErr == nil {
				p.untrackInUse(slot)
				if err := p.enqueueReplacement(ctx, slot); err != nil {
					p.trackInUse(slot)
					return fmt.Errorf("failed to enqueue replenished slot: %w", err)
				}
				getLogger().Info("slot released back to pool", ulog.F("slot_index", slot.Idx))
			} else {
				getLogger().Warn("slot unhealthy, dropping from the pool", ulog.F("slot_index", slot.Idx), ulog.F("error", slotHealthErr))
				err := p.teardownSlotNetwork(ctx, slot)
				if err != nil {
					getLogger().Error("failed to remove network", ulog.F("slot_index", slot.Idx), ulog.F("error", err))
					return fmt.Errorf("failed to remove network for slot index %d: %w", slot.Idx, err)
				}
				err = p.slotStorage.Release(slot)
				if err != nil {
					getLogger().Error("failed to release network slot", ulog.F("slot_index", slot.Idx), ulog.F("error", err))
					return fmt.Errorf("failed to release network slot for slot index %d: %w", slot.Idx, err)
				}
				p.untrackInUse(slot)
			}
		}
		return nil
	}
}

func (p *Pool) teardownSlotNetwork(ctx context.Context, slot *Slot) error {
	if slot == nil {
		return nil
	}

	var errs []error
	netnsPath := slot.NetNSPath()
	var cniErr error
	var cniID string
	var cniIP string
	if slot.CNIResult() != nil {
		cniID = slot.CNIContainerID()
		cniIP = slot.CNIResult().IP
		if err := TeardownGuestTapNetwork(ctx, slot, netnsPath, slot.CNIResult()); err != nil {
			errs = append(errs, err)
		}
		if p != nil && p.cniManager != nil {
			cniErr = p.teardownPodNetworkWithRetry(ctx, slot, netnsPath)
		}
		if cniErr == nil {
			slot.clearSlotNetwork()
		}
	}
	slot.clearSandboxAssignment()

	if err := DeleteSandboxNetworkNamespace(netnsPath); err != nil {
		errs = append(errs, err)
	}

	if cniErr != nil {
		if isCNIBusyTeardownError(cniErr) && p.validateBusyCNITeardownClean(slot, cniID, cniIP, netnsPath) == nil {
			getLogger().Warn("cni teardown reported busy state after cleanup completed", ulog.F("slot_index", slot.Idx), ulog.F("error", cniErr))
		} else {
			errs = append(errs, cniErr)
		}
	}

	return errors.Join(errs...)
}

func (p *Pool) teardownPodNetworkWithRetry(ctx context.Context, slot *Slot, netnsPath string) error {
	var lastErr error
	for attempt := 0; attempt <= cniTeardownRetryAttempts; attempt++ {
		err := p.cniManager.TeardownPodNetwork(ctx, slot.CNIContainerID(), netnsPath, slot.cniOpts...)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isCNIBusyTeardownError(err) || attempt == cniTeardownRetryAttempts {
			return err
		}
		// Retry teardown with exponential backoff
		delay := cniTeardownRetryDelay * time.Duration(1<<attempt)
		select {
		case <-ctx.Done():
			return errors.Join(lastErr, ctx.Err())
		case <-time.After(delay):
		}
	}
	return lastErr
}

func isCNIBusyTeardownError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "resource busy")
}

func (p *Pool) validateBusyCNITeardownClean(slot *Slot, cniID, cniIP, netnsPath string) error {
	if cniID == "" {
		return fmt.Errorf("cni id unavailable")
	}

	if _, err := os.Stat(netnsPath); err == nil {
		return fmt.Errorf("namespace still exists after teardown")
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to check namespace after teardown: %w", err)
	}
	if p.cniManager == nil || p.cniManager.selectedConf == "" {
		return fmt.Errorf("cni config name unavailable")
	}

	networkName := p.cniManager.selectedConf
	networkDir := filepath.Join(hostLocalIPAMNetworkDir, networkName)
	if _, err := os.Stat(networkDir); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to check host-local ipam dir %s: %w", networkDir, err)
	}

	if cniIP != "" {
		ipFileName, _, _ := strings.Cut(cniIP, "/")
		if ipFileName != "" {
			ipFile := filepath.Join(networkDir, ipFileName)
			if _, err := os.Stat(ipFile); err == nil {
				return fmt.Errorf("host-local allocation %s still exists", ipFile)
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("checking host-local allocation %s: %w", ipFile, err)
			}
		}
	}

	entries, err := os.ReadDir(networkDir)
	if err != nil {
		return fmt.Errorf("reading host-local ipam dir %s: %w", networkDir, err)
	}
	for _, entry := range entries {
		// host-local stores cursor/lock metadata next to IP allocation files; only allocation files can belong to a CNI ID.
		if entry.IsDir() || strings.HasPrefix(entry.Name(), "last_reserved_ip") || entry.Name() == "lock" {
			continue
		}
		path := filepath.Join(networkDir, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading host-local allocation %s: %w", path, err)
		}
		if strings.Contains(strings.TrimSpace(string(content)), cniID) {
			return fmt.Errorf("host-local allocation %s still belongs to %s", path, cniID)
		}
	}
	return nil
}

func (p *Pool) trackInUse(slot *Slot) {
	p.inUseMu.Lock()
	defer p.inUseMu.Unlock()
	p.inUse[slot.Key] = slot
}

func (p *Pool) untrackInUse(slot *Slot) {
	p.inUseMu.Lock()
	defer p.inUseMu.Unlock()
	delete(p.inUse, slot.Key)
}

func (p *Pool) drainInUse() []*Slot {
	p.inUseMu.Lock()
	defer p.inUseMu.Unlock()
	slots := make([]*Slot, 0, len(p.inUse))
	for key, slot := range p.inUse {
		slots = append(slots, slot)
		delete(p.inUse, key)
	}
	return slots
}

func (p *Pool) slotHealth(ctx context.Context, slot *Slot) error {
	if slot == nil {
		return fmt.Errorf("slot is nil")
	}
	if _, err := os.Stat(slot.NetNSPath()); err != nil {
		return fmt.Errorf("namespace missing: %w", err)
	}
	if slot.SandboxID() != "" {
		return fmt.Errorf("slot is still assigned to sandbox %s", slot.SandboxID())
	}
	if slot.CNIResult() == nil || slot.CNIResult().IP == "" {
		return fmt.Errorf("slot has no cni result")
	}
	if p == nil || p.cniManager == nil {
		return fmt.Errorf("cni config not initialized")
	}
	if err := ValidateReusableSlotNetwork(ctx, slot, slot.NetNSPath(), p.cniManager.config.InterfaceName); err != nil {
		return err
	}
	return nil
}

func (p *Pool) Cleanup() error {
	p.stopOp()
	p.wg.Wait()
	p.closeNewSlots()

	var errs []error
	cleaned := 0
	failed := 0
	cleanupSlot := func(slot *Slot, category string) {
		if slot == nil {
			failed++
			return
		}
		err := p.teardownSlotNetwork(context.Background(), slot)
		if err != nil {
			getLogger().Error("cleanup slot failed when removing network", ulog.F("slot", slot.Key), ulog.F("category", category), ulog.F("error", err))
			errs = append(errs, fmt.Errorf("cleanup slot %s failed, %w", slot.Key, err))
			failed++
			return
		}
		err = p.slotStorage.Release(slot)
		if err != nil {
			getLogger().Error("cleanup slot failed when releasing", ulog.F("slot", slot.Key), ulog.F("category", category), ulog.F("error", err))
			errs = append(errs, fmt.Errorf("cleanup slot %s failed, %w", slot.Key, err))
			failed++
			return
		}
		cleaned++
	}

	for _, slot := range p.drainInUse() {
		cleanupSlot(slot, "in_use")
	}

	for slot := range p.newSlots {
		cleanupSlot(slot, "queue")
	}

	getLogger().Info("pool cleanup summary", ulog.F("cleaned_slots", cleaned), ulog.F("failed_slots", failed))

	return errors.Join(errs...)
}
