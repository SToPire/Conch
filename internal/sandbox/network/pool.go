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
	"strings"
	"sync"

	"github.com/openeuler/Conch/pkg/ulog"
)

const (
	defaultPoolSize = 250
	prefillWorkers  = 16
)

func getLogger() ulog.Logger {
	return ulog.GetLogger()
}

type Pool struct {
	slotStorage        Storage
	newSlots           chan *Slot
	done               chan struct{}
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
		getLogger().Error("failed to create network", ulog.F("error", err))
		return nil, fmt.Errorf("failed to create network: %w", err)
	}
	if err := p.setupSlotNetwork(ctx, slot); err != nil {
		teardownErr := p.teardownSlotNetwork(context.WithoutCancel(ctx), slot, true)
		releaseErr := p.slotStorage.Release(slot)
		err = errors.Join(err, teardownErr, releaseErr)
		if isExpectedShutdownError(ctx, err) {
			getLogger().Debug("network creation interrupted during shutdown", ulog.F("error", err))
			return nil, context.Canceled
		}
		getLogger().Error("failed to setup slot network", ulog.F("error", err))
		return nil, fmt.Errorf("failed to setup slot network: %w", err)
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
	defer close(p.newSlots)

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
			p.newSlots <- slot
		}
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

	// Start a bounded worker pool to avoid overwhelming netns/iptables operations.
	for w := 0; w < workers; w++ {
		go func() {
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
						return
					case <-p.done:
						return
					case results <- result{slot: slot, err: err}:
					}
				}
			}
		}()
	}

	// Submit one prefill task per target slot; static reservation is one-shot.
	for i := 0; i < target; i++ {
		jobs <- job{}
	}
	close(jobs)

	var firstErr error
	// Drain all worker results to avoid blocked goroutine sends, then return first error if any.
	for i := 0; i < target; i++ {
		select {
		case <-p.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case r := <-results:
			if r.err != nil {
				if firstErr == nil {
					firstErr = r.err
				}
				continue
			}
			p.newSlots <- r.slot
		}
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
		teardownErr := p.teardownSlotNetwork(context.WithoutCancel(ctx), slot, false)
		return errors.Join(fmt.Errorf("failed to setup guest tap network: %w", err), teardownErr)
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
			} else {
				getLogger().Warn("slot unhealthy, dropping from the pool", ulog.F("slot", slot.Key), ulog.F("error", slotHealthErr))
				err := p.teardownSlotNetwork(ctx, slot, true)
				if err != nil {
					getLogger().Error("failed to remove network", ulog.F("error", err))
					return fmt.Errorf("failed to remove network: %w", err)
				}
				err = p.slotStorage.Release(slot)
				if err != nil {
					getLogger().Error("failed to release network slot", ulog.F("error", err))
					return fmt.Errorf("failed to release network slot: %w", err)
				}
				p.untrackInUse(slot)
			}
		}
		return nil
	}
}

func (p *Pool) teardownSlotNetwork(ctx context.Context, slot *Slot, deleteNetNS bool) error {
	if slot == nil {
		return nil
	}

	var errs []error
	netnsPath := slot.NetNSPath()
	if slot.CNIResult() != nil {
		if err := TeardownGuestTapNetwork(ctx, slot, netnsPath, slot.CNIResult()); err != nil {
			errs = append(errs, err)
		}
		if p != nil && p.cniManager != nil {
			if err := p.cniManager.TeardownPodNetwork(ctx, slot.CNIContainerID(), netnsPath, slot.cniOpts...); err != nil {
				errs = append(errs, err)
			}
		}
		slot.clearSlotNetwork()
	}
	slot.clearSandboxAssignment()

	if deleteNetNS {
		if err := DeleteSandboxNetworkNamespace(netnsPath); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
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
	var errs []error
	cleaned := 0
	failed := 0
	cleanupSlot := func(slot *Slot, category string) {
		if slot == nil {
			failed++
			return
		}
		err := p.teardownSlotNetwork(context.Background(), slot, true)
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
