// Copyright 2018-2024 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package balancer

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routingtable"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/service"
	"github.com/buraksezer/olric/pkg/flog"
)

type Balancer struct {
	sync.Mutex

	log     *flog.Logger
	config  *config.Config
	primary *partitions.Partitions
	backup  *partitions.Partitions
	rt      *routingtable.RoutingTable
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func New(e *environment.Environment) *Balancer {
	c := e.Get("config").(*config.Config)
	log := e.Get("logger").(*flog.Logger)
	ctx, cancel := context.WithCancel(context.Background())
	return &Balancer{
		config:  c,
		primary: e.Get("primary").(*partitions.Partitions),
		backup:  e.Get("backup").(*partitions.Partitions),
		rt:      e.Get("routingtable").(*routingtable.RoutingTable),
		log:     log,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (b *Balancer) isAlive() bool {
	select {
	case <-b.ctx.Done():
		// The node is gone.
		return false
	default:
	}
	return true
}

func (b *Balancer) scanPartition(sign uint64, part *partitions.Partition, owners ...discovery.Member) {
	ownersStr := func() string {
		var names []string
		for _, owner := range owners {
			names = append(names, owner.String())
		}
		return strings.Join(names, ",")
	}()

	part.Map().Range(func(rawName, rawFragment interface{}) bool {
		f := rawFragment.(partitions.Fragment)
		if f.Stats().Length == 0 {
			return false
		}
		name := strings.TrimPrefix(rawName.(string), "dmap.")

		b.log.V(2).Printf("[INFO] Moving %s fragment: %s (kind: %s) on PartID: %d to %s",
			f.Name(), name, part.Kind(), part.ID(), ownersStr)

		err := f.Move(part, name, owners)
		if err != nil {
			b.log.V(2).Printf("[ERROR] Failed to move %s fragment: %s on PartID: %d to %s: %v",
				f.Name(), name, part.ID(), ownersStr, err)
		}

		// if this returns true, the iteration continues
		return !b.breakLoop(sign)
	})
}

func (b *Balancer) primaryCopies() {
	sign := b.rt.Signature()
	for partID := uint64(0); partID < b.config.PartitionCount; partID++ {
		if b.breakLoop(sign) {
			break
		}

		part := b.primary.PartitionByID(partID)
		if part.Length() == 0 {
			// Empty partition. Skip it.
			continue
		}

		owner := part.Owner()
		// Here we don't use CompareByID function because the routing table is an
		// eventually consistent data structure and a node can try to move data
		// to previous instance(the same name but a different birthdate)
		// of itself. So just check the name.
		if owner.CompareByName(b.rt.This()) {
			// Already belongs to me.
			continue
		}

		// This is a previous owner. Move the keys.
		b.scanPartition(sign, part, owner)
	}
}

func (b *Balancer) breakLoop(sign uint64) bool {
	if !b.isAlive() {
		return true
	}

	if sign != b.rt.Signature() {
		// Routing table is updated. Just quit. Another balancer goroutine
		// will work on the new table immediately.
		return true
	}

	return false
}

func (b *Balancer) backupCopies() {
	sign := b.rt.Signature()
LOOP:
	for partID := uint64(0); partID < b.config.PartitionCount; partID++ {
		if b.breakLoop(sign) {
			break
		}

		part := b.backup.PartitionByID(partID)
		if part.Length() == 0 || part.OwnerCount() == 0 {
			continue
		}

		var (
			counter       = 1
			currentOwners []discovery.Member
		)

		owners := part.Owners()
		for i := len(owners) - 1; i >= 0; i-- {
			if counter > b.config.ReplicaCount-1 {
				break
			}

			counter++
			owner := owners[i]
			// Here we don't use CompareById function because the routing table
			// is an eventually consistent data structure and a node can try to
			// move data to previous instance(the same name but a different birthdate)
			// of itself. So just check the name.
			if b.rt.This().CompareByName(owner) {
				// Already belongs to me.
				continue LOOP
			}
			currentOwners = append(currentOwners, owner)
		}

		if len(currentOwners) == 0 {
			continue LOOP
		}

		b.scanPartition(sign, part, currentOwners...)
	}
}

func (b *Balancer) triggerBalancer() {
	b.Lock()
	defer b.Unlock()

	if err := b.rt.CheckBootstrap(); err != nil {
		b.log.V(2).Printf("[WARN] Balancer awaits for bootstrapping")
		return
	}

	b.primaryCopies()

	if b.config.ReplicaCount > config.MinimumReplicaCount {
		b.backupCopies()
	}
}

func (b *Balancer) BalanceEagerly() {
	b.triggerBalancer()
}

func (b *Balancer) balance() {
	defer b.wg.Done()

	timer := time.NewTimer(b.config.TriggerBalancerInterval)
	defer timer.Stop()

	for {
		timer.Reset(b.config.TriggerBalancerInterval)
		select {
		case <-timer.C:
			b.triggerBalancer()
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Balancer) Start() error {
	b.wg.Add(1)
	go b.balance()
	return nil
}

func (b *Balancer) RegisterHandlers() {}

func (b *Balancer) Shutdown(ctx context.Context) error {
	select {
	case <-b.ctx.Done():
		// already closed
		return nil
	default:
	}

	b.cancel()
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return err
		}
	case <-done:
	}

	return nil
}

var _ service.Service = (*Balancer)(nil)
