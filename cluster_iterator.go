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

package olric

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/protocol"
)

type currentCursor struct {
	primary uint64
	replica uint64
}

// ClusterIterator implements distributed query on DMaps.
type ClusterIterator struct {
	mtx             sync.Mutex // protects pos and page
	routingTableMtx sync.Mutex // protects routingTable and partitionCount

	logger         *log.Logger
	dm             *ClusterDMap
	clusterClient  *ClusterClient
	pos            int
	page           []string
	route          *Route
	partitionKeys  map[string]struct{}
	cursors        map[uint64]map[string]*currentCursor
	partID         uint64 // current partition id
	routingTable   RoutingTable
	partitionCount uint64
	config         *dmap.ScanConfig
	scanner        func() error
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

func (i *ClusterIterator) loadRoute() {
	i.routingTableMtx.Lock()
	defer i.routingTableMtx.Unlock()

	route, ok := i.routingTable[i.partID]
	if !ok {
		panic("partID: could not be found in the routing table")
	}
	i.route = &route
}

func (i *ClusterIterator) updateCursor(owner string, cursor uint64) {
	if _, ok := i.cursors[i.partID]; !ok {
		i.cursors[i.partID] = make(map[string]*currentCursor)
	}
	cc, ok := i.cursors[i.partID][owner]
	if !ok {
		cc = &currentCursor{}
		if i.config.Replica {
			cc.replica = cursor
		} else {
			cc.primary = cursor
		}
		i.cursors[i.partID][owner] = cc
		return
	}

	if i.config.Replica {
		cc.replica = cursor
	} else {
		cc.primary = cursor
	}
	i.cursors[i.partID][owner] = cc
}

func (i *ClusterIterator) loadCursor(owner string) uint64 {
	if _, ok := i.cursors[i.partID]; !ok {
		return 0
	}
	cc, ok := i.cursors[i.partID][owner]
	if !ok {
		return 0
	}
	if i.config.Replica {
		return cc.replica
	}
	return cc.primary
}

func (i *ClusterIterator) updateIterator(keys []string, cursor uint64, owner string) {
	for _, key := range keys {
		if _, ok := i.partitionKeys[key]; !ok {
			i.page = append(i.page, key)
			i.partitionKeys[key] = struct{}{}
		}
	}
	i.updateCursor(owner, cursor)
}

func (i *ClusterIterator) getOwners() []string {
	var raw []string
	if i.config.Replica {
		raw = i.routingTable[i.partID].ReplicaOwners
	} else {
		raw = i.routingTable[i.partID].PrimaryOwners
	}
	var owners []string
	// Make a safe copy of the raw.
	for _, owner := range raw {
		owners = append(owners, owner)
	}
	return owners
}

func (i *ClusterIterator) removeScannedOwner(idx int) {
	if i.config.Replica {
		if len(i.route.ReplicaOwners) > 0 && len(i.route.ReplicaOwners) > idx {
			i.route.ReplicaOwners = append(i.route.ReplicaOwners[:idx], i.route.ReplicaOwners[idx+1:]...)
		}
	} else {
		if len(i.route.PrimaryOwners) > 0 && len(i.route.PrimaryOwners) > idx {
			i.route.PrimaryOwners = append(i.route.PrimaryOwners[:idx], i.route.PrimaryOwners[idx+1:]...)
		}
	}
}

func (i *ClusterIterator) scanOnOwners() error {
	owners := i.getOwners()

	for idx, owner := range owners {
		cursor := i.loadCursor(owner)

		// Build a scan command here
		s := protocol.NewScan(i.partID, i.dm.Name(), cursor)
		if i.config.HasCount {
			s.SetCount(i.config.Count)
		}
		if i.config.HasMatch {
			s.SetMatch(i.config.Match)
		}
		if i.config.Replica {
			s.SetReplica()
		}

		scanCmd := s.Command(i.ctx)
		// Fetch a Redis client for the given owner.
		rc := i.clusterClient.client.Get(owner)
		err := rc.Process(i.ctx, scanCmd)
		if err != nil {
			return err
		}

		keys, newCursor, err := scanCmd.Result()
		if err != nil {
			return err
		}
		i.updateIterator(keys, newCursor, owner)
		if newCursor == 0 {
			i.removeScannedOwner(idx)
		}
	}
	return nil
}

func (i *ClusterIterator) resetPage() {
	if len(i.page) != 0 {
		i.page = []string{}
	}
	i.pos = 0
}

func (i *ClusterIterator) fetchData() error {
	i.config.Replica = false
	if err := i.scanner(); err != nil {
		return err
	}

	i.config.Replica = true
	return i.scanner()
}

func (i *ClusterIterator) reset() {
	i.partitionKeys = make(map[string]struct{})
	i.resetPage()
	i.loadRoute()
}

func (i *ClusterIterator) next() bool {
	if len(i.page) != 0 {
		i.pos++
		if i.pos <= len(i.page) {
			return true
		}
	}

	i.resetPage()

	for {
		if err := i.fetchData(); err != nil {
			i.logger.Printf("[ERROR] Failed to fetch data: %s", err)
			return false
		}
		if len(i.page) != 0 {
			// We have data on the page to read. Stop the iteration.
			break
		}

		if len(i.route.PrimaryOwners) == 0 && len(i.route.ReplicaOwners) == 0 {
			// We completed scanning all the owners. Stop the iteration.
			break
		}
	}

	if len(i.page) == 0 && len(i.route.PrimaryOwners) == 0 && len(i.route.ReplicaOwners) == 0 {
		i.partID++
		if i.partID >= i.partitionCount {
			return false
		}
		i.reset()
		return i.next()
	}
	i.pos = 1
	return true
}

// Next returns true if there is more key in the iterator implementation.
// Otherwise, it returns false
func (i *ClusterIterator) Next() bool {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	select {
	case <-i.ctx.Done():
		return false
	default:
	}

	return i.next()
}

// Key returns a key name from the distributed map.
func (i *ClusterIterator) Key() string {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var key string
	if i.pos > 0 && i.pos <= len(i.page) {
		key = i.page[i.pos-1]
	}
	return key
}

func (i *ClusterIterator) fetchRoutingTablePeriodically() {
	defer i.wg.Done()

	for {
		select {
		case <-i.ctx.Done():
			return
		case <-time.After(time.Second):
			if err := i.fetchRoutingTable(); err != nil {
				i.logger.Printf("[ERROR] Failed to fetch the latest version of the routing table: %s", err)
			}
		}
	}
}

func (i *ClusterIterator) fetchRoutingTable() error {
	routingTable, err := i.clusterClient.RoutingTable(i.ctx)
	if err != nil {
		return err
	}

	i.routingTableMtx.Lock()
	defer i.routingTableMtx.Unlock()

	// Partition count is a constant, actually. It has to be greater than zero.
	i.partitionCount = uint64(len(routingTable))
	i.routingTable = routingTable
	return nil
}

// Close stops the iteration and releases allocated resources.
func (i *ClusterIterator) Close() {
	select {
	case <-i.ctx.Done():
		return
	default:
	}

	i.cancel()

	// await for routing table updater
	i.wg.Wait()
}
