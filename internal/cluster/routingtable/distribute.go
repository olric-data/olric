// Copyright 2018-2026 The Olric Authors
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

package routingtable

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/olric-data/olric/internal/discovery"
	"github.com/olric-data/olric/internal/protocol"
)

func (r *RoutingTable) distributePrimaryCopies(partID uint64, unreachable *unreachableOwners) []discovery.Member {
	// First you need to create a copy of the owners list. Don't modify the current list.
	part := r.primary.PartitionByID(partID)
	owners := make([]discovery.Member, part.OwnerCount())
	copy(owners, part.Owners())

	// Find the new partition owner.
	newOwner := r.consistent.GetPartitionOwner(int(partID))

	// First run.
	if len(owners) == 0 {
		owners = append(owners, newOwner.(discovery.Member))
		return owners
	}

	// Prune dead nodes
	for i := 0; i < len(owners); i++ {
		owner := owners[i]
		current, err := r.discovery.FindMemberByName(owner.Name)
		if err != nil {
			r.log.V(6).Printf("[DEBUG] Failed to find %s in the cluster: %v", owner, err)
			owners = append(owners[:i], owners[i+1:]...)
			i--
			r.log.V(3).Printf("[INFO] Member: %s has been deleted from the primary owners list of PartID: %v", owner.String(), partID)
			continue
		}
		if !owner.CompareByID(current) {
			r.log.V(3).Printf("[WARN] One of the partitions owners is probably re-joined: %s", current)
			owners = append(owners[:i], owners[i+1:]...)
			i--
			continue
		}
	}

	// Prune empty nodes
	for i := 0; i < len(owners); i++ {
		owner := owners[i]
		count, ok := r.partitionKeyCount(partID, owner, false, unreachable)
		if !ok {
			// Unknown. If the node is down, memberlist will send a leave event.
			continue
		}

		if count == 0 {
			// Empty partition. Delete it from ownership list.
			owners = append(owners[:i], owners[i+1:]...)
			i--
		}
	}

	// Here add the new partition newOwner.
	for i, owner := range owners {
		if owner.CompareByID(newOwner.(discovery.Member)) {
			// Remove it from the current position
			owners = append(owners[:i], owners[i+1:]...)
			// Append it again to head
			return append(owners, newOwner.(discovery.Member))
		}
	}
	return append(owners, newOwner.(discovery.Member))
}

func (r *RoutingTable) getReplicaOwners(partID uint64) ([]consistent.Member, error) {
	for i := r.config.ReplicaCount; i > 0; i-- {
		newOwners, err := r.consistent.GetClosestNForPartition(int(partID), i)
		if errors.Is(err, consistent.ErrInsufficientMemberCount) {
			continue
		}
		if err != nil {
			// Fail early
			return nil, err
		}
		return newOwners, nil
	}
	return nil, consistent.ErrInsufficientMemberCount
}

func isOwner(member discovery.Member, owners []consistent.Member) bool {
	for _, owner := range owners {
		if member.Name == owner.String() {
			return true
		}
	}
	return false
}

func (r *RoutingTable) distributeBackups(partID uint64, unreachable *unreachableOwners) []discovery.Member {
	part := r.backup.PartitionByID(partID)
	owners := make([]discovery.Member, part.OwnerCount())
	copy(owners, part.Owners())

	newOwners, err := r.getReplicaOwners(partID)
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to get replica owners for PartID: %d: %v",
			partID, err)
		return nil
	}

	// Remove the primary owner
	newOwners = newOwners[1:]

	// First run
	if len(owners) == 0 {
		for _, owner := range newOwners {
			owners = append(owners, owner.(discovery.Member))
		}
		return owners
	}

	// Prune dead nodes
	for i := 0; i < len(owners); i++ {
		backup := owners[i]
		cur, err := r.discovery.FindMemberByName(backup.Name)
		if err != nil {
			r.log.V(6).Printf("[DEBUG] Failed to find %s in the cluster: %v", backup, err)
			// Delete it.
			owners = append(owners[:i], owners[i+1:]...)
			i--
			r.log.V(6).Printf("[INFO] Member: %s has been deleted from the backup owners list of PartID: %v", backup.String(), partID)
			continue
		}
		if !backup.CompareByID(cur) {
			r.log.V(3).Printf("[WARN] One of the backup owners is probably re-joined: %s", cur)
			// Delete it.
			owners = append(owners[:i], owners[i+1:]...)
			i--
			continue
		}
	}

	// Prune empty nodes
	for i := 0; i < len(owners); i++ {
		backup := owners[i]
		count, ok := r.partitionKeyCount(partID, backup, true, unreachable)
		if !ok {
			// Unknown. If the node is down, memberlist will send a leave event.
			continue
		}

		if count != 0 {
			// About this scenario:
			//
			// * ReplicaCount = 3
			// * Create three nodes and insert some keys
			// * Kill one of the nodes
			// * Now we have replicas that it's impossible to transfer its ownership
			// * Since we cannot drop a healthy replica, we prefer to keep it until
			//   a new node joined. Then, we transfer the ownership safely.
			// * During this incident, a node owns a primary and backup replicas at the same time.
			if !isOwner(backup, newOwners) {
				r.log.V(3).Printf("[WARN] %s hosts primary and replica copies "+
					"for PartID: %d", backup, partID)
			}
			continue
		}

		// Empty node, delete it.
		owners = append(owners[:i], owners[i+1:]...)
		i--
	}

	// Here add the new backup owners.
	for _, newOwner := range newOwners {
		var exists bool
		for i, owner := range owners {
			if owner.CompareByID(newOwner.(discovery.Member)) {
				exists = true
				// Remove it from the current position
				owners = append(owners[:i], owners[i+1:]...)
				// Append it again to head
				owners = append(owners, newOwner.(discovery.Member))
				break
			}
		}
		if !exists {
			owners = append(owners, newOwner.(discovery.Member))
		}
	}
	return owners
}

// unreachableOwners tracks owners whose probe failed during one fillRoutingTable
// pass so the remaining partitions skip them instead of paying the timeout again.
type unreachableOwners struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newUnreachableOwners() *unreachableOwners {
	return &unreachableOwners{m: make(map[string]struct{})}
}

func (u *unreachableOwners) has(addr string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	_, ok := u.m[addr]
	return ok
}

func (u *unreachableOwners) add(addr string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.m[addr] = struct{}{}
}

// probeTimeout bounds a single partition key-count probe. The probe is a cheap
// bookkeeping question, so a slow answer means the owner is unhealthy, not that
// the answer is worth waiting for.
const probeTimeout = 250 * time.Millisecond

// partitionKeyCount asks an owner how many keys it holds for a partition.
//
// fillRoutingTable runs this for every owner of every partition, so an owner
// that blocks costs PartitionCount timeouts per routing update, which stalls the
// coordinator's event loop for minutes and leaves joining nodes without a
// routing table. unreachable is per fillRoutingTable pass: once an owner fails,
// the remaining partitions skip it. ok=false means "unknown", and callers keep
// the owner rather than pruning on a failed probe.
func (r *RoutingTable) partitionKeyCount(partID uint64, owner discovery.Member, replica bool, unreachable *unreachableOwners) (int64, bool) {
	addr := owner.String()
	if unreachable.has(addr) {
		return 0, false
	}

	ctx, cancel := context.WithTimeout(r.ctx, probeTimeout)
	defer cancel()

	builder := protocol.NewLengthOfPart(partID)
	if replica {
		builder = builder.SetReplica()
	}
	cmd := builder.Command(ctx)

	if err := r.client.Get(addr).Process(ctx, cmd); err != nil {
		r.log.V(6).Printf("[DEBUG] Failed to check key count on partition: %d: %v", partID, err)
		unreachable.add(addr)
		return 0, false
	}
	count, err := cmd.Result()
	if err != nil {
		r.log.V(6).Printf("[DEBUG] Failed to check key count on partition: %d: %v", partID, err)
		unreachable.add(addr)
		return 0, false
	}
	return count, true
}
