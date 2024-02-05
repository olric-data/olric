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

package routingtable

import (
	"errors"
	"github.com/buraksezer/consistent"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
)

func (r *RoutingTable) distributePrimaryCopies(partID uint64) []discovery.Member {
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
		cmd := protocol.NewLengthOfPart(partID).Command(r.ctx)
		rc := r.client.Get(owner.String())
		err := rc.Process(r.ctx, cmd)
		if err != nil {
			r.log.V(6).Printf("[DEBUG] Failed to check key count on backup "+
				"partition: %d: %v", partID, err)
			// Pass it. If the node is down, memberlist package will send a leave event.
			continue
		}

		count, err := cmd.Result()
		if err != nil {
			r.log.V(6).Printf("[DEBUG] Failed to check key count on backup "+
				"partition: %d: %v", partID, err)
			// Pass it. If the node is down, memberlist package will send a leave event.
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

func (r *RoutingTable) distributeBackups(partID uint64) []discovery.Member {
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
		cmd := protocol.NewLengthOfPart(partID).SetReplica().Command(r.ctx)
		rc := r.client.Get(backup.String())
		err := rc.Process(r.ctx, cmd)
		if err != nil {
			r.log.V(6).Printf("[DEBUG] Failed to check key count on backup "+
				"partition: %d: %v", partID, err)
			// Pass it. If the node is down, memberlist package will send a leave event.
			continue
		}
		count, err := cmd.Result()
		if err != nil {
			r.log.V(6).Printf("[DEBUG] Failed to check key count on backup "+
				"partition: %d: %v", partID, err)
			// Pass it. If the node is down, memberlist package will send a leave event.
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
