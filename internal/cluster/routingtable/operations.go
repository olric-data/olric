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
	"fmt"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/cespare/xxhash/v2"
	"github.com/tidwall/redcon"
	"github.com/vmihailenco/msgpack/v5"
)

func (r *RoutingTable) lengthOfPartCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	// The command handlers of the routing table service should wait for the cluster join event.
	<-r.joined

	lengthOfPartCmd, err := protocol.ParseLengthOfPartCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var part *partitions.Partition
	if lengthOfPartCmd.Replica {
		part = r.backup.PartitionByID(lengthOfPartCmd.PartID)
	} else {
		part = r.primary.PartitionByID(lengthOfPartCmd.PartID)
	}

	conn.WriteInt(part.Length())
}

func (r *RoutingTable) verifyRoutingTable(id uint64, table map[uint64]*route) error {
	// Check the coordinator
	coordinator, err := r.discovery.FindMemberByID(id)
	if err != nil {
		return err
	}

	myCoordinator := r.discovery.GetCoordinator()
	if !coordinator.CompareByID(myCoordinator) {
		return fmt.Errorf("unrecognized cluster coordinator: %s: %s", coordinator, myCoordinator)
	}

	// Compare partition counts to catch a possible inconsistencies in configuration
	if r.config.PartitionCount != uint64(len(table)) {
		return fmt.Errorf("invalid partition count: %d", len(table))
	}
	return nil
}

func (r *RoutingTable) updateRoutingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	// The command handlers of the routing table service should wait for the cluster join event.
	<-r.joined

	r.updateRoutingMtx.Lock()
	defer r.updateRoutingMtx.Unlock()

	updateRoutingCmd, err := protocol.ParseUpdateRoutingCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	table := make(map[uint64]*route)
	err = msgpack.Unmarshal(updateRoutingCmd.Payload, &table)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	// Log this event
	coordinator, err := r.discovery.FindMemberByID(updateRoutingCmd.CoordinatorID)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	r.log.V(3).Printf("[INFO] Routing table has been pushed by %s", coordinator)

	if err = r.verifyRoutingTable(updateRoutingCmd.CoordinatorID, table); err != nil {
		protocol.WriteError(conn, err)
		return
	}

	// owners(atomic.value) is guarded by routingUpdateMtx against parallel writers.
	// Calculate routing signature. This is useful to control balancing tasks.
	r.setSignature(xxhash.Sum64(updateRoutingCmd.Payload))
	for partID, data := range table {
		// Set partition(primary copies) owners
		part := r.primary.PartitionByID(partID)
		part.SetOwners(data.Owners)

		// Set backup owners
		bpart := r.backup.PartitionByID(partID)
		bpart.SetOwners(data.Backups)
	}

	// Used by the LRU implementation.
	r.setOwnedPartitionCount()

	// Bootstrapped by the coordinator.
	r.markBootstrapped()

	// Collect report
	value, err := r.prepareLeftOverDataReport()
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	// Call balancer to distribute load evenly
	r.wg.Add(1)
	go r.runCallbacks()
	conn.WriteBulk(value)
}
