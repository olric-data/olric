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
	"fmt"
	"strconv"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

type Route struct {
	PrimaryOwners []string
	ReplicaOwners []string
}

type RoutingTable map[uint64]Route

func mapToRoutingTable(slice []interface{}) (RoutingTable, error) {
	rt := make(RoutingTable)
	for _, raw := range slice {
		item := raw.([]interface{})
		rawPartID, rawPrimaryOwners, rawReplicaOwners := item[0], item[1], item[2]
		var partID uint64
		switch rawPartID.(type) {
		case int64:
			partID = uint64(rawPartID.(int64))
		case string:
			raw, err := strconv.ParseUint(rawPartID.(string), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid partition id: %v: %w", rawPartID, err)
			}
			partID = raw
		default:
			return nil, fmt.Errorf("invalid partition id: %v", rawPartID)
		}

		r := Route{}
		primaryOwners, ok := rawPrimaryOwners.([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid primary owners: %v", rawPrimaryOwners)
		}
		for _, rawOwner := range primaryOwners {
			owner, ok := rawOwner.(string)
			if !ok {
				return nil, fmt.Errorf("invalid owner: %v", owner)
			}
			r.PrimaryOwners = append(r.PrimaryOwners, owner)
		}

		replicaOwners, ok := rawReplicaOwners.([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid replica owners: %v", rawPrimaryOwners)
		}
		for _, rawOwner := range replicaOwners {
			owner, ok := rawOwner.(string)
			if !ok {
				return nil, fmt.Errorf("invalid owner: %v", owner)
			}
			r.ReplicaOwners = append(r.ReplicaOwners, owner)
		}
		rt[partID] = r
	}
	return rt, nil
}

func (db *Olric) clusterRoutingTableCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := protocol.ParseClusterRoutingTable(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	coordinator := db.rt.Discovery().GetCoordinator()
	if coordinator.CompareByID(db.rt.This()) {
		conn.WriteArray(int(db.config.PartitionCount))
		rt := db.fillRoutingTable()
		for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
			conn.WriteArray(3)
			conn.WriteUint64(partID)

			r := rt[partID]
			primaryOwners := r.PrimaryOwners
			conn.WriteArray(len(primaryOwners))
			for _, owner := range primaryOwners {
				conn.WriteBulkString(owner)
			}

			replicaOwners := r.ReplicaOwners
			conn.WriteArray(len(replicaOwners))
			for _, owner := range replicaOwners {
				conn.WriteBulkString(owner)
			}
		}
		return
	}

	// Redirect to the cluster coordinator
	rtCmd := protocol.NewClusterRoutingTable().Command(db.ctx)
	rc := db.client.Get(coordinator.String())
	err = rc.Process(db.ctx, rtCmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	slice, err := rtCmd.Slice()
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteAny(slice)
}

func (db *Olric) fillRoutingTable() RoutingTable {
	rt := make(RoutingTable)
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		r := Route{}
		primaryOwners := db.primary.PartitionOwnersByID(partID)
		for _, owner := range primaryOwners {
			r.PrimaryOwners = append(r.PrimaryOwners, owner.String())
		}
		replicaOwners := db.backup.PartitionOwnersByID(partID)
		for _, owner := range replicaOwners {
			r.ReplicaOwners = append(r.ReplicaOwners, owner.String())
		}
		rt[partID] = r
	}
	return rt
}

func (db *Olric) routingTable(ctx context.Context) (RoutingTable, error) {
	coordinator := db.rt.Discovery().GetCoordinator()
	if coordinator.CompareByID(db.rt.This()) {
		return db.fillRoutingTable(), nil
	}

	rtCmd := protocol.NewClusterRoutingTable().Command(ctx)
	rc := db.client.Get(coordinator.String())
	err := rc.Process(ctx, rtCmd)
	if err != nil {
		return nil, err
	}
	slice, err := rtCmd.Slice()
	if err != nil {
		return nil, err
	}
	return mapToRoutingTable(slice)
}

func (db *Olric) clusterMembersCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := protocol.ParseClusterMembers(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	coordinator := db.rt.Discovery().GetCoordinator()
	members := db.rt.Discovery().GetMembers()
	conn.WriteArray(len(members))
	for _, member := range members {
		conn.WriteArray(3)
		conn.WriteBulkString(member.Name)
		// go-redis/redis package cannot handle uint64. At the time of this writing,
		// there is no solution for this, and I don't want to use a soft fork to repair it.
		//conn.WriteUint64(member.ID)
		conn.WriteInt64(member.Birthdate)
		if coordinator.CompareByID(member) {
			conn.WriteBulkString("true")
		} else {
			conn.WriteBulkString("false")
		}
	}
}
