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
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestOlric_ClusterRoutingTable_clusterRoutingTableCommandHandler(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	rtCmd := protocol.NewClusterRoutingTable().Command(db.ctx)
	rc := db.client.Get(db.rt.This().String())
	err := rc.Process(db.ctx, rtCmd)
	require.NoError(t, err)
	slice, err := rtCmd.Slice()
	require.NoError(t, err)

	rt, err := mapToRoutingTable(slice)
	require.NoError(t, err)
	require.Len(t, rt, int(db.config.PartitionCount))
	for _, route := range rt {
		require.Len(t, route.PrimaryOwners, 1)
		require.Equal(t, db.rt.This().String(), route.PrimaryOwners[0])
		require.Len(t, route.ReplicaOwners, 0)
	}
}

func TestOlric_RoutingTable_Standalone(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	rt, err := db.routingTable(context.Background())
	require.NoError(t, err)
	require.Len(t, rt, int(db.config.PartitionCount))
	for _, route := range rt {
		require.Len(t, route.PrimaryOwners, 1)
		require.Equal(t, db.rt.This().String(), route.PrimaryOwners[0])
		require.Len(t, route.ReplicaOwners, 0)
	}
}
