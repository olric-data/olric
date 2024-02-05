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

package dmap

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/events"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestDMap_Balance_Invalid_PartID(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	fp := &fragmentPack{
		PartID:  12312,
		Kind:    partitions.PRIMARY,
		Name:    "foobar",
		Payload: nil,
	}
	err := s.validateFragmentPack(fp)
	require.Error(t, err)
}

func TestDMap_Balance_FragmentMergeFunction(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	require.NoError(t, err)

	err = dm.Put(context.Background(), "mykey", "myval", nil)
	require.NoError(t, err)

	hkey := partitions.HKey("mymap", "mykey")
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	require.NoError(t, err)

	currentValue := []byte("current-value")
	e := dm.engine.NewEntry()
	e.SetKey("mykey")
	e.SetTimestamp(time.Now().UnixNano())
	e.SetValue(currentValue)

	err = dm.fragmentMergeFunction(f, hkey, e)
	require.NoError(t, err)

	winner, err := f.storage.Get(hkey)
	require.NoError(t, err)
	require.Equal(t, currentValue, winner.Value())
}

func TestDMap_Balancer_JoinNewNode(t *testing.T) {
	cluster := testcluster.New(NewService)
	db1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := db1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	var totalKeys = 1000
	for i := 0; i < totalKeys; i++ {
		key := "balancer-test." + strconv.Itoa(i)
		err = dm.Put(ctx, key, testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	// This is an integration test. Here we try to observe the behavior of
	// balancer with the DMap service.

	db2 := cluster.AddMember(nil).(*Service) // This automatically syncs the cluster.

	var db1TotalKeys int
	for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
		part := db1.primary.PartitionByID(partID)
		db1TotalKeys += part.Length()
	}
	require.Less(t, db1TotalKeys, totalKeys)

	var db2TotalKeys int
	for partID := uint64(0); partID < db2.config.PartitionCount; partID++ {
		part := db2.primary.PartitionByID(partID)
		db2TotalKeys += part.Length()
	}
	require.Less(t, db2TotalKeys, totalKeys)

	require.Equal(t, totalKeys, db1TotalKeys+db2TotalKeys)
}

func TestDMap_Balancer_WrongOwnership(t *testing.T) {
	cluster := testcluster.New(NewService)
	db1 := cluster.AddMember(nil).(*Service)
	db2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var id uint64
	for partID := uint64(0); partID < db2.config.PartitionCount; partID++ {
		part := db2.primary.PartitionByID(partID)
		if part.Owner().CompareByID(db2.rt.This()) {
			id = part.ID()
			break
		}
	}
	fp := &fragmentPack{
		PartID: id,
		Kind:   partitions.PRIMARY,
	}
	// invalid argument: partID: 1 (kind: Primary) doesn't belong to 127.0.0.1:62096
	require.Error(t, db1.validateFragmentPack(fp))
}

func TestDMap_Balancer_ClusterEvents(t *testing.T) {
	c1 := testutil.NewConfig()
	c1.TriggerBalancerInterval = time.Millisecond
	c1.EnableClusterEventsChannel = true
	e1 := testcluster.NewEnvironment(c1)

	cluster := testcluster.New(NewService)
	db1 := cluster.AddMember(e1).(*Service)
	defer cluster.Shutdown()

	result := make(chan string, 1)
	db1.server.ServeMux().HandleFunc(protocol.PubSub.Publish, func(conn redcon.Conn, cmd redcon.Command) {
		publishCmd, err := protocol.ParsePublishCommand(cmd)
		require.NoError(t, err)
		require.Equal(t, events.ClusterEventsChannel, publishCmd.Channel)

		result <- publishCmd.Message

		conn.WriteInt(1)
	})

	dm, err := db1.NewDMap("mymap")
	require.NoError(t, err)

	var totalKeys = 1000
	for i := 0; i < totalKeys; i++ {
		key := "balancer-test." + strconv.Itoa(i)
		err = dm.Put(context.Background(), key, testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	go func() {
		c2 := testutil.NewConfig()
		c1.TriggerBalancerInterval = time.Millisecond
		c2.EnableClusterEventsChannel = true
		e2 := testcluster.NewEnvironment(c2)
		s2 := testutil.NewServer(c2)
		s2.ServeMux().HandleFunc(protocol.PubSub.Publish, func(conn redcon.Conn, cmd redcon.Command) {
			publishCmd, err := protocol.ParsePublishCommand(cmd)
			require.NoError(t, err)
			require.Equal(t, events.ClusterEventsChannel, publishCmd.Channel)

			result <- publishCmd.Message

			conn.WriteInt(1)
		})
		e2.Set("server", s2)
		cluster.AddMember(e2)
	}()

	fragmentEvents := make(map[uint64]map[string]struct{})
L:
	for {
		select {
		case msg := <-result:
			value := make(map[string]interface{})
			err = json.Unmarshal([]byte(msg), &value)
			require.NoError(t, err)

			kind := value["kind"].(string)
			if kind == events.KindFragmentMigrationEvent || kind == events.KindFragmentReceivedEvent {
				partID := uint64(value["partition_id"].(float64))
				ev, ok := fragmentEvents[partID]
				if ok {
					ev[kind] = struct{}{}
				} else {
					fragmentEvents[partID] = map[string]struct{}{kind: {}}
				}
			}
		case <-time.After(time.Second):
			break L
		}
	}

	for partID, data := range fragmentEvents {
		require.Len(t, data, 2)
		part := db1.primary.PartitionByID(partID)
		// Transferred to db2
		require.NotEqual(t, part.Owner().ID, db1.rt.This().ID)
	}
}
