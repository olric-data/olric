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
	"errors"
	"fmt"
	"github.com/buraksezer/olric/internal/kvstore"
	"github.com/buraksezer/olric/pkg/storage"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func checkEmptyStorageEngine(t *testing.T, s *Service) {
	maximum := 50
	check := func(current int) (bool, error) {
		for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
			part := s.primary.PartitionByID(partID)
			tmp, ok := part.Map().Load("dmap.mymap")
			if !ok {
				continue
			}

			f := tmp.(*fragment)
			f.RLock()
			numTables := f.storage.Stats().NumTables
			f.RUnlock()

			if numTables != 1 && current < maximum-1 {
				return false, nil
			}
			if numTables != 1 && current >= maximum-1 {
				return false, fmt.Errorf("numTables=%d PartID: %d", numTables, partID)
			}
		}
		return true, nil
	}

	for i := 0; i < maximum; i++ {
		done, err := check(i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if done {
			return
		}
		<-time.After(100 * time.Millisecond)
	}
	t.Fatalf("Failed to control compaction status")
}

func TestDMap_Delete_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	dm2, err := s2.NewDMap("mymap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = dm2.Delete(ctx, testutil.ToKey(i))
		require.NoError(t, err)

		_, err = dm2.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestDMap_Delete_Lookup(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	s3 := cluster.AddMember(nil).(*Service)

	dm2, err := s3.NewDMap("mymap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = dm2.Delete(ctx, testutil.ToKey(i))
		require.NoError(t, err)

		_, err = dm2.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestDMap_Delete_StaleFragments(t *testing.T) {
	cluster := testcluster.New(NewService)
	c1 := testutil.NewConfig()
	c1.DMaps.CheckEmptyFragmentsInterval = time.Millisecond
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.DMaps.CheckEmptyFragmentsInterval = time.Millisecond
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)

	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := s2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err = dm2.Delete(ctx, testutil.ToKey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		_, err = dm2.Get(ctx, testutil.ToKey(i))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}

	s1.wg.Add(1)
	go s1.janitorWorker()
	s2.wg.Add(1)
	go s2.janitorWorker()

	var dc int32
	for i := 0; i < 1000; i++ {
		dc = 0
		for partID := uint64(0); partID < s1.config.PartitionCount; partID++ {
			for _, instance := range []*Service{s1, s2} {
				part := instance.primary.PartitionByID(partID)
				part.Map().Range(func(name, dm interface{}) bool { dc++; return true })

				bpart := instance.backup.PartitionByID(partID)
				bpart.Map().Range(func(name, dm interface{}) bool { dc++; return true })
			}
		}
		if dc == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if dc != 0 {
		t.Fatalf("Expected dmap count is 0. Got: %d", dc)
	}
}

func TestDMap_Delete_PreviousOwner(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put(context.Background(), "mykey", "myvalue", nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	cmd := protocol.NewDelEntry("mydmap", "mykey").Command(context.Background())
	rc := s.client.Get(s.rt.This().String())
	err = rc.Process(context.Background(), cmd)
	require.NoError(t, err)
	require.NoError(t, cmd.Err())

	_, err = dm.Get(context.Background(), "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMap_Delete_DeleteKeyValFromPreviousOwners(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put(context.Background(), "mykey", "myvalue", nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Prepare fragmented partition owners list
	hkey := partitions.HKey("mydmap", "mykey")
	owners := s.primary.PartitionOwnersByHKey(hkey)
	owner := owners[len(owners)-1]

	var data []discovery.Member
	for _, member := range s.rt.Discovery().GetMembers() {
		if member.CompareByID(owner) {
			continue
		}
		data = append(data, member)
	}
	// this has to be the last one
	data = append(data, owner)
	err = dm.deleteFromPreviousOwners("mykey", data)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMap_Delete_Backup(t *testing.T) {
	cluster := testcluster.New(NewService)

	c1 := testutil.NewConfig()
	c1.ReadRepair = true
	c1.ReplicaCount = 2
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReadRepair = true
	c2.ReplicaCount = 2
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)

	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := s2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 10; i++ {
		_, err = dm2.Delete(ctx, testutil.ToKey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		_, err = dm2.Get(ctx, testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Delete_Compaction(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.ReadRepair = true
	c.ReplicaCount = 2
	c.DMaps.TriggerCompactionInterval = time.Millisecond
	c.DMaps.Engine.Name = config.DefaultStorageEngine

	c.DMaps.Engine.Config = map[string]interface{}{
		"tableSize":           uint64(100), // overwrite tableSize to trigger compaction.
		"maxIdleTableTimeout": time.Millisecond,
	}

	kv, err := kvstore.New(storage.NewConfig(c.DMaps.Engine.Config))
	require.NoError(t, err)
	c.DMaps.Engine.Implementation = kv

	e := testcluster.NewEnvironment(c)

	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		_, err = dm.Delete(ctx, testutil.ToKey(i))
		require.NoError(t, err)

		_, err = dm.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
	checkEmptyStorageEngine(t, s)
}
