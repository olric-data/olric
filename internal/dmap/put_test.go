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
	"encoding/hex"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDMap_Put_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		gr, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_Put_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()

	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		gr, err := dm2.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_Put_AsyncReplicationMode(t *testing.T) {
	cluster := testcluster.New(NewService)
	// Create DMap services with custom configuration
	c1 := testutil.NewConfig()
	c1.ReplicationMode = config.AsyncReplicationMode
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReplicationMode = config.AsyncReplicationMode
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()

	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	// Wait some time for async replication
	<-time.After(100 * time.Millisecond)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		gr, err := dm2.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_Put_WriteQuorum(t *testing.T) {
	cluster := testcluster.New(NewService)
	// Create DMap services with custom configuration
	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.WriteQuorum = 2
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	var hit bool
	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key := testutil.ToKey(i)

		hkey := partitions.HKey(dm.name, key)
		host := dm.s.primary.PartitionByHKey(hkey).Owner()
		if s1.rt.This().CompareByID(host) {
			err = dm.Put(ctx, key, testutil.ToVal(i), nil)
			if err != ErrWriteQuorum {
				t.Fatalf("Expected ErrWriteQuorum. Got: %v", err)
			}
			hit = true
		}
	}
	if !hit {
		t.Fatalf("No keys checked on %v", s1)
	}
}

func TestDMap_Put_PX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasPX: true,
		PX:    time.Millisecond,
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(10 * time.Millisecond)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := dm2.Get(ctx, testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Put_NX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	pc := &PutConfig{
		HasNX: true,
	}
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i*2), pc)
		if err == ErrKeyFound {
			err = nil
		}
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		gr, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_Put_XX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasXX: true,
	}
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i*2), pc)
		if errors.Is(err, ErrKeyNotFound) {
			err = nil
		}
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		_, err = dm.Get(ctx, testutil.ToKey(i))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Put_EX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasEX: true,
		EX:    time.Second / 4,
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(time.Second)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := dm2.Get(ctx, testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Put_EXAT(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasEXAT: true,
		EXAT:    time.Duration(time.Now().Add(time.Second).UnixNano()),
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(time.Second)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := dm2.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestDMap_Put_PXAT(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasPXAT: true,
		PXAT:    time.Duration(time.Now().Add(time.Millisecond).UnixNano()),
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(10 * time.Millisecond)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := dm2.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestDMap_Put_ErrKeyTooLarge(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	data := make([]byte, 300)
	_, err = rand.Read(data)
	require.NoError(t, err)
	key := hex.EncodeToString(data)
	err = dm.Put(ctx, key, "value", nil)
	require.ErrorIs(t, err, ErrKeyTooLarge)
}

func TestDMap_Put_ErrEntryTooLarge(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	data := make([]byte, 1<<21)
	_, err = rand.Read(data)
	require.NoError(t, err)

	err = dm.Put(ctx, "key", data, nil)
	require.ErrorIs(t, err, ErrEntryTooLarge)
}
