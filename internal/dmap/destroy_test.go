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
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDMap_Destroy_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	err = dm.Destroy(ctx)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err = dm.Get(ctx, testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Destroy_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	e1 := testcluster.NewEnvironment(c1)
	s := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	e2 := testcluster.NewEnvironment(c2)
	cluster.AddMember(e2)

	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	err = dm.Destroy(ctx)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err = dm.Get(ctx, testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Destroy_destroyOperation(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	cmd := protocol.NewDestroy("mydmap").Command(s.ctx)
	rc := s.client.Get(s.rt.This().String())
	err = rc.Process(s.ctx, cmd)
	require.NoError(t, err)
	require.NoError(t, cmd.Err())

	for i := 0; i < 100; i++ {
		_, err = dm.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}
