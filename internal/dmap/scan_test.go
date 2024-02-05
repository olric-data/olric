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
	"fmt"
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func testScanIterator(t *testing.T, s *Service, allKeys map[string]bool, sc *ScanConfig) int {
	if sc == nil {
		sc = &ScanConfig{}
	}
	ctx := context.Background()
	rc := s.client.Get(s.rt.This().String())

	var totalKeys int
	var partID, cursor uint64
	for {
		r := protocol.NewScan(partID, "mydmap", cursor)
		if sc.Replica {
			r.SetReplica()
		}
		if sc.HasMatch {
			r.SetMatch(sc.Match)
		}
		cmd := r.Command(ctx)
		err := rc.Process(ctx, cmd)
		require.NoError(t, err)

		var keys []string
		keys, cursor, err = cmd.Result()
		require.NoError(t, err)
		totalKeys += len(keys)

		for _, key := range keys {
			_, ok := allKeys[key]
			require.True(t, ok)
			allKeys[key] = true
		}
		if cursor == 0 {
			if partID+1 < s.config.PartitionCount {
				partID++
				continue
			}
			break
		}
	}
	return totalKeys
}

func TestDMap_scanCommandHandler_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	allKeys := make(map[string]bool)

	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), i, nil)
		require.NoError(t, err)

		allKeys[testutil.ToKey(i)] = false
	}

	totalKeys := testScanIterator(t, s, allKeys, nil)
	require.Equal(t, 100, totalKeys)
	for _, value := range allKeys {
		require.True(t, value)
	}
}

func TestDMap_scanCommandHandler_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.WriteQuorum = 2
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	c1.WriteQuorum = 2
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)

	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	allKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), i, nil)
		require.NoError(t, err)

		allKeys[testutil.ToKey(i)] = false
	}

	t.Run("Scan on primary copies", func(t *testing.T) {
		var totalKeys int
		totalKeys += testScanIterator(t, s1, allKeys, nil)
		totalKeys += testScanIterator(t, s2, allKeys, nil)

		require.Equal(t, 100, totalKeys)
		for _, value := range allKeys {
			require.True(t, value)
		}
	})

	t.Run("Scan on replicas", func(t *testing.T) {
		var totalKeys int
		sc := &ScanConfig{Replica: true}
		totalKeys += testScanIterator(t, s1, allKeys, sc)
		totalKeys += testScanIterator(t, s2, allKeys, sc)

		require.Equal(t, 100, totalKeys)
		for _, value := range allKeys {
			require.True(t, value)
		}
	})
}

func TestDMap_scanCommandHandler_match(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	evenKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		var key string
		if i%2 == 0 {
			key = fmt.Sprintf("even:%s", testutil.ToKey(i))
			evenKeys[key] = false
		} else {
			key = fmt.Sprintf("odd:%s", testutil.ToKey(i))
		}
		err = dm.Put(ctx, key, i, nil)
		require.NoError(t, err)
	}

	sc := &ScanConfig{
		HasMatch: true,
		Match:    "^even:",
	}
	totalKeys := testScanIterator(t, s, evenKeys, sc)
	require.Equal(t, 50, totalKeys)
	for _, value := range evenKeys {
		require.True(t, value)
	}
}

func TestDMap_scanCommandHandler_count(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), i, nil)
		require.NoError(t, err)
	}

	rc := s.client.Get(s.rt.This().String())
	var partID, cursor uint64
	r := protocol.NewScan(partID, "mydmap", cursor)
	r.SetCount(5)
	cmd := r.Command(ctx)
	err = rc.Process(ctx, cmd)
	require.NoError(t, err)

	var keys []string
	keys, _, err = cmd.Result()
	require.NoError(t, err)
	require.Len(t, keys, 5)
}
