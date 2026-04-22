// Copyright 2018-2025 The Olric Authors
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
	"runtime"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestEmbeddedClient_NewDMap(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	_, err := e.NewDMap("mydmap")
	require.NoError(t, err)
}

func TestEmbeddedClient_DMap_Put(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(context.Background(), "mykey", "myvalue")
	require.NoError(t, err)
}

func TestEmbeddedClient_DMap_Put_EX(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", EX(time.Second))
	require.NoError(t, err)

	<-time.After(time.Second)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestEmbeddedClient_DMap_Put_PX(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", PX(time.Millisecond))
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestEmbeddedClient_DMap_Put_EXAT(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", EXAT(time.Duration(time.Now().Add(time.Second).UnixNano())))
	require.NoError(t, err)

	<-time.After(time.Second)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestEmbeddedClient_DMap_Put_PXAT(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", PXAT(time.Duration(time.Now().Add(time.Millisecond).UnixNano())))
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestEmbeddedClient_DMap_Put_NX(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", NX())
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	_, err = dm.Get(ctx, "mykey")
	require.NoError(t, err)
}

func TestEmbeddedClient_DMap_Put_XX(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", XX())
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestEmbeddedClient_DMap_Get(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(context.Background(), "mykey", "myvalue")
	require.NoError(t, err)

	gr, err := dm.Get(context.Background(), "mykey")
	require.NoError(t, err)

	value, err := gr.String()
	require.NoError(t, err)
	require.Equal(t, "myvalue", value)
}

func TestEmbeddedClient_DMap_Delete(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(context.Background(), "mykey", "myvalue")
	require.NoError(t, err)

	count, err := dm.Delete(context.Background(), "mykey")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = dm.Get(context.Background(), "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestEmbeddedClient_DMap_Delete_Many_Keys(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	var keys []string
	for i := 0; i < 10; i++ {
		key := testutil.ToKey(i)
		err = dm.Put(context.Background(), key, "myvalue")
		require.NoError(t, err)
		keys = append(keys, key)
	}

	count, err := dm.Delete(context.Background(), keys...)
	require.NoError(t, err)
	require.Equal(t, 10, count)
}

func TestEmbeddedClient_DMap_Atomic_Incr(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		errGr.Go(func() error {
			_, err = dm.Incr(ctx, "mykey", 1)
			return err
		})
	}
	require.NoError(t, errGr.Wait())

	gr, err := dm.Get(context.Background(), "mykey")
	res, err := gr.Int()
	require.NoError(t, err)
	require.Equal(t, 100, res)
}

func TestEmbeddedClient_DMap_Atomic_Decr(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	err = dm.Put(ctx, "mykey", 100)
	require.NoError(t, err)

	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		errGr.Go(func() error {
			_, err = dm.Decr(ctx, "mykey", 1)
			return err
		})
	}
	require.NoError(t, errGr.Wait())

	gr, err := dm.Get(context.Background(), "mykey")
	res, err := gr.Int()
	require.NoError(t, err)
	require.Equal(t, 0, res)
}

func TestEmbeddedClient_DMap_GetPut(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	gr, err := dm.GetPut(context.Background(), "mykey", "myvalue")
	require.NoError(t, err)

	_, err = gr.String()
	require.ErrorIs(t, err, ErrNilResponse)

	gr, err = dm.GetPut(context.Background(), "mykey", "myvalue-2")
	require.NoError(t, err)

	value, err := gr.String()
	require.NoError(t, err)
	require.Equal(t, "myvalue", value)
}

func TestEmbeddedClient_DMap_Atomic_IncrByFloat(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		errGr.Go(func() error {
			_, err = dm.IncrByFloat(ctx, "mykey", 1.2)
			return err
		})
	}
	require.NoError(t, errGr.Wait())

	gr, err := dm.Get(context.Background(), "mykey")
	res, err := gr.Float64()
	require.NoError(t, err)
	require.Equal(t, 120.0000000000002, res)
}

func TestEmbeddedClient_DMap_Expire(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)

	err = dm.Expire(ctx, "mykey", time.Millisecond)
	require.NoError(t, err)

	<-time.After(2 * time.Millisecond)

	_, err = dm.Get(context.Background(), "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestEmbeddedClient_DMap_Destroy(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	err = dm.Destroy(ctx)
	require.NoError(t, err)

	// Destroy is an async command. Wait for some time to see its effect.
	<-time.After(100 * time.Millisecond)

	stats, err := e.Stats(ctx, e.db.rt.This().String())
	require.NoError(t, err)
	var total int
	for _, part := range stats.Partitions {
		total += part.Length
	}
	require.Greater(t, 100, total)
}

func TestEmbeddedClient_DMap_Lock(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := "lock.key.test"

	lx, err := dm.Lock(ctx, key, time.Second)
	require.NoError(t, err)

	err = lx.Unlock(ctx)
	require.NoError(t, err)
}

func TestEmbeddedClient_DMap_Lock_ErrLockNotAcquired(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := "lock.key.test"

	_, err = dm.Lock(ctx, key, time.Second)
	require.NoError(t, err)

	_, err = dm.Lock(ctx, key, time.Millisecond)
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestEmbeddedClient_DMap_Lock_ErrNoSuchLock(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := "lock.key.test"

	lx, err := dm.Lock(ctx, key, time.Second)
	require.NoError(t, err)

	err = lx.Unlock(ctx)
	require.NoError(t, err)

	err = lx.Unlock(ctx)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestEmbeddedClient_DMap_LockWithTimeout(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := "lock.key.test"

	lx, err := dm.LockWithTimeout(ctx, key, 5*time.Second, time.Second)
	require.NoError(t, err)

	err = lx.Unlock(ctx)
	require.NoError(t, err)
}

func TestEmbeddedClient_DMap_LockWithTimeout_Timeout(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := "lock.key.test"

	lx, err := dm.LockWithTimeout(ctx, key, time.Millisecond, time.Second)
	require.NoError(t, err)

	<-time.After(2 * time.Millisecond)

	err = lx.Unlock(ctx)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestEmbeddedClient_DMap_LockWithTimeout_ErrLockNotAcquired(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := "lock.key.test"

	_, err = dm.LockWithTimeout(ctx, key, 10*time.Second, time.Second)
	require.NoError(t, err)

	_, err = dm.LockWithTimeout(ctx, key, 10*time.Second, time.Millisecond)
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestEmbeddedClient_DMap_LockWithTimeout_ErrNoSuchLock(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := "lock.key.test"

	lx, err := dm.LockWithTimeout(ctx, key, time.Second, time.Second)
	require.NoError(t, err)

	err = lx.Unlock(ctx)
	require.NoError(t, err)

	err = lx.Unlock(ctx)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestEmbeddedClient_DMap_LockWithTimeout_ErrNoSuchLock_Timeout(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := "lock.key.test"

	lx, err := dm.LockWithTimeout(ctx, key, time.Millisecond, time.Second)
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	err = lx.Unlock(ctx)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestEmbeddedClient_DMap_LockWithTimeout_Then_Lease(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := "lock.key.test"

	lx, err := dm.LockWithTimeout(ctx, key, 50*time.Millisecond, time.Second)
	require.NoError(t, err)

	// Expand its timeout value
	err = lx.Lease(ctx, time.Hour)
	require.NoError(t, err)

	<-time.After(100 * time.Millisecond)

	_, err = dm.Lock(ctx, key, time.Millisecond)
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestEmbeddedClient_RoutingTable_Standalone(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	rt, err := e.RoutingTable(context.Background())
	require.NoError(t, err)
	require.Len(t, rt, int(db.config.PartitionCount))
	for _, route := range rt {
		require.Len(t, route.PrimaryOwners, 1)
		require.Equal(t, db.rt.This().String(), route.PrimaryOwners[0])
		require.Len(t, route.ReplicaOwners, 0)
	}
}

func TestEmbeddedClient_RoutingTable_Cluster(t *testing.T) {
	cluster := newTestOlricCluster(t)

	cluster.addMember(t) // Cluster coordinator
	<-time.After(250 * time.Millisecond)

	cluster.addMember(t)
	db2 := cluster.addMember(t)

	e := db2.NewEmbeddedClient()
	rt, err := e.RoutingTable(context.Background())
	require.NoError(t, err)
	require.Len(t, rt, int(db2.config.PartitionCount))
	owners := make(map[string]struct{})
	for _, route := range rt {
		for _, owner := range route.PrimaryOwners {
			owners[owner] = struct{}{}
		}
	}
	require.Len(t, owners, 3)
}

func TestEmbeddedClient_Member(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)
	cluster.addMember(t)

	e := db.NewEmbeddedClient()
	members, err := e.Members(context.Background())
	require.NoError(t, err)
	require.Len(t, members, 2)
	coordinator := db.rt.Discovery().GetCoordinator()
	for _, member := range members {
		require.NotEqual(t, "", member.Name)
		require.NotEqual(t, 0, member.ID)
		require.NotEqual(t, 0, member.Birthdate)
		if coordinator.ID == member.ID {
			require.True(t, member.Coordinator)
		} else {
			require.False(t, member.Coordinator)
		}
	}
}

func TestEmbeddedClient_Ping(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	ctx := context.Background()
	response, err := e.Ping(ctx, db.rt.This().String(), "")
	require.NoError(t, err)
	require.Equal(t, DefaultPingResponse, response)
}

func TestEmbeddedClient_Ping_WithMessage(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	ctx := context.Background()
	message := "Olric is the best"
	response, err := e.Ping(ctx, db.rt.This().String(), message)
	require.NoError(t, err)
	require.Equal(t, message, response)
}

func TestEmbeddedClient_DMap_Put_PX_With_NX(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db0 := cluster.addMember(t)
	db1 := cluster.addMember(t)

	ctx := context.Background()
	e := db0.NewEmbeddedClient()
	dm0, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm0.Put(ctx, "mykey", "myvalue", PX(time.Minute), NX())
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	e = db1.NewEmbeddedClient()
	dm1, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	gr, err := dm1.Get(ctx, "mykey")
	require.NoError(t, err)
	assert.NotZero(t, gr.TTL())
}

// TestEmbeddedClient_DMap_CompareAndSwap_Standalone exercises basic CAS flow on
// a single-node embedded cluster.
func TestEmbeddedClient_DMap_CompareAndSwap_Standalone(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()

	// NX-style: insert when absent.
	swapped, current, err := dm.CompareAndSwap(ctx, "key", nil, "v1")
	require.NoError(t, err)
	require.True(t, swapped)
	require.Nil(t, current)

	// NX-style on existing key fails.
	swapped, current, err = dm.CompareAndSwap(ctx, "key", nil, "v2")
	require.NoError(t, err)
	require.False(t, swapped)
	require.NotNil(t, current)
	v, err := current.String()
	require.NoError(t, err)
	require.Equal(t, "v1", v)

	// Proper update using observed raw bytes.
	gr, err := dm.Get(ctx, "key")
	require.NoError(t, err)
	swapped, current, err = dm.CompareAndSwap(ctx, "key", gr.RawValue(), "v2")
	require.NoError(t, err)
	require.True(t, swapped)
	require.Nil(t, current)

	gr, err = dm.Get(ctx, "key")
	require.NoError(t, err)
	v, err = gr.String()
	require.NoError(t, err)
	require.Equal(t, "v2", v)
}

// TestEmbeddedClient_DMap_CompareAndSwap_CrossNode validates that a CAS from
// one node is immediately observed by other nodes and that mismatches are
// reported correctly. This is the core cluster-wide CAS contract the gateway
// relies on.
func TestEmbeddedClient_DMap_CompareAndSwap_CrossNode(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db0 := cluster.addMember(t)
	db1 := cluster.addMember(t)

	// Give the cluster a brief moment to stabilize.
	time.Sleep(250 * time.Millisecond)

	e0 := db0.NewEmbeddedClient()
	dm0, err := e0.NewDMap("mydmap")
	require.NoError(t, err)

	e1 := db1.NewEmbeddedClient()
	dm1, err := e1.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()

	// Node 0 inserts.
	swapped, _, err := dm0.CompareAndSwap(ctx, "shared", nil, "from-node-0")
	require.NoError(t, err)
	require.True(t, swapped)

	// Node 1 reads and sees it.
	gr, err := dm1.Get(ctx, "shared")
	require.NoError(t, err)
	v, err := gr.String()
	require.NoError(t, err)
	require.Equal(t, "from-node-0", v)

	// Node 1 does a conflicting CAS with stale nil-expected; must fail and
	// return the current value.
	swapped, current, err := dm1.CompareAndSwap(ctx, "shared", nil, "from-node-1")
	require.NoError(t, err)
	require.False(t, swapped)
	require.NotNil(t, current)
	v, err = current.String()
	require.NoError(t, err)
	require.Equal(t, "from-node-0", v)

	// Node 1 retries with the observed bytes and wins.
	swapped, _, err = dm1.CompareAndSwap(ctx, "shared", gr.RawValue(), "from-node-1")
	require.NoError(t, err)
	require.True(t, swapped)

	// Node 0 sees the new value.
	gr, err = dm0.Get(ctx, "shared")
	require.NoError(t, err)
	v, err = gr.String()
	require.NoError(t, err)
	require.Equal(t, "from-node-1", v)
}

// TestEmbeddedClient_DMap_CompareAndSwap_ConcurrentTwoNodes hammers the same
// key with N goroutines from each of two nodes, each doing a read-modify-write
// via CAS-with-retry. Final value must equal the exact number of increments
// — proving CAS serializes correctly across nodes.
func TestEmbeddedClient_DMap_CompareAndSwap_ConcurrentTwoNodes(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db0 := cluster.addMember(t)
	db1 := cluster.addMember(t)

	time.Sleep(250 * time.Millisecond)

	e0 := db0.NewEmbeddedClient()
	dm0, err := e0.NewDMap("mydmap")
	require.NoError(t, err)

	e1 := db1.NewEmbeddedClient()
	dm1, err := e1.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()

	// Seed.
	require.NoError(t, dm0.Put(ctx, "counter", 0))
	time.Sleep(50 * time.Millisecond)

	const perSide = 50
	var wg errgroup.Group

	increment := func(dm DMap) error {
		for i := 0; i < perSide; i++ {
			for {
				gr, err := dm.Get(ctx, "counter")
				if err != nil {
					return fmt.Errorf("Get: %w", err)
				}
				cur, err := gr.Int()
				if err != nil {
					return fmt.Errorf("Int: %w", err)
				}
				swapped, _, err := dm.CompareAndSwap(ctx, "counter", gr.RawValue(), cur+1)
				if err != nil {
					return fmt.Errorf("CAS: %w", err)
				}
				if swapped {
					break
				}
			}
		}
		return nil
	}

	wg.Go(func() error { return increment(dm0) })
	wg.Go(func() error { return increment(dm1) })
	require.NoError(t, wg.Wait())

	gr, err := dm0.Get(ctx, "counter")
	require.NoError(t, err)
	final, err := gr.Int()
	require.NoError(t, err)
	require.Equal(t, 2*perSide, final, "cross-node CAS-retry must produce lossless increments")
}

func TestEmbeddedClient_Issue263(t *testing.T) {
	initNumRoutines := runtime.NumGoroutine()

	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	ctx, cancel := context.WithCancel(context.Background())
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	// Create N key-value pairs:
	const N = 100
	for i := range N {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := dm.Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Iterate M times over N keys:
	const M = 100
	for range M {
		iter, err := dm.Scan(ctx)
		require.NoError(t, err)
		for iter.Next() {
			// Do nothing
		}
		iter.Close()
	}

	require.NoError(t, dm.Close(ctx))
	require.NoError(t, e.Close(ctx))
	require.NoError(t, db.Shutdown(ctx))

	cancel()

	assert.Equal(t, initNumRoutines, runtime.NumGoroutine())

	runtime.GC()
	time.Sleep(time.Second)

	s := runtime.MemStats{}
	runtime.ReadMemStats(&s)

	const (
		KB = 1 << 10
		MB = KB << 10
	)

	buf := make([]byte, MB)
	stackSize := runtime.Stack(buf, true)

	t.Logf("Non-freed objects: %d\n", s.Mallocs-s.Frees)
	t.Logf("Mem in use (KB): %d\n", s.HeapAlloc/KB)
	t.Logf("Go-routines remained: %d\n", runtime.NumGoroutine())
	t.Logf("Stack traces:\n%s\n", buf[:stackSize])
}
