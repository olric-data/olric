// Copyright 2018-2026 The Olric Authors
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
	"net"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/discovery"

	"github.com/olric-data/olric/config"
	"github.com/olric-data/olric/internal/testcluster"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDMap_Eviction_TTL(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	pc := &PutConfig{
		HasEX: true,
		EX:    time.Millisecond,
	}
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(time.Millisecond)
	for i := 0; i < 100; i++ {
		s1.evictKeys()
		s2.evictKeys()
	}

	length := 0
	for _, ins := range []*Service{s1, s2} {
		for partID := uint64(0); partID < s1.config.PartitionCount; partID++ {
			part := ins.primary.PartitionByID(partID)
			part.Map().Range(func(k, v interface{}) bool {
				f := v.(*fragment)
				length += f.storage.Stats().Length
				return true
			})
		}
	}
	require.NotEqual(t, 100, length)
}

func TestDMap_Eviction_Config_TTLDuration(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps = &config.DMaps{
		TTLDuration: time.Duration(0.1 * float64(time.Second)),
		Engine:      config.NewEngine(),
	}
	require.NoError(t, c.DMaps.Engine.Sanitize())

	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	<-time.After(200 * time.Millisecond)
	for i := 0; i < 100; i++ {
		s.evictKeys()
	}

	length := 0
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := s.primary.PartitionByID(partID)
		part.Map().Range(func(k, v interface{}) bool {
			f := v.(*fragment)
			length += f.storage.Stats().Length
			return true
		})
	}
	require.NotEqual(t, 100, length)
}

func TestDMap_Eviction_Config_MaxIdleDuration(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps = &config.DMaps{
		MaxIdleDuration: 100 * time.Millisecond,
		Engine:          config.NewEngine(),
	}
	require.NoError(t, c.DMaps.Engine.Sanitize())

	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	<-time.After(150 * time.Millisecond)
	for i := 0; i < 100; i++ {
		s.evictKeys()
	}

	length := 0
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := s.primary.PartitionByID(partID)
		part.Map().Range(func(k, v interface{}) bool {
			f := v.(*fragment)
			length += f.storage.Stats().Length
			return true
		})
	}

	require.NotEqual(t, 100, length)
}

func TestDMap_Eviction_LRU_Config_MaxKeys(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps = &config.DMaps{
		MaxKeys:        70,
		EvictionPolicy: config.LRUEviction,
		Engine:         config.NewEngine(),
	}
	require.NoError(t, c.DMaps.Engine.Sanitize())

	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}
	length := 0
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := s.primary.PartitionByID(partID)
		part.Map().Range(func(k, v interface{}) bool {
			f := v.(*fragment)
			length += f.storage.Stats().Length
			return true
		})
	}

	require.NotEqual(t, 100, length)
}

func TestDMap_Eviction_LRU_Config_MaxInuse(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps = &config.DMaps{
		MaxInuse:       2048,
		EvictionPolicy: config.LRUEviction,
		Engine:         testutil.NewEngineConfig(t),
	}

	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}
	length := 0
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := s.primary.PartitionByID(partID)
		part.Map().Range(func(k, v interface{}) bool {
			f := v.(*fragment)
			length += f.storage.Stats().Length
			return true
		})
	}

	require.NotEqual(t, 100, length)
}

// A previous owner that stops answering must not be able to pin the fragment
// lock. scanFragmentForEviction contacts previous owners for every expired key,
// and holding the fragment lock across those calls blocks Partition.Length,
// which is what prepareLeftOverDataReport walks before a node can apply a new
// routing table. One departed owner then freezes routing updates cluster-wide.
func TestDMap_Eviction_DoesNotHoldFragmentLockDuringClusterDelete(t *testing.T) {
	// Accepts connections and then stays silent, so every delete sent here
	// costs a full read timeout instead of failing fast like a closed port.
	blackhole, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer blackhole.Close()
	go func() {
		var held []net.Conn
		defer func() {
			for _, c := range held {
				_ = c.Close()
			}
		}()
		for {
			conn, err := blackhole.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
		}
	}()

	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	pc := &PutConfig{HasEX: true, EX: time.Millisecond}
	for i := 0; i < 200; i++ {
		require.NoError(t, dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc))
	}

	// Work on whichever partition ended up with the most keys, so the eviction
	// scan has plenty of expired entries to walk.
	var part *partitions.Partition
	var f *fragment
	best := 0
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		p := s.primary.PartitionByID(partID)
		if n := p.Length(); n > best {
			candidate, err := dm.loadFragment(p)
			if err != nil {
				continue
			}
			best, part, f = n, p, candidate
		}
	}
	require.NotNil(t, part, "no partition holds any keys")

	// Put the silent listener ahead of the real owner. deleteFromPreviousOwners
	// walks everything except the last entry, so this is the departed node.
	dead := discovery.Member{Name: blackhole.Addr().String(), ID: 1, Birthdate: 1}
	part.SetOwners(append([]discovery.Member{dead}, part.Owners()...))

	<-time.After(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.scanFragmentForEviction(part.ID(), "mydmap", f)
	}()

	// Poll for as long as eviction runs and record the worst single Length call.
	// Length takes the fragment lock, so if eviction holds that lock across its
	// deletes this spikes to multiples of the client read timeout.
	var worst time.Duration
	deadline := time.After(20 * time.Second)
poll:
	for {
		select {
		case <-done:
			break poll
		case <-deadline:
			break poll
		default:
		}
		begin := time.Now()
		part.Length()
		if d := time.Since(begin); d > worst {
			worst = d
		}
		<-time.After(5 * time.Millisecond)
	}

	t.Logf("worst Partition.Length latency during eviction: %s", worst)
	require.Less(t, worst, time.Second,
		"Partition.Length blocked behind eviction's cluster delete: the fragment lock is held across network IO")
}

// Eviction decides a key is expired, releases the fragment lock, then talks to
// the previous owners and backups before it can remove the entry. A writer that
// revives the key inside that window must win: deleting the fresh value would
// silently reset a rate limiter counter. This drives deleteExpiredKey with an
// expiry decision that has gone stale, which is exactly what the window yields.
func TestDMap_Eviction_DoesNotDeleteKeyRevivedDuringClusterDelete(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	key := testutil.ToKey(1)
	require.NoError(t, dm.Put(ctx, key, testutil.ToVal(1), &PutConfig{HasEX: true, EX: time.Millisecond}))

	hkey := partitions.HKey(dm.name, key)
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	require.NoError(t, err)

	// The scan would mark it dead here.
	<-time.After(10 * time.Millisecond)
	ttl, err := f.storage.GetTTL(hkey)
	require.NoError(t, err)
	require.True(t, isKeyExpired(ttl), "key should be expired at the point eviction picks it")

	// A request revives it while eviction is still on the network.
	require.NoError(t, dm.Put(ctx, key, testutil.ToVal(42), &PutConfig{HasEX: true, EX: time.Hour}))

	// Eviction now completes with its stale decision.
	deleted, err := dm.deleteExpiredKey(hkey, key, f)
	require.NoError(t, err)
	require.False(t, deleted, "eviction removed a key that was revived after it was picked")

	got, err := dm.Get(ctx, key)
	require.NoError(t, err, "eviction deleted a key that was revived while it was on the network")
	require.Equal(t, testutil.ToVal(42), got.Value())
}
