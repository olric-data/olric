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

package dmap

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/protocol"
	"github.com/olric-data/olric/internal/resp"
	"github.com/olric-data/olric/internal/testcluster"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestDMap_loadCurrentAtomicInt(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	key := "incr"

	ttlDuration := time.Second * 5
	s.config.DMaps.TTLDuration = time.Second * 5

	dm, err := s.NewDMap("atomic_test")
	require.NoError(t, err)

	_, err = dm.Incr(ctx, key, 1)
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to call Incr: %v", err)
		return
	}

	e := newEnv(ctx)
	e.dmap = dm.name
	e.key = key
	_, ttl, err := dm.loadCurrentAtomicInt(e)
	require.NoError(t, err)

	<-time.After(time.Millisecond * 500)
	require.WithinDuration(t, time.UnixMilli(ttl), time.Now(), ttlDuration)
}

func TestDMap_Atomic_Incr(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var wg sync.WaitGroup
	var start chan struct{}
	key := "incr"

	ctx := context.Background()
	incr := func(dm *DMap) {
		<-start
		defer wg.Done()

		_, err := dm.Incr(ctx, key, 1)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call Incr: %v", err)
			return
		}
	}

	dm, err := s.NewDMap("atomic_test")
	require.NoError(t, err)

	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go incr(dm)
	}
	close(start)
	wg.Wait()

	gr, err := dm.Get(ctx, key)
	require.NoError(t, err)

	var res int
	err = resp.Scan(gr.Value(), &res)
	require.NoError(t, err)
	require.Equal(t, 100, res)
}

func TestDMap_Atomic_Decr(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var wg sync.WaitGroup
	var start chan struct{}
	key := "decr"

	ctx := context.Background()

	decr := func(dm *DMap) {
		<-start
		defer wg.Done()

		_, err := dm.Decr(ctx, key, 1)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call Decr: %v", err)
			return
		}
	}

	dm, err := s.NewDMap("atomic_test")
	require.NoError(t, err)

	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go decr(dm)
	}
	close(start)
	wg.Wait()

	res, err := dm.Get(context.Background(), key)
	require.NoError(t, err)

	var value int
	err = resp.Scan(res.Value(), &value)
	require.NoError(t, err)
	require.Equal(t, -100, value)
}

func TestDMap_Atomic_GetPut(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var total int64
	var wg sync.WaitGroup
	var start chan struct{}
	key := "getput"
	getput := func(dm *DMap, i int) {
		<-start
		defer wg.Done()

		gr, err := dm.GetPut(context.Background(), key, i)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call Decr: %v", err)
			return
		}
		if gr != nil {
			var oldval int
			err = resp.Scan(gr.Value(), &oldval)
			require.NoError(t, err)
			atomic.AddInt64(&total, int64(oldval))
		}
	}

	dm, err := s.NewDMap("atomic_test")
	require.NoError(t, err)

	start = make(chan struct{})
	var final int64
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go getput(dm, i)
		final += int64(i)
	}
	close(start)
	wg.Wait()

	gr, err := dm.Get(context.Background(), key)
	require.NoError(t, err)

	var last int
	err = resp.Scan(gr.Value(), &last)
	require.NoError(t, err)

	atomic.AddInt64(&total, int64(last))
	require.Equal(t, final, atomic.LoadInt64(&total))
}

func TestDMap_Atomic_IncrByFloat(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var wg sync.WaitGroup
	var start chan struct{}
	key := "incrbyfloat"

	ctx := context.Background()
	incrByFloat := func(dm *DMap) {
		<-start
		defer wg.Done()

		_, err := dm.IncrByFloat(ctx, key, 1.2)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call IncrByFloat: %v", err)
			return
		}
	}

	dm, err := s.NewDMap("atomic_test")
	require.NoError(t, err)

	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go incrByFloat(dm)
	}
	close(start)
	wg.Wait()

	gr, err := dm.Get(ctx, key)
	require.NoError(t, err)

	var res float64
	err = resp.Scan(gr.Value(), &res)
	require.NoError(t, err)
	require.Equal(t, 120.0000000000002, res)
}

func TestDMap_incrCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		errGr.Go(func() error {
			cmd := protocol.NewIncr("mydmap", "mykey", 1).Command(context.Background())
			rc := s.client.Get(s.rt.This().String())
			err := rc.Process(context.Background(), cmd)
			if err != nil {
				return err
			}
			_, err = cmd.Result()
			return err
		})
	}
	require.NoError(t, errGr.Wait())

	cmd := protocol.NewGet("mydmap", "mykey").Command(context.Background())
	rc := s.client.Get(s.rt.This().String())
	err := rc.Process(context.Background(), cmd)
	require.NoError(t, err)

	value, err := cmd.Bytes()
	require.NoError(t, err)
	v := new(int)
	err = resp.Scan(value, v)
	require.NoError(t, err)
	require.Equal(t, 100, *v)
}

func TestDMap_incrCommandHandler_Single_Request(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := protocol.NewIncr("mydmap", "mykey", 100).Command(context.Background())
	rc := s.client.Get(s.rt.This().String())
	err := rc.Process(context.Background(), cmd)
	require.NoError(t, err)
	value, err := cmd.Result()

	require.NoError(t, err)
	require.Equal(t, 100, int(value))
}

func TestDMap_decrCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		errGr.Go(func() error {
			cmd := protocol.NewDecr("mydmap", "mykey", 1).Command(context.Background())
			rc := s.client.Get(s.rt.This().String())
			err := rc.Process(context.Background(), cmd)
			if err != nil {
				return err
			}
			_, err = cmd.Result()
			return err
		})
	}
	require.NoError(t, errGr.Wait())

	cmd := protocol.NewGet("mydmap", "mykey").Command(context.Background())
	rc := s.client.Get(s.rt.This().String())
	err := rc.Process(context.Background(), cmd)
	require.NoError(t, err)

	value, err := cmd.Bytes()
	require.NoError(t, err)
	v := new(int)
	err = resp.Scan(value, v)
	require.NoError(t, err)
	require.Equal(t, -100, *v)
}

func TestDMap_decrCommandHandler_Single_Request(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := protocol.NewDecr("mydmap", "mykey", 100).Command(context.Background())
	rc := s.client.Get(s.rt.This().String())
	err := rc.Process(context.Background(), cmd)
	require.NoError(t, err)
	value, err := cmd.Result()

	require.NoError(t, err)
	require.Equal(t, -100, int(value))
}

func TestDMap_exGetPutOperation(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var total int64
	var final int64
	start := make(chan struct{})

	getPut := func(i int) error {
		<-start

		buf := bytes.NewBuffer(nil)
		enc := resp.New(buf)
		err := enc.Encode(i)
		if err != nil {
			return err
		}

		cmd := protocol.NewGetPut("mydmap", "mykey", buf.Bytes()).Command(context.Background())
		rc := s.client.Get(s.rt.This().String())
		err = rc.Process(context.Background(), cmd)
		if err == redis.Nil {
			return nil
		}
		if err != nil {
			return err
		}
		val, err := cmd.Bytes()
		if err != nil {
			return err
		}

		if len(val) != 0 {
			oldval := new(int)
			err = resp.Scan(val, oldval)
			if err != nil {
				return err
			}
			atomic.AddInt64(&total, int64(*oldval))
		}
		return nil
	}

	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		num := i
		errGr.Go(func() error {
			return getPut(num)
		})
		final += int64(i)
	}

	close(start)
	require.NoError(t, errGr.Wait())

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	gr, err := dm.Get(context.Background(), "mykey")
	require.NoError(t, err)

	var last int
	err = resp.Scan(gr.Value(), &last)
	require.NoError(t, err)

	atomic.AddInt64(&total, int64(last))
	require.Equal(t, final, atomic.LoadInt64(&total))
}

func TestDMap_incrByFloatCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		errGr.Go(func() error {
			cmd := protocol.NewIncrByFloat("mydmap", "mykey", 1.2).Command(context.Background())
			rc := s.client.Get(s.rt.This().String())
			err := rc.Process(context.Background(), cmd)
			if err != nil {
				return err
			}
			_, err = cmd.Result()
			return err
		})
	}
	require.NoError(t, errGr.Wait())

	cmd := protocol.NewGet("mydmap", "mykey").Command(context.Background())
	rc := s.client.Get(s.rt.This().String())
	err := rc.Process(context.Background(), cmd)
	require.NoError(t, err)

	value, err := cmd.Bytes()
	require.NoError(t, err)
	v := new(float64)
	err = resp.Scan(value, v)
	require.NoError(t, err)
	require.Equal(t, 120.0000000000002, *v)
}

// respEncode is a test helper that RESP-encodes a value the same way the CAS
// code path does, so tests can compute the exact bytes that will be stored.
func respEncode(t *testing.T, v interface{}) []byte {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	require.NoError(t, resp.New(buf).Encode(v))
	return buf.Bytes()
}

// TestDMap_CompareAndSwap_Insert exercises the NX path: empty expected against
// a key that doesn't exist succeeds; the next attempt with empty expected
// fails because the key now exists.
func TestDMap_CompareAndSwap_Insert(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("cas_insert")
	require.NoError(t, err)
	ctx := context.Background()

	swapped, current, err := dm.CompareAndSwap(ctx, "key", nil, 42, nil)
	require.NoError(t, err)
	require.True(t, swapped, "first NX CAS on absent key should swap")
	require.Nil(t, current, "no current value returned on successful swap")

	gr, err := dm.Get(ctx, "key")
	require.NoError(t, err)
	var v int
	require.NoError(t, resp.Scan(gr.Value(), &v))
	require.Equal(t, 42, v)

	swapped, current, err = dm.CompareAndSwap(ctx, "key", nil, 99, nil)
	require.NoError(t, err)
	require.False(t, swapped, "NX CAS on existing key must not swap")
	require.NotNil(t, current, "current entry returned on mismatch")
	require.True(t, bytes.Equal(current.Value(), respEncode(t, 42)),
		"mismatch current must return the actual stored bytes")
}

// TestDMap_CompareAndSwap_Update exercises the successful update path: read,
// CAS against the observed bytes, succeed.
func TestDMap_CompareAndSwap_Update(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("cas_update")
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, dm.Put(ctx, "k", 1, nil))

	gr, err := dm.Get(ctx, "k")
	require.NoError(t, err)
	expected := gr.Value()

	swapped, current, err := dm.CompareAndSwap(ctx, "k", expected, 2, nil)
	require.NoError(t, err)
	require.True(t, swapped)
	require.Nil(t, current)

	gr2, err := dm.Get(ctx, "k")
	require.NoError(t, err)
	var v int
	require.NoError(t, resp.Scan(gr2.Value(), &v))
	require.Equal(t, 2, v)
}

// TestDMap_CompareAndSwap_Mismatch verifies that a stale `expected` causes the
// swap to fail and returns the actual current value.
func TestDMap_CompareAndSwap_Mismatch(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("cas_mismatch")
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, dm.Put(ctx, "k", "v1", nil))

	stale := respEncode(t, "wrong")
	swapped, current, err := dm.CompareAndSwap(ctx, "k", stale, "v2", nil)
	require.NoError(t, err)
	require.False(t, swapped)
	require.NotNil(t, current)
	require.True(t, bytes.Equal(current.Value(), respEncode(t, "v1")))

	gr, err := dm.Get(ctx, "k")
	require.NoError(t, err)
	var v string
	require.NoError(t, resp.Scan(gr.Value(), &v))
	require.Equal(t, "v1", v, "value must be unchanged after a failed CAS")
}

// TestDMap_CompareAndSwap_TTL checks that PX is honored: the value expires
// within the expected window.
func TestDMap_CompareAndSwap_TTL(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("cas_ttl")
	require.NoError(t, err)
	ctx := context.Background()

	cfg := &PutConfig{HasPX: true, PX: 50 * time.Millisecond}
	swapped, _, err := dm.CompareAndSwap(ctx, "k", nil, "ephemeral", cfg)
	require.NoError(t, err)
	require.True(t, swapped)

	gr, err := dm.Get(ctx, "k")
	require.NoError(t, err)
	var v string
	require.NoError(t, resp.Scan(gr.Value(), &v))
	require.Equal(t, "ephemeral", v)

	// Wait for expiry.
	time.Sleep(150 * time.Millisecond)

	_, err = dm.Get(ctx, "k")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// TestDMap_CompareAndSwap_Concurrent hammers a key with concurrent CAS updates
// from many goroutines; each goroutine retries on mismatch. At the end we
// expect a key incremented exactly N times — no lost updates.
func TestDMap_CompareAndSwap_Concurrent(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("cas_concurrent")
	require.NoError(t, err)
	ctx := context.Background()

	// Seed at 0.
	require.NoError(t, dm.Put(ctx, "counter", 0, nil))

	const goroutines = 64
	const perG = 20

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				for {
					gr, err := dm.Get(ctx, "counter")
					if err != nil {
						errCh <- err
						return
					}
					var cur int
					if err := resp.Scan(gr.Value(), &cur); err != nil {
						errCh <- err
						return
					}
					swapped, _, err := dm.CompareAndSwap(ctx, "counter", gr.Value(), cur+1, nil)
					if err != nil {
						errCh <- err
						return
					}
					if swapped {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		require.NoError(t, e)
	}

	gr, err := dm.Get(ctx, "counter")
	require.NoError(t, err)
	var final int
	require.NoError(t, resp.Scan(gr.Value(), &final))
	require.Equal(t, goroutines*perG, final, "CAS-retry loop must produce lossless increments")
}

// TestDMap_CompareAndSwap_WireProtocol sends a DM.CAS command directly over
// the redis client and checks the two-element reply shape.
func TestDMap_CompareAndSwap_WireProtocol(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()

	// Initial NX CAS on absent key.
	newBytes := respEncode(t, "hello")
	cmd := protocol.NewCompareAndSwap("wire", "k", nil, newBytes).Command(ctx)
	require.NoError(t, rc.Process(ctx, cmd))
	items, err := cmd.Slice()
	require.NoError(t, err)
	require.Len(t, items, 2)
	swapped, _ := items[0].(int64)
	require.Equal(t, int64(1), swapped)
	require.Nil(t, items[1])

	// Mismatched CAS returns current encoded entry.
	wrongExpected := respEncode(t, "nope")
	cmd2 := protocol.NewCompareAndSwap("wire", "k", wrongExpected, newBytes).Command(ctx)
	require.NoError(t, rc.Process(ctx, cmd2))
	items2, err := cmd2.Slice()
	require.NoError(t, err)
	require.Len(t, items2, 2)
	swapped2, _ := items2[0].(int64)
	require.Equal(t, int64(0), swapped2)
	require.NotNil(t, items2[1], "mismatch reply must carry the current entry")
}
