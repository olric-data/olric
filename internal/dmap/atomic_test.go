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
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/resp"
	"github.com/buraksezer/olric/internal/testcluster"
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
