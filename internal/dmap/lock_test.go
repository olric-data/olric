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
	"github.com/buraksezer/olric/internal/protocol"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/stretchr/testify/require"
)

func TestDMap_Lock_With_Timeout_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()
	token, err := dm.Lock(ctx, key, time.Second, time.Second)
	require.NoError(t, err)

	err = dm.Unlock(ctx, key, token)
	require.NoError(t, err)
}

func TestDMap_Unlock_After_Timeout_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()
	token, err := dm.Lock(context.Background(), key, time.Millisecond, time.Second)
	require.NoError(t, err)

	<-time.After(10 * time.Millisecond)

	err = dm.Unlock(ctx, key, token)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestDMap_Lock_With_Timeout_ErrLockNotAcquired_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()
	_, err = dm.Lock(ctx, key, time.Second, time.Second)
	require.NoError(t, err)

	_, err = dm.Lock(context.Background(), key, time.Second, time.Millisecond)
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestDMap_LockLease_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()
	token, err := dm.Lock(context.Background(), key, time.Second, time.Second)
	require.NoError(t, err)

	err = dm.Lease(ctx, key, token, 2*time.Second)
	require.NoError(t, err)

	e, err := dm.Get(ctx, key)
	require.NoError(t, err)

	if e.TTL()-(time.Now().UnixNano()/1000000) <= 1900 {
		t.Fatalf("Expected >=1900. Got: %v", e.TTL()-(time.Now().UnixNano()/1000000))
	}

	<-time.After(3 * time.Second)

	err = dm.Lease(ctx, key, token, 3*time.Second)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestDMap_Lock_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	token, err := dm.Lock(ctx, key, nilTimeout, time.Second)
	require.NoError(t, err)

	err = dm.Unlock(ctx, key, token)
	require.NoError(t, err)
}

func TestDMap_Lock_ErrLockNotAcquired_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	_, err = dm.Lock(ctx, key, nilTimeout, time.Second)
	require.NoError(t, err)

	_, err = dm.Lock(ctx, key, nilTimeout, time.Millisecond)
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestDMap_LockWithTimeout_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()
	tokens := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		token, err := dm.Lock(ctx, key, time.Hour, time.Second)
		require.NoError(t, err)
		tokens[key] = token
	}

	cluster.AddMember(nil)
	for key, token := range tokens {
		err = dm.Unlock(ctx, key, token)
		require.NoError(t, err)
	}
}

func TestDMap_LockLease_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()
	tokens := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		token, err := dm.Lock(ctx, key, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, err)
		tokens[key] = token
	}

	cluster.AddMember(nil)
	for key, token := range tokens {
		err = dm.Lease(ctx, key, token, 10*time.Second)
		require.NoError(t, err)
	}
}

func TestDMap_Lock_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()
	tokens := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		token, err := dm.Lock(ctx, key, nilTimeout, time.Second)
		require.NoError(t, err)
		tokens[key] = token
	}

	cluster.AddMember(nil)
	for key, token := range tokens {
		err = dm.Unlock(ctx, key, token)
		require.NoError(t, err)
	}
}

func TestDMap_LockWithTimeout_ErrLockNotAcquired_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err := dm.Lock(ctx, key, time.Second, time.Second)
		require.NoError(t, err)
	}

	cluster.AddMember(nil)

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = dm.Lock(ctx, key, time.Second, time.Millisecond)
		require.ErrorIs(t, err, ErrLockNotAcquired)
	}
}

func TestDMap_Lock_After_Lock_With_Timeout_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = dm.Lock(ctx, key, time.Millisecond, time.Second)
		require.NoError(t, err)
	}

	cluster.AddMember(nil)
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = dm.Lock(ctx, key, nilTimeout, time.Second)
		require.NoError(t, err)
	}
}

func TestDMap_tryLock(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	_, err = dm.Lock(context.Background(), key, time.Second, time.Second)
	require.NoError(t, err)

	var i int
	var acquired bool
	for i <= 10 {
		i++
		_, err := dm.Lock(context.Background(), key, nilTimeout, 100*time.Millisecond)
		if err == ErrLockNotAcquired {
			// already acquired
			continue
		}
		require.NoError(t, err)
		// Acquired
		acquired = true
		break
	}
	if !acquired {
		t.Fatal("Failed to acquire lock")
	}
}

func TestDMap_lockCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := protocol.NewLock("lock.test", "lock.test.foo", 1).Command(s.ctx)
	rc := s.client.Get(s.rt.This().String())
	err := rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	token, err := cmd.Bytes()
	require.NoError(t, err)

	cmdUnlock := protocol.NewUnlock("lock.test", "lock.test.foo", string(token)).Command(s.ctx)
	err = rc.Process(s.ctx, cmdUnlock)
	require.NoError(t, err)

	err = cmdUnlock.Err()
	require.NoError(t, err)
}

func TestDMap_lockCommandHandler_EX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := protocol.NewLock("lock.test", "lock.test.foo", 1).SetEX(1).Command(s.ctx)
	rc := s.client.Get(s.rt.This().String())
	err := rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	token, err := cmd.Bytes()
	require.NoError(t, err)

	<-time.After(2 * time.Second)

	cmdUnlock := protocol.NewUnlock("lock.test", "lock.test.foo", string(token)).Command(s.ctx)
	err = rc.Process(s.ctx, cmdUnlock)
	err = protocol.ConvertError(err)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestDMap_lockCommandHandler_PX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := protocol.NewLock("lock.test", "lock.test.foo", 1).
		SetPX((10 * time.Millisecond).Milliseconds()).Command(s.ctx)
	rc := s.client.Get(s.rt.This().String())
	err := rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	token, err := cmd.Bytes()
	require.NoError(t, err)

	<-time.After(20 * time.Millisecond)

	cmdUnlock := protocol.NewUnlock("lock.test", "lock.test.foo", string(token)).Command(s.ctx)
	err = rc.Process(s.ctx, cmdUnlock)
	err = protocol.ConvertError(err)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestDMap_lockLeaseCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()

	token, err := dm.Lock(ctx, key, time.Second, time.Second)
	require.NoError(t, err)

	// Update the timeout
	etoken := hex.EncodeToString(token)
	cmd := protocol.NewLockLease("lock.test", key, etoken, 10).Command(s.ctx)
	rc := s.client.Get(s.rt.This().String())
	err = rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	<-time.After(2 * time.Second)

	err = dm.Unlock(ctx, key, token)
	require.NoError(t, err)
}

func TestDMap_plockLeaseCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx := context.Background()

	token, err := dm.Lock(ctx, key, 250*time.Millisecond, time.Second)
	require.NoError(t, err)

	// Update the timeout
	etoken := hex.EncodeToString(token)
	cmd := protocol.NewPLockLease("lock.test", key, etoken, 2000).Command(s.ctx)
	rc := s.client.Get(s.rt.This().String())
	err = rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	<-time.After(500 * time.Millisecond)

	err = dm.Unlock(ctx, key, token)
	require.NoError(t, err)
}
