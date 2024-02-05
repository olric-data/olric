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
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/stretchr/testify/require"
)

func TestDMap_Expire(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	key := "mykey"
	err = dm.Put(ctx, key, "myvalue", nil)
	require.NoError(t, err)

	_, err = dm.Get(ctx, key)
	require.NoError(t, err)

	err = dm.Expire(ctx, key, time.Millisecond)
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	// Get the value and check it.
	_, err = dm.Get(ctx, key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMap_Expire_ErrKeyNotFound(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Expire(context.Background(), "mykey", time.Millisecond)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMap_Expire_expireCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	key := "mykey"
	err = dm.Put(ctx, key, "myvalue", nil)
	require.NoError(t, err)

	cmd := protocol.NewExpire("mydmap", "mykey", time.Duration(0.1*float64(time.Second))).Command(s.ctx)
	rc := s.client.Get(s.rt.This().String())
	err = rc.Process(ctx, cmd)
	require.NoError(t, err)

	<-time.After(200 * time.Millisecond)

	// Get the value and check it.
	_, err = dm.Get(ctx, key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMap_Expire_pexpireCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	key := "mykey"
	err = dm.Put(ctx, key, "myvalue", nil)
	require.NoError(t, err)

	cmd := protocol.NewPExpire("mydmap", "mykey", time.Millisecond).Command(s.ctx)
	rc := s.client.Get(s.rt.This().String())
	err = rc.Process(ctx, cmd)
	require.NoError(t, err)

	<-time.After(10 * time.Millisecond)

	// Get the value and check it.
	_, err = dm.Get(ctx, key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}
