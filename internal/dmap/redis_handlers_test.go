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
	"context"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/protocol"
	"github.com/olric-data/olric/internal/testcluster"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestRedis_Set_Get_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap(protocol.DefaultDMapName)
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

func TestRedis_Set_EX_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	pc := &PutConfig{
		HasEX: true,
		EX:    time.Second / 4,
	}
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(time.Second)

	for i := 0; i < 10; i++ {
		_, err := dm.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestRedis_Set_PX_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	pc := &PutConfig{
		HasPX: true,
		PX:    time.Millisecond,
	}
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(10 * time.Millisecond)

	for i := 0; i < 10; i++ {
		_, err := dm.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestRedis_Set_NX_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	// Set initial values
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	// Try to set with NX — should fail because keys exist
	pc := &PutConfig{HasNX: true}
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i*2), pc)
		if err == ErrKeyFound {
			err = nil
		}
		require.NoError(t, err)
	}

	// Values should be unchanged
	for i := 0; i < 10; i++ {
		gr, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestRedis_Set_XX_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	// Try to set with XX on missing keys — should fail
	pc := &PutConfig{HasXX: true}
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.ErrorIs(t, err, ErrKeyNotFound)
	}

	// Keys should not exist
	for i := 0; i < 10; i++ {
		_, err = dm.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestRedis_Get_KeyNotFound(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	_, err = dm.Get(ctx, "nonexistent-key")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestRedis_Del_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = testutil.ToKey(i)
	}

	count, err := dm.Delete(ctx, keys...)
	require.NoError(t, err)
	require.Equal(t, 10, count)

	for i := 0; i < 10; i++ {
		_, err = dm.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestRedis_Del_KeyNotFound(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	// Olric's deleteKeys returns len(keys) even for missing keys.
	count, err := dm.Delete(ctx, "nonexistent-key")
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestRedis_Set_Get_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()

	dm1, err := s1.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	dm2, err := s2.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		gr, err := dm2.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestRedis_Del_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()

	dm1, err := s1.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	dm2, err := s2.NewDMap(protocol.DefaultDMapName)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = dm2.Delete(ctx, testutil.ToKey(i))
		require.NoError(t, err)

		_, err = dm2.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}
