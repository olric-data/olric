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

package balancer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routingtable"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/testutil/mockfragment"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func newTestEnvironment(c *config.Config) *environment.Environment {
	if c == nil {
		c = testutil.NewConfig()
	}

	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("client", server.NewClient(c.Client))
	return e
}

func newBalancerForTest(e *environment.Environment) *Balancer {
	rt := routingtable.New(e)
	srv := e.Get("server").(*server.Server)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			panic(fmt.Sprintf("ListenAndServe returned an error: %v", err))
		}
	}()
	<-srv.StartedCtx.Done()

	e.Set("routingtable", rt)
	b := New(e)
	return b
}

type mockCluster struct {
	t         *testing.T
	peerPorts []int
	errGr     errgroup.Group
	ctx       context.Context
	cancel    context.CancelFunc
}

func newMockCluster(t *testing.T) *mockCluster {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockCluster{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (mc *mockCluster) addNode(e *environment.Environment) *Balancer {
	if e == nil {
		e = newTestEnvironment(nil)
	}
	c := e.Get("config").(*config.Config)
	c.TriggerBalancerInterval = time.Millisecond
	c.DMaps.CheckEmptyFragmentsInterval = time.Millisecond

	port, err := testutil.GetFreePort()
	if err != nil {
		require.NoError(mc.t, err)
	}
	c.MemberlistConfig.BindPort = port

	var peers []string
	for _, peerPort := range mc.peerPorts {
		peers = append(peers, net.JoinHostPort("127.0.0.1", strconv.Itoa(peerPort)))
	}
	c.Peers = peers

	srv := testutil.NewServer(c)
	e.Set("server", srv)
	b := newBalancerForTest(e)

	err = b.Start()
	if err != nil {
		require.NoError(mc.t, err)
	}

	err = b.rt.Join()
	require.NoError(mc.t, err)

	err = b.rt.Start()
	if err != nil {
		require.NoError(mc.t, err)
	}

	mc.errGr.Go(func() error {
		<-mc.ctx.Done()
		return srv.Shutdown(context.Background())
	})

	mc.errGr.Go(func() error {
		<-mc.ctx.Done()
		return b.rt.Shutdown(context.Background())
	})

	mc.peerPorts = append(mc.peerPorts, port)

	mc.t.Cleanup(func() {
		require.NoError(mc.t, b.Shutdown(context.Background()))
	})

	return b
}

func (mc *mockCluster) shutdown() {
	mc.cancel()
	require.NoError(mc.t, mc.errGr.Wait())
}

func TestBalance_Primary_Move(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	e1 := newTestEnvironment(nil)
	cluster.addNode(e1)

	fragments := make(map[uint64]*mockfragment.MockFragment)

	// Create a MockFragment and insert some fake data
	c := e1.Get("config").(*config.Config)
	part := e1.Get(strings.ToLower(partitions.PRIMARY.String())).(*partitions.Partitions)
	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		part := part.PartitionByID(partID)
		s := mockfragment.New()
		s.Fill()
		part.Map().Store("dmap.test-data", s)
		fragments[partID] = s
	}

	e2 := newTestEnvironment(nil)
	b2 := cluster.addNode(e2)

	err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	for partID, f := range fragments {
		result := f.Result()
		if len(result) == 0 {
			continue
		}

		require.Len(t, result, 1)
		require.NotNil(t, result[partitions.PRIMARY])
		r := result[partitions.PRIMARY]
		require.NotNil(t, r[partID])
		require.Equal(t, "test-data", r[partID].Name)
		require.Equal(t, []discovery.Member{b2.rt.This()}, r[partID].Owners)
	}
}

func checkBackupOwnership(e *environment.Environment) error {
	c := e.Get("config").(*config.Config)
	primary := e.Get(strings.ToLower(partitions.PRIMARY.String())).(*partitions.Partitions)
	backup := e.Get(strings.ToLower(partitions.BACKUP.String())).(*partitions.Partitions)

	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		primaryOwner := primary.PartitionByID(partID).Owner()
		part := backup.PartitionByID(partID)
		for _, owner := range part.Owners() {
			if primaryOwner.CompareByID(owner) {
				return fmt.Errorf("%s is the primary and backup owner of partID: %d at the same time", primaryOwner, partID)
			}
		}
	}

	return nil
}

func TestBalance_Empty_Backup_Move(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	e1 := newTestEnvironment(c1)
	b1 := cluster.addNode(e1)

	b1.rt.UpdateEagerly()

	err := checkBackupOwnership(e1)
	require.NoError(t, err)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	e2 := newTestEnvironment(c2)
	b2 := cluster.addNode(e2)

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	b1.rt.UpdateEagerly()

	err = checkBackupOwnership(e2)
	require.NoError(t, err)
}

func TestBalance_Backup_Move(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	e1 := newTestEnvironment(c1)
	b1 := cluster.addNode(e1)

	fragments := make(map[uint64]*mockfragment.MockFragment)

	c := e1.Get("config").(*config.Config)
	part := e1.Get(strings.ToLower(partitions.BACKUP.String())).(*partitions.Partitions)
	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		part := part.PartitionByID(partID)
		s := mockfragment.New()
		s.Fill()
		part.Map().Store("dmap.test-data", s)
		fragments[partID] = s
	}

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	e2 := newTestEnvironment(c2)
	b2 := cluster.addNode(e2)

	err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		b1.rt.UpdateEagerly()
		err = checkBackupOwnership(e2)
		require.NoError(t, err)
	}

	for partID, f := range fragments {
		result := f.Result()
		if len(result) == 0 {
			continue
		}

		require.Len(t, result, 1)
		require.NotNil(t, result[partitions.BACKUP])
		r := result[partitions.BACKUP]
		require.NotNil(t, r[partID])
		require.Equal(t, "test-data", r[partID].Name)
		require.Equal(t, []discovery.Member{b2.rt.This()}, r[partID].Owners)
	}
}
