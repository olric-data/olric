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

package routingtable

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/hashicorp/memberlist"
	"golang.org/x/sync/errgroup"
)

func newRoutingTableForTest(c *config.Config, srv *server.Server) *RoutingTable {
	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("client", server.NewClient(c.Client))
	e.Set("server", srv)

	rt := New(e)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			panic(fmt.Sprintf("ListenAndServe returned an error: %v", err))
		}
	}()
	<-srv.StartedCtx.Done()

	return rt
}

type testCluster struct {
	peerPorts []int
	errGr     errgroup.Group
	ctx       context.Context
	cancel    context.CancelFunc
}

func newTestCluster() *testCluster {
	ctx, cancel := context.WithCancel(context.Background())
	return &testCluster{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (t *testCluster) addNode(c *config.Config) (*RoutingTable, error) {
	if c == nil {
		c = testutil.NewConfig()
	}
	port, err := testutil.GetFreePort()
	if err != nil {
		return nil, err
	}
	c.MemberlistConfig.BindPort = port

	var peers []string
	for _, peerPort := range t.peerPorts {
		peers = append(peers, net.JoinHostPort("127.0.0.1", strconv.Itoa(peerPort)))
	}
	c.Peers = peers

	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	err = rt.Join()
	if err != nil {
		return nil, err
	}
	err = rt.Start()
	if err != nil {
		return nil, err
	}

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return srv.Shutdown(context.Background())
	})

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return rt.Shutdown(context.Background())
	})

	t.peerPorts = append(t.peerPorts, port)
	return rt, err
}

func (t *testCluster) shutdown() error {
	t.cancel()
	return t.errGr.Wait()
}

func TestRoutingTable_SingleNode(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt.This().CompareByID(rt.Discovery().GetCoordinator()) {
		t.Fatalf("Coordinator is different")
	}

	if !rt.IsBootstrapped() {
		t.Fatalf("The coordinator node cannot be bootstrapped")
	}

	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		part := rt.primary.PartitionByID(partID)
		if !part.Owner().CompareByID(rt.This()) {
			t.Fatalf("PartID: %d has a different owner", partID)
		}
	}

	if rt.Signature() == 0 {
		t.Fatalf("routingTable.signature is zero")
	}

	if rt.OwnedPartitionCount() != c.PartitionCount {
		t.Fatalf("Expected owned partition count: %d. Got: %d", rt.OwnedPartitionCount(), c.PartitionCount)
	}
}

func TestRoutingTable_Cluster(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c1 := testutil.NewConfig()
	rt1, err := cluster.addNode(c1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt1.IsBootstrapped() {
		t.Fatalf("The coordinator node cannot be bootstrapped")
	}

	firstSignature := rt1.Signature()

	c2 := testutil.NewConfig()
	rt2, err := cluster.addNode(c2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt2.Discovery().GetCoordinator().CompareByID(rt1.Discovery().GetCoordinator()) {
		t.Fatalf("Coordinator is different")
	}

	if firstSignature == rt2.Signature() {
		t.Fatalf("routingTable signature did not changed after node join")
	}

	if rt1.OwnedPartitionCount() == c1.PartitionCount {
		t.Fatalf("rt1 has all the partitions")
	}

	if rt2.OwnedPartitionCount() == c2.PartitionCount {
		t.Fatalf("rt2 has all the partitions")
	}

	totalPartitionCount := rt1.OwnedPartitionCount() + rt2.OwnedPartitionCount()
	if totalPartitionCount != c1.PartitionCount {
		t.Fatalf("Total partition count is wrong: %d", totalPartitionCount)
	}

	err = cluster.shutdown()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestRoutingTable_CheckPartitionOwnership(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c1 := testutil.NewConfig()
	rt1, err := cluster.addNode(c1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt1.IsBootstrapped() {
		t.Fatalf("The coordinator node cannot be bootstrapped")
	}

	c2 := testutil.NewConfig()
	rt2, err := cluster.addNode(c2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		ownerOne := rt1.primary.PartitionByID(partID).Owner()
		ownerTwo := rt2.primary.PartitionByID(partID).Owner()
		if !ownerOne.CompareByID(ownerTwo) {
			t.Fatalf("Different partition: %d owner: %s != %s", partID, ownerOne, ownerTwo)
		}
	}

	err = cluster.shutdown()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestRoutingTable_NodeLeave(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c1 := testutil.NewConfig()
	rt1, err := cluster.addNode(c1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt1.IsBootstrapped() {
		t.Fatalf("The coordinator node cannot be bootstrapped")
	}

	c2 := testutil.NewConfig()
	rt2, err := cluster.addNode(c2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	signatureWithTwoNode := rt1.Signature()
	err = rt1.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if rt2.Signature() == signatureWithTwoNode {
			return errors.New("still has the same signature")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt2.Discovery().GetCoordinator().CompareByID(rt2.This()) {
		t.Fatalf("Coordinator is different")
	}

	for partID := uint64(0); partID < c2.PartitionCount; partID++ {
		part := rt2.primary.PartitionByID(partID)
		if !part.Owner().CompareByID(rt2.This()) {
			t.Fatalf("PartID: %d has a different owner", partID)
		}
	}

	err = cluster.shutdown()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestRoutingTable_NodeUpdate(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c1 := testutil.NewConfig()
	rt1, err := cluster.addNode(c1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt1.IsBootstrapped() {
		t.Fatalf("The coordinator node cannot be bootstrapped")
	}

	c2 := testutil.NewConfig()
	rt2, err := cluster.addNode(c2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	n := rt2.Discovery().LocalNode()
	meta, err := discovery.NewMember(c2).Encode()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	n.Meta = meta
	event := memberlist.NodeEvent{Event: memberlist.NodeUpdate, Node: n}
	rt2.Discovery().ClusterEvents <- discovery.ToClusterEvent(event)

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		_, err = rt1.Members().Get(rt2.This().ID)
		if err == nil {
			// node id is updated.
			return errors.New("rt2 could not be updated")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = cluster.shutdown()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}
