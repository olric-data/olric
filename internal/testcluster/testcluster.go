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

package testcluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/balancer"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routingtable"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/locker"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/internal/service"
	"github.com/buraksezer/olric/internal/testutil"
	"golang.org/x/sync/errgroup"
)

type TestCluster struct {
	mu sync.Mutex

	environments []*environment.Environment
	memberPorts  []int
	constructor  func(e *environment.Environment) (service.Service, error)
	errGr        errgroup.Group
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewEnvironment(c *config.Config) *environment.Environment {
	if c == nil {
		c = testutil.NewConfig()
	}

	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("client", server.NewClient(c.Client))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("locker", locker.New())
	e.Set("server", testutil.NewServer(c))
	return e
}

func (t *TestCluster) newService(e *environment.Environment) service.Service {
	rt := routingtable.New(e)
	e.Set("routingtable", rt)

	b := balancer.New(e)
	e.Set("balancer", b)
	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return b.Shutdown(context.Background())
	})

	srv := e.Get("server").(*server.Server)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			panic(fmt.Sprintf("ListenAndServe returned an error: %v", err))
		}
	}()
	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return srv.Shutdown(context.Background())
	})
	<-srv.StartedCtx.Done()

	s, err := t.constructor(e)
	if err != nil {
		panic(fmt.Sprintf("failed to start DMap service: %v", err))
	}
	return s
}

func New(constructor func(e *environment.Environment) (service.Service, error)) *TestCluster {
	ctx, cancel := context.WithCancel(context.Background())
	return &TestCluster{
		constructor: constructor,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (t *TestCluster) syncCluster() {
	// Update routing table on the cluster before running balancer
	for _, e := range t.environments {
		rt := e.Get("routingtable").(*routingtable.RoutingTable)
		if rt.Discovery().IsCoordinator() {
			// The coordinator pushes the routing table immediately.
			// Normally, this is triggered by every cluster event but we don't want to
			// do this asynchronously to avoid randomness in tests.
			rt.UpdateEagerly()
		}
	}
	// Normally, balancer is triggered by routing table after a successful update, but we don't want to
	// balance the test cluster asynchronously. So we balance the partitions here explicitly.
	for _, e := range t.environments {
		e.Get("balancer").(*balancer.Balancer).BalanceEagerly()
	}
}

func (t *TestCluster) AddMember(e *environment.Environment) service.Service {
	t.mu.Lock()
	defer t.mu.Unlock()

	if e == nil {
		e = NewEnvironment(nil)
	}
	c := e.Get("config").(*config.Config)
	partitions.SetHashFunc(c.Hasher)

	port, err := testutil.GetFreePort()
	if err != nil {
		panic(fmt.Sprintf("failed to a random port: %v", err))
	}
	c.MemberlistConfig.BindPort = port

	var peers []string
	for _, peerPort := range t.memberPorts {
		peers = append(peers, net.JoinHostPort("127.0.0.1", strconv.Itoa(peerPort)))
	}
	c.Peers = peers

	s := t.newService(e)
	rt := e.Get("routingtable").(*routingtable.RoutingTable)
	err = rt.Join()
	if err != nil {
		panic(fmt.Sprintf("failed to join the Olric cluster: %v", err))
	}
	err = rt.Start()
	if err != nil {
		panic(fmt.Sprintf("failed to start the routing table: %v", err))
	}

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return rt.Shutdown(context.Background())
	})

	t.errGr.Go(func() error {
		return s.Start()
	})

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return s.Shutdown(context.Background())
	})

	t.environments = append(t.environments, e)
	t.memberPorts = append(t.memberPorts, port)
	t.syncCluster()
	return s
}

func (t *TestCluster) Shutdown() {
	t.cancel()
	err := t.errGr.Wait()
	if err != nil {
		panic(fmt.Sprintf("failed to shutdown the cluster: %v", err))
	}
}
