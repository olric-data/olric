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

package discovery

import (
	"fmt"
	"github.com/buraksezer/olric/pkg/service_discovery"
	"github.com/hashicorp/memberlist"
	"log"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

type testCluster struct {
	mtx       sync.RWMutex
	instances []*Discovery
	members   []string
}

func newTestCluster(t *testing.T) *testCluster {
	tc := &testCluster{}
	t.Cleanup(func() {
		tc.mtx.Lock()
		defer tc.mtx.Unlock()

		for _, instance := range tc.instances {
			require.NoError(t, instance.Shutdown())
		}
	})
	return tc
}

func (tc *testCluster) addNewMember(t *testing.T) *Discovery {
	tc.mtx.Lock()
	defer tc.mtx.Unlock()

	cfg := testutil.NewConfig()
	for _, peer := range tc.members {
		cfg.Peers = append(cfg.Peers, peer)
	}

	flogger := testutil.NewFlogger(cfg)
	d := New(flogger, cfg)
	err := d.Start()
	require.NoError(t, err)

	_, err = d.Join()
	require.NoError(t, err)

	tc.instances = append(tc.instances, d)
	addr := net.JoinHostPort(
		d.config.MemberlistConfig.BindAddr,
		strconv.Itoa(d.config.MemberlistConfig.BindPort),
	)
	tc.members = append(tc.members, addr)

	return d
}

func TestDiscovery_GetCoordinator(t *testing.T) {
	c := newTestCluster(t)
	d1 := c.addNewMember(t)
	d2 := c.addNewMember(t)

	require.Equal(t, d1.GetCoordinator(), d2.GetCoordinator())
}

func TestDiscovery_GetMembers(t *testing.T) {
	c := newTestCluster(t)
	d1 := c.addNewMember(t)
	c.addNewMember(t)
	c.addNewMember(t)

	require.Len(t, d1.GetMembers(), 3)
}

func TestDiscovery_IsCoordinator(t *testing.T) {
	c := newTestCluster(t)
	d1 := c.addNewMember(t)

	<-time.After(100 * time.Millisecond)
	d2 := c.addNewMember(t)

	<-time.After(100 * time.Millisecond)
	d3 := c.addNewMember(t)

	require.True(t, d1.IsCoordinator())
	require.False(t, d2.IsCoordinator())
	require.False(t, d3.IsCoordinator())
}

func TestDiscovery_NumMembers(t *testing.T) {
	c := newTestCluster(t)
	d1 := c.addNewMember(t)
	c.addNewMember(t)
	c.addNewMember(t)

	require.Equal(t, d1.NumMembers(), 3)
}

func TestDiscovery_LocalNode(t *testing.T) {
	c := newTestCluster(t)
	d1 := c.addNewMember(t)

	require.Equal(t, d1.LocalNode().Name, d1.config.MemberlistConfig.Name)
}

func TestDiscovery_FindMemberByID(t *testing.T) {
	c := newTestCluster(t)
	c.addNewMember(t)
	c.addNewMember(t)
	c.addNewMember(t)

	for i, instance := range c.instances {
		m, err := instance.FindMemberByID(c.instances[i].member.ID)
		require.NoError(t, err)
		require.Equal(t, m.Name, instance.config.MemberlistConfig.Name)
	}
}

func TestDiscovery_FindMemberByName(t *testing.T) {
	c := newTestCluster(t)
	c.addNewMember(t)
	c.addNewMember(t)
	c.addNewMember(t)

	for i, instance := range c.instances {
		m, err := instance.FindMemberByName(c.instances[i].member.Name)
		require.NoError(t, err)
		require.Equal(t, m.Name, instance.config.MemberlistConfig.Name)
	}
}

func TestDiscovery_increaseUptimeSeconds(t *testing.T) {
	c := newTestCluster(t)
	c.addNewMember(t)

	<-time.After(2 * time.Second)

	require.Greater(t, UptimeSeconds.Read(), int64(0))
}

type dummyServiceDiscovery struct {
	mtx sync.Mutex

	initialized   bool
	closed        bool
	setLogger     bool
	setConfig     bool
	register      bool
	discoverPeers bool
	deregister    bool
	log           *log.Logger
}

func (d *dummyServiceDiscovery) Initialize() error {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	d.initialized = true

	return nil
}

func (d *dummyServiceDiscovery) SetConfig(_ map[string]interface{}) error {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	d.setConfig = true

	return nil
}

func (d *dummyServiceDiscovery) SetLogger(_ *log.Logger) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	d.setLogger = true
}

func (d *dummyServiceDiscovery) Register() error {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	d.register = true

	return nil
}

func (d *dummyServiceDiscovery) Deregister() error {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	d.deregister = true

	return nil
}

func (d *dummyServiceDiscovery) DiscoverPeers() ([]string, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	d.discoverPeers = true

	return []string{}, nil
}

func (d *dummyServiceDiscovery) Close() error {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	d.closed = true

	return nil
}

var _ service_discovery.ServiceDiscovery = (*dummyServiceDiscovery)(nil)

func TestDiscovery_loadServiceDiscoveryPlugin(t *testing.T) {
	c := testutil.NewConfig()

	sd := &dummyServiceDiscovery{}
	c.ServiceDiscovery = map[string]interface{}{
		"plugin":   sd,
		"provider": "dummy",
		"args":     fmt.Sprintf("namespace=%s label_selector=\"%s\"", "foo_namespace", "foo_label_selector"),
	}

	f := testutil.NewFlogger(c)
	d := New(f, c)
	err := d.Start()
	require.NoError(t, err)

	_, err = d.Join()
	require.NoError(t, err)

	require.True(t, sd.initialized)
	require.True(t, sd.setConfig)
	require.True(t, sd.setLogger)
	require.True(t, sd.register)
	require.True(t, sd.discoverPeers)
}

func TestDiscovery_ClusterEvents(t *testing.T) {
	c := newTestCluster(t)
	d1 := c.addNewMember(t)
	d2 := c.addNewMember(t)
	d3 := c.addNewMember(t)

	var members []string
loop:
	for {
		select {
		case e := <-d1.ClusterEvents:
			require.Equal(t, memberlist.NodeJoin, e.Event)
			members = append(members, e.MemberAddr())
			if len(members) == 2 {
				break loop
			}
		case <-time.After(2 * time.Second):
			break loop
		}
	}

	require.Contains(t, members, net.JoinHostPort(d2.config.MemberlistConfig.BindAddr, strconv.Itoa(d2.config.MemberlistConfig.BindPort)))
	require.Contains(t, members, net.JoinHostPort(d3.config.MemberlistConfig.BindAddr, strconv.Itoa(d3.config.MemberlistConfig.BindPort)))
}
