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

package olric

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/stats"
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

// newTestOlricWithConfig creates a new Olric instance with the given configuration.
// This function is intended for internal use. Please use testOlricCluster and its
// methods to form a cluster in tests.
func newTestOlricWithConfig(t *testing.T, c *config.Config) *Olric {
	port, err := testutil.GetFreePort()
	require.NoError(t, err)

	if c.MemberlistConfig == nil {
		c.MemberlistConfig = memberlist.DefaultLocalConfig()
	}
	c.MemberlistConfig.BindPort = 0

	c.BindAddr = "127.0.0.1"
	c.BindPort = port

	err = c.Sanitize()
	require.NoError(t, err)

	err = c.Validate()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		cancel()
	}

	db, err := New(c)
	require.NoError(t, err)

	go func() {
		if err := db.Start(); err != nil {
			panic(fmt.Sprintf("Failed to run Olric: %v", err))
		}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatalf("Olric cannot be started in one second")
	case <-ctx.Done():
		// everything is fine
	}

	return db
}

type testOlricCluster struct {
	mtx     sync.Mutex
	members map[string]*Olric
}

func newTestOlricCluster(t *testing.T) *testOlricCluster {
	cl := &testOlricCluster{members: make(map[string]*Olric)}
	t.Cleanup(func() {
		cl.mtx.Lock()
		defer cl.mtx.Unlock()
		for _, member := range cl.members {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := member.Shutdown(ctx)
			cancel()
			require.NoError(t, err)
		}
	})
	return cl
}

func (cl *testOlricCluster) addMemberWithConfig(t *testing.T, c *config.Config) *Olric {
	cl.mtx.Lock()
	defer cl.mtx.Unlock()

	if c == nil {
		c = testutil.NewConfig()
	}

	for _, member := range cl.members {
		c.Peers = append(c.Peers, member.rt.Discovery().LocalNode().Address())
	}

	db := newTestOlricWithConfig(t, c)
	cl.members[db.rt.This().String()] = db
	t.Logf("A new cluster member has been created: %s", db.rt.This())
	return db
}

func (cl *testOlricCluster) addMember(t *testing.T) *Olric {
	return cl.addMemberWithConfig(t, nil)
}

func TestOlric_StartAndShutdown(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	err := db.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestOlricCluster_StartAndShutdown(t *testing.T) {
	cluster := newTestOlricCluster(t)
	cluster.addMember(t)
	db := cluster.addMember(t)
	require.Len(t, cluster.members, 2)

	e := db.NewEmbeddedClient()
	st, err := e.Stats(context.Background(), db.rt.This().String())
	require.NoError(t, err)
	require.Len(t, st.ClusterMembers, 2)
	for _, member := range cluster.members {
		require.Contains(t, st.ClusterMembers, stats.MemberID(member.rt.This().ID))
	}
}
