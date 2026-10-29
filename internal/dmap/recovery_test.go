// Copyright 2018-2026 The Olric Authors
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
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/cluster/routingtable"
	"github.com/olric-data/olric/internal/discovery"
	"github.com/olric-data/olric/internal/protocol"
	"github.com/olric-data/olric/internal/server"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// newBlockedDeleteDMap uses a real RESP peer which accepts deletes but does not
// reply until released. No background workers run, so the tests control expiry.
func newBlockedDeleteDMap(t *testing.T, timeout time.Duration) (*DMap, *fragment, <-chan struct{}, func()) {
	t.Helper()
	c := testutil.NewConfig()
	c.PartitionCount = 1
	partitions.SetHashFunc(c.Hasher)
	c.Client.ReadTimeout = timeout
	c.Client.MaxRetries = -1
	peer := testutil.NewServer(c)
	entered := make(chan struct{}, 1)
	released := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(released) }) }
	peer.ServeMux().HandleFunc(protocol.Generic.Ping, func(conn redcon.Conn, _ redcon.Command) {
		conn.WriteString("PONG")
	})
	blockDelete := func(conn redcon.Conn, _ redcon.Command) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-released
		conn.WriteInt(1)
	}
	peer.ServeMux().HandleFunc(protocol.DMap.DelEntry, blockDelete)
	peer.ServeMux().HandleFunc(protocol.DMap.Del, blockDelete)
	serverDone := make(chan error, 1)
	go func() { serverDone <- peer.ListenAndServe() }()
	select {
	case <-peer.StartedCtx.Done():
	case err := <-serverDone:
		t.Fatalf("failed to start peer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		config:  c,
		log:     testutil.NewFlogger(c),
		client:  server.NewClient(c.Client),
		rt:      &routingtable.RoutingTable{},
		primary: partitions.New(1, partitions.PRIMARY),
		backup:  partitions.New(1, partitions.BACKUP),
		dmaps:   make(map[string]*DMap),
		ctx:     ctx,
		cancel:  cancel,
	}
	t.Cleanup(func() {
		release()
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		err := s.client.Shutdown(shutdownCtx)
		if err != nil {
			t.Error(err)
		}
		err = peer.Shutdown(shutdownCtx)
		if err != nil {
			t.Error(err)
		}
		err = <-serverDone
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	addr := net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))
	err := s.client.Get(addr).Ping(ctx).Err()
	require.NoError(t, err)
	// The zero member matches rt.This(), so foreground operations run locally.
	s.primary.PartitionByID(0).SetOwners([]discovery.Member{discovery.NewMember(c), {}})
	s.backup.PartitionByID(0).SetOwners([]discovery.Member{})
	dm := &DMap{name: "expiry", fragmentName: "dmap.expiry", s: s, config: &dmapConfig{}}
	err = dm.config.load(c.DMaps, dm.name)
	require.NoError(t, err)
	dm.engine = dm.config.engine.Implementation
	s.dmaps[dm.name] = dm
	f, err := dm.loadOrCreateFragment(s.primary.PartitionByID(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		err := f.Close()
		if err != nil {
			t.Error(err)
		}
		err = f.Destroy()
		if err != nil {
			t.Error(err)
		}
	})
	return dm, f, entered, release
}

func TestDMap_Delete_CallerDeadline(t *testing.T) {
	for _, name := range []string{"previous owner", "replica", "remote primary"} {
		t.Run(name, func(t *testing.T) {
			dm, f, entered, release := newBlockedDeleteDMap(t, time.Second)
			local := name != "remote primary"
			if local {
				err := dm.Put(context.Background(), "key", []byte("old"), nil)
				require.NoError(t, err)
			}
			switch name {
			case "replica":
				peer := dm.s.primary.PartitionByID(0).Owners()[0]
				dm.s.primary.PartitionByID(0).SetOwners([]discovery.Member{{}})
				dm.s.backup.PartitionByID(0).SetOwners([]discovery.Member{peer})
				dm.s.config.ReplicaCount = 2
			case "remote primary":
				peer := dm.s.primary.PartitionByID(0).Owners()[0]
				dm.s.primary.PartitionByID(0).SetOwners([]discovery.Member{peer})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			done := make(chan struct{})
			var deleteErr error
			go func() {
				_, deleteErr = dm.Delete(ctx, "key")
				close(done)
			}()
			t.Cleanup(func() { release(); <-done })
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("delete did not reach the remote owner")
			}
			select {
			case <-done:
				require.ErrorIs(t, deleteErr, context.DeadlineExceeded)
			case <-time.After(500 * time.Millisecond):
				t.Fatal("remote delete ignored the caller deadline")
			}
			if local {
				require.True(t, f.TryLock(), "timed-out delete kept the fragment locked")
				exists := f.storage.Check(partitions.HKey(dm.name, "key"))
				f.Unlock()
				require.True(t, exists, "failed remote delete removed the local value")
			}
		})
	}
}

func TestDMap_Delete_RemoteContextError(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "canceled"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			dm, _, _, _ := newBlockedDeleteDMap(t, time.Second)
			peer := dm.s.primary.PartitionByID(0).Owners()[0]
			dm.s.primary.PartitionByID(0).SetOwners([]discovery.Member{peer})
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if deadline {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
			}
			_, err := dm.Delete(ctx, "key")
			require.ErrorIs(t, err, ctx.Err())
		})
	}
}

func TestDMap_Fragment_TryStats(t *testing.T) {
	dm, f, _, _ := newBlockedDeleteDMap(t, time.Second)
	err := dm.Put(context.Background(), "key", []byte("value"), nil)
	require.NoError(t, err)
	stats, available := f.TryStats()
	require.True(t, available)
	require.Equal(t, 1, stats.Length)
	f.Lock()
	_, available = f.TryStats()
	f.Unlock()
	require.False(t, available, "busy fragment returned statistics")
	stats, available = f.TryStats()
	require.True(t, available, "statistics unavailable after unlocking")
	require.Equal(t, 1, stats.Length)
}

func TestDMap_Eviction_BoundedBatchPreservesConcurrentPut(t *testing.T) {
	dm, f, entered, release := newBlockedDeleteDMap(t, 200*time.Millisecond)
	for i := range 10 {
		key := strconv.Itoa(i)
		err := dm.Put(context.Background(), key, []byte("old"), nil)
		require.NoError(t, err)
		f.Lock()
		entry := f.storage.NewEntry()
		entry.SetTTL(time.Now().Add(-time.Minute).UnixMilli())
		err = f.storage.UpdateTTL(partitions.HKey(dm.name, key), entry)
		f.Unlock()
		require.NoError(t, err)
	}
	evicted := make(chan struct{})
	go func() {
		dm.s.scanFragmentForEviction(0, dm.name, f)
		close(evicted)
	}()
	t.Cleanup(func() { release(); <-evicted })
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("eviction did not reach the remote owner")
	}
	written := make(chan struct{})
	var writeErr error
	go func() {
		writeErr = dm.Put(context.Background(), "0", []byte("fresh"), nil)
		close(written)
	}()
	t.Cleanup(func() { release(); <-written })
	select {
	case <-evicted:
	case <-time.After(time.Second):
		t.Fatal("eviction gave each expired key a fresh timeout")
	}
	<-written
	require.NoError(t, writeErr)
	release()
	// Retrying expiry after the stalled peer recovers must preserve the write
	// which arrived while the first eviction pass owned the fragment lock.
	dm.s.scanFragmentForEviction(0, dm.name, f)
	f.RLock()
	entry, err := f.storage.Get(partitions.HKey(dm.name, "0"))
	f.RUnlock()
	require.NoError(t, err)
	require.Equal(t, []byte("fresh"), entry.Value(), "expiry replaced the concurrent write")
}
