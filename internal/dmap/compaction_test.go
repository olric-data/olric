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
	"fmt"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/ramblock"
	"github.com/olric-data/olric/pkg/storage"

	"github.com/olric-data/olric/config"
	"github.com/olric-data/olric/internal/testcluster"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDMap_Compaction(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps.TriggerCompactionInterval = time.Millisecond
	c.DMaps.Engine.Name = config.DefaultStorageEngine

	c.DMaps.Engine.Config = map[string]interface{}{
		"tableSize":           uint64(2048), // overwrite tableSize to trigger compaction.
		"maxIdleTableTimeout": time.Millisecond,
	}
	kv, err := ramblock.New(storage.NewConfig(c.DMaps.Engine.Config))
	require.NoError(t, err)
	c.DMaps.Engine.Implementation = kv

	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	checkStorageStats := func() (allocated int) {
		for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
			part := s.primary.PartitionByID(partID)
			tmp, ok := part.Map().Load(s.fragmentName("mymap"))
			if !ok {
				continue
			}

			f := tmp.(*fragment)
			f.RLock()
			s := f.storage.Stats()
			allocated += s.Allocated
			f.RUnlock()
		}
		return
	}

	dm, err := s.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	initialAllocated := checkStorageStats()

	for i := 0; i < 10000; i++ {
		if i%2 != 0 {
			continue
		}

		_, err = dm.Delete(ctx, testutil.ToKey(i))
		require.NoError(t, err)
	}

	err = testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		allocated := checkStorageStats()
		if initialAllocated <= allocated {
			return fmt.Errorf("initial allocation is still greater than or equal the current allocation")
		}
		return nil
	})
	require.NoError(t, err)
}

// TestFragment_Compaction_ClosedFragmentReportsDone guards against a regression
// where a closed fragment made Compaction return (false, nil). That value sends
// callCompactionOnFragment into an endless retry loop (it only stops on
// done=true or a non-nil error), which blocks triggerCompaction's wg.Wait() and
// stops compactionWorker for the whole node, letting garbage grow until the
// process is OOM-killed.
func TestFragment_Compaction_ClosedFragmentReportsDone(t *testing.T) {
	kv, err := ramblock.New(storage.NewConfig(map[string]interface{}{
		"tableSize":           uint64(2048),
		"maxIdleTableTimeout": time.Millisecond,
	}))
	require.NoError(t, err)
	require.NoError(t, kv.Start())

	ctx, cancel := context.WithCancel(context.Background())
	f := &fragment{
		storage: kv,
		ctx:     ctx,
		cancel:  cancel,
	}

	done, err := f.Compaction()
	require.NoError(t, err)
	require.True(t, done, "empty storage must report compaction as done")

	require.NoError(t, f.Close())

	done, err = f.Compaction()
	require.NoError(t, err)
	require.True(t, done, "a closed fragment must report compaction as done; returning false loops forever")
}

// TestCallCompactionOnFragment_ClosedFragmentReturnsImmediately exercises the
// real caller path. callCompactionOnFragment retries every millisecond while
// Compaction reports done=false, so a closed fragment used to hang this call
// forever and, through triggerCompaction's wg.Wait(), stop compactionWorker for
// the whole node.
func TestCallCompactionOnFragment_ClosedFragmentReturnsImmediately(t *testing.T) {
	kv, err := ramblock.New(storage.NewConfig(map[string]interface{}{
		"tableSize":           uint64(2048),
		"maxIdleTableTimeout": time.Millisecond,
	}))
	require.NoError(t, err)
	require.NoError(t, kv.Start())

	ctx, cancel := context.WithCancel(context.Background())
	f := &fragment{
		storage: kv,
		ctx:     ctx,
		cancel:  cancel,
	}
	require.NoError(t, f.Close())

	s := &Service{ctx: context.Background()}

	returned := make(chan bool, 1)
	go func() { returned <- s.callCompactionOnFragment(f) }()

	select {
	case done := <-returned:
		require.True(t, done)
	case <-time.After(2 * time.Second):
		t.Fatal("callCompactionOnFragment blocked on a closed fragment; the compaction worker would never run again")
	}
}
