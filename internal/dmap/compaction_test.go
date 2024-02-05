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
	"fmt"
	"github.com/buraksezer/olric/internal/kvstore"
	"github.com/buraksezer/olric/pkg/storage"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
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
	kv, err := kvstore.New(storage.NewConfig(c.DMaps.Engine.Config))
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
