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

package kvstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/buraksezer/olric/internal/kvstore/table"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestKVStore_Compaction(t *testing.T) {
	s := testKVStore(t, nil)

	timestamp := time.Now().UnixNano()
	// Current free space is 1 MB. Trigger a compaction operation.
	for i := 0; i < 1500; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTTL(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 750; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)
	}

	for {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			break
		}
	}

	var compacted bool
	for _, tb := range s.(*KVStore).tables {
		stats := tb.Stats()
		if stats.Inuse == 0 {
			require.Equal(t, table.RecycledState, tb.State())
			compacted = true
		} else {
			require.Equal(t, 750, stats.Length)
			require.Equal(t, table.ReadWriteState, tb.State())
		}
	}

	require.Truef(t, compacted, "Compaction could not work properly")
}

func TestKVStore_Compaction_MaxIdleTableDuration(t *testing.T) {
	c := DefaultConfig()
	c.Add("maxIdleTableTimeout", time.Millisecond)

	s := testKVStore(t, c)

	timestamp := time.Now().UnixNano()
	// Current free space is 1 MB. Trigger a compaction operation.
	for i := 0; i < 1500; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTTL(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	require.Equal(t, 2, len(s.(*KVStore).tables))

	for i := 0; i < 800; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)
	}

	// It's still two because we have not triggered the compaction yet.
	require.Equal(t, 2, len(s.(*KVStore).tables))

	for {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			break
		}
	}

	<-time.After(100 * time.Millisecond)

	// Be sure deletion of the idle table.
	for {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			break
		}
	}

	require.Equal(t, 1, len(s.(*KVStore).tables))
}
