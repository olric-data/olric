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

package ramblock

import (
	"fmt"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/olric-data/olric/internal/ramblock/entry"
	"github.com/olric-data/olric/internal/ramblock/table"
	"github.com/stretchr/testify/require"
)

func TestRamBlock_Compaction(t *testing.T) {
	s := testRamBlock(t, nil)

	timestamp := time.Now().UnixNano()
	// The current free space is 1 MB. Trigger a compaction operation.
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
	for _, tb := range s.(*RamBlock).tables {
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

func TestRamBlock_Compaction_MaxIdleTableDuration(t *testing.T) {
	c := DefaultConfig()
	c.Add("maxIdleTableTimeout", time.Millisecond)

	s := testRamBlock(t, c)

	timestamp := time.Now().UnixNano()
	// The current free space is 1 MB. Trigger a compaction operation.
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

	require.Equal(t, 2, len(s.(*RamBlock).tables))

	for i := 0; i < 800; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)
	}

	// It's still two because we have not triggered the compaction yet.
	require.Equal(t, 2, len(s.(*RamBlock).tables))

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

	require.Equal(t, 1, len(s.(*RamBlock).tables))
}
