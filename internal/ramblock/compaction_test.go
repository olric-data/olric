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

package ramblock

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/olric-data/olric/internal/ramblock/entry"
	"github.com/olric-data/olric/internal/ramblock/table"
	"github.com/olric-data/olric/pkg/storage"
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

func putStringEntry(t *testing.T, s storage.Engine, key, value string) {
	t.Helper()

	e := entry.New()
	e.SetKey(key)
	e.SetValue([]byte(value))
	e.SetTTL(1)
	e.SetTimestamp(time.Now().UnixNano())

	hkey := xxhash.Sum64([]byte(key))
	require.NoError(t, s.Put(hkey, e))
}

func compactUntilDone(t *testing.T, s storage.Engine) {
	t.Helper()

	for i := 0; i < 10000; i++ {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			return
		}
	}
	t.Fatal("compaction did not finish")
}

// TestRamBlock_Compaction_PurgesStaleEntries repeatedly overwrites a key with
// a value close to tableSize. Since there is no room for a second entry, every
// overwrite lands in a new table and leaves the stale copy in a read-only
// table. Compaction has to drop the stale copies and recycle the emptied
// tables instead of accumulating an unbounded number of tables.
func TestRamBlock_Compaction_PurgesStaleEntries(t *testing.T) {
	const tableSize = 16 * 1024
	c := DefaultConfig()
	c.Add("tableSize", uint64(tableSize))
	c.Add("purgeStaleEntries", true)
	s := testRamBlock(t, c)

	key := "k"
	valueLen := tableSize - len(key) - table.MetadataLength - 100
	hkey := xxhash.Sum64([]byte(key))

	const writes = 50
	for i := 0; i < writes; i++ {
		putStringEntry(t, s, key, fmt.Sprintf("%0*d", valueLen, i))
	}

	rb := s.(*RamBlock)
	require.Equal(t, writes, len(rb.tables))

	compactUntilDone(t, s)

	// Only the latest version must survive.
	stats := s.Stats()
	require.Equal(t, 1, stats.Length)
	require.Equal(t, tableSize-100, stats.Inuse)

	res, err := s.Get(hkey)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%0*d", valueLen, writes-1), string(res.Value()))

	var recycled int
	for _, tb := range rb.tables {
		if tb.State() == table.RecycledState {
			recycled++
		}
	}
	require.Equal(t, writes-1, recycled)
}

// TestRamBlock_Compaction_PurgeDisabledByDefault pins the opt-in semantics of
// the stale entry purge: with the default configuration
// (purgeStaleEntries=false) compaction must keep overwritten entries to stay
// compatible with the upstream behavior, so the read-only tables holding the
// stale copies are neither purged nor recycled.
func TestRamBlock_Compaction_PurgeDisabledByDefault(t *testing.T) {
	const tableSize = 16 * 1024
	c := DefaultConfig()
	c.Add("tableSize", uint64(tableSize))
	s := testRamBlock(t, c)

	key := "k"
	valueLen := tableSize - len(key) - table.MetadataLength - 100
	hkey := xxhash.Sum64([]byte(key))

	const writes = 50
	for i := 0; i < writes; i++ {
		putStringEntry(t, s, key, fmt.Sprintf("%0*d", valueLen, i))
	}

	rb := s.(*RamBlock)
	require.Equal(t, writes, len(rb.tables))

	compactUntilDone(t, s)

	// The purge is opt-in, so the stale copies survive and no read-only table
	// is recycled.
	require.Equal(t, writes, s.Stats().Length)
	require.Equal(t, writes, len(rb.tables))

	var recycled int
	for _, tb := range rb.tables {
		if tb.State() == table.RecycledState {
			recycled++
		}
	}
	require.Equal(t, 0, recycled)

	// The latest version must still be readable.
	res, err := s.Get(hkey)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%0*d", valueLen, writes-1), string(res.Value()))
}

// TestRamBlock_Compaction_PurgeKeepsLiveEntries verifies that the stale entry
// purge never drops live data, even when overwritten and untouched keys are
// spread over multiple tables.
func TestRamBlock_Compaction_PurgeKeepsLiveEntries(t *testing.T) {
	const tableSize = 1024
	c := DefaultConfig()
	c.Add("tableSize", uint64(tableSize))
	c.Add("purgeStaleEntries", true)
	s := testRamBlock(t, c)

	// Every entry is 9 + 62 + 29 = 100 bytes, so ten entries fit into a table.
	const valueLen = 62
	const numKeys = 30
	for i := 0; i < numKeys; i++ {
		putStringEntry(t, s, bkey(i), fmt.Sprintf("%0*d", valueLen, i))
	}
	// Overwrite the first ten keys: the stale copies stay in the oldest table
	// and the new versions are written to a new table.
	for i := 0; i < 10; i++ {
		putStringEntry(t, s, bkey(i), fmt.Sprintf("%0*d", valueLen, i+1000))
	}

	compactUntilDone(t, s)

	require.Equal(t, numKeys, s.Stats().Length)
	for i := 0; i < numKeys; i++ {
		res, err := s.Get(xxhash.Sum64([]byte(bkey(i))))
		require.NoError(t, err)

		want := fmt.Sprintf("%0*d", valueLen, i)
		if i < 10 {
			want = fmt.Sprintf("%0*d", valueLen, i+1000)
		}
		require.Equal(t, want, string(res.Value()))
	}
}

// TestRamBlock_Compaction_PurgeThenEvict verifies the handover between the
// stale entry purge and the eviction pass: the purge turns overwritten entries
// into garbage, and the eviction pass then migrates the surviving entries and
// recycles the table.
func TestRamBlock_Compaction_PurgeThenEvict(t *testing.T) {
	const tableSize = 1024
	c := DefaultConfig()
	c.Add("tableSize", uint64(tableSize))
	c.Add("purgeStaleEntries", true)
	s := testRamBlock(t, c)

	rb := s.(*RamBlock)

	// Ten entries (98 bytes each) fill the oldest table.
	for i := 0; i < 10; i++ {
		putStringEntry(t, s, bkey(i), fmt.Sprintf("%060d", i))
	}
	// Overwrite five of them. The stale copies stay in the oldest table.
	for i := 0; i < 5; i++ {
		putStringEntry(t, s, bkey(i), fmt.Sprintf("%060d", i+1000))
	}

	compactUntilDone(t, s)

	// The oldest table must have been recycled and every value must still be
	// readable.
	require.Equal(t, 10, s.Stats().Length)
	require.Equal(t, table.RecycledState, rb.tables[0].State())
	for i := 0; i < 10; i++ {
		res, err := s.Get(xxhash.Sum64([]byte(bkey(i))))
		require.NoError(t, err)

		want := fmt.Sprintf("%060d", i)
		if i < 5 {
			want = fmt.Sprintf("%060d", i+1000)
		}
		require.Equal(t, want, string(res.Value()))
	}
}

// TestRamBlock_EvictTable_DoesNotReviveStaleEntries verifies that the
// eviction pass never copies a stale entry back to the latest table. Doing so
// would point the hkey of the newer version to the older value.
func TestRamBlock_EvictTable_DoesNotReviveStaleEntries(t *testing.T) {
	const tableSize = 1024
	c := DefaultConfig()
	c.Add("tableSize", uint64(tableSize))
	s := testRamBlock(t, c)

	rb := s.(*RamBlock)

	// Fill the oldest table with a version of key "k" followed by filler
	// entries.
	putStringEntry(t, s, "k", strings.Repeat("a", 62))
	for i := 0; i < 9; i++ {
		putStringEntry(t, s, bkey(i), strings.Repeat("f", 62))
	}
	// The oldest table is full. The newer version of "k" goes to a new table.
	putStringEntry(t, s, "k", strings.Repeat("b", 62))
	require.Equal(t, 2, len(rb.tables))

	// Delete the filler entries to make the oldest table garbage-heavy.
	for i := 0; i < 9; i++ {
		require.NoError(t, s.Delete(xxhash.Sum64([]byte(bkey(i)))))
	}

	// Evict the oldest table directly. The stale copy of "k" must be dropped
	// instead of being copied to the latest table, where it would shadow the
	// newer version.
	require.NoError(t, rb.evictTable(rb.tables[0]))

	res, err := s.Get(xxhash.Sum64([]byte("k")))
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("b", 62), string(res.Value()))
	require.Equal(t, table.RecycledState, rb.tables[0].State())
}

// TestRamBlock_Compaction_SingleTable makes sure a single table is never
// purged: there cannot be a stale copy without a younger table.
func TestRamBlock_Compaction_SingleTable(t *testing.T) {
	s := testRamBlock(t, nil)

	for i := 0; i < 10; i++ {
		putStringEntry(t, s, bkey(i), strings.Repeat("a", 24))
	}

	compactUntilDone(t, s)

	require.Equal(t, 10, s.Stats().Length)
	for i := 0; i < 10; i++ {
		_, err := s.Get(xxhash.Sum64([]byte(bkey(i))))
		require.NoError(t, err)
	}
}

// TestRamBlock_Compaction_GarbageHeavyWritableTable repeatedly overwrites a
// key that fits many times into a single table. The latest table itself
// becomes garbage-heavy, and eviction has to rotate it first. Otherwise its
// entries would be copied back into itself, Inuse would never drop to zero and
// Compaction would never report that it is done.
func TestRamBlock_Compaction_GarbageHeavyWritableTable(t *testing.T) {
	const tableSize = 16 * 1024
	c := DefaultConfig()
	c.Add("tableSize", uint64(tableSize))
	s := testRamBlock(t, c)

	const writes = 100
	for i := 0; i < writes; i++ {
		putStringEntry(t, s, "k", fmt.Sprintf("%0250d", i))
	}

	compactUntilDone(t, s)

	require.Equal(t, 1, s.Stats().Length)

	res, err := s.Get(xxhash.Sum64([]byte("k")))
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%0250d", writes-1), string(res.Value()))

	var recycled int
	for _, tb := range s.(*RamBlock).tables {
		if tb.State() == table.RecycledState {
			recycled++
		}
	}
	require.GreaterOrEqual(t, recycled, 1)
}
