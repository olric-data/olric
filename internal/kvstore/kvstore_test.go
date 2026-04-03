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

package kvstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/olric-data/olric/internal/kvstore/entry"
	"github.com/olric-data/olric/internal/kvstore/table"
	"github.com/olric-data/olric/pkg/storage"
	"github.com/stretchr/testify/require"
)

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func testKVStore(t *testing.T, c *storage.Config) storage.Engine {
	kv, err := New(c)
	require.NoError(t, err)

	child, err := kv.Fork(nil)
	require.NoError(t, err)

	err = child.Start()
	require.NoError(t, err)

	return child
}

func TestKVStore_Put(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTTL(int64(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}
}

func TestKVStore_Get(t *testing.T) {
	s := testKVStore(t, nil)

	timestamp := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := s.Get(hkey)
		require.NoError(t, err)

		require.Equal(t, bkey(i), e.Key())
		require.Equal(t, int64(i), e.TTL())
		require.Equal(t, bval(i), e.Value())
		require.Equal(t, timestamp, e.Timestamp())
	}
}

func TestKVStore_Delete(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	garbage := make(map[int]uint64)
	for i, tb := range s.(*KVStore).tables {
		s := tb.Stats()
		garbage[i] = s.Inuse
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)

		_, err = s.Get(hkey)
		require.ErrorIs(t, err, storage.ErrKeyNotFound)
	}

	for i, tb := range s.(*KVStore).tables {
		s := tb.Stats()
		require.Equal(t, uint64(0), s.Inuse)
		require.Equal(t, 0, s.Length)
		require.Equal(t, garbage[i], s.Garbage)
	}
}

func TestKVStore_ExportImport(t *testing.T) {
	timestamp := time.Now().UnixNano()
	s := testKVStore(t, nil)

	for i := 0; i < 1000; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	fresh := testKVStore(t, nil)

	ti := s.TransferIterator()
	for ti.Next() {
		data, index, err := ti.Export()
		require.NoError(t, err)

		err = fresh.Import(data, func(u uint64, e storage.Entry) error {
			return fresh.Put(u, e)
		})
		require.NoError(t, err)

		err = ti.Drop(index)
		require.NoError(t, err)
	}

	_, _, err := ti.Export()
	require.ErrorIs(t, err, io.EOF)

	for i := 0; i < 1000; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := fresh.Get(hkey)
		require.NoError(t, err)
		require.Equal(t, bkey(i), e.Key())
		require.Equal(t, int64(i), e.TTL())
		require.Equal(t, bval(i), e.Value())
		require.Equal(t, timestamp, e.Timestamp())
	}
}

func TestKVStore_Stats_Length(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	require.Equal(t, 100, s.Stats().Length)
}

func TestKVStore_Range(t *testing.T) {
	s := testKVStore(t, nil)

	hkeys := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)

		hkeys[hkey] = struct{}{}
	}

	s.Range(func(hkey uint64, entry storage.Entry) bool {
		_, ok := hkeys[hkey]
		require.Truef(t, ok, "Invalid hkey: %d", hkey)
		return true
	})
}

func TestKVStore_Check(t *testing.T) {
	s := testKVStore(t, nil)

	hkeys := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)

		hkeys[hkey] = struct{}{}
	}

	for hkey := range hkeys {
		require.Truef(t, s.Check(hkey), "hkey could not be found: %d", hkey)
	}
}

func TestKVStore_UpdateTTL(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(10)
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.UpdateTTL(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := s.Get(hkey)
		require.NoError(t, err)

		if e.Key() != bkey(i) {
			t.Fatalf("Expected key: %s. Got %s", bkey(i), e.Key())
		}
		if e.TTL() != 10 {
			t.Fatalf("Expected ttl: %d. Got %v", i, e.TTL())
		}
	}
}

func TestKVStore_GetKey(t *testing.T) {
	s := testKVStore(t, nil)

	e := entry.New()
	e.SetKey(bkey(1))
	e.SetTTL(int64(1))
	e.SetValue(bval(1))
	hkey := xxhash.Sum64([]byte(e.Key()))
	err := s.Put(hkey, e)
	require.NoError(t, err)

	key, err := s.GetKey(hkey)
	require.NoError(t, err)

	if key != bkey(1) {
		t.Fatalf("Expected %s. Got %v", bkey(1), key)
	}
}

func TestKVStore_PutRawGetRaw(t *testing.T) {
	s := testKVStore(t, nil)

	value := []byte("value")
	hkey := xxhash.Sum64([]byte("key"))
	err := s.PutRaw(hkey, value)
	require.NoError(t, err)

	rawval, err := s.GetRaw(hkey)
	require.NoError(t, err)

	if bytes.Equal(value, rawval) {
		t.Fatalf("Expected %s. Got %v", value, rawval)
	}
}

func TestKVStore_GetTTL(t *testing.T) {
	s := testKVStore(t, nil)

	e := entry.New()
	e.SetKey(bkey(1))
	e.SetTTL(int64(1))
	e.SetValue(bval(1))

	hkey := xxhash.Sum64([]byte(e.Key()))
	err := s.Put(hkey, e)
	require.NoError(t, err)

	ttl, err := s.GetTTL(hkey)
	require.NoError(t, err)

	if ttl != e.TTL() {
		t.Fatalf("Expected TTL %d. Got %d", ttl, e.TTL())
	}
}

func TestKVStore_GetLastAccess(t *testing.T) {
	s := testKVStore(t, nil)

	e := entry.New()
	e.SetKey(bkey(1))
	e.SetTTL(int64(1))
	e.SetValue(bval(1))

	hkey := xxhash.Sum64([]byte(e.Key()))
	err := s.Put(hkey, e)
	require.NoError(t, err)

	lastAccess, err := s.GetLastAccess(hkey)
	require.NoError(t, err)
	require.NotEqual(t, 0, lastAccess)
}

func TestKVStore_Fork(t *testing.T) {
	s := testKVStore(t, nil)

	timestamp := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	child, err := s.Fork(nil)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		_, err = child.Get(hkey)
		if !errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("Expected storage.ErrKeyNotFound. Got %v", err)
		}
	}

	stats := child.Stats()
	if uint64(stats.Allocated) != defaultTableSize {
		t.Fatalf("Expected Stats.Allocated: %d. Got: %d", defaultTableSize, stats.Allocated)
	}

	if stats.Inuse != 0 {
		t.Fatalf("Expected Stats.Inuse: 0. Got: %d", stats.Inuse)
	}

	if stats.Garbage != 0 {
		t.Fatalf("Expected Stats.Garbage: 0. Got: %d", stats.Garbage)
	}

	if stats.Length != 0 {
		t.Fatalf("Expected Stats.Length: 0. Got: %d", stats.Length)
	}

	if stats.NumTables != 1 {
		t.Fatalf("Expected Stats.NumTables: 1. Got: %d", stats.NumTables)
	}
}

func TestKVStore_StateChange(t *testing.T) {
	s := testKVStore(t, nil)

	timestamp := time.Now().UnixNano()
	// Current free space is 1 MB. Trigger a compaction operation.
	for i := 0; i < 100000; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTTL(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i, tb := range s.(*KVStore).tables {
		if tb.State() == table.ReadWriteState {
			require.Equalf(t, len(s.(*KVStore).tables)-1, i, "Writable table has to be the latest table")
		} else if tb.State() == table.ReadOnlyState {
			require.True(t, i < len(s.(*KVStore).tables)-1)
		}
	}
}

func TestKVStore_NewEntry(t *testing.T) {
	s := testKVStore(t, nil)

	i := s.NewEntry()
	_, ok := i.(*entry.Entry)
	require.True(t, ok)
}

func TestKVStore_Name(t *testing.T) {
	s := testKVStore(t, nil)
	require.Equal(t, "kvstore", s.Name())
}

func TestKVStore_CloseDestroy(t *testing.T) {
	s := testKVStore(t, nil)
	require.NoError(t, s.Close())
	require.NoError(t, s.Destroy())
}

func TestStorage_Scan(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 1000000; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	var (
		count  int
		cursor uint64
		err    error
	)
	k := s.(*KVStore)
	for {
		cursor, err = k.Scan(cursor, 10, func(e storage.Entry) bool {
			count++
			return true
		})
		require.NoError(t, err)
		if cursor == 0 {
			break
		}
	}

	require.Equal(t, 1000000, count)
}

func TestStorage_ScanRegexMatch(t *testing.T) {
	s := testKVStore(t, nil)

	var key string
	for i := 0; i < 1000000; i++ {
		if i%2 == 0 {
			key = "even:" + strconv.Itoa(i)
		} else {
			key = "odd:" + strconv.Itoa(i)
		}

		e := entry.New()
		e.SetKey(key)
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	var (
		count  int
		cursor uint64
		err    error
	)
	k := s.(*KVStore)
	for {
		cursor, err = k.ScanRegexMatch(cursor, "even:", 10, func(entry storage.Entry) bool {
			count++
			return true
		})
		require.NoError(t, err)
		if cursor == 0 {
			break
		}
	}

	require.Equal(t, 500000, count)
}

func TestStorage_ScanRegexMatch_OnlyOneEntry(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	e := entry.New()
	e.SetKey("even:200")
	e.SetTTL(123123)
	e.SetValue([]byte("my-value"))
	e.SetTimestamp(time.Now().UnixNano())
	hkey := xxhash.Sum64([]byte(e.Key()))
	err := s.Put(hkey, e)
	require.NoError(t, err)

	var (
		num    int
		count  int
		cursor uint64
	)
	k := s.(*KVStore)
	for {
		num += 1
		cursor, err = k.ScanRegexMatch(cursor, "even:", 10, func(entry storage.Entry) bool {
			count++
			require.Equal(t, "even:200", e.Key())
			require.Equal(t, "my-value", string(e.Value()))
			return true
		})
		require.NoError(t, err)
		if cursor == 0 {
			break
		}
	}

	require.Equal(t, 1, num)
	require.Equal(t, 1, count)
}

func TestStorage_Scan_NonContiguousCoefficients(t *testing.T) {
	// Use a small tableSize so that multiple tables are created quickly.
	c := DefaultConfig()
	c.Add("tableSize", 1024)
	s := testKVStore(t, c)
	k := s.(*KVStore)

	// Insert enough entries to create several tables (at least 4).
	for i := 0; i < 200; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTTL(int64(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	require.Greater(t, len(k.tables), 3, "need at least 4 tables for this test")

	// Count entries per table before deletion.
	totalBefore := 0
	for _, tbl := range k.tables {
		totalBefore += tbl.Stats().Length
	}

	// Pick a middle table to delete (simulate compaction gap).
	// Find a table that is not the first or last and has entries.
	var deletedTable *table.Table
	for _, tbl := range k.tables[1 : len(k.tables)-1] {
		if tbl.Stats().Length > 0 {
			deletedTable = tbl
			break
		}
	}
	require.NotNil(t, deletedTable, "could not find a middle table to delete")

	deletedCf := deletedTable.Coefficient()
	deletedCount := deletedTable.Stats().Length

	// Remove the table from tablesByCoefficient to create a gap (like compaction does).
	delete(k.tablesByCoefficient, deletedCf)

	// Remove from tables slice as well.
	for i, tbl := range k.tables {
		if tbl == deletedTable {
			k.tables = append(k.tables[:i], k.tables[i+1:]...)
			break
		}
	}

	expectedCount := totalBefore - deletedCount

	// Scan all remaining entries.
	var (
		scannedCount int
		cursor       uint64
		err          error
	)
	for {
		cursor, err = k.Scan(cursor, 10, func(e storage.Entry) bool {
			scannedCount++
			return true
		})
		require.NoError(t, err)
		if cursor == 0 {
			break
		}
	}

	require.Equal(t, expectedCount, scannedCount,
		"scan should find all entries in remaining tables after coefficient gap")
}

func TestKVStore_Put_ErrEntryTooLarge(t *testing.T) {
	c := DefaultConfig()
	c.Add("tableSize", 1024)
	s := testKVStore(t, c)
	value := make([]byte, 2048)
	e := entry.New()
	e.SetKey("key")
	e.SetValue(value)
	e.SetTTL(10)
	e.SetTimestamp(time.Now().UnixNano())
	hkey := xxhash.Sum64([]byte(e.Key()))

	err := s.Put(hkey, e)
	require.ErrorIs(t, err, storage.ErrEntryTooLarge)
}

func TestPrepareTableSize_NegativeValues(t *testing.T) {
	negativeTests := []struct {
		name string
		raw  interface{}
	}{
		{"int", int(-1)},
		{"int8", int8(-1)},
		{"int16", int16(-1)},
		{"int32", int32(-1)},
		{"int64", int64(-1)},
	}

	for _, tt := range negativeTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := prepareTableSize(tt.raw)
			require.Error(t, err)
			require.Contains(t, err.Error(), "tableSize cannot be negative")
		})
	}
}

func TestPrepareTableSize_ValidValues(t *testing.T) {
	tests := []struct {
		name     string
		raw      interface{}
		expected uint64
	}{
		{"uint64", uint64(1024), 1024},
		{"uint32", uint32(1024), 1024},
		{"uint", uint(1024), 1024},
		{"int", int(1024), 1024},
		{"int64", int64(1024), 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, err := prepareTableSize(tt.raw)
			require.NoError(t, err)
			require.Equal(t, tt.expected, size)
		})
	}
}

func TestPrepareTableSize_InvalidType(t *testing.T) {
	_, err := prepareTableSize("invalid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid type for tableSize")
}

func TestKVStore_New_NegativeTableSize(t *testing.T) {
	c := storage.NewConfig(nil)
	c.Add("tableSize", int(-1))
	c.Add("maxIdleTableTimeout", defaultMaxIdleTableTimeout)
	_, err := New(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tableSize cannot be negative")
}

func TestKVStore_Start_NilConfig(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)

	kv.SetConfig(nil)
	err = kv.Start()
	require.Error(t, err)
	require.Equal(t, "config cannot be nil", err.Error())
}

func TestKVStore_PutRaw_ErrEntryTooLarge(t *testing.T) {
	c := DefaultConfig()
	c.Add("tableSize", 1024)
	s := testKVStore(t, c)

	value := make([]byte, 2048)
	hkey := xxhash.Sum64([]byte("key"))
	err := s.PutRaw(hkey, value)
	require.ErrorIs(t, err, storage.ErrEntryTooLarge)
}

func TestKVStore_GetRaw_KeyNotFound(t *testing.T) {
	s := testKVStore(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	raw, err := s.GetRaw(hkey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Nil(t, raw)
}

func TestKVStore_GetTTL_KeyNotFound(t *testing.T) {
	s := testKVStore(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	ttl, err := s.GetTTL(hkey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Equal(t, int64(0), ttl)
}

func TestKVStore_GetLastAccess_KeyNotFound(t *testing.T) {
	s := testKVStore(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	lastAccess, err := s.GetLastAccess(hkey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Equal(t, int64(0), lastAccess)
}

func TestKVStore_GetKey_KeyNotFound(t *testing.T) {
	s := testKVStore(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	key, err := s.GetKey(hkey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Equal(t, "", key)
}

func TestKVStore_Delete_NonExistentKey(t *testing.T) {
	s := testKVStore(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	err := s.Delete(hkey)
	require.NoError(t, err)
}

func TestKVStore_UpdateTTL_KeyNotFound(t *testing.T) {
	s := testKVStore(t, nil)

	e := entry.New()
	e.SetTTL(100)
	e.SetTimestamp(time.Now().UnixNano())

	hkey := xxhash.Sum64([]byte("nonexistent"))
	err := s.UpdateTTL(hkey, e)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestKVStore_Check_KeyNotFound(t *testing.T) {
	s := testKVStore(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	require.False(t, s.Check(hkey))
}

func TestKVStore_RangeHKey(t *testing.T) {
	s := testKVStore(t, nil)

	expected := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
		expected[hkey] = struct{}{}
	}

	collected := make(map[uint64]struct{})
	s.RangeHKey(func(hkey uint64) bool {
		collected[hkey] = struct{}{}
		return true
	})

	require.Equal(t, expected, collected)
}

func TestKVStore_SetConfig(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)

	newConfig := storage.NewConfig(nil)
	newConfig.Add("tableSize", uint64(2048))
	newConfig.Add("maxIdleTableTimeout", defaultMaxIdleTableTimeout)

	kv.SetConfig(newConfig)

	raw, err := kv.config.Get("tableSize")
	require.NoError(t, err)
	require.Equal(t, uint64(2048), raw)
}

func TestKVStore_Fork_CustomConfig(t *testing.T) {
	s := testKVStore(t, nil)

	customConfig := DefaultConfig()
	customConfig.Add("tableSize", uint64(2048))

	child, err := s.Fork(customConfig)
	require.NoError(t, err)

	childKV := child.(*KVStore)
	require.Equal(t, uint64(2048), childKV.tableSize)
	require.Equal(t, 1, len(childKV.tables))
	require.Equal(t, 0, child.Stats().Length)
}

func TestKVStore_MakeTable_RecycledTableReuse(t *testing.T) {
	s := testKVStore(t, nil)

	timestamp := time.Now().UnixNano()
	// Insert entries with large values to fill multiple tables (default 1MB each).
	for i := 0; i < 1500; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	// Delete enough entries to exceed the 40% garbage ratio on a table.
	for i := 0; i < 750; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)
	}

	// Run compaction until done to produce RecycledState tables.
	for {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			break
		}
	}

	k := s.(*KVStore)

	// Verify at least one recycled table exists.
	var recycledFound bool
	for _, tb := range k.tables {
		if tb.State() == table.RecycledState {
			recycledFound = true
			break
		}
	}
	require.True(t, recycledFound, "Expected at least one RecycledState table after compaction")

	tableCountBefore := len(k.tables)

	// Fill the current writable table to trigger makeTable via putWithRetry.
	startIdx := 2000
	for i := startIdx; i < startIdx+1500; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	// The recycled table should have been reused: no RecycledState tables remain.
	for _, tb := range k.tables {
		require.NotEqual(t, table.RecycledState, tb.State(),
			"Expected no RecycledState tables after reuse")
	}

	// The last table must be writable.
	lastTable := k.tables[len(k.tables)-1]
	require.Equal(t, table.ReadWriteState, lastTable.State())

	// Table count should not have grown beyond what's needed (recycled table was reused).
	require.LessOrEqual(t, len(k.tables), tableCountBefore+1,
		"Expected recycled table reuse to limit table growth")
}
