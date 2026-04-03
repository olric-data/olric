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
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/olric-data/olric/internal/ramblock/entry"
	"github.com/olric-data/olric/internal/ramblock/table"
	"github.com/olric-data/olric/pkg/storage"
	"github.com/stretchr/testify/require"
)

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func testRamBlock(t *testing.T, c *storage.Config) storage.Engine {
	kv, err := New(c)
	require.NoError(t, err)

	child, err := kv.Fork(nil)
	require.NoError(t, err)

	err = child.Start()
	require.NoError(t, err)

	return child
}

func TestRamBlock_Put(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_Get(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_Delete(t *testing.T) {
	s := testRamBlock(t, nil)

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
	for i, tb := range s.(*RamBlock).tables {
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

	for i, tb := range s.(*RamBlock).tables {
		s := tb.Stats()
		require.Equal(t, uint64(0), s.Inuse)
		require.Equal(t, 0, s.Length)
		require.Equal(t, garbage[i], s.Garbage)
	}
}

func TestRamBlock_ExportImport(t *testing.T) {
	timestamp := time.Now().UnixNano()
	s := testRamBlock(t, nil)

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

	fresh := testRamBlock(t, nil)

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

func TestRamBlock_Import_CallbackError(t *testing.T) {
	s := testRamBlock(t, nil)

	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	ti := s.TransferIterator()
	require.True(t, ti.Next())

	data, _, err := ti.Export()
	require.NoError(t, err)

	expectedErr := errors.New("callback error")
	fresh := testRamBlock(t, nil)
	err = fresh.Import(data, func(hkey uint64, e storage.Entry) error {
		return expectedErr
	})
	require.ErrorIs(t, err, expectedErr)
}

func TestRamBlock_Stats_Length(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_Range(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_Check(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_UpdateTTL(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_GetKey(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_PutRawGetRaw(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_GetTTL(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_GetLastAccess(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_Fork(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_StateChange(t *testing.T) {
	s := testRamBlock(t, nil)

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

	for i, tb := range s.(*RamBlock).tables {
		if tb.State() == table.ReadWriteState {
			require.Equalf(t, len(s.(*RamBlock).tables)-1, i, "Writable table has to be the latest table")
		} else if tb.State() == table.ReadOnlyState {
			require.True(t, i < len(s.(*RamBlock).tables)-1)
		}
	}
}

func TestRamBlock_NewEntry(t *testing.T) {
	s := testRamBlock(t, nil)

	i := s.NewEntry()
	_, ok := i.(*entry.Entry)
	require.True(t, ok)
}

func TestRamBlock_Name(t *testing.T) {
	s := testRamBlock(t, nil)
	require.Equal(t, "ramblock", s.Name())
}

func TestRamBlock_CloseDestroy(t *testing.T) {
	s := testRamBlock(t, nil)
	require.NoError(t, s.Close())
	require.NoError(t, s.Destroy())
}

func TestStorage_Scan(t *testing.T) {
	s := testRamBlock(t, nil)

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
	k := s.(*RamBlock)
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
	s := testRamBlock(t, nil)

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
	k := s.(*RamBlock)
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
	s := testRamBlock(t, nil)

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
	k := s.(*RamBlock)
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
	s := testRamBlock(t, c)
	k := s.(*RamBlock)

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

func TestRamBlock_Put_ErrEntryTooLarge(t *testing.T) {
	c := DefaultConfig()
	c.Add("tableSize", 1024)
	s := testRamBlock(t, c)
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

func TestRamBlock_New_NegativeTableSize(t *testing.T) {
	c := storage.NewConfig(nil)
	c.Add("tableSize", int(-1))
	c.Add("maxIdleTableTimeout", defaultMaxIdleTableTimeout)
	_, err := New(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tableSize cannot be negative")
}

func TestRamBlock_Start_NilConfig(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)

	kv.SetConfig(nil)
	err = kv.Start()
	require.Error(t, err)
	require.Equal(t, "config cannot be nil", err.Error())
}

func TestRamBlock_PutRaw_ErrEntryTooLarge(t *testing.T) {
	c := DefaultConfig()
	c.Add("tableSize", 1024)
	s := testRamBlock(t, c)

	value := make([]byte, 2048)
	hkey := xxhash.Sum64([]byte("key"))
	err := s.PutRaw(hkey, value)
	require.ErrorIs(t, err, storage.ErrEntryTooLarge)
}

func TestRamBlock_GetRaw_KeyNotFound(t *testing.T) {
	s := testRamBlock(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	raw, err := s.GetRaw(hkey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Nil(t, raw)
}

func TestRamBlock_GetTTL_KeyNotFound(t *testing.T) {
	s := testRamBlock(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	ttl, err := s.GetTTL(hkey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Equal(t, int64(0), ttl)
}

func TestRamBlock_GetLastAccess_KeyNotFound(t *testing.T) {
	s := testRamBlock(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	lastAccess, err := s.GetLastAccess(hkey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Equal(t, int64(0), lastAccess)
}

func TestRamBlock_GetKey_KeyNotFound(t *testing.T) {
	s := testRamBlock(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	key, err := s.GetKey(hkey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Equal(t, "", key)
}

func TestRamBlock_Delete_NonExistentKey(t *testing.T) {
	s := testRamBlock(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	err := s.Delete(hkey)
	require.NoError(t, err)
}

func TestRamBlock_UpdateTTL_KeyNotFound(t *testing.T) {
	s := testRamBlock(t, nil)

	e := entry.New()
	e.SetTTL(100)
	e.SetTimestamp(time.Now().UnixNano())

	hkey := xxhash.Sum64([]byte("nonexistent"))
	err := s.UpdateTTL(hkey, e)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestRamBlock_Check_KeyNotFound(t *testing.T) {
	s := testRamBlock(t, nil)

	hkey := xxhash.Sum64([]byte("nonexistent"))
	require.False(t, s.Check(hkey))
}

func TestRamBlock_RangeHKey(t *testing.T) {
	s := testRamBlock(t, nil)

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

func TestRamBlock_SetConfig(t *testing.T) {
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

func TestRamBlock_Fork_CustomConfig(t *testing.T) {
	s := testRamBlock(t, nil)

	customConfig := DefaultConfig()
	customConfig.Add("tableSize", uint64(2048))

	child, err := s.Fork(customConfig)
	require.NoError(t, err)

	childKV := child.(*RamBlock)
	require.Equal(t, uint64(2048), childKV.tableSize)
	require.Equal(t, 1, len(childKV.tables))
	require.Equal(t, 0, child.Stats().Length)
}

func TestRamBlock_MakeTable_RecycledTableReuse(t *testing.T) {
	s := testRamBlock(t, nil)

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

	k := s.(*RamBlock)

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

func TestRamBlock_EvictTable_PutRawError(t *testing.T) {
	s := testRamBlock(t, nil)

	timestamp := time.Now().UnixNano()
	// Insert entries with large values to fill multiple tables.
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

	k := s.(*RamBlock)

	// Shrink tableSize to force PutRaw to return ErrEntryTooLarge during eviction.
	k.tableSize = 1

	done, err := k.Compaction()
	require.False(t, done)
	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrEntryTooLarge)
}

func TestRamBlock_Compaction_NoTables(t *testing.T) {
	k, err := New(nil)
	require.NoError(t, err)

	err = k.Start()
	require.NoError(t, err)

	// No tables exist — Compaction should return done=true with no error.
	done, compactionErr := k.Compaction()
	require.NoError(t, compactionErr)
	require.True(t, done)
}

func TestRamBlock_IsCompactionOK_ExactThreshold(t *testing.T) {
	// Each entry: key=1 byte + value=70 bytes + metadata=29 bytes = 100 bytes.
	// Table size 1000 → 9 entries fit. Deleting N entries → garbage = N*100.
	// maxGarbageRatio = 0.40, threshold = 1000 * 0.40 = 400.

	tests := []struct {
		name        string
		deleteCount int
		expected    bool
	}{
		{"ExactBoundary", 4, true},  // garbage=400, ratio=0.40 → >=0.40 → true
		{"BelowBoundary", 3, false}, // garbage=300, ratio=0.30 → <0.40 → false
		{"AboveBoundary", 5, true},  // garbage=500, ratio=0.50 → >=0.40 → true
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultConfig()
			config.Add("tableSize", uint64(1000))
			s := testRamBlock(t, config)

			timestamp := time.Now().UnixNano()
			// Insert 9 entries of exactly 100 bytes each.
			for i := 0; i < 9; i++ {
				e := entry.New()
				e.SetKey(fmt.Sprintf("%01d", i))            // 1-byte key
				e.SetValue([]byte(fmt.Sprintf("%070d", i))) // 70-byte value
				e.SetTimestamp(timestamp)
				hkey := xxhash.Sum64([]byte(e.Key()))
				err := s.Put(hkey, e)
				require.NoError(t, err)
			}

			// Delete entries to reach desired garbage level.
			for i := 0; i < tc.deleteCount; i++ {
				hkey := xxhash.Sum64([]byte(fmt.Sprintf("%01d", i)))
				err := s.Delete(hkey)
				require.NoError(t, err)
			}

			k := s.(*RamBlock)
			tb := k.tables[0]

			stats := tb.Stats()
			require.Equal(t, uint64(1000), stats.Allocated)
			require.Equal(t, uint64(tc.deleteCount*100), stats.Garbage,
				"Expected garbage = %d", tc.deleteCount*100)

			result := k.isCompactionOK(tb)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestTransferIterator_Drop_EmptyTables(t *testing.T) {
	s := testRamBlock(t, nil)

	// Put a single entry so we have one table with data.
	e := entry.New()
	e.SetKey(bkey(0))
	e.SetValue(bval(0))
	e.SetTimestamp(time.Now().UnixNano())
	hkey := xxhash.Sum64([]byte(e.Key()))
	err := s.Put(hkey, e)
	require.NoError(t, err)

	// Drain all tables via Export + Drop.
	ti := s.TransferIterator()
	for ti.Next() {
		_, index, err := ti.Export()
		require.NoError(t, err)

		err = ti.Drop(index)
		require.NoError(t, err)
	}

	// Now tables slice is empty. Drop should return an error.
	err = ti.Drop(0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "there is no table to drop")
}

func TestTransferIterator_Export_SkipsRecycledState(t *testing.T) {
	s := testRamBlock(t, nil)
	k := s.(*RamBlock)

	// Insert enough data to create at least 2 tables.
	timestamp := time.Now().UnixNano()
	for i := 0; i < 100000; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}
	require.Greater(t, len(k.tables), 1, "need at least 2 tables for this test")

	// Set the first table to RecycledState.
	k.tables[0].SetState(table.RecycledState)

	ti := s.TransferIterator()

	// Export should skip the recycled table and return the next non-recycled one.
	data, index, err := ti.Export()
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Greater(t, index, 0, "Expected Export to skip index 0 (recycled table)")

	// Now set ALL tables to RecycledState.
	for _, tb := range k.tables {
		tb.SetState(table.RecycledState)
	}

	// Export should return io.EOF when all tables are recycled.
	_, _, err = ti.Export()
	require.ErrorIs(t, err, io.EOF)
}
