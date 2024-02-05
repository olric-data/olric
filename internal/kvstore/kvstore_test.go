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
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/buraksezer/olric/internal/kvstore/table"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/cespare/xxhash/v2"
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
