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

package table

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

var key = "foobar"

const hkey uint64 = 18071988

func setupTable() (*Table, storage.Entry) {
	tb := New(1024)
	e := entry.New()
	e.SetKey(key)
	e.SetValue([]byte("foobar-value"))
	return tb, e
}

func TestTable_Put(t *testing.T) {
	tb, e := setupTable()
	err := tb.Put(hkey, e)
	require.NoError(t, err)
}

func TestTable_Get(t *testing.T) {
	tb, e := setupTable()
	err := tb.Put(hkey, e)
	require.NoError(t, err)

	value, err := tb.Get(hkey)
	require.NoError(t, err)

	require.Equal(t, e.Key(), value.Key())
	require.Equal(t, e.Value(), value.Value())
	require.Equal(t, e.TTL(), value.TTL())
	require.Equal(t, int64(0), e.LastAccess())
	require.NotEqual(t, int64(0), value.LastAccess())
}

func TestTable_Delete(t *testing.T) {
	tb, e := setupTable()

	err := tb.Put(hkey, e)
	require.NoError(t, err)

	err = tb.Delete(hkey)
	require.NoError(t, err)

	_, err = tb.Get(hkey)
	require.ErrorIs(t, ErrHKeyNotFound, err)
}

func TestTable_Check(t *testing.T) {
	tb, e := setupTable()
	err := tb.Put(hkey, e)
	require.NoError(t, err)

	require.True(t, tb.Check(hkey))

	err = tb.Delete(hkey)
	require.NoError(t, err)

	require.False(t, tb.Check(hkey))
}

func TestTable_PutRaw(t *testing.T) {
	tb, e := setupTable()

	err := tb.PutRaw(hkey, e.Encode())
	require.NoError(t, err)

	value, err := tb.Get(hkey)
	require.NoError(t, err)
	require.Equal(t, e, value)
}

func TestTable_GetRaw(t *testing.T) {
	tb, e := setupTable()

	err := tb.Put(hkey, e)
	require.NoError(t, err)

	raw, err := tb.GetRaw(hkey)
	require.NoError(t, err)
	extracted := entry.New()
	extracted.Decode(raw)

	require.Equal(t, e.Key(), extracted.Key())
	require.Equal(t, e.Value(), extracted.Value())
	require.Equal(t, e.TTL(), extracted.TTL())
	require.Equal(t, int64(0), e.LastAccess())
	require.NotEqual(t, int64(0), extracted.LastAccess())
}

func TestTable_GetRawKey(t *testing.T) {
	tb, e := setupTable()

	err := tb.Put(hkey, e)
	require.NoError(t, err)

	rawKey, err := tb.GetRawKey(hkey)
	require.NoError(t, err)
	require.Equal(t, key, string(rawKey))
}

func TestTable_GetKey(t *testing.T) {
	tb, e := setupTable()

	err := tb.Put(hkey, e)
	require.NoError(t, err)

	k, err := tb.GetKey(hkey)
	require.NoError(t, err)
	require.Equal(t, key, k)
}

func TestTable_SetState(t *testing.T) {
	tb, _ := setupTable()
	tb.SetState(ReadOnlyState)
	require.Equal(t, ReadOnlyState, tb.State())
}

func TestTable_GetTTL(t *testing.T) {
	tb, e := setupTable()
	ttl := time.Now().UnixNano()
	e.SetTTL(ttl)

	err := tb.Put(hkey, e)
	require.NoError(t, err)

	value, err := tb.GetTTL(hkey)
	require.NoError(t, err)
	require.Equal(t, ttl, value)
}

func TestTable_GetLastAccess(t *testing.T) {
	tb, e := setupTable()

	err := tb.Put(hkey, e)
	require.NoError(t, err)

	value, err := tb.GetLastAccess(hkey)
	require.NoError(t, err)
	require.NotEqual(t, 0, value)
}

func TestTable_UpdateTTL(t *testing.T) {
	tb, e := setupTable()
	ttl := time.Now().UnixNano()
	e.SetTTL(ttl)

	err := tb.Put(hkey, e)
	require.NoError(t, err)

	e.SetTTL(ttl + 1000)
	err = tb.UpdateTTL(hkey, e)
	require.NoError(t, err)

	value, err := tb.GetTTL(hkey)
	require.NoError(t, err)
	require.Equal(t, ttl+1000, value)
}

func TestTable_UpdateTTL_Update_LastAccess(t *testing.T) {
	tb, e := setupTable()

	err := tb.Put(hkey, e)
	require.NoError(t, err)

	lastAccessOne, err := tb.GetLastAccess(hkey)
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	ttl := time.Now().UnixNano() + 1000
	e.SetTTL(ttl)

	err = tb.UpdateTTL(hkey, e)
	require.NoError(t, err)

	lastAccessTwo, err := tb.GetLastAccess(hkey)
	require.NoError(t, err)

	require.Greater(t, lastAccessTwo, lastAccessOne)
}

func TestTable_State(t *testing.T) {
	tb, _ := setupTable()
	require.Equal(t, ReadWriteState, tb.State())
}

func TestTable_Range(t *testing.T) {
	data := make(map[uint64]storage.Entry)

	tb := New(1 << 20)
	for i := 0; i < 100; i++ {
		e := entry.New()
		ikey := fmt.Sprintf("key-%d", i)
		idata := []byte(fmt.Sprintf("value-%d", i))
		ihkey := xxhash.Sum64String(ikey)
		e.SetKey(ikey)
		e.SetValue(idata)
		data[ihkey] = e

		err := tb.Put(ihkey, e)
		require.NoError(t, err)
	}

	tb.Range(func(hk uint64, e storage.Entry) bool {
		item, ok := data[hk]
		require.True(t, ok)

		require.Equal(t, item.Key(), e.Key())
		require.Equal(t, item.Value(), e.Value())
		require.Equal(t, item.TTL(), e.TTL())
		require.Equal(t, int64(0), item.LastAccess())
		require.NotEqual(t, int64(0), e.LastAccess())

		return true
	})
}

func TestTable_Stats(t *testing.T) {
	tb := New(1 << 20)
	for i := 0; i < 100; i++ {
		e := entry.New()
		ikey := fmt.Sprintf("key-%d", i)
		idata := []byte(fmt.Sprintf("value-%d", i))
		ihkey := xxhash.Sum64String(ikey)
		e.SetKey(ikey)
		e.SetValue(idata)
		err := tb.Put(ihkey, e)
		require.NoError(t, err)
	}

	s := tb.Stats()
	require.Equal(t, uint64(1<<20), s.Allocated)
	require.Equal(t, 100, s.Length)
	require.Equal(t, uint64(4280), s.Inuse)
	require.Equal(t, uint64(0), s.Garbage)

	for i := 0; i < 100; i++ {
		ikey := fmt.Sprintf("key-%d", i)
		ihkey := xxhash.Sum64String(ikey)
		err := tb.Delete(ihkey)
		require.NoError(t, err)
	}

	s = tb.Stats()
	require.Equal(t, uint64(1<<20), s.Allocated)
	require.Equal(t, 0, s.Length)
	require.Equal(t, uint64(0), s.Inuse)
	require.Equal(t, uint64(4280), s.Garbage)
}

func TestTable_Reset(t *testing.T) {
	tb := New(1 << 20)
	for i := 0; i < 100; i++ {
		e := entry.New()
		ikey := fmt.Sprintf("key-%d", i)
		idata := []byte(fmt.Sprintf("value-%d", i))
		ihkey := xxhash.Sum64String(ikey)
		e.SetKey(ikey)
		e.SetValue(idata)
		err := tb.Put(ihkey, e)
		require.NoError(t, err)
	}

	tb.Reset()

	stats := tb.Stats()
	require.Equal(t, RecycledState, tb.State())
	require.Equal(t, uint64(0), stats.Garbage)
	require.Equal(t, uint64(0), stats.Inuse)
	require.Equal(t, tb.allocated, stats.Allocated)
	require.Equal(t, 0, stats.Length)
}

func TestTable_Scan(t *testing.T) {

	tb := New(1 << 20)
	for i := 0; i < 100; i++ {
		e := entry.New()
		key := fmt.Sprintf("key-%d", i)
		data := []byte(fmt.Sprintf("value-%d", i))
		hkey := xxhash.Sum64String(key)
		e.SetKey(key)
		e.SetValue(data)

		err := tb.Put(hkey, e)
		require.NoError(t, err)
	}

	var err error
	var cursor uint64
	for {
		cursor, err = tb.Scan(cursor, 10, func(e storage.Entry) bool {
			return true
		})
		require.NoError(t, err)
		if cursor == 0 {
			break
		}
	}
}

func TestTable_ScanRegexMatch(t *testing.T) {
	tb := New(1 << 20)
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + strconv.Itoa(i)
		} else {
			key = "odd:" + strconv.Itoa(i)
		}
		e := entry.New()
		data := []byte(fmt.Sprintf("value-%d", i))
		hkey := xxhash.Sum64String(key)
		e.SetKey(key)
		e.SetValue(data)

		err := tb.Put(hkey, e)
		require.NoError(t, err)
	}

	var err error
	var num int
	var count int
	var cursor uint64
	for {
		num++
		cursor, err = tb.ScanRegexMatch(cursor, "even:", 10, func(e storage.Entry) bool {
			count++
			return true
		})
		require.NoError(t, err)
		if cursor == 0 {
			break
		}
	}

	require.Equal(t, 6, num)
	require.Equal(t, 50, count)
}

func TestTable_ScanRegexMatch_SingleMatch(t *testing.T) {
	tb := New(1 << 20)
	for i := 0; i < 100; i++ {
		e := entry.New()
		key := fmt.Sprintf("key-%d", i)
		data := []byte(fmt.Sprintf("value-%d", i))
		hkey := xxhash.Sum64String(key)
		e.SetKey(key)
		e.SetValue(data)

		err := tb.Put(hkey, e)
		require.NoError(t, err)
	}

	e := entry.New()
	e.SetKey("even:200")
	e.SetTTL(123123)
	e.SetValue([]byte("my-value"))
	e.SetTimestamp(time.Now().UnixNano())
	hkey := xxhash.Sum64([]byte(e.Key()))
	err := tb.Put(hkey, e)
	require.NoError(t, err)

	var num int
	var count int
	var cursor uint64
	for {
		num++
		cursor, err = tb.ScanRegexMatch(cursor, "even:", 10, func(e storage.Entry) bool {
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
