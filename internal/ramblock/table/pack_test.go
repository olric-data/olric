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

package table

import (
	"fmt"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/olric-data/olric/internal/ramblock/entry"
	"github.com/stretchr/testify/require"
)

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func TestTable_Pack_Decode_CorruptData(t *testing.T) {
	_, err := Decode([]byte("this is not valid msgpack data"))
	require.Error(t, err)
}

func TestTable_Pack_EncodeDecode_GarbageAndRecycledAt(t *testing.T) {
	size := uint64(1 << 16)
	tb := New(size)

	for i := 0; i < 20; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := tb.Put(hkey, e)
		require.NoError(t, err)
	}

	// Delete some entries to create garbage
	for i := 0; i < 10; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := tb.Delete(hkey)
		require.NoError(t, err)
	}

	statsBefore := tb.Stats()
	require.Greater(t, statsBefore.Garbage, uint64(0))

	// Reset to set recycledAt
	tb.Reset()
	statsAfterReset := tb.Stats()
	require.NotEqual(t, int64(0), statsAfterReset.RecycledAt)

	// Re-add some entries after reset to have garbage again
	tb.SetState(ReadWriteState)
	for i := 100; i < 110; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := tb.Put(hkey, e)
		require.NoError(t, err)
	}
	// Delete a few to accumulate garbage
	for i := 100; i < 105; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := tb.Delete(hkey)
		require.NoError(t, err)
	}

	statsBeforeEncode := tb.Stats()
	require.Greater(t, statsBeforeEncode.Garbage, uint64(0))
	require.NotEqual(t, int64(0), statsBeforeEncode.RecycledAt)

	encoded, err := Encode(tb)
	require.NoError(t, err)

	decoded, err := Decode(encoded)
	require.NoError(t, err)

	statsAfterDecode := decoded.Stats()
	require.Equal(t, statsBeforeEncode.Garbage, statsAfterDecode.Garbage)
	require.Equal(t, statsBeforeEncode.RecycledAt, statsAfterDecode.RecycledAt)
	require.Equal(t, statsBeforeEncode.Inuse, statsAfterDecode.Inuse)
	require.Equal(t, statsBeforeEncode.Length, statsAfterDecode.Length)
}

func TestTable_Pack_EncodeDecode(t *testing.T) {
	size := uint64(1 << 16)
	tb := New(size)

	timestamp := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetLastAccess(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := tb.Put(hkey, e)
		require.NoError(t, err)
	}

	encoded, err := Encode(tb)
	require.NoError(t, err)

	newTable, err := Decode(encoded)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := newTable.Get(hkey)
		require.NoError(t, err)
		require.Equal(t, e.Key(), bkey(i))
		require.Equal(t, e.Value(), bval(i))
		require.Equal(t, e.TTL(), int64(i))
		require.NotEqual(t, timestamp, e.LastAccess())
	}

}
