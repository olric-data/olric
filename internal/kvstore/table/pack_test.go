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
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
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
