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

func TestTable_ExportSnapshotMeta(t *testing.T) {
	tb := New(1024)
	tb.SetCoefficient(42)

	e := entry.New()
	e.SetKey("testkey")
	e.SetValue([]byte("testvalue"))
	e.SetTTL(int64(5 * time.Second))
	hk := xxhash.Sum64([]byte(e.Key()))
	err := tb.Put(hk, e)
	require.NoError(t, err)

	meta, err := ExportSnapshotMeta(tb)
	require.NoError(t, err)

	require.Equal(t, uint64(42), meta.Coefficient)
	require.Equal(t, tb.offset, meta.Offset)
	require.Equal(t, tb.allocated, meta.Allocated)
	require.Equal(t, tb.inuse, meta.Inuse)
	require.Equal(t, tb.garbage, meta.Garbage)
	require.Equal(t, tb.state, meta.State)
	require.Len(t, meta.HKeys, 1)
	require.Contains(t, meta.HKeys, hk)
	require.NotEmpty(t, meta.OffsetIndex)
}

func TestTable_RestoreFromSnapshot(t *testing.T) {
	tb := New(1024)
	tb.SetCoefficient(7)

	timestamp := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(fmt.Sprintf("key-%d", i))
		e.SetValue([]byte(fmt.Sprintf("value-%d", i)))
		e.SetTTL(int64(i))
		e.SetTimestamp(timestamp)
		hk := xxhash.Sum64([]byte(e.Key()))
		err := tb.Put(hk, e)
		require.NoError(t, err)
	}

	meta, err := ExportSnapshotMeta(tb)
	require.NoError(t, err)
	data := tb.SnapshotData()

	// Make a copy of data to simulate reading from disk.
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	restored, err := RestoreFromSnapshot(meta, dataCopy)
	require.NoError(t, err)

	require.Equal(t, uint64(7), restored.Coefficient())
	require.Equal(t, tb.Stats().Length, restored.Stats().Length)
	require.Equal(t, tb.Stats().Inuse, restored.Stats().Inuse)
	require.Equal(t, tb.Stats().Allocated, restored.Stats().Allocated)
	require.Equal(t, tb.State(), restored.State())

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		hk := xxhash.Sum64([]byte(key))
		got, err := restored.Get(hk)
		require.NoError(t, err)
		require.Equal(t, key, got.Key())
		require.Equal(t, []byte(fmt.Sprintf("value-%d", i)), got.Value())
		require.Equal(t, int64(i), got.TTL())
		require.Equal(t, timestamp, got.Timestamp())
	}
}

func TestTable_SnapshotData(t *testing.T) {
	tb := New(1024)

	// Empty table should return empty slice.
	require.Len(t, tb.SnapshotData(), 0)

	e := entry.New()
	e.SetKey("testkey")
	e.SetValue([]byte("testvalue"))
	hk := xxhash.Sum64([]byte(e.Key()))
	err := tb.Put(hk, e)
	require.NoError(t, err)

	data := tb.SnapshotData()
	require.Equal(t, int(tb.offset), len(data))
	require.True(t, len(data) > 0)
}
