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
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/olric-data/olric/internal/objectid"
	"github.com/olric-data/olric/internal/ramblock/entry"
	"github.com/olric-data/olric/internal/ramblock/table"
	"github.com/olric-data/olric/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestRamBlock_Snapshot_Empty(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	err := s.Snapshot(dir)
	require.NoError(t, err)

	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.NoError(t, err)

	origStats := s.Stats()
	restoredStats := restored.Stats()
	require.Equal(t, origStats.Length, restoredStats.Length)
	require.Equal(t, 0, restoredStats.Length)

	_, err = restored.Get(xxhash.Sum64([]byte("nonexistent")))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestRamBlock_Snapshot_SingleTable(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	timestamp := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTTL(int64(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	err := s.Snapshot(dir)
	require.NoError(t, err)

	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := restored.Get(hkey)
		require.NoError(t, err)
		require.Equal(t, bkey(i), e.Key())
		require.Equal(t, bval(i), e.Value())
		require.Equal(t, int64(i), e.TTL())
		require.Equal(t, timestamp, e.Timestamp())
	}

	require.Equal(t, s.Stats().Length, restored.Stats().Length)
	require.Equal(t, s.Stats().Inuse, restored.Stats().Inuse)
}

func largeVal(i int) []byte {
	// ~1KB value to fill 1MB tables faster.
	return []byte(fmt.Sprintf("%01000d", i))
}

func TestRamBlock_Snapshot_MultipleTables(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	// Insert entries with ~1KB values to span multiple 1MB tables.
	const numEntries = 1500
	for i := 0; i < numEntries; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(largeVal(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	rb := s.(*RamBlock)
	require.Greater(t, len(rb.tables), 1, "should have multiple tables")
	origStats := s.Stats()

	err := s.Snapshot(dir)
	require.NoError(t, err)

	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.NoError(t, err)

	restoredStats := restored.Stats()
	require.Equal(t, origStats.Length, restoredStats.Length)
	require.Equal(t, origStats.NumTables, restoredStats.NumTables)

	for i := 0; i < numEntries; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := restored.Get(hkey)
		require.NoError(t, err)
		require.Equal(t, bkey(i), e.Key())
		require.Equal(t, largeVal(i), e.Value())
	}
}

func TestRamBlock_Snapshot_WithGarbage(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	// Delete half the entries to create garbage.
	for i := 0; i < 50; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)
	}

	origStats := s.Stats()
	require.Greater(t, origStats.Garbage, 0)

	err := s.Snapshot(dir)
	require.NoError(t, err)

	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.NoError(t, err)

	restoredStats := restored.Stats()
	require.Equal(t, origStats.Length, restoredStats.Length)
	require.Equal(t, origStats.Garbage, restoredStats.Garbage)

	// Deleted entries should stay deleted.
	for i := 0; i < 50; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		_, err := restored.Get(hkey)
		require.ErrorIs(t, err, storage.ErrKeyNotFound)
	}

	// Surviving entries should be present.
	for i := 50; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := restored.Get(hkey)
		require.NoError(t, err)
		require.Equal(t, bkey(i), e.Key())
	}
}

func TestRamBlock_Snapshot_SkipsRecycledTables(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	// Fill enough to create multiple tables with large values.
	const numEntries = 1500
	for i := 0; i < numEntries; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(largeVal(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	// Delete enough entries to trigger compaction and create recycled tables.
	for i := 0; i < 750; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)
	}

	// Run compaction until done.
	for {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			break
		}
	}

	rb := s.(*RamBlock)
	hasRecycled := false
	nonRecycledCount := 0
	for _, tb := range rb.tables {
		if tb.State() == table.RecycledState {
			hasRecycled = true
		} else {
			nonRecycledCount++
		}
	}
	require.True(t, hasRecycled, "should have at least one recycled table")

	err := s.Snapshot(dir)
	require.NoError(t, err)

	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.NoError(t, err)

	restoredRb := restored.(*RamBlock)
	require.Equal(t, nonRecycledCount, len(restoredRb.tables))

	// Surviving entries should be present.
	for i := 750; i < 1500; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := restored.Get(hkey)
		require.NoError(t, err)
		require.Equal(t, bkey(i), e.Key())
	}
}

func TestRamBlock_Snapshot_CoefficientPreserved(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	// Insert enough to create multiple tables.
	for i := 0; i < 1500; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(largeVal(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	rb := s.(*RamBlock)
	origCoefficient := rb.coefficient

	err := s.Snapshot(dir)
	require.NoError(t, err)

	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.NoError(t, err)

	restoredRb := restored.(*RamBlock)
	require.Equal(t, origCoefficient, restoredRb.coefficient)

	// New writes should work without collisions.
	for i := 1500; i < 1600; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := restored.Put(hkey, e)
		require.NoError(t, err)
	}
}

func TestRamBlock_Snapshot_WriteAfterRestore(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	for i := 0; i < 50; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	err := s.Snapshot(dir)
	require.NoError(t, err)

	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.NoError(t, err)

	// Write new entries.
	for i := 50; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := restored.Put(hkey, e)
		require.NoError(t, err)
	}

	// Both old and new entries should be present.
	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := restored.Get(hkey)
		require.NoError(t, err)
		require.Equal(t, bkey(i), e.Key())
		require.Equal(t, bval(i), e.Value())
	}
}

func TestRamBlock_Snapshot_MultipleSnapshots(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	err := s.Snapshot(dir)
	require.NoError(t, err)

	// Add more entries and take another snapshot.
	for i := 10; i < 20; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	time.Sleep(time.Second) // Ensure different ObjectID timestamp.
	err = s.Snapshot(dir)
	require.NoError(t, err)

	// Should have two manifest files.
	matches, err := filepath.Glob(filepath.Join(dir, "*.manifest"))
	require.NoError(t, err)
	require.Len(t, matches, 2)
}

func TestRamBlock_Snapshot_RestoreLatest(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	// First snapshot with 10 entries.
	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	err := s.Snapshot(dir)
	require.NoError(t, err)

	// Second snapshot with 20 entries.
	for i := 10; i < 20; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	time.Sleep(time.Second) // Ensure different ObjectID timestamp.
	err = s.Snapshot(dir)
	require.NoError(t, err)

	// Restore should pick the latest (20 entries).
	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.NoError(t, err)

	require.Equal(t, 20, restored.Stats().Length)
}

func TestRamBlock_DeleteSnapshot(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	err := s.Snapshot(dir)
	require.NoError(t, err)

	// Find the snapshot ID.
	id, err := latestSnapshotID(dir)
	require.NoError(t, err)

	// Take a second snapshot.
	time.Sleep(time.Second)
	err = s.Snapshot(dir)
	require.NoError(t, err)

	// Delete the first snapshot.
	err = s.DeleteSnapshot(dir, id)
	require.NoError(t, err)

	// Only one manifest should remain.
	matches, err := filepath.Glob(filepath.Join(dir, "*.manifest"))
	require.NoError(t, err)
	require.Len(t, matches, 1)

	// Restore should still work with the remaining snapshot.
	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.NoError(t, err)
	require.Equal(t, 10, restored.Stats().Length)
}

func TestRamBlock_Restore_NoSnapshots(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	err := s.Restore(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no snapshots found")
}

func TestRamBlock_Restore_InvalidVersion(t *testing.T) {
	s := testRamBlock(t, nil)
	dir := t.TempDir()

	// Create a snapshot with an invalid version.
	err := s.Snapshot(dir)
	require.NoError(t, err)

	id, err := latestSnapshotID(dir)
	require.NoError(t, err)

	// Tamper with the manifest: change the version.
	manifestPath := snapshotManifestPath(dir, id)
	manifestData, err := os.ReadFile(manifestPath)
	require.NoError(t, err)

	// Unmarshal, modify, re-marshal.
	var manifest SnapshotManifest
	err = msgpack.Unmarshal(manifestData, &manifest)
	require.NoError(t, err)
	manifest.Version = 99
	tampered, err := msgpack.Marshal(manifest)
	require.NoError(t, err)
	err = os.WriteFile(manifestPath, tampered, 0644)
	require.NoError(t, err)

	restored := testRamBlock(t, nil)
	err = restored.Restore(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported snapshot version")
}

func TestRamBlock_Restore_CorruptManifest(t *testing.T) {
	dir := t.TempDir()

	// Write garbage manifest.
	err := os.WriteFile(filepath.Join(dir, "aabbccdd00112233aabbccdd.manifest"), []byte("not msgpack"), 0644)
	require.NoError(t, err)

	s := testRamBlock(t, nil)
	err = s.Restore(dir)
	require.Error(t, err)
}

func TestObjectID_Ordering(t *testing.T) {
	ids := make([]string, 10)
	for i := range ids {
		ids[i] = objectid.NewObjectID().Hex()
	}

	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)

	// ObjectIDs generated in sequence should already be sorted
	// (same second, counter increments).
	require.Equal(t, ids, sorted)
}

func TestObjectID_HexRoundtrip(t *testing.T) {
	original := objectid.NewObjectID()
	hexStr := original.Hex()
	require.Len(t, hexStr, 24)

	parsed, err := objectid.ObjectIDFromHex(hexStr)
	require.NoError(t, err)
	require.Equal(t, original, parsed)
}
