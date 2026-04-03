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
	"strings"
	"time"

	"github.com/olric-data/olric/internal/objectid"
	"github.com/olric-data/olric/internal/ramblock/table"
	"github.com/vmihailenco/msgpack/v5"
)

const snapshotVersion = uint16(1)

// SnapshotManifest is the top-level metadata for a snapshot, serialized as msgpack.
type SnapshotManifest struct {
	Version     uint16
	EngineName  string
	TableSize   uint64
	Coefficient uint64
	CreatedAt   int64
	Tables      []table.SnapshotMeta
}

func snapshotManifestPath(dir string, id string) string {
	return filepath.Join(dir, id+".manifest")
}

func snapshotDataPath(dir string, id string, tableIndex int) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%d.data", id, tableIndex))
}

// latestSnapshotID returns the ObjectID hex of the latest snapshot in dir.
func latestSnapshotID(dir string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.manifest"))
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no snapshots found in %s", dir)
	}

	var ids []string
	for _, m := range matches {
		base := filepath.Base(m)
		id := strings.TrimSuffix(base, ".manifest")
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids[len(ids)-1], nil
}

// Snapshot writes a point-in-time snapshot to dir with an auto-generated ObjectID.
func (rb *RamBlock) Snapshot(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	id := objectid.NewObjectID().Hex()

	manifest := SnapshotManifest{
		Version:     snapshotVersion,
		EngineName:  rb.Name(),
		TableSize:   rb.tableSize,
		Coefficient: rb.coefficient,
		CreatedAt:   time.Now().UnixNano(),
	}

	// Write data files first. Manifest is the commit marker.
	tableIndex := 0
	for _, t := range rb.tables {
		if t.State() == table.RecycledState {
			continue
		}

		meta, err := table.ExportSnapshotMeta(t)
		if err != nil {
			return fmt.Errorf("export table metadata: %w", err)
		}
		manifest.Tables = append(manifest.Tables, *meta)

		dataPath := snapshotDataPath(dir, id, tableIndex)
		if err := os.WriteFile(dataPath, t.SnapshotData(), 0644); err != nil {
			return fmt.Errorf("write table data: %w", err)
		}
		tableIndex++
	}

	// Write manifest last — acts as atomic commit marker.
	data, err := msgpack.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := os.WriteFile(snapshotManifestPath(dir, id), data, 0644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	return nil
}

// Restore loads the latest snapshot from dir, replacing the engine's current state.
func (rb *RamBlock) Restore(dir string) error {
	id, err := latestSnapshotID(dir)
	if err != nil {
		return err
	}

	manifestData, err := os.ReadFile(snapshotManifestPath(dir, id))
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	var manifest SnapshotManifest
	if err := msgpack.Unmarshal(manifestData, &manifest); err != nil {
		return fmt.Errorf("unmarshal manifest: %w", err)
	}

	if manifest.Version != snapshotVersion {
		return fmt.Errorf("unsupported snapshot version: %d", manifest.Version)
	}
	if manifest.EngineName != rb.Name() {
		return fmt.Errorf("engine name mismatch: snapshot has %q, expected %q", manifest.EngineName, rb.Name())
	}

	// Reset engine state.
	rb.tables = rb.tables[:0]
	rb.tablesByCoefficient = make(map[uint64]*table.Table)
	rb.tableSize = manifest.TableSize
	rb.coefficient = manifest.Coefficient

	for i, meta := range manifest.Tables {
		dataPath := snapshotDataPath(dir, id, i)
		data, err := os.ReadFile(dataPath)
		if err != nil {
			return fmt.Errorf("read table data %d: %w", i, err)
		}

		t, err := table.RestoreFromSnapshot(&meta, data)
		if err != nil {
			return fmt.Errorf("restore table %d: %w", i, err)
		}

		rb.tables = append(rb.tables, t)
		rb.tablesByCoefficient[t.Coefficient()] = t
	}

	return nil
}

// DeleteSnapshot removes a specific snapshot (manifest + data files) from dir.
func (rb *RamBlock) DeleteSnapshot(dir string, id string) error {
	manifestPath := snapshotManifestPath(dir, id)
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	var manifest SnapshotManifest
	if err := msgpack.Unmarshal(manifestData, &manifest); err != nil {
		return fmt.Errorf("unmarshal manifest: %w", err)
	}

	for i := range manifest.Tables {
		dataPath := snapshotDataPath(dir, id, i)
		if err := os.Remove(dataPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove table data %d: %w", i, err)
		}
	}

	if err := os.Remove(manifestPath); err != nil {
		return fmt.Errorf("remove manifest: %w", err)
	}

	return nil
}
