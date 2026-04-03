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
	"github.com/RoaringBitmap/roaring/roaring64"
)

// SnapshotMeta holds all metadata of a Table needed to persist and restore it.
// The raw memory buffer is handled separately for zero-copy writes.
type SnapshotMeta struct {
	Coefficient uint64
	Offset      uint64
	Allocated   uint64
	Inuse       uint64
	Garbage     uint64
	RecycledAt  int64
	State       State
	HKeys       map[uint64]uint64
	OffsetIndex []byte // roaring bitmap binary
}

// ExportSnapshotMeta extracts all metadata from a Table for snapshot persistence.
func ExportSnapshotMeta(t *Table) (*SnapshotMeta, error) {
	offsetIndex, err := t.offsetIndex.MarshalBinary()
	if err != nil {
		return nil, err
	}

	hkeys := make(map[uint64]uint64, len(t.hkeys))
	for k, v := range t.hkeys {
		hkeys[k] = v
	}

	return &SnapshotMeta{
		Coefficient: t.coefficient,
		Offset:      t.offset,
		Allocated:   t.allocated,
		Inuse:       t.inuse,
		Garbage:     t.garbage,
		RecycledAt:  t.recycledAt,
		State:       t.state,
		HKeys:       hkeys,
		OffsetIndex: offsetIndex,
	}, nil
}

// RestoreFromSnapshot creates a Table from metadata and raw memory data.
func RestoreFromSnapshot(meta *SnapshotMeta, data []byte) (*Table, error) {
	rb := roaring64.New()
	if err := rb.UnmarshalBinary(meta.OffsetIndex); err != nil {
		return nil, err
	}

	t := New(meta.Allocated)
	t.coefficient = meta.Coefficient
	t.offset = meta.Offset
	t.inuse = meta.Inuse
	t.garbage = meta.Garbage
	t.recycledAt = meta.RecycledAt
	t.state = meta.State
	t.hkeys = meta.HKeys
	t.offsetIndex = rb

	copy(t.memory[:meta.Offset], data)

	return t, nil
}

// SnapshotData returns the active portion of the table's memory buffer.
// The returned slice is a view into the table's internal memory.
func (t *Table) SnapshotData() []byte {
	return t.memory[:t.offset]
}
