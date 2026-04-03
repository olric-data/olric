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
	"github.com/vmihailenco/msgpack/v5"
)

// Pack is the serializable representation of a Table. It is used by Encode and
// Decode to transfer table data between nodes via msgpack serialization.
type Pack struct {
	Offset      uint64
	Allocated   uint64
	Inuse       uint64
	Garbage     uint64
	RecycledAt  int64
	State       State
	HKeys       map[uint64]uint64
	OffsetIndex []byte
	Memory      []byte
}

// Encode serializes the given Table into a msgpack-encoded byte slice. Only the
// active portion of the memory buffer (up to the current offset) is included.
func Encode(t *Table) ([]byte, error) {
	offsetIndex, err := t.offsetIndex.MarshalBinary()
	if err != nil {
		return nil, err
	}
	p := Pack{
		Offset:      t.offset,
		Allocated:   t.allocated,
		Inuse:       t.inuse,
		Garbage:     t.garbage,
		RecycledAt:  t.recycledAt,
		State:       t.state,
		HKeys:       t.hkeys,
		OffsetIndex: offsetIndex,
	}
	p.Memory = make([]byte, t.offset)
	copy(p.Memory, t.memory[:t.offset])

	return msgpack.Marshal(p)
}

// Decode deserializes a msgpack-encoded byte slice into a new Table, restoring
// all entries, metadata, and the offset index.
func Decode(data []byte) (*Table, error) {
	p := &Pack{}
	err := msgpack.Unmarshal(data, p)
	if err != nil {
		return nil, err
	}

	rb := roaring64.New()
	err = rb.UnmarshalBinary(p.OffsetIndex)
	if err != nil {
		return nil, err
	}

	t := New(p.Allocated)
	t.offset = p.Offset
	t.inuse = p.Inuse
	t.garbage = p.Garbage
	t.recycledAt = p.RecycledAt
	t.state = p.State
	t.hkeys = p.HKeys
	t.offsetIndex = rb

	copy(t.memory[:t.offset], p.Memory)

	return t, nil
}
