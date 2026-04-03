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
	"encoding/binary"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/olric-data/olric/internal/ramblock/entry"
	"github.com/olric-data/olric/pkg/storage"
	"github.com/pkg/errors"
)

const (
	// MaxKeyLength is the maximum allowed key size in bytes.
	MaxKeyLength = 256

	// MetadataLength is the fixed number of bytes used to store per-entry metadata
	// (TTL + Timestamp + LastAccess + ValueLength + KeyLength = 8+8+8+4+1 = 29).
	MetadataLength = 29
)

// State represents the operational state of a Table.
type State uint8

const (
	// ReadWriteState indicates the table accepts both reads and writes.
	ReadWriteState = State(iota + 1)

	// ReadOnlyState indicates the table only accepts read operations.
	ReadOnlyState

	// RecycledState indicates the table has been reset and is ready for reuse.
	RecycledState
)

var (
	// ErrNotEnoughSpace is returned when the table's pre-allocated memory buffer
	// does not have enough room to store a new entry.
	ErrNotEnoughSpace = errors.New("not enough space")

	// ErrHKeyNotFound is returned when the given hash key does not exist in the table.
	ErrHKeyNotFound = errors.New("hkey not found")
)

// Stats holds memory usage statistics and metadata for a Table.
type Stats struct {
	// Allocated is the total size of the pre-allocated memory buffer in bytes.
	Allocated uint64

	// Inuse is the number of bytes currently occupied by active entries.
	Inuse uint64

	// Garbage is the number of bytes occupied by deleted entries that have not been reclaimed.
	Garbage uint64

	// Length is the number of active entries in the table.
	Length int

	// RecycledAt is the UnixNano timestamp of the last Reset call, or zero if never recycled.
	RecycledAt int64
}

// Table is an in-memory key-value store backed by a pre-allocated byte slice.
// Entries are written sequentially into the buffer using a compact binary layout:
//
//	KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | TIMESTAMP(uint64) | LASTACCESS(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
//
// A hash key (uint64) to offset mapping provides O(1) lookups. Deleted entries
// are tracked as garbage but not reclaimed until the table is compacted or recycled.
type Table struct {
	lastAccessMtx sync.RWMutex
	coefficient   uint64
	offset        uint64
	allocated     uint64
	inuse         uint64
	garbage       uint64
	recycledAt    int64
	state         State
	hkeys         map[uint64]uint64
	offsetIndex   *roaring64.Bitmap
	memory        []byte
}

// New creates a new Table with a pre-allocated memory buffer of the given size in bytes.
func New(size uint64) *Table {
	t := &Table{
		hkeys:       make(map[uint64]uint64),
		allocated:   size,
		offsetIndex: roaring64.New(),
		state:       ReadWriteState,
	}
	//  From builtin.go:
	//
	//  The size specifies the length. The capacity of the slice is
	//	equal to its length. A second integer argument may be provided to
	//	specify a different capacity; it must be no smaller than the
	//	length. For example, make([]int, 0, 10) allocates an underlying array
	//	of size 10 and returns a slice of length 0 and capacity 10 that is
	//	backed by this underlying array.
	t.memory = make([]byte, size)
	return t
}

// SetCoefficient sets the coefficient value used for load-balancing and distribution purposes.
func (t *Table) SetCoefficient(cf uint64) {
	t.coefficient = cf
}

// Coefficient returns the current coefficient value of the table.
func (t *Table) Coefficient() uint64 {
	return t.coefficient
}

// SetState sets the operational state of the table.
func (t *Table) SetState(s State) {
	t.state = s
}

// State returns the current operational state of the table.
func (t *Table) State() State {
	return t.state
}

// PutRaw stores pre-encoded raw bytes into the table under the given hash key.
// It copies the value directly into the memory buffer without any metadata encoding.
// Returns ErrNotEnoughSpace if the buffer cannot accommodate the value.
func (t *Table) PutRaw(hkey uint64, value []byte) error {
	// Check empty space on the allocated memory area.
	inuse := uint64(len(value))
	if inuse+t.offset >= t.allocated {
		return ErrNotEnoughSpace
	}
	t.hkeys[hkey] = t.offset
	t.offsetIndex.Add(t.offset)
	copy(t.memory[t.offset:], value)
	t.inuse += inuse
	t.offset += inuse
	return nil
}

// Put stores a storage.Entry into the table under the given hash key. It encodes
// the entry's key, TTL, timestamp, last access time and value into the memory buffer
// using the following binary layout:
//
//	KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | TIMESTAMP(uint64) | LASTACCESS(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
//
// If the hash key already exists, the previous entry is deleted first. Returns
// ErrNotEnoughSpace if the buffer cannot accommodate the entry, or storage.ErrKeyTooLarge
// if the key exceeds MaxKeyLength.
func (t *Table) Put(hkey uint64, value storage.Entry) error {
	if len(value.Key()) >= MaxKeyLength {
		return storage.ErrKeyTooLarge
	}

	// Check empty space on the allocated memory area.

	// TTL + Timestamp + LastAccess + + value-Length + key-Length
	inuse := uint64(len(value.Key()) + len(value.Value()) + MetadataLength)
	if inuse+t.offset >= t.allocated {
		return ErrNotEnoughSpace
	}

	// If we already have the key, delete it.
	err := t.Delete(hkey)
	if errors.Is(err, ErrHKeyNotFound) {
		err = nil
	}
	if err != nil {
		return err
	}

	t.hkeys[hkey] = t.offset
	t.offsetIndex.Add(t.offset)
	t.inuse += inuse

	// Set key length. It's 1 byte.
	klen := uint8(len(value.Key()))
	copy(t.memory[t.offset:], []byte{klen})
	t.offset++

	// Set the key.
	copy(t.memory[t.offset:], value.Key())
	t.offset += uint64(len(value.Key()))

	// Set the TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(value.TTL()))
	t.offset += 8

	// Set the Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(value.Timestamp()))
	t.offset += 8

	// Set the last access. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(time.Now().UnixNano()))
	t.offset += 8

	// Set the value length. It's 4 bytes.
	binary.BigEndian.PutUint32(t.memory[t.offset:], uint32(len(value.Value())))
	t.offset += 4

	// Set the value.
	copy(t.memory[t.offset:], value.Value())
	t.offset += uint64(len(value.Value()))
	return nil
}

// GetRaw returns the raw byte representation of the entry stored under the given hash key.
// The returned slice is a copy and includes the full binary-encoded entry (key, metadata and value).
// Returns ErrHKeyNotFound if the hash key does not exist.
func (t *Table) GetRaw(hkey uint64) ([]byte, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, ErrHKeyNotFound
	}
	start, end := offset, offset

	// In-memory structure:
	// 1                 | klen       | 8           | 8                  | 8                  | 4                    | vlen
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | TIMESTAMP(uint64)  | LASTACCESS(uint64) | VALUE-LENGTH(uint64) | VALUE(bytes)
	klen := uint64(t.memory[end])
	end++       // One byte to keep key length
	end += klen // key length
	end += 8    // TTL
	end += 8    // Timestamp
	end += 8    // LastAccess

	vlen := binary.BigEndian.Uint32(t.memory[end : end+4])
	end += 4            // 4 bytes to keep value length
	end += uint64(vlen) // value length

	// Create a copy of the requested data.
	rawval := make([]byte, end-start)
	copy(rawval, t.memory[start:end])
	return rawval, nil
}

// getRawKey reads and returns the raw key bytes from the memory buffer at the given offset.
func (t *Table) getRawKey(offset uint64) ([]byte, error) {
	klen := uint64(t.memory[offset])
	offset++
	return t.memory[offset : offset+klen], nil
}

// GetRawKey returns the raw key bytes for the given hash key.
// Returns ErrHKeyNotFound if the hash key does not exist.
func (t *Table) GetRawKey(hkey uint64) ([]byte, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, ErrHKeyNotFound
	}

	return t.getRawKey(offset)
}

// GetKey returns the key as a string for the given hash key.
// Returns ErrHKeyNotFound if the hash key does not exist.
func (t *Table) GetKey(hkey uint64) (string, error) {
	raw, err := t.GetRawKey(hkey)
	if raw == nil {
		return "", err
	}
	return string(raw), err
}

// GetTTL returns the TTL value in nanoseconds for the given hash key.
// Returns ErrHKeyNotFound if the hash key does not exist.
func (t *Table) GetTTL(hkey uint64) (int64, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return 0, ErrHKeyNotFound
	}

	klen := uint64(t.memory[offset])
	offset++
	offset += klen

	return int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])), nil
}

// GetLastAccess returns the last access timestamp in nanoseconds for the given hash key.
// Returns ErrHKeyNotFound if the hash key does not exist.
func (t *Table) GetLastAccess(hkey uint64) (int64, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return 0, ErrHKeyNotFound
	}

	klen := uint64(t.memory[offset])
	offset++       // Key length
	offset += klen // Key's itself
	offset += 8    // TTL
	offset += 8    // Timestamp

	return int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])), nil
}

// get decodes a storage.Entry from the memory buffer at the given offset and updates
// the entry's last access time to the current time. It is used internally by Scan methods.
func (t *Table) get(offset uint64) storage.Entry {
	e := &entry.Entry{}
	// In-memory structure:
	//
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | TIMESTAMP(uint64) | LASTACCESS(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := uint64(t.memory[offset])
	offset++

	e.SetKey(string(t.memory[offset : offset+klen]))
	offset += klen

	e.SetTTL(int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])))
	offset += 8

	e.SetTimestamp(int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])))
	offset += 8

	// Every SCAN call updates the last access time. We have to serialize the access to that field.
	t.lastAccessMtx.RLock()
	e.SetLastAccess(int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])))
	t.lastAccessMtx.RUnlock()

	// Update the last access field
	lastAccess := uint64(time.Now().UnixNano())
	t.lastAccessMtx.Lock()
	binary.BigEndian.PutUint64(t.memory[offset:], lastAccess)
	t.lastAccessMtx.Unlock()
	offset += 8

	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	offset += 4
	e.SetValue(t.memory[offset : offset+uint64(vlen)])
	return e
}

// Get retrieves the storage.Entry for the given hash key and updates the entry's
// last access time. Returns ErrHKeyNotFound if the hash key does not exist.
func (t *Table) Get(hkey uint64) (storage.Entry, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, ErrHKeyNotFound
	}

	return t.get(offset), nil
}

// Delete removes the entry associated with the given hash key from the table.
// The occupied memory is marked as garbage but not reclaimed. Returns
// ErrHKeyNotFound if the hash key does not exist.
func (t *Table) Delete(hkey uint64) error {
	offset, ok := t.hkeys[hkey]
	if !ok {
		// Try the previous tables.
		return ErrHKeyNotFound
	}
	var garbage uint64

	// key, 1 byte for key size, klen for key's actual length.
	klen := uint64(t.memory[offset])

	// Delete the offset from offsetIndex
	t.offsetIndex.Remove(offset)

	offset += 1 + klen
	garbage += 1 + klen

	// TTL, skip it.
	offset += 8
	garbage += 8

	// Timestamp, skip it.
	offset += 8
	garbage += 8

	// LastAccess, skip it.
	offset += 8
	garbage += 8

	// value len and its header.
	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	garbage += 4 + uint64(vlen)

	// Delete it from metadata
	delete(t.hkeys, hkey)

	t.garbage += garbage
	t.inuse -= garbage
	return nil
}

// UpdateTTL updates the TTL and timestamp fields of the entry identified by the
// given hash key in-place, and refreshes its last access time. Returns
// ErrHKeyNotFound if the hash key does not exist.
func (t *Table) UpdateTTL(hkey uint64, value storage.Entry) error {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return ErrHKeyNotFound
	}

	// key, 1 byte for key size, klen for key's actual length.
	klen := uint64(t.memory[offset])
	offset += 1 + klen

	// Set the new TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(value.TTL()))
	offset += 8

	// Set the new Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(value.Timestamp()))

	offset += 8

	// Update the last access field
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(time.Now().UnixNano()))

	return nil
}

// Check reports whether the given hash key exists in the table.
func (t *Table) Check(hkey uint64) bool {
	_, ok := t.hkeys[hkey]
	return ok
}

// Stats returns the current memory usage statistics for the table.
func (t *Table) Stats() Stats {
	return Stats{
		Allocated:  t.allocated,
		Inuse:      t.inuse,
		Garbage:    t.garbage,
		Length:     len(t.hkeys),
		RecycledAt: t.recycledAt,
	}
}

// Range iterates over all entries in the table, calling f for each one.
// If f returns false, iteration stops. The iteration order is non-deterministic.
func (t *Table) Range(f func(hkey uint64, e storage.Entry) bool) {
	for hkey := range t.hkeys {
		e, err := t.Get(hkey)
		if errors.Is(err, ErrHKeyNotFound) {
			panic(fmt.Errorf("hkey: %d found in index, but Get could not find it", hkey))
		}

		if !f(hkey, e) {
			break
		}
	}
}

// RangeHKey iterates over all hash keys in the table without decoding entries.
// If f returns false, iteration stops. The iteration order is non-deterministic.
func (t *Table) RangeHKey(f func(hkey uint64) bool) {
	for hkey := range t.hkeys {
		if !f(hkey) {
			break
		}
	}
}

// Reset clears all entries and metadata, resets memory usage counters, and
// transitions the table to RecycledState. The underlying memory buffer is
// retained for reuse.
func (t *Table) Reset() {
	if len(t.hkeys) != 0 {
		t.hkeys = make(map[uint64]uint64)
	}
	t.offsetIndex = roaring64.New()
	t.SetState(RecycledState)
	t.inuse = 0
	t.garbage = 0
	t.offset = 0
	t.coefficient = 0
	t.recycledAt = time.Now().UnixNano()
}

// Scan performs a cursor-based iteration over the table entries. Starting from
// the given cursor position, it calls f for up to count entries. It returns
// the next cursor to resume scanning, or 0 when all entries have been visited.
// If f returns false, iteration stops early.
func (t *Table) Scan(cursor uint64, count int, f func(e storage.Entry) bool) (uint64, error) {
	it := t.offsetIndex.Iterator()
	if cursor != 0 {
		it.AdvanceIfNeeded(cursor)
	}
	var num int
	for it.HasNext() && num < count {
		offset := it.Next()
		e := t.get(offset)
		if !f(e) {
			break
		}
		cursor = offset + 1
		num++
	}

	if !it.HasNext() {
		// end of the scan
		cursor = 0
	}

	return cursor, nil
}

// ScanRegexMatch performs a cursor-based iteration like Scan, but only yields
// entries whose keys match the given regular expression. Returns the next cursor
// to resume scanning, or 0 when all entries have been visited. Returns an error
// if the regular expression is invalid.
func (t *Table) ScanRegexMatch(cursor uint64, expr string, count int, f func(e storage.Entry) bool) (uint64, error) {
	r, err := regexp.Compile(expr)
	if err != nil {
		return 0, err
	}

	it := t.offsetIndex.Iterator()
	if cursor != 0 {
		it.AdvanceIfNeeded(cursor)
	}

	var num int
	for it.HasNext() && num < count {
		offset := it.Next()

		key, _ := t.getRawKey(offset)
		if !r.Match(key) {
			continue
		}

		e := t.get(offset)
		if !f(e) {
			break
		}
		cursor = offset + 1
		num++
	}

	if !it.HasNext() {
		// end of the scan
		cursor = 0
	}
	return cursor, nil
}
