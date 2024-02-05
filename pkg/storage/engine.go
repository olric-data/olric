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

package storage

import (
	"errors"
	"log"
)

// ErrKeyTooLarge is an error that indicates the given key is larger than the determined key size.
// The current maximum key length is 256.
var ErrKeyTooLarge = errors.New("key too large")

// ErrEntryTooLarge returned if required space for an entry is bigger than table size.
var ErrEntryTooLarge = errors.New("entry too large for the configured table size")

// ErrKeyNotFound is an error that indicates that the requested key could not be found in the DB.
var ErrKeyNotFound = errors.New("key not found")

// ErrNotImplemented means that the interface implementation does not support
// the functionality required to fulfill the request.
var ErrNotImplemented = errors.New("not implemented yet")

// TransferIterator is an interface to implement iterators to encode and transfer
// the underlying tables to another Olric member.
type TransferIterator interface {
	// Next returns true if there are more tables to Export in the storage instance.
	// Otherwise, it returns false.
	Next() bool

	// Export encodes a table and returns result. This encoded table can be moved to another Olric node.
	Export() ([]byte, int, error)

	// Drop drops a table with its index from the storage engine instance and frees allocated resources.
	Drop(int) error
}

// Engine defines methods for a storage engine implementation.
type Engine interface {
	// SetConfig sets a storage engine configuration. nil can be accepted, but
	// it depends on the implementation.
	SetConfig(*Config)

	// SetLogger sets a logger. nil can be accepted, but it depends on the implementation.
	SetLogger(*log.Logger)

	// Start can be used to run background services before starting operation.
	Start() error

	// NewEntry returns a new Entry interface implemented by the current storage
	// engine implementation.
	NewEntry() Entry

	// Name returns name of the current storage engine implementation.
	Name() string

	// Fork creates an empty instance of an online engine by using the current
	// configuration.
	Fork(*Config) (Engine, error)

	// PutRaw inserts an encoded entry into the storage engine.
	PutRaw(uint64, []byte) error

	// Put inserts a new Entry into the storage engine.
	Put(uint64, Entry) error

	// GetRaw reads an encoded entry from the storage engine.
	GetRaw(uint64) ([]byte, error)

	// Get reads an entry from the storage engine.
	Get(uint64) (Entry, error)

	// GetTTL extracts TTL of an entry.
	GetTTL(uint64) (int64, error)

	// GetLastAccess extracts LastAccess of an entry.
	GetLastAccess(uint64) (int64, error)

	// GetKey extracts key of an entry.
	GetKey(uint64) (string, error)

	// Delete deletes an entry from the storage engine.
	Delete(uint64) error

	// UpdateTTL updates TTL of an entry. It returns ErrKeyNotFound,
	// if the key doesn't exist.
	UpdateTTL(uint64, Entry) error

	// TransferIterator returns a new TransferIterator instance to the caller.
	TransferIterator() TransferIterator

	// Import imports an encoded table of the storage engine implementation and
	// calls f for every Entry item in that table.
	Import(data []byte, f func(uint64, Entry) error) error

	// Stats returns metrics for an online storage engine.
	Stats() Stats

	// Check returns true, if the key exists.
	Check(uint64) bool

	// Range implements a loop over the storage engine
	Range(func(uint64, Entry) bool)

	// RangeHKey implements a loop for hashed keys(HKeys).
	RangeHKey(func(uint64) bool)

	// Scan implements an iterator. The caller starts iterating from the cursor. "count" is the number of entries
	// that will be returned during the iteration. Scan calls the function "f" on Entry items for every iteration.
	//It returns the next cursor if everything is okay. Otherwise, it returns an error.
	Scan(cursor uint64, count int, f func(Entry) bool) (uint64, error)

	// ScanRegexMatch is the same with the Scan method, but it supports regular expressions on keys.
	ScanRegexMatch(cursor uint64, match string, count int, f func(Entry) bool) (uint64, error)

	// Compaction reorganizes storage tables and reclaims wasted resources.
	Compaction() (bool, error)

	// Close stops an online storage engine instance. It may free some of allocated
	// resources. A storage engine implementation should be started again, but it
	// depends on the implementation.
	Close() error

	// Destroy stops an online storage engine instance and frees allocated resources.
	// It should not be possible to reuse a destroyed storage engine.
	Destroy() error
}
