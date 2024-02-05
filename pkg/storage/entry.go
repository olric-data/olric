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

// Entry interface defines methods for a storage entry.
type Entry interface {
	// SetKey accepts strings as a key and inserts the key into the underlying
	// data structure.
	SetKey(string)

	// Key returns the key as string
	Key() string

	// SetValue accepts a byte slice as a value and inserts the value into the
	// underlying data structure.
	SetValue([]byte)

	// Value returns the value as a byte slice.
	Value() []byte

	// SetTTL sets TTL to an entry.
	SetTTL(int64)

	// TTL returns the current TTL for an entry.
	TTL() int64

	// SetTimestamp sets the current timestamp to an entry.
	SetTimestamp(int64)

	// Timestamp returns the current timestamp for an entry.
	Timestamp() int64

	SetLastAccess(int64)

	LastAccess() int64

	// Encode encodes an entry into a binary form and returns the result.
	Encode() []byte

	// Decode decodes a byte slice into an Entry.
	Decode([]byte)
}
