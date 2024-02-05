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

package hasher

import "github.com/cespare/xxhash/v2"

// NewDefaultHasher returns an instance of xxhash package which implements the 64-bit variant of
// xxHash (XXH64) as described at http://cyan4973.github.io/xxHash/.
func NewDefaultHasher() Hasher {
	return xxhasher{}
}

type xxhasher struct{}

func (x xxhasher) Sum64(key []byte) uint64 {
	return xxhash.Sum64(key)
}

// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
// Hasher should minimize collisions (generating same hash for different byte slice)
// and while performance is also important fast functions are preferable (i.e.
// you can use FarmHash family).
type Hasher interface {
	Sum64([]byte) uint64
}
