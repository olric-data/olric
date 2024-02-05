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

package partitions

import (
	"sync"
	"unsafe"

	"github.com/buraksezer/olric/hasher"
)

var (
	hashFunc hasher.Hasher
	once     sync.Once
)

func SetHashFunc(h hasher.Hasher) {
	once.Do(func() {
		hashFunc = h
	})
}

func HKey(name, key string) uint64 {
	tmp := name + key
	return hashFunc.Sum64(*(*[]byte)(unsafe.Pointer(&tmp)))
}
