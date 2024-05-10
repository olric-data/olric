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

package mockfragment

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"sync"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/pkg/storage"
)

type Result struct {
	Name   string
	Owners []discovery.Member
}

type MockFragment struct {
	sync.RWMutex
	m      map[string]interface{}
	result map[partitions.Kind]map[uint64]Result
}

func New() *MockFragment {
	return &MockFragment{
		m:      make(map[string]interface{}),
		result: make(map[partitions.Kind]map[uint64]Result),
	}
}

func (f *MockFragment) Stats() storage.Stats {
	f.Lock()
	defer f.Unlock()
	return storage.Stats{
		Length: len(f.m),
	}
}

func (f *MockFragment) Name() string {
	return "Mock-DMap"
}

func (f *MockFragment) Put(key string, value interface{}) {
	f.Lock()
	defer f.Unlock()
	f.m[key] = value
}

func (f *MockFragment) Get(key string) interface{} {
	f.Lock()
	defer f.Unlock()
	return f.m[key]
}

func (f *MockFragment) Delete(key string) {
	f.Lock()
	defer f.Unlock()
	delete(f.m, key)
}

func (f *MockFragment) Fill() {
	n := 5
	b := make([]byte, n)
	randKey := func() string {
		if _, err := rand.Read(b); err != nil {
			panic(err)
		}
		return fmt.Sprintf("%X", b)
	}
	num := mrand.Intn(100)
	for i := 0; i < num; i++ {
		f.Put(randKey(), i)
	}
}

func (f *MockFragment) Result() map[partitions.Kind]map[uint64]Result {
	return f.result
}

func (f *MockFragment) Move(part *partitions.Partition, name string, owners []discovery.Member) error {
	f.Lock()
	defer f.Unlock()

	f.result[part.Kind()] = map[uint64]Result{
		part.ID(): {
			Name:   name,
			Owners: owners,
		},
	}

	for key := range f.m {
		delete(f.m, key)
	}

	return nil
}

func (f *MockFragment) Compaction() (bool, error) {
	return false, nil
}

func (f *MockFragment) Destroy() error {
	f.Lock()
	defer f.Unlock()
	f.m = make(map[string]interface{})
	return nil
}

func (f *MockFragment) Close() error {
	return nil
}

var _ partitions.Fragment = (*MockFragment)(nil)
