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

package dmap

import (
	"errors"
	"sync"
	"testing"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
)

func TestDMap_Fragment(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	t.Run("loadFragment", func(t *testing.T) {
		part := s.primary.PartitionByID(1)
		_, err = dm.loadFragment(part)
		if !errors.Is(err, errFragmentNotFound) {
			t.Fatalf("Expected %v. Got: %v", errFragmentNotFound, err)
		}
	})

	t.Run("newFragment", func(t *testing.T) {
		_, err := dm.newFragment()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	t.Run("loadFragment -- errFragmentNotFound", func(t *testing.T) {
		part := dm.getPartitionByHKey(123, partitions.PRIMARY)
		_, err := dm.loadFragment(part)
		if !errors.Is(err, errFragmentNotFound) {
			t.Fatalf("Expected %v. Got: %v", errFragmentNotFound, err)
		}
	})

	t.Run("loadOrCreateFragment", func(t *testing.T) {
		part := dm.getPartitionByHKey(123, partitions.PRIMARY)
		_, err = dm.loadOrCreateFragment(part)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		_, err := dm.loadFragment(part)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
}

func TestDMap_Fragment_Concurrent_Access(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	part := dm.getPartitionByHKey(123, partitions.PRIMARY)

	var mtx sync.RWMutex
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			f, err := dm.loadOrCreateFragment(part)
			if err != nil {
				t.Errorf("Expected nil. Got: %v", err)
			}

			e := f.storage.NewEntry()
			e.SetKey(testutil.ToKey(idx))

			mtx.Lock()
			// storage engine is not thread-safe
			err = f.storage.Put(uint64(idx), e)
			mtx.Unlock()

			if err != nil {
				t.Errorf("Expected nil. Got: %v", err)
			}
		}(i)
	}

	wg.Wait()

	f, err := dm.loadFragment(part)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 1000; i++ {
		entry, err := f.storage.Get(uint64(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if entry.Key() != testutil.ToKey(i) {
			t.Fatalf("Expected key: %s. Got: %s", testutil.ToKey(i), entry.Key())
		}
	}
}
