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
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/pkg/storage"
	"golang.org/x/sync/semaphore"
)

// isKeyIdleOnFragment is not a thread-safe function. It accesses underlying fragment for the given hkey.
func (dm *DMap) isKeyIdleOnFragment(hkey uint64, f *fragment) bool {
	if dm.config == nil {
		return false
	}

	if dm.config.maxIdleDuration.Nanoseconds() == 0 {
		return false
	}
	// Maximum time in seconds for each entry to stay idle in the map.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period
	// exceeds this limit are expired and evicted automatically.
	lastAccess, err := f.storage.GetLastAccess(hkey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false
	}
	//TODO: Handle other errors.
	ttl := (dm.config.maxIdleDuration.Nanoseconds() + lastAccess) / 1000000
	return isKeyExpired(ttl)
}

func (dm *DMap) isKeyIdle(hkey uint64) bool {
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	if errors.Is(err, errFragmentNotFound) {
		// it's no possible to know whether the key is idle or not.
		return false
	}
	if err != nil {
		// This could be a programming error and should never be happened on production systems.
		panic(fmt.Sprintf("failed to get primary partition for: %d: %v", hkey, err))
	}
	f.Lock()
	defer f.Unlock()
	return dm.isKeyIdleOnFragment(hkey, f)
}

func (s *Service) evictKeysAtBackground() {
	defer s.wg.Done()

	num := int64(runtime.NumCPU())
	if s.config.DMaps != nil && s.config.DMaps.NumEvictionWorkers != 0 {
		num = s.config.DMaps.NumEvictionWorkers
	}
	sem := semaphore.NewWeighted(num)
	for {
		if !s.isAlive() {
			return
		}

		if err := sem.Acquire(s.ctx, 1); err != nil {
			s.log.V(3).Printf("[ERROR] Failed to acquire semaphore: %v", err)
			return
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer sem.Release(1)
			// Good for developing tests.
			s.evictKeys()
			select {
			case <-time.After(100 * time.Millisecond):
			case <-s.ctx.Done():
				return
			}
		}()
	}
}

func (s *Service) evictKeys() {
	partID := uint64(rand.Intn(int(s.config.PartitionCount)))
	part := s.primary.PartitionByID(partID)
	part.Map().Range(func(name, tmp interface{}) bool {
		f := tmp.(*fragment)
		s.scanFragmentForEviction(partID, name.(string), f)
		// this breaks the loop, we only scan one dmap instance per call
		return false
	})
}

func (s *Service) scanFragmentForEviction(partID uint64, name string, f *fragment) {
	/*
		From Redis Docs:
			1- Test 20 random keys from the set of keys with an associated expire.
			2- Delete all the keys found expired.
			3- If more than 25% of keys were expired, start again from step 1.
	*/

	// We need limits to prevent CPU starvation. deleteOnCluster does some network operation
	// to delete keys from the backup nodes and the previous owners.
	var maxKeyCount = 20
	var maxTotalCount = 100
	var totalCount = 0

	dm, err := s.getOrCreateDMap(name)
	if err != nil {
		s.log.V(3).Printf("[ERROR] Failed to load DMap: %s: %v", name, err)
		return
	}

	janitor := func() bool {
		if totalCount > maxTotalCount {
			// Release the lock. Eviction will be triggered again.
			return false
		}
		f.Lock()
		defer f.Unlock()
		count, keyCount := 0, 0
		f.storage.RangeHKey(func(hkey uint64) bool {
			keyCount++
			if keyCount >= maxKeyCount {
				// this means 'break'.
				return false
			}
			ttl, err := f.storage.GetTTL(hkey)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to get TTL for: %d", hkey)
				return true // continue
			}
			key, err := f.storage.GetKey(hkey)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to get key for: %d", hkey)
				return true // continue
			}

			if isKeyExpired(ttl) || dm.isKeyIdleOnFragment(hkey, f) {
				err = dm.deleteOnCluster(hkey, key, f)
				if err != nil {
					// It will be tried again.
					dm.s.log.V(3).Printf("[ERROR] Failed to delete expired key: %s on DMap: %s: %v",
						key, dm.name, err)
					return true
				}

				// number of valid items removed from cache to free memory for new items.
				EvictedTotal.Increase(1)
			}
			return true
		})

		totalCount += count
		return count >= maxKeyCount/4
	}

	defer func() {
		if totalCount > 0 {
			if s.log.V(6).Ok() {
				s.log.V(6).Printf("[DEBUG] Evicted key count is %d on PartID: %d", totalCount, partID)
			}
		}
	}()
	for {
		select {
		case <-f.ctx.Done():
			// the fragment is closed.
			return
		case <-s.ctx.Done():
			// The server has gone.
			return
		default:
		}
		// Call janitorWorker again until it returns false.
		if !janitor() {
			return
		}
	}
}

type lruItem struct {
	HKey       uint64
	LastAccess int64
}

func (dm *DMap) evictKeyWithLRU(e *env) error {
	var idx = 1
	var items []lruItem

	// Warning: fragment is already locked by DMap.Put. Be sure about that before editing this function.

	// Pick random items from the distributed map and sort them by accessedAt.
	e.fragment.storage.Range(func(hkey uint64, e storage.Entry) bool {
		if idx >= dm.config.lruSamples {
			return false
		}
		idx++
		i := lruItem{
			HKey:       hkey,
			LastAccess: e.LastAccess(),
		}
		items = append(items, i)
		return true
	})

	if len(items) == 0 {
		return fmt.Errorf("nothing found to expire with LRU")
	}

	sort.Slice(items, func(i, j int) bool { return items[i].LastAccess < items[j].LastAccess })
	// Pick the first item to delete. It's the least recently used item in the sample.
	item := items[0]
	key, err := e.fragment.storage.GetKey(item.HKey)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = ErrKeyNotFound
			GetMisses.Increase(1)
		}
		return err
	}
	// Here we have a key/value pair to evict for making room for a new pair.
	if dm.s.log.V(6).Ok() {
		dm.s.log.V(6).Printf("[DEBUG] Evicted item on DMap: %s, key: %s with LRU", e.dmap, key)
	}
	err = dm.deleteOnCluster(item.HKey, key, e.fragment)
	if err != nil {
		return err
	}

	// number of valid items removed from cache to free memory for new items.
	EvictedTotal.Increase(1)
	return nil
}
