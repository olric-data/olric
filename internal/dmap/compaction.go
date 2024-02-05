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
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"golang.org/x/sync/semaphore"
)

func (s *Service) callCompactionOnFragment(f *fragment) bool {
	for {
		f.Lock()
		done, err := f.Compaction()
		if err != nil {
			f.Unlock()
			// Continue
			return true
		}
		f.Unlock()

		if done {
			return true
		}

		select {
		case <-s.ctx.Done():
			// Break
			return false
		case <-time.After(time.Millisecond):
		}
	}
}

func (s *Service) doCompaction(partID uint64) {
	compaction := func(part *partitions.Partition) {
		part.Map().Range(func(name, tmp interface{}) bool {
			if !strings.HasPrefix(name.(string), "dmap.") {
				// Continue. This fragment belongs to a different data structure.
				return true
			}

			f := tmp.(*fragment)
			return s.callCompactionOnFragment(f)
		})
	}

	part := s.primary.PartitionByID(partID)
	compaction(part)

	backup := s.backup.PartitionByID(partID)
	compaction(backup)
}

func (s *Service) triggerCompaction() {
	var wg sync.WaitGroup

	// NumCPU returns the number of logical CPUs usable by the current process.
	//
	// The set of available CPUs is checked by querying the operating system
	// at process startup. Changes to operating system CPU allocation after
	// process startup are not reflected.
	numWorkers := runtime.NumCPU()
	sem := semaphore.NewWeighted(int64(numWorkers))
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		select {
		case <-s.ctx.Done():
			break
		default:
		}

		if err := sem.Acquire(s.ctx, 1); err != nil {
			if err != context.Canceled {
				s.log.V(3).Printf("[ERROR] Failed to acquire semaphore for DMap compaction: %v", err)
			}
			continue
		}

		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			defer sem.Release(1)
			s.doCompaction(id)
		}(partID)
	}

	wg.Wait()
}

func (s *Service) compactionWorker() {
	defer s.wg.Done()

	timer := time.NewTimer(s.config.DMaps.TriggerCompactionInterval)
	defer timer.Stop()

	for {
		timer.Reset(s.config.DMaps.TriggerCompactionInterval)
		select {
		case <-timer.C:
			s.triggerCompaction()
		case <-s.ctx.Done():
			return
		}
	}
}
