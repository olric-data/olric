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
	"strings"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
)

func wipeOutFragment(part *partitions.Partition, name string, f *fragment) error {
	// Stop background services if there is any.
	err := f.Close()
	if err != nil {
		return err
	}
	// Destroy data on-disk or in-memory.
	err = f.Destroy()
	if err != nil {
		return err
	}
	// Delete the fragment from partition.
	part.Map().Delete(name)
	return nil
}

func (s *Service) janitor(part *partitions.Partition) {
	part.Map().Range(func(name, tmp interface{}) bool {
		if !strings.HasPrefix(name.(string), "dmap.") {
			// This fragment belongs to a different data structure.
			return true
		}

		f := tmp.(*fragment)
		f.Lock()
		defer f.Unlock()

		if f.storage.Stats().Length != 0 {
			// It's not empty. Continue scanning.
			return true
		}

		err := wipeOutFragment(part, name.(string), f)
		if err != nil {
			s.log.V(3).Printf("[ERROR] Failed to delete empty DMap fragment (kind: %s): %s on PartID: %d",
				part.Kind(), name, part.ID())
			// continue scanning
			return true
		}

		s.log.V(4).Printf("[INFO] Empty DMap fragment (kind: %s) has been deleted: %s on PartID: %d",
			part.Kind(), name, part.ID())
		return true
	})
}

func (s *Service) deleteEmptyFragments() {
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		// Clean stale DMap fragments on partition table
		part := s.primary.PartitionByID(partID)
		s.janitor(part)

		// Clean stale DMap fragments on backup partition table
		backup := s.backup.PartitionByID(partID)
		s.janitor(backup)
	}
}

func (s *Service) janitorWorker() {
	defer s.wg.Done()
	timer := time.NewTimer(s.config.DMaps.CheckEmptyFragmentsInterval)
	defer timer.Stop()

	for {
		timer.Reset(s.config.DMaps.CheckEmptyFragmentsInterval)
		select {
		case <-timer.C:
			s.deleteEmptyFragments()
		case <-s.ctx.Done():
			return
		}
	}
}
