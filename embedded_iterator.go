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

package olric

import (
	"sync"

	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/protocol"
)

// EmbeddedIterator implements distributed query on DMaps.
type EmbeddedIterator struct {
	mtx sync.Mutex

	client          *EmbeddedClient
	dm              *dmap.DMap
	clusterIterator *ClusterIterator
}

func (e *EmbeddedIterator) scanOnOwners() error {
	owners := e.clusterIterator.getOwners()

	for idx, owner := range owners {
		cursor := e.clusterIterator.loadCursor(owner)

		if e.client.db.rt.This().String() == owner {
			keys, newCursor, err := e.dm.Scan(e.clusterIterator.partID, cursor, e.clusterIterator.config)
			if err != nil {
				return err
			}
			e.clusterIterator.updateIterator(keys, newCursor, owner)
			if newCursor == 0 {
				e.clusterIterator.removeScannedOwner(idx)
			}
			continue
		}

		// Build a scan command here
		s := protocol.NewScan(e.clusterIterator.partID, e.clusterIterator.dm.Name(), cursor)
		if e.clusterIterator.config.HasCount {
			s.SetCount(e.clusterIterator.config.Count)
		}
		if e.clusterIterator.config.HasMatch {
			s.SetMatch(e.clusterIterator.config.Match)
		}
		if e.clusterIterator.config.Replica {
			s.SetReplica()
		}

		scanCmd := s.Command(e.clusterIterator.ctx)
		// Fetch a Redis client for the given owner.
		rc := e.clusterIterator.clusterClient.client.Get(owner)
		err := rc.Process(e.clusterIterator.ctx, scanCmd)
		if err != nil {
			return err
		}

		keys, newCursor, err := scanCmd.Result()
		if err != nil {
			return err
		}
		e.clusterIterator.updateIterator(keys, newCursor, owner)
		if newCursor == 0 {
			e.clusterIterator.removeScannedOwner(idx)
		}
	}
	return nil
}

// Next returns true if there is more key in the iterator implementation.
// Otherwise, it returns false.
func (e *EmbeddedIterator) Next() bool {
	return e.clusterIterator.Next()
}

// Key returns a key name from the distributed map.
func (e *EmbeddedIterator) Key() string {
	return e.clusterIterator.Key()
}

// Close stops the iteration and releases allocated resources.
func (e *EmbeddedIterator) Close() {
	e.clusterIterator.Close()
}
