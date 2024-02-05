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

package kvstore

import (
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/kvstore/table"
	"github.com/buraksezer/olric/pkg/storage"
)

func (k *KVStore) evictTable(t *table.Table) error {
	var total int
	var evictErr error
	t.Range(func(hkey uint64, e storage.Entry) bool {
		entry, _ := t.GetRaw(hkey)
		err := k.PutRaw(hkey, entry)
		if errors.Is(err, table.ErrNotEnoughSpace) {
			err := k.makeTable()
			if err != nil {
				evictErr = err
				return false
			}
			// try again
			return false
		}
		if err != nil {
			// log this error and continue
			evictErr = fmt.Errorf("put command failed: HKey: %d: %w", hkey, err)
			return false
		}

		err = t.Delete(hkey)
		if errors.Is(err, table.ErrHKeyNotFound) {
			err = nil
		}
		if err != nil {
			evictErr = err
			return false
		}
		total++

		return total <= 1000
	})

	stats := t.Stats()
	if stats.Inuse == 0 {
		delete(k.tablesByCoefficient, t.Coefficient())
		t.Reset()
	}

	return evictErr
}

func (k *KVStore) isTableExpired(recycledAt int64) bool {
	timeout, err := k.config.Get("maxIdleTableTimeout")
	if err != nil {
		// That would be impossible
		panic(err)
	}
	limit := (timeout.(time.Duration).Nanoseconds() + recycledAt) / 1000000
	return (time.Now().UnixNano() / 1000000) >= limit
}

func (k *KVStore) isCompactionOK(t *table.Table) bool {
	s := t.Stats()
	return float64(s.Garbage) >= float64(s.Allocated)*maxGarbageRatio
}

func (k *KVStore) Compaction() (bool, error) {
	for _, t := range k.tables {
		if k.isCompactionOK(t) {
			err := k.evictTable(t)
			if err != nil {
				return false, err
			}
			// Continue scanning
			return false, nil
		}
	}

	for i := 0; i < len(k.tables); i++ {
		t := k.tables[i]
		s := t.Stats()
		if t.State() == table.RecycledState {
			if k.isTableExpired(s.RecycledAt) {
				if len(k.tables) == 1 {
					break
				}
				delete(k.tablesByCoefficient, t.Coefficient())
				k.tables = append(k.tables[:i], k.tables[i+1:]...)
				i--
			}
		}
	}

	return true, nil
}
