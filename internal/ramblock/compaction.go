// Copyright 2018-2025 The Olric Authors
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

package ramblock

import (
	"errors"
	"fmt"
	"time"

	"github.com/olric-data/olric/internal/ramblock/table"
	"github.com/olric-data/olric/pkg/storage"
)

func (rb *RamBlock) evictTable(t *table.Table) error {
	var total int
	var evictErr error
	t.Range(func(hkey uint64, e storage.Entry) bool {
		entry, _ := t.GetRaw(hkey)
		err := rb.PutRaw(hkey, entry)
		if errors.Is(err, table.ErrNotEnoughSpace) {
			err := rb.makeTable()
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
		delete(rb.tablesByCoefficient, t.Coefficient())
		t.Reset()
	}

	return evictErr
}

func (rb *RamBlock) isTableExpired(recycledAt int64) bool {
	timeout, err := rb.config.Get("maxIdleTableTimeout")
	if err != nil {
		// That would be impossible
		panic(err)
	}
	limit := (timeout.(time.Duration).Nanoseconds() + recycledAt) / 1000000
	return (time.Now().UnixNano() / 1000000) >= limit
}

func (rb *RamBlock) isCompactionOK(t *table.Table) bool {
	s := t.Stats()
	return float64(s.Garbage) >= float64(s.Allocated)*maxGarbageRatio
}

func (rb *RamBlock) Compaction() (bool, error) {
	for _, t := range rb.tables {
		if rb.isCompactionOK(t) {
			err := rb.evictTable(t)
			if err != nil {
				return false, err
			}
			// Continue scanning
			return false, nil
		}
	}

	for i := 0; i < len(rb.tables); i++ {
		t := rb.tables[i]
		s := t.Stats()
		if t.State() == table.RecycledState {
			if rb.isTableExpired(s.RecycledAt) {
				if len(rb.tables) == 1 {
					break
				}
				delete(rb.tablesByCoefficient, t.Coefficient())
				rb.tables = append(rb.tables[:i], rb.tables[i+1:]...)
				i--
			}
		}
	}

	return true, nil
}
