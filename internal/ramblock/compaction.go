// Copyright 2018-2026 The Olric Authors
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
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/olric-data/olric/internal/ramblock/table"
	"github.com/olric-data/olric/pkg/storage"
)

func (rb *RamBlock) evictTable(t *table.Table) error {
	if t == rb.tables[len(rb.tables)-1] {
		// The latest table is the target of every write. Rotate it before
		// eviction: otherwise the entries of t would be copied back into t
		// itself and leave unreachable bytes behind, keeping Inuse above zero
		// so the table could never be recycled.
		if err := rb.makeTable(); err != nil {
			return err
		}
	}

	var total int
	var evictErr error
	t.Range(func(hkey uint64, e storage.Entry) bool {
		if rb.hasNewerVersion(hkey, t) {
			// The key has been overwritten by a newer version stored in a
			// younger table. Delete the stale entry instead of copying it to
			// the latest table, where it would shadow the newer version.
			if err := t.Delete(hkey); err != nil {
				evictErr = err
				return false
			}
			total++
			return true
		}

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

// hasNewerVersion reports whether the key exists in a table with a higher
// coefficient. New versions are always written to the latest table, so a
// higher coefficient means that the copy stored in t is stale.
func (rb *RamBlock) hasNewerVersion(hkey uint64, t *table.Table) bool {
	coefficient := t.Coefficient()
	for _, other := range rb.tablesByCoefficient {
		if other.Coefficient() > coefficient && other.Check(hkey) {
			return true
		}
	}
	return false
}

// purgeStaleEntries deletes entries that have been overwritten by a newer
// version of the same key stored in a more recently created table. Stale
// entries are deleted instead of being moved to a new table, so they cannot
// occupy memory again. A ReadOnly table emptied by the purge is recycled
// immediately.
//
// Tables are scanned from the youngest one to the oldest one while keeping a
// set of hkeys observed in younger tables on the side. The cost is linear in
// the total number of entries, and the temporary memory usage is bounded by
// the number of live keys.
func (rb *RamBlock) purgeStaleEntries() error {
	// The purge is opt-in (see defaultPurgeStaleEntries): it is skipped unless
	// the "purgeStaleEntries" engine config is explicitly set to true.
	enabled, err := rb.config.Get("purgeStaleEntries")
	if err != nil {
		return nil
	}
	if v, ok := enabled.(bool); !ok || !v {
		return nil
	}

	if len(rb.tables) <= 1 {
		return nil
	}

	tables := make([]*table.Table, len(rb.tables))
	copy(tables, rb.tables)
	// Process the youngest table first, so the latest version of a key is the
	// one that stays when the same key exists in multiple tables.
	slices.SortFunc(tables, func(a, b *table.Table) int {
		return cmp.Compare(b.Coefficient(), a.Coefficient())
	})

	var purgeErr error
	observed := make(map[uint64]struct{})
	for _, t := range tables {
		if t.State() == table.RecycledState {
			continue
		}

		t.RangeHKey(func(hkey uint64) bool {
			if _, ok := observed[hkey]; ok {
				// This entry has been overwritten in a younger table. Deleting
				// the key that is currently visited is safe.
				err := t.Delete(hkey)
				if errors.Is(err, table.ErrHKeyNotFound) {
					err = nil
				}
				if err != nil {
					purgeErr = err
					return false
				}
				return true
			}
			observed[hkey] = struct{}{}
			return true
		})
		if purgeErr != nil {
			return purgeErr
		}

		if t.State() == table.ReadOnlyState && t.Stats().Inuse == 0 {
			delete(rb.tablesByCoefficient, t.Coefficient())
			t.Reset()
		}
	}

	return nil
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
	if err := rb.purgeStaleEntries(); err != nil {
		return false, err
	}

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
