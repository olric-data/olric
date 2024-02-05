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

	"github.com/buraksezer/olric/internal/discovery"
)

type Kind int

func (k Kind) String() string {
	switch {
	case k == PRIMARY:
		return "Primary"
	case k == BACKUP:
		return "Backup"
	default:
		return "Unknown"
	}
}

const (
	PRIMARY = Kind(iota + 1)
	BACKUP
)

type Partitions struct {
	count uint64
	kind  Kind
	m     map[uint64]*Partition
}

func New(count uint64, kind Kind) *Partitions {
	ps := &Partitions{
		kind:  kind,
		count: count,
		m:     make(map[uint64]*Partition),
	}
	for i := uint64(0); i < count; i++ {
		ps.m[i] = &Partition{
			id:   i,
			kind: kind,
			m:    &sync.Map{},
		}
	}
	return ps
}

// PartitionByID returns the partition for the given HKey
func (ps *Partitions) PartitionByID(partID uint64) *Partition {
	return ps.m[partID]
}

// PartitionIDByHKey returns partition ID for a given HKey.
func (ps *Partitions) PartitionIDByHKey(hkey uint64) uint64 {
	return hkey % ps.count
}

// PartitionByHKey returns the partition for the given HKey
func (ps *Partitions) PartitionByHKey(hkey uint64) *Partition {
	partID := ps.PartitionIDByHKey(hkey)
	return ps.m[partID]
}

// PartitionOwnersByHKey loads the partition owners list for a given hkey.
func (ps *Partitions) PartitionOwnersByHKey(hkey uint64) []discovery.Member {
	part := ps.PartitionByHKey(hkey)
	return part.owners.Load().([]discovery.Member)
}

// PartitionOwnersByID loads the partition owners list for a given hkey.
func (ps *Partitions) PartitionOwnersByID(partID uint64) []discovery.Member {
	part := ps.PartitionByID(partID)
	return part.owners.Load().([]discovery.Member)
}
