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
	"sync/atomic"

	"github.com/buraksezer/olric/internal/discovery"
)

// Partition is a basic, logical storage unit in Olric and stores DMaps in a sync.Map.
type Partition struct {
	sync.RWMutex

	id     uint64
	kind   Kind
	m      *sync.Map
	owners atomic.Value
}

func (p *Partition) Kind() Kind {
	return p.kind
}

func (p *Partition) ID() uint64 {
	return p.id
}

func (p *Partition) Map() *sync.Map {
	return p.m
}

// Owner returns partition Owner. It's not thread-safe.
func (p *Partition) Owner() discovery.Member {
	if p.Kind() == BACKUP {
		// programming error. it cannot occur at production!
		panic("cannot call this if backup is true")
	}
	owners := p.owners.Load().([]discovery.Member)
	if len(owners) == 0 {
		panic("owners list cannot be empty")
	}
	return owners[len(owners)-1]
}

// OwnerCount returns the current Owner count of a partition.
func (p *Partition) OwnerCount() int {
	owners := p.owners.Load()
	if owners == nil {
		return 0
	}
	return len(owners.([]discovery.Member))
}

// Owners loads the partition owners from atomic.Value and returns.
func (p *Partition) Owners() []discovery.Member {
	owners := p.owners.Load()
	if owners == nil {
		return []discovery.Member{}
	}
	return owners.([]discovery.Member)
}

func (p *Partition) SetOwners(owners []discovery.Member) {
	p.owners.Store(owners)
}

func (p *Partition) Length() int {
	var length int
	p.Map().Range(func(_, tmp interface{}) bool {
		u := tmp.(Fragment)
		length += u.Stats().Length
		// Continue scanning.
		return true
	})
	return length
}
