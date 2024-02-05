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

package routingtable

import (
	"fmt"
	"sync"

	"github.com/buraksezer/olric/internal/discovery"
)

type Members struct {
	sync.RWMutex
	m map[uint64]discovery.Member
}

func newMembers() *Members {
	return &Members{
		m: map[uint64]discovery.Member{},
	}
}

func (m *Members) Add(member discovery.Member) {
	m.m[member.ID] = member
}

func (m *Members) Get(id uint64) (discovery.Member, error) {
	member, ok := m.m[id]
	if !ok {
		return discovery.Member{}, fmt.Errorf("member not found with id: %d", id)
	}
	return member, nil
}

func (m *Members) Delete(id uint64) {
	delete(m.m, id)
}

func (m *Members) DeleteByName(other discovery.Member) {
	for id, member := range m.m {
		if member.CompareByName(other) {
			delete(m.m, id)
		}
	}
}

func (m *Members) Length() int {
	return len(m.m)
}

func (m *Members) Range(f func(id uint64, member discovery.Member) bool) {
	for id, member := range m.m {
		if !f(id, member) {
			break
		}
	}
}
