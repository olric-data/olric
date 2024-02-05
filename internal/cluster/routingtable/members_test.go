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
	"reflect"
	"testing"

	"github.com/buraksezer/olric/internal/discovery"
)

func TestMembers_Get(t *testing.T) {
	m := newMembers()
	member := discovery.Member{
		Name: "localhost:3320",
		ID:   6054057,
	}
	m.Add(member)

	r, err := m.Get(member.ID)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if !reflect.DeepEqual(r, member) {
		t.Fatalf("Retrived member is different")
	}
}
func TestMembers_Delete(t *testing.T) {
	m := newMembers()
	member := discovery.Member{
		Name: "localhost:3320",
		ID:   6054057,
	}
	m.Add(member)
	m.Delete(member.ID)
	_, err := m.Get(member.ID)
	if err == nil {
		t.Fatalf("Expected and error. Got: %v", err)
	}
}

func TestMembers_DeleteByName(t *testing.T) {
	m := newMembers()
	member := discovery.Member{
		Name: "localhost:3320",
		ID:   6054057,
	}
	m.Add(member)
	m.DeleteByName(member)
	_, err := m.Get(member.ID)
	if err == nil {
		t.Fatalf("Expected and error. Got: %v", err)
	}
}

func TestMembers_Length(t *testing.T) {
	m := newMembers()
	member := discovery.Member{
		Name: "localhost:3320",
		ID:   6054057,
	}
	m.Add(member)
	if m.Length() != 1 {
		t.Fatalf("Expected length: 1. Got: %d", m.Length())
	}
}

func TestMembers_Range(t *testing.T) {
	m := newMembers()
	member := discovery.Member{
		Name: "localhost:3320",
		ID:   6054057,
	}
	m.Add(member)
	m.Range(func(id uint64, m discovery.Member) bool {
		if id != member.ID {
			t.Fatalf("Expected id: %d. Got: %d", member.ID, id)
		}

		if member.Name != m.Name {
			t.Fatalf("Expected Name: %s. Got: %s", member.Name, m.Name)
		}
		return true
	})
}
