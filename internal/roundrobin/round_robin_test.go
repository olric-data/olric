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

package roundrobin

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundRobin(t *testing.T) {
	items := []string{"127.0.0.1:2323", "127.0.0.1:4556", "127.0.0.1:7889"}
	r := New(items)

	t.Run("Get", func(t *testing.T) {
		items := make(map[string]int)
		for i := 0; i < r.Length(); i++ {
			item, err := r.Get()
			require.NoError(t, err)
			items[item]++
		}
		if len(items) != r.Length() {
			t.Fatalf("Expected item count: %d. Got: %d", r.Length(), len(items))
		}
	})

	t.Run("Add", func(t *testing.T) {
		item := "127.0.0.1:3320"
		r.Add(item)
		items := make(map[string]int)
		for i := 0; i < r.Length(); i++ {
			item, err := r.Get()
			require.NoError(t, err)
			items[item]++
		}
		if _, ok := items[item]; !ok {
			t.Fatalf("Item not processed: %s", item)
		}
		if len(items) != r.Length() {
			t.Fatalf("Expected item count: %d. Got: %d", r.Length(), len(items))
		}
	})

	t.Run("Delete", func(t *testing.T) {
		item := "127.0.0.1:7889"
		r.Delete(item)

		items := make(map[string]int)
		for i := 0; i < r.Length(); i++ {
			item, err := r.Get()
			require.NoError(t, err)
			items[item]++
		}
		if _, ok := items[item]; ok {
			t.Fatalf("Item stil exists: %s", item)
		}
		if len(items) != r.Length() {
			t.Fatalf("Expected item count: %d. Got: %d", r.Length(), len(items))
		}
	})
}

func TestRoundRobin_Delete_NonExistent(t *testing.T) {
	items := []string{"127.0.0.1:2323", "127.0.0.1:4556", "127.0.0.1:7889"}
	r := New(items)

	var fresh []string
	fresh = append(fresh, items...)
	for i, item := range fresh {
		if i+1 == len(items) {
			r.Delete(item)
		} else {
			r.Delete(item)
		}
	}
}
