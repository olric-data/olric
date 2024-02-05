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
	"errors"
	"fmt"
	"sync"
)

// ErrEmptyInstance denotes that there is nothing in the round-robin instance to schedule.
var ErrEmptyInstance = errors.New("empty round-robin instance")

// RoundRobin implements quite simple round-robin scheduling algorithm to distribute load fairly between servers.
type RoundRobin struct {
	// Mutual exclusion lock is required here because the Get method
	// is called concurrently by the client component, and it modifies the state
	// in every call.
	mtx     sync.RWMutex
	current int
	items   []string
}

// New returns a new RoundRobin instance.
func New(items []string) *RoundRobin {
	return &RoundRobin{
		current: 0,
		items:   items,
	}
}

// Get returns an item.
func (r *RoundRobin) Get() (string, error) {
	// Acquire the lock here. This function modifies the internal state.
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if len(r.items) == 0 {
		return "", ErrEmptyInstance
	}

	if r.current >= len(r.items) {
		r.current %= len(r.items)
	}

	if r.current >= len(r.items) {
		return "", fmt.Errorf("round-robin: corrupted internal state")
	}

	item := r.items[r.current]
	r.current++
	return item, nil
}

// Add adds a new item to the Round-Robin scheduler.
func (r *RoundRobin) Add(item string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.items = append(r.items, item)
}

// Delete deletes an item from the Round-Robin scheduler.
func (r *RoundRobin) Delete(item string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	for i := 0; i < len(r.items); i++ {
		if r.items[i] == item {
			r.items = append(r.items[:i], r.items[i+1:]...)
			i--
		}
	}
}

// Length returns the count of items
func (r *RoundRobin) Length() int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	return len(r.items)
}
