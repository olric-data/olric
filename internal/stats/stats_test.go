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

package stats

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUint64Counter(t *testing.T) {
	c := NewInt64Counter()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Increase(1)
		}()
	}

	wg.Wait()

	require.Equal(t, int64(100), c.Read())
}

func TestUint64Gauge(t *testing.T) {
	g := NewInt64Gauge()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Increase(1)
		}()
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Decrease(1)
		}()
	}

	wg.Wait()

	require.Equal(t, int64(80), g.Read())
}
