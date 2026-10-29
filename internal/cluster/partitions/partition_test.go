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

package partitions

import (
	"sync"
	"testing"

	"github.com/olric-data/olric/internal/discovery"
	"github.com/olric-data/olric/pkg/storage"
	"github.com/stretchr/testify/require"
)

type testFragment struct {
	length int
	busy   bool
}

func (tf *testFragment) Stats() storage.Stats {
	return storage.Stats{Length: tf.length}
}

func (tf *testFragment) TryStats() (storage.Stats, bool) {
	if tf.busy {
		return storage.Stats{}, false
	}
	return tf.Stats(), true
}

func (tf *testFragment) Name() string {
	return "test-data-structure"
}

func (tf *testFragment) Move(_ *Partition, _ string, _ []discovery.Member) error {
	return nil
}

func (tf *testFragment) Close() error {
	return nil
}

func (tf *testFragment) Destroy() error {
	return nil
}

func (tf *testFragment) Compaction() (bool, error) {
	return false, nil
}

func TestPartition(t *testing.T) {
	p := Partition{
		id:   1,
		kind: PRIMARY,
		m:    &sync.Map{},
	}

	tmp := []discovery.Member{{
		Name: "test-member",
	}}
	p.SetOwners(tmp)

	t.Run("Owners", func(t *testing.T) {
		owners := p.Owners()
		require.Equal(t, tmp, owners, "Partition owners slice is different")
	})

	t.Run("Owner", func(t *testing.T) {
		owner := p.Owner()
		require.Equal(t, tmp[0], owner, "Partition owners slice is different")
	})

	t.Run("OwnerCount", func(t *testing.T) {
		count := p.OwnerCount()
		require.Equal(t, 1, count)
	})

	t.Run("Length", func(t *testing.T) {
		s1 := &testFragment{length: 10}
		s2 := &testFragment{length: 20}
		p.Map().Store("s1", s1)
		p.Map().Store("s2", s2)
		length := p.Length()
		require.Equal(t, 30, length)
	})
}

func TestPartition_TryLength(t *testing.T) {
	tests := []struct {
		name      string
		fragments []testFragment
		length    int
		available bool
	}{
		{name: "no fragments", available: true},
		{name: "empty fragments", fragments: []testFragment{{}, {}}, available: true},
		{name: "multiple fragments", fragments: []testFragment{{length: 10}, {length: 20}}, length: 30, available: true},
		{name: "busy empty fragment", fragments: []testFragment{{busy: true}}},
		{name: "busy populated fragment", fragments: []testFragment{{length: 10, busy: true}, {length: 20}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(1, PRIMARY).PartitionByID(0)
			for i := range tt.fragments {
				p.Map().Store(i, &tt.fragments[i])
			}
			length, available := p.TryLength()
			require.Equal(t, tt.available, available)
			if available {
				require.Equal(t, tt.length, length)
			}
		})
	}
}
