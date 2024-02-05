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
	"reflect"
	"testing"

	"github.com/buraksezer/olric/internal/discovery"
)

func TestPartitions(t *testing.T) {
	var partitionCount uint64 = 271
	ps := New(partitionCount, PRIMARY)

	t.Run("PartitionById", func(t *testing.T) {
		for partID := uint64(0); partID < partitionCount; partID++ {
			part := ps.PartitionByID(partID)
			if part.ID() != partID {
				t.Fatalf("Expected PartID: %d. Got: %d", partID, part.ID())
			}
			if part.Kind() != PRIMARY {
				t.Fatalf("Expected Kind: %s. Got: %s", PRIMARY, part.Kind())
			}
		}
	})

	t.Run("PartitionIdByHKey", func(t *testing.T) {
		// 1 % 271 = 1
		partID := ps.PartitionIDByHKey(1)
		if partID != 1 {
			t.Fatalf("Expected PartID: 1. Got: %d", partID)
		}
	})

	t.Run("PartitionByHKey", func(t *testing.T) {
		// 1 % 271 = 1
		part := ps.PartitionByHKey(1)
		if part.ID() != 1 {
			t.Fatalf("Expected PartID: 1. Got: %d", part.ID())
		}
	})

	t.Run("PartitionOwnersByHKey", func(t *testing.T) {
		part := ps.PartitionByHKey(1)
		tmp := []discovery.Member{{
			Name: "test-member",
		}}
		part.SetOwners(tmp)
		owners := ps.PartitionOwnersByHKey(1)
		if !reflect.DeepEqual(owners, tmp) {
			t.Fatalf("Partition owners slice is different")
		}
	})

	t.Run("PartitionOwnersById", func(t *testing.T) {
		part := ps.PartitionByID(1)
		tmp := []discovery.Member{{
			Name: "test-member",
		}}
		part.SetOwners(tmp)
		owners := ps.PartitionOwnersByID(1)
		if !reflect.DeepEqual(owners, tmp) {
			t.Fatalf("Partition owners slice is different")
		}
	})

	t.Run("Kind as string", func(t *testing.T) {
		// 1 % 271 = 1
		part := ps.PartitionByHKey(1)

		if part.Kind().String() != PRIMARY.String() {
			t.Fatalf("Expected partition kind: %s. Got: %d", PRIMARY, part.Kind())
		}
	})
}
