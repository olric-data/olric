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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
)

func TestRoutingTable_distributedBackups(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	rt1, err := cluster.addNode(c1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt1.IsBootstrapped() {
		t.Fatalf("The coordinator node cannot be bootstrapped")
	}

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	rt2, err := cluster.addNode(c2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = rt1.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	c3 := testutil.NewConfig()
	c3.ReplicaCount = 2
	rt3, err := cluster.addNode(c3)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	rt2.UpdateEagerly()

	for partID := uint64(0); partID < c3.PartitionCount; partID++ {
		part := rt3.backup.PartitionByID(partID)
		if part.OwnerCount() != 1 {
			t.Fatalf("Expected backup owners count: 1. Got: %d", part.OwnerCount())
		}

		for _, owner := range part.Owners() {
			if owner.CompareByID(rt1.This()) {
				t.Fatalf("Dead node still a replica owner: %v", rt1.This())
			}
		}
	}

	err = cluster.shutdown()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}
