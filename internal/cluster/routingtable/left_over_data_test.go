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
	"errors"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/testutil/mockfragment"
	"github.com/stretchr/testify/require"
)

func TestRoutingTable_LeftOverData(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c1 := testutil.NewConfig()
	rt1, err := cluster.addNode(c1)
	require.NoError(t, err)

	if !rt1.IsBootstrapped() {
		t.Fatalf("The coordinator node cannot be bootstrapped")
	}

	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		part := rt1.primary.PartitionByID(partID)
		ts := mockfragment.New()
		ts.Fill()
		part.Map().Store("test-data", ts)
	}

	c2 := testutil.NewConfig()
	rt2, err := cluster.addNode(c2)
	require.NoError(t, err)

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	for partID := uint64(0); partID < c2.PartitionCount; partID++ {
		part := rt2.primary.PartitionByID(partID)
		ts := mockfragment.New()
		ts.Fill()
		part.Map().Store("test-data", ts)
	}

	rt1.UpdateEagerly()

	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		part := rt1.primary.PartitionByID(partID)
		if len(part.Owners()) != 2 {
			t.Fatalf("Expected partition owners count: 2. Got: %d, PartID: %d", part.OwnerCount(), partID)
		}
	}

}
