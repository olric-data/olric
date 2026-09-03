// Copyright 2018-2025 The Olric Authors
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
	"testing"
	"time"

	"github.com/olric-data/olric/internal/discovery"
	"github.com/olric-data/olric/internal/testutil"
)

func TestUnreachableOwners(t *testing.T) {
	u := newUnreachableOwners()

	if u.has("127.0.0.1:1") {
		t.Fatal("empty set reported a member as unreachable")
	}

	u.add("127.0.0.1:1")
	if !u.has("127.0.0.1:1") {
		t.Fatal("added member was not reported as unreachable")
	}
	if u.has("127.0.0.1:2") {
		t.Fatal("unrelated member was reported as unreachable")
	}
}

// A probe to an owner that never answers must give up quickly, and every later
// partition must skip that owner outright. Serially probing PartitionCount
// partitions against one unresponsive owner is what stalls a routing table
// rebuild long enough for joining nodes to give up and crashloop.
func TestPartitionKeyCountBoundsAndMemoizes(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	rt, err := cluster.addNode(testutil.NewConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Port 1 is not listening, so the probe can only end in a timeout.
	dead := discovery.Member{Name: "127.0.0.1:1"}
	unreachable := newUnreachableOwners()

	start := time.Now()
	_, ok := rt.partitionKeyCount(0, dead, false, unreachable)
	firstCall := time.Since(start)

	if ok {
		t.Fatal("probe against a dead address reported a usable key count")
	}
	if firstCall > 5*time.Second {
		t.Fatalf("probe took %v, want it bounded near %v", firstCall, probeTimeout)
	}
	if !unreachable.has(dead.String()) {
		t.Fatal("failed probe did not mark the owner unreachable")
	}

	// Every remaining partition must now skip this owner rather than pay again.
	start = time.Now()
	if _, ok = rt.partitionKeyCount(1, dead, false, unreachable); ok {
		t.Fatal("memoized probe reported a usable key count")
	}
	if secondCall := time.Since(start); secondCall > 50*time.Millisecond {
		t.Fatalf("memoized probe took %v, want it to return immediately", secondCall)
	}
}
