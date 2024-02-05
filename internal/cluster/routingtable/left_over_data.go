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
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
)

func (r *RoutingTable) processLeftOverDataReports(reports map[discovery.Member]*leftOverDataReport) {
	check := func(member discovery.Member, owners []discovery.Member) bool {
		for _, owner := range owners {
			if member.CompareByID(owner) {
				return true
			}
		}
		return false
	}

	ensureOwnership := func(member discovery.Member, partID uint64, part *partitions.Partition) {
		owners := part.Owners()
		if check(member, owners) {
			return
		}
		// This section is protected by routingMtx against parallel writers.
		//
		// Copy owners and append the member to head
		newOwners := make([]discovery.Member, len(owners))
		copy(newOwners, owners)
		// Prepend
		newOwners = append([]discovery.Member{member}, newOwners...)
		part.SetOwners(newOwners)
		r.log.V(2).Printf("[INFO] %s still have some data for PartID (kind: %s): %d", member, part.Kind(), partID)
	}

	// data structures in this function is guarded by routingMtx
	for member, report := range reports {
		for _, partID := range report.Partitions {
			part := r.primary.PartitionByID(partID)
			ensureOwnership(member, partID, part)
		}

		for _, partID := range report.Backups {
			part := r.backup.PartitionByID(partID)
			ensureOwnership(member, partID, part)
		}
	}
}
