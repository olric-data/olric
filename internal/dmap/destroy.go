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

package dmap

import (
	"context"
	"runtime"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func (dm *DMap) destroyOnCluster(ctx context.Context) error {
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)

	var g errgroup.Group

	// Don't block routing table to destroy a DMap on the cluster.
	// Just get a copy of members and run Destroy.
	var members []discovery.Member
	m := dm.s.rt.Members()
	m.RLock()
	m.Range(func(_ uint64, member discovery.Member) bool {
		members = append(members, member)
		return true
	})
	m.RUnlock()

	for _, item := range members {
		addr := item.String()
		g.Go(func() error {
			if err := sem.Acquire(dm.s.ctx, 1); err != nil {
				dm.s.log.V(3).
					Printf("[ERROR] Failed to acquire semaphore to call Destroy command on %s for %s: %v",
						addr, dm.name, err)
				return err
			}
			defer sem.Release(1)

			dm.s.log.V(6).Printf("[DEBUG] Calling DM.DESTROY command on %s for %s", addr, dm.name)
			cmd := protocol.NewDestroy(dm.name).SetLocal().Command(dm.s.ctx)
			rc := dm.s.client.Get(addr)
			err := rc.Process(ctx, cmd)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] DM.DESTROY returned an error: %v", err)
				return err
			}
			return cmd.Err()
		})
	}
	return g.Wait()
}

// Destroy flushes the given DMap on the cluster. You should know that there
// is no global lock on DMaps. So if you call Put, Put with EX and Destroy methods
// concurrently on the cluster, Put and Put with EX calls may set new values to the DMap.
func (dm *DMap) Destroy(ctx context.Context) error {
	return dm.destroyOnCluster(ctx)
}
