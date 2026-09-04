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

package dmap

import (
	"context"
	"errors"

	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/discovery"
	"github.com/olric-data/olric/internal/protocol"
	"github.com/olric-data/olric/internal/stats"
	"golang.org/x/sync/errgroup"
)

var (
	// DeleteHits is the number of deletion requests resulting in an item being removed.
	DeleteHits = stats.NewInt64Counter()

	// DeleteMisses is the number of deletion requests for missing keys.
	DeleteMisses = stats.NewInt64Counter()
)

func (dm *DMap) deleteFromFragment(key string, kind partitions.Kind) error {
	hkey := partitions.HKey(dm.name, key)
	part := dm.getPartitionByHKey(hkey, kind)
	f, err := dm.loadFragment(part)
	if errors.Is(err, errFragmentNotFound) {
		// key doesn't exist
		return nil
	}
	if err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()

	return f.storage.Delete(hkey)
}

func (dm *DMap) deleteFromPreviousOwners(key string, owners []discovery.Member) error {
	// Traverse in reverse order. Except from the latest host, this one.
	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		cmd := protocol.NewDelEntry(dm.name, key).Command(dm.s.ctx)
		rc := dm.s.client.Get(owner.String())
		err := rc.Process(dm.s.ctx, cmd)
		if err != nil {
			return protocol.ConvertError(err)
		}
		err = cmd.Err()
		if err != nil {
			return protocol.ConvertError(err)
		}
	}
	return nil
}

func (dm *DMap) deleteBackupOnCluster(hkey uint64, key string) error {
	owners := dm.s.backup.PartitionOwnersByHKey(hkey)
	var g errgroup.Group
	for _, owner := range owners {
		mem := owner
		g.Go(func() error {
			cmd := protocol.NewDelEntry(dm.name, key).SetReplica().Command(dm.s.ctx)
			rc := dm.s.client.Get(mem.String())
			err := rc.Process(dm.s.ctx, cmd)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to delete replica key/value on %s: %s", dm.name, err)
				return protocol.ConvertError(err)
			}
			return protocol.ConvertError(cmd.Err())
		})
	}
	return g.Wait()
}

// deleteOnCluster is not a thread-safe function

// deleteExpiredKey removes a key that the eviction scanner has already decided
// is dead. Unlike deleteOnCluster it must NOT be called with the fragment lock
// held: deleteFromPreviousOwners and deleteBackupOnCluster talk to other nodes,
// and an owner that departed during a rolling update stalls those for a full
// read timeout per key.
//
// Because the lock is not held throughout, the caller's expiry decision can be
// stale by the time we get here. A rate limiter refreshes the same keys
// constantly, so the entry is re-checked under the lock and kept if a writer
// revived it. Reports whether the key was actually removed.
func (dm *DMap) deleteExpiredKey(hkey uint64, key string, f *fragment) (bool, error) {
	owners := dm.s.primary.PartitionOwnersByHKey(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	if err := dm.deleteFromPreviousOwners(key, owners); err != nil {
		return false, err
	}

	if dm.s.config.ReplicaCount != 0 {
		if err := dm.deleteBackupOnCluster(hkey, key); err != nil {
			return false, err
		}
	}

	f.Lock()
	defer f.Unlock()

	ttl, err := f.storage.GetTTL(hkey)
	if err != nil {
		// Already gone. Nothing left to remove.
		return false, nil
	}
	if !isKeyExpired(ttl) && !dm.isKeyIdleOnFragment(hkey, f) {
		// Revived while we were on the network. Deleting now would drop a live
		// entry, which for a rate limiter means silently losing a counter.
		return false, nil
	}

	if err := f.storage.Delete(hkey); err != nil {
		return false, err
	}

	DeleteHits.Increase(1)

	return true, nil
}

func (dm *DMap) deleteOnCluster(hkey uint64, key string, f *fragment) error {
	owners := dm.s.primary.PartitionOwnersByHKey(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	err := dm.deleteFromPreviousOwners(key, owners)
	if err != nil {
		return err
	}

	if dm.s.config.ReplicaCount != 0 {
		err := dm.deleteBackupOnCluster(hkey, key)
		if err != nil {
			return err
		}
	}

	err = f.storage.Delete(hkey)
	if err != nil {
		return err
	}

	// DeleteHits is the number of deletion reqs resulting in an item being removed.
	DeleteHits.Increase(1)

	return nil
}

func (dm *DMap) deleteKey(key string) error {
	hkey := partitions.HKey(dm.name, key)
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()

	// Check the HKey before trying to delete it.
	if !f.storage.Check(hkey) {
		// DeleteMisses is the number of deletions reqs for missing keys
		DeleteMisses.Increase(1)
		return nil
	}

	return dm.deleteOnCluster(hkey, key, f)
}

func (dm *DMap) deleteKeys(ctx context.Context, keys ...string) (int, error) {
	members := make(map[discovery.Member][]string)
	for _, key := range keys {
		hkey := partitions.HKey(dm.name, key)
		member := dm.s.primary.PartitionByHKey(hkey).Owner()
		members[member] = append(members[member], key)
	}

	for member, distributedKeys := range members {
		if member.CompareByName(dm.s.rt.This()) {
			for _, key := range distributedKeys {
				if err := dm.deleteKey(key); err != nil {
					return 0, err
				}
			}
		} else {
			cmd := protocol.NewDel(dm.name, distributedKeys...).Command(dm.s.ctx)
			rc := dm.s.client.Get(member.String())
			err := rc.Process(ctx, cmd)
			if err != nil {
				return 0, protocol.ConvertError(err)
			}

			return 0, protocol.ConvertError(cmd.Err())
		}
	}

	return len(keys), nil
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.
// It is safe to modify the contents of the argument after Delete returns.
func (dm *DMap) Delete(ctx context.Context, keys ...string) (int, error) {
	return dm.deleteKeys(ctx, keys...)
}
