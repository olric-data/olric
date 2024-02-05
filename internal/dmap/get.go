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
	"errors"
	"sort"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/stats"
	"github.com/buraksezer/olric/pkg/storage"
)

// Entry is a DMap entry with its metadata.
type Entry struct {
	Key       string
	Value     interface{}
	TTL       int64
	Timestamp int64
}

var (
	// GetMisses is the number of entries that have been requested and not found
	GetMisses = stats.NewInt64Counter()

	// GetHits is the number of entries that have been requested and found present
	GetHits = stats.NewInt64Counter()

	// EvictedTotal is the number of entries removed from cache to free memory for new entries.
	EvictedTotal = stats.NewInt64Counter()
)

// ErrReadQuorum means that read quorum cannot be reached to operate.
var ErrReadQuorum = errors.New("read quorum cannot be reached")

type version struct {
	host  *discovery.Member
	entry storage.Entry
}

func (dm *DMap) getOnFragment(e *env) (storage.Entry, error) {
	part := dm.getPartitionByHKey(e.hkey, e.kind)
	f, err := dm.loadFragment(part)
	if err != nil {
		return nil, err
	}

	f.RLock()
	defer f.RUnlock()

	entry, err := f.storage.Get(e.hkey)
	switch err {
	case storage.ErrKeyNotFound:
		err = ErrKeyNotFound
	case storage.ErrKeyTooLarge:
		err = ErrKeyTooLarge
	}
	if err != nil {
		return nil, err
	}

	if isKeyExpired(entry.TTL()) {
		return nil, ErrKeyNotFound
	}
	return entry, nil
}

func (dm *DMap) lookupOnPreviousOwner(owner *discovery.Member, key string) (*version, error) {
	cmd := protocol.NewGetEntry(dm.name, key).Command(dm.s.ctx)
	rc := dm.s.client.Get(owner.String())
	err := rc.Process(dm.s.ctx, cmd)
	if err != nil {
		return nil, protocol.ConvertError(err)
	}
	value, err := cmd.Bytes()
	if err != nil {
		return nil, protocol.ConvertError(err)
	}

	v := &version{host: owner}
	e := dm.engine.NewEntry()
	e.Decode(value)
	v.entry = e
	return v, nil
}

func (dm *DMap) valueToVersion(value storage.Entry) *version {
	this := dm.s.rt.This()
	return &version{
		host:  &this,
		entry: value,
	}
}

func (dm *DMap) lookupOnThisNode(hkey uint64, key string) *version {
	// Check on localhost, the partition owner.
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	if err != nil {
		if !errors.Is(err, errFragmentNotFound) {
			dm.s.log.V(3).Printf("[ERROR] Failed to get DMap fragment: %v", err)
		}
		return dm.valueToVersion(nil)
	}
	f.RLock()
	defer f.RUnlock()

	value, err := f.storage.Get(hkey)
	if err != nil {
		if !errors.Is(err, storage.ErrKeyNotFound) {
			// still need to use "ver". just log this error.
			dm.s.log.V(3).Printf("[ERROR] Failed to get key: %s on %s: %s", key, dm.name, err)
		}
		return dm.valueToVersion(nil)
	}
	// We found the key
	//
	// LRU and MaxIdleDuration eviction policies are only valid on
	// the partition owner. Normally, we shouldn't need to retrieve the keys
	// from the backup or the previous owners. When the fsck merge
	// a fragmented partition or recover keys from a backup, Olric
	// continue maintaining a reliable access log.
	return dm.valueToVersion(value)
}

// lookupOnOwners collects versions of a key/value pair on the partition owner
// by including previous partition owners.
func (dm *DMap) lookupOnOwners(hkey uint64, key string) []*version {
	owners := dm.s.primary.PartitionOwnersByHKey(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	var versions []*version
	versions = append(versions, dm.lookupOnThisNode(hkey, key))

	// Run a query on the previous owners.
	// Traverse in reverse order. Except from the latest host, this one.
	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		v, err := dm.lookupOnPreviousOwner(&owner, key)
		if err != nil {
			if dm.s.log.V(6).Ok() {
				dm.s.log.V(6).Printf("[ERROR] Failed to call get on a previous "+
					"primary owner: %s: %v", owner, err)
			}
			continue
		}
		// Ignore failed owners. The data on those hosts will be wiped out
		// by the balancer.
		versions = append(versions, v)
	}
	return versions
}

func (dm *DMap) sortVersions(versions []*version) []*version {
	sort.Slice(versions,
		func(i, j int) bool {
			return versions[i].entry.Timestamp() >= versions[j].entry.Timestamp()
		},
	)
	// Explicit is better than implicit.
	return versions
}

func (dm *DMap) sanitizeAndSortVersions(versions []*version) []*version {
	var sanitized []*version
	// We use versions slice for read-repair. Clear nil values first.
	for _, ver := range versions {
		if ver.entry != nil {
			sanitized = append(sanitized, ver)
		}
	}
	if len(sanitized) <= 1 {
		return sanitized
	}
	return dm.sortVersions(sanitized)
}

func (dm *DMap) lookupOnReplicas(hkey uint64, key string) []*version {
	// Check backup.
	backups := dm.s.backup.PartitionOwnersByHKey(hkey)
	versions := make([]*version, 0, len(backups))
	for _, replica := range backups {
		host := replica
		cmd := protocol.NewGetEntry(dm.name, key).SetReplica().Command(dm.s.ctx)
		rc := dm.s.client.Get(host.String())
		err := rc.Process(dm.s.ctx, cmd)
		err = protocol.ConvertError(err)
		if err != nil {
			if dm.s.log.V(6).Ok() {
				dm.s.log.V(6).Printf("[DEBUG] Failed to call get on"+
					" a replica owner: %s: %v", host, err)
			}
			continue
		}

		value, err := cmd.Bytes()
		err = protocol.ConvertError(err)
		if err != nil {
			if dm.s.log.V(6).Ok() {
				dm.s.log.V(6).Printf("[DEBUG] Failed to call get on"+
					" a replica owner: %s: %v", host, err)
			}
		}

		v := &version{host: &host}
		e := dm.engine.NewEntry()
		e.Decode(value)
		v.entry = e
		versions = append(versions, v)
	}
	return versions
}

func (dm *DMap) readRepair(winner *version, versions []*version) {
	for _, version := range versions {
		if version.entry != nil && winner.entry.Timestamp() == version.entry.Timestamp() {
			continue
		}

		// Sync
		tmp := *version.host
		if tmp.CompareByID(dm.s.rt.This()) {
			hkey := partitions.HKey(dm.name, winner.entry.Key())
			part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
			f, err := dm.loadOrCreateFragment(part)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to get or create the fragment for: %s on %s: %v",
					winner.entry.Key(), dm.name, err)
				return
			}

			f.Lock()
			e := newEnv(context.Background())
			e.hkey = hkey
			e.fragment = f
			err = dm.putEntryOnFragment(e, winner.entry)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to synchronize with replica: %v", err)
			}
			f.Unlock()
		} else {
			// If readRepair is enabled, this function is called by every GET request.
			cmd := protocol.NewPutEntry(dm.name, winner.entry.Key(), winner.entry.Encode()).Command(dm.s.ctx)
			rc := dm.s.client.Get(version.host.String())
			err := rc.Process(dm.s.ctx, cmd)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to synchronize replica %s: %v", version.host, err)
				continue
			}
			err = cmd.Err()
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to synchronize replica %s: %v", version.host, err)
			}
		}
	}
}

func (dm *DMap) getOnCluster(hkey uint64, key string) (storage.Entry, error) {
	// RUnlock should not be called with defer statement here because
	// readRepair function may call putOnFragment function which needs a write
	// lock. Please don't forget calling RUnlock before returning here.
	versions := dm.lookupOnOwners(hkey, key)
	if dm.s.config.ReadQuorum >= config.MinimumReplicaCount {
		v := dm.lookupOnReplicas(hkey, key)
		versions = append(versions, v...)
	}

	if len(versions) < dm.s.config.ReadQuorum {
		return nil, ErrReadQuorum
	}

	sorted := dm.sanitizeAndSortVersions(versions)
	if len(sorted) == 0 {
		// We checked everywhere, it's not here.
		return nil, ErrKeyNotFound
	}

	if len(sorted) < dm.s.config.ReadQuorum {
		return nil, ErrReadQuorum
	}

	// The most up-to-date version of the values.
	winner := sorted[0]
	if isKeyExpired(winner.entry.TTL()) || dm.isKeyIdle(hkey) {
		return nil, ErrKeyNotFound
	}

	if dm.s.config.ReadRepair {
		// Parallel read operations may propagate different versions of
		// the same key/value pair. The rule is simple: last write wins.
		dm.readRepair(winner, versions)
	}
	return winner.entry, nil
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
// does not contain the key. It's thread-safe. It is safe to modify the contents
// of the returned value.
func (dm *DMap) Get(ctx context.Context, key string) (storage.Entry, error) {
	hkey := partitions.HKey(dm.name, key)
	member := dm.s.primary.PartitionByHKey(hkey).Owner()
	// We are on the partition owner
	if member.CompareByName(dm.s.rt.This()) {
		entry, err := dm.getOnCluster(hkey, key)
		if errors.Is(err, ErrKeyNotFound) {
			GetMisses.Increase(1)
		}
		if err != nil {
			return nil, err
		}

		// number of keys that have been requested and found present
		GetHits.Increase(1)

		return entry, nil
	}

	// Redirect to the partition owner
	cmd := protocol.NewGet(dm.name, key).SetRaw().Command(dm.s.ctx)
	rc := dm.s.client.Get(member.String())
	err := rc.Process(ctx, cmd)
	if err != nil {
		return nil, protocol.ConvertError(err)
	}

	value, err := cmd.Bytes()
	if err != nil {
		return nil, protocol.ConvertError(err)
	}

	// number of keys that have been requested and found present
	GetHits.Increase(1)

	entry := dm.engine.NewEntry()
	entry.Decode(value)
	return entry, nil
}
