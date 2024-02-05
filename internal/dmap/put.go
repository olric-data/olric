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
	"fmt"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/resp"
	"github.com/buraksezer/olric/internal/stats"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/redis/go-redis/v9"
)

var pool = bufpool.New()

// EntriesTotal is the total number of entries(including replicas)
// stored during the life of this instance.
var EntriesTotal = stats.NewInt64Counter()

var (
	ErrKeyFound      = errors.New("key found")
	ErrWriteQuorum   = errors.New("write quorum cannot be reached")
	ErrKeyTooLarge   = errors.New("key too large")
	ErrEntryTooLarge = errors.New("entry too large for the configured table size")
)

func prepareTTL(e *env) int64 {
	var ttl int64
	switch {
	case e.putConfig.HasEX:
		ttl = (e.putConfig.EX.Nanoseconds() + time.Now().UnixNano()) / 1000000
	case e.putConfig.HasPX:
		ttl = (e.putConfig.PX.Nanoseconds() + time.Now().UnixNano()) / 1000000
	case e.putConfig.HasEXAT:
		ttl = e.putConfig.EXAT.Nanoseconds() / 1000000
	case e.putConfig.HasPXAT:
		ttl = e.putConfig.PXAT.Nanoseconds() / 1000000
	default:
		ns := e.timeout.Nanoseconds()
		if ns != 0 {
			ttl = (ns + time.Now().UnixNano()) / 1000000
		}
	}
	return ttl
}

// putOnFragment calls underlying storage engine's Put method to store the key/value pair. It's not thread-safe.
func (dm *DMap) putEntryOnFragment(e *env, nt storage.Entry) error {
	if e.putConfig.OnlyUpdateTTL {
		err := e.fragment.storage.UpdateTTL(e.hkey, nt)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				err = ErrKeyNotFound
			}
			return err
		}
		return nil
	}
	err := e.fragment.storage.Put(e.hkey, nt)
	if errors.Is(err, storage.ErrKeyTooLarge) {
		err = ErrKeyTooLarge
	}
	if errors.Is(err, storage.ErrEntryTooLarge) {
		err = ErrEntryTooLarge
	}
	if err != nil {
		return err
	}

	// total number of entries stored during the life of this instance.
	EntriesTotal.Increase(1)

	return nil
}

func (dm *DMap) prepareEntry(e *env) storage.Entry {
	nt := e.fragment.storage.NewEntry()
	nt.SetKey(e.key)
	nt.SetValue(e.value)
	nt.SetTTL(prepareTTL(e))
	nt.SetTimestamp(e.timestamp)
	return nt
}

func (dm *DMap) putOnReplicaFragment(e *env) error {
	part := dm.getPartitionByHKey(e.hkey, partitions.BACKUP)
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	e.fragment = f
	f.Lock()
	defer f.Unlock()

	err = f.storage.PutRaw(e.hkey, e.value)
	if errors.Is(err, storage.ErrKeyTooLarge) {
		err = ErrKeyTooLarge
	}
	if errors.Is(err, storage.ErrEntryTooLarge) {
		err = ErrEntryTooLarge
	}
	if err != nil {
		return err
	}

	// total number of entries stored during the life of this instance.
	EntriesTotal.Increase(1)

	return nil
}

func (dm *DMap) asyncPutOnBackup(e *env, data []byte, owner discovery.Member) {
	defer dm.s.wg.Done()

	rc := dm.s.client.Get(owner.String())
	cmd := protocol.NewPutEntry(e.dmap, e.key, data).Command(dm.s.ctx)
	err := rc.Process(dm.s.ctx, cmd)
	if err != nil {
		if dm.s.log.V(3).Ok() {
			dm.s.log.V(3).Printf("[ERROR] Failed to create replica in async mode: %v", err)
		}
		return
	}
	err = cmd.Err()
	if err != nil {
		if dm.s.log.V(3).Ok() {
			dm.s.log.V(3).Printf("[ERROR] Failed to create replica in async mode: %v", err)
		}
	}
}

func (dm *DMap) asyncPutOnCluster(e *env, nt storage.Entry) error {
	err := dm.putEntryOnFragment(e, nt)
	if err != nil {
		return err
	}

	encodedEntry := nt.Encode()
	// Fire and forget mode.
	owners := dm.s.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		if !dm.s.isAlive() {
			return ErrServerGone
		}

		dm.s.wg.Add(1)
		go dm.asyncPutOnBackup(e, encodedEntry, owner)
	}

	return nil
}

func (dm *DMap) syncPutOnCluster(e *env, nt storage.Entry) error {
	// Quorum based replication.
	var successful int

	encodedEntry := nt.Encode()

	owners := dm.s.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		rc := dm.s.client.Get(owner.String())
		cmd := protocol.NewPutEntry(dm.name, e.key, encodedEntry).Command(dm.s.ctx)
		err := rc.Process(dm.s.ctx, cmd)
		if err != nil {
			return protocol.ConvertError(err)
		}
		err = protocol.ConvertError(cmd.Err())
		if err != nil {
			if dm.s.log.V(3).Ok() {
				dm.s.log.V(3).Printf("[ERROR] Failed to call put command on %s for DMap: %s: %v", owner, e.dmap, err)
			}
			continue
		}
		successful++
	}
	err := dm.putEntryOnFragment(e, nt)
	if err != nil {
		if dm.s.log.V(3).Ok() {
			dm.s.log.V(3).Printf("[ERROR] Failed to call put command on %s for DMap: %s: %v", dm.s.rt.This(), e.dmap, err)
		}
	} else {
		successful++
	}
	if successful >= dm.s.config.WriteQuorum {
		return nil
	}
	return ErrWriteQuorum
}

func (dm *DMap) setLRUEvictionStats(e *env) error {
	// Try to make room for the new item, if it's required.
	// MaxKeys and MaxInuse properties of LRU can be used in the same time.
	// But I think that it's good to use only one of time in a production system.
	// Because it should be easy to understand and debug.
	st := e.fragment.storage.Stats()

	// This works for every request if you enabled LRU.
	// But loading a number from memory should be very cheap.
	// ownedPartitionCount changes in the case of node join or leave.
	ownedPartitionCount := dm.s.rt.OwnedPartitionCount()
	if ownedPartitionCount == 0 {
		// Routing table is an eventually consistent data structure. In order to prevent a panic in prod,
		// check the owned partition count before doing math.
		return nil
	}

	if dm.config.maxKeys > 0 {
		// MaxKeys controls maximum key count owned by this node.
		// We need ownedPartitionCount property because every partition
		// manages itself independently. So if you set MaxKeys=70 and
		// your partition count is 7, every partition 10 keys at maximum.
		if st.Length > 0 && st.Length >= dm.config.maxKeys/int(ownedPartitionCount) {
			err := dm.evictKeyWithLRU(e)
			if err != nil {
				return err
			}
		}
	}

	if dm.config.maxInuse > 0 {
		// MaxInuse controls maximum in-use memory of partitions on this node.
		// We need ownedPartitionCount property because every partition
		// manages itself independently. So if you set MaxInuse=70M(in bytes) and
		// your partition count is 7, every partition consumes 10M in-use space at maximum.
		// WARNING: Actual allocated memory can be different.
		if st.Inuse > 0 && st.Inuse >= dm.config.maxInuse/int(ownedPartitionCount) {
			err := dm.evictKeyWithLRU(e)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (dm *DMap) checkPutConditions(e *env) error {
	// Only set the key if it does not already exist.
	if e.putConfig.HasNX {
		ttl, err := e.fragment.storage.GetTTL(e.hkey)
		if err == nil {
			if !isKeyExpired(ttl) {
				return ErrKeyFound
			}
		}
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = nil
		}
		if err != nil {
			return err
		}
	}

	// Only set the key if it already exists.
	if e.putConfig.HasXX && !e.fragment.storage.Check(e.hkey) {
		ttl, err := e.fragment.storage.GetTTL(e.hkey)
		if err == nil {
			if isKeyExpired(ttl) {
				return ErrKeyNotFound
			}
		}
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = ErrKeyNotFound
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (dm *DMap) putOnCluster(e *env) error {
	part := dm.getPartitionByHKey(e.hkey, partitions.PRIMARY)
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	e.fragment = f
	f.Lock()
	defer f.Unlock()

	if err = dm.checkPutConditions(e); err != nil {
		return err
	}

	if dm.config != nil {
		if dm.config.ttlDuration.Seconds() != 0 && e.timeout.Seconds() == 0 {
			e.timeout = dm.config.ttlDuration
		}
		if dm.config.evictionPolicy == config.LRUEviction {
			if err = dm.setLRUEvictionStats(e); err != nil {
				return err
			}
		}
	}

	nt := dm.prepareEntry(e)
	if dm.s.config.ReplicaCount > config.MinimumReplicaCount {
		switch dm.s.config.ReplicationMode {
		case config.AsyncReplicationMode:
			// Fire and forget mode. Calls PutBackup command in different goroutines
			// and stores the key/value pair on local storage instance.
			return dm.asyncPutOnCluster(e, nt)
		case config.SyncReplicationMode:
			// Quorum based replication.
			return dm.syncPutOnCluster(e, nt)
		default:
			return fmt.Errorf("invalid replication mode: %v", dm.s.config.ReplicationMode)
		}
	}

	// single replica
	return dm.putEntryOnFragment(e, nt)
}

func (dm *DMap) writePutCommand(e *env) (*redis.StatusCmd, error) {
	cmd := protocol.NewPut(e.dmap, e.key, e.value)
	switch {
	case e.putConfig.HasEX:
		cmd.SetEX(e.putConfig.EX.Seconds())
	case e.putConfig.HasPX:
		cmd.SetPX(e.putConfig.PX.Milliseconds())
	case e.putConfig.HasEXAT:
		cmd.SetEXAT(e.putConfig.EXAT.Seconds())
	case e.putConfig.HasPXAT:
		cmd.SetPXAT(e.putConfig.PXAT.Milliseconds())
	}

	switch {
	case e.putConfig.HasNX:
		cmd.SetNX()
	case e.putConfig.HasXX:
		cmd.SetXX()
	}

	return cmd.Command(dm.s.ctx), nil
}

// put controls every write operation in Olric. It redirects the requests to its owner,
// if the key belongs to another host.
func (dm *DMap) put(e *env) error {
	e.hkey = partitions.HKey(e.dmap, e.key)
	member := dm.s.primary.PartitionByHKey(e.hkey).Owner()
	if member.CompareByName(dm.s.rt.This()) {
		// We are on the partition owner.
		return dm.putOnCluster(e)
	}

	// Redirect to the partition owner.
	cmd, err := dm.writePutCommand(e)
	if err != nil {
		return err
	}
	rc := dm.s.client.Get(member.String())
	err = rc.Process(e.ctx, cmd)
	if err != nil {
		return protocol.ConvertError(err)
	}
	return protocol.ConvertError(cmd.Err())
}

type PutConfig struct {
	HasEX         bool
	EX            time.Duration
	HasPX         bool
	PX            time.Duration
	HasEXAT       bool
	EXAT          time.Duration
	HasPXAT       bool
	PXAT          time.Duration
	HasNX         bool
	HasXX         bool
	OnlyUpdateTTL bool
}

// Put sets the value for the given key. It overwrites any previous value
// for that key, and it's thread-safe. The key has to be a string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
func (dm *DMap) Put(ctx context.Context, key string, value interface{}, cfg *PutConfig) error {
	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := resp.New(valueBuf)
	err := enc.Encode(value)
	if err != nil {
		return err
	}

	if cfg == nil {
		cfg = &PutConfig{}
	}
	e := newEnv(ctx)
	e.putConfig = cfg
	e.dmap = dm.name
	e.key = key
	e.value = make([]byte, valueBuf.Len())
	copy(e.value[:], valueBuf.Bytes())
	return dm.put(e)
}
