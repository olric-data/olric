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
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/protocol"
	"github.com/olric-data/olric/internal/resp"
	"github.com/olric-data/olric/internal/util"
	"github.com/olric-data/olric/pkg/storage"
)

func (dm *DMap) loadCurrentAtomicInt(e *env) (int, int64, error) {
	entry, err := dm.Get(e.ctx, e.key)
	if errors.Is(err, ErrKeyNotFound) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}

	if entry == nil {
		return 0, 0, nil
	}
	nr, err := util.ParseInt(entry.Value(), 10, 64)
	if err != nil {
		return 0, 0, nil
	}
	return int(nr), entry.TTL(), nil
}

func (dm *DMap) atomicIncrDecr(cmd string, e *env, delta int) (int, error) {
	atomicKey := e.dmap + e.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", e.key, e.dmap, err)
		}
	}()

	current, ttl, err := dm.loadCurrentAtomicInt(e)
	if err != nil {
		return 0, err
	}

	var updated int
	switch cmd {
	case protocol.DMap.Incr:
		updated = current + delta
	case protocol.DMap.Decr:
		updated = current - delta
	default:
		return 0, fmt.Errorf("invalid operation")
	}

	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := resp.New(valueBuf)
	err = enc.Encode(updated)
	if err != nil {
		return 0, err
	}
	e.value = make([]byte, valueBuf.Len())
	copy(e.value, valueBuf.Bytes())

	if ttl != 0 {
		e.putConfig.HasPX = true
		e.putConfig.PX = time.Until(time.UnixMilli(ttl))
	}
	err = dm.put(e)
	if err != nil {
		return 0, err
	}

	return updated, nil
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *DMap) Incr(ctx context.Context, key string, delta int) (int, error) {
	e := newEnv(ctx)
	e.dmap = dm.name
	e.key = key
	return dm.atomicIncrDecr(protocol.DMap.Incr, e, delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (dm *DMap) Decr(ctx context.Context, key string, delta int) (int, error) {
	e := newEnv(ctx)
	e.dmap = dm.name
	e.key = key
	return dm.atomicIncrDecr(protocol.DMap.Decr, e, delta)
}

func (dm *DMap) getPut(e *env) (storage.Entry, error) {
	atomicKey := e.dmap + e.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the lock for key: %s on DMap: %s: %v", e.key, e.dmap, err)
		}
	}()

	entry, err := dm.Get(e.ctx, e.key)
	if errors.Is(err, ErrKeyNotFound) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	err = dm.put(e)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		// The value is nil.
		return nil, nil
	}
	return entry, nil
}

// GetPut atomically sets key to value and returns the old value stored at key.
func (dm *DMap) GetPut(ctx context.Context, key string, value interface{}) (storage.Entry, error) {
	if value == nil {
		value = struct{}{}
	}

	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := resp.New(valueBuf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}

	e := newEnv(ctx)
	e.dmap = dm.name
	e.key = key
	e.value = make([]byte, valueBuf.Len())
	copy(e.value, valueBuf.Bytes())

	raw, err := dm.getPut(e)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	return raw, nil
}

func (dm *DMap) atomicIncrByFloat(e *env, delta float64) (float64, error) {
	atomicKey := e.dmap + e.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", e.key, e.dmap, err)
		}
	}()

	var current float64
	entry, err := dm.Get(e.ctx, e.key)
	if errors.Is(err, ErrKeyNotFound) {
		err = nil
	}
	if err != nil {
		return 0, err
	}

	if entry != nil {
		current, err = util.ParseFloat(entry.Value(), 64)
		if err != nil {
			return 0, err
		}
	}

	latest := current + delta

	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := resp.New(valueBuf)
	err = enc.Encode(latest)
	if err != nil {
		return 0, err
	}
	e.value = valueBuf.Bytes()
	e.value = make([]byte, valueBuf.Len())
	copy(e.value, valueBuf.Bytes())

	err = dm.put(e)
	if err != nil {
		return 0, err
	}

	return latest, nil
}

// IncrByFloat atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *DMap) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	e := newEnv(ctx)
	e.dmap = dm.name
	e.key = key
	return dm.atomicIncrByFloat(e, delta)
}

// compareAndSwap performs the CAS on the partition owner: it takes the fine-
// grained atomic-key lock, reads the current entry, compares its raw value
// bytes against expected, and conditionally writes. Callers must ensure this
// method only runs on the owner of hkey(e.dmap, e.key); the public
// CompareAndSwap method handles routing.
//
// A nil expected means "expect the key to be absent". A non-nil expected means
// "expect the key to exist with exactly these raw value bytes".
//
// Returns (swapped, currentEntry, err):
//   - swapped=true, current=nil: the swap succeeded, new value stored.
//   - swapped=false, current=<entry>: mismatch, current contains what was there.
//   - swapped=false, current=nil: mismatch, key was absent when we expected bytes.
func (dm *DMap) compareAndSwap(e *env, expected []byte) (bool, storage.Entry, error) {
	atomicKey := e.dmap + e.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		if err := dm.s.locker.Unlock(atomicKey); err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the atomic-key lock for key: %s on DMap: %s: %v", e.key, e.dmap, err)
		}
	}()

	current, err := dm.Get(e.ctx, e.key)
	if errors.Is(err, ErrKeyNotFound) {
		current = nil
		err = nil
	}
	if err != nil {
		return false, nil, err
	}

	// Evaluate the compare step.
	exists := current != nil
	wantExists := expected != nil

	if exists != wantExists {
		// Mismatch on presence: either we expected absent and it exists, or we
		// expected a value and it's gone.
		return false, current, nil
	}
	if exists {
		if !bytes.Equal(current.Value(), expected) {
			return false, current, nil
		}
	}

	// Matched; write the new value. dm.put handles replication.
	if err := dm.put(e); err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

// CompareAndSwap atomically replaces the value stored at key with newValue iff
// the raw value bytes currently stored for key equal expected. A nil/empty
// expected means "compare against key non-existence".
//
// On a successful swap it returns (true, nil, nil). On a mismatch it returns
// (false, currentEntry, nil), where currentEntry is nil if the key did not
// exist at the moment of comparison. This lets callers retry without an extra
// Get.
//
// The TTL semantics mirror Put: pass EX/PX/EXAT/PXAT via options to control
// expiry of the newly-written value.
func (dm *DMap) CompareAndSwap(
	ctx context.Context,
	key string,
	expected []byte,
	newValue interface{},
	cfg *PutConfig,
) (bool, storage.Entry, error) {
	if newValue == nil {
		newValue = struct{}{}
	}

	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := resp.New(valueBuf)
	if err := enc.Encode(newValue); err != nil {
		return false, nil, err
	}

	encoded := make([]byte, valueBuf.Len())
	copy(encoded, valueBuf.Bytes())

	if cfg == nil {
		cfg = &PutConfig{}
	}

	hkey := partitions.HKey(dm.name, key)
	member := dm.s.primary.PartitionByHKey(hkey).Owner()
	if member.CompareByName(dm.s.rt.This()) {
		// Local (owner) path.
		e := newEnv(ctx)
		e.dmap = dm.name
		e.key = key
		e.value = encoded
		e.putConfig = cfg
		return dm.compareAndSwap(e, expected)
	}

	// Remote path: dispatch DM.CAS to the owner so the atomic-key lock runs
	// there. This makes CompareAndSwap cluster-wide serializing for the key,
	// stricter than GetPut/Incr which only lock locally.
	casCmd := protocol.NewCompareAndSwap(dm.name, key, expected, encoded)
	switch {
	case cfg.HasEX:
		casCmd.SetEX(cfg.EX.Seconds())
	case cfg.HasPX:
		casCmd.SetPX(cfg.PX.Milliseconds())
	case cfg.HasEXAT:
		casCmd.SetEXAT(cfg.EXAT.Seconds())
	case cfg.HasPXAT:
		casCmd.SetPXAT(cfg.PXAT.Milliseconds())
	}
	cmd := casCmd.Command(ctx)
	rc := dm.s.client.Get(member.String())
	if err := rc.Process(ctx, cmd); err != nil {
		return false, nil, protocol.ConvertError(err)
	}
	if err := cmd.Err(); err != nil {
		return false, nil, protocol.ConvertError(err)
	}

	items, err := cmd.Slice()
	if err != nil {
		return false, nil, protocol.ConvertError(err)
	}
	return decodeCompareAndSwapReply(items, dm.engine.NewEntry)
}

// decodeCompareAndSwapReply parses the two-element reply from DM.CAS.
// Layout: [swappedInt, currentEncodedEntryBytesOrNil].
func decodeCompareAndSwapReply(items []interface{}, newEntry func() storage.Entry) (bool, storage.Entry, error) {
	if len(items) != 2 {
		return false, nil, fmt.Errorf("unexpected CAS reply length %d", len(items))
	}
	swappedInt, ok := items[0].(int64)
	if !ok {
		return false, nil, fmt.Errorf("unexpected CAS reply type for swapped: %T", items[0])
	}
	swapped := swappedInt == 1
	if items[1] == nil {
		return swapped, nil, nil
	}
	raw, ok := items[1].(string)
	if !ok {
		return false, nil, fmt.Errorf("unexpected CAS reply type for current: %T", items[1])
	}
	entry := newEntry()
	entry.Decode([]byte(raw))
	return swapped, entry, nil
}
