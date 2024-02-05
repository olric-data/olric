// Copyright 2018-2022 Burak Sezer
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

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/resp"
	"github.com/buraksezer/olric/internal/util"
	"github.com/buraksezer/olric/pkg/storage"
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
	if err != nil {
		return 0, err
	}

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
