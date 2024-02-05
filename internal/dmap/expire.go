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
	"time"
)

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contain the key. It's thread-safe.
func (dm *DMap) Expire(ctx context.Context, key string, timeout time.Duration) error {
	pc := &PutConfig{
		OnlyUpdateTTL: true,
	}
	e := newEnv(ctx)
	e.putConfig = pc
	e.dmap = dm.name
	e.key = key
	e.timeout = timeout
	return dm.put(e)
}
