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
	"fmt"
	"time"

	"github.com/buraksezer/olric/config"
)

// dmapConfig keeps DMap config control parameters and access-log for keys in a dmap.
type dmapConfig struct {
	engine          *config.Engine
	maxIdleDuration time.Duration
	ttlDuration     time.Duration
	maxKeys         int
	maxInuse        int
	lruSamples      int
	evictionPolicy  config.EvictionPolicy
}

func (c *dmapConfig) load(dc *config.DMaps, name string) error {
	// Try to set config configuration for this dmap.
	c.maxIdleDuration = dc.MaxIdleDuration
	c.ttlDuration = dc.TTLDuration
	c.maxKeys = dc.MaxKeys
	c.maxInuse = dc.MaxInuse
	c.lruSamples = dc.LRUSamples
	c.evictionPolicy = dc.EvictionPolicy
	c.engine = dc.Engine

	if dc.Custom != nil {
		// config.DMap struct can be used for fine-grained control.
		cs, ok := dc.Custom[name]
		if ok {
			if c.maxIdleDuration != cs.MaxIdleDuration {
				c.maxIdleDuration = cs.MaxIdleDuration
			}
			if c.ttlDuration != cs.TTLDuration {
				c.ttlDuration = cs.TTLDuration
			}
			if c.evictionPolicy != cs.EvictionPolicy {
				c.evictionPolicy = cs.EvictionPolicy
			}
			if c.maxKeys != cs.MaxKeys {
				c.maxKeys = cs.MaxKeys
			}
			if c.maxInuse != cs.MaxInuse {
				c.maxInuse = cs.MaxInuse
			}
			if c.lruSamples != cs.LRUSamples {
				c.lruSamples = cs.LRUSamples
			}
			if c.evictionPolicy != cs.EvictionPolicy {
				c.evictionPolicy = cs.EvictionPolicy
			}
			if c.engine == nil {
				c.engine = cs.Engine
			}
		}
	}

	//TODO: Create a new function to verify config.
	if c.evictionPolicy == config.LRUEviction {
		if c.maxInuse <= 0 && c.maxKeys <= 0 {
			return fmt.Errorf("maxInuse or maxKeys have to be greater than zero")
		}
		// set the default value.
		if c.lruSamples == 0 {
			c.lruSamples = config.DefaultLRUSamples
		}
	}
	return nil
}
