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

package config

import (
	"fmt"
	"time"
)

// EvictionPolicy denotes eviction policy. Currently: LRU or NONE.
type EvictionPolicy string

// Important note on DMap and DMaps structs:
// Golang does not provide the typical notion of inheritance.
// because of that I preferred to define the types explicitly.

// DMap denotes configuration for a particular distributed map. Most of the
// fields are related with distributed cache implementation.
type DMap struct {
	// Engine contains storage engine configuration and their implementations.
	// If you don't have a custom storage engine implementation or configuration for
	// the default one, just leave it empty.
	Engine *Engine

	// MaxIdleDuration denotes maximum time for each entry to stay idle in the
	// DMap. It limits the lifetime of the entries relative to the time of the
	// last read or write access performed on them. The entries whose idle period
	// exceeds this limit are expired and evicted automatically. An entry is idle
	// if no Get, GetEntry, Put, Expire on it. Configuration
	// of MaxIdleDuration feature varies by preferred deployment method.
	MaxIdleDuration time.Duration

	// TTLDuration is useful to set a default TTL for every key/value pair a DMap
	// instance.
	TTLDuration time.Duration

	// MaxKeys denotes maximum key count on a particular node. So if you have 10
	// nodes with MaxKeys=100000, your key count in the cluster should be around
	// MaxKeys*10=1000000
	MaxKeys int

	// MaxInuse denotes maximum amount of in-use memory on a particular node. So
	// if you have 10 nodes with MaxInuse=100M (it has to be in bytes), amount of
	// in-use memory should be around MaxInuse*10=1G
	MaxInuse int

	// LRUSamples denotes amount of randomly selected key count by the approximate
	// LRU implementation. Lower values are better for high performance. It's 5
	// by default.
	LRUSamples int

	// EvictionPolicy determines the eviction policy in use. It's NONE by default.
	// Set as LRU to enable LRU eviction policy.
	EvictionPolicy EvictionPolicy
}

// Sanitize sets default values to empty configuration variables, if it's possible.
func (dm *DMap) Sanitize() error {
	if dm.EvictionPolicy == "" {
		dm.EvictionPolicy = "NONE"
	}
	if dm.LRUSamples <= 0 {
		dm.LRUSamples = DefaultLRUSamples
	}
	if dm.MaxInuse < 0 {
		dm.MaxInuse = 0
	}
	if dm.MaxKeys < 0 {
		dm.MaxKeys = 0
	}

	if dm.Engine == nil {
		dm.Engine = NewEngine()
	}

	if err := dm.Engine.Sanitize(); err != nil {
		return fmt.Errorf("failed to sanitize storage engine configuration: %w", err)
	}

	return nil
}

// Validate finds errors in the current configuration.
func (dm *DMap) Validate() error {
	if err := dm.Engine.Validate(); err != nil {
		return fmt.Errorf("failed to validate storage engine configuration: %w", err)
	}

	return nil
}

var _ IConfig = (*DMap)(nil)
