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
	"runtime"
	"time"
)

// DMaps denotes a global configuration for DMaps. You can still overwrite it by
// setting a DMap for a particular distributed map via Custom field. Most of the
// fields are related with distributed cache implementation.
type DMaps struct {
	// Engine contains configuration for a storage engine implementation. It may contain the implementation.
	// See Engine itself.
	Engine *Engine

	// NumEvictionWorkers denotes the number of goroutines that are used to find
	// keys for eviction. This is a global configuration variable. So you cannot set
	//	// different values per DMap.
	NumEvictionWorkers int64

	// MaxIdleDuration denotes maximum time for each entry to stay idle in the DMap.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period exceeds
	// this limit are expired and evicted automatically. An entry is idle if no Get,
	// Put, Expire on it. Configuration of MaxIdleDuration feature varies by preferred
	// deployment method.
	MaxIdleDuration time.Duration

	// TTLDuration is useful to set a default TTL for every key/value pair a
	// distributed map instance.
	TTLDuration time.Duration

	// MaxKeys denotes maximum key count on a particular node. So if you have 10
	// nodes with MaxKeys=100000, your key count in the cluster should be around
	// MaxKeys*10=1000000
	MaxKeys int

	// MaxInuse denotes maximum amount of in-use memory on a particular node.
	// So if you have 10 nodes with MaxInuse=100M (it has to be in bytes), amount
	// of in-use memory should be around MaxInuse*10=1G
	MaxInuse int

	// LRUSamples denotes amount of randomly selected key count by the approximate
	// LRU implementation. Lower values are better for high performance. It's
	// 5 by default.
	LRUSamples int

	// EvictionPolicy determines the eviction policy in use. It's NONE by default.
	// Set as LRU to enable LRU eviction policy.
	EvictionPolicy EvictionPolicy

	// CheckEmptyFragmentsInterval is the interval between two sequential calls of empty
	// fragment cleaner. This is a global configuration variable. So you cannot set
	// different values per DMap.
	CheckEmptyFragmentsInterval time.Duration

	// TriggerCompactionInterval is interval between two sequential call of compaction worker.
	// This is a global configuration variable. So you cannot set
	// different values per DMap.
	TriggerCompactionInterval time.Duration

	// Custom is useful to set custom cache config per DMap instance.
	Custom map[string]DMap
}

// Sanitize sets default values to empty configuration variables, if it's possible.
func (dm *DMaps) Sanitize() error {
	if dm.Engine == nil {
		dm.Engine = NewEngine()
	}

	if dm.Custom == nil {
		dm.Custom = make(map[string]DMap)
	}

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

	if dm.NumEvictionWorkers <= 0 {
		dm.NumEvictionWorkers = int64(runtime.NumCPU())
	}

	if dm.CheckEmptyFragmentsInterval.Microseconds() == 0 {
		dm.CheckEmptyFragmentsInterval = DefaultCheckEmptyFragmentsInterval
	}

	if dm.TriggerCompactionInterval.Microseconds() == 0 {
		dm.TriggerCompactionInterval = DefaultTriggerCompactionInterval
	}

	for _, d := range dm.Custom {
		if err := d.Sanitize(); err != nil {
			return err
		}
	}

	if err := dm.Engine.Sanitize(); err != nil {
		return fmt.Errorf("failed to sanitize storage engine configuration: %w", err)
	}

	return nil
}

func (dm *DMaps) Validate() error {
	if err := dm.Engine.Validate(); err != nil {
		return fmt.Errorf("failed to validate storage engine configuration: %w", err)
	}
	return nil
}

var _ IConfig = (*DMaps)(nil)
