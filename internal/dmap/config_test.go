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
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDMap_Config(t *testing.T) {
	c := config.New("local")
	// Config for all new DMaps
	c.DMaps.NumEvictionWorkers = 1
	c.DMaps.TTLDuration = 100 * time.Second
	c.DMaps.MaxKeys = 100000
	c.DMaps.MaxInuse = 1000000
	c.DMaps.LRUSamples = 10
	c.DMaps.EvictionPolicy = config.LRUEviction
	c.DMaps.Engine = testutil.NewEngineConfig(t)

	// Config for specified DMaps
	c.DMaps.Custom = map[string]config.DMap{"foobar": {
		MaxIdleDuration: 60 * time.Second,
		TTLDuration:     300 * time.Second,
		MaxKeys:         500000,
		LRUSamples:      20,
		EvictionPolicy:  "NONE",
		Engine: &config.Engine{
			Name: "kvstore",
			Config: map[string]interface{}{
				"maxIdleTableTimeout": 15 * time.Minute,
				"tableSize":           uint64(1048576),
			},
		},
	}}

	dc := dmapConfig{}
	err := dc.load(c.DMaps, "mydmap")
	require.NoError(t, err)
	require.Equal(t, c.DMaps.TTLDuration, dc.ttlDuration)
	require.Equal(t, c.DMaps.MaxKeys, dc.maxKeys)
	require.Equal(t, c.DMaps.MaxInuse, dc.maxInuse)
	require.Equal(t, c.DMaps.LRUSamples, dc.lruSamples)
	require.Equal(t, c.DMaps.EvictionPolicy, dc.evictionPolicy)
	require.Equal(t, c.DMaps.Engine.Name, dc.engine.Name)

	t.Run("Custom config", func(t *testing.T) {
		dcc := dmapConfig{}
		err := dcc.load(c.DMaps, "foobar")
		require.NoError(t, err)
		require.Equal(t, c.DMaps.Custom["foobar"].TTLDuration, dcc.ttlDuration)
		require.Equal(t, c.DMaps.Custom["foobar"].MaxKeys, dcc.maxKeys)
		require.Equal(t, c.DMaps.Custom["foobar"].MaxInuse, dcc.maxInuse)
		require.Equal(t, c.DMaps.Custom["foobar"].LRUSamples, dcc.lruSamples)
		require.Equal(t, c.DMaps.Custom["foobar"].EvictionPolicy, dcc.evictionPolicy)

		c.DMaps.Custom["foobar"].Engine.Implementation = nil
		dcc.engine.Implementation = nil

		require.Equal(t, c.DMaps.Custom["foobar"].Engine, dcc.engine)
	})
}
