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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConfig_DMap(t *testing.T) {
	d := &DMap{
		MaxInuse:   -1,
		MaxKeys:    -1,
		LRUSamples: -1,
	}
	require.NoError(t, d.Sanitize())
	require.NoError(t, d.Validate())

	require.Greater(t, d.MaxInuse, -1)
	require.Greater(t, d.MaxKeys, -1)
	require.Equal(t, DefaultLRUSamples, d.LRUSamples)
	require.Equal(t, EvictionPolicy("NONE"), d.EvictionPolicy)
	require.NotNil(t, d.Engine)
}
