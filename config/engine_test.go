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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEngine_Dont_Overwrite_TableSize(t *testing.T) {
	e := NewEngine()
	e.Name = DefaultStorageEngine
	e.Config = map[string]interface{}{
		"tableSize": 1235,
	}

	require.NoError(t, e.Sanitize())
	require.NoError(t, e.Validate())
	require.Equal(t, 1235, e.Config["tableSize"])
}
