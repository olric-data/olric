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

package environment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type envTest struct {
	number uint64
}

func TestEnvironment(t *testing.T) {
	e := New()

	st := &envTest{
		number: 1988,
	}

	e.Set("my-struct", st)
	e.Set("my-string", "value")
	e.Set("my-uint64", uint64(4576))

	structVal := e.Get("my-struct")
	require.Equal(t, uint64(1988), structVal.(*envTest).number)

	stringVal := e.Get("my-string")
	require.Equal(t, "value", stringVal)

	stringUint64 := e.Get("my-uint64")
	require.Equal(t, uint64(4576), stringUint64.(uint64))

	t.Run("Clone", func(t *testing.T) {
		clone := e.Clone()
		require.Equal(t, e, clone)
	})
}
