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

package partitions

import (
	"testing"

	"github.com/buraksezer/olric/hasher"
	"github.com/stretchr/testify/require"
)

func TestPartitions_HKey(t *testing.T) {
	SetHashFunc(hasher.NewDefaultHasher())
	hkey := HKey("storage-unit-name", "some-key")
	require.NotEqualf(t, 0, hkey, "HKey is zero. This shouldn't be normal")
}
