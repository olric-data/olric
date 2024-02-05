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

package entry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEntryEncodeDecode(t *testing.T) {
	e := New()
	e.SetKey("mykey")
	e.SetTTL(200)
	e.SetTimestamp(time.Now().UnixNano())
	e.SetLastAccess(time.Now().UnixNano())
	e.SetValue([]byte("mydata"))

	t.Run("Encode", func(t *testing.T) {
		buf := e.Encode()
		require.NotNilf(t, buf, "Expected some data. Got nil")

		t.Run("Decode", func(t *testing.T) {
			item := New()
			item.Decode(buf)
			require.Equalf(t, e, item, "Decoded Entry is different")
		})
	})
}
