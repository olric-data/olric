// Copyright 2018-2025 The Olric Authors
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
	"strings"
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

func TestEntry_Encode_EmptyKey(t *testing.T) {
	e := New()
	e.SetKey("")
	e.SetTTL(100)
	e.SetTimestamp(time.Now().UnixNano())
	e.SetLastAccess(time.Now().UnixNano())
	e.SetValue([]byte("somevalue"))

	buf := e.Encode()
	require.NotNil(t, buf)

	decoded := New()
	decoded.Decode(buf)
	require.Equal(t, "", decoded.Key())
	require.Equal(t, []byte("somevalue"), decoded.Value())
	require.Equal(t, e.TTL(), decoded.TTL())
	require.Equal(t, e.Timestamp(), decoded.Timestamp())
	require.Equal(t, e.LastAccess(), decoded.LastAccess())
}

func TestEntry_Encode_EmptyValue(t *testing.T) {
	e := New()
	e.SetKey("mykey")
	e.SetTTL(50)
	e.SetTimestamp(time.Now().UnixNano())
	e.SetLastAccess(time.Now().UnixNano())
	// value is nil (zero value)

	buf := e.Encode()
	require.NotNil(t, buf)

	decoded := New()
	decoded.Decode(buf)
	require.Equal(t, "mykey", decoded.Key())
	require.Empty(t, decoded.Value())
	require.Equal(t, e.TTL(), decoded.TTL())
	require.Equal(t, e.Timestamp(), decoded.Timestamp())
	require.Equal(t, e.LastAccess(), decoded.LastAccess())
}

func TestEntry_Encode_MaxLengthKey(t *testing.T) {
	// 255 bytes is the maximum key length that fits in a uint8.
	maxKey := strings.Repeat("k", 255)

	e := New()
	e.SetKey(maxKey)
	e.SetTTL(999)
	e.SetTimestamp(time.Now().UnixNano())
	e.SetLastAccess(time.Now().UnixNano())
	e.SetValue([]byte("val"))

	buf := e.Encode()
	require.NotNil(t, buf)

	decoded := New()
	decoded.Decode(buf)
	require.Equal(t, maxKey, decoded.Key())
	require.Equal(t, []byte("val"), decoded.Value())
	require.Equal(t, e.TTL(), decoded.TTL())
}

func TestEntry_Encode_KeyLengthOverflow(t *testing.T) {
	// 256-byte key overflows uint8. The cast `uint8(256)` becomes 0,
	// so Encode writes the key length as 0 but copies the full 256 bytes
	// into the buffer. Decode then reads corrupted metadata offsets,
	// which causes a panic due to slice bounds out of range.
	overflowKey := strings.Repeat("x", 256)

	e := New()
	e.SetKey(overflowKey)
	e.SetTTL(1)
	e.SetTimestamp(time.Now().UnixNano())
	e.SetLastAccess(time.Now().UnixNano())
	e.SetValue([]byte("v"))

	buf := e.Encode()
	require.NotNil(t, buf)

	// Decode panics because the uint8-truncated key length (0) shifts all
	// field offsets, causing the decoded value length to be a garbage large
	// number that exceeds the buffer capacity.
	require.Panics(t, func() {
		decoded := New()
		decoded.Decode(buf)
	}, "Expected panic due to uint8 overflow of key length causing corrupted offsets")
}
