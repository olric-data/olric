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

package olric

import (
	"context"
	"fmt"
	"testing"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedClient_ScanMatch(t *testing.T) {
	cl := newTestOlricCluster(t)
	db := cl.addMember(t)
	cl.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()

	evenKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		var key string
		if i%2 == 0 {
			key = fmt.Sprintf("even:%s", testutil.ToKey(i))
			evenKeys[key] = false
		} else {
			key = fmt.Sprintf("odd:%s", testutil.ToKey(i))
		}
		err = dm.Put(ctx, key, i)
		require.NoError(t, err)
	}
	i, err := dm.Scan(ctx, Match("^even:"))
	require.NoError(t, err)
	var count int
	defer i.Close()

	for i.Next() {
		count++
		require.Contains(t, evenKeys, i.Key())
	}
	require.Equal(t, 50, count)
}

func TestEmbeddedClient_Scan(t *testing.T) {
	cl := newTestOlricCluster(t)
	db := cl.addMember(t)
	cl.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	allKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), i)
		require.NoError(t, err)
		allKeys[testutil.ToKey(i)] = false
	}
	i, err := dm.Scan(ctx)
	require.NoError(t, err)
	var count int
	defer i.Close()

	for i.Next() {
		count++
		require.Contains(t, allKeys, i.Key())
	}
	require.Equal(t, 100, count)
}
