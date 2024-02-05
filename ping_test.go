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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOlric_Ping(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	result, err := db.ping(context.Background(), db.rt.This().String(), "")
	require.NoError(t, err)
	require.Equal(t, []byte(DefaultPingResponse), result)
}

func TestOlric_PingWithMessage(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	msg := "Olric rocks!"
	response, err := db.ping(context.Background(), db.rt.This().String(), msg)
	require.NoError(t, err)
	require.Equal(t, []byte(msg), response)
}
