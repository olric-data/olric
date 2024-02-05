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

package server

import (
	"context"
	"math/rand"
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestMux_PubSub_Command(t *testing.T) {
	s := newServer(t)

	data := make([]byte, 8)
	_, err := rand.Read(data)
	require.NoError(t, err)

	s.ServeMux().HandleFunc(protocol.PubSub.PubSubNumpat, func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteInt(10)
	})

	<-s.StartedCtx.Done()

	rdb := redis.NewClient(defaultRedisOptions(s.config))

	ctx := context.Background()
	var args []interface{}
	args = append(args, "pubsub")
	args = append(args, "numpat")
	cmd := redis.NewIntCmd(ctx, args...)
	err = rdb.Process(ctx, cmd)
	require.NoError(t, err)

	num, err := cmd.Result()
	require.NoError(t, err)
	require.Equal(t, int64(10), num)
}
