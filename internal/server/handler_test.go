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
	"crypto/rand"
	"sync/atomic"
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func respEcho(t *testing.T, s *Server) {
	data := make([]byte, 8)
	_, err := rand.Read(data)
	require.NoError(t, err)

	s.ServeMux().HandleFunc(protocol.DMap.Get, func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteBulk(data)
	})

	<-s.StartedCtx.Done()

	rdb := redis.NewClient(defaultRedisOptions(s.config))

	ctx := context.Background()
	cmd := protocol.NewGet("mydmap", "mykey").Command(ctx)
	err = rdb.Process(ctx, cmd)
	require.NoError(t, err)

	result, err := cmd.Bytes()
	require.NoError(t, err)
	require.Equal(t, data, result)
}

func TestHandler_ServeRESP_PreCondition(t *testing.T) {
	var precond int32
	s := newServerWithPreConditionFunc(t, func(conn redcon.Conn, cmd redcon.Command) bool {
		atomic.AddInt32(&precond, 1)
		return true
	})

	defer func() {
		require.NoError(t, s.Shutdown(context.Background()))
	}()

	respEcho(t, s)
	require.Equal(t, int32(1), atomic.LoadInt32(&precond))
}

func TestHandler_ServeRESP_PreCondition_DontCheck(t *testing.T) {
	var precond int32
	s := newServerWithPreConditionFunc(t, func(conn redcon.Conn, cmd redcon.Command) bool {
		atomic.AddInt32(&precond, 1)
		return true
	})

	defer func() {
		require.NoError(t, s.Shutdown(context.Background()))
	}()

	data := make([]byte, 8)
	_, err := rand.Read(data)
	require.NoError(t, err)

	// The node is bootstrapped by UpdateRoutingCmd. Don't check any preconditions to run that command.
	s.ServeMux().HandleFunc(protocol.Internal.UpdateRouting, func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteBulk(data)
	})

	<-s.StartedCtx.Done()

	rdb := redis.NewClient(defaultRedisOptions(s.config))

	ctx := context.Background()
	cmd := protocol.NewUpdateRouting([]byte("dummy-data"), 1).Command(ctx)
	err = rdb.Process(ctx, cmd)
	require.NoError(t, err)

	result, err := cmd.Bytes()
	require.NoError(t, err)
	require.Equal(t, data, result)

	require.Equal(t, int32(0), atomic.LoadInt32(&precond))
}
