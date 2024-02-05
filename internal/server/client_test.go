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
	"net"
	"strconv"
	"testing"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestServer_Client_Get(t *testing.T) {
	srv := newServer(t)
	srv.ServeMux().HandleFunc(protocol.Generic.Ping, func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteBulkString("pong")
	})

	<-srv.StartedCtx.Done()

	addr := net.JoinHostPort(srv.config.BindAddr, strconv.Itoa(srv.config.BindPort))
	c := config.NewClient()
	require.NoError(t, c.Sanitize())

	cs := NewClient(c)
	rc := cs.Get(addr)

	ctx := context.Background()
	cmd := protocol.NewPing().Command(ctx)
	err := rc.Process(ctx, cmd)
	require.NoError(t, err)

	result, err := cmd.Result()
	require.NoError(t, err)
	require.Equal(t, "pong", result)

	t.Run("Fetch cached client", func(t *testing.T) {
		newClient := cs.Get(addr)
		require.Equal(t, rc, newClient)
	})
}

func TestServer_Client_Pick(t *testing.T) {
	servers := make(map[string]*Server)
	for i := 0; i < 10; i++ {
		srv := newServer(t)
		srv.ServeMux().HandleFunc(protocol.Generic.Ping, func(conn redcon.Conn, cmd redcon.Command) {
			conn.WriteBulkString("pong")
		})
		addr := net.JoinHostPort(srv.config.BindAddr, strconv.Itoa(srv.config.BindPort))
		servers[addr] = srv
	}

	c := config.NewClient()
	require.NoError(t, c.Sanitize())

	cs := NewClient(c)

	for addr, srv := range servers {
		<-srv.StartedCtx.Done()
		cs.Get(addr)
	}
	// All the servers have been started.

	clients := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		rc, err := cs.Pick()
		require.NoError(t, err)

		ctx := context.Background()
		cmd := protocol.NewPing().Command(ctx)
		err = rc.Process(ctx, cmd)
		require.NoError(t, err)

		result, err := cmd.Result()
		require.NoError(t, err)
		require.Equal(t, "pong", result)
		clients[rc.String()] = struct{}{}
	}
	require.Greater(t, len(clients), 1)
}

func TestServer_Client_Close(t *testing.T) {
	srv := newServer(t)

	<-srv.StartedCtx.Done()

	c := config.NewClient()
	require.NoError(t, c.Sanitize())

	addr := net.JoinHostPort(srv.config.BindAddr, strconv.Itoa(srv.config.BindPort))
	cs := NewClient(c)
	rc1 := cs.Get(addr)

	require.NoError(t, cs.Close(addr))
	rc2 := cs.Get(addr)
	require.NotEqual(t, rc1, rc2)
	require.Len(t, cs.clients, 1)
	require.Equal(t, 1, cs.roundRobin.Length())
}

func TestServer_Client_Shutdown(t *testing.T) {
	servers := make(map[string]*Server)
	for i := 0; i < 10; i++ {
		srv := newServer(t)
		srv.ServeMux().HandleFunc(protocol.Generic.Ping, func(conn redcon.Conn, cmd redcon.Command) {
			conn.WriteBulkString("pong")
		})
		addr := net.JoinHostPort(srv.config.BindAddr, strconv.Itoa(srv.config.BindPort))
		servers[addr] = srv
	}

	c := config.NewClient()
	require.NoError(t, c.Sanitize())

	cs := NewClient(c)

	for addr, srv := range servers {
		<-srv.StartedCtx.Done()
		cs.Get(addr)
	}
	// All the servers have been started.
	err := cs.Shutdown(context.Background())
	require.NoError(t, err)
	require.Empty(t, cs.clients)
	require.Equal(t, 0, cs.roundRobin.Length())
}
