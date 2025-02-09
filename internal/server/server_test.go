// Copyright 2018-2025 Burak Sezer
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
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/pkg/flog"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// getFreePort copied from testutil package to prevent cycle import.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

func newServerWithPreConditionFunc(t *testing.T, precond func(conn redcon.Conn, cmd redcon.Command) bool) *Server {
	bindPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	l := log.New(os.Stdout, "server-test: ", log.LstdFlags)
	fl := flog.New(l)
	fl.SetLevel(6)
	fl.ShowLineNumber(1)
	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        bindPort,
		KeepAlivePeriod: time.Second,
	}
	s := New(c, fl)
	s.SetPreConditionFunc(precond)

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()

	t.Cleanup(func() {
		err = s.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	return s
}

func newServer(t *testing.T) *Server {
	srv := newServerWithPreConditionFunc(t, nil)
	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	})
	return srv
}

func defaultRedisOptions(c *Config) *redis.Options {
	return &redis.Options{
		Addr: net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort)),
	}
}

func TestServer_RESP(t *testing.T) {
	s := newServer(t)

	respEcho(t, s)
}

func TestServer_RESP_Stats(t *testing.T) {
	s := newServer(t)

	respEcho(t, s)

	require.NotEqual(t, int64(0), CommandsTotal.Read())
	require.NotEqual(t, int64(0), ConnectionsTotal.Read())
	require.NotEqual(t, int64(0), CurrentConnections.Read())
	require.NotEqual(t, int64(0), WrittenBytesTotal.Read())
	require.NotEqual(t, int64(0), ReadBytesTotal.Read())
}

func TestConnContext_Authentication(t *testing.T) {
	ctx := NewConnContext()
	require.False(t, ctx.IsAuthenticated())

	t.Run("Authenticated", func(t *testing.T) {
		ctx.SetAuthenticated(true)
		require.True(t, ctx.IsAuthenticated())
	})
}
