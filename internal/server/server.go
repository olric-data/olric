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
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/checkpoint"
	"github.com/buraksezer/olric/internal/stats"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/tidwall/redcon"
)

var (
	// CommandsTotal is total number of all requests broken down by command (get, put, etc.) and status.
	CommandsTotal = stats.NewInt64Counter()

	// ConnectionsTotal is total number of connections opened since the server started running.
	ConnectionsTotal = stats.NewInt64Counter()

	// CurrentConnections is current number of open connections.
	CurrentConnections = stats.NewInt64Gauge()

	// WrittenBytesTotal is total number of bytes sent by this server to network.
	WrittenBytesTotal = stats.NewInt64Counter()

	// ReadBytesTotal is total number of bytes read by this server from network.
	ReadBytesTotal = stats.NewInt64Counter()
)

// Config is a composite type to bundle configuration parameters.
type Config struct {
	BindAddr        string
	BindPort        int
	KeepAlivePeriod time.Duration
	IdleClose       time.Duration
	RequireAuth     bool
}

type ConnContext struct {
	mtx           sync.RWMutex
	authenticated bool
}

func NewConnContext() *ConnContext {
	return &ConnContext{}
}

func (c *ConnContext) SetAuthenticated(authenticated bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.authenticated = authenticated
}

func (c *ConnContext) IsAuthenticated() bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.authenticated
}

type ConnWrapper struct {
	net.Conn
}

func (cw *ConnWrapper) Write(b []byte) (n int, err error) {
	nr, err := cw.Conn.Write(b)
	if err != nil {
		return 0, err
	}

	WrittenBytesTotal.Increase(int64(nr))
	return nr, nil
}

func (cw *ConnWrapper) Read(b []byte) (n int, err error) {
	nr, err := cw.Conn.Read(b)
	if err != nil {
		return 0, err
	}

	ReadBytesTotal.Increase(int64(nr))
	return nr, nil
}

type ListenerWrapper struct {
	net.Listener
	keepAlivePeriod time.Duration
}

func (lw *ListenerWrapper) Accept() (net.Conn, error) {
	conn, err := lw.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if lw.keepAlivePeriod != 0 {
			if keepAliveErr := tcpConn.SetKeepAlive(true); keepAliveErr != nil {
				return nil, keepAliveErr
			}
			if keepAliveErr := tcpConn.SetKeepAlivePeriod(lw.keepAlivePeriod); keepAliveErr != nil {
				return nil, keepAliveErr
			}
		}
	}
	return &ConnWrapper{conn}, nil
}

type Server struct {
	config     *Config
	mux        *ServeMux
	wmux       *ServeMuxWrapper
	server     *redcon.Server
	log        *flog.Logger
	listener   *ListenerWrapper
	StartedCtx context.Context
	started    context.CancelFunc
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	// some components of the TCP server should be closed after the listener
	stopped chan struct{}
}

// New creates and returns a new Server.
func New(c *Config, l *flog.Logger) *Server {
	// The server has to be started properly before accepting connections.
	checkpoint.Add()

	ctx, cancel := context.WithCancel(context.Background())
	startedCtx, started := context.WithCancel(context.Background())
	s := &Server{
		config:     c,
		mux:        NewServeMux(c),
		log:        l,
		started:    started,
		StartedCtx: startedCtx,
		stopped:    make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}
	s.wmux = &ServeMuxWrapper{mux: s.mux}
	return s
}

func (s *Server) SetPreConditionFunc(f func(conn redcon.Conn, cmd redcon.Command) bool) {
	select {
	case <-s.StartedCtx.Done():
		// It's already started.
		return
	default:
	}
	s.wmux.precond = f
}

func (s *Server) ServeMux() *ServeMuxWrapper {
	return s.wmux
}

// ListenAndServe listens on the TCP network address addr.
func (s *Server) ListenAndServe() error {
	addr := net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	lw := &ListenerWrapper{
		Listener:        listener,
		keepAlivePeriod: s.config.KeepAlivePeriod,
	}

	defer close(s.stopped)
	s.listener = lw

	srv := redcon.NewServer(addr,
		s.mux.ServeRESP,
		func(conn redcon.Conn) bool {
			conn.SetContext(NewConnContext())
			ConnectionsTotal.Increase(1)
			CurrentConnections.Increase(1)
			return true
		},
		func(conn redcon.Conn, err error) {
			CurrentConnections.Increase(-1)
		},
	)

	if s.config.IdleClose != 0 {
		srv.SetIdleClose(s.config.IdleClose)
	}
	s.server = srv

	// The TCP server has been started
	s.started()
	checkpoint.Pass()
	return s.server.Serve(lw)
}

// Shutdown gracefully shuts down the server without interrupting any active connections.
// Shutdown works by first closing all open listeners, then closing all idle connections,
// and then waiting indefinitely for connections to return to idle and then shut down.
// If the provided context expires before the shutdown is complete, Shutdown returns
// the context's error, otherwise it returns any error returned from closing the Server's
// underlying Listener(s).
func (s *Server) Shutdown(ctx context.Context) error {
	select {
	case <-s.ctx.Done():
		// It's already closed.
		return nil
	default:
	}

	s.cancel()

	if s.server == nil {
		// There is nothing to close.
		return nil
	}

	var latestError error
	err := s.server.Close()
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to close listener: %v", err)
		latestError = err
	}

	// Listener is closed successfully. Now we can await for closing
	// other components of the TCP server.
	<-s.stopped

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
		if err != nil {
			s.log.V(2).Printf("[ERROR] Context has an error: %v", err)
			latestError = err
		}
	case <-done:
	}

	return latestError
}
