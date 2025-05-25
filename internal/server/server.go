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

package server

import (
	"context"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/olric-data/olric/internal/checkpoint"
	"github.com/olric-data/olric/internal/stats"
	"github.com/olric-data/olric/pkg/flog"
	"github.com/tidwall/redcon"
)

var (
	// CommandsTotal is the total number of all requests broken down by command (get, put, etc.) and status.
	CommandsTotal = stats.NewInt64Counter()

	// ConnectionsTotal is the total number of connections opened since the server started running.
	ConnectionsTotal = stats.NewInt64Counter()

	// CurrentConnections is the current number of open connections.
	CurrentConnections = stats.NewInt64Gauge()

	// WrittenBytesTotal is the total number of bytes sent by this server to network.
	WrittenBytesTotal = stats.NewInt64Counter()

	// ReadBytesTotal is the total number of bytes read by this server from network.
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

// ConnContext represents the context for a connection with authentication state management.
type ConnContext struct {
	mtx sync.RWMutex

	// authenticated indicates whether the connection is successfully authenticated.
	authenticated bool
}

// NewConnContext initializes and returns a new instance of ConnContext for managing connection states like authentication.
func NewConnContext() *ConnContext {
	return &ConnContext{}
}

// SetAuthenticated sets the authentication state of the connection to the specified value. It is thread-safe.
func (c *ConnContext) SetAuthenticated(authenticated bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.authenticated = authenticated
}

// IsAuthenticated checks if the connection is authenticated. It is thread-safe and returns true if authenticated.
func (c *ConnContext) IsAuthenticated() bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.authenticated
}

// ConnWrapper is a wrapper around net.Conn that enables tracking of read and written bytes.
type ConnWrapper struct {
	net.Conn
}

// Write sends data over the underlying connection and updates the total written bytes counter.
// It returns the number of bytes written and any error encountered.
func (cw *ConnWrapper) Write(b []byte) (n int, err error) {
	nr, err := cw.Conn.Write(b)
	if err != nil {
		return 0, err
	}

	WrittenBytesTotal.Increase(int64(nr))
	return nr, nil
}

// Read reads data into the provided byte slice, updates the read bytes counter, and returns the number of bytes read.
func (cw *ConnWrapper) Read(b []byte) (n int, err error) {
	nr, err := cw.Conn.Read(b)
	if err != nil {
		return 0, err
	}

	ReadBytesTotal.Increase(int64(nr))
	return nr, nil
}

// ListenerWrapper is a wrapper around net.Listener that supports setting a TCP keep-alive period for accepted connections.
type ListenerWrapper struct {
	net.Listener
	keepAlivePeriod time.Duration
}

// Accept waits for and returns the next connection to the ListenerWrapper, applying TCP keep-alive settings if specified.
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

// Server is a TCP server struct that manages configurations, logging, and connection handling for RESP-based protocols.
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

// New initializes and returns a new Server configured with the specified Config and Logger.
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

// SetPreConditionFunc sets a precondition function to be executed before serving each command on the server.
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

// ListenAndServe starts the TCP server, initializes internal components, and begins accepting connections.
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
// the context's error; otherwise it returns any error returned from closing the Server's
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
