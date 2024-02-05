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

/*Package server provides a standalone server implementation for Olric*/
package server

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
	"golang.org/x/sync/errgroup"
)

// Olricd represents a new Olricd instance.
type Olricd struct {
	log    *log.Logger
	config *config.Config
	db     *olric.Olric
	errGr  errgroup.Group
}

// New creates a new Server instance
func New(c *config.Config) (*Olricd, error) {
	db, err := olric.New(c)
	if err != nil {
		return nil, err
	}
	return &Olricd{
		config: c,
		log:    c.Logger,
		db:     db,
	}, nil
}

func (s *Olricd) waitForInterrupt() {
	shutDownChan := make(chan os.Signal, 1)
	signal.Notify(shutDownChan, syscall.SIGTERM, syscall.SIGINT)
	ch := <-shutDownChan
	s.log.Printf("[INFO] Signal catched: %s", ch.String())

	// Awaits for shutdown
	s.errGr.Go(func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := s.db.Shutdown(ctx); err != nil {
			s.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
			return err
		}

		return nil
	})

	// This is not a goroutine leak. The process will quit.
	go func() {
		s.log.Printf("[INFO] Awaiting for background tasks")
		s.log.Printf("[INFO] Press CTRL+C or send SIGTERM/SIGINT to quit immediately")

		forceQuitCh := make(chan os.Signal, 1)
		signal.Notify(forceQuitCh, syscall.SIGTERM, syscall.SIGINT)
		ch := <-forceQuitCh

		s.log.Printf("[INFO] Signal caught: %s", ch.String())
		s.log.Printf("[INFO] Quits with exit code 1")
		os.Exit(1)
	}()
}

// Start starts a new olricd server instance and blocks until the server is closed.
func (s *Olricd) Start() error {
	s.log.Printf("[INFO] pid: %d has been started", os.Getpid())
	// Wait for SIGTERM or SIGINT
	go s.waitForInterrupt()

	s.errGr.Go(func() error {
		return s.db.Start()
	})

	return s.errGr.Wait()
}

// Shutdown stops background servers and leaves the cluster.
func (s *Olricd) Shutdown(ctx context.Context) error {
	return s.db.Shutdown(ctx)
}
