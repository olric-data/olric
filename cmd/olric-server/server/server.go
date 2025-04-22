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

/*Package server provides a standalone server implementation for Olric*/
package server

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/olric-data/olric"
	"github.com/olric-data/olric/config"
	"golang.org/x/sync/errgroup"
)

// OlricServer represents an instance of the Olric distributed in-memory data structure store.
// It encapsulates logging, configuration, the Olric database instance, and an error group for
// concurrency management.
type OlricServer struct {
	log    *log.Logger
	config *config.Config
	db     *olric.Olric
	errGr  errgroup.Group
}

// New initializes a new OlricServer instance using the provided configuration and returns it or an error.
func New(c *config.Config) (*OlricServer, error) {
	db, err := olric.New(c)
	if err != nil {
		return nil, err
	}
	return &OlricServer{
		config: c,
		log:    c.Logger,
		db:     db,
	}, nil
}

// waitForInterrupt waits for termination signals (SIGTERM, SIGINT) to gracefully shut down the Olric server instance.
func (s *OlricServer) waitForInterrupt() {
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

// Start launches the Olric server instance and begins listening for incoming requests and termination signals.
func (s *OlricServer) Start() error {
	s.log.Printf("[INFO] pid: %d has been started", os.Getpid())
	// Wait for SIGTERM or SIGINT
	go s.waitForInterrupt()

	s.errGr.Go(func() error {
		return s.db.Start()
	})

	return s.errGr.Wait()
}

// Shutdown gracefully stops the Olric server instance, releasing resources and ensuring a clean termination.
func (s *OlricServer) Shutdown(ctx context.Context) error {
	return s.db.Shutdown(ctx)
}
