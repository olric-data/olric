// Copyright 2018-2026 The Olric Authors
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

package dmap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/testcluster"
)

func TestDMapService(t *testing.T) {
	cluster := testcluster.New(NewService)
	defer cluster.Shutdown()
	s, ok := cluster.AddMember(nil).(*Service)
	if !ok {
		t.Fatal("AddMember returned a different service.Service implementation")
	}

	err := s.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMapService_Shutdown_WaitsForPendingBackups(t *testing.T) {
	cluster := testcluster.New(NewService)
	defer cluster.Shutdown()
	s := cluster.AddMember(nil).(*Service)

	s.pendingBackups.Add(1)
	done := make(chan error, 1)
	go func() {
		done <- s.Shutdown(context.Background())
	}()

	select {
	case <-done:
		t.Fatal("Shutdown returned with a backup write in flight")
	case <-time.After(100 * time.Millisecond):
	}
	if s.ctx.Err() != nil {
		t.Fatal("Service context cancelled with a backup write in flight")
	}

	s.pendingBackups.Add(-1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not return after the backup write finished")
	}
}

func TestDMapService_Shutdown_PendingBackupsBoundedByContext(t *testing.T) {
	cluster := testcluster.New(NewService)
	defer cluster.Shutdown()
	s := cluster.AddMember(nil).(*Service)

	s.pendingBackups.Add(1)
	defer s.pendingBackups.Add(-1)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := s.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Expected context.DeadlineExceeded. Got: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("Shutdown ignored the context deadline")
	}
	if s.ctx.Err() == nil {
		t.Fatal("Service context not cancelled after the deadline")
	}
}
