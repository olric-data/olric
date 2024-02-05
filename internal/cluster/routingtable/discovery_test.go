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

package routingtable

import (
	"context"
	"errors"
	"github.com/buraksezer/olric/internal/testutil"
	"testing"
	"time"
)

func TestRoutingTable_tryWithInterval(t *testing.T) {
	c := testutil.NewConfig()
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	var foobarError = errors.New("foobar")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := rt.tryWithInterval(ctx, time.Millisecond, func() error {
		return foobarError
	})

	if err != foobarError {
		t.Fatalf("Expected foobarError. Got: %v", foobarError)
	}
}

func TestRoutingTable_attemptToJoin(t *testing.T) {
	c := testutil.NewConfig()
	c.MaxJoinAttempts = 3
	c.JoinRetryInterval = 100 * time.Millisecond
	c.Peers = []string{"127.0.0.1:0"} // An invalid peer
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	err := rt.discovery.Start()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = rt.discovery.Shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	err = rt.attemptToJoin()
	if err != ErrClusterJoin {
		t.Fatalf("Expected ErrClusterJoin. Got: %v", err)
	}
}
