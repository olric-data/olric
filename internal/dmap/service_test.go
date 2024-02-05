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

package dmap

import (
	"context"
	"testing"

	"github.com/buraksezer/olric/internal/testcluster"
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
