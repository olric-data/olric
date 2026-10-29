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

package routingtable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/protocol"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/olric-data/olric/internal/testutil/mockfragment"
	"github.com/vmihailenco/msgpack/v5"
)

func TestRoutingTable_BusyPartitionError(t *testing.T) {
	for _, kind := range []partitions.Kind{partitions.PRIMARY, partitions.BACKUP} {
		t.Run(kind.String(), func(t *testing.T) {
			cluster := newTestCluster()
			defer func() {
				err := cluster.shutdown()
				if err != nil {
					t.Error(err)
				}
			}()
			r, err := cluster.addNode(nil)
			if err != nil {
				t.Fatal(err)
			}
			ps := r.primary
			if kind == partitions.BACKUP {
				ps = r.backup
			}
			f := mockfragment.New()
			f.Put("key", "value")
			ps.PartitionByID(0).Map().Store("dmap.busy", f)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			probe := protocol.NewLengthOfPart(0)
			if kind == partitions.BACKUP {
				probe.SetReplica()
			}
			cmd := probe.Command(ctx)
			f.Lock()
			err = r.client.Get(r.This().String()).Process(ctx, cmd)
			f.Unlock()
			if !errors.Is(protocol.ConvertError(err), ErrPartitionBusy) {
				t.Fatalf("busy probe returned %v, want %v", err, ErrPartitionBusy)
			}
			cmd = probe.Command(ctx)
			err = r.client.Get(r.This().String()).Process(ctx, cmd)
			if err != nil {
				t.Fatal(err)
			}
			length, err := cmd.Result()
			if err != nil || length != 1 {
				t.Fatalf("probe after unlocking returned length %d, error %v", length, err)
			}
		})
	}
}

func TestRoutingTable_ReportBusyFragment(t *testing.T) {
	for _, kind := range []partitions.Kind{partitions.PRIMARY, partitions.BACKUP} {
		t.Run(kind.String(), func(t *testing.T) {
			c := testutil.NewConfig()
			c.PartitionCount = 1
			r := &RoutingTable{
				config:  c,
				primary: partitions.New(1, partitions.PRIMARY),
				backup:  partitions.New(1, partitions.BACKUP),
			}
			ps := r.primary
			if kind == partitions.BACKUP {
				ps = r.backup
			}
			f := mockfragment.New()
			ps.PartitionByID(0).Map().Store("dmap.busy", f)
			f.Lock()
			locked := true
			defer func() {
				if locked {
					f.Unlock()
				}
			}()

			type result struct {
				data []byte
				err  error
			}
			done := make(chan result, 1)
			go func() {
				data, err := r.prepareLeftOverDataReport()
				done <- result{data: data, err: err}
			}()
			select {
			case got := <-done:
				if got.err != nil {
					t.Fatal(got.err)
				}
				var report leftOverDataReport
				err := msgpack.Unmarshal(got.data, &report)
				if err != nil {
					t.Fatal(err)
				}
				ids := report.Partitions
				if kind == partitions.BACKUP {
					ids = report.Backups
				}
				if len(ids) != 1 || ids[0] != 0 {
					t.Fatalf("busy partition must be retained: %+v", report)
				}
			case <-time.After(time.Second):
				f.Unlock()
				locked = false
				<-done
				t.Fatal("ownership report blocked on a busy fragment")
			}
			f.Unlock()
			locked = false

			data, err := r.prepareLeftOverDataReport()
			if err != nil {
				t.Fatal(err)
			}
			var report leftOverDataReport
			err = msgpack.Unmarshal(data, &report)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Partitions) != 0 || len(report.Backups) != 0 {
				t.Fatalf("unlocked empty fragment must not retain ownership: %+v", report)
			}
		})
	}
}

func TestRoutingTable_BootstrapWithBusyFragment(t *testing.T) {
	for _, kind := range []partitions.Kind{partitions.PRIMARY, partitions.BACKUP} {
		t.Run(kind.String(), func(t *testing.T) {
			cluster := newTestCluster()
			defer func() {
				err := cluster.shutdown()
				if err != nil {
					t.Error(err)
				}
			}()
			c1 := testutil.NewConfig()
			c1.ReplicaCount = 2
			r1, err := cluster.addNode(c1)
			if err != nil {
				t.Fatal(err)
			}
			c2 := testutil.NewConfig()
			c2.ReplicaCount = 2
			r2, err := cluster.addNode(c2)
			if err != nil {
				t.Fatal(err)
			}
			err = r2.CheckBootstrap()
			if err != nil {
				t.Fatal(err)
			}

			ps := r1.primary
			if kind == partitions.BACKUP {
				ps = r1.backup
			}
			f := mockfragment.New()
			f.Put("key", "value")
			ps.PartitionByID(0).Map().Store("dmap.busy", f)
			f.Lock()
			defer f.Unlock()

			// A replacement must receive the routing table while an existing
			// member's eviction or migration holds a fragment lock.
			err = r2.Shutdown(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			c3 := testutil.NewConfig()
			c3.ReplicaCount = 2
			c3.BootstrapTimeout = time.Second
			r3, err := cluster.addNode(c3)
			if err != nil {
				t.Fatal(err)
			}
			err = r3.CheckBootstrap()
			if err != nil {
				t.Fatalf("replacement could not bootstrap with a busy fragment: %v", err)
			}
		})
	}
}
