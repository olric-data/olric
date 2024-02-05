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

package olric

import (
	"encoding/json"
	"os"
	"runtime"
	"strings"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/pubsub"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/stats"
	"github.com/tidwall/redcon"
)

func toMember(member discovery.Member) stats.Member {
	return stats.Member{
		Name:      member.Name,
		ID:        member.ID,
		Birthdate: member.Birthdate,
	}
}

func toMembers(members []discovery.Member) []stats.Member {
	var _stats []stats.Member
	for _, m := range members {
		_stats = append(_stats, toMember(m))
	}
	return _stats
}

func (db *Olric) collectPartitionMetrics(partID uint64, part *partitions.Partition) stats.Partition {
	owners := part.Owners()
	p := stats.Partition{
		Backups: toMembers(db.backup.PartitionOwnersByID(partID)),
		Length:  part.Length(),
		DMaps:   make(map[string]stats.DMap),
	}
	if len(owners) > 0 {
		p.PreviousOwners = toMembers(owners[:len(owners)-1])
	}
	part.Map().Range(func(name, item interface{}) bool {
		f := item.(partitions.Fragment)
		st := f.Stats()
		tmp := stats.DMap{
			Length:    st.Length,
			NumTables: st.NumTables,
		}
		tmp.SlabInfo.Allocated = st.Allocated
		tmp.SlabInfo.Garbage = st.Garbage
		tmp.SlabInfo.Inuse = st.Inuse
		dmapName := strings.TrimPrefix(name.(string), "dmap.")
		p.DMaps[dmapName] = tmp
		return true
	})
	return p
}

func (db *Olric) checkPartitionOwnership(part *partitions.Partition) bool {
	owners := part.Owners()
	for _, owner := range owners {
		if owner.CompareByID(db.rt.This()) {
			return true
		}
	}
	return false
}

func (db *Olric) stats(cfg statsConfig) stats.Stats {
	s := stats.Stats{
		Cmdline:            os.Args,
		ReleaseVersion:     ReleaseVersion,
		UptimeSeconds:      discovery.UptimeSeconds.Read(),
		ClusterCoordinator: toMember(db.rt.Discovery().GetCoordinator()),
		Member:             toMember(db.rt.This()),
		Partitions:         make(map[stats.PartitionID]stats.Partition),
		Backups:            make(map[stats.PartitionID]stats.Partition),
		ClusterMembers:     make(map[stats.MemberID]stats.Member),
		Network: stats.Network{
			ConnectionsTotal:   server.ConnectionsTotal.Read(),
			CurrentConnections: server.CurrentConnections.Read(),
			WrittenBytesTotal:  server.WrittenBytesTotal.Read(),
			ReadBytesTotal:     server.ReadBytesTotal.Read(),
			CommandsTotal:      server.CommandsTotal.Read(),
		},
		DMaps: stats.DMaps{
			EntriesTotal: dmap.EntriesTotal.Read(),
			DeleteHits:   dmap.DeleteHits.Read(),
			DeleteMisses: dmap.DeleteMisses.Read(),
			GetMisses:    dmap.GetMisses.Read(),
			GetHits:      dmap.GetHits.Read(),
			EvictedTotal: dmap.EvictedTotal.Read(),
		},
		PubSub: stats.PubSub{
			PublishedTotal:      pubsub.PublishedTotal.Read(),
			CurrentSubscribers:  pubsub.CurrentSubscribers.Read(),
			SubscribersTotal:    pubsub.SubscribersTotal.Read(),
			CurrentPSubscribers: pubsub.CurrentPSubscribers.Read(),
			PSubscribersTotal:   pubsub.PSubscribersTotal.Read(),
		},
	}

	if cfg.CollectRuntime {
		s.Runtime = &stats.Runtime{
			GOOS:         runtime.GOOS,
			GOARCH:       runtime.GOARCH,
			Version:      runtime.Version(),
			NumCPU:       runtime.NumCPU(),
			NumGoroutine: runtime.NumGoroutine(),
		}
		runtime.ReadMemStats(&s.Runtime.MemStats)
	}

	db.rt.RLock()
	defer db.rt.RUnlock()

	db.rt.Members().Range(func(id uint64, member discovery.Member) bool {
		s.ClusterMembers[stats.MemberID(id)] = toMember(member)
		return true
	})

	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		primary := db.primary.PartitionByID(partID)
		if db.checkPartitionOwnership(primary) {
			s.Partitions[stats.PartitionID(partID)] = db.collectPartitionMetrics(partID, primary)
		}
		backup := db.backup.PartitionByID(partID)
		if db.checkPartitionOwnership(backup) {
			s.Backups[stats.PartitionID(partID)] = db.collectPartitionMetrics(partID, backup)
		}
	}

	return s
}

func (db *Olric) statsCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	statsCmd, err := protocol.ParseStatsCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	sc := statsConfig{}
	if statsCmd.CollectRuntime {
		sc.CollectRuntime = true
	}
	memberStats := db.stats(sc)
	data, err := json.Marshal(memberStats)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteBulk(data)
}
