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

/*Package stats exposes internal data structures for Stat command*/
package stats

import "runtime"

type (
	// PartitionID denotes ID of a partition in the cluster.
	PartitionID uint64

	// MemberID denotes ID of a member in the cluster.
	MemberID uint64
)

// SlabInfo denotes memory usage of the storage engine(a hash indexed, append only byte slice).
type SlabInfo struct {
	// Total allocated space by the append-only byte slice.
	Allocated int `json:"allocated"`

	// Total inuse memory space in the append-only byte slice.
	Inuse int `json:"inuse"`

	// Total garbage(deleted key/value pairs) space in the append-only byte slice.
	Garbage int `json:"garbage"`
}

// DMap denotes a distributed map instance on the cluster.
type DMap struct {
	// Number of keys in the DMap.
	Length int `json:"length"`

	// Statistics about memory representation of a DMap.
	SlabInfo SlabInfo `json:"slab_info"`

	// Number of tables in a storage instance.
	NumTables int `json:"num_tables"`
}

// Partition denotes a partition and its metadata in the cluster.
type Partition struct {
	// PreviousOwners is a list of members whose still owns some fragments.
	PreviousOwners []Member `json:"previous_owners"`

	// Backups is a list of members whose holds replicas of this partition.
	Backups []Member `json:"backups"`

	// Total number of entries in the partition.
	Length int `json:"length"`

	// DMaps is a map that contains statistics of DMaps in this partition.
	DMaps map[string]DMap `json:"dmaps"`
}

// Runtime exposes memory stats and various metrics from Go runtime.
type Runtime struct {
	// GOOS is the running program's operating system target
	GOOS string `json:"goos"`

	// GOARCH is the running program's architecture target
	GOARCH string `json:"goarch"`

	// Version returns the Go tree's version string.
	Version string `json:"version"`

	// NumCPU returns the number of logical CPUs usable by the current process.
	NumCPU int `json:"num_cpu"`

	// NumGoroutine returns the number of goroutines that currently exist.
	NumGoroutine int `json:"num_goroutine"`

	// MemStats records statistics about the memory allocator.
	MemStats runtime.MemStats `json:"mem_stats"`
}

// Member denotes a cluster member.
type Member struct {
	// Name is name of the node in the cluster.
	Name string `json:"name"`

	// ID is the unique identifier of this node in the cluster. It's derived
	// from Name and Birthdate.
	ID uint64 `json:"id"`

	// Birthdate is UNIX time in nanoseconds.
	Birthdate int64 `json:"birthdate"`
}

// String returns the member name.
func (m Member) String() string {
	return m.Name
}

// Network holds network statistics.
type Network struct {
	// ConnectionsTotal is total number of connections opened since the server started running.
	ConnectionsTotal int64 `json:"connections_total"`

	// CurrentConnections is current number of open connections.
	CurrentConnections int64 `json:"current_connections"`

	// WrittenBytesTotal is total number of bytes sent by this server to network.
	WrittenBytesTotal int64 `json:"written_bytes_total"`

	// ReadBytesTotal is total number of bytes read by this server from network.
	ReadBytesTotal int64 `json:"read_bytes_total"`

	// CommandsTotal is total number of all requests (get, put, etc.).
	CommandsTotal int64 `json:"commands_total"`
}

// DMaps holds global DMap statistics.
type DMaps struct {
	// EntriesTotal is the total number of entries(including replicas) stored during the life of this instance.
	EntriesTotal int64 `json:"entries_total"`

	// DeleteHits is the number of deletion reqs resulting in an item being removed.
	DeleteHits int64 `json:"delete_hits"`

	// DeleteMisses is the number of deletions reqs for missing keys
	DeleteMisses int64 `json:"delete_misses"`

	// GetMisses is the number of entries that have been requested and not found
	GetMisses int64 `json:"get_misses"`

	// GetHits is the number of entries that have been requested and found present
	GetHits int64 `json:"get_hits"`

	// EvictedTotal is the number of entries removed from cache to free memory for new entries.
	EvictedTotal int64 `json:"evicted_total"`
}

// PubSub holds global Pub/Sub statistics.
type PubSub struct {
	// PublishedTotal is the total number of published messages to PubSub during the life of this instance.
	PublishedTotal int64 `json:"published_total"`

	// CurrentSubscribers is the current number of Pub/Sub listeners of PubSub.
	CurrentSubscribers int64 `json:"current_subscribers"`

	// SubscribersTotal is the total number of registered Pub/Sub listeners during the life of this instance.
	SubscribersTotal int64 `json:"subscribers_total"`

	// CurrentSubscribers is the current number of Pub/Sub listeners of PubSub.
	CurrentPSubscribers int64 `json:"current_psubscribers"`

	// SubscribersTotal is the total number of registered Pub/Sub listeners during the life of this instance.
	PSubscribersTotal int64 `json:"psubscribers_total"`
}

// Stats is a struct that exposes statistics about the current state of a member.
type Stats struct {
	// Cmdline holds the command-line arguments, starting with the program name.
	Cmdline []string `json:"cmdline"`

	// ReleaseVersion is the current Olric version
	ReleaseVersion string `json:"release_version"`

	// UptimeSeconds is number of seconds since the server started.
	UptimeSeconds int64 `json:"uptime_seconds"`

	// Stats from Golang runtime
	Runtime *Runtime `json:"runtime"`

	// ClusterCoordinator is the current cluster coordinator.
	ClusterCoordinator Member `json:"cluster_coordinator"`

	// Member denotes the current member.
	Member Member `json:"member"`

	// Partitions is a map that contains partition statistics.
	Partitions map[PartitionID]Partition `json:"partitions"`

	// Backups is a map that contains backup partition statistics.
	Backups map[PartitionID]Partition `json:"backups"`

	// ClusterMembers is a map that contains bootstrapped cluster members
	ClusterMembers map[MemberID]Member `json:"cluster_members"`

	// Network holds network statistics.
	Network Network `json:"network"`

	// DMaps holds global DMap statistics.
	DMaps DMaps `json:"dmaps"`

	// PubSub holds global Pub/Sub statistics.
	PubSub PubSub `json:"pub_sub"`
}
