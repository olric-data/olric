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
	"context"
	"time"

	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/buraksezer/olric/stats"
)

const DefaultScanCount = 10

// Member denotes a member of the Olric cluster.
type Member struct {
	// Member name in the cluster. It's also host:port of the node.
	Name string

	// ID of the Member in the cluster. Hash of Name and Birthdate of the member
	ID uint64

	// Birthdate of the member in nanoseconds.
	Birthdate int64

	// Role of the member in the cluster. There is only one coordinator member
	// in a healthy cluster.
	Coordinator bool
}

// Iterator defines an interface to implement iterators on the distributed maps.
type Iterator interface {
	// Next returns true if there is more key in the iterator implementation.
	// Otherwise, it returns false.
	Next() bool

	// Key returns a key name from the distributed map.
	Key() string

	// Close stops the iteration and releases allocated resources.
	Close()
}

// LockContext interface defines methods to manage locks on distributed maps.
type LockContext interface {
	// Unlock releases an acquired lock for the given key. It returns ErrNoSuchLock
	// if there is no lock for the given key.
	Unlock(ctx context.Context) error

	// Lease sets or updates the timeout of the acquired lock for the given key.
	// It returns ErrNoSuchLock if there is no lock for the given key.
	Lease(ctx context.Context, duration time.Duration) error
}

// PutOption is a function for define options to control behavior of the Put command.
type PutOption func(*dmap.PutConfig)

// EX sets the specified expire time, in seconds.
func EX(ex time.Duration) PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasEX = true
		cfg.EX = ex
	}
}

// PX sets the specified expire time, in milliseconds.
func PX(px time.Duration) PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasPX = true
		cfg.PX = px
	}
}

// EXAT sets the specified Unix time at which the key will expire, in seconds.
func EXAT(exat time.Duration) PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasEXAT = true
		cfg.EXAT = exat
	}
}

// PXAT sets the specified Unix time at which the key will expire, in milliseconds.
func PXAT(pxat time.Duration) PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasPXAT = true
		cfg.PXAT = pxat
	}
}

// NX only sets the key if it does not already exist.
func NX() PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasNX = true
	}
}

// XX only sets the key if it already exists.
func XX() PutOption {
	return func(cfg *dmap.PutConfig) {
		cfg.HasXX = true
	}
}

type dmapConfig struct {
	storageEntryImplementation func() storage.Entry
}

// DMapOption is a function for defining options to control behavior of distributed map instances.
type DMapOption func(*dmapConfig)

// StorageEntryImplementation sets and encoder/decoder implementation for your choice of storage engine.
func StorageEntryImplementation(e func() storage.Entry) DMapOption {
	return func(cfg *dmapConfig) {
		cfg.storageEntryImplementation = e
	}
}

// ScanOption is a function for defining options to control behavior of the SCAN command.
type ScanOption func(*dmap.ScanConfig)

// Count is the user specified the amount of work that should be done at every call in order to
// retrieve elements from the distributed map. This is just a hint for the implementation,
// however generally speaking this is what you could expect most of the time from the implementation.
// The default value is 10.
func Count(c int) ScanOption {
	return func(cfg *dmap.ScanConfig) {
		cfg.HasCount = true
		cfg.Count = c
	}
}

// Match is used for using regular expressions on keys. See https://pkg.go.dev/regexp
func Match(s string) ScanOption {
	return func(cfg *dmap.ScanConfig) {
		cfg.HasMatch = true
		cfg.Match = s
	}
}

// DMap defines methods to access and manipulate distributed maps.
type DMap interface {
	// Name exposes name of the DMap.
	Name() string

	// Put sets the value for the given key. It overwrites any previous value for
	// that key, and it's thread-safe. The key has to be a string. value type is arbitrary.
	// It is safe to modify the contents of the arguments after Put returns but not before.
	Put(ctx context.Context, key string, value interface{}, options ...PutOption) error

	// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
	// does not contain the key. It's thread-safe. It is safe to modify the contents
	// of the returned value. See GetResponse for the details.
	Get(ctx context.Context, key string) (*GetResponse, error)

	// Delete deletes values for the given keys. Delete will not return error
	// if key doesn't exist. It's thread-safe. It is safe to modify the contents
	// of the argument after Delete returns.
	Delete(ctx context.Context, keys ...string) (int, error)

	// Incr atomically increments the key by delta. The return value is the new value
	// after being incremented or an error.
	Incr(ctx context.Context, key string, delta int) (int, error)

	// Decr atomically decrements the key by delta. The return value is the new value
	// after being decremented or an error.
	Decr(ctx context.Context, key string, delta int) (int, error)

	// GetPut atomically sets the key to value and returns the old value stored at key. It returns nil if there is no
	// previous value.
	GetPut(ctx context.Context, key string, value interface{}) (*GetResponse, error)

	// IncrByFloat atomically increments the key by delta. The return value is the new value
	// after being incremented or an error.
	IncrByFloat(ctx context.Context, key string, delta float64) (float64, error)

	// Expire updates the expiry for the given key. It returns ErrKeyNotFound if
	// the DB does not contain the key. It's thread-safe.
	Expire(ctx context.Context, key string, timeout time.Duration) error

	// Lock sets a lock for the given key. Acquired lock is only for the key in
	// this dmap.
	//
	// It returns immediately if it acquires the lock for the given key. Otherwise,
	// it waits until deadline.
	//
	// You should know that the locks are approximate, and only to be used for
	// non-critical purposes.
	Lock(ctx context.Context, key string, deadline time.Duration) (LockContext, error)

	// LockWithTimeout sets a lock for the given key. If the lock is still unreleased
	// the end of given period of time,
	// it automatically releases the lock. Acquired lock is only for the key in
	// this dmap.
	//
	// It returns immediately if it acquires the lock for the given key. Otherwise,
	// it waits until deadline.
	//
	// You should know that the locks are approximate, and only to be used for
	// non-critical purposes.
	LockWithTimeout(ctx context.Context, key string, timeout, deadline time.Duration) (LockContext, error)

	// Scan returns an iterator to loop over the keys.
	//
	// Available scan options:
	//
	// * Count
	// * Match
	Scan(ctx context.Context, options ...ScanOption) (Iterator, error)

	// Destroy flushes the given DMap on the cluster. You should know that there
	// is no global lock on DMaps. So if you call Put/PutEx and Destroy methods
	// concurrently on the cluster, Put call may set new values to the DMap.
	Destroy(ctx context.Context) error

	// Pipeline is a mechanism to realise Redis Pipeline technique.
	//
	// Pipelining is a technique to extremely speed up processing by packing
	// operations to batches, send them at once to Redis and read a replies in a
	// singe step.
	// See https://redis.io/topics/pipelining
	//
	// Pay attention, that Pipeline is not a transaction, so you can get unexpected
	// results in case of big pipelines and small read/write timeouts.
	// Redis client has retransmission logic in case of timeouts, pipeline
	// can be retransmitted and commands can be executed more than once.
	Pipeline(opts ...PipelineOption) (*DMapPipeline, error)
}

// PipelineOption is a function for defining options to control behavior of the Pipeline command.
type PipelineOption func(pipeline *DMapPipeline)

// PipelineConcurrency is a PipelineOption controlling the number of concurrent goroutines.
func PipelineConcurrency(concurrency int) PipelineOption {
	return func(dp *DMapPipeline) {
		dp.concurrency = concurrency
	}
}

type statsConfig struct {
	CollectRuntime bool
}

// StatsOption is a function for defining options to control behavior of the STATS command.
type StatsOption func(*statsConfig)

// CollectRuntime is a StatsOption for collecting Go runtime statistics from a cluster member.
func CollectRuntime() StatsOption {
	return func(cfg *statsConfig) {
		cfg.CollectRuntime = true
	}
}

type pubsubConfig struct {
	Address string
}

// ToAddress is a PubSubOption for using a specific cluster member to publish messages to a channel.
func ToAddress(addr string) PubSubOption {
	return func(cfg *pubsubConfig) {
		cfg.Address = addr
	}
}

// PubSubOption is a function for defining options to control behavior of the Publish-Subscribe service.
type PubSubOption func(option *pubsubConfig)

// Client is an interface that denotes an Olric client.
type Client interface {
	// NewDMap returns a new DMap client with the given options.
	NewDMap(name string, options ...DMapOption) (DMap, error)

	// NewPubSub returns a new PubSub client with the given options.
	NewPubSub(options ...PubSubOption) (*PubSub, error)

	// Stats returns stats.Stats with the given options.
	Stats(ctx context.Context, address string, options ...StatsOption) (stats.Stats, error)

	// Ping sends a ping message to an Olric node. Returns PONG if message is empty,
	// otherwise return a copy of the message as a bulk. This command is often used to test
	// if a connection is still alive, or to measure latency.
	Ping(ctx context.Context, address, message string) (string, error)

	// RoutingTable returns the latest version of the routing table.
	RoutingTable(ctx context.Context) (RoutingTable, error)

	// Members returns a thread-safe list of cluster members.
	Members(ctx context.Context) ([]Member, error)

	// RefreshMetadata fetches a list of available members and the latest routing
	// table version. It also closes stale clients, if there are any.
	RefreshMetadata(ctx context.Context) error

	// Close stops background routines and frees allocated resources.
	Close(ctx context.Context) error
}
