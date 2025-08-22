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

package olric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/olric-data/olric/config"
	"github.com/olric-data/olric/hasher"
	"github.com/olric-data/olric/internal/bufpool"
	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/discovery"
	"github.com/olric-data/olric/internal/dmap"
	"github.com/olric-data/olric/internal/kvstore/entry"
	"github.com/olric-data/olric/internal/protocol"
	"github.com/olric-data/olric/internal/resp"
	"github.com/olric-data/olric/internal/server"
	"github.com/olric-data/olric/pkg/storage"
	"github.com/olric-data/olric/stats"
	"github.com/redis/go-redis/v9"
)

var pool = bufpool.New()

// DefaultRoutingTableFetchInterval is the default value of RoutingTableFetchInterval. ClusterClient implementation
// fetches the routing table from the cluster to route requests to the right partition.
const DefaultRoutingTableFetchInterval = time.Minute

type ClusterLockContext struct {
	key   string
	token string
	dm    *ClusterDMap
}

// ClusterDMap implements a client for DMaps.
type ClusterDMap struct {
	name          string
	newEntry      func() storage.Entry
	config        *dmapConfig
	client        *server.Client
	clusterClient *ClusterClient
}

// Name exposes name of the DMap.
func (dm *ClusterDMap) Name() string {
	return dm.name
}

// processProtocolError processes protocol-related errors and translates them into defined application-level errors.
func processProtocolError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, redis.Nil) {
		return ErrKeyNotFound
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		opErr := err.(*net.OpError)
		return fmt.Errorf("%s %s %s: %w", opErr.Op, opErr.Net, opErr.Addr, ErrConnRefused)
	}
	return convertDMapError(protocol.ConvertError(err))
}

// writePutCommand constructs and returns a new protocol.Put command based on the provided key, value, and configuration options.
func (dm *ClusterDMap) writePutCommand(c *dmap.PutConfig, key string, value []byte) *protocol.Put {
	cmd := protocol.NewPut(dm.name, key, value)
	switch {
	case c.HasEX:
		cmd.SetEX(c.EX.Seconds())
	case c.HasPX:
		cmd.SetPX(c.PX.Milliseconds())
	case c.HasEXAT:
		cmd.SetEXAT(c.EXAT.Seconds())
	case c.HasPXAT:
		cmd.SetPXAT(c.PXAT.Milliseconds())
	}

	switch {
	case c.HasNX:
		cmd.SetNX()
	case c.HasXX:
		cmd.SetXX()
	}

	return cmd
}

func (cl *ClusterClient) clientByPartID(partID uint64) (*redis.Client, error) {
	raw := cl.routingTable.Load()
	if raw == nil {
		return nil, fmt.Errorf("routing table is empty")
	}

	routingTable, ok := raw.(RoutingTable)
	if !ok {
		return nil, fmt.Errorf("routing table is corrupt")
	}

	route := routingTable[partID]
	if len(route.PrimaryOwners) == 0 {
		return nil, fmt.Errorf("primary owners list for %d is empty", partID)
	}

	primaryOwner := route.PrimaryOwners[len(route.PrimaryOwners)-1]
	return cl.client.Get(primaryOwner), nil
}

func (cl *ClusterClient) smartPick(dmap, key string) (*redis.Client, error) {
	hkey := partitions.HKey(dmap, key)
	partID := hkey % cl.partitionCount
	return cl.clientByPartID(partID)
}

// Put sets the value for the given key. It overwrites any previous value for
// that key, and it's thread-safe. The key has to be a string. value type is arbitrary.
// It is safe to modify the contents of the arguments after Put returns but not before.
func (dm *ClusterDMap) Put(ctx context.Context, key string, value interface{}, options ...PutOption) error {
	rc, err := dm.clusterClient.smartPick(dm.name, key)
	if err != nil {
		return err
	}

	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := resp.New(valueBuf)
	err = enc.Encode(value)
	if err != nil {
		return err
	}

	var pc dmap.PutConfig
	for _, opt := range options {
		opt(&pc)
	}
	putCmd := dm.writePutCommand(&pc, key, valueBuf.Bytes())
	cmd := putCmd.Command(ctx)

	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}
	return processProtocolError(cmd.Err())
}

func (dm *ClusterDMap) makeGetResponse(cmd *redis.StringCmd) (*GetResponse, error) {
	raw, err := cmd.Bytes()
	if err != nil {
		return nil, processProtocolError(err)
	}

	e := dm.newEntry()
	e.Decode(raw)
	return &GetResponse{
		entry: e,
	}, nil
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
// does not contain the key. It's thread-safe. It is safe to modify the contents
// of the returned value. See GetResponse for the details.
func (dm *ClusterDMap) Get(ctx context.Context, key string) (*GetResponse, error) {
	cmd := protocol.NewGet(dm.name, key).SetRaw().Command(ctx)
	rc, err := dm.clusterClient.smartPick(dm.name, key)
	if err != nil {
		return nil, err
	}
	err = rc.Process(ctx, cmd)
	if err != nil {
		return nil, processProtocolError(err)
	}
	return dm.makeGetResponse(cmd)
}

// Delete deletes values for the given keys. Delete will not return an error if the key doesn't exist.
// It's thread-safe. It is safe to modify the contents of the argument after Delete returns.
func (dm *ClusterDMap) Delete(ctx context.Context, keys ...string) (int, error) {
	rc, err := dm.client.Pick()
	if err != nil {
		return 0, err
	}

	cmd := protocol.NewDel(dm.name, keys...).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return 0, processProtocolError(err)
	}

	res, err := cmd.Uint64()
	if err != nil {
		return 0, processProtocolError(cmd.Err())
	}
	return int(res), nil
}

// Incr atomically increments the key by delta. The return value is the new value
// after being incremented or an error.
func (dm *ClusterDMap) Incr(ctx context.Context, key string, delta int) (int, error) {
	rc, err := dm.clusterClient.smartPick(dm.name, key)
	if err != nil {
		return 0, err
	}

	cmd := protocol.NewIncr(dm.name, key, delta).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return 0, processProtocolError(err)
	}
	res, err := cmd.Uint64()
	if err != nil {
		return 0, processProtocolError(cmd.Err())
	}
	return int(res), nil
}

// Decr atomically decrements the key by delta. The return value is the new value
// after being decremented or an error.
func (dm *ClusterDMap) Decr(ctx context.Context, key string, delta int) (int, error) {
	rc, err := dm.clusterClient.smartPick(dm.name, key)
	if err != nil {
		return 0, err
	}

	cmd := protocol.NewDecr(dm.name, key, delta).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return 0, processProtocolError(err)
	}
	res, err := cmd.Uint64()
	if err != nil {
		return 0, processProtocolError(cmd.Err())
	}
	return int(res), nil
}

// GetPut atomically sets the key to value and returns the old value stored at a key. It returns nil if there is no
// previous value.
func (dm *ClusterDMap) GetPut(ctx context.Context, key string, value interface{}) (*GetResponse, error) {
	rc, err := dm.clusterClient.smartPick(dm.name, key)
	if err != nil {
		return nil, err
	}

	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := resp.New(valueBuf)
	err = enc.Encode(value)
	if err != nil {
		return nil, err
	}

	cmd := protocol.NewGetPut(dm.name, key, valueBuf.Bytes()).SetRaw().Command(ctx)
	err = rc.Process(ctx, cmd)
	err = processProtocolError(err)
	if err != nil {
		// First try to set a key/value with GetPut
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	raw, err := cmd.Bytes()
	if err != nil {
		return nil, processProtocolError(err)
	}

	e := dm.newEntry()
	e.Decode(raw)
	return &GetResponse{
		entry: e,
	}, nil
}

// IncrByFloat atomically increments the key by delta. The return value is the new value
// after being incremented or an error.
func (dm *ClusterDMap) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	rc, err := dm.clusterClient.smartPick(dm.name, key)
	if err != nil {
		return 0, err
	}

	cmd := protocol.NewIncrByFloat(dm.name, key, delta).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return 0, processProtocolError(err)
	}
	res, err := cmd.Result()
	if err != nil {
		return 0, processProtocolError(cmd.Err())
	}
	return res, nil
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if
// the DB does not contain the key. It's thread-safe.
func (dm *ClusterDMap) Expire(ctx context.Context, key string, timeout time.Duration) error {
	rc, err := dm.clusterClient.smartPick(dm.name, key)
	if err != nil {
		return err
	}

	cmd := protocol.NewExpire(dm.name, key, timeout).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}
	return processProtocolError(cmd.Err())
}

// Lock sets a lock for the given key. Acquired lock is only for the key in
// this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise,
// it waits until deadline.
//
// You should know that the locks are approximate and only to be used for
// non-critical purposes.
func (dm *ClusterDMap) Lock(ctx context.Context, key string, deadline time.Duration) (LockContext, error) {
	rc, err := dm.clusterClient.smartPick(dm.name, key)
	if err != nil {
		return nil, err
	}

	cmd := protocol.NewLock(dm.name, key, deadline.Seconds()).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return nil, processProtocolError(err)
	}

	token, err := cmd.Bytes()
	if err != nil {
		return nil, processProtocolError(err)
	}
	return &ClusterLockContext{
		key:   key,
		token: string(token),
		dm:    dm,
	}, nil
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased
// the end of a given period of time, it automatically releases the lock.
// Acquired lock is only for the key in this DMap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise,
// it waits until deadline.
//
// You should know that the locks are approximate and only to be used for
// non-critical purposes.
func (dm *ClusterDMap) LockWithTimeout(ctx context.Context, key string, timeout, deadline time.Duration) (LockContext, error) {
	rc, err := dm.clusterClient.smartPick(dm.name, key)
	if err != nil {
		return nil, err
	}

	cmd := protocol.NewLock(dm.name, key, deadline.Seconds()).SetPX(timeout.Milliseconds()).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return nil, processProtocolError(err)
	}

	token, err := cmd.Bytes()
	if err != nil {
		return nil, processProtocolError(err)
	}

	return &ClusterLockContext{
		key:   key,
		token: string(token),
		dm:    dm,
	}, nil
}

// Close stops background routines and frees allocated resources.
func (dm *ClusterDMap) Close(_ context.Context) error {
	return nil
}

// Unlock releases the distributed lock associated with the current context by using the provided context for execution.
func (c *ClusterLockContext) Unlock(ctx context.Context) error {
	rc, err := c.dm.clusterClient.smartPick(c.dm.name, c.key)
	if err != nil {
		return err
	}
	cmd := protocol.NewUnlock(c.dm.name, c.key, c.token).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}
	return processProtocolError(cmd.Err())
}

// Lease extends the lease of the distributed lock associated with the context for the specified duration.
func (c *ClusterLockContext) Lease(ctx context.Context, duration time.Duration) error {
	rc, err := c.dm.clusterClient.smartPick(c.dm.name, c.key)
	if err != nil {
		return err
	}
	cmd := protocol.NewLockLease(c.dm.name, c.key, c.token, duration.Seconds()).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}
	return processProtocolError(cmd.Err())
}

// Scan returns an iterator to loop over the keys.
//
// Available scan options:
//
// * Count
// * Match
func (dm *ClusterDMap) Scan(ctx context.Context, options ...ScanOption) (Iterator, error) {
	var sc dmap.ScanConfig
	for _, opt := range options {
		opt(&sc)
	}
	if sc.Count == 0 {
		sc.Count = DefaultScanCount
	}

	ictx, cancel := context.WithCancel(ctx)
	i := &ClusterIterator{
		dm:            dm,
		clusterClient: dm.clusterClient,
		config:        &sc,
		logger:        dm.clusterClient.logger,
		partitionKeys: make(map[string]struct{}),
		cursors:       make(map[uint64]map[string]*currentCursor),
		ctx:           ictx,
		cancel:        cancel,
	}

	// Embedded iterator uses a slightly different scan function.
	i.scanner = i.scanOnOwners

	if err := i.fetchRoutingTable(); err != nil {
		return nil, err
	}
	// Load the route for the first partition (0) to scan.
	i.loadRoute()

	i.wg.Add(1)
	go i.fetchRoutingTablePeriodically()

	return i, nil
}

// Destroy flushes the given DMap on the cluster. You should know that there
// is no global lock on DMaps. So if you call Put/PutEx and Destroy methods
// concurrently on the cluster, Put call may set new values to the DMap.
func (dm *ClusterDMap) Destroy(ctx context.Context) error {
	rc, err := dm.client.Pick()
	if err != nil {
		return err
	}

	cmd := protocol.NewDestroy(dm.name).Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return processProtocolError(err)
	}

	return processProtocolError(cmd.Err())
}

// ClusterClient is a client for managing and interacting with a distributed cluster of nodes.
type ClusterClient struct {
	client         *server.Client
	config         *clusterClientConfig
	logger         *log.Logger
	routingTable   atomic.Value
	partitionCount uint64
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

// Ping sends a ping message to an Olric node. Returns PONG if a message is empty,
// otherwise return a copy of the message as bulk. This command is often used to test
// if a connection is still alive or to measure latency.
func (cl *ClusterClient) Ping(ctx context.Context, addr, message string) (string, error) {
	pingCmd := protocol.NewPing()
	if message != "" {
		pingCmd.SetMessage(message)
	}
	cmd := pingCmd.Command(ctx)

	rc := cl.client.Get(addr)
	err := rc.Process(ctx, cmd)
	if err != nil {
		return "", processProtocolError(err)
	}
	err = processProtocolError(cmd.Err())
	if err != nil {
		return "", nil
	}

	return cmd.Result()
}

// RoutingTable returns the latest version of the routing table.
func (cl *ClusterClient) RoutingTable(ctx context.Context) (RoutingTable, error) {
	cmd := protocol.NewClusterRoutingTable().Command(ctx)
	rc, err := cl.client.Pick()
	if err != nil {
		return RoutingTable{}, err
	}

	err = rc.Process(ctx, cmd)
	if err != nil {
		return RoutingTable{}, processProtocolError(err)
	}

	if err = cmd.Err(); err != nil {
		return RoutingTable{}, processProtocolError(err)
	}

	result, err := cmd.Slice()
	if err != nil {
		return RoutingTable{}, processProtocolError(err)

	}
	return mapToRoutingTable(result)
}

// Stats returns stats.Stats with the given options.
func (cl *ClusterClient) Stats(ctx context.Context, address string, options ...StatsOption) (stats.Stats, error) {
	var cfg statsConfig
	for _, opt := range options {
		opt(&cfg)
	}

	statsCmd := protocol.NewStats()
	if cfg.CollectRuntime {
		statsCmd.SetCollectRuntime()
	}

	cmd := statsCmd.Command(ctx)
	rc := cl.client.Get(address)

	err := rc.Process(ctx, cmd)
	if err != nil {
		return stats.Stats{}, processProtocolError(err)
	}

	if err = cmd.Err(); err != nil {
		return stats.Stats{}, processProtocolError(err)
	}
	data, err := cmd.Bytes()
	if err != nil {
		return stats.Stats{}, processProtocolError(err)
	}
	var s stats.Stats
	err = json.Unmarshal(data, &s)
	if err != nil {
		return stats.Stats{}, processProtocolError(err)
	}
	return s, nil
}

// Members returns a thread-safe list of cluster members.
func (cl *ClusterClient) Members(ctx context.Context) ([]Member, error) {
	rc, err := cl.client.Pick()
	if err != nil {
		return []Member{}, err
	}

	cmd := protocol.NewClusterMembers().Command(ctx)
	err = rc.Process(ctx, cmd)
	if err != nil {
		return []Member{}, processProtocolError(err)
	}

	if err = cmd.Err(); err != nil {
		return []Member{}, processProtocolError(err)
	}

	items, err := cmd.Slice()
	if err != nil {
		return []Member{}, processProtocolError(err)
	}
	var members []Member
	for _, rawItem := range items {
		m := Member{}
		item := rawItem.([]interface{})
		m.Name = item[0].(string)
		m.Birthdate = item[1].(int64)

		// go-redis/redis package cannot handle uint64 type. At the time of this writing,
		// there is no solution for this, and I don't want to use a soft fork to repair it.
		m.ID = discovery.MemberID(m.Name, m.Birthdate)

		if item[2] == "true" {
			m.Coordinator = true
		}
		members = append(members, m)
	}
	return members, nil
}

// RefreshMetadata fetches a list of available members and the latest routing
// table version. It also closes stale clients if there are any.
func (cl *ClusterClient) RefreshMetadata(ctx context.Context) error {
	// Fetch a list of currently available cluster members.
	var members []Member
	var err error
	for {
		members, err = cl.Members(ctx)
		if errors.Is(err, ErrConnRefused) {
			continue
		}
		if err != nil {
			return err
		}
		break
	}
	// Use a map for fast access.
	addresses := make(map[string]struct{})
	for _, member := range members {
		addresses[member.Name] = struct{}{}
	}

	// Clean stale client connections
	for addr := range cl.client.Addresses() {
		if _, ok := addresses[addr]; !ok {
			// Gone
			if err := cl.client.Close(addr); err != nil {
				return err
			}
		}
	}

	// Re-fetch the routing table, we should use the latest routing table version.
	return cl.fetchRoutingTable()
}

// Close stops background routines and frees allocated resources.
func (cl *ClusterClient) Close(ctx context.Context) error {
	select {
	case <-cl.ctx.Done():
		return nil
	default:
	}

	cl.cancel()

	// Wait for the background workers:
	// * fetchRoutingTablePeriodically
	cl.wg.Wait()

	// Close the underlying TCP sockets gracefully.
	return cl.client.Shutdown(ctx)
}

// NewPubSub returns a new PubSub client with the given options.
func (cl *ClusterClient) NewPubSub(options ...PubSubOption) (*PubSub, error) {
	return newPubSub(cl.client, options...)
}

// NewDMap returns a new DMap client with the given options.
func (cl *ClusterClient) NewDMap(name string, options ...DMapOption) (DMap, error) {
	var dc dmapConfig
	for _, opt := range options {
		opt(&dc)
	}

	if dc.storageEntryImplementation == nil {
		dc.storageEntryImplementation = func() storage.Entry {
			return entry.New()
		}
	}

	return &ClusterDMap{name: name,
		config:        &dc,
		newEntry:      dc.storageEntryImplementation,
		client:        cl.client,
		clusterClient: cl,
	}, nil
}

// ClusterClientOption is a functional option for configuring a clusterClientConfig instance.
type ClusterClientOption func(c *clusterClientConfig)

// clusterClientConfig holds the configuration required to initialize and manage a cluster client instance.
type clusterClientConfig struct {
	logger                    *log.Logger
	config                    *config.Client
	authentication            *config.Authentication
	hasher                    hasher.Hasher
	routingTableFetchInterval time.Duration
}

// WithHasher sets a custom hasher implementation to the cluster client configuration.
func WithHasher(h hasher.Hasher) ClusterClientOption {
	return func(cfg *clusterClientConfig) {
		cfg.hasher = h
	}
}

// WithLogger sets a custom logger for the cluster client configuration.
func WithLogger(l *log.Logger) ClusterClientOption {
	return func(cfg *clusterClientConfig) {
		cfg.logger = l
	}
}

// WithConfig applies a specified config.Client to the clusterClientConfig.
func WithConfig(c *config.Client) ClusterClientOption {
	return func(cfg *clusterClientConfig) {
		cfg.config = c
	}
}

// WithPassword configures a cluster client with the specified password for authentication.
func WithPassword(password string) ClusterClientOption {
	return func(cfg *clusterClientConfig) {
		cfg.authentication = &config.Authentication{
			Password: password,
		}
	}
}

// WithRoutingTableFetchInterval sets the interval for periodic fetching of the routing table in a cluster client configuration.
func WithRoutingTableFetchInterval(interval time.Duration) ClusterClientOption {
	return func(cfg *clusterClientConfig) {
		cfg.routingTableFetchInterval = interval
	}
}

// fetchRoutingTable updates the cluster routing table by fetching the latest version from the cluster.
// It initializes the partition count if it's the first invocation. Returns an error if fetching fails.
func (cl *ClusterClient) fetchRoutingTable() error {
	ctx, cancel := context.WithCancel(cl.ctx)
	defer cancel()

	routingTable, err := cl.RoutingTable(ctx)
	if err != nil {
		return fmt.Errorf("error while loading the routing table: %w", err)
	}

	previous := cl.routingTable.Load()
	if previous == nil {
		// First run. Partition count is a constant, actually. It has to be greater than zero.
		cl.partitionCount = uint64(len(routingTable))
	}
	cl.routingTable.Store(routingTable)
	return nil
}

// fetchRoutingTablePeriodically periodically updates the routing table by invoking fetchRoutingTable at configured intervals.
// It stops gracefully when the context is canceled or an error occurs.
func (cl *ClusterClient) fetchRoutingTablePeriodically() {
	defer cl.wg.Done()

	ticker := time.NewTicker(cl.config.routingTableFetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cl.ctx.Done():
			return
		case <-ticker.C:
			err := cl.fetchRoutingTable()
			if err != nil {
				cl.logger.Printf("[ERROR] Failed to fetch the latest version of the routing table: %s", err)
			}
		}
	}
}

// NewClusterClient creates a new Client instance. It needs one node address at least to discover the whole cluster.
func NewClusterClient(addresses []string, options ...ClusterClientOption) (*ClusterClient, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("addresses cannot be empty")
	}

	var cc clusterClientConfig
	for _, opt := range options {
		opt(&cc)
	}

	if cc.hasher == nil {
		cc.hasher = hasher.NewDefaultHasher()
	}

	if cc.logger == nil {
		cc.logger = log.New(os.Stderr, "logger: ", log.Lshortfile)
	}

	if cc.config == nil {
		cc.config = config.NewClient()
	}

	if cc.authentication != nil {
		cc.config.Authentication = cc.authentication
	}

	if cc.routingTableFetchInterval <= 0 {
		cc.routingTableFetchInterval = DefaultRoutingTableFetchInterval
	}

	if err := cc.config.Sanitize(); err != nil {
		return nil, err
	}
	if err := cc.config.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	cl := &ClusterClient{
		client: server.NewClient(cc.config),
		config: &cc,
		logger: cc.logger,
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize clients for the given cluster members.
	for _, address := range addresses {
		cl.client.Get(address)
	}

	// Discover all cluster members
	members, err := cl.Members(ctx)
	if err != nil {
		return nil, fmt.Errorf("error while discovering the cluster members: %w", err)
	}
	for _, member := range members {
		cl.client.Get(member.Name)
	}

	// Hash function is required to target primary owners instead of random cluster members.
	partitions.SetHashFunc(cc.hasher)

	// Initial fetch. ClusterClient targets the primary owners for a smooth and quick operation.
	if err := cl.fetchRoutingTable(); err != nil {
		return nil, err
	}

	// Refresh the routing table in every 15 seconds.
	cl.wg.Add(1)
	go cl.fetchRoutingTablePeriodically()

	return cl, nil
}

var (
	_ Client = (*ClusterClient)(nil)
	_ DMap   = (*ClusterDMap)(nil)
)
