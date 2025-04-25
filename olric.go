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

/*
Package olric provides a distributed cache and in-memory key/value data store.
It can be used both as an embedded Go library and as a language-independent
service.

With Olric, you can instantly create a fast, scalable, shared pool of RAM across
a cluster of computers.

Olric is designed to be a distributed cache. But it also provides Publish/Subscribe,
data replication, failure detection and simple anti-entropy services.
So it can be used as an ordinary key/value data store to scale your cloud
application.
*/
package olric

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/logutils"
	"github.com/olric-data/olric/config"
	"github.com/olric-data/olric/hasher"
	"github.com/olric-data/olric/internal/checkpoint"
	"github.com/olric-data/olric/internal/cluster/balancer"
	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/cluster/routingtable"
	"github.com/olric-data/olric/internal/dmap"
	"github.com/olric-data/olric/internal/environment"
	"github.com/olric-data/olric/internal/locker"
	"github.com/olric-data/olric/internal/protocol"
	"github.com/olric-data/olric/internal/pubsub"
	"github.com/olric-data/olric/internal/server"
	"github.com/olric-data/olric/pkg/flog"
	"github.com/pkg/errors"
	"github.com/tidwall/redcon"
	"golang.org/x/sync/errgroup"
)

// ReleaseVersion is the current stable version of Olric
const ReleaseVersion string = "0.6.1"

var (
	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")

	// ErrServerGone means that a cluster member is closed unexpectedly.
	ErrServerGone = errors.New("server is gone")

	// ErrKeyNotFound means that returned when a key could not be found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyFound means that the requested key found in the cluster.
	ErrKeyFound = errors.New("key found")

	// ErrWriteQuorum means that write quorum cannot be reached to operate.
	ErrWriteQuorum = errors.New("write quorum cannot be reached")

	// ErrReadQuorum means that read quorum cannot be reached to operate.
	ErrReadQuorum = errors.New("read quorum cannot be reached")

	// ErrLockNotAcquired is returned when the requested lock could not be acquired
	ErrLockNotAcquired = errors.New("lock not acquired")

	// ErrNoSuchLock is returned when the requested lock does not exist
	ErrNoSuchLock = errors.New("no such lock")

	// ErrClusterQuorum means that the cluster could not reach a healthy numbers of members to operate.
	ErrClusterQuorum = errors.New("failed to find enough peers to create quorum")

	// ErrKeyTooLarge means that the given key is too large to process.
	// Maximum length of a key is 256 bytes.
	ErrKeyTooLarge = errors.New("key too large")

	// ErrEntryTooLarge returned if the required space for an entry is bigger than table size.
	ErrEntryTooLarge = errors.New("entry too large for the configured table size")

	// ErrConnRefused returned if the target node refused a connection request.
	// It is good to call RefreshMetadata to update the underlying data structures.
	ErrConnRefused = errors.New("connection refused")

	ErrWrongPass = errors.New("invalid username-password pair or user is disabled")
)

// Olric implements a distributed cache and in-memory key/value data store.
// It can be used both as an embedded Go library and as a language-independent
// service.
type Olric struct {
	// name is BindAddr:BindPort. It defines servers unique name in the cluster.
	name     string
	env      *environment.Environment
	config   *config.Config
	log      *flog.Logger
	hashFunc hasher.Hasher

	// Logical units to store data
	primary *partitions.Partitions
	backup  *partitions.Partitions

	// RESP server and clients.
	server *server.Server
	client *server.Client

	rt       *routingtable.RoutingTable
	balancer *balancer.Balancer

	pubsub *pubsub.Service
	dmap   *dmap.Service

	// Structures for flow control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Callback function. Olric calls this after
	// the server is ready to accept new connections.
	started func()
}

func prepareConfig(c *config.Config) (*config.Config, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	err := c.Sanitize()
	if err != nil {
		return nil, err
	}

	err = c.Validate()
	if err != nil {
		return nil, err
	}

	err = c.SetupNetworkConfig()
	if err != nil {
		return nil, err
	}
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr,
		strconv.Itoa(c.BindPort))

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"},
		MinLevel: logutils.LogLevel(strings.ToUpper(c.LogLevel)),
		Writer:   c.Logger.Writer(),
	}
	c.Logger.SetOutput(filter)

	return c, nil
}

func initializeServices(db *Olric) error {
	db.rt = routingtable.New(db.env)
	db.env.Set("routingtable", db.rt)

	db.balancer = balancer.New(db.env)

	// Add Services
	dt, err := pubsub.NewService(db.env)
	if err != nil {
		return err
	}
	db.pubsub = dt.(*pubsub.Service)

	dm, err := dmap.NewService(db.env)
	if err != nil {
		return err
	}
	db.dmap = dm.(*dmap.Service)

	return nil
}

// New creates a new Olric instance, otherwise returns an error.
func New(c *config.Config) (*Olric, error) {
	var err error
	c, err = prepareConfig(c)
	if err != nil {
		return nil, err
	}

	e := environment.New()
	e.Set("config", c)

	// Set the hash function. Olric distributes keys over partitions by hashing.
	partitions.SetHashFunc(c.Hasher)

	flogger := flog.New(c.Logger)
	flogger.SetLevel(c.LogVerbosity)
	if c.LogLevel == "DEBUG" {
		flogger.ShowLineNumber(1)
	}
	e.Set("logger", flogger)

	client := server.NewClient(c.Client)
	e.Set("client", client)
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("locker", locker.New())
	ctx, cancel := context.WithCancel(context.Background())
	db := &Olric{
		name:     c.MemberlistConfig.Name,
		env:      e,
		log:      flogger,
		config:   c,
		hashFunc: c.Hasher,
		client:   client,
		primary:  e.Get("primary").(*partitions.Partitions),
		backup:   e.Get("backup").(*partitions.Partitions),
		started:  c.Started,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Create a Redcon server instance
	rc := &server.Config{
		BindAddr:        c.BindAddr,
		BindPort:        c.BindPort,
		KeepAlivePeriod: c.KeepAlivePeriod,
		RequireAuth:     c.Authentication.Enabled,
	}
	srv := server.New(rc, flogger)
	srv.SetPreConditionFunc(db.preconditionFunc)

	db.server = srv
	e.Set("server", srv)

	err = initializeServices(db)
	if err != nil {
		return nil, err
	}

	db.registerCommandHandlers()
	registerErrors()

	return db, nil
}

func (db *Olric) preconditionFunc(conn redcon.Conn, _ redcon.Command) bool {
	err := db.isOperable()
	if err != nil {
		protocol.WriteError(conn, err)
		return false
	}
	return true
}

func (db *Olric) registerCommandHandlers() {
	db.server.ServeMux().HandleFunc(protocol.Generic.Ping, db.pingCommandHandler)
	db.server.ServeMux().HandleFunc(protocol.Cluster.RoutingTable, db.clusterRoutingTableCommandHandler)
	db.server.ServeMux().HandleFunc(protocol.Generic.Stats, db.statsCommandHandler)
	db.server.ServeMux().HandleFunc(protocol.Cluster.Members, db.clusterMembersCommandHandler)
	db.server.ServeMux().HandleFunc(protocol.Generic.Auth, db.authCommandHandler)
}

// callStartedCallback checks passed checkpoint count and calls the callback
// function.
func (db *Olric) callStartedCallback() {
	defer db.wg.Done()

	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()

	for {
		timer.Reset(10 * time.Millisecond)
		select {
		case <-timer.C:
			if checkpoint.AllPassed() {
				if db.started != nil {
					db.started()
				}
				return
			}
		case <-db.ctx.Done():
			return
		}
	}
}

func convertClusterError(err error) error {
	switch {
	case errors.Is(err, routingtable.ErrClusterQuorum):
		return ErrClusterQuorum
	case errors.Is(err, routingtable.ErrServerGone):
		return ErrServerGone
	case errors.Is(err, routingtable.ErrOperationTimeout):
		return ErrOperationTimeout
	default:
		return err
	}
}

// isOperable controls bootstrapping status and cluster quorum to prevent split-brain syndrome.
func (db *Olric) isOperable() error {
	if err := db.rt.CheckMemberCountQuorum(); err != nil {
		return convertClusterError(err)
	}
	// An Olric node has to be bootstrapped to function properly.
	return db.rt.CheckBootstrap()
}

// Start starts background servers and joins the cluster. You still must call Shutdown
// method if Start function returns an early error.
func (db *Olric) Start() error {
	db.log.V(1).Printf("[INFO] Olric %s on %s/%s %s", ReleaseVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())

	// This error group is responsible to run the TCP server at background and report errors.
	errGr, ctx := errgroup.WithContext(context.Background())
	errGr.Go(func() error {
		return db.server.ListenAndServe()
	})

	select {
	case <-db.server.StartedCtx.Done():
		// TCP server has been started
	case <-ctx.Done():
		// TCP server could not be started due to an error. There is no need to run
		// Olric.Shutdown here because we could not start anything.
		return errGr.Wait()
	}

	// Balancer works periodically to balance partition data across the cluster.
	if err := db.balancer.Start(); err != nil {
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to run the balancer subsystem: %v", err)
		}
		return err
	}

	// First, we need to join the cluster. Then, the routing table has been started.
	if err := db.rt.Join(); err != nil {
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to join the Olric cluster: %v", err)
		}
		return err
	}
	// Start routing table service and member discovery subsystem.
	if err := db.rt.Start(); err != nil {
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to run the routing table subsystem: %v", err)
		}
		return err
	}

	// Start publish-subscribe service
	if err := db.pubsub.Start(); err != nil {
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to run the Publish-Subscribe service: %v", err)
		}
		return err
	}

	// Start distributed map service
	if err := db.dmap.Start(); err != nil {
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to run the Distributed Map service: %v", err)
		}
		return err
	}

	// Warn the user about his/her choice of configuration
	if db.config.ReplicationMode == config.AsyncReplicationMode && db.config.WriteQuorum > 1 {
		db.log.V(2).
			Printf("[WARN] Olric is running in async replication mode. WriteQuorum (%d) is ineffective",
				db.config.WriteQuorum)
	}

	if db.started != nil {
		db.wg.Add(1)
		go db.callStartedCallback()
	}

	db.log.V(2).Printf("[INFO] Node name in the cluster: %s",
		db.name)
	if db.config.Interface != "" {
		db.log.V(2).Printf("[INFO] Olric uses interface: %s",
			db.config.Interface)
	}
	db.log.V(2).Printf("[INFO] Olric bindAddr: %s, bindPort: %d",
		db.config.BindAddr, db.config.BindPort)
	db.log.V(2).Printf("[INFO] Replication count is %d", db.config.ReplicaCount)

	// Wait for the TCP server.
	return errGr.Wait()
}

// Shutdown stops background servers and leaves the cluster.
func (db *Olric) Shutdown(ctx context.Context) error {
	select {
	case <-db.ctx.Done():
		// Shutdown only once.
		return nil
	default:
	}

	db.cancel()

	var latestError error

	if err := db.pubsub.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown PubSub service: %v", err)
		latestError = err
	}

	if err := db.dmap.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown DMap service: %v", err)
		latestError = err
	}

	if err := db.balancer.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown balancer service: %v", err)
		latestError = err
	}

	if err := db.rt.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown routing table service: %v", err)
		latestError = err
	}

	// Shutdown Redcon server
	if err := db.server.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown RESP server: %v", err)
		latestError = err
	}

	done := make(chan struct{})
	go func() {
		defer func() {
			close(done)
		}()
		db.wg.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}

	// db.name will be shown as empty string, if the program is killed before
	// bootstrapping.
	db.log.V(2).Printf("[INFO] %s is gone", db.name)
	return latestError
}

func convertDMapError(err error) error {
	switch {
	case errors.Is(err, dmap.ErrKeyFound):
		return ErrKeyFound
	case errors.Is(err, dmap.ErrKeyNotFound):
		return ErrKeyNotFound
	case errors.Is(err, dmap.ErrDMapNotFound):
		return ErrKeyNotFound
	case errors.Is(err, dmap.ErrLockNotAcquired):
		return ErrLockNotAcquired
	case errors.Is(err, dmap.ErrNoSuchLock):
		return ErrNoSuchLock
	case errors.Is(err, dmap.ErrReadQuorum):
		return ErrReadQuorum
	case errors.Is(err, dmap.ErrWriteQuorum):
		return ErrWriteQuorum
	case errors.Is(err, dmap.ErrServerGone):
		return ErrServerGone
	case errors.Is(err, dmap.ErrKeyTooLarge):
		return ErrKeyTooLarge
	case errors.Is(err, dmap.ErrEntryTooLarge):
		return ErrEntryTooLarge
	default:
		return convertClusterError(err)
	}
}

func registerErrors() {
	protocol.SetError("WRONGPASS", ErrWrongPass)
}
