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

/*Package discovery provides a basic memberlist integration.*/
package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"plugin"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/stats"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/buraksezer/olric/pkg/service_discovery"
	"github.com/hashicorp/memberlist"
)

const eventChanCapacity = 256

// UptimeSeconds is number of seconds since the server started.
var UptimeSeconds = stats.NewInt64Counter()

// ErrMemberNotFound indicates that the requested member could not be found in the member list.
var ErrMemberNotFound = errors.New("member not found")

// ClusterEvent is a single event related to node activity in the memberlist.
// The Node member of this struct must not be directly modified.
type ClusterEvent struct {
	Event    memberlist.NodeEventType
	NodeName string
	NodeAddr net.IP
	NodePort uint16
	NodeMeta []byte // Metadata from the delegate for this node.
}

func (c *ClusterEvent) MemberAddr() string {
	port := strconv.Itoa(int(c.NodePort))
	return net.JoinHostPort(c.NodeAddr.String(), port)
}

// Discovery is a structure that encapsulates memberlist and
// provides useful functions to utilize it.
type Discovery struct {
	log        *flog.Logger
	member     *Member
	memberlist *memberlist.Memberlist
	config     *config.Config

	// To manage Join/Leave/Update events
	clusterEventsMtx sync.RWMutex
	ClusterEvents    chan *ClusterEvent

	// Try to reconnect dead members
	eventSubscribers []chan *ClusterEvent
	serviceDiscovery service_discovery.ServiceDiscovery

	// Flow control
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a new memberlist with a proper configuration and returns a new Discovery instance along with it.
func New(log *flog.Logger, c *config.Config) *Discovery {
	member := NewMember(c)
	ctx, cancel := context.WithCancel(context.Background())
	d := &Discovery{
		member: &member,
		config: c,
		log:    log,
		ctx:    ctx,
		cancel: cancel,
	}
	return d
}

func (d *Discovery) loadServiceDiscoveryPlugin() error {
	var sd service_discovery.ServiceDiscovery

	if val, ok := d.config.ServiceDiscovery["plugin"]; ok {
		if sd, ok = val.(service_discovery.ServiceDiscovery); !ok {
			return fmt.Errorf("plugin type %T is not a ServiceDiscovery interface", val)
		}
	} else {
		pluginPath, ok := d.config.ServiceDiscovery["path"]
		if !ok {
			return fmt.Errorf("plugin path could not be found")
		}
		plug, err := plugin.Open(pluginPath.(string))
		if err != nil {
			return fmt.Errorf("failed to open plugin: %w", err)
		}

		symDiscovery, err := plug.Lookup("ServiceDiscovery")
		if err != nil {
			return fmt.Errorf("failed to lookup serviceDiscovery symbol: %w", err)
		}

		if sd, ok = symDiscovery.(service_discovery.ServiceDiscovery); !ok {
			return fmt.Errorf("unable to assert type to serviceDiscovery")
		}
	}

	if err := sd.SetConfig(d.config.ServiceDiscovery); err != nil {
		return err
	}
	sd.SetLogger(d.config.Logger)
	if err := sd.Initialize(); err != nil {
		return err
	}

	d.serviceDiscovery = sd
	return nil
}

// increaseUptimeSeconds calls UptimeSeconds.Increase function every second.
func (d *Discovery) increaseUptimeSeconds() {
	defer d.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			UptimeSeconds.Increase(1)
		case <-d.ctx.Done():
			return
		}
	}
}

func (d *Discovery) Start() error {
	if d.config.ServiceDiscovery != nil {
		if err := d.loadServiceDiscoveryPlugin(); err != nil {
			return err
		}
	}
	// ClusterEvents chan is consumed by the Olric package to maintain a consistent hash ring.
	d.ClusterEvents = d.SubscribeNodeEvents()

	// Initialize a new memberlist
	dl, err := d.newDelegate()
	if err != nil {
		return err
	}
	eventsCh := make(chan memberlist.NodeEvent, eventChanCapacity)
	d.config.MemberlistConfig.Delegate = dl
	d.config.MemberlistConfig.Logger = d.config.Logger
	d.config.MemberlistConfig.Events = &memberlist.ChannelEventDelegate{
		Ch: eventsCh,
	}
	list, err := memberlist.Create(d.config.MemberlistConfig)
	if err != nil {
		return err
	}
	d.memberlist = list

	if d.serviceDiscovery != nil {
		if err := d.serviceDiscovery.Register(); err != nil {
			return err
		}
	}

	d.wg.Add(1)
	go d.eventLoop(eventsCh)

	d.wg.Add(1)
	go d.increaseUptimeSeconds()

	return nil
}

// Join is used to take an existing Memberlist and attempt to Join a cluster
// by contacting all the given hosts and performing a state sync. Initially,
// the Memberlist only contains our own state, so doing this will cause remote
// nodes to become aware of the existence of this node, effectively joining the cluster.
func (d *Discovery) Join() (int, error) {
	if d.serviceDiscovery != nil {
		peers, err := d.serviceDiscovery.DiscoverPeers()
		if err != nil {
			return 0, err
		}
		return d.memberlist.Join(peers)
	}
	return d.memberlist.Join(d.config.Peers)
}

func (d *Discovery) Rejoin(peers []string) (int, error) {
	return d.memberlist.Join(peers)
}

// GetMembers returns a full list of known alive nodes.
func (d *Discovery) GetMembers() []Member {
	var members []Member
	nodes := d.memberlist.Members()
	for _, node := range nodes {
		member, _ := NewMemberFromMetadata(node.Meta)
		members = append(members, member)
	}

	// sort members by birthdate
	sort.Slice(members, func(i int, j int) bool {
		return members[i].Birthdate < members[j].Birthdate
	})
	return members
}

func (d *Discovery) NumMembers() int {
	return d.memberlist.NumMembers()
}

// FindMemberByName finds and returns an alive member.
func (d *Discovery) FindMemberByName(name string) (Member, error) {
	members := d.GetMembers()
	for _, member := range members {
		if member.Name == name {
			return member, nil
		}
	}
	return Member{}, ErrMemberNotFound
}

// FindMemberByID finds and returns an alive member.
func (d *Discovery) FindMemberByID(id uint64) (Member, error) {
	members := d.GetMembers()
	for _, member := range members {
		if member.ID == id {
			return member, nil
		}
	}
	return Member{}, ErrMemberNotFound
}

// GetCoordinator returns the oldest node in the memberlist.
func (d *Discovery) GetCoordinator() Member {
	members := d.GetMembers()
	if len(members) == 0 {
		d.log.V(1).Printf("[ERROR] There is no member in memberlist")
		return Member{}
	}
	return members[0]
}

// IsCoordinator returns true if the caller is the coordinator node.
func (d *Discovery) IsCoordinator() bool {
	return d.GetCoordinator().ID == d.member.ID
}

// LocalNode is used to return the local Node
func (d *Discovery) LocalNode() *memberlist.Node {
	return d.memberlist.LocalNode()
}

// Shutdown will stop any background maintenance of network activity
// for this memberlist, causing it to appear "dead". A leave message
// will not be broadcasted prior, so the cluster being left will have
// to detect this node's Shutdown using probing. If you wish to more
// gracefully exit the cluster, call Leave prior to shutting down.
//
// This method is safe to call multiple times.
func (d *Discovery) Shutdown() error {
	select {
	case <-d.ctx.Done():
		return nil
	default:
	}
	d.cancel()
	// We don't do that in a goroutine with a timeout mechanism
	// because this mechanism may cause goroutine leak.
	d.wg.Wait()

	if d.memberlist != nil {
		// Leave will broadcast a leave message but will not shutdown the background
		// listeners, meaning the node will continue participating in gossip and state
		// updates.
		d.log.V(2).Printf("[INFO] Broadcasting a leave message")
		if err := d.memberlist.Leave(d.config.LeaveTimeout); err != nil {
			d.log.V(3).Printf("[WARN] memberlist.Leave returned an error: %v", err)
		}
	}

	if d.serviceDiscovery != nil {
		defer func(serviceDiscovery service_discovery.ServiceDiscovery) {
			err := serviceDiscovery.Close()
			if err != nil {
				d.log.V(3).Printf("[ERROR] ServiceDiscovery.Close returned an error: %v", err)
			}
		}(d.serviceDiscovery)

		if err := d.serviceDiscovery.Deregister(); err != nil {
			d.log.V(3).Printf("[ERROR] ServiceDiscovery.Deregister returned an error: %v", err)
		}
	}

	if d.memberlist != nil {
		return d.memberlist.Shutdown()
	}
	return nil
}
