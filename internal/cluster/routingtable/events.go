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
	"github.com/buraksezer/olric/events"
	"github.com/buraksezer/olric/internal/discovery"
	"time"
)

func (r *RoutingTable) publishNodeJoinEvent(m *discovery.Member) {
	defer r.wg.Done()

	rc := r.client.Get(r.this.String())
	message := events.NodeJoinEvent{
		Kind:      events.KindNodeJoinEvent,
		Source:    r.this.String(),
		NodeJoin:  m.String(),
		Timestamp: time.Now().UnixNano(),
	}
	data, err := message.Encode()
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to encode NodeJoinEvent: %v", err)
		return
	}
	err = rc.Publish(r.ctx, events.ClusterEventsChannel, data).Err()
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to publish NodeJoinEvent to %s: %v", events.ClusterEventsChannel, err)
	}
}

func (r *RoutingTable) publishNodeLeftEvent(m *discovery.Member) {
	defer r.wg.Done()

	rc := r.client.Get(r.this.String())
	message := events.NodeLeftEvent{
		Kind:      events.KindNodeLeftEvent,
		Source:    r.this.String(),
		NodeLeft:  m.String(),
		Timestamp: time.Now().UnixNano(),
	}
	data, err := message.Encode()
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to encode NodeLeftEvent: %v", err)
		return
	}
	err = rc.Publish(r.ctx, events.ClusterEventsChannel, data).Err()
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to publish NodeLeftEvent to %s: %v", events.ClusterEventsChannel, err)
	}
}
