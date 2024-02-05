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
	"encoding/json"
	"testing"
	"time"

	"github.com/buraksezer/olric/events"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestRoutingTable_publishNodeJoinEvent(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rt.server.ServeMux().HandleFunc(protocol.PubSub.Publish, func(conn redcon.Conn, cmd redcon.Command) {
		defer cancel()

		publishCmd, err := protocol.ParsePublishCommand(cmd)
		require.NoError(t, err)
		require.Equal(t, events.ClusterEventsChannel, publishCmd.Channel)

		v := events.NodeJoinEvent{}
		err = json.Unmarshal([]byte(publishCmd.Message), &v)
		require.NoError(t, err)
		require.Equal(t, events.KindNodeJoinEvent, v.Kind)
		require.Equal(t, rt.this.String(), v.Source)
		require.Equal(t, rt.this.String(), v.NodeJoin)

		conn.WriteInt(1)
	})

	m := discovery.NewMember(c)
	rt.wg.Add(1)
	go rt.publishNodeJoinEvent(&m)
	<-ctx.Done()
	require.ErrorIs(t, context.Canceled, ctx.Err())
}

func TestRoutingTable_publishNodeLeftEvent(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rt.server.ServeMux().HandleFunc(protocol.PubSub.Publish, func(conn redcon.Conn, cmd redcon.Command) {
		defer cancel()
		publishCmd, err := protocol.ParsePublishCommand(cmd)
		require.NoError(t, err)
		require.Equal(t, events.ClusterEventsChannel, publishCmd.Channel)

		v := events.NodeLeftEvent{}
		err = json.Unmarshal([]byte(publishCmd.Message), &v)
		require.NoError(t, err)
		require.Equal(t, events.KindNodeLeftEvent, v.Kind)
		require.Equal(t, rt.this.String(), v.Source)
		require.Equal(t, rt.this.String(), v.NodeLeft)

		conn.WriteInt(1)
	})

	m := discovery.NewMember(c)
	rt.wg.Add(1)
	go rt.publishNodeLeftEvent(&m)
	<-ctx.Done()
	require.ErrorIs(t, context.Canceled, ctx.Err())
}
