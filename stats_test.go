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
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/pubsub"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/stats"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func resetPubSubStats() {
	pubsub.SubscribersTotal.Reset()
	pubsub.CurrentPSubscribers.Reset()
	pubsub.CurrentSubscribers.Reset()
	pubsub.PSubscribersTotal.Reset()
	pubsub.PublishedTotal.Reset()
}

func TestOlric_Stats(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	c := db.NewEmbeddedClient()
	dm, err := c.NewDMap("mymap")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
	}

	s, err := c.Stats(ctx, db.rt.This().String())
	require.NoError(t, err)

	if s.ClusterCoordinator.ID != db.rt.This().ID {
		t.Fatalf("Expected cluster coordinator: %v. Got: %v", db.rt.This(), s.ClusterCoordinator)
	}

	require.Equal(t, s.Member.Name, db.rt.This().Name)
	require.Equal(t, s.Member.ID, db.rt.This().ID)
	require.Equal(t, s.Member.Birthdate, db.rt.This().Birthdate)
	if s.Runtime != nil {
		t.Error("Runtime stats must not be collected by default:", s.Runtime)
	}

	var total int
	for partID, part := range s.Partitions {
		total += part.Length
		if _, ok := part.DMaps["mymap"]; !ok {
			t.Fatalf("Expected dmap check result is true. Got false")
		}
		if len(part.PreviousOwners) != 0 {
			t.Fatalf("Expected PreviosOwners list is empty. "+
				"Got: %v for PartID: %d", part.PreviousOwners, partID)
		}
		if part.Length <= 0 {
			t.Fatalf("Unexpected Length: %d", part.Length)
		}
	}
	if total != 100 {
		t.Fatalf("Expected total length of partition in stats is 100. Got: %d", total)
	}
	_, ok := s.ClusterMembers[stats.MemberID(db.rt.This().ID)]
	if !ok {
		t.Fatalf("Expected member ID: %d could not be found in ClusterMembers", db.rt.This().ID)
	}
}

func TestOlric_Stats_CollectRuntime(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	s, err := e.Stats(context.Background(), db.rt.This().String(), CollectRuntime())
	require.NoError(t, err)

	if s.Runtime == nil {
		t.Fatal("Runtime stats must be collected by default:", s.Runtime)
	}
}

func TestOlric_Stats_Cluster(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)
	db2 := cluster.addMember(t)

	e := db.NewEmbeddedClient()
	s, err := e.Stats(context.Background(), db2.rt.This().String())
	require.NoError(t, err)
	require.Nil(t, s.Runtime)
	require.Equal(t, s.Member.String(), db2.rt.This().String())
}

func TestStats_PubSub(t *testing.T) {
	resetPubSubStats()

	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	rc := redis.NewClient(&redis.Options{Addr: db.rt.This().String()})
	ctx := context.Background()

	t.Run("Subscribe", func(t *testing.T) {
		defer func() {
			resetPubSubStats()
		}()

		var subscribers []*redis.PubSub
		for i := 0; i < 5; i++ {
			ps := rc.Subscribe(ctx, "my-channel")
			// Wait for confirmation that subscription is created before publishing anything.
			_, err := ps.Receive(ctx)
			require.NoError(t, err)
			subscribers = append(subscribers, ps)
		}

		for i := 0; i < 10; i++ {
			cmd := rc.Publish(ctx, "my-channel", fmt.Sprintf("message-%d", i))
			res, err := cmd.Result()
			require.Equal(t, int64(5), res)
			require.NoError(t, err)
		}
		require.Equal(t, int64(50), pubsub.PublishedTotal.Read())
		require.Equal(t, int64(5), pubsub.SubscribersTotal.Read())
		require.Equal(t, int64(5), pubsub.CurrentSubscribers.Read())
		require.Equal(t, int64(0), pubsub.PSubscribersTotal.Read())
		require.Equal(t, int64(0), pubsub.CurrentPSubscribers.Read())

		// Unsubscribe
		for _, s := range subscribers {
			err := s.Unsubscribe(ctx, "my-channel")
			require.NoError(t, err)
			<-time.After(100 * time.Millisecond)
		}

		require.Equal(t, int64(5), pubsub.SubscribersTotal.Read())
		require.Equal(t, int64(0), pubsub.CurrentSubscribers.Read())
		require.Equal(t, int64(0), pubsub.PSubscribersTotal.Read())
		require.Equal(t, int64(0), pubsub.CurrentPSubscribers.Read())
	})

	t.Run("PSubscribe", func(t *testing.T) {
		defer func() {
			resetPubSubStats()
		}()

		ps := rc.PSubscribe(ctx, "h?llo")
		// Wait for confirmation that subscription is created before publishing anything.
		_, err := ps.Receive(ctx)
		require.NoError(t, err)

		cmd := rc.Publish(ctx, "hxllo", "message")
		res, err := cmd.Result()
		require.Equal(t, int64(1), res)
		require.NoError(t, err)

		require.Equal(t, int64(1), pubsub.PublishedTotal.Read())
		require.Equal(t, int64(1), pubsub.PSubscribersTotal.Read())
		require.Equal(t, int64(1), pubsub.CurrentPSubscribers.Read())

		require.Equal(t, int64(0), pubsub.SubscribersTotal.Read())
		require.Equal(t, int64(0), pubsub.CurrentSubscribers.Read())

		err = ps.PUnsubscribe(ctx, "h?llo")
		require.NoError(t, err)

		<-time.After(100 * time.Millisecond)
		require.Equal(t, int64(1), pubsub.PSubscribersTotal.Read())
		require.Equal(t, int64(0), pubsub.CurrentPSubscribers.Read())
		require.Equal(t, int64(0), pubsub.SubscribersTotal.Read())
		require.Equal(t, int64(0), pubsub.CurrentSubscribers.Read())
	})
}

func TestStats_DMap(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	rc := redis.NewClient(&redis.Options{Addr: db.rt.This().String()})
	ctx := context.Background()

	t.Run("DMap stats without eviction", func(t *testing.T) {
		// EntriesTotal
		for i := 0; i < 10; i++ {
			cmd := protocol.NewPut("mydmap", fmt.Sprintf("mykey-%d", i), []byte("myvalue")).Command(ctx)
			err := rc.Process(ctx, cmd)
			require.NoError(t, err)
			require.NoError(t, cmd.Err())
		}

		// GetHits
		for i := 0; i < 10; i++ {
			cmd := protocol.NewGet("mydmap", fmt.Sprintf("mykey-%d", i)).Command(ctx)
			err := rc.Process(ctx, cmd)
			require.NoError(t, err)
			require.NoError(t, cmd.Err())
		}

		// DeleteHits
		for i := 0; i < 10; i++ {
			cmd := protocol.NewDel("mydmap", fmt.Sprintf("mykey-%d", i)).Command(ctx)
			err := rc.Process(ctx, cmd)
			require.NoError(t, err)
			require.NoError(t, cmd.Err())
		}

		// GetMisses
		for i := 0; i < 10; i++ {
			cmd := protocol.NewGet("mydmap", fmt.Sprintf("mykey-%d", i)).Command(ctx)
			err := rc.Process(ctx, cmd)
			err = protocol.ConvertError(err)
			require.ErrorIs(t, err, dmap.ErrKeyNotFound)
		}

		// DeleteMisses
		for i := 0; i < 10; i++ {
			cmd := protocol.NewDel("mydmap", fmt.Sprintf("mykey-%d", i)).Command(ctx)
			err := rc.Process(ctx, cmd)
			require.NoError(t, err)
			require.NoError(t, cmd.Err())
		}

		require.GreaterOrEqual(t, dmap.EntriesTotal.Read(), int64(10))
		require.GreaterOrEqual(t, dmap.GetMisses.Read(), int64(10))
		require.GreaterOrEqual(t, dmap.GetHits.Read(), int64(10))
		require.GreaterOrEqual(t, dmap.DeleteHits.Read(), int64(10))
		require.GreaterOrEqual(t, dmap.DeleteMisses.Read(), int64(10))
	})

	t.Run("DMap eviction stats", func(t *testing.T) {
		// EntriesTotal, EvictedTotal
		for i := 0; i < 10; i++ {
			cmd := protocol.
				NewPut("mydmap", fmt.Sprintf("mykey-%d", i), []byte("myvalue")).
				SetPX(time.Millisecond.Milliseconds()).
				Command(ctx)
			err := rc.Process(ctx, cmd)
			require.NoError(t, err)
			require.NoError(t, cmd.Err())
		}
		<-time.After(100 * time.Millisecond)

		// GetMisses
		for i := 0; i < 10; i++ {
			cmd := protocol.NewGet("mydmap", "mykey").Command(ctx)
			err := rc.Process(ctx, cmd)
			err = protocol.ConvertError(err)
			require.ErrorIs(t, err, dmap.ErrKeyNotFound)
		}

		require.Greater(t, dmap.DeleteHits.Read(), int64(0))
		require.Greater(t, dmap.EvictedTotal.Read(), int64(0))
		require.GreaterOrEqual(t, dmap.EntriesTotal.Read(), int64(10))
	})
}
