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

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func pubsubTestRunner(t *testing.T, ps *PubSub, kind, channel string) {
	ctx := context.Background()
	var rp *redis.PubSub
	switch kind {
	case "subscribe":
		rp = ps.Subscribe(ctx, channel)
	case "psubscribe":
		rp = ps.PSubscribe(ctx, channel)
	}

	defer func() {
		require.NoError(t, rp.Close())
	}()

	// Wait for confirmation that subscription is created before publishing anything.
	msgi, err := rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	subs := msgi.(*redis.Subscription)
	require.Equal(t, kind, subs.Kind)
	require.Equal(t, channel, subs.Channel)
	require.Equal(t, 1, subs.Count)

	// Go channel which receives messages.
	ch := rp.Channel()

	expected := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("my-message-%d", i)
		count, err := ps.Publish(ctx, "my-channel", msg)
		require.Equal(t, int64(1), count)
		require.NoError(t, err)
		expected[msg] = struct{}{}
	}

	consumed := make(map[string]struct{})
L:
	for {
		select {
		case msg := <-ch:
			require.Equal(t, "my-channel", msg.Channel)
			consumed[msg.Payload] = struct{}{}
			if len(consumed) == 10 {
				// It would be OK
				break L
			}
		case <-time.After(5 * time.Second):
			// Enough. Break it and check the consumed items.
			break L
		}
	}

	require.Equal(t, expected, consumed)
}

func TestPubSub_Publish_Subscribe(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
	require.NoError(t, err)

	pubsubTestRunner(t, ps, "subscribe", "my-channel")
}

func TestPubSub_Publish_PSubscribe(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
	require.NoError(t, err)
	pubsubTestRunner(t, ps, "psubscribe", "my-*")
}

func TestPubSub_PubSubChannels(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
	require.NoError(t, err)

	rp := ps.Subscribe(ctx, "my-channel")

	defer func() {
		require.NoError(t, rp.Close())
	}()

	// Wait for confirmation that subscription is created before publishing anything.
	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	channels, err := ps.PubSubChannels(ctx, "my-*")
	require.NoError(t, err)

	require.Equal(t, []string{"my-channel"}, channels)
}

func TestPubSub_PubSubNumSub(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
	require.NoError(t, err)

	rp := ps.Subscribe(ctx, "my-channel")

	defer func() {
		require.NoError(t, rp.Close())
	}()

	// Wait for confirmation that subscription is created before publishing anything.
	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	numsub, err := ps.PubSubNumSub(ctx, "my-channel", "foobar")
	require.NoError(t, err)

	expected := map[string]int64{
		"foobar":     0,
		"my-channel": 1,
	}
	require.Equal(t, expected, numsub)
}

func TestPubSub_PubSubNumPat(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
	require.NoError(t, err)

	rp := ps.PSubscribe(ctx, "my-*")

	defer func() {
		require.NoError(t, rp.Close())
	}()

	// Wait for confirmation that subscription is created before publishing anything.
	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	numpat, err := ps.PubSubNumPat(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), numpat)
}

func TestPubSub_Cluster(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db1 := cluster.addMember(t)
	db2 := cluster.addMember(t)

	// Create a subscriber
	ctx := context.Background()
	c, err := NewClusterClient([]string{db1.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	ps1, err := c.NewPubSub(ToAddress(db1.rt.This().String()))
	require.NoError(t, err)

	rp := ps1.Subscribe(ctx, "my-channel")
	defer func() {
		require.NoError(t, rp.Close())
	}()
	// Wait for confirmation that subscription is created before publishing anything.
	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)
	receiveChan := rp.Channel()

	// Create a publisher

	e := db2.NewEmbeddedClient()
	ps2, err := e.NewPubSub(ToAddress(db2.rt.This().String()))
	require.NoError(t, err)
	expected := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("my-message-%d", i)
		count, err := ps2.Publish(ctx, "my-channel", msg)
		require.Equal(t, int64(1), count)
		require.NoError(t, err)
		expected[msg] = struct{}{}
	}

	consumed := make(map[string]struct{})
L:
	for {
		select {
		case msg := <-receiveChan:
			require.Equal(t, "my-channel", msg.Channel)
			consumed[msg.Payload] = struct{}{}
			if len(consumed) == 10 {
				// It would be OK
				break L
			}
		case <-time.After(5 * time.Second):
			// Enough. Break it and check the consumed items.
			break L
		}
	}
}
