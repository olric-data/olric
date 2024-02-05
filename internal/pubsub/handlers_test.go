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

package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestPubSub_Handler_Subscribe(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()
	ps := rc.Subscribe(ctx, "my-channel")

	// Wait for confirmation that subscription is created before publishing anything.
	msgi, err := ps.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	subs := msgi.(*redis.Subscription)
	require.Equal(t, "subscribe", subs.Kind)
	require.Equal(t, "my-channel", subs.Channel)
	require.Equal(t, 1, subs.Count)

	// Go channel which receives messages.
	ch := ps.Channel()

	expected := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("my-message-%d", i)
		err = rc.Publish(ctx, "my-channel", msg).Err()
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

func TestPubSub_Handler_Unsubscribe(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()
	ps := rc.Subscribe(ctx, "my-channel")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := ps.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	// Go channel which receives messages.
	ch := ps.Channel()

	err = ps.Unsubscribe(ctx, "my-channel")
	require.NoError(t, err)

	// Wait for some time. Because the Redis client doesn't wait for the response after
	// writing 'unsubscribe' command.
	<-time.After(250 * time.Millisecond)

	err = rc.Publish(ctx, "my-channel", "hello, world!").Err()
	require.NoError(t, err)
L:
	for {
		select {
		case <-ch:
			require.Fail(t, "Received a message from an unsubscribed channel")
		case <-time.After(250 * time.Millisecond):
			// Enough. Break it and check the consumed items.
			break L
		}
	}
}

func TestPubSub_Handler_PSubscribe(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()
	ps := rc.PSubscribe(ctx, "h?llo")

	// Wait for confirmation that subscription is created before publishing anything.
	msgi, err := ps.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	subs := msgi.(*redis.Subscription)
	require.Equal(t, "psubscribe", subs.Kind)
	require.Equal(t, "h?llo", subs.Channel)
	require.Equal(t, 1, subs.Count)

	// Go channel which receives messages.
	ch := ps.Channel()

	expected := make(map[string]struct{})
	for _, channel := range []string{"hello", "hallo", "hxllo"} {
		for i := 0; i < 10; i++ {
			msg := fmt.Sprintf("my-message-%s-%d", channel, i)
			err = rc.Publish(ctx, channel, msg).Err()
			require.NoError(t, err)
			expected[msg] = struct{}{}
		}
	}

	consumed := make(map[string]struct{})
L:
	for {
		select {
		case msg := <-ch:
			consumed[msg.Payload] = struct{}{}
			if len(consumed) == 30 {
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

func TestPubSub_Handler_PUnsubscribe(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()
	ps := rc.PSubscribe(ctx, "h?llo")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := ps.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	// Go channel which receives messages.
	ch := ps.Channel()

	err = ps.PUnsubscribe(ctx, "h?llo")
	require.NoError(t, err)

	// Wait for some time. Because the Redis client doesn't wait for the response after
	// writing 'unsubscribe' command.
	<-time.After(250 * time.Millisecond)

	for _, channel := range []string{"hello", "hallo", "hxllo"} {
		err = rc.Publish(ctx, channel, "hello, world!").Err()
		require.NoError(t, err)
	}

L:
	for {
		select {
		case <-ch:
			require.Fail(t, "Received a message from an unsubscribed channel")
		case <-time.After(250 * time.Millisecond):
			// Enough. Break it and check the consumed items.
			break L
		}
	}
}

func TestPubSub_Handler_Ping(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()
	ps := rc.Subscribe(ctx, "my-channel")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := ps.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	err = ps.Ping(ctx, "hello, world!")
	require.NoError(t, err)

	msg, err := ps.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)
	require.Equal(t, "Pong<hello, world!>", msg.(*redis.Pong).String())
}

func TestPubSub_Handler_Close(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()
	ps := rc.Subscribe(ctx, "my-channel")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := ps.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	err = ps.Close()
	require.NoError(t, err)

	err = ps.Ping(ctx)
	require.Error(t, err, "redis: client is closed")
	//TODO: Control active subscriber count
}

func TestPubSub_Handler_PubSubChannels_Without_Patterns(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()
	channels := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		channel := fmt.Sprintf("my-channel-%d", i)
		ps := rc.Subscribe(ctx, channel)
		// Wait for confirmation that subscription is created before publishing anything.
		_, err := ps.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)
		channels[channel] = struct{}{}
	}

	res := rc.PubSubChannels(ctx, "")
	result, err := res.Result()
	require.NoError(t, err)
	require.Len(t, result, len(channels))

	for _, channel := range result {
		require.Contains(t, channels, channel)
	}
}

func TestPubSub_Handler_PubSubChannels_With_Patterns(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()

	channels := make(map[string]struct{})
	for _, channel := range []string{"hello-1", "hello-2", "hello-3", "foobar"} {
		ps := rc.Subscribe(ctx, channel)
		// Wait for confirmation that subscription is created before publishing anything.
		_, err := ps.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)
		channels[channel] = struct{}{}
	}

	res := rc.PubSubChannels(ctx, "h*")
	result, err := res.Result()
	require.NoError(t, err)
	require.Len(t, result, len(channels)-1)
	require.NotContains(t, result, "foobar")

	for _, channel := range result {
		require.Contains(t, channels, channel)
	}
}

func TestPubSub_Handler_PubSubNumpat(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()

	for _, channel := range []string{"h*llo", "f*bar"} {
		ps := rc.PSubscribe(ctx, channel)
		// Wait for confirmation that subscription is created before publishing anything.
		_, err := ps.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)
	}

	res := rc.PubSubNumPat(ctx)
	nr, err := res.Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), nr)
}

func TestPubSub_Handler_PubSubNumsub(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())
	ctx := context.Background()

	for _, channel := range []string{"hello", "hello", "foobar", "barfoo"} {
		ps := rc.Subscribe(ctx, channel)
		// Wait for confirmation that subscription is created before publishing anything.
		_, err := ps.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)
	}

	res := rc.PubSubNumSub(ctx, "hello", "foobar", "barfoo")
	nr, err := res.Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), nr["hello"])
	require.Equal(t, int64(1), nr["foobar"])
	require.Equal(t, int64(1), nr["barfoo"])
}

func TestPubSub_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc1 := s1.client.Get(s1.rt.This().String())
	ctx := context.Background()
	ps := rc1.Subscribe(ctx, "my-channel")

	// Wait for confirmation that subscription is created before publishing anything.
	msgi, err := ps.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	subs := msgi.(*redis.Subscription)
	require.Equal(t, "subscribe", subs.Kind)
	require.Equal(t, "my-channel", subs.Channel)
	require.Equal(t, 1, subs.Count)

	// Go channel which receives messages.
	ch := ps.Channel()

	rc2 := s2.client.Get(s2.rt.This().String())
	expected := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("my-message-%d", i)
		err = rc2.Publish(ctx, "my-channel", msg).Err()
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
