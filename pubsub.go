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
	"strings"

	"github.com/buraksezer/olric/internal/server"
	"github.com/redis/go-redis/v9"
)

type PubSub struct {
	config *pubsubConfig
	rc     *redis.Client
	client *server.Client
}

func newPubSub(client *server.Client, options ...PubSubOption) (*PubSub, error) {
	var (
		err error
		rc  *redis.Client
		pc  pubsubConfig
	)
	for _, opt := range options {
		opt(&pc)
	}

	addr := strings.Trim(pc.Address, " ")
	if addr != "" {
		rc = client.Get(addr)
	} else {
		rc, err = client.Pick()
		if err != nil {
			return nil, err
		}
	}

	return &PubSub{
		config: &pc,
		rc:     rc,
		client: client,
	}, nil
}

func (ps *PubSub) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return ps.rc.Subscribe(ctx, channels...)
}

func (ps *PubSub) PSubscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return ps.rc.PSubscribe(ctx, channels...)
}

func (ps *PubSub) Publish(ctx context.Context, channel string, message interface{}) (int64, error) {
	return ps.rc.Publish(ctx, channel, message).Result()
}

func (ps *PubSub) PubSubChannels(ctx context.Context, pattern string) ([]string, error) {
	return ps.rc.PubSubChannels(ctx, pattern).Result()
}

func (ps *PubSub) PubSubNumSub(ctx context.Context, channels ...string) (map[string]int64, error) {
	return ps.rc.PubSubNumSub(ctx, channels...).Result()
}

func (ps *PubSub) PubSubNumPat(ctx context.Context) (int64, error) {
	return ps.rc.PubSubNumPat(ctx).Result()
}
