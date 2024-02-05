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

package protocol

import (
	"context"

	"github.com/buraksezer/olric/internal/util"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

type Publish struct {
	Channel string
	Message string
}

func NewPublish(channel, message string) *Publish {
	return &Publish{
		Channel: channel,
		Message: message,
	}
}

func (p *Publish) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, PubSub.Publish)
	args = append(args, p.Channel)
	args = append(args, p.Message)
	return redis.NewIntCmd(ctx, args...)
}

func ParsePublishCommand(cmd redcon.Command) (*Publish, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewPublish(
		util.BytesToString(cmd.Args[1]), // Channel
		util.BytesToString(cmd.Args[2]), // Message
	), nil
}

type PublishInternal struct {
	Channel string
	Message string
}

func NewPublishInternal(channel, message string) *PublishInternal {
	return &PublishInternal{
		Channel: channel,
		Message: message,
	}
}

func (p *PublishInternal) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, PubSub.PublishInternal)
	args = append(args, p.Channel)
	args = append(args, p.Message)
	return redis.NewIntCmd(ctx, args...)
}

func ParsePublishInternalCommand(cmd redcon.Command) (*PublishInternal, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewPublishInternal(
		util.BytesToString(cmd.Args[1]), // Channel
		util.BytesToString(cmd.Args[2]), // Message
	), nil
}

type Subscribe struct {
	Channels []string
}

func NewSubscribe(channels ...string) *Subscribe {
	return &Subscribe{
		Channels: channels,
	}
}

func (s *Subscribe) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, PubSub.Subscribe)
	for _, channel := range s.Channels {
		args = append(args, channel)
	}
	return redis.NewSliceCmd(ctx, args...)
}

func ParseSubscribeCommand(cmd redcon.Command) (*Subscribe, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	var channels []string
	args := cmd.Args[1:]
	for len(args) > 0 {
		arg := util.BytesToString(args[0])
		channels = append(channels, arg)
		args = args[1:]
	}
	return NewSubscribe(channels...), nil
}

type PSubscribe struct {
	Patterns []string
}

func NewPSubscribe(patterns ...string) *PSubscribe {
	return &PSubscribe{
		Patterns: patterns,
	}
}

func (s *PSubscribe) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, PubSub.Subscribe)
	for _, channel := range s.Patterns {
		args = append(args, channel)
	}
	return redis.NewSliceCmd(ctx, args...)
}

func ParsePSubscribeCommand(cmd redcon.Command) (*PSubscribe, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	var patterns []string
	args := cmd.Args[1:]
	for len(args) > 0 {
		arg := util.BytesToString(args[0])
		patterns = append(patterns, arg)
		args = args[1:]
	}
	return NewPSubscribe(patterns...), nil
}

type PubSubChannels struct {
	Pattern string
}

func NewPubSubChannels() *PubSubChannels {
	return &PubSubChannels{}
}

func (ps *PubSubChannels) SetPattern(pattern string) *PubSubChannels {
	ps.Pattern = pattern
	return ps
}

func (ps *PubSubChannels) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, PubSub.PubSubChannels)
	if ps.Pattern != "" {
		args = append(args, ps.Pattern)
	}
	return redis.NewSliceCmd(ctx, args...)
}

func ParsePubSubChannelsCommand(cmd redcon.Command) (*PubSubChannels, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	ps := NewPubSubChannels()
	if len(cmd.Args) >= 3 {
		ps.SetPattern(util.BytesToString(cmd.Args[2]))
	}
	return ps, nil
}

type PubSubNumpat struct{}

func NewPubSubNumpat() *PubSubNumpat {
	return &PubSubNumpat{}
}

func (ps *PubSubNumpat) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, PubSub.PubSubNumpat)
	return redis.NewIntCmd(ctx, args...)
}

func ParsePubSubNumpatCommand(cmd redcon.Command) (*PubSubNumpat, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewPubSubNumpat(), nil
}

type PubSubNumsub struct {
	Channels []string
}

func NewPubSubNumsub(channels ...string) *PubSubNumsub {
	return &PubSubNumsub{
		Channels: channels,
	}
}

func (ps *PubSubNumsub) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, PubSub.PubSubNumsub)
	for _, channel := range ps.Channels {
		args = append(args, channel)
	}
	return redis.NewSliceCmd(ctx, args...)
}

func ParsePubSubNumsubCommand(cmd redcon.Command) (*PubSubNumsub, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	var channels []string
	args := cmd.Args[2:]
	for len(args) > 0 {
		arg := util.BytesToString(args[0])
		channels = append(channels, arg)
		args = args[1:]
	}
	return NewPubSubNumsub(channels...), nil
}
