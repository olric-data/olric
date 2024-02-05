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
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (s *Service) subscribeCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	subscribeCmd, err := protocol.ParseSubscribeCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	for _, channel := range subscribeCmd.Channels {
		s.pubsub.Subscribe(conn, channel)
		CurrentSubscribers.Increase(1)
		SubscribersTotal.Increase(1)
	}
}

func (s *Service) publishCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	publishCmd, err := protocol.ParsePublishCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var total int
	members := s.rt.Discovery().GetMembers()
	for _, member := range members {
		if member.CompareByID(s.rt.This()) {
			count := s.pubsub.Publish(publishCmd.Channel, publishCmd.Message)
			total += count
			PublishedTotal.Increase(int64(count))
			continue
		}

		pi := protocol.NewPublishInternal(publishCmd.Channel, publishCmd.Message).Command(s.ctx)
		rc := s.client.Get(member.String())
		err = rc.Process(s.ctx, pi)
		if err != nil {
			protocol.WriteError(conn, err)
			return
		}
		pcount, err := pi.Result()
		if err != nil {
			protocol.WriteError(conn, err)
			return
		}
		total += int(pcount)
		PublishedTotal.Increase(pcount)
	}

	conn.WriteInt(total)
}

func (s *Service) publishInternalCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	publishInternalCmd, err := protocol.ParsePublishInternalCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	count := s.pubsub.Publish(publishInternalCmd.Channel, publishInternalCmd.Message)
	conn.WriteInt(count)
}

func (s *Service) psubscribeCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	psubscribeCmd, err := protocol.ParsePSubscribeCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	for _, pattern := range psubscribeCmd.Patterns {
		s.pubsub.Psubscribe(conn, pattern)
		PSubscribersTotal.Increase(1)
		CurrentPSubscribers.Increase(1)
	}
}

func (s *Service) pubsubChannelsCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pubsubChannelsCmd, err := protocol.ParsePubSubChannelsCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var channels []string
	if pubsubChannelsCmd.Pattern != "" {
		channels = s.pubsub.ChannelsWithPatterns(pubsubChannelsCmd.Pattern)
	} else {
		channels = s.pubsub.Channels()
	}
	conn.WriteArray(len(channels))
	for _, channel := range channels {
		conn.WriteBulkString(channel)
	}
}

func (s *Service) pubsubNumpatCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := protocol.ParsePubSubNumpatCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteInt(s.pubsub.Numpat())
}

func (s *Service) pubsubNumsubCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pubsubNumsubCmd, err := protocol.ParsePubSubNumsubCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if len(pubsubNumsubCmd.Channels) == 0 {
		conn.WriteArray(0)
		return
	}

	conn.WriteArray(len(pubsubNumsubCmd.Channels) * 2)
	for _, channel := range pubsubNumsubCmd.Channels {
		conn.WriteBulkString(channel)
		conn.WriteInt(s.pubsub.Numsub(channel))
	}
}
