// Copyright 2018-2025 Burak Sezer
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
	"fmt"
	"strconv"

	"github.com/olric-data/olric/internal/util"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

type Ping struct {
	Message string
}

func NewPing() *Ping {
	return &Ping{}
}

func (p *Ping) SetMessage(m string) *Ping {
	p.Message = m
	return p
}

func (p *Ping) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, Generic.Ping)
	if p.Message != "" {
		args = append(args, p.Message)
	}
	return redis.NewStringCmd(ctx, args...)
}

func ParsePingCommand(cmd redcon.Command) (*Ping, error) {
	if len(cmd.Args) < 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	p := NewPing()
	if len(cmd.Args) == 2 {
		p.SetMessage(util.BytesToString(cmd.Args[1]))
	}
	return p, nil
}

type MoveFragment struct {
	Payload []byte
}

func NewMoveFragment(payload []byte) *MoveFragment {
	return &MoveFragment{
		Payload: payload,
	}
}

func (m *MoveFragment) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, Internal.MoveFragment)
	args = append(args, m.Payload)
	return redis.NewStatusCmd(ctx, args...)
}

func ParseMoveFragmentCommand(cmd redcon.Command) (*MoveFragment, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewMoveFragment(cmd.Args[1]), nil
}

type UpdateRouting struct {
	Payload       []byte
	CoordinatorID uint64
}

func NewUpdateRouting(payload []byte, coordinatorID uint64) *UpdateRouting {
	return &UpdateRouting{
		Payload:       payload,
		CoordinatorID: coordinatorID,
	}
}

func (u *UpdateRouting) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, Internal.UpdateRouting)
	args = append(args, u.Payload)
	args = append(args, u.CoordinatorID)
	return redis.NewStringCmd(ctx, args...)
}

func ParseUpdateRoutingCommand(cmd redcon.Command) (*UpdateRouting, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}
	coordinatorID, err := strconv.ParseUint(util.BytesToString(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}

	return NewUpdateRouting(cmd.Args[1], coordinatorID), nil
}

type LengthOfPart struct {
	PartID  uint64
	Replica bool
}

func NewLengthOfPart(partID uint64) *LengthOfPart {
	return &LengthOfPart{
		PartID: partID,
	}
}

func (l *LengthOfPart) SetReplica() *LengthOfPart {
	l.Replica = true
	return l
}

func (l *LengthOfPart) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, Internal.LengthOfPart)
	args = append(args, l.PartID)
	if l.Replica {
		args = append(args, "RC")
	}
	return redis.NewIntCmd(ctx, args...)
}

func ParseLengthOfPartCommand(cmd redcon.Command) (*LengthOfPart, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}
	partID, err := strconv.ParseUint(util.BytesToString(cmd.Args[1]), 10, 64)
	if err != nil {
		return nil, err
	}

	l := NewLengthOfPart(partID)
	if len(cmd.Args) == 3 {
		arg := util.BytesToString(cmd.Args[2])
		if arg == "RC" {
			l.SetReplica()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return l, nil
}

type Stats struct {
	CollectRuntime bool
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) SetCollectRuntime() *Stats {
	s.CollectRuntime = true
	return s
}

func (s *Stats) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, Generic.Stats)
	if s.CollectRuntime {
		args = append(args, "CR")
	}
	return redis.NewStringCmd(ctx, args...)
}

func ParseStatsCommand(cmd redcon.Command) (*Stats, error) {
	if len(cmd.Args) < 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	s := NewStats()
	if len(cmd.Args) == 2 {
		arg := util.BytesToString(cmd.Args[1])
		if arg == "CR" {
			s.SetCollectRuntime()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return s, nil
}

type Auth struct {
	Username string
	Password string
}

func NewAuth(username, password string) *Auth {
	return &Auth{
		Username: username,
		Password: password,
	}
}

func (a *Auth) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}

	args = append(args, Generic.Auth)
	args = append(args, a.Username)
	args = append(args, a.Password)

	return redis.NewStatusCmd(ctx, args...)
}

func ParseAuthCommand(cmd redcon.Command) (*Auth, error) {
	if len(cmd.Args) != 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewAuth(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
	), nil
}
