// Copyright 2018-2025 The Olric Authors
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

package dmap

import (
	"errors"
	"time"

	"github.com/olric-data/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (s *Service) redisSetCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	putCmd, err := protocol.ParseRedisSetCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	dm, err := s.getOrCreateDMap(putCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var pc PutConfig
	switch {
	case putCmd.NX:
		pc.HasNX = true
	case putCmd.XX:
		pc.HasXX = true
	}

	switch {
	case putCmd.EX != 0:
		pc.HasEX = true
		pc.EX = time.Duration(putCmd.EX * float64(time.Second))
	case putCmd.PX != 0:
		pc.HasPX = true
		pc.PX = time.Duration(putCmd.PX * int64(time.Millisecond))
	case putCmd.EXAT != 0:
		pc.HasEXAT = true
		pc.EXAT = time.Duration(putCmd.EXAT * float64(time.Second))
	case putCmd.PXAT != 0:
		pc.HasPXAT = true
		pc.PXAT = time.Duration(putCmd.PXAT * int64(time.Millisecond))
	}

	e := newEnv(s.ctx)
	e.putConfig = &pc
	e.dmap = putCmd.DMap
	e.key = putCmd.Key
	e.value = putCmd.Value
	err = dm.put(e)
	if err != nil {
		if putCmd.NX && errors.Is(err, ErrKeyFound) {
			conn.WriteNull()
			return
		}
		if putCmd.XX && errors.Is(err, ErrKeyNotFound) {
			conn.WriteNull()
			return
		}
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteString(protocol.StatusOK)
}

func (s *Service) redisGetCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	getCmd, err := protocol.ParseRedisGetCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	dm, err := s.getOrCreateDMap(getCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	raw, err := dm.Get(s.ctx, getCmd.Key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			conn.WriteNull()
			return
		}
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteBulk(raw.Value())
}

func (s *Service) redisDelCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	delCmd, err := protocol.ParseRedisDelCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	dm, err := s.getOrCreateDMap(delCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	count, err := dm.deleteKeys(s.ctx, delCmd.Keys...)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteInt(count)
}
