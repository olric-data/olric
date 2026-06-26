// Copyright 2018-2026 The Olric Authors
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
	"strconv"
	"time"

	"github.com/olric-data/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (s *Service) incrDecrCommon(cmd, dmap, key string, delta int) (int, error) {
	dm, err := s.getOrCreateDMap(dmap)
	if err != nil {
		return 0, err
	}

	e := newEnv(s.ctx)
	e.dmap = dm.name
	e.key = key
	return dm.atomicIncrDecr(cmd, e, delta)
}

func (s *Service) incrCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	incrCmd, err := protocol.ParseIncrCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	latest, err := s.incrDecrCommon(protocol.DMap.Incr, incrCmd.DMap, incrCmd.Key, incrCmd.Delta)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteInt(latest)
}

func (s *Service) decrCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	decrCmd, err := protocol.ParseDecrCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	latest, err := s.incrDecrCommon(protocol.DMap.Decr, decrCmd.DMap, decrCmd.Key, decrCmd.Delta)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteInt(latest)
}

func (s *Service) getPutCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	getPutCmd, err := protocol.ParseGetPutCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	dm, err := s.getOrCreateDMap(getPutCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	e := newEnv(s.ctx)
	e.dmap = getPutCmd.DMap
	e.key = getPutCmd.Key
	e.value = getPutCmd.Value
	old, err := dm.getPut(e)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if old == nil {
		conn.WriteNull()
		return
	}

	if getPutCmd.Raw {
		conn.WriteBulk(old.Encode())
		return
	}

	conn.WriteBulk(old.Value())
}

func (s *Service) compareAndSwapCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	casCmd, err := protocol.ParseCompareAndSwapCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(casCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var pc PutConfig
	switch {
	case casCmd.EX != 0:
		pc.HasEX = true
		pc.EX = time.Duration(casCmd.EX * float64(time.Second))
	case casCmd.PX != 0:
		pc.HasPX = true
		pc.PX = time.Duration(casCmd.PX * int64(time.Millisecond))
	case casCmd.EXAT != 0:
		pc.HasEXAT = true
		pc.EXAT = time.Duration(casCmd.EXAT * float64(time.Second))
	case casCmd.PXAT != 0:
		pc.HasPXAT = true
		pc.PXAT = time.Duration(casCmd.PXAT * int64(time.Millisecond))
	}

	e := newEnv(s.ctx)
	e.dmap = casCmd.DMap
	e.key = casCmd.Key
	e.value = casCmd.Value
	e.putConfig = &pc

	swapped, current, err := dm.compareAndSwap(e, casCmd.Expected)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteArray(2)
	if swapped {
		conn.WriteInt(1)
	} else {
		conn.WriteInt(0)
	}
	if current == nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(current.Encode())
	}
}

func (s *Service) incrByFloatCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	incrCmd, err := protocol.ParseIncrByFloatCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(incrCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	e := newEnv(s.ctx)
	e.dmap = dm.name
	e.key = incrCmd.Key
	latest, err := dm.atomicIncrByFloat(e, incrCmd.Delta)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteBulkString(strconv.FormatFloat(latest, 'f', -1, 64))
}
