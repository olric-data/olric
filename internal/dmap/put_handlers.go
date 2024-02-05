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

package dmap

import (
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (s *Service) putCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	putCmd, err := protocol.ParsePutCommand(cmd)
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
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteString(protocol.StatusOK)
}

func (s *Service) putEntryCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	putEntryCmd, err := protocol.ParsePutEntryCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(putEntryCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	e := newEnv(s.ctx)
	e.hkey = partitions.HKey(putEntryCmd.DMap, putEntryCmd.Key)
	e.dmap = putEntryCmd.DMap
	e.key = putEntryCmd.Key
	e.value = putEntryCmd.Value
	err = dm.putOnReplicaFragment(e)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteString(protocol.StatusOK)
}
