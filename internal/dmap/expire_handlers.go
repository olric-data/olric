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
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (s *Service) expireCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	expireCmd, err := protocol.ParseExpireCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(expireCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	pc := &PutConfig{
		OnlyUpdateTTL: true,
	}

	e := newEnv(s.ctx)
	e.putConfig = pc
	e.dmap = expireCmd.DMap
	e.key = expireCmd.Key
	e.timeout = expireCmd.Seconds
	err = dm.put(e)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteString(protocol.StatusOK)
}

func (s *Service) pexpireCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pexpireCmd, err := protocol.ParsePExpireCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(pexpireCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	pc := &PutConfig{
		OnlyUpdateTTL: true,
	}

	e := newEnv(s.ctx)
	e.putConfig = pc
	e.dmap = pexpireCmd.DMap
	e.key = pexpireCmd.Key
	e.timeout = pexpireCmd.Milliseconds
	err = dm.put(e)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteString(protocol.StatusOK)
}
