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
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (s *Service) getCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	getCmd, err := protocol.ParseGetCommand(cmd)
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
		protocol.WriteError(conn, err)
		return
	}

	if getCmd.Raw {
		conn.WriteBulk(raw.Encode())
		return
	}
	conn.WriteBulk(raw.Value())
}

func (s *Service) getEntryCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	getEntryCmd, err := protocol.ParseGetEntryCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	dm, err := s.getOrCreateDMap(getEntryCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var kind = partitions.PRIMARY
	if getEntryCmd.Replica {
		kind = partitions.BACKUP
	}

	e := newEnv(s.ctx)
	e.dmap = getEntryCmd.DMap
	e.key = getEntryCmd.Key
	e.hkey = partitions.HKey(getEntryCmd.DMap, getEntryCmd.Key)
	e.kind = kind
	nt, err := dm.getOnFragment(e)
	if err == errFragmentNotFound {
		err = ErrKeyNotFound
	}
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	// We found it.
	conn.WriteBulk(nt.Encode())
}
