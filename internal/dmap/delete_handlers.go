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

func (s *Service) delCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	delCmd, err := protocol.ParseDelCommand(cmd)
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

func (s *Service) delEntryCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	delCmd, err := protocol.ParseDelEntryCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	dm, err := s.getOrCreateDMap(delCmd.Del.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var kind = partitions.PRIMARY
	if delCmd.Replica {
		kind = partitions.BACKUP
	}
	for _, key := range delCmd.Del.Keys {
		err = dm.deleteFromFragment(key, kind)
		if err != nil {
			protocol.WriteError(conn, err)
			return
		}
	}

	conn.WriteInt(len(delCmd.Del.Keys))
}
