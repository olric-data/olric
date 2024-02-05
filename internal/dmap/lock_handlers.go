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
	"encoding/hex"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (s *Service) unlockCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	unlockCmd, err := protocol.ParseUnlockCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(unlockCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	token, err := hex.DecodeString(unlockCmd.Token)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	err = dm.Unlock(s.ctx, unlockCmd.Key, token)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteString(protocol.StatusOK)
}

func (s *Service) lockCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	lockCmd, err := protocol.ParseLockCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(lockCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var timeout = nilTimeout
	switch {
	case lockCmd.EX != 0:
		timeout = time.Duration(lockCmd.EX * float64(time.Second))
	case lockCmd.PX != 0:
		timeout = time.Duration(lockCmd.PX * int64(time.Millisecond))
	}

	var deadline = time.Duration(lockCmd.Deadline * float64(time.Second))
	token, err := dm.Lock(s.ctx, lockCmd.Key, timeout, deadline)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteString(hex.EncodeToString(token))
}

func (s *Service) lockLeaseCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	lockLeaseCmd, err := protocol.ParseLockLeaseCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(lockLeaseCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	timeout := time.Duration(lockLeaseCmd.Timeout * float64(time.Second))
	token, err := hex.DecodeString(lockLeaseCmd.Token)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	err = dm.Lease(s.ctx, lockLeaseCmd.Key, token, timeout)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteString(protocol.StatusOK)
}

func (s *Service) plockLeaseCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	plockLeaseCmd, err := protocol.ParsePLockLeaseCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(plockLeaseCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	timeout := time.Duration(plockLeaseCmd.Timeout * int64(time.Millisecond))
	token, err := hex.DecodeString(plockLeaseCmd.Token)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	err = dm.Lease(s.ctx, plockLeaseCmd.Key, token, timeout)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteString(protocol.StatusOK)
}
