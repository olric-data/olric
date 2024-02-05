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
)

func (s *Service) RegisterHandlers() {
	s.server.ServeMux().HandleFunc(protocol.DMap.Put, s.putCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.Get, s.getCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.Del, s.delCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.DelEntry, s.delEntryCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.GetEntry, s.getEntryCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.PutEntry, s.putEntryCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.Expire, s.expireCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.PExpire, s.pexpireCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.Destroy, s.destroyCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.Scan, s.scanCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.Incr, s.incrCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.Decr, s.decrCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.GetPut, s.getPutCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.IncrByFloat, s.incrByFloatCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.Lock, s.lockCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.Unlock, s.unlockCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.LockLease, s.lockLeaseCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DMap.PLockLease, s.plockLeaseCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.Internal.MoveFragment, s.moveFragmentCommandHandler)
}
