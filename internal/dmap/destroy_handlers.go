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
	"errors"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (dm *DMap) destroyFragmentOnPartition(part *partitions.Partition) error {
	f, err := dm.loadFragment(part)
	if errors.Is(err, errFragmentNotFound) {
		// not exists
		return nil
	}
	if err != nil {
		return err
	}
	return wipeOutFragment(part, dm.fragmentName, f)
}

func (s *Service) destroyLocalDMap(name string) error {
	// This is very similar with rm -rf. Destroys given dmap on the cluster
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		dm, err := s.getDMap(name)
		if errors.Is(err, ErrDMapNotFound) {
			continue
		}
		if err != nil {
			return err
		}

		part := dm.s.primary.PartitionByID(partID)
		err = dm.destroyFragmentOnPartition(part)
		if err != nil {
			return err
		}

		// Destroy on replicas
		if s.config.ReplicaCount > config.MinimumReplicaCount {
			backup := dm.s.backup.PartitionByID(partID)
			err = dm.destroyFragmentOnPartition(backup)
			if err != nil {
				return err
			}
		}
	}

	s.Lock()
	delete(s.dmaps, name)
	s.Unlock()

	return nil
}

func (s *Service) destroyCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	destroyCmd, err := protocol.ParseDestroyCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(destroyCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if destroyCmd.Local {
		err = s.destroyLocalDMap(destroyCmd.DMap)
	} else {
		err = dm.destroyOnCluster(s.ctx)
	}

	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteString(protocol.StatusOK)
}
