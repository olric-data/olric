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
	"fmt"
	"time"

	"github.com/buraksezer/olric/events"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/tidwall/redcon"
	"github.com/vmihailenco/msgpack/v5"
)

type fragmentPack struct {
	PartID  uint64
	Kind    partitions.Kind
	Name    string
	Payload []byte
}

func (dm *DMap) fragmentMergeFunction(f *fragment, hkey uint64, entry storage.Entry) error {
	current, err := f.storage.Get(hkey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return f.storage.Put(hkey, entry)
	}
	if err != nil {
		return err
	}

	versions := []*version{{entry: current}, {entry: entry}}
	versions = dm.sortVersions(versions)
	winner := versions[0].entry
	if winner == current {
		// No need to insert the winner
		return nil
	}
	return f.storage.Put(hkey, winner)
}

func (dm *DMap) mergeFragments(part *partitions.Partition, fp *fragmentPack) error {
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	// Acquire fragment's lock. No one should work on it.
	f.Lock()
	defer f.Unlock()

	return f.storage.Import(fp.Payload, func(hkey uint64, entry storage.Entry) error {
		return dm.fragmentMergeFunction(f, hkey, entry)
	})
}

func (s *Service) checkOwnership(part *partitions.Partition) bool {
	owners := part.Owners()
	for _, owner := range owners {
		if owner.CompareByID(s.rt.This()) {
			return true
		}
	}
	return false
}

func (s *Service) validateFragmentPack(fp *fragmentPack) error {
	if fp.PartID >= s.config.PartitionCount {
		return fmt.Errorf("invalid partition id: %d", fp.PartID)
	}

	var part *partitions.Partition
	if fp.Kind == partitions.PRIMARY {
		part = s.primary.PartitionByID(fp.PartID)
	} else {
		part = s.backup.PartitionByID(fp.PartID)
	}

	// Check ownership before merging. This is useful to prevent data corruption in network partitioning case.
	if !s.checkOwnership(part) {
		return fmt.Errorf("%w: %s",
			neterrors.ErrInvalidArgument, fmt.Sprintf("partID: %d (kind: %s) doesn't belong to %s",
				fp.PartID, fp.Kind, s.rt.This()))
	}
	return nil
}

func (s *Service) moveFragmentCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	moveFragmentCmd, err := protocol.ParseMoveFragmentCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	fp := &fragmentPack{}
	err = msgpack.Unmarshal(moveFragmentCmd.Payload, fp)
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to unmarshal DMap: %v", err)
		protocol.WriteError(conn, err)
		return
	}

	if err = s.validateFragmentPack(fp); err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var part *partitions.Partition
	if fp.Kind == partitions.PRIMARY {
		part = s.primary.PartitionByID(fp.PartID)
	} else {
		part = s.backup.PartitionByID(fp.PartID)
	}
	s.log.V(2).Printf("[INFO] Received DMap (kind: %s): %s on PartID: %d", fp.Kind, fp.Name, fp.PartID)

	dm, err := s.NewDMap(fp.Name)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	err = dm.mergeFragments(part, fp)
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to merge Received DMap (kind: %s): %s on PartID: %d: %v",
			fp.Kind, fp.Name, fp.PartID, err)
		protocol.WriteError(conn, err)
		return
	}

	if s.config.EnableClusterEventsChannel {
		e := &events.FragmentReceivedEvent{
			Kind:          events.KindFragmentReceivedEvent,
			Source:        s.rt.This().String(),
			DataStructure: "dmap",
			PartitionID:   part.ID(),
			Identifier:    fp.Name,
			Length:        len(moveFragmentCmd.Payload),
			IsBackup:      part.Kind() == partitions.BACKUP,
			Timestamp:     time.Now().UnixNano(),
		}
		s.wg.Add(1)
		go s.publishEvent(e)
	}

	conn.WriteString(protocol.StatusOK)
}
