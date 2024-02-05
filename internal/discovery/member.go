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

package discovery

import (
	"encoding/binary"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/cespare/xxhash/v2"
	"github.com/vmihailenco/msgpack/v5"
)

// Member represents a node in the cluster.
type Member struct {
	Name      string
	NameHash  uint64
	ID        uint64
	Birthdate int64
}

// CompareByID returns true if two members denote the same member in the cluster.
func (m Member) CompareByID(other Member) bool {
	// ID variable is calculated by combining member's name and birthdate
	return m.ID == other.ID
}

// CompareByName returns true if the two members has the same name in the cluster.
// This function is intended to redirect the requests to the partition owner.
func (m Member) CompareByName(other Member) bool {
	return m.NameHash == other.NameHash
}

func (m Member) String() string {
	return m.Name
}

func (m Member) Encode() ([]byte, error) {
	return msgpack.Marshal(m)
}

func NewMemberFromMetadata(metadata []byte) (Member, error) {
	res := &Member{}
	err := msgpack.Unmarshal(metadata, res)
	return *res, err
}

func MemberID(name string, birthdate int64) uint64 {
	// Calculate member's identity. It's useful to compare hosts.
	buf := make([]byte, 8+len(name))
	binary.BigEndian.PutUint64(buf, uint64(birthdate))
	buf = append(buf, []byte(name)...)
	return xxhash.Sum64(buf)
}

func NewMember(c *config.Config) Member {
	birthdate := time.Now().UnixNano()
	nameHash := xxhash.Sum64([]byte(c.MemberlistConfig.Name))
	return Member{
		Name:      c.MemberlistConfig.Name,
		NameHash:  nameHash,
		ID:        MemberID(c.MemberlistConfig.Name, birthdate),
		Birthdate: birthdate,
	}
}
