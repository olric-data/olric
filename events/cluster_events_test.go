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

package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterEvents_NodeJoinEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := NodeJoinEvent{
		Kind:      KindNodeJoinEvent,
		Source:    "127.0.0.1:3423",
		NodeJoin:  "127.0.0.1:3576",
		Timestamp: timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"node-join-event","node_join":"127.0.0.1:3576"}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_NodeLeftEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := NodeLeftEvent{
		Kind:      KindNodeLeftEvent,
		Source:    "127.0.0.1:3423",
		NodeLeft:  "127.0.0.1:3576",
		Timestamp: timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"node-left-event","node_left":"127.0.0.1:3576"}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_FragmentMigrationEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := FragmentMigrationEvent{
		Kind:          KindFragmentMigrationEvent,
		Source:        "127.0.0.1:3423",
		Target:        "127.0.0.1:3576",
		Identifier:    "mydmap",
		PartitionID:   123,
		DataStructure: "dmap",
		Length:        1234,
		Timestamp:     timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"fragment-migration-event","target":"127.0.0.1:3576","data_structure":"dmap","partition_id":123,"identifier":"mydmap","is_backup":false,"length":1234}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_FragmentReceivedEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := FragmentReceivedEvent{
		Kind:          KindFragmentReceivedEvent,
		Source:        "127.0.0.1:3423",
		Identifier:    "mydmap",
		PartitionID:   123,
		DataStructure: "dmap",
		Length:        1234,
		Timestamp:     timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"fragment-received-event","data_structure":"dmap","partition_id":123,"identifier":"mydmap","is_backup":false,"length":1234}`
	require.Equal(t, expected, result)
}
