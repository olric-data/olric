// Copyright 2018-2025 Burak Sezer
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

package protocol

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtocol_Ping(t *testing.T) {
	ping := NewPing()

	cmd := stringToCommand(ping.Command(context.Background()).String())
	parsed, err := ParsePingCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "", parsed.Message)
}

func TestProtocol_Ping_Message(t *testing.T) {
	ping := NewPing()
	ping.SetMessage("message")

	cmd := stringToCommand(ping.Command(context.Background()).String())
	parsed, err := ParsePingCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "message", parsed.Message)
}

func TestProtocol_MoveFragment(t *testing.T) {
	moveFragmentCmd := NewMoveFragment([]byte("payload"))

	cmd := stringToCommand(moveFragmentCmd.Command(context.Background()).String())
	parsed, err := ParseMoveFragmentCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, []byte("payload"), parsed.Payload)
}

func TestProtocol_UpdateRoutingTable(t *testing.T) {
	updateRoutingTableCmd := NewUpdateRouting([]byte("payload"), 123)

	cmd := stringToCommand(updateRoutingTableCmd.Command(context.Background()).String())
	parsed, err := ParseUpdateRoutingCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, []byte("payload"), parsed.Payload)
	require.Equal(t, uint64(123), parsed.CoordinatorID)
}

func TestProtocol_LengthOfPart(t *testing.T) {
	updateRoutingTableCmd := NewLengthOfPart(123)

	cmd := stringToCommand(updateRoutingTableCmd.Command(context.Background()).String())
	parsed, err := ParseLengthOfPartCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, uint64(123), parsed.PartID)
	require.False(t, parsed.Replica)
}

func TestProtocol_LengthOfPart_RC(t *testing.T) {
	updateRoutingTableCmd := NewLengthOfPart(123)
	updateRoutingTableCmd.SetReplica()

	cmd := stringToCommand(updateRoutingTableCmd.Command(context.Background()).String())
	parsed, err := ParseLengthOfPartCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, uint64(123), parsed.PartID)
	require.True(t, parsed.Replica)
}

func TestProtocol_Stats(t *testing.T) {
	statsCmd := NewStats()

	cmd := stringToCommand(statsCmd.Command(context.Background()).String())
	parsed, err := ParseStatsCommand(cmd)
	require.NoError(t, err)

	require.False(t, parsed.CollectRuntime)
}

func TestProtocol_Stats_CR(t *testing.T) {
	statsCmd := NewStats()
	statsCmd.SetCollectRuntime()

	cmd := stringToCommand(statsCmd.Command(context.Background()).String())
	parsed, err := ParseStatsCommand(cmd)
	require.NoError(t, err)

	require.True(t, parsed.CollectRuntime)
}

func TestProtocol_Auth(t *testing.T) {
	auth := NewAuth("foobar", "secret")

	cmd := stringToCommand(auth.Command(context.Background()).String())
	parsed, err := ParseAuthCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "foobar", parsed.Username)
	require.Equal(t, "secret", parsed.Password)
}

func TestProtocol_Auth_errWrongNumber(t *testing.T) {
	cmd := stringToCommand("auth foobar:")

	_, err := ParseAuthCommand(cmd)
	require.Equal(t, "wrong number of arguments for 'auth foobar' command", err.Error())
}
