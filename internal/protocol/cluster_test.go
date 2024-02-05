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

package protocol

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtocol_ClusterRoutingTable(t *testing.T) {
	rtCmd := NewClusterRoutingTable()

	cmd := stringToCommand(rtCmd.Command(context.Background()).String())
	_, err := ParseClusterRoutingTable(cmd)
	require.NoError(t, err)

	t.Run("CLUSTER.ROUTINGTABLE invalid command", func(t *testing.T) {
		cmd := stringToCommand("cluster routing table foobar")
		_, err = ParseClusterRoutingTable(cmd)
		require.Error(t, err)
	})
}

func TestProtocol_ClusterMembers(t *testing.T) {
	membersCmd := NewClusterMembers()

	cmd := stringToCommand(membersCmd.Command(context.Background()).String())
	_, err := ParseClusterMembers(cmd)
	require.NoError(t, err)

	t.Run("CLUSTER.MEMBERS invalid command", func(t *testing.T) {
		cmd := stringToCommand("cluster members foobar")
		_, err = ParseClusterMembers(cmd)
		require.Error(t, err)
	})
}
