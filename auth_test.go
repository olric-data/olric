// Copyright 2018-2025 The Olric Authors
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

package olric

import (
	"context"
	"testing"

	"github.com/olric-data/olric/config"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestAuthCommandHandler_WithPassword(t *testing.T) {
	cluster := newTestOlricCluster(t)
	testConfig := testutil.NewConfig()
	testConfig.Authentication = &config.Authentication{
		Password: "test-password",
	}
	db := cluster.addMemberWithConfig(t, testConfig)

	expectedMessage := "error while discovering the cluster members: wrong password"
	ctx := context.Background()
	t.Run("With correct credentials", func(t *testing.T) {
		c, err := NewClusterClient([]string{db.name}, WithPassword("test-password"))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		response, err := c.Ping(ctx, db.rt.This().String(), "")
		require.NoError(t, err)
		require.Equal(t, DefaultPingResponse, response)
	})

	t.Run("With wrong credentials", func(t *testing.T) {
		_, err := NewClusterClient([]string{db.name}, WithPassword("wrong"))
		require.ErrorContains(t, err, expectedMessage)
	})

	t.Run("Without credentials", func(t *testing.T) {
		_, err := NewClusterClient([]string{db.name}, WithPassword("wrong"))
		require.ErrorContains(t, err, expectedMessage)
	})
}
