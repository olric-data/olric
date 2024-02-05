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

package olric

import (
	"context"
	"strings"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

const DefaultPingResponse = "PONG"

func (db *Olric) ping(ctx context.Context, addr, message string) ([]byte, error) {
	message = strings.TrimSpace(message)

	pingCmd := protocol.NewPing()
	if message != "" {
		pingCmd = pingCmd.SetMessage(message)
	}

	cmd := pingCmd.Command(ctx)
	rc := db.client.Get(addr)
	err := rc.Process(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return cmd.Bytes()
}

func (db *Olric) pingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pingCmd, err := protocol.ParsePingCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if pingCmd.Message != "" {
		conn.WriteString(pingCmd.Message)
		return
	}
	conn.WriteString(DefaultPingResponse)
}
