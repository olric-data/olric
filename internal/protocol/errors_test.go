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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var errSomethingWentWrong = errors.New("something went wrong")

func TestProtocol_errWrongNumber(t *testing.T) {
	getCmd := NewGet("my-dmap", "my-key").Command(context.Background())
	cmd := stringToCommand(getCmd.String())

	err := errWrongNumber(cmd.Args)
	require.Equal(t, "wrong number of arguments for 'dm.get my-dmap my-key' command", err.Error())
}

func TestProtocol_GetPrefix(t *testing.T) {
	SetError("WRONG", errSomethingWentWrong)
	prefix := GetPrefix(errSomethingWentWrong)
	require.Equal(t, "WRONG", prefix)
}

func TestProtocol_GetError(t *testing.T) {
	SetError("WRONG", errSomethingWentWrong)
	err := GetError("WRONG")
	require.ErrorIs(t, err, errSomethingWentWrong)
}

func TestProtocol_ConvertError(t *testing.T) {
	SetError("WRONG", errSomethingWentWrong)
	err := fmt.Errorf("WRONG %s", errSomethingWentWrong.Error())
	cerr := ConvertError(err)
	require.ErrorIs(t, cerr, errSomethingWentWrong)
}
