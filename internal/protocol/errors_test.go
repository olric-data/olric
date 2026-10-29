// Copyright 2018-2026 The Olric Authors
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
	"net"
	"os"
	"testing"
	"time"

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

func TestProtocol_ConvertError_Context(t *testing.T) {
	for _, sentinel := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			for _, err := range []error{sentinel, fmt.Errorf("command failed: %w", sentinel)} {
				converted := ConvertError(err)
				require.ErrorIs(t, converted, sentinel)
				require.Equal(t, err, converted)
			}
		})
	}
}

// delayedDeadlineContext models a socket deadline firing before cancellation
// of the context becomes visible to the caller.
type delayedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c delayedDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestProtocol_ConvertContextError(t *testing.T) {
	tests := []struct {
		name       string
		newContext func() (context.Context, context.CancelFunc)
		sentinel   error
	}{
		{
			name: "canceled",
			newContext: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			sentinel: context.Canceled,
		},
		{
			name: "deadline",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			sentinel: context.DeadlineExceeded,
		},
		{
			name: "deadline before cancellation notification",
			newContext: func() (context.Context, context.CancelFunc) {
				return delayedDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}, func() {}
			},
			sentinel: context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.newContext()
			defer cancel()

			require.NoError(t, ConvertContextError(ctx, nil), "a completed command must remain successful")
			transportErr := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
			require.ErrorIs(t, ConvertContextError(ctx, transportErr), tt.sentinel)
			require.ErrorIs(t, ConvertContextError(ctx, errSomethingWentWrong), tt.sentinel, "a completed context takes precedence over a server error")
			wrapped := fmt.Errorf("command failed: %w", tt.sentinel)
			require.Equal(t, wrapped, ConvertContextError(ctx, wrapped))
		})
	}
	SetError("WRONG", errSomethingWentWrong)
	err := fmt.Errorf("WRONG %s", errSomethingWentWrong.Error())
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	require.ErrorIs(t, ConvertContextError(ctx, err), errSomethingWentWrong)
}
