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
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

var ErrInvalidArgument = errors.New("invalid argument")

var GenericError = "ERR"

var errorWithPrefix = struct {
	mtx    sync.RWMutex
	prefix map[string]error
	err    map[error]string
}{
	prefix: make(map[string]error),
	err:    make(map[error]string),
}

func init() {
	SetError("INVALIDARGUMENT", ErrInvalidArgument)
}

func SetError(prefix string, err error) {
	errorWithPrefix.mtx.Lock()
	defer errorWithPrefix.mtx.Unlock()

	e, ok := errorWithPrefix.prefix[prefix]
	if ok && e != err {
		panic(fmt.Sprintf("prefix collision: %s: %v != %v", prefix, err, e))
	}
	errorWithPrefix.err[err] = prefix
	errorWithPrefix.prefix[prefix] = err
}

func GetError(prefix string) error {
	errorWithPrefix.mtx.RLock()
	defer errorWithPrefix.mtx.RUnlock()

	return errorWithPrefix.prefix[prefix]
}

func getPrefix(err error) string {
	prefix, ok := errorWithPrefix.err[err]
	if !ok {
		return GenericError
	}
	return prefix
}

func GetPrefix(err error) string {
	errorWithPrefix.mtx.RLock()
	defer errorWithPrefix.mtx.RUnlock()

	prefix := getPrefix(err)
	if prefix == GenericError {
		return getPrefix(errors.Unwrap(err))
	}
	return prefix
}

func ConvertError(err error) error {
	if err == nil {
		return nil
	}

	parsed := strings.SplitN(err.Error(), " ", 2)
	if perr := GetError(parsed[0]); perr != nil {
		return perr
	}

	if len(parsed) > 1 {
		return fmt.Errorf("%s", parsed[1])
	}

	return err
}

func WriteError(conn redcon.Conn, err error) {
	prefix := GetPrefix(err)
	conn.WriteError(fmt.Sprintf("%s %s", prefix, err.Error()))
}

func errWrongNumber(args [][]byte) error {
	sb := strings.Builder{}
	for {
		arg := args[0]
		sb.Write(arg)
		args = args[1:]
		if len(args) == 0 {
			break
		}
		sb.WriteByte(0x20)
	}
	return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(sb.String()))
}
