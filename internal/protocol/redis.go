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

package protocol

import (
	"errors"
	"strconv"
	"strings"

	"github.com/olric-data/olric/internal/util"
	"github.com/tidwall/redcon"
)

const DefaultDMapName = "default"

func ParseRedisSetCommand(cmd redcon.Command) (*Put, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	p := NewPut(
		DefaultDMapName,
		util.BytesToString(cmd.Args[1]), // Key
		cmd.Args[2],                     // Value
	)

	args := cmd.Args[3:]
	for len(args) > 0 {
		switch arg := strings.ToUpper(util.BytesToString(args[0])); arg {
		case "NX":
			p.SetNX()
			args = args[1:]
			continue
		case "XX":
			p.SetXX()
			args = args[1:]
			continue
		case "PX":
			if len(args) < 2 {
				return nil, errors.New("syntax error")
			}
			px, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPX(px)
			args = args[2:]
			continue
		case "EX":
			if len(args) < 2 {
				return nil, errors.New("syntax error")
			}
			ex, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEX(ex)
			args = args[2:]
			continue
		case "EXAT":
			if len(args) < 2 {
				return nil, errors.New("syntax error")
			}
			exat, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEXAT(exat)
			args = args[2:]
			continue
		case "PXAT":
			if len(args) < 2 {
				return nil, errors.New("syntax error")
			}
			pxat, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPXAT(pxat)
			args = args[2:]
			continue
		default:
			return nil, errors.New("syntax error")
		}
	}

	return p, nil
}

func ParseRedisGetCommand(cmd redcon.Command) (*Get, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewGet(
		DefaultDMapName,
		util.BytesToString(cmd.Args[1]), // Key
	), nil
}

func ParseRedisDelCommand(cmd redcon.Command) (*Del, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	d := NewDel(DefaultDMapName)
	for _, key := range cmd.Args[1:] {
		d.Keys = append(d.Keys, util.BytesToString(key))
	}
	return d, nil
}
