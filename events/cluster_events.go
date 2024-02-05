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
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/buraksezer/olric/internal/util"
)

const (
	ClusterEventsChannel       = "cluster.events"
	KindNodeJoinEvent          = "node-join-event"
	KindNodeLeftEvent          = "node-left-event"
	KindFragmentMigrationEvent = "fragment-migration-event"
	KindFragmentReceivedEvent  = "fragment-received-event"
)

type Event interface {
	Encode() (string, error)
}

// encodeEvents encodes given interface to its JSON representation and preserves the order in fields slice.
func encodeEvent(data interface{}, fields []string, valueExtractor func(r reflect.Value, field string) (interface{}, error)) (string, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("{")
	r := reflect.Indirect(reflect.ValueOf(data))
	for i, field := range fields {
		sf, ok := r.Type().FieldByName(field)
		if !ok {
			return "", fmt.Errorf("field not found: %s", field)
		}

		tag := strings.Trim(string(sf.Tag), "json:")
		tag, err := strconv.Unquote(tag)
		if err != nil {
			return "", err
		}

		value, err := valueExtractor(r, field)
		if err != nil {
			return "", err
		}

		if i != 0 {
			buf.WriteString(",")
		}

		// marshal key
		key, err := json.Marshal(tag)
		if err != nil {
			return "", err
		}
		buf.Write(key)

		buf.WriteString(":")
		// marshal value
		val, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		buf.Write(val)
	}
	buf.WriteString("}")
	return util.BytesToString(buf.Bytes()), nil
}

type NodeJoinEvent struct {
	Kind      string `json:"kind"`
	Source    string `json:"source"`
	NodeJoin  string `json:"node_join"`
	Timestamp int64  `json:"timestamp"`
}

func (n *NodeJoinEvent) Encode() (string, error) {
	fields := []string{"Timestamp", "Source", "Kind", "NodeJoin"}
	return encodeEvent(n, fields, func(r reflect.Value, field string) (interface{}, error) {
		var value interface{}
		switch field {
		case "Timestamp":
			value = r.FieldByName(field).Int()
		case "Source", "Kind", "NodeJoin":
			value = r.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}

type NodeLeftEvent struct {
	Kind      string `json:"kind"`
	Source    string `json:"source"`
	NodeLeft  string `json:"node_left"`
	Timestamp int64  `json:"timestamp"`
}

func (n *NodeLeftEvent) Encode() (string, error) {
	fields := []string{"Timestamp", "Source", "Kind", "NodeLeft"}
	return encodeEvent(n, fields, func(r reflect.Value, field string) (interface{}, error) {
		var value interface{}
		switch field {
		case "Timestamp":
			value = r.FieldByName(field).Int()
		case "Source", "Kind", "NodeLeft":
			value = r.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}

type FragmentMigrationEvent struct {
	Kind          string `json:"kind"`
	Source        string `json:"source"`
	Target        string `json:"target"`
	Identifier    string `json:"identifier"`
	PartitionID   uint64 `json:"partition_id"`
	DataStructure string `json:"data_structure"`
	Length        int    `json:"length"`
	IsBackup      bool   `json:"is_backup"`
	Timestamp     int64  `json:"timestamp"`
}

func (f *FragmentMigrationEvent) Encode() (string, error) {
	fields := []string{
		"Timestamp",
		"Source",
		"Kind",
		"Target",
		"DataStructure",
		"PartitionID",
		"Identifier",
		"IsBackup",
		"Length",
	}
	return encodeEvent(f, fields, func(r reflect.Value, field string) (interface{}, error) {
		var value interface{}
		switch field {
		case "IsBackup":
			value = r.FieldByName(field).Bool()
		case "PartitionID":
			value = r.FieldByName(field).Uint()
		case "Timestamp", "Length":
			value = r.FieldByName(field).Int()
		case "Source", "Kind", "Target", "DataStructure", "Identifier":
			value = r.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}

type FragmentReceivedEvent struct {
	Kind          string `json:"kind"`
	Source        string `json:"source"`
	Identifier    string `json:"identifier"`
	PartitionID   uint64 `json:"partition_id"`
	DataStructure string `json:"data_structure"`
	Length        int    `json:"length"`
	IsBackup      bool   `json:"is_backup"`
	Timestamp     int64  `json:"timestamp"`
}

func (f *FragmentReceivedEvent) Encode() (string, error) {
	fields := []string{
		"Timestamp",
		"Source",
		"Kind",
		"DataStructure",
		"PartitionID",
		"Identifier",
		"IsBackup",
		"Length",
	}
	return encodeEvent(f, fields, func(r reflect.Value, field string) (interface{}, error) {
		var value interface{}
		switch field {
		case "IsBackup":
			value = r.FieldByName(field).Bool()
		case "PartitionID":
			value = r.FieldByName(field).Uint()
		case "Timestamp", "Length":
			value = r.FieldByName(field).Int()
		case "Source", "Kind", "DataStructure", "Identifier":
			value = r.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}
