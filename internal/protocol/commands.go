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

const StatusOK = "OK"

type ClusterCommands struct {
	RoutingTable string
	Members      string
}

var Cluster = &ClusterCommands{
	RoutingTable: "cluster.routingtable",
	Members:      "cluster.members",
}

type InternalCommands struct {
	MoveFragment        string
	UpdateRouting       string
	LengthOfPart        string
	ClusterRoutingTable string
}

var Internal = &InternalCommands{
	MoveFragment:  "internal.node.movefragment",
	UpdateRouting: "internal.node.updaterouting",
	LengthOfPart:  "internal.node.lengthofpart",
}

type GenericCommands struct {
	Ping  string
	Stats string
	Auth  string
}

var Generic = &GenericCommands{
	Ping:  "ping",
	Stats: "stats",
	Auth:  "auth",
}

type DMapCommands struct {
	Get         string
	GetEntry    string
	Put         string
	PutEntry    string
	Del         string
	DelEntry    string
	Expire      string
	PExpire     string
	Destroy     string
	Query       string
	Incr        string
	Decr        string
	GetPut      string
	IncrByFloat string
	Lock        string
	Unlock      string
	LockLease   string
	PLockLease  string
	Scan        string
}

var DMap = &DMapCommands{
	Get:         "dm.get",
	GetEntry:    "dm.getentry",
	Put:         "dm.put",
	PutEntry:    "dm.putentry",
	Del:         "dm.del",
	DelEntry:    "dm.delentry",
	Expire:      "dm.expire",
	PExpire:     "dm.pexpire",
	Destroy:     "dm.destroy",
	Incr:        "dm.incr",
	Decr:        "dm.decr",
	GetPut:      "dm.getput",
	IncrByFloat: "dm.incrbyfloat",
	Lock:        "dm.lock",
	Unlock:      "dm.unlock",
	LockLease:   "dm.locklease",
	PLockLease:  "dm.plocklease",
	Scan:        "dm.scan",
}

type PubSubCommands struct {
	PubSub          string
	Publish         string
	PublishInternal string
	Subscribe       string
	PSubscribe      string
	PubSubChannels  string
	PubSubNumpat    string
	PubSubNumsub    string
}

var PubSub = &PubSubCommands{
	PubSub:          "pubsub",
	Publish:         "publish",
	PublishInternal: "publish.internal",
	Subscribe:       "subscribe",
	PSubscribe:      "psubscribe",
	PubSubChannels:  "pubsub channels",
	PubSubNumpat:    "pubsub numpat",
	PubSubNumsub:    "pubsub numsub",
}
