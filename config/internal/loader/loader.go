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

package loader

import "gopkg.in/yaml.v2"

type server struct {
	Name                       string  `yaml:"name"`
	BindAddr                   string  `yaml:"bindAddr"`
	BindPort                   int     `yaml:"bindPort"`
	Interface                  string  `yaml:"interface"`
	ReplicationMode            int     `yaml:"replicationMode"`
	PartitionCount             uint64  `yaml:"partitionCount"`
	LoadFactor                 float64 `yaml:"loadFactor"`
	KeepAlivePeriod            string  `yaml:"keepAlivePeriod"`
	IdleClose                  string  `yaml:"idleClose"`
	BootstrapTimeout           string  `yaml:"bootstrapTimeout"`
	ReplicaCount               int     `yaml:"replicaCount"`
	WriteQuorum                int     `yaml:"writeQuorum"`
	ReadQuorum                 int     `yaml:"readQuorum"`
	ReadRepair                 bool    `yaml:"readRepair"`
	MemberCountQuorum          int32   `yaml:"memberCountQuorum"`
	RoutingTablePushInterval   string  `yaml:"routingTablePushInterval"`
	TriggerBalancerInterval    string  `yaml:"triggerBalancerInterval"`
	LeaveTimeout               string  `yaml:"leaveTimeout"`
	EnableClusterEventsChannel bool    `yaml:"enableClusterEventsChannel"`
}

type authentication struct {
	RequirePass string `yaml:"requirepass"`
	Enabled     bool   `yaml:"enabled"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
}

type client struct {
	DialTimeout     string `yaml:"dialTimeout"`
	ReadTimeout     string `yaml:"readTimeout"`
	WriteTimeout    string `yaml:"writeTimeout"`
	MaxRetries      int    `yaml:"maxRetries"`
	MinRetryBackoff string `yaml:"minRetryBackoff"`
	MaxRetryBackoff string `yaml:"maxRetryBackoff"`
	PoolFIFO        bool   `yaml:"poolFIFO"`
	PoolSize        int    `yaml:"poolSize"`
	MinIdleConns    int    `yaml:"minIdleConns"`
	MaxConnAge      string `yaml:"maxConnAge"`
	PoolTimeout     string `yaml:"poolTimeout"`
	IdleTimeout     string `yaml:"idleTimeout"`
}

// logging contains configuration variables of logging section of config file.
type logging struct {
	Verbosity int32  `yaml:"verbosity"`
	Level     string `yaml:"level"`
	Output    string `yaml:"output"`
}

type memberlist struct {
	Environment             string   `yaml:"environment"` // required
	BindAddr                string   `yaml:"bindAddr"`    // required
	BindPort                int      `yaml:"bindPort"`    // required
	Interface               string   `yaml:"interface"`
	EnableCompression       *bool    `yaml:"enableCompression"`
	JoinRetryInterval       string   `yaml:"joinRetryInterval"` // required
	MaxJoinAttempts         int      `yaml:"maxJoinAttempts"`   // required
	Peers                   []string `yaml:"peers"`
	IndirectChecks          *int     `yaml:"indirectChecks"`
	RetransmitMult          *int     `yaml:"retransmitMult"`
	SuspicionMult           *int     `yaml:"suspicionMult"`
	TCPTimeout              *string  `yaml:"tcpTimeout"`
	PushPullInterval        *string  `yaml:"pushPullInterval"`
	ProbeTimeout            *string  `yaml:"probeTimeout"`
	ProbeInterval           *string  `yaml:"probeInterval"`
	GossipInterval          *string  `yaml:"gossipInterval"`
	GossipToTheDeadTime     *string  `yaml:"gossipToTheDeadTime"`
	AdvertiseAddr           *string  `yaml:"advertiseAddr"`
	AdvertisePort           *int     `yaml:"advertisePort"`
	SuspicionMaxTimeoutMult *int     `yaml:"suspicionMaxTimeoutMult"`
	DisableTCPPings         *bool    `yaml:"disableTCPPings"`
	AwarenessMaxMultiplier  *int     `yaml:"awarenessMaxMultiplier"`
	GossipNodes             *int     `yaml:"gossipNodes"`
	GossipVerifyIncoming    *bool    `yaml:"gossipVerifyIncoming"`
	GossipVerifyOutgoing    *bool    `yaml:"gossipVerifyOutgoing"`
	DNSConfigPath           *string  `yaml:"dnsConfigPath"`
	HandoffQueueDepth       *int     `yaml:"handoffQueueDepth"`
	UDPBufferSize           *int     `yaml:"udpBufferSize"`
}

type engine struct {
	Name   string                 `yaml:"name"`
	Config map[string]interface{} `yaml:"config"`
}

type dmap struct {
	Engine          *engine `yaml:"engine"`
	MaxIdleDuration string  `yaml:"maxIdleDuration"`
	TTLDuration     string  `yaml:"ttlDuration"`
	MaxKeys         int     `yaml:"maxKeys"`
	MaxInuse        int     `yaml:"maxInuse"`
	LRUSamples      int     `yaml:"lruSamples"`
	EvictionPolicy  string  `yaml:"evictionPolicy"`
}

type dmaps struct {
	Engine                      *engine         `yaml:"engine"`
	NumEvictionWorkers          int64           `yaml:"numEvictionWorkers"`
	MaxIdleDuration             string          `yaml:"maxIdleDuration"`
	TTLDuration                 string          `yaml:"ttlDuration"`
	MaxKeys                     int             `yaml:"maxKeys"`
	MaxInuse                    int             `yaml:"maxInuse"`
	LRUSamples                  int             `yaml:"lruSamples"`
	EvictionPolicy              string          `yaml:"evictionPolicy"`
	CheckEmptyFragmentsInterval string          `yaml:"checkEmptyFragmentsInterval"`
	TriggerCompactionInterval   string          `yaml:"triggerCompactionInterval"`
	Custom                      map[string]dmap `yaml:"custom"`
}

type serviceDiscovery map[string]interface{}

// Loader is the main configuration struct
type Loader struct {
	Memberlist       memberlist       `yaml:"memberlist"`
	Logging          logging          `yaml:"logging"`
	Server           server           `yaml:"server"`
	Client           client           `yaml:"client"`
	DMaps            dmaps            `yaml:"dmaps"`
	ServiceDiscovery serviceDiscovery `yaml:"serviceDiscovery"`
	Authentication   authentication   `yaml:"authentication"`
}

// New tries to read Olric configuration from a YAML file.
func New(data []byte) (*Loader, error) {
	var lc Loader
	if err := yaml.Unmarshal(data, &lc); err != nil {
		return nil, err
	}
	return &lc, nil
}
