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

package config

import (
	"fmt"
	"net"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/memberlist"
)

func (c *Config) validateMemberlistConfig() error {
	var result error
	if c.MemberlistConfig.AdvertiseAddr != "" {
		if ip := net.ParseIP(c.MemberlistConfig.AdvertiseAddr); ip == nil {
			result = multierror.Append(result,
				fmt.Errorf("memberlist: AdvertiseAddr has to be a valid IPv4 or IPv6 address"))
		}
	}
	if c.MemberlistConfig.BindAddr == "" {
		result = multierror.Append(result,
			fmt.Errorf("memberlist: BindAddr cannot be an empty string"))
	}
	return result
}

// NewMemberlistConfig returns a new memberlist.Config for a given environment.
//
// It takes an env parameter: local, lan and wan.
//
// local:
// DefaultLocalConfig works like DefaultConfig, however it returns a configuration that
// is optimized for a local loopback environments. The default configuration is still very conservative
// and errs on the side of caution.
//
// lan:
// DefaultLANConfig returns a sane set of configurations for Memberlist. It uses the hostname
// as the node name, and otherwise sets very conservative values that are sane for most LAN environments.
// The default configuration errs on the side of caution, choosing values that are optimized for higher convergence
// at the cost of higher bandwidth usage. Regardless, these values are a good starting point when getting started with
// memberlist.
//
// wan:
// DefaultWANConfig works like DefaultConfig, however it returns a configuration that is optimized for most WAN
// environments. The default configuration is still very conservative and errs on the side of caution.
func NewMemberlistConfig(env string) (*memberlist.Config, error) {
	e := strings.ToLower(env)
	switch e {
	case "local":
		return memberlist.DefaultLocalConfig(), nil
	case "lan":
		return memberlist.DefaultLANConfig(), nil
	case "wan":
		return memberlist.DefaultWANConfig(), nil
	}
	return nil, fmt.Errorf("unknown env: %s", env)
}
