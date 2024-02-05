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
	"runtime"
	"strconv"

	"github.com/hashicorp/go-sockaddr"
)

// The following functions are mostly extracted from Serf. See setupAgent function in cmd/serf/command/agent/command.go
// Thanks for the extraordinary software.
//
// Source: https://github.com/hashicorp/serf/blob/master/cmd/serf/command/agent/command.go#L204

func addrParts(address string) (string, int, error) {
	// Get the address
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return "", 0, err
	}

	return addr.IP.String(), addr.Port, nil
}

func getBindIPFromNetworkInterface(addrs []net.Addr) (string, error) {
	for _, a := range addrs {
		var addrIP net.IP
		if runtime.GOOS == "windows" {
			// Waiting for https://github.com/golang/go/issues/5395 to use IPNet only
			addr, ok := a.(*net.IPAddr)
			if !ok {
				continue
			}
			addrIP = addr.IP
		} else {
			addr, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			addrIP = addr.IP
		}

		// Skip self-assigned IPs
		if addrIP.IsLinkLocalUnicast() {
			continue
		}
		return addrIP.String(), nil
	}
	return "", fmt.Errorf("failed to find usable address for interface")
}

func getBindIP(ifname, address string) (string, error) {
	bindIP, _, err := addrParts(address)
	if err != nil {
		return "", fmt.Errorf("invalid BindAddr: %w", err)
	}

	// Check if we have an interface
	if iface, _ := net.InterfaceByName(ifname); iface != nil {
		addrs, err := iface.Addrs()
		if err != nil {
			return "", fmt.Errorf("failed to get interface addresses: %w", err)
		}
		if len(addrs) == 0 {
			return "", fmt.Errorf("interface '%s' has no addresses", ifname)
		}

		// If there is no bind IP, pick an address
		if bindIP == "0.0.0.0" {
			addr, err := getBindIPFromNetworkInterface(addrs)
			if err != nil {
				return "", fmt.Errorf("ip scan on %s: %w", ifname, err)
			}
			return addr, nil
		}
		// If there is a bind IP, ensure it is available
		for _, a := range addrs {
			addr, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if addr.IP.String() == bindIP {
				return bindIP, nil
			}
		}
		return "", fmt.Errorf("interface '%s' has no '%s' address", ifname, bindIP)
	}
	if bindIP == "0.0.0.0" {
		// if we're not bound to a specific IP, let's use a suitable private IP address.
		ipStr, err := sockaddr.GetPrivateIP()
		if err != nil {
			return "", fmt.Errorf("failed to get private interface addresses: %w", err)
		}

		// if we could not find a private address, we need to expand our search to a public
		// ip address
		if ipStr == "" {
			ipStr, err = sockaddr.GetPublicIP()
			if err != nil {
				return "", fmt.Errorf("failed to get public interface addresses: %w", err)
			}
		}

		if ipStr == "" {
			return "", fmt.Errorf("no private IP address found, and explicit IP not provided")
		}

		parsed := net.ParseIP(ipStr)
		if parsed == nil {
			return "", fmt.Errorf("failed to parse private IP address: %q", ipStr)
		}
		bindIP = parsed.String()
	}
	return bindIP, nil
}

// SetupNetworkConfig tries to find an appropriate bindIP to bind and propagate.
func (c *Config) SetupNetworkConfig() (err error) {
	address := net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))
	c.BindAddr, err = getBindIP(c.Interface, address)
	if err != nil {
		return err
	}

	address = net.JoinHostPort(c.MemberlistConfig.BindAddr, strconv.Itoa(c.MemberlistConfig.BindPort))
	c.MemberlistConfig.BindAddr, err = getBindIP(c.MemberlistInterface, address)
	if err != nil {
		return err
	}

	if c.MemberlistConfig.AdvertiseAddr != "" {
		advertisePort := c.MemberlistConfig.AdvertisePort
		if advertisePort == 0 {
			advertisePort = c.MemberlistConfig.BindPort
		}
		address := net.JoinHostPort(c.MemberlistConfig.AdvertiseAddr, strconv.Itoa(advertisePort))
		advertiseAddr, _, err := addrParts(address)
		if err != nil {
			return err
		}
		c.MemberlistConfig.AdvertiseAddr = advertiseAddr
	}
	return nil
}
