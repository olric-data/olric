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

/*Package service_discovery provides ServiceDiscovery interface for plugins*/
package service_discovery // import "github.com/buraksezer/olric/pkg/service_discovery"

import "log"

// ServiceDiscovery is an interface that defines a unified API for service discovery plugins.
type ServiceDiscovery interface {
	// Initialize initializes the plugin: registers some internal data structures, clients etc.
	Initialize() error

	// SetConfig registers plugin configuration
	SetConfig(c map[string]interface{}) error

	// SetLogger sets an appropriate
	SetLogger(l *log.Logger)

	// Register registers this node to a service discovery directory.
	Register() error

	// Deregister removes this node from a service discovery directory.
	Deregister() error

	// DiscoverPeers returns a list of known Olric nodes.
	DiscoverPeers() ([]string, error)

	// Close stops underlying goroutines, if there is any. It should be a blocking call.
	Close() error
}
