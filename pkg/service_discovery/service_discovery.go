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

/*Package service_discovery provides ServiceDiscovery interface for plugins*/
package service_discovery // import "github.com/olric-data/olric/pkg/service_discovery"

import "log"

// ServiceDiscovery represents an interface for discovering, registering nodes within an Olric cluster.
type ServiceDiscovery interface {

	// Initialize prepares the service discovery plugin for use and ensures it is ready for further operations.
	Initialize() error

	// SetConfig sets the configuration for the service discovery plugin using the provided map of settings.
	SetConfig(c map[string]interface{}) error

	// SetLogger assigns a custom logger to the service discovery instance for logging operations.
	SetLogger(l *log.Logger)

	// Register registers the current node in the service discovery directory, enabling it to participate in the cluster.
	Register() error

	// Deregister removes the current node from the service discovery directory and stops its participation in the cluster.
	Deregister() error

	// DiscoverPeers retrieves a list of available peers in the cluster and returns their addresses or an error if any occurs.
	DiscoverPeers() ([]string, error)

	// Close gracefully terminates all operations and releases resources associated with the service discovery instance.
	Close() error
}
