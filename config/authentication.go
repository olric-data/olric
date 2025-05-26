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

package config

import "strings"

type Authentication struct {
	Password string
}

// Sanitize ensures the Authentication configuration is pre-processed and prepared for use, with no changes currently applied.
func (a *Authentication) Sanitize() error {
	a.Password = strings.TrimSpace(a.Password)
	return nil
}

// Validate checks the current Authentication configuration for validity and returns an error if issues are found.
func (a *Authentication) Validate() error {
	// Nothing to do
	return nil
}

// Enabled checks if authentication is enabled by verifying if the password is set and returns true if it is configured.
func (a *Authentication) Enabled() bool {
	return len(a.Password) > 0
}

// Interface guard
var _ IConfig = (*Authentication)(nil)
