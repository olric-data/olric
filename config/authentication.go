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

import "errors"

type Authentication struct {
	Enabled  bool
	Username string
	Password string
}

func (a *Authentication) Sanitize() error {
	// Nothing to do
	return nil
}

func (a *Authentication) Validate() error {
	if a.Enabled {
		if a.Username == "" {
			return errors.New("if authentication is enabled, username cannot be empty")
		}
		if a.Password == "" {
			return errors.New("if authentication is enabled, password cannot be empty")
		}
	}
	return nil
}

// Interface guard
var _ IConfig = (*Authentication)(nil)
