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

package storage

import (
	"fmt"
	"sync"
)

// Config defines a new storage engine configuration
type Config struct {
	m map[string]interface{}
	sync.RWMutex
}

// NewConfig returns a new Config
func NewConfig(cfg map[string]interface{}) *Config {
	if cfg == nil {
		cfg = make(map[string]interface{})
	}
	return &Config{
		m: cfg,
	}
}

// Add adds a new key/value pair to Config
func (c *Config) Add(key string, value interface{}) {
	c.Lock()
	defer c.Unlock()
	c.m[key] = value
}

// Get loads a configuration variable with its key, otherwise it returns an error.
func (c *Config) Get(key string) (interface{}, error) {
	c.Lock()
	defer c.Unlock()
	value, ok := c.m[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return value, nil
}

// Delete deletes a configuration variable with its key.
func (c *Config) Delete(key string) {
	c.Lock()
	defer c.Unlock()
	delete(c.m, key)
}

// Copy creates a thread-safe copy of the existing Config struct.
func (c *Config) Copy() *Config {
	c.Lock()
	defer c.Unlock()
	n := &Config{
		m: make(map[string]interface{}),
	}
	for key, value := range c.m {
		n.m[key] = value
	}
	return n
}

// ToMap casts Config to map[string]interface{} type.
func (c *Config) ToMap() map[string]interface{} {
	return c.Copy().m
}
