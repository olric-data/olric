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

package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/olric-data/olric/config"
	"github.com/olric-data/olric/internal/roundrobin"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	mu sync.RWMutex

	config     *config.Client
	clients    map[string]*redis.Client
	roundRobin *roundrobin.RoundRobin
}

func NewClient(c *config.Client) *Client {
	if c == nil {
		c = config.NewClient()
		err := c.Sanitize()
		if err != nil {
			panic(fmt.Sprintf("failed to sanitize client config: %s", err))
		}
	}
	return &Client{
		config:     c,
		clients:    make(map[string]*redis.Client),
		roundRobin: roundrobin.New(nil),
	}
}

func (c *Client) Addresses() map[string]struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addresses := make(map[string]struct{})
	for address, _ := range c.clients {
		addresses[address] = struct{}{}
	}
	return addresses
}

func (c *Client) Get(addr string) *redis.Client {
	c.mu.RLock()
	rc, ok := c.clients[addr]
	if ok {
		c.mu.RUnlock()
		return rc
	}
	c.mu.RUnlock()

	// Need the lock for writing, we modify c.clients map and the round-robin
	// implementation updates its internal state.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Need to check again, because another goroutine may have updated clients
	// between our calls to RUnlock and Lock.
	if rc, ok = c.clients[addr]; ok {
		return rc
	}

	opt := c.config.RedisOptions()
	opt.Protocol = 2
	opt.Addr = addr
	rc = redis.NewClient(opt)
	c.clients[addr] = rc
	c.roundRobin.Add(addr)
	return rc
}

func (c *Client) pickNodeRoundRobin() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addr, err := c.roundRobin.Get()
	if err == roundrobin.ErrEmptyInstance {
		return "", fmt.Errorf("no available client found")
	}
	if err != nil {
		return "", err
	}
	return addr, nil
}

func (c *Client) Pick() (*redis.Client, error) {
	addr, err := c.pickNodeRoundRobin()
	if err != nil {
		return nil, err
	}
	return c.Get(addr), nil
}

func (c *Client) Close(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	rc, ok := c.clients[addr]
	if ok {
		err := rc.Close()
		if err != nil {
			return err
		}
		c.roundRobin.Delete(addr)
		delete(c.clients, addr)
	}

	return nil
}

func (c *Client) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, rc := range c.clients {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := rc.Close(); err != nil {
			return err
		}
		delete(c.clients, addr)
		c.roundRobin.Delete(addr)
	}

	return nil
}
