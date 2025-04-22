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

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultDialTimeout     = 5 * time.Second
	DefaultKeepalive       = 5 * time.Minute
	DefaultReadTimeout     = 3 * time.Second
	DefaultIdleTimeout     = 5 * time.Minute
	DefaultMinRetryBackoff = 8 * time.Millisecond
	DefaultMaxRetryBackoff = 512 * time.Millisecond
	DefaultMaxRetries      = 3
)

// Client denotes configuration for TCP clients in Olric and the official Golang client.
type Client struct {
	Authentication *Authentication

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration

	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration

	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is ReadTimeout.
	WriteTimeout time.Duration

	// Dialer creates new network connection and has priority over
	// Network and Addr options.
	Dialer func(ctx context.Context, network, addr string) (net.Conn, error)

	// Hook that is called when new connection is established.
	OnConnect func(ctx context.Context, cn *redis.Conn) error

	// Maximum number of retries before giving up.
	// Default is 3 retries; -1 (not 0) disables retries.
	MaxRetries int

	// Minimum backoff between each retry.
	// Default is 8 milliseconds; -1 disables backoff.
	MinRetryBackoff time.Duration

	// Maximum backoff between each retry.
	// Default is 512 milliseconds; -1 disables backoff.
	MaxRetryBackoff time.Duration

	// Type of connection pool.
	// true for FIFO pool, false for LIFO pool.
	// Note that fifo has higher overhead compared to lifo.
	PoolFIFO bool

	// Maximum number of socket connections.
	// Default is 10 connections per every available CPU as reported by runtime.GOMAXPROCS.
	PoolSize int

	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConns int

	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	MaxConnAge time.Duration

	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration

	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	IdleTimeout time.Duration

	// TLS Config to use. When set TLS will be negotiated.
	TLSConfig *tls.Config

	// Limiter interface used to implemented circuit breaker or rate limiter.
	Limiter redis.Limiter
}

// NewClient returns a new configuration object for clients.
func NewClient() *Client {
	c := &Client{
		Authentication: &Authentication{},
	}
	err := c.Sanitize()
	if err != nil {
		panic(fmt.Sprintf("failed to create a new client configuration: %v", err))
	}
	return c
}

// Sanitize sets default values to empty configuration variables, if it's possible.
func (c *Client) Sanitize() error {
	if err := c.Authentication.Sanitize(); err != nil {
		return fmt.Errorf("failed to sanitize authentication configuration: %w", err)
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	if c.Dialer == nil {
		c.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			netDialer := &net.Dialer{
				Timeout:   c.DialTimeout,
				KeepAlive: DefaultKeepalive,
			}
			if c.TLSConfig == nil {
				return netDialer.DialContext(ctx, network, addr)
			}
			return tls.DialWithDialer(netDialer, network, addr, c.TLSConfig)
		}
	}
	if c.PoolSize == 0 {
		c.PoolSize = 10 * runtime.GOMAXPROCS(0)
	}
	switch c.ReadTimeout {
	case -1:
		c.ReadTimeout = 0
	case 0:
		c.ReadTimeout = DefaultReadTimeout
	}
	switch c.WriteTimeout {
	case -1:
		c.WriteTimeout = 0
	case 0:
		c.WriteTimeout = c.ReadTimeout
	}
	if c.PoolTimeout == 0 {
		c.PoolTimeout = c.ReadTimeout + time.Second
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}

	if c.MaxRetries == -1 {
		c.MaxRetries = 0
	} else if c.MaxRetries == 0 {
		c.MaxRetries = DefaultMaxRetries
	}
	switch c.MinRetryBackoff {
	case -1:
		c.MinRetryBackoff = 0
	case 0:
		c.MinRetryBackoff = DefaultMinRetryBackoff
	}
	switch c.MaxRetryBackoff {
	case -1:
		c.MaxRetryBackoff = 0
	case 0:
		c.MaxRetryBackoff = DefaultMaxRetryBackoff
	}

	return nil
}

// Validate finds errors in the current configuration.
func (c *Client) Validate() error {
	if err := c.Authentication.Validate(); err != nil {
		return fmt.Errorf("failed to validate authentication configuration: %w", err)
	}
	return nil
}

func (c *Client) RedisOptions() *redis.Options {
	// Note: IdleCheckFrequency is gone since go-redis no longer checks idle connections.
	// See https://github.com/redis/go-redis/discussions/2635
	options := &redis.Options{
		Network:         "tcp",
		Dialer:          c.Dialer,
		OnConnect:       c.OnConnect,
		MaxRetries:      c.MaxRetries,
		MinRetryBackoff: c.MinRetryBackoff,
		MaxRetryBackoff: c.MaxRetryBackoff,
		DialTimeout:     c.DialTimeout,
		ReadTimeout:     c.ReadTimeout,
		WriteTimeout:    c.WriteTimeout,
		PoolFIFO:        c.PoolFIFO,
		PoolSize:        c.PoolSize,
		MinIdleConns:    c.MinIdleConns,
		ConnMaxLifetime: c.MaxConnAge,
		PoolTimeout:     c.PoolTimeout,
		ConnMaxIdleTime: c.IdleTimeout,
		TLSConfig:       c.TLSConfig,
		Limiter:         c.Limiter,
	}
	if c.Authentication.Enabled {
		options.Username = c.Authentication.Username
		options.Password = c.Authentication.Password
	}
	return options
}

// Interface guard
var _ IConfig = (*Client)(nil)
