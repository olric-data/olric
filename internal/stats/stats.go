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

package stats

import "sync/atomic"

// Int64Counter is a cumulative metric that represents a single monotonically
// increasing counter whose value can only increase or be reset to zero on restart.
type Int64Counter struct {
	counter int64
}

// NewInt64Counter returns a new Int64Counter
func NewInt64Counter() *Int64Counter {
	return &Int64Counter{}
}

// Increase increases the counter by delta.
func (c *Int64Counter) Increase(delta int64) {
	atomic.AddInt64(&c.counter, delta)
}

// Read returns the current value of counter.
func (c *Int64Counter) Read() int64 {
	return atomic.LoadInt64(&c.counter)
}

// Reset sets zero to the underlying counter.
func (c *Int64Counter) Reset() {
	atomic.StoreInt64(&c.counter, 0)
}

// Int64Gauge is a metric that represents a single numerical value that can
// arbitrarily go up and down.
type Int64Gauge struct {
	gauge int64
}

// NewInt64Gauge returns a new Int64Gauge
func NewInt64Gauge() *Int64Gauge {
	return &Int64Gauge{}
}

// Increase increases the gauge by delta.
func (c *Int64Gauge) Increase(delta int64) {
	atomic.AddInt64(&c.gauge, delta)
}

// Decrease decreases the counter by delta.
func (c *Int64Gauge) Decrease(delta int64) {
	atomic.AddInt64(&c.gauge, -1*delta)
}

// Read returns the current value of gauge.
func (c *Int64Gauge) Read() int64 {
	return atomic.LoadInt64(&c.gauge)
}

// Reset sets zero to the underlying gauge.
func (c *Int64Gauge) Reset() {
	atomic.StoreInt64(&c.gauge, 0)
}
