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

package bufpool

import (
	"bytes"
	"sync"
)

// BufPool maintains a buffer pool.
type BufPool struct {
	p sync.Pool
}

// New creates a new BufPool.
func New() *BufPool {
	return &BufPool{
		p: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// Put resets the buffer and puts it back to the pool.
func (p *BufPool) Put(b *bytes.Buffer) {
	b.Reset()
	p.p.Put(b)
}

// Get returns an empty buffer from the pool. It creates a new buffer, if there
// is no bytes.Buffer available in the pool.
func (p *BufPool) Get() *bytes.Buffer {
	return p.p.Get().(*bytes.Buffer)
}
