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

package olric

import (
	"errors"
	"time"

	"github.com/buraksezer/olric/internal/resp"
	"github.com/buraksezer/olric/pkg/storage"
)

var ErrNilResponse = errors.New("storage entry is nil")

type GetResponse struct {
	entry storage.Entry
}

func (g *GetResponse) Scan(v interface{}) error {
	if g.entry == nil {
		return ErrNilResponse
	}
	return resp.Scan(g.entry.Value(), v)
}

func (g *GetResponse) Int() (int, error) {
	v := new(int)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) String() (string, error) {
	v := new(string)
	err := g.Scan(v)
	if err != nil {
		return "", err
	}
	return *v, nil
}

func (g *GetResponse) Int8() (int8, error) {
	v := new(int8)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Int16() (int16, error) {
	v := new(int16)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Int32() (int32, error) {
	v := new(int32)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Int64() (int64, error) {
	v := new(int64)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Uint() (uint, error) {
	v := new(uint)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Uint8() (uint8, error) {
	v := new(uint8)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Uint16() (uint16, error) {
	v := new(uint16)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Uint32() (uint32, error) {
	v := new(uint32)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Uint64() (uint64, error) {
	v := new(uint64)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Float32() (float32, error) {
	v := new(float32)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Float64() (float64, error) {
	v := new(float64)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Bool() (bool, error) {
	v := new(bool)
	err := g.Scan(v)
	if err != nil {
		return false, err
	}
	return *v, nil
}

func (g *GetResponse) Time() (time.Time, error) {
	v := new(time.Time)
	err := g.Scan(v)
	if err != nil {
		return time.Time{}, err
	}
	return *v, nil
}

func (g *GetResponse) Duration() (time.Duration, error) {
	v := new(time.Duration)
	err := g.Scan(v)
	if err != nil {
		return 0, err
	}
	return *v, nil
}

func (g *GetResponse) Byte() ([]byte, error) {
	v := new([]byte)
	err := g.Scan(v)
	if err != nil {
		return nil, err
	}
	return *v, nil
}

func (g *GetResponse) TTL() int64 {
	return g.entry.TTL()
}

func (g *GetResponse) Timestamp() int64 {
	return g.entry.Timestamp()
}
