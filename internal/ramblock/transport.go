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

package ramblock

import (
	"fmt"
	"io"

	"github.com/olric-data/olric/internal/ramblock/table"
	"github.com/olric-data/olric/pkg/storage"
)

type transferIterator struct {
	storage *RamBlock
}

func (t *transferIterator) Next() bool {
	for _, tb := range t.storage.tables {
		if tb.State() != table.RecycledState {
			return true
		}
	}
	return false
}

func (t *transferIterator) Drop(index int) error {
	if len(t.storage.tables) == 0 {
		return fmt.Errorf("there is no table to drop")
	}

	tb := t.storage.tables[index]
	t.storage.tables = append(t.storage.tables[:index], t.storage.tables[index+1:]...)
	delete(t.storage.tablesByCoefficient, tb.Coefficient())

	return nil
}

func (t *transferIterator) Export() ([]byte, int, error) {
	for index, t := range t.storage.tables {
		if t.State() == table.RecycledState {
			continue
		}

		data, err := table.Encode(t)
		if err != nil {
			return nil, 0, err
		}
		return data, index, nil
	}
	return nil, 0, io.EOF
}

func (rb *RamBlock) Import(data []byte, f func(uint64, storage.Entry) error) error {
	tb, err := table.Decode(data)
	if err != nil {
		return err
	}

	var callbackErr error
	tb.Range(func(hkey uint64, e storage.Entry) bool {
		callbackErr = f(hkey, e)
		return callbackErr == nil
	})
	return callbackErr
}

func (rb *RamBlock) TransferIterator() storage.TransferIterator {
	return &transferIterator{
		storage: rb,
	}
}
