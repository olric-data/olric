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

package kvstore

import (
	"fmt"
	"io"

	"github.com/buraksezer/olric/internal/kvstore/table"
	"github.com/buraksezer/olric/pkg/storage"
)

type transferIterator struct {
	storage *KVStore
}

func (t *transferIterator) Next() bool {
	return len(t.storage.tables) != 0
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

func (k *KVStore) Import(data []byte, f func(uint64, storage.Entry) error) error {
	tb, err := table.Decode(data)
	if err != nil {
		return err
	}

	tb.Range(func(hkey uint64, e storage.Entry) bool {
		return f(hkey, e) == nil
	})
	return err
}

func (k *KVStore) TransferIterator() storage.TransferIterator {
	return &transferIterator{
		storage: k,
	}
}
