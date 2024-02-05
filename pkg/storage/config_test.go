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
	"reflect"
	"testing"
)

func Test_Config(t *testing.T) {
	c := NewConfig(nil)
	c.Add("string-key", "string-value")
	c.Add("integer-key", 65786)

	t.Run("Get", func(t *testing.T) {
		sv, err := c.Get("string-key")
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if sv.(string) != "string-value" {
			t.Fatalf("Expected string-value. Got %v", sv)
		}

		iv, err := c.Get("integer-key")
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if iv.(int) != 65786 {
			t.Fatalf("Expected integer-value. Got %v", iv)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		c.Delete("string-key")
		_, err := c.Get("string-key")
		if err == nil {
			t.Fatalf("Expected an error. Got %v", err)
		}
	})

	t.Run("Copy", func(t *testing.T) {
		copied := c.Copy()
		if copied == c {
			t.Fatalf("New config is the same with the previous one")
		}
		if !reflect.DeepEqual(c, copied) {
			t.Fatalf("New config is not idential with the previous one")
		}
	})
}
