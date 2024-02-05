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

package discovery

import (
	"testing"

	"github.com/buraksezer/olric/internal/testutil"
)

func TestMembers(t *testing.T) {
	c1 := testutil.NewConfig()
	member1 := NewMember(c1)

	c2 := testutil.NewConfig()
	member2 := NewMember(c2)

	t.Run("Name", func(t *testing.T) {
		if member1.String() != c1.MemberlistConfig.Name {
			t.Fatalf("Expected member name: %s. Got: %s", c1.MemberlistConfig.Name, member1.Name)
		}
	})

	t.Run("CompareByID", func(t *testing.T) {
		if !member1.CompareByID(member1) {
			t.Fatalf("members were the same")
		}

		if member1.CompareByID(member2) {
			t.Fatalf("members were different")
		}
	})

	t.Run("CompareByName", func(t *testing.T) {
		if !member1.CompareByName(member1) {
			t.Fatalf("members were the same")
		}

		if member1.CompareByName(member2) {
			t.Fatalf("members were different")
		}
	})

	t.Run("Encode/Decode", func(t *testing.T) {
		data, err := member1.Encode()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		decoded, err := NewMemberFromMetadata(data)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !member1.CompareByID(decoded) {
			t.Fatalf("Decoded member is different")
		}

		if !member1.CompareByName(decoded) {
			t.Fatalf("Decoded member is different")
		}
	})
}
