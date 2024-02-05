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

// delegate is a struct which implements memberlist.Delegate interface.
type delegate struct {
	meta []byte
}

// newDelegate returns a new delegate instance.
func (d *Discovery) newDelegate() (delegate, error) {
	data, err := d.member.Encode()
	if err != nil {
		return delegate{}, err
	}
	return delegate{
		meta: data,
	}, nil
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (d delegate) NodeMeta(limit int) []byte {
	return d.meta
}

// NotifyMsg is called when a user-data message is received.
func (d delegate) NotifyMsg(data []byte) {}

// GetBroadcasts is called when user data messages can be broadcast.
func (d delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState is used for a TCP Push/Pull.
func (d delegate) LocalState(join bool) []byte { return nil }

// MergeRemoteState is invoked after a TCP Push/Pull.
func (d delegate) MergeRemoteState(buf []byte, join bool) {}
