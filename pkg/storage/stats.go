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

// Stats defines metrics exposed by a storage engine implementation.
type Stats struct {
	// Currently allocated memory by the engine.
	Allocated int

	// Used portion of allocated memory
	Inuse int

	// Deleted portions of allocated memory.
	Garbage int

	// Total number of keys hosted by the engine instance.
	Length int

	// Number of tables hosted by the engine instance.
	NumTables int

	// Any other metrics that's specific to an engine implementation.
	Extras map[string]interface{}
}
