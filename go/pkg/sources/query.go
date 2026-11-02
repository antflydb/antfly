// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package sources

// documentIDsQuery is the tagged native query representation, also valid inside
// boolean clauses. Bare {"ids": ...} is not accepted by the rc.6 data plane.
func documentIDsQuery(ids []string) map[string]any {
	return map[string]any{"doc_id": map[string]any{"ids": ids}}
}
