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

// queryFreshness is how a query response says what it read from: a retained
// snapshot, whose capture proves indexes caught up, or live state, because the
// server's storage (Lite) can't retain one. Live reads show writes as soon as
// the writer's sync level made them visible, not before. A response with
// neither is not understood, so the source gates keep rejecting it.
type queryFreshness struct {
	Snapshot string `json:"remote_snapshot"`
	LiveRead bool   `json:"live_read"`
}

// A live read can prove that a matching row is visible, but an empty live read
// cannot prove absence: an acknowledged insert may still be awaiting indexing.
func (f queryFreshness) readable() bool { return f.Snapshot != "" || f.LiveRead }

// documentIDsQuery is the tagged native query representation, also valid inside
// boolean clauses. Bare {"ids": ...} is not accepted by the rc.6 data plane.
func documentIDsQuery(ids []string) map[string]any {
	return map[string]any{"doc_id": map[string]any{"ids": ids}}
}
