// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package foreign

import (
	"encoding/json"

	"github.com/antflydb/antfly/lib/evaluator"
)

// EvaluateFilter evaluates a bleve-style filter query against a decoded row.
// Returns true if the row matches the filter, false otherwise.
// An empty/nil filter returns (true, nil) — equivalent to match_all.
//
// NOTE: This function re-parses the JSON filter on every call. For hot paths
// (e.g., per-row CDC processing), use evaluator.ParseFilter once and call
// FilterNode.Evaluate directly. See ReplicationWorker.Run for an example.
func EvaluateFilter(filter json.RawMessage, doc map[string]any) (bool, error) {
	node, err := evaluator.ParseFilter(filter)
	if err != nil {
		return false, err
	}
	return node.Evaluate(doc)
}
