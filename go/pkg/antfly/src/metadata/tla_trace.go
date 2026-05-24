// Copyright 2026 Antfly, Inc.
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

//go:build with_tla

package metadata

import (
	"encoding/hex"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/tracing"
	"github.com/google/uuid"
)

// traceCheckPredicates emits a TLA+ trace event when the orchestrator begins
// the intent-write phase (which includes OCC predicate checks on each shard).
func (ms *MetadataStore) traceCheckPredicates(txnID uuid.UUID, shards map[types.ID]struct{}) {
	tw := ms.traceWriter
	if tw == nil {
		return
	}
	shardIDs := make([]string, 0, len(shards))
	for id := range shards {
		shardIDs = append(shardIDs, id.String())
	}
	tw.TraceAntflyEvent(&tracing.AntflyTracingEvent{
		Name:  "CheckPredicates",
		TxnID: hex.EncodeToString(txnID[:]),
		State: map[string]any{
			"shards": shardIDs,
		},
	})
}
