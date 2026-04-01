//go:build with_tla

package metadata

import (
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/tracing"
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
		TxnID: txnID.String(),
		State: map[string]any{
			"shards": shardIDs,
		},
	})
}
