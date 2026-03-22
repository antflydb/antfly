package sim

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	storedb "github.com/antflydb/antfly/src/store/db"
	"github.com/cespare/xxhash/v2"
)

type TraceEvent struct {
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Message string    `json:"message"`
}

type ClusterDigest struct {
	At            time.Time `json:"at"`
	Note          string    `json:"note"`
	Hash          string    `json:"hash"`
	Tables        int       `json:"tables"`
	Shards        int       `json:"shards"`
	HealthyStores int       `json:"healthy_stores"`
	ActiveSplits  int       `json:"active_splits"`
	ActiveMerges  int       `json:"active_merges"`
}

type TraceRecorder struct {
	mu      sync.Mutex
	events  []TraceEvent
	digests []ClusterDigest
}

func NewTraceRecorder() *TraceRecorder {
	return &TraceRecorder{}
}

func (r *TraceRecorder) RecordEvent(at time.Time, kind, format string, args ...any) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, TraceEvent{
		At:      at,
		Kind:    kind,
		Message: fmt.Sprintf(format, args...),
	})
}

func (r *TraceRecorder) RecordDigest(snapshot *ClusterSnapshot, note string) error {
	if r == nil || snapshot == nil {
		return nil
	}
	digest, err := computeClusterDigest(snapshot, note)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.digests = append(r.digests, digest)
	return nil
}

func (r *TraceRecorder) Events() []TraceEvent {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]TraceEvent(nil), r.events...)
}

func (r *TraceRecorder) Digests() []ClusterDigest {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ClusterDigest(nil), r.digests...)
}

func (r *TraceRecorder) CompactTrace(maxEvents, maxDigests int) string {
	if r == nil {
		return ""
	}
	events := r.Events()
	digests := r.Digests()
	if maxEvents > 0 && len(events) > maxEvents {
		events = events[len(events)-maxEvents:]
	}
	if maxDigests > 0 && len(digests) > maxDigests {
		digests = digests[len(digests)-maxDigests:]
	}

	parts := make([]string, 0, len(events)+len(digests)+2)
	if len(events) > 0 {
		parts = append(parts, "events:")
		for _, event := range events {
			parts = append(parts, fmt.Sprintf(
				"  %s [%s] %s",
				event.At.Format(time.RFC3339Nano),
				event.Kind,
				event.Message,
			))
		}
	}
	if len(digests) > 0 {
		parts = append(parts, "digests:")
		for _, digest := range digests {
			parts = append(parts, fmt.Sprintf(
				"  %s note=%s hash=%s tables=%d shards=%d healthy=%d active_splits=%d active_merges=%d",
				digest.At.Format(time.RFC3339Nano),
				digest.Note,
				digest.Hash,
				digest.Tables,
				digest.Shards,
				digest.HealthyStores,
				digest.ActiveSplits,
				digest.ActiveMerges,
			))
		}
	}
	return strings.Join(parts, "\n")
}

func computeClusterDigest(snapshot *ClusterSnapshot, note string) (ClusterDigest, error) {
	type tableDigest struct {
		Name   string   `json:"name"`
		Shards []string `json:"shards"`
	}
	type shardDigest struct {
		ID         string   `json:"id"`
		Table      string   `json:"table"`
		State      string   `json:"state"`
		Lead       string   `json:"lead"`
		Peers      []string `json:"peers"`
		Reported   []string `json:"reported"`
		SplitPhase string   `json:"split_phase,omitempty"`
		MergePhase string   `json:"merge_phase,omitempty"`
		Flags      string   `json:"flags,omitempty"`
	}
	type storeDigest struct {
		ID     string   `json:"id"`
		State  string   `json:"state"`
		Shards []string `json:"shards"`
	}

	tableNames := make([]string, 0, len(snapshot.Tables))
	for tableName := range snapshot.Tables {
		tableNames = append(tableNames, tableName)
	}
	slices.Sort(tableNames)

	var tables []tableDigest
	for _, tableName := range tableNames {
		table := snapshot.Tables[tableName]
		shardIDs := make([]types.ID, 0, len(table.Shards))
		for shardID := range table.Shards {
			shardIDs = append(shardIDs, shardID)
		}
		slices.Sort(shardIDs)
		shards := make([]string, 0, len(shardIDs))
		for _, shardID := range shardIDs {
			conf := table.Shards[shardID]
			shards = append(shards, fmt.Sprintf("%s:%s", shardID, conf.ByteRange))
		}
		tables = append(tables, tableDigest{Name: tableName, Shards: shards})
	}

	shardIDs := make([]types.ID, 0, len(snapshot.Shards))
	for shardID := range snapshot.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.Sort(shardIDs)
	shards := make([]shardDigest, 0, len(shardIDs))
	activeSplits := 0
	activeMerges := 0
	for _, shardID := range shardIDs {
		status := snapshot.Shards[shardID]
		peers := peerSetStrings(status.Peers)
		reported := peerSetStrings(status.ReportedBy)
		lead := ""
		if status.RaftStatus != nil {
			lead = types.ID(status.RaftStatus.Lead).String()
		}
		splitPhase := ""
		if status.SplitState != nil {
			splitPhase = status.SplitState.GetPhase().String()
			activeSplits++
		} else if status.State.Transitioning() ||
			status.State == store.ShardState_SplitOffPreSnap ||
			status.SplitReplayRequired && !status.SplitCutoverReady {
			activeSplits++
		}
		mergePhase := ""
		if status.MergeState != nil {
			mergePhase = status.MergeState.GetPhase().String()
			if status.MergeState.GetPhase() != storedb.MergeState_PHASE_NONE {
				activeMerges++
			}
		} else if status.State == store.ShardState_PreMerge {
			activeMerges++
		}
		flags := transitionFlags(status)
		shards = append(shards, shardDigest{
			ID:         shardID.String(),
			Table:      status.Table,
			State:      status.State.String(),
			Lead:       lead,
			Peers:      peers,
			Reported:   reported,
			SplitPhase: splitPhase,
			MergePhase: mergePhase,
			Flags:      flags,
		})
	}

	storeIDs := make([]types.ID, 0, len(snapshot.Stores))
	for storeID := range snapshot.Stores {
		storeIDs = append(storeIDs, storeID)
	}
	slices.Sort(storeIDs)
	stores := make([]storeDigest, 0, len(storeIDs))
	healthyStores := 0
	for _, storeID := range storeIDs {
		status := snapshot.Stores[storeID]
		shardIDs := make([]types.ID, 0, len(status.Shards))
		for shardID := range status.Shards {
			shardIDs = append(shardIDs, shardID)
		}
		slices.Sort(shardIDs)
		storeShards := make([]string, 0, len(shardIDs))
		for _, shardID := range shardIDs {
			storeShards = append(storeShards, shardID.String())
		}
		if status.IsReachable() {
			healthyStores++
		}
		stores = append(stores, storeDigest{
			ID:     storeID.String(),
			State:  status.State.String(),
			Shards: storeShards,
		})
	}

	payload, err := json.Marshal(struct {
		Tables []tableDigest `json:"tables"`
		Shards []shardDigest `json:"shards"`
		Stores []storeDigest `json:"stores"`
	}{
		Tables: tables,
		Shards: shards,
		Stores: stores,
	})
	if err != nil {
		return ClusterDigest{}, err
	}

	return ClusterDigest{
		At:            snapshot.Now,
		Note:          note,
		Hash:          fmt.Sprintf("%016x", xxhash.Sum64(payload)),
		Tables:        len(snapshot.Tables),
		Shards:        len(snapshot.Shards),
		HealthyStores: healthyStores,
		ActiveSplits:  activeSplits,
		ActiveMerges:  activeMerges,
	}, nil
}

func peerSetStrings(peerSet map[types.ID]struct{}) []string {
	if len(peerSet) == 0 {
		return nil
	}
	ids := make([]types.ID, 0, len(peerSet))
	for peerID := range peerSet {
		ids = append(ids, peerID)
	}
	slices.Sort(ids)
	out := make([]string, 0, len(ids))
	for _, peerID := range ids {
		out = append(out, peerID.String())
	}
	return out
}

func transitionFlags(status *store.ShardStatus) string {
	if status == nil {
		return ""
	}
	flags := make([]string, 0, 8)
	if status.HasSnapshot {
		flags = append(flags, "snapshot")
	}
	if status.Initializing {
		flags = append(flags, "initializing")
	}
	if status.SplitReplayRequired {
		flags = append(flags, "replay_required")
	}
	if status.SplitCutoverReady {
		flags = append(flags, "cutover_ready")
	}
	if status.MergeState != nil {
		if status.MergeState.GetCopyCompleted() {
			flags = append(flags, "merge_copy_done")
		}
		if status.MergeState.GetCaptureDeltas() {
			flags = append(flags, "merge_capture")
		}
		if status.MergeState.GetAcceptDonorRange() {
			flags = append(flags, "merge_accept")
		}
		if status.MergeState.GetDenyDonorWrites() {
			flags = append(flags, "merge_deny_writes")
		}
	}
	return strings.Join(flags, ",")
}

func splitFlags(status *store.ShardStatus) string {
	if status == nil {
		return ""
	}
	flags := make([]string, 0, 4)
	if status.HasSnapshot {
		flags = append(flags, "snapshot")
	}
	if status.Initializing {
		flags = append(flags, "initializing")
	}
	if status.SplitReplayRequired {
		flags = append(flags, "replay_required")
	}
	if status.SplitCutoverReady {
		flags = append(flags, "cutover_ready")
	}
	return strings.Join(flags, ",")
}
