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

package db

//go:generate protoc --go_out=. --go_opt=paths=source_relative types.proto

import (
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// DBStorageStats holds statistics for Pebble storage.
type DBStorageStats struct {
	// This is the disk size of storage for split computations.
	DiskSize uint64 `json:"disk_size"`
	Empty    bool   `json:"empty,omitempty"`
}

// DBStats holds statistics for both the Bleve index and Pebble DB.
type DBStats struct {
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	// This is the total disk size footprint of the shard.
	// DiskSize uint64        `json:"disk_size"`
	Storage   *DBStorageStats               `json:"storage,omitempty"`
	Indexes   map[string]indexes.IndexStats `json:"indexes,omitempty"`
	Splitting bool                          `json:"splitting,omitempty,omitzero"`
}

type ShardConfig struct {
	ByteRange types.Range                    `json:"byte_range,omitzero,omitempty"`
	Schema    *schema.TableSchema            `json:"schema,omitempty"`
	Indexes   map[string]indexes.IndexConfig `json:"index_configs,omitempty"`
	// RestoreConfig holds configuration for restoring a shard from backup.
	// This is used by the metadata service to track restore configuration needed when scaling.
	RestoreConfig *common.BackupConfig `json:"restore_config,omitempty"`
}

func (sc *ShardConfig) String() string {
	return fmt.Sprintf(
		"ShardConfig{ByteRange: %s, Schema: %v, Indexes: %v}",
		sc.ByteRange,
		sc.Schema,
		sc.Indexes,
	)
}

func (sc *ShardConfig) Equal(other *ShardConfig) bool {
	return sc == nil && other == nil || sc != nil && other != nil &&
		sc.ByteRange.Equal(other.ByteRange) &&
		sc.Schema.Equal(other.Schema) &&
		maps.EqualFunc(sc.Indexes, other.Indexes, func(v1, v2 indexes.IndexConfig) bool {
			return v1.Equal(v2)
		}) &&
		sc.RestoreConfig.Equal(other.RestoreConfig)
}

func NewShardInfo() *ShardInfo {
	return &ShardInfo{
		ShardStats: &DBStats{},
		Peers:      make(map[types.ID]struct{}),
		ReportedBy: make(map[types.ID]struct{}),
		RaftStatus: &common.RaftStatus{},
		// These fields start true so the AND logic in Merge works correctly:
		// if any node reports false, the merged result becomes false.
		HasSnapshot:         true,
		SplitReplayCaughtUp: true,
		SplitCutoverReady:   true,
	}
}

type ShardInfo struct {
	ShardConfig

	ShardStats *DBStats           `json:"shard_stats,omitempty"`
	Peers      common.PeerSet     `json:"peers,omitzero"`
	RaftStatus *common.RaftStatus `json:"raft_status,omitempty"`
	// ReportedBy tracks which nodes actually reported having this shard loaded.
	// This is distinct from Peers, which includes nodes from the Raft voter config.
	// Used by the reconciler to detect shards that are missing on nodes.
	ReportedBy common.PeerSet `json:"reported_by,omitzero"`
	// Whether all peers have a snapshot.
	HasSnapshot bool `json:"has_snapshot,omitempty,omitzero"`
	// Whether any peer is currently initializing.
	// Initializing bool `json:"-"`
	Initializing bool `json:"initializing,omitempty,omitzero"`
	// Whether the shard is currently splitting.
	Splitting bool `json:"splitting,omitempty,omitzero"`
	// SplitState contains the Raft-replicated split state for zero-downtime splits.
	// This is populated from the shard's internal state during status reporting.
	SplitState *SplitState `json:"split_state,omitempty"`
	// SplitReplayRequired indicates that this shard was created from a split archive and
	// must catch up parent-side split deltas before it is considered ready.
	SplitReplayRequired bool `json:"split_replay_required,omitempty,omitzero"`
	// SplitReplayCaughtUp reports whether replay has caught up to the current parent-side
	// fence for this split child shard.
	SplitReplayCaughtUp bool `json:"split_replay_caught_up,omitempty,omitzero"`
	// SplitCutoverReady reports whether the split child has replayed through the
	// parent's final fence and can safely take over reads from the parent.
	SplitCutoverReady bool `json:"split_cutover_ready,omitempty,omitzero"`
	// SplitReplaySeq is the highest split delta sequence applied on this shard.
	SplitReplaySeq uint64 `json:"split_replay_seq,omitempty,omitzero"`
	// SplitParentShardID is the parent shard that produced this split child archive.
	SplitParentShardID types.ID `json:"split_parent_shard_id,omitempty,omitzero"`
}

// IsReadyForSplitReads returns true when a split-off shard can serve reads directly:
// it has a snapshot, is no longer initializing, has an elected Raft leader, and
// if replay is required, it has crossed the final cutover fence.
// This is the single source of truth for post-cutover child readiness — used by
// the routing layer (shouldFallbackToParentShard) and tablemgr (splitOffShardIsReady).
func (s *ShardInfo) IsReadyForSplitReads() bool {
	return s.HasSnapshot && !s.Initializing &&
		s.RaftStatus != nil && s.RaftStatus.Lead != 0 &&
		(!s.SplitReplayRequired || s.SplitCutoverReady)
}

// CanInitiateSplitCutover returns true when a split-off shard has bootstrapped
// enough state that the parent can begin cutover. This is weaker than
// IsReadyForSplitReads because it only requires the child to be live and caught
// up to the parent's current sequence, not through the final cutover fence.
func (s *ShardInfo) CanInitiateSplitCutover() bool {
	return s.HasSnapshot && !s.Initializing &&
		s.RaftStatus != nil && s.RaftStatus.Lead != 0 &&
		(!s.SplitReplayRequired || s.SplitReplayCaughtUp)
}

func (s *ShardInfo) Equal(o *ShardInfo) bool {
	// We don't care about shard stats for equality checks.
	if s == nil || o == nil {
		return s == o
	}
	return s.ShardConfig.Equal(&o.ShardConfig) &&
		s.Peers.Equal(o.Peers) &&
		s.RaftStatus.Equal(o.RaftStatus) &&
		s.SplitReplayRequired == o.SplitReplayRequired &&
		s.SplitReplayCaughtUp == o.SplitReplayCaughtUp &&
		s.SplitCutoverReady == o.SplitCutoverReady &&
		s.SplitReplaySeq == o.SplitReplaySeq &&
		s.SplitParentShardID == o.SplitParentShardID
}

func (s *ShardInfo) DeepCopy() *ShardInfo {
	if s == nil {
		return nil
	}
	// Create a new ShardInfo and copy the fields.
	newShardInfo := &ShardInfo{
		ShardConfig:         s.ShardConfig,
		ShardStats:          &DBStats{},
		Peers:               s.Peers.Copy(),
		ReportedBy:          s.ReportedBy.Copy(),
		RaftStatus:          &common.RaftStatus{},
		SplitReplayRequired: s.SplitReplayRequired,
		SplitReplayCaughtUp: s.SplitReplayCaughtUp,
		SplitCutoverReady:   s.SplitCutoverReady,
		SplitReplaySeq:      s.SplitReplaySeq,
		SplitParentShardID:  s.SplitParentShardID,
	}

	if s.ShardStats != nil {
		*newShardInfo.ShardStats = *s.ShardStats
	}
	if s.RaftStatus != nil {
		*newShardInfo.RaftStatus = *s.RaftStatus
	}
	if s.SplitState != nil {
		newShardInfo.SplitState = proto.Clone(s.SplitState).(*SplitState)
	}

	return newShardInfo
}

func (s *ShardInfo) Merge(nodeID types.ID, o *ShardInfo) {
	if s.ByteRange.Equal(types.Range{}) {
		s.ShardConfig = o.ShardConfig
	}
	if s.Peers == nil {
		s.Peers = make(map[types.ID]struct{})
	}
	// Track which node actually reported this shard (vs just being in the Raft voter list)
	if s.ReportedBy == nil {
		s.ReportedBy = make(map[types.ID]struct{})
	}
	s.ReportedBy.Add(nodeID)

	s.Peers.Add(nodeID)
	for k := range o.Peers {
		s.Peers.Add(k)
	}
	if s.RaftStatus != nil && s.RaftStatus.Lead != 0 {
		if s.RaftStatus.Lead == nodeID {
			s.RaftStatus = o.RaftStatus
			s.ShardStats = o.ShardStats
			s.SplitState = o.SplitState
		}
	}
	if o.RaftStatus != nil && o.RaftStatus.Lead != 0 {
		if o.RaftStatus.Lead == nodeID {
			s.RaftStatus = o.RaftStatus
			s.ShardStats = o.ShardStats
			s.SplitState = o.SplitState
		}
	}
	if s.RaftStatus == nil || s.RaftStatus.Lead == 0 {
		s.RaftStatus = o.RaftStatus
	}
	if s.ShardStats == nil || s.ShardStats.Created.IsZero() {
		s.ShardStats = o.ShardStats
	}
	s.HasSnapshot = s.HasSnapshot && o.HasSnapshot
	s.Initializing = s.Initializing || o.Initializing
	s.Splitting = s.Splitting || o.Splitting
	s.SplitReplayRequired = s.SplitReplayRequired || o.SplitReplayRequired
	s.SplitReplayCaughtUp = s.SplitReplayCaughtUp && o.SplitReplayCaughtUp
	s.SplitCutoverReady = s.SplitCutoverReady && o.SplitCutoverReady
	if o.SplitReplaySeq > s.SplitReplaySeq {
		s.SplitReplaySeq = o.SplitReplaySeq
	}
	if o.SplitParentShardID != 0 {
		s.SplitParentShardID = o.SplitParentShardID
	}
}

// shardInfoJSON is used for JSON marshaling/unmarshaling of ShardInfo.
// It has json.RawMessage for SplitState to handle protobuf serialization separately.
type shardInfoJSON struct {
	ShardConfig
	ShardStats          *DBStats           `json:"shard_stats,omitempty"`
	Peers               common.PeerSet     `json:"peers,omitzero"`
	RaftStatus          *common.RaftStatus `json:"raft_status,omitempty"`
	ReportedBy          common.PeerSet     `json:"reported_by,omitzero"`
	HasSnapshot         bool               `json:"has_snapshot,omitempty,omitzero"`
	Initializing        bool               `json:"initializing,omitempty,omitzero"`
	Splitting           bool               `json:"splitting,omitempty,omitzero"`
	SplitState          json.RawMessage    `json:"split_state,omitempty"`
	SplitReplayRequired bool               `json:"split_replay_required,omitempty,omitzero"`
	SplitReplayCaughtUp bool               `json:"split_replay_caught_up,omitempty,omitzero"`
	SplitCutoverReady   bool               `json:"split_cutover_ready,omitempty,omitzero"`
	SplitReplaySeq      uint64             `json:"split_replay_seq,omitempty,omitzero"`
	SplitParentShardID  types.ID           `json:"split_parent_shard_id,omitempty,omitzero"`
}

// MarshalJSON implements json.Marshaler for ShardInfo.
// It uses protojson to properly serialize the SplitState protobuf field.
func (s ShardInfo) MarshalJSON() ([]byte, error) {
	j := shardInfoJSON{
		ShardConfig:         s.ShardConfig,
		ShardStats:          s.ShardStats,
		Peers:               s.Peers,
		RaftStatus:          s.RaftStatus,
		ReportedBy:          s.ReportedBy,
		HasSnapshot:         s.HasSnapshot,
		Initializing:        s.Initializing,
		Splitting:           s.Splitting,
		SplitReplayRequired: s.SplitReplayRequired,
		SplitReplayCaughtUp: s.SplitReplayCaughtUp,
		SplitCutoverReady:   s.SplitCutoverReady,
		SplitReplaySeq:      s.SplitReplaySeq,
		SplitParentShardID:  s.SplitParentShardID,
	}
	if s.SplitState != nil {
		data, err := protojson.Marshal(s.SplitState)
		if err != nil {
			return nil, fmt.Errorf("marshaling SplitState: %w", err)
		}
		j.SplitState = data
	}
	return json.Marshal(j)
}

// UnmarshalJSON implements json.Unmarshaler for ShardInfo.
// It uses protojson to properly deserialize the SplitState protobuf field.
func (s *ShardInfo) UnmarshalJSON(data []byte) error {
	var j shardInfoJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	s.ShardConfig = j.ShardConfig
	s.ShardStats = j.ShardStats
	s.Peers = j.Peers
	s.RaftStatus = j.RaftStatus
	s.ReportedBy = j.ReportedBy
	s.HasSnapshot = j.HasSnapshot
	s.Initializing = j.Initializing
	s.Splitting = j.Splitting
	s.SplitReplayRequired = j.SplitReplayRequired
	s.SplitReplayCaughtUp = j.SplitReplayCaughtUp
	s.SplitCutoverReady = j.SplitCutoverReady
	s.SplitReplaySeq = j.SplitReplaySeq
	s.SplitParentShardID = j.SplitParentShardID
	if len(j.SplitState) > 0 {
		s.SplitState = &SplitState{}
		if err := protojson.Unmarshal(j.SplitState, s.SplitState); err != nil {
			return fmt.Errorf("unmarshaling SplitState: %w", err)
		}
	}
	return nil
}

type ShardStatus struct {
	ShardInfo `json:"info"`

	ID    types.ID   `json:"id"`
	Table string     `json:"table"`
	State ShardState `json:"state"`
}

func (ss *ShardStatus) DeepCopy() *ShardStatus {
	if ss == nil {
		return nil
	}
	return &ShardStatus{
		ShardInfo: *ss.ShardInfo.DeepCopy(),
		ID:        ss.ID,
		Table:     ss.Table,
		State:     ss.State,
	}
}

// shardStatusJSON is used for JSON marshaling/unmarshaling of ShardStatus.
// This is necessary because ShardInfo has a custom MarshalJSON method which would
// otherwise override the entire ShardStatus serialization, causing ID, Table, and
// State fields to be lost.
type shardStatusJSON struct {
	Info  ShardInfo  `json:"info"`
	ID    types.ID   `json:"id"`
	Table string     `json:"table"`
	State ShardState `json:"state"`
}

// MarshalJSON implements json.Marshaler for ShardStatus.
// This is required to properly serialize all fields since the embedded ShardInfo
// has its own MarshalJSON method that would otherwise take precedence.
func (ss ShardStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(shardStatusJSON{
		Info:  ss.ShardInfo,
		ID:    ss.ID,
		Table: ss.Table,
		State: ss.State,
	})
}

// UnmarshalJSON implements json.Unmarshaler for ShardStatus.
// This is required to properly deserialize all fields since the embedded ShardInfo
// has its own UnmarshalJSON method that would otherwise take precedence.
func (ss *ShardStatus) UnmarshalJSON(data []byte) error {
	var j shardStatusJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	ss.ShardInfo = j.Info
	ss.ID = j.ID
	ss.Table = j.Table
	ss.State = j.State
	return nil
}

func (state ShardState) Transitioning() bool {
	switch state {
	case ShardState_Default:
		return false
	case ShardState_SplitOffPreSnap:
		return false
	case ShardState_PreMerge, ShardState_PreSplit, ShardState_SplittingOff:
		return true
	case ShardState_Initializing:
		return true
	case ShardState_Splitting:
		return true
	default:
		return false
	}
}
