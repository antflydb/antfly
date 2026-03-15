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

package store

import (
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
)

type IndexStatus struct {
	indexes.IndexConfig

	Status      indexes.IndexStats              `json:"status,omitzero"`
	ShardStatus map[types.ID]indexes.IndexStats `json:"shard_status,omitempty"`
}

// Type aliases for backward compatibility - types have moved to db package
type (
	ShardConfig              = db.ShardConfig
	ShardInfo                = db.ShardInfo
	ShardStats               = db.DBStats
	StorageStats             = db.DBStorageStats
	ShardStatus              = db.ShardStatus
	ShardIface               = db.ShardIface
	Shard                    = db.Shard
	ShardStartRequest        = db.ShardStartRequest
	ShardSplitRequest        = db.ShardSplitRequest
	ShardPrepareSplitRequest = db.ShardPrepareSplitRequest
	ShardAddPeerRequest      = db.ShardAddPeerRequest
	ShardRemovePeerRequest   = db.ShardRemovePeerRequest
	ShardSetRangeRequest     = db.ShardSetRangeRequest
	ShardUpdateSchemaRequest = db.ShardUpdateSchemaRequest
	ShardDropIndexRequest    = db.ShardDropIndexRequest
	ShardState               = db.ShardState
)

// ShardState constants for backward compatibility
const (
	ShardState_Default         = db.ShardState_Default
	ShardState_SplittingOff    = db.ShardState_SplittingOff
	ShardState_SplitOffPreSnap = db.ShardState_SplitOffPreSnap
	ShardState_PreSplit        = db.ShardState_PreSplit
	ShardState_PreMerge        = db.ShardState_PreMerge
	ShardState_Initializing    = db.ShardState_Initializing
	ShardState_Splitting       = db.ShardState_Splitting
)

// NewShardInfo creates a new ShardInfo with default values.
var NewShardInfo = db.NewShardInfo

// ErrShardNotReady is returned when a shard operation is attempted before the shard is fully initialized.
var ErrShardNotReady = db.ErrShardNotReady

type StoreInfo struct {
	ID      types.ID `json:"id"`
	RaftURL string   `json:"raft_url"`
	ApiURL  string   `json:"api_url"`
}

// StoreRegistrationRequest represents the data sent when a node registers with the leader
type StoreRegistrationRequest struct {
	StoreInfo `json:"store_info"`

	Shards map[types.ID]*ShardInfo `json:"shards,omitempty"`
}

type StoreDeregistrationRequest struct {
	ID types.ID `json:"id"`
}
