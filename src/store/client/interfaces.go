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

package client

import (
	"context"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
)

// StoreRPC abstracts shard and store RPCs so callers do not depend on the HTTP implementation.
type StoreRPC interface {
	ID() types.ID
	Batch(
		ctx context.Context,
		shardID types.ID,
		writes [][2][]byte,
		deletes [][]byte,
		transforms []*db.Transform,
		syncLevel db.Op_SyncLevel,
	) error
	ApplyMergeChunk(
		ctx context.Context,
		shardID types.ID,
		writes [][2][]byte,
		deletes [][]byte,
		syncLevel db.Op_SyncLevel,
	) error
	Backup(ctx context.Context, shardID types.ID, loc, id string, format common.BackupFormat) error
	Lookup(ctx context.Context, shardID types.ID, keys []string) (map[string][]byte, error)
	LookupWithVersion(ctx context.Context, shardID types.ID, key string) ([]byte, uint64, error)
	AddIndex(ctx context.Context, shardID types.ID, name string, config *indexes.IndexConfig) error
	MergeRange(ctx context.Context, shardID types.ID, byteRange [2][]byte) error
	SetMergeState(ctx context.Context, shardID types.ID, state *db.MergeState) error
	FinalizeMerge(ctx context.Context, shardID types.ID, byteRange [2][]byte) error
	UpdateSchema(ctx context.Context, shardID types.ID, tableSchema *schema.TableSchema) error
	PrepareSplit(ctx context.Context, shardID types.ID, splitKey []byte) error
	SplitShard(ctx context.Context, shardID types.ID, newShardID types.ID, splitKey []byte) error
	FinalizeSplit(ctx context.Context, shardID types.ID, newRangeEnd []byte) error
	RollbackSplit(ctx context.Context, shardID types.ID) error
	TransferLeadership(ctx context.Context, shardID types.ID, targetNodeID types.ID) error
	AddPeer(ctx context.Context, shardID types.ID, newPeerID types.ID, newPeerURL string) error
	RemovePeer(ctx context.Context, shardID types.ID, peerToRemoveID types.ID, timestamp []byte) error
	RemovePeerSync(ctx context.Context, shardID types.ID, peerToRemoveID types.ID, timestamp []byte) error
	StopShard(ctx context.Context, shardID types.ID) error
	StartShard(ctx context.Context, shardID types.ID, shardStartReq *store.ShardStartRequest) error
	DropIndex(ctx context.Context, shardID types.ID, name string) error
	Status(ctx context.Context) (*store.StoreStatus, error)
	MedianKeyForShard(ctx context.Context, shardID types.ID) ([]byte, error)
	IsIDRemoved(ctx context.Context, shardID types.ID, nodeID types.ID) (bool, error)
	Scan(ctx context.Context, shardID types.ID, fromKey []byte, toKey []byte, opts ScanOptions) (*db.ScanResult, error)
	ExportRangeChunk(ctx context.Context, shardID types.ID, startKey, endKey, afterKey []byte, limit int) ([][2][]byte, []byte, bool, error)
	ListMergeDeltaEntriesAfter(ctx context.Context, shardID types.ID, afterSeq uint64) ([]*db.MergeDeltaEntry, error)
	InitTransaction(
		ctx context.Context,
		shardID types.ID,
		txnID []byte,
		timestamp uint64,
		participants [][]byte,
	) error
	WriteIntent(
		ctx context.Context,
		shardID types.ID,
		txnID []byte,
		timestamp uint64,
		coordinatorShard []byte,
		writes [][2][]byte,
		deletes [][]byte,
		transforms []*db.Transform,
		predicates []*db.VersionPredicate,
	) error
	CommitTransaction(ctx context.Context, shardID types.ID, txnID []byte) (uint64, error)
	AbortTransaction(ctx context.Context, shardID types.ID, txnID []byte) error
	ResolveIntent(ctx context.Context, shardID types.ID, txnID []byte, status int32, commitVersion uint64) error
	GetTransactionStatus(ctx context.Context, shardID types.ID, txnID []byte) (int32, error)
	GetEdges(
		ctx context.Context,
		shardID types.ID,
		indexName string,
		key string,
		edgeType string,
		direction indexes.EdgeDirection,
	) ([]indexes.Edge, error)
}

var _ StoreRPC = (*StoreClient)(nil)
