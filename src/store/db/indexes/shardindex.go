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

package indexes

import (
	"context"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

// ShardIndex is the interface for querying a shard, whether local (in-process)
// or remote (over HTTP). Both RemoteIndex and LocalIndex implement this.
type ShardIndex interface {
	bleve.Index // SearchInContext for FullTextSearch via bleve IndexAlias
	RemoteSearch(ctx context.Context, req *RemoteIndexSearchRequest) (*RemoteIndexSearchResult, error)
	BatchRemoteSearch(ctx context.Context, reqs []*RemoteIndexSearchRequest) ([]*RemoteIndexSearchResult, []error)
	Name() string
	ShardID() types.ID
	IndexMapping() mapping.IndexMapping
	SchemaVersion() uint32
	// WithFieldFilter returns a shallow clone with the given field projection applied.
	WithFieldFilter(ff *FieldFilter) ShardIndex
}

// ShardIndexes is a collection of ShardIndex, one per shard.
type ShardIndexes []ShardIndex

// ShardIndexFactory builds base ShardIndexes (without FieldFilter) for the
// given schema and shards. Set once at startup based on deployment mode.
type ShardIndexFactory func(tableSchema *schema.TableSchema, shardIDs []types.ID, peers map[types.ID][]string) (ShardIndexes, error)

// ShardSearcher provides direct (in-process) shard search, bypassing HTTP.
// Implemented by a thin adapter over store.StoreIface in swarm mode.
type ShardSearcher interface {
	SearchShardTyped(ctx context.Context, shardID types.ID, req *RemoteIndexSearchRequest) (*RemoteIndexSearchResult, error)
}

// WithFieldFilter returns a copy of each ShardIndex with the given FieldFilter
// applied. The underlying client, mapping, and schema are shared (not copied).
func (s ShardIndexes) WithFieldFilter(ff *FieldFilter) ShardIndexes {
	out := make(ShardIndexes, len(s))
	for i, si := range s {
		out[i] = si.WithFieldFilter(ff)
	}
	return out
}
