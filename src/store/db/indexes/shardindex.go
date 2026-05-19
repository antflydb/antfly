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

package indexes

import (
	"context"
	"errors"
	"slices"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	bleveindex "github.com/blevesearch/bleve_index_api"
)

// ShardIndex is the interface for querying a shard, whether local (in-process)
// or remote (over HTTP). Both RemoteIndex and LocalIndex implement this.
type ShardIndex interface {
	SearchInContext(ctx context.Context, req *bleve.SearchRequest) (*bleve.SearchResult, error)
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

// BaseShardIndexPlan is the immutable portion of cross-shard query planning:
// the schema-derived Bleve mapping, schema version, and the shard iteration
// order for a given topology.
type BaseShardIndexPlan struct {
	ShardIDs      []types.ID
	IndexMapping  mapping.IndexMapping
	SchemaVersion uint32
}

func NewBaseShardIndexPlan(tableSchema *schema.TableSchema, shardIDs []types.ID) *BaseShardIndexPlan {
	plan := &BaseShardIndexPlan{
		ShardIDs:     slices.Clone(shardIDs),
		IndexMapping: schema.NewIndexMapFromSchema(tableSchema),
	}
	if tableSchema != nil {
		plan.SchemaVersion = tableSchema.Version
	}
	return plan
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

// bleveShardIndexAdapter lets ShardIndexes participate in bleve.IndexAlias
// without forcing the shard abstraction itself to expose the full writable
// bleve.Index surface.
type bleveShardIndexAdapter struct {
	ShardIndex
}

func newBleveShardIndexAdapter(idx ShardIndex) bleve.Index {
	return bleveShardIndexAdapter{ShardIndex: idx}
}

func (a bleveShardIndexAdapter) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	return a.SearchInContext(context.Background(), req)
}

func (a bleveShardIndexAdapter) Index(string, any) error {
	return errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) Delete(string) error {
	return errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) NewBatch() *bleve.Batch {
	panic("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) Batch(*bleve.Batch) error {
	return errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) Document(string) (bleveindex.Document, error) {
	return nil, errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) DocCount() (uint64, error) {
	return 0, errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) Fields() ([]string, error) {
	return nil, errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) FieldDict(string) (bleveindex.FieldDict, error) {
	return nil, errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) FieldDictRange(string, []byte, []byte) (bleveindex.FieldDict, error) {
	return nil, errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) FieldDictPrefix(string, []byte) (bleveindex.FieldDict, error) {
	return nil, errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) Close() error { return nil }

func (a bleveShardIndexAdapter) Mapping() mapping.IndexMapping { return a.IndexMapping() }

func (a bleveShardIndexAdapter) Stats() *bleve.IndexStat  { return nil }
func (a bleveShardIndexAdapter) StatsMap() map[string]any { return nil }

func (a bleveShardIndexAdapter) GetInternal([]byte) ([]byte, error) {
	return nil, errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) SetInternal([]byte, []byte) error {
	return errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) DeleteInternal([]byte) error {
	return errors.New("operation not supported on shard search adapter")
}

func (a bleveShardIndexAdapter) SetName(string) {}

func (a bleveShardIndexAdapter) Advanced() (bleveindex.Index, error) {
	return nil, errors.New("operation not supported on shard search adapter")
}
