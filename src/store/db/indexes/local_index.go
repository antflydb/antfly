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
	"errors"
	"fmt"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	bleveindex "github.com/blevesearch/bleve_index_api"
)

var _ ShardIndex = (*LocalIndex)(nil)

// LocalIndex implements ShardIndex by calling a shard directly in-process,
// bypassing HTTP. Used in swarm mode where metadata and store share a process.
type LocalIndex struct {
	shard      types.ID
	searcher   ShardSearcher
	idxMapping mapping.IndexMapping
	schema     *schema.TableSchema
	q          *FieldFilter
}

func (l *LocalIndex) Name() string                        { return l.shard.String() }
func (l *LocalIndex) ShardID() types.ID                   { return l.shard }
func (l *LocalIndex) IndexMapping() mapping.IndexMapping   { return l.idxMapping }
func (l *LocalIndex) SchemaVersion() uint32 {
	if l.schema != nil {
		return l.schema.Version
	}
	return 0
}

func (l *LocalIndex) RemoteSearch(
	ctx context.Context,
	req *RemoteIndexSearchRequest,
) (*RemoteIndexSearchResult, error) {
	version := uint32(0)
	if l.schema != nil {
		version = l.schema.Version
	}
	req.FullTextIndexVersion = version

	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal search request: %w", err)
	}

	respBytes, err := l.searcher.SearchShard(ctx, l.shard, reqBytes)
	if err != nil {
		return nil, fmt.Errorf("local shard search: %w", err)
	}

	var result RemoteIndexSearchResult
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("unmarshal search result: %w", err)
	}
	return &result, nil
}

func (l *LocalIndex) BatchRemoteSearch(
	ctx context.Context,
	reqs []*RemoteIndexSearchRequest,
) ([]*RemoteIndexSearchResult, []error) {
	results := make([]*RemoteIndexSearchResult, len(reqs))
	errs := make([]error, len(reqs))
	for i, req := range reqs {
		results[i], errs[i] = l.RemoteSearch(ctx, req)
	}
	return results, errs
}

func (l *LocalIndex) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	return l.SearchInContext(context.Background(), req)
}

func (l *LocalIndex) SearchInContext(
	ctx context.Context,
	req *bleve.SearchRequest,
) (*bleve.SearchResult, error) {
	version := uint32(0)
	if l.schema != nil {
		version = l.schema.Version
	}
	riReq := RemoteIndexSearchRequest{BleveSearchRequest: req, FullTextIndexVersion: version}
	if l.q != nil {
		if l.q.CountStar {
			riReq.CountStar = true
		} else if l.q.Star {
			riReq.Star = true
		} else {
			riReq.Columns = l.q.Fields
		}
	}

	reqBytes, err := json.Marshal(riReq)
	if err != nil {
		return nil, fmt.Errorf("marshal search request: %w", err)
	}

	respBytes, err := l.searcher.SearchShard(ctx, l.shard, reqBytes)
	if err != nil {
		return nil, fmt.Errorf("local shard search: %w", err)
	}

	var result RemoteIndexSearchResult
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("unmarshal search result: %w", err)
	}
	return result.BleveSearchResult, nil
}

// Unsupported write operations — LocalIndex is read-only for query fan-out.
func (l *LocalIndex) IndexSynonym(string, string, *bleve.SynonymDefinition) error {
	return errors.New("operation not supported on local index")
}
func (l *LocalIndex) Index(string, any) error                              { return errors.New("operation not supported on local index") }
func (l *LocalIndex) Delete(string) error                                  { return errors.New("operation not supported on local index") }
func (l *LocalIndex) NewBatch() *bleve.Batch                               { panic("operation not supported on local index") }
func (l *LocalIndex) Batch(*bleve.Batch) error                             { return errors.New("operation not supported on local index") }
func (l *LocalIndex) Document(string) (bleveindex.Document, error)         { return nil, errors.New("operation not supported on local index") }
func (l *LocalIndex) DocCount() (uint64, error)                            { return 0, errors.New("operation not supported on local index") }
func (l *LocalIndex) Fields() ([]string, error)                            { return nil, errors.New("operation not supported on local index") }
func (l *LocalIndex) FieldDict(string) (bleveindex.FieldDict, error)       { return nil, errors.New("operation not supported on local index") }
func (l *LocalIndex) FieldDictRange(string, []byte, []byte) (bleveindex.FieldDict, error) {
	return nil, errors.New("operation not supported on local index")
}
func (l *LocalIndex) FieldDictPrefix(string, []byte) (bleveindex.FieldDict, error) {
	return nil, errors.New("operation not supported on local index")
}
func (l *LocalIndex) Close() error                  { return nil }
func (l *LocalIndex) Mapping() mapping.IndexMapping { return nil }
func (l *LocalIndex) Stats() *bleve.IndexStat       { return nil }
func (l *LocalIndex) StatsMap() map[string]any      { return nil }
func (l *LocalIndex) GetInternal([]byte) ([]byte, error) {
	return nil, errors.New("operation not supported on local index")
}
func (l *LocalIndex) SetInternal([]byte, []byte) error  { return errors.New("operation not supported on local index") }
func (l *LocalIndex) DeleteInternal([]byte) error        { return errors.New("operation not supported on local index") }
func (l *LocalIndex) SetName(string)                     {}
func (l *LocalIndex) Advanced() (bleveindex.Index, error) {
	return nil, errors.New("operation not supported on local index")
}
