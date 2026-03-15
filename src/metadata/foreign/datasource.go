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
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/antflydb/antfly/src/store/db/indexes"
)

// DataSource is a backend-agnostic interface for querying external data sources.
type DataSource interface {
	Query(ctx context.Context, params *QueryParams) (*QueryResult, error)
	Statistics(ctx context.Context, table string) (rowCount int64, sizeBytes int64, err error)
	Close()
}

// Aggregator is an optional interface that DataSource implementations may
// support to execute aggregation queries (COUNT, SUM, AVG, terms, etc.).
type Aggregator interface {
	Aggregate(ctx context.Context, params *AggregateParams) (*AggregateResult, error)
}

// Convenience aliases so callers in the foreign package don't need to import indexes directly.
const (
	SortAsc  = indexes.SortDirection(false)
	SortDesc = indexes.SortDirection(true)
)

// QueryParams describes a query against a foreign data source.
type QueryParams struct {
	Table       string          // foreign table/view name
	Fields      []string        // columns to return; empty = all
	FilterQuery json.RawMessage // Bleve-style DSL, translated to native query by the DataSource
	Limit       int
	Offset      int
	Columns     []ForeignColumn     // known columns for filter validation
	OrderBy     []indexes.SortField // sort fields with direction
}

// AggregateParams describes an aggregation request against a foreign data source.
type AggregateParams struct {
	Table        string          // foreign table/view name
	FilterQuery  json.RawMessage // optional filter
	Columns      []ForeignColumn // known columns
	Aggregations map[string]AggregationDef
}

// AggregationDef defines a single aggregation to compute.
type AggregationDef struct {
	Type  string // count, sum, avg, min, max, stats, terms
	Field string
	Size  int // for terms: max number of buckets
}

// AggregateResult holds aggregation results keyed by aggregation name.
type AggregateResult struct {
	Results map[string]any
}

// QueryResult holds rows returned from a foreign data source.
type QueryResult struct {
	Rows  []map[string]any
	Total int
}

// ForeignColumn describes a column in a foreign table.
type ForeignColumn struct {
	Name     string
	Type     string
	Nullable bool
}

// DataSourceFactory creates a DataSource for a given DSN and table.
type DataSourceFactory func(dsn string) (DataSource, error)

var (
	factoryMu sync.RWMutex
	factories = map[string]DataSourceFactory{}
)

// RegisterFactory registers a DataSourceFactory for a source type (e.g. "postgres").
func RegisterFactory(sourceType string, f DataSourceFactory) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	factories[sourceType] = f
}

// NewDataSource creates a DataSource for the given source type and DSN.
func NewDataSource(sourceType, dsn string) (DataSource, error) {
	factoryMu.RLock()
	f, ok := factories[sourceType]
	factoryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unsupported foreign source type: %q", sourceType)
	}
	return f(dsn)
}
