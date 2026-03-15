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

// //go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
//
//go:generate protoc --go_out=. --go_opt=paths=source_relative store.proto
package store

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/kaptinlin/jsonschema"

	json "github.com/antflydb/antfly/pkg/libaf/json"
)

type Table struct {
	Name               string                         `json:"name"`
	Description        string                         `json:"description,omitempty"`
	ReadSchema         *schema.TableSchema            `json:"read_schema,omitempty"` // Previous Schema for the table while a Schema change is in progress
	Schema             *schema.TableSchema            `json:"schema,omitempty"`
	Indexes            map[string]indexes.IndexConfig `json:"indexes,omitempty"`
	Shards             map[types.ID]*ShardConfig      `json:"shards,omitempty"`
	ReplicationSources []ReplicationSourceConfig      `json:"replication_sources,omitempty"`

	// Private fields for compiled schemas (not serialized)
	compiledSchemas     map[string]*jsonschema.Schema
	compiledSchemasOnce sync.Once
	compiledSchemasErr  error
}

type TableStatus struct {
	Table         `json:"table"`
	*StorageStats `json:"storage_stats,omitempty"`
	*ShardStats   `json:"shard_stats,omitempty"`
}

// CompiledSchemas returns the compiled JSON schemas for all document types in the table.
// The schemas are compiled lazily on first access and cached for subsequent calls.
// Returns nil if the table has no schema or no document schemas defined.
func (t *Table) CompiledSchemas() (map[string]*jsonschema.Schema, error) {
	if t.Schema == nil || len(t.Schema.DocumentSchemas) == 0 {
		return nil, nil
	}

	t.compiledSchemasOnce.Do(func() {
		compiler := jsonschema.NewCompiler()
		compiler.WithDecoderJSON(json.Unmarshal)
		compiler.WithEncoderJSON(json.Marshal)

		t.compiledSchemas = make(map[string]*jsonschema.Schema, len(t.Schema.DocumentSchemas))
		for typeName, docSchema := range t.Schema.DocumentSchemas {
			if len(docSchema.Schema) == 0 {
				continue
			}
			schemaBytes, err := json.Marshal(docSchema.Schema)
			if err != nil {
				t.compiledSchemasErr = fmt.Errorf(
					"marshalling schema for type %s: %w",
					typeName,
					err,
				)
				return
			}
			schema, err := compiler.Compile(schemaBytes)
			if err != nil {
				t.compiledSchemasErr = fmt.Errorf("compiling schema for type %s: %w", typeName, err)
				return
			}
			t.compiledSchemas[typeName] = schema
		}
	})

	return t.compiledSchemas, t.compiledSchemasErr
}

// ValidateDoc validates a document against the table schema.
// It performs type checking, TTL field validation, and JSON schema validation.
// Returns the document type or an error if validation fails.
func (t *Table) ValidateDoc(doc map[string]any) (docType string, err error) {
	if t.Schema == nil {
		return "", nil
	}

	// Use the TableSchema's ValidateDoc for basic validation
	docType, err = t.Schema.ValidateDoc(doc)
	if err != nil || docType == "" {
		return "", err
	}

	// If type enforcement is enabled, validate against compiled JSON schema
	if t.Schema.EnforceTypes {
		compiledSchemas, err := t.CompiledSchemas()
		if err != nil {
			return "", fmt.Errorf("getting compiled schemas: %w", err)
		}

		if compiledSchemas != nil {
			schema, ok := compiledSchemas[docType]
			if !ok || schema == nil {
				return "", fmt.Errorf("compiled schema not found for type %q", docType)
			}
			if result := schema.ValidateMap(doc); !result.IsValid() {
				var errs []string
				for field, e := range result.Errors {
					errs = append(errs, fmt.Sprintf("- %s: %s", field, e.Message))
				}
				return "", fmt.Errorf(
					"document failed validation for type %s:\n%s",
					docType,
					joinErrors(errs),
				)
			}
		}
	}

	return docType, nil
}

// joinErrors joins error messages with newlines
func joinErrors(errs []string) string {
	result := ""
	var resultSb114 strings.Builder
	for i, err := range errs {
		if i > 0 {
			resultSb114.WriteString("\n")
		}
		resultSb114.WriteString(err)
	}
	result += resultSb114.String()
	return result
}

func (table *Table) FindShardForKey(key string) (types.ID, error) {
	// Convert to storage key format by appending DBRangeStart suffix.
	// Shard ranges are defined in storage key space (with :\x00 suffix),
	// so we must convert the lookup key to match.
	keyBytes := storeutils.KeyRangeStart([]byte(key))
	for shardID, status := range table.Shards {
		// Check if key is in this shard's range using common.KeyInByteRange
		// which properly handles unbounded ranges (empty end = +infinity)
		if status.ByteRange.Contains(keyBytes) {
			return shardID, nil
		}
	}
	return 0, fmt.Errorf("no shard found for key: %s", key)
}

// PartitionKeysByShard takes a list of keys and groups them by the shard ID they belong to.
// It returns a map where keys are shard IDs and values are slices of the input keys belonging to that shard.
// It also returns a slice containing keys that could not be assigned to any shard.
func (table *Table) PartitionKeysByShard(keys []string) (map[types.ID][]string, []string) {
	partitions := make(map[types.ID][]string)
	var unfoundKeys []string

	if len(table.Shards) == 1 {
		// If there's only one shard, all keys go to that shard
		for k := range table.Shards {
			partitions[k] = keys
		}
		return partitions, nil
	}

	for _, key := range keys {
		// Convert to storage key format by appending DBRangeStart suffix.
		// Shard ranges are defined in storage key space (with :\x00 suffix),
		// so we must convert the lookup key to match.
		keyBytes := storeutils.KeyRangeStart([]byte(key))
		found := false
		// FIXME (ajr) This operation is not thread-safe.
		for shardID, conf := range table.Shards {
			byteRange := conf.ByteRange
			// Check if key is in this shard's range: [start, end)
			start := byteRange[0]
			end := byteRange[1]

			// Key >= start
			lowerBoundOK := len(start) == 0 || bytes.Compare(keyBytes, start) >= 0
			// Key < end
			upperBoundOK := len(end) == 0 || bytes.Compare(keyBytes, end) < 0

			if lowerBoundOK && upperBoundOK {
				partitions[shardID] = append(partitions[shardID], key)
				found = true
				break // Key found in a shard, move to the next key
			}
		}
		if !found {
			unfoundKeys = append(unfoundKeys, key)
		}
	}

	return partitions, unfoundKeys
}

// ReplicationSourceConfig defines a PostgreSQL CDC replication source for a table.
type ReplicationSourceConfig struct {
	Type              string                   `json:"type"`
	DSN               string                   `json:"dsn"`
	PostgresTable     string                   `json:"postgres_table"`
	KeyTemplate       string                   `json:"key_template"`
	SlotName          string                   `json:"slot_name,omitempty"`
	PublicationName   string                   `json:"publication_name,omitempty"`
	OnUpdate          []ReplicationTransformOp `json:"on_update,omitempty"`
	OnDelete          []ReplicationTransformOp `json:"on_delete,omitempty"`
	PublicationFilter json.RawMessage          `json:"publication_filter,omitempty"`
	Routes            []ReplicationRouteConfig `json:"routes,omitempty"`
}

// ReplicationTransformOp defines a single transform operation with {{column}} references.
type ReplicationTransformOp struct {
	Op    string `json:"op"`
	Path  string `json:"path,omitempty"`
	Value any    `json:"value,omitempty"`
}

// ReplicationRouteConfig defines a conditional route that fans out CDC rows to a target table.
type ReplicationRouteConfig struct {
	TargetTable string                   `json:"target_table"`
	Where       json.RawMessage          `json:"where,omitempty"`
	KeyTemplate string                   `json:"key_template,omitempty"`
	OnUpdate    []ReplicationTransformOp `json:"on_update,omitempty"`
	OnDelete    []ReplicationTransformOp `json:"on_delete,omitempty"`
}
