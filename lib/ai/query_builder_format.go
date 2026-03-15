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

package ai

import (
	"context"

	"github.com/antflydb/antfly/lib/query"
)

// BuildQuery generates a query in native Bleve format from natural language.
// This is a convenience alias for BuildQueryBleve.
func (g *GenKitModelImpl) BuildQuery(
	ctx context.Context,
	intent string,
	schemaDescription query.SchemaDescription,
	opts ...QueryBuilderOption,
) (*QueryBuilderResult, error) {
	return g.BuildQueryBleve(ctx, intent, schemaDescription, opts...)
}

// BuildQueryFromFieldNames is a convenience method that builds a Bleve query using just field names.
func (g *GenKitModelImpl) BuildQueryFromFieldNames(
	ctx context.Context,
	intent string,
	fieldNames []string,
	opts ...QueryBuilderOption,
) (*QueryBuilderResult, error) {
	schema := query.SchemaDescription{
		Fields: make([]query.FieldInfo, 0, len(fieldNames)),
	}

	for _, name := range fieldNames {
		schema.Fields = append(schema.Fields, query.FieldInfo{
			Name:       name,
			Type:       "text", // Default assumption
			Searchable: true,
		})
	}

	return g.BuildQueryBleve(ctx, intent, schema, opts...)
}
