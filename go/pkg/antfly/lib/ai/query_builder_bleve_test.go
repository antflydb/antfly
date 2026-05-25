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

package ai

import (
	"testing"

	querylib "github.com/antflydb/antfly/go/pkg/antfly/lib/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildBleveQueryBuilderSystemPromptIncludesDetailedSchemaAndExampleDocuments(t *testing.T) {
	min := 1.0
	max := 5.0
	schema := querylib.SchemaDescription{
		Description: "Article search fields",
		Fields: []querylib.FieldInfo{
			{
				Name:          "status",
				Type:          "keyword",
				Searchable:    true,
				ExampleValues: []string{"draft", "published"},
			},
			{
				Name:       "rating",
				Type:       "numeric",
				Searchable: true,
				ValueRange: &querylib.ValueRange{
					Min: &min,
					Max: &max,
				},
			},
		},
	}

	prompt := buildBleveQueryBuilderSystemPrompt(schema, false, []string{"status: published"})

	require.NotEmpty(t, prompt)
	assert.Contains(t, prompt, "## Available Fields")
	assert.Contains(t, prompt, "Values: draft, published")
	assert.Contains(t, prompt, "Range: 1 to 5")
	assert.Contains(t, prompt, "## Example Documents")
	assert.Contains(t, prompt, "status: published")
	assert.Contains(t, prompt, `"term": "draft", "field": "status"`)
}
