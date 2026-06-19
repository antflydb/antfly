// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

package metadata

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMCPFullTextSearchJSONWithField(t *testing.T) {
	raw, err := mcpFullTextSearchJSON("individual freedom personality development", "content")
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, map[string]any{
		"match": "individual freedom personality development",
		"field": "content",
	}, got)
}

func TestMCPFullTextSearchJSONWithoutFieldUsesQueryString(t *testing.T) {
	raw, err := mcpFullTextSearchJSON("content:montessori", "")
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, "content:montessori", got["query"])
}

func TestMCPFullTextSearchJSONPassesThroughObject(t *testing.T) {
	raw, err := mcpFullTextSearchJSON(map[string]any{
		"match": "individual freedom personality development",
		"field": "content",
	}, "")
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, map[string]any{
		"match": "individual freedom personality development",
		"field": "content",
	}, got)
}
