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

package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyPattern_Compile(t *testing.T) {
	tests := []struct {
		name        string
		pattern     string
		expectError bool
	}{
		{
			name:        "simple prefix",
			pattern:     "tables/",
			expectError: false,
		},
		{
			name:        "named parameters",
			pattern:     "tables/{tableID}/shards/{shardID}",
			expectError: false,
		},
		{
			name:        "wildcards",
			pattern:     "tables/*/shards/*",
			expectError: false,
		},
		{
			name:        "mixed literal and params",
			pattern:     "tables/{tableID}/config",
			expectError: false,
		},
		{
			name:        "empty parameter name",
			pattern:     "tables/{}/config",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				return nil
			}
			kp, err := NewKeyPattern(tt.pattern, handler)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, kp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, kp)
			}
		})
	}
}

func TestKeyPattern_Match_NamedParameters(t *testing.T) {
	handler := func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
		return nil
	}
	kp, err := NewKeyPattern("tables/{tableID}/shards/{shardID}", handler)
	require.NoError(t, err)

	tests := []struct {
		name           string
		key            string
		expectMatch    bool
		expectedParams map[string]string
	}{
		{
			name:        "exact match",
			key:         "tables/users/shards/shard1",
			expectMatch: true,
			expectedParams: map[string]string{
				"tableID": "users",
				"shardID": "shard1",
			},
		},
		{
			name:        "different values",
			key:         "tables/products/shards/shard2",
			expectMatch: true,
			expectedParams: map[string]string{
				"tableID": "products",
				"shardID": "shard2",
			},
		},
		{
			name:           "wrong prefix",
			key:            "databases/users/shards/shard1",
			expectMatch:    false,
			expectedParams: nil,
		},
		{
			name:           "too few segments",
			key:            "tables/users",
			expectMatch:    false,
			expectedParams: nil,
		},
		{
			name:           "too many segments",
			key:            "tables/users/shards/shard1/extra",
			expectMatch:    false,
			expectedParams: nil,
		},
		{
			name:           "wrong middle segment",
			key:            "tables/users/indexes/shard1",
			expectMatch:    false,
			expectedParams: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, params, err := kp.Match([]byte(tt.key))
			require.NoError(t, err)
			assert.Equal(t, tt.expectMatch, matched)
			if tt.expectMatch {
				assert.Equal(t, tt.expectedParams, params)
			}
		})
	}
}

func TestKeyPattern_Match_Wildcards(t *testing.T) {
	handler := func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
		return nil
	}
	kp, err := NewKeyPattern("tables/*/shards/*", handler)
	require.NoError(t, err)

	tests := []struct {
		name        string
		key         string
		expectMatch bool
	}{
		{
			name:        "matches with wildcards",
			key:         "tables/users/shards/shard1",
			expectMatch: true,
		},
		{
			name:        "different values still match",
			key:         "tables/products/shards/shard2",
			expectMatch: true,
		},
		{
			name:        "wrong prefix",
			key:         "databases/users/shards/shard1",
			expectMatch: false,
		},
		{
			name:        "too few segments",
			key:         "tables/users",
			expectMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, params, err := kp.Match([]byte(tt.key))
			require.NoError(t, err)
			assert.Equal(t, tt.expectMatch, matched)
			// Wildcards don't extract parameters
			assert.Empty(t, params)
		})
	}
}

func TestKeyPattern_Match_SimplePrefix(t *testing.T) {
	handler := func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
		return nil
	}
	kp, err := NewKeyPattern("tables/", handler)
	require.NoError(t, err)

	tests := []struct {
		name        string
		key         string
		expectMatch bool
	}{
		{
			name:        "matches prefix",
			key:         "tables/users",
			expectMatch: true,
		},
		{
			name:        "matches longer path",
			key:         "tables/users/shards/shard1",
			expectMatch: true,
		},
		{
			name:        "exact match",
			key:         "tables/",
			expectMatch: true,
		},
		{
			name:        "no match",
			key:         "databases/users",
			expectMatch: false,
		},
		{
			name:        "partial word no match",
			key:         "tablesystem",
			expectMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, params, err := kp.Match([]byte(tt.key))
			require.NoError(t, err)
			assert.Equal(t, tt.expectMatch, matched)
			assert.Empty(t, params)
		})
	}
}

func TestKeyPattern_Match_MixedLiteralAndParams(t *testing.T) {
	handler := func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
		return nil
	}
	kp, err := NewKeyPattern("tables/{tableID}/config", handler)
	require.NoError(t, err)

	tests := []struct {
		name           string
		key            string
		expectMatch    bool
		expectedParams map[string]string
	}{
		{
			name:        "exact match",
			key:         "tables/users/config",
			expectMatch: true,
			expectedParams: map[string]string{
				"tableID": "users",
			},
		},
		{
			name:           "wrong suffix",
			key:            "tables/users/schema",
			expectMatch:    false,
			expectedParams: nil,
		},
		{
			name:           "too few segments",
			key:            "tables/users",
			expectMatch:    false,
			expectedParams: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, params, err := kp.Match([]byte(tt.key))
			require.NoError(t, err)
			assert.Equal(t, tt.expectMatch, matched)
			if tt.expectMatch {
				assert.Equal(t, tt.expectedParams, params)
			}
		})
	}
}

func TestKeyPattern_Match_ColonSeparator(t *testing.T) {
	handler := func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
		return nil
	}
	kp, err := NewKeyPattern("tm:t:{tableName}", handler)
	require.NoError(t, err)

	tests := []struct {
		name           string
		key            string
		expectMatch    bool
		expectedParams map[string]string
	}{
		{
			name:        "exact match with colon separator",
			key:         "tm:t:users",
			expectMatch: true,
			expectedParams: map[string]string{
				"tableName": "users",
			},
		},
		{
			name:           "does NOT over-match with extra segments",
			key:            "tm:t:users:i:myindex",
			expectMatch:    false,
			expectedParams: nil,
		},
		{
			name:        "different table name",
			key:         "tm:t:products",
			expectMatch: true,
			expectedParams: map[string]string{
				"tableName": "products",
			},
		},
		{
			name:           "wrong prefix",
			key:            "tm:s:users",
			expectMatch:    false,
			expectedParams: nil,
		},
		{
			name:           "too few segments",
			key:            "tm:t",
			expectMatch:    false,
			expectedParams: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, params, err := kp.Match([]byte(tt.key))
			require.NoError(t, err)
			assert.Equal(t, tt.expectMatch, matched)
			if tt.expectMatch {
				assert.Equal(t, tt.expectedParams, params)
			}
		})
	}
}

func TestKeyPrefixListener_Match(t *testing.T) {
	listener := KeyPrefixListener{
		Prefix: []byte("tables/"),
		Handler: func(ctx context.Context, key, value []byte, isDelete bool) error {
			return nil
		},
	}

	tests := []struct {
		name        string
		key         string
		expectMatch bool
	}{
		{
			name:        "matches prefix",
			key:         "tables/users",
			expectMatch: true,
		},
		{
			name:        "exact prefix",
			key:         "tables/",
			expectMatch: true,
		},
		{
			name:        "no match",
			key:         "databases/users",
			expectMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched := listener.Match([]byte(tt.key))
			assert.Equal(t, tt.expectMatch, matched)
		})
	}
}
