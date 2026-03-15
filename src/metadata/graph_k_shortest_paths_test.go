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

package metadata

import (
	"encoding/base64"
	"testing"

	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/stretchr/testify/assert"
)

// TestKShortestPaths_PathToKey tests the path hashing helper
func TestKShortestPaths_PathToKey(t *testing.T) {
	t.Run("unique key for different paths", func(t *testing.T) {
		path1 := &indexes.Path{
			Nodes: []string{
				base64.StdEncoding.EncodeToString([]byte("A")),
				base64.StdEncoding.EncodeToString([]byte("B")),
				base64.StdEncoding.EncodeToString([]byte("C")),
			},
		}
		path2 := &indexes.Path{
			Nodes: []string{
				base64.StdEncoding.EncodeToString([]byte("A")),
				base64.StdEncoding.EncodeToString([]byte("D")),
				base64.StdEncoding.EncodeToString([]byte("C")),
			},
		}

		key1 := pathToKey(path1)
		key2 := pathToKey(path2)

		assert.NotEqual(t, key1, key2, "Different paths should have different keys")
	})

	t.Run("same key for identical paths", func(t *testing.T) {
		path1 := &indexes.Path{
			Nodes: []string{
				base64.StdEncoding.EncodeToString([]byte("A")),
				base64.StdEncoding.EncodeToString([]byte("B")),
			},
		}
		path2 := &indexes.Path{
			Nodes: []string{
				base64.StdEncoding.EncodeToString([]byte("A")),
				base64.StdEncoding.EncodeToString([]byte("B")),
			},
		}

		key1 := pathToKey(path1)
		key2 := pathToKey(path2)

		assert.Equal(t, key1, key2, "Identical paths should have the same key")
	})

	t.Run("handles empty path", func(t *testing.T) {
		path := &indexes.Path{Nodes: []string{}}
		key := pathToKey(path)
		assert.Empty(t, key)
	})

	t.Run("handles single node path", func(t *testing.T) {
		path := &indexes.Path{
			Nodes: []string{base64.StdEncoding.EncodeToString([]byte("A"))},
		}
		key := pathToKey(path)
		assert.NotEmpty(t, key)
	})
}

// NOTE: K-shortest paths functionality is tested at the store level in:
// - TestGraphQueryEngine_Execute_KShortestPaths (src/store/graph_query_test.go)
//
// Multi-shard integration tests would require cluster setup infrastructure.
