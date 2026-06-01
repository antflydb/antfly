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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestAlgebraicIndexStub(t *testing.T) {
	cfg := NewAlgebraicIndexConfig("alg", true)
	idx, err := MakeIndex(zaptest.NewLogger(t), nil, nil, t.TempDir(), "alg", cfg, nil)
	require.NoError(t, err)
	require.Equal(t, IndexTypeAlgebraic, idx.Type())

	assert.NoError(t, idx.Batch(context.Background(), nil, nil, false))
	_, err = idx.Search(context.Background(), nil)
	assert.ErrorContains(t, err, algebraicGoStubMessage)

	stats, err := idx.Stats().AsAlgebraicIndexStats()
	require.NoError(t, err)
	assert.Equal(t, AlgebraicIndexStatsIndexTypeAlgebraic, stats.IndexType)
	assert.False(t, stats.Healthy)
	assert.Equal(t, algebraicGoStubMessage, stats.Error)
}
