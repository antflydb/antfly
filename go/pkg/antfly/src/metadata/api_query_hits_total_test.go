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

package metadata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExactQueryHitsTotal(t *testing.T) {
	total := exactQueryHitsTotal(42)
	require.Equal(t, uint64(42), total.Value)
	require.Equal(t, QueryHitsTotalRelationExact, total.Relation)
	require.Equal(t, uint64(42), queryHitsTotalValue(total))
}

func TestExactQueryHitsTotalFromInt(t *testing.T) {
	require.Equal(t, exactQueryHitsTotal(7), exactQueryHitsTotalFromInt(7))
	require.Equal(t, exactQueryHitsTotal(0), exactQueryHitsTotalFromInt(0))
	require.Equal(t, exactQueryHitsTotal(0), exactQueryHitsTotalFromInt(-1))
}

func TestGteQueryHitsTotal(t *testing.T) {
	total := gteQueryHitsTotal(100)
	require.Equal(t, uint64(100), total.Value)
	require.Equal(t, QueryHitsTotalRelationGte, total.Relation)
	require.Equal(t, uint64(100), queryHitsTotalValue(total))
}
