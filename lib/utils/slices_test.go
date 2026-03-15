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

package utils_test

import (
	"testing"

	"github.com/antflydb/antfly/lib/utils"
	"github.com/stretchr/testify/require"
)

func TestReplaceWithLast(t *testing.T) {
	// Test basic slice
	s := []int{1, 2, 3, 4}
	result := utils.ReplaceWithLast(s, 1)
	require.Equal(t, []int{1, 4, 3}, result)

	// Test single element slice
	s = []int{1}
	result = utils.ReplaceWithLast(s, 0)
	require.Empty(t, result)
}
