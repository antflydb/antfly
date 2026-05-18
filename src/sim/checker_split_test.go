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

package sim

import (
	"testing"

	"github.com/antflydb/antfly/src/store"
	storedb "github.com/antflydb/antfly/src/store/db"
	"github.com/stretchr/testify/require"
)

func TestCheckerIsSplitActive_IgnoresPhaseNone(t *testing.T) {
	checker := NewChecker(CheckerConfig{})

	status := &store.ShardStatus{
		State: store.ShardState_Default,
		ShardInfo: store.ShardInfo{
			SplitState: &storedb.SplitState{},
		},
	}

	require.False(t, checker.isSplitActive(status))

	status.SplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	require.True(t, checker.isSplitActive(status))
}
