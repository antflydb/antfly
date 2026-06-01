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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifyFailure_SplitTraceMetadataMismatch(t *testing.T) {
	err := errors.New("events:\n  [split] shard=2600\nshard 2600 range mismatch: metadata status=[,ff) table=[,302f)")
	require.Equal(t, FailureCategorySplitLiveness, ClassifyFailure(err))
}

func TestClassifyFailure_SplitTraceParentStillActive(t *testing.T) {
	err := errors.New("events:\n  [split] shard=2600\nparent split still active in phase PHASE_SPLITTING")
	require.Equal(t, FailureCategorySplitLiveness, ClassifyFailure(err))
}

func TestClassifyFailure_SplitTraceStatusRefreshTimeout(t *testing.T) {
	err := errors.New(
		"events:\n  [split] shard=4000\n" +
			"digests:\n  note=step-04-drop_next_msg active_splits=2\n" +
			"updating store statuses: saving store and shard statuses: operation did not complete within 5m0s",
	)
	require.Equal(t, FailureCategorySplitLiveness, ClassifyFailure(err))
}
