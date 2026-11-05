// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sources

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncGateAcceptsOnlyAnExplicitFreshnessProof(t *testing.T) {
	hits := `"hits":{"hits":[{"_id":"a"}]}`
	for name, tc := range map[string]struct {
		body string
		ok   bool
	}{
		"retained snapshot": {`{"responses":[{"status":200,"remote_snapshot":"token",` + hits + `}]}`, true},
		"live read":         {`{"responses":[{"status":200,"live_read":true,` + hits + `}]}`, true},
		"neither":           {`{"responses":[{"status":200,` + hits + `}]}`, false},
		"live read denied":  {`{"responses":[{"status":200,"live_read":false,` + hits + `}]}`, false},
		"failed live read":  {`{"responses":[{"status":500,"live_read":true,` + hits + `}]}`, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := decodeSyncHits([]byte(tc.body))
			if !tc.ok {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1)
		})
	}
}
