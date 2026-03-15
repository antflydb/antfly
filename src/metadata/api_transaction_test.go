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
	"fmt"
	"testing"

	"github.com/antflydb/antfly/src/store/client"
	"github.com/stretchr/testify/assert"
)

func TestIsVersionConflict(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// True positives: ResponseError from store with version/intent conflict bodies
		{"predicate check failed", &client.ResponseError{StatusCode: 409, Body: "version predicate check failed: conflict on key abc"}, true},
		{"version conflict on key", &client.ResponseError{StatusCode: 409, Body: "version conflict on key abc: expected 5, got 6"}, true},
		{"intent conflict on key", &client.ResponseError{StatusCode: 409, Body: "intent conflict on key abc: txn 123 has pending intent"}, true},
		{"wrapped ResponseError", fmt.Errorf("executing transaction: %w", &client.ResponseError{StatusCode: 409, Body: "version conflict on key abc"}), true},

		// False positives: version conflict without "on key" should NOT match
		{"schema version conflict", &client.ResponseError{StatusCode: 409, Body: "schema version conflict between nodes"}, false},
		{"generic version conflict", &client.ResponseError{StatusCode: 409, Body: "version conflict"}, false},

		// Unrelated errors
		{"unrelated error", fmt.Errorf("connection refused"), false},
		{"no leader error", fmt.Errorf("no leader elected for shard"), false},
		{"generic 500", &client.ResponseError{StatusCode: 500, Body: "internal server error"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isVersionConflict(tt.err))
		})
	}
}
