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

package proxy

import "testing"

func TestValidatePolicy(t *testing.T) {
	requiredVersion := new(uint64)
	*requiredVersion = 7

	tests := []struct {
		name string
		req  RequestContext
		ok   bool
	}{
		{
			name: "default published read is valid",
			req: RequestContext{
				Operation: OperationRead,
				Policy:    RequestPolicy{},
			},
			ok: true,
		},
		{
			name: "latest write is rejected",
			req: RequestContext{
				Operation: OperationWrite,
				Policy: RequestPolicy{
					View: ViewLatest,
				},
			},
		},
		{
			name: "required version with latest is rejected",
			req: RequestContext{
				Operation: OperationRead,
				Policy: RequestPolicy{
					View:            ViewLatest,
					RequiredVersion: requiredVersion,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePolicy(tt.req)
			if tt.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
