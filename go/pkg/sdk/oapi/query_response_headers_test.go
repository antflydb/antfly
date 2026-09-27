// Copyright 2026 Antfly, Inc.
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

package oapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestQueryResponseParsersExposeLegacyDeprecationHeader(t *testing.T) {
	const deprecation = "@1787702400"
	newResponse := func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type": {"application/json"},
				"Deprecation":  {deprecation},
			},
			Body: io.NopCloser(strings.NewReader(`{"responses":[]}`)),
		}
	}

	t.Run("global", func(t *testing.T) {
		response, err := ParseGlobalQueryResponse(newResponse())
		if err != nil {
			t.Fatalf("ParseGlobalQueryResponse: %v", err)
		}
		if response.Headers200 == nil || response.Headers200.Deprecation != deprecation {
			t.Fatalf("Deprecation = %#v", response.Headers200)
		}
	})

	t.Run("table", func(t *testing.T) {
		response, err := ParseQueryTableResponse(newResponse())
		if err != nil {
			t.Fatalf("ParseQueryTableResponse: %v", err)
		}
		if response.Headers200 == nil || response.Headers200.Deprecation != deprecation {
			t.Fatalf("Deprecation = %#v", response.Headers200)
		}
	})
}
