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

package template

import (
	"fmt"
	"testing"

	"github.com/antflydb/antfly/lib/scraping"
	"github.com/stretchr/testify/assert"
)

func TestErrorToDirective(t *testing.T) {
	t.Run("HTTPError includes status code", func(t *testing.T) {
		err := &scraping.HTTPError{StatusCode: 404, Status: "404 Not Found"}
		result := string(errorToDirective(err))
		assert.Equal(t, "<<<error:status=404 message=404 Not Found>>>", result)

		directives := ParseErrorDirectives(result)
		assert.Len(t, directives, 1)
		assert.Equal(t, 404, directives[0].Status)
		assert.Equal(t, "404 Not Found", directives[0].Message)
	})

	t.Run("wrapped HTTPError still extracts status", func(t *testing.T) {
		httpErr := &scraping.HTTPError{StatusCode: 503, Status: "503 Service Unavailable"}
		wrapped := fmt.Errorf("download failed: %w", httpErr)
		result := string(errorToDirective(wrapped))

		directives := ParseErrorDirectives(result)
		assert.Len(t, directives, 1)
		assert.Equal(t, 503, directives[0].Status)
		assert.Equal(t, "503 Service Unavailable", directives[0].Message)
	})

	t.Run("non-HTTP error has zero status", func(t *testing.T) {
		err := fmt.Errorf("connection refused")
		result := string(errorToDirective(err))
		assert.Equal(t, "<<<error:message=connection refused>>>", result)

		directives := ParseErrorDirectives(result)
		assert.Len(t, directives, 1)
		assert.Equal(t, 0, directives[0].Status)
		assert.Equal(t, "connection refused", directives[0].Message)
	})
}
