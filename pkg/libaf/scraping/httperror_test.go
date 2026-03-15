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

package scraping

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPError(t *testing.T) {
	t.Run("Error() includes status code and text", func(t *testing.T) {
		err := &HTTPError{StatusCode: 404, Status: "404 Not Found"}
		assert.Contains(t, err.Error(), "404")
		assert.Contains(t, err.Error(), "Not Found")
	})

	t.Run("errors.As unwraps HTTPError", func(t *testing.T) {
		original := &HTTPError{StatusCode: 503, Status: "503 Service Unavailable"}
		wrapped := fmt.Errorf("download failed: %w", original)

		var httpErr *HTTPError
		require.ErrorAs(t, wrapped, &httpErr)
		assert.Equal(t, 503, httpErr.StatusCode)
	})

	t.Run("errors.As does not match non-HTTPError", func(t *testing.T) {
		err := fmt.Errorf("some other error")
		var httpErr *HTTPError
		assert.NotErrorAs(t, err, &httpErr)
	})
}
