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

package scraping

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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

func TestParseDataURIWithLimit(t *testing.T) {
	t.Run("rejects base64 before decode", func(t *testing.T) {
		payload := base64.StdEncoding.EncodeToString([]byte("0123456789"))
		_, _, err := ParseDataURIWithLimit("data:text/plain;base64,"+payload, 9)
		require.ErrorIs(t, err, ErrDownloadTooLarge)
	})

	t.Run("allows payload at limit", func(t *testing.T) {
		payload := base64.StdEncoding.EncodeToString([]byte("0123456789"))
		contentType, data, err := ParseDataURIWithLimit("data:text/plain;base64,"+payload, 10)
		require.NoError(t, err)
		assert.Equal(t, "text/plain", contentType)
		assert.Equal(t, []byte("0123456789"), data)
	})

	t.Run("rejects non-base64 payload before copy", func(t *testing.T) {
		_, _, err := ParseDataURIWithLimit("data:text/plain,0123456789", 9)
		require.ErrorIs(t, err, ErrDownloadTooLarge)
	})
}

func TestDownloadContentRejectsHTTPOverLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("0123456789"))
	}))
	defer srv.Close()

	_, _, err := DownloadContent(context.Background(), srv.URL, &ContentSecurityConfig{MaxDownloadSizeBytes: 9}, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrDownloadTooLarge), "expected ErrDownloadTooLarge, got %v", err)
}
