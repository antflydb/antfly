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
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/antflydb/antfly/lib/scraping"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadTestPDF(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("../../pkg/docsaf/testdata/pdf/sample.pdf")
	if err != nil {
		t.Skip("Test PDF not found, skipping test")
	}
	return data
}

func TestPdfModeProcessor(t *testing.T) {
	ctx := context.Background()
	pdfData := loadTestPDF(t)

	t.Run("raw mode returns data URI with application/pdf", func(t *testing.T) {
		p := &pdfModeProcessor{mode: "raw"}
		result, err := p.Process(ctx, "application/pdf", pdfData)
		require.NoError(t, err)
		assert.Equal(t, "pdf", result.Format)

		dataURI := string(result.Data)
		assert.True(t, strings.HasPrefix(dataURI, "data:application/pdf;base64,"),
			"expected data URI prefix, got: %s", dataURI[:50])

		// Verify the base64 decodes back to the original PDF
		encoded := strings.TrimPrefix(dataURI, "data:application/pdf;base64,")
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		require.NoError(t, err)
		assert.Equal(t, pdfData, decoded)
	})

	t.Run("extract mode returns text content", func(t *testing.T) {
		p := &pdfModeProcessor{mode: "extract"}
		result, err := p.Process(ctx, "application/pdf", pdfData)
		require.NoError(t, err)
		assert.Equal(t, "text", result.Format)
		assert.NotEmpty(t, strings.TrimSpace(string(result.Data)))
	})

	t.Run("non-PDF content delegates to default processor", func(t *testing.T) {
		p := &pdfModeProcessor{mode: "raw"}

		// Image content should be processed by default processor
		// Use a minimal 1x1 PNG
		pngData := []byte{
			0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG header
			0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, // IHDR chunk
			0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, // 1x1
			0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53, 0xDE, // 8-bit RGB
			0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, // IDAT chunk
			0x08, 0xD7, 0x63, 0xF8, 0xCF, 0xC0, 0x00, 0x00,
			0x00, 0x02, 0x00, 0x01, 0xE2, 0x21, 0xBC, 0x33,
			0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, // IEND chunk
			0xAE, 0x42, 0x60, 0x82,
		}
		result, err := p.Process(ctx, "image/png", pngData)
		require.NoError(t, err)
		assert.Equal(t, "image", result.Format)
		assert.True(t, strings.HasPrefix(string(result.Data), "data:image/png;base64,"))
	})

	t.Run("non-PDF content ignores mode", func(t *testing.T) {
		// Even with extract mode, non-PDF content is handled normally
		p := &pdfModeProcessor{mode: "extract"}
		result, err := p.Process(ctx, "text/plain", []byte("hello"))
		require.NoError(t, err)
		assert.Equal(t, "text", result.Format)
		assert.Equal(t, "hello", string(result.Data))
	})

	t.Run("unsupported mode returns error", func(t *testing.T) {
		p := &pdfModeProcessor{mode: "invalid"}
		_, err := p.Process(ctx, "application/pdf", pdfData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported PDF mode")
	})
}

func TestValidateRemoteMediaMode(t *testing.T) {
	assert.NoError(t, validateRemoteMediaMode(""))
	assert.NoError(t, validateRemoteMediaMode("raw"))
	assert.NoError(t, validateRemoteMediaMode("extract"))
	assert.NoError(t, validateRemoteMediaMode("render"))
	assert.ErrorContains(t, validateRemoteMediaMode("invalid"), "invalid mode")
}

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
