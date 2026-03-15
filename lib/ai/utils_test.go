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

package ai

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_textToParts(t *testing.T) {
	t.Run("HTTP URL only", func(t *testing.T) {
		text := "http://example.com/image.jpg"
		parts := textToParts(text)

		require.Len(t, parts, 1)
		imageContent, ok := parts[0].(ImageURLContent)
		require.True(t, ok, "Expected ImageURLContent")
		assert.Equal(t, text, imageContent.URL)
	})

	t.Run("HTTPS URL only", func(t *testing.T) {
		text := "https://example.com/photo.png"
		parts := textToParts(text)

		require.Len(t, parts, 1)
		imageContent, ok := parts[0].(ImageURLContent)
		require.True(t, ok, "Expected ImageURLContent")
		assert.Equal(t, text, imageContent.URL)
	})

	t.Run("S3 URL only", func(t *testing.T) {
		text := "s3://bucket/key/image.jpg"
		parts := textToParts(text)

		require.Len(t, parts, 1)
		imageContent, ok := parts[0].(ImageURLContent)
		require.True(t, ok, "Expected ImageURLContent")
		assert.Equal(t, text, imageContent.URL)
	})

	t.Run("file URL only", func(t *testing.T) {
		text := "file:///path/to/image.jpg"
		parts := textToParts(text)

		require.Len(t, parts, 1)
		imageContent, ok := parts[0].(ImageURLContent)
		require.True(t, ok, "Expected ImageURLContent")
		assert.Equal(t, text, imageContent.URL)
	})

	t.Run("data URI only", func(t *testing.T) {
		text := "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUA"
		parts := textToParts(text)

		require.Len(t, parts, 1)
		imageContent, ok := parts[0].(ImageURLContent)
		require.True(t, ok, "Expected ImageURLContent")
		assert.Equal(t, text, imageContent.URL)
	})

	t.Run("plain text without URLs", func(t *testing.T) {
		text := "This is just plain text without any URLs."
		parts := textToParts(text)

		require.Len(t, parts, 1)
		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "Expected TextContent")
		assert.Equal(t, text, textContent.Text)
	})

	t.Run("text with single embedded URL", func(t *testing.T) {
		text := "Check out this image: https://example.com/photo.jpg for more details."
		parts := textToParts(text)

		require.Len(t, parts, 2, "Should have text and one image URL")

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(
			t,
			"Check out this image: <see appended content> for more details.",
			textContent.Text,
		)

		imageContent, ok := parts[1].(ImageURLContent)
		require.True(t, ok, "Second part should be ImageURLContent")
		assert.Equal(t, "https://example.com/photo.jpg", imageContent.URL)
	})

	t.Run("text with multiple embedded URLs", func(t *testing.T) {
		text := "First image: https://example.com/1.jpg and second: https://example.com/2.png here."
		parts := textToParts(text)

		require.Len(t, parts, 3, "Should have text and two image URLs")

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(
			t,
			"First image: <see appended content> and second: <see appended content> here.",
			textContent.Text,
		)

		imageContent1, ok := parts[1].(ImageURLContent)
		require.True(t, ok, "Second part should be ImageURLContent")
		assert.Equal(t, "https://example.com/1.jpg", imageContent1.URL)

		imageContent2, ok := parts[2].(ImageURLContent)
		require.True(t, ok, "Third part should be ImageURLContent")
		assert.Equal(t, "https://example.com/2.png", imageContent2.URL)
	})

	t.Run("text with HTTP (not HTTPS) embedded URL", func(t *testing.T) {
		text := "Visit http://example.com/image.jpg for the photo."
		parts := textToParts(text)

		require.Len(t, parts, 2)

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(t, "Visit <see appended content> for the photo.", textContent.Text)

		imageContent, ok := parts[1].(ImageURLContent)
		require.True(t, ok, "Second part should be ImageURLContent")
		assert.Equal(t, "http://example.com/image.jpg", imageContent.URL)
	})

	t.Run("empty string", func(t *testing.T) {
		text := ""
		parts := textToParts(text)

		require.Len(t, parts, 1)
		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "Expected TextContent")
		assert.Empty(t, textContent.Text)
	})

	t.Run("URL at beginning of text followed by space", func(t *testing.T) {
		// When the string starts with https: it's treated as a pure URL
		// even if there's trailing text with no clear URL boundary
		text := "https://example.com/photo.jpg is a great image"
		parts := textToParts(text)

		// This is treated as a pure URL because it starts with https:
		require.Len(t, parts, 1)

		imageContent, ok := parts[0].(ImageURLContent)
		require.True(t, ok, "Should be treated as ImageURLContent when starting with https:")
		assert.Equal(t, text, imageContent.URL)
	})

	t.Run("URL at end of text", func(t *testing.T) {
		text := "Here is the image: https://example.com/photo.jpg"
		parts := textToParts(text)

		require.Len(t, parts, 2)

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(t, "Here is the image: <see appended content>", textContent.Text)

		imageContent, ok := parts[1].(ImageURLContent)
		require.True(t, ok, "Second part should be ImageURLContent")
		assert.Equal(t, "https://example.com/photo.jpg", imageContent.URL)
	})

	t.Run("text with FTP URL (should be extracted)", func(t *testing.T) {
		text := "Download from ftp://files.example.com/data.zip"
		parts := textToParts(text)

		require.Len(t, parts, 2)

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(t, "Download from <see appended content>", textContent.Text)

		imageContent, ok := parts[1].(ImageURLContent)
		require.True(t, ok, "Second part should be ImageURLContent")
		assert.Equal(t, "ftp://files.example.com/data.zip", imageContent.URL)
	})

	t.Run("text with same URL multiple times", func(t *testing.T) {
		text := "See https://example.com/img.jpg here and https://example.com/img.jpg again."
		parts := textToParts(text)

		require.Len(
			t,
			parts,
			3,
			"Should have text and two image URLs (duplicates counted separately)",
		)

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(
			t,
			"See <see appended content> here and <see appended content> again.",
			textContent.Text,
		)

		imageContent1, ok := parts[1].(ImageURLContent)
		require.True(t, ok, "Second part should be ImageURLContent")
		assert.Equal(t, "https://example.com/img.jpg", imageContent1.URL)

		imageContent2, ok := parts[2].(ImageURLContent)
		require.True(t, ok, "Third part should be ImageURLContent")
		assert.Equal(t, "https://example.com/img.jpg", imageContent2.URL)
	})

	t.Run("URL-like text but not at start", func(t *testing.T) {
		// This tests that we check prefix strictly
		text := "Visit our site at https://example.com/page"
		parts := textToParts(text)

		// Should extract the URL since it's embedded
		require.Len(t, parts, 2)

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(t, "Visit our site at <see appended content>", textContent.Text)

		imageContent, ok := parts[1].(ImageURLContent)
		require.True(t, ok, "Second part should be ImageURLContent")
		assert.Equal(t, "https://example.com/page", imageContent.URL)
	})

	t.Run("text with URL in parentheses", func(t *testing.T) {
		text := "Check this (https://example.com/image.jpg) out!"
		parts := textToParts(text)

		require.Len(t, parts, 2)

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(t, "Check this (<see appended content>) out!", textContent.Text)

		imageContent, ok := parts[1].(ImageURLContent)
		require.True(t, ok, "Second part should be ImageURLContent")
		assert.Equal(t, "https://example.com/image.jpg", imageContent.URL)
	})

	t.Run("text with markdown-style URL", func(t *testing.T) {
		text := "See [this image](https://example.com/photo.jpg) for details."
		parts := textToParts(text)

		require.Len(t, parts, 2)

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(t, "See [this image](<see appended content>) for details.", textContent.Text)

		imageContent, ok := parts[1].(ImageURLContent)
		require.True(t, ok, "Second part should be ImageURLContent")
		assert.Equal(t, "https://example.com/photo.jpg", imageContent.URL)
	})

	t.Run("mixed protocol URLs", func(t *testing.T) {
		text := "HTTP: http://a.com/1.jpg HTTPS: https://b.com/2.png FTP: ftp://c.com/3.gif"
		parts := textToParts(text)

		require.Len(t, parts, 4, "Should have text and three URLs")

		textContent, ok := parts[0].(TextContent)
		require.True(t, ok, "First part should be TextContent")
		assert.Equal(
			t,
			"HTTP: <see appended content> HTTPS: <see appended content> FTP: <see appended content>",
			textContent.Text,
		)

		assert.Equal(t, "http://a.com/1.jpg", parts[1].(ImageURLContent).URL)
		assert.Equal(t, "https://b.com/2.png", parts[2].(ImageURLContent).URL)
		assert.Equal(t, "ftp://c.com/3.gif", parts[3].(ImageURLContent).URL)
	})
}

func TestTextToParts(t *testing.T) {
	t.Run("no markers falls back to plain text parsing", func(t *testing.T) {
		parts, err := TextToParts("hello world")
		require.NoError(t, err)
		require.Len(t, parts, 1)
		assert.Equal(t, "hello world", parts[0].(TextContent).Text)
	})

	t.Run("single data URI marker", func(t *testing.T) {
		text := "<<<dotprompt:media:url data:image/png;base64,aWdv>>>"
		parts, err := TextToParts(text)
		require.NoError(t, err)
		require.Len(t, parts, 1)
		bc, ok := parts[0].(BinaryContent)
		require.True(t, ok, "expected BinaryContent, got %T", parts[0])
		assert.Equal(t, "image/png", bc.MIMEType)
		assert.Equal(t, []byte("igo"), bc.Data)
	})

	t.Run("marker with surrounding text", func(t *testing.T) {
		text := "A blue square <<<dotprompt:media:url data:image/png;base64,aWdv>>> described here"
		parts, err := TextToParts(text)
		require.NoError(t, err)
		require.Len(t, parts, 3)
		assert.Equal(t, "A blue square", parts[0].(TextContent).Text)
		bc := parts[1].(BinaryContent)
		assert.Equal(t, "image/png", bc.MIMEType)
		assert.Equal(t, "described here", parts[2].(TextContent).Text)
	})

	t.Run("URL marker (not data URI)", func(t *testing.T) {
		text := "<<<dotprompt:media:url https://example.com/img.png>>>"
		parts, err := TextToParts(text)
		require.NoError(t, err)
		require.Len(t, parts, 1)
		assert.Equal(t, "https://example.com/img.png", parts[0].(ImageURLContent).URL)
	})

	t.Run("multiple markers", func(t *testing.T) {
		text := "Image: <<<dotprompt:media:url data:image/png;base64,aWdv>>> Audio: <<<dotprompt:media:url data:audio/wav;base64,AAAA>>>"
		parts, err := TextToParts(text)
		require.NoError(t, err)
		require.Len(t, parts, 4)
		assert.Equal(t, "Image:", parts[0].(TextContent).Text)
		assert.Equal(t, "image/png", parts[1].(BinaryContent).MIMEType)
		assert.Equal(t, "Audio:", parts[2].(TextContent).Text)
		assert.Equal(t, "audio/wav", parts[3].(BinaryContent).MIMEType)
	})

	t.Run("error directive only is stripped to empty", func(t *testing.T) {
		text := "<<<error:status=404 message=Not Found>>>"
		parts, err := TextToParts(text)
		require.NoError(t, err)
		// After stripping the directive, the remaining text is empty
		require.Len(t, parts, 1)
		assert.Empty(t, parts[0].(TextContent).Text)
	})

	t.Run("error directive with surrounding text is stripped", func(t *testing.T) {
		text := "Some text <<<error:message=timeout>>> more text"
		parts, err := TextToParts(text)
		require.NoError(t, err)
		require.Len(t, parts, 1)
		tc, ok := parts[0].(TextContent)
		require.True(t, ok)
		assert.NotContains(t, tc.Text, "<<<error:")
	})

	t.Run("error directive stripped before dotprompt parsing", func(t *testing.T) {
		text := "<<<dotprompt:media:url https://example.com/img.png>>> <<<error:status=503 message=Service Unavailable>>>"
		parts, err := TextToParts(text)
		require.NoError(t, err)
		// Should have the image URL, but no error directive text
		found := false
		for _, p := range parts {
			if img, ok := p.(ImageURLContent); ok {
				assert.Equal(t, "https://example.com/img.png", img.URL)
				found = true
			}
			if tc, ok := p.(TextContent); ok {
				assert.NotContains(t, tc.Text, "<<<error:")
			}
		}
		assert.True(t, found, "should contain the image URL part")
	})
}
