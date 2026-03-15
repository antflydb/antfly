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
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/audio"
	"github.com/antflydb/antfly/lib/scraping"
	"github.com/mbleigh/raymond"
)

// errorToDirective converts an error into an error directive string.
// If the error is an HTTPError, the status code is included.
func errorToDirective(err error) raymond.SafeString {
	var httpErr *scraping.HTTPError
	if errors.As(err, &httpErr) {
		return raymond.SafeString(FormatErrorDirective(httpErr.StatusCode, httpErr.Status))
	}
	return raymond.SafeString(FormatErrorDirective(0, err.Error()))
}

// RemoteMediaFn is a Handlebars helper that downloads media (image or audio) from a URL
// and returns a Genkit dotprompt media directive with the content as a data URI.
// Usage: {{remoteMedia url="https://example.com/image.jpg"}}
// Usage: {{remoteMedia url="https://example.com/audio.mp3"}}
// Usage: {{remoteMedia url="s3://bucket/image.jpg" credentials="primary"}}
// Returns: <<<dotprompt:media:url data:image/png;base64,...>>> or <<<dotprompt:media:url data:audio/mpeg;base64,...>>>
func RemoteMediaFn(options *raymond.Options) raymond.SafeString {
	url := options.HashStr("url")
	if url == "" {
		log.Printf("RemoteMediaFn: missing required 'url' parameter")
		return raymond.SafeString("")
	}

	credentials := options.HashStr("credentials")

	// Resolve credentials and security config based on URL and explicit credential name
	s3Creds, securityConfig, err := scraping.ResolveS3Credentials(url, credentials)
	if err != nil {
		log.Printf("RemoteMediaFn: credential resolution failed: %v", err)
		// Fall back to defaults
		s3Creds = scraping.GetDefaultS3Credentials()
		securityConfig = scraping.GetDefaultSecurityConfig()
	}
	if securityConfig == nil {
		securityConfig = scraping.GetDefaultSecurityConfig()
	}

	// Use background context with timeout from security config
	ctx := context.Background()
	if securityConfig.DownloadTimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(
			ctx,
			time.Duration(securityConfig.DownloadTimeoutSeconds)*time.Second,
		)
		defer cancel()
	}

	// Download and process the media (image or audio)
	result, err := scraping.DownloadAndProcessLink(
		ctx,
		url,
		securityConfig,
		s3Creds,
		nil,
	)
	if err != nil {
		log.Printf("RemoteMediaFn: failed to download/process media from %s: %v", url, err)
		return errorToDirective(err)
	}

	// Verify we got an image, audio, or data-url output
	if result.Format != "image" && result.Format != "audio" && result.Format != "data-url" {
		log.Printf("RemoteMediaFn: expected image/audio output format, got %s", result.Format)
		return raymond.SafeString("")
	}

	// result.Data is already a data URI like "data:image/png;base64,..." or "data:audio/mpeg;base64,..."
	dataURI := string(result.Data)

	// Return Genkit dotprompt media directive
	return raymond.SafeString(fmt.Sprintf("<<<dotprompt:media:url %s>>>", dataURI))
}

// RemotePDFFn is a Handlebars helper that downloads a PDF from a URL and extracts text.
// Usage: {{remotePDF url="https://example.com/doc.pdf"}}
// Usage: {{remotePDF url="https://example.com/doc.pdf" output="markdown"}}
// Usage: {{remotePDF url="s3://bucket/doc.pdf" credentials="primary"}}
// The output parameter can be "text" (default) or "markdown"
// Returns: Extracted text content (plain string, not a dotprompt directive)
func RemotePDFFn(options *raymond.Options) raymond.SafeString {
	url := options.HashStr("url")
	if url == "" {
		log.Printf("RemotePDFFn: missing required 'url' parameter")
		return raymond.SafeString("")
	}

	output := options.HashStr("output")
	if output == "" {
		output = "text" // default to text output
	}

	credentials := options.HashStr("credentials")

	// Resolve credentials and security config based on URL and explicit credential name
	s3Creds, securityConfig, err := scraping.ResolveS3Credentials(url, credentials)
	if err != nil {
		log.Printf("RemotePDFFn: credential resolution failed: %v", err)
		// Fall back to defaults
		s3Creds = scraping.GetDefaultS3Credentials()
		securityConfig = scraping.GetDefaultSecurityConfig()
	}
	if securityConfig == nil {
		securityConfig = scraping.GetDefaultSecurityConfig()
	}

	// Use background context with timeout from security config
	ctx := context.Background()
	if securityConfig.DownloadTimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(
			ctx,
			time.Duration(securityConfig.DownloadTimeoutSeconds)*time.Second,
		)
		defer cancel()
	}

	// Create a custom processor for PDF that only extracts text
	processor := &pdfTextProcessor{}

	// Download and process the PDF
	result, err := scraping.DownloadAndProcessLink(
		ctx,
		url,
		securityConfig,
		s3Creds,
		processor,
	)
	if err != nil {
		log.Printf("RemotePDFFn: failed to download/process PDF from %s: %v", url, err)
		return errorToDirective(err)
	}

	// Verify we got text output
	if result.Format != "text" {
		log.Printf("RemotePDFFn: expected text output format, got %s", result.Format)
		return raymond.SafeString("")
	}

	text := string(result.Data)

	// Apply output formatting
	switch output {
	case "markdown":
		// For markdown output, we could add some basic formatting
		// For now, just return the text as-is since PDFs don't have inherent markdown structure
		// In the future, could add header detection, list formatting, etc.
		return raymond.SafeString(text)
	case "text":
		fallthrough
	default:
		return raymond.SafeString(text)
	}
}

// RemoteTextFn is a Handlebars helper that downloads text content from a URL.
// It preserves the format as-is (HTML stays HTML, markdown stays markdown, etc.)
// Usage: {{remoteText url="https://example.com/article.html"}}
// Usage: {{remoteText url="s3://bucket/doc.txt" credentials="primary"}}
// Returns: Content as plain string (not a dotprompt directive)
func RemoteTextFn(options *raymond.Options) raymond.SafeString {
	url := options.HashStr("url")
	if url == "" {
		log.Printf("RemoteTextFn: missing required 'url' parameter")
		return raymond.SafeString("")
	}

	credentials := options.HashStr("credentials")

	// Resolve credentials and security config based on URL and explicit credential name
	s3Creds, securityConfig, err := scraping.ResolveS3Credentials(url, credentials)
	if err != nil {
		log.Printf("RemoteTextFn: credential resolution failed: %v", err)
		// Fall back to defaults
		s3Creds = scraping.GetDefaultS3Credentials()
		securityConfig = scraping.GetDefaultSecurityConfig()
	}
	if securityConfig == nil {
		securityConfig = scraping.GetDefaultSecurityConfig()
	}

	// Use background context with timeout from security config
	ctx := context.Background()
	if securityConfig.DownloadTimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(
			ctx,
			time.Duration(securityConfig.DownloadTimeoutSeconds)*time.Second,
		)
		defer cancel()
	}

	// Create a custom processor that preserves text as-is
	processor := &preserveTextProcessor{}

	// Download and process the content
	result, err := scraping.DownloadAndProcessLink(ctx, url, securityConfig, s3Creds, processor)
	if err != nil {
		log.Printf("RemoteTextFn: failed to download/process text from %s: %v", url, err)
		return errorToDirective(err)
	}

	return raymond.SafeString(string(result.Data))
}

// pdfTextProcessor is a custom ContentProcessor that only extracts text from PDFs
type pdfTextProcessor struct{}

func (p *pdfTextProcessor) Process(
	ctx context.Context,
	contentType string,
	data []byte,
) (*scraping.ProcessResult, error) {
	if contentType != "application/pdf" {
		return nil, fmt.Errorf("expected PDF content type, got %s", contentType)
	}

	// Extract text from PDF
	text, err := scraping.ExtractPDFText(data)
	if err != nil {
		return nil, fmt.Errorf("failed to extract PDF text: %w", err)
	}

	return &scraping.ProcessResult{Data: []byte(text), Format: "text"}, nil
}

// preserveTextProcessor is a custom ContentProcessor that preserves text content as-is
type preserveTextProcessor struct{}

func (p *preserveTextProcessor) Process(
	ctx context.Context,
	contentType string,
	data []byte,
) (*scraping.ProcessResult, error) {
	// For text content types, return as-is
	if strings.HasPrefix(contentType, "text/") {
		return &scraping.ProcessResult{Data: data, Format: "text"}, nil
	}

	return nil, fmt.Errorf("unsupported content type for text processing: %s", contentType)
}

// TranscribeAudioFn is a Handlebars helper that transcribes audio to text.
// It uses the default STT provider configured in Antfly.
// Usage: {{transcribeAudio url="https://example.com/audio.mp3"}}
// Usage: {{transcribeAudio url="s3://bucket/audio.wav" credentials="primary"}}
// Usage: {{transcribeAudio url="https://example.com/audio.mp3" language="en"}}
// Returns: Transcribed text content (plain string)
func TranscribeAudioFn(options *raymond.Options) raymond.SafeString {
	url := options.HashStr("url")
	if url == "" {
		log.Printf("TranscribeAudioFn: missing required 'url' parameter")
		return raymond.SafeString("")
	}

	// Get default STT provider
	stt := audio.GetDefaultSTT()
	if stt == nil {
		log.Printf("TranscribeAudioFn: no default STT provider configured")
		return raymond.SafeString("")
	}

	credentials := options.HashStr("credentials")
	language := options.HashStr("language")

	// Resolve S3 credentials if needed
	s3Creds, securityConfig, err := scraping.ResolveS3Credentials(url, credentials)
	if err != nil {
		log.Printf("TranscribeAudioFn: credential resolution failed: %v", err)
		s3Creds = scraping.GetDefaultS3Credentials()
		securityConfig = scraping.GetDefaultSecurityConfig()
	}
	if securityConfig == nil {
		securityConfig = scraping.GetDefaultSecurityConfig()
	}

	// Use background context with timeout from security config
	ctx := context.Background()
	if securityConfig.DownloadTimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(
			ctx,
			time.Duration(securityConfig.DownloadTimeoutSeconds)*time.Second,
		)
		defer cancel()
	}

	// Build transcribe request - the STT provider handles URL downloading
	req := audio.TranscribeRequest{
		URL:      url,
		Language: language,
	}

	// Add S3 credentials if we have them
	if s3Creds != nil {
		req.S3Credentials = s3Creds
	}

	// Transcribe the audio
	resp, err := stt.Transcribe(ctx, req)
	if err != nil {
		log.Printf("TranscribeAudioFn: failed to transcribe audio from %s: %v", url, err)
		return errorToDirective(err)
	}

	return raymond.SafeString(resp.Text)
}

// init registers the remote content helpers with Handlebars
func init() {
	raymond.RegisterHelper("remoteMedia", RemoteMediaFn)
	raymond.RegisterHelper("remotePDF", RemotePDFFn)
	raymond.RegisterHelper("remoteText", RemoteTextFn)
	raymond.RegisterHelper("transcribeAudio", TranscribeAudioFn)
}
