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

// resolveCredentials resolves S3 credentials and security config for a URL,
// falling back to defaults on error.
func resolveCredentials(url, credentials string) (*scraping.S3Credentials, *scraping.ContentSecurityConfig) {
	s3Creds, securityConfig, err := scraping.ResolveS3Credentials(url, credentials)
	if err != nil {
		log.Printf("credential resolution failed for %s: %v", url, err)
		s3Creds = scraping.GetDefaultS3Credentials()
		securityConfig = scraping.GetDefaultSecurityConfig()
	}
	if securityConfig == nil {
		securityConfig = scraping.GetDefaultSecurityConfig()
	}
	return s3Creds, securityConfig
}

func validateRemoteMediaMode(mode string) error {
	if mode == "" {
		return nil
	}
	switch mode {
	case "raw", "extract", "render":
		return nil
	default:
		return fmt.Errorf("invalid mode %q, must be raw, extract, or render", mode)
	}
}

// RemoteMediaFn is a Handlebars helper that downloads media (image, audio, or PDF) from a URL
// and returns a Genkit dotprompt media directive with the content as a data URI.
// Usage: {{remoteMedia url="https://example.com/image.jpg"}}
// Usage: {{remoteMedia url="https://example.com/audio.mp3"}}
// Usage: {{remoteMedia url="https://example.com/doc.pdf"}}
// Usage: {{remoteMedia url="https://example.com/doc.pdf" mode="render"}}
// Usage: {{remoteMedia url="s3://bucket/image.jpg" credentials="primary"}}
//
// The mode parameter controls PDF processing (ignored for non-PDF content):
//   - "raw" (default): returns the PDF as-is via data:application/pdf;base64,... data URI
//   - "extract": extracts text from the PDF and returns it as plain text (not a media directive)
//   - "render": renders the first page as image/png
func RemoteMediaFn(options *raymond.Options) raymond.SafeString {
	url := options.HashStr("url")
	if url == "" {
		log.Printf("RemoteMediaFn: missing required 'url' parameter")
		return raymond.SafeString("")
	}

	mode := options.HashStr("mode")
	if mode == "" {
		mode = "raw"
	}
	if err := validateRemoteMediaMode(mode); err != nil {
		log.Printf("RemoteMediaFn: %v", err)
		return errorToDirective(err)
	}

	s3Creds, securityConfig := resolveCredentials(url, options.HashStr("credentials"))

	processor := &pdfModeProcessor{
		mode:           mode,
		securityConfig: securityConfig,
	}

	// DownloadAndProcessLink handles timeout from securityConfig
	result, err := scraping.DownloadAndProcessLink(
		context.Background(),
		url,
		securityConfig,
		s3Creds,
		processor,
	)
	if err != nil {
		log.Printf("RemoteMediaFn: failed to download/process media from %s: %v", url, err)
		return errorToDirective(err)
	}

	// For extract mode on PDFs, return plain text (not a media directive)
	if result.Format == scraping.FormatText {
		return raymond.SafeString(string(result.Data))
	}

	// Verify we got a media output
	if result.Format != scraping.FormatImage && result.Format != scraping.FormatAudio && result.Format != scraping.FormatPDF {
		log.Printf("RemoteMediaFn: unexpected output format %q", result.Format)
		return raymond.SafeString("")
	}

	// Build media directive without intermediate string copies of the data URI
	const prefix = "<<<dotprompt:media:url "
	const suffix = ">>>"
	var sb strings.Builder
	sb.Grow(len(prefix) + len(result.Data) + len(suffix))
	sb.WriteString(prefix)
	sb.Write(result.Data)
	sb.WriteString(suffix)
	return raymond.SafeString(sb.String())
}

// pdfModeProcessor wraps the default content processor but overrides PDF handling
// based on the configured mode (raw, extract, render).
type pdfModeProcessor struct {
	mode           string
	securityConfig *scraping.ContentSecurityConfig
}

func (p *pdfModeProcessor) Process(
	ctx context.Context,
	contentType string,
	data []byte,
) (*scraping.ProcessResult, error) {
	// For non-PDF content, delegate to the default processor
	if contentType != "application/pdf" {
		return scraping.NewDefaultContentProcessor(p.securityConfig).Process(ctx, contentType, data)
	}

	switch p.mode {
	case "raw":
		return &scraping.ProcessResult{Data: scraping.EncodeDataURI("application/pdf", data), Format: scraping.FormatPDF}, nil

	case "extract":
		text, err := scraping.ExtractPDFText(data)
		if err != nil {
			return nil, fmt.Errorf("failed to extract PDF text: %w", err)
		}
		if strings.TrimSpace(text) == "" {
			return nil, fmt.Errorf("no extractable text found in PDF")
		}
		return &scraping.ProcessResult{Data: []byte(text), Format: scraping.FormatText}, nil

	case "render":
		return scraping.RenderPDFAsImage(data, p.securityConfig)

	default:
		return nil, fmt.Errorf("unsupported PDF mode: %s", p.mode)
	}
}

// RemotePDFFn is a Handlebars helper that downloads a PDF from a URL and extracts text.
// Equivalent to {{remoteMedia url="..." mode="extract"}} but kept for backward compatibility.
// Usage: {{remotePDF url="https://example.com/doc.pdf"}}
// Usage: {{remotePDF url="s3://bucket/doc.pdf" credentials="primary"}}
// Returns: Extracted text content (plain string, not a dotprompt directive)
func RemotePDFFn(options *raymond.Options) raymond.SafeString {
	url := options.HashStr("url")
	if url == "" {
		log.Printf("RemotePDFFn: missing required 'url' parameter")
		return raymond.SafeString("")
	}

	s3Creds, securityConfig := resolveCredentials(url, options.HashStr("credentials"))

	// DownloadAndProcessLink handles timeout from securityConfig
	result, err := scraping.DownloadAndProcessLink(
		context.Background(),
		url,
		securityConfig,
		s3Creds,
		&pdfTextProcessor{},
	)
	if err != nil {
		log.Printf("RemotePDFFn: failed to download/process PDF from %s: %v", url, err)
		return errorToDirective(err)
	}

	if result.Format != scraping.FormatText {
		log.Printf("RemotePDFFn: expected text output format, got %s", result.Format)
		return raymond.SafeString("")
	}

	return raymond.SafeString(string(result.Data))
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

	s3Creds, securityConfig := resolveCredentials(url, options.HashStr("credentials"))

	// DownloadAndProcessLink handles timeout from securityConfig
	result, err := scraping.DownloadAndProcessLink(
		context.Background(),
		url,
		securityConfig,
		s3Creds,
		&preserveTextProcessor{},
	)
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

	return &scraping.ProcessResult{Data: []byte(text), Format: scraping.FormatText}, nil
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
		return &scraping.ProcessResult{Data: data, Format: scraping.FormatText}, nil
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

	stt := audio.GetDefaultSTT()
	if stt == nil {
		log.Printf("TranscribeAudioFn: no default STT provider configured")
		return raymond.SafeString("")
	}

	s3Creds, securityConfig := resolveCredentials(url, options.HashStr("credentials"))

	// TranscribeAudioFn doesn't use DownloadAndProcessLink, so set timeout here
	ctx := context.Background()
	if securityConfig.DownloadTimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(
			ctx,
			time.Duration(securityConfig.DownloadTimeoutSeconds)*time.Second,
		)
		defer cancel()
	}

	req := audio.TranscribeRequest{
		URL:      url,
		Language: options.HashStr("language"),
	}
	if s3Creds != nil {
		req.S3Credentials = s3Creds
	}

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
