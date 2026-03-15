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

// Package scraping provides content downloading and processing functionality.
// Core download functions are provided by pkg/libaf/scraping.
// This package adds content processing (HTML extraction, PDF handling, image resizing).
package scraping

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"strings"
	"time"

	readability "codeberg.org/readeck/go-readability/v2"
	pdf "github.com/ajroetker/pdf"
	"github.com/ajroetker/pdf/render"
	"github.com/antflydb/antfly/pkg/libaf/s3"
	libscraping "github.com/antflydb/antfly/pkg/libaf/scraping"
	"go.uber.org/zap"
	"golang.org/x/image/draw"
)

// Re-export types and functions from libaf/scraping for convenience.
// This maintains backwards compatibility for existing consumers.
type (
	ContentSecurityConfig = libscraping.ContentSecurityConfig
	S3Credentials         = s3.Credentials
	RemoteContentConfig   = libscraping.RemoteContentConfig
	S3CredentialConfig    = libscraping.S3CredentialConfig
	HTTPCredentialConfig  = libscraping.HTTPCredentialConfig
	HTTPError             = libscraping.HTTPError
)

// IsSecurityConfigEmpty checks if a ContentSecurityConfig is effectively empty.
func IsSecurityConfigEmpty(cfg ContentSecurityConfig) bool {
	return len(cfg.AllowedHosts) == 0 &&
		len(cfg.AllowedPaths) == 0 &&
		!cfg.BlockPrivateIps &&
		cfg.DownloadTimeoutSeconds == 0 &&
		cfg.MaxDownloadSizeBytes == 0 &&
		cfg.MaxImageDimension == 0
}

// Package-level defaults for security config and S3 credentials.
// These can be set at startup by the application (e.g., from antfly/termite config).
var (
	defaultSecurityConfig *ContentSecurityConfig
	defaultS3Credentials  *S3Credentials
)

// GetDefaultSecurityConfig returns the default security configuration for content downloads.
// If no custom config has been set, returns safe defaults.
func GetDefaultSecurityConfig() *ContentSecurityConfig {
	if defaultSecurityConfig != nil {
		return defaultSecurityConfig
	}
	return &ContentSecurityConfig{
		BlockPrivateIps:        true,
		MaxDownloadSizeBytes:   100 * 1024 * 1024, // 100MB
		DownloadTimeoutSeconds: 30,
		MaxImageDimension:      2048,
	}
}

// SetDefaultSecurityConfig sets the package-level default security configuration.
// This affects all content downloads unless overridden.
func SetDefaultSecurityConfig(config *ContentSecurityConfig) {
	defaultSecurityConfig = config
}

// GetDefaultS3Credentials returns the default S3 credentials for content downloads.
// Returns nil if no credentials have been configured.
func GetDefaultS3Credentials() *S3Credentials {
	return defaultS3Credentials
}

// SetDefaultS3Credentials sets the package-level default S3 credentials.
// This enables S3 URL downloads in content processing.
func SetDefaultS3Credentials(creds *S3Credentials) {
	defaultS3Credentials = creds
}

// ProcessResult contains the result of processing content
type ProcessResult struct {
	// Data is the processed content
	Data []byte
	// Format describes the output type: "text", "image", or "data-url"
	Format string
	// Title is the page title (for HTML content)
	Title string
}

// ContentProcessor handles processing of downloaded content based on type
type ContentProcessor interface {
	// Process converts raw content to LLM-compatible format
	Process(ctx context.Context, contentType string, data []byte) (*ProcessResult, error)
}

// DownloadAndProcessLink downloads content from a URL and processes it based on content type.
// For S3 URLs, s3Creds must be provided with valid credentials.
func DownloadAndProcessLink(
	ctx context.Context,
	uri string,
	securityConfig *ContentSecurityConfig,
	s3Creds *S3Credentials,
	processor ContentProcessor,
) (*ProcessResult, error) {
	logger := zap.L()

	// Apply security config defaults if provided
	if securityConfig != nil {
		// Create child context with timeout
		if securityConfig.DownloadTimeoutSeconds > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(
				ctx,
				time.Duration(securityConfig.DownloadTimeoutSeconds)*time.Second,
			)
			defer cancel()
		}
	}

	// Download content (security validation is handled by DownloadContent)
	contentType, data, err := libscraping.DownloadContent(ctx, uri, securityConfig, s3Creds)
	if err != nil {
		logger.Error("failed to download content", zap.String("url", uri), zap.Error(err))
		return nil, fmt.Errorf("download failed: %w", err)
	}

	// Use default processor if none provided
	if processor == nil {
		processor = NewDefaultContentProcessor(securityConfig)
	}

	// Process content
	result, err := processor.Process(ctx, contentType, data)
	if err != nil {
		logger.Error(
			"failed to process content",
			zap.String("url", uri),
			zap.String("contentType", contentType),
			zap.Error(err),
		)
		return nil, fmt.Errorf("processing failed: %w", err)
	}

	return result, nil
}

// DefaultContentProcessor routes content processing based on MIME type
type DefaultContentProcessor struct {
	securityConfig *ContentSecurityConfig
}

// NewDefaultContentProcessor creates a default content processor
func NewDefaultContentProcessor(config *ContentSecurityConfig) *DefaultContentProcessor {
	return &DefaultContentProcessor{securityConfig: config}
}

// Process routes content based on MIME type
func (p *DefaultContentProcessor) Process(
	ctx context.Context,
	contentType string,
	data []byte,
) (*ProcessResult, error) {
	switch {
	case strings.HasPrefix(contentType, "text/html"):
		return extractHTMLText(data)
	case contentType == "application/pdf":
		return processPDF(data, p.securityConfig)
	case strings.HasPrefix(contentType, "image/"):
		return processImage(data, contentType, p.securityConfig)
	case strings.HasPrefix(contentType, "audio/"):
		return processAudio(data, contentType)
	case strings.HasPrefix(contentType, "text/"):
		// Plain text, return as-is
		return &ProcessResult{Data: data, Format: "text"}, nil
	default:
		return nil, fmt.Errorf("unsupported content type: %s", contentType)
	}
}

// extractHTMLText extracts readable text from HTML using go-readability
func extractHTMLText(data []byte) (*ProcessResult, error) {
	reader := bytes.NewReader(data)
	article, err := readability.FromReader(reader, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	// Return extracted text content
	var textBuf bytes.Buffer
	if err := article.RenderText(&textBuf); err != nil {
		// Fallback to HTML content if text rendering fails
		if err := article.RenderHTML(&textBuf); err != nil {
			return nil, fmt.Errorf("failed to render article content: %w", err)
		}
	}

	text := textBuf.String()
	if text == "" {
		// Try HTML as final fallback
		textBuf.Reset()
		if err := article.RenderHTML(&textBuf); err == nil {
			text = textBuf.String()
		}
	}

	return &ProcessResult{
		Data:   []byte(text),
		Format: "text",
		Title:  article.Title(),
	}, nil
}

// processPDF attempts text extraction, falls back to rendering first page as image
func processPDF(data []byte, config *ContentSecurityConfig) (*ProcessResult, error) {
	// Try text extraction first
	text, err := ExtractPDFText(data)
	if err == nil && strings.TrimSpace(text) != "" {
		return &ProcessResult{Data: []byte(text), Format: "text"}, nil
	}

	// Fallback to rendering first page as PNG
	imageData, err := renderPDFFirstPage(data)
	if err != nil {
		return nil, fmt.Errorf("failed to render PDF: %w", err)
	}

	// Resize if needed
	if config != nil && config.MaxImageDimension > 0 {
		imageData, err = resizeImageIfNeeded(imageData, "image/png", config.MaxImageDimension)
		if err != nil {
			return nil, fmt.Errorf("failed to resize PDF image: %w", err)
		}
	}

	// Convert to data URI
	dataURI := "data:image/png;base64," + base64.StdEncoding.EncodeToString(imageData)
	return &ProcessResult{Data: []byte(dataURI), Format: "image"}, nil
}

// ExtractPDFText extracts text from PDF using ajroetker/pdf
func ExtractPDFText(data []byte) (string, error) {
	reader := bytes.NewReader(data)
	pdfReader, err := pdf.NewReader(reader, int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("failed to open PDF: %w", err)
	}

	var textBuilder strings.Builder
	numPages := pdfReader.NumPage()

	for i := 1; i <= numPages; i++ {
		page := pdfReader.Page(i)
		if page.V.IsNull() {
			continue
		}

		text, err := page.GetPlainText(nil)
		if err != nil {
			continue
		}

		textBuilder.WriteString(text)
		textBuilder.WriteString("\n\n")
	}

	return textBuilder.String(), nil
}

// renderPDFFirstPage renders the first page of a PDF as PNG at 150 DPI.
// This is used for OCR fallback on pages with minimal or no extractable text.
func renderPDFFirstPage(data []byte) ([]byte, error) {
	// Create renderer from PDF data
	renderer, err := render.NewRenderer(data)
	if err != nil {
		return nil, fmt.Errorf("create pdf renderer: %w", err)
	}
	defer func() { _ = renderer.Close() }()

	// Check if there are any pages
	if renderer.NumPages() == 0 {
		return nil, errors.New("pdf has no pages")
	}

	// Render first page (1-indexed) at 150 DPI
	const dpi = 150
	pngData, err := renderer.RenderPageToPNG(1, dpi)
	if err != nil {
		return nil, fmt.Errorf("render page 1: %w", err)
	}

	return pngData, nil
}

// processAudio converts audio to data URI
func processAudio(data []byte, contentType string) (*ProcessResult, error) {
	// Convert to data URI - no processing needed for audio
	dataURI := fmt.Sprintf(
		"data:%s;base64,%s",
		contentType,
		base64.StdEncoding.EncodeToString(data),
	)
	return &ProcessResult{Data: []byte(dataURI), Format: "audio"}, nil
}

// processImage converts image to data URI, resizing if needed
func processImage(
	data []byte,
	contentType string,
	config *ContentSecurityConfig,
) (*ProcessResult, error) {
	// Resize if needed
	if config != nil && config.MaxImageDimension > 0 {
		var err error
		data, err = resizeImageIfNeeded(data, contentType, config.MaxImageDimension)
		if err != nil {
			return nil, fmt.Errorf("failed to resize image: %w", err)
		}
	}

	// Convert to data URI
	dataURI := fmt.Sprintf(
		"data:%s;base64,%s",
		contentType,
		base64.StdEncoding.EncodeToString(data),
	)
	return &ProcessResult{Data: []byte(dataURI), Format: "image"}, nil
}

// resizeImageIfNeeded resizes image if dimensions exceed maxDimension
func resizeImageIfNeeded(data []byte, contentType string, maxDimension int) ([]byte, error) {
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to decode image: %w", err)
	}

	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	// Check if resize is needed
	if width <= maxDimension && height <= maxDimension {
		return data, nil // No resize needed
	}

	// Calculate new dimensions maintaining aspect ratio
	var newWidth, newHeight int
	if width > height {
		newWidth = maxDimension
		newHeight = (height * maxDimension) / width
	} else {
		newHeight = maxDimension
		newWidth = (width * maxDimension) / height
	}

	// Create new image
	dst := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))
	draw.BiLinear.Scale(dst, dst.Bounds(), img, bounds, draw.Over, nil)

	// Encode back to bytes
	var buf bytes.Buffer
	if strings.Contains(contentType, "png") {
		err = png.Encode(&buf, dst)
	} else {
		err = jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 85})
	}

	if err != nil {
		return nil, fmt.Errorf("failed to encode resized image: %w", err)
	}

	return buf.Bytes(), nil
}
