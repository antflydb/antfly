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

package audio

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/antflydb/antfly/lib/scraping"
	"github.com/antflydb/antfly/pkg/libaf/s3"
	libscraping "github.com/antflydb/antfly/pkg/libaf/scraping"
	"github.com/minio/minio-go/v7"
)

// DownloadAudio downloads audio from a URL (http, https, s3, file, or data URI).
// Uses the existing scraping infrastructure for downloading with security validation.
func DownloadAudio(ctx context.Context, uri string, s3Creds *s3.Credentials) ([]byte, AudioFormat, error) {
	// Get default security config for validation
	securityConfig := scraping.GetDefaultSecurityConfig()

	contentType, data, err := libscraping.DownloadContent(ctx, uri, securityConfig, s3Creds)
	if err != nil {
		return nil, "", fmt.Errorf("downloading audio: %w", err)
	}

	format := FormatFromMIME(contentType)
	if format == "" {
		// Try extension-based detection
		format = formatFromExtension(uri)
	}

	return data, format, nil
}

// UploadAudioToS3 uploads audio bytes to an S3 URI.
func UploadAudioToS3(ctx context.Context, s3URI string, creds *s3.Credentials, data []byte, format AudioFormat) error {
	parsed, err := url.Parse(s3URI)
	if err != nil {
		return fmt.Errorf("parsing S3 URI: %w", err)
	}

	if parsed.Scheme != "s3" {
		return fmt.Errorf("expected s3:// URI, got %s", parsed.Scheme)
	}

	bucket := parsed.Host
	key := strings.TrimPrefix(parsed.Path, "/")

	if creds == nil {
		return fmt.Errorf("S3 credentials required for upload")
	}

	client, err := creds.NewMinioClient()
	if err != nil {
		return fmt.Errorf("creating S3 client: %w", err)
	}

	_, err = client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: format.MIMEType(),
	})
	if err != nil {
		return fmt.Errorf("uploading audio: %w", err)
	}

	return nil
}

// formatFromExtension detects audio format from file extension in a path/URL.
func formatFromExtension(path string) AudioFormat {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".mp3":
		return AudioFormatMp3
	case ".wav":
		return AudioFormatWav
	case ".ogg":
		return AudioFormatOgg
	case ".opus":
		return AudioFormatOpus
	case ".flac":
		return AudioFormatFlac
	case ".aac", ".m4a":
		return AudioFormatAac
	case ".webm":
		return AudioFormatWebm
	case ".pcm":
		return AudioFormatPcm
	default:
		return AudioFormatMp3 // Default
	}
}
