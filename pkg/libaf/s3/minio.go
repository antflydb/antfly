package s3

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// NewMinioClient creates a Minio client from a Credentials struct.
// This is the preferred method when using the full Credentials configuration.
// The endpoint can be either a hostname (e.g., "s3.amazonaws.com") or a full URL
// (e.g., "https://storage.googleapis.com"). If a URL with scheme is provided,
// the scheme is stripped and used to infer the SSL setting.
func (creds *Credentials) NewMinioClient() (*minio.Client, error) {
	if creds.Endpoint == "" {
		return nil, errors.New("endpoint is required")
	}
	if creds.AccessKeyId == "" {
		return nil, errors.New("access key ID is required")
	}
	if creds.SecretAccessKey == "" {
		return nil, errors.New("secret access key is required")
	}

	endpoint, secure := parseEndpoint(creds.Endpoint, creds.UseSsl)

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(creds.AccessKeyId, creds.SecretAccessKey, creds.SessionToken),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("creating S3 client for endpoint %s: %w", endpoint, err)
	}
	return minioClient, nil
}

// parseEndpoint extracts the host from an endpoint that may be a full URL or just a hostname.
// If the endpoint has a scheme (http:// or https://), the scheme is stripped and used to
// determine the SSL setting. Otherwise, the provided useSsl value is used.
// Returns the cleaned endpoint (host only) and whether to use SSL.
func parseEndpoint(endpoint string, useSsl bool) (string, bool) {
	// Check if endpoint looks like a URL (has scheme)
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		parsed, err := url.Parse(endpoint)
		if err == nil && parsed.Host != "" {
			// Use scheme to determine SSL: https = true, http = false
			return parsed.Host, parsed.Scheme == "https"
		}
	}
	// Not a URL or failed to parse - use as-is with provided useSsl
	return endpoint, useSsl
}

// DownloadObjectOptions configures how an S3 object is downloaded.
type DownloadObjectOptions struct {
	// ProgressFn is called for each part downloaded (partNumber, partSize, totalParts).
	// If nil, no progress tracking is performed.
	ProgressFn func(partNumber, partSize, totalParts int)
}

// DownloadObject downloads an object from S3 to a local file.
// For multipart objects, it downloads parts individually for progress tracking.
// The download is atomic - a temp file is used and renamed on success.
func (creds *Credentials) DownloadObject(
	ctx context.Context,
	bucketName, objectKey, destPath string,
	opts *DownloadObjectOptions,
) error {
	client, err := creds.NewMinioClient()
	if err != nil {
		return fmt.Errorf("creating S3 client: %w", err)
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0o750); err != nil { //nolint:gosec // destPath is controlled by caller
		return fmt.Errorf("creating directory %s: %w", filepath.Dir(destPath), err)
	}

	// Try to get object attributes for multipart download with progress
	attrs, err := client.GetObjectAttributes(ctx, bucketName, objectKey, minio.ObjectAttributesOptions{})
	if err == nil && attrs.ObjectParts.PartsCount > 1 && opts != nil && opts.ProgressFn != nil {
		// Multipart object - download parts individually for progress tracking
		return creds.downloadMultipart(ctx, client, bucketName, objectKey, destPath, attrs, opts.ProgressFn)
	}

	// Single-part object or GetObjectAttributes not supported - use simple download
	if err := client.FGetObject(ctx, bucketName, objectKey, destPath, minio.GetObjectOptions{}); err != nil {
		return fmt.Errorf("downloading object %s from bucket %s: %w", objectKey, bucketName, err)
	}

	return nil
}

// downloadMultipart downloads a multipart object part-by-part with progress tracking.
func (creds *Credentials) downloadMultipart(
	ctx context.Context,
	client *minio.Client,
	bucketName, objectKey, destPath string,
	attrs *minio.ObjectAttributes,
	progressFn func(partNumber, partSize, totalParts int),
) error {
	filePartPath := destPath + ".part.minio"
	f, err := os.OpenFile(filePartPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // filePartPath derived from caller-controlled destPath
	if err != nil {
		return fmt.Errorf("creating temp file %s: %w", filePartPath, err)
	}

	cleanupOnError := true
	defer func() {
		if cleanupOnError {
			_ = f.Close()
			_ = os.Remove(filePartPath)
		}
	}()

	for _, part := range attrs.ObjectParts.Parts {
		if progressFn != nil {
			progressFn(part.PartNumber, part.Size, attrs.ObjectParts.PartsCount)
		}

		obj, err := client.GetObject(ctx, bucketName, objectKey, minio.GetObjectOptions{
			VersionID:  attrs.VersionID,
			PartNumber: part.PartNumber,
		})
		if err != nil {
			return fmt.Errorf("getting part %d: %w", part.PartNumber, err)
		}

		_, copyErr := io.Copy(f, obj)
		_ = obj.Close()
		if copyErr != nil {
			return fmt.Errorf("downloading part %d: %w", part.PartNumber, copyErr)
		}
	}

	// Close file before rename (required for Windows)
	cleanupOnError = false
	if err = f.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Atomically move to final destination
	if err = os.Rename(filePartPath, destPath); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", filePartPath, destPath, err)
	}

	return nil
}
