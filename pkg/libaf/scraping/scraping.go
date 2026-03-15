//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
package scraping

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/antflydb/antfly/pkg/libaf/s3"
	"github.com/minio/minio-go/v7"
)

// HTTPError represents an HTTP response with a non-200 status code.
// Callers can inspect StatusCode to classify the error (e.g., 404 vs 503).
type HTTPError struct {
	StatusCode int
	Status     string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP error: %d %s", e.StatusCode, e.Status)
}

// DownloadContent downloads content from a URL and returns the content type and data.
// Supports data:, http://, https://, file://, and s3:// URLs.
func DownloadContent(
	ctx context.Context,
	uri string,
	securityConfig *ContentSecurityConfig,
	s3Creds *s3.Credentials,
) (string, []byte, error) {
	// Parse data URIs
	if strings.HasPrefix(uri, "data:") {
		contentType, data, err := ParseDataURI(uri)
		if err != nil {
			return "", nil, err
		}
		return contentType, data, nil
	}

	parsedURL, err := url.Parse(uri)
	if err != nil {
		return "", nil, fmt.Errorf("parsing URL: %w", err)
	}

	var data []byte
	var mimeType string

	switch parsedURL.Scheme {
	case "http", "https":
		// Validate URL against security settings
		if err := validateURLSecurity(uri, securityConfig); err != nil {
			return "", nil, fmt.Errorf("security validation failed: %w", err)
		}
		mimeType, data, err = downloadHTTPWithMime(ctx, uri, securityConfig)
	case "file":
		filePath := strings.TrimPrefix(uri, "file://")
		if err := validatePathSecurity(filePath, securityConfig); err != nil {
			return "", nil, fmt.Errorf("security validation failed: %w", err)
		}
		mimeType, data, err = downloadFileWithMime(filePath)
	case "s3":
		// S3 path format: /bucket/key - validate without leading slash for prefix matching
		s3Path := strings.TrimPrefix(parsedURL.Path, "/")
		if err := validatePathSecurity(s3Path, securityConfig); err != nil {
			return "", nil, fmt.Errorf("security validation failed: %w", err)
		}
		creds := &s3.Credentials{}
		if s3Creds != nil {
			creds = s3Creds
		}
		creds.Endpoint = parsedURL.Host
		mimeType, data, err = downloadS3WithMime(ctx, parsedURL.Path, *creds)
	default:
		return "", nil, fmt.Errorf("unsupported URL scheme: %s", parsedURL.Scheme)
	}

	if err != nil {
		return "", nil, err
	}

	return mimeType, data, nil
}

// ParseDataURI returns the content type and bytes of the data uri.
func ParseDataURI(uri string) (string, []byte, error) {
	if contents, isData := strings.CutPrefix(uri, "data:"); isData {
		prefix, data, found := strings.Cut(contents, ",")
		if !found {
			return "", nil, errors.New("failed to parse data URI: missing comma")
		}

		var dataBytes []byte
		var contentType string
		if p, isBase64 := strings.CutSuffix(prefix, ";base64"); isBase64 {
			contentType = p
			var err error
			dataBytes, err = base64.StdEncoding.DecodeString(data)
			if err != nil {
				return "", nil, fmt.Errorf("failed to decode base64 data: %w", err)
			}
		} else {
			contentType = prefix
			dataBytes = []byte(data)
		}

		return contentType, dataBytes, nil
	}

	return "", nil, errors.New("could not parse uri: missing file data")
}

// downloadHTTPWithMime downloads content via HTTP and returns full MIME type
func downloadHTTPWithMime(
	ctx context.Context,
	uri string,
	securityConfig *ContentSecurityConfig,
) (string, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return "", nil, fmt.Errorf("creating request: %w", err)
	}

	// Set User-Agent — many servers (e.g., Wikipedia) reject requests without one.
	userAgent := "AntflyDB/1.0"
	if securityConfig != nil && securityConfig.UserAgent != "" {
		userAgent = securityConfig.UserAgent
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("failed to fetch content: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", nil, &HTTPError{StatusCode: resp.StatusCode, Status: resp.Status}
	}

	// Read with size limit if configured
	var reader io.Reader = resp.Body
	if securityConfig != nil && securityConfig.MaxDownloadSizeBytes > 0 {
		reader = io.LimitReader(resp.Body, securityConfig.MaxDownloadSizeBytes)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read content: %w", err)
	}

	mimeType := resp.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	// Strip charset and other parameters
	if idx := strings.Index(mimeType, ";"); idx > 0 {
		mimeType = strings.TrimSpace(mimeType[:idx])
	}

	return mimeType, data, nil
}

// downloadFileWithMime reads a local file and guesses MIME type
func downloadFileWithMime(path string) (string, []byte, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is caller-controlled, not user input
	if err != nil {
		return "", nil, fmt.Errorf("reading file: %w", err)
	}

	// Guess MIME type from extension
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(path), "."))
	mimeType := guessMimeTypeFromExt(ext)

	return mimeType, data, nil
}

// downloadS3WithMime downloads from S3 and returns full MIME type
func downloadS3WithMime(ctx context.Context, path string, creds s3.Credentials) (string, []byte, error) {
	// S3 URL format: s3://endpoint/bucket/key
	path = strings.TrimPrefix(path, "/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		return "", nil, errors.New("invalid S3 URL format, expected s3://endpoint/bucket/key")
	}
	bucket := parts[0]
	key := strings.Join(parts[1:], "/")

	// Create MinIO client
	client, err := creds.NewMinioClient()
	if err != nil {
		return "", nil, fmt.Errorf("creating S3 client: %w", err)
	}

	// Get object
	object, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return "", nil, fmt.Errorf("getting S3 object: %w", err)
	}
	defer func() { _ = object.Close() }()

	// Read object data
	data, err := io.ReadAll(object)
	if err != nil {
		return "", nil, fmt.Errorf("reading S3 object: %w", err)
	}

	// Get object info for content type
	stat, err := object.Stat()
	if err != nil {
		return "", nil, fmt.Errorf("getting S3 object info: %w", err)
	}

	// Use content type from S3 metadata
	contentType := stat.ContentType
	if contentType == "" {
		// Fallback to extension-based detection
		ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(key), "."))
		contentType = guessMimeTypeFromExt(ext)
	}

	return contentType, data, nil
}

// guessMimeTypeFromExt returns MIME type based on file extension
func guessMimeTypeFromExt(ext string) string {
	switch ext {
	case "html", "htm":
		return "text/html"
	case "pdf":
		return "application/pdf"
	case "png":
		return "image/png"
	case "jpg", "jpeg":
		return "image/jpeg"
	case "gif":
		return "image/gif"
	case "webp":
		return "image/webp"
	case "svg":
		return "image/svg+xml"
	case "txt":
		return "text/plain"
	case "md", "markdown":
		return "text/markdown"
	default:
		return "application/octet-stream"
	}
}

// validateURLSecurity validates URL against security configuration
func validateURLSecurity(uri string, config *ContentSecurityConfig) error {
	if config == nil {
		return nil
	}

	parsedURL, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	// Only validate http/https URLs
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return nil
	}

	hostname := parsedURL.Hostname()

	// Check allowlist if configured
	if len(config.AllowedHosts) > 0 {
		allowed := slices.Contains(config.AllowedHosts, hostname)
		if !allowed {
			return fmt.Errorf("host %s not in allowlist", hostname)
		}
	}

	// Check private IP blocking
	if config.BlockPrivateIps {
		if isPrivateIP(hostname) {
			return fmt.Errorf("private IP addresses are blocked: %s", hostname)
		}
	}

	return nil
}

// validatePathSecurity validates file and S3 paths against allowed path prefixes
func validatePathSecurity(path string, config *ContentSecurityConfig) error {
	if config == nil || len(config.AllowedPaths) == 0 {
		return nil
	}

	// Clean the path to prevent traversal attacks
	cleanPath := filepath.Clean(path)

	// Check if path starts with any allowed prefix
	for _, allowed := range config.AllowedPaths {
		cleanAllowed := filepath.Clean(allowed)
		if strings.HasPrefix(cleanPath, cleanAllowed) {
			return nil
		}
	}

	return fmt.Errorf("path %s not in allowed paths", path)
}

// isPrivateIP checks if a hostname resolves to a private IP address
func isPrivateIP(hostname string) bool {
	// Check if it's a literal IP address
	ip := net.ParseIP(hostname)
	if ip != nil {
		return isPrivateIPAddr(ip)
	}

	// Try to resolve hostname
	ips, err := net.LookupIP(hostname)
	if err != nil {
		// If we can't resolve, be conservative and block it
		return true
	}

	// Check if any resolved IP is private
	return slices.ContainsFunc(ips, isPrivateIPAddr)
}

// isPrivateIPAddr checks if an IP address is private
func isPrivateIPAddr(ip net.IP) bool {
	// Check for loopback
	if ip.IsLoopback() {
		return true
	}

	// Check for link-local
	if ip.IsLinkLocalUnicast() {
		return true
	}

	// Check private ranges
	privateRanges := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"169.254.0.0/16", // link-local
		"127.0.0.0/8",    // loopback
	}

	for _, cidr := range privateRanges {
		_, subnet, _ := net.ParseCIDR(cidr)
		if subnet != nil && subnet.Contains(ip) {
			return true
		}
	}

	return false
}
