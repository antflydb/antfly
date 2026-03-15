package docsaf

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/antflydb/antfly/pkg/libaf/s3"
	"github.com/bmatcuk/doublestar/v4"
	"github.com/minio/minio-go/v7"
)

// S3SourceConfig holds configuration for an S3Source.
type S3SourceConfig struct {
	// Credentials holds S3/MinIO connection credentials.
	// Supports keystore syntax and environment variable fallbacks.
	Credentials s3.Credentials

	// Bucket is the S3 bucket name (required).
	Bucket string

	// Prefix is an optional key prefix to filter objects.
	// Only objects with this prefix will be listed.
	// Example: "docs/" to only process objects in the docs/ folder.
	Prefix string

	// BaseURL is the base URL for generating document links (optional).
	// If empty, URLs will use the s3:// scheme.
	BaseURL string

	// IncludePatterns is a list of glob patterns to include.
	// Objects matching any include pattern will be processed.
	// If empty, all objects are included (subject to exclude patterns).
	// Supports ** wildcards for recursive matching.
	// Patterns are matched against the object key (with prefix stripped if configured).
	IncludePatterns []string

	// ExcludePatterns is a list of glob patterns to exclude.
	// Objects matching any exclude pattern will be skipped.
	// Supports ** wildcards for recursive matching.
	// Patterns are matched against the object key (with prefix stripped if configured).
	ExcludePatterns []string

	// Concurrency controls how many S3 GetObject requests run in parallel.
	// Default: 5
	Concurrency int
}

// S3Source traverses objects in an S3-compatible bucket and yields content items.
type S3Source struct {
	config    S3SourceConfig
	client    *minio.Client
	semaphore chan struct{} // Concurrency control
}

// NewS3Source creates a new S3 content source.
func NewS3Source(config S3SourceConfig) (*S3Source, error) {
	if config.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}

	// Create MinIO client from credentials
	client, err := config.Credentials.NewMinioClient()
	if err != nil {
		return nil, fmt.Errorf("creating S3 client: %w", err)
	}

	// Verify bucket exists
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, config.Bucket)
	if err != nil {
		return nil, fmt.Errorf("checking if bucket %s exists: %w", config.Bucket, err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket %s does not exist", config.Bucket)
	}

	// Set default concurrency
	if config.Concurrency <= 0 {
		config.Concurrency = 5
	}

	// Normalize prefix (ensure trailing slash if non-empty)
	if config.Prefix != "" && !strings.HasSuffix(config.Prefix, "/") {
		config.Prefix = config.Prefix + "/"
	}

	return &S3Source{
		config:    config,
		client:    client,
		semaphore: make(chan struct{}, config.Concurrency),
	}, nil
}

// Type returns "s3" as the source type.
func (s *S3Source) Type() string {
	return "s3"
}

// BaseURL returns the base URL for this source.
// If not configured, returns an s3:// URL.
func (s *S3Source) BaseURL() string {
	if s.config.BaseURL != "" {
		return s.config.BaseURL
	}
	return fmt.Sprintf("s3://%s/%s", s.config.Bucket, s.config.Prefix)
}

// Traverse lists objects in the S3 bucket and yields content items in
// lexicographic key order. S3 ListObjects returns keys sorted; we download
// concurrently but emit items in listing order via ordered result channels.
func (s *S3Source) Traverse(ctx context.Context) (<-chan ContentItem, <-chan error) {
	items := make(chan ContentItem)
	errs := make(chan error, 1)

	go func() {
		defer close(items)
		defer close(errs)

		// List objects with the configured prefix.
		// S3 returns objects in lexicographic key order.
		objectCh := s.client.ListObjects(ctx, s.config.Bucket, minio.ListObjectsOptions{
			Prefix:    s.config.Prefix,
			Recursive: true,
		})

		// Collect eligible objects into a batch, download concurrently,
		// then emit in listing order. We process in batches to bound
		// memory while preserving ordering.
		const batchSize = 100
		type objectEntry struct {
			key          string
			relPath      string
			size         int64
			lastModified any
		}
		var batch []objectEntry

		emitBatch := func(batch []objectEntry) error {
			if len(batch) == 0 {
				return nil
			}

			// Create ordered result channels
			slots := make([]chan ContentItem, len(batch))
			var wg sync.WaitGroup

			for i, entry := range batch {
				slots[i] = make(chan ContentItem, 1)

				wg.Add(1)
				s.semaphore <- struct{}{} // Acquire slot
				go func(key, relPath string, size int64, lastModified any, slot chan<- ContentItem) {
					defer wg.Done()
					defer func() { <-s.semaphore }()
					defer close(slot)

					obj, err := s.client.GetObject(ctx, s.config.Bucket, key, minio.GetObjectOptions{})
					if err != nil {
						log.Printf("Warning: Failed to get object %s: %v", key, err)
						return
					}
					defer obj.Close() //nolint:errcheck // best-effort close in deferred cleanup

					content, err := io.ReadAll(obj)
					if err != nil {
						log.Printf("Warning: Failed to read object %s: %v", key, err)
						return
					}

					contentType := DetectContentType(relPath, content)
					sourceURL := fmt.Sprintf("s3://%s/%s", s.config.Bucket, key)

					select {
					case slot <- ContentItem{
						Path:        relPath,
						SourceURL:   sourceURL,
						Content:     content,
						ContentType: contentType,
						Metadata: map[string]any{
							"source_type":   "s3",
							"bucket":        s.config.Bucket,
							"key":           key,
							"size":          size,
							"last_modified": lastModified,
						},
					}:
					case <-ctx.Done():
					}
				}(entry.key, entry.relPath, entry.size, entry.lastModified, slots[i])
			}

			// Emit in listing order
			for _, slot := range slots {
				if item, ok := <-slot; ok {
					select {
					case items <- item:
					case <-ctx.Done():
						wg.Wait()
						return ctx.Err()
					}
				}
			}

			wg.Wait()
			return nil
		}

		for object := range objectCh {
			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			default:
			}

			if object.Err != nil {
				log.Printf("Warning: Error listing objects: %v", object.Err)
				continue
			}

			if strings.HasSuffix(object.Key, "/") {
				continue
			}

			relPath := object.Key
			if s.config.Prefix != "" {
				relPath = strings.TrimPrefix(object.Key, s.config.Prefix)
			}

			if s.shouldExclude(relPath) {
				continue
			}
			if !s.shouldInclude(relPath) {
				continue
			}

			batch = append(batch, objectEntry{
				key:          object.Key,
				relPath:      relPath,
				size:         object.Size,
				lastModified: object.LastModified,
			})

			if len(batch) >= batchSize {
				if err := emitBatch(batch); err != nil {
					errs <- err
					return
				}
				batch = batch[:0]
			}
		}

		// Emit remaining objects
		if err := emitBatch(batch); err != nil {
			errs <- err
		}
	}()

	return items, errs
}

// shouldExclude checks if a path matches any exclude pattern.
func (s *S3Source) shouldExclude(path string) bool {
	for _, pattern := range s.config.ExcludePatterns {
		matched, err := doublestar.Match(pattern, path)
		if err != nil {
			log.Printf("Warning: Invalid exclude pattern %s: %v", pattern, err)
			continue
		}
		if matched {
			return true
		}
	}
	return false
}

// shouldInclude checks if a path matches include patterns.
// If no include patterns are configured, returns true.
func (s *S3Source) shouldInclude(path string) bool {
	// If no include patterns, include everything (subject to excludes)
	if len(s.config.IncludePatterns) == 0 {
		return true
	}

	// Check if path matches any include pattern
	for _, pattern := range s.config.IncludePatterns {
		matched, err := doublestar.Match(pattern, path)
		if err != nil {
			log.Printf("Warning: Invalid include pattern %s: %v", pattern, err)
			continue
		}
		if matched {
			return true
		}
	}

	return false
}
