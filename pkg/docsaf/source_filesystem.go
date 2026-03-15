package docsaf

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
)

// FilesystemSourceConfig holds configuration for a FilesystemSource.
type FilesystemSourceConfig struct {
	// BaseDir is the base directory to traverse
	BaseDir string

	// BaseURL is the base URL for generating document links (optional).
	BaseURL string

	// IncludePatterns is a list of glob patterns to include.
	// Files matching any include pattern will be processed.
	// If empty, all files are included (subject to exclude patterns).
	// Supports ** wildcards for recursive matching.
	IncludePatterns []string

	// ExcludePatterns is a list of glob patterns to exclude.
	// Files matching any exclude pattern will be skipped.
	// Default excludes are: .git/**
	// Supports ** wildcards for recursive matching.
	ExcludePatterns []string
}

// FilesystemSource traverses a local filesystem directory and yields content items.
type FilesystemSource struct {
	config FilesystemSourceConfig
}

// NewFilesystemSource creates a new filesystem content source.
func NewFilesystemSource(config FilesystemSourceConfig) *FilesystemSource {
	// Add default excludes if not already present
	defaultExcludes := []string{".git/**"}
	config.ExcludePatterns = append(defaultExcludes, config.ExcludePatterns...)

	return &FilesystemSource{config: config}
}

// Type returns "filesystem" as the source type.
func (fs *FilesystemSource) Type() string {
	return "filesystem"
}

// BaseURL returns the base URL for this source.
func (fs *FilesystemSource) BaseURL() string {
	return fs.config.BaseURL
}

// Traverse walks the directory tree and yields content items for all matching files.
// It returns a channel of ContentItems and a channel for errors.
func (fs *FilesystemSource) Traverse(ctx context.Context) (<-chan ContentItem, <-chan error) {
	items := make(chan ContentItem)
	errs := make(chan error, 1)

	go func() {
		defer close(items)
		defer close(errs)

		err := filepath.Walk(fs.config.BaseDir, func(path string, info os.FileInfo, err error) error {
			// Check for cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err != nil {
				return err
			}

			// Get relative path for pattern matching
			relPath, err := filepath.Rel(fs.config.BaseDir, path)
			if err != nil {
				relPath = path
			}

			// Check exclude patterns first
			for _, pattern := range fs.config.ExcludePatterns {
				matched, err := doublestar.Match(pattern, relPath)
				if err != nil {
					log.Printf("Warning: Invalid exclude pattern %s: %v", pattern, err)
					continue
				}
				if matched {
					if info.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}

			// If include patterns are specified, check them
			if len(fs.config.IncludePatterns) > 0 {
				included := false
				for _, pattern := range fs.config.IncludePatterns {
					matched, err := doublestar.Match(pattern, relPath)
					if err != nil {
						log.Printf("Warning: Invalid include pattern %s: %v", pattern, err)
						continue
					}
					if matched {
						included = true
						break
					}
				}
				if !included {
					if info.IsDir() {
						// Check if this directory could contain matching files
						couldContainMatches := false
						for _, pattern := range fs.config.IncludePatterns {
							if strings.Contains(pattern, "**") {
								couldContainMatches = true
								break
							}
						}
						if !couldContainMatches {
							return filepath.SkipDir
						}
					}
					return nil
				}
			}

			if info.IsDir() {
				return nil
			}

			// Read file content
			content, err := os.ReadFile(path) //nolint:gosec // path comes from filepath.WalkDir within configured base directory
			if err != nil {
				log.Printf("Warning: Failed to read file %s: %v", path, err)
				return nil
			}

			// Detect content type
			contentType := DetectContentType(path, content)

			// Send content item
			select {
			case items <- ContentItem{
				Path:        relPath,
				Content:     content,
				ContentType: contentType,
				Metadata: map[string]any{
					"source_type": "filesystem",
					"file_size":   info.Size(),
					"mod_time":    info.ModTime(),
				},
			}:
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		})

		if err != nil {
			errs <- err
		}
	}()

	return items, errs
}
