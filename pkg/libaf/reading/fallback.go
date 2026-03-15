package reading

import (
	"context"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/pkg/libaf/ai"
)

// FallbackReader tries multiple Readers in order, returning the first
// result where at least one page produced non-empty text.
type FallbackReader struct {
	readers []Reader
}

// NewFallbackReader creates a Reader that tries each reader in order.
func NewFallbackReader(readers ...Reader) *FallbackReader {
	return &FallbackReader{readers: readers}
}

// Read tries each reader in order, returning the first result where any
// page produced non-empty text. If all readers fail or return empty results,
// returns the last error (or empty strings if no errors occurred).
func (f *FallbackReader) Read(ctx context.Context, pages []ai.BinaryContent, opts *ReadOptions) ([]string, error) {
	var lastErr error

	for _, r := range f.readers {
		results, err := r.Read(ctx, pages, opts)
		if err != nil {
			lastErr = err
			continue
		}

		// Check if any page produced non-empty text.
		for _, text := range results {
			if strings.TrimSpace(text) != "" {
				return results, nil
			}
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("all readers failed: %w", lastErr)
	}

	// All readers returned empty results without errors.
	return make([]string, len(pages)), nil
}

// Close closes all underlying readers, collecting any errors.
func (f *FallbackReader) Close() error {
	var errs []error
	for _, r := range f.readers {
		if err := r.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("closing readers: %v", errs)
	}
	return nil
}
