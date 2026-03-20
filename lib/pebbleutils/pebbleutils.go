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

package pebbleutils

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/lib/logger"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// NewPebbleOpts returns Pebble options with NoopLoggerAndTracer to silence internal logging.
// Use this for production code or tests where you want to suppress Pebble's internal logs.
func NewPebbleOpts() *pebble.Options {
	return &pebble.Options{
		Logger:          &logger.NoopLoggerAndTracer{},
		LoggerAndTracer: &logger.NoopLoggerAndTracer{},
	}
}

// NewMemPebbleOpts returns Pebble options configured for in-memory testing.
// This includes NoopLoggerAndTracer to silence logs and vfs.NewMem() for fast in-memory storage.
func NewMemPebbleOpts() *pebble.Options {
	return &pebble.Options{
		Logger:          &logger.NoopLoggerAndTracer{},
		LoggerAndTracer: &logger.NoopLoggerAndTracer{},
		FS:              vfs.NewMem(),
	}
}

// IsPebbleEmpty checks if a Pebble database is empty using an iterator.
// It returns true if the database is empty, false otherwise, or an error
// if the check could not be performed.
func IsPebbleEmpty(db *pebble.DB, skipPrefix []byte) (bool, error) {
	// Create an iterator with default options (nil).
	// NewIter returns an iterator and an error.[1, 6]
	iter, err := db.NewIter(&pebble.IterOptions{
		SkipPoint: func(userKey []byte) bool {
			return bytes.HasPrefix(userKey, skipPrefix)
		},
	})
	if err != nil {
		return false, fmt.Errorf("failed to create iterator: %w", err)
	}
	// Ensure the iterator is closed to release resources.[6]
	defer func() { _ = iter.Close() }()

	// Attempt to position the iterator at the first key.[10]
	// If the database is empty, First() will result in the iterator
	// being invalid.[10]
	iter.First()

	// Check if the iterator is pointing to a valid key after First().[10]
	// If not valid, the database contains no keys.
	isEmpty := !iter.Valid()

	// Check for any errors encountered during iteration, such as during First().[10]
	if err := iter.Error(); err != nil {
		// Return the error, as the state couldn't be reliably determined.
		return false, fmt.Errorf("iterator error after First(): %w", err)
	}

	// Return the determined emptiness state.
	return isEmpty, nil
}

// DefaultCacheSizeMB is the default shared block cache size in megabytes.
const DefaultCacheSizeMB int64 = 256

// Cache wraps a shared pebble.Cache that can be used across multiple pebble.DB instances.
// A shared cache lets Pebble's LRU globally evict cold blocks from idle DBs in favor of
// hot blocks from active ones, reducing total memory usage.
type Cache struct {
	cache *pebble.Cache
}

// NewCache creates a new shared Pebble block cache with the given size in bytes.
func NewCache(sizeBytes int64) *Cache {
	return &Cache{cache: pebble.NewCache(sizeBytes)}
}

// Get returns the underlying pebble.Cache. It is nil-safe: if c is nil, returns nil.
func (c *Cache) Get() *pebble.Cache {
	if c == nil {
		return nil
	}
	return c.cache
}

// Close releases the cache's initial reference. The cache is freed only when all
// pebble.DB instances that reference it have also been closed.
func (c *Cache) Close() {
	if c != nil && c.cache != nil {
		c.cache.Unref()
	}
}

// Apply sets opts.Cache to the shared cache if c is non-nil, otherwise creates
// a dedicated cache of fallbackBytes size.
func (c *Cache) Apply(opts *pebble.Options, fallbackBytes int64) {
	if c != nil && c.cache != nil {
		opts.Cache = c.cache
	} else {
		opts.Cache = pebble.NewCache(fallbackBytes)
	}
}

// RecoverPebbleClosed recovers from pebble.ErrClosed panics during shutdown.
// Pebble panics (rather than returning an error) when operating on a closed DB.
// This helper converts the panic to an error for graceful shutdown handling.
// Usage: defer pebbleutils.RecoverPebbleClosed(&err)
func RecoverPebbleClosed(err *error) {
	if r := recover(); r != nil {
		// Check if it's an error type (Pebble panics with ErrClosed error)
		if e, ok := r.(error); ok {
			if errors.Is(e, pebble.ErrClosed) {
				*err = e
				return
			}
			// Some Pebble call paths panic with a raw "pebble: closed" error that does
			// not satisfy errors.Is against pebble.ErrClosed after crossing package
			// boundaries. Treat that as the same shutdown condition.
			if strings.Contains(e.Error(), "pebble: closed") {
				*err = pebble.ErrClosed
				return
			}
			// Also handle "closed LogWriter" panics from Pebble's WAL
			if strings.Contains(e.Error(), "pebble/record: closed LogWriter") {
				*err = pebble.ErrClosed
				return
			}
		}
		// Also check for string (in case of other panic sources)
		if msg, ok := r.(string); ok && strings.Contains(msg, "closed") {
			*err = pebble.ErrClosed
			return
		}
		// Re-panic for other panics
		panic(r)
	}
}
