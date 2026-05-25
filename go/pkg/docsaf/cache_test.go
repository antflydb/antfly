package docsaf

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestContentCache_BasicOperations(t *testing.T) {
	cache, err := NewContentCache(CacheConfig{
		Enabled:        true,
		TTL:            1 * time.Hour,
		MaxMemoryItems: 100,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	// Test Set and Get
	headers := http.Header{}
	headers.Set("Content-Type", "text/html")

	cache.Set("http://example.com/page1", []byte("content1"), headers, 200)

	entry := cache.Get("http://example.com/page1")
	if entry == nil {
		t.Fatal("Expected to find cached entry")
	}
	if string(entry.Body) != "content1" {
		t.Errorf("Body = %q, want %q", entry.Body, "content1")
	}
	if entry.ContentType != "text/html" {
		t.Errorf("ContentType = %q, want %q", entry.ContentType, "text/html")
	}
	if entry.StatusCode != 200 {
		t.Errorf("StatusCode = %d, want %d", entry.StatusCode, 200)
	}

	// Test cache miss
	entry = cache.Get("http://example.com/notfound")
	if entry != nil {
		t.Error("Expected cache miss for non-existent URL")
	}
}

func TestContentCache_HTTPHeaders(t *testing.T) {
	cache, err := NewContentCache(CacheConfig{
		Enabled:             true,
		TTL:                 1 * time.Hour,
		RespectCacheHeaders: true,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	// Test max-age directive
	headers := http.Header{}
	headers.Set("Cache-Control", "max-age=3600")

	cache.Set("http://example.com/page1", []byte("content"), headers, 200)

	entry := cache.Get("http://example.com/page1")
	if entry == nil {
		t.Fatal("Expected to find cached entry")
	}
	if entry.IsExpired() {
		t.Error("Entry should not be expired with max-age=3600")
	}

	// Test no-store directive (should expire immediately)
	headers2 := http.Header{}
	headers2.Set("Cache-Control", "no-store")

	cache.Set("http://example.com/page2", []byte("content"), headers2, 200)

	entry2 := cache.Get("http://example.com/page2")
	if entry2 != nil && !entry2.IsExpired() {
		t.Error("Entry with no-store should be expired immediately")
	}
}

func TestContentCache_ETagValidation(t *testing.T) {
	cache, err := NewContentCache(CacheConfig{
		Enabled:             true,
		TTL:                 1 * time.Millisecond, // Very short TTL
		RespectCacheHeaders: true,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	// Add entry with ETag
	headers := http.Header{}
	headers.Set("ETag", `"abc123"`)
	headers.Set("Content-Type", "text/html")

	cache.Set("http://example.com/page", []byte("content"), headers, 200)

	// Wait for expiry
	time.Sleep(5 * time.Millisecond)

	entry := cache.Get("http://example.com/page")
	if entry == nil {
		t.Fatal("Expected to find stale entry with ETag")
	}
	if !entry.IsStale() {
		t.Error("Entry should be stale")
	}
	if !entry.CanRevalidate() {
		t.Error("Entry should be revalidatable with ETag")
	}
	if entry.ETag != `"abc123"` {
		t.Errorf("ETag = %q, want %q", entry.ETag, `"abc123"`)
	}
}

func TestContentCache_LastModifiedValidation(t *testing.T) {
	cache, err := NewContentCache(CacheConfig{
		Enabled:             true,
		TTL:                 1 * time.Millisecond,
		RespectCacheHeaders: true,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	// Add entry with Last-Modified
	headers := http.Header{}
	headers.Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")

	cache.Set("http://example.com/page", []byte("content"), headers, 200)

	time.Sleep(5 * time.Millisecond)

	entry := cache.Get("http://example.com/page")
	if entry == nil {
		t.Fatal("Expected to find stale entry")
	}
	if !entry.CanRevalidate() {
		t.Error("Entry should be revalidatable with Last-Modified")
	}
}

func TestContentCache_Deduplication(t *testing.T) {
	cache, err := NewContentCache(CacheConfig{
		Enabled:             true,
		TTL:                 1 * time.Hour,
		EnableDeduplication: true,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	content := []byte("identical content for both pages")
	headers := http.Header{}

	// Add same content under two different URLs
	cache.Set("http://example.com/page1", content, headers, 200)
	cache.Set("http://example.com/page2", content, headers, 200)

	// Both should have the same content hash
	entry1 := cache.Get("http://example.com/page1")
	entry2 := cache.Get("http://example.com/page2")

	if entry1 == nil || entry2 == nil {
		t.Fatal("Expected to find both entries")
	}

	if entry1.ContentHash != entry2.ContentHash {
		t.Error("Identical content should have same hash")
	}

	// Check stats - should only have one unique content
	stats := cache.Stats()
	if stats.UniqueContents != 1 {
		t.Errorf("UniqueContents = %d, want 1", stats.UniqueContents)
	}
	if stats.MemoryEntries != 2 {
		t.Errorf("MemoryEntries = %d, want 2", stats.MemoryEntries)
	}
}

func TestContentCache_IsDuplicate(t *testing.T) {
	cache, err := NewContentCache(CacheConfig{
		Enabled:             true,
		TTL:                 1 * time.Hour,
		EnableDeduplication: true,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	content := []byte("test content")
	headers := http.Header{}

	// First check - should not be duplicate
	isDup, _ := cache.IsDuplicate(content)
	if isDup {
		t.Error("Content should not be duplicate before adding")
	}

	// Add content
	cache.Set("http://example.com/page1", content, headers, 200)

	// Now should be duplicate
	isDup, hash := cache.IsDuplicate(content)
	if !isDup {
		t.Error("Content should be duplicate after adding")
	}
	if hash == "" {
		t.Error("Hash should not be empty")
	}
}

func TestContentCache_LRUEviction(t *testing.T) {
	cache, err := NewContentCache(CacheConfig{
		Enabled:        true,
		TTL:            1 * time.Hour,
		MaxMemoryItems: 3,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	headers := http.Header{}

	// Add 4 items (max is 3)
	cache.Set("http://example.com/page1", []byte("1"), headers, 200)
	cache.Set("http://example.com/page2", []byte("2"), headers, 200)
	cache.Set("http://example.com/page3", []byte("3"), headers, 200)
	cache.Set("http://example.com/page4", []byte("4"), headers, 200)

	// page1 should have been evicted
	if cache.Get("http://example.com/page1") != nil {
		t.Error("page1 should have been evicted")
	}

	// Others should still exist
	for _, url := range []string{
		"http://example.com/page2",
		"http://example.com/page3",
		"http://example.com/page4",
	} {
		if cache.Get(url) == nil {
			t.Errorf("%s should still be in cache", url)
		}
	}

	stats := cache.Stats()
	if stats.MemoryEntries != 3 {
		t.Errorf("MemoryEntries = %d, want 3", stats.MemoryEntries)
	}
}

func TestContentCache_DiskPersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cache-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cacheDir := filepath.Join(tempDir, "cache")

	// Create cache with disk storage
	cache1, err := NewContentCache(CacheConfig{
		Enabled:        true,
		Dir:            cacheDir,
		TTL:            1 * time.Hour,
		MaxMemoryItems: 100,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	headers := http.Header{}
	headers.Set("Content-Type", "text/html")
	headers.Set("ETag", `"test123"`)

	cache1.Set("http://example.com/page", []byte("cached content"), headers, 200)

	// Verify files were created
	files, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("Failed to read cache dir: %v", err)
	}
	if len(files) == 0 {
		t.Error("Expected cache files to be created")
	}

	// Create new cache instance to test loading
	cache2, err := NewContentCache(CacheConfig{
		Enabled:        true,
		Dir:            cacheDir,
		TTL:            1 * time.Hour,
		MaxMemoryItems: 100,
	})
	if err != nil {
		t.Fatalf("NewContentCache (reload) failed: %v", err)
	}

	// Entry should be loaded from disk
	entry := cache2.Get("http://example.com/page")
	if entry == nil {
		t.Fatal("Expected to find entry loaded from disk")
	}
	if entry.ETag != `"test123"` {
		t.Errorf("ETag = %q, want %q", entry.ETag, `"test123"`)
	}
}

func TestContentCache_Clear(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cache-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cacheDir := filepath.Join(tempDir, "cache")

	cache, err := NewContentCache(CacheConfig{
		Enabled: true,
		Dir:     cacheDir,
		TTL:     1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	headers := http.Header{}
	cache.Set("http://example.com/page1", []byte("1"), headers, 200)
	cache.Set("http://example.com/page2", []byte("2"), headers, 200)

	// Clear cache
	if err := cache.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	// Entries should be gone
	if cache.Get("http://example.com/page1") != nil {
		t.Error("Expected empty cache after clear")
	}

	stats := cache.Stats()
	if stats.MemoryEntries != 0 {
		t.Errorf("MemoryEntries = %d, want 0", stats.MemoryEntries)
	}
}

func TestContentCache_AddConditionalHeaders(t *testing.T) {
	cache, err := NewContentCache(CacheConfig{
		Enabled:             true,
		TTL:                 1 * time.Millisecond,
		RespectCacheHeaders: true,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	// Add entry with validators
	headers := http.Header{}
	headers.Set("ETag", `"abc123"`)
	headers.Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")

	cache.Set("http://example.com/page", []byte("content"), headers, 200)

	time.Sleep(5 * time.Millisecond)

	// Create request and add conditional headers
	req, _ := http.NewRequest("GET", "http://example.com/page", nil)
	added := cache.AddConditionalHeaders(req)

	if !added {
		t.Error("Expected conditional headers to be added")
	}
	if req.Header.Get("If-None-Match") != `"abc123"` {
		t.Errorf("If-None-Match = %q, want %q", req.Header.Get("If-None-Match"), `"abc123"`)
	}
	if req.Header.Get("If-Modified-Since") != "Wed, 21 Oct 2015 07:28:00 GMT" {
		t.Errorf("If-Modified-Since = %q, want %q", req.Header.Get("If-Modified-Since"), "Wed, 21 Oct 2015 07:28:00 GMT")
	}
}

func TestContentCache_HandleNotModified(t *testing.T) {
	cache, err := NewContentCache(CacheConfig{
		Enabled:             true,
		TTL:                 1 * time.Millisecond,
		RespectCacheHeaders: true,
	})
	if err != nil {
		t.Fatalf("NewContentCache failed: %v", err)
	}

	// Add entry
	headers := http.Header{}
	headers.Set("ETag", `"v1"`)

	cache.Set("http://example.com/page", []byte("content"), headers, 200)

	time.Sleep(5 * time.Millisecond)

	// Simulate 304 response with new headers
	newHeaders := http.Header{}
	newHeaders.Set("ETag", `"v2"`)
	newHeaders.Set("Cache-Control", "max-age=3600")

	entry := cache.HandleNotModified("http://example.com/page", newHeaders)
	if entry == nil {
		t.Fatal("Expected entry to be updated")
	}

	if entry.ETag != `"v2"` {
		t.Errorf("ETag = %q, want %q", entry.ETag, `"v2"`)
	}
	if entry.IsExpired() {
		t.Error("Entry should not be expired after update")
	}
}

func TestCacheEntry_IsExpired(t *testing.T) {
	tests := []struct {
		name     string
		entry    CacheEntry
		expected bool
	}{
		{
			name:     "zero expires never expires",
			entry:    CacheEntry{Expires: time.Time{}},
			expected: false,
		},
		{
			name:     "future expires not expired",
			entry:    CacheEntry{Expires: time.Now().Add(1 * time.Hour)},
			expected: false,
		},
		{
			name:     "past expires is expired",
			entry:    CacheEntry{Expires: time.Now().Add(-1 * time.Hour)},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.entry.IsExpired() != tt.expected {
				t.Errorf("IsExpired() = %v, want %v", tt.entry.IsExpired(), tt.expected)
			}
		})
	}
}

func TestContentHash(t *testing.T) {
	cache, _ := NewContentCache(CacheConfig{Enabled: true})

	content1 := []byte("hello world")
	content2 := []byte("hello world")
	content3 := []byte("different content")

	hash1 := cache.ContentHash(content1)
	hash2 := cache.ContentHash(content2)
	hash3 := cache.ContentHash(content3)

	if hash1 != hash2 {
		t.Error("Same content should produce same hash")
	}
	if hash1 == hash3 {
		t.Error("Different content should produce different hash")
	}
	if len(hash1) != 64 { // SHA-256 produces 64 hex characters
		t.Errorf("Hash length = %d, want 64", len(hash1))
	}
}
