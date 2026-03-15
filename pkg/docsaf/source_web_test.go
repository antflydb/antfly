package docsaf

import (
	"testing"
	"time"
)

func TestURLNormalizer(t *testing.T) {
	n := newURLNormalizer()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "lowercase scheme",
			input:    "HTTP://Example.com/path",
			expected: "http://example.com/path",
		},
		{
			name:     "lowercase host",
			input:    "https://EXAMPLE.COM/Path",
			expected: "https://example.com/Path",
		},
		{
			name:     "remove default http port",
			input:    "http://example.com:80/path",
			expected: "http://example.com/path",
		},
		{
			name:     "remove default https port",
			input:    "https://example.com:443/path",
			expected: "https://example.com/path",
		},
		{
			name:     "keep non-default port",
			input:    "https://example.com:8080/path",
			expected: "https://example.com:8080/path",
		},
		{
			name:     "remove trailing slash",
			input:    "https://example.com/path/",
			expected: "https://example.com/path",
		},
		{
			name:     "keep root path",
			input:    "https://example.com/",
			expected: "https://example.com/",
		},
		{
			name:     "add root path when empty",
			input:    "https://example.com",
			expected: "https://example.com/",
		},
		{
			name:     "remove fragment",
			input:    "https://example.com/path#section",
			expected: "https://example.com/path",
		},
		{
			name:     "preserve query string",
			input:    "https://example.com/path?foo=bar",
			expected: "https://example.com/path?foo=bar",
		},
		{
			name:     "complex URL normalization",
			input:    "HTTPS://Example.COM:443/Path/To/Page/#section",
			expected: "https://example.com/Path/To/Page",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := n.Normalize(tt.input)
			if err != nil {
				t.Fatalf("Normalize(%q) error: %v", tt.input, err)
			}
			if got != tt.expected {
				t.Errorf("Normalize(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestResponseCache(t *testing.T) {
	cache := newResponseCache(100*time.Millisecond, 3)

	// Test set and get
	cache.Set("url1", []byte("body1"), "text/html", 200)
	resp, ok := cache.Get("url1")
	if !ok {
		t.Fatal("Expected to find url1 in cache")
	}
	if string(resp.body) != "body1" {
		t.Errorf("body = %q, want %q", resp.body, "body1")
	}
	if resp.contentType != "text/html" {
		t.Errorf("contentType = %q, want %q", resp.contentType, "text/html")
	}
	if resp.statusCode != 200 {
		t.Errorf("statusCode = %d, want %d", resp.statusCode, 200)
	}

	// Test cache miss
	_, ok = cache.Get("url_not_found")
	if ok {
		t.Error("Expected cache miss for url_not_found")
	}

	// Test TTL expiration
	time.Sleep(150 * time.Millisecond)
	_, ok = cache.Get("url1")
	if ok {
		t.Error("Expected cache miss after TTL expiration")
	}

	// Test max items eviction
	cache.Set("a", []byte("1"), "text/plain", 200)
	cache.Set("b", []byte("2"), "text/plain", 200)
	cache.Set("c", []byte("3"), "text/plain", 200)
	cache.Set("d", []byte("4"), "text/plain", 200) // Should evict oldest

	// At most 3 items should exist
	count := 0
	for _, key := range []string{"a", "b", "c", "d"} {
		if _, ok := cache.Get(key); ok {
			count++
		}
	}
	if count > 3 {
		t.Errorf("Cache has %d items, expected max 3", count)
	}
}

func TestNewWebSource(t *testing.T) {
	// Test basic creation
	ws, err := NewWebSource(WebSourceConfig{
		StartURL: "https://example.com",
	})
	if err != nil {
		t.Fatalf("NewWebSource failed: %v", err)
	}

	if ws.config.BaseURL != "https://example.com" {
		t.Errorf("BaseURL = %q, want %q", ws.config.BaseURL, "https://example.com")
	}
	if len(ws.config.AllowedDomains) != 1 || ws.config.AllowedDomains[0] != "example.com" {
		t.Errorf("AllowedDomains = %v, want [example.com]", ws.config.AllowedDomains)
	}
	if ws.normalizer == nil {
		t.Error("Expected normalizer to be initialized")
	}
	if ws.httpClient == nil {
		t.Error("Expected httpClient to be initialized")
	}

	// Test with caching enabled
	wsWithCache, err := NewWebSource(WebSourceConfig{
		StartURL: "https://example.com",
		CacheTTL: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewWebSource with cache failed: %v", err)
	}
	if wsWithCache.cache == nil {
		t.Error("Expected cache to be initialized when CacheTTL > 0")
	}

	// Test missing StartURL
	_, err = NewWebSource(WebSourceConfig{})
	if err == nil {
		t.Error("Expected error for missing StartURL")
	}
}

func TestWebSource_normalizeURL(t *testing.T) {
	ws, _ := NewWebSource(WebSourceConfig{
		StartURL: "https://example.com",
	})

	// Test with normalizer
	normalized := ws.normalizeURL("HTTPS://Example.com:443/path/")
	if normalized != "https://example.com/path" {
		t.Errorf("normalizeURL = %q, want %q", normalized, "https://example.com/path")
	}

	// Test with normalizer disabled
	wsNoNorm, _ := NewWebSource(WebSourceConfig{
		StartURL:      "https://example.com",
		NormalizeURLs: false,
	})
	// Note: Go can't distinguish explicit false from zero value, so normalizer is still enabled
	// This tests that the method handles nil normalizer gracefully
	wsNoNorm.normalizer = nil
	result := wsNoNorm.normalizeURL("HTTPS://Example.com/path/")
	if result != "HTTPS://Example.com/path/" {
		t.Errorf("normalizeURL without normalizer = %q, want original URL", result)
	}
}

func TestWebSource_shouldIncludePath(t *testing.T) {
	ws, _ := NewWebSource(WebSourceConfig{
		StartURL:        "https://example.com",
		IncludePatterns: []string{"/docs/**", "/guides/*"},
		ExcludePatterns: []string{"/docs/internal/**", "/**/*.css"},
	})

	tests := []struct {
		path   string
		expect bool
	}{
		{"/docs/getting-started", true},
		{"/docs/api/reference", true},
		{"/guides/intro", true},
		{"/docs/internal/secret", false},
		{"/docs/style.css", false},
		{"/blog/post", false},
		{"/", false},
	}

	for _, tt := range tests {
		got := ws.shouldIncludePath(tt.path)
		if got != tt.expect {
			t.Errorf("shouldIncludePath(%q) = %v, want %v", tt.path, got, tt.expect)
		}
	}
}
