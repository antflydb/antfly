package docsaf

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CacheConfig configures the content cache behavior.
type CacheConfig struct {
	// Enabled enables caching (default: false)
	Enabled bool

	// Dir is the directory for persistent cache storage.
	// If empty, only in-memory caching is used.
	Dir string

	// TTL is the default time-to-live for cached entries.
	// HTTP Cache-Control headers take precedence when present.
	TTL time.Duration

	// MaxMemoryItems is the maximum number of items to keep in memory (default: 1000).
	MaxMemoryItems int

	// MaxDiskSize is the maximum disk cache size in bytes (default: 100MB).
	// Set to 0 for unlimited.
	MaxDiskSize int64

	// RespectCacheHeaders enables HTTP cache header parsing (default: true).
	// When enabled, Cache-Control, ETag, and Last-Modified are respected.
	RespectCacheHeaders bool

	// EnableDeduplication enables content hash deduplication (default: true).
	// Identical content from different URLs will be stored only once.
	EnableDeduplication bool
}

// CacheEntry represents a cached HTTP response.
type CacheEntry struct {
	// URL is the original request URL.
	URL string `json:"url"`

	// Body is the response body content.
	Body []byte `json:"body,omitempty"`

	// ContentType is the Content-Type header value.
	ContentType string `json:"content_type"`

	// StatusCode is the HTTP status code.
	StatusCode int `json:"status_code"`

	// ETag is the ETag header for conditional requests.
	ETag string `json:"etag,omitempty"`

	// LastModified is the Last-Modified header for conditional requests.
	LastModified string `json:"last_modified,omitempty"`

	// Expires is when this entry expires.
	Expires time.Time `json:"expires"`

	// CachedAt is when this entry was cached.
	CachedAt time.Time `json:"cached_at"`

	// ContentHash is the SHA-256 hash of the body for deduplication.
	ContentHash string `json:"content_hash,omitempty"`

	// BodyFile is the filename for disk-cached body (when using deduplication).
	BodyFile string `json:"body_file,omitempty"`
}

// IsExpired returns true if the cache entry has expired.
func (e *CacheEntry) IsExpired() bool {
	if e.Expires.IsZero() {
		return false
	}
	return time.Now().After(e.Expires)
}

// IsStale returns true if the entry is expired but may be revalidated.
func (e *CacheEntry) IsStale() bool {
	return e.IsExpired() && (e.ETag != "" || e.LastModified != "")
}

// CanRevalidate returns true if the entry has validators for conditional requests.
func (e *CacheEntry) CanRevalidate() bool {
	return e.ETag != "" || e.LastModified != ""
}

// ContentCache provides HTTP-aware caching with optional disk persistence.
type ContentCache struct {
	config CacheConfig

	mu      sync.RWMutex
	memory  map[string]*CacheEntry
	lruList []string // Simple LRU tracking

	// Content deduplication: hash -> body
	contentStore    map[string][]byte
	contentRefCount map[string]int

	diskSize int64
}

// NewContentCache creates a new content cache with the given configuration.
func NewContentCache(config CacheConfig) (*ContentCache, error) {
	if config.MaxMemoryItems == 0 {
		config.MaxMemoryItems = 1000
	}
	if config.MaxDiskSize == 0 {
		config.MaxDiskSize = 100 * 1024 * 1024 // 100MB
	}
	if config.TTL == 0 {
		config.TTL = 1 * time.Hour
	}

	cache := &ContentCache{
		config:          config,
		memory:          make(map[string]*CacheEntry),
		lruList:         make([]string, 0),
		contentStore:    make(map[string][]byte),
		contentRefCount: make(map[string]int),
	}

	// Create cache directory if specified
	if config.Dir != "" {
		if err := os.MkdirAll(config.Dir, 0750); err != nil {
			return nil, fmt.Errorf("failed to create cache directory: %w", err)
		}

		// Load existing cache index
		if err := cache.loadIndex(); err != nil {
			// Not fatal, just start fresh
			cache.memory = make(map[string]*CacheEntry)
		}
	}

	return cache, nil
}

// Get retrieves a cached entry for the given URL.
// Returns nil if not found or expired (and not revalidatable).
func (c *ContentCache) Get(url string) *CacheEntry {
	c.mu.RLock()
	entry, ok := c.memory[url]
	c.mu.RUnlock()

	if !ok {
		return nil
	}

	// Load body from disk or content store if needed
	if entry.Body == nil {
		c.mu.Lock()
		if entry.ContentHash != "" && c.config.EnableDeduplication {
			entry.Body = c.contentStore[entry.ContentHash]
		} else if entry.BodyFile != "" && c.config.Dir != "" {
			body, err := os.ReadFile(filepath.Join(c.config.Dir, entry.BodyFile)) //nolint:gosec // BodyFile is internally generated hash
			if err == nil {
				entry.Body = body
			}
		}
		c.mu.Unlock()
	}

	// Check if expired and not revalidatable
	if entry.IsExpired() && !entry.CanRevalidate() {
		c.mu.Lock()
		delete(c.memory, url)
		c.mu.Unlock()
		return nil
	}

	// Update LRU
	c.mu.Lock()
	c.touchLRU(url)
	c.mu.Unlock()

	return entry
}

// GetWithContentHash retrieves a cached entry by content hash (for deduplication).
func (c *ContentCache) GetWithContentHash(hash string) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.contentStore[hash]
}

// Set stores a response in the cache.
func (c *ContentCache) Set(url string, body []byte, headers http.Header, statusCode int) {
	entry := &CacheEntry{
		URL:          url,
		StatusCode:   statusCode,
		ContentType:  headers.Get("Content-Type"),
		ETag:         headers.Get("ETag"),
		LastModified: headers.Get("Last-Modified"),
		CachedAt:     time.Now(),
	}

	// Parse cache headers to determine expiry
	if c.config.RespectCacheHeaders {
		entry.Expires = c.parseExpiry(headers)
	} else {
		entry.Expires = time.Now().Add(c.config.TTL)
	}

	// Handle content deduplication
	if c.config.EnableDeduplication && len(body) > 0 {
		hash := c.hashContent(body)
		entry.ContentHash = hash

		c.mu.Lock()
		if _, exists := c.contentStore[hash]; !exists {
			c.contentStore[hash] = body
		}
		c.contentRefCount[hash]++
		c.mu.Unlock()
	} else {
		entry.Body = body
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict if at capacity
	for len(c.memory) >= c.config.MaxMemoryItems {
		c.evictOldest()
	}

	c.memory[url] = entry
	c.lruList = append(c.lruList, url)

	// Persist to disk if configured
	if c.config.Dir != "" {
		c.persistEntry(entry)
	}
}

// SetFromResponse stores an HTTP response in the cache.
func (c *ContentCache) SetFromResponse(resp *http.Response, body []byte) {
	c.Set(resp.Request.URL.String(), body, resp.Header, resp.StatusCode)
}

// AddConditionalHeaders adds If-None-Match and If-Modified-Since headers
// to a request if we have cached validators.
func (c *ContentCache) AddConditionalHeaders(req *http.Request) bool {
	entry := c.Get(req.URL.String())
	if entry == nil {
		return false
	}

	added := false
	if entry.ETag != "" {
		req.Header.Set("If-None-Match", entry.ETag)
		added = true
	}
	if entry.LastModified != "" {
		req.Header.Set("If-Modified-Since", entry.LastModified)
		added = true
	}

	return added
}

// HandleNotModified updates an existing cache entry when a 304 is received.
func (c *ContentCache) HandleNotModified(url string, headers http.Header) *CacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.memory[url]
	if !ok {
		return nil
	}

	// Update expiry based on new headers
	if c.config.RespectCacheHeaders {
		entry.Expires = c.parseExpiry(headers)
	} else {
		entry.Expires = time.Now().Add(c.config.TTL)
	}

	// Update validators if provided
	if etag := headers.Get("ETag"); etag != "" {
		entry.ETag = etag
	}
	if lastMod := headers.Get("Last-Modified"); lastMod != "" {
		entry.LastModified = lastMod
	}

	entry.CachedAt = time.Now()

	// Persist updated entry
	if c.config.Dir != "" {
		c.persistEntry(entry)
	}

	return entry
}

// IsDuplicate checks if content with this hash already exists.
func (c *ContentCache) IsDuplicate(body []byte) (bool, string) {
	if !c.config.EnableDeduplication {
		return false, ""
	}

	hash := c.hashContent(body)

	c.mu.RLock()
	_, exists := c.contentStore[hash]
	c.mu.RUnlock()

	return exists, hash
}

// ContentHash returns the hash of the given content.
func (c *ContentCache) ContentHash(body []byte) string {
	return c.hashContent(body)
}

// Clear removes all entries from the cache.
func (c *ContentCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.memory = make(map[string]*CacheEntry)
	c.lruList = make([]string, 0)
	c.contentStore = make(map[string][]byte)
	c.contentRefCount = make(map[string]int)
	c.diskSize = 0

	if c.config.Dir != "" {
		return os.RemoveAll(c.config.Dir)
	}

	return nil
}

// Stats returns cache statistics.
func (c *ContentCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var totalSize int64
	for _, entry := range c.memory {
		totalSize += int64(len(entry.Body))
	}
	for _, body := range c.contentStore {
		totalSize += int64(len(body))
	}

	return CacheStats{
		MemoryEntries:  len(c.memory),
		UniqueContents: len(c.contentStore),
		TotalSizeBytes: totalSize,
		DiskSizeBytes:  c.diskSize,
	}
}

// CacheStats contains cache statistics.
type CacheStats struct {
	MemoryEntries  int
	UniqueContents int
	TotalSizeBytes int64
	DiskSizeBytes  int64
}

// parseExpiry parses HTTP cache headers to determine expiry time.
func (c *ContentCache) parseExpiry(headers http.Header) time.Time {
	// Check Cache-Control header
	cacheControl := headers.Get("Cache-Control")
	if cacheControl != "" {
		// Parse max-age directive
		for directive := range strings.SplitSeq(cacheControl, ",") {
			directive = strings.TrimSpace(directive)

			// no-store or no-cache means don't cache
			if directive == "no-store" || directive == "no-cache" {
				return time.Now() // Expire immediately
			}

			// Parse max-age
			if after, ok := strings.CutPrefix(directive, "max-age="); ok {
				seconds, err := strconv.ParseInt(after, 10, 64)
				if err == nil && seconds > 0 {
					return time.Now().Add(time.Duration(seconds) * time.Second)
				}
			}
		}
	}

	// Check Expires header
	expires := headers.Get("Expires")
	if expires != "" {
		if t, err := http.ParseTime(expires); err == nil {
			return t
		}
	}

	// Default to configured TTL
	return time.Now().Add(c.config.TTL)
}

// hashContent computes SHA-256 hash of content.
func (c *ContentCache) hashContent(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

// touchLRU moves a URL to the end of the LRU list.
func (c *ContentCache) touchLRU(url string) {
	// Remove from current position
	for i, u := range c.lruList {
		if u == url {
			c.lruList = append(c.lruList[:i], c.lruList[i+1:]...)
			break
		}
	}
	// Add to end
	c.lruList = append(c.lruList, url)
}

// evictOldest removes the oldest entry from the cache.
func (c *ContentCache) evictOldest() {
	if len(c.lruList) == 0 {
		return
	}

	oldestURL := c.lruList[0]
	c.lruList = c.lruList[1:]

	entry, ok := c.memory[oldestURL]
	if ok {
		// Decrement content ref count
		if entry.ContentHash != "" {
			c.contentRefCount[entry.ContentHash]--
			if c.contentRefCount[entry.ContentHash] <= 0 {
				delete(c.contentStore, entry.ContentHash)
				delete(c.contentRefCount, entry.ContentHash)
			}
		}

		// Remove disk file if exists
		if entry.BodyFile != "" && c.config.Dir != "" {
			_ = os.Remove(filepath.Join(c.config.Dir, entry.BodyFile)) //nolint:gosec // BodyFile is internally generated hash, not user input
		}

		delete(c.memory, oldestURL)
	}
}

// persistEntry saves an entry to disk.
func (c *ContentCache) persistEntry(entry *CacheEntry) {
	if c.config.Dir == "" {
		return
	}

	// Save body to separate file if not using deduplication
	if entry.ContentHash == "" && len(entry.Body) > 0 {
		bodyHash := c.hashContent(entry.Body)
		bodyFile := bodyHash + ".body"
		bodyPath := filepath.Join(c.config.Dir, bodyFile)

		if err := os.WriteFile(bodyPath, entry.Body, 0600); err == nil { //nolint:gosec // bodyPath is internally generated hash path
			entry.BodyFile = bodyFile
			c.diskSize += int64(len(entry.Body))
		}
	}

	// Save entry metadata
	entryCopy := *entry
	entryCopy.Body = nil // Don't duplicate body in JSON

	data, err := json.Marshal(entryCopy)
	if err != nil {
		return
	}

	urlHash := c.hashContent([]byte(entry.URL))
	metaPath := filepath.Join(c.config.Dir, urlHash+".meta")
	_ = os.WriteFile(metaPath, data, 0600) //nolint:gosec // metaPath is internally generated hash path
}

// loadIndex loads the cache index from disk.
func (c *ContentCache) loadIndex() error {
	if c.config.Dir == "" {
		return nil
	}

	entries, err := os.ReadDir(c.config.Dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".meta") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(c.config.Dir, entry.Name()))
		if err != nil {
			continue
		}

		var cacheEntry CacheEntry
		if err := json.Unmarshal(data, &cacheEntry); err != nil {
			continue
		}

		// Skip expired entries that can't be revalidated
		if cacheEntry.IsExpired() && !cacheEntry.CanRevalidate() {
			continue
		}

		c.memory[cacheEntry.URL] = &cacheEntry
		c.lruList = append(c.lruList, cacheEntry.URL)

		// Track content for deduplication
		if cacheEntry.ContentHash != "" {
			c.contentRefCount[cacheEntry.ContentHash]++
		}

		// Track disk size
		if cacheEntry.BodyFile != "" {
			info, err := os.Stat(filepath.Join(c.config.Dir, cacheEntry.BodyFile))
			if err == nil {
				c.diskSize += info.Size()
			}
		}
	}

	return nil
}

// CachingTransport wraps http.RoundTripper with caching support.
type CachingTransport struct {
	Transport http.RoundTripper
	Cache     *ContentCache
}

// NewCachingTransport creates a new caching transport.
func NewCachingTransport(transport http.RoundTripper, cache *ContentCache) *CachingTransport {
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &CachingTransport{
		Transport: transport,
		Cache:     cache,
	}
}

// RoundTrip implements http.RoundTripper with caching.
func (ct *CachingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Only cache GET requests
	if req.Method != http.MethodGet {
		return ct.Transport.RoundTrip(req)
	}

	url := req.URL.String()

	// Check cache first
	entry := ct.Cache.Get(url)
	if entry != nil && !entry.IsExpired() {
		// Return cached response
		return ct.createCachedResponse(req, entry), nil
	}

	// Add conditional headers if we have a stale entry
	if entry != nil && entry.CanRevalidate() {
		ct.Cache.AddConditionalHeaders(req)
	}

	// Make the request
	resp, err := ct.Transport.RoundTrip(req)
	if err != nil {
		// On error, return stale cache if available
		if entry != nil {
			return ct.createCachedResponse(req, entry), nil
		}
		return nil, err
	}

	// Handle 304 Not Modified
	if resp.StatusCode == http.StatusNotModified && entry != nil {
		_ = resp.Body.Close()
		updatedEntry := ct.Cache.HandleNotModified(url, resp.Header)
		if updatedEntry != nil {
			return ct.createCachedResponse(req, updatedEntry), nil
		}
	}

	// Cache successful responses
	if resp.StatusCode == http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, err
		}

		ct.Cache.SetFromResponse(resp, body)

		// Return response with new body reader
		resp.Body = io.NopCloser(strings.NewReader(string(body)))
	}

	return resp, nil
}

// createCachedResponse creates an http.Response from a cache entry.
func (ct *CachingTransport) createCachedResponse(req *http.Request, entry *CacheEntry) *http.Response {
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", entry.StatusCode, http.StatusText(entry.StatusCode)),
		StatusCode:    entry.StatusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(string(entry.Body))),
		ContentLength: int64(len(entry.Body)),
		Request:       req,
	}
}
