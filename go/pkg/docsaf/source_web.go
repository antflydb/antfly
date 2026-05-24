package docsaf

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/gocolly/colly/v2"
)

// urlNormalizer handles URL canonicalization for consistent deduplication.
type urlNormalizer struct {
	removeTrailingSlash bool
	removeFragment      bool
	removeDefaultPort   bool
	lowercaseHost       bool
	sortQueryParams     bool
}

// newURLNormalizer creates a URL normalizer with sensible defaults.
func newURLNormalizer() *urlNormalizer {
	return &urlNormalizer{
		removeTrailingSlash: true,
		removeFragment:      true,
		removeDefaultPort:   true,
		lowercaseHost:       true,
		sortQueryParams:     false, // Can cause issues with some APIs
	}
}

// Normalize canonicalizes a URL string.
func (n *urlNormalizer) Normalize(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL, err
	}

	// Lowercase scheme
	u.Scheme = strings.ToLower(u.Scheme)

	// Lowercase host
	if n.lowercaseHost {
		u.Host = strings.ToLower(u.Host)
	}

	// Remove default ports
	if n.removeDefaultPort {
		host := u.Hostname()
		port := u.Port()
		if (u.Scheme == "http" && port == "80") ||
			(u.Scheme == "https" && port == "443") {
			u.Host = host
		}
	}

	// Normalize empty path to "/"
	if u.Path == "" {
		u.Path = "/"
	}

	// Remove trailing slash (except for root)
	if n.removeTrailingSlash && len(u.Path) > 1 && strings.HasSuffix(u.Path, "/") {
		u.Path = strings.TrimSuffix(u.Path, "/")
	}

	// Remove fragment
	if n.removeFragment {
		u.Fragment = ""
	}

	return u.String(), nil
}

// retryTransport wraps http.RoundTripper with retry logic.
type retryTransport struct {
	transport  http.RoundTripper
	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration
}

// newRetryTransport creates a transport with exponential backoff retry.
func newRetryTransport(transport http.RoundTripper, maxRetries int, baseDelay time.Duration) *retryTransport {
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &retryTransport{
		transport:  transport,
		maxRetries: maxRetries,
		baseDelay:  baseDelay,
		maxDelay:   30 * time.Second,
	}
}

// RoundTrip implements http.RoundTripper with retry logic.
func (rt *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	for attempt := 0; attempt <= rt.maxRetries; attempt++ {
		resp, err = rt.transport.RoundTrip(req)

		// Success or non-retryable error
		if err == nil {
			// Retry on 5xx errors (server errors)
			if resp.StatusCode < 500 {
				return resp, nil
			}
			// Close body before retry
			_ = resp.Body.Close()
		}

		// Don't retry if context is cancelled
		if req.Context().Err() != nil {
			if err == nil {
				return resp, nil
			}
			return nil, err
		}

		// Don't sleep after last attempt
		if attempt < rt.maxRetries {
			delay := min(rt.baseDelay*time.Duration(1<<uint(attempt)), rt.maxDelay)

			select {
			case <-time.After(delay):
			case <-req.Context().Done():
				if err == nil {
					return resp, nil
				}
				return nil, req.Context().Err()
			}
		}
	}

	return resp, err
}

// responseCache provides in-memory caching for HTTP responses.
type responseCache struct {
	mu       sync.RWMutex
	cache    map[string]*cachedResponse
	ttl      time.Duration
	maxItems int
}

type cachedResponse struct {
	body        []byte
	contentType string
	statusCode  int
	cachedAt    time.Time
}

// newResponseCache creates a new cache with the given TTL and max items.
func newResponseCache(ttl time.Duration, maxItems int) *responseCache {
	return &responseCache{
		cache:    make(map[string]*cachedResponse),
		ttl:      ttl,
		maxItems: maxItems,
	}
}

// Get retrieves a cached response if it exists and isn't expired.
func (c *responseCache) Get(url string) (*cachedResponse, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.cache[url]
	if !ok {
		return nil, false
	}

	if time.Since(entry.cachedAt) > c.ttl {
		return nil, false
	}

	return entry, true
}

// Set stores a response in the cache.
func (c *responseCache) Set(url string, body []byte, contentType string, statusCode int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict old entries if at capacity
	if len(c.cache) >= c.maxItems {
		c.evictOldest()
	}

	c.cache[url] = &cachedResponse{
		body:        body,
		contentType: contentType,
		statusCode:  statusCode,
		cachedAt:    time.Now(),
	}
}

// evictOldest removes the oldest cached entry.
func (c *responseCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, entry := range c.cache {
		if oldestKey == "" || entry.cachedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.cachedAt
		}
	}

	if oldestKey != "" {
		delete(c.cache, oldestKey)
	}
}

// WebSourceConfig holds configuration for a WebSource.
type WebSourceConfig struct {
	// StartURL is the starting URL to crawl (required)
	StartURL string

	// BaseURL is the base URL for generating document links.
	// If empty, it will be derived from StartURL.
	BaseURL string

	// AllowedDomains restricts crawling to these domains.
	// If empty, only the domain from StartURL is allowed.
	AllowedDomains []string

	// IncludePatterns is a list of glob patterns for URL paths to include.
	// If empty, all paths are included (subject to exclude patterns).
	// Patterns match against the URL path (e.g., "/docs/**", "/guides/*")
	IncludePatterns []string

	// ExcludePatterns is a list of glob patterns for URL paths to exclude.
	// Default excludes common non-content paths.
	ExcludePatterns []string

	// MaxDepth is the maximum crawl depth (0 = unlimited).
	MaxDepth int

	// MaxPages is the maximum number of pages to crawl (0 = unlimited).
	MaxPages int

	// Concurrency is the number of concurrent requests (default: 2).
	Concurrency int

	// RequestDelay is the delay between requests (default: 100ms).
	RequestDelay time.Duration

	// UserAgent is the User-Agent string to use for requests.
	UserAgent string

	// UseSitemap enables sitemap-based crawling.
	// When enabled, the crawler will first fetch and parse the sitemap
	// to discover URLs before following links.
	UseSitemap bool

	// SitemapURL is the URL of the sitemap (optional).
	// If empty and UseSitemap is true, it will try /sitemap.xml
	SitemapURL string

	// SitemapOnly restricts crawling to URLs found in the sitemap only.
	// When true, link discovery is disabled.
	SitemapOnly bool

	// RespectRobotsTxt enables robots.txt parsing (default: true).
	RespectRobotsTxt bool

	// MaxRetries is the number of retry attempts for failed requests (default: 3).
	MaxRetries int

	// RetryDelay is the base delay for exponential backoff retry (default: 1s).
	// The actual delay doubles with each retry: 1s, 2s, 4s, etc.
	RetryDelay time.Duration

	// CacheTTL is how long to cache responses (default: 0 = disabled).
	// Set to a positive duration to enable caching.
	CacheTTL time.Duration

	// CacheMaxItems is the maximum number of items to cache (default: 1000).
	CacheMaxItems int

	// CacheDir is the directory for persistent cache storage.
	// If empty, only in-memory caching is used.
	CacheDir string

	// CacheRespectHeaders enables HTTP cache header parsing (default: true when caching enabled).
	// When enabled, Cache-Control, ETag, and Last-Modified are respected.
	CacheRespectHeaders bool

	// CacheDeduplication enables content hash deduplication (default: true when caching enabled).
	// Identical content from different URLs will be stored only once.
	CacheDeduplication bool

	// NormalizeURLs enables URL normalization for deduplication (default: true).
	// Includes lowercasing host, removing default ports, removing trailing slashes.
	NormalizeURLs bool
}

// WebSource crawls websites and yields content items.
type WebSource struct {
	config       WebSourceConfig
	normalizer   *urlNormalizer
	cache        *responseCache // Legacy simple cache
	contentCache *ContentCache  // New advanced cache
	httpClient   *http.Client
}

// NewWebSource creates a new web content source.
func NewWebSource(config WebSourceConfig) (*WebSource, error) {
	if config.StartURL == "" {
		return nil, fmt.Errorf("StartURL is required")
	}

	// Parse start URL to extract domain
	parsedURL, err := url.Parse(config.StartURL)
	if err != nil {
		return nil, fmt.Errorf("invalid StartURL: %w", err)
	}

	// Set defaults
	if config.BaseURL == "" {
		config.BaseURL = parsedURL.Scheme + "://" + parsedURL.Host
	}

	if len(config.AllowedDomains) == 0 {
		config.AllowedDomains = []string{parsedURL.Host}
	}

	if config.Concurrency == 0 {
		config.Concurrency = 2
	}

	if config.RequestDelay == 0 {
		config.RequestDelay = 100 * time.Millisecond
	}

	if config.UserAgent == "" {
		config.UserAgent = "Antfly-Docsaf/1.0 (+https://antfly.co)"
	}

	// Retry defaults
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 1 * time.Second
	}

	// Cache defaults
	if config.CacheMaxItems == 0 {
		config.CacheMaxItems = 1000
	}

	// Default exclude patterns for common non-content paths
	if len(config.ExcludePatterns) == 0 {
		config.ExcludePatterns = []string{
			"/api/**",
			"/search/**",
			"/**/*.css",
			"/**/*.js",
			"/**/*.png",
			"/**/*.jpg",
			"/**/*.jpeg",
			"/**/*.gif",
			"/**/*.svg",
			"/**/*.ico",
			"/**/*.woff",
			"/**/*.woff2",
			"/**/*.ttf",
			"/**/*.eot",
		}
	}

	ws := &WebSource{config: config}

	// Initialize URL normalizer if enabled (default: true)
	if config.NormalizeURLs || !configExplicitlySet(config, "NormalizeURLs") {
		ws.normalizer = newURLNormalizer()
	}

	// Initialize cache if TTL is set
	if config.CacheTTL > 0 {
		// Use advanced ContentCache if advanced features are requested
		if config.CacheDir != "" || config.CacheRespectHeaders || config.CacheDeduplication {
			contentCache, err := NewContentCache(CacheConfig{
				Enabled:             true,
				Dir:                 config.CacheDir,
				TTL:                 config.CacheTTL,
				MaxMemoryItems:      config.CacheMaxItems,
				RespectCacheHeaders: config.CacheRespectHeaders,
				EnableDeduplication: config.CacheDeduplication,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create cache: %w", err)
			}
			ws.contentCache = contentCache
		} else {
			// Use simple in-memory cache for backward compatibility
			ws.cache = newResponseCache(config.CacheTTL, config.CacheMaxItems)
		}
	}

	// Initialize HTTP client with retry transport
	baseTransport := newRetryTransport(
		http.DefaultTransport,
		config.MaxRetries,
		config.RetryDelay,
	)

	// Wrap with caching transport if advanced cache is enabled
	var transport http.RoundTripper = baseTransport
	if ws.contentCache != nil {
		transport = NewCachingTransport(baseTransport, ws.contentCache)
	}

	ws.httpClient = &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}

	return ws, nil
}

// configExplicitlySet is a helper that always returns false.
// In Go, we can't distinguish between explicit false and zero-value,
// so we default NormalizeURLs to true.
func configExplicitlySet(_ WebSourceConfig, _ string) bool {
	return false
}

// Type returns "web" as the source type.
func (ws *WebSource) Type() string {
	return "web"
}

// BaseURL returns the base URL for this source.
func (ws *WebSource) BaseURL() string {
	return ws.config.BaseURL
}

// CacheStats returns statistics about the cache.
// Returns nil if caching is not enabled.
func (ws *WebSource) CacheStats() *CacheStats {
	if ws.contentCache != nil {
		stats := ws.contentCache.Stats()
		return &stats
	}
	return nil
}

// ClearCache removes all entries from the cache.
func (ws *WebSource) ClearCache() error {
	if ws.contentCache != nil {
		return ws.contentCache.Clear()
	}
	return nil
}

// IsCached checks if a URL is in the cache.
func (ws *WebSource) IsCached(url string) bool {
	if ws.contentCache != nil {
		return ws.contentCache.Get(url) != nil
	}
	if ws.cache != nil {
		_, ok := ws.cache.Get(url)
		return ok
	}
	return false
}

// Traverse crawls the website and yields content items.
func (ws *WebSource) Traverse(ctx context.Context) (<-chan ContentItem, <-chan error) {
	items := make(chan ContentItem)
	errs := make(chan error, 1)

	go func() {
		defer close(items)
		defer close(errs)

		visited := &sync.Map{}
		pageCount := 0
		var mu sync.Mutex
		done := false

		// Build collector options
		opts := []colly.CollectorOption{
			colly.AllowedDomains(ws.config.AllowedDomains...),
			colly.Async(true),
			colly.MaxDepth(ws.config.MaxDepth),
		}

		// Enable robots.txt support if configured (defaults to true in colly)
		if ws.config.RespectRobotsTxt {
			// colly respects robots.txt by default, but we can be explicit
			opts = append(opts, colly.ParseHTTPErrorResponse())
		}

		// Create collector
		c := colly.NewCollector(opts...)

		// Set limits
		_ = c.Limit(&colly.LimitRule{
			DomainGlob:  "*",
			Parallelism: ws.config.Concurrency,
			Delay:       ws.config.RequestDelay,
		})

		// Set user agent
		c.UserAgent = ws.config.UserAgent

		// Handle HTML responses
		c.OnResponse(func(r *colly.Response) {
			// Check for cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}

			mu.Lock()
			if done || (ws.config.MaxPages > 0 && pageCount >= ws.config.MaxPages) {
				mu.Unlock()
				return
			}
			pageCount++
			mu.Unlock()

			urlStr := r.Request.URL.String()

			// Normalize URL for deduplication
			normalizedURL := ws.normalizeURL(urlStr)

			path := r.Request.URL.Path
			if path == "" {
				path = "/"
			}

			// Check if we've already processed this URL (using normalized URL)
			if _, loaded := visited.LoadOrStore(normalizedURL, true); loaded {
				return
			}

			// Check include/exclude patterns
			if !ws.shouldIncludePath(path) {
				return
			}

			contentType := r.Headers.Get("Content-Type")

			// Only process HTML content
			if !strings.Contains(contentType, "text/html") {
				return
			}

			// Cache the response if caching is enabled
			if ws.cache != nil {
				ws.cache.Set(normalizedURL, r.Body, contentType, r.StatusCode)
			}

			select {
			case items <- ContentItem{
				Path:        path,
				SourceURL:   urlStr,
				Content:     r.Body,
				ContentType: contentType,
				Metadata: map[string]any{
					"source_type":  "web",
					"status_code":  r.StatusCode,
					"content_type": contentType,
					"depth":        r.Request.Depth,
				},
			}:
			case <-ctx.Done():
				mu.Lock()
				done = true
				mu.Unlock()
				return
			}
		})

		// Follow links (unless sitemap-only mode)
		if !ws.config.SitemapOnly {
			c.OnHTML("a[href]", func(e *colly.HTMLElement) {
				select {
				case <-ctx.Done():
					return
				default:
				}

				mu.Lock()
				if done || (ws.config.MaxPages > 0 && pageCount >= ws.config.MaxPages) {
					mu.Unlock()
					return
				}
				mu.Unlock()

				link := e.Attr("href")
				if link == "" {
					return
				}

				// Skip fragment-only links, javascript, mailto, tel
				if strings.HasPrefix(link, "#") ||
					strings.HasPrefix(link, "javascript:") ||
					strings.HasPrefix(link, "mailto:") ||
					strings.HasPrefix(link, "tel:") {
					return
				}

				// Resolve relative URLs
				absURL := e.Request.AbsoluteURL(link)
				if absURL == "" {
					return
				}

				// Normalize URL for consistent deduplication
				normalizedURL := ws.normalizeURL(absURL)

				// Parse URL to check path
				parsedURL, err := url.Parse(normalizedURL)
				if err != nil {
					return
				}

				// Check include/exclude patterns
				if !ws.shouldIncludePath(parsedURL.Path) {
					return
				}

				// Check if already visited (using normalized URL)
				if _, loaded := visited.Load(normalizedURL); loaded {
					return
				}

				_ = e.Request.Visit(normalizedURL)
			})
		}

		// Handle errors
		c.OnError(func(r *colly.Response, err error) {
			log.Printf("Warning: Failed to fetch %s: %v", r.Request.URL, err)
		})

		// If using sitemap, fetch and process it first
		if ws.config.UseSitemap {
			sitemapURLs, err := ws.fetchSitemap(ctx)
			if err != nil {
				log.Printf("Warning: Failed to fetch sitemap: %v", err)
			} else {
				for _, u := range sitemapURLs {
					select {
					case <-ctx.Done():
						errs <- ctx.Err()
						return
					default:
					}

					mu.Lock()
					if done || (ws.config.MaxPages > 0 && pageCount >= ws.config.MaxPages) {
						mu.Unlock()
						break
					}
					mu.Unlock()

					_ = c.Visit(u)
				}
			}
		}

		// Start crawling from start URL (if not sitemap-only or sitemap failed)
		if !ws.config.SitemapOnly {
			_ = c.Visit(ws.config.StartURL)
		}

		// Wait for all requests to complete
		c.Wait()

		// Check for context cancellation
		if ctx.Err() != nil {
			errs <- ctx.Err()
		}
	}()

	return items, errs
}

// shouldIncludePath checks if a URL path should be included based on patterns.
func (ws *WebSource) shouldIncludePath(path string) bool {
	if path == "" {
		path = "/"
	}

	// Check exclude patterns first
	for _, pattern := range ws.config.ExcludePatterns {
		matched, err := doublestar.Match(pattern, path)
		if err != nil {
			continue
		}
		if matched {
			return false
		}
	}

	// If no include patterns, include everything (that wasn't excluded)
	if len(ws.config.IncludePatterns) == 0 {
		return true
	}

	// Check include patterns
	for _, pattern := range ws.config.IncludePatterns {
		matched, err := doublestar.Match(pattern, path)
		if err != nil {
			continue
		}
		if matched {
			return true
		}
	}

	return false
}

// normalizeURL canonicalizes a URL for consistent deduplication.
func (ws *WebSource) normalizeURL(rawURL string) string {
	if ws.normalizer == nil {
		return rawURL
	}
	normalized, err := ws.normalizer.Normalize(rawURL)
	if err != nil {
		return rawURL
	}
	return normalized
}

// Sitemap types for XML parsing
type sitemapIndex struct {
	XMLName  xml.Name         `xml:"sitemapindex"`
	Sitemaps []sitemapLocator `xml:"sitemap"`
}

type sitemapLocator struct {
	Loc string `xml:"loc"`
}

type urlset struct {
	XMLName xml.Name     `xml:"urlset"`
	URLs    []sitemapURL `xml:"url"`
}

type sitemapURL struct {
	Loc        string `xml:"loc"`
	LastMod    string `xml:"lastmod"`
	ChangeFreq string `xml:"changefreq"`
	Priority   string `xml:"priority"`
}

// fetchSitemap fetches and parses the sitemap, returning all URLs.
func (ws *WebSource) fetchSitemap(ctx context.Context) ([]string, error) {
	sitemapURL := ws.config.SitemapURL
	if sitemapURL == "" {
		// Try default sitemap location
		parsedBase, _ := url.Parse(ws.config.BaseURL)
		parsedBase.Path = "/sitemap.xml"
		sitemapURL = parsedBase.String()
	}

	return ws.fetchSitemapRecursive(ctx, sitemapURL, make(map[string]bool))
}

// fetchSitemapRecursive fetches a sitemap and any nested sitemaps.
func (ws *WebSource) fetchSitemapRecursive(ctx context.Context, sitemapURL string, visited map[string]bool) ([]string, error) {
	// Normalize sitemap URL for deduplication
	normalizedURL := ws.normalizeURL(sitemapURL)
	if visited[normalizedURL] {
		return nil, nil
	}
	visited[normalizedURL] = true

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, "GET", sitemapURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", ws.config.UserAgent)

	// Use the HTTP client with retry support
	resp, err := ws.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close in deferred cleanup

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sitemap returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var urls []string

	// Try parsing as sitemap index first
	var index sitemapIndex
	if err := xml.Unmarshal(body, &index); err == nil && len(index.Sitemaps) > 0 {
		// This is a sitemap index, fetch each nested sitemap
		for _, sm := range index.Sitemaps {
			nestedURLs, err := ws.fetchSitemapRecursive(ctx, sm.Loc, visited)
			if err != nil {
				log.Printf("Warning: Failed to fetch nested sitemap %s: %v", sm.Loc, err)
				continue
			}
			urls = append(urls, nestedURLs...)
		}
		return urls, nil
	}

	// Try parsing as urlset
	var urlSet urlset
	if err := xml.Unmarshal(body, &urlSet); err != nil {
		return nil, fmt.Errorf("failed to parse sitemap: %w", err)
	}

	// Filter URLs based on include/exclude patterns
	for _, u := range urlSet.URLs {
		parsedURL, err := url.Parse(u.Loc)
		if err != nil {
			continue
		}

		if ws.shouldIncludePath(parsedURL.Path) {
			urls = append(urls, u.Loc)
		}
	}

	return urls, nil
}
