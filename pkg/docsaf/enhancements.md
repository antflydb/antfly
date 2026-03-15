# Docsaf Enhancements

This document tracks planned enhancements for the docsaf generic content traversal system.

## Implemented

- [x] **Generic Content Source Interface** - Abstracted content sources with `ContentSource` and `ContentItem` types
- [x] **Filesystem Source** - `FilesystemSource` for traversing local directories
- [x] **Web Crawler Source** - `WebSource` using go-colly for web scraping
- [x] **Sitemap Support** - Fetch and parse XML sitemaps (including sitemap indexes) for URL discovery
- [x] **Content Processors** - `ContentProcessor` interface working on raw bytes instead of files
- [x] **Unified Processor** - `Processor` that works with any content source
- [x] **Exponential Backoff Retry** - Automatic retry with exponential backoff for transient failures (5xx, timeouts)
- [x] **robots.txt Support** - Respect robots.txt via colly's built-in support with `RespectRobotsTxt` config option
- [x] **URL Canonicalization** - Normalize URLs for consistent deduplication (lowercase host, remove default ports, remove trailing slashes, remove fragments)
- [x] **In-Memory Response Caching** - Optional TTL-based caching of HTTP responses
- [x] **PDF Processor** - Extract text content from PDF files with page-level sections
- [x] **OpenAPI Processor** - Parse OpenAPI/Swagger specs and extract endpoint documentation
- [x] **HTML Processor** - Extract content from HTML with heading-based section splitting
- [x] **Markdown Processor** - Parse Markdown files with heading-based sections
- [x] **Git Source** - Clone and traverse Git repositories with branch/tag support, GitHub/GitLab URL detection, and authentication
- [x] **HTTP Caching** - Respect Cache-Control, ETag, and Last-Modified headers with conditional request support
- [x] **Persistent Cache Storage** - Cache fetched content to disk for faster re-crawls with automatic loading
- [x] **Content Deduplication** - Skip pages with identical content hashes using SHA-256

## Planned Enhancements

### Rate Limiting & Politeness

- [ ] **Configurable Rate Limiting** - Add token bucket or leaky bucket rate limiting per domain
- [ ] **Adaptive Rate Limiting** - Automatically slow down based on server response times or 429 status codes
- [ ] **Concurrent Domain Limiting** - Limit concurrent requests per domain independently

### Retry & Error Handling

- [ ] **Circuit Breaker** - Stop hitting failing domains temporarily to avoid wasting resources
- [ ] **Detailed Error Reporting** - Collect and report errors per-URL for debugging

### JavaScript Rendering

- [ ] **Headless Browser Integration** - Add optional headless Chrome/Chromium support via chromedp or rod
- [ ] **SPA Detection** - Automatically detect SPAs that require JS rendering
- [ ] **Selective JS Rendering** - Only use headless browser for pages that need it

### Caching

- [ ] **Incremental Crawling** - Only fetch pages that have changed since last crawl

### Link & URL Handling

- [ ] **Query Parameter Filtering** - Option to strip or filter query parameters
- [ ] **Fragment Handling** - Better handling of fragment identifiers for SPAs
- [ ] **Redirect Following** - Track and report redirect chains

### Content Processing

- [ ] **Content Type Detection** - Better MIME type sniffing for ambiguous content
- [ ] **Encoding Detection** - Auto-detect and handle various character encodings
- [ ] **RST Processor** - Parse reStructuredText files (common in Python docs)
- [ ] **AsciiDoc Processor** - Parse AsciiDoc files

### Additional Content Sources

- [ ] **S3 Source** - Traverse AWS S3 buckets
- [ ] **Google Drive Source** - Traverse Google Drive folders
- [ ] **Confluence Source** - Fetch pages from Atlassian Confluence
- [ ] **Notion Source** - Fetch pages from Notion workspaces

### Monitoring & Observability

- [ ] **Progress Reporting** - Real-time progress callbacks for UI integration
- [ ] **Metrics Collection** - Collect crawl metrics (pages/sec, bytes, errors)
- [ ] **OpenTelemetry Integration** - Add tracing and metrics via OpenTelemetry

### Performance

- [ ] **Connection Pooling** - Reuse HTTP connections per domain
- [ ] **HTTP/2 Support** - Enable HTTP/2 for supported servers
- [ ] **Compression** - Request and handle gzip/brotli compressed responses
- [ ] **Streaming Processing** - Process content as it streams in for large files

### Security

- [ ] **Authentication Support** - Support for Basic Auth, OAuth, API keys, cookies
- [ ] **Cookie Handling** - Maintain session cookies across requests
- [ ] **Proxy Support** - Route requests through HTTP/SOCKS proxies
- [ ] **TLS Configuration** - Custom TLS settings and certificate handling

### Configuration

- [ ] **YAML/JSON Config Files** - Load source configuration from files
- [ ] **Environment Variable Support** - Configure via environment variables
- [ ] **Preset Configurations** - Common configurations for popular documentation platforms (ReadTheDocs, Docusaurus, GitBook, etc.)

## Architecture Notes

The current architecture follows these principles:

1. **Single Responsibility** - Sources handle traversal, Processors handle content extraction
2. **Open/Closed** - New sources can be added without modifying existing processors
3. **Interface Segregation** - Small, focused interfaces (`ContentSource`, `ContentProcessor`)
4. **Dependency Inversion** - Depend on interfaces, not concrete implementations

When adding new features, prefer:
- Composition over inheritance
- Configuration over code changes
- Streaming over buffering for large content
- Fail-safe defaults (respect robots.txt, rate limit by default)
