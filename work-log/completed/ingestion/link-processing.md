# Schema-Aware Link Processing and Content Security

## Status

✅ **Completed** - ContentProcessor system with template-driven link detection and security controls

## Overview

Enhanced summarization with schema-aware link detection and download processing. Links marked with `x-antfly-types: ["link"]` are automatically downloaded, processed (HTML article extraction, PDF text extraction, image conversion), and embedded in the document before LLM summarization.

## Architecture

### ContentProcessor Interface

Extensible system for handling different content types:

```go
// ContentProcessor handles processing of downloaded content based on type
type ContentProcessor interface {
    // Process converts raw content to LLM-compatible format
    // Returns: processedData, outputFormat (e.g., "text", "image", "data-url"), error
    Process(ctx context.Context, contentType string, data []byte) ([]byte, string, error)
}
```

### Default Implementations

**DefaultContentProcessor** routes based on content type:
- **HTML**: Uses `github.com/go-shiori/go-readability` for article extraction
- **PDF**: Uses `github.com/ledongthuc/pdf` for text extraction
- **Images**: Converts to data URIs with automatic resizing

### Main Function

```go
func DownloadAndProcessLink(
    ctx context.Context,
    url string,
    config *ContentSecurityConfig,
    processor ContentProcessor,  // nil uses default
) (processedData []byte, outputFormat string, error)
```

**Flow**:
1. Validate URL against security config
2. Create child context with download timeout
3. Fetch content via HTTP
4. Detect content type from headers/extension
5. Route to appropriate processor
6. Return processed data ready for LLM

## Template-Driven Link Detection

### Field Path Extraction

Uses raymond AST parser to extract referenced fields from templates:

```go
// ExtractFieldPaths parses a Handlebars template
// Returns paths like ["title"], ["metadata", "author", "name"]
func ExtractFieldPaths(template string) ([][]string, error)
```

**Example**:
```handlebars
{{title}}
{{metadata.author.name}}
{{#if image_url}}![]({{image_url}}){{/if}}
```

**Extracted paths**: `[["title"], ["metadata", "author", "name"], ["image_url"]]`

### Schema-Aware Processing

Only processes fields referenced in template AND marked as `link` type:

```go
// Check if field is referenced in template
isReferenced := len(referencedFields) == 0 || isFieldInList(path, referencedFields)

if !isReferenced {
    return // Skip fields not used in template
}

// Check schema for link type
isLink := schema != nil && HasAntflyType(schema, docType, path, AntflyTypeLink)

if isLink {
    // Download and process link
    processedData, outputFormat, err := DownloadAndProcessLink(ctx, strValue, config, nil)
    // ...
}
```

## Content Security Configuration

### Security Settings

```go
type ContentSecurityConfig struct {
    // AllowedHosts whitelist (empty = all allowed except private IPs)
    // Example: ["example.com", "cdn.example.com", "192.0.2.1"]
    AllowedHosts []string

    // BlockPrivateIPs blocks private IP ranges (default: true)
    // Blocked: 127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 169.254.0.0/16
    BlockPrivateIPs bool

    // MaxDownloadSizeBytes limits download size (default: 100MB)
    MaxDownloadSizeBytes int64

    // DownloadTimeoutSeconds sets timeout (default: 30 seconds)
    DownloadTimeoutSeconds int

    // MaxImageDimension limits image size (default: 2048 pixels)
    MaxImageDimension int
}
```

### Configuration Example

```yaml
content_security:
  allowed_hosts:
    - "example.com"
    - "cdn.example.com"
  block_private_ips: true
  max_download_size_bytes: 104857600  # 100MB
  download_timeout_seconds: 30
  max_image_dimension: 2048
```

### Security Validations

1. **URL Validation**: Check against `AllowedHosts` whitelist
2. **Private IP Blocking**: Prevent SSRF attacks by blocking private IP ranges
3. **Size Limits**: Use `io.LimitReader` with `MaxDownloadSizeBytes`
4. **Timeouts**: Download context with `DownloadTimeoutSeconds`
5. **Image Resizing**: Automatic resizing to `MaxImageDimension`

## Content Type Processing

### HTML Processing

**Library**: `github.com/go-shiori/go-readability`

**Process**:
1. Parse HTML document
2. Extract main article content
3. Remove scripts, ads, navigation
4. Return cleaned text for LLM

**Output format**: `"text"`

### PDF Processing

**Library**: `github.com/ledongthuc/pdf`

**Process**:
1. Parse PDF document
2. Extract text from all pages
3. Combine into single text stream

**Output format**: `"text"`

**Note**: Image rendering not yet implemented

### Image Processing

**Libraries**: `image`, `image/jpeg`, `image/png`

**Process**:
1. Decode image
2. Resize if dimensions exceed limit (preserving aspect ratio)
3. Encode as JPEG
4. Convert to data URI: `data:image/jpeg;base64,...`

**Output format**: `"data-url"`

**Supported formats**: JPEG, PNG, GIF

## Schema Utilities

### Type Checking

```go
// GetFieldTypeFromSchema navigates nested schema to find field type
// Returns: []AntflyType from x-antfly-types
func GetFieldTypeFromSchema(
    schema *TableSchema,
    docType string,
    fieldPath []string,
) []AntflyType

// HasAntflyType checks if field has specific type annotation
func HasAntflyType(
    schema *TableSchema,
    docType string,
    fieldPath []string,
    targetType AntflyType,
) bool
```

### Document Traversal

```go
// TraverseDocumentFields recursively walks document structure
// Yields (fieldPath, value) pairs for all fields including nested ones
func TraverseDocumentFields(
    doc map[string]any,
    callback func(path []string, value any),
)
```

## Usage Example

### Schema Definition

```json
{
  "type": "object",
  "properties": {
    "title": {"type": "string"},
    "article_url": {
      "type": "string",
      "x-antfly-types": ["link"]
    },
    "thumbnail_url": {
      "type": "string",
      "x-antfly-types": ["link"]
    }
  }
}
```

### Document Renderer Template

```handlebars
# {{title}}

Article: {{article_url}}

![Thumbnail]({{thumbnail_url}})
```

### Processing Flow

1. **Template Analysis**: Extract referenced fields: `["title", "article_url", "thumbnail_url"]`
2. **Schema Check**: Identify link fields: `["article_url", "thumbnail_url"]`
3. **Download**:
   - `article_url` → HTML → Readability → Extracted text
   - `thumbnail_url` → Image → Resize → Data URI
4. **Field Replacement**:
   - `article_url` value replaced with extracted article text
   - `thumbnail_url` value replaced with data URI
5. **Template Rendering**: Render template with processed values
6. **LLM Input**: Send rendered content to LLM for summarization

## Error Handling

### Fail-Fast Strategy

Link processing errors fail the entire summarization request:
```go
processedData, outputFormat, err := DownloadAndProcessLink(ctx, url, config, nil)
if err != nil {
    // Fail fast - return error immediately
    return "", fmt.Errorf("failed to process link field %v: %w", path, err)
}
```

**Rationale**: Missing content may produce incorrect/incomplete summaries

### Error Types

- **Security violation**: URL blocked by security config
- **Download timeout**: Request exceeded timeout
- **Size exceeded**: Content larger than limit
- **Unsupported type**: No processor for content type
- **Processing failed**: Content extraction/conversion error

## Custom Prompt Support

### WithPrompt Option

```go
// Add custom prompt option
func WithPrompt(prompt string) SummarizeOption
```

**Behavior**:
- Custom prompt overrides default template
- Still performs link processing before applying prompt
- Useful for specialized summarization tasks

**Example**:
```go
summarizer.SummarizeDocs(ctx, docs, schema,
    ai.WithPrompt("Extract key technical specifications from the documents"),
    ai.WithDocumentRenderer("{{title}}: {{article_url}}"),
)
```

## Benefits

- ✅ **Automatic content extraction**: No manual preprocessing needed
- ✅ **Schema-driven**: Only process fields marked as links
- ✅ **Template-aware**: Only download referenced fields
- ✅ **Security-first**: Multiple validation layers prevent SSRF/abuse
- ✅ **Extensible**: Custom ContentProcessor for new types
- ✅ **Multi-format**: HTML, PDF, images supported out of the box
- ✅ **Fail-safe**: Clear error handling prevents bad summaries

## Implementation

- **Content processing**: `lib/ai/contentutils.go` (ContentProcessor system)
- **Template parsing**: `lib/template/extractor.go` (raymond AST extraction)
- **Schema utilities**: `src/store/indexes/schemautils.go` (type checking, traversal)
- **Integration**: `lib/ai/genkit.go` (SummarizeDocs refactoring)
- **Configuration**: `src/common/config.go` (ContentSecurityConfig)

## Future Enhancements

- Video content processing (extract frames, transcripts)
- Audio processing (transcription)
- Document format support (DOCX, TXT, Markdown)
- Caching layer for frequently accessed links
- Async/background processing for large downloads

## References

- ContentProcessor: `lib/ai/contentutils.go`
- Template extraction: `lib/template/extractor.go`
- Schema utilities: `src/store/indexes/schemautils.go`
- Security config: `src/common/config.go`
- Integration: `lib/ai/genkit.go` (generateMultimodalSummary)
