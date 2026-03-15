# Reader Interface for OCR/Vision Models in libaf

## Context

The epstein example (`examples/epstein/main.go`) implements OCR support ad-hoc using Termite's `/read` and `/generate` endpoints. This works but isn't reusable — every application that needs OCR (docsaf PDF processing, Google Drive image files, S3 scanned documents) would need to reimplement the same integration. We need a `Reader` interface in `libaf/reading/` following the same pattern as `libaf/embeddings/`, `libaf/chunking/`, and `libaf/reranking/`.

## Design Summary

- **`libaf/reading/reader.go`** — Reader interface: `Read(ctx, []ai.BinaryContent, *ReadOptions) ([]string, error)` + `Close() error`. Includes `ReadPages` convenience helper and `FallbackReader` compositor.
- **Termite implementations** — `NewReader` (wraps `/read` for OCR models like TrOCR, Florence-2) and `NewGeneratorReader` (wraps `/generate` for vision LLMs like Gemma3). Both satisfy Reader.
- **`docsaf/pdf.go`** — PDFProcessor gets optional `OCR reading.Reader` field. Renders pages to PNG when text extraction quality is poor, calls Reader.
- **`docsaf/image.go`** — New ImageProcessor that delegates to Reader for image files from any source.
- **`docsaf/ocr_quality.go`** — Quality detection utilities extracted from epstein (`NeedsOCRFallback`, `HasGarbledPatterns`, etc.). Separate from Reader.

## Implementation Steps

### Step 1: Create `libaf/reading/reader.go`

New file at `antfly-go/libaf/reading/reader.go`:

```go
package reading

import (
    "context"
    "github.com/antflydb/antfly-go/libaf/ai"
)

type Reader interface {
    Read(ctx context.Context, pages []ai.BinaryContent, opts *ReadOptions) ([]string, error)
    Close() error
}

type ReadOptions struct {
    Prompt    string
    MaxTokens int
}

func ReadPages(ctx context.Context, r Reader, pages [][]byte, mimeType string, opts *ReadOptions) ([]string, error) {
    contents := make([]ai.BinaryContent, len(pages))
    for i, p := range pages {
        contents[i] = ai.BinaryContent{MIMEType: mimeType, Data: p}
    }
    return r.Read(ctx, contents, opts)
}
```

### Step 2: Create `libaf/reading/fallback.go`

`FallbackReader` that tries multiple Readers in order, returning first non-empty result:

```go
type FallbackReader struct {
    readers []Reader
}

func NewFallbackReader(readers ...Reader) *FallbackReader
func (f *FallbackReader) Read(ctx context.Context, pages []ai.BinaryContent, opts *ReadOptions) ([]string, error)
func (f *FallbackReader) Close() error
```

### Step 3: Create Termite Reader implementations

Location TBD — either `libaf/reading/termite/` or alongside Termite client. Two constructors:

- `NewReader(client, model)` — calls Termite `/read` endpoint (OCR models: trocr-base-printed, florence-2, donut, etc.)
- `NewGeneratorReader(client, model)` — calls Termite `/generate` endpoint with multimodal messages (vision LLMs: gemma3, etc.)

Key files to reference:
- Termite client: `termite/pkg/client/` — `ReadImagesWithResponse()`, `GenerateContentWithResponse()`
- Epstein OCR impl: `examples/epstein/main.go:92-242` — `ProcessPage()`, `ReadPageWithPrompt()`, `GeneratePageWithPrompt()`
- Content parts: `libaf/ai/content.go` — `BinaryContent`, `TextContent`

### Step 4: Create `docsaf/ocr_quality.go`

Extract quality detection from epstein into docsaf utilities:

- `NeedsOCRFallback(text string, minContentLen int) bool`
- `HasGarbledPatterns(text string) bool`
- `HasFontEncodingCorruption(repair *TextRepair, text string) bool`
- `CountReplacementChars(text string) int`

Reference: `examples/epstein/main.go:245-270` (needsOCRFallback), `main.go:2282-2402` (detection functions)

Reuse: `docsaf/text_repair.go` already has `TextRepair` with `IsFontEncodingCorrupted()` — integrate with that.

### Step 5: Add OCR fallback to `docsaf/pdf.go`

Add fields to `PDFProcessor`:

```go
OCR           reading.Reader  // nil = no OCR fallback
OCRMinContent int             // default 50
OCRRenderDPI  float64         // default 150
```

Per-page processing flow:
1. Extract text (existing)
2. Text repair (existing)
3. If `OCR != nil && NeedsOCRFallback(text, OCRMinContent)` → render page to PNG at OCRRenderDPI → `OCR.Read()` → use OCR text
4. Add `"extraction_method": "ocr"` or `"text_stream"` to section metadata

Dependency: `github.com/ajroetker/pdf/render` for PDF→PNG rendering (already used by epstein).

### Step 6: Create `docsaf/image.go`

New `ImageProcessor` implementing `ContentProcessor`:

```go
type ImageProcessor struct {
    Reader reading.Reader
}

func (p *ImageProcessor) CanProcess(contentType, path string) bool  // image/png, image/jpeg, image/tiff
func (p *ImageProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error)
```

Returns single `DocumentSection` with OCR-extracted text, type `"image"`.

### Step 7: Register ImageProcessor in docsaf registry

Update `docsaf/registry.go` — add a new factory function or update `DefaultRegistry` to optionally include ImageProcessor when a Reader is provided.

### Step 8: Update docsaf go.mod

Add dependencies:
- `golang.org/x/time` (if not already present)
- `github.com/ajroetker/pdf/render` for PDF page rendering

### Step 9: Tests

- `libaf/reading/reader_test.go` — test ReadPages helper, FallbackReader logic
- `docsaf/ocr_quality_test.go` — test quality detection functions with known garbled/clean text
- `docsaf/image_test.go` — test ImageProcessor with mock Reader
- `docsaf/pdf_test.go` — add test for OCR fallback path with mock Reader

## Verification

1. `cd antfly-go/libaf && go build ./reading/...` — reading package compiles
2. `cd antfly-go/libaf && go test ./reading/...` — unit tests pass
3. `cd antfly-go/docsaf && go test ./...` — all docsaf tests pass including new OCR quality and image processor tests
4. Manual: run epstein example refactored to use `reading.NewFallbackReader` instead of ad-hoc OCR client — verify same behavior

## Files to Create

- `antfly-go/libaf/reading/reader.go`
- `antfly-go/libaf/reading/fallback.go`
- `antfly-go/libaf/reading/reader_test.go`
- `antfly-go/docsaf/ocr_quality.go`
- `antfly-go/docsaf/ocr_quality_test.go`
- `antfly-go/docsaf/image.go`
- `antfly-go/docsaf/image_test.go`

## Files to Modify

- `antfly-go/docsaf/pdf.go` — add OCR fallback fields and logic
- `antfly-go/docsaf/pdf_test.go` — add OCR fallback tests
- `antfly-go/docsaf/registry.go` — ImageProcessor registration
- `antfly-go/docsaf/go.mod` — new dependencies
- `antfly-go/libaf/go.mod` — if reading/ needs new deps (unlikely)

## Termite implementation files (TBD on exact location)

- `NewReader` wrapping Termite `/read`
- `NewGeneratorReader` wrapping Termite `/generate`
