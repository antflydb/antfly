# DOCX, PPTX, Google Docs & Google Slides Support for docsaf

## Context

The docsaf package currently handles Markdown, HTML, PDF, OpenAPI, and images. The Google Drive source exports Google Docs and Slides as `text/plain`, which loses all document structure (headings, slide boundaries). Native Office formats (.docx, .pptx) aren't supported at all. This plan adds structured extraction for all four types using only the standard library.

## Approach

**Google Workspace** — change export formats so existing processors handle them:
- Google Docs → export as `text/html` → existing `HTMLProcessor` chunks by headings
- Google Slides → export as `application/pdf` → existing `PDFProcessor` chunks by page/slide

**Native Office formats** — new processors using `archive/zip` + `encoding/xml` (no external deps):
- `.docx` → `DocxProcessor` — heading-aware chunking (like MarkdownProcessor/HTMLProcessor)
- `.pptx` → `PptxProcessor` — per-slide sections (like PDFProcessor pages)

## Implementation Steps

### Step 1: Change Google Drive export formats

**File**: `docsaf/source_googledrive.go`

Change `workspaceExportFormats` map:
```go
// Before:
"application/vnd.google-apps.document":     "text/plain",
"application/vnd.google-apps.presentation": "text/plain",

// After:
"application/vnd.google-apps.document":     "text/html",
"application/vnd.google-apps.presentation": "application/pdf",
```

Update matching assertions in `docsaf/source_googledrive_test.go`.

### Step 2: Create shared OOXML helpers

**New file**: `docsaf/ooxml.go`

Both .docx and .pptx are ZIP archives with `docProps/core.xml` for metadata. Shared helpers:

- `readZipFile(zr *zip.Reader, name string) ([]byte, error)` — read a named file from ZIP
- `extractOOXMLMetadata(zr *zip.Reader, path string) map[string]any` — parse Dublin Core metadata (title, creator, subject, keywords, dates) from `docProps/core.xml`

### Step 3: Implement DocxProcessor

**New file**: `docsaf/docx.go`

DOCX XML structure (`word/document.xml`):
- `<w:p>` — paragraphs
- `<w:p>/<w:pPr>/<w:pStyle w:val="Heading1"/>` — heading level
- `<w:p>/<w:r>/<w:t>` — text runs

Processing:
1. Open as ZIP, read `word/document.xml`
2. Walk paragraphs, detect heading styles (`Heading1`–`Heading9`, case-insensitive)
3. Chunk by headings using a heading stack (same pattern as `html.go`)
4. If no headings, return single section with filename as title
5. Extract metadata from `docProps/core.xml` via shared helper

Section details:
- Type: `"docx_section"`
- URL anchor: `#heading-slug`
- Metadata: doc-level (title, author, dates) merged with section-level (heading_level)
- Uses existing `generateID`, `generateSlug` from `markdown.go`

**New file**: `docsaf/docx_test.go`

Build minimal DOCX ZIPs in-memory for testing:
- CanProcess (MIME type + extension)
- Process with headings (multiple sections, correct hierarchy)
- Process without headings (single section fallback)
- Metadata extraction from core.xml
- URL generation
- Invalid/missing content error handling

### Step 4: Implement PptxProcessor

**New file**: `docsaf/pptx.go`

PPTX XML structure:
- `ppt/slides/slideN.xml` — one file per slide
- `<p:sld>/<p:cSld>/<p:spTree>/<p:sp>/<p:txBody>/<a:p>/<a:r>/<a:t>` — text in shapes
- `ppt/notesSlides/notesSlideN.xml` — speaker notes (same shape/text structure)

Processing:
1. Open as ZIP, enumerate `ppt/slides/slide*.xml`, sort by number
2. For each slide, extract text from all shapes
3. Optionally extract speaker notes from matching `notesSlide*.xml`
4. One `DocumentSection` per slide (like PDFProcessor)
5. Extract metadata from `docProps/core.xml`

Section details:
- Type: `"pptx_slide"`
- Title: `"{DocTitle} - Slide {N}"` (same pattern as PDFProcessor)
- URL anchor: `#slide-N`
- Metadata: `slide_number`, `total_slides`, plus doc-level metadata
- Speaker notes appended to content with separator when present

Config field:
- `IncludeNotes bool` — whether to include speaker notes (default true)

**New file**: `docsaf/pptx_test.go`

Build minimal PPTX ZIPs in-memory:
- CanProcess (MIME type + extension)
- Process with multiple slides (correct count, order, content)
- Process with speaker notes
- Slide ordering (numeric sort regardless of ZIP entry order)
- Metadata extraction
- URL generation with `#slide-N` anchors
- Empty/invalid content handling

### Step 5: Register processors and update content type detection

**Modify**: `docsaf/registry.go`
- Register `&DocxProcessor{}` and `&PptxProcessor{}` in `DefaultRegistry()` after PDFProcessor

**Modify**: `docsaf/markdown.go` (in `DetectContentType` switch)
- Add `.docx` → `application/vnd.openxmlformats-officedocument.wordprocessingml.document`
- Add `.pptx` → `application/vnd.openxmlformats-officedocument.presentationml.presentation`

## Files Summary

**Create**:
- `docsaf/ooxml.go` — shared ZIP/metadata helpers
- `docsaf/docx.go` — DocxProcessor
- `docsaf/docx_test.go`
- `docsaf/pptx.go` — PptxProcessor
- `docsaf/pptx_test.go`

**Modify**:
- `docsaf/source_googledrive.go` — export format changes
- `docsaf/source_googledrive_test.go` — update test assertions
- `docsaf/registry.go` — register new processors
- `docsaf/markdown.go` — add MIME types to DetectContentType

**No new dependencies** — uses only `archive/zip` and `encoding/xml` from stdlib.

## Verification

```bash
cd /Users/ajroetker/go/src/github.com/antflydb/antfly2/antfly-go
GOEXPERIMENT=simd go test ./docsaf/...
GOEXPERIMENT=simd go build ./docsaf/...
```
