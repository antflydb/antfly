# Screenshots Shots Shots

Search your local screenshots, PDFs, and images with natural language using CLIP embeddings.

Point it at a folder of screenshots and ask questions like "terminal with error message" or "login page" — Antfly + CLIP will find the most visually relevant matches.

## Prerequisites

- Antfly running with Antfly inference and ONNX Runtime
- CLIP model: `antfly inference pull openai/clip-vit-base-patch32`

## Supported File Types

Images: `.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`, `.bmp`, `.tiff`
Documents: `.pdf`

## Usage

```bash
# Search your Desktop screenshots with default queries
go run ./examples/screenshots-shots-shots ~/Desktop

# Search with custom queries
go run ./examples/screenshots-shots-shots ~/Screenshots "error message" "login page" "dark mode UI"

# Search current directory
go run ./examples/screenshots-shots-shots .
```

## How It Works

<!-- include: main.go#create_table -->

1. **Scans** the target folder for supported image and PDF files
2. **Inserts** lightweight documents with `file://` URLs — no image data is stored in Antfly
3. **`remoteMedia`** fetches file content from disk at enrichment time, so CLIP can embed it
4. **CLIP embeds** both the image pixels and filename for multimodal search
5. **Queries** the index with your natural language search terms

The template `{{remoteMedia url=file_url}}{{filename}}` tells the enricher to read the file via its `file://` URL when generating embeddings. If a file is missing or unreadable, the error is detected and the document is skipped gracefully.

<!-- include: main.go#ingest -->

### Searching

<!-- include: main.go#search -->

Example output:

```
Query: "terminal with code"
  0.0312  2024-03-15-terminal.png
  0.0287  vscode-debug-session.png
  0.0251  iterm2-split-panes.png

Query: "chart or graph"
  0.0298  quarterly-revenue.pdf
  0.0276  dashboard-screenshot.png
```

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `ANTFLY_URL` | `http://localhost:8080/db/v1` | Antfly API endpoint |

## Tips

- **No data stored in Antfly.** Only the file path and metadata are stored. The enricher reads files from disk via `file://` URLs at embedding time.
- **Files must stay in place.** Since Antfly references files by path, moving or deleting them will cause enrichment to fail (gracefully — the error directive system marks them as permanent failures).
- **Filenames help.** The embedding template includes the filename, so descriptive names like `error-dialog.png` improve search quality.
- **PDFs are supported** but only the first page is embedded by CLIP. For full-text PDF search, combine with a text index.
- **Re-run is safe.** Documents are inserted by ID derived from the file path, so re-running updates existing entries.

## Related

- [Image Search Example](../image-search/) — Search remote image collections with CLIP
- [Multimodal Guide](/docs/guides/multimodal) — PDFs, audio, and remote content
- [Inference Models](/inference) — Available CLIP variants
