package docsaf

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"strings"

	"github.com/ajroetker/pdf"
	"github.com/ajroetker/pdf/render"
	"github.com/antflydb/antfly/pkg/libaf/ai"
	"github.com/antflydb/antfly/pkg/libaf/reading"
)

// PDFProgress reports progress during PDF processing.
type PDFProgress struct {
	Phase      string // "header_detection", "extraction"
	Page       int    // Current page number (1-based)
	TotalPages int    // Total pages in document
	FilePath   string // Path to the PDF file
}

// PDFProgressFunc is called to report PDF processing progress.
// Return an error to abort processing.
type PDFProgressFunc func(progress PDFProgress) error

// PDFProcessor processes PDF (.pdf) content using the ledongthuc/pdf library.
// It chunks content into sections by pages and extracts metadata from the PDF Info dictionary.
type PDFProcessor struct {
	textRepair *TextRepair

	// EnableHeaderFooterDetection enables cross-page header/footer detection.
	// When enabled, the processor makes two passes: first to detect patterns,
	// then to extract text with headers/footers removed.
	EnableHeaderFooterDetection bool

	// EnableMirroredTextRepair enables automatic detection and repair of mirrored/reversed text.
	// Uses bigram frequency analysis to detect text that has been horizontally flipped.
	EnableMirroredTextRepair bool

	// ProgressFunc is called to report processing progress.
	// If nil, no progress is reported.
	ProgressFunc PDFProgressFunc

	// ProgressInterval controls how often ProgressFunc is called.
	// If 0, defaults to every 100 pages.
	ProgressInterval int

	// OCR is an optional Reader for fallback OCR on pages with poor text extraction.
	// When nil, no OCR fallback is attempted.
	OCR reading.Reader

	// OCRMinContent is the minimum character count before OCR fallback triggers.
	// Default: 50 (DefaultOCRMinContent).
	OCRMinContent int

	// OCRRenderDPI is the DPI for rendering PDF pages to images for OCR.
	// Default: 150 (DefaultOCRRenderDPI).
	OCRRenderDPI float64
}

func (pp *PDFProcessor) ocrMinContent() int {
	if pp.OCRMinContent > 0 {
		return pp.OCRMinContent
	}
	return DefaultOCRMinContent
}

func (pp *PDFProcessor) ocrRenderDPI() float64 {
	if pp.OCRRenderDPI > 0 {
		return pp.OCRRenderDPI
	}
	return DefaultOCRRenderDPI
}

// ocrFallback renders a PDF page to PNG and runs OCR on it.
// Returns the OCR text and true if OCR was used, or empty string and false.
func (pp *PDFProcessor) ocrFallback(pdfData []byte, pageNum int) (string, bool) {
	if pp.OCR == nil {
		return "", false
	}

	renderer, err := render.NewRenderer(pdfData)
	if err != nil {
		return "", false
	}
	defer renderer.Close() //nolint:errcheck // best-effort close in deferred cleanup

	pngBytes, err := renderer.RenderPageToPNG(pageNum, pp.ocrRenderDPI())
	if err != nil {
		return "", false
	}

	results, err := pp.OCR.Read(context.TODO(), []ai.BinaryContent{
		{MIMEType: "image/png", Data: pngBytes},
	}, nil)
	if err != nil || len(results) == 0 {
		return "", false
	}

	text := strings.TrimSpace(results[0])
	if text == "" {
		return "", false
	}
	return text, true
}

func (pp *PDFProcessor) getTextRepair() *TextRepair {
	if pp.textRepair == nil {
		pp.textRepair = NewTextRepair()
	}
	return pp.textRepair
}

func (pp *PDFProcessor) progressInterval() int {
	if pp.ProgressInterval > 0 {
		return pp.ProgressInterval
	}
	return 100 // default: report every 100 pages
}

func (pp *PDFProcessor) reportProgress(phase string, page, total int, path string) error {
	if pp.ProgressFunc == nil {
		return nil
	}
	interval := pp.progressInterval()
	// Report on first page, last page, and at intervals
	if page == 1 || page == total || page%interval == 0 {
		return pp.ProgressFunc(PDFProgress{
			Phase:      phase,
			Page:       page,
			TotalPages: total,
			FilePath:   path,
		})
	}
	return nil
}

// CanProcess returns true for PDF content types or .pdf extensions.
func (pp *PDFProcessor) CanProcess(contentType, path string) bool {
	if strings.Contains(contentType, "application/pdf") {
		return true
	}
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".pdf")
}

// Process processes PDF content and returns document sections.
// Each page becomes a separate section, with text extracted via GetTextByRow()
// for better handling of tables and complex layouts.
func (pp *PDFProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error) {
	// Create a reader from bytes
	reader, err := pdf.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return nil, fmt.Errorf("failed to read PDF: %w", err)
	}

	// Extract document-level metadata
	docMetadata := pp.extractMetadata(reader, path)
	if sourceURL != "" {
		docMetadata["source_url"] = sourceURL
	}

	totalPages := reader.NumPage()
	textRepair := pp.getTextRepair()

	// If header/footer detection is enabled, make a first pass to collect page patterns
	var headers, footers []string
	if pp.EnableHeaderFooterDetection && totalPages >= 3 {
		for pageNum := 1; pageNum <= totalPages; pageNum++ {
			if err := pp.reportProgress("header_detection", pageNum, totalPages, path); err != nil {
				return nil, err
			}
			page := reader.Page(pageNum)
			if page.V.IsNull() {
				continue
			}
			pageContent, _ := page.GetPlainText(nil)
			textRepair.RecordPageContent(pageContent)
		}
		headers = textRepair.GetDetectedHeaders()
		footers = textRepair.GetDetectedFooters()
	}

	// Process each page
	sections := make([]DocumentSection, 0, totalPages)

	for pageNum := 1; pageNum <= totalPages; pageNum++ {
		if err := pp.reportProgress("extraction", pageNum, totalPages, path); err != nil {
			return nil, err
		}
		page := reader.Page(pageNum)
		if page.V.IsNull() {
			continue
		}

		// Use GetPlainText for reliable word boundary detection
		// Layout analysis was introducing word fragmentation artifacts
		pageContent, _ := page.GetPlainText(nil)

		// Decode PUA-preserved bytes from custom font encodings
		// The pdf library stores unmapped font bytes in U+E000-U+E0FF to preserve them.
		// This step detects the encoding shift and decodes them to readable text.
		if HasPUAChars(pageContent) {
			pageContent, _ = textRepair.AutoDecodePUA(pageContent)
		}

		// Remove detected headers and footers
		if len(headers) > 0 || len(footers) > 0 {
			pageContent = textRepair.RemoveHeadersFooters(pageContent, headers, footers)
		}

		pageContent = stripGarbledHeaders(pageContent)

		// Repair mirrored/reversed text if enabled
		if pp.EnableMirroredTextRepair {
			pageContent, _ = textRepair.AutoRepairMirroredText(pageContent)
		}

		// Note: SegmentWords was removed - it was designed for layout-analyzed text
		// with merged words, but GetPlainText already handles word boundaries correctly

		// OCR fallback: if text extraction produced poor results, try OCR
		extractionMethod := "text_stream"
		if pp.OCR != nil && NeedsOCRFallback(pageContent, pp.ocrMinContent()) {
			if ocrText, used := pp.ocrFallback(content, pageNum); used {
				pageContent = ocrText
				extractionMethod = "ocr"
			}
		}

		if pageContent == "" {
			continue
		}

		title := pp.getSectionTitle(pageNum, docMetadata)

		url := ""
		if baseURL != "" {
			cleanPath := transformPDFPath(path)
			url = fmt.Sprintf("%s/%s#page-%d", baseURL, cleanPath, pageNum)
		}

		section := DocumentSection{
			ID:       generateID(path, fmt.Sprintf("page_%d", pageNum)),
			FilePath: path,
			Title:    title,
			Content:  pageContent,
			Type:     "pdf_page",
			URL:      url,
			Metadata: pp.mergeSectionMetadata(docMetadata, map[string]any{
				"page_number":       pageNum,
				"total_pages":       totalPages,
				"extraction_method": extractionMethod,
			}),
		}

		sections = append(sections, section)
	}

	return sections, nil
}

// extractMetadata extracts metadata from the PDF Info dictionary.
func (pp *PDFProcessor) extractMetadata(reader *pdf.Reader, path string) map[string]any {
	metadata := make(map[string]any)

	trailer := reader.Trailer()
	if trailer.IsNull() {
		metadata["title"] = filepath.Base(path)
		return metadata
	}

	info := trailer.Key("Info")
	if info.IsNull() {
		metadata["title"] = filepath.Base(path)
		return metadata
	}

	if title := info.Key("Title"); !title.IsNull() {
		if titleStr := title.Text(); titleStr != "" {
			metadata["title"] = titleStr
		}
	}

	if author := info.Key("Author"); !author.IsNull() {
		if authorStr := author.Text(); authorStr != "" {
			metadata["author"] = authorStr
		}
	}

	if subject := info.Key("Subject"); !subject.IsNull() {
		if subjectStr := subject.Text(); subjectStr != "" {
			metadata["subject"] = subjectStr
		}
	}

	if keywords := info.Key("Keywords"); !keywords.IsNull() {
		if keywordsStr := keywords.Text(); keywordsStr != "" {
			metadata["keywords"] = keywordsStr
		}
	}

	if creator := info.Key("Creator"); !creator.IsNull() {
		if creatorStr := creator.Text(); creatorStr != "" {
			metadata["creator"] = creatorStr
		}
	}

	if producer := info.Key("Producer"); !producer.IsNull() {
		if producerStr := producer.Text(); producerStr != "" {
			metadata["producer"] = producerStr
		}
	}

	if creationDate := info.Key("CreationDate"); !creationDate.IsNull() {
		if dateStr := creationDate.Text(); dateStr != "" {
			metadata["creation_date"] = dateStr
		}
	}

	if modDate := info.Key("ModDate"); !modDate.IsNull() {
		if dateStr := modDate.Text(); dateStr != "" {
			metadata["mod_date"] = dateStr
		}
	}

	if _, hasTitle := metadata["title"]; !hasTitle {
		metadata["title"] = filepath.Base(path)
	}

	return metadata
}

func (pp *PDFProcessor) getSectionTitle(pageNum int, docMetadata map[string]any) string {
	docTitle := "Document"
	if title, ok := docMetadata["title"].(string); ok && title != "" {
		docTitle = title
	}
	return fmt.Sprintf("%s - Page %d", docTitle, pageNum)
}

func (pp *PDFProcessor) mergeSectionMetadata(docMeta, sectionMeta map[string]any) map[string]any {
	merged := make(map[string]any)
	maps.Copy(merged, docMeta)
	maps.Copy(merged, sectionMeta)
	return merged
}

func transformPDFPath(path string) string {
	return strings.TrimSuffix(path, ".pdf")
}

// stripGarbledHeaders removes ROT3-encoded or otherwise garbled headers
// that appear in some PDFs (e.g., court reporter watermarks).
// These appear as random-looking letter sequences with spaces between characters.
func stripGarbledHeaders(text string) string {
	// First, try to strip garbled prefix from beginning of text
	text = stripGarbledPrefix(text)

	// Then filter out entirely garbled lines
	lines := strings.Split(text, "\n")
	var result []string

	for _, line := range lines {
		if isGarbledLine(line) {
			continue // skip this line
		}
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

// stripGarbledPrefix removes garbled text from the start of content
// Pattern: sequences of single chars with spaces like "$ J U H Q % O D Q G R"
func stripGarbledPrefix(text string) string {
	// Look for pattern of single-char words at start
	// e.g., "$ J U H Q % O D Q G R ... actual content"
	words := strings.Fields(text)
	if len(words) < 10 {
		return text
	}

	// Count consecutive single-character words at start
	singleCharRun := 0
	for _, word := range words {
		if len(word) == 1 || (len(word) <= 3 && containsOnlySpecialChars(word)) {
			singleCharRun++
		} else {
			break
		}
	}

	// If we found a significant run of single chars (>10), strip them
	if singleCharRun >= 10 {
		// Find the position after the garbled prefix
		// We'll skip past the single-char words
		remaining := words[singleCharRun:]
		return strings.Join(remaining, " ")
	}

	return text
}

// containsOnlySpecialChars checks if string has only special/symbol chars
func containsOnlySpecialChars(s string) bool {
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			if len(s) > 1 {
				return false
			}
		}
	}
	return true
}

// isGarbledLine detects lines that appear to be garbled/encoded text
func isGarbledLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) < 10 {
		return false
	}

	// Count single-character "words" (like "J U H Q")
	words := strings.Fields(trimmed)
	if len(words) < 5 {
		return false
	}

	singleCharCount := 0
	replacementCount := 0

	for _, word := range words {
		if len(word) == 1 {
			singleCharCount++
		}
		for _, r := range word {
			if r == '\uFFFD' || r == '�' || IsPUAChar(r) {
				replacementCount++
			}
		}
	}

	// If >60% of words are single characters, likely garbled
	singleCharRatio := float64(singleCharCount) / float64(len(words))
	if singleCharRatio > 0.6 {
		return true
	}

	// If many replacement characters, likely garbled
	if replacementCount > 3 {
		return true
	}

	return false
}
