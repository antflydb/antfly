package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajroetker/pdf"
	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/docsaf"
	generatingreading "github.com/antflydb/antfly/pkg/generating/reading"
	libai "github.com/antflydb/antfly/pkg/libaf/ai"
	libreading "github.com/antflydb/antfly/pkg/libaf/reading"
	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
)

// SplitMetadata stores document-level metadata extracted during PDF splitting.
// This is saved as metadata.json alongside the split page PDFs to preserve
// information needed when parsing individual pages.
type SplitMetadata struct {
	SourceFile      string   `json:"source_file"`
	TotalPages      int      `json:"total_pages"`
	Title           string   `json:"title,omitempty"`
	Author          string   `json:"author,omitempty"`
	Subject         string   `json:"subject,omitempty"`
	Keywords        string   `json:"keywords,omitempty"`
	Creator         string   `json:"creator,omitempty"`
	Producer        string   `json:"producer,omitempty"`
	CreationDate    string   `json:"creation_date,omitempty"`
	ModDate         string   `json:"mod_date,omitempty"`
	DetectedHeaders []string `json:"detected_headers,omitempty"`
	DetectedFooters []string `json:"detected_footers,omitempty"`
}

// OCRConfig holds configuration for OCR fallback when text extraction fails.
type OCRConfig struct {
	Enabled       bool     // Enable OCR fallback
	TermiteURL    string   // Termite service URL (e.g., "http://localhost:8082")
	Models        []string // OCR models to try in order (e.g., ["trocr-base-printed", "florence-2"])
	MinContentLen int      // Trigger OCR if extracted text is shorter than this
	RenderDPI     float64  // DPI for rendering PDF pages to images (default 150)
}

// OCRClient wraps the Termite client for OCR operations.
type OCRClient struct {
	readReader    *generatingreading.TermiteReadReader
	visionReader  *generatingreading.TermiteGenerateReader
	renderDPI     float64
	lastUsedModel string
}

// NewOCRClient creates a new OCR client connected to Termite.
func NewOCRClient(termiteURL string, models []string, renderDPI float64) (*OCRClient, error) {
	cfg := generatingreading.TermiteConfig{
		BaseURL:          termiteURL,
		Models:           models,
		DefaultMaxTokens: 2048,
		RenderDPI:        renderDPI,
	}

	readReader, err := generatingreading.NewTermiteReadReader(cfg)
	if err != nil {
		return nil, fmt.Errorf("create termite read reader: %w", err)
	}

	visionReader, err := generatingreading.NewTermiteGenerateReader(cfg)
	if err != nil {
		return nil, fmt.Errorf("create termite generate reader: %w", err)
	}

	return &OCRClient{
		readReader:   readReader,
		visionReader: visionReader,
		renderDPI:    cfg.RenderDPI,
	}, nil
}

// ProcessPage attempts OCR on a PDF page if text extraction produced poor results.
// Returns the text (either original or OCR'd), whether OCR was used, and any error.
func (o *OCRClient) ProcessPage(ctx context.Context, pdfData []byte, pageNum int, extractedText string, minContentLen int) (string, bool, error) {
	// Check if OCR is needed
	if !needsOCRFallback(extractedText, minContentLen) {
		return extractedText, false, nil
	}

	page, err := generatingreading.RenderPDFPage(pdfData, pageNum, o.renderDPI)
	if err != nil {
		return extractedText, false, fmt.Errorf("render page: %w", err)
	}

	results, err := o.readReader.ReadDetailed(ctx, []libai.BinaryContent{page}, &libreading.ReadOptions{
		MaxTokens: 2048,
	})
	if err != nil {
		return extractedText, false, err
	}
	if len(results) == 0 {
		return extractedText, false, nil
	}

	ocrText := results[0].Text
	if ocrText != "" && len(ocrText) > len(extractedText) {
		o.lastUsedModel = results[0].Model
		return ocrText, true, nil
	}

	return extractedText, false, nil
}

// LastUsedModel returns the model that was used for the last successful OCR.
func (o *OCRClient) LastUsedModel() string {
	return o.lastUsedModel
}

// ReadPageWithPrompt renders a single-page PDF and sends to Termite with a custom prompt and model.
// Returns (text, model_used, error). The page PDF is expected to be a single-page document
// (as produced by split-pages mode).
func (o *OCRClient) ReadPageWithPrompt(ctx context.Context, pdfData []byte, prompt string, maxTokens int) (string, string, error) {
	page, err := generatingreading.RenderPDFPage(pdfData, 1, o.renderDPI)
	if err != nil {
		return "", "", fmt.Errorf("create renderer: %w", err)
	}

	results, err := o.readReader.ReadDetailed(ctx, []libai.BinaryContent{page}, &libreading.ReadOptions{
		Prompt:    prompt,
		MaxTokens: maxTokens,
	})
	if err != nil {
		return "", "", err
	}
	if len(results) > 0 && results[0].Text != "" {
		return results[0].Text, results[0].Model, nil
	}

	return "", "", fmt.Errorf("all models failed to produce text")
}

// GeneratePageWithPrompt renders a single-page PDF and sends it to Termite's generate
// endpoint (e.g. Gemma 3 vision) instead of the reader endpoint. Used for vision captioning
// of image pages. Returns (text, model_used, error).
func (o *OCRClient) GeneratePageWithPrompt(ctx context.Context, pdfData []byte, prompt string, maxTokens int) (string, string, error) {
	page, err := generatingreading.RenderPDFPage(pdfData, 1, o.renderDPI)
	if err != nil {
		return "", "", fmt.Errorf("create renderer: %w", err)
	}
	results, err := o.visionReader.ReadDetailed(ctx, []libai.BinaryContent{page}, &libreading.ReadOptions{
		Prompt:    prompt,
		MaxTokens: maxTokens,
	})
	if err != nil {
		return "", "", err
	}
	if len(results) > 0 && results[0].Text != "" {
		return results[0].Text, results[0].Model, nil
	}

	return "", "", fmt.Errorf("all models failed to produce text")
}

// needsOCRFallback determines if OCR should be attempted based on extracted text quality.
func needsOCRFallback(content string, minContentLen int) bool {
	content = strings.TrimSpace(content)

	// Check minimum content length
	if len(content) < minContentLen {
		return true
	}

	// Check for garbled text patterns
	if hasGarbledPatterns(content) {
		return true
	}

	// Check for font encoding corruption
	if hasFontEncodingCorruption(content) {
		return true
	}

	// Check replacement character ratio (encoding issues)
	replacementCount := countReplacementChars(content)
	if len(content) > 0 && float64(replacementCount)/float64(len(content)) > 0.05 {
		return true
	}

	return false
}

//go:embed templates/*
var templatesFS embed.FS

// Archive.org download URLs for Epstein documents
const (
	// January 2024 Court Unsealing - Giuffre v. Maxwell (943 pages, ~23MB PDF)
	Jan2024CourtDocsURL = "https://archive.org/download/final-epstein-documents/Final_Epstein_documents.pdf"

	// DOJ Complete Release - December 2025 (8 consolidated PDFs, ~4.8GB total)
	DOJDataSet1URL = "https://archive.org/download/combined-all-epstein-files/DataSet_1_COMPLETE.pdf"
	DOJDataSet2URL = "https://archive.org/download/combined-all-epstein-files/DataSet_2_COMPLETE.pdf"
	DOJDataSet3URL = "https://archive.org/download/combined-all-epstein-files/DataSet_3_COMPLETE.pdf"
	DOJDataSet4URL = "https://archive.org/download/combined-all-epstein-files/DataSet_4_COMPLETE.pdf"
	DOJDataSet5URL = "https://archive.org/download/combined-all-epstein-files/DataSet_5_COMPLETE.pdf"
	DOJDataSet6URL = "https://archive.org/download/combined-all-epstein-files/DataSet_6_COMPLETE.pdf"
	DOJDataSet7URL = "https://archive.org/download/combined-all-epstein-files/DataSet_7_COMPLETE.pdf"
	DOJDataSet8URL = "https://archive.org/download/combined-all-epstein-files/DataSet_8_COMPLETE.pdf"

	// Alternative: Single combined PDF of all DOJ datasets (6GB)
	DOJCombinedPDF = "https://archive.org/download/combined-all-epstein-files/COMBINED_ALL_EPSTEIN_FILES.pdf"

	// DOJ January 30, 2026 Release - Datasets 10-12 (ZIP archives of individual PDFs)
	// Dataset 9 is excluded: the DOJ release was incomplete and contained unredacted CSAM
	// that was subsequently removed. No reliable complete archive is available.
	DOJDataSet10URL = "https://archive.org/download/data-set-10/DataSet%2010.zip"
	DOJDataSet11URL = "https://archive.org/download/www.justice.gov_epstein_files_DataSet_11.zip/www.justice.gov_epstein_files_DataSet_11.zip"
	DOJDataSet12URL = "https://archive.org/download/data-set-12_202601/DataSet%2012.zip"
)

// DownloadConfig holds download configuration
type DownloadConfig struct {
	OutputDir string
	Dataset   string // "court-2024", "doj-complete", or "all"
}

// StringSliceFlag allows repeated flags to build a slice
type StringSliceFlag []string

func (s *StringSliceFlag) String() string {
	return strings.Join(*s, ", ")
}

func (s *StringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// downloadCmd downloads Epstein documents from archive.org
func downloadCmd(args []string) error {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	outputDir := fs.String("output", "./epstein-docs", "Output directory for downloaded files")
	dataset := fs.String("dataset", "court-2024", "Dataset to download: court-2024, doj-complete, or all")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	fmt.Printf("=== Epstein Documents Download ===\n")
	fmt.Printf("Output directory: %s\n", *outputDir)
	fmt.Printf("Dataset: %s\n\n", *dataset)

	// Create output directory
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	type downloadItem struct {
		URL      string
		Filename string
		IsZip    bool // ZIP archives are extracted after download
	}

	var urls []downloadItem

	switch *dataset {
	case "court-2024":
		urls = []downloadItem{
			{Jan2024CourtDocsURL, "court-2024-giuffre-v-maxwell.pdf", false},
		}
	case "doj-complete":
		urls = []downloadItem{
			{DOJDataSet1URL, "doj-dataset-1.pdf", false},
			{DOJDataSet2URL, "doj-dataset-2.pdf", false},
			{DOJDataSet3URL, "doj-dataset-3.pdf", false},
			{DOJDataSet4URL, "doj-dataset-4.pdf", false},
			{DOJDataSet5URL, "doj-dataset-5.pdf", false},
			{DOJDataSet6URL, "doj-dataset-6.pdf", false},
			{DOJDataSet7URL, "doj-dataset-7.pdf", false},
			{DOJDataSet8URL, "doj-dataset-8.pdf", false},
		}
	case "doj-jan2026":
		urls = []downloadItem{
			{DOJDataSet10URL, "doj-dataset-10.zip", true},
			{DOJDataSet11URL, "doj-dataset-11.zip", true},
			{DOJDataSet12URL, "doj-dataset-12.zip", true},
		}
	case "all":
		urls = []downloadItem{
			{Jan2024CourtDocsURL, "court-2024-giuffre-v-maxwell.pdf", false},
			{DOJDataSet1URL, "doj-dataset-1.pdf", false},
			{DOJDataSet2URL, "doj-dataset-2.pdf", false},
			{DOJDataSet3URL, "doj-dataset-3.pdf", false},
			{DOJDataSet4URL, "doj-dataset-4.pdf", false},
			{DOJDataSet5URL, "doj-dataset-5.pdf", false},
			{DOJDataSet6URL, "doj-dataset-6.pdf", false},
			{DOJDataSet7URL, "doj-dataset-7.pdf", false},
			{DOJDataSet8URL, "doj-dataset-8.pdf", false},
			{DOJDataSet10URL, "doj-dataset-10.zip", true},
			{DOJDataSet11URL, "doj-dataset-11.zip", true},
			{DOJDataSet12URL, "doj-dataset-12.zip", true},
		}
	default:
		return fmt.Errorf("unknown dataset: %s (valid: court-2024, doj-complete, doj-jan2026, all)", *dataset)
	}

	for i, item := range urls {
		outputPath := filepath.Join(*outputDir, item.Filename)

		if item.IsZip {
			// For ZIP datasets, check if the extraction directory already exists
			extractDir := filepath.Join(*outputDir, strings.TrimSuffix(item.Filename, ".zip"))
			if _, err := os.Stat(extractDir); err == nil {
				fmt.Printf("[%d/%d] Skipping %s (already extracted to %s)\n", i+1, len(urls), item.Filename, extractDir)
				continue
			}
		}

		// Skip if already exists
		if _, err := os.Stat(outputPath); err == nil {
			if !item.IsZip {
				fmt.Printf("[%d/%d] Skipping %s (already exists)\n", i+1, len(urls), item.Filename)
				continue
			}
			// ZIP exists but wasn't extracted yet; skip download, extract below
		} else {
			fmt.Printf("[%d/%d] Downloading %s...\n", i+1, len(urls), item.Filename)

			if err := downloadFile(item.URL, outputPath); err != nil {
				return fmt.Errorf("failed to download %s: %w", item.Filename, err)
			}

			// Get file size
			info, _ := os.Stat(outputPath)
			fmt.Printf("  Downloaded: %s (%.2f MB)\n", outputPath, float64(info.Size())/(1024*1024))
		}

		// Extract ZIP archives (keep the archive for --zip direct processing)
		if item.IsZip {
			extractDir := filepath.Join(*outputDir, strings.TrimSuffix(item.Filename, ".zip"))
			fmt.Printf("  Extracting %s to %s...\n", item.Filename, extractDir)
			extracted, err := extractZipPDFs(outputPath, extractDir)
			if err != nil {
				return fmt.Errorf("failed to extract %s: %w", item.Filename, err)
			}
			fmt.Printf("  Extracted %d PDF files\n", extracted)
		}
	}

	fmt.Printf("\n Download complete. Files saved to: %s\n", *outputDir)
	return nil
}

// downloadFile downloads a file from URL to the specified path with progress
func downloadFile(url, outputPath string) error {
	// Create temporary file
	tmpPath := outputPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer out.Close()

	// No overall timeout — these files can be tens of GBs.
	// Use transport-level timeouts so stalled connections still fail.
	client := &http.Client{
		Transport: &http.Transport{
			ResponseHeaderTimeout: 2 * time.Minute,
		},
	}

	resp, err := client.Get(url)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		os.Remove(tmpPath)
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	totalSize := resp.ContentLength // -1 if unknown

	// Copy with progress reporting
	pr := &progressReader{
		reader:    resp.Body,
		totalSize: totalSize,
		start:     time.Now(),
	}

	written, err := io.Copy(out, pr)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Clear progress line and print final summary
	fmt.Printf("\r  Done: %s in %s\n", formatBytes(written), time.Since(pr.start).Truncate(time.Second))

	out.Close()

	// Rename to final path
	if err := os.Rename(tmpPath, outputPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename file: %w", err)
	}
	return nil
}

// progressReader wraps an io.Reader and prints download progress to stderr.
type progressReader struct {
	reader    io.Reader
	totalSize int64 // -1 if unknown
	written   int64
	start     time.Time
	lastPrint time.Time
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	pr.written += int64(n)

	if now := time.Now(); now.Sub(pr.lastPrint) >= 500*time.Millisecond {
		pr.lastPrint = now
		elapsed := now.Sub(pr.start).Seconds()
		speed := float64(pr.written) / elapsed

		if pr.totalSize > 0 {
			pct := float64(pr.written) / float64(pr.totalSize) * 100
			fmt.Printf("\r  %s / %s (%.1f%%) %s/s",
				formatBytes(pr.written), formatBytes(pr.totalSize), pct, formatBytes(int64(speed)))
		} else {
			fmt.Printf("\r  %s %s/s", formatBytes(pr.written), formatBytes(int64(speed)))
		}
	}

	return n, err
}

func formatBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.0f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// extractZipPDFs extracts PDF files from a ZIP archive into the given directory.
// Returns the number of PDFs extracted.
func extractZipPDFs(zipPath, extractDir string) (int, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	if err := os.MkdirAll(extractDir, 0755); err != nil {
		return 0, fmt.Errorf("create extract dir: %w", err)
	}

	// Count total PDFs for progress
	var pdfFiles []*zip.File
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if strings.HasSuffix(strings.ToLower(f.Name), ".pdf") {
			pdfFiles = append(pdfFiles, f)
		}
	}
	total := len(pdfFiles)

	start := time.Now()
	var extracted atomic.Int64
	var firstErr atomic.Pointer[error]

	// Worker pool for parallel extraction
	fileChan := make(chan *zip.File, len(pdfFiles))
	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Go(func() {
			for f := range fileChan {
				if firstErr.Load() != nil {
					return
				}
				if err := extractOneZipPDF(f, extractDir); err != nil {
					firstErr.CompareAndSwap(nil, &err)
					return
				}
				count := extracted.Add(1)
				if count%1000 == 0 {
					pct := float64(count) / float64(int64(total)) * 100
					fmt.Printf("\r  Extracting: %d/%d files (%.1f%%)", count, total, pct)
				}
			}
		})
	}

	for _, f := range pdfFiles {
		fileChan <- f
	}
	close(fileChan)
	wg.Wait()

	if ep := firstErr.Load(); ep != nil {
		return int(extracted.Load()), *ep
	}

	fmt.Printf("\r  Extracted %d files in %s\n", extracted.Load(), time.Since(start).Truncate(time.Second))
	return int(extracted.Load()), nil
}

// extractOneZipPDF extracts a single PDF from a zip entry to the given directory
// using buffered I/O for better write performance.
func extractOneZipPDF(f *zip.File, extractDir string) error {
	outPath := filepath.Join(extractDir, filepath.Base(f.Name))

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("open zip entry %s: %w", f.Name, err)
	}
	defer rc.Close()

	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create file %s: %w", outPath, err)
	}
	defer out.Close()

	bw := bufio.NewWriterSize(out, 64*1024)
	if _, err := io.Copy(bw, rc); err != nil {
		return fmt.Errorf("extract %s: %w", f.Name, err)
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flush %s: %w", outPath, err)
	}
	return nil
}

// iterateZipPDFs iterates over PDF entries in a ZIP file, calling fn with
// the base filename and raw bytes for each PDF entry.
func iterateZipPDFs(zipPath string, fn func(name string, data []byte) error) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(f.Name), ".pdf") {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("open zip entry %s: %w", f.Name, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return fmt.Errorf("read zip entry %s: %w", f.Name, err)
		}

		if err := fn(filepath.Base(f.Name), data); err != nil {
			return err
		}
	}
	return nil
}

// countZipPDFs counts the number of PDF files in a ZIP archive.
func countZipPDFs(zipPath string) (int, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	count := 0
	for _, f := range r.File {
		if !f.FileInfo().IsDir() && strings.HasSuffix(strings.ToLower(f.Name), ".pdf") {
			count++
		}
	}
	return count, nil
}

// splitPDFToPages splits a PDF file into individual page PDFs.
// It also extracts document metadata and detects headers/footers from the original,
// saving this information to metadata.json for use when parsing individual pages.
// Returns a map of page number to the output file path, plus the metadata.
func splitPDFToPages(pdfPath, outputDir string) (map[int]string, *SplitMetadata, error) {
	// Create output directory for this PDF's pages
	baseName := strings.TrimSuffix(filepath.Base(pdfPath), filepath.Ext(pdfPath))
	pagesDir := filepath.Join(outputDir, "pages", baseName)
	metadataPath := filepath.Join(pagesDir, "metadata.json")

	// Check if metadata already exists (indicates previous split)
	if existingMeta, err := loadSplitMetadata(metadataPath); err == nil {
		// Verify all page files exist (pdfcpu names them baseName_1.pdf, baseName_2.pdf, etc.)
		allPagesExist := true
		pageFiles := make(map[int]string, existingMeta.TotalPages)
		for pageNum := 1; pageNum <= existingMeta.TotalPages; pageNum++ {
			pagePath := filepath.Join(pagesDir, fmt.Sprintf("%s_%d.pdf", baseName, pageNum))
			if _, err := os.Stat(pagePath); err != nil {
				allPagesExist = false
				break
			}
			pageFiles[pageNum] = pagePath
		}
		if allPagesExist {
			fmt.Printf("      Using existing split (%d pages)\n", existingMeta.TotalPages)
			return pageFiles, existingMeta, nil
		}
		// Some pages missing, re-split
		fmt.Printf("      Some pages missing, re-splitting...\n")
	}

	if err := os.MkdirAll(pagesDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create pages directory: %w", err)
	}

	// Read the PDF
	pdfData, err := os.ReadFile(pdfPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read PDF: %w", err)
	}

	// Get page count using the proper API
	conf := model.NewDefaultConfiguration()
	pageCount, err := api.PageCount(bytes.NewReader(pdfData), conf)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get page count: %w", err)
	}

	fmt.Printf("      PDF has %d pages\n", pageCount)

	// Extract metadata and detect headers/footers from original PDF
	fmt.Printf("      Extracting metadata and detecting headers/footers...\n")
	metadata, err := extractPDFMetadata(pdfData, filepath.Base(pdfPath), pageCount)
	if err != nil {
		// Non-fatal: continue with empty metadata
		fmt.Printf("      Warning: failed to extract metadata: %v\n", err)
		metadata = &SplitMetadata{
			SourceFile: filepath.Base(pdfPath),
			TotalPages: pageCount,
			Title:      baseName,
		}
	}

	// Split into individual pages using pdfcpu's bulk split (single pass through PDF)
	fmt.Printf("      Splitting pages (single-pass)...\n")
	pageFiles := make(map[int]string, pageCount)

	// Check if we need to split (some pages might already exist)
	needsSplit := false
	for pageNum := 1; pageNum <= pageCount; pageNum++ {
		// pdfcpu names files as: baseName_1.pdf, baseName_2.pdf, etc.
		outputPath := filepath.Join(pagesDir, fmt.Sprintf("%s_%d.pdf", baseName, pageNum))
		if _, err := os.Stat(outputPath); err != nil {
			needsSplit = true
			break
		}
		pageFiles[pageNum] = outputPath
	}

	if needsSplit {
		// Use SplitFile with span=1 to split into individual pages in one pass
		// This is O(n) instead of O(n²) - processes the PDF once instead of once per page
		if err := api.SplitFile(pdfPath, pagesDir, 1, conf); err != nil {
			return nil, nil, fmt.Errorf("failed to split PDF: %w", err)
		}

		// Collect the output file paths (pdfcpu names them baseName_1.pdf, baseName_2.pdf, etc.)
		for pageNum := 1; pageNum <= pageCount; pageNum++ {
			outputPath := filepath.Join(pagesDir, fmt.Sprintf("%s_%d.pdf", baseName, pageNum))
			if _, err := os.Stat(outputPath); err != nil {
				return nil, nil, fmt.Errorf("split produced no file for page %d: %w", pageNum, err)
			}
			pageFiles[pageNum] = outputPath
		}
		fmt.Printf("      Split complete: %d pages\n", pageCount)
	} else {
		fmt.Printf("      Using existing split files\n")
	}

	// Save metadata
	if err := saveSplitMetadata(metadataPath, metadata); err != nil {
		fmt.Printf("      Warning: failed to save metadata: %v\n", err)
	}

	return pageFiles, metadata, nil
}

// splitPDFToPagesFromBytes splits an in-memory PDF into individual pages.
// Returns a map of page number to page PDF bytes, plus the metadata.
// Single-page PDFs skip the split entirely and return the original bytes.
func splitPDFToPagesFromBytes(pdfData []byte, filename string) (map[int][]byte, *SplitMetadata, error) {
	conf := model.NewDefaultConfiguration()

	pageCount, err := api.PageCount(bytes.NewReader(pdfData), conf)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get page count: %w", err)
	}

	metadata, err := extractPDFMetadata(pdfData, filename, pageCount)
	if err != nil {
		metadata = &SplitMetadata{
			SourceFile: filename,
			TotalPages: pageCount,
			Title:      strings.TrimSuffix(filename, filepath.Ext(filename)),
		}
	}

	// Single-page PDFs skip splitting
	if pageCount == 1 {
		return map[int][]byte{1: pdfData}, metadata, nil
	}

	spans, err := api.SplitRaw(bytes.NewReader(pdfData), 1, conf)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to split PDF: %w", err)
	}

	pageBytes := make(map[int][]byte, len(spans))
	for _, span := range spans {
		data, err := io.ReadAll(span.Reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read page %d: %w", span.From, err)
		}
		pageBytes[span.From] = data
	}

	return pageBytes, metadata, nil
}

// extractPDFMetadata extracts document metadata and detects headers/footers.
func extractPDFMetadata(pdfData []byte, filename string, pageCount int) (*SplitMetadata, error) {
	reader, err := pdf.NewReader(bytes.NewReader(pdfData), int64(len(pdfData)))
	if err != nil {
		return nil, fmt.Errorf("failed to read PDF: %w", err)
	}

	metadata := &SplitMetadata{
		SourceFile: filename,
		TotalPages: pageCount,
	}

	// Extract Info dictionary metadata
	trailer := reader.Trailer()
	if !trailer.IsNull() {
		info := trailer.Key("Info")
		if !info.IsNull() {
			if v := info.Key("Title"); !v.IsNull() {
				metadata.Title = v.Text()
			}
			if v := info.Key("Author"); !v.IsNull() {
				metadata.Author = v.Text()
			}
			if v := info.Key("Subject"); !v.IsNull() {
				metadata.Subject = v.Text()
			}
			if v := info.Key("Keywords"); !v.IsNull() {
				metadata.Keywords = v.Text()
			}
			if v := info.Key("Creator"); !v.IsNull() {
				metadata.Creator = v.Text()
			}
			if v := info.Key("Producer"); !v.IsNull() {
				metadata.Producer = v.Text()
			}
			if v := info.Key("CreationDate"); !v.IsNull() {
				metadata.CreationDate = v.Text()
			}
			if v := info.Key("ModDate"); !v.IsNull() {
				metadata.ModDate = v.Text()
			}
		}
	}

	// Default title to filename if not set
	if metadata.Title == "" {
		metadata.Title = strings.TrimSuffix(filename, filepath.Ext(filename))
	}

	// Detect headers/footers by scanning pages (need at least 3 pages for detection)
	if pageCount >= 3 {
		textRepair := docsaf.NewTextRepair()

		// Sample pages for header/footer detection (first, middle, last)
		samplePages := []int{1, pageCount / 2, pageCount}
		if pageCount >= 10 {
			// Add more samples for larger documents
			samplePages = append(samplePages, 2, 3, pageCount-1, pageCount-2)
		}

		for _, pageNum := range samplePages {
			page := reader.Page(pageNum)
			if page.V.IsNull() {
				continue
			}
			pageContent, _ := page.GetPlainText(nil)
			textRepair.RecordPageContent(pageContent)
		}

		metadata.DetectedHeaders = textRepair.GetDetectedHeaders()
		metadata.DetectedFooters = textRepair.GetDetectedFooters()

		if len(metadata.DetectedHeaders) > 0 || len(metadata.DetectedFooters) > 0 {
			fmt.Printf("      Detected %d headers, %d footers\n",
				len(metadata.DetectedHeaders), len(metadata.DetectedFooters))
		}
	}

	return metadata, nil
}

// saveSplitMetadata saves metadata to a JSON file.
func saveSplitMetadata(path string, metadata *SplitMetadata) error {
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// loadSplitMetadata loads metadata from a JSON file.
func loadSplitMetadata(path string) (*SplitMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var metadata SplitMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

// prepareCmd processes PDF files and creates JSON data
func prepareCmd(args []string) error {
	fs := flag.NewFlagSet("prepare", flag.ExitOnError)
	dirPath := fs.String("dir", "./epstein-docs", "Path to directory containing PDF files")
	outputFile := fs.String("output", "epstein-docs.json", "Output JSON file path")
	baseURL := fs.String("base-url", "", "Base URL for generating document links (optional)")
	splitPages := fs.Bool("split-pages", false, "Split PDFs into individual page files for direct viewing")
	noHeaderFooter := fs.Bool("no-header-footer-detection", false, "Disable header/footer detection (faster, single pass)")
	noMirroredRepair := fs.Bool("no-mirrored-text-repair", false, "Disable mirrored text repair (faster)")
	smartJoin := fs.Bool("smart-join", false, "Enable smart line joining to defragment text layouts")
	numWorkers := fs.Int("workers", runtime.NumCPU(), "Number of parallel workers for parsing (split-pages mode only)")
	var zipPaths StringSliceFlag
	fs.Var(&zipPaths, "zip", "Path to ZIP archive containing PDFs (repeatable, skips extraction)")

	// OCR fallback flags
	enableOCR := fs.Bool("enable-ocr", false, "Enable OCR fallback when text extraction fails or produces poor results")
	ocrTermiteURL := fs.String("ocr-url", "http://localhost:8082", "Termite URL for OCR")
	ocrModels := fs.String("ocr-models", "trocr-base-printed,florence-2", "OCR models to try (comma-separated, in order)")
	ocrMinContent := fs.Int("ocr-min-content", 50, "Trigger OCR if extracted content is shorter than this")
	ocrDPI := fs.Float64("ocr-dpi", 150, "DPI for rendering PDF pages to images for OCR")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	// Determine available sources
	dirExists := false
	if fi, err := os.Stat(*dirPath); err == nil && fi.IsDir() {
		dirExists = true
	}
	if !dirExists && len(zipPaths) == 0 {
		return fmt.Errorf("no sources: --dir %s does not exist and no --zip provided", *dirPath)
	}

	fmt.Printf("=== Epstein Documents Prepare ===\n")
	fmt.Printf("Directory: %s\n", *dirPath)
	fmt.Printf("Output: %s\n", *outputFile)
	fmt.Printf("Split pages: %v\n", *splitPages)
	fmt.Printf("Header/footer detection: %v\n", !*noHeaderFooter)
	fmt.Printf("Mirrored text repair: %v\n", !*noMirroredRepair)
	fmt.Printf("Smart line joining: %v\n", *smartJoin)
	if *splitPages {
		fmt.Printf("Workers: %d\n", *numWorkers)
	}
	if len(zipPaths) > 0 {
		fmt.Printf("ZIP sources: %d\n", len(zipPaths))
		for _, zp := range zipPaths {
			fmt.Printf("  - %s\n", zp)
		}
	}
	if *enableOCR {
		fmt.Printf("OCR fallback: enabled\n")
		fmt.Printf("OCR Termite URL: %s\n", *ocrTermiteURL)
		fmt.Printf("OCR models: %s\n", *ocrModels)
		fmt.Printf("OCR min content: %d chars\n", *ocrMinContent)
		fmt.Printf("OCR render DPI: %.0f\n", *ocrDPI)
	}
	fmt.Printf("\n")

	// Initialize OCR client if enabled
	var ocrClient *OCRClient
	if *enableOCR {
		models := strings.Split(*ocrModels, ",")
		for i := range models {
			models[i] = strings.TrimSpace(models[i])
		}
		var err error
		ocrClient, err = NewOCRClient(*ocrTermiteURL, models, *ocrDPI)
		if err != nil {
			return fmt.Errorf("failed to create OCR client: %w", err)
		}
		fmt.Printf("OCR client initialized\n\n")
	}

	var sections []docsaf.DocumentSection

	if *splitPages {
		// Split-pages mode: split PDFs first, then parse individual page files.
		// Zip entries are streamed directly to workers to avoid holding all
		// split page bytes in memory at once (prevents OOM on large archives).
		fmt.Printf("=== Split Pages Mode ===\n")
		fmt.Printf("Step 1: Splitting directory PDFs and extracting metadata...\n\n")

		// pageJob represents a single page to process
		type pageJob struct {
			sourceFile string
			pageNum    int
			pagePath   string
			pageData   []byte
			metadata   *SplitMetadata
		}

		// splitResult for directory-based PDFs only (paths, not bytes)
		type splitResult struct {
			pageFiles map[int]string
			metadata  *SplitMetadata
		}
		splitResults := make(map[string]splitResult)

		// Find and split all PDF files from directory (including subdirectories from extracted ZIPs)
		if dirExists {
			if err := filepath.WalkDir(*dirPath, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				// Skip the pages/ directory tree (contains already-split page PDFs)
				if d.IsDir() && d.Name() == "pages" {
					return filepath.SkipDir
				}
				if d.IsDir() {
					return nil
				}
				if !strings.HasSuffix(strings.ToLower(d.Name()), ".pdf") {
					return nil
				}

				relPath, _ := filepath.Rel(*dirPath, path)
				fmt.Printf("  Processing %s...\n", relPath)

				pageFiles, metadata, err := splitPDFToPages(path, *dirPath)
				if err != nil {
					return fmt.Errorf("failed to split %s: %w", relPath, err)
				}

				splitResults[relPath] = splitResult{pageFiles: pageFiles, metadata: metadata}
				fmt.Printf("    %d pages ready\n", len(pageFiles))
				return nil
			}); err != nil {
				return fmt.Errorf("failed to walk directory: %w", err)
			}
		}

		if len(splitResults) == 0 && len(zipPaths) == 0 {
			return fmt.Errorf("no PDF files found in directory")
		}
		fmt.Printf("\n")

		// Step 2: Parse individual page PDFs in parallel (streaming zip entries to workers)
		fmt.Printf("Step 2: Parsing individual page PDFs (%d workers)...\n\n", *numWorkers)

		// Bounded channels — backpressure keeps memory bounded
		jobChan := make(chan pageJob, *numWorkers*4)
		resultChan := make(chan docsaf.DocumentSection, *numWorkers*4)

		// Progress: producer counts pages sent, workers count pages processed
		var pagesProduced atomic.Int64
		var pagesProcessed atomic.Int64

		// Start workers before producing jobs
		var wg sync.WaitGroup
		for w := 0; w < *numWorkers; w++ {
			wg.Go(func() {

				// Each worker gets its own PDF processor (not thread-safe)
				pdfProcessor := &docsaf.PDFProcessor{
					EnableHeaderFooterDetection: false,
					EnableMirroredTextRepair:    !*noMirroredRepair,
				}

				for job := range jobChan {
					// Read the page PDF
					pageData := job.pageData
					if pageData == nil {
						var err error
						pageData, err = os.ReadFile(job.pagePath)
						if err != nil {
							pagesProcessed.Add(1)
							continue
						}
					}

					// Process the single-page PDF
					var relPagePath string
					if job.pagePath != "" {
						relPagePath, _ = filepath.Rel(*dirPath, job.pagePath)
					} else {
						relPagePath = fmt.Sprintf("%s_page_%d.pdf", strings.TrimSuffix(job.sourceFile, ".pdf"), job.pageNum)
					}
					pageSections, err := pdfProcessor.Process(relPagePath, "", *baseURL, pageData)
					if err != nil {
						pagesProcessed.Add(1)
						continue
					}

					// Should be exactly 1 section for a single-page PDF
					for i := range pageSections {
						section := &pageSections[i]

						// Override with metadata from the original document
						section.ID = generateSectionID(job.sourceFile, fmt.Sprintf("page_%d", job.pageNum))
						section.FilePath = job.sourceFile
						section.Title = fmt.Sprintf("%s - Page %d", job.metadata.Title, job.pageNum)

						// Apply pre-detected headers/footers
						if len(job.metadata.DetectedHeaders) > 0 || len(job.metadata.DetectedFooters) > 0 {
							textRepair := docsaf.NewTextRepair()
							section.Content = textRepair.RemoveHeadersFooters(
								section.Content,
								job.metadata.DetectedHeaders,
								job.metadata.DetectedFooters,
							)
						}

						// Apply OCR fallback if enabled and text quality is poor
						if ocrClient != nil {
							ctx := context.Background()
							// For single-page PDFs, the page number is always 1
							ocrText, ocrUsed, ocrErr := ocrClient.ProcessPage(ctx, pageData, 1, section.Content, *ocrMinContent)
							if ocrErr != nil {
								// Log OCR errors but continue with original text
								fmt.Printf("    OCR error on %s page %d: %v\n", job.sourceFile, job.pageNum, ocrErr)
							} else if ocrUsed {
								section.Content = ocrText
								section.Metadata["ocr_used"] = true
								section.Metadata["ocr_model"] = ocrClient.LastUsedModel()
							}
						}

						// Apply smart line joining if enabled
						if *smartJoin {
							section.Content = smartJoinLines(section.Content)
						}

						// Set correct metadata
						section.Metadata["page_number"] = job.pageNum
						section.Metadata["total_pages"] = job.metadata.TotalPages
						section.Metadata["source_file"] = job.sourceFile

						if job.metadata.Author != "" {
							section.Metadata["author"] = job.metadata.Author
						}
						if job.metadata.Subject != "" {
							section.Metadata["subject"] = job.metadata.Subject
						}
						if job.metadata.Keywords != "" {
							section.Metadata["keywords"] = job.metadata.Keywords
						}

						section.URL = "/pdfs/" + relPagePath
						section.Metadata["page_pdf_path"] = relPagePath

						resultChan <- *section
					}

					// Progress tracking
					count := pagesProcessed.Add(1)
					if count%100 == 0 {
						fmt.Printf("  Processed %d/%d pages...\n", count, pagesProduced.Load())
					}
				}
			})
		}

		// Producer goroutine: sends dir-based jobs, then streams zip entries
		var producerErr error
		var producerDone sync.WaitGroup
		producerDone.Add(1)
		go func() {
			defer producerDone.Done()
			defer close(jobChan)

			// Send directory-based page jobs (paths only, cheap)
			for sourceFile, result := range splitResults {
				for pageNum, pagePath := range result.pageFiles {
					jobChan <- pageJob{
						sourceFile: sourceFile,
						pageNum:    pageNum,
						pagePath:   pagePath,
						metadata:   result.metadata,
					}
					pagesProduced.Add(1)
				}
			}

			// Stream zip entries: split each PDF and send page jobs immediately
			for _, zipPath := range zipPaths {
				fmt.Printf("  Streaming ZIP: %s\n", zipPath)
				zipPDFCount, err := countZipPDFs(zipPath)
				if err != nil {
					producerErr = fmt.Errorf("failed to count PDFs in %s: %w", zipPath, err)
					return
				}
				fmt.Printf("    %d PDFs in archive\n", zipPDFCount)
				zipIdx := 0
				if err := iterateZipPDFs(zipPath, func(name string, data []byte) error {
					zipIdx++
					pb, meta, err := splitPDFToPagesFromBytes(data, name)
					if err != nil {
						return fmt.Errorf("failed to split %s: %w", name, err)
					}
					// Send page jobs directly — bytes are consumed by workers
					// and freed once the job is processed
					for pageNum, pageData := range pb {
						jobChan <- pageJob{
							sourceFile: name,
							pageNum:    pageNum,
							pageData:   pageData,
							metadata:   meta,
						}
						pagesProduced.Add(1)
					}
					if zipIdx%100 == 0 || zipIdx == zipPDFCount {
						fmt.Printf("    [%d/%d] PDFs split, %d pages produced\n", zipIdx, zipPDFCount, pagesProduced.Load())
					}
					return nil
				}); err != nil {
					producerErr = fmt.Errorf("failed to process zip %s: %w", zipPath, err)
					return
				}
			}
		}()

		// Wait for workers and close result channel
		go func() {
			wg.Wait()
			close(resultChan)
		}()

		// Collect results
		for section := range resultChan {
			sections = append(sections, section)
		}

		// Wait for producer to finish and check for errors
		producerDone.Wait()
		if producerErr != nil {
			return producerErr
		}

		// Sort sections by source file and page number for consistent output
		slices.SortFunc(sections, func(a, b docsaf.DocumentSection) int {
			if a.FilePath != b.FilePath {
				return strings.Compare(a.FilePath, b.FilePath)
			}
			aPage := a.Metadata["page_number"].(int)
			bPage := b.Metadata["page_number"].(int)
			return aPage - bPage
		})

		fmt.Printf("  Completed %d pages\n", len(sections))

		// Count OCR-processed pages
		if ocrClient != nil {
			ocrCount := 0
			for _, section := range sections {
				if used, ok := section.Metadata["ocr_used"].(bool); ok && used {
					ocrCount++
				}
			}
			if ocrCount > 0 {
				fmt.Printf("  OCR fallback used: %d pages (%.1f%%)\n", ocrCount, float64(ocrCount)/float64(len(sections))*100)
			} else {
				fmt.Printf("  OCR fallback: not needed for any pages\n")
			}
		}
		fmt.Printf("\n")
	} else {
		// Standard mode: parse original multi-page PDFs
		if dirExists {
			source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
				BaseDir: *dirPath,
				BaseURL: *baseURL,
				IncludePatterns: []string{
					"**/*.pdf",
					"**/*.PDF",
				},
				ExcludePatterns: []string{
					"**/pages/**", // Exclude any split page PDFs
				},
			})

			// Create registry with enhanced PDF processor
			registry := docsaf.NewRegistry()
			registry.Register(&docsaf.MarkdownProcessor{})
			registry.Register(&docsaf.OpenAPIProcessor{})
			registry.Register(&docsaf.HTMLProcessor{})
			registry.Register(&docsaf.PDFProcessor{
				EnableHeaderFooterDetection: !*noHeaderFooter,
				EnableMirroredTextRepair:    !*noMirroredRepair,
				ProgressInterval:            100,
				ProgressFunc: func(p docsaf.PDFProgress) error {
					fmt.Printf("  [%s] %s: page %d/%d\n", p.Phase, p.FilePath, p.Page, p.TotalPages)
					return nil
				},
			})

			processor := docsaf.NewProcessor(source, registry)

			fmt.Printf("Processing PDF files with enhanced text repair...\n")
			var err error
			sections, err = processor.Process(context.Background())
			if err != nil {
				return fmt.Errorf("failed to process directory: %w", err)
			}
		}

		// Process ZIP sources in standard mode
		if len(zipPaths) > 0 {
			pdfProcessor := &docsaf.PDFProcessor{
				EnableHeaderFooterDetection: !*noHeaderFooter,
				EnableMirroredTextRepair:    !*noMirroredRepair,
			}
			for _, zipPath := range zipPaths {
				fmt.Printf("Processing ZIP: %s\n", zipPath)
				if err := iterateZipPDFs(zipPath, func(name string, data []byte) error {
					zipSections, err := pdfProcessor.Process(name, "", *baseURL, data)
					if err != nil {
						fmt.Printf("  Warning: failed to process %s: %v\n", name, err)
						return nil
					}
					sections = append(sections, zipSections...)
					return nil
				}); err != nil {
					return fmt.Errorf("failed to process zip %s: %w", zipPath, err)
				}
			}
		}
	}

	// Apply smart line joining if enabled (for standard mode)
	if *smartJoin && !*splitPages {
		fmt.Printf("Applying smart line joining to %d sections...\n", len(sections))
		for i := range sections {
			sections[i].Content = smartJoinLines(sections[i].Content)
		}
		fmt.Printf("\n")
	}

	fmt.Printf("Found %d document sections (pages)\n\n", len(sections))

	if len(sections) == 0 {
		return fmt.Errorf("no PDF files found in directory")
	}

	// Count sections by source file
	fileCounts := make(map[string]int)
	for _, section := range sections {
		fileCounts[section.FilePath]++
	}

	fmt.Printf("Source files:\n")
	for file, count := range fileCounts {
		fmt.Printf("  - %s: %d pages\n", file, count)
	}
	fmt.Printf("\n")

	// Show sample of documents
	fmt.Printf("Sample pages:\n")
	for i, section := range sections {
		if i >= 10 {
			fmt.Printf("  ... and %d more\n", len(sections)-i)
			break
		}
		// Truncate content for display
		content := section.Content
		if len(content) > 100 {
			content = content[:100] + "..."
		}
		content = strings.ReplaceAll(content, "\n", " ")
		fmt.Printf("  [%d] %s\n      %s\n", i+1, section.Title, content)
	}
	fmt.Printf("\n")

	// Convert sections to records map
	records := make(map[string]any)
	for _, section := range sections {
		records[section.ID] = section.ToDocument()
	}

	// Write to JSON file
	fmt.Printf("Writing %d records to %s...\n", len(records), *outputFile)
	jsonData, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	err = os.WriteFile(*outputFile, jsonData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("Prepared data written to %s\n", *outputFile)
	return nil
}

// loadCmd loads prepared JSON data into Antfly
func loadCmd(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	antflyURL := fs.String("url", "http://localhost:8080/api/v1", "Antfly API URL")
	tableName := fs.String("table", "epstein_docs", "Table name to merge into")
	inputFile := fs.String("input", "epstein-docs.json", "Input JSON file path")
	dryRun := fs.Bool("dry-run", false, "Preview changes without applying them")
	createTable := fs.Bool("create-table", false, "Create table if it doesn't exist")
	numShards := fs.Int("num-shards", 1, "Number of shards for new table")
	batchSize := fs.Int("batch-size", 25, "Linear merge batch size")
	embeddingModel := fs.String("embedding-model", "embeddinggemma", "Embedding model to use")
	chunkerModel := fs.String("chunker-model", "fixed-bert-tokenizer", "Chunker model")
	targetTokens := fs.Int("target-tokens", 512, "Target tokens for chunking")
	overlapTokens := fs.Int("overlap-tokens", 50, "Overlap tokens for chunking")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	ctx := context.Background()

	// Create Antfly client
	client, err := antfly.NewAntflyClient(*antflyURL, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to create Antfly client: %w", err)
	}

	fmt.Printf("=== Epstein Documents Load ===\n")
	fmt.Printf("Antfly URL: %s\n", *antflyURL)
	fmt.Printf("Table: %s\n", *tableName)
	fmt.Printf("Input: %s\n", *inputFile)
	fmt.Printf("Dry run: %v\n\n", *dryRun)

	// Create table if requested
	if *createTable {
		fmt.Printf("Creating table '%s' with %d shards...\n", *tableName, *numShards)

		embeddingIndex, err := createEmbeddingIndex(*embeddingModel, *chunkerModel, *targetTokens, *overlapTokens)
		if err != nil {
			return fmt.Errorf("failed to create embedding index config: %w", err)
		}

		err = client.CreateTable(ctx, *tableName, antfly.CreateTableRequest{
			NumShards: uint(*numShards),
			Indexes: map[string]antfly.IndexConfig{
				"embeddings": *embeddingIndex,
			},
		})
		if err != nil {
			log.Printf("Warning: Failed to create table (may already exist): %v\n", err)
		} else {
			fmt.Printf("Table created with BM25 and embedding indexes\n\n")
		}

		if err := waitForShardsReady(ctx, client, *tableName, 30*time.Second); err != nil {
			return fmt.Errorf("error waiting for shards: %w", err)
		}
	}

	// Read JSON file
	fmt.Printf("Reading records from %s...\n", *inputFile)
	jsonData, err := os.ReadFile(*inputFile)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var records map[string]any
	err = json.Unmarshal(jsonData, &records)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	fmt.Printf("Loaded %d records\n\n", len(records))

	// Perform batched linear merge
	finalCursor, err := performBatchedLinearMerge(ctx, client, *tableName, records, *batchSize, *dryRun)
	if err != nil {
		return fmt.Errorf("batched linear merge failed: %w", err)
	}

	// Final cleanup
	if finalCursor != "" && !*dryRun {
		fmt.Printf("\nPerforming final cleanup...\n")
		cleanupResult, err := client.LinearMerge(ctx, *tableName, antfly.LinearMergeRequest{
			Records:      map[string]any{},
			LastMergedId: finalCursor,
			DryRun:       false,
			SyncLevel:    antfly.SyncLevelAknn,
		})
		if err != nil {
			return fmt.Errorf("final cleanup failed: %w", err)
		}
		fmt.Printf("Final cleanup completed in %s\n", cleanupResult.Took)
		fmt.Printf("  Deleted: %d orphaned documents\n", cleanupResult.Deleted)
	}

	fmt.Printf("\nLoad completed successfully\n")
	return nil
}

// syncCmd combines download, prepare, and load
func syncCmd(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)
	antflyURL := fs.String("url", "http://localhost:8080/api/v1", "Antfly API URL")
	tableName := fs.String("table", "epstein_docs", "Table name")
	dirPath := fs.String("dir", "./epstein-docs", "Path to PDF directory")
	baseURL := fs.String("base-url", "", "Base URL for document links")
	dryRun := fs.Bool("dry-run", false, "Preview changes")
	createTable := fs.Bool("create-table", false, "Create table if needed")
	numShards := fs.Int("num-shards", 1, "Number of shards")
	batchSize := fs.Int("batch-size", 25, "Batch size")
	embeddingModel := fs.String("embedding-model", "embeddinggemma", "Embedding model")
	chunkerModel := fs.String("chunker-model", "fixed-bert-tokenizer", "Chunker model")
	targetTokens := fs.Int("target-tokens", 512, "Target tokens")
	overlapTokens := fs.Int("overlap-tokens", 50, "Overlap tokens")
	noHeaderFooter := fs.Bool("no-header-footer-detection", false, "Disable header/footer detection (faster)")
	noMirroredRepair := fs.Bool("no-mirrored-text-repair", false, "Disable mirrored text repair (faster)")
	var zipPaths StringSliceFlag
	fs.Var(&zipPaths, "zip", "Path to ZIP archive containing PDFs (repeatable, skips extraction)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	ctx := context.Background()

	// Create client
	client, err := antfly.NewAntflyClient(*antflyURL, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	fmt.Printf("=== Epstein Documents Sync ===\n")
	fmt.Printf("Directory: %s\n", *dirPath)
	fmt.Printf("Table: %s\n", *tableName)
	if len(zipPaths) > 0 {
		fmt.Printf("ZIP sources: %d\n", len(zipPaths))
		for _, zp := range zipPaths {
			fmt.Printf("  - %s\n", zp)
		}
	}
	fmt.Printf("\n")

	// Create table if requested
	if *createTable {
		fmt.Printf("Creating table '%s'...\n", *tableName)

		embeddingIndex, err := createEmbeddingIndex(*embeddingModel, *chunkerModel, *targetTokens, *overlapTokens)
		if err != nil {
			return fmt.Errorf("failed to create embedding index: %w", err)
		}

		err = client.CreateTable(ctx, *tableName, antfly.CreateTableRequest{
			NumShards: uint(*numShards),
			Indexes: map[string]antfly.IndexConfig{
				"embeddings": *embeddingIndex,
			},
		})
		if err != nil {
			log.Printf("Warning: %v\n", err)
		}

		if err := waitForShardsReady(ctx, client, *tableName, 30*time.Second); err != nil {
			return fmt.Errorf("error waiting for shards: %w", err)
		}
	}

	// Determine available sources
	dirExists := false
	if fi, err := os.Stat(*dirPath); err == nil && fi.IsDir() {
		dirExists = true
	}
	if !dirExists && len(zipPaths) == 0 {
		return fmt.Errorf("no sources: --dir %s does not exist and no --zip provided", *dirPath)
	}

	// Process PDFs from directory
	var sections []docsaf.DocumentSection
	if dirExists {
		source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
			BaseDir:         *dirPath,
			BaseURL:         *baseURL,
			IncludePatterns: []string{"**/*.pdf", "**/*.PDF"},
		})

		registry := docsaf.NewRegistry()
		registry.Register(&docsaf.MarkdownProcessor{})
		registry.Register(&docsaf.OpenAPIProcessor{})
		registry.Register(&docsaf.HTMLProcessor{})
		registry.Register(&docsaf.PDFProcessor{
			EnableHeaderFooterDetection: !*noHeaderFooter,
			EnableMirroredTextRepair:    !*noMirroredRepair,
			ProgressInterval:            100,
			ProgressFunc: func(p docsaf.PDFProgress) error {
				fmt.Printf("  [%s] %s: page %d/%d\n", p.Phase, p.FilePath, p.Page, p.TotalPages)
				return nil
			},
		})

		processor := docsaf.NewProcessor(source, registry)

		fmt.Printf("Processing PDF files...\n")
		var err error
		sections, err = processor.Process(ctx)
		if err != nil {
			return fmt.Errorf("failed to process: %w", err)
		}
	}

	// Process ZIP sources
	if len(zipPaths) > 0 {
		pdfProcessor := &docsaf.PDFProcessor{
			EnableHeaderFooterDetection: !*noHeaderFooter,
			EnableMirroredTextRepair:    !*noMirroredRepair,
		}
		for _, zipPath := range zipPaths {
			fmt.Printf("Processing ZIP: %s\n", zipPath)
			if err := iterateZipPDFs(zipPath, func(name string, data []byte) error {
				zipSections, err := pdfProcessor.Process(name, "", *baseURL, data)
				if err != nil {
					fmt.Printf("  Warning: failed to process %s: %v\n", name, err)
					return nil
				}
				sections = append(sections, zipSections...)
				return nil
			}); err != nil {
				return fmt.Errorf("failed to process zip %s: %w", zipPath, err)
			}
		}
	}

	fmt.Printf("Found %d pages\n\n", len(sections))

	if len(sections) == 0 {
		return fmt.Errorf("no PDF files found")
	}

	// Convert to records
	records := make(map[string]any)
	for _, section := range sections {
		records[section.ID] = section.ToDocument()
	}

	// Load
	finalCursor, err := performBatchedLinearMerge(ctx, client, *tableName, records, *batchSize, *dryRun)
	if err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Cleanup
	if finalCursor != "" && !*dryRun {
		client.LinearMerge(ctx, *tableName, antfly.LinearMergeRequest{
			Records:      map[string]any{},
			LastMergedId: finalCursor,
			SyncLevel:    antfly.SyncLevelAknn,
		})
	}

	fmt.Printf("\nSync completed\n")
	return nil
}

// serveCmd starts a web server with a search interface
func serveCmd(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	antflyURL := fs.String("url", "http://localhost:8080/api/v1", "Antfly API URL")
	tableName := fs.String("table", "epstein_docs", "Table name to search")
	listenAddr := fs.String("listen", ":3000", "Listen address for web server")
	pdfDir := fs.String("pdf-dir", "./epstein-docs", "Directory containing PDF files (including pages/ subdirectory)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	// Create Antfly client
	client, err := antfly.NewAntflyClient(*antflyURL, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	// Parse templates
	tmpl, err := template.ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		return fmt.Errorf("failed to parse templates: %w", err)
	}

	// Create server
	server := &SearchServer{
		client:    client,
		tableName: *tableName,
		tmpl:      tmpl,
	}

	// Create a new mux to avoid conflicts with default mux
	mux := http.NewServeMux()

	// Set up routes
	mux.HandleFunc("/", server.handleIndex)
	mux.HandleFunc("/search", server.handleSearch)
	mux.HandleFunc("/api/search", server.handleAPISearch)

	// Serve static PDF files from the pages directory
	pagesDir := filepath.Join(*pdfDir, "pages")
	if _, err := os.Stat(pagesDir); err == nil {
		// Serve PDFs with proper content type
		pdfHandler := http.StripPrefix("/pdfs/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			filePath := filepath.Join(pagesDir, r.URL.Path)
			// Security: ensure path doesn't escape the pages directory
			if !strings.HasPrefix(filepath.Clean(filePath), filepath.Clean(pagesDir)) {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/pdf")
			http.ServeFile(w, r, filePath)
		}))
		mux.Handle("/pdfs/", pdfHandler)
		fmt.Printf("PDF files: %s (serving at /pdfs/)\n", pagesDir)
	} else {
		fmt.Printf("Note: No pages/ directory found at %s\n", pagesDir)
		fmt.Printf("      Run 'epstein prepare --split-pages' to create individual page PDFs\n")
	}

	fmt.Printf("=== Epstein Documents Search Server ===\n")
	fmt.Printf("Antfly URL: %s\n", *antflyURL)
	fmt.Printf("Table: %s\n", *tableName)
	fmt.Printf("Listen: %s\n", *listenAddr)
	fmt.Printf("\nOpen http://localhost%s in your browser\n\n", *listenAddr)

	return http.ListenAndServe(*listenAddr, mux)
}

// SearchServer handles web requests
type SearchServer struct {
	client    *antfly.AntflyClient
	tableName string
	tmpl      *template.Template
}

// SearchResult represents a search result for the template
type SearchResult struct {
	ID       string
	Title    string
	Content  string
	Score    float64
	FilePath string
	PageNum  int
	URL      string
}

// SearchPageData holds data for the search page template
type SearchPageData struct {
	Query   string
	Results []SearchResult
	Error   string
	Took    string
	Total   int
}

func (s *SearchServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	s.tmpl.ExecuteTemplate(w, "index.html", SearchPageData{})
}

func (s *SearchServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		s.tmpl.ExecuteTemplate(w, "index.html", SearchPageData{})
		return
	}

	ctx := r.Context()
	start := time.Now()

	// Perform search
	resp, err := s.client.Query(ctx, antfly.QueryRequest{
		Table:          s.tableName,
		SemanticSearch: query,
		Indexes:        []string{"full_text_index", "embeddings"},
		Limit:          20,
	})

	data := SearchPageData{
		Query: query,
		Took:  time.Since(start).String(),
	}

	if err != nil {
		data.Error = err.Error()
	} else if len(resp.Responses) > 0 {
		hits := resp.Responses[0].Hits.Hits
		data.Total = len(hits)
		for _, hit := range hits {
			result := SearchResult{
				ID:    hit.ID,
				Score: hit.Score,
			}

			// Extract fields from hit source
			if hit.Source != nil {
				if title, ok := hit.Source["title"].(string); ok {
					result.Title = title
				}
				if content, ok := hit.Source["content"].(string); ok {
					result.Content = truncateContent(content, 2000)
				}
				if filePath, ok := hit.Source["file_path"].(string); ok {
					result.FilePath = filePath
				}
				if url, ok := hit.Source["url"].(string); ok {
					result.URL = url
				}
				if metadata, ok := hit.Source["metadata"].(map[string]any); ok {
					if pageNum, ok := metadata["page_number"].(float64); ok {
						result.PageNum = int(pageNum)
					}
				}
			}

			data.Results = append(data.Results, result)
		}
	}

	s.tmpl.ExecuteTemplate(w, "index.html", data)
}

func (s *SearchServer) handleAPISearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "missing query parameter 'q'", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	resp, err := s.client.Query(ctx, antfly.QueryRequest{
		Table:          s.tableName,
		SemanticSearch: query,
		Indexes:        []string{"full_text_index", "embeddings"},
		Limit:          20,
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Helper functions

func createEmbeddingIndex(embeddingModel, chunkerModel string, targetTokens, overlapTokens int) (*antfly.IndexConfig, error) {
	embeddingIndexConfig := antfly.IndexConfig{
		Name: "embeddings",
		Type: antfly.IndexTypeEmbeddings,
	}

	embedder, err := antfly.NewEmbedderConfig(antfly.OllamaEmbedderConfig{
		Model: embeddingModel,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure embedder: %w", err)
	}

	chunker := antfly.ChunkerConfig{}
	err = chunker.FromTermiteChunkerConfig(antfly.TermiteChunkerConfig{
		Model: chunkerModel,
		Text: antfly.TextChunkOptions{
			TargetTokens:  targetTokens,
			OverlapTokens: overlapTokens,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure chunker: %w", err)
	}

	err = embeddingIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Field:    "content",
		Embedder: *embedder,
		Chunker:  chunker,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure embedding index: %w", err)
	}

	return &embeddingIndexConfig, nil
}

func performBatchedLinearMerge(
	ctx context.Context,
	client *antfly.AntflyClient,
	tableName string,
	records map[string]any,
	batchSize int,
	dryRun bool,
) (string, error) {
	ids := make([]string, 0, len(records))
	for id := range records {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	totalBatches := (len(ids) + batchSize - 1) / batchSize
	fmt.Printf("Loading %d documents in %d batches\n", len(ids), totalBatches)

	cursor := ""
	totalUpserted := 0

	for batchNum := range totalBatches {
		start := batchNum * batchSize
		end := min(start+batchSize, len(ids))

		batchIDs := ids[start:end]
		batchRecords := make(map[string]any, len(batchIDs))
		for _, id := range batchIDs {
			batchRecords[id] = records[id]
		}

		fmt.Printf("[%d/%d] Merging %d-%d...\n", batchNum+1, totalBatches, start+1, end)

		result, err := client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
			Records:      batchRecords,
			LastMergedId: cursor,
			DryRun:       dryRun,
			SyncLevel:    antfly.SyncLevelAknn,
		})
		if err != nil {
			return "", fmt.Errorf("batch %d failed: %w", batchNum+1, err)
		}

		if result.NextCursor != "" {
			cursor = result.NextCursor
		} else {
			cursor = batchIDs[len(batchIDs)-1]
		}

		totalUpserted += result.Upserted
		fmt.Printf("  Upserted: %d, Took: %s\n", result.Upserted, result.Took)
	}

	fmt.Printf("\nTotal upserted: %d\n", totalUpserted)
	return cursor, nil
}

// truncateContent truncates content at a natural boundary (paragraph or sentence)
func truncateContent(content string, maxLen int) string {
	if len(content) <= maxLen {
		return content
	}

	// Try to find a paragraph break near the limit
	truncated := content[:maxLen]
	if idx := strings.LastIndex(truncated, "\n\n"); idx > maxLen/2 {
		return strings.TrimSpace(truncated[:idx]) + "\n\n..."
	}

	// Try to find a sentence break
	if idx := strings.LastIndex(truncated, ". "); idx > maxLen/2 {
		return truncated[:idx+1] + "..."
	}

	// Try to find a newline
	if idx := strings.LastIndex(truncated, "\n"); idx > maxLen/2 {
		return strings.TrimSpace(truncated[:idx]) + "\n..."
	}

	// Fall back to word boundary
	if idx := strings.LastIndex(truncated, " "); idx > maxLen/2 {
		return truncated[:idx] + "..."
	}

	return truncated + "..."
}

func waitForShardsReady(ctx context.Context, client *antfly.AntflyClient, tableName string, timeout time.Duration) error {
	fmt.Printf("Waiting for shards...\n")

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	pollCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pollCount++

			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for shards")
			}

			status, err := client.GetTable(ctx, tableName)
			if err != nil {
				continue
			}

			if len(status.Shards) > 0 && pollCount >= 6 {
				fmt.Printf("Shards ready\n\n")
				return nil
			}
		}
	}
}

// auditCmd audits parsed documents for errors and quality issues
func auditCmd(args []string) error {
	fs := flag.NewFlagSet("audit", flag.ExitOnError)
	inputFile := fs.String("input", "epstein-docs.json", "Input JSON file to audit")
	dirPath := fs.String("dir", "", "Alternatively, audit PDFs directly from directory")
	verbose := fs.Bool("verbose", false, "Show detailed issues per document")
	minContentLen := fs.Int("min-content", 50, "Minimum content length to consider valid")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	fmt.Printf("=== Epstein Documents Audit ===\n\n")

	var sections []auditSection

	if *dirPath != "" {
		// Audit directly from PDFs
		fmt.Printf("Auditing PDFs from: %s\n\n", *dirPath)
		source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
			BaseDir:         *dirPath,
			IncludePatterns: []string{"**/*.pdf", "**/*.PDF"},
		})
		processor := docsaf.NewProcessor(source, docsaf.DefaultRegistry())

		docSections, err := processor.Process(context.Background())
		if err != nil {
			return fmt.Errorf("failed to process: %w", err)
		}
		for _, s := range docSections {
			sections = append(sections, auditSection{
				ID:       s.ID,
				Title:    s.Title,
				Content:  s.Content,
				FilePath: s.FilePath,
				PageNum:  getPageNum(s.Metadata),
			})
		}
	} else {
		// Audit from JSON file
		fmt.Printf("Auditing JSON file: %s\n\n", *inputFile)
		jsonData, err := os.ReadFile(*inputFile)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		var records map[string]map[string]any
		if err := json.Unmarshal(jsonData, &records); err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %w", err)
		}

		for id, rec := range records {
			content, _ := rec["content"].(string)
			title, _ := rec["title"].(string)
			filePath, _ := rec["file_path"].(string)
			var pageNum int
			if meta, ok := rec["metadata"].(map[string]any); ok {
				if pn, ok := meta["page_number"].(float64); ok {
					pageNum = int(pn)
				}
			}
			sections = append(sections, auditSection{
				ID:       id,
				Title:    title,
				Content:  content,
				FilePath: filePath,
				PageNum:  pageNum,
			})
		}
	}

	fmt.Printf("Total sections: %d\n\n", len(sections))

	// Run audit checks
	issues := runAudit(sections, *minContentLen)

	// Print summary
	fmt.Printf("=== Audit Summary ===\n\n")
	fmt.Printf("Total sections analyzed: %d\n", len(sections))
	fmt.Printf("Sections with issues: %d (%.1f%%)\n\n",
		issues.sectionsWithIssues,
		float64(issues.sectionsWithIssues)/float64(len(sections))*100)

	fmt.Printf("Issue breakdown:\n")
	fmt.Printf("  - Short content (<%d chars): %d\n", *minContentLen, issues.shortContent)
	fmt.Printf("  - Fragmented layout: %d\n", issues.fragmentedLayout)
	fmt.Printf("  - Word fragments (spaces in words): %d\n", issues.wordFragments)
	fmt.Printf("  - Garbled text detected: %d\n", issues.garbledText)
	fmt.Printf("  - Encoding issues (replacement chars): %d\n", issues.encodingIssues)
	fmt.Printf("  - Possible mirrored text: %d\n", issues.mirroredText)
	fmt.Printf("  - High symbol ratio: %d\n", issues.highSymbolRatio)
	fmt.Printf("  - Font encoding corruption: %d\n", issues.fontEncodingCorruption)

	if *verbose && len(issues.details) > 0 {
		fmt.Printf("\n=== Detailed Issues ===\n\n")
		for _, detail := range issues.details {
			fmt.Printf("[%s] Page %d\n", detail.id, detail.pageNum)
			for _, issue := range detail.issues {
				fmt.Printf("  - %s\n", issue)
			}
			if detail.sample != "" {
				fmt.Printf("  Sample: %q\n", truncateStr(detail.sample, 100))
			}
			fmt.Println()
		}
	}

	// List pages by issue type
	if len(issues.issuePages) > 0 {
		fmt.Printf("\n=== Pages by Issue Type ===\n\n")
		for issueType, pages := range issues.issuePages {
			if len(pages) <= 10 {
				fmt.Printf("%s: pages %v\n", issueType, pages)
			} else {
				fmt.Printf("%s: %d pages (first 10: %v...)\n", issueType, len(pages), pages[:10])
			}
		}
	}

	return nil
}

type auditSection struct {
	ID       string
	Title    string
	Content  string
	FilePath string
	PageNum  int
}

type auditIssues struct {
	sectionsWithIssues     int
	shortContent           int
	fragmentedLayout       int
	garbledText            int
	encodingIssues         int
	mirroredText           int
	highSymbolRatio        int
	fontEncodingCorruption int
	wordFragments          int
	details                []issueDetail
	issuePages             map[string][]int
}

type issueDetail struct {
	id      string
	pageNum int
	issues  []string
	sample  string
}

func runAudit(sections []auditSection, minContentLen int) auditIssues {
	issues := auditIssues{
		issuePages: make(map[string][]int),
	}

	for _, s := range sections {
		var sectionIssues []string
		hasIssue := false

		// Check 1: Short content
		if len(s.Content) < minContentLen {
			issues.shortContent++
			sectionIssues = append(sectionIssues, fmt.Sprintf("Short content: %d chars", len(s.Content)))
			issues.issuePages["short_content"] = append(issues.issuePages["short_content"], s.PageNum)
			hasIssue = true
		}

		// Check 2: Fragmented layout (many very short lines)
		if isFragmentedLayout(s.Content) {
			issues.fragmentedLayout++
			sectionIssues = append(sectionIssues, "Fragmented layout (many short lines)")
			issues.issuePages["fragmented_layout"] = append(issues.issuePages["fragmented_layout"], s.PageNum)
			hasIssue = true
		}

		// Check 3: Garbled text patterns
		if hasGarbledPatterns(s.Content) {
			issues.garbledText++
			sectionIssues = append(sectionIssues, "Garbled text patterns detected")
			issues.issuePages["garbled_text"] = append(issues.issuePages["garbled_text"], s.PageNum)
			hasIssue = true
		}

		// Check 4: Encoding issues (replacement characters)
		if count := countReplacementChars(s.Content); count > 0 {
			issues.encodingIssues++
			sectionIssues = append(sectionIssues, fmt.Sprintf("Encoding issues: %d replacement chars", count))
			issues.issuePages["encoding_issues"] = append(issues.issuePages["encoding_issues"], s.PageNum)
			hasIssue = true
		}

		// Check 5: Possible mirrored text patterns
		if hasMirroredPatterns(s.Content) {
			issues.mirroredText++
			sectionIssues = append(sectionIssues, "Possible mirrored text patterns")
			issues.issuePages["mirrored_text"] = append(issues.issuePages["mirrored_text"], s.PageNum)
			hasIssue = true
		}

		// Check 6: High symbol ratio
		if ratio := symbolRatio(s.Content); ratio > 0.15 {
			issues.highSymbolRatio++
			sectionIssues = append(sectionIssues, fmt.Sprintf("High symbol ratio: %.1f%%", ratio*100))
			issues.issuePages["high_symbol_ratio"] = append(issues.issuePages["high_symbol_ratio"], s.PageNum)
			hasIssue = true
		}

		// Check 7: Font encoding corruption (e.g., NWNRJcvJMTQPPJLAP should be 1:15-cv-07433-LAP)
		if hasFontEncodingCorruption(s.Content) {
			issues.fontEncodingCorruption++
			sectionIssues = append(sectionIssues, "Font encoding corruption detected")
			issues.issuePages["font_encoding"] = append(issues.issuePages["font_encoding"], s.PageNum)
			hasIssue = true
		}

		// Check 8: Word fragments (spaces inserted within words, e.g., "Virg In Ia" instead of "Virginia")
		if count := countWordFragments(s.Content); count > 0 {
			issues.wordFragments++
			sectionIssues = append(sectionIssues, fmt.Sprintf("Word fragments: %d detected", count))
			issues.issuePages["word_fragments"] = append(issues.issuePages["word_fragments"], s.PageNum)
			hasIssue = true
		}

		if hasIssue {
			issues.sectionsWithIssues++
			issues.details = append(issues.details, issueDetail{
				id:      s.ID,
				pageNum: s.PageNum,
				issues:  sectionIssues,
				sample:  s.Content,
			})
		}
	}

	return issues
}

func isFragmentedLayout(content string) bool {
	lines := strings.Split(content, "\n")
	if len(lines) < 10 {
		return false
	}

	shortLines := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if len(trimmed) > 0 && len(trimmed) < 20 {
			shortLines++
		}
	}

	// If more than 60% of lines are very short, it's fragmented
	return float64(shortLines)/float64(len(lines)) > 0.6
}

func hasGarbledPatterns(content string) bool {
	// Check for sequences of single-character "words"
	// Excludes common legal punctuation that appears as standalone characters
	words := strings.Fields(content)
	if len(words) < 20 {
		return false
	}

	singleCharWords := 0
	for _, w := range words[:min(50, len(words))] {
		if len(w) == 1 {
			// Exclude common legal formatting characters that often appear as single "words"
			// e.g., "." spacers in court case headers, "-" separators, "X" markers, "v" (versus)
			r := rune(w[0])
			if r != '.' && r != '-' && r != 'X' && r != 'x' && r != 'v' && r != ':' {
				singleCharWords++
			}
		}
	}

	// Many single-char words in first 50 suggests garbled headers
	return float64(singleCharWords)/float64(min(50, len(words))) > 0.4
}

func countReplacementChars(content string) int {
	count := 0
	for _, r := range content {
		if r == '\uFFFD' || r == '�' {
			count++
		}
	}
	return count
}

func hasMirroredPatterns(content string) bool {
	// Use bigram-based detection from docsaf
	tr := docsaf.NewTextRepair()
	confidence := tr.DetectMirroredText(content)

	// Consider mirrored if confidence >= 0.3
	if confidence >= 0.3 {
		return true
	}

	// Also check for common hardcoded patterns as fallback
	mirroredPatterns := []string{
		"HCAEB MLAP", // PALM BEACH reversed
		"ECILOP",     // POLICE reversed
		"TROPER",     // REPORT reversed
	}

	upper := strings.ToUpper(content)
	for _, pattern := range mirroredPatterns {
		if strings.Contains(upper, strings.ToUpper(pattern)) {
			return true
		}
	}
	return false
}

func symbolRatio(content string) float64 {
	if len(content) == 0 {
		return 0
	}

	symbols := 0
	total := 0
	for _, r := range content {
		if r > 32 { // Non-whitespace
			total++
			if !isAlphanumeric(r) {
				// Exclude common punctuation that's expected in legal documents
				// (periods, commas, colons, semicolons, hyphens, quotes, parentheses)
				if !isCommonPunctuation(r) {
					symbols++
				}
			}
		}
	}

	if total == 0 {
		return 0
	}
	return float64(symbols) / float64(total)
}

func isCommonPunctuation(r rune) bool {
	switch r {
	case '.', ',', ':', ';', '-', '"', '\'', '(', ')', '[', ']', '/', '!', '?', '_':
		return true
	}
	return false
}

func hasFontEncodingCorruption(content string) bool {
	tr := docsaf.NewTextRepair()

	// Check each line for font encoding issues
	lines := strings.Split(content, "\n")
	corruptedLines := 0
	totalLines := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if len(trimmed) < 15 {
			continue
		}
		totalLines++

		if tr.IsFontEncodingCorrupted(trimmed) {
			corruptedLines++
		}
	}

	// If more than 20% of substantial lines are corrupted
	if totalLines > 0 && float64(corruptedLines)/float64(totalLines) > 0.2 {
		return true
	}

	return false
}

func isAlphanumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// countWordFragments detects text with spaces inserted within words
// e.g., "Virg In Ia" instead of "Virginia", "do cu me nt" instead of "document"
func countWordFragments(content string) int {
	// Known fragment patterns that indicate text extraction issues
	fragmentPatterns := []string{
		`\bdo cu me nt\b`,     // document
		`\bVirg In Ia\b`,      // Virginia
		`\bGh Is La In E\b`,   // Ghislaine
		`\bKnow Ledge\b`,      // Knowledge
		`\bWhet Her\b`,        // Whether
		`\bTell In G\b`,       // Telling
		`\bmax Well\b`,        // Maxwell
		`\bwo me n\b`,         // women
		`\bor gies\b`,         // orgies
		`\bAt To Rney\b`,      // Attorney
		`\bIn For Mati On\b`,  // Information
		`\bAs Sist\b`,         // assist
		`\bMe Nta\b`,          // mental
		`\bCon Fi Den Tial\b`, // confidential
		`\bRe Lat\b`,          // relat-
		`\bPro Vi D\b`,        // provid-
		`\bAt Tac He\b`,       // Attached
		`\bSo Ught\b`,         // Sought
		`\bHe Nder So N\b`,    // Henderson
	}

	count := 0
	for _, pattern := range fragmentPatterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindAllString(content, -1)
		count += len(matches)
	}
	return count
}

func getPageNum(metadata map[string]any) int {
	if metadata == nil {
		return 0
	}
	if pn, ok := metadata["page_number"].(int); ok {
		return pn
	}
	if pn, ok := metadata["page_number"].(float64); ok {
		return int(pn)
	}
	return 0
}

func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// generateSectionID creates a unique ID for a document section using SHA-256 hash.
func generateSectionID(path, identifier string) string {
	hasher := sha256.New()
	hasher.Write([]byte(path + "|" + identifier))
	hash := hex.EncodeToString(hasher.Sum(nil))
	return "doc_" + hash[:16]
}

// smartJoinLines intelligently joins fragmented lines into coherent paragraphs.
// It handles common document layouts like forms, tables, and multi-column text
// by detecting patterns and joining appropriately.
func smartJoinLines(content string) string {
	lines := strings.Split(content, "\n")
	if len(lines) < 5 {
		return content
	}

	// Check if this looks like a court transcript (has line numbers and Q/A)
	if isCourtTranscriptLayout(content) {
		return content // Don't modify court transcripts
	}

	var result strings.Builder
	var currentParagraph strings.Builder

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip empty lines - they mark paragraph boundaries
		if trimmed == "" {
			if currentParagraph.Len() > 0 {
				result.WriteString(strings.TrimSpace(currentParagraph.String()))
				result.WriteString("\n\n")
				currentParagraph.Reset()
			}
			continue
		}

		// Check if this line should start a new paragraph
		startsNewParagraph := false

		// Lines that end with colons are usually labels (keep separate)
		if strings.HasSuffix(trimmed, ":") && len(trimmed) < 40 {
			if currentParagraph.Len() > 0 {
				result.WriteString(strings.TrimSpace(currentParagraph.String()))
				result.WriteString("\n")
				currentParagraph.Reset()
			}
			result.WriteString(trimmed)
			result.WriteString(" ")
			continue
		}

		// Lines starting with bullets/numbers start new items
		if len(trimmed) > 0 {
			firstRune := []rune(trimmed)[0]
			if firstRune == '-' || firstRune == '*' || firstRune == '•' ||
				(len(trimmed) > 2 && trimmed[0] >= '0' && trimmed[0] <= '9' && (trimmed[1] == '.' || trimmed[1] == ')')) {
				startsNewParagraph = true
			}
		}

		// All caps short lines are usually headers
		if len(trimmed) < 60 && trimmed == strings.ToUpper(trimmed) && strings.ContainsAny(trimmed, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
			// Check if it's really a header (mostly letters)
			letters := 0
			for _, r := range trimmed {
				if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
					letters++
				}
			}
			if float64(letters)/float64(len(trimmed)) > 0.5 {
				startsNewParagraph = true
			}
		}

		if startsNewParagraph && currentParagraph.Len() > 0 {
			result.WriteString(strings.TrimSpace(currentParagraph.String()))
			result.WriteString("\n")
			currentParagraph.Reset()
		}

		// Add to current paragraph
		if currentParagraph.Len() > 0 {
			// Check if we need a space before joining
			prevContent := currentParagraph.String()
			if len(prevContent) > 0 {
				lastChar := prevContent[len(prevContent)-1]
				// Don't add space if previous ends with hyphen (word continuation)
				if lastChar == '-' {
					// Remove the hyphen and join directly
					currentParagraph.Reset()
					currentParagraph.WriteString(prevContent[:len(prevContent)-1])
					currentParagraph.WriteString(trimmed)
				} else {
					currentParagraph.WriteString(" ")
					currentParagraph.WriteString(trimmed)
				}
			}
		} else {
			currentParagraph.WriteString(trimmed)
		}

		// Check if this line ends a sentence (likely end of paragraph)
		if len(trimmed) > 0 {
			lastChar := trimmed[len(trimmed)-1]
			// If line ends with sentence-ending punctuation AND next line starts with capital
			// it might be end of paragraph
			if (lastChar == '.' || lastChar == '!' || lastChar == '?') && i+1 < len(lines) {
				nextTrimmed := strings.TrimSpace(lines[i+1])
				if len(nextTrimmed) > 0 {
					firstChar := rune(nextTrimmed[0])
					// If next line starts with capital and current line is reasonably long,
					// consider this a paragraph break
					if firstChar >= 'A' && firstChar <= 'Z' && len(trimmed) > 50 {
						result.WriteString(strings.TrimSpace(currentParagraph.String()))
						result.WriteString("\n")
						currentParagraph.Reset()
					}
				}
			}
		}
	}

	// Flush remaining content
	if currentParagraph.Len() > 0 {
		result.WriteString(strings.TrimSpace(currentParagraph.String()))
		result.WriteString("\n")
	}

	return strings.TrimSpace(result.String())
}

// isCourtTranscriptLayout detects if content is a court transcript format
func isCourtTranscriptLayout(content string) bool {
	lines := strings.Split(content, "\n")

	// Check for line numbers at start of lines
	numberedLines := 0
	qaPatterns := 0

	for _, line := range lines[:min(30, len(lines))] {
		trimmed := strings.TrimSpace(line)
		// Line starts with 1-2 digit number followed by space
		if regexp.MustCompile(`^\d{1,2}\s+`).MatchString(trimmed) {
			numberedLines++
		}
		// Q. or A. patterns
		if regexp.MustCompile(`^\s*[QA]\.\s`).MatchString(trimmed) {
			qaPatterns++
		}
	}

	return numberedLines > 10 || qaPatterns > 3
}

// enrichCandidate represents a page that may benefit from re-OCR or other enrichment.
type enrichCandidate struct {
	id         string
	content    string
	pdfPath    string // on-disk page PDF (from dir-based prepare)
	sourceFile string // source PDF filename (for zip-based lookup)
	pageNum    int    // page number within source PDF
	reasons    []string
	category   string // "ocr", "vision", or "quality"
}

// identifyEnrichCandidates scans prepare output records and returns pages whose content
// quality is below threshold. Each candidate includes the reasons it was flagged and a
// category: "ocr" (empty/short content needing OCR), "vision" (image pages needing
// captioning), or "quality" (substantial text with layout/encoding issues).
// Candidates must have either a page_pdf_path or a source_file + page_number for zip lookup.
func identifyEnrichCandidates(records map[string]map[string]any, minContentLen int, reprocess bool) []enrichCandidate {
	var candidates []enrichCandidate

	for id, rec := range records {
		content, _ := rec["content"].(string)
		meta, _ := rec["metadata"].(map[string]any)
		if meta == nil {
			continue
		}

		pdfPath, _ := meta["page_pdf_path"].(string)
		sourceFile, _ := meta["source_file"].(string)
		pageNum := 0
		if pn, ok := meta["page_number"].(float64); ok {
			pageNum = int(pn)
		}

		// Need either a disk path or source_file+page for zip lookup
		if pdfPath == "" && (sourceFile == "" || pageNum == 0) {
			continue
		}

		// Skip already-enriched pages unless reprocess is set
		if enriched, ok := meta["enriched"].(bool); ok && enriched && !reprocess {
			continue
		}

		trimmed := strings.TrimSpace(content)
		var reasons []string
		var category string

		switch {
		case len(trimmed) == 0:
			// Empty content: check if already tagged as image page from prior OCR pass
			pageType, _ := meta["page_type"].(string)
			if pageType == "image" {
				reasons = append(reasons, "image_page")
				category = "vision"
			} else {
				reasons = append(reasons, "empty")
				category = "ocr"
			}

		case len(trimmed) < minContentLen:
			reasons = append(reasons, "short_content")
			category = "ocr"

		default:
			// Substantial content: check quality heuristics
			if symbolRatio(content) > 0.15 {
				reasons = append(reasons, "high_symbols")
			}
			if isFragmentedLayout(content) {
				reasons = append(reasons, "fragmented")
			}
			if hasFontEncodingCorruption(content) {
				reasons = append(reasons, "font_corruption")
			}
			if len(reasons) > 0 {
				category = "quality"
			}
		}

		if len(reasons) > 0 {
			candidates = append(candidates, enrichCandidate{
				id:         id,
				content:    content,
				pdfPath:    pdfPath,
				sourceFile: sourceFile,
				pageNum:    pageNum,
				reasons:    reasons,
				category:   category,
			})
		}
	}

	return candidates
}

// buildZipIndex builds a map from base filename to zip entry for fast lookup.
func buildZipIndex(zipPaths []string) (map[string]*zip.File, []*zip.ReadCloser, error) {
	index := make(map[string]*zip.File)
	var readers []*zip.ReadCloser

	for _, zp := range zipPaths {
		r, err := zip.OpenReader(zp)
		if err != nil {
			// Close any already-opened readers
			for _, rc := range readers {
				rc.Close()
			}
			return nil, nil, fmt.Errorf("open zip %s: %w", zp, err)
		}
		readers = append(readers, r)

		for _, f := range r.File {
			if f.FileInfo().IsDir() {
				continue
			}
			name := strings.ToLower(filepath.Base(f.Name))
			if strings.HasSuffix(name, ".pdf") {
				index[filepath.Base(f.Name)] = f
			}
		}
	}

	return index, readers, nil
}

// extractPageFromZip extracts a specific page from a source PDF in a zip archive.
// Uses pdfcpu Trim to extract only the requested page instead of splitting all pages.
func extractPageFromZip(zipIndex map[string]*zip.File, sourceFile string, pageNum int) ([]byte, error) {
	f, ok := zipIndex[sourceFile]
	if !ok {
		return nil, fmt.Errorf("source file %q not found in zip", sourceFile)
	}

	rc, err := f.Open()
	if err != nil {
		return nil, fmt.Errorf("open zip entry: %w", err)
	}
	defer rc.Close()

	pdfData, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read zip entry: %w", err)
	}

	// Extract just the single page using Trim
	conf := model.NewDefaultConfiguration()
	var buf bytes.Buffer
	if err := api.Trim(bytes.NewReader(pdfData), &buf, []string{strconv.Itoa(pageNum)}, conf); err != nil {
		return nil, fmt.Errorf("trim page %d from %s: %w", pageNum, sourceFile, err)
	}

	return buf.Bytes(), nil
}

// enrichCmd implements the "enrich" subcommand: a second pass over prepare output
// that re-OCRs low-quality pages using Florence 2 (via Termite).
func enrichCmd(args []string) error {
	fs := flag.NewFlagSet("enrich", flag.ExitOnError)
	inputFile := fs.String("input", "epstein-docs.json", "Input JSON file from prepare")
	outputFile := fs.String("output", "", "Output JSON file (default: {input-base}-enriched.json)")
	termiteURL := fs.String("termite-url", "http://localhost:11433/api", "Termite service URL")
	model := fs.String("model", "onnx-community/Florence-2-base-ft", "Reader model for OCR")
	prompt := fs.String("prompt", "<OCR>", "Florence 2 task prompt")
	maxTokens := fs.Int("max-tokens", 4096, "Max generation tokens")
	dpi := fs.Float64("dpi", 150, "Render DPI for page images")
	minContentLen := fs.Int("min-content", 50, "Short content threshold (chars)")
	workers := fs.Int("workers", 4, "Concurrent enrichment workers")
	dryRun := fs.Bool("dry-run", false, "Report candidates only, don't process")
	reprocess := fs.Bool("reprocess", false, "Re-process already-enriched pages")
	category := fs.String("category", "ocr", "Category to process: ocr, vision, quality, or all")
	dirPath := fs.String("dir", "", "Base directory for resolving relative page_pdf_path values")

	var zipPaths StringSliceFlag
	fs.Var(&zipPaths, "zip", "Path to ZIP archive containing source PDFs (repeatable)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	// Derive output path
	outPath := *outputFile
	if outPath == "" {
		base := strings.TrimSuffix(*inputFile, filepath.Ext(*inputFile))
		outPath = base + "-enriched.json"
	}

	fmt.Printf("=== Epstein Documents Enrich ===\n\n")
	fmt.Printf("Input:  %s\n", *inputFile)
	fmt.Printf("Output: %s\n", outPath)
	fmt.Printf("Model:  %s\n", *model)
	fmt.Printf("Prompt: %s\n", *prompt)
	if len(zipPaths) > 0 {
		fmt.Printf("ZIP sources: %d\n", len(zipPaths))
		for _, zp := range zipPaths {
			fmt.Printf("  - %s\n", zp)
		}
	}
	fmt.Println()

	// Load input JSON
	jsonData, err := os.ReadFile(*inputFile)
	if err != nil {
		return fmt.Errorf("failed to read input: %w", err)
	}

	var records map[string]map[string]any
	if err := json.Unmarshal(jsonData, &records); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	fmt.Printf("Total records: %d\n", len(records))

	// Resolve relative page_pdf_path values if -dir is provided
	if *dirPath != "" {
		resolved := 0
		for _, rec := range records {
			meta, _ := rec["metadata"].(map[string]any)
			if meta == nil {
				continue
			}
			p, _ := meta["page_pdf_path"].(string)
			if p != "" && !filepath.IsAbs(p) {
				meta["page_pdf_path"] = filepath.Join(*dirPath, p)
				resolved++
			}
		}
		if resolved > 0 {
			fmt.Printf("Resolved %d relative page paths with -dir %s\n", resolved, *dirPath)
		}
	}

	// Identify all candidates
	allCandidates := identifyEnrichCandidates(records, *minContentLen, *reprocess)

	// Print category breakdown
	categoryCounts := map[string]int{}
	for _, c := range allCandidates {
		categoryCounts[c.category]++
	}
	fmt.Printf("All candidates: %d\n", len(allCandidates))
	fmt.Printf("Category breakdown:\n")
	for cat, count := range categoryCounts {
		fmt.Printf("  - %s: %d\n", cat, count)
	}

	// Print reason breakdown
	reasonCounts := map[string]int{}
	for _, c := range allCandidates {
		for _, r := range c.reasons {
			reasonCounts[r]++
		}
	}
	fmt.Printf("Reason breakdown:\n")
	for reason, count := range reasonCounts {
		fmt.Printf("  - %s: %d\n", reason, count)
	}
	fmt.Println()

	// Filter to selected category
	var candidates []enrichCandidate
	if *category == "all" {
		candidates = allCandidates
	} else {
		for _, c := range allCandidates {
			if c.category == *category {
				candidates = append(candidates, c)
			}
		}
	}
	fmt.Printf("Selected category %q: %d candidates\n\n", *category, len(candidates))

	if *dryRun {
		fmt.Printf("Dry run complete. Use without -dry-run to process.\n")
		return nil
	}

	if len(candidates) == 0 {
		fmt.Printf("No candidates to enrich. Writing unchanged output.\n")
		out, err := json.MarshalIndent(records, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal output: %w", err)
		}
		return os.WriteFile(outPath, out, 0644)
	}

	// Build zip index if zip sources provided
	var zipIndex map[string]*zip.File
	var zipReaders []*zip.ReadCloser
	if len(zipPaths) > 0 {
		fmt.Printf("Indexing ZIP archives...\n")
		zipIndex, zipReaders, err = buildZipIndex(zipPaths)
		if err != nil {
			return fmt.Errorf("build zip index: %w", err)
		}
		defer func() {
			for _, r := range zipReaders {
				r.Close()
			}
		}()
		fmt.Printf("  %d PDFs indexed from %d archives\n\n", len(zipIndex), len(zipPaths))
	}

	// Init OCR client
	ocrClient, err := NewOCRClient(*termiteURL, []string{*model}, *dpi)
	if err != nil {
		return fmt.Errorf("create OCR client: %w", err)
	}

	// Process candidates concurrently
	type enrichResult struct {
		id        string
		text      string
		model     string
		reasons   []string
		category  string
		origEmpty bool // original content was empty
		err       error
		replaced  bool
	}

	candidateCh := make(chan enrichCandidate, len(candidates))
	resultCh := make(chan enrichResult, len(candidates))

	for _, c := range candidates {
		candidateCh <- c
	}
	close(candidateCh)

	var processed atomic.Int64
	total := len(candidates)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range candidateCh {
				var pageData []byte

				// Try disk first, fall back to zip extraction
				if c.pdfPath != "" {
					pageData, _ = os.ReadFile(c.pdfPath)
				}
				if pageData == nil && zipIndex != nil && c.sourceFile != "" && c.pageNum > 0 {
					var err error
					pageData, err = extractPageFromZip(zipIndex, c.sourceFile, c.pageNum)
					if err != nil {
						resultCh <- enrichResult{id: c.id, err: fmt.Errorf("extract from zip: %w", err)}
						continue
					}
				}
				if pageData == nil {
					resultCh <- enrichResult{id: c.id, err: fmt.Errorf("no page source available")}
					continue
				}

				// Route through reader (OCR) or generator (vision) endpoint
				var text, usedModel string
				var callErr error
				if c.category == "vision" {
					text, usedModel, callErr = ocrClient.GeneratePageWithPrompt(context.Background(), pageData, *prompt, *maxTokens)
				} else {
					text, usedModel, callErr = ocrClient.ReadPageWithPrompt(context.Background(), pageData, *prompt, *maxTokens)
				}
				if callErr != nil {
					resultCh <- enrichResult{id: c.id, err: callErr, category: c.category, origEmpty: len(strings.TrimSpace(c.content)) == 0}
					continue
				}

				origEmpty := len(strings.TrimSpace(c.content)) == 0

				// Per-category replacement rules
				replaced := false
				if text != "" {
					switch c.category {
					case "ocr":
						// Replace if OCR text is longer or passes quality checks where original didn't
						origFails := needsOCRFallback(c.content, *minContentLen)
						newFails := needsOCRFallback(text, *minContentLen)
						if len(text) > len(c.content) || (origFails && !newFails) {
							replaced = true
						}
					case "vision":
						// Any caption text is an improvement over empty content
						replaced = true
					case "quality":
						// Replace only if significantly better (len > 2x original)
						if len(text) > 2*len(c.content) {
							replaced = true
						}
					}
				}

				n := processed.Add(1)
				if n%100 == 0 || int(n) == total {
					fmt.Printf("  Processed %d/%d candidates...\n", n, total)
				}

				resultCh <- enrichResult{
					id:        c.id,
					text:      text,
					model:     usedModel,
					reasons:   c.reasons,
					category:  c.category,
					origEmpty: origEmpty,
					replaced:  replaced,
				}
			}
		}()
	}

	// Close results when workers done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results
	var enriched, skipped, errors int
	for r := range resultCh {
		if r.err != nil {
			errors++
			log.Printf("Error enriching %s: %v", r.id, r.err)
			// Still tag empty-content pages as image so vision pass can pick them up
			if r.origEmpty && r.category == "ocr" {
				rec := records[r.id]
				meta, _ := rec["metadata"].(map[string]any)
				if meta == nil {
					meta = map[string]any{}
					rec["metadata"] = meta
				}
				meta["page_type"] = "image"
			}
			continue
		}

		rec := records[r.id]
		meta, _ := rec["metadata"].(map[string]any)
		if meta == nil {
			meta = map[string]any{}
			rec["metadata"] = meta
		}

		// Tag empty-content pages that returned no OCR text as image pages
		// so a future vision pass can pick them up.
		if r.origEmpty && strings.TrimSpace(r.text) == "" {
			meta["page_type"] = "image"
		}

		if !r.replaced {
			skipped++
			continue
		}

		enriched++

		// Update record
		rec["content"] = r.text
		meta["enriched"] = true
		meta["enrich_model"] = r.model
		meta["enrich_reasons"] = r.reasons
		meta["enrich_category"] = r.category
	}

	fmt.Printf("\n=== Enrich Summary ===\n")
	fmt.Printf("  Enriched: %d\n", enriched)
	fmt.Printf("  Skipped (no improvement): %d\n", skipped)
	fmt.Printf("  Errors: %d\n", errors)

	// Write output
	out, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal output: %w", err)
	}

	if err := os.WriteFile(outPath, out, 0644); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	fmt.Printf("\nWrote %s\n", outPath)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "epstein - Epstein Documents Search Tool\n\n")
		fmt.Fprintf(os.Stderr, "A tool to download, index, and search the publicly released\n")
		fmt.Fprintf(os.Stderr, "Epstein court documents and DOJ files using Antfly.\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  epstein download [flags]  - Download documents from archive.org\n")
		fmt.Fprintf(os.Stderr, "  epstein prepare [flags]   - Process PDFs and create JSON data\n")
		fmt.Fprintf(os.Stderr, "  epstein load [flags]      - Load JSON data into Antfly\n")
		fmt.Fprintf(os.Stderr, "  epstein sync [flags]      - Full pipeline (process + load)\n")
		fmt.Fprintf(os.Stderr, "  epstein serve [flags]     - Start web search interface\n")
		fmt.Fprintf(os.Stderr, "  epstein audit [flags]     - Audit parsed documents for errors\n")
		fmt.Fprintf(os.Stderr, "  epstein enrich [flags]    - Re-OCR low-quality pages with Florence 2\n")
		fmt.Fprintf(os.Stderr, "\nQuick Start:\n")
		fmt.Fprintf(os.Stderr, "  # 1. Start Antfly\n")
		fmt.Fprintf(os.Stderr, "  go run ./cmd/antfly swarm\n\n")
		fmt.Fprintf(os.Stderr, "  # 2. Download documents (choose dataset)\n")
		fmt.Fprintf(os.Stderr, "  epstein download --dataset court-2024    # ~23MB, 943 pages\n")
		fmt.Fprintf(os.Stderr, "  epstein download --dataset doj-complete  # ~4.8GB, 8 datasets (Dec 2025)\n")
		fmt.Fprintf(os.Stderr, "  epstein download --dataset doj-jan2026   # ~104GB, 3 datasets (Jan 2026)\n\n")
		fmt.Fprintf(os.Stderr, "  # 3. Prepare with page splitting (enables PDF viewing)\n")
		fmt.Fprintf(os.Stderr, "  epstein prepare --split-pages\n\n")
		fmt.Fprintf(os.Stderr, "  # 3b. (Optional) Enable OCR for scanned documents\n")
		fmt.Fprintf(os.Stderr, "  #     Requires Termite running with OCR models\n")
		fmt.Fprintf(os.Stderr, "  epstein prepare --split-pages --enable-ocr\n\n")
		fmt.Fprintf(os.Stderr, "  # 4. Index and load\n")
		fmt.Fprintf(os.Stderr, "  epstein load --create-table\n\n")
		fmt.Fprintf(os.Stderr, "  # 5. Start search UI\n")
		fmt.Fprintf(os.Stderr, "  epstein serve\n\n")
		fmt.Fprintf(os.Stderr, "OCR Fallback:\n")
		fmt.Fprintf(os.Stderr, "  The --enable-ocr flag enables automatic OCR when text extraction fails.\n")
		fmt.Fprintf(os.Stderr, "  This requires Termite running with OCR models (trocr, florence-2).\n")
		fmt.Fprintf(os.Stderr, "  Pages are rendered to images and sent to Termite for text recognition.\n")
		fmt.Fprintf(os.Stderr, "  Useful for scanned documents or PDFs with garbled text extraction.\n\n")
		fmt.Fprintf(os.Stderr, "Datasets:\n")
		fmt.Fprintf(os.Stderr, "  court-2024    January 2024 court unsealing (Giuffre v. Maxwell)\n")
		fmt.Fprintf(os.Stderr, "  doj-complete  DOJ December 2025 release, datasets 1-8 (EFTA)\n")
		fmt.Fprintf(os.Stderr, "  doj-jan2026   DOJ January 2026 release, datasets 10-12 (EFTA)\n")
		fmt.Fprintf(os.Stderr, "                Note: Dataset 9 excluded due to incomplete release\n")
		fmt.Fprintf(os.Stderr, "  all           All datasets\n")
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "download":
		err = downloadCmd(os.Args[2:])
	case "prepare":
		err = prepareCmd(os.Args[2:])
	case "load":
		err = loadCmd(os.Args[2:])
	case "sync":
		err = syncCmd(os.Args[2:])
	case "serve":
		err = serveCmd(os.Args[2:])
	case "audit":
		err = auditCmd(os.Args[2:])
	case "enrich":
		err = enrichCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		fmt.Fprintf(os.Stderr, "Valid commands: download, prepare, load, sync, serve, audit, enrich\n")
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}
