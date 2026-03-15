package main

import (
	"archive/zip"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
)

// createMinimalPDF creates a valid single-page PDF in memory.
func createMinimalPDF() []byte {
	var buf bytes.Buffer
	buf.WriteString("%PDF-1.4\n")

	off1 := buf.Len()
	buf.WriteString("1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")

	off2 := buf.Len()
	buf.WriteString("2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n")

	off3 := buf.Len()
	buf.WriteString("3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\nendobj\n")

	xrefOff := buf.Len()
	buf.WriteString("xref\n")
	buf.WriteString("0 4\n")
	buf.WriteString("0000000000 65535 f \n")
	fmt.Fprintf(&buf, "%010d 00000 n \n", off1)
	fmt.Fprintf(&buf, "%010d 00000 n \n", off2)
	fmt.Fprintf(&buf, "%010d 00000 n \n", off3)
	buf.WriteString("trailer\n<< /Size 4 /Root 1 0 R >>\n")
	buf.WriteString("startxref\n")
	fmt.Fprintf(&buf, "%d\n", xrefOff)
	buf.WriteString("%%EOF\n")

	return buf.Bytes()
}

// createMultiPagePDF creates a valid multi-page PDF in memory.
func createMultiPagePDF(numPages int) []byte {
	var buf bytes.Buffer
	buf.WriteString("%PDF-1.4\n")

	offsets := make([]int, 0, numPages+2)

	// Object 1: Catalog
	offsets = append(offsets, buf.Len())
	buf.WriteString("1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")

	// Object 2: Pages
	offsets = append(offsets, buf.Len())
	kids := make([]string, numPages)
	for i := range numPages {
		kids[i] = fmt.Sprintf("%d 0 R", i+3)
	}
	fmt.Fprintf(&buf, "2 0 obj\n<< /Type /Pages /Kids [%s] /Count %d >>\nendobj\n",
		strings.Join(kids, " "), numPages)

	// Page objects
	for i := range numPages {
		offsets = append(offsets, buf.Len())
		fmt.Fprintf(&buf, "%d 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\nendobj\n", i+3)
	}

	// Cross-reference table
	numObjs := numPages + 3
	xrefOff := buf.Len()
	buf.WriteString("xref\n")
	fmt.Fprintf(&buf, "0 %d\n", numObjs)
	buf.WriteString("0000000000 65535 f \n")
	for _, off := range offsets {
		fmt.Fprintf(&buf, "%010d 00000 n \n", off)
	}

	buf.WriteString("trailer\n")
	fmt.Fprintf(&buf, "<< /Size %d /Root 1 0 R >>\n", numObjs)
	buf.WriteString("startxref\n")
	fmt.Fprintf(&buf, "%d\n", xrefOff)
	buf.WriteString("%%EOF\n")

	return buf.Bytes()
}

// createTestZip creates a zip file in a temp directory with the given files.
func createTestZip(t *testing.T, files map[string][]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.zip")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	for name, data := range files {
		fw, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fw.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExtractOneZipPDF(t *testing.T) {
	pdfData := createMinimalPDF()
	zipPath := createTestZip(t, map[string][]byte{
		"subdir/test.pdf": pdfData,
	})

	// Open the zip and get the file entry
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if len(r.File) != 1 {
		t.Fatalf("expected 1 file in zip, got %d", len(r.File))
	}

	extractDir := t.TempDir()
	if err := extractOneZipPDF(r.File[0], extractDir); err != nil {
		t.Fatalf("extractOneZipPDF failed: %v", err)
	}

	// Verify file exists with flattened name (base name only)
	outPath := filepath.Join(extractDir, "test.pdf")
	outData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("output file not found: %v", err)
	}

	if !bytes.Equal(outData, pdfData) {
		t.Errorf("output content mismatch: got %d bytes, want %d bytes", len(outData), len(pdfData))
	}
}

func TestExtractZipPDFs_Parallel(t *testing.T) {
	pdfData := createMinimalPDF()

	// Create a zip with 50 PDFs
	files := make(map[string][]byte, 50)
	for i := range 50 {
		files[fmt.Sprintf("dir%d/doc_%03d.pdf", i%5, i)] = pdfData
	}
	zipPath := createTestZip(t, files)

	extractDir := t.TempDir()
	count, err := extractZipPDFs(zipPath, extractDir)
	if err != nil {
		t.Fatalf("extractZipPDFs failed: %v", err)
	}

	if count != 50 {
		t.Errorf("expected 50 extracted, got %d", count)
	}

	// Verify all files extracted with correct content
	entries, err := os.ReadDir(extractDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 50 {
		t.Errorf("expected 50 files in extract dir, got %d", len(entries))
	}

	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(extractDir, entry.Name()))
		if err != nil {
			t.Errorf("failed to read %s: %v", entry.Name(), err)
			continue
		}
		if !bytes.Equal(data, pdfData) {
			t.Errorf("content mismatch for %s: got %d bytes, want %d bytes",
				entry.Name(), len(data), len(pdfData))
		}
	}
}

func TestIterateZipPDFs(t *testing.T) {
	pdfData := createMinimalPDF()
	txtData := []byte("not a pdf")
	jpgData := []byte("\xff\xd8\xff fake jpeg")

	zipPath := createTestZip(t, map[string][]byte{
		"docs/report.pdf":    pdfData,
		"docs/summary.PDF":   pdfData, // uppercase extension
		"docs/readme.txt":    txtData,
		"images/photo.jpg":   jpgData,
		"nested/a/b/deep.pdf": pdfData,
	})

	type result struct {
		name string
		data []byte
	}
	var results []result

	err := iterateZipPDFs(zipPath, func(name string, data []byte) error {
		results = append(results, result{name: name, data: data})
		return nil
	})
	if err != nil {
		t.Fatalf("iterateZipPDFs failed: %v", err)
	}

	// Should only yield PDF files
	if len(results) != 3 {
		t.Fatalf("expected 3 PDFs, got %d", len(results))
	}

	// Verify filenames are flattened (base name only)
	names := make(map[string]bool)
	for _, r := range results {
		names[r.name] = true
		if !bytes.Equal(r.data, pdfData) {
			t.Errorf("data mismatch for %s", r.name)
		}
		// Should not contain directory separators
		if strings.Contains(r.name, "/") {
			t.Errorf("expected flattened name, got %q", r.name)
		}
	}

	if !names["report.pdf"] {
		t.Error("missing report.pdf")
	}
	if !names["summary.PDF"] {
		t.Error("missing summary.PDF")
	}
	if !names["deep.pdf"] {
		t.Error("missing deep.pdf")
	}
}

func TestIterateZipPDFs_Empty(t *testing.T) {
	zipPath := createTestZip(t, map[string][]byte{
		"readme.txt":  []byte("hello"),
		"data.csv":    []byte("a,b,c"),
	})

	called := false
	err := iterateZipPDFs(zipPath, func(name string, data []byte) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("iterateZipPDFs failed: %v", err)
	}
	if called {
		t.Error("callback should not have been called for zip with no PDFs")
	}
}

func TestSplitPDFToPagesFromBytes(t *testing.T) {
	pdfData := createMultiPagePDF(3)

	// Verify the test PDF is valid and has 3 pages
	conf := model.NewDefaultConfiguration()
	pc, err := api.PageCount(bytes.NewReader(pdfData), conf)
	if err != nil {
		t.Fatalf("test PDF invalid: %v", err)
	}
	if pc != 3 {
		t.Fatalf("test PDF has %d pages, expected 3", pc)
	}

	pageBytes, metadata, err := splitPDFToPagesFromBytes(pdfData, "test-doc.pdf")
	if err != nil {
		t.Fatalf("splitPDFToPagesFromBytes failed: %v", err)
	}

	// Verify correct number of pages
	if len(pageBytes) != 3 {
		t.Errorf("expected 3 pages, got %d", len(pageBytes))
	}

	// Verify each page is a valid single-page PDF
	for pageNum, data := range pageBytes {
		pc, err := api.PageCount(bytes.NewReader(data), conf)
		if err != nil {
			t.Errorf("page %d is not a valid PDF: %v", pageNum, err)
			continue
		}
		if pc != 1 {
			t.Errorf("page %d has %d pages, expected 1", pageNum, pc)
		}
	}

	// Verify metadata
	if metadata == nil {
		t.Fatal("metadata is nil")
	}
	if metadata.TotalPages != 3 {
		t.Errorf("metadata.TotalPages = %d, want 3", metadata.TotalPages)
	}
	if metadata.SourceFile != "test-doc.pdf" {
		t.Errorf("metadata.SourceFile = %q, want %q", metadata.SourceFile, "test-doc.pdf")
	}
}

func TestIdentifyEnrichCandidates(t *testing.T) {
	tests := []struct {
		name         string
		records      map[string]map[string]any
		minContent   int
		reprocess    bool
		wantIDs      []string            // expected candidate IDs (order-independent)
		wantReason   map[string][]string  // id → expected reasons
		wantCategory map[string]string    // id → expected category
	}{
		{
			name: "short content triggers short_content with ocr category",
			records: map[string]map[string]any{
				"page-1": {
					"content": "hi",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"page-1"},
			wantReason: map[string][]string{
				"page-1": {"short_content"},
			},
			wantCategory: map[string]string{
				"page-1": "ocr",
			},
		},
		{
			name: "empty content triggers empty with ocr category",
			records: map[string]map[string]any{
				"page-1": {
					"content": "",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"page-1"},
			wantReason: map[string][]string{
				"page-1": {"empty"},
			},
			wantCategory: map[string]string{
				"page-1": "ocr",
			},
		},
		{
			name: "empty content with page_type=image triggers vision category",
			records: map[string]map[string]any{
				"page-1": {
					"content": "",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
						"page_type":     "image",
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"page-1"},
			wantReason: map[string][]string{
				"page-1": {"image_page"},
			},
			wantCategory: map[string]string{
				"page-1": "vision",
			},
		},
		{
			name: "good content not flagged",
			records: map[string]map[string]any{
				"page-1": {
					"content": "This is a perfectly normal paragraph of text that contains enough characters to pass the minimum content length check and has no quality issues whatsoever.",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
					},
				},
			},
			minContent: 50,
			wantIDs:    nil,
		},
		{
			name: "source_file + page_number works without page_pdf_path",
			records: map[string]map[string]any{
				"page-1": {
					"content": "short",
					"metadata": map[string]any{
						"source_file": "test.pdf",
						"page_number": float64(1),
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"page-1"},
			wantReason: map[string][]string{
				"page-1": {"short_content"},
			},
			wantCategory: map[string]string{
				"page-1": "ocr",
			},
		},
		{
			name: "no page source skipped",
			records: map[string]map[string]any{
				"page-1": {
					"content": "short",
					"metadata": map[string]any{},
				},
			},
			minContent: 50,
			wantIDs:    nil,
		},
		{
			name: "no metadata skipped",
			records: map[string]map[string]any{
				"page-1": {
					"content": "short",
				},
			},
			minContent: 50,
			wantIDs:    nil,
		},
		{
			name: "already enriched skipped by default",
			records: map[string]map[string]any{
				"page-1": {
					"content": "x",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
						"enriched":      true,
					},
				},
			},
			minContent: 50,
			reprocess:  false,
			wantIDs:    nil,
		},
		{
			name: "already enriched reprocessed when flag set",
			records: map[string]map[string]any{
				"page-1": {
					"content": "x",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
						"enriched":      true,
					},
				},
			},
			minContent: 50,
			reprocess:  true,
			wantIDs:    []string{"page-1"},
			wantCategory: map[string]string{
				"page-1": "ocr",
			},
		},
		{
			name: "high symbol ratio flagged with quality category",
			records: map[string]map[string]any{
				"page-1": {
					// Content with lots of unusual symbols but long enough to pass minContent
					"content": strings.Repeat("§±†‡™®©℗℠", 20),
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
					},
				},
			},
			minContent: 10,
			wantIDs:    []string{"page-1"},
			wantCategory: map[string]string{
				"page-1": "quality",
			},
		},
		{
			name: "multiple records mixed",
			records: map[string]map[string]any{
				"good": {
					"content": "This document contains a thorough discussion of the legal proceedings that took place in the Southern District of Florida during the year two thousand and eight.",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/good.pdf",
					},
				},
				"bad": {
					"content": "",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/bad.pdf",
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"bad"},
			wantCategory: map[string]string{
				"bad": "ocr",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates := identifyEnrichCandidates(tt.records, tt.minContent, tt.reprocess)

			gotIDs := make(map[string]bool)
			gotReasons := make(map[string][]string)
			gotCategories := make(map[string]string)
			for _, c := range candidates {
				gotIDs[c.id] = true
				gotReasons[c.id] = c.reasons
				gotCategories[c.id] = c.category
			}

			wantIDSet := make(map[string]bool)
			for _, id := range tt.wantIDs {
				wantIDSet[id] = true
			}

			if len(gotIDs) != len(wantIDSet) {
				t.Errorf("got %d candidates, want %d", len(gotIDs), len(wantIDSet))
			}

			for id := range wantIDSet {
				if !gotIDs[id] {
					t.Errorf("missing expected candidate %q", id)
				}
			}
			for id := range gotIDs {
				if !wantIDSet[id] {
					t.Errorf("unexpected candidate %q", id)
				}
			}

			// Check expected reasons if specified
			for id, wantReasons := range tt.wantReason {
				got := gotReasons[id]
				for _, wr := range wantReasons {
					found := false
					for _, gr := range got {
						if gr == wr {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("candidate %q: missing expected reason %q (got %v)", id, wr, got)
					}
				}
			}

			// Check expected categories if specified
			for id, wantCat := range tt.wantCategory {
				if gotCat := gotCategories[id]; gotCat != wantCat {
					t.Errorf("candidate %q: got category %q, want %q", id, gotCat, wantCat)
				}
			}
		})
	}
}

func TestIdentifyEnrichCandidates_FragmentedLayout(t *testing.T) {
	// Build content that triggers isFragmentedLayout: >60% of lines shorter than 20 chars,
	// with at least 10 lines
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, fmt.Sprintf("line %d", i))
	}
	fragmented := strings.Join(lines, "\n")

	records := map[string]map[string]any{
		"frag": {
			"content": fragmented,
			"metadata": map[string]any{
				"page_pdf_path": "/tmp/frag.pdf",
			},
		},
	}

	candidates := identifyEnrichCandidates(records, 10, false)
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}

	found := false
	for _, r := range candidates[0].reasons {
		if r == "fragmented" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'fragmented' reason, got %v", candidates[0].reasons)
	}
	if candidates[0].category != "quality" {
		t.Errorf("expected 'quality' category, got %q", candidates[0].category)
	}
}

func TestSplitPDFToPagesFromBytes_SinglePage(t *testing.T) {
	pdfData := createMinimalPDF()

	// Verify the test PDF is valid
	conf := model.NewDefaultConfiguration()
	pc, err := api.PageCount(bytes.NewReader(pdfData), conf)
	if err != nil {
		t.Fatalf("test PDF invalid: %v", err)
	}
	if pc != 1 {
		t.Fatalf("test PDF has %d pages, expected 1", pc)
	}

	pageBytes, metadata, err := splitPDFToPagesFromBytes(pdfData, "single.pdf")
	if err != nil {
		t.Fatalf("splitPDFToPagesFromBytes failed: %v", err)
	}

	// Should return exactly 1 page
	if len(pageBytes) != 1 {
		t.Fatalf("expected 1 page, got %d", len(pageBytes))
	}

	// Single-page optimization: should return original bytes directly
	if !bytes.Equal(pageBytes[1], pdfData) {
		t.Error("single-page PDF should return original bytes (skip split optimization)")
	}

	if metadata.TotalPages != 1 {
		t.Errorf("metadata.TotalPages = %d, want 1", metadata.TotalPages)
	}
}
