package docsaf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/time/rate"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

func TestGoogleDriveSource_Type(t *testing.T) {
	src := &GoogleDriveSource{
		config: GoogleDriveSourceConfig{
			FolderID: "test-folder-id",
		},
	}
	if src.Type() != "google_drive" {
		t.Errorf("Type() = %q, want %q", src.Type(), "google_drive")
	}
}

func TestGoogleDriveSource_BaseURL(t *testing.T) {
	tests := []struct {
		name     string
		config   GoogleDriveSourceConfig
		expected string
	}{
		{
			name: "Custom BaseURL",
			config: GoogleDriveSourceConfig{
				FolderID: "abc123",
				BaseURL:  "https://docs.example.com",
			},
			expected: "https://docs.example.com",
		},
		{
			name: "Default BaseURL",
			config: GoogleDriveSourceConfig{
				FolderID: "abc123",
			},
			expected: "https://drive.google.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &GoogleDriveSource{config: tt.config}
			got := src.BaseURL()
			if got != tt.expected {
				t.Errorf("BaseURL() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestGoogleDriveSource_IncludeSharedDrives(t *testing.T) {
	boolPtr := func(v bool) *bool { return &v }

	tests := []struct {
		name     string
		config   GoogleDriveSourceConfig
		expected bool
	}{
		{
			name:     "Nil defaults to true",
			config:   GoogleDriveSourceConfig{},
			expected: true,
		},
		{
			name:     "Explicit true",
			config:   GoogleDriveSourceConfig{IncludeSharedDrives: boolPtr(true)},
			expected: true,
		},
		{
			name:     "Explicit false",
			config:   GoogleDriveSourceConfig{IncludeSharedDrives: boolPtr(false)},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &GoogleDriveSource{config: tt.config}
			got := src.includeSharedDrives()
			if got != tt.expected {
				t.Errorf("includeSharedDrives() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestGoogleDriveSource_ShouldExclude(t *testing.T) {
	src := &GoogleDriveSource{
		config: GoogleDriveSourceConfig{
			ExcludePatterns: []string{
				"**/*.tmp",
				"drafts/**",
				"**/.DS_Store",
			},
		},
	}

	tests := []struct {
		path     string
		expected bool
	}{
		{"README.md", false},
		{"docs/guide.md", false},
		{"temp.tmp", true},
		{"docs/temp.tmp", true},
		{"drafts/new.md", true},
		{"drafts/folder/doc.md", true},
		{".DS_Store", true},
		{"docs/.DS_Store", true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := src.shouldExclude(tt.path)
			if got != tt.expected {
				t.Errorf("shouldExclude(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

func TestGoogleDriveSource_ShouldInclude(t *testing.T) {
	tests := []struct {
		name            string
		includePatterns []string
		path            string
		expected        bool
	}{
		{
			name:            "No patterns - include all",
			includePatterns: []string{},
			path:            "any/file.txt",
			expected:        true,
		},
		{
			name:            "Match markdown files",
			includePatterns: []string{"**/*.md"},
			path:            "docs/guide.md",
			expected:        true,
		},
		{
			name:            "No match markdown files",
			includePatterns: []string{"**/*.md"},
			path:            "docs/image.png",
			expected:        false,
		},
		{
			name:            "Multiple patterns - match first",
			includePatterns: []string{"**/*.md", "**/*.mdx"},
			path:            "docs/guide.md",
			expected:        true,
		},
		{
			name:            "Multiple patterns - match second",
			includePatterns: []string{"**/*.md", "**/*.mdx"},
			path:            "docs/component.mdx",
			expected:        true,
		},
		{
			name:            "Multiple patterns - no match",
			includePatterns: []string{"**/*.md", "**/*.mdx"},
			path:            "docs/script.js",
			expected:        false,
		},
		{
			name:            "Exact directory match",
			includePatterns: []string{"docs/**"},
			path:            "docs/file.txt",
			expected:        true,
		},
		{
			name:            "Wrong directory",
			includePatterns: []string{"docs/**"},
			path:            "other/file.txt",
			expected:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &GoogleDriveSource{
				config: GoogleDriveSourceConfig{
					IncludePatterns: tt.includePatterns,
				},
			}
			got := src.shouldInclude(tt.path)
			if got != tt.expected {
				t.Errorf("shouldInclude(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

func TestGoogleDriveSource_PatternCombinations(t *testing.T) {
	src := &GoogleDriveSource{
		config: GoogleDriveSourceConfig{
			IncludePatterns: []string{"**/*.md", "**/*.mdx"},
			ExcludePatterns: []string{"**/drafts/**", "**/*.draft.md"},
		},
	}

	tests := []struct {
		path           string
		shouldInclude  bool
		shouldExclude  bool
		expectedResult bool
	}{
		{"docs/guide.md", true, false, true},
		{"docs/component.mdx", true, false, true},
		{"docs/image.png", false, false, false},
		{"drafts/new.md", true, true, false},
		{"docs/drafts/temp.md", true, true, false},
		{"docs/new.draft.md", true, true, false},
		{"docs/final.md", true, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			include := src.shouldInclude(tt.path)
			exclude := src.shouldExclude(tt.path)

			if include != tt.shouldInclude {
				t.Errorf("shouldInclude(%q) = %v, want %v", tt.path, include, tt.shouldInclude)
			}
			if exclude != tt.shouldExclude {
				t.Errorf("shouldExclude(%q) = %v, want %v", tt.path, exclude, tt.shouldExclude)
			}

			result := include && !exclude
			if result != tt.expectedResult {
				t.Errorf("Processing decision for %q = %v, want %v", tt.path, result, tt.expectedResult)
			}
		})
	}
}

func TestGoogleDriveSource_DefaultConcurrency(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
		expected    int
	}{
		{"Default (zero)", 0, 5},
		{"Negative", -1, 5},
		{"Explicit value", 10, 10},
		{"Low value", 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := GoogleDriveSourceConfig{
				FolderID:    "test",
				Concurrency: tt.concurrency,
			}

			if config.Concurrency <= 0 {
				config.Concurrency = 5
			}

			if config.Concurrency != tt.expected {
				t.Errorf("Concurrency = %d, want %d", config.Concurrency, tt.expected)
			}
		})
	}
}

func TestGoogleDriveSource_WorkspaceExportFormats(t *testing.T) {
	// Verify exportable types
	expected := map[string]string{
		"application/vnd.google-apps.document":     "text/html",
		"application/vnd.google-apps.presentation": "application/pdf",
		"application/vnd.google-apps.form":         "text/plain",
	}

	for mimeType, exportType := range expected {
		got, ok := workspaceExportFormats[mimeType]
		if !ok {
			t.Errorf("Missing export format for %s", mimeType)
			continue
		}
		if got != exportType {
			t.Errorf("workspaceExportFormats[%s] = %q, want %q", mimeType, got, exportType)
		}
	}

	// Verify skipped types
	skipped := []string{
		"application/vnd.google-apps.spreadsheet",
		"application/vnd.google-apps.drawing",
		"application/vnd.google-apps.map",
		"application/vnd.google-apps.site",
		"application/vnd.google-apps.shortcut",
		"application/vnd.google-apps.folder",
	}

	for _, mimeType := range skipped {
		if !workspaceSkipTypes[mimeType] {
			t.Errorf("Expected %s to be in workspaceSkipTypes", mimeType)
		}
	}
}

func TestReadLimitedDriveContentRejectsOversize(t *testing.T) {
	data, err := readLimitedDriveContent(strings.NewReader("12345"), 5)
	if err != nil {
		t.Fatalf("readLimitedDriveContent exact limit: %v", err)
	}
	if string(data) != "12345" {
		t.Fatalf("data = %q, want 12345", data)
	}

	_, err = readLimitedDriveContent(strings.NewReader("123456"), 5)
	if err == nil || !strings.Contains(err.Error(), "exceeds download limit of 5 bytes") {
		t.Fatalf("readLimitedDriveContent oversize error = %v, want limit error", err)
	}
}

func TestGoogleDriveSourceTraversePropagatesDownloadErrors(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/files" && r.URL.Query().Get("alt") != "media":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"files": []map[string]any{
					{
						"id":           "file-id",
						"name":         "huge.txt",
						"mimeType":     "text/plain",
						"size":         "12",
						"modifiedTime": "2026-01-02T03:04:05.000Z",
						"md5Checksum":  "checksum",
						"webViewLink":  "https://drive.google.com/file/d/file-id/view",
					},
				},
			}); err != nil {
				t.Fatalf("encode list response: %v", err)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/files/file-id" && r.URL.Query().Get("alt") == "media":
			http.Error(w, "download failed", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected Drive API request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer server.Close()

	service, err := drive.NewService(ctx, option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("new Drive service: %v", err)
	}
	source := &GoogleDriveSource{
		config:    GoogleDriveSourceConfig{FolderID: "folder-id"},
		service:   service,
		semaphore: make(chan struct{}, 1),
		limiter:   rate.NewLimiter(rate.Inf, 1),
	}

	items, errs := source.Traverse(ctx)
	for item := range items {
		t.Fatalf("unexpected item emitted after failed download: %#v", item)
	}
	var gotErr error
	for err := range errs {
		gotErr = err
	}
	if gotErr == nil || !strings.Contains(gotErr.Error(), "failed to download huge.txt") {
		t.Fatalf("Traverse error = %v, want download failure", gotErr)
	}
}

func TestParseFolderID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Raw folder ID",
			input:    "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2wtIs",
			expected: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2wtIs",
		},
		{
			name:     "Short folder ID",
			input:    "abc123",
			expected: "abc123",
		},
		{
			name:     "Full Drive URL",
			input:    "https://drive.google.com/drive/folders/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2wtIs",
			expected: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2wtIs",
		},
		{
			name:     "Drive URL with user prefix",
			input:    "https://drive.google.com/drive/u/0/folders/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2wtIs",
			expected: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2wtIs",
		},
		{
			name:     "Drive URL with query params",
			input:    "https://drive.google.com/drive/folders/abc123?resourcekey=xyz",
			expected: "abc123",
		},
		{
			name:     "Drive URL with trailing slash",
			input:    "https://drive.google.com/drive/folders/abc123/",
			expected: "abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseFolderID(tt.input)
			if got != tt.expected {
				t.Errorf("parseFolderID(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestNewGoogleDriveSource_Validation(t *testing.T) {
	ctx := t.Context()

	// Missing credentials
	_, err := NewGoogleDriveSource(ctx, GoogleDriveSourceConfig{
		FolderID: "abc123",
	})
	if err == nil {
		t.Error("Expected error for missing credentials")
	}

	// Missing folder ID
	_, err = NewGoogleDriveSource(ctx, GoogleDriveSourceConfig{
		AccessToken: "fake-token",
	})
	if err == nil {
		t.Error("Expected error for missing FolderID")
	}
}
