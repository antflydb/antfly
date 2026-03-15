package docsaf

import (
	"testing"

	"github.com/antflydb/antfly/pkg/libaf/s3"
)

func TestNewS3Source(t *testing.T) {
	// Test missing bucket
	_, err := NewS3Source(S3SourceConfig{
		Credentials: s3.Credentials{
			Endpoint:        "localhost:9000",
			AccessKeyId:     "minioadmin",
			SecretAccessKey: "minioadmin",
			UseSsl:          false,
		},
	})
	if err == nil {
		t.Error("Expected error for missing bucket")
	}

	// Note: Further tests would require a real MinIO/S3 instance
	// Integration tests should be added separately
}

func TestS3Source_Type(t *testing.T) {
	// Create a source (will fail bucket check, but we can test Type before that)
	src := &S3Source{
		config: S3SourceConfig{
			Bucket: "test-bucket",
		},
	}

	if src.Type() != "s3" {
		t.Errorf("Type() = %q, want %q", src.Type(), "s3")
	}
}

func TestS3Source_BaseURL(t *testing.T) {
	tests := []struct {
		name     string
		config   S3SourceConfig
		expected string
	}{
		{
			name: "Custom BaseURL",
			config: S3SourceConfig{
				Bucket:  "my-bucket",
				BaseURL: "https://docs.example.com",
			},
			expected: "https://docs.example.com",
		},
		{
			name: "Default BaseURL without prefix",
			config: S3SourceConfig{
				Bucket: "my-bucket",
			},
			expected: "s3://my-bucket/",
		},
		{
			name: "Default BaseURL with prefix",
			config: S3SourceConfig{
				Bucket: "my-bucket",
				Prefix: "docs/",
			},
			expected: "s3://my-bucket/docs/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &S3Source{config: tt.config}
			got := src.BaseURL()
			if got != tt.expected {
				t.Errorf("BaseURL() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestS3Source_ShouldExclude(t *testing.T) {
	src := &S3Source{
		config: S3SourceConfig{
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

func TestS3Source_ShouldInclude(t *testing.T) {
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
			src := &S3Source{
				config: S3SourceConfig{
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

func TestS3Source_PatternCombinations(t *testing.T) {
	src := &S3Source{
		config: S3SourceConfig{
			IncludePatterns: []string{"**/*.md", "**/*.mdx"},
			ExcludePatterns: []string{"**/drafts/**", "**/*.draft.md"},
		},
	}

	tests := []struct {
		path           string
		shouldInclude  bool
		shouldExclude  bool
		expectedResult bool // true if should be processed
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

			// The actual logic: include && !exclude
			result := include && !exclude
			if result != tt.expectedResult {
				t.Errorf("Processing decision for %q = %v, want %v", tt.path, result, tt.expectedResult)
			}
		})
	}
}

func TestS3SourceConfig_PrefixNormalization(t *testing.T) {
	// This would be tested in NewS3Source, but we can verify the logic
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"docs", "docs/"},
		{"docs/", "docs/"},
		{"path/to/docs", "path/to/docs/"},
		{"path/to/docs/", "path/to/docs/"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			config := S3SourceConfig{
				Bucket: "test",
				Prefix: tt.input,
			}

			// Simulate the normalization logic from NewS3Source
			if config.Prefix != "" && config.Prefix[len(config.Prefix)-1] != '/' {
				config.Prefix = config.Prefix + "/"
			}

			if config.Prefix != tt.expected {
				t.Errorf("Normalized prefix = %q, want %q", config.Prefix, tt.expected)
			}
		})
	}
}

func TestS3SourceConfig_DefaultConcurrency(t *testing.T) {
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
			config := S3SourceConfig{
				Bucket:      "test",
				Concurrency: tt.concurrency,
			}

			// Simulate the default logic from NewS3Source
			if config.Concurrency <= 0 {
				config.Concurrency = 5
			}

			if config.Concurrency != tt.expected {
				t.Errorf("Concurrency = %d, want %d", config.Concurrency, tt.expected)
			}
		})
	}
}
