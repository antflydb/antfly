package docsaf

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestExpandGitURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "GitHub shorthand",
			input:    "owner/repo",
			expected: "https://github.com/owner/repo.git",
		},
		{
			name:     "HTTPS URL unchanged",
			input:    "https://github.com/owner/repo.git",
			expected: "https://github.com/owner/repo.git",
		},
		{
			name:     "HTTP URL unchanged",
			input:    "http://example.com/repo.git",
			expected: "http://example.com/repo.git",
		},
		{
			name:     "SSH URL unchanged",
			input:    "git@github.com:owner/repo.git",
			expected: "git@github.com:owner/repo.git",
		},
		{
			name:     "Git protocol unchanged",
			input:    "git://github.com/owner/repo.git",
			expected: "git://github.com/owner/repo.git",
		},
		{
			name:     "SSH protocol unchanged",
			input:    "ssh://git@github.com/owner/repo.git",
			expected: "ssh://git@github.com/owner/repo.git",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := expandGitURL(tt.input)
			if got != tt.expected {
				t.Errorf("expandGitURL(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestDeriveBaseURL(t *testing.T) {
	tests := []struct {
		name     string
		repoURL  string
		ref      string
		expected string
	}{
		{
			name:     "GitHub HTTPS URL with branch",
			repoURL:  "https://github.com/owner/repo.git",
			ref:      "develop",
			expected: "https://github.com/owner/repo/blob/develop",
		},
		{
			name:     "GitHub HTTPS URL default branch",
			repoURL:  "https://github.com/owner/repo.git",
			ref:      "",
			expected: "https://github.com/owner/repo/blob/main",
		},
		{
			name:     "GitHub SSH URL",
			repoURL:  "git@github.com:owner/repo.git",
			ref:      "v1.0.0",
			expected: "https://github.com/owner/repo/blob/v1.0.0",
		},
		{
			name:     "GitLab HTTPS URL",
			repoURL:  "https://gitlab.com/owner/repo.git",
			ref:      "main",
			expected: "https://gitlab.com/owner/repo/-/blob/main",
		},
		{
			name:     "GitLab SSH URL",
			repoURL:  "git@gitlab.com:owner/repo.git",
			ref:      "",
			expected: "https://gitlab.com/owner/repo/-/blob/main",
		},
		{
			name:     "Unknown host returns empty",
			repoURL:  "https://bitbucket.org/owner/repo.git",
			ref:      "main",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveBaseURL(tt.repoURL, tt.ref)
			if got != tt.expected {
				t.Errorf("deriveBaseURL(%q, %q) = %q, want %q", tt.repoURL, tt.ref, got, tt.expected)
			}
		})
	}
}

func TestNewGitSource(t *testing.T) {
	// Test basic creation
	gs, err := NewGitSource(GitSourceConfig{
		URL: "owner/repo",
	})
	if err != nil {
		t.Fatalf("NewGitSource failed: %v", err)
	}

	if gs.Type() != "git" {
		t.Errorf("Type() = %q, want %q", gs.Type(), "git")
	}

	expectedBaseURL := "https://github.com/owner/repo/blob/main"
	if gs.BaseURL() != expectedBaseURL {
		t.Errorf("BaseURL() = %q, want %q", gs.BaseURL(), expectedBaseURL)
	}

	// Test with explicit config
	gs2, err := NewGitSource(GitSourceConfig{
		URL:     "https://github.com/owner/repo.git",
		Ref:     "v2.0.0",
		BaseURL: "https://custom.url",
	})
	if err != nil {
		t.Fatalf("NewGitSource with config failed: %v", err)
	}

	if gs2.BaseURL() != "https://custom.url" {
		t.Errorf("BaseURL() = %q, want %q", gs2.BaseURL(), "https://custom.url")
	}

	// Test missing URL
	_, err = NewGitSource(GitSourceConfig{})
	if err == nil {
		t.Error("Expected error for missing URL")
	}
}

func TestGitSource_DefaultExcludePatterns(t *testing.T) {
	gs, err := NewGitSource(GitSourceConfig{
		URL: "owner/repo",
	})
	if err != nil {
		t.Fatalf("NewGitSource failed: %v", err)
	}

	// Check that default exclude patterns are set
	if len(gs.config.ExcludePatterns) == 0 {
		t.Error("Expected default exclude patterns to be set")
	}

	// Check for common patterns
	patterns := gs.config.ExcludePatterns
	hasGit := false
	hasNodeModules := false
	for _, p := range patterns {
		if p == ".git/**" {
			hasGit = true
		}
		if p == "node_modules/**" {
			hasNodeModules = true
		}
	}

	if !hasGit {
		t.Error("Expected .git/** in default exclude patterns")
	}
	if !hasNodeModules {
		t.Error("Expected node_modules/** in default exclude patterns")
	}
}

func TestGitSource_CloneDir(t *testing.T) {
	gs, err := NewGitSource(GitSourceConfig{
		URL: "owner/repo",
	})
	if err != nil {
		t.Fatalf("NewGitSource failed: %v", err)
	}

	// Before clone, CloneDir should be empty
	if gs.CloneDir() != "" {
		t.Errorf("CloneDir() before clone = %q, want empty", gs.CloneDir())
	}
}

func TestGitSource_WithCustomCloneDir(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "git-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cloneDir := filepath.Join(tempDir, "repo")

	gs, err := NewGitSource(GitSourceConfig{
		URL:      "owner/repo",
		CloneDir: cloneDir,
	})
	if err != nil {
		t.Fatalf("NewGitSource failed: %v", err)
	}

	if gs.config.CloneDir != cloneDir {
		t.Errorf("config.CloneDir = %q, want %q", gs.config.CloneDir, cloneDir)
	}
}

func TestGitSource_WithAuth(t *testing.T) {
	gs, err := NewGitSource(GitSourceConfig{
		URL: "owner/repo",
		Auth: &GitAuth{
			Username: "user",
			Password: "token",
		},
	})
	if err != nil {
		t.Fatalf("NewGitSource failed: %v", err)
	}

	if gs.config.Auth == nil {
		t.Error("Expected Auth to be set")
	}
	if gs.config.Auth.Username != "user" {
		t.Errorf("Auth.Username = %q, want %q", gs.config.Auth.Username, "user")
	}
}

func TestGitSource_WithSubPath(t *testing.T) {
	gs, err := NewGitSource(GitSourceConfig{
		URL:     "owner/repo",
		SubPath: "docs",
	})
	if err != nil {
		t.Fatalf("NewGitSource failed: %v", err)
	}

	if gs.config.SubPath != "docs" {
		t.Errorf("config.SubPath = %q, want %q", gs.config.SubPath, "docs")
	}
}

// Integration test - only runs if git is available and network is accessible
func TestGitSource_Clone_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Check if git is available
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available, skipping integration test")
	}

	// Use a small, public repository for testing
	gs, err := NewGitSource(GitSourceConfig{
		URL:          "https://github.com/octocat/Hello-World.git",
		ShallowClone: true,
	})
	if err != nil {
		t.Fatalf("NewGitSource failed: %v", err)
	}
	defer gs.Cleanup()

	ctx := context.Background()
	err = gs.Clone(ctx)
	if err != nil {
		t.Fatalf("Clone failed: %v", err)
	}

	// Verify clone directory exists
	if gs.CloneDir() == "" {
		t.Error("CloneDir() should not be empty after clone")
	}

	// Verify it's a valid directory
	info, err := os.Stat(gs.CloneDir())
	if err != nil {
		t.Errorf("Failed to stat clone directory: %v", err)
	}
	if !info.IsDir() {
		t.Error("Clone directory should be a directory")
	}

	// Verify .git exists (it's a git repo)
	gitDir := filepath.Join(gs.CloneDir(), ".git")
	if _, err := os.Stat(gitDir); os.IsNotExist(err) {
		t.Error(".git directory should exist in cloned repo")
	}
}
