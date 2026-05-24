package docsaf

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// GitSourceConfig holds configuration for a GitSource.
type GitSourceConfig struct {
	// URL is the Git repository URL (required).
	// Supports:
	//   - Full URLs: https://github.com/owner/repo.git
	//   - GitHub shorthand: owner/repo (automatically expanded to https://github.com/owner/repo.git)
	//   - SSH URLs: git@github.com:owner/repo.git
	URL string

	// Ref is the branch, tag, or commit to checkout (default: default branch).
	Ref string

	// BaseURL is the base URL for generating document links.
	// If empty, it will be derived from the repository URL.
	BaseURL string

	// SubPath is an optional subdirectory within the repo to traverse.
	// Useful for monorepos or repos where docs are in a specific folder.
	SubPath string

	// IncludePatterns is a list of glob patterns for files to include.
	// If empty, all files are included (subject to exclude patterns).
	IncludePatterns []string

	// ExcludePatterns is a list of glob patterns for files to exclude.
	// Default excludes common non-content paths (.git, node_modules, etc.).
	ExcludePatterns []string

	// ShallowClone enables shallow cloning with depth 1 (default: true).
	// Set to false for full history (needed for some operations).
	ShallowClone bool

	// CloneDir is an optional directory to clone into.
	// If empty, a temporary directory is created and cleaned up after traversal.
	CloneDir string

	// KeepClone prevents cleanup of the cloned directory after traversal.
	// Only applies when CloneDir is empty (temp directories).
	KeepClone bool

	// Auth holds optional authentication credentials.
	Auth *GitAuth
}

// GitAuth holds authentication credentials for private repositories.
type GitAuth struct {
	// Username for HTTPS authentication.
	Username string

	// Password or personal access token for HTTPS authentication.
	Password string

	// SSHKeyPath is the path to an SSH private key file.
	SSHKeyPath string
}

// GitSource clones a Git repository and traverses its contents.
type GitSource struct {
	config    GitSourceConfig
	cloneDir  string
	tempDir   bool
	fsSource  *FilesystemSource
	cleanedUp bool
}

// NewGitSource creates a new Git content source.
func NewGitSource(config GitSourceConfig) (*GitSource, error) {
	if config.URL == "" {
		return nil, fmt.Errorf("URL is required")
	}

	// Expand GitHub shorthand (owner/repo -> https://github.com/owner/repo.git)
	config.URL = expandGitURL(config.URL)

	// Default to shallow clone
	if !config.ShallowClone {
		// Check if explicitly set to false by looking at zero value behavior
		// Since Go can't distinguish, we default to true
		config.ShallowClone = true
	}

	// Set default exclude patterns
	if len(config.ExcludePatterns) == 0 {
		config.ExcludePatterns = []string{
			".git/**",
			"node_modules/**",
			"vendor/**",
			".venv/**",
			"__pycache__/**",
			"*.pyc",
			".DS_Store",
			"Thumbs.db",
		}
	}

	// Derive BaseURL from repository URL if not provided
	if config.BaseURL == "" {
		config.BaseURL = deriveBaseURL(config.URL, config.Ref)
	}

	return &GitSource{
		config: config,
	}, nil
}

// expandGitURL expands shorthand Git URLs to full URLs.
func expandGitURL(url string) string {
	// Already a full URL
	if strings.HasPrefix(url, "http://") ||
		strings.HasPrefix(url, "https://") ||
		strings.HasPrefix(url, "git://") ||
		strings.HasPrefix(url, "git@") ||
		strings.HasPrefix(url, "ssh://") {
		return url
	}

	// GitHub shorthand: owner/repo
	if strings.Count(url, "/") == 1 && !strings.Contains(url, ":") {
		return "https://github.com/" + url + ".git"
	}

	return url
}

// deriveBaseURL generates a base URL for document links from the repo URL.
func deriveBaseURL(repoURL, ref string) string {
	// Handle GitHub URLs
	if strings.Contains(repoURL, "github.com") {
		// Extract owner/repo from various URL formats
		var ownerRepo string

		if after, ok := strings.CutPrefix(repoURL, "git@github.com:"); ok {
			ownerRepo = after
		} else if strings.Contains(repoURL, "github.com/") {
			parts := strings.SplitN(repoURL, "github.com/", 2)
			if len(parts) == 2 {
				ownerRepo = parts[1]
			}
		}

		// Clean up owner/repo
		ownerRepo = strings.TrimSuffix(ownerRepo, ".git")

		if ownerRepo != "" {
			branch := ref
			if branch == "" {
				branch = "main"
			}
			return fmt.Sprintf("https://github.com/%s/blob/%s", ownerRepo, branch)
		}
	}

	// Handle GitLab URLs
	if strings.Contains(repoURL, "gitlab.com") {
		var ownerRepo string

		if after, ok := strings.CutPrefix(repoURL, "git@gitlab.com:"); ok {
			ownerRepo = after
		} else if strings.Contains(repoURL, "gitlab.com/") {
			parts := strings.SplitN(repoURL, "gitlab.com/", 2)
			if len(parts) == 2 {
				ownerRepo = parts[1]
			}
		}

		ownerRepo = strings.TrimSuffix(ownerRepo, ".git")

		if ownerRepo != "" {
			branch := ref
			if branch == "" {
				branch = "main"
			}
			return fmt.Sprintf("https://gitlab.com/%s/-/blob/%s", ownerRepo, branch)
		}
	}

	// Fallback: return empty, will use file paths only
	return ""
}

// Type returns "git" as the source type.
func (gs *GitSource) Type() string {
	return "git"
}

// BaseURL returns the base URL for this source.
func (gs *GitSource) BaseURL() string {
	return gs.config.BaseURL
}

// Clone clones the repository. Called automatically by Traverse if not already cloned.
func (gs *GitSource) Clone(ctx context.Context) error {
	if gs.cloneDir != "" {
		return nil // Already cloned
	}

	var err error

	// Determine clone directory
	if gs.config.CloneDir != "" {
		gs.cloneDir = gs.config.CloneDir
		gs.tempDir = false
	} else {
		gs.cloneDir, err = os.MkdirTemp("", "docsaf-git-*")
		if err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}
		gs.tempDir = true
	}

	// Build git clone command
	args := []string{"clone"}

	if gs.config.ShallowClone {
		args = append(args, "--depth", "1")
	}

	if gs.config.Ref != "" {
		args = append(args, "--branch", gs.config.Ref)
	}

	// Handle authentication
	cloneURL := gs.config.URL
	if gs.config.Auth != nil && gs.config.Auth.Username != "" && gs.config.Auth.Password != "" {
		// Inject credentials into HTTPS URL
		if strings.HasPrefix(cloneURL, "https://") {
			cloneURL = strings.Replace(
				cloneURL,
				"https://",
				fmt.Sprintf("https://%s:%s@", gs.config.Auth.Username, gs.config.Auth.Password),
				1,
			)
		}
	}

	args = append(args, cloneURL, gs.cloneDir)

	// Execute git clone
	cmd := exec.CommandContext(ctx, "git", args...)

	// Set up SSH key if provided
	if gs.config.Auth != nil && gs.config.Auth.SSHKeyPath != "" {
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("GIT_SSH_COMMAND=ssh -i %s -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new",
				gs.config.Auth.SSHKeyPath))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		// Clean up temp directory on failure
		if gs.tempDir {
			_ = os.RemoveAll(gs.cloneDir)
			gs.cloneDir = ""
		}
		return fmt.Errorf("git clone failed: %w\nOutput: %s", err, string(output))
	}

	// Create filesystem source for the cloned directory
	baseDir := gs.cloneDir
	if gs.config.SubPath != "" {
		baseDir = filepath.Join(gs.cloneDir, gs.config.SubPath)
	}

	gs.fsSource = NewFilesystemSource(FilesystemSourceConfig{
		BaseDir:         baseDir,
		BaseURL:         gs.config.BaseURL,
		IncludePatterns: gs.config.IncludePatterns,
		ExcludePatterns: gs.config.ExcludePatterns,
	})

	return nil
}

// Traverse clones the repository (if not already) and yields content items.
func (gs *GitSource) Traverse(ctx context.Context) (<-chan ContentItem, <-chan error) {
	items := make(chan ContentItem)
	errs := make(chan error, 1)

	go func() {
		defer close(items)
		defer close(errs)
		defer gs.Cleanup()

		// Clone if not already done
		if err := gs.Clone(ctx); err != nil {
			errs <- err
			return
		}

		// Delegate to filesystem source
		fsItems, fsErrs := gs.fsSource.Traverse(ctx)

		// Forward items with git-specific metadata
		for item := range fsItems {
			if item.Metadata == nil {
				item.Metadata = make(map[string]any)
			}
			item.Metadata["source_type"] = "git"
			item.Metadata["repository"] = gs.config.URL
			if gs.config.Ref != "" {
				item.Metadata["ref"] = gs.config.Ref
			}

			select {
			case items <- item:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
		}

		// Forward any errors
		for err := range fsErrs {
			errs <- err
		}
	}()

	return items, errs
}

// Cleanup removes the cloned directory if it was a temporary directory.
func (gs *GitSource) Cleanup() {
	if gs.cleanedUp {
		return
	}
	gs.cleanedUp = true

	if gs.tempDir && !gs.config.KeepClone && gs.cloneDir != "" {
		_ = os.RemoveAll(gs.cloneDir)
		gs.cloneDir = ""
	}
}

// CloneDir returns the path to the cloned repository.
// Returns empty string if not yet cloned.
func (gs *GitSource) CloneDir() string {
	return gs.cloneDir
}
