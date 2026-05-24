// Package paths provides cross-platform path utilities for Termite.
package paths

import (
	"os"
	"path/filepath"
	"runtime"
)

// DefaultModelsDir returns the platform-specific default models directory for Termite.
// Returns ~/.termite/models on Unix-like systems and %USERPROFILE%\.termite\models on Windows.
// Falls back to "./models" if home directory cannot be determined.
func DefaultModelsDir() string {
	home := userHomeDir()
	if home == "" {
		return filepath.FromSlash("./models") // fallback to legacy behavior
	}
	return filepath.Join(home, ".termite", "models")
}

// userHomeDir returns the user's home directory in a cross-platform manner.
// On Unix: $HOME
// On Windows: %USERPROFILE% (preferred) or %HOMEDRIVE%%HOMEPATH%
// Note: On Windows, we check USERPROFILE first because $HOME from Git Bash/MSYS2
// may contain Unix-style paths (e.g., /c/Users/...) that don't work with Windows APIs.
func userHomeDir() string {
	// Windows-specific: check USERPROFILE first to avoid Unix-style $HOME from Git Bash
	if runtime.GOOS == "windows" {
		// USERPROFILE is the most reliable on Windows
		if home := os.Getenv("USERPROFILE"); home != "" {
			return home
		}
		// Fallback to HOMEDRIVE+HOMEPATH
		if drive, path := os.Getenv("HOMEDRIVE"), os.Getenv("HOMEPATH"); drive != "" && path != "" {
			return filepath.Join(drive, path)
		}
	}

	// Unix: use $HOME
	if home := os.Getenv("HOME"); home != "" {
		return home
	}

	// Use Go's built-in (Go 1.12+) as last resort
	home, _ := os.UserHomeDir()
	return home
}
