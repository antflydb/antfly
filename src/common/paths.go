// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package common

import (
	"os"
	"path/filepath"
	"runtime"
)

// DefaultDataDir returns the platform-specific default data directory for Antfly.
// Returns ~/.antfly on Unix-like systems and %USERPROFILE%\.antfly on Windows.
// Falls back to "antflydb" if home directory cannot be determined.
func DefaultDataDir() string {
	home := userHomeDir()
	if home == "" {
		return "antflydb" // fallback to legacy behavior
	}
	return filepath.Join(home, ".antfly")
}

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
