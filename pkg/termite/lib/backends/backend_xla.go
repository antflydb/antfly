// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build xla && XLA

package backends

import (
	"os"
	"path/filepath"
	"runtime"

	// Import XLA backend - requires PJRT runtime
	_ "github.com/gomlx/gomlx/backends/xla"
)

func init() {
	// Register XLA backend (hardware accelerated via PJRT)
	RegisterBackend(newGomlxBackend(BackendXLA, "xla"))

	// Discover bundled PJRT plugin so getEngine() can pass the absolute path
	// to GoMLX, bypassing auto-download. Setting PJRT_PLUGIN_LIBRARY_PATH in
	// init() is too late — pjrt.init() (from the imported gomlx/backends/xla
	// package) caches search paths before this init() runs. Instead, we store
	// the absolute plugin file path and pass it via "xla:/abs/path" config.
	if pjrtPluginPath == "" {
		if p := findPjrtPluginFile(); p != "" {
			pjrtPluginPath = p
		}
	}
}

// findPjrtPluginFile returns the absolute path to a bundled PJRT CPU plugin file.
// Discovery order mirrors getOnnxLibraryPath():
//  1. PJRT_PLUGIN_LIBRARY_PATH environment variable (explicit user override)
//  2. PJRT_ROOT environment variable (set by Makefile during build)
//  3. lib/ directory next to the running binary (omni install layout)
//  4. LD_LIBRARY_PATH or DYLD_LIBRARY_PATH
func findPjrtPluginFile() string {
	// Check explicit PJRT_PLUGIN_LIBRARY_PATH first
	if libPath := os.Getenv("PJRT_PLUGIN_LIBRARY_PATH"); libPath != "" {
		for _, dir := range filepath.SplitList(libPath) {
			if p := findPjrtInDir(dir); p != "" {
				return p
			}
		}
	}

	platform := runtime.GOOS + "-" + runtime.GOARCH

	// Check PJRT_ROOT (set by Makefile)
	if root := os.Getenv("PJRT_ROOT"); root != "" {
		for _, dir := range []string{
			filepath.Join(root, platform, "lib"),
			filepath.Join(root, "lib"),
		} {
			if p := findPjrtInDir(dir); p != "" {
				return p
			}
		}
	}

	// Check lib/ relative to the binary (omni install layout)
	if exe, err := os.Executable(); err == nil {
		if exe, err = filepath.EvalSymlinks(exe); err == nil {
			binDir := filepath.Dir(exe)
			for _, dir := range []string{
				filepath.Join(binDir, "lib"),
				filepath.Join(binDir, "..", "lib", "antfly"),
				filepath.Join(binDir, "..", "lib", "termite"),
			} {
				if p := findPjrtInDir(dir); p != "" {
					return p
				}
			}
		}
	}

	// Check library path environment variable (platform-specific)
	ldPath := os.Getenv("LD_LIBRARY_PATH")
	if runtime.GOOS == "darwin" {
		if dyldPath := os.Getenv("DYLD_LIBRARY_PATH"); dyldPath != "" {
			ldPath = dyldPath
		}
	}
	if ldPath != "" {
		for _, dir := range filepath.SplitList(ldPath) {
			if p := findPjrtInDir(dir); p != "" {
				return p
			}
		}
	}

	return ""
}

// findPjrtInDir returns the absolute path to the first PJRT CPU plugin file
// found in dir, or "" if none found.
// PJRT plugins are named pjrt_c_api_cpu*plugin*.so (Linux) or .dylib (macOS).
func findPjrtInDir(dir string) string {
	ext := ".so"
	if runtime.GOOS == "darwin" {
		ext = ".dylib"
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "pjrt_c_api_cpu*plugin*"+ext))
	if len(matches) > 0 {
		if abs, err := filepath.Abs(matches[0]); err == nil {
			return abs
		}
		return matches[0]
	}
	return ""
}
