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

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/termite/pkg/termite/lib/cli"
	"github.com/antflydb/termite/pkg/termite/lib/modelregistry"
)

// testModelsDir is the shared models directory for all e2e tests
var testModelsDir string

// modelDownloadMu ensures only one model downloads at a time
var modelDownloadMu sync.Mutex

// TestMain sets up the e2e test environment.
func TestMain(m *testing.M) {
	// Use TERMITE_MODELS_DIR if set, otherwise use repo's models directory
	testModelsDir = os.Getenv("TERMITE_MODELS_DIR")
	if testModelsDir == "" {
		// Find repository root and use its models directory
		repoRoot, err := findRepoRootFromDir(".")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to find repo root: %v\n", err)
			os.Exit(1)
		}
		testModelsDir = filepath.Join(repoRoot, "models")
	}

	fmt.Printf("E2E Test Setup: Using models directory: %s\n", testModelsDir)

	// Ensure models directory exists
	if err := os.MkdirAll(testModelsDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create models directory: %v\n", err)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()
	os.Exit(code)
}

// findRepoRootFromDir walks up from a directory to find the repository root.
// Uses .git instead of go.mod because sub-modules (e.g., e2e/) have their own go.mod.
func findRepoRootFromDir(startDir string) (string, error) {
	absDir, err := filepath.Abs(startDir)
	if err != nil {
		return "", err
	}

	dir := absDir
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf(".git not found in any parent directory of %s", startDir)
		}
		dir = parent
	}
}

// ensureRegistryModel downloads a model from the Antfly model registry if it
// doesn't already exist locally. Safe to call from multiple tests concurrently.
func ensureRegistryModel(t *testing.T, name string, modelType modelregistry.ModelType, variants []string) {
	t.Helper()

	modelDownloadMu.Lock()
	defer modelDownloadMu.Unlock()

	ref, err := modelregistry.ParseModelRef(name)
	if err != nil {
		t.Fatalf("parse model ref %s: %v", name, err)
	}

	modelPath := filepath.Join(testModelsDir, modelType.DirName(), ref.DirPath())
	if modelExists(modelPath) {
		t.Logf("Model %s already exists at %s", name, modelPath)
		return
	}

	t.Logf("Downloading model from registry: %s (variants: %v)", name, variants)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	lastMilestone := make(map[string]int)
	regClient := modelregistry.NewClient(
		modelregistry.WithProgressHandler(func(downloaded, total int64, filename string) {
			if total > 0 {
				percent := float64(downloaded) / float64(total) * 100
				milestone := int(percent / 25)
				if milestone > lastMilestone[filename] || (downloaded == total && lastMilestone[filename] < 4) {
					lastMilestone[filename] = milestone
					t.Logf("  %s: %.0f%%", filename, percent)
				}
			}
		}),
	)

	manifest, err := regClient.FetchManifest(ctx, name)
	if err != nil {
		t.Fatalf("fetch manifest for %s: %v", name, err)
	}

	if len(variants) == 0 {
		variants = []string{modelregistry.VariantF32}
	}

	if err := regClient.PullModel(ctx, manifest, testModelsDir, variants); err != nil {
		t.Fatalf("pull model %s: %v", name, err)
	}

	t.Logf("Downloaded model: %s", name)
}

// ensureHuggingFaceModel downloads a model from HuggingFace if it doesn't
// already exist locally. Safe to call from multiple tests concurrently.
func ensureHuggingFaceModel(t *testing.T, repo string, modelType modelregistry.ModelType) {
	t.Helper()

	modelDownloadMu.Lock()
	defer modelDownloadMu.Unlock()

	modelPath := filepath.Join(testModelsDir, modelType.DirName(), repo)

	// Check for genai_config.json (required for generators)
	genaiConfigPath := filepath.Join(modelPath, "genai_config.json")
	if _, err := os.Stat(genaiConfigPath); err == nil {
		t.Logf("Model %s already exists at %s", repo, modelPath)
		return
	}

	t.Logf("Downloading HuggingFace model: %s", repo)

	err := cli.PullFromHuggingFace(repo, cli.HuggingFaceOptions{
		ModelsDir: testModelsDir,
		ModelType: "", // Auto-detect
	})
	if err != nil {
		t.Fatalf("pull model %s from HuggingFace: %v", repo, err)
	}

	t.Logf("Downloaded HuggingFace model: %s", repo)
}

// modelExists checks if a model directory contains ONNX model files
func modelExists(modelPath string) bool {
	onnxFiles := []string{"model.onnx", "model_i8.onnx", "model_f16.onnx"}
	for _, filename := range onnxFiles {
		if _, err := os.Stat(filepath.Join(modelPath, filename)); err == nil {
			return true
		}
	}
	return false
}
