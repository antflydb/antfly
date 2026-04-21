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

// Package cli provides shared CLI functions for termite model management.
// These functions are used by both the standalone termite binary and the antfly termite subcommand.
package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
)

// PullOptions contains options for pulling models from the registry
type PullOptions struct {
	RegistryURL string
	ModelsDir   string
	Variants    []string // Variant IDs to download (e.g., ["f16", "i8"])
}

// HuggingFaceOptions contains options for pulling from HuggingFace
type HuggingFaceOptions struct {
	ModelsDir string
	ModelType string
	HFToken   string
	Variant   string
}

// ListOptions contains options for listing models
type ListOptions struct {
	RegistryURL string
	ModelsDir   string
	TypeFilter  string
	BinaryName  string // Used for help messages (e.g., "termite" or "antfly termite")
}

// knownVariants are the recognized model variant suffixes
var knownVariants = []string{"f32", "f16", "bf16", "i8", "i8-st", "i4"}

// parseModelRef parses a model reference like "bge-small-en-v1.5-i8" into
// name ("bge-small-en-v1.5") and variant ("i8"). If no known variant suffix
// is found, returns the original ref with empty variant.
// For owner-qualified refs like "owner/model-i8", the variant is stripped
// only from the model name portion after the "/".
func parseModelRef(ref string) (name, variant string) {
	// For owner-qualified refs, only strip variant from the name portion
	prefix := ""
	target := ref
	if idx := strings.Index(ref, "/"); idx != -1 {
		prefix = ref[:idx+1]
		target = ref[idx+1:]
	}

	for _, v := range knownVariants {
		suffix := "-" + v
		if before, ok := strings.CutSuffix(target, suffix); ok {
			return prefix + before, v
		}
	}
	return ref, ""
}

// resolveModelName resolves a bare model name (without owner prefix) to its
// fully qualified owner/name form by looking it up in the registry index.
// If the name already contains "/", it is returned unchanged.
// For legacy models with no owner, the bare name is returned as-is.
func resolveModelName(ctx context.Context, client *modelregistry.Client, name string) (string, error) {
	if strings.Contains(name, "/") {
		return name, nil
	}

	index, err := client.FetchIndex(ctx)
	if err != nil {
		return "", fmt.Errorf("fetching registry index: %w", err)
	}

	var matches []string
	var fullPathMatch bool
	for _, m := range index.Models {
		if m.Name == name {
			if m.Owner != "" {
				matches = append(matches, m.Owner+"/"+m.Name)
			} else {
				fullPathMatch = true
			}
		}
	}

	switch {
	case len(matches) == 1 && !fullPathMatch:
		return matches[0], nil
	case len(matches) > 1, len(matches) == 1 && fullPathMatch:
		if fullPathMatch {
			matches = append(matches, name)
		}
		return "", fmt.Errorf("ambiguous model name %q, specify owner explicitly: %s", name, strings.Join(matches, ", "))
	case fullPathMatch:
		// Legacy model with no owner — pass bare name through
		return name, nil
	default:
		return "", fmt.Errorf("model %q not found in registry", name)
	}
}

// PullFromRegistry pulls a model from the Antfly model registry.
// modelRef can be "name" or "name-variant" (e.g., "bge-small-en-v1.5-i8").
func PullFromRegistry(modelRef string, opts PullOptions) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Parse model reference for inline variant suffix
	modelName, inlineVariant := parseModelRef(modelRef)
	variants := opts.Variants
	if inlineVariant != "" {
		// Add inline variant if not already in list
		found := slices.Contains(variants, inlineVariant)
		if !found {
			variants = append(variants, inlineVariant)
		}
	}

	client := modelregistry.NewClient(
		modelregistry.WithBaseURL(opts.RegistryURL),
		modelregistry.WithProgressHandler(PrintProgress),
	)

	// Resolve bare model names to owner/name using the registry index
	resolved, err := resolveModelName(ctx, client, modelName)
	if err != nil {
		return err
	}
	if resolved != modelName {
		fmt.Printf("Resolved %s -> %s\n", modelName, resolved)
	}
	modelName = resolved

	fmt.Printf("Fetching manifest for %s...\n", modelName)
	manifest, err := client.FetchManifest(ctx, modelName)
	if err != nil {
		return fmt.Errorf("failed to fetch manifest: %w", err)
	}

	fmt.Printf("Model: %s\n", manifest.Name)
	fmt.Printf("Type:  %s\n", manifest.Type)
	if manifest.Description != "" {
		fmt.Printf("Description: %s\n", manifest.Description)
	}

	// Default to f32 if no variants specified (matches PullModel behavior)
	effectiveVariants := variants
	if len(effectiveVariants) == 0 {
		effectiveVariants = []string{modelregistry.VariantF32}
	}

	totalSize := manifest.DownloadSize(effectiveVariants)

	fmt.Printf("Variants: %v\n", effectiveVariants)
	fmt.Printf("Total size: %s\n", FormatBytes(totalSize))
	fmt.Println()

	fmt.Println("Downloading files...")
	if err := client.PullModel(ctx, manifest, opts.ModelsDir, effectiveVariants); err != nil {
		return fmt.Errorf("failed to pull model: %w", err)
	}

	// Build destination path with owner if present
	// Use DirPath() for cross-platform path separators
	destDir := filepath.Join(opts.ModelsDir, manifest.Type.DirName(), manifest.DirPath())
	fmt.Printf("\n✓ Model pulled successfully to %s\n", destDir)
	return nil
}

// PullFromHuggingFace pulls a model from HuggingFace
func PullFromHuggingFace(repoID string, opts HuggingFaceOptions) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Parse repoID to get owner/model
	ref, err := modelregistry.ParseModelRef(repoID)
	if err != nil {
		return fmt.Errorf("invalid model reference: %w", err)
	}

	// Model type can be auto-detected for generators, but required for others
	var modelType modelregistry.ModelType
	if opts.ModelType != "" {
		modelType, err = modelregistry.ParseModelType(opts.ModelType)
		if err != nil {
			return err
		}
	}

	if opts.Variant != "" && !modelregistry.IsValidVariant(opts.Variant) {
		return fmt.Errorf("invalid variant %q, valid options: fp16, q4, q4f16, quantized", opts.Variant)
	}

	hfToken := opts.HFToken
	if hfToken == "" {
		hfToken = os.Getenv("HF_TOKEN")
	}

	client := modelregistry.NewHuggingFaceClient(
		modelregistry.WithHFToken(hfToken),
		modelregistry.WithHFProgressHandler(PrintProgress),
	)

	fmt.Printf("Pulling from HuggingFace: %s\n", repoID)

	// Auto-detect model type if not specified
	if modelType == "" {
		fmt.Println("Detecting model type...")
		detected, err := client.DetectModelType(ctx, repoID)
		if err != nil {
			return fmt.Errorf("failed to detect model type: %w\nUse --type flag to specify manually (embedder, chunker, reranker, generator, recognizer, rewriter)", err)
		}
		modelType = detected
		fmt.Printf("Detected type: %s\n", modelType)
	} else {
		fmt.Printf("Type: %s\n", modelType)
	}

	if opts.Variant != "" {
		fmt.Printf("Variant: %s (%s)\n", opts.Variant, modelregistry.VariantDescription(opts.Variant))
	} else {
		fmt.Printf("Variant: %s\n", modelregistry.VariantDescription(""))
	}
	fmt.Println()
	fmt.Println("Downloading files...")

	if err := client.PullFromHuggingFace(ctx, repoID, modelType, opts.ModelsDir, opts.Variant); err != nil {
		return fmt.Errorf("failed to pull model: %w", err)
	}

	// Destination uses owner/model structure
	destDir := filepath.Join(opts.ModelsDir, modelType.DirName(), ref.DirPath())
	fmt.Printf("\n✓ Model pulled successfully to %s\n", destDir)
	return nil
}

// ListRemoteModels lists models available in the remote registry
func ListRemoteModels(opts ListOptions) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := modelregistry.NewClient(
		modelregistry.WithBaseURL(opts.RegistryURL),
	)

	fmt.Printf("Fetching model list from %s...\n\n", opts.RegistryURL)

	index, err := client.FetchIndex(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch registry index: %w", err)
	}

	if len(index.Models) == 0 {
		fmt.Println("No models available in registry")
		return nil
	}

	var filteredType modelregistry.ModelType
	if opts.TypeFilter != "" {
		filteredType, err = modelregistry.ParseModelType(opts.TypeFilter)
		if err != nil {
			return err
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tTYPE\tSIZE\tVARIANTS\tDESCRIPTION")

	for _, model := range index.Models {
		if filteredType != "" && model.Type != filteredType {
			continue
		}

		variantsStr := ""
		if len(model.Variants) > 0 {
			variantsStr = strings.Join(model.Variants, ",")
		}

		desc := model.Description
		if len(desc) > 50 {
			desc = desc[:47] + "..."
		}

		displayName := model.Name
		if model.Owner != "" {
			displayName = model.Owner + "/" + model.Name
		}

		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			displayName,
			model.Type,
			FormatBytes(model.Size),
			variantsStr,
			desc,
		)
	}
	return w.Flush()
}

// ListLocalModels lists locally installed models
func ListLocalModels(opts ListOptions) error {
	fmt.Printf("Local models in %s:\n\n", opts.ModelsDir)

	var filteredType modelregistry.ModelType
	if opts.TypeFilter != "" {
		var err error
		filteredType, err = modelregistry.ParseModelType(opts.TypeFilter)
		if err != nil {
			return err
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tTYPE\tSIZE\tVARIANTS\tSOURCE")

	totalModels := 0

	for _, modelType := range modelregistry.AllModelTypes {
		if filteredType != "" && modelType != filteredType {
			continue
		}

		typeDir := filepath.Join(opts.ModelsDir, modelType.DirName())
		discovered, err := modelregistry.DiscoverModelsInDir(typeDir, modelType, nil)
		if err != nil {
			continue
		}

		for _, dm := range discovered {
			displayName := dm.FullName()

			// Compute total size of all files in the model directory
			var totalSize int64
			files, _ := os.ReadDir(dm.Path)
			for _, f := range files {
				if f.IsDir() {
					continue
				}
				if info, err := f.Info(); err == nil {
					totalSize += info.Size()
				}
			}

			// Collect variant IDs (skip the default empty-string key)
			var variantIDs []string
			for v := range dm.Variants {
				if v != "" {
					variantIDs = append(variantIDs, v)
				}
			}

			// Detect multimodal capabilities
			mc := modelregistry.DetectMultimodalCapabilities(dm.Path)
			var capabilities []string
			if mc.HasImage || mc.HasImageQuantized {
				capabilities = append(capabilities, "image")
			}
			if mc.HasAudio || mc.HasAudioQuantized {
				capabilities = append(capabilities, "audio")
			}
			if _, err := os.Stat(filepath.Join(dm.Path, "genai_config.json")); err == nil {
				capabilities = append(capabilities, "genai")
			}

			displayType := string(modelType)
			if len(capabilities) > 0 {
				displayType = displayType + " [" + strings.Join(capabilities, ",") + "]"
			}

			source := ""
			if dm.Manifest != nil {
				source = dm.Manifest.Source
			}

			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				displayName,
				displayType,
				FormatBytes(totalSize),
				strings.Join(variantIDs, ","),
				source,
			)
			totalModels++
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}

	if totalModels == 0 {
		binaryName := opts.BinaryName
		if binaryName == "" {
			binaryName = "termite"
		}
		fmt.Println("No models found locally.")
		fmt.Printf("\nUse '%s pull <model-name>' to download models.\n", binaryName)
		fmt.Printf("Use '%s list --remote' to see available models.\n", binaryName)
	}

	return nil
}

// FormatBytes formats bytes as human-readable string
func FormatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// PrintProgress prints download progress to stdout
func PrintProgress(downloaded, total int64, filename string) {
	if total <= 0 {
		fmt.Printf("\r  %s: %s", filename, FormatBytes(downloaded))
		return
	}

	percent := float64(downloaded) / float64(total) * 100
	barWidth := 30
	filled := int(float64(barWidth) * float64(downloaded) / float64(total))

	bar := strings.Repeat("=", filled) + strings.Repeat("-", barWidth-filled)
	fmt.Printf("\r  %s: [%s] %.1f%% (%s/%s)",
		filename, bar, percent, FormatBytes(downloaded), FormatBytes(total))

	if downloaded >= total {
		fmt.Println()
	}
}
