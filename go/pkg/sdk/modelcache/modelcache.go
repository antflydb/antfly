/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package modelcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	ModelFormatGGUF = "gguf"
)

// ModelSpec describes how a model should be pulled into the local inference
// cache. Higher-level packages own the supported-model catalog and pass the
// resolved defaults here.
type ModelSpec struct {
	Task           string
	DefaultFormat  string
	DefaultVariant string
}

// DefaultModelsDir returns the Antfly v0.2 local inference model cache:
// ~/.antfly/inference/models.
func DefaultModelsDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if home == "" {
		return "", fmt.Errorf("user home directory is empty")
	}
	return filepath.Join(home, ".antfly", "inference", "models"), nil
}

// ModelDir returns the flat owner/repo directory for model under modelsDir.
func ModelDir(modelsDir, model string) (string, error) {
	if modelsDir == "" {
		var err error
		modelsDir, err = DefaultModelsDir()
		if err != nil {
			return "", err
		}
	}
	ref, err := ParseModelRef(model)
	if err != nil {
		return "", err
	}
	return filepath.Join(modelsDir, ref.Owner, ref.Repo), nil
}

// ModelPullOptions configures PullHuggingFaceModel.
type ModelPullOptions struct {
	// ModelsDir is the root cache directory. When empty, DefaultModelsDir is used.
	ModelsDir string
	// Variant selects a precision/quantization variant. When empty, the model's
	// SDK default is used; clipclap defaults to Q4_K.
	Variant string
	// HuggingFaceToken is sent as a bearer token for gated/private repos.
	HuggingFaceToken string
	// HuggingFaceBaseURL overrides https://huggingface.co for tests or mirrors.
	HuggingFaceBaseURL string
	// HTTPClient overrides the client used for HuggingFace calls.
	HTTPClient *http.Client
	// DiskFreeBytes returns free bytes for the cache volume. When set, pulls
	// preflight the remaining download size with a 20% buffer.
	DiskFreeBytes func(dir string) (int64, error)
	// Progress receives cache and per-file byte progress.
	Progress func(ModelPullProgress)
}

// ModelPullProgress reports cache and per-file download progress.
type ModelPullProgress struct {
	Model          string
	File           string
	Downloaded     int64
	Total          int64
	FileDownloaded int64
	FileTotal      int64
	BlobsDone      int
	BlobsTotal     int
	ResumedBytes   int64
}

// ModelPullError is returned for machine-readable pull failures.
type ModelPullError struct {
	Code    string
	Message string
}

func (e *ModelPullError) Error() string { return e.Message }

func newModelPullError(code, message string) *ModelPullError {
	return &ModelPullError{Code: code, Message: message}
}

type hfTreeEntry struct {
	Path string `json:"path"`
	Type string `json:"type"`
	Size int64  `json:"size"`
}

type localManifest struct {
	SchemaVersion int                 `json:"schemaVersion"`
	Name          string              `json:"name"`
	Owner         string              `json:"owner"`
	Source        string              `json:"source"`
	Type          string              `json:"type"`
	Variant       string              `json:"variant,omitempty"`
	Files         []localManifestFile `json:"files"`
	Provenance    localProvenance     `json:"provenance"`
}

type localManifestFile struct {
	Name   string `json:"name"`
	Digest string `json:"digest,omitempty"`
	Size   int64  `json:"size"`
}

type localProvenance struct {
	DownloadedFrom string    `json:"downloadedFrom"`
	DownloadedAt   time.Time `json:"downloadedAt"`
}

// PullHuggingFaceModel pulls a model from HuggingFace into
// ~/.antfly/inference/models/<owner>/<repo> and returns that model directory.
func PullHuggingFaceModel(ctx context.Context, model string, spec ModelSpec, opts ModelPullOptions) (string, error) {
	ref, err := ParseModelRef(model)
	if err != nil {
		return "", err
	}
	owner, repo := ref.Owner, ref.Repo
	modelsDir := opts.ModelsDir
	if modelsDir == "" {
		modelsDir, err = DefaultModelsDir()
		if err != nil {
			return "", err
		}
	}
	modelDir := filepath.Join(modelsDir, owner, repo)
	if err := os.MkdirAll(modelDir, 0o755); err != nil {
		return "", fmt.Errorf("creating model directory: %w", err)
	}

	format := ref.Format
	if format == "" {
		format = spec.DefaultFormat
	}
	variant := strings.TrimSpace(opts.Variant)
	if variant == "" {
		variant = ref.Variant
	}
	if variant == "" {
		variant = spec.DefaultVariant
	}
	resolvedRef := ref
	resolvedRef.Format = format
	resolvedRef.Variant = variant
	baseURL := strings.TrimRight(opts.HuggingFaceBaseURL, "/")
	if baseURL == "" {
		baseURL = "https://huggingface.co"
	}
	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	repoID := ref.BaseName()
	entries, err := listHuggingFaceFiles(ctx, client, baseURL, repoID, opts.HuggingFaceToken)
	if err != nil {
		return "", err
	}
	selected := selectModelFiles(entries, format, variant)
	if len(selected) == 0 {
		return "", fmt.Errorf("no downloadable files found for %s", model)
	}

	var totalBytes int64
	for _, entry := range selected {
		totalBytes += entry.Size
	}
	var completedBytes, resumedBytes int64
	var blobsDone int
	for _, entry := range selected {
		destPath, err := localModelFilePath(modelDir, entry.Path)
		if err != nil {
			return "", err
		}
		if info, err := os.Stat(destPath); err == nil && entry.Size > 0 && info.Size() == entry.Size {
			resumedBytes += info.Size()
			blobsDone++
		}
	}
	completedBytes = resumedBytes
	neededBytes := totalBytes - resumedBytes
	if neededBytes < 0 {
		neededBytes = 0
	}
	if opts.DiskFreeBytes != nil && neededBytes > 0 {
		if free, ferr := opts.DiskFreeBytes(modelsDir); ferr == nil && free > 0 {
			required := neededBytes + neededBytes/5
			if free < required {
				return "", newModelPullError("disk_space", fmt.Sprintf("need %s free on %s, only %s available", formatBytes(required), modelsDir, formatBytes(free)))
			}
		}
	}
	reportProgress(opts.Progress, ModelPullProgress{
		Model:        repoID,
		Downloaded:   completedBytes,
		Total:        totalBytes,
		BlobsDone:    blobsDone,
		BlobsTotal:   len(selected),
		ResumedBytes: resumedBytes,
	})

	manifest := localManifest{
		SchemaVersion: 1,
		Name:          repo,
		Owner:         owner,
		Source:        resolvedRef.String(),
		Type:          spec.Task,
		Variant:       variant,
		Provenance: localProvenance{
			DownloadedFrom: "huggingface",
			DownloadedAt:   time.Now().UTC(),
		},
	}
	for _, entry := range selected {
		destName, destPath, err := localModelFile(modelDir, entry.Path)
		if err != nil {
			return "", err
		}
		if info, err := os.Stat(destPath); err == nil && entry.Size > 0 && info.Size() == entry.Size {
			manifest.Files = append(manifest.Files, localManifestFile{Name: destName, Size: info.Size()})
			reportProgress(opts.Progress, ModelPullProgress{
				Model:        repoID,
				File:         destName,
				Downloaded:   completedBytes,
				Total:        totalBytes,
				FileTotal:    entry.Size,
				BlobsDone:    blobsDone,
				BlobsTotal:   len(selected),
				ResumedBytes: resumedBytes,
			})
			continue
		}
		if err := downloadHuggingFaceFile(ctx, client, baseURL, repoID, entry, destPath, opts.HuggingFaceToken, completedBytes, totalBytes, blobsDone, len(selected), resumedBytes, opts.Progress); err != nil {
			return "", err
		}
		mf, err := manifestFile(destPath, destName)
		if err != nil {
			return "", err
		}
		manifest.Files = append(manifest.Files, mf)
		completedBytes += mf.Size
		blobsDone++
		reportProgress(opts.Progress, ModelPullProgress{
			Model:          repoID,
			File:           destName,
			Downloaded:     completedBytes,
			Total:          totalBytes,
			FileDownloaded: mf.Size,
			FileTotal:      entry.Size,
			BlobsDone:      blobsDone,
			BlobsTotal:     len(selected),
			ResumedBytes:   resumedBytes,
		})
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(modelDir, "model_manifest.json"), data, 0o644); err != nil {
		return "", fmt.Errorf("writing model manifest: %w", err)
	}
	return modelDir, nil
}

// ModelRef is an Antfly model reference in owner/repo[:format[:variant]] form.
type ModelRef struct {
	Owner   string
	Repo    string
	Format  string
	Variant string
}

// ParseModelRef parses owner/repo, owner/repo:variant, and
// owner/repo:format:variant refs. The cache directory always uses BaseName.
func ParseModelRef(model string) (ModelRef, error) {
	parts := strings.Split(model, ":")
	if len(parts) > 3 {
		return ModelRef{}, fmt.Errorf("model must be owner/repo[:format[:variant]], got %q", model)
	}
	nameParts := strings.Split(parts[0], "/")
	if len(nameParts) != 2 || !validPathComponent(nameParts[0]) || !validPathComponent(nameParts[1]) {
		return ModelRef{}, fmt.Errorf("model must be owner/repo[:format[:variant]], got %q", model)
	}
	ref := ModelRef{Owner: nameParts[0], Repo: nameParts[1]}
	if len(parts) == 2 {
		ref.Variant = parts[1]
	} else if len(parts) == 3 {
		ref.Format = strings.ToLower(parts[1])
		ref.Variant = parts[2]
	}
	return ref, nil
}

// BaseName returns owner/repo without format or variant tags.
func (r ModelRef) BaseName() string { return r.Owner + "/" + r.Repo }

func (r ModelRef) String() string {
	out := r.BaseName()
	if r.Format != "" && r.Variant != "" {
		return out + ":" + r.Format + ":" + r.Variant
	}
	if r.Variant != "" {
		return out + ":" + r.Variant
	}
	return out
}

func validPathComponent(part string) bool {
	return part != "" && part != "." && part != ".." && !strings.Contains(part, `/`) && !strings.Contains(part, `\`)
}

func localModelFile(modelDir, remotePath string) (string, string, error) {
	rel, err := safeLocalModelRelPath(remotePath)
	if err != nil {
		return "", "", err
	}
	return rel, filepath.Join(modelDir, filepath.FromSlash(rel)), nil
}

func localModelFilePath(modelDir, remotePath string) (string, error) {
	rel, err := safeLocalModelRelPath(remotePath)
	if err != nil {
		return "", err
	}
	return filepath.Join(modelDir, filepath.FromSlash(rel)), nil
}

func safeLocalModelRelPath(remotePath string) (string, error) {
	remotePath = strings.TrimSpace(strings.ReplaceAll(remotePath, `\`, `/`))
	if remotePath == "" || strings.HasPrefix(remotePath, "/") {
		return "", fmt.Errorf("unsafe HuggingFace file path %q", remotePath)
	}
	parts := strings.Split(remotePath, "/")
	for _, part := range parts {
		if !validPathComponent(part) {
			return "", fmt.Errorf("unsafe HuggingFace file path %q", remotePath)
		}
	}
	return strings.Join(parts, "/"), nil
}

// BaseModelName returns owner/repo for model refs with optional tags.
func BaseModelName(model string) string {
	ref, err := ParseModelRef(model)
	if err != nil {
		return model
	}
	return ref.BaseName()
}

func listHuggingFaceFiles(ctx context.Context, client *http.Client, baseURL, repoID, token string) ([]hfTreeEntry, error) {
	apiURL := fmt.Sprintf("%s/api/models/%s/tree/main?recursive=1", baseURL, escapeRepoID(repoID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listing HuggingFace files: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("listing HuggingFace files: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var entries []hfTreeEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("decoding HuggingFace file list: %w", err)
	}
	return entries, nil
}

func selectModelFiles(entries []hfTreeEntry, format, variant string) []hfTreeEntry {
	format = strings.ToLower(strings.TrimSpace(format))
	variant = normalizeVariant(variant)
	var support []hfTreeEntry
	var artifacts []hfTreeEntry
	var onnxData []hfTreeEntry
	for _, entry := range entries {
		if entry.Type != "" && entry.Type != "file" {
			continue
		}
		base := strings.ToLower(filepath.Base(entry.Path))
		ext := strings.ToLower(filepath.Ext(base))
		switch ext {
		case ".json", ".txt", ".model", ".spm", ".tiktoken":
			support = append(support, entry)
		case ".onnx":
			if format == ModelFormatGGUF {
				continue
			}
			if variant == "" || strings.Contains(normalizeVariant(base), variant) || !looksVariantFile(base) {
				artifacts = append(artifacts, entry)
			}
		case ".gguf":
			if format != "" && format != ModelFormatGGUF {
				continue
			}
			if variant == "" || strings.Contains(normalizeVariant(base), variant) {
				artifacts = append(artifacts, entry)
			}
		}
		if format != ModelFormatGGUF && (strings.HasSuffix(base, ".onnx_data") || strings.HasSuffix(base, ".onnx.data")) {
			onnxData = append(onnxData, entry)
		}
	}
	if len(artifacts) == 0 {
		return nil
	}
	out := make([]hfTreeEntry, 0, len(support)+len(artifacts)+len(onnxData))
	out = append(out, artifacts...)
	out = append(out, onnxData...)
	out = append(out, support...)
	return out
}

func looksVariantFile(name string) bool {
	name = normalizeVariant(name)
	for _, marker := range []string{"q4", "q5", "q6", "q8", "int8", "i8", "fp16", "f16", "bf16", "quantized"} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

func normalizeVariant(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, ".", "_")
	return s
}

func downloadHuggingFaceFile(ctx context.Context, client *http.Client, baseURL, repoID string, entry hfTreeEntry, destPath, token string, completedBeforeFile, totalBytes int64, blobsDone, blobsTotal int, resumedBytes int64, progress func(ModelPullProgress)) error {
	fileURL := fmt.Sprintf("%s/%s/resolve/main/%s", baseURL, escapeRepoID(repoID), escapeFilePath(entry.Path))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("downloading %s: %w", entry.Path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("downloading %s: status %d: %s", entry.Path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	tmpPath := destPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("creating %s: %w", tmpPath, err)
	}
	defer out.Close()
	var downloaded int64
	buf := make([]byte, 64*1024)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, err := out.Write(buf[:n]); err != nil {
				_ = os.Remove(tmpPath)
				return fmt.Errorf("writing %s: %w", destPath, err)
			}
			downloaded += int64(n)
			reportProgress(progress, ModelPullProgress{
				Model:          repoID,
				File:           filepath.Base(entry.Path),
				Downloaded:     completedBeforeFile + downloaded,
				Total:          totalBytes,
				FileDownloaded: downloaded,
				FileTotal:      entry.Size,
				BlobsDone:      blobsDone,
				BlobsTotal:     blobsTotal,
				ResumedBytes:   resumedBytes,
			})
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("reading %s: %w", entry.Path, readErr)
		}
	}
	if entry.Size > 0 && downloaded != entry.Size {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("downloaded %s size = %d, want %d", entry.Path, downloaded, entry.Size)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		return fmt.Errorf("renaming %s: %w", destPath, err)
	}
	return nil
}

func reportProgress(progress func(ModelPullProgress), p ModelPullProgress) {
	if progress != nil {
		progress(p)
	}
}

func formatBytes(n int64) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
	)
	switch {
	case n >= gb:
		return fmt.Sprintf("%.1f GB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.0f MB", float64(n)/float64(mb))
	default:
		return fmt.Sprintf("%d KB", n/kb)
	}
}

func manifestFile(path, name string) (localManifestFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return localManifestFile{}, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return localManifestFile{}, err
	}
	return localManifestFile{Name: name, Digest: "sha256:" + hex.EncodeToString(h.Sum(nil)), Size: n}, nil
}

func escapeRepoID(repoID string) string {
	parts := strings.Split(repoID, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func escapeFilePath(path string) string {
	parts := strings.Split(path, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}
