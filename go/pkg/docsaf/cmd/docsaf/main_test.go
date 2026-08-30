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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/docsaf"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

func parseSourceFlagsForTest(t *testing.T, args ...string) sourceFlags {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	flags := registerSourceFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse source flags: %v", err)
	}
	return flags
}

func TestDocsafEmbedderConfigDefaultsToAntflyClipclap(t *testing.T) {
	cfg, err := docsafEmbedderConfig("", defaultDocsafEmbeddingModel, "")
	if err != nil {
		t.Fatalf("docsafEmbedderConfig: %v", err)
	}
	antflyCfg, err := cfg.AsAntflyEmbedderConfig()
	if err != nil {
		t.Fatalf("AsAntflyEmbedderConfig: %v", err)
	}
	if antflyCfg.Model != defaultDocsafEmbeddingModel {
		t.Fatalf("model = %q, want %q", antflyCfg.Model, defaultDocsafEmbeddingModel)
	}
}

func TestDocsafEmbedderConfigSupportsOllama(t *testing.T) {
	cfg, err := docsafEmbedderConfig("ollama", "embeddinggemma", "")
	if err != nil {
		t.Fatalf("docsafEmbedderConfig: %v", err)
	}
	ollamaCfg, err := cfg.AsOllamaEmbedderConfig()
	if err != nil {
		t.Fatalf("AsOllamaEmbedderConfig: %v", err)
	}
	if ollamaCfg.Model != "embeddinggemma" {
		t.Fatalf("model = %q, want embeddinggemma", ollamaCfg.Model)
	}
}

func TestCreateHierarchyIndexesIncludesSelectivePDFOCRInSingleConfig(t *testing.T) {
	indexes, err := createHierarchyIndexes(512, 50, defaultDocsafEmbeddingProvider, defaultDocsafEmbeddingModel, "", 0, `{"provider":"antfly","model":"reader"}`, 200, 75)
	if err != nil {
		t.Fatalf("createHierarchyIndexes: %v", err)
	}
	documentUnits := indexes["document_units"].(map[string]any)
	if documentUnits["type"] != "graph" {
		t.Fatalf("document_units type = %#v, want graph", documentUnits["type"])
	}
	source := documentUnits["source"].(map[string]any)
	if _, exists := source["kind"]; exists {
		t.Fatalf("graph source must not contain a kind discriminator: %#v", source)
	}
	if source["artifact"] != docsaf.DefaultDocumentUnitsArtifact || source["path"] != "$.edges[*]" {
		t.Fatalf("unexpected graph source: %#v", source)
	}
	artifact := documentUnits["artifact"].(map[string]any)
	if _, exists := artifact["field"]; exists {
		t.Fatalf("graph artifact producer must use typed source: %#v", artifact)
	}
	artifactSource := artifact["source"].(map[string]any)
	if artifactSource["type"] != "field" || artifactSource["value"] != "url" {
		t.Fatalf("unexpected graph artifact producer source: %#v", artifactSource)
	}
	producer := artifact["producer_json"].(map[string]any)
	config := producer["config"].(map[string]any)
	ocr := config["ocr"].(map[string]any)
	if ocr["render_dpi"] != float64(200) {
		t.Fatalf("render_dpi = %#v, want 200", ocr["render_dpi"])
	}
	quality := ocr["quality"].(map[string]any)
	if quality["min_content_chars"] != float64(75) {
		t.Fatalf("min_content_chars = %#v, want 75", quality["min_content_chars"])
	}
	reader := ocr["config"].(map[string]any)
	if reader["model"] != "reader" {
		t.Fatalf("reader model = %#v, want reader", reader["model"])
	}
	documentText := indexes["document_text"].(map[string]any)
	enrichments := documentText["enrichments"].([]map[string]any)
	chunk := enrichments[1]
	if chunk["name"] != docsaf.DefaultDocumentChunksArtifact {
		t.Fatalf("chunk name = %#v, want %q", chunk["name"], docsaf.DefaultDocumentChunksArtifact)
	}
	if chunk["full_text_index"] != true {
		t.Fatalf("full_text_index = %#v, want true", chunk["full_text_index"])
	}
}

func TestDocsafEmbedderConfigSupportsOpenRouter(t *testing.T) {
	cfg, err := docsafEmbedderConfig("openrouter", "openai/text-embedding-3-small", "")
	if err != nil {
		t.Fatalf("docsafEmbedderConfig: %v", err)
	}
	openRouterCfg, err := cfg.AsOpenRouterEmbedderConfig()
	if err != nil {
		t.Fatalf("AsOpenRouterEmbedderConfig: %v", err)
	}
	if openRouterCfg.Model != "openai/text-embedding-3-small" {
		t.Fatalf("model = %q, want openai/text-embedding-3-small", openRouterCfg.Model)
	}
}

func TestDocsafEmbedderConfigPassesThroughFutureProvider(t *testing.T) {
	cfg, err := docsafEmbedderConfig("future-provider", "future-model", "")
	if err != nil {
		t.Fatalf("docsafEmbedderConfig: %v", err)
	}
	if string(cfg.Provider) != "future-provider" {
		t.Fatalf("provider = %q, want future-provider", cfg.Provider)
	}
}

func TestDocsafEmbedderConfigAcceptsFullSDKJSON(t *testing.T) {
	cfg, err := docsafEmbedderConfig("antfly", "ignored", `{"provider":"openai","model":"text-embedding-3-large","dimensions":1024}`)
	if err != nil {
		t.Fatalf("docsafEmbedderConfig: %v", err)
	}
	openAICfg, err := cfg.AsOpenAIEmbedderConfig()
	if err != nil {
		t.Fatalf("AsOpenAIEmbedderConfig: %v", err)
	}
	if openAICfg.Model != "text-embedding-3-large" {
		t.Fatalf("model = %q, want text-embedding-3-large", openAICfg.Model)
	}
	if openAICfg.Dimensions != 1024 {
		t.Fatalf("dimensions = %v, want 1024", openAICfg.Dimensions)
	}
}

func TestDocsafEmbedderConfigPassesThroughFutureProviderInJSON(t *testing.T) {
	cfg, err := docsafEmbedderConfig("", "", `{"provider":"future-provider","model":"future-model","option":true}`)
	if err != nil {
		t.Fatalf("docsafEmbedderConfig: %v", err)
	}
	if string(cfg.Provider) != "future-provider" {
		t.Fatalf("provider = %q, want future-provider", cfg.Provider)
	}
}

func TestDocsafEmbedderConfigRejectsMissingProviderInJSON(t *testing.T) {
	if _, err := docsafEmbedderConfig("", "", `{"model":"model"}`); err == nil {
		t.Fatalf("docsafEmbedderConfig error = nil, want missing provider")
	}
}

func TestSourceFlagsFilesystemDefaultRequiresDir(t *testing.T) {
	flags := parseSourceFlagsForTest(t, "--base-url", "s3://docs")
	err := flags.validate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--dir flag is required") {
		t.Fatalf("validate error = %v, want missing dir", err)
	}
}

func TestSourceFlagsGoogleDriveRequiresFolder(t *testing.T) {
	flags := parseSourceFlagsForTest(t, "--source", "google-drive", "--drive-access-token", "token")
	err := flags.validate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--drive-folder is required") {
		t.Fatalf("validate error = %v, want missing drive folder", err)
	}
}

func TestSourceFlagsGoogleDriveRequiresAuth(t *testing.T) {
	t.Setenv("GOOGLE_DRIVE_ACCESS_TOKEN", "")
	originalADC := googleDriveADCTokenSource
	googleDriveADCTokenSource = func(context.Context) (oauth2.TokenSource, error) {
		return nil, fmt.Errorf("adc unavailable")
	}
	t.Cleanup(func() { googleDriveADCTokenSource = originalADC })

	flags := parseSourceFlagsForTest(t,
		"--source", "google-drive",
		"--drive-folder", "folder-id",
		"--drive-token-file", filepath.Join(t.TempDir(), "missing-token.json"),
	)
	err := flags.validate(context.Background())
	if err == nil {
		t.Fatalf("validate error = nil, want missing auth")
	}
	for _, want := range []string{"no Google Drive auth found", "docsaf auth google-drive", "gcloud auth application-default login"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("validate error = %v, want %q", err, want)
		}
	}
}

func TestSourceFlagsGoogleDriveAccessTokenBuildsSource(t *testing.T) {
	flags := parseSourceFlagsForTest(t,
		"--source", "google-drive",
		"--drive-folder", "folder-id",
		"--drive-access-token", "token",
		"--include", "**/*.md",
		"--exclude", "**/drafts/**",
	)
	if err := flags.validate(context.Background()); err != nil {
		t.Fatalf("validate: %v", err)
	}
	source, err := flags.source(context.Background())
	if err != nil {
		t.Fatalf("source: %v", err)
	}
	if source.Type() != "google_drive" {
		t.Fatalf("source.Type() = %q, want google_drive", source.Type())
	}
	if _, ok := source.(*docsaf.GoogleDriveSource); !ok {
		t.Fatalf("source type = %T, want *docsaf.GoogleDriveSource", source)
	}
}

func TestSourceFlagsGoogleDriveInlineDefaultIsLargeEnoughForDocuments(t *testing.T) {
	flags := parseSourceFlagsForTest(t,
		"--source", "google-drive",
		"--drive-folder", "folder-id",
		"--drive-access-token", "token",
		"--inline-content",
	)
	flags.normalizeForSource()
	if got := *flags.maxInlineBytes; got != defaultDriveMaxInlineContentBytes {
		t.Fatalf("maxInlineBytes = %d, want %d", got, defaultDriveMaxInlineContentBytes)
	}
}

func TestSourceFlagsGoogleDriveInlineMaxInlineBytesCanBeExplicitlySmall(t *testing.T) {
	flags := parseSourceFlagsForTest(t,
		"--source", "google-drive",
		"--drive-folder", "folder-id",
		"--drive-access-token", "token",
		"--inline-content",
		"--max-inline-bytes", fmt.Sprint(defaultDocsafMaxInlineContentBytes),
	)
	flags.normalizeForSource()
	if got := *flags.maxInlineBytes; got != defaultDocsafMaxInlineContentBytes {
		t.Fatalf("maxInlineBytes = %d, want explicit %d", got, defaultDocsafMaxInlineContentBytes)
	}
}

func TestSourceFlagsInlineMaxInlineBytesCanDisableGuard(t *testing.T) {
	flags := parseSourceFlagsForTest(t,
		"--source", "google-drive",
		"--drive-folder", "folder-id",
		"--drive-access-token", "token",
		"--inline-content",
		"--max-inline-bytes", "0",
	)
	flags.normalizeForSource()
	if err := flags.validate(context.Background()); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got := *flags.maxInlineBytes; got != 0 {
		t.Fatalf("maxInlineBytes = %d, want disabled guard", got)
	}
	if got := flags.options().MaxInlineBytes; got != 0 {
		t.Fatalf("options().MaxInlineBytes = %d, want disabled guard", got)
	}
}

func TestGoogleDriveTokenCacheLoadTokenSource(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "google-drive-token.json")
	cache := googleDriveTokenCache{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Endpoint:     google.Endpoint,
		Scopes:       []string{drive.DriveReadonlyScope},
		Token: &oauth2.Token{
			AccessToken:  "access-token",
			RefreshToken: "refresh-token",
			TokenType:    "Bearer",
			Expiry:       time.Now().Add(time.Hour),
		},
	}
	if err := writeGoogleDriveTokenCache(tokenFile, cache); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	info, err := os.Stat(tokenFile)
	if err != nil {
		t.Fatalf("stat token cache: %v", err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("token cache mode = %v, want 0600", got)
	}

	source, err := loadGoogleDriveTokenSource(context.Background(), tokenFile)
	if err != nil {
		t.Fatalf("load token source: %v", err)
	}
	token, err := source.Token()
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	if token.AccessToken != "access-token" {
		t.Fatalf("AccessToken = %q, want access-token", token.AccessToken)
	}
}

func TestResolveGoogleDriveTokenSourceFallsBackToADCWhenCacheInvalid(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "google-drive-token.json")
	if err := os.WriteFile(tokenFile, []byte("{"), 0600); err != nil {
		t.Fatalf("write invalid token cache: %v", err)
	}

	originalADC := googleDriveADCTokenSource
	googleDriveADCTokenSource = func(context.Context) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "adc-token"}), nil
	}
	t.Cleanup(func() { googleDriveADCTokenSource = originalADC })

	source, err := resolveGoogleDriveTokenSource(context.Background(), tokenFile)
	if err != nil {
		t.Fatalf("resolve token source: %v", err)
	}
	token, err := source.Token()
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	if token.AccessToken != "adc-token" {
		t.Fatalf("AccessToken = %q, want adc-token", token.AccessToken)
	}
}

func TestCreateTableWithIndexesReturnsNonConflictError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid create table request", http.StatusBadRequest)
	}))
	defer server.Close()

	err := createTableWithIndexes(context.Background(), server.URL, "", nil, "docs", 1, map[string]any{})
	if err == nil {
		t.Fatal("createTableWithIndexes error = nil, want create-table error")
	}
	if !strings.Contains(err.Error(), "HTTP 400") || !strings.Contains(err.Error(), "invalid create table request") {
		t.Fatalf("createTableWithIndexes error = %q, want status and response body", err)
	}
}
