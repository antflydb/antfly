// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ajroetker/pdf"
	antfly "github.com/antflydb/antfly/go/pkg/sdk"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCorpusFixture(t *testing.T, name string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func testCorpusConfig(t *testing.T, state string) corpusConfig {
	t.Helper()
	var cfg corpusConfig
	if err := readCorpusJSON(filepath.Join(state, "sources.json"), &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}
func testCorpusRecords(t *testing.T, state string) []corpusRecord {
	t.Helper()
	f, err := os.Open(filepath.Join(state, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var records []corpusRecord
	for {
		var v corpusRecord
		_, err := readCorpusLine(r, &v)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, v)
	}
	return records
}
func TestCorpusPilotStopsBeforeOpeningLaterPDF(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "sources")
	state := filepath.Join(dir, "state")
	writeCorpusFixture(t, filepath.Join(source, "EFTA00000001.pdf"), createMultiPagePDF(4))
	writeCorpusFixture(t, filepath.Join(source, "EFTA00000002.pdf"), []byte("broken PDF that must not be opened"))
	writeCorpusFixture(t, filepath.Join(source, "movie.mp4"), []byte("video excluded"))
	writeCorpusFixture(t, filepath.Join(source, "pages", "copy.pdf"), createMinimalPDF())
	if err := corpusPrepareCmd([]string{"--state", state, "--source", "ds9=" + source, "--limit-pages", "2"}); err != nil {
		t.Fatal(err)
	}
	records := testCorpusRecords(t, state)
	if len(records) != 1 {
		t.Fatalf("records=%d", len(records))
	}
	loc := records[0].Document["metadata"].(map[string]any)["source_locator"].(map[string]any)
	if loc["first"] != float64(1) || loc["last"] != float64(2) {
		t.Fatalf("range=%v", loc)
	}
	cfg := testCorpusConfig(t, state)
	store := newCorpusStore(cfg)
	defer store.Close()
	recorder := httptest.NewRecorder()
	store.ServeHTTP(recorder, httptest.NewRequest("GET", records[0].Document["url"].(string), nil))
	if recorder.Code != 200 {
		t.Fatalf("source: %d %s", recorder.Code, recorder.Body.String())
	}
	f := filepath.Join(dir, "selected.pdf")
	writeCorpusFixture(t, f, recorder.Body.Bytes())
	num, err := store.pages(sourceLocator{Source: 0, Name: "EFTA00000001.pdf", Size: int64(len(createMultiPagePDF(4))), Modified: mustStat(t, filepath.Join(source, "EFTA00000001.pdf")).ModTime().UnixNano()})
	if err != nil || num != 4 {
		t.Fatalf("original pages=%d %v", num, err)
	}
	// Read the served pilot PDF independently: the original remains four pages.
	reader, err := os.Open(f)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	parsed, err := pdfPageCount(reader, int64(recorder.Body.Len()))
	if err != nil || parsed != 2 {
		t.Fatalf("pilot pages=%d %v", parsed, err)
	}
}
func mustStat(t *testing.T, name string) os.FileInfo {
	t.Helper()
	v, e := os.Stat(name)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func TestCorpusResumeTruncatesUncommittedTail(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "sources")
	state := filepath.Join(dir, "state")
	for i := 1; i <= 2; i++ {
		writeCorpusFixture(t, filepath.Join(source, fmt.Sprintf("EFTA%08d.pdf", i)), createMultiPagePDF(3))
	}
	if err := corpusPrepareCmd([]string{"--state", state, "--source", "ds10=" + source, "--pages-per-record", "2"}); err != nil {
		t.Fatal(err)
	}
	expected, err := os.ReadFile(filepath.Join(state, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	inventory, _ := os.ReadFile(filepath.Join(state, "inventory.ndjson"))
	lines := bytes.SplitAfter(expected, []byte("\n"))
	firstOffset := len(lines[0]) + len(lines[1])
	progress := corpusProgress{InputOffset: int64(bytes.IndexByte(inventory, '\n') + 1), OutputOffset: int64(firstOffset), Files: 1, Pages: 3, Records: 2}
	if err = atomicCorpusJSON(filepath.Join(state, "prepare.json"), progress); err != nil {
		t.Fatal(err)
	}
	writeCorpusFixture(t, filepath.Join(state, "records.ndjson"), append(expected[:firstOffset:firstOffset], []byte("uncommitted partial JSON")...))
	if err = corpusPrepareCmd([]string{"--state", state, "--resume"}); err != nil {
		t.Fatal(err)
	}
	actual, _ := os.ReadFile(filepath.Join(state, "records.ndjson"))
	if !bytes.Equal(actual, expected) {
		t.Fatal("resume changed the stable manifest")
	}
	if err = corpusPrepareCmd([]string{"--state", state, "--resume", "--limit-pages", "1"}); err == nil {
		t.Fatal("changed pilot settings accepted")
	}
}
func TestCorpusAudioZIPRangeAndSourceSecurity(t *testing.T) {
	dir := t.TempDir()
	archive := createTestZip(t, map[string][]byte{"nested/EFTA00000001.wav": []byte("RIFFaudio"), "EFTA00000002.mp4": []byte("video"), "__MACOSX/._noise.pdf": []byte("noise")})
	cfg := corpusConfig{Key: "test-key", Sources: []corpusSource{{Dataset: "ds11", Path: archive, Zip: true}}}
	inventory := filepath.Join(dir, "inventory")
	if err := discoverCorpus(cfg, inventory); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(inventory)
	defer f.Close()
	var entry corpusEntry
	if _, err := readCorpusLine(bufio.NewReader(f), &entry); err != nil {
		t.Fatal(err)
	}
	if entry.MIME != "audio/wav" || entry.ID != "ds11:EFTA00000001.wav" {
		t.Fatalf("entry=%+v", entry)
	}
	store := newCorpusStore(cfg)
	defer store.Close()
	request := httptest.NewRequest("GET", "/sources/"+sourceToken(cfg, entry.Locator), nil)
	request.Header.Set("Range", "bytes=0-3")
	response := httptest.NewRecorder()
	store.ServeHTTP(response, request)
	if response.Code != 206 || response.Body.String() != "RIFF" {
		t.Fatalf("audio range=%d %q", response.Code, response.Body.String())
	}
	for _, loc := range []sourceLocator{{Source: 0, Name: "../secret.pdf"}, {Source: 0, Name: "EFTA00000002.mp4"}} {
		response = httptest.NewRecorder()
		store.ServeHTTP(response, httptest.NewRequest("GET", "/sources/"+sourceToken(cfg, loc), nil))
		if response.Code == 200 {
			t.Fatal("unsafe locator served")
		}
	}
	response = httptest.NewRecorder()
	store.ServeHTTP(response, httptest.NewRequest("GET", "/sources/invalid.token", nil))
	if response.Code != 403 {
		t.Fatal("unsigned request accepted")
	}
}
func TestCorpusDedupAndConflicts(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a")
	b := filepath.Join(dir, "b")
	writeCorpusFixture(t, filepath.Join(a, "EFTA00000001.pdf"), createMinimalPDF())
	writeCorpusFixture(t, filepath.Join(b, "nested", "EFTA00000001.pdf"), createMinimalPDF())
	cfg := corpusConfig{Sources: []corpusSource{{Dataset: "ds9", Path: a}, {Dataset: "ds9", Path: b}}}
	output := filepath.Join(dir, "out")
	if err := discoverCorpus(cfg, output); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(output)
	if bytes.Count(data, []byte("\n")) != 1 {
		t.Fatal("duplicate indexed twice")
	}
	writeCorpusFixture(t, filepath.Join(b, "nested", "EFTA00000001.pdf"), createMultiPagePDF(2))
	if err := discoverCorpus(cfg, output); err == nil || !strings.Contains(err.Error(), "conflicting copies") {
		t.Fatalf("conflicting duplicate: %v", err)
	}
}
func TestCorpusExternalSort(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "sources")
	for i := 0; i < 4100; i++ {
		writeCorpusFixture(t, filepath.Join(source, fmt.Sprintf("EFTA%08d.wav", i)), []byte("audio"))
	}
	output := filepath.Join(dir, "out")
	if err := discoverCorpus(corpusConfig{Sources: []corpusSource{{Dataset: "ds9", Path: source}}}, output); err != nil {
		t.Fatal(err)
	}
	file, _ := os.Open(output)
	defer file.Close()
	reader := bufio.NewReader(file)
	previous := ""
	count := 0
	for {
		var entry corpusEntry
		_, err := readCorpusLine(reader, &entry)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if entry.ID <= previous {
			t.Fatal("inventory is not strictly sorted")
		}
		previous = entry.ID
		count++
	}
	if count != 4100 {
		t.Fatalf("count=%d", count)
	}
}
func TestCorpusLoadResumeAfterFailedBatch(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	source := filepath.Join(dir, "sources")
	for i := 1; i <= 2; i++ {
		writeCorpusFixture(t, filepath.Join(source, fmt.Sprintf("EFTA%08d.wav", i)), []byte("audio"))
	}
	if err := corpusPrepareCmd([]string{"--state", state, "--source", "ds9=" + source}); err != nil {
		t.Fatal(err)
	}
	indexConfig, err := corpusIndexes(DefaultEmbeddingModel, DefaultInferenceURL, "en-US", false, false)
	if err != nil {
		t.Fatal(err)
	}
	description := corpusDescription(indexConfig, 25)
	failed := true
	var accepted []string
	identity := "table-1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprintf(w, `{"table_id":%q,"description":%q}`, identity, description)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/batch") {
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		var body struct {
			Inserts map[string]any `json:"inserts"`
			Sync    string         `json:"sync_level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.Sync != "write" {
			t.Error("non-durable batch")
		}
		for id := range body.Inserts {
			if strings.Contains(id, "00000002") && failed {
				http.Error(w, "temporary failure", 503)
				return
			}
			accepted = append(accepted, id)
		}
		io.WriteString(w, `{}`)
	}))
	defer server.Close()
	args := []string{"--state", state, "--url", server.URL, "--batch-size", "1", "--semantic=false"}
	if err := corpusLoadCmd(args); err == nil {
		t.Fatal("failed batch reported success")
	}
	var progress corpusProgress
	if err := readCorpusJSON(filepath.Join(state, "load.json"), &progress); err != nil {
		t.Fatal(err)
	}
	if progress.Records != 1 || progress.Complete {
		t.Fatalf("checkpoint=%+v", progress)
	}
	failed = false
	if err := corpusLoadCmd(args); err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 2 {
		t.Fatalf("accepted=%v", accepted)
	}
	identity = "replacement-table"
	if err := corpusLoadCmd(args); err == nil {
		t.Fatal("resumed into recreated table")
	}
}
func TestCorpusTruncatedManifestIsNotEOF(t *testing.T) {
	var record corpusRecord
	if _, err := readCorpusLine(bufio.NewReader(strings.NewReader(`{"id":"partial"}`)), &record); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error=%v", err)
	}
}
func TestCorpusAppleIndexConfiguration(t *testing.T) {
	indexes, err := corpusIndexes(DefaultEmbeddingModel, DefaultInferenceURL, "en-US", true, true)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(indexes)
	var decoded map[string]any
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	text := decoded[corpusTextIndex].(map[string]any)
	units := text["enrichments"].([]any)[0].(map[string]any)
	var producer map[string]any
	if err = json.Unmarshal([]byte(units["producer_json"].(string)), &producer); err != nil {
		t.Fatal(err)
	}
	config := producer["config"].(map[string]any)
	for _, kind := range []string{"ocr", "transcription"} {
		provider := config[kind].(map[string]any)["config"].(map[string]any)
		if provider["provider"] != "apple" {
			t.Fatal(provider)
		}
		if _, ok := provider["model"]; ok {
			t.Fatal("Apple provider takes no model")
		}
		if _, ok := provider["max_tokens"]; ok {
			t.Fatal("Apple provider takes no token limit")
		}
	}
	graph := decoded[DefaultAutographIndex].(map[string]any)
	artifact := graph["artifact"].(map[string]any)
	if artifact["source"].(map[string]any)["value"] != "content" {
		t.Fatal("graph must consume materialized unit text")
	}

}

func pdfPageCount(reader io.ReaderAt, size int64) (int, error) {
	p, e := pdf.NewReader(reader, size)
	if e != nil {
		return 0, e
	}
	return p.NumPage(), nil
}

func TestCorpusGraphTraversesUnitsAndCheckpoints(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	source := filepath.Join(dir, "sources")
	writeCorpusFixture(t, filepath.Join(source, "EFTA00000001.pdf"), createMultiPagePDF(2))
	if err := corpusPrepareCmd([]string{"--state", state, "--source", "ds9=" + source}); err != nil {
		t.Fatal(err)
	}
	queryCount := 0
	insertCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if strings.Contains(r.URL.Path, "/artifacts/") {
				io.WriteString(w, `{"unit_count":2}`)
			} else {
				io.WriteString(w, `{"table_id":"table-graph","indexes":{"autograph_relations":{}}}`)
			}
			return
		}
		if strings.HasSuffix(r.URL.Path, "/query") {
			queryCount++
			var request map[string]any
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request["search_after"] != nil {
				io.WriteString(w, `{"responses":[{"hits":{"hits":[]}}]}`)
				return
			}
			io.WriteString(w, `{"responses":[{"hits":{"hits":[{"_id":"unit1","_source":{"unit_id":"page:1","text":"First page","provenance":{"page_number":1}},"_sort":["position-1","unit1"]},{"_id":"unit2","_source":{"unit_id":"page:2","text":"Second page","provenance":{"page_number":2}},"_sort":["position-2","unit2"]}]}}]}`)
			return
		}
		var request struct {
			Inserts map[string]map[string]any `json:"inserts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		for key, doc := range request.Inserts {
			if !strings.Contains(key, ":graph:") {
				t.Error("unstable graph ID")
			}
			if doc["content"] == nil || doc["url"] != nil {
				t.Error("graph must consume text and avoid recursive media extraction")
			}
			insertCount++
		}
		io.WriteString(w, `{}`)
	}))
	defer server.Close()
	args := []string{"--state", state, "--url", server.URL}
	if err := corpusGraphCmd(args); err != nil {
		t.Fatal(err)
	}
	if queryCount != 2 || insertCount != 2 {
		t.Fatalf("queries=%d inserts=%d", queryCount, insertCount)
	}
	if err := corpusGraphCmd(args); err != nil {
		t.Fatal(err)
	}
	if insertCount != 2 {
		t.Fatal("completed graph work replayed")
	}
}
func TestCorpusSearchProvenance(t *testing.T) {
	server := &SearchServer{corpus: true, indexes: []string{corpusTextIndex}, tableName: "corpus"}
	request := server.searchRequest("Monday")
	if request.Table != "corpus" || request.Limit != 20 || request.SemanticSearch != "" || len(request.Indexes) != 0 || request.FullTextSearch == nil || len(request.Fields) == 0 || request.Hierarchy.GroupBy == nil {
		t.Fatalf("query=%+v", request)
	}
	hit := antfly.QueryHit{Source: map[string]any{"original_url": "http://source/file#page=26", "metadata": map[string]any{"page_start": float64(26)}}, Hierarchy: antfly.QueryHitHierarchy{Matches: []antfly.HierarchyMatchHit{{Source: map[string]any{"text": "match"}, Hierarchy: antfly.HierarchyMatchContext{Ancestors: antfly.QueryHitHierarchyAncestors{Unit: antfly.HierarchyAncestor{Document: map[string]any{"provenance": map[string]any{"page_number": float64(3)}}}}}}}}}
	var result SearchResult
	applyCorpusHit(&result, hit)
	if result.PageNum != 28 || result.URL != "http://source/file#page=28" || result.Content != "match" {
		t.Fatalf("result=%+v", result)
	}
	hit.Source["mime_type"] = "audio/wav"
	hit.Hierarchy.Matches[0].Hierarchy.Ancestors.Unit.Document = map[string]any{"provenance": map[string]any{"transcript_spans": []any{map[string]any{"start_ms": float64(1250)}}}}
	applyCorpusHit(&result, hit)
	if !result.Audio || result.URL != "http://source/file#t=1.250" {
		t.Fatalf("audio=%+v", result)
	}
}
func TestCorpusSourceRejectsSymlinkEscapeAndChangedFile(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "sources")
	outside := filepath.Join(dir, "secret.pdf")
	writeCorpusFixture(t, outside, createMinimalPDF())
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(source, "escape.pdf")); err != nil {
		t.Fatal(err)
	}
	cfg := corpusConfig{Key: "key", Sources: []corpusSource{{Dataset: "ds", Path: source}}}
	store := newCorpusStore(cfg)
	defer store.Close()
	info := mustStat(t, outside)
	loc := sourceLocator{Source: 0, Name: "escape.pdf", Size: info.Size(), Modified: info.ModTime().UnixNano()}
	response := httptest.NewRecorder()
	store.ServeHTTP(response, httptest.NewRequest("GET", "/sources/"+sourceToken(cfg, loc), nil))
	if response.Code == 200 {
		t.Fatal("symlink escaped source root")
	}
	file := filepath.Join(source, "file.pdf")
	writeCorpusFixture(t, file, createMinimalPDF())
	info = mustStat(t, file)
	loc = sourceLocator{Source: 0, Name: "file.pdf", Size: info.Size(), Modified: info.ModTime().UnixNano()}
	writeCorpusFixture(t, file, []byte("modified"))
	response = httptest.NewRecorder()
	store.ServeHTTP(response, httptest.NewRequest("GET", "/sources/"+sourceToken(cfg, loc), nil))
	if response.Code != 409 {
		t.Fatalf("changed source returned %d", response.Code)
	}
}
