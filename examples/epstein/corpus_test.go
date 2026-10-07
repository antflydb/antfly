// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package main

import (
	"archive/zip"
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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
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
	indexConfig, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, DefaultInferenceURL, "en-US", false, false)
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
	indexes, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, DefaultInferenceURL, "en-US", true, true)
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
	if decoded[DefaultEmbeddingIndex].(map[string]any)["coverage_policy"] != "partial" {
		t.Fatal("intentional graph-row skips must settle without hiding provider failures")
	}
	artifact := graph["artifact"].(map[string]any)
	if artifact["source"].(map[string]any)["value"] != "content" {
		t.Fatal("graph must consume materialized unit text")
	}

}

func TestCorpusExtractorModelUsesRegisteredIdentifier(t *testing.T) {
	for _, model := range []string{DefaultAutographModel, "custom/extractor:gguf:Q4_K"} {
		indexes, err := corpusIndexes(DefaultEmbeddingModel, model, DefaultInferenceURL, "en-US", true, false)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(indexes[DefaultAutographIndex])
		if err != nil {
			t.Fatal(err)
		}
		var graph map[string]any
		if err = json.Unmarshal(raw, &graph); err != nil {
			t.Fatal(err)
		}
		producer := graph["artifact"].(map[string]any)["producer_json"].(map[string]any)
		if got := producer["config"].(map[string]any)["model"]; got != model {
			t.Fatalf("model=%v want=%s", got, model)
		}
	}
	if DefaultAutographModel != "antflydb/gliner2-base-v1" {
		t.Fatalf("unregistered default %s", DefaultAutographModel)
	}
}

func pdfPageCount(reader io.ReaderAt, size int64) (int, error) {
	p, e := pdf.NewReader(reader, size)
	if e != nil {
		return 0, e
	}
	return p.NumPage(), nil
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
	hit.Hierarchy.Matches[0].Source["_start_time_ms"] = float64(1250)
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

func TestCorpusPDFWhitespaceHeader(t *testing.T) {
	// Insert header whitespace and adjust xref offsets to keep the PDF valid.
	lines := strings.Split(string(bytes.Replace(createMultiPagePDF(2), []byte("%PDF-1.4\n"), []byte("%PDF-1.4 \n"), 1)), "\n")
	for i, line := range lines {
		var offset int
		if strings.HasSuffix(line, "00000 n ") {
			fmt.Sscanf(line, "%d", &offset)
			lines[i] = fmt.Sprintf("%010d 00000 n ", offset+1)
		}
		if i > 0 && lines[i-1] == "startxref" {
			fmt.Sscanf(line, "%d", &offset)
			lines[i] = fmt.Sprint(offset + 1)
		}
	}
	data := []byte(strings.Join(lines, "\n"))
	source := t.TempDir()
	writeCorpusFixture(t, filepath.Join(source, "whitespace.pdf"), data)
	state := filepath.Join(t.TempDir(), "state")
	if err := corpusPrepareCmd([]string{"--state", state, "--source", "test=" + source, "--base-url", "http://localhost", "--pages-per-record", "1"}); err != nil {
		t.Fatal(err)
	}
	cfg := testCorpusConfig(t, state)
	store := newCorpusStore(cfg)
	defer store.Close()
	info := mustStat(t, filepath.Join(source, "whitespace.pdf"))
	for _, bounds := range [][2]int{{1, 2}, {2, 2}} {
		loc := sourceLocator{Source: 0, Name: "whitespace.pdf", Size: info.Size(), Modified: info.ModTime().UnixNano(), First: bounds[0], Last: bounds[1]}
		response := httptest.NewRecorder()
		store.ServeHTTP(response, httptest.NewRequest("GET", "/sources/"+sourceToken(cfg, loc), nil))
		if response.Code != 200 {
			t.Fatalf("range %v: %d %s", bounds, response.Code, response.Body.String())
		}
		if bounds[0] == 1 {
			if !bytes.Equal(response.Body.Bytes(), data) {
				t.Fatal("whole-document request changed source bytes")
			}
		} else {
			doc, err := pdf.NewReader(bytes.NewReader(response.Body.Bytes()), int64(response.Body.Len()))
			if err != nil || doc.NumPage() != 1 {
				t.Fatalf("split PDF: %v", err)
			}
		}
	}
}

type blockedCorpusResponse struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockedCorpusResponse) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return w.ResponseRecorder.Write(p)
}

func TestCorpusConcurrentArchiveTransfers(t *testing.T) {
	dir := t.TempDir()
	cfg := corpusConfig{Key: "key"}
	var locs []sourceLocator
	payload := bytes.Repeat([]byte("PDF source bytes\n"), 20000)
	for i := range 2 {
		name := filepath.Join(dir, fmt.Sprintf("source%d.zip", i))
		f, err := os.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		zw := zip.NewWriter(f)
		w, err := zw.CreateHeader(&zip.FileHeader{Name: "file.pdf", Method: zip.Store})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err = zw.Close(); err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
		zr, err := zip.OpenReader(name)
		if err != nil {
			t.Fatal(err)
		}
		info := mustStat(t, name)
		locs = append(locs, sourceLocator{Source: i, Name: "file.pdf", Size: int64(len(payload)), Modified: info.ModTime().UnixNano(), CRC: zr.File[0].CRC32})
		zr.Close()
		cfg.Sources = append(cfg.Sources, corpusSource{Path: name, Zip: true})
	}
	store := newCorpusStore(cfg)
	defer store.Close()
	first := &blockedCorpusResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(first.release) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		store.ServeHTTP(first, httptest.NewRequest("GET", "/sources/"+sourceToken(cfg, locs[0]), nil))
	}()
	select {
	case <-first.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first transfer did not start")
	}
	second := httptest.NewRecorder()
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		store.ServeHTTP(second, httptest.NewRequest("GET", "/sources/"+sourceToken(cfg, locs[1]), nil))
	}()
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("second archive blocked behind first transfer")
	}
	// Also exercise Close while an evicted archive still has an active reader.
	store.Close()
	release.Do(func() { close(first.release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("first transfer did not finish")
	}
	for _, response := range []*httptest.ResponseRecorder{first.ResponseRecorder, second} {
		if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), payload) {
			t.Fatalf("archive transfer truncated: status=%d bytes=%d", response.Code, response.Body.Len())
		}
	}
}

func TestCorpusAudioMatchingChunkTimestamp(t *testing.T) {
	server := &SearchServer{corpus: true, indexes: []string{corpusTextIndex}}
	request := server.searchRequest("late phrase")
	if !slices.Contains(request.Hierarchy.GroupBy.Matches.Fields, "_start_time_ms") {
		t.Fatal("search must project the matching chunk timestamp")
	}
	hit := antfly.QueryHit{Source: map[string]any{"mime_type": "audio/wav", "original_url": "http://source/audio"}, Hierarchy: antfly.QueryHitHierarchy{Matches: []antfly.HierarchyMatchHit{{Source: map[string]any{"text": "late phrase", "_start_time_ms": float64(9000)}, Hierarchy: antfly.HierarchyMatchContext{Ancestors: antfly.QueryHitHierarchyAncestors{Unit: antfly.HierarchyAncestor{Document: map[string]any{"provenance": map[string]any{"transcript_spans": []any{map[string]any{"start_ms": float64(0)}, map[string]any{"start_ms": float64(9000)}}}}}}}}}}}
	for _, test := range []struct {
		time any
		want string
	}{{float64(9000), "http://source/audio#t=9.000"}, {float64(0), "http://source/audio#t=0.000"}, {nil, "http://source/audio"}, {float64(-1), "http://source/audio"}} {
		hit.Hierarchy.Matches[0].Source["_start_time_ms"] = test.time
		var result SearchResult
		applyCorpusHit(&result, hit)
		if result.URL != test.want {
			t.Fatalf("timestamp %v: %s", test.time, result.URL)
		}
	}
}
