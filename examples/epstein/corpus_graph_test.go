// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type graphFixture struct {
	versions                       map[string]int
	sourceVersion                  int
	replaceSourceDuringPublish     bool
	supersedeDuringPublish         bool
	record                         corpusRecord
	state                          string
	server                         *httptest.Server
	generation                     uint64
	units                          []map[string]any
	rows                           map[string]map[string]any
	queries, writes, manifestReads int
	changeDuringStage              bool
	failMarker                     bool
}

func newGraphFixture(t *testing.T) *graphFixture {
	t.Helper()
	f := &graphFixture{state: t.TempDir(), generation: 1, sourceVersion: 1, versions: map[string]int{}, rows: map[string]map[string]any{}, record: corpusRecord{ID: "ds:EFTA00000001.pdf:p0000026", Document: map[string]any{"title": "source.pdf", "original_url": "http://source/pdf#page=26", "mime_type": "application/pdf", "metadata": map[string]any{"page_start": float64(26)}}}}
	for i := 1; i <= 2; i++ {
		f.units = append(f.units, graphTestUnit(i, fmt.Sprintf("page %d", i), "completed"))
	}
	data, _ := json.Marshal(f.record)
	if err := os.WriteFile(filepath.Join(f.state, "records.ndjson"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}
func graphTestUnit(i int, text, status string) map[string]any {
	id := fmt.Sprintf("unit%03d", i)
	return map[string]any{"_id": id, "_source": map[string]any{"unit_id": fmt.Sprintf("page:%06d", i), "text": text, "provenance": map[string]any{"page_number": i, "extraction_status": status}}, "_sort": []any{id, id}}
}
func (f *graphFixture) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		if strings.Contains(r.URL.Path, "/artifacts/") {
			f.manifestReads++
			if f.changeDuringStage && f.manifestReads == 2 {
				f.generation++
			}
			json.NewEncoder(w).Encode(map[string]any{"unit_count": len(f.units), "generation": f.generation, "source_fingerprint": "source-fingerprint"})
			return
		}
		if strings.Contains(r.URL.Path, "/documents/") {
			key, _ := url.PathUnescape(strings.SplitN(r.URL.EscapedPath(), "/documents/", 2)[1])
			if key == f.record.ID {
				w.Header().Set("X-Antfly-Version", strconv.Itoa(f.sourceVersion))
				json.NewEncoder(w).Encode(f.record.Document)
				return
			}
			doc, ok := f.rows[key]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("X-Antfly-Version", strconv.Itoa(f.versions[key]))
			json.NewEncoder(w).Encode(doc)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"table_id": "table-graph", "indexes": map[string]any{DefaultAutographIndex: map[string]any{}}})
		return
	}
	var request map[string]any
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/query") {
		f.queries++
		start := 0
		if cursor, ok := request["search_after"].([]any); ok {
			id := cursor[0].(string)
			for start < len(f.units) && f.units[start]["_id"].(string) <= id {
				start++
			}
		}
		end := min(start+50, len(f.units))
		json.NewEncoder(w).Encode(map[string]any{"responses": []any{map[string]any{"hits": map[string]any{"hits": f.units[start:end]}}}})
		return
	}
	if strings.HasSuffix(r.URL.Path, "/documents") {
		from := request["from"].(string)
		to := request["to"].(string)
		var keys []string
		for key := range f.rows {
			if key > from && key < to {
				keys = append(keys, key)
			}
		}
		slices.Sort(keys)
		for _, key := range keys[:min(50, len(keys))] {
			json.NewEncoder(w).Encode(map[string]any{"_id": key})
		}
		return
	}
	readSet := request["read_set"].([]any)
	tables := request["tables"].(map[string]any)
	for _, operations := range tables {
		request = operations.(map[string]any)
	}
	var publishingUnits bool
	if inserts, ok := request["inserts"].(map[string]any); ok {
		for _, doc := range inserts {
			if doc.(map[string]any)["content"] != nil {
				publishingUnits = true
			}
		}
	}
	markerKey := f.record.ID + ":graph:state"
	if publishingUnits && f.replaceSourceDuringPublish {
		f.sourceVersion++
		f.replaceSourceDuringPublish = false
	}
	if publishingUnits && f.supersedeDuringPublish {
		f.versions[markerKey]++
		f.rows[markerKey] = map[string]any{"metadata": map[string]any{"corpus_graph_owner": "newer-writer", "corpus_graph_revision": "newer-revision"}}
		f.supersedeDuringPublish = false
	}
	for _, raw := range readSet {
		item := raw.(map[string]any)
		key := item["key"].(string)
		version := f.versions[key]
		if key == f.record.ID {
			version = f.sourceVersion
		}
		if item["version"] != strconv.Itoa(version) {
			http.Error(w, "version conflict", 409)
			return
		}
	}
	f.writes++
	if inserts, ok := request["inserts"].(map[string]any); ok {
		for key, raw := range inserts {
			if f.failMarker && strings.HasSuffix(key, ":state") && raw.(map[string]any)["metadata"].(map[string]any)["corpus_graph_revision"] != "" {
				http.Error(w, "injected marker failure", 500)
				return
			}
			doc := raw.(map[string]any)
			if doc["url"] != nil {
				http.Error(w, "recursive media extraction", 400)
				return
			}
			f.rows[key] = doc
			f.versions[key]++
		}
	}
	if deletes, ok := request["deletes"].([]any); ok {
		for _, key := range deletes {
			delete(f.rows, key.(string))
			delete(f.versions, key.(string))
		}
	}
	json.NewEncoder(w).Encode(map[string]any{"status": "committed"})
}
func (f *graphFixture) run() error {
	f.manifestReads = 0
	return corpusGraphCmd([]string{"--state", f.state, "--url", f.server.URL})
}
func (f *graphFixture) unitRows() []map[string]any {
	var result []map[string]any
	for _, row := range f.rows {
		if row["content"] != nil {
			result = append(result, row)
		}
	}
	return result
}

func TestCorpusGraphRevisionRefreshAndCrashRecovery(t *testing.T) {
	f := newGraphFixture(t)
	if err := f.run(); err != nil {
		t.Fatal(err)
	}
	if len(f.unitRows()) != 2 {
		t.Fatalf("initial rows=%v", f.rows)
	}
	writes, queries := f.writes, f.queries
	if err := f.run(); err != nil {
		t.Fatal(err)
	}
	if f.writes != writes || f.queries != queries {
		t.Fatal("unchanged materialization was rewritten")
	}
	f.generation = 2
	f.units = []map[string]any{graphTestUnit(1, "repaired OCR", "completed"), graphTestUnit(3, "new third page", "completed")}
	f.failMarker = true
	if err := f.run(); err == nil {
		t.Fatal("marker failure must surface")
	}
	// An interrupted publication leaves a new row which never entered a completed
	// checkpoint. A subsequent revision must still find and delete it.
	f.generation = 3
	f.units = []map[string]any{graphTestUnit(1, "newest OCR", "completed")}
	f.failMarker = false
	if err := f.run(); err != nil {
		t.Fatal(err)
	}
	rows := f.unitRows()
	if len(rows) != 1 || rows[0]["content"] != "newest OCR" {
		t.Fatalf("stale materialization survived: %v", f.rows)
	}
	if err := os.Remove(filepath.Join(f.state, "graph.json")); err != nil {
		t.Fatal(err)
	}
	writes = f.writes
	if err := f.run(); err != nil {
		t.Fatal(err)
	}
	if f.writes != writes {
		t.Fatal("lost local checkpoint must reuse durable server revision")
	}
}

func TestCorpusGraphRejectsChangingAndFailedArtifactsBeforePublishing(t *testing.T) {
	for _, scenario := range []string{"generation change", "failed OCR"} {
		t.Run(scenario, func(t *testing.T) {
			f := newGraphFixture(t)
			if err := f.run(); err != nil {
				t.Fatal(err)
			}
			writes := f.writes
			f.generation = 2
			if scenario == "generation change" {
				f.changeDuringStage = true
			} else {
				f.units[1] = graphTestUnit(2, "bad text", "failed_ocr")
			}
			if err := f.run(); err == nil {
				t.Fatal("unstable/failed source must be rejected")
			}
			if f.writes != writes {
				t.Fatal("existing materialization changed before source validation completed")
			}
		})
	}
}

func TestCorpusGraphPagesUnitsAndDeletesBeyondOneBatch(t *testing.T) {
	f := newGraphFixture(t)
	f.units = nil
	for i := 1; i <= 105; i++ {
		f.units = append(f.units, graphTestUnit(i, fmt.Sprintf("page %d", i), "completed"))
	}
	if err := f.run(); err != nil {
		t.Fatal(err)
	}
	if len(f.unitRows()) != 105 {
		t.Fatal("unit pagination truncated")
	}
	f.generation = 2
	f.units = f.units[:1]
	if err := f.run(); err != nil {
		t.Fatal(err)
	}
	if len(f.unitRows()) != 1 {
		t.Fatal("stale-row pagination truncated")
	}
}

func TestCorpusGraphUnitCitationsAndVisualization(t *testing.T) {
	f := newGraphFixture(t)
	f.units = []map[string]any{graphTestUnit(3, "third page", "completed")}
	if err := f.run(); err != nil {
		t.Fatal(err)
	}
	row := f.unitRows()[0]
	if row["original_url"] != "http://source/pdf#page=28" {
		t.Fatalf("citation=%v", row["original_url"])
	}
	node := graphNodeFromDocument("graph-unit", row, 1)
	if node.URL != "http://source/pdf#page=28" || node.Subtitle != "page 28" {
		t.Fatalf("node=%+v", node)
	}
	audio := corpusRecord{Document: map[string]any{"mime_type": "audio/aiff", "original_url": "http://source/audio"}}
	citation, _ := corpusGraphCitation(audio, map[string]any{"transcript_spans": []any{map[string]any{"start_ms": float64(9000)}}})
	if citation != "http://source/audio#t=9.000" {
		t.Fatalf("audio citation=%s", citation)
	}
}

func TestCorpusGraphFencesSourceReplacementAndSupersededWriters(t *testing.T) {
	for _, scenario := range []string{"source replacement", "newer writer"} {
		t.Run(scenario, func(t *testing.T) {
			f := newGraphFixture(t)
			if err := f.run(); err != nil {
				t.Fatal(err)
			}
			f.generation = 2
			f.units = []map[string]any{graphTestUnit(1, "stale publication", "completed")}
			if scenario == "source replacement" {
				f.replaceSourceDuringPublish = true
			} else {
				f.supersedeDuringPublish = true
			}
			if err := f.run(); err == nil {
				t.Fatal("unfenced publication succeeded")
			}
			for _, doc := range f.unitRows() {
				if doc["content"] == "stale publication" {
					t.Fatal("obsolete writer overwrote unit rows")
				}
			}
			if scenario == "newer writer" {
				marker := f.rows[f.record.ID+":graph:state"]["metadata"].(map[string]any)
				if marker["corpus_graph_owner"] != "newer-writer" {
					t.Fatal("old pass modified the newer writer's marker")
				}
			}
			f.generation = 3
			f.units = []map[string]any{graphTestUnit(1, "current publication", "completed")}
			if err := f.run(); err != nil {
				t.Fatal(err)
			}
			if len(f.unitRows()) != 1 || f.unitRows()[0]["content"] != "current publication" {
				t.Fatal("new pass did not reconcile interrupted state")
			}
		})
	}
}
