package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	antfly "github.com/antflydb/antfly/go/pkg/sdk"
	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
)

// createMinimalPDF creates a valid single-page PDF in memory.
func createMinimalPDF() []byte {
	var buf bytes.Buffer
	buf.WriteString("%PDF-1.4\n")

	off1 := buf.Len()
	buf.WriteString("1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")

	off2 := buf.Len()
	buf.WriteString("2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n")

	off3 := buf.Len()
	buf.WriteString("3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\nendobj\n")

	xrefOff := buf.Len()
	buf.WriteString("xref\n")
	buf.WriteString("0 4\n")
	buf.WriteString("0000000000 65535 f \n")
	fmt.Fprintf(&buf, "%010d 00000 n \n", off1)
	fmt.Fprintf(&buf, "%010d 00000 n \n", off2)
	fmt.Fprintf(&buf, "%010d 00000 n \n", off3)
	buf.WriteString("trailer\n<< /Size 4 /Root 1 0 R >>\n")
	buf.WriteString("startxref\n")
	fmt.Fprintf(&buf, "%d\n", xrefOff)
	buf.WriteString("%%EOF\n")

	return buf.Bytes()
}

// createMultiPagePDF creates a valid multi-page PDF in memory.
func createMultiPagePDF(numPages int) []byte {
	var buf bytes.Buffer
	buf.WriteString("%PDF-1.4\n")

	offsets := make([]int, 0, numPages+2)

	// Object 1: Catalog
	offsets = append(offsets, buf.Len())
	buf.WriteString("1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")

	// Object 2: Pages
	offsets = append(offsets, buf.Len())
	kids := make([]string, numPages)
	for i := range numPages {
		kids[i] = fmt.Sprintf("%d 0 R", i+3)
	}
	fmt.Fprintf(&buf, "2 0 obj\n<< /Type /Pages /Kids [%s] /Count %d >>\nendobj\n",
		strings.Join(kids, " "), numPages)

	// Page objects
	for i := range numPages {
		offsets = append(offsets, buf.Len())
		fmt.Fprintf(&buf, "%d 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\nendobj\n", i+3)
	}

	// Cross-reference table
	numObjs := numPages + 3
	xrefOff := buf.Len()
	buf.WriteString("xref\n")
	fmt.Fprintf(&buf, "0 %d\n", numObjs)
	buf.WriteString("0000000000 65535 f \n")
	for _, off := range offsets {
		fmt.Fprintf(&buf, "%010d 00000 n \n", off)
	}

	buf.WriteString("trailer\n")
	fmt.Fprintf(&buf, "<< /Size %d /Root 1 0 R >>\n", numObjs)
	buf.WriteString("startxref\n")
	fmt.Fprintf(&buf, "%d\n", xrefOff)
	buf.WriteString("%%EOF\n")

	return buf.Bytes()
}

// createTestZip creates a zip file in a temp directory with the given files.
func createTestZip(t *testing.T, files map[string][]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.zip")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	for name, data := range files {
		fw, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fw.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCreateEmbeddingIndexUsesAntflyClipClap(t *testing.T) {
	idx, err := createEmbeddingIndex(DefaultEmbeddingModel, DefaultInferenceURL, DefaultChunkerModel, 512, 50)
	if err != nil {
		t.Fatalf("createEmbeddingIndex failed: %v", err)
	}

	cfg, err := idx.AsEmbeddingsIndexConfig()
	if err != nil {
		t.Fatalf("AsEmbeddingsIndexConfig failed: %v", err)
	}

	embedder, err := cfg.Embedder.AsAntflyEmbedderConfig()
	if err != nil {
		t.Fatalf("AsAntflyEmbedderConfig failed: %v", err)
	}
	if cfg.Embedder.Provider != antfly.EmbedderProviderAntfly {
		t.Fatalf("embedder provider = %q, want %q", cfg.Embedder.Provider, antfly.EmbedderProviderAntfly)
	}
	if cfg.Dimension != DefaultEmbeddingDims {
		t.Fatalf("embedding dimension = %d, want %d", cfg.Dimension, DefaultEmbeddingDims)
	}
	if embedder.Model != DefaultEmbeddingModel {
		t.Fatalf("embedder model = %q, want %q", embedder.Model, DefaultEmbeddingModel)
	}

	if cfg.Chunker.Provider != antfly.ChunkerProviderAntfly {
		t.Fatalf("chunker provider = %q, want %q", cfg.Chunker.Provider, antfly.ChunkerProviderAntfly)
	}
	wantChunkerURL := DefaultInferenceURL + "/ai/v1"
	if cfg.Chunker.ApiUrl != wantChunkerURL {
		t.Fatalf("chunker api URL = %q, want %q", cfg.Chunker.ApiUrl, wantChunkerURL)
	}
	if cfg.Chunker.Model != DefaultChunkerModel {
		t.Fatalf("chunker model = %q, want %q", cfg.Chunker.Model, DefaultChunkerModel)
	}
}

func TestCreateArtifactGraphIndexIncludesProducerConfig(t *testing.T) {
	idx, err := createArtifactGraphIndex(
		DefaultAutographIndex,
		DefaultAutographAsset,
		"generator",
		"gemma4-test",
		DefaultInferenceURL,
		[]string{"person", "organization"},
		[]string{"communicated with"},
	)
	if err != nil {
		t.Fatalf("createArtifactGraphIndex failed: %v", err)
	}

	encoded, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("marshal graph index: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal graph index: %v", err)
	}
	if got["name"] != DefaultAutographIndex {
		t.Fatalf("name = %v, want %s", got["name"], DefaultAutographIndex)
	}
	source, ok := got["source"].(map[string]any)
	if !ok {
		t.Fatalf("source missing from graph index: %s", encoded)
	}
	if source["artifact"] != DefaultAutographAsset || source["path"] != "$.relations[*]" {
		t.Fatalf("unexpected source config: %#v", source)
	}
	if _, exists := source["kind"]; exists {
		t.Fatalf("graph source must not contain a kind discriminator: %#v", source)
	}
	artifact, ok := got["artifact"].(map[string]any)
	if !ok {
		t.Fatalf("artifact missing from graph index: %s", encoded)
	}
	if _, exists := artifact["field"]; exists {
		t.Fatalf("graph artifact producer must use source instead of root field: %#v", artifact)
	}
	producer, ok := artifact["producer_json"].(map[string]any)
	if !ok {
		t.Fatalf("producer_json missing from artifact config: %#v", artifact)
	}
	artifactSource, ok := artifact["source"].(map[string]any)
	if !ok || artifactSource["type"] != "field" || artifactSource["value"] != "content" {
		t.Fatalf("unexpected artifact producer source: %#v", artifact["source"])
	}
	cfg, ok := producer["config"].(map[string]any)
	if !ok {
		t.Fatalf("producer config missing: %#v", producer)
	}
	if cfg["provider"] != "antfly" || cfg["model"] != "gemma4-test" {
		t.Fatalf("unexpected producer config: %#v", cfg)
	}
	if cfg["api_url"] != DefaultInferenceURL+"/ai/v1" {
		t.Fatalf("api_url = %v, want %s", cfg["api_url"], DefaultInferenceURL+"/ai/v1")
	}
	if producer["type"] != "generator" {
		t.Fatalf("producer type = %v, want generator", producer["type"])
	}
	if cfg["tool_output"] != "arguments" || cfg["tool_name"] != "emit_relations" {
		t.Fatalf("unexpected tool output config: %#v", cfg)
	}
	tools, ok := cfg["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools missing from producer config: %#v", cfg)
	}
}

func TestCreateArtifactGraphIndexDefaultsToExtractorConfig(t *testing.T) {
	idx, err := createArtifactGraphIndex(
		DefaultAutographIndex,
		DefaultAutographAsset,
		"",
		DefaultRecognizerModel,
		DefaultInferenceURL,
		[]string{"person", "organization"},
		[]string{"communicated with"},
	)
	if err != nil {
		t.Fatalf("createArtifactGraphIndex failed: %v", err)
	}

	encoded, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("marshal graph index: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal graph index: %v", err)
	}
	artifact := got["artifact"].(map[string]any)
	producer := artifact["producer_json"].(map[string]any)
	if producer["type"] != "extractor" {
		t.Fatalf("producer type = %v, want extractor", producer["type"])
	}
	cfg := producer["config"].(map[string]any)
	if cfg["provider"] != "antfly" || cfg["model"] != DefaultRecognizerModel {
		t.Fatalf("unexpected extractor config: %#v", cfg)
	}
	schema := cfg["schema"].(map[string]any)
	if len(schema["entities"].([]any)) != 2 || len(schema["relations"].([]any)) != 1 {
		t.Fatalf("unexpected extractor schema: %#v", schema)
	}
	source := got["source"].(map[string]any)
	nodes := source["nodes"].(map[string]any)
	if nodes["target"] != "{{ _item.target.text }}" {
		t.Fatalf("unexpected extractor node mapping: %#v", nodes)
	}
	edge := source["edge"].(map[string]any)
	if edge["weight"] != "{{ _item.score }}" {
		t.Fatalf("unexpected extractor edge mapping: %#v", edge)
	}
}

func TestGraphVisualizationQueryUsesAutographIndex(t *testing.T) {
	req := graphVisualizationQuery("Maxwell")
	fullText, ok := req["full_text_search"].(map[string]any)
	if !ok || fullText["query"] != "Maxwell" {
		t.Fatalf("unexpected full-text search: %#v", req["full_text_search"])
	}
	graphSearches, ok := req["graph_queries"].(map[string]any)
	if !ok {
		t.Fatalf("graph searches missing: %#v", req)
	}
	graph, ok := graphSearches["relations"].(map[string]any)
	if !ok {
		t.Fatalf("relations graph search missing: %#v", graphSearches)
	}
	if graph["index"] != DefaultAutographIndex {
		t.Fatalf("graph index = %q, want %s", graph["index"], DefaultAutographIndex)
	}
	traverse, ok := graph["traverse"].(map[string]any)
	if !ok {
		t.Fatalf("traverse operation missing: %#v", graph)
	}
	startNodes, ok := traverse["start"].(map[string]any)
	if !ok || startNodes["result_ref"] != "$query_results" || startNodes["limit"] != 8 {
		t.Fatalf("unexpected start nodes: %#v", traverse["start"])
	}
	if traverse["direction"] != "both" || traverse["max_depth"] != 1 {
		t.Fatalf("unexpected traversal: %#v", traverse)
	}
	if traverse["include_documents"] != true {
		t.Fatalf("graph traversal must hydrate visualization documents: %#v", traverse)
	}
	fields, ok := traverse["fields"].([]string)
	if !ok || len(fields) != 3 || fields[0] != "title" || fields[1] != "url" || fields[2] != "metadata" {
		t.Fatalf("unexpected graph document fields: %#v", traverse["fields"])
	}
}

func TestBuildGraphVisualizationConvertsGraphResults(t *testing.T) {
	var graphResult antfly.GraphResult
	err := graphResult.FromGraphNodesResult(antfly.GraphNodesResult{
		Kind: antfly.GraphNodesResultKindNodes,
		Nodes: []antfly.GraphResultNode{
			{
				Key:   "doc-a",
				Depth: 0,
				Document: map[string]any{
					"title": "Alpha page",
					"url":   "http://example.test/a.pdf",
					"metadata": map[string]any{
						"source_file": "court/source.pdf",
						"page_number": float64(12),
					},
				},
			},
			{
				Key:   "doc-b",
				Depth: 1,
				Document: map[string]any{
					"title": "Beta page",
				},
				Path: []antfly.GraphPathEndpoint{
					{Key: "doc-a"},
					{Key: "doc-b"},
				},
				PathEdges: []antfly.GraphPathEdge{
					{From: antfly.GraphPathEndpoint{Key: "doc-a"}, To: antfly.GraphPathEndpoint{Key: "doc-b"}, Direction: antfly.GraphPathEdgeDirectionOut, Type: "mentioned in", Weight: 0.75},
				},
			},
			{
				Key:   "Jeffrey Epstein",
				Depth: 1,
				Path: []antfly.GraphPathEndpoint{
					{Key: "doc-a"},
					{Key: "Jeffrey Epstein"},
				},
			},
		},
		Paths: []antfly.GraphPath{},
		Stats: antfly.GraphQueryStats{ReturnedItems: 3, Truncated: false},
	})
	if err != nil {
		t.Fatalf("construct graph result: %v", err)
	}
	if _, err := antfly.DecodeCanonicalGraphResult(graphResult); err != nil {
		t.Fatalf("decode graph result envelope: %v", err)
	}
	resp := antfly.QueryResponses{
		Responses: []antfly.QueryResult{
			{
				GraphResults: map[string]antfly.GraphResult{
					"relations": graphResult,
				},
			},
		},
	}

	viz := buildGraphVisualization("Maxwell", &resp)
	if viz.Query != "Maxwell" {
		t.Fatalf("query = %q, want Maxwell", viz.Query)
	}
	if len(viz.Nodes) != 3 {
		t.Fatalf("nodes = %d, want 3: %#v", len(viz.Nodes), viz.Nodes)
	}
	if viz.Nodes[0].Label != "Alpha page" || viz.Nodes[0].Subtitle != "source.pdf - page 12" {
		t.Fatalf("unexpected first node: %#v", viz.Nodes[0])
	}
	if len(viz.Edges) != 2 {
		t.Fatalf("edges = %d, want 2: %#v", len(viz.Edges), viz.Edges)
	}
	if viz.Edges[0].Source != "doc-a" || viz.Edges[0].Target != "doc-b" || viz.Edges[0].Type != "mentioned in" {
		t.Fatalf("unexpected edge: %#v", viz.Edges[0])
	}
	if viz.Edges[1].Source != "doc-a" || viz.Edges[1].Target != "Jeffrey Epstein" || viz.Edges[1].Type != "related" {
		t.Fatalf("unexpected path-derived edge: %#v", viz.Edges[1])
	}
}

func TestInferenceMLBaseURLNormalizesRoots(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "root", in: "http://localhost:8080", want: "http://localhost:8080/ai/v1"},
		{name: "api root", in: "http://localhost:8080/db/v1", want: "http://localhost:8080/ai/v1"},
		{name: "already ml", in: "http://localhost:8080/ai/v1/", want: "http://localhost:8080/ai/v1"},
		{name: "cloud root", in: "https://platform.antfly.io/cloud/v1/instance", want: "https://platform.antfly.io/cloud/v1/instance/ai/v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := inferenceMLBaseURL(tt.in)
			if err != nil {
				t.Fatalf("inferenceMLBaseURL failed: %v", err)
			}
			if got != tt.want {
				t.Fatalf("inferenceMLBaseURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestInferenceMLBaseURLRejectsLegacyAPI(t *testing.T) {
	if _, err := inferenceMLBaseURL("http://localhost:8082/api"); err == nil {
		t.Fatalf("inferenceMLBaseURL accepted legacy /api URL")
	}
}

func TestParseSyncLevelFlag(t *testing.T) {
	tests := []struct {
		in   string
		want antfly.SyncLevel
	}{
		{in: "write", want: antfly.SyncLevelWrite},
		{in: " full_text ", want: antfly.SyncLevelFullText},
		{in: "embeddings", want: antfly.SyncLevelFullIndex},
		{in: "full_index", want: antfly.SyncLevelFullIndex},
		{in: "enrichments", want: antfly.SyncLevelEnrichments},
		{in: "propose", want: antfly.SyncLevelPropose},
	}

	for _, tt := range tests {
		got, err := parseSyncLevelFlag(tt.in)
		if err != nil {
			t.Fatalf("parseSyncLevelFlag(%q) failed: %v", tt.in, err)
		}
		if got != tt.want {
			t.Fatalf("parseSyncLevelFlag(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}

	if _, err := parseSyncLevelFlag("later"); err == nil {
		t.Fatalf("parseSyncLevelFlag accepted invalid value")
	}
}

func TestCreateSearchTableIndexesUsesServerDefaultFullText(t *testing.T) {
	embeddingIndex, err := createEmbeddingIndex(DefaultEmbeddingModel, DefaultInferenceURL, DefaultChunkerModel, 512, 50)
	if err != nil {
		t.Fatalf("createEmbeddingIndex failed: %v", err)
	}
	indexes, err := createSearchTableIndexes(*embeddingIndex)
	if err != nil {
		t.Fatalf("createSearchTableIndexes failed: %v", err)
	}
	if _, ok := indexes[DefaultEmbeddingIndex]; !ok {
		t.Fatalf("embedding index missing from created table indexes: %#v", indexes)
	}
	if _, ok := indexes[DefaultFullTextIndex]; ok {
		t.Fatalf("full-text index should be provided by server default, got explicit config: %#v", indexes[DefaultFullTextIndex])
	}
}

func TestStreamJSONPagesNormalizesReservedTypeField(t *testing.T) {
	input := `{
  "doc_001": {
    "_type": "pdf_page",
    "content": "one",
    "metadata": {"page_number": 1}
  },
  "doc_002": {
    "_type": "pdf_page",
    "document_type": "custom_page",
    "content": "two"
  }
}`

	var batches []map[string]any
	for batch := range streamJSONPages(strings.NewReader(input), 1) {
		batches = append(batches, batch)
	}
	if len(batches) != 2 {
		t.Fatalf("batch count = %d, want 2", len(batches))
	}

	first, ok := batches[0]["doc_001"].(map[string]any)
	if !ok {
		t.Fatalf("doc_001 has type %T, want map[string]any", batches[0]["doc_001"])
	}
	if _, ok := first["_type"]; ok {
		t.Fatalf("doc_001 still has reserved _type field")
	}
	if got := first["document_type"]; got != "pdf_page" {
		t.Fatalf("doc_001 document_type = %v, want pdf_page", got)
	}
	if got := first["content"]; got != "one" {
		t.Fatalf("doc_001 content = %v, want one", got)
	}

	second, ok := batches[1]["doc_002"].(map[string]any)
	if !ok {
		t.Fatalf("doc_002 has type %T, want map[string]any", batches[1]["doc_002"])
	}
	if _, ok := second["_type"]; ok {
		t.Fatalf("doc_002 still has reserved _type field")
	}
	if got := second["document_type"]; got != "custom_page" {
		t.Fatalf("doc_002 document_type = %v, want custom_page", got)
	}
}

func TestStreamJSONPagesWithLimit(t *testing.T) {
	input := `{
  "doc_001": {"content": "one"},
  "doc_002": {"content": "two"},
  "doc_003": {"content": "three"}
}`

	var batches []map[string]any
	for batch := range streamJSONPagesWithLimit(strings.NewReader(input), 2, 1) {
		batches = append(batches, batch)
	}
	if len(batches) != 1 {
		t.Fatalf("batch count = %d, want 1", len(batches))
	}
	if len(batches[0]) != 1 {
		t.Fatalf("record count = %d, want 1", len(batches[0]))
	}
	if _, ok := batches[0]["doc_001"]; !ok {
		t.Fatalf("limited batch missing first record: %#v", batches[0])
	}
}

func TestEntityCandidatesSkipExistingUnlessReprocess(t *testing.T) {
	records := map[string]map[string]any{
		"b": {"content": "Jane Doe wrote to Acme."},
		"a": {
			"content": "John Smith works at Google.",
			"metadata": map[string]any{
				"entities": []any{},
			},
		},
		"c": {"content": "   "},
	}

	candidates := entityCandidates(records, false, DefaultRecognizerModel, splitCSV(DefaultEntityLabels), splitCSV(DefaultRelationLabels), DefaultEntityMaxChars, DefaultEntityOverlap)
	if len(candidates) != 1 || candidates[0].id != "b" {
		t.Fatalf("candidates = %#v, want only b", candidates)
	}

	candidates = entityCandidates(records, true, DefaultRecognizerModel, splitCSV(DefaultEntityLabels), splitCSV(DefaultRelationLabels), DefaultEntityMaxChars, DefaultEntityOverlap)
	if len(candidates) != 2 || candidates[0].id != "a" || candidates[1].id != "b" {
		t.Fatalf("reprocess candidates = %#v, want a then b", candidates)
	}
}

func TestEntityCandidatesRetryIncompleteMatchingWindows(t *testing.T) {
	records := map[string]map[string]any{
		"a": {
			"content": "John Smith works at Google.",
			"metadata": map[string]any{
				"entities":              []any{},
				"entity_model":          DefaultRecognizerModel,
				"entity_labels":         []any{"person", "organization"},
				"entity_window_chars":   float64(20),
				"entity_overlap_chars":  float64(5),
				"relations":             []any{},
				"relation_labels":       []any{"worked for"},
				"entity_error_windows":  float64(1),
				"entity_failed_windows": []any{map[string]any{"start": float64(0), "end": float64(20)}},
			},
		},
	}

	candidates := entityCandidates(records, false, DefaultRecognizerModel, []string{"person", "organization"}, []string{"worked for"}, 20, 5)
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
	}
	if len(candidates[0].retryWindows) != 1 || candidates[0].retryWindows[0] != (entityWindowSpan{Start: 0, End: 20}) {
		t.Fatalf("retry windows = %#v, want failed span", candidates[0].retryWindows)
	}

	windows, retryOnly := entityWindows(candidates, 20, 5)
	if !retryOnly["a"] {
		t.Fatalf("retryOnly[a] = false, want true")
	}
	if len(windows) != 1 || windows[0].start != 0 || windows[0].end != 20 {
		t.Fatalf("windows = %#v, want only failed window", windows)
	}
}

func TestExtractEntityChipsDedupesAndLimits(t *testing.T) {
	chips := extractEntityChips([]any{
		map[string]any{"text": "Jane Doe", "label": "person"},
		map[string]any{"text": "Jane Doe", "label": "person"},
		map[string]any{"text": "Acme", "label": "organization"},
	}, 2)

	if len(chips) != 2 {
		t.Fatalf("len(chips) = %d, want 2", len(chips))
	}
	if chips[0] != (EntityChip{Text: "Jane Doe", Label: "person"}) {
		t.Fatalf("chips[0] = %#v", chips[0])
	}
	if chips[1] != (EntityChip{Text: "Acme", Label: "organization"}) {
		t.Fatalf("chips[1] = %#v", chips[1])
	}
}

func TestSplitEntityWindowsRebasesAndOverlaps(t *testing.T) {
	candidate := entityCandidate{
		id:      "page-1",
		content: "Alpha Beta Gamma Delta Epsilon Zeta Eta Theta",
	}
	windows := splitEntityWindows(candidate, 20, 5)
	if len(windows) < 2 {
		t.Fatalf("len(windows) = %d, want at least 2", len(windows))
	}
	if windows[0].id != "page-1" || windows[0].start != 0 {
		t.Fatalf("first window = %#v", windows[0])
	}
	if windows[1].start <= windows[0].start {
		t.Fatalf("second window start = %d, want > %d", windows[1].start, windows[0].start)
	}
	if !strings.Contains(windows[1].content, "Delta") {
		t.Fatalf("second window content = %q, want overlap around Delta", windows[1].content)
	}
}

func TestOffsetAndDedupeEntities(t *testing.T) {
	entities := []recognizedEntity{
		{Text: "Jane", Label: "person", Start: 2, End: 6, Score: 0.2},
		{Text: "Jane", Label: "person", Start: 2, End: 6, Score: 0.9},
	}

	rebased := offsetEntities(entities, 10)
	deduped := dedupeEntities(rebased)
	if len(deduped) != 1 {
		t.Fatalf("len(deduped) = %d, want 1", len(deduped))
	}
	if deduped[0].Start != 12 || deduped[0].End != 16 {
		t.Fatalf("deduped offsets = %d-%d, want 12-16", deduped[0].Start, deduped[0].End)
	}
	if deduped[0].Score != 0.9 {
		t.Fatalf("deduped score = %v, want highest score", deduped[0].Score)
	}
}

func TestDedupeRelationsKeepsHighestScore(t *testing.T) {
	head := recognizedEntity{Text: "Jane", Label: "person", Start: 12, End: 16}
	tail := recognizedEntity{Text: "Acme", Label: "organization", Start: 25, End: 29}
	relations := dedupeRelations([]recognizedRelation{
		{Head: head, Tail: tail, Label: "worked for", Score: 0.4},
		{Head: head, Tail: tail, Label: "worked for", Score: 0.8},
	})

	if len(relations) != 1 {
		t.Fatalf("len(relations) = %d, want 1", len(relations))
	}
	if relations[0].Score != 0.8 {
		t.Fatalf("deduped relation score = %v, want highest score", relations[0].Score)
	}
}

func TestClearEntityMetadataRemovesPriorRunFields(t *testing.T) {
	meta := map[string]any{
		"entities":              []any{"stale"},
		"entity_model":          "old",
		"entity_labels":         []string{"person"},
		"entity_window_chars":   500,
		"entity_overlap_chars":  50,
		"relations":             []any{"stale"},
		"relation_labels":       []string{"worked for"},
		"entity_error_windows":  2,
		"entity_failed_windows": []entityWindowSpan{{Start: 0, End: 10}},
		"unrelated_page_source": "kept",
	}

	clearEntityMetadata(meta)

	for _, key := range []string{
		"entities",
		"entity_model",
		"entity_labels",
		"entity_window_chars",
		"entity_overlap_chars",
		"relations",
		"relation_labels",
		"entity_error_windows",
		"entity_failed_windows",
	} {
		if _, ok := meta[key]; ok {
			t.Fatalf("metadata key %q was not cleared", key)
		}
	}
	if meta["unrelated_page_source"] != "kept" {
		t.Fatalf("unrelated metadata was changed: %#v", meta)
	}
}

func TestExtractOneZipPDF(t *testing.T) {
	pdfData := createMinimalPDF()
	zipPath := createTestZip(t, map[string][]byte{
		"subdir/test.pdf": pdfData,
	})

	// Open the zip and get the file entry
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if len(r.File) != 1 {
		t.Fatalf("expected 1 file in zip, got %d", len(r.File))
	}

	extractDir := t.TempDir()
	if err := extractOneZipPDF(r.File[0], extractDir); err != nil {
		t.Fatalf("extractOneZipPDF failed: %v", err)
	}

	// Verify file exists with flattened name (base name only)
	outPath := filepath.Join(extractDir, "test.pdf")
	outData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("output file not found: %v", err)
	}

	if !bytes.Equal(outData, pdfData) {
		t.Errorf("output content mismatch: got %d bytes, want %d bytes", len(outData), len(pdfData))
	}
}

func TestExtractZipPDFs_Parallel(t *testing.T) {
	pdfData := createMinimalPDF()

	// Create a zip with 50 PDFs
	files := make(map[string][]byte, 50)
	for i := range 50 {
		files[fmt.Sprintf("dir%d/doc_%03d.pdf", i%5, i)] = pdfData
	}
	zipPath := createTestZip(t, files)

	extractDir := t.TempDir()
	count, err := extractZipPDFs(zipPath, extractDir)
	if err != nil {
		t.Fatalf("extractZipPDFs failed: %v", err)
	}

	if count != 50 {
		t.Errorf("expected 50 extracted, got %d", count)
	}

	// Verify all files extracted with correct content
	entries, err := os.ReadDir(extractDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 50 {
		t.Errorf("expected 50 files in extract dir, got %d", len(entries))
	}

	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(extractDir, entry.Name()))
		if err != nil {
			t.Errorf("failed to read %s: %v", entry.Name(), err)
			continue
		}
		if !bytes.Equal(data, pdfData) {
			t.Errorf("content mismatch for %s: got %d bytes, want %d bytes",
				entry.Name(), len(data), len(pdfData))
		}
	}

	if _, err := parseSyncLevelFlag("aknn"); err == nil {
		t.Fatalf("parseSyncLevelFlag accepted removed aknn sync level")
	}
}

func TestIterateZipPDFs(t *testing.T) {
	pdfData := createMinimalPDF()
	txtData := []byte("not a pdf")
	jpgData := []byte("\xff\xd8\xff fake jpeg")

	zipPath := createTestZip(t, map[string][]byte{
		"docs/report.pdf":     pdfData,
		"docs/summary.PDF":    pdfData, // uppercase extension
		"docs/readme.txt":     txtData,
		"images/photo.jpg":    jpgData,
		"nested/a/b/deep.pdf": pdfData,
	})

	type result struct {
		name string
		data []byte
	}
	var results []result

	err := iterateZipPDFs(zipPath, func(name string, data []byte) error {
		results = append(results, result{name: name, data: data})
		return nil
	})
	if err != nil {
		t.Fatalf("iterateZipPDFs failed: %v", err)
	}

	// Should only yield PDF files
	if len(results) != 3 {
		t.Fatalf("expected 3 PDFs, got %d", len(results))
	}

	// Verify filenames are flattened (base name only)
	names := make(map[string]bool)
	for _, r := range results {
		names[r.name] = true
		if !bytes.Equal(r.data, pdfData) {
			t.Errorf("data mismatch for %s", r.name)
		}
		// Should not contain directory separators
		if strings.Contains(r.name, "/") {
			t.Errorf("expected flattened name, got %q", r.name)
		}
	}

	if !names["report.pdf"] {
		t.Error("missing report.pdf")
	}
	if !names["summary.PDF"] {
		t.Error("missing summary.PDF")
	}
	if !names["deep.pdf"] {
		t.Error("missing deep.pdf")
	}
}

func TestIterateZipPDFs_Empty(t *testing.T) {
	zipPath := createTestZip(t, map[string][]byte{
		"readme.txt": []byte("hello"),
		"data.csv":   []byte("a,b,c"),
	})

	called := false
	err := iterateZipPDFs(zipPath, func(name string, data []byte) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("iterateZipPDFs failed: %v", err)
	}
	if called {
		t.Error("callback should not have been called for zip with no PDFs")
	}
}

func TestSplitPDFToPagesFromBytes(t *testing.T) {
	pdfData := createMultiPagePDF(3)

	// Verify the test PDF is valid and has 3 pages
	conf := model.NewDefaultConfiguration()
	pc, err := api.PageCount(bytes.NewReader(pdfData), conf)
	if err != nil {
		t.Fatalf("test PDF invalid: %v", err)
	}
	if pc != 3 {
		t.Fatalf("test PDF has %d pages, expected 3", pc)
	}

	pageBytes, metadata, err := splitPDFToPagesFromBytes(pdfData, "test-doc.pdf")
	if err != nil {
		t.Fatalf("splitPDFToPagesFromBytes failed: %v", err)
	}

	// Verify correct number of pages
	if len(pageBytes) != 3 {
		t.Errorf("expected 3 pages, got %d", len(pageBytes))
	}

	// Verify each page is a valid single-page PDF
	for pageNum, data := range pageBytes {
		pc, err := api.PageCount(bytes.NewReader(data), conf)
		if err != nil {
			t.Errorf("page %d is not a valid PDF: %v", pageNum, err)
			continue
		}
		if pc != 1 {
			t.Errorf("page %d has %d pages, expected 1", pageNum, pc)
		}
	}

	// Verify metadata
	if metadata == nil {
		t.Fatal("metadata is nil")
	}
	if metadata.TotalPages != 3 {
		t.Errorf("metadata.TotalPages = %d, want 3", metadata.TotalPages)
	}
	if metadata.SourceFile != "test-doc.pdf" {
		t.Errorf("metadata.SourceFile = %q, want %q", metadata.SourceFile, "test-doc.pdf")
	}
}

func TestIdentifyEnrichCandidates(t *testing.T) {
	tests := []struct {
		name         string
		records      map[string]map[string]any
		minContent   int
		reprocess    bool
		wantIDs      []string            // expected candidate IDs (order-independent)
		wantReason   map[string][]string // id → expected reasons
		wantCategory map[string]string   // id → expected category
	}{
		{
			name: "short content triggers short_content with ocr category",
			records: map[string]map[string]any{
				"page-1": {
					"content": "hi",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"page-1"},
			wantReason: map[string][]string{
				"page-1": {"short_content"},
			},
			wantCategory: map[string]string{
				"page-1": "ocr",
			},
		},
		{
			name: "empty content triggers empty with ocr category",
			records: map[string]map[string]any{
				"page-1": {
					"content": "",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"page-1"},
			wantReason: map[string][]string{
				"page-1": {"empty"},
			},
			wantCategory: map[string]string{
				"page-1": "ocr",
			},
		},
		{
			name: "empty content with page_type=image triggers vision category",
			records: map[string]map[string]any{
				"page-1": {
					"content": "",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
						"page_type":     "image",
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"page-1"},
			wantReason: map[string][]string{
				"page-1": {"image_page"},
			},
			wantCategory: map[string]string{
				"page-1": "vision",
			},
		},
		{
			name: "good content not flagged",
			records: map[string]map[string]any{
				"page-1": {
					"content": "This is a perfectly normal paragraph of text that contains enough characters to pass the minimum content length check and has no quality issues whatsoever.",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
					},
				},
			},
			minContent: 50,
			wantIDs:    nil,
		},
		{
			name: "source_file + page_number works without page_pdf_path",
			records: map[string]map[string]any{
				"page-1": {
					"content": "short",
					"metadata": map[string]any{
						"source_file": "test.pdf",
						"page_number": float64(1),
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"page-1"},
			wantReason: map[string][]string{
				"page-1": {"short_content"},
			},
			wantCategory: map[string]string{
				"page-1": "ocr",
			},
		},
		{
			name: "no page source skipped",
			records: map[string]map[string]any{
				"page-1": {
					"content":  "short",
					"metadata": map[string]any{},
				},
			},
			minContent: 50,
			wantIDs:    nil,
		},
		{
			name: "no metadata skipped",
			records: map[string]map[string]any{
				"page-1": {
					"content": "short",
				},
			},
			minContent: 50,
			wantIDs:    nil,
		},
		{
			name: "already enriched skipped by default",
			records: map[string]map[string]any{
				"page-1": {
					"content": "x",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
						"enriched":      true,
					},
				},
			},
			minContent: 50,
			reprocess:  false,
			wantIDs:    nil,
		},
		{
			name: "already enriched reprocessed when flag set",
			records: map[string]map[string]any{
				"page-1": {
					"content": "x",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
						"enriched":      true,
					},
				},
			},
			minContent: 50,
			reprocess:  true,
			wantIDs:    []string{"page-1"},
			wantCategory: map[string]string{
				"page-1": "ocr",
			},
		},
		{
			name: "high symbol ratio flagged with quality category",
			records: map[string]map[string]any{
				"page-1": {
					// Content with lots of unusual symbols but long enough to pass minContent
					"content": strings.Repeat("§±†‡™®©℗℠", 20),
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/page1.pdf",
					},
				},
			},
			minContent: 10,
			wantIDs:    []string{"page-1"},
			wantCategory: map[string]string{
				"page-1": "quality",
			},
		},
		{
			name: "multiple records mixed",
			records: map[string]map[string]any{
				"good": {
					"content": "This document contains a thorough discussion of the legal proceedings that took place in the Southern District of Florida during the year two thousand and eight.",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/good.pdf",
					},
				},
				"bad": {
					"content": "",
					"metadata": map[string]any{
						"page_pdf_path": "/tmp/bad.pdf",
					},
				},
			},
			minContent: 50,
			wantIDs:    []string{"bad"},
			wantCategory: map[string]string{
				"bad": "ocr",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates := identifyEnrichCandidates(tt.records, tt.minContent, tt.reprocess)

			gotIDs := make(map[string]bool)
			gotReasons := make(map[string][]string)
			gotCategories := make(map[string]string)
			for _, c := range candidates {
				gotIDs[c.id] = true
				gotReasons[c.id] = c.reasons
				gotCategories[c.id] = c.category
			}

			wantIDSet := make(map[string]bool)
			for _, id := range tt.wantIDs {
				wantIDSet[id] = true
			}

			if len(gotIDs) != len(wantIDSet) {
				t.Errorf("got %d candidates, want %d", len(gotIDs), len(wantIDSet))
			}

			for id := range wantIDSet {
				if !gotIDs[id] {
					t.Errorf("missing expected candidate %q", id)
				}
			}
			for id := range gotIDs {
				if !wantIDSet[id] {
					t.Errorf("unexpected candidate %q", id)
				}
			}

			// Check expected reasons if specified
			for id, wantReasons := range tt.wantReason {
				got := gotReasons[id]
				for _, wr := range wantReasons {
					found := false
					for _, gr := range got {
						if gr == wr {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("candidate %q: missing expected reason %q (got %v)", id, wr, got)
					}
				}
			}

			// Check expected categories if specified
			for id, wantCat := range tt.wantCategory {
				if gotCat := gotCategories[id]; gotCat != wantCat {
					t.Errorf("candidate %q: got category %q, want %q", id, gotCat, wantCat)
				}
			}
		})
	}
}

func TestIdentifyEnrichCandidates_FragmentedLayout(t *testing.T) {
	// Build content that triggers isFragmentedLayout: >60% of lines shorter than 20 chars,
	// with at least 10 lines
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, fmt.Sprintf("line %d", i))
	}
	fragmented := strings.Join(lines, "\n")

	records := map[string]map[string]any{
		"frag": {
			"content": fragmented,
			"metadata": map[string]any{
				"page_pdf_path": "/tmp/frag.pdf",
			},
		},
	}

	candidates := identifyEnrichCandidates(records, 10, false)
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}

	found := false
	for _, r := range candidates[0].reasons {
		if r == "fragmented" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'fragmented' reason, got %v", candidates[0].reasons)
	}
	if candidates[0].category != "quality" {
		t.Errorf("expected 'quality' category, got %q", candidates[0].category)
	}
}

func TestSplitPDFToPagesFromBytes_SinglePage(t *testing.T) {
	pdfData := createMinimalPDF()

	// Verify the test PDF is valid
	conf := model.NewDefaultConfiguration()
	pc, err := api.PageCount(bytes.NewReader(pdfData), conf)
	if err != nil {
		t.Fatalf("test PDF invalid: %v", err)
	}
	if pc != 1 {
		t.Fatalf("test PDF has %d pages, expected 1", pc)
	}

	pageBytes, metadata, err := splitPDFToPagesFromBytes(pdfData, "single.pdf")
	if err != nil {
		t.Fatalf("splitPDFToPagesFromBytes failed: %v", err)
	}

	// Should return exactly 1 page
	if len(pageBytes) != 1 {
		t.Fatalf("expected 1 page, got %d", len(pageBytes))
	}

	// Single-page optimization: should return original bytes directly
	if !bytes.Equal(pageBytes[1], pdfData) {
		t.Error("single-page PDF should return original bytes (skip split optimization)")
	}

	if metadata.TotalPages != 1 {
		t.Errorf("metadata.TotalPages = %d, want 1", metadata.TotalPages)
	}
}
