package main

import (
	"encoding/json"
	"strings"
	"testing"

	antfly "github.com/antflydb/antfly/go/pkg/sdk"
)

// marshalIndexConfig round-trips an IndexConfig through JSON so tests assert
// against the exact wire shape the server admits.
func marshalIndexConfig(t *testing.T, idx *antfly.IndexConfig) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("marshal index config: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal index config: %v", err)
	}
	return got
}

func decodeProducerJSON(t *testing.T, enrichment map[string]any) map[string]any {
	t.Helper()
	raw, ok := enrichment["producer_json"].(string)
	if !ok {
		t.Fatalf("producer_json missing or not a string: %#v", enrichment)
	}
	var producer map[string]any
	if err := json.Unmarshal([]byte(raw), &producer); err != nil {
		t.Fatalf("producer_json is not valid JSON: %v\n%s", err, raw)
	}
	return producer
}

func producerToolParameters(t *testing.T, producer map[string]any, wantTool string) map[string]any {
	t.Helper()
	if producer["type"] != "generator" {
		t.Fatalf("producer type = %v, want generator", producer["type"])
	}
	cfg, ok := producer["config"].(map[string]any)
	if !ok {
		t.Fatalf("producer config missing: %#v", producer)
	}
	if cfg["provider"] != "antfly" {
		t.Fatalf("provider = %v, want antfly", cfg["provider"])
	}
	if cfg["tool_output"] != "arguments" || cfg["tool_name"] != wantTool {
		t.Fatalf("unexpected tool output config: %#v", cfg)
	}
	// Instructions live in the enrichment source template, never here: the
	// canonical generator contract rejects a `prompt` config field.
	if cfg["prompt"] != nil {
		t.Fatalf("producer config must not carry a prompt field: %#v", cfg)
	}
	choice, ok := cfg["tool_choice"].(map[string]any)
	if !ok {
		t.Fatalf("tool_choice missing: %#v", cfg)
	}
	choiceFn, _ := choice["function"].(map[string]any)
	if choiceFn == nil || choiceFn["name"] != wantTool {
		t.Fatalf("tool_choice not pinned to %s: %#v", wantTool, choice)
	}
	tools, ok := cfg["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools must contain exactly one function: %#v", cfg["tools"])
	}
	fn, _ := tools[0].(map[string]any)["function"].(map[string]any)
	if fn == nil || fn["name"] != wantTool {
		t.Fatalf("tool function not named %s: %#v", wantTool, tools[0])
	}
	params, ok := fn["parameters"].(map[string]any)
	if !ok {
		t.Fatalf("tool parameters missing: %#v", fn)
	}
	if params["additionalProperties"] != false {
		t.Fatalf("tool parameters must set additionalProperties:false: %#v", params)
	}
	return params
}

func schemaItems(t *testing.T, params map[string]any, field string) map[string]any {
	t.Helper()
	props, _ := params["properties"].(map[string]any)
	arr, _ := props[field].(map[string]any)
	items, _ := arr["items"].(map[string]any)
	if items == nil {
		t.Fatalf("%s items missing from tool schema: %#v", field, params)
	}
	return items
}

func itemPropertyEnum(t *testing.T, items map[string]any, property string) []any {
	t.Helper()
	props, _ := items["properties"].(map[string]any)
	prop, _ := props[property].(map[string]any)
	if prop == nil {
		t.Fatalf("property %s missing: %#v", property, items)
	}
	enum, _ := prop["enum"].([]any)
	return enum
}

func TestCreateAutoschemaKnowledgeGraphIndexConfig(t *testing.T) {
	// The GLiNER lane is disabled here so this test pins the paper's
	// two-stage LLM shape; the lane has its own test below.
	idx, err := createAutoschemaKnowledgeGraphIndex(DefaultAutoschemaModel, "", DefaultInferenceURL)
	if err != nil {
		t.Fatalf("createAutoschemaKnowledgeGraphIndex failed: %v", err)
	}
	got := marshalIndexConfig(t, idx)

	if got["name"] != AutoschemaKnowledgeGraphIndex || got["type"] != "graph" {
		t.Fatalf("unexpected index identity: name=%v type=%v", got["name"], got["type"])
	}
	if _, exists := got["source"]; exists {
		t.Fatalf("multi-artifact index must use sources, not source: %#v", got)
	}
	if _, exists := got["artifact"]; exists {
		t.Fatalf("multi-artifact index must declare producers via enrichments, not the artifact shorthand: %#v", got)
	}

	// Stage 1: two generator asset enrichments, one per extraction pass.
	enrichments, ok := got["enrichments"].([]any)
	if !ok || len(enrichments) != 2 {
		t.Fatalf("enrichments = %#v, want 2 entries", got["enrichments"])
	}
	wantAssets := []string{AutoschemaEntityEntityAsset, AutoschemaEventsAsset}
	byName := map[string]map[string]any{}
	for i, raw := range enrichments {
		enrichment, _ := raw.(map[string]any)
		if enrichment["name"] != wantAssets[i] {
			t.Fatalf("enrichments[%d] = %v, want %s", i, enrichment["name"], wantAssets[i])
		}
		if enrichment["kind"] != "asset" || enrichment["content_type"] != "application/json" {
			t.Fatalf("unexpected enrichment declaration: %#v", enrichment)
		}
		// The rendered template IS the generator prompt: stage instructions
		// followed by the document content. The producer config must NOT
		// carry a `prompt` key (the canonical generator contract rejects
		// unknown fields with UnknownField at enrichment time).
		template, _ := enrichment["template"].(string)
		if !strings.Contains(template, "{{ content }}") || len(template) < 200 {
			t.Fatalf("enrichment template must embed instructions and {{ content }}: %q", template)
		}
		byName[enrichment["name"].(string)] = enrichment
	}

	// Per-pass tool schema guardrails.
	eeParams := producerToolParameters(t, decodeProducerJSON(t, byName[AutoschemaEntityEntityAsset]), autoschemaExtractionToolName)
	if enum := itemPropertyEnum(t, schemaItems(t, eeParams, "entities"), "label"); len(enum) != 0 {
		t.Fatalf("kg_ee_v1 entity labels must be open-vocabulary, got enum %#v", enum)
	}
	if enum := itemPropertyEnum(t, schemaItems(t, eeParams, "relations"), "type"); len(enum) != 0 {
		t.Fatalf("kg_ee_v1 relation types must be open-vocabulary, got enum %#v", enum)
	}

	eventsParams := producerToolParameters(t, decodeProducerJSON(t, byName[AutoschemaEventsAsset]), autoschemaExtractionToolName)
	if enum := itemPropertyEnum(t, schemaItems(t, eventsParams, "entities"), "label"); len(enum) != 0 {
		t.Fatalf("kg_events_v1 entity labels must stay open for participants, got enum %#v", enum)
	}
	wantEventRelations := []any{"participates_in", "before", "after", "concurrent", "because", "as_result"}
	if enum := itemPropertyEnum(t, schemaItems(t, eventsParams, "relations"), "type"); len(enum) != len(wantEventRelations) {
		t.Fatalf("kg_events_v1 relation types = %#v, want %#v", enum, wantEventRelations)
	}
	// The prompts ask for a predicate lemma; the tool schema must ADMIT the
	// field or structured-output enforcement silently drops it and every
	// event identity degrades to the sentence text (two wordings of the same
	// event then mint two nodes).
	for _, pass := range []struct {
		name   string
		params map[string]any
	}{{AutoschemaEntityEntityAsset, eeParams}, {AutoschemaEventsAsset, eventsParams}} {
		items := schemaItems(t, pass.params, "entities")
		properties, _ := items["properties"].(map[string]any)
		if _, ok := properties["predicate"]; !ok {
			t.Fatalf("%s entity item schema must admit the predicate field: %#v", pass.name, properties)
		}
	}

	// Stage 2: two ordered extraction_graph sources.
	sources, ok := got["sources"].([]any)
	if !ok || len(sources) != 2 {
		t.Fatalf("sources = %#v, want 2 entries", got["sources"])
	}
	for i, raw := range sources {
		source, _ := raw.(map[string]any)
		if source["artifact"] != wantAssets[i] || source["format"] != "extraction_graph" || source["mention_edge_type"] != "mentions" {
			t.Fatalf("unexpected sources[%d]: %#v", i, source)
		}
	}

	// Label-routed resolvers: the ee catch-all plus the events/catch-all
	// pair on the merged events artifact.
	resolvers, ok := got["resolvers"].([]any)
	if !ok || len(resolvers) != 3 {
		t.Fatalf("resolvers = %#v, want 3 entries", got["resolvers"])
	}
	seenResolution := map[string]bool{}
	for _, raw := range resolvers {
		resolver, _ := raw.(map[string]any)
		labels, _ := resolver["labels"].([]any)
		resolution, _ := resolver["resolution_artifact"].(string)
		if resolution == "" || seenResolution[resolution] {
			t.Fatalf("resolution artifacts must be unique and non-empty: %#v", resolver)
		}
		seenResolution[resolution] = true
		switch resolver["table"] {
		case AutoschemaEventsTable:
			if len(labels) != 1 || labels[0] != "event" {
				t.Fatalf("events resolver must claim only the event label: %#v", resolver)
			}
			if resolver["key_template"] != "event/{{ hash _entity.event_identity }}" {
				t.Fatalf("events resolver key template = %v", resolver["key_template"])
			}
		case AutoschemaEntitiesTable:
			if len(labels) != 0 {
				t.Fatalf("entities resolver must be a catch-all: %#v", resolver)
			}
			if resolver["key_template"] != "entity/{{ slug _entity.text }}" {
				t.Fatalf("entities resolver key template = %v", resolver["key_template"])
			}
			if resolver["candidate_search"] != "prefix" {
				t.Fatalf("entities resolver candidate search = %v, want prefix", resolver["candidate_search"])
			}
		default:
			t.Fatalf("unexpected resolver table: %#v", resolver)
		}
	}

	edgeTypes, ok := got["edge_types"].([]any)
	if !ok {
		t.Fatalf("edge_types missing: %#v", got)
	}
	wantEdges := map[string]bool{
		"mentions": false, "participates_in": false, "before": false,
		"after": false, "concurrent": false, "because": false, "as_result": false,
	}
	for _, raw := range edgeTypes {
		edge, _ := raw.(map[string]any)
		name, _ := edge["name"].(string)
		if _, ok := wantEdges[name]; !ok {
			t.Fatalf("unexpected edge type %q", name)
		}
		wantEdges[name] = true
	}
	for name, seen := range wantEdges {
		if !seen {
			t.Fatalf("edge type %q missing from %#v", name, edgeTypes)
		}
	}

	// The request builder enforces the sources/source exclusivity contract.
	if _, err := antfly.NewCreateIndexRequest(idx); err != nil {
		t.Fatalf("NewCreateIndexRequest rejected knowledge graph config: %v", err)
	}
}

func TestCreateAutoschemaKnowledgeGraphIndexWithGlinerLane(t *testing.T) {
	idx, err := createAutoschemaKnowledgeGraphIndex(DefaultAutoschemaModel, DefaultAutoschemaGlinerModel, DefaultInferenceURL)
	if err != nil {
		t.Fatalf("createAutoschemaKnowledgeGraphIndex failed: %v", err)
	}
	got := marshalIndexConfig(t, idx)

	// A third extraction_relation source with positional GLiNER endpoints,
	// after the two LLM extraction_graph sources.
	sources, ok := got["sources"].([]any)
	if !ok || len(sources) != 3 {
		t.Fatalf("sources = %#v, want 3 entries", got["sources"])
	}
	gliner, _ := sources[2].(map[string]any)
	if gliner["artifact"] != AutoschemaGlinerAsset || gliner["format"] != "extraction_relation" ||
		gliner["path"] != "$.relations[*]" || gliner["mention_edge_type"] != "mentions" {
		t.Fatalf("unexpected gliner source: %#v", gliner)
	}
	edge, _ := gliner["edge"].(map[string]any)
	if edge == nil || edge["type"] != "{{ _item.type }}" || edge["weight"] != "{{ _item.score }}" {
		t.Fatalf("gliner edge mapping must map type/weight from the relation item: %#v", gliner)
	}

	// The lane's extractor enrichment reads the raw content field (no prompt
	// template: GLiNER consumes the text directly).
	enrichments, ok := got["enrichments"].([]any)
	if !ok || len(enrichments) != 3 {
		t.Fatalf("enrichments = %#v, want 3 entries", got["enrichments"])
	}
	glinerEnrichment, _ := enrichments[2].(map[string]any)
	if glinerEnrichment["name"] != AutoschemaGlinerAsset || glinerEnrichment["kind"] != "asset" ||
		glinerEnrichment["field"] != "content" || glinerEnrichment["content_type"] != "application/json" {
		t.Fatalf("unexpected gliner enrichment: %#v", glinerEnrichment)
	}
	producer := decodeProducerJSON(t, glinerEnrichment)
	if producer["type"] != "extractor" {
		t.Fatalf("gliner producer must be an extractor: %#v", producer)
	}
	config, _ := producer["config"].(map[string]any)
	if config["model"] != DefaultAutoschemaGlinerModel {
		t.Fatalf("gliner producer model = %v", config["model"])
	}
	options, _ := config["options"].(map[string]any)
	longDoc, _ := options["long_document"].(map[string]any)
	if longDoc == nil || longDoc["mode"] != "window" {
		t.Fatalf("gliner producer must request windowed long-document execution: %#v", config)
	}

	// A fourth resolver over the GLiNER artifact sharing the entity key
	// template, so both lanes converge on the same canonical entities.
	resolvers, ok := got["resolvers"].([]any)
	if !ok || len(resolvers) != 4 {
		t.Fatalf("resolvers = %#v, want 4 entries", got["resolvers"])
	}
	glinerResolver, _ := resolvers[3].(map[string]any)
	if glinerResolver["source_artifact"] != AutoschemaGlinerAsset ||
		glinerResolver["table"] != AutoschemaEntitiesTable ||
		glinerResolver["resolution_artifact"] != "entities_gliner_resolution_v1" ||
		glinerResolver["key_template"] != "entity/{{ slug _entity.text }}" {
		t.Fatalf("unexpected gliner resolver: %#v", glinerResolver)
	}
	if labels, _ := glinerResolver["labels"].([]any); len(labels) != 0 {
		t.Fatalf("gliner resolver must be a catch-all: %#v", glinerResolver)
	}

	if _, err := antfly.NewCreateIndexRequest(idx); err != nil {
		t.Fatalf("NewCreateIndexRequest rejected gliner-enabled knowledge graph config: %v", err)
	}

	// The required-enrichment list ensureAutoschemaIndexes verifies must
	// include the lane exactly when it is enabled.
	if got := autoschemaRequiredEnrichments(DefaultAutoschemaGlinerModel); len(got) != 3 || got[2] != AutoschemaGlinerAsset {
		t.Fatalf("autoschemaRequiredEnrichments(gliner) = %v", got)
	}
	if got := autoschemaRequiredEnrichments(""); len(got) != 2 {
		t.Fatalf("autoschemaRequiredEnrichments(\"\") = %v", got)
	}
}

func TestCreateAutoschemaTaxonomyIndexConfig(t *testing.T) {
	idx, err := createAutoschemaTaxonomyIndex(DefaultAutoschemaModel, DefaultInferenceURL)
	if err != nil {
		t.Fatalf("createAutoschemaTaxonomyIndex failed: %v", err)
	}
	got := marshalIndexConfig(t, idx)

	if got["name"] != AutoschemaTaxonomyIndex || got["type"] != "graph" {
		t.Fatalf("unexpected index identity: name=%v type=%v", got["name"], got["type"])
	}

	enrichments, ok := got["enrichments"].([]any)
	if !ok || len(enrichments) != 1 {
		t.Fatalf("enrichments = %#v, want 1 entry", got["enrichments"])
	}
	enrichment, _ := enrichments[0].(map[string]any)
	if enrichment["name"] != AutoschemaConceptAsset || enrichment["kind"] != "asset" {
		t.Fatalf("unexpected conceptualizer enrichment: %#v", enrichment)
	}
	template, _ := enrichment["template"].(string)
	if template == "" {
		t.Fatalf("conceptualizer must read promoted entity fields via template: %#v", enrichment)
	}

	// Neighbor context must reference a graph index on the same table:
	// taxonomy itself is the only admissible choice on entities.
	neighborContext, _ := enrichment["neighbor_context"].(map[string]any)
	if neighborContext == nil || neighborContext["graph_index"] != AutoschemaTaxonomyIndex {
		t.Fatalf("neighbor_context must reference the taxonomy index: %#v", enrichment)
	}
	if neighborContext["direction"] != "out" || neighborContext["limit"] != float64(8) {
		t.Fatalf("unexpected neighbor_context tuning: %#v", neighborContext)
	}

	params := producerToolParameters(t, decodeProducerJSON(t, enrichment), autoschemaConceptToolName)
	conceptItems := schemaItems(t, params, "entities")
	props, _ := params["properties"].(map[string]any)
	conceptsArr, _ := props["entities"].(map[string]any)
	if conceptsArr["minItems"] != float64(3) {
		t.Fatalf("conceptualizer must require >=3 concepts: %#v", conceptsArr)
	}
	if enum := itemPropertyEnum(t, conceptItems, "label"); len(enum) != 1 || enum[0] != "concept" {
		t.Fatalf("concept labels = %#v, want [concept]", enum)
	}
	if enum := itemPropertyEnum(t, schemaItems(t, params, "relations"), "type"); len(enum) != 1 || enum[0] != "is_a" {
		t.Fatalf("concept relation types = %#v, want [is_a]", enum)
	}

	sources, ok := got["sources"].([]any)
	if !ok || len(sources) != 1 {
		t.Fatalf("sources = %#v, want 1 entry", got["sources"])
	}
	source, _ := sources[0].(map[string]any)
	if source["artifact"] != AutoschemaConceptAsset || source["format"] != "extraction_graph" {
		t.Fatalf("unexpected taxonomy source: %#v", source)
	}

	resolvers, ok := got["resolvers"].([]any)
	if !ok || len(resolvers) != 1 {
		t.Fatalf("resolvers = %#v, want 1 entry", got["resolvers"])
	}
	resolver, _ := resolvers[0].(map[string]any)
	labels, _ := resolver["labels"].([]any)
	if resolver["table"] != AutoschemaConceptsTable || len(labels) != 1 || labels[0] != "concept" {
		t.Fatalf("unexpected concepts resolver: %#v", resolver)
	}
	if resolver["key_template"] != "concept/{{ slug _entity.text }}" {
		t.Fatalf("concepts key template = %v", resolver["key_template"])
	}

	if _, err := antfly.NewCreateIndexRequest(idx); err != nil {
		t.Fatalf("NewCreateIndexRequest rejected taxonomy config: %v", err)
	}
}

func decodeIndexStatus(t *testing.T, raw string) antfly.IndexStatus {
	t.Helper()
	var status antfly.IndexStatus
	if err := json.Unmarshal([]byte(raw), &status); err != nil {
		t.Fatalf("unmarshal index status: %v", err)
	}
	return status
}

func TestVerifyAutoschemaGraphIndexEnrichments(t *testing.T) {
	// A pre-existing taxonomy index admitted with its conceptualizer
	// enrichment passes verification.
	complete := decodeIndexStatus(t,
		`{"config":{"type":"graph","name":"taxonomy","enrichments":[{"name":"conceptualize_v1","kind":"asset"}]}}`)
	if err := verifyAutoschemaGraphIndexEnrichments(complete, AutoschemaEntitiesTable, AutoschemaTaxonomyIndex, []string{AutoschemaConceptAsset}); err != nil {
		t.Fatalf("complete taxonomy index rejected: %v", err)
	}

	// A pre-existing index admitted without the conceptualizer is a hard,
	// actionable error naming the missing enrichment, never a warning.
	partial := decodeIndexStatus(t,
		`{"config":{"type":"graph","name":"taxonomy"}}`)
	err := verifyAutoschemaGraphIndexEnrichments(partial, AutoschemaEntitiesTable, AutoschemaTaxonomyIndex, []string{AutoschemaConceptAsset})
	if err == nil || !strings.Contains(err.Error(), AutoschemaConceptAsset) || !strings.Contains(err.Error(), "pre-exists") {
		t.Fatalf("partial taxonomy index: err = %v, want missing-enrichment error naming %s", err, AutoschemaConceptAsset)
	}

	// A name-collided index of a different type is reported as a
	// configuration mismatch on the pre-existing table.
	wrongType := decodeIndexStatus(t,
		`{"config":{"type":"full_text","name":"taxonomy"}}`)
	err = verifyAutoschemaGraphIndexEnrichments(wrongType, AutoschemaEntitiesTable, AutoschemaTaxonomyIndex, []string{AutoschemaConceptAsset})
	if err == nil || !strings.Contains(err.Error(), "not the expected graph index") {
		t.Fatalf("wrong-type index: err = %v, want graph-index mismatch error", err)
	}
}

func TestVerifyAutoschemaGraphIndexConfigDetectsDrift(t *testing.T) {
	idx, err := createAutoschemaTaxonomyIndex(DefaultAutoschemaModel, DefaultInferenceURL)
	if err != nil {
		t.Fatalf("createAutoschemaTaxonomyIndex failed: %v", err)
	}
	request, err := antfly.NewCreateIndexRequest(idx)
	if err != nil {
		t.Fatalf("NewCreateIndexRequest failed: %v", err)
	}

	// Identical want and got must verify: the created shape mirrors the
	// request's graph config sections after a JSON round-trip.
	same := statusFromCreateRequest(t, *request)
	if err := verifyAutoschemaGraphIndexConfig(same, "entities", AutoschemaTaxonomyIndex, *request); err != nil {
		t.Fatalf("identical config must verify, got: %v", err)
	}

	// A drifted resolver key template must fail with a named path even
	// though the index type and enrichment names all match.
	drifted := *idx
	driftedCfg := drifted
	driftedRequest, err := antfly.NewCreateIndexRequest(&driftedCfg)
	if err != nil {
		t.Fatalf("NewCreateIndexRequest(drift) failed: %v", err)
	}
	got := statusFromCreateRequest(t, *driftedRequest)
	mutateStatusJSON(t, &got, func(m map[string]any) {
		resolvers := m["resolvers"].([]any)
		resolvers[0].(map[string]any)["key_template"] = "{{ _entity.text }}"
	})
	err = verifyAutoschemaGraphIndexConfig(got, "entities", AutoschemaTaxonomyIndex, *request)
	if err == nil || !strings.Contains(err.Error(), "resolvers") {
		t.Fatalf("drifted resolver template must fail verification naming resolvers, got: %v", err)
	}

	// A drifted enrichment template (the conceptualizer prompt) must fail.
	got = statusFromCreateRequest(t, *request)
	mutateStatusJSON(t, &got, func(m map[string]any) {
		enrichments := m["enrichments"].([]any)
		enrichments[0].(map[string]any)["template"] = "{{ canonical_name }}"
	})
	err = verifyAutoschemaGraphIndexConfig(got, "entities", AutoschemaTaxonomyIndex, *request)
	if err == nil || !strings.Contains(err.Error(), "enrichment configuration") {
		t.Fatalf("drifted enrichment template must fail verification, got: %v", err)
	}

	// A missing or drifted conceptualizer neighbor_context must fail: the
	// prompt alone does not prove the grounding configuration.
	got = statusFromCreateRequest(t, *request)
	mutateStatusJSON(t, &got, func(m map[string]any) {
		enrichments := m["enrichments"].([]any)
		delete(enrichments[0].(map[string]any), "neighbor_context")
	})
	err = verifyAutoschemaGraphIndexConfig(got, "entities", AutoschemaTaxonomyIndex, *request)
	if err == nil || !strings.Contains(err.Error(), "neighbor_context") {
		t.Fatalf("missing neighbor_context must fail verification, got: %v", err)
	}

	got = statusFromCreateRequest(t, *request)
	mutateStatusJSON(t, &got, func(m map[string]any) {
		enrichments := m["enrichments"].([]any)
		nc := enrichments[0].(map[string]any)["neighbor_context"].(map[string]any)
		nc["graph_index"] = "wrong_index"
	})
	err = verifyAutoschemaGraphIndexConfig(got, "entities", AutoschemaTaxonomyIndex, *request)
	if err == nil || !strings.Contains(err.Error(), "neighbor_context") {
		t.Fatalf("drifted neighbor_context must fail verification, got: %v", err)
	}
}

// statusFromCreateRequest round-trips a create request into the created
// status shape the server would return for it.
func statusFromCreateRequest(t *testing.T, request antfly.CreateIndexRequest) antfly.IndexStatus {
	t.Helper()
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	var status antfly.IndexStatus
	if err := json.Unmarshal([]byte(`{"config":`+string(encoded)+`}`), &status); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	return status
}

func mutateStatusJSON(t *testing.T, status *antfly.IndexStatus, mutate func(map[string]any)) {
	t.Helper()
	encoded, err := json.Marshal(status.Config)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(encoded, &m); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	mutate(m)
	mutated, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal mutated: %v", err)
	}
	if err := json.Unmarshal(mutated, &status.Config); err != nil {
		t.Fatalf("unmarshal mutated config: %v", err)
	}
}
