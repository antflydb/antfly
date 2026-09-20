package main

import (
	"encoding/json"
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
	prompt, _ := cfg["prompt"].(string)
	if prompt == "" {
		t.Fatalf("prompt missing from producer config: %#v", cfg)
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
	idx, err := createAutoschemaKnowledgeGraphIndex(DefaultAutoschemaModel, DefaultInferenceURL)
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

	// Stage 1: three generator asset enrichments, one per extraction pass.
	enrichments, ok := got["enrichments"].([]any)
	if !ok || len(enrichments) != 3 {
		t.Fatalf("enrichments = %#v, want 3 entries", got["enrichments"])
	}
	wantAssets := []string{AutoschemaEntityEntityAsset, AutoschemaEntityEventAsset, AutoschemaEventEventAsset}
	byName := map[string]map[string]any{}
	for i, raw := range enrichments {
		enrichment, _ := raw.(map[string]any)
		if enrichment["name"] != wantAssets[i] {
			t.Fatalf("enrichments[%d] = %v, want %s", i, enrichment["name"], wantAssets[i])
		}
		if enrichment["kind"] != "asset" || enrichment["field"] != "content" || enrichment["content_type"] != "application/json" {
			t.Fatalf("unexpected enrichment declaration: %#v", enrichment)
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

	evParams := producerToolParameters(t, decodeProducerJSON(t, byName[AutoschemaEntityEventAsset]), autoschemaExtractionToolName)
	if enum := itemPropertyEnum(t, schemaItems(t, evParams, "relations"), "type"); len(enum) != 1 || enum[0] != "participates_in" {
		t.Fatalf("kg_ev_v1 relation types = %#v, want [participates_in]", enum)
	}

	vvParams := producerToolParameters(t, decodeProducerJSON(t, byName[AutoschemaEventEventAsset]), autoschemaExtractionToolName)
	if enum := itemPropertyEnum(t, schemaItems(t, vvParams, "entities"), "label"); len(enum) != 1 || enum[0] != "event" {
		t.Fatalf("kg_vv_v1 entity labels = %#v, want [event]", enum)
	}
	wantEventRelations := []any{"before", "after", "concurrent", "because", "as_result"}
	if enum := itemPropertyEnum(t, schemaItems(t, vvParams, "relations"), "type"); len(enum) != len(wantEventRelations) {
		t.Fatalf("kg_vv_v1 relation types = %#v, want %#v", enum, wantEventRelations)
	}

	// Stage 2: three ordered extraction_graph sources.
	sources, ok := got["sources"].([]any)
	if !ok || len(sources) != 3 {
		t.Fatalf("sources = %#v, want 3 entries", got["sources"])
	}
	for i, raw := range sources {
		source, _ := raw.(map[string]any)
		if source["artifact"] != wantAssets[i] || source["format"] != "extraction_graph" || source["mention_edge_type"] != "mentions" {
			t.Fatalf("unexpected sources[%d]: %#v", i, source)
		}
	}

	// Label-routed resolvers, one events/catch-all pair per mention class.
	resolvers, ok := got["resolvers"].([]any)
	if !ok || len(resolvers) != 4 {
		t.Fatalf("resolvers = %#v, want 4 entries", got["resolvers"])
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
			if resolver["key_template"] != "event/{{ hash _entity.text }}" {
				t.Fatalf("events resolver key template = %v", resolver["key_template"])
			}
		case AutoschemaEntitiesTable:
			if len(labels) != 0 {
				t.Fatalf("entities resolver must be a catch-all: %#v", resolver)
			}
			if resolver["key_template"] != "{{ lower _entity.label }}/{{ slug _entity.text }}" {
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
	if resolver["key_template"] != "{{ slug _entity.text }}" {
		t.Fatalf("concepts key template = %v", resolver["key_template"])
	}

	if _, err := antfly.NewCreateIndexRequest(idx); err != nil {
		t.Fatalf("NewCreateIndexRequest rejected taxonomy config: %v", err)
	}
}
