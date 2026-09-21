package main

// AutoSchemaKG provisioning (zig/AUTOSCHEMA.md, arXiv:2505.23628).
//
// --autoschema provisions the paper's pipeline as pure Antfly configuration:
//
//   Stage 1: three generator asset enrichments on the documents table
//            (kg_ee_v1 entity-entity, kg_ev_v1 entity-event, kg_vv_v1
//            event-event), each a forced tool call emitting the
//            extraction_graph shape {entities:[{id,label,text}],
//            relations:[{type,source,target,evidence}]}.
//   Stage 2: one "knowledge_graph" graph index merging the three artifact
//            streams, with label-routed resolvers promoting mentions into
//            the entities and events tables.
//   Stage 3: a recursive autograph on the entities table: the
//            conceptualize_v1 enrichment abstracts each promoted entity
//            into >=3 concept phrases, and the "taxonomy" graph index
//            promotes them into the concepts table with is_a edges.
//
// The extraction prompts are ported from the reference implementation's
// MIT-licensed prompt set (github.com/HKUST-KnowComp/AutoSchemaKG,
// atlas_rag/llm_generator/prompt/triple_extraction_prompt.py), adapted to
// forced tool calling.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	antfly "github.com/antflydb/antfly/go/pkg/sdk"
	oapi "github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

const (
	// DefaultAutoschemaModel is shared by the three extraction passes and the
	// conceptualizer. AutoSchemaKG's extraction works with 8B-class models.
	DefaultAutoschemaModel = DefaultGeneratorModel

	AutoschemaKnowledgeGraphIndex = "knowledge_graph"
	AutoschemaTaxonomyIndex       = "taxonomy"

	AutoschemaEntityEntityAsset = "kg_ee_v1"
	AutoschemaEntityEventAsset  = "kg_ev_v1"
	AutoschemaEventEventAsset   = "kg_vv_v1"
	AutoschemaConceptAsset      = "conceptualize_v1"

	AutoschemaEntitiesTable = "entities"
	AutoschemaEventsTable   = "events"
	AutoschemaConceptsTable = "concepts"

	autoschemaExtractionToolName = "emit_graph"
	autoschemaConceptToolName    = "emit_concepts"
)

// Extraction prompts, one per AutoSchemaKG pass, ported from the reference
// TRIPLE_INSTRUCTIONS/CONCEPT_INSTRUCTIONS prompt set and adapted to emit the
// extraction_graph tool-call shape instead of free-form JSON arrays.
const (
	autoschemaEntityEntityPrompt = "Given the following passage, summarize all the important entities and " +
		"the relations between them in a concise manner. Relations should briefly capture the connections " +
		"between entities, without repeating information from the head and tail entities. The entities " +
		"should be as specific as possible. Exclude pronouns from being considered as entities. " +
		"Call the emit_graph tool exactly once: list each entity under \"entities\" with a unique id, a " +
		"short lowercase label such as person, organization, or location, and its text; list each relation " +
		"under \"relations\" with a short verb phrase as its type, the source and target entity ids, and a " +
		"short evidence span from the passage. Extract only what the passage itself states: never invent entities, events, or relations that are not present, and never reuse examples from these instructions. If the passage is empty, unreadable, or contains nothing to extract, call the tool with empty \"entities\" and \"relations\" arrays. Here is the passage:"

	autoschemaEntityEventPrompt = "Please analyze and summarize the participation relations between the " +
		"events and entities in the following passage. Each event is a single independent sentence written " +
		"as a simple normalized sentence. Additionally, identify all the entities that participated in the " +
		"events. Do not use ellipses. Call the emit_graph tool exactly once: list each event under " +
		"\"entities\" with label \"event\" and its text set to the normalized simple sentence describing the " +
		"event; list each participating entity with a short lowercase label and its text; for every entity " +
		"that participates in an event, add a relation of type \"participates_in\" from the entity id to the " +
		"event id, with a short evidence span from the passage. Extract only what the passage itself states: never invent entities, events, or relations that are not present, and never reuse examples from these instructions. If the passage is empty, unreadable, or contains nothing to extract, call the tool with empty \"entities\" and \"relations\" arrays. Here is the passage:"

	autoschemaEventEventPrompt = "Please analyze and summarize the relationships between the events in the " +
		"following passage. Each event is a single independent sentence. Identify temporal and causal " +
		"relationships between the events using only the relation types before, after, concurrent, because, " +
		"and as_result. Each relation should be specific, meaningful, and able to stand alone. Do not use " +
		"ellipses. Call the emit_graph tool exactly once: list each event under \"entities\" with label " +
		"\"event\" and its text set to the simple sentence describing the event; list each temporal or " +
		"causal relation under \"relations\" with the source and target event ids and a short evidence span " +
		"from the passage. Extract only what the passage itself states: never invent entities, events, or relations that are not present, and never reuse examples from these instructions. If the passage is empty, unreadable, or contains nothing to extract, call the tool with empty \"entities\" and \"relations\" arrays. Here is the passage:"

	autoschemaConceptPrompt = "You are given an entity from a knowledge graph (its canonical name and type, " +
		"followed by a JSON block of its sampled graph neighbors when available). Produce three or more " +
		"concept phrases of one or two words for the abstract concepts of this entity, at increasing levels " +
		"of abstraction. Each concept phrase should represent the entity well: it can be the type of the " +
		"entity or a closely related concept. Do not repeat the entity itself and do not repeat the same " +
		"phrase. Call the emit_concepts tool exactly once: list each concept phrase under \"entities\" with " +
		"label \"concept\", a unique id, and the phrase as its text; for each concept add a relation of type " +
		"\"is_a\" with the concept id as target. Here is the entity:"
)

// autoschemaExtractionToolParameters builds the JSON Schema for the pinned
// extraction tool call. All three passes emit the same extraction_graph shape;
// the label and relation-type sub-schemas are the per-pass guardrails
// (open string for kg_ee_v1, pinned enums for kg_ev_v1/kg_vv_v1).
func autoschemaExtractionToolParameters(labelSchema, relationTypeSchema map[string]any) map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"entities", "relations"},
		"properties": map[string]any{
			"entities": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"required":             []string{"id", "label", "text"},
					"properties": map[string]any{
						"id":    map[string]any{"type": "string", "description": "Unique local identifier such as e0, e1, v0."},
						"label": labelSchema,
						"text":  map[string]any{"type": "string", "description": "Surface text of the entity or the normalized event sentence."},
					},
				},
			},
			"relations": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"required":             []string{"type", "source", "target"},
					"properties": map[string]any{
						"type":     relationTypeSchema,
						"source":   map[string]any{"type": "string", "description": "id of the source item in entities."},
						"target":   map[string]any{"type": "string", "description": "id of the target item in entities."},
						"evidence": map[string]any{"type": "string", "description": "Short text span from the passage supporting the relation."},
					},
				},
			},
		},
	}
}

// autoschemaConceptToolParameters is the Stage 3 guardrail: at least three
// concept phrases, every item labelled "concept", every relation an is_a edge
// whose source (the promoted entity document) is implied by the enrichment's
// owning document.
func autoschemaConceptToolParameters() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"entities", "relations"},
		"properties": map[string]any{
			"entities": map[string]any{
				"type":     "array",
				"minItems": 3,
				"items": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"required":             []string{"id", "label", "text"},
					"properties": map[string]any{
						"id":    map[string]any{"type": "string", "description": "Unique local identifier such as c0, c1."},
						"label": map[string]any{"type": "string", "enum": []string{"concept"}},
						"text":  map[string]any{"type": "string", "description": "Concept phrase of one or two words."},
					},
				},
			},
			"relations": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"required":             []string{"type", "target"},
					"properties": map[string]any{
						"type":   map[string]any{"type": "string", "enum": []string{"is_a"}},
						"source": map[string]any{"type": "string", "description": "Omit to use the current entity as the source."},
						"target": map[string]any{"type": "string", "description": "id of the concept item in entities."},
					},
				},
			},
		},
	}
}

// autoschemaGeneratorProducerJSON serializes a forced-tool-call generator
// producer following the pattern proven by artifactProducerConfig: pinned
// tool_choice, tool_output "arguments", additionalProperties:false schema.
// The extraction instructions travel in the enrichment source template, not
// here: the generator producer's canonical config contract has no `prompt`
// field (the rendered source_text IS the prompt; see asset_producer_runtime
// `.source_text = "prompt"` fixtures and generatorConfigFromValue's strict
// envelope parse, which rejects unknown config fields).
func autoschemaGeneratorProducerJSON(model, inferenceAPIURL, toolName, toolDescription string, parameters map[string]any) (string, error) {
	producer := map[string]any{
		"type": "generator",
		"config": map[string]any{
			"provider":    "antfly",
			"model":       model,
			"api_url":     inferenceAPIURL,
			"tool_output": "arguments",
			"tool_name":   toolName,
			"tool_choice": map[string]any{
				"type": "function",
				"function": map[string]any{
					"name": toolName,
				},
			},
			"tools": []map[string]any{
				{
					"type": "function",
					"function": map[string]any{
						"name":        toolName,
						"description": toolDescription,
						"parameters":  parameters,
					},
				},
			},
		},
	}
	encoded, err := json.Marshal(producer)
	if err != nil {
		return "", fmt.Errorf("marshal %s producer config: %w", toolName, err)
	}
	return string(encoded), nil
}

// autoschemaExtractionEnrichments declares the three Stage 1 asset enrichments
// on the documents table. Multiple asset artifacts for one graph index are
// declared through the index-level `enrichments` field (the graph shorthand
// `artifact` field supports exactly one producer); admission collects
// enrichments from every index config in the table request and validates the
// graph `sources` references against them.
func autoschemaExtractionEnrichments(model, inferenceAPIURL string) ([]antfly.EnrichmentConfig, error) {
	passes := []struct {
		name               string
		prompt             string
		description        string
		labelSchema        map[string]any
		relationTypeSchema map[string]any
	}{
		{
			name:        AutoschemaEntityEntityAsset,
			prompt:      autoschemaEntityEntityPrompt,
			description: "Emit the important entities in the passage and the relations between them.",
			labelSchema: map[string]any{
				"type":        "string",
				"description": "Short lowercase entity type such as person, organization, or location.",
			},
			relationTypeSchema: map[string]any{
				"type":        "string",
				"description": "Short verb phrase capturing the connection between source and target.",
			},
		},
		{
			name:        AutoschemaEntityEventAsset,
			prompt:      autoschemaEntityEventPrompt,
			description: "Emit the events in the passage and the entities participating in them.",
			labelSchema: map[string]any{
				"type":        "string",
				"description": "Short lowercase entity type, or exactly \"event\" for event items.",
			},
			relationTypeSchema: map[string]any{
				"type": "string",
				"enum": []string{"participates_in"},
			},
		},
		{
			name:        AutoschemaEventEventAsset,
			prompt:      autoschemaEventEventPrompt,
			description: "Emit the events in the passage and the temporal or causal relations between them.",
			labelSchema: map[string]any{
				"type": "string",
				"enum": []string{"event"},
			},
			relationTypeSchema: map[string]any{
				"type": "string",
				"enum": []string{"before", "after", "concurrent", "because", "as_result"},
			},
		},
	}

	enrichments := make([]antfly.EnrichmentConfig, 0, len(passes))
	for _, pass := range passes {
		producerJSON, err := autoschemaGeneratorProducerJSON(
			model,
			inferenceAPIURL,
			autoschemaExtractionToolName,
			pass.description,
			autoschemaExtractionToolParameters(pass.labelSchema, pass.relationTypeSchema),
		)
		if err != nil {
			return nil, err
		}
		enrichments = append(enrichments, antfly.EnrichmentConfig{
			Name: pass.name,
			Kind: antfly.EnrichmentKindAsset,
			// The rendered template is the generator's prompt: stage
			// instructions followed by the document text.
			Template:     pass.prompt + "\n\nDocument:\n{{ content }}",
			ContentType:  "application/json",
			ProducerJson: producerJSON,
		})
	}
	return enrichments, nil
}

// createAutoschemaKnowledgeGraphIndex builds the Stage 1+2 "knowledge_graph"
// index for the documents table: three extraction_graph sources merged in
// declaration order plus label-routed resolvers.
//
// Resolvers are scoped to one source artifact each (GraphResolverConfig's
// source_artifact is required and singular, and sibling label disjointness is
// enforced per artifact), so the doc's "two resolvers" become one
// events/catch-all pair per artifact that can emit that mention class, each
// with its own resolution artifact name.
func createAutoschemaKnowledgeGraphIndex(model, inferenceURL string) (*antfly.IndexConfig, error) {
	if strings.TrimSpace(model) == "" {
		return nil, fmt.Errorf("autoschema model is required")
	}
	inferenceAPIURL, err := inferenceMLBaseURL(inferenceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid autoschema inference URL: %w", err)
	}

	enrichments, err := autoschemaExtractionEnrichments(model, inferenceAPIURL)
	if err != nil {
		return nil, err
	}

	sources, err := antfly.NewGraphIndexSources(
		antfly.GraphArtifactSourceConfig{
			Artifact:        AutoschemaEntityEntityAsset,
			Format:          antfly.GraphArtifactSourceConfigFormatExtractionGraph,
			MentionEdgeType: "mentions",
		},
		antfly.GraphArtifactSourceConfig{
			Artifact:        AutoschemaEntityEventAsset,
			Format:          antfly.GraphArtifactSourceConfigFormatExtractionGraph,
			MentionEdgeType: "mentions",
		},
		antfly.GraphArtifactSourceConfig{
			Artifact:        AutoschemaEventEventAsset,
			Format:          antfly.GraphArtifactSourceConfigFormatExtractionGraph,
			MentionEdgeType: "mentions",
		},
	)
	if err != nil {
		return nil, fmt.Errorf("build knowledge graph sources: %w", err)
	}

	entityKeyTemplate := "{{ lower _entity.label }}/{{ slug _entity.text }}"
	// Event keys hash the normalized event sentence so re-extraction converges
	// on the same event documents across replays.
	eventKeyTemplate := "event/{{ hash _entity.text }}"

	cfg := antfly.GraphIndexConfig{
		Sources: sources,
		// Named pagerank metric for HippoRAG-style retrieval: query-seeded
		// personalized reads reference it by name with metric_freshness=fresh
		// (seed_nodes + damping on the graph_metric query).
		Metrics: map[string]oapi.GraphMetricConfig{
			"ppr": {
				Kind:    oapi.GraphMetricConfigKindPagerank,
				Enabled: true,
				Damping: 0.85,
			},
		},
		EdgeTypes: []antfly.EdgeTypeConfig{
			{Name: "mentions"},
			{Name: "participates_in"},
			{Name: "before"},
			{Name: "after"},
			{Name: "concurrent"},
			{Name: "because"},
			{Name: "as_result"},
		},
		Resolvers: []oapi.GraphResolverConfig{
			{
				// kg_ee_v1 emits open-vocabulary entity labels only; a
				// catch-all resolver keeps them open while routing every
				// mention into the entities table.
				Name:               "entities_ee",
				Table:              AutoschemaEntitiesTable,
				SourceArtifact:     AutoschemaEntityEntityAsset,
				ResolutionArtifact: "entities_ee_resolution_v1",
				KeyTemplate:        entityKeyTemplate,
				CandidateSearch:    oapi.GraphResolverConfigCandidateSearchPrefix,
				ConfigGeneration:   1,
			},
			{
				// kg_ev_v1 mixes entities and events; the labeled resolver
				// claims "event" mentions and the catch-all below skips them.
				Name:               "events_ev",
				Table:              AutoschemaEventsTable,
				SourceArtifact:     AutoschemaEntityEventAsset,
				ResolutionArtifact: "events_ev_resolution_v1",
				KeyTemplate:        eventKeyTemplate,
				Labels:             []string{"event"},
				ConfigGeneration:   1,
			},
			{
				Name:               "entities_ev",
				Table:              AutoschemaEntitiesTable,
				SourceArtifact:     AutoschemaEntityEventAsset,
				ResolutionArtifact: "entities_ev_resolution_v1",
				KeyTemplate:        entityKeyTemplate,
				CandidateSearch:    oapi.GraphResolverConfigCandidateSearchPrefix,
				ConfigGeneration:   1,
			},
			{
				// kg_vv_v1 endpoints are all events (tool schema pins the
				// label), so a single labeled resolver suffices.
				Name:               "events_vv",
				Table:              AutoschemaEventsTable,
				SourceArtifact:     AutoschemaEventEventAsset,
				ResolutionArtifact: "events_vv_resolution_v1",
				KeyTemplate:        eventKeyTemplate,
				Labels:             []string{"event"},
				ConfigGeneration:   1,
			},
		},
	}

	idx, err := antfly.NewIndexConfig(AutoschemaKnowledgeGraphIndex, cfg)
	if err != nil {
		return nil, fmt.Errorf("build knowledge graph index config: %w", err)
	}
	idx.Enrichments = enrichments
	return idx, nil
}

// createAutoschemaTaxonomyIndex builds the Stage 3 recursive autograph for the
// entities table: the conceptualize_v1 enrichment plus the "taxonomy" graph
// index promoting concept phrases into the concepts table with is_a edges.
//
// neighbor_context must name a graph index on the same table (admission
// closes the reference against the table's index catalog), and the only graph
// index on entities is "taxonomy" itself, so the conceptualizer samples its
// own is_a adjacency: the first pass runs without neighbors and later passes
// ground re-conceptualization in the previously promoted concepts. Declaring
// the enrichment and the index in the same create-table request is what makes
// the reference admissible; provisioning them separately would require the
// taxonomy index to exist first.
func createAutoschemaTaxonomyIndex(model, inferenceURL string) (*antfly.IndexConfig, error) {
	if strings.TrimSpace(model) == "" {
		return nil, fmt.Errorf("autoschema model is required")
	}
	inferenceAPIURL, err := inferenceMLBaseURL(inferenceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid autoschema inference URL: %w", err)
	}

	producerJSON, err := autoschemaGeneratorProducerJSON(
		model,
		inferenceAPIURL,
		autoschemaConceptToolName,
		"Emit three or more abstract concept phrases for the entity, each linked by an is_a relation.",
		autoschemaConceptToolParameters(),
	)
	if err != nil {
		return nil, err
	}

	sources, err := antfly.NewGraphIndexSources(
		antfly.GraphArtifactSourceConfig{
			Artifact:        AutoschemaConceptAsset,
			Format:          antfly.GraphArtifactSourceConfigFormatExtractionGraph,
			MentionEdgeType: "mentions",
		},
	)
	if err != nil {
		return nil, fmt.Errorf("build taxonomy sources: %w", err)
	}

	cfg := antfly.GraphIndexConfig{
		Sources: sources,
		EdgeTypes: []antfly.EdgeTypeConfig{
			{Name: "is_a"},
			{Name: "mentions"},
		},
		Resolvers: []oapi.GraphResolverConfig{
			{
				// Identical phrases from different entities converge on the
				// same concept document: the emergent taxonomy is
				// deduplication by canonical key.
				Name:               "concepts",
				Table:              AutoschemaConceptsTable,
				SourceArtifact:     AutoschemaConceptAsset,
				ResolutionArtifact: "concepts_resolution_v1",
				KeyTemplate:        "{{ slug _entity.text }}",
				Labels:             []string{"concept"},
				ConfigGeneration:   1,
			},
		},
	}

	idx, err := antfly.NewIndexConfig(AutoschemaTaxonomyIndex, cfg)
	if err != nil {
		return nil, fmt.Errorf("build taxonomy index config: %w", err)
	}
	idx.Enrichments = []antfly.EnrichmentConfig{
		{
			Name: AutoschemaConceptAsset,
			Kind: antfly.EnrichmentKindAsset,
			// The rendered template is the generator's prompt: concept
			// instructions followed by the promoted entity document's
			// fields (entity_type, canonical_name; see promotion_runtime.zig).
			Template:     autoschemaConceptPrompt + "\n\nEntity: {{ canonical_name }} ({{ entity_type }})",
			ContentType:  "application/json",
			ProducerJson: producerJSON,
			NeighborContext: oapi.EnrichmentNeighborContextConfig{
				GraphIndex: AutoschemaTaxonomyIndex,
				EdgeTypes:  []string{"is_a"},
				Direction:  oapi.EnrichmentNeighborContextConfigDirectionOut,
				Limit:      8,
			},
		},
	}
	return idx, nil
}

// provisionAutoschemaTables creates the concepts and events tables (plain) and
// the entities table with the Stage 3 taxonomy autograph. The entity, event,
// and concept tables must exist before the documents table's knowledge_graph
// resolvers start promoting into them.
func provisionAutoschemaTables(ctx context.Context, client *antfly.AntflyClient, model, inferenceURL string) error {
	for _, tableName := range []string{AutoschemaConceptsTable, AutoschemaEventsTable} {
		fmt.Printf("Creating table '%s'...\n", tableName)
		if err := client.CreateTable(ctx, tableName, antfly.CreateTableRequest{NumShards: 1}); err != nil {
			log.Printf("Warning: Failed to create table %s (may already exist): %v\n", tableName, err)
		}
	}

	taxonomyIndex, err := createAutoschemaTaxonomyIndex(model, inferenceURL)
	if err != nil {
		return fmt.Errorf("failed to create taxonomy index config: %w", err)
	}
	taxonomyRequest, err := antfly.NewCreateIndexRequest(taxonomyIndex)
	if err != nil {
		return fmt.Errorf("failed to create taxonomy index request: %w", err)
	}
	fmt.Printf("Creating table '%s' with taxonomy autograph...\n", AutoschemaEntitiesTable)
	if err := client.CreateTable(ctx, AutoschemaEntitiesTable, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]antfly.CreateIndexRequest{
			AutoschemaTaxonomyIndex: *taxonomyRequest,
		},
	}); err != nil {
		log.Printf("Warning: Failed to create table %s (may already exist): %v\n", AutoschemaEntitiesTable, err)
	}

	for _, tableName := range []string{AutoschemaConceptsTable, AutoschemaEventsTable, AutoschemaEntitiesTable} {
		if err := client.WaitForTable(ctx, tableName, 30*time.Second); err != nil {
			return fmt.Errorf("error waiting for table %s: %w", tableName, err)
		}
	}
	return nil
}
