// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package metadata

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sort"

	"github.com/alpkeskin/gotoon"
	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/ai/eval"
	"github.com/antflydb/antfly/lib/query"
	"github.com/antflydb/antfly/lib/schema"
	generating "github.com/antflydb/antfly/pkg/generating"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/usermgr"
	"go.uber.org/zap"
)

// DefaultReserveTokens is the default number of tokens reserved for system prompt,
// answer generation, and other overhead when max_context_tokens is set.
const DefaultReserveTokens = 4000

const defaultQueryBuilderExampleDocumentLimit = 1

// cachedPruner returns a Pruner for the given document renderer template,
// creating and caching one if it doesn't already exist. The Pruner is safe
// for concurrent use, so a single instance per renderer string is sufficient.
func (t *TableApi) cachedPruner(documentRenderer string) (*ai.Pruner, error) {
	if documentRenderer == "" {
		documentRenderer = ai.DefaultDocumentRenderer
	}
	if v, ok := t.prunerCache.Load(documentRenderer); ok {
		return v.(*ai.Pruner), nil
	}
	p, err := ai.NewPruner(documentRenderer)
	if err != nil {
		return nil, err
	}
	actual, _ := t.prunerCache.LoadOrStore(documentRenderer, p)
	return actual.(*ai.Pruner), nil
}

type documentWithSubQuestions struct {
	Doc          schema.Document
	SubQuestions []int // Indices of sub-questions this document matches
}

// deduplicateDocuments deduplicates documents by ID and tracks which sub-questions each document matched.
// Returns deduplicated documents and a map from document ID to sub-question indices.
func deduplicateDocuments(docsWithSubQ []documentWithSubQuestions) ([]schema.Document, map[string][]int) {
	seen := make(map[string]*documentWithSubQuestions)
	order := make([]string, 0) // Preserve order of first occurrence

	for _, dwq := range docsWithSubQ {
		if existing, ok := seen[dwq.Doc.ID]; ok {
			// Merge sub-questions (avoid duplicates)
			subQSet := make(map[int]bool)
			for _, sq := range existing.SubQuestions {
				subQSet[sq] = true
			}
			for _, sq := range dwq.SubQuestions {
				if !subQSet[sq] {
					existing.SubQuestions = append(existing.SubQuestions, sq)
				}
			}
		} else {
			// Copy to avoid mutation issues
			copy := documentWithSubQuestions{
				Doc:          dwq.Doc,
				SubQuestions: append([]int{}, dwq.SubQuestions...),
			}
			seen[dwq.Doc.ID] = &copy
			order = append(order, dwq.Doc.ID)
		}
	}

	// Build result in original order
	docs := make([]schema.Document, 0, len(order))
	subQMap := make(map[string][]int, len(order))
	for _, id := range order {
		dwq := seen[id]
		docs = append(docs, dwq.Doc)
		subQMap[id] = dwq.SubQuestions
	}

	return docs, subQMap
}

// applyTokenPruning prunes documents to fit within the token budget if max_context_tokens is set.
// The documentRenderer should match the renderer used in the RAG pipeline for accurate estimates.
// Returns the pruned documents.
func (t *TableApi) applyTokenPruning(
	docs []schema.Document,
	maxContextTokens int,
	reserveTokens int,
	documentRenderer string,
	logger *zap.Logger,
) ([]schema.Document, *ai.PruneStats) {
	if maxContextTokens <= 0 {
		return docs, nil
	}

	// Use default reserve if not specified
	if reserveTokens <= 0 {
		reserveTokens = DefaultReserveTokens
	}

	// Get or create a cached pruner for this document renderer template
	pruner, err := t.cachedPruner(documentRenderer)
	if err != nil {
		logger.Warn("Failed to create pruner, skipping token pruning", zap.Error(err))
		return docs, nil
	}

	// Prune documents
	prunedDocs, stats, err := pruner.PruneToTokenBudget(docs, maxContextTokens, reserveTokens)
	if err != nil {
		logger.Warn("Token pruning failed, using all documents", zap.Error(err))
		return docs, nil
	}

	logger.Info("Token pruning applied",
		zap.Int("resources_kept", stats.ResourcesKept),
		zap.Int("tokens_kept", stats.TokensKept),
		zap.Int("resources_pruned", stats.ResourcesPruned),
		zap.Int("tokens_pruned", stats.TokensPruned),
	)

	return prunedDocs, &stats
}

// applyFiltersToQuery applies accumulated filters to a query's FullTextSearch field.
func applyFiltersToQuery(q *QueryRequest, filters []ai.FilterSpec, logger *zap.Logger) {
	if len(filters) == 0 || q.FullTextSearch != nil {
		return
	}

	var filterQueries []map[string]any
	for _, filter := range filters {
		filterQuery, err := ai.FilterSpecToQuery(filter)
		if err != nil {
			if logger != nil {
				logger.Warn("Failed to convert filter to query", zap.Error(err))
			}
			continue
		}
		filterQueries = append(filterQueries, filterQuery)
	}

	if len(filterQueries) > 0 {
		boolQuery := map[string]any{"conjuncts": filterQueries}
		queryBytes, _ := json.Marshal(boolQuery)
		q.FullTextSearch = queryBytes
	}
}

type sseEvent struct {
	Type SSEEvent // event type
	Data any      // event payload
}

// HitsStartEvent marks the beginning of results for a table
type HitsStartEvent struct {
	Table  string `json:"table"`
	Status int32  `json:"status"`
	Error  string `json:"error,omitempty"`
}

// HitsEndEvent marks the end of results for a table
type HitsEndEvent struct {
	Table    string `json:"table"`
	Total    int    `json:"total"`
	Returned int    `json:"returned"`
}

// KeywordsEvent is data for "keywords" event
type KeywordsEvent struct {
	Keywords []string `json:"keywords"`
}

// streamEvent writes an SSE event with JSON-encoded data.
// All data is JSON-encoded for consistency - strings, structs, maps, etc.
// Clients should always JSON.parse() the data field.
func streamEvent(w http.ResponseWriter, rc *http.ResponseController, eventType SSEEvent, data any) error {
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshaling event data: %w", err)
	}

	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, dataJSON); err != nil { //nolint:gosec // G705: JSON/SSE API response, not HTML
		return fmt.Errorf("writing SSE event: %w", err)
	}

	if err := rc.Flush(); err != nil {
		return fmt.Errorf("flushing SSE event: %w", err)
	}
	return nil
}

// QueryBuilderAgent translates natural language search intent into a structured Bleve query.
func (t *TableApi) QueryBuilderAgent(w http.ResponseWriter, r *http.Request) {
	// Decode the request
	var req QueryBuilderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Auth check for table access
	if req.Table != "" {
		if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, req.Table, usermgr.PermissionTypeRead) {
			return
		}
	}

	response, err := t.ExecuteQueryBuilder(r.Context(), &req)
	if err != nil {
		t.logger.Error("Query builder failed", zap.Error(err), zap.String("intent", req.Intent))
		errorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		t.logger.Warn("Failed to marshal response", zap.Error(err))
		errorResponse(w, "Failed to marshal response", http.StatusInternalServerError)
	}
}

// ExecuteQueryBuilder runs the query builder agent logic without HTTP concerns.
// It translates a natural language intent into a Bleve query using an LLM.
func (t *TableApi) ExecuteQueryBuilder(ctx context.Context, req *QueryBuilderRequest) (*QueryBuilderResult, error) {
	if req.Intent == "" {
		return nil, fmt.Errorf("intent cannot be empty")
	}

	// Build schema description - from table schema or provided fields
	var schemaDesc query.SchemaDescription
	exampleDocs := append([]map[string]any(nil), req.ExampleDocuments...)

	if req.Table != "" {
		tableData, err := t.tm.GetTable(req.Table)
		if err != nil {
			return nil, fmt.Errorf("table not found: %w", err)
		}
		schemaDesc = t.buildSchemaDescription(tableData.Schema, req.SchemaFields)
		if len(exampleDocs) == 0 && !hasDocumentSchema(tableData.Schema) {
			exampleDocs, err = t.loadQueryBuilderExampleDocuments(
				ctx,
				req.Table,
				req.SchemaFields,
				defaultQueryBuilderExampleDocumentLimit,
			)
			if err != nil {
				t.logger.Warn("Failed to load query builder example documents",
					zap.Error(err),
					zap.String("table", req.Table),
				)
			}
		}
	} else if len(req.SchemaFields) > 0 {
		schemaDesc = t.buildSchemaDescription(nil, req.SchemaFields)
	} else {
		schemaDesc = query.SchemaDescription{Fields: []query.FieldInfo{}}
	}

	// Get or create the generator config - use default chain if available
	var generatorConfig ai.GeneratorConfig
	defaultChain := generating.GetDefaultChain()
	if req.Generator.Provider != "" {
		generatorConfig = req.Generator
	} else if len(defaultChain) > 0 {
		generatorConfig = defaultChain[0].Generator
	} else {
		return nil, fmt.Errorf("generator must be provided (no default chain configured)")
	}

	generator, err := ai.NewGenKitGenerator(ctx, generatorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create generator: %w", err)
	}

	generationOpts := make([]ai.QueryBuilderOption, 0, 1)
	if len(exampleDocs) > 0 {
		rendered := renderQueryBuilderExampleDocuments(exampleDocs)
		if len(rendered) > 0 {
			generationOpts = append(generationOpts, ai.WithQueryBuilderExampleDocuments(rendered))
		}
	}

	result, err := generator.BuildQueryBleve(ctx, req.Intent, schemaDesc, generationOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	return &QueryBuilderResult{
		Query:       result.Query,
		Explanation: result.Explanation,
		Confidence:  result.Confidence,
		Warnings:    result.Warnings,
	}, nil
}

// buildSchemaDescription builds a query.SchemaDescription from a TableSchema.
// If schemaFields is provided, only those fields are included.
func (t *TableApi) buildSchemaDescription(tableSchema *schema.TableSchema, schemaFields []string) query.SchemaDescription {
	desc := query.SchemaDescription{
		Fields: []query.FieldInfo{},
	}

	if tableSchema == nil || len(tableSchema.DocumentSchemas) == 0 {
		for _, name := range schemaFields {
			desc.Fields = append(desc.Fields, query.FieldInfo{
				Name:       name,
				Type:       "text",
				Searchable: true,
			})
		}
		return desc
	}

	fieldFilter := make(map[string]struct{}, len(schemaFields))
	for _, field := range schemaFields {
		fieldFilter[field] = struct{}{}
	}

	docTypeNames := make([]string, 0, len(tableSchema.DocumentSchemas))
	for docType := range tableSchema.DocumentSchemas {
		docTypeNames = append(docTypeNames, docType)
	}
	sort.Strings(docTypeNames)

	fieldByName := make(map[string]query.FieldInfo)
	for _, docType := range docTypeNames {
		docSchema := tableSchema.DocumentSchemas[docType]
		extracted := query.ExtractSchemaDescription(docSchema.Schema)
		if desc.Description == "" {
			switch {
			case docType == tableSchema.DefaultType && docSchema.Description != "":
				desc.Description = docSchema.Description
			case extracted.Description != "":
				desc.Description = extracted.Description
			case docSchema.Description != "":
				desc.Description = docSchema.Description
			}
		}

		for _, field := range extracted.Fields {
			if len(fieldFilter) > 0 {
				if _, ok := fieldFilter[field.Name]; !ok {
					continue
				}
			}
			existing, ok := fieldByName[field.Name]
			if !ok {
				fieldByName[field.Name] = field
				continue
			}
			fieldByName[field.Name] = mergeQueryBuilderFieldInfo(existing, field)
		}
	}

	fieldNames := make([]string, 0, len(fieldByName))
	for fieldName := range fieldByName {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)
	for _, fieldName := range fieldNames {
		desc.Fields = append(desc.Fields, fieldByName[fieldName])
	}

	return desc
}

func mergeQueryBuilderFieldInfo(existing, incoming query.FieldInfo) query.FieldInfo {
	if existing.Description == "" {
		existing.Description = incoming.Description
	}
	if existing.Type == "" {
		existing.Type = incoming.Type
	}
	existing.Searchable = existing.Searchable || incoming.Searchable
	existing.Nested = existing.Nested || incoming.Nested
	if existing.ArrayOf == "" {
		existing.ArrayOf = incoming.ArrayOf
	}
	if existing.Format == "" {
		existing.Format = incoming.Format
	}
	existing.Nullable = existing.Nullable || incoming.Nullable
	if existing.ValueRange == nil {
		existing.ValueRange = incoming.ValueRange
	}
	if len(existing.Children) == 0 {
		existing.Children = incoming.Children
	}
	existing.Types = appendUniqueStrings(existing.Types, incoming.Types...)
	existing.ExampleValues = appendUniqueStrings(existing.ExampleValues, incoming.ExampleValues...)
	existing.CommonQueries = appendUniqueStrings(existing.CommonQueries, incoming.CommonQueries...)
	return existing
}

func appendUniqueStrings(dst []string, values ...string) []string {
	for _, value := range values {
		if value == "" || slices.Contains(dst, value) {
			continue
		}
		dst = append(dst, value)
	}
	return dst
}

func hasDocumentSchema(tableSchema *schema.TableSchema) bool {
	return tableSchema != nil && len(tableSchema.DocumentSchemas) > 0
}

func (t *TableApi) loadQueryBuilderExampleDocuments(
	ctx context.Context,
	tableName string,
	schemaFields []string,
	limit int,
) ([]map[string]any, error) {
	if tableName == "" || limit <= 0 {
		return nil, nil
	}

	queryReq := &QueryRequest{
		Table: tableName,
		Limit: limit,
	}
	if len(schemaFields) > 0 {
		queryReq.Fields = append([]string(nil), schemaFields...)
	}

	result := t.runQuery(ctx, queryReq)
	if result.Status != http.StatusOK {
		return nil, fmt.Errorf("query failed with status %d: %s", result.Status, result.Error)
	}

	exampleDocs := make([]map[string]any, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		if len(hit.Source) == 0 {
			continue
		}
		exampleDocs = append(exampleDocs, filterQueryBuilderExampleDocument(hit.Source, schemaFields))
	}

	return exampleDocs, nil
}

func filterQueryBuilderExampleDocument(doc map[string]any, schemaFields []string) map[string]any {
	filtered := make(map[string]any)
	if len(schemaFields) == 0 {
		for key, value := range doc {
			filtered[key] = value
		}
		return filtered
	}

	for _, field := range schemaFields {
		if value, ok := doc[field]; ok {
			filtered[field] = value
		}
	}
	return filtered
}

func renderQueryBuilderExampleDocuments(exampleDocs []map[string]any) []string {
	rendered := make([]string, 0, len(exampleDocs))
	for _, doc := range exampleDocs {
		if len(doc) == 0 {
			continue
		}
		encoded, err := gotoon.Encode(doc, gotoon.WithLengthMarker(), gotoon.WithIndent(2))
		if err != nil {
			continue
		}
		rendered = append(rendered, encoded)
	}
	return rendered
}

// Evaluate handles standalone evaluation requests (POST /eval).
func (t *TableApi) Evaluate(w http.ResponseWriter, r *http.Request) {
	// Decode the request
	var req eval.EvalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate evaluators
	if len(req.Evaluators) == 0 {
		errorResponse(w, "At least one evaluator must be specified", http.StatusBadRequest)
		return
	}

	// Create orchestrator and run evaluation
	orchestrator := eval.NewOrchestrator()
	result, err := orchestrator.EvaluateRequest(r.Context(), req)
	if err != nil {
		t.logger.Error("Evaluation failed", zap.Error(err))
		errorResponse(w, fmt.Sprintf("Evaluation failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Return result
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		t.logger.Error("Failed to encode response", zap.Error(err))
	}
}
