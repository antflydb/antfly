package docsaf

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pb33f/libopenapi"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
	orderedmap "github.com/pb33f/libopenapi/orderedmap"
	"go.yaml.in/yaml/v4"
)

// OpenAPIProcessor processes OpenAPI specification content using libopenapi.
// It extracts API info, paths, and schemas as separate document sections.
type OpenAPIProcessor struct{}

// CanProcess returns true for .yaml, .yml, and .json files.
// Note: The content will only be processed if it's a valid OpenAPI v3 specification.
func (op *OpenAPIProcessor) CanProcess(contentType, path string) bool {
	if strings.Contains(contentType, "application/x-yaml") ||
		strings.Contains(contentType, "application/yaml") ||
		strings.Contains(contentType, "text/yaml") ||
		strings.Contains(contentType, "application/json") {
		return true
	}
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".yaml") ||
		strings.HasSuffix(lower, ".yml") ||
		strings.HasSuffix(lower, ".json")
}

// Process processes OpenAPI specification content and returns document sections.
// Returns an error if the content is not a valid OpenAPI v3 specification.
// Questions from x-docsaf-questions extensions are associated with their sections.
func (op *OpenAPIProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error) {
	// Try to parse as OpenAPI
	doc, err := libopenapi.NewDocument(content)
	if err != nil {
		return nil, fmt.Errorf("not a valid OpenAPI document: %w", err)
	}

	var sections []DocumentSection

	v3Model, err := doc.BuildV3Model()
	if err != nil {
		return nil, fmt.Errorf("failed to build OpenAPI v3 model: %w", err)
	}

	if v3Model != nil {
		sections = append(sections, op.extractV3Sections(&v3Model.Model, path, sourceURL, baseURL)...)
	}

	return sections, nil
}

// extractV3Sections extracts document sections with questions from an OpenAPI v3 document.
func (op *OpenAPIProcessor) extractV3Sections(model *v3.Document, path, sourceURL, baseURL string) []DocumentSection {
	var sections []DocumentSection
	extractor := &QuestionsExtractor{}

	// Extract API info as a section with questions
	if model.Info != nil {
		infoJSON, _ := json.MarshalIndent(model.Info, "", "  ")

		url := ""
		if baseURL != "" {
			url = baseURL + "/" + path + "#info"
		}

		metadata := map[string]any{
			"openapi_version": model.Version,
			"api_version":     model.Info.Version,
			"api_title":       model.Info.Title,
		}
		if sourceURL != "" {
			metadata["source_url"] = sourceURL
		}

		section := DocumentSection{
			ID:       generateID(path, "info"),
			FilePath: path,
			Title:    fmt.Sprintf("%s (Info)", model.Info.Title),
			Content:  string(infoJSON),
			Type:     "openapi_info",
			URL:      url,
			Metadata: metadata,
		}

		// Extract questions from Info extensions
		if model.Info.Extensions != nil {
			ext := orderedMapToMap(model.Info.Extensions)
			questions := extractor.ExtractFromOpenAPI(path, sourceURL, "openapi_info", model.Info.Title, ext)
			section.Questions = questionsToStrings(questions)
		}

		sections = append(sections, section)
	}

	// Extract paths as individual sections with questions
	if model.Paths != nil && model.Paths.PathItems != nil {
		for pathPair := model.Paths.PathItems.First(); pathPair != nil; pathPair = pathPair.Next() {
			pathKey := pathPair.Key()
			pathItem := pathPair.Value()

			operations := extractOperations(pathItem)
			for method, operation := range operations {
				opJSON, _ := json.MarshalIndent(operation, "", "  ")
				operationID := operation.OperationId
				if operationID == "" {
					operationID = fmt.Sprintf("%s_%s", method, strings.ReplaceAll(pathKey, "/", "_"))
				}

				url := ""
				if baseURL != "" {
					slug := strings.ToLower(method) + "-" + pathKey
					url = baseURL + "/" + path + "#" + slug
				}

				metadata := map[string]any{
					"http_method":  method,
					"path":         pathKey,
					"operation_id": operationID,
					"summary":      operation.Summary,
					"description":  operation.Description,
					"tags":         operation.Tags,
				}
				if sourceURL != "" {
					metadata["source_url"] = sourceURL
				}

				section := DocumentSection{
					ID:       generateID(path, fmt.Sprintf("path_%s_%s", method, pathKey)),
					FilePath: path,
					Title:    fmt.Sprintf("%s %s", strings.ToUpper(method), pathKey),
					Content:  string(opJSON),
					Type:     "openapi_path",
					URL:      url,
					Metadata: metadata,
				}

				// Extract questions from operation extensions
				if operation.Extensions != nil {
					ext := orderedMapToMap(operation.Extensions)
					context := fmt.Sprintf("%s %s", strings.ToUpper(method), pathKey)
					if operation.OperationId != "" {
						context = operation.OperationId
					}
					questions := extractor.ExtractFromOpenAPI(path, sourceURL, "openapi_operation", context, ext)
					section.Questions = questionsToStrings(questions)
				}

				sections = append(sections, section)
			}
		}
	}

	// Extract schemas as individual sections with questions
	if model.Components != nil && model.Components.Schemas != nil {
		for schemaPair := model.Components.Schemas.First(); schemaPair != nil; schemaPair = schemaPair.Next() {
			schemaName := schemaPair.Key()
			schemaProxy := schemaPair.Value()
			schema := schemaProxy.Schema()
			if schema == nil {
				continue
			}

			schemaJSON, _ := json.MarshalIndent(schema, "", "  ")

			url := ""
			if baseURL != "" {
				slug := "schema-" + strings.ToLower(schemaName)
				url = baseURL + "/" + path + "#" + slug
			}

			metadata := map[string]any{
				"schema_name": schemaName,
				"schema_type": schema.Type,
				"description": schema.Description,
			}
			if sourceURL != "" {
				metadata["source_url"] = sourceURL
			}

			section := DocumentSection{
				ID:       generateID(path, fmt.Sprintf("schema_%s", schemaName)),
				FilePath: path,
				Title:    fmt.Sprintf("Schema: %s", schemaName),
				Content:  string(schemaJSON),
				Type:     "openapi_schema",
				URL:      url,
				Metadata: metadata,
			}

			// Extract questions from schema extensions
			if schema.Extensions != nil {
				ext := orderedMapToMap(schema.Extensions)
				questions := extractor.ExtractFromOpenAPI(path, sourceURL, "openapi_schema", schemaName, ext)
				section.Questions = questionsToStrings(questions)
			}

			sections = append(sections, section)
		}
	}

	return sections
}

// questionsToStrings extracts just the text from a slice of Questions.
func questionsToStrings(questions []Question) []string {
	if len(questions) == 0 {
		return nil
	}
	result := make([]string, len(questions))
	for i, q := range questions {
		result[i] = q.Text
	}
	return result
}

func extractOperations(pathItem *v3.PathItem) map[string]*v3.Operation {
	ops := make(map[string]*v3.Operation)
	if pathItem.Get != nil {
		ops["get"] = pathItem.Get
	}
	if pathItem.Post != nil {
		ops["post"] = pathItem.Post
	}
	if pathItem.Put != nil {
		ops["put"] = pathItem.Put
	}
	if pathItem.Delete != nil {
		ops["delete"] = pathItem.Delete
	}
	if pathItem.Patch != nil {
		ops["patch"] = pathItem.Patch
	}
	if pathItem.Options != nil {
		ops["options"] = pathItem.Options
	}
	if pathItem.Head != nil {
		ops["head"] = pathItem.Head
	}
	return ops
}

// ExtractQuestions extracts x-docsaf-questions from OpenAPI extensions.
// It looks for questions at:
// 1. Top-level document info
// 2. Individual paths/operations
// 3. Component schemas
func (op *OpenAPIProcessor) ExtractQuestions(path, sourceURL string, content []byte) ([]Question, error) {
	doc, err := libopenapi.NewDocument(content)
	if err != nil {
		return nil, fmt.Errorf("not a valid OpenAPI document: %w", err)
	}

	v3Model, err := doc.BuildV3Model()
	if err != nil {
		return nil, fmt.Errorf("failed to build OpenAPI v3 model: %w", err)
	}

	if v3Model == nil {
		return nil, nil
	}

	return op.extractV3Questions(&v3Model.Model, path, sourceURL), nil
}

// extractV3Questions extracts questions from OpenAPI v3 extensions.
func (op *OpenAPIProcessor) extractV3Questions(model *v3.Document, path, sourceURL string) []Question {
	var questions []Question
	extractor := &QuestionsExtractor{}

	// Extract from document-level extensions
	if model.Extensions != nil {
		ext := orderedMapToMap(model.Extensions)
		questions = append(questions, extractor.ExtractFromOpenAPI(
			path, sourceURL, "openapi_document", "Document", ext,
		)...)
	}

	// Extract from Info extensions
	if model.Info != nil && model.Info.Extensions != nil {
		ext := orderedMapToMap(model.Info.Extensions)
		context := model.Info.Title
		questions = append(questions, extractor.ExtractFromOpenAPI(
			path, sourceURL, "openapi_info", context, ext,
		)...)
	}

	// Extract from Paths extensions
	if model.Paths != nil {
		// Extract from Paths-level extensions
		if model.Paths.Extensions != nil {
			ext := orderedMapToMap(model.Paths.Extensions)
			questions = append(questions, extractor.ExtractFromOpenAPI(
				path, sourceURL, "openapi_paths", "Paths", ext,
			)...)
		}

		// Extract from individual PathItems and Operations
		if model.Paths.PathItems != nil {
			for pathPair := model.Paths.PathItems.First(); pathPair != nil; pathPair = pathPair.Next() {
				pathKey := pathPair.Key()
				pathItem := pathPair.Value()

				// Extract from PathItem extensions
				if pathItem.Extensions != nil {
					ext := orderedMapToMap(pathItem.Extensions)
					questions = append(questions, extractor.ExtractFromOpenAPI(
						path, sourceURL, "openapi_path", pathKey, ext,
					)...)
				}

				// Extract from individual Operation extensions
				operations := extractOperations(pathItem)
				for method, operation := range operations {
					if operation.Extensions != nil {
						ext := orderedMapToMap(operation.Extensions)
						context := fmt.Sprintf("%s %s", strings.ToUpper(method), pathKey)
						if operation.OperationId != "" {
							context = operation.OperationId
						}
						questions = append(questions, extractor.ExtractFromOpenAPI(
							path, sourceURL, "openapi_operation", context, ext,
						)...)
					}
				}
			}
		}
	}

	// Extract from Schema extensions
	if model.Components != nil && model.Components.Schemas != nil {
		for schemaPair := model.Components.Schemas.First(); schemaPair != nil; schemaPair = schemaPair.Next() {
			schemaName := schemaPair.Key()
			schemaProxy := schemaPair.Value()
			schema := schemaProxy.Schema()

			if schema != nil && schema.Extensions != nil {
				ext := orderedMapToMap(schema.Extensions)
				questions = append(questions, extractor.ExtractFromOpenAPI(
					path, sourceURL, "openapi_schema", schemaName, ext,
				)...)
			}
		}
	}

	return questions
}

// orderedMapToMap converts an orderedmap of yaml.Node to a regular map.
// It extracts x-docsaf-questions and other extensions into a usable format.
func orderedMapToMap(om *orderedmap.Map[string, *yaml.Node]) map[string]any {
	if om == nil {
		return nil
	}

	result := make(map[string]any)
	for pair := om.First(); pair != nil; pair = pair.Next() {
		key := pair.Key()
		node := pair.Value()

		if node == nil {
			continue
		}

		// Decode the yaml.Node into a generic interface
		var value any
		if err := node.Decode(&value); err == nil {
			result[key] = value
		}
	}

	return result
}
