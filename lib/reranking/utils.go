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

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
package reranking

import (
	"fmt"
	"maps"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/template"
)

// ExtractDocumentText extracts text from a document using either field or template.
// If template is provided, it takes precedence over field.
// Returns an error if neither is provided or if extraction fails.
func ExtractDocumentText(doc schema.Document, field, tmpl string) (string, error) {
	if tmpl != "" {
		return extractWithTemplate(doc, tmpl)
	}
	if field != "" {
		return extractWithField(doc, field)
	}
	return "", fmt.Errorf("neither field nor template specified for document %s", doc.ID)
}

// extractWithTemplate renders a handlebars template with the document as context
func extractWithTemplate(doc schema.Document, tmpl string) (string, error) {
	// Create context map with document fields and ID
	context := make(map[string]any)
	context["id"] = doc.ID

	// Add all fields to the context
	maps.Copy(context, doc.Fields)

	// Render the template
	result, err := template.Render(tmpl, context)
	if err != nil {
		return "", fmt.Errorf("rendering template for document %s: %w", doc.ID, err)
	}

	return result, nil
}

// extractWithField extracts a single field from the document
func extractWithField(doc schema.Document, field string) (string, error) {
	value, ok := doc.Fields[field]
	if !ok {
		return "", fmt.Errorf("field %s not found in document %s", field, doc.ID)
	}

	// Convert to string
	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("field %s in document %s is not a string (type: %T)", field, doc.ID, value)
	}

	return str, nil
}

// ExtractDocumentTexts extracts text from multiple documents using the same field or template.
// Returns a slice of strings in the same order as the input documents.
// If any extraction fails, returns an error.
func ExtractDocumentTexts(docs []schema.Document, field, tmpl string) ([]string, error) {
	texts := make([]string, len(docs))
	for i, doc := range docs {
		text, err := ExtractDocumentText(doc, field, tmpl)
		if err != nil {
			return nil, fmt.Errorf("extracting text from document %d: %w", i, err)
		}
		texts[i] = text
	}
	return texts, nil
}
