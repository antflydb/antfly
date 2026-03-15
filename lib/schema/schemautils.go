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

package schema

import (
	"slices"
	"strings"
)

// GetFieldTypeFromSchema navigates nested schema to find field type annotations.
// fieldPath: ["metadata", "author", "avatar"]
// Returns: []AntflyType from x-antfly-types extension
func GetFieldTypeFromSchema(
	schema *TableSchema,
	docType string,
	fieldPath []string,
) []AntflyType {
	if schema == nil || len(fieldPath) == 0 {
		return nil
	}

	// Get the document schema for the specified type
	docSchema, ok := schema.DocumentSchemas[docType]
	if !ok {
		return nil
	}

	// Navigate through the JSON Schema
	return getFieldTypesFromJSONSchema(docSchema.Schema, fieldPath)
}

// getFieldTypesFromJSONSchema recursively navigates a JSON Schema to find x-antfly-types
func getFieldTypesFromJSONSchema(schema map[string]any, fieldPath []string) []AntflyType {
	if len(fieldPath) == 0 {
		return nil
	}

	// Get properties object
	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		// If no properties, check if additionalProperties allows dynamic fields
		if additionalProps, ok := schema["additionalProperties"].(map[string]any); ok {
			// Try to get types from additionalProperties schema
			return extractAntflyTypes(additionalProps)
		}
		return nil
	}

	// Get the field definition
	fieldName := fieldPath[0]
	fieldDef, ok := properties[fieldName].(map[string]any)
	if !ok {
		// Field not found in properties, check additionalProperties
		if additionalProps, ok := schema["additionalProperties"].(map[string]any); ok {
			return extractAntflyTypes(additionalProps)
		}
		return nil
	}

	// If this is the last segment of the path, extract types
	if len(fieldPath) == 1 {
		return extractAntflyTypes(fieldDef)
	}

	// Otherwise, recurse into nested object
	return getFieldTypesFromJSONSchema(fieldDef, fieldPath[1:])
}

// extractAntflyTypes extracts x-antfly-types from a field definition
func extractAntflyTypes(fieldDef map[string]any) []AntflyType {
	typesRaw, ok := fieldDef["x-antfly-types"]
	if !ok {
		return nil
	}

	// x-antfly-types can be []string or []any
	switch types := typesRaw.(type) {
	case []string:
		result := make([]AntflyType, len(types))
		for i, t := range types {
			result[i] = AntflyType(t)
		}
		return result
	case []any:
		var result []AntflyType
		for _, t := range types {
			if str, ok := t.(string); ok {
				result = append(result, AntflyType(str))
			}
		}
		return result
	default:
		return nil
	}
}

// HasAntflyType checks if a field has a specific type annotation.
func HasAntflyType(
	schema *TableSchema,
	docType string,
	fieldPath []string,
	targetType AntflyType,
) bool {
	types := GetFieldTypeFromSchema(schema, docType, fieldPath)
	return slices.Contains(types, targetType)
}

// TraverseDocumentFields recursively walks document structure.
// Yields (fieldPath, value) pairs for all fields including nested ones.
func TraverseDocumentFields(
	doc map[string]any,
	callback func(path []string, value any),
) {
	traverseDocumentFieldsRecursive(doc, []string{}, callback)
}

// traverseDocumentFieldsRecursive is the internal recursive implementation
func traverseDocumentFieldsRecursive(
	obj map[string]any,
	currentPath []string,
	callback func(path []string, value any),
) {
	for key, value := range obj {
		newPath := append(currentPath, key)

		// Call callback for this field
		callback(newPath, value)

		// Recurse into nested maps
		if nested, ok := value.(map[string]any); ok {
			traverseDocumentFieldsRecursive(nested, newPath, callback)
		}
	}
}

// SetFieldValue sets a value at a specific field path in a document
func SetFieldValue(doc map[string]any, fieldPath []string, value any) {
	if len(fieldPath) == 0 {
		return
	}

	// Navigate to the parent of the target field
	current := doc
	for i := 0; i < len(fieldPath)-1; i++ {
		fieldName := fieldPath[i]

		// Get or create nested map
		if nested, ok := current[fieldName].(map[string]any); ok {
			current = nested
		} else {
			// Create new nested map
			nested := make(map[string]any)
			current[fieldName] = nested
			current = nested
		}
	}

	// Set the value at the final field
	current[fieldPath[len(fieldPath)-1]] = value
}

// GetFieldValue gets a value at a specific field path in a document
func GetFieldValue(doc map[string]any, fieldPath []string) any {
	if len(fieldPath) == 0 {
		return nil
	}

	current := any(doc)
	for _, fieldName := range fieldPath {
		if m, ok := current.(map[string]any); ok {
			current = m[fieldName]
		} else {
			return nil
		}
	}

	return current
}

// NormalizeFieldPath removes common prefixes like "this" and "fields" for template matching
func NormalizeFieldPath(path []string) []string {
	if len(path) == 0 {
		return path
	}

	// Remove "this" prefix
	if path[0] == "this" {
		path = path[1:]
	}

	// Remove "fields" prefix (commonly used in templates like {{this.fields.title}})
	if len(path) > 0 && path[0] == "fields" {
		path = path[1:]
	}

	return path
}

// IsFieldInList checks if a field path matches any path in the reference list
func IsFieldInList(fieldPath []string, referencedFields [][]string) bool {
	normalized := NormalizeFieldPath(fieldPath)
	normalizedKey := strings.Join(normalized, ".")

	for _, refPath := range referencedFields {
		refNormalized := NormalizeFieldPath(refPath)
		refKey := strings.Join(refNormalized, ".")

		if normalizedKey == refKey {
			return true
		}
	}

	return false
}
