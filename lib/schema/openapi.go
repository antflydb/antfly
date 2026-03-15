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
package schema

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/kaptinlin/jsonschema"
)

// Document represents a document with an ID and metadata fields
type Document struct {
	ID     string         `json:"id"     handlebars:"id"`
	Fields map[string]any `json:"fields"`
}

func (ds *DocumentSchema) Equal(other *DocumentSchema) bool {
	if ds == nil || other == nil {
		return ds == other
	}
	return reflect.DeepEqual(ds, other)
}

func (s *TableSchema) Equal(other *TableSchema) bool {
	if s == nil || other == nil {
		return s == other
	}
	if s.DefaultType != other.DefaultType ||
		!maps.EqualFunc(s.DocumentSchemas, other.DocumentSchemas, func(v, ov DocumentSchema) bool {
			return v.Equal(&ov)
		}) {
		return false
	}
	return true
}

func (s *TableSchema) Validate() error {
	if s == nil {
		return nil
	}
	// Validate the `document_schemas` are valid JSON schemas
	compiler := jsonschema.NewCompiler()
	for typeName, schema := range s.DocumentSchemas {
		b, err := json.Marshal(schema)
		if err != nil {
			return fmt.Errorf("marshalling schema for type %q: %w", typeName, err)
		}

		// TODO (ajr) Validate that all the types in x-antfly-types are known
		_, err = compiler.Compile(b)
		if err != nil {
			return fmt.Errorf("compiling schema for type %q: %w", typeName, err)
		}
	}
	// Validate that the default type is in the schemas
	if s.DefaultType != "" {
		if _, ok := s.DocumentSchemas[s.DefaultType]; !ok {
			return fmt.Errorf("default type %q not found in document_schemas", s.DefaultType)
		}
	}
	return nil
}

// GetTTLField returns the TTL field name, defaulting to "_timestamp" if not set
func (t *TableSchema) GetTTLField() string {
	if t.TtlField != "" {
		return t.TtlField
	}
	return "_timestamp"
}

// ValidateDoc validates a document against the table schema.
// It checks:
// - _type field is valid (if present or default_type is set)
// - Document matches the schema (if enforce_types is enabled)
// - Document has the required TTL field (if ttl_duration is configured)
// Returns the document type string or an error if validation fails.
//
// NOTE: Adds the _type field to the document if default_type is used.
func (s *TableSchema) ValidateDoc(doc map[string]any) (docType string, err error) {
	if s == nil {
		return "", errors.New("table schema is nil")
	}

	// Extract and validate _type
	var typeStr string
	typeVal, hasType := doc["_type"]
	if hasType {
		var ok bool
		typeStr, ok = typeVal.(string)
		if !ok {
			return "", errors.New("document _type field must be a string")
		}
	} else if s.DefaultType != "" {
		typeStr = s.DefaultType
		// NOTE: Side effect: set the _type field in the document
		doc["_type"] = typeStr
	}

	// Check type enforcement
	if s.EnforceTypes {
		if typeStr == "" {
			return "", errors.New(
				"document missing _type and no default_type is set for table, but type enforcement is on",
			)
		}
		if _, ok := s.DocumentSchemas[typeStr]; !ok {
			return "", fmt.Errorf(
				"document _type %q not found in schemas and type enforcement is on",
				typeStr,
			)
		}
	}

	// Validate TTL field if ttl_duration is configured
	if s.TtlDuration != "" {
		ttlField := s.GetTTLField()
		if _, ok := doc[ttlField]; !ok {
			return "", fmt.Errorf(
				"document missing TTL field %q (required when ttl_duration is set)",
				ttlField,
			)
		}
	}

	return typeStr, nil
}

// IsDocumentExpired checks if a document has expired based on the table's TTL configuration.
// Returns true if the document is expired, false otherwise.
// Returns error if TTL field is missing or cannot be parsed.
// If TTL is not configured for the table, returns false (document never expires).
func (s *TableSchema) IsDocumentExpired(doc map[string]any, now time.Time) (bool, error) {
	// No TTL configured - document never expires
	if s.TtlDuration == "" {
		return false, nil
	}

	// Parse TTL duration
	ttlDuration, err := time.ParseDuration(s.TtlDuration)
	if err != nil {
		return false, fmt.Errorf("parsing TTL duration %q: %w", s.TtlDuration, err)
	}

	// Get the TTL reference field
	ttlField := s.GetTTLField()

	// Get the timestamp field value
	timestampVal, ok := doc[ttlField]
	if !ok {
		// Document missing TTL field
		return false, fmt.Errorf("document missing TTL field %q", ttlField)
	}

	// Parse timestamp (should be in RFC3339Nano format)
	timestampStr, ok := timestampVal.(string)
	if !ok {
		return false, fmt.Errorf("TTL field %q is not a string", ttlField)
	}

	timestamp, err := time.Parse(time.RFC3339Nano, timestampStr)
	if err != nil {
		// Try parsing with RFC3339 (without nano precision)
		timestamp, err = time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			return false, fmt.Errorf("parsing timestamp %q: %w", timestampStr, err)
		}
	}

	// Calculate expiration time
	expirationTime := timestamp.Add(ttlDuration)

	return now.After(expirationTime), nil
}
