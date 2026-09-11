// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// ExtractionV2Options preserves omitted values, including an explicit zero
// threshold and empty per-input replacement. The existing ExtractionOptions
// remains unchanged for source compatibility with legacy extraction callers.
type ExtractionV2Options struct {
	Threshold         *float32                            `json:"threshold,omitempty"`
	IncludeConfidence *bool                               `json:"include_confidence,omitempty"`
	IncludeSpans      *bool                               `json:"include_spans,omitempty"`
	WordSplitter      *oapi.ExtractionOptionsWordSplitter `json:"word_splitter,omitempty"`
	Overlap           *oapi.ExtractionOptionsOverlap      `json:"overlap,omitempty"`
	OffsetUnit        *oapi.ExtractionOffsetUnit          `json:"offset_unit,omitempty"`
	LongDocument      *oapi.ExtractionLongDocumentOptions `json:"long_document,omitempty"`
	Decoder           *oapi.ExtractionDecoderOptions      `json:"decoder,omitempty"`
	JointIE           *oapi.ExtractionJointOptions        `json:"joint_ie,omitempty"`
}

// ExtractionV2Input applies whole schema/options replacements when non-nil.
// Content is a JSON string or an array of text content parts. Part text is
// joined with a newline before inference and source offsets use that text.
type ExtractionV2Input struct {
	ID       *string                 `json:"id,omitempty"`
	Content  oapi.ChatMessageContent `json:"content"`
	Metadata map[string]any          `json:"metadata,omitempty"`
	Schema   *oapi.ExtractionSchema  `json:"schema,omitempty"`
	Options  *ExtractionV2Options    `json:"options,omitempty"`
}

// ExtractionV2Request opts into the strict mixed-task protocol. Ordinary
// tasks share an encoding; schema.joint_ie is mutually exclusive with them.
type ExtractionV2Request struct {
	Model   string
	Inputs  []ExtractionV2Input
	Schema  oapi.ExtractionSchema
	Options *ExtractionV2Options
}

// ExtractionV2Response preserves optional offsets and endpoint references:
// an absent entity_index is distinct from a reference to entity zero.
type ExtractionV2Response struct {
	// Raw retains the complete, owned JSON response returned by ExtractV2,
	// including extensions not represented by the typed fields. Marshaling
	// the typed response does not include this original snapshot.
	Raw           json.RawMessage      `json:"-"`
	Object        string               `json:"object"`
	Model         string               `json:"model"`
	SchemaVersion int                  `json:"schema_version"`
	Data          []ExtractionV2Object `json:"data"`
	Usage         struct {
		PromptTokens     uint64 `json:"prompt_tokens"`
		CompletionTokens uint64 `json:"completion_tokens"`
		TotalTokens      uint64 `json:"total_tokens"`
	} `json:"usage"`
}
type ExtractionV2Object struct {
	ID                *string                                    `json:"id,omitempty"`
	OffsetUnit        oapi.ExtractionOffsetUnit                  `json:"offset_unit"`
	Entities          []ExtractionV2Entity                       `json:"entities,omitempty"`
	Classifications   []ExtractionV2Classification               `json:"classifications,omitempty"`
	Relations         []ExtractionV2Relation                     `json:"relations,omitempty"`
	Structures        map[string][]map[string]ExtractionV2Field  `json:"structures,omitempty"`
	StructureMetadata map[string][]oapi.ExtractionRecordMetadata `json:"structure_metadata,omitempty"`
	Solvers           *oapi.ExtractionSolverDiagnostics          `json:"solvers,omitempty"`
	LongDocument      *oapi.ExtractionLongDocumentMetadata       `json:"long_document,omitempty"`
}

// A missing ID denotes an anonymous input. A present ID must be a string;
// decoding directly into *string would incorrectly accept JSON null.
func (o *ExtractionV2Object) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return fmt.Errorf("extraction v2 response item must be an object")
	}
	type object ExtractionV2Object
	var wire struct {
		object
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if len(wire.ID) != 0 {
		if bytes.Equal(bytes.TrimSpace(wire.ID), []byte("null")) {
			return fmt.Errorf("extraction v2 response item id must be a string")
		}
		var id string
		if err := json.Unmarshal(wire.ID, &id); err != nil {
			return fmt.Errorf("extraction v2 response item id must be a string: %w", err)
		}
		wire.object.ID = &id
	}
	*o = ExtractionV2Object(wire.object)
	return nil
}

type ExtractionV2Entity struct {
	Label      string                                       `json:"label"`
	Text       string                                       `json:"text"`
	Score      *float64                                     `json:"score,omitempty"`
	Start      *int                                         `json:"start,omitempty"`
	End        *int                                         `json:"end,omitempty"`
	Attributes map[string]oapi.ExtractionAttributeSelection `json:"attributes,omitempty"`
}
type ExtractionV2Classification struct {
	Name  string   `json:"name"`
	Label string   `json:"label"`
	Score *float64 `json:"score,omitempty"`
}
type ExtractionV2Endpoint struct {
	EntityIndex *int     `json:"entity_index,omitempty"`
	Label       *string  `json:"label,omitempty"`
	Text        string   `json:"text"`
	Score       *float64 `json:"score,omitempty"`
	Start       *int     `json:"start,omitempty"`
	End         *int     `json:"end,omitempty"`
}
type ExtractionV2Relation struct {
	Type    string               `json:"type"`
	Source  ExtractionV2Endpoint `json:"source"`
	Target  ExtractionV2Endpoint `json:"target"`
	Score   *float64             `json:"score,omitempty"`
	Derived bool                 `json:"derived,omitempty"`
}
type ExtractionV2FieldValue struct {
	Value  string   `json:"value"`
	Source string   `json:"source"`
	Score  *float64 `json:"score,omitempty"`
	Start  *int     `json:"start,omitempty"`
	End    *int     `json:"end,omitempty"`
}

// ExtractionV2Field is a scalar (one Values item) or a list, including an
// empty list. Source="schema" enum values intentionally have no source offsets.
type ExtractionV2Field struct {
	List   bool
	Values []ExtractionV2FieldValue
}

func (f *ExtractionV2Field) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	var result ExtractionV2Field
	if len(data) > 0 && data[0] == '[' {
		result.List = true
		if err := json.Unmarshal(data, &result.Values); err != nil {
			return err
		}
	} else if len(data) > 0 && data[0] == '{' {
		var value ExtractionV2FieldValue
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		result.Values = []ExtractionV2FieldValue{value}
	} else {
		return fmt.Errorf("extraction field must be a value object or list")
	}
	*f = result
	return nil
}
func (f ExtractionV2Field) MarshalJSON() ([]byte, error) {
	if f.List {
		if f.Values == nil {
			return []byte("[]"), nil
		}
		return json.Marshal(f.Values)
	}
	if len(f.Values) != 1 {
		return nil, fmt.Errorf("scalar extraction field must have one value")
	}
	return json.Marshal(f.Values[0])
}

// MarshalJSON always writes schema_version:2 and retains replacement presence.
func (r ExtractionV2Request) MarshalJSON() ([]byte, error) {
	if strings.TrimSpace(r.Model) == "" || !utf8.ValidString(r.Model) || len(r.Inputs) == 0 {
		return nil, fmt.Errorf("extraction v2 requires a model and at least one input")
	}
	shared, err := extractionV2Schema(r.Schema)
	if err != nil {
		return nil, err
	}
	inputs := append([]ExtractionV2Input(nil), r.Inputs...)
	for i := range inputs {
		item := &inputs[i]
		if !utf8.Valid(item.Content) || !json.Valid(item.Content) || (item.ID != nil && !utf8.ValidString(*item.ID)) {
			return nil, fmt.Errorf("extraction v2 input %d has invalid UTF-8 or content JSON", i)
		}
		if item.Schema != nil {
			replacement, err := extractionV2Schema(*item.Schema)
			if err != nil {
				return nil, fmt.Errorf("extraction v2 input %d: %w", i, err)
			}
			item.Schema = &replacement
		}
	}
	return json.Marshal(struct {
		Model         string                `json:"model"`
		SchemaVersion int                   `json:"schema_version"`
		Inputs        []ExtractionV2Input   `json:"inputs"`
		Schema        oapi.ExtractionSchema `json:"schema"`
		Options       *ExtractionV2Options  `json:"options,omitempty"`
	}{r.Model, 2, inputs, shared, r.Options})
}

// The legacy generated record-field marshaller always writes its non-pointer
// type, including an empty Go zero value. Resolve that absent type using the
// canonical dtype/default without mutating caller-owned maps or slices.
func extractionV2Schema(input oapi.ExtractionSchema) (oapi.ExtractionSchema, error) {
	output := input
	if input.Structures == nil {
		return output, nil
	}
	output.Structures = make(map[string]oapi.ExtractionStructureSchema, len(input.Structures))
	for name, structure := range input.Structures {
		fields := make(map[string]oapi.ExtractionStructureField, len(structure.Fields))
		for fieldName, union := range structure.Fields {
			encoded, err := json.Marshal(union)
			if err != nil {
				return output, fmt.Errorf("extraction field %s.%s: %w", name, fieldName, err)
			}
			if len(encoded) > 0 && encoded[0] == '{' {
				field, err := union.AsExtractionStructureField1()
				if err != nil {
					return output, err
				}
				if field.Type == "" {
					field.Type = "str"
					if field.Dtype != nil {
						field.Type = oapi.ExtractionStructureField1Type(*field.Dtype)
					}
					if err := union.FromExtractionStructureField1(field); err != nil {
						return output, err
					}
				}
			}
			fields[fieldName] = union
		}
		structure.Fields = fields
		output.Structures[name] = structure
	}
	return output, nil
}

// ExtractV2 submits an atomic version 2 request. Search exhaustion is an error
// unless best_effort explicitly permits a valid witness; diagnostics preserve
// whether a returned witness exhausted its budget.
func (c *InferenceClient) ExtractV2(ctx context.Context, req ExtractionV2Request) (*ExtractionV2Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encoding extraction v2 request: %w", err)
	}
	resp, err := c.client.ExtractWithBodyWithResponse(ctx, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}
	if err := inferenceResponseErrorWithCapacity(resp.StatusCode(), resp.Body, resp.JSON503); err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode(), string(resp.Body))
	}
	var result ExtractionV2Response
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("decoding extraction v2 response: %w", err)
	}
	if result.Object != "extraction" || result.SchemaVersion != 2 || len(result.Data) != len(req.Inputs) {
		return nil, fmt.Errorf("invalid extraction v2 response version or item cardinality")
	}
	if result.Model != req.Model {
		return nil, fmt.Errorf("invalid extraction v2 response model")
	}
	for i, input := range req.Inputs {
		actual := result.Data[i].ID
		if (input.ID == nil) != (actual == nil) || (input.ID != nil && *input.ID != *actual) {
			return nil, fmt.Errorf("invalid extraction v2 response item %d id", i)
		}
	}
	// The generated response owns Body and is not exposed to the caller, so
	// transfer that allocation without copying the already bounded payload.
	result.Raw = resp.Body
	return &result, nil
}
