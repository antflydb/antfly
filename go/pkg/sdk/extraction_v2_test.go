// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"github.com/stretchr/testify/require"
)

func TestExtractionV2RequestPresenceAndLegacyCompatibility(t *testing.T) {
	zero, disabled := float32(0), false
	splitter := oapi.ExtractionOptionsWordSplitterChar
	dtype := oapi.ExtractionStructureField1Dtype("list")
	var field oapi.ExtractionStructureField
	require.NoError(t, field.FromExtractionStructureField1(oapi.ExtractionStructureField1{Dtype: &dtype}))
	schema := oapi.ExtractionSchema{
		Entities:        []string{"person"},
		Classifications: []oapi.ExtractionClassificationSchema{{Name: "t", Labels: []string{"a", "b"}, MaxLabels: json.RawMessage("null"), Ordered: &disabled}},
		Structures:      map[string]oapi.ExtractionStructureSchema{"s": {Fields: map[string]oapi.ExtractionStructureField{"tags": field}}},
	}
	body, err := json.Marshal(ExtractionV2Request{
		Model: "m", Schema: schema,
		Options: &ExtractionV2Options{Threshold: &zero, IncludeSpans: &disabled, WordSplitter: &splitter},
		Inputs:  []ExtractionV2Input{{Content: oapi.ChatMessageContent(`"Ada"`), Options: &ExtractionV2Options{}}, {Content: oapi.ChatMessageContent(`"Bob"`)}},
	})
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(body, &wire))
	require.Equal(t, float64(2), wire["schema_version"])
	require.Equal(t, map[string]any{"threshold": float64(0), "include_spans": false, "word_splitter": "char"}, wire["options"])
	inputs := wire["inputs"].([]any)
	require.Equal(t, map[string]any{}, inputs[0].(map[string]any)["options"])
	require.NotContains(t, inputs[1].(map[string]any), "options")
	compiled := wire["schema"].(map[string]any)
	classification := compiled["classifications"].([]any)[0].(map[string]any)
	require.Contains(t, classification, "max_labels")
	require.Nil(t, classification["max_labels"])
	require.Equal(t, false, classification["ordered"])
	require.NotContains(t, classification, "hypothesis_template")
	tags := compiled["structures"].(map[string]any)["s"].(map[string]any)["fields"].(map[string]any)["tags"].(map[string]any)
	require.Equal(t, "list", tags["type"])
	original, err := schema.Structures["s"].Fields["tags"].AsExtractionStructureField1()
	require.NoError(t, err)
	require.Empty(t, original.Type)
	legacy, err := json.Marshal(oapi.ExtractionRequest{Model: "m", Inputs: []oapi.ExtractionInput{{Content: oapi.ChatMessageContent(`"Ada"`)}}, Schema: oapi.ExtractionSchema{Entities: []string{"person"}}})
	require.NoError(t, err)
	for _, name := range []string{"schema_version", "joint_ie", "entity_attributes", "classification_constraints", "decoder", "long_document", "word_splitter"} {
		require.NotContains(t, string(legacy), `"`+name+`"`)
	}
}

func TestExtractionV2DecoderPreservesModelDefaultAndExplicitAlgorithms(t *testing.T) {
	width := 16
	for _, selector := range []string{"", "auto", "exact", "beam"} {
		t.Run(selector, func(t *testing.T) {
			decoder := oapi.ExtractionDecoderOptions{BeamWidth: &width}
			if selector != "" {
				algorithm := oapi.ExtractionDecoderOptionsAlgorithm(selector)
				decoder.Algorithm = &algorithm
			}
			body, err := json.Marshal(ExtractionV2Options{Decoder: &decoder})
			require.NoError(t, err)
			var wire map[string]any
			require.NoError(t, json.Unmarshal(body, &wire))
			expected := map[string]any{"beam_width": float64(16)}
			if selector != "" {
				expected["algorithm"] = selector
			}
			require.Equal(t, expected, wire["decoder"])
		})
	}
}

func TestExtractionV2ResponseKeepsMissingOffsetsAndEntityZeroDistinct(t *testing.T) {
	var response ExtractionV2Response
	require.NoError(t, json.Unmarshal([]byte(`{"object":"extraction","schema_version":2,"model":"m","data":[{"offset_unit":"utf8_bytes","relations":[{"type":"r","source":{"text":"Ada","entity_index":0,"start":0,"end":3},"target":{"text":"company"}}],"structures":{"s":[{"status":{"value":"yes","source":"schema"},"tags":[]}]}}],"usage":{"prompt_tokens":4,"completion_tokens":0,"total_tokens":4}}`), &response))
	relation := response.Data[0].Relations[0]
	require.NotNil(t, relation.Source.EntityIndex)
	require.Zero(t, *relation.Source.EntityIndex)
	require.NotNil(t, relation.Source.Start)
	require.Nil(t, relation.Target.EntityIndex)
	require.Nil(t, relation.Target.Start)
	record := response.Data[0].Structures["s"][0]
	require.False(t, record["status"].List)
	require.Nil(t, record["status"].Values[0].Start)
	require.True(t, record["tags"].List)
	require.Empty(t, record["tags"].Values)
	roundtrip, err := json.Marshal(record)
	require.NoError(t, err)
	require.JSONEq(t, `{"status":{"value":"yes","source":"schema"},"tags":[]}`, string(roundtrip))
}

func TestExtractionV2HTTPFailurePreservesAtomicLocation(t *testing.T) {
	transport := &http.Client{Transport: inferenceRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "/ai/v1/extract", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, float64(2), body["schema_version"])
		return &http.Response{StatusCode: http.StatusUnprocessableEntity, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":"EXTRACTION_SEARCH_EXHAUSTED","message":"no accepted witness","input_index":0,"stage":"decode"}`))}, nil
	})}
	client, err := NewInferenceClient("http://test", transport)
	require.NoError(t, err)
	_, err = client.ExtractV2(context.Background(), ExtractionV2Request{Model: "m", Schema: oapi.ExtractionSchema{Entities: []string{"p"}}, Inputs: []ExtractionV2Input{{Content: oapi.ChatMessageContent(`"Ada"`)}}})
	var failure *InferenceAPIError
	require.True(t, errors.As(err, &failure))
	require.Equal(t, "EXTRACTION_SEARCH_EXHAUSTED", failure.Code)
	require.Equal(t, "decode", failure.Stage)
	require.NotNil(t, failure.InputIndex)
	require.Zero(t, *failure.InputIndex)
}

func TestExtractionV2LongDocumentMetadataAndRecordDiagnostics(t *testing.T) {
	mode := oapi.ExtractionLongDocumentOptionsMode("window")
	identity := oapi.ExtractionLongDocumentOptionsRecordIdentity("semantic")
	request, err := json.Marshal(ExtractionV2Request{
		Model: "m", Schema: oapi.ExtractionSchema{Entities: []string{"person"}},
		Inputs: []ExtractionV2Input{{Content: oapi.ChatMessageContent(`"Ada"`)}},
		Options: &ExtractionV2Options{LongDocument: &oapi.ExtractionLongDocumentOptions{
			Mode: &mode, RecordIdentity: &identity,
		}},
	})
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(request, &wire))
	require.Equal(t, map[string]any{"mode": "window", "record_identity": "semantic"}, wire["options"].(map[string]any)["long_document"])
	const object = `{"offset_unit":"utf8_bytes","long_document":{"version":1,"window_count":3,"window_policy":"source_words_midpoint_ownership","classification_aggregation":"owned_word_weighted_mean_raw_logits","duplicate_score":"maximum_calibrated_score","natural_record_identity":"exact_source_anchor","other_record_identity":"semantic","solver_optimality_scope":"retained_candidate_graph"},"solvers":{"records":{"status":"feasible","utility":1.25,"visited_nodes":0,"exhausted":true}}}`
	var result ExtractionV2Object
	require.NoError(t, json.Unmarshal([]byte(object), &result))
	require.NotNil(t, result.LongDocument)
	require.Equal(t, 3, result.LongDocument.WindowCount)
	require.Equal(t, "semantic", string(result.LongDocument.OtherRecordIdentity))
	require.NotNil(t, result.Solvers)
	require.NotNil(t, result.Solvers.Records)
	require.True(t, result.Solvers.Records.Exhausted)
	roundtrip, err := json.Marshal(result)
	require.NoError(t, err)
	require.JSONEq(t, object, string(roundtrip))
}

func extractionV2ResponseClient(t *testing.T, body string) *InferenceClient {
	t.Helper()
	transport := &http.Client{Transport: inferenceRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	client, err := NewInferenceClient("http://test", transport)
	require.NoError(t, err)
	return client
}

func TestExtractionV2RejectsInvalidResponseEnvelope(t *testing.T) {
	a, b := "a", "b"
	request := ExtractionV2Request{Model: "m", Schema: oapi.ExtractionSchema{Entities: []string{"person"}}, Inputs: []ExtractionV2Input{
		{ID: &a, Content: oapi.ChatMessageContent(`"Ada"`)},
		{ID: &b, Content: oapi.ChatMessageContent(`"Bob"`)},
	}}
	base := func() map[string]any {
		return map[string]any{"object": "extraction", "model": "m", "schema_version": 2, "data": []any{map[string]any{"id": "a"}, map[string]any{"id": "b"}}}
	}
	for _, test := range []struct {
		name  string
		field string
		value any
	}{
		{"object missing", "object", nil},
		{"object wrong", "object", "embedding"},
		{"object number", "object", 2},
		{"version missing", "schema_version", nil},
		{"version wrong", "schema_version", 1},
		{"version string", "schema_version", "2"},
		{"version fractional", "schema_version", 2.5},
		{"model missing", "model", nil},
		{"model wrong", "model", "other/model"},
		{"model number", "model", 4},
		{"data missing", "data", nil},
		{"data object", "data", map[string]any{}},
		{"data empty", "data", []any{}},
		{"data partial", "data", []any{map[string]any{"id": "a"}}},
		{"data extra", "data", []any{map[string]any{"id": "a"}, map[string]any{"id": "b"}, map[string]any{}}},
		{"null row", "data", []any{nil, map[string]any{"id": "b"}}},
		{"array row", "data", []any{[]any{}, map[string]any{"id": "b"}}},
		{"string row", "data", []any{"a", map[string]any{"id": "b"}}},
		{"missing explicit id", "data", []any{map[string]any{}, map[string]any{"id": "b"}}},
		{"wrong id", "data", []any{map[string]any{"id": "other"}, map[string]any{"id": "b"}}},
		{"reordered ids", "data", []any{map[string]any{"id": "b"}, map[string]any{"id": "a"}}},
		{"null explicit id", "data", []any{map[string]any{"id": nil}, map[string]any{"id": "b"}}},
		{"numeric id", "data", []any{map[string]any{"id": 0}, map[string]any{"id": "b"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := base()
			if test.value == nil {
				delete(payload, test.field)
			} else {
				payload[test.field] = test.value
			}
			body, err := json.Marshal(payload)
			require.NoError(t, err)
			response, err := extractionV2ResponseClient(t, string(body)).ExtractV2(context.Background(), request)
			require.Error(t, err)
			require.Nil(t, response)
		})
	}
	for _, body := range []string{`null`, `[]`, `"extraction"`, `true`} {
		t.Run("nonobject "+body, func(t *testing.T) {
			response, err := extractionV2ResponseClient(t, body).ExtractV2(context.Background(), request)
			require.Error(t, err)
			require.Nil(t, response)
		})
	}
}

func TestExtractionV2ResponseIDsArePositionalAndPreserveRawExtensions(t *testing.T) {
	repeated, empty := "repeat", ""
	request := ExtractionV2Request{Model: "m", Schema: oapi.ExtractionSchema{Entities: []string{"person"}}, Inputs: []ExtractionV2Input{
		{ID: &repeated, Content: oapi.ChatMessageContent(`"Ada"`)},
		{ID: &repeated, Content: oapi.ChatMessageContent(`"Bob"`)},
		{ID: &empty, Content: oapi.ChatMessageContent(`"Eve"`)},
		{Content: oapi.ChatMessageContent(`"Max"`)},
	}}
	const body = `{"object":"extraction","model":"m","schema_version":2,"future":{"enabled":true},"data":[{"id":"repeat","offset_unit":"utf8_bytes","entities":[{"label":"person","text":"Ada","start":0,"end":3,"future_entity":7}],"future_row":{"index":0}},{"id":"repeat","offset_unit":"utf8_bytes","entities":[{"label":"person","text":"Bob"}]},{"id":"","offset_unit":"utf8_bytes"},{"offset_unit":"utf8_bytes"}]}`
	response, err := extractionV2ResponseClient(t, body).ExtractV2(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, body, string(response.Raw))
	require.Equal(t, "Ada", response.Data[0].Entities[0].Text)
	require.Equal(t, "Bob", response.Data[1].Entities[0].Text)
	require.NotNil(t, response.Data[0].Entities[0].Start)
	require.Zero(t, *response.Data[0].Entities[0].Start)
	require.Nil(t, response.Data[1].Entities[0].Start)
	require.NotNil(t, response.Data[2].ID)
	require.Empty(t, *response.Data[2].ID)
	require.Nil(t, response.Data[3].ID)
	for _, id := range []string{`"unexpected"`, `null`, `0`, `false`, `[]`, `{}`} {
		t.Run("anonymous id "+id, func(t *testing.T) {
			anonymous := ExtractionV2Request{Model: "m", Schema: request.Schema, Inputs: []ExtractionV2Input{request.Inputs[3]}}
			invalid := `{"object":"extraction","model":"m","schema_version":2,"data":[{"id":` + id + `}]}`
			response, err := extractionV2ResponseClient(t, invalid).ExtractV2(context.Background(), anonymous)
			require.Error(t, err)
			require.Nil(t, response)
		})
	}
	emptyRequest := ExtractionV2Request{Model: "m", Schema: request.Schema, Inputs: []ExtractionV2Input{request.Inputs[2]}}
	_, err = extractionV2ResponseClient(t, `{"object":"extraction","model":"m","schema_version":2,"data":[{}]}`).ExtractV2(context.Background(), emptyRequest)
	require.Error(t, err, "an omitted ID must not match an explicitly empty ID")
}
