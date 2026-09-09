// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package oapi

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestAgentCapacityResponseContract(t *testing.T) {
	const body = `{"error":"GenerationCapacityUnavailable","message":"inference capacity temporarily unavailable","reason":"inference_capacity","retryable":true,"retry_after_ms":1000}`
	newResponse := func() *http.Response {
		return &http.Response{StatusCode: 503, Header: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"1"}}, Body: io.NopCloser(strings.NewReader(body))}
	}
	doc, err := GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	var payload any
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/db/v1/agents/retrieval", "/db/v1/agents/query-builder"} {
		t.Run(path, func(t *testing.T) {
			schema := doc.Paths.Value(path).Post.Responses.Value("503").Value.Content["application/json"].Schema.Value
			if err := schema.VisitJSON(payload); err != nil {
				t.Fatalf("public 503 schema rejected capacity envelope: %v", err)
			}
		})
	}
	if err := doc.Components.Schemas["SSEError"].Value.VisitJSON(payload); err != nil {
		t.Fatalf("SSE schema rejected capacity envelope: %v", err)
	}

	retrieval, err := ParseRetrievalAgentResponse(newResponse())
	if err != nil {
		t.Fatal(err)
	}
	builder, err := ParseQueryBuilderAgentResponse(newResponse())
	if err != nil {
		t.Fatal(err)
	}
	if retrieval.JSON503 == nil || builder.JSON503 == nil {
		t.Fatal("capacity response was not parsed as a declared 503")
	}
	if retrieval.Headers503 == nil || builder.Headers503 == nil || retrieval.Headers503.RetryAfter != 1 || builder.Headers503.RetryAfter != 1 {
		t.Fatal("missing typed retry delay header")
	}
	for _, response := range []AgentTemporarilyUnavailable{*retrieval.JSON503, *builder.JSON503} {
		capacity, err := response.AsInferenceCapacityError()
		if err != nil {
			t.Fatal(err)
		}
		if capacity.Error != "GenerationCapacityUnavailable" || !capacity.Retryable || capacity.RetryAfterMs != 1000 || capacity.Reason != "inference_capacity" || capacity.Message == "" {
			t.Fatalf("capacity details lost: %#v", capacity)
		}
	}
}

func TestAgentDependencyUnavailableResponseContract(t *testing.T) {
	doc, err := GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"doc_identity_unavailable", "query_embedding_temporarily_unavailable"} {
		t.Run(code, func(t *testing.T) {
			payload := map[string]any{"code": code, "message": "temporarily unavailable", "retryable": true}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"/db/v1/agents/retrieval", "/db/v1/agents/query-builder"} {
				schema := doc.Paths.Value(path).Post.Responses.Value("503").Value.Content["application/json"].Schema.Value
				if err := schema.VisitJSON(payload); err != nil {
					t.Fatal(err)
				}
			}
			response := &http.Response{StatusCode: 503, Header: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"1"}}, Body: io.NopCloser(strings.NewReader(string(body)))}
			parsed, err := ParseQueryBuilderAgentResponse(response)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.JSON503 == nil || parsed.Headers503 == nil || parsed.Headers503.RetryAfter != 1 {
				t.Fatal("missing typed 503 response or retry header")
			}
			failure, err := parsed.JSON503.AsQueryTemporarilyUnavailableError()
			if err != nil {
				t.Fatal(err)
			}
			if string(failure.Code) != code || !failure.Retryable || failure.Message == "" {
				t.Fatalf("dependency retry metadata lost: %#v", failure)
			}
		})
	}
}
