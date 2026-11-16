// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sdk

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"io"
	"net/http"
)

type GraphCatalogEntry = oapi.GraphCatalogEntry
type GraphDefinition = oapi.GraphDefinition
type GraphQueryRequest = oapi.GraphQueryRequest
type GraphQueryResult = oapi.GraphQueryResult
type GraphValue = oapi.GraphWireValue
type GraphWriteReceipt = oapi.GraphWriteReceipt
type GraphNoReplayDoer = oapi.GraphNoReplayDoer

// CypherExecutionError retains diagnostics and write delivery outcomes.
type CypherExecutionError struct {
	StatusCode int
	Code       string `json:"code"`
	Message    string `json:"message"`
	Span       *struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"span,omitempty"`
	WriteOutcome string `json:"write_outcome,omitempty"`
}

func (e *CypherExecutionError) Error() string {
	return fmt.Sprintf("Cypher query failed (%s): %s", e.Code, e.Message)
}

func decodeCypherResponse[T any](response *http.Response) (*T, error) {
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		data, err := io.ReadAll(io.LimitReader(response.Body, maxErrorResponseBytes))
		if err != nil {
			return nil, err
		}
		diagnostic := &CypherExecutionError{StatusCode: response.StatusCode, Code: "HTTPError", Message: string(data)}
		_ = json.Unmarshal(data, diagnostic)
		return nil, diagnostic
	}
	var value T
	decoder := json.NewDecoder(io.LimitReader(response.Body, 32<<20))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decoding graph response: %w", err)
	}
	return &value, nil
}
func (c *AntflyClient) ListGraphs(ctx context.Context) ([]GraphCatalogEntry, error) {
	response, err := c.client.ListGraphs(ctx)
	if err != nil {
		return nil, err
	}
	value, err := decodeCypherResponse[[]GraphCatalogEntry](response)
	if err != nil {
		return nil, err
	}
	return *value, nil
}
func (c *AntflyClient) GetGraph(ctx context.Context, name string) (*GraphCatalogEntry, error) {
	response, err := c.client.GetGraph(ctx, name)
	if err != nil {
		return nil, err
	}
	return decodeCypherResponse[GraphCatalogEntry](response)
}

// GraphCatalogMutationResult distinguishes a visible catalog entry from a
// durable mutation whose projection is still pending. Never replay a pending
// mutation; inspect GetGraph to observe its eventual visibility.
type GraphCatalogMutationResult struct {
	Entry   *GraphCatalogEntry
	Outcome *oapi.CommittedMutationOutcome
}

func decodeGraphCatalogMutation(response *http.Response) (*GraphCatalogMutationResult, error) {
	if response.StatusCode == http.StatusAccepted {
		outcome, err := decodeCypherResponse[oapi.CommittedMutationOutcome](response)
		if err != nil {
			return nil, err
		}
		if outcome.Status != oapi.CommittedMutationOutcomeStatusCommittedVisibilityPending {
			return nil, fmt.Errorf("unexpected pending graph mutation status %q", outcome.Status)
		}
		return &GraphCatalogMutationResult{Outcome: outcome}, nil
	}
	entry, err := decodeCypherResponse[GraphCatalogEntry](response)
	if err != nil {
		return nil, err
	}
	if entry.GraphId == "" || entry.Name == "" || entry.Revision == "" {
		return nil, fmt.Errorf("graph mutation returned an invalid catalog entry")
	}
	return &GraphCatalogMutationResult{Entry: entry}, nil
}

func (c *AntflyClient) CreateGraph(ctx context.Context, name string, definition GraphDefinition) (*GraphCatalogMutationResult, error) {
	response, err := c.client.CreateGraph(ctx, name, definition)
	if err != nil {
		return nil, err
	}
	return decodeGraphCatalogMutation(response)
}

// UpdateGraph conditionally replaces mappings without replaying a stale revision.
func (c *AntflyClient) UpdateGraph(ctx context.Context, name, expectedRevision string, definition GraphDefinition) (*GraphCatalogMutationResult, error) {
	if expectedRevision == "" || expectedRevision[0] == '0' {
		return nil, fmt.Errorf("expected a positive decimal graph revision")
	}
	for _, digit := range expectedRevision {
		if digit < '0' || digit > '9' {
			return nil, fmt.Errorf("expected a positive decimal graph revision")
		}
	}
	response, err := c.client.UpdateGraph(ctx, name, oapi.GraphDefinitionUpdate{ExpectedRevision: expectedRevision, Definition: definition})
	if err != nil {
		return nil, err
	}
	return decodeGraphCatalogMutation(response)
}

// DropGraph returns a non-nil outcome when deletion committed but catalog
// visibility is pending. A nil outcome and nil error mean deletion is visible.
func (c *AntflyClient) DropGraph(ctx context.Context, name string) (*oapi.CommittedMutationOutcome, error) {
	response, err := c.client.DropGraph(ctx, name)
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusAccepted {
		result, err := decodeGraphCatalogMutation(response)
		if err != nil {
			return nil, err
		}
		return result.Outcome, nil
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	return nil, nil
}

// ExecuteGraphQuery never retries a write. Retain WriteRequestId and inspect
// its transaction session when result delivery is canceled or uncertain.
func (c *AntflyClient) ExecuteGraphQuery(ctx context.Context, name string, request GraphQueryRequest) (*GraphQueryResult, error) {
	if request.Parameters != nil && request.TypedParameters.Type != "" {
		return nil, fmt.Errorf("choose parameters or typed_parameters")
	}
	response, err := c.client.ExecuteGraphQuery(ctx, name, request)
	if err != nil {
		return nil, err
	}
	result, err := decodeCypherResponse[GraphQueryResult](response)
	if err != nil {
		return nil, err
	}
	if result.WireVersion != 1 {
		return nil, fmt.Errorf("unsupported graph wire version %d", result.WireVersion)
	}
	for _, row := range result.Rows {
		if len(row) != len(result.Columns) {
			return nil, fmt.Errorf("graph result row width does not match columns")
		}
	}
	return result, nil
}

// GraphWriteStatus reads a principal-bound receipt without replaying a query.
func (c *AntflyClient) GraphWriteStatus(ctx context.Context, id string) (*oapi.TransactionSessionStatus, error) {
	decoded, err := hex.DecodeString(id)
	if err != nil || len(decoded) != 16 {
		return nil, fmt.Errorf("invalid graph write transaction ID")
	}
	response, err := c.client.GetTransactionStatus(ctx, oapi.TransactionId(id))
	if err != nil {
		return nil, err
	}
	return decodeCypherResponse[oapi.TransactionSessionStatus](response)
}
