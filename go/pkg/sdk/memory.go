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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// MemorySpace is a convenience handle for one native memory space. It covers
// the common agent loop (brief, recall, remember, outcome) and generates
// Idempotency-Key headers for mutations. Use Client.Memory for the full API.
type MemorySpace struct {
	api       *oapi.ClientWithResponses
	space     string
	database  *string
	namespace *string
	branch    *string
}

// MemorySpaceOption scopes a MemorySpace to a catalog database, namespace, or branch.
type MemorySpaceOption func(*MemorySpace)

// WithMemoryDatabase sends X-Antfly-Database on every request.
func WithMemoryDatabase(database string) MemorySpaceOption {
	return func(s *MemorySpace) { s.database = nonEmpty(database) }
}

// WithMemoryNamespace sends X-Antfly-Namespace on every request.
func WithMemoryNamespace(namespace string) MemorySpaceOption {
	return func(s *MemorySpace) { s.namespace = nonEmpty(namespace) }
}

// WithMemoryBranch sends X-Antfly-Memory-Branch on every request.
func WithMemoryBranch(branch string) MemorySpaceOption {
	return func(s *MemorySpace) { s.branch = nonEmpty(branch) }
}

// MemorySpace returns a convenience handle for the named memory space.
func (c *Client) MemorySpace(space string, opts ...MemorySpaceOption) *MemorySpace {
	s := &MemorySpace{api: c.Memory(), space: space}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// Name returns the memory space name.
func (s *MemorySpace) Name() string { return s.space }

// MemoryCallOption customizes a single memory mutation.
type MemoryCallOption func(*memoryCall)

type memoryCall struct {
	idempotencyKey string
	evidenceID     string
	sourceID       string
}

// WithIdempotencyKey sets the Idempotency-Key for a mutation. Reuse the same
// key when retrying so the server applies the write once. When omitted, a
// random key is generated per call.
func WithIdempotencyKey(key string) MemoryCallOption {
	return func(c *memoryCall) { c.idempotencyKey = key }
}

// WithEvidenceID sets the evidence id Remember records; a random id is used
// when omitted. Evidence ids name immutable source versions.
func WithEvidenceID(id string) MemoryCallOption {
	return func(c *memoryCall) { c.evidenceID = id }
}

// WithSourceID sets the evidence source_id Remember records.
func WithSourceID(id string) MemoryCallOption {
	return func(c *memoryCall) { c.sourceID = id }
}

func newMemoryCall(opts []MemoryCallOption) (memoryCall, error) {
	var call memoryCall
	for _, opt := range opts {
		if opt != nil {
			opt(&call)
		}
	}
	if call.idempotencyKey == "" {
		key, err := NewIdempotencyKey()
		if err != nil {
			return call, err
		}
		call.idempotencyKey = key
	}
	if len(call.idempotencyKey) > 256 {
		return call, fmt.Errorf("memory: idempotency key exceeds 256 characters")
	}
	return call, nil
}

// NewIdempotencyKey returns a random 128-bit hex key suitable for Idempotency-Key.
func NewIdempotencyKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("memory: generating idempotency key: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// MemoryAPIError is a non-2xx response from the memory API.
type MemoryAPIError struct {
	StatusCode int
	// Code is the server's machine-readable error code, e.g. "TableNotFound".
	Code      string
	Message   string
	Retryable bool
}

func (e *MemoryAPIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("memory request failed (%d %s): %s", e.StatusCode, e.Code, e.Message)
	}
	return fmt.Sprintf("memory request failed (%d): %s", e.StatusCode, e.Message)
}

// Create creates the memory space with its initial configuration.
func (s *MemorySpace) Create(ctx context.Context, config oapi.MemorySpaceConfig, opts ...MemoryCallOption) (*oapi.MemoryOperation, error) {
	call, err := newMemoryCall(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.api.PutMemorySpaceWithResponse(ctx, s.space, &oapi.PutMemorySpaceParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
		IdempotencyKey: call.idempotencyKey,
	}, config)
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON201)
}

// Brief returns the small context an agent should load at session start.
func (s *MemorySpace) Brief(ctx context.Context, req oapi.MemoryBriefRequest) (*oapi.MemoryBrief, error) {
	resp, err := s.api.BriefMemoryWithResponse(ctx, s.space, &oapi.BriefMemoryParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
	}, req)
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON200)
}

// Recall searches the space for memories relevant to req.
func (s *MemorySpace) Recall(ctx context.Context, req oapi.MemoryRecallRequest) (*oapi.MemoryRecallResult, error) {
	resp, err := s.api.RecallMemoryWithResponse(ctx, s.space, &oapi.RecallMemoryParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
	}, req)
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON200)
}

// Observe records evidence and the assertions that cite it.
func (s *MemorySpace) Observe(ctx context.Context, req oapi.MemoryObservationRequest, opts ...MemoryCallOption) (*oapi.MemoryOperation, error) {
	call, err := newMemoryCall(opts)
	if err != nil {
		return nil, err
	}
	return s.observe(ctx, req, call)
}

func (s *MemorySpace) observe(ctx context.Context, req oapi.MemoryObservationRequest, call memoryCall) (*oapi.MemoryOperation, error) {
	resp, err := s.api.ObserveMemoryWithResponse(ctx, s.space, &oapi.ObserveMemoryParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
		IdempotencyKey: call.idempotencyKey,
	}, req)
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON200)
}

// Remember records text as a single evidence item. Use Observe to attach
// assertions, provenance, or several evidence items.
func (s *MemorySpace) Remember(ctx context.Context, text string, opts ...MemoryCallOption) (*oapi.MemoryOperation, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("memory: remember text is empty")
	}
	call, err := newMemoryCall(opts)
	if err != nil {
		return nil, err
	}
	id := call.evidenceID
	if id == "" {
		// Derive identity from the operation key so explicit-key retries send
		// the same immutable evidence and replay the committed receipt.
		digest := sha256.Sum256([]byte("antfly-memory-remember:" + call.idempotencyKey))
		id = hex.EncodeToString(digest[:])
	}
	return s.observe(ctx, oapi.MemoryObservationRequest{
		Evidence: []oapi.MemoryEvidence{{Id: id, SourceId: call.sourceID, Text: text}},
	}, call)
}

// Outcome records whether a task that used recalled memories succeeded.
func (s *MemorySpace) Outcome(ctx context.Context, outcome oapi.MemoryOutcome, opts ...MemoryCallOption) (*oapi.MemoryOperation, error) {
	call, err := newMemoryCall(opts)
	if err != nil {
		return nil, err
	}
	if outcome.UsedMemoryIds == nil {
		outcome.UsedMemoryIds = []string{}
	}
	resp, err := s.api.RecordMemoryOutcomeWithResponse(ctx, s.space, &oapi.RecordMemoryOutcomeParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
		IdempotencyKey: call.idempotencyKey,
	}, outcome)
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON200)
}

// Consolidate runs a read-only health review of the space.
func (s *MemorySpace) Consolidate(ctx context.Context, req oapi.MemoryConsolidationRequest) (*oapi.MemoryConsolidation, error) {
	resp, err := s.api.ConsolidateMemoryWithResponse(ctx, s.space, &oapi.ConsolidateMemoryParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
	}, req)
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON200)
}

// Pin prioritizes memory id in the brief within its item and byte limits.
func (s *MemorySpace) Pin(ctx context.Context, id, reason string, opts ...MemoryCallOption) (*oapi.MemoryOperation, error) {
	return s.pin(ctx, id, true, reason, opts)
}

// Unpin removes a pin from memory id.
func (s *MemorySpace) Unpin(ctx context.Context, id, reason string, opts ...MemoryCallOption) (*oapi.MemoryOperation, error) {
	return s.pin(ctx, id, false, reason, opts)
}

func (s *MemorySpace) pin(ctx context.Context, id string, pinned bool, reason string, opts []MemoryCallOption) (*oapi.MemoryOperation, error) {
	call, err := newMemoryCall(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.api.PinMemoryWithResponse(ctx, s.space, &oapi.PinMemoryParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
		IdempotencyKey: call.idempotencyKey,
	}, oapi.MemoryPin{Id: id, Pinned: &pinned, Reason: reason})
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON200)
}

// Correct changes a memory's lifecycle, e.g. to retract or dispute it.
func (s *MemorySpace) Correct(ctx context.Context, correction oapi.MemoryCorrection, opts ...MemoryCallOption) (*oapi.MemoryOperation, error) {
	call, err := newMemoryCall(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.api.CorrectMemoryWithResponse(ctx, s.space, &oapi.CorrectMemoryParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
		IdempotencyKey: call.idempotencyKey,
	}, correction)
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON200)
}

// Forget deletes the selected evidence, assertions, or sources.
func (s *MemorySpace) Forget(ctx context.Context, req oapi.MemoryForgetRequest, opts ...MemoryCallOption) (*oapi.MemoryOperation, error) {
	call, err := newMemoryCall(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.api.ForgetMemoryWithResponse(ctx, s.space, &oapi.ForgetMemoryParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
		IdempotencyKey: call.idempotencyKey,
	}, req)
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON200)
}

// Get returns one memory record by id.
func (s *MemorySpace) Get(ctx context.Context, id string) (*oapi.MemoryRecord, error) {
	resp, err := s.api.GetMemoryWithResponse(ctx, s.space, id, &oapi.GetMemoryParams{
		XAntflyDatabase: s.database, XAntflyNamespace: s.namespace, XAntflyMemoryBranch: s.branch,
	})
	if err != nil {
		return nil, err
	}
	return memoryResult(resp.HTTPResponse, resp.Body, resp.JSON200)
}

func memoryResult[T any](resp *http.Response, body []byte, ok *T) (*T, error) {
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	if status >= 200 && status < 300 {
		if ok == nil {
			return nil, fmt.Errorf("memory: unexpected %d response body", status)
		}
		return ok, nil
	}
	apiErr := &MemoryAPIError{StatusCode: status}
	var payload oapi.MemoryError
	if json.Unmarshal(body, &payload) == nil && (payload.Code != "" || payload.Error != "") {
		apiErr.Code, apiErr.Message, apiErr.Retryable = payload.Code, payload.Error, payload.Retryable
	} else {
		apiErr.Message = strings.TrimSpace(string(body))
		if apiErr.Message == "" {
			apiErr.Message = http.StatusText(status)
		}
	}
	return nil, apiErr
}

func nonEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
