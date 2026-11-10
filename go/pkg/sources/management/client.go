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

package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Client targets an explicit source-management mount, not a database listener.
// Credential installation remains a hosting-runtime operation.
type Client struct {
	base  string
	token func(context.Context) (string, error)
	http  *http.Client
}

func NewClient(base, token string) (*Client, error) {
	opts := Options{}
	if token != "" && !strings.ContainsAny(token, "\r\n") {
		opts.Token = func(context.Context) (string, error) { return token, nil }
	}
	return NewClientWithOptions(base, opts)
}
func (c *Client) call(ctx context.Context, method, path, key string, input, output any, headers ...http.Header) error {
	token, err := c.token(ctx)
	if err != nil {
		return fmt.Errorf("source-management token: %w", err)
	}
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return fmt.Errorf("invalid source-management token")
	}
	var body io.Reader
	if input != nil {
		raw, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	r, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	for _, header := range headers {
		for name, values := range header {
			for _, value := range values {
				r.Header.Add(name, value)
			}
		}
	}
	response, err := c.http.Do(r)
	if err != nil {
		var urlErr *url.Error
		return &TransportError{
			Timeout: errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(err, &urlErr) && urlErr.Timeout()),
			cause:   ctx.Err(),
		}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("source-management response failed")
	}
	if len(raw) > MaxResponseBytes {
		return fmt.Errorf("source-management response exceeds limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure Error
		_ = json.Unmarshal(raw, &failure)
		if failure.Code == "" {
			failure.Code = "request_failed"
		}
		return Failure(response.StatusCode, failure.Code)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("invalid management response")
	}
	return nil
}
func (c *Client) Capabilities(ctx context.Context) (out Capabilities, err error) {
	err = c.call(ctx, "GET", "/capabilities", "", nil, &out)
	return
}
func (c *Client) ListSources(ctx context.Context, limit int, cursor string) (out SourcePage, err error) {
	q := url.Values{"limit": {fmt.Sprint(limit)}, "cursor": {cursor}}
	err = c.call(ctx, "GET", "/sources?"+q.Encode(), "", nil, &out)
	return
}
func (c *Client) GetSource(ctx context.Context, id string) (out Source, err error) {
	err = c.call(ctx, "GET", "/sources/"+url.PathEscape(id), "", nil, &out)
	return
}
func (c *Client) PutSource(ctx context.Context, source Source) (Source, error) {
	return c.putSource(ctx, source, "")
}

// PutSourceIfMatch requires a matching declaration revision for a PUT.
func (c *Client) PutSourceIfMatch(ctx context.Context, source Source, revision string) (Source, error) {
	if revision == "" {
		return Source{}, Failure(400, "invalid_precondition")
	}
	return c.putSource(ctx, source, revision)
}

func (c *Client) putSource(ctx context.Context, source Source, revision string) (out Source, err error) {
	source.Revision = ""
	source.KnowledgeStatus = nil
	source.SearchStatus = nil // GET observations are never submitted as desired state.
	var headers http.Header
	if revision != "" {
		headers = http.Header{"If-Match": {`"` + revision + `"`}}
	}
	err = c.call(ctx, "PUT", "/sources/"+url.PathEscape(source.Id), "", source, &out, headers)
	return
}
func (c *Client) RemoveSource(ctx context.Context, id string, purge bool) (out Mutation, err error) {
	err = c.call(ctx, "DELETE", "/sources/"+url.PathEscape(id)+"?purge="+fmt.Sprint(purge), "", nil, &out)
	return
}
func (c *Client) ListRuns(ctx context.Context, id string, limit int, cursor string) (out RunPage, err error) {
	q := url.Values{"limit": {fmt.Sprint(limit)}, "cursor": {cursor}}
	err = c.call(ctx, "GET", "/sources/"+url.PathEscape(id)+"/runs?"+q.Encode(), "", nil, &out)
	return
}
func (c *Client) GetRun(ctx context.Context, source, id string) (out Run, err error) {
	err = c.call(ctx, "GET", "/sources/"+url.PathEscape(source)+"/runs/"+url.PathEscape(id), "", nil, &out)
	return
}
func (c *Client) StartRun(ctx context.Context, id, key string, input RunInput) (out Run, err error) {
	if !validID(key) {
		return out, Failure(400, "invalid_idempotency_key")
	}
	err = c.call(ctx, "POST", "/sources/"+url.PathEscape(id)+"/runs", key, input, &out)
	return
}
func (c *Client) CancelRun(ctx context.Context, source, id string) (out Run, err error) {
	err = c.call(ctx, "POST", "/sources/"+url.PathEscape(source)+"/runs/"+url.PathEscape(id)+"/cancel", "", struct{}{}, &out)
	return
}

func (c *Client) PreviewProcessing(ctx context.Context, id string, input ProcessingPreset) (out ProcessingPreview, err error) {
	err = c.call(ctx, "POST", "/sources/"+url.PathEscape(id)+"/processing-preview", "", input, &out)
	return
}
func (c *Client) ProcessingJob(ctx context.Context, id string, input ProcessingJobRequest) (out map[string]any, err error) {
	err = c.call(ctx, "POST", "/sources/"+url.PathEscape(id)+"/processing-jobs", "", input, &out)
	return
}

// QueryKnowledge executes a bounded source graph query on an explicitly enabled runtime.
func (c *Client) QueryKnowledge(ctx context.Context, id string, input KnowledgeRequest) (json.RawMessage, error) {
	var out json.RawMessage
	if !validID(id) {
		return nil, Failure(400, "invalid_source_id")
	}
	err := c.call(ctx, http.MethodPost, "/sources/"+id+"/knowledge-query", "", input, &out)
	return out, err
}
