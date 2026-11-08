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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client targets an explicit source-management mount, not a database listener.
// Credential installation remains a hosting-runtime operation.
type Client struct {
	base, token string
	http        *http.Client
}

func NewClient(base, token string) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid source-management endpoint")
	}
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return nil, fmt.Errorf("source-management endpoint requires HTTPS or loopback HTTP")
	}
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, fmt.Errorf("invalid source-management token")
	}
	return &Client{base: strings.TrimRight(base, "/"), token: token, http: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) call(ctx context.Context, method, path, key string, input, output any) error {
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
	r.Header.Set("Authorization", "Bearer "+c.token)
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	response, err := c.http.Do(r)
	if err != nil {
		return fmt.Errorf("source-management transport failed")
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
	return json.Unmarshal(raw, output)
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
func (c *Client) PutSource(ctx context.Context, source Source) (out Source, err error) {
	source.SearchStatus = nil // GET observations are never submitted as desired state.
	err = c.call(ctx, "PUT", "/sources/"+url.PathEscape(source.Id), "", source, &out)
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
