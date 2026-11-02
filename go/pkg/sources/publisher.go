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

package sources

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Publisher writes the ordinary Antfly table API. It installs no new database
// routes and never treats a local catalog as server-side authorization.
type Publisher struct {
	Endpoint, Token string
	Client          *http.Client
}

func ValidateEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid Antfly endpoint")
	}
	if u.Scheme == "https" {
		return nil
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme == "http" && (u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()) {
		return nil
	}
	return fmt.Errorf("Antfly endpoint requires HTTPS or loopback HTTP")
}
func (p *Publisher) call(ctx context.Context, method, path string, body any) ([]byte, error) {
	if method == http.MethodGet {
		return p.callRead(ctx, method, path, body)
	}
	return p.callContentType(ctx, method, path, body, "application/json")
}
func (p *Publisher) callContentType(ctx context.Context, method, path string, body any, contentType string) ([]byte, error) {
	if err := ValidateEndpoint(p.Endpoint); err != nil {
		return nil, err
	}
	if strings.ContainsAny(p.Token, "\r\n") {
		return nil, fmt.Errorf("invalid data-plane credential")
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(p.Endpoint, "/")+path, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", contentType)
	if p.Token != "" {
		request.Header.Set("Authorization", "Bearer "+p.Token)
	}
	client := http.Client{Timeout: 60 * time.Second}
	if p.Client != nil {
		client = *p.Client
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8<<20+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 8<<20 {
		return nil, fmt.Errorf("Antfly response exceeds budget")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, &PublishHTTPError{Status: response.StatusCode, RetryAt: providerRetryAt(response.Header.Get("Retry-After"), time.Now())}
	}
	return data, nil
}

type PublishHTTPError struct {
	Status  int
	RetryAt time.Time
}

func (e *PublishHTTPError) Error() string {
	return fmt.Sprintf("Antfly publication returned HTTP %d", e.Status)
}
func tablePath(table string) string { return "/tables/" + url.PathEscape(table) }
func (p *Publisher) EnsureTable(ctx context.Context, table string) error {
	_, err := p.call(ctx, "GET", tablePath(table), nil)
	if err == nil {
		return nil
	}
	if status, ok := err.(*PublishHTTPError); !ok || status.Status != 404 {
		return err
	}
	_, err = p.call(ctx, "POST", tablePath(table), map[string]any{"num_shards": 1})
	return err
}
func (p *Publisher) Batch(ctx context.Context, table string, inserts map[string]any, deletes []string) error {
	body, err := p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": inserts, "deletes": deletes, "sync_level": "write"})
	if err != nil {
		return err
	}
	var result struct {
		Errors map[string]any    `json:"errors"`
		Failed []json.RawMessage `json:"failed"`
		Status string            `json:"status"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("invalid batch acknowledgement: %w", err)
	}
	if len(result.Errors) > 0 || len(result.Failed) > 0 || result.Status != "" && result.Status != "committed" {
		return fmt.Errorf("Antfly batch contains record failures")
	}
	return nil
}
func (p *Publisher) Ready(ctx context.Context, table, id string, doc Document) error {
	return WaitForDocuments(ctx, p.Query, table, map[string]Document{id: doc})
}

// ReadyAbsent prevents publishing a complete catalog while retired rows are
// still visible to a fresh query snapshot.
func (p *Publisher) ReadyAbsent(ctx context.Context, table string, ids []string) error {
	return WaitForAbsent(ctx, p.Query, table, ids)
}

// WaitForAbsent verifies retirement through fresh ordinary table-query snapshots.
func WaitForAbsent(ctx context.Context, query func(context.Context, string, []byte) ([]byte, error), table string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if len(ids) > 1000 {
		return fmt.Errorf("canonical absence batch exceeds 1000 records")
	}

	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		body, _ := json.Marshal(map[string]any{"query": documentIDsQuery(ids), "fields": []string{"_id"}, "limit": 1, "order_by": []any{map[string]any{"field": "_id", "desc": false}}})
		data, err := query(wait, table, body)
		if err != nil {
			return err
		}
		var result struct {
			Responses []struct {
				Status   int             `json:"status"`
				Error    json.RawMessage `json:"error"`
				Snapshot string          `json:"remote_snapshot"`
				Hits     struct {
					Hits []json.RawMessage `json:"hits"`
				} `json:"hits"`
			} `json:"responses"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return err
		}
		if len(result.Responses) != 1 || result.Responses[0].Status != 200 || result.Responses[0].Snapshot == "" || len(result.Responses[0].Error) > 0 && string(result.Responses[0].Error) != "null" {
			return fmt.Errorf("Antfly did not provide successful deletion snapshot readiness")
		}
		if len(result.Responses[0].Hits.Hits) == 0 {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-wait.Done():
			timer.Stop()
			return fmt.Errorf("retired sources remain query-visible: %w", wait.Err())
		case <-timer.C:
		}
	}
}

func (p *Publisher) Merge(ctx context.Context, table string, body []byte) ([]byte, error) {
	return p.callContentType(ctx, "POST", tablePath(table)+"/merge", json.RawMessage(body), ScopedMergeContentType)
}
