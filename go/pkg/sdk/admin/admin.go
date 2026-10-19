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

package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/sdk/admission"
)

// HTTPRequestDoer is the transport contract shared by internal and standby clients.
type HTTPRequestDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// InternalClient provides access to internal admin APIs for cluster management
type InternalClient struct {
	baseURL    string
	httpClient *admission.Client
	token      string
	timeout    time.Duration
}

// NewInternalClient creates a reusable internal admin client with bounded local
// admission. A nil HTTP client keeps the legacy 90-second operation timeout.
func NewInternalClient(baseURL string, httpClient *http.Client) *InternalClient {
	client, _ := NewInternalClientWithAdmission(baseURL, httpClient, nil)
	return client
}

// NewInternalClientWithAdmission configures local request capacity. Nil policy
// selects 100 active operations, 100 waiters and a one-second queue ceiling.
// A supplied bounded client is shared when policy is nil; it is never rewrapped.
// Caller HTTP clients retain their transport ownership and timeout policy.
func NewInternalClientWithAdmission(baseURL string, next HTTPRequestDoer, policy *admission.Config) (*InternalClient, error) {
	bounded, err := admission.New(next, policy)
	if err != nil {
		return nil, err
	}
	client := &InternalClient{baseURL: baseURL, httpClient: bounded}
	standard, isStandard := next.(*http.Client)
	if next == nil || (isStandard && standard == nil) {
		client.timeout = 90 * time.Second
	}
	return client, nil
}

// Close stops new operations and wakes local waiters. Active operations retain
// their response body until completion; caller-supplied transports remain usable.
// Closing a shared bounded client closes admission for all its users.
func (c *InternalClient) Close() error {
	if c == nil {
		return nil
	}
	return c.httpClient.Close()
}

// SDK-owned clients never redirect an admin mutation. Borrowed clients retain
// the redirect policy explicitly chosen by their caller.
func (c *InternalClient) do(req *http.Request) (*http.Response, error) {
	if c.timeout > 0 {
		return c.httpClient.DoSQLNoReplay(req)
	}
	return c.httpClient.Do(req)
}

func (c *InternalClient) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout > 0 {
		return context.WithTimeout(ctx, c.timeout)
	}
	return ctx, func() {}
}

// WithToken configures token authentication for internal admin requests.
func (c *InternalClient) WithToken(token string) *InternalClient {
	c.token = token
	return c
}

func (c *InternalClient) newRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

// AddMetadataPeer adds a new peer to the metadata raft cluster
func (c *InternalClient) AddMetadataPeer(nodeID uint64, raftURL string) error {
	return c.AddMetadataPeerContext(context.Background(), nodeID, raftURL)
}

// AddMetadataPeerContext retains the original context through local waiting and the response.
func (c *InternalClient) AddMetadataPeerContext(ctx context.Context, nodeID uint64, raftURL string) error {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	// Make POST request to internal endpoint
	url := fmt.Sprintf("%s/_internal/v1/peer/%d", c.baseURL, nodeID)
	req, err := c.newRequest(ctx, http.MethodPost, url, strings.NewReader(raftURL))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.do(req) //nolint:gosec // URL constructed from trusted c.baseURL
	if err != nil {
		return fmt.Errorf("failed to add peer: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to add peer: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// RemoveMetadataPeer removes a peer from the metadata raft cluster
func (c *InternalClient) RemoveMetadataPeer(nodeID uint64) error {
	return c.RemoveMetadataPeerContext(context.Background(), nodeID)
}

// RemoveMetadataPeerContext retains the original context through local waiting and the response.
func (c *InternalClient) RemoveMetadataPeerContext(ctx context.Context, nodeID uint64) error {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	// Make DELETE request to internal endpoint
	url := fmt.Sprintf("%s/_internal/v1/peer/%d", c.baseURL, nodeID)
	req, err := c.newRequest(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.do(req) //nolint:gosec // URL constructed from trusted c.baseURL
	if err != nil {
		return fmt.Errorf("failed to remove peer: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to remove peer: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// MetadataStatus represents the status of the metadata raft cluster
type MetadataStatus struct {
	Leader  uint64
	Members map[uint64]string // nodeID -> raft URL (empty URL if not available)
}

// RaftStatus represents raft cluster status
type RaftStatus struct {
	Lead   uint64            `json:"leader_id,omitempty"`
	Voters map[uint64]string `json:"voters,omitempty"`
}

// ShardInfo contains metadata shard information
type ShardInfo struct {
	Peers      map[uint64]string `json:"peers,omitempty"`
	RaftStatus *RaftStatus       `json:"raft_status,omitempty"`
}

// GetMetadataStatus retrieves the current status of the metadata raft cluster
func (c *InternalClient) GetMetadataStatus() (*MetadataStatus, error) {
	return c.GetMetadataStatusContext(context.Background())
}

// GetMetadataStatusContext retains the original context through local waiting and the response.
func (c *InternalClient) GetMetadataStatusContext(ctx context.Context) (*MetadataStatus, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	// Make GET request to the internal status endpoint
	url := fmt.Sprintf("%s/_internal/v1/status", c.baseURL)
	req, err := c.newRequest(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.do(req) //nolint:gosec // URL constructed from trusted c.baseURL
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata status: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get metadata status: status %d, body: %s", resp.StatusCode, string(body))
	}

	var shardInfo ShardInfo
	if err := json.NewDecoder(resp.Body).Decode(&shardInfo); err != nil {
		return nil, fmt.Errorf("failed to decode status response: %w", err)
	}

	if shardInfo.RaftStatus == nil {
		return nil, fmt.Errorf("no metadata raft status available")
	}

	status := &MetadataStatus{
		Leader:  shardInfo.RaftStatus.Lead,
		Members: make(map[uint64]string),
	}

	// Populate members from voters (these are the raft cluster members)
	if shardInfo.RaftStatus.Voters != nil {
		maps.Copy(status.Members, shardInfo.RaftStatus.Voters)
	}

	// Also include peers if available (may have URLs)
	if shardInfo.Peers != nil {
		for nodeID, url := range shardInfo.Peers {
			if _, exists := status.Members[nodeID]; !exists {
				status.Members[nodeID] = url
			} else if status.Members[nodeID] == "" && url != "" {
				// Update with actual URL if we have it from peers
				status.Members[nodeID] = url
			}
		}
	}

	return status, nil
}
