package admin

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/json"
)

// InternalClient provides access to internal admin APIs for cluster management
type InternalClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewInternalClient creates a new internal admin client
func NewInternalClient(baseURL string, httpClient *http.Client) *InternalClient {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: time.Second * 90,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     time.Minute,
				DisableKeepAlives:   false,
			},
		}
	}
	return &InternalClient{
		baseURL:    baseURL,
		httpClient: httpClient,
	}
}

// AddMetadataPeer adds a new peer to the metadata raft cluster
func (c *InternalClient) AddMetadataPeer(nodeID uint64, raftURL string) error {
	// Make POST request to internal endpoint
	url := fmt.Sprintf("%s/_internal/v1/peer/%d", c.baseURL, nodeID)
	resp, err := c.httpClient.Post(url, "application/octet-stream", strings.NewReader(raftURL))
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
	// Make DELETE request to internal endpoint
	url := fmt.Sprintf("%s/_internal/v1/peer/%d", c.baseURL, nodeID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req) //nolint:gosec // URL constructed from trusted c.baseURL
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
	// Make GET request to the internal status endpoint
	url := fmt.Sprintf("%s/_internal/v1/status", c.baseURL)
	resp, err := c.httpClient.Get(url)
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
