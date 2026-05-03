// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package store

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// TestNodeRegistrationEndpoint verifies that data-node registration uses the
// node resource and includes the hosted store metadata in the same request.
func TestNodeRegistrationEndpoint(t *testing.T) {
	var requestPaths []string
	var requestMethods []string
	var body []byte

	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestPaths = append(requestPaths, req.URL.Path)
		requestMethods = append(requestMethods, req.Method)

		var err error
		body, err = io.ReadAll(req.Body)
		require.NoError(t, err)

		return &http.Response{
			StatusCode: http.StatusAccepted,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
			Request:    req,
		}, nil
	})}

	// Create a minimal store for testing
	store := &Store{
		config:    &StoreInfo{ID: types.ID(1)},
		shardsMap: xsync.NewMap[types.ID, *Shard](),
	}

	// Create a StoreInfo
	conf := &StoreInfo{
		ID:      types.ID(1),
		RaftURL: "http://localhost:9021",
		ApiURL:  "http://localhost:12380",
	}

	// Attempt registration
	err := registerWithLeader(context.Background(), client, "http://metadata.test", store, conf)

	require.NoError(t, err, "Registration failed")

	assert.Equal(t, []string{"/internal/v1/nodes"}, requestPaths, "registration should use the node resource")
	assert.Equal(t, []string{http.MethodPost}, requestMethods, "Should use POST methods")

	var payload struct {
		NodeID      uint64 `json:"node_id"`
		StoreID     uint64 `json:"store_id"`
		Role        string `json:"role"`
		HealthClass string `json:"health_class"`
		Live        bool   `json:"live"`
		RaftURL     string `json:"raft_url"`
		APIURL      string `json:"api_url"`
	}
	require.NoError(t, json.Unmarshal(body, &payload))
	assert.Equal(t, uint64(1), payload.NodeID)
	assert.Equal(t, uint64(1), payload.StoreID)
	assert.Equal(t, "data", payload.Role)
	assert.Equal(t, "healthy", payload.HealthClass)
	assert.True(t, payload.Live)
	assert.Equal(t, "http://localhost:9021", payload.RaftURL)
	assert.Equal(t, "http://localhost:12380", payload.APIURL)
}

func TestShardInfosToNodeGroupStatusReports(t *testing.T) {
	reports := shardInfosToNodeGroupStatusReports(types.ID(2), map[types.ID]*ShardInfo{
		10: {
			RaftStatus: &common.RaftStatus{
				Lead:   2,
				Voters: common.NewPeerSet(1, 2, 3),
			},
		},
	})

	require.Len(t, reports, 1)
	assert.Equal(t, uint64(10), reports[0].GroupID)
	assert.True(t, reports[0].LocalLeader)
	assert.True(t, reports[0].LocalVoter)
	assert.Equal(t, 3, reports[0].VoterCount)
}

// TestNodeRegistrationURL verifies the full URL construction.
func TestNodeRegistrationURL(t *testing.T) {
	tests := []struct {
		name        string
		leaderURL   string
		expectedURL string
	}{
		{
			name:        "http URL",
			leaderURL:   "http://127.0.0.1:12277",
			expectedURL: "http://127.0.0.1:12277/internal/v1/nodes",
		},
		{
			name:        "https URL",
			leaderURL:   "https://metadata.example.com:8080",
			expectedURL: "https://metadata.example.com:8080/internal/v1/nodes",
		},
		{
			name:        "URL with trailing slash",
			leaderURL:   "http://127.0.0.1:12277/",
			expectedURL: "http://127.0.0.1:12277//internal/v1/nodes", // Will still work with double slash
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedURLs []string
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				capturedURLs = append(capturedURLs, req.URL.String())
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
					Request:    req,
				}, nil
			})}

			store := &Store{
				config:    &StoreInfo{ID: types.ID(1)},
				shardsMap: xsync.NewMap[types.ID, *Shard](),
			}

			conf := &StoreInfo{
				ID:      types.ID(1),
				RaftURL: "http://localhost:9021",
				ApiURL:  "http://localhost:12380",
			}

			err := registerWithLeader(context.Background(), client, tt.leaderURL, store, conf)

			require.NoError(t, err)
			assert.Equal(t, []string{tt.expectedURL}, capturedURLs)
		})
	}
}

func TestNodeRegistrationFailsWhenNodeEndpointMissing(t *testing.T) {
	var requestPaths []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestPaths = append(requestPaths, req.URL.Path)
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
			Request:    req,
		}, nil
	})}

	store := &Store{
		config:    &StoreInfo{ID: types.ID(1)},
		shardsMap: xsync.NewMap[types.ID, *Shard](),
	}
	conf := &StoreInfo{
		ID:      types.ID(1),
		RaftURL: "http://localhost:9021",
		ApiURL:  "http://localhost:12380",
	}

	err := registerWithLeader(context.Background(), client, "http://metadata.test", store, conf)

	require.Error(t, err)
	assert.Equal(t, []string{"/internal/v1/nodes"}, requestPaths)
}
