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

package main

import (
	"encoding/json"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antflydb/antfly/lib/types"
)

// Backend represents a backend server
type Backend struct {
	URL       *url.URL
	Proxy     *httputil.ReverseProxy
	Alive     atomic.Bool
	Transport *http.Transport
}

// LoadBalancer represents the load balancer
type LoadBalancer struct {
	backends          map[types.ID]*Backend
	backendIDs        []types.ID
	current           int          // Current index for round-robin
	currentBestLeader types.ID     // Current best guess ID of the leader
	mu                sync.RWMutex // Protects currentBestLeader
}

// MetadataStatus represents the response from the /status endpoint
type MetadataStatus struct {
	MetadataInfo struct {
		RaftStatus struct {
			LeaderID string `json:"leader_id"`
			Voters   string `json:"voters"`
		} `json:"raft_status"`
	} `json:"metadata_info"`
}

// NewLoadBalancer creates a new LoadBalancer
func NewLoadBalancer(serverURLs map[types.ID]string) *LoadBalancer {
	backends := make(map[types.ID]*Backend, len(serverURLs))
	currBestLeader := types.ID(0)
	for i, u := range serverURLs {
		if currBestLeader == 0 {
			// Initialize currentBestLeader with the first ID
			currBestLeader = i
		}
		parsedURL, err := url.Parse(u)
		if err != nil {
			log.Fatalf("Invalid backend URL: %s, %v", u, err)
		}

		// Create a custom transport with connection pooling
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,              // Maximum idle connections across all hosts
			MaxIdleConnsPerHost:   20,               // Maximum idle connections per host
			MaxConnsPerHost:       50,               // Maximum total connections per host
			IdleConnTimeout:       90 * time.Second, // How long idle connections are kept
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    false, // Enable compression
			ForceAttemptHTTP2:     true,  // Try HTTP/2 when available
		}

		proxy := httputil.NewSingleHostReverseProxy(parsedURL)
		proxy.Transport = transport

		// Customize error handling
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("Proxy error for backend %s: %v", parsedURL, err) //nolint:gosec // G706: internal structured logging
			http.Error(w, "Backend server error", http.StatusBadGateway)
		}

		backends[i] = &Backend{
			URL:       parsedURL,
			Proxy:     proxy,
			Transport: transport,
		}
		// Assume alive initially, health checks will update
		backends[i].Alive.Store(true)
	}
	return &LoadBalancer{
		backends:          backends,
		backendIDs:        slices.Collect(maps.Keys(backends)),
		current:           0,
		currentBestLeader: currBestLeader,
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set allowed origin(s) - replace "*" with specific origins for production
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().
			Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			// Add other headers if needed

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ServeHTTP handles incoming requests
func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// For write operations (POST, PUT, DELETE), prefer routing to the leader
	if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete {
		leaderID := lb.getLeader()
		if backend, ok := lb.backends[leaderID]; ok && backend.Alive.Load() {
			// log.Printf("Routing %s request to leader %s", r.Method, leaderID)
			backend.Proxy.ServeHTTP(w, r) //nolint:gosec // G704: HTTP client calling configured endpoint
			return
		}
	}

	// Find the next available backend in a round-robin fashion
	for range lb.backends {
		lb.current = (lb.current + 1) % len(lb.backends)
		backend := lb.backends[lb.backendIDs[lb.current]]
		if backend.Alive.Load() {
			backend.Proxy.ServeHTTP(w, r) //nolint:gosec // G704: HTTP client calling configured endpoint
			return
		}
	}

	http.Error(w, "No healthy backend servers available", http.StatusServiceUnavailable)
}

// CloseIdleConnections closes all idle connections for all backends
func (lb *LoadBalancer) CloseIdleConnections() {
	for _, backend := range lb.backends {
		backend.Transport.CloseIdleConnections()
	}
}

// updateLeaderInfo queries metadata servers for the current Raft leader
func (lb *LoadBalancer) updateLeaderInfo() {
	for id, backend := range lb.backends {
		if !backend.Alive.Load() {
			continue
		}

		func() {
			// Query the status endpoint
			statusURL := backend.URL.String() + "/status"
			resp, err := http.Get(statusURL) //nolint:gosec // G107: health check to configured backend
			if err != nil {
				log.Printf("Failed to query status from backend %s: %v", id, err)
				return
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				log.Printf("Non-OK status from backend %s: %d", id, resp.StatusCode) //nolint:gosec // G706: internal structured logging
				return
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Failed to read response from backend %s: %v", id, err)
				return
			}

			var status MetadataStatus
			if err := json.Unmarshal(body, &status); err != nil {
				log.Printf("Failed to parse status from backend %s: %v", id, err)
				return
			}

			// Parse the leader ID from the response
			if status.MetadataInfo.RaftStatus.LeaderID != "" {
				leaderID, err := types.IDFromString(status.MetadataInfo.RaftStatus.LeaderID)
				if err != nil {
					log.Printf("Failed to parse leader ID from backend %s: %v", id, err)
					return
				}

				lb.mu.Lock()
				if lb.currentBestLeader != leaderID {
					log.Printf("Updating leader from %s to %s", lb.currentBestLeader, leaderID)
					lb.currentBestLeader = leaderID
				}
				lb.mu.Unlock()
				return // Successfully got leader info
			}
		}()
	}
}

// getLeader returns the current best guess of the leader ID
func (lb *LoadBalancer) getLeader() types.ID {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.currentBestLeader
}

func main() {
	backendURLs := map[types.ID]string{
		11: "http://127.0.0.1:12277",
		12: "http://127.0.0.1:12278",
		13: "http://127.0.0.1:12279",
	}

	lb := NewLoadBalancer(backendURLs)

	// Periodically update leader information
	go func() {
		// Initial update
		lb.updateLeaderInfo()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			lb.updateLeaderInfo()
		}
	}()

	// Periodically close idle connections to prevent stale connections
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			lb.CloseIdleConnections()
			log.Println("Closed idle connections")
		}
	}()

	log.Println("Starting load balancer on :8080")
	srv := http.Server{
		Addr:        ":8080",
		Handler:     corsMiddleware(lb),
		ReadTimeout: 540 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
