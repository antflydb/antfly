// Copyright 2025 Antfly, Inc.
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

// Package healthserver provides a shared health/metrics server for Kubernetes probes.
package healthserver

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Start starts a health/metrics server on the specified port.
// This provides:
//   - /healthz - Kubernetes liveness probe (always returns 200 if process is alive)
//   - /readyz  - Kubernetes readiness probe (calls readyChecker to verify readiness)
//   - /metrics - Prometheus metrics endpoint
//
// The server runs in a goroutine and does not block.
func Start(logger *zap.Logger, port int, readyChecker func() bool) {
	http.Handle("/metrics", promhttp.Handler())

	// Liveness probe - process is alive
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte("ok")); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	// Readiness probe - servers are ready to accept traffic
	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if readyChecker != nil && readyChecker() {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("ready")); err != nil {
				logger.Error("failed to write ready response", zap.Error(err))
			}
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte("not ready")); err != nil {
				logger.Error("failed to write not ready response", zap.Error(err))
			}
		}
	})

	go func() {
		addr := fmt.Sprintf("0.0.0.0:%d", port)
		server := &http.Server{
			Addr:              addr,
			ReadHeaderTimeout: 40 * time.Second,
		}
		logger.Info("Starting health/metrics server", zap.String("addr", addr))
		if err := server.ListenAndServe(); err != nil {
			logger.Error("Health server error", zap.Error(err))
		}
	}()
}
