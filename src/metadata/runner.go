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

package metadata

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// RunAsMetadataServer implements a leader node that monitors and manages the cluster
// The readyC channel will be closed when all HTTP servers are ready to accept connections
func RunAsMetadataServer(
	ctx context.Context,
	zl *zap.Logger,
	config *common.Config,
	conf *store.StoreInfo,
	peers common.Peers,
	join bool,
	readyC chan<- struct{},
	cache *pebbleutils.Cache,
) {
	zl = zl.Named("metadataServer")
	zl.Info("Starting metadata node",
		zap.Stringer("peers", peers),
		zap.Any("config", config),
		zap.Any("storeInfo", conf))

	u, err := url.Parse(conf.ApiURL)
	if err != nil {
		zl.Fatal("Error parsing API URL", zap.Error(err))
	}

	runtime, err := NewRuntime(zl, config, conf, peers, join, cache)
	if err != nil {
		zl.Fatal("Failed to create metadata runtime", zap.Error(err))
	}
	runtime.StartRaft()
	defer func() {
		if err := runtime.Close(); err != nil {
			zl.Error("failed to close metadata runtime", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	eg, egCtx := errgroup.WithContext(ctx)
	defer cancel()

	// Combined API server replaces both internal and public API servers
	eg.Go(func() error {
		srv := http.Server{
			Addr:        u.Host,
			Handler:     runtime.HTTPHandler(),
			ReadTimeout: 10 * time.Second,
		}

		// Graceful shutdown
		go func() {
			<-egCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()

		zl.Info("Combined API server starting", zap.String("address", u.Host))

		// Create listener first so we know when we're ready
		listener, err := net.Listen("tcp", u.Host)
		if err != nil {
			return fmt.Errorf("failed to create listener: %w", err)
		}

		// Signal that API server is ready
		if readyC != nil {
			close(readyC)
			zl.Info("API server is ready")
		}

		// Serve on single port
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			zl.Error("HTTP server error", zap.Error(err))
			return fmt.Errorf("starting HTTP server: %w", err)
		} else if errors.Is(err, http.ErrServerClosed) {
			zl.Info("HTTP server closed gracefully")
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		zl.Fatal("HTTP server error", zap.Error(err))
	}
	zl.Info("HTTP server stopped")
}
