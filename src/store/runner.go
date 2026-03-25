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
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp/transport"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/goccy/go-json"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/sethvargo/go-retry"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// registerWithLeader sends a registration request to the leader node
func registerWithLeader(
	ctx context.Context,
	client *http.Client,
	leaderURL string,
	m *Store,
	conf *StoreInfo,
) error {
	// Prepare the registration request
	status := m.Status()
	regReq := StoreRegistrationRequest{
		StoreInfo: *conf,
		Shards:    status.Shards,
	}

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(regReq); err != nil {
		return fmt.Errorf("failed to encode registration request: %w", err)
	}

	// Send the request
	// FIXME (ajr) Use TLS if configured
	req, err := http.NewRequest(http.MethodPost, leaderURL+"/_internal/v1/store", buf)
	if err != nil {
		return fmt.Errorf("creating registration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)
	resp, err := client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return fmt.Errorf("failed to send registration request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("leader returned error status: %s", resp.Status)
	}

	return nil
}

// RegisterWithLeaderWithRetry registers a store with the metadata cluster using
// a default HTTP client. Suitable for swarm mode where the caller manages runtimes.
func RegisterWithLeaderWithRetry(
	ctx context.Context,
	lg *zap.Logger,
	store *Store,
	conf *StoreInfo,
	registrationsURLsMap map[types.ID]string,
) error {
	client := &http.Client{Timeout: 30 * time.Second}
	return registerWithLeaderWithRetry(ctx, lg, client, store, conf, registrationsURLsMap)
}

// registerWithLeaderWithRetry attempts to register with the leader with exponential backoff
// It retries until successful or until the context is canceled
func registerWithLeaderWithRetry(
	ctx context.Context,
	lg *zap.Logger,
	client *http.Client,
	store *Store,
	conf *StoreInfo,
	registrationsURLsMap map[types.ID]string,
) error {
	registrationsURLs := slices.Collect(maps.Values(registrationsURLsMap))
	i := 0

	lg.Info("Registering with metadata servers",
		zap.Any("urls", registrationsURLs))
	b := retry.NewExponential(time.Second)
	b = retry.WithMaxRetries(10, b)
	b = retry.WithCappedDuration(30*time.Second, b)
	return retry.Do(ctx, b, func(ctx context.Context) error {
		if err := registerWithLeader(ctx, client, registrationsURLs[i], store, conf); err != nil {
			lg.Info(
				"Failed to register with metadata server",
				zap.String("leader", registrationsURLs[i]),
				zap.Error(err),
			)
			i = (i + 1) % len(registrationsURLs)
			return retry.RetryableError(err)
		}
		lg.Info("Successfully registered with metadata server",
			zap.String("registrationURL", registrationsURLs[i]), zap.Stringer("storeID", conf.ID),
			zap.String("raftURL", conf.RaftURL), zap.String("apiURL", conf.ApiURL),
		)
		return nil
	})
}

func RunAsStore(
	ctx context.Context,
	zl *zap.Logger,
	c *common.Config,
	conf *StoreInfo,
	serviceHostname string,
	readyC chan<- struct{},
	cache *pebbleutils.Cache,
) {
	storeID := conf.ID
	zl = zl.Named("store").With(zap.Stringer("id", storeID))

	zl.Info("Starting store node",
		zap.Any("config", c),
		zap.Any("storeInfo", conf))

	runtime, err := NewRuntime(zl, c, conf, cache)
	if err != nil {
		zl.Fatal("Failed to create store runtime", zap.Error(err))
	}
	eg, egCtx := errgroup.WithContext(ctx)
	runtime.StartRaft()
	defer func() {
		if err := runtime.Close(); err != nil {
			zl.Error("Failed to close store runtime", zap.Error(err))
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	eg.Go(func() error {
		defer zl.Debug("HTTP server goroutine exited")
		url, err := url.Parse(conf.ApiURL)
		if err != nil {
			return fmt.Errorf("parsing URL: %w", err)
		}

		if url.Scheme == "https" {
			tlsInfo := transport.TLSInfo{
				CertFile:           c.Tls.Cert,
				KeyFile:            c.Tls.Key,
				ClientCertFile:     "certificate.crt",
				ClientKeyFile:      "private.key",
				ClientCertAuth:     true,
				InsecureSkipVerify: true,
			}
			tlsConfig, err := tlsInfo.ServerConfig()
			if err != nil {
				return fmt.Errorf("setting up TLS config: %w", err)
			}
			server := &http3.Server{
				QUICConfig:  &quic.Config{},
				Addr:        url.Host,
				Handler:     runtime.HTTPHandler(),
				IdleTimeout: time.Minute,
				TLSConfig:   tlsConfig,
			}
			eg.Go(func() error {
				wg.Done()
				if err := server.ListenAndServe(); err != nil {
					if err == http.ErrServerClosed {
						runtime.Store().logger.Info("HTTP server closed gracefully")
						return nil
					}
					return fmt.Errorf("HTTP/3 server failed: %w", err)
				}
				return nil
			})
			defer func() { _ = server.Close() }()
		} else {
			server := &http.Server{
				Addr:        url.Host,
				Handler:     runtime.HTTPHandler(),
				ReadTimeout: time.Minute,
			}
			eg.Go(func() error {
				wg.Done()
				if err := server.ListenAndServe(); err != nil {
					if err == http.ErrServerClosed {
						runtime.Store().logger.Info("HTTP server closed gracefully")
						return nil
					}
					return fmt.Errorf("HTTP server failed: %w", err)
				}
				return nil
			})
			defer func() { _ = server.Close() }()
		}
		select {
		case <-egCtx.Done():
			runtime.Store().logger.Info("HTTP server stopped")
			return nil
		case err, ok := <-runtime.ErrorC():
			if !ok {
				runtime.Store().logger.Info("Error channel closed, stopping store")
				return nil
			}
			// FIXME (ajr) How do we handle the error channels for multiraft?
			// exit when raft goes down
			return fmt.Errorf("error occurred: %w", err)
		}
	})
	// Give time for the API server to start
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	// Signal ready after HTTP server has started
	if readyC != nil {
		close(readyC)
		zl.Info("Store HTTP server is ready")
	}

	eg.Go(func() error {
		t := &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     time.Minute,
			DisableKeepAlives:   false,
			ForceAttemptHTTP2:   true,
		}
		if c.Tls.Cert != "" && c.Tls.Key != "" {
			tlsInfo := transport.TLSInfo{
				CertFile: c.Tls.Cert,
				KeyFile:  c.Tls.Key,
			}
			tlsConfig, err := tlsInfo.ClientConfig()
			if err != nil {
				return fmt.Errorf("setting up TLS config: %w", err)
			}
			tr := &http3.Transport{
				TLSClientConfig: tlsConfig,
				QUICConfig: &quic.Config{
					HandshakeIdleTimeout: 10 * time.Second,
					MaxIdleTimeout:       10 * time.Second,
					KeepAlivePeriod:      5 * time.Second,
				}, // QUIC connection options
			}
			defer func() { _ = tr.Close() }()
			t.RegisterProtocol("https", tr)
		}
		client := &http.Client{
			Timeout:   time.Second * 540,
			Transport: t,
		}
		if serviceHostname != "" {
			apiUrl, _ := url.Parse(conf.ApiURL)
			raftUrl, _ := url.Parse(conf.RaftURL)
			apiUrl.Host = serviceHostname + ":" + apiUrl.Port()
			raftUrl.Host = serviceHostname + ":" + raftUrl.Port()
			conf.ApiURL = apiUrl.String()
			conf.RaftURL = raftUrl.String()
		}
		// Errors are checked at configuration parsing time
		orchURLs, _ := c.Metadata.GetOrchestrationURLs()
		return registerWithLeaderWithRetry(egCtx, zl, client, runtime.Store(), conf, orchURLs)
	})
	if err := eg.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			zl.Info("Store shut down")
			return
		}
		zl.Fatal("Store failure", zap.Error(err))
	}
}
