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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/oauth2"
)

type providerClientEntry struct {
	connector  Connector
	connection Connection
	used       uint64
}
type providerClientBuild struct {
	done      chan struct{}
	connector Connector
	err       error
}
type providerClients struct {
	mu       sync.Mutex
	epoch    atomic.Uint64
	tick     uint64
	entries  map[string]providerClientEntry
	building map[string]*providerClientBuild
	closed   bool
	secrets  map[string]uint64
}

func closeProviderClient(connector Connector) {
	if closer, ok := connector.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
		return
	}
	switch client := connector.(type) {
	case *GoogleConnector:
		closeGoogleTransport(client.HTTPClient)
	case *CollaborationConnector:
		closeGoogleTransport(client.HTTPClient)
	}
}
func (s *Service) providerConnector(ctx context.Context, connection Connection) (Connector, error) {
	raw, err := json.Marshal(connection)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	generation := s.clientGeneration(connection)
	key := fmt.Sprintf("%s/%x/%s", generation, reflect.ValueOf(s.factory).Pointer(), hex.EncodeToString(digest[:]))
	pool := &s.clients
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, context.Canceled
	}
	if entry, ok := pool.entries[key]; ok {
		pool.tick++
		entry.used = pool.tick
		pool.entries[key] = entry
		pool.mu.Unlock()
		return entry.connector, nil
	}
	if build, ok := pool.building[key]; ok {
		pool.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-build.done:
			return build.connector, build.err
		}
	}
	if pool.building == nil {
		pool.building = map[string]*providerClientBuild{}
	}
	build := &providerClientBuild{done: make(chan struct{})}
	pool.building[key] = build
	pool.mu.Unlock()
	// Token sources retain a service-lifetime context, not the context of the first
	// request. Refresh itself has a bounded HTTP client; data operations use their
	// own cancellable deadline. In particular, cancelling one request cannot poison
	// every later request using the same refreshed Google credential.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	credentialContext := context.WithValue(s.ctx, oauth2.HTTPClient, &http.Client{Transport: transport, Timeout: 30 * time.Second})
	connector, err := s.factory(credentialContext, connection)
	pool.mu.Lock()
	delete(pool.building, key)
	if err == nil && (pool.closed || generation != s.clientGenerationLocked(connection)) {
		err = fmt.Errorf("%w: connection generation changed", ErrInvalidInput)
	}
	var retired Connector
	if err == nil {
		if pool.entries == nil {
			pool.entries = map[string]providerClientEntry{}
		}
		if len(pool.entries) >= 64 {
			var oldest string
			var age uint64 = ^uint64(0)
			for id, entry := range pool.entries {
				if entry.used < age {
					oldest, age = id, entry.used
				}
			}
			retired = pool.entries[oldest].connector
			delete(pool.entries, oldest)
		}
		pool.tick++
		pool.entries[key] = providerClientEntry{connector: connector, connection: connection, used: pool.tick}
		build.connector = connector
	} else if connector != nil {
		retired = connector
	}
	build.err = err
	close(build.done)
	pool.mu.Unlock()
	closeProviderClient(retired)
	return build.connector, build.err
}
func (s *Service) invalidateClients() {
	pool := &s.clients
	pool.mu.Lock()
	pool.epoch.Add(1)
	entries := pool.entries
	pool.entries = nil
	pool.mu.Unlock()
	for _, entry := range entries {
		closeProviderClient(entry.connector)
	}
}
func (s *Service) closeClients() {
	s.clients.mu.Lock()
	s.clients.closed = true
	s.clients.mu.Unlock()
	s.invalidateClients()
}

func closeGoogleTransport(client *http.Client) {
	if client == nil {
		return
	}
	var transport http.RoundTripper = client.Transport
	if wrapped, ok := transport.(*googleCredentialTransport); ok {
		transport = wrapped.Transport
	}
	if oauth, ok := transport.(*oauth2.Transport); ok {
		transport = oauth.Base
	}
	if closer, ok := transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// Called under the pool mutex. Credential generations depend only on secrets
// referenced by this connection, so unrelated registration cannot revoke reads.
func (s *Service) clientGenerationLocked(connection Connection) string {
	versions := make(map[string]uint64, len(connection.SecretRefs))
	for _, key := range connection.SecretRefs {
		versions[key] = s.clients.secrets[key]
	}
	raw, _ := json.Marshal(struct {
		Epoch   uint64
		Secrets map[string]uint64
	}{s.clients.epoch.Load(), versions})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (s *Service) clientGeneration(connection Connection) string {
	s.clients.mu.Lock()
	defer s.clients.mu.Unlock()
	return s.clientGenerationLocked(connection)
}
func (s *Service) invalidateSecret(key string) {
	pool := &s.clients
	pool.mu.Lock()
	if pool.secrets == nil {
		pool.secrets = map[string]uint64{}
	}
	pool.secrets[key]++
	retired := []Connector{}
	for id, entry := range pool.entries {
		for _, ref := range entry.connection.SecretRefs {
			if ref == key {
				retired = append(retired, entry.connector)
				delete(pool.entries, id)
				break
			}
		}
	}
	pool.mu.Unlock()
	for _, connector := range retired {
		closeProviderClient(connector)
	}
}
func (s *Service) retireConnection(connection Connection) {
	identity := connectionMediaBinding(connection)
	pool := &s.clients
	pool.mu.Lock()
	retired := []Connector{}
	for id, entry := range pool.entries {
		if connectionMediaBinding(entry.connection) == identity {
			retired = append(retired, entry.connector)
			delete(pool.entries, id)
		}
	}
	pool.mu.Unlock()
	for _, connector := range retired {
		closeProviderClient(connector)
	}
}
