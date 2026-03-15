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

package foreign

import (
	"fmt"
	"sync"

	"github.com/antflydb/antfly/lib/secrets"
)

// PoolManager caches DataSource instances keyed by resolved DSN.
// Thread-safe via RWMutex. Lazy creation: pools are opened on first use.
type PoolManager struct {
	mu    sync.RWMutex
	pools map[string]DataSource // keyed by resolved DSN
}

// NewPoolManager creates a new PoolManager.
func NewPoolManager() *PoolManager {
	return &PoolManager{
		pools: make(map[string]DataSource),
	}
}

// Get returns a DataSource for the given source type and DSN, creating one if needed.
// The DSN may contain ${secret:...} references which are resolved via the global secrets resolver.
func (pm *PoolManager) Get(sourceType, dsn string) (DataSource, error) {
	resolvedDSN, err := secrets.GetGlobalResolver().Resolve(dsn)
	if err != nil {
		return nil, fmt.Errorf("resolving DSN secrets: %w", err)
	}

	pm.mu.RLock()
	ds, ok := pm.pools[resolvedDSN]
	pm.mu.RUnlock()
	if ok {
		return ds, nil
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Double-check after acquiring write lock
	if ds, ok := pm.pools[resolvedDSN]; ok {
		return ds, nil
	}

	ds, err = NewDataSource(sourceType, resolvedDSN)
	if err != nil {
		return nil, fmt.Errorf("creating data source: %w", err)
	}
	pm.pools[resolvedDSN] = ds
	return ds, nil
}

// Close closes all cached DataSource instances.
func (pm *PoolManager) Close() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for dsn, ds := range pm.pools {
		ds.Close()
		delete(pm.pools, dsn)
	}
}
