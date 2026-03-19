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
	"fmt"
	"net/http"
	"sync"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft"
	"go.uber.org/zap"
)

// Runtime owns the long-lived store components without binding them to any
// specific bootstrap mechanism such as real network listeners.
type Runtime struct {
	logger *zap.Logger
	config *common.Config
	info   *StoreInfo

	raft  *raft.MultiRaft
	store *Store
	errC  <-chan error

	closeOnce sync.Once
	closeErr  error
}

func NewRuntime(
	zl *zap.Logger,
	c *common.Config,
	conf *StoreInfo,
	cache *pebbleutils.Cache,
) (*Runtime, error) {
	dataDir := c.GetBaseDir()
	rs, err := raft.NewMultiRaftServer(zl, dataDir, conf.ID, conf.RaftURL)
	if err != nil {
		return nil, fmt.Errorf("creating raft server: %w", err)
	}

	store, errChan, err := NewStore(zl, c, rs, conf, cache)
	if err != nil {
		return nil, fmt.Errorf("creating store: %w", err)
	}

	return &Runtime{
		logger: zl,
		config: c,
		info:   conf,
		raft:   rs,
		store:  store,
		errC:   errChan,
	}, nil
}

func (r *Runtime) StartRaft() {
	go r.raft.Start()
}

func (r *Runtime) HTTPHandler() http.Handler {
	return r.store.NewHttpAPI()
}

func (r *Runtime) ErrorC() <-chan error {
	return r.errC
}

func (r *Runtime) Store() *Store {
	return r.store
}

func (r *Runtime) Close() error {
	r.closeOnce.Do(func() {
		if r.store != nil {
			r.store.Close()
		}
		if r.raft != nil {
			if err := r.raft.Stop(); err != nil {
				if r.closeErr == nil {
					r.closeErr = err
				}
				r.logger.Error("Failed to stop Raft server", zap.Error(err))
			} else {
				r.logger.Info("Raft server stopped gracefully")
			}
		}
	})
	return r.closeErr
}
