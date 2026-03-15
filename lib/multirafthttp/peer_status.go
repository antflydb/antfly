// Copyright 2015 The etcd Authors
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

package multirafthttp

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/types"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type failureType struct {
	source string
	action string
}

type peerStatus struct {
	lg     *zap.Logger
	local  types.ID
	id     types.ID
	mu     sync.RWMutex // protect variables below
	active bool
	since  time.Time
}

func newPeerStatus(lg *zap.Logger, local, id types.ID) *peerStatus {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &peerStatus{lg: lg, local: local, id: id}
}

func (s *peerStatus) activate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		if peerActivatedLogRateLimiter.Allow() {
			s.lg.Debug("peer became active", zap.Stringer("peer-id", s.id))
		}
		s.active = true
		s.since = time.Now()

		activePeers.WithLabelValues(s.local.String(), s.id.String()).Inc()
	}
}

// Rate limiters for peer status logging to reduce log spam during cluster operations
var (
	peerActivatedLogRateLimiter   = rate.NewLimiter(rate.Every(2*time.Second), 5)
	peerDeactivatedLogRateLimiter = rate.NewLimiter(rate.Every(2*time.Second), 5)
)

func (s *peerStatus) deactivate(failure failureType, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := fmt.Sprintf("failed to %s %s on %s (%s)", failure.action, s.id, failure.source, reason)
	if s.active {
		if peerDeactivatedLogRateLimiter.Allow() {
			s.lg.Debug("peer became inactive (message send to peer failed)", zap.Stringer("peer-id", s.id), zap.Error(errors.New(msg)))
		}
		s.active = false
		s.since = time.Time{}

		activePeers.WithLabelValues(s.local.String(), s.id.String()).Dec()
		disconnectedPeers.WithLabelValues(s.local.String(), s.id.String()).Inc()
		return
	}

	if s.lg != nil {
		if peerDeactivatedLogRateLimiter.Allow() {
			s.lg.Debug("peer deactivated again",
				zap.Stringer("peer-id", s.id), zap.Error(errors.New(msg)))
		}
	}
}

func (s *peerStatus) isActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

func (s *peerStatus) activeSince() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.since
}
