// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueryAdmissionBounds128TimedOutClientsAndRecovers(t *testing.T) {
	const limit = 8
	admission := newQueryAdmission(limit)
	release := make(chan struct{})
	started := make(chan struct{}, limit)
	var active atomic.Int32
	var maxActive atomic.Int32

	handler := admission.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	// Hold the admitted work open, then simulate 128 clients whose deadlines
	// expire.  Excess clients must be rejected without creating work.
	var wg sync.WaitGroup
	var overloaded atomic.Int32
	for range 128 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req := httptest.NewRequest(http.MethodPost, "/search", nil).WithContext(ctx)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code == http.StatusServiceUnavailable {
				overloaded.Add(1)
			}
		}()
	}

	for range limit {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("admitted search did not start")
		}
	}
	wg.Wait()
	close(release)

	require.LessOrEqual(t, maxActive.Load(), int32(limit))
	require.GreaterOrEqual(t, overloaded.Load(), int32(128-limit))
	require.Eventually(t, func() bool { return admission.inFlight() == 0 }, time.Second, time.Millisecond)

	// Once timed-out work has drained, a normal query is admitted immediately.
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/search", nil))
	require.Equal(t, http.StatusNoContent, rr.Code)
}

func TestQueryAdmissionNeverStartsCanceledRequest(t *testing.T) {
	admission := newQueryAdmission(1)
	called := atomic.Bool{}
	handler := admission.wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called.Store(true) }))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/search", nil).WithContext(ctx))
	require.False(t, called.Load())
	require.Zero(t, admission.inFlight())
}

func TestConcurrentQueriesFromEnv(t *testing.T) {
	require.Equal(t, defaultConcurrentQueries, concurrentQueriesFromEnv(""))
	require.Equal(t, 12, concurrentQueriesFromEnv("12"))
	require.Equal(t, defaultConcurrentQueries, concurrentQueriesFromEnv("0"))
	require.Equal(t, defaultConcurrentQueries, concurrentQueriesFromEnv("bad"))
}
