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
	"net/http"
	"strconv"
)

// queryAdmission is deliberately a non-queueing semaphore.  Queuing a search
// after the client has timed out is precisely the overload failure mode this
// protects against: it consumes a connection and eventually starts work that
// no client is waiting for.  Control and health endpoints do not use it.
type queryAdmission struct {
	slots chan struct{}
}

func newQueryAdmission(limit int) *queryAdmission {
	if limit < 1 {
		limit = 1
	}
	return &queryAdmission{slots: make(chan struct{}, limit)}
}

func (a *queryAdmission) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do not admit work for a request that has already been abandoned.
		if err := r.Context().Err(); err != nil {
			return
		}

		select {
		case a.slots <- struct{}{}:
			defer func() { <-a.slots }()
		default:
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-Antfly-Overload", "query-admission")
			http.Error(w, "query capacity exhausted; retry shortly", http.StatusServiceUnavailable)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (a *queryAdmission) inFlight() int { return len(a.slots) }

const defaultConcurrentQueries = 32

// concurrentQueriesFromEnv accepts a small, explicit operational override
// without allowing an invalid value to disable overload protection.
func concurrentQueriesFromEnv(value string) int {
	if value == "" {
		return defaultConcurrentQueries
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 {
		return defaultConcurrentQueries
	}
	return limit
}
