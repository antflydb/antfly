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

package indexes

import (
	"time"

	"github.com/antflydb/antfly/lib/inflight"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	writeOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "indexes",
			Name:      "write_ops_total",
			Help:      "The total number of writes.",
		},
		[]string{"Name"},
	)
	deleteOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "indexes",
			Name:      "delete_ops_total",
			Help:      "The total number of deletes.",
		},
		[]string{"Name"},
	)
	queryOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "indexes",
			Name:      "query_ops_total",
			Help:      "The total number of queries.",
		},
		[]string{"Name"},
	)
	queryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "antfly",
			Subsystem: "indexes",
			Name:      "query_duration_seconds",
			Help:      "Index query latency in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"Name", "query_type"},
	)
	embeddingCreationOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "indexes",
			Name:      "embedding_creation_ops_total",
			Help:      "The total number of embedding creations.",
		},
		[]string{"Name"},
	)
	embeddingPersistOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "indexes",
			Name:      "embedding_persist_ops_total",
			Help:      "The total number of embedding persists.",
		},
		[]string{"Name"},
	)

	// WAL buffer metrics
	walDequeueAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "wal",
			Name:      "dequeue_attempts_total",
			Help:      "Total dequeue attempts from WAL buffer.",
		},
		[]string{"buffer_id"},
	)
	walDequeueFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "wal",
			Name:      "dequeue_failures_total",
			Help:      "Total dequeue failures by error type.",
		},
		[]string{"buffer_id", "error_type"},
	)
	walItemsDiscarded = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "wal",
			Name:      "items_discarded_total",
			Help:      "Total items discarded after max retries.",
		},
		[]string{"buffer_id"},
	)
	walPendingRetries = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "antfly",
			Subsystem: "wal",
			Name:      "pending_retries",
			Help:      "Current number of items pending retry.",
		},
		[]string{"buffer_id"},
	)

	// Backfill progress metrics
	backfillProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "antfly",
			Subsystem: "enricher",
			Name:      "backfill_progress",
			Help:      "Estimated backfill progress as a ratio from 0.0 to 1.0.",
		},
		[]string{"index"},
	)
	backfillItemsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "enricher",
			Name:      "backfill_items_processed_total",
			Help:      "Total items processed during enricher backfill.",
		},
		[]string{"index"},
	)
	enricherWALBacklog = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "antfly",
			Subsystem: "enricher",
			Name:      "wal_backlog",
			Help:      "Current number of logical items in the enricher WAL buffer.",
		},
		[]string{"index"},
	)
)

func init() {
	prometheus.MustRegister(writeOps)
	prometheus.MustRegister(deleteOps)
	prometheus.MustRegister(queryOps)
	prometheus.MustRegister(queryDuration)
	prometheus.MustRegister(embeddingCreationOps)
	prometheus.MustRegister(embeddingPersistOps)
	prometheus.MustRegister(walDequeueAttempts)
	prometheus.MustRegister(walDequeueFailures)
	prometheus.MustRegister(walItemsDiscarded)
	prometheus.MustRegister(walPendingRetries)
	prometheus.MustRegister(backfillProgress)
	prometheus.MustRegister(backfillItemsProcessed)
	prometheus.MustRegister(enricherWALBacklog)
}

func observeQueryDuration(name, queryType string, start time.Time) {
	queryDuration.WithLabelValues(name, queryType).Observe(time.Since(start).Seconds())
}

// PrometheusWALMetrics implements inflight.WALBufferMetrics using Prometheus.
type PrometheusWALMetrics struct {
	bufferID string
}

// NewPrometheusWALMetrics creates a new PrometheusWALMetrics for the given buffer ID.
func NewPrometheusWALMetrics(bufferID string) *PrometheusWALMetrics {
	return &PrometheusWALMetrics{bufferID: bufferID}
}

func (m *PrometheusWALMetrics) IncDequeueAttempts(count int) {
	walDequeueAttempts.WithLabelValues(m.bufferID).Add(float64(count))
}

func (m *PrometheusWALMetrics) IncDequeueFailure(errorType string) {
	walDequeueFailures.WithLabelValues(m.bufferID, errorType).Inc()
}

func (m *PrometheusWALMetrics) IncItemsDiscarded(count int) {
	walItemsDiscarded.WithLabelValues(m.bufferID).Add(float64(count))
}

func (m *PrometheusWALMetrics) SetPendingRetries(count int) {
	walPendingRetries.WithLabelValues(m.bufferID).Set(float64(count))
}

// Ensure PrometheusWALMetrics implements inflight.WALBufferMetrics
var _ inflight.WALBufferMetrics = (*PrometheusWALMetrics)(nil)

// estimateProgress estimates the progress of a scan through a byte range.
// Returns a value between 0.0 and 1.0 based on comparing the first few bytes
// of currentKey against rangeStart and rangeEnd.
func estimateProgress(rangeStart, rangeEnd, currentKey []byte) float64 {
	if len(rangeEnd) == 0 || len(currentKey) == 0 {
		return 0
	}
	// Use first 8 bytes for a rough numeric comparison
	current := keyToUint64(currentKey)
	start := keyToUint64(rangeStart)
	end := keyToUint64(rangeEnd)
	if end <= start {
		return 1.0
	}
	if current <= start {
		return 0
	}
	if current >= end {
		return 1.0
	}
	return float64(current-start) / float64(end-start)
}

func keyToUint64(key []byte) uint64 {
	var v uint64
	for i := range 8 {
		v <<= 8
		if i < len(key) {
			v |= uint64(key[i])
		}
	}
	return v
}
