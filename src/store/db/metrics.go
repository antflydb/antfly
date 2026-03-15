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

package db

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	writeOps = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "storage",
			Name:      "write_ops_total",
			Help:      "The total number of writes.",
		},
		[]string{},
	)
	deleteOps = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "storage",
			Name:      "delete_ops_total",
			Help:      "The total number of deletes.",
		},
		[]string{},
	)
	indexOps = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "storage",
			Name:      "indexing_ops_total",
			Help:      "The total number of indexing operations.",
		},
		[]string{},
	)
	queryOps = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "storage",
			Name:      "query_ops_total",
			Help:      "The total number of queries.",
		},
		[]string{},
	)

	// Transaction metrics
	transactionOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "transactions",
			Name:      "ops_total",
			Help:      "Total number of transaction operations",
		},
		[]string{"operation", "status"},
	)

	transactionDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "antfly",
			Subsystem: "transactions",
			Name:      "duration_seconds",
			Help:      "Transaction operation duration in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	intentResolutionDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "antfly",
			Subsystem: "transactions",
			Name:      "intent_resolution_duration_seconds",
			Help:      "Duration of intent resolution operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
	)

	transactionsRecoveredTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "transactions",
			Name:      "recovered_total",
			Help:      "Total number of transactions recovered by the coordinator",
		},
	)

	transactionsCleanedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "transactions",
			Name:      "cleaned_total",
			Help:      "Total number of old transaction records cleaned up",
		},
	)

	activeTransactionsGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "antfly",
			Subsystem: "transactions",
			Name:      "active",
			Help:      "Number of currently active transactions",
		},
	)

	transactionIntentsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "transactions",
			Name:      "intents_total",
			Help:      "Total number of write intents created",
		},
		[]string{"type"}, // "write" or "delete"
	)
)
