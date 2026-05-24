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

import "github.com/prometheus/client_golang/prometheus"

var (
	activePeers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "antfly",
			Subsystem: "network",
			Name:      "active_peers",
			Help:      "The current number of active peer connections.",
		},
		[]string{"local", "remote"},
	)

	disconnectedPeers = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "network",
			Name:      "disconnected_peers_total",
			Help:      "The total number of disconnected peers.",
		},
		[]string{"local", "remote"},
	)

	sentBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "network",
			Name:      "peer_sent_bytes_total",
			Help:      "The total number of bytes sent to peers.",
		},
		[]string{"to"},
	)

	receivedBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "network",
			Name:      "peer_received_bytes_total",
			Help:      "The total number of bytes received from peers.",
		},
		[]string{"from"},
	)

	sentFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "network",
			Name:      "peer_sent_failures_total",
			Help:      "The total number of send failures from peers.",
		},
		[]string{"to"},
	)

	recvFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "network",
			Name:      "peer_received_failures_total",
			Help:      "The total number of receive failures from peers.",
		},
		[]string{"from"},
	)

	rttSec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "antfly",
			Subsystem: "network",
			Name:      "peer_round_trip_time_seconds",
			Help:      "Round-Trip-Time histogram between peers",

			// lowest bucket start of upper bound 0.0001 sec (0.1 ms) with factor 2
			// highest bucket start of 0.0001 sec * 2^15 == 3.2768 sec
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
		},
		[]string{"to"},
	)

	shardNotFoundTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "raft",
			Name:      "shard_not_found_total",
			Help:      "Total number of Raft messages for shards that don't exist on this node.",
		},
		[]string{"shard_id", "message_type"},
	)
)

func init() {
	prometheus.MustRegister(activePeers)
	prometheus.MustRegister(disconnectedPeers)
	prometheus.MustRegister(sentBytes)
	prometheus.MustRegister(receivedBytes)
	prometheus.MustRegister(sentFailures)
	prometheus.MustRegister(recvFailures)

	prometheus.MustRegister(rttSec)
	prometheus.MustRegister(shardNotFoundTotal)
}
