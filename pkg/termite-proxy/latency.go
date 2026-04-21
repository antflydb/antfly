// Copyright 2025 Antfly, Inc.
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

package proxy

import (
	"math"
	"time"
)

const (
	defaultLatencyWindowSize = 128
	latencyBucketWidth       = 10 * time.Millisecond
	latencyBucketLimit       = 5 * time.Second
	latencyOverflowUpper     = 30 * time.Second
)

var latencyBucketCount = int(latencyBucketLimit / latencyBucketWidth)

// RollingLatency tracks a bounded rolling latency distribution.
// Samples are stored in a fixed-size ring and aggregated into coarse buckets so
// percentile lookups stay O(bucket_count) with constant memory.
type RollingLatency struct {
	samples      []int
	next         int
	count        int
	bucketCounts []int
}

func NewRollingLatency(windowSize int) *RollingLatency {
	if windowSize <= 0 {
		windowSize = defaultLatencyWindowSize
	}
	return &RollingLatency{
		samples:      make([]int, windowSize),
		bucketCounts: make([]int, latencyBucketCount+1),
	}
}

func (r *RollingLatency) Record(duration time.Duration) {
	if r == nil || len(r.samples) == 0 {
		return
	}

	bucket := latencyBucketIndex(duration)
	if r.count == len(r.samples) {
		evictedBucket := r.samples[r.next]
		r.bucketCounts[evictedBucket]--
	} else {
		r.count++
	}

	r.samples[r.next] = bucket
	r.bucketCounts[bucket]++
	r.next = (r.next + 1) % len(r.samples)
}

func (r *RollingLatency) MergeInto(dst []int) int {
	if r == nil {
		return 0
	}
	for i, count := range r.bucketCounts {
		dst[i] += count
	}
	return r.count
}

func QuantileFromBuckets(bucketCounts []int, sampleCount int, quantile float64) (time.Duration, bool) {
	if sampleCount == 0 || len(bucketCounts) == 0 {
		return 0, false
	}

	if quantile <= 0 {
		quantile = 0
	}
	if quantile > 1 {
		quantile = 1
	}

	targetRank := int(math.Ceil(quantile * float64(sampleCount)))
	if targetRank < 1 {
		targetRank = 1
	}

	seen := 0
	for i, count := range bucketCounts {
		seen += count
		if seen >= targetRank {
			return latencyBucketUpperBound(i), true
		}
	}

	return latencyBucketUpperBound(len(bucketCounts) - 1), true
}

func latencyBucketIndex(duration time.Duration) int {
	if duration <= 0 {
		return 0
	}
	if duration >= latencyBucketLimit {
		return latencyBucketCount
	}
	return int((duration - 1) / latencyBucketWidth)
}

func latencyBucketUpperBound(bucket int) time.Duration {
	if bucket >= latencyBucketCount {
		return latencyOverflowUpper
	}
	return time.Duration(bucket+1) * latencyBucketWidth
}
