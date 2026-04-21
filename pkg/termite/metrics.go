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

package termite

import "github.com/prometheus/client_golang/prometheus"

var (
	embeddingRequestOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "embedding_request_ops_total",
			Help:      "The total number of embedding requests.",
		},
		[]string{"provider"},
	)
	embeddingCreationOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "embedding_creation_ops_total",
			Help:      "The total number of embedding creations.",
		},
		[]string{"provider"},
	)

	rerankerRequestOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "reranker_request_ops_total",
			Help:      "The total number of reranker requests.",
		},
		[]string{"model"},
	)
	rerankingCreationOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "reranking_creation_ops_total",
			Help:      "The total number of documents reranked.",
		},
		[]string{"model"},
	)

	chunkerRequestOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "chunker_request_ops_total",
			Help:      "The total number of chunker requests.",
		},
		[]string{"model"},
	)
	chunkCreationOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "chunk_creation_ops_total",
			Help:      "The total number of chunks created.",
		},
		[]string{"model"},
	)

	generatorRequestOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "generator_request_ops_total",
			Help:      "The total number of generator (LLM) requests.",
		},
		[]string{"model"},
	)
	tokenGenerationOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "token_generation_ops_total",
			Help:      "The total number of tokens generated.",
		},
		[]string{"model"},
	)
	nerRequestOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "ner_request_ops_total",
			Help:      "The total number of NER requests.",
		},
		[]string{"model"},
	)
	nerCreationOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "ner_creation_ops_total",
			Help:      "The total number of entities extracted.",
		},
		[]string{"model"},
	)

	extractionRequestOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "extraction_request_ops_total",
			Help:      "The total number of JSON extraction requests.",
		},
		[]string{"model"},
	)
	extractionFieldsOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "extraction_fields_total",
			Help:      "The total number of fields extracted.",
		},
		[]string{"model"},
	)

	readerRequestOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "reader_request_ops_total",
			Help:      "The total number of reader (OCR/Vision2Seq) requests.",
		},
		[]string{"model"},
	)

	transcriberRequestOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "transcriber_request_ops_total",
			Help:      "The total number of transcriber (Speech2Seq) requests.",
		},
		[]string{"model"},
	)

	modelLoadDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "model_load_duration_seconds",
			Help:      "Time taken to load a model.",
			Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
		[]string{"model", "type"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "request_duration_seconds",
			Help:      "Time taken to process a request.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"endpoint", "model", "status"},
	)

	cacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "cache_hits_total",
			Help:      "Total number of cache hits.",
		},
		[]string{"type"}, // chunking, embedding
	)

	cacheMisses = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "cache_misses_total",
			Help:      "Total number of cache misses.",
		},
		[]string{"type"}, // chunking, embedding
	)

	// Queue metrics
	queueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "queue_depth",
			Help:      "Number of requests currently waiting in queue.",
		},
	)

	queueActiveRequests = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "queue_active_requests",
			Help:      "Number of requests currently being processed.",
		},
	)

	queueRejectedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "queue_rejected_total",
			Help:      "Total number of requests rejected due to full queue.",
		},
	)

	queueTimedOutTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "queue_timed_out_total",
			Help:      "Total number of requests that timed out while waiting in queue.",
		},
	)

	queueWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "antfly",
			Subsystem: "termite",
			Name:      "queue_wait_duration_seconds",
			Help:      "Time spent waiting in queue before processing.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
	)
)

func init() {
	prometheus.MustRegister(embeddingRequestOps)
	prometheus.MustRegister(embeddingCreationOps)
	prometheus.MustRegister(rerankerRequestOps)
	prometheus.MustRegister(rerankingCreationOps)
	prometheus.MustRegister(chunkerRequestOps)
	prometheus.MustRegister(chunkCreationOps)
	prometheus.MustRegister(generatorRequestOps)
	prometheus.MustRegister(tokenGenerationOps)
	prometheus.MustRegister(nerRequestOps)
	prometheus.MustRegister(nerCreationOps)
	prometheus.MustRegister(extractionRequestOps)
	prometheus.MustRegister(extractionFieldsOps)
	prometheus.MustRegister(readerRequestOps)
	prometheus.MustRegister(transcriberRequestOps)
	prometheus.MustRegister(modelLoadDuration)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(cacheHits)
	prometheus.MustRegister(cacheMisses)
	prometheus.MustRegister(queueDepth)
	prometheus.MustRegister(queueActiveRequests)
	prometheus.MustRegister(queueRejectedTotal)
	prometheus.MustRegister(queueTimedOutTotal)
	prometheus.MustRegister(queueWaitDuration)
}

// RecordModelLoadDuration records how long it took to load a model
func RecordModelLoadDuration(model, modelType string, seconds float64) {
	modelLoadDuration.WithLabelValues(model, modelType).Observe(seconds)
}

// RecordRequestDuration records how long a request took
func RecordRequestDuration(endpoint, model, status string, seconds float64) {
	requestDuration.WithLabelValues(endpoint, model, status).Observe(seconds)
}

// RecordCacheHit increments the cache hit counter
func RecordCacheHit(cacheType string) {
	cacheHits.WithLabelValues(cacheType).Inc()
}

// RecordCacheMiss increments the cache miss counter
func RecordCacheMiss(cacheType string) {
	cacheMisses.WithLabelValues(cacheType).Inc()
}

// UpdateQueueMetrics updates all queue-related metrics from QueueStats
func UpdateQueueMetrics(stats QueueStats) {
	queueDepth.Set(float64(stats.CurrentQueued))
	queueActiveRequests.Set(float64(stats.CurrentActive))
}

// RecordQueueRejection increments the rejected counter
func RecordQueueRejection() {
	queueRejectedTotal.Inc()
}

// RecordQueueTimeout increments the timeout counter
func RecordQueueTimeout() {
	queueTimedOutTotal.Inc()
}

// RecordQueueWaitTime records how long a request waited in queue
func RecordQueueWaitTime(seconds float64) {
	queueWaitDuration.Observe(seconds)
}

// RecordRerankerRequest increments the reranker request counter
func RecordRerankerRequest(model string) {
	rerankerRequestOps.WithLabelValues(model).Inc()
}

// RecordRerankingCreation records the number of documents reranked
func RecordRerankingCreation(model string, count int) {
	rerankingCreationOps.WithLabelValues(model).Add(float64(count))
}

// RecordChunkerRequest increments the chunker request counter
func RecordChunkerRequest(model string) {
	chunkerRequestOps.WithLabelValues(model).Inc()
}

// RecordChunkCreation records the number of chunks created
func RecordChunkCreation(model string, count int) {
	chunkCreationOps.WithLabelValues(model).Add(float64(count))
}

// RecordGeneratorRequest increments the generator request counter
func RecordGeneratorRequest(model string) {
	generatorRequestOps.WithLabelValues(model).Inc()
}

// RecordTokenGeneration records the number of tokens generated
func RecordTokenGeneration(model string, count int) {
	tokenGenerationOps.WithLabelValues(model).Add(float64(count))
}

// RecordNERRequest increments the NER request counter
func RecordNERRequest(model string) {
	nerRequestOps.WithLabelValues(model).Inc()
}

// RecordNERCreation records the number of entities extracted
func RecordNERCreation(model string, count int) {
	nerCreationOps.WithLabelValues(model).Add(float64(count))
}

// RecordExtractionRequest increments the extraction request counter
func RecordExtractionRequest(model string) {
	extractionRequestOps.WithLabelValues(model).Inc()
}

// RecordExtractionFields records the number of fields extracted
func RecordExtractionFields(model string, count int) {
	extractionFieldsOps.WithLabelValues(model).Add(float64(count))
}

// RecordReaderRequest increments the reader request counter
func RecordReaderRequest(model string) {
	readerRequestOps.WithLabelValues(model).Inc()
}

// RecordTranscriberRequest increments the transcriber request counter
func RecordTranscriberRequest(model string) {
	transcriberRequestOps.WithLabelValues(model).Inc()
}
