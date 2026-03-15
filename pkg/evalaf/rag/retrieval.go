package rag

import (
	"context"
	"fmt"
	"math"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
)

// RetrievalMetric represents different retrieval metrics.
type RetrievalMetric string

const (
	MetricNDCG      RetrievalMetric = "ndcg"
	MetricMRR       RetrievalMetric = "mrr"
	MetricPrecision RetrievalMetric = "precision"
	MetricRecall    RetrievalMetric = "recall"
	MetricMAP       RetrievalMetric = "map" // Mean Average Precision
)

// RetrievalEvaluator evaluates retrieval quality using various metrics.
type RetrievalEvaluator struct {
	name   string
	metric RetrievalMetric
	k      int // For metrics like Precision@k, Recall@k, NDCG@k
}

// NewRetrievalEvaluator creates a new retrieval evaluator.
func NewRetrievalEvaluator(name string, metric RetrievalMetric, k int) *RetrievalEvaluator {
	if name == "" {
		name = string(metric)
		if k > 0 {
			name = fmt.Sprintf("%s@%d", metric, k)
		}
	}

	if k <= 0 {
		k = 10 // default k=10
	}

	return &RetrievalEvaluator{
		name:   name,
		metric: metric,
		k:      k,
	}
}

// Name returns the evaluator name.
func (e *RetrievalEvaluator) Name() string {
	return e.name
}

// Evaluate performs retrieval evaluation.
// Expects metadata to contain "retrieved_ids" ([]int) and "relevant_ids" ([]int).
func (e *RetrievalEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	// Extract retrieved and relevant document IDs from metadata
	retrievedIDs, err := extractIntSlice(input.Metadata, "retrieved_ids")
	if err != nil {
		return nil, fmt.Errorf("missing or invalid retrieved_ids in metadata: %w", err)
	}

	relevantIDs, err := extractIntSlice(input.Metadata, "relevant_ids")
	if err != nil {
		return nil, fmt.Errorf("missing or invalid relevant_ids in metadata: %w", err)
	}

	// Calculate metric
	var score float64
	var reason string

	switch e.metric {
	case MetricNDCG:
		score = calculateNDCG(retrievedIDs, relevantIDs, e.k)
		reason = fmt.Sprintf("NDCG@%d: %.4f", e.k, score)

	case MetricMRR:
		score = calculateMRR(retrievedIDs, relevantIDs)
		reason = fmt.Sprintf("MRR: %.4f (first relevant at position %d)", score, findFirstRelevant(retrievedIDs, relevantIDs)+1)

	case MetricPrecision:
		score = calculatePrecision(retrievedIDs, relevantIDs, e.k)
		reason = fmt.Sprintf("Precision@%d: %.4f (%d relevant in top %d)", e.k, score, countRelevant(retrievedIDs[:min(e.k, len(retrievedIDs))], relevantIDs), e.k)

	case MetricRecall:
		score = calculateRecall(retrievedIDs, relevantIDs, e.k)
		reason = fmt.Sprintf("Recall@%d: %.4f (%d of %d relevant retrieved)", e.k, score, countRelevant(retrievedIDs[:min(e.k, len(retrievedIDs))], relevantIDs), len(relevantIDs))

	case MetricMAP:
		score = calculateMAP(retrievedIDs, relevantIDs)
		reason = fmt.Sprintf("MAP: %.4f", score)

	default:
		return nil, fmt.Errorf("unknown metric: %s", e.metric)
	}

	// Pass if score is above threshold (0.5 for most metrics, 0.7 for NDCG)
	threshold := 0.5
	if e.metric == MetricNDCG {
		threshold = 0.7
	}

	pass := score >= threshold

	return &eval.EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
		Metadata: map[string]any{
			"metric":          string(e.metric),
			"k":               e.k,
			"retrieved_count": len(retrievedIDs),
			"relevant_count":  len(relevantIDs),
		},
	}, nil
}

// SupportsStreaming returns false.
func (e *RetrievalEvaluator) SupportsStreaming() bool {
	return false
}

// calculateNDCG computes Normalized Discounted Cumulative Gain at k.
func calculateNDCG(retrieved, relevant []int, k int) float64 {
	if len(relevant) == 0 {
		return 1.0
	}

	relevantSet := makeSet(relevant)
	k = min(k, len(retrieved))

	// Calculate DCG
	dcg := 0.0
	for i := 0; i < k; i++ {
		if relevantSet[retrieved[i]] {
			// Gain = 1 for relevant, 0 for non-relevant
			// Discount = log2(position + 1)
			dcg += 1.0 / math.Log2(float64(i+2)) // i+2 because positions are 1-indexed
		}
	}

	// Calculate IDCG (ideal DCG - all relevant docs at top)
	idcg := 0.0
	idealK := min(k, len(relevant))
	for i := range idealK {
		idcg += 1.0 / math.Log2(float64(i+2))
	}

	if idcg == 0 {
		return 0.0
	}

	return dcg / idcg
}

// calculateMRR computes Mean Reciprocal Rank.
func calculateMRR(retrieved, relevant []int) float64 {
	relevantSet := makeSet(relevant)

	for i, docID := range retrieved {
		if relevantSet[docID] {
			return 1.0 / float64(i+1)
		}
	}

	return 0.0
}

// calculatePrecision computes Precision at k.
func calculatePrecision(retrieved, relevant []int, k int) float64 {
	k = min(k, len(retrieved))
	if k == 0 {
		return 0.0
	}

	relevantCount := countRelevant(retrieved[:k], relevant)
	return float64(relevantCount) / float64(k)
}

// calculateRecall computes Recall at k.
func calculateRecall(retrieved, relevant []int, k int) float64 {
	if len(relevant) == 0 {
		return 1.0
	}

	k = min(k, len(retrieved))
	relevantCount := countRelevant(retrieved[:k], relevant)
	return float64(relevantCount) / float64(len(relevant))
}

// calculateMAP computes Mean Average Precision.
func calculateMAP(retrieved, relevant []int) float64 {
	if len(relevant) == 0 {
		return 1.0
	}

	relevantSet := makeSet(relevant)
	sum := 0.0
	relevantSeen := 0

	for i, docID := range retrieved {
		if relevantSet[docID] {
			relevantSeen++
			precision := float64(relevantSeen) / float64(i+1)
			sum += precision
		}
	}

	if relevantSeen == 0 {
		return 0.0
	}

	return sum / float64(len(relevant))
}

// Helper functions

func extractIntSlice(metadata map[string]any, key string) ([]int, error) {
	value, ok := metadata[key]
	if !ok {
		return nil, fmt.Errorf("key %s not found", key)
	}

	switch v := value.(type) {
	case []int:
		return v, nil
	case []any:
		result := make([]int, 0, len(v))
		for i, item := range v {
			switch num := item.(type) {
			case int:
				result = append(result, num)
			case float64:
				result = append(result, int(num))
			default:
				return nil, fmt.Errorf("element %d is not an int: %T", i, item)
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("value is not a slice: %T", value)
	}
}

func makeSet(slice []int) map[int]bool {
	set := make(map[int]bool, len(slice))
	for _, v := range slice {
		set[v] = true
	}
	return set
}

func countRelevant(retrieved, relevant []int) int {
	relevantSet := makeSet(relevant)
	count := 0
	for _, docID := range retrieved {
		if relevantSet[docID] {
			count++
		}
	}
	return count
}

func findFirstRelevant(retrieved, relevant []int) int {
	relevantSet := makeSet(relevant)
	for i, docID := range retrieved {
		if relevantSet[docID] {
			return i
		}
	}
	return -1
}
