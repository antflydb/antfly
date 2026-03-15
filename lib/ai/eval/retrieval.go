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

package eval

import (
	"context"
	"fmt"
	"math"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/firebase/genkit/go/genkit"
)

// retrievalEvaluator wraps retrieval metric calculations.
type retrievalEvaluator struct {
	name     EvaluatorName
	category EvalCategory
	k        int
}

// Name returns the evaluator name.
func (e *retrievalEvaluator) Name() string {
	return string(e.name)
}

// Category returns the evaluator category.
func (e *retrievalEvaluator) Category() EvalCategory {
	return e.category
}

// Evaluate runs the retrieval metric evaluation.
func (e *retrievalEvaluator) Evaluate(ctx context.Context, input InternalEvalInput) (*EvaluatorScore, error) {
	if input.GroundTruth == nil || len(input.GroundTruth.RelevantIds) == 0 {
		return nil, fmt.Errorf("ground_truth.relevant_ids required for %s", e.name)
	}

	// Build a map from string ID to index for retrieved docs
	retrievedIndexes := make([]int, len(input.RetrievedIDs))
	idToIndex := make(map[string]int, len(input.RetrievedIDs))
	for i, id := range input.RetrievedIDs {
		retrievedIndexes[i] = i
		idToIndex[id] = i
	}

	// Map relevant IDs to their indexes (only those that were retrieved)
	var relevantIndexes []int
	for _, id := range input.GroundTruth.RelevantIds {
		if idx, ok := idToIndex[id]; ok {
			relevantIndexes = append(relevantIndexes, idx)
		}
	}

	// Use configured k or fall back to options
	k := e.k
	if k == 0 {
		k = input.Options.K
	}
	if k == 0 {
		k = 10
	}

	var score float64
	var reason string
	metadata := map[string]any{
		"k":               k,
		"retrieved_count": len(input.RetrievedIDs),
		"relevant_count":  len(input.GroundTruth.RelevantIds),
	}

	switch e.name {
	case EvaluatorNameRecall:
		score = calculateRecall(retrievedIndexes, relevantIndexes, k, len(input.GroundTruth.RelevantIds))
		relevant := countRelevant(retrievedIndexes[:min(k, len(retrievedIndexes))], relevantIndexes)
		reason = fmt.Sprintf("Recall@%d: %.4f (%d of %d relevant retrieved)", k, score, relevant, len(input.GroundTruth.RelevantIds))

	case EvaluatorNamePrecision:
		score = calculatePrecision(retrievedIndexes, relevantIndexes, k)
		relevant := countRelevant(retrievedIndexes[:min(k, len(retrievedIndexes))], relevantIndexes)
		reason = fmt.Sprintf("Precision@%d: %.4f (%d relevant in top %d)", k, score, relevant, k)

	case EvaluatorNameNdcg:
		score = calculateNDCG(retrievedIndexes, relevantIndexes, k)
		reason = fmt.Sprintf("NDCG@%d: %.4f", k, score)

	case EvaluatorNameMrr:
		var firstPos int
		score, firstPos = calculateMRR(retrievedIndexes, relevantIndexes)
		if firstPos >= 0 {
			reason = fmt.Sprintf("MRR: %.4f (first relevant at position %d)", score, firstPos+1)
		} else {
			reason = "MRR: 0.0 (no relevant documents found)"
		}
		metadata["first_relevant_position"] = firstPos + 1

	case EvaluatorNameMap:
		score = calculateMAP(retrievedIndexes, relevantIndexes, len(input.GroundTruth.RelevantIds))
		reason = fmt.Sprintf("MAP: %.4f", score)

	default:
		return nil, fmt.Errorf("unknown retrieval metric: %s", e.name)
	}

	threshold := input.Options.PassThreshold
	if threshold == 0 {
		threshold = 0.5
	}

	return &EvaluatorScore{
		Score:    float32(score),
		Pass:     score >= float64(threshold),
		Reason:   reason,
		Metadata: metadata,
	}, nil
}

// Factory functions for retrieval evaluators

func newRecallFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	return &retrievalEvaluator{
		name:     EvaluatorNameRecall,
		category: CategoryRetrieval,
		k:        cfg.Options.K,
	}, nil
}

func newPrecisionFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	return &retrievalEvaluator{
		name:     EvaluatorNamePrecision,
		category: CategoryRetrieval,
		k:        cfg.Options.K,
	}, nil
}

func newNDCGFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	return &retrievalEvaluator{
		name:     EvaluatorNameNdcg,
		category: CategoryRetrieval,
		k:        cfg.Options.K,
	}, nil
}

func newMRRFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	return &retrievalEvaluator{
		name:     EvaluatorNameMrr,
		category: CategoryRetrieval,
		k:        cfg.Options.K,
	}, nil
}

func newMAPFactory(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	return &retrievalEvaluator{
		name:     EvaluatorNameMap,
		category: CategoryRetrieval,
		k:        cfg.Options.K,
	}, nil
}

// Metric calculation functions

// calculateRecall computes Recall@k: fraction of relevant docs that were retrieved.
func calculateRecall(retrieved, relevant []int, k, totalRelevant int) float64 {
	if totalRelevant == 0 {
		return 1.0
	}

	k = min(k, len(retrieved))
	relevantCount := countRelevant(retrieved[:k], relevant)
	return float64(relevantCount) / float64(totalRelevant)
}

// calculatePrecision computes Precision@k: fraction of retrieved docs that are relevant.
func calculatePrecision(retrieved, relevant []int, k int) float64 {
	k = min(k, len(retrieved))
	if k == 0 {
		return 0.0
	}

	relevantCount := countRelevant(retrieved[:k], relevant)
	return float64(relevantCount) / float64(k)
}

// calculateNDCG computes Normalized Discounted Cumulative Gain at k.
func calculateNDCG(retrieved, relevant []int, k int) float64 {
	if len(relevant) == 0 {
		return 1.0
	}

	relevantSet := makeIntSet(relevant)
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

// calculateMRR computes Mean Reciprocal Rank (returns score and position).
func calculateMRR(retrieved, relevant []int) (float64, int) {
	relevantSet := makeIntSet(relevant)

	for i, docID := range retrieved {
		if relevantSet[docID] {
			return 1.0 / float64(i+1), i
		}
	}

	return 0.0, -1
}

// calculateMAP computes Mean Average Precision.
func calculateMAP(retrieved, relevant []int, totalRelevant int) float64 {
	if totalRelevant == 0 {
		return 1.0
	}

	relevantSet := makeIntSet(relevant)
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

	return sum / float64(totalRelevant)
}

// Helper functions

func makeIntSet(slice []int) map[int]bool {
	set := make(map[int]bool, len(slice))
	for _, v := range slice {
		set[v] = true
	}
	return set
}

func countRelevant(retrieved, relevant []int) int {
	relevantSet := makeIntSet(relevant)
	count := 0
	for _, docID := range retrieved {
		if relevantSet[docID] {
			count++
		}
	}
	return count
}
