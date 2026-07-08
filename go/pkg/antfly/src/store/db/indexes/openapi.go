// Copyright 2026 Antfly, Inc.
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

//go:generate go tool oapi-codegen --config=cfg.yaml ../../../../../../../specs/openapi/antfly/indexes.yaml
package indexes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/embeddings"
	"go.uber.org/zap"
)

// Legacy type name constants for backward compatibility with persisted data.
const (
	IndexTypeAknnV0     IndexType = "aknn_v0"
	IndexTypeFullTextV0 IndexType = "full_text_v0"
	IndexTypeGraphV0    IndexType = "graph_v0"
)

func NewIndexConfig(name string, config any) (*IndexConfig, error) {
	var t IndexType
	idxConfig := &IndexConfig{
		Name: name,
	}
	switch v := config.(type) {
	case EmbeddingsIndexConfig:
		t = IndexTypeEmbeddings
		_ = idxConfig.FromEmbeddingsIndexConfig(v)
	case FullTextIndexConfig:
		t = IndexTypeFullText
		_ = idxConfig.FromFullTextIndexConfig(v)
	case GraphIndexConfig:
		t = IndexTypeGraph
		_ = idxConfig.FromGraphIndexConfig(v)
	case AlgebraicIndexConfig:
		t = IndexTypeAlgebraic
		_ = idxConfig.FromAlgebraicIndexConfig(v)
	default:
		return nil, fmt.Errorf("unsupported index config type: %T", config)
	}
	idxConfig.Type = t

	return idxConfig, nil
}

func NewEmbeddingsConfig(name string, v EmbeddingsIndexConfig) *IndexConfig {
	ic := &IndexConfig{
		Name: name,
		Type: IndexTypeEmbeddings,
	}
	_ = ic.FromEmbeddingsIndexConfig(v)
	return ic
}

func NewFullTextIndexConfig(name string, memOnly bool) *IndexConfig {
	ic := &IndexConfig{
		Name: name,
		Type: IndexTypeFullText,
	}
	_ = ic.FromFullTextIndexConfig(FullTextIndexConfig{
		MemOnly: memOnly,
	})
	return ic
}

func NewAlgebraicIndexConfig(name string, deriveFromSchema bool) *IndexConfig {
	ic := &IndexConfig{
		Name: name,
		Type: IndexTypeAlgebraic,
	}
	_ = ic.FromAlgebraicIndexConfig(AlgebraicIndexConfig{
		DeriveFromSchema: deriveFromSchema,
	})
	return ic
}

func (bc FullTextIndexConfig) Equal(oc FullTextIndexConfig) bool {
	return bc.MemOnly == oc.MemOnly
}

func (ac AlgebraicIndexConfig) Equal(oc AlgebraicIndexConfig) bool {
	return ac.DeriveFromSchema == oc.DeriveFromSchema
}

// normalizeDistanceMetric treats the zero value ("") as the default "l2_squared"
// so that configs that omit the field compare equal to ones that set the default
// explicitly. This prevents false mismatches from JSON omitempty round-trips.
func normalizeDistanceMetric(dm DistanceMetric) DistanceMetric {
	if dm == "" {
		return DistanceMetricL2Squared
	}
	return dm
}

func (ec EmbeddingsIndexConfig) HasPromptSource() bool {
	return ec.Field != "" || ec.Template != ""
}

func (ec EmbeddingsIndexConfig) IsExternal() bool {
	return ec.External
}

func (ec EmbeddingsIndexConfig) Equal(oc EmbeddingsIndexConfig) bool {
	return ec.MemOnly == oc.MemOnly &&
		ec.Dimension == oc.Dimension &&
		ec.Field == oc.Field &&
		ec.Template == oc.Template &&
		normalizeDistanceMetric(ec.DistanceMetric) == normalizeDistanceMetric(oc.DistanceMetric) &&
		ec.External == oc.External &&
		ec.Sparse == oc.Sparse &&
		ec.ChunkSize == oc.ChunkSize &&
		ec.MinWeight == oc.MinWeight &&
		ec.TopK == oc.TopK &&
		reflect.DeepEqual(ec.Embedder, oc.Embedder) &&
		reflect.DeepEqual(ec.Summarizer, oc.Summarizer) &&
		reflect.DeepEqual(ec.Chunker, oc.Chunker)
}

func (ic IndexConfig) Equal(oc IndexConfig) bool {
	if ic.Name != oc.Name || NormalizeIndexType(ic.Type) != NormalizeIndexType(oc.Type) {
		return false
	}
	switch NormalizeIndexType(ic.Type) {
	case IndexTypeFullText:
		i, _ := ic.AsFullTextIndexConfig()
		o, _ := oc.AsFullTextIndexConfig()
		return i.Equal(o)
	case IndexTypeEmbeddings:
		i, _ := ic.AsEmbeddingsIndexConfig()
		o, _ := oc.AsEmbeddingsIndexConfig()
		return i.Equal(o)
	case IndexTypeGraph:
		i, _ := ic.AsGraphIndexConfig()
		o, _ := oc.AsGraphIndexConfig()
		return i.Equal(o)
	case IndexTypeAlgebraic:
		i, _ := ic.AsAlgebraicIndexConfig()
		o, _ := oc.AsAlgebraicIndexConfig()
		return i.Equal(o)
	}

	return true
}

func (ic IndexConfig) GetEmbedderConfig() (*embeddings.EmbedderConfig, error) {
	indexConfig, err := ic.AsEmbeddingsIndexConfig()
	if err != nil {
		return nil, fmt.Errorf("getting embedder config from index config: %w", err)
	}
	return indexConfig.Embedder, nil
}

func (b FullTextIndexStats) AsIndexStats() IndexStats {
	var is IndexStats
	if b.IndexType == "" {
		b.IndexType = FullTextIndexStatsIndexTypeFullText
	}
	_ = is.FromFullTextIndexStats(b)
	return is
}

func (e EmbeddingsIndexStats) AsIndexStats() IndexStats {
	var is IndexStats
	if e.IndexType == "" {
		e.IndexType = EmbeddingsIndexStatsIndexTypeEmbeddings
	}
	_ = is.FromEmbeddingsIndexStats(e)
	return is
}

func (a AlgebraicIndexStats) AsIndexStats() IndexStats {
	var is IndexStats
	if a.IndexType == "" {
		a.IndexType = AlgebraicIndexStatsIndexTypeAlgebraic
	}
	_ = is.FromAlgebraicIndexStats(a)
	return is
}

func (bc FullTextIndexStats) Equal(oc FullTextIndexStats) bool {
	return bc.Rebuilding == oc.Rebuilding &&
		bc.BackfillActive == oc.BackfillActive &&
		bc.Error == oc.Error &&
		bc.TotalIndexed == oc.TotalIndexed &&
		bc.BackfillProgress == oc.BackfillProgress &&
		bc.BackfillState == oc.BackfillState &&
		bc.DocCount == oc.DocCount
}

func (bc EmbeddingsIndexStats) Equal(oc EmbeddingsIndexStats) bool {
	return bc.Error == oc.Error &&
		bc.TotalIndexed == oc.TotalIndexed &&
		bc.TotalNodes == oc.TotalNodes &&
		bc.TotalTerms == oc.TotalTerms &&
		bc.Rebuilding == oc.Rebuilding &&
		bc.BackfillActive == oc.BackfillActive &&
		bc.DensePublishPending == oc.DensePublishPending &&
		bc.BackfillProgress == oc.BackfillProgress &&
		bc.BackfillState == oc.BackfillState &&
		bc.DocCount == oc.DocCount &&
		bc.QueryVisibleDocCount == oc.QueryVisibleDocCount
}

func (gc GraphIndexConfig) Equal(oc GraphIndexConfig) bool {
	return gc.MaxEdgesPerDocument == oc.MaxEdgesPerDocument &&
		gc.Template == oc.Template &&
		reflect.DeepEqual(gc.EdgeTypes, oc.EdgeTypes) &&
		reflect.DeepEqual(gc.Summarizer, oc.Summarizer)
}

func (gc GraphIndexStats) Equal(oc GraphIndexStats) bool {
	return gc.Error == oc.Error &&
		gc.TotalEdges == oc.TotalEdges &&
		reflect.DeepEqual(gc.EdgeTypes, oc.EdgeTypes) &&
		gc.Rebuilding == oc.Rebuilding &&
		gc.BackfillActive == oc.BackfillActive &&
		gc.BackfillProgress == oc.BackfillProgress &&
		gc.BackfillState == oc.BackfillState &&
		gc.DocCount == oc.DocCount &&
		gc.EdgeCount == oc.EdgeCount &&
		gc.NodeCount == oc.NodeCount
}

func (g GraphIndexStats) AsIndexStats() IndexStats {
	var is IndexStats
	if g.IndexType == "" {
		g.IndexType = GraphIndexStatsIndexTypeGraph
	}
	_ = is.FromGraphIndexStats(g)
	return is
}

func (ac AlgebraicIndexStats) Equal(oc AlgebraicIndexStats) bool {
	return ac.Error == oc.Error &&
		ac.TotalIndexed == oc.TotalIndexed &&
		ac.Rebuilding == oc.Rebuilding &&
		ac.BackfillActive == oc.BackfillActive &&
		ac.BackfillProgress == oc.BackfillProgress &&
		ac.BackfillState == oc.BackfillState &&
		ac.DocCount == oc.DocCount &&
		ac.Healthy == oc.Healthy &&
		ac.ParseErrorCount == oc.ParseErrorCount &&
		ac.SchemaVersion == oc.SchemaVersion &&
		ac.CapabilityLifecycleStatus == oc.CapabilityLifecycleStatus &&
		ac.PlannerSelected == oc.PlannerSelected &&
		ac.PlannerFallbackCount == oc.PlannerFallbackCount &&
		ac.PlannerLastDecision == oc.PlannerLastDecision &&
		ac.PlannerLastFallbackReason == oc.PlannerLastFallbackReason &&
		ac.PlannerLastEstimatedScanRows == oc.PlannerLastEstimatedScanRows &&
		ac.PlannerLastEstimatedResultBuckets == oc.PlannerLastEstimatedResultBuckets &&
		ac.PlannerLifecycleReady == oc.PlannerLifecycleReady &&
		ac.PlannerLifecycleBlockingReason == oc.PlannerLifecycleBlockingReason &&
		ac.AdaptiveProgressCount == oc.AdaptiveProgressCount &&
		ac.RecommendationCount == oc.RecommendationCount &&
		ac.AdaptiveBackfillingCount == oc.AdaptiveBackfillingCount &&
		ac.AdaptiveReadyCount == oc.AdaptiveReadyCount &&
		ac.AdaptiveStaleCount == oc.AdaptiveStaleCount &&
		ac.AdaptiveCleanupRecommendedCount == oc.AdaptiveCleanupRecommendedCount &&
		ac.ActiveProgressLifecycle == oc.ActiveProgressLifecycle &&
		ac.ActiveProgressRowsProcessed == oc.ActiveProgressRowsProcessed &&
		ac.ActiveProgressTargetRows == oc.ActiveProgressTargetRows &&
		ac.LastErrorReason == oc.LastErrorReason
}

// indexStatsKind identifies the concrete type inside an IndexStats union.
// Since the generated As* methods always succeed (JSON unmarshal is lenient),
// we detect the type by checking for unique JSON keys in the raw union bytes.
type indexStatsKind int

const (
	indexStatsUnknown    indexStatsKind = iota
	indexStatsFullText                  // has "total_indexed" but not graph, embedding, or algebraic discriminator fields
	indexStatsEmbeddings                // has vector-specific fields such as "total_nodes", "total_terms", or "dense_publish_pending"
	indexStatsGraph                     // has "total_edges" or "edge_types"
	indexStatsAlgebraic                 // has index_type "algebraic" or algebraic-specific planner/adaptive keys
)

func detectIndexStatsKind(union []byte) indexStatsKind {
	var discriminator struct {
		IndexType string `json:"index_type"`
	}
	if err := json.Unmarshal(union, &discriminator); err == nil {
		switch discriminator.IndexType {
		case string(FullTextIndexStatsIndexTypeFullText):
			return indexStatsFullText
		case string(EmbeddingsIndexStatsIndexTypeEmbeddings):
			return indexStatsEmbeddings
		case string(GraphIndexStatsIndexTypeGraph):
			return indexStatsGraph
		case string(AlgebraicIndexStatsIndexTypeAlgebraic):
			return indexStatsAlgebraic
		}
	}
	if bytes.Contains(union, []byte(`"planner_selected"`)) || bytes.Contains(union, []byte(`"adaptive_progress_count"`)) {
		return indexStatsAlgebraic
	}
	// Graph-unique keys
	if bytes.Contains(union, []byte(`"total_edges"`)) || bytes.Contains(union, []byte(`"edge_types"`)) {
		return indexStatsGraph
	}
	// Embeddings-unique keys
	if bytes.Contains(union, []byte(`"dense_publish_pending"`)) || bytes.Contains(union, []byte(`"total_nodes"`)) || bytes.Contains(union, []byte(`"total_terms"`)) {
		return indexStatsEmbeddings
	}
	// If it has any content, assume FullText (the default/first oneOf variant)
	if len(union) > 2 { // more than "{}"
		return indexStatsFullText
	}
	return indexStatsUnknown
}

// mergeErrors concatenates two error strings with "; " separator.
func mergeErrors(dst *string, src string) {
	if src == "" {
		return
	}
	if *dst != "" {
		*dst += "; " + src
	} else {
		*dst = src
	}
}

// mergeBackfillFields merges backfill state across shards: OR for activity and
// min progress among active shards only.
func mergeBackfillFields(
	dstRebuilding *bool, dstActive *bool, dstProgress *float64,
	srcRebuilding bool, srcActive bool, srcProgress float64,
) {
	dstWasActive := *dstRebuilding || *dstActive
	*dstRebuilding = *dstRebuilding || srcRebuilding
	*dstActive = *dstActive || srcActive
	if srcRebuilding || srcActive {
		if !dstWasActive || srcProgress < *dstProgress {
			*dstProgress = srcProgress
		}
	}
}

// MergeIndexStats merges src into dst by summing numeric fields across shards.
// Errors are concatenated. Boolean flags like Rebuilding use logical OR.
// If dst has no data yet (empty union), src is used as-is.
// Logs a warning via zap.L() if dst and src are different stat types.
func MergeIndexStats(dst *IndexStats, src IndexStats) {
	if len(src.union) == 0 {
		return
	}
	if len(dst.union) == 0 {
		*dst = src
		return
	}

	dstKind := detectIndexStatsKind(dst.union)
	srcKind := detectIndexStatsKind(src.union)

	// For empty objects (e.g. all zero values), fall back to the other side's kind
	if dstKind == indexStatsUnknown {
		dstKind = srcKind
	}
	if srcKind == indexStatsUnknown {
		srcKind = dstKind
	}

	if dstKind != srcKind {
		zap.L().Warn("MergeIndexStats: cannot merge mismatched stat types",
			zap.Int("dst_kind", int(dstKind)),
			zap.Int("src_kind", int(srcKind)))
		return
	}

	switch dstKind {
	case indexStatsFullText:
		dstFT, _ := dst.AsFullTextIndexStats()
		srcFT, _ := src.AsFullTextIndexStats()
		dstFT.TotalIndexed += srcFT.TotalIndexed
		dstFT.DocCount += srcFT.DocCount
		mergeBackfillFields(&dstFT.Rebuilding, &dstFT.BackfillActive, &dstFT.BackfillProgress,
			srcFT.Rebuilding, srcFT.BackfillActive, srcFT.BackfillProgress)
		mergeErrors(&dstFT.Error, srcFT.Error)
		_ = dst.FromFullTextIndexStats(dstFT)

	case indexStatsEmbeddings:
		dstEmb, _ := dst.AsEmbeddingsIndexStats()
		srcEmb, _ := src.AsEmbeddingsIndexStats()
		dstEmb.TotalIndexed += srcEmb.TotalIndexed
		dstEmb.TotalNodes += srcEmb.TotalNodes
		dstEmb.TotalTerms += srcEmb.TotalTerms
		dstEmb.DocCount += srcEmb.DocCount
		dstEmb.QueryVisibleDocCount += srcEmb.QueryVisibleDocCount
		dstEmb.DensePublishPending = dstEmb.DensePublishPending || srcEmb.DensePublishPending
		mergeBackfillFields(&dstEmb.Rebuilding, &dstEmb.BackfillActive, &dstEmb.BackfillProgress,
			srcEmb.Rebuilding, srcEmb.BackfillActive, srcEmb.BackfillProgress)
		mergeErrors(&dstEmb.Error, srcEmb.Error)
		_ = dst.FromEmbeddingsIndexStats(dstEmb)

	case indexStatsGraph:
		dstGraph, _ := dst.AsGraphIndexStats()
		srcGraph, _ := src.AsGraphIndexStats()
		dstGraph.TotalEdges += srcGraph.TotalEdges
		dstGraph.DocCount += srcGraph.DocCount
		dstGraph.EdgeCount += srcGraph.EdgeCount
		dstGraph.NodeCount += srcGraph.NodeCount
		mergeBackfillFields(&dstGraph.Rebuilding, &dstGraph.BackfillActive, &dstGraph.BackfillProgress,
			srcGraph.Rebuilding, srcGraph.BackfillActive, srcGraph.BackfillProgress)
		if srcGraph.EdgeTypes != nil {
			if dstGraph.EdgeTypes == nil {
				dstGraph.EdgeTypes = srcGraph.EdgeTypes
			} else {
				for k, v := range *srcGraph.EdgeTypes {
					(*dstGraph.EdgeTypes)[k] += v
				}
			}
		}
		mergeErrors(&dstGraph.Error, srcGraph.Error)
		_ = dst.FromGraphIndexStats(dstGraph)

	case indexStatsAlgebraic:
		dstAlg, _ := dst.AsAlgebraicIndexStats()
		srcAlg, _ := src.AsAlgebraicIndexStats()
		dstAlg.TotalIndexed += srcAlg.TotalIndexed
		dstAlg.DocCount += srcAlg.DocCount
		dstAlg.ParseErrorCount += srcAlg.ParseErrorCount
		dstAlg.PlannerSelected += srcAlg.PlannerSelected
		dstAlg.PlannerFallbackCount += srcAlg.PlannerFallbackCount
		dstAlg.AdaptiveProgressCount += srcAlg.AdaptiveProgressCount
		dstAlg.RecommendationCount += srcAlg.RecommendationCount
		dstAlg.AdaptiveBackfillingCount += srcAlg.AdaptiveBackfillingCount
		dstAlg.AdaptiveReadyCount += srcAlg.AdaptiveReadyCount
		dstAlg.AdaptiveStaleCount += srcAlg.AdaptiveStaleCount
		dstAlg.AdaptiveCleanupRecommendedCount += srcAlg.AdaptiveCleanupRecommendedCount
		dstAlg.ActiveProgressRowsProcessed += srcAlg.ActiveProgressRowsProcessed
		dstAlg.ActiveProgressTargetRows += srcAlg.ActiveProgressTargetRows
		dstAlg.Healthy = dstAlg.Healthy && srcAlg.Healthy
		mergeBackfillFields(&dstAlg.Rebuilding, &dstAlg.BackfillActive, &dstAlg.BackfillProgress,
			srcAlg.Rebuilding, srcAlg.BackfillActive, srcAlg.BackfillProgress)
		if srcAlg.SchemaVersion > dstAlg.SchemaVersion {
			dstAlg.SchemaVersion = srcAlg.SchemaVersion
		}
		if srcAlg.CapabilityLifecycleStatus != "" {
			dstAlg.CapabilityLifecycleStatus = srcAlg.CapabilityLifecycleStatus
		}
		if srcAlg.PlannerLastDecision != "" {
			dstAlg.PlannerLastDecision = srcAlg.PlannerLastDecision
		}
		if srcAlg.PlannerLastFallbackReason != "" {
			dstAlg.PlannerLastFallbackReason = srcAlg.PlannerLastFallbackReason
		}
		if srcAlg.PlannerLastEstimatedScanRows != 0 {
			dstAlg.PlannerLastEstimatedScanRows = srcAlg.PlannerLastEstimatedScanRows
		}
		if srcAlg.PlannerLastEstimatedResultBuckets != 0 {
			dstAlg.PlannerLastEstimatedResultBuckets = srcAlg.PlannerLastEstimatedResultBuckets
		}
		if srcAlg.PlannerLifecycleBlockingReason != "" {
			dstAlg.PlannerLifecycleBlockingReason = srcAlg.PlannerLifecycleBlockingReason
		}
		dstAlg.PlannerLifecycleReady = dstAlg.PlannerLifecycleReady && srcAlg.PlannerLifecycleReady
		if srcAlg.ActiveProgressLifecycle != "" {
			dstAlg.ActiveProgressLifecycle = srcAlg.ActiveProgressLifecycle
		}
		if srcAlg.LastErrorReason != "" {
			dstAlg.LastErrorReason = srcAlg.LastErrorReason
		}
		mergeErrors(&dstAlg.Error, srcAlg.Error)
		_ = dst.FromAlgebraicIndexStats(dstAlg)

	default:
		zap.L().Warn("MergeIndexStats: unknown stat type",
			zap.Int("dst_union_len", len(dst.union)),
			zap.Int("src_union_len", len(src.union)))
	}
}
