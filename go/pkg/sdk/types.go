/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// Re-export commonly used types from oapi package
type (
	// Table and Index types
	CreateTableRequest = oapi.CreateTableRequest
	TableStatus        = oapi.TableStatus
	TableMigration     = oapi.TableMigration
	TableSchema        = oapi.TableSchema
	IndexConfig        = oapi.IndexConfig
	IndexStatus        = oapi.IndexStatus
	IndexType          = oapi.IndexType

	// Artifact types
	DocumentArtifactChildRange               = oapi.DocumentArtifactChildRange
	DocumentArtifactManifest                 = oapi.DocumentArtifactManifest
	DocumentArtifactManifestList             = oapi.DocumentArtifactManifestList
	DocumentArtifactReprocessFailure         = oapi.DocumentArtifactReprocessFailure
	DocumentArtifactReprocessJob             = oapi.DocumentArtifactReprocessJob
	DocumentArtifactReprocessJobPhase        = oapi.DocumentArtifactReprocessJobPhase
	DocumentArtifactReprocessJobStartRequest = oapi.DocumentArtifactReprocessJobStartRequest
	DocumentArtifactReprocessResponse        = oapi.DocumentArtifactReprocessResponse
	DocumentArtifactReprocessShardCursor     = oapi.DocumentArtifactReprocessShardCursor
	DocumentArtifactTableReprocessRequest    = oapi.DocumentArtifactTableReprocessRequest
	DocumentArtifactTableReprocessResponse   = oapi.DocumentArtifactTableReprocessResponse
	TableArtifactEnrichmentList              = oapi.TableArtifactEnrichmentList

	// Index config types
	EmbeddingsIndexConfig = oapi.EmbeddingsIndexConfig
	DistanceMetric        = oapi.DistanceMetric
	EmbeddingsIndexStats  = oapi.EmbeddingsIndexStats
	EnrichmentConfig      = oapi.EnrichmentConfig
	EnrichmentKind        = oapi.EnrichmentKind
	FullTextIndexConfig   = oapi.FullTextIndexConfig
	FullTextIndexStats    = oapi.FullTextIndexStats

	EmbedderProvider         = oapi.EmbedderProvider
	GeneratorProvider        = oapi.GeneratorProvider
	EmbedderConfig           = oapi.EmbedderConfig
	GeneratorConfig          = oapi.GeneratorConfig
	OllamaEmbedderConfig     = oapi.OllamaEmbedderConfig
	OpenAIEmbedderConfig     = oapi.OpenAIEmbedderConfig
	GoogleEmbedderConfig     = oapi.GoogleEmbedderConfig
	BedrockEmbedderConfig    = oapi.BedrockEmbedderConfig
	VertexEmbedderConfig     = oapi.VertexEmbedderConfig
	AntflyEmbedderConfig     = oapi.AntflyEmbedderConfig
	OllamaGeneratorConfig    = oapi.OllamaGeneratorConfig
	OpenAIGeneratorConfig    = oapi.OpenAIGeneratorConfig
	GoogleGeneratorConfig    = oapi.GoogleGeneratorConfig
	BedrockGeneratorConfig   = oapi.BedrockGeneratorConfig
	VertexGeneratorConfig    = oapi.VertexGeneratorConfig
	AnthropicGeneratorConfig = oapi.AnthropicGeneratorConfig
	AntflyGeneratorConfig    = oapi.AntflyGeneratorConfig
	RerankerConfig           = oapi.RerankerConfig
	AntflyRerankerConfig     = oapi.AntflyRerankerConfig
	OllamaRerankerConfig     = oapi.OllamaRerankerConfig
	RerankerProvider         = oapi.RerankerProvider
	Pruner                   = oapi.Pruner

	// Chunker config types
	ChunkerProvider     = oapi.ChunkerProvider
	ChunkerConfig       = oapi.ChunkerConfig
	AntflyChunkerConfig = oapi.AntflyChunkerConfig
	TextChunkOptions    = oapi.TextChunkOptions

	// Sort types
	SortField = oapi.SortField

	// Query response types
	QueryResponses              = oapi.QueryResponses
	QueryResult                 = oapi.QueryResult
	Hits                        = oapi.QueryHits
	QueryHitsTotal              = oapi.QueryHitsTotal
	QueryHitsTotalRelation      = oapi.QueryHitsTotalRelation
	Hit                         = oapi.QueryHit
	HierarchyAncestor           = oapi.HierarchyAncestor
	HierarchyArtifact           = oapi.HierarchyArtifact
	HierarchyArtifactKind       = oapi.HierarchyArtifactKind
	HierarchyArtifactSource     = oapi.HierarchyArtifactSource
	HierarchyArtifactSourceKind = oapi.HierarchyArtifactSourceKind
	HierarchyEvidence           = oapi.HierarchyEvidence
	HierarchyMatchContext       = oapi.HierarchyMatchContext
	HierarchyMatchHit           = oapi.HierarchyMatchHit
	QueryHitHierarchy           = oapi.QueryHitHierarchy
	QueryHitHierarchyAncestors  = oapi.QueryHitHierarchyAncestors
	QueryHitHierarchyLevel      = oapi.QueryHitHierarchyLevel
	AggregationRequest          = oapi.AggregationRequest
	AggregationOption           = oapi.AggregationBucket
	AggregationResult           = oapi.AggregationResult
	AggregationType             = oapi.AggregationType

	// Embedding types
	Embedding             = oapi.Embedding
	DenseEmbedding        = oapi.Embedding0 // []float32
	SparseEmbedding       = oapi.Embedding1 // {Indices []uint32, Values []float32}
	PackedDenseEmbedding  = oapi.Embedding2 // base64-encoded little-endian float32 bytes
	PackedSparseEmbedding = oapi.Embedding3 // {PackedIndices, PackedValues} as base64 LE bytes

	// Other types
	AntflyType     = oapi.AntflyType
	MergeStrategy  = oapi.MergeStrategy
	MergeConfig    = oapi.MergeConfig
	DocumentSchema = oapi.DocumentSchema

	// Validation types
	ValidationError  = oapi.ValidationError
	ValidationResult = oapi.ValidationResult

	// LinearMerge types
	LinearMergePageStatus = oapi.LinearMergePageStatus
	LinearMergeRequest    = oapi.LinearMergeRequest
	LinearMergeResult     = oapi.LinearMergeResult
	FailedOperation       = oapi.FailedOperation
	KeyRange              = oapi.KeyRange
	SyncLevel             = oapi.SyncLevel

	// Transform types for MongoDB-style atomic updates
	Transform       = oapi.Transform
	TransformOp     = oapi.TransformOp
	TransformOpType = oapi.TransformOpType

	// Key scan types
	ScanKeysRequest = oapi.ScanKeysRequest
	LookupKeyParams = oapi.LookupKeyParams

	// AI Agent types
	ClassificationTransformationResult = oapi.ClassificationTransformationResult
	RouteType                          = oapi.RouteType
	QueryStrategy                      = oapi.QueryStrategy
	SemanticQueryMode                  = oapi.SemanticQueryMode
	ClassificationStepConfig           = oapi.ClassificationStepConfig
	GenerationStepConfig               = oapi.GenerationStepConfig
	FollowupStepConfig                 = oapi.FollowupStepConfig
	ConfidenceStepConfig               = oapi.ConfidenceStepConfig
	RetryConfig                        = oapi.RetryConfig
	ChainLink                          = oapi.ChainLink
	ChainCondition                     = oapi.ChainCondition

	// Chat/Agent types (used by retrieval agent)
	ChatMessage         = oapi.ChatMessage
	ChatMessageContent  = oapi.ChatMessageContent
	ChatMessageRole     = oapi.ChatMessageRole
	ContentPart         = oapi.ContentPart
	TextContentPart     = oapi.TextContentPart
	ImageURL            = oapi.ImageURL
	ImageURLContentPart = oapi.ImageURLContentPart
	MediaContentPart    = oapi.MediaContentPart
	ToolCall            = oapi.ToolCall
	ToolCallFunction    = oapi.ToolCallFunction
	ChatToolName        = oapi.ChatToolName
	ChatToolsConfig     = oapi.ChatToolsConfig
	FetchConfig         = oapi.FetchConfig
	WebSearchConfig     = oapi.WebSearchConfig
	FilterSpec          = oapi.FilterSpec
	FilterSpecOperator  = oapi.FilterSpecOperator
	AgentDecision       = oapi.AgentDecision
	AgentQuestion       = oapi.AgentQuestion
	AgentQuestionKind   = oapi.AgentQuestionKind
	AgentStatus         = oapi.AgentStatus
	AgentStep           = oapi.AgentStep
	AgentStepKind       = oapi.AgentStepKind
	AgentStepStatus     = oapi.AgentStepStatus

	// Query Builder types
	QueryBuilderRequest = oapi.QueryBuilderRequest
	QueryBuilderResult  = oapi.QueryBuilderResult

	// Retrieval Agent types
	RetrievalAgentRequest = oapi.RetrievalAgentRequest
	RetrievalAgentResult  = oapi.RetrievalAgentResult
	RetrievalAgentUsage   = oapi.RetrievalAgentUsage
	IncompleteDetails     = oapi.IncompleteDetails
	PruneStats            = oapi.PruneStats
	RetrievalAgentSteps   = oapi.RetrievalAgentSteps

	// SSE event types for streaming
	SSEEvent         = oapi.SSEEvent
	SSEStepStarted   = oapi.SSEStepStarted
	SSEStepProgress  = oapi.SSEStepProgress
	SSEStepCompleted = oapi.SSEStepCompleted
	SSEToolMode      = oapi.SSEToolMode
	SSEError         = oapi.SSEError

	RetrievalQueryRequest = oapi.RetrievalQueryRequest
	RetrievalStrategy     = oapi.RetrievalStrategy
	TreeSearchConfig      = oapi.TreeSearchConfig
	QueryHit              = oapi.QueryHit

	// Evaluation types
	EvalConfig    = oapi.EvalConfig
	EvalOptions   = oapi.EvalOptions
	EvalResult    = oapi.EvalResult
	EvalSummary   = oapi.EvalSummary
	EvaluatorName = oapi.EvaluatorName
	GroundTruth   = oapi.GroundTruth

	// Join types
	JoinClause    = oapi.JoinClause
	JoinCondition = oapi.JoinCondition
	JoinFilters   = oapi.JoinFilters
	JoinOperator  = oapi.JoinOperator
	JoinProfile   = oapi.JoinProfile
	JoinStrategy  = oapi.JoinStrategy
	JoinType      = oapi.JoinType

	// Query profiling types
	QueryProfile    = oapi.QueryProfile
	ShardsProfile   = oapi.ShardsProfile
	RerankerProfile = oapi.RerankerProfile
	MergeProfile    = oapi.MergeProfile

	// Foreign table types
	ForeignSource     = oapi.ForeignSource
	ForeignColumn     = oapi.ForeignColumn
	ForeignSourceType = oapi.ForeignSourceType

	// Replication types
	ReplicationSource      = oapi.ReplicationSource
	ReplicationSourceType  = oapi.ReplicationSourceType
	ReplicationTransformOp = oapi.ReplicationTransformOp
	ReplicationRoute       = oapi.ReplicationRoute

	// Graph index types
	GraphIndexConfig                    = oapi.GraphIndexConfig
	GraphIndexStats                     = oapi.GraphIndexStats
	GraphIndexStatsIndexType            = oapi.GraphIndexStatsIndexType
	GraphMetricActionResponse           = oapi.GraphMetricActionResponse
	GraphMetricBuildPageStatus          = oapi.GraphMetricBuildPageStatus
	GraphMetricBuildPageStatusRangeKind = oapi.GraphMetricBuildPageStatusRangeKind
	GraphMetricBuildPageStatusState     = oapi.GraphMetricBuildPageStatusState
	GraphMetricEdgeFilterStatus         = oapi.GraphMetricEdgeFilterStatus
	GraphMetricEdgeFilterStatusMode     = oapi.GraphMetricEdgeFilterStatusMode
	GraphMetricEvent                    = oapi.GraphMetricEvent
	GraphMetricEventKind                = oapi.GraphMetricEventKind
	GraphMetricFilter                   = oapi.GraphMetricFilter
	GraphMetricFilterOp                 = oapi.GraphMetricFilterOp
	GraphMetricOrder                    = oapi.GraphMetricOrder
	GraphMetricOrderDirection           = oapi.GraphMetricOrderDirection
	GraphMetricOrderNulls               = oapi.GraphMetricOrderNulls
	GraphMetricProfile                  = oapi.GraphMetricProfile
	GraphMetricQuery                    = oapi.GraphMetricQuery
	GraphMetricQueryMetricFreshness     = oapi.GraphMetricQueryMetricFreshness
	GraphMetricRerank                   = oapi.GraphMetricRerank
	GraphMetricRerankMetricFreshness    = oapi.GraphMetricRerankMetricFreshness
	GraphMetricRerankScoreDetails       = oapi.GraphMetricRerankScoreDetails
	GraphMetricResult                   = oapi.GraphMetricResult
	GraphMetricRuntimeStats             = oapi.GraphMetricRuntimeStats
	GraphMetricRuntimeStatsRole         = oapi.GraphMetricRuntimeStatsRole
	GraphMetricScore                    = oapi.GraphMetricScore
	GraphMetricStatus                   = oapi.GraphMetricStatus
	GraphMetricStatusPhase              = oapi.GraphMetricStatusPhase
	GraphQueryMetricFreshness           = oapi.GraphQueryMetricFreshness
	QueryScoreDetails                   = oapi.QueryScoreDetails
	EdgeTypeConfig                      = oapi.EdgeTypeConfig
	EdgeTypeConfigTopology              = oapi.EdgeTypeConfigTopology
	EdgeDirection                       = oapi.EdgeDirection
	Edge                                = oapi.Edge
	EdgesResponse                       = oapi.EdgesResponse

	// Graph query types
	GraphQuery        = oapi.GraphQuery
	GraphQueryParams  = oapi.GraphQueryParams
	GraphQueryResult  = oapi.GraphQueryResult
	GraphQueryType    = oapi.GraphQueryType
	GraphNodeSelector = oapi.GraphNodeSelector
	GraphResultNode   = oapi.GraphResultNode

	// Graph pattern types
	PatternEdgeStep = oapi.PatternEdgeStep
	PatternMatch    = oapi.PatternMatch
	PatternStep     = oapi.PatternStep

	// Graph traversal types
	TraverseResponse = oapi.TraverseResponse

	// Path types
	Path               = oapi.Path
	PathEdge           = oapi.PathEdge
	PathFindRequest    = oapi.PathFindRequest
	PathFindResult     = oapi.PathFindResult
	PathFindWeightMode = oapi.PathFindWeightMode
	PathWeightMode     = oapi.PathWeightMode
)

const (
	QueryHitsTotalRelationExact = oapi.QueryHitsTotalRelationExact
	QueryHitsTotalRelationGte   = oapi.QueryHitsTotalRelationGte

	HierarchyArtifactKindAsset           = oapi.HierarchyArtifactKindAsset
	HierarchyArtifactKindChunk           = oapi.HierarchyArtifactKindChunk
	HierarchyArtifactKindEmbedding       = oapi.HierarchyArtifactKindEmbedding
	HierarchyArtifactSourceKindAsset     = oapi.HierarchyArtifactSourceKindAsset
	HierarchyArtifactSourceKindChunk     = oapi.HierarchyArtifactSourceKindChunk
	HierarchyArtifactSourceKindEmbedding = oapi.HierarchyArtifactSourceKindEmbedding

	QueryHitHierarchyLevelArtifact  = oapi.QueryHitHierarchyLevelArtifact
	QueryHitHierarchyLevelChunk     = oapi.QueryHitHierarchyLevelChunk
	QueryHitHierarchyLevelEmbedding = oapi.QueryHitHierarchyLevelEmbedding
	QueryHitHierarchyLevelMention   = oapi.QueryHitHierarchyLevelMention
	QueryHitHierarchyLevelSource    = oapi.QueryHitHierarchyLevelSource
	QueryHitHierarchyLevelUnit      = oapi.QueryHitHierarchyLevelUnit
)

// QueryHitsTotalValue returns the numeric value from total hit-count metadata.
// Callers that render user-facing output should inspect Relation as well so
// lower-bound totals are not presented as exact counts.
func QueryHitsTotalValue(total QueryHitsTotal) uint64 {
	return total.Value
}

// NewDenseEmbedding creates an Embedding from a float32 slice.
// The vector is sent as a JSON array of floats on the wire.
func NewDenseEmbedding(v []float32) Embedding {
	var emb oapi.Embedding
	if err := emb.FromEmbedding0(oapi.Embedding0(v)); err != nil {
		panic(err) // only fails on marshal error, which can't happen for []float32
	}
	return emb
}

// NewPackedDenseEmbedding creates an Embedding from a float32 slice using the
// packed dense format (base64-encoded little-endian float32 bytes). This is
// ~4x more compact on the wire than the JSON array format from NewDenseEmbedding.
func NewPackedDenseEmbedding(v []float32) Embedding {
	raw := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(f))
	}
	var emb oapi.Embedding
	if err := emb.FromEmbedding2(oapi.Embedding2(raw)); err != nil {
		panic(err) // only fails on marshal error, which can't happen for []byte
	}
	return emb
}

// NewSparseEmbedding creates a sparse Embedding from indices and values.
func NewSparseEmbedding(indices []uint32, values []float32) Embedding {
	var emb oapi.Embedding
	if err := emb.FromEmbedding1(oapi.Embedding1{Indices: indices, Values: values}); err != nil {
		panic(err) // only fails on marshal error
	}
	return emb
}

// NewPackedSparseEmbedding creates a sparse Embedding using the packed format
// (base64-encoded little-endian bytes for both indices and values).
func NewPackedSparseEmbedding(indices []uint32, values []float32) Embedding {
	rawIndices := make([]byte, len(indices)*4)
	for i, idx := range indices {
		binary.LittleEndian.PutUint32(rawIndices[i*4:], idx)
	}
	rawValues := make([]byte, len(values)*4)
	for i, f := range values {
		binary.LittleEndian.PutUint32(rawValues[i*4:], math.Float32bits(f))
	}
	var emb oapi.Embedding
	if err := emb.FromEmbedding3(oapi.Embedding3{PackedIndices: rawIndices, PackedValues: rawValues}); err != nil {
		panic(err) // only fails on marshal error
	}
	return emb
}

// ChunkingModel is just a string - use "fixed" or any ONNX model directory name
// No predefined constants needed since any model name is valid

// Constants from oapi
const (
	// IndexType values
	IndexTypeEmbeddings = oapi.IndexTypeEmbeddings
	IndexTypeFullText   = oapi.IndexTypeFullText
	IndexTypeGraph      = oapi.IndexTypeGraph

	// EnrichmentKind values
	EnrichmentKindAsset     = oapi.EnrichmentKindAsset
	EnrichmentKindChunk     = oapi.EnrichmentKindChunk
	EnrichmentKindEmbedding = oapi.EnrichmentKindEmbedding

	// DistanceMetric values
	DistanceMetricCosine       = oapi.DistanceMetricCosine
	DistanceMetricInnerProduct = oapi.DistanceMetricInnerProduct
	DistanceMetricL2Squared    = oapi.DistanceMetricL2Squared

	// Provider values
	EmbedderProviderAntfly     = oapi.EmbedderProviderAntfly
	EmbedderProviderOllama     = oapi.EmbedderProviderOllama
	EmbedderProviderOpenai     = oapi.EmbedderProviderOpenai
	EmbedderProviderGemini     = oapi.EmbedderProviderGemini
	EmbedderProviderBedrock    = oapi.EmbedderProviderBedrock
	EmbedderProviderVertex     = oapi.EmbedderProviderVertex
	EmbedderProviderMock       = oapi.EmbedderProviderMock
	GeneratorProviderAntfly    = oapi.GeneratorProviderAntfly
	GeneratorProviderOllama    = oapi.GeneratorProviderOllama
	GeneratorProviderOpenai    = oapi.GeneratorProviderOpenai
	GeneratorProviderGemini    = oapi.GeneratorProviderGemini
	GeneratorProviderBedrock   = oapi.GeneratorProviderBedrock
	GeneratorProviderVertex    = oapi.GeneratorProviderVertex
	GeneratorProviderAnthropic = oapi.GeneratorProviderAnthropic
	GeneratorProviderMock      = oapi.GeneratorProviderMock
	RerankerProviderAntfly     = oapi.RerankerProviderAntfly
	RerankerProviderOllama     = oapi.RerankerProviderOllama
	ChunkerProviderAntfly      = oapi.ChunkerProviderAntfly

	// MergeStrategy values
	MergeStrategyRrf      = oapi.MergeStrategyRrf
	MergeStrategyRsf      = oapi.MergeStrategyRsf
	MergeStrategyFailover = oapi.MergeStrategyFailover

	// LinearMergePageStatus values
	LinearMergePageStatusSuccess = oapi.LinearMergePageStatusSuccess
	LinearMergePageStatusPartial = oapi.LinearMergePageStatusPartial
	LinearMergePageStatusError   = oapi.LinearMergePageStatusError

	// SyncLevel values
	SyncLevelPropose     = oapi.SyncLevelPropose
	SyncLevelWrite       = oapi.SyncLevelWrite
	SyncLevelFullText    = oapi.SyncLevelFullText
	SyncLevelFullIndex   = oapi.SyncLevelFullIndex
	SyncLevelEnrichments = oapi.SyncLevelEnrichments

	// RouteType values
	RouteTypeQuestion = oapi.RouteTypeQuestion
	RouteTypeSearch   = oapi.RouteTypeSearch

	// QueryStrategy values
	QueryStrategySimple    = oapi.QueryStrategySimple
	QueryStrategyDecompose = oapi.QueryStrategyDecompose
	QueryStrategyStepBack  = oapi.QueryStrategyStepBack
	QueryStrategyHyde      = oapi.QueryStrategyHyde

	// SemanticQueryMode values
	SemanticQueryModeRewrite      = oapi.SemanticQueryModeRewrite
	SemanticQueryModeHypothetical = oapi.SemanticQueryModeHypothetical

	// ChainCondition values
	ChainConditionAlways      = oapi.ChainConditionAlways
	ChainConditionOnError     = oapi.ChainConditionOnError
	ChainConditionOnTimeout   = oapi.ChainConditionOnTimeout
	ChainConditionOnRateLimit = oapi.ChainConditionOnRateLimit

	// ChatMessageRole values
	ChatMessageRoleUser      = oapi.ChatMessageRoleUser
	ChatMessageRoleAssistant = oapi.ChatMessageRoleAssistant
	ChatMessageRoleSystem    = oapi.ChatMessageRoleSystem
	ChatMessageRoleTool      = oapi.ChatMessageRoleTool

	// ChatToolName values
	ChatToolNameAggregate        = oapi.ChatToolNameAggregate
	ChatToolNameAddFilter        = oapi.ChatToolNameAddFilter
	ChatToolNameAskClarification = oapi.ChatToolNameAskClarification
	ChatToolNameFetch            = oapi.ChatToolNameFetch
	ChatToolNameFullTextSearch   = oapi.ChatToolNameFullTextSearch
	ChatToolNameGraphSearch      = oapi.ChatToolNameGraphSearch
	ChatToolNameSemanticSearch   = oapi.ChatToolNameSemanticSearch
	ChatToolNameTreeSearch       = oapi.ChatToolNameTreeSearch
	ChatToolNameWebSearch        = oapi.ChatToolNameWebSearch

	// FilterSpecOperator values
	FilterSpecOperatorEq       = oapi.FilterSpecOperatorEq
	FilterSpecOperatorNe       = oapi.FilterSpecOperatorNe
	FilterSpecOperatorGt       = oapi.FilterSpecOperatorGt
	FilterSpecOperatorGte      = oapi.FilterSpecOperatorGte
	FilterSpecOperatorLt       = oapi.FilterSpecOperatorLt
	FilterSpecOperatorLte      = oapi.FilterSpecOperatorLte
	FilterSpecOperatorContains = oapi.FilterSpecOperatorContains
	FilterSpecOperatorPrefix   = oapi.FilterSpecOperatorPrefix
	FilterSpecOperatorRange    = oapi.FilterSpecOperatorRange
	FilterSpecOperatorIn       = oapi.FilterSpecOperatorIn

	// AgentQuestionKind values
	AgentQuestionKindConfirm      = oapi.AgentQuestionKindConfirm
	AgentQuestionKindSingleChoice = oapi.AgentQuestionKindSingleChoice
	AgentQuestionKindMultiChoice  = oapi.AgentQuestionKindMultiChoice
	AgentQuestionKindFreeText     = oapi.AgentQuestionKindFreeText
	AgentQuestionKindFieldPolicy  = oapi.AgentQuestionKindFieldPolicy

	// AgentStatus values
	AgentStatusClarificationRequired = oapi.AgentStatusClarificationRequired
	AgentStatusCompleted             = oapi.AgentStatusCompleted
	AgentStatusInProgress            = oapi.AgentStatusInProgress
	AgentStatusIncomplete            = oapi.AgentStatusIncomplete
	AgentStatusFailed                = oapi.AgentStatusFailed

	// AgentStepKind values
	AgentStepKindToolCall       = oapi.AgentStepKindToolCall
	AgentStepKindPlanning       = oapi.AgentStepKindPlanning
	AgentStepKindClassification = oapi.AgentStepKindClassification
	AgentStepKindGeneration     = oapi.AgentStepKindGeneration
	AgentStepKindValidation     = oapi.AgentStepKindValidation
	AgentStepKindClarification  = oapi.AgentStepKindClarification

	// AgentStepStatus values
	AgentStepStatusSuccess = oapi.AgentStepStatusSuccess
	AgentStepStatusError   = oapi.AgentStepStatusError
	AgentStepStatusSkipped = oapi.AgentStepStatusSkipped

	// RetrievalStrategy values
	RetrievalStrategySemantic = oapi.RetrievalStrategySemantic
	RetrievalStrategyBm25     = oapi.RetrievalStrategyBm25
	RetrievalStrategyTree     = oapi.RetrievalStrategyTree
	RetrievalStrategyGraph    = oapi.RetrievalStrategyGraph
	RetrievalStrategyMetadata = oapi.RetrievalStrategyMetadata
	RetrievalStrategyHybrid   = oapi.RetrievalStrategyHybrid

	// EvaluatorName values
	EvaluatorNameCitationQuality = oapi.EvaluatorNameCitationQuality
	EvaluatorNameCoherence       = oapi.EvaluatorNameCoherence
	EvaluatorNameCompleteness    = oapi.EvaluatorNameCompleteness
	EvaluatorNameCorrectness     = oapi.EvaluatorNameCorrectness
	EvaluatorNameFaithfulness    = oapi.EvaluatorNameFaithfulness
	EvaluatorNameHelpfulness     = oapi.EvaluatorNameHelpfulness
	EvaluatorNameMap             = oapi.EvaluatorNameMap
	EvaluatorNameMrr             = oapi.EvaluatorNameMrr
	EvaluatorNameNdcg            = oapi.EvaluatorNameNdcg
	EvaluatorNamePrecision       = oapi.EvaluatorNamePrecision
	EvaluatorNameRecall          = oapi.EvaluatorNameRecall
	EvaluatorNameRelevance       = oapi.EvaluatorNameRelevance
	EvaluatorNameSafety          = oapi.EvaluatorNameSafety

	// AggregationType values
	AggregationTypeAvg              = oapi.AggregationTypeAvg
	AggregationTypeCardinality      = oapi.AggregationTypeCardinality
	AggregationTypeCount            = oapi.AggregationTypeCount
	AggregationTypeDateHistogram    = oapi.AggregationTypeDateHistogram
	AggregationTypeDateRange        = oapi.AggregationTypeDateRange
	AggregationTypeGeoDistance      = oapi.AggregationTypeGeoDistance
	AggregationTypeGeohashGrid      = oapi.AggregationTypeGeohashGrid
	AggregationTypeHistogram        = oapi.AggregationTypeHistogram
	AggregationTypeMax              = oapi.AggregationTypeMax
	AggregationTypeMin              = oapi.AggregationTypeMin
	AggregationTypeRange            = oapi.AggregationTypeRange
	AggregationTypeSignificantTerms = oapi.AggregationTypeSignificantTerms
	AggregationTypeStats            = oapi.AggregationTypeStats
	AggregationTypeSum              = oapi.AggregationTypeSum
	AggregationTypeSumsquares       = oapi.AggregationTypeSumsquares
	AggregationTypeTerms            = oapi.AggregationTypeTerms

	// ForeignSourceType values
	ForeignSourceTypePostgres = oapi.ForeignSourceTypePostgres

	// ReplicationSourceType values
	ReplicationSourceTypePostgres = oapi.ReplicationSourceTypePostgres

	// JoinOperator values
	JoinOperatorEq  = oapi.JoinOperatorEq
	JoinOperatorNeq = oapi.JoinOperatorNeq
	JoinOperatorLt  = oapi.JoinOperatorLt
	JoinOperatorLte = oapi.JoinOperatorLte
	JoinOperatorGt  = oapi.JoinOperatorGt
	JoinOperatorGte = oapi.JoinOperatorGte

	// JoinStrategy values
	JoinStrategyBroadcast   = oapi.JoinStrategyBroadcast
	JoinStrategyIndexLookup = oapi.JoinStrategyIndexLookup
	JoinStrategyShuffle     = oapi.JoinStrategyShuffle

	// JoinType values
	JoinTypeInner = oapi.JoinTypeInner
	JoinTypeLeft  = oapi.JoinTypeLeft
	JoinTypeRight = oapi.JoinTypeRight

	// TransformOpType values
	TransformOpTypeSet         = oapi.TransformOpTypeSET
	TransformOpTypeSetOnInsert = oapi.TransformOpTypeSETONINSERT
	TransformOpTypeUnset       = oapi.TransformOpTypeUNSET
	TransformOpTypeInc         = oapi.TransformOpTypeINC
	TransformOpTypePush        = oapi.TransformOpTypePUSH
	TransformOpTypeMin         = oapi.TransformOpTypeMIN
	TransformOpTypeMax         = oapi.TransformOpTypeMAX
	TransformOpTypeAddToSet    = oapi.TransformOpTypeADDTOSET

	// SyncLevelEmbeddings is a compatibility alias for waiting on all managed index writes.
	SyncLevelEmbeddings = oapi.SyncLevelFullIndex

	// EdgeDirection values
	EdgeDirectionBoth = oapi.EdgeDirectionBoth
	EdgeDirectionIn   = oapi.EdgeDirectionIn
	EdgeDirectionOut  = oapi.EdgeDirectionOut

	// EdgeTypeConfigTopology values
	EdgeTypeConfigTopologyGraph = oapi.EdgeTypeConfigTopologyGraph
	EdgeTypeConfigTopologyTree  = oapi.EdgeTypeConfigTopologyTree

	// GraphQueryType values
	GraphQueryTypeKShortestPaths = oapi.GraphQueryTypeKShortestPaths
	GraphQueryTypeNeighbors      = oapi.GraphQueryTypeNeighbors
	GraphQueryTypePattern        = oapi.GraphQueryTypePattern
	GraphQueryTypeShortestPath   = oapi.GraphQueryTypeShortestPath
	GraphQueryTypeTraverse       = oapi.GraphQueryTypeTraverse

	// PathFindWeightMode values
	PathFindWeightModeMaxWeight = oapi.PathFindWeightModeMaxWeight
	PathFindWeightModeMinHops   = oapi.PathFindWeightModeMinHops
	PathFindWeightModeMinWeight = oapi.PathFindWeightModeMinWeight

	// PathWeightMode values
	PathWeightModeMaxWeight = oapi.PathWeightModeMaxWeight
	PathWeightModeMinHops   = oapi.PathWeightModeMinHops
	PathWeightModeMinWeight = oapi.PathWeightModeMinWeight
)

// allToolNames is the complete set of valid ChatToolName values.
var allToolNames = []ChatToolName{
	ChatToolNameAddFilter, ChatToolNameAskClarification,
	ChatToolNameWebSearch, ChatToolNameFetch,
	ChatToolNameSemanticSearch, ChatToolNameFullTextSearch,
	ChatToolNameTreeSearch, ChatToolNameGraphSearch, ChatToolNameAggregate,
}

// ValidateToolName checks if a tool name is valid.
func ValidateToolName(name ChatToolName) error {
	if slices.Contains(allToolNames, name) {
		return nil
	}
	return fmt.Errorf("unknown tool name %q; valid tools: %v", name, allToolNames)
}
