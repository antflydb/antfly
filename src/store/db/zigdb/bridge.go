//go:build zigdb

package zigdb

/*
#cgo LDFLAGS: -lantfly_zig_capi
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	const uint8_t* ptr;
	size_t len;
} AntflySlice;

typedef struct {
	uint8_t* ptr;
	size_t len;
} AntflyBuffer;

typedef struct {
	uint8_t* id_ptr;
	size_t id_len;
	float score;
} AntflyDenseSearchHit;

typedef struct {
	AntflyDenseSearchHit* hits_ptr;
	size_t hit_count;
	uint32_t total_hits;
} AntflyDenseSearchResult;

typedef struct {
	AntflySlice key;
	AntflySlice value;
	_Bool is_delete;
} AntflyWriteIntent;

typedef struct {
	AntflySlice key;
	uint64_t expected_version;
} AntflyVersionPredicate;

typedef int32_t AntflyErrorCode;

AntflyErrorCode antfly_db_open(const char* path, void** out_handle);
void antfly_db_close(void* handle);
void antfly_db_buffer_free(uint8_t* ptr, size_t len);
void antfly_db_dense_search_result_free(AntflyDenseSearchResult* result);
AntflyErrorCode antfly_db_batch(void* handle, const AntflyWriteIntent* writes, size_t write_count, const AntflyVersionPredicate* predicates, size_t predicate_count, uint64_t timestamp_ns, uint8_t sync_level);
AntflyErrorCode antfly_db_begin_transaction_with_id(void* handle, const uint8_t (*txn_id)[16], uint64_t timestamp_ns, const AntflySlice* participants, size_t participant_count);
AntflyErrorCode antfly_db_write_transaction(void* handle, const uint8_t (*txn_id)[16], const AntflyWriteIntent* writes, size_t write_count, const AntflyVersionPredicate* predicates, size_t predicate_count);
AntflyErrorCode antfly_db_resolve_intents(void* handle, const uint8_t (*txn_id)[16], uint8_t status, uint64_t commit_version);
AntflyErrorCode antfly_db_get_transaction_status(void* handle, const uint8_t (*txn_id)[16], uint8_t* out_status);
AntflyErrorCode antfly_db_get_commit_version(void* handle, const uint8_t (*txn_id)[16], uint64_t* out_commit_version);
AntflyErrorCode antfly_db_get_timestamp(void* handle, AntflySlice key, uint64_t* out_timestamp);
AntflyErrorCode antfly_db_get_raw(void* handle, AntflySlice key, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_lookup_json(void* handle, AntflySlice key, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_set_schema_json(void* handle, AntflySlice schema_json);
AntflyErrorCode antfly_db_extract_enrichments_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_compute_enrichments_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_scan_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_search_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_search_hits_json(void* handle, AntflySlice request_json, AntflyDenseSearchResult* out_result);
AntflyErrorCode antfly_db_search_dense(void* handle, AntflySlice index_name, const float* vector_ptr, size_t vector_len, uint32_t k, uint32_t limit, uint32_t offset, AntflyDenseSearchResult* out_result);
AntflyErrorCode antfly_db_execute_graph_queries_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_aggregate_hits_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_stats_json(void* handle, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_add_index_json(void* handle, AntflySlice config_json);
AntflyErrorCode antfly_db_delete_index(void* handle, AntflySlice name, _Bool* out_deleted);
AntflyErrorCode antfly_db_update_range(void* handle, AntflySlice start, AntflySlice end);
AntflyErrorCode antfly_db_get_range_json(void* handle, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_get_split_state_json(void* handle, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_set_split_state_json(void* handle, AntflySlice state_json);
AntflyErrorCode antfly_db_clear_split_state(void* handle);
AntflyErrorCode antfly_db_get_split_delta_seq(void* handle, uint64_t* out_seq);
AntflyErrorCode antfly_db_get_split_delta_final_seq(void* handle, uint64_t* out_seq);
AntflyErrorCode antfly_db_set_split_delta_final_seq(void* handle, uint64_t seq);
AntflyErrorCode antfly_db_clear_split_delta_final_seq(void* handle);
AntflyErrorCode antfly_db_list_split_delta_entries_after_json(void* handle, uint64_t after_seq, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_clear_split_delta_entries(void* handle);
AntflyErrorCode antfly_db_list_indexes_json(void* handle, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_get_edges_json(void* handle, AntflySlice index_name, AntflySlice key, AntflySlice edge_type, uint8_t direction, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_traverse_edges_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_get_neighbors_json(void* handle, AntflySlice index_name, AntflySlice key, AntflySlice edge_type, uint8_t direction, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_find_shortest_path_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_find_k_shortest_paths_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_match_pattern_json(void* handle, AntflySlice request_json, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_create_shadow_index_manager(void* handle, AntflySlice split_key, AntflySlice original_range_end);
AntflyErrorCode antfly_db_close_shadow_index_manager(void* handle);
AntflyErrorCode antfly_db_get_shadow_index_dir(void* handle, AntflyBuffer* out_buf);
AntflyErrorCode antfly_db_split(void* handle, AntflySlice curr_start, AntflySlice curr_end, AntflySlice split_key, AntflySlice dest_dir1, AntflySlice dest_dir2, _Bool prepare_only);
AntflyErrorCode antfly_db_finalize_split(void* handle, AntflySlice new_start, AntflySlice new_end);
AntflyErrorCode antfly_db_snapshot(void* handle, AntflySlice id, uint64_t* out_size);
*/
import "C"

import (
	"encoding/base64"
	"errors"
	"fmt"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"unsafe"
)

type ErrorCode int32

const (
	ErrorOK              ErrorCode = 0
	ErrorInvalidArgument ErrorCode = 1
	ErrorNotFound        ErrorCode = 2
	ErrorVersionConflict ErrorCode = 3
	ErrorIntentConflict  ErrorCode = 4
	ErrorTxnNotFound     ErrorCode = 5
	ErrorInternal        ErrorCode = 255
)

var (
	ErrInvalidArgument = errors.New("zigdb invalid argument")
	ErrNotFound        = errors.New("zigdb not found")
	ErrVersionConflict = errors.New("zigdb version conflict")
	ErrIntentConflict  = errors.New("zigdb intent conflict")
	ErrTxnNotFound     = errors.New("zigdb transaction not found")
	ErrInternal        = errors.New("zigdb internal error")
)

func mapError(code C.AntflyErrorCode) error {
	switch ErrorCode(code) {
	case ErrorOK:
		return nil
	case ErrorInvalidArgument:
		return ErrInvalidArgument
	case ErrorNotFound:
		return ErrNotFound
	case ErrorVersionConflict:
		return ErrVersionConflict
	case ErrorIntentConflict:
		return ErrIntentConflict
	case ErrorTxnNotFound:
		return ErrTxnNotFound
	case ErrorInternal:
		return ErrInternal
	default:
		return fmt.Errorf("zigdb error code %d", int32(code))
	}
}

type Bridge struct {
	handle unsafe.Pointer
}

type WriteIntent struct {
	Key      []byte
	Value    []byte
	IsDelete bool
}

type VersionPredicate struct {
	Key             []byte
	ExpectedVersion uint64
}

type RangePayload struct {
	StartB64 string `json:"start_b64"`
	EndB64   string `json:"end_b64"`
}

type SplitStatePayload struct {
	Phase               uint8  `json:"phase"`
	SplitKeyB64         string `json:"split_key_b64"`
	NewShardID          uint64 `json:"new_shard_id"`
	StartedAt           uint64 `json:"started_at"`
	OriginalRangeEndB64 string `json:"original_range_end_b64"`
}

type SplitDeltaWritePayload struct {
	KeyB64   string `json:"key_b64"`
	ValueB64 string `json:"value_b64"`
}

type SplitDeltaEntryPayload struct {
	Sequence   uint64                   `json:"sequence"`
	Timestamp  uint64                   `json:"timestamp"`
	Writes     []SplitDeltaWritePayload `json:"writes"`
	DeletesB64 []string                 `json:"deletes_b64"`
}

type IndexConfigPayload struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	ConfigJSON string `json:"config_json"`
}

type ScanRequestPayload struct {
	FromKeyB64       string   `json:"from_key_b64"`
	ToKeyB64         string   `json:"to_key_b64"`
	InclusiveFrom    bool     `json:"inclusive_from"`
	ExclusiveTo      bool     `json:"exclusive_to"`
	IncludeDocuments bool     `json:"include_documents"`
	Limit            uint32   `json:"limit"`
	Fields           []string `json:"fields,omitempty"`
	IncludeAllFields bool     `json:"include_all_fields"`
}

type ScanHashPayload struct {
	IDB64 string `json:"id_b64"`
	Hash  uint64 `json:"hash"`
}

type ScanDocumentPayload struct {
	IDB64 string `json:"id_b64"`
	JSON  string `json:"json"`
}

type ScanResultPayload struct {
	Hashes    []ScanHashPayload     `json:"hashes"`
	Documents []ScanDocumentPayload `json:"documents"`
}

type EnrichmentWriteRequestPayload struct {
	Writes []WritePairPayload `json:"writes"`
}

type WritePairPayload struct {
	KeyB64   string `json:"key_b64"`
	ValueB64 string `json:"value_b64"`
}

type ExtractDenseEmbeddingPayload struct {
	IndexName      string    `json:"index_name"`
	DocKeyB64      string    `json:"doc_key_b64"`
	ArtifactKeyB64 string    `json:"artifact_key_b64,omitempty"`
	Vector         []float32 `json:"vector"`
}

type ExtractSparseEmbeddingPayload struct {
	IndexName string    `json:"index_name"`
	DocKeyB64 string    `json:"doc_key_b64"`
	Indices   []uint32  `json:"indices"`
	Values    []float32 `json:"values"`
}

type ExtractSummaryPayload struct {
	IndexName string `json:"index_name"`
	DocKeyB64 string `json:"doc_key_b64"`
	Text      string `json:"text"`
}

type ExtractGraphWritePayload struct {
	IndexName    string  `json:"index_name"`
	SourceB64    string  `json:"source_b64"`
	TargetB64    string  `json:"target_b64"`
	EdgeType     string  `json:"edge_type"`
	Weight       float64 `json:"weight"`
	CreatedAt    uint64  `json:"created_at"`
	UpdatedAt    uint64  `json:"updated_at"`
	MetadataJSON string  `json:"metadata_json"`
}

type ExtractEnrichmentsPayload struct {
	DenseEmbeddings  []ExtractDenseEmbeddingPayload  `json:"dense_embeddings"`
	SparseEmbeddings []ExtractSparseEmbeddingPayload `json:"sparse_embeddings"`
	Summaries        []ExtractSummaryPayload         `json:"summaries"`
	GraphWrites      []ExtractGraphWritePayload      `json:"graph_writes"`
}

type ComputeDocumentPayload struct {
	KeyB64           string   `json:"key_b64"`
	ValueB64         string   `json:"value_b64"`
	TargetIndexNames []string `json:"target_index_names"`
}

type ComputeEnrichmentsPayload struct {
	ArtifactWrites  []WritePairPayload             `json:"artifact_writes"`
	Documents       []ComputeDocumentPayload       `json:"documents"`
	DenseEmbeddings []ExtractDenseEmbeddingPayload `json:"dense_embeddings"`
	FailedKeysB64   []string                       `json:"failed_keys_b64"`
}

type SearchRequestPayload struct {
	Mode               string                            `json:"mode"`
	IndexName          string                            `json:"index_name,omitempty"`
	TextQueryType      string                            `json:"text_query_type,omitempty"`
	TextQueryJSON      string                            `json:"text_query_json,omitempty"`
	Field              string                            `json:"field,omitempty"`
	Text               string                            `json:"text,omitempty"`
	Vector             []float32                         `json:"vector,omitempty"`
	Indices            []uint32                          `json:"indices,omitempty"`
	Values             []float32                         `json:"values,omitempty"`
	K                  uint32                            `json:"k,omitempty"`
	ReturnMode         string                            `json:"return_mode,omitempty"`
	MaxChunksPerParent uint32                            `json:"max_chunks_per_parent,omitempty"`
	Limit              uint32                            `json:"limit,omitempty"`
	Offset             uint32                            `json:"offset,omitempty"`
	IncludeStored      bool                              `json:"include_stored"`
	GraphQueries       []SearchGraphQueryPayload         `json:"graph_queries,omitempty"`
	Aggregations       []SearchAggregationRequestPayload `json:"aggregations,omitempty"`
}

type SearchGraphNodeSelectorPayload struct {
	Keys      []string `json:"keys,omitempty"`
	ResultRef string   `json:"result_ref,omitempty"`
	Limit     uint32   `json:"limit,omitempty"`
}

type SearchGraphQueryPayload struct {
	Name         string                          `json:"name"`
	Type         string                          `json:"type"`
	IndexName    string                          `json:"index_name"`
	StartNodes   SearchGraphNodeSelectorPayload  `json:"start_nodes"`
	TargetNodes  *SearchGraphNodeSelectorPayload `json:"target_nodes,omitempty"`
	EdgeTypes    []string                        `json:"edge_types,omitempty"`
	Direction    string                          `json:"direction,omitempty"`
	MaxDepth     uint32                          `json:"max_depth,omitempty"`
	MaxResults   uint32                          `json:"max_results,omitempty"`
	MinWeight    float64                         `json:"min_weight,omitempty"`
	MaxWeight    float64                         `json:"max_weight,omitempty"`
	Deduplicate  bool                            `json:"deduplicate,omitempty"`
	IncludePaths bool                            `json:"include_paths,omitempty"`
	WeightMode   string                          `json:"weight_mode,omitempty"`
	K            uint32                          `json:"k,omitempty"`
}

type NamedGraphInputSetPayload struct {
	Name      string   `json:"name"`
	HitIDsB64 []string `json:"hit_ids_b64,omitempty"`
	TotalHits uint32   `json:"total_hits,omitempty"`
}

type ExecuteGraphQueriesRequestPayload struct {
	GraphQueries  []SearchGraphQueryPayload   `json:"graph_queries"`
	NamedSets     []NamedGraphInputSetPayload `json:"named_sets"`
	Limit         uint32                      `json:"limit,omitempty"`
	Offset        uint32                      `json:"offset,omitempty"`
	IncludeStored bool                        `json:"include_stored"`
}

type SearchAggregationRequestPayload struct {
	Name                  string                            `json:"name"`
	Type                  string                            `json:"type"`
	Field                 string                            `json:"field"`
	Size                  int                               `json:"size,omitempty"`
	Interval              float64                           `json:"interval,omitempty"`
	CalendarInterval      string                            `json:"calendar_interval,omitempty"`
	FixedInterval         string                            `json:"fixed_interval,omitempty"`
	MinDocCount           int64                             `json:"min_doc_count,omitempty"`
	SignificanceAlgorithm string                            `json:"significance_algorithm,omitempty"`
	BackgroundQueryType   string                            `json:"background_query_type,omitempty"`
	BackgroundField       string                            `json:"background_field,omitempty"`
	BackgroundText        string                            `json:"background_text,omitempty"`
	BucketPath            string                            `json:"bucket_path,omitempty"`
	SortOrder             string                            `json:"sort_order,omitempty"`
	From                  int                               `json:"from,omitempty"`
	Window                int                               `json:"window,omitempty"`
	GapPolicy             string                            `json:"gap_policy,omitempty"`
	TermPrefix            string                            `json:"term_prefix,omitempty"`
	TermPattern           string                            `json:"term_pattern,omitempty"`
	NumericRanges         []SearchNumericRangePayload       `json:"ranges,omitempty"`
	DateTimeRanges        []SearchDateTimeRangePayload      `json:"date_ranges,omitempty"`
	DistanceRanges        []SearchDistanceRangePayload      `json:"distance_ranges,omitempty"`
	CenterLat             float64                           `json:"center_lat,omitempty"`
	CenterLon             float64                           `json:"center_lon,omitempty"`
	DistanceUnit          string                            `json:"distance_unit,omitempty"`
	GeohashPrecision      int                               `json:"geohash_precision,omitempty"`
	Aggregations          []SearchAggregationRequestPayload `json:"aggregations,omitempty"`
}

type SearchNumericRangePayload struct {
	Name  string   `json:"name,omitempty"`
	Start *float64 `json:"start,omitempty"`
	End   *float64 `json:"end,omitempty"`
}

type SearchDateTimeRangePayload struct {
	Name  string  `json:"name,omitempty"`
	Start *string `json:"start,omitempty"`
	End   *string `json:"end,omitempty"`
}

type SearchDistanceRangePayload struct {
	Name string   `json:"name,omitempty"`
	From *float64 `json:"from,omitempty"`
	To   *float64 `json:"to,omitempty"`
}

type SearchChunkHitPayload struct {
	IDB64      string   `json:"id_b64"`
	IDRaw      []byte   `json:"-"`
	Score      *float32 `json:"score,omitempty"`
	StoredJSON string   `json:"stored_json,omitempty"`
}

type SearchHitPayload struct {
	IDB64      string                  `json:"id_b64"`
	IDRaw      []byte                  `json:"-"`
	Score      *float32                `json:"score,omitempty"`
	StoredJSON string                  `json:"stored_json,omitempty"`
	ChunkHits  []SearchChunkHitPayload `json:"chunk_hits,omitempty"`
}

type SearchResultPayload struct {
	TotalHits    uint32                           `json:"total_hits"`
	Hits         []SearchHitPayload               `json:"hits"`
	GraphResults []SearchGraphResultPayload       `json:"graph_results,omitempty"`
	Aggregations []SearchAggregationResultPayload `json:"aggregations,omitempty"`
}

type SearchGraphResultPayload struct {
	Name      string                   `json:"name"`
	TotalHits uint32                   `json:"total_hits"`
	Nodes     []SearchGraphNodePayload `json:"nodes,omitempty"`
	Paths     []PathPayload            `json:"paths,omitempty"`
	Hits      []SearchHitPayload       `json:"hits"`
}

type SearchGraphNodePayload struct {
	KeyB64    string            `json:"key_b64"`
	Depth     uint32            `json:"depth"`
	Distance  float64           `json:"distance"`
	PathB64   []string          `json:"path_b64,omitempty"`
	PathEdges []PathEdgePayload `json:"path_edges,omitempty"`
}

type SearchAggregationResultPayload struct {
	Name         string                           `json:"name"`
	Field        string                           `json:"field"`
	Type         string                           `json:"type"`
	ValueJSON    string                           `json:"value_json,omitempty"`
	MetadataJSON string                           `json:"metadata_json,omitempty"`
	Buckets      []SearchAggregationBucketPayload `json:"buckets,omitempty"`
}

type SearchAggregationBucketPayload struct {
	KeyJSON      string                           `json:"key_json"`
	Count        int64                            `json:"count"`
	Score        *float64                         `json:"score,omitempty"`
	BgCount      *int64                           `json:"bg_count,omitempty"`
	Aggregations []SearchAggregationResultPayload `json:"aggregations,omitempty"`
}

type AggregateHitsRequestPayload struct {
	IndexName    string                            `json:"index_name,omitempty"`
	HitIDsB64    []string                          `json:"hit_ids_b64"`
	Aggregations []SearchAggregationRequestPayload `json:"aggregations,omitempty"`
}

type DBStatsPayload struct {
	DocCount               uint64                          `json:"doc_count"`
	IndexCount             uint32                          `json:"index_count"`
	Indexes                []DBIndexStatsPayload           `json:"indexes"`
	Enrichment             EnrichmentStatsPayload          `json:"enrichment"`
	TTLCleanup             TTLCleanupStatsPayload          `json:"ttl_cleanup"`
	TransactionRecovery    TransactionRecoveryStatsPayload `json:"transaction_recovery"`
	TermDocFreqCacheHits   uint64                          `json:"term_doc_freq_cache_hits"`
	TermDocFreqCacheMisses uint64                          `json:"term_doc_freq_cache_misses"`
}

type DBIndexStatsPayload struct {
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	DocCount  uint64 `json:"doc_count"`
	TermCount uint64 `json:"term_count"`
	EdgeCount uint64 `json:"edge_count"`
	NodeCount uint64 `json:"node_count"`
}

type EnrichmentStatsPayload struct {
	Enabled              bool   `json:"enabled"`
	LeaseOwned           bool   `json:"lease_owned"`
	HasLease             bool   `json:"has_lease"`
	AcquisitionCount     uint64 `json:"acquisition_count"`
	LeaseAcquireFailures uint64 `json:"lease_acquire_failures"`
	LostLeases           uint64 `json:"lost_leases"`
	LastAcquiredMS       uint64 `json:"last_acquired_ms"`
	TargetSequence       uint64 `json:"target_sequence"`
	AppliedSequence      uint64 `json:"applied_sequence"`
	ProcessedRequests    uint64 `json:"processed_requests"`
	ErrorCount           uint64 `json:"error_count"`
}

type TTLCleanupStatsPayload struct {
	Enabled              bool   `json:"enabled"`
	LeaseOwned           bool   `json:"lease_owned"`
	HasLease             bool   `json:"has_lease"`
	AcquisitionCount     uint64 `json:"acquisition_count"`
	Runs                 uint64 `json:"runs"`
	ScannedTimestamps    uint64 `json:"scanned_timestamps"`
	DeletedDocs          uint64 `json:"deleted_docs"`
	LastRunNS            uint64 `json:"last_run_ns"`
	ErrorCount           uint64 `json:"error_count"`
	LeaseAcquireFailures uint64 `json:"lease_acquire_failures"`
	LostLeases           uint64 `json:"lost_leases"`
	LastAcquiredMS       uint64 `json:"last_acquired_ms"`
}

type TransactionRecoveryStatsPayload struct {
	Enabled               bool   `json:"enabled"`
	LeaseOwned            bool   `json:"lease_owned"`
	HasLease              bool   `json:"has_lease"`
	AcquisitionCount      uint64 `json:"acquisition_count"`
	LeaseAcquireFailures  uint64 `json:"lease_acquire_failures"`
	LostLeases            uint64 `json:"lost_leases"`
	LastAcquiredMS        uint64 `json:"last_acquired_ms"`
	Runs                  uint64 `json:"runs"`
	ScannedRecords        uint64 `json:"scanned_records"`
	AutoAborted           uint64 `json:"auto_aborted"`
	ResolvedFinalized     uint64 `json:"resolved_finalized"`
	CleanedRecords        uint64 `json:"cleaned_records"`
	KeptRecentPending     uint64 `json:"kept_recent_pending"`
	DeferredUnresolved    uint64 `json:"deferred_unresolved"`
	NotificationAttempts  uint64 `json:"notification_attempts"`
	NotificationSuccesses uint64 `json:"notification_successes"`
	NotificationFailures  uint64 `json:"notification_failures"`
	LastRunNS             uint64 `json:"last_run_ns"`
	ErrorCount            uint64 `json:"error_count"`
}

type AddIndexPayload struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	ConfigJSON string `json:"config_json"`
}

type EdgePayload struct {
	SourceB64    string  `json:"source_b64"`
	TargetB64    string  `json:"target_b64"`
	EdgeType     string  `json:"edge_type"`
	Weight       float64 `json:"weight"`
	CreatedAt    uint64  `json:"created_at"`
	UpdatedAt    uint64  `json:"updated_at"`
	MetadataJSON string  `json:"metadata_json"`
}

type TraversalRequestPayload struct {
	IndexName        string   `json:"index_name"`
	StartKeyB64      string   `json:"start_key_b64"`
	EdgeTypes        []string `json:"edge_types,omitempty"`
	Direction        uint8    `json:"direction"`
	MaxDepth         uint32   `json:"max_depth"`
	MinWeight        float64  `json:"min_weight"`
	MaxWeight        float64  `json:"max_weight"`
	MaxResults       uint32   `json:"max_results"`
	DeduplicateNodes bool     `json:"deduplicate_nodes"`
	IncludePaths     bool     `json:"include_paths"`
}

type TraversalResultPayload struct {
	KeyB64      string   `json:"key_b64"`
	Depth       uint32   `json:"depth"`
	TotalWeight float64  `json:"total_weight"`
	PathB64     []string `json:"path_b64,omitempty"`
}

type ShortestPathRequestPayload struct {
	IndexName  string   `json:"index_name"`
	SourceB64  string   `json:"source_b64"`
	TargetB64  string   `json:"target_b64"`
	K          uint32   `json:"k,omitempty"`
	EdgeTypes  []string `json:"edge_types,omitempty"`
	Direction  uint8    `json:"direction"`
	WeightMode string   `json:"weight_mode"`
	MaxDepth   uint32   `json:"max_depth"`
	MinWeight  float64  `json:"min_weight"`
	MaxWeight  float64  `json:"max_weight"`
}

type PathEdgePayload struct {
	SourceB64 string  `json:"source_b64"`
	TargetB64 string  `json:"target_b64"`
	EdgeType  string  `json:"edge_type"`
	Weight    float64 `json:"weight"`
}

type PathPayload struct {
	NodesB64    []string          `json:"nodes_b64"`
	Edges       []PathEdgePayload `json:"edges"`
	TotalWeight float64           `json:"total_weight"`
	Length      uint32            `json:"length"`
}

type PatternNodeFilterPayload struct {
	FilterPrefix string `json:"filter_prefix,omitempty"`
	QueryJSON    string `json:"query_json,omitempty"`
}

type PatternEdgeStepPayload struct {
	Direction uint8    `json:"direction"`
	MinHops   uint32   `json:"min_hops,omitempty"`
	MaxHops   uint32   `json:"max_hops,omitempty"`
	MinWeight float64  `json:"min_weight,omitempty"`
	MaxWeight float64  `json:"max_weight,omitempty"`
	Types     []string `json:"types,omitempty"`
}

type PatternStepPayload struct {
	Alias      string                   `json:"alias,omitempty"`
	Edge       PatternEdgeStepPayload   `json:"edge,omitempty"`
	NodeFilter PatternNodeFilterPayload `json:"node_filter,omitempty"`
}

type PatternRequestPayload struct {
	IndexName     string               `json:"index_name"`
	StartNodesB64 []string             `json:"start_nodes_b64"`
	Pattern       []PatternStepPayload `json:"pattern"`
	MaxResults    uint32               `json:"max_results,omitempty"`
	ReturnAliases []string             `json:"return_aliases,omitempty"`
}

type PatternBindingPayload struct {
	Alias  string `json:"alias"`
	KeyB64 string `json:"key_b64"`
	Depth  uint32 `json:"depth"`
}

type PatternMatchPayload struct {
	Bindings []PatternBindingPayload `json:"bindings"`
	Path     []PathEdgePayload       `json:"path,omitempty"`
}

func Open(path string) (*Bridge, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var handle unsafe.Pointer
	if err := mapError(C.antfly_db_open(cPath, (*unsafe.Pointer)(unsafe.Pointer(&handle)))); err != nil {
		return nil, err
	}
	return &Bridge{handle: handle}, nil
}

func (b *Bridge) Close() {
	if b == nil || b.handle == nil {
		return
	}
	C.antfly_db_close(b.handle)
	b.handle = nil
}

func (b *Bridge) Batch(writes []WriteIntent, predicates []VersionPredicate, timestamp uint64, syncLevel uint8) error {
	cWrites := make([]C.AntflyWriteIntent, len(writes))
	writeAllocs := make([]unsafe.Pointer, 0, len(writes)*2)
	defer freeCBytes(writeAllocs)
	for i := range writes {
		keySlice, keyAlloc := toSliceCopy(writes[i].Key)
		if keyAlloc != nil {
			writeAllocs = append(writeAllocs, keyAlloc)
		}
		valSlice, valAlloc := toSliceCopy(writes[i].Value)
		if valAlloc != nil {
			writeAllocs = append(writeAllocs, valAlloc)
		}
		cWrites[i] = C.AntflyWriteIntent{
			key:       keySlice,
			value:     valSlice,
			is_delete: C._Bool(writes[i].IsDelete),
		}
	}
	cPredicates := make([]C.AntflyVersionPredicate, len(predicates))
	predicateAllocs := make([]unsafe.Pointer, 0, len(predicates))
	defer freeCBytes(predicateAllocs)
	for i := range predicates {
		keySlice, keyAlloc := toSliceCopy(predicates[i].Key)
		if keyAlloc != nil {
			predicateAllocs = append(predicateAllocs, keyAlloc)
		}
		cPredicates[i] = C.AntflyVersionPredicate{
			key:              keySlice,
			expected_version: C.uint64_t(predicates[i].ExpectedVersion),
		}
	}
	var writesPtr *C.AntflyWriteIntent
	if len(cWrites) > 0 {
		writesPtr = &cWrites[0]
	}
	var predPtr *C.AntflyVersionPredicate
	if len(cPredicates) > 0 {
		predPtr = &cPredicates[0]
	}
	return mapError(C.antfly_db_batch(
		b.handle,
		writesPtr,
		C.size_t(len(cWrites)),
		predPtr,
		C.size_t(len(cPredicates)),
		C.uint64_t(timestamp),
		C.uint8_t(syncLevel),
	))
}

func (b *Bridge) BeginTransactionWithID(txnID [16]byte, timestamp uint64, participants [][]byte) error {
	cParticipants := make([]C.AntflySlice, len(participants))
	participantAllocs := make([]unsafe.Pointer, 0, len(participants))
	defer freeCBytes(participantAllocs)
	for i := range participants {
		slice, alloc := toSliceCopy(participants[i])
		if alloc != nil {
			participantAllocs = append(participantAllocs, alloc)
		}
		cParticipants[i] = slice
	}
	var ptr *C.AntflySlice
	if len(cParticipants) > 0 {
		ptr = &cParticipants[0]
	}
	return mapError(C.antfly_db_begin_transaction_with_id(
		b.handle,
		(*[16]C.uint8_t)(unsafe.Pointer(&txnID[0])),
		C.uint64_t(timestamp),
		ptr,
		C.size_t(len(cParticipants)),
	))
}

func (b *Bridge) WriteTransaction(txnID [16]byte, writes []WriteIntent, predicates []VersionPredicate) error {
	cWrites := make([]C.AntflyWriteIntent, len(writes))
	writeAllocs := make([]unsafe.Pointer, 0, len(writes)*2)
	defer freeCBytes(writeAllocs)
	for i := range writes {
		keySlice, keyAlloc := toSliceCopy(writes[i].Key)
		if keyAlloc != nil {
			writeAllocs = append(writeAllocs, keyAlloc)
		}
		valSlice, valAlloc := toSliceCopy(writes[i].Value)
		if valAlloc != nil {
			writeAllocs = append(writeAllocs, valAlloc)
		}
		cWrites[i] = C.AntflyWriteIntent{
			key:       keySlice,
			value:     valSlice,
			is_delete: C._Bool(writes[i].IsDelete),
		}
	}
	cPredicates := make([]C.AntflyVersionPredicate, len(predicates))
	predicateAllocs := make([]unsafe.Pointer, 0, len(predicates))
	defer freeCBytes(predicateAllocs)
	for i := range predicates {
		keySlice, keyAlloc := toSliceCopy(predicates[i].Key)
		if keyAlloc != nil {
			predicateAllocs = append(predicateAllocs, keyAlloc)
		}
		cPredicates[i] = C.AntflyVersionPredicate{
			key:              keySlice,
			expected_version: C.uint64_t(predicates[i].ExpectedVersion),
		}
	}
	var writesPtr *C.AntflyWriteIntent
	if len(cWrites) > 0 {
		writesPtr = &cWrites[0]
	}
	var predPtr *C.AntflyVersionPredicate
	if len(cPredicates) > 0 {
		predPtr = &cPredicates[0]
	}
	return mapError(C.antfly_db_write_transaction(
		b.handle,
		(*[16]C.uint8_t)(unsafe.Pointer(&txnID[0])),
		writesPtr,
		C.size_t(len(cWrites)),
		predPtr,
		C.size_t(len(cPredicates)),
	))
}

func (b *Bridge) ResolveIntents(txnID [16]byte, status uint8, commitVersion uint64) error {
	return mapError(C.antfly_db_resolve_intents(
		b.handle,
		(*[16]C.uint8_t)(unsafe.Pointer(&txnID[0])),
		C.uint8_t(status),
		C.uint64_t(commitVersion),
	))
}

func (b *Bridge) GetTransactionStatus(txnID [16]byte) (uint8, error) {
	var status C.uint8_t
	if err := mapError(C.antfly_db_get_transaction_status(
		b.handle,
		(*[16]C.uint8_t)(unsafe.Pointer(&txnID[0])),
		&status,
	)); err != nil {
		return 0, err
	}
	return uint8(status), nil
}

func (b *Bridge) GetCommitVersion(txnID [16]byte) (uint64, error) {
	var commitVersion C.uint64_t
	if err := mapError(C.antfly_db_get_commit_version(
		b.handle,
		(*[16]C.uint8_t)(unsafe.Pointer(&txnID[0])),
		&commitVersion,
	)); err != nil {
		return 0, err
	}
	return uint64(commitVersion), nil
}

func (b *Bridge) GetTimestamp(key []byte) (uint64, error) {
	var timestamp C.uint64_t
	if err := mapError(C.antfly_db_get_timestamp(b.handle, toSlice(key), &timestamp)); err != nil {
		return 0, err
	}
	return uint64(timestamp), nil
}

func (b *Bridge) LookupJSON(key []byte) ([]byte, error) {
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_lookup_json(b.handle, toSlice(key), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	if buf.ptr == nil || buf.len == 0 {
		return nil, nil
	}
	return C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), nil
}

func (b *Bridge) GetRaw(key []byte) ([]byte, error) {
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_get_raw(b.handle, toSlice(key), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	if buf.ptr == nil || buf.len == 0 {
		return nil, nil
	}
	return C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), nil
}

func (b *Bridge) SetSchemaJSON(raw []byte) error {
	return mapError(C.antfly_db_set_schema_json(b.handle, toSlice(raw)))
}

func (b *Bridge) ExtractEnrichments(writes [][2][]byte) (*ExtractEnrichmentsPayload, error) {
	req := EnrichmentWriteRequestPayload{
		Writes: make([]WritePairPayload, 0, len(writes)),
	}
	for _, write := range writes {
		req.Writes = append(req.Writes, WritePairPayload{
			KeyB64:   EncodeBase64(write[0]),
			ValueB64: EncodeBase64(write[1]),
		})
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_extract_enrichments_json(b.handle, toSlice(raw), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload ExtractEnrichmentsPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (b *Bridge) ComputeEnrichments(writes [][2][]byte) (*ComputeEnrichmentsPayload, error) {
	req := EnrichmentWriteRequestPayload{
		Writes: make([]WritePairPayload, 0, len(writes)),
	}
	for _, write := range writes {
		req.Writes = append(req.Writes, WritePairPayload{
			KeyB64:   EncodeBase64(write[0]),
			ValueB64: EncodeBase64(write[1]),
		})
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_compute_enrichments_json(b.handle, toSlice(raw), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload ComputeEnrichmentsPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (b *Bridge) UpdateRange(start, end []byte) error {
	return mapError(C.antfly_db_update_range(b.handle, toSlice(start), toSlice(end)))
}

func (b *Bridge) GetRange() (RangePayload, error) {
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_get_range_json(b.handle, &buf)); err != nil {
		return RangePayload{}, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload RangePayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return RangePayload{}, err
	}
	return payload, nil
}

func (b *Bridge) GetSplitState() (*SplitStatePayload, error) {
	var buf C.AntflyBuffer
	err := mapError(C.antfly_db_get_split_state_json(b.handle, &buf))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload SplitStatePayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (b *Bridge) SetSplitState(state SplitStatePayload) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return mapError(C.antfly_db_set_split_state_json(b.handle, toSlice(raw)))
}

func (b *Bridge) ClearSplitState() error {
	return mapError(C.antfly_db_clear_split_state(b.handle))
}

func (b *Bridge) GetSplitDeltaSeq() (uint64, error) {
	var seq C.uint64_t
	if err := mapError(C.antfly_db_get_split_delta_seq(b.handle, &seq)); err != nil {
		return 0, err
	}
	return uint64(seq), nil
}

func (b *Bridge) GetSplitDeltaFinalSeq() (uint64, error) {
	var seq C.uint64_t
	if err := mapError(C.antfly_db_get_split_delta_final_seq(b.handle, &seq)); err != nil {
		return 0, err
	}
	return uint64(seq), nil
}

func (b *Bridge) SetSplitDeltaFinalSeq(seq uint64) error {
	return mapError(C.antfly_db_set_split_delta_final_seq(b.handle, C.uint64_t(seq)))
}

func (b *Bridge) ClearSplitDeltaFinalSeq() error {
	return mapError(C.antfly_db_clear_split_delta_final_seq(b.handle))
}

func (b *Bridge) ListSplitDeltaEntriesAfter(afterSeq uint64) ([]SplitDeltaEntryPayload, error) {
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_list_split_delta_entries_after_json(b.handle, C.uint64_t(afterSeq), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload []SplitDeltaEntryPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *Bridge) ClearSplitDeltaEntries() error {
	return mapError(C.antfly_db_clear_split_delta_entries(b.handle))
}

func (b *Bridge) ListIndexes() ([]IndexConfigPayload, error) {
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_list_indexes_json(b.handle, &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload []IndexConfigPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *Bridge) Scan(req ScanRequestPayload) (*ScanResultPayload, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_scan_json(b.handle, toSlice(raw), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload ScanResultPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (b *Bridge) Search(req SearchRequestPayload) (*SearchResultPayload, error) {
	if densePayload, ok, err := b.searchDenseFast(req); err != nil {
		return nil, err
	} else if ok {
		return densePayload, nil
	}
	if simplePayload, ok, err := b.searchHitsFast(req); err != nil {
		return nil, err
	} else if ok {
		return simplePayload, nil
	}

	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_search_json(b.handle, toSlice(raw), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload SearchResultPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (b *Bridge) searchDenseFast(req SearchRequestPayload) (*SearchResultPayload, bool, error) {
	if req.Mode != "dense" || req.IndexName == "" || len(req.Vector) == 0 {
		return nil, false, nil
	}
	if req.IncludeStored || len(req.Aggregations) > 0 {
		return nil, false, nil
	}
	if req.ReturnMode != "" && req.ReturnMode != "parent" {
		return nil, false, nil
	}

	var result C.AntflyDenseSearchResult
	var vecPtr *C.float
	if len(req.Vector) > 0 {
		vecPtr = (*C.float)(unsafe.Pointer(&req.Vector[0]))
	}
	if err := mapError(C.antfly_db_search_dense(
		b.handle,
		toSlice([]byte(req.IndexName)),
		vecPtr,
		C.size_t(len(req.Vector)),
		C.uint32_t(req.K),
		C.uint32_t(req.Limit),
		C.uint32_t(req.Offset),
		&result,
	)); err != nil {
		return nil, false, err
	}
	defer C.antfly_db_dense_search_result_free(&result)

	hits := make([]SearchHitPayload, int(result.hit_count))
	if result.hit_count > 0 {
		rawHits := unsafe.Slice(result.hits_ptr, int(result.hit_count))
		for i, hit := range rawHits {
			id := C.GoBytes(unsafe.Pointer(hit.id_ptr), C.int(hit.id_len))
			score := float32(hit.score)
			hits[i] = SearchHitPayload{
				IDB64: base64.StdEncoding.EncodeToString(id),
				Score: &score,
			}
		}
	}

	return &SearchResultPayload{
		TotalHits: uint32(result.total_hits),
		Hits:      hits,
	}, true, nil
}

func (b *Bridge) searchHitsFast(req SearchRequestPayload) (*SearchResultPayload, bool, error) {
	if req.Mode != "full_text" && req.Mode != "sparse" {
		return nil, false, nil
	}
	if req.IncludeStored || len(req.Aggregations) > 0 || len(req.GraphQueries) > 0 {
		return nil, false, nil
	}
	if req.ReturnMode != "" && req.ReturnMode != "parent" {
		return nil, false, nil
	}

	raw, err := json.Marshal(req)
	if err != nil {
		return nil, false, err
	}
	var result C.AntflyDenseSearchResult
	if err := mapError(C.antfly_db_search_hits_json(b.handle, toSlice(raw), &result)); err != nil {
		return nil, false, err
	}
	defer C.antfly_db_dense_search_result_free(&result)

	hits := make([]SearchHitPayload, int(result.hit_count))
	if result.hit_count > 0 {
		rawHits := unsafe.Slice(result.hits_ptr, int(result.hit_count))
		for i, hit := range rawHits {
			id := C.GoBytes(unsafe.Pointer(hit.id_ptr), C.int(hit.id_len))
			score := float32(hit.score)
			hits[i] = SearchHitPayload{
				IDRaw: id,
				Score: &score,
			}
		}
	}

	return &SearchResultPayload{
		TotalHits: uint32(result.total_hits),
		Hits:      hits,
	}, true, nil
}

func (b *Bridge) ExecuteGraphQueries(req ExecuteGraphQueriesRequestPayload) ([]SearchGraphResultPayload, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_execute_graph_queries_json(b.handle, toSlice(raw), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload []SearchGraphResultPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *Bridge) AggregateHits(req AggregateHitsRequestPayload) ([]SearchAggregationResultPayload, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_aggregate_hits_json(b.handle, toSlice(raw), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload []SearchAggregationResultPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *Bridge) Stats() (*DBStatsPayload, error) {
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_stats_json(b.handle, &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload DBStatsPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (b *Bridge) AddIndex(req AddIndexPayload) error {
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return mapError(C.antfly_db_add_index_json(b.handle, toSlice(raw)))
}

func (b *Bridge) DeleteIndex(name []byte) (bool, error) {
	var deleted C._Bool
	if err := mapError(C.antfly_db_delete_index(b.handle, toSlice(name), &deleted)); err != nil {
		return false, err
	}
	return bool(deleted), nil
}

func (b *Bridge) GetEdges(indexName, key, edgeType []byte, direction uint8) ([]EdgePayload, error) {
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_get_edges_json(b.handle, toSlice(indexName), toSlice(key), toSlice(edgeType), C.uint8_t(direction), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload []EdgePayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *Bridge) TraverseEdges(req TraversalRequestPayload) ([]TraversalResultPayload, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_traverse_edges_json(b.handle, toSlice(raw), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload []TraversalResultPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *Bridge) GetNeighbors(indexName, key, edgeType []byte, direction uint8) ([]TraversalResultPayload, error) {
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_get_neighbors_json(b.handle, toSlice(indexName), toSlice(key), toSlice(edgeType), C.uint8_t(direction), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload []TraversalResultPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *Bridge) FindShortestPath(req ShortestPathRequestPayload) (*PathPayload, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	err = mapError(C.antfly_db_find_shortest_path_json(b.handle, toSlice(raw), &buf))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload PathPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (b *Bridge) FindKShortestPaths(req ShortestPathRequestPayload) ([]PathPayload, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	err = mapError(C.antfly_db_find_k_shortest_paths_json(b.handle, toSlice(raw), &buf))
	if err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload []PathPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *Bridge) MatchPattern(req PatternRequestPayload) ([]PatternMatchPayload, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_match_pattern_json(b.handle, toSlice(raw), &buf)); err != nil {
		return nil, err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	var payload []PatternMatchPayload
	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(buf.ptr), C.int(buf.len)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *Bridge) CreateShadowIndexManager(splitKey, originalRangeEnd []byte) error {
	return mapError(C.antfly_db_create_shadow_index_manager(
		b.handle,
		toSlice(splitKey),
		toSlice(originalRangeEnd),
	))
}

func (b *Bridge) CloseShadowIndexManager() error {
	return mapError(C.antfly_db_close_shadow_index_manager(b.handle))
}

func (b *Bridge) GetShadowIndexDir() (string, error) {
	var buf C.AntflyBuffer
	if err := mapError(C.antfly_db_get_shadow_index_dir(b.handle, &buf)); err != nil {
		return "", err
	}
	defer C.antfly_db_buffer_free(buf.ptr, buf.len)
	if buf.ptr == nil || buf.len == 0 {
		return "", nil
	}
	return C.GoStringN((*C.char)(unsafe.Pointer(buf.ptr)), C.int(buf.len)), nil
}

func (b *Bridge) Split(currStart, currEnd, splitKey []byte, destDir1, destDir2 string, prepareOnly bool) error {
	cDestDir1 := []byte(destDir1)
	cDestDir2 := []byte(destDir2)
	return mapError(C.antfly_db_split(
		b.handle,
		toSlice(currStart),
		toSlice(currEnd),
		toSlice(splitKey),
		toSlice(cDestDir1),
		toSlice(cDestDir2),
		C._Bool(prepareOnly),
	))
}

func (b *Bridge) FinalizeSplit(newStart, newEnd []byte) error {
	return mapError(C.antfly_db_finalize_split(
		b.handle,
		toSlice(newStart),
		toSlice(newEnd),
	))
}

func (b *Bridge) Snapshot(id string) (uint64, error) {
	cID := []byte(id)
	var size C.uint64_t
	if err := mapError(C.antfly_db_snapshot(b.handle, toSlice(cID), &size)); err != nil {
		return 0, err
	}
	return uint64(size), nil
}

func toSlice(b []byte) C.AntflySlice {
	if len(b) == 0 {
		return C.AntflySlice{}
	}
	return C.AntflySlice{
		ptr: (*C.uint8_t)(unsafe.Pointer(&b[0])),
		len: C.size_t(len(b)),
	}
}

func toSliceCopy(b []byte) (C.AntflySlice, unsafe.Pointer) {
	if len(b) == 0 {
		return C.AntflySlice{}, nil
	}
	ptr := C.CBytes(b)
	return C.AntflySlice{
		ptr: (*C.uint8_t)(ptr),
		len: C.size_t(len(b)),
	}, ptr
}

func freeCBytes(ptrs []unsafe.Pointer) {
	for _, ptr := range ptrs {
		C.free(ptr)
	}
}

func DecodeBase64(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(s)
}

func EncodeBase64(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}
