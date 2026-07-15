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

const std = @import("std");
const builtin = @import("builtin");
const query_search = @import("query/search_exec.zig");
const distributed_stats = @import("../../search/distributed_stats.zig");
const planning_adapter = @import("planning_adapter.zig");
const planning_bindings = @import("planning_bindings.zig");
const planning_collectors = @import("planning_collectors.zig");
const planning_stats = @import("planning_stats.zig");
const doc_identity_mod = @import("doc_identity.zig");
const query_graph = @import("query/graph_exec.zig");
const query_result_shape = @import("query/result_shape.zig");
const db_impl = @import("db.zig");
const db_config = @import("config.zig");
const db_internal = @import("internal.zig");
const ha_types = @import("ha_types.zig");

pub const types = @import("types.zig");
pub const docstore = @import("../docstore.zig");
pub const lease = @import("lease.zig");
pub const ownership = @import("ownership.zig");
pub const transaction_resolution = @import("transaction_resolution.zig");
pub const apply_state = @import("derived/apply_state.zig");
pub const embedder = @import("enrichment/embedder.zig");
pub const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
pub const enrichment_catalog = @import("catalog/enrichment_catalog.zig");
pub const enrichment_types = @import("enrichment/enrichment_types.zig");
pub const enrichment_lease = @import("enrichment/enrichment_lease.zig");
pub const enrichment_state = @import("enrichment/enrichment_state.zig");
pub const enrichment_runtime = @import("enrichment/enrichment_runtime.zig");
pub const enrichment_worker = @import("enrichment/enrichment_worker.zig");
pub const chunker = @import("enrichment/chunker.zig");
pub const derived_types = @import("derived/derived_types.zig");
pub const derived_worker = @import("derived/derived_worker.zig");
pub const derived_executor = @import("derived/derived_executor.zig");
pub const replay_stream = @import("derived/replay_stream.zig");
pub const replay_source = @import("derived/replay_source.zig");
pub const runtime_backend = @import("../runtime_backend.zig");
pub const background_runtime = @import("../background_runtime.zig");
pub const async_runtime = @import("derived/async_runtime.zig");
pub const io_threaded_runtime = @import("derived/io_threaded_runtime.zig");
pub const ttl_runtime = @import("maintenance/ttl_runtime.zig");
pub const transaction_runtime = @import("maintenance/transaction_runtime.zig");
pub const graph_metric_runtime = @import("maintenance/graph_metric_runtime.zig");
pub const document_query = @import("document_query.zig");
pub const document_mapper = @import("document_mapper.zig");
pub const document_write = @import("document_write.zig");
pub const relational_store = @import("relational_store.zig");
pub const relational_collation = @import("relational_collation.zig");
pub const DocIdentityNamespace = doc_identity_mod.Namespace;
pub const doc_filter_wire = @import("doc_filter_wire.zig");
pub const artifact_ids = @import("artifact_ids.zig");
pub const internal_keys = @import("../internal_keys.zig");
pub const transform = @import("transform.zig");
pub const aggregations = @import("aggregations.zig");
pub const algebraic = @import("algebraic/mod.zig");
pub const backfill_state = @import("backfill_state.zig");
pub const apply_rw_lock = @import("apply_rw_lock.zig");
pub const ChangeJournal = @import("derived/change_journal.zig").Journal;
pub const DerivedLog = @import("derived/derived_log.zig").DerivedLog;
pub const IndexManager = @import("catalog/index_manager.zig").IndexManager;
pub const TextMemoryAttributionStats = @import("catalog/index_manager.zig").TextMemoryAttributionStats;
pub const DenseSplitHandoff = @import("catalog/index_manager.zig").DenseSplitHandoff;
pub const TextSplitHandoff = @import("catalog/index_manager.zig").TextSplitHandoff;
pub const SparseSplitHandoff = @import("catalog/index_manager.zig").SparseSplitHandoff;
pub const ResolverConfig = @import("catalog/index_manager.zig").ResolverConfig;
pub const resolution_runtime = @import("resolution_runtime.zig");
pub const CandidateSource = resolution_runtime.CandidateSource;
pub const PendingReview = resolution_runtime.PendingReview;
pub const freePendingReviews = resolution_runtime.freePendingReviews;
pub const promotion_runtime = @import("promotion_runtime.zig");
pub const EntitySink = promotion_runtime.EntitySink;
pub const EntityUpsert = promotion_runtime.EntityUpsert;
pub const PromotionOwner = promotion_runtime.PromotionOwner;
pub const DB = db_impl.DB;
pub const DocumentArtifactChildRangeApplyBatch = db_impl.DocumentArtifactChildRangeApplyBatch;
pub const DocumentArtifactChildRangeDispatch = db_impl.DocumentArtifactChildRangeDispatch;
pub const DocumentArtifactChildRangeDispatcher = db_impl.DocumentArtifactChildRangeDispatcher;
pub const OpenOptions = db_impl.OpenOptions;
pub const OpenMode = db_impl.OpenMode;
pub const PrimaryBackend = db_config.PrimaryBackend;
pub const local_schema_json_key = db_impl.local_schema_json_key;
pub const local_lite_sql_table_record_json_key = db_impl.local_lite_sql_table_record_json_key;
pub const HAAsyncEffectMirror = ha_types.AsyncEffectMirror;
pub const HAAsyncBatchMirror = ha_types.AsyncBatchMirror;
pub const HAAsyncMetadataMirror = ha_types.AsyncMetadataMirror;
pub const HAProgressPollFn = ha_types.ProgressPollFn;
pub const HAPrimaryProgressSyncWait = ha_types.PrimaryProgressSyncWait;
pub const HAWriteGate = ha_types.WriteGate;
pub const HAReplicationRecordView = @import("../ha/replication_record.zig").RecordView;
pub const ReplayProgress = db_internal.ReplayProgress;
pub const QueryVisibilityHook = db_internal.QueryVisibilityHook(DB);
pub const QueryVisibilityChange = db_internal.QueryVisibilityChange;
pub const DerivedReplayDebtStatus = db_impl.DerivedReplayDebtStatus;
pub const BatchProfile = db_impl.BatchProfile;
pub const RuntimePreflight = query_search.RuntimePreflight;
pub const RuntimePreflightSummary = query_search.RuntimePreflightSummary;
pub const SortRejectionDiagnostic = query_search.SortRejectionDiagnostic;
pub const PlanningStatsSummary = planning_stats.PlanningStatsSummary;
pub const PlanningStatsProvider = planning_stats.PlanningStatsProvider;
pub const PlanningStatsCollector = planning_stats.PlanningStatsCollector;
pub const planningAdapter = planning_adapter;
pub const validatePlanningBindings = planning_bindings.validateSearchRequestBindings;
pub const planningCollectors = planning_collectors;
pub const query_metrics = @import("query_metrics.zig");
pub const TextIndexEstimate = query_search.TextIndexEstimate;
pub const EmbeddingIndexEstimate = query_search.EmbeddingIndexEstimate;
pub const GraphIndexEstimate = query_search.GraphIndexEstimate;
pub const TextFieldStats = distributed_stats.TextFieldStats;
pub const TermDocFreq = distributed_stats.TermDocFreq;

pub fn preflightRuntimeAlloc(alloc: std.mem.Allocator, runtime: RuntimePreflight) !RuntimePreflightSummary {
    return try query_search.preflightRuntimeAlloc(alloc, runtime);
}

pub fn preflightSearchRequestAlloc(alloc: std.mem.Allocator, req: types.SearchRequest) !RuntimePreflightSummary {
    return try query_search.preflightSearchRequestAlloc(alloc, req);
}

pub fn deriveRuntimePreflightEstimates(summary: *RuntimePreflightSummary) void {
    query_search.deriveEstimateFields(summary);
}

pub fn searchRequestHasScoreBearingTextSource(req: types.SearchRequest) bool {
    return query_search.searchRequestHasScoreBearingTextSource(req);
}

pub fn searchRequestHasScoreBearingVectorSource(req: types.SearchRequest) bool {
    return query_search.searchRequestHasScoreBearingVectorSource(req);
}

pub fn searchRequestHasScoreBearingSource(req: types.SearchRequest) bool {
    return query_search.searchRequestHasScoreBearingSource(req);
}

pub fn requestHasVectorScoreOrderOnly(req: types.SearchRequest) bool {
    return query_search.requestHasVectorScoreOrderOnly(req);
}

pub fn resetLastSortRejectionDiagnostic() void {
    query_search.resetLastSortRejectionDiagnostic();
}

pub fn takeLastSortRejectionDiagnostic() ?SortRejectionDiagnostic {
    return query_search.takeLastSortRejectionDiagnostic();
}

pub fn peekLastSortRejectionDiagnostic() ?SortRejectionDiagnostic {
    return query_search.peekLastSortRejectionDiagnostic();
}

pub fn recordSortRejectionDiagnostic(field: []const u8, reason: []const u8, detail: []const u8) void {
    query_search.recordSortRejectionDiagnostic(field, reason, detail);
}

pub const testing = if (builtin.is_test) struct {
    pub fn recordSortRejectionDiagnostic(field: []const u8, reason: []const u8, detail: []const u8) void {
        query_search.recordSortRejectionDiagnosticForTesting(field, reason, detail);
    }
} else struct {};

test {
    _ = types;
    _ = docstore;
    _ = lease;
    _ = ownership;
    _ = transaction_resolution;
    _ = apply_state;
    _ = embedder;
    _ = enrichment_artifact_codec;
    _ = enrichment_catalog;
    _ = enrichment_types;
    _ = enrichment_lease;
    _ = enrichment_state;
    _ = enrichment_runtime;
    _ = enrichment_worker;
    _ = chunker;
    _ = derived_types;
    _ = derived_worker;
    _ = derived_executor;
    _ = replay_stream;
    _ = replay_source;
    _ = runtime_backend;
    _ = background_runtime;
    _ = async_runtime;
    _ = io_threaded_runtime;
    _ = ttl_runtime;
    _ = transaction_runtime;
    _ = graph_metric_runtime;
    _ = document_query;
    _ = document_mapper;
    _ = relational_store;
    _ = DocIdentityNamespace;
    _ = doc_filter_wire;
    _ = artifact_ids;
    _ = internal_keys;
    _ = transform;
    _ = aggregations;
    _ = algebraic;
    _ = backfill_state;
    _ = apply_rw_lock;
    _ = ChangeJournal;
    _ = DerivedLog;
    _ = IndexManager;
    _ = DenseSplitHandoff;
    _ = TextSplitHandoff;
    _ = SparseSplitHandoff;
    _ = DB;
    _ = OpenOptions;
    _ = OpenMode;
    _ = QueryVisibilityHook;
    _ = DerivedReplayDebtStatus;
    _ = BatchProfile;
    _ = RuntimePreflight;
    _ = RuntimePreflightSummary;
    _ = SortRejectionDiagnostic;
    _ = PlanningStatsSummary;
    _ = PlanningStatsProvider;
    _ = PlanningStatsCollector;
    _ = planningAdapter;
    _ = validatePlanningBindings;
    _ = planningCollectors;
    _ = query_metrics;
    _ = query_graph;
    _ = query_result_shape;
    _ = TextIndexEstimate;
    _ = EmbeddingIndexEstimate;
    _ = GraphIndexEstimate;
    _ = TextFieldStats;
    _ = TermDocFreq;
}
