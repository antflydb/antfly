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
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const common_secrets = @import("../../../common/secrets.zig");
const backend_erased = @import("../../backend_erased.zig");
const backend_scan = @import("../../backend_scan.zig");
const mem_backend = @import("../../mem_backend.zig");
const internal_keys = @import("../../internal_keys.zig");
const resource_manager_mod = @import("../../resource_manager.zig");
const change_journal_mod = @import("../derived/change_journal.zig");
const replay_source_mod = @import("../derived/replay_source.zig");
const derived_types = @import("../derived/derived_types.zig");
const enrichment_types = @import("enrichment_types.zig");
const enrichment_artifact_codec = @import("artifact_codec.zig");
const enrichment_worker = @import("enrichment_worker.zig");
const enrichment_lease = @import("enrichment_lease.zig");
const enrichment_state = @import("enrichment_state.zig");
const embedder_mod = @import("embedder.zig");
const asset_producer_mod = @import("asset_producer.zig");
const document_extraction_mod = @import("document_extraction.zig");
const artifact_ids = @import("../artifact_ids.zig");
const chunker_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("chunker_stub.zig")
else
    @import("chunker.zig");
const chunk_artifact_mod = @import("../../../chunking/chunk.zig");
const chunking_types_mod = @import("../../../chunking/types.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const apply_rw_lock_mod = @import("../apply_rw_lock.zig");
const ownership_mod = @import("../ownership.zig");
const types = @import("../types.zig");
const platform_clock = @import("antfly_platform").clock;
const background_runtime_mod = @import("../../background_runtime.zig");
const template = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("../template_stub.zig")
else
    @import("../../../template.zig");
const template_remote = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("../template_remote_stub.zig")
else
    @import("../../../template_remote.zig");
const scraping = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps)
    @import("../scraping_stub.zig")
else
    @import("antfly_scraping");
const mapper = @import("../document_mapper.zig");

fn getenv(name: [*:0]const u8) ?[]const u8 {
    return platform.env.getenv(name);
}

pub const Config = struct {
    owner_id: []const u8 = "local",
    lease_ttl_ms: u64 = 30_000,
    dense_embedder: ?embedder_mod.DenseEmbedder = null,
    sparse_embedder: ?embedder_mod.SparseEmbedder = null,
    asset_producer: ?asset_producer_mod.Producer = null,
    enable_without_producers: bool = false,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
};

pub const RuntimeError = error{ EnrichmentWorkerFailed, EnrichmentRetryInProgress };

pub const GeneratedRecordWriter = *const fn (ptr: *anyopaque, batch: derived_types.DerivedBatch, artifact_delete_keys: []const []const u8) anyerror!u64;
pub const NotifyFn = *const fn (ptr: *anyopaque, sequence: u64) void;
pub const StatusHook = struct {
    ptr: *anyopaque,
    on_change: *const fn (ptr: *anyopaque) void,

    pub fn notify(self: @This()) void {
        self.on_change(self.ptr);
    }
};

pub const scope_name = "generated";
const writer_locked_retry_count: usize = 1000;
const writer_locked_retry_sleep_ns: u64 = 100_000;
const generated_replay_default_window_items: usize = 2048;
const generated_embed_default_batch_items: usize = 8;
const generated_embed_default_batch_bytes: usize = 256 * 1024;
const generated_ocr_default_batch_items: usize = 4;
const generated_ocr_default_batch_max_items: usize = 8;
const generated_ocr_default_batch_bytes: usize = 64 * 1024 * 1024;
const transient_embed_retry_max_attempts: u32 = 6;
const transient_embed_retry_base_sleep_ns: u64 = 250 * std.time.ns_per_ms;
const transient_embed_retry_max_sleep_ns: u64 = 5 * std.time.ns_per_s;
const transient_worker_retry_sleep_ns: u64 = 100 * std.time.ns_per_ms;
const lease_denied_retry_sleep_ns: u64 = 100 * std.time.ns_per_ms;

const CoverageOutcome = enum { produced, skipped, terminal_failed };
const coverage_outcome_count = std.meta.fields(CoverageOutcome).len;

const CoverageOutcomeTransition = struct {
    index_name: []u8,
    generation: u64,
    outcome: CoverageOutcome,
    marker_key: []u8,
    counter_keys: [coverage_outcome_count][]u8,
};

const GeneratedReplayWindow = struct {
    alloc: Allocator,
    documents: std.ArrayListUnmanaged(derived_types.DerivedDocument) = .empty,
    deleted_keys: std.ArrayListUnmanaged([]u8) = .empty,
    artifact_delete_keys: std.ArrayListUnmanaged([]u8) = .empty,
    changed_artifact_keys: std.ArrayListUnmanaged([]u8) = .empty,
    dense_embeddings: std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite) = .empty,
    sparse_embeddings: std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite) = .empty,
    coverage_transitions: std.ArrayListUnmanaged(CoverageOutcomeTransition) = .empty,
    coverage_transition_keys: std.StringHashMapUnmanaged(void) = .empty,

    fn hasDerivedItems(self: *const @This()) bool {
        return self.documents.items.len != 0 or
            self.deleted_keys.items.len != 0 or
            self.artifact_delete_keys.items.len != 0 or
            self.changed_artifact_keys.items.len != 0 or
            self.dense_embeddings.items.len != 0 or
            self.sparse_embeddings.items.len != 0;
    }

    fn isEmpty(self: *const @This()) bool {
        return !self.hasDerivedItems() and self.coverage_transitions.items.len == 0;
    }

    fn itemCount(self: *const @This()) usize {
        return self.documents.items.len +
            self.deleted_keys.items.len +
            self.artifact_delete_keys.items.len +
            self.changed_artifact_keys.items.len +
            self.dense_embeddings.items.len +
            self.sparse_embeddings.items.len +
            self.coverage_transitions.items.len;
    }

    fn toOwnedBatch(self: *@This()) !derived_types.DerivedBatch {
        var batch = derived_types.DerivedBatch{
            .documents = try self.documents.toOwnedSlice(self.alloc),
        };
        errdefer derived_types.deinitDerivedBatch(self.alloc, &batch);
        batch.deleted_keys = try self.deleted_keys.toOwnedSlice(self.alloc);
        batch.changed_artifact_keys = try self.changed_artifact_keys.toOwnedSlice(self.alloc);
        batch.dense_embeddings = try self.dense_embeddings.toOwnedSlice(self.alloc);
        batch.sparse_embeddings = try self.sparse_embeddings.toOwnedSlice(self.alloc);
        return batch;
    }

    fn deinit(self: *@This()) void {
        for (self.documents.items) |doc| {
            self.alloc.free(@constCast(doc.key));
            if (doc.cleaned_value) |value| self.alloc.free(@constCast(value));
            for (doc.targets) |target| self.alloc.free(@constCast(target.index_name));
            if (doc.targets.len > 0) self.alloc.free(@constCast(doc.targets));
        }
        self.documents.deinit(self.alloc);

        for (self.deleted_keys.items) |key| self.alloc.free(key);
        self.deleted_keys.deinit(self.alloc);

        for (self.artifact_delete_keys.items) |key| self.alloc.free(key);
        self.artifact_delete_keys.deinit(self.alloc);

        for (self.changed_artifact_keys.items) |key| self.alloc.free(key);
        self.changed_artifact_keys.deinit(self.alloc);

        for (self.dense_embeddings.items) |embedding| freeDerivedDenseEmbedding(self.alloc, embedding);
        self.dense_embeddings.deinit(self.alloc);

        for (self.sparse_embeddings.items) |embedding| {
            self.alloc.free(@constCast(embedding.index_name));
            self.alloc.free(@constCast(embedding.doc_key));
            if (embedding.artifact_key) |key| self.alloc.free(@constCast(key));
            self.alloc.free(@constCast(embedding.indices));
            self.alloc.free(@constCast(embedding.values));
        }
        self.sparse_embeddings.deinit(self.alloc);

        clearQueuedCoverageTransitions(self.alloc, &self.coverage_transitions, &self.coverage_transition_keys);
        self.coverage_transitions.deinit(self.alloc);
        self.coverage_transition_keys.deinit(self.alloc);
    }
};

fn generatedReplayWindowItems() usize {
    if (comptime builtin.os.tag == .freestanding) return generated_replay_default_window_items;
    const raw = getenv("ANTFLY_ENRICHMENT_WINDOW_ITEMS") orelse return generated_replay_default_window_items;
    if (raw.len == 0) return generated_replay_default_window_items;
    const parsed = std.fmt.parseUnsigned(usize, raw, 10) catch return generated_replay_default_window_items;
    return @max(@as(usize, 1), parsed);
}

fn generatedEmbedBatchItems() usize {
    if (comptime builtin.os.tag == .freestanding) return generated_embed_default_batch_items;
    const raw = getenv("ANTFLY_ENRICHMENT_EMBED_BATCH_ITEMS") orelse return generated_embed_default_batch_items;
    if (raw.len == 0) return generated_embed_default_batch_items;
    const parsed = std.fmt.parseUnsigned(usize, raw, 10) catch return generated_embed_default_batch_items;
    return @max(@as(usize, 1), parsed);
}

fn generatedEmbedBatchBytes() usize {
    if (comptime builtin.os.tag == .freestanding) return generated_embed_default_batch_bytes;
    const raw = getenv("ANTFLY_ENRICHMENT_EMBED_BATCH_BYTES") orelse return generated_embed_default_batch_bytes;
    if (raw.len == 0) return generated_embed_default_batch_bytes;
    const parsed = std.fmt.parseUnsigned(usize, raw, 10) catch return generated_embed_default_batch_bytes;
    return @max(@as(usize, 1), parsed);
}

fn generatedOcrBatchItems() usize {
    if (comptime builtin.os.tag == .freestanding) return generated_ocr_default_batch_items;
    const raw = getenv("ANTFLY_ENRICHMENT_OCR_BATCH_ITEMS") orelse return generated_ocr_default_batch_items;
    if (raw.len == 0) return generated_ocr_default_batch_items;
    const parsed = std.fmt.parseUnsigned(usize, raw, 10) catch return generated_ocr_default_batch_items;
    return @max(@as(usize, 1), parsed);
}

fn generatedOcrBatchMaxItems() usize {
    if (comptime builtin.os.tag == .freestanding) return generated_ocr_default_batch_max_items;
    const raw = getenv("ANTFLY_ENRICHMENT_OCR_BATCH_MAX_ITEMS") orelse return generated_ocr_default_batch_max_items;
    if (raw.len == 0) return generated_ocr_default_batch_max_items;
    const parsed = std.fmt.parseUnsigned(usize, raw, 10) catch return generated_ocr_default_batch_max_items;
    return @max(@as(usize, 1), parsed);
}

fn generatedOcrBatchBytes() usize {
    if (comptime builtin.os.tag == .freestanding) return generated_ocr_default_batch_bytes;
    const raw = getenv("ANTFLY_ENRICHMENT_OCR_BATCH_BYTES") orelse return generated_ocr_default_batch_bytes;
    if (raw.len == 0) return generated_ocr_default_batch_bytes;
    const parsed = std.fmt.parseUnsigned(usize, raw, 10) catch return generated_ocr_default_batch_bytes;
    return @max(@as(usize, 1), parsed);
}

fn requestEmbedBatchItems(alloc: Allocator, request: enrichment_types.GeneratedEnrichmentRequest) usize {
    return enrichment_types.executionBatchItemsOrDefault(alloc, request.execution_json, generatedEmbedBatchItems());
}

fn requestEmbedBatchBytes(alloc: Allocator, request: enrichment_types.GeneratedEnrichmentRequest) usize {
    return enrichment_types.executionBatchBytesOrDefault(alloc, request.execution_json, generatedEmbedBatchBytes());
}

const GeneratedTextBatchPolicy = struct {
    max_items: usize,
    max_bytes: usize,
};

fn requestGeneratedTextBatchPolicy(alloc: Allocator, request: enrichment_types.GeneratedEnrichmentRequest) GeneratedTextBatchPolicy {
    const operator_max_items = generatedOcrBatchMaxItems();
    const requested_items = enrichment_types.executionBatchItemsOrDefault(alloc, request.execution_json, generatedOcrBatchItems());
    return .{
        .max_items = @max(@as(usize, 1), @min(requested_items, operator_max_items)),
        .max_bytes = enrichment_types.executionBatchBytesOrDefault(alloc, request.execution_json, generatedOcrBatchBytes()),
    };
}

fn backoffWriterLockRetry() void {
    if (comptime builtin.os.tag == .freestanding) return;
    std.Thread.yield() catch {};
    if (@hasDecl(std.Thread, "sleep")) {
        std.Thread.sleep(writer_locked_retry_sleep_ns);
    }
}

fn sleepRetryBackoff(sleep_ns: u64) void {
    if (comptime builtin.os.tag == .freestanding) return;
    std.Thread.yield() catch {};
    if (@hasDecl(std.Thread, "sleep")) {
        std.Thread.sleep(sleep_ns);
    }
}

fn transientEmbedRetrySleepNs(attempt: u32) u64 {
    const shift = @min(attempt, 5);
    return @min(transient_embed_retry_base_sleep_ns << @intCast(shift), transient_embed_retry_max_sleep_ns);
}

const query_yield_poll_ns: u64 = 25 * std.time.ns_per_ms;
const query_yield_max_ns: u64 = 5 * std.time.ns_per_s;

// yieldToInteractiveEmbeds briefly defers the next embed batch while a
// query-time embed is in flight, so interactive embeds aren't starved by
// backfill. The flag covers only the embed call itself (milliseconds), so
// the wait normally clears on the first poll; the 5s cap is a safety bound —
// backfill must always make progress.
fn yieldToInteractiveEmbeds(runtime: *EnrichmentRuntime) void {
    if (comptime builtin.os.tag == .freestanding) return;
    if (enrichment_types.interactive_embed_inflight.load(.monotonic) == 0) return;
    const start_ns = runtime.config.clock.nowRealtimeNs();
    while (enrichment_types.interactive_embed_inflight.load(.monotonic) > 0) {
        if (elapsedNsSince(runtime, start_ns) >= query_yield_max_ns) return;
        if (runtimeShuttingDown(runtime)) return;
        sleepRetryBackoff(query_yield_poll_ns);
    }
}

fn runtimeShuttingDown(runtime: *EnrichmentRuntime) bool {
    if (comptime builtin.os.tag == .freestanding) return false;
    const io_impl = runtime.io_impl orelse return runtime.shutdown;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    const shutdown = runtime.shutdown;
    runtime.mutex.unlock(io);
    return shutdown;
}

fn elapsedNsSince(runtime: *EnrichmentRuntime, start_ns: u64) u64 {
    const end_ns = runtime.config.clock.nowRealtimeNs();
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}

fn noteEmbedBatchStarted(runtime: *EnrichmentRuntime, items: usize, bytes: usize, max_bytes: usize) void {
    const now_ms = runtime.config.clock.nowRealtimeMs();
    if (comptime builtin.os.tag == .freestanding) {
        runtime.embed_batches_started += 1;
        runtime.embed_items_started += @intCast(items);
        runtime.active_embed_batch_items = @intCast(items);
        runtime.active_embed_batch_bytes = @intCast(bytes);
        runtime.active_embed_batch_max_bytes = @intCast(max_bytes);
        runtime.active_embed_batch_started_ms = now_ms;
        return;
    }

    if (runtime.io_impl) |io_impl| {
        const io = io_impl.io();
        runtime.mutex.lockUncancelable(io);
        defer runtime.mutex.unlock(io);
        runtime.embed_batches_started += 1;
        runtime.embed_items_started += @intCast(items);
        runtime.active_embed_batch_items = @intCast(items);
        runtime.active_embed_batch_bytes = @intCast(bytes);
        runtime.active_embed_batch_max_bytes = @intCast(max_bytes);
        runtime.active_embed_batch_started_ms = now_ms;
    } else {
        runtime.embed_batches_started += 1;
        runtime.embed_items_started += @intCast(items);
        runtime.active_embed_batch_items = @intCast(items);
        runtime.active_embed_batch_bytes = @intCast(bytes);
        runtime.active_embed_batch_max_bytes = @intCast(max_bytes);
        runtime.active_embed_batch_started_ms = now_ms;
    }
}

fn noteEmbedBatchFinished(runtime: *EnrichmentRuntime, items: usize, bytes: usize, max_bytes: usize, elapsed_ns: u64, success: bool) void {
    if (comptime builtin.os.tag == .freestanding) {
        if (success) {
            runtime.embed_batches_completed += 1;
            runtime.embed_items_completed += @intCast(items);
            runtime.last_embed_batch_items = @intCast(items);
            runtime.last_embed_batch_bytes = @intCast(bytes);
            runtime.last_embed_batch_max_bytes = @intCast(max_bytes);
            runtime.last_embed_batch_completed_ms = @max(runtime.last_embed_batch_completed_ms, runtime.config.clock.nowRealtimeMs());
            runtime.last_embed_batch_ns = elapsed_ns;
            runtime.total_embed_ns += elapsed_ns;
        }
        runtime.active_embed_batch_items = 0;
        runtime.active_embed_batch_bytes = 0;
        runtime.active_embed_batch_max_bytes = 0;
        runtime.active_embed_batch_started_ms = 0;
        return;
    }

    if (runtime.io_impl) |io_impl| {
        const io = io_impl.io();
        runtime.mutex.lockUncancelable(io);
        defer runtime.mutex.unlock(io);
        if (success) {
            runtime.embed_batches_completed += 1;
            runtime.embed_items_completed += @intCast(items);
            runtime.last_embed_batch_items = @intCast(items);
            runtime.last_embed_batch_bytes = @intCast(bytes);
            runtime.last_embed_batch_max_bytes = @intCast(max_bytes);
            runtime.last_embed_batch_completed_ms = @max(runtime.last_embed_batch_completed_ms, runtime.config.clock.nowRealtimeMs());
            runtime.last_embed_batch_ns = elapsed_ns;
            runtime.total_embed_ns += elapsed_ns;
        }
        runtime.active_embed_batch_items = 0;
        runtime.active_embed_batch_bytes = 0;
        runtime.active_embed_batch_max_bytes = 0;
        runtime.active_embed_batch_started_ms = 0;
    } else {
        if (success) {
            runtime.embed_batches_completed += 1;
            runtime.embed_items_completed += @intCast(items);
            runtime.last_embed_batch_items = @intCast(items);
            runtime.last_embed_batch_bytes = @intCast(bytes);
            runtime.last_embed_batch_max_bytes = @intCast(max_bytes);
            runtime.last_embed_batch_completed_ms = @max(runtime.last_embed_batch_completed_ms, runtime.config.clock.nowRealtimeMs());
            runtime.last_embed_batch_ns = elapsed_ns;
            runtime.total_embed_ns += elapsed_ns;
        }
        runtime.active_embed_batch_items = 0;
        runtime.active_embed_batch_bytes = 0;
        runtime.active_embed_batch_max_bytes = 0;
        runtime.active_embed_batch_started_ms = 0;
    }
}

const TextBatchByteStats = struct {
    total_bytes: usize = 0,
    max_bytes: usize = 0,
};

fn textBatchByteStats(texts: []const []const u8) TextBatchByteStats {
    var stats = TextBatchByteStats{};
    for (texts) |text| {
        stats.total_bytes += text.len;
        stats.max_bytes = @max(stats.max_bytes, text.len);
    }
    return stats;
}

fn boundedTextBatchEnd(texts: []const []const u8, start: usize, max_items: usize, max_bytes: usize) usize {
    var end = start;
    var bytes: usize = 0;
    while (end < texts.len and end - start < max_items) : (end += 1) {
        const next_bytes = bytes + texts[end].len;
        if (end > start and next_bytes > max_bytes) break;
        bytes = next_bytes;
    }
    return if (end == start) start + 1 else end;
}

const TransientEmbedRetryDecision = enum {
    retry_inline,
    yield_to_worker,
    abort_shutdown,
};

fn transientEmbedRetryDecision(runtime: *EnrichmentRuntime, attempt: u32) TransientEmbedRetryDecision {
    if (comptime builtin.os.tag != .freestanding) {
        if (runtimeShuttingDown(runtime)) return .abort_shutdown;
    }
    if (attempt + 1 >= transient_embed_retry_max_attempts) return .yield_to_worker;
    return .retry_inline;
}

fn isRetryableEnrichmentError(err: anyerror) bool {
    return switch (err) {
        error.EmbedRateLimited,
        error.EmbedTransientFailure,
        error.ModelNotFound,
        error.ConnectionRefused,
        error.ConnectionResetByPeer,
        error.ConnectionTimedOut,
        error.Timeout,
        error.NetworkUnreachable,
        error.HostLacksNetworkAddresses,
        error.TemporaryNameServerFailure,
        error.NameServerFailure,
        error.UnexpectedReadFailure,
        error.SendFailed,
        error.RecvFailed,
        error.ResourceBudgetExceeded,
        error.GenerateBatchTransientFailure,
        => true,
        else => false,
    };
}

fn isEnrichmentControlError(err: anyerror) bool {
    return err == error.EnrichmentRetryAborted;
}

test "enrichment treats missing local model as retryable" {
    try std.testing.expect(isRetryableEnrichmentError(error.ModelNotFound));
}

fn noteTransientEmbedRetry(runtime: *EnrichmentRuntime, err: anyerror) void {
    if (builtin.os.tag == .freestanding) {
        runtime.error_count += 1;
        runtime.retryable_error_count += 1;
        runtime.retrying = true;
        runtime.worker_failed = false;
        return;
    }

    if (runtime.io_impl) |io_impl| {
        runtime.recordRetryableError(io_impl.io(), err);
    } else {
        runtime.error_count += 1;
        runtime.retryable_error_count += 1;
        runtime.retrying = true;
        runtime.worker_failed = false;
    }
}

fn runtimeStatusSnapshot(runtime: *EnrichmentRuntime) enrichment_state.RuntimeStatus {
    return .{
        .target_sequence = runtime.target_sequence,
        .error_count = runtime.error_count,
        .retryable_error_count = runtime.retryable_error_count,
        .fatal_error_count = runtime.fatal_error_count,
        .skipped_source_count = runtime.skipped_source_count,
        .retrying = runtime.retrying,
        .worker_failed = runtime.worker_failed,
    };
}

fn runtimeProjectionStatus(retrying: bool, worker_failed: bool) enrichment_state.ProjectionStatus {
    if (worker_failed) return .repair_required;
    if (retrying) return .degraded;
    return .clean;
}

fn restorePersistedRuntimeStatus(runtime: anytype, persisted_status: enrichment_state.RuntimeStatus) void {
    runtime.error_count = persisted_status.error_count;
    runtime.retryable_error_count = persisted_status.retryable_error_count;
    runtime.fatal_error_count = persisted_status.fatal_error_count;
    runtime.skipped_source_count = persisted_status.skipped_source_count;
    runtime.retrying = persisted_status.retrying and !persisted_status.worker_failed;
    runtime.worker_failed = persisted_status.worker_failed;
    runtime.target_sequence = @max(runtime.applied_sequence, persisted_status.target_sequence);
}

test "enrichment runtime restore preserves retry target across restart" {
    var runtime = struct {
        applied_sequence: u64 = 3,
        target_sequence: u64 = 0,
        error_count: u64 = 0,
        retryable_error_count: u64 = 0,
        fatal_error_count: u64 = 0,
        skipped_source_count: u64 = 0,
        retrying: bool = false,
        worker_failed: bool = false,
    }{};

    restorePersistedRuntimeStatus(&runtime, .{
        .target_sequence = 9,
        .error_count = 2,
        .retryable_error_count = 2,
        .fatal_error_count = 0,
        .retrying = true,
        .worker_failed = false,
    });

    try std.testing.expectEqual(@as(u64, 9), runtime.target_sequence);
    try std.testing.expectEqual(@as(u64, 2), runtime.error_count);
    try std.testing.expectEqual(@as(u64, 2), runtime.retryable_error_count);
    try std.testing.expect(runtime.retrying);
    try std.testing.expect(!runtime.worker_failed);
}

test "enrichment runtime restore does not resume persisted fatal failure" {
    var runtime = struct {
        applied_sequence: u64 = 7,
        target_sequence: u64 = 0,
        error_count: u64 = 0,
        retryable_error_count: u64 = 0,
        fatal_error_count: u64 = 0,
        skipped_source_count: u64 = 0,
        retrying: bool = false,
        worker_failed: bool = false,
    }{};

    restorePersistedRuntimeStatus(&runtime, .{
        .target_sequence = 4,
        .error_count = 1,
        .fatal_error_count = 1,
        .retrying = true,
        .worker_failed = true,
    });

    try std.testing.expectEqual(@as(u64, 7), runtime.target_sequence);
    try std.testing.expect(!runtime.retrying);
    try std.testing.expect(runtime.worker_failed);
}

fn clearPublishedGeneratedArtifacts(runtime: *EnrichmentRuntime) void {
    var it = runtime.published_generated_artifacts.iterator();
    while (it.next()) |entry| runtime.alloc.free(@constCast(entry.key_ptr.*));
    runtime.published_generated_artifacts.clearAndFree(runtime.alloc);
}

fn clearIsolatedFailedIndexes(runtime: *EnrichmentRuntime) void {
    var it = runtime.isolated_failed_indexes.iterator();
    while (it.next()) |entry| runtime.alloc.free(@constCast(entry.key_ptr.*));
    runtime.isolated_failed_indexes.clearAndFree(runtime.alloc);
}

fn markIsolatedFailedIndex(runtime: *EnrichmentRuntime, index_name: []const u8) void {
    if (runtime.isolated_failed_indexes.contains(index_name)) return;
    const owned_key = runtime.alloc.dupe(u8, index_name) catch return;
    errdefer runtime.alloc.free(owned_key);
    runtime.isolated_failed_indexes.put(runtime.alloc, owned_key, {}) catch return;
}

fn generatedArtifactAlreadyPublished(runtime: *EnrichmentRuntime, artifact_key: []const u8) bool {
    return runtime.published_generated_artifacts.contains(artifact_key);
}

fn rememberPublishedGeneratedArtifact(runtime: *EnrichmentRuntime, artifact_key: []const u8) !void {
    if (runtime.published_generated_artifacts.contains(artifact_key)) return;
    const owned_key = try runtime.alloc.dupe(u8, artifact_key);
    errdefer runtime.alloc.free(owned_key);
    try runtime.published_generated_artifacts.put(runtime.alloc, owned_key, {});
}

fn rememberPublishedGeneratedBatch(runtime: *EnrichmentRuntime, batch: derived_types.DerivedBatch) !void {
    for (batch.dense_embeddings) |embedding| {
        if (embedding.artifact_key) |artifact_key| try rememberPublishedGeneratedArtifact(runtime, artifact_key);
    }
    for (batch.sparse_embeddings) |embedding| {
        if (embedding.artifact_key) |artifact_key| try rememberPublishedGeneratedArtifact(runtime, artifact_key);
    }
}

fn embedDenseWithRetry(
    dense_embedder: embedder_mod.DenseEmbedder,
    runtime: *EnrichmentRuntime,
    embedding_name: []const u8,
    text: []const u8,
    dims: u32,
) ![]f32 {
    var attempt: u32 = 0;
    while (true) : (attempt += 1) {
        const vector = dense_embedder.embedDense(runtime.alloc, embedding_name, text, dims) catch |err| {
            if (!isRetryableEnrichmentError(err)) return err;
            switch (transientEmbedRetryDecision(runtime, attempt)) {
                .retry_inline => {},
                .yield_to_worker => return err,
                .abort_shutdown => return error.EnrichmentRetryAborted,
            }
            if (attempt == 0) noteTransientEmbedRetry(runtime, err);
            sleepRetryBackoff(transientEmbedRetrySleepNs(attempt));
            continue;
        };
        return vector;
    }
}

fn embedDenseBatchWithRetry(
    dense_embedder: embedder_mod.DenseEmbedder,
    runtime: *EnrichmentRuntime,
    embedding_name: []const u8,
    texts: []const []const u8,
    dims: u32,
) ![]const []const f32 {
    var attempt: u32 = 0;
    while (true) : (attempt += 1) {
        const vectors = dense_embedder.embedDenseBatch(runtime.alloc, embedding_name, texts, dims) catch |err| {
            if (!isRetryableEnrichmentError(err)) return err;
            switch (transientEmbedRetryDecision(runtime, attempt)) {
                .retry_inline => {},
                .yield_to_worker => return err,
                .abort_shutdown => return error.EnrichmentRetryAborted,
            }
            if (attempt == 0) noteTransientEmbedRetry(runtime, err);
            sleepRetryBackoff(transientEmbedRetrySleepNs(attempt));
            continue;
        };
        return vectors;
    }
}

fn embedDensePartsWithRetry(
    dense_embedder: embedder_mod.DenseEmbedder,
    runtime: *EnrichmentRuntime,
    embedding_name: []const u8,
    parts: []const template.ContentPart,
    dims: u32,
) ![]f32 {
    var attempt: u32 = 0;
    while (true) : (attempt += 1) {
        const vector = dense_embedder.embedDenseParts(runtime.alloc, embedding_name, parts, dims) catch |err| {
            if (!isRetryableEnrichmentError(err)) return err;
            switch (transientEmbedRetryDecision(runtime, attempt)) {
                .retry_inline => {},
                .yield_to_worker => return err,
                .abort_shutdown => return error.EnrichmentRetryAborted,
            }
            if (attempt == 0) noteTransientEmbedRetry(runtime, err);
            sleepRetryBackoff(transientEmbedRetrySleepNs(attempt));
            continue;
        };
        return vector;
    }
}

fn embedSparseWithRetry(
    sparse_embedder: embedder_mod.SparseEmbedder,
    runtime: *EnrichmentRuntime,
    embedding_name: []const u8,
    text: []const u8,
) !embedder_mod.SparseEmbedding {
    var attempt: u32 = 0;
    while (true) : (attempt += 1) {
        const sparse = sparse_embedder.embedSparse(runtime.alloc, embedding_name, text) catch |err| {
            if (!isRetryableEnrichmentError(err)) return err;
            switch (transientEmbedRetryDecision(runtime, attempt)) {
                .retry_inline => {},
                .yield_to_worker => return err,
                .abort_shutdown => return error.EnrichmentRetryAborted,
            }
            if (attempt == 0) noteTransientEmbedRetry(runtime, err);
            sleepRetryBackoff(transientEmbedRetrySleepNs(attempt));
            continue;
        };
        return sparse;
    }
}

fn embedSparseBatchWithRetry(
    sparse_embedder: embedder_mod.SparseEmbedder,
    runtime: *EnrichmentRuntime,
    embedding_name: []const u8,
    texts: []const []const u8,
) ![]embedder_mod.SparseEmbedding {
    var attempt: u32 = 0;
    while (true) : (attempt += 1) {
        const sparse_batch = sparse_embedder.embedSparseBatch(runtime.alloc, embedding_name, texts) catch |err| {
            if (!isRetryableEnrichmentError(err)) return err;
            switch (transientEmbedRetryDecision(runtime, attempt)) {
                .retry_inline => {},
                .yield_to_worker => return err,
                .abort_shutdown => return error.EnrichmentRetryAborted,
            }
            if (attempt == 0) noteTransientEmbedRetry(runtime, err);
            sleepRetryBackoff(transientEmbedRetrySleepNs(attempt));
            continue;
        };
        return sparse_batch;
    }
}

fn shouldStoreChunkArtifacts(
    alloc: Allocator,
    request: enrichment_types.GeneratedEnrichmentRequest,
    has_durable_text_consumer: bool,
) !bool {
    if (request.full_text_index) return true;
    if (has_durable_text_consumer) return true;
    if (request.chunker_json.len == 0) return true;
    if (try chunking_types_mod.parseHasFullTextIndexFromSlice(alloc, request.chunker_json)) return true;
    return try chunking_types_mod.parseStoreChunksFromSlice(alloc, request.chunker_json);
}

const WorkerChunkCacheEntry = struct {
    key: []u8,
    chunks: []chunker_mod.Chunk,
};

const RequestPlanCacheEntry = struct {
    doc_key: []u8,
    requests: []const enrichment_types.GeneratedEnrichmentRequest,
};

const ChunkedDenseWindowItem = struct {
    request: enrichment_types.GeneratedEnrichmentRequest,
    parent_doc_key: []const u8,
    source_field: []const u8,
    artifact_name: []const u8,
    chunk_key: []u8,
    source_hash: u64,
};

const ChunkEmbeddingSource = struct {
    key: []u8,
    text: []u8,
};

const CachedChunkDenseWindowItem = struct {
    chunk_key: []u8,
    embedding_key: []u8,
};

fn freeChunkEmbeddingSources(alloc: Allocator, sources: []const ChunkEmbeddingSource) void {
    for (sources) |source| {
        alloc.free(source.key);
        alloc.free(source.text);
    }
    if (sources.len > 0) alloc.free(sources);
}

fn clearChunkEmbeddingSourceList(alloc: Allocator, sources: *std.ArrayListUnmanaged(ChunkEmbeddingSource)) void {
    for (sources.items) |source| {
        alloc.free(source.key);
        alloc.free(source.text);
    }
    sources.clearRetainingCapacity();
}

const ChunkEmbeddingSourceSet = struct {
    sources: []ChunkEmbeddingSource = &.{},
    desired_chunk_keys: [][]u8 = &.{},

    fn deinit(self: *@This(), alloc: Allocator) void {
        freeChunkEmbeddingSources(alloc, self.sources);
        freeKeyList(alloc, self.desired_chunk_keys);
        self.* = .{};
    }
};

fn requestUsesMaterializedChunkArtifact(
    runtime: *EnrichmentRuntime,
    artifact_name: []const u8,
) bool {
    if (artifact_name.len == 0) return false;
    const chunk_cfg = runtime.index_manager.getEnrichment(.chunk, artifact_name) orelse return false;
    return chunk_cfg.source_artifact_name.len > 0;
}

const StaleEmbeddingDeletes = struct {
    vector_keys: [][]u8 = &.{},
    artifact_delete_keys: [][]u8 = &.{},

    fn deinit(self: *@This(), alloc: Allocator) void {
        freeKeyList(alloc, self.vector_keys);
        freeKeyList(alloc, self.artifact_delete_keys);
        self.* = .{};
    }
};

const PlainDenseBatchItem = struct {
    request: enrichment_types.GeneratedEnrichmentRequest,
    source_text: []const u8,
    source_hash: u64,
    artifact_key: []u8,
};

const AssetProducerBatchItem = struct {
    request: enrichment_types.GeneratedEnrichmentRequest,
    producer_type: asset_producer_mod.ProducerType,
    config_json: []u8,
    raw_doc: []u8,
    source_text: []const u8,
    source_parts_json: ?[]u8 = null,
    artifact_key: []u8,
    state_key: []u8,
    state_value: []u8,

    fn asRequest(self: *const @This()) asset_producer_mod.Request {
        return .{
            .producer_type = self.producer_type,
            .config_json = self.config_json,
            .source_text = self.source_text,
            .source_parts_json = self.source_parts_json,
            .content_type = self.request.content_type,
        };
    }
};

fn freePlainDenseBatchItems(alloc: Allocator, items: []PlainDenseBatchItem) void {
    for (items) |item| {
        alloc.free(@constCast(item.source_text));
        alloc.free(item.artifact_key);
    }
}

fn freeAssetProducerBatchItem(alloc: Allocator, item: AssetProducerBatchItem) void {
    if (item.config_json.len > 0) alloc.free(item.config_json);
    alloc.free(item.raw_doc);
    alloc.free(@constCast(item.source_text));
    if (item.source_parts_json) |parts| alloc.free(parts);
    alloc.free(item.artifact_key);
    alloc.free(item.state_key);
    alloc.free(item.state_value);
}

fn clearAssetProducerBatchItems(
    alloc: Allocator,
    items: *std.ArrayListUnmanaged(AssetProducerBatchItem),
) void {
    for (items.items) |item| freeAssetProducerBatchItem(alloc, item);
    items.clearRetainingCapacity();
}

fn freeWorkerChunkCache(alloc: Allocator, cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry)) void {
    for (cache.items) |entry| {
        alloc.free(entry.key);
        chunker_mod.freeChunks(alloc, entry.chunks);
    }
    cache.deinit(alloc);
}

fn freeRequestPlanCache(alloc: Allocator, cache: *std.ArrayListUnmanaged(RequestPlanCacheEntry)) void {
    for (cache.items) |entry| {
        alloc.free(entry.doc_key);
        enrichment_types.deinitGeneratedRequests(alloc, entry.requests);
    }
    cache.deinit(alloc);
}

fn requestHasChunking(request: enrichment_types.GeneratedEnrichmentRequest) bool {
    return request.chunk_size > 0 or request.chunker_json.len > 0;
}

fn requestCanBatchPlainDense(request: enrichment_types.GeneratedEnrichmentRequest) bool {
    return request.kind == .dense_embedding and
        !requestHasChunking(request) and
        request.source_template.len == 0;
}

fn samePlainDenseBatchKey(
    lhs: enrichment_types.GeneratedEnrichmentRequest,
    rhs: enrichment_types.GeneratedEnrichmentRequest,
) bool {
    return lhs.expected_dims == rhs.expected_dims and
        std.mem.eql(u8, requestEmbeddingName(lhs), requestEmbeddingName(rhs)) and
        std.mem.eql(u8, lhs.execution_json, rhs.execution_json);
}

fn sameAssetProducerBatchKey(lhs: AssetProducerBatchItem, rhs: AssetProducerBatchItem) bool {
    return lhs.producer_type == rhs.producer_type and
        std.mem.eql(u8, lhs.config_json, rhs.config_json) and
        std.mem.eql(u8, lhs.request.content_type, rhs.request.content_type) and
        std.mem.eql(u8, lhs.request.execution_json, rhs.request.execution_json);
}

fn assetProducerBatchItemBytes(item: AssetProducerBatchItem) usize {
    return addUsizeSaturating(
        addUsizeSaturating(item.config_json.len, item.source_text.len),
        if (item.source_parts_json) |parts| parts.len else 0,
    );
}

fn assetProducerBatchBytes(items: []const AssetProducerBatchItem) usize {
    var total: usize = 0;
    for (items) |item| total = addUsizeSaturating(total, assetProducerBatchItemBytes(item));
    return total;
}

fn workerChunkCacheKey(
    alloc: Allocator,
    request: enrichment_types.GeneratedEnrichmentRequest,
) ![]u8 {
    var chunk_size: [@sizeOf(u32)]u8 = undefined;
    var chunk_overlap: [@sizeOf(u32)]u8 = undefined;
    std.mem.writeInt(u32, &chunk_size, request.chunk_size, .big);
    std.mem.writeInt(u32, &chunk_overlap, request.chunk_overlap, .big);
    return try workerChunkCacheTupleKeyAlloc(alloc, &.{
        request.doc_key,
        request.source_field,
        request.source_template,
        &chunk_size,
        &chunk_overlap,
        request.chunker_json,
    });
}

fn workerChunkCacheTupleKeyAlloc(alloc: Allocator, components: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (components) |component| {
        if (component.len > std.math.maxInt(u32)) return error.KeyComponentTooLarge;
        var len_buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(component.len), .big);
        try out.appendSlice(alloc, &len_buf);
        try out.appendSlice(alloc, component);
    }

    return try out.toOwnedSlice(alloc);
}

test "enrichment worker chunk cache keys preserve embedded separators" {
    const alloc = std.testing.allocator;

    const left = try workerChunkCacheKey(alloc, .{
        .kind = .chunk_text,
        .index_name = "idx",
        .artifact_name = "artifact",
        .embedding_name = "embedding",
        .doc_key = "doc\x1ffield",
        .source_field = "field",
        .source_template = "{{body}}",
        .chunk_size = 64,
        .chunk_overlap = 8,
        .chunker_json = "{\"mode\":\"a\"}",
    });
    defer alloc.free(left);

    const right = try workerChunkCacheKey(alloc, .{
        .kind = .chunk_text,
        .index_name = "idx",
        .artifact_name = "artifact",
        .embedding_name = "embedding",
        .doc_key = "doc",
        .source_field = "field\x1ffield",
        .source_template = "{{body}}",
        .chunk_size = 64,
        .chunk_overlap = 8,
        .chunker_json = "{\"mode\":\"a\"}",
    });
    defer alloc.free(right);

    try std.testing.expect(!std.mem.eql(u8, left, right));
}

fn getOrCreateRequestChunks(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
) ![]const chunker_mod.Chunk {
    if (!requestHasChunking(request)) return &.{};

    const cache_key = try workerChunkCacheKey(runtime.alloc, request);
    errdefer runtime.alloc.free(cache_key);

    for (cache.items) |entry| {
        if (std.mem.eql(u8, entry.key, cache_key)) {
            runtime.alloc.free(cache_key);
            return entry.chunks;
        }
    }

    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => null,
    };
    if (raw == null) {
        const empty = try runtime.alloc.alloc(chunker_mod.Chunk, 0);
        try cache.append(runtime.alloc, .{
            .key = cache_key,
            .chunks = empty,
        });
        return cache.items[cache.items.len - 1].chunks;
    }
    defer runtime.alloc.free(raw.?);

    const source_text = try extractSourceText(runtime.alloc, runtime.config, raw.?, request) orelse {
        const empty = try runtime.alloc.alloc(chunker_mod.Chunk, 0);
        try cache.append(runtime.alloc, .{
            .key = cache_key,
            .chunks = empty,
        });
        return cache.items[cache.items.len - 1].chunks;
    };
    defer runtime.alloc.free(source_text);

    const chunks = if (request.chunker_json.len > 0)
        try chunker_mod.chunkTextWithConfigJson(runtime.alloc, source_text, request.chunker_json)
    else
        try chunker_mod.chunkText(runtime.alloc, source_text, request.chunk_size, request.chunk_overlap);

    try cache.append(runtime.alloc, .{
        .key = cache_key,
        .chunks = chunks,
    });
    return cache.items[cache.items.len - 1].chunks;
}

pub const EnrichmentRuntime = if (builtin.os.tag == .freestanding) struct {
    alloc: Allocator,
    store: backend_erased.Store,
    owns_store: bool,
    change_journal: *change_journal_mod.Journal,
    replay_source: replay_source_mod.Source,
    index_manager: *index_manager_mod.IndexManager,
    coverage_apply_mutex: ?*apply_rw_lock_mod.ApplyRwLock = null,
    write_ctx: *anyopaque,
    write_fn: GeneratedRecordWriter,
    notify_ctx: *anyopaque,
    notify_fn: NotifyFn,
    config: Config,
    applied_sequence: u64 = 0,
    target_sequence: u64 = 0,
    processed_requests: u64 = 0,
    error_count: u64 = 0,
    retryable_error_count: u64 = 0,
    fatal_error_count: u64 = 0,
    retrying: bool = false,
    worker_failed: bool = false,
    skip_by_hash_count: u64 = 0,
    skipped_source_count: u64 = 0,
    codec_decode_failures: u64 = 0,
    embed_batches_started: u64 = 0,
    embed_batches_completed: u64 = 0,
    embed_items_started: u64 = 0,
    embed_items_completed: u64 = 0,
    active_embed_batch_items: u64 = 0,
    active_embed_batch_bytes: u64 = 0,
    active_embed_batch_max_bytes: u64 = 0,
    active_embed_batch_started_ms: u64 = 0,
    last_embed_batch_items: u64 = 0,
    last_embed_batch_bytes: u64 = 0,
    last_embed_batch_max_bytes: u64 = 0,
    last_embed_batch_completed_ms: u64 = 0,
    last_embed_batch_ns: u64 = 0,
    total_embed_ns: u64 = 0,
    dense_artifact_bytes_written: u64 = 0,
    sparse_artifact_bytes_written: u64 = 0,
    chunk_artifact_bytes_written: u64 = 0,
    published_generated_artifacts: std.StringHashMapUnmanaged(void) = .empty,
    isolated_failed_indexes: std.StringHashMapUnmanaged(void) = .empty,

    pub fn init(
        alloc: Allocator,
        store: anytype,
        change_journal: *change_journal_mod.Journal,
        replay_source: replay_source_mod.Source,
        index_manager: *index_manager_mod.IndexManager,
        coverage_apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
        write_ctx: *anyopaque,
        write_fn: GeneratedRecordWriter,
        notify_ctx: *anyopaque,
        notify_fn: NotifyFn,
        _: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !@This() {
        const runtime_store = try initRuntimeStore(alloc, store);
        var runtime = @This(){
            .alloc = alloc,
            .store = runtime_store.store,
            .owns_store = runtime_store.owned,
            .change_journal = change_journal,
            .replay_source = replay_source,
            .index_manager = index_manager,
            .coverage_apply_mutex = coverage_apply_mutex,
            .write_ctx = write_ctx,
            .write_fn = write_fn,
            .notify_ctx = notify_ctx,
            .notify_fn = notify_fn,
            .config = .{
                .lease_ttl_ms = config.lease_ttl_ms,
                .dense_embedder = config.dense_embedder,
                .sparse_embedder = config.sparse_embedder,
                .asset_producer = config.asset_producer,
                .enable_without_producers = config.enable_without_producers,
                .secret_store = config.secret_store,
                .remote_content = config.remote_content,
                .resource_manager = config.resource_manager,
                .clock = config.clock,
            },
        };
        runtime.applied_sequence = try enrichment_state.loadAppliedSequence(alloc, store, scope_name);
        const persisted_status = try enrichment_state.loadRuntimeStatus(alloc, store, scope_name);
        restorePersistedRuntimeStatus(&runtime, persisted_status);
        return runtime;
    }

    pub fn deinit(self: *@This()) void {
        clearPublishedGeneratedArtifacts(self);
        clearIsolatedFailedIndexes(self);
        if (self.owns_store) self.store.deinit();
        if (self.config.dense_embedder) |dense_embedder| dense_embedder.deinit(self.alloc);
        if (self.config.sparse_embedder) |sparse_embedder| sparse_embedder.deinit(self.alloc);
        if (self.config.asset_producer) |producer| producer.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn start(self: *@This()) !void {
        _ = self;
    }

    pub fn stop(self: *@This()) void {
        _ = self;
    }

    pub fn isStarted(_: *const @This()) bool {
        return false;
    }

    pub fn setStatusHook(self: *@This(), hook: ?StatusHook) void {
        _ = self;
        _ = hook;
    }

    pub fn notifySequence(self: *@This(), sequence: u64) void {
        if (sequence > self.target_sequence) clearIsolatedFailedIndexes(self);
        self.target_sequence = @max(self.target_sequence, sequence);
    }

    pub fn resumeFrom(self: *@This(), sequence: u64, target_sequence: u64) !void {
        const next_applied = @min(self.applied_sequence, sequence);
        if (next_applied != self.applied_sequence) {
            try saveAppliedSequenceWithRetry(self, scope_name, next_applied);
            self.applied_sequence = next_applied;
        }
        clearPublishedGeneratedArtifacts(self);
        clearIsolatedFailedIndexes(self);
        self.target_sequence = @max(self.target_sequence, @max(target_sequence, next_applied));
    }

    pub fn waitForApplied(self: *@This(), sequence: u64) !void {
        try self.catchUpUntil(sequence);
    }

    pub fn catchUpUntil(self: *@This(), sequence: u64) !void {
        if (self.config.dense_embedder == null and self.config.sparse_embedder == null and self.config.asset_producer == null and !self.config.enable_without_producers) return;

        self.notifySequence(sequence);
        const pending = try enrichment_worker.collectPendingDocumentGroups(self.alloc, self.replay_source, self.applied_sequence);
        defer enrichment_worker.freePendingDocumentGroups(self.alloc, pending);

        var chunk_cache = std.ArrayListUnmanaged(WorkerChunkCacheEntry).empty;
        defer freeWorkerChunkCache(self.alloc, &chunk_cache);
        var request_plan_cache = std.ArrayListUnmanaged(RequestPlanCacheEntry).empty;
        defer freeRequestPlanCache(self.alloc, &request_plan_cache);
        var deferred_plain_dense = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest).empty;
        defer deferred_plain_dense.deinit(self.alloc);
        var deferred_chunked_dense = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest).empty;
        defer deferred_chunked_dense.deinit(self.alloc);
        var deferred_assets = std.ArrayListUnmanaged(AssetProducerBatchItem).empty;
        defer {
            clearAssetProducerBatchItems(self.alloc, &deferred_assets);
            deferred_assets.deinit(self.alloc);
        }
        var window = GeneratedReplayWindow{ .alloc = self.alloc };
        defer window.deinit();
        const max_window_items = generatedReplayWindowItems();
        var processed_request_count: u64 = 0;

        var max_seen = self.applied_sequence;
        for (pending) |group| {
            max_seen = @max(max_seen, group.sequence);
            try processPendingDocumentGroup(self, group, &chunk_cache, &request_plan_cache, &deferred_plain_dense, &deferred_chunked_dense, &deferred_assets, &window, &processed_request_count);
            if (window.itemCount() >= max_window_items) try flushGeneratedReplayWindow(self, &window);
        }
        try flushAssetProducerBatch(self, &deferred_assets, &window);
        try processPlainDenseWindow(self, deferred_plain_dense.items, &window);
        try processChunkedDenseWindow(self, deferred_chunked_dense.items, &chunk_cache, &window);
        try flushGeneratedReplayWindow(self, &window);
        if (pending.len == 0) {
            max_seen = sequence;
        }

        if (max_seen > self.applied_sequence) {
            try saveAppliedSequenceWithRetry(self, scope_name, max_seen);
            self.applied_sequence = max_seen;
            self.processed_requests += processed_request_count;
            self.retrying = false;
            self.worker_failed = false;
            clearPublishedGeneratedArtifacts(self);
            clearIsolatedFailedIndexes(self);
        }
    }

    pub fn markAppliedThrough(self: *@This(), sequence: u64) !void {
        if (sequence <= self.applied_sequence) {
            self.target_sequence = @max(self.target_sequence, sequence);
            return;
        }
        try saveAppliedSequenceWithRetry(self, scope_name, sequence);
        self.applied_sequence = sequence;
        self.target_sequence = @max(self.target_sequence, sequence);
        clearPublishedGeneratedArtifacts(self);
        clearIsolatedFailedIndexes(self);
    }

    pub fn stats(self: *@This()) types.EnrichmentStats {
        const projection_status = runtimeProjectionStatus(self.retrying, self.worker_failed);
        const config_hash = enrichmentCatalogConfigHash(self.alloc, self.index_manager) catch 0;
        return .{
            .enabled = self.config.dense_embedder != null or self.config.sparse_embedder != null or self.config.asset_producer != null or self.config.enable_without_producers,
            .lease_owned = true,
            .has_lease = true,
            .acquisition_count = 0,
            .lease_acquire_failures = 0,
            .lost_leases = 0,
            .last_acquired_ms = 0,
            .target_sequence = self.target_sequence,
            .applied_sequence = self.applied_sequence,
            .projection_checkpoint_status = enrichment_state.projectionStatusName(projection_status),
            .projection_checkpoint_applied_sequence = self.applied_sequence,
            .projection_checkpoint_config_hash = config_hash,
            .checkpoint_replay_tail_sequence_count = self.target_sequence -| self.applied_sequence,
            .processed_requests = self.processed_requests,
            .error_count = self.error_count,
            .retryable_error_count = self.retryable_error_count,
            .fatal_error_count = self.fatal_error_count,
            .retrying = self.retrying,
            .worker_failed = self.worker_failed,
            .skip_by_hash_count = self.skip_by_hash_count,
            .skipped_source_count = self.skipped_source_count,
            .codec_decode_failures = self.codec_decode_failures,
            .embed_batches_started = self.embed_batches_started,
            .embed_batches_completed = self.embed_batches_completed,
            .embed_items_started = self.embed_items_started,
            .embed_items_completed = self.embed_items_completed,
            .active_embed_batch_items = self.active_embed_batch_items,
            .active_embed_batch_bytes = self.active_embed_batch_bytes,
            .active_embed_batch_max_bytes = self.active_embed_batch_max_bytes,
            .active_embed_batch_started_ms = self.active_embed_batch_started_ms,
            .last_embed_batch_items = self.last_embed_batch_items,
            .last_embed_batch_bytes = self.last_embed_batch_bytes,
            .last_embed_batch_max_bytes = self.last_embed_batch_max_bytes,
            .last_embed_batch_completed_ms = self.last_embed_batch_completed_ms,
            .last_embed_batch_ns = self.last_embed_batch_ns,
            .total_embed_ns = self.total_embed_ns,
            .dense_artifact_bytes_written = self.dense_artifact_bytes_written,
            .sparse_artifact_bytes_written = self.sparse_artifact_bytes_written,
            .chunk_artifact_bytes_written = self.chunk_artifact_bytes_written,
            .artifact_bytes_written = self.dense_artifact_bytes_written + self.sparse_artifact_bytes_written + self.chunk_artifact_bytes_written,
        };
    }

    pub fn indexHasIsolatedFailure(self: *@This(), index_name: []const u8) bool {
        return self.isolated_failed_indexes.contains(index_name);
    }
} else struct {
    alloc: Allocator,
    io_impl: ?*Io.Threaded,
    store: backend_erased.Store,
    owns_store: bool,
    change_journal: *change_journal_mod.Journal,
    replay_source: replay_source_mod.Source,
    index_manager: *index_manager_mod.IndexManager,
    coverage_apply_mutex: ?*apply_rw_lock_mod.ApplyRwLock = null,
    write_ctx: *anyopaque,
    write_fn: GeneratedRecordWriter,
    notify_ctx: *anyopaque,
    notify_fn: NotifyFn,
    config: Config,
    ownership: ownership_mod.State,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    shutdown: bool = false,
    target_sequence: u64 = 0,
    applied_sequence: u64 = 0,
    processed_requests: u64 = 0,
    error_count: u64 = 0,
    retryable_error_count: u64 = 0,
    fatal_error_count: u64 = 0,
    retrying: bool = false,
    worker_failed: bool = false,
    skip_by_hash_count: u64 = 0,
    skipped_source_count: u64 = 0,
    codec_decode_failures: u64 = 0,
    embed_batches_started: u64 = 0,
    embed_batches_completed: u64 = 0,
    embed_items_started: u64 = 0,
    embed_items_completed: u64 = 0,
    active_embed_batch_items: u64 = 0,
    active_embed_batch_bytes: u64 = 0,
    active_embed_batch_max_bytes: u64 = 0,
    active_embed_batch_started_ms: u64 = 0,
    last_embed_batch_items: u64 = 0,
    last_embed_batch_bytes: u64 = 0,
    last_embed_batch_max_bytes: u64 = 0,
    last_embed_batch_completed_ms: u64 = 0,
    last_embed_batch_ns: u64 = 0,
    total_embed_ns: u64 = 0,
    dense_artifact_bytes_written: u64 = 0,
    sparse_artifact_bytes_written: u64 = 0,
    chunk_artifact_bytes_written: u64 = 0,
    last_error_name: ?[]const u8 = null,
    published_generated_artifacts: std.StringHashMapUnmanaged(void) = .empty,
    isolated_failed_indexes: std.StringHashMapUnmanaged(void) = .empty,
    status_hook: ?StatusHook = null,
    future: ?Io.Future(void) = null,

    pub fn init(
        alloc: Allocator,
        store: anytype,
        change_journal: *change_journal_mod.Journal,
        replay_source: replay_source_mod.Source,
        index_manager: *index_manager_mod.IndexManager,
        coverage_apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
        write_ctx: *anyopaque,
        write_fn: GeneratedRecordWriter,
        notify_ctx: *anyopaque,
        notify_fn: NotifyFn,
        backend_runtime: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !EnrichmentRuntime {
        const io_impl = backend_runtime.io_impl;
        if ((config.dense_embedder != null or config.sparse_embedder != null or config.asset_producer != null or config.enable_without_producers) and io_impl == null) return error.MissingBackendRuntimeIo;
        var runtime_store = try initRuntimeStore(alloc, store);
        errdefer runtime_store.deinit();
        var runtime = EnrichmentRuntime{
            .alloc = alloc,
            .io_impl = io_impl,
            .store = runtime_store.store,
            .owns_store = runtime_store.owned,
            .change_journal = change_journal,
            .replay_source = replay_source,
            .index_manager = index_manager,
            .coverage_apply_mutex = coverage_apply_mutex,
            .write_ctx = write_ctx,
            .write_fn = write_fn,
            .notify_ctx = notify_ctx,
            .notify_fn = notify_fn,
            .config = .{
                .lease_ttl_ms = config.lease_ttl_ms,
                .dense_embedder = config.dense_embedder,
                .sparse_embedder = config.sparse_embedder,
                .asset_producer = config.asset_producer,
                .enable_without_producers = config.enable_without_producers,
                .secret_store = config.secret_store,
                .remote_content = config.remote_content,
                .resource_manager = config.resource_manager,
                .clock = config.clock,
            },
            .ownership = try ownership_mod.State.init(alloc, store, enrichment_lease.default_lease_key, .{
                .lease_owned = true,
                .owner_id = config.owner_id,
                .lease_ttl_ms = config.lease_ttl_ms,
            }),
        };
        runtime.applied_sequence = try enrichment_state.loadAppliedSequence(alloc, store, scope_name);
        const persisted_status = try enrichment_state.loadRuntimeStatus(alloc, store, scope_name);
        restorePersistedRuntimeStatus(&runtime, persisted_status);
        return runtime;
    }

    pub fn deinit(self: *EnrichmentRuntime) void {
        self.stop();
        clearPublishedGeneratedArtifacts(self);
        clearIsolatedFailedIndexes(self);
        self.ownership.deinit(self.alloc);
        if (self.owns_store) self.store.deinit();
        if (self.config.dense_embedder) |dense_embedder| dense_embedder.deinit(self.alloc);
        if (self.config.sparse_embedder) |sparse_embedder| sparse_embedder.deinit(self.alloc);
        if (self.config.asset_producer) |producer| producer.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn stop(self: *EnrichmentRuntime) void {
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            self.shutdown = true;
            self.cond.broadcast(io);
            self.mutex.unlock(io);

            if (self.future) |*future| _ = future.await(io);
        }
        self.future = null;
        self.shutdown = false;
        self.ownership.release();
    }

    pub fn isStarted(self: *const EnrichmentRuntime) bool {
        return self.future != null;
    }

    pub fn start(self: *EnrichmentRuntime) !void {
        if (self.future != null) return;
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        self.future = try io.concurrent(workerMain, .{self});
    }

    pub fn setStatusHook(self: *EnrichmentRuntime, hook: ?StatusHook) void {
        const io_impl = self.io_impl orelse {
            self.status_hook = hook;
            return;
        };
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        self.status_hook = hook;
        self.mutex.unlock(io);
    }

    fn notifyStatusHook(self: *EnrichmentRuntime) void {
        const hook = blk: {
            const io_impl = self.io_impl orelse break :blk self.status_hook;
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            break :blk self.status_hook;
        };
        if (hook) |value| value.notify();
    }

    pub fn notifySequence(self: *EnrichmentRuntime, sequence: u64) void {
        const io_impl = self.io_impl orelse {
            if (sequence > self.target_sequence) self.last_error_name = null;
            self.retrying = false;
            self.worker_failed = false;
            if (sequence > self.target_sequence) clearIsolatedFailedIndexes(self);
            self.target_sequence = @max(self.target_sequence, sequence);
            return;
        };
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        if (sequence > self.target_sequence) self.last_error_name = null;
        self.retrying = false;
        self.worker_failed = false;
        if (sequence > self.target_sequence) clearIsolatedFailedIndexes(self);
        self.target_sequence = @max(self.target_sequence, sequence);
        self.cond.broadcast(io);
        self.mutex.unlock(io);
    }

    pub fn resumeFrom(self: *EnrichmentRuntime, sequence: u64, target_sequence: u64) !void {
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        const current_applied = self.applied_sequence;
        const next_applied = @min(current_applied, sequence);
        self.applied_sequence = next_applied;
        self.target_sequence = @max(self.target_sequence, @max(target_sequence, next_applied));
        self.last_error_name = null;
        self.retrying = false;
        self.worker_failed = false;
        clearPublishedGeneratedArtifacts(self);
        clearIsolatedFailedIndexes(self);
        self.cond.broadcast(io);
        self.mutex.unlock(io);

        if (next_applied != current_applied) {
            try saveAppliedSequenceWithRetry(self, scope_name, next_applied);
        }
    }

    pub fn waitForApplied(self: *EnrichmentRuntime, sequence: u64) !void {
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        while (self.applied_sequence < sequence and self.last_error_name == null and !self.retrying) {
            self.cond.waitUncancelable(io, &self.mutex);
        }
        if (self.last_error_name != null) return RuntimeError.EnrichmentWorkerFailed;
        if (self.applied_sequence < sequence and self.retrying) return RuntimeError.EnrichmentRetryInProgress;
    }

    pub fn catchUpUntil(self: *EnrichmentRuntime, sequence: u64) !void {
        if (sequence == 0) return;
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();

        self.notifySequence(sequence);
        while (true) {
            self.mutex.lockUncancelable(io);
            const applied = self.applied_sequence;
            const failed = self.last_error_name != null;
            const retrying = self.retrying;
            self.mutex.unlock(io);

            if (failed) return RuntimeError.EnrichmentWorkerFailed;
            if (applied >= sequence) return;
            if (retrying) return RuntimeError.EnrichmentRetryInProgress;
            try runForegroundCatchUpPass(self, io, sequence);
        }
    }

    pub fn markAppliedThrough(self: *EnrichmentRuntime, sequence: u64) !void {
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        var changed = false;
        var status: enrichment_state.RuntimeStatus = .{};
        self.mutex.lockUncancelable(io);
        if (sequence > self.applied_sequence) {
            self.applied_sequence = sequence;
            changed = true;
        }
        self.target_sequence = @max(self.target_sequence, sequence);
        self.last_error_name = null;
        self.retrying = false;
        self.worker_failed = false;
        clearPublishedGeneratedArtifacts(self);
        clearIsolatedFailedIndexes(self);
        status = runtimeStatusSnapshot(self);
        self.cond.broadcast(io);
        self.mutex.unlock(io);

        if (changed) {
            try saveAppliedSequenceWithRetry(self, scope_name, sequence);
        }
        try saveRuntimeStatusWithRetry(self, scope_name, status);
        self.notifyStatusHook();
    }

    pub fn stats(self: *EnrichmentRuntime) types.EnrichmentStats {
        const maybe_io = if (self.io_impl) |io_impl| io_impl.io() else null;
        if (maybe_io) |io| self.mutex.lockUncancelable(io);
        defer if (maybe_io) |io| self.mutex.unlock(io);

        const ownership_stats = self.ownership.stats();
        const projection_status = runtimeProjectionStatus(self.retrying, self.worker_failed);
        const config_hash = enrichmentCatalogConfigHash(self.alloc, self.index_manager) catch 0;
        const enabled = self.config.dense_embedder != null or
            self.config.sparse_embedder != null or
            self.config.asset_producer != null or
            self.config.enable_without_producers;
        const worker_started = self.future != null;
        return .{
            .enabled = enabled,
            .lease_owned = ownership_stats.lease_owned,
            .has_lease = ownership_stats.has_lease,
            .acquisition_count = ownership_stats.acquisition_count,
            .lease_acquire_failures = ownership_stats.lease_acquire_failures,
            .lost_leases = ownership_stats.lost_leases,
            .last_acquired_ms = ownership_stats.last_acquired_ms,
            .target_sequence = self.target_sequence,
            .applied_sequence = self.applied_sequence,
            .projection_checkpoint_status = enrichment_state.projectionStatusName(projection_status),
            .projection_checkpoint_applied_sequence = self.applied_sequence,
            .projection_checkpoint_config_hash = config_hash,
            .checkpoint_replay_tail_sequence_count = self.target_sequence -| self.applied_sequence,
            .processed_requests = self.processed_requests,
            .error_count = self.error_count,
            .retryable_error_count = self.retryable_error_count,
            .fatal_error_count = self.fatal_error_count,
            .retrying = self.retrying,
            .worker_failed = self.worker_failed,
            .worker_started = worker_started,
            .stalled = enrichmentWorkerStalled(
                enabled,
                self.target_sequence,
                self.applied_sequence,
                worker_started,
                self.retrying,
                self.worker_failed,
            ),
            .skip_by_hash_count = self.skip_by_hash_count,
            .skipped_source_count = self.skipped_source_count,
            .codec_decode_failures = self.codec_decode_failures,
            .embed_batches_started = self.embed_batches_started,
            .embed_batches_completed = self.embed_batches_completed,
            .embed_items_started = self.embed_items_started,
            .embed_items_completed = self.embed_items_completed,
            .active_embed_batch_items = self.active_embed_batch_items,
            .active_embed_batch_bytes = self.active_embed_batch_bytes,
            .active_embed_batch_max_bytes = self.active_embed_batch_max_bytes,
            .active_embed_batch_started_ms = self.active_embed_batch_started_ms,
            .last_embed_batch_items = self.last_embed_batch_items,
            .last_embed_batch_bytes = self.last_embed_batch_bytes,
            .last_embed_batch_max_bytes = self.last_embed_batch_max_bytes,
            .last_embed_batch_completed_ms = self.last_embed_batch_completed_ms,
            .last_embed_batch_ns = self.last_embed_batch_ns,
            .total_embed_ns = self.total_embed_ns,
            .dense_artifact_bytes_written = self.dense_artifact_bytes_written,
            .sparse_artifact_bytes_written = self.sparse_artifact_bytes_written,
            .chunk_artifact_bytes_written = self.chunk_artifact_bytes_written,
            .artifact_bytes_written = self.dense_artifact_bytes_written + self.sparse_artifact_bytes_written + self.chunk_artifact_bytes_written,
        };
    }

    pub fn indexHasIsolatedFailure(self: *EnrichmentRuntime, index_name: []const u8) bool {
        const maybe_io = if (self.io_impl) |io_impl| io_impl.io() else null;
        if (maybe_io) |io| self.mutex.lockUncancelable(io);
        defer if (maybe_io) |io| self.mutex.unlock(io);
        return self.isolated_failed_indexes.contains(index_name);
    }

    fn recordError(self: *EnrichmentRuntime, io: Io, err: anyerror) void {
        std.log.err("enrichment worker failed: {s}", .{@errorName(err)});
        var status: enrichment_state.RuntimeStatus = .{};
        self.mutex.lockUncancelable(io);
        self.error_count += 1;
        self.fatal_error_count += 1;
        self.retrying = false;
        self.worker_failed = true;
        if (self.last_error_name == null) self.last_error_name = @errorName(err);
        status = runtimeStatusSnapshot(self);
        self.cond.broadcast(io);
        self.mutex.unlock(io);
        saveRuntimeStatusWithRetry(self, scope_name, status) catch |save_err| {
            std.log.warn("failed to persist enrichment worker failure status: {s}", .{@errorName(save_err)});
        };
        self.notifyStatusHook();
    }

    fn recordRetryableError(self: *EnrichmentRuntime, io: Io, err: anyerror) void {
        std.log.warn("enrichment worker transient failure, will retry: {s}", .{@errorName(err)});
        var status: enrichment_state.RuntimeStatus = .{};
        self.mutex.lockUncancelable(io);
        self.error_count += 1;
        self.retryable_error_count += 1;
        self.retrying = true;
        status = runtimeStatusSnapshot(self);
        self.cond.broadcast(io);
        self.mutex.unlock(io);
        saveRuntimeStatusWithRetry(self, scope_name, status) catch |save_err| {
            std.log.warn("failed to persist enrichment retry status: {s}", .{@errorName(save_err)});
        };
        self.notifyStatusHook();
    }
};

fn enrichmentWorkerStalled(
    enabled: bool,
    target_sequence: u64,
    applied_sequence: u64,
    worker_started: bool,
    retrying: bool,
    worker_failed: bool,
) bool {
    return enabled and
        target_sequence > applied_sequence and
        !worker_started and
        !retrying and
        !worker_failed;
}

test "enrichment runtime status reports worker lifecycle diagnostics" {
    try std.testing.expect(enrichmentWorkerStalled(true, 5, 1, false, false, false));
    try std.testing.expect(!enrichmentWorkerStalled(true, 5, 1, true, false, false));
    try std.testing.expect(!enrichmentWorkerStalled(true, 5, 1, false, true, false));
    try std.testing.expect(!enrichmentWorkerStalled(true, 5, 1, false, false, true));
    try std.testing.expect(!enrichmentWorkerStalled(true, 5, 5, false, false, false));
    try std.testing.expect(!enrichmentWorkerStalled(false, 5, 1, false, false, false));
}

fn handleWorkerLoopError(runtime: *EnrichmentRuntime, io: Io, err: anyerror) void {
    if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return;
    if (isRetryableEnrichmentError(err)) {
        runtime.recordRetryableError(io, err);
        io.sleep(Io.Duration.fromMilliseconds(@intCast(transient_worker_retry_sleep_ns / std.time.ns_per_ms)), .awake) catch {};
        return;
    }
    runtime.recordError(io, err);
}

fn queueTerminalCoverageForRequest(runtime: *EnrichmentRuntime, window: *GeneratedReplayWindow, request: enrichment_types.GeneratedEnrichmentRequest) !void {
    switch (request.kind) {
        .dense_embedding => {
            const indexes = try runtime.index_manager.denseIndexesForEmbedding(runtime.alloc, requestEmbeddingName(request), request.expected_dims);
            defer {
                for (indexes) |index_name| runtime.alloc.free(index_name);
                runtime.alloc.free(indexes);
            }
            if (indexes.len > 0) return queueDerivedCoverageOutcome(runtime, window, request.doc_key, indexes, .terminal_failed);
        },
        .sparse_embedding => {
            const indexes = try runtime.index_manager.sparseIndexesForEmbedding(runtime.alloc, requestEmbeddingName(request));
            defer {
                for (indexes) |index_name| runtime.alloc.free(index_name);
                runtime.alloc.free(indexes);
            }
            if (indexes.len > 0) return queueDerivedCoverageOutcome(runtime, window, request.doc_key, indexes, .terminal_failed);
        },
        .chunk_text => {
            const indexes = try runtime.index_manager.vectorIndexesForChunk(runtime.alloc, requestArtifactName(request));
            defer {
                for (indexes) |index_name| runtime.alloc.free(index_name);
                runtime.alloc.free(indexes);
            }
            if (indexes.len > 0) return queueDerivedCoverageOutcome(runtime, window, request.doc_key, indexes, .terminal_failed);
        },
        .asset => {
            const indexes = try runtime.index_manager.vectorIndexesDependingOnArtifact(runtime.alloc, requestArtifactName(request));
            defer {
                for (indexes) |index_name| runtime.alloc.free(index_name);
                runtime.alloc.free(indexes);
            }
            if (indexes.len > 0) return queueDerivedCoverageOutcome(runtime, window, request.doc_key, indexes, .terminal_failed);
        },
    }
    try queueDerivedCoverageOutcomeForIndex(runtime, window, request.index_name, request.doc_key, .terminal_failed);
}

fn recordIsolatedRequestError(runtime: *EnrichmentRuntime, window: ?*GeneratedReplayWindow, request: enrichment_types.GeneratedEnrichmentRequest, err: anyerror) void {
    std.log.warn("enrichment request failed index={s} artifact={s}: {s}", .{ request.index_name, requestEmbeddingName(request), @errorName(err) });
    if (runtime.coverage_apply_mutex != null) {
        if (window) |active_window| queueTerminalCoverageForRequest(runtime, active_window, request) catch |coverage_err| {
            std.log.err("failed to persist terminal coverage outcome index={s} doc={s}: {s}", .{ request.index_name, request.doc_key, @errorName(coverage_err) });
        } else markDerivedCoverageTerminalFailedForIndex(runtime, request.index_name, request.doc_key) catch |coverage_err| {
            std.log.err("failed to persist terminal coverage outcome index={s} doc={s}: {s}", .{ request.index_name, request.doc_key, @errorName(coverage_err) });
        };
    }
    if (runtime.io_impl) |io_impl| {
        const io = io_impl.io();
        runtime.mutex.lockUncancelable(io);
        runtime.error_count += 1;
        runtime.fatal_error_count += 1;
        runtime.retrying = false;
        runtime.worker_failed = false;
        markIsolatedFailedIndex(runtime, request.index_name);
        runtime.mutex.unlock(io);
    } else {
        runtime.error_count += 1;
        runtime.fatal_error_count += 1;
        runtime.retrying = false;
        runtime.worker_failed = false;
        markIsolatedFailedIndex(runtime, request.index_name);
    }
    runtime.notifyStatusHook();
}

test "isolated enrichment request error does not mark worker failed" {
    var runtime = EnrichmentRuntime{
        .alloc = std.testing.allocator,
        .io_impl = null,
        .store = undefined,
        .owns_store = false,
        .change_journal = undefined,
        .replay_source = undefined,
        .index_manager = undefined,
        .write_ctx = undefined,
        .write_fn = undefined,
        .notify_ctx = undefined,
        .notify_fn = undefined,
        .config = .{},
        .ownership = undefined,
    };
    runtime.retrying = true;
    runtime.worker_failed = true;

    recordIsolatedRequestError(&runtime, null, .{
        .kind = .dense_embedding,
        .index_name = "bad_visual",
        .embedding_name = "clipclap",
        .doc_key = "doc:1",
        .source_field = "image_url",
    }, error.UnsupportedEmbeddingProvider);

    try std.testing.expectEqual(@as(u64, 1), runtime.error_count);
    try std.testing.expectEqual(@as(u64, 1), runtime.fatal_error_count);
    try std.testing.expect(!runtime.retrying);
    try std.testing.expect(!runtime.worker_failed);
    try std.testing.expect(runtime.indexHasIsolatedFailure("bad_visual"));
    try std.testing.expect(!runtime.indexHasIsolatedFailure("healthy_text"));
    clearIsolatedFailedIndexes(&runtime);
}

fn workerMain(runtime: *EnrichmentRuntime) void {
    const io_impl = runtime.io_impl orelse return;
    const io = io_impl.io();

    worker_loop: while (true) {
        runtime.mutex.lockUncancelable(io);
        while (!runtime.shutdown and (runtime.last_error_name != null or (runtime.target_sequence <= runtime.applied_sequence and !runtime.retrying))) {
            runtime.cond.waitUncancelable(io, &runtime.mutex);
        }
        if (runtime.shutdown) {
            runtime.mutex.unlock(io);
            return;
        }
        const target_sequence = runtime.target_sequence;
        runtime.mutex.unlock(io);

        runForegroundCatchUpPass(runtime, io, target_sequence) catch |err| {
            handleWorkerLoopError(runtime, io, err);
            continue :worker_loop;
        };
    }
}

fn runForegroundCatchUpPass(runtime: *EnrichmentRuntime, io: Io, target_sequence: u64) !void {
    const now_ms = runtime.config.clock.nowRealtimeMs();
    runtime.mutex.lockUncancelable(io);
    const acquired = runtime.ownership.ensureLease(now_ms) catch |err| {
        runtime.ownership.noteAcquireFailure();
        runtime.mutex.unlock(io);
        return err;
    };
    runtime.mutex.unlock(io);
    if (!acquired) {
        // A live lease held by another owner can remain valid for the full
        // 30-second TTL. Pace denial retries so failover does not monopolize a
        // core or hammer the durable lease record while still reacting quickly
        // after expiry.
        io.sleep(
            Io.Duration.fromMilliseconds(@intCast(lease_denied_retry_sleep_ns / std.time.ns_per_ms)),
            .awake,
        ) catch {};
        return;
    }

    const pending = try enrichment_worker.collectPendingDocumentGroups(runtime.alloc, runtime.replay_source, runtime.applied_sequence);
    defer enrichment_worker.freePendingDocumentGroups(runtime.alloc, pending);

    var processed_request_count: u64 = 0;
    var max_seen = runtime.applied_sequence;

    retry_pending: while (true) {
        if (runtimeShuttingDown(runtime)) return error.EnrichmentRetryAborted;
        var chunk_cache = std.ArrayListUnmanaged(WorkerChunkCacheEntry).empty;
        defer freeWorkerChunkCache(runtime.alloc, &chunk_cache);
        var request_plan_cache = std.ArrayListUnmanaged(RequestPlanCacheEntry).empty;
        defer freeRequestPlanCache(runtime.alloc, &request_plan_cache);
        var deferred_plain_dense = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest).empty;
        defer deferred_plain_dense.deinit(runtime.alloc);
        var deferred_chunked_dense = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest).empty;
        defer deferred_chunked_dense.deinit(runtime.alloc);
        var deferred_assets = std.ArrayListUnmanaged(AssetProducerBatchItem).empty;
        defer {
            clearAssetProducerBatchItems(runtime.alloc, &deferred_assets);
            deferred_assets.deinit(runtime.alloc);
        }
        var window = GeneratedReplayWindow{ .alloc = runtime.alloc };
        defer window.deinit();
        const max_window_items = generatedReplayWindowItems();

        processed_request_count = 0;
        max_seen = runtime.applied_sequence;

        for (pending) |group| {
            max_seen = @max(max_seen, group.sequence);
            processPendingDocumentGroup(runtime, group, &chunk_cache, &request_plan_cache, &deferred_plain_dense, &deferred_chunked_dense, &deferred_assets, &window, &processed_request_count) catch |err| {
                if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return err;
                if (isRetryableEnrichmentError(err)) continue :retry_pending;
                return err;
            };
            flushGeneratedReplayWindowIfNeeded(runtime, &window, max_window_items) catch |err| {
                if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return err;
                if (isRetryableEnrichmentError(err)) continue :retry_pending;
                return err;
            };
        }
        flushAssetProducerBatch(runtime, &deferred_assets, &window) catch |err| {
            if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return err;
            if (isRetryableEnrichmentError(err)) continue :retry_pending;
            return err;
        };
        processPlainDenseWindow(runtime, deferred_plain_dense.items, &window) catch |err| {
            if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return err;
            if (isRetryableEnrichmentError(err)) continue :retry_pending;
            return err;
        };
        processChunkedDenseWindow(runtime, deferred_chunked_dense.items, &chunk_cache, &window) catch |err| {
            if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return err;
            if (isRetryableEnrichmentError(err)) continue :retry_pending;
            return err;
        };
        flushGeneratedReplayWindow(runtime, &window) catch |err| {
            if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return err;
            if (isRetryableEnrichmentError(err)) continue :retry_pending;
            return err;
        };
        break :retry_pending;
    }
    if (pending.len == 0) {
        max_seen = target_sequence;
    }

    if (max_seen > runtime.applied_sequence) {
        try saveAppliedSequenceWithRetry(runtime, scope_name, max_seen);
        var status: enrichment_state.RuntimeStatus = .{};
        runtime.mutex.lockUncancelable(io);
        runtime.applied_sequence = max_seen;
        runtime.processed_requests += processed_request_count;
        runtime.retrying = false;
        runtime.worker_failed = false;
        clearPublishedGeneratedArtifacts(runtime);
        status = runtimeStatusSnapshot(runtime);
        runtime.cond.broadcast(io);
        runtime.mutex.unlock(io);
        try saveRuntimeStatusWithRetry(runtime, scope_name, status);
        runtime.notifyStatusHook();
    } else if (pending.len == 0) {
        var status: enrichment_state.RuntimeStatus = .{};
        runtime.mutex.lockUncancelable(io);
        runtime.retrying = false;
        runtime.worker_failed = false;
        status = runtimeStatusSnapshot(runtime);
        runtime.cond.broadcast(io);
        runtime.mutex.unlock(io);
        try saveRuntimeStatusWithRetry(runtime, scope_name, status);
        runtime.notifyStatusHook();
    }
}

fn processPendingDocumentGroup(
    runtime: *EnrichmentRuntime,
    pending: enrichment_worker.PendingDocumentGroup,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
    request_plan_cache: *std.ArrayListUnmanaged(RequestPlanCacheEntry),
    deferred_plain_dense: *std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest),
    deferred_chunked_dense: *std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest),
    deferred_assets: *std.ArrayListUnmanaged(AssetProducerBatchItem),
    window: *GeneratedReplayWindow,
    processed_request_count: *u64,
) !void {
    const planned = try getOrCreatePlannedRequests(runtime, pending.doc_key, request_plan_cache);
    for (planned) |request| {
        // Publish completed generated writes before the next external embedder call can enter retry backoff.
        if (window.hasDerivedItems()) try flushGeneratedReplayWindow(runtime, window);
        processed_request_count.* += 1;
        if (requestCanBatchPlainDense(request)) {
            try deferred_plain_dense.append(runtime.alloc, request);
            continue;
        }
        if (request.kind == .dense_embedding and requestHasChunking(request)) {
            try deferred_chunked_dense.append(runtime.alloc, request);
            continue;
        }
        switch (request.kind) {
            .asset => processAsset(runtime, request, deferred_assets, window) catch |err| {
                if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
                recordIsolatedRequestError(runtime, window, request, err);
                continue;
            },
            .chunk_text => processChunkText(runtime, request, chunk_cache, window) catch |err| {
                if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
                recordIsolatedRequestError(runtime, window, request, err);
                continue;
            },
            .dense_embedding => processDenseEmbedding(runtime, request, chunk_cache, window) catch |err| {
                if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
                recordIsolatedRequestError(runtime, window, request, err);
                continue;
            },
            .sparse_embedding => processSparseEmbedding(runtime, request, chunk_cache, window) catch |err| {
                if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
                recordIsolatedRequestError(runtime, window, request, err);
                continue;
            },
        }
    }
}

fn processAsset(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    deferred_assets: *std.ArrayListUnmanaged(AssetProducerBatchItem),
    window: *GeneratedReplayWindow,
) !void {
    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return,
    };
    var raw_owned = true;
    defer if (raw_owned) runtime.alloc.free(raw);

    var producer_cfg = try asset_producer_mod.parseProducerConfig(runtime.alloc, request.producer_json);
    defer producer_cfg.deinit(runtime.alloc);

    const artifact_name = requestArtifactName(request);
    const key = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, request.doc_key, "asset", artifact_name);
    var key_owned = true;
    defer if (key_owned) runtime.alloc.free(key);

    const text_indexes = try runtime.index_manager.textIndexesForChunk(runtime.alloc, artifact_name, request.full_text_index);
    defer {
        for (text_indexes) |name| runtime.alloc.free(name);
        runtime.alloc.free(text_indexes);
    }

    const source_text = try extractAssetSourceValue(runtime.alloc, runtime.config, raw, request) orelse {
        const state_key = try assetStateKeyAlloc(runtime.alloc, request.doc_key, artifact_name);
        defer runtime.alloc.free(state_key);
        if (producer_cfg.type == .document_extraction) {
            try deleteDocumentExtractionForRuntime(runtime, request.doc_key, artifact_name, key, state_key, window);
        } else {
            try storePutBatchWithRetry(runtime, &.{}, &.{ key, state_key });
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
        }
        try appendFullTextDeleteDocumentToWindow(runtime, window, key, text_indexes);
        try materializeGraphAssetDeleteForRuntime(runtime, request, window);
        return;
    };
    var source_text_owned = true;
    defer if (source_text_owned) runtime.alloc.free(@constCast(source_text));
    if (source_text.len == 0) {
        const state_key = try assetStateKeyAlloc(runtime.alloc, request.doc_key, artifact_name);
        defer runtime.alloc.free(state_key);
        if (producer_cfg.type == .document_extraction) {
            try deleteDocumentExtractionForRuntime(runtime, request.doc_key, artifact_name, key, state_key, window);
        } else {
            try storePutBatchWithRetry(runtime, &.{}, &.{ key, state_key });
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
        }
        try appendFullTextDeleteDocumentToWindow(runtime, window, key, text_indexes);
        try materializeGraphAssetDeleteForRuntime(runtime, request, window);
        return;
    }

    if (producer_cfg.type == .document_extraction) {
        try processDocumentExtractionAsset(runtime, request, raw, source_text, producer_cfg.config_json, key, window);
        return;
    }

    const source_parts_json = if (producer_cfg.type != .copy and request.source_template.len > 0)
        try renderSourcePartsJson(runtime.alloc, runtime.config, raw, request)
    else
        null;
    var source_parts_json_owned = true;
    defer if (source_parts_json_owned) {
        if (source_parts_json) |value| runtime.alloc.free(value);
    };

    if (producer_cfg.type == .copy) {
        if (try shouldSkipAssetArtifact(runtime, key, source_text)) {
            try appendInlineFullTextDocumentToWindow(runtime, window, key, source_text, text_indexes);
            try materializeGraphAssetForRuntime(runtime, request, source_text, raw, window);
            return;
        }
        try storePutWithRetry(runtime, key, source_text);
        try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
        try appendInlineFullTextDocumentToWindow(runtime, window, key, source_text, text_indexes);
        try materializeGraphAssetForRuntime(runtime, request, source_text, raw, window);
        recordArtifactBytes(runtime, .asset, source_text.len);
        return;
    }

    const state_key = try assetStateKeyAlloc(runtime.alloc, request.doc_key, artifact_name);
    var state_key_owned = true;
    defer if (state_key_owned) runtime.alloc.free(state_key);
    const state_value = try assetStateValueAlloc(runtime.alloc, source_text, source_parts_json, request.producer_json);
    var state_value_owned = true;
    defer if (state_value_owned) runtime.alloc.free(state_value);
    if (try shouldSkipAssetProducer(runtime, state_key, state_value)) {
        const existing = storeGetAlloc(runtime, key) catch |err| switch (err) {
            std.mem.Allocator.Error.OutOfMemory => return err,
            else => null,
        };
        if (existing) |value| {
            defer runtime.alloc.free(value);
            try appendInlineFullTextDocumentToWindow(runtime, window, key, value, text_indexes);
            try materializeGraphAssetForRuntime(runtime, request, value, raw, window);
            return;
        }
    }

    const config_json = producer_cfg.config_json;
    producer_cfg.config_json = "";
    var config_json_owned = true;
    errdefer if (config_json_owned and config_json.len > 0) runtime.alloc.free(config_json);

    try appendAssetProducerBatchItem(runtime, deferred_assets, window, .{
        .request = request,
        .producer_type = producer_cfg.type,
        .config_json = @constCast(config_json),
        .raw_doc = raw,
        .source_text = source_text,
        .source_parts_json = source_parts_json,
        .artifact_key = key,
        .state_key = state_key,
        .state_value = state_value,
    });
    config_json_owned = false;
    raw_owned = false;
    source_text_owned = false;
    source_parts_json_owned = false;
    key_owned = false;
    state_key_owned = false;
    state_value_owned = false;
}

fn appendAssetProducerBatchItem(
    runtime: *EnrichmentRuntime,
    items: *std.ArrayListUnmanaged(AssetProducerBatchItem),
    window: *GeneratedReplayWindow,
    item: AssetProducerBatchItem,
) !void {
    const policy = requestGeneratedTextBatchPolicy(runtime.alloc, item.request);
    if (items.items.len > 0) {
        const current_bytes = assetProducerBatchBytes(items.items);
        const item_bytes = assetProducerBatchItemBytes(item);
        if (!sameAssetProducerBatchKey(items.items[0], item) or
            items.items.len >= policy.max_items or
            addUsizeSaturating(current_bytes, item_bytes) > policy.max_bytes)
        {
            try flushAssetProducerBatch(runtime, items, window);
        }
    }
    try items.append(runtime.alloc, item);
}

fn flushAssetProducerBatch(
    runtime: *EnrichmentRuntime,
    items: *std.ArrayListUnmanaged(AssetProducerBatchItem),
    window: *GeneratedReplayWindow,
) !void {
    if (items.items.len == 0) return;
    defer clearAssetProducerBatchItems(runtime.alloc, items);

    const producer = runtime.config.asset_producer orelse return error.MissingAssetProducer;
    const requests = try runtime.alloc.alloc(asset_producer_mod.Request, items.items.len);
    defer runtime.alloc.free(requests);
    for (items.items, 0..) |*item, idx| requests[idx] = item.asRequest();

    var produced = producer.produceBatch(runtime.alloc, requests) catch |err| {
        if (err == error.OutOfMemory) return err;
        if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
        return try flushAssetProducerBatchSequential(runtime, producer, items.items, window);
    };
    if (produced.len != items.items.len) {
        for (produced) |output| {
            if (output.len > 0) runtime.alloc.free(output);
        }
        runtime.alloc.free(produced);
        return try flushAssetProducerBatchSequential(runtime, producer, items.items, window);
    }

    defer runtime.alloc.free(produced);
    errdefer {
        for (produced) |output| {
            if (output.len > 0) runtime.alloc.free(output);
        }
    }

    for (items.items, produced, 0..) |*item, output, idx| {
        applyAssetProducerBatchOutput(runtime, item.*, output, window) catch |err| {
            runtime.alloc.free(output);
            produced[idx] = "";
            if (err == error.OutOfMemory) return err;
            if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
            recordIsolatedRequestError(runtime, window, item.request, err);
            continue;
        };
        runtime.alloc.free(output);
        produced[idx] = "";
    }
}

fn flushAssetProducerBatchSequential(
    runtime: *EnrichmentRuntime,
    producer: asset_producer_mod.Producer,
    items: []const AssetProducerBatchItem,
    window: *GeneratedReplayWindow,
) !void {
    for (items) |item| {
        const request = item.asRequest();
        const produced = producer.produce(runtime.alloc, request) catch |err| {
            if (err == error.OutOfMemory) return err;
            if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
            recordIsolatedRequestError(runtime, window, item.request, err);
            continue;
        };
        defer runtime.alloc.free(produced);
        applyAssetProducerBatchOutput(runtime, item, produced, window) catch |err| {
            if (err == error.OutOfMemory) return err;
            if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
            recordIsolatedRequestError(runtime, window, item.request, err);
        };
    }
}

fn applyAssetProducerBatchOutput(
    runtime: *EnrichmentRuntime,
    item: AssetProducerBatchItem,
    produced: []const u8,
    window: *GeneratedReplayWindow,
) !void {
    const writes = [_]KVPair{
        .{ .key = item.artifact_key, .value = produced },
        .{ .key = item.state_key, .value = item.state_value },
    };
    try storePutBatch(runtime, &writes, &.{});
    try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, item.artifact_key);

    const artifact_name = requestArtifactName(item.request);
    const text_indexes = try runtime.index_manager.textIndexesForChunk(runtime.alloc, artifact_name, item.request.full_text_index);
    defer {
        for (text_indexes) |name| runtime.alloc.free(name);
        runtime.alloc.free(text_indexes);
    }
    try appendInlineFullTextDocumentToWindow(runtime, window, item.artifact_key, produced, text_indexes);
    try materializeGraphAssetForRuntime(runtime, item.request, produced, item.raw_doc, window);
    recordArtifactBytes(runtime, .asset, produced.len);
}

fn processDocumentExtractionAsset(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    raw_doc: []const u8,
    source_url: []const u8,
    config_json: []const u8,
    manifest_key: []const u8,
    window: *GeneratedReplayWindow,
) !void {
    const artifact_name = requestArtifactName(request);
    var config = try document_extraction_mod.parseConfig(runtime.alloc, config_json);
    defer config.deinit(runtime.alloc);
    try document_extraction_mod.applySourceMetadataFromJson(runtime.alloc, &config, raw_doc);

    const state_key = try assetStateKeyAlloc(runtime.alloc, request.doc_key, artifact_name);
    defer runtime.alloc.free(state_key);
    const existing_state = storeGetAlloc(runtime, state_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => null,
    };
    defer if (existing_state) |value| runtime.alloc.free(value);
    const existing_manifest = storeGetAlloc(runtime, manifest_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => null,
    };
    defer if (existing_manifest) |value| runtime.alloc.free(value);
    var previous_child_ranges: []types.DocumentArtifactChildRange = &.{};
    defer freeDocumentArtifactChildRanges(runtime.alloc, previous_child_ranges);
    if (existing_manifest) |value| {
        previous_child_ranges = try documentArtifactChildRangesFromManifestJsonAlloc(runtime.alloc, value);
    }
    const from_generation = if (existing_manifest) |value| try documentExtractionManifestGeneration(runtime.alloc, value) else 0;
    const to_generation = from_generation + 1;

    const metadata_fingerprint = try document_extraction_mod.metadataFingerprintAlloc(runtime.alloc, source_url, config_json, config);
    defer if (metadata_fingerprint) |fingerprint| runtime.alloc.free(fingerprint);
    if (metadata_fingerprint) |fingerprint| {
        if (existing_state) |state| {
            if (documentExtractionStateFingerprintMatches(runtime.alloc, state, fingerprint)) {
                if (existing_manifest) |value| {
                    if (!(try documentExtractionManifestHasLastError(runtime.alloc, value))) {
                        runtime.skip_by_hash_count += 1;
                        return;
                    }
                }
            }
        }
    }

    const fetched = template_remote.downloadRemoteContentOutcomeAllocWithConfig(
        runtime.alloc,
        runtime.config.remote_content,
        runtime.config.secret_store,
        source_url,
        if (config.credentials.len > 0) config.credentials else null,
    ) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => {
            try writeDocumentExtractionFailureManifest(
                runtime,
                request.doc_key,
                artifact_name,
                source_url,
                metadata_fingerprint orelse "",
                config.content_type,
                @errorName(err),
                "remote content download failed",
                manifest_key,
                state_key,
                previous_child_ranges,
                existing_state,
                from_generation,
                window,
            );
            return;
        },
    };
    const downloaded = switch (fetched) {
        .ok => |content| content,
        .http_error => |http_error| {
            const message = try std.fmt.allocPrint(runtime.alloc, "{s}: HTTP {d}", .{ http_error.message, http_error.status });
            defer runtime.alloc.free(message);
            try writeDocumentExtractionFailureManifest(
                runtime,
                request.doc_key,
                artifact_name,
                source_url,
                metadata_fingerprint orelse "",
                config.content_type,
                "RemoteDocumentFetchFailed",
                message,
                manifest_key,
                state_key,
                previous_child_ranges,
                existing_state,
                from_generation,
                window,
            );
            return;
        },
    };
    var downloaded_mut = downloaded;
    defer downloaded_mut.deinit(runtime.alloc);
    var resource_tracker = RuntimeDocumentExtractionResourceTracker.init(runtime);
    defer resource_tracker.deinit();
    try resource_tracker.setDownloadedBytes(downloaded_mut.data.len);

    const byte_source_fingerprint = if (metadata_fingerprint == null)
        try documentExtractionFingerprintAlloc(runtime.alloc, source_url, config_json, config.content_type, config.filename, downloaded_mut.content_type, downloaded_mut.data)
    else
        null;
    defer if (byte_source_fingerprint) |fingerprint| runtime.alloc.free(fingerprint);
    const source_fingerprint = metadata_fingerprint orelse byte_source_fingerprint.?;

    var desired_unit_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (desired_unit_keys.items) |key| runtime.alloc.free(@constCast(key));
        desired_unit_keys.deinit(runtime.alloc);
    }
    var desired_unit_fingerprints = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (desired_unit_fingerprints.items) |fingerprint| runtime.alloc.free(@constCast(fingerprint));
        desired_unit_fingerprints.deinit(runtime.alloc);
    }
    var desired_chunk_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (desired_chunk_keys.items) |key| runtime.alloc.free(@constCast(key));
        desired_chunk_keys.deinit(runtime.alloc);
    }
    var unit_text_lengths = std.ArrayListUnmanaged(usize).empty;
    defer unit_text_lengths.deinit(runtime.alloc);
    var generated_units = RuntimeGeneratedUnitCache{};
    defer generated_units.deinit(runtime.alloc);

    var collect_ctx = RuntimeDocumentExtractionCollectContext{
        .runtime = runtime,
        .config = config,
        .batch_policy = requestGeneratedTextBatchPolicy(runtime.alloc, request),
        .source_url = source_url,
        .source_bytes = downloaded_mut.data,
        .doc_key = request.doc_key,
        .artifact_name = artifact_name,
        .desired_unit_keys = &desired_unit_keys,
        .desired_unit_fingerprints = &desired_unit_fingerprints,
        .desired_chunk_keys = &desired_chunk_keys,
        .unit_text_lengths = &unit_text_lengths,
        .resource_tracker = &resource_tracker,
        .generated_units = &generated_units,
    };
    defer collect_ctx.deinit(runtime.alloc);
    document_extraction_mod.extractDownloadedStreaming(runtime.alloc, downloaded_mut, source_url, config, collect_ctx.sink()) catch |err| {
        if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
        try writeDocumentExtractionFailureManifest(
            runtime,
            request.doc_key,
            artifact_name,
            source_url,
            source_fingerprint,
            if (config.content_type.len > 0) config.content_type else downloaded_mut.content_type,
            @errorName(err),
            "document extraction failed",
            manifest_key,
            state_key,
            previous_child_ranges,
            existing_state,
            from_generation,
            window,
        );
        return;
    };

    const desired_unit_descriptors = try documentExtractionUnitDescriptorsFromKeysAlloc(runtime.alloc, desired_unit_keys.items, desired_unit_fingerprints.items);
    defer runtime.alloc.free(desired_unit_descriptors);

    const new_state = try documentExtractionStateValueAlloc(runtime.alloc, source_fingerprint, desired_unit_keys.items, desired_unit_descriptors, desired_chunk_keys.items);
    defer runtime.alloc.free(new_state);

    if (existing_state) |state| {
        if (std.mem.eql(u8, state, new_state)) {
            if (existing_manifest) |value| {
                if (!(try documentExtractionManifestHasLastError(runtime.alloc, value))) {
                    runtime.skip_by_hash_count += 1;
                    return;
                }
            }
        }
    }

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer {
        for (writes.items) |write| {
            runtime.alloc.free(@constCast(write.key));
            runtime.alloc.free(@constCast(write.value));
        }
        writes.deinit(runtime.alloc);
    }

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (deletes.items) |key| runtime.alloc.free(@constCast(key));
        deletes.deinit(runtime.alloc);
    }

    var previous_state = RuntimeDocumentExtractionPreviousState{};
    defer previous_state.deinit(runtime.alloc);
    if (existing_state) |state| {
        previous_state = try loadRuntimeDocumentExtractionPreviousState(runtime, request.doc_key, artifact_name, state);
    }

    if (existing_state != null) {
        for (previous_state.unit_keys) |previous_key| {
            if (runtimeContainsConstKey(desired_unit_keys.items, previous_key)) continue;
            try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
            try appendUniqueDupeKey(runtime.alloc, &window.deleted_keys, previous_key);
        }
        for (previous_state.chunk_keys) |previous_key| {
            if (runtimeContainsConstKey(desired_chunk_keys.items, previous_key)) continue;
            try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
            try appendUniqueDupeKey(runtime.alloc, &window.deleted_keys, previous_key);
        }
    }

    const empty_units: [0]document_extraction_mod.Unit = .{};
    const streamed_extraction = document_extraction_mod.Result{
        .content_type = collect_ctx.info.content_type,
        .route_type = collect_ctx.info.route_type,
        .unsupported_reason = collect_ctx.info.unsupported_reason,
        .units = @constCast(empty_units[0..]),
    };

    const in_progress_manifest = try documentExtractionManifestPayloadAlloc(
        runtime.alloc,
        request.doc_key,
        artifact_name,
        source_url,
        source_fingerprint,
        streamed_extraction,
        unit_text_lengths.items,
        desired_unit_keys.items,
        desired_unit_descriptors,
        desired_chunk_keys.items,
        previous_child_ranges,
        previous_state.unit_keys,
        previous_state.unit_descriptors,
        previous_state.chunk_keys,
        &.{},
        from_generation,
        from_generation,
        to_generation,
        "in_progress",
        null,
    );
    defer runtime.alloc.free(in_progress_manifest);
    const in_progress_key = try runtime.alloc.dupe(u8, manifest_key);
    defer runtime.alloc.free(in_progress_key);
    const in_progress_writes = [_]KVPair{.{ .key = in_progress_key, .value = in_progress_manifest }};
    try storePutBatchWithRetry(runtime, in_progress_writes[0..], &.{});

    const text_indexes = try runtime.index_manager.textIndexesForChunk(runtime.alloc, artifact_name, request.full_text_index);
    defer {
        for (text_indexes) |name| runtime.alloc.free(name);
        runtime.alloc.free(text_indexes);
    }

    const max_window_items = generatedReplayWindowItems();
    var store_ctx = RuntimeDocumentExtractionMaterializeContext{
        .runtime = runtime,
        .config = config,
        .source_url = source_url,
        .doc_key = request.doc_key,
        .artifact_name = artifact_name,
        .info = collect_ctx.info,
        .desired_unit_keys = desired_unit_keys.items,
        .desired_unit_descriptors = desired_unit_descriptors,
        .desired_chunk_keys = desired_chunk_keys.items,
        .unit_text_lengths = unit_text_lengths.items,
        .previous_child_ranges = previous_child_ranges,
        .text_indexes = text_indexes,
        .writes = &writes,
        .deletes = &deletes,
        .window = window,
        .max_window_items = max_window_items,
        .resource_tracker = &resource_tracker,
        .generated_units = &generated_units,
        .mode = .store_artifacts,
    };
    document_extraction_mod.extractDownloadedStreaming(runtime.alloc, downloaded_mut, source_url, config, store_ctx.sink()) catch |err| {
        if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
        try writeDocumentExtractionFailureManifest(
            runtime,
            request.doc_key,
            artifact_name,
            source_url,
            source_fingerprint,
            collect_ctx.info.content_type,
            @errorName(err),
            "document extraction materialization failed",
            manifest_key,
            state_key,
            previous_child_ranges,
            existing_state,
            from_generation,
            window,
        );
        return;
    };
    try flushRuntimeKVBatchAndClear(runtime, &writes, &deletes);

    const manifest = try documentExtractionManifestPayloadAlloc(
        runtime.alloc,
        request.doc_key,
        artifact_name,
        source_url,
        source_fingerprint,
        streamed_extraction,
        unit_text_lengths.items,
        desired_unit_keys.items,
        desired_unit_descriptors,
        desired_chunk_keys.items,
        previous_child_ranges,
        previous_state.unit_keys,
        previous_state.unit_descriptors,
        previous_state.chunk_keys,
        &.{},
        to_generation,
        from_generation,
        to_generation,
        "converged",
        null,
    );
    defer runtime.alloc.free(manifest);
    try writes.append(runtime.alloc, .{
        .key = try runtime.alloc.dupe(u8, manifest_key),
        .value = try runtime.alloc.dupe(u8, manifest),
    });
    try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, manifest_key);

    try storePutBatchWithRetry(runtime, writes.items, deletes.items);
    recordArtifactBytes(runtime, .asset, manifest.len);
    clearRuntimeKVBatch(runtime, &writes, &deletes);

    var replay_ctx = RuntimeDocumentExtractionMaterializeContext{
        .runtime = runtime,
        .config = config,
        .source_url = source_url,
        .doc_key = request.doc_key,
        .artifact_name = artifact_name,
        .info = collect_ctx.info,
        .desired_unit_keys = desired_unit_keys.items,
        .desired_unit_descriptors = desired_unit_descriptors,
        .desired_chunk_keys = desired_chunk_keys.items,
        .unit_text_lengths = unit_text_lengths.items,
        .previous_child_ranges = previous_child_ranges,
        .text_indexes = text_indexes,
        .writes = &writes,
        .deletes = &deletes,
        .window = window,
        .max_window_items = max_window_items,
        .resource_tracker = &resource_tracker,
        .generated_units = &generated_units,
        .mode = .publish_replay,
    };
    try document_extraction_mod.extractDownloadedStreaming(runtime.alloc, downloaded_mut, source_url, config, replay_ctx.sink());
    try flushGeneratedReplayWindow(runtime, window);

    try writes.append(runtime.alloc, .{
        .key = try runtime.alloc.dupe(u8, state_key),
        .value = try runtime.alloc.dupe(u8, new_state),
    });
    try storePutBatchWithRetry(runtime, writes.items, deletes.items);
    clearRuntimeKVBatch(runtime, &writes, &deletes);
}

fn writeDocumentExtractionFailureManifest(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    artifact_name: []const u8,
    source_url: []const u8,
    source_fingerprint: []const u8,
    content_type: []const u8,
    error_code: []const u8,
    error_message: []const u8,
    manifest_key: []const u8,
    state_key: []const u8,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
    existing_state: ?[]const u8,
    from_generation: u64,
    window: *GeneratedReplayWindow,
) !void {
    var previous_state = RuntimeDocumentExtractionPreviousState{};
    defer previous_state.deinit(runtime.alloc);
    if (existing_state) |state| {
        previous_state = try loadRuntimeDocumentExtractionPreviousState(runtime, doc_key, artifact_name, state);
    }

    const empty_units: [0]document_extraction_mod.Unit = .{};
    const failed_extraction = document_extraction_mod.Result{
        .content_type = @constCast(content_type),
        .route_type = @constCast("error"),
        .units = @constCast(empty_units[0..]),
    };
    const to_generation = from_generation + 1;
    const manifest = try documentExtractionManifestPayloadAlloc(
        runtime.alloc,
        doc_key,
        artifact_name,
        source_url,
        source_fingerprint,
        failed_extraction,
        &.{},
        &.{},
        &.{},
        &.{},
        previous_child_ranges,
        previous_state.unit_keys,
        previous_state.unit_descriptors,
        previous_state.chunk_keys,
        previous_child_ranges,
        to_generation,
        from_generation,
        to_generation,
        "failed",
        .{ .code = error_code, .message = error_message },
    );
    defer runtime.alloc.free(manifest);

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer {
        for (writes.items) |write| {
            runtime.alloc.free(@constCast(write.key));
            runtime.alloc.free(@constCast(write.value));
        }
        writes.deinit(runtime.alloc);
    }
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (deletes.items) |key| runtime.alloc.free(@constCast(key));
        deletes.deinit(runtime.alloc);
    }

    try writes.append(runtime.alloc, .{
        .key = try runtime.alloc.dupe(u8, manifest_key),
        .value = try runtime.alloc.dupe(u8, manifest),
    });
    try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, manifest_key);

    try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, state_key));
    for (previous_state.unit_keys) |previous_key| {
        try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
        try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
        try appendUniqueDupeKey(runtime.alloc, &window.deleted_keys, previous_key);
    }
    for (previous_state.chunk_keys) |previous_key| {
        try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
        try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
        try appendUniqueDupeKey(runtime.alloc, &window.deleted_keys, previous_key);
    }

    try storePutBatchWithRetry(runtime, writes.items, deletes.items);
    recordArtifactBytes(runtime, .asset, manifest.len);
}

fn deleteDocumentExtractionForRuntime(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    artifact_name: []const u8,
    manifest_key: []const u8,
    state_key: []const u8,
    window: *GeneratedReplayWindow,
) !void {
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (deletes.items) |key| runtime.alloc.free(@constCast(key));
        deletes.deinit(runtime.alloc);
    }

    try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, manifest_key));
    try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, state_key));
    try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, manifest_key);
    try appendUniqueDupeKey(runtime.alloc, &window.artifact_delete_keys, manifest_key);

    const existing_state = storeGetAlloc(runtime, state_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => null,
    };
    defer if (existing_state) |value| runtime.alloc.free(value);
    if (existing_state) |state| {
        var previous_state = try loadRuntimeDocumentExtractionPreviousState(runtime, doc_key, artifact_name, state);
        defer previous_state.deinit(runtime.alloc);
        for (previous_state.unit_keys) |previous_key| {
            try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
            try appendUniqueDupeKey(runtime.alloc, &window.artifact_delete_keys, previous_key);
        }
        for (previous_state.chunk_keys) |previous_key| {
            try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
            try appendUniqueDupeKey(runtime.alloc, &window.artifact_delete_keys, previous_key);
        }
    }

    try storePutBatchWithRetry(runtime, &.{}, deletes.items);
}

fn completeRuntimeDocumentExtractionGeneratedText(
    runtime: *EnrichmentRuntime,
    config: document_extraction_mod.Config,
    batch_policy: GeneratedTextBatchPolicy,
    source_url: []const u8,
    source_bytes: []const u8,
    source_content_type: []const u8,
    extraction: *document_extraction_mod.Result,
) !void {
    const producer = runtime.config.asset_producer orelse return;
    try completeRuntimeDocumentExtractionGeneratedTextBatch(runtime, producer, config, batch_policy, source_url, source_bytes, extraction.route_type, source_content_type, extraction.units, .ocr);
    try completeRuntimeDocumentExtractionGeneratedTextBatch(runtime, producer, config, batch_policy, source_url, source_bytes, extraction.route_type, source_content_type, extraction.units, .transcript);
}

fn completeRuntimeDocumentExtractionGeneratedTextBatch(
    runtime: *EnrichmentRuntime,
    producer: asset_producer_mod.Producer,
    config: document_extraction_mod.Config,
    batch_policy: GeneratedTextBatchPolicy,
    source_url: []const u8,
    source_bytes: []const u8,
    route_type: []const u8,
    source_content_type: []const u8,
    units: []document_extraction_mod.Unit,
    kind: RuntimeGeneratedUnitTextKind,
) !void {
    const enabled = switch (kind) {
        .ocr => document_extraction_mod.ocrEnabledForRoute(config, route_type),
        .transcript => config.transcription_enabled,
    };
    if (!enabled) return;

    const pending_status = switch (kind) {
        .ocr => "pending_ocr",
        .transcript => "pending_transcription",
    };
    const producer_type: asset_producer_mod.ProducerType = switch (kind) {
        .ocr => .reader,
        .transcript => .transcriber,
    };
    const config_json = switch (kind) {
        .ocr => document_extraction_mod.effectiveOcrConfigJson(config),
        .transcript => config.transcription_config_json,
    };
    const method = switch (kind) {
        .ocr => "ocr_text",
        .transcript => "transcript_text",
    };

    var requests = std.ArrayListUnmanaged(asset_producer_mod.Request).empty;
    defer requests.deinit(runtime.alloc);
    var unit_indices = std.ArrayListUnmanaged(usize).empty;
    defer unit_indices.deinit(runtime.alloc);
    var parts_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        clearRuntimeGeneratedTextBatchParts(runtime.alloc, &parts_values);
        parts_values.deinit(runtime.alloc);
    }

    var batch_bytes: usize = 0;
    for (units, 0..) |unit, idx| {
        if (unit.extraction_status == null or !std.mem.eql(u8, unit.extraction_status.?, pending_status)) continue;
        var rendered_page: ?[]u8 = null;
        defer if (rendered_page) |png| runtime.alloc.free(png);
        if (kind == .ocr and std.mem.eql(u8, route_type, "pdf")) {
            rendered_page = document_extraction_mod.renderPdfPagePngAlloc(runtime.alloc, source_bytes, unit.page_number orelse 1) catch |err| {
                if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
                try markRuntimeGeneratedUnitTextFailure(runtime.alloc, &units[idx], method, kind, err);
                continue;
            };
        }
        const parts_json = if (rendered_page) |png|
            try document_extraction_mod.ocrPagePartsJsonAlloc(runtime.alloc, route_type, source_content_type, unit, png)
        else
            try runtimeDocumentGeneratedTextPartsJsonAlloc(runtime.alloc, route_type, source_content_type, unit);
        var owns_parts_json = true;
        errdefer if (owns_parts_json) runtime.alloc.free(parts_json);
        const request = asset_producer_mod.Request{
            .producer_type = producer_type,
            .config_json = config_json,
            .source_text = if (rendered_page != null) "" else source_url,
            .source_parts_json = parts_json,
            .content_type = "text/plain",
        };
        const request_bytes = runtimeGeneratedTextRequestBytes(request);
        if (requests.items.len > 0 and (requests.items.len >= batch_policy.max_items or batch_bytes + request_bytes > batch_policy.max_bytes)) {
            try flushRuntimeGeneratedTextBatch(runtime, producer, requests.items, unit_indices.items, &parts_values, units, method, kind);
            requests.clearRetainingCapacity();
            unit_indices.clearRetainingCapacity();
            batch_bytes = 0;
        }
        try parts_values.append(runtime.alloc, parts_json);
        owns_parts_json = false;
        try unit_indices.append(runtime.alloc, idx);
        try requests.append(runtime.alloc, request);
        batch_bytes = addUsizeSaturating(batch_bytes, request_bytes);
    }
    if (requests.items.len > 0) {
        try flushRuntimeGeneratedTextBatch(runtime, producer, requests.items, unit_indices.items, &parts_values, units, method, kind);
    }
}

fn runtimeGeneratedTextRequestBytes(request: asset_producer_mod.Request) usize {
    return addUsizeSaturating(
        addUsizeSaturating(request.config_json.len, request.source_text.len),
        if (request.source_parts_json) |parts| parts.len else 0,
    );
}

fn clearRuntimeGeneratedTextBatchParts(
    alloc: Allocator,
    parts_values: *std.ArrayListUnmanaged([]u8),
) void {
    for (parts_values.items) |parts_json| alloc.free(parts_json);
    parts_values.clearRetainingCapacity();
}

fn flushRuntimeGeneratedTextBatch(
    runtime: *EnrichmentRuntime,
    producer: asset_producer_mod.Producer,
    requests: []const asset_producer_mod.Request,
    unit_indices: []const usize,
    parts_values: *std.ArrayListUnmanaged([]u8),
    units: []document_extraction_mod.Unit,
    method: []const u8,
    kind: RuntimeGeneratedUnitTextKind,
) !void {
    if (requests.len == 0) return;
    if (requests.len != unit_indices.len) return error.InvalidAssetProducerResponse;

    var produced = producer.produceBatch(runtime.alloc, requests) catch |err| {
        if (isUnavailableOcrModelError(kind, err)) {
            for (unit_indices) |unit_idx| {
                try markRuntimeGeneratedUnitTextFailure(runtime.alloc, &units[unit_idx], method, kind, err);
            }
            clearRuntimeGeneratedTextBatchParts(runtime.alloc, parts_values);
            return;
        }
        if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
        return try flushRuntimeGeneratedTextBatchSequential(runtime, producer, requests, unit_indices, parts_values, units, method, kind);
    };
    if (produced.len != requests.len) {
        for (produced) |item| {
            if (item.len > 0) runtime.alloc.free(item);
        }
        runtime.alloc.free(produced);
        return try flushRuntimeGeneratedTextBatchSequential(runtime, producer, requests, unit_indices, parts_values, units, method, kind);
    }

    defer runtime.alloc.free(produced);
    errdefer {
        for (produced) |item| {
            if (item.len > 0) runtime.alloc.free(item);
        }
    }
    for (produced, unit_indices, 0..) |item, unit_idx, i| {
        produced[i] = &.{};
        applyRuntimeGeneratedUnitText(runtime.alloc, &units[unit_idx], item, method, "completed", kind) catch |err| {
            if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
            try markRuntimeGeneratedUnitTextFailure(runtime.alloc, &units[unit_idx], method, kind, err);
        };
    }
    clearRuntimeGeneratedTextBatchParts(runtime.alloc, parts_values);
}

fn flushRuntimeGeneratedTextBatchSequential(
    runtime: *EnrichmentRuntime,
    producer: asset_producer_mod.Producer,
    requests: []const asset_producer_mod.Request,
    unit_indices: []const usize,
    parts_values: *std.ArrayListUnmanaged([]u8),
    units: []document_extraction_mod.Unit,
    method: []const u8,
    kind: RuntimeGeneratedUnitTextKind,
) !void {
    if (requests.len != unit_indices.len) return error.InvalidAssetProducerResponse;
    for (requests, unit_indices) |request, unit_idx| {
        const produced = producer.produce(runtime.alloc, request) catch |err| {
            if (isUnavailableOcrModelError(kind, err)) {
                try markRuntimeGeneratedUnitTextFailure(runtime.alloc, &units[unit_idx], method, kind, err);
                continue;
            }
            if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
            try markRuntimeGeneratedUnitTextFailure(runtime.alloc, &units[unit_idx], method, kind, err);
            continue;
        };
        applyRuntimeGeneratedUnitText(runtime.alloc, &units[unit_idx], produced, method, "completed", kind) catch |err| {
            if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
            try markRuntimeGeneratedUnitTextFailure(runtime.alloc, &units[unit_idx], method, kind, err);
        };
    }
    clearRuntimeGeneratedTextBatchParts(runtime.alloc, parts_values);
}

const RuntimeGeneratedUnitTextKind = enum { ocr, transcript };

fn isUnavailableOcrModelError(kind: RuntimeGeneratedUnitTextKind, err: anyerror) bool {
    if (kind != .ocr) return false;
    return switch (err) {
        error.ModelNotFound,
        error.ModelNotSpecified,
        error.UnsupportedReaderProvider,
        => true,
        else => false,
    };
}

fn applyRuntimeGeneratedUnitText(
    alloc: Allocator,
    unit: *document_extraction_mod.Unit,
    produced: []u8,
    method: []const u8,
    status: []const u8,
    kind: RuntimeGeneratedUnitTextKind,
) !void {
    if (produced.len == 0) {
        alloc.free(produced);
        return error.EmptyGeneratedText;
    }
    defer alloc.free(produced);
    var parsed = try parseRuntimeGeneratedUnitTextOutputAlloc(alloc, produced);
    errdefer parsed.deinit(alloc);
    const owned_method = try alloc.dupe(u8, method);
    errdefer alloc.free(owned_method);
    const owned_status = try alloc.dupe(u8, status);
    errdefer alloc.free(owned_status);

    alloc.free(unit.text);
    alloc.free(unit.method);
    if (unit.extraction_status) |value| alloc.free(value);
    if (unit.extraction_warning) |value| alloc.free(value);
    unit.text = parsed.text;
    parsed.text = &.{};
    unit.method = owned_method;
    unit.extraction_status = owned_status;
    switch (kind) {
        .ocr => {
            unit.ocr_used = true;
            unit.ocr_confidence = parsed.confidence;
            unit.ocr_bbox = parsed.bbox;
        },
        .transcript => {
            unit.transcript_used = true;
            unit.transcript_confidence = parsed.confidence;
        },
    }
    unit.extraction_warning = parsed.warning;
    parsed.warning = null;
    const start = unit.char_start orelse 0;
    unit.char_start = start;
    unit.char_end = std.math.cast(u32, @as(usize, @intCast(start)) + unit.text.len);
}

fn markRuntimeGeneratedUnitTextFailure(
    alloc: Allocator,
    unit: *document_extraction_mod.Unit,
    method: []const u8,
    kind: RuntimeGeneratedUnitTextKind,
    err: anyerror,
) !void {
    const failed_status = switch (kind) {
        .ocr => "failed_ocr",
        .transcript => "failed_transcription",
    };
    const warning = try std.fmt.allocPrint(alloc, "{s} failed: {s}", .{ method, @errorName(err) });
    errdefer alloc.free(warning);
    const owned_text = try alloc.dupe(u8, "");
    errdefer alloc.free(owned_text);
    const owned_method = try alloc.dupe(u8, method);
    errdefer alloc.free(owned_method);
    const owned_status = try alloc.dupe(u8, failed_status);
    errdefer alloc.free(owned_status);

    alloc.free(unit.text);
    alloc.free(unit.method);
    if (unit.extraction_status) |value| alloc.free(value);
    if (unit.extraction_warning) |value| alloc.free(value);

    unit.text = owned_text;
    unit.method = owned_method;
    unit.extraction_status = owned_status;
    unit.extraction_warning = warning;
    switch (kind) {
        .ocr => {
            unit.ocr_used = false;
            unit.ocr_confidence = null;
            unit.ocr_bbox = null;
        },
        .transcript => {
            unit.transcript_used = false;
            unit.transcript_confidence = null;
        },
    }
    const start = unit.char_start orelse 0;
    unit.char_start = start;
    unit.char_end = @intCast(start);
}

const RuntimeParsedGeneratedUnitText = struct {
    text: []u8,
    confidence: ?f64 = null,
    bbox: ?[4]f64 = null,
    warning: ?[]u8 = null,

    fn deinit(self: *RuntimeParsedGeneratedUnitText, alloc: Allocator) void {
        if (self.text.len > 0) alloc.free(self.text);
        if (self.warning) |value| alloc.free(value);
        self.* = undefined;
    }
};

fn parseRuntimeGeneratedUnitTextOutputAlloc(alloc: Allocator, produced: []const u8) !RuntimeParsedGeneratedUnitText {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, produced, .{}) catch {
        return .{ .text = try alloc.dupe(u8, produced) };
    };
    defer parsed.deinit();
    if (parsed.value != .object) return .{ .text = try alloc.dupe(u8, produced) };
    const text_value = parsed.value.object.get("text") orelse return .{ .text = try alloc.dupe(u8, produced) };
    if (text_value != .string) return .{ .text = try alloc.dupe(u8, produced) };

    var out = RuntimeParsedGeneratedUnitText{ .text = try alloc.dupe(u8, text_value.string) };
    errdefer out.deinit(alloc);
    out.confidence = runtimeGeneratedTextJsonFloatField(parsed.value.object, "confidence");
    out.bbox = runtimeGeneratedTextJsonBboxField(parsed.value.object, "ocr_bbox") orelse runtimeGeneratedTextJsonBboxField(parsed.value.object, "bbox") orelse runtimeGeneratedTextJsonBboxField(parsed.value.object, "coordinates");
    if (runtimeGeneratedTextJsonStringField(parsed.value.object, "warning") orelse runtimeGeneratedTextJsonStringField(parsed.value.object, "extraction_warning")) |warning| {
        out.warning = try alloc.dupe(u8, warning);
    }
    return out;
}

fn runtimeGeneratedTextJsonStringField(object: std.json.ObjectMap, field: []const u8) ?[]const u8 {
    const value = object.get(field) orelse return null;
    return if (value == .string) value.string else null;
}

fn runtimeGeneratedTextJsonFloatField(object: std.json.ObjectMap, field: []const u8) ?f64 {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .float => |v| v,
        .integer => |v| @floatFromInt(v),
        else => null,
    };
}

fn runtimeGeneratedTextJsonBboxField(object: std.json.ObjectMap, field: []const u8) ?[4]f64 {
    const value = object.get(field) orelse return null;
    if (value != .array or value.array.items.len != 4) return null;
    var out: [4]f64 = undefined;
    for (value.array.items, 0..) |item, i| {
        out[i] = switch (item) {
            .float => |v| v,
            .integer => |v| @floatFromInt(v),
            else => return null,
        };
    }
    return out;
}

fn runtimeDocumentGeneratedTextPartsJsonAlloc(
    alloc: Allocator,
    route_type: []const u8,
    source_content_type: []const u8,
    unit: document_extraction_mod.Unit,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .schema = "antfly.document_generated_text_request.v1",
        .route_type = route_type,
        .source_content_type = source_content_type,
        .unit_id = unit.unit_id,
        .unit_type = unit.unit_type,
        .method = unit.method,
        .extraction_status = unit.extraction_status,
        .source_path = unit.source_path,
        .page_number = unit.page_number,
        .page_label = unit.page_label,
        .page_bbox = unit.page_bbox,
        .page_rotation = unit.page_rotation,
        .text_regions = unit.text_regions,
        .byte_length = unit.byte_length,
        .source_sha256 = unit.source_sha256,
        .ocr_confidence = unit.ocr_confidence,
        .ocr_bbox = unit.ocr_bbox,
        .transcript_confidence = unit.transcript_confidence,
        .extraction_warning = unit.extraction_warning,
    }, .{});
}

fn collectRuntimeDocumentExtractionDesiredKeys(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    artifact_name: []const u8,
    units: []const document_extraction_mod.Unit,
    desired_unit_keys: *std.ArrayListUnmanaged([]const u8),
    desired_unit_fingerprints: *std.ArrayListUnmanaged([]const u8),
    desired_chunk_keys: *std.ArrayListUnmanaged([]const u8),
) !void {
    for (units) |unit| {
        try collectRuntimeDocumentExtractionDesiredKeysForUnit(runtime, doc_key, artifact_name, unit, desired_unit_keys, desired_unit_fingerprints, desired_chunk_keys);
    }
}

fn collectRuntimeDocumentExtractionDesiredKeysForUnit(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    artifact_name: []const u8,
    unit: document_extraction_mod.Unit,
    desired_unit_keys: *std.ArrayListUnmanaged([]const u8),
    desired_unit_fingerprints: *std.ArrayListUnmanaged([]const u8),
    desired_chunk_keys: *std.ArrayListUnmanaged([]const u8),
) !void {
    try desired_unit_keys.append(runtime.alloc, try internal_keys.documentUnitArtifactKeyAlloc(runtime.alloc, doc_key, artifact_name, unit.unit_id));
    try desired_unit_fingerprints.append(runtime.alloc, try documentExtractionUnitFingerprintAlloc(runtime.alloc, unit));
    for (runtime.index_manager.enrichments.items) |entry| {
        if (entry.kind != .chunk) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, artifact_name)) continue;
        const chunks = if (entry.chunker_json.len > 0)
            try chunker_mod.chunkTextWithConfigJson(runtime.alloc, unit.text, entry.chunker_json)
        else
            try chunker_mod.chunkText(runtime.alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
        defer chunker_mod.freeChunks(runtime.alloc, chunks);
        for (chunks) |chunk| {
            try desired_chunk_keys.append(runtime.alloc, try internal_keys.documentUnitChunkArtifactKeyAlloc(runtime.alloc, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id)));
        }
    }
}

const RuntimeDocumentExtractionStreamInfo = struct {
    content_type: []u8 = &.{},
    route_type: []u8 = &.{},
    unsupported_reason: []u8 = &.{},

    fn set(self: *@This(), alloc: Allocator, info: document_extraction_mod.StreamInfo) !void {
        self.content_type = try alloc.dupe(u8, info.content_type);
        errdefer alloc.free(self.content_type);
        self.route_type = try alloc.dupe(u8, info.route_type);
        errdefer alloc.free(self.route_type);
        if (info.unsupported_reason.len > 0) {
            self.unsupported_reason = try alloc.dupe(u8, info.unsupported_reason);
        }
    }

    fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.content_type.len > 0) alloc.free(self.content_type);
        if (self.route_type.len > 0) alloc.free(self.route_type);
        if (self.unsupported_reason.len > 0) alloc.free(self.unsupported_reason);
        self.* = .{};
    }
};

const RuntimeGeneratedUnitCacheEntry = struct {
    unit_id: []u8,
    unit: document_extraction_mod.Unit,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.unit_id);
        self.unit.deinit(alloc);
        self.* = undefined;
    }
};

const RuntimeGeneratedUnitCache = struct {
    entries: std.ArrayListUnmanaged(RuntimeGeneratedUnitCacheEntry) = .empty,
    bytes: usize = 0,

    fn putClone(self: *@This(), alloc: Allocator, unit: document_extraction_mod.Unit) !void {
        for (self.entries.items) |*entry| {
            if (!std.mem.eql(u8, entry.unit_id, unit.unit_id)) continue;
            var cloned = try cloneDocumentExtractionUnit(alloc, unit);
            errdefer cloned.deinit(alloc);
            self.bytes = self.bytes -| runtimeGeneratedUnitCacheEntryBytes(entry.*);
            entry.unit.deinit(alloc);
            entry.unit = cloned;
            self.bytes = addUsizeSaturating(self.bytes, runtimeGeneratedUnitCacheEntryBytes(entry.*));
            return;
        }
        const unit_id = try alloc.dupe(u8, unit.unit_id);
        errdefer alloc.free(unit_id);
        var cloned = try cloneDocumentExtractionUnit(alloc, unit);
        errdefer cloned.deinit(alloc);
        try self.entries.append(alloc, .{
            .unit_id = unit_id,
            .unit = cloned,
        });
        self.bytes = addUsizeSaturating(self.bytes, unit_id.len + runtimeDocumentExtractionUnitOwnedBytes(cloned));
    }

    fn get(self: *const @This(), unit_id: []const u8) ?*const document_extraction_mod.Unit {
        for (self.entries.items) |*entry| {
            if (std.mem.eql(u8, entry.unit_id, unit_id)) return &entry.unit;
        }
        return null;
    }

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.entries.items) |*entry| entry.deinit(alloc);
        self.entries.deinit(alloc);
        self.* = .{};
    }
};

fn runtimeGeneratedUnitCacheEntryBytes(entry: RuntimeGeneratedUnitCacheEntry) usize {
    return addUsizeSaturating(entry.unit_id.len, runtimeDocumentExtractionUnitOwnedBytes(entry.unit));
}

fn runtimeDocumentExtractionUnitOwnedBytes(unit: document_extraction_mod.Unit) usize {
    var total = unit.unit_id.len;
    total = addUsizeSaturating(total, unit.unit_type.len);
    total = addUsizeSaturating(total, unit.text.len);
    total = addUsizeSaturating(total, unit.method.len);
    if (unit.source_path) |value| total = addUsizeSaturating(total, value.len);
    if (unit.extraction_status) |value| total = addUsizeSaturating(total, value.len);
    if (unit.source_sha256) |value| total = addUsizeSaturating(total, value.len);
    if (unit.extraction_warning) |value| total = addUsizeSaturating(total, value.len);
    if (unit.page_label) |value| total = addUsizeSaturating(total, value.len);
    total = addUsizeSaturating(total, unit.text_regions.len * @sizeOf(document_extraction_mod.TextRegion));
    return total;
}

fn cloneOptionalBytes(alloc: Allocator, value: ?[]const u8) !?[]u8 {
    return if (value) |bytes| try alloc.dupe(u8, bytes) else null;
}

fn cloneDocumentExtractionUnit(alloc: Allocator, unit: document_extraction_mod.Unit) !document_extraction_mod.Unit {
    var unit_id: ?[]u8 = try alloc.dupe(u8, unit.unit_id);
    errdefer if (unit_id) |value| alloc.free(value);
    var unit_type: ?[]u8 = try alloc.dupe(u8, unit.unit_type);
    errdefer if (unit_type) |value| alloc.free(value);
    var text: ?[]u8 = try alloc.dupe(u8, unit.text);
    errdefer if (text) |value| alloc.free(value);
    var method: ?[]u8 = try alloc.dupe(u8, unit.method);
    errdefer if (method) |value| alloc.free(value);
    var source_path = try cloneOptionalBytes(alloc, unit.source_path);
    errdefer if (source_path) |value| alloc.free(value);
    var extraction_status = try cloneOptionalBytes(alloc, unit.extraction_status);
    errdefer if (extraction_status) |value| alloc.free(value);
    var source_sha256 = try cloneOptionalBytes(alloc, unit.source_sha256);
    errdefer if (source_sha256) |value| alloc.free(value);
    var extraction_warning = try cloneOptionalBytes(alloc, unit.extraction_warning);
    errdefer if (extraction_warning) |value| alloc.free(value);
    var page_label = try cloneOptionalBytes(alloc, unit.page_label);
    errdefer if (page_label) |value| alloc.free(value);
    var text_regions: []document_extraction_mod.TextRegion = if (unit.text_regions.len > 0) try alloc.dupe(document_extraction_mod.TextRegion, unit.text_regions) else &.{};
    errdefer if (text_regions.len > 0) alloc.free(text_regions);

    const cloned = document_extraction_mod.Unit{
        .unit_id = unit_id.?,
        .unit_type = unit_type.?,
        .text = text.?,
        .method = method.?,
        .source_path = source_path,
        .extraction_status = extraction_status,
        .source_sha256 = source_sha256,
        .byte_length = unit.byte_length,
        .ocr_used = unit.ocr_used,
        .ocr_confidence = unit.ocr_confidence,
        .ocr_bbox = unit.ocr_bbox,
        .transcript_used = unit.transcript_used,
        .transcript_confidence = unit.transcript_confidence,
        .extraction_warning = extraction_warning,
        .page_number = unit.page_number,
        .page_label = page_label,
        .page_bbox = unit.page_bbox,
        .page_rotation = unit.page_rotation,
        .text_regions = text_regions,
        .char_start = unit.char_start,
        .char_end = unit.char_end,
    };
    unit_id = null;
    unit_type = null;
    text = null;
    method = null;
    source_path = null;
    extraction_status = null;
    source_sha256 = null;
    extraction_warning = null;
    page_label = null;
    text_regions = &.{};
    return cloned;
}

fn replaceDocumentExtractionUnitWithClone(alloc: Allocator, dst: *document_extraction_mod.Unit, src: document_extraction_mod.Unit) !void {
    var cloned = try cloneDocumentExtractionUnit(alloc, src);
    errdefer cloned.deinit(alloc);
    dst.deinit(alloc);
    dst.* = cloned;
}

fn runtimeGeneratedTextNeeded(config: document_extraction_mod.Config, route_type: []const u8, unit: document_extraction_mod.Unit) bool {
    return runtimeGeneratedTextKind(config, route_type, unit) != null;
}

fn runtimeGeneratedTextKind(config: document_extraction_mod.Config, route_type: []const u8, unit: document_extraction_mod.Unit) ?RuntimeGeneratedUnitTextKind {
    const status = unit.extraction_status orelse return null;
    if (document_extraction_mod.ocrEnabledForRoute(config, route_type) and std.mem.eql(u8, status, "pending_ocr")) return .ocr;
    if (config.transcription_enabled and std.mem.eql(u8, status, "pending_transcription")) return .transcript;
    return null;
}

const RuntimeDocumentExtractionCollectContext = struct {
    runtime: *EnrichmentRuntime,
    config: document_extraction_mod.Config,
    batch_policy: GeneratedTextBatchPolicy,
    source_url: []const u8,
    source_bytes: []const u8,
    doc_key: []const u8,
    artifact_name: []const u8,
    info: RuntimeDocumentExtractionStreamInfo = .{},
    desired_unit_keys: *std.ArrayListUnmanaged([]const u8),
    desired_unit_fingerprints: *std.ArrayListUnmanaged([]const u8),
    desired_chunk_keys: *std.ArrayListUnmanaged([]const u8),
    unit_text_lengths: *std.ArrayListUnmanaged(usize),
    resource_tracker: *RuntimeDocumentExtractionResourceTracker,
    generated_units: *RuntimeGeneratedUnitCache,
    pending_generated_units: std.ArrayListUnmanaged(document_extraction_mod.Unit) = .empty,
    pending_generated_kind: ?RuntimeGeneratedUnitTextKind = null,
    pending_generated_bytes: usize = 0,

    fn sink(self: *@This()) document_extraction_mod.UnitSink {
        return .{
            .ptr = self,
            .on_begin = onBegin,
            .on_unit = onUnit,
            .on_end = onEnd,
        };
    }

    fn deinit(self: *@This(), alloc: Allocator) void {
        self.info.deinit(alloc);
        self.clearPendingGeneratedUnits(alloc);
        self.pending_generated_units.deinit(alloc);
    }

    fn onBegin(ptr: *anyopaque, info: document_extraction_mod.StreamInfo) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.info.set(self.runtime.alloc, info);
    }

    fn onUnit(ptr: *anyopaque, unit: *document_extraction_mod.Unit) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtimeGeneratedTextKind(self.config, self.info.route_type, unit.*)) |kind| {
            if (self.pending_generated_kind != null and self.pending_generated_kind.? != kind) {
                try self.flushPendingGeneratedText();
            }
            self.pending_generated_kind = kind;
            const unit_bytes = runtimeDocumentExtractionUnitOwnedBytes(unit.*);
            if (self.pending_generated_units.items.len > 0 and
                addUsizeSaturating(self.pending_generated_bytes, unit_bytes) > self.batch_policy.max_bytes)
            {
                try self.flushPendingGeneratedText();
                self.pending_generated_kind = kind;
            }
            var cloned = try cloneDocumentExtractionUnit(self.runtime.alloc, unit.*);
            var owns_cloned = true;
            errdefer if (owns_cloned) cloned.deinit(self.runtime.alloc);
            try self.pending_generated_units.append(self.runtime.alloc, cloned);
            owns_cloned = false;
            self.pending_generated_bytes = addUsizeSaturating(self.pending_generated_bytes, runtimeDocumentExtractionUnitOwnedBytes(cloned));
            if (self.pending_generated_units.items.len >= self.batch_policy.max_items or self.pending_generated_bytes >= self.batch_policy.max_bytes) {
                try self.flushPendingGeneratedText();
            }
            return;
        }
        try self.flushPendingGeneratedText();
        try self.collectUnit(unit.*, false);
    }

    fn onEnd(ptr: *anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.flushPendingGeneratedText();
    }

    fn flushPendingGeneratedText(self: *@This()) !void {
        if (self.pending_generated_units.items.len == 0) return;
        const producer = self.runtime.config.asset_producer orelse return error.MissingAssetProducer;
        const kind = self.pending_generated_kind orelse return error.InvalidAssetProducerResponse;
        try completeRuntimeDocumentExtractionGeneratedTextBatch(
            self.runtime,
            producer,
            self.config,
            self.batch_policy,
            self.source_url,
            self.source_bytes,
            self.info.route_type,
            self.info.content_type,
            self.pending_generated_units.items,
            kind,
        );
        for (self.pending_generated_units.items) |unit| {
            try self.generated_units.putClone(self.runtime.alloc, unit);
            try self.collectUnit(unit, true);
        }
        self.clearPendingGeneratedUnits(self.runtime.alloc);
    }

    fn collectUnit(self: *@This(), unit: document_extraction_mod.Unit, generated: bool) !void {
        const current_unit_bytes: usize = if (generated) 0 else runtimeDocumentExtractionUnitOwnedBytes(unit);
        try self.resource_tracker.setBytes(addUsizeSaturating(
            addUsizeSaturating(self.resource_tracker.downloaded_bytes, self.generated_units.bytes),
            current_unit_bytes,
        ));
        try collectRuntimeDocumentExtractionDesiredKeysForUnit(self.runtime, self.doc_key, self.artifact_name, unit, self.desired_unit_keys, self.desired_unit_fingerprints, self.desired_chunk_keys);
        try self.unit_text_lengths.append(self.runtime.alloc, unit.text.len);
    }

    fn clearPendingGeneratedUnits(self: *@This(), alloc: Allocator) void {
        for (self.pending_generated_units.items) |*unit| unit.deinit(alloc);
        self.pending_generated_units.clearRetainingCapacity();
        self.pending_generated_kind = null;
        self.pending_generated_bytes = 0;
    }
};

const runtime_document_extraction_flush_write_count: usize = 128;
const runtime_document_extraction_flush_write_bytes: usize = 4 * 1024 * 1024;
const RuntimeDocumentExtractionMaterializeMode = enum { store_artifacts, publish_replay };

const RuntimeDocumentExtractionResourceTracker = struct {
    manager: ?*resource_manager_mod.ResourceManager,
    current_bytes: u64 = 0,
    downloaded_bytes: usize = 0,

    fn init(runtime: *EnrichmentRuntime) @This() {
        return .{ .manager = runtime.config.resource_manager orelse runtime.index_manager.resource_manager };
    }

    fn setDownloadedBytes(self: *@This(), bytes: usize) !void {
        self.downloaded_bytes = bytes;
        try self.setBytes(bytes);
    }

    fn updateWorkingSet(
        self: *@This(),
        unit_bytes: usize,
        generated_cache_bytes: usize,
        writes: []const KVPair,
        window: *const GeneratedReplayWindow,
    ) !void {
        var total = self.downloaded_bytes;
        total = addUsizeSaturating(total, unit_bytes);
        total = addUsizeSaturating(total, generated_cache_bytes);
        total = addUsizeSaturating(total, runtimeDocumentExtractionWriteBytes(writes));
        total = addUsizeSaturating(total, runtimeDocumentExtractionWindowBytes(window));
        try self.setBytes(total);
    }

    fn observeWorkingSet(
        self: *@This(),
        unit_bytes: usize,
        generated_cache_bytes: usize,
        writes: []const KVPair,
        window: *const GeneratedReplayWindow,
    ) void {
        var total = self.downloaded_bytes;
        total = addUsizeSaturating(total, unit_bytes);
        total = addUsizeSaturating(total, generated_cache_bytes);
        total = addUsizeSaturating(total, runtimeDocumentExtractionWriteBytes(writes));
        total = addUsizeSaturating(total, runtimeDocumentExtractionWindowBytes(window));
        self.observeBytes(total);
    }

    fn setBytes(self: *@This(), bytes: usize) !void {
        const manager = self.manager orelse return;
        const next = std.math.cast(u64, bytes) orelse return error.ResourceBudgetExceeded;
        const stats = manager.sliceStats(.document_extraction_working_set);
        if (stats.hard_limit_bytes > 0 and next > stats.hard_limit_bytes) {
            return error.DocumentExtractionWorkingSetTooLarge;
        }
        try manager.adjustUsage(.document_extraction_working_set, &self.current_bytes, next);
    }

    fn observeBytes(self: *@This(), bytes: usize) void {
        const manager = self.manager orelse return;
        const next = std.math.cast(u64, bytes) orelse std.math.maxInt(u64);
        manager.observeUsage(.document_extraction_working_set, &self.current_bytes, next);
    }

    fn deinit(self: *@This()) void {
        if (self.manager) |manager| {
            manager.observeUsage(.document_extraction_working_set, &self.current_bytes, 0);
        }
    }
};

test "document extraction working set accounts generated unit cache bytes" {
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.document_extraction_working_set)] = .{
        .soft_limit_bytes = 0,
        .hard_limit_bytes = 100,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = RuntimeDocumentExtractionResourceTracker{
        .manager = &manager,
        .downloaded_bytes = 10,
    };
    defer tracker.deinit();

    var window = GeneratedReplayWindow{ .alloc = std.testing.allocator };
    defer window.deinit();

    try tracker.updateWorkingSet(40, 0, &.{}, &window);
    try std.testing.expectError(error.DocumentExtractionWorkingSetTooLarge, tracker.updateWorkingSet(40, 60, &.{}, &window));
}

fn addUsizeSaturating(a: usize, b: usize) usize {
    return std.math.add(usize, a, b) catch std.math.maxInt(usize);
}

fn runtimeDocumentExtractionWriteBytes(writes: []const KVPair) usize {
    var total: usize = 0;
    for (writes) |write| total += write.key.len + write.value.len;
    return total;
}

fn runtimeDocumentExtractionWindowBytes(window: *const GeneratedReplayWindow) usize {
    var total: usize = 0;
    for (window.documents.items) |doc| {
        total = addUsizeSaturating(total, doc.key.len);
        if (doc.cleaned_value) |value| total = addUsizeSaturating(total, value.len);
        for (doc.targets) |target| total = addUsizeSaturating(total, target.index_name.len);
    }
    for (window.deleted_keys.items) |key| total = addUsizeSaturating(total, key.len);
    for (window.changed_artifact_keys.items) |key| total = addUsizeSaturating(total, key.len);
    for (window.dense_embeddings.items) |embedding| {
        total = addUsizeSaturating(total, embedding.index_name.len);
        total = addUsizeSaturating(total, embedding.doc_key.len);
        if (embedding.parent_doc_key) |key| total = addUsizeSaturating(total, key.len);
        total = addUsizeSaturating(total, embedding.vector.len * @sizeOf(f32));
        if (embedding.artifact_key) |key| total = addUsizeSaturating(total, key.len);
    }
    for (window.sparse_embeddings.items) |embedding| {
        total = addUsizeSaturating(total, embedding.index_name.len);
        total = addUsizeSaturating(total, embedding.doc_key.len);
        total = addUsizeSaturating(total, embedding.indices.len * @sizeOf(u32));
        total = addUsizeSaturating(total, embedding.values.len * @sizeOf(f32));
        if (embedding.artifact_key) |key| total = addUsizeSaturating(total, key.len);
    }
    return total;
}

fn clearRuntimeKVBatch(runtime: *EnrichmentRuntime, writes: *std.ArrayListUnmanaged(KVPair), deletes: *std.ArrayListUnmanaged([]const u8)) void {
    for (writes.items) |write| {
        runtime.alloc.free(@constCast(write.key));
        runtime.alloc.free(@constCast(write.value));
    }
    writes.clearRetainingCapacity();
    for (deletes.items) |key| runtime.alloc.free(@constCast(key));
    deletes.clearRetainingCapacity();
}

fn flushRuntimeKVBatchAndClear(
    runtime: *EnrichmentRuntime,
    writes: *std.ArrayListUnmanaged(KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
) !void {
    if (writes.items.len == 0 and deletes.items.len == 0) return;
    try storePutBatchWithRetry(runtime, writes.items, deletes.items);
    clearRuntimeKVBatch(runtime, writes, deletes);
}

const RuntimeDocumentExtractionMaterializeContext = struct {
    runtime: *EnrichmentRuntime,
    config: document_extraction_mod.Config,
    source_url: []const u8,
    doc_key: []const u8,
    artifact_name: []const u8,
    info: RuntimeDocumentExtractionStreamInfo,
    desired_unit_keys: []const []const u8,
    desired_unit_descriptors: []const DocumentExtractionUnitDescriptor,
    desired_chunk_keys: []const []const u8,
    unit_text_lengths: []const usize,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
    text_indexes: []const []const u8,
    writes: *std.ArrayListUnmanaged(KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    window: *GeneratedReplayWindow,
    max_window_items: usize,
    resource_tracker: *RuntimeDocumentExtractionResourceTracker,
    generated_units: *const RuntimeGeneratedUnitCache,
    mode: RuntimeDocumentExtractionMaterializeMode,
    unit_index: usize = 0,
    chunk_range_base_index: usize = 0,

    fn sink(self: *@This()) document_extraction_mod.UnitSink {
        self.chunk_range_base_index = documentExtractionUnitRangeCountFromTextLengths(self.unit_text_lengths);
        return .{
            .ptr = self,
            .on_begin = onBegin,
            .on_unit = onUnit,
            .on_end = onEnd,
        };
    }

    fn onBegin(_: *anyopaque, _: document_extraction_mod.StreamInfo) anyerror!void {}

    fn accountWorkingSet(self: *@This(), unit_bytes: usize, generated_cache_bytes: usize) !void {
        if (self.mode == .store_artifacts) {
            try self.resource_tracker.updateWorkingSet(unit_bytes, generated_cache_bytes, self.writes.items, self.window);
        } else {
            self.resource_tracker.observeWorkingSet(unit_bytes, generated_cache_bytes, self.writes.items, self.window);
        }
    }

    fn onUnit(ptr: *anyopaque, unit: *document_extraction_mod.Unit) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtimeGeneratedTextNeeded(self.config, self.info.route_type, unit.*)) {
            const cached = self.generated_units.get(unit.unit_id) orelse return error.MissingGeneratedUnitCache;
            try replaceDocumentExtractionUnitWithClone(self.runtime.alloc, unit, cached.*);
        }
        const unit_working_bytes = runtimeDocumentExtractionUnitOwnedBytes(unit.*);
        const generated_cache_bytes = self.generated_units.bytes;
        try self.accountWorkingSet(unit_working_bytes, generated_cache_bytes);

        const unit_key = self.desired_unit_keys[self.unit_index];
        const unit_range_id = try documentExtractionRangeIdAlloc(self.runtime.alloc, documentExtractionUnitRangeIndexFromTextLengths(self.unit_text_lengths, self.unit_index));
        defer self.runtime.alloc.free(unit_range_id);
        const unit_route = documentExtractionRangeRoute(self.previous_child_ranges, unit_range_id, "unit", self.artifact_name);
        const payload = try documentUnitPayloadAlloc(self.runtime.alloc, self.doc_key, self.artifact_name, unit.*, self.source_url, self.info.content_type, unit_route);
        defer self.runtime.alloc.free(payload);

        if (self.mode == .store_artifacts) {
            try self.writes.append(self.runtime.alloc, .{
                .key = try self.runtime.alloc.dupe(u8, unit_key),
                .value = try self.runtime.alloc.dupe(u8, payload),
            });
        } else {
            try appendUniqueDupeKey(self.runtime.alloc, &self.window.changed_artifact_keys, unit_key);
        }

        if (self.mode == .publish_replay and self.text_indexes.len > 0) {
            var targets = try self.runtime.alloc.alloc(derived_types.DerivedTargetRef, self.text_indexes.len);
            errdefer {
                for (targets) |target| self.runtime.alloc.free(@constCast(target.index_name));
                self.runtime.alloc.free(targets);
            }
            for (self.text_indexes, 0..) |index_name, i| {
                targets[i] = .{
                    .kind = .full_text,
                    .index_name = try self.runtime.alloc.dupe(u8, index_name),
                };
            }
            try self.window.documents.append(self.runtime.alloc, .{
                .key = try self.runtime.alloc.dupe(u8, unit_key),
                .action = .upsert,
                .cleaned_value = try self.runtime.alloc.dupe(u8, payload),
                .targets = targets,
            });
        }

        try appendRuntimeDocumentUnitChunkWrites(self.runtime, self.doc_key, self.artifact_name, unit_key, unit.*, self.desired_chunk_keys, self.chunk_range_base_index, self.previous_child_ranges, self.writes, self.window, self.mode);
        self.unit_index += 1;
        try self.accountWorkingSet(unit_working_bytes, generated_cache_bytes);

        if (self.mode == .store_artifacts and (self.writes.items.len >= runtime_document_extraction_flush_write_count or
            runtimeDocumentExtractionWriteBytes(self.writes.items) >= runtime_document_extraction_flush_write_bytes))
        {
            try flushRuntimeKVBatchAndClear(self.runtime, self.writes, self.deletes);
            try self.accountWorkingSet(unit_working_bytes, generated_cache_bytes);
        }
        if (self.mode == .publish_replay) {
            try flushGeneratedReplayWindowIfNeeded(self.runtime, self.window, self.max_window_items);
        }
        try self.accountWorkingSet(unit_working_bytes, generated_cache_bytes);
    }

    fn onEnd(_: *anyopaque) anyerror!void {}
};

fn appendRuntimeDocumentUnitChunkWrites(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    source_artifact_name: []const u8,
    unit_key: []const u8,
    unit: document_extraction_mod.Unit,
    desired_chunk_keys: []const []const u8,
    chunk_range_base_index: usize,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
    writes: *std.ArrayListUnmanaged(KVPair),
    window: *GeneratedReplayWindow,
    mode: RuntimeDocumentExtractionMaterializeMode,
) !void {
    for (runtime.index_manager.enrichments.items) |entry| {
        if (entry.kind != .chunk) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, source_artifact_name)) continue;
        const chunks = if (entry.chunker_json.len > 0)
            try chunker_mod.chunkTextWithConfigJson(runtime.alloc, unit.text, entry.chunker_json)
        else
            try chunker_mod.chunkText(runtime.alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
        defer chunker_mod.freeChunks(runtime.alloc, chunks);
        if (chunks.len == 0) continue;

        const include_default_full_text = entry.full_text_index or
            try chunking_types_mod.parseHasFullTextIndexFromSlice(runtime.alloc, entry.chunker_json);
        const text_indexes = try runtime.index_manager.textIndexesForChunk(runtime.alloc, entry.name, include_default_full_text);
        defer {
            for (text_indexes) |name| runtime.alloc.free(name);
            runtime.alloc.free(text_indexes);
        }

        var arena_state = std.heap.ArenaAllocator.init(runtime.alloc);
        defer arena_state.deinit();
        const scratch = arena_state.allocator();

        for (chunks) |chunk| {
            if (!chunk.isText()) continue;
            const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(runtime.alloc, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id));
            defer runtime.alloc.free(chunk_key);
            const chunk_key_index = documentExtractionKeyIndex(desired_chunk_keys, chunk_key) orelse return error.DocumentExtractionChunkRangeMissing;
            const chunk_range_id = try documentExtractionRangeIdAlloc(scratch, chunk_range_base_index + (chunk_key_index / document_extraction_range_target_children));
            const chunk_route = documentExtractionRangeRoute(previous_child_ranges, chunk_range_id, "chunk", "derived_chunks");
            const payload = try buildDocumentUnitChunkPayloadAlloc(scratch, doc_key, unit_key, entry.name, source_artifact_name, entry.source_field, unit, chunk, true, chunk_route);
            if (mode == .store_artifacts) {
                try writes.append(runtime.alloc, .{
                    .key = try runtime.alloc.dupe(u8, chunk_key),
                    .value = try runtime.alloc.dupe(u8, payload),
                });
            } else {
                try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, chunk_key);
            }

            if (mode == .publish_replay and text_indexes.len > 0) {
                var targets = try runtime.alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
                errdefer {
                    for (targets) |target| runtime.alloc.free(@constCast(target.index_name));
                    runtime.alloc.free(targets);
                }
                for (text_indexes, 0..) |index_name, i| {
                    targets[i] = .{
                        .kind = .full_text,
                        .index_name = try runtime.alloc.dupe(u8, index_name),
                    };
                }
                try window.documents.append(runtime.alloc, .{
                    .key = try runtime.alloc.dupe(u8, chunk_key),
                    .action = .upsert,
                    .cleaned_value = try runtime.alloc.dupe(u8, payload),
                    .targets = targets,
                });
            }

            _ = arena_state.reset(.retain_capacity);
        }
    }
}

fn buildDocumentUnitChunkPayloadAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    unit_key: []const u8,
    artifact_name: []const u8,
    source_artifact_name: []const u8,
    source_field: []const u8,
    unit: document_extraction_mod.Unit,
    chunk: chunker_mod.Chunk,
    include_payload: bool,
    route: DocumentExtractionRangeRoute,
) ![]u8 {
    const owner_group_id = std.math.cast(i64, route.owner_group_id) orelse return error.InvalidDocumentExtractionManifest;
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, try alloc.dupe(u8, "_parent_doc_key"), .{ .string = try alloc.dupe(u8, doc_key) });
    try obj.put(alloc, try alloc.dupe(u8, "_parent_unit_key"), .{ .string = try alloc.dupe(u8, unit_key) });
    try obj.put(alloc, try alloc.dupe(u8, "_parent_unit_id"), .{ .string = try alloc.dupe(u8, unit.unit_id) });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_name"), .{ .string = try alloc.dupe(u8, artifact_name) });
    try obj.put(alloc, try alloc.dupe(u8, "_source_artifact_name"), .{ .string = try alloc.dupe(u8, source_artifact_name) });
    try obj.put(alloc, try alloc.dupe(u8, "_source_field"), .{ .string = try alloc.dupe(u8, source_field) });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_range_id"), .{ .string = try alloc.dupe(u8, route.range_id) });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_range_kind"), .{ .string = try alloc.dupe(u8, "chunk") });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_route_status"), .{ .string = try alloc.dupe(u8, route.route_status) });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_owner_group_id"), .{ .integer = owner_group_id });
    try chunk_artifact_mod.appendArtifactFieldsWithProvenance(alloc, &obj, source_field, chunk, include_payload, .{
        .scope = .unit,
        .parent_doc_key = doc_key,
        .parent_unit_key = unit_key,
        .parent_unit_id = unit.unit_id,
        .source_artifact_name = source_artifact_name,
        .document_char_base = unit.char_start,
        .page_number = unit.page_number,
        .page_label = unit.page_label,
        .page_bbox = unit.page_bbox,
        .page_rotation = unit.page_rotation,
        .extraction_method = unit.method,
        .extraction_status = unit.extraction_status,
        .confidence = documentUnitConfidence(unit),
        .ocr_used = unit.ocr_used,
        .ocr_confidence = unit.ocr_confidence,
        .ocr_bbox = unit.ocr_bbox,
        .transcript_used = unit.transcript_used,
        .transcript_confidence = unit.transcript_confidence,
        .extraction_warning = unit.extraction_warning,
    });
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = obj }, .{});
}

fn materializeGraphAssetForRuntime(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    value: []const u8,
    raw_doc: []const u8,
    window: *GeneratedReplayWindow,
) !void {
    if (!runtime.index_manager.hasGraphIndexes()) return;
    const artifact_name = requestArtifactName(request);

    for (runtime.index_manager.graphIndexes()) |graph_entry| {
        const source = graph_entry.artifact_source orelse continue;
        if (!std.mem.eql(u8, source.artifact_name, artifact_name)) continue;

        const graph_writes = try runtimeGraphWritesFromArtifactValueAlloc(runtime.alloc, graph_entry.config.name, request.doc_key, value, source, request.content_type, raw_doc);
        defer runtimeFreeGraphWrites(runtime.alloc, graph_writes);

        var writes = std.ArrayListUnmanaged(KVPair).empty;
        defer {
            for (writes.items) |write| {
                runtime.alloc.free(@constCast(write.key));
                runtime.alloc.free(@constCast(write.value));
            }
            writes.deinit(runtime.alloc);
        }
        for (graph_writes) |write| {
            const key = try internal_keys.graphEdgeArtifactKeyAlloc(runtime.alloc, write.source, write.index_name, write.edge_type, write.target);
            var key_owned = true;
            errdefer if (key_owned) runtime.alloc.free(key);
            const payload = try enrichment_artifact_codec.encodeGraphEdgeAlloc(runtime.alloc, null, write.weight, write.created_at, write.updated_at, write.metadata_json);
            var payload_owned = true;
            errdefer if (payload_owned) runtime.alloc.free(payload);
            try writes.append(runtime.alloc, .{ .key = key, .value = payload });
            key_owned = false;
            payload_owned = false;
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
        }

        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (deletes.items) |key| runtime.alloc.free(@constCast(key));
            deletes.deinit(runtime.alloc);
        }

        const state_key = try graphAssetStateKeyAlloc(runtime.alloc, request.doc_key, graph_entry.config.name, artifact_name);
        defer runtime.alloc.free(state_key);
        if (try loadGraphAssetStateKeysAlloc(runtime, state_key)) |previous_keys| {
            defer freeOwnedConstKeySlice(runtime.alloc, previous_keys);
            for (previous_keys) |previous_key| {
                if (runtimeContainsKVKey(writes.items, previous_key)) continue;
                try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
                try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
            }
        } else {
            const protected_keys = try runtimeResolutionMentionStateKeysForGraphSourceAlloc(runtime, request.doc_key, graph_entry.config.name, source);
            defer freeOwnedConstKeySlice(runtime.alloc, protected_keys);
            const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(runtime.alloc, request.doc_key, graph_entry.config.name);
            defer runtime.alloc.free(prefix);
            const existing = try backend_scan.scanPrefix(runtime.alloc, &runtime.store, prefix);
            defer backend_scan.freeResults(runtime.alloc, existing);
            for (existing) |entry| {
                if (runtimeContainsKVKey(writes.items, entry.key)) continue;
                if (runtimeContainsConstKey(protected_keys, entry.key)) continue;
                try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, entry.key));
                try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, entry.key);
            }
        }

        const state_value = try encodeGraphAssetStateKeysAlloc(runtime.alloc, writes.items);
        var state_owned = true;
        defer if (state_owned) runtime.alloc.free(state_value);
        try writes.append(runtime.alloc, .{
            .key = try runtime.alloc.dupe(u8, state_key),
            .value = state_value,
        });
        state_owned = false;

        if (writes.items.len > 0 or deletes.items.len > 0) {
            try storePutBatchWithRetry(runtime, writes.items, deletes.items);
        }
    }
}

fn materializeGraphAssetDeleteForRuntime(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    window: *GeneratedReplayWindow,
) !void {
    if (!runtime.index_manager.hasGraphIndexes()) return;
    const artifact_name = requestArtifactName(request);

    for (runtime.index_manager.graphIndexes()) |graph_entry| {
        const source = graph_entry.artifact_source orelse continue;
        if (!std.mem.eql(u8, source.artifact_name, artifact_name)) continue;

        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (deletes.items) |key| runtime.alloc.free(@constCast(key));
            deletes.deinit(runtime.alloc);
        }

        const state_key = try graphAssetStateKeyAlloc(runtime.alloc, request.doc_key, graph_entry.config.name, artifact_name);
        defer runtime.alloc.free(state_key);
        if (try loadGraphAssetStateKeysAlloc(runtime, state_key)) |previous_keys| {
            defer freeOwnedConstKeySlice(runtime.alloc, previous_keys);
            for (previous_keys) |previous_key| {
                try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
                try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
            }
        } else {
            const protected_keys = try runtimeResolutionMentionStateKeysForGraphSourceAlloc(runtime, request.doc_key, graph_entry.config.name, source);
            defer freeOwnedConstKeySlice(runtime.alloc, protected_keys);
            const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(runtime.alloc, request.doc_key, graph_entry.config.name);
            defer runtime.alloc.free(prefix);
            const existing = try backend_scan.scanPrefix(runtime.alloc, &runtime.store, prefix);
            defer backend_scan.freeResults(runtime.alloc, existing);
            for (existing) |entry| {
                if (runtimeContainsConstKey(protected_keys, entry.key)) continue;
                try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, entry.key));
                try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, entry.key);
            }
        }

        const state_value = try encodeGraphAssetStateKeysAlloc(runtime.alloc, &.{});
        defer runtime.alloc.free(state_value);
        const writes = [_]KVPair{.{ .key = state_key, .value = state_value }};
        if (writes.len > 0 or deletes.items.len > 0) {
            try storePutBatchWithRetry(runtime, &writes, deletes.items);
        }
    }
}

fn runtimeContainsKVKey(items: []const KVPair, key: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item.key, key)) return true;
    }
    return false;
}

fn runtimeContainsConstKey(items: []const []const u8, key: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, key)) return true;
    }
    return false;
}

fn runtimeGraphWritesFromArtifactValueAlloc(
    alloc: Allocator,
    index_name: []const u8,
    doc_key: []const u8,
    raw: []const u8,
    source: index_manager_mod.GraphArtifactSource,
    artifact_content_type: []const u8,
    raw_doc: ?[]const u8,
) ![]types.GraphEdgeWrite {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    var parsed_doc = if (raw_doc) |doc| try std.json.parseFromSlice(std.json.Value, alloc, doc, .{}) else null;
    defer if (parsed_doc) |*doc| doc.deinit();
    const doc_value: ?std.json.Value = if (parsed_doc) |doc| doc.value else null;

    var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    errdefer runtimeFreeGraphWrites(alloc, writes.items);

    switch (source.format) {
        .extraction_relation => try runtimeAppendRelationItemsFromPath(alloc, &writes, index_name, doc_key, doc_value, parsed.value, source.path, source.mapping, source.artifact_name, artifact_content_type, parsed.value),
        .extraction_graph => {
            if (source.path.len > 0) {
                try runtimeAppendRelationItemsFromPath(alloc, &writes, index_name, doc_key, doc_value, parsed.value, source.path, source.mapping, source.artifact_name, artifact_content_type, parsed.value);
            } else if (parsed.value == .object) {
                if (parsed.value.object.get("relations")) |relations| try runtimeAppendRelationValueItems(alloc, &writes, index_name, doc_key, doc_value, relations, source.mapping, source.artifact_name, artifact_content_type, parsed.value);
                if (parsed.value.object.get("edges")) |edges| try runtimeAppendRelationValueItems(alloc, &writes, index_name, doc_key, doc_value, edges, source.mapping, source.artifact_name, artifact_content_type, parsed.value);
            }
        },
    }

    return try writes.toOwnedSlice(alloc);
}

fn runtimeFreeGraphWrites(alloc: Allocator, writes: []types.GraphEdgeWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.index_name));
        alloc.free(@constCast(write.source));
        alloc.free(@constCast(write.target));
        alloc.free(@constCast(write.edge_type));
        if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
    }
    if (writes.len > 0) alloc.free(writes);
}

fn runtimeAppendRelationItemsFromPath(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    index_name: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    root: std.json.Value,
    path: []const u8,
    mapping: index_manager_mod.GraphArtifactMapping,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) !void {
    if (path.len == 0 or std.mem.eql(u8, path, "$")) return runtimeAppendRelationValueItems(alloc, writes, index_name, doc_key, doc_value, root, mapping, artifact_name, artifact_content_type, artifact_value);
    const selected = runtimeSelectGraphArtifactPath(root, path) orelse return;
    try runtimeAppendRelationValueItems(alloc, writes, index_name, doc_key, doc_value, selected, mapping, artifact_name, artifact_content_type, artifact_value);
}

fn runtimeSelectGraphArtifactPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var trimmed = path;
    if (std.mem.startsWith(u8, trimmed, "$.")) trimmed = trimmed[2..];
    if (std.mem.endsWith(u8, trimmed, "[*]")) trimmed = trimmed[0 .. trimmed.len - 3];
    if (trimmed.len == 0) return root;

    var current = root;
    var parts = std.mem.splitScalar(u8, trimmed, '.');
    while (parts.next()) |part| {
        if (part.len == 0) return null;
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

fn runtimeAppendRelationValueItems(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    index_name: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    value: std.json.Value,
    mapping: index_manager_mod.GraphArtifactMapping,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) !void {
    if (value == .array) {
        for (value.array.items, 0..) |item, i| try runtimeAppendRelationItem(alloc, writes, index_name, doc_key, doc_value, item, i, mapping, artifact_name, artifact_content_type, artifact_value);
    } else {
        try runtimeAppendRelationItem(alloc, writes, index_name, doc_key, doc_value, value, 0, mapping, artifact_name, artifact_content_type, artifact_value);
    }
}

fn runtimeAppendRelationItem(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    index_name: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    mapping: index_manager_mod.GraphArtifactMapping,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) !void {
    if (item != .object) return;
    const mapped_edge_type = if (mapping.edge_type_template.len > 0)
        try runtimeRenderGraphArtifactTemplateAlloc(alloc, mapping.edge_type_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_edge_type) |value| alloc.free(value);
    const edge_type = if (mapped_edge_type) |value|
        std.mem.trim(u8, value, &std.ascii.whitespace)
    else
        runtimeJsonStringField(item, "type") orelse runtimeJsonStringField(item, "edge_type") orelse runtimeJsonStringField(item, "relation") orelse return;
    if (edge_type.len == 0) return;

    const mapped_source = if (mapping.source_template.len > 0)
        try runtimeRenderGraphArtifactTemplateAlloc(alloc, mapping.source_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_source) |value| alloc.free(value);
    const source_doc = if (mapped_source) |value| blk: {
        const trimmed = std.mem.trim(u8, value, &std.ascii.whitespace);
        break :blk if (trimmed.len > 0) trimmed else doc_key;
    } else if (item.object.get("source")) |source_value|
        runtimeJsonEndpointDocumentIdResolved(source_value, artifact_value) orelse doc_key
    else
        doc_key;

    const mapped_target = if (mapping.target_template.len > 0)
        try runtimeRenderGraphArtifactTemplateAlloc(alloc, mapping.target_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_target) |value| alloc.free(value);
    const target_doc = if (mapped_target) |value| blk: {
        const trimmed = std.mem.trim(u8, value, &std.ascii.whitespace);
        if (trimmed.len == 0) return;
        break :blk trimmed;
    } else blk: {
        const target_value = item.object.get("target") orelse return;
        break :blk runtimeJsonEndpointDocumentIdResolved(target_value, artifact_value) orelse return;
    };

    const weight = if (mapping.weight_template.len > 0) blk: {
        const rendered = try runtimeRenderGraphArtifactTemplateAlloc(alloc, mapping.weight_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        defer alloc.free(rendered);
        const trimmed = std.mem.trim(u8, rendered, &std.ascii.whitespace);
        break :blk if (trimmed.len > 0) try std.fmt.parseFloat(f64, trimmed) else 1.0;
    } else runtimeJsonFloatField(item, "weight") orelse runtimeJsonFloatField(item, "confidence") orelse 1.0;
    const metadata_json = if (mapping.metadata_template_json.len > 0)
        try runtimeRenderGraphArtifactMetadataTemplateAlloc(alloc, mapping.metadata_template_json, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        try std.json.Stringify.valueAlloc(alloc, item, .{});
    errdefer alloc.free(metadata_json);

    try writes.append(alloc, .{
        .index_name = try alloc.dupe(u8, index_name),
        .source = try alloc.dupe(u8, source_doc),
        .target = try alloc.dupe(u8, target_doc),
        .edge_type = try alloc.dupe(u8, edge_type),
        .weight = weight,
        .metadata_json = metadata_json,
    });
}

fn runtimeRenderGraphArtifactTemplateAlloc(
    alloc: Allocator,
    template_source: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var pos: usize = 0;
    while (pos < template_source.len) {
        const start = std.mem.indexOfPos(u8, template_source, pos, "{{") orelse {
            try out.appendSlice(alloc, template_source[pos..]);
            break;
        };
        try out.appendSlice(alloc, template_source[pos..start]);
        const body_start = start + 2;
        const end = std.mem.indexOfPos(u8, template_source, body_start, "}}") orelse {
            try out.appendSlice(alloc, template_source[start..]);
            break;
        };
        const expr = std.mem.trim(u8, template_source[body_start..end], &std.ascii.whitespace);
        const rendered = try runtimeRenderGraphArtifactExpressionAlloc(alloc, expr, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
        pos = end + 2;
    }
    return try out.toOwnedSlice(alloc);
}

fn runtimeRenderGraphArtifactExpressionAlloc(
    alloc: Allocator,
    expr: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ![]u8 {
    if (std.mem.startsWith(u8, expr, "default ")) {
        var parts = std.mem.tokenizeAny(u8, expr["default ".len..], &std.ascii.whitespace);
        const path = parts.next() orelse return try alloc.dupe(u8, "");
        const fallback = parts.next() orelse "";
        const value = runtimeGraphTemplateValue(path, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        const text = if (value) |found| try runtimeGraphJsonValueTextAlloc(alloc, found) else try alloc.dupe(u8, fallback);
        if (std.mem.trim(u8, text, &std.ascii.whitespace).len == 0 and fallback.len > 0) {
            alloc.free(text);
            return try alloc.dupe(u8, fallback);
        }
        return text;
    }
    if (runtimeGraphTemplateValue(expr, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)) |value| {
        return try runtimeGraphJsonValueTextAlloc(alloc, value);
    }
    return try alloc.dupe(u8, "");
}

fn runtimeGraphTemplateValue(
    path: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ?std.json.Value {
    if (std.mem.eql(u8, path, "_doc.key")) return .{ .string = doc_key };
    if (std.mem.startsWith(u8, path, "_doc.value.")) {
        const doc = doc_value orelse return null;
        return runtimeSelectJsonDotPath(doc, path["_doc.value.".len..]);
    }
    if (std.mem.eql(u8, path, "_artifact.name")) return .{ .string = artifact_name };
    if (std.mem.eql(u8, path, "_artifact.content_type")) return .{ .string = artifact_content_type };
    if (std.mem.eql(u8, path, "_artifact.value")) return artifact_value;
    if (std.mem.startsWith(u8, path, "_artifact.value.")) return runtimeSelectJsonDotPath(artifact_value, path["_artifact.value.".len..]);
    if (std.mem.eql(u8, path, "_item_index")) return .{ .integer = @intCast(item_index) };
    if (std.mem.eql(u8, path, "_item")) return item;
    if (std.mem.startsWith(u8, path, "_item.")) return runtimeSelectGraphItemDotPath(item, path["_item.".len..], artifact_value);
    return null;
}

fn runtimeSelectGraphItemDotPath(item: std.json.Value, path: []const u8, artifact_value: std.json.Value) ?std.json.Value {
    if (std.mem.eql(u8, path, "source") or std.mem.startsWith(u8, path, "source.")) {
        if (item != .object) return null;
        const endpoint = item.object.get("source") orelse return null;
        const selected = runtimeResolveGraphEndpointEntity(endpoint, artifact_value) orelse endpoint;
        if (std.mem.eql(u8, path, "source")) return selected;
        return runtimeSelectJsonDotPath(selected, path["source.".len..]);
    }
    if (std.mem.eql(u8, path, "target") or std.mem.startsWith(u8, path, "target.")) {
        if (item != .object) return null;
        const endpoint = item.object.get("target") orelse return null;
        const selected = runtimeResolveGraphEndpointEntity(endpoint, artifact_value) orelse endpoint;
        if (std.mem.eql(u8, path, "target")) return selected;
        return runtimeSelectJsonDotPath(selected, path["target.".len..]);
    }
    return runtimeSelectJsonDotPath(item, path);
}

fn runtimeSelectJsonDotPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var current = root;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0) return null;
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

fn runtimeGraphJsonValueTextAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .null => try alloc.dupe(u8, ""),
        .bool => |b| try alloc.dupe(u8, if (b) "true" else "false"),
        .integer => |n| try std.fmt.allocPrint(alloc, "{d}", .{n}),
        .float => |n| try std.fmt.allocPrint(alloc, "{d}", .{n}),
        .number_string => |s| try alloc.dupe(u8, s),
        .string => |s| try alloc.dupe(u8, s),
        .array, .object => try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
}

fn runtimeRenderGraphArtifactMetadataTemplateAlloc(
    alloc: Allocator,
    metadata_template_json: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, metadata_template_json, .{});
    defer parsed.deinit();
    var rendered = try runtimeRenderGraphArtifactMetadataValueAlloc(alloc, parsed.value, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
    defer runtimeFreeGraphRenderedJsonValue(alloc, &rendered);
    return try std.json.Stringify.valueAlloc(alloc, rendered, .{});
}

fn runtimeRenderGraphArtifactMetadataValueAlloc(
    alloc: Allocator,
    value: std.json.Value,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) !std.json.Value {
    return switch (value) {
        .string => |text| .{ .string = try runtimeRenderGraphArtifactTemplateAlloc(alloc, text, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value) },
        .array => |array| blk: {
            var out = std.json.Array.init(alloc);
            errdefer out.deinit();
            for (array.items) |child| try out.append(try runtimeRenderGraphArtifactMetadataValueAlloc(alloc, child, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value));
            break :blk .{ .array = out };
        },
        .object => |object| blk: {
            var out = std.json.ObjectMap.empty;
            errdefer out.deinit(alloc);
            var it = object.iterator();
            while (it.next()) |entry| {
                try out.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try runtimeRenderGraphArtifactMetadataValueAlloc(alloc, entry.value_ptr.*, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value));
            }
            break :blk .{ .object = out };
        },
        else => value,
    };
}

fn runtimeFreeGraphRenderedJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string => |text| alloc.free(@constCast(text)),
        .array => |*array| {
            for (array.items) |*item| runtimeFreeGraphRenderedJsonValue(alloc, item);
            array.deinit();
        },
        .object => |*object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                runtimeFreeGraphRenderedJsonValue(alloc, entry.value_ptr);
            }
            object.deinit(alloc);
        },
        else => {},
    }
    value.* = .null;
}

fn runtimeJsonEndpointDocumentId(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => value.string,
        .object => runtimeJsonStringField(value, "document_id") orelse runtimeJsonStringField(value, "doc_key") orelse runtimeJsonStringField(value, "key") orelse runtimeJsonStringField(value, "id") orelse runtimeJsonStringField(value, "local_id") orelse if (value.object.get("doc_ref")) |doc_ref| runtimeJsonEndpointDocumentId(doc_ref) else null,
        else => null,
    };
}

fn runtimeJsonEndpointDocumentIdResolved(value: std.json.Value, artifact_value: std.json.Value) ?[]const u8 {
    return runtimeJsonEndpointDocumentId(value) orelse if (runtimeResolveGraphEndpointEntity(value, artifact_value)) |entity| runtimeJsonEndpointDocumentId(entity) else null;
}

fn runtimeResolveGraphEndpointEntity(value: std.json.Value, artifact_value: std.json.Value) ?std.json.Value {
    if (value != .object) return null;
    if (runtimeJsonIntegerField(value, "entity_index")) |entity_index| return runtimeGraphArtifactEntityAtIndex(artifact_value, entity_index);
    const entity_id = runtimeJsonStringField(value, "entity_id") orelse runtimeJsonStringField(value, "id") orelse runtimeJsonStringField(value, "local_id") orelse return null;
    return runtimeFindGraphArtifactEntity(artifact_value, entity_id);
}

fn runtimeFindGraphArtifactEntity(artifact_value: std.json.Value, entity_id: []const u8) ?std.json.Value {
    if (artifact_value != .object) return null;
    const entities = artifact_value.object.get("_entities") orelse artifact_value.object.get("entities") orelse return null;
    return switch (entities) {
        .array => |array| blk: {
            for (array.items) |entity| {
                const id = runtimeJsonStringField(entity, "id") orelse runtimeJsonStringField(entity, "local_id") orelse continue;
                if (std.mem.eql(u8, id, entity_id)) break :blk entity;
            }
            break :blk null;
        },
        .object => entities.object.get(entity_id),
        else => null,
    };
}

fn runtimeGraphArtifactEntityAtIndex(artifact_value: std.json.Value, entity_index: i64) ?std.json.Value {
    if (entity_index < 0 or artifact_value != .object) return null;
    const entities = artifact_value.object.get("_entities") orelse artifact_value.object.get("entities") orelse return null;
    if (entities != .array) return null;
    const index: usize = @intCast(entity_index);
    if (index >= entities.array.items.len) return null;
    return entities.array.items[index];
}

fn runtimeJsonStringField(value: std.json.Value, field: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return if (found == .string) found.string else null;
}

fn runtimeJsonIntegerField(value: std.json.Value, field: []const u8) ?i64 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return switch (found) {
        .integer => found.integer,
        else => null,
    };
}

fn runtimeJsonFloatField(value: std.json.Value, field: []const u8) ?f64 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return switch (found) {
        .float => found.float,
        .integer => @floatFromInt(found.integer),
        else => null,
    };
}

fn sameChunkedDenseBatchKey(
    lhs: enrichment_types.GeneratedEnrichmentRequest,
    rhs: enrichment_types.GeneratedEnrichmentRequest,
) bool {
    return lhs.expected_dims == rhs.expected_dims and
        std.mem.eql(u8, requestEmbeddingName(lhs), requestEmbeddingName(rhs)) and
        std.mem.eql(u8, lhs.execution_json, rhs.execution_json);
}

fn appendCachedChunkDenseEmbeddingToWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    request: enrichment_types.GeneratedEnrichmentRequest,
    chunk_key: []const u8,
    artifact_key: []const u8,
    consumer_indexes: []const []const u8,
) !bool {
    if (generatedArtifactAlreadyPublished(runtime, artifact_key)) return false;
    const index_name = try runtime.alloc.dupe(u8, request.index_name);
    var index_name_owned = true;
    errdefer if (index_name_owned) runtime.alloc.free(index_name);
    const parent_doc_key = try runtime.alloc.dupe(u8, request.doc_key);
    var parent_doc_key_owned = true;
    errdefer if (parent_doc_key_owned) runtime.alloc.free(parent_doc_key);
    const doc_key = try runtime.alloc.dupe(u8, chunk_key);
    var doc_key_owned = true;
    errdefer if (doc_key_owned) runtime.alloc.free(doc_key);
    const cached_artifact_key = try runtime.alloc.dupe(u8, artifact_key);
    var cached_artifact_key_owned = true;
    errdefer if (cached_artifact_key_owned) runtime.alloc.free(cached_artifact_key);

    var cached = [_]derived_types.DerivedDenseEmbeddingWrite{.{
        .index_name = index_name,
        .parent_doc_key = parent_doc_key,
        .doc_key = doc_key,
        .artifact_key = cached_artifact_key,
        .vector = &.{},
    }};
    index_name_owned = false;
    parent_doc_key_owned = false;
    doc_key_owned = false;
    cached_artifact_key_owned = false;
    defer freeDerivedDenseEmbedding(runtime.alloc, cached[0]);

    var expanded_cached = try expandDenseEmbeddingsForConsumers(runtime, &cached, consumer_indexes);
    defer {
        for (expanded_cached) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
        if (expanded_cached.len > 0) runtime.alloc.free(expanded_cached);
    }
    try appendOwnedDenseEmbeddingsToWindow(runtime, window, &expanded_cached);
    return true;
}

fn freeChunkedDenseWindowItems(
    alloc: Allocator,
    items: []const ChunkedDenseWindowItem,
) void {
    for (items) |item| alloc.free(item.chunk_key);
}

fn freeCachedChunkDenseWindowItems(
    alloc: Allocator,
    items: []const CachedChunkDenseWindowItem,
) void {
    for (items) |item| {
        alloc.free(item.chunk_key);
        alloc.free(item.embedding_key);
    }
}

fn clearChunkedDenseBatch(
    alloc: Allocator,
    chunk_texts: *std.ArrayListUnmanaged([]const u8),
    chunk_items: *std.ArrayListUnmanaged(ChunkedDenseWindowItem),
    owns_texts: bool,
) void {
    if (owns_texts) {
        for (chunk_texts.items) |text| alloc.free(@constCast(text));
    }
    freeChunkedDenseWindowItems(alloc, chunk_items.items);
    chunk_items.clearRetainingCapacity();
    chunk_texts.clearRetainingCapacity();
}

fn flushChunkedDenseItems(
    runtime: *EnrichmentRuntime,
    dense_embedder: embedder_mod.DenseEmbedder,
    embedding_artifact_name: []const u8,
    expected_dims: u32,
    consumer_indexes: []const []const u8,
    chunk_texts: *std.ArrayListUnmanaged([]const u8),
    chunk_items: *std.ArrayListUnmanaged(ChunkedDenseWindowItem),
    window: *GeneratedReplayWindow,
    owns_texts: bool,
) !bool {
    if (chunk_items.items.len == 0) return true;

    const batch_texts = chunk_texts.items;
    const batch_items = chunk_items.items;
    const batch_stats = textBatchByteStats(batch_texts);
    yieldToInteractiveEmbeds(runtime);
    noteEmbedBatchStarted(runtime, batch_texts.len, batch_stats.total_bytes, batch_stats.max_bytes);
    const embed_started_ns = runtime.config.clock.nowRealtimeNs();
    const vectors = embedDenseBatchWithRetry(dense_embedder, runtime, embedding_artifact_name, batch_texts, expected_dims) catch |err| {
        noteEmbedBatchFinished(runtime, batch_texts.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), false);
        if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
        for (batch_items) |item| recordIsolatedRequestError(runtime, window, item.request, err);
        clearChunkedDenseBatch(runtime.alloc, chunk_texts, chunk_items, owns_texts);
        return false;
    };
    noteEmbedBatchFinished(runtime, batch_texts.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), true);
    defer embedder_mod.freeDenseEmbeddingBatch(runtime.alloc, vectors);
    if (vectors.len != batch_items.len) return error.InvalidEmbeddingResponse;

    var embeddings = try runtime.alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, batch_items.len);
    var initialized_embeddings: usize = 0;
    defer {
        for (embeddings[0..initialized_embeddings]) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
        if (embeddings.len > 0) runtime.alloc.free(embeddings);
    }

    for (batch_items, vectors, 0..) |item, vector, idx| {
        try appendUniqueDupeKey(runtime.alloc, &window.deleted_keys, item.chunk_key);
        try writeEmbeddingArtifact(runtime, .{
            .base_key = item.chunk_key,
            .parent_doc_key = item.parent_doc_key,
            .artifact_name = item.artifact_name,
            .source_field = item.source_field,
            .source_key = item.chunk_key,
            .source_hash = item.source_hash,
            .vector = vector,
        });
        try queueDerivedCoverageProduced(runtime, window, item.request.doc_key, consumer_indexes);
        const artifact_key = try embeddingArtifactKey(runtime, item.chunk_key, item.artifact_name);
        var artifact_key_owned = true;
        errdefer if (artifact_key_owned) runtime.alloc.free(artifact_key);
        const index_name = try runtime.alloc.dupe(u8, item.request.index_name);
        var index_name_owned = true;
        errdefer if (index_name_owned) runtime.alloc.free(index_name);
        const parent_doc_key = try runtime.alloc.dupe(u8, item.parent_doc_key);
        var parent_doc_key_owned = true;
        errdefer if (parent_doc_key_owned) runtime.alloc.free(parent_doc_key);
        const doc_key = try runtime.alloc.dupe(u8, item.chunk_key);
        var doc_key_owned = true;
        errdefer if (doc_key_owned) runtime.alloc.free(doc_key);
        embeddings[idx] = .{
            .index_name = index_name,
            .parent_doc_key = parent_doc_key,
            .doc_key = doc_key,
            .artifact_key = artifact_key,
            .vector = &.{},
        };
        artifact_key_owned = false;
        index_name_owned = false;
        parent_doc_key_owned = false;
        doc_key_owned = false;
        initialized_embeddings += 1;
    }

    var expanded = try expandDenseEmbeddingsForConsumers(runtime, embeddings, consumer_indexes);
    defer {
        for (expanded) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
        if (expanded.len > 0) runtime.alloc.free(expanded);
    }
    try appendOwnedDenseEmbeddingsToWindow(runtime, window, &expanded);

    clearChunkedDenseBatch(runtime.alloc, chunk_texts, chunk_items, owns_texts);
    return true;
}

fn processCachedChunkDenseItems(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    consumer_indexes: []const []const u8,
    window: *GeneratedReplayWindow,
    cached_items: *std.ArrayListUnmanaged(CachedChunkDenseWindowItem),
    max_window_items: usize,
) !void {
    var queued_produced = false;
    for (cached_items.items) |item| {
        if (try appendCachedChunkDenseEmbeddingToWindow(runtime, window, request, item.chunk_key, item.embedding_key, consumer_indexes)) {
            if (!queued_produced) {
                try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);
                queued_produced = true;
            }
        }
        try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
    }
    freeCachedChunkDenseWindowItems(runtime.alloc, cached_items.items);
    cached_items.clearRetainingCapacity();
}

fn processMaterializedChunkDenseRequest(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    chunk_artifact_name: []const u8,
    embedding_artifact_name: []const u8,
    dense_embedder: embedder_mod.DenseEmbedder,
    consumer_indexes: []const []const u8,
    window: *GeneratedReplayWindow,
) !void {
    const max_window_items = generatedReplayWindowItems();
    const max_batch_items = requestEmbedBatchItems(runtime.alloc, request);
    const max_batch_bytes = requestEmbedBatchBytes(runtime.alloc, request);

    var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (chunk_texts.items) |text| runtime.alloc.free(@constCast(text));
        chunk_texts.deinit(runtime.alloc);
    }
    var chunk_items = std.ArrayListUnmanaged(ChunkedDenseWindowItem).empty;
    defer {
        freeChunkedDenseWindowItems(runtime.alloc, chunk_items.items);
        chunk_items.deinit(runtime.alloc);
    }
    var cached_items = std.ArrayListUnmanaged(CachedChunkDenseWindowItem).empty;
    defer {
        freeCachedChunkDenseWindowItems(runtime.alloc, cached_items.items);
        cached_items.deinit(runtime.alloc);
    }
    var desired_chunk_keys = std.StringHashMapUnmanaged(void).empty;
    defer freeOwnedKeySet(runtime.alloc, &desired_chunk_keys);
    var existing_embedding_keys = std.ArrayListUnmanaged([]u8).empty;
    defer freeKeyList(runtime.alloc, existing_embedding_keys.items);

    const prefix = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, request.doc_key, "chunk", chunk_artifact_name);
    defer runtime.alloc.free(prefix);

    const upper = try internal_keys.nextPrefixAlloc(runtime.alloc, prefix);
    defer if (upper) |key| runtime.alloc.free(key);
    const upper_bound = if (upper) |key| key else "";

    const Discovery = struct {
        runtime: *EnrichmentRuntime,
        prefix: []const u8,
        source_field: []const u8,
        embedding_artifact_name: []const u8,
        desired: *std.StringHashMapUnmanaged(void),
        existing_embeddings: *std.ArrayListUnmanaged([]u8),

        fn scan(ctx_ptr: ?*anyopaque, key: []const u8, value: []const u8) anyerror!backend_scan.ScanAction {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr orelse return error.InvalidArgument));
            if (!std.mem.startsWith(u8, key, ctx.prefix)) return .stop;
            if (internal_keys.isDerivedEmbeddingArtifactKey(key)) {
                if (!internal_keys.matchesDerivedEmbeddingArtifactName(key, ctx.embedding_artifact_name)) return .@"continue";
                try appendUniqueDupeKey(ctx.runtime.alloc, ctx.existing_embeddings, key);
                return .@"continue";
            }
            if (!internal_keys.isChunkArtifactRecordKey(key)) return .@"continue";
            if (!try chunkPayloadHasText(ctx.runtime.alloc, value, ctx.source_field)) return .@"continue";
            try putOwnedKeySetDupeKey(ctx.runtime.alloc, ctx.desired, key);
            return .@"continue";
        }
    };
    var discovery = Discovery{
        .runtime = runtime,
        .prefix = prefix,
        .source_field = request.source_field,
        .embedding_artifact_name = embedding_artifact_name,
        .desired = &desired_chunk_keys,
        .existing_embeddings = &existing_embedding_keys,
    };
    try backend_scan.scanWithContext(&runtime.store, prefix, upper_bound, .{}, &discovery, Discovery.scan);

    var batch_source_bytes: usize = 0;
    var lower = try runtime.alloc.dupe(u8, prefix);
    defer runtime.alloc.free(lower);
    while (true) {
        const Collect = struct {
            runtime: *EnrichmentRuntime,
            request: enrichment_types.GeneratedEnrichmentRequest,
            prefix: []const u8,
            embedding_artifact_name: []const u8,
            chunk_texts: *std.ArrayListUnmanaged([]const u8),
            chunk_items: *std.ArrayListUnmanaged(ChunkedDenseWindowItem),
            cached_items: *std.ArrayListUnmanaged(CachedChunkDenseWindowItem),
            batch_source_bytes: *usize,
            max_batch_items: usize,
            max_batch_bytes: usize,
            stopped_for_batch: bool = false,
            last_key: ?[]u8 = null,

            fn scan(ctx_ptr: ?*anyopaque, key: []const u8, value: []const u8) anyerror!backend_scan.ScanAction {
                const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr orelse return error.InvalidArgument));
                if (!std.mem.startsWith(u8, key, ctx.prefix)) return .stop;
                if (!internal_keys.isChunkArtifactRecordKey(key)) return .@"continue";

                const text = (try chunkPayloadTextAlloc(ctx.runtime.alloc, value, ctx.request.source_field)) orelse return .@"continue";
                var text_owned = true;
                errdefer if (text_owned) ctx.runtime.alloc.free(text);
                const source_hash = enrichment_artifact_codec.hashSource(text);
                const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(ctx.runtime.alloc, key, ctx.embedding_artifact_name);
                var embedding_key_owned = true;
                errdefer if (embedding_key_owned) ctx.runtime.alloc.free(embedding_key);
                if (try shouldSkipEmbeddingArtifact(ctx.runtime, embedding_key, source_hash)) {
                    ctx.runtime.alloc.free(text);
                    text_owned = false;
                    try ctx.cached_items.append(ctx.runtime.alloc, .{
                        .chunk_key = try ctx.runtime.alloc.dupe(u8, key),
                        .embedding_key = embedding_key,
                    });
                    embedding_key_owned = false;
                } else {
                    try ctx.chunk_texts.append(ctx.runtime.alloc, text);
                    text_owned = false;
                    try ctx.chunk_items.append(ctx.runtime.alloc, .{
                        .request = ctx.request,
                        .parent_doc_key = ctx.request.doc_key,
                        .source_field = ctx.request.source_field,
                        .artifact_name = ctx.embedding_artifact_name,
                        .chunk_key = try ctx.runtime.alloc.dupe(u8, key),
                        .source_hash = source_hash,
                    });
                    ctx.batch_source_bytes.* += text.len;
                    ctx.runtime.alloc.free(embedding_key);
                    embedding_key_owned = false;
                }

                if (ctx.chunk_items.items.len + ctx.cached_items.items.len >= ctx.max_batch_items or
                    ctx.batch_source_bytes.* >= ctx.max_batch_bytes)
                {
                    ctx.stopped_for_batch = true;
                    ctx.last_key = try ctx.runtime.alloc.dupe(u8, key);
                    return .stop;
                }
                return .@"continue";
            }
        };
        var collect = Collect{
            .runtime = runtime,
            .request = request,
            .prefix = prefix,
            .embedding_artifact_name = embedding_artifact_name,
            .chunk_texts = &chunk_texts,
            .chunk_items = &chunk_items,
            .cached_items = &cached_items,
            .batch_source_bytes = &batch_source_bytes,
            .max_batch_items = max_batch_items,
            .max_batch_bytes = max_batch_bytes,
        };
        try backend_scan.scanWithContext(&runtime.store, lower, upper_bound, .{}, &collect, Collect.scan);

        try processCachedChunkDenseItems(runtime, request, consumer_indexes, window, &cached_items, max_window_items);
        _ = try flushChunkedDenseItems(runtime, dense_embedder, embedding_artifact_name, request.expected_dims, consumer_indexes, &chunk_texts, &chunk_items, window, true);
        try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
        batch_source_bytes = 0;

        if (!collect.stopped_for_batch) break;
        const next_lower = try keyAfterAlloc(runtime.alloc, collect.last_key.?);
        runtime.alloc.free(collect.last_key.?);
        runtime.alloc.free(lower);
        lower = next_lower;
    }

    for (existing_embedding_keys.items) |embedding_key| {
        if (try derivedEmbeddingBelongsToDesiredChunkSet(runtime.alloc, embedding_key, &desired_chunk_keys)) continue;
        if (try internal_keys.derivedEmbeddingBaseKeyAlloc(runtime.alloc, embedding_key)) |base_key| {
            try appendUniqueOwnedKey(runtime.alloc, &window.deleted_keys, base_key);
        }
        try appendUniqueDupeKey(runtime.alloc, &window.artifact_delete_keys, embedding_key);
        try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
    }
    if (desired_chunk_keys.count() == 0) try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
    try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
}

fn flushMaterializedSparseChunkSources(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    sparse_embedder: embedder_mod.SparseEmbedder,
    consumer_indexes: []const []const u8,
    sources: *std.ArrayListUnmanaged(ChunkEmbeddingSource),
    window: *GeneratedReplayWindow,
) !void {
    if (sources.items.len == 0) return;
    defer clearChunkEmbeddingSourceList(runtime.alloc, sources);

    const chunk_embeddings = try buildChunkSparseEmbeddingsFromSources(runtime, request, sparse_embedder, sources.items);
    defer {
        for (chunk_embeddings) |embedding| freeDerivedSparseEmbedding(runtime.alloc, embedding);
        if (chunk_embeddings.len > 0) runtime.alloc.free(chunk_embeddings);
    }
    if (chunk_embeddings.len == 0) return;
    try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);

    var expanded = try expandSparseEmbeddingsForConsumers(runtime, chunk_embeddings, consumer_indexes);
    defer {
        for (expanded) |embedding| freeDerivedSparseEmbedding(runtime.alloc, embedding);
        if (expanded.len > 0) runtime.alloc.free(expanded);
    }
    try appendOwnedSparseEmbeddingsToWindow(runtime, window, &expanded);
}

fn processCachedChunkSparseItems(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    consumer_indexes: []const []const u8,
    window: *GeneratedReplayWindow,
    cached_items: *std.ArrayListUnmanaged(CachedChunkDenseWindowItem),
    max_window_items: usize,
) !void {
    var queued_produced = false;
    for (cached_items.items) |item| {
        if (try appendCachedSparseEmbeddingToWindow(runtime, window, item.chunk_key, item.embedding_key, consumer_indexes)) {
            if (!queued_produced) {
                try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);
                queued_produced = true;
            }
        }
        try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
    }
    freeCachedChunkDenseWindowItems(runtime.alloc, cached_items.items);
    cached_items.clearRetainingCapacity();
}

fn processMaterializedChunkSparseRequest(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    chunk_artifact_name: []const u8,
    embedding_artifact_name: []const u8,
    sparse_embedder: embedder_mod.SparseEmbedder,
    consumer_indexes: []const []const u8,
    window: *GeneratedReplayWindow,
) !void {
    const max_window_items = generatedReplayWindowItems();
    const max_batch_items = requestEmbedBatchItems(runtime.alloc, request);
    const max_batch_bytes = requestEmbedBatchBytes(runtime.alloc, request);

    var sources = std.ArrayListUnmanaged(ChunkEmbeddingSource).empty;
    defer {
        clearChunkEmbeddingSourceList(runtime.alloc, &sources);
        sources.deinit(runtime.alloc);
    }
    var cached_items = std.ArrayListUnmanaged(CachedChunkDenseWindowItem).empty;
    defer {
        freeCachedChunkDenseWindowItems(runtime.alloc, cached_items.items);
        cached_items.deinit(runtime.alloc);
    }
    var desired_chunk_keys = std.StringHashMapUnmanaged(void).empty;
    defer freeOwnedKeySet(runtime.alloc, &desired_chunk_keys);
    var existing_embedding_keys = std.ArrayListUnmanaged([]u8).empty;
    defer freeKeyList(runtime.alloc, existing_embedding_keys.items);

    const prefix = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, request.doc_key, "chunk", chunk_artifact_name);
    defer runtime.alloc.free(prefix);

    const upper = try internal_keys.nextPrefixAlloc(runtime.alloc, prefix);
    defer if (upper) |key| runtime.alloc.free(key);
    const upper_bound = if (upper) |key| key else "";

    const Discovery = struct {
        runtime: *EnrichmentRuntime,
        prefix: []const u8,
        source_field: []const u8,
        embedding_artifact_name: []const u8,
        desired: *std.StringHashMapUnmanaged(void),
        existing_embeddings: *std.ArrayListUnmanaged([]u8),

        fn scan(ctx_ptr: ?*anyopaque, key: []const u8, value: []const u8) anyerror!backend_scan.ScanAction {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr orelse return error.InvalidArgument));
            if (!std.mem.startsWith(u8, key, ctx.prefix)) return .stop;
            if (internal_keys.isDerivedEmbeddingArtifactKey(key)) {
                if (!internal_keys.matchesDerivedEmbeddingArtifactName(key, ctx.embedding_artifact_name)) return .@"continue";
                try appendUniqueDupeKey(ctx.runtime.alloc, ctx.existing_embeddings, key);
                return .@"continue";
            }
            if (!internal_keys.isChunkArtifactRecordKey(key)) return .@"continue";
            if (!try chunkPayloadHasText(ctx.runtime.alloc, value, ctx.source_field)) return .@"continue";
            try putOwnedKeySetDupeKey(ctx.runtime.alloc, ctx.desired, key);
            return .@"continue";
        }
    };
    var discovery = Discovery{
        .runtime = runtime,
        .prefix = prefix,
        .source_field = request.source_field,
        .embedding_artifact_name = embedding_artifact_name,
        .desired = &desired_chunk_keys,
        .existing_embeddings = &existing_embedding_keys,
    };
    try backend_scan.scanWithContext(&runtime.store, prefix, upper_bound, .{}, &discovery, Discovery.scan);

    var batch_source_bytes: usize = 0;
    var lower = try runtime.alloc.dupe(u8, prefix);
    defer runtime.alloc.free(lower);
    while (true) {
        const Collect = struct {
            runtime: *EnrichmentRuntime,
            request: enrichment_types.GeneratedEnrichmentRequest,
            prefix: []const u8,
            embedding_artifact_name: []const u8,
            sources: *std.ArrayListUnmanaged(ChunkEmbeddingSource),
            cached_items: *std.ArrayListUnmanaged(CachedChunkDenseWindowItem),
            batch_source_bytes: *usize,
            max_batch_items: usize,
            max_batch_bytes: usize,
            stopped_for_batch: bool = false,
            last_key: ?[]u8 = null,

            fn scan(ctx_ptr: ?*anyopaque, key: []const u8, value: []const u8) anyerror!backend_scan.ScanAction {
                const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr orelse return error.InvalidArgument));
                if (!std.mem.startsWith(u8, key, ctx.prefix)) return .stop;
                if (!internal_keys.isChunkArtifactRecordKey(key)) return .@"continue";

                const text = (try chunkPayloadTextAlloc(ctx.runtime.alloc, value, ctx.request.source_field)) orelse return .@"continue";
                var text_owned = true;
                errdefer if (text_owned) ctx.runtime.alloc.free(text);
                const source_hash = enrichment_artifact_codec.hashSource(text);
                const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(ctx.runtime.alloc, key, ctx.embedding_artifact_name);
                var embedding_key_owned = true;
                errdefer if (embedding_key_owned) ctx.runtime.alloc.free(embedding_key);
                if (try shouldSkipEmbeddingArtifact(ctx.runtime, embedding_key, source_hash)) {
                    ctx.runtime.alloc.free(text);
                    text_owned = false;
                    try ctx.cached_items.append(ctx.runtime.alloc, .{
                        .chunk_key = try ctx.runtime.alloc.dupe(u8, key),
                        .embedding_key = embedding_key,
                    });
                    embedding_key_owned = false;
                } else {
                    try ctx.sources.append(ctx.runtime.alloc, .{
                        .key = try ctx.runtime.alloc.dupe(u8, key),
                        .text = text,
                    });
                    text_owned = false;
                    ctx.batch_source_bytes.* += text.len;
                    ctx.runtime.alloc.free(embedding_key);
                    embedding_key_owned = false;
                }

                if (ctx.sources.items.len + ctx.cached_items.items.len >= ctx.max_batch_items or
                    ctx.batch_source_bytes.* >= ctx.max_batch_bytes)
                {
                    ctx.stopped_for_batch = true;
                    ctx.last_key = try ctx.runtime.alloc.dupe(u8, key);
                    return .stop;
                }
                return .@"continue";
            }
        };
        var collect = Collect{
            .runtime = runtime,
            .request = request,
            .prefix = prefix,
            .embedding_artifact_name = embedding_artifact_name,
            .sources = &sources,
            .cached_items = &cached_items,
            .batch_source_bytes = &batch_source_bytes,
            .max_batch_items = max_batch_items,
            .max_batch_bytes = max_batch_bytes,
        };
        try backend_scan.scanWithContext(&runtime.store, lower, upper_bound, .{}, &collect, Collect.scan);

        try processCachedChunkSparseItems(runtime, request, consumer_indexes, window, &cached_items, max_window_items);
        try flushMaterializedSparseChunkSources(runtime, request, sparse_embedder, consumer_indexes, &sources, window);
        try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
        batch_source_bytes = 0;

        if (!collect.stopped_for_batch) break;
        const next_lower = try keyAfterAlloc(runtime.alloc, collect.last_key.?);
        runtime.alloc.free(collect.last_key.?);
        runtime.alloc.free(lower);
        lower = next_lower;
    }

    for (existing_embedding_keys.items) |embedding_key| {
        if (try derivedEmbeddingBelongsToDesiredChunkSet(runtime.alloc, embedding_key, &desired_chunk_keys)) continue;
        if (try internal_keys.derivedEmbeddingBaseKeyAlloc(runtime.alloc, embedding_key)) |base_key| {
            try appendUniqueOwnedKey(runtime.alloc, &window.deleted_keys, base_key);
        }
        try appendUniqueDupeKey(runtime.alloc, &window.artifact_delete_keys, embedding_key);
        try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
    }
    if (desired_chunk_keys.count() == 0) try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
    try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
}

fn collectPlainDenseBatchItem(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    consumer_indexes: []const []const u8,
    window: *GeneratedReplayWindow,
) !?PlainDenseBatchItem {
    const embedding_artifact_name = requestEmbeddingName(request);
    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return null,
    };
    defer runtime.alloc.free(raw);

    const source_text = try extractSourceText(runtime.alloc, runtime.config, raw, request) orelse {
        try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
        return null;
    };
    errdefer runtime.alloc.free(@constCast(source_text));
    const source_hash = enrichment_artifact_codec.hashSource(source_text);

    const artifact_key = try embeddingArtifactKey(runtime, request.doc_key, embedding_artifact_name);
    errdefer runtime.alloc.free(artifact_key);
    if (try shouldSkipEmbeddingArtifact(runtime, artifact_key, source_hash)) {
        if (try appendCachedDenseEmbeddingToWindow(runtime, window, request.doc_key, artifact_key, consumer_indexes)) {
            try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);
        }
        runtime.alloc.free(@constCast(source_text));
        runtime.alloc.free(artifact_key);
        return null;
    }

    return .{
        .request = request,
        .source_text = source_text,
        .source_hash = source_hash,
        .artifact_key = artifact_key,
    };
}

fn flushPlainDenseItems(
    runtime: *EnrichmentRuntime,
    dense_embedder: embedder_mod.DenseEmbedder,
    embedding_artifact_name: []const u8,
    expected_dims: u32,
    consumer_indexes: []const []const u8,
    items: []PlainDenseBatchItem,
    window: *GeneratedReplayWindow,
) !void {
    if (items.len == 0) return;

    const texts = try runtime.alloc.alloc([]const u8, items.len);
    defer runtime.alloc.free(texts);
    var total_source_bytes: usize = 0;
    var max_source_bytes: usize = 0;
    for (items, 0..) |item, i| {
        texts[i] = item.source_text;
        total_source_bytes += item.source_text.len;
        max_source_bytes = @max(max_source_bytes, item.source_text.len);
    }

    yieldToInteractiveEmbeds(runtime);
    noteEmbedBatchStarted(runtime, items.len, total_source_bytes, max_source_bytes);
    const embed_started_ns = runtime.config.clock.nowRealtimeNs();
    const vectors = embedDenseBatchWithRetry(dense_embedder, runtime, embedding_artifact_name, texts, expected_dims) catch |err| {
        noteEmbedBatchFinished(runtime, items.len, total_source_bytes, max_source_bytes, elapsedNsSince(runtime, embed_started_ns), false);
        return err;
    };
    noteEmbedBatchFinished(runtime, items.len, total_source_bytes, max_source_bytes, elapsedNsSince(runtime, embed_started_ns), true);
    defer embedder_mod.freeDenseEmbeddingBatch(runtime.alloc, vectors);
    if (vectors.len != items.len) return error.InvalidEmbeddingResponse;

    for (items, vectors) |item, vector| {
        try writeEmbeddingArtifact(runtime, .{
            .base_key = item.request.doc_key,
            .parent_doc_key = item.request.doc_key,
            .artifact_name = embedding_artifact_name,
            .source_field = item.request.source_field,
            .source_key = null,
            .source_hash = item.source_hash,
            .vector = vector,
        });
        try queueDerivedCoverageProduced(runtime, window, item.request.doc_key, consumer_indexes);

        var embeddings = try singleDenseEmbeddingForConsumers(runtime, item.request.doc_key, item.artifact_key, vector, consumer_indexes);
        defer {
            for (embeddings) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
            if (embeddings.len > 0) runtime.alloc.free(embeddings);
        }
        try appendOwnedDenseEmbeddingsToWindow(runtime, window, &embeddings);
    }
}

fn processPlainDenseWindow(
    runtime: *EnrichmentRuntime,
    requests: []const enrichment_types.GeneratedEnrichmentRequest,
    window: *GeneratedReplayWindow,
) !void {
    if (requests.len == 0) return;
    const dense_embedder = runtime.config.dense_embedder orelse return;

    const processed = try runtime.alloc.alloc(bool, requests.len);
    defer runtime.alloc.free(processed);
    @memset(processed, false);

    var i: usize = 0;
    while (i < requests.len) : (i += 1) {
        if (processed[i]) continue;
        processed[i] = true;

        const seed = requests[i];
        const max_batch_items = requestEmbedBatchItems(runtime.alloc, seed);
        const max_batch_bytes = requestEmbedBatchBytes(runtime.alloc, seed);
        const embedding_artifact_name = requestEmbeddingName(seed);
        const consumer_indexes = try runtime.index_manager.denseIndexesForEmbedding(runtime.alloc, embedding_artifact_name, seed.expected_dims);
        defer {
            for (consumer_indexes) |index_name| runtime.alloc.free(index_name);
            runtime.alloc.free(consumer_indexes);
        }
        if (consumer_indexes.len == 0) continue;

        var items = std.ArrayListUnmanaged(PlainDenseBatchItem).empty;
        defer {
            freePlainDenseBatchItems(runtime.alloc, items.items);
            items.deinit(runtime.alloc);
        }
        var batch_source_bytes: usize = 0;

        var j: usize = i;
        while (j < requests.len) : (j += 1) {
            if (items.items.len >= max_batch_items) break;
            if (processed[j] and j != i) continue;
            const request = requests[j];
            if (!samePlainDenseBatchKey(seed, request)) continue;
            processed[j] = true;

            if (try collectPlainDenseBatchItem(runtime, request, consumer_indexes, window)) |item| {
                if (items.items.len > 0 and batch_source_bytes + item.source_text.len > max_batch_bytes) {
                    var single = [_]PlainDenseBatchItem{item};
                    freePlainDenseBatchItems(runtime.alloc, &single);
                    processed[j] = false;
                    break;
                }
                batch_source_bytes += item.source_text.len;
                try items.append(runtime.alloc, item);
            }
        }

        flushPlainDenseItems(runtime, dense_embedder, embedding_artifact_name, seed.expected_dims, consumer_indexes, items.items, window) catch |err| {
            if (isEnrichmentControlError(err) or isRetryableEnrichmentError(err)) return err;
            for (items.items) |item| recordIsolatedRequestError(runtime, window, item.request, err);
            continue;
        };
    }
}

fn processChunkedDenseWindow(
    runtime: *EnrichmentRuntime,
    requests: []const enrichment_types.GeneratedEnrichmentRequest,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
    window: *GeneratedReplayWindow,
) !void {
    if (requests.len == 0) return;
    const dense_embedder = runtime.config.dense_embedder orelse return;

    const processed = try runtime.alloc.alloc(bool, requests.len);
    defer runtime.alloc.free(processed);
    @memset(processed, false);

    var i: usize = 0;
    while (i < requests.len) : (i += 1) {
        if (processed[i]) continue;
        processed[i] = true;

        const seed = requests[i];
        const embedding_artifact_name = requestEmbeddingName(seed);
        const consumer_indexes = try runtime.index_manager.denseIndexesForEmbedding(runtime.alloc, embedding_artifact_name, seed.expected_dims);
        defer {
            for (consumer_indexes) |index_name| runtime.alloc.free(index_name);
            runtime.alloc.free(consumer_indexes);
        }
        if (consumer_indexes.len == 0) continue;

        const max_window_items = generatedReplayWindowItems();
        var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
        defer chunk_texts.deinit(runtime.alloc);
        var chunk_items = std.ArrayListUnmanaged(ChunkedDenseWindowItem).empty;
        defer {
            freeChunkedDenseWindowItems(runtime.alloc, chunk_items.items);
            chunk_items.deinit(runtime.alloc);
        }
        const max_batch_items = requestEmbedBatchItems(runtime.alloc, seed);
        const max_batch_bytes = requestEmbedBatchBytes(runtime.alloc, seed);
        var batch_source_bytes: usize = 0;

        var j: usize = i;
        while (j < requests.len) : (j += 1) {
            if (processed[j] and j != i) continue;
            const request = requests[j];
            if (!sameChunkedDenseBatchKey(seed, request)) continue;
            processed[j] = true;

            const chunk_artifact_name = requestArtifactName(request);
            if (requestUsesMaterializedChunkArtifact(runtime, chunk_artifact_name)) {
                try processMaterializedChunkDenseRequest(runtime, request, chunk_artifact_name, embedding_artifact_name, dense_embedder, consumer_indexes, window);
                continue;
            }

            var source_set = try chunkEmbeddingSourceSetForRequest(runtime, request, chunk_artifact_name, chunk_cache);
            defer source_set.deinit(runtime.alloc);
            const request_stale = try deleteStaleChunkEmbeddingArtifacts(runtime, request.doc_key, chunk_artifact_name, embedding_artifact_name, source_set.desired_chunk_keys);
            var stale_deletes = request_stale;
            errdefer stale_deletes.deinit(runtime.alloc);
            try mergeOwnedStaleEmbeddingDeletesIntoWindow(runtime, window, &stale_deletes);
            try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
            if (source_set.sources.len == 0) {
                try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
                continue;
            }

            for (source_set.sources) |source| {
                const source_hash = enrichment_artifact_codec.hashSource(source.text);
                const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, source.key, embedding_artifact_name);
                defer runtime.alloc.free(embedding_key);
                if (try shouldSkipEmbeddingArtifact(runtime, embedding_key, source_hash)) {
                    if (try appendCachedChunkDenseEmbeddingToWindow(runtime, window, request, source.key, embedding_key, consumer_indexes)) {
                        try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);
                    }
                    try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
                    continue;
                }
                if (chunk_items.items.len > 0 and
                    (chunk_items.items.len >= max_batch_items or batch_source_bytes + source.text.len > max_batch_bytes))
                {
                    _ = try flushChunkedDenseItems(runtime, dense_embedder, embedding_artifact_name, seed.expected_dims, consumer_indexes, &chunk_texts, &chunk_items, window, false);
                    try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
                    batch_source_bytes = 0;
                }
                try chunk_texts.append(runtime.alloc, source.text);
                try chunk_items.append(runtime.alloc, .{
                    .request = request,
                    .parent_doc_key = request.doc_key,
                    .source_field = request.source_field,
                    .artifact_name = embedding_artifact_name,
                    .chunk_key = try runtime.alloc.dupe(u8, source.key),
                    .source_hash = source_hash,
                });
                batch_source_bytes += source.text.len;
                if (chunk_items.items.len >= max_batch_items or batch_source_bytes >= max_batch_bytes) {
                    _ = try flushChunkedDenseItems(runtime, dense_embedder, embedding_artifact_name, seed.expected_dims, consumer_indexes, &chunk_texts, &chunk_items, window, false);
                    try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
                    batch_source_bytes = 0;
                }
            }
        }

        if (chunk_items.items.len == 0) continue;
        _ = try flushChunkedDenseItems(runtime, dense_embedder, embedding_artifact_name, seed.expected_dims, consumer_indexes, &chunk_texts, &chunk_items, window, false);
        try flushGeneratedReplayWindowIfNeeded(runtime, window, max_window_items);
    }
}

fn getOrCreatePlannedRequests(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    request_plan_cache: *std.ArrayListUnmanaged(RequestPlanCacheEntry),
) ![]const enrichment_types.GeneratedEnrichmentRequest {
    for (request_plan_cache.items) |entry| {
        if (std.mem.eql(u8, entry.doc_key, doc_key)) return entry.requests;
    }

    const owned_doc_key = try runtime.alloc.dupe(u8, doc_key);
    errdefer runtime.alloc.free(owned_doc_key);

    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => {
            const empty = try runtime.alloc.alloc(enrichment_types.GeneratedEnrichmentRequest, 0);
            try request_plan_cache.append(runtime.alloc, .{
                .doc_key = owned_doc_key,
                .requests = empty,
            });
            return request_plan_cache.items[request_plan_cache.items.len - 1].requests;
        },
    };
    defer runtime.alloc.free(raw);

    const explicit_dense: []const mapper.DenseEmbeddingWrite = &.{};
    const explicit_sparse: []const mapper.SparseEmbeddingWrite = &.{};
    const planned = try runtime.index_manager.planGeneratedEnrichments(
        runtime.alloc,
        doc_key,
        raw,
        explicit_dense,
        explicit_sparse,
    );
    try request_plan_cache.append(runtime.alloc, .{
        .doc_key = owned_doc_key,
        .requests = planned,
    });
    return request_plan_cache.items[request_plan_cache.items.len - 1].requests;
}

fn flushGeneratedReplayWindowIfNeeded(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    max_items: usize,
) !void {
    if (window.itemCount() < max_items) return;
    try flushGeneratedReplayWindow(runtime, window);
}

fn flushGeneratedReplayWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
) !void {
    if (window.isEmpty()) return;

    if (!window.hasDerivedItems()) {
        try applyCoverageOutcomeTransitions(runtime, window.coverage_transitions.items);
        clearQueuedCoverageTransitions(runtime.alloc, &window.coverage_transitions, &window.coverage_transition_keys);
        return;
    }

    const artifact_delete_keys = try window.artifact_delete_keys.toOwnedSlice(runtime.alloc);
    errdefer freeKeyList(runtime.alloc, artifact_delete_keys);
    var batch = try window.toOwnedBatch();
    defer derived_types.deinitDerivedBatch(runtime.alloc, &batch);
    defer freeKeyList(runtime.alloc, artifact_delete_keys);
    const sequence = try appendGeneratedBatchWithRetry(runtime, batch, artifact_delete_keys);
    try applyQueuedCoverageTransitionsAfterReplayAppend(runtime, window.coverage_transitions.items);
    clearQueuedCoverageTransitions(runtime.alloc, &window.coverage_transitions, &window.coverage_transition_keys);
    try rememberPublishedGeneratedBatch(runtime, batch);
    runtime.notify_fn(runtime.notify_ctx, sequence);
}

fn appendOwnedDocumentsToWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    docs: *[]derived_types.DerivedDocument,
) !void {
    if (docs.*.len == 0) return;
    try window.documents.appendSlice(runtime.alloc, docs.*);
    runtime.alloc.free(docs.*);
    docs.* = &.{};
}

fn appendInlineFullTextDocumentToWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    key: []const u8,
    value: []const u8,
    text_indexes: []const []const u8,
) !void {
    if (text_indexes.len == 0) return;
    const targets = try runtime.alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
    errdefer {
        for (targets) |target| runtime.alloc.free(@constCast(target.index_name));
        runtime.alloc.free(targets);
    }
    for (text_indexes, 0..) |index_name, i| {
        targets[i] = .{
            .kind = .full_text,
            .index_name = try runtime.alloc.dupe(u8, index_name),
        };
    }
    try window.documents.append(runtime.alloc, .{
        .key = try runtime.alloc.dupe(u8, key),
        .action = .upsert,
        .cleaned_value = try runtime.alloc.dupe(u8, value),
        .targets = targets,
    });
}

fn appendFullTextDeleteDocumentToWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    key: []const u8,
    text_indexes: []const []const u8,
) !void {
    if (text_indexes.len == 0) return;
    const targets = try runtime.alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
    errdefer {
        for (targets) |target| runtime.alloc.free(@constCast(target.index_name));
        runtime.alloc.free(targets);
    }
    for (text_indexes, 0..) |index_name, i| {
        targets[i] = .{
            .kind = .full_text,
            .index_name = try runtime.alloc.dupe(u8, index_name),
        };
    }
    try window.documents.append(runtime.alloc, .{
        .key = try runtime.alloc.dupe(u8, key),
        .action = .delete,
        .targets = targets,
    });
}

fn appendOwnedDenseEmbeddingsToWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    embeddings: *[]derived_types.DerivedDenseEmbeddingWrite,
) !void {
    if (embeddings.*.len == 0) return;
    try window.dense_embeddings.appendSlice(runtime.alloc, embeddings.*);
    runtime.alloc.free(embeddings.*);
    embeddings.* = &.{};
}

fn appendOwnedSparseEmbeddingsToWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    embeddings: *[]derived_types.DerivedSparseEmbeddingWrite,
) !void {
    if (embeddings.*.len == 0) return;
    try window.sparse_embeddings.appendSlice(runtime.alloc, embeddings.*);
    runtime.alloc.free(embeddings.*);
    embeddings.* = &.{};
}

fn appendCachedDenseEmbeddingToWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    doc_key: []const u8,
    artifact_key: []const u8,
    consumer_indexes: []const []const u8,
) !bool {
    if (generatedArtifactAlreadyPublished(runtime, artifact_key)) return false;
    var embeddings = try singleDenseEmbeddingForConsumers(runtime, doc_key, artifact_key, &.{}, consumer_indexes);
    try appendOwnedDenseEmbeddingsToWindow(runtime, window, &embeddings);
    return true;
}

fn appendCachedSparseEmbeddingToWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    doc_key: []const u8,
    artifact_key: []const u8,
    consumer_indexes: []const []const u8,
) !bool {
    if (generatedArtifactAlreadyPublished(runtime, artifact_key)) return false;
    var embeddings = try singleSparseEmbeddingForConsumers(runtime, doc_key, artifact_key, &.{}, &.{}, consumer_indexes);
    try appendOwnedSparseEmbeddingsToWindow(runtime, window, &embeddings);
    return true;
}

fn mergeOwnedDeletedKeysIntoWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    keys: []const []u8,
) !void {
    defer runtime.alloc.free(keys);
    for (keys) |key| {
        try appendUniqueOwnedKey(runtime.alloc, &window.deleted_keys, key);
    }
}

fn mergeOwnedArtifactDeleteKeysIntoWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    keys: []const []u8,
) !void {
    defer runtime.alloc.free(keys);
    for (keys) |key| {
        try appendUniqueOwnedKey(runtime.alloc, &window.artifact_delete_keys, key);
    }
}

fn mergeOwnedStaleEmbeddingDeletesIntoWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    stale: *StaleEmbeddingDeletes,
) !void {
    errdefer stale.deinit(runtime.alloc);
    try mergeOwnedDeletedKeysIntoWindow(runtime, window, stale.vector_keys);
    stale.vector_keys = &.{};
    try mergeOwnedArtifactDeleteKeysIntoWindow(runtime, window, stale.artifact_delete_keys);
    stale.artifact_delete_keys = &.{};
}

fn processChunkText(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
    window: *GeneratedReplayWindow,
) !void {
    if (request.chunk_size == 0 and request.chunker_json.len == 0) return;

    const chunks = try getOrCreateRequestChunks(runtime, request, chunk_cache);
    if (chunks.len == 0) return;

    const artifact_name = requestArtifactName(request);
    const include_default_full_text = request.full_text_index or
        try chunking_types_mod.parseHasFullTextIndexFromSlice(runtime.alloc, request.chunker_json);
    const text_indexes = try runtime.index_manager.textIndexesForChunk(runtime.alloc, artifact_name, include_default_full_text);
    defer {
        for (text_indexes) |name| runtime.alloc.free(name);
        runtime.alloc.free(text_indexes);
    }

    const persist_chunks = try shouldStoreChunkArtifacts(runtime.alloc, request, text_indexes.len != 0);
    const desired_chunks: []const chunker_mod.Chunk = if (persist_chunks) chunks else &.{};
    const desired_chunk_keys = try chunkKeysForChunks(runtime.alloc, request.doc_key, artifact_name, desired_chunks);
    defer freeKeyList(runtime.alloc, desired_chunk_keys);
    const stale_vector_keys = try deleteStaleChunkArtifacts(runtime, request.doc_key, artifact_name, desired_chunk_keys);
    if (chunks.len == 0) {
        try mergeOwnedDeletedKeysIntoWindow(runtime, window, stale_vector_keys);
        return;
    }

    if (persist_chunks) {
        var writes = try runtime.alloc.alloc(KVPair, chunks.len);
        defer {
            for (writes) |write| runtime.alloc.free(@constCast(write.key));
            runtime.alloc.free(writes);
        }
        var payloads = try runtime.alloc.alloc([]u8, chunks.len);
        defer {
            for (payloads) |payload| runtime.alloc.free(payload);
            runtime.alloc.free(payloads);
        }

        for (chunks, 0..) |chunk, i| {
            const key = try internal_keys.chunkArtifactKeyAlloc(runtime.alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id));
            defer runtime.alloc.free(key);
            writes[i] = .{
                .key = try runtime.alloc.dupe(u8, key),
                .value = undefined,
            };
            var obj = std.json.ObjectMap.empty;
            errdefer {
                var it = obj.iterator();
                while (it.next()) |entry| {
                    runtime.alloc.free(entry.key_ptr.*);
                    freeJsonValue(runtime.alloc, entry.value_ptr);
                }
                obj.deinit(runtime.alloc);
            }
            try obj.put(runtime.alloc, try runtime.alloc.dupe(u8, "_parent_doc_key"), .{ .string = try runtime.alloc.dupe(u8, request.doc_key) });
            try obj.put(runtime.alloc, try runtime.alloc.dupe(u8, "_artifact_name"), .{ .string = try runtime.alloc.dupe(u8, artifact_name) });
            try obj.put(runtime.alloc, try runtime.alloc.dupe(u8, "_source_field"), .{ .string = try runtime.alloc.dupe(u8, request.source_field) });
            try chunk_artifact_mod.appendArtifactFields(runtime.alloc, &obj, request.source_field, chunk, true);
            payloads[i] = try std.json.Stringify.valueAlloc(runtime.alloc, std.json.Value{ .object = obj }, .{});
            var it = obj.iterator();
            while (it.next()) |entry| {
                runtime.alloc.free(entry.key_ptr.*);
                freeJsonValue(runtime.alloc, entry.value_ptr);
            }
            obj.deinit(runtime.alloc);
            writes[i].value = payloads[i];
        }

        try storePutBatchWithRetry(runtime, writes, &.{});
    }

    if (text_indexes.len == 0) {
        try mergeOwnedDeletedKeysIntoWindow(runtime, window, stale_vector_keys);
        return;
    }

    var text_chunk_count: usize = 0;
    for (chunks) |chunk| {
        if (chunk.isText()) text_chunk_count += 1;
    }
    if (text_chunk_count == 0) {
        try mergeOwnedDeletedKeysIntoWindow(runtime, window, stale_vector_keys);
        return;
    }

    var docs = try runtime.alloc.alloc(derived_types.DerivedDocument, text_chunk_count);
    var initialized_docs: usize = 0;
    defer {
        for (docs[0..initialized_docs]) |doc| {
            runtime.alloc.free(@constCast(doc.key));
            if (doc.cleaned_value) |value| runtime.alloc.free(@constCast(value));
            for (doc.targets) |target| runtime.alloc.free(@constCast(target.index_name));
            if (doc.targets.len > 0) runtime.alloc.free(@constCast(doc.targets));
        }
        if (docs.len > 0) runtime.alloc.free(docs);
    }

    for (chunks) |chunk| {
        if (!chunk.isText()) continue;
        const key = try internal_keys.chunkArtifactKeyAlloc(runtime.alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id));
        defer runtime.alloc.free(key);
        var targets = try runtime.alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
        for (text_indexes, 0..) |index_name, j| {
            targets[j] = .{
                .kind = .full_text,
                .index_name = try runtime.alloc.dupe(u8, index_name),
            };
        }
        var obj = std.json.ObjectMap.empty;
        errdefer {
            var it = obj.iterator();
            while (it.next()) |entry| {
                runtime.alloc.free(entry.key_ptr.*);
                freeJsonValue(runtime.alloc, entry.value_ptr);
            }
            obj.deinit(runtime.alloc);
        }
        try obj.put(runtime.alloc, try runtime.alloc.dupe(u8, "_parent_doc_key"), .{ .string = try runtime.alloc.dupe(u8, request.doc_key) });
        try obj.put(runtime.alloc, try runtime.alloc.dupe(u8, "_artifact_name"), .{ .string = try runtime.alloc.dupe(u8, artifact_name) });
        try obj.put(runtime.alloc, try runtime.alloc.dupe(u8, "_source_field"), .{ .string = try runtime.alloc.dupe(u8, request.source_field) });
        try chunk_artifact_mod.appendArtifactFields(runtime.alloc, &obj, request.source_field, chunk, true);
        const payload = try std.json.Stringify.valueAlloc(runtime.alloc, std.json.Value{ .object = obj }, .{});
        var it = obj.iterator();
        while (it.next()) |entry| {
            runtime.alloc.free(entry.key_ptr.*);
            freeJsonValue(runtime.alloc, entry.value_ptr);
        }
        obj.deinit(runtime.alloc);

        docs[initialized_docs] = .{
            .key = try runtime.alloc.dupe(u8, key),
            .action = .upsert,
            .cleaned_value = payload,
            .targets = targets,
        };
        initialized_docs += 1;
    }
    try mergeOwnedDeletedKeysIntoWindow(runtime, window, stale_vector_keys);
    try appendOwnedDocumentsToWindow(runtime, window, &docs);
    initialized_docs = 0;
}

fn processDenseEmbedding(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
    window: *GeneratedReplayWindow,
) !void {
    const dense_embedder = runtime.config.dense_embedder orelse return;
    const chunk_artifact_name = requestArtifactName(request);
    const embedding_artifact_name = requestEmbeddingName(request);
    const consumer_indexes = try runtime.index_manager.denseIndexesForEmbedding(runtime.alloc, embedding_artifact_name, request.expected_dims);
    defer {
        for (consumer_indexes) |index_name| runtime.alloc.free(index_name);
        runtime.alloc.free(consumer_indexes);
    }
    if (consumer_indexes.len == 0) return;
    if ((request.chunk_size > 0 or request.chunker_json.len > 0) and chunk_artifact_name.len > 0) {
        var source_set = try chunkEmbeddingSourceSetForRequest(runtime, request, chunk_artifact_name, chunk_cache);
        defer source_set.deinit(runtime.alloc);

        var stale_deletes = try deleteStaleChunkEmbeddingArtifacts(runtime, request.doc_key, chunk_artifact_name, embedding_artifact_name, source_set.desired_chunk_keys);
        errdefer stale_deletes.deinit(runtime.alloc);
        if (source_set.sources.len == 0) {
            try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
            try mergeOwnedStaleEmbeddingDeletesIntoWindow(runtime, window, &stale_deletes);
            return;
        }

        const chunk_embeddings = try buildChunkDenseEmbeddingsFromSources(runtime, request, dense_embedder, source_set.sources);
        defer {
            for (chunk_embeddings) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
            runtime.alloc.free(chunk_embeddings);
        }

        if (chunk_embeddings.len == 0) {
            try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
            try mergeOwnedStaleEmbeddingDeletesIntoWindow(runtime, window, &stale_deletes);
            return;
        }

        for (chunk_embeddings) |embedding| {
            if (embedding.vector.len > 0) try appendUniqueDupeKey(runtime.alloc, &window.deleted_keys, embedding.doc_key);
        }
        try writeChunkEmbeddingArtifacts(runtime, request.doc_key, request.source_field, embedding_artifact_name, chunk_embeddings);
        try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);
        var expanded = try expandDenseEmbeddingsForConsumers(runtime, chunk_embeddings, consumer_indexes);
        defer {
            for (expanded) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
            if (expanded.len > 0) runtime.alloc.free(expanded);
        }
        try mergeOwnedStaleEmbeddingDeletesIntoWindow(runtime, window, &stale_deletes);
        try appendOwnedDenseEmbeddingsToWindow(runtime, window, &expanded);
        return;
    }

    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return,
    };
    defer runtime.alloc.free(raw);

    if (request.source_template.len > 0 and dense_embedder.supportsParts()) {
        const source_parts = try renderSourceParts(runtime.alloc, runtime.config, raw, request);
        if (source_parts) |parts| {
            defer template.freeContentParts(runtime.alloc, parts);

            const vector = try embedDensePartsWithRetry(dense_embedder, runtime, embedding_artifact_name, parts, request.expected_dims);
            defer runtime.alloc.free(vector);

            try writeEmbeddingArtifact(runtime, .{
                .base_key = request.doc_key,
                .parent_doc_key = request.doc_key,
                .artifact_name = embedding_artifact_name,
                .source_field = request.source_field,
                .source_key = null,
                .source_hash = null,
                .vector = vector,
            });
            try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);
            const artifact_key = try embeddingArtifactKey(runtime, request.doc_key, embedding_artifact_name);
            defer runtime.alloc.free(artifact_key);

            var embeddings = try singleDenseEmbeddingForConsumers(runtime, request.doc_key, artifact_key, vector, consumer_indexes);
            defer {
                for (embeddings) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
                if (embeddings.len > 0) runtime.alloc.free(embeddings);
            }
            try appendOwnedDenseEmbeddingsToWindow(runtime, window, &embeddings);
            return;
        }
        try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
        return;
    }

    const source_text = try extractSourceText(runtime.alloc, runtime.config, raw, request) orelse {
        try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
        return;
    };
    defer runtime.alloc.free(source_text);
    const source_hash = enrichment_artifact_codec.hashSource(source_text);

    const artifact_key = try embeddingArtifactKey(runtime, request.doc_key, embedding_artifact_name);
    defer runtime.alloc.free(artifact_key);
    if (try shouldSkipEmbeddingArtifact(runtime, artifact_key, source_hash)) {
        if (try appendCachedDenseEmbeddingToWindow(runtime, window, request.doc_key, artifact_key, consumer_indexes)) {
            try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);
        }
        return;
    }

    const vector = try embedDenseWithRetry(dense_embedder, runtime, embedding_artifact_name, source_text, request.expected_dims);
    defer runtime.alloc.free(vector);

    try writeEmbeddingArtifact(runtime, .{
        .base_key = request.doc_key,
        .parent_doc_key = request.doc_key,
        .artifact_name = embedding_artifact_name,
        .source_field = request.source_field,
        .source_key = null,
        .source_hash = source_hash,
        .vector = vector,
    });
    try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);

    var embeddings = try singleDenseEmbeddingForConsumers(runtime, request.doc_key, artifact_key, vector, consumer_indexes);
    defer {
        for (embeddings) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
        if (embeddings.len > 0) runtime.alloc.free(embeddings);
    }
    try appendOwnedDenseEmbeddingsToWindow(runtime, window, &embeddings);
}

fn processSparseEmbedding(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
    window: *GeneratedReplayWindow,
) !void {
    const sparse_embedder = runtime.config.sparse_embedder orelse return;
    const embedding_artifact_name = requestEmbeddingName(request);
    const consumer_indexes = try runtime.index_manager.sparseIndexesForEmbedding(runtime.alloc, embedding_artifact_name);
    defer {
        for (consumer_indexes) |index_name| runtime.alloc.free(index_name);
        runtime.alloc.free(consumer_indexes);
    }
    if (consumer_indexes.len == 0) return;

    const chunk_artifact_name = requestArtifactName(request);
    if ((request.chunk_size > 0 or request.chunker_json.len > 0) and chunk_artifact_name.len > 0) {
        if (requestUsesMaterializedChunkArtifact(runtime, chunk_artifact_name)) {
            try processMaterializedChunkSparseRequest(runtime, request, chunk_artifact_name, embedding_artifact_name, sparse_embedder, consumer_indexes, window);
            return;
        }
        var source_set = try chunkEmbeddingSourceSetForRequest(runtime, request, chunk_artifact_name, chunk_cache);
        defer source_set.deinit(runtime.alloc);

        var stale_deletes = try deleteStaleChunkEmbeddingArtifacts(runtime, request.doc_key, chunk_artifact_name, embedding_artifact_name, source_set.desired_chunk_keys);
        errdefer stale_deletes.deinit(runtime.alloc);
        if (source_set.sources.len == 0) {
            try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
            try mergeOwnedStaleEmbeddingDeletesIntoWindow(runtime, window, &stale_deletes);
            return;
        }

        const chunk_embeddings = try buildChunkSparseEmbeddingsFromSources(runtime, request, sparse_embedder, source_set.sources);
        defer {
            for (chunk_embeddings) |embedding| freeDerivedSparseEmbedding(runtime.alloc, embedding);
            runtime.alloc.free(chunk_embeddings);
        }

        if (chunk_embeddings.len == 0) {
            try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
            try mergeOwnedStaleEmbeddingDeletesIntoWindow(runtime, window, &stale_deletes);
            return;
        }

        try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);
        var expanded = try expandSparseEmbeddingsForConsumers(runtime, chunk_embeddings, consumer_indexes);
        defer {
            for (expanded) |embedding| freeDerivedSparseEmbedding(runtime.alloc, embedding);
            if (expanded.len > 0) runtime.alloc.free(expanded);
        }
        try mergeOwnedStaleEmbeddingDeletesIntoWindow(runtime, window, &stale_deletes);
        try appendOwnedSparseEmbeddingsToWindow(runtime, window, &expanded);
        return;
    }

    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return,
    };
    defer runtime.alloc.free(raw);

    const source_text = try extractSourceText(runtime.alloc, runtime.config, raw, request) orelse {
        try markDerivedCoverageSkipped(runtime, window, request.doc_key, consumer_indexes);
        return;
    };
    defer runtime.alloc.free(source_text);
    const source_hash = enrichment_artifact_codec.hashSource(source_text);

    const artifact_key = try embeddingArtifactKey(runtime, request.doc_key, embedding_artifact_name);
    defer runtime.alloc.free(artifact_key);
    if (try shouldSkipEmbeddingArtifact(runtime, artifact_key, source_hash)) {
        if (try appendCachedSparseEmbeddingToWindow(runtime, window, request.doc_key, artifact_key, consumer_indexes)) {
            try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);
        }
        return;
    }

    var sparse = try embedSparseWithRetry(sparse_embedder, runtime, embedding_artifact_name, source_text);
    defer sparse.deinit(runtime.alloc);
    try writeSparseEmbeddingArtifact(runtime, request.doc_key, embedding_artifact_name, source_hash, sparse.indices, sparse.values);
    try queueDerivedCoverageProduced(runtime, window, request.doc_key, consumer_indexes);

    var embeddings = try singleSparseEmbeddingForConsumers(runtime, request.doc_key, artifact_key, sparse.indices, sparse.values, consumer_indexes);
    defer {
        for (embeddings) |embedding| freeDerivedSparseEmbedding(runtime.alloc, embedding);
        if (embeddings.len > 0) runtime.alloc.free(embeddings);
    }
    try appendOwnedSparseEmbeddingsToWindow(runtime, window, &embeddings);
}

fn buildChunkDenseEmbeddingsFromSources(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    dense_embedder: embedder_mod.DenseEmbedder,
    sources: []const ChunkEmbeddingSource,
) ![]derived_types.DerivedDenseEmbeddingWrite {
    if (sources.len == 0) return try runtime.alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, 0);

    var embeddings = std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite).empty;
    errdefer {
        for (embeddings.items) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
        embeddings.deinit(runtime.alloc);
    }
    var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
    defer chunk_texts.deinit(runtime.alloc);
    var chunk_keys = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (chunk_keys.items) |chunk_key| {
            if (!denseEmbeddingsOwnDocKey(embeddings.items, chunk_key)) runtime.alloc.free(chunk_key);
        }
        chunk_keys.deinit(runtime.alloc);
    }

    for (sources) |source| {
        const chunk_key = try runtime.alloc.dupe(u8, source.key);
        var chunk_key_owned = true;
        errdefer if (chunk_key_owned) runtime.alloc.free(chunk_key);
        const source_hash = enrichment_artifact_codec.hashSource(source.text);
        const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, source.key, requestEmbeddingName(request));
        defer runtime.alloc.free(embedding_key);
        if (try shouldSkipEmbeddingArtifact(runtime, embedding_key, source_hash)) {
            if (generatedArtifactAlreadyPublished(runtime, embedding_key)) {
                runtime.alloc.free(chunk_key);
                chunk_key_owned = false;
                continue;
            }
            try embeddings.append(runtime.alloc, .{
                .index_name = try runtime.alloc.dupe(u8, request.index_name),
                .parent_doc_key = try runtime.alloc.dupe(u8, request.doc_key),
                .doc_key = chunk_key,
                .artifact_key = try runtime.alloc.dupe(u8, embedding_key),
                .vector = &.{},
            });
            chunk_key_owned = false;
            continue;
        }
        try chunk_texts.append(runtime.alloc, source.text);
        try chunk_keys.append(runtime.alloc, chunk_key);
        chunk_key_owned = false;
    }

    if (chunk_texts.items.len == 0) return try embeddings.toOwnedSlice(runtime.alloc);

    const max_batch_items = requestEmbedBatchItems(runtime.alloc, request);
    const max_batch_bytes = requestEmbedBatchBytes(runtime.alloc, request);
    var start: usize = 0;
    while (start < chunk_texts.items.len) {
        const end = boundedTextBatchEnd(chunk_texts.items, start, max_batch_items, max_batch_bytes);
        const batch_texts = chunk_texts.items[start..end];
        const batch_keys = chunk_keys.items[start..end];
        const batch_stats = textBatchByteStats(batch_texts);
        noteEmbedBatchStarted(runtime, batch_texts.len, batch_stats.total_bytes, batch_stats.max_bytes);
        const embed_started_ns = runtime.config.clock.nowRealtimeNs();
        const vectors = embedDenseBatchWithRetry(dense_embedder, runtime, requestEmbeddingName(request), batch_texts, request.expected_dims) catch |err| {
            noteEmbedBatchFinished(runtime, batch_texts.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), false);
            return err;
        };
        noteEmbedBatchFinished(runtime, batch_texts.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), true);
        errdefer embedder_mod.freeDenseEmbeddingBatch(runtime.alloc, vectors);
        if (vectors.len != batch_keys.len) return error.InvalidEmbeddingResponse;

        for (batch_keys, vectors) |chunk_key, vector| {
            try embeddings.append(runtime.alloc, .{
                .index_name = try runtime.alloc.dupe(u8, request.index_name),
                .parent_doc_key = try runtime.alloc.dupe(u8, request.doc_key),
                .doc_key = chunk_key,
                .vector = vector,
            });
        }
        runtime.alloc.free(@constCast(vectors));
        start = end;
    }
    chunk_keys.deinit(runtime.alloc);

    return try embeddings.toOwnedSlice(runtime.alloc);
}

fn freeDerivedDenseEmbedding(alloc: Allocator, embedding: derived_types.DerivedDenseEmbeddingWrite) void {
    alloc.free(@constCast(embedding.index_name));
    if (embedding.parent_doc_key) |parent_doc_key| alloc.free(@constCast(parent_doc_key));
    alloc.free(@constCast(embedding.doc_key));
    if (embedding.artifact_key) |artifact_key| alloc.free(@constCast(artifact_key));
    alloc.free(@constCast(embedding.vector));
}

fn freeDerivedSparseEmbedding(alloc: Allocator, embedding: derived_types.DerivedSparseEmbeddingWrite) void {
    alloc.free(@constCast(embedding.index_name));
    alloc.free(@constCast(embedding.doc_key));
    if (embedding.artifact_key) |key| alloc.free(@constCast(key));
    if (embedding.indices.len > 0) alloc.free(@constCast(embedding.indices));
    if (embedding.values.len > 0) alloc.free(@constCast(embedding.values));
}

fn sameOwnedSlice(a: []const u8, b: []const u8) bool {
    return a.ptr == b.ptr and a.len == b.len;
}

fn denseEmbeddingsOwnDocKey(embeddings: []const derived_types.DerivedDenseEmbeddingWrite, key: []const u8) bool {
    for (embeddings) |embedding| {
        if (sameOwnedSlice(embedding.doc_key, key)) return true;
    }
    return false;
}

fn sparseEmbeddingsOwnDocKey(embeddings: []const derived_types.DerivedSparseEmbeddingWrite, key: []const u8) bool {
    for (embeddings) |embedding| {
        if (sameOwnedSlice(embedding.doc_key, key)) return true;
    }
    return false;
}

fn buildChunkSparseEmbeddingsFromSources(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    sparse_embedder: embedder_mod.SparseEmbedder,
    sources: []const ChunkEmbeddingSource,
) ![]derived_types.DerivedSparseEmbeddingWrite {
    if (sources.len == 0) return try runtime.alloc.alloc(derived_types.DerivedSparseEmbeddingWrite, 0);

    var embeddings = std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite).empty;
    errdefer {
        for (embeddings.items) |embedding| freeDerivedSparseEmbedding(runtime.alloc, embedding);
        embeddings.deinit(runtime.alloc);
    }
    var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
    defer chunk_texts.deinit(runtime.alloc);
    var chunk_keys = std.ArrayListUnmanaged([]u8).empty;
    var chunk_hashes = std.ArrayListUnmanaged(u64).empty;
    defer chunk_hashes.deinit(runtime.alloc);
    errdefer {
        for (chunk_keys.items) |chunk_key| {
            if (!sparseEmbeddingsOwnDocKey(embeddings.items, chunk_key)) runtime.alloc.free(chunk_key);
        }
        chunk_keys.deinit(runtime.alloc);
    }

    for (sources) |source| {
        const chunk_key = try runtime.alloc.dupe(u8, source.key);
        var chunk_key_owned = true;
        errdefer if (chunk_key_owned) runtime.alloc.free(chunk_key);
        const source_hash = enrichment_artifact_codec.hashSource(source.text);
        const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, source.key, requestEmbeddingName(request));
        defer runtime.alloc.free(embedding_key);
        if (try shouldSkipEmbeddingArtifact(runtime, embedding_key, source_hash)) {
            if (generatedArtifactAlreadyPublished(runtime, embedding_key)) {
                runtime.alloc.free(chunk_key);
                chunk_key_owned = false;
                continue;
            }
            try embeddings.append(runtime.alloc, .{
                .index_name = try runtime.alloc.dupe(u8, request.index_name),
                .doc_key = chunk_key,
                .artifact_key = try runtime.alloc.dupe(u8, embedding_key),
                .indices = &.{},
                .values = &.{},
            });
            chunk_key_owned = false;
            continue;
        }
        try chunk_texts.append(runtime.alloc, source.text);
        try chunk_keys.append(runtime.alloc, chunk_key);
        try chunk_hashes.append(runtime.alloc, source_hash);
        chunk_key_owned = false;
    }

    if (chunk_texts.items.len == 0) return try embeddings.toOwnedSlice(runtime.alloc);

    const max_batch_items = requestEmbedBatchItems(runtime.alloc, request);
    const max_batch_bytes = requestEmbedBatchBytes(runtime.alloc, request);
    var start: usize = 0;
    while (start < chunk_texts.items.len) {
        const end = boundedTextBatchEnd(chunk_texts.items, start, max_batch_items, max_batch_bytes);
        const batch_texts = chunk_texts.items[start..end];
        const batch_keys = chunk_keys.items[start..end];
        const batch_hashes = chunk_hashes.items[start..end];
        const batch_stats = textBatchByteStats(batch_texts);
        noteEmbedBatchStarted(runtime, batch_texts.len, batch_stats.total_bytes, batch_stats.max_bytes);
        const embed_started_ns = runtime.config.clock.nowRealtimeNs();
        const sparse_batch = embedSparseBatchWithRetry(sparse_embedder, runtime, requestEmbeddingName(request), batch_texts) catch |err| {
            noteEmbedBatchFinished(runtime, batch_texts.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), false);
            return err;
        };
        noteEmbedBatchFinished(runtime, batch_texts.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), true);
        errdefer embedder_mod.freeSparseEmbeddingBatch(runtime.alloc, sparse_batch);
        if (sparse_batch.len != batch_keys.len) return error.InvalidEmbeddingResponse;

        for (batch_keys, batch_hashes, sparse_batch) |chunk_key, source_hash, sparse| {
            try writeSparseEmbeddingArtifact(runtime, chunk_key, requestEmbeddingName(request), source_hash, sparse.indices, sparse.values);
            try embeddings.append(runtime.alloc, .{
                .index_name = try runtime.alloc.dupe(u8, request.index_name),
                .doc_key = chunk_key,
                .artifact_key = try embeddingArtifactKey(runtime, chunk_key, requestEmbeddingName(request)),
                .indices = &.{},
                .values = &.{},
            });
        }
        embedder_mod.freeSparseEmbeddingBatch(runtime.alloc, sparse_batch);
        start = end;
    }
    chunk_keys.deinit(runtime.alloc);

    return try embeddings.toOwnedSlice(runtime.alloc);
}

fn requestArtifactName(request: enrichment_types.GeneratedEnrichmentRequest) []const u8 {
    return enrichment_types.requestArtifactName(request);
}

fn requestEmbeddingName(request: enrichment_types.GeneratedEnrichmentRequest) []const u8 {
    return enrichment_types.requestEmbeddingName(request);
}

fn singleDenseEmbeddingForConsumers(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    artifact_key: []const u8,
    vector: []const f32,
    consumer_indexes: []const []const u8,
) ![]derived_types.DerivedDenseEmbeddingWrite {
    _ = vector;
    const out = try runtime.alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, consumer_indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
        runtime.alloc.free(out);
    }
    for (consumer_indexes, 0..) |index_name, i| {
        out[i] = .{
            .index_name = try runtime.alloc.dupe(u8, index_name),
            .doc_key = try runtime.alloc.dupe(u8, doc_key),
            .artifact_key = try runtime.alloc.dupe(u8, artifact_key),
            .vector = &.{},
        };
        initialized += 1;
    }
    return out;
}

fn singleSparseEmbeddingForConsumers(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    artifact_key: []const u8,
    indices: []const u32,
    values: []const f32,
    consumer_indexes: []const []const u8,
) ![]derived_types.DerivedSparseEmbeddingWrite {
    _ = indices;
    _ = values;
    const out = try runtime.alloc.alloc(derived_types.DerivedSparseEmbeddingWrite, consumer_indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |embedding| freeDerivedSparseEmbedding(runtime.alloc, embedding);
        runtime.alloc.free(out);
    }
    for (consumer_indexes, 0..) |index_name, i| {
        out[i] = .{
            .index_name = try runtime.alloc.dupe(u8, index_name),
            .doc_key = try runtime.alloc.dupe(u8, doc_key),
            .artifact_key = try runtime.alloc.dupe(u8, artifact_key),
            .indices = &.{},
            .values = &.{},
        };
        initialized += 1;
    }
    return out;
}

fn expandSparseEmbeddingsForConsumers(
    runtime: *EnrichmentRuntime,
    chunk_embeddings: []const derived_types.DerivedSparseEmbeddingWrite,
    consumer_indexes: []const []const u8,
) ![]derived_types.DerivedSparseEmbeddingWrite {
    var out = std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite).empty;
    errdefer {
        for (out.items) |embedding| freeDerivedSparseEmbedding(runtime.alloc, embedding);
        out.deinit(runtime.alloc);
    }

    for (chunk_embeddings) |embedding| {
        for (consumer_indexes) |index_name| {
            try out.append(runtime.alloc, .{
                .index_name = try runtime.alloc.dupe(u8, index_name),
                .doc_key = try runtime.alloc.dupe(u8, embedding.doc_key),
                .artifact_key = if (embedding.artifact_key) |key| try runtime.alloc.dupe(u8, key) else null,
                .indices = &.{},
                .values = &.{},
            });
        }
    }
    return try out.toOwnedSlice(runtime.alloc);
}

fn expandDenseEmbeddingsForConsumers(
    runtime: *EnrichmentRuntime,
    embeddings: []const derived_types.DerivedDenseEmbeddingWrite,
    consumer_indexes: []const []const u8,
) ![]derived_types.DerivedDenseEmbeddingWrite {
    const out = try runtime.alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, embeddings.len * consumer_indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
        runtime.alloc.free(out);
    }
    for (embeddings) |embedding| {
        for (consumer_indexes) |index_name| {
            out[initialized] = .{
                .index_name = try runtime.alloc.dupe(u8, index_name),
                .parent_doc_key = if (embedding.parent_doc_key) |parent_doc_key| try runtime.alloc.dupe(u8, parent_doc_key) else null,
                .doc_key = try runtime.alloc.dupe(u8, embedding.doc_key),
                .artifact_key = if (embedding.artifact_key) |key| try runtime.alloc.dupe(u8, key) else null,
                .vector = &.{},
            };
            initialized += 1;
        }
    }
    return out[0..initialized];
}

const EmbeddingArtifactWrite = struct {
    base_key: []const u8,
    parent_doc_key: []const u8,
    artifact_name: []const u8,
    source_field: []const u8,
    source_key: ?[]const u8,
    source_hash: ?u64 = null,
    vector: []const f32,
};

fn writeEmbeddingArtifact(runtime: *EnrichmentRuntime, write: EmbeddingArtifactWrite) !void {
    _ = write.parent_doc_key;
    _ = write.source_field;
    _ = write.source_key;
    const key = if (internal_keys.isInternalUserKey(write.base_key))
        try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, write.base_key, write.artifact_name)
    else
        try internal_keys.embeddingArtifactKeyForDocumentAlloc(runtime.alloc, write.base_key, write.artifact_name);
    defer runtime.alloc.free(key);
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(runtime.alloc, write.source_hash, write.vector);
    defer runtime.alloc.free(payload);

    try storePutWithRetry(runtime, key, payload);
    recordArtifactBytes(runtime, .dense_embedding, payload.len);
}

fn writeSparseEmbeddingArtifact(
    runtime: *EnrichmentRuntime,
    base_key: []const u8,
    artifact_name: []const u8,
    source_hash: u64,
    indices: []const u32,
    values: []const f32,
) !void {
    const key = try embeddingArtifactKey(runtime, base_key, artifact_name);
    defer runtime.alloc.free(key);
    const payload = try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(runtime.alloc, source_hash, indices, values);
    defer runtime.alloc.free(payload);

    try storePutWithRetry(runtime, key, payload);
    recordArtifactBytes(runtime, .sparse_embedding, payload.len);
}

fn publishDeletedKeys(runtime: *EnrichmentRuntime, deleted_keys: []const []const u8) !void {
    if (deleted_keys.len == 0) return;
    const batch = derived_types.DerivedBatch{
        .deleted_keys = deleted_keys,
    };
    var cloned = try derived_types.cloneBatch(runtime.alloc, batch);
    defer derived_types.deinitDerivedBatch(runtime.alloc, &cloned);
    const sequence = try appendGeneratedBatchWithRetry(runtime, cloned, &.{});
    runtime.notify_fn(runtime.notify_ctx, sequence);
}

fn embeddingArtifactKey(runtime: *EnrichmentRuntime, base_key: []const u8, artifact_name: []const u8) ![]u8 {
    return if (internal_keys.isInternalUserKey(base_key))
        try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, base_key, artifact_name)
    else
        try internal_keys.embeddingArtifactKeyForDocumentAlloc(runtime.alloc, base_key, artifact_name);
}

fn shouldSkipEmbeddingArtifact(runtime: *EnrichmentRuntime, artifact_key: []const u8, source_hash: u64) !bool {
    const raw = storeGetAlloc(runtime, artifact_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return false,
    };
    defer runtime.alloc.free(raw);
    const existing_hash = enrichment_artifact_codec.sourceHash(raw) catch {
        runtime.codec_decode_failures += 1;
        return false;
    };
    if (existing_hash != null and existing_hash.? == source_hash) {
        runtime.skip_by_hash_count += 1;
        return true;
    }
    return false;
}

fn shouldSkipAssetArtifact(runtime: *EnrichmentRuntime, artifact_key: []const u8, value: []const u8) !bool {
    const raw = storeGetAlloc(runtime, artifact_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return false,
    };
    defer runtime.alloc.free(raw);
    if (std.mem.eql(u8, raw, value)) {
        runtime.skip_by_hash_count += 1;
        return true;
    }
    return false;
}

fn shouldSkipAssetProducer(runtime: *EnrichmentRuntime, state_key: []const u8, expected_state: []const u8) !bool {
    const raw = storeGetAlloc(runtime, state_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return false,
    };
    defer runtime.alloc.free(raw);
    if (std.mem.eql(u8, raw, expected_state)) {
        runtime.skip_by_hash_count += 1;
        return true;
    }
    return false;
}

fn assetStateKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.asset_state_kind);
    try internal_keys.appendEncodedComponent(&list, alloc, artifact_name);
    return try list.toOwnedSlice(alloc);
}

fn assetStateValueAlloc(
    alloc: Allocator,
    source_text: []const u8,
    source_parts_json: ?[]const u8,
    producer_json: []const u8,
) ![]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(source_text);
    if (source_parts_json) |parts| hasher.update(parts);
    hasher.update(producer_json);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try alloc.dupe(u8, &digest);
}

fn documentExtractionFingerprintAlloc(
    alloc: Allocator,
    source_url: []const u8,
    config_json: []const u8,
    configured_content_type: []const u8,
    configured_filename: []const u8,
    downloaded_content_type: []const u8,
    data: []const u8,
) ![]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(source_url);
    hasher.update(config_json);
    hasher.update(configured_content_type);
    hasher.update(configured_filename);
    hasher.update(downloaded_content_type);
    hasher.update(data);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try hexBytesAlloc(alloc, &digest);
}

fn hexBytesAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

const DocumentExtractionUnitDescriptor = struct {
    key: []const u8,
    fingerprint: []const u8,
};

const DocumentExtractionRangeRoute = struct {
    range_id: []const u8,
    route_status: []const u8 = "local_committed",
    owner_group_id: u64 = 0,
};

const RuntimeDocumentExtractionPreviousState = struct {
    unit_keys: []const []const u8 = &.{},
    unit_descriptors: []DocumentExtractionUnitDescriptor = &.{},
    chunk_keys: []const []const u8 = &.{},
    recovered_from_store_scan: bool = false,

    fn deinit(self: *@This(), alloc: Allocator) void {
        freeOwnedConstKeySlice(alloc, self.unit_keys);
        freeDocumentExtractionUnitDescriptors(alloc, self.unit_descriptors);
        freeOwnedConstKeySlice(alloc, self.chunk_keys);
        self.* = undefined;
    }
};

fn loadRuntimeDocumentExtractionPreviousState(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    artifact_name: []const u8,
    state: []const u8,
) !RuntimeDocumentExtractionPreviousState {
    if (loadRuntimeDocumentExtractionPreviousStateFromJson(runtime.alloc, state)) |parsed| {
        return parsed;
    } else |err| switch (err) {
        error.OutOfMemory => return err,
        else => {},
    }
    var recovered = try scanRuntimeDocumentExtractionPreviousStateFromStore(runtime, doc_key, artifact_name);
    recovered.recovered_from_store_scan = true;
    return recovered;
}

fn loadRuntimeDocumentExtractionPreviousStateFromJson(alloc: Allocator, state: []const u8) !RuntimeDocumentExtractionPreviousState {
    var out = RuntimeDocumentExtractionPreviousState{};
    errdefer out.deinit(alloc);
    out.unit_keys = try documentExtractionStateUnitKeysAlloc(alloc, state);
    out.unit_descriptors = try documentExtractionStateUnitDescriptorsAlloc(alloc, state);
    out.chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, state);
    return out;
}

fn scanRuntimeDocumentExtractionPreviousStateFromStore(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    artifact_name: []const u8,
) !RuntimeDocumentExtractionPreviousState {
    var out = RuntimeDocumentExtractionPreviousState{};
    errdefer out.deinit(runtime.alloc);

    var unit_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (unit_keys.items) |key| runtime.alloc.free(@constCast(key));
        unit_keys.deinit(runtime.alloc);
    }
    const unit_prefix = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, doc_key, "asset", artifact_name);
    defer runtime.alloc.free(unit_prefix);
    const unit_rows = try backend_scan.scanPrefix(runtime.alloc, &runtime.store, unit_prefix);
    defer backend_scan.freeResults(runtime.alloc, unit_rows);
    for (unit_rows) |entry| {
        if (std.mem.eql(u8, entry.key, unit_prefix)) continue;
        if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) continue;
        try unit_keys.append(runtime.alloc, try runtime.alloc.dupe(u8, entry.key));
    }

    var chunk_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (chunk_keys.items) |key| runtime.alloc.free(@constCast(key));
        chunk_keys.deinit(runtime.alloc);
    }
    for (runtime.index_manager.enrichments.items) |entry| {
        if (entry.kind != .chunk) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, artifact_name)) continue;
        const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, doc_key, "chunk", entry.name);
        defer runtime.alloc.free(chunk_prefix);
        const chunk_rows = try backend_scan.scanPrefix(runtime.alloc, &runtime.store, chunk_prefix);
        defer backend_scan.freeResults(runtime.alloc, chunk_rows);
        for (chunk_rows) |row| {
            if (!internal_keys.isChunkArtifactRecordKey(row.key)) continue;
            try chunk_keys.append(runtime.alloc, try runtime.alloc.dupe(u8, row.key));
        }
    }

    out.unit_keys = try unit_keys.toOwnedSlice(runtime.alloc);
    out.chunk_keys = try chunk_keys.toOwnedSlice(runtime.alloc);
    out.unit_descriptors = try runtime.alloc.alloc(DocumentExtractionUnitDescriptor, out.unit_keys.len);
    for (out.unit_descriptors) |*descriptor| {
        descriptor.* = .{ .key = "", .fingerprint = "" };
    }
    for (out.unit_descriptors, out.unit_keys) |*descriptor, key| {
        descriptor.* = .{
            .key = try runtime.alloc.dupe(u8, key),
            .fingerprint = "",
        };
    }
    return out;
}

fn documentExtractionUnitFingerprintAlloc(alloc: Allocator, unit: document_extraction_mod.Unit) ![]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(unit.unit_id);
    hasher.update(unit.unit_type);
    hasher.update(unit.text);
    hasher.update(unit.method);
    if (unit.source_path) |source_path| hasher.update(source_path);
    if (unit.extraction_status) |extraction_status| hasher.update(extraction_status);
    if (unit.source_sha256) |source_sha256| hasher.update(source_sha256);
    if (unit.byte_length) |byte_length| {
        var buf: [@sizeOf(u64)]u8 = undefined;
        std.mem.writeInt(u64, &buf, byte_length, .big);
        hasher.update(&buf);
    }
    hasher.update(if (unit.ocr_used) "ocr:1" else "ocr:0");
    if (unit.ocr_confidence) |confidence| {
        var value = confidence;
        hasher.update(std.mem.asBytes(&value));
    }
    if (unit.ocr_bbox) |bbox| {
        for (bbox) |coord| {
            var value = coord;
            hasher.update(std.mem.asBytes(&value));
        }
    }
    hasher.update(if (unit.transcript_used) "transcript:1" else "transcript:0");
    if (unit.transcript_confidence) |confidence| {
        var value = confidence;
        hasher.update(std.mem.asBytes(&value));
    }
    if (unit.extraction_warning) |warning| hasher.update(warning);
    if (unit.page_number) |page_number| {
        var buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &buf, page_number, .big);
        hasher.update(&buf);
    }
    if (unit.page_label) |page_label| hasher.update(page_label);
    if (unit.page_bbox) |bbox| {
        for (bbox) |coord| {
            const coord_value: u64 = @bitCast(coord);
            hasher.update(std.mem.asBytes(&coord_value));
        }
    }
    if (unit.page_rotation) |rotation| {
        var buf: [@sizeOf(i32)]u8 = undefined;
        std.mem.writeInt(i32, &buf, rotation, .big);
        hasher.update(&buf);
    }
    for (unit.text_regions) |region| {
        for (region.span) |span| {
            var buf: [@sizeOf(u32)]u8 = undefined;
            std.mem.writeInt(u32, &buf, span, .big);
            hasher.update(&buf);
        }
        for (region.bbox) |coord| {
            const coord_value: u64 = @bitCast(coord);
            hasher.update(std.mem.asBytes(&coord_value));
        }
    }
    if (unit.char_start) |char_start| {
        var buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &buf, char_start, .big);
        hasher.update(&buf);
    }
    if (unit.char_end) |char_end| {
        var buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &buf, char_end, .big);
        hasher.update(&buf);
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try hexBytesAlloc(alloc, &digest);
}

fn documentExtractionUnitDescriptorsFromKeysAlloc(
    alloc: Allocator,
    unit_keys: []const []const u8,
    fingerprints: []const []const u8,
) ![]DocumentExtractionUnitDescriptor {
    if (unit_keys.len != fingerprints.len) return error.InvalidDocumentExtractionState;
    const out = try alloc.alloc(DocumentExtractionUnitDescriptor, unit_keys.len);
    for (unit_keys, fingerprints, 0..) |key, fingerprint, i| {
        out[i] = .{
            .key = key,
            .fingerprint = fingerprint,
        };
    }
    return out;
}

fn freeDocumentExtractionUnitDescriptors(alloc: Allocator, descriptors: []DocumentExtractionUnitDescriptor) void {
    for (descriptors) |descriptor| {
        if (descriptor.key.len > 0) alloc.free(@constCast(descriptor.key));
        if (descriptor.fingerprint.len > 0) alloc.free(@constCast(descriptor.fingerprint));
    }
    if (descriptors.len > 0) alloc.free(descriptors);
}

fn documentExtractionStateValueAlloc(
    alloc: Allocator,
    fingerprint: []const u8,
    unit_keys: []const []const u8,
    unit_descriptors: []const DocumentExtractionUnitDescriptor,
    chunk_keys: []const []const u8,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .kind = "document_extraction_state_v1",
        .fingerprint = fingerprint,
        .unit_keys = unit_keys,
        .unit_descriptors = unit_descriptors,
        .chunk_keys = chunk_keys,
    }, .{});
}

fn documentExtractionStateFingerprintMatches(alloc: Allocator, state: []const u8, fingerprint: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, state, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const value = parsed.value.object.get("fingerprint") orelse return false;
    return value == .string and std.mem.eql(u8, value.string, fingerprint);
}

fn documentExtractionStateUnitKeysAlloc(alloc: Allocator, state: []const u8) ![]const []const u8 {
    return try documentExtractionStateKeysAlloc(alloc, state, "unit_keys");
}

fn documentExtractionStateChunkKeysAlloc(alloc: Allocator, state: []const u8) ![]const []const u8 {
    return try documentExtractionStateKeysAlloc(alloc, state, "chunk_keys");
}

fn documentExtractionStateUnitDescriptorsAlloc(alloc: Allocator, state: []const u8) ![]DocumentExtractionUnitDescriptor {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, state, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.alloc(DocumentExtractionUnitDescriptor, 0);
    const descriptors_value = parsed.value.object.get("unit_descriptors") orelse return documentExtractionStateUnitDescriptorFallbackAlloc(alloc, parsed.value.object);
    if (descriptors_value != .array) return error.InvalidDocumentExtractionState;
    const out = try alloc.alloc(DocumentExtractionUnitDescriptor, descriptors_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |descriptor| {
            alloc.free(@constCast(descriptor.key));
            alloc.free(@constCast(descriptor.fingerprint));
        }
        alloc.free(out);
    }
    for (descriptors_value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidDocumentExtractionState;
        const key_value = item.object.get("key") orelse return error.InvalidDocumentExtractionState;
        const fingerprint_value = item.object.get("fingerprint") orelse return error.InvalidDocumentExtractionState;
        if (fingerprint_value != .string) return error.InvalidDocumentExtractionState;
        const key = try documentExtractionStateByteSliceAlloc(alloc, key_value);
        errdefer alloc.free(@constCast(key));
        const fingerprint = try alloc.dupe(u8, fingerprint_value.string);
        errdefer alloc.free(fingerprint);
        out[i] = .{
            .key = key,
            .fingerprint = fingerprint,
        };
        initialized += 1;
    }
    return out;
}

fn documentExtractionStateUnitDescriptorFallbackAlloc(alloc: Allocator, object: std.json.ObjectMap) ![]DocumentExtractionUnitDescriptor {
    const keys_value = object.get("unit_keys") orelse return try alloc.alloc(DocumentExtractionUnitDescriptor, 0);
    if (keys_value != .array) return try alloc.alloc(DocumentExtractionUnitDescriptor, 0);
    const out = try alloc.alloc(DocumentExtractionUnitDescriptor, keys_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |descriptor| {
            alloc.free(@constCast(descriptor.key));
            if (descriptor.fingerprint.len > 0) alloc.free(@constCast(descriptor.fingerprint));
        }
        alloc.free(out);
    }
    for (keys_value.array.items, 0..) |item, i| {
        out[i] = .{
            .key = try documentExtractionStateByteSliceAlloc(alloc, item),
            .fingerprint = "",
        };
        initialized += 1;
    }
    return out;
}

fn documentExtractionStateKeysAlloc(alloc: Allocator, state: []const u8, field_name: []const u8) ![]const []const u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, state, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.alloc([]const u8, 0);
    const keys_value = parsed.value.object.get(field_name) orelse return try alloc.alloc([]const u8, 0);
    if (keys_value != .array) return try alloc.alloc([]const u8, 0);
    const out = try alloc.alloc([]const u8, keys_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| alloc.free(@constCast(key));
        alloc.free(out);
    }
    for (keys_value.array.items, 0..) |item, i| {
        out[i] = try documentExtractionStateByteSliceAlloc(alloc, item);
        initialized += 1;
    }
    return out;
}

fn documentExtractionStateByteSliceAlloc(alloc: Allocator, value: std.json.Value) ![]const u8 {
    switch (value) {
        .string => |string| return try alloc.dupe(u8, string),
        .array => |array| {
            const out = try alloc.alloc(u8, array.items.len);
            errdefer alloc.free(out);
            for (array.items, 0..) |item, i| {
                if (item != .integer) return error.InvalidDocumentExtractionState;
                out[i] = std.math.cast(u8, item.integer) orelse return error.InvalidDocumentExtractionState;
            }
            return out;
        },
        else => return error.InvalidDocumentExtractionState,
    }
}

fn documentExtractionUnitKeyStillPresent(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    previous_key: []const u8,
    units: []const document_extraction_mod.Unit,
) !bool {
    for (units) |unit| {
        const key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, doc_key, artifact_name, unit.unit_id);
        defer alloc.free(key);
        if (std.mem.eql(u8, previous_key, key)) return true;
    }
    return false;
}

fn documentUnitPayloadAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    unit: document_extraction_mod.Unit,
    source_url: []const u8,
    content_type: []const u8,
    route: DocumentExtractionRangeRoute,
) ![]u8 {
    const owner_group_id = std.math.cast(i64, route.owner_group_id) orelse return error.InvalidDocumentExtractionManifest;
    return try std.json.Stringify.valueAlloc(alloc, .{
        ._parent_doc_key = doc_key,
        ._artifact_name = artifact_name,
        ._artifact_range_id = route.range_id,
        ._artifact_range_kind = "unit",
        ._artifact_route_status = route.route_status,
        ._artifact_owner_group_id = owner_group_id,
        .unit_id = unit.unit_id,
        .unit_type = unit.unit_type,
        .text = unit.text,
        .content_type = "text/plain",
        .language = "",
        .source_path = unit.source_path,
        .extraction_status = unit.extraction_status,
        .source_sha256 = unit.source_sha256,
        .byte_length = unit.byte_length,
        .confidence = documentUnitConfidence(unit),
        .ocr_confidence = unit.ocr_confidence,
        .ocr_bbox = unit.ocr_bbox,
        .transcript_confidence = unit.transcript_confidence,
        .extraction_warning = unit.extraction_warning,
        .provenance = .{
            .source_url = source_url,
            .source_path = unit.source_path,
            .method = unit.method,
            .extraction_status = unit.extraction_status,
            .source_sha256 = unit.source_sha256,
            .byte_length = unit.byte_length,
            .confidence = documentUnitConfidence(unit),
            .ocr_used = unit.ocr_used,
            .ocr_confidence = unit.ocr_confidence,
            .ocr_bbox = unit.ocr_bbox,
            .transcript_used = unit.transcript_used,
            .transcript_confidence = unit.transcript_confidence,
            .extraction_warning = unit.extraction_warning,
            .page_number = unit.page_number,
            .page_label = unit.page_label,
            .page_bbox = unit.page_bbox,
            .page_rotation = unit.page_rotation,
            .text_regions = unit.text_regions,
            .char_start = unit.char_start,
            .char_end = unit.char_end,
            .source_content_type = content_type,
            .format_provenance = .{
                .schema = "antfly.document_format_provenance.v1",
                .source_content_type = content_type,
                .source_path = unit.source_path,
                .coordinate_system = "source_page_points",
                .extraction_method = unit.method,
                .extraction_status = unit.extraction_status,
                .source_sha256 = unit.source_sha256,
                .byte_length = unit.byte_length,
                .confidence = documentUnitConfidence(unit),
                .ocr_used = unit.ocr_used,
                .ocr_confidence = unit.ocr_confidence,
                .ocr_bbox = unit.ocr_bbox,
                .transcript_used = unit.transcript_used,
                .transcript_confidence = unit.transcript_confidence,
                .extraction_warning = unit.extraction_warning,
                .page_number = unit.page_number,
                .page_label = unit.page_label,
                .page_bbox = unit.page_bbox,
                .page_rotation = unit.page_rotation,
                .text_regions = unit.text_regions,
            },
        },
    }, .{});
}

fn documentUnitConfidence(unit: document_extraction_mod.Unit) ?f64 {
    return unit.ocr_confidence orelse unit.transcript_confidence;
}

const document_extraction_range_target_children = 256;
const document_extraction_range_target_text_bytes = 1024 * 1024;

fn documentExtractionRangeCount(key_count: usize) usize {
    if (key_count == 0) return 0;
    return (key_count + document_extraction_range_target_children - 1) / document_extraction_range_target_children;
}

fn documentExtractionUnitRangeCount(units: []const document_extraction_mod.Unit) usize {
    var count: usize = 0;
    var start: usize = 0;
    while (start < units.len) {
        count += 1;
        start = documentExtractionRangeEnd(units.len, units, start);
    }
    return count;
}

fn documentExtractionUnitRangeIndex(units: []const document_extraction_mod.Unit, unit_index: usize) usize {
    var range_index: usize = 0;
    var start: usize = 0;
    while (start < units.len) : (range_index += 1) {
        const end = documentExtractionRangeEnd(units.len, units, start);
        if (unit_index < end) return range_index;
        start = end;
    }
    return range_index;
}

fn documentExtractionUnitRangeCountFromTextLengths(unit_text_lengths: []const usize) usize {
    var count: usize = 0;
    var start: usize = 0;
    while (start < unit_text_lengths.len) {
        count += 1;
        start = documentExtractionRangeEndFromTextLengths(unit_text_lengths.len, unit_text_lengths, start);
    }
    return count;
}

fn documentExtractionUnitRangeIndexFromTextLengths(unit_text_lengths: []const usize, unit_index: usize) usize {
    var range_index: usize = 0;
    var start: usize = 0;
    while (start < unit_text_lengths.len) : (range_index += 1) {
        const end = documentExtractionRangeEndFromTextLengths(unit_text_lengths.len, unit_text_lengths, start);
        if (unit_index < end) return range_index;
        start = end;
    }
    return range_index;
}

fn documentExtractionRangeIdAlloc(alloc: Allocator, range_index: usize) ![]u8 {
    return try std.fmt.allocPrint(alloc, "range:{d:0>6}", .{range_index});
}

fn documentExtractionKeyIndex(keys: []const []const u8, key: []const u8) ?usize {
    for (keys, 0..) |candidate, i| {
        if (std.mem.eql(u8, candidate, key)) return i;
    }
    return null;
}

fn documentExtractionManifestGeneration(alloc: Allocator, manifest: []const u8) !u64 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, manifest, .{}) catch return 0;
    defer parsed.deinit();
    if (parsed.value != .object) return 0;
    const generation = parsed.value.object.get("generation") orelse return 0;
    if (generation != .integer or generation.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(u64, generation.integer) orelse return error.InvalidDocumentExtractionManifest;
}

fn documentExtractionManifestHasLastError(alloc: Allocator, manifest_json: []const u8) !bool {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, manifest_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    return parsed.value.object.get("last_error") != null;
}

fn jsonObjectStringDup(alloc: Allocator, object: std.json.ObjectMap, field_name: []const u8) ![]u8 {
    const value = object.get(field_name) orelse return "";
    if (value != .string) return "";
    return try alloc.dupe(u8, value.string);
}

fn jsonObjectOptionalStringDup(alloc: Allocator, object: std.json.ObjectMap, field_name: []const u8) !?[]u8 {
    const value = object.get(field_name) orelse return null;
    if (value != .string) return null;
    return try alloc.dupe(u8, value.string);
}

fn jsonObjectU64(object: std.json.ObjectMap, field_name: []const u8) !u64 {
    const value = object.get(field_name) orelse return 0;
    if (value != .integer or value.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(u64, value.integer) orelse return error.InvalidDocumentExtractionManifest;
}

fn jsonObjectUsize(object: std.json.ObjectMap, field_name: []const u8) !usize {
    return std.math.cast(usize, try jsonObjectU64(object, field_name)) orelse return error.InvalidDocumentExtractionManifest;
}

fn jsonObjectOptionalUsize(object: std.json.ObjectMap, field_name: []const u8) !?usize {
    const value = object.get(field_name) orelse return null;
    if (value != .integer or value.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(usize, value.integer) orelse return error.InvalidDocumentExtractionManifest;
}

fn jsonObjectOptionalU64(object: std.json.ObjectMap, field_name: []const u8) !?u64 {
    const value = object.get(field_name) orelse return null;
    if (value != .integer or value.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(u64, value.integer) orelse return error.InvalidDocumentExtractionManifest;
}

fn jsonObjectOptionalBool(object: std.json.ObjectMap, field_name: []const u8) !?bool {
    const value = object.get(field_name) orelse return null;
    if (value != .bool) return error.InvalidDocumentExtractionManifest;
    return value.bool;
}

fn documentArtifactChildRangesFromManifestJsonAlloc(alloc: Allocator, manifest_json: []const u8) ![]types.DocumentArtifactChildRange {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, manifest_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.alloc(types.DocumentArtifactChildRange, 0);
    const value = parsed.value.object.get("child_ranges") orelse return try alloc.alloc(types.DocumentArtifactChildRange, 0);
    if (value != .array) return try alloc.alloc(types.DocumentArtifactChildRange, 0);

    const out = try alloc.alloc(types.DocumentArtifactChildRange, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*range| range.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }

    for (value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidDocumentExtractionManifest;
        out[i] = .{
            .range_id = try jsonObjectStringDup(alloc, item.object, "range_id"),
            .range_kind = try jsonObjectStringDup(alloc, item.object, "range_kind"),
            .artifact_name = try jsonObjectStringDup(alloc, item.object, "artifact_name"),
            .split_boundary = try jsonObjectStringDup(alloc, item.object, "split_boundary"),
            .placement = try jsonObjectStringDup(alloc, item.object, "placement"),
            .owner_group_id = try jsonObjectOptionalU64(item.object, "owner_group_id"),
            .placement_generation = try jsonObjectOptionalU64(item.object, "placement_generation"),
            .route_status = try jsonObjectOptionalStringDup(alloc, item.object, "route_status"),
            .split_eligible = try jsonObjectOptionalBool(item.object, "split_eligible"),
            .start_key = try jsonObjectStringDup(alloc, item.object, "start_key"),
            .end_key_exclusive = try jsonObjectStringDup(alloc, item.object, "end_key_exclusive"),
            .last_key = try jsonObjectStringDup(alloc, item.object, "last_key"),
            .child_count = try jsonObjectUsize(item.object, "child_count"),
            .text_bytes = try jsonObjectOptionalUsize(item.object, "text_bytes"),
        };
        initialized += 1;
    }

    return out;
}

fn freeDocumentArtifactChildRanges(alloc: Allocator, child_ranges: []types.DocumentArtifactChildRange) void {
    for (child_ranges) |*child_range| child_range.deinit(alloc);
    if (child_ranges.len > 0) alloc.free(child_ranges);
}

fn appendJsonFieldName(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8) !void {
    if (first.*) {
        first.* = false;
    } else {
        try out.append(alloc, ',');
    }
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

fn appendJsonFieldString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: []const u8) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonString(alloc, out, value);
}

fn appendJsonFieldU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: u64) !void {
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

fn appendJsonFieldUsize(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: usize) !void {
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

fn appendJsonFieldBool(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: bool) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

fn appendDocumentExtractionRangeDescriptors(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    artifact_name: []const u8,
    unit_keys: []const []const u8,
    chunk_keys: []const []const u8,
    units: []const document_extraction_mod.Unit,
    unit_text_lengths: []const usize,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
) !void {
    var first_range = true;
    var range_index: usize = 0;
    try appendDocumentExtractionKeyRanges(alloc, out, &first_range, &range_index, "unit", artifact_name, unit_keys, units, unit_text_lengths, previous_child_ranges);
    try appendDocumentExtractionKeyRanges(alloc, out, &first_range, &range_index, "chunk", "derived_chunks", chunk_keys, &.{}, &.{}, previous_child_ranges);
}

fn appendDocumentExtractionRangePolicy(alloc: Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldU64(alloc, out, &first, "policy_version", 1);
    try appendJsonFieldUsize(alloc, out, &first, "unit_target_children", document_extraction_range_target_children);
    try appendJsonFieldUsize(alloc, out, &first, "unit_target_text_bytes", document_extraction_range_target_text_bytes);
    try appendJsonFieldUsize(alloc, out, &first, "chunk_target_children", document_extraction_range_target_children);
    try appendJsonFieldString(alloc, out, &first, "oversized_unit_policy", "single_unit_range");
    try out.append(alloc, '}');
}

fn appendDocumentExtractionExistingRanges(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    ranges: []const types.DocumentArtifactChildRange,
) !void {
    for (ranges, 0..) |range, i| {
        if (i > 0) try out.append(alloc, ',');
        var first = true;
        try out.append(alloc, '{');
        try appendJsonFieldString(alloc, out, &first, "range_id", range.range_id);
        try appendJsonFieldString(alloc, out, &first, "range_kind", range.range_kind);
        try appendJsonFieldString(alloc, out, &first, "artifact_name", range.artifact_name);
        try appendJsonFieldString(alloc, out, &first, "split_boundary", range.split_boundary);
        try appendJsonFieldString(alloc, out, &first, "placement", range.placement);
        if (range.owner_group_id) |value| try appendJsonFieldU64(alloc, out, &first, "owner_group_id", value);
        if (range.placement_generation) |value| try appendJsonFieldU64(alloc, out, &first, "placement_generation", value);
        if (range.route_status) |value| try appendJsonFieldString(alloc, out, &first, "route_status", value);
        if (range.split_eligible) |value| try appendJsonFieldBool(alloc, out, &first, "split_eligible", value);
        try appendJsonFieldString(alloc, out, &first, "start_key", range.start_key);
        try appendJsonFieldString(alloc, out, &first, "end_key_exclusive", range.end_key_exclusive);
        try appendJsonFieldString(alloc, out, &first, "last_key", range.last_key);
        try appendJsonFieldUsize(alloc, out, &first, "child_count", range.child_count);
        if (range.text_bytes) |value| try appendJsonFieldUsize(alloc, out, &first, "text_bytes", value);
        try out.append(alloc, '}');
    }
}

fn appendDocumentExtractionKeyRanges(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first_range: *bool,
    range_index: *usize,
    range_kind: []const u8,
    artifact_name: []const u8,
    keys: []const []const u8,
    units: []const document_extraction_mod.Unit,
    unit_text_lengths: []const usize,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
) !void {
    var start: usize = 0;
    while (start < keys.len) {
        const end = documentExtractionRangeEndWithTextLengths(keys.len, units, unit_text_lengths, start);
        if (first_range.*) {
            first_range.* = false;
        } else {
            try out.append(alloc, ',');
        }
        var first = true;
        try out.append(alloc, '{');
        const range_id = try std.fmt.allocPrint(alloc, "range:{d:0>6}", .{range_index.*});
        defer alloc.free(range_id);
        const previous_range = findDocumentArtifactChildRange(previous_child_ranges, range_id, range_kind, artifact_name);
        try appendJsonFieldString(alloc, out, &first, "range_id", range_id);
        try appendJsonFieldString(alloc, out, &first, "range_kind", range_kind);
        try appendJsonFieldString(alloc, out, &first, "artifact_name", artifact_name);
        try appendJsonFieldString(alloc, out, &first, "split_boundary", documentExtractionSplitBoundary(range_kind));
        try appendJsonFieldString(alloc, out, &first, "placement", if (previous_range) |range| range.placement else "parent");
        try appendJsonFieldU64(alloc, out, &first, "owner_group_id", if (previous_range) |range| range.owner_group_id orelse 0 else 0);
        try appendJsonFieldU64(alloc, out, &first, "placement_generation", if (previous_range) |range| range.placement_generation orelse 0 else 0);
        try appendJsonFieldString(alloc, out, &first, "route_status", if (previous_range) |range| range.route_status orelse "local_committed" else "local_committed");
        try appendJsonFieldBool(alloc, out, &first, "split_eligible", if (previous_range) |range| range.split_eligible orelse (end - start > 1) else end - start > 1);
        try appendJsonFieldString(alloc, out, &first, "start_key", keys[start]);
        try appendJsonFieldString(alloc, out, &first, "end_key_exclusive", if (end < keys.len) keys[end] else "");
        try appendJsonFieldString(alloc, out, &first, "last_key", keys[end - 1]);
        try appendJsonFieldUsize(alloc, out, &first, "child_count", end - start);
        if (unit_text_lengths.len == keys.len) {
            var text_bytes: usize = 0;
            for (unit_text_lengths[start..end]) |unit_text_len| text_bytes += unit_text_len;
            try appendJsonFieldUsize(alloc, out, &first, "text_bytes", text_bytes);
        } else if (units.len >= end) {
            var text_bytes: usize = 0;
            for (units[start..end]) |unit| text_bytes += unit.text.len;
            try appendJsonFieldUsize(alloc, out, &first, "text_bytes", text_bytes);
        }
        try out.append(alloc, '}');
        range_index.* += 1;
        start = end;
    }
}

fn documentExtractionSplitBoundary(range_kind: []const u8) []const u8 {
    if (std.mem.eql(u8, range_kind, "chunk")) return "chunk";
    return "unit";
}

fn documentExtractionRangeEnd(
    key_count: usize,
    units: []const document_extraction_mod.Unit,
    start: usize,
) usize {
    return documentExtractionRangeEndWithTextLengths(key_count, units, &.{}, start);
}

fn documentExtractionRangeEndWithTextLengths(
    key_count: usize,
    units: []const document_extraction_mod.Unit,
    unit_text_lengths: []const usize,
    start: usize,
) usize {
    if (unit_text_lengths.len == key_count) return documentExtractionRangeEndFromTextLengths(key_count, unit_text_lengths, start);
    var end = start;
    var text_bytes: usize = 0;
    const use_text_limit = units.len == key_count;
    while (end < key_count and end - start < document_extraction_range_target_children) {
        if (use_text_limit) {
            const unit_bytes = units[end].text.len;
            if (end > start and text_bytes + unit_bytes > document_extraction_range_target_text_bytes) break;
            text_bytes += unit_bytes;
        }
        end += 1;
    }
    return end;
}

fn documentExtractionRangeEndFromTextLengths(
    key_count: usize,
    unit_text_lengths: []const usize,
    start: usize,
) usize {
    var end = start;
    var text_bytes: usize = 0;
    const use_text_limit = unit_text_lengths.len == key_count;
    while (end < key_count and end - start < document_extraction_range_target_children) {
        if (use_text_limit) {
            const unit_bytes = unit_text_lengths[end];
            if (end > start and text_bytes + unit_bytes > document_extraction_range_target_text_bytes) break;
            text_bytes += unit_bytes;
        }
        end += 1;
    }
    return end;
}

fn findDocumentArtifactChildRange(
    ranges: []const types.DocumentArtifactChildRange,
    range_id: []const u8,
    range_kind: []const u8,
    artifact_name: []const u8,
) ?*const types.DocumentArtifactChildRange {
    for (ranges) |*range| {
        if (std.mem.eql(u8, range.range_id, range_id) and
            std.mem.eql(u8, range.range_kind, range_kind) and
            std.mem.eql(u8, range.artifact_name, artifact_name))
        {
            return range;
        }
    }
    return null;
}

fn countKeysNotIn(keys: []const []const u8, exclude_keys: []const []const u8) usize {
    var count: usize = 0;
    for (keys) |key| {
        if (!runtimeContainsConstKey(exclude_keys, key)) count += 1;
    }
    return count;
}

fn documentExtractionRangeRoute(
    ranges: []const types.DocumentArtifactChildRange,
    range_id: []const u8,
    range_kind: []const u8,
    artifact_name: []const u8,
) DocumentExtractionRangeRoute {
    const range = findDocumentArtifactChildRange(ranges, range_id, range_kind, artifact_name) orelse return .{ .range_id = range_id };
    return .{
        .range_id = range_id,
        .route_status = range.route_status orelse "local_committed",
        .owner_group_id = range.owner_group_id orelse 0,
    };
}

fn unitDescriptorFingerprintMatches(descriptors: []const DocumentExtractionUnitDescriptor, key: []const u8, fingerprint: []const u8) bool {
    if (fingerprint.len == 0) return false;
    for (descriptors) |descriptor| {
        if (std.mem.eql(u8, descriptor.key, key) and std.mem.eql(u8, descriptor.fingerprint, fingerprint)) return true;
    }
    return false;
}

fn countUnitDescriptorsByFingerprintMatch(
    descriptors: []const DocumentExtractionUnitDescriptor,
    comparison: []const DocumentExtractionUnitDescriptor,
    want_match: bool,
) usize {
    var count: usize = 0;
    for (descriptors) |descriptor| {
        const matched = unitDescriptorFingerprintMatches(comparison, descriptor.key, descriptor.fingerprint);
        if (matched == want_match) count += 1;
    }
    return count;
}

fn appendDocumentExtractionUnitMergeOperation(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first_operation: *bool,
    op: []const u8,
    artifact_name: []const u8,
    descriptors: []const DocumentExtractionUnitDescriptor,
    comparison: []const DocumentExtractionUnitDescriptor,
    want_fingerprint_match: bool,
) !void {
    const count = countUnitDescriptorsByFingerprintMatch(descriptors, comparison, want_fingerprint_match);
    if (count == 0) return;

    var first_key: ?[]const u8 = null;
    var last_key: ?[]const u8 = null;
    for (descriptors) |descriptor| {
        const matched = unitDescriptorFingerprintMatches(comparison, descriptor.key, descriptor.fingerprint);
        if (matched != want_fingerprint_match) continue;
        if (first_key == null) first_key = descriptor.key;
        last_key = descriptor.key;
    }

    if (first_operation.*) {
        first_operation.* = false;
    } else {
        try out.append(alloc, ',');
    }

    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, out, &first, "op", op);
    try appendJsonFieldString(alloc, out, &first, "range_kind", "unit");
    try appendJsonFieldString(alloc, out, &first, "artifact_name", artifact_name);
    try appendJsonFieldString(alloc, out, &first, "first_key", first_key.?);
    try appendJsonFieldString(alloc, out, &first, "last_key", last_key.?);
    try appendJsonFieldUsize(alloc, out, &first, "key_count", count);
    try appendJsonFieldBool(alloc, out, &first, "fingerprint_match", want_fingerprint_match);
    try out.append(alloc, '}');
}

fn appendDocumentExtractionMergeOperation(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first_operation: *bool,
    op: []const u8,
    range_kind: []const u8,
    artifact_name: []const u8,
    keys: []const []const u8,
    exclude_keys: []const []const u8,
) !void {
    const count = countKeysNotIn(keys, exclude_keys);
    if (count == 0) return;

    var first_key: ?[]const u8 = null;
    var last_key: ?[]const u8 = null;
    for (keys) |key| {
        if (runtimeContainsConstKey(exclude_keys, key)) continue;
        if (first_key == null) first_key = key;
        last_key = key;
    }

    if (first_operation.*) {
        first_operation.* = false;
    } else {
        try out.append(alloc, ',');
    }

    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, out, &first, "op", op);
    try appendJsonFieldString(alloc, out, &first, "range_kind", range_kind);
    try appendJsonFieldString(alloc, out, &first, "artifact_name", artifact_name);
    try appendJsonFieldString(alloc, out, &first, "first_key", first_key.?);
    try appendJsonFieldString(alloc, out, &first, "last_key", last_key.?);
    try appendJsonFieldUsize(alloc, out, &first, "key_count", count);
    try out.append(alloc, '}');
}

const DocumentExtractionLastError = struct {
    code: []const u8,
    message: []const u8,
};

fn documentExtractionManifestPayloadAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    source_url: []const u8,
    fingerprint: []const u8,
    extraction: document_extraction_mod.Result,
    unit_text_lengths: []const usize,
    unit_keys: []const []const u8,
    unit_descriptors: []const DocumentExtractionUnitDescriptor,
    chunk_keys: []const []const u8,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
    previous_unit_keys: []const []const u8,
    previous_unit_descriptors: []const DocumentExtractionUnitDescriptor,
    previous_chunk_keys: []const []const u8,
    child_ranges_override: []const types.DocumentArtifactChildRange,
    manifest_generation: u64,
    from_generation: u64,
    to_generation: u64,
    merge_status: []const u8,
    last_error: ?DocumentExtractionLastError,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "_parent_doc_key", doc_key);
    try appendJsonFieldString(alloc, &out, &first, "_artifact_name", artifact_name);
    try appendJsonFieldString(alloc, &out, &first, "artifact_type", "document_units");
    try appendJsonFieldU64(alloc, &out, &first, "manifest_version", 2);
    try appendJsonFieldU64(alloc, &out, &first, "generation", manifest_generation);
    try appendJsonFieldString(alloc, &out, &first, "source_url", source_url);
    try appendJsonFieldString(alloc, &out, &first, "source_fingerprint", fingerprint);
    try appendJsonFieldString(alloc, &out, &first, "content_type", extraction.content_type);
    try appendJsonFieldString(alloc, &out, &first, "route_type", extraction.route_type);
    if (extraction.unsupported_reason.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "unsupported_reason", extraction.unsupported_reason);
    }
    if (last_error) |value| {
        try appendJsonFieldName(alloc, &out, &first, "last_error");
        try out.append(alloc, '{');
        var error_first = true;
        try appendJsonFieldString(alloc, &out, &error_first, "code", value.code);
        try appendJsonFieldString(alloc, &out, &error_first, "message", value.message);
        try out.append(alloc, '}');
    }
    try appendJsonFieldUsize(alloc, &out, &first, "unit_count", if (unit_text_lengths.len > 0) unit_text_lengths.len else extraction.units.len);
    try appendJsonFieldUsize(alloc, &out, &first, "chunk_count", chunk_keys.len);
    try appendJsonFieldName(alloc, &out, &first, "child_ranges");
    try out.append(alloc, '[');
    if (child_ranges_override.len > 0) {
        try appendDocumentExtractionExistingRanges(alloc, &out, child_ranges_override);
    } else {
        try appendDocumentExtractionRangeDescriptors(alloc, &out, artifact_name, unit_keys, chunk_keys, extraction.units, unit_text_lengths, previous_child_ranges);
    }
    try out.append(alloc, ']');
    try appendJsonFieldName(alloc, &out, &first, "range_policy");
    try appendDocumentExtractionRangePolicy(alloc, &out);
    try appendJsonFieldName(alloc, &out, &first, "merge_plan");
    try out.append(alloc, '{');
    var merge_first = true;
    try appendJsonFieldU64(alloc, &out, &merge_first, "plan_version", 1);
    try appendJsonFieldU64(alloc, &out, &merge_first, "from_generation", from_generation);
    try appendJsonFieldU64(alloc, &out, &merge_first, "to_generation", to_generation);
    try appendJsonFieldString(alloc, &out, &merge_first, "status", merge_status);
    try appendJsonFieldString(alloc, &out, &merge_first, "operation_granularity", "unit_fingerprint");
    try appendJsonFieldName(alloc, &out, &merge_first, "operations");
    try out.append(alloc, '[');
    var first_operation = true;
    try appendDocumentExtractionUnitMergeOperation(alloc, &out, &first_operation, "keep", artifact_name, unit_descriptors, previous_unit_descriptors, true);
    try appendDocumentExtractionUnitMergeOperation(alloc, &out, &first_operation, "upsert", artifact_name, unit_descriptors, previous_unit_descriptors, false);
    try appendDocumentExtractionMergeOperation(alloc, &out, &first_operation, "upsert", "chunk", "derived_chunks", chunk_keys, &.{});
    try appendDocumentExtractionMergeOperation(alloc, &out, &first_operation, "delete", "unit", artifact_name, previous_unit_keys, unit_keys);
    try appendDocumentExtractionMergeOperation(alloc, &out, &first_operation, "delete", "chunk", "derived_chunks", previous_chunk_keys, chunk_keys);
    try out.append(alloc, ']');
    try out.append(alloc, '}');
    try appendJsonFieldName(alloc, &out, &first, "coverage_plan");
    try out.append(alloc, '{');
    var coverage_first = true;
    try appendJsonFieldU64(alloc, &out, &coverage_first, "plan_version", 1);
    try appendJsonFieldString(alloc, &out, &coverage_first, "full_text_replay", "stored_artifact_required");
    try appendJsonFieldBool(alloc, &out, &coverage_first, "full_text_replay_suppressed", false);
    try appendJsonFieldBool(alloc, &out, &coverage_first, "watermark_required_before_suppression", true);
    try out.append(alloc, '}');
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn graphAssetStateKeyAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8, artifact_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.graph_asset_state_kind);
    try internal_keys.appendEncodedComponent(&list, alloc, index_name);
    try internal_keys.appendEncodedComponent(&list, alloc, artifact_name);
    return try list.toOwnedSlice(alloc);
}

fn runtimeMentionGraphStateNameAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}\x1fresolution_mentions\x1f{s}", .{ source_artifact, resolution_artifact });
}

fn runtimeResolutionMentionStateKeysForGraphSourceAlloc(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    index_name: []const u8,
    source: index_manager_mod.GraphArtifactSource,
) ![][]const u8 {
    if (source.mention_edge_type.len == 0) return try runtime.alloc.alloc([]const u8, 0);

    var protected = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (protected.items) |key| runtime.alloc.free(@constCast(key));
        protected.deinit(runtime.alloc);
    }

    for (runtime.index_manager.resolvers.items) |cfg| {
        if (!std.mem.eql(u8, cfg.source_artifact, source.artifact_name)) continue;

        const state_name = try runtimeMentionGraphStateNameAlloc(runtime.alloc, source.artifact_name, cfg.resolution_artifact);
        defer runtime.alloc.free(state_name);
        const state_key = try graphAssetStateKeyAlloc(runtime.alloc, doc_key, index_name, state_name);
        defer runtime.alloc.free(state_key);

        const state_keys = try loadGraphAssetStateKeysAlloc(runtime, state_key) orelse continue;
        defer freeOwnedConstKeySlice(runtime.alloc, state_keys);
        for (state_keys) |key| {
            try protected.append(runtime.alloc, try runtime.alloc.dupe(u8, key));
        }
    }

    return try protected.toOwnedSlice(runtime.alloc);
}

fn encodeGraphAssetStateKeysAlloc(alloc: Allocator, writes: []const KVPair) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendU32Big(&out, alloc, @intCast(writes.len));
    for (writes) |write| {
        try appendU32Big(&out, alloc, @intCast(write.key.len));
        try out.appendSlice(alloc, write.key);
    }
    return try out.toOwnedSlice(alloc);
}

fn loadGraphAssetStateKeysAlloc(runtime: *EnrichmentRuntime, state_key: []const u8) !?[][]const u8 {
    const alloc = runtime.alloc;
    const raw = storeGetAlloc(runtime, state_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return null,
    };
    defer alloc.free(raw);
    var pos: usize = 0;
    const count = readU32Big(raw, &pos) catch return null;
    const keys = try alloc.alloc([]const u8, count);
    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |key| alloc.free(@constCast(key));
        alloc.free(keys);
    }
    for (keys) |*key| {
        const len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (len > raw.len - pos) return error.InvalidGraphAssetState;
        key.* = try alloc.dupe(u8, raw[pos..][0..len]);
        pos += len;
        initialized += 1;
    }
    return keys;
}

fn readU32Big(bytes: []const u8, pos: *usize) !u32 {
    if (bytes.len - pos.* < @sizeOf(u32)) return error.EndOfStream;
    const value = std.mem.readInt(u32, bytes[pos.*..][0..@sizeOf(u32)], .big);
    pos.* += @sizeOf(u32);
    return value;
}

fn appendU32Big(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: u32) !void {
    const be = std.mem.nativeToBig(u32, value);
    try out.appendSlice(alloc, std.mem.asBytes(&be));
}

fn freeOwnedConstKeySlice(alloc: Allocator, keys: []const []const u8) void {
    for (keys) |key| alloc.free(@constCast(key));
    if (keys.len > 0) alloc.free(keys);
}

fn recordArtifactBytes(runtime: *EnrichmentRuntime, kind: enrichment_artifact_codec.Kind, byte_count: usize) void {
    const bytes: u64 = @intCast(byte_count);
    switch (kind) {
        .dense_embedding => runtime.dense_artifact_bytes_written += bytes,
        .sparse_embedding => runtime.sparse_artifact_bytes_written += bytes,
        .chunk_json, .asset => runtime.chunk_artifact_bytes_written += bytes,
        .graph_edge => {},
    }
}

fn writeChunkEmbeddingArtifacts(
    runtime: *EnrichmentRuntime,
    parent_doc_key: []const u8,
    source_field: []const u8,
    artifact_name: []const u8,
    embeddings: []derived_types.DerivedDenseEmbeddingWrite,
) !void {
    for (embeddings) |*embedding| {
        if (embedding.artifact_key != null or embedding.vector.len == 0) continue;
        const artifact_key = try embeddingArtifactKey(runtime, embedding.doc_key, artifact_name);
        errdefer runtime.alloc.free(artifact_key);
        try writeEmbeddingArtifact(runtime, .{
            .base_key = embedding.doc_key,
            .parent_doc_key = parent_doc_key,
            .artifact_name = artifact_name,
            .source_field = source_field,
            .source_key = embedding.doc_key,
            .source_hash = try chunkArtifactSourceHash(runtime, embedding.doc_key, source_field),
            .vector = embedding.vector,
        });
        embedding.artifact_key = artifact_key;
    }
}

fn deleteStaleChunkEmbeddingArtifacts(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    chunk_artifact_name: []const u8,
    embedding_artifact_name: []const u8,
    desired_chunk_keys: []const []const u8,
) !StaleEmbeddingDeletes {
    const prefix = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, doc_key, "chunk", chunk_artifact_name);
    defer runtime.alloc.free(prefix);
    const existing = try backend_scan.scanPrefix(runtime.alloc, &runtime.store, prefix);
    defer backend_scan.freeResults(runtime.alloc, existing);
    if (existing.len == 0) return .{};

    var stale_vector_keys = std.ArrayListUnmanaged([]u8).empty;
    errdefer freeKeyList(runtime.alloc, stale_vector_keys.items);
    var artifact_delete_keys = std.ArrayListUnmanaged([]u8).empty;
    errdefer freeKeyList(runtime.alloc, artifact_delete_keys.items);

    for (existing) |entry| {
        if (!internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) continue;
        if (!internal_keys.matchesDerivedEmbeddingArtifactName(entry.key, embedding_artifact_name)) continue;
        if (derivedEmbeddingBelongsToDesiredChunk(entry.key, desired_chunk_keys)) continue;
        if (try internal_keys.derivedEmbeddingBaseKeyAlloc(runtime.alloc, entry.key)) |base_key| {
            try appendUniqueOwnedKey(runtime.alloc, &stale_vector_keys, base_key);
        }
        try appendUniqueDupeKey(runtime.alloc, &artifact_delete_keys, entry.key);
    }
    return .{
        .vector_keys = try stale_vector_keys.toOwnedSlice(runtime.alloc),
        .artifact_delete_keys = try artifact_delete_keys.toOwnedSlice(runtime.alloc),
    };
}

fn deleteStaleChunkArtifacts(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    artifact_name: []const u8,
    desired_chunk_keys: []const []const u8,
) ![][]u8 {
    const prefix = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, doc_key, "chunk", artifact_name);
    defer runtime.alloc.free(prefix);
    const existing = try backend_scan.scanPrefix(runtime.alloc, &runtime.store, prefix);
    defer backend_scan.freeResults(runtime.alloc, existing);
    if (existing.len == 0) return try runtime.alloc.alloc([]u8, 0);

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (deletes.items) |key| runtime.alloc.free(@constCast(key));
        deletes.deinit(runtime.alloc);
    }
    var stale_vector_keys = std.ArrayListUnmanaged([]u8).empty;
    errdefer freeKeyList(runtime.alloc, stale_vector_keys.items);

    for (existing) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) {
            if (keyInList(entry.key, desired_chunk_keys)) continue;
            try appendUniqueDupeKey(runtime.alloc, &stale_vector_keys, entry.key);
            try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, entry.key));
            continue;
        }
        if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) {
            if (derivedEmbeddingBelongsToDesiredChunk(entry.key, desired_chunk_keys)) continue;
            if (try internal_keys.derivedEmbeddingBaseKeyAlloc(runtime.alloc, entry.key)) |base_key| {
                try appendUniqueOwnedKey(runtime.alloc, &stale_vector_keys, base_key);
            }
            try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, entry.key));
        }
    }
    if (deletes.items.len > 0) try storePutBatchWithRetry(runtime, &.{}, deletes.items);
    return try stale_vector_keys.toOwnedSlice(runtime.alloc);
}

fn chunkArtifactSourceHash(runtime: *EnrichmentRuntime, chunk_key: []const u8, source_field: []const u8) !?u64 {
    const raw = storeGetAlloc(runtime, chunk_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return null,
    };
    defer runtime.alloc.free(raw);

    const parsed = std.json.parseFromSlice(std.json.Value, runtime.alloc, raw, .{}) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const source = parsed.value.object.get(source_field) orelse return null;
    if (source != .string) return null;
    return enrichment_artifact_codec.hashSource(source.string);
}

fn chunkPayloadHasText(alloc: Allocator, payload: []const u8, source_field: []const u8) !bool {
    const parsed = std.json.parseFromSlice(std.json.Value, alloc, payload, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const source = parsed.value.object.get(source_field) orelse return false;
    return source == .string and source.string.len > 0;
}

fn chunkPayloadTextAlloc(alloc: Allocator, payload: []const u8, source_field: []const u8) !?[]u8 {
    const parsed = std.json.parseFromSlice(std.json.Value, alloc, payload, .{}) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const source = parsed.value.object.get(source_field) orelse return null;
    if (source != .string or source.string.len == 0) return null;
    return try alloc.dupe(u8, source.string);
}

fn storedChunkEmbeddingSourcesForRequest(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_name: []const u8,
) ![]ChunkEmbeddingSource {
    const prefix = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, request.doc_key, "chunk", artifact_name);
    defer runtime.alloc.free(prefix);
    const existing = try backend_scan.scanPrefix(runtime.alloc, &runtime.store, prefix);
    defer backend_scan.freeResults(runtime.alloc, existing);

    var sources = std.ArrayListUnmanaged(ChunkEmbeddingSource).empty;
    errdefer {
        for (sources.items) |source| {
            runtime.alloc.free(source.key);
            runtime.alloc.free(source.text);
        }
        sources.deinit(runtime.alloc);
    }

    for (existing) |entry| {
        if (!internal_keys.isChunkArtifactRecordKey(entry.key)) continue;
        const text = (try chunkPayloadTextAlloc(runtime.alloc, entry.value, request.source_field)) orelse continue;
        errdefer runtime.alloc.free(text);
        try sources.append(runtime.alloc, .{
            .key = try runtime.alloc.dupe(u8, entry.key),
            .text = text,
        });
    }
    return try sources.toOwnedSlice(runtime.alloc);
}

fn chunkEmbeddingSourceSetForRequest(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_name: []const u8,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
) !ChunkEmbeddingSourceSet {
    if (requestUsesMaterializedChunkArtifact(runtime, artifact_name)) {
        return error.InvalidEnrichmentConfig;
    }

    const chunks = try getOrCreateRequestChunks(runtime, request, chunk_cache);
    if (chunks.len > 0) {
        var sources = std.ArrayListUnmanaged(ChunkEmbeddingSource).empty;
        errdefer {
            for (sources.items) |source| {
                runtime.alloc.free(source.key);
                runtime.alloc.free(source.text);
            }
            sources.deinit(runtime.alloc);
        }
        var keys = std.ArrayListUnmanaged([]u8).empty;
        errdefer freeKeyList(runtime.alloc, keys.items);

        for (chunks) |chunk| {
            const key = try internal_keys.chunkArtifactKeyAlloc(runtime.alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id));
            var key_owned = true;
            errdefer if (key_owned) runtime.alloc.free(key);
            const desired_key = try runtime.alloc.dupe(u8, key);
            var desired_key_owned = true;
            errdefer if (desired_key_owned) runtime.alloc.free(desired_key);
            try keys.append(runtime.alloc, desired_key);
            desired_key_owned = false;
            const source = chunk.text orelse {
                runtime.alloc.free(key);
                key_owned = false;
                continue;
            };
            if (source.len == 0) {
                runtime.alloc.free(key);
                key_owned = false;
                continue;
            }
            const text = try runtime.alloc.dupe(u8, source);
            var text_owned = true;
            errdefer if (text_owned) runtime.alloc.free(text);
            try sources.append(runtime.alloc, .{
                .key = key,
                .text = text,
            });
            key_owned = false;
            text_owned = false;
        }

        const owned_sources = try sources.toOwnedSlice(runtime.alloc);
        errdefer freeChunkEmbeddingSources(runtime.alloc, owned_sources);
        const owned_keys = try keys.toOwnedSlice(runtime.alloc);
        return .{
            .sources = owned_sources,
            .desired_chunk_keys = owned_keys,
        };
    }

    const sources = try storedChunkEmbeddingSourcesForRequest(runtime, request, artifact_name);
    errdefer freeChunkEmbeddingSources(runtime.alloc, sources);
    const keys = try runtime.alloc.alloc([]u8, sources.len);
    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |key| runtime.alloc.free(key);
        runtime.alloc.free(keys);
    }
    for (sources, 0..) |source, i| {
        keys[i] = try runtime.alloc.dupe(u8, source.key);
        initialized += 1;
    }

    return .{
        .sources = sources,
        .desired_chunk_keys = keys,
    };
}

fn chunkKeysForChunks(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8, chunks: []const chunker_mod.Chunk) ![][]u8 {
    const keys = try alloc.alloc([]u8, chunks.len);
    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |key| alloc.free(key);
        alloc.free(keys);
    }
    for (chunks, 0..) |chunk, i| {
        keys[i] = try internal_keys.chunkArtifactKeyAlloc(alloc, doc_key, artifact_name, @intCast(chunk.chunk_id));
        initialized += 1;
    }
    return keys;
}

fn freeKeyList(alloc: Allocator, keys: []const []u8) void {
    for (keys) |key| alloc.free(key);
    alloc.free(keys);
}

fn freeOwnedKeySet(alloc: Allocator, keys: *std.StringHashMapUnmanaged(void)) void {
    var it = keys.iterator();
    while (it.next()) |entry| alloc.free(@constCast(entry.key_ptr.*));
    keys.deinit(alloc);
    keys.* = .empty;
}

fn putOwnedKeySetDupeKey(alloc: Allocator, keys: *std.StringHashMapUnmanaged(void), key: []const u8) !void {
    if (keys.contains(key)) return;
    const owned = try alloc.dupe(u8, key);
    errdefer alloc.free(owned);
    try keys.put(alloc, owned, {});
}

fn keyAfterAlloc(alloc: Allocator, key: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, key.len + 1);
    @memcpy(out[0..key.len], key);
    out[key.len] = 0;
    return out;
}

fn keyInList(key: []const u8, keys: []const []const u8) bool {
    for (keys) |candidate| {
        if (std.mem.eql(u8, key, candidate)) return true;
    }
    return false;
}

fn enrichmentConfigLessThan(_: void, lhs: types.EnrichmentConfig, rhs: types.EnrichmentConfig) bool {
    const lhs_kind = @intFromEnum(lhs.kind);
    const rhs_kind = @intFromEnum(rhs.kind);
    if (lhs_kind != rhs_kind) return lhs_kind < rhs_kind;
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn enrichmentCatalogConfigHash(alloc: Allocator, index_manager: *const index_manager_mod.IndexManager) !u64 {
    const configs = try index_manager.listEnrichmentsPublic(alloc);
    defer types.freeEnrichmentConfigs(alloc, configs);
    std.mem.sort(types.EnrichmentConfig, configs, {}, enrichmentConfigLessThan);

    var hasher = std.hash.Wyhash.init(0x41454a4341540001);
    var count_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &count_buf, configs.len, .little);
    hasher.update(&count_buf);
    for (configs) |cfg| {
        var hash_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &hash_buf, types.enrichmentConfigHash(cfg), .little);
        hasher.update(&hash_buf);
    }
    return hasher.final();
}

const dense_artifact_target_counter_prefix = "\x00\x00__metadata__:dense_artifact_target_count:";

fn denseArtifactTargetCounterKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ dense_artifact_target_counter_prefix, index_name });
}

fn denseArtifactTargetsForArtifact(
    runtime: *EnrichmentRuntime,
    artifact_name: []const u8,
    dims: u32,
    out: *std.ArrayListUnmanaged(usize),
) !void {
    for (runtime.index_manager.dense_indexes.items, 0..) |*entry, dense_index_idx| {
        const artifact_backed = entry.external or entry.chunk_name != null or entry.embedding_name != null;
        if (!artifact_backed) continue;
        if (entry.dims != dims) continue;
        if (std.mem.eql(u8, entry.config.name, artifact_name) or
            (entry.embedding_name != null and std.mem.eql(u8, entry.embedding_name.?, artifact_name)))
        {
            try out.append(runtime.alloc, dense_index_idx);
        }
    }
}

fn loadDenseArtifactTargetCounterTxn(runtime: *EnrichmentRuntime, txn: anytype, index_name: []const u8) !u64 {
    var mutable_txn = txn;
    const key = try denseArtifactTargetCounterKeyAlloc(runtime.alloc, index_name);
    defer runtime.alloc.free(key);
    const raw = mutable_txn.get(key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    if (raw.len != 8) return error.InvalidDenseArtifactTargetCounter;
    return std.mem.readInt(u64, raw[0..8], .little);
}

fn saveDenseArtifactTargetCounterTxn(runtime: *EnrichmentRuntime, txn: anytype, index_name: []const u8, count: u64) !void {
    var mutable_txn = txn;
    const key = try denseArtifactTargetCounterKeyAlloc(runtime.alloc, index_name);
    defer runtime.alloc.free(key);
    var value: [8]u8 = undefined;
    std.mem.writeInt(u64, &value, count, .little);
    try mutable_txn.put(key, &value);
}

fn applyDenseArtifactCounterDeltaRuntime(
    runtime: *EnrichmentRuntime,
    txn: anytype,
    counts: *std.AutoHashMapUnmanaged(usize, u64),
    artifact_key: []const u8,
    artifact_value: ?[]const u8,
    delta: i64,
) !void {
    if (delta == 0) return;
    var identity = (try artifact_ids.decodeEmbeddingArtifactIdentityAlloc(runtime.alloc, artifact_key)) orelse return;
    defer identity.deinit(runtime.alloc);
    const value = artifact_value orelse return;
    const dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(value) catch return;
    if (dims == 0) return;

    var targets = std.ArrayListUnmanaged(usize).empty;
    defer targets.deinit(runtime.alloc);
    try denseArtifactTargetsForArtifact(runtime, identity.embedding_name, dims, &targets);
    for (targets.items) |dense_index_idx| {
        const entry = &runtime.index_manager.dense_indexes.items[dense_index_idx];
        const gop = try counts.getOrPut(runtime.alloc, dense_index_idx);
        if (!gop.found_existing) {
            gop.value_ptr.* = try loadDenseArtifactTargetCounterTxn(runtime, txn, entry.config.name);
        }
        if (delta > 0) {
            gop.value_ptr.* +|= @as(u64, @intCast(delta));
        } else {
            gop.value_ptr.* -|= @as(u64, @intCast(-delta));
        }
    }
}

fn updateDenseArtifactTargetCountersTxn(
    runtime: *EnrichmentRuntime,
    txn: anytype,
    writes: []const KVPair,
    deletes: []const []const u8,
) !void {
    if (runtime.index_manager.dense_indexes.items.len == 0) return;
    var mutable_txn = txn;
    var counts = std.AutoHashMapUnmanaged(usize, u64){};
    defer counts.deinit(runtime.alloc);

    for (deletes) |key| {
        const old_value = mutable_txn.get(key) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        try applyDenseArtifactCounterDeltaRuntime(runtime, mutable_txn, &counts, key, old_value, -1);
    }
    for (writes) |write| {
        const old_value = mutable_txn.get(write.key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (old_value) |value| {
            try applyDenseArtifactCounterDeltaRuntime(runtime, mutable_txn, &counts, write.key, value, -1);
        }
        try applyDenseArtifactCounterDeltaRuntime(runtime, mutable_txn, &counts, write.key, write.value, 1);
    }

    var it = counts.iterator();
    while (it.next()) |entry| {
        const dense_entry = &runtime.index_manager.dense_indexes.items[entry.key_ptr.*];
        try saveDenseArtifactTargetCounterTxn(runtime, mutable_txn, dense_entry.config.name, entry.value_ptr.*);
    }
}

fn appendUniqueDupeKey(alloc: Allocator, keys: *std.ArrayListUnmanaged([]u8), key: []const u8) !void {
    for (keys.items) |candidate| {
        if (std.mem.eql(u8, candidate, key)) return;
    }
    try keys.append(alloc, try alloc.dupe(u8, key));
}

fn appendUniqueOwnedKey(alloc: Allocator, keys: *std.ArrayListUnmanaged([]u8), key: []u8) !void {
    errdefer alloc.free(key);
    for (keys.items) |candidate| {
        if (std.mem.eql(u8, candidate, key)) {
            alloc.free(key);
            return;
        }
    }
    try keys.append(alloc, key);
}

fn derivedEmbeddingBelongsToDesiredChunk(key: []const u8, desired_chunk_keys: []const []const u8) bool {
    for (desired_chunk_keys) |chunk_key| {
        if (std.mem.startsWith(u8, key, chunk_key)) return true;
    }
    return false;
}

fn derivedEmbeddingBelongsToDesiredChunkSet(
    alloc: Allocator,
    key: []const u8,
    desired_chunk_keys: *const std.StringHashMapUnmanaged(void),
) !bool {
    const base_key = (try internal_keys.derivedEmbeddingBaseKeyAlloc(alloc, key)) orelse return false;
    defer alloc.free(base_key);
    return desired_chunk_keys.contains(base_key);
}

fn assetSourceIndexKeyForArtifactAlloc(alloc: Allocator, artifact_key: []const u8) !?[]u8 {
    const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(alloc, artifact_key)) orelse return null;
    defer {
        alloc.free(parsed.doc_key);
        alloc.free(parsed.artifact_name);
    }
    return try internal_keys.assetArtifactSourceIndexKeyAlloc(alloc, parsed.artifact_name, parsed.doc_key);
}

fn storePutWithRetry(runtime: *EnrichmentRuntime, key: []const u8, value: []const u8) !void {
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        storePut(runtime, key, value) catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        return;
    }
}

fn storePutBatchWithRetry(runtime: *EnrichmentRuntime, writes: []const KVPair, deletes: []const []const u8) !void {
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        storePutBatch(runtime, writes, deletes) catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        try runtime.store.sync(false);
        return;
    }
}

fn storeCoverageBatchWithRetry(runtime: *EnrichmentRuntime, writes: []const KVPair, deletes: []const []const u8) !void {
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        var batch = runtime.store.beginBatch() catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        var committed = false;
        defer if (!committed) batch.abort();
        for (writes) |write| try batch.put(write.key, write.value);
        for (deletes) |key| batch.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        batch.commit() catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        committed = true;
        try runtime.store.sync(false);
        return;
    }
}

fn saveAppliedSequenceWithRetry(runtime: *EnrichmentRuntime, scope: []const u8, sequence: u64) !void {
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        var checkpoint = enrichment_state.loadProjectionCheckpoint(runtime.alloc, runtime.store, scope) catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        checkpoint.applied_sequence = sequence;
        checkpoint.status = runtimeProjectionStatus(runtime.retrying, runtime.worker_failed);
        checkpoint.config_hash = try enrichmentCatalogConfigHash(runtime.alloc, runtime.index_manager);
        enrichment_state.saveProjectionCheckpoint(runtime.store, scope, checkpoint) catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        return;
    }
}

fn saveRuntimeStatusWithRetry(runtime: *EnrichmentRuntime, scope: []const u8, status: enrichment_state.RuntimeStatus) !void {
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        enrichment_state.saveRuntimeStatus(runtime.store, scope, status) catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        var checkpoint = enrichment_state.loadProjectionCheckpoint(runtime.alloc, runtime.store, scope) catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        checkpoint.status = runtimeProjectionStatus(status.retrying, status.worker_failed);
        checkpoint.config_hash = try enrichmentCatalogConfigHash(runtime.alloc, runtime.index_manager);
        enrichment_state.saveProjectionCheckpoint(runtime.store, scope, checkpoint) catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        return;
    }
}

fn coverageOutcomeName(outcome: CoverageOutcome) []const u8 {
    return @tagName(outcome);
}

fn initCoverageOutcomeTransition(runtime: *EnrichmentRuntime, index_name: []const u8, generation: u64, doc_key: []const u8, outcome: CoverageOutcome) !CoverageOutcomeTransition {
    var transition: CoverageOutcomeTransition = .{
        .index_name = try runtime.alloc.dupe(u8, index_name),
        .generation = generation,
        .outcome = outcome,
        .marker_key = undefined,
        .counter_keys = undefined,
    };
    var marker_initialized = false;
    var initialized_counters: usize = 0;
    errdefer {
        if (marker_initialized) runtime.alloc.free(transition.marker_key);
        for (transition.counter_keys[0..initialized_counters]) |key| runtime.alloc.free(key);
        runtime.alloc.free(transition.index_name);
    }
    transition.marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(runtime.alloc, index_name, generation, doc_key);
    marker_initialized = true;
    inline for (std.meta.tags(CoverageOutcome), 0..) |candidate_outcome, i| {
        transition.counter_keys[i] = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(runtime.alloc, index_name, generation, coverageOutcomeName(candidate_outcome));
        initialized_counters += 1;
    }
    return transition;
}

fn deinitCoverageOutcomeTransition(alloc: Allocator, transition: CoverageOutcomeTransition) void {
    alloc.free(transition.index_name);
    alloc.free(transition.marker_key);
    for (transition.counter_keys) |key| alloc.free(key);
}

fn markDerivedCoverageOutcomeForIndex(runtime: *EnrichmentRuntime, index_name: []const u8, doc_key: []const u8, outcome: CoverageOutcome) !void {
    const generation = runtime.index_manager.coverageGenerationForIndex(index_name) orelse return;
    const transition = try initCoverageOutcomeTransition(runtime, index_name, generation, doc_key, outcome);
    defer deinitCoverageOutcomeTransition(runtime.alloc, transition);
    try applyCoverageOutcomeTransitions(runtime, &.{transition});
}

fn markDerivedCoverageTerminalFailedForIndex(runtime: *EnrichmentRuntime, index_name: []const u8, doc_key: []const u8) !void {
    try markDerivedCoverageOutcomeForIndex(runtime, index_name, doc_key, .terminal_failed);
}

fn clearQueuedCoverageTransitions(
    alloc: Allocator,
    transitions: *std.ArrayListUnmanaged(CoverageOutcomeTransition),
    transition_keys: *std.StringHashMapUnmanaged(void),
) void {
    transition_keys.clearRetainingCapacity();
    for (transitions.items) |item| deinitCoverageOutcomeTransition(alloc, item);
    transitions.clearRetainingCapacity();
}

fn loadDerivedCoverageOutcomeCounter(runtime: *EnrichmentRuntime, counter_key: []const u8) !?u64 {
    const raw = storeGetAlloc(runtime, counter_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer runtime.alloc.free(raw);
    return try internal_keys.decodeDerivedCoverageOutcomeCount(raw);
}

fn scanDerivedCoverageOutcome(runtime: *EnrichmentRuntime, index_name: []const u8, generation: u64, outcome: CoverageOutcome) !u64 {
    const lower = try internal_keys.derivedCoverageOutcomeMarkerPrefixAlloc(runtime.alloc, index_name, generation);
    defer runtime.alloc.free(lower);
    const upper = try internal_keys.nextPrefixAlloc(runtime.alloc, lower);
    defer if (upper) |key| runtime.alloc.free(key);
    const upper_bound = if (upper) |key| key else "";

    var skipped: u64 = 0;
    const CountState = struct {
        count: *u64,
        outcome_name: []const u8,

        fn scan(ctx_ptr: ?*anyopaque, key: []const u8, value: []const u8) anyerror!backend_scan.ScanAction {
            _ = key;
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr orelse return error.InvalidArgument));
            if (std.mem.eql(u8, value, ctx.outcome_name)) ctx.count.* += 1;
            return .@"continue";
        }
    };
    var state = CountState{ .count = &skipped, .outcome_name = coverageOutcomeName(outcome) };
    try backend_scan.scanWithContext(&runtime.store, lower, upper_bound, .{}, &state, CountState.scan);
    return skipped;
}

fn derivedCoverageOutcomeCounterValue(runtime: *EnrichmentRuntime, counter_key: []const u8, index_name: []const u8, generation: u64, outcome: CoverageOutcome) !u64 {
    return (try loadDerivedCoverageOutcomeCounter(runtime, counter_key)) orelse
        try scanDerivedCoverageOutcome(runtime, index_name, generation, outcome);
}

fn queueDerivedCoverageOutcomeForIndex(runtime: *EnrichmentRuntime, window: *GeneratedReplayWindow, index_name: []const u8, doc_key: []const u8, outcome: CoverageOutcome) !void {
    const generation = runtime.index_manager.coverageGenerationForIndex(index_name) orelse return;
    const transition = try initCoverageOutcomeTransition(runtime, index_name, generation, doc_key, outcome);
    errdefer deinitCoverageOutcomeTransition(runtime.alloc, transition);
    const identity_key = transition.marker_key;
    if (window.coverage_transition_keys.getKey(identity_key)) |existing_key| {
        for (window.coverage_transitions.items) |*queued| {
            if (std.mem.eql(u8, queued.marker_key, existing_key)) {
                queued.outcome = outcome;
                break;
            }
        }
        deinitCoverageOutcomeTransition(runtime.alloc, transition);
        return;
    }
    try window.coverage_transitions.append(runtime.alloc, transition);
    errdefer _ = window.coverage_transitions.pop();
    try window.coverage_transition_keys.put(runtime.alloc, identity_key, {});
}

fn queueDerivedCoverageOutcome(runtime: *EnrichmentRuntime, window: *GeneratedReplayWindow, doc_key: []const u8, consumer_indexes: []const []const u8, outcome: CoverageOutcome) !void {
    for (consumer_indexes) |index_name| try queueDerivedCoverageOutcomeForIndex(runtime, window, index_name, doc_key, outcome);
}

fn markDerivedCoverageSkipped(runtime: *EnrichmentRuntime, window: *GeneratedReplayWindow, doc_key: []const u8, consumer_indexes: []const []const u8) !void {
    try queueDerivedCoverageOutcome(runtime, window, doc_key, consumer_indexes, .skipped);
}

fn queueDerivedCoverageProduced(runtime: *EnrichmentRuntime, window: *GeneratedReplayWindow, doc_key: []const u8, consumer_indexes: []const []const u8) !void {
    try queueDerivedCoverageOutcome(runtime, window, doc_key, consumer_indexes, .produced);
}

fn applyCoverageOutcomeTransitions(runtime: *EnrichmentRuntime, transitions: []const CoverageOutcomeTransition) !void {
    if (transitions.len == 0) return;

    const ordered = try runtime.alloc.dupe(CoverageOutcomeTransition, transitions);
    defer runtime.alloc.free(ordered);
    std.mem.sort(CoverageOutcomeTransition, ordered, {}, struct {
        fn lessThan(_: void, lhs: CoverageOutcomeTransition, rhs: CoverageOutcomeTransition) bool {
            const order = std.mem.order(u8, lhs.index_name, rhs.index_name);
            if (order != .eq) return order == .lt;
            return std.mem.order(u8, lhs.marker_key, rhs.marker_key) == .lt;
        }
    }.lessThan);

    var group_start: usize = 0;
    while (group_start < ordered.len) {
        var group_end = group_start + 1;
        while (group_end < ordered.len and std.mem.eql(u8, ordered[group_start].index_name, ordered[group_end].index_name)) : (group_end += 1) {}

        var apply_guard = runtime.index_manager.lockVectorIndexApply(ordered[group_start].index_name) catch |err| switch (err) {
            error.IndexNotFound => {
                group_start = group_end;
                continue;
            },
        };

        const current_generation = runtime.index_manager.coverageGenerationForIndex(ordered[group_start].index_name);
        var retained: usize = 0;
        for (ordered[group_start..group_end]) |transition| {
            if (current_generation == null or transition.generation != current_generation.?) continue;
            ordered[group_start + retained] = transition;
            retained += 1;
        }
        if (retained > 0) {
            applyCoverageOutcomeTransitionsForIndex(runtime, ordered[group_start .. group_start + retained]) catch |err| {
                apply_guard.unlock();
                return err;
            };
        }
        apply_guard.unlock();
        group_start = group_end;
    }
}

fn applyCoverageOutcomeTransitionsForIndex(runtime: *EnrichmentRuntime, transitions: []const CoverageOutcomeTransition) !void {
    if (transitions.len == 0) return;

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer writes.deinit(runtime.alloc);

    const CounterState = struct {
        key: []const u8,
        outcome: CoverageOutcome,
        count: u64,
        value: [8]u8 = undefined,
    };
    var counter_states = std.ArrayListUnmanaged(CounterState).empty;
    defer counter_states.deinit(runtime.alloc);
    var counter_indexes = std.StringHashMapUnmanaged(usize).empty;
    defer counter_indexes.deinit(runtime.alloc);
    var seen_transitions = std.StringHashMapUnmanaged(void).empty;
    defer seen_transitions.deinit(runtime.alloc);
    const counterState = struct {
        fn get(
            runtime_value: *EnrichmentRuntime,
            states: *std.ArrayListUnmanaged(CounterState),
            indexes: *std.StringHashMapUnmanaged(usize),
            transition: CoverageOutcomeTransition,
            outcome: CoverageOutcome,
        ) !usize {
            const outcome_index = @intFromEnum(outcome);
            const counter_key = transition.counter_keys[outcome_index];
            if (indexes.get(counter_key)) |index| return index;
            const current_count = (try loadDerivedCoverageOutcomeCounter(runtime_value, counter_key)) orelse
                try scanDerivedCoverageOutcome(runtime_value, transition.index_name, transition.generation, outcome);
            const index = states.items.len;
            try states.append(runtime_value.alloc, .{ .key = counter_key, .outcome = outcome, .count = current_count });
            try indexes.put(runtime_value.alloc, counter_key, index);
            return index;
        }
    }.get;

    var skipped_delta: i64 = 0;
    for (transitions) |transition| {
        const target_outcome = transition.outcome;
        if (seen_transitions.contains(transition.marker_key)) continue;
        try seen_transitions.put(runtime.alloc, transition.marker_key, {});

        inline for (std.meta.tags(CoverageOutcome)) |candidate_outcome| {
            _ = try counterState(runtime, &counter_states, &counter_indexes, transition, candidate_outcome);
        }

        const existing_value = storeGetAlloc(runtime, transition.marker_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        defer if (existing_value) |value| runtime.alloc.free(value);
        const existing_outcome: ?CoverageOutcome = if (existing_value) |value|
            std.meta.stringToEnum(CoverageOutcome, value) orelse return error.InvalidDerivedCoverageOutcome
        else
            null;

        if (existing_outcome == null or existing_outcome.? != target_outcome) {
            if (existing_outcome) |previous_outcome| {
                const previous_state_index = try counterState(runtime, &counter_states, &counter_indexes, transition, previous_outcome);
                if (counter_states.items[previous_state_index].count == 0) return error.InvalidDerivedCoverageCounter;
                counter_states.items[previous_state_index].count -= 1;
                if (previous_outcome == .skipped) skipped_delta -= 1;
            }
            const state_index = try counterState(runtime, &counter_states, &counter_indexes, transition, target_outcome);
            counter_states.items[state_index].count +|= 1;
            try writes.append(runtime.alloc, .{
                .key = transition.marker_key,
                .value = coverageOutcomeName(target_outcome),
            });
            if (target_outcome == .skipped) skipped_delta += 1;
        }
    }

    if (writes.items.len == 0) return;
    for (counter_states.items) |*state| {
        try writes.append(runtime.alloc, .{
            .key = state.key,
            .value = internal_keys.encodeDerivedCoverageOutcomeCount(&state.value, state.count),
        });
    }
    try storeCoverageBatchWithRetry(runtime, writes.items, &.{});
    if (skipped_delta > 0) {
        runtime.skipped_source_count +|= @intCast(skipped_delta);
    } else if (skipped_delta < 0) {
        runtime.skipped_source_count -|= @intCast(-skipped_delta);
    }
}

fn applyQueuedCoverageTransitionsAfterReplayAppend(runtime: *EnrichmentRuntime, transitions: []const CoverageOutcomeTransition) !void {
    try applyCoverageOutcomeTransitions(runtime, transitions);
}

test "derived coverage outcome transitions are exclusive and idempotent" {
    const alloc = std.testing.allocator;

    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer store.deinit();
    var erased_store = try backend_erased.storeFrom(alloc, store);
    defer erased_store.deinit();
    var coverage_apply_mutex = apply_rw_lock_mod.ApplyRwLock{};
    var runtime = EnrichmentRuntime{
        .alloc = alloc,
        .io_impl = null,
        .store = erased_store,
        .owns_store = false,
        .change_journal = undefined,
        .replay_source = undefined,
        .index_manager = undefined,
        .write_ctx = undefined,
        .write_fn = undefined,
        .notify_ctx = undefined,
        .notify_fn = undefined,
        .config = .{},
        .ownership = undefined,
        .coverage_apply_mutex = &coverage_apply_mutex,
    };

    var transition = try initCoverageOutcomeTransition(&runtime, "visual", 7, "doc:1", .skipped);
    defer deinitCoverageOutcomeTransition(alloc, transition);

    try applyCoverageOutcomeTransitionsForIndex(&runtime, &.{transition});
    const stored_outcome = try storeGetAlloc(&runtime, transition.marker_key);
    defer runtime.alloc.free(stored_outcome);
    try std.testing.expectEqualStrings("skipped", stored_outcome);
    try std.testing.expectEqual(@as(?u64, 0), try loadDerivedCoverageOutcomeCounter(&runtime, transition.counter_keys[@intFromEnum(CoverageOutcome.produced)]));
    try std.testing.expectEqual(@as(?u64, 1), try loadDerivedCoverageOutcomeCounter(&runtime, transition.counter_keys[@intFromEnum(CoverageOutcome.skipped)]));
    try std.testing.expectEqual(@as(u64, 1), runtime.skipped_source_count);

    transition.outcome = .produced;
    try applyCoverageOutcomeTransitionsForIndex(&runtime, &.{ transition, transition });
    try std.testing.expectEqual(@as(?u64, 1), try loadDerivedCoverageOutcomeCounter(&runtime, transition.counter_keys[@intFromEnum(CoverageOutcome.produced)]));
    try std.testing.expectEqual(@as(?u64, 0), try loadDerivedCoverageOutcomeCounter(&runtime, transition.counter_keys[@intFromEnum(CoverageOutcome.skipped)]));
    try std.testing.expectEqual(@as(u64, 0), runtime.skipped_source_count);

    transition.outcome = .terminal_failed;
    try applyCoverageOutcomeTransitionsForIndex(&runtime, &.{transition});
    try std.testing.expectEqual(@as(?u64, 0), try loadDerivedCoverageOutcomeCounter(&runtime, transition.counter_keys[@intFromEnum(CoverageOutcome.produced)]));
    try std.testing.expectEqual(@as(?u64, 1), try loadDerivedCoverageOutcomeCounter(&runtime, transition.counter_keys[@intFromEnum(CoverageOutcome.terminal_failed)]));
}

test "enrichment applied checkpoint stays degraded until runtime status clears" {
    const alloc = std.testing.allocator;

    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer store.deinit();

    var erased_store = try backend_erased.storeFrom(alloc, store);
    defer erased_store.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const index_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/indexes", .{tmp.sub_path});
    var index_manager = try index_manager_mod.IndexManager.init(alloc, index_path);
    defer index_manager.deinit();

    var runtime = EnrichmentRuntime{
        .alloc = alloc,
        .io_impl = null,
        .store = erased_store,
        .owns_store = false,
        .change_journal = undefined,
        .replay_source = undefined,
        .index_manager = &index_manager,
        .write_ctx = undefined,
        .write_fn = undefined,
        .notify_ctx = undefined,
        .notify_fn = undefined,
        .config = .{},
        .ownership = undefined,
        .retrying = true,
        .worker_failed = false,
    };

    try enrichment_state.saveProjectionCheckpoint(runtime.store, scope_name, .{
        .applied_sequence = 3,
        .status = .degraded,
        .generation = 2,
        .config_hash = 0,
    });

    try saveAppliedSequenceWithRetry(&runtime, scope_name, 5);
    const degraded_checkpoint = try enrichment_state.loadProjectionCheckpoint(alloc, runtime.store, scope_name);
    try std.testing.expectEqual(@as(u64, 5), degraded_checkpoint.applied_sequence);
    try std.testing.expectEqual(enrichment_state.ProjectionStatus.degraded, degraded_checkpoint.status);

    runtime.retrying = false;
    runtime.worker_failed = false;
    try saveRuntimeStatusWithRetry(&runtime, scope_name, runtimeStatusSnapshot(&runtime));
    const clean_checkpoint = try enrichment_state.loadProjectionCheckpoint(alloc, runtime.store, scope_name);
    try std.testing.expectEqual(@as(u64, 5), clean_checkpoint.applied_sequence);
    try std.testing.expectEqual(enrichment_state.ProjectionStatus.clean, clean_checkpoint.status);
}

fn appendGeneratedBatchWithRetry(
    runtime: *EnrichmentRuntime,
    batch: derived_types.DerivedBatch,
    artifact_delete_keys: []const []const u8,
) !u64 {
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        const sequence = runtime.write_fn(runtime.write_ctx, batch, artifact_delete_keys) catch |err| switch (err) {
            error.WriterLocked => {
                if (attempt >= writer_locked_retry_count) return err;
                backoffWriterLockRetry();
                continue;
            },
            else => return err,
        };
        return sequence;
    }
}

const KVPair = struct {
    key: []const u8,
    value: []const u8,
};

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn storeGetAlloc(runtime: *EnrichmentRuntime, key: []const u8) ![]u8 {
    var txn = try runtime.store.beginRead();
    defer txn.abort();
    const raw = try txn.get(key);
    return try runtime.alloc.dupe(u8, raw);
}

fn storePut(runtime: *EnrichmentRuntime, key: []const u8, value: []const u8) !void {
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    const write = KVPair{ .key = key, .value = value };
    try updateDenseArtifactTargetCountersTxn(runtime, &txn, &.{write}, &.{});
    try txn.put(key, value);
    if (try assetSourceIndexKeyForArtifactAlloc(runtime.alloc, key)) |marker_key| {
        defer runtime.alloc.free(marker_key);
        try txn.put(marker_key, key);
    }
    try txn.commit();
}

fn storePutBatch(runtime: *EnrichmentRuntime, writes: []const KVPair, deletes: []const []const u8) !void {
    var batch = try runtime.store.beginBatch();
    errdefer batch.abort();
    try updateDenseArtifactTargetCountersTxn(runtime, &batch, writes, deletes);
    var marker_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (marker_keys.items) |key| runtime.alloc.free(key);
        marker_keys.deinit(runtime.alloc);
    }
    for (writes) |write| {
        try batch.put(write.key, write.value);
        if (try assetSourceIndexKeyForArtifactAlloc(runtime.alloc, write.key)) |marker_key| {
            try marker_keys.append(runtime.alloc, marker_key);
            try batch.put(marker_key, write.key);
        }
    }
    for (deletes) |key| {
        batch.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        if (try assetSourceIndexKeyForArtifactAlloc(runtime.alloc, key)) |marker_key| {
            try marker_keys.append(runtime.alloc, marker_key);
            batch.delete(marker_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }
    try batch.commit();
}

fn remoteRenderConfig(
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
) template_remote.RenderConfig {
    var config: template_remote.RenderConfig = .{};
    if (comptime @hasField(template_remote.RenderConfig, "secret_store")) {
        config.secret_store = secret_store;
    }
    if (comptime @hasField(template_remote.RenderConfig, "remote_content")) {
        config.remote_content = remote_content;
    }
    return config;
}

/// Extract the source text for an enrichment request from a document.
/// If the request has a source_template, renders the full document through the
/// Handlebars template. Otherwise, extracts the single source_field from the JSON.
fn extractSourceText(
    alloc: Allocator,
    config: Config,
    raw_doc: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
) !?[]const u8 {
    if (request.source_template.len > 0) {
        // Render via Handlebars template
        const rendered = renderSourceTemplateText(alloc, config, raw_doc, request.source_template) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => return null,
        };
        if (rendered.len == 0) {
            alloc.free(rendered);
            return null;
        }
        return rendered;
    }

    // Fall back to single-field extraction
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_doc, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const source = parsed.value.object.get(request.source_field) orelse return null;
    if (source != .string) return null;
    return try alloc.dupe(u8, source.string);
}

fn renderSourceTemplateText(
    alloc: Allocator,
    config: Config,
    raw_doc: []const u8,
    source_template: []const u8,
) ![]const u8 {
    if (comptime @hasDecl(template_remote, "renderJsonToValidatedTextWithConfig")) {
        return try template_remote.renderJsonToValidatedTextWithConfig(
            alloc,
            source_template,
            raw_doc,
            remoteRenderConfig(config.secret_store, config.remote_content),
        );
    }
    return try template_remote.renderJsonToTextWithConfig(
        alloc,
        source_template,
        raw_doc,
        remoteRenderConfig(config.secret_store, config.remote_content),
    );
}

fn extractAssetSourceValue(
    alloc: Allocator,
    config: Config,
    raw_doc: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
) !?[]const u8 {
    if (request.source_template.len > 0) {
        const rendered = renderSourceTemplateText(alloc, config, raw_doc, request.source_template) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => return null,
        };
        if (rendered.len == 0) {
            alloc.free(rendered);
            return null;
        }
        try document_extraction_mod.validateInlineSourceSize(config.remote_content, rendered);
        return rendered;
    }

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_doc, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const source = parsed.value.object.get(request.source_field) orelse return null;
    return switch (source) {
        .null => null,
        .string => |value| blk: {
            try document_extraction_mod.validateInlineSourceSize(config.remote_content, value);
            break :blk try alloc.dupe(u8, value);
        },
        else => blk: {
            const rendered = try std.json.Stringify.valueAlloc(alloc, source, .{});
            errdefer alloc.free(rendered);
            try document_extraction_mod.validateInlineSourceSize(config.remote_content, rendered);
            break :blk rendered;
        },
    };
}

fn renderSourceParts(
    alloc: Allocator,
    config: Config,
    raw_doc: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
) !?[]template.ContentPart {
    if (request.source_template.len == 0) return null;
    const parts = if (comptime @hasDecl(template_remote, "renderJsonToPartsWithConfig"))
        template_remote.renderJsonToPartsWithConfig(alloc, request.source_template, raw_doc, remoteRenderConfig(config.secret_store, config.remote_content)) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => return null,
        }
    else
        template_remote.renderJsonToParts(alloc, request.source_template, raw_doc) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => return null,
        };
    if (parts.len == 0) {
        template.freeContentParts(alloc, parts);
        return null;
    }
    return parts;
}

fn renderSourcePartsJson(
    alloc: Allocator,
    config: Config,
    raw_doc: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
) !?[]u8 {
    const parts = try renderSourceParts(alloc, config, raw_doc, request) orelse return null;
    defer template.freeContentParts(alloc, parts);
    return try contentPartsJsonAlloc(alloc, parts);
}

fn contentPartsJsonAlloc(alloc: Allocator, parts: []const template.ContentPart) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    for (parts, 0..) |part, i| {
        if (i > 0) try out.append(alloc, ',');
        switch (part) {
            .text => |text| {
                try out.appendSlice(alloc, "{\"type\":\"text\",\"text\":");
                try appendJsonString(alloc, &out, text);
                try out.append(alloc, '}');
            },
            .media_url => |url| {
                try out.appendSlice(alloc, "{\"type\":\"media\",\"url\":");
                try appendJsonString(alloc, &out, url);
                try out.append(alloc, '}');
            },
            .binary => |binary| {
                const encoded_len = std.base64.standard.Encoder.calcSize(binary.data.len);
                const encoded = try alloc.alloc(u8, encoded_len);
                defer alloc.free(encoded);
                _ = std.base64.standard.Encoder.encode(encoded, binary.data);
                try out.appendSlice(alloc, "{\"type\":\"media\",\"mime_type\":");
                try appendJsonString(alloc, &out, binary.mime_type);
                try out.appendSlice(alloc, ",\"data\":");
                try appendJsonString(alloc, &out, encoded);
                try out.append(alloc, '}');
            },
        }
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn freeJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string => |s| alloc.free(s),
        .array => |*arr| {
            for (arr.items) |*item| freeJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                freeJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        },
        .number_string => |s| alloc.free(s),
        else => {},
    }
}

// ============================================================================
// Tests
// ============================================================================

test "document extraction generated OCR batches honor execution item cap" {
    const alloc = std.testing.allocator;

    const FakeProducer = struct {
        batch_count: usize = 0,
        batch_lengths: [4]usize = .{ 0, 0, 0, 0 },

        fn producer(self: *@This()) asset_producer_mod.Producer {
            return .{
                .ptr = self,
                .vtable = &.{
                    .produce = produce,
                    .produce_batch = produceBatch,
                },
            };
        }

        fn produce(_: *anyopaque, _: Allocator, _: asset_producer_mod.Request) ![]u8 {
            return error.TestUnexpectedResult;
        }

        fn produceBatch(ptr: *anyopaque, a: Allocator, requests: []const asset_producer_mod.Request) ![][]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_lengths[self.batch_count] = requests.len;
            self.batch_count += 1;
            const out = try a.alloc([]u8, requests.len);
            errdefer {
                for (out) |item| {
                    if (item.len > 0) a.free(item);
                }
                a.free(out);
            }
            for (out, 0..) |*item, idx| {
                item.* = try std.fmt.allocPrint(a, "ocr text {d}", .{idx});
            }
            return out;
        }
    };

    const TestUnit = struct {
        fn make(a: Allocator, id: []const u8) !document_extraction_mod.Unit {
            return .{
                .unit_id = try a.dupe(u8, id),
                .unit_type = try a.dupe(u8, "image"),
                .text = try a.dupe(u8, "ocr_pending"),
                .method = try a.dupe(u8, "ocr_pending"),
                .extraction_status = try a.dupe(u8, "pending_ocr"),
            };
        }
    };

    var fake = FakeProducer{};
    const producer = fake.producer();
    var runtime = EnrichmentRuntime{
        .alloc = alloc,
        .io_impl = null,
        .store = undefined,
        .owns_store = false,
        .change_journal = undefined,
        .replay_source = undefined,
        .index_manager = undefined,
        .write_ctx = undefined,
        .write_fn = undefined,
        .notify_ctx = undefined,
        .notify_fn = undefined,
        .config = .{ .asset_producer = producer },
        .ownership = undefined,
    };

    var units = [_]document_extraction_mod.Unit{
        try TestUnit.make(alloc, "unit:1"),
        try TestUnit.make(alloc, "unit:2"),
        try TestUnit.make(alloc, "unit:3"),
    };
    defer for (&units) |*unit| unit.deinit(alloc);

    try completeRuntimeDocumentExtractionGeneratedTextBatch(
        &runtime,
        producer,
        .{ .ocr_enabled = true },
        .{ .max_items = 2, .max_bytes = 1024 * 1024 },
        "data:application/pdf;base64,AA==",
        &.{},
        "ocr",
        "application/pdf",
        units[0..],
        .ocr,
    );

    try std.testing.expectEqual(@as(usize, 2), fake.batch_count);
    try std.testing.expectEqual(@as(usize, 2), fake.batch_lengths[0]);
    try std.testing.expectEqual(@as(usize, 1), fake.batch_lengths[1]);
    try std.testing.expectEqualStrings("ocr text 0", units[0].text);
    try std.testing.expectEqualStrings("ocr text 1", units[1].text);
    try std.testing.expectEqualStrings("ocr text 0", units[2].text);
}

test "document extraction missing OCR model is a terminal unit failure" {
    const alloc = std.testing.allocator;

    const MissingModelProducer = struct {
        fn producer(self: *@This()) asset_producer_mod.Producer {
            return .{ .ptr = self, .vtable = &.{ .produce = produce, .produce_batch = produceBatch } };
        }

        fn produce(_: *anyopaque, _: Allocator, _: asset_producer_mod.Request) ![]u8 {
            return error.ModelNotFound;
        }

        fn produceBatch(_: *anyopaque, _: Allocator, _: []const asset_producer_mod.Request) ![][]u8 {
            return error.ModelNotFound;
        }
    };

    var fake = MissingModelProducer{};
    const producer = fake.producer();
    var runtime = EnrichmentRuntime{
        .alloc = alloc,
        .io_impl = null,
        .store = undefined,
        .owns_store = false,
        .change_journal = undefined,
        .replay_source = undefined,
        .index_manager = undefined,
        .write_ctx = undefined,
        .write_fn = undefined,
        .notify_ctx = undefined,
        .notify_fn = undefined,
        .config = .{ .asset_producer = producer },
        .ownership = undefined,
    };
    var units = [_]document_extraction_mod.Unit{.{
        .unit_id = try alloc.dupe(u8, "unit:1"),
        .unit_type = try alloc.dupe(u8, "image"),
        .text = try alloc.dupe(u8, ""),
        .method = try alloc.dupe(u8, "ocr_pending"),
        .extraction_status = try alloc.dupe(u8, "pending_ocr"),
    }};
    defer units[0].deinit(alloc);

    try completeRuntimeDocumentExtractionGeneratedTextBatch(
        &runtime,
        producer,
        .{ .ocr_enabled = true },
        .{ .max_items = 8, .max_bytes = 1024 * 1024 },
        "https://example.test/image.png",
        &.{},
        "image",
        "image/png",
        units[0..],
        .ocr,
    );

    try std.testing.expectEqualStrings("failed_ocr", units[0].extraction_status.?);
    try std.testing.expect(!units[0].ocr_used);
    try std.testing.expect(std.mem.indexOf(u8, units[0].extraction_warning.?, "ModelNotFound") != null);
}

test "generic generated asset batch fallback isolates malformed batch envelope" {
    const alloc = std.testing.allocator;

    const FallbackProducer = struct {
        batch_count: usize = 0,
        single_count: usize = 0,

        fn producer(self: *@This()) asset_producer_mod.Producer {
            return .{
                .ptr = self,
                .vtable = &.{
                    .produce = produce,
                    .produce_batch = produceBatch,
                },
            };
        }

        fn produce(ptr: *anyopaque, a: Allocator, request: asset_producer_mod.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.single_count += 1;
            return try std.fmt.allocPrint(a, "ok:{s}", .{request.source_text});
        }

        fn produceBatch(ptr: *anyopaque, a: Allocator, _: []const asset_producer_mod.Request) ![][]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_count += 1;
            const malformed = try a.alloc([]u8, 1);
            errdefer a.free(malformed);
            malformed[0] = try a.dupe(u8, "orphaned-output");
            return malformed;
        }
    };

    const TestItem = struct {
        fn make(a: Allocator, doc_key: []const u8, source: []const u8, artifact_key: []const u8, state_key: []const u8) !AssetProducerBatchItem {
            return .{
                .request = .{
                    .kind = .asset,
                    .index_name = "asset_idx",
                    .artifact_name = "asset",
                    .doc_key = doc_key,
                    .source_field = "body",
                    .content_type = "text/plain",
                },
                .producer_type = .generator,
                .config_json = try a.dupe(u8, "{\"provider\":\"test\"}"),
                .raw_doc = try a.dupe(u8, "{}"),
                .source_text = try a.dupe(u8, source),
                .artifact_key = try a.dupe(u8, artifact_key),
                .state_key = try a.dupe(u8, state_key),
                .state_value = try a.dupe(u8, "{\"state\":\"done\"}"),
            };
        }
    };

    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer store.deinit();
    var erased_store = try backend_erased.storeFrom(alloc, store);
    defer erased_store.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const index_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/indexes", .{tmp.sub_path});
    var index_manager = try index_manager_mod.IndexManager.init(alloc, index_path);
    defer index_manager.deinit();

    var fake = FallbackProducer{};
    const producer = fake.producer();
    var runtime = EnrichmentRuntime{
        .alloc = alloc,
        .io_impl = null,
        .store = erased_store,
        .owns_store = false,
        .change_journal = undefined,
        .replay_source = undefined,
        .index_manager = &index_manager,
        .write_ctx = undefined,
        .write_fn = undefined,
        .notify_ctx = undefined,
        .notify_fn = undefined,
        .config = .{ .asset_producer = producer },
        .ownership = undefined,
    };

    var items = std.ArrayListUnmanaged(AssetProducerBatchItem).empty;
    defer {
        clearAssetProducerBatchItems(alloc, &items);
        items.deinit(alloc);
    }
    try items.append(alloc, try TestItem.make(alloc, "doc:1", "one", "artifact:one", "state:one"));
    try items.append(alloc, try TestItem.make(alloc, "doc:2", "two", "artifact:two", "state:two"));

    var window = GeneratedReplayWindow{ .alloc = alloc };
    defer window.deinit();

    try flushAssetProducerBatch(&runtime, &items, &window);

    try std.testing.expectEqual(@as(usize, 1), fake.batch_count);
    try std.testing.expectEqual(@as(usize, 2), fake.single_count);
    try std.testing.expectEqual(@as(usize, 2), window.changed_artifact_keys.items.len);

    const first = try storeGetAlloc(&runtime, "artifact:one");
    defer alloc.free(first);
    try std.testing.expectEqualStrings("ok:one", first);
    const second = try storeGetAlloc(&runtime, "artifact:two");
    defer alloc.free(second);
    try std.testing.expectEqualStrings("ok:two", second);
}

test "document extraction generated OCR batch fallback isolates permanent unit failure" {
    const alloc = std.testing.allocator;

    const FallbackProducer = struct {
        batch_count: usize = 0,
        single_count: usize = 0,

        fn producer(self: *@This()) asset_producer_mod.Producer {
            return .{
                .ptr = self,
                .vtable = &.{
                    .produce = produce,
                    .produce_batch = produceBatch,
                },
            };
        }

        fn produce(ptr: *anyopaque, a: Allocator, request: asset_producer_mod.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.single_count += 1;
            const parts = request.source_parts_json orelse "";
            if (std.mem.indexOf(u8, parts, "unit:2") != null) return error.BadUnitInput;
            if (std.mem.indexOf(u8, parts, "unit:1") != null) return try a.dupe(u8, "ok:unit:1");
            if (std.mem.indexOf(u8, parts, "unit:3") != null) return try a.dupe(u8, "ok:unit:3");
            return error.BadUnitInput;
        }

        fn produceBatch(ptr: *anyopaque, _: Allocator, _: []const asset_producer_mod.Request) ![][]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_count += 1;
            return error.BatchEnvelopeRejected;
        }
    };

    const TestUnit = struct {
        fn make(a: Allocator, id: []const u8) !document_extraction_mod.Unit {
            return .{
                .unit_id = try a.dupe(u8, id),
                .unit_type = try a.dupe(u8, "image"),
                .text = try a.dupe(u8, "ocr_pending"),
                .method = try a.dupe(u8, "ocr_pending"),
                .extraction_status = try a.dupe(u8, "pending_ocr"),
            };
        }
    };

    var fake = FallbackProducer{};
    const producer = fake.producer();
    var runtime = EnrichmentRuntime{
        .alloc = alloc,
        .io_impl = null,
        .store = undefined,
        .owns_store = false,
        .change_journal = undefined,
        .replay_source = undefined,
        .index_manager = undefined,
        .write_ctx = undefined,
        .write_fn = undefined,
        .notify_ctx = undefined,
        .notify_fn = undefined,
        .config = .{ .asset_producer = producer },
        .ownership = undefined,
    };

    var units = [_]document_extraction_mod.Unit{
        try TestUnit.make(alloc, "unit:1"),
        try TestUnit.make(alloc, "unit:2"),
        try TestUnit.make(alloc, "unit:3"),
    };
    defer for (&units) |*unit| unit.deinit(alloc);

    try completeRuntimeDocumentExtractionGeneratedTextBatch(
        &runtime,
        producer,
        .{ .ocr_enabled = true },
        .{ .max_items = 8, .max_bytes = 1024 * 1024 },
        "data:application/pdf;base64,AA==",
        &.{},
        "ocr",
        "application/pdf",
        units[0..],
        .ocr,
    );

    try std.testing.expectEqual(@as(usize, 1), fake.batch_count);
    try std.testing.expectEqual(@as(usize, 3), fake.single_count);
    try std.testing.expectEqualStrings("ok:unit:1", units[0].text);
    try std.testing.expectEqualStrings("completed", units[0].extraction_status.?);
    try std.testing.expectEqualStrings("", units[1].text);
    try std.testing.expectEqualStrings("failed_ocr", units[1].extraction_status.?);
    try std.testing.expect(units[1].extraction_warning != null);
    try std.testing.expect(std.mem.indexOf(u8, units[1].extraction_warning.?, "BadUnitInput") != null);
    try std.testing.expectEqualStrings("ok:unit:3", units[2].text);
    try std.testing.expectEqualStrings("completed", units[2].extraction_status.?);
}

test "document extraction generated OCR batch fallback isolates malformed batch response" {
    const alloc = std.testing.allocator;

    const FallbackProducer = struct {
        batch_count: usize = 0,
        single_count: usize = 0,

        fn producer(self: *@This()) asset_producer_mod.Producer {
            return .{
                .ptr = self,
                .vtable = &.{
                    .produce = produce,
                    .produce_batch = produceBatch,
                },
            };
        }

        fn produce(ptr: *anyopaque, a: Allocator, request: asset_producer_mod.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.single_count += 1;
            const parts = request.source_parts_json orelse "";
            if (std.mem.indexOf(u8, parts, "unit:1") != null) return try a.dupe(u8, "ok:unit:1");
            if (std.mem.indexOf(u8, parts, "unit:2") != null) return try a.dupe(u8, "ok:unit:2");
            return error.BadUnitInput;
        }

        fn produceBatch(ptr: *anyopaque, a: Allocator, _: []const asset_producer_mod.Request) ![][]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_count += 1;
            const malformed = try a.alloc([]u8, 1);
            errdefer a.free(malformed);
            malformed[0] = try a.dupe(u8, "orphaned-batch-output");
            return malformed;
        }
    };

    const TestUnit = struct {
        fn make(a: Allocator, id: []const u8) !document_extraction_mod.Unit {
            return .{
                .unit_id = try a.dupe(u8, id),
                .unit_type = try a.dupe(u8, "image"),
                .text = try a.dupe(u8, "ocr_pending"),
                .method = try a.dupe(u8, "ocr_pending"),
                .extraction_status = try a.dupe(u8, "pending_ocr"),
            };
        }
    };

    var fake = FallbackProducer{};
    const producer = fake.producer();
    var runtime = EnrichmentRuntime{
        .alloc = alloc,
        .io_impl = null,
        .store = undefined,
        .owns_store = false,
        .change_journal = undefined,
        .replay_source = undefined,
        .index_manager = undefined,
        .write_ctx = undefined,
        .write_fn = undefined,
        .notify_ctx = undefined,
        .notify_fn = undefined,
        .config = .{ .asset_producer = producer },
        .ownership = undefined,
    };

    var units = [_]document_extraction_mod.Unit{
        try TestUnit.make(alloc, "unit:1"),
        try TestUnit.make(alloc, "unit:2"),
    };
    defer for (&units) |*unit| unit.deinit(alloc);

    try completeRuntimeDocumentExtractionGeneratedTextBatch(
        &runtime,
        producer,
        .{ .ocr_enabled = true },
        .{ .max_items = 8, .max_bytes = 1024 * 1024 },
        "data:application/pdf;base64,AA==",
        &.{},
        "ocr",
        "application/pdf",
        units[0..],
        .ocr,
    );

    try std.testing.expectEqual(@as(usize, 1), fake.batch_count);
    try std.testing.expectEqual(@as(usize, 2), fake.single_count);
    try std.testing.expectEqualStrings("ok:unit:1", units[0].text);
    try std.testing.expectEqualStrings("completed", units[0].extraction_status.?);
    try std.testing.expectEqualStrings("ok:unit:2", units[1].text);
    try std.testing.expectEqualStrings("completed", units[1].extraction_status.?);
}

test "enrichment runtime document extraction state parses byte-array keys" {
    const alloc = std.testing.allocator;
    const state = "{\"kind\":\"document_extraction_state_v1\",\"fingerprint\":\"source\",\"unit_keys\":[[65,0,255]],\"unit_descriptors\":[{\"key\":[65,0,255],\"fingerprint\":\"fp\"}],\"chunk_keys\":[[66,1,254]]}";

    var parsed = try loadRuntimeDocumentExtractionPreviousStateFromJson(alloc, state);
    defer parsed.deinit(alloc);

    const expected_unit_key = [_]u8{ 65, 0, 255 };
    const expected_chunk_key = [_]u8{ 66, 1, 254 };
    try std.testing.expectEqual(@as(usize, 1), parsed.unit_keys.len);
    try std.testing.expectEqualSlices(u8, &expected_unit_key, parsed.unit_keys[0]);
    try std.testing.expectEqual(@as(usize, 1), parsed.unit_descriptors.len);
    try std.testing.expectEqualSlices(u8, &expected_unit_key, parsed.unit_descriptors[0].key);
    try std.testing.expectEqualStrings("fp", parsed.unit_descriptors[0].fingerprint);
    try std.testing.expectEqual(@as(usize, 1), parsed.chunk_keys.len);
    try std.testing.expectEqualSlices(u8, &expected_chunk_key, parsed.chunk_keys[0]);
}

test "enrichment runtime document extraction manifest uses v2 range and merge shape" {
    const alloc = std.testing.allocator;
    const unit_keys = [_][]const u8{
        "doc:a\x1fartifact\x1fdocument_units_v1\x1funit:a",
        "doc:a\x1fartifact\x1fdocument_units_v1\x1funit:b",
    };
    const chunk_keys = [_][]const u8{
        "doc:a\x1fartifact\x1fdocument_chunks_v1\x1funit:a\x1fchunk:000000",
    };
    const previous_unit_keys = [_][]const u8{
        "doc:a\x1fartifact\x1fdocument_units_v1\x1funit:a",
        "doc:a\x1fartifact\x1fdocument_units_v1\x1fstale",
    };
    const previous_chunk_keys = [_][]const u8{
        "doc:a\x1fartifact\x1fdocument_chunks_v1\x1fstale\x1fchunk:000000",
    };
    const units = [_]document_extraction_mod.Unit{
        .{
            .unit_id = @constCast("unit:a"),
            .unit_type = @constCast("document"),
            .text = @constCast("same"),
            .method = @constCast("text"),
        },
        .{
            .unit_id = @constCast("unit:b"),
            .unit_type = @constCast("document"),
            .text = @constCast("changed"),
            .method = @constCast("text"),
        },
    };
    const extraction = document_extraction_mod.Result{
        .content_type = @constCast("text/plain"),
        .route_type = @constCast("text"),
        .units = @constCast(units[0..]),
    };
    const desired_descriptors = [_]DocumentExtractionUnitDescriptor{
        .{ .key = unit_keys[0], .fingerprint = "same-fingerprint" },
        .{ .key = unit_keys[1], .fingerprint = "new-fingerprint" },
    };
    const previous_descriptors = [_]DocumentExtractionUnitDescriptor{
        .{ .key = previous_unit_keys[0], .fingerprint = "same-fingerprint" },
        .{ .key = previous_unit_keys[1], .fingerprint = "old-fingerprint" },
    };
    const previous_ranges = [_]types.DocumentArtifactChildRange{.{
        .range_id = @constCast("range:000000"),
        .range_kind = @constCast("unit"),
        .artifact_name = @constCast("document_units_v1"),
        .split_boundary = @constCast("unit"),
        .placement = @constCast("child"),
        .owner_group_id = 2001,
        .placement_generation = 17,
        .route_status = @constCast("remote_committed"),
        .split_eligible = true,
        .start_key = @constCast(previous_unit_keys[0]),
        .end_key_exclusive = @constCast(""),
        .last_key = @constCast(previous_unit_keys[1]),
        .child_count = previous_unit_keys.len,
        .text_bytes = 123,
    }};

    const state = try documentExtractionStateValueAlloc(alloc, "source-fingerprint", &unit_keys, &desired_descriptors, &chunk_keys);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_descriptors\"") != null);

    const manifest = try documentExtractionManifestPayloadAlloc(
        alloc,
        "doc:a",
        "document_units_v1",
        "data:text/plain,same",
        "source-fingerprint",
        extraction,
        &.{},
        &unit_keys,
        &desired_descriptors,
        &chunk_keys,
        &.{},
        &previous_unit_keys,
        &previous_descriptors,
        &previous_chunk_keys,
        &.{},
        5,
        4,
        5,
        "converged",
        null,
    );
    defer alloc.free(manifest);

    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"manifest_version\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"child_ranges\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"range_kind\":\"unit\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"range_kind\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"split_boundary\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"text_bytes\":11") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"range_policy\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_target_children\":256") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_target_text_bytes\":1048576") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"oversized_unit_policy\":\"single_unit_range\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"owner_group_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"placement_generation\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_status\":\"local_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"split_eligible\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"coverage_plan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"full_text_replay\":\"stored_artifact_required\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"watermark_required_before_suppression\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"merge_plan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"status\":\"converged\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"from_generation\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"to_generation\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"operation_granularity\":\"unit_fingerprint\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"op\":\"keep\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"op\":\"upsert\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"op\":\"delete\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"fingerprint_match\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"fingerprint_match\":false") != null);

    const in_progress = try documentExtractionManifestPayloadAlloc(
        alloc,
        "doc:a",
        "document_units_v1",
        "data:text/plain,same",
        "source-fingerprint",
        extraction,
        &.{},
        &unit_keys,
        &desired_descriptors,
        &chunk_keys,
        &.{},
        &previous_unit_keys,
        &previous_descriptors,
        &previous_chunk_keys,
        &.{},
        4,
        4,
        5,
        "in_progress",
        null,
    );
    defer alloc.free(in_progress);
    try std.testing.expect(std.mem.indexOf(u8, in_progress, "\"generation\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, in_progress, "\"from_generation\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, in_progress, "\"to_generation\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, in_progress, "\"status\":\"in_progress\"") != null);

    const failed_extraction = document_extraction_mod.Result{
        .content_type = @constCast("application/pdf"),
        .route_type = @constCast("error"),
        .units = @constCast(&.{}),
    };
    const failed = try documentExtractionManifestPayloadAlloc(
        alloc,
        "doc:a",
        "document_units_v1",
        "data:application/pdf;base64,bad",
        "source-fingerprint",
        failed_extraction,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &previous_unit_keys,
        &previous_descriptors,
        &previous_chunk_keys,
        &previous_ranges,
        6,
        5,
        6,
        "failed",
        .{ .code = "InvalidPdf", .message = "document extraction failed" },
    );
    defer alloc.free(failed);
    try std.testing.expect(std.mem.indexOf(u8, failed, "\"generation\":6") != null);
    try std.testing.expect(std.mem.indexOf(u8, failed, "\"last_error\":{\"code\":\"InvalidPdf\",\"message\":\"document extraction failed\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, failed, "\"unit_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, failed, "\"chunk_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, failed, "\"child_count\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, failed, "\"route_status\":\"remote_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, failed, "\"status\":\"failed\"") != null);
}

test "extractSourceText with template renders all document fields" {
    const alloc = std.testing.allocator;
    const doc = "{\"title\":\"Hello\",\"body\":\"World\"}";
    const request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "body",
        .source_template = "{{title}} {{body}}",
    };
    const result = try extractSourceText(alloc, .{}, doc, request) orelse return error.TestUnexpectedResult;
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Hello World", result);
}

test "extractSourceText without template extracts single field" {
    const alloc = std.testing.allocator;
    const doc = "{\"title\":\"Hello\",\"body\":\"World\"}";
    const request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "body",
    };
    const result = try extractSourceText(alloc, .{}, doc, request) orelse return error.TestUnexpectedResult;
    defer alloc.free(result);
    try std.testing.expectEqualStrings("World", result);
}

test "extractSourceText without template returns null for missing field" {
    const alloc = std.testing.allocator;
    const doc = "{\"title\":\"Hello\"}";
    const request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "body",
    };
    const result = try extractSourceText(alloc, .{}, doc, request);
    try std.testing.expect(result == null);
}

test "extractSourceText with template skips _embeddings field" {
    const alloc = std.testing.allocator;
    const doc = "{\"title\":\"Hello\",\"_embeddings\":[1,2,3]}";
    const request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "title",
        .source_template = "{{title}}{{_embeddings}}",
    };
    const result = try extractSourceText(alloc, .{}, doc, request) orelse return error.TestUnexpectedResult;
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Hello", result);
}

test "extractSourceText with template and invalid JSON returns null" {
    const alloc = std.testing.allocator;
    const request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "body",
        .source_template = "{{body}}",
    };
    const result = try extractSourceText(alloc, .{}, "not json", request);
    try std.testing.expect(result == null);
}

test "enrichment extractSourceText with template error directive fails instead of returning text" {
    const alloc = std.testing.allocator;
    const doc = "{\"body\":\"large image description\"}";
    const request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "body",
        .source_template = "<<<error:status=413 message=StreamTooLong>>> fallback text",
    };
    try std.testing.expectError(error.PermanentPromptFailure, extractSourceText(alloc, .{}, doc, request));
}

test "extractSourceText with template and scrubHtml helper" {
    const alloc = std.testing.allocator;
    const doc = "{\"body\":\"<p>Hello</p><script>evil()</script><p>World</p>\"}";
    const request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "body",
        .source_template = "{{scrubHtml body}}",
    };
    const result = try extractSourceText(alloc, .{}, doc, request) orelse return error.TestUnexpectedResult;
    defer alloc.free(result);
    try std.testing.expectEqualStrings("HelloWorld", result);
}
