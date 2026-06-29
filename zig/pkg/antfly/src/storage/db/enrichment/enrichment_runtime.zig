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
const docstore_mod = @import("../../docstore.zig");
const internal_keys = @import("../../internal_keys.zig");
const artifact_ids = @import("../artifact_ids.zig");
const doc_identity = @import("../doc_identity.zig");
const relational_row_codec = @import("../algebraic/relational_row_codec.zig");
const resource_manager_mod = @import("../../resource_manager.zig");
const change_journal_mod = @import("../derived/change_journal.zig");
const replay_source_mod = @import("../derived/replay_source.zig");
const replay_stream_mod = @import("../derived/replay_stream.zig");
const derived_types = @import("../derived/derived_types.zig");
const enrichment_types = @import("enrichment_types.zig");
const enrichment_artifact_codec = @import("artifact_codec.zig");
const enrichment_worker = @import("enrichment_worker.zig");
const enrichment_lease = @import("enrichment_lease.zig");
const enrichment_state = @import("enrichment_state.zig");
const embedder_mod = @import("embedder.zig");
const asset_producer_mod = @import("asset_producer.zig");
const document_extraction_mod = @import("document_extraction.zig");
const chunker_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("chunker_stub.zig")
else
    @import("chunker.zig");
const chunk_artifact_mod = @import("../../../chunking/chunk.zig");
const chunking_types_mod = @import("../../../chunking/types.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const ownership_mod = @import("../ownership.zig");
const types = @import("../types.zig");
const db_config = @import("../config.zig");
const platform_clock = @import("../../../platform/clock.zig");
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

const TestHelpers = if (builtin.is_test) @import("../test_support.zig") else struct {};

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
    relational_base_rows: bool = false,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
};

pub const RuntimeError = error{ EnrichmentWorkerFailed, EnrichmentRetryInProgress };

pub const DerivedRecordWriter = *const fn (ptr: *anyopaque, batch: derived_types.DerivedBatch) anyerror!u64;
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
const transient_embed_retry_max_attempts: u32 = 6;
const transient_embed_retry_base_sleep_ns: u64 = 250 * std.time.ns_per_ms;
const transient_embed_retry_max_sleep_ns: u64 = 5 * std.time.ns_per_s;
const transient_worker_retry_sleep_ns: u64 = 100 * std.time.ns_per_ms;

const GeneratedReplayWindow = struct {
    alloc: Allocator,
    documents: std.ArrayListUnmanaged(derived_types.DerivedDocument) = .empty,
    deleted_keys: std.ArrayListUnmanaged([]u8) = .empty,
    changed_artifact_keys: std.ArrayListUnmanaged([]u8) = .empty,
    dense_embeddings: std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite) = .empty,
    sparse_embeddings: std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite) = .empty,

    fn isEmpty(self: *const @This()) bool {
        return self.documents.items.len == 0 and
            self.deleted_keys.items.len == 0 and
            self.changed_artifact_keys.items.len == 0 and
            self.dense_embeddings.items.len == 0 and
            self.sparse_embeddings.items.len == 0;
    }

    fn itemCount(self: *const @This()) usize {
        return self.documents.items.len +
            self.deleted_keys.items.len +
            self.changed_artifact_keys.items.len +
            self.dense_embeddings.items.len +
            self.sparse_embeddings.items.len;
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

fn backoffWriterLockRetry() void {
    if (comptime builtin.os.tag == .freestanding) return;
    platform.time.yieldBriefly();
    platform.time.sleepNs(writer_locked_retry_sleep_ns);
}

fn sleepRetryBackoff(sleep_ns: u64) void {
    if (comptime builtin.os.tag == .freestanding) return;
    platform.time.yieldBriefly();
    platform.time.sleepNs(sleep_ns);
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
        => true,
        else => false,
    };
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
        .retrying = runtime.retrying,
        .worker_failed = runtime.worker_failed,
    };
}

fn restorePersistedRuntimeStatus(runtime: anytype, persisted_status: enrichment_state.RuntimeStatus) void {
    runtime.error_count = persisted_status.error_count;
    runtime.retryable_error_count = persisted_status.retryable_error_count;
    runtime.fatal_error_count = persisted_status.fatal_error_count;
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

fn freeChunkEmbeddingSources(alloc: Allocator, sources: []const ChunkEmbeddingSource) void {
    for (sources) |source| {
        alloc.free(source.key);
        alloc.free(source.text);
    }
    if (sources.len > 0) alloc.free(sources);
}

const PlainDenseBatchItem = struct {
    request: enrichment_types.GeneratedEnrichmentRequest,
    source_text: []const u8,
    source_hash: u64,
    artifact_key: []u8,
};

fn freePlainDenseBatchItems(alloc: Allocator, items: []PlainDenseBatchItem) void {
    for (items) |item| {
        alloc.free(@constCast(item.source_text));
        alloc.free(item.artifact_key);
    }
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
        std.mem.eql(u8, requestEmbeddingName(lhs), requestEmbeddingName(rhs));
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

    const doc_store_key = try documentSourceStoreKeyAlloc(runtime, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetDocumentAlloc(runtime, doc_store_key) catch |err| switch (err) {
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
    write_ctx: *anyopaque,
    write_fn: DerivedRecordWriter,
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
    relational_base_rows: bool = false,
    skip_by_hash_count: u64 = 0,
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
        write_ctx: *anyopaque,
        write_fn: DerivedRecordWriter,
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
            .write_ctx = write_ctx,
            .write_fn = write_fn,
            .notify_ctx = notify_ctx,
            .notify_fn = notify_fn,
            .relational_base_rows = config.relational_base_rows,
            .config = .{
                .owner_id = config.owner_id,
                .lease_ttl_ms = config.lease_ttl_ms,
                .dense_embedder = config.dense_embedder,
                .sparse_embedder = config.sparse_embedder,
                .asset_producer = config.asset_producer,
                .enable_without_producers = config.enable_without_producers,
                .relational_base_rows = config.relational_base_rows,
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

    pub fn setStatusHook(self: *@This(), hook: ?StatusHook) void {
        _ = self;
        _ = hook;
    }

    pub fn setRelationalBaseRows(self: *@This(), enabled: bool) void {
        self.relational_base_rows = enabled;
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
        if (self.config.dense_embedder == null and self.config.sparse_embedder == null and self.config.asset_producer == null and !self.config.enable_without_producers) return;

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
        var window = GeneratedReplayWindow{ .alloc = self.alloc };
        defer window.deinit();
        const max_window_items = generatedReplayWindowItems();
        var processed_request_count: u64 = 0;

        var max_seen = self.applied_sequence;
        for (pending) |group| {
            max_seen = @max(max_seen, group.sequence);
            try processPendingDocumentGroup(self, group, &chunk_cache, &request_plan_cache, &deferred_plain_dense, &deferred_chunked_dense, &window, &processed_request_count);
            if (window.itemCount() >= max_window_items) try flushGeneratedReplayWindow(self, &window);
        }
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
            .processed_requests = self.processed_requests,
            .error_count = self.error_count,
            .retryable_error_count = self.retryable_error_count,
            .fatal_error_count = self.fatal_error_count,
            .retrying = self.retrying,
            .worker_failed = self.worker_failed,
            .skip_by_hash_count = self.skip_by_hash_count,
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
    write_ctx: *anyopaque,
    write_fn: DerivedRecordWriter,
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
    relational_base_rows: std.atomic.Value(bool) = .init(false),
    skip_by_hash_count: u64 = 0,
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
        write_ctx: *anyopaque,
        write_fn: DerivedRecordWriter,
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
            .write_ctx = write_ctx,
            .write_fn = write_fn,
            .notify_ctx = notify_ctx,
            .notify_fn = notify_fn,
            .relational_base_rows = .init(config.relational_base_rows),
            .config = .{
                .owner_id = config.owner_id,
                .lease_ttl_ms = config.lease_ttl_ms,
                .dense_embedder = config.dense_embedder,
                .sparse_embedder = config.sparse_embedder,
                .asset_producer = config.asset_producer,
                .enable_without_producers = config.enable_without_producers,
                .relational_base_rows = config.relational_base_rows,
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

    pub fn setRelationalBaseRows(self: *EnrichmentRuntime, enabled: bool) void {
        self.relational_base_rows.store(enabled, .monotonic);
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
        return .{
            .enabled = self.config.dense_embedder != null or self.config.sparse_embedder != null or self.config.asset_producer != null or self.config.enable_without_producers,
            .lease_owned = ownership_stats.lease_owned,
            .has_lease = ownership_stats.has_lease,
            .acquisition_count = ownership_stats.acquisition_count,
            .lease_acquire_failures = ownership_stats.lease_acquire_failures,
            .lost_leases = ownership_stats.lost_leases,
            .last_acquired_ms = ownership_stats.last_acquired_ms,
            .target_sequence = self.target_sequence,
            .applied_sequence = self.applied_sequence,
            .processed_requests = self.processed_requests,
            .error_count = self.error_count,
            .retryable_error_count = self.retryable_error_count,
            .fatal_error_count = self.fatal_error_count,
            .retrying = self.retrying,
            .worker_failed = self.worker_failed,
            .skip_by_hash_count = self.skip_by_hash_count,
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

fn handleWorkerLoopError(runtime: *EnrichmentRuntime, io: Io, err: anyerror) void {
    if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return;
    if (isRetryableEnrichmentError(err)) {
        runtime.recordRetryableError(io, err);
        io.sleep(Io.Duration.fromMilliseconds(@intCast(transient_worker_retry_sleep_ns / std.time.ns_per_ms)), .awake) catch {};
        return;
    }
    runtime.recordError(io, err);
}

fn recordIsolatedRequestError(runtime: *EnrichmentRuntime, request: enrichment_types.GeneratedEnrichmentRequest, err: anyerror) void {
    std.log.warn("enrichment request failed index={s} artifact={s}: {s}", .{ request.index_name, requestEmbeddingName(request), @errorName(err) });
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

    recordIsolatedRequestError(&runtime, .{
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

        const now_ms = runtime.config.clock.nowRealtimeMs();
        runtime.mutex.lockUncancelable(io);
        const acquired = runtime.ownership.ensureLease(now_ms) catch |err| {
            runtime.ownership.noteAcquireFailure();
            runtime.mutex.unlock(io);
            runtime.recordError(io, err);
            continue :worker_loop;
        };
        runtime.mutex.unlock(io);
        if (!acquired) {
            io.sleep(Io.Duration.zero, .awake) catch {};
            continue;
        }

        const pending = enrichment_worker.collectPendingDocumentGroups(runtime.alloc, runtime.replay_source, runtime.applied_sequence) catch |err| {
            handleWorkerLoopError(runtime, io, err);
            continue :worker_loop;
        };
        defer enrichment_worker.freePendingDocumentGroups(runtime.alloc, pending);

        var processed_request_count: u64 = 0;
        var max_seen = runtime.applied_sequence;

        retry_pending: while (true) {
            var chunk_cache = std.ArrayListUnmanaged(WorkerChunkCacheEntry).empty;
            defer freeWorkerChunkCache(runtime.alloc, &chunk_cache);
            var request_plan_cache = std.ArrayListUnmanaged(RequestPlanCacheEntry).empty;
            defer freeRequestPlanCache(runtime.alloc, &request_plan_cache);
            var deferred_plain_dense = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest).empty;
            defer deferred_plain_dense.deinit(runtime.alloc);
            var deferred_chunked_dense = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest).empty;
            defer deferred_chunked_dense.deinit(runtime.alloc);
            var window = GeneratedReplayWindow{ .alloc = runtime.alloc };
            defer window.deinit();
            const max_window_items = generatedReplayWindowItems();

            processed_request_count = 0;
            max_seen = runtime.applied_sequence;

            for (pending) |group| {
                max_seen = @max(max_seen, group.sequence);
                processPendingDocumentGroup(runtime, group, &chunk_cache, &request_plan_cache, &deferred_plain_dense, &deferred_chunked_dense, &window, &processed_request_count) catch |err| {
                    handleWorkerLoopError(runtime, io, err);
                    if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return;
                    if (isRetryableEnrichmentError(err)) continue :retry_pending;
                    continue :worker_loop;
                };
                flushGeneratedReplayWindowIfNeeded(runtime, &window, max_window_items) catch |err| {
                    handleWorkerLoopError(runtime, io, err);
                    if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return;
                    if (isRetryableEnrichmentError(err)) continue :retry_pending;
                    continue :worker_loop;
                };
            }
            processPlainDenseWindow(runtime, deferred_plain_dense.items, &window) catch |err| {
                handleWorkerLoopError(runtime, io, err);
                if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return;
                if (isRetryableEnrichmentError(err)) continue :retry_pending;
                continue :worker_loop;
            };
            processChunkedDenseWindow(runtime, deferred_chunked_dense.items, &chunk_cache, &window) catch |err| {
                handleWorkerLoopError(runtime, io, err);
                if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return;
                if (isRetryableEnrichmentError(err)) continue :retry_pending;
                continue :worker_loop;
            };
            flushGeneratedReplayWindow(runtime, &window) catch |err| {
                handleWorkerLoopError(runtime, io, err);
                if (err == error.EnrichmentRetryAborted and runtimeShuttingDown(runtime)) return;
                if (isRetryableEnrichmentError(err)) continue :retry_pending;
                continue :worker_loop;
            };
            break :retry_pending;
        }
        if (pending.len == 0) {
            max_seen = target_sequence;
        }

        if (max_seen > runtime.applied_sequence) {
            saveAppliedSequenceWithRetry(runtime, scope_name, max_seen) catch |err| {
                handleWorkerLoopError(runtime, io, err);
                continue :worker_loop;
            };
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
            saveRuntimeStatusWithRetry(runtime, scope_name, status) catch |err| {
                handleWorkerLoopError(runtime, io, err);
                continue :worker_loop;
            };
            runtime.notifyStatusHook();
        } else if (pending.len == 0) {
            var status: enrichment_state.RuntimeStatus = .{};
            runtime.mutex.lockUncancelable(io);
            runtime.retrying = false;
            runtime.worker_failed = false;
            status = runtimeStatusSnapshot(runtime);
            runtime.cond.broadcast(io);
            runtime.mutex.unlock(io);
            saveRuntimeStatusWithRetry(runtime, scope_name, status) catch |err| {
                handleWorkerLoopError(runtime, io, err);
                continue :worker_loop;
            };
            runtime.notifyStatusHook();
        }
    }
}

fn processPendingDocumentGroup(
    runtime: *EnrichmentRuntime,
    pending: enrichment_worker.PendingDocumentGroup,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
    request_plan_cache: *std.ArrayListUnmanaged(RequestPlanCacheEntry),
    deferred_plain_dense: *std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest),
    deferred_chunked_dense: *std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest),
    window: *GeneratedReplayWindow,
    processed_request_count: *u64,
) !void {
    const planned = try getOrCreatePlannedRequests(runtime, pending.doc_key, request_plan_cache);
    for (planned) |request| {
        // Publish completed generated writes before the next external embedder call can enter retry backoff.
        if (!window.isEmpty()) try flushGeneratedReplayWindow(runtime, window);
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
            .asset => processAsset(runtime, request, window) catch |err| {
                if (isRetryableEnrichmentError(err)) return err;
                recordIsolatedRequestError(runtime, request, err);
                continue;
            },
            .chunk_text => processChunkText(runtime, request, chunk_cache, window) catch |err| {
                if (isRetryableEnrichmentError(err)) return err;
                recordIsolatedRequestError(runtime, request, err);
                continue;
            },
            .dense_embedding => processDenseEmbedding(runtime, request, chunk_cache, window) catch |err| {
                if (isRetryableEnrichmentError(err)) return err;
                recordIsolatedRequestError(runtime, request, err);
                continue;
            },
            .sparse_embedding => processSparseEmbedding(runtime, request, chunk_cache, window) catch |err| {
                if (isRetryableEnrichmentError(err)) return err;
                recordIsolatedRequestError(runtime, request, err);
                continue;
            },
        }
    }
}

fn processAsset(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    window: *GeneratedReplayWindow,
) !void {
    const doc_store_key = try documentSourceStoreKeyAlloc(runtime, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetDocumentAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return,
    };
    defer runtime.alloc.free(raw);

    var producer_cfg = try asset_producer_mod.parseProducerConfig(runtime.alloc, request.producer_json);
    defer producer_cfg.deinit(runtime.alloc);

    const artifact_name = requestArtifactName(request);
    const key = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, request.doc_key, "asset", artifact_name);
    defer runtime.alloc.free(key);

    const source_text = try extractAssetSourceValue(runtime.alloc, runtime.config, raw, request) orelse {
        const state_key = try assetStateKeyAlloc(runtime.alloc, request.doc_key, artifact_name);
        defer runtime.alloc.free(state_key);
        if (producer_cfg.type == .document_extraction) {
            try deleteDocumentExtractionForRuntime(runtime, key, state_key, window);
        } else {
            try storePutBatchWithRetry(runtime, &.{}, &.{ key, state_key });
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
        }
        try materializeGraphAssetDeleteForRuntime(runtime, request, window);
        return;
    };
    defer runtime.alloc.free(@constCast(source_text));
    if (source_text.len == 0) {
        const state_key = try assetStateKeyAlloc(runtime.alloc, request.doc_key, artifact_name);
        defer runtime.alloc.free(state_key);
        if (producer_cfg.type == .document_extraction) {
            try deleteDocumentExtractionForRuntime(runtime, key, state_key, window);
        } else {
            try storePutBatchWithRetry(runtime, &.{}, &.{ key, state_key });
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
        }
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
    defer if (source_parts_json) |value| runtime.alloc.free(value);

    if (producer_cfg.type == .copy) {
        if (try shouldSkipAssetArtifact(runtime, key, source_text)) {
            try materializeGraphAssetForRuntime(runtime, request, source_text, raw, window);
            return;
        }
        try storePutWithRetry(runtime, key, source_text);
        try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
        try materializeGraphAssetForRuntime(runtime, request, source_text, raw, window);
        recordArtifactBytes(runtime, .asset, source_text.len);
        return;
    }

    const state_key = try assetStateKeyAlloc(runtime.alloc, request.doc_key, artifact_name);
    defer runtime.alloc.free(state_key);
    const state_value = try assetStateValueAlloc(runtime.alloc, source_text, source_parts_json, request.producer_json);
    defer runtime.alloc.free(state_value);
    if (try shouldSkipAssetProducer(runtime, state_key, state_value)) {
        const existing = storeGetAlloc(runtime, key) catch |err| switch (err) {
            std.mem.Allocator.Error.OutOfMemory => return err,
            else => null,
        };
        if (existing) |value| {
            defer runtime.alloc.free(value);
            try materializeGraphAssetForRuntime(runtime, request, value, raw, window);
            return;
        }
    }

    const producer = runtime.config.asset_producer orelse return error.MissingAssetProducer;
    const produced = try producer.produce(runtime.alloc, .{
        .producer_type = producer_cfg.type,
        .config_json = producer_cfg.config_json,
        .source_text = source_text,
        .source_parts_json = source_parts_json,
        .content_type = request.content_type,
    });
    defer runtime.alloc.free(produced);

    const writes = [_]KVPair{
        .{ .key = key, .value = produced },
        .{ .key = state_key, .value = state_value },
    };
    try storePutBatch(runtime, &writes, &.{});
    try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
    try materializeGraphAssetForRuntime(runtime, request, produced, raw, window);
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
        .source_url = source_url,
        .doc_key = request.doc_key,
        .artifact_name = artifact_name,
        .desired_unit_keys = &desired_unit_keys,
        .desired_unit_fingerprints = &desired_unit_fingerprints,
        .desired_chunk_keys = &desired_chunk_keys,
        .unit_text_lengths = &unit_text_lengths,
        .resource_tracker = &resource_tracker,
        .generated_units = &generated_units,
    };
    defer collect_ctx.info.deinit(runtime.alloc);
    document_extraction_mod.extractDownloadedStreaming(runtime.alloc, downloaded_mut, source_url, config, collect_ctx.sink()) catch |err| {
        if (isRetryableEnrichmentError(err)) return err;
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

    var previous_unit_keys: []const []const u8 = &.{};
    defer freeOwnedConstKeySlice(runtime.alloc, previous_unit_keys);
    var previous_unit_descriptors: []DocumentExtractionUnitDescriptor = &.{};
    defer freeDocumentExtractionUnitDescriptors(runtime.alloc, previous_unit_descriptors);
    var previous_chunk_keys: []const []const u8 = &.{};
    defer freeOwnedConstKeySlice(runtime.alloc, previous_chunk_keys);
    if (existing_state) |state| {
        previous_unit_keys = try documentExtractionStateUnitKeysAlloc(runtime.alloc, state);
        previous_unit_descriptors = try documentExtractionStateUnitDescriptorsAlloc(runtime.alloc, state);
        previous_chunk_keys = try documentExtractionStateChunkKeysAlloc(runtime.alloc, state);
    }

    if (existing_state != null) {
        for (previous_unit_keys) |previous_key| {
            if (runtimeContainsConstKey(desired_unit_keys.items, previous_key)) continue;
            try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
            try appendUniqueDupeKey(runtime.alloc, &window.deleted_keys, previous_key);
        }
        for (previous_chunk_keys) |previous_key| {
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
        previous_unit_keys,
        previous_unit_descriptors,
        previous_chunk_keys,
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

    const text_indexes = try runtime.index_manager.textIndexesForChunk(runtime.alloc, artifact_name, false);
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
        if (isRetryableEnrichmentError(err)) return err;
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
        previous_unit_keys,
        previous_unit_descriptors,
        previous_chunk_keys,
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
    var previous_unit_keys: []const []const u8 = &.{};
    defer freeOwnedConstKeySlice(runtime.alloc, previous_unit_keys);
    var previous_unit_descriptors: []DocumentExtractionUnitDescriptor = &.{};
    defer freeDocumentExtractionUnitDescriptors(runtime.alloc, previous_unit_descriptors);
    var previous_chunk_keys: []const []const u8 = &.{};
    defer freeOwnedConstKeySlice(runtime.alloc, previous_chunk_keys);
    if (existing_state) |state| {
        previous_unit_keys = try documentExtractionStateUnitKeysAlloc(runtime.alloc, state);
        previous_unit_descriptors = try documentExtractionStateUnitDescriptorsAlloc(runtime.alloc, state);
        previous_chunk_keys = try documentExtractionStateChunkKeysAlloc(runtime.alloc, state);
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
        previous_unit_keys,
        previous_unit_descriptors,
        previous_chunk_keys,
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
    for (previous_unit_keys) |previous_key| {
        try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
        try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
        try appendUniqueDupeKey(runtime.alloc, &window.deleted_keys, previous_key);
    }
    for (previous_chunk_keys) |previous_key| {
        try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
        try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
        try appendUniqueDupeKey(runtime.alloc, &window.deleted_keys, previous_key);
    }

    try storePutBatchWithRetry(runtime, writes.items, deletes.items);
    recordArtifactBytes(runtime, .asset, manifest.len);
}

fn deleteDocumentExtractionForRuntime(
    runtime: *EnrichmentRuntime,
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

    const existing_state = storeGetAlloc(runtime, state_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => null,
    };
    defer if (existing_state) |value| runtime.alloc.free(value);
    if (existing_state) |state| {
        const previous_keys = try documentExtractionStateUnitKeysAlloc(runtime.alloc, state);
        defer freeOwnedConstKeySlice(runtime.alloc, previous_keys);
        for (previous_keys) |previous_key| {
            try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
        }
        const previous_chunk_keys = try documentExtractionStateChunkKeysAlloc(runtime.alloc, state);
        defer freeOwnedConstKeySlice(runtime.alloc, previous_chunk_keys);
        for (previous_chunk_keys) |previous_key| {
            try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, previous_key));
            try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, previous_key);
        }
    }

    try storePutBatchWithRetry(runtime, &.{}, deletes.items);
}

fn completeRuntimeDocumentExtractionGeneratedText(
    runtime: *EnrichmentRuntime,
    config: document_extraction_mod.Config,
    source_url: []const u8,
    source_content_type: []const u8,
    extraction: *document_extraction_mod.Result,
) !void {
    const producer = runtime.config.asset_producer orelse return;
    for (extraction.units) |*unit| {
        try completeRuntimeDocumentExtractionGeneratedTextUnit(runtime, producer, config, source_url, extraction.route_type, source_content_type, unit);
    }
}

fn completeRuntimeDocumentExtractionGeneratedTextUnit(
    runtime: *EnrichmentRuntime,
    producer: asset_producer_mod.Producer,
    config: document_extraction_mod.Config,
    source_url: []const u8,
    route_type: []const u8,
    source_content_type: []const u8,
    unit: *document_extraction_mod.Unit,
) !void {
    if (config.ocr_enabled and unit.extraction_status != null and std.mem.eql(u8, unit.extraction_status.?, "pending_ocr")) {
        const parts_json = try runtimeDocumentGeneratedTextPartsJsonAlloc(runtime.alloc, route_type, source_content_type, unit.*);
        defer runtime.alloc.free(parts_json);
        const produced = try producer.produce(runtime.alloc, .{
            .producer_type = .reader,
            .config_json = config.ocr_config_json,
            .source_text = source_url,
            .source_parts_json = parts_json,
            .content_type = "text/plain",
        });
        errdefer runtime.alloc.free(produced);
        try applyRuntimeGeneratedUnitText(runtime.alloc, unit, produced, "ocr_text", "completed", .ocr);
        return;
    }
    if (config.transcription_enabled and unit.extraction_status != null and std.mem.eql(u8, unit.extraction_status.?, "pending_transcription")) {
        const parts_json = try runtimeDocumentGeneratedTextPartsJsonAlloc(runtime.alloc, route_type, source_content_type, unit.*);
        defer runtime.alloc.free(parts_json);
        const produced = try producer.produce(runtime.alloc, .{
            .producer_type = .transcriber,
            .config_json = config.transcription_config_json,
            .source_text = source_url,
            .source_parts_json = parts_json,
            .content_type = "text/plain",
        });
        errdefer runtime.alloc.free(produced);
        try applyRuntimeGeneratedUnitText(runtime.alloc, unit, produced, "transcript_text", "completed", .transcript);
    }
}

const RuntimeGeneratedUnitTextKind = enum { ocr, transcript };

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
        return;
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

fn runtimeGeneratedTextNeeded(config: document_extraction_mod.Config, unit: document_extraction_mod.Unit) bool {
    const status = unit.extraction_status orelse return false;
    if (config.ocr_enabled and std.mem.eql(u8, status, "pending_ocr")) return true;
    if (config.transcription_enabled and std.mem.eql(u8, status, "pending_transcription")) return true;
    return false;
}

const RuntimeDocumentExtractionCollectContext = struct {
    runtime: *EnrichmentRuntime,
    config: document_extraction_mod.Config,
    source_url: []const u8,
    doc_key: []const u8,
    artifact_name: []const u8,
    info: RuntimeDocumentExtractionStreamInfo = .{},
    desired_unit_keys: *std.ArrayListUnmanaged([]const u8),
    desired_unit_fingerprints: *std.ArrayListUnmanaged([]const u8),
    desired_chunk_keys: *std.ArrayListUnmanaged([]const u8),
    unit_text_lengths: *std.ArrayListUnmanaged(usize),
    resource_tracker: *RuntimeDocumentExtractionResourceTracker,
    generated_units: *RuntimeGeneratedUnitCache,

    fn sink(self: *@This()) document_extraction_mod.UnitSink {
        return .{
            .ptr = self,
            .on_begin = onBegin,
            .on_unit = onUnit,
            .on_end = onEnd,
        };
    }

    fn onBegin(ptr: *anyopaque, info: document_extraction_mod.StreamInfo) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.info.set(self.runtime.alloc, info);
    }

    fn onUnit(ptr: *anyopaque, unit: *document_extraction_mod.Unit) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const needs_generated_text = runtimeGeneratedTextNeeded(self.config, unit.*);
        if (needs_generated_text) {
            const producer = self.runtime.config.asset_producer orelse return error.MissingAssetProducer;
            try completeRuntimeDocumentExtractionGeneratedTextUnit(self.runtime, producer, self.config, self.source_url, self.info.route_type, self.info.content_type, unit);
            try self.generated_units.putClone(self.runtime.alloc, unit.*);
        }
        const current_unit_bytes: usize = if (needs_generated_text) 0 else runtimeDocumentExtractionUnitOwnedBytes(unit.*);
        try self.resource_tracker.setBytes(addUsizeSaturating(
            addUsizeSaturating(self.resource_tracker.downloaded_bytes, self.generated_units.bytes),
            current_unit_bytes,
        ));
        try collectRuntimeDocumentExtractionDesiredKeysForUnit(self.runtime, self.doc_key, self.artifact_name, unit.*, self.desired_unit_keys, self.desired_unit_fingerprints, self.desired_chunk_keys);
        try self.unit_text_lengths.append(self.runtime.alloc, unit.text.len);
    }

    fn onEnd(_: *anyopaque) anyerror!void {}
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
        if (runtimeGeneratedTextNeeded(self.config, unit.*)) {
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

            if (mode == .publish_replay) {
                try appendRuntimeDocumentUnitChunkDenseEmbeddingWrites(runtime, doc_key, chunk_key, entry.name, entry.source_field, chunk, window);
                try appendRuntimeDocumentUnitChunkSparseEmbeddingWrites(runtime, chunk_key, entry.name, chunk, window);
            }

            _ = arena_state.reset(.retain_capacity);
        }
    }
}

fn appendRuntimeDocumentUnitChunkDenseEmbeddingWrites(
    runtime: *EnrichmentRuntime,
    doc_key: []const u8,
    chunk_key: []const u8,
    chunk_artifact_name: []const u8,
    source_field: []const u8,
    chunk: chunker_mod.Chunk,
    window: *GeneratedReplayWindow,
) !void {
    const chunk_text = chunk.text orelse return;
    const dense_embedder = runtime.config.dense_embedder orelse return;

    for (runtime.index_manager.enrichments.items) |entry| {
        if (entry.kind != .embedding) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, chunk_artifact_name)) continue;
        if (entry.expected_dims == 0) continue;

        const consumer_indexes = try runtime.index_manager.denseIndexesForEmbedding(runtime.alloc, entry.name, entry.expected_dims);
        defer {
            for (consumer_indexes) |name| runtime.alloc.free(name);
            runtime.alloc.free(consumer_indexes);
        }
        if (consumer_indexes.len == 0) continue;

        const source_hash = enrichment_artifact_codec.hashSource(chunk_text);
        const artifact_key = try embeddingArtifactKey(runtime, chunk_key, entry.name);
        defer runtime.alloc.free(artifact_key);
        if (try shouldSkipEmbeddingArtifact(runtime, artifact_key, source_hash)) {
            try appendCachedDenseEmbeddingToWindow(runtime, window, chunk_key, artifact_key, consumer_indexes);
            continue;
        }

        const vector = try embedDenseWithRetry(dense_embedder, runtime, entry.name, chunk_text, entry.expected_dims);
        defer runtime.alloc.free(vector);
        try writeEmbeddingArtifact(runtime, .{
            .base_key = chunk_key,
            .parent_doc_key = doc_key,
            .artifact_name = entry.name,
            .source_field = source_field,
            .source_key = chunk_key,
            .source_hash = source_hash,
            .vector = vector,
        });

        var embeddings = try singleDenseEmbeddingForConsumers(runtime, chunk_key, artifact_key, vector, consumer_indexes);
        defer {
            for (embeddings) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
            if (embeddings.len > 0) runtime.alloc.free(embeddings);
        }
        try appendOwnedDenseEmbeddingsToWindow(runtime, window, &embeddings);
    }
}

fn appendRuntimeDocumentUnitChunkSparseEmbeddingWrites(
    runtime: *EnrichmentRuntime,
    chunk_key: []const u8,
    chunk_artifact_name: []const u8,
    chunk: chunker_mod.Chunk,
    window: *GeneratedReplayWindow,
) !void {
    const chunk_text = chunk.text orelse return;
    const sparse_embedder = runtime.config.sparse_embedder orelse return;

    for (runtime.index_manager.enrichments.items) |entry| {
        if (entry.kind != .embedding) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, chunk_artifact_name)) continue;
        if (entry.expected_dims != 0) continue;

        const consumer_indexes = try runtime.index_manager.sparseIndexesForEmbedding(runtime.alloc, entry.name);
        defer {
            for (consumer_indexes) |name| runtime.alloc.free(name);
            runtime.alloc.free(consumer_indexes);
        }
        if (consumer_indexes.len == 0) continue;

        const source_hash = enrichment_artifact_codec.hashSource(chunk_text);
        const artifact_key = try embeddingArtifactKey(runtime, chunk_key, entry.name);
        defer runtime.alloc.free(artifact_key);
        if (try shouldSkipEmbeddingArtifact(runtime, artifact_key, source_hash)) {
            try appendCachedSparseEmbeddingToWindow(runtime, window, chunk_key, artifact_key, consumer_indexes);
            continue;
        }

        var sparse = try embedSparseWithRetry(sparse_embedder, runtime, entry.name, chunk_text);
        defer sparse.deinit(runtime.alloc);
        try writeSparseEmbeddingArtifact(runtime, chunk_key, entry.name, source_hash, sparse.indices, sparse.values);

        var embeddings = try singleSparseEmbeddingForConsumers(runtime, chunk_key, artifact_key, sparse.indices, sparse.values, consumer_indexes);
        defer {
            for (embeddings) |embedding| {
                runtime.alloc.free(@constCast(embedding.index_name));
                runtime.alloc.free(@constCast(embedding.doc_key));
                if (embedding.artifact_key) |key| runtime.alloc.free(@constCast(key));
                if (embedding.indices.len > 0) runtime.alloc.free(@constCast(embedding.indices));
                if (embedding.values.len > 0) runtime.alloc.free(@constCast(embedding.values));
            }
            if (embeddings.len > 0) runtime.alloc.free(embeddings);
        }
        try appendOwnedSparseEmbeddingsToWindow(runtime, window, &embeddings);
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
        std.mem.eql(u8, requestEmbeddingName(lhs), requestEmbeddingName(rhs));
}

fn collectPlainDenseBatchItem(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    consumer_indexes: []const []const u8,
    window: *GeneratedReplayWindow,
) !?PlainDenseBatchItem {
    const embedding_artifact_name = requestEmbeddingName(request);
    const doc_store_key = try documentSourceStoreKeyAlloc(runtime, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    // Relational tables read from the committed base-row keyspace. extractSourceText
    // reads one typed column for source_field and materializes only for templates.
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return null,
    };
    defer runtime.alloc.free(raw);

    const source_text = try extractSourceText(runtime.alloc, runtime.config, raw, request) orelse return null;
    errdefer runtime.alloc.free(@constCast(source_text));
    const source_hash = enrichment_artifact_codec.hashSource(source_text);

    const artifact_key = try embeddingArtifactKey(runtime, request.doc_key, embedding_artifact_name);
    errdefer runtime.alloc.free(artifact_key);
    if (try shouldSkipEmbeddingArtifact(runtime, artifact_key, source_hash)) {
        try appendCachedDenseEmbeddingToWindow(runtime, window, request.doc_key, artifact_key, consumer_indexes);
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
    const max_batch_items = generatedEmbedBatchItems();
    const max_batch_bytes = generatedEmbedBatchBytes();

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
            if (isRetryableEnrichmentError(err)) return err;
            for (items.items) |item| recordIsolatedRequestError(runtime, item.request, err);
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

        var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
        defer chunk_texts.deinit(runtime.alloc);
        var chunk_items = std.ArrayListUnmanaged(ChunkedDenseWindowItem).empty;
        defer {
            for (chunk_items.items) |item| runtime.alloc.free(item.chunk_key);
            chunk_items.deinit(runtime.alloc);
        }
        var cached_embeddings = std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite).empty;
        defer {
            for (cached_embeddings.items) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
            cached_embeddings.deinit(runtime.alloc);
        }
        var stale_vector_keys = std.ArrayListUnmanaged([]u8).empty;
        defer freeKeyList(runtime.alloc, stale_vector_keys.items);

        var j: usize = i;
        while (j < requests.len) : (j += 1) {
            if (processed[j] and j != i) continue;
            const request = requests[j];
            if (!sameChunkedDenseBatchKey(seed, request)) continue;
            processed[j] = true;

            const chunk_artifact_name = requestArtifactName(request);
            const desired_chunk_keys = try chunkKeysForDenseRequest(runtime, request, chunk_artifact_name, chunk_cache);
            defer freeKeyList(runtime.alloc, desired_chunk_keys);
            const request_stale_vector_keys = try deleteStaleChunkEmbeddingArtifacts(runtime, request.doc_key, chunk_artifact_name, embedding_artifact_name, desired_chunk_keys);
            defer runtime.alloc.free(request_stale_vector_keys);
            for (request_stale_vector_keys) |key| {
                try appendUniqueOwnedKey(runtime.alloc, &stale_vector_keys, key);
            }

            const sources = try chunkEmbeddingSourcesForRequest(runtime, request, chunk_artifact_name, chunk_cache);
            defer freeChunkEmbeddingSources(runtime.alloc, sources);
            for (sources) |source| {
                const source_hash = enrichment_artifact_codec.hashSource(source.text);
                const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, source.key, embedding_artifact_name);
                defer runtime.alloc.free(embedding_key);
                if (try shouldSkipEmbeddingArtifact(runtime, embedding_key, source_hash)) {
                    if (generatedArtifactAlreadyPublished(runtime, embedding_key)) {
                        continue;
                    }
                    try cached_embeddings.append(runtime.alloc, .{
                        .index_name = try runtime.alloc.dupe(u8, seed.index_name),
                        .parent_doc_key = try runtime.alloc.dupe(u8, request.doc_key),
                        .doc_key = try runtime.alloc.dupe(u8, source.key),
                        .artifact_key = try runtime.alloc.dupe(u8, embedding_key),
                        .vector = &.{},
                    });
                    continue;
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
            }
        }

        try mergeOwnedDeletedKeysIntoWindow(runtime, window, try stale_vector_keys.toOwnedSlice(runtime.alloc));
        if (cached_embeddings.items.len > 0) {
            var expanded_cached = try expandDenseEmbeddingsForConsumers(runtime, cached_embeddings.items, consumer_indexes);
            defer {
                for (expanded_cached) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
                if (expanded_cached.len > 0) runtime.alloc.free(expanded_cached);
            }
            try appendOwnedDenseEmbeddingsToWindow(runtime, window, &expanded_cached);
        }
        if (chunk_items.items.len == 0) continue;

        const batch_stats = textBatchByteStats(chunk_texts.items);
        noteEmbedBatchStarted(runtime, chunk_texts.items.len, batch_stats.total_bytes, batch_stats.max_bytes);
        const embed_started_ns = runtime.config.clock.nowRealtimeNs();
        const vectors = embedDenseBatchWithRetry(dense_embedder, runtime, embedding_artifact_name, chunk_texts.items, seed.expected_dims) catch |err| {
            noteEmbedBatchFinished(runtime, chunk_texts.items.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), false);
            if (isRetryableEnrichmentError(err)) return err;
            for (chunk_items.items) |item| recordIsolatedRequestError(runtime, item.request, err);
            continue;
        };
        noteEmbedBatchFinished(runtime, chunk_texts.items.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), true);
        errdefer embedder_mod.freeDenseEmbeddingBatch(runtime.alloc, vectors);
        if (vectors.len != chunk_items.items.len) return error.InvalidEmbeddingResponse;

        var embeddings = try runtime.alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, chunk_items.items.len);
        defer {
            for (embeddings) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
            if (embeddings.len > 0) runtime.alloc.free(embeddings);
        }

        for (chunk_items.items, vectors, 0..) |item, vector, idx| {
            try writeEmbeddingArtifact(runtime, .{
                .base_key = item.chunk_key,
                .parent_doc_key = item.parent_doc_key,
                .artifact_name = item.artifact_name,
                .source_field = item.source_field,
                .source_key = item.chunk_key,
                .source_hash = item.source_hash,
                .vector = vector,
            });
            const artifact_key = try embeddingArtifactKey(runtime, item.chunk_key, item.artifact_name);
            embeddings[idx] = .{
                .index_name = try runtime.alloc.dupe(u8, seed.index_name),
                .parent_doc_key = try runtime.alloc.dupe(u8, item.parent_doc_key),
                .doc_key = try runtime.alloc.dupe(u8, item.chunk_key),
                .artifact_key = artifact_key,
                .vector = vector,
            };
        }
        runtime.alloc.free(@constCast(vectors));

        var expanded = try expandDenseEmbeddingsForConsumers(runtime, embeddings, consumer_indexes);
        defer {
            for (expanded) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
            if (expanded.len > 0) runtime.alloc.free(expanded);
        }
        try appendOwnedDenseEmbeddingsToWindow(runtime, window, &expanded);
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

    const doc_store_key = try documentSourceStoreKeyAlloc(runtime, doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetDocumentAlloc(runtime, doc_store_key) catch |err| switch (err) {
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

    var batch = try window.toOwnedBatch();
    defer derived_types.deinitDerivedBatch(runtime.alloc, &batch);
    const sequence = try appendDerivedBatchWithRetry(runtime, batch);
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
) !void {
    if (generatedArtifactAlreadyPublished(runtime, artifact_key)) return;
    var embeddings = try singleDenseEmbeddingForConsumers(runtime, doc_key, artifact_key, &.{}, consumer_indexes);
    try appendOwnedDenseEmbeddingsToWindow(runtime, window, &embeddings);
}

fn appendCachedSparseEmbeddingToWindow(
    runtime: *EnrichmentRuntime,
    window: *GeneratedReplayWindow,
    doc_key: []const u8,
    artifact_key: []const u8,
    consumer_indexes: []const []const u8,
) !void {
    if (generatedArtifactAlreadyPublished(runtime, artifact_key)) return;
    var embeddings = try singleSparseEmbeddingForConsumers(runtime, doc_key, artifact_key, &.{}, &.{}, consumer_indexes);
    try appendOwnedSparseEmbeddingsToWindow(runtime, window, &embeddings);
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
        const chunk_embeddings = try buildChunkDenseEmbeddings(runtime, request, dense_embedder, chunk_artifact_name, chunk_cache);
        defer {
            for (chunk_embeddings) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
            runtime.alloc.free(chunk_embeddings);
        }

        const desired_chunk_keys = try chunkKeysForDenseRequest(runtime, request, chunk_artifact_name, chunk_cache);
        defer freeKeyList(runtime.alloc, desired_chunk_keys);
        const stale_vector_keys = try deleteStaleChunkEmbeddingArtifacts(runtime, request.doc_key, chunk_artifact_name, embedding_artifact_name, desired_chunk_keys);
        if (chunk_embeddings.len == 0) {
            try mergeOwnedDeletedKeysIntoWindow(runtime, window, stale_vector_keys);
            return;
        }

        try writeChunkEmbeddingArtifacts(runtime, request.doc_key, request.source_field, embedding_artifact_name, chunk_embeddings);
        var expanded = try expandDenseEmbeddingsForConsumers(runtime, chunk_embeddings, consumer_indexes);
        defer {
            for (expanded) |embedding| freeDerivedDenseEmbedding(runtime.alloc, embedding);
            if (expanded.len > 0) runtime.alloc.free(expanded);
        }
        try mergeOwnedDeletedKeysIntoWindow(runtime, window, stale_vector_keys);
        try appendOwnedDenseEmbeddingsToWindow(runtime, window, &expanded);
        return;
    }

    const doc_store_key = try documentSourceStoreKeyAlloc(runtime, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetDocumentAlloc(runtime, doc_store_key) catch |err| switch (err) {
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
    }

    const source_text = try extractSourceText(runtime.alloc, runtime.config, raw, request) orelse return;
    defer runtime.alloc.free(source_text);
    const source_hash = enrichment_artifact_codec.hashSource(source_text);

    const artifact_key = try embeddingArtifactKey(runtime, request.doc_key, embedding_artifact_name);
    defer runtime.alloc.free(artifact_key);
    if (try shouldSkipEmbeddingArtifact(runtime, artifact_key, source_hash)) {
        try appendCachedDenseEmbeddingToWindow(runtime, window, request.doc_key, artifact_key, consumer_indexes);
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

    if ((request.chunk_size > 0 or request.chunker_json.len > 0) and requestArtifactName(request).len > 0) {
        const chunk_embeddings = try buildChunkSparseEmbeddings(runtime, request, sparse_embedder, requestArtifactName(request), chunk_cache);
        defer {
            for (chunk_embeddings) |embedding| {
                runtime.alloc.free(@constCast(embedding.index_name));
                runtime.alloc.free(@constCast(embedding.doc_key));
                if (embedding.artifact_key) |key| runtime.alloc.free(@constCast(key));
                runtime.alloc.free(@constCast(embedding.indices));
                runtime.alloc.free(@constCast(embedding.values));
            }
            runtime.alloc.free(chunk_embeddings);
        }

        const desired_chunk_keys = try chunkKeysForDenseRequest(runtime, request, requestArtifactName(request), chunk_cache);
        defer freeKeyList(runtime.alloc, desired_chunk_keys);
        const stale_vector_keys = try deleteStaleChunkEmbeddingArtifacts(runtime, request.doc_key, requestArtifactName(request), embedding_artifact_name, desired_chunk_keys);
        if (chunk_embeddings.len == 0) {
            try mergeOwnedDeletedKeysIntoWindow(runtime, window, stale_vector_keys);
            return;
        }

        var expanded = try expandSparseEmbeddingsForConsumers(runtime, chunk_embeddings, consumer_indexes);
        defer {
            for (expanded) |embedding| {
                runtime.alloc.free(@constCast(embedding.index_name));
                runtime.alloc.free(@constCast(embedding.doc_key));
                if (embedding.artifact_key) |key| runtime.alloc.free(@constCast(key));
                runtime.alloc.free(@constCast(embedding.indices));
                runtime.alloc.free(@constCast(embedding.values));
            }
            if (expanded.len > 0) runtime.alloc.free(expanded);
        }
        try mergeOwnedDeletedKeysIntoWindow(runtime, window, stale_vector_keys);
        try appendOwnedSparseEmbeddingsToWindow(runtime, window, &expanded);
        return;
    }

    const doc_store_key = try documentSourceStoreKeyAlloc(runtime, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    // Relational tables read from the committed base-row keyspace. extractSourceText
    // reads one typed column for source_field and materializes only for templates.
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return,
    };
    defer runtime.alloc.free(raw);

    const source_text = try extractSourceText(runtime.alloc, runtime.config, raw, request) orelse return;
    defer runtime.alloc.free(source_text);
    const source_hash = enrichment_artifact_codec.hashSource(source_text);

    const artifact_key = try embeddingArtifactKey(runtime, request.doc_key, embedding_artifact_name);
    defer runtime.alloc.free(artifact_key);
    if (try shouldSkipEmbeddingArtifact(runtime, artifact_key, source_hash)) {
        try appendCachedSparseEmbeddingToWindow(runtime, window, request.doc_key, artifact_key, consumer_indexes);
        return;
    }

    var sparse = try embedSparseWithRetry(sparse_embedder, runtime, embedding_artifact_name, source_text);
    defer sparse.deinit(runtime.alloc);
    try writeSparseEmbeddingArtifact(runtime, request.doc_key, embedding_artifact_name, source_hash, sparse.indices, sparse.values);

    var embeddings = try singleSparseEmbeddingForConsumers(runtime, request.doc_key, artifact_key, sparse.indices, sparse.values, consumer_indexes);
    defer {
        for (embeddings) |embedding| {
            runtime.alloc.free(@constCast(embedding.index_name));
            runtime.alloc.free(@constCast(embedding.doc_key));
            if (embedding.artifact_key) |key| runtime.alloc.free(@constCast(key));
            runtime.alloc.free(@constCast(embedding.indices));
            runtime.alloc.free(@constCast(embedding.values));
        }
        if (embeddings.len > 0) runtime.alloc.free(embeddings);
    }
    try appendOwnedSparseEmbeddingsToWindow(runtime, window, &embeddings);
}

fn buildChunkDenseEmbeddings(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    dense_embedder: embedder_mod.DenseEmbedder,
    artifact_name: []const u8,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
) ![]derived_types.DerivedDenseEmbeddingWrite {
    const sources = try chunkEmbeddingSourcesForRequest(runtime, request, artifact_name, chunk_cache);
    defer freeChunkEmbeddingSources(runtime.alloc, sources);
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
        for (chunk_keys.items) |chunk_key| runtime.alloc.free(chunk_key);
        chunk_keys.deinit(runtime.alloc);
    }

    for (sources) |source| {
        const chunk_key = try runtime.alloc.dupe(u8, source.key);
        const source_hash = enrichment_artifact_codec.hashSource(source.text);
        const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, source.key, requestEmbeddingName(request));
        defer runtime.alloc.free(embedding_key);
        if (try shouldSkipEmbeddingArtifact(runtime, embedding_key, source_hash)) {
            if (generatedArtifactAlreadyPublished(runtime, embedding_key)) {
                runtime.alloc.free(chunk_key);
                continue;
            }
            try embeddings.append(runtime.alloc, .{
                .index_name = try runtime.alloc.dupe(u8, request.index_name),
                .parent_doc_key = try runtime.alloc.dupe(u8, request.doc_key),
                .doc_key = chunk_key,
                .artifact_key = try runtime.alloc.dupe(u8, embedding_key),
                .vector = &.{},
            });
            continue;
        }
        try chunk_texts.append(runtime.alloc, source.text);
        try chunk_keys.append(runtime.alloc, chunk_key);
    }

    if (chunk_texts.items.len == 0) return try embeddings.toOwnedSlice(runtime.alloc);

    const batch_stats = textBatchByteStats(chunk_texts.items);
    noteEmbedBatchStarted(runtime, chunk_texts.items.len, batch_stats.total_bytes, batch_stats.max_bytes);
    const embed_started_ns = runtime.config.clock.nowRealtimeNs();
    const vectors = embedDenseBatchWithRetry(dense_embedder, runtime, requestEmbeddingName(request), chunk_texts.items, request.expected_dims) catch |err| {
        noteEmbedBatchFinished(runtime, chunk_texts.items.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), false);
        return err;
    };
    noteEmbedBatchFinished(runtime, chunk_texts.items.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), true);
    errdefer embedder_mod.freeDenseEmbeddingBatch(runtime.alloc, vectors);
    if (vectors.len != chunk_keys.items.len) return error.InvalidEmbeddingResponse;

    for (chunk_keys.items, vectors) |chunk_key, vector| {
        try embeddings.append(runtime.alloc, .{
            .index_name = try runtime.alloc.dupe(u8, request.index_name),
            .parent_doc_key = try runtime.alloc.dupe(u8, request.doc_key),
            .doc_key = chunk_key,
            .vector = vector,
        });
    }

    runtime.alloc.free(@constCast(vectors));
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

fn buildChunkSparseEmbeddings(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    sparse_embedder: embedder_mod.SparseEmbedder,
    artifact_name: []const u8,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
) ![]derived_types.DerivedSparseEmbeddingWrite {
    const sources = try chunkEmbeddingSourcesForRequest(runtime, request, artifact_name, chunk_cache);
    defer freeChunkEmbeddingSources(runtime.alloc, sources);
    if (sources.len == 0) return try runtime.alloc.alloc(derived_types.DerivedSparseEmbeddingWrite, 0);

    var embeddings = std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite).empty;
    errdefer {
        for (embeddings.items) |embedding| {
            runtime.alloc.free(@constCast(embedding.index_name));
            runtime.alloc.free(@constCast(embedding.doc_key));
            if (embedding.artifact_key) |key| runtime.alloc.free(@constCast(key));
            if (embedding.indices.len > 0) runtime.alloc.free(@constCast(embedding.indices));
            if (embedding.values.len > 0) runtime.alloc.free(@constCast(embedding.values));
        }
        embeddings.deinit(runtime.alloc);
    }
    var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
    defer chunk_texts.deinit(runtime.alloc);
    var chunk_keys = std.ArrayListUnmanaged([]u8).empty;
    var chunk_hashes = std.ArrayListUnmanaged(u64).empty;
    defer chunk_hashes.deinit(runtime.alloc);
    errdefer {
        for (chunk_keys.items) |chunk_key| runtime.alloc.free(chunk_key);
        chunk_keys.deinit(runtime.alloc);
    }

    for (sources) |source| {
        const chunk_key = try runtime.alloc.dupe(u8, source.key);
        const source_hash = enrichment_artifact_codec.hashSource(source.text);
        const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, source.key, requestEmbeddingName(request));
        defer runtime.alloc.free(embedding_key);
        if (try shouldSkipEmbeddingArtifact(runtime, embedding_key, source_hash)) {
            if (generatedArtifactAlreadyPublished(runtime, embedding_key)) {
                runtime.alloc.free(chunk_key);
                continue;
            }
            try embeddings.append(runtime.alloc, .{
                .index_name = try runtime.alloc.dupe(u8, request.index_name),
                .doc_key = chunk_key,
                .artifact_key = try runtime.alloc.dupe(u8, embedding_key),
                .indices = &.{},
                .values = &.{},
            });
            continue;
        }
        try chunk_texts.append(runtime.alloc, source.text);
        try chunk_keys.append(runtime.alloc, chunk_key);
        try chunk_hashes.append(runtime.alloc, source_hash);
    }

    if (chunk_texts.items.len == 0) return try embeddings.toOwnedSlice(runtime.alloc);

    const batch_stats = textBatchByteStats(chunk_texts.items);
    noteEmbedBatchStarted(runtime, chunk_texts.items.len, batch_stats.total_bytes, batch_stats.max_bytes);
    const embed_started_ns = runtime.config.clock.nowRealtimeNs();
    const sparse_batch = embedSparseBatchWithRetry(sparse_embedder, runtime, requestEmbeddingName(request), chunk_texts.items) catch |err| {
        noteEmbedBatchFinished(runtime, chunk_texts.items.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), false);
        return err;
    };
    noteEmbedBatchFinished(runtime, chunk_texts.items.len, batch_stats.total_bytes, batch_stats.max_bytes, elapsedNsSince(runtime, embed_started_ns), true);
    errdefer embedder_mod.freeSparseEmbeddingBatch(runtime.alloc, sparse_batch);
    if (sparse_batch.len != chunk_keys.items.len) return error.InvalidEmbeddingResponse;

    for (chunk_keys.items, chunk_hashes.items, sparse_batch) |chunk_key, source_hash, sparse| {
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
        for (out[0..initialized]) |embedding| {
            runtime.alloc.free(@constCast(embedding.index_name));
            runtime.alloc.free(@constCast(embedding.doc_key));
            if (embedding.artifact_key) |key| runtime.alloc.free(@constCast(key));
            if (embedding.indices.len > 0) runtime.alloc.free(@constCast(embedding.indices));
            if (embedding.values.len > 0) runtime.alloc.free(@constCast(embedding.values));
        }
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
        for (out.items) |embedding| {
            runtime.alloc.free(@constCast(embedding.index_name));
            runtime.alloc.free(@constCast(embedding.doc_key));
            if (embedding.artifact_key) |key| runtime.alloc.free(@constCast(key));
            if (embedding.indices.len > 0) runtime.alloc.free(@constCast(embedding.indices));
            if (embedding.values.len > 0) runtime.alloc.free(@constCast(embedding.values));
        }
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
    const sequence = try appendDerivedBatchWithRetry(runtime, cloned);
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
        if (key_value != .string or fingerprint_value != .string) return error.InvalidDocumentExtractionState;
        out[i] = .{
            .key = try alloc.dupe(u8, key_value.string),
            .fingerprint = try alloc.dupe(u8, fingerprint_value.string),
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
        if (item != .string) return error.InvalidDocumentExtractionState;
        out[i] = .{
            .key = try alloc.dupe(u8, item.string),
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
        if (item != .string) return error.InvalidDocumentExtractionState;
        out[i] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return out;
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
) ![][]u8 {
    const prefix = try internal_keys.artifactNamedPrefixAlloc(runtime.alloc, doc_key, "chunk", chunk_artifact_name);
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

    _ = embedding_artifact_name;
    for (existing) |entry| {
        if (!internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) continue;
        if (derivedEmbeddingBelongsToDesiredChunk(entry.key, desired_chunk_keys)) continue;
        if (try internal_keys.derivedEmbeddingBaseKeyAlloc(runtime.alloc, entry.key)) |base_key| {
            try appendUniqueOwnedKey(runtime.alloc, &stale_vector_keys, base_key);
        }
        try deletes.append(runtime.alloc, try runtime.alloc.dupe(u8, entry.key));
    }
    if (deletes.items.len > 0) try storePutBatchWithRetry(runtime, &.{}, deletes.items);
    return try stale_vector_keys.toOwnedSlice(runtime.alloc);
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

fn chunkEmbeddingSourcesForRequest(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_name: []const u8,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
) ![]ChunkEmbeddingSource {
    const chunks = try getOrCreateRequestChunks(runtime, request, chunk_cache);
    var sources = std.ArrayListUnmanaged(ChunkEmbeddingSource).empty;
    errdefer {
        for (sources.items) |source| {
            runtime.alloc.free(source.key);
            runtime.alloc.free(source.text);
        }
        sources.deinit(runtime.alloc);
    }
    for (chunks) |chunk| {
        const source = chunk.text orelse continue;
        if (source.len == 0) continue;
        try sources.append(runtime.alloc, .{
            .key = try internal_keys.chunkArtifactKeyAlloc(runtime.alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id)),
            .text = try runtime.alloc.dupe(u8, source),
        });
    }
    if (sources.items.len > 0) return try sources.toOwnedSlice(runtime.alloc);

    const stored = try storedChunkEmbeddingSourcesForRequest(runtime, request, artifact_name);
    if (stored.len > 0) return stored;
    runtime.alloc.free(stored);
    return try sources.toOwnedSlice(runtime.alloc);
}

fn chunkKeysForDenseRequest(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_name: []const u8,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
) ![][]u8 {
    const chunks = try getOrCreateRequestChunks(runtime, request, chunk_cache);
    if (chunks.len > 0) return try chunkKeysForChunks(runtime.alloc, request.doc_key, artifact_name, chunks);

    const stored = try storedChunkEmbeddingSourcesForRequest(runtime, request, artifact_name);
    defer freeChunkEmbeddingSources(runtime.alloc, stored);
    if (stored.len > 0) {
        const keys = try runtime.alloc.alloc([]u8, stored.len);
        var initialized: usize = 0;
        errdefer {
            for (keys[0..initialized]) |key| runtime.alloc.free(key);
            runtime.alloc.free(keys);
        }
        for (stored, 0..) |source, i| {
            keys[i] = try runtime.alloc.dupe(u8, source.key);
            initialized += 1;
        }
        return keys;
    }

    return try chunkKeysForChunks(runtime.alloc, request.doc_key, artifact_name, chunks);
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

fn keyInList(key: []const u8, keys: []const []const u8) bool {
    for (keys) |candidate| {
        if (std.mem.eql(u8, key, candidate)) return true;
    }
    return false;
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

fn saveAppliedSequenceWithRetry(runtime: *EnrichmentRuntime, scope: []const u8, sequence: u64) !void {
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        enrichment_state.saveAppliedSequence(runtime.store, scope, sequence) catch |err| switch (err) {
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
        return;
    }
}

fn appendDerivedBatchWithRetry(runtime: *EnrichmentRuntime, batch: derived_types.DerivedBatch) !u64 {
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        const sequence = runtime.write_fn(runtime.write_ctx, batch) catch |err| switch (err) {
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

fn runtimeUsesRelationalBaseRows(runtime: *EnrichmentRuntime) bool {
    return if (comptime builtin.os.tag == .freestanding)
        runtime.relational_base_rows
    else
        runtime.relational_base_rows.load(.monotonic);
}

/// Read a DOCUMENT store value and materialize it as JSON. Relational tables
/// read from the relational row keyspace and require a typed row there; there
/// is no supported JSON blob fallback for this new storage mode.
fn storeGetDocumentAlloc(runtime: *EnrichmentRuntime, key: []const u8) ![]u8 {
    const raw = try storeGetAlloc(runtime, key);
    if (runtimeUsesRelationalBaseRows(runtime)) {
        defer runtime.alloc.free(raw);
        return try relational_row_codec.reconstructValueAlloc(runtime.alloc, raw);
    }
    return try relational_row_codec.materializeOwnedDocumentValueAlloc(runtime.alloc, raw);
}

fn documentSourceStoreKeyAlloc(runtime: *EnrichmentRuntime, doc_key: []const u8) ![]u8 {
    const relational_base_rows = runtimeUsesRelationalBaseRows(runtime);
    return if (relational_base_rows)
        try internal_keys.relationalRowKeyAlloc(runtime.alloc, doc_key)
    else
        try internal_keys.documentKeyAlloc(runtime.alloc, doc_key);
}

fn storePut(runtime: *EnrichmentRuntime, key: []const u8, value: []const u8) !void {
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
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
/// Handlebars template. Otherwise, extracts the single source_field.
///
/// `raw_doc` is the value as stored: a JSON blob (document mode) or a serialized
/// relational typed row. The single-field path takes Seam B — it reads just the
/// requested column straight from the typed row via `findCellByPath`, skipping
/// the full reconstruct-then-reparse. The template path needs the whole
/// document, so the row is materialized to JSON first (Seam A). A document-mode
/// blob takes the JSON paths directly.
fn extractSourceText(
    alloc: Allocator,
    config: Config,
    raw_doc: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
) !?[]const u8 {
    if (request.source_template.len > 0) {
        // Template needs the whole document: materialize a typed row to JSON.
        const doc_json = try relational_row_codec.materializeDocumentValueAlloc(alloc, raw_doc);
        defer alloc.free(doc_json);
        const rendered = renderSourceTemplateText(alloc, config, doc_json, request.source_template) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => return null,
        };
        if (rendered.len == 0) {
            alloc.free(rendered);
            return null;
        }
        return rendered;
    }

    // Single-field path. Seam B: read just the column from a typed row.
    if (try relational_row_codec.findCellByPath(raw_doc, request.source_field)) |cell| {
        if (cell.value_type != .bytes_val or cell.is_json) return null;
        return try alloc.dupe(u8, cell.value.bytes_val);
    }
    if (try extractRelationalJsonSourceTextAlloc(alloc, raw_doc, request.source_field)) |text| return text;
    if (relational_row_codec.looksLikeRow(raw_doc)) return null; // row without that column

    // Document-mode blob: extract the single source_field from the JSON.
    return try extractJsonSourceTextAlloc(alloc, raw_doc, request.source_field);
}

fn extractRelationalJsonSourceTextAlloc(
    alloc: Allocator,
    raw_doc: []const u8,
    source_field: []const u8,
) !?[]const u8 {
    const dot = std.mem.indexOfScalar(u8, source_field, '.') orelse return null;
    const root = source_field[0..dot];
    const json_path = source_field[dot + 1 ..];
    if (root.len == 0 or json_path.len == 0) return null;
    const cell = (try relational_row_codec.findCellByPath(raw_doc, root)) orelse return null;
    if (cell.value_type != .bytes_val or !cell.is_json) return null;
    return try extractJsonSourceTextAlloc(alloc, cell.value.bytes_val, json_path);
}

fn extractJsonSourceTextAlloc(
    alloc: Allocator,
    json: []const u8,
    source_field: []const u8,
) !?[]const u8 {
    if (source_field.len == 0) return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    const source = jsonValueAtDottedPath(parsed.value, source_field) orelse return null;
    if (source != .string) return null;
    return try alloc.dupe(u8, source.string);
}

fn jsonValueAtDottedPath(value: std.json.Value, path: []const u8) ?std.json.Value {
    if (value != .object) return null;
    var current = value;
    var it = std.mem.splitScalar(u8, path, '.');
    while (it.next()) |segment| {
        if (segment.len == 0 or current != .object) return null;
        current = current.object.get(segment) orelse return null;
    }
    return current;
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
    const source = jsonValueAtDottedPath(parsed.value, request.source_field) orelse return null;
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
    // Template rendering needs the whole document; materialize a typed row.
    const doc_json = try relational_row_codec.materializeDocumentValueAlloc(alloc, raw_doc);
    defer alloc.free(doc_json);
    const parts = if (comptime @hasDecl(template_remote, "renderJsonToPartsWithConfig"))
        template_remote.renderJsonToPartsWithConfig(alloc, request.source_template, doc_json, remoteRenderConfig(config.secret_store, config.remote_content)) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => return null,
        }
    else
        template_remote.renderJsonToParts(alloc, request.source_template, doc_json) catch |err| switch (err) {
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

test "db enrichment runtime document extraction unit payload preserves pdf page provenance" {
    const alloc = std.testing.allocator;
    var text_regions = [_]document_extraction_mod.TextRegion{.{
        .span = .{ 0, 5 },
        .bbox = .{ 72, 700, 120, 712 },
    }};
    const unit = document_extraction_mod.Unit{
        .unit_id = @constCast("page:000001"),
        .unit_type = @constCast("page"),
        .text = @constCast("hello"),
        .method = @constCast("pdf_text"),
        .page_number = 1,
        .page_label = @constCast("i"),
        .page_bbox = .{ 0, 0, 612, 792 },
        .page_rotation = 90,
        .text_regions = text_regions[0..],
        .char_start = 0,
        .char_end = 5,
    };

    const payload = try documentUnitPayloadAlloc(alloc, "doc:a", "document_units_v1", unit, "data:application/pdf;base64,AA==", "application/pdf", .{ .range_id = "range:000000" });
    defer alloc.free(payload);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer parsed.deinit();
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectEqualStrings("range:000000", parsed.value.object.get("_artifact_range_id").?.string);
    try std.testing.expectEqualStrings("unit", parsed.value.object.get("_artifact_range_kind").?.string);
    try std.testing.expectEqualStrings("local_committed", parsed.value.object.get("_artifact_route_status").?.string);
    try std.testing.expectEqual(@as(i64, 0), parsed.value.object.get("_artifact_owner_group_id").?.integer);
    try std.testing.expectEqual(@as(i64, 1), provenance.get("page_number").?.integer);
    try std.testing.expectEqualStrings("i", provenance.get("page_label").?.string);
    const page_bbox = provenance.get("page_bbox").?.array.items;
    try std.testing.expectEqual(@as(usize, 4), page_bbox.len);
    try std.testing.expectEqual(@as(i64, 612), page_bbox[2].integer);
    try std.testing.expectEqual(@as(i64, 90), provenance.get("page_rotation").?.integer);
    const format_provenance = provenance.get("format_provenance").?.object;
    try std.testing.expectEqualStrings("antfly.document_format_provenance.v1", format_provenance.get("schema").?.string);
    try std.testing.expectEqualStrings("application/pdf", format_provenance.get("source_content_type").?.string);
    try std.testing.expectEqualStrings("source_page_points", format_provenance.get("coordinate_system").?.string);
    try std.testing.expectEqualStrings("pdf_text", format_provenance.get("extraction_method").?.string);
    try std.testing.expect(!format_provenance.get("ocr_used").?.bool);
    const regions = format_provenance.get("text_regions").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), regions.len);
    const region = regions[0].object;
    try std.testing.expectEqual(@as(i64, 0), region.get("span").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 5), region.get("span").?.array.items[1].integer);
    try std.testing.expectEqual(@as(i64, 72), region.get("bbox").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 712), region.get("bbox").?.array.items[3].integer);
}

test "db enrichment runtime document extraction unit payload marks scanned pdf pages as pending OCR" {
    const alloc = std.testing.allocator;
    const unit = document_extraction_mod.Unit{
        .unit_id = @constCast("page:000002"),
        .unit_type = @constCast("page"),
        .text = @constCast(""),
        .method = @constCast("pdf_ocr_pending"),
        .extraction_status = @constCast("pending_ocr"),
        .ocr_used = false,
        .page_number = 2,
        .page_label = @constCast("2"),
        .page_bbox = .{ 0, 0, 612, 792 },
        .char_start = 5,
        .char_end = 5,
    };

    const payload = try documentUnitPayloadAlloc(alloc, "doc:a", "document_units_v1", unit, "data:application/pdf;base64,AA==", "application/pdf", .{ .range_id = "range:000000" });
    defer alloc.free(payload);
    const fingerprint = try documentExtractionUnitFingerprintAlloc(alloc, unit);
    defer alloc.free(fingerprint);
    try std.testing.expectEqual(@as(usize, 64), fingerprint.len);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer parsed.deinit();
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectEqualStrings("pending_ocr", parsed.value.object.get("extraction_status").?.string);
    try std.testing.expectEqualStrings("pdf_ocr_pending", provenance.get("method").?.string);
    try std.testing.expectEqualStrings("pending_ocr", provenance.get("extraction_status").?.string);
    try std.testing.expect(!provenance.get("ocr_used").?.bool);
    try std.testing.expectEqual(@as(i64, 2), provenance.get("page_number").?.integer);
    try std.testing.expectEqual(@as(i64, 5), provenance.get("char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 5), provenance.get("char_end").?.integer);
    const format_provenance = provenance.get("format_provenance").?.object;
    try std.testing.expectEqualStrings("pdf_ocr_pending", format_provenance.get("extraction_method").?.string);
    try std.testing.expectEqualStrings("pending_ocr", format_provenance.get("extraction_status").?.string);
    try std.testing.expect(!format_provenance.get("ocr_used").?.bool);
}

test "db enrichment runtime document extraction asset materializes unit artifacts from data url" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var fake = @import("../test_support.zig").TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 0), fake.calls);

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const state_key = try assetStateKeyAlloc(alloc, "doc:a", "document_units_v1");
    defer alloc.free(state_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"artifact_type\":\"document_units\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);

    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"_parent_doc_key\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"_artifact_name\":\"document_units_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"unit_id\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"alpha beta\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"text\"") != null);

    const state = try db.core.store.get(alloc, state_key);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"kind\":\"document_extraction_state_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_descriptors\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "document:000001") != null);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, manifest_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unit_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, state_key));
}

test "db enrichment runtime document extraction async accounts resource manager working set" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var db = try DB.open(alloc, std.mem.span(path), .{
        .resource_manager = &resource_manager,
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"status\":\"converged\"") != null);

    const stats = resource_manager.snapshot().slices[@intFromEnum(resource_manager_mod.Slice.document_extraction_working_set)];
    try std.testing.expect(stats.peak_bytes > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
}

test "db enrichment runtime document extraction routes mixed files using source metadata fields" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json =
        \\{"type":"document_extraction","config":{"source":{"filename_field":"filename"},"routes":[{"match":{"extension":["md"]},"extractor":{"type":"text","unit":"note"}}]}}
        ,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:application/octet-stream;base64,YWxwaGEgYmV0YQ==\",\"filename\":\"notes.md\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "note:000001");
    defer alloc.free(unit_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"text\"") != null);

    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"unit_id\":\"note:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"unit_type\":\"note\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"alpha beta\"") != null);
}

test "db enrichment runtime document extraction stores docx section units" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:docx",
            .value = "{\"filename\":\"report.docx\",\"mime_type\":\"application/vnd.openxmlformats-officedocument.wordprocessingml.document\",\"url\":\"data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,UEsDBBQAAAAAAAAAAABUVz0vhwAAAIcAAAARAAAAd29yZC9kb2N1bWVudC54bWw8dzpkb2N1bWVudCB4bWxuczp3PSJ3Ij48dzpib2R5Pjx3OnA+PHc6cj48dzp0PkFscGhhIERCPC93OnQ+PC93OnI+PC93OnA+PHc6cD48dzpyPjx3OnQ+QmV0YSBEQjwvdzp0PjwvdzpyPjwvdzpwPjwvdzpib2R5Pjwvdzpkb2N1bWVudD5QSwECFAAUAAAAAAAAAAAAVFc9L4cAAACHAAAAEQAAAAAAAAAAAAAAAAAAAAAAd29yZC9kb2N1bWVudC54bWxQSwUGAAAAAAEAAQA/AAAAtgAAAAAA\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:docx", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const section_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:docx", "document_units_v1", "section:000001");
    defer alloc.free(section_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"docx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);

    const section_payload = try db.core.store.get(alloc, section_key);
    defer alloc.free(section_payload);
    try std.testing.expect(std.mem.indexOf(u8, section_payload, "\"unit_type\":\"section\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, section_payload, "\"method\":\"docx_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, section_payload, "\"text\":\"Alpha DB\\nBeta DB\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, section_payload, "\"source_content_type\":\"application/vnd.openxmlformats-officedocument.wordprocessingml.document\"") != null);
}

test "db enrichment runtime document extraction stores zip archive entry units" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:archive",
            .value = "{\"filename\":\"bundle.zip\",\"mime_type\":\"application/zip\",\"url\":\"data:application/zip;base64,UEsDBBQAAAAAAAAAAADxLiMkDwAAAA8AAAAPAAAAZG9jcy9yZWFkbWUudHh0QXJjaGl2ZSBEQiB0ZXh0UEsBAhQAFAAAAAAAAAAAAPEuIyQPAAAADwAAAA8AAAAAAAAAAAAAAAAAAAAAAGRvY3MvcmVhZG1lLnR4dFBLBQYAAAAAAQABAD0AAAA8AAAAAAA=\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:archive", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const entry_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:archive", "document_units_v1", "archive:entry:000001");
    defer alloc.free(entry_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"archive\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);

    const entry_payload = try db.core.store.get(alloc, entry_key);
    defer alloc.free(entry_payload);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"unit_type\":\"archive_entry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"method\":\"zip_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"source_path\":\"docs/readme.txt\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"text\":\"Archive DB text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"source_content_type\":\"application/zip\"") != null);
}

test "db enrichment runtime document extraction stores image pending OCR unit" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:image", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"image\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);

    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"unit_type\":\"image\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"ocr_pending\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"pending_ocr\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"byte_length\":19") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"source_sha256\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_used\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"transcript_used\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"source_content_type\":\"image/png\"") != null);
}

test "db enrichment runtime document extraction completes image OCR with reader producer" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var fake = @import("../test_support.zig").TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"enabled\":true,\"config\":{\"provider\":\"mock-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image-ocr", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"ocr_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"completed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_used\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"reader:data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"") != null);
}

test "db enrichment runtime document extraction async reuses generated OCR text across streaming passes" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var fake = @import("../test_support.zig").TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"enabled\":true,\"config\":{\"provider\":\"mock-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr-async",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image-ocr-async", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"ocr_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"completed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"reader:data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"") != null);
}

test "db enrichment runtime document extraction stores structured OCR confidence and coordinates" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var fake = @import("../test_support.zig").TestAssetProducer{
        .reader_output = "{\"text\":\"invoice total\",\"confidence\":0.92,\"bbox\":[1,2,101,42],\"warning\":\"low contrast\"}",
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"enabled\":true,\"config\":{\"provider\":\"mock-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr-structured",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image-ocr-structured", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("invoice total", parsed.value.object.get("text").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), parsed.value.object.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), parsed.value.object.get("ocr_confidence").?.float, 0.0001);
    const bbox = parsed.value.object.get("ocr_bbox").?.array.items;
    try std.testing.expectEqual(@as(usize, 4), bbox.len);
    const bbox_x0: f64 = switch (bbox[0]) {
        .float => |value| value,
        .integer => |value| @floatFromInt(value),
        else => return error.TestUnexpectedResult,
    };
    const bbox_y1: f64 = switch (bbox[3]) {
        .float => |value| value,
        .integer => |value| @floatFromInt(value),
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectApproxEqAbs(@as(f64, 1), bbox_x0, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 42), bbox_y1, 0.0001);
    try std.testing.expectEqualStrings("low contrast", parsed.value.object.get("extraction_warning").?.string);
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), provenance.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), provenance.get("ocr_confidence").?.float, 0.0001);
    try std.testing.expectEqualStrings("low contrast", provenance.get("extraction_warning").?.string);
}

test "db enrichment runtime document extraction completes audio transcription with transcriber producer" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var fake = @import("../test_support.zig").TestAssetProducer{
        .transcriber_output = "{\"text\":\"spoken words\",\"confidence\":0.81}",
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"transcription\":{\"enabled\":true,\"config\":{\"provider\":\"mock-transcriber\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:audio-transcript",
            .value = "{\"filename\":\"audio.mp3\",\"mime_type\":\"audio/mpeg\",\"url\":\"data:audio/mpeg;base64,SUQzYXVkaW8gYnl0ZXM=\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:audio-transcript", "document_units_v1", "audio:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"transcript_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"completed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"transcript_used\":true") != null);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("spoken words", parsed.value.object.get("text").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.81), parsed.value.object.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.81), parsed.value.object.get("transcript_confidence").?.float, 0.0001);
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectApproxEqAbs(@as(f64, 0.81), provenance.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.81), provenance.get("transcript_confidence").?.float, 0.0001);
}

test "db enrichment runtime document extraction stores rfc822 email units" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:email",
            .value = "{\"filename\":\"message.eml\",\"mime_type\":\"message/rfc822\",\"url\":\"data:message/rfc822;base64,U3ViamVjdDogQWxwaGENCkZyb206IGFAZXhhbXBsZS50ZXN0DQoNCkhlbGxvIGVtYWls\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:email", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const headers_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:email", "document_units_v1", "email:headers");
    defer alloc.free(headers_key);
    const body_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:email", "document_units_v1", "email:body");
    defer alloc.free(body_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"email\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":2") != null);

    const headers_payload = try db.core.store.get(alloc, headers_key);
    defer alloc.free(headers_payload);
    try std.testing.expect(std.mem.indexOf(u8, headers_payload, "\"unit_type\":\"email_headers\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, headers_payload, "Subject: Alpha") != null);
    try std.testing.expect(std.mem.indexOf(u8, headers_payload, "\"method\":\"email_rfc822\"") != null);

    const body_payload = try db.core.store.get(alloc, body_key);
    defer alloc.free(body_payload);
    try std.testing.expect(std.mem.indexOf(u8, body_payload, "\"unit_type\":\"email_body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body_payload, "\"text\":\"Hello email\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body_payload, "\"source_content_type\":\"message/rfc822\"") != null);
}

test "db enrichment runtime document extraction stores multipart rfc822 text parts" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:multipart-email",
            .value = "{\"filename\":\"message.eml\",\"mime_type\":\"message/rfc822\",\"url\":\"data:message/rfc822;base64,U3ViamVjdDogQWxwaGENCkNvbnRlbnQtVHlwZTogbXVsdGlwYXJ0L2FsdGVybmF0aXZlOyBib3VuZGFyeT0iYjEiDQoNCi0tYjENCkNvbnRlbnQtVHlwZTogdGV4dC9wbGFpbg0KDQpQbGFpbiBib2R5DQotLWIxDQpDb250ZW50LVR5cGU6IHRleHQvaHRtbA0KDQo8cD5IVE1MIGJvZHk8L3A+DQotLWIxLS0NCg==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:multipart-email", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const plain_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:multipart-email", "document_units_v1", "email:part:000001");
    defer alloc.free(plain_key);
    const html_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:multipart-email", "document_units_v1", "email:part:000002");
    defer alloc.free(html_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"email\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":3") != null);

    const plain_payload = try db.core.store.get(alloc, plain_key);
    defer alloc.free(plain_payload);
    try std.testing.expect(std.mem.indexOf(u8, plain_payload, "\"unit_type\":\"email_part\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, plain_payload, "\"method\":\"email_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, plain_payload, "Plain body") != null);

    const html_payload = try db.core.store.get(alloc, html_key);
    defer alloc.free(html_payload);
    try std.testing.expect(std.mem.indexOf(u8, html_payload, "\"method\":\"email_html\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, html_payload, "\"text\":\"HTML body\"") != null);
}

test "db enrichment runtime document extraction stores unsupported file manifest without searchable units" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addIndex(.{
        .name = "ft_document_units",
        .kind = .full_text,
        .config_json = "{\"artifact_name\":\"document_units_v1\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:bin",
            .value = "{\"url\":\"data:application/octet-stream;base64,AAEC\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:bin", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const state_key = try assetStateKeyAlloc(alloc, "doc:bin", "document_units_v1");
    defer alloc.free(state_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"unsupported\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unsupported_reason\":\"unsupported_content_type\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"chunk_count\":0") != null);

    const state = try db.core.store.get(alloc, state_key);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_keys\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"chunk_keys\":[]") != null);

    var result = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match_all = {} },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 0), result.total_hits);
}

test "db enrichment runtime document extraction manifest classifies unit fingerprint keeps" {
    const alloc = std.testing.allocator;
    const unit_keys = [_][]const u8{ "unit:a", "unit:b" };
    const chunk_keys = [_][]const u8{};
    const previous_unit_keys = [_][]const u8{ "unit:a", "unit:b" };
    const previous_chunk_keys = [_][]const u8{};
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
        .{ .key = "unit:a", .fingerprint = "same-fingerprint" },
        .{ .key = "unit:b", .fingerprint = "new-fingerprint" },
    };
    const previous_descriptors = [_]DocumentExtractionUnitDescriptor{
        .{ .key = "unit:a", .fingerprint = "same-fingerprint" },
        .{ .key = "unit:b", .fingerprint = "old-fingerprint" },
    };
    const unit_text_lengths = [_]usize{ units[0].text.len, units[1].text.len };

    const manifest = try documentExtractionManifestPayloadAlloc(
        alloc,
        "doc:a",
        "document_units_v1",
        "data:text/plain,same",
        "source-fingerprint",
        extraction,
        &unit_text_lengths,
        &unit_keys,
        &desired_descriptors,
        &chunk_keys,
        &.{},
        &previous_unit_keys,
        &previous_descriptors,
        &previous_chunk_keys,
        &.{},
        2,
        1,
        2,
        "converged",
        null,
    );
    defer alloc.free(manifest);

    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"operation_granularity\":\"unit_fingerprint\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"status\":\"converged\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"owner_group_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"placement_generation\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_status\":\"local_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"split_eligible\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"coverage_plan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"full_text_replay\":\"stored_artifact_required\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"watermark_required_before_suppression\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"op\":\"keep\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"op\":\"upsert\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"fingerprint_match\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"fingerprint_match\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"first_key\":\"unit:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"first_key\":\"unit:b\"") != null);
}

test "db enrichment runtime document extraction skips stable unit local rewrites without text consumers" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingDenseEmbedder{};
    var fake = @import("../test_support.zig").TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "dv_document_chunks",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expectEqual(@as(usize, 1), first_calls);

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain,alpha%20beta%20gamma\"}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(first_calls, counting.calls);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"op\":\"keep\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"fingerprint_match\":true") != null);
}

test "db enrichment runtime document extraction manifest inspection and reprocess API" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    var inspected = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer inspected.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", inspected.document_id);
    try std.testing.expectEqualStrings("document_units_v1", inspected.artifact_name);
    try std.testing.expect(std.mem.startsWith(u8, inspected.artifact_id, "af1:asset:"));
    try std.testing.expectEqual(@as(u64, 2), inspected.manifest_version);
    try std.testing.expectEqual(@as(u64, 1), inspected.generation);
    try std.testing.expectEqualStrings("data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==", inspected.source_url);
    try std.testing.expectEqual(@as(usize, 64), inspected.source_fingerprint.len);
    try std.testing.expectEqualStrings("text/plain", inspected.content_type);
    try std.testing.expectEqualStrings("text", inspected.route_type);
    try std.testing.expectEqual(@as(usize, 1), inspected.unit_count);
    try std.testing.expectEqual(@as(usize, 1), inspected.chunk_count);
    try std.testing.expectEqual(@as(usize, 2), inspected.child_range_count);
    try std.testing.expectEqual(@as(usize, 2), inspected.child_ranges.len);
    try std.testing.expectEqualStrings("range:000000", inspected.child_ranges[0].range_id);
    try std.testing.expectEqualStrings("unit", inspected.child_ranges[0].range_kind);
    try std.testing.expectEqualStrings("document_units_v1", inspected.child_ranges[0].artifact_name);
    try std.testing.expectEqual(@as(?u64, 0), inspected.child_ranges[0].owner_group_id);
    try std.testing.expectEqual(@as(?u64, 0), inspected.child_ranges[0].placement_generation);
    try std.testing.expectEqualStrings("local_committed", inspected.child_ranges[0].route_status.?);
    try std.testing.expectEqual(@as(?bool, false), inspected.child_ranges[0].split_eligible);
    try std.testing.expectEqual(@as(usize, 1), inspected.child_ranges[0].child_count);
    try std.testing.expect(inspected.child_ranges[0].text_bytes != null);
    try std.testing.expectEqualStrings("chunk", inspected.child_ranges[1].range_kind);
    try std.testing.expectEqualStrings("chunk", inspected.child_ranges[1].split_boundary);
    try std.testing.expect(std.mem.indexOf(u8, inspected.manifest_json, "\"range_policy\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, inspected.manifest_json, "\"unit_target_children\":256") != null);
    try std.testing.expect(std.mem.indexOf(u8, inspected.manifest_json, "\"unit_target_text_bytes\":1048576") != null);
    try std.testing.expect(std.mem.indexOf(u8, inspected.manifest_json, "\"oversized_unit_policy\":\"single_unit_range\"") != null);
    try std.testing.expectEqualStrings("converged", inspected.merge_status);
    try std.testing.expectEqual(@as(u64, 0), inspected.merge_from_generation);
    try std.testing.expectEqual(@as(u64, 1), inspected.merge_to_generation);
    try std.testing.expectEqualStrings("unit_fingerprint", inspected.merge_operation_granularity);
    try std.testing.expect(inspected.merge_operation_count > 0);
    try std.testing.expect(inspected.state_json != null);

    try std.testing.expect(try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "document_units_v1", .{
        .range_id = "range:000000",
        .placement = "remote",
        .owner_group_id = 7001,
        .placement_generation = 2,
        .route_status = "remote_committed",
        .split_eligible = true,
    }));
    var moved = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer moved.deinit(alloc);
    try std.testing.expectEqualStrings("remote", moved.child_ranges[0].placement);
    try std.testing.expectEqual(@as(?u64, 7001), moved.child_ranges[0].owner_group_id);
    try std.testing.expectEqual(@as(?u64, 2), moved.child_ranges[0].placement_generation);
    try std.testing.expectEqualStrings("remote_committed", moved.child_ranges[0].route_status.?);
    try std.testing.expectEqual(@as(?bool, true), moved.child_ranges[0].split_eligible);
    try std.testing.expect(std.mem.indexOf(u8, moved.manifest_json, "\"owner_group_id\":7001") != null);
    try std.testing.expect(!try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "document_units_v1", .{
        .range_id = "range:missing",
        .placement = "remote",
    }));
    try std.testing.expect(!try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:missing", "document_units_v1", .{
        .range_id = "range:000000",
        .placement = "remote",
    }));

    var artifact_list = try db.listDocumentArtifactManifests(alloc, "doc:a");
    defer artifact_list.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", artifact_list.document_id);
    try std.testing.expectEqual(@as(usize, 1), artifact_list.artifacts.len);
    try std.testing.expectEqualStrings("document_units_v1", artifact_list.artifacts[0].artifact_name);
    try std.testing.expectEqual(@as(u64, 1), artifact_list.artifacts[0].generation);
    try std.testing.expect(artifact_list.artifacts[0].state_json != null);

    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", "document_units_v1"));

    var after = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), after.generation);
    try std.testing.expectEqualStrings("remote", after.child_ranges[0].placement);
    try std.testing.expectEqual(@as(?u64, 7001), after.child_ranges[0].owner_group_id);
    try std.testing.expectEqual(@as(?u64, 2), after.child_ranges[0].placement_generation);
    try std.testing.expectEqualStrings("remote_committed", after.child_ranges[0].route_status.?);
    try std.testing.expect(std.mem.indexOf(u8, after.manifest_json, "\"op\":\"upsert\"") != null);
    const routed_unit_payload = try db.core.store.get(alloc, after.child_ranges[0].start_key);
    defer alloc.free(routed_unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, routed_unit_payload, "\"_artifact_route_status\":\"remote_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, routed_unit_payload, "\"_artifact_owner_group_id\":7001") != null);

    try std.testing.expect(!try db.reprocessDocumentArtifact(alloc, "doc:missing", "document_units_v1"));
    try std.testing.expect((try db.getDocumentArtifactManifest(alloc, "doc:missing", "document_units_v1")) == null);
    var missing_list = try db.listDocumentArtifactManifests(alloc, "doc:missing");
    defer missing_list.deinit(alloc);
    try std.testing.expectEqualStrings("doc:missing", missing_list.document_id);
    try std.testing.expectEqual(@as(usize, 0), missing_list.artifacts.len);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:b",
            .value = "{\"url\":\"data:text/plain;base64,ZGVsdGEgZXBzaWxvbg==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    var range_first = try db.reprocessDocumentArtifactRange(alloc, "document_units_v1", .{ .limit = 1 });
    defer range_first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_first.scanned);
    try std.testing.expectEqual(@as(usize, 1), range_first.reprocessed);
    try std.testing.expectEqual(@as(usize, 0), range_first.failed);
    try std.testing.expectEqual(@as(usize, 0), range_first.failures.len);
    try std.testing.expect(range_first.next_key != null);
    try std.testing.expectEqualStrings("doc:a", range_first.next_key.?);

    var range_after_a = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer range_after_a.deinit(alloc);
    try std.testing.expect(range_after_a.generation >= 2);

    var range_second = try db.reprocessDocumentArtifactRange(alloc, "document_units_v1", .{ .from_key = range_first.next_key.? });
    defer range_second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_second.scanned);
    try std.testing.expectEqual(@as(usize, 1), range_second.reprocessed);
    try std.testing.expect(range_second.next_key == null);

    var range_after_b = (try db.getDocumentArtifactManifest(alloc, "doc:b", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer range_after_b.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), range_after_b.generation);
}

test "db enrichment runtime document extraction unit ranges split by text bytes" {
    const alloc = std.testing.allocator;
    const text_a = try alloc.alloc(u8, 600 * 1024);
    defer alloc.free(text_a);
    const text_b = try alloc.alloc(u8, 400 * 1024);
    defer alloc.free(text_b);
    const text_c = try alloc.alloc(u8, 100 * 1024);
    defer alloc.free(text_c);
    @memset(text_a, 'a');
    @memset(text_b, 'b');
    @memset(text_c, 'c');

    const units = [_]document_extraction_mod.Unit{
        .{
            .unit_id = @constCast("unit:a"),
            .unit_type = @constCast("document"),
            .text = text_a,
            .method = @constCast("text"),
        },
        .{
            .unit_id = @constCast("unit:b"),
            .unit_type = @constCast("document"),
            .text = text_b,
            .method = @constCast("text"),
        },
        .{
            .unit_id = @constCast("unit:c"),
            .unit_type = @constCast("document"),
            .text = text_c,
            .method = @constCast("text"),
        },
    };

    try std.testing.expectEqual(@as(usize, 2), documentExtractionUnitRangeCount(&units));
    try std.testing.expectEqual(@as(usize, 0), documentExtractionUnitRangeIndex(&units, 0));
    try std.testing.expectEqual(@as(usize, 0), documentExtractionUnitRangeIndex(&units, 1));
    try std.testing.expectEqual(@as(usize, 1), documentExtractionUnitRangeIndex(&units, 2));
}

test "db enrichment runtime document extraction failure records last error and clears stale children" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    var before = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer before.deinit(alloc);
    try std.testing.expectEqualStrings("text", before.route_type);
    try std.testing.expect(before.last_error_code == null);
    try std.testing.expectEqual(@as(usize, 1), before.child_ranges.len);
    const stale_unit_key = try alloc.dupe(u8, before.child_ranges[0].start_key);
    defer alloc.free(stale_unit_key);
    {
        const stale_unit = try db.core.store.get(alloc, stale_unit_key);
        defer alloc.free(stale_unit);
    }

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64\"}",
        }},
        .sync_level = .full_index,
    });

    var after = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, before.generation + 1), after.generation);
    try std.testing.expectEqualStrings("error", after.route_type);
    try std.testing.expectEqualStrings("failed", after.merge_status);
    try std.testing.expectEqual(@as(usize, 0), after.unit_count);
    try std.testing.expectEqual(@as(usize, 0), after.child_range_count);
    try std.testing.expect(after.state_json == null);
    try std.testing.expect(after.last_error_code != null);
    try std.testing.expectEqualStrings("InvalidDataUri", after.last_error_code.?);
    try std.testing.expect(after.last_error_message != null);
    try std.testing.expectEqualStrings("remote content download failed", after.last_error_message.?);
    try std.testing.expect(std.mem.indexOf(u8, after.manifest_json, "\"last_error\"") != null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_unit_key));

    var list = try db.listDocumentArtifactManifests(alloc, "doc:a");
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), list.artifacts.len);
    try std.testing.expectEqualStrings("InvalidDataUri", list.artifacts[0].last_error_code.?);
}

test "db enrichment runtime document extraction skips stable unit local rewrites while replaying full text from stored artifacts" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "ft_document_units",
        .kind = .full_text,
        .config_json = "{\"artifact_name\":\"document_units_v1\"}",
    });
    try db.addIndex(.{
        .name = "ft_document_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"document_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "dv_document_chunks",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    const first_value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}";
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = first_value }},
        .sync_level = .full_index,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expectEqual(@as(usize, 1), first_calls);

    const second_value = "{\"url\":\"data:text/plain,alpha%20beta%20gamma\"}";
    var extracted = [_]mapper.ExtractedWrite{try mapper.extractWrite(alloc, "doc:a", second_value)};
    defer extracted[0].deinit(alloc);
    var precomputed = try db.prepareGeneratedEnrichments(.{
        .writes = &.{.{ .key = "doc:a", .value = second_value }},
    }, &extracted, .all, &.{});
    defer precomputed.deinit(alloc);

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "document:000001", 0);
    defer alloc.free(chunk_key);

    try std.testing.expectEqual(first_calls, counting.calls);
    try std.testing.expectEqual(@as(usize, 2), precomputed.documents.len);
    var saw_unit_doc = false;
    var saw_chunk_doc = false;
    for (precomputed.documents) |doc| {
        try std.testing.expectEqual(@as(?[]const u8, null), doc.cleaned_value);
        if (std.mem.eql(u8, doc.key, unit_key)) saw_unit_doc = true;
        if (std.mem.eql(u8, doc.key, chunk_key)) saw_chunk_doc = true;
    }
    try std.testing.expect(saw_unit_doc);
    try std.testing.expect(saw_chunk_doc);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = second_value }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(first_calls, counting.calls);

    var unit_result = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .chunk,
    });
    defer unit_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), unit_result.total_hits);

    var chunk_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .chunk,
    });
    defer chunk_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), chunk_result.total_hits);
}

test "db enrichment runtime document extraction chunks units through source artifact enrichment" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addEnrichment(.{
        .name = "document_chunk_sparse_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
    });
    try db.addIndex(.{
        .name = "ft_document_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"document_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "dv_document_chunks",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });
    try db.addIndex(.{
        .name = "document_chunk_sparse_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\"}",
    });
    const embedding_cfg = db.core.index_manager.getEnrichment(.embedding, "document_chunk_dense_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("document_chunks_v1", embedding_cfg.source_artifact_name);
    try std.testing.expectEqual(@as(u32, 3), embedding_cfg.expected_dims);
    const dense_consumers = try db.core.index_manager.denseIndexesForEmbedding(alloc, "document_chunk_dense_v1", 3);
    defer {
        for (dense_consumers) |name| alloc.free(name);
        alloc.free(dense_consumers);
    }
    try std.testing.expectEqual(@as(usize, 1), dense_consumers.len);
    try std.testing.expectEqualStrings("dv_document_chunks", dense_consumers[0]);
    const sparse_consumers = try db.core.index_manager.sparseIndexesForEmbedding(alloc, "document_chunk_sparse_v1");
    defer {
        for (sparse_consumers) |name| alloc.free(name);
        alloc.free(sparse_consumers);
    }
    try std.testing.expectEqual(@as(usize, 1), sparse_consumers.len);
    try std.testing.expectEqualStrings("document_chunk_sparse_v1", sparse_consumers[0]);

    const planned = try db.core.planGeneratedEnrichments(
        alloc,
        "doc:planned",
        "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        &.{},
        &.{},
    );
    defer enrichment_types.deinitGeneratedRequests(alloc, planned);
    var saw_document_asset = false;
    var saw_document_chunk_dense = false;
    var saw_document_chunk_sparse = false;
    for (planned) |request| {
        if (request.kind == .asset and std.mem.eql(u8, request.artifact_name, "document_units_v1")) saw_document_asset = true;
        if (request.kind == .dense_embedding and
            std.mem.eql(u8, request.artifact_name, "document_chunks_v1") and
            std.mem.eql(u8, request.embedding_name, "document_chunk_dense_v1") and
            request.chunk_size == 256)
        {
            saw_document_chunk_dense = true;
        }
        if (request.kind == .sparse_embedding and
            std.mem.eql(u8, request.artifact_name, "document_chunks_v1") and
            std.mem.eql(u8, request.embedding_name, "document_chunk_sparse_v1") and
            request.chunk_size == 256)
        {
            saw_document_chunk_sparse = true;
        }
    }
    try std.testing.expect(saw_document_asset);
    try std.testing.expect(!saw_document_chunk_dense);
    try std.testing.expect(!saw_document_chunk_sparse);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .full_index,
    });

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "document:000001", 0);
    defer alloc.free(chunk_key);
    const state_key = try assetStateKeyAlloc(alloc, "doc:a", "document_units_v1");
    defer alloc.free(state_key);
    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);

    const chunk_payload = try db.core.store.get(alloc, chunk_key);
    defer alloc.free(chunk_payload);
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"_parent_doc_key\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"_parent_unit_id\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"_artifact_name\":\"document_chunks_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"_source_artifact_name\":\"document_units_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"text\":\"alpha beta gamma\"") != null);
    var parsed_chunk_payload = try std.json.parseFromSlice(std.json.Value, alloc, chunk_payload, .{});
    defer parsed_chunk_payload.deinit();
    try std.testing.expectEqualStrings("range:000001", parsed_chunk_payload.value.object.get("_artifact_range_id").?.string);
    try std.testing.expectEqualStrings("chunk", parsed_chunk_payload.value.object.get("_artifact_range_kind").?.string);
    try std.testing.expectEqualStrings("local_committed", parsed_chunk_payload.value.object.get("_artifact_route_status").?.string);
    try std.testing.expectEqual(@as(i64, 0), parsed_chunk_payload.value.object.get("_artifact_owner_group_id").?.integer);
    const chunk_provenance = parsed_chunk_payload.value.object.get("provenance").?.object;
    try std.testing.expectEqualStrings("unit", chunk_provenance.get("offset_basis").?.string);
    try std.testing.expectEqualStrings("document:000001", chunk_provenance.get("parent_unit_id").?.string);
    try std.testing.expectEqualStrings("document_units_v1", chunk_provenance.get("source_artifact_name").?.string);
    try std.testing.expectEqual(@as(i64, 0), chunk_provenance.get("unit_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 16), chunk_provenance.get("unit_char_end").?.integer);
    try std.testing.expectEqual(@as(i64, 0), chunk_provenance.get("document_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 16), chunk_provenance.get("document_char_end").?.integer);

    const dense_artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "document_chunk_dense_v1");
    defer alloc.free(dense_artifact_key);
    const dense_artifact_payload = try db.core.store.get(alloc, dense_artifact_key);
    defer alloc.free(dense_artifact_payload);
    const sparse_artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "document_chunk_sparse_v1");
    defer alloc.free(sparse_artifact_key);
    const sparse_artifact_payload = try db.core.store.get(alloc, sparse_artifact_key);
    defer alloc.free(sparse_artifact_payload);

    const state = try db.core.store.get(alloc, state_key);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"chunk_keys\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_descriptors\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"fingerprint\"") != null);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"manifest_version\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"child_ranges\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"range_kind\":\"unit\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"range_kind\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"merge_plan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"from_generation\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"to_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"operation_granularity\":\"unit_fingerprint\"") != null);

    var result = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    var parent_with_chunks = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .parent_with_chunks,
        .include_stored = false,
    });
    defer parent_with_chunks.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_with_chunks.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_with_chunks.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), parent_with_chunks.hits[0].chunk_hits.len);
    var chunk_public_id_for_rollup = try artifact_ids.resolvePublicHitIdentityAlloc(alloc, chunk_key);
    defer chunk_public_id_for_rollup.deinit(alloc);
    try std.testing.expectEqualStrings(chunk_public_id_for_rollup.id, parent_with_chunks.hits[0].chunk_hits[0].id);
    const rollup_chunk_ref = parent_with_chunks.hits[0].chunk_hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("doc:a", rollup_chunk_ref.document_id);
    try std.testing.expectEqualStrings("document_chunks_v1", rollup_chunk_ref.name);
    try std.testing.expectEqual(types.ArtifactKind.chunk, rollup_chunk_ref.kind);
    try std.testing.expectEqual(@as(?u32, 0), rollup_chunk_ref.chunk_id);
    try std.testing.expectEqualStrings("document:000001", rollup_chunk_ref.unit_id.?);

    const query_vec = try deterministic_dense.interface().embedDense(alloc, "document_chunk_dense_v1", "alpha beta gamma", 3);
    defer alloc.free(query_vec);
    const dense_index = db.core.index_manager.denseIndex("dv_document_chunks") orelse return error.IndexNotFound;
    var direct = try TestHelpers.waitForDenseIndexResultsWithAttempts(&dense_index.index, query_vec, 3, 1, TestHelpers.slow_test_wait_attempts);
    defer direct.deinit();
    const dense_internal_id = if (direct.takeMetadata(0)) |metadata|
        metadata
    else blk: {
        const hit = direct.getHits()[0];
        break :blk (try dense_index.index.getMetadata(hit.vector_id)) orelse return error.TestUnexpectedResult;
    };
    defer alloc.free(dense_internal_id);
    try std.testing.expectEqualStrings(chunk_key, dense_internal_id);

    var sparse_query = try deterministic_sparse.interface().embedSparse(alloc, "document_chunk_sparse_v1", "alpha beta gamma");
    defer sparse_query.deinit(alloc);
    var sparse_result = try db.search(alloc, .{
        .index_name = "document_chunk_sparse_v1",
        .query = .{ .sparse_knn = .{
            .indices = sparse_query.indices,
            .values = sparse_query.values,
            .k = 3,
        } },
        .return_mode = .chunk,
        .limit = 1,
        .include_stored = false,
        .hierarchy_include_source = true,
        .hierarchy_include_unit = true,
    });
    defer sparse_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_result.total_hits);
    var chunk_public_id = try artifact_ids.resolvePublicHitIdentityAlloc(alloc, chunk_key);
    defer chunk_public_id.deinit(alloc);
    try std.testing.expectEqualStrings(chunk_public_id.id, sparse_result.hits[0].id);
    try std.testing.expect(sparse_result.hits[0].stored_data == null);
    try std.testing.expect(sparse_result.hits[0].ancestor_source_data != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_result.hits[0].ancestor_source_data.?, "\"url\"") != null);
    try std.testing.expect(sparse_result.hits[0].ancestor_unit_data != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_result.hits[0].ancestor_unit_data.?, "\"unit_id\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_result.hits[0].ancestor_unit_data.?, "\"text\":\"alpha beta gamma\"") != null);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBkZWx0YQ==\"}",
        }},
        .sync_level = .full_index,
    });
    const updated_manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(updated_manifest);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"from_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"to_generation\":2") != null);
    const updated_chunk_payload = try db.core.store.get(alloc, chunk_key);
    defer alloc.free(updated_chunk_payload);
    try std.testing.expect(std.mem.indexOf(u8, updated_chunk_payload, "\"text\":\"alpha beta delta\"") != null);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"\"}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unit_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, chunk_key));
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

test "extractSourceText reads a single field straight from a typed row (Seam B)" {
    const alloc = std.testing.allocator;
    // A serialized relational typed row, not JSON. The single-field path must
    // read the requested column directly without reconstructing the document.
    const cells = [_]relational_row_codec.Cell{
        .{ .path = "title", .value_type = .bytes_val, .value = .{ .bytes_val = "Hello" } },
        .{ .path = "body", .value_type = .bytes_val, .value = .{ .bytes_val = "World" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 12.5 } },
    };
    const row = try relational_row_codec.serialize(alloc, &cells);
    defer alloc.free(row);

    const request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "body",
    };
    const result = try extractSourceText(alloc, .{}, row, request) orelse return error.TestUnexpectedResult;
    defer alloc.free(result);
    try std.testing.expectEqualStrings("World", result);

    // A non-string column (numeric) is not valid source text -> null.
    const numeric_request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "amount",
    };
    try std.testing.expect((try extractSourceText(alloc, .{}, row, numeric_request)) == null);

    // A column the row does not carry -> null.
    const missing_request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "nope",
    };
    try std.testing.expect((try extractSourceText(alloc, .{}, row, missing_request)) == null);
}

test "extractSourceText reads embedded json fields from typed rows" {
    const alloc = std.testing.allocator;
    const cells = [_]relational_row_codec.Cell{
        .{ .path = "id", .value_type = .bytes_val, .value = .{ .bytes_val = "doc:1" } },
        .{ .path = "attrs", .value_type = .bytes_val, .is_json = true, .value = .{ .bytes_val = "{\"plan\":\"pro\",\"body\":\"embedded text\",\"nested\":{\"title\":\"deep text\"},\"rank\":7}" } },
    };
    const row = try relational_row_codec.serialize(alloc, &cells);
    defer alloc.free(row);

    const body_request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "attrs.body",
    };
    const body = try extractSourceText(alloc, .{}, row, body_request) orelse return error.TestUnexpectedResult;
    defer alloc.free(body);
    try std.testing.expectEqualStrings("embedded text", body);

    const nested_request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "attrs.nested.title",
    };
    const nested = try extractSourceText(alloc, .{}, row, nested_request) orelse return error.TestUnexpectedResult;
    defer alloc.free(nested);
    try std.testing.expectEqualStrings("deep text", nested);

    const numeric_request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "attrs.rank",
    };
    try std.testing.expect((try extractSourceText(alloc, .{}, row, numeric_request)) == null);
}

test "extractSourceText reads dotted fields from document blobs" {
    const alloc = std.testing.allocator;
    const doc = "{\"attrs\":{\"body\":\"document embedded text\"}}";
    const request = enrichment_types.GeneratedEnrichmentRequest{
        .kind = .dense_embedding,
        .index_name = "idx",
        .doc_key = "doc:1",
        .source_field = "attrs.body",
    };
    const result = try extractSourceText(alloc, .{}, doc, request) orelse return error.TestUnexpectedResult;
    defer alloc.free(result);
    try std.testing.expectEqualStrings("document embedded text", result);
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

test "db enrichment runtime batch marks generated enrichment replay for generator-enabled dense index" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_size\":256,\"chunk_overlap\":32}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"needs generated embedding\"}" },
        },
        .sync_level = .write,
    });

    const journal_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);

    var journal_record = try change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expect(journal_record.record.changed_doc_keys.len >= 1);
    try std.testing.expectEqualStrings("doc:a", journal_record.record.changed_doc_keys[0]);
    try std.testing.expect(change_journal_mod.recordHasHint(journal_record.record, .enrichment));
    try std.testing.expect(!change_journal_mod.recordHasHint(journal_record.record, .dense_vector));
}

test "db enrichment runtime status changes notify query visibility hook" {
    const alloc = std.testing.allocator;
    const db_mod = @import("../mod.zig");
    const DB = db_mod.DB;
    const QueryVisibilityChange = db_mod.QueryVisibilityChange;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();

    const HookCtx = struct {
        calls: u64 = 0,
        table_name: ?[]const u8 = null,
        group_id: u64 = 0,
        saw_db: bool = false,
        change: ?QueryVisibilityChange = null,

        fn onChange(ptr: *anyopaque, table_name: []const u8, group_id: u64, changed_db: ?*DB, change: QueryVisibilityChange) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.table_name = table_name;
            self.group_id = group_id;
            self.saw_db = changed_db != null;
            self.change = change;
        }
    };
    var hook_ctx = HookCtx{};
    db.setQueryVisibilityHook(.{
        .ptr = &hook_ctx,
        .table_name = "docs",
        .group_id = 7001,
        .db = &db,
        .on_change = HookCtx.onChange,
    });

    try db.enrichment_runtime.?.markAppliedThrough(1);

    try std.testing.expectEqual(@as(u64, 1), hook_ctx.calls);
    try std.testing.expectEqualStrings("docs", hook_ctx.table_name.?);
    try std.testing.expectEqual(@as(u64, 7001), hook_ctx.group_id);
    try std.testing.expect(hook_ctx.saw_db);
    try std.testing.expectEqual(QueryVisibilityChange.invalidate, hook_ctx.change.?);
}

test "db enrichment runtime full_index sync precomputes generated enrichments into the committed batch" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .full_index,
    });

    var text_result = try db.search(alloc, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
    });
    defer text_result.deinit();
    try std.testing.expect(text_result.total_hits > 0);

    var dense_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 0.0, 0.0 }, .k = 1 },
    });
    defer dense_result.deinit();
    try std.testing.expect(dense_result.total_hits > 0);

    const sparse_applied = try db.core.loadAppliedSequence(alloc, "sp_v1");
    try std.testing.expect(sparse_applied > 0);
}

test "db enrichment runtime enrichments sync precomputes generated enrichments into the committed batch" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .enrichments,
    });
    try std.testing.expectEqual(@as(u64, 1), db.enrichment_runtime.?.stats().applied_sequence);

    const journal_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);

    var journal_record = try change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expect(journal_record.record.changed_doc_keys.len >= 1);
    try std.testing.expectEqualStrings("doc:a", journal_record.record.changed_doc_keys[0]);
    try std.testing.expect(change_journal_mod.recordHasHint(journal_record.record, .full_text));
    try std.testing.expect(change_journal_mod.recordHasHint(journal_record.record, .dense_vector));
    try std.testing.expect(change_journal_mod.recordHasHint(journal_record.record, .sparse_vector));
    try std.testing.expect(!change_journal_mod.recordHasHint(journal_record.record, .enrichment));
    try std.testing.expect(journal_record.record.changed_artifact_keys.len > 0);
}

test "db enrichment runtime replicated apply decouples client sync from raft apply execution" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });

    try db.batchReplicatedApply(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .enrichments,
    });

    const journal_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);

    var journal_record = try change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expect(change_journal_mod.recordHasHint(journal_record.record, .enrichment));
    try std.testing.expect(!change_journal_mod.recordHasHint(journal_record.record, .dense_vector));
    try std.testing.expectEqual(@as(usize, 0), journal_record.record.changed_artifact_keys.len);
}

test "db enrichment runtime precomputed watermark advances across replay entries without enrichment debt" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
        },
    });
    defer db.close();

    const inert_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:before"},
        .target_hints = &.{.full_text},
    });
    defer alloc.free(inert_payload);
    try db.core.store.appendReplayOpaque(alloc, 1, inert_payload);
    try db.enrichment_runtime.?.resumeFrom(0, 0);
    try std.testing.expectEqual(@as(u64, 0), db.enrichment_runtime.?.stats().applied_sequence);

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"precomputed dense text\"}" },
        },
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(db.core.nextDerivedSequence(), db.enrichment_runtime.?.stats().applied_sequence);
}

test "db enrichment runtime asset producer executes fake providers and skips unchanged state" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var fake = @import("../test_support.zig").TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "generated_title_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"mock\"}}",
    });
    try db.addEnrichment(.{
        .name = "image_text_v1",
        .kind = .asset,
        .field = "image",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"reader\",\"config\":{\"provider\":\"mock\"}}",
    });
    try db.addEnrichment(.{
        .name = "audio_text_v1",
        .kind = .asset,
        .field = "audio",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"transcriber\",\"config\":{\"provider\":\"mock\"}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"hello\",\"image\":\"data:image/png;base64,aaa\",\"audio\":\"https://example.test/a.wav\"}",
        }},
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.generator_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);

    var first = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{"_artifacts"},
        .include_all_fields = false,
    })).?;
    defer first.deinit(alloc);
    var parsed_first = try std.json.parseFromSlice(std.json.Value, alloc, first.json, .{});
    defer parsed_first.deinit();
    const first_artifacts = parsed_first.value.object.get("_artifacts").?.object;
    try std.testing.expectEqualStrings("generator:hello", first_artifacts.get("generated_title_v1").?.object.get("value").?.string);
    try std.testing.expectEqualStrings("reader:data:image/png;base64,aaa", first_artifacts.get("image_text_v1").?.object.get("value").?.string);
    try std.testing.expectEqualStrings("transcriber:https://example.test/a.wav", first_artifacts.get("audio_text_v1").?.object.get("value").?.string);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"hello\",\"image\":\"data:image/png;base64,aaa\",\"audio\":\"https://example.test/a.wav\"}",
        }},
        .sync_level = .enrichments,
    });
    try std.testing.expectEqual(@as(usize, 3), fake.calls);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"goodbye\",\"image\":\"data:image/png;base64,aaa\",\"audio\":\"https://example.test/a.wav\"}",
        }},
        .sync_level = .enrichments,
    });
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
    try std.testing.expectEqual(@as(usize, 2), fake.generator_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);

    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "generated_title_v1");
    defer alloc.free(asset_key);
    const marker_key = try internal_keys.assetArtifactSourceIndexKeyAlloc(alloc, "generated_title_v1", "doc:a");
    defer alloc.free(marker_key);
    const state_key = try assetStateKeyAlloc(alloc, "doc:a", "generated_title_v1");
    defer alloc.free(state_key);
    const stored_asset = try db.core.store.get(alloc, asset_key);
    alloc.free(stored_asset);
    const stored_marker = try db.core.store.get(alloc, marker_key);
    defer alloc.free(stored_marker);
    try std.testing.expectEqualStrings(asset_key, stored_marker);
    const stored_state = try db.core.store.get(alloc, state_key);
    alloc.free(stored_state);

    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .write,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, asset_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, marker_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, state_key));
}

test "db enrichment runtime runUntilIdle drains enrichment and derived indexing" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "embedded-worker",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"generated vector text\"}" },
        },
        .sync_level = .write,
    });

    const pending_before = db.pendingWorkStats();
    try std.testing.expect(pending_before.derived_target_sequence >= 1);

    try db.runUntilIdle();

    const pending_after = db.pendingWorkStats();
    try std.testing.expectEqual(pending_after.derived_target_sequence, pending_after.enrichment.applied_sequence);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "generated vector text", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db enrichment runtime relational sources read committed base rows" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;
    const schema_api_mod = @import("../../../schema/mod.zig");
    const schema_mod = @import("../../schema.zig");
    const relational_store_mod = @import("../relational_store.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"body":{"type":"text"}},"required":["title","body"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "row:a",
            .value = "{\"title\":\"alpha\",\"body\":\"generated relational vector text\"}",
        }},
        .sync_level = .write,
    });
    try db.enrichment_runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:a");
    defer alloc.free(primary_key);
    const maybe_primary = db.core.store.get(alloc, primary_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (maybe_primary) |primary_value| {
        defer alloc.free(primary_value);
        return error.TestExpectedEqual;
    }

    const relational_key = try relational_store_mod.rowKeyAlloc(alloc, "row:a");
    defer alloc.free(relational_key);
    const raw_row = try db.core.store.get(alloc, relational_key);
    defer alloc.free(raw_row);
    try std.testing.expect(mapper.isRelationalRowValue(raw_row));

    const query_vec = try deterministic.interface().embedDense(alloc, "", "generated relational vector text", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("row:a", result.hits[0].id);
}

test "db enrichment runtime managed dense remains searchable after transient rate limits" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;
    const GateDenseEmbedder = @import("../test_support.zig").GateDenseEmbedder;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var gated = GateDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"gamma implementation details\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        const snapshot = gated.snapshot();
        if (snapshot.rate_limited_requests > 0 and snapshot.successful_requests >= 1) break;
        platform.time.sleepMs(10);
    }

    const before_release = gated.snapshot();
    try std.testing.expect(before_release.rate_limited_requests > 0);
    try std.testing.expect(before_release.successful_requests >= 1);

    gated.allowAll();

    var ready = false;
    var attempts_after_release: usize = 0;
    while (attempts_after_release < 500) : (attempts_after_release += 1) {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        if (stats.indexes[0].doc_count == 3 and
            stats.indexes[0].replay_applied_sequence >= 2 and
            stats.indexes[0].replay_applied_sequence == stats.indexes[0].replay_target_sequence and
            !stats.indexes[0].backfill_active)
        {
            ready = true;
            break;
        }
        platform.time.sleepMs(10);
    }
    try std.testing.expect(ready);

    var result = try db.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 3,
        },
        .limit = 3,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    try db.runUntilIdle();
}

test "db enrichment runtime managed dense delete recreate recovers after corrupt artifact" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    const cfg: types.IndexConfig = .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    };

    try db.addIndex(cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
    }

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic_idx");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-artifact");

    try std.testing.expect(try db.deleteIndex("semantic_idx"));
    try db.addIndex(cfg);
    try db.runUntilIdle();

    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
        try std.testing.expect(stats.indexes[0].replay_applied_sequence >= 2);
        try std.testing.expectEqual(stats.indexes[0].replay_target_sequence, stats.indexes[0].replay_applied_sequence);
    }

    const query_vec = try counting.interface().embedDense(alloc, "semantic_idx", "alpha concept overview", 3);
    defer alloc.free(query_vec);
    var result = try db.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db enrichment runtime managed dense delete recreate recovers after corrupt artifact across reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingDenseEmbedder{};
    const cfg: types.IndexConfig = .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = counting.interface(),
            },
        });
        defer db.close();

        try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic_idx");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer reopened.close();

    try std.testing.expect(try reopened.deleteIndex("semantic_idx"));
    try reopened.addIndex(cfg);
    try reopened.runUntilIdle();

    {
        const stats = try reopened.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
        try std.testing.expect(stats.indexes[0].replay_applied_sequence >= 2);
        try std.testing.expectEqual(stats.indexes[0].replay_target_sequence, stats.indexes[0].replay_applied_sequence);
    }

    const query_vec = try counting.interface().embedDense(alloc, "semantic_idx", "alpha concept overview", 3);
    defer alloc.free(query_vec);
    var result = try reopened.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db enrichment runtime dense skips unchanged source hash" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"new source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 2), counting.calls);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, artifact_key);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);
    try enrichment_artifact_codec.expectDenseEmbeddingValue(alloc, artifacts[0].value, enrichment_artifact_codec.hashSource("new source"), 3);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 1), stats.enrichment.skip_by_hash_count);
    try std.testing.expectEqual(@as(u64, 0), stats.enrichment.codec_decode_failures);
    try std.testing.expect(stats.enrichment.dense_artifact_bytes_written >= @as(u64, @intCast(artifacts[0].value.len * 2)));
    try std.testing.expectEqual(stats.enrichment.dense_artifact_bytes_written, stats.enrichment.artifact_bytes_written);
}

test "db enrichment runtime dense republishes unchanged source hash from cached artifact after index reset" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), counting.calls);
    try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try db.core.index_manager.resetDenseIndexForArtifactRebuild("dv_v1");
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), counting.calls);
    try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    const query_vec = try counting.interface().embedDense(alloc, "", "same source", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db enrichment runtime chunked dense skips unchanged chunks and deletes stale chunk artifacts" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expect(first_calls > 0);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(first_calls, counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"abcdefgh\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(first_calls, counting.calls);

    {
        const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
        defer alloc.free(chunk_prefix);
        const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, artifacts);

        var chunk_count: usize = 0;
        var embedding_count: usize = 0;
        for (artifacts) |entry| {
            if (internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
            if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) embedding_count += 1;
        }
        try std.testing.expectEqual(@as(usize, 1), chunk_count);
        try std.testing.expectEqual(@as(usize, 1), embedding_count);
        try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    }

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"xyzuvw\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expect(counting.calls > first_calls);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    var embedding_count: usize = 0;
    for (artifacts) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) {
            chunk_count += 1;
            try std.testing.expect(std.mem.indexOf(u8, entry.value, "\"xyzuvw\"") != null);
        } else if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) {
            embedding_count += 1;
            try enrichment_artifact_codec.expectDenseEmbeddingValue(alloc, entry.value, enrichment_artifact_codec.hashSource("xyzuvw"), 3);
        }
    }
    try std.testing.expectEqual(@as(usize, 1), chunk_count);
    try std.testing.expectEqual(@as(usize, 1), embedding_count);
    try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
}

test "db enrichment runtime chunked dense replays cached artifacts after dense reset without re-embedding" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const first_calls = counting.calls;
    try std.testing.expect(first_calls > 0);
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try db.core.index_manager.resetDenseIndexForArtifactRebuild("dv_v1");
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(first_calls, counting.calls);
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    const query_vec = try counting.interface().embedDense(alloc, "chunk_dense_v1", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 3,
        },
        .return_mode = .parent,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db enrichment runtime reopened chunked dense HBC deletes stale vectors through artifact loader" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingDenseEmbedder{};
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = counting.interface(),
            },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
        });

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
            .sync_level = .write,
        });
        try db.runUntilIdle();
        try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    }

    const calls_after_first_open = counting.calls;
    try std.testing.expect(calls_after_first_open > 0);

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = counting.interface(),
            },
        });
        defer reopened.close();

        const entry = reopened.core.index_manager.denseIndex("dv_v1") orelse return error.TestUnexpectedResult;
        try std.testing.expect(entry.index.hasExternalVectorLoader());
        try std.testing.expectEqual(@as(u64, 3), entry.index.metadata.active_count);

        try reopened.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"abcdefgh\"}" }},
            .sync_level = .write,
        });
        try reopened.runUntilIdle();

        try std.testing.expectEqual(calls_after_first_open, counting.calls);
        try std.testing.expectEqual(@as(u64, 1), reopened.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    }
}

test "db enrichment runtime chunked generated dense and sparse embeddings search as parent results" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const dense_vec = try deterministic_dense.interface().embedDense(alloc, "chunk_dense_v1", "abcdefgh", 3);
    defer alloc.free(dense_vec);
    var dense_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = dense_vec, .k = 3 },
        .limit = 1,
        .include_stored = true,
    });
    defer dense_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", dense_result.hits[0].id);
    try std.testing.expect(dense_result.hits[0].stored_data != null);

    var sparse_query = try deterministic_sparse.interface().embedSparse(alloc, "sp_v1", "abcdefgh");
    defer sparse_query.deinit(alloc);
    var sparse_result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = sparse_query.indices,
            .values = sparse_query.values,
            .k = 3,
        } },
        .limit = 1,
        .include_stored = true,
    });
    defer sparse_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", sparse_result.hits[0].id);
    try std.testing.expect(sparse_result.hits[0].stored_data != null);
    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, ordinal), sparse_result.hits[0].doc_ordinal);
    }

    const doc_a_store_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_a_store_key);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(doc_a_store_key);
        try doc_identity.markDeletedTxn(alloc, &txn, 2, "doc:a");
        try txn.commit();
    }
    db.identity_visibility_summary_cache = null;

    var stale_sparse_result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = sparse_query.indices,
            .values = sparse_query.values,
            .k = 3,
        } },
        .return_mode = .chunk,
        .limit = 3,
        .include_stored = false,
    });
    defer stale_sparse_result.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale_sparse_result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), stale_sparse_result.hits.len);
}

test "db enrichment runtime sparse skips unchanged source hash" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .sparse_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\"}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"new source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 2), counting.calls);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sp_v1");
    defer alloc.free(artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, artifact_key);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);
    try enrichment_artifact_codec.expectSparseEmbeddingValue(alloc, artifacts[0].value, enrichment_artifact_codec.hashSource("new source"), 2);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 1), stats.enrichment.skip_by_hash_count);
    try std.testing.expectEqual(@as(u64, 0), stats.enrichment.codec_decode_failures);
    try std.testing.expect(stats.enrichment.sparse_artifact_bytes_written >= @as(u64, @intCast(artifacts[0].value.len * 2)));
    try std.testing.expectEqual(stats.enrichment.sparse_artifact_bytes_written, stats.enrichment.artifact_bytes_written);
}

test "db enrichment runtime chunked sparse skips unchanged chunks and deletes stale sparse artifacts" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = @import("../test_support.zig").CountingSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .sparse_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expect(first_calls > 0);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(first_calls, counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"abcdefgh\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(first_calls, counting.calls);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    var sparse_artifact_count: usize = 0;
    for (artifacts) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
        if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) {
            sparse_artifact_count += 1;
            try enrichment_artifact_codec.expectSparseEmbeddingValue(alloc, entry.value, enrichment_artifact_codec.hashSource("abcdefgh"), 2);
        }
    }
    try std.testing.expectEqual(@as(usize, 1), chunk_count);
    try std.testing.expectEqual(@as(usize, 1), sparse_artifact_count);

    var stale_query = try counting.deterministic.interface().embedSparse(alloc, "sp_v1", "ghijklmn");
    defer stale_query.deinit(alloc);
    var stale_result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = stale_query.indices,
            .values = stale_query.values,
            .k = 10,
        } },
        .return_mode = .chunk,
        .limit = 10,
        .include_stored = false,
    });
    defer stale_result.deinit();

    const stale_chunk_id = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(stale_chunk_id);
    for (stale_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, stale_chunk_id));
    }
}

test "db enrichment runtime computeEnrichments synchronously builds chunk and embedding outputs" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    var result = try db.computeEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value = "{\"body\":\"abcdefghijklmno\"}",
        },
    });
    defer result.deinit(alloc);

    var chunk_artifacts: usize = 0;
    var embedding_artifacts: usize = 0;
    for (result.artifact_writes) |write| {
        if (write.artifact_ref.kind == .chunk) {
            chunk_artifacts += 1;
        } else if (write.artifact_ref.kind == .embedding) {
            embedding_artifacts += 1;
        }
    }

    try std.testing.expectEqual(@as(usize, 3), chunk_artifacts);
    try std.testing.expectEqual(@as(usize, 3), embedding_artifacts);
    try std.testing.expectEqual(@as(usize, 3), result.documents.len);
    try std.testing.expectEqual(@as(usize, 3), result.dense_embeddings.len);
    try std.testing.expectEqual(@as(usize, 0), result.failed_keys.len);
    try std.testing.expectEqualStrings("ft_chunks", result.documents[0].target_index_names[0]);
    try std.testing.expectEqualStrings("dv_v1", result.dense_embeddings[0].index_name);
}

test "db enrichment runtime leased enrichment worker generates dense embeddings" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"generated vector text\"}" },
        },
        .sync_level = .full_index,
    });

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.enrichment.enabled);
    try std.testing.expect(stats.enrichment.lease_owned);
    try std.testing.expect(stats.enrichment.has_lease);
    try std.testing.expect(stats.enrichment.acquisition_count > 0);
    try std.testing.expectEqual(@as(u64, 1), stats.enrichment.applied_sequence);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "generated vector text", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, artifact_key);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);
    try std.testing.expectEqualStrings(artifact_key, artifacts[0].key);
    try enrichment_artifact_codec.expectDenseEmbeddingValue(alloc, artifacts[0].value, enrichment_artifact_codec.hashSource("generated vector text"), 3);
}

test "db enrichment runtime leased enrichment worker generates dense embeddings with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"generated vector text\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.enrichment.enabled);
    try std.testing.expect(stats.enrichment.lease_owned);
    try std.testing.expect(stats.enrichment.has_lease);
    try std.testing.expect(stats.enrichment.acquisition_count > 0);
    try std.testing.expectEqual(@as(u64, 1), stats.enrichment.applied_sequence);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "generated vector text", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, artifact_key);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);
    try std.testing.expectEqualStrings(artifact_key, artifacts[0].key);
    try enrichment_artifact_codec.expectDenseEmbeddingValue(alloc, artifacts[0].value, enrichment_artifact_codec.hashSource("generated vector text"), 3);
}

test "db enrichment runtime leased enrichment worker materializes chunk artifacts" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    var embedding_count: usize = 0;
    for (artifacts) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) {
            if (chunk_count == 0) {
                try std.testing.expectEqualStrings(chunk_zero, entry.key);
                try std.testing.expect(std.mem.indexOf(u8, entry.value, "\"abcdefgh\"") != null);
            }
            chunk_count += 1;
            continue;
        }
        if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) {
            embedding_count += 1;
            try enrichment_artifact_codec.expectDenseEmbeddingWithSourceHash(alloc, entry.value, 3);
        }
    }
    try std.testing.expect(chunk_count > 0);
    try std.testing.expectEqual(@as(usize, 3), embedding_count);
}

test "db enrichment runtime leased enrichment worker keeps chunk storage ephemeral when store_chunks is false" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":8,\"overlap_tokens\":2}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    for (artifacts) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }

    try std.testing.expectEqual(@as(usize, 0), chunk_count);
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
}

test "db enrichment runtime leased enrichment worker persists chunk storage when full text consumes chunked text" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":8,\"overlap_tokens\":2}}}}",
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    for (artifacts) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }

    try std.testing.expect(chunk_count > 0);
}

test "db enrichment runtime leased enrichment worker persists chunk storage when chunker enables full text indexing" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":8,\"overlap_tokens\":2}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    for (artifacts) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }

    try std.testing.expect(chunk_count > 0);
}

test "db enrichment runtime leased enrichment worker materializes chunk artifacts with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    var embedding_count: usize = 0;
    for (artifacts) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) {
            if (chunk_count == 0) {
                try std.testing.expectEqualStrings(chunk_zero, entry.key);
                try std.testing.expect(std.mem.indexOf(u8, entry.value, "\"abcdefgh\"") != null);
            }
            chunk_count += 1;
            continue;
        }
        if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) {
            embedding_count += 1;
            try enrichment_artifact_codec.expectDenseEmbeddingWithSourceHash(alloc, entry.value, 3);
        }
    }
    try std.testing.expectEqual(@as(usize, 3), chunk_count);
    try std.testing.expectEqual(@as(usize, 3), embedding_count);
}

test "db enrichment runtime shared embedding enrichment feeds multiple dense indexes" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"shared_dense_v1\"}}";
    try db.addIndex(.{
        .name = "dv_a",
        .kind = .dense_vector,
        .config_json = shared_cfg,
    });
    try db.addIndex(.{
        .name = "dv_b",
        .kind = .dense_vector,
        .config_json = shared_cfg,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"shared embedding text\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const shared_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "shared_dense_v1");
    defer alloc.free(shared_artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, shared_artifact_key);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "shared embedding text", 3);
    defer alloc.free(query_vec);

    var result_a = try db.search(alloc, .{
        .index_name = "dv_a",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result_a.deinit();
    try std.testing.expectEqual(@as(u32, 1), result_a.total_hits);
    try std.testing.expectEqualStrings("doc:a", result_a.hits[0].id);

    var result_b = try db.search(alloc, .{
        .index_name = "dv_b",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result_b.deinit();
    try std.testing.expectEqual(@as(u32, 1), result_b.total_hits);
    try std.testing.expectEqualStrings("doc:a", result_b.hits[0].id);
}

test "db enrichment runtime shared embedding enrichment feeds multiple dense indexes with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"shared_dense_v1\"}}";
    try db.addIndex(.{
        .name = "dv_a",
        .kind = .dense_vector,
        .config_json = shared_cfg,
    });
    try db.addIndex(.{
        .name = "dv_b",
        .kind = .dense_vector,
        .config_json = shared_cfg,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"shared embedding text\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const shared_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "shared_dense_v1");
    defer alloc.free(shared_artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, shared_artifact_key);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "shared embedding text", 3);
    defer alloc.free(query_vec);

    var result_a = try db.search(alloc, .{
        .index_name = "dv_a",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result_a.deinit();
    try std.testing.expectEqual(@as(u32, 1), result_a.total_hits);
    try std.testing.expectEqualStrings("doc:a", result_a.hits[0].id);

    var result_b = try db.search(alloc, .{
        .index_name = "dv_b",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result_b.deinit();
    try std.testing.expectEqual(@as(u32, 1), result_b.total_hits);
    try std.testing.expectEqualStrings("doc:a", result_b.hits[0].id);
}

test "db enrichment runtime dense index can reference existing whole-doc embedding enrichment" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_gen",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"shared_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "dv_ref",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"shared_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"reference embedding text\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const query_vec = try deterministic.interface().embedDense(alloc, "", "reference embedding text", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_ref",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db enrichment runtime dense index can reference existing chunk embedding enrichment" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_gen",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "dv_ref",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"chunk_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    _ = try TestHelpers.waitForAppliedSequenceAdvance(alloc, &db, "dv_ref", 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);
    const dv_ref = db.core.index_manager.denseIndex("dv_ref") orelse return error.IndexNotFound;
    var direct = try TestHelpers.waitForDenseIndexResultsWithAttempts(&dv_ref.index, query_vec, 3, 1, TestHelpers.slow_test_wait_attempts);
    defer direct.deinit();

    const internal_id = if (direct.takeMetadata(0)) |metadata|
        metadata
    else blk: {
        const hit = direct.getHits()[0];
        break :blk (try dv_ref.index.getMetadata(hit.vector_id)) orelse return error.TestUnexpectedResult;
    };
    defer alloc.free(internal_id);
    var identity = try artifact_ids.resolvePublicHitIdentityAlloc(alloc, internal_id);
    defer identity.deinit(alloc);
    const artifact_ref = identity.artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(types.ArtifactKind.chunk, artifact_ref.kind);
    try std.testing.expectEqualStrings("doc:a", artifact_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", artifact_ref.name);
    try std.testing.expect(artifact_ref.chunk_id != null);
}

test "db enrichment runtime persists shorthand chunk enrichment catalog across reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"body_dense_v1\"}}",
        });

        const enrichment = (try db.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
        defer {
            var tmp = enrichment;
            tmp.deinit(alloc);
        }
        try std.testing.expectEqualStrings("body", enrichment.field);
        try std.testing.expectEqual(@as(u32, 8), enrichment.chunk_size);
        try std.testing.expectEqual(@as(u32, 2), enrichment.chunk_overlap);

        const embedding = (try db.getEnrichment(alloc, .embedding, "body_dense_v1")) orelse return error.TestUnexpectedResult;
        defer {
            var tmp = embedding;
            tmp.deinit(alloc);
        }
        try std.testing.expectEqualStrings("body", embedding.field);
        try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);
        try std.testing.expectEqual(@as(u32, 3), embedding.expected_dims);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    const enrichment = (try reopened.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = enrichment;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", enrichment.field);
    try std.testing.expectEqual(@as(u32, 8), enrichment.chunk_size);
    try std.testing.expectEqual(@as(u32, 2), enrichment.chunk_overlap);

    const embedding = (try reopened.getEnrichment(alloc, .embedding, "body_dense_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = embedding;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", embedding.field);
    try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);
    try std.testing.expectEqual(@as(u32, 3), embedding.expected_dims);
}

test "db enrichment runtime persists shorthand chunk enrichment catalog across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const primary_backend: db_config.PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"body_dense_v1\"}}",
        });

        const enrichment = (try db.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
        defer {
            var tmp = enrichment;
            tmp.deinit(alloc);
        }
        try std.testing.expectEqualStrings("body", enrichment.field);
        try std.testing.expectEqual(@as(u32, 8), enrichment.chunk_size);
        try std.testing.expectEqual(@as(u32, 2), enrichment.chunk_overlap);

        const embedding = (try db.getEnrichment(alloc, .embedding, "body_dense_v1")) orelse return error.TestUnexpectedResult;
        defer {
            var tmp = embedding;
            tmp.deinit(alloc);
        }
        try std.testing.expectEqualStrings("body", embedding.field);
        try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);
        try std.testing.expectEqual(@as(u32, 3), embedding.expected_dims);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
    });
    defer reopened.close();

    const enrichment = (try reopened.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = enrichment;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", enrichment.field);
    try std.testing.expectEqual(@as(u32, 8), enrichment.chunk_size);
    try std.testing.expectEqual(@as(u32, 2), enrichment.chunk_overlap);

    const embedding = (try reopened.getEnrichment(alloc, .embedding, "body_dense_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = embedding;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", embedding.field);
    try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);
    try std.testing.expectEqual(@as(u32, 3), embedding.expected_dims);
}

test "db enrichment runtime listEnrichments returns explicit definitions across reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addEnrichment(.{
            .name = "body_chunks_v1",
            .kind = .chunk,
            .field = "body",
            .chunk_size = 8,
            .chunk_overlap = 2,
        });
        try db.addEnrichment(.{
            .name = "body_dense_v1",
            .kind = .embedding,
            .field = "body",
            .source_artifact_name = "body_chunks_v1",
            .expected_dims = 3,
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    const enrichments = try reopened.listEnrichments(alloc);
    defer types.freeEnrichmentConfigs(alloc, enrichments);
    try std.testing.expectEqual(@as(usize, 2), enrichments.len);
    try std.testing.expectEqualStrings("body_chunks_v1", enrichments[0].name);
    try std.testing.expectEqual(.chunk, enrichments[0].kind);
    try std.testing.expectEqualStrings("body_dense_v1", enrichments[1].name);
    try std.testing.expectEqual(.embedding, enrichments[1].kind);
}

test "db enrichment runtime addEnrichment supports explicit shared definitions" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 8,
        .chunk_overlap = 2,
    });
    try db.addEnrichment(.{
        .name = "chunk_dense_v1",
        .kind = .embedding,
        .field = "body",
        .source_artifact_name = "body_chunks_v1",
        .expected_dims = 3,
    });

    try db.addIndex(.{
        .name = "dv_ref",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"chunk_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    const req: types.SearchRequest = .{
        .index_name = "dv_ref",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .chunk,
    };
    var dense_result = try TestHelpers.waitForDenseSearchResult(alloc, &db, req, 1);
    dense_result.deinit();

    var result = try TestHelpers.waitForSearchResult(alloc, &db, req, 1);
    defer result.deinit();
    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const chunk_one = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(chunk_one);
    try std.testing.expect(std.mem.eql(u8, result.hits[0].id, chunk_zero) or
        std.mem.eql(u8, result.hits[0].id, chunk_one));

    const chunk = (try db.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = chunk;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", chunk.field);

    const embedding = (try db.getEnrichment(alloc, .embedding, "chunk_dense_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = embedding;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);

    const enrichments = try db.listEnrichments(alloc);
    defer types.freeEnrichmentConfigs(alloc, enrichments);
    try std.testing.expectEqual(@as(usize, 2), enrichments.len);
}

test "db enrichment runtime listEnrichments returns explicit definitions across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const primary_backend: db_config.PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.addEnrichment(.{
            .name = "body_chunks_v1",
            .kind = .chunk,
            .field = "body",
            .chunk_size = 8,
            .chunk_overlap = 2,
        });
        try db.addEnrichment(.{
            .name = "body_dense_v1",
            .kind = .embedding,
            .field = "body",
            .source_artifact_name = "body_chunks_v1",
            .expected_dims = 3,
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
    });
    defer reopened.close();

    const enrichments = try reopened.listEnrichments(alloc);
    defer types.freeEnrichmentConfigs(alloc, enrichments);
    try std.testing.expectEqual(@as(usize, 2), enrichments.len);
    try std.testing.expectEqualStrings("body_chunks_v1", enrichments[0].name);
    try std.testing.expectEqual(.chunk, enrichments[0].kind);
    try std.testing.expectEqualStrings("body_dense_v1", enrichments[1].name);
    try std.testing.expectEqual(.embedding, enrichments[1].kind);
}

test "db enrichment runtime dense parent paging fetches enough chunk hits before grouping" {
    const alloc = std.testing.allocator;
    const DB = @import("../mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghabcdefghabcdefgh\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"abcdefzz\"}" },
        },
        .sync_level = .full_index,
    });

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var first_parent = try TestHelpers.waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 1 },
        .return_mode = .parent,
        .limit = 1,
    }, 1);
    defer first_parent.deinit();
    try std.testing.expectEqual(@as(u32, 2), first_parent.total_hits);
    try std.testing.expectEqual(@as(usize, 1), first_parent.hits.len);
    try std.testing.expectEqualStrings("doc:a", first_parent.hits[0].id);

    var second_parent = try TestHelpers.waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 1 },
        .return_mode = .parent,
        .limit = 1,
        .offset = 1,
    }, 1);
    defer second_parent.deinit();
    try std.testing.expectEqual(@as(u32, 2), second_parent.total_hits);
    try std.testing.expectEqual(@as(usize, 1), second_parent.hits.len);
    try std.testing.expectEqualStrings("doc:b", second_parent.hits[0].id);
}
