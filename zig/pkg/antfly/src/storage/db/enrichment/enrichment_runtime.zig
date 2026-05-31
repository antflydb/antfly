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
const internal_keys = @import("../../internal_keys.zig");
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
const chunker_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("chunker_stub.zig")
else
    @import("chunker.zig");
const chunk_artifact_mod = @import("../../../chunking/chunk.zig");
const chunking_types_mod = @import("../../../chunking/types.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const resolver_lib = @import("antfly_resolver");
const ownership_mod = @import("../ownership.zig");
const types = @import("../types.zig");
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
        => true,
        else => false,
    };
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

fn clearPublishedGeneratedArtifacts(runtime: *EnrichmentRuntime) void {
    var it = runtime.published_generated_artifacts.iterator();
    while (it.next()) |entry| runtime.alloc.free(@constCast(entry.key_ptr.*));
    runtime.published_generated_artifacts.clearAndFree(runtime.alloc);
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
            .config = .{
                .lease_ttl_ms = config.lease_ttl_ms,
                .dense_embedder = config.dense_embedder,
                .sparse_embedder = config.sparse_embedder,
                .asset_producer = config.asset_producer,
                .enable_without_producers = config.enable_without_producers,
                .secret_store = config.secret_store,
                .remote_content = config.remote_content,
                .clock = config.clock,
            },
        };
        runtime.applied_sequence = try enrichment_state.loadAppliedSequence(alloc, store, scope_name);
        runtime.target_sequence = runtime.applied_sequence;
        return runtime;
    }

    pub fn deinit(self: *@This()) void {
        clearPublishedGeneratedArtifacts(self);
        if (self.owns_store) self.store.deinit();
        if (self.config.dense_embedder) |dense_embedder| dense_embedder.deinit(self.alloc);
        if (self.config.sparse_embedder) |sparse_embedder| sparse_embedder.deinit(self.alloc);
        if (self.config.asset_producer) |producer| producer.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn start(self: *@This()) !void {
        _ = self;
    }

    pub fn setStatusHook(self: *@This(), hook: ?StatusHook) void {
        _ = self;
        _ = hook;
    }

    pub fn notifySequence(self: *@This(), sequence: u64) void {
        self.target_sequence = @max(self.target_sequence, sequence);
    }

    pub fn resumeFrom(self: *@This(), sequence: u64, target_sequence: u64) !void {
        const next_applied = @min(self.applied_sequence, sequence);
        if (next_applied != self.applied_sequence) {
            try saveAppliedSequenceWithRetry(self, scope_name, next_applied);
            self.applied_sequence = next_applied;
        }
        clearPublishedGeneratedArtifacts(self);
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
            .config = .{
                .lease_ttl_ms = config.lease_ttl_ms,
                .dense_embedder = config.dense_embedder,
                .sparse_embedder = config.sparse_embedder,
                .asset_producer = config.asset_producer,
                .enable_without_producers = config.enable_without_producers,
                .secret_store = config.secret_store,
                .remote_content = config.remote_content,
                .clock = config.clock,
            },
            .ownership = try ownership_mod.State.init(alloc, store, enrichment_lease.default_lease_key, .{
                .lease_owned = true,
                .owner_id = config.owner_id,
                .lease_ttl_ms = config.lease_ttl_ms,
            }),
        };
        runtime.applied_sequence = try enrichment_state.loadAppliedSequence(alloc, store, scope_name);
        runtime.target_sequence = runtime.applied_sequence;
        return runtime;
    }

    pub fn deinit(self: *EnrichmentRuntime) void {
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            self.shutdown = true;
            self.cond.broadcast(io);
            self.mutex.unlock(io);

            if (self.future) |*future| _ = future.await(io);
        }
        self.future = null;
        clearPublishedGeneratedArtifacts(self);
        self.ownership.deinit(self.alloc);
        if (self.owns_store) self.store.deinit();
        if (self.config.dense_embedder) |dense_embedder| dense_embedder.deinit(self.alloc);
        if (self.config.sparse_embedder) |sparse_embedder| sparse_embedder.deinit(self.alloc);
        if (self.config.asset_producer) |producer| producer.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn start(self: *EnrichmentRuntime) !void {
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
            self.target_sequence = @max(self.target_sequence, sequence);
            return;
        };
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        if (sequence > self.target_sequence) self.last_error_name = null;
        self.retrying = false;
        self.worker_failed = false;
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
            .asset => try processAsset(runtime, request, window),
            .chunk_text => try processChunkText(runtime, request, chunk_cache, window),
            .dense_embedding => try processDenseEmbedding(runtime, request, chunk_cache, window),
            .sparse_embedding => try processSparseEmbedding(runtime, request, chunk_cache, window),
        }
    }
}

fn processAsset(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    window: *GeneratedReplayWindow,
) !void {
    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
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
        try storePutBatchWithRetry(runtime, &.{}, &.{ key, state_key });
        try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
        try materializeGraphAssetDeleteForRuntime(runtime, request, window);
        return;
    };
    defer runtime.alloc.free(@constCast(source_text));
    if (source_text.len == 0) {
        const state_key = try assetStateKeyAlloc(runtime.alloc, request.doc_key, artifact_name);
        defer runtime.alloc.free(state_key);
        try storePutBatchWithRetry(runtime, &.{}, &.{ key, state_key });
        try appendUniqueDupeKey(runtime.alloc, &window.changed_artifact_keys, key);
        try materializeGraphAssetDeleteForRuntime(runtime, request, window);
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
        // Provenance: doc->entity mention edges (resolved canonical keys),
        // keyed and tracked alongside the relation edges so they share
        // replace-on-rerender and delete-on-source-delete semantics.
        const mention_writes: []types.GraphEdgeWrite = if (source.mention_edge_type.len > 0)
            try runtimeMentionEdgeWritesAlloc(runtime, graph_entry.config.name, request.doc_key, value, source.mention_edge_type, artifact_name)
        else
            &.{};
        defer runtimeFreeGraphWrites(runtime.alloc, mention_writes);

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
        for (mention_writes) |write| {
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
            const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(runtime.alloc, request.doc_key, graph_entry.config.name);
            defer runtime.alloc.free(prefix);
            const existing = try backend_scan.scanPrefix(runtime.alloc, &runtime.store, prefix);
            defer backend_scan.freeResults(runtime.alloc, existing);
            for (existing) |entry| {
                if (runtimeContainsKVKey(writes.items, entry.key)) continue;
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
            const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(runtime.alloc, request.doc_key, graph_entry.config.name);
            defer runtime.alloc.free(prefix);
            const existing = try backend_scan.scanPrefix(runtime.alloc, &runtime.store, prefix);
            defer backend_scan.freeResults(runtime.alloc, existing);
            for (existing) |entry| {
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

/// Build `doc -> entity` mention edges (provenance) for the extracted mentions
/// in `extraction_raw`. The target is the canonical entity key the resolver that
/// consumes `artifact_name` would render via its `key_template` -- deterministic
/// from the mention text, so it works even before the entity is promoted (the
/// entity node hydrates only once the entity document exists; missing entities
/// fail closed at query time). Returns an empty slice if no resolver consumes
/// the artifact.
fn runtimeMentionEdgeWritesAlloc(
    runtime: *EnrichmentRuntime,
    index_name: []const u8,
    doc_key: []const u8,
    extraction_raw: []const u8,
    mention_edge_type: []const u8,
    artifact_name: []const u8,
) ![]types.GraphEdgeWrite {
    var resolver_cfg: ?*const index_manager_mod.ResolverConfig = null;
    for (runtime.index_manager.resolvers.items) |*cfg| {
        if (std.mem.eql(u8, cfg.source_artifact, artifact_name)) {
            resolver_cfg = cfg;
            break;
        }
    }
    const cfg = resolver_cfg orelse return &.{};

    var resolver = try resolver_lib.Resolver.initFromParts(
        runtime.alloc,
        cfg.table,
        cfg.key_template,
        cfg.type_must_match,
        cfg.scorer_json,
    );
    defer resolver.deinit();

    var parsed = resolver_lib.parseExtractionEntities(runtime.alloc, extraction_raw) catch return &.{};
    defer parsed.deinit();

    var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    errdefer runtimeFreeGraphWrites(runtime.alloc, writes.items);
    for (parsed.entities) |entity| {
        const key = resolver.renderKeyAlloc(runtime.alloc, entity) catch continue;
        defer runtime.alloc.free(@constCast(key));
        if (key.len == 0) continue;
        // One mention edge per distinct entity even if mentioned repeatedly.
        var duplicate = false;
        for (writes.items) |existing| {
            if (std.mem.eql(u8, existing.target, key)) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;
        const metadata = try std.fmt.allocPrint(runtime.alloc, "{{\"target_table\":{f}}}", .{std.json.fmt(cfg.table, .{})});
        errdefer runtime.alloc.free(metadata);
        try writes.append(runtime.alloc, .{
            .index_name = try runtime.alloc.dupe(u8, index_name),
            .source = try runtime.alloc.dupe(u8, doc_key),
            .target = try runtime.alloc.dupe(u8, key),
            .edge_type = try runtime.alloc.dupe(u8, mention_edge_type),
            .weight = 1.0,
            .metadata_json = metadata,
        });
    }
    return try writes.toOwnedSlice(runtime.alloc);
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
    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
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

        try flushPlainDenseItems(runtime, dense_embedder, embedding_artifact_name, seed.expected_dims, consumer_indexes, items.items, window);
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

            const chunks = try getOrCreateRequestChunks(runtime, request, chunk_cache);
            for (chunks) |chunk| {
                const source = chunk.text orelse continue;
                const chunk_key = try internal_keys.chunkArtifactKeyAlloc(runtime.alloc, request.doc_key, chunk_artifact_name, @intCast(chunk.chunk_id));
                errdefer runtime.alloc.free(chunk_key);
                const source_hash = enrichment_artifact_codec.hashSource(source);
                const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, chunk_key, embedding_artifact_name);
                defer runtime.alloc.free(embedding_key);
                if (try shouldSkipEmbeddingArtifact(runtime, embedding_key, source_hash)) {
                    if (generatedArtifactAlreadyPublished(runtime, embedding_key)) {
                        runtime.alloc.free(chunk_key);
                        continue;
                    }
                    try cached_embeddings.append(runtime.alloc, .{
                        .index_name = try runtime.alloc.dupe(u8, seed.index_name),
                        .parent_doc_key = try runtime.alloc.dupe(u8, request.doc_key),
                        .doc_key = chunk_key,
                        .artifact_key = try runtime.alloc.dupe(u8, embedding_key),
                        .vector = &.{},
                    });
                    continue;
                }
                try chunk_texts.append(runtime.alloc, source);
                try chunk_items.append(runtime.alloc, .{
                    .request = request,
                    .parent_doc_key = request.doc_key,
                    .source_field = request.source_field,
                    .artifact_name = embedding_artifact_name,
                    .chunk_key = chunk_key,
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

        const vectors = try embedDenseBatchWithRetry(dense_embedder, runtime, embedding_artifact_name, chunk_texts.items, seed.expected_dims);
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
    const include_default_full_text = try chunking_types_mod.parseHasFullTextIndexFromSlice(runtime.alloc, request.chunker_json);
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

    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
    const raw = storeGetAlloc(runtime, doc_store_key) catch |err| switch (err) {
        std.mem.Allocator.Error.OutOfMemory => return err,
        else => return,
    };
    defer runtime.alloc.free(raw);

    if (request.source_template.len > 0 and dense_embedder.supportsParts()) {
        const source_parts = renderSourceParts(runtime.alloc, runtime.config, raw, request) catch null;
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

    const doc_store_key = try internal_keys.documentKeyAlloc(runtime.alloc, request.doc_key);
    defer runtime.alloc.free(doc_store_key);
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
    const chunks = try getOrCreateRequestChunks(runtime, request, chunk_cache);
    if (chunks.len == 0) return try runtime.alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, 0);

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

    for (chunks) |chunk| {
        const source = chunk.text orelse continue;
        const chunk_key = try internal_keys.chunkArtifactKeyAlloc(runtime.alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id));
        const source_hash = enrichment_artifact_codec.hashSource(source);
        const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, chunk_key, requestEmbeddingName(request));
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
        try chunk_texts.append(runtime.alloc, source);
        try chunk_keys.append(runtime.alloc, chunk_key);
    }

    if (chunk_texts.items.len == 0) return try embeddings.toOwnedSlice(runtime.alloc);

    const vectors = try embedDenseBatchWithRetry(dense_embedder, runtime, requestEmbeddingName(request), chunk_texts.items, request.expected_dims);
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
    const chunks = try getOrCreateRequestChunks(runtime, request, chunk_cache);
    if (chunks.len == 0) return try runtime.alloc.alloc(derived_types.DerivedSparseEmbeddingWrite, 0);

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

    for (chunks) |chunk| {
        const source = chunk.text orelse continue;
        const chunk_key = try internal_keys.chunkArtifactKeyAlloc(runtime.alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id));
        const source_hash = enrichment_artifact_codec.hashSource(source);
        const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(runtime.alloc, chunk_key, requestEmbeddingName(request));
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
        try chunk_texts.append(runtime.alloc, source);
        try chunk_keys.append(runtime.alloc, chunk_key);
        try chunk_hashes.append(runtime.alloc, source_hash);
    }

    if (chunk_texts.items.len == 0) return try embeddings.toOwnedSlice(runtime.alloc);

    const sparse_batch = try embedSparseBatchWithRetry(sparse_embedder, runtime, requestEmbeddingName(request), chunk_texts.items);
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

fn graphAssetStateKeyAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8, artifact_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.graph_asset_state_kind);
    try internal_keys.appendEncodedComponent(&list, alloc, index_name);
    try internal_keys.appendEncodedComponent(&list, alloc, artifact_name);
    return try list.toOwnedSlice(alloc);
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

fn chunkKeysForDenseRequest(
    runtime: *EnrichmentRuntime,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_name: []const u8,
    chunk_cache: *std.ArrayListUnmanaged(WorkerChunkCacheEntry),
) ![][]u8 {
    const chunks = try getOrCreateRequestChunks(runtime, request, chunk_cache);
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

fn storePut(runtime: *EnrichmentRuntime, key: []const u8, value: []const u8) !void {
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, value);
    try txn.commit();
}

fn storePutBatch(runtime: *EnrichmentRuntime, writes: []const KVPair, deletes: []const []const u8) !void {
    var batch = try runtime.store.beginBatch();
    errdefer batch.abort();
    for (writes) |write| try batch.put(write.key, write.value);
    for (deletes) |key| {
        batch.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
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
        const rendered = template_remote.renderJsonToTextWithConfig(
            alloc,
            request.source_template,
            raw_doc,
            remoteRenderConfig(config.secret_store, config.remote_content),
        ) catch return null;
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

fn extractAssetSourceValue(
    alloc: Allocator,
    config: Config,
    raw_doc: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
) !?[]const u8 {
    if (request.source_template.len > 0) {
        const rendered = template_remote.renderJsonToTextWithConfig(
            alloc,
            request.source_template,
            raw_doc,
            remoteRenderConfig(config.secret_store, config.remote_content),
        ) catch return null;
        if (rendered.len == 0) {
            alloc.free(rendered);
            return null;
        }
        return rendered;
    }

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_doc, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const source = parsed.value.object.get(request.source_field) orelse return null;
    return switch (source) {
        .null => null,
        .string => |value| try alloc.dupe(u8, value),
        else => try std.json.Stringify.valueAlloc(alloc, source, .{}),
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
        template_remote.renderJsonToPartsWithConfig(alloc, request.source_template, raw_doc, remoteRenderConfig(config.secret_store, config.remote_content)) catch return null
    else
        template_remote.renderJsonToParts(alloc, request.source_template, raw_doc) catch return null;
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
