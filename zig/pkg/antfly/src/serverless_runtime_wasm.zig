// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

const std = @import("std");
const objects = @import("objectstore");
const wasm_allocator = std.heap.wasm_allocator;
var working_set_peak: usize = 0;
pub fn resetWorkingSetMetrics() void {
    working_set_peak = 0;
}
export fn antfly_host_working_set_peak_bytes() usize {
    return working_set_peak;
}
pub const panic = std.debug.FullPanic(hostPanic);
fn hostPanic(message: []const u8, _: ?usize) noreturn {
    log(message.ptr, message.len);
    @trap();
}
pub const std_options: std.Options = .{ .logFn = hostLog };
extern "antfly_host" fn log(ptr: [*]const u8, len: usize) void;
fn hostLog(comptime _: std.log.Level, comptime _: @EnumLiteral(), comptime format: []const u8, args: anytype) void {
    var buffer: [1024]u8 = undefined;
    const message = std.fmt.bufPrint(&buffer, format, args) catch "Antfly runtime diagnostic exceeded buffer";
    log(message.ptr, message.len);
}
pub const antfly_sources = @import("antfly_local_sources").hosted_sources;
comptime {
    _ = @import("serverless_host_wasm.zig");
}
extern "antfly_host" fn now_ns(kind: u32) u64;
fn monotonicNs() u64 {
    return now_ns(0);
}
fn realtimeNs() u64 {
    return now_ns(1);
}
fn authorityNs() u64 {
    return now_ns(2);
}
extern "env" fn antfly_platform_random_secure(ptr: [*]u8, len: usize) u32;
fn hostNow(_: ?*anyopaque, clock: std.Io.Clock) std.Io.Timestamp {
    return .{ .nanoseconds = if (clock == .real) realtimeNs() else monotonicNs() };
}
fn hostRandom(_: ?*anyopaque, buffer: []u8) void {
    if (antfly_platform_random_secure(buffer.ptr, buffer.len) != 0) @panic("secure host entropy unavailable");
}
fn hostRandomSecure(_: ?*anyopaque, buffer: []u8) std.Io.RandomSecureError!void {
    if (antfly_platform_random_secure(buffer.ptr, buffer.len) != 0) return error.EntropyUnavailable;
}
extern "antfly_host" fn check_cancel() u32;
var cancellation_context: u8 = 0;
fn hostCanceled(_: *const anyopaque) bool {
    return check_cancel() != 0;
}
const cancellation: @import("antfly_cancellation").CancellationToken = .{ .ptr = &cancellation_context, .is_cancelled_fn = hostCanceled };
const transport_cancellation: objects.CancellationToken = .{ .ptr = &cancellation_context, .is_cancelled_fn = hostCanceled };
var cancel_protection: std.Io.CancelProtection = .unblocked;
fn hostCheckCancel(_: ?*anyopaque) std.Io.Cancelable!void {
    if (cancel_protection == .unblocked and check_cancel() != 0) return error.Canceled;
}
fn hostSwapCancelProtection(_: ?*anyopaque, next: std.Io.CancelProtection) std.Io.CancelProtection {
    const previous = cancel_protection;
    cancel_protection = next;
    return previous;
}
const host_io_vtable: std.Io.VTable = blk: {
    var table = std.Io.failing.vtable.*;
    table.now = hostNow;
    table.checkCancel = hostCheckCancel;
    table.swapCancelProtection = hostSwapCancelProtection;
    table.random = hostRandom;
    table.randomSecure = hostRandomSecure;
    break :blk table;
};
const host_io: std.Io = .{ .userdata = null, .vtable = &host_io_vtable };
pub fn requestAdditional(raw: []const u8) ![]u8 {
    const envelope = try std.json.parseFromSlice(struct { working_set_bytes: usize = 20 * 1024 * 1024 }, wasm_allocator, raw, .{ .ignore_unknown_fields = true });
    defer envelope.deinit();
    const maximum = envelope.value.working_set_bytes;
    if (maximum < 4096 or maximum > 20 * 1024 * 1024) return error.InvalidHostedWorkingSetBudget;
    var working = try @import("serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(wasm_allocator, .{ .max_working_set_bytes = maximum });
    working_set_peak = 0;
    defer working_set_peak = working.peak_live_bytes;
    const result = executeAdditional(working.allocator(), raw) catch |err| {
        if (err == error.OutOfMemory and working.limit_exceeded) return error.HostedWorkingSetBudgetExceeded;
        return err;
    };
    defer working.allocator().free(result);
    // The ABI response outlives this request's allocator wrapper.
    return wasm_allocator.dupe(u8, result);
}
fn executeAdditional(a: std.mem.Allocator, raw: []const u8) ![]u8 {
    @import("antfly_platform").time.installHostClocks(.{ .monotonic_ns = monotonicNs, .realtime_ns = realtimeNs, .authority_ns = authorityNs });
    const Request = struct {
        op: enum { search, mutate, publish, compact, lookup, list, count, status, enrich, gc_prepare, gc_wal, gc_trace, reindex, coordinate },
        coordinate: ?@import("serverless/coordinator_store.zig").Request = null,
        bucket: []const u8,
        prefix: []const u8,
        namespace: []const u8,
        reindex_action: @import("serverless/hosted_reindex.zig").Action = .start,
        reindex_job_id: ?[]const u8 = null,
        reindex_records: usize = 16,
        gc_task: ?@import("serverless/hosted_collection.zig").Task = null,
        gc_id: ?[]const u8 = null,
        gc_keep_from: []const u8 = "1",
        publication_records: usize = 16,
        paged_text: bool = false,
        paged_projections: bool = false,
        compaction_max_documents: u64 = 1000,
        compaction_max_artifact_bytes: u64 = 4 * 1024 * 1024,
        mode: @import("serverless/query/request.zig").QueryMode = .text,
        vector: ?[]f32 = null,
        sparse: ?[]@import("serverless/query/request.zig").SparseTermWeight = null,
        num_probes: u32 = 2,
        search_effort: ?f32 = null,
        enrichment: bool = false,
        embedding_dimensions: u32 = 384,
        embedding_identity: []const u8 = "cloudflare-default",
        embedding_checkpoints: bool = false,
        embedding_windows: usize = 1,
        enrichment_documents: usize = 1,
        working_set_bytes: usize = 20 * 1024 * 1024,
        filters: []const @import("serverless/query/live_filter.zig").Clause = &.{},
        fresh: bool = false,
        required_visibility: ?[]const u8 = null,
        cursor: ?[]const u8 = null,
        doc_id: []const u8 = "",
        text: []u8 = &.{},
        limit: usize = 10,
        mutations: []const @import("serverless/api/types.zig").DocumentMutation = &.{},
        operation_id: ?[]const u8 = null,
        timestamp_ns: []const u8 = "0",
    };
    const parsed = try std.json.parseFromSlice(Request, a, raw, .{});
    defer parsed.deinit();
    const req = parsed.value;
    if (req.limit > 100 or (req.limit == 0 and (req.op == .list or req.op == .search))) return error.QueryCandidateBudgetExceeded;
    const filter = @import("serverless/query/live_filter.zig").Filter{ .alloc = a, .clauses = req.filters };
    try filter.validate();
    if (req.filters.len != 0 and req.op != .list and req.op != .search) return error.FilterCapabilityUnsupported;
    if (req.cursor != null and req.op != .list and !(req.op == .search and req.mode == .text)) return error.QueryCursorCapabilityUnsupported;
    var host = objects.wasm_host.Host{ .alloc = a, .transfer_allocator = wasm_allocator };
    if (req.op == .coordinate) return (@import("serverless/coordinator_store.zig").Context{ .alloc = a, .client = host.client(), .bucket = req.bucket, .prefix = req.prefix, .namespace = req.namespace, .io = host_io, .cancellation = objects.CancellationToken.fromCallback(cancellation.ptr, cancellation.is_cancelled_fn) }).execute(req.coordinate orelse return error.CoordinatorRequestRequired);
    const artifact_prefix = try std.fmt.allocPrint(a, "{s}/artifacts", .{req.prefix});
    defer a.free(artifact_prefix);
    var object_artifacts = try @import("serverless/artifacts/object_store.zig").ObjectStore.initWithClient(a, host.client(), req.bucket, artifact_prefix);
    var artifacts = object_artifacts.artifactStore();
    defer artifacts.deinit();
    const manifest_prefix = try std.fmt.allocPrint(a, "{s}/manifests", .{req.prefix});
    defer a.free(manifest_prefix);
    var object_manifests = try @import("serverless/manifest/object_store.zig").ObjectStore.initWithClient(a, host.client(), req.bucket, manifest_prefix);
    var manifests = object_manifests.manifestStore();
    defer manifests.deinit();
    const progress_prefix = try std.fmt.allocPrint(a, "{s}/progress", .{req.prefix});
    defer a.free(progress_prefix);
    var object_progress = try @import("serverless/catalog/object_progress_store.zig").ObjectProgressStore.initWithClient(a, host.client(), req.bucket, progress_prefix);
    var progress = object_progress.progressStore();
    defer progress.deinit();
    var indexed_wal = try @import("serverless/wal/indexed_object_store.zig").IndexedObjectStore.init(a, host.client(), req.bucket, req.prefix);
    defer indexed_wal.deinit();
    var wal = indexed_wal.walStore();
    const reindex_registry = @import("serverless/hosted_reindex.zig").Registry{ .client = host.client(), .bucket = req.bucket, .prefix = req.prefix, .namespace = req.namespace, .store = &artifacts };
    if (req.op == .reindex) return (@import("serverless/hosted_reindex.zig").Context{ .alloc = a, .registry = reindex_registry, .manifests = &manifests, .progress = &progress, .wal = &wal, .io = host_io, .cancellation = .{ .io = host_io, .cooperative = cancellation } }).run(req.reindex_action, req.reindex_job_id, req.reindex_records);
    const collection = @import("serverless/hosted_collection.zig").Context{ .alloc = a, .namespace = req.namespace, .prefix = req.prefix, .artifacts = &artifacts, .manifests = &manifests, .progress = &progress, .wal = &indexed_wal, .io = host_io, .reindex_registry = reindex_registry };
    switch (req.op) {
        .gc_prepare => return collection.prepare(),
        .gc_trace => return collection.trace(req.gc_task orelse return error.InvalidCollectionTask),
        .gc_wal => {
            var id: [16]u8 = undefined;
            const text = req.gc_id orelse return error.InvalidCollectionId;
            if (text.len != 32) return error.InvalidCollectionId;
            _ = std.fmt.hexToBytes(&id, text) catch return error.InvalidCollectionId;
            return collection.prepareWal(try std.fmt.parseInt(u64, req.gc_keep_from, 10), id);
        },
        else => {},
    }
    if (req.op == .mutate) {
        const published = progress.getHead(req.namespace) catch |err| switch (err) {
            error.FileNotFound => 0,
            else => return err,
        };
        var published_lsn: u64 = 0;
        if (published != 0) {
            var manifest = try manifests.getAlloc(req.namespace, published);
            defer manifest.deinit(a);
            published_lsn = manifest.wal_end_lsn;
        }
        indexed_wal.max_new_lsn = published_lsn +| 4096;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        for (req.mutations) |mutation| {
            if (mutation.doc_id.len == 0 or mutation.doc_id.len > 256 * 1024) return error.InvalidDocumentMutation;
            switch (mutation.kind) {
                .delete => if (mutation.body != null) return error.InvalidDocumentMutation,
                .upsert => {
                    const body = mutation.body orelse return error.InvalidDocumentMutation;
                    const document = try std.json.parseFromSlice(std.json.Value, arena.allocator(), body, .{});
                    defer document.deinit();
                    if (document.value != .object) return error.InvalidDocumentMutation;
                },
            }
        }
        const payloads = try arena.allocator().alloc([]const u8, req.mutations.len);
        for (req.mutations, payloads) |mutation, *payload| {
            var normalized = mutation;
            if (mutation.kind == .upsert) {
                var document = try std.json.parseFromSlice(std.json.Value, arena.allocator(), mutation.body.?, .{});
                defer document.deinit();
                var changed = false;
                for ([_][]const u8{ "_enrichment", "chunk_embeddings", "chunk_preview" }) |field| changed = document.value.object.swapRemove(field) or changed;
                // Public writes cannot carry a stale derived readiness marker
                // forward after changing content. Internal enrichment appends
                // use the native conditional WAL path directly.
                if (changed) normalized.body = try std.json.Stringify.valueAlloc(arena.allocator(), document.value, .{});
            }
            payload.* = try @import("serverless/api/codec.zig").encodeMutationAlloc(arena.allocator(), normalized);
        }
        const result = try wal.appendBatchIdempotent(req.namespace, try std.fmt.parseInt(u64, req.timestamp_ns, 10), payloads, req.operation_id orelse return error.InvalidWalOperationId);
        return std.json.Stringify.valueAlloc(a, .{ .visibility = @import("serverless/visibility.zig").Token.init(req.namespace, result.end_lsn).encode(), .committed = true }, .{});
    }
    if (req.op == .enrich) {
        if (!req.enrichment or req.embedding_dimensions == 0 or req.embedding_dimensions > 1536) return error.HostedEnrichmentNotConfigured;
        var enricher = @import("serverless/enrichment/worker.zig").SparseEnricher.init(a, &artifacts, &manifests, &progress, &wal);
        defer enricher.deinit();
        if (req.embedding_windows == 0 or req.embedding_windows > 8 or req.embedding_identity.len > 1024) return error.InvalidEnrichmentBudget;
        var context = @import("serverless/hosted_embedding.zig").Context{
            .client = host.client(),
            .bucket = req.bucket,
            .prefix = req.prefix,
            .namespace = req.namespace,
            .identity = req.embedding_identity,
            .day = @intCast(realtimeNs() / (std.time.ns_per_day)),
            .enabled = req.embedding_checkpoints,
            .max_provider_calls = if (req.embedding_checkpoints) req.embedding_windows else 8,
            .cancellation = transport_cancellation,
        };
        try enricher.setChunkEmbedder(.{ .ptr = &context, .dense_embed_fn = @import("serverless/hosted_embedding.zig").Dense.embed, .dense_embed_batch_fn = @import("serverless/hosted_embedding.zig").Dense.batch }, "cloudflare", req.embedding_dimensions);
        if (req.enrichment_documents == 0 or req.enrichment_documents > 4) return error.InvalidEnrichmentBudget;
        const stats = enricher.runNamespaceWithConfigUntil(req.namespace, .{
            .batch_size = if (req.embedding_checkpoints) 1 else req.enrichment_documents,
            .scan_batch_size = 32,
            .max_source_read_bytes = 4 * 1024 * 1024,
            .stage = .chunk_embeddings,
            .model_preference = .require_model,
            .failure_policy = .fail_stage,
            .cancellation = cancellation,
        }, .{ .io = host_io, .cooperative = cancellation }) catch |err| switch (err) {
            error.HostedEmbeddingYield => return std.json.Stringify.valueAlloc(a, .{ .yielded = true, .wal_appends = @as(usize, 0), .checkpoint_hits = context.hits, .checkpoint_writes = context.writes, .provider_calls = context.provider_calls }, .{}),
            else => return err,
        };
        return std.json.Stringify.valueAlloc(a, .{ .yielded = false, .wal_appends = stats.wal_appends, .enriched_documents = stats.enriched_documents, .enriched_namespaces = stats.enriched_namespaces, .model_documents = stats.model_documents, .fallback_documents = stats.fallback_documents, .failed_documents = stats.failed_documents, .stage_failures = stats.stage_failures, .idle_namespaces = stats.idle_namespaces, .checkpoint_hits = context.hits, .checkpoint_writes = context.writes, .provider_calls = context.provider_calls }, .{});
    }
    if (req.op == .compact) {
        _ = progress.getHead(req.namespace) catch |err| switch (err) {
            error.FileNotFound => return std.json.Stringify.valueAlloc(a, .{ .published = false, .version = "0", .artifact_count = @as(usize, 0) }, .{}),
            else => return err,
        };
        var compactor = @import("serverless/build/compactor.zig").Compactor.init(a, &artifacts, &manifests, &progress);
        compactor.max_documents = req.compaction_max_documents;
        compactor.max_artifact_bytes = req.compaction_max_artifact_bytes;
        var result = try compactor.compactHeadGuardedUntil(req.namespace, null, .{ .io = host_io, .cooperative = cancellation });
        defer result.deinit(a);
        var version: [20]u8 = undefined;
        return std.json.Stringify.valueAlloc(a, .{ .published = result.published, .version = try std.fmt.bufPrint(&version, "{d}", .{result.version}), .artifact_count = result.artifact_count }, .{});
    }
    if (req.op == .publish) {
        var builder = @import("serverless/build/builder.zig").Builder.init(a, &artifacts, &manifests, &progress, &wal);
        builder.setIo(host_io);
        builder.paged_text = req.paged_text;
        builder.paged_projections = req.paged_projections;
        builder.graph_metric_max_parallelism = 1;
        if (req.publication_records == 0 or req.publication_records > 256) return error.InvalidWalBatch;
        builder.max_wal_records = req.publication_records;
        builder.max_wal_bytes = 4 * 1024 * 1024;
        var result = try builder.publishNamespaceWithMetricAndPlanWithCancellation(req.namespace, .cosine, .{ .targets = .{
            .published_search_sources = @import("serverless/search_sources.zig").defaultPublishedSearchSources(),
            .include_graph = false,
        }, .policy = .{ .chunk_embeddings_enabled = req.enrichment, .chunk_embeddings_model_preference = .require_model } }, cancellation);
        defer result.deinit(a);
        const version = try std.fmt.allocPrint(a, "{d}", .{result.version});
        defer a.free(version);
        const watermark = try std.fmt.allocPrint(a, "{d}", .{result.wal_end_lsn});
        defer a.free(watermark);
        return std.json.Stringify.valueAlloc(a, .{ .published = result.published, .version = version, .wal_end_lsn = watermark, .has_more = (try wal.latestLsn(req.namespace)) > result.wal_end_lsn }, .{});
    }
    var runtime = @import("serverless/query/runtime.zig").QueryRuntime.init(a, &artifacts, &manifests, &progress);
    defer runtime.deinit();
    if (req.op == .status) {
        const latest = try wal.latestLsn(req.namespace);
        var session = runtime.openHeadSessionWithCancellation(req.namespace, cancellation) catch |err| switch (err) {
            error.FileNotFound => return std.json.Stringify.valueAlloc(a, .{ .published = false, .visibility = @import("serverless/visibility.zig").Token.init(req.namespace, latest).encode() }, .{}),
            else => return err,
        };
        defer session.deinit();
        const published_lsn = try std.fmt.allocPrint(a, "{d}", .{session.manifest.wal_end_lsn});
        defer a.free(published_lsn);
        const pending = try std.fmt.allocPrint(a, "{d}", .{try pendingEmbeddings(a, &session)});
        defer a.free(pending);
        return std.json.Stringify.valueAlloc(a, .{ .published = true, .published_lsn = published_lsn, .pending_embeddings = pending, .visibility = @import("serverless/visibility.zig").Token.init(req.namespace, latest).encode() }, .{});
    }
    if (req.op == .search and req.mode != .text) {
        if (req.filters.len != 0) return error.FreshQueryCapabilityUnsupported;
        var session = try runtime.openHeadSessionWithCancellation(req.namespace, cancellation);
        defer session.deinit();
        const visibility = @import("serverless/visibility.zig");
        if (req.required_visibility) |text| {
            const required = try visibility.Token.decode(text);
            try required.require(req.namespace, session.manifest.wal_end_lsn);
        }
        if (req.fresh and try wal.latestLsn(req.namespace) != session.manifest.wal_end_lsn) return error.IndexPublicationPending;
        const pending = try pendingEmbeddings(a, &session);
        if ((req.fresh or req.required_visibility != null) and pending != 0) return error.EmbeddingPending;
        const hits = try @import("serverless/query/indexed_reader.zig").searchAlloc(a, &session, .{ .text = req.text, .vector = req.vector, .sparse = req.sparse, .mode = req.mode, .limit = req.limit, .num_probes = req.num_probes, .search_effort = req.search_effort });
        defer @import("serverless/query/request.zig").freeHits(a, hits);
        return std.json.Stringify.valueAlloc(a, .{ .hits = hits, .visibility = visibility.Token.init(req.namespace, session.manifest.wal_end_lsn).encode(), .embeddings_ready = pending == 0 }, .{});
    }
    if (req.op == .lookup or req.op == .list or req.op == .count or (req.op == .search and (req.fresh or req.required_visibility != null or req.filters.len != 0 or req.cursor != null))) {
        const visibility = @import("serverless/visibility.zig");
        const query_identity = try std.json.Stringify.valueAlloc(a, .{ .op = req.op, .limit = req.limit, .filters = req.filters, .text = req.text }, .{});
        defer a.free(query_identity);
        const query_hash = visibility.Token.init(query_identity, 0).scope;
        const cursor = if (req.cursor) |text| try visibility.Cursor.decode(text, a, req.namespace, query_hash) else null;
        defer if (cursor) |value| a.free(value.raw_storage);
        const required = if (req.required_visibility) |text| try visibility.Token.decode(text) else null;
        const live = try @import("serverless/query/live_session.zig").Session.open(a, &runtime, &wal, req.namespace, .{
            .io = host_io,
            .cancellation = cancellation,
            .version = if (cursor) |value| value.cursor.generation else null,
            .view = .{ .required = required, .cutoff = if (cursor) |value| value.cursor.cutoff else null, .cancellation = transport_cancellation },
        });
        defer live.deinit();
        if (req.op == .search) {
            const after: ?@import("serverless/query/live_session.zig").Session.RankPosition = if (cursor) |value| .{ .score = value.cursor.rank_score orelse return error.InvalidQueryCursor, .id = value.cursor.after } else null;
            const hits = if (req.filters.len == 0 and after == null) try live.searchTextAlloc(.{ .text = req.text, .limit = req.limit }, 1024 * 1024) else try live.searchTextPageAlloc(.{ .text = req.text, .limit = req.limit }, 1024 * 1024, filter, after);
            defer @import("serverless/query/request.zig").freeHits(a, hits);
            const next_cursor = if (hits.len == req.limit) try (visibility.Cursor{ .scope = live.view.token.scope, .query = query_hash, .generation = live.generation(), .cutoff = live.view.token.lsn, .after = hits[hits.len - 1].doc_id, .rank_score = hits[hits.len - 1].score }).encode(a) else null;
            defer if (next_cursor) |value| a.free(value);
            return std.json.Stringify.valueAlloc(a, .{ .hits = hits, .cursor = next_cursor, .visibility = live.view.token.encode() }, .{});
        }
        if (req.op == .count) {
            const count = try std.fmt.allocPrint(a, "{d}", .{try live.countDocuments()});
            defer a.free(count);
            return std.json.Stringify.valueAlloc(a, .{ .count = count, .visibility = live.view.token.encode() }, .{});
        }
        if (req.op == .lookup) {
            const body = try live.lookupAlloc(req.doc_id);
            defer if (body) |value| a.free(value);
            return std.json.Stringify.valueAlloc(a, .{ .body = body, .visibility = live.view.token.encode() }, .{});
        }
        var page = try live.list(if (cursor) |value| value.cursor.after else "", req.limit, 1024 * 1024, filter, .{ .cancellation = transport_cancellation });
        defer page.deinit(a);
        const next_cursor = if (page.documents.len == req.limit) try (visibility.Cursor{
            .scope = live.view.token.scope,
            .query = query_hash,
            .generation = live.generation(),
            .cutoff = live.view.token.lsn,
            .after = page.documents[page.documents.len - 1].doc_id,
        }).encode(a) else null;
        defer if (next_cursor) |value| a.free(value);
        return std.json.Stringify.valueAlloc(a, .{ .documents = page.documents, .cursor = next_cursor, .visibility = live.view.token.encode() }, .{});
    }
    var session = try runtime.openHeadSessionWithCancellation(req.namespace, cancellation);
    defer session.deinit();
    const reader = @import("serverless/query/indexed_reader.zig");
    const hits = try reader.searchAlloc(a, &session, .{ .text = req.text, .limit = req.limit });
    defer @import("serverless/query/request.zig").freeHits(a, hits);
    return std.json.Stringify.valueAlloc(a, .{ .hits = hits }, .{});
}

fn pendingEmbeddings(a: std.mem.Allocator, session: *@import("serverless/query/runtime.zig").QuerySession) !u64 {
    var remaining: u64 = 4 * 1024 * 1024;
    const reader = (try @import("serverless/query/document_facts_reader.zig").Reader.create(a, session, &remaining)) orelse return error.HostedDocumentFactsRequired;
    defer reader.destroy();
    return reader.root.counts[5];
}
