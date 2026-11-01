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
var rejected_growth: usize = 0;
var backing_failure: usize = 0;
var decoded_manifests: @import("serverless/manifest/decoded_cache.zig").Cache = .{};
var shared_read_leases: @import("serverless/manifest/read_lease.zig").Cache = .{};
var prepared_reads: @import("serverless/query/read_plan.zig").PreparedCache = .{};
var read_states: @import("serverless/query/read_state_cache.zig").Cache = .{};
var resident_shard: @import("serverless/query/serving_shard.zig").Shard = .{};
var serving_scope: ?[32]u8 = null;
// A kernel is serialized by the host pool. These caches own no request pointers
// and must never cross a bucket/prefix boundary when a kernel is reused.
fn prepareServingScope(bucket: []const u8, prefix: []const u8) void {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var length: [8]u8 = undefined;
    for ([_][]const u8{ bucket, prefix }) |value| {
        std.mem.writeInt(u64, &length, value.len, .little);
        hasher.update(&length);
        hasher.update(value);
    }
    const digest = hasher.finalResult();
    if (serving_scope == null or !std.mem.eql(u8, &serving_scope.?, &digest)) {
        decoded_manifests.deinit(wasm_allocator);
        prepared_reads.deinit(wasm_allocator);
        resident_shard.deinit(wasm_allocator);
        read_states.deinit();
        shared_read_leases = .{};
        serving_scope = digest;
    }
}

export fn antfly_host_immutable_preparation_abi() u32 {
    return 1;
}

export fn antfly_host_serving_capsule_v1() u32 {
    return 1;
}
export fn antfly_host_read_graph_abi() u32 {
    return 1;
}
export fn antfly_host_resident_state_abi() u32 {
    return 1;
}

export fn antfly_host_resident_read_abi() u32 {
    return 1;
}
export fn antfly_host_resident_read_hits() u64 {
    return resident_shard.hits;
}
export fn antfly_host_resident_read_bytes() usize {
    return resident_shard.bytes;
}

export fn antfly_host_read_dependency_abi() u32 {
    return 1;
}

export fn antfly_host_manifest_cache_hits() u64 {
    return decoded_manifests.hits;
}
export fn antfly_host_manifest_cache_bytes() usize {
    return decoded_manifests.retainedBytes();
}
export fn antfly_host_prepared_read_cache_bytes() usize {
    return prepared_reads.bytes;
}
export fn antfly_host_prepared_read_cache_hits() u64 {
    return prepared_reads.hits;
}
export fn antfly_host_read_state_cache_bytes() usize {
    return read_states.bytes;
}
export fn antfly_host_read_state_cache_hits() u64 {
    return read_states.hits;
}
export fn antfly_host_read_state_cache_extensions() u64 {
    return read_states.extensions;
}
pub fn resetWorkingSetMetrics() void {
    working_set_peak = 0;
    rejected_growth = 0;
    backing_failure = 0;
}
export fn antfly_host_rejected_growth_bytes() usize {
    return rejected_growth;
}
export fn antfly_host_backing_failure_bytes() usize {
    return backing_failure;
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
    const envelope = try std.json.parseFromSlice(struct { working_set_bytes: usize = 20 * 1024 * 1024, op: []const u8 = "", native_read_yield: bool = false }, wasm_allocator, raw, .{ .ignore_unknown_fields = true });
    defer envelope.deinit();
    if (envelope.value.native_read_yield and !std.mem.eql(u8, envelope.value.op, "lookup")) return error.InvalidNativeReadPlan;
    const maximum = envelope.value.working_set_bytes;
    if (maximum < 4096 or maximum > 20 * 1024 * 1024) return error.InvalidHostedWorkingSetBudget;
    var working = try @import("serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(wasm_allocator, .{ .max_working_set_bytes = maximum });
    defer working.deinit();
    working_set_peak = 0;
    rejected_growth = 0;
    backing_failure = 0;
    defer {
        working_set_peak = working.peakBytes();
        rejected_growth = working.largestRejectedGrowth();
        backing_failure = working.largestBackingFailure();
    }
    const result = executeAdditional(working.allocator(), raw) catch |err| {
        if (err == error.ImmutableReadDependency and envelope.value.native_read_yield) return wasm_allocator.dupe(u8, "{\"__read_dependency\":true}");
        if (err == error.OutOfMemory and working.limitExceeded()) return error.HostedWorkingSetBudgetExceeded;
        return err;
    };
    defer working.allocator().free(result);
    // The ABI response outlives this request's allocator wrapper.
    return wasm_allocator.dupe(u8, result);
}
fn executeAdditional(a: std.mem.Allocator, raw: []const u8) ![]u8 {
    @import("antfly_platform").time.installHostClocks(.{ .monotonic_ns = monotonicNs, .realtime_ns = realtimeNs, .authority_ns = authorityNs });
    const MutationGroup = struct { mutations: []const @import("serverless/api/types.zig").DocumentMutation, operation_id: []const u8, timestamp_ns: []const u8 = "0" };
    const Request = struct {
        op: enum { search, mutate, publish, compact, lookup, list, count, status, enrich, gc_prepare, gc_wal, gc_trace, reindex, coordinate, publication_acquire, publication_validate, publication_release },
        pack_small_bodies: bool = false,
        inline_point_bodies: bool = false,
        canonical_point_bodies: bool = false,
        point_read_blocks: bool = false,
        inline_fact_leaves: bool = false,
        wal_packed_pages: bool = false,
        wal_deferred_indexes: bool = false,
        bounded_manifest_reads: bool = false,
        native_write_plan: ?@import("serverless/wal/write_plan.zig").Input = null,
        native_write_read_ahead: bool = false,
        native_read_plan_mode: bool = false,
        native_read_state: bool = false,
        wal_document_index: bool = false,
        native_read_prepare: ?[16]u8 = null,
        native_read_reuse: bool = false,
        resident_cut: ?[32]u8 = null,
        resident_read_state: bool = false,
        resident_tail: ?@import("serverless/query/live_view.zig").AdmittedTail = null,
        resident_docs: []const struct { doc_id: []const u8, required_visibility: ?[]const u8 = null } = &.{},
        native_immutable_manifest_version: ?[]const u8 = null,
        native_read_yield: bool = false,
        native_read_graph: bool = false,
        native_read_resume: ?[16]u8 = null,
        native_read_spent_bytes: u64 = 0,
        native_read_plan: ?@import("serverless/query/read_plan.zig").Plan = null,
        compaction_probe_only: bool = false,
        wal_inline_tail: bool = false,
        publication_owner: []const u8 = "",
        publication_lease: ?@import("serverless/hosted_publication.zig").Lease = null,
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
        publication_continuations: bool = false,
        publication_batch_probe: bool = false,
        publication_batch_version: ?u64 = null,
        publication_continuation_force: bool = false,
        publication_units: usize = 128,
        publication_adaptive_slices: bool = false,
        publication_max_units: usize = 4096,
        publication_prepared_slices: bool = false,
        publication_action: @import("serverless/hosted_publication_plan.zig").Action = .step,
        publication_job_id: ?[]const u8 = null,
        paged_text: bool = false,
        paged_projections: bool = false,
        compaction_max_documents: u64 = 1000,
        compaction_max_artifact_bytes: u64 = 4 * 1024 * 1024,
        mode: @import("serverless/query/request.zig").QueryMode = .text,
        vector: ?[]f32 = null,
        sparse: ?[]@import("serverless/query/request.zig").SparseTermWeight = null,
        num_probes: u32 = 2,
        coarse_probes: u32 = 0,
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
        exact_scan: bool = false,
        exact_state: ?@import("serverless/query/exact_scan.zig").State = null,
        exact_records: usize = 512,
        doc_id: []const u8 = "",
        text: []u8 = &.{},
        limit: usize = 10,
        mutations: []const @import("serverless/api/types.zig").DocumentMutation = &.{},
        mutation_groups: []const MutationGroup = &.{},
        operation_id: ?[]const u8 = null,
        timestamp_ns: []const u8 = "0",
    };
    const parsed = try std.json.parseFromSlice(Request, a, raw, .{});
    defer parsed.deinit();
    const req = parsed.value;
    if (req.native_immutable_manifest_version != null and (req.op != .lookup or !req.native_read_plan_mode or req.doc_id.len != 0 or req.resident_cut != null or req.native_read_plan != null or req.native_read_prepare != null or req.native_read_resume != null or req.native_read_spent_bytes != 0)) return error.InvalidNativeReadPlan;
    if (req.resident_docs.len > 16 or req.resident_docs.len != 0 and (!req.resident_read_state or req.resident_cut == null)) return error.InvalidResidentRead;
    if (req.resident_tail != null and (req.resident_cut == null or !req.resident_read_state)) return error.InvalidResidentRead;
    if (req.resident_cut != null and (req.op != .lookup or req.cursor != null or req.native_read_plan_mode)) return error.InvalidResidentRead;
    if (req.limit > 100 or (req.limit == 0 and (req.op == .list or req.op == .search))) return error.QueryCandidateBudgetExceeded;
    const filter = @import("serverless/query/live_filter.zig").Filter{ .alloc = a, .clauses = req.filters };
    try filter.validate();
    if (req.filters.len != 0 and req.op != .list and req.op != .search) return error.FilterCapabilityUnsupported;
    if (req.cursor != null and req.op != .list and !(req.op == .search and req.mode == .text)) return error.QueryCursorCapabilityUnsupported;
    var host = objects.wasm_host.Host{ .alloc = a, .transfer_allocator = wasm_allocator };
    if (req.op == .coordinate) return (@import("serverless/coordinator_store.zig").Context{ .alloc = a, .client = host.client(), .bucket = req.bucket, .prefix = req.prefix, .namespace = req.namespace, .io = host_io, .cancellation = objects.CancellationToken.fromCallback(cancellation.ptr, cancellation.is_cancelled_fn) }).execute(req.coordinate orelse return error.CoordinatorRequestRequired);
    switch (req.op) {
        .publication_acquire, .publication_validate, .publication_release => return @import("serverless/hosted_publication.zig").run(a, host.client(), req.bucket, req.prefix, req.namespace, switch (req.op) {
            .publication_acquire => .acquire,
            .publication_validate => .validate,
            else => .release,
        }, req.publication_owner, req.publication_lease, @intCast(realtimeNs())),
        else => {},
    }
    var write_plan: ?@import("serverless/wal/write_plan.zig").Plan = null;
    defer if (write_plan) |*value| value.deinit();
    var data_client = host.client();
    if (req.native_write_plan) |input| {
        if (req.op != .mutate) return error.InvalidNativeWritePlan;
        write_plan = try @import("serverless/wal/write_plan.zig").Plan.init(a, data_client, req.bucket, req.prefix, req.namespace, input, host_io);
        data_client = write_plan.?.client(a);
        if (req.native_write_read_ahead) {
            var indexed = try @import("serverless/wal/indexed_object_store.zig").IndexedObjectStore.init(a, data_client, req.bucket, req.prefix);
            defer indexed.deinit();
            return std.json.Stringify.valueAlloc(a, .{ .reads = try indexed.writeReadAhead(a, req.namespace) }, .{});
        }
    }
    const artifact_prefix = try std.fmt.allocPrint(a, "{s}/artifacts", .{req.prefix});
    defer a.free(artifact_prefix);
    var object_artifacts = try @import("serverless/artifacts/object_store.zig").ObjectStore.initWithClient(a, data_client, req.bucket, artifact_prefix);
    var artifacts = object_artifacts.artifactStore();
    artifacts.pack_document_bodies = req.pack_small_bodies;
    artifacts.inline_point_bodies = req.inline_point_bodies;
    artifacts.canonical_point_bodies = req.canonical_point_bodies;
    artifacts.point_read_blocks = req.point_read_blocks;
    artifacts.inline_fact_leaves = req.inline_fact_leaves;
    defer artifacts.deinit();
    const manifest_prefix = try std.fmt.allocPrint(a, "{s}/manifests", .{req.prefix});
    defer a.free(manifest_prefix);
    var object_manifests = try @import("serverless/manifest/object_store.zig").ObjectStore.initWithClient(a, data_client, req.bucket, manifest_prefix);
    var manifests = object_manifests.manifestStore();
    defer manifests.deinit();
    // Authority-free publication preparation reads only immutable bytes. It
    // cannot create a live view, pin, visibility result or reusable ticket.
    if (req.native_immutable_manifest_version) |text| {
        if (text.len == 0 or text.len > 20 or text[0] == '0') return error.InvalidNativeReadPlan;
        for (text) |c| if (c < '0' or c > '9') return error.InvalidNativeReadPlan;
        const version = std.fmt.parseInt(u64, text, 10) catch return error.InvalidNativeReadPlan;
        var manifest = try manifests.getAllocWithCancellation(req.namespace, version, cancellation);
        defer manifest.deinit(a);
        if (manifest.version != version) return error.InvalidNativeReadPlan;
        const plan = @import("serverless/query/read_plan.zig").Plan.fromManifest(manifest) orelse return std.json.Stringify.valueAlloc(a, .{ .__read_headers_prepared = false, .__read_spent_bytes = 0 }, .{});
        var remaining: u64 = 512 * 1024;
        var no_writes: u64 = 0;
        var pages = @import("serverless/graph_segment/page_store.zig").PageStore{ .domain = @import("serverless/graph_segment/page_store.zig").PageStore.namespaceDomain(req.namespace), .artifacts = &artifacts, .cancellation = cancellation, .remaining_read_bytes = &remaining, .remaining_write_bytes = &no_writes };
        var prepared = try plan.prepare(a, req.namespace, "", &pages);
        defer prepared.deinit(a);
        try cancellation.check();
        return std.json.Stringify.valueAlloc(a, .{ .__read_headers_prepared = true, .__read_spent_bytes = 512 * 1024 - remaining }, .{});
    }
    const progress_prefix = try std.fmt.allocPrint(a, "{s}/progress", .{req.prefix});
    defer a.free(progress_prefix);
    var object_progress = try @import("serverless/catalog/object_progress_store.zig").ObjectProgressStore.initWithClient(a, data_client, req.bucket, progress_prefix);
    var progress = object_progress.progressStore();
    defer progress.deinit();
    var indexed_wal = try @import("serverless/wal/indexed_object_store.zig").IndexedObjectStore.init(a, data_client, req.bucket, req.prefix);
    defer indexed_wal.deinit();
    indexed_wal.inline_tail_enabled = req.wal_inline_tail;
    indexed_wal.read_state_enabled = req.native_read_state;
    indexed_wal.document_index_enabled = req.wal_document_index;
    indexed_wal.packed_pages_enabled = req.wal_packed_pages;
    indexed_wal.deferred_indexes_enabled = req.wal_deferred_indexes;
    var wal = indexed_wal.walStore();
    const reindex_registry = @import("serverless/hosted_reindex.zig").Registry{ .client = host.client(), .bucket = req.bucket, .prefix = req.prefix, .namespace = req.namespace, .store = &artifacts };
    if (req.op == .reindex) return (@import("serverless/hosted_reindex.zig").Context{ .alloc = a, .registry = reindex_registry, .manifests = &manifests, .progress = &progress, .wal = &wal, .io = host_io, .cancellation = .{ .io = host_io, .cooperative = cancellation } }).run(req.reindex_action, req.reindex_job_id, req.reindex_records);
    const publication_registry = @import("serverless/hosted_publication_plan.zig").Registry{ .client = host.client(), .bucket = req.bucket, .prefix = req.prefix, .namespace = req.namespace, .store = &artifacts };
    const collection = @import("serverless/hosted_collection.zig").Context{ .alloc = a, .namespace = req.namespace, .prefix = req.prefix, .artifacts = &artifacts, .manifests = &manifests, .progress = &progress, .wal = &indexed_wal, .io = host_io, .reindex_registry = reindex_registry, .publication_registry = publication_registry };
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
        const groups = if (req.mutation_groups.len != 0) req.mutation_groups else &[_]MutationGroup{.{ .mutations = req.mutations, .operation_id = req.operation_id orelse return error.InvalidWalOperationId, .timestamp_ns = req.timestamp_ns }};
        if (groups.len > 16) return error.InvalidWalBatch;
        const native_groups = try arena.allocator().alloc(@import("serverless/wal/indexed_object_store.zig").IndexedObjectStore.Group, groups.len);
        for (groups, native_groups) |group, *native_group| {
            for (group.mutations) |mutation| {
                try @import("serverless/api/service.zig").validatePublicMutation(arena.allocator(), mutation);
                if (mutation.doc_id.len == 0 or mutation.doc_id.len > 256 * 1024) return error.InvalidDocumentMutation;
                switch (mutation.kind) {
                    .checkpoint, .enriched => return error.InvalidDocumentMutation,
                    .delete => if (mutation.body != null) return error.InvalidDocumentMutation,
                    .upsert => {
                        const body = mutation.body orelse return error.InvalidDocumentMutation;
                        const document = try std.json.parseFromSlice(std.json.Value, arena.allocator(), body, .{});
                        defer document.deinit();
                        if (document.value != .object) return error.InvalidDocumentMutation;
                    },
                }
            }
            const payloads = try arena.allocator().alloc([]const u8, group.mutations.len);
            for (group.mutations, payloads) |mutation, *payload| {
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
            native_group.* = .{ .timestamp = try std.fmt.parseInt(u64, group.timestamp_ns, 10), .payloads = payloads, .id = group.operation_id };
        }
        const results = try indexed_wal.appendGroupsIdempotent(req.namespace, native_groups);
        defer a.free(results);
        const Result = struct {
            visibility: @TypeOf(@import("serverless/visibility.zig").Token.init("", 0).encode()),
            committed: bool = true,
            tail: ?@import("serverless/query/live_view.zig").AdmittedTail = null,
            pub fn jsonStringify(self: @This(), writer: anytype) !void {
                if (self.tail) |tail| return writer.write(.{ .visibility = self.visibility, .committed = self.committed, .__resident_tail = tail });
                return writer.write(.{ .visibility = self.visibility, .committed = self.committed });
            }
        };
        const output = try arena.allocator().alloc(Result, results.len);
        for (results, output) |result, *item| item.* = .{ .visibility = @import("serverless/visibility.zig").Token.init(req.namespace, result.end_lsn).encode() };
        if (req.resident_read_state) for (results, native_groups, output) |result, group, *item| {
            var bytes: usize = 0;
            for (group.payloads) |payload| bytes = std.math.add(usize, bytes, payload.len) catch 32769;
            if (bytes <= 32768 and group.payloads.len <= 128 and result.end_lsn >= result.start_lsn and result.end_lsn - result.start_lsn + 1 == group.payloads.len) {
                const mutations = try arena.allocator().alloc(@import("serverless/api/types.zig").DocumentMutation, group.payloads.len);
                for (group.payloads, mutations) |payload, *mutation| mutation.* = try @import("serverless/api/codec.zig").decodeMutationAlloc(arena.allocator(), payload);
                item.tail = .{ .scope = @import("serverless/visibility.zig").Token.init(req.namespace, 0).scope, .start_lsn = try std.fmt.allocPrint(arena.allocator(), "{d}", .{result.start_lsn}), .end_lsn = try std.fmt.allocPrint(arena.allocator(), "{d}", .{result.end_lsn}), .mutations = mutations };
            }
        };
        if (write_plan) |*plan| {
            var maximum: u64 = 0;
            for (results) |result| maximum = @max(maximum, result.end_lsn);
            const watermark = try std.fmt.allocPrint(arena.allocator(), "{d}", .{maximum});
            const commit = try plan.finish(arena.allocator(), watermark, @intCast(realtimeNs() / std.time.ns_per_ms));
            return std.json.Stringify.valueAlloc(a, .{ .results = output, .__write_plan = commit }, .{});
        }
        return if (req.mutation_groups.len == 0) std.json.Stringify.valueAlloc(a, output[0], .{}) else std.json.Stringify.valueAlloc(a, .{ .results = output }, .{});
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
        const work = @import("serverless/build/work_lease.zig");
        var held: ?work.HeldLease = if (req.publication_lease) |lease| .{
            .provider = try progress.workLeaseProvider(),
            .io = host_io,
            .namespace = req.namespace,
            .owner_id = lease.owner,
            .acquisition = .{ .fencing_token = try std.fmt.parseInt(u64, lease.token, 10), .expires_at_unix_ns = try std.fmt.parseInt(u64, lease.expires, 10), .took_over = false },
            .ttl_ns = 30 * std.time.ns_per_s,
        } else null;
        _ = progress.getHead(req.namespace) catch |err| switch (err) {
            error.FileNotFound => return std.json.Stringify.valueAlloc(a, .{ .published = false, .version = "0", .artifact_count = @as(usize, 0) }, .{}),
            else => return err,
        };
        var compactor = @import("serverless/build/compactor.zig").Compactor.init(a, &artifacts, &manifests, &progress);
        compactor.probe_only = req.compaction_probe_only;
        compactor.max_documents = req.compaction_max_documents;
        compactor.max_artifact_bytes = req.compaction_max_artifact_bytes;
        var result = try compactor.compactHeadGuardedUntil(req.namespace, if (held) |*lease| lease.guard() else null, if (held) |*lease| lease.cancellation(.{ .io = host_io, .cooperative = cancellation }) else .{ .io = host_io, .cooperative = cancellation });
        defer result.deinit(a);
        var version: [20]u8 = undefined;
        return std.json.Stringify.valueAlloc(a, .{ .published = result.published, .version = try std.fmt.bufPrint(&version, "{d}", .{result.version}), .artifact_count = result.artifact_count }, .{});
    }
    if (req.op == .publish) {
        if (req.publication_batch_version) |expected| {
            const current = progress.getHead(req.namespace) catch |err| switch (err) {
                error.FileNotFound => 0,
                else => return err,
            };
            if (current != expected) return error.PublicationBatchSourceChanged;
        }
        const work = @import("serverless/build/work_lease.zig");
        var held: ?work.HeldLease = if (req.publication_lease) |lease| .{
            .provider = try progress.workLeaseProvider(),
            .io = host_io,
            .namespace = req.namespace,
            .owner_id = lease.owner,
            .acquisition = .{ .fencing_token = try std.fmt.parseInt(u64, lease.token, 10), .expires_at_unix_ns = try std.fmt.parseInt(u64, lease.expires, 10), .took_over = false },
            .ttl_ns = 30 * std.time.ns_per_s,
        } else null;
        if (req.publication_continuations) {
            if (try (@import("serverless/hosted_publication_plan.zig").Context{ .alloc = a, .registry = publication_registry, .manifests = &manifests, .progress = &progress, .wal = &wal, .io = host_io, .cancellation = .{ .io = host_io, .cooperative = cancellation }, .policy = .{ .chunk_embeddings_enabled = req.enrichment, .chunk_embeddings_model_preference = .require_model }, .force = req.publication_continuation_force, .adaptive_slices = req.publication_adaptive_slices, .max_posting_units = req.publication_max_units, .prepare_slices = req.publication_prepared_slices, .deferred_wal_maintenance = req.wal_deferred_indexes, .prepared_lease = if (held) |*value| value else null, .batch_probe_records = if (req.publication_batch_probe) @min(64, @max(1, req.publication_records)) else null }).run(req.publication_action, req.publication_job_id, req.publication_units)) |result| return result;
            if (req.publication_batch_probe) return error.PublicationNeedsPreparedBatch;
        }
        var builder = @import("serverless/build/builder.zig").Builder.init(a, &artifacts, &manifests, &progress, &wal);
        builder.deferred_wal_maintenance = req.wal_deferred_indexes;
        builder.setIo(host_io);
        builder.paged_text = req.paged_text;
        builder.paged_projections = req.paged_projections;
        builder.graph_metric_max_parallelism = 1;
        if (req.publication_records == 0 or req.publication_records > 256) return error.InvalidWalBatch;
        builder.max_wal_records = req.publication_records;
        builder.max_wal_bytes = 4 * 1024 * 1024;
        var result = try builder.publishNamespaceWithMetricAndPlanGuardedUntil(req.namespace, .cosine, .{ .targets = .{
            .published_search_sources = @import("serverless/search_sources.zig").defaultPublishedSearchSources(),
            .include_graph = false,
        }, .policy = .{ .chunk_embeddings_enabled = req.enrichment, .chunk_embeddings_model_preference = .require_model } }, if (held) |*lease| lease.guard() else null, if (held) |*lease| lease.cancellation(.{ .io = host_io, .cooperative = cancellation }) else .{ .io = host_io, .cooperative = cancellation });
        defer result.deinit(a);
        const version = try std.fmt.allocPrint(a, "{d}", .{result.version});
        defer a.free(version);
        const watermark = try std.fmt.allocPrint(a, "{d}", .{result.wal_end_lsn});
        defer a.free(watermark);
        return std.json.Stringify.valueAlloc(a, .{ .published = result.published, .version = version, .wal_end_lsn = watermark, .consumed_records = result.consumed_records, .has_more = result.maintenance_pending or (try wal.latestLsn(req.namespace)) > result.wal_end_lsn }, .{});
    }
    var runtime = @import("serverless/query/runtime.zig").QueryRuntime.init(a, &artifacts, &manifests, &progress);
    defer runtime.deinit();
    prepareServingScope(req.bucket, req.prefix);
    if (!req.resident_read_state) if (req.resident_cut) |cut| {
        if (req.op != .lookup or req.cursor != null or req.native_read_plan_mode) return error.InvalidResidentRead;
        const required = if (req.required_visibility) |text| try @import("serverless/visibility.zig").Token.decode(text) else null;
        if (try resident_shard.get(a, req.namespace, cut, req.doc_id, required)) |value| {
            defer value.deinit(a);
            try cancellation.check();
            return std.json.Stringify.valueAlloc(a, .{ .body = value.body, .visibility = @import("serverless/visibility.zig").Token.init(req.namespace, value.lsn).encode() }, .{});
        }
    };
    runtime.shared_read_leases = &shared_read_leases;
    runtime.bounded_manifest_reads = req.bounded_manifest_reads;
    runtime.decoded_manifests = &decoded_manifests;
    runtime.decoded_manifest_allocator = wasm_allocator;
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
        if (req.exact_scan and (req.mode != .vector or req.search_effort == null or req.search_effort.? != 1)) return error.ExactScanCapabilityUnsupported;
        if (!req.exact_scan and req.exact_state != null) return error.InvalidExactScanState;
        const exact = @import("serverless/query/exact_scan.zig");
        var session = if (req.exact_scan and req.exact_state != null)
            try runtime.openVersionSessionWithCancellation(req.namespace, try exact.generation(req.exact_state.?), cancellation)
        else
            try runtime.openHeadSessionWithCancellation(req.namespace, cancellation);
        defer session.deinit();
        const visibility = @import("serverless/visibility.zig");
        if (req.required_visibility) |text| {
            const required = try visibility.Token.decode(text);
            try required.require(req.namespace, session.manifest.wal_end_lsn);
        }
        if (req.fresh and try wal.latestLsn(req.namespace) != session.manifest.wal_end_lsn) return error.IndexPublicationPending;
        const pending = try pendingEmbeddings(a, &session);
        if ((req.fresh or req.required_visibility != null) and pending != 0) return error.EmbeddingPending;
        if (req.exact_scan) {
            if (req.exact_records == 0 or req.exact_records > 512) return error.QueryCandidateBudgetExceeded;
            var step = try exact.scan(a, &session, req.vector orelse return error.VectorQueryRequired, req.limit, req.exact_state, req.exact_records);
            defer step.deinit(a);
            const hits = if (step.complete) try exact.materialize(a, &session, step.state.top) else try a.alloc(@import("serverless/query/request.zig").SearchHit, 0);
            defer @import("serverless/query/request.zig").freeHits(a, hits);
            try session.checkCancellation();
            return std.json.Stringify.valueAlloc(a, .{ .hits = hits, .complete = step.complete, .exact_state = if (step.complete) null else step.state, .step_records = step.step_records, .page_reads = step.page_reads, .scan_read_bytes = step.read_bytes, .pending_records = step.state.pending.len, .total_refined_records = step.state.refined_records, .total_pruned_records = step.state.pruned_records, .refined_records = step.refined_records, .pruned_records = step.pruned_records, .residual_fetches = step.residual_fetches, .body_refinements = step.body_refinements, .scanned = step.state.scanned, .generation = step.state.generation, .visibility = visibility.Token.init(req.namespace, session.manifest.wal_end_lsn).encode(), .embeddings_ready = pending == 0 }, .{});
        }
        const hits = try @import("serverless/query/indexed_reader.zig").searchAlloc(a, &session, .{ .text = req.text, .vector = req.vector, .sparse = req.sparse, .mode = req.mode, .limit = req.limit, .num_probes = req.num_probes, .coarse_probes = req.coarse_probes, .search_effort = req.search_effort });
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
        const read_plan = @import("serverless/query/read_plan.zig");
        var prepared_read: ?read_plan.Prepared = null;
        var prepared_allocator = a;
        defer if (prepared_read) |*value| value.deinit(prepared_allocator);
        const max_read_bytes: u64 = 16 * 1024 * 1024;
        if (req.native_read_spent_bytes > max_read_bytes) return error.InvalidNativeReadPlan;
        var read_allowance: u64 = max_read_bytes - req.native_read_spent_bytes;
        if (req.native_read_plan_mode) {
            if (req.op != .lookup or req.cursor != null) return error.InvalidNativeReadPlan;
            if (req.native_read_prepare != null and req.native_read_resume != null) return error.InvalidNativeReadPlan;
            if (req.native_read_resume) |ticket| {
                if (req.native_read_plan != null) return error.InvalidNativeReadPlan;
                prepared_read = prepared_reads.take(req.namespace, req.doc_id, ticket);
                prepared_allocator = wasm_allocator;
            } else if (req.native_read_plan) |plan| {
                var no_writes: u64 = 0;
                var pages = @import("serverless/graph_segment/page_store.zig").PageStore{
                    .domain = @import("serverless/graph_segment/page_store.zig").PageStore.namespaceDomain(req.namespace),
                    .artifacts = &artifacts,
                    .cancellation = cancellation,
                    .remaining_read_bytes = &read_allowance,
                    .remaining_write_bytes = &no_writes,
                };
                prepared_read = plan.prepareCached(a, req.namespace, req.doc_id, &pages, if (req.native_read_reuse) &prepared_reads else null) catch |err| switch (err) {
                    error.FileNotFound => null, // stale immutable hint, never authority
                    else => return err,
                };
            }
            if (req.native_read_reuse) if (prepared_read) |value| {
                _ = try prepared_reads.remember(wasm_allocator, req.namespace, value);
            };
            if (req.native_read_prepare) |ticket| {
                const retained = if (prepared_read) |value| try prepared_reads.put(wasm_allocator, req.namespace, ticket, value) else false;
                return std.json.Stringify.valueAlloc(a, .{ .__read_prepared = retained, .__read_spent_bytes = max_read_bytes - read_allowance }, .{});
            }
            if (req.native_read_resume == null) try host.readViewBarrier();
        } else if (req.native_read_plan != null or req.native_read_prepare != null or req.native_read_resume != null or req.native_read_spent_bytes != 0) return error.InvalidNativeReadPlan;
        const live = try @import("serverless/query/live_session.zig").Session.open(a, &runtime, &wal, req.namespace, .{
            .io = host_io,
            .cancellation = cancellation,
            .version = if (cursor) |value| value.cursor.generation else null,
            .max_read_bytes = read_allowance,
            .lazy_facts = req.op == .lookup,
            .resident_shard = if (req.resident_read_state) &resident_shard else null,
            .resident_allocator = if (req.resident_read_state) wasm_allocator else null,
            .view = .{ .admitted_tail = req.resident_tail, .required = required, .cutoff = if (cursor) |value| value.cursor.cutoff else null, .cancellation = transport_cancellation, .state_cache = if (req.native_read_state) &read_states else null, .cache_allocator = wasm_allocator, .point_id = if (req.op == .lookup and req.wal_document_index and req.resident_docs.len == 0) req.doc_id else null },
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
            if (req.resident_docs.len != 0) {
                const Item = struct {
                    body: ?[]u8 = null,
                    visibility: @TypeOf(live.view.token.encode()),
                    error_code: ?[]const u8 = null,
                    individual: bool = false,
                    pub fn jsonStringify(self: @This(), writer: anytype) !void {
                        if (self.error_code) |code| return writer.write(.{ .@"error" = code });
                        if (self.individual) return writer.write(.{ .__individual = true });
                        return writer.write(.{ .body = self.body, .visibility = self.visibility });
                    }
                };
                const items = try a.alloc(Item, req.resident_docs.len);
                defer {
                    for (items) |item| if (item.body) |body| a.free(body);
                    a.free(items);
                }
                for (items) |*item| item.* = .{ .visibility = live.view.token.encode() };
                var needs_data = false;
                for (req.resident_docs, items) |doc, *item| {
                    try cancellation.check();
                    if (doc.required_visibility) |encoded| {
                        const required_doc = @import("serverless/visibility.zig").Token.decode(encoded) catch |err| {
                            item.error_code = @errorName(err);
                            continue;
                        };
                        required_doc.require(req.namespace, live.view.token.lsn) catch |err| {
                            item.error_code = @errorName(err);
                            continue;
                        };
                    }
                    const body = live.lookupAlloc(doc.doc_id) catch |err| {
                        if (err == error.ImmutableReadDependency and req.native_read_yield and req.native_read_graph) {
                            needs_data = true;
                            continue;
                        }
                        return err;
                    };
                    if (body != null and body.?.len > 16 * 1024) {
                        a.free(body.?);
                        item.individual = true;
                    } else item.body = body;
                }
                if (needs_data) return error.ImmutableReadDependency;
                return std.json.Stringify.valueAlloc(a, .{ .results = items }, .{});
            }
            const body = if (prepared_read) |prepared| try prepared.lookupAlloc(live, req.doc_id) else try live.lookupAlloc(req.doc_id);
            defer if (body) |value| a.free(value);
            if (!req.resident_read_state) if (req.resident_cut) |cut| {
                _ = try resident_shard.put(wasm_allocator, req.namespace, cut, req.doc_id, body, live.view.token.lsn);
            };
            if (req.native_read_plan_mode) return std.json.Stringify.valueAlloc(a, .{ .body = body, .visibility = live.view.token.encode(), .__read_plan = read_plan.Plan.fromSession(live) }, .{});
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
