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
const a = std.heap.wasm_allocator;
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
    @import("antfly_platform").time.installHostClocks(.{ .monotonic_ns = monotonicNs, .realtime_ns = realtimeNs, .authority_ns = authorityNs });
    const Request = struct {
        op: enum { search, mutate, publish, lookup, list },
        bucket: []const u8,
        prefix: []const u8,
        namespace: []const u8,
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
    if (req.cursor != null and req.op != .list) return error.QueryCursorCapabilityUnsupported;
    var host = objects.wasm_host.Host{ .alloc = a };
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
    if (req.op == .mutate) {
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
        for (req.mutations, payloads) |mutation, *payload| payload.* = try @import("serverless/api/codec.zig").encodeMutationAlloc(arena.allocator(), mutation);
        const result = try wal.appendBatchIdempotent(req.namespace, try std.fmt.parseInt(u64, req.timestamp_ns, 10), payloads, req.operation_id orelse return error.InvalidWalOperationId);
        return std.json.Stringify.valueAlloc(a, .{ .visibility = @import("serverless/visibility.zig").Token.init(req.namespace, result.end_lsn).encode(), .committed = true }, .{});
    }
    if (req.op == .publish) {
        var builder = @import("serverless/build/builder.zig").Builder.init(a, &artifacts, &manifests, &progress, &wal);
        builder.setIo(host_io);
        builder.graph_metric_max_parallelism = 1;
        builder.max_wal_records = 256;
        builder.max_wal_bytes = 4 * 1024 * 1024;
        var result = try builder.publishNamespaceWithMetricAndPlanWithCancellation(req.namespace, .cosine, .{ .targets = .{
            .published_search_sources = @import("serverless/search_sources.zig").defaultPublishedSearchSources(),
            .include_graph = false,
        } }, cancellation);
        defer result.deinit(a);
        const version = try std.fmt.allocPrint(a, "{d}", .{result.version});
        defer a.free(version);
        const watermark = try std.fmt.allocPrint(a, "{d}", .{result.wal_end_lsn});
        defer a.free(watermark);
        return std.json.Stringify.valueAlloc(a, .{ .published = result.published, .version = version, .wal_end_lsn = watermark, .has_more = (try wal.latestLsn(req.namespace)) > result.wal_end_lsn }, .{});
    }
    var runtime = @import("serverless/query/runtime.zig").QueryRuntime.init(a, &artifacts, &manifests, &progress);
    defer runtime.deinit();
    if (req.op == .lookup or req.op == .list or (req.op == .search and (req.fresh or req.required_visibility != null))) {
        const visibility = @import("serverless/visibility.zig");
        var query_buffer: [80]u8 = undefined;
        const query_identity = try std.fmt.bufPrint(&query_buffer, "list:doc_id:ascending:limit={d}", .{req.limit});
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
            const hits = try live.searchTextAlloc(.{ .text = req.text, .limit = req.limit }, 1024 * 1024);
            defer @import("serverless/query/request.zig").freeHits(a, hits);
            return std.json.Stringify.valueAlloc(a, .{ .hits = hits, .visibility = live.view.token.encode() }, .{});
        }
        if (req.op == .lookup) {
            const body = try live.lookupAlloc(req.doc_id);
            defer if (body) |value| a.free(value);
            return std.json.Stringify.valueAlloc(a, .{ .body = body, .visibility = live.view.token.encode() }, .{});
        }
        const AcceptAll = struct {
            pub fn matches(_: @This(), _: @import("serverless/query/live_overlay.zig").Overlay.Document) !bool {
                return true;
            }
        };
        var page = try live.list(if (cursor) |value| value.cursor.after else "", req.limit, 1024 * 1024, AcceptAll{}, .{ .cancellation = transport_cancellation });
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
