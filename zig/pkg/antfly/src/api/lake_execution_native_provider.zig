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

//! Complete physical attempts execute inside the serverless runtime owner.
//! Reopen resources here; never execute API-owned object-store/scanner vtables.
const std = @import("std");
const local = @import("antfly_local_sources");
const c = @import("lake_execution_native_contract.zig");
const Store = @import("lake_index_store.zig").Store;
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
// Only the independently compiled qualification archive supplies this hook.
// Production roots compile these inline helpers away entirely.
inline fn probeNow() u64 {
    if (comptime @hasDecl(@import("root"), "lakeExecutionProbeStage")) return @import("antfly_platform").time.monotonicNs();
    return 0;
}
inline fn probeStage(comptime name: []const u8, started: u64) void {
    if (comptime @hasDecl(@import("root"), "lakeExecutionProbeStage")) @import("root").lakeExecutionProbeStage(name, probeNow() - started);
}

const Callbacks = struct {
    host: c.Control,
    lock: std.atomic.Mutex = .unlocked,
    first_failure: c.Failure = .{},
    fn unwrap(self: *@This(), comptime T: type, result: c.Outcome(T)) !T {
        if (result.failure.ptr != null) {
            while (!self.lock.tryLock()) std.atomic.spinLoopHint();
            if (self.first_failure.ptr == null) self.first_failure = result.failure;
            self.lock.unlock();
            return error.Canceled;
        }
        return result.value;
    }
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.unwrap(void, self.host.check(self.host.ptr));
    }
    fn now(raw: *const anyopaque) !u64 {
        const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
        return self.unwrap(u64, self.host.now(self.host.ptr));
    }
    fn snapshot(raw: *anyopaque, a: A) !local.common_topology_records.TableRecord {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.unwrap(local.common_topology_records.TableRecord, self.host.snapshot(self.host.ptr, a));
    }
    fn save(raw: *anyopaque, publication: local.metadata_lake_index_catalog.Publication) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.unwrap(void, self.host.save_checkpoint(self.host.ptr, publication));
    }
    fn discard(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.unwrap(void, self.host.discard_checkpoint(self.host.ptr));
    }
    fn failure(self: *@This(), err: anyerror) c.Failure {
        while (!self.lock.tryLock()) std.atomic.spinLoopHint();
        defer self.lock.unlock();
        if (err == error.Canceled and self.first_failure.ptr != null) return self.first_failure;
        return .fromError(err);
    }
};
fn attempt(request: c.Request, callbacks: *Callbacks, io: std.Io, transport_io: ?std.Io, cause: *c.Failure) ![]u8 {
    const a = request.allocator;
    const context: Context = .{ .io = io, .checkpoint = .{ .ptr = callbacks, .check = Callbacks.check } };
    try context.ensureActive();
    try request.cancellation.check();
    const store_started = probeNow();
    var store = if (request.store.config) |config|
        try Store.openRetainedNativeWithTransport(a, config, request.store.secrets, request.store.locator, false, request.store.deployment, request.store.local_base_dir, .{ .io = transport_io, .requests = request.source.requests })
    else
        try Store.openNativeWithTransport(a, null, request.store.secrets, false, request.store.deployment, request.store.local_base_dir, .{ .io = transport_io, .requests = request.source.requests });
    defer store.deinit();
    probeStage("store_open_ns", store_started);
    if (store.locator.protocol != request.store.locator.protocol or store.locator.tls != request.store.locator.tls or
        !std.mem.eql(u8, store.locator.root, request.store.locator.root) or !std.mem.eql(u8, store.locator.bucket, request.store.locator.bucket) or
        !std.mem.eql(u8, store.locator.prefix, request.store.locator.prefix)) return error.NativeArtifactStoreLocationChanged;
    if (!std.mem.eql(u8, &store.identity, &request.store.identity)) return error.ExternalLakeIndexStoreChanged;
    store.implementation.cloud_vacuum = request.store.cloud_vacuum;
    var source_options = request.source;
    source_options.read_only = true;
    source_options.io = transport_io;
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, request.table.schema_json)) orelse return error.InvalidExternalTableBinding;
    defer binding.deinit(a);
    // Each writer family revisits source metadata and ranges. Keep reuse in
    // this operation's owner, charged to its existing working-set quota. No
    // persistent cache or borrowed caller I/O survives this synchronous call.
    var read_cache = local.serverless_query_lake_serving_cache.Cache.initWithMemoryLimit(a, @min(64 * 1024 * 1024, request.build_limits.max_working_set_bytes / 8));
    defer read_cache.deinit();
    defer if (comptime @hasDecl(@import("root"), "lakeExecutionProbeCache")) @import("root").lakeExecutionProbeCache(read_cache.snapshot(), &read_cache);
    const source_started = probeNow();
    var source = try local.serverless_query_lake_serving.ServingSource.openCached(a, .{ .storage_mode = .relational, .external_base_source = binding }, source_options.lakeOptions(), context, &read_cache);
    defer source.deinit();
    try source.attachCache(&read_cache, binding.binding, context);
    probeStage("source_open_ns", source_started);
    const Embedder = local.inference_managed_embedder;
    var runtime = Embedder.ProviderRuntime.init(a, io);
    defer runtime.deinit();
    if (request.provider_limits) |registry| runtime.limits = registry;
    var embedding_options = request.embedding_options;
    if (embedding_options) |*options| {
        options.io = io;
        options.provider_runtime = &runtime;
    }
    var artifact_reads = @import("lake_index_artifact_recovery.zig").Reads.init(store.artifactStore());
    artifact_reads.enableWriteCache(a, 8 * 1024 * 1024);
    defer artifact_reads.close();
    var handle = artifact_reads.store();
    const physical_started = probeNow();
    defer probeStage("physical_ns", physical_started);
    return @import("lake_execution_build_impl.zig").buildWithReuse(a, &handle, request.table, &source, store.identity, context, request.cancellation, .{ .ptr = callbacks, .now_ms = Callbacks.now }, .{ .ptr = callbacks, .snapshot = Callbacks.snapshot, .save_checkpoint = Callbacks.save, .discard_checkpoint = Callbacks.discard }, embedding_options, .{ .store = &store, .table_id = request.table.table_id, .recipe = local.metadata_lake_index_catalog.desiredFingerprint(request.table), .context = context }, request.policy) catch |err| {
        if (artifact_reads.cause()) |artifact_error| cause.* = .fromError(artifact_error);
        return err;
    };
}
pub fn execute(request: c.Request) c.Response {
    var callbacks: Callbacks = .{ .host = request.control };
    var backing = c.memory.BorrowedAllocator.init(request.allocator, request.allocator_headroom) catch |err| return .{ .failure = .fromError(err) };
    var receiver = request.io.receive() catch |err| return .{ .failure = .fromError(err) };
    var transport_receiver = if (request.transport_io) |borrow| borrow.receive() catch |err| return .{ .failure = .fromError(err) } else null;
    var cause: c.Failure = .{};
    // Headroom/admission inspects allocator identity. Keep the quota in this
    // owner instead of asking physical writers to interpret a foreign quota.
    var working = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(backing.allocator(), request.build_limits) catch |err| return .{ .failure = .fromError(err) };
    defer working.deinit();
    defer if (comptime @hasDecl(@import("root"), "lakeExecutionProbeQuota")) @import("root").lakeExecutionProbeQuota(working.quota.peak_bytes);
    var local_request = request;
    local_request.allocator = working.allocator();
    const bytes = attempt(local_request, &callbacks, receiver.io(), if (transport_receiver) |*value| value.io() else null, &cause) catch |err| {
        const failure = callbacks.failure(err);
        // A control callback may allocate from this owner's quota. Its error
        // token remains opaque, so classify that actual OOM by name as well.
        const quota_oom = working.limitExceeded() and failure.ptr != null and std.mem.eql(u8, failure.ptr.?[0..failure.len], "OutOfMemory");
        return .{ .failure = if (quota_oom) .fromError(error.LakeSidecarBuildLimitExceeded) else failure, .artifact_cause = cause };
    };
    defer working.allocator().free(bytes);
    // No allocator wrapper or physical-resource lifetime escapes the attempt.
    const owned = request.allocator.dupe(u8, bytes) catch |err| return .{ .failure = .fromError(err) };
    return .{ .bytes = owned };
}
fn invoke(signature: *const c.native.CallContract, raw: *const anyopaque, out_raw: *anyopaque) callconv(.c) void {
    const out: *c.Response = @ptrCast(@alignCast(out_raw));
    if (!signature.matches(c.signature())) {
        out.* = .{ .failure = .fromError(error.InvalidArgument) };
        return;
    }
    const request: *const c.Request = @ptrCast(@alignCast(raw));
    out.* = execute(request.*);
}
const endpoint: c.Endpoint = .{ .contract = c.signature(), .invoke = invoke };
fn get() callconv(.c) *const c.Endpoint {
    return &endpoint;
}
pub fn exportOperations() void {
    @export(&get, .{ .name = "antfly_lake_execute_v4", .visibility = .hidden });
}
