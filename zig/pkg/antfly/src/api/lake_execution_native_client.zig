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

//! API-owned catalog/lease callbacks for one synchronous physical attempt.
const std = @import("std");
const local = @import("antfly_local_sources");
const c = @import("lake_execution_native_contract.zig");
const Context = local.serverless_query_lake_read_context.Context;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Callbacks = struct {
    context: Context,
    cancellation: Cancellation,
    clock: c.values.Clock,
    lease: c.values.Lease,
    fn check(raw: *anyopaque) c.Outcome(void) {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.context.ensureActive() catch |err| return .{ .failure = .fromError(err) };
        self.cancellation.check() catch |err| return .{ .failure = .fromError(err) };
        return .{ .value = {} };
    }
    fn now(raw: *anyopaque) c.Outcome(u64) {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return .{ .value = self.clock.now_ms(self.clock.ptr) catch |err| return .{ .failure = .fromError(err) } };
    }
    fn snapshot(raw: *anyopaque, a: A) c.Outcome(local.common_topology_records.TableRecord) {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return .{ .value = self.lease.snapshot(self.lease.ptr, a) catch |err| return .{ .failure = .fromError(err) } };
    }
    fn save(raw: *anyopaque, publication: local.metadata_lake_index_catalog.Publication) c.Outcome(void) {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const callback = self.lease.save_checkpoint orelse return .{ .failure = .fromError(error.InvalidArgument) };
        callback(self.lease.ptr, publication) catch |err| return .{ .failure = .fromError(err) };
        return .{ .value = {} };
    }
    fn discard(raw: *anyopaque) c.Outcome(void) {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const callback = self.lease.discard_checkpoint orelse return .{ .failure = .fromError(error.InvalidArgument) };
        callback(self.lease.ptr) catch |err| return .{ .failure = .fromError(err) };
        return .{ .value = {} };
    }
};
/// The returned operation owns no borrowed handles after this call returns.
/// Publication authority remains with the caller throughout construction.
pub fn build(a: A, io: std.Io, table: local.common_topology_records.TableRecord, source_options: c.SourceOptions, store: c.values.StoreDescriptor, context: Context, cancellation: Cancellation, clock: c.values.Clock, lease: c.values.Lease, embedding_options: ?local.inference_managed_embedder.InitOptions, policy: c.values.BuildPolicy, build_limits: c.BuildLimits, artifact_cause: *?anyerror) ![]u8 {
    var callbacks: Callbacks = .{ .context = context, .cancellation = cancellation, .clock = clock, .lease = lease };
    var request: c.Request = .{ .allocator = a, .table = table, .source = source_options, .store = store, .io = c.native.IoBorrow.init(&io), .cancellation = cancellation, .control = .{ .ptr = &callbacks, .check = Callbacks.check, .now = Callbacks.now, .snapshot = Callbacks.snapshot, .save_checkpoint = Callbacks.save, .discard_checkpoint = Callbacks.discard }, .embedding_options = embedding_options, .policy = policy, .build_limits = build_limits };
    // Clients/caches embed the creator's executor and cannot cross error domains.
    // Only the checked AntflyProvider and secret-store capabilities are borrowed.
    if (request.embedding_options) |*options| {
        if (options.provider_runtime) |runtime| request.provider_limits = runtime.limits;
        options.provider_runtime = null;
        options.io = null;
        options.remote_capability_cache = null;
        if (options.antfly_provider) |*provider| provider.remote_capability_cache = null;
    }
    const response = try c.execute(request);
    artifact_cause.* = response.artifact_cause.decode();
    if (response.failure.decode()) |err| return err;
    return response.bytes orelse error.InvalidArgument;
}
