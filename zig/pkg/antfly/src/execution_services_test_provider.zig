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

//! Independently compiled producer for the native execution-services contract.
const std = @import("std");
const local = @import("antfly_local_sources");
const services = local.storage_execution_services;
const background = local.storage_background_runtime;
const runtime = @import("antfly_runtime_abi");
const memory = runtime.memory_abi;
const status = runtime.error_abi;
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
const Methods = @typeInfo(@FieldType(services.Offer, "methods")).optional.child;
const Table = @typeInfo(Methods).pointer.child;
const Node = struct {
    allocator: memory.Allocator,
    resources: local.storage_resource_manager.ResourceManager,
    runtime: background.BackendRuntimeHandle,
    methods: Table,
    pressure: usize = 0,
};
const Dispatch = @typeInfo(@FieldType(services.Offer, "dispatch")).optional.child;
var dispatch: std.atomic.Value(?Dispatch) = .init(null);
var jobs_callback: std.atomic.Value(?@FieldType(Table, "jobs")) = .init(null);
var jobs_dispatch: std.atomic.Value(?Dispatch) = .init(null);
var invocation_count: std.atomic.Value(usize) = .init(0);
fn forward(contract: *const runtime.native_abi.CallContract, method: *const anyopaque, arguments: *const anyopaque, output: *anyopaque) callconv(.c) status.Status {
    _ = invocation_count.fetchAdd(1, .monotonic);
    return dispatch.load(.acquire).?(contract, method, arguments, output);
}
fn forwardJobs(contract: *const runtime.native_abi.CallContract, method: *const anyopaque, arguments: *const anyopaque, output: *anyopaque) callconv(.c) status.Status {
    return jobs_dispatch.load(.acquire).?(contract, method, arguments, output);
}
fn jobs(scope: *anyopaque) !background.DurableJobLane {
    var lane = try jobs_callback.load(.acquire).?(scope);
    jobs_dispatch.store(lane.callback_dispatch, .release);
    lane.callback_dispatch = forwardJobs;
    return lane;
}
export fn antfly_execution_fixture_create(allocator: *const memory.Allocator, out: *services.Offer, context: *?*anyopaque) callconv(.c) status.Status {
    if (!allocator.valid()) return status.statusFromError(error.UnsupportedVersion);
    const node = allocator.asStd().create(Node) catch return status.statusFromError(error.OutOfMemory);
    node.allocator = allocator.*;
    node.pressure = 0;
    node.resources = .init(.{ .memory_budget = .{ .hard_limit_bytes = 2048 << 20 } });
    node.runtime = background.BackendRuntimeHandle.init(node.allocator.asStd(), .{
        .memory_parent = node.resources.memoryParentPort(),
        .shared_sql_memory_parent = node.resources.memoryParentPort(),
        .lane_limits = .{ .worker_capacity = 2 },
    }) catch |err| {
        node.resources.deinit(node.allocator.asStd());
        allocator.asStd().destroy(node);
        return status.statusFromError(err);
    };
    var offer = services.Offer.init(node.runtime.ptr());
    dispatch.store(offer.dispatch, .release);
    node.methods = offer.methods.?.*;
    jobs_callback.store(node.methods.jobs, .release);
    node.methods.jobs = jobs;
    offer.methods = &node.methods;
    offer.dispatch = forward;
    out.* = offer;
    context.* = node;
    return .ok;
}
export fn antfly_execution_fixture_pressure(raw: *anyopaque, acquire: bool) callconv(.c) u64 {
    const node: *Node = @ptrCast(@alignCast(raw));
    if (acquire) {
        std.debug.assert(node.pressure == 0);
        node.pressure = (2048 << 20) - node.resources.snapshot().memory.used_bytes;
        std.debug.assert(node.resources.reserveExternal(node.pressure));
    } else {
        node.resources.releaseExternal(node.pressure);
        node.pressure = 0;
    }
    return node.resources.snapshot().memory.used_bytes;
}
export fn antfly_execution_fixture_destroy(raw: *anyopaque) callconv(.c) void {
    const node: *Node = @ptrCast(@alignCast(raw));
    const allocator = node.allocator;
    if (node.pressure != 0) node.resources.releaseExternal(node.pressure);
    node.runtime.deinit();
    std.debug.assert(node.resources.snapshot().memory.used_bytes == 0);
    node.resources.deinit(allocator.asStd());
    allocator.asStd().destroy(node);
}

export fn antfly_execution_fixture_invocations() callconv(.c) usize {
    return invocation_count.load(.monotonic);
}

export fn antfly_execution_fixture_poll_publisher(contract: *const runtime.native_abi.TypeContract, raw: *const anyopaque, output: *bool, failure: *?[*:0]const u8) callconv(.c) status.Status {
    const Publisher = local.storage_db_replication_contract.Publisher;
    failure.* = null;
    if (!contract.matches(runtime.native_abi.TypeContract.of(Publisher))) return status.statusFromError(error.UnsupportedVersion);
    if (@intFromPtr(raw) % @alignOf(Publisher) != 0) return status.statusFromError(error.InvalidArgument);
    const publisher: *const Publisher = @ptrCast(@alignCast(raw));
    output.* = publisher.pollCompletion(.{ .publisher = publisher.* }, 7) catch |err| {
        // Publisher domain errors travel by name, just like the production
        // native callback boundary; generic C status details are a smaller set.
        failure.* = @errorName(err).ptr;
        return .ok;
    };
    return .ok;
}

export fn antfly_execution_fixture_recover_publisher(contract: *const runtime.native_abi.TypeContract, raw: *const anyopaque, port_raw: *const anyopaque, output: *u64, failure: *?[*:0]const u8) callconv(.c) status.Status {
    const Publisher = local.storage_db_replication_contract.Publisher;
    const Port = local.common_read_admission.Port;
    failure.* = null;
    output.* = 0;
    if (!contract.matches(runtime.native_abi.TypeContract.of(Publisher))) return status.statusFromError(error.UnsupportedVersion);
    if (@intFromPtr(raw) % @alignOf(Publisher) != 0 or @intFromPtr(port_raw) % @alignOf(Port) != 0) return status.statusFromError(error.InvalidArgument);
    const publisher: *const Publisher = @ptrCast(@alignCast(raw));
    const port: *const Port = @ptrCast(@alignCast(port_raw));
    var cursor: local.storage_db_durable_outbox.RecoveryCursor = .{ .state = .{ .from_lsn = 99 } };
    output.* = publisher.recoverWithAdmission(.{ .publisher = publisher.* }, .batch, .{ .from_lsn = 3, .payload = "payload", .recovery_identity = @splat(0x5a), .recovery_cursor = &cursor }, .{ .table_id = 1, .shard_id = 2 }, port.*) catch |err| {
        failure.* = @errorName(err).ptr;
        return .ok;
    };
    if (cursor.state.next_lsn != 123) return status.statusFromError(error.InvalidArgument);
    return .ok;
}

/// Construct the shipped server factory in an independent compilation. All
/// borrowed native layouts are checked before the provider reads their fields.
export fn antfly_execution_fixture_transaction_runtime(runtime_contract: *const runtime.native_abi.TypeContract, store_contract: *const runtime.native_abi.TypeContract, backend_contract: *const runtime.native_abi.TypeContract, allocator: *const memory.Allocator, store_raw: *anyopaque, backend_raw: *anyopaque, output: *anyopaque) callconv(.c) status.Status {
    const Owned = local.storage_db_transaction_recovery_contract.OwnedRuntime;
    const Store = local.storage_backend_erased.Store;
    if (!runtime_contract.matches(runtime.native_abi.TypeContract.of(Owned)) or
        !store_contract.matches(runtime.native_abi.TypeContract.of(Store)) or
        !backend_contract.matches(runtime.native_abi.TypeContract.of(background.BackendRuntime)) or !allocator.valid()) return status.statusFromError(error.UnsupportedVersion);
    if (@intFromPtr(store_raw) % @alignOf(Store) != 0 or @intFromPtr(backend_raw) % @alignOf(background.BackendRuntime) != 0 or @intFromPtr(output) % @alignOf(Owned) != 0) return status.statusFromError(error.InvalidArgument);
    const store: *Store = @ptrCast(@alignCast(store_raw));
    const backend: *background.BackendRuntime = @ptrCast(@alignCast(backend_raw));
    const out: *Owned = @ptrCast(@alignCast(output));
    const server = @import("storage/server_transaction_recovery.zig");
    const Resolver = struct {
        fn resolve(_: *anyopaque, _: local.storage_transactions.TxnId, _: []const u8, _: local.storage_transactions.TxnStatus, _: u64) !void {}
    };
    const config: server.Config = .{ .enabled = true, .lease_owned = true, .resolver_ctx = backend_raw, .resolve_participant_fn = Resolver.resolve };
    const factory = server.borrowedConfig(&config).factory.?;
    out.* = factory.create(factory.ptr, allocator.asStd(), store.*, backend, .{ .resolution_extra_hooks = .{}, .local_resolution_ctx = null, .resolve_local_fn = null }) catch |cause| return status.statusFromError(cause);
    return .ok;
}

export fn antfly_execution_fixture_drain_guard(scope: *anyopaque, registry: bool, acquire: bool) callconv(.c) void {
    services.qualifyDrainGuard(scope, registry, acquire);
}
export fn antfly_execution_fixture_drain_calls(scope: *anyopaque) callconv(.c) usize {
    return services.qualifyDrainCalls(scope);
}
export fn antfly_execution_fixture_drain_closing(scope: *anyopaque) callconv(.c) bool {
    return services.qualifyDrainClosing(scope);
}

export fn antfly_execution_fixture_drain_try_guard(scope: *anyopaque, registry: bool) callconv(.c) bool {
    return services.qualifyDrainTryGuard(scope, registry);
}

/// Exercise the real checked Boundary dispatch from a separately compiled
/// producer. Host callbacks must retain their original stack scope and owner.
export fn antfly_execution_fixture_publication(contract: *const runtime.native_abi.TypeContract, raw: *const anyopaque, phase: u8) callconv(.c) status.Status {
    const Publication = runtime.execution_wait.GuardedPublication;
    if (!contract.matches(runtime.native_abi.TypeContract.of(Publication))) return status.statusFromError(error.UnsupportedVersion);
    if (@intFromPtr(raw) % @alignOf(Publication) != 0) return status.statusFromError(error.InvalidArgument);
    const publication: *const Publication = @ptrCast(@alignCast(raw));
    const result = switch (phase) {
        0 => publication.scope.beginWait(),
        1 => publication.wait.beginWait(),
        2 => publication.wait.endWait(),
        3 => publication.scope.endWait(),
        else => return status.statusFromError(error.InvalidArgument),
    };
    result catch |cause| return status.statusFromError(cause);
    return .ok;
}
