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

//! Analyze the actual exported storage context constructor and destructor.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const capi_dependencies = @import("capi_dependencies.zig");
const provider = @import("capi/server_owner.zig");
const services = @import("storage/kernel_runtime_services.zig");
const std = @import("std");
const local = @import("antfly_local_sources");
const background = local.storage_background_runtime;

test "shared execution storage context uses node workers and returns final physical charges" {
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 2048 << 20 } });
    defer root.deinit(std.testing.allocator);
    const parent = root.memoryParentPort();
    var node = try background.BackendRuntimeHandle.init(std.testing.allocator, .{
        .memory_parent = parent,
        .shared_sql_memory_parent = parent,
        .lane_limits = .{ .worker_capacity = 2 },
    });
    var node_live = true;
    defer if (node_live) node.deinit();
    const offer = local.storage_execution_services.Offer.init(node.ptr());
    var allocator = std.testing.allocator;
    const allocator_port = services.memory.Allocator.fromStd(&allocator);
    var builder = try services.RuntimeRequest.init(node.ptr(), .{
        .allocator = allocator_port,
        .memory_limit_bytes = 8 << 20,
    });
    const request = builder.request();
    try std.testing.expectEqual(parent.ptr, request.memory_parent.?.ptr);
    const first = try provider.createStorageOwnerContext(request);
    var first_live = true;
    defer if (first_live) std.debug.assert(first.deinitIfIdle());
    const second = try provider.createStorageOwnerContext(request);
    var second_live = true;
    defer if (second_live) std.debug.assert(second.deinitIfIdle());
    try std.testing.expect(!first.backend_runtime.ptr().usesBorrowedIo());
    try std.testing.expect(!first.backend_runtime.ptr().durable_jobs.executesInline());
    var one = try first.backend_runtime.ptr().acquireWorkers(.{});
    defer one.release();
    var two = try second.backend_runtime.ptr().acquireWorkers(.{});
    defer two.release();
    try std.testing.expectError(error.WorkerCapacityExceeded, node.ptr().acquireWorkers(.{}));
    one.release();
    try std.testing.expect(first.deinitIfIdle());
    first_live = false;
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    const io = two.io();
    var future = try io.concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), future.await(io));
    // Constructor failure must unwind both scoped runtimes, the child manager
    // and bootstrap charge without closing a peer's shared execution.
    const before = root.snapshot().memory.used_bytes;
    var invalid = request;
    invalid.context.storage_kind = .lite;
    try std.testing.expectError(error.InvalidArgument, provider.createStorageOwnerContext(invalid));
    try std.testing.expectEqual(before, root.snapshot().memory.used_bytes);
    const Close = struct {
        node: *background.BackendRuntimeHandle,
        root: *local.storage_resource_manager.ResourceManager,
        bytes_after_join: u64 = undefined,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.node.deinit();
            self.bytes_after_join = self.root.snapshot().memory.used_bytes;
            self.done.store(true, .release);
        }
    };
    var close: Close = .{ .node = &node, .root = &root };
    const closer = try std.Thread.spawn(.{}, Close.run, .{&close});
    defer closer.join();
    node_live = false;
    // Failure paths must release the retained storage scopes before joining
    // a source close that is deliberately blocked on those same scopes.
    defer if (second_live) {
        two.release();
        std.debug.assert(second.deinitIfIdle());
        second_live = false;
    };
    // Ensure the source runtime has actually fenced new scope admission.
    // Retained context capabilities remain valid for cleanup and physical join.
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        var candidate = local.storage_execution_services.Client.open(&offer) catch |err| {
            try std.testing.expectEqual(error.BackendRuntimeShuttingDown, err);
            break;
        };
        try candidate.close();
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        std.Thread.yield() catch {};
    }
    node_live = false;
    try std.testing.expect(!close.done.load(.acquire));
    two.release();
    try std.testing.expect(second.deinitIfIdle());
    second_live = false;
    while (!close.done.load(.acquire)) std.Thread.yield() catch {};
    try std.testing.expectEqual(@as(u64, 0), close.bytes_after_join);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "shared execution storage context rejects old request prefix before appended offer" {
    const Header = extern struct { version: u32 = services.abi_version - 1 };
    var old: Header align(@alignOf(services.Request)) = .{};
    var output: ?*anyopaque = @ptrFromInt(8);
    try std.testing.expectEqual(.invalid_abi, provider.storageOwnerContextCreateWithRuntime(@ptrCast(&old), &output));
    try std.testing.expect(output == null);
}

test "shared execution storage context Lite commit and reopen borrow node filesystem execution" {
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 2048 << 20 } });
    defer root.deinit(std.testing.allocator);
    var parent = root.memoryParentPort();
    var node = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .memory_parent = parent, .shared_sql_memory_parent = parent });
    defer node.deinit();
    const offer = local.storage_execution_services.Offer.init(node.ptr());
    var allocator = std.testing.allocator;
    const allocator_port = services.memory.Allocator.fromStd(&allocator);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/shared.aflite", .{tmp.sub_path});
    defer allocator.free(path);
    const request: services.Request = .{
        .context = .{ .storage_kind = .lite, .storage_path = .fromSlice(path) },
        .allocator = &allocator_port,
        .memory_limit_bytes = 128 << 20,
        .memory_parent = &parent,
        .execution_services = &offer,
    };
    {
        const context = try provider.createStorageOwnerContext(request);
        defer std.debug.assert(context.deinitIfIdle());
        const file = &context.lite_backend.?.native_docstore.?.file;
        try std.testing.expect(file.owned_io == null and file.borrowed_io != null);
        const store = try context.lite_backend.?.runtimeStoreForNamespace("system/execution-sharing");
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put("committed", "durable");
        try write.commit();
    }
    const reopened = try provider.createStorageOwnerContext(request);
    defer std.debug.assert(reopened.deinitIfIdle());
    const file = &reopened.lite_backend.?.native_docstore.?.file;
    try std.testing.expect(file.owned_io == null and file.borrowed_io != null);
    const store = try reopened.lite_backend.?.runtimeStoreForNamespace("system/execution-sharing");
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("durable", try read.get("committed"));
}

test "shared execution storage context helper inherits parent and preserves modeled manual execution" {
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 8 << 20 } });
    defer root.deinit(std.testing.allocator);
    var node = try background.BackendRuntimeHandle.init(std.testing.allocator, .{
        .backend = .manual,
        .borrowed_io = .{ .general = std.testing.io },
        .memory_parent = root.memoryParentPort(),
    });
    defer node.deinit();
    var allocator = std.testing.allocator;
    var builder = try services.RuntimeRequest.init(node.ptr(), .{ .allocator = services.memory.Allocator.fromStd(&allocator) });
    const request = builder.request();
    try std.testing.expectEqual(@as(u64, 8 << 20), request.memory_limit_bytes);
    try std.testing.expectEqual(root.memoryParentPort().ptr, request.memory_parent.?.ptr);
    const context = try provider.createStorageOwnerContext(request);
    defer std.debug.assert(context.deinitIfIdle());
    try std.testing.expect(context.backend_runtime.ptr().usesBorrowedIo());
    try std.testing.expect(context.backend_runtime.ptr().durable_jobs.executesInline());
    const hard = context.resources.resource_manager.snapshot().memory.hard_limit_bytes;
    try std.testing.expect(hard > 0 and hard <= 8 << 20);
}

test "shared execution storage context helper without parent keeps fixed unavailable budget defaults" {
    var node = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual, .borrowed_io = .{ .general = std.testing.io } });
    defer node.deinit();
    var allocator = std.testing.allocator;
    var builder = try services.RuntimeRequest.init(node.ptr(), .{ .allocator = services.memory.Allocator.fromStd(&allocator) });
    const request = builder.request();
    try std.testing.expect(request.memory_parent == null and request.memory_limit_bytes == 0);
    const context = try provider.createStorageOwnerContext(request);
    defer std.debug.assert(context.deinitIfIdle());
    try std.testing.expectEqual(@as(u64, 0), context.resources.resource_manager.snapshot().memory.hard_limit_bytes);
    try std.testing.expect(context.backend_runtime.ptr().durable_jobs.executesInline());
}

// Exercise the typed client against the actual source provider. The separate
// execution-services archive tests cover the independently compiled producer.
export fn antfly_storage_context_create_with_runtime(request: *const services.Request, out: *?*anyopaque) callconv(.c) @import("kernel_owner_abi").Status {
    return provider.storageOwnerContextCreateWithRuntime(request, out);
}
export fn antfly_storage_context_destroy(context: ?*anyopaque) callconv(.c) @import("kernel_owner_abi").Status {
    return provider.storageOwnerContextDestroy(context);
}

test "shared execution storage context typed client copies scoped request and preserves idempotent ensure" {
    var root = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 8 << 20 } });
    defer root.deinit(std.testing.allocator);
    var node = try background.BackendRuntimeHandle.init(std.testing.allocator, .{
        .backend = .manual,
        .borrowed_io = .{ .general = std.testing.io },
        .memory_parent = root.memoryParentPort(),
    });
    defer node.deinit();
    var allocator = std.testing.allocator;
    var context: @import("storage/kernel_owner_client.zig").Context = .{};
    defer context.deinit();
    try context.ensureWithBackendRuntime(node.ptr(), .{ .allocator = services.memory.Allocator.fromStd(&allocator) });
    const handle = context.handle;
    const held = root.snapshot().memory.used_bytes;
    try context.ensureWithBackendRuntime(node.ptr(), .{});
    try std.testing.expectEqual(handle, context.handle);
    try std.testing.expectEqual(held, root.snapshot().memory.used_bytes);
    const owner = provider.asStorageOwnerContext(context.handle).?;
    try std.testing.expect(owner.backend_runtime.ptr().durable_jobs.executesInline());
    try std.testing.expect(owner.backend_runtime.ptr().shared_execution != null);
    context.deinit();
    try std.testing.expect(root.snapshot().memory.used_bytes < held);
}

test "shared execution storage context inherits helper-owned process envelope and retires its charges" {
    var helper = try @import("storage/helper_runtime_resources.zig").Owner.init(std.testing.allocator, .{ .limit_bytes = 2048 << 20 });
    var helper_live = true;
    defer if (helper_live) helper.deinit();
    // Keep the ledger inspectable after the helper has physically joined all
    // workers. Retaining it does not retain execution or add memory capacity.
    const ledger = helper.manager.retainLifetime().?;
    defer ledger.release();
    const before = helper.manager.snapshot().memory.used_bytes;
    var builder = try services.RuntimeRequest.init(helper.ptr(), .{ .memory_limit_bytes = 8 << 20 });
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(helper.manager)), builder.request().memory_parent.?.ptr);
    const context = try provider.createStorageOwnerContext(builder.request());
    var live = true;
    defer if (live) std.debug.assert(context.deinitIfIdle());
    try std.testing.expect(helper.manager.snapshot().memory.used_bytes > before);
    var worker = try context.backend_runtime.ptr().acquireWorkers(.{});
    defer worker.release();
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    var result = try worker.io().concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), result.await(worker.io()));
    worker.release();
    try std.testing.expect(context.deinitIfIdle());
    live = false;
    // A shared worker may retain lazy IO/TLS allocations until runtime close.
    // Context close must not pretend those physical bytes have retired early.
    helper.deinit();
    helper_live = false;
    try std.testing.expectEqual(@as(u64, @sizeOf(local.storage_resource_manager.SharedResourceManager)), ledger.manager.snapshot().memory.used_bytes);
}

test "shared execution storage context Raft attempt accounts live physical tasks before admission" {
    var node = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .lane_limits = .{ .raft_outbound = 12 } });
    defer node.deinit();
    const io = node.ptr().raftOutboundIo().?;
    var done: std.Io.Event = .unset;
    var started: std.atomic.Value(usize) = .init(0);
    const Blocked = struct {
        fn run(task_io: std.Io, stop: *std.Io.Event, count: *std.atomic.Value(usize)) void {
            _ = count.fetchAdd(1, .acq_rel);
            stop.waitUncancelable(task_io);
        }
    };
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    defer done.set(io);
    for (0..6) |_| try group.concurrent(io, Blocked.run, .{ io, &done, &started });
    const until = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (started.load(.acquire) != 6) {
        if (@import("antfly_platform").time.monotonicNs() >= until) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    var first = try node.ptr().acquireRaftOutboundLane();
    defer first.release();
    try std.testing.expectError(error.RaftOutboundCapacityUnavailable, node.ptr().acquireRaftOutboundLane());
    try std.testing.expectEqual(@as(usize, 1), node.ptr().laneStats().raft_outbound_active_leases);
    done.set(io);
    try group.await(io);
}

test "shared execution storage context quota retains actual host allocator and process parent across busy typed destruction" {
    const a = std.testing.allocator;
    var root = local.storage_resource_manager.ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 16 << 20 } });
    defer root.deinit(a);
    var node = try background.BackendRuntimeHandle.init(a, .{ .backend = .manual, .borrowed_io = .{ .general = std.testing.io }, .memory_parent = root.memoryParentPort() });
    defer node.deinit();
    var allocator = a;
    const before = root.snapshot().memory.used_bytes;
    var context: @import("storage/kernel_owner_client.zig").Context = .{};
    defer context.deinit();
    try context.ensureWithBackendRuntime(node.ptr(), .{ .allocator = services.memory.Allocator.fromStd(&allocator), .memory_limit_bytes = 8 << 20 });
    const original_handle = context.handle;
    const actual = provider.asStorageOwnerContext(context.handle).?;
    const backend = local.storage_lsm_backend_mod;
    var close: backend.PinQuota.Close = .{};
    defer close.deinitJoined();
    var creator = try actual.resources.beginPinQuotaCreation(&close);
    defer creator.deinitJoined();
    const Control = struct {
        fn check(_: *anyopaque) !void {}
        fn wait(_: *anyopaque) !void {
            return error.ParentMemoryBusy;
        }
    };
    var tag: u8 = 0;
    const payload: local.common_read_admission.Payload = .{ .ptr = &tag, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .wait_for_progress = Control.wait } };
    const control = local.common_read_admission.Port.init(&payload);
    try creator.resumeWithAdmission(control);
    const owner = try creator.takeWithAdmission(control);
    var operation = backend.Backend.prepareRunSnapshotRefOperation("host-context-quota-file", .retain);
    defer operation.deinit();
    try operation.resumeFundedWithAdmission(owner, control);
    try operation.publishFundedWithAdmission(control);
    const pin = try operation.takePinWithAdmission(control);
    var pin_live = true;
    defer if (pin_live) pin.release();
    try operation.retireWithAdmission(control);
    const held = root.snapshot().memory.used_bytes;
    try std.testing.expectError(error.StorageBusy, context.tryDeinit());
    try std.testing.expectEqual(original_handle, context.handle);
    try std.testing.expectEqual(held, root.snapshot().memory.used_bytes);
    try std.testing.expectError(error.ParentMemoryBusy, close.resumeWithAdmission(control));
    pin.release();
    pin_live = false;
    var forget = backend.Backend.prepareRunSnapshotRefOperation("host-context-quota-file", .forget);
    defer forget.deinit();
    _ = try forget.planWithAdmission(control);
    try forget.resumeWithAdmission(control);
    try forget.publishWithAdmission(control);
    try forget.retireWithAdmission(control);
    try close.resumeWithAdmission(control);
    try std.testing.expectError(error.StorageBusy, context.tryDeinit());
    close.deinitJoined();
    try context.tryDeinit();
    try std.testing.expect(context.handle == null);
    try std.testing.expectEqual(before, root.snapshot().memory.used_bytes);
}
