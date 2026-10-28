// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const rpc = @import("inference_worker_rpc");
const resources = @import("antfly_local_sources").storage_resource_manager;
const metadata_fixture = resources.metadata_fixture;
const transient = @import("standalone/inference_transient_memory.zig");
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;

comptime {
    _ = @import("antfly_inference_native_exports");
}

const Owner = struct {
    manager: *resources.ResourceManager,
    transient_bytes: std.atomic.Value(u64) = .init(0),
    references: std.atomic.Value(usize) = .init(1),
    fn retain(raw: *anyopaque) callconv(.c) u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        _ = self.references.fetchAdd(1, .acq_rel);
        return 1;
    }
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.references.fetchSub(1, .acq_rel) > 1);
    }
    fn adjust(raw: *anyopaque, identity: usize, previous: u64, next: u64) callconv(.c) u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return @intFromBool(transient.adjust(self, identity, previous, next));
    }
    fn port(self: *Owner) rpc.PayloadBudget.External {
        return .{ .context = self, .retain = retain, .release = release, .adjust = adjust };
    }
};
fn endpoint(budget: *rpc.PayloadBudget) rpc.Endpoint {
    return .{
        .alloc = std.testing.allocator,
        .io = std.testing.io,
        .input = undefined,
        .output = undefined,
        .context = undefined,
        .handler = undefined,
        .on_closed = undefined,
        .next_id = 1,
        .payload_budget = budget,
    };
}

test "worker node memory shares the real aggregate limit with query and model reservations" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var owner = Owner{ .manager = &manager };
    var query = try manager.reserveWithoutReclaim(.dense_search_working_set, 40);
    defer query.release();
    var model = try manager.reserveWithoutReclaim(.inference_model_residency, 20);
    defer model.release();
    const a = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer a.releaseOwner();
    try a.configureExternal(owner.port());
    const b = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer b.releaseOwner();
    try b.configureExternal(owner.port());
    const alloc = a.retainedAllocator();
    const bytes = try alloc.alloc(u8, 32);
    var live = true;
    defer if (live) alloc.free(bytes);
    try std.testing.expectEqual(@as(u64, 92), metadata_fixture.payloadUsage(&manager));
    var peer = endpoint(b);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, peer.copyPayload(.{ .body = "123456789" }));
    try std.testing.expectEqual(@as(usize, 0), b.used().control);
    query.release();
    const small = try peer.copyPayload(.{ .body = "123456789" });
    var small_live = true;
    defer if (small_live) small.deinit();
    try std.testing.expectEqual(@as(u64, 61), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 41), owner.transient_bytes.load(.acquire));
    try std.testing.expectError(error.OutOfMemory, b.retainedAllocator().alloc(u8, 40));
    alloc.free(bytes);
    live = false;
    small.deinit();
    small_live = false;
    try std.testing.expectEqual(@as(u64, 20), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), owner.transient_bytes.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "worker node memory stays owned after client teardown and shrinks under tightened pressure" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var owner = Owner{ .manager = &manager };
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    var anchored = true;
    defer if (anchored) budget.releaseOwner();
    try budget.configureExternal(owner.port());
    const alloc = budget.retainedAllocator();
    const bytes = try alloc.dupe(u8, "retained after client");
    budget.releaseOwner();
    anchored = false;
    try std.testing.expectEqual(@as(usize, 2), owner.references.load(.acquire));
    try std.testing.expectEqual(@as(u64, bytes.len), metadata_fixture.payloadUsage(&manager));
    metadata_fixture.setPayloadLimit(&manager, 1, &.{});
    try std.testing.expectEqualStrings("retained after client", bytes);
    alloc.free(bytes);
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), owner.transient_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), owner.references.load(.acquire));
}

test "worker node memory backing allocation refusal rolls both ledgers back" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var owner = Owner{ .manager = &manager };
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    const budget = try rpc.PayloadBudget.create(failing.allocator(), .{});
    defer budget.releaseOwner();
    try budget.configureExternal(owner.port());
    const alloc = budget.retainedAllocator();
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 16));
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), owner.transient_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    try std.testing.expectEqual(@as(usize, 1), budget.references.load(.acquire));
}

test "worker node memory rejects stale identities without changing aggregate ownership" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var owner = Owner{ .manager = &manager };
    try std.testing.expect(transient.adjust(&owner, 11, 0, 40));
    try std.testing.expect(!transient.adjust(&owner, 11, 0, 20));
    try std.testing.expect(!transient.adjust(&owner, 12, 5, 20));
    try std.testing.expectEqual(@as(u64, 40), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 40), owner.transient_bytes.load(.acquire));
    try std.testing.expect(transient.adjust(&owner, 11, 40, 0));
}

test "worker node memory obeys shared parent pressure from sibling storage and models" {
    var root = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var inference = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 200 }, .identity_allocator = std.testing.allocator });
    defer inference.deinit(std.testing.allocator);
    var storage = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer storage.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&inference, &root);
    try metadata_fixture.installParent(&storage, &root);
    var model = try root.reserveWithoutReclaim(.inference_model_residency, 40);
    defer model.release();
    var work = try storage.reserveWithoutReclaim(.lsm_wal_write_working_set, 50);
    defer work.release();
    var owner = Owner{ .manager = &inference };
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer budget.releaseOwner();
    try budget.configureExternal(owner.port());
    const alloc = budget.retainedAllocator();
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 16));
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&inference));
    try std.testing.expectEqual(@as(u64, 90), (metadata_fixture.payloadUsage(&root) - inference.identity_metadata_bytes - storage.identity_metadata_bytes));
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    work.release();
    const bytes = try alloc.alloc(u8, 16);
    try std.testing.expectEqual(@as(u64, 56), (metadata_fixture.payloadUsage(&root) - inference.identity_metadata_bytes - storage.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 16), metadata_fixture.payloadUsage(&inference));
    alloc.free(bytes);
    try std.testing.expectEqual(@as(u64, 40), (metadata_fixture.payloadUsage(&root) - inference.identity_metadata_bytes - storage.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 0), owner.transient_bytes.load(.acquire));
}

test "worker node memory concurrent clients cannot oversubscribe the shared ledger" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var owner = Owner{ .manager = &manager };
    const a = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer a.releaseOwner();
    try a.configureExternal(owner.port());
    const b = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer b.releaseOwner();
    try b.configureExternal(owner.port());
    const Probe = struct {
        start: std.atomic.Value(bool) = .init(false),
        finish: std.atomic.Value(bool) = .init(false),
        attempts: std.atomic.Value(usize) = .init(0),
        admitted: std.atomic.Value(usize) = .init(0),
        unexpected: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This(), budget: *rpc.PayloadBudget) void {
            while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
            const alloc = budget.retainedAllocator();
            const bytes = alloc.alloc(u8, 80) catch |err| {
                if (err != error.OutOfMemory) self.unexpected.store(true, .release);
                _ = self.attempts.fetchAdd(1, .acq_rel);
                return;
            };
            _ = self.admitted.fetchAdd(1, .acq_rel);
            _ = self.attempts.fetchAdd(1, .acq_rel);
            while (!self.finish.load(.acquire)) std.atomic.spinLoopHint();
            alloc.free(bytes);
        }
    };
    var probe: Probe = .{};
    var threads: [2]std.Thread = undefined;
    var count: usize = 0;
    defer {
        probe.start.store(true, .release);
        probe.finish.store(true, .release);
        for (threads[0..count]) |thread| thread.join();
    }
    for ([_]*rpc.PayloadBudget{ a, b }, &threads) |budget, *thread| {
        thread.* = try std.Thread.spawn(.{}, Probe.run, .{ &probe, budget });
        count += 1;
    }
    probe.start.store(true, .release);
    const deadline = std.Io.Timestamp.now(std.testing.io, .awake).toNanoseconds() + 5 * std.time.ns_per_s;
    while (probe.attempts.load(.acquire) != 2) {
        if (std.Io.Timestamp.now(std.testing.io, .awake).toNanoseconds() >= deadline) return error.TestTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!probe.unexpected.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), probe.admitted.load(.acquire));
    try std.testing.expectEqual(@as(u64, 80), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 80), owner.transient_bytes.load(.acquire));
    probe.finish.store(true, .release);
    for (threads[0..count]) |thread| thread.join();
    count = 0;
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), owner.transient_bytes.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "worker node memory admission refuses contention before touching the node ledger" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var owner = Owner{ .manager = &manager };
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer budget.releaseOwner();
    try budget.configureExternal(owner.port());
    try std.testing.expect(budget.external_mutex.tryLock());
    defer budget.external_mutex.unlock();
    try std.testing.expectError(error.OutOfMemory, budget.retainedAllocator().alloc(u8, 16));
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), owner.transient_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    try std.testing.expectEqual(@as(usize, 1), budget.references.load(.acquire));
}

test "worker node memory child heap RPC and receive grants share real storage pressure" {
    const heap_mod = @import("antfly_inference_host").worker_module.heap_module;
    const wire = @import("inference_worker_wire");
    const bridge = @import("inference_worker_bridge");
    const io = std.testing.io;
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var owner = Owner{ .manager = &manager };
    var query = try manager.reserveWithoutReclaim(.dense_search_working_set, 32);
    defer query.release();
    var storage = try manager.reserveWithoutReclaim(.lsm_wal_write_working_set, 60);
    defer storage.release();
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer budget.releaseOwner();
    try budget.configureExternal(owner.port());
    const Parent = struct {
        current: u64 = 0,
        large_attempts: usize = 0,
        port: bridge.ResourceBudget,
        fn resource(raw: *anyopaque, bytes: []const u8) !rpc.ResourceReply {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const request = try std.json.parseFromSliceLeaky(wire.ResourceRequest, arena.allocator(), bytes, .{});
            if (request.operation != .transient_host) return error.UnsupportedOperation;
            const observation = try std.json.parseFromSliceLeaky(wire.Observation, arena.allocator(), request.data, .{});
            if (observation.next >= 300 * 1024 * 1024) self.large_attempts += 1;
            heap_mod.adjustParent(&self.current, &self.port, observation) catch |err| {
                if (err != error.ResourceTemporarilyUnavailable) return err;
                const status = bridge.statusFromError(err);
                return .{ .code = status.code, .detail = status.detail };
            };
            return .{};
        }
        fn respond(_: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
            return request.endpoint.copyPayload(.{ .body = "child" });
        }
    };
    var parent = Parent{ .port = undefined };
    parent.port.context = &owner;
    parent.port.adjust_transient_host = Owner.adjust;
    var pair: rpc.TestPair = undefined;
    try pair.initUnstarted();
    var heap: heap_mod.Heap = undefined;
    try heap.init(std.testing.allocator, &pair.child);
    defer heap.deinit();
    pair.parent.context = &parent;
    pair.parent.resource_handler = Parent.resource;
    pair.parent.payload_budget = budget;
    pair.parent.peer_payload_budget = budget;
    pair.child.report_retirement = true;
    pair.child.compute_allocator = heap.allocator();
    pair.child.handler = Parent.respond;
    defer {
        pair.parent.fail();
        pair.child.deinit();
        heap_mod.retireParentAfterJoin(&parent.current, &parent.port);
        pair.parent.releasePeerAfterPhysicalJoin();
        pair.parent.deinit();
        for (pair.files) |file| file.close(io);
    }
    try pair.parent.start();
    try pair.child.start();
    // Both transport precharge and the child's backing allocator see the same
    // authoritative ledger as query and storage work.
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pair.parent.call(.{ .body = "sixteen-byte-msg!" }, .{}));
    try std.testing.expectError(error.OutOfMemory, heap.allocator().alloc(u8, 16));
    // A cache-sized request must reach the real node budget, rather than being
    // rejected by RPC's unrelated 256 MiB frame/payload guard. The node refuses
    // it before any 300 MiB backing allocation takes place.
    try std.testing.expectError(error.OutOfMemory, heap.allocator().alloc(u8, 300 * 1024 * 1024));
    try std.testing.expectEqual(@as(usize, 1), parent.large_attempts);
    try std.testing.expectEqual(@as(u64, 92), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), parent.current);
    storage.release();
    const bytes = try heap.allocator().alloc(u8, 16);
    try std.testing.expectEqual(@as(u64, 48), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 16), parent.current);
    heap.allocator().free(bytes);
    try std.testing.expectEqual(@as(u64, 32), metadata_fixture.payloadUsage(&manager));
    const response = try pair.parent.call(.{ .body = "input" }, .{});
    var response_live = true;
    defer if (response_live) response.deinit();
    try std.testing.expectEqualStrings("child", response.view().body);
    const deadline = std.Io.Timestamp.now(io, .awake).toNanoseconds() + 3 * std.time.ns_per_s;
    while (metadata_fixture.payloadUsage(&manager) != 37) {
        if (std.Io.Timestamp.now(io, .awake).toNanoseconds() >= deadline) return error.TestTimeout;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    // Only the delivered parent's actual slab remains; delivery alone never
    // refunds it. Child heap and incoming grants have physically retired.
    response.deinit();
    response_live = false;
    try std.testing.expectEqual(@as(u64, 32), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), owner.transient_bytes.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "worker node memory bootstrap and configured data share one parent without duplicate charges" {
    const Bootstrap = @import("antfly_inference_host").worker_module.heap_module.Bootstrap;
    var root = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var parent = root.memoryParentPort();
    var child = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 }, .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    const bootstrap = try Bootstrap.create(std.testing.allocator, @ptrCast(&parent));
    var anchor_live = true;
    defer if (anchor_live) bootstrap.release();
    try metadata_fixture.installParent(&child, &root);
    var owner: Owner = .{ .manager = &child };
    const budget = try rpc.PayloadBudget.createWithObjectAllocator(std.testing.allocator, bootstrap.allocator(), .{});
    var budget_live = true;
    defer if (budget_live) budget.releaseOwner();
    try budget.configureExternal(owner.port());
    const baseline = @sizeOf(Bootstrap) + @sizeOf(rpc.PayloadBudget);
    try std.testing.expectEqual(@as(u64, baseline), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&child));
    const alloc = budget.retainedAllocator();
    const bytes = try alloc.alloc(u8, 32);
    var data_live = true;
    defer if (data_live) alloc.free(bytes);
    try std.testing.expectEqual(@as(u64, baseline + 32), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 32), metadata_fixture.payloadUsage(&child));
    bootstrap.release();
    anchor_live = false;
    budget.releaseOwner();
    budget_live = false;
    try std.testing.expectEqual(@as(u64, baseline + 32), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    alloc.free(bytes);
    data_live = false;
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&child));
    try std.testing.expectEqual(@as(usize, 1), owner.references.load(.acquire));
}

test "worker node memory embedded startup parent shares bounded pressure across allocations" {
    const provider = @import("antfly_local_sources").storage_inference_provider;
    const owner = try provider.LiteInferenceResourceOwner.create(std.testing.allocator, 100);
    defer owner.releaseBaseReference();
    const parent = owner.manager.memoryParentPort();
    try std.testing.expect(parent.valid());
    try std.testing.expectEqual(@as(u64, 100), parent.snapshot().hard_limit_bytes);
    try std.testing.expectEqual(.reserved, parent.try_reserve_fn.?(parent.ptr, 60));
    defer parent.release_fn(parent.ptr, 60);
    try std.testing.expectEqual(.denied, parent.try_reserve_fn.?(parent.ptr, 41));
    try std.testing.expectEqual(@as(u64, 60), parent.snapshot().used_bytes);
    try std.testing.expectEqual(.reserved, parent.try_reserve_fn.?(parent.ptr, 40));
    parent.release_fn(parent.ptr, 40);
    try std.testing.expectEqual(@as(u64, 60), parent.snapshot().used_bytes);
}

test "worker node memory embedded startup owner creation preserves physical allocation failure" {
    const provider = @import("antfly_local_sources").storage_inference_provider;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, provider.LiteInferenceResourceOwner.create(failing.allocator(), 100));
}

test "worker node memory embedded parent survives original service reference until metadata free" {
    const provider = @import("antfly_local_sources").storage_inference_provider;
    const Bootstrap = @import("antfly_inference_host").worker_module.heap_module.Bootstrap;
    const owner = try provider.LiteInferenceResourceOwner.create(std.testing.allocator, @sizeOf(Bootstrap) + 100);
    defer owner.releaseBaseReference();
    try std.testing.expect(owner.retainReference());
    owner.releaseBaseReference(); // Original connection closes; retained service remains.
    const parent = owner.manager.memoryParentPort();
    const bootstrap = try Bootstrap.create(std.testing.allocator, @ptrCast(&parent));
    defer bootstrap.release();
    const metadata = bootstrap.allocator();
    const bytes = try metadata.alloc(u8, 80);
    var live = true;
    defer if (live) metadata.free(bytes);
    try std.testing.expectError(error.OutOfMemory, metadata.alloc(u8, 21));
    try std.testing.expectEqual(@as(u64, @sizeOf(Bootstrap) + 80), parent.snapshot().used_bytes);
    metadata.free(bytes);
    live = false;
    try std.testing.expectEqual(@as(u64, @sizeOf(Bootstrap)), parent.snapshot().used_bytes);
    const retry = try metadata.alloc(u8, 100);
    metadata.free(retry);
}

test "worker node memory embedded production startup admits and retires actual host metadata" {
    const provider = @import("antfly_local_sources").storage_inference_provider;
    const created = try provider.createEmbeddedInferenceNode(".", std.testing.io, .{
        .models_dir = "fixture-models",
        .process_memory_budget_mb = 64,
    });
    defer provider.destroyEmbeddedInferenceNode(created.handle, created.resource_owner);
    const owner = created.resource_owner.?;
    try std.testing.expectEqual(@as(u64, 64 * 1024 * 1024), owner.manager.snapshot().memory.hard_limit_bytes);
    try std.testing.expect(owner.manager.snapshot().memory.external_bytes > 0);
    try std.testing.expect(owner.references.load(.acquire) > 1);
}

fn createLeaseFixture(payload_bytes: u64) !*@import("antfly_local_sources").storage_inference_provider.LiteInferenceResourceOwner {
    const O = @import("antfly_local_sources").storage_inference_provider.LiteInferenceResourceOwner;
    const owner = try O.create(std.testing.allocator, 0);
    errdefer owner.releaseBaseReference();
    try metadata_fixture.provision(&owner.manager, .{ .hard_limit_bytes = payload_bytes }, O.identity_capacity);
    return owner;
}

const LiteBudgetTest = struct {
    const provider = @import("antfly_local_sources").storage_inference_provider;
    const bridge = provider.inference_bridge;
    fn amounts(weights: usize, kv: usize, scratch: usize) bridge.AdmissionAmounts {
        return .{ .host_weight_bytes = weights, .backend_weight_bytes = 0, .host_kv_bytes = kv, .backend_kv_bytes = 0, .host_scratch_bytes = scratch, .backend_scratch_bytes = 0 };
    }
    fn reserve(port: bridge.ResourceBudget, value: bridge.AdmissionAmounts) !usize {
        var token: usize = 0;
        const status = port.reserve_admission(port.context, &value, &token);
        if (!status.isOk()) return bridge.errorFromStatus(status);
        try std.testing.expect(token != 0);
        return token;
    }
    fn allocationCase(alloc: std.mem.Allocator) !void {
        const owner = try provider.LiteInferenceResourceOwner.create(alloc, 1024 * 1024);
        defer owner.releaseBaseReference();
        const port = owner.resourceBudget();
        const token = try reserve(port, amounts(40, 20, 10));
        defer port.release_admission(port.context, token);
        const status = port.retain_admission(port.context, token, &amounts(20, 0, 0));
        if (!status.isOk()) return bridge.errorFromStatus(status);
        if (port.observe_tokenizer_cache(port.context, 1, 0, 10) == 0) return error.OutOfMemory;
        defer std.debug.assert(port.observe_tokenizer_cache(port.context, 1, 10, 0) == 1);
        if (port.adjust_transient_host.?(port.context, 2, 0, 10) == 0) return error.OutOfMemory;
        defer std.debug.assert(port.adjust_transient_host.?(port.context, 2, 10, 0) == 1);
        try std.testing.expectEqual(@as(u64, 40), metadata_fixture.payloadUsage(&owner.manager));
    }
};

test "worker node memory embedded admissions share metadata pressure and retain exact physical amounts" {
    const T = LiteBudgetTest;
    const owner = try createLeaseFixture(100);
    defer owner.releaseBaseReference();
    const port = owner.resourceBudget();
    const parent = owner.manager.memoryParentPort();
    try std.testing.expectEqual(.reserved, parent.try_reserve_fn.?(parent.ptr, 30));
    var metadata_live = true;
    defer if (metadata_live) parent.release_fn(parent.ptr, 30);
    const token = try T.reserve(port, T.amounts(40, 20, 10));
    defer port.release_admission(port.context, token);
    try std.testing.expectEqual(@as(u64, 100), metadata_fixture.payloadUsage(&owner.manager));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, T.reserve(port, T.amounts(1, 0, 0)));
    const reduced = T.amounts(40, 0, 0);
    try std.testing.expect(port.retain_admission(port.context, token, &reduced).isOk());
    try std.testing.expectEqual(@as(u64, 70), metadata_fixture.payloadUsage(&owner.manager));
    try std.testing.expect(!port.retain_admission(port.context, token, &T.amounts(41, 0, 0)).isOk());
    try std.testing.expectEqual(@as(u64, 70), metadata_fixture.payloadUsage(&owner.manager));
    parent.release_fn(parent.ptr, 30);
    metadata_live = false;
    const peer = try T.reserve(port, T.amounts(60, 0, 0));
    port.release_admission(port.context, peer);
}

test "worker node memory embedded lease metadata is bounded and stale tokens cannot debit successors" {
    const T = LiteBudgetTest;
    const O = T.provider.LiteInferenceResourceOwner;
    const owner = try createLeaseFixture(100);
    defer owner.releaseBaseReference();
    const port = owner.resourceBudget();
    var tokens: [O.identity_capacity]usize = @splat(0);
    defer for (tokens) |token| port.release_admission(port.context, token);
    for (&tokens) |*token| token.* = try T.reserve(port, T.amounts(0, 0, 0));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, T.reserve(port, T.amounts(0, 0, 0)));
    const stale = tokens[0];
    port.release_admission(port.context, stale);
    tokens[0] = 0;
    tokens[0] = try T.reserve(port, T.amounts(20, 0, 0));
    try std.testing.expect(tokens[0] != stale);
    port.release_admission(port.context, stale);
    try std.testing.expect(!port.retain_admission(port.context, stale, &T.amounts(0, 0, 0)).isOk());
    try std.testing.expectEqual(@as(u64, 20), metadata_fixture.payloadUsage(&owner.manager));
}

test "worker node memory embedded lease namespace never wraps and arithmetic refusal leaves no publication" {
    const T = LiteBudgetTest;
    const owner = try createLeaseFixture(100);
    defer owner.releaseBaseReference();
    const port = owner.resourceBudget();
    var overflow = T.amounts(std.math.maxInt(usize), 1, 0);
    var token: usize = 999;
    try std.testing.expect(!port.reserve_admission(port.context, &overflow, &token).isOk());
    try std.testing.expectEqual(@as(usize, 0), token);
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&owner.manager));
    owner.next_lease_token = std.math.maxInt(usize);
    token = try T.reserve(port, T.amounts(1, 0, 0));
    try std.testing.expectEqual(std.math.maxInt(usize), token);
    port.release_admission(port.context, token);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, T.reserve(port, T.amounts(0, 0, 0)));
}

test "worker node memory embedded tokenizer and transport growth refuse pressure while observed prompt bytes remain truthful" {
    const T = LiteBudgetTest;
    const owner = try createLeaseFixture(100);
    defer owner.releaseBaseReference();
    const port = owner.resourceBudget();
    try std.testing.expectEqual(@as(u8, 1), port.observe_tokenizer_cache(port.context, 1, 0, 50));
    defer std.debug.assert(port.observe_tokenizer_cache(port.context, 1, 50, 0) == 1);
    try std.testing.expectEqual(@as(u8, 1), port.adjust_transient_host.?(port.context, 1, 0, 30));
    var transient_live_bytes: u64 = 30;
    defer std.debug.assert(port.adjust_transient_host.?(port.context, 1, transient_live_bytes, 0) == 1);
    const token = try T.reserve(port, T.amounts(20, 0, 0));
    defer port.release_admission(port.context, token);
    try std.testing.expectEqual(@as(u8, 0), port.observe_tokenizer_cache(port.context, 1, 50, 51));
    try std.testing.expectEqual(@as(u8, 0), port.adjust_transient_host.?(port.context, 1, 30, 31));
    try std.testing.expectEqual(@as(u8, 0), port.adjust_transient_host.?(port.context, 1, 29, 0));
    try std.testing.expectEqual(@as(u64, 100), metadata_fixture.payloadUsage(&owner.manager));
    try std.testing.expectEqual(@as(u8, 1), port.observe_prompt_cache(port.context, 1, 0, 10));
    defer std.debug.assert(port.observe_prompt_cache(port.context, 1, 10, 0) == 1);
    try std.testing.expectEqual(@as(u64, 110), metadata_fixture.payloadUsage(&owner.manager));
    try std.testing.expectEqual(@as(u8, 1), port.adjust_transient_host.?(port.context, 1, 30, 10));
    transient_live_bytes = 10;
    try std.testing.expectEqual(@as(u64, 90), metadata_fixture.payloadUsage(&owner.manager));
}

test "worker node memory embedded observer identities have fixed capacity and reuse only after retirement" {
    const T = LiteBudgetTest;
    const O = T.provider.LiteInferenceResourceOwner;
    const owner = try createLeaseFixture(1024);
    defer owner.releaseBaseReference();
    const port = owner.resourceBudget();
    var count: usize = 0;
    defer for (0..count) |i| std.debug.assert(port.observe_tokenizer_cache(port.context, i + 1, 1, 0) == 1);
    for (0..O.identity_capacity) |i| {
        try std.testing.expectEqual(@as(u8, 1), port.observe_tokenizer_cache(port.context, i + 1, 0, 1));
        count += 1;
    }
    try std.testing.expectEqual(@as(u8, 0), port.observe_tokenizer_cache(port.context, O.identity_capacity + 1, 0, 1));
    try std.testing.expectEqual(@as(u64, O.identity_capacity), metadata_fixture.payloadUsage(&owner.manager));
    try std.testing.expectEqual(@as(u8, 1), port.observe_tokenizer_cache(port.context, 1, 1, 0));
    try std.testing.expectEqual(@as(u8, 1), port.observe_tokenizer_cache(port.context, O.identity_capacity + 1, 0, 1));
    try std.testing.expectEqual(@as(u8, 1), port.observe_tokenizer_cache(port.context, O.identity_capacity + 1, 1, 0));
    try std.testing.expectEqual(@as(u8, 1), port.observe_tokenizer_cache(port.context, 1, 0, 1));
}

test "worker node memory embedded resource admission cleans every metadata allocation failure" {
    try LiteBudgetTest.allocationCase(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, LiteBudgetTest.allocationCase, .{});
}

test "worker node memory embedded concurrent model admissions cannot oversubscribe the service" {
    const T = LiteBudgetTest;
    const O = T.provider.LiteInferenceResourceOwner;
    const Race = struct {
        fn run(owner: *O, token: *usize, unexpected: *bool) void {
            const port = owner.resourceBudget();
            const status = port.reserve_admission(port.context, &T.amounts(30, 0, 0), token);
            unexpected.* = !status.isOk() and T.bridge.errorFromStatus(status) != error.ResourceTemporarilyUnavailable;
        }
    };
    const owner = try createLeaseFixture(100);
    defer owner.releaseBaseReference();
    var tokens: [8]usize = @splat(0);
    defer for (tokens) |token| owner.resourceBudget().release_admission(owner, token);
    var unexpected: [8]bool = @splat(false);
    var threads: [8]std.Thread = undefined;
    var spawned: usize = 0;
    var joined = false;
    defer if (!joined) for (threads[0..spawned]) |thread| thread.join();
    for (&threads, 0..) |*thread, i| {
        thread.* = try std.Thread.spawn(.{}, Race.run, .{ owner, &tokens[i], &unexpected[i] });
        spawned += 1;
    }
    for (threads) |thread| thread.join();
    joined = true;
    var accepted: u64 = 0;
    for (tokens, unexpected) |token, failed| {
        try std.testing.expect(!failed);
        if (token != 0) accepted += 1;
    }
    try std.testing.expect(accepted > 0 and accepted <= 3);
    try std.testing.expectEqual(accepted * 30, metadata_fixture.payloadUsage(&owner.manager));
}

test "worker node memory embedded backend metrics preserve platform host charging" {
    const T = LiteBudgetTest;
    const owner = try createLeaseFixture(100);
    defer owner.releaseBaseReference();
    const port = owner.resourceBudget();
    const value: T.bridge.AdmissionAmounts = .{ .host_weight_bytes = 10, .backend_weight_bytes = 20, .host_kv_bytes = 3, .backend_kv_bytes = 4, .host_scratch_bytes = 1, .backend_scratch_bytes = 2 };
    const token = try T.reserve(port, value);
    defer port.release_admission(port.context, token);
    const expected: u64 = if (@import("builtin").os.tag == .macos) 40 else 14;
    try std.testing.expectEqual(expected, metadata_fixture.payloadUsage(&owner.manager));
    try std.testing.expectEqual(@as(u64, 30), owner.manager.sliceStats(.inference_model_residency).used_bytes);
    try std.testing.expectEqual(@as(u64, 7), owner.manager.sliceStats(.inference_kv_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 3), owner.manager.sliceStats(.inference_scratch_working_set).used_bytes);
}

test "worker node memory embedded growth refuses callback contention before changing ownership" {
    const T = LiteBudgetTest;
    const owner = try createLeaseFixture(100);
    defer owner.releaseBaseReference();
    const port = owner.resourceBudget();
    {
        try std.testing.expect(owner.lease_mutex.tryLock());
        defer owner.lease_mutex.unlock();
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, T.reserve(port, T.amounts(1, 0, 0)));
        try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&owner.manager));
    }
    {
        try std.testing.expect(owner.observer_mutex.tryLock());
        defer owner.observer_mutex.unlock();
        try std.testing.expectEqual(@as(u8, 0), port.observe_tokenizer_cache(port.context, 1, 0, 10));
        try std.testing.expectEqual(@as(u8, 0), port.adjust_transient_host.?(port.context, 1, 0, 10));
        try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&owner.manager));
    }
    try std.testing.expectEqual(@as(u8, 1), port.adjust_transient_host.?(port.context, 1, 0, 10));
    try std.testing.expectEqual(@as(u8, 1), port.adjust_transient_host.?(port.context, 1, 10, 0));
}

test "worker node memory embedded detached payload keeps service and charge until physical free" {
    const T = LiteBudgetTest;
    const owner = try createLeaseFixture(100);
    var base_live = true;
    defer if (base_live) owner.releaseBaseReference();
    const port = owner.resourceBudget();
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    var anchor_live = true;
    defer if (anchor_live) budget.releaseOwner();
    try budget.configureExternal(.{ .context = port.context, .retain = port.retain_context, .release = port.release_context, .adjust = port.adjust_transient_host.? });
    const alloc = budget.retainedAllocator();
    const bytes = try alloc.alloc(u8, 60);
    var bytes_live = true;
    defer if (bytes_live) alloc.free(bytes);
    var token = try T.reserve(port, T.amounts(40, 0, 0));
    defer if (token != 0) port.release_admission(port.context, token);
    owner.releaseBaseReference();
    base_live = false;
    budget.releaseOwner();
    anchor_live = false;
    try std.testing.expectEqual(@as(usize, 1), owner.references.load(.acquire));
    try std.testing.expectEqual(@as(u64, 100), metadata_fixture.payloadUsage(&owner.manager));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, T.reserve(port, T.amounts(1, 0, 0)));
    port.release_admission(port.context, token);
    token = 0;
    try std.testing.expectEqual(@as(u64, 60), metadata_fixture.payloadUsage(&owner.manager));
    alloc.free(bytes); // Physical buffer, budget object, then final service capability.
    bytes_live = false;
}

test "worker node memory checked batch admission distinguishes physical refusal from legacy pressure" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 }, .identity_allocator = failing.allocator() });
    defer manager.deinit(std.testing.allocator);
    const amounts = [_]resources.SliceAmount{.{ .slice = .inference_model_residency, .bytes = 10 }};
    try std.testing.expectError(error.OutOfMemory, manager.reserveBatchCheckedWithHostCharge(&amounts, 10));
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.hard_limit_rejections);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, manager.reserveBatchClassifiedWithHostCharge(&amounts, 10));
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    try std.testing.expect(manager.snapshot().memory.hard_limit_rejections > 0);
}

test "worker node memory embedded batch transfer preserves shared capacity and independent tokens" {
    const T = LiteBudgetTest;
    const owner = try createLeaseFixture(100);
    defer owner.releaseBaseReference();
    const port = owner.resourceBudget();
    var source = try T.reserve(port, T.amounts(10, 60, 30));
    defer port.release_admission(port.context, source);
    var destination = try T.reserve(port, T.amounts(0, 0, 0));
    defer port.release_admission(port.context, destination);
    const transfer = port.transfer_admission.?;
    const before = owner.manager.snapshot();
    const epoch_port = owner.manager.memoryParentPort();
    const epoch = epoch_port.epoch_fn(epoch_port.ptr);
    try std.testing.expect(owner.lease_mutex.tryLock());
    const busy = transfer(port.context, source, destination, &T.amounts(0, 20, 10));
    owner.lease_mutex.unlock();
    try std.testing.expectEqual(error.ResourceTemporarilyUnavailable, T.bridge.errorFromStatus(busy));
    try std.testing.expect(!transfer(port.context, source, destination, &T.amounts(0, 61, 10)).isOk());
    try std.testing.expect(!transfer(port.context, source, source, &T.amounts(0, 0, 0)).isOk());
    try std.testing.expect(!transfer(port.context, source, 0, &T.amounts(0, 0, 0)).isOk());
    try std.testing.expectEqualDeep(before, owner.manager.snapshot());
    try std.testing.expect(transfer(port.context, source, destination, &T.amounts(0, 20, 10)).isOk());
    try std.testing.expectEqualDeep(before, owner.manager.snapshot());
    try std.testing.expectEqual(epoch, epoch_port.epoch_fn(epoch_port.ptr));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, T.reserve(port, T.amounts(1, 0, 0)));
    port.release_admission(port.context, source);
    const stale = source;
    source = 0;
    try std.testing.expectEqual(@as(u64, 30), metadata_fixture.payloadUsage(&owner.manager));
    try std.testing.expect(!transfer(port.context, stale, destination, &T.amounts(0, 0, 0)).isOk());
    try std.testing.expectEqual(@as(u64, 30), metadata_fixture.payloadUsage(&owner.manager));
    port.release_admission(port.context, destination);
    destination = 0;
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&owner.manager));
}
