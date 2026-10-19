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
const owner = @import("remote_attempt_owner.zig");
const worker = @import("../common/remote_worker.zig");
const durable = @import("../common/remote_fence_store.zig");
const replies = @import("../common/remote_attempt_reply.zig");
const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

const Fixture = struct {
    resolves: usize = 0,
    activations: usize = 0,
    refuse_activation: bool = false,
    active: bool = false,
    fn resolve(raw: *anyopaque) !durable.InitializationGrant {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.resolves += 1;
        return .{ .id = @splat(9), .phase = if (self.active) .active else .prepared };
    }
    fn activate(raw: *anyopaque, _: durable.InitializationId) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.activations += 1;
        if (self.refuse_activation) return error.TestActivationRefused;
        self.active = true;
    }
    fn initialization(self: *@This(), scheduler: *scheduling.Scheduler) owner.Initialization {
        return .{ .authority = .{ .context = self, .resolve = resolve }, .activate = activate, .scheduler = scheduler };
    }
};
const local: @import("../common/remote_membership.zig").Local = .{
    .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
    .node_id = 9,
    .store_id = 13,
    .root_incarnation = 22,
    .reporter_incarnation = 17,
};
const targets = [_]owner.Target{.{ .local = local, .base_uri = "http://127.0.0.1:1", .max_attempts = 2, .max_bytes = 1024 }};
fn config(path: []const u8) owner.Bootstrap {
    return .{ .root_path = path, .local = local, .targets = &targets, .worker_limits = .{ .coordinators = 2, .attempts = 4, .per_coordinator_attempts = 2, .duplicate_window = 2, .bytes = 4096, .per_coordinator_bytes = 1024, .resource_references = 2 }, .max_outbound_attempts = 4, .max_outbound_bytes = 4096 };
}
fn signing() !@import("antfly_local_sources").storage_db_root_signing_identity.State {
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    return .{ .root_incarnation = local.root_incarnation, .seed = @splat(7), .public_key = pair.public_key.toBytes() };
}

test "remote owner baseline refuses before allocation and journal initialization" {
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .max_bytes = 32 << 20, .control_bytes = 8 << 20 });
    defer node.deinit();
    const cfg = config("/unused-owner-baseline-path");
    const bytes = try owner.State.reservationBytes(cfg);
    var held = try node.scheduler.reserveControlBaseline(node.scheduler.config.control_bytes / 4 - bytes + 1);
    defer held.release();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var fixture: Fixture = .{};
    const key = try signing();
    const authority = replies.Authority{ .local = local, .public_key = key.public_key };
    try std.testing.expectError(error.CleanupCapacityUnavailable, owner.State.create(failing.allocator(), failing.allocator(), std.testing.io, cfg, key, authority, &.{authority}, fixture.initialization(&node.scheduler)));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), fixture.resolves);
    try std.testing.expectEqual(held.bytes, node.scheduler.stats().control_retained_bytes);
    held.release();
    try std.testing.expectError(error.OutOfMemory, owner.State.create(failing.allocator(), failing.allocator(), std.testing.io, cfg, key, authority, &.{authority}, fixture.initialization(&node.scheduler)));
    try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().control_retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), fixture.resolves);
}

test "remote owner baseline survives initialized lifetime and rolls back late bootstrap failure" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |refuse| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
        defer alloc.free(path);
        var node = try scheduling.NodeOwner.init(.{ .slots = 2, .max_bytes = 32 << 20, .control_bytes = 8 << 20 });
        defer node.deinit();
        var fixture: Fixture = .{ .refuse_activation = refuse };
        const key = try signing();
        const authority = replies.Authority{ .local = local, .public_key = key.public_key };
        if (refuse) {
            try std.testing.expectError(error.TestActivationRefused, owner.State.create(alloc, alloc, std.testing.io, config(path), key, authority, &.{authority}, fixture.initialization(&node.scheduler)));
        } else {
            const state = try owner.State.create(alloc, alloc, std.testing.io, config(path), key, authority, &.{authority}, fixture.initialization(&node.scheduler));
            var owned = true;
            defer if (owned) state.destroy();
            try std.testing.expectEqual(try owner.State.reservationBytes(config(path)), state.reservedBytes());
            const registry = &state.registry;
            const store = &state.store;
            const backing_bytes = @sizeOf(owner.State) + state.slots.len * @sizeOf(@TypeOf(state.slots[0])) +
                state.destinations.len * @sizeOf(@TypeOf(state.destinations[0])) +
                registry.coordinators.len * @sizeOf(@TypeOf(registry.coordinators[0])) +
                registry.records.len * @sizeOf(@TypeOf(registry.records[0])) +
                registry.entries.len * @sizeOf(@TypeOf(registry.entries[0])) +
                store.floors.len * @sizeOf(@TypeOf(store.floors[0])) +
                store.outgoing_destinations.len * @sizeOf(@TypeOf(store.outgoing_destinations[0])) + store.buffer.len;
            try std.testing.expectEqual(@as(u64, backing_bytes), state.reservedBytes());
            try std.testing.expectEqual(state.reservedBytes() + state.endpoint_view.reservedBytes() + state.control_admission.reservedBytes(), node.scheduler.stats().control_retained_bytes);
            try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
            try std.testing.expect(state.stats().recovery_complete);
            owned = false;
            state.destroy();
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.activations);
        try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().control_retained_bytes);
        try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().retained_bytes);
    }
}

test "remote owner baseline metadata sizes reject invalid and overflowing envelopes" {
    try std.testing.expectError(error.InvalidRemoteAttemptConfig, durable.Store.metadataBytes(0));
    var cfg = config(".");
    cfg.worker_limits.duplicate_window = std.math.maxInt(usize);
    try std.testing.expectError(error.InvalidRemoteAttemptConfig, owner.State.reservationBytes(cfg));
    const normal = config(".");
    const bytes = try owner.State.reservationBytes(normal);
    try std.testing.expect(bytes >= @sizeOf(owner.State) + try worker.Registry.metadataBytes(normal.worker_limits) + try durable.Store.metadataBytes(normal.worker_limits.coordinators));
}

test "remote owner baseline default node metadata fits protected control envelope" {
    const cfg: owner.Bootstrap = .{ .root_path = ".", .local = local, .targets = &targets };
    const extra = try std.math.add(u64, try owner.State.reservationBytes(cfg), try std.math.add(u64, try @import("remote_capability_publication.zig").Cache.reservationBytes(targets.len), try std.math.add(u64, try @import("remote_endpoint_view.zig").View.reservationBytes(&targets), try @import("remote_endpoint_source.zig").Source.reservationBytes(targets.len))));
    const policy = @import("antfly_local_sources").common_config.default_runnable_policy;
    try @import("retained_read_retirement.zig").validateNodeCapacity(policy, .{}, extra);
    try std.testing.expect(policy.max_bytes - policy.control_bytes >= 7 << 20);
    const small = try policy.resolveCapacity(.{ .millicpus = 2000 }, .{ .limit_bytes = 64 << 20 });
    try std.testing.expectEqual(@as(u64, 2 << 20), small.max_bytes);
    try std.testing.expectEqual(@as(u64, 1 << 20), small.control_bytes);
    const fixed: scheduling.Config = .{ .control_bytes = 1 << 20 };
    try std.testing.expectEqualDeep(fixed, try fixed.resolveCapacity(.{ .millicpus = 2000 }, .{ .limit_bytes = 1 }));
}

fn createRecoveryOwner(alloc: std.mem.Allocator, node: *scheduling.NodeOwner, path: []const u8, fixture: *Fixture, reporter: u64) !*owner.State {
    var cfg = config(path);
    cfg.local.reporter_incarnation = reporter;
    var target = targets[0];
    target.local = cfg.local;
    cfg.targets = &.{target};
    const key = try signing();
    const authority = replies.Authority{ .local = cfg.local, .public_key = key.public_key };
    return owner.State.create(alloc, alloc, std.testing.io, cfg, key, authority, &.{authority}, fixture.initialization(&node.scheduler));
}
fn acknowledgeForRecoveryTest(state: *owner.State) !void {
    // Inject only remote fencing evidence here; production authenticates the
    // corresponding ACK through the signed control/TCP contracts.
    try state.ledger.acknowledgeFence(0, .{ .coordinator = state.ledger.coordinator, .generation = state.ledger.generation, .worker_incarnation = local.root_incarnation });
}

test "remote owner recovery clean joined checkpoint survives restart without clearing publication history" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(path);
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .max_bytes = 32 << 20, .control_bytes = 8 << 20 });
    defer node.deinit();
    var fixture: Fixture = .{};
    const first = try createRecoveryOwner(alloc, &node, path, &fixture, 17);
    var first_owned = true;
    defer if (first_owned) first.destroy();
    try acknowledgeForRecoveryTest(first);
    const id = try first.ledger.reserve(0, 9, 8);
    try first.store.prepareDispatch(&first.ledger, id);
    try std.testing.expect(try first.ledger.terminal(id, local.root_incarnation));
    try std.testing.expect((try first.store.initializationState()).publication_started);
    const old_generation = first.ledger.generation;
    const incoming_coordinator = try @import("../common/remote_membership.zig").coordinatorIdentity(local.metadata_incarnation, 10);
    try first.store.advance(incoming_coordinator, 5);
    first_owned = false;
    first.destroy();
    const second = try createRecoveryOwner(alloc, &node, path, &fixture, 18);
    var second_owned = true;
    defer if (second_owned) second.destroy();
    try std.testing.expect(second.registry.recoveryComplete());
    try std.testing.expect(second.client(alloc, .{ .secret = "test-recovery-key", .issuer = "cluster" }).reads_ready);
    try std.testing.expect((try second.store.initializationState()).publication_started);
    try std.testing.expect(second.ledger.generation > old_generation);
    try std.testing.expectEqual(@as(u64, 5), try second.store.floor(incoming_coordinator));
    var records: [2]?@import("../common/remote_attempts.zig").WorkerRecord = undefined;
    var restored_gate = try second.store.restoreGate(incoming_coordinator, &records);
    const old_attempt: @import("../common/remote_attempts.zig").Id = .{ .coordinator = incoming_coordinator, .generation = 5, .operation = 9, .sequence = 1 };
    try std.testing.expectEqual(@import("../common/remote_attempts.zig").WorkerGate.Status.closed, restored_gate.status(old_attempt));
    try std.testing.expectError(error.RemoteGenerationFenced, restored_gate.admit(old_attempt));
    try std.testing.expectError(error.RemoteGenerationNotReady, second.ledger.reserve(0, 10, 8));
    const authority = replies.Authority{ .local = second.registry.local, .public_key = second.signer.pair.public_key.toBytes() };
    try std.testing.expect(!@import("../common/remote_quiescence.zig").matches(&second.store, authority));
    // No new clean receipt: the startup destination fence is still pending.
    second_owned = false;
    second.destroy();
    const third = try createRecoveryOwner(alloc, &node, path, &fixture, 19);
    var third_owned = true;
    defer if (third_owned) third.destroy();
    try std.testing.expect(!third.registry.recoveryComplete());
    // A newly empty but recovery-blocked registry cannot manufacture proof,
    // even after all outgoing fences have been acknowledged.
    try acknowledgeForRecoveryTest(third);
    third_owned = false;
    third.destroy();
    const fourth = try createRecoveryOwner(alloc, &node, path, &fixture, 20);
    defer fourth.destroy();
    try std.testing.expect(!fourth.registry.recoveryComplete());
}

test "remote owner recovery uncertain outbound publication cannot produce clean restart authority" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(path);
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .max_bytes = 32 << 20, .control_bytes = 8 << 20 });
    defer node.deinit();
    var fixture: Fixture = .{};
    const first = try createRecoveryOwner(alloc, &node, path, &fixture, 17);
    var owned = true;
    defer if (owned) first.destroy();
    try acknowledgeForRecoveryTest(first);
    const id = try first.ledger.reserve(0, 9, 8);
    try first.store.prepareDispatch(&first.ledger, id);
    try std.testing.expectEqual(@as(usize, 1), first.ledger.snapshot().attempts);
    owned = false;
    first.destroy();
    const second = try createRecoveryOwner(alloc, &node, path, &fixture, 18);
    defer second.destroy();
    try std.testing.expect(!second.registry.recoveryComplete());
    try std.testing.expectEqual(@as(usize, 1), second.ledger.snapshot().startup_fences_remaining);
}

test "remote owner recovery receipt write failure leaves restarted admission closed" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(path);
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .max_bytes = 32 << 20, .control_bytes = 8 << 20 });
    defer node.deinit();
    var fixture: Fixture = .{};
    const first = try createRecoveryOwner(alloc, &node, path, &fixture, 17);
    var owned = true;
    defer if (owned) first.destroy();
    try acknowledgeForRecoveryTest(first);
    try first.store.markPublicationStarted();
    try tmp.dir.createDir(std.testing.io, "remote-quiescence.v1.pending", .default_dir);
    owned = false;
    first.destroy();
    const second = try createRecoveryOwner(alloc, &node, path, &fixture, 18);
    defer second.destroy();
    try std.testing.expect(!second.registry.recoveryComplete());
}

test "remote owner recovery late allocation refusal preserves the joined checkpoint for retry" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(path);
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .max_bytes = 32 << 20, .control_bytes = 8 << 20 });
    defer node.deinit();
    var fixture: Fixture = .{};
    var counted = std.testing.FailingAllocator.init(alloc, .{});
    const first = try createRecoveryOwner(counted.allocator(), &node, path, &fixture, 17);
    var owned = true;
    defer if (owned) first.destroy();
    const allocation_count = counted.alloc_index;
    try std.testing.expect(allocation_count > 1);
    try acknowledgeForRecoveryTest(first);
    try first.store.markPublicationStarted();
    owned = false;
    first.destroy();
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = allocation_count - 1 });
    try std.testing.expectError(error.OutOfMemory, createRecoveryOwner(failing.allocator(), &node, path, &fixture, 18));
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().control_retained_bytes);
    const retried = try createRecoveryOwner(alloc, &node, path, &fixture, 19);
    defer retried.destroy();
    try std.testing.expect(retried.registry.recoveryComplete());
    try std.testing.expect((try retried.store.initializationState()).publication_started);
}
