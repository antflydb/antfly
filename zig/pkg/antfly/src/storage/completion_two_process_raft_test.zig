// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! A real second process owns the leader Raft WAL. The follower acknowledges
//! only after persistReady returns; the leader is killed before follower
//! replay. This qualifies the Raft WAL/native apply seam, not DATA networking.
const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const raft = @import("raft_engine");
const raft_storage = @import("../raft/storage/mod.zig");
const WalReplicaState = raft_storage.WalReplicaState;
const db_mod = @import("db/db.zig");
const DB = db_mod.DB;
const resource_manager = @import("resource_manager.zig");
const abi = @import("kernel_owner_abi").completion_pool;
const Status = @import("runtime_failure_abi").Status;
const codec = @import("lsm_backend/completion_entry.zig");
const completion_bridge = @import("../data/completion_admission_bridge.zig");

const group_id: u64 = 302;
const peers = [_]u64{ 1, 2 };
const test_filter = "physical completion two-process Raft follower durable acknowledgement";
const settings: @import("../common/table_storage.zig").Settings = .{ .transaction_recovery = .{
    .protocol_version = 1,
    .max_count = 4,
    .max_bytes = 1024 * 1024,
    .max_transaction_bytes = 64 * 1024,
    .completion_protocol_version = 1,
    .profile_version = 1,
} };

fn options(resources: *resource_manager.ResourceManager) db_mod.OpenOptions {
    return .{
        .resource_manager = resources,
        .durable_completion_enabled = true,
        .durable_completion_authority = .standalone_local,
        .table_storage = settings,
        .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    };
}

fn binding(alloc: std.mem.Allocator) !abi.InstallBinding {
    return .{
        .identity = .{
            .group_id = group_id,
            .node_id = 2,
            .capacity = 4,
            .generation = 1,
            .incarnation = @splat(41),
            .policy_digest = @import("../metadata/completion_activation.zig").policyDigest(settings.transaction_recovery.?),
        },
        .table_id = 1,
        .range_id = 3,
        .schema_catalog_digest = try @import("../common/completion_catalog_digest.zig").digest(alloc, "", "", "{}"),
    };
}

fn path(alloc: std.mem.Allocator, root: []const u8, direction: []const u8, sequence: usize, suffix: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}/{s}-{d}{s}", .{ root, direction, sequence, suffix });
}

fn marker(root: []const u8, name: []const u8, data: []const u8) !void {
    const p = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ root, name });
    defer std.testing.allocator.free(p);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = p, .data = data });
}

fn exists(root: []const u8, name: []const u8) bool {
    const p = std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ root, name }) catch return false;
    defer std.testing.allocator.free(p);
    std.Io.Dir.cwd().access(std.testing.io, p, .{}) catch return false;
    return true;
}

fn waitFor(p: []const u8) !void {
    for (0..600) |_| {
        std.Io.Dir.cwd().access(std.testing.io, p, .{}) catch {
            platform.time.sleepNs(50 * std.time.ns_per_ms);
            continue;
        };
        return;
    }
    return error.TimedOut;
}

fn send(root: []const u8, direction: []const u8, sequence: usize, to: u64, messages: []const raft.core.Message) !void {
    const alloc = std.testing.allocator;
    const p = try path(alloc, root, direction, sequence, ".frame");
    defer alloc.free(p);
    const ready_path = try path(alloc, root, direction, sequence, ".ready");
    defer alloc.free(ready_path);
    const batches = [_]raft.runtime.transport_iface.GroupMessageBatch{.{ .group_id = group_id, .messages = messages }};
    const wire_codec = raft.runtime.BinaryCodec.codec();
    const frame = try wire_codec.encodePeerBatch(alloc, .{ .peer_id = to, .groups = &batches });
    defer wire_codec.freeFrame(alloc, frame);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = p, .data = frame.bytes });
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = ready_path, .data = "ready" });
}

fn receive(root: []const u8, direction: []const u8, sequence: usize) !raft.runtime.codec_iface.DecodedFrame {
    const alloc = std.testing.allocator;
    const p = try path(alloc, root, direction, sequence, ".frame");
    defer alloc.free(p);
    const ready_path = try path(alloc, root, direction, sequence, ".ready");
    defer alloc.free(ready_path);
    try waitFor(ready_path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, p, alloc, .limited(1024 * 1024));
    defer alloc.free(bytes);
    return raft.runtime.BinaryCodec.codec().decodeFrame(alloc, .{ .bytes = bytes, .media_type = "application/x-antflydb-raft-binary-v1" });
}

const Committed = struct { term: u64 = 0, index: u64 = 0 };

fn drain(
    raw: *raft.core.RawNode,
    state: *WalReplicaState,
    alloc: std.mem.Allocator,
    candidate: []const u8,
    committed: *Committed,
    guard: ?raft.runtime.completion_admission_iface.Guard,
) ![]raft.core.Message {
    var outgoing: std.ArrayList(raft.core.Message) = .empty;
    errdefer {
        for (outgoing.items) |*msg| msg.deinit(alloc);
        outgoing.deinit(alloc);
    }
    while (raw.hasReady()) {
        const ready = raw.ready();
        if (guard) |owned| try owned.check(raw.status(), .{ .ready = ready });
        // In particular, no append response can leave this process first.
        try state.groupStorage().persistReady(group_id, ready);
        for (ready.committed_entries) |entry| {
            if (entry.data.len == 0 and entry.index == 1) {
                try state.setDurableNoop(entry);
                try state.setAppliedIndex(entry.index);
            }
            if (std.mem.eql(u8, entry.data, candidate)) {
                committed.* = .{ .term = entry.term, .index = entry.index };
            }
        }
        for (ready.messages) |msg| {
            if (msg.to == 1 or msg.to == 2) try outgoing.append(alloc, try msg.clone(alloc));
        }
        raw.advance(ready);
    }
    return outgoing.toOwnedSlice(alloc);
}

fn openReplica(alloc: std.mem.Allocator, root: []const u8, id: u64) !struct { layout: raft_storage.ReplicaPathLayout, state: WalReplicaState } {
    var layout = try raft_storage.ReplicaPathLayout.initForReplica(alloc, root, group_id, id);
    errdefer layout.deinit(alloc);
    var state = try WalReplicaState.init(alloc, layout, .{});
    errdefer state.deinit();
    try state.seedConfStateIfEmpty(&peers);
    return .{ .layout = layout, .state = state };
}

fn rawNode(alloc: std.mem.Allocator, state: *WalReplicaState, id: u64) !raft.core.RawNode {
    return raft.core.RawNode.init(alloc, .{
        .id = id,
        .group_id = group_id,
        .peers = &peers,
        .election_tick = 5,
        .heartbeat_tick = 1,
        .pre_vote = false,
        .check_quorum = true,
        .applied = state.appliedIndex(),
    }, state.storage());
}

fn child(root: []const u8) !void {
    const alloc = std.testing.allocator;
    const candidate_path = try std.fmt.allocPrint(alloc, "{s}/candidate", .{root});
    defer alloc.free(candidate_path);
    const candidate = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, candidate_path, alloc, .limited(codec.max_wire_bytes + 1));
    defer alloc.free(candidate);
    var replica = try openReplica(alloc, root, 1);
    defer replica.layout.deinit(alloc);
    defer replica.state.deinit();
    var raw = try rawNode(alloc, &replica.state, 1);
    defer raw.deinit();
    try raw.campaign();
    var committed: Committed = .{};
    var proposed = false;
    for (0..300) |sequence| {
        const outbound = try drain(&raw, &replica.state, alloc, candidate, &committed, null);
        defer raft.core.message.freeMessages(alloc, outbound);
        try send(root, "leader", sequence, 2, outbound);
        if (committed.index != 0) {
            const proof = try std.fmt.allocPrint(alloc, "{d}:{d}", .{ committed.term, committed.index });
            defer alloc.free(proof);
            try marker(root, "leader-committed", proof);
        }
        const decoded = try receive(root, "follower", sequence);
        defer raft.runtime.BinaryCodec.codec().freeDecoded(alloc, decoded);
        switch (decoded) {
            .raft_peer_batch => |batch| for (batch.groups) |group| for (group.messages) |msg| try raw.step(msg),
            else => return error.UnexpectedFrame,
        }
        if (!proposed and raw.status().soft.role == .leader and raw.status().hard.commit_index >= 1 and exists(root, "follower-noop-applied")) {
            var index: ?u64 = null;
            try raw.proposeWithReceipt(candidate, &index);
            try std.testing.expect(index != null);
            proposed = true;
        } else raw.tick();
    }
    return error.LeaderDidNotCommit;
}

test "workload admission physical completion two-process Raft follower durable acknowledgement and leader kill" {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .wasi or builtin.os.tag == .windows) return error.SkipZigTest;
    if (platform.env.getenv("ANTFLY_COMPLETION_RAFT_CHILD")) |root| return child(root);
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = try @import("../common/test_directory.zig").TestDirectory.init("completion-two-process-raft");
    defer tmp.cleanup();
    const root = tmp.path();
    const db_path = try std.fmt.allocPrint(alloc, "{s}/db", .{root});
    defer alloc.free(db_path);
    var resources = resource_manager.ResourceManager.init(.{ .identity_allocator = alloc });
    defer resources.deinit(alloc);
    try resources.configureTransactionCompletion(1024 * 1024);
    const identity = try binding(alloc);
    var wire: []u8 = undefined;
    {
        var db = try DB.open(alloc, db_path, options(&resources));
        defer db.close();
        try db.installCompletionBinding(identity, "", "", "{}", settings);
        wire = try db.compileReplicatedMutation(alloc, .{
            .writes = &.{.{ .key = "doc", .value = "{\"value\":7}" }},
            .timestamp_ns = 100,
        }, .{ .term = 1, .index = 1 });
    }
    defer alloc.free(wire);
    try marker(root, "candidate", wire);
    const exe = try std.process.executablePathAlloc(io, alloc);
    defer alloc.free(exe);
    var env = try std.testing.environ.createMap(alloc);
    defer env.deinit();
    try env.put("ANTFLY_COMPLETION_RAFT_CHILD", root);
    var process = try std.process.spawn(io, .{
        .argv = &.{ exe, "--test-filter", test_filter },
        .environ_map = &env,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .inherit,
    });
    var killed = false;
    defer if (!killed) process.kill(io);
    var committed: Committed = .{};
    {
        var replica = try openReplica(alloc, root, 2);
        defer replica.layout.deinit(alloc);
        defer replica.state.deinit();
        var config = options(&resources);
        config.durable_completion_enabled = false;
        config.durable_completion_authority = .raft_apply;
        config.completion_pool_config = (try DB.completionInstallationPreflight(alloc, io, db_path, identity, "", "", "{}")).?;
        var db = try DB.open(alloc, db_path, config);
        defer db.close();
        try db.installCompletionBinding(identity, "", "", "{}", settings);
        const lease = try db.acquireCompletionLease(group_id, 2);
        const startup: abi.DurableLog = .{ .mode = .startup_complete };
        try std.testing.expectEqual(Status.ok, lease.vtable.reconcile_durable.?(lease.context, &startup));
        const guard = try completion_bridge.Bridge.createWithStorage(alloc, lease, group_id, 2, replica.state.storage());
        defer guard.detach();
        var raw = try rawNode(alloc, &replica.state, 2);
        defer raw.deinit();
        for (0..300) |sequence| {
            const decoded = try receive(root, "leader", sequence);
            defer raft.runtime.BinaryCodec.codec().freeDecoded(alloc, decoded);
            switch (decoded) {
                .raft_peer_batch => |batch| for (batch.groups) |group| for (group.messages) |msg| try raw.step(msg),
                else => return error.UnexpectedFrame,
            }
            const outbound = try drain(&raw, &replica.state, alloc, wire, &committed, guard);
            defer raft.core.message.freeMessages(alloc, outbound);
            if (replica.state.appliedIndex() >= 1 and !exists(root, "follower-noop-applied"))
                try marker(root, "follower-noop-applied", "ready");
            try send(root, "follower", sequence, 1, outbound);
            if (committed.index != 0 and exists(root, "leader-committed")) break;
        }
    }
    try std.testing.expect(committed.index != 0);
    const leader_proof_path = try std.fmt.allocPrint(alloc, "{s}/leader-committed", .{root});
    defer alloc.free(leader_proof_path);
    try waitFor(leader_proof_path);
    const leader_proof = try std.Io.Dir.cwd().readFileAlloc(io, leader_proof_path, alloc, .limited(64));
    defer alloc.free(leader_proof);
    const expected_proof = try std.fmt.allocPrint(alloc, "{d}:{d}", .{ committed.term, committed.index });
    defer alloc.free(expected_proof);
    try std.testing.expectEqualStrings(expected_proof, leader_proof);
    process.kill(io);
    killed = true;
    {
        var layout = try raft_storage.ReplicaPathLayout.initForReplica(alloc, root, group_id, 2);
        defer layout.deinit(alloc);
        var replay = try WalReplicaState.init(alloc, layout, .{});
        defer replay.deinit();
        var reopened = try rawNode(alloc, &replay, 2);
        defer reopened.deinit();
        try std.testing.expect(reopened.hasReady());
        const ready = reopened.ready();
        var found = false;
        for (ready.committed_entries) |entry| {
            if (!std.mem.eql(u8, entry.data, wire)) continue;
            try std.testing.expectEqual(committed.term, entry.term);
            try std.testing.expectEqual(committed.index, entry.index);
            found = true;
        }
        try std.testing.expect(found);
    }
    {
        var config = options(&resources);
        config.durable_completion_enabled = false;
        config.durable_completion_authority = .raft_apply;
        config.completion_pool_config = (try DB.completionInstallationPreflight(alloc, io, db_path, identity, "", "", "{}")).?;
        var db = try DB.open(alloc, db_path, config);
        defer db.close();
        try db.installCompletionBinding(identity, "", "", "{}", settings);
        const lease = try db.acquireCompletionLease(group_id, 2);
        defer lease.vtable.release(lease.context);
        var cells: abi.DurableCells = undefined;
        try std.testing.expectEqual(Status.ok, lease.vtable.durable_cells.?(lease.context, &cells));
        try std.testing.expectEqual(@as(u32, 1), cells.count);
        var proof: abi.DurableLog = .{ .mode = .startup_complete, .last_index = committed.index, .commit_index = committed.index, .count = 1 };
        var decoded_wire = try codec.decode(alloc, wire);
        defer decoded_wire.deinit();
        proof.observations[0] = .{ .expected = cells.cells[0].identity, .present = 1, .observed_term = committed.term, .observed_digest = decoded_wire.digest };
        try std.testing.expectEqual(Status.ok, lease.vtable.reconcile_durable.?(lease.context, &proof));
        const payload: abi.Bytes = .{ .ptr = wire.ptr, .len = wire.len };
        const applied = lease.vtable.apply_accepted.?(lease.context, committed.term, committed.index, payload);
        if (applied != .ok) std.debug.print("two-process accepted apply status: {s}\n", .{@tagName(applied)});
        try std.testing.expectEqual(Status.ok, applied);
        const value = (try db.get(alloc, "doc")).?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("{\"value\":7}", value);
        var progress: abi.Progress = undefined;
        try std.testing.expectEqual(Status.ok, lease.vtable.progress.?(lease.context, &progress));
        try std.testing.expectEqual(committed.term, progress.term);
        try std.testing.expectEqual(committed.index, progress.index);
        try std.testing.expectEqualSlices(u8, &decoded_wire.digest, &progress.payload_digest);
    }
}
