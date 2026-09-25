// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Actual process death at native storage boundaries, including real torn
//! records and injected syscall failures. Process restart cannot simulate a
//! power loss; DATA/quorum/log-replacement qualification is also separate.
const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const db_mod = @import("db/db.zig");
const DB = db_mod.DB;
const resource_manager = @import("resource_manager.zig");
const abi = @import("kernel_owner_abi").completion_pool;
const Status = @import("runtime_failure_abi").Status;
const codec = @import("lsm_backend/completion_entry.zig");
const native = @import("lsm_backend/completion_runtime.zig");
const storage_io = @import("lsm_backend/storage_io.zig");
const Stage = enum {
    accepted,
    wal,
    manifest,
    partial_wal,
    wal_sync,
    partial_manifest,
    manifest_sync,
    sst_sync,
    sst_rename,
    sst_directory_sync,

    fn isFault(self: Stage) bool {
        return switch (self) {
            .accepted, .wal, .manifest => false,
            else => true,
        };
    }
};
const settings: @import("../common/table_storage.zig").Settings = .{ .transaction_recovery = .{
    .protocol_version = 1,
    .max_count = 4,
    .max_bytes = 1024 * 1024,
    .max_transaction_bytes = 64 * 1024,
    .completion_protocol_version = 1,
    .profile_version = 1,
} };
const logical_id: [16]u8 = @splat(214);

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
            .group_id = 2,
            .node_id = 7,
            .capacity = 4,
            .generation = 1,
            .incarnation = @splat(31),
            .policy_digest = @import("../metadata/completion_activation.zig").policyDigest(settings.transaction_recovery.?),
        },
        .table_id = 1,
        .range_id = 3,
        .schema_catalog_digest = try @import("../common/completion_catalog_digest.zig").digest(alloc, "", "", "{}"),
    };
}

const CrashPoint = struct {
    var ready_path: []const u8 = "";
    var stage: Stage = .accepted;
    var fault_hit: bool = false;

    fn fault(point: storage_io.CompletionIoFault, path: []const u8) bool {
        if (fault_hit) return false;
        const matches = switch (stage) {
            .partial_wal => point == .partial_append and std.mem.endsWith(u8, path, ".log"),
            .wal_sync => point == .append_sync and std.mem.endsWith(u8, path, ".log"),
            .partial_manifest => point == .partial_append and std.mem.endsWith(u8, path, ".journal"),
            .manifest_sync => point == .append_sync and std.mem.endsWith(u8, path, ".journal"),
            .sst_sync => point == .atomic_file_sync and std.mem.endsWith(u8, path, ".tbl"),
            .sst_rename => point == .atomic_rename and std.mem.endsWith(u8, path, ".tbl"),
            .sst_directory_sync => point == .atomic_directory_sync and std.mem.endsWith(u8, path, ".tbl"),
            else => false,
        };
        fault_hit = matches;
        return matches;
    }

    fn hold() noreturn {
        std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = ready_path, .data = "ready" }) catch std.process.exit(17);
        // No DB.close, error unwind, checkpoint or publication cleanup is
        // allowed between this marker and the parent's forcible termination.
        while (true) platform.time.sleepNs(100 * std.time.ns_per_ms);
    }
    fn stop() bool {
        hold();
    }
};

fn child(root: []const u8, stage: Stage, begin: bool) !void {
    const alloc = std.testing.allocator;
    var resources = resource_manager.ResourceManager.init(.{ .identity_allocator = alloc });
    defer resources.deinit(alloc);
    try resources.configureTransactionCompletion(1024 * 1024);
    var db = try DB.open(alloc, root, options(&resources));
    defer db.close();
    const identity = try binding(alloc);
    try db.installCompletionBinding(identity, "", "", "{}", settings);
    const lease = try db.acquireCompletionLease(2, 7);
    defer lease.vtable.release(lease.context);
    const startup: abi.DurableLog = .{ .mode = .startup_complete };
    try std.testing.expectEqual(Status.ok, lease.vtable.reconcile_durable.?(lease.context, &startup));
    const participant = try @import("../api/distributed_txn.zig").participantIdForGroup(alloc, "docs", 2);
    defer alloc.free(participant);
    const wire = if (begin) try db.compileReplicatedBegin(alloc, .{
        .txn_id = logical_id,
        .timestamp = 100,
        .created_at = 90,
        .topology_epoch = 1,
        .participants = &.{participant},
        .coordinator = true,
        .retain_terminal = true,
    }, .{ .term = 0, .index = 0 }) else try db.compileReplicatedMutation(alloc, .{
        .writes = &.{.{ .key = "doc", .value = "{\"value\":7}" }},
        .timestamp_ns = 100,
    }, .{ .term = 0, .index = 0 });
    defer alloc.free(wire);
    const wire_path = try std.fmt.allocPrint(alloc, "{s}.candidate", .{root});
    defer alloc.free(wire_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = wire_path, .data = wire });
    const ready_path = try std.fmt.allocPrint(alloc, "{s}.ready", .{root});
    defer alloc.free(ready_path);
    CrashPoint.ready_path = ready_path;
    const payloads = [_]abi.Bytes{.{ .ptr = wire.ptr, .len = wire.len }};
    const proposal: abi.Check = .{ .kind = .proposal, .new_work_allowed = 1, .state = .{ .term = 1, .applied_term_known = 1 }, .proposals = .{ .ptr = &payloads, .len = 1 } };
    var result: abi.CheckResult = undefined;
    try std.testing.expectEqual(Status.ok, lease.vtable.check(lease.context, &proposal, &result));
    lease.vtable.proposal_result(lease.context, &.{ .state = proposal.state, .first_index = 1, .last_index = 1, .payloads = proposal.proposals });
    const pool = db.core.primary_store_owner.lsmBackend().?.completion_pool.?;
    const accepted_before = if (stage.isFault()) try std.Io.Dir.cwd().readFileAlloc(std.testing.io, pool.accepted_paths[0], alloc, .limited(1024 * 1024)) else null;
    defer if (accepted_before) |bytes| alloc.free(bytes);
    switch (stage) {
        .accepted => CrashPoint.hold(),
        .wal => native.test_after_wal = CrashPoint.stop,
        .manifest => native.test_after_manifest = CrashPoint.stop,
        else => {
            CrashPoint.stage = stage;
            CrashPoint.fault_hit = false;
            storage_io.test_completion_io_fault = CrashPoint.fault;
        },
    }
    defer {
        native.test_after_wal = null;
        native.test_after_manifest = null;
        storage_io.test_completion_io_fault = null;
    }
    const applied = lease.vtable.apply_accepted.?(lease.context, 1, 1, payloads[0]);
    if (stage.isFault()) {
        try std.testing.expect(CrashPoint.fault_hit);
        try std.testing.expect(applied != .ok);
        // A failed attempt must fence same-process retries and must not consume
        // the accepted sidecar. Restart, not a fresh ordinary allocation, owns
        // repair of a torn record or uncertain publication.
        storage_io.test_completion_io_fault = null;
        try std.testing.expect(lease.vtable.apply_accepted.?(lease.context, 1, 1, payloads[0]) != .ok);
        try std.testing.expect(pool.failed);
        // Pool adoption names its live Slot phase "prepared" even for a
        // single-phase mutation; it is not a transaction prepare or durability
        // claim. The failed slot and exact accepted sidecar must both survive.
        try std.testing.expectEqual(.prepared, pool.cells[0].phase);
        try std.testing.expect(pool.cells[0].slot.attempted and !pool.cells[0].slot.durable);
        const accepted_after = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, pool.accepted_paths[0], alloc, .limited(1024 * 1024));
        defer alloc.free(accepted_after);
        try std.testing.expectEqualSlices(u8, accepted_before.?, accepted_after);
        CrashPoint.hold();
    }
    return error.CrashBoundaryNotReached;
}

test "workload admission physical completion process kill preserves accepted document and begin obligations" {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .wasi or builtin.os.tag == .windows) return error.SkipZigTest;
    if (platform.env.getenv("ANTFLY_COMPLETION_CRASH_ROOT")) |root| {
        const stage = std.meta.stringToEnum(Stage, platform.env.getenv("ANTFLY_COMPLETION_CRASH_STAGE") orelse return error.InvalidArgument) orelse return error.InvalidArgument;
        const begin = std.mem.eql(u8, platform.env.getenv("ANTFLY_COMPLETION_CRASH_KIND") orelse return error.InvalidArgument, "begin");
        return child(root, stage, begin);
    }
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const exe = try std.process.executablePathAlloc(io, alloc);
    defer alloc.free(exe);
    for ([_]bool{ false, true }) |begin| {
        for (std.enums.values(Stage)) |stage| {
            errdefer std.debug.print("completion process recovery failed: kind={s}, stage={s}\n", .{ if (begin) "begin" else "document", @tagName(stage) });
            var tmp = try @import("../common/test_directory.zig").TestDirectory.init("completion-process-kill");
            defer tmp.cleanup();
            const root = tmp.path();
            const ready_path = try std.fmt.allocPrint(alloc, "{s}.ready", .{root});
            defer alloc.free(ready_path);
            const wire_path = try std.fmt.allocPrint(alloc, "{s}.candidate", .{root});
            defer alloc.free(wire_path);
            var env = try std.testing.environ.createMap(alloc);
            defer env.deinit();
            try env.put("ANTFLY_COMPLETION_CRASH_ROOT", root);
            try env.put("ANTFLY_COMPLETION_CRASH_STAGE", @tagName(stage));
            try env.put("ANTFLY_COMPLETION_CRASH_KIND", if (begin) "begin" else "document");
            var process = try std.process.spawn(io, .{
                .argv = &.{ exe, "--test-filter", "physical completion process kill preserves accepted" },
                .environ_map = &env,
                .stdin = .ignore,
                .stdout = .ignore,
                .stderr = .inherit,
            });
            defer process.kill(io);
            var ready = false;
            for (0..600) |_| {
                std.Io.Dir.cwd().access(io, ready_path, .{}) catch {
                    platform.time.sleepNs(50 * std.time.ns_per_ms);
                    continue;
                };
                ready = true;
                break;
            }
            try std.testing.expect(ready);
            process.kill(io);
            const wire = try std.Io.Dir.cwd().readFileAlloc(io, wire_path, alloc, .limited(codec.max_wire_bytes + 1));
            defer alloc.free(wire);
            var entry = try codec.decode(alloc, wire);
            defer entry.deinit();
            // The process has terminated and released its kernel locks. Open
            // solely from trusted local installation state, with admission off.
            // Reopen a second time after recovery has checkpointed: a repaired
            // torn journal must not hide later publication on the next restart.
            for (0..2) |restart| {
                var resources = resource_manager.ResourceManager.init(.{ .identity_allocator = alloc });
                defer resources.deinit(alloc);
                try resources.configureTransactionCompletion(1024 * 1024);
                const identity = try binding(alloc);
                var config = options(&resources);
                config.durable_completion_authority = .raft_apply;
                config.durable_completion_enabled = false;
                config.completion_pool_config = (try DB.completionInstallationPreflight(alloc, io, root, identity, "", "", "{}")).?;
                var db = try DB.open(alloc, root, config);
                defer db.close();
                try db.installCompletionBinding(identity, "", "", "{}", settings);
                const lease = try db.acquireCompletionLease(2, 7);
                defer lease.vtable.release(lease.context);
                var cells: abi.DurableCells = undefined;
                try std.testing.expectEqual(Status.ok, lease.vtable.durable_cells.?(lease.context, &cells));
                if (restart == 0 and (stage == .accepted or stage == .partial_wal))
                    try std.testing.expectEqual(@as(u32, 1), cells.count);
                if (restart == 1) {
                    try std.testing.expectEqual(@as(u32, 0), cells.count);
                    var persisted: abi.Progress = undefined;
                    try std.testing.expectEqual(Status.ok, lease.vtable.progress.?(lease.context, &persisted));
                    try std.testing.expectEqual(@as(u64, 1), persisted.term);
                    try std.testing.expectEqual(@as(u64, 1), persisted.index);
                    try std.testing.expectEqualSlices(u8, &entry.digest, &persisted.payload_digest);
                }
                if (cells.count != 0) {
                    try std.testing.expectEqual(@as(u32, 1), cells.count);
                    var proof: abi.DurableLog = .{ .mode = .startup_complete, .last_index = 1, .commit_index = 1, .count = 1 };
                    proof.observations[0] = .{ .expected = cells.cells[0].identity, .present = 1, .observed_term = 1, .observed_digest = entry.digest };
                    try std.testing.expectEqual(Status.ok, lease.vtable.reconcile_durable.?(lease.context, &proof));
                }
                const payload: abi.Bytes = .{ .ptr = wire.ptr, .len = wire.len };
                try std.testing.expectEqual(Status.ok, lease.vtable.apply_accepted.?(lease.context, 1, 1, payload));
                try std.testing.expectEqual(Status.ok, lease.vtable.apply_accepted.?(lease.context, 1, 1, payload));
                for (entry.entry.prepare_operations, 0..) |op, i| {
                    const overwritten = for (entry.entry.prepare_operations[i + 1 ..]) |later| {
                        if (std.mem.eql(u8, op.key, later.key)) break true;
                    } else false;
                    if (overwritten) continue;
                    const actual = try db.core.getStoreValue(alloc, op.key);
                    defer if (actual) |value| alloc.free(value);
                    if (op.kind == .put) {
                        try std.testing.expectEqualSlices(u8, op.value, actual orelse return error.TestUnexpectedResult);
                    } else try std.testing.expect(actual == null);
                }
                try std.testing.expectError(error.TxnNotFound, db.getTransactionStatus(entry.entry.txn_id));
                if (begin) {
                    try std.testing.expectEqual(@import("transactions.zig").TxnStatus.pending, try db.getTransactionStatus(logical_id));
                } else {
                    const value = (try db.get(alloc, "doc")).?;
                    defer alloc.free(value);
                    try std.testing.expectEqualStrings("{\"value\":7}", value);
                }
                var progress: abi.Progress = undefined;
                try std.testing.expectEqual(Status.ok, lease.vtable.progress.?(lease.context, &progress));
                try std.testing.expectEqual(@as(u64, 1), progress.term);
                try std.testing.expectEqual(@as(u64, 1), progress.index);
                try std.testing.expectEqualSlices(u8, &entry.digest, &progress.payload_digest);
            }
        }
    }
}
