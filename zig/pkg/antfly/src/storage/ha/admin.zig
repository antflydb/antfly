// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! HA admin command facade.
//!
//! CLI commands, HTTP admin routes, and operators should delegate to this layer
//! rather than each assembling HA storage calls differently. The facade keeps
//! slot lifecycle, status, read freshness, sync durability, and fenced promotion
//! semantics aligned with the storage primitives.

const std = @import("std");
const Allocator = std.mem.Allocator;
const backup_manifest = @import("backup_manifest.zig");
const bootstrap = @import("bootstrap.zig");
const commit_gate = @import("commit_gate.zig");
const fencing = @import("fencing.zig");
const owner_job_gate = @import("owner_job_gate.zig");
const primary_mod = @import("primary.zig");
const read_gate = @import("read_gate.zig");
const rejoin = @import("rejoin.zig");
const replication_api = @import("replication_api.zig");
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const slot_store = @import("slot_store.zig");
const standby_mod = @import("standby.zig");
const status = @import("status.zig");
const write_gate = @import("write_gate.zig");

var test_path_counter: u64 = 0;

pub const SlotAction = enum {
    create,
    pause,
    @"resume",
    drop,
};

pub const SlotRequest = struct {
    slot_name: []const u8,
    initial_lsn: ?u64 = null,
};

pub const SlotResult = union(SlotAction) {
    create: replication_api.CreateReplicationSlotResponse,
    pause: replication_api.SlotLifecycleResponse,
    @"resume": replication_api.SlotLifecycleResponse,
    drop: replication_api.SlotLifecycleResponse,
};

pub const FencedPromotionRequest = struct {
    fence: fencing.FenceRequest,
};

pub const FenceReceiptResult = struct {
    receipt: fencing.Receipt,

    pub fn deinit(self: *FenceReceiptResult, alloc: Allocator) void {
        fencing.freeReceipt(alloc, self.receipt);
        self.* = undefined;
    }
};

pub const FencedPromotionResult = struct {
    assessment: status.PromotionAssessment,
    promotion: standby_mod.PromotionResult,
    promoted_node_id: []const u8,
    fence_generation: u64,
    fence_token: []const u8,
    forced: bool,

    pub fn deinit(self: *FencedPromotionResult, alloc: Allocator) void {
        alloc.free(self.promoted_node_id);
        alloc.free(self.fence_token);
        self.* = undefined;
    }
};

pub fn identifyPrimary(primary: *const primary_mod.Primary) replication_api.IdentifySystemResponse {
    return replication_api.identifySystem(primary);
}

pub fn applySlotAction(primary: *primary_mod.Primary, action: SlotAction, request: SlotRequest) !SlotResult {
    return switch (action) {
        .create => .{ .create = try replication_api.createReplicationSlot(primary, .{
            .slot_name = request.slot_name,
            .initial_lsn = request.initial_lsn,
        }) },
        .pause => .{ .pause = try replication_api.pauseReplicationSlot(primary, .{
            .slot_name = request.slot_name,
        }) },
        .@"resume" => .{ .@"resume" = try replication_api.resumeReplicationSlot(primary, .{
            .slot_name = request.slot_name,
        }) },
        .drop => .{ .drop = try replication_api.dropReplicationSlot(primary, .{
            .slot_name = request.slot_name,
        }) },
    };
}

pub fn beginBaseBackup(primary: *primary_mod.Primary, request: primary_mod.BaseBackupStart) !primary_mod.BaseBackupStartResult {
    return try primary.beginBaseBackup(request);
}

pub fn endBaseBackup(primary: *primary_mod.Primary, manifest: backup_manifest.Manifest) !primary_mod.BaseBackupEndResult {
    return try primary.endBaseBackup(manifest);
}

pub fn bootstrapStandby(
    alloc: Allocator,
    standby: *standby_mod.Standby,
    manifest: backup_manifest.ManifestView,
    contents: []const backup_manifest.FileContent,
) !bootstrap.BootstrapResult {
    return try bootstrap.bootstrapFromManifest(alloc, standby, manifest, contents);
}

pub fn updateStandbyProgress(
    primary: *primary_mod.Primary,
    request: replication_api.StandbyStatusUpdateRequest,
) !replication_api.StandbyStatusUpdateResponse {
    return try replication_api.standbyStatusUpdate(primary, request);
}

pub fn primaryStatus(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    retention_policy: slot_store.RetentionPolicy,
    sync_policy: ?primary_mod.SyncPolicy,
) !status.PrimarySnapshot {
    return try status.primarySnapshot(alloc, primary, retention_policy, sync_policy);
}

pub fn standbyStatus(standby: *const standby_mod.Standby, upstream_lsn: ?u64) status.StandbySnapshot {
    return status.standbySnapshot(standby, upstream_lsn);
}

pub fn evaluateCommit(
    primary: *const primary_mod.Primary,
    target_lsn: u64,
    policy: primary_mod.SyncPolicy,
) !commit_gate.GateResult {
    return try commit_gate.evaluate(primary, target_lsn, policy);
}

pub fn evaluateStandbyRead(
    standby: *const standby_mod.Standby,
    request: read_gate.Request,
) !read_gate.Decision {
    return try read_gate.evaluateStandby(standby, request);
}

pub fn evaluatePrimaryWrite(
    primary: *const primary_mod.Primary,
    request: write_gate.Request,
) !write_gate.Decision {
    return try write_gate.evaluatePrimary(primary, request);
}

pub fn evaluateStandbyWrite(
    standby: *standby_mod.Standby,
    request: write_gate.Request,
) !write_gate.Decision {
    return try write_gate.evaluateStandby(standby, request);
}

pub fn evaluatePromotedPrimaryWrite(
    primary: *const primary_mod.Primary,
    handoff: standby_mod.PromotionHandoff,
    request: write_gate.Request,
) !write_gate.Decision {
    return try write_gate.evaluatePromotedPrimary(primary, handoff, request);
}

pub fn evaluatePrimaryOwnerJob(
    primary: *const primary_mod.Primary,
    request: owner_job_gate.Request,
) !owner_job_gate.Decision {
    return try owner_job_gate.evaluatePrimary(primary, request);
}

pub fn evaluateStandbyOwnerJob(
    standby: *standby_mod.Standby,
    request: owner_job_gate.Request,
) !owner_job_gate.Decision {
    return try owner_job_gate.evaluateStandby(standby, request);
}

pub fn evaluatePromotedPrimaryOwnerJob(
    primary: *const primary_mod.Primary,
    handoff: standby_mod.PromotionHandoff,
    request: owner_job_gate.Request,
) !owner_job_gate.Decision {
    return try owner_job_gate.evaluatePromotedPrimary(primary, handoff, request);
}

pub fn assessPromotion(
    standby: *const standby_mod.Standby,
    check: status.PromotionCheck,
) status.PromotionAssessment {
    return status.assessPromotion(standby, check);
}

pub fn assessPromotionWithFence(
    standby: *const standby_mod.Standby,
    receipt: fencing.Receipt,
) status.PromotionAssessment {
    return status.assessPromotionWithFence(standby, receipt);
}

pub fn acquirePromotionFence(
    alloc: Allocator,
    fence_store: *fencing.Store,
    request: fencing.FenceRequest,
) !FenceReceiptResult {
    const receipt = try fence_store.acquirePromotionFence(request);
    defer fencing.freeReceipt(fence_store.alloc, receipt);
    return .{ .receipt = try cloneReceiptAlloc(alloc, receipt) };
}

pub fn currentPromotionFence(
    alloc: Allocator,
    fence_store: *const fencing.Store,
) !?FenceReceiptResult {
    const receipt = (try fence_store.current(fence_store.alloc)) orelse return null;
    defer fencing.freeReceipt(fence_store.alloc, receipt);
    return .{ .receipt = try cloneReceiptAlloc(alloc, receipt) };
}

pub fn promoteWithFence(
    alloc: Allocator,
    fence_store: *fencing.Store,
    standby: *standby_mod.Standby,
    request: FencedPromotionRequest,
) !FencedPromotionResult {
    try validateFenceRequestForStandby(standby.identitySnapshot(), request.fence);
    const receipt = try fence_store.acquirePromotionFence(request.fence);
    defer fencing.freeReceipt(fence_store.alloc, receipt);
    return try promoteWithReceipt(alloc, standby, receipt);
}

pub fn promoteWithCurrentFence(
    alloc: Allocator,
    fence_store: *const fencing.Store,
    standby: *standby_mod.Standby,
) !FencedPromotionResult {
    const receipt = (try fence_store.current(fence_store.alloc)) orelse return error.FenceReceiptMissing;
    defer fencing.freeReceipt(fence_store.alloc, receipt);
    return try promoteWithReceipt(alloc, standby, receipt);
}

fn promoteWithReceipt(
    alloc: Allocator,
    standby: *standby_mod.Standby,
    receipt: fencing.Receipt,
) !FencedPromotionResult {
    try validateFenceReceiptForStandby(standby.identitySnapshot(), receipt);
    const assessment = status.assessPromotionWithFence(standby, receipt);
    if (!assessment.can_promote) return error.PromotionNotAllowed;

    const promoted_node_id = try alloc.dupe(u8, receipt.promoted_node_id);
    errdefer alloc.free(promoted_node_id);
    const token = try alloc.dupe(u8, receipt.token);
    errdefer alloc.free(token);
    const promotion = try standby.promote(receipt.promotionRequest());

    return .{
        .assessment = assessment,
        .promotion = promotion,
        .promoted_node_id = promoted_node_id,
        .fence_generation = receipt.generation,
        .fence_token = token,
        .forced = receipt.forced,
    };
}

fn validateFenceRequestForStandby(identity: standby_mod.Identity, request: fencing.FenceRequest) !void {
    try validateParentIdentityForStandby(identity, request.identity.cluster_id, request.identity.shard_id, request.identity.table_id, request.identity.timeline_id, request.identity.epoch);
}

fn validateFenceReceiptForStandby(identity: standby_mod.Identity, receipt: fencing.Receipt) !void {
    try validateParentIdentityForStandby(identity, receipt.identity.cluster_id, receipt.identity.shard_id, receipt.identity.table_id, receipt.parent_timeline_id, receipt.parent_epoch);
}

fn validateParentIdentityForStandby(
    standby_identity: standby_mod.Identity,
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
) !void {
    if (standby_identity.cluster_id != cluster_id) return error.WrongCluster;
    if (standby_identity.shard_id != shard_id) return error.WrongShard;
    if (standby_identity.table_id != table_id) return error.WrongTable;
    if (standby_identity.timeline_id != timeline_id) return error.WrongTimeline;
    if (standby_identity.epoch != epoch) return error.WrongEpoch;
}

pub fn assessFormerPrimaryRejoin(
    former: rejoin.FormerPrimaryState,
    receipt: ?fencing.Receipt,
    policy: rejoin.RejoinPolicy,
) rejoin.Assessment {
    return rejoin.assessFormerPrimary(former, receipt, policy);
}

pub fn rewindFormerPrimaryReplicationLog(
    alloc: Allocator,
    log: *replication_log.ReplicationLog,
    assessment: rejoin.Assessment,
) !rejoin.RewindResult {
    return try rejoin.rewindReplicationLog(alloc, log, assessment);
}

pub fn markFormerPrimaryForReseed(
    primary: *primary_mod.Primary,
    assessment: rejoin.Assessment,
) !rejoin.ReseedResult {
    if (assessment.action != .reseed) return error.RejoinReseedNotAllowed;
    primary.markSlotReseedRequired(assessment.former_node_id) catch |err| switch (err) {
        error.SlotNotFound => {
            try primary.createSlot(assessment.former_node_id, primary.lastLsn());
            try primary.markSlotReseedRequired(assessment.former_node_id);
        },
        else => return err,
    };
    return .{
        .node_id = assessment.former_node_id,
        .slot_name = assessment.former_node_id,
        .target_timeline_id = assessment.target_timeline_id,
        .target_epoch = assessment.target_epoch,
        .fork_lsn = assessment.fork_lsn,
        .former_last_lsn = assessment.former_last_lsn,
        .reseed_required = true,
        .base_backup_required = true,
    };
}

fn cloneReceiptAlloc(alloc: Allocator, receipt: fencing.Receipt) !fencing.Receipt {
    var cloned = fencing.Receipt{
        .identity = receipt.identity,
        .old_primary_id = try alloc.dupe(u8, receipt.old_primary_id),
        .promoted_node_id = &.{},
        .parent_timeline_id = receipt.parent_timeline_id,
        .parent_epoch = receipt.parent_epoch,
        .new_timeline_id = receipt.new_timeline_id,
        .new_epoch = receipt.new_epoch,
        .required_lsn = receipt.required_lsn,
        .observed_lsn = receipt.observed_lsn,
        .generation = receipt.generation,
        .forced = receipt.forced,
        .token = &.{},
        .reason = &.{},
    };
    errdefer fencing.freeReceipt(alloc, cloned);
    cloned.promoted_node_id = try alloc.dupe(u8, receipt.promoted_node_id);
    cloned.token = try alloc.dupe(u8, receipt.token);
    cloned.reason = try alloc.dupe(u8, receipt.reason);
    return cloned;
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,
    fence_wal: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
        alloc.free(self.fence_wal);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const primary_log = try allocPrintPath(alloc, name, "primary-log", nonce);
    defer alloc.free(primary_log);
    const primary_slots = try allocPrintPath(alloc, name, "primary-slots", nonce);
    defer alloc.free(primary_slots);
    const standby_log = try allocPrintPath(alloc, name, "standby-log", nonce);
    defer alloc.free(standby_log);
    const standby_progress = try allocPrintPath(alloc, name, "standby-progress", nonce);
    defer alloc.free(standby_progress);
    const fence_wal = try allocPrintPath(alloc, name, "fence-wal", nonce);
    defer alloc.free(fence_wal);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_wal) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .standby_log = try alloc.dupeZ(u8, standby_log),
        .standby_progress = try alloc.dupeZ(u8, standby_progress),
        .fence_wal = try alloc.dupeZ(u8, fence_wal),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-admin-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
}

fn testIdentity() standby_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

fn baseRecord(identity: standby_mod.Identity, lsn: u64, payload: []const u8) replication_record.Record {
    return .{
        .kind = .batch_mutation,
        .payload_codec = .raw,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = lsn,
        .previous_lsn = lsn - 1,
        .payload = payload,
    };
}

const ApplyCounter = struct {
    count: usize = 0,

    fn apply(ctx: *anyopaque, _: replication_record.RecordView) !void {
        const self: *ApplyCounter = @ptrCast(@alignCast(ctx));
        self.count += 1;
    }
};

test "storage.ha admin manages slot lifecycle and status" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "slots");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    const created = try applySlotAction(&primary, .create, .{ .slot_name = "standby-a", .initial_lsn = 1 });
    try std.testing.expectEqualStrings("standby-a", created.create.slot_name);
    try std.testing.expectEqual(@as(u64, 1), created.create.restart_lsn);

    _ = try updateStandbyProgress(&primary, .{
        .slot_name = "standby-a",
        .timeline_id = identity.timeline_id,
        .received_lsn = 2,
        .applied_lsn = 1,
    });

    var snapshot = try primaryStatus(alloc, &primary, .{}, null);
    defer snapshot.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), snapshot.current_lsn);
    try std.testing.expectEqual(@as(usize, 1), snapshot.slots.len);
    try std.testing.expectEqualStrings("standby-a", snapshot.slots[0].name);
    try std.testing.expectEqual(@as(u64, 1), snapshot.slots[0].apply_lag_lsn);

    const paused = try applySlotAction(&primary, .pause, .{ .slot_name = "standby-a" });
    try std.testing.expect(!paused.pause.active);
    const resumed = try applySlotAction(&primary, .@"resume", .{ .slot_name = "standby-a" });
    try std.testing.expect(resumed.@"resume".active);
    const dropped = try applySlotAction(&primary, .drop, .{ .slot_name = "standby-a" });
    try std.testing.expect(dropped.drop.dropped);
}

test "storage.ha admin seeds standby from base backup workflow" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "seed");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    _ = try primary.append(.{ .payload = "before-backup" });
    const started = try beginBaseBackup(&primary, .{
        .slot_name = "standby-a",
        .manifest_id = "base-0001",
    });
    try std.testing.expectEqual(@as(u64, 2), started.backup_lsn);
    try std.testing.expectEqual(@as(u64, 2), started.start_record_lsn);

    const files = [_]backup_manifest.FileEntry{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
        .{ .path = "sst/0001", .kind = .sstable, .size_bytes = 7, .crc32 = backup_manifest.crc32("sstable") },
    };
    const contents = [_]backup_manifest.FileContent{
        .{ .path = "manifest", .bytes = "manifest" },
        .{ .path = "sst/0001", .bytes = "sstable" },
    };
    const manifest = backup_manifest.Manifest{
        .identity = identity,
        .manifest_id = "base-0001",
        .backup_lsn = started.backup_lsn,
        .checkpoint_lsn = started.backup_lsn,
        .files = &files,
    };

    const ended = try endBaseBackup(&primary, manifest);
    try std.testing.expectEqual(started.backup_lsn, ended.backup_lsn);
    try std.testing.expectEqual(@as(u64, 3), ended.end_record_lsn);
    try std.testing.expectEqualStrings("base-0001", ended.manifest_id);

    const view = backup_manifest.ManifestView{
        .identity = manifest.identity,
        .manifest_id = manifest.manifest_id,
        .backup_lsn = manifest.backup_lsn,
        .checkpoint_lsn = manifest.checkpoint_lsn,
        .files = manifest.files,
        .flags = manifest.flags,
    };
    const bootstrapped = try bootstrapStandby(alloc, &standby, view, &contents);
    try std.testing.expectEqual(started.backup_lsn, bootstrapped.backup_lsn);
    try std.testing.expectEqual(started.backup_lsn, bootstrapped.checkpoint_lsn);
    try std.testing.expectEqualStrings("base-0001", bootstrapped.manifest_id);
    try std.testing.expectEqual(started.backup_lsn + 1, standby.nextReceiveLsn());
}

test "storage.ha admin exposes commit and read freshness decisions" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "gates");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    _ = try applySlotAction(&primary, .create, .{ .slot_name = "standby-a", .initial_lsn = 0 });
    const primary_write = try evaluatePrimaryWrite(&primary, .{ .expected_identity = identity });
    try std.testing.expect(primary_write.canWrite());
    try std.testing.expectEqual(write_gate.Action.allow_write, primary_write.action);

    const primary_job = try evaluatePrimaryOwnerJob(&primary, .{
        .kind = .derived_effect_writer,
        .expected_identity = identity,
    });
    try std.testing.expect(primary_job.canRun());
    try std.testing.expectEqual(owner_job_gate.Action.run, primary_job.action);

    const standby_write = try evaluateStandbyWrite(&standby, .{ .expected_identity = identity });
    try std.testing.expect(!standby_write.canWrite());
    try std.testing.expectEqual(write_gate.Action.reject_read_only_standby, standby_write.action);

    const standby_job = try evaluateStandbyOwnerJob(&standby, .{
        .kind = .derived_effect_writer,
        .expected_identity = identity,
    });
    try std.testing.expect(!standby_job.canRun());
    try std.testing.expectEqual(owner_job_gate.Action.disable_on_standby, standby_job.action);

    _ = try primary.append(.{ .payload = "one" });
    const names = [_][]const u8{"standby-a"};
    var gate = try evaluateCommit(&primary, 1, .{
        .mode = .remote_apply,
        .standby_names = &names,
    });
    try std.testing.expectEqual(commit_gate.Action.wait_for_standby, gate.action);

    _ = try standby.receive(baseRecord(identity, 1, "one"));
    var apply_counter = ApplyCounter{};
    try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&apply_counter, ApplyCounter.apply));
    _ = try updateStandbyProgress(&primary, .{
        .slot_name = "standby-a",
        .timeline_id = identity.timeline_id,
        .received_lsn = 1,
        .applied_lsn = 1,
    });

    gate = try evaluateCommit(&primary, 1, .{
        .mode = .remote_apply,
        .standby_names = &names,
    });
    try std.testing.expectEqual(commit_gate.Action.acknowledge, gate.action);

    const read_ready = try evaluateStandbyRead(&standby, .{
        .consistency = .at_least_lsn,
        .required_lsn = 1,
    });
    try std.testing.expectEqual(read_gate.Action.serve_standby, read_ready.action);

    const read_primary = try evaluateStandbyRead(&standby, .{ .consistency = .primary });
    try std.testing.expectEqual(read_gate.Action.route_to_primary, read_primary.action);
}

test "storage.ha admin acquires fence and promotes standby" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "promote");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    _ = try standby.receive(baseRecord(identity, 2, "two"));
    var apply_counter = ApplyCounter{};
    try std.testing.expectEqual(@as(usize, 2), try standby.applyAvailable(&apply_counter, ApplyCounter.apply));

    var store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer store.close();
    var result = try promoteWithFence(alloc, &store, &standby, .{
        .fence = .{
            .identity = identity,
            .old_primary_id = "primary-a",
            .promoted_node_id = "standby-a",
            .new_timeline_id = 2,
            .new_epoch = 2,
            .required_lsn = 2,
            .observed_lsn = 2,
            .reason = "admin-test",
        },
    });
    defer result.deinit(alloc);

    try std.testing.expect(result.assessment.can_promote);
    try std.testing.expectEqual(@as(u64, 1), result.fence_generation);
    try std.testing.expect(result.fence_token.len > 0);
    try std.testing.expectEqual(@as(u64, 3), result.promotion.switch_lsn);
    try std.testing.expectEqual(@as(u64, 2), standby.identitySnapshot().timeline_id);
    try std.testing.expect(!result.forced);

    const promoted_write = try evaluateStandbyWrite(&standby, .{ .expected_identity = result.promotion.new_identity });
    try std.testing.expect(!promoted_write.canWrite());
    try std.testing.expectEqual(write_gate.Action.open_promoted_primary, promoted_write.action);
    try std.testing.expect(promoted_write.promotion_handoff != null);

    const promoted_job = try evaluateStandbyOwnerJob(&standby, .{
        .kind = .retention_advance,
        .expected_identity = result.promotion.new_identity,
    });
    try std.testing.expect(!promoted_job.canRun());
    try std.testing.expectEqual(owner_job_gate.Action.open_promoted_primary, promoted_job.action);
    try std.testing.expect(promoted_job.promotion_handoff != null);
}

test "storage.ha admin rejects mismatched fence identity for promotion" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "promote-fence-identity");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    var apply_counter = ApplyCounter{};
    try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&apply_counter, ApplyCounter.apply));

    var store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer store.close();

    var wrong_request_identity = identity;
    wrong_request_identity.shard_id += 1;
    try std.testing.expectError(error.WrongShard, promoteWithFence(alloc, &store, &standby, .{
        .fence = .{
            .identity = wrong_request_identity,
            .old_primary_id = "primary-a",
            .promoted_node_id = "standby-a",
            .new_timeline_id = 2,
            .new_epoch = 2,
            .required_lsn = 1,
            .observed_lsn = 1,
            .reason = "admin-test",
        },
    }));
    try std.testing.expect((try store.current(alloc)) == null);

    var wrong_parent = identity;
    wrong_parent.timeline_id += 1;
    wrong_parent.epoch += 1;
    const wrong_receipt = try store.acquirePromotionFence(.{
        .identity = wrong_parent,
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = wrong_parent.timeline_id + 1,
        .new_epoch = wrong_parent.epoch + 1,
        .required_lsn = 1,
        .observed_lsn = 1,
        .reason = "admin-test",
    });
    defer fencing.freeReceipt(alloc, wrong_receipt);
    try std.testing.expectError(error.WrongTimeline, promoteWithCurrentFence(alloc, &store, &standby));
    try std.testing.expectEqual(@as(u64, identity.timeline_id), standby.identitySnapshot().timeline_id);
}

test "storage.ha admin assesses former primary rejoin workflow" {
    const identity = testIdentity();
    const former = rejoin.FormerPrimaryState{
        .node_id = "primary-a",
        .identity = identity,
        .last_lsn = 12,
    };
    const receipt = fencing.Receipt{
        .identity = .{
            .cluster_id = identity.cluster_id,
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
            .timeline_id = 2,
            .epoch = 2,
        },
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-b",
        .parent_timeline_id = identity.timeline_id,
        .parent_epoch = identity.epoch,
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 10,
        .observed_lsn = 10,
        .generation = 1,
        .forced = false,
        .token = "token",
        .reason = "manual",
    };

    const rewind = assessFormerPrimaryRejoin(former, receipt, .{ .retained_from_lsn = 8 });
    try std.testing.expectEqual(rejoin.Action.rewind, rewind.action);
    try std.testing.expectEqual(@as(u64, 10), rewind.fork_lsn);
    try std.testing.expect(rewind.data_loss_discarded);

    const reseed = assessFormerPrimaryRejoin(former, receipt, .{ .retained_from_lsn = 11 });
    try std.testing.expectEqual(rejoin.Action.reseed, reseed.action);
    try std.testing.expectEqual(rejoin.Reason.parent_timeline_wal_expired, reseed.reason);
}
