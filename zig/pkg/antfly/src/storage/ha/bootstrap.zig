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

//! Standby bootstrap from a verified HA base-backup manifest.
//!
//! The primary publishes a manifest and retains WAL from the backup boundary.
//! After the files are copied and verified, this module initializes an empty
//! standby receive log at the manifest checkpoint so streaming can resume from
//! `checkpoint_lsn + 1` without replaying records already represented by the
//! copied storage snapshot.

const std = @import("std");
const Allocator = std.mem.Allocator;
const backup_manifest = @import("backup_manifest.zig");
const primary_mod = @import("primary.zig");
const replication_record = @import("replication_record.zig");
const session = @import("session.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const BootstrapResult = struct {
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
};

pub fn bootstrapFromManifest(
    alloc: Allocator,
    standby: *standby_mod.Standby,
    manifest: backup_manifest.ManifestView,
    contents: []const backup_manifest.FileContent,
) !BootstrapResult {
    try backup_manifest.validateManifestView(manifest);
    try validateIdentity(manifest.identity, standby.identitySnapshot());
    try backup_manifest.verifyFileContents(manifest, contents);

    const payload = try checkpointPayload(alloc, manifest);
    defer alloc.free(payload);
    try standby.bootstrapCheckpoint(manifest.checkpoint_lsn, payload);

    return .{
        .manifest_id = manifest.manifest_id,
        .backup_lsn = manifest.backup_lsn,
        .checkpoint_lsn = manifest.checkpoint_lsn,
    };
}

fn validateIdentity(actual: standby_mod.Identity, expected: standby_mod.Identity) !void {
    if (actual.cluster_id != expected.cluster_id) return error.WrongCluster;
    if (actual.shard_id != expected.shard_id) return error.WrongShard;
    if (actual.table_id != expected.table_id) return error.WrongTable;
    if (actual.timeline_id != expected.timeline_id) return error.WrongTimeline;
    if (actual.epoch != expected.epoch) return error.WrongEpoch;
}

fn checkpointPayload(alloc: Allocator, manifest: backup_manifest.ManifestView) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .manifest_id = manifest.manifest_id,
        .backup_lsn = manifest.backup_lsn,
        .checkpoint_lsn = manifest.checkpoint_lsn,
        .file_count = manifest.files.len,
        .total_bytes = manifest.totalBytes(),
    }, .{});
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
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

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .standby_log = try alloc.dupeZ(u8, standby_log),
        .standby_progress = try alloc.dupeZ(u8, standby_progress),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-bootstrap-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
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

fn testFiles() [2]backup_manifest.FileEntry {
    return .{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
        .{ .path = "sst/0001", .kind = .sstable, .size_bytes = 7, .crc32 = backup_manifest.crc32("sstable") },
    };
}

fn testContents() [2]backup_manifest.FileContent {
    return .{
        .{ .path = "manifest", .bytes = "manifest" },
        .{ .path = "sst/0001", .bytes = "sstable" },
    };
}

const ApplyCapture = struct {
    alloc: Allocator,
    payloads: std.ArrayListUnmanaged([]u8) = .empty,

    fn deinit(self: *ApplyCapture) void {
        for (self.payloads.items) |payload| self.alloc.free(payload);
        self.payloads.deinit(self.alloc);
        self.* = undefined;
    }

    fn apply(ctx: *anyopaque, record: replication_record.RecordView) !void {
        const self: *ApplyCapture = @ptrCast(@alignCast(ctx));
        const owned = try self.alloc.dupe(u8, record.payload);
        errdefer self.alloc.free(owned);
        try self.payloads.append(self.alloc, owned);
    }
};

test "storage.ha bootstrap verifies manifest files and starts standby at checkpoint" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "checkpoint");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const files = testFiles();
    const contents = testContents();
    const manifest = backup_manifest.ManifestView{
        .identity = identity,
        .manifest_id = "base-0001",
        .backup_lsn = 3,
        .checkpoint_lsn = 5,
        .files = &files,
        .flags = 0,
    };

    {
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        const result = try bootstrapFromManifest(alloc, &standby, manifest, &contents);
        try std.testing.expectEqual(@as(u64, 3), result.backup_lsn);
        try std.testing.expectEqual(@as(u64, 5), result.checkpoint_lsn);
        try std.testing.expectEqualStrings("base-0001", result.manifest_id);
        try std.testing.expectEqual(@as(u64, 6), standby.nextReceiveLsn());
        try std.testing.expectEqual(@as(u64, 6), standby.nextApplyLsn());
        try std.testing.expectError(error.RecordAlreadyReceived, standby.receive(baseRecord(identity, 5, "old")));
        try std.testing.expectEqual(@as(u64, 6), try standby.receive(baseRecord(identity, 6, "catch-up")));
    }

    {
        var reopened = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 6), reopened.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 5), reopened.currentProgress().applied_lsn);

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 1), try reopened.applyAvailable(&capture, ApplyCapture.apply));
        try std.testing.expectEqualStrings("catch-up", capture.payloads.items[0]);
    }
}

test "storage.ha bootstrap rejects bad copied files and wrong identities before changing standby" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "reject");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const files = testFiles();
    const bad_contents = [_]backup_manifest.FileContent{
        .{ .path = "manifest", .bytes = "MANIFEST" },
        .{ .path = "sst/0001", .bytes = "sstable" },
    };
    const manifest = backup_manifest.ManifestView{
        .identity = identity,
        .manifest_id = "base-0001",
        .backup_lsn = 3,
        .checkpoint_lsn = 5,
        .files = &files,
        .flags = 0,
    };

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    try std.testing.expectError(error.ManifestFileCrcMismatch, bootstrapFromManifest(alloc, &standby, manifest, &bad_contents));
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 0), standby.receive_log.lastLsn());

    var wrong_identity_manifest = manifest;
    wrong_identity_manifest.identity.timeline_id = 2;
    const contents = testContents();
    try std.testing.expectError(error.WrongTimeline, bootstrapFromManifest(alloc, &standby, wrong_identity_manifest, &contents));
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 0), standby.receive_log.lastLsn());
}

test "storage.ha bootstrap catches up from primary backup end record" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "catch-up");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const files = testFiles();
    const contents = testContents();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    const started = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "base-0001",
    });
    try std.testing.expectEqual(@as(u64, 1), started.backup_lsn);
    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "during-copy" }));
    const ended = try primary.endBaseBackup(.{
        .identity = identity,
        .manifest_id = "base-0001",
        .backup_lsn = started.backup_lsn,
        .checkpoint_lsn = 2,
        .files = &files,
    });
    try std.testing.expectEqual(@as(u64, 3), ended.end_record_lsn);

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    const manifest = backup_manifest.ManifestView{
        .identity = identity,
        .manifest_id = "base-0001",
        .backup_lsn = started.backup_lsn,
        .checkpoint_lsn = 2,
        .files = &files,
        .flags = 0,
    };
    _ = try bootstrapFromManifest(alloc, &standby, manifest, &contents);

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    const replicated = try session.replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply);
    try std.testing.expectEqual(@as(usize, 1), replicated.received_count);
    try std.testing.expectEqual(@as(usize, 1), replicated.applied_count);
    try std.testing.expectEqual(@as(u64, 3), replicated.progress.received_lsn);
    try std.testing.expectEqual(@as(u64, 3), replicated.progress.applied_lsn);

    const decoded = try backup_manifest.decodeAlloc(alloc, capture.payloads.items[0]);
    defer backup_manifest.freeDecoded(alloc, decoded);
    try std.testing.expectEqualStrings("base-0001", decoded.manifest_id);
    try std.testing.expectEqual(@as(u64, 2), decoded.checkpoint_lsn);

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 3), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 3), slot.applied_lsn);
}
