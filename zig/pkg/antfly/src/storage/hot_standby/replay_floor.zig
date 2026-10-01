// Copyright 2026 Antfly, Inc.
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

//! Durable rejection boundary for replay and offline seed rollback. A floor
//! is derived only from contiguous, durably applied HA progress, never from
//! retention estimates or a truncated WAL. Epoch changes require the caller's
//! authenticated HA timeline transition; numeric timeline IDs are not ordered.
const std = @import("std");
const fs = @import("../../common/fs_paths.zig");
pub const size = 68;
pub const anchor_name = ".antfly-ha-replay-floor";

pub const Floor = struct {
    cluster_id: u64,
    timeline_id: u64,
    epoch: u64,
    lsn: u64,

    pub fn encode(self: Floor) ![size]u8 {
        if (self.cluster_id == 0 or self.timeline_id == 0 or self.epoch == 0 or self.lsn == 0) return error.InvalidHAReplayFloor;
        var out: [size]u8 = undefined;
        out[0..4].* = "ARF1".*;
        inline for (.{ "cluster_id", "timeline_id", "epoch", "lsn" }, 0..) |field, index| std.mem.writeInt(u64, out[4 + index * 8 ..][0..8], @field(self, field), .big);
        std.crypto.hash.Blake3.hash(out[0..36], out[36..68], .{});
        return out;
    }
    pub fn decode(bytes: []const u8) !Floor {
        if (bytes.len != size or !std.mem.eql(u8, bytes[0..4], "ARF1")) return error.InvalidHAReplayFloor;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..36], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[36..68])) return error.InvalidHAReplayFloor;
        var value: Floor = undefined;
        inline for (.{ "cluster_id", "timeline_id", "epoch", "lsn" }, 0..) |field, index| @field(value, field) = std.mem.readInt(u64, bytes[4 + index * 8 ..][0..8], .big);
        _ = try value.encode();
        return value;
    }
    pub fn covers(self: Floor, candidate: Floor) !bool {
        if (self.cluster_id != candidate.cluster_id) return error.HAReplayFloorIdentityMismatch;
        if (self.epoch == candidate.epoch and self.timeline_id != candidate.timeline_id) return error.HAReplayFloorIdentityMismatch;
        return candidate.epoch < self.epoch or (candidate.epoch == self.epoch and candidate.lsn <= self.lsn);
    }
    pub fn requireSeed(self: Floor, candidate: Floor) !void {
        if (self.cluster_id != candidate.cluster_id or (self.epoch == candidate.epoch and self.timeline_id != candidate.timeline_id)) return error.HAReplayFloorIdentityMismatch;
        if (candidate.epoch < self.epoch or (candidate.epoch == self.epoch and candidate.lsn < self.lsn)) return error.SeedBelowHAReplayFloor;
    }
};

/// The anchor is outside replaceable live generations. Runtime writes are
/// serialized by HA role ownership; activation runs offline on the same PVC.
pub fn load(alloc: std.mem.Allocator, io: std.Io, root: []const u8) !?Floor {
    const path = try std.fs.path.join(alloc, &.{ root, anchor_name });
    defer alloc.free(path);
    var file = std.Io.Dir.cwd().openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);
    var bytes: [size]u8 = undefined;
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &bytes, 0) != bytes.len or try file.readPositionalAll(io, &extra, bytes.len) != 0) return error.InvalidHAReplayFloor;
    return try Floor.decode(&bytes);
}

pub fn advance(alloc: std.mem.Allocator, io: std.Io, root: []const u8, next: Floor) !void {
    const encoded = try next.encode();
    if (try load(alloc, io, root)) |previous| {
        if (try previous.covers(next)) {
            // Repeat the parent barrier after ambiguous rename completion.
            try fs.syncDirPortable(io, root);
            return;
        }
    }
    try fs.createDirPathPortable(io, root);
    const path = try std.fs.path.join(alloc, &.{ root, anchor_name });
    defer alloc.free(path);
    const temp = try std.fmt.allocPrint(alloc, "{s}.tmp", .{path});
    defer alloc.free(temp);
    {
        var file = try std.Io.Dir.cwd().createFile(io, temp, .{ .truncate = true });
        defer file.close(io);
        try file.writeStreamingAll(io, &encoded);
        try file.sync(io);
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temp, std.Io.Dir.cwd(), path, io);
    try fs.syncDirPortable(io, root);
}

pub fn requireSeed(alloc: std.mem.Allocator, io: std.Io, root: []const u8, identity: anytype, checkpoint_lsn: u64) !void {
    if (try load(alloc, io, root)) |floor| try floor.requireSeed(.{ .cluster_id = identity.cluster_id, .timeline_id = identity.timeline_id, .epoch = identity.epoch, .lsn = checkpoint_lsn });
}

test "HA replay floor survives restart and rejects older seeds and foreign timelines" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const floor: Floor = .{ .cluster_id = 1, .timeline_id = 10, .epoch = 2, .lsn = 100 };
    try advance(alloc, io, root, floor);
    try advance(alloc, io, root, .{ .cluster_id = 1, .timeline_id = 9, .epoch = 1, .lsn = 500 });
    const restored = (try load(alloc, io, root)).?;
    try std.testing.expectEqual(floor, restored);
    try std.testing.expectError(error.SeedBelowHAReplayFloor, restored.requireSeed(.{ .cluster_id = 1, .timeline_id = 10, .epoch = 2, .lsn = 99 }));
    try std.testing.expectError(error.HAReplayFloorIdentityMismatch, restored.requireSeed(.{ .cluster_id = 1, .timeline_id = 11, .epoch = 2, .lsn = 200 }));
    try restored.requireSeed(floor);
    try std.testing.expect(try restored.covers(.{ .cluster_id = 1, .timeline_id = 9, .epoch = 1, .lsn = 1000 }));
}
