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

//! Revision-5 cross-process reader epochs. Registration holds the data inode's
//! shared lock until a locked, immutable marker is complete. A writer holds the
//! exclusive data lock while scanning markers and promoting/reusing pages. Thus
//! registration cannot cross a writer's scan-to-allocation window. Kernel locks
//! reclaim crashed registrations; no clock, PID reuse, or timeout decides safety.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Frontier = struct { data: ?u64 = null, metadata: ?u64 = null };
pub const Lease = struct {
    file: std.Io.File,
    path: []u8,
    pub fn close(self: Lease, a: Allocator, io: std.Io) void {
        // Unlink while still locked; a scanner either observes the immutable
        // marker or misses an already completed reader.
        std.Io.Dir.cwd().deleteFile(io, self.path) catch {};
        self.file.close(io);
        a.free(self.path);
    }
};

pub fn register(a: Allocator, io: std.Io, path: []const u8, inode: u64, sequence: u64, metadata: bool) !Lease {
    const canonical = try std.Io.Dir.cwd().realPathFileAlloc(io, path, a);
    defer a.free(canonical);
    const directory = try std.fmt.allocPrint(a, "{s}.readers", .{canonical});
    defer a.free(directory);
    std.Io.Dir.cwd().createDir(io, directory, .fromMode(0o700)) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const marker_path = try std.fmt.allocPrint(a, "{s}/{x}", .{ directory, nonce });
    errdefer a.free(marker_path);
    const marker = try std.Io.Dir.cwd().createFile(io, marker_path, .{ .read = true, .exclusive = true, .lock = .exclusive, .lock_nonblocking = true, .permissions = .fromMode(0o600) });
    errdefer {
        std.Io.Dir.cwd().deleteFile(io, marker_path) catch {};
        marker.close(io);
    }
    var raw: [64]u8 = @splat(0);
    @memcpy(raw[0..8], "AFREAD05");
    std.mem.writeInt(u64, raw[8..16], inode, .little);
    std.mem.writeInt(u64, raw[16..24], sequence, .little);
    std.mem.writeInt(u64, raw[24..32], @intFromBool(metadata), .little);
    std.crypto.hash.Blake3.hash(raw[0..32], raw[32..64], .{});
    try marker.writePositionalAll(io, &raw, 0);
    // Ephemeral markers need no fsync: after a machine crash no reader survives.
    return .{ .file = marker, .path = marker_path };
}

/// Caller holds the exclusive data inode lock. Missing directories mean no
/// cooperating readers. Uncooperative readers still hold ordinary inode locks.
/// Bound work under admission; too many registrations conservatively deny reuse.
pub fn scan(a: Allocator, io: std.Io, path: []const u8, inode: u64) !Frontier {
    const canonical = try std.Io.Dir.cwd().realPathFileAlloc(io, path, a);
    defer a.free(canonical);
    const directory = try std.fmt.allocPrint(a, "{s}.readers", .{canonical});
    defer a.free(directory);
    const dir = std.Io.Dir.cwd().openDir(io, directory, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return .{},
        else => return err,
    };
    defer dir.close(io);
    var it = dir.iterate();
    var result: Frontier = .{};
    var visited: usize = 0;
    while (try it.next(io)) |entry| {
        visited += 1;
        if (visited > 4096) return error.WouldBlock;
        if (entry.kind != .file or entry.name.len != 32) return error.InvalidNativeReaderEpoch;
        const marker = dir.openFile(io, entry.name, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer marker.close(io);
        if (try marker.tryLock(io, .exclusive)) {
            // No reader can acquire an existing UUID. The creating reader held
            // the data lock until it acquired the marker lock and wrote bytes.
            dir.deleteFile(io, entry.name) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
            continue;
        }
        var raw: [64]u8 = undefined;
        const n = try marker.readPositionalAll(io, &raw, 0);
        if (n != raw.len) return error.InvalidNativeReaderEpoch;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw[0..32], &digest, .{});
        if (!std.mem.eql(u8, raw[0..8], "AFREAD05") or !std.mem.eql(u8, raw[32..64], &digest) or std.mem.readInt(u64, raw[24..32], .little) > 1) return error.InvalidNativeReaderEpoch;
        if (std.mem.readInt(u64, raw[8..16], .little) != inode) continue;
        const sequence = std.mem.readInt(u64, raw[16..24], .little);
        result.data = @min(result.data orelse sequence, sequence);
        if (raw[24] == 1) result.metadata = @min(result.metadata orelse sequence, sequence);
    }
    return result;
}

test "lite reader epochs isolate inode frontiers and reclaim crashed registrations" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/epochs.aflite", .{tmp.sub_path});
    defer a.free(path);
    const data = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer data.close(io);
    const inode = (try data.stat(io)).inode;
    const first = try register(a, io, path, inode, 10, false);
    var first_open = true;
    defer if (first_open) first.close(a, io);
    const full = try register(a, io, path, inode, 20, true);
    defer full.close(a, io);
    const unrelated = try register(a, io, path, inode + 1, 1, true);
    defer unrelated.close(a, io);
    var frontiers = try scan(a, io, path, inode);
    try std.testing.expectEqual(@as(?u64, 10), frontiers.data);
    try std.testing.expectEqual(@as(?u64, 20), frontiers.metadata);
    // Simulate process death: leave the pathname but release its kernel lock.
    first.file.close(io);
    first_open = false;
    defer a.free(first.path);
    frontiers = try scan(a, io, path, inode);
    try std.testing.expectEqual(@as(?u64, 20), frontiers.data);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, first.path, .{}));
    // A live malformed registration must fail closed, never disappear.
    try full.file.writePositionalAll(io, "broken", 0);
    try std.testing.expectError(error.InvalidNativeReaderEpoch, scan(a, io, path, inode));
}
