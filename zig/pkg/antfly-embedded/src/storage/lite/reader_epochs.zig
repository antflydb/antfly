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

//! Revision-6 cross-process reader epochs with an immutable namespace binding. Registration holds the data inode's
//! shared lock until a locked, immutable marker is complete. A writer holds the
//! exclusive data lock while scanning markers and promoting/reusing pages. Thus
//! registration cannot cross a writer's scan-to-allocation window. Kernel locks
//! reclaim crashed registrations; no clock, PID reuse, or timeout decides safety.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Frontier = struct { data: ?u64 = null, metadata: ?u64 = null };
pub const Lease = struct {
    file: std.Io.File,
    directory: std.Io.Dir,
    name: [32]u8,
    path: []u8,
    pub fn close(self: Lease, a: Allocator, io: std.Io) void {
        // Use the captured directory inode even if a parent was renamed.
        self.directory.deleteFile(io, &self.name) catch {};
        self.file.close(io);
        self.directory.close(io);
        a.free(self.path);
    }
};

const Namespace = struct {
    canonical: [:0]u8,
    directory: std.Io.Dir,
    guard: std.Io.File,
    digest: [32]u8,
    fn open(a: Allocator, io: std.Io, path: []const u8, publication: bool, no_sync: bool) !Namespace {
        const canonical = if (publication) blk: {
            const parent = try std.Io.Dir.cwd().realPathFileAlloc(io, std.fs.path.dirname(path) orelse ".", a);
            defer a.free(parent);
            break :blk try std.fs.path.joinZ(a, &.{ parent, std.fs.path.basename(path) });
        } else try std.Io.Dir.cwd().realPathFileAlloc(io, path, a);
        errdefer a.free(canonical);
        const parent = try std.Io.Dir.cwd().openDir(io, std.fs.path.dirname(canonical).?, .{});
        defer parent.close(io);
        // The path chooses a bounded location, never the authority to reuse.
        var path_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(canonical, &path_digest, .{});
        var name: [80]u8 = undefined;
        const directory_name = try std.fmt.bufPrint(&name, ".aflite-readers-{x}", .{path_digest});
        if (publication) parent.createDir(io, directory_name, .fromMode(0o700)) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
        const directory = try parent.openDir(io, directory_name, .{ .iterate = true });
        errdefer directory.close(io);
        // Never recreate this authority for an already published inode. Missing
        // or replaced namespaces must retain inode fencing, including writers.
        const guard = if (publication)
            try directory.createFile(io, "identity", .{ .read = true, .truncate = false, .lock = .exclusive, .permissions = .fromMode(0o600) })
        else
            try directory.openFile(io, "identity", .{ .mode = .read_write });
        errdefer guard.close(io);
        const stat = try guard.stat(io);
        if (stat.kind != .file or stat.size > 32) return error.InvalidNativeReaderEpoch;
        var seed: [32]u8 = undefined;
        if (publication and stat.size < 32) {
            try io.randomSecure(&seed);
            try guard.writePositionalAll(io, &seed, 0);
        } else if (try guard.readPositionalAll(io, &seed, 0) != 32) return error.InvalidNativeReaderEpoch;
        if (publication) {
            if (!no_sync) {
                try guard.sync(io);
                const directory_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(canonical).?, directory_name });
                defer a.free(directory_path);
                try @import("antfly_runtime_fs").fs_paths.syncDirPortable(io, directory_path);
                try @import("antfly_runtime_fs").fs_paths.syncDirPortable(io, std.fs.path.dirname(canonical).?);
            }
            guard.unlock(io);
        }
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, @intCast((try directory.stat(io)).inode), .little);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("AFLITE reader namespace v6");
        hash.update(&seed);
        hash.update(&id);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return .{ .canonical = canonical, .directory = directory, .guard = guard, .digest = digest };
    }
    fn close(self: Namespace, a: Allocator, io: std.Io) void {
        self.guard.close(io);
        self.directory.close(io);
        a.free(self.canonical);
    }
};

/// Only a newly created/private inode may establish or change this binding.
/// Identity persists across inode generations, but cannot be reconstructed from
/// an alias or a recreated directory. Existing markers retain their authority.
pub fn namespaceForPublication(a: Allocator, io: std.Io, path: []const u8) ![32]u8 {
    return namespaceForPublicationWithDurability(a, io, path, true);
}
pub fn namespaceForPublicationWithDurability(a: Allocator, io: std.Io, path: []const u8, no_sync: bool) ![32]u8 {
    const namespace = try Namespace.open(a, io, path, true, no_sync);
    defer namespace.close(a, io);
    return namespace.digest;
}

pub fn register(a: Allocator, io: std.Io, path: []const u8, binding: [32]u8, inode: u64, sequence: u64, metadata: bool) !Lease {
    const namespace = Namespace.open(a, io, path, false, true) catch |err| switch (err) {
        error.FileNotFound => return error.UnsafeNativeReaderAlias,
        else => return err,
    };
    defer namespace.close(a, io);
    if (!std.mem.eql(u8, &binding, &namespace.digest)) return error.UnsafeNativeReaderAlias;
    // Different file generations share this directory but have different data
    // locks. A shared guard covers create-to-flock, so a scanner for an older
    // generation cannot reap an incompletely registered new marker.
    if (!try namespace.guard.tryLock(io, .shared)) return error.UnsafeNativeReaderAlias;
    defer namespace.guard.unlock(io);
    const directory = try namespace.directory.openDir(io, ".", .{});
    errdefer directory.close(io);
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    var name: [32]u8 = undefined;
    _ = try std.fmt.bufPrint(&name, "{x}", .{nonce});
    const directory_path = try namespace.directory.realPathFileAlloc(io, ".", a);
    defer a.free(directory_path);
    const marker_path = try std.fs.path.join(a, &.{ directory_path, &name });
    errdefer a.free(marker_path);
    const marker = try directory.createFile(io, &name, .{ .read = true, .exclusive = true, .lock = .exclusive, .lock_nonblocking = true, .permissions = .fromMode(0o600) });
    errdefer {
        directory.deleteFile(io, &name) catch {};
        marker.close(io);
    }
    var raw: [64]u8 = @splat(0);
    @memcpy(raw[0..8], "AFREAD05");
    std.mem.writeInt(u64, raw[8..16], inode, .little);
    std.mem.writeInt(u64, raw[16..24], sequence, .little);
    std.mem.writeInt(u64, raw[24..32], @intFromBool(metadata), .little);
    std.crypto.hash.Blake3.hash(raw[0..32], raw[32..64], .{});
    try marker.writePositionalAll(io, &raw, 0);
    return .{ .file = marker, .directory = directory, .name = name, .path = marker_path };
}

/// Caller holds the exclusive data inode lock. Missing/mismatched namespaces
/// deny promotion and reuse; uncooperative readers retain ordinary inode locks.
/// Bound work under admission; too many registrations conservatively deny reuse.
pub fn scan(a: Allocator, io: std.Io, path: []const u8, binding: [32]u8, inode: u64) !Frontier {
    const namespace = Namespace.open(a, io, path, false, true) catch |err| switch (err) {
        error.FileNotFound => return error.WouldBlock,
        else => return err,
    };
    defer namespace.close(a, io);
    if (!std.mem.eql(u8, &binding, &namespace.digest)) return error.WouldBlock;
    if (!try namespace.guard.tryLock(io, .exclusive)) return error.WouldBlock;
    defer namespace.guard.unlock(io);
    const dir = namespace.directory;
    var it = dir.iterate();
    var result: Frontier = .{};
    var visited: usize = 0;
    while (try it.next(io)) |entry| {
        if (std.mem.eql(u8, entry.name, "identity")) continue;
        visited += 1;
        if (visited > 4096) return error.WouldBlock;
        if ((entry.kind != .file and entry.kind != .unknown) or entry.name.len != 32) return error.InvalidNativeReaderEpoch;
        const marker = dir.openFile(io, entry.name, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => continue,
            error.IsDir => return error.InvalidNativeReaderEpoch,
            else => return err,
        };
        defer marker.close(io);
        // d_type is optional on Linux filesystems. Authenticate the opened
        // descriptor's type rather than refusing an unknown directory hint.
        if (entry.kind == .unknown and (try marker.stat(io)).kind != .file) return error.InvalidNativeReaderEpoch;
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
    const binding = try namespaceForPublication(a, io, path);
    const first = try register(a, io, path, binding, inode, 10, false);
    var first_open = true;
    defer if (first_open) first.close(a, io);
    const full = try register(a, io, path, binding, inode, 20, true);
    defer full.close(a, io);
    const unrelated = try register(a, io, path, binding, inode + 1, 1, true);
    defer unrelated.close(a, io);
    var frontiers = try scan(a, io, path, binding, inode);
    try std.testing.expectEqual(@as(?u64, 10), frontiers.data);
    try std.testing.expectEqual(@as(?u64, 20), frontiers.metadata);
    // Simulate process death: leave the pathname but release its kernel lock.
    first.file.close(io);
    first.directory.close(io);
    first_open = false;
    defer a.free(first.path);
    frontiers = try scan(a, io, path, binding, inode);
    try std.testing.expectEqual(@as(?u64, 20), frontiers.data);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, first.path, .{}));
    // A live malformed registration must fail closed, never disappear.
    try full.file.writePositionalAll(io, "broken", 0);
    try std.testing.expectError(error.InvalidNativeReaderEpoch, scan(a, io, path, binding, inode));
}

test "lite reader epochs accept unknown directory entry types using descriptor metadata" {
    const Probe = struct {
        var changed: usize = 0;
        fn read(userdata: ?*anyopaque, reader: *std.Io.Dir.Reader, entries: []std.Io.Dir.Entry) std.Io.Dir.Reader.Error!usize {
            const count = try std.Options.debug_io.vtable.dirRead(userdata, reader, entries);
            for (entries[0..count]) |*entry| {
                entry.kind = .unknown;
                changed += 1;
            }
            return count;
        }
    };
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/unknown-type.aflite", .{tmp.sub_path});
    defer a.free(path);
    const data = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer data.close(io);
    const inode = (try data.stat(io)).inode;
    const binding = try namespaceForPublication(a, io, path);
    const reader = try register(a, io, path, binding, inode, 17, true);
    defer reader.close(a, io);
    const expected = try scan(a, io, path, binding, inode);
    var vtable = std.Options.debug_io.vtable.*;
    vtable.dirRead = Probe.read;
    const unknown_io: std.Io = .{ .userdata = std.Options.debug_io.userdata, .vtable = &vtable };
    Probe.changed = 0;
    try std.testing.expectEqual(expected, try scan(a, unknown_io, path, binding, inode));
    try std.testing.expect(Probe.changed != 0);
    // Unknown must not turn a non-file entry into an accepted marker either.
    try reader.directory.createDir(io, "00000000000000000000000000000000", .fromMode(0o700));
    try std.testing.expectError(error.InvalidNativeReaderEpoch, scan(a, unknown_io, path, binding, inode));
}

test "lite reader epochs guard cross-generation registration and reject replaced namespaces" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/generation.aflite", .{tmp.sub_path});
    defer a.free(path);
    const data = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer data.close(io);
    const inode = (try data.stat(io)).inode;
    const binding = try namespaceForPublication(a, io, path);
    const namespace = try Namespace.open(a, io, path, false, true);
    defer namespace.close(a, io);
    try std.testing.expect(try namespace.guard.tryLock(io, .shared));
    // A different generation's data lock cannot protect marker initialization.
    // Its namespace guard must prevent this scan from reaping the marker.
    try std.testing.expectError(error.WouldBlock, scan(a, io, path, binding, inode + 1));
    namespace.guard.unlock(io);
    try std.testing.expect(try namespace.guard.tryLock(io, .exclusive));
    try std.testing.expectError(error.UnsafeNativeReaderAlias, register(a, io, path, binding, inode, 1, true));
    namespace.guard.unlock(io);
    const empty = try scan(a, io, path, binding, inode);
    try std.testing.expectEqual(@as(?u64, null), empty.data);
    // Recreating the identity cannot authorize reuse under an existing header.
    var changed: [32]u8 = undefined;
    try io.randomSecure(&changed);
    try namespace.guard.writePositionalAll(io, &changed, 0);
    try std.testing.expectError(error.UnsafeNativeReaderAlias, register(a, io, path, binding, inode, 1, true));
    try std.testing.expectError(error.WouldBlock, scan(a, io, path, binding, inode));
    try namespace.directory.deleteFile(io, "identity");
    try std.testing.expectError(error.UnsafeNativeReaderAlias, register(a, io, path, binding, inode, 1, true));
    try std.testing.expectError(error.WouldBlock, scan(a, io, path, binding, inode));
    try std.testing.expectError(error.FileNotFound, namespace.directory.access(io, "identity", .{}));
}
