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

//! Shared root-local checkpoint IO, not publication or restore authority.
//! Protocols retain their own signed envelope and scope validation. Callers
//! retain signing-root and physical-root lifecycle leases through close().
const std = @import("std");
const signing = @import("root_signing_identity.zig");
const root_identity = @import("root_identity.zig");
const Context = @import("../../api/operation.zig").RequestContext;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const A = std.mem.Allocator;
const Digest = [32]u8;

pub const Errors = struct { invalid: anyerror, scope_changed: anyerror };
pub const Opened = struct {
    alloc: A,
    io: std.Io,
    root_dir: []const u8,
    root: signing.State,
    path: []u8,
    directory: std.Io.Dir,
    lock: std.Io.File,
    context: Context,
    checkpoint_digest: ?Digest = null,
};

fn permissions() std.Io.File.Permissions {
    if (comptime @import("builtin").os.tag == .windows) return .default_file;
    return @fromBackingInt(0o600);
}
fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

pub fn close(guard: anytype) void {
    guard.lock.unlock(guard.io);
    guard.lock.close(guard.io);
    guard.directory.close(guard.io);
    guard.alloc.free(guard.path);
    guard.* = undefined;
}

pub fn requireRoot(guard: anytype, comptime errors: Errors) !void {
    try guard.context.ensureActive();
    const current = try root_identity.load(guard.alloc, guard.io, guard.root_dir);
    if (current.incarnation != guard.root.root_incarnation) return errors.scope_changed;
    const key = try signing.load(guard.alloc, guard.io, guard.root_dir);
    if (!std.mem.eql(u8, &key.public_key, &guard.root.public_key)) return errors.scope_changed;
}

/// Owns returned bytes. The protocol authenticates BEFORE parsing them.
pub fn read(guard: anytype, max_bytes: usize, comptime errors: Errors) !?[]u8 {
    try requireRoot(guard, errors);
    const file = guard.directory.openFile(guard.io, std.fs.path.basename(guard.path), .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(guard.io);
    const stat = try file.stat(guard.io);
    if (stat.kind != .file or stat.size > max_bytes) return errors.invalid;
    if (comptime @import("builtin").os.tag != .windows) if ((stat.permissions.toMode() & 0o077) != 0) return errors.invalid;
    var buffer: [4096]u8 = undefined;
    var reader = file.reader(guard.io, &buffer);
    return try reader.interface.allocRemaining(guard.alloc, .limited(max_bytes + 1));
}

pub fn write(guard: anytype, bytes: []const u8, comptime errors: Errors) !void {
    try requireRoot(guard, errors);
    const digest = hash(bytes);
    if (guard.checkpoint_digest) |previous| if (std.mem.eql(u8, &previous, &digest)) return;
    const temporary = try std.fmt.allocPrint(guard.alloc, "{s}.tmp", .{std.fs.path.basename(guard.path)});
    defer guard.alloc.free(temporary);
    // Only the selected namespace's private temporary file under its lock.
    // Before rename, the prior durable checkpoint remains authoritative.
    guard.directory.deleteFile(guard.io, temporary) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    errdefer guard.directory.deleteFile(guard.io, temporary) catch {};
    {
        var file = try guard.directory.createFile(guard.io, temporary, .{ .exclusive = true, .permissions = permissions() });
        defer file.close(guard.io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(guard.io, &buffer);
        try writer.interface.writeAll(bytes);
        try writer.end();
        try file.sync(guard.io);
    }
    try requireRoot(guard, errors);
    try std.Io.Dir.rename(guard.directory, temporary, guard.directory, std.fs.path.basename(guard.path), guard.io);
    const directory_file: std.Io.File = .{ .handle = guard.directory.handle, .flags = .{ .nonblocking = false } };
    try directory_file.sync(guard.io);
    // Close the mkdir-observer race before the first durable checkpoint.
    if (guard.checkpoint_digest == null) try fs_paths.syncDirPortable(guard.io, guard.root_dir);
    guard.checkpoint_digest = digest;
}

pub fn open(alloc: A, io: std.Io, root_dir: []const u8, root_incarnation: u128, namespace: [24]u8, comptime directory_name: []const u8, context: Context, comptime errors: Errors) !Opened {
    comptime {
        if (directory_name.len == 0 or std.mem.eql(u8, directory_name, ".") or std.mem.eql(u8, directory_name, "..") or
            std.mem.indexOfAny(u8, directory_name, "/\\") != null) @compileError("checkpoint directory must be a fixed child name");
    }
    try context.ensureActive();
    const identity = try root_identity.load(alloc, io, root_dir);
    if (root_incarnation == 0 or identity.incarnation != root_incarnation) return errors.scope_changed;
    // Provisioning owns key creation; an audit never mints an identity.
    const root = try signing.load(alloc, io, root_dir);
    if (root.root_incarnation != root_incarnation) return errors.scope_changed;
    const directory = try std.fs.path.join(alloc, &.{ root_dir, directory_name });
    defer alloc.free(directory);
    if (std.Io.Dir.cwd().createDir(io, directory, .default_dir)) |_| {
        try fs_paths.syncDirPortable(io, root_dir);
    } else |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    }
    const opened_directory = try std.Io.Dir.cwd().openDir(io, directory, .{ .follow_symlinks = false, .iterate = true });
    errdefer opened_directory.close(io);
    // One canonical slot per namespace bounds storage after job churn.
    const id = std.fmt.bytesToHex(hash(&namespace), .lower);
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}.checkpoint", .{ directory, id });
    errdefer alloc.free(path);
    const lock_path = try std.fmt.allocPrint(alloc, "{s}.lock", .{std.fs.path.basename(path)});
    defer alloc.free(lock_path);
    if (opened_directory.createFile(io, lock_path, .{ .exclusive = true, .permissions = permissions() })) |seed| {
        seed.close(io);
    } else |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    }
    const file = try opened_directory.openFile(io, lock_path, .{ .mode = .read_write, .follow_symlinks = false });
    errdefer file.close(io);
    if ((try file.stat(io)).kind != .file) return errors.invalid;
    while (!try file.tryLock(io, .exclusive)) {
        try context.ensureActive();
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    errdefer file.unlock(io);
    const result: Opened = .{ .alloc = alloc, .io = io, .root_dir = root_dir, .root = root, .path = path, .directory = opened_directory, .lock = file, .context = context };
    try requireRoot(&result, errors);
    const temporary = try std.fmt.allocPrint(alloc, "{s}.tmp", .{std.fs.path.basename(path)});
    defer alloc.free(temporary);
    opened_directory.deleteFile(io, temporary) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    return result;
}
