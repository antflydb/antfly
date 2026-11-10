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

//! Independent, resumable expiry of private query caches. Never captures data.
const std = @import("std");
const cut = @import("native_query_cut.zig");
const backup = @import("native_backup.zig");
const fs = @import("antfly_runtime_fs").fs_paths;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;

pub const Collector = struct {
    a: A,
    root: ?[]u8 = null,
    dir: ?std.Io.Dir = null,
    iterator: ?std.Io.Dir.Iterator = null,
    trash_dir: ?std.Io.Dir = null,
    trash_iterator: ?std.Io.Dir.Iterator = null,
    chunks_dir: ?std.Io.Dir = null,
    chunks_iterator: ?std.Io.Dir.Iterator = null,
    reaper: ?Reaper = null,
    phase: enum { trash, live, chunks, done } = .trash,
    pub fn deinit(self: *Collector, io: std.Io) void {
        if (self.reaper) |*v| v.deinit();
        if (self.dir) |v| v.close(io);
        if (self.trash_dir) |v| v.close(io);
        if (self.chunks_dir) |v| v.close(io);
        if (self.root) |v| self.a.free(v);
        self.* = .{ .a = self.a };
    }
    /// Every visited entry or reaper operation consumes one unit. Parent and
    /// reader locks are nonblocking; busy captures/readers are retried later.
    pub fn step(self: *Collector, io: std.Io, db_path: []const u8, now: u64, budget: usize, cancellation: Cancellation) !bool {
        if (budget == 0) return false;
        errdefer self.deinit(io);
        if (self.root == null) {
            const root = try std.fmt.allocPrint(self.a, "{s}.query-pins", .{db_path});

            const dir = std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true }) catch |err| switch (err) {
                error.FileNotFound => {
                    self.a.free(root);
                    return true;
                },
                else => {
                    self.a.free(root);
                    return err;
                },
            };
            self.root = root;
            self.dir = dir;
            self.iterator = dir.iterate();

            const trash = try std.fs.path.join(self.a, &.{ root, ".trash" });
            defer self.a.free(trash);
            try fs.createDirPathPortable(io, trash);
            self.trash_dir = try std.Io.Dir.cwd().openDir(io, trash, .{ .iterate = true });
            self.trash_iterator = self.trash_dir.?.iterate();
        }
        const lock_path = try std.fs.path.join(self.a, &.{ self.root.?, ".lock" });
        defer self.a.free(lock_path);
        const lock = try std.Io.Dir.cwd().createFile(io, lock_path, .{ .truncate = false });
        defer lock.close(io);
        if (!try lock.tryLock(io, .exclusive)) return false;
        defer lock.unlock(io);
        var read_bytes: usize = 0;
        for (0..budget) |_| {
            try cancellation.check();
            if (read_bytes >= backup.max_manifest_bytes) return false;
            if (self.reaper) |*value| {
                if (try value.step()) {
                    value.deinit();
                    self.reaper = null;
                }
                continue;
            }
            if (self.phase == .done) {
                self.deinit(io);
                return true;
            }
            if (self.phase == .trash) {
                if (try self.trash_iterator.?.next(io)) |entry| {
                    if (entry.kind != .directory) continue;
                    const path = try std.fs.path.join(self.a, &.{ self.root.?, ".trash", entry.name });
                    defer self.a.free(path);
                    self.reaper = try Reaper.init(self.a, io, path);
                } else self.phase = .live;
                continue;
            }
            if (self.phase == .chunks) {
                if (self.chunks_iterator == null) {
                    const path = try std.fs.path.join(self.a, &.{ self.root.?, ".chunk-cache" });
                    defer self.a.free(path);
                    self.chunks_dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
                        error.FileNotFound => {
                            self.phase = .done;
                            continue;
                        },
                        else => return err,
                    };
                    self.chunks_iterator = self.chunks_dir.?.iterate();
                }
                const entry = (try self.chunks_iterator.?.next(io)) orelse {
                    self.phase = .done;
                    continue;
                };
                if (entry.kind != .directory) continue;
                const expiry = std.fmt.parseInt(u64, entry.name, 10) catch continue;
                if (expiry +| 30_000 >= now) continue;
                const path = try std.fs.path.join(self.a, &.{ self.root.?, ".chunk-cache", entry.name });
                defer self.a.free(path);
                const name = try std.fmt.allocPrint(self.a, "chunks-{s}", .{entry.name});
                defer self.a.free(name);
                const target = try std.fs.path.join(self.a, &.{ self.root.?, ".trash", name });
                defer self.a.free(target);
                try std.Io.Dir.rename(.cwd(), path, .cwd(), target, io);
                self.reaper = try Reaper.init(self.a, io, target);
                self.chunks_iterator = self.chunks_dir.?.iterate();
                continue;
            }
            const entry = (try self.iterator.?.next(io)) orelse {
                self.phase = .chunks;
                continue;
            };
            if (entry.kind != .directory) continue;
            const staging = entry.name.len == 72 and std.mem.endsWith(u8, entry.name, ".staging");
            if (!staging and entry.name.len != 64) continue;
            var valid_id = true;
            for (entry.name[0..64]) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) {
                valid_id = false;
                break;
            };
            if (!valid_id) continue;
            const path = try std.fs.path.join(self.a, &.{ self.root.?, entry.name });
            defer self.a.free(path);
            if (!staging) {
                const manifest_path = try std.fs.path.join(self.a, &.{ path, "query-cut.json" });
                defer self.a.free(manifest_path);
                const bytes = backup.readFileAlloc(self.a, io, manifest_path, backup.max_manifest_bytes) catch |err| switch (err) {
                    // Invalid caches can still be in use. Treat them just like
                    // expired caches, with the same per-generation lease.
                    error.FileNotFound, error.StreamTooLong => null,
                    else => return err,
                };
                defer if (bytes) |v| self.a.free(v);
                if (bytes) |v| {
                    read_bytes += v.len;
                    const parsed = std.json.parseFromSlice(cut.Manifest, self.a, v, .{}) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => null,
                    };
                    if (parsed) |value| {
                        defer value.deinit();
                        if (value.value.request.expires_ms +| 30_000 >= now) continue;
                    }
                }
            }
            const lease_path = try std.fmt.allocPrint(self.a, "{s}/{s}.lease", .{ self.root.?, if (staging) entry.name[0..64] else entry.name });
            defer self.a.free(lease_path);
            const lease = try std.Io.Dir.cwd().createFile(io, lease_path, .{ .truncate = false });
            defer lease.close(io);
            if (!try lease.tryLock(io, .exclusive)) continue;
            defer lease.unlock(io);
            const target = try std.fs.path.join(self.a, &.{ self.root.?, ".trash", entry.name });
            defer self.a.free(target);
            try std.Io.Dir.rename(.cwd(), path, .cwd(), target, io);
            try fs.syncDirPortable(io, self.root.?);
            try self.dir.?.deleteFile(io, std.fs.path.basename(lease_path));
            self.reaper = try Reaper.init(self.a, io, target);
            // Directory cookies can change after retirement. Restart this
            // small private cache scan; deletion work itself always resumes.
            self.iterator = self.dir.?.iterate();
        }
        return false;
    }
};

const Reaper = struct {
    a: A,
    io: std.Io,
    root_len: usize,
    path: []u8,
    dir: ?std.Io.Dir,
    iterator: std.Io.Dir.Iterator,
    fn init(a: A, io: std.Io, root: []const u8) !Reaper {
        const path = try a.dupe(u8, root);
        errdefer a.free(path);
        const dir = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true, .follow_symlinks = false });
        return .{ .a = a, .io = io, .root_len = path.len, .path = path, .dir = dir, .iterator = dir.iterate() };
    }
    fn deinit(self: *Reaper) void {
        if (self.dir) |dir| dir.close(self.io);
        self.a.free(self.path);
    }
    fn step(self: *Reaper) !bool {
        const dir = self.dir.?;
        if (try self.iterator.next(self.io)) |entry| {
            if (entry.kind == .directory) {
                const path = try std.fs.path.join(self.a, &.{ self.path, entry.name });
                errdefer self.a.free(path);
                const child = try dir.openDir(self.io, entry.name, .{ .iterate = true, .follow_symlinks = false });
                dir.close(self.io);
                self.a.free(self.path);
                self.path = path;
                self.dir = child;
                self.iterator = child.iterate();
            } else try dir.deleteFile(self.io, entry.name);
            return false;
        }
        // Removing entries can invalidate filesystem directory cookies. Retry
        // a nonempty directory from its beginning instead of dropping work.
        std.Io.Dir.cwd().deleteDir(self.io, self.path) catch |err| switch (err) {
            error.DirNotEmpty => {
                self.iterator = dir.iterate();
                return false;
            },
            else => return err,
        };
        dir.close(self.io);
        self.dir = null;
        if (self.path.len == self.root_len) return true;
        const parent = try self.a.dupe(u8, std.fs.path.dirname(self.path).?);
        self.a.free(self.path);
        self.path = parent;
        const opened = try std.Io.Dir.cwd().openDir(self.io, parent, .{ .iterate = true, .follow_symlinks = false });
        self.dir = opened;
        self.iterator = opened.iterate();
        return false;
    }
};
