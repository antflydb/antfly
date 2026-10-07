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

const native_platform = @import("antfly_platform");
const std = @import("std");

const fs_paths = @import("antfly_runtime_fs").fs_paths;
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Discard unpublished snapshot files even if cancellation is pending. Keep
/// protection scoped to deletion so callers retain their cancellation state.
pub fn cleanup(io: Io, root: []const u8) void {
    const previous = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(previous);
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
}

pub fn createRoot(alloc: Allocator, io: Io, parent: []const u8, id: []const u8) ![]u8 {
    return createRootWithSync(alloc, io, parent, id, syncParent);
}

fn syncParent(io: Io, parent: []const u8) !void {
    try fs_paths.syncDirPortable(io, parent);
}

fn createRootWithSync(alloc: Allocator, io: Io, parent: []const u8, id: []const u8, comptime sync: fn (Io, []const u8) anyerror!void) ![]u8 {
    for (0..64) |_| {
        var entropy: [8]u8 = undefined;
        try native_platform.entropy.fill(io, &entropy);
        const nonce = std.fmt.bytesToHex(entropy, .lower);
        const candidate = try std.fmt.allocPrint(alloc, "{s}/.{s}.staging-{s}", .{ parent, id, &nonce });
        errdefer alloc.free(candidate);
        std.Io.Dir.cwd().createDir(io, candidate, .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {
                alloc.free(candidate);
                continue;
            },
            else => return err,
        };
        errdefer cleanup(io, candidate);
        try sync(io, parent);
        return candidate;
    }
    return error.SnapshotStagingCollision;
}

test "snapshot staging cleanup removes nested files and preserves pending cancellation" {
    const alloc = std.testing.allocator;
    var pool = native_platform.Threaded.init(alloc, .{ .concurrent_limit = .limited(2) });
    defer pool.deinit();
    const io = pool.io();
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "survivor", .data = "published" });
    const parent = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(parent);
    const root = try createRoot(alloc, io, parent, "discarded");
    defer alloc.free(root);
    defer cleanup(io, root);
    const nested = try std.fmt.allocPrint(alloc, "{s}/nested", .{root});
    defer alloc.free(nested);
    try fs_paths.createDirPathPortable(io, nested);
    const payload = try std.fmt.allocPrint(alloc, "{s}/payload", .{nested});
    defer alloc.free(payload);
    try Io.Dir.cwd().writeFile(io, .{ .sub_path = payload, .data = "partial backup" });
    const State = struct {
        started: Io.Event = .unset,
        gate: Io.Event = .unset,
        restored: bool = false,
        fn run(i: Io, self: *@This(), path: []const u8) void {
            self.started.set(i);
            self.gate.wait(i) catch i.recancel();
            cleanup(i, path);
            i.checkCancel() catch |err| {
                self.restored = err == error.Canceled;
            };
        }
    };
    var state: State = .{};
    var task = try io.concurrent(State.run, .{ io, &state, root });
    state.started.waitUncancelable(io);
    task.cancel(io);
    try std.testing.expectError(error.FileNotFound, Io.Dir.cwd().access(io, root, .{}));
    try std.testing.expect(state.restored);
    const survivor = try tmp.dir.readFileAlloc(io, "survivor", alloc, .limited(32));
    defer alloc.free(survivor);
    try std.testing.expectEqualStrings("published", survivor);
}

test "snapshot staging creation cleans up when parent sync fails with cancellation pending" {
    const alloc = std.testing.allocator;
    var pool = native_platform.Threaded.init(alloc, .{ .concurrent_limit = .limited(2) });
    defer pool.deinit();
    const io = pool.io();
    var tmp = native_platform.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const parent = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(parent);
    const State = struct {
        started: Io.Event = .unset,
        gate: Io.Event = .unset,
        err: ?anyerror = null,
        restored: bool = false,
        threadlocal var current: ?*@This() = null;
        fn failSync(i: Io, _: []const u8) !void {
            const self = current.?;
            self.started.set(i);
            self.gate.wait(i) catch i.recancel();
            return error.InputOutput;
        }
        fn run(i: Io, self: *@This(), a: Allocator, path: []const u8) void {
            current = self;
            defer current = null;
            const root = createRootWithSync(a, i, path, "failed", failSync) catch |err| {
                self.err = err;
                i.checkCancel() catch |c| {
                    self.restored = c == error.Canceled;
                };
                return;
            };
            defer a.free(root);
            cleanup(i, root);
        }
    };
    var state: State = .{};
    var task = try io.concurrent(State.run, .{ io, &state, alloc, parent });
    state.started.waitUncancelable(io);
    task.cancel(io);
    try std.testing.expectEqual(error.InputOutput, state.err.?);
    var iterator = tmp.dir.iterate();
    try std.testing.expect((try iterator.next(io)) == null);
    try std.testing.expect(state.restored);
}
