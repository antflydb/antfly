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

const platform = @import("antfly_platform");
const std = @import("std");

test "Windows hardlinks preserve identity through relative directory handles and source removal" {
    const io = platform.testing.io;
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "source");
    try tmp.dir.createDirPath(io, "destination");
    var source = try tmp.dir.openDir(io, "source", .{});
    defer source.close(io);
    var destination = try tmp.dir.openDir(io, "destination", .{});
    defer destination.close(io);
    const file = try source.createFile(io, "日本語 original", .{ .read = true });
    defer file.close(io);
    try file.writeStreamingAll(io, "retained");
    try std.Io.Dir.hardLink(source, "日本語 original", destination, "pin", io, .{});
    const pinned = try destination.openFile(io, "pin", .{});
    defer pinned.close(io);
    const initial = try file.stat(io);
    const linked = try pinned.stat(io);
    try std.testing.expectEqual(initial.inode, linked.inode);
    try std.testing.expectEqual(initial.size, linked.size);
    try std.testing.expect(std.meta.eql(initial.mtime, linked.mtime));
    try std.testing.expectError(error.PathAlreadyExists, std.Io.Dir.hardLink(source, "日本語 original", destination, "pin", io, .{}));
    try file.hardLink(io, destination, "handle-pin", .{});
    const handle_pin = try destination.openFile(io, "handle-pin", .{});
    defer handle_pin.close(io);
    try std.testing.expectEqual(initial.inode, (try handle_pin.stat(io)).inode);
    try source.deleteFile(io, "日本語 original");
    var data: [8]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 8), try pinned.readPositionalAll(io, &data, 0));
    try std.testing.expectEqualStrings("retained", &data);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.hardLink(source, "missing", destination, "absent", io, .{}));
}

test "Windows hardlinks preserve caller I/O authority" {
    const State = struct {
        calls: usize = 0,
        fn link(ptr: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: std.Io.Dir, _: []const u8, _: std.Io.Dir.HardLinkOptions) std.Io.Dir.HardLinkError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            return error.AccessDenied;
        }
    };
    var state: State = .{};
    var vtable = std.Io.failing.vtable.*;
    vtable.dirHardLink = State.link;
    const io = std.Io{ .userdata = &state, .vtable = &vtable };
    try std.testing.expectError(error.AccessDenied, std.Io.Dir.hardLink(.cwd(), "source", .cwd(), "destination", io, .{}));
    try std.testing.expectEqual(@as(usize, 1), state.calls);
}

test "Windows hardlink cancellation prevents publication" {
    var pool = platform.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer pool.deinit();
    const io = pool.io();
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(io, "source", .{});
    file.close(io);
    const State = struct {
        started: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        result: ?anyerror = null,
        fn run(i: std.Io, self: *@This(), dir: std.Io.Dir) void {
            self.started.set(i);
            self.gate.wait(i) catch i.recancel();
            std.Io.Dir.hardLink(dir, "source", dir, "canceled-pin", i, .{}) catch |err| {
                self.result = err;
            };
        }
    };
    var state: State = .{};
    var child = try io.concurrent(State.run, .{ io, &state, tmp.dir });
    state.started.waitUncancelable(io);
    child.cancel(io);
    try std.testing.expectEqual(error.Canceled, state.result.?);
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "canceled-pin", .{}));
}
