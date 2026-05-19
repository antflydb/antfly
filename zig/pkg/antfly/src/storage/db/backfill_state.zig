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

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../common/fs_paths.zig");
const storage_io = @import("../lsm_backend/storage_io.zig");

const rebuild_state_name = "rebuild.state";
var temp_nonce: u64 = 0;

pub const RebuildState = struct {
    root_path: []const u8,

    pub fn init(root_path: []const u8) RebuildState {
        return .{ .root_path = root_path };
    }

    pub fn check(self: RebuildState, alloc: Allocator) !?[]u8 {
        if (builtin.os.tag == .freestanding) {
            return null;
        }
        const path = try self.pathAlloc(alloc);
        defer alloc.free(path);

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        return std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(64 * 1024)) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn update(self: RebuildState, key: []const u8) !void {
        if (builtin.os.tag == .freestanding) {
            return;
        }
        const path = try self.pathAlloc(std.heap.page_allocator);
        defer std.heap.page_allocator.free(path);
        temp_nonce +%= 1;
        const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-{d}", .{
            path,
            temp_nonce,
        });
        defer std.heap.page_allocator.free(tmp_path);

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        try ensureParentDir(io, path);
        try ensureParentDir(io, tmp_path);

        try writeStateFile(io, tmp_path, key);

        if (std.fs.path.isAbsolute(path)) {
            renameAbsolutePortable(tmp_path, path) catch |err| switch (err) {
                error.FileNotFound => {
                    try ensureParentDir(io, path);
                    try writeStateFile(io, path, key);
                    std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
                },
                else => return err,
            };
        } else {
            std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| switch (err) {
                error.FileNotFound => {
                    try ensureParentDir(io, path);
                    try writeStateFile(io, path, key);
                    std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
                },
                else => return err,
            };
        }
    }

    pub fn clear(self: RebuildState) !void {
        if (builtin.os.tag == .freestanding) {
            return;
        }
        const path = try self.pathAlloc(std.heap.page_allocator);
        defer std.heap.page_allocator.free(path);

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        if (std.fs.path.isAbsolute(path)) {
            std.Io.Dir.deleteFileAbsolute(io_impl.io(), path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
        } else {
            std.Io.Dir.cwd().deleteFile(io_impl.io(), path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
        }
    }

    pub fn estimateProgress(self: RebuildState, range_start: []const u8, range_end: []const u8, alloc: Allocator) !?f64 {
        const resume_key = (try self.check(alloc)) orelse return null;
        defer alloc.free(resume_key);
        return estimateProgressForKey(range_start, range_end, resume_key);
    }

    pub fn pathAlloc(self: RebuildState, alloc: Allocator) ![]u8 {
        return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.root_path, rebuild_state_name });
    }
};

fn ensureParentDir(io: std.Io, path: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| {
        try storage_io.createDirPathPortable(io, parent);
    }
}

fn renameAbsolutePortable(old_path: []const u8, new_path: []const u8) !void {
    const allocator = std.heap.page_allocator;
    const old_path_z = try allocator.dupeZ(u8, old_path);
    defer allocator.free(old_path_z);
    const new_path_z = try allocator.dupeZ(u8, new_path);
    defer allocator.free(new_path_z);

    while (true) {
        const rc = std.posix.system.rename(old_path_z, new_path_z);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .BUSY => return error.FileBusy,
            .IO => return error.InputOutput,
            .INVAL => return error.InvalidArgument,
            .ISDIR => return error.IsDir,
            .LOOP => return error.SymLinkLoop,
            .MLINK => return error.LinkQuotaExceeded,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .NOMEM => return error.SystemResources,
            .NOSPC => return error.NoSpaceLeft,
            .NOTEMPTY, .EXIST => return error.PathAlreadyExists,
            .XDEV => return error.RenameAcrossMountPoints,
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn writeStateFile(io: std.Io, path: []const u8, key: []const u8) !void {
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);

    var buf: [4096]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(key);
    try writer.end();
}

pub fn estimateProgressForKey(range_start: []const u8, range_end: []const u8, current_key: []const u8) f64 {
    const start_val = keyToU64(range_start);
    const end_val = keyToU64(range_end);
    const cur_val = keyToU64(current_key);

    if (end_val <= start_val) return if (current_key.len == 0) 0.0 else 1.0;
    if (cur_val <= start_val) return 0.0;
    if (cur_val >= end_val) return 1.0;

    const range: f64 = @floatFromInt(end_val - start_val);
    const pos: f64 = @floatFromInt(cur_val - start_val);
    return pos / range;
}

fn keyToU64(key: []const u8) u64 {
    if (key.len == 0) return 0;
    var buf: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0 };
    const copy_len = @min(key.len, 8);
    @memcpy(buf[0..copy_len], key[0..copy_len]);
    return std.mem.readInt(u64, &buf, .big);
}

test "rebuild state round trips and clears" {
    const path = "/tmp/antfly-backfill-state-test";
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    try fs_paths.createDirPathPortable(io_impl.io(), path);

    const state = RebuildState.init(path);
    try std.testing.expect((try state.check(std.testing.allocator)) == null);
    try state.update("doc:m");
    const loaded = (try state.check(std.testing.allocator)) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(loaded);
    try std.testing.expectEqualStrings("doc:m", loaded);
    try state.clear();
    try std.testing.expect((try state.check(std.testing.allocator)) == null);
}

test "rebuild state estimates progress from resume key" {
    const progress = estimateProgressForKey("doc:a", "doc:z", "doc:m");
    try std.testing.expect(progress > 0.0);
    try std.testing.expect(progress < 1.0);
}
