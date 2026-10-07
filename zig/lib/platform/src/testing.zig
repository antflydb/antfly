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

//! Test I/O for the repository-owned Windows executor. Other targets retain std.testing.
const std = @import("std");
const builtin = @import("builtin");
const Threaded = @import("root.zig").Threaded;
var windows_instance: Threaded = instance: {
    var result = Threaded.init_single_threaded;
    result.allocator = std.heap.page_allocator;
    result.concurrent_limit = .unlimited;
    break :instance result;
};
pub const io = if (builtin.os.tag == .windows) windows_instance.io() else std.testing.io;
pub fn init(options: Threaded.InitOptions) void {
    if (builtin.os.tag == .windows) windows_instance = .init(std.testing.allocator, options);
}
pub fn deinit() void {
    if (builtin.os.tag == .windows) windows_instance.deinit();
}
pub const TmpDir = if (builtin.os.tag == .windows) WindowsTmpDir else std.testing.TmpDir;
pub fn tmpDir(options: std.Io.Dir.OpenOptions) TmpDir {
    if (builtin.os.tag != .windows) return std.testing.tmpDir(options);
    var random: [12]u8 = undefined;
    io.random(&random);
    var path: [16]u8 = undefined;
    _ = std.base64.url_safe.Encoder.encode(&path, &random);
    var cache = std.Io.Dir.cwd().createDirPathOpen(io, ".zig-cache", .{}) catch @panic("cannot create test cache");
    defer cache.close(io);
    const parent = cache.createDirPathOpen(io, "tmp", .{}) catch @panic("cannot create test parent");
    const dir = parent.createDirPathOpen(io, &path, .{ .open_options = options }) catch @panic("cannot create test directory");
    return .{ .dir = dir, .parent_dir = parent, .sub_path = path };
}
const WindowsTmpDir = struct {
    dir: std.Io.Dir,
    parent_dir: std.Io.Dir,
    sub_path: [16]u8,
    pub const parent_dir_path = ".zig-cache" ++ std.fs.path.sep_str ++ "tmp";
    pub fn cleanup(self: *WindowsTmpDir) void {
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        self.dir.close(io);
        self.parent_dir.deleteTree(io, &self.sub_path) catch {};
        self.parent_dir.close(io);
        self.* = undefined;
    }
};
