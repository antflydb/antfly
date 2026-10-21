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

//! Owner-only scratch files. POSIX files are unlinked immediately, including on crashes.
const std = @import("std");
pub const File = struct {
    file: std.Io.File,
    path: ?[]const u8 = null,
    pub fn create(a: std.mem.Allocator, io: std.Io, directory: ?[]const u8) !File {
        var nonce: [16]u8 = undefined;
        io.random(&nonce);
        const path = try std.fmt.allocPrint(a, "{s}/antfly-grep-{s}.tmp", .{ directory orelse if (@import("builtin").os.tag == .windows) "." else "/tmp", std.fmt.bytesToHex(nonce, .lower) });
        errdefer a.free(path);
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .exclusive = true, .permissions = if (@import("builtin").os.tag == .windows) .default_file else .fromMode(0o600) });
        errdefer file.close(io);
        errdefer std.Io.Dir.cwd().deleteFile(io, path) catch {};
        if (@import("builtin").os.tag == .windows) return .{ .file = file, .path = path };
        try std.Io.Dir.cwd().deleteFile(io, path);
        a.free(path);
        return .{ .file = file };
    }
    pub fn deinit(self: *File, a: std.mem.Allocator, io: std.Io) void {
        self.file.close(io);
        if (self.path) |path| {
            std.Io.Dir.cwd().deleteFile(io, path) catch {};
            a.free(path);
        }
    }
};
