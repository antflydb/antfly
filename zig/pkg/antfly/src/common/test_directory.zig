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

/// Owns a temporary parent around a database/file path. Locks, staging roots,
/// and other siblings of path() are removed along with the fixture.
pub const TestDirectory = struct {
    tmp: std.testing.TmpDir,
    path_buffer: [std.fs.max_path_bytes]u8,
    path_len: usize,

    pub fn init(comptime name: []const u8) !TestDirectory {
        comptime std.debug.assert(name.len > 0 and std.mem.indexOfAny(u8, name, "/\\") == null and
            !std.mem.eql(u8, name, ".") and !std.mem.eql(u8, name, ".."));
        var result: TestDirectory = .{
            .tmp = std.testing.tmpDir(.{}),
            .path_buffer = undefined,
            .path_len = undefined,
        };
        errdefer result.tmp.cleanup();
        const root_len = try result.tmp.dir.realPath(std.testing.io, &result.path_buffer);
        const suffix = try std.fmt.bufPrintZ(result.path_buffer[root_len..], "/{s}", .{name});
        result.path_len = root_len + suffix.len;
        return result;
    }

    pub fn path(self: *const TestDirectory) [:0]const u8 {
        return self.path_buffer[0..self.path_len :0];
    }

    pub fn cleanup(self: *TestDirectory) void {
        self.tmp.cleanup();
        self.* = undefined;
    }
};

test "test directory isolates identical child names and removes sibling files" {
    var first = try TestDirectory.init("db");
    var first_active = true;
    defer if (first_active) first.cleanup();
    var second = try TestDirectory.init("db");
    defer second.cleanup();
    try std.testing.expect(!std.mem.eql(u8, first.path(), second.path()));

    try std.Io.Dir.cwd().createDirPath(std.testing.io, first.path());
    const lock_path = try std.fmt.allocPrint(std.testing.allocator, "{s}.lock", .{first.path()});
    defer std.testing.allocator.free(lock_path);
    const file = try std.Io.Dir.cwd().createFile(std.testing.io, lock_path, .{});
    file.close(std.testing.io);
    first.cleanup();
    first_active = false;
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, lock_path, .{}));
    try second.tmp.dir.access(std.testing.io, ".", .{});
}
