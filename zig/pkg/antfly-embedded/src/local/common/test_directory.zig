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

/// An empty CI environment value disables the opt-in workspace just like an
/// absent variable. GitHub Actions clears job-scoped values by writing `NAME=`.
pub fn workspaceRoot() ?[]const u8 {
    const root = @import("antfly_platform").env.getenv("ANTFLY_TEST_WORKSPACE") orelse return null;
    return if (root.len == 0) null else root;
}

/// Opt-in workspace for correctness fixtures. Durability tests keep using
/// std.testing.tmpDir and therefore remain on the ordinary filesystem.
pub fn fastTmpDir(opts: std.Io.Dir.OpenOptions) std.testing.TmpDir {
    const root = workspaceRoot() orelse return std.testing.tmpDir(opts);
    std.debug.assert(std.fs.path.isAbsolute(root));
    var random: [12]u8 = undefined;
    std.testing.io.random(&random);
    var sub_path: [16]u8 = undefined;
    _ = std.base64.url_safe.Encoder.encode(&sub_path, &random);
    const parent = std.Io.Dir.cwd().openDir(std.testing.io, root, .{}) catch @panic("cannot open test workspace");
    const dir = parent.createDirPathOpen(std.testing.io, &sub_path, .{ .open_options = opts }) catch @panic("cannot create test workspace fixture");
    return .{ .dir = dir, .parent_dir = parent, .sub_path = sub_path };
}

/// Owns a temporary parent around a database/file path. Locks, staging roots,
/// and other siblings of path() are removed along with the fixture.
pub const TestDirectory = struct {
    tmp: std.testing.TmpDir,
    path_buffer: [std.fs.max_path_bytes]u8,
    path_len: usize,

    pub fn init(comptime name: []const u8) !TestDirectory {
        return initWithTmp(name, std.testing.tmpDir(.{}));
    }

    pub fn initFast(comptime name: []const u8) !TestDirectory {
        return initWithTmp(name, fastTmpDir(.{}));
    }

    fn initWithTmp(comptime name: []const u8, tmp: std.testing.TmpDir) !TestDirectory {
        comptime std.debug.assert(name.len > 0 and std.mem.indexOfAny(u8, name, "/\\") == null and
            !std.mem.eql(u8, name, ".") and !std.mem.eql(u8, name, ".."));
        var result: TestDirectory = .{
            .tmp = tmp,
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

test "test directory fast workspace isolates fixtures and cleans siblings" {
    var first = try TestDirectory.initFast("db");
    var active = true;
    defer if (active) first.cleanup();
    var second = try TestDirectory.initFast("db");
    defer second.cleanup();
    try std.testing.expect(!std.mem.eql(u8, first.path(), second.path()));
    if (workspaceRoot()) |root| {
        const resolved = try std.Io.Dir.cwd().realPathFileAlloc(std.testing.io, root, std.testing.allocator);
        defer std.testing.allocator.free(resolved);
        try std.testing.expect(std.mem.startsWith(u8, first.path(), resolved));
    }
    const sibling = try std.fmt.allocPrint(std.testing.allocator, "{s}.lock", .{first.path()});
    defer std.testing.allocator.free(sibling);
    const file = try std.Io.Dir.cwd().createFile(std.testing.io, sibling, .{});
    file.close(std.testing.io);
    first.cleanup();
    active = false;
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, sibling, .{}));
    try second.tmp.dir.access(std.testing.io, ".", .{});
}
