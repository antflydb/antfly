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

//! Server COPY owns its file descriptor and process group until completion.
//! All blocking work uses std.Io; stderr cannot fill an undrained pipe or leak
//! command credentials into a SQL diagnostic.
const std = @import("std");
const builtin = @import("builtin");
const Target = @import("antfly_local_sources").sql_ast.CopyTarget;
pub const Endpoint = struct {
    io: std.Io,
    file: std.Io.File,
    child: ?std.process.Child = null,
    group: ?std.posix.pid_t = null,
    input: bool,
    closed: bool = false,

    pub fn open(io: std.Io, target: Target, input: bool) !Endpoint {
        return switch (target) {
            .file => |path| blk: {
                if (!input and !std.fs.path.isAbsolute(path)) return error.SqlInvalidParameterValue;
                const file = (if (input) std.Io.Dir.cwd().openFile(io, path, .{}) else std.Io.Dir.cwd().createFile(io, path, .{ .permissions = .fromMode(0o600) })) catch |err| return fileError(err);
                break :blk .{ .io = io, .file = file, .input = input };
            },
            .program => |command| blk: {
                // std.Io currently exposes process-group creation only on
                // POSIX. Refuse platforms without group cleanup before spawn.
                if (comptime builtin.os.tag == .windows or builtin.os.tag == .wasi) return error.UnsupportedSqlExecution;
                var child = std.process.spawn(io, .{ .argv = &.{ "/bin/sh", "-c", command }, .stdin = if (input) .ignore else .pipe, .stdout = if (input) .pipe else .ignore, .stderr = .ignore, .pgid = 0 }) catch return error.SqlCopyProgramFailed;
                errdefer child.kill(io);
                break :blk .{ .io = io, .file = if (input) child.stdout.? else child.stdin.?, .child = child, .group = child.id, .input = input };
            },
        };
    }
    pub fn read(self: *Endpoint, buffer: []u8) !usize {
        return self.file.readStreaming(self.io, &.{buffer}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return fileError(err),
        };
    }
    pub fn fileError(err: anyerror) anyerror {
        return switch (err) {
            error.Canceled => error.Canceled,
            error.AccessDenied, error.PermissionDenied => error.Forbidden,
            error.FileNotFound => error.SqlCopyFileNotFound,
            else => error.SqlCopyIoFailure,
        };
    }
    fn closeFile(self: *Endpoint) void {
        if (self.closed) return;
        self.file.close(self.io);
        self.closed = true;
        if (self.child) |*child| {
            if (self.input) child.stdout = null else child.stdin = null;
        }
    }
    pub fn finish(self: *Endpoint) !void {
        // The producer's successful exit precedes commit. For consumers,
        // closing stdin delivers EOF before waiting for successful completion.
        self.closeFile();
        if (self.child) |*child| {
            const term = try child.wait(self.io);
            if (term != .exited or term.exited != 0) return error.SqlCopyProgramFailed;
        }
    }
    pub fn deinit(self: *Endpoint) void {
        // Cancellation cleanup must run even when the producing task has been
        // cancelled. Kill descendants as well as the immediate shell.
        const prior = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(prior);
        if (self.group) |group| {
            if (comptime builtin.os.tag != .windows and builtin.os.tag != .wasi) std.posix.kill(-group, .KILL) catch {};
        }
        if (self.child) |*child| child.kill(self.io);
        if (self.child != null) self.closed = true;
        self.closeFile();
    }
};

test "SQL server COPY endpoints stream files and require producer and consumer completion" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    const path = try std.fs.path.join(a, &.{ directory, "rows.csv" });
    defer a.free(path);
    {
        var output = try Endpoint.open(io, .{ .file = path }, false);
        defer output.deinit();
        try output.file.writeStreamingAll(io, "a,queued\n");
        try output.finish();
    }
    {
        var input = try Endpoint.open(io, .{ .file = path }, true);
        defer input.deinit();
        var buffer: [32]u8 = undefined;
        const n = try input.read(&buffer);
        try std.testing.expectEqualStrings("a,queued\n", buffer[0..n]);
        try std.testing.expectEqual(@as(usize, 0), try input.read(&buffer));
        try input.finish();
    }
    {
        var source = try Endpoint.open(io, .{ .program = "printf 'a\\tqueued\\n'; exit 7" }, true);
        defer source.deinit();
        var buffer: [32]u8 = undefined;
        const n = try source.file.readStreaming(io, &.{&buffer});
        try std.testing.expectEqualStrings("a\tqueued\n", buffer[0..n]);
        try std.testing.expectError(error.SqlCopyProgramFailed, source.finish());
    }
    {
        var sink = try Endpoint.open(io, .{ .program = "cat > /dev/null; exit 9" }, false);
        defer sink.deinit();
        try sink.file.writeStreamingAll(io, "a,queued\n");
        try std.testing.expectError(error.SqlCopyProgramFailed, sink.finish());
    }
    try std.testing.expectError(error.SqlInvalidParameterValue, Endpoint.open(io, .{ .file = "relative-copy-output" }, false));
}
