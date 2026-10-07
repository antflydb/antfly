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

/// Each producer owns its file. Exclusive creation also preserves evidence if
/// a PID is reused within a long-running trace collection.
pub fn openProcessFile(directory: []const u8, pid: std.c.pid_t) !std.c.fd_t {
    var path: [std.fs.max_path_bytes]u8 = undefined;
    var sequence: u32 = 0;
    while (true) : (sequence = try std.math.add(u32, sequence, 1)) {
        const name = try std.fmt.bufPrintSentinel(&path, "{s}/trace-{d}-{d}.ndjson", .{ directory, pid, sequence }, 0);
        const fd = std.c.open(name, .{ .ACCMODE = .WRONLY, .CREAT = true, .EXCL = true, .CLOEXEC = true }, @as(std.c.mode_t, 0o644));
        switch (std.posix.errno(fd)) {
            .SUCCESS => return fd,
            .EXIST => continue,
            .INTR => continue,
            else => return error.TraceFileOpenFailed,
        }
    }
}

test "trace files preserve overlapping producers and reused process identities" {
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(platform.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    const first = try openProcessFile(root, 11);
    defer _ = std.c.close(first);
    const line = "{\"event\":1}\n";
    try std.testing.expectEqual(@as(isize, line.len), std.c.write(first, line.ptr, line.len));
    const second = try openProcessFile(root, 22);
    defer _ = std.c.close(second);
    try std.testing.expectEqual(@as(isize, line.len), std.c.write(second, line.ptr, line.len));
    const reused = try openProcessFile(root, 11);
    defer _ = std.c.close(reused);
    try std.testing.expectEqual(@as(isize, line.len), std.c.write(reused, line.ptr, line.len));
    // Resume the original descriptor after both other producers have opened.
    try std.testing.expectEqual(@as(isize, line.len), std.c.write(first, line.ptr, line.len));
    for ([_][]const u8{ "trace-11-0.ndjson", "trace-22-0.ndjson", "trace-11-1.ndjson" }, [_][]const u8{ line ++ line, line, line }) |name, expected| {
        const bytes = try tmp.dir.readFileAlloc(platform.testing.io, name, std.testing.allocator, .limited(1024));
        defer std.testing.allocator.free(bytes);
        try std.testing.expectEqualStrings(expected, bytes);
    }
}
