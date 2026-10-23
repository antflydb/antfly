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

//! Honor scheduler affinity and hierarchical cgroup v2 CPU bandwidth quotas.
//! Explicit -j remains authoritative; this only sizes automatic local workers.
const std = @import("std");

pub fn cpuCount(a: std.mem.Allocator, io: std.Io) usize {
    const available = std.Thread.getCpuCount() catch 4;
    if (@import("builtin").os.tag != .linux) return @max(1, available);
    const groups = readControl(a, io, "/proc/self/cgroup", 16384) catch return @max(1, available);
    defer a.free(groups);
    var lines = std.mem.splitScalar(u8, groups, '\n');
    while (lines.next()) |line| if (std.mem.startsWith(u8, line, "0::/")) {
        return quotaCount(a, io, "/sys/fs/cgroup", line[4..], available) catch @max(1, available);
    };
    return @max(1, available);
}

/// procfs/sysfs report zero lengths. A positional regular-file reader can
/// interpret that metadata as EOF; explicitly stream these bounded controls.
fn readControl(a: std.mem.Allocator, io: std.Io, path: []const u8, limit: usize) ![]u8 {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = file.readerStreaming(io, &.{});
    return reader.interface.allocRemaining(a, .limited(limit));
}

fn quota(raw: []const u8) ?usize {
    var words = std.mem.tokenizeAny(u8, raw, " \t\r\n");
    const amount = std.fmt.parseInt(u64, words.next() orelse return null, 10) catch return null;
    const period = std.fmt.parseInt(u64, words.next() orelse return null, 10) catch return null;
    if (period == 0 or amount == 0 or words.next() != null) return null;
    const rounded = amount / period + @intFromBool(amount % period != 0);
    return @intCast(@min(rounded, std.math.maxInt(usize)));
}

pub fn quotaCount(a: std.mem.Allocator, io: std.Io, root: []const u8, group: []const u8, available: usize) !usize {
    const base = try std.fs.path.resolveAlloc(a, &.{root});
    defer a.free(base);
    const full = try std.fs.path.resolveAlloc(a, &.{ base, group });
    defer a.free(full);
    if (!std.mem.eql(u8, full, base) and !(std.mem.startsWith(u8, full, base) and full.len > base.len and full[base.len] == '/')) return @max(1, available);
    var count = @max(1, available);
    var at: ?[]const u8 = full;
    var depth: usize = 0;
    while (at) |directory| : (depth += 1) {
        if (depth >= 128) break;
        const path = try std.fs.path.join(a, &.{ directory, "cpu.max" });
        defer a.free(path);
        if (readControl(a, io, path, 256)) |raw| {
            defer a.free(raw);
            if (quota(raw)) |limit| count = @min(count, limit);
        } else |_| {}
        if (std.mem.eql(u8, directory, base)) break;
        at = std.fs.path.dirname(directory);
    }
    return count;
}

test "quota rounding and unlimited or malformed controls" {
    try std.testing.expectEqual(@as(?usize, 2), quota("150000 100000\n"));
    try std.testing.expectEqual(@as(?usize, 1), quota("100000 100000"));
    try std.testing.expectEqual(@as(?usize, 1), quota("1 100000"));
    for ([_][]const u8{ "max 100000", "0 100000", "100 0", "-1 100", "100 100 extra", "" }) |raw| try std.testing.expectEqual(@as(?usize, null), quota(raw));
}

test "ancestor quotas constrain descendants and affinity remains an upper bound" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    try temporary.dir.createDirPath(io, "parent/child");
    try temporary.dir.writeFile(io, .{ .sub_path = "cpu.max", .data = "max 100000" });
    try temporary.dir.writeFile(io, .{ .sub_path = "parent/cpu.max", .data = "150000 100000" });
    try temporary.dir.writeFile(io, .{ .sub_path = "parent/child/cpu.max", .data = "800000 100000" });
    const root = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    try std.testing.expectEqual(@as(usize, 2), try quotaCount(a, io, root, "parent/child", 32));
    try std.testing.expectEqual(@as(usize, 1), try quotaCount(a, io, root, "parent/child", 1));
    try std.testing.expectEqual(@as(usize, 32), try quotaCount(a, io, root, "../escape", 32));
}

test "Linux procfs controls are read despite zero reported file size" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const a = std.testing.allocator;
    const raw = readControl(a, std.testing.io, "/proc/self/cgroup", 16384) catch |err| switch (err) {
        error.FileNotFound, error.AccessDenied => return error.SkipZigTest,
        else => return err,
    };
    defer a.free(raw);
    try std.testing.expect(raw.len > 0);
    try std.testing.expect(std.mem.indexOfScalar(u8, raw, ':') != null);
}
