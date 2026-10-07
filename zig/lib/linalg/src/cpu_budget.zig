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

const std = @import("std");
const builtin = @import("builtin");

pub fn quotaThreads(quota: []const u8, period: []const u8) ?usize {
    const q = std.fmt.parseUnsigned(u64, std.mem.trim(u8, quota, " \t\r\n"), 10) catch return null;
    const p = std.fmt.parseUnsigned(u64, std.mem.trim(u8, period, " \t\r\n"), 10) catch return null;
    if (q == 0 or p == 0) return null;
    return @intCast(@max(1, @min(q / p, std.math.maxInt(usize))));
}

fn read(path: []const u8, buf: []u8) ?[]const u8 {
    const fd = std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0) catch return null;
    defer _ = std.posix.system.close(fd);
    var used: usize = 0;
    while (used < buf.len) {
        const n = std.posix.read(fd, buf[used..]) catch return null;
        if (n == 0) return buf[0..used];
        used += n;
    }
    return null; // Do not parse a truncated proc file.
}

fn containsController(list: []const u8, name: []const u8) bool {
    var it = std.mem.splitScalar(u8, list, ',');
    while (it.next()) |item| if (std.mem.eql(u8, item, name)) return true;
    return false;
}

fn safePath(path: []const u8) bool {
    if (path.len == 0 or path[0] != '/' or std.mem.indexOfScalar(u8, path, 0) != null) return false;
    var parts = std.mem.splitScalar(u8, path, '/');
    while (parts.next()) |part| if (std.mem.eql(u8, part, "..") or std.mem.eql(u8, part, ".")) return false;
    return true;
}

fn decodePath(encoded: []const u8, buf: []u8) ?[]const u8 {
    var i: usize = 0;
    var n: usize = 0;
    while (i < encoded.len) : (i += 1) {
        if (n == buf.len) return null;
        if (encoded[i] == '\\') {
            if (i + 3 >= encoded.len) return null;
            buf[n] = std.fmt.parseUnsigned(u8, encoded[i + 1 .. i + 4], 8) catch return null;
            i += 3;
        } else buf[n] = encoded[i];
        n += 1;
    }
    return if (safePath(buf[0..n])) buf[0..n] else null;
}

fn hierarchyLimit(mount: []const u8, root: []const u8, process: []const u8, v2: bool, limit: usize) usize {
    if (!safePath(process)) return limit;
    // Cgroup namespaces can expose a path relative to the namespace root.
    const relative = if (std.mem.eql(u8, root, "/")) process else if (std.mem.eql(u8, root, process)) "/" else if (std.mem.startsWith(u8, process, root) and process.len > root.len and process[root.len] == '/') process[root.len..] else process;
    var directory_buf: [4096]u8 = undefined;
    const base = std.mem.trimEnd(u8, mount, "/");
    const directory = std.fmt.bufPrint(&directory_buf, "{s}{s}", .{ base, if (std.mem.eql(u8, relative, "/")) "" else relative }) catch return limit;
    var end = directory.len;
    var result = limit;
    while (end >= base.len) {
        var path: [4096]u8 = undefined;
        var bytes: [128]u8 = undefined;
        const file = std.fmt.bufPrint(&path, "{s}/{s}", .{ directory[0..end], if (v2) "cpu.max" else "cpu.cfs_quota_us" }) catch return result;
        if (read(file, &bytes)) |value| {
            if (v2) {
                var parts = std.mem.tokenizeAny(u8, value, " \t\r\n");
                const q = parts.next() orelse "";
                const p = parts.next() orelse "";
                if (quotaThreads(q, p)) |threads| result = @min(result, threads);
            } else {
                var period_buf: [128]u8 = undefined;
                const period_file = std.fmt.bufPrint(&path, "{s}/cpu.cfs_period_us", .{directory[0..end]}) catch return result;
                if (read(period_file, &period_buf)) |period| {
                    if (quotaThreads(value, period)) |threads| result = @min(result, threads);
                }
            }
        }
        if (end == base.len) break;
        end = std.mem.lastIndexOfScalar(u8, directory[0..end], '/') orelse break;
    }
    return result;
}

pub fn effective(affinity: usize) usize {
    if (comptime builtin.os.tag != .linux or builtin.cpu.arch != .x86_64) return affinity;
    var result: usize = @max(1, @min(affinity, 8));
    if (builtin.link_libc) {
        if (std.c.getenv("ANTFLY_INFERENCE_CPU_THREADS")) |raw| {
            const n = std.fmt.parseUnsigned(usize, std.mem.span(raw), 10) catch @panic("invalid ANTFLY_INFERENCE_CPU_THREADS");
            if (n == 0 or n > 8) @panic("ANTFLY_INFERENCE_CPU_THREADS must be 1..8");
            result = @min(result, n);
        }
    }
    var cgroup_buf: [8192]u8 = undefined;
    const cgroups = read("/proc/self/cgroup", &cgroup_buf) orelse return result;
    var v1_path: ?[]const u8 = null;
    var v2_path: ?[]const u8 = null;
    var lines = std.mem.splitScalar(u8, cgroups, '\n');
    while (lines.next()) |line| {
        var fields = std.mem.splitScalar(u8, line, ':');
        _ = fields.next() orelse continue;
        const controllers = fields.next() orelse continue;
        const path = fields.next() orelse continue;
        if (controllers.len == 0) v2_path = path;
        if (containsController(controllers, "cpu")) v1_path = path;
    }
    var mounts_buf: [65536]u8 = undefined;
    const mounts = read("/proc/self/mountinfo", &mounts_buf) orelse return result;
    lines = std.mem.splitScalar(u8, mounts, '\n');
    while (lines.next()) |line| {
        const sep = std.mem.indexOf(u8, line, " - ") orelse continue;
        var after = std.mem.tokenizeScalar(u8, line[sep + 3 ..], ' ');
        const fs = after.next() orelse continue;
        _ = after.next() orelse continue;
        const options = after.next() orelse "";
        const v2 = std.mem.eql(u8, fs, "cgroup2");
        if (!v2 and !(std.mem.eql(u8, fs, "cgroup") and containsController(options, "cpu"))) continue;
        const process = (if (v2) v2_path else v1_path) orelse continue;
        var before = std.mem.tokenizeScalar(u8, line[0..sep], ' ');
        for (0..3) |_| _ = before.next() orelse break;
        var root_buf: [4096]u8 = undefined;
        var mount_buf: [4096]u8 = undefined;
        const root = decodePath(before.next() orelse continue, &root_buf) orelse continue;
        const mount = decodePath(before.next() orelse continue, &mount_buf) orelse continue;
        result = hierarchyLimit(mount, root, process, v2, result);
    }
    return result;
}

test "CPU quota uses whole cores with a one-worker floor" {
    try std.testing.expectEqual(@as(?usize, 2), quotaThreads("250000", "100000"));
    try std.testing.expectEqual(@as(?usize, 1), quotaThreads("50000", "100000"));
    try std.testing.expectEqual(@as(?usize, null), quotaThreads("max", "100000"));
    try std.testing.expectEqual(@as(?usize, null), quotaThreads("-1", "100000"));
    try std.testing.expectEqual(@as(?usize, null), quotaThreads("1", "0"));
    var buf: [128]u8 = undefined;
    try std.testing.expectEqualStrings("/cpu mount", decodePath("/cpu\\040mount", &buf).?);
    try std.testing.expect(decodePath("/../cpu", &buf) == null);
    try std.testing.expect(!containsController("cpuset", "cpu"));
}

test "CPU quota follows nested and subtree-mounted cgroup ancestors" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    try tmp.dir.createDirPath(io, "parent/child");
    try tmp.dir.writeFile(io, .{ .sub_path = "cpu.max", .data = "300000 100000\n" });
    try tmp.dir.writeFile(io, .{ .sub_path = "parent/cpu.max", .data = "150000 100000\n" });
    try tmp.dir.writeFile(io, .{ .sub_path = "parent/child/cpu.max", .data = "max 100000\n" });
    var buffer: [4096]u8 = undefined;
    const mount = buffer[0..try tmp.dir.realPath(io, &buffer)];
    try std.testing.expectEqual(@as(usize, 1), hierarchyLimit(mount, "/", "/parent/child", true, 8));
    try std.testing.expectEqual(@as(usize, 1), hierarchyLimit(mount, "/container", "/container/parent/child", true, 8));
    try std.testing.expectEqual(@as(usize, 3), hierarchyLimit(mount, "/container", "/", true, 8));
    try tmp.dir.writeFile(io, .{ .sub_path = "cpu.cfs_quota_us", .data = "250000\n" });
    try tmp.dir.writeFile(io, .{ .sub_path = "cpu.cfs_period_us", .data = "100000\n" });
    try tmp.dir.writeFile(io, .{ .sub_path = "parent/cpu.cfs_quota_us", .data = "-1\n" });
    try tmp.dir.writeFile(io, .{ .sub_path = "parent/cpu.cfs_period_us", .data = "100000\n" });
    try std.testing.expectEqual(@as(usize, 2), hierarchyLimit(mount, "/", "/parent/child", false, 8));
}
