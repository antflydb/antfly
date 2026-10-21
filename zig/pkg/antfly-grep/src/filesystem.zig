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

//! Platform filesystem identity and creation times, using native OS interfaces.
const std = @import("std");
pub const Info = struct { device: u64, created: ?i96 };
pub fn info(a: std.mem.Allocator, io: std.Io, path: []const u8) !Info {
    const os = @import("builtin").os.tag;
    if (os == .windows) {
        const stat = try std.Io.Dir.cwd().statFile(io, path, .{});
        if (stat.kind == .directory) {
            var dir = try std.Io.Dir.cwd().openDir(io, path, .{});
            defer dir.close(io);
            return windowsInfo(dir.handle);
        } else {
            var file = try std.Io.Dir.cwd().openFile(io, path, .{});
            defer file.close(io);
            return windowsInfo(file.handle);
        }
    }
    const terminated = try a.dupeSentinel(u8, path, 0);
    defer a.free(terminated);
    if (os == .linux) {
        const linux = std.os.linux;
        var stat: linux.Statx = undefined;
        const result = linux.statx(linux.AT.FDCWD, terminated, 0, .{ .TYPE = true, .BTIME = true }, &stat);
        if (linux.errno(result) != .SUCCESS) return error.StatFailed;
        return .{ .device = (@as(u64, stat.dev_major) << 32) | stat.dev_minor, .created = if (stat.mask.BTIME) @as(i96, stat.btime.sec) * 1_000_000_000 + stat.btime.nsec else null };
    }
    var stat: std.c.Stat = undefined;
    if (std.c.stat(terminated, &stat) != 0) return error.StatFailed;
    const Unsigned = @Int(.unsigned, @bitSizeOf(@TypeOf(stat.dev)));
    const created: ?i96 = if (@hasDecl(std.c.Stat, "birthtime")) @as(i96, stat.birthtime().sec) * 1_000_000_000 + stat.birthtime().nsec else null;
    return .{ .device = @intCast(@as(Unsigned, @bitCast(stat.dev))), .created = created };
}
fn windowsInfo(handle: std.os.windows.HANDLE) !Info {
    const w = std.os.windows;
    var status: w.IO_STATUS_BLOCK = undefined;
    var buffer: [1024]u8 align(@alignOf(w.FILE.FS_VOLUME_INFORMATION)) = undefined;
    const code = w.ntdll.NtQueryVolumeInformationFile(handle, &status, &buffer, buffer.len, .Volume);
    if (code != .SUCCESS and code != .BUFFER_OVERFLOW) return error.StatFailed;
    const volume: *const w.FILE.FS_VOLUME_INFORMATION = @ptrCast(&buffer);
    var basic: w.FILE.BASIC_INFORMATION = undefined;
    if (w.ntdll.NtQueryInformationFile(handle, &status, &basic, @sizeOf(w.FILE.BASIC_INFORMATION), .Basic) != .SUCCESS) return error.StatFailed;
    return .{ .device = volume.VolumeSerialNumber, .created = (@as(i96, basic.CreationTime) - 116444736000000000) * 100 };
}

/// Read-only private mappings use copy-on-write OS mapping semantics.
/// Windows retains the standard library's native mapping owner.
pub const Mapping = struct {
    memory: []const u8,
    standard: ?std.Io.File.MemoryMap = null,
    pub fn open(io: std.Io, file: std.Io.File, len: usize) !Mapping {
        if (@import("builtin").os.tag == .windows) {
            const mapping = try file.createMemoryMap(io, .{ .len = len, .protection = .{ .read = true }, .populate = false });
            return .{ .memory = mapping.memory, .standard = mapping };
        }
        const address = std.c.mmap(null, len, .{ .READ = true }, .{ .TYPE = .PRIVATE }, file.handle, 0);
        if (address == std.c.MAP_FAILED) return error.MappingFailed;
        return .{ .memory = @as([*]const u8, @ptrCast(address))[0..len] };
    }
    pub fn deinit(self: *Mapping, io: std.Io) void {
        if (@import("builtin").os.tag == .windows) {
            self.standard.?.destroy(io);
        } else _ = std.c.munmap(@ptrCast(@alignCast(self.memory.ptr)), self.memory.len);
    }
};
