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

//! Compile-time opt-in profiling. No timers, storage or branches in normal builds.
//! Main-thread CPU excludes worker CPU; wall time includes output backpressure.
const std = @import("std");
const builtin = @import("builtin");
const enabled = @import("grep_cache_options").workspace_profile;
const cpu_supported = builtin.link_libc and (builtin.os.tag == .linux or builtin.os.tag == .macos);
pub const Phases = if (enabled) struct {
    const Self = @This();
    const Stamp = struct { wall: i128, cpu: i128 };
    const Sample = struct { name: []const u8, wall_ns: u64, cpu_ns: u64 };
    const Record = struct { scope: []const u8, phases: [16]Sample, count: usize };
    threadlocal var records: [8]Record = undefined;
    threadlocal var record_count: usize = 0;
    io: std.Io,
    scope: []const u8,
    previous: Stamp,
    samples: [16]Sample = undefined,
    count: usize = 0,
    fn now(io: std.Io) Stamp {
        var cpu: i128 = 0;
        if (comptime cpu_supported) {
            var ts: std.c.timespec = undefined;
            if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &ts) == 0) cpu = @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
        }
        return .{ .wall = std.Io.Clock.awake.now(io).nanoseconds, .cpu = cpu };
    }
    pub fn init(io: std.Io, scope: []const u8) Self {
        if (std.mem.eql(u8, scope, "request")) record_count = 0;
        return .{ .io = io, .scope = scope, .previous = now(io) };
    }
    pub fn mark(self: *Self, name: []const u8) void {
        const stamp = now(self.io);
        if (self.count < self.samples.len) {
            self.samples[self.count] = .{ .name = name, .wall_ns = @intCast(@max(0, stamp.wall - self.previous.wall)), .cpu_ns = @intCast(@max(0, stamp.cpu - self.previous.cpu)) };
            self.count += 1;
        }
        self.previous = stamp;
    }
    pub fn finish(self: *Self) void {
        self.mark("finish");
        if (record_count < records.len) {
            // Copy only initialized samples. Flush after the outer timestamp;
            // nested diagnostic encoding/writes never enter parent timings.
            var record = &records[record_count];
            record.scope = self.scope;
            record.count = self.count;
            @memcpy(record.phases[0..self.count], self.samples[0..self.count]);
            record_count += 1;
        }
        if (!std.mem.eql(u8, self.scope, "request")) return;
        defer record_count = 0;
        const Output = struct { scope: []const u8, phases: []const Sample };
        var output: [8]Output = undefined;
        for (records[0..record_count], 0..) |*record, i| output[i] = .{ .scope = record.scope, .phases = record.phases[0..record.count] };
        // One bounded line: diagnostics cannot interleave with protocol stdout.
        var bytes: [16384]u8 = undefined;
        var writer = std.Io.Writer.fixed(&bytes);
        std.json.Stringify.value(.{ .type = "workspace_profile", .thread_cpu_supported = cpu_supported, .records = output[0..record_count] }, .{}, &writer) catch return;
        std.Io.File.stderr().writeStreamingAll(self.io, writer.buffered()) catch return;
        std.Io.File.stderr().writeStreamingAll(self.io, "\n") catch {};
    }
} else struct {
    pub inline fn init(_: std.Io, _: []const u8) @This() {
        return .{};
    }
    pub inline fn mark(_: *@This(), _: []const u8) void {}
    pub inline fn finish(_: *@This()) void {}
};
