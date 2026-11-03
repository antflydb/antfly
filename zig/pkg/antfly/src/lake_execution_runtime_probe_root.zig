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

//! Source oracle plus independently linked native whole-attempt runtime probe.
pub const antfly_sources = @import("source_owner_physical.zig");
const std = @import("std");
pub const probe_metrics = @import("api/lake_execution_probe_metrics.zig");
pub var families: probe_metrics.Metrics = .{};
pub fn lakeExecutionProbeParallelJob() void {
    _ = families.parallel_jobs.fetchAdd(1, .monotonic);
}
pub fn lakeExecutionProbeFamily(comptime name: []const u8, elapsed: u64) void {
    families.record(name, elapsed);
}
var headroom_queries: std.atomic.Value(u64) = .init(0);
var headroom_mode: enum { auto, unknown, low } = .auto;
pub fn lakeExecutionProbeHeadroom(value: ?usize) ?usize {
    _ = headroom_queries.fetchAdd(1, .monotonic);
    return switch (headroom_mode) {
        .auto => value,
        .unknown => null,
        .low => if (value != null) 0 else null,
    };
}
pub fn headroomMode() []const u8 {
    return @tagName(headroom_mode);
}
pub fn headroomQueries() u64 {
    return headroom_queries.load(.monotonic);
}
pub fn resetHeadroomQueries() void {
    headroom_queries.store(0, .monotonic);
}
var linear_io: bool = true;
var io_calls: std.atomic.Value(u64) = .init(0);
pub fn runtimeIoDispatchLinear() bool {
    _ = io_calls.fetchAdd(1, .monotonic);
    return linear_io;
}
pub fn resetIoDispatchCalls() void {
    io_calls.store(0, .monotonic);
}
pub fn ioDispatchCalls() u64 {
    return io_calls.load(.monotonic);
}
pub fn ioDispatchIsLinear() bool {
    return linear_io;
}
pub fn main(init: @import("std").process.Init) !void {
    if (std.c.getenv("ANTFLY_LAKE_PROBE_HEADROOM")) |value| {
        if (std.mem.eql(u8, std.mem.span(value), "unknown")) headroom_mode = .unknown else if (std.mem.eql(u8, std.mem.span(value), "low")) headroom_mode = .low;
    }
    linear_io = if (std.c.getenv("ANTFLY_LAKE_PROBE_LINEAR_IO")) |value| !std.mem.eql(u8, std.mem.span(value), "0") else true;
    const a = std.heap.smp_allocator;
    const binary = try std.process.executablePathAlloc(init.io, a);
    defer a.free(binary);
    const provenance = try std.json.Stringify.valueAlloc(a, .{ .path = binary, .builtin_is_test = @import("builtin").is_test }, .{});
    defer a.free(provenance);
    std.debug.print("LAKE_RUNTIME_EXECUTABLE {s}\n", .{provenance});
    try @import("api/lake_execution_runtime_probe.zig").run(init.io);
}
