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
const routing = @import("api/table_catalog.zig");
const metadata = @import("metadata/api.zig");
const records = @import("metadata/table_manager.zig");
const samples = 5;
const requests = 100;
fn median(values: *[samples]i96) f64 {
    std.mem.sort(i96, values, {}, std.sort.asc(i96));
    return @floatFromInt(values[samples / 2]);
}
fn query(generation: *routing.RoutingGeneration, alloc: std.mem.Allocator, name: []const u8) !void {
    var session = generation.session(alloc, routing.emptyCatalogSource(), true);
    defer session.deinit();
    var selected = try session.documentKeyRoutes(alloc, name, &.{ "a", "m", "z" }, null);
    defer selected.span.deinit(alloc);
    defer alloc.free(selected.key_groups);
    std.mem.doNotOptimizeAway(selected.span);
}
pub fn main() !void {
    const alloc = std.heap.c_allocator;
    var runtime = std.Io.Threaded.init(alloc, .{});
    defer runtime.deinit();
    const io = runtime.io();
    for ([_]usize{ 10, 1000, 10000 }) |n| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const tables = try a.alloc(records.TableRecord, n);
        const ranges = try a.alloc(records.RangeRecord, n);
        for (tables, ranges, 0..) |*table, *range, i| {
            table.* = .{ .table_id = i + 1, .name = try std.fmt.allocPrint(a, "table-{d}", .{i}) };
            range.* = .{ .group_id = i + 7001, .table_id = i + 1, .start_key = "" };
        }
        const snapshot = metadata.CatalogRoutingSnapshot{ .metadata_group_id = 1, .catalog_revision = 7, .tables = tables, .ranges = ranges };
        const retained = try routing.RoutingGeneration.create(alloc, snapshot, .{});
        defer retained.release();
        var rebuilt_ns: [samples]i96 = undefined;
        var retained_ns: [samples]i96 = undefined;
        for (0..samples) |sample| {
            var start = std.Io.Clock.now(.awake, io).nanoseconds;
            for (0..requests) |_| try query(try routing.RoutingGeneration.create(alloc, snapshot, .{}), alloc, tables[n - 1].name);
            rebuilt_ns[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
            start = std.Io.Clock.now(.awake, io).nanoseconds;
            for (0..requests) |_| {
                retained.retain();
                try query(retained, alloc, tables[n - 1].name);
            }
            retained_ns[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
        }
        std.debug.print("tables={d} samples={d} requests={d} rebuilt_snapshot_query_us={d:.3} retained_snapshot_query_us={d:.3}\n", .{ n, samples, requests, median(&rebuilt_ns) / requests / 1000, median(&retained_ns) / requests / 1000 });
    }
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
