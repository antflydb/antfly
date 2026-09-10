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

// Run: zig build antfly-system-catalog-bench
const std = @import("std");
const catalog = @import("system_catalog");
const samples = 5;
const lookups = 1000;

fn median(values: *[samples]i96) f64 {
    std.mem.sort(i96, values, {}, std.sort.asc(i96));
    return @as(f64, @floatFromInt(values[samples / 2]));
}
pub fn main() !void {
    const alloc = std.heap.page_allocator;
    var runtime = std.Io.Threaded.init(alloc, .{});
    defer runtime.deinit();
    const io = runtime.io();
    for ([_]usize{ 1000, 10000, 100000 }) |n| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const resources = try a.alloc(catalog.Resource, n);
        const tables = try a.alloc(catalog.PhysicalTable, n);
        for (resources, tables, 0..) |*r, *t, i| {
            const name = try std.fmt.allocPrint(a, "table-{d}", .{i});
            const physical = try std.fmt.allocPrint(a, "table:{d}", .{i});
            r.* = .{ .kind = .table, .id = i + 100, .parent_id = 2, .name = name, .storage_name = physical };
            t.* = .{ .id = i + 100, .name = physical };
        }
        const state: catalog.State = .{ .resources = resources, .next_id = n + 100 };
        var index = try catalog.StateIndex.init(alloc, state);
        defer index.deinit(alloc);
        var rename_ns: [samples]i96 = undefined;
        var scan_ns: [samples]i96 = undefined;
        var indexed_ns: [samples]i96 = undefined;
        for (0..samples) |sample| {
            var start = std.Io.Clock.now(.awake, io).nanoseconds;
            var delta = try catalog.plan(alloc, state, .{ .action = .rename, .kind = .table, .name = resources[n - 1].name, .new_name = "renamed" }, tables);
            std.mem.doNotOptimizeAway(delta.upserts);
            delta.deinit(alloc);
            rename_ns[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
            start = std.Io.Clock.now(.awake, io).nanoseconds;
            for (0..lookups) |i| std.mem.doNotOptimizeAway(state.find(.table, 2, resources[(i * 7919) % n].name));
            scan_ns[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
            start = std.Io.Clock.now(.awake, io).nanoseconds;
            for (0..lookups) |i| std.mem.doNotOptimizeAway(index.find(.table, 2, resources[(i * 7919) % n].name));
            indexed_ns[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
        }
        std.debug.print("tables={d} samples={d} rename_median_ms={d:.3} scan_lookup_ns={d:.1} indexed_lookup_ns={d:.1}\n", .{ n, samples, median(&rename_ns) / 1e6, median(&scan_ns) / lookups, median(&indexed_ns) / lookups });
    }
    // Tenant offboarding: many empty namespaces in one database, alongside
    // another database's tables. Include both planning and standalone apply.
    for ([_]usize{ 1000, 10000 }) |n| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const resources = try a.alloc(catalog.Resource, 2 * n + 1);
        resources[0] = .{ .kind = .database, .id = 10, .name = "retired" };
        for (0..n) |i| {
            resources[1 + i] = .{ .kind = .namespace, .id = i + 100, .parent_id = 10, .name = try std.fmt.allocPrint(a, "namespace_{d}", .{i}) };
            resources[1 + n + i] = .{ .kind = .table, .id = i + n + 100, .parent_id = 2, .name = try std.fmt.allocPrint(a, "table_{d}", .{i}), .storage_name = try std.fmt.allocPrint(a, "table:{d}", .{i}) };
        }
        const state: catalog.State = .{ .revision = 1, .resources = resources, .next_id = 2 * n + 100 };
        var drop_ns: [samples]i96 = undefined;
        for (0..samples) |sample| {
            const start = std.Io.Clock.now(.awake, io).nanoseconds;
            var delta = try catalog.plan(alloc, state, .{ .action = .drop, .kind = .database, .name = "retired" }, &.{});
            defer delta.deinit(alloc);
            var next = try catalog.applyDeltaStateAlloc(alloc, state, delta);
            defer next.deinit();
            std.mem.doNotOptimizeAway(next.value.resources);
            drop_ns[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
        }
        std.debug.print("namespaces={d} unrelated_tables={d} samples={d} drop_plan_apply_median_ms={d:.3}\n", .{ n, n, samples, median(&drop_ns) / 1e6 });
    }
}
