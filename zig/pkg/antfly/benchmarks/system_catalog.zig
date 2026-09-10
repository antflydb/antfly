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
noinline fn scanTablespace(state: catalog.State, id: u64) ?catalog.Resource {
    return state.byId(.tablespace, id);
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
    // Tenant control-plane work: unrelated namespaces/tables must not enter
    // point rename planning or related-record lookup for each listed tenant.
    for ([_]usize{ 1000, 10000 }) |n| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const resources = try a.alloc(catalog.Resource, n * 2);
        for (0..n) |i| {
            resources[i * 2] = .{ .kind = .database, .id = i * 2 + 100, .name = try std.fmt.allocPrint(a, "tenant_{d}", .{i}) };
            resources[i * 2 + 1] = .{ .kind = .namespace, .id = i * 2 + 101, .parent_id = i * 2 + 100, .name = "public" };
        }
        const state: catalog.State = .{ .revision = 1, .next_id = n * 2 + 100, .resources = resources };
        var index = try catalog.StateIndex.init(alloc, state);
        defer index.deinit(alloc);
        var scan_listing: [samples]i96 = undefined;
        var indexed_listing: [samples]i96 = undefined;
        var rebuilt_plan: [samples]i96 = undefined;
        var indexed_plan: [samples]i96 = undefined;
        for (0..samples) |sample| {
            var start = std.Io.Clock.now(.awake, io).nanoseconds;
            for (index.list(.database, 0)) |record| std.mem.doNotOptimizeAway(scanTablespace(state, record.tablespace_id));
            scan_listing[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
            start = std.Io.Clock.now(.awake, io).nanoseconds;
            const projected = try catalog.projectRead(alloc, &index, .{ .kind = .database });
            std.mem.doNotOptimizeAway(projected);
            alloc.free(projected);
            indexed_listing[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
            const mutation: catalog.Mutation = .{ .action = .rename, .kind = .database, .name = resources[resources.len - 2].name, .new_name = "renamed" };
            start = std.Io.Clock.now(.awake, io).nanoseconds;
            var rebuilt = try catalog.plan(alloc, state, mutation, &.{});
            rebuilt.deinit(alloc);
            rebuilt_plan[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
            start = std.Io.Clock.now(.awake, io).nanoseconds;
            for (0..lookups) |_| {
                var delta = try catalog.planWithReader(alloc, catalog.MemoryReader{ .index = &index, .tables = &.{} }, state.next_id, mutation);
                std.mem.doNotOptimizeAway(delta.upserts);
                delta.deinit(alloc);
            }
            indexed_plan[sample] = std.Io.Clock.now(.awake, io).nanoseconds - start;
        }
        std.debug.print("tenants={d} listing_scan_ms={d:.3} listing_indexed_ms={d:.3} rename_rebuild_ms={d:.3} rename_indexed_us={d:.3}\n", .{ n, median(&scan_listing) / 1e6, median(&indexed_listing) / 1e6, median(&rebuilt_plan) / 1e6, median(&indexed_plan) / lookups / 1e3 });
    }
}
