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

const std = @import("std");
const metadata = @import("table_manager.zig");

/// Borrowed, immutable inventory from one captured generation. Indexes never
/// resolve names against a newer catalog while a report is being collected.
pub const Index = struct {
    tables: []const metadata.TableRecord,
    ranges: []const metadata.RangeRecord,
    table_positions: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    range_positions: std.AutoHashMapUnmanaged(u64, usize) = .empty,

    pub fn init(alloc: std.mem.Allocator, tables: []const metadata.TableRecord, ranges: []const metadata.RangeRecord) !Index {
        var self: Index = .{ .tables = tables, .ranges = ranges };
        errdefer self.deinit(alloc);
        try self.table_positions.ensureTotalCapacity(alloc, @intCast(tables.len));
        try self.range_positions.ensureTotalCapacity(alloc, @intCast(ranges.len));
        for (tables, 0..) |record, i| {
            const entry = self.table_positions.getOrPutAssumeCapacity(record.table_id);
            if (entry.found_existing) return error.InvalidCatalogProjection;
            entry.value_ptr.* = i;
        }
        for (ranges, 0..) |record, i| {
            const entry = self.range_positions.getOrPutAssumeCapacity(record.group_id);
            if (entry.found_existing) return error.InvalidCatalogProjection;
            entry.value_ptr.* = i;
        }
        return self;
    }

    pub fn deinit(self: *Index, alloc: std.mem.Allocator) void {
        self.table_positions.deinit(alloc);
        self.range_positions.deinit(alloc);
    }
    pub fn range(self: *const Index, id: u64) ?metadata.RangeRecord {
        return self.ranges[self.range_positions.get(id) orelse return null];
    }
    pub fn table(self: *const Index, id: u64) ?metadata.TableRecord {
        return self.tables[self.table_positions.get(id) orelse return null];
    }
};

test "system catalog report collection retains captured identity and missing groups" {
    const alloc = std.testing.allocator;
    const tables = [_]metadata.TableRecord{.{ .table_id = 7, .name = "before" }};
    const ranges = [_]metadata.RangeRecord{ .{ .table_id = 7, .group_id = 101, .start_key = "" }, .{ .table_id = 99, .group_id = 102, .start_key = "" } };
    var index = try Index.init(alloc, &tables, &ranges);
    defer index.deinit(alloc);
    try std.testing.expectEqualStrings("before", index.table(index.range(101).?.table_id).?.name);
    try std.testing.expect(index.range(100) == null);
    try std.testing.expect(index.table(index.range(102).?.table_id) == null);
    try std.testing.expectError(error.InvalidCatalogProjection, Index.init(alloc, &.{ tables[0], tables[0] }, &ranges));
    try std.testing.expectError(error.InvalidCatalogProjection, Index.init(alloc, &tables, &.{ ranges[0], ranges[0] }));
}

test "store report workload benchmark collection indexes" {
    if (std.c.getenv("ANTFLY_CATALOG_REPORT_BENCH") == null) return;
    const alloc = std.heap.c_allocator;
    for ([_]usize{ 100, 1000, 10000 }) |count| {
        const tables = try alloc.alloc(metadata.TableRecord, count);
        defer alloc.free(tables);
        const ranges = try alloc.alloc(metadata.RangeRecord, count);
        defer alloc.free(ranges);
        for (tables, ranges, 0..) |*table, *range, i| {
            table.* = .{ .table_id = i + 1, .name = "tenant" };
            range.* = .{ .table_id = i + 1, .group_id = i + 100, .start_key = "" };
        }
        for ([_]bool{ false, true }) |indexed| {
            var samples: [9]u64 = undefined;
            for (&samples) |*sample| {
                const start = @import("antfly_platform").time.monotonicNs();
                var sum: u64 = 0;
                if (indexed) {
                    var index = try Index.init(alloc, tables, ranges);
                    defer index.deinit(alloc);
                    for (0..count) |i| sum += index.table(index.range(i + 100).?.table_id).?.table_id;
                } else {
                    for (0..count) |i| {
                        const id = for (ranges) |range| {
                            if (range.group_id == i + 100) break range.table_id;
                        } else unreachable;
                        for (tables) |table| {
                            if (table.table_id == id) {
                                sum += table.table_id;
                                break;
                            }
                        }
                    }
                }
                try std.testing.expectEqual(count * (count + 1) / 2, sum);
                sample.* = @import("antfly_platform").time.monotonicNs() - start;
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            std.debug.print("COLLECTION_BENCH groups={d} indexed={} p50_ms={d:.6}\n", .{ count, indexed, @as(f64, @floatFromInt(samples[4])) / 1e6 });
        }
    }
}
