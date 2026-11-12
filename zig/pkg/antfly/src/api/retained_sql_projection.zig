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

//! Compile the SQL scan's required retained columns once. Finalization can
//! fail (for example a binary64 SUM overflow), so unrequested cells stay lazy.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const types = local.retained_dataset_types;
const series = local.storage_retained_series;
const A = std.mem.Allocator;
pub const Projection = struct {
    const Dimension = struct { name: []const u8, index: usize };
    const Measure = struct { name: []const u8, index: usize, law: types.Law };
    timestamp: bool,
    dimension_count: usize,
    measure_count: usize,
    dimensions: []const Dimension,
    measures: []const Measure,
    layout: *const catalog.Row.TypedLayout,

    fn needed(scan: catalog.Scan, name: []const u8) bool {
        for (scan.fields) |field| if (std.mem.eql(u8, field, name)) return true;
        for (scan.conditions) |condition| if (std.mem.eql(u8, condition.column, name)) return true;
        for (scan.order) |order| if (std.mem.eql(u8, order.column, name)) return true;
        return false;
    }
    /// All output names belong to the statement arena, independently of the
    /// parsed descriptor and the temporary partition/result arenas.
    pub fn init(a: A, summary: types.Summary, scan: catalog.Scan) !Projection {
        var dimensions: std.ArrayList(Dimension) = .empty;
        var measures: std.ArrayList(Measure) = .empty;
        for (summary.dimensions, 0..) |name, i| if (needed(scan, name)) {
            try dimensions.append(a, .{ .name = try a.dupe(u8, name), .index = i });
        };
        for (summary.measures, 0..) |measure, i| if (needed(scan, measure.name)) {
            try measures.append(a, .{ .name = try a.dupe(u8, measure.name), .index = i, .law = measure.law });
        };
        var names: std.ArrayList([]const u8) = .empty;
        if (needed(scan, "bucket_start")) try names.append(a, "bucket_start");
        for (dimensions.items) |column| try names.append(a, column.name);
        for (measures.items) |column| try names.append(a, column.name);
        return .{ .layout = try catalog.Row.TypedLayout.init(a, names.items), .timestamp = needed(scan, "bucket_start"), .dimension_count = summary.dimensions.len, .measure_count = summary.measures.len, .dimensions = dimensions.items, .measures = measures.items };
    }
    pub fn rowCost(self: Projection, row: series.Bucket) usize {
        return @sizeOf(catalog.Row) + @sizeOf(types.RowKey) + row.dimensions.len +
            (self.dimensions.len + self.measures.len + @intFromBool(self.timestamp)) * @sizeOf(local.sql_scalar.Datum);
    }
    pub fn project(self: Projection, a: A, row: series.Bucket) !catalog.Row {
        if (row.metrics.len != self.measure_count) return error.RetainedSeriesCorrupt;
        const dimensions = try std.json.parseFromSliceLeaky([]std.json.Value, a, row.dimensions, .{ .allocate = .alloc_always });
        if (dimensions.len != self.dimension_count) return error.RetainedSeriesCorrupt;
        const Datum = local.sql_scalar.Datum;
        const cells = try a.alloc(Datum, self.layout.names.len);
        var index: usize = 0;
        if (self.timestamp) {
            cells[index] = Datum.json(.{ .string = try local.datetime.formatDateTimeSignedNsAlloc(a, row.start_ns) });
            index += 1;
        }
        for (self.dimensions) |column| {
            cells[index] = Datum.fromJson(dimensions[column.index]);
            index += 1;
        }
        for (self.measures) |column| {
            const value = row.metrics[column.index].finalize(column.law) catch |err| switch (err) {
                error.RetainedSeriesNumericOverflow => {
                    cells[index] = Datum.failed(.retained_numeric_overflow);
                    index += 1;
                    continue;
                },
            };
            cells[index] = switch (value) {
                .null => .{},
                .integer => |number| Datum.json(.{ .integer = number }),
                .number => |number| Datum.json(.{ .float = number }),
            };
            index += 1;
        }
        return .{ .id = try a.dupe(u8, &local.storage_retained_series_archive.key(row)), .version = 0, .value = .null, .typed_cells = .{ .layout = self.layout, .values = cells } };
    }
};

test "SQL retained source projection isolates overflow and preserves predicate and ordering dependencies" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const maximum = try series.ExactSum.input(std.math.floatMax(f64));
    const bytes = try (try maximum.add(maximum)).encode(a);
    const numeric: series.Metric = .{ .count = 2, .exact_sum = bytes };
    var metrics = [_]series.Metric{ numeric, numeric, .{ .count = 2 } };
    const row: series.Bucket = .{ .start_ns = 0, .dimensions = "[]", .rows = 2, .metrics = &metrics };
    const summary: types.Summary = .{ .source = @splat(1), .timestamp = "ts", .interval_ns = 1, .mutable_for_ns = 1, .measures = &.{ .{ .name = "sum_v", .field = "v", .law = .sum }, .{ .name = "avg_v", .field = "v", .law = .avg }, .{ .name = "n", .law = .count } } };
    var scan: catalog.Scan = .{ .fields = &.{ "avg_v", "n" }, .limit = 1 };
    const selected = try Projection.init(a, summary, scan);
    const value = try selected.project(a, row);
    try std.testing.expect(!try value.hasField("sum_v"));
    try std.testing.expect(!try value.hasField("bucket_start"));
    try std.testing.expectEqual(std.math.floatMax(f64), (try value.cell("avg_v")).value.float);
    try std.testing.expectEqual(@as(i64, 2), (try value.cell("n")).value.integer);
    scan.fields = &.{};
    try std.testing.expectEqual(@as(usize, 0), (try (try Projection.init(a, summary, scan)).project(a, row)).typed_cells.?.count());
    scan.fields = &.{"sum_v"};
    try std.testing.expectError(error.RetainedSeriesNumericOverflow, (try (try (try Projection.init(a, summary, scan)).project(a, row)).cell("sum_v")).requireValue());
    scan.fields = &.{"n"};
    scan.order = &.{.{ .column = "sum_v" }};
    try std.testing.expectError(error.RetainedSeriesNumericOverflow, (try (try (try Projection.init(a, summary, scan)).project(a, row)).cell("sum_v")).requireValue());
    scan.order = &.{};
    scan.conditions = &.{.{ .column = "sum_v", .op = .is_not_null }};
    try std.testing.expectError(error.RetainedSeriesNumericOverflow, (try (try (try Projection.init(a, summary, scan)).project(a, row)).cell("sum_v")).requireValue());
}
