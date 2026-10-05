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

//! Row-fragment pruning metadata. This is the first Antfly-owned stats artifact:
//! compact enough to keep beside row fragments and explicit enough for future
//! fragment pruning, hot projection planning, and adaptive promotion decisions.

const std = @import("std");
const Allocator = std.mem.Allocator;
const row_fragment = @import("types.zig");

pub const ScalarValue = union(row_fragment.ColumnKind) {
    bytes: []u8,
    json: []u8,
    i64: i64,
    f64: f64,
    bool: bool,
    vector_f32: []f32,

    pub fn deinit(self: *ScalarValue, alloc: Allocator) void {
        switch (self.*) {
            .bytes => |value| alloc.free(value),
            .json => |value| alloc.free(value),
            .vector_f32 => |value| alloc.free(value),
            else => {},
        }
        self.* = undefined;
    }
};

pub const ColumnStats = struct {
    name: []u8,
    kind: row_fragment.ColumnKind,
    null_count: u64 = 0,
    present_count: u64 = 0,
    min: ?ScalarValue = null,
    max: ?ScalarValue = null,
    dictionary_samples: [][]u8 = &.{},

    pub fn deinit(self: *ColumnStats, alloc: Allocator) void {
        alloc.free(self.name);
        if (self.min) |*value| value.deinit(alloc);
        if (self.max) |*value| value.deinit(alloc);
        for (self.dictionary_samples) |sample| alloc.free(sample);
        alloc.free(self.dictionary_samples);
        self.* = undefined;
    }
};

pub const FragmentStats = struct {
    schema_fingerprint: []u8,
    row_count: u64,
    columns: []ColumnStats,

    pub fn deinit(self: *FragmentStats, alloc: Allocator) void {
        alloc.free(self.schema_fingerprint);
        for (self.columns) |*column| column.deinit(alloc);
        alloc.free(self.columns);
        self.* = undefined;
    }

    pub fn findColumn(self: FragmentStats, name: []const u8) ?ColumnStats {
        for (self.columns) |column| {
            if (std.mem.eql(u8, column.name, name)) return column;
        }
        return null;
    }
};

pub fn buildAlloc(
    alloc: Allocator,
    fragment: row_fragment.Fragment,
    max_dictionary_samples: usize,
) !FragmentStats {
    try fragment.validate();

    const columns = try alloc.alloc(ColumnStats, fragment.columns.len);
    errdefer alloc.free(columns);
    var initialized: usize = 0;
    errdefer {
        for (columns[0..initialized]) |*column| column.deinit(alloc);
    }

    for (fragment.columns, columns) |column, *out| {
        out.* = try buildColumnStatsAlloc(alloc, column, max_dictionary_samples);
        initialized += 1;
    }

    return .{
        .schema_fingerprint = try alloc.dupe(u8, fragment.schema_fingerprint),
        .row_count = @intCast(fragment.rowCount()),
        .columns = columns,
    };
}

pub fn encodeAlloc(alloc: Allocator, stats: FragmentStats) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"schema\":\"");
    try appendEscapedJson(alloc, &out, stats.schema_fingerprint);
    try out.appendSlice(alloc, "\",\"row_count\":");
    try appendInt(alloc, &out, stats.row_count);
    try out.appendSlice(alloc, ",\"columns\":[");
    for (stats.columns, 0..) |column, idx| {
        if (idx != 0) try out.append(alloc, ',');
        try encodeColumnStats(alloc, &out, column);
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn buildColumnStatsAlloc(
    alloc: Allocator,
    column: row_fragment.Column,
    max_dictionary_samples: usize,
) !ColumnStats {
    var stats = ColumnStats{
        .name = try alloc.dupe(u8, column.name),
        .kind = column.kind,
    };
    errdefer stats.deinit(alloc);

    var samples = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (samples.items) |sample| alloc.free(sample);
        samples.deinit(alloc);
    }

    for (column.values) |value| {
        switch (value) {
            .null => stats.null_count += 1,
            .vector_f32 => {
                stats.present_count += 1;
            },
            else => {
                stats.present_count += 1;
                try observeValue(alloc, &stats, value);
                if (samples.items.len < max_dictionary_samples) {
                    if (sampleBytes(value)) |bytes| {
                        if (!containsSample(samples.items, bytes)) {
                            try samples.append(alloc, try alloc.dupe(u8, bytes));
                        }
                    }
                }
            },
        }
    }

    stats.dictionary_samples = try samples.toOwnedSlice(alloc);
    return stats;
}

fn observeValue(alloc: Allocator, stats: *ColumnStats, value: row_fragment.CellValue) !void {
    if (stats.min == null) {
        var min_value = try scalarFromCellAlloc(alloc, value);
        errdefer min_value.deinit(alloc);
        var max_value = try scalarFromCellAlloc(alloc, value);
        errdefer max_value.deinit(alloc);
        stats.min = min_value;
        stats.max = max_value;
        return;
    }

    var scalar = try scalarFromCellAlloc(alloc, value);
    var scalar_installed = false;
    defer if (!scalar_installed) scalar.deinit(alloc);
    if (compareScalar(scalar, stats.min.?) < 0) {
        stats.min.?.deinit(alloc);
        stats.min = scalar;
        scalar_installed = true;
    }

    var max_candidate = try scalarFromCellAlloc(alloc, value);
    var max_candidate_installed = false;
    defer if (!max_candidate_installed) max_candidate.deinit(alloc);
    if (compareScalar(max_candidate, stats.max.?) > 0) {
        stats.max.?.deinit(alloc);
        stats.max = max_candidate;
        max_candidate_installed = true;
    }
}

fn scalarFromCellAlloc(alloc: Allocator, value: row_fragment.CellValue) !ScalarValue {
    return switch (value) {
        .null => error.InvalidRowFragmentStats,
        .bytes => |bytes| .{ .bytes = try alloc.dupe(u8, bytes) },
        .json => |bytes| .{ .json = try alloc.dupe(u8, bytes) },
        .i64 => |int| .{ .i64 = int },
        .f64 => |float| .{ .f64 = float },
        .bool => |boolean| .{ .bool = boolean },
        .vector_f32 => error.InvalidRowFragmentStats,
    };
}

fn compareScalar(a: ScalarValue, b: ScalarValue) i8 {
    return switch (a) {
        .bytes => |value| compareBytes(value, b.bytes),
        .json => |value| compareBytes(value, b.json),
        .i64 => |value| compareOrder(value, b.i64),
        .f64 => |value| compareOrder(value, b.f64),
        .bool => |value| compareOrder(@intFromBool(value), @intFromBool(b.bool)),
        .vector_f32 => |value| compareOrder(value.len, b.vector_f32.len),
    };
}

fn compareBytes(a: []const u8, b: []const u8) i8 {
    if (std.mem.lessThan(u8, a, b)) return -1;
    if (std.mem.eql(u8, a, b)) return 0;
    return 1;
}

fn compareOrder(a: anytype, b: @TypeOf(a)) i8 {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

fn sampleBytes(value: row_fragment.CellValue) ?[]const u8 {
    return switch (value) {
        .bytes => |bytes| bytes,
        .json => |bytes| bytes,
        else => null,
    };
}

fn containsSample(samples: []const []u8, value: []const u8) bool {
    for (samples) |sample| {
        if (std.mem.eql(u8, sample, value)) return true;
    }
    return false;
}

fn encodeColumnStats(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    column: ColumnStats,
) !void {
    try out.appendSlice(alloc, "{\"name\":\"");
    try appendEscapedJson(alloc, out, column.name);
    try out.appendSlice(alloc, "\",\"kind\":\"");
    try out.appendSlice(alloc, @tagName(column.kind));
    try out.appendSlice(alloc, "\",\"null_count\":");
    try appendInt(alloc, out, column.null_count);
    try out.appendSlice(alloc, ",\"present_count\":");
    try appendInt(alloc, out, column.present_count);
    if (column.min) |min_value| {
        try out.appendSlice(alloc, ",\"min\":");
        try encodeScalar(alloc, out, min_value);
    }
    if (column.max) |max_value| {
        try out.appendSlice(alloc, ",\"max\":");
        try encodeScalar(alloc, out, max_value);
    }
    try out.appendSlice(alloc, ",\"dictionary_samples\":[");
    for (column.dictionary_samples, 0..) |sample, idx| {
        if (idx != 0) try out.append(alloc, ',');
        try out.append(alloc, '"');
        try appendEscapedJson(alloc, out, sample);
        try out.append(alloc, '"');
    }
    try out.appendSlice(alloc, "]}");
}

fn encodeScalar(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: ScalarValue) !void {
    switch (value) {
        .bytes => |bytes| {
            try out.append(alloc, '"');
            try appendEscapedJson(alloc, out, bytes);
            try out.append(alloc, '"');
        },
        .json => |bytes| {
            try out.append(alloc, '"');
            try appendEscapedJson(alloc, out, bytes);
            try out.append(alloc, '"');
        },
        .i64 => |int| try appendSignedInt(alloc, out, int),
        .f64 => |float| try appendFormatted(alloc, out, "{d}", .{float}),
        .bool => |boolean| try out.appendSlice(alloc, if (boolean) "true" else "false"),
        .vector_f32 => |vector| {
            try out.append(alloc, '[');
            for (vector, 0..) |item, idx| {
                if (idx != 0) try out.append(alloc, ',');
                try appendFormatted(alloc, out, "{d}", .{item});
            }
            try out.append(alloc, ']');
        },
    }
}

fn appendEscapedJson(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    for (value) |byte| {
        switch (byte) {
            '\\' => try out.appendSlice(alloc, "\\\\"),
            '"' => try out.appendSlice(alloc, "\\\""),
            '\n' => try out.appendSlice(alloc, "\\n"),
            '\r' => try out.appendSlice(alloc, "\\r"),
            '\t' => try out.appendSlice(alloc, "\\t"),
            else => try out.append(alloc, byte),
        }
    }
}

fn appendInt(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    try appendFormatted(alloc, out, "{d}", .{value});
}

fn appendSignedInt(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: i64) !void {
    try appendFormatted(alloc, out, "{d}", .{value});
}

fn appendFormatted(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const rendered = try std.fmt.allocPrint(alloc, fmt, args);
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

test "row fragment stats capture nulls min max and dictionary samples" {
    const alloc = std.testing.allocator;
    var fragment = row_fragment.Fragment{
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .row_refs = try alloc.alloc(row_fragment.RowRef, 3),
        .columns = try alloc.alloc(row_fragment.Column, 3),
    };
    defer fragment.deinit(alloc);
    fragment.row_refs[0] = .{ .key = try alloc.dupe(u8, "row:a"), .ordinal = 0 };
    fragment.row_refs[1] = .{ .key = try alloc.dupe(u8, "row:b"), .ordinal = 1 };
    fragment.row_refs[2] = .{ .key = try alloc.dupe(u8, "row:c"), .ordinal = 2 };
    fragment.columns[0] = .{
        .name = try alloc.dupe(u8, "tenant"),
        .kind = .bytes,
        .values = try alloc.alloc(row_fragment.CellValue, 3),
    };
    fragment.columns[0].values[0] = .{ .bytes = try alloc.dupe(u8, "t2") };
    fragment.columns[0].values[1] = .{ .bytes = try alloc.dupe(u8, "t1") };
    fragment.columns[0].values[2] = .{ .bytes = try alloc.dupe(u8, "t2") };
    fragment.columns[1] = .{
        .name = try alloc.dupe(u8, "amount"),
        .kind = .i64,
        .values = try alloc.alloc(row_fragment.CellValue, 3),
    };
    fragment.columns[1].values[0] = .{ .i64 = 30 };
    fragment.columns[1].values[1] = .null;
    fragment.columns[1].values[2] = .{ .i64 = 10 };
    fragment.columns[2] = .{
        .name = try alloc.dupe(u8, "embedding"),
        .kind = .vector_f32,
        .values = try alloc.alloc(row_fragment.CellValue, 3),
    };
    fragment.columns[2].values[0] = .{ .vector_f32 = try alloc.dupe(f32, &.{ 1.0, 0.0 }) };
    fragment.columns[2].values[1] = .null;
    fragment.columns[2].values[2] = .{ .vector_f32 = try alloc.dupe(f32, &.{ 0.5, 0.5 }) };

    var stats = try buildAlloc(alloc, fragment, 2);
    defer stats.deinit(alloc);

    try std.testing.expectEqual(@as(u64, 3), stats.row_count);
    const tenant = stats.findColumn("tenant").?;
    try std.testing.expectEqual(@as(u64, 0), tenant.null_count);
    try std.testing.expectEqualStrings("t1", tenant.min.?.bytes);
    try std.testing.expectEqualStrings("t2", tenant.max.?.bytes);
    try std.testing.expectEqual(@as(usize, 2), tenant.dictionary_samples.len);
    const amount = stats.findColumn("amount").?;
    try std.testing.expectEqual(@as(u64, 1), amount.null_count);
    try std.testing.expectEqual(@as(i64, 10), amount.min.?.i64);
    try std.testing.expectEqual(@as(i64, 30), amount.max.?.i64);
    const embedding = stats.findColumn("embedding").?;
    try std.testing.expectEqual(@as(u64, 1), embedding.null_count);
    try std.testing.expectEqual(@as(u64, 2), embedding.present_count);
    try std.testing.expect(embedding.min == null);
    try std.testing.expect(embedding.max == null);

    const encoded = try encodeAlloc(alloc, stats);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"row_count\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dictionary_samples\":[\"t2\",\"t1\"]") != null);
}
