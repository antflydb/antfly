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

//! Projected NOT NULL validation over immutable typed-row layouts. This is
//! read execution, not a publication receipt: the owner must independently
//! bind the exact schema delta, durable fence/drain, range and source cut.
//! No row encoding, JSON reconstruction, index mutation or durable progress
//! happens here. A successful snapshot alone cannot authorize publication.
const std = @import("std");
const schema = @import("../schema.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const Allocator = std.mem.Allocator;
pub const max_columns = 4096;

/// Borrowed execution checkpoint. Owners adapt their request cancellation,
/// deadline and work budget here without pulling API/runtime ABI into decoding.
pub const Control = struct {
    context: ?*anyopaque = null,
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,

    fn ensureActive(self: Control) !void {
        if (self.checkpoint) |call| try call(self.context);
    }
};

/// Owns only the requested names. The active layout is used to reject unknown
/// columns at compilation; every historical source independently binds names
/// to its own ordinals. Numeric version equality is not layout identity.
pub const Plan = struct {
    alloc: Allocator,
    names: []const []const u8,

    pub fn init(alloc: Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout, fields: []const []const u8, context: Control) !Plan {
        try context.ensureActive();
        try requireLayout(table, layout);
        if (fields.len == 0 or fields.len > max_columns) return error.InvalidNullabilityProjection;
        const names = try alloc.alloc([]const u8, fields.len);
        var initialized: usize = 0;
        errdefer {
            for (names[0..initialized]) |name| alloc.free(name);
            alloc.free(names);
        }
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        defer seen.deinit(alloc);
        try seen.ensureTotalCapacity(alloc, @intCast(fields.len));
        var name_bytes: usize = 0;
        for (fields, names) |field, *name| {
            try context.ensureActive();
            name_bytes = std.math.add(usize, name_bytes, field.len) catch return error.InvalidNullabilityProjection;
            if (field.len == 0 or name_bytes > 1024 * 1024 or std.mem.indexOfScalar(u8, field, 0) != null or seen.contains(field))
                return error.InvalidNullabilityProjection;
            if (layout.ordinalForName(table.relational_columns, field) == null) return error.RelationalIndexColumnNotFound;
            name.* = try alloc.dupe(u8, field);
            initialized += 1;
            seen.putAssumeCapacityNoClobber(name.*, {});
        }
        return .{ .alloc = alloc, .names = names };
    }

    pub fn deinit(self: *Plan) void {
        for (self.names) |name| self.alloc.free(name);
        self.alloc.free(self.names);
        self.* = undefined;
    }

    /// Caller pins this source schema/layout until Source.deinit. Missing
    /// historical columns are SQL NULL, never a rebound current ordinal.
    pub fn projectSource(self: *const Plan, alloc: Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout, context: Control) !Source {
        try context.ensureActive();
        try requireLayout(table, layout);
        const ordinals = try alloc.alloc(?u32, self.names.len);
        errdefer alloc.free(ordinals);
        for (self.names, ordinals) |name, *ordinal| {
            try context.ensureActive();
            ordinal.* = if (layout.ordinalForName(table.relational_columns, name)) |index| @intCast(index) else null;
        }
        return .{ .alloc = alloc, .layout = layout, .columns = table.relational_columns, .ordinals = ordinals };
    }
};

fn requireLayout(table: schema.TableSchema, layout: *const codec.PhysicalLayout) !void {
    if (table.storage_mode != .relational or table.version != layout.schema_version or table.relational_columns.len != layout.column_count)
        return error.RelationalRowSchemaMismatch;
}

pub const Source = struct {
    alloc: Allocator,
    layout: *const codec.PhysicalLayout,
    columns: []const schema.RelationalColumn,
    ordinals: []const ?u32,

    pub fn deinit(self: *Source) void {
        self.alloc.free(self.ordinals);
        self.* = undefined;
    }

    /// Index into Plan.names for the first violation; null means this row
    /// satisfies the projection. SQL NULL is distinct from present JSON null,
    /// empty arrays, zero, false and empty strings. No allocator is used, even
    /// for multi-megabyte nonnull payloads or sparse/wide historical layouts.
    pub fn firstNull(self: *const Source, row: codec.OrdinalRowView, context: Control) !?usize {
        try context.ensureActive();
        if (row.layout != self.layout or row.table_schema.version != self.layout.schema_version or
            row.table_schema.relational_columns.ptr != self.columns.ptr or row.table_schema.relational_columns.len != self.columns.len)
            return error.RelationalRowSchemaMismatch;
        for (self.ordinals, 0..) |ordinal, i| {
            if (i % 256 == 0) try context.ensureActive();
            if (ordinal == null or try row.isSqlNull(ordinal.?)) return i;
        }
        return null;
    }
};

const test_columns = [_]schema.RelationalColumn{
    .{ .name = "j", .path = "j", .column_type = .json, .is_json = true, .allows_null = true },
    .{ .name = "n", .path = "n", .column_type = .integer, .allows_null = true },
    .{ .name = "missing", .path = "missing", .column_type = .string, .allows_null = true },
};

fn allocationCases(alloc: Allocator) !void {
    const owner = std.testing.allocator;
    const table = schema.TableSchema{ .version = 7, .storage_mode = .relational, .relational_columns = &test_columns };
    var layout = try codec.PhysicalLayout.init(owner, table);
    defer layout.deinit();
    var plan = try Plan.init(alloc, table, &layout, &.{ "j", "n", "missing" }, .{});
    defer plan.deinit();
    var source = try plan.projectSource(alloc, table, &layout, .{});
    defer source.deinit();
    const encoded = try codec.serializeOrdinal(owner, 7, &test_columns, &.{
        .{ .ordinal = 0, .path = "j", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = "null" } },
        .{ .ordinal = 1, .path = "n", .value_type = .i64_val, .value = .{ .i64_val = 0 } },
    }, @splat(0));
    defer owner.free(encoded);
    const row = try codec.ordinalRowViewSelective(encoded, table, &layout);
    try std.testing.expectEqual(@as(?usize, 2), try source.firstNull(row, .{}));
}

test "projected column nullability preserves JSON null and cleans every allocation failure" {
    try allocationCases(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, allocationCases, .{});
}

test "projected column nullability binds historical layouts rather than numeric epochs" {
    const a = std.testing.allocator;
    const table = schema.TableSchema{ .version = 7, .storage_mode = .relational, .relational_columns = &test_columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var plan = try Plan.init(a, table, &layout, &.{ "j", "n" }, .{});
    defer plan.deinit();
    // Same numeric epoch, different ordinal assignment and an absent column.
    const old_columns = [_]schema.RelationalColumn{test_columns[1]};
    const old_table = schema.TableSchema{ .version = 7, .storage_mode = .relational, .relational_columns = &old_columns };
    var old_layout = try codec.PhysicalLayout.init(a, old_table);
    defer old_layout.deinit();
    var old = try plan.projectSource(a, old_table, &old_layout, .{});
    defer old.deinit();
    const raw = try codec.serializeOrdinal(a, 7, &old_columns, &.{.{ .ordinal = 0, .path = "n", .value_type = .i64_val, .value = .{ .i64_val = 42 } }}, @splat(0));
    defer a.free(raw);
    const old_row = try codec.ordinalRowViewSelective(raw, old_table, &old_layout);
    try std.testing.expectEqual(@as(?usize, 0), try old.firstNull(old_row, .{}));
    var current = try plan.projectSource(a, table, &layout, .{});
    defer current.deinit();
    try std.testing.expectError(error.RelationalRowSchemaMismatch, current.firstNull(old_row, .{}));
    var only_n = try Plan.init(a, table, &layout, &.{"n"}, .{});
    defer only_n.deinit();
    var old_n = try only_n.projectSource(a, old_table, &old_layout, .{});
    defer old_n.deinit();
    try std.testing.expectEqual(@as(?usize, null), try old_n.firstNull(old_row, .{}));
    try std.testing.expectError(error.RelationalIndexColumnNotFound, Plan.init(a, table, &layout, &.{"unknown"}, .{}));
    try std.testing.expectError(error.InvalidNullabilityProjection, Plan.init(a, table, &layout, &.{ "n", "n" }, .{}));
    const Canceled = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    const context: Control = .{ .checkpoint = Canceled.check };
    try std.testing.expectError(error.Canceled, current.firstNull(old_row, context));
    try std.testing.expectError(error.Canceled, plan.projectSource(a, old_table, &old_layout, context));
    try std.testing.expectError(error.Canceled, Plan.init(a, table, &layout, &.{"n"}, context));
}
