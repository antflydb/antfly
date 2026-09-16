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

//! Ordered keys over AROW v2, adapted from the mega branch's tuple semantics.
//! Bind names once against an immutable epoch; row preparation addresses only
//! the selected ordinals. No JSON reconstruction, float coercion of integers,
//! or per-component temporary allocations occur on the write path.

const std = @import("std");
const schema = @import("../schema.zig");
const indexes = @import("../relational_index.zig");
const rows = @import("algebraic/relational_row_codec.zig");
const expressions = @import("../../schema/relational_expression.zig");

const Allocator = std.mem.Allocator;

/// Index generations bind this format along with their schema/key definition.
/// There is no compatibility decoder for the mega branch's unpublished format.
pub const encoding_version: u32 = 1;

/// Transport-neutral typed bounds and constraint operands. In particular an
/// integer operand never crosses a floating-point/JSON-number conversion.
pub const Value = union(enum) {
    null,
    string: []const u8,
    blob: []const u8,
    boolean: bool,
    datetime: u64,
    integer: i64,
    number: f64,
};

const BoundKey = struct {
    ordinal: u32,
    expression: ?*CompiledExpression = null,
    column_type: schema.RelationalColumnType,
    descending: bool,
    nulls_first: bool,
    fold_ascii: bool,
};

const CompiledExpression = struct {
    plan: expressions.Plan,
    json: []u8,
    fingerprint: [32]u8,

    fn init(alloc: Allocator, table: schema.TableSchema, json: []const u8, result_type: schema.RelationalColumnType) !*CompiledExpression {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
        defer parsed.deinit();
        var plan = try expressions.Plan.init(alloc, table, parsed.value, result_type);
        errdefer plan.deinit();
        const canonical = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
        errdefer alloc.free(canonical);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly:index-expression:v1\x00");
        hash.update(&plan.fingerprint);
        for (plan.dependencies) |ordinal| {
            const column = table.relational_columns[ordinal];
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, column.path.len, .little);
            hash.update(&length);
            hash.update(column.path);
        }
        const result = try alloc.create(CompiledExpression);
        result.* = .{ .plan = plan, .json = canonical, .fingerprint = undefined };
        hash.final(&result.fingerprint);
        return result;
    }

    fn deinit(self: *CompiledExpression, alloc: Allocator) void {
        self.plan.deinit();
        alloc.free(self.json);
        alloc.destroy(self);
    }
};

pub const EncodedTuple = struct {
    bytes: []u8,
    /// Null and absent values sort identically, but uniqueness enforcement
    /// must distinguish a nullable tuple from a tuple subject to exclusion.
    has_null: bool,

    pub fn deinit(self: *EncodedTuple, alloc: Allocator) void {
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

pub const TuplePlan = struct {
    alloc: Allocator,
    layout: *const rows.PhysicalLayout,
    columns: []const schema.RelationalColumn,
    keys: []const BoundKey,
    /// Physical definition identity, independent of layout ordinal placement
    /// and schema version. Generation publication must match this identity.
    fingerprint: [std.crypto.hash.Blake3.digest_length]u8,

    /// The caller must retain the schema epoch through the plan's lifetime.
    /// Only addressing metadata is borrowed; key names/collations are resolved
    /// here and need not outlive this call. A row from a different epoch is
    /// rejected even when its numeric schema version happens to match.
    pub fn init(
        alloc: Allocator,
        table_schema: schema.TableSchema,
        layout: *const rows.PhysicalLayout,
        definitions: []const indexes.RelationalIndexKey,
    ) !TuplePlan {
        if (table_schema.storage_mode != .relational or definitions.len == 0 or definitions.len > 32)
            return error.InvalidRelationalIndexDefinition;
        if (layout.schema_version != table_schema.version or layout.column_count != table_schema.relational_columns.len)
            return error.RelationalRowSchemaMismatch;
        const keys = try alloc.alloc(BoundKey, definitions.len);
        var initialized: usize = 0;
        errdefer {
            for (keys[0..initialized]) |key| if (key.expression) |expression| expression.deinit(alloc);
            alloc.free(keys);
        }
        var expression_nodes: usize = 0;
        var expression_literal_bytes: usize = 0;
        for (definitions, keys) |definition, *key| {
            if ((definition.expression_json != null) != (definition.result_type != null) or
                (definition.expression_json != null) == (definition.column.len != 0)) return error.InvalidRelationalIndexDefinition;
            const expression = if (definition.expression_json) |json|
                try CompiledExpression.init(alloc, table_schema, json, definition.result_type.?)
            else
                null;
            errdefer if (expression) |compiled| compiled.deinit(alloc);
            if (expression) |compiled| {
                expression_nodes += compiled.plan.nodes.len;
                expression_literal_bytes += compiled.plan.literal_bytes;
                if (expression_nodes > 4096 or expression_literal_bytes > expressions.max_allocated_bytes) return error.RelationalExpressionBudgetExceeded;
            }
            const ordinal = if (expression == null) layout.ordinalForName(table_schema.relational_columns, definition.column) orelse
                return error.RelationalIndexColumnNotFound else 0;
            const column_type = if (expression) |compiled| compiled.plan.result_kind else table_schema.relational_columns[ordinal].column_type;
            switch (column_type) {
                .string, .blob, .boolean, .datetime, .integer, .number => {},
                .json, .geopoint, .geoshape, .dense_vector => return error.UnsupportedRelationalIndexColumn,
            }
            var fold_ascii = false;
            if (definition.collation) |collation| {
                if (column_type != .string) return error.UnsupportedRelationalIndexCollation;
                if (std.ascii.eqlIgnoreCase(collation, "C") or
                    std.ascii.eqlIgnoreCase(collation, "POSIX") or
                    std.ascii.eqlIgnoreCase(collation, "binary"))
                {} else if (std.ascii.eqlIgnoreCase(collation, "ci") or
                    std.ascii.eqlIgnoreCase(collation, "case_insensitive") or
                    std.ascii.eqlIgnoreCase(collation, "antfly.case_insensitive"))
                {
                    fold_ascii = true;
                } else return error.UnsupportedRelationalIndexCollation;
            }
            key.* = .{
                .ordinal = @intCast(ordinal),
                .expression = expression,
                .column_type = column_type,
                .descending = definition.direction == .desc,
                .nulls_first = switch (definition.nulls) {
                    .first => true,
                    .last => false,
                    .default => definition.direction == .desc,
                },
                .fold_ascii = fold_ascii,
            };
            initialized += 1;
        }
        var hasher = std.crypto.hash.Blake3.init(.{});
        hasher.update("antfly:relational-ordered-tuple-definition\x00");
        var version: [4]u8 = undefined;
        std.mem.writeInt(u32, &version, encoding_version, .little);
        hasher.update(&version);
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, keys.len, .little);
        hasher.update(&size);
        for (keys) |key| {
            const name = if (key.expression != null) "" else table_schema.relational_columns[key.ordinal].name;
            std.mem.writeInt(u64, &size, name.len, .little);
            hasher.update(&size);
            hasher.update(name);
            if (key.expression) |expression| hasher.update(&expression.fingerprint);
            hasher.update(&.{ @intFromEnum(key.column_type), @intFromBool(key.descending), @intFromBool(key.nulls_first), @intFromBool(key.fold_ascii) });
        }
        var fingerprint: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
        hasher.final(&fingerprint);
        return .{ .alloc = alloc, .layout = layout, .columns = table_schema.relational_columns, .keys = keys, .fingerprint = fingerprint };
    }

    pub fn deinit(self: *TuplePlan) void {
        for (self.keys) |key| if (key.expression) |expression| expression.deinit(self.alloc);
        self.alloc.free(self.keys);
        self.* = undefined;
    }

    /// Bind this logical key to another immutable source layout for cold scans
    /// and restoration. Resolve names once per source epoch, never per row.
    /// Absent or differently typed source columns require an explicit migration;
    /// silently treating them as NULL would change index comparison semantics.
    /// Both the destination plan and source epoch must outlive the returned plan.
    pub fn projectSource(self: TuplePlan, alloc: Allocator, source: schema.TableSchema, layout: *const rows.PhysicalLayout) !TuplePlan {
        if (source.storage_mode != .relational or layout.schema_version != source.version or
            layout.column_count != source.relational_columns.len)
            return error.RelationalRowSchemaMismatch;
        const keys = try alloc.alloc(BoundKey, self.keys.len);
        var initialized: usize = 0;
        errdefer {
            for (keys[0..initialized]) |key| if (key.expression) |expression| expression.deinit(alloc);
            alloc.free(keys);
        }
        for (self.keys, keys) |original, *key| {
            key.* = original;
            key.expression = null;
            if (original.expression) |expression| {
                const projected = try CompiledExpression.init(alloc, source, expression.json, original.column_type);
                if (!std.mem.eql(u8, &projected.fingerprint, &expression.fingerprint)) {
                    projected.deinit(alloc);
                    return error.RelationalIndexColumnTypeMismatch;
                }
                key.expression = projected;
                initialized += 1;
                continue;
            }
            const name = self.columns[original.ordinal].name;
            const ordinal = layout.ordinalForName(source.relational_columns, name) orelse
                return error.RelationalIndexColumnNotFound;
            if (source.relational_columns[ordinal].column_type != original.column_type)
                return error.RelationalIndexColumnTypeMismatch;
            key.ordinal = @intCast(ordinal);
            initialized += 1;
        }
        return .{ .alloc = alloc, .layout = layout, .columns = source.relational_columns, .keys = keys, .fingerprint = self.fingerprint };
    }

    pub fn encodeAlloc(self: TuplePlan, alloc: Allocator, row: rows.OrdinalRowView) !EncodedTuple {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(alloc);
        const has_null = try self.append(alloc, &out, row);
        return .{ .bytes = try out.toOwnedSlice(alloc), .has_null = has_null };
    }

    /// Encode a left prefix using precisely the write-side comparison rules.
    /// Bounds are values, not a partially fabricated row or client-supplied
    /// physical bytes. This also supplies the equality representation for
    /// composite constraint claims and FK probes.
    pub fn appendValues(self: TuplePlan, alloc: Allocator, out: *std.ArrayList(u8), values: []const Value) !bool {
        if (values.len > self.keys.len) return error.InvalidRelationalIndexBound;
        const start = out.items.len;
        errdefer out.shrinkRetainingCapacity(start);
        var has_null = false;
        for (values, self.keys[0..values.len]) |value, key| {
            if (value == .null) has_null = true;
            try appendValue(alloc, out, key, value);
        }
        return has_null;
    }

    /// Append into the transaction's region or a reusable scratch buffer.
    /// Failure restores the original length, including on late corrupt cells.
    pub fn append(self: TuplePlan, alloc: Allocator, out: *std.ArrayList(u8), row: rows.OrdinalRowView) !bool {
        if (row.layout != self.layout or row.table_schema.relational_columns.ptr != self.columns.ptr or
            row.table_schema.relational_columns.len != self.columns.len)
            return error.RelationalRowSchemaMismatch;
        const start = out.items.len;
        errdefer out.shrinkRetainingCapacity(start);
        var has_null = false;
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        var expression_budget: usize = expressions.max_allocated_bytes;
        for (self.keys) |key| {
            if (key.expression) |expression| {
                // The tuple's layout/column-pointer fence above certifies the
                // expression compiler's ordinals, including cold projections.
                const value = try expression.plan.evaluateBoundRowWithBudget(scratch.allocator(), row, &expression_budget);
                // Borrowed column/literal results allocate nothing in the
                // evaluator but still expand the physical key. Charge that
                // output before appending, including worst-case escaping.
                const encoded_bound: usize = switch (value) {
                    .string, .blob => |bytes| blk: {
                        if (bytes.len > expressions.max_output_bytes) return error.RelationalExpressionBudgetExceeded;
                        break :blk bytes.len * 2 + 3;
                    },
                    .null => 1,
                    .boolean => 2,
                    else => 9,
                };
                if (encoded_bound > expression_budget) return error.RelationalExpressionBudgetExceeded;
                expression_budget -= encoded_bound;
                has_null = has_null or value == .null;
                try appendValue(alloc, out, key, value);
                continue;
            }
            const cell = try row.findCell(key.ordinal);
            if (cell == null or cell.?.is_null) {
                has_null = true;
                try appendValue(alloc, out, key, .null);
                continue;
            }
            const value = cell.?.value;
            try appendValue(alloc, out, key, switch (key.column_type) {
                .string => .{ .string = value.bytes_val },
                .blob => .{ .blob = value.bytes_val },
                .boolean => .{ .boolean = value.bool_val },
                .datetime => .{ .datetime = value.u64_val },
                .integer => .{ .integer = value.i64_val },
                .number => .{ .number = value.f64_val },
                else => unreachable,
            });
        }
        return has_null;
    }

    /// Validate component framing and return the tuple length before any
    /// document-identity suffix. Payload semantics remain schema-bound; this
    /// is not a replacement for row/generation integrity validation.
    pub fn prefixLen(self: TuplePlan, bytes: []const u8) !usize {
        var pos: usize = 0;
        for (self.keys) |key| {
            if (pos == bytes.len) return error.InvalidRelationalIndexKey;
            const tag = bytes[pos];
            pos += 1;
            if (tag == (if (key.nulls_first) @as(u8, 0) else 0xff)) continue;
            if (tag != 0x80) return error.InvalidRelationalIndexKey;
            if (key.column_type == .string or key.column_type == .blob) {
                const marker: u8 = if (key.descending) 0xff else 0;
                while (true) {
                    if (pos == bytes.len) return error.InvalidRelationalIndexKey;
                    const byte = bytes[pos];
                    pos += 1;
                    if (byte != marker) continue;
                    if (pos == bytes.len) return error.InvalidRelationalIndexKey;
                    const escaped = bytes[pos];
                    pos += 1;
                    if (escaped == marker) break;
                    if (escaped != ~marker) return error.InvalidRelationalIndexKey;
                }
            } else {
                const width: usize = if (key.column_type == .boolean) 1 else 8;
                if (width > bytes.len - pos) return error.InvalidRelationalIndexKey;
                pos += width;
            }
        }
        return pos;
    }
};

fn appendValue(alloc: Allocator, out: *std.ArrayList(u8), key: BoundKey, value: Value) !void {
    if (value == .null) {
        try out.append(alloc, if (key.nulls_first) @as(u8, 0) else 0xff);
        return;
    }
    switch (value) {
        .null => unreachable,
        inline else => |_, tag| if (key.column_type != @field(schema.RelationalColumnType, @tagName(tag))) return error.InvalidRelationalIndexBound,
    }
    try out.append(alloc, 0x80);
    var scalar: [8]u8 = undefined;
    const bytes: []const u8 = switch (value) {
        .string => |v| v,
        .blob => |v| v,
        .boolean => |v| blk: {
            scalar[0] = @intFromBool(v);
            break :blk scalar[0..1];
        },
        .datetime => |v| blk: {
            std.mem.writeInt(u64, &scalar, v, .big);
            break :blk &scalar;
        },
        .integer => |v| blk: {
            const bits: u64 = @bitCast(v);
            std.mem.writeInt(u64, &scalar, bits ^ (@as(u64, 1) << 63), .big);
            break :blk &scalar;
        },
        .number => |v| blk: {
            if (!std.math.isFinite(v)) return error.InvalidColumnValue;
            const bits: u64 = @bitCast(if (v == 0) @as(f64, 0) else v);
            const ordered = if (bits >> 63 != 0) ~bits else bits ^ (@as(u64, 1) << 63);
            std.mem.writeInt(u64, &scalar, ordered, .big);
            break :blk &scalar;
        },
        .null => unreachable,
    };
    if (key.column_type == .string or key.column_type == .blob) {
        try appendVariableScalar(alloc, out, bytes, key.descending, key.fold_ascii);
    } else {
        try out.ensureUnusedCapacity(alloc, bytes.len);
        for (bytes) |byte| out.appendAssumeCapacity(if (key.descending) ~byte else byte);
    }
}

fn appendVariableScalar(alloc: Allocator, out: *std.ArrayList(u8), bytes: []const u8, descending: bool, fold_ascii: bool) !void {
    // Descending reverses the escaped scalar INCLUDING its terminator. Simply
    // complementing raw bytes would leave string prefixes in ascending order.
    const mask: u8 = if (descending) 0xff else 0;
    for (bytes) |raw| {
        const byte = if (fold_ascii) std.ascii.toLower(raw) else raw;
        try out.append(alloc, byte ^ mask);
        if (byte == 0) try out.append(alloc, 0xff ^ mask);
    }
    try out.appendSlice(alloc, &.{ mask, mask });
}

fn testTupleAlloc(alloc: Allocator, plan: TuplePlan, table_schema: schema.TableSchema, cells: []const rows.Cell) !EncodedTuple {
    const encoded_row = try rows.serializeOrdinal(alloc, table_schema.version, table_schema.relational_columns, cells, @splat(0));
    defer alloc.free(encoded_row);
    const view = try rows.ordinalRowView(encoded_row, table_schema, plan.layout);
    return plan.encodeAlloc(alloc, view);
}

test "relational tuple cold projections bind historical ordinals without changing comparison bytes" {
    const alloc = std.testing.allocator;
    const old_columns = [_]schema.RelationalColumn{
        .{ .name = "tenant", .path = "tenant", .column_type = .string },
        .{ .name = "id", .path = "id", .column_type = .integer },
    };
    const new_columns = [_]schema.RelationalColumn{ old_columns[1], old_columns[0] };
    const old = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &old_columns };
    const current = schema.TableSchema{ .version = 2, .storage_mode = .relational, .relational_columns = &new_columns };
    var old_layout = try rows.PhysicalLayout.init(alloc, old);
    defer old_layout.deinit();
    var layout = try rows.PhysicalLayout.init(alloc, current);
    defer layout.deinit();
    var plan = try TuplePlan.init(alloc, current, &layout, &.{ .{ .column = "tenant", .collation = "ci" }, .{ .column = "id", .direction = .desc } });
    defer plan.deinit();
    var projected = try plan.projectSource(alloc, old, &old_layout);
    defer projected.deinit();
    const old_cells = [_]rows.Cell{
        .{ .ordinal = 0, .path = "tenant", .value_type = .bytes_val, .value = .{ .bytes_val = "Acme\x00" } },
        .{ .ordinal = 1, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = 9007199254740993 } },
    };
    var current_cells = [_]rows.Cell{ old_cells[1], old_cells[0] };
    current_cells[0].ordinal = 0;
    current_cells[1].ordinal = 1;
    var before = try testTupleAlloc(alloc, projected, old, &old_cells);
    defer before.deinit(alloc);
    var after = try testTupleAlloc(alloc, plan, current, &current_cells);
    defer after.deinit(alloc);
    try std.testing.expectEqualSlices(u8, before.bytes, after.bytes);
    try std.testing.expectEqualSlices(u8, &plan.fingerprint, &projected.fingerprint);
    var incompatible_columns = old_columns;
    incompatible_columns[1].column_type = .number;
    var incompatible = old;
    incompatible.relational_columns = &incompatible_columns;
    var incompatible_layout = try rows.PhysicalLayout.init(alloc, incompatible);
    defer incompatible_layout.deinit();
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, plan.projectSource(alloc, incompatible, &incompatible_layout));
}

test "relational tuple ordinals preserve exact integer ordering and descending prefixes" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "label", .path = "label", .column_type = .string },
    };
    const table_schema = schema.TableSchema{ .version = 7, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try rows.PhysicalLayout.init(alloc, table_schema);
    defer layout.deinit();
    var plan = try TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "id" }});
    defer plan.deinit();
    const values = [_]i64{ std.math.minInt(i64), -9007199254740993, -1, 0, 1, 9007199254740992, 9007199254740993, std.math.maxInt(i64) };
    var previous: ?EncodedTuple = null;
    defer if (previous) |*tuple| tuple.deinit(alloc);
    for (values) |value| {
        var tuple = try testTupleAlloc(alloc, plan, table_schema, &.{.{ .ordinal = 0, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = value } }});
        errdefer tuple.deinit(alloc);
        try std.testing.expect(!tuple.has_null);
        try std.testing.expectEqual(@as(usize, 9), tuple.bytes.len);
        try std.testing.expectEqual(tuple.bytes.len, try plan.prefixLen(tuple.bytes));
        if (previous) |*old| {
            try std.testing.expect(std.mem.lessThan(u8, old.bytes, tuple.bytes));
            old.deinit(alloc);
        }
        previous = tuple;
    }

    var desc = try TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "label", .direction = .desc }});
    defer desc.deinit();
    var prefix = try testTupleAlloc(alloc, desc, table_schema, &.{.{ .ordinal = 1, .path = "label", .value_type = .bytes_val, .value = .{ .bytes_val = "a" } }});
    defer prefix.deinit(alloc);
    var longer = try testTupleAlloc(alloc, desc, table_schema, &.{.{ .ordinal = 1, .path = "label", .value_type = .bytes_val, .value = .{ .bytes_val = "a\x00b" } }});
    defer longer.deinit(alloc);
    try std.testing.expect(std.mem.lessThan(u8, longer.bytes, prefix.bytes));
}

test "relational tuple nulls and collation retain uniqueness semantics" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{.{ .name = "name", .path = "name", .column_type = .string, .allows_null = true }};
    const table_schema = schema.TableSchema{ .version = 7, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try rows.PhysicalLayout.init(alloc, table_schema);
    defer layout.deinit();
    inline for (.{ indexes.RelationalIndexKeyDirection.asc, .desc }) |direction| {
        inline for (.{ indexes.RelationalIndexKeyNulls.default, .first, .last }) |nulls| {
            var plan = try TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "name", .collation = "ci", .direction = direction, .nulls = nulls }});
            defer plan.deinit();
            var missing = try testTupleAlloc(alloc, plan, table_schema, &.{});
            defer missing.deinit(alloc);
            var explicit_null = try testTupleAlloc(alloc, plan, table_schema, &.{.{ .ordinal = 0, .path = "name", .is_null = true, .value_type = .bytes_val, .value = .{ .bytes_val = "" } }});
            defer explicit_null.deinit(alloc);
            var upper = try testTupleAlloc(alloc, plan, table_schema, &.{.{ .ordinal = 0, .path = "name", .value_type = .bytes_val, .value = .{ .bytes_val = "Alpha" } }});
            defer upper.deinit(alloc);
            var lower = try testTupleAlloc(alloc, plan, table_schema, &.{.{ .ordinal = 0, .path = "name", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } }});
            defer lower.deinit(alloc);
            try std.testing.expect(missing.has_null and explicit_null.has_null and !upper.has_null);
            try std.testing.expectEqualSlices(u8, missing.bytes, explicit_null.bytes);
            try std.testing.expectEqualSlices(u8, upper.bytes, lower.bytes);
            const first = nulls == .first or (nulls == .default and direction == .desc);
            try std.testing.expectEqual(first, std.mem.lessThan(u8, missing.bytes, upper.bytes));
        }
    }
}

test "relational tuple composite keys preserve component order and leading prefixes" {
    const alloc = std.testing.allocator;
    // Physical column order deliberately differs from index component order.
    const columns = [_]schema.RelationalColumn{
        .{ .name = "created_at", .path = "created_at", .column_type = .datetime, .allows_null = true },
        .{ .name = "label", .path = "label", .column_type = .string },
        .{ .name = "tenant_id", .path = "tenant_id", .column_type = .integer },
    };
    const table_schema = schema.TableSchema{ .version = 7, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try rows.PhysicalLayout.init(alloc, table_schema);
    defer layout.deinit();
    var plan = try TuplePlan.init(alloc, table_schema, &layout, &.{
        .{ .column = "tenant_id" },
        .{ .column = "created_at", .direction = .desc, .nulls = .last },
        .{ .column = "label", .collation = "ci" },
    });
    defer plan.deinit();
    var leading = try TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "tenant_id" }});
    defer leading.deinit();
    const Fixture = struct { tenant: i64, timestamp: ?u64, label: []const u8 };
    const fixtures = [_]Fixture{
        .{ .tenant = 1, .timestamp = 30, .label = "A" },
        .{ .tenant = 1, .timestamp = 30, .label = "a" }, // Same composite equality key.
        .{ .tenant = 1, .timestamp = 30, .label = "a\x00b" },
        .{ .tenant = 1, .timestamp = 20, .label = "z" },
        .{ .tenant = 1, .timestamp = null, .label = "b" },
        .{ .tenant = 2, .timestamp = 50, .label = "a" },
    };
    var previous: ?EncodedTuple = null;
    defer if (previous) |*tuple| tuple.deinit(alloc);
    for (fixtures, 0..) |fixture, i| {
        const cells = [_]rows.Cell{
            .{ .ordinal = 0, .path = "created_at", .is_null = fixture.timestamp == null, .value_type = .u64_val, .value = .{ .u64_val = fixture.timestamp orelse 0 } },
            .{ .ordinal = 1, .path = "label", .value_type = .bytes_val, .value = .{ .bytes_val = fixture.label } },
            .{ .ordinal = 2, .path = "tenant_id", .value_type = .i64_val, .value = .{ .i64_val = fixture.tenant } },
        };
        var tuple = try testTupleAlloc(alloc, plan, table_schema, &cells);
        errdefer tuple.deinit(alloc);
        try std.testing.expectEqual(fixture.timestamp == null, tuple.has_null);
        var prefix = try testTupleAlloc(alloc, leading, table_schema, &cells);
        defer prefix.deinit(alloc);
        try std.testing.expect(std.mem.startsWith(u8, tuple.bytes, prefix.bytes));
        const suffixed = try std.mem.concat(alloc, u8, &.{ tuple.bytes, "document-identity" });
        defer alloc.free(suffixed);
        try std.testing.expectEqual(tuple.bytes.len, try plan.prefixLen(suffixed));
        for (0..tuple.bytes.len) |len| {
            try std.testing.expectError(error.InvalidRelationalIndexKey, plan.prefixLen(tuple.bytes[0..len]));
        }
        if (previous) |*old| {
            if (i == 1) {
                try std.testing.expectEqualSlices(u8, old.bytes, tuple.bytes);
            } else {
                try std.testing.expect(std.mem.lessThan(u8, old.bytes, tuple.bytes));
            }
            old.deinit(alloc);
        }
        previous = tuple;
    }
}

test "relational tuple composite variable keys do not alias across component boundaries" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "first", .path = "first", .column_type = .string },
        .{ .name = "second", .path = "second", .column_type = .string },
    };
    const table_schema = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try rows.PhysicalLayout.init(alloc, table_schema);
    defer layout.deinit();
    inline for (.{ indexes.RelationalIndexKeyDirection.asc, .desc }) |direction| {
        var plan = try TuplePlan.init(alloc, table_schema, &layout, &.{
            .{ .column = "first", .direction = direction },
            .{ .column = "second" },
        });
        defer plan.deinit();
        const pairs = [_][2][]const u8{ .{ "a", "bc" }, .{ "ab", "c" }, .{ "a\x00", "b" }, .{ "a", "\x00b" } };
        var tuples: [pairs.len]EncodedTuple = undefined;
        var initialized: usize = 0;
        defer for (tuples[0..initialized]) |*tuple| tuple.deinit(alloc);
        for (pairs, &tuples) |pair, *tuple| {
            tuple.* = try testTupleAlloc(alloc, plan, table_schema, &.{
                .{ .ordinal = 0, .path = "first", .value_type = .bytes_val, .value = .{ .bytes_val = pair[0] } },
                .{ .ordinal = 1, .path = "second", .value_type = .bytes_val, .value = .{ .bytes_val = pair[1] } },
            });
            initialized += 1;
        }
        for (tuples, 0..) |tuple, i| {
            try std.testing.expectEqual(tuple.bytes.len, try plan.prefixLen(tuple.bytes));
            for (tuples[i + 1 ..]) |other| {
                try std.testing.expect(!std.mem.eql(u8, tuple.bytes, other.bytes));
            }
        }
        try std.testing.expectEqual(direction == .asc, std.mem.lessThan(u8, tuples[0].bytes, tuples[1].bytes));
    }
}

test "relational tuple plans reject unsupported keys and foreign epochs" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .number }};
    const table_schema = schema.TableSchema{ .version = 7, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try rows.PhysicalLayout.init(alloc, table_schema);
    defer layout.deinit();
    try std.testing.expectError(error.RelationalIndexColumnNotFound, TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "missing" }}));
    try std.testing.expectError(error.UnsupportedRelationalIndexCollation, TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "n", .collation = "ci" }}));
    var plan = try TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "n" }});
    defer plan.deinit();
    var positive_zero = try testTupleAlloc(alloc, plan, table_schema, &.{.{ .ordinal = 0, .path = "n", .value_type = .f64_val, .value = .{ .f64_val = 0.0 } }});
    defer positive_zero.deinit(alloc);
    var negative_zero = try testTupleAlloc(alloc, plan, table_schema, &.{.{ .ordinal = 0, .path = "n", .value_type = .f64_val, .value = .{ .f64_val = -0.0 } }});
    defer negative_zero.deinit(alloc);
    try std.testing.expectEqualSlices(u8, positive_zero.bytes, negative_zero.bytes);
    var other_layout = try rows.PhysicalLayout.init(alloc, table_schema);
    defer other_layout.deinit();
    const encoded_row = try rows.serializeOrdinal(alloc, table_schema.version, &columns, &.{}, @splat(0));
    defer alloc.free(encoded_row);
    const foreign = try rows.ordinalRowView(encoded_row, table_schema, &other_layout);
    var output = std.ArrayList(u8).empty;
    defer output.deinit(alloc);
    try output.appendSlice(alloc, "existing-prefix");
    try std.testing.expectError(error.RelationalRowSchemaMismatch, plan.append(alloc, &output, foreign));
    try std.testing.expectEqualStrings("existing-prefix", output.items);
}

test "relational tuple variable components frame arbitrary binary prefixes" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{.{ .name = "b", .path = "b", .column_type = .blob }};
    const table_schema = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try rows.PhysicalLayout.init(alloc, table_schema);
    defer layout.deinit();
    const values = [_][]const u8{ "", "\x00", "\x00\x00", "\x00a", "a", "a\x00", "a\xff", "\xff" };
    inline for (.{ indexes.RelationalIndexKeyDirection.asc, .desc }) |direction| {
        var plan = try TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "b", .direction = direction }});
        defer plan.deinit();
        var previous: ?EncodedTuple = null;
        defer if (previous) |*tuple| tuple.deinit(alloc);
        for (values) |value| {
            // Exercise binary framing independently of the public row codec,
            // which currently requires UTF-8 for JSON-facing byte columns.
            var encoded = std.ArrayList(u8).empty;
            defer encoded.deinit(alloc);
            try encoded.append(alloc, 0x80);
            try appendVariableScalar(alloc, &encoded, value, direction == .desc, false);
            var tuple = EncodedTuple{ .bytes = try encoded.toOwnedSlice(alloc), .has_null = false };
            errdefer tuple.deinit(alloc);
            try std.testing.expectEqual(tuple.bytes.len, try plan.prefixLen(tuple.bytes));
            const suffixed = try std.mem.concat(alloc, u8, &.{ tuple.bytes, "document-identity" });
            defer alloc.free(suffixed);
            try std.testing.expectEqual(tuple.bytes.len, try plan.prefixLen(suffixed));
            for (0..tuple.bytes.len) |len| {
                try std.testing.expectError(error.InvalidRelationalIndexKey, plan.prefixLen(tuple.bytes[0..len]));
            }
            if (previous) |*old| {
                try std.testing.expectEqual(direction == .asc, std.mem.lessThan(u8, old.bytes, tuple.bytes));
                old.deinit(alloc);
            }
            previous = tuple;
        }
    }
}

fn testTupleAllocation(alloc: Allocator) !void {
    const columns = [_]schema.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    const table_schema = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try rows.PhysicalLayout.init(alloc, table_schema);
    defer layout.deinit();
    var plan = try TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "n" }});
    defer plan.deinit();
    var tuple = try testTupleAlloc(alloc, plan, table_schema, &.{.{ .ordinal = 0, .path = "n", .value_type = .i64_val, .value = .{ .i64_val = 9007199254740993 } }});
    defer tuple.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 9), tuple.bytes.len);
}

test "relational tuple preparation cleans up every allocation failure" {
    try testTupleAllocation(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testTupleAllocation, .{});
}

test "relational tuple wide row allocation benchmark and append rollback" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "payload", .path = "payload", .column_type = .string },
    };
    const table_schema = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try rows.PhysicalLayout.init(alloc, table_schema);
    defer layout.deinit();
    var plan = try TuplePlan.init(alloc, table_schema, &layout, &.{.{ .column = "id" }});
    defer plan.deinit();
    const payload = try alloc.alloc(u8, 64 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');
    const encoded_row = try rows.serializeOrdinal(alloc, table_schema.version, &columns, &.{
        .{ .ordinal = 0, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = 9007199254740993 } },
        .{ .ordinal = 1, .path = "payload", .value_type = .bytes_val, .value = .{ .bytes_val = payload } },
    }, @splat(0));
    defer alloc.free(encoded_row);
    const view = try rows.ordinalRowView(encoded_row, table_schema, &layout);
    var output = std.ArrayList(u8).empty;
    defer output.deinit(alloc);
    try output.ensureTotalCapacity(alloc, 32);
    var no_alloc = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0, .resize_fail_index = 0 });
    var materialization_alloc = std.testing.FailingAllocator.init(alloc, .{});
    const iterations: usize = 200;
    for (0..iterations) |_| {
        output.clearRetainingCapacity();
        try std.testing.expect(!(try plan.append(no_alloc.allocator(), &output, view)));
        try std.testing.expectEqual(@as(usize, 9), output.items.len);
        // A control for the full-row materialization/parsing that the old
        // extraction boundary would require. This is NOT an end-to-end write
        // benchmark, and no latency threshold is asserted in CI.
        const json = try view.reconstructValueAlloc(materialization_alloc.allocator());
        defer materialization_alloc.allocator().free(json);
        var parsed = try std.json.parseFromSlice(std.json.Value, materialization_alloc.allocator(), json, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, 9007199254740993), parsed.value.object.get("id").?.integer);
    }
    try std.testing.expectEqual(@as(usize, 0), no_alloc.allocations);
    try std.testing.expectEqual(materialization_alloc.allocated_bytes, materialization_alloc.freed_bytes);
    std.debug.print("\nrelational tuple: rows={d} payload_bytes={d} key_bytes=9 ordinal_allocated_bytes=0 materialize_parse_allocated_bytes={d}\n", .{
        iterations, payload.len, materialization_alloc.allocated_bytes,
    });

    var wide_plan = try TuplePlan.init(alloc, table_schema, &layout, &.{ .{ .column = "id" }, .{ .column = "payload" } });
    defer wide_plan.deinit();
    output.clearRetainingCapacity();
    try output.appendSlice(alloc, "existing-prefix");
    try std.testing.expectError(error.OutOfMemory, wide_plan.append(no_alloc.allocator(), &output, view));
    try std.testing.expectEqualStrings("existing-prefix", output.items);
}

test "relational tuple definition identity tracks semantics instead of epoch placement" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "label", .path = "label", .column_type = .string },
    };
    const reordered = [_]schema.RelationalColumn{ columns[1], columns[0] };
    const a = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    const b = schema.TableSchema{ .version = 2, .storage_mode = .relational, .relational_columns = &reordered };
    var layout_a = try rows.PhysicalLayout.init(alloc, a);
    defer layout_a.deinit();
    var layout_b = try rows.PhysicalLayout.init(alloc, b);
    defer layout_b.deinit();
    var left = try TuplePlan.init(alloc, a, &layout_a, &.{.{ .column = "label", .collation = "ci" }});
    defer left.deinit();
    var right = try TuplePlan.init(alloc, b, &layout_b, &.{.{ .column = "label", .collation = "antfly.case_insensitive", .nulls = .last }});
    defer right.deinit();
    try std.testing.expectEqualSlices(u8, &left.fingerprint, &right.fingerprint);
    var different = try TuplePlan.init(alloc, a, &layout_a, &.{.{ .column = "label", .collation = "C" }});
    defer different.deinit();
    try std.testing.expect(!std.mem.eql(u8, &left.fingerprint, &different.fingerprint));
    var descending = try TuplePlan.init(alloc, a, &layout_a, &.{.{ .column = "label", .collation = "ci", .direction = .desc }});
    defer descending.deinit();
    try std.testing.expect(!std.mem.eql(u8, &left.fingerprint, &descending.fingerprint));
    var nullable_first = try TuplePlan.init(alloc, a, &layout_a, &.{.{ .column = "label", .collation = "ci", .nulls = .first }});
    defer nullable_first.deinit();
    try std.testing.expect(!std.mem.eql(u8, &left.fingerprint, &nullable_first.fingerprint));
}
