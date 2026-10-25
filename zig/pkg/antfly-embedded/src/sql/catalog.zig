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

//! Request-owned SQL binding contract. Implementations resolve only referenced
//! names, authorize their current logical resources, and pin physical identity
//! and schema together. This is not another durable catalog or storage engine.
const std = @import("std");
const ast = @import("ast.zig");

pub const cloneDocumentPath = @import("document_path.zig").clone;

pub const Column = struct {
    name: []const u8,
    path: []const u8,
    /// Compiled literal object keys; path remains the logical SQL cell name.
    document_path: @import("document_path.zig").Path = &.{},
    type: ast.ColumnType,
    temporal_kind: ?@import("temporal_value.zig").Kind = null,
    element_type: ?@import("array_value.zig").ElementType = null,
    numeric_modifier: ?@import("../common/sql_builtin_type.zig").NumericModifier = null,
    nullable: bool = true,
    /// Optional document properties that reject JSON null represent SQL NULL
    /// by absence. Raw _doc values never pass through this assignment boundary.
    document_null_is_absent: bool = false,

    /// Native stored generated columns are readable but never SQL assignable.
    generated: bool = false,
    /// Native row preparation fills absent DEFAULT cells after SQL evaluation.
    defaulted: bool = false,
    /// An omitted default depends on this SQL transaction's captured clock.
    clock_defaulted: bool = false,
    sequence_default: ?*const @import("../system_catalog/column_sequences.zig").Binding = null,

    pub fn assignStorageValue(self: Column, alloc: std.mem.Allocator, object: *std.json.ObjectMap, value: std.json.Value, sql_null: bool) !void {
        if (self.document_path.len != 0) return @import("document_path.zig").assign(alloc, object, self.document_path, value, sql_null and self.document_null_is_absent);
        if (sql_null and self.document_null_is_absent) {
            _ = object.orderedRemove(self.path);
        } else try object.put(alloc, self.path, value);
    }

    /// Borrow one immutable native layout's complete SQL domain. Shared by
    /// query binding and installed RLS: unrelated array/numeric columns must
    /// neither block activation nor lose their widths/modifiers in policies.
    pub fn fromRelational(column: anytype) !Column {
        return .{
            .name = column.name,
            .path = column.path,
            .nullable = !column.required or column.allows_null,
            .element_type = column.sql_element_type,
            .numeric_modifier = column.numeric_modifier,
            .type = switch (column.column_type) {
                .string => if (column.sql_element_type == .uuid) .uuid else .string,
                .numrange => .numrange,
                .daterange => .daterange,
                .tsrange => .tsrange,
                .tstzrange => .tstzrange,
                .integer => .integer,
                .number, .numeric => .number,
                .boolean => .boolean,
                .datetime => .datetime,
                .json => .json,
                .sql_array => .array,
                else => return error.UnsupportedSqlExecution,
            },
        };
    }

    /// A view dependency binds a logical SQL domain, not the spelling of its
    /// name/path, nullability, generation expression, or DEFAULT clause.
    pub fn identitySignature(self: Column, _: std.mem.Allocator) ![32]u8 {
        if (self.temporal_kind != null and (self.type != .datetime or self.element_type != null or self.numeric_modifier != null)) return error.InvalidSqlBackendResponse;
        const Kind = @import("../common/sql_builtin_type.zig").Type;
        const identity: ?Kind = self.element_type orelse switch (self.type) {
            .integer => Kind.int64,
            .number => Kind.float64,
            .string => Kind.text,
            .uuid => Kind.uuid,
            .boolean => Kind.boolean,
            .json => Kind.jsonb,
            .datetime, .numrange, .daterange, .tsrange, .tstzrange => null,
            .array => return error.InvalidSqlBackendResponse,
        };
        if (self.numeric_modifier) |modifier| {
            if (identity != .numeric or (self.type != .number and self.type != .array)) return error.InvalidSqlBackendResponse;
            try modifier.validate();
        }
        // This descriptor contains only enum tags and bounded numeric modifiers.
        // Hash its historical JSON bytes without allocating per dependency.
        var storage: [512]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&storage);
        std.json.Stringify.value(.{ .kind = self.type, .identity = identity, .numeric_modifier = self.numeric_modifier }, .{}, &writer) catch return error.InvalidSqlBackendResponse;
        const bytes = writer.buffered();
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:sql-column-domain:v1\x00");
        hash.update(bytes);
        // Preserve existing scalar signatures and the implicit timestamptz
        // domain. Other temporal kinds cannot share that identity: replacement
        // and nested-view dependencies must fail closed on a type change.
        if (self.type == .datetime) if (self.temporal_kind) |kind| if (kind != .timestamp_tz) {
            hash.update("\x00temporal-kind\x00");
            hash.update(&.{@backingInt(kind)});
        };
        return hash.finalResult();
    }
};

test "SQL column identity signatures normalize defaults and retain exact builtin array and numeric domains" {
    const a = std.testing.allocator;
    const base: Column = .{ .name = "a", .path = "a", .type = .integer };
    var renamed = base;
    renamed.name = "renamed";
    renamed.path = "physical";
    renamed.nullable = false;
    renamed.defaulted = true;
    renamed.clock_defaulted = true;
    renamed.element_type = .int64;
    const signature = try base.identitySignature(a);
    try std.testing.expectEqual(signature, try renamed.identitySignature(a));
    renamed.element_type = .int32;
    try std.testing.expect(!std.mem.eql(u8, &signature, &try renamed.identitySignature(a)));
    const text_array: Column = .{ .name = "a", .path = "a", .type = .array, .element_type = .text };
    var json_array = text_array;
    json_array.element_type = .jsonb;
    try std.testing.expect(!std.mem.eql(u8, &try text_array.identitySignature(a), &try json_array.identitySignature(a)));
    const numeric: Column = .{ .name = "a", .path = "a", .type = .number, .element_type = .numeric, .numeric_modifier = .{ .precision = 20, .scale = 4 } };
    var wide = numeric;
    wide.numeric_modifier = .{ .precision = 22, .scale = 4 };
    try std.testing.expect(!std.mem.eql(u8, &try numeric.identitySignature(a), &try wide.identitySignature(a)));
}

pub fn hasColumnDefault(schema: anytype, name: []const u8) bool {
    const entries = schema.column_defaults orelse return false;
    if (entries.value != .array) return false;
    for (entries.value.array.items) |entry| {
        if (entry != .object) continue;
        const column = entry.object.get("column") orelse continue;
        if (column == .string and std.mem.eql(u8, column.string, name)) return true;
    }
    return false;
}

/// Derived once with the immutable column layout, never by parsing on writes.
/// Native declaration admission owns validation of operators and tree bounds.
pub fn columnDefaultUsesClock(schema: anytype, name: []const u8) bool {
    const entries = schema.column_defaults orelse return false;
    if (entries.value != .array) return false;
    for (entries.value.array.items) |entry| {
        if (entry != .object) continue;
        const column = entry.object.get("column") orelse continue;
        if (column != .string or !std.mem.eql(u8, column.string, name)) continue;
        return defaultExpressionUsesClock(entry.object.get("expression") orelse return false, 0);
    }
    return false;
}

fn defaultExpressionUsesClock(expression: std.json.Value, depth: usize) bool {
    // Conservatively retain preparation on a shape outside native admission.
    if (depth >= 16) return true;
    if (expression != .object) return false;
    if (expression.object.get("op")) |op| if (op == .string) {
        if (std.mem.eql(u8, op.string, "transaction_timestamp")) return true;
        if (std.mem.eql(u8, op.string, "literal")) return false;
    };
    if (expression.object.get("args")) |args| if (args == .array) {
        for (args.array.items) |arg| if (defaultExpressionUsesClock(arg, depth + 1)) return true;
    };
    return false;
}

pub const Index = struct {
    name: []const u8,
    /// Only direct ordered-tuple keys are exposed to SQL access planning.
    /// Expression and partial indexes require a separate implication proof.
    columns: []const []const u8,
};

/// Immutable declared access shapes, not proof of readiness or membership.
/// Keep these separate from `Index`: existing direct-total probe consumers
/// must not accidentally treat a partial or expression index as total.
pub const IndexDefinition = struct {
    const native = @import("../storage/relational_index.zig");
    pub const KeyIdentity = native.ExpressionKeyIdentity;
    pub const Key = struct {
        column: []const u8,
        expression: ?std.json.Value,
        result_type: ?@import("../storage/schema.zig").RelationalColumnType,
        collation: ?[]const u8,
        direction: native.RelationalIndexKeyDirection,
        nulls: native.RelationalIndexKeyNulls,
        identity: ?KeyIdentity = null,

        pub fn bindIdentity(self: Key, temporary: std.mem.Allocator, table: @import("../storage/schema.zig").TableSchema) !KeyIdentity {
            var column: std.json.ObjectMap = .empty;
            defer column.deinit(temporary);
            const expression = self.expression orelse blk: {
                try column.put(temporary, "op", .{ .string = "column" });
                try column.put(temporary, "column", .{ .string = self.column });
                break :blk std.json.Value{ .object = column };
            };
            const expected = self.result_type orelse for (table.relational_columns) |item| {
                if (std.mem.eql(u8, item.name, self.column)) break item.column_type;
            } else return error.RelationalIndexColumnNotFound;
            var plan = try @import("../schema/relational_expression.zig").Plan.init(temporary, table, expression, expected);
            defer plan.deinit();
            return .{ .fingerprint = plan.fingerprint, .kind = plan.result_kind, .builtin = plan.result_sql_type };
        }

        /// Equality shape only, not readiness, membership or a range proof.
        /// Collation is part of the comparison domain; direction/NULL ordering
        /// remain tuple-encoding properties owned by the selected index.
        pub fn matchesEquality(self: Key, query: KeyIdentity, collation: ?[]const u8) !bool {
            const stored = self.identity orelse return false;
            if (stored.kind != query.kind or stored.builtin != query.builtin or !std.mem.eql(u8, &stored.fingerprint, &query.fingerprint)) return false;
            const expressions = @import("../schema/relational_expression.zig");
            const stored_fold = if (self.collation) |name| try expressions.foldAsciiCollation(stored.kind, name) else false;
            const query_fold = if (collation) |name| try expressions.foldAsciiCollation(query.kind, name) else false;
            return stored_fold == query_fold;
        }
    };
    pub const Predicate = struct {
        constant: ?bool,
        column: []const u8,
        op: native.UniquePredicateOp,
        value: ?std.json.Value,
        collation: ?[]const u8,
    };
    name: []const u8,
    keys: []const Key,
    include_columns: []const []const u8,
    where: []const Predicate,

    fn copyOptional(a: std.mem.Allocator, value: ?[]const u8) !?[]const u8 {
        return if (value) |bytes| try a.dupe(u8, bytes) else null;
    }

    /// Both embedded and server resolvers consume the same validated schema
    /// vocabulary. Temporary wire conversion never escapes into a binding.
    pub fn deriveLeaky(a: std.mem.Allocator, temporary: std.mem.Allocator, parsed: anytype, table: @import("../storage/schema.zig").TableSchema) ![]const IndexDefinition {
        const declared = (try parsed.relationalIndexDefinitions(temporary)) orelse return &.{};
        const output = try a.alloc(IndexDefinition, declared.len);
        for (declared, output) |definition, *out| {
            out.* = try fromNativeLeaky(a, definition);
            for (@constCast(out.keys)) |*key| key.identity = try key.bindIdentity(temporary, table);
        }
        return output;
    }

    /// All allocations, including partial failure, belong to the caller's
    /// preparation region. Decode only on schema derivation, never cache hits.
    pub fn fromNativeLeaky(a: std.mem.Allocator, definition: native.RelationalIndexDefinition) !IndexDefinition {
        // This view describes the schema's ordered tuple declarations only.
        // Other access methods and legacy expression channels need their own
        // binding contracts rather than an incomplete tuple approximation.
        if (definition.access_method != .ordered_tuple or definition.expressions.len != 0 or definition.where_expressions.len != 0 or
            definition.keys.len == 0 or definition.keys.len > 32 or definition.where.len > 256 or definition.include_columns.len > 256)
            return error.InvalidSqlBackendResponse;
        const keys = try a.alloc(Key, definition.keys.len);
        for (definition.keys, keys) |source, *key| {
            if ((source.expression_json != null) != (source.result_type != null) or
                (source.expression_json != null) == (source.column.len != 0)) return error.InvalidSqlBackendResponse;
            key.* = .{
                .column = try a.dupe(u8, source.column),
                .expression = if (source.expression_json) |bytes| try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .parse_numbers = false, .allocate = .alloc_always }) else null,
                .result_type = source.result_type,
                .collation = try copyOptional(a, source.collation),
                .direction = source.direction,
                .nulls = source.nulls,
            };
        }
        const predicates = try a.alloc(Predicate, definition.where.len);
        for (definition.where, predicates) |source, *predicate| {
            if (source.constant != null) {
                if (source.field.len != 0 or source.op != .eq or source.value_json != null or source.collation != null)
                    return error.InvalidSqlBackendResponse;
            } else if (source.field.len == 0) return error.InvalidSqlBackendResponse;
            predicate.* = .{
                .constant = source.constant,
                .column = try a.dupe(u8, source.field),
                .op = source.op,
                .value = if (source.value_json) |bytes| try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .parse_numbers = false, .allocate = .alloc_always }) else null,
                .collation = try copyOptional(a, source.collation),
            };
        }
        return .{ .name = try a.dupe(u8, definition.name), .keys = keys, .include_columns = try copyNames(a, definition.include_columns), .where = predicates };
    }

    fn copyNames(a: std.mem.Allocator, input: []const []const u8) ![]const []const u8 {
        const output = try a.alloc([]const u8, input.len);
        for (input, output) |name, *copy| copy.* = try a.dupe(u8, name);
        return output;
    }

    /// Request ownership survives cache eviction. Clone the compact trees
    /// directly, preserving exact literal tokens without stringify/reparse.
    pub fn cloneLeaky(self: IndexDefinition, a: std.mem.Allocator) !IndexDefinition {
        const keys = try a.alloc(Key, self.keys.len);
        for (self.keys, keys) |source, *key| {
            key.* = source;
            key.column = try a.dupe(u8, source.column);
            key.collation = try copyOptional(a, source.collation);
            key.expression = if (source.expression) |value| try @import("../storage/typed_json.zig").clone(a, value) else null;
        }
        const predicates = try a.alloc(Predicate, self.where.len);
        for (self.where, predicates) |source, *predicate| {
            predicate.* = source;
            predicate.column = try a.dupe(u8, source.column);
            predicate.collation = try copyOptional(a, source.collation);
            predicate.value = if (source.value) |value| try @import("../storage/typed_json.zig").clone(a, value) else null;
        }
        return .{ .name = try a.dupe(u8, self.name), .keys = keys, .include_columns = try copyNames(a, self.include_columns), .where = predicates };
    }
};

test "SQL index descriptors reject incomplete native access shapes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const native = @import("../storage/relational_index.zig");
    const valid: native.RelationalIndexDefinition = .{ .name = "key", .owner_kind = .table, .owner_name = native.relational_table_index_owner_name, .access_method = .ordered_tuple, .keys = &.{.{ .column = "id" }} };
    _ = try IndexDefinition.fromNativeLeaky(a, valid);
    for ([_][]const native.RelationalIndexKey{
        &.{},
        &.{.{}},
        &.{.{ .column = "id", .expression_json = "{}", .result_type = .integer }},
        &.{.{ .expression_json = "{}" }},
        &.{.{ .column = "id", .result_type = .integer }},
    }) |keys| {
        var malformed = valid;
        malformed.keys = keys;
        try std.testing.expectError(error.InvalidSqlBackendResponse, IndexDefinition.fromNativeLeaky(a, malformed));
    }
    for ([_]native.UniquePredicate{
        .{},
        .{ .constant = true, .field = "id" },
        .{ .constant = false, .op = .ne },
        .{ .constant = true, .value_json = "null" },
        .{ .constant = true, .collation = "binary" },
    }) |predicate| {
        var malformed = valid;
        malformed.where = &.{predicate};
        try std.testing.expectError(error.InvalidSqlBackendResponse, IndexDefinition.fromNativeLeaky(a, malformed));
    }
    var other_method = valid;
    other_method.access_method = .scalar_column;
    try std.testing.expectError(error.InvalidSqlBackendResponse, IndexDefinition.fromNativeLeaky(a, other_method));
}

/// Pinned declarative constraints, not similarly named access indexes. Native
/// execution independently rebinds the name to its guarded durable generation.
pub const Constraint = struct {
    name: []const u8,
    kind: enum { unique, check, foreign_key },
    deferrable: bool = false,

    fn namedConstraint(definition: anytype) bool {
        if (comptime @hasField(@TypeOf(definition), "origin")) return (definition.origin orelse .constraint) != .index;
        return true;
    }

    pub fn derive(alloc: std.mem.Allocator, schema: anytype) ![]const Constraint {
        var count: usize = 0;
        inline for (.{ "unique_constraints", "checks", "foreign_keys" }) |field| {
            if (@field(schema, field)) |values| for (values.value) |definition| {
                if (namedConstraint(definition)) count = try std.math.add(usize, count, 1);
            };
        }
        const output = try alloc.alloc(Constraint, count);
        errdefer alloc.free(output);
        var initialized: usize = 0;
        errdefer for (output[0..initialized]) |value| alloc.free(value.name);
        inline for (.{ "unique_constraints", "checks", "foreign_keys" }, .{ .unique, .check, .foreign_key }) |field, kind| {
            if (@field(schema, field)) |values| for (values.value) |definition| {
                // Native uniqueness ownership and SQL constraint identity are
                // distinct. Classify once in this immutable schema view, not
                // by scanning access indexes or mutable labels per request.
                if (!namedConstraint(definition)) continue;
                output[initialized] = .{ .name = try alloc.dupe(u8, definition.name), .kind = kind, .deferrable = if (@hasField(@TypeOf(definition), "deferrable")) definition.deferrable orelse false else false };
                initialized += 1;
            };
        }
        return output;
    }
};

pub const Table = struct {
    pub const ExternalIndexes = struct {
        /// Fresh query-definition metadata, allocated for this execution. A
        /// schema/prepared-plan cache must never retain a publication pointer.
        catalog_json: []const u8,
        object_generation: u64 = 0,
        indexes_json: []const u8,
        schema_json: []const u8 = "",
        desired: [32]u8,
    };
    pub const Scope = struct {
        database: []const u8,
        namespace: []const u8,
        name: []const u8,
        revision: u64,

        pub fn matchesQualifier(self: Scope, qualifier: []const u8) bool {
            var parts = std.mem.splitScalar(u8, qualifier, 0);
            const first = parts.next().?;
            const second = parts.next() orelse return std.mem.eql(u8, first, self.name);
            const third = parts.next() orelse return std.mem.eql(u8, first, self.namespace) and std.mem.eql(u8, second, self.name);
            return parts.next() == null and std.mem.eql(u8, first, self.database) and std.mem.eql(u8, second, self.namespace) and std.mem.eql(u8, third, self.name);
        }
    };
    id: u64,
    physical_name: []const u8,
    schema_version: u32,
    storage_mode: enum { relational, document } = .relational,
    /// A pinned custom TTL field owns the physical version when present.
    /// Its absence/null falls back to the retained transaction write stamp.
    version_field: ?[]const u8 = null,
    /// External bindings are read-only and pinned by the serving cursor.
    external_base_source: ?@import("../serverless/external_source/schema_binding.zig").OwnedExternalTableBinding = null,
    external_indexes: ?ExternalIndexes = null,
    columns: []const Column,
    /// Optional directory retained by the same immutable owner as columns.
    column_index: ?*const std.StringHashMapUnmanaged(usize) = null,
    indexes: []const Index = &.{},
    index_definitions: []const IndexDefinition = &.{},
    constraints: []const Constraint = &.{},
    /// Request-owned logical authority. Never use a mutable adapter's last
    /// resolved name to validate an earlier table in a join or subquery.
    scope: ?Scope = null,
    row_policy_purpose: @import("../usermgr/row_policy_authority.zig").Purpose = .ordinary,
    sql_select_required: bool = false,
    /// Stored SQL keeps creation-time lexical names independently of the
    /// current logical scope used for catalog and authorization checks.
    binding_scope: ?Scope = null,
    /// Request-owned read authority. Never serialized, cached in a plan, or
    /// inferred from a SQL identifier; native owners receive signed proofs.
    read_authority: ?*const anyopaque = null,
    /// Definition-only logical shapes can suppress physical row-envelope
    /// columns. Executable stored views use ReadRelation, never table routing.
    virtual_columns_override: ?[]const Column = null,

    /// Hidden SQL columns owned by the row envelope, not the user schema.
    /// Binders enumerate these explicitly so SELECT * keeps its stored shape.
    const identity_virtual_columns = [_]Column{.{ .name = "_id", .path = "_id", .type = .string, .nullable = false }};
    const document_virtual_columns = identity_virtual_columns ++ [_]Column{
        .{ .name = "_doc", .path = "_doc", .type = .json, .element_type = .jsonb, .nullable = false },
        .{ .name = "_version", .path = "_version", .type = .integer, .element_type = .int64, .nullable = false, .generated = true },
    };

    pub fn virtualColumns(self: Table) []const Column {
        if (self.virtual_columns_override) |columns| return columns;
        return if (self.storage_mode == .document) &document_virtual_columns else &identity_virtual_columns;
    }

    /// A virtual JSONB value, never a stored property or a SELECT * column.
    pub fn isDocumentRoot(self: Table, name: []const u8) bool {
        if (self.storage_mode != .document) return false;
        if (std.mem.eql(u8, name, "_doc")) return true;
        if (!std.mem.endsWith(u8, name, "\x00_doc")) return false;
        const resolved = self.column(name) catch return false;
        return std.mem.eql(u8, resolved.name, "_doc");
    }

    pub fn isDocumentVersion(self: Table, name: []const u8) bool {
        if (self.storage_mode != .document) return false;
        const resolved = self.column(name) catch return false;
        return std.mem.eql(u8, resolved.name, "_version");
    }

    pub fn preparedVersion(self: Table, row: std.json.Value, fallback: u64) !u64 {
        if (self.version_field) |field| if (row == .object) {
            if (row.object.get(field)) |value| if (value != .null)
                return @import("../schema/table_schema_impl.zig").parseTtlTimestampNs(value);
        };
        return fallback;
    }

    /// Inserts may supply the physical key; updates cannot move an engine
    /// envelope or assign its observed version. All mutation planners share
    /// this binding check before capturing source rows or proposing writes.
    pub fn updateTarget(self: Table, name: []const u8) !Column {
        const selected = try self.column(name);
        if (std.mem.eql(u8, selected.name, "_id") or self.isDocumentVersion(selected.name)) return error.SqlSystemColumnWrite;
        return selected;
    }

    pub fn validateDocumentWriteColumns(self: Table, names: []const []const u8) !void {
        var root = false;
        var fields = false;
        for (names) |name| {
            root = root or self.isDocumentRoot(name);
            fields = fields or (!self.isDocumentRoot(name) and !std.mem.eql(u8, name, "_id"));
        }
        if (root and fields) return error.UnsupportedSqlShape;
        if (self.storage_mode != .document) return;
        for (names, 0..) |name, position| {
            if (self.isDocumentRoot(name) or self.isDocumentVersion(name) or std.mem.eql(u8, name, "_id")) continue;
            const selected_column = try self.column(name);
            const fallback = [_][]const u8{selected_column.path};
            const path = if (selected_column.document_path.len != 0) selected_column.document_path else &fallback;
            for (names[0..position]) |previous| {
                if (self.isDocumentRoot(previous) or self.isDocumentVersion(previous) or std.mem.eql(u8, previous, "_id")) continue;
                const other = try self.column(previous);
                const previous_fallback = [_][]const u8{other.path};
                const other_path = if (other.document_path.len != 0) other.document_path else &previous_fallback;
                if (selected_column.document_path.len == 0 and other.document_path.len == 0) continue;
                var overlaps = true;
                for (path[0..@min(path.len, other_path.len)], other_path[0..@min(path.len, other_path.len)]) |left, right| overlaps = overlaps and std.mem.eql(u8, left, right);
                if (overlaps) return error.UnsupportedSqlShape;
            }
        }
    }

    /// Explicit JSON null members in a whole-document value are JSON datums,
    /// not SQL NULL property assignments. Names borrow the pinned schema;
    /// only the compact provenance slice belongs to the preparation arena.
    pub fn documentRootNulls(self: Table, alloc: std.mem.Allocator, root: std.json.Value) ![]const []const u8 {
        if (self.storage_mode != .document or root != .object) return error.InvalidSqlBackendResponse;
        var names: std.ArrayList([]const u8) = .empty;
        errdefer names.deinit(alloc);
        for (self.columns) |field| if (field.type == .json) {
            if (root.object.get(field.path)) |value| if (value == .null) try names.append(alloc, field.path);
        };
        return names.toOwnedSlice(alloc);
    }

    pub fn column(self: Table, name: []const u8) !Column {
        for (self.virtualColumns()) |value| if (std.mem.eql(u8, value.name, name)) return value;
        if (self.column_index) |index| {
            if (index.get(name)) |ordinal| {
                if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
                return self.columns[ordinal];
            }
        } else for (self.columns) |value| if (std.mem.eql(u8, value.name, name)) return value;
        // Point predicate planning must use the same pinned logical authority
        // as relation binding; never strip a namespace before validating it.
        const separator = std.mem.lastIndexOfScalar(u8, name, 0) orelse return error.UndefinedColumn;
        const scope = self.scope orelse return error.UndefinedColumn;
        if (!scope.matchesQualifier(name[0..separator])) return error.UndefinedColumn;
        const column_name = name[separator + 1 ..];
        for (self.virtualColumns()) |value| if (std.mem.eql(u8, value.name, column_name)) return value;
        if (self.column_index) |index| {
            if (index.get(column_name)) |ordinal| {
                if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
                return self.columns[ordinal];
            }
        } else for (self.columns) |value| if (std.mem.eql(u8, value.name, column_name)) return value;
        return error.UndefinedColumn;
    }

    /// Compiler-only typed input view. It grants no native capability or
    /// storage authority and must never be published as a storage schema.
    /// Identity is compared only within this already-authorized pinned table.
    /// The region owns the column vector; names/paths borrow that pinned table.
    pub fn expressionSchemaLeaky(self: Table, a: std.mem.Allocator) !@import("../storage/schema.zig").TableSchema {
        if (self.storage_mode != .relational) return error.UnsupportedSqlShape;
        const native_schema = @import("../storage/schema.zig");
        const columns = try a.alloc(native_schema.RelationalColumn, self.columns.len);
        for (self.columns, columns) |column_, *out| out.* = .{
            .name = column_.name,
            .path = column_.path,
            .column_type = switch (column_.type) {
                .string, .uuid => .string,
                .integer => .integer,
                .number => if (column_.element_type == .numeric) .numeric else .number,
                .array => .sql_array,
                .numrange => .numrange,
                .daterange => .daterange,
                .tsrange => .tsrange,
                .tstzrange => .tstzrange,
                .boolean => .boolean,
                .datetime => .datetime,
                .json => .json,
            },
            .required = !column_.nullable,
            .allows_null = column_.nullable,
            .sql_element_type = column_.element_type,
            .numeric_modifier = column_.numeric_modifier,
        };
        return .{ .storage_mode = .relational, .version = self.schema_version, .relational_columns = columns, .requires_json_expressions = true };
    }

    pub fn columnAliases(self: Table, alloc: std.mem.Allocator, name: []const u8) ![]const []const u8 {
        const scope = self.scope orelse return &.{};
        const aliases = try alloc.alloc([]const u8, 3);
        aliases[0] = try std.fmt.allocPrint(alloc, "{s}\x00{s}", .{ scope.name, name });
        aliases[1] = try std.fmt.allocPrint(alloc, "{s}\x00{s}\x00{s}", .{ scope.namespace, scope.name, name });
        aliases[2] = try std.fmt.allocPrint(alloc, "{s}\x00{s}\x00{s}\x00{s}", .{ scope.database, scope.namespace, scope.name, name });
        return aliases;
    }
};

pub const Action = enum { read, write, read_write, admin };
pub const Condition = struct {
    pub const Op = enum { eq, neq, lt, lte, gt, gte, is_null, is_not_null };
    /// Native relational three-valued comparison; only TRUE rows match.
    /// Equality with NULL is UNKNOWN, distinct from the explicit null tests.
    column: []const u8,
    op: Op,
    value: std.json.Value = .null,
    /// Exact statement-owned operand. `value` is its lossless compatibility
    /// encoding for tuple bounds; typed consumers must retain this identity.
    operand: ?@import("scalar.zig").Datum = null,
    pub fn datum(self: Condition) @import("scalar.zig").Datum {
        return self.operand orelse @import("scalar.zig").Datum.fromJson(self.value);
    }
};
test "SQL document root virtual columns preserve narrow storage projections and authority" {
    const table: Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = &.{.{ .name = "title", .path = "title", .type = .string }}, .scope = .{ .database = "default", .namespace = "public", .name = "docs", .revision = 1 } };
    const root = try table.column("_doc");
    try std.testing.expectEqual(ast.ColumnType.json, root.type);
    try std.testing.expectEqual(@import("array_value.zig").ElementType.jsonb, root.element_type.?);
    try std.testing.expect(table.isDocumentRoot("public\x00docs\x00_doc"));
    try std.testing.expect(!table.isDocumentRoot("other\x00docs\x00_doc"));
    try std.testing.expectError(error.UndefinedColumn, table.column("other\x00docs\x00_doc"));
    try std.testing.expectEqual(@as(usize, 1), table.columns.len);
    var empty: std.heap.FixedBufferAllocator = .init(&.{});
    const narrow: Scan = .{ .fields = &.{"title"}, .limit = 1 };
    try std.testing.expect((try narrow.physicalFields(empty.allocator(), table)).ptr == narrow.fields.ptr);
    try std.testing.expect(!narrow.needsDocument(table));
    const whole: Scan = .{ .fields = &.{ "_doc", "title" }, .limit = 1 };
    const fields = try whole.physicalFields(std.testing.allocator, table);
    defer std.testing.allocator.free(fields);
    try std.testing.expectEqual(@as(usize, 1), fields.len);
    try std.testing.expectEqualStrings("title", fields[0]);
    try std.testing.expect(whole.needsDocument(table));
    var relational = table;
    relational.storage_mode = .relational;
    relational.columns = &.{.{ .name = "_doc", .path = "_doc", .type = .string }};
    try std.testing.expectEqual(ast.ColumnType.string, (try relational.column("_doc")).type);
    try std.testing.expect(!whole.needsDocument(relational));
    try std.testing.expect((try whole.physicalFields(empty.allocator(), relational)).ptr == whole.fields.ptr);
    const invalid: Row = .{ .id = "bad", .version = 1, .value = .null, .document = .null };
    try std.testing.expectError(error.InvalidSqlBackendResponse, invalid.cell("_doc"));
    try std.testing.expectError(error.InvalidSqlBackendResponse, invalid.hasField("_doc"));
}

test "SQL document root null provenance marks only explicit declared JSON members" {
    const alloc = std.testing.allocator;
    var root = try std.json.parseFromSlice(std.json.Value, alloc, "{\"metadata\":null,\"title\":null,\"unknown\":null}", .{});
    defer root.deinit();
    const table: Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = &.{
        .{ .name = "metadata", .path = "metadata", .type = .json },
        .{ .name = "missing", .path = "missing", .type = .json },
        .{ .name = "title", .path = "title", .type = .string },
    } };
    const fields = try table.documentRootNulls(alloc, root.value);
    defer alloc.free(fields);
    try std.testing.expectEqual(@as(usize, 1), fields.len);
    try std.testing.expectEqualStrings("metadata", fields[0]);
    const Faults = struct {
        fn run(a: std.mem.Allocator, definition: Table, value: std.json.Value) !void {
            const nulls = try definition.documentRootNulls(a, value);
            defer a.free(nulls);
            try std.testing.expectEqual(@as(usize, 1), nulls.len);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Faults.run, .{ table, root.value });
}

pub const Scan = struct {
    pub fn needsDocument(self: Scan, table: Table) bool {
        if (self.include_document) return true;
        for (self.fields) |name| if (table.isDocumentRoot(name)) return true;
        return false;
    }

    /// Virtual document values travel as page-owned preimages, never as a
    /// fictitious storage field. Ordinary narrow projections borrow unchanged.
    pub fn physicalFields(self: Scan, alloc: std.mem.Allocator, table: Table) ![]const []const u8 {
        var mapped = false;
        for (self.fields) |name| {
            if (table.isDocumentRoot(name) or table.isDocumentVersion(name)) {
                mapped = true;
                continue;
            }
            if (table.storage_mode == .document and (try table.column(name)).document_path.len != 0) mapped = true;
        }
        if (!mapped) return self.fields;
        var selected: std.StringArrayHashMapUnmanaged(void) = .empty;
        defer selected.deinit(alloc);
        for (self.fields) |name| {
            if (table.isDocumentRoot(name) or table.isDocumentVersion(name)) continue;
            const column = try table.column(name);
            const path = if (table.storage_mode == .document and column.document_path.len != 0) column.document_path[0] else name;
            try selected.put(alloc, path, {});
        }
        return alloc.dupe([]const u8, selected.keys());
    }
    pub const Order = struct { column: []const u8, descending: bool = false, nulls_first: bool = false };
    pub const IndexEquality = struct {
        name: []const u8,
        values: []const std.json.Value,
    };
    pub const IndexRange = struct {
        pub const Bound = struct { values: []const std.json.Value, inclusive: bool = true };
        name: []const u8,
        lower: ?Bound = null,
        upper: ?Bound = null,
        after: ?[]const u8 = null,
    };
    pub const Search = struct { expression: ast.Search, request_text: ?[]const u8 = null, limit: ?u32 = null };
    search: ?*const Search = null,
    /// A request, never proof. Providers explicitly attest the entire order.
    order: []const Order = &.{},
    /// Advisory SQL OFFSET + LIMIT for costing only. Never a scan stop bound.
    /// Absent when residual selectivity is unknown. Page size remains limit.
    row_goal: ?u64 = null,
    index_range: ?IndexRange = null,
    fields: []const []const u8,
    /// Full document preimage for replacement preparation or explicit _doc.
    include_document: bool = false,
    include_primary_digest: bool = false,
    /// Ordered SQL must not be satisfied by an unrelated secondary-key order.
    primary_order: bool = false,
    /// Exact physical identity, not a schema predicate. Point pages exhaust
    /// after their matching row (or absence), without a continuation probe.
    primary_key: ?[]const u8 = null,
    /// Snapshot-bound remote candidates. null means a full scan; an empty
    /// slice means no rows. Consumers retain ranking separately from physical
    /// hydration order. The lake cursor owns and validates the selection.
    physical_selection: ?@import("../serverless/query/lake_row_selection.zig").Selection = null,
    row_refs: ?[]const @import("../storage/rowsource/types.zig").RowRef = null,
    /// Require a READY schema-bound index. Unlike auto_index this must fail
    /// closed; the coordinated owner read returns one exact-span proof.
    index_equality: ?IndexEquality = null,
    conditions: []const Condition = &.{},
    expression_equalities: []const @import("../storage/relational_index.zig").ExpressionEqualityHint = &.{},
    after: ?[]const u8 = null,
    /// External scan upper bound, validated against the opened snapshot.
    before: ?[]const u8 = null,
    limit: u32,
};
pub const Row = struct {
    id: []const u8,
    version: u64,
    /// A staged document exposes its prepared write stamp to SQL, while
    /// version and expected_content_digest still fence the observed primary.
    /// Never use the projected stamp as an optimistic concurrency predicate.
    document_version: ?u64 = null,
    value: std.json.Value,
    index_cursor: ?[]const u8 = null,
    /// Digest of the exact primary bytes in this snapshot. Timestamps alone
    /// are not a version fence when a custom TTL field is unchanged.
    expected_content_digest: ?[32]u8 = null,
    document: ?std.json.Value = null,
    /// Native document metadata is not a relational user column of the same
    /// spelling. Only schema-bound document projections grant this identity.
    document_envelope: bool = false,
    /// Authoritative native null flags aligned with value.object insertion
    /// order. Null means a legacy JSON-only backend without that distinction.
    sql_nulls: ?[]const bool = null,
    pattern_sources: ?[]const ?*@import("scalar.zig").PatternSet = null,
    /// Internal relation rows retain complete, owned Datums and share a single
    /// page-local name directory, without constructing per-row JSON objects.
    /// Legacy backend rows continue to use JSON and the null/pattern channels.
    typed_cells: ?TypedCells = null,

    pub const TypedCells = struct {
        layout: *const TypedLayout,
        values: []const Cell = &.{},
        presence: ?[]const bool = null,
        /// Shared immutable column blocks, owned by the enclosing page lease.
        columnar: ?struct { blocks: []const @import("spill.zig").ColumnarBlock, row: usize } = null,
        pub fn count(self: TypedCells) usize {
            if (self.columnar != null) return self.layout.names.len;
            return self.values.len;
        }
        pub fn frozenJson(self: TypedCells, ordinal: usize) !?@import("execution_batch.zig").FrozenJson {
            if (ordinal >= self.count()) return error.InvalidSqlBackendResponse;
            const source = self.columnar orelse return null;
            if (self.values.len != 0) return error.InvalidSqlBackendResponse;
            if (self.presence) |flags| if (!flags[ordinal]) return null;
            var first: usize = 0;
            for (source.blocks) |block| {
                if (ordinal - first < block.values.len) return block.frozenJson(source.row, ordinal - first, false);
                first += block.values.len;
            }
            return error.InvalidSqlBackendResponse;
        }
        pub fn encodedJson(self: TypedCells, ordinal: usize) !?[]const u8 {
            if (ordinal >= self.count()) return error.InvalidSqlBackendResponse;
            const source = self.columnar orelse return null;
            if (self.values.len != 0) return error.InvalidSqlBackendResponse;
            if (self.presence) |flags| if (!flags[ordinal]) return null;
            var first: usize = 0;
            for (source.blocks) |block| {
                if (ordinal - first < block.values.len) return block.encodedJson(source.row, ordinal - first, false);
                first += block.values.len;
            }
            return error.InvalidSqlBackendResponse;
        }
        pub fn at(self: TypedCells, ordinal: usize) !Cell {
            if (self.columnar) |source| {
                if (self.values.len != 0 or ordinal >= self.layout.names.len) return error.InvalidSqlBackendResponse;
                var first: usize = 0;
                for (source.blocks) |block| {
                    if (ordinal - first < block.values.len) return block.cell(source.row, ordinal - first);
                    first += block.values.len;
                }
                return error.InvalidSqlBackendResponse;
            }
            if (ordinal >= self.values.len) return error.InvalidSqlBackendResponse;
            return self.values[ordinal];
        }
    };

    pub const Cell = @import("scalar.zig").Datum;
    pub const TypedLayout = struct {
        names: []const []const u8 = &.{},
        ordinals: std.StringHashMapUnmanaged(usize) = .empty,

        /// Names borrow the immutable relation binding; entries belong to the
        /// page arena. Duplicate names must not silently overwrite an ordinal.
        pub fn init(arena: std.mem.Allocator, names: []const []const u8) !*const TypedLayout {
            const result = try arena.create(TypedLayout);
            result.* = .{ .names = names };
            for (names, 0..) |name, index| {
                const entry = try result.ordinals.getOrPut(arena, name);
                if (entry.found_existing) return error.InvalidSqlBackendResponse;
                entry.value_ptr.* = index;
            }
            return result;
        }

        /// Clone once per emitted page, not once per row. No names or directory
        /// entries may borrow a cursor that can close before the page consumer.
        pub fn clone(self: *const TypedLayout, alloc: std.mem.Allocator) !*const TypedLayout {
            const names = try alloc.alloc([]const u8, self.names.len);
            for (self.names, names) |name, *out| out.* = try alloc.dupe(u8, name);
            return init(alloc, names);
        }
    };

    /// Clone once at the page ownership boundary; consumers borrow the typed
    /// cells until that page is released. Layout and allocations must belong
    /// to the same page owner (or a longer-lived immutable binding).
    pub fn fromDatums(alloc: std.mem.Allocator, id: []const u8, layout: *const TypedLayout, values: []const Cell) !Row {
        if (layout.ordinals.count() != values.len) return error.InvalidSqlBackendResponse;
        const owned = try alloc.alloc(Cell, values.len);
        for (values, owned) |value, *out| out.* = try @import("operators.zig").cloneDatum(alloc, value);
        return .{ .id = try alloc.dupe(u8, id), .version = 0, .value = .null, .typed_cells = .{ .layout = layout, .values = owned } };
    }

    /// Decoding is explicit about the SQL/JSON null boundary. The native
    /// projection's field names are literal names, never dotted JSON paths.
    pub fn frozenJson(self: Row, name: []const u8) !?@import("execution_batch.zig").FrozenJson {
        if (std.mem.eql(u8, name, "_id")) return null;
        const cells = self.typed_cells orelse return null;
        try self.validateTyped();
        const ordinal = cells.layout.ordinals.get(name) orelse return null;
        return cells.frozenJson(ordinal);
    }
    pub fn encodedJson(self: Row, name: []const u8) !?[]const u8 {
        if (std.mem.eql(u8, name, "_id")) return null;
        const cells = self.typed_cells orelse return null;
        try self.validateTyped();
        const ordinal = cells.layout.ordinals.get(name) orelse return null;
        return cells.encodedJson(ordinal);
    }
    pub fn cell(self: Row, name: []const u8) !Cell {
        if (std.mem.eql(u8, name, "_id")) return .{ .value = .{ .string = self.id }, .sql_null = false };
        if (self.document_envelope and std.mem.eql(u8, name, "_version")) return Cell.json(.{ .integer = std.math.cast(i64, self.document_version orelse self.version) orelse return error.SqlNumericOutOfRange });
        if (std.mem.eql(u8, name, "_doc")) if (self.document) |document| {
            if (document != .object) return error.InvalidSqlBackendResponse;
            return Cell.json(document);
        };
        if (self.typed_cells) |cells| {
            try self.validateTyped();
            const index = cells.layout.ordinals.get(name) orelse return .{};
            if (index >= cells.count()) return error.InvalidSqlBackendResponse;
            const result = try cells.at(index);
            if (cells.presence) |present| if (!present[index] and !result.sql_null) return error.InvalidSqlBackendResponse;
            if (result.array != null and (result.sql_null or result.value != .null or result.patterns() != null)) return error.InvalidSqlBackendResponse;
            if (result.sql_null and result.value != .null) return error.InvalidSqlBackendResponse;
            return result;
        }
        if (self.value != .object) return error.InvalidSqlBackendResponse;
        if (self.sql_nulls) |flags| if (flags.len != self.value.object.count()) return error.InvalidSqlBackendResponse;
        const index = self.value.object.getIndex(name) orelse return .{ .value = .null, .sql_null = true };
        const value = self.value.object.values()[index];
        const sql_null = if (self.sql_nulls) |flags| flags[index] else value == .null;
        if (sql_null and value != .null) return error.InvalidSqlBackendResponse;
        return @import("scalar.zig").Datum.fromParts(value, sql_null, if (self.pattern_sources) |sources| if (index < sources.len) sources[index] else return error.InvalidSqlBackendResponse else null);
    }

    fn validateTyped(self: Row) !void {
        const cells = self.typed_cells orelse return error.InvalidSqlBackendResponse;
        if (cells.count() != cells.layout.ordinals.count() or cells.count() != cells.layout.names.len or
            self.value != .null or self.sql_nulls != null or self.pattern_sources != null)
            return error.InvalidSqlBackendResponse;
        if (cells.presence) |present| if (present.len != cells.count()) return error.InvalidSqlBackendResponse;
    }

    pub fn fieldNames(self: Row) ![]const []const u8 {
        if (self.typed_cells) |cells| {
            try self.validateTyped();
            return cells.layout.names;
        }
        if (self.value != .object) return error.InvalidSqlBackendResponse;
        return self.value.object.keys();
    }

    pub fn hasField(self: Row, name: []const u8) !bool {
        if (self.document_envelope and std.mem.eql(u8, name, "_version")) return true;
        if (std.mem.eql(u8, name, "_doc")) if (self.document) |document| {
            if (document != .object) return error.InvalidSqlBackendResponse;
            return true;
        };
        if (self.typed_cells) |cells| {
            try self.validateTyped();
            const index = cells.layout.ordinals.get(name) orelse return false;
            if (index >= cells.count()) return error.InvalidSqlBackendResponse;
            return if (cells.presence) |present| present[index] else true;
        }
        if (self.value != .object) return error.InvalidSqlBackendResponse;
        return self.value.object.contains(name);
    }

    pub fn cloneOwned(self: Row, alloc: std.mem.Allocator) !Row {
        return self.cloneWithLayout(alloc, if (self.typed_cells) |cells| try cells.layout.clone(alloc) else null);
    }

    /// Payload ownership transfer with a page-shared, owned name directory.
    /// Compiled pattern objects retain their existing statement-owner lifetime.
    pub fn cloneWithLayout(self: Row, alloc: std.mem.Allocator, layout: ?*const TypedLayout) !Row {
        var result = self;
        result.id = try alloc.dupe(u8, self.id);
        if (self.index_cursor) |cursor| result.index_cursor = try alloc.dupe(u8, cursor);
        if (self.document) |document| result.document = try @import("../storage/typed_json.zig").clone(alloc, document);
        if (self.typed_cells) |cells| {
            try self.validateTyped();
            const owned_layout = layout orelse return error.InvalidSqlBackendResponse;
            if (owned_layout.names.len != cells.count()) return error.InvalidSqlBackendResponse;
            const values = try alloc.alloc(Cell, cells.count());
            for (values, cells.layout.names, owned_layout.names, 0..) |*out, old_name, new_name, ordinal| {
                const value = try cells.at(ordinal);
                if (!std.mem.eql(u8, old_name, new_name)) return error.InvalidSqlBackendResponse;
                _ = try self.cell(old_name);
                out.* = try @import("operators.zig").cloneDatum(alloc, value);
            }
            result.typed_cells = .{ .layout = owned_layout, .values = values, .presence = if (cells.presence) |present| try alloc.dupe(bool, present) else null };
        } else {
            result.value = try @import("../storage/typed_json.zig").clone(alloc, self.value);
            if (self.sql_nulls) |flags| result.sql_nulls = try alloc.dupe(bool, flags);
            if (self.pattern_sources) |sources| result.pattern_sources = try alloc.dupe(?*@import("scalar.zig").PatternSet, sources);
        }
        return result;
    }
};
pub const Page = struct {
    rows: []const Row,
    owned_arena: ?std.heap.ArenaAllocator = null,
    storage_page: ?@import("../storage/relational_read_view.zig").View.Page = null,
    /// Exclusive native continuation; null proves exhaustion. Implementations
    /// must not report a short filtered page as exhaustion without that proof.
    after: ?[]const u8 = null,

    pub fn deinit(self: Page) void {
        if (self.storage_page) |owned| {
            var page = owned;
            page.deinit();
        }
        if (self.owned_arena) |owned| {
            var arena = owned;
            arena.deinit();
        }
    }
};

/// Borrowed typed vectors plus a page-owned selection. Vectors remain valid
/// until the cursor's next pull or close; consumers retain only their results.
/// SQL names are literal, and null bitmaps distinguish SQL NULL from JSON null.
pub const ColumnPage = struct {
    batch: @import("../storage/rowsource/types.zig").ColumnBatch = .{ .snapshot = .{ .table_id = "sql-relation", .snapshot_id = "statement" }, .row_refs = &.{}, .columns = &.{} },
    /// Borrowed operator columns, preserving JSON and retained typed storage.
    /// The pointer avoids embedding mutually recursive Batch/ColumnPage values.
    native: ?struct { values: *const @import("execution_batch.zig").Batch, names: []const []const u8 } = null,
    /// Optional producer capability; call before the next pull. A successful
    /// lease owns vector payloads, independently of page/cursor allocators.
    retain_columns: ?struct {
        ptr: *anyopaque,
        retain_fn: *const fn (*anyopaque, std.mem.Allocator) anyerror!?@import("../storage/rowsource/types.zig").ColumnOwner,
    } = null,
    selection: []const usize,
    after: ?[]const u8 = null,
    pub fn validate(self: ColumnPage) !void {
        if (self.native) |native| {
            if (native.values.len() != 0 and native.names.len != native.values.width()) return error.InvalidSqlBackendResponse;
            for (self.selection) |index| if (index >= native.values.len()) return error.InvalidSqlBackendResponse;
        } else {
            try self.batch.validate();
            for (self.selection) |index| if (index >= self.batch.rowCount()) return error.InvalidSqlBackendResponse;
        }
    }
    /// Declared NUMERIC reads fixed coefficients directly. Raw callers retain
    /// the established exact string contract for detected decimal columns.
    pub fn typedCell(self: ColumnPage, alloc: std.mem.Allocator, row: usize, name: []const u8, numeric: bool) !Row.Cell {
        if (row >= self.selection.len) return error.InvalidSqlBackendResponse;
        if (numeric and self.native == null) if (self.batch.findColumn(name)) |column| {
            const index = self.selection[row];
            if (index >= self.batch.rowCount() or index >= column.rowCount()) return error.InvalidSqlBackendResponse;
            if (column.values == .decimal128 and !column.nulls.isNull(index)) {
                const decimal = column.values.decimal128;
                @import("../common/decimal128.zig").validateCoefficient(decimal.values[index], decimal.precision, decimal.scale) catch return error.InvalidSqlBackendResponse;
                var context: @import("numeric_value.zig").Context = .{ .alloc = alloc };
                var number = try @import("numeric_value.zig").fromScaledInteger(&context, decimal.values[index], @intCast(decimal.scale));
                errdefer number.deinit();
                const value = try alloc.create(@import("numeric_value.zig").Value);
                value.* = number.value;
                return @import("scalar.zig").Datum.typedNumeric(value);
            }
        };
        return self.cell(alloc, row, name);
    }
    pub fn frozenJson(self: ColumnPage, row: usize, name: []const u8) !?@import("execution_batch.zig").FrozenJson {
        if (row >= self.selection.len) return error.InvalidSqlBackendResponse;
        if (self.native) |native| {
            for (native.names, 0..) |column, ordinal| if (std.mem.eql(u8, column, name)) return native.values.frozenJson(self.selection[row], ordinal);
        }
        return null;
    }
    pub fn encodedJson(self: ColumnPage, row: usize, name: []const u8) !?[]const u8 {
        if (row >= self.selection.len) return error.InvalidSqlBackendResponse;
        if (self.native) |native| {
            for (native.names, 0..) |column, ordinal| if (std.mem.eql(u8, column, name)) return native.values.encodedJson(self.selection[row], ordinal);
        }
        return null;
    }
    pub fn cell(self: ColumnPage, alloc: std.mem.Allocator, row: usize, name: []const u8) !Row.Cell {
        if (row >= self.selection.len) return error.InvalidSqlBackendResponse;
        const index = self.selection[row];
        if (self.native) |native| {
            if (index >= native.values.len() or native.names.len != native.values.width()) return error.InvalidSqlBackendResponse;
            for (native.names, 0..) |column, ordinal| if (std.mem.eql(u8, column, name)) {
                const value = try native.values.cell(alloc, index, ordinal);
                return value;
            };
            return .{ .value = .null, .sql_null = true };
        }
        if (index >= self.batch.rowCount()) return error.InvalidSqlBackendResponse;
        if (std.mem.eql(u8, name, "_id")) return .{ .value = .{ .string = try @import("../storage/rowsource/identity.zig").allocId(alloc, self.batch.rowRef(index)) }, .sql_null = false };
        const column = self.batch.findColumn(name) orelse return .{ .value = .null, .sql_null = true };
        if (column.nulls.isNull(index)) return .{ .value = .null, .sql_null = true };
        const value: std.json.Value = switch (column.values) {
            .encoded => |values| try values.at(index),
            .dictionary_i64 => |values| .{ .integer = values.at(index) },
            .dictionary_f64 => |values| .{ .float = values.at(index) },
            .i64 => |values| .{ .integer = values[index] },
            .f64 => |values| .{ .float = values[index] },
            .bool => |values| .{ .bool = values[index] },
            .bytes => |values| .{ .string = values[index] },
            .dictionary_bytes => |values| .{ .string = values.at(index) },
            .json => |values| try std.json.parseFromSliceLeaky(std.json.Value, alloc, values[index], .{ .allocate = .alloc_always, .parse_numbers = false }),
            .decimal128 => |values| .{ .string = try values.text(alloc, index) },
            .vector_f32 => return error.UnsupportedSqlExecution,
        };
        return .{ .value = value, .sql_null = false };
    }
};

/// Owned statement read view. Opening pins data, not just routing metadata.
/// Page values belong to the next() allocator; the cursor lives until close().
/// Borrowed snapshot-local costing evidence. Providers never perform I/O for
/// optimizer statistics; absent fields remain unknown. Frequencies and hashes
/// describe semantic equality keys, and are advisory rather than pruning proof.
pub const ColumnStatistics = @import("../serverless/external_source/types.zig").ColumnStatistics;
/// Mergeable, advisory snapshot statistics. Fixed binary aggregates fit ordered
/// page summaries; no sketch or frequency estimate can authorize pruning.
pub const StatisticsSketch = struct {
    pub const encoded_bytes = 224;
    rows: u64 = 0,
    non_null: u64 = 0,
    bytes: u64 = 0,
    registers: [64]u8 = @splat(0),
    bounded: bool = true,
    minimum: i64 = std.math.maxInt(i64),
    maximum: i64 = std.math.minInt(i64),
    hot: [4]ColumnStatistics.Frequency = @splat(.{ .hash = 0, .rows = 0 }),
    fn frequency(self: *@This(), hash: u64, count: u64) void {
        if (count == 0) return;
        var least: usize = 0;
        for (&self.hot, 0..) |*value, i| {
            if (value.rows != 0 and value.hash == hash) {
                value.rows +|= count;
                return;
            }
            if (value.rows < self.hot[least].rows) least = i;
        }
        // Weighted Misra-Gries cancellation keeps counts as lower bounds.
        if (self.hot[least].rows == 0) {
            self.hot[least] = .{ .hash = hash, .rows = count };
            return;
        }
        const removed = @min(count, self.hot[least].rows);
        for (&self.hot) |*value| value.rows -= removed;
        if (count > removed) self.frequency(hash, count - removed);
    }
    pub fn add(self: *@This(), value: @import("scalar.zig").Datum, width: usize) !void {
        self.rows +|= 1;
        self.bytes +|= 1;
        if (value.sql_null) return;
        self.non_null +|= 1;
        self.bytes +|= width;
        const digest = try @import("scalar.zig").semanticHashDatum(value);
        const bucket: usize = @intCast(digest & 63);
        self.registers[bucket] = @max(self.registers[bucket], @as(u8, @intCast(@min(59, @ctz(digest >> 6) + 1))));
        self.frequency(digest, 1);
        const integer: ?i64 = if (value.numeric != null) null else switch (value.value) {
            .integer => |v| v,
            .bool => |v| @intFromBool(v),
            else => null,
        };
        if (integer) |v| {
            self.minimum = @min(self.minimum, v);
            self.maximum = @max(self.maximum, v);
        } else self.bounded = false;
    }
    pub fn merge(self: *@This(), other: @This()) void {
        self.rows +|= other.rows;
        self.non_null +|= other.non_null;
        self.bytes +|= other.bytes;
        for (&self.registers, other.registers) |*r, value| r.* = @max(r.*, value);
        self.bounded = self.bounded and other.bounded;
        self.minimum = @min(self.minimum, other.minimum);
        self.maximum = @max(self.maximum, other.maximum);
        for (other.hot) |value| self.frequency(value.hash, value.rows);
    }
    pub fn estimate(self: @This(), a: std.mem.Allocator, name: []const u8) !ColumnStatistics {
        var inverse: f64 = 0;
        var empty: f64 = 0;
        for (self.registers) |rank| {
            inverse += std.math.pow(f64, 2, -@as(f64, @floatFromInt(rank)));
            if (rank == 0) empty += 1;
        }
        const raw = if (empty != 0) 64 * @log(64 / empty) else 0.709 * 64 * 64 / inverse;
        const count: u64 = @intFromFloat(@min(@as(f64, @floatFromInt(self.non_null)), @max(0, @min(@as(f64, @floatFromInt(std.math.maxInt(u64) - 2048)), @round(raw)))));
        var frequencies: std.ArrayList(ColumnStatistics.Frequency) = .empty;
        errdefer frequencies.deinit(a);
        for (self.hot) |value| if (value.rows != 0) try frequencies.append(a, value);
        return .{ .name = name, .population_rows = self.rows, .non_null_rows = self.non_null, .distinct_count = count, .average_bytes = @intCast(@min(std.math.maxInt(u32), @max(1, self.bytes / @max(1, self.rows)))), .min_integer = if (self.bounded and self.non_null != 0) self.minimum else null, .max_integer = if (self.bounded and self.non_null != 0) self.maximum else null, .frequencies = try frequencies.toOwnedSlice(a) };
    }
    pub fn encode(self: @This()) [encoded_bytes]u8 {
        var bytes: [encoded_bytes]u8 = @splat(0);
        bytes[0] = 1;
        bytes[1] = @intFromBool(self.bounded);
        std.mem.writeInt(u64, bytes[8..16], self.rows, .little);
        std.mem.writeInt(u64, bytes[16..24], self.non_null, .little);
        std.mem.writeInt(u64, bytes[24..32], self.bytes, .little);
        std.mem.writeInt(i64, bytes[32..40], self.minimum, .little);
        std.mem.writeInt(i64, bytes[40..48], self.maximum, .little);
        bytes[48..112].* = self.registers;
        for (self.hot, 0..) |value, i| {
            const offset = 112 + i * 16;
            std.mem.writeInt(u64, bytes[offset..][0..8], value.hash, .little);
            std.mem.writeInt(u64, bytes[offset + 8 ..][0..8], value.rows, .little);
        }
        return bytes;
    }
    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != encoded_bytes or bytes[0] != 1 or bytes[1] > 1) return error.InvalidSqlBackendResponse;
        var result: @This() = .{ .rows = std.mem.readInt(u64, bytes[8..16], .little), .non_null = std.mem.readInt(u64, bytes[16..24], .little), .bytes = std.mem.readInt(u64, bytes[24..32], .little), .minimum = std.mem.readInt(i64, bytes[32..40], .little), .maximum = std.mem.readInt(i64, bytes[40..48], .little), .bounded = bytes[1] != 0, .registers = bytes[48..112].* };
        if (result.non_null > result.rows or (result.bounded and result.non_null != 0 and result.minimum > result.maximum)) return error.InvalidSqlBackendResponse;
        for (result.registers) |rank| if (rank > 59) return error.InvalidSqlBackendResponse;
        for (&result.hot, 0..) |*value, i| {
            const offset = 112 + i * 16;
            value.* = .{ .hash = std.mem.readInt(u64, bytes[offset..][0..8], .little), .rows = std.mem.readInt(u64, bytes[offset + 8 ..][0..8], .little) };
            if (value.rows > result.non_null) return error.InvalidSqlBackendResponse;
        }
        return result;
    }
};
pub const Cursor = struct {
    /// True only when this cursor preserves every requested Scan.order key.
    order_satisfied: bool = false,
    /// Snapshot-local optimizer estimates; absent means unknown, never zero.
    /// Optional estimate after the captured Scan.conditions, never an execution cap.
    column_statistics: []const ColumnStatistics = &.{},
    estimated_filtered_rows: ?u64 = null,
    estimated_rows: ?u64 = null,
    estimated_bytes: ?u64 = null,
    ptr: *anyopaque,
    next: *const fn (*anyopaque, std.mem.Allocator, u32) anyerror!Page,
    close: *const fn (*anyopaque) void,
    /// Optional native column path; consumers may fall back to next().
    next_columns: ?*const fn (*anyopaque, std.mem.Allocator, u32) anyerror!ColumnPage = null,
    /// Borrow immutable filter evidence before any pull; false declines it.
    set_dynamic_filter: ?*const fn (*anyopaque, *const @import("dynamic_filter.zig").Filter) anyerror!bool = null,
    /// Negotiate complete key spooling before building the filter. Absent
    /// means Bloom/domain evidence suffices; do not spill unused exact tuples.
    /// Planning may also inspect this capability for identity join keys.
    /// Read captured metadata only; do not start scans or fetch source objects.
    wants_exact_filter: ?*const fn (*anyopaque, []const @import("dynamic_filter.zig").Column) anyerror!bool = null,
    /// Split an unopened pinned scan into disjoint work for exact associative
    /// reducers. Providers may distribute row groups dynamically; child order
    /// is not source order. Children share the snapshot and close before their parent.
    split_scan: ?*const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]Cursor = null,
    /// Release dynamically claimed work when a reducer completes or stops
    /// early. This does not close the cursor; its owner still calls close().
    finish_partition: ?*const fn (*anyopaque) void = null,
    /// Ordered, disjoint ranges whose concatenation preserves source order.
    split_ordered: ?*const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]Cursor = null,
    /// Rolling ordered ranges. Consume and close every child before asking for
    /// the next bounded batch; null is end of source. Later file/footer errors
    /// are reported only when the caller reaches that batch.
    split_ordered_batch: ?*const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]Cursor = null,
    /// Provider estimate of retained metadata per ordered child. Planning
    /// limits fan-out before cloning a large immutable inventory.
    ordered_split_bytes: usize = 0,
    /// Exact snapshot count; null means the retained cursor must be scanned.
    /// Providers may use metadata only after accounting for filters/deletes.
    count_rows: ?*const fn (*anyopaque) anyerror!?u64 = null,
};

/// Planning-only metadata. Estimates may be stale and never establish an
/// execution snapshot, exact count, visibility proof or row-admission bound.
pub const ScanEstimate = struct {
    estimated_rows: ?u64 = null,
    estimated_filtered_rows: ?u64 = null,
    estimated_bytes: ?u64 = null,
};
pub const StatementScan = struct { table: Table, request: Scan };
/// Every physical scan instance is pinned to one coordinated visibility cut.
/// Cursor handles and their storage belong to this owner; callers invoke only
/// next(), never cursor.close(). close() releases every cursor exactly once.
pub const StatementRead = struct {
    ptr: *anyopaque,
    cursors: []const Cursor,
    close: *const fn (*anyopaque) void,
};
pub const Mutation = struct {
    row_policy_requirement: @import("../usermgr/row_policy_authority.zig").MutationRequirement = .native,
    /// Owned native authority envelope. The SQL engine carries but never
    /// interprets native claim keys, generation fences or commands.
    conflict_guard: ?*const anyopaque = null,
    /// Read-set fence only; never a delete, write or affected result row.
    predicate_only: bool = false,
    key: []const u8,
    expected_version: u64,
    expected_content_digest: ?[32]u8 = null,
    /// A server-authored INSERT identity constraint, rather than an observed read.
    unique_absence: bool = false,
    row: ?std.json.Value,
    json_null_fields: []const []const u8 = &.{},
    /// Owned preimage retained only for DELETE RETURNING. The observed version
    /// predicate makes it authoritative only after a successful commit.
    previous: ?*const Row = null,
};
pub const ConflictExpression = struct { json: []const u8, result_type: ast.ColumnType };
pub const ConflictTarget = struct {
    columns: []const []const u8 = &.{},
    expressions: []const ConflictExpression = &.{},
    conditions: []const Condition = &.{},
    constraint_name: ?[]const u8 = null,
};

pub const ConflictOwner = struct {
    key: ?[]const u8,
    identity: ?[]const u8,
    identities: []const []const u8 = &.{},
    guard: ?*const anyopaque,
    /// The bound schema declares no native unique arbiters. The ordinary
    /// schema-version and primary predicates protect this negative proof.
    primary_only: bool = false,
};
pub const MutationOutcome = enum {
    committed,
    committed_pending,
    committed_repair_required,
    committed_graph_metric_materialization_rejected,
};

pub const Ddl = union(enum) {
    create_table: struct {
        name: ast.Name,
        schema_json: []const u8,
        if_not_exists: bool,
        tablespace: ?[]const u8 = null,
        column_sequences: []const @import("../system_catalog/column_sequences.zig").Creation = &.{},
        /// Compiler-owned provenance for admission-time name selection.
        /// Explicit user names must never be included or silently renamed.
        generated_constraint_names: []const []const u8 = &.{},
    },
    drop_table: ast.DropTable,
    catalog_ddl: ast.CatalogDdl,
    policy_ddl: ast.PolicyDdl,
};
pub const DdlReceipt = struct {
    pub const State = enum { ready, pending, invalid, admission_unknown };

    database: []const u8,
    namespace: []const u8,
    table: []const u8,
    table_id: []const u8,
    schema_version: u32,
    state: State,
    diagnostic: ?[]const u8 = null,
    restore_job_id: ?[]const u8 = null,
    idempotency_key: ?[]const u8 = null,
    fk_generation_publication_id: ?[]const u8 = null,
    index_publication_token: ?[]const u8 = null,
    schema_validation_token: ?[]const u8 = null,
};

/// Native catalog operations have no physical table/schema receipt. Identity
/// integers stay native here and stringify as exact decimal strings on wire.
pub const CatalogOperationReceipt = struct {
    pub const Kind = enum { sequence };
    pub const Operation = enum { create, alter, rename, drop };
    pub const State = enum { committed, rejected, unknown };
    pub const Diagnostic = struct { code: []const u8, message: []const u8, retryable: bool };
    kind: Kind = .sequence,
    operation: Operation,
    database: []const u8,
    namespace: []const u8,
    name: []const u8,
    state: State = .unknown,
    recovery_token: []const u8,
    object_id: ?u64 = null,
    generation: ?u64 = null,
    catalog_revision: ?u64 = null,
    diagnostic: ?Diagnostic = null,

    /// These four strings are owned by the result allocator. Result arenas
    /// can reclaim them together; direct callers may release individually.
    pub fn deinit(self: CatalogOperationReceipt, a: std.mem.Allocator) void {
        a.free(self.database);
        a.free(self.namespace);
        a.free(self.name);
        a.free(self.recovery_token);
    }

    pub fn jsonStringify(self: CatalogOperationReceipt, writer: anytype) !void {
        // Match the generated HTTP model independently of the outer encoder's
        // options: optional identity/diagnostic fields are absent, never null.
        try writer.beginObject();
        inline for (.{ "kind", "operation", "database", "namespace", "name", "state", "recovery_token" }) |field| {
            try writer.objectField(field);
            try writer.write(@field(self, field));
        }
        inline for (.{ "object_id", "generation", "catalog_revision" }) |field| {
            if (@field(self, field)) |value| {
                var buffer: [20]u8 = undefined;
                try writer.objectField(field);
                try writer.write(std.fmt.bufPrint(&buffer, "{d}", .{value}) catch unreachable);
            }
        }
        if (self.diagnostic) |value| {
            try writer.objectField("diagnostic");
            try writer.write(value);
        }
        try writer.endObject();
    }
};
/// Scratch must reclaim individual allocations; result may be an arena. DDL
/// implementations reserve durable receipts in result before admission and
/// release parsing/validation/plan memory through scratch before returning.
pub const DdlAllocators = struct {
    /// HTTP can return a pre-owned recovery receipt after bounded observation;
    /// PostgreSQL wire waits for terminal completion within its request budget.
    pub const Completion = enum { bounded, until_deadline };
    result: std.mem.Allocator,
    scratch: std.mem.Allocator,
    completion: Completion = .bounded,
};

pub const DdlOutcome = struct { mutation_outcome: ?MutationOutcome = .committed, receipt: ?DdlReceipt = null, catalog_receipt: ?CatalogOperationReceipt = null };

pub const ScanQuota = @import("native_scan_quota.zig").Quota;

/// Complete, authorized aggregate states for one pinned table and recipe.
/// Keys and versioned aggregate cells borrow the supplied page allocator until the next pull.
/// A provider must return null before opening if it cannot prove equivalence;
/// errors after selection abort execution rather than mixing source snapshots.
pub const GroupSelection = struct {
    spill: ?*@import("spill.zig").Manager = null,
    memory_bytes: usize = 8 * 1024 * 1024,
    row_limit: usize = std.math.maxInt(usize),
    page_limit: usize = std.math.maxInt(usize),
    conditions: []const Condition = &.{},
    order: []const Scan.Order = &.{},
    /// The caller accepts any source prefix (SQL has no ORDER BY).
    unordered_prefix: bool = false,
    /// Proven upper bound on the consumed prefix, including OFFSET.
    /// Absent when SQL still needs residual filtering or an unbounded result.
    maximum_groups: ?usize = null,
    batch_rows: u32 = 4096,
};
pub const FinalGroupRequest = struct {
    page_limit: usize,
    row_limit: usize = std.math.maxInt(usize),
    selection: GroupSelection = .{},
    /// Input batch size is independent of the requested result fragment.
    batch_rows: u32 = 4096,
    /// Complete, exactly typed grouping key for native hash/range pruning.
    point: ?[]const @import("scalar.zig").Datum = null,
    predicate: ?struct {
        ptr: *anyopaque,
        matches: *const fn (*anyopaque, std.mem.Allocator, []const @import("scalar.zig").Datum) anyerror!bool,
    } = null,
};

pub const AggregatePartialCursor = struct {
    /// Providers may prove a unique complete cohort before the first pull.
    /// Slots refer to the requested recipe, independent of physical order.
    final_group_proof: ?struct { count: u64, slots: []const u16 } = null,
    select_keys: ?*const fn (*anyopaque, GroupSelection) anyerror!bool = null,
    select_point: ?*const fn (*anyopaque, []const @import("scalar.zig").Datum) anyerror!void = null,
    /// Transfer the cursor's ownership into an empty result operator. False
    /// preserves the ordinary partial path; a successful transfer is streamed.
    adopt_final_groups: ?*const fn (*anyopaque, *@import("operators.zig").Grouped, FinalGroupRequest) anyerror!bool = null,
    /// Optional ownership transfer before the first pull. The provider proves
    /// one state per final recipe key, with the exact recipe slots and limits.
    /// A successful transfer leaves close responsible only for cursor resources.
    /// Upper key predicates and FILTER adjustments must be absent for adoption.
    take_final_groups: ?*const fn (*anyopaque) anyerror!?*@import("operators.zig").Grouped = null,
    ptr: *anyopaque,
    next: *const fn (*anyopaque, std.mem.Allocator, u32) anyerror!?[]const @import("operators.zig").GroupResult,
    close: *const fn (*anyopaque) void,
};

pub const Clock = @import("statement_clock.zig");
pub const ViewExposure = struct { ordinal: usize, name: []const u8 };
/// Statement-owned authorization predicate over complete typed view outputs.
/// Providers reuse their existing filter compiler; SQL never invents a second
/// public policy grammar or serializes a row to invoke it.
pub const ViewRowFilter = struct {
    ptr: *const anyopaque,
    matches: *const fn (*const anyopaque, std.mem.Allocator, []const @import("scalar.zig").Datum) anyerror!bool,
};
pub const ReadRelation = union(enum) {
    table: Table,
    view: struct {
        id: u64,
        generation: u32,
        query: *const ast.Select,
        columns: []const Column,
        exposure: ?[]const ViewExposure = null,
        backend: Backend,
        security_barrier: bool,
        row_filter: ?ViewRowFilter = null,
    },
};

/// Statement-owned native row normalizer. The owner pins one schema and policy
/// cut while preparing rows, and releases it before writer admission. Stateful
/// defaults can be evaluated row by row without repeated remote opens.
pub const MutationPreparation = struct {
    ptr: *anyopaque,
    prepare: *const fn (*anyopaque, std.mem.Allocator, []const Mutation) anyerror![]const Mutation,
    finish: *const fn (*anyopaque, std.mem.Allocator) anyerror!void,
    close: *const fn (*anyopaque) void,
};

pub const Backend = struct {
    pub const PreparedRead = struct { name: ast.Name, relation: ReadRelation };
    /// Private resolver namespace. Distinct delegated view contexts must not
    /// share name bindings even when their SQL references have the same text.
    binding_scope: usize = 0,
    prepared_read: ?*const PreparedRead = null,
    /// Trusted execution source and authorized durable transaction start.
    /// Neither is populated from SQL parameters, request JSON or cached plans.
    clock_source: ?Clock.Source = null,
    transaction_start: ?Clock.Instant = null,
    /// Capability for planning is distinct from an execution-owned stamp.
    /// A prepared plan must never retain another transaction's write version.
    supports_write_versions: bool = false,
    write_version: ?u64 = null,
    /// Value-owned by this statement/cursor. Child adapters copy it unchanged.
    statement_clock: ?Clock.View = null,
    /// Immutable typed precision slots borrowed from the result/cursor owner.
    /// A child adapter preserves this address; it must never cache it in plans.
    clock_values: ?*const Clock.Frame = null,
    error_context: ?*@import("errors.zig").Context = null,
    /// Execution-local only: never store this owner in an immutable plan.
    regex_execution: ?*@import("regex_execution.zig") = null,
    /// Native scalar callbacks cannot suspend/yield or perform provider I/O.
    /// Carry an explicit context so lowering can change Backend.ptr safely.
    scalar_control: ?struct { ptr: ?*anyopaque, checkpoint: *const fn (?*anyopaque) anyerror!void } = null,
    supports_search_relations: bool = false,
    /// Advisory expression probes with mandatory SQL residual evaluation.
    /// This is an execution optimization, never a readiness/capability proof.
    supports_expression_access_hints: bool = false,
    execution_io: ?std.Io = null,
    spill_manager: ?*@import("spill.zig").Manager = null,
    sequence_resolver: ?@import("sequence_effects.zig").Resolver = null,
    sequence_provider: ?@import("sequence_effects.zig").Provider = null,
    decision_provider: ?@import("../functions/decisions.zig").DecisionProvider = null,
    ptr: *anyopaque,
    vtable: *const VTable,
    /// Runtime captures a fresh owner-authorized view for each statement.
    /// Overlay entries carry durable setting identities, never raw names.
    setting_capture: ?struct {
        owner: @import("setting_catalog.zig").Owner,
        scope: @import("setting_catalog.zig").Scope,
        sql_user: ?[]const u8 = null,
        overlay: []const @import("setting_catalog.zig").OverlayEntry = &.{},
    } = null,
    /// Authorized, immutable settings captured for this exact statement.
    /// Providers must never populate this from pgwire-local string settings.
    settings_view: ?*const @import("setting_catalog.zig").View = null,
    /// Request-owned transport types fill only unconstrained parameters after
    /// SQL inference. They never replace explicit or schema-derived types.
    parameter_fallback_types: []const ?ast.ColumnType = &.{},
    /// Authoritative Parse-time descriptors, including primitive widths and
    /// array element identity. Unlike fallbacks these constrain SQL binding.
    parameter_descriptor_hints: []const @import("scalar.zig").Type = &.{},
    /// Shared precise parameter constraints and execution frame for this
    /// statement, including its lowered/derived/mutation child plans.
    parameter_invocation: ?*@import("parameter_binding.zig").Invocation = null,
    /// Only set when every page belongs to the same retained statement read
    /// view. Catalog revisions and per-page read_index are not such a view.
    pinned_statement_snapshot: bool = false,
    /// Validate execution policy against the complete binding before choosing
    /// a cursor or fallback. Child plans inherit the already validated policy.
    execution_binding_validator: ?struct {
        ptr: *anyopaque,
        validate: *const fn (*anyopaque, std.mem.Allocator, @import("describe.zig").BoundStatement) anyerror!void,
    } = null,
    /// mutate retains predicate-only entries in the same atomic commit.
    predicate_only_mutations: bool = false,
    /// Statement capture range proofs are retained with the subsequent
    /// mutation in one durable transaction. MERGE absence decisions may not
    /// execute through a plain autocommit batch endpoint.
    atomic_statement_read_set: bool = false,
    /// Multiple bounded statement captures share one transaction read set;
    /// each point absence proof is validated with the eventual mutation.
    coordinated_point_reads: bool = false,
    /// Exact secondary-index probes also share that read set, and cannot be
    /// selected while a staged-session overlay requires primary ordering.
    coordinated_index_reads: bool = false,
    /// Every later scan can fork the same owner-issued cut and retain its
    /// route/range proof in the mutation's atomic commit read set.
    dynamic_statement_read_set: bool = false,

    /// Capture once at execution admission. Describe never calls this method.
    /// Ingress pins before selecting a cursor or materializing fallback.
    pub fn pinStatementClock(self: Backend) !Backend {
        var pinned = self;
        if (self.statement_clock) |view| {
            try view.validate();
            if (self.clock_values) |frame| if (!frame.matches(view)) return error.InvalidTransactionClock;
            if (self.transaction_start) |start| {
                if (start.micros != view.transaction.micros) return error.InvalidTransactionClock;
            }
            return pinned;
        }
        if (self.clock_values != null) return error.InvalidTransactionClock;
        const source = self.clock_source orelse if (self.execution_io) |io| Clock.Source{ .io = io } else {
            if (self.transaction_start != null) return error.SqlClockUnavailable;
            return pinned;
        };
        pinned.statement_clock = try Clock.View.capture(source, self.transaction_start);
        return pinned;
    }

    pub const VTable = struct {
        resolve_read: ?*const fn (*anyopaque, std.mem.Allocator, ast.Name) anyerror!ReadRelation = null,
        /// Native opaque identity, generated once before mutation admission.
        /// Providers without this capability require an explicit _id.
        generate_row_id: ?*const fn (*anyopaque, std.mem.Allocator) anyerror![]const u8 = null,
        resolve_conflict_owners: ?*const fn (*anyopaque, std.mem.Allocator, std.mem.Allocator, Table, ConflictTarget, []const Mutation) anyerror![]const ConflictOwner = null,
        // All returned data belongs to the supplied allocator. Scans receive
        // a short-lived page arena, not the retained statement result arena.
        resolve: *const fn (*anyopaque, std.mem.Allocator, ast.Name, Action) anyerror!Table,
        scan: *const fn (*anyopaque, std.mem.Allocator, Table, Scan) anyerror!Page,
        /// null means this provider cannot retain a statement snapshot. Never
        /// substitute a collection of independently refreshed shard pages.
        /// Allows order negotiation before rows are pulled. Ordinary backends
        /// need not open speculative scans merely to decline an ordering.
        supports_scan_order: bool = false,
        open_scan: ?*const fn (*anyopaque, std.mem.Allocator, Table, Scan) anyerror!?Cursor = null,
        /// Fresh authority/source/coverage proofs belong to the provider. The
        /// SQL engine binds a strict recipe and retains projection, HAVING,
        /// ordering, limits and exact native reducer semantics.
        aggregate_partials: ?*const fn (*anyopaque, std.mem.Allocator, Table, @import("aggregate_materialization.zig").Recipe) anyerror!?AggregatePartialCursor = null,
        open_statement: ?*const fn (*anyopaque, std.mem.Allocator, []const StatementScan) anyerror!StatementRead = null,
        /// Metadata-only planning port: never open/pull cursors, execute search,
        /// activate tracking, publish state or sample rows to obtain estimates.
        /// Return one entry per input; absent metadata stays null.
        estimate_scans: ?*const fn (*anyopaque, std.mem.Allocator, []const StatementScan) anyerror![]const ScanEstimate = null,
        // The mutation allocator is a call-scoped arena. Providers must copy
        // any data retained after return into their own durable/session owner.
        // Exactly one atomic commit, retaining all schema/row-version fences.
        // An ambiguous outcome is propagated, never replayed by SQL.
        /// First allocator owns commit-scoped wire buffers; the second is a
        /// reclaiming, budgeted backing allocator for nested temporary regions.
        /// Borrowed scratch must never escape the mutation call.
        mutate: *const fn (*anyopaque, std.mem.Allocator, std.mem.Allocator, Table, []const Mutation) anyerror!MutationOutcome,
        /// Commit the exact images returned by prepare_mutations without
        /// applying defaults/generated values a second time. Required when
        /// SQL exposes a prepared postimage through RETURNING.
        mutate_prepared: ?*const fn (*anyopaque, std.mem.Allocator, std.mem.Allocator, Table, []const Mutation) anyerror!MutationOutcome = null,
        /// Native deterministic defaults/generated/check preparation under the
        /// bound schema epoch. No writes occur. Mutation consumes these exact
        /// normalized values; SQL must never guess postimages or read them back
        /// after commit. Returned rows preserve key/version/order/deletion.
        open_mutation_preparation: ?*const fn (*anyopaque, std.mem.Allocator, Table, []const u8, bool) anyerror!MutationPreparation = null,
        prepare_mutations: ?*const fn (*anyopaque, std.mem.Allocator, Table, []const Mutation) anyerror![]const Mutation = null,
        checkpoint: *const fn (*anyopaque) anyerror!void,
        /// Native authority owns atomic existence checks and durable catalog
        /// publication. SQL must never emulate DDL with read-then-write.
        ddl: ?*const fn (*anyopaque, DdlAllocators, Ddl) anyerror!DdlOutcome = null,
    };

    pub fn resolveRead(self: Backend, a: std.mem.Allocator, name: ast.Name) !ReadRelation {
        if (self.prepared_read) |prepared| {
            if (std.mem.eql(u8, prepared.name.table, name.table) and
                std.mem.eql(u8, prepared.name.namespace orelse "", name.namespace orelse "") and
                std.mem.eql(u8, prepared.name.database orelse "", name.database orelse ""))
                return prepared.relation;
        }
        if (self.vtable.resolve_read) |resolve| return resolve(self.ptr, a, name);
        return .{ .table = try self.vtable.resolve(self.ptr, a, name, .read) };
    }
};

test "SQL row cells distinguish absent SQL NULL and JSON null and reject invalid flags" {
    const alloc = std.testing.allocator;
    var object: std.json.ObjectMap = .empty;
    defer object.deinit(alloc);
    try object.put(alloc, "j", .null);
    try object.put(alloc, "n", .null);
    try object.put(alloc, "i", .{ .integer = 7 });
    var row: Row = .{ .id = "identity", .version = 1, .value = .{ .object = object }, .sql_nulls = &.{ false, true, false } };
    try std.testing.expect(!(try row.cell("j")).sql_null);
    try std.testing.expect((try row.cell("n")).sql_null);
    try std.testing.expect((try row.cell("missing")).sql_null);
    try std.testing.expectEqualStrings("identity", (try row.cell("_id")).value.string);
    row.sql_nulls = &.{true};
    try std.testing.expectError(error.InvalidSqlBackendResponse, row.cell("j"));
    row.sql_nulls = &.{ false, true, true };
    try std.testing.expectError(error.InvalidSqlBackendResponse, row.cell("i"));
}

test "SQL typed rows own arrays without conflating JSON null or array null elements" {
    const scalar = @import("scalar.zig");
    const arrays = @import("array_value.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var dimensions = [_]arrays.Dimension{.{ .length = 2, .lower = -3 }};
    var elements = [_]scalar.Datum{ scalar.Datum.json(.{ .integer = 9007199254740993 }), .{} };
    var array = try arrays.Value.init(.int64, &dimensions, &elements, .{});
    const layout = try Row.TypedLayout.init(a, &.{ "array", "json", "null" });
    var row = try Row.fromDatums(a, "row", layout, &.{ scalar.Datum.typedArray(&array), scalar.Datum.json(.null), .{} });
    dimensions[0].lower = 1;
    elements[0].value = .{ .integer = 0 };
    const cell = try row.cell("array");
    try std.testing.expect(!cell.sql_null);
    try std.testing.expectEqual(@as(i32, -3), cell.array.?.dimensions[0].lower);
    try std.testing.expectEqual(@as(i64, 9007199254740993), cell.array.?.flatCell(0).value.integer);
    try std.testing.expect(cell.array.?.flatCell(1).sql_null);
    try std.testing.expect(!(try row.cell("json")).sql_null);
    try std.testing.expect((try row.cell("null")).sql_null);
    try std.testing.expect((try row.cell("missing")).sql_null);
    try std.testing.expectError(error.InvalidSqlBackendResponse, Row.TypedLayout.init(a, &.{ "x", "x" }));
    try std.testing.expectError(error.InvalidSqlBackendResponse, Row.fromDatums(a, "", layout, &.{}));
    row.sql_nulls = &.{ false, false, true };
    try std.testing.expectError(error.InvalidSqlBackendResponse, row.cell("array"));
    row.sql_nulls = null;
    row.typed_cells.?.values = &.{};
    try std.testing.expectError(error.InvalidSqlBackendResponse, row.cell("array"));
}

test "SQL native column pages preserve selection JSON null and validate schema width" {
    const Datum = @import("scalar.zig").Datum;
    const values = [_]Datum{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.null) };
    const vectors = [_][]const Datum{&values};
    const batch: @import("execution_batch.zig").Batch = .{ .vectors = .{ .values = &vectors, .count = 3 } };
    var page: ColumnPage = .{ .native = .{ .values = &batch, .names = &.{"literal.name"} }, .selection = &.{ 2, 0, 1 } };
    try page.validate();
    const json_null = try page.cell(std.testing.allocator, 0, "literal.name");
    try std.testing.expect(!json_null.sql_null and json_null.value == .null);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try page.cell(std.testing.allocator, 1, "literal.name")).value.integer);
    try std.testing.expect((try page.cell(std.testing.allocator, 2, "literal.name")).sql_null);
    page.selection = &.{3};
    try std.testing.expectError(error.InvalidSqlBackendResponse, page.validate());
    page.selection = &.{0};
    page.native.?.names = &.{};
    try std.testing.expectError(error.InvalidSqlBackendResponse, page.validate());
}

test "SQL typed array column adapters preserve dimensions and owned index continuation" {
    const scalar = @import("scalar.zig");
    const arrays = @import("array_value.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const dimensions = [_]arrays.Dimension{.{ .length = 2, .lower = -3 }};
    const elements = [_]scalar.Datum{ scalar.Datum.json(.{ .integer = 9007199254740993 }), .{} };
    var array = try arrays.Value.init(.int64, &dimensions, &elements, .{});
    const cells = [_]scalar.Datum{scalar.Datum.typedArray(&array)};
    const rows = [_][]const scalar.Datum{&cells};
    const source: @import("execution_batch.zig").Batch = .{ .rows = &rows };
    const page: ColumnPage = .{ .native = .{ .values = &source, .names = &.{"a"} }, .selection = &.{0} };
    const definitions = [_]scalar.Column{.{ .name = "a", .type = .array, .element_type = .int64 }};
    const adapter: @import("execution_batch.zig").Batch = .{ .columns = .{ .page = page, .definitions = &definitions } };
    const cell = try adapter.cell(a, 0, 0);
    try std.testing.expectEqual(@as(i32, -3), cell.array.?.dimensions[0].lower);
    try std.testing.expectEqual(@as(i64, 9007199254740993), cell.array.?.flatCell(0).value.integer);
    try std.testing.expect(cell.array.?.flatCell(1).sql_null);
    var cursor = [_]u8{ 'n', 'e', 'x', 't' };
    var row = try Row.fromDatums(a, "row", try Row.TypedLayout.init(a, &.{"a"}), &cells);
    row.index_cursor = &cursor;
    const owned = try row.cloneOwned(a);
    @memset(&cursor, 'x');
    try std.testing.expectEqualStrings("next", owned.index_cursor.?);
    try std.testing.expectEqual(@as(i32, -3), (try owned.cell("a")).array.?.dimensions[0].lower);
}

test "lake SQL decimal columns distinguish declared NUMERIC from exact string reads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const rowsource = @import("../storage/rowsource/types.zig");
    const page: ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{.{ .relational_key = "a" }}, .columns = &.{rowsource.ColumnVector{ .name = "n", .values = .{ .decimal128 = .{ .values = &.{9007199254740993125}, .precision = 22, .scale = 3 } } }} }, .selection = &.{0} };
    const raw = try page.cell(a, 0, "n");
    try std.testing.expectEqualStrings("9007199254740993.125", raw.value.string);
    const native = try page.typedCell(a, 0, "n", true);
    try std.testing.expect(native.numeric != null);
    var context: @import("numeric_value.zig").Context = .{ .alloc = a };
    try std.testing.expectEqualStrings(raw.value.string, try @import("numeric_value.zig").format(&context, native.numeric.?.*));
}

test "SQL durable planner sketches merge duplicate keys nulls widths and bounded heavy frequencies" {
    const a = std.testing.allocator;
    var full: StatisticsSketch = .{};
    var left: StatisticsSketch = .{};
    var right: StatisticsSketch = .{};
    for (0..20000) |i| {
        const value = @import("scalar.zig").Datum.fromJson(if (i % 5 == 0) .null else .{ .integer = if (i < 10000) 7 else @intCast(i) });
        try full.add(value, 8);
        if (i < 10000) try left.add(value, 8) else try right.add(value, 8);
    }
    left.merge(right);
    try std.testing.expectEqualDeep(full.registers, left.registers);
    try std.testing.expectEqual(full.rows, left.rows);
    const roundtrip = try StatisticsSketch.decode(&left.encode());
    try std.testing.expectEqualDeep(left, roundtrip);
    const estimate = try left.estimate(a, "key");
    defer a.free(estimate.frequencies);
    try std.testing.expectEqual(@as(?u64, 20000), estimate.population_rows);
    try std.testing.expectEqual(@as(?u64, 16000), estimate.non_null_rows);
    try std.testing.expect(estimate.distinct_count.? > 5000 and estimate.distinct_count.? < 12000);
    try std.testing.expectEqual(@as(?u32, 7), estimate.average_bytes);
    try std.testing.expectEqual(@as(?i64, 7), estimate.min_integer);
    try std.testing.expectEqual(@as(?i64, 19999), estimate.max_integer);
    for (estimate.frequencies) |frequency| try std.testing.expect(frequency.rows <= 8000);
    var malformed = left.encode();
    malformed[48] = 60;
    try std.testing.expectError(error.InvalidSqlBackendResponse, StatisticsSketch.decode(&malformed));
}

test "SQL view temporal domain signatures distinguish every scalar kind and normalize timestamptz" {
    const a = std.testing.allocator;
    const old: Column = .{ .name = "v", .path = "v", .type = .datetime };
    var historical = std.crypto.hash.sha2.Sha256.init(.{});
    historical.update("antfly:sql-column-domain:v1\x00");
    historical.update("{\"kind\":\"datetime\",\"identity\":null,\"numeric_modifier\":null}");
    const frozen = historical.finalResult();
    try std.testing.expectEqualSlices(u8, &frozen, &try old.identitySignature(a));
    var explicit = old;
    explicit.temporal_kind = .timestamp_tz;
    try std.testing.expectEqualSlices(u8, &frozen, &try explicit.identitySignature(a));
    const kinds = [_]@import("temporal_value.zig").Kind{ .date, .time, .time_tz, .timestamp, .timestamp_tz, .interval };
    for (kinds) |left| for (kinds) |right| {
        var one = old;
        one.temporal_kind = left;
        var two = old;
        two.temporal_kind = right;
        try std.testing.expectEqual(left == right, std.mem.eql(u8, &try one.identitySignature(a), &try two.identitySignature(a)));
    };
    explicit.type = .string;
    try std.testing.expectError(error.InvalidSqlBackendResponse, explicit.identitySignature(a));
}

test "SQL system column updates enforce envelope ownership while insert identities remain readable" {
    const table: Table = .{ .id = 7, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = &.{.{ .name = "title", .path = "title", .type = .string }} };
    try std.testing.expectError(error.SqlSystemColumnWrite, table.updateTarget("_id"));
    try std.testing.expectError(error.SqlSystemColumnWrite, table.updateTarget("_version"));
    try std.testing.expectEqual(ast.ColumnType.string, (try table.updateTarget("title")).type);
    try std.testing.expectEqual(ast.ColumnType.json, (try table.updateTarget("_doc")).type);
    try std.testing.expectEqual(ast.ColumnType.string, (try table.column("_id")).type);
    const diagnostic = @import("errors.zig").describe(error.SqlSystemColumnWrite);
    try std.testing.expectEqualStrings("428C9", diagnostic.code);
}

test "SQL view domain signatures allocate no heap across wide dependency sets" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const columns = [_]Column{
        .{ .name = "i", .path = "i", .type = .integer },
        .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric, .numeric_modifier = .{ .precision = 38, .scale = 12 } },
        .{ .name = "a", .path = "a", .type = .array, .element_type = .text },
        .{ .name = "d", .path = "d", .type = .datetime, .temporal_kind = .date },
    };
    for (0..512) |_| for (columns) |column| {
        _ = try column.identitySignature(failing.allocator());
    };
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}
