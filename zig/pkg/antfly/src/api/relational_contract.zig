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

//! Public relational vocabulary is generated from OpenAPI. The native storage
//! types keep explicit format tags and remain independent of HTTP/SDK code.
//! All conversions use semantic names, never generated enum ordinals.

const std = @import("std");
const wire = @import("antfly_schema_openapi");
const native = @import("../storage/relational_index.zig");
const storage = @import("../storage/schema.zig");

pub const StorageMode = EnumBridge(wire.TableStorageMode, storage.StorageMode);
pub const IndexAccessMethod = EnumBridge(wire.RelationalIndexAccessMethod, native.RelationalIndexAccessMethod);
pub const IndexLifecycle = EnumBridge(wire.RelationalIndexLifecycle, native.RelationalIndexLifecycle);
pub const IndexKeyDirection = EnumBridge(wire.RelationalIndexKeyDirection, native.RelationalIndexKeyDirection);
pub const IndexKeyNulls = EnumBridge(wire.RelationalIndexKeyNulls, native.RelationalIndexKeyNulls);
pub const IndexOwnerKind = EnumBridge(wire.RelationalIndexOwnerKind, native.RelationalIndexOwnerKind);
pub const ForeignKeyAction = EnumBridge(wire.ForeignKeyAction, native.ForeignKeyAction);
pub const ForeignKeyTiming = EnumBridge(wire.ForeignKeyTiming, native.ForeignKeyTiming);
pub const ForeignKeyMatch = EnumBridge(wire.ForeignKeyMatch, native.ForeignKeyMatch);
pub const ForeignKeyValidation = EnumBridge(wire.RelationalConstraintValidationState, native.ForeignKeyValidationState);
pub const UniqueConstraintValidation = EnumBridge(wire.RelationalConstraintValidationState, native.UniqueConstraintValidationState);
pub const CheckValidation = EnumBridge(wire.RelationalConstraintValidationState, native.RelationalCheckValidationState);
pub const ComparisonOp = EnumBridge(wire.RelationalComparisonOp, native.RelationalCheckOp);

pub const IndexKey = struct {
    /// Strings remain borrowed from the request; expression bytes belong to
    /// the supplied request arena. Schema binding validates expression types.
    pub fn toNativeArena(arena: std.mem.Allocator, value: wire.RelationalIndexKey) !native.RelationalIndexKey {
        const column = value.column orelse "";
        if ((column.len != 0) == (value.expression != null) or
            (value.column != null and column.len == 0) or
            ((value.expression != null) != (value.result_type != null))) return error.InvalidRelationalIndexDefinition;
        if (value.collation) |collation| {
            if (collation.len == 0) return error.InvalidRelationalIndexDefinition;
        }
        return .{
            .column = column,
            .expression_json = if (value.expression) |expression| try std.json.Stringify.valueAlloc(arena, expression, .{ .emit_null_optional_fields = false }) else null,
            .result_type = if (value.result_type) |kind| switch (kind) {
                inline else => |tag| @field(storage.RelationalColumnType, @tagName(tag)),
            } else null,
            .collation = value.collation,
            .direction = IndexKeyDirection.toNative(value.direction orelse .asc),
            .nulls = IndexKeyNulls.toNative(value.nulls orelse .default),
        };
    }

    /// The response borrows native strings and arena-owned expression nodes.
    pub fn toWireArena(arena: std.mem.Allocator, value: native.RelationalIndexKey) !wire.RelationalIndexKey {
        return .{
            .column = if (value.column.len != 0) value.column else null,
            .expression = if (value.expression_json) |json| try std.json.parseFromSliceLeaky(wire.RelationalScalarExpression, arena, json, .{}) else null,
            .result_type = if (value.result_type) |kind| std.meta.stringToEnum(wire.RelationalExpressionType, @tagName(kind)) orelse return error.InvalidRelationalIndexDefinition else null,
            .collation = value.collation,
            .direction = IndexKeyDirection.toWire(value.direction),
            .nulls = IndexKeyNulls.toWire(value.nulls),
        };
    }
};

/// A vocabulary change must be handled at this boundary instead of silently
/// mapping an unknown value to a permissive default. Reordering wire enums
/// is harmless; adding/removing/renaming a value fails compilation.
fn EnumBridge(comptime Wire: type, comptime Native: type) type {
    const wire_fields = @typeInfo(Wire).@"enum".fields;
    const native_fields = @typeInfo(Native).@"enum".fields;
    if (wire_fields.len != native_fields.len)
        @compileError("relational wire/native enum vocabularies differ: " ++ @typeName(Wire));
    for (wire_fields) |field| {
        if (!@hasField(Native, field.name))
            @compileError("relational native enum is missing wire value: " ++ field.name);
    }
    return struct {
        pub const WireType = Wire;
        pub const NativeType = Native;

        pub fn toNative(value: Wire) Native {
            return switch (value) {
                inline else => |tag| @field(Native, @tagName(tag)),
            };
        }

        pub fn toWire(value: Native) Wire {
            return switch (value) {
                inline else => |tag| @field(Wire, @tagName(tag)),
            };
        }
    };
}

test "relational contract generated enums round trip through durable vocabulary" {
    inline for (.{
        StorageMode,
        IndexAccessMethod,
        IndexLifecycle,
        IndexKeyDirection,
        IndexKeyNulls,
        IndexOwnerKind,
        ForeignKeyAction,
        ForeignKeyTiming,
        ForeignKeyMatch,
        ForeignKeyValidation,
        UniqueConstraintValidation,
        CheckValidation,
        ComparisonOp,
    }) |Bridge| {
        inline for (std.meta.tags(Bridge.WireType)) |tag| {
            const native_tag = Bridge.toNative(tag);
            try std.testing.expectEqualStrings(@tagName(tag), @tagName(native_tag));
            try std.testing.expectEqual(tag, Bridge.toWire(native_tag));
            const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, tag, .{});
            defer std.testing.allocator.free(encoded);
            var parsed = try std.json.parseFromSlice(Bridge.WireType, std.testing.allocator, encoded, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(tag, parsed.value);
        }
        try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(
            Bridge.WireType,
            std.testing.allocator,
            "\"unknown_future_value\"",
            .{},
        ));
    }
}

test "relational contract conversion is independent of generated ordinal order" {
    const Wire = enum { relational, document };
    const Bridge = EnumBridge(Wire, storage.StorageMode);
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(Bridge.toNative(.document)));
    try std.testing.expectEqual(@as(u8, 1), @intFromEnum(Bridge.toNative(.relational)));
    try std.testing.expectEqual(Wire.document, Bridge.toWire(.document));
    try std.testing.expectEqual(Wire.relational, Bridge.toWire(.relational));
}

test "relational contract generated index key preserves defaults and typed options" {
    const alloc = std.testing.allocator;
    var minimal = try std.json.parseFromSlice(wire.RelationalIndexKey, alloc, "{\"column\":\"id\"}", .{});
    defer minimal.deinit();
    const default_key = try IndexKey.toNativeArena(alloc, minimal.value);
    try std.testing.expectEqual(native.RelationalIndexKeyDirection.asc, default_key.direction);
    try std.testing.expectEqual(native.RelationalIndexKeyNulls.default, default_key.nulls);
    try std.testing.expectEqual(@as(?[]const u8, null), default_key.collation);

    inline for (std.meta.tags(wire.RelationalIndexKeyDirection)) |direction| {
        inline for (std.meta.tags(wire.RelationalIndexKeyNulls)) |nulls| {
            const original = wire.RelationalIndexKey{
                .column = "title",
                .collation = "ci",
                .direction = direction,
                .nulls = nulls,
            };
            const encoded = try std.json.Stringify.valueAlloc(alloc, original, .{});
            defer alloc.free(encoded);
            var parsed = try std.json.parseFromSlice(wire.RelationalIndexKey, alloc, encoded, .{});
            defer parsed.deinit();
            const restored = try IndexKey.toWireArena(alloc, try IndexKey.toNativeArena(alloc, parsed.value));
            try std.testing.expectEqualStrings(original.column.?, restored.column.?);
            try std.testing.expectEqualStrings(original.collation.?, restored.collation.?);
            try std.testing.expectEqual(direction, restored.direction.?);
            try std.testing.expectEqual(nulls, restored.nulls.?);
        }
    }
}

test "relational contract generated index key rejects invalid definitions" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidRelationalIndexDefinition, IndexKey.toNativeArena(alloc, .{ .column = "" }));
    try std.testing.expectError(error.InvalidRelationalIndexDefinition, IndexKey.toNativeArena(alloc, .{ .column = "id", .collation = "" }));
    try std.testing.expectError(error.InvalidRelationalIndexDefinition, IndexKey.toNativeArena(alloc, .{}));
    try std.testing.expectError(error.InvalidRelationalIndexDefinition, IndexKey.toNativeArena(alloc, .{ .column = "id", .result_type = .integer }));
    try std.testing.expectError(error.InvalidRelationalIndexDefinition, IndexKey.toNativeArena(alloc, .{ .expression = .{ .op = .column, .column = "id" } }));
    try std.testing.expectError(error.InvalidRelationalIndexDefinition, IndexKey.toNativeArena(alloc, .{ .column = "id", .expression = .{ .op = .column, .column = "id" }, .result_type = .integer }));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(wire.RelationalIndexKey, alloc, "{\"column\":\"id\",\"direction\":\"sideways\"}", .{}));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(wire.RelationalIndexKey, alloc, "{\"column\":\"id\",\"nulls\":\"middle\"}", .{}));
    try std.testing.expectError(error.UnknownField, std.json.parseFromSlice(wire.RelationalIndexKey, alloc, "{\"column\":\"id\",\"direciton\":\"desc\"}", .{}));
}

test "relational contract expression index key survives arena conversion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const original: wire.RelationalIndexKey = .{
        .expression = .{ .op = .column, .column = "id" },
        .result_type = .integer,
        .direction = .desc,
        .nulls = .last,
    };
    const stored = try IndexKey.toNativeArena(alloc, original);
    try std.testing.expectEqualStrings("", stored.column);
    try std.testing.expectEqual(storage.RelationalColumnType.integer, stored.result_type.?);
    const restored = try IndexKey.toWireArena(alloc, stored);
    try std.testing.expectEqualDeep(original, restored);
}
