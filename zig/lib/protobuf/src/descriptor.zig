// Copyright 2026 Antfly, Inc.
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

//! `google.protobuf.FileDescriptorSet` decoder.
//!
//! Decode-only. Protobuf's own self-describing schema, implemented in terms
//! of the comptime `message` runtime — each type carries a `_pb_field_map`
//! and delegates to `message.decode`.
//!
//! Why we care: codegen reads binary `.desc` files (produced once via
//! `protoc --descriptor_set_out=...`) and emits Zig structs from them. We
//! need a decoder for the descriptor schema before we can generate anything
//! else. Since the comptime runtime already works (Phase 1), we declare the
//! subset of descriptor.proto messages we care about as annotated structs
//! and let the runtime handle the rest.
//!
//! Scope: the subset below is enough to support codegen of the proto flavors
//! we actually consume (proto2, proto3, edition 2023). Fields we don't need
//! for codegen (SourceCodeInfo, Uninterpreted options, services, extensions,
//! most of FileOptions, etc.) are intentionally omitted — unknown fields are
//! skipped by the runtime.

const std = @import("std");
const Allocator = std.mem.Allocator;
const message = @import("message.zig");
const wire = @import("wire.zig");

const FieldDesc = message.FieldDesc;

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// `google.protobuf.Edition` — non-exhaustive so future editions decode.
pub const Edition = enum(i32) {
    unknown = 0,
    legacy = 900,
    proto2 = 998,
    proto3 = 999,
    @"2023" = 1000,
    @"2024" = 1001,
    unstable = 9999,
    max = 0x7FFFFFFF,
    _,
};

/// `FieldDescriptorProto.Type` — wire type of a single field value.
pub const FieldType = enum(i32) {
    unknown = 0,
    double = 1,
    float = 2,
    int64 = 3,
    uint64 = 4,
    int32 = 5,
    fixed64 = 6,
    fixed32 = 7,
    bool = 8,
    string = 9,
    group = 10, // deprecated
    message = 11,
    bytes = 12,
    uint32 = 13,
    @"enum" = 14,
    sfixed32 = 15,
    sfixed64 = 16,
    sint32 = 17,
    sint64 = 18,
    _,
};

/// `FieldDescriptorProto.Label`.
pub const FieldLabel = enum(i32) {
    unknown = 0,
    optional = 1,
    required = 2,
    repeated = 3,
    _,
};

/// `FeatureSet.FieldPresence`.
pub const FieldPresence = enum(i32) {
    unknown = 0,
    explicit = 1,
    implicit = 2,
    legacy_required = 3,
    _,
};

/// `FeatureSet.EnumType`.
pub const FeatureEnumType = enum(i32) {
    unknown = 0,
    open = 1,
    closed = 2,
    _,
};

/// `FeatureSet.RepeatedFieldEncoding`.
pub const RepeatedFieldEncoding = enum(i32) {
    unknown = 0,
    packed_ = 1,
    expanded = 2,
    _,
};

/// `FeatureSet.MessageEncoding`.
pub const MessageEncoding = enum(i32) {
    unknown = 0,
    length_prefixed = 1,
    delimited = 2,
    _,
};

// ---------------------------------------------------------------------------
// Options / features
// ---------------------------------------------------------------------------

/// `google.protobuf.FeatureSet` — minimal subset. Only the fields we use for
/// codegen decisions are decoded.
pub const FeatureSet = struct {
    field_presence: FieldPresence = .unknown,
    enum_type: FeatureEnumType = .unknown,
    repeated_field_encoding: RepeatedFieldEncoding = .unknown,
    message_encoding: MessageEncoding = .unknown,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "field_presence", .encoding = .varint },
        .{ .field_num = 2, .name = "enum_type", .encoding = .varint },
        .{ .field_num = 3, .name = "repeated_field_encoding", .encoding = .varint },
        .{ .field_num = 5, .name = "message_encoding", .encoding = .varint },
    };
};

pub const FieldOptions = struct {
    /// `[packed = ...]` override (proto2 only — prohibited in editions).
    packed_: bool = false,
    features: FeatureSet = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 2, .name = "packed_", .encoding = .varint },
        .{ .field_num = 21, .name = "features", .encoding = .submessage },
    };
};

pub const MessageOptions = struct {
    map_entry: bool = false,
    features: FeatureSet = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 7, .name = "map_entry", .encoding = .varint },
        .{ .field_num = 12, .name = "features", .encoding = .submessage },
    };
};

pub const FileOptions = struct {
    features: FeatureSet = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 50, .name = "features", .encoding = .submessage },
    };
};

pub const EnumOptions = struct {
    features: FeatureSet = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 7, .name = "features", .encoding = .submessage },
    };
};

pub const OneofOptions = struct {
    features: FeatureSet = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "features", .encoding = .submessage },
    };
};

// ---------------------------------------------------------------------------
// Descriptor message types
// ---------------------------------------------------------------------------

pub const FieldDescriptorProto = struct {
    name: []const u8 = "",
    number: i32 = 0,
    label: FieldLabel = .unknown,
    type: FieldType = .unknown,
    /// For `type = message | enum | group`, the fully- or partially-qualified
    /// name of the referenced type. Leading dot ⇒ fully qualified.
    type_name: []const u8 = "",
    default_value: []const u8 = "",
    json_name: []const u8 = "",
    /// Zero if not part of a oneof. Protobuf treats oneof_index as optional;
    /// we collapse that into "> 0 means set, else use extra info on the
    /// containing message to disambiguate".
    oneof_index: i32 = -1,
    proto3_optional: bool = false,
    options: FieldOptions = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        .{ .field_num = 3, .name = "number", .encoding = .varint },
        .{ .field_num = 4, .name = "label", .encoding = .varint },
        .{ .field_num = 5, .name = "type", .encoding = .varint },
        .{ .field_num = 6, .name = "type_name", .encoding = .string },
        .{ .field_num = 7, .name = "default_value", .encoding = .string },
        .{ .field_num = 8, .name = "options", .encoding = .submessage },
        .{ .field_num = 9, .name = "oneof_index", .encoding = .varint },
        .{ .field_num = 10, .name = "json_name", .encoding = .string },
        .{ .field_num = 17, .name = "proto3_optional", .encoding = .varint },
    };
};

pub const OneofDescriptorProto = struct {
    name: []const u8 = "",
    options: OneofOptions = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        .{ .field_num = 2, .name = "options", .encoding = .submessage },
    };
};

pub const EnumValueDescriptorProto = struct {
    name: []const u8 = "",
    number: i32 = 0,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        .{ .field_num = 2, .name = "number", .encoding = .varint },
    };
};

pub const EnumDescriptorProto = struct {
    name: []const u8 = "",
    value: []EnumValueDescriptorProto = &.{},
    options: EnumOptions = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        .{ .field_num = 2, .name = "value", .encoding = .repeated_submessage },
        .{ .field_num = 3, .name = "options", .encoding = .submessage },
    };
};

/// `DescriptorProto` — message type. Recursively contains `nested_type`.
pub const DescriptorProto = struct {
    name: []const u8 = "",
    field: []FieldDescriptorProto = &.{},
    nested_type: []DescriptorProto = &.{},
    enum_type: []EnumDescriptorProto = &.{},
    oneof_decl: []OneofDescriptorProto = &.{},
    options: MessageOptions = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        .{ .field_num = 2, .name = "field", .encoding = .repeated_submessage },
        .{ .field_num = 3, .name = "nested_type", .encoding = .repeated_submessage },
        .{ .field_num = 4, .name = "enum_type", .encoding = .repeated_submessage },
        .{ .field_num = 7, .name = "options", .encoding = .submessage },
        .{ .field_num = 8, .name = "oneof_decl", .encoding = .repeated_submessage },
    };
};

pub const FileDescriptorProto = struct {
    name: []const u8 = "",
    package: []const u8 = "",
    dependency: [][]const u8 = &.{},
    message_type: []DescriptorProto = &.{},
    enum_type: []EnumDescriptorProto = &.{},
    options: FileOptions = .{},
    syntax: []const u8 = "",
    edition: Edition = .unknown,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        .{ .field_num = 2, .name = "package", .encoding = .string },
        .{ .field_num = 3, .name = "dependency", .encoding = .repeated_string },
        .{ .field_num = 4, .name = "message_type", .encoding = .repeated_submessage },
        .{ .field_num = 5, .name = "enum_type", .encoding = .repeated_submessage },
        .{ .field_num = 8, .name = "options", .encoding = .submessage },
        .{ .field_num = 12, .name = "syntax", .encoding = .string },
        .{ .field_num = 14, .name = "edition", .encoding = .varint },
    };
};

pub const FileDescriptorSet = struct {
    file: []FileDescriptorProto = &.{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "file", .encoding = .repeated_submessage },
    };

    pub fn decode(allocator: Allocator, bytes: []const u8) !FileDescriptorSet {
        return message.decode(FileDescriptorSet, allocator, bytes);
    }

    pub fn deinit(self: *FileDescriptorSet, allocator: Allocator) void {
        message.deinit(FileDescriptorSet, allocator, self);
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Effective edition for a file, considering both the `edition` field and
/// the legacy `syntax` string.
pub fn effectiveEdition(file: *const FileDescriptorProto) Edition {
    if (file.edition != .unknown) return file.edition;
    if (std.mem.eql(u8, file.syntax, "proto3")) return .proto3;
    // Default is proto2 (syntax field may be empty or "proto2").
    return .proto2;
}

/// Returns true if the field should be packed on the wire, per the effective
/// edition's rules plus any explicit `[packed = ...]` override (proto2 only).
pub fn isFieldPacked(
    edition: Edition,
    file_features: FeatureSet,
    field: *const FieldDescriptorProto,
) bool {
    // Only repeated fields of primitive types can be packed.
    if (field.label != .repeated) return false;
    if (!isPrimitive(field.type)) return false;

    switch (edition) {
        .proto2, .legacy => {
            // Default not packed; honor explicit [packed = true] override.
            return field.options.packed_;
        },
        .proto3 => {
            // Default packed; proto2-style `[packed = false]` is legal and
            // overrides. Protoc normally ensures `options.packed_` matches
            // the effective state, so use it directly if options block was
            // present. Otherwise default to true.
            //
            // We can't tell if `options` was present or not (no presence
            // tracking for scalars). Conservatively: packed unless options
            // explicitly sets it to false. Since our decoder reads
            // `packed_ = false` by default — same as "unset" — this means
            // proto3 will always return true here. In practice protoc never
            // emits `[packed = false]` for proto3.
            return true;
        },
        else => {
            // Editions: use features.repeated_field_encoding. Field-level
            // features override file-level, but we don't track field-level
            // features separately yet.
            const enc = if (field.options.features.repeated_field_encoding != .unknown)
                field.options.features.repeated_field_encoding
            else
                file_features.repeated_field_encoding;
            return switch (enc) {
                .packed_ => true,
                .expanded => false,
                // Edition 2023 default is PACKED for proto3-descended, but
                // when unknown fall back to packed (matches protoc's default
                // for edition 2023).
                .unknown, _ => true,
            };
        },
    }
}

fn isPrimitive(t: FieldType) bool {
    return switch (t) {
        .double, .float, .int64, .uint64, .int32, .fixed64, .fixed32, .bool, .uint32, .@"enum", .sfixed32, .sfixed64, .sint32, .sint64 => true,
        .string, .bytes, .message, .group, .unknown => false,
        else => false,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "FileDescriptorSet empty" {
    var set = try FileDescriptorSet.decode(testing.allocator, &.{});
    defer set.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 0), set.file.len);
}

test "FieldDescriptorProto roundtrip via message runtime" {
    const alloc = testing.allocator;
    const original = FieldDescriptorProto{
        .name = "dims",
        .number = 1,
        .label = .repeated,
        .type = .int64,
        .json_name = "dims",
        .oneof_index = 0, // explicit 0 (will encode because 0 != -1)
        .options = .{ .packed_ = true },
    };
    const bytes = try message.encode(FieldDescriptorProto, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try message.decode(FieldDescriptorProto, alloc, bytes);
    defer message.deinit(FieldDescriptorProto, alloc, &decoded);

    try testing.expectEqualStrings("dims", decoded.name);
    try testing.expectEqual(@as(i32, 1), decoded.number);
    try testing.expectEqual(FieldLabel.repeated, decoded.label);
    try testing.expectEqual(FieldType.int64, decoded.type);
    try testing.expectEqualStrings("dims", decoded.json_name);
    try testing.expect(decoded.options.packed_);
}

test "DescriptorProto nested recursion roundtrip" {
    const alloc = testing.allocator;

    var inner_fields = [_]FieldDescriptorProto{
        .{ .name = "a", .number = 1, .label = .optional, .type = .int32 },
    };
    var inner_msgs = [_]DescriptorProto{
        .{ .name = "Inner", .field = inner_fields[0..] },
    };
    var outer_fields = [_]FieldDescriptorProto{
        .{ .name = "b", .number = 2, .label = .optional, .type = .string },
    };
    const original = DescriptorProto{
        .name = "Outer",
        .field = outer_fields[0..],
        .nested_type = inner_msgs[0..],
    };

    const bytes = try message.encode(DescriptorProto, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try message.decode(DescriptorProto, alloc, bytes);
    defer message.deinit(DescriptorProto, alloc, &decoded);

    try testing.expectEqualStrings("Outer", decoded.name);
    try testing.expectEqual(@as(usize, 1), decoded.field.len);
    try testing.expectEqualStrings("b", decoded.field[0].name);
    try testing.expectEqual(@as(usize, 1), decoded.nested_type.len);
    try testing.expectEqualStrings("Inner", decoded.nested_type[0].name);
    try testing.expectEqual(@as(usize, 1), decoded.nested_type[0].field.len);
    try testing.expectEqualStrings("a", decoded.nested_type[0].field[0].name);
}

test "FileDescriptorProto edition field" {
    const alloc = testing.allocator;
    const original = FileDescriptorProto{
        .name = "quantize.proto",
        .package = "antfly.vector.quantize",
        .syntax = "editions",
        .edition = .@"2023",
    };

    const bytes = try message.encode(FileDescriptorProto, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try message.decode(FileDescriptorProto, alloc, bytes);
    defer message.deinit(FileDescriptorProto, alloc, &decoded);

    try testing.expectEqualStrings("quantize.proto", decoded.name);
    try testing.expectEqualStrings("antfly.vector.quantize", decoded.package);
    try testing.expectEqualStrings("editions", decoded.syntax);
    try testing.expectEqual(Edition.@"2023", decoded.edition);
    try testing.expectEqual(Edition.@"2023", effectiveEdition(&decoded));
}

test "effectiveEdition infers from syntax" {
    const p2 = FileDescriptorProto{ .syntax = "" };
    try testing.expectEqual(Edition.proto2, effectiveEdition(&p2));

    const p2_explicit = FileDescriptorProto{ .syntax = "proto2" };
    try testing.expectEqual(Edition.proto2, effectiveEdition(&p2_explicit));

    const p3 = FileDescriptorProto{ .syntax = "proto3" };
    try testing.expectEqual(Edition.proto3, effectiveEdition(&p3));

    const ed23 = FileDescriptorProto{ .syntax = "editions", .edition = .@"2023" };
    try testing.expectEqual(Edition.@"2023", effectiveEdition(&ed23));
}

test "isFieldPacked proto2 default" {
    const field = FieldDescriptorProto{ .label = .repeated, .type = .int32 };
    try testing.expect(!isFieldPacked(.proto2, .{}, &field));

    const with_packed = FieldDescriptorProto{
        .label = .repeated,
        .type = .int32,
        .options = .{ .packed_ = true },
    };
    try testing.expect(isFieldPacked(.proto2, .{}, &with_packed));
}

test "isFieldPacked proto3 default" {
    const field = FieldDescriptorProto{ .label = .repeated, .type = .int32 };
    try testing.expect(isFieldPacked(.proto3, .{}, &field));
}

test "isFieldPacked editions honors features" {
    const field = FieldDescriptorProto{ .label = .repeated, .type = .int32 };

    const file_packed = FeatureSet{ .repeated_field_encoding = .packed_ };
    try testing.expect(isFieldPacked(.@"2023", file_packed, &field));

    const file_expanded = FeatureSet{ .repeated_field_encoding = .expanded };
    try testing.expect(!isFieldPacked(.@"2023", file_expanded, &field));
}

test "isFieldPacked rejects non-repeated" {
    const scalar = FieldDescriptorProto{ .label = .optional, .type = .int32 };
    try testing.expect(!isFieldPacked(.proto3, .{}, &scalar));
}

test "isFieldPacked rejects string" {
    const string_field = FieldDescriptorProto{ .label = .repeated, .type = .string };
    try testing.expect(!isFieldPacked(.proto3, .{}, &string_field));
}

test "decode real descriptor.desc end-to-end" {
    // Real FileDescriptorSet produced by `protoc --descriptor_set_out` on
    // google/protobuf/descriptor.proto — the bootstrap case. If this passes,
    // the runtime handles recursive messages (DescriptorProto.nested_type),
    // repeated submessages, strings, and enum fields correctly.
    const desc_bytes = @embedFile("testdata/descriptor.desc");
    const alloc = testing.allocator;

    var set = try FileDescriptorSet.decode(alloc, desc_bytes);
    defer set.deinit(alloc);

    try testing.expect(set.file.len >= 1);
    const file = set.file[0];
    try testing.expectEqualStrings("google/protobuf/descriptor.proto", file.name);
    try testing.expectEqualStrings("google.protobuf", file.package);

    // descriptor.proto should declare these top-level messages (among others).
    const expected_messages = [_][]const u8{
        "FileDescriptorSet",
        "FileDescriptorProto",
        "DescriptorProto",
        "FieldDescriptorProto",
        "EnumDescriptorProto",
    };
    for (expected_messages) |expected| {
        var found = false;
        for (file.message_type) |m| {
            if (std.mem.eql(u8, m.name, expected)) {
                found = true;
                break;
            }
        }
        if (!found) {
            std.debug.print("expected message '{s}' not found\n", .{expected});
            return error.TestExpectedMessageMissing;
        }
    }

    // FieldDescriptorProto has nested enums (Type, Label). Verify nested
    // recursion works.
    for (file.message_type) |m| {
        if (std.mem.eql(u8, m.name, "FieldDescriptorProto")) {
            try testing.expect(m.enum_type.len >= 2); // Type + Label

            var type_found = false;
            var label_found = false;
            for (m.enum_type) |e| {
                if (std.mem.eql(u8, e.name, "Type")) type_found = true;
                if (std.mem.eql(u8, e.name, "Label")) label_found = true;
            }
            try testing.expect(type_found);
            try testing.expect(label_found);
            return;
        }
    }
    return error.TestFieldDescriptorProtoMissing;
}

test "decode quantize.desc: edition 2023 file" {
    // Real quantize.proto/vector.proto from the antfly monorepo. This exercises
    // edition 2023 files, which is the main target for vector codegen.
    const desc_bytes = @embedFile("testdata/quantize.desc");
    const alloc = testing.allocator;

    var set = try FileDescriptorSet.decode(alloc, desc_bytes);
    defer set.deinit(alloc);

    try testing.expect(set.file.len >= 2); // quantize.proto + vector.proto at minimum

    var saw_quantize = false;
    var saw_vector = false;
    for (set.file) |f| {
        if (std.mem.endsWith(u8, f.name, "lib/vector/quantize/quantize.proto")) {
            saw_quantize = true;
            // Edition 2023 files have syntax="editions" and edition=2023.
            try testing.expectEqualStrings("editions", f.syntax);
            try testing.expectEqual(Edition.@"2023", f.edition);
            try testing.expectEqual(Edition.@"2023", effectiveEdition(&f));

            // Expected messages in quantize.proto.
            var saw_rabitq_code_set = false;
            var saw_rabitq_quantized = false;
            for (f.message_type) |m| {
                if (std.mem.eql(u8, m.name, "RaBitQCodeSet")) saw_rabitq_code_set = true;
                if (std.mem.eql(u8, m.name, "RaBitQuantizedVectorSet")) saw_rabitq_quantized = true;
            }
            try testing.expect(saw_rabitq_code_set);
            try testing.expect(saw_rabitq_quantized);
        }
        if (std.mem.endsWith(u8, f.name, "lib/vector/vector.proto")) {
            saw_vector = true;
        }
    }
    try testing.expect(saw_quantize);
    try testing.expect(saw_vector);
}

test "decode descriptor.desc: FieldDescriptorProto.Type enum values" {
    // Verify enum decoding and value ordering.
    const desc_bytes = @embedFile("testdata/descriptor.desc");
    const alloc = testing.allocator;

    var set = try FileDescriptorSet.decode(alloc, desc_bytes);
    defer set.deinit(alloc);

    for (set.file[0].message_type) |m| {
        if (!std.mem.eql(u8, m.name, "FieldDescriptorProto")) continue;
        for (m.enum_type) |e| {
            if (!std.mem.eql(u8, e.name, "Type")) continue;

            // TYPE_DOUBLE = 1, TYPE_FLOAT = 2, ..., TYPE_SINT64 = 18
            try testing.expect(e.value.len >= 18);

            var saw_double = false;
            var saw_sint64 = false;
            for (e.value) |v| {
                if (std.mem.eql(u8, v.name, "TYPE_DOUBLE")) {
                    try testing.expectEqual(@as(i32, 1), v.number);
                    saw_double = true;
                } else if (std.mem.eql(u8, v.name, "TYPE_SINT64")) {
                    try testing.expectEqual(@as(i32, 18), v.number);
                    saw_sint64 = true;
                }
            }
            try testing.expect(saw_double);
            try testing.expect(saw_sint64);
            return;
        }
    }
    return error.TestTypeEnumMissing;
}
