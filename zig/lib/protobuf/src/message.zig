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

//! Comptime-driven protobuf encode/decode runtime.
//!
//! Works with any struct carrying a `_pb_field_map` comptime declaration.
//! Uses `inline for` over the field map to generate a per-message switch at
//! compile time, compiling to roughly the same code as hand-written
//! per-message encoders/decoders.
//!
//! Built on top of `wire.zig` primitives — see that file for the underlying
//! wire-format operations.
//!
//! Example:
//! ```
//! pub const MyMessage = struct {
//!     count: i64 = 0,
//!     name: []const u8 = "",
//!     values: []f32 = &.{},
//!
//!     pub const _pb_field_map = [_]message.FieldDesc{
//!         .{ .field_num = 1, .name = "count",  .encoding = .varint },
//!         .{ .field_num = 2, .name = "name",   .encoding = .string },
//!         .{ .field_num = 3, .name = "values", .encoding = .repeated_fixed32 },
//!     };
//! };
//!
//! const bytes = try message.encode(MyMessage, alloc, &my_msg);
//! defer alloc.free(bytes);
//! var decoded = try message.decode(MyMessage, alloc, bytes);
//! defer message.deinit(MyMessage, alloc, &decoded);
//! ```

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const wire = @import("wire.zig");

const Buf = wire.Buf;
const WireType = wire.WireType;

/// Encoding mode for a protobuf field. The Zig field type determines the
/// in-memory representation; this enum specifies the wire encoding.
pub const Encoding = enum {
    /// Varint scalar: i32, i64, u32, u64, bool, enum.
    varint,
    /// Zigzag-encoded signed varint: sint32, sint64.
    sint,
    /// Fixed32: f32, u32, i32 (wire: fixed32/sfixed32/float).
    fixed32,
    /// Fixed64: f64, u64, i64 (wire: fixed64/sfixed64/double).
    fixed64,
    /// Length-delimited bytes: field type []const u8 (string or bytes).
    string,
    /// Nested message: field type T or ?T where T is a struct.
    submessage,
    /// Repeated packed varint: []i32, []i64, []u32, []u64, []bool, []enum.
    repeated_varint,
    /// Repeated packed zigzag varint: []i32 or []i64 (sint32/sint64).
    repeated_sint,
    /// Repeated packed fixed32: []f32, []u32, []i32.
    repeated_fixed32,
    /// Repeated packed fixed64: []f64, []u64, []i64.
    repeated_fixed64,
    /// Repeated string: [][]const u8.
    repeated_string,
    /// Repeated message: []T.
    repeated_submessage,
    /// Repeated packed fixed32 stored as raw wire bytes — field type
    /// `[]const u8`. Zero-copy on decode (borrows from the input buffer).
    /// Length must be a multiple of 4. Use for large packed float/fixed32
    /// fields where allocating `[]f32` is wasteful.
    packed_raw_fixed32,
    /// Repeated packed fixed64 stored as raw wire bytes — field type
    /// `[]const u8`. Zero-copy on decode. Length must be a multiple of 8.
    packed_raw_fixed64,
    /// Repeated packed varint stored as raw wire bytes — field type
    /// `[]const u8`. Zero-copy on decode. Encodes unchanged.
    packed_raw_varint,
    /// Repeated sub-message stored as raw wire bytes, one slice per element
    /// — field type `[][]const u8`. Zero-copy on decode: each element points
    /// directly at the length-delimited payload inside the input buffer. The
    /// caller parses individual elements on demand via the sub-message's
    /// `decode()` method. Use for large repeated sub-messages (e.g. ONNX
    /// `GraphProto.initializer`) where parsing every element up front is
    /// wasteful. Encoding writes each element as-is with its own length
    /// prefix, so the caller is responsible for having produced valid
    /// sub-message bytes.
    lazy_repeated_submessage,
};

/// Field descriptor entry. Place a `pub const _pb_field_map = [_]FieldDesc{...}`
/// on a struct type to enable generic encode/decode.
pub const FieldDesc = struct {
    field_num: u32,
    name: []const u8,
    encoding: Encoding,
    /// If true, encode this field even when the current value equals its zero
    /// default. Used for proto2 fields with an explicit non-zero default: e.g.
    /// `optional bool add_dummy_prefix = 3 [default = true]`. Without this,
    /// writing `false` would be incorrectly skipped because `false` is the
    /// Zig zero default for `bool`, even though the proto default is `true`.
    /// Only affects encoding; decoding is unchanged.
    always_emit: bool = false,
};

/// Explicit error set for decode. An explicit set is required for recursive
/// message types (e.g. `DescriptorProto.nested_type: []DescriptorProto`) —
/// inferred error sets create a dependency loop between `decodeInto` and
/// `decodeField` when they call each other through `repeated_submessage`.
pub const DecodeError = wire.DecodeError || Allocator.Error || error{InvalidEnumValue};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Encode `msg` into newly allocated bytes. Caller owns the returned slice.
pub fn encode(comptime T: type, allocator: Allocator, msg: *const T) ![]u8 {
    var buf: Buf = .empty;
    errdefer buf.deinit(allocator);
    try buf.ensureTotalCapacityPrecise(allocator, encodedLen(T, msg));
    try encodeInto(T, allocator, &buf, msg);
    return buf.toOwnedSlice(allocator);
}

/// Encode `msg` into an existing buffer without writing a length prefix.
pub fn encodeInto(comptime T: type, allocator: Allocator, buf: *Buf, msg: *const T) Allocator.Error!void {
    @setEvalBranchQuota(100_000);
    inline for (T._pb_field_map) |fd| {
        try encodeField(fd, @TypeOf(@field(msg.*, fd.name)), allocator, buf, @field(msg.*, fd.name));
    }
}

/// Compute the byte length of encoding `msg` without allocating.
pub fn encodedLen(comptime T: type, msg: *const T) usize {
    @setEvalBranchQuota(100_000);
    var total: usize = 0;
    inline for (T._pb_field_map) |fd| {
        total += fieldEncodedLen(fd, @TypeOf(@field(msg.*, fd.name)), @field(msg.*, fd.name));
    }
    return total;
}

/// Decode `T` from protobuf bytes. Returned value owns allocator-allocated
/// slices for repeated, string, and submessage fields. Use `deinit` to free.
pub fn decode(comptime T: type, allocator: Allocator, bytes: []const u8) DecodeError!T {
    var result: T = .{};
    errdefer deinit(T, allocator, &result);
    try decodeInto(T, allocator, &result, bytes);
    return result;
}

/// Decode into an already-initialized value. Intended for sub-message decode
/// where the caller holds the T by pointer.
pub fn decodeInto(comptime T: type, allocator: Allocator, out: *T, bytes: []const u8) DecodeError!void {
    @setEvalBranchQuota(100_000);
    // Per-field accumulators for repeated fields. We use comptime reflection
    // to build a tuple of ArrayLists (one per repeated field).
    var lists = initRepeatedLists(T, allocator);
    errdefer deinitRepeatedLists(T, allocator, &lists);

    var pos: usize = 0;
    while (pos < bytes.len) {
        const tag = try wire.readTag(bytes, &pos);
        var handled = false;
        inline for (T._pb_field_map) |fd| {
            if (!handled and tag.field == fd.field_num) {
                try decodeField(
                    T,
                    fd.encoding,
                    fd.name,
                    @TypeOf(@field(out.*, fd.name)),
                    allocator,
                    bytes,
                    &pos,
                    tag.wire_type,
                    &@field(out.*, fd.name),
                    &lists,
                );
                handled = true;
            }
        }
        if (!handled) {
            try wire.skipField(bytes, &pos, tag.wire_type);
        }
    }

    // Finalize repeated fields: move ArrayList contents to owned slices.
    try finalizeRepeatedLists(T, allocator, out, &lists);
}

/// Recursively free allocator-owned fields (slices, sub-message slices).
/// Safe to call on a partially-decoded or zero-initialized struct.
pub fn deinit(comptime T: type, allocator: Allocator, msg: *T) void {
    @setEvalBranchQuota(100_000);
    inline for (T._pb_field_map) |fd| {
        deinitField(fd.encoding, @TypeOf(@field(msg.*, fd.name)), allocator, &@field(msg.*, fd.name));
    }
}

// ---------------------------------------------------------------------------
// Encode helpers
// ---------------------------------------------------------------------------

fn encodeField(
    comptime fd: FieldDesc,
    comptime FieldT: type,
    allocator: Allocator,
    buf: *Buf,
    value: FieldT,
) Allocator.Error!void {
    switch (fd.encoding) {
        .varint => try encodeVarintScalar(fd.field_num, fd.always_emit, FieldT, allocator, buf, value),
        .sint => try encodeSintScalar(fd.field_num, fd.always_emit, FieldT, allocator, buf, value),
        .fixed32 => try encodeFixed32Scalar(fd.field_num, fd.always_emit, FieldT, allocator, buf, value),
        .fixed64 => try encodeFixed64Scalar(fd.field_num, fd.always_emit, FieldT, allocator, buf, value),
        .string => try encodeStringField(fd.field_num, fd.always_emit, FieldT, allocator, buf, value),
        .submessage => try encodeSubmessageField(fd.field_num, FieldT, allocator, buf, value),
        .repeated_varint => try encodeRepeatedVarint(fd.field_num, FieldT, allocator, buf, value),
        .repeated_sint => try encodeRepeatedSint(fd.field_num, FieldT, allocator, buf, value),
        .repeated_fixed32 => try encodeRepeatedFixed32(fd.field_num, FieldT, allocator, buf, value),
        .repeated_fixed64 => try encodeRepeatedFixed64(fd.field_num, FieldT, allocator, buf, value),
        .repeated_string => try encodeRepeatedString(fd.field_num, FieldT, allocator, buf, value),
        .repeated_submessage => try encodeRepeatedSubmessage(fd.field_num, FieldT, allocator, buf, value),
        .packed_raw_fixed32, .packed_raw_fixed64, .packed_raw_varint => try encodePackedRaw(fd.field_num, allocator, buf, value),
        .lazy_repeated_submessage => try encodeLazyRepeatedSubmessage(fd.field_num, FieldT, allocator, buf, value),
    }
}

fn fieldEncodedLen(
    comptime fd: FieldDesc,
    comptime FieldT: type,
    value: FieldT,
) usize {
    const tag_size: usize = wire.varintSize(@as(u64, fd.field_num) << 3);
    switch (fd.encoding) {
        .varint => {
            if (@typeInfo(FieldT) == .optional) {
                const Child = @typeInfo(FieldT).optional.child;
                if (value) |v| return tag_size + wire.varintSize(varintWireValue(Child, v));
                return 0;
            }
            if (!fd.always_emit and isScalarDefault(FieldT, value)) return 0;
            return tag_size + wire.varintSize(varintWireValue(FieldT, value));
        },
        .sint => {
            if (@typeInfo(FieldT) == .optional) {
                if (value) |v| return tag_size + wire.varintSize(zigzagEncode(@intCast(v)));
                return 0;
            }
            if (!fd.always_emit and isScalarDefault(FieldT, value)) return 0;
            return tag_size + wire.varintSize(zigzagEncode(@intCast(value)));
        },
        .fixed32 => {
            if (@typeInfo(FieldT) == .optional) {
                if (value != null) return tag_size + 4;
                return 0;
            }
            if (!fd.always_emit and isScalarDefault(FieldT, value)) return 0;
            return tag_size + 4;
        },
        .fixed64 => {
            if (@typeInfo(FieldT) == .optional) {
                if (value != null) return tag_size + 8;
                return 0;
            }
            if (!fd.always_emit and isScalarDefault(FieldT, value)) return 0;
            return tag_size + 8;
        },
        .string => {
            if (@typeInfo(FieldT) == .optional) {
                if (value) |v| return tag_size + wire.varintSize(v.len) + v.len;
                return 0;
            }
            if (!fd.always_emit and value.len == 0) return 0;
            return tag_size + wire.varintSize(value.len) + value.len;
        },
        .submessage => return submessageFieldLen(tag_size, FieldT, value),
        .repeated_varint => {
            if (value.len == 0) return 0;
            const payload: usize = repeatedVarintPayloadLen(childElem(FieldT), value);
            if (payload == 0) return 0;
            return tag_size + wire.varintSize(payload) + payload;
        },
        .repeated_sint => {
            if (value.len == 0) return 0;
            const payload: usize = repeatedSintPayloadLen(childElem(FieldT), value);
            if (payload == 0) return 0;
            return tag_size + wire.varintSize(payload) + payload;
        },
        .repeated_fixed32 => {
            if (value.len == 0) return 0;
            const payload: usize = value.len * 4;
            return tag_size + wire.varintSize(payload) + payload;
        },
        .repeated_fixed64 => {
            if (value.len == 0) return 0;
            const payload: usize = value.len * 8;
            return tag_size + wire.varintSize(payload) + payload;
        },
        .repeated_string => {
            if (value.len == 0) return 0;
            var total: usize = 0;
            for (value) |item| {
                total += tag_size + wire.varintSize(item.len) + item.len;
            }
            return total;
        },
        .repeated_submessage => {
            if (value.len == 0) return 0;
            var total: usize = 0;
            for (value) |*item| {
                const Sub = @TypeOf(item.*);
                const sub_len = encodedLen(Sub, item);
                total += tag_size + wire.varintSize(sub_len) + sub_len;
            }
            return total;
        },
        .packed_raw_fixed32, .packed_raw_fixed64, .packed_raw_varint => {
            if (value.len == 0) return 0;
            return tag_size + wire.varintSize(value.len) + value.len;
        },
        .lazy_repeated_submessage => {
            if (value.len == 0) return 0;
            var total: usize = 0;
            for (value) |item| {
                total += tag_size + wire.varintSize(item.len) + item.len;
            }
            return total;
        },
    }
}

fn childElem(comptime T: type) type {
    // Walk through slice or array to get the child element type.
    return switch (@typeInfo(T)) {
        .pointer => |p| p.child,
        .array => |a| a.child,
        else => @compileError("expected slice/array, got " ++ @typeName(T)),
    };
}

/// Unwrap an optional type to its inner child, or return T unchanged.
fn OptionalChild(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .optional => |o| o.child,
        else => T,
    };
}

fn isScalarDefault(comptime T: type, value: T) bool {
    return switch (@typeInfo(T)) {
        .int => value == 0,
        .float => value == 0.0,
        .bool => !value,
        .@"enum" => @intFromEnum(value) == 0,
        else => @compileError("isScalarDefault: unsupported type " ++ @typeName(T)),
    };
}

/// Convert a scalar value to its u64 wire representation for varint encoding.
fn varintWireValue(comptime T: type, value: T) u64 {
    return switch (@typeInfo(T)) {
        .int => |int_info| if (int_info.signedness == .signed)
            @bitCast(@as(i64, @intCast(value)))
        else
            @intCast(value),
        .bool => @intFromBool(value),
        .@"enum" => |enum_info| blk: {
            const tag_val = @intFromEnum(value);
            const tag_info = @typeInfo(enum_info.tag_type).int;
            break :blk if (tag_info.signedness == .signed)
                @bitCast(@as(i64, @intCast(tag_val)))
            else
                @intCast(tag_val);
        },
        else => @compileError("varintWireValue: unsupported type " ++ @typeName(T)),
    };
}

fn zigzagEncode(value: i64) u64 {
    return @bitCast((value << 1) ^ (value >> 63));
}

fn zigzagDecode(value: u64) i64 {
    const v: i64 = @bitCast(value >> 1);
    const sign: i64 = -@as(i64, @bitCast(value & 1));
    return v ^ sign;
}

fn encodeVarintScalar(
    comptime field_num: u32,
    comptime always_emit: bool,
    comptime T: type,
    allocator: Allocator,
    buf: *Buf,
    value: T,
) Allocator.Error!void {
    if (@typeInfo(T) == .optional) {
        const Child = @typeInfo(T).optional.child;
        if (value) |v| {
            try wire.writeTag(allocator, buf, field_num, .varint);
            try wire.writeVarint(allocator, buf, varintWireValue(Child, v));
        }
        return;
    }
    if (!always_emit and isScalarDefault(T, value)) return;
    try wire.writeTag(allocator, buf, field_num, .varint);
    try wire.writeVarint(allocator, buf, varintWireValue(T, value));
}

fn encodeSintScalar(
    comptime field_num: u32,
    comptime always_emit: bool,
    comptime T: type,
    allocator: Allocator,
    buf: *Buf,
    value: T,
) Allocator.Error!void {
    if (@typeInfo(T) == .optional) {
        const Child = @typeInfo(T).optional.child;
        const child_info = @typeInfo(Child).int;
        if (child_info.signedness != .signed) @compileError("sint requires signed integer, got " ++ @typeName(Child));
        if (value) |v| {
            try wire.writeTag(allocator, buf, field_num, .varint);
            try wire.writeVarint(allocator, buf, zigzagEncode(@intCast(v)));
        }
        return;
    }
    const int_info = @typeInfo(T).int;
    if (int_info.signedness != .signed) @compileError("sint requires signed integer, got " ++ @typeName(T));
    if (!always_emit and value == 0) return;
    try wire.writeTag(allocator, buf, field_num, .varint);
    try wire.writeVarint(allocator, buf, zigzagEncode(@intCast(value)));
}

fn encodeFixed32Scalar(
    comptime field_num: u32,
    comptime always_emit: bool,
    comptime T: type,
    allocator: Allocator,
    buf: *Buf,
    value: T,
) Allocator.Error!void {
    if (@typeInfo(T) == .optional) {
        const Child = @typeInfo(T).optional.child;
        if (value) |v| try wire.writeFixed32(allocator, buf, field_num, fixed32Bits(Child, v));
        return;
    }
    if (!always_emit and isScalarDefault(T, value)) return;
    try wire.writeFixed32(allocator, buf, field_num, fixed32Bits(T, value));
}

fn fixed32Bits(comptime T: type, value: T) u32 {
    return switch (@typeInfo(T)) {
        .float => |f| blk: {
            if (f.bits != 32) @compileError("fixed32 requires f32, got " ++ @typeName(T));
            break :blk @bitCast(value);
        },
        .int => |i| blk: {
            if (i.bits != 32) @compileError("fixed32 requires 32-bit int, got " ++ @typeName(T));
            break :blk if (i.signedness == .signed) @bitCast(@as(i32, value)) else value;
        },
        else => @compileError("fixed32 requires f32/u32/i32, got " ++ @typeName(T)),
    };
}

fn encodeFixed64Scalar(
    comptime field_num: u32,
    comptime always_emit: bool,
    comptime T: type,
    allocator: Allocator,
    buf: *Buf,
    value: T,
) Allocator.Error!void {
    if (@typeInfo(T) == .optional) {
        const Child = @typeInfo(T).optional.child;
        if (value) |v| try wire.writeFixed64(allocator, buf, field_num, fixed64Bits(Child, v));
        return;
    }
    if (!always_emit and isScalarDefault(T, value)) return;
    try wire.writeFixed64(allocator, buf, field_num, fixed64Bits(T, value));
}

fn fixed64Bits(comptime T: type, value: T) u64 {
    return switch (@typeInfo(T)) {
        .float => |f| blk: {
            if (f.bits != 64) @compileError("fixed64 requires f64, got " ++ @typeName(T));
            break :blk @bitCast(value);
        },
        .int => |i| blk: {
            if (i.bits != 64) @compileError("fixed64 requires 64-bit int, got " ++ @typeName(T));
            break :blk if (i.signedness == .signed) @bitCast(@as(i64, value)) else value;
        },
        else => @compileError("fixed64 requires f64/u64/i64, got " ++ @typeName(T)),
    };
}

fn encodeStringField(
    comptime field_num: u32,
    comptime always_emit: bool,
    comptime T: type,
    allocator: Allocator,
    buf: *Buf,
    value: T,
) Allocator.Error!void {
    if (@typeInfo(T) == .optional) {
        // ?[]const u8: set → always encode (presence semantics), null → skip.
        if (value) |v| try wire.writeString(allocator, buf, field_num, v);
        return;
    }
    if (!always_emit and value.len == 0) return;
    try wire.writeString(allocator, buf, field_num, value);
}

/// Handle T, ?T, *T, and ?*T for submessages. `?*T` is used for recursive
/// message types that can't be stored inline (e.g. `ShapeProto ↔ LayoutProto`
/// in xla.proto — both sides must box the recursive edge).
fn encodeSubmessageField(
    comptime field_num: u32,
    comptime FieldT: type,
    allocator: Allocator,
    buf: *Buf,
    value: FieldT,
) Allocator.Error!void {
    switch (@typeInfo(FieldT)) {
        .optional => |opt| switch (@typeInfo(opt.child)) {
            .pointer => |ptr| {
                if (value) |p| {
                    try encodeSubmessageFieldInline(field_num, ptr.child, allocator, buf, p);
                }
            },
            .@"struct" => {
                if (value) |v| {
                    try encodeSubmessageFieldInline(field_num, opt.child, allocator, buf, &v);
                }
            },
            else => @compileError("optional submessage must wrap struct or pointer-to-struct, got " ++ @typeName(FieldT)),
        },
        .pointer => |ptr| {
            try encodeSubmessageFieldInline(field_num, ptr.child, allocator, buf, value);
        },
        .@"struct" => {
            try encodeSubmessageFieldInline(field_num, FieldT, allocator, buf, &value);
        },
        else => @compileError("submessage requires struct or optional/pointer struct, got " ++ @typeName(FieldT)),
    }
}

fn encodeSubmessageFieldInline(
    comptime field_num: u32,
    comptime SubT: type,
    allocator: Allocator,
    buf: *Buf,
    value: *const SubT,
) Allocator.Error!void {
    const sub_len = encodedLen(SubT, value);
    if (sub_len == 0) return;
    try wire.writeTag(allocator, buf, field_num, .length_delimited);
    try wire.writeVarint(allocator, buf, sub_len);
    try encodeInto(SubT, allocator, buf, value);
}

fn submessageFieldLen(tag_size: usize, comptime FieldT: type, value: FieldT) usize {
    switch (@typeInfo(FieldT)) {
        .optional => |opt| switch (@typeInfo(opt.child)) {
            .pointer => |ptr| {
                if (value) |p| {
                    const sub_len = encodedLen(ptr.child, p);
                    if (sub_len == 0) return 0;
                    return tag_size + wire.varintSize(sub_len) + sub_len;
                }
                return 0;
            },
            .@"struct" => {
                if (value) |v| {
                    const sub_len = encodedLen(opt.child, &v);
                    if (sub_len == 0) return 0;
                    return tag_size + wire.varintSize(sub_len) + sub_len;
                }
                return 0;
            },
            else => @compileError("optional submessage must wrap struct or pointer-to-struct, got " ++ @typeName(FieldT)),
        },
        .pointer => |ptr| {
            const sub_len = encodedLen(ptr.child, value);
            if (sub_len == 0) return 0;
            return tag_size + wire.varintSize(sub_len) + sub_len;
        },
        .@"struct" => {
            const sub_len = encodedLen(FieldT, &value);
            if (sub_len == 0) return 0;
            return tag_size + wire.varintSize(sub_len) + sub_len;
        },
        else => @compileError("submessage requires struct or optional/pointer struct, got " ++ @typeName(FieldT)),
    }
}

fn repeatedVarintPayloadLen(comptime Elem: type, values: anytype) usize {
    var total: usize = 0;
    for (values) |v| total += wire.varintSize(varintWireValue(Elem, v));
    return total;
}

fn repeatedSintPayloadLen(comptime Elem: type, values: anytype) usize {
    const int_info = @typeInfo(Elem).int;
    if (int_info.signedness != .signed) @compileError("repeated_sint requires signed element, got " ++ @typeName(Elem));
    var total: usize = 0;
    for (values) |v| total += wire.varintSize(zigzagEncode(@intCast(v)));
    return total;
}

fn encodeRepeatedVarint(
    comptime field_num: u32,
    comptime FieldT: type,
    allocator: Allocator,
    buf: *Buf,
    value: FieldT,
) Allocator.Error!void {
    if (value.len == 0) return;
    const Elem = childElem(FieldT);
    const payload: usize = repeatedVarintPayloadLen(Elem, value);
    try wire.writeTag(allocator, buf, field_num, .length_delimited);
    try wire.writeVarint(allocator, buf, payload);
    for (value) |v| try wire.writeVarint(allocator, buf, varintWireValue(Elem, v));
}

fn encodeRepeatedSint(
    comptime field_num: u32,
    comptime FieldT: type,
    allocator: Allocator,
    buf: *Buf,
    value: FieldT,
) Allocator.Error!void {
    if (value.len == 0) return;
    const Elem = childElem(FieldT);
    const int_info = @typeInfo(Elem).int;
    if (int_info.signedness != .signed) @compileError("repeated_sint requires signed element, got " ++ @typeName(Elem));
    const payload: usize = repeatedSintPayloadLen(Elem, value);
    try wire.writeTag(allocator, buf, field_num, .length_delimited);
    try wire.writeVarint(allocator, buf, payload);
    for (value) |v| try wire.writeVarint(allocator, buf, zigzagEncode(@intCast(v)));
}

fn encodeRepeatedFixed32(
    comptime field_num: u32,
    comptime FieldT: type,
    allocator: Allocator,
    buf: *Buf,
    value: FieldT,
) Allocator.Error!void {
    if (value.len == 0) return;
    const Elem = childElem(FieldT);
    const payload: usize = value.len * 4;
    try wire.writeTag(allocator, buf, field_num, .length_delimited);
    try wire.writeVarint(allocator, buf, payload);
    switch (@typeInfo(Elem)) {
        .float => {
            if (@typeInfo(Elem).float.bits != 32) @compileError("repeated_fixed32 requires f32, got " ++ @typeName(Elem));
            if (builtin.target.cpu.arch.endian() == .little) {
                try buf.appendSlice(allocator, std.mem.sliceAsBytes(value));
            } else {
                for (value) |v| {
                    const bits: u32 = @bitCast(v);
                    var bytes: [4]u8 = undefined;
                    std.mem.writeInt(u32, &bytes, bits, .little);
                    try buf.appendSlice(allocator, &bytes);
                }
            }
        },
        .int => |int_info| {
            if (int_info.bits != 32) @compileError("repeated_fixed32 requires 32-bit int, got " ++ @typeName(Elem));
            if (builtin.target.cpu.arch.endian() == .little) {
                try buf.appendSlice(allocator, std.mem.sliceAsBytes(value));
            } else {
                for (value) |v| {
                    const bits: u32 = if (int_info.signedness == .signed) @bitCast(@as(i32, v)) else v;
                    var bytes: [4]u8 = undefined;
                    std.mem.writeInt(u32, &bytes, bits, .little);
                    try buf.appendSlice(allocator, &bytes);
                }
            }
        },
        else => @compileError("repeated_fixed32 requires f32/u32/i32 elements, got " ++ @typeName(Elem)),
    }
}

fn encodeRepeatedFixed64(
    comptime field_num: u32,
    comptime FieldT: type,
    allocator: Allocator,
    buf: *Buf,
    value: FieldT,
) Allocator.Error!void {
    if (value.len == 0) return;
    const Elem = childElem(FieldT);
    const payload: usize = value.len * 8;
    try wire.writeTag(allocator, buf, field_num, .length_delimited);
    try wire.writeVarint(allocator, buf, payload);
    switch (@typeInfo(Elem)) {
        .float => {
            if (@typeInfo(Elem).float.bits != 64) @compileError("repeated_fixed64 requires f64, got " ++ @typeName(Elem));
            if (builtin.target.cpu.arch.endian() == .little) {
                try buf.appendSlice(allocator, std.mem.sliceAsBytes(value));
            } else {
                for (value) |v| {
                    const bits: u64 = @bitCast(v);
                    var bytes: [8]u8 = undefined;
                    std.mem.writeInt(u64, &bytes, bits, .little);
                    try buf.appendSlice(allocator, &bytes);
                }
            }
        },
        .int => |int_info| {
            if (int_info.bits != 64) @compileError("repeated_fixed64 requires 64-bit int, got " ++ @typeName(Elem));
            if (builtin.target.cpu.arch.endian() == .little) {
                try buf.appendSlice(allocator, std.mem.sliceAsBytes(value));
            } else {
                for (value) |v| {
                    const bits: u64 = if (int_info.signedness == .signed) @bitCast(@as(i64, v)) else v;
                    var bytes: [8]u8 = undefined;
                    std.mem.writeInt(u64, &bytes, bits, .little);
                    try buf.appendSlice(allocator, &bytes);
                }
            }
        },
        else => @compileError("repeated_fixed64 requires f64/u64/i64 elements, got " ++ @typeName(Elem)),
    }
}

fn encodeRepeatedString(
    comptime field_num: u32,
    comptime FieldT: type,
    allocator: Allocator,
    buf: *Buf,
    value: FieldT,
) Allocator.Error!void {
    for (value) |item| {
        try wire.writeString(allocator, buf, field_num, item);
    }
}

fn encodeRepeatedSubmessage(
    comptime field_num: u32,
    comptime FieldT: type,
    allocator: Allocator,
    buf: *Buf,
    value: FieldT,
) Allocator.Error!void {
    const Elem = childElem(FieldT);
    for (value) |*item| {
        try encodeSubmessageFieldInline(field_num, Elem, allocator, buf, item);
    }
}

fn encodePackedRaw(
    comptime field_num: u32,
    allocator: Allocator,
    buf: *Buf,
    value: []const u8,
) Allocator.Error!void {
    if (value.len == 0) return;
    try wire.writeBytes(allocator, buf, field_num, value);
}

fn encodeLazyRepeatedSubmessage(
    comptime field_num: u32,
    comptime FieldT: type,
    allocator: Allocator,
    buf: *Buf,
    value: FieldT,
) Allocator.Error!void {
    _ = childElem(FieldT);
    for (value) |item| {
        try wire.writeMessage(allocator, buf, field_num, item);
    }
}

// ---------------------------------------------------------------------------
// Decode helpers
// ---------------------------------------------------------------------------

/// For each repeated field in T, we maintain an ArrayListUnmanaged while
/// decoding. The list type depends on the element type. `finalizeRepeatedLists`
/// converts them to owned slices and assigns them to the output struct.
///
/// We build a struct at comptime where each field corresponds to a field of T
/// (in the same order and with the same name). Non-repeated fields get a
/// `void` placeholder so `@field(lists, fd.name)` works uniformly.
fn RepeatedLists(comptime T: type) type {
    @setEvalBranchQuota(100_000);
    const map = T._pb_field_map;
    comptime var field_names: [map.len][]const u8 = undefined;
    comptime var field_types: [map.len]type = undefined;
    comptime var field_attrs: [map.len]std.builtin.Type.StructField.Attributes = undefined;
    inline for (map, 0..) |fd, i| {
        const FieldT = @FieldType(T, fd.name);
        const ListT: type = switch (fd.encoding) {
            .repeated_varint,
            .repeated_sint,
            .repeated_fixed32,
            .repeated_fixed64,
            .repeated_string,
            .repeated_submessage,
            .lazy_repeated_submessage,
            => std.ArrayListUnmanaged(childElem(FieldT)),
            else => void,
        };
        field_names[i] = fd.name;
        field_types[i] = ListT;
        field_attrs[i] = .{};
    }
    return @Struct(.auto, null, &field_names, &field_types, &field_attrs);
}

fn initRepeatedLists(comptime T: type, _: Allocator) RepeatedLists(T) {
    var lists: RepeatedLists(T) = undefined;
    inline for (T._pb_field_map) |fd| {
        const FieldListT = @FieldType(RepeatedLists(T), fd.name);
        if (FieldListT != void) {
            @field(lists, fd.name) = .empty;
        }
    }
    return lists;
}

fn deinitRepeatedLists(comptime T: type, allocator: Allocator, lists: *RepeatedLists(T)) void {
    inline for (T._pb_field_map) |fd| {
        const FieldListT = @FieldType(RepeatedLists(T), fd.name);
        if (FieldListT != void) {
            @field(lists.*, fd.name).deinit(allocator);
        }
    }
}

fn finalizeRepeatedLists(
    comptime T: type,
    allocator: Allocator,
    out: *T,
    lists: *RepeatedLists(T),
) !void {
    inline for (T._pb_field_map) |fd| {
        const FieldListT = @FieldType(RepeatedLists(T), fd.name);
        if (FieldListT != void) {
            @field(out.*, fd.name) = try @field(lists.*, fd.name).toOwnedSlice(allocator);
        }
    }
}

fn decodeField(
    comptime T: type,
    comptime encoding: Encoding,
    comptime field_name: []const u8,
    comptime FieldT: type,
    allocator: Allocator,
    bytes: []const u8,
    pos: *usize,
    wire_type: WireType,
    out_ptr: anytype,
    lists: *RepeatedLists(T),
) DecodeError!void {
    switch (encoding) {
        .varint => {
            const raw = try wire.readVarint(bytes, pos);
            if (@typeInfo(FieldT) == .optional) {
                const Child = @typeInfo(FieldT).optional.child;
                out_ptr.* = try varintFromWire(Child, raw);
            } else {
                out_ptr.* = try varintFromWire(FieldT, raw);
            }
        },
        .sint => {
            const raw = try wire.readVarint(bytes, pos);
            if (@typeInfo(FieldT) == .optional) {
                const Child = @typeInfo(FieldT).optional.child;
                const decoded: Child = @intCast(zigzagDecode(raw));
                out_ptr.* = decoded;
            } else {
                out_ptr.* = @intCast(zigzagDecode(raw));
            }
        },
        .fixed32 => {
            const raw = try wire.readFixed32(bytes, pos);
            if (@typeInfo(FieldT) == .optional) {
                const Child = @typeInfo(FieldT).optional.child;
                out_ptr.* = fixed32FromWire(Child, raw);
            } else {
                out_ptr.* = fixed32FromWire(FieldT, raw);
            }
        },
        .fixed64 => {
            const raw = try wire.readFixed64(bytes, pos);
            if (@typeInfo(FieldT) == .optional) {
                const Child = @typeInfo(FieldT).optional.child;
                out_ptr.* = fixed64FromWire(Child, raw);
            } else {
                out_ptr.* = fixed64FromWire(FieldT, raw);
            }
        },
        .string => {
            out_ptr.* = try wire.readLengthDelimited(bytes, pos);
        },
        .submessage => {
            const sub_bytes = try wire.readLengthDelimited(bytes, pos);
            try decodeSubmessageInto(FieldT, allocator, out_ptr, sub_bytes);
        },
        .repeated_varint => try decodeRepeatedVarintInto(
            childElem(FieldT),
            allocator,
            bytes,
            pos,
            wire_type,
            &@field(lists.*, field_name),
        ),
        .repeated_sint => try decodeRepeatedSintInto(
            childElem(FieldT),
            allocator,
            bytes,
            pos,
            wire_type,
            &@field(lists.*, field_name),
        ),
        .repeated_fixed32 => try decodeRepeatedFixed32Into(
            childElem(FieldT),
            allocator,
            bytes,
            pos,
            wire_type,
            &@field(lists.*, field_name),
        ),
        .repeated_fixed64 => try decodeRepeatedFixed64Into(
            childElem(FieldT),
            allocator,
            bytes,
            pos,
            wire_type,
            &@field(lists.*, field_name),
        ),
        .repeated_string => {
            const sub_bytes = try wire.readLengthDelimited(bytes, pos);
            try @field(lists.*, field_name).append(allocator, sub_bytes);
        },
        .repeated_submessage => {
            const Elem = childElem(FieldT);
            const sub_bytes = try wire.readLengthDelimited(bytes, pos);
            var item: Elem = .{};
            errdefer deinit(Elem, allocator, &item);
            try decodeInto(Elem, allocator, &item, sub_bytes);
            try @field(lists.*, field_name).append(allocator, item);
        },
        .packed_raw_fixed32, .packed_raw_fixed64, .packed_raw_varint => {
            out_ptr.* = try wire.readLengthDelimited(bytes, pos);
        },
        .lazy_repeated_submessage => {
            const sub_bytes = try wire.readLengthDelimited(bytes, pos);
            try @field(lists.*, field_name).append(allocator, sub_bytes);
        },
    }
}

fn varintFromWire(comptime T: type, raw: u64) DecodeError!T {
    return switch (@typeInfo(T)) {
        .int => |i| if (i.signedness == .signed)
            @intCast(@as(i64, @bitCast(raw)))
        else
            @intCast(raw),
        .bool => raw != 0,
        .@"enum" => |e| blk: {
            const tag_info = @typeInfo(e.tag_type).int;
            const tag_val: e.tag_type = if (tag_info.signedness == .signed)
                std.math.cast(e.tag_type, @as(i64, @bitCast(raw))) orelse return error.InvalidEnumValue
            else
                std.math.cast(e.tag_type, raw) orelse return error.InvalidEnumValue;
            break :blk enumFromWireUnchecked(T, tag_val);
        },
        else => @compileError("varintFromWire: unsupported " ++ @typeName(T)),
    };
}

fn enumFromWireUnchecked(comptime T: type, tag_val: @typeInfo(T).@"enum".tag_type) T {
    // Proto enums preserve unknown in-range values. Zig exhaustive enums trap on
    // unknown @enumFromInt values in safe builds, so the decoder only rejects
    // values that cannot fit the enum tag type and leaves semantic validation to
    // callers that own the concrete enum.
    @setRuntimeSafety(false);
    return @enumFromInt(tag_val);
}

fn fixed32FromWire(comptime T: type, raw: u32) T {
    return switch (@typeInfo(T)) {
        .float => @bitCast(raw),
        .int => |i| if (i.signedness == .signed) @intCast(@as(i32, @bitCast(raw))) else @intCast(raw),
        else => @compileError("fixed32FromWire: unsupported " ++ @typeName(T)),
    };
}

fn fixed64FromWire(comptime T: type, raw: u64) T {
    return switch (@typeInfo(T)) {
        .float => @bitCast(raw),
        .int => |i| if (i.signedness == .signed) @intCast(@as(i64, @bitCast(raw))) else @intCast(raw),
        else => @compileError("fixed64FromWire: unsupported " ++ @typeName(T)),
    };
}

fn decodeSubmessageInto(
    comptime FieldT: type,
    allocator: Allocator,
    out_ptr: *FieldT,
    sub_bytes: []const u8,
) DecodeError!void {
    switch (@typeInfo(FieldT)) {
        .optional => |opt| switch (@typeInfo(opt.child)) {
            .pointer => |ptr| {
                // Merge into existing boxed value if present; otherwise allocate.
                if (out_ptr.*) |existing| {
                    try decodeInto(ptr.child, allocator, existing, sub_bytes);
                } else {
                    const boxed = try allocator.create(ptr.child);
                    errdefer allocator.destroy(boxed);
                    boxed.* = .{};
                    errdefer deinit(ptr.child, allocator, boxed);
                    try decodeInto(ptr.child, allocator, boxed, sub_bytes);
                    out_ptr.* = boxed;
                }
            },
            .@"struct" => {
                var inner: opt.child = .{};
                errdefer deinit(opt.child, allocator, &inner);
                try decodeInto(opt.child, allocator, &inner, sub_bytes);
                out_ptr.* = inner;
            },
            else => @compileError("optional submessage must wrap struct or pointer-to-struct, got " ++ @typeName(FieldT)),
        },
        .pointer => |ptr| {
            // Unreachable for decode because default is `?*T = null`; if someone
            // declares a non-optional pointer field, require them to preallocate.
            try decodeInto(ptr.child, allocator, out_ptr.*, sub_bytes);
        },
        .@"struct" => {
            // Free any existing allocations before overwriting.
            deinit(FieldT, allocator, out_ptr);
            out_ptr.* = .{};
            try decodeInto(FieldT, allocator, out_ptr, sub_bytes);
        },
        else => @compileError("submessage requires struct or optional/pointer struct, got " ++ @typeName(FieldT)),
    }
}

fn decodeRepeatedVarintInto(
    comptime Elem: type,
    allocator: Allocator,
    bytes: []const u8,
    pos: *usize,
    wire_type: WireType,
    list: *std.ArrayListUnmanaged(Elem),
) !void {
    if (wire_type == .length_delimited) {
        const payload = try wire.readLengthDelimited(bytes, pos);
        var ipos: usize = 0;
        while (ipos < payload.len) {
            const raw = try wire.readVarint(payload, &ipos);
            try list.append(allocator, try varintFromWire(Elem, raw));
        }
    } else {
        const raw = try wire.readVarint(bytes, pos);
        try list.append(allocator, try varintFromWire(Elem, raw));
    }
}

fn decodeRepeatedSintInto(
    comptime Elem: type,
    allocator: Allocator,
    bytes: []const u8,
    pos: *usize,
    wire_type: WireType,
    list: *std.ArrayListUnmanaged(Elem),
) !void {
    if (wire_type == .length_delimited) {
        const payload = try wire.readLengthDelimited(bytes, pos);
        var ipos: usize = 0;
        while (ipos < payload.len) {
            const raw = try wire.readVarint(payload, &ipos);
            try list.append(allocator, @intCast(zigzagDecode(raw)));
        }
    } else {
        const raw = try wire.readVarint(bytes, pos);
        try list.append(allocator, @intCast(zigzagDecode(raw)));
    }
}

fn decodeRepeatedFixed32Into(
    comptime Elem: type,
    allocator: Allocator,
    bytes: []const u8,
    pos: *usize,
    wire_type: WireType,
    list: *std.ArrayListUnmanaged(Elem),
) !void {
    if (wire_type == .length_delimited) {
        const payload = try wire.readLengthDelimited(bytes, pos);
        const count = payload.len / 4;
        try list.ensureUnusedCapacity(allocator, count);
        var ipos: usize = 0;
        for (0..count) |_| {
            const raw = std.mem.readInt(u32, payload[ipos..][0..4], .little);
            ipos += 4;
            list.appendAssumeCapacity(fixed32FromWire(Elem, raw));
        }
    } else {
        const raw = try wire.readFixed32(bytes, pos);
        try list.append(allocator, fixed32FromWire(Elem, raw));
    }
}

fn decodeRepeatedFixed64Into(
    comptime Elem: type,
    allocator: Allocator,
    bytes: []const u8,
    pos: *usize,
    wire_type: WireType,
    list: *std.ArrayListUnmanaged(Elem),
) !void {
    if (wire_type == .length_delimited) {
        const payload = try wire.readLengthDelimited(bytes, pos);
        const count = payload.len / 8;
        try list.ensureUnusedCapacity(allocator, count);
        var ipos: usize = 0;
        for (0..count) |_| {
            const raw = std.mem.readInt(u64, payload[ipos..][0..8], .little);
            ipos += 8;
            list.appendAssumeCapacity(fixed64FromWire(Elem, raw));
        }
    } else {
        const raw = try wire.readFixed64(bytes, pos);
        try list.append(allocator, fixed64FromWire(Elem, raw));
    }
}

// ---------------------------------------------------------------------------
// Deinit helpers
// ---------------------------------------------------------------------------

fn deinitField(
    comptime encoding: Encoding,
    comptime FieldT: type,
    allocator: Allocator,
    ptr: *FieldT,
) void {
    switch (encoding) {
        .varint, .sint, .fixed32, .fixed64 => {},
        .string => {}, // []const u8 points into input buffer — caller owns bytes
        .submessage => {
            switch (@typeInfo(FieldT)) {
                .optional => |opt| switch (@typeInfo(opt.child)) {
                    .pointer => |p| {
                        if (ptr.*) |boxed| {
                            deinit(p.child, allocator, boxed);
                            allocator.destroy(boxed);
                        }
                        ptr.* = null;
                    },
                    .@"struct" => {
                        if (ptr.*) |*inner| deinit(opt.child, allocator, inner);
                        ptr.* = null;
                    },
                    else => {},
                },
                .@"struct" => deinit(FieldT, allocator, ptr),
                else => {},
            }
        },
        .repeated_varint,
        .repeated_sint,
        .repeated_fixed32,
        .repeated_fixed64,
        .repeated_string,
        => {
            if (ptr.len > 0) allocator.free(ptr.*);
            ptr.* = &.{};
        },
        .packed_raw_fixed32,
        .packed_raw_fixed64,
        .packed_raw_varint,
        => {},
        .lazy_repeated_submessage => {
            if (ptr.len > 0) allocator.free(ptr.*);
            ptr.* = &.{};
        },
        .repeated_submessage => {
            const Elem = childElem(FieldT);
            for (ptr.*) |*item| deinit(Elem, allocator, item);
            if (ptr.len > 0) allocator.free(ptr.*);
            ptr.* = &.{};
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

const ScalarMsg = struct {
    a: i32 = 0,
    b: i64 = 0,
    c: u32 = 0,
    d: u64 = 0,
    e: bool = false,
    f: f32 = 0.0,
    g: f64 = 0.0,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "a", .encoding = .varint },
        .{ .field_num = 2, .name = "b", .encoding = .varint },
        .{ .field_num = 3, .name = "c", .encoding = .varint },
        .{ .field_num = 4, .name = "d", .encoding = .varint },
        .{ .field_num = 5, .name = "e", .encoding = .varint },
        .{ .field_num = 6, .name = "f", .encoding = .fixed32 },
        .{ .field_num = 7, .name = "g", .encoding = .fixed64 },
    };
};

test "scalar roundtrip" {
    const alloc = testing.allocator;
    const original = ScalarMsg{
        .a = -42,
        .b = 1_000_000_000,
        .c = 1234,
        .d = 0xDEADBEEF_CAFEBABE,
        .e = true,
        .f = 3.14,
        .g = 2.71828,
    };

    const bytes = try encode(ScalarMsg, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(ScalarMsg, alloc, bytes);
    defer deinit(ScalarMsg, alloc, &decoded);

    try testing.expectEqual(original.a, decoded.a);
    try testing.expectEqual(original.b, decoded.b);
    try testing.expectEqual(original.c, decoded.c);
    try testing.expectEqual(original.d, decoded.d);
    try testing.expectEqual(original.e, decoded.e);
    try testing.expectEqual(original.f, decoded.f);
    try testing.expectEqual(original.g, decoded.g);
}

test "scalar defaults skip encoding" {
    const alloc = testing.allocator;
    const original = ScalarMsg{};

    const bytes = try encode(ScalarMsg, alloc, &original);
    defer alloc.free(bytes);
    try testing.expectEqual(@as(usize, 0), bytes.len);
}

const StringMsg = struct {
    name: []const u8 = "",
    data: []const u8 = "",

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        .{ .field_num = 2, .name = "data", .encoding = .string },
    };
};

test "string roundtrip" {
    const alloc = testing.allocator;
    const original = StringMsg{
        .name = "hello world",
        .data = "\x00\x01\x02\xff",
    };

    const bytes = try encode(StringMsg, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(StringMsg, alloc, bytes);
    defer deinit(StringMsg, alloc, &decoded);

    try testing.expectEqualStrings(original.name, decoded.name);
    try testing.expectEqualSlices(u8, original.data, decoded.data);
}

const SintMsg = struct {
    x: i32 = 0,
    y: i64 = 0,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "x", .encoding = .sint },
        .{ .field_num = 2, .name = "y", .encoding = .sint },
    };
};

test "sint zigzag roundtrip" {
    const alloc = testing.allocator;
    const cases = [_]SintMsg{
        .{ .x = 0, .y = 0 },
        .{ .x = -1, .y = -1 },
        .{ .x = 1, .y = 1 },
        .{ .x = -1_000_000, .y = 9_000_000_000_000 },
        .{ .x = std.math.maxInt(i32), .y = std.math.maxInt(i64) },
        .{ .x = std.math.minInt(i32), .y = std.math.minInt(i64) },
    };
    for (cases) |expected| {
        const bytes = try encode(SintMsg, alloc, &expected);
        defer alloc.free(bytes);
        var decoded = try decode(SintMsg, alloc, bytes);
        defer deinit(SintMsg, alloc, &decoded);
        try testing.expectEqual(expected.x, decoded.x);
        try testing.expectEqual(expected.y, decoded.y);
    }
}

const RepeatedMsg = struct {
    ints: []i64 = &.{},
    floats: []f32 = &.{},
    doubles: []f64 = &.{},
    uints: []u64 = &.{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "ints", .encoding = .repeated_varint },
        .{ .field_num = 2, .name = "floats", .encoding = .repeated_fixed32 },
        .{ .field_num = 3, .name = "doubles", .encoding = .repeated_fixed64 },
        .{ .field_num = 4, .name = "uints", .encoding = .repeated_fixed64 },
    };
};

test "repeated packed roundtrip" {
    const alloc = testing.allocator;
    const ints_in = [_]i64{ 1, -2, 3, -4, 5 };
    const floats_in = [_]f32{ 1.0, 2.0, 3.5 };
    const doubles_in = [_]f64{ 1.1, 2.2, 3.3 };
    const uints_in = [_]u64{ 100, 200, 300 };

    const original = RepeatedMsg{
        .ints = @constCast(ints_in[0..]),
        .floats = @constCast(floats_in[0..]),
        .doubles = @constCast(doubles_in[0..]),
        .uints = @constCast(uints_in[0..]),
    };

    const bytes = try encode(RepeatedMsg, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(RepeatedMsg, alloc, bytes);
    defer deinit(RepeatedMsg, alloc, &decoded);

    try testing.expectEqualSlices(i64, &ints_in, decoded.ints);
    try testing.expectEqualSlices(f32, &floats_in, decoded.floats);
    try testing.expectEqualSlices(f64, &doubles_in, decoded.doubles);
    try testing.expectEqualSlices(u64, &uints_in, decoded.uints);
}

const EnumField = enum(i32) {
    zero = 0,
    one = 1,
    two = 2,
    _,
};

const EnumMsg = struct {
    kind: EnumField = .zero,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "kind", .encoding = .varint },
    };
};

test "enum field roundtrip" {
    const alloc = testing.allocator;
    const original = EnumMsg{ .kind = .two };
    const bytes = try encode(EnumMsg, alloc, &original);
    defer alloc.free(bytes);
    var decoded = try decode(EnumMsg, alloc, bytes);
    defer deinit(EnumMsg, alloc, &decoded);
    try testing.expectEqual(EnumField.two, decoded.kind);
}

const SmallEnumField = enum(u1) {
    zero = 0,
    one = 1,
};

const SmallEnumMsg = struct {
    kind: SmallEnumField = .zero,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "kind", .encoding = .varint },
    };
};

test "enum rejects wire value outside tag type" {
    const bytes = [_]u8{ 0x08, 0x03 };
    try testing.expectError(error.InvalidEnumValue, decode(SmallEnumMsg, testing.allocator, &bytes));
}

test "enum preserves unknown in-range wire value" {
    const bytes = [_]u8{ 0x08, 0x63 };
    var decoded = try decode(EnumMsg, testing.allocator, &bytes);
    defer deinit(EnumMsg, testing.allocator, &decoded);
    try testing.expectEqual(@as(i32, 99), @intFromEnum(decoded.kind));
}

const Inner = struct {
    value: i64 = 0,
    label: []const u8 = "",

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "value", .encoding = .varint },
        .{ .field_num = 2, .name = "label", .encoding = .string },
    };
};

const Outer = struct {
    id: i32 = 0,
    inner: Inner = .{},
    items: []Inner = &.{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "id", .encoding = .varint },
        .{ .field_num = 2, .name = "inner", .encoding = .submessage },
        .{ .field_num = 3, .name = "items", .encoding = .repeated_submessage },
    };
};

test "submessage and repeated submessage roundtrip" {
    const alloc = testing.allocator;
    const items_in = [_]Inner{
        .{ .value = 10, .label = "a" },
        .{ .value = 20, .label = "bb" },
        .{ .value = 30, .label = "ccc" },
    };
    const original = Outer{
        .id = 7,
        .inner = .{ .value = 42, .label = "answer" },
        .items = @constCast(items_in[0..]),
    };

    const bytes = try encode(Outer, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(Outer, alloc, bytes);
    defer deinit(Outer, alloc, &decoded);

    try testing.expectEqual(@as(i32, 7), decoded.id);
    try testing.expectEqual(@as(i64, 42), decoded.inner.value);
    try testing.expectEqualStrings("answer", decoded.inner.label);
    try testing.expectEqual(@as(usize, 3), decoded.items.len);
    for (items_in, decoded.items) |expected, got| {
        try testing.expectEqual(expected.value, got.value);
        try testing.expectEqualStrings(expected.label, got.label);
    }
}

const StringsMsg = struct {
    tags: [][]const u8 = &.{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "tags", .encoding = .repeated_string },
    };
};

test "repeated string roundtrip" {
    const alloc = testing.allocator;
    const tags_in = [_][]const u8{ "alpha", "beta", "gamma" };
    const original = StringsMsg{ .tags = @constCast(tags_in[0..]) };

    const bytes = try encode(StringsMsg, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(StringsMsg, alloc, bytes);
    defer deinit(StringsMsg, alloc, &decoded);

    try testing.expectEqual(@as(usize, 3), decoded.tags.len);
    try testing.expectEqualStrings("alpha", decoded.tags[0]);
    try testing.expectEqualStrings("beta", decoded.tags[1]);
    try testing.expectEqualStrings("gamma", decoded.tags[2]);
}

test "repeated varint accepts non-packed wire" {
    const alloc = testing.allocator;
    // Manually build a payload with three non-packed varint entries for
    // field 1 (wire type 0): tag=0x08 repeated.
    var buf: Buf = .empty;
    defer buf.deinit(alloc);
    try wire.writeTag(alloc, &buf, 1, .varint);
    try wire.writeVarint(alloc, &buf, 11);
    try wire.writeTag(alloc, &buf, 1, .varint);
    try wire.writeVarint(alloc, &buf, 22);
    try wire.writeTag(alloc, &buf, 1, .varint);
    try wire.writeVarint(alloc, &buf, 33);

    const NonPacked = struct {
        values: []u64 = &.{},
        pub const _pb_field_map = [_]FieldDesc{
            .{ .field_num = 1, .name = "values", .encoding = .repeated_varint },
        };
    };

    var decoded = try decode(NonPacked, alloc, buf.items);
    defer deinit(NonPacked, alloc, &decoded);

    try testing.expectEqualSlices(u64, &[_]u64{ 11, 22, 33 }, decoded.values);
}

test "unknown fields are skipped" {
    const alloc = testing.allocator;
    // Build bytes with known field 1 and unknown field 999.
    var buf: Buf = .empty;
    defer buf.deinit(alloc);
    try wire.writeUint64(alloc, &buf, 1, 42);
    try wire.writeString(alloc, &buf, 999, "ignored");
    try wire.writeUint64(alloc, &buf, 4, 100);

    var decoded = try decode(ScalarMsg, alloc, buf.items);
    defer deinit(ScalarMsg, alloc, &decoded);

    try testing.expectEqual(@as(i32, 42), decoded.a);
    try testing.expectEqual(@as(u64, 100), decoded.d);
}

// -- Raw packed fields -----------------------------------------------------

const RawPackedMsg = struct {
    // Raw packed fixed32 — e.g. ONNX TensorProto.float_data stored as bytes.
    floats_raw: []const u8 = "",
    // Raw packed fixed64.
    doubles_raw: []const u8 = "",
    // Raw packed varint.
    ints_raw: []const u8 = "",

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "floats_raw", .encoding = .packed_raw_fixed32 },
        .{ .field_num = 2, .name = "doubles_raw", .encoding = .packed_raw_fixed64 },
        .{ .field_num = 3, .name = "ints_raw", .encoding = .packed_raw_varint },
    };
};

test "packed_raw fields are zero-copy on decode" {
    const alloc = testing.allocator;

    // Craft a message by hand so we know exactly what bytes end up on the wire.
    var buf: Buf = .empty;
    defer buf.deinit(alloc);
    const floats_raw = [_]u8{
        0x00, 0x00, 0x80, 0x3f, // 1.0
        0x00, 0x00, 0x00, 0x40, // 2.0
        0x00, 0x00, 0x40, 0x40, // 3.0
    };
    const doubles_raw = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // 1.0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // 2.0
    };
    // Varint payload for values 1, 2, 300: 0x01, 0x02, 0xac, 0x02
    const ints_raw = [_]u8{ 0x01, 0x02, 0xac, 0x02 };

    const original = RawPackedMsg{
        .floats_raw = &floats_raw,
        .doubles_raw = &doubles_raw,
        .ints_raw = &ints_raw,
    };

    const bytes = try encode(RawPackedMsg, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(RawPackedMsg, alloc, bytes);
    defer deinit(RawPackedMsg, alloc, &decoded);

    try testing.expectEqualSlices(u8, &floats_raw, decoded.floats_raw);
    try testing.expectEqualSlices(u8, &doubles_raw, decoded.doubles_raw);
    try testing.expectEqualSlices(u8, &ints_raw, decoded.ints_raw);

    // Prove zero-copy: decoded slices point into the input buffer.
    const bytes_start = @intFromPtr(bytes.ptr);
    const bytes_end = bytes_start + bytes.len;
    const floats_start = @intFromPtr(decoded.floats_raw.ptr);
    try testing.expect(floats_start >= bytes_start and floats_start < bytes_end);
}

// -- Lazy repeated submessage ---------------------------------------------

const LazyOuter = struct {
    id: i32 = 0,
    // Each element is pre-encoded Inner bytes. Decoded slices borrow from
    // the input buffer; encoding writes each as its own length-delimited record.
    items_bytes: [][]const u8 = &.{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "id", .encoding = .varint },
        .{ .field_num = 2, .name = "items_bytes", .encoding = .lazy_repeated_submessage },
    };
};

test "lazy_repeated_submessage roundtrip" {
    const alloc = testing.allocator;

    // Pre-encode three Inner messages.
    const inner_a = Inner{ .value = 10, .label = "a" };
    const inner_b = Inner{ .value = 20, .label = "bb" };
    const inner_c = Inner{ .value = 30, .label = "ccc" };
    const a_bytes = try encode(Inner, alloc, &inner_a);
    defer alloc.free(a_bytes);
    const b_bytes = try encode(Inner, alloc, &inner_b);
    defer alloc.free(b_bytes);
    const c_bytes = try encode(Inner, alloc, &inner_c);
    defer alloc.free(c_bytes);

    const items = [_][]const u8{ a_bytes, b_bytes, c_bytes };
    const original = LazyOuter{
        .id = 42,
        .items_bytes = @constCast(items[0..]),
    };

    const bytes = try encode(LazyOuter, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(LazyOuter, alloc, bytes);
    defer deinit(LazyOuter, alloc, &decoded);

    try testing.expectEqual(@as(i32, 42), decoded.id);
    try testing.expectEqual(@as(usize, 3), decoded.items_bytes.len);

    // Parse lazily.
    for (decoded.items_bytes, [_]Inner{ inner_a, inner_b, inner_c }) |raw, expected| {
        var parsed = try decode(Inner, alloc, raw);
        defer deinit(Inner, alloc, &parsed);
        try testing.expectEqual(expected.value, parsed.value);
        try testing.expectEqualStrings(expected.label, parsed.label);
    }
}

// -- always_emit for proto2 non-zero defaults ------------------------------

const Proto2DefaultsMsg = struct {
    // proto2: optional bool flag = 1 [default = true];
    flag: bool = true,
    // proto2: optional int32 count = 2 [default = 5];
    count: i32 = 5,
    // proto2: optional string name = 3 [default = "hello"];
    name: []const u8 = "hello",

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "flag", .encoding = .varint, .always_emit = true },
        .{ .field_num = 2, .name = "count", .encoding = .varint, .always_emit = true },
        .{ .field_num = 3, .name = "name", .encoding = .string, .always_emit = true },
    };
};

test "always_emit encodes non-zero proto2 defaults set to Zig zero" {
    const alloc = testing.allocator;

    // Explicitly set the fields to Zig zero values. A proto2 reader expects
    // to see the wire bytes so it can distinguish "unset (→ use default=true)"
    // from "explicitly set to false".
    const original = Proto2DefaultsMsg{
        .flag = false,
        .count = 0,
        .name = "",
    };

    const bytes = try encode(Proto2DefaultsMsg, alloc, &original);
    defer alloc.free(bytes);

    // All three fields must be present on the wire.
    try testing.expect(bytes.len > 0);

    var decoded = try decode(Proto2DefaultsMsg, alloc, bytes);
    defer deinit(Proto2DefaultsMsg, alloc, &decoded);

    try testing.expectEqual(false, decoded.flag);
    try testing.expectEqual(@as(i32, 0), decoded.count);
    try testing.expectEqualStrings("", decoded.name);
}

test "always_emit still encodes non-default values normally" {
    const alloc = testing.allocator;

    const original = Proto2DefaultsMsg{
        .flag = true,
        .count = 42,
        .name = "world",
    };

    const bytes = try encode(Proto2DefaultsMsg, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(Proto2DefaultsMsg, alloc, bytes);
    defer deinit(Proto2DefaultsMsg, alloc, &decoded);

    try testing.expectEqual(true, decoded.flag);
    try testing.expectEqual(@as(i32, 42), decoded.count);
    try testing.expectEqualStrings("world", decoded.name);
}

// -- Optional scalar fields (proto2 oneof / explicit presence) -------------

const OptionalScalarMsg = struct {
    // ONNX-style: dim_value is in a `oneof`, so null = unset.
    dim_value: ?i64 = null,
    // Optional float with presence.
    weight: ?f32 = null,
    // Optional zigzag signed.
    delta: ?i32 = null,
    // Optional unsigned fixed64.
    id64: ?u64 = null,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "dim_value", .encoding = .varint },
        .{ .field_num = 2, .name = "weight", .encoding = .fixed32 },
        .{ .field_num = 3, .name = "delta", .encoding = .sint },
        .{ .field_num = 4, .name = "id64", .encoding = .fixed64 },
    };
};

test "optional scalar: null skips encoding" {
    const alloc = testing.allocator;
    const original = OptionalScalarMsg{};
    const bytes = try encode(OptionalScalarMsg, alloc, &original);
    defer alloc.free(bytes);
    try testing.expectEqual(@as(usize, 0), bytes.len);
}

test "optional scalar: explicit zero still encodes (presence)" {
    const alloc = testing.allocator;
    // Each field is explicitly set to its Zig zero; presence means we must encode.
    const original = OptionalScalarMsg{
        .dim_value = 0,
        .weight = 0.0,
        .delta = 0,
        .id64 = 0,
    };
    const bytes = try encode(OptionalScalarMsg, alloc, &original);
    defer alloc.free(bytes);
    try testing.expect(bytes.len > 0);

    var decoded = try decode(OptionalScalarMsg, alloc, bytes);
    defer deinit(OptionalScalarMsg, alloc, &decoded);

    try testing.expectEqual(@as(?i64, 0), decoded.dim_value);
    try testing.expectEqual(@as(?f32, 0.0), decoded.weight);
    try testing.expectEqual(@as(?i32, 0), decoded.delta);
    try testing.expectEqual(@as(?u64, 0), decoded.id64);
}

test "optional scalar: non-zero roundtrip" {
    const alloc = testing.allocator;
    const original = OptionalScalarMsg{
        .dim_value = -42,
        .weight = 3.14,
        .delta = -1_000_000,
        .id64 = 0xDEADBEEF_CAFEBABE,
    };
    const bytes = try encode(OptionalScalarMsg, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(OptionalScalarMsg, alloc, bytes);
    defer deinit(OptionalScalarMsg, alloc, &decoded);

    try testing.expectEqual(@as(?i64, -42), decoded.dim_value);
    try testing.expectEqual(@as(?f32, 3.14), decoded.weight);
    try testing.expectEqual(@as(?i32, -1_000_000), decoded.delta);
    try testing.expectEqual(@as(?u64, 0xDEADBEEF_CAFEBABE), decoded.id64);
}

// -- Optional strings (oneof string alternatives) -------------------------

const OptionalStringMsg = struct {
    // Oneof alternative: name set with any value (including "") means present;
    // null means the oneof chose a different alternative.
    name: ?[]const u8 = null,
    // Regular (non-optional) string for comparison: empty skips encoding.
    label: []const u8 = "",

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        .{ .field_num = 2, .name = "label", .encoding = .string },
    };
};

test "optional string: null skips encoding" {
    const alloc = testing.allocator;
    const original = OptionalStringMsg{};
    const bytes = try encode(OptionalStringMsg, alloc, &original);
    defer alloc.free(bytes);
    try testing.expectEqual(@as(usize, 0), bytes.len);
}

test "optional string: empty-but-set still encodes" {
    const alloc = testing.allocator;
    // Explicit empty string — presence semantics require a zero-length record.
    const original = OptionalStringMsg{ .name = "" };
    const bytes = try encode(OptionalStringMsg, alloc, &original);
    defer alloc.free(bytes);
    try testing.expect(bytes.len > 0);

    var decoded = try decode(OptionalStringMsg, alloc, bytes);
    defer deinit(OptionalStringMsg, alloc, &decoded);
    try testing.expect(decoded.name != null);
    try testing.expectEqualStrings("", decoded.name.?);
}

test "optional string: non-empty roundtrip" {
    const alloc = testing.allocator;
    const original = OptionalStringMsg{
        .name = "hello",
        .label = "world",
    };
    const bytes = try encode(OptionalStringMsg, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(OptionalStringMsg, alloc, bytes);
    defer deinit(OptionalStringMsg, alloc, &decoded);
    try testing.expectEqualStrings("hello", decoded.name.?);
    try testing.expectEqualStrings("world", decoded.label);
}

test "optional scalar: partially set" {
    const alloc = testing.allocator;
    const original = OptionalScalarMsg{
        .dim_value = 128,
        // weight, delta, id64 all null → skipped
    };
    const bytes = try encode(OptionalScalarMsg, alloc, &original);
    defer alloc.free(bytes);

    var decoded = try decode(OptionalScalarMsg, alloc, bytes);
    defer deinit(OptionalScalarMsg, alloc, &decoded);

    try testing.expectEqual(@as(?i64, 128), decoded.dim_value);
    try testing.expectEqual(@as(?f32, null), decoded.weight);
    try testing.expectEqual(@as(?i32, null), decoded.delta);
    try testing.expectEqual(@as(?u64, null), decoded.id64);
}
