// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Fail-closed contracts for the private native data plane between static
//! runtime archives. The public control plane remains C-layout-only. Native
//! payloads are permitted solely because every archive is hidden, statically
//! linked, and produced by one target/toolchain invocation; these descriptors
//! prevent an incompatible payload from ever reaching a cast.

const std = @import("std");
const builtin = @import("builtin");

pub const abi_version: u32 = 2;
pub const zig_compiler_id: u64 = stableId(builtin.zig_version_string);

pub const TypeContract = extern struct {
    version: u32 = abi_version,
    alignment: u32,
    size: u64,
    bit_size: u64,
    type_id: u64,
    layout_id: u64,
    compiler_id: u64 = zig_compiler_id,

    pub fn of(comptime T: type) TypeContract {
        return .{
            .alignment = @alignOf(T),
            .size = @sizeOf(T),
            .bit_size = @sizeOf(T) * 8,
            .type_id = stableId(@typeName(T)),
            .layout_id = shallowLayoutId(T),
        };
    }

    pub fn matches(self: TypeContract, expected: TypeContract) bool {
        return self.version == abi_version and
            self.version == expected.version and
            self.alignment == expected.alignment and
            self.size == expected.size and
            self.bit_size == expected.bit_size and
            self.type_id == expected.type_id and
            self.layout_id == expected.layout_id and
            self.compiler_id == zig_compiler_id and
            self.compiler_id == expected.compiler_id;
    }
};

/// Versioned, same-toolchain borrow of a host-owned executor. The receiver may
/// copy the std.Io interface but must not retain it beyond the host's declared
/// lease lifetime and must never deinitialize it.
pub const IoBorrow = extern struct {
    pointer: *const anyopaque,
    contract: TypeContract,

    pub fn init(io: *const std.Io) IoBorrow {
        return .{ .pointer = io, .contract = .of(std.Io) };
    }

    pub fn get(self: IoBorrow) !std.Io {
        if (!self.contract.matches(.of(std.Io))) return error.InvalidArgument;
        if (@intFromPtr(self.pointer) % @alignOf(std.Io) != 0) return error.InvalidArgument;
        const io: *const std.Io = @ptrCast(@alignCast(self.pointer));
        return io.*;
    }
};

pub const CallContract = extern struct {
    version: u32 = abi_version,
    _reserved: u32 = 0,
    method_id: u64,
    function: TypeContract,
    arguments: TypeContract,
    output: TypeContract,

    pub fn of(
        comptime method_name: []const u8,
        comptime Function: type,
        comptime Arguments: type,
        comptime Output: type,
    ) CallContract {
        return .{
            .method_id = stableId(method_name),
            .function = .of(Function),
            .arguments = .of(Arguments),
            .output = .of(Output),
        };
    }

    pub fn matches(self: CallContract, expected: CallContract) bool {
        return self.version == abi_version and
            self.version == expected.version and
            self._reserved == 0 and
            self.method_id == expected.method_id and
            self.function.matches(expected.function) and
            self.arguments.matches(expected.arguments) and
            self.output.matches(expected.output);
    }
};

pub fn stableId(comptime name: []const u8) u64 {
    // FNV-1a is deliberately simple and reproducible in every compilation
    // unit. Type size/alignment/bit-size are checked independently.
    var hash: u64 = 0xcbf29ce484222325;
    for (name) |byte| {
        hash ^= byte;
        hash *%= 0x100000001b3;
    }
    return hash;
}

/// Fingerprint the immediate representation of a transported type without
/// recursively expanding its full dependency graph. The Zig build cache
/// already keys every archive on transitive source inputs; this contract adds
/// a cheap runtime guard for field order, offsets, tags, calling conventions,
/// and exact compiler identity. Keeping the fingerprint shallow is deliberate:
/// recursive reflection over the large query/storage type graph materially
/// increases analysis and code-generation time in every runtime unit.
pub fn shallowLayoutId(comptime T: type) u64 {
    var hash: u64 = stableId(@tagName(@typeInfo(T)));
    hashInteger(&hash, @sizeOf(T));
    hashInteger(&hash, @alignOf(T));
    hashInteger(&hash, @sizeOf(T) * 8);
    switch (@typeInfo(T)) {
        .@"struct" => |structure| {
            hashBytes(&hash, @tagName(structure.layout));
            inline for (structure.field_names, structure.field_types) |field_name, Field| {
                hashBytes(&hash, field_name);
                hashBytes(&hash, @typeName(Field));
                hashInteger(&hash, @offsetOf(T, field_name));
                hashInteger(&hash, @sizeOf(Field));
                hashInteger(&hash, @alignOf(Field));
            }
        },
        .@"union" => |value_union| {
            hashBytes(&hash, @tagName(value_union.layout));
            if (value_union.tag_type) |Tag| hashBytes(&hash, @typeName(Tag));
            inline for (value_union.field_names, value_union.field_types) |field_name, Field| {
                hashBytes(&hash, field_name);
                hashBytes(&hash, @typeName(Field));
                hashInteger(&hash, @sizeOf(Field));
                hashInteger(&hash, @alignOf(Field));
            }
        },
        .@"enum" => |value_enum| {
            hashBytes(&hash, @typeName(value_enum.tag_type));
            inline for (value_enum.field_names, value_enum.field_values) |field_name, field_value| {
                hashBytes(&hash, field_name);
                hashInteger(&hash, field_value);
            }
        },
        .array => |array| {
            hashInteger(&hash, array.len);
            hashBytes(&hash, @typeName(array.child));
        },
        .vector => |vector| {
            hashInteger(&hash, vector.len);
            hashBytes(&hash, @typeName(vector.child));
        },
        .optional => |optional| hashBytes(&hash, @typeName(optional.child)),
        .error_union => |error_union| hashBytes(&hash, @typeName(error_union.payload)),
        .pointer => |pointer| {
            hashBytes(&hash, @tagName(pointer.size));
            hashInteger(&hash, pointer.attrs.@"align" orelse @alignOf(pointer.child));
            hashInteger(&hash, @intFromBool(pointer.attrs.@"const"));
            hashInteger(&hash, @intFromBool(pointer.attrs.@"volatile"));
            hashBytes(&hash, @typeName(pointer.child));
        },
        .@"fn" => |function| {
            hashBytes(&hash, @tagName(function.calling_convention));
            hashInteger(&hash, @intFromBool(function.is_var_args));
            hashBytes(&hash, @typeName(T));
        },
        else => {},
    }
    return hash;
}

fn hashBytes(hash: *u64, bytes: []const u8) void {
    for (bytes) |byte| {
        hash.* ^= byte;
        hash.* *%= 0x100000001b3;
    }
    hash.* ^= 0xff;
    hash.* *%= 0x100000001b3;
}

fn hashInteger(hash: *u64, comptime value: anytype) void {
    var remaining: u64 = @intCast(value);
    inline for (0..8) |_| {
        hash.* ^= @truncate(remaining);
        hash.* *%= 0x100000001b3;
        remaining >>= 8;
    }
}

pub fn assertUniqueMethodIds(comptime VTable: type) void {
    @setEvalBranchQuota(100_000);
    const field_names = @typeInfo(VTable).@"struct".field_names;
    inline for (field_names, 0..) |left, left_index| {
        inline for (field_names[left_index + 1 ..]) |right| {
            if (stableId(left) == stableId(right))
                @compileError("native ABI method-id collision between " ++ left ++ " and " ++ right);
        }
    }
}

test "native type contracts reject layout and identity mismatches" {
    const A = extern struct { value: u64 };
    const B = extern struct { value: i64 };
    const a = TypeContract.of(A);
    try std.testing.expect(a.matches(.of(A)));
    try std.testing.expect(!a.matches(.of(B)));

    var wrong_version = a;
    wrong_version.version += 1;
    try std.testing.expect(!wrong_version.matches(.of(A)));

    var wrong_compiler = a;
    wrong_compiler.compiler_id +%= 1;
    try std.testing.expect(!wrong_compiler.matches(.of(A)));
}

test "native shallow layout fingerprints include field order" {
    const Left = extern struct { first: u32, second: u64 };
    const Right = extern struct { second: u64, first: u32 };
    try std.testing.expect(shallowLayoutId(Left) != shallowLayoutId(Right));
}

test "native method identifiers are deterministic" {
    try std.testing.expectEqual(stableId("lookup"), stableId("lookup"));
    try std.testing.expect(stableId("lookup") != stableId("scan"));
}

test "native executor borrows validate before reconstructing std.Io" {
    const io = std.testing.io;
    const borrow = IoBorrow.init(&io);
    _ = try borrow.get();

    var incompatible = borrow;
    incompatible.contract.version += 1;
    try std.testing.expectError(error.InvalidArgument, incompatible.get());

    var misaligned = borrow;
    misaligned.pointer = @ptrFromInt(1);
    try std.testing.expectError(error.InvalidArgument, misaligned.get());
}
