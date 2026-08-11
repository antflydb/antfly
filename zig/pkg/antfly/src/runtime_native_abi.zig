// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Fail-closed contracts for the private native data plane between static
//! runtime archives. The public control plane remains C-layout-only. Native
//! payloads are permitted solely because every archive is hidden, statically
//! linked, and produced by one target/toolchain invocation; these descriptors
//! prevent an incompatible payload from ever reaching a cast.

const std = @import("std");

pub const abi_version: u32 = 1;

pub const TypeContract = extern struct {
    version: u32 = abi_version,
    alignment: u32,
    size: u64,
    bit_size: u64,
    type_id: u64,

    pub fn of(comptime T: type) TypeContract {
        return .{
            .alignment = @alignOf(T),
            .size = @sizeOf(T),
            .bit_size = @bitSizeOf(T),
            .type_id = stableId(@typeName(T)),
        };
    }

    pub fn matches(self: TypeContract, expected: TypeContract) bool {
        return self.version == abi_version and
            self.version == expected.version and
            self.alignment == expected.alignment and
            self.size == expected.size and
            self.bit_size == expected.bit_size and
            self.type_id == expected.type_id;
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

pub fn assertUniqueMethodIds(comptime VTable: type) void {
    @setEvalBranchQuota(100_000);
    const fields = std.meta.fields(VTable);
    inline for (fields, 0..) |left, left_index| {
        inline for (fields[left_index + 1 ..]) |right| {
            if (stableId(left.name) == stableId(right.name))
                @compileError("native ABI method-id collision between " ++ left.name ++ " and " ++ right.name);
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
}

test "native method identifiers are deterministic" {
    try std.testing.expectEqual(stableId("lookup"), stableId("lookup"));
    try std.testing.expect(stableId("lookup") != stableId("scan"));
}
