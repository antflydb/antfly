// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Layout-checked references for synchronously borrowed Lite SQL values.
//! The caller owns each typed value for the duration of a provider call; the
//! provider validates the descriptor before interpreting it.

const std = @import("std");

pub const abi_version: u32 = 1;

pub const Kind = enum(u32) {
    table_ddl = 1,
    insert_source = 2,
    read_plan = 3,
    ddl_result = 4,
    search_request = 5,
    table_schema = 6,
    rows_query_plan = 7,
    rows_query_result = 8,
    batch_request = 9,
    unique_selector_resolver = 10,
    default_value_context = 11,
    rows_batch_result = 12,
    read_plan_result = 13,
};

pub const Ref = extern struct {
    version: u32 = abi_version,
    kind: Kind,
    byte_size: usize,
    byte_alignment: usize,
    ptr: ?*const anyopaque,

    pub fn from(comptime T: type, kind: Kind, value: *const T) Ref {
        return .{
            .kind = kind,
            .byte_size = @sizeOf(T),
            .byte_alignment = @alignOf(T),
            .ptr = value,
        };
    }

    pub fn cast(self: Ref, comptime T: type, expected_kind: Kind) !*const T {
        if (self.version != abi_version) return error.InvalidLiteSqlValueAbiVersion;
        if (self.kind != expected_kind) return error.InvalidLiteSqlValueKind;
        if (self.byte_size != @sizeOf(T)) return error.InvalidLiteSqlValueSize;
        if (self.byte_alignment != @alignOf(T)) return error.InvalidLiteSqlValueAlignment;
        return @ptrCast(@alignCast(self.ptr orelse return error.InvalidLiteSqlValuePointer));
    }
};

pub const OutRef = extern struct {
    version: u32 = abi_version,
    kind: Kind,
    byte_size: usize,
    byte_alignment: usize,
    ptr: ?*anyopaque,

    pub fn from(comptime T: type, kind: Kind, value: *T) OutRef {
        return .{
            .kind = kind,
            .byte_size = @sizeOf(T),
            .byte_alignment = @alignOf(T),
            .ptr = value,
        };
    }

    pub fn cast(self: OutRef, comptime T: type, expected_kind: Kind) !*T {
        if (self.version != abi_version) return error.InvalidLiteSqlValueAbiVersion;
        if (self.kind != expected_kind) return error.InvalidLiteSqlValueKind;
        if (self.byte_size != @sizeOf(T)) return error.InvalidLiteSqlValueSize;
        if (self.byte_alignment != @alignOf(T)) return error.InvalidLiteSqlValueAlignment;
        return @ptrCast(@alignCast(self.ptr orelse return error.InvalidLiteSqlValuePointer));
    }
};

test "lite sql value references validate layout and kind" {
    const Example = struct { value: u64 };
    const value = Example{ .value = 42 };
    const ref = Ref.from(Example, .read_plan, &value);
    try std.testing.expectEqual(@as(u64, 42), (try ref.cast(Example, .read_plan)).value);
    try std.testing.expectError(error.InvalidLiteSqlValueKind, ref.cast(Example, .table_ddl));

    var invalid_size = ref;
    invalid_size.byte_size += 1;
    try std.testing.expectError(error.InvalidLiteSqlValueSize, invalid_size.cast(Example, .read_plan));

    var out: Example = undefined;
    const out_ref = OutRef.from(Example, .ddl_result, &out);
    (try out_ref.cast(Example, .ddl_result)).* = value;
    try std.testing.expectEqual(@as(u64, 42), out.value);
}
