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

//! CUDA copies argument values during cuLaunchKernel, including graph capture.
//! The slots must remain live through that call; their device-buffer lifetime
//! belongs to the request/frame. These helpers only build stack pointer tables.
const std = @import("std");

fn requireWire(comptime T: type) void {
    switch (@typeInfo(T)) {
        .int => |info| if (info.bits != 8 and info.bits != 16 and info.bits != 32 and info.bits != 64) @compileError("unsupported CUDA integer wire width"),
        .float => |info| if (info.bits != 16 and info.bits != 32 and info.bits != 64) @compileError("unsupported CUDA float wire width"),
        .bool => {},
        .pointer => |info| if (info.size == .slice) @compileError("CUDA wire pointers cannot be Zig slices"),
        .optional => |info| if (@typeInfo(info.child) != .pointer) @compileError("CUDA optional wire values must be pointers"),
        .array => |info| requireWire(info.child),
        .@"struct" => |info| {
            if (info.layout != .@"extern") @compileError("CUDA by-value records require extern layout");
            inline for (info.field_types) |field_type| requireWire(field_type);
        },
        else => @compileError("unsupported CUDA launch wire type"),
    }
}

/// Preserves the original slots, including callers that update them between
/// launches. No value copies, heap allocation, or runtime type dispatch.
pub inline fn addresses(slots: anytype) [@typeInfo(@TypeOf(slots)).@"struct".field_names.len]?*anyopaque {
    var result: [@typeInfo(@TypeOf(slots)).@"struct".field_names.len]?*anyopaque = undefined;
    inline for (@typeInfo(@TypeOf(slots)).@"struct".field_names, @typeInfo(@TypeOf(slots)).@"struct".field_types, 0..) |name, field_type, index| {
        const pointer = @typeInfo(field_type).pointer;
        if (pointer.size != .one or pointer.attrs.@"const") @compileError("CUDA arguments require mutable single-item slots");
        comptime requireWire(pointer.child);
        result[index] = @ptrCast(@field(slots, name));
    }
    return result;
}

/// Named records keep values and wire types together. Build the pointer table
/// after the record reaches its final stack location; never retain it after
/// the corresponding launch/capture call.
pub inline fn fields(storage: anytype) [@typeInfo(@TypeOf(storage.*)).@"struct".field_names.len]?*anyopaque {
    var result: [@typeInfo(@TypeOf(storage.*)).@"struct".field_names.len]?*anyopaque = undefined;
    inline for (@typeInfo(@TypeOf(storage.*)).@"struct".field_names, @typeInfo(@TypeOf(storage.*)).@"struct".field_types, 0..) |name, field_type, index| {
        comptime requireWire(field_type);
        result[index] = @ptrCast(&@field(storage.*, name));
    }
    return result;
}

// The native CUDA parameter-list header remains the single signature owner.
// This consumer validates named host slots and orders them from that header;
// storage formats/extent checks continue to belong to the native launcher.
const native_signatures = @embedFile("kernels/kernel_abi.cuh");
const NativeWire = enum { u32, u64, i32, f32 };
const NativeSlot = struct {
    name: []const u8,
    declaration: []const u8,
    native_wire: NativeWire,
    device_pointer: bool,
};
// Every expensive scan/split/type construction is a family container constant.
// Both public consumers reference this exact cached slot shape. Inline launch
// recipes only visit the parsed slots; they never re-read signature strings.
fn Family(comptime family: []const u8) type {
    return struct {
        const text = parseSignature(family);
        const count = parameterCount(text);
        const slots = parseSlots(text, count);
        const Arguments = argumentsFromSlots(slots);
    };
}
fn parseSignature(comptime family: []const u8) []const u8 {
    @setEvalBranchQuota(20000);
    const marker = "#define ANTFLY_CUDA_" ++ family ++ "_PARAMS ";
    const start = std.mem.indexOf(u8, native_signatures, marker) orelse @compileError("unknown CUDA ABI family: " ++ family);
    const begin = start + std.mem.indexOfScalar(u8, native_signatures[start..], '\n').? + 1;
    const end = std.mem.indexOf(u8, native_signatures[begin..], "\n\n") orelse native_signatures.len - begin;
    return native_signatures[begin .. begin + end];
}
fn parameterCount(comptime declarations: []const u8) usize {
    @setEvalBranchQuota(20000);
    var lines = std.mem.splitScalar(u8, declarations, '\n');
    var count: usize = 0;
    while (lines.next()) |raw| if (std.mem.trim(u8, raw, " \t\r\\,").len != 0) {
        count += 1;
    };
    return count;
}
fn parseSlots(comptime declarations: []const u8, comptime count: usize) [count]NativeSlot {
    @setEvalBranchQuota(20000);
    var result: [count]NativeSlot = undefined;
    var lines = std.mem.splitScalar(u8, declarations, '\n');
    var index: usize = 0;
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r\\,");
        if (line.len == 0) continue;
        const split = std.mem.lastIndexOfScalar(u8, line, ' ').?;
        const declaration = line[0..split];
        const is_pointer = std.mem.indexOfScalar(u8, declaration, '*') != null;
        result[index] = .{
            .name = line[split + 1 ..],
            .declaration = declaration,
            .native_wire = if (is_pointer) .u64 else if (std.mem.eql(u8, declaration, "unsigned int")) .u32 else if (std.mem.eql(u8, declaration, "unsigned long long")) .u64 else if (std.mem.eql(u8, declaration, "int")) .i32 else if (std.mem.eql(u8, declaration, "float")) .f32 else @compileError("unsupported native CUDA value type: " ++ declaration),
            .device_pointer = is_pointer,
        };
        index += 1;
    }
    std.debug.assert(index == count);
    return result;
}
fn valueType(comptime wire: NativeWire) type {
    return switch (wire) {
        .u32 => u32,
        .u64 => u64,
        .i32 => i32,
        .f32 => f32,
    };
}
fn argumentsFromSlots(comptime slots: anytype) type {
    var names: [slots.len][]const u8 = undefined;
    var types: [slots.len]type = undefined;
    for (slots, 0..) |slot, index| {
        names[index] = slot.name;
        types[index] = valueType(slot.native_wire);
    }
    return @Struct(.auto, null, &names, &types, &@splat(.{}));
}
/// Derive named value records directly from the native parameter list. Device
/// addresses use u64 while scalar u64 remains a scalar contract. Only the cold
/// cached Family initializer parses declaration text or constructs this type.
pub fn ArgumentsFor(comptime family: []const u8) type {
    return Family(family).Arguments;
}
fn matchesWire(comptime slot: NativeSlot, comptime T: type) bool {
    if (slot.device_pointer)
        return @sizeOf(T) == 8 and (T == u64 or @typeInfo(T) == .pointer or (@typeInfo(T) == .optional and @typeInfo(@typeInfo(T).optional.child) == .pointer));
    return T == valueType(slot.native_wire);
}
pub inline fn fieldsFor(comptime family: []const u8, storage: anytype) [@typeInfo(@TypeOf(storage.*)).@"struct".field_names.len]?*anyopaque {
    const slots = comptime Family(family).slots;
    var result: [@typeInfo(@TypeOf(storage.*)).@"struct".field_names.len]?*anyopaque = undefined;
    if (slots.len != result.len) @compileError("CUDA launch field count differs from native ABI");
    inline for (slots, 0..) |slot, index| {
        const T = @TypeOf(@field(storage.*, slot.name));
        if (comptime !matchesWire(slot, T)) @compileError("CUDA wire type mismatch: " ++ family ++ "." ++ slot.name);
        result[index] = @ptrCast(&@field(storage.*, slot.name));
    }
    return result;
}

test "CUDA launch ABI native signature orders named fields independently of record layout" {
    var values = struct { rows: u32, weight: u64, out_dim: u32, dst: u64, in_dim: u32, input: u64 }{ .rows = 7, .weight = 0x3333, .out_dim = 13, .dst = 0x1111, .in_dim = 11, .input = 0x2222 };
    const table = fieldsFor("QUANT_LINEAR", &values);
    try std.testing.expectEqual(@as(u64, 0x1111), @as(*u64, @ptrCast(@alignCast(table[0].?))).*);
    try std.testing.expectEqual(@as(u64, 0x2222), @as(*u64, @ptrCast(@alignCast(table[1].?))).*);
    try std.testing.expectEqual(@as(u64, 0x3333), @as(*u64, @ptrCast(@alignCast(table[2].?))).*);
    try std.testing.expectEqual(@as(u32, 7), @as(*u32, @ptrCast(@alignCast(table[3].?))).*);
    values.rows = 19;
    try std.testing.expectEqual(@as(u32, 19), @as(*u32, @ptrCast(@alignCast(table[3].?))).*);
    try std.testing.expectEqual(@as(u32, 11), @as(*u32, @ptrCast(@alignCast(table[4].?))).*);
    try std.testing.expectEqual(@as(u32, 13), @as(*u32, @ptrCast(@alignCast(table[5].?))).*);
}

test "CUDA launch ABI preserves wire order addresses and live scalar updates" {
    var pointer: u64 = 0x1020304050607080;
    var count: u32 = 0x12345678;
    var scale: f32 = -0.125;
    const table = addresses(.{ &pointer, &count, &scale });
    try std.testing.expectEqual(@intFromPtr(&pointer), @intFromPtr(table[0].?));
    try std.testing.expectEqual(@intFromPtr(&count), @intFromPtr(table[1].?));
    try std.testing.expectEqual(@intFromPtr(&scale), @intFromPtr(table[2].?));
    count = 19;
    try std.testing.expectEqual(@as(u32, 19), @as(*u32, @ptrCast(@alignCast(table[1].?))).*);
    var record = .{ .destination = pointer, .count = count, .scale = scale };
    const named = fields(&record);
    try std.testing.expectEqual(pointer, @as(*u64, @ptrCast(@alignCast(named[0].?))).*);
    try std.testing.expectEqual(count, @as(*u32, @ptrCast(@alignCast(named[1].?))).*);
    try std.testing.expectEqual(scale, @as(*f32, @ptrCast(@alignCast(named[2].?))).*);
}

test "CUDA launch ABI native-derived records preserve live slots and wire widths" {
    var arguments = std.mem.zeroes(ArgumentsFor("TQ_SPLIT_STAGE1"));
    arguments.partial_acc = 0x123456789abcdef0;
    arguments.chunk_count = 7;
    arguments.decode_scalars = 0x7777;
    const slots = fieldsFor("TQ_SPLIT_STAGE1", &arguments);
    try std.testing.expectEqual(@as(usize, 23), slots.len);
    try std.testing.expectEqual(@as(u64, 0x123456789abcdef0), @as(*u64, @ptrCast(@alignCast(slots[0].?))).*);
    try std.testing.expectEqual(@as(u64, 0x7777), @as(*u64, @ptrCast(@alignCast(slots[20].?))).*);
    arguments.chunk_count = 19;
    try std.testing.expectEqual(@as(u32, 19), @as(*u32, @ptrCast(@alignCast(slots[22].?))).*);
    try std.testing.expectEqual(u32, @FieldType(ArgumentsFor("QUANT_QKV"), "kv_out_dim"));
    try std.testing.expectEqual(u64, @FieldType(ArgumentsFor("Q8_INPUT_QKV"), "q8_input"));
}

test "CUDA launch ABI keeps both words of native expert strides live" {
    var arguments = std.mem.zeroes(ArgumentsFor("GEMMA4_A4B_Q4_0_DOWN_ROWS_Q8_1_F32"));
    arguments.down_expert_stride = 0x12345678abcdef01;
    const table = fieldsFor("GEMMA4_A4B_Q4_0_DOWN_ROWS_Q8_1_F32", &arguments);
    const stride: *u64 = @ptrCast(@alignCast(table[4].?));
    try std.testing.expectEqual(@intFromPtr(&arguments.down_expert_stride), @intFromPtr(stride));
    try std.testing.expectEqual(@as(u64, 0x12345678abcdef01), stride.*);
    arguments.down_expert_stride = 0xfedcba9876543210;
    try std.testing.expectEqual(@as(u64, 0xfedcba9876543210), stride.*);
}

test "CUDA cached ABI slots preserve scalar u64 versus device pointer contracts" {
    const down = comptime Family("GEMMA4_A4B_Q4_0_DOWN_ROWS_Q8_1_F32").slots;
    try std.testing.expectEqualStrings("down_expert_stride", down[4].name);
    try std.testing.expectEqualStrings("unsigned long long", down[4].declaration);
    try std.testing.expectEqual(NativeWire.u64, down[4].native_wire);
    try std.testing.expect(!down[4].device_pointer);
    try std.testing.expect(matchesWire(down[4], u64));
    try std.testing.expect(!matchesWire(down[4], *u64));
    try std.testing.expect(down[0].device_pointer);
    try std.testing.expect(matchesWire(down[0], u64));
    try std.testing.expect(matchesWire(down[0], ?*anyopaque));
    try std.testing.expect(!matchesWire(down[0], u32));
    var record = struct { dst: ?*anyopaque, input: *const anyopaque, weight: u64, rows: u32, in_dim: u32, out_dim: u32 }{ .dst = @ptrFromInt(0x1100), .input = @ptrFromInt(0x2200), .weight = 0x3300, .rows = 2, .in_dim = 3, .out_dim = 4 };
    const table = fieldsFor("QUANT_LINEAR", &record);
    try std.testing.expectEqual(@intFromPtr(&record.dst), @intFromPtr(table[0].?));
    try std.testing.expectEqual(@intFromPtr(&record.input), @intFromPtr(table[1].?));
    record.dst = @ptrFromInt(0x4400);
    const live: *?*anyopaque = @ptrCast(@alignCast(table[0].?));
    try std.testing.expectEqual(@as(usize, 0x4400), @intFromPtr(live.*.?));
}
test "CUDA cached ABI slots bound repeated inline recipe packing without rescanning text" {
    @setEvalBranchQuota(20000);
    const Recipe = struct {
        inline fn pack(storage: *ArgumentsFor("Q8_INPUT_LINEAR")) [6]?*anyopaque {
            return fieldsFor("Q8_INPUT_LINEAR", storage);
        }
    };
    var arguments = std.mem.zeroes(ArgumentsFor("Q8_INPUT_LINEAR"));
    inline for (0..64) |index| {
        const table = Recipe.pack(&arguments);
        try std.testing.expectEqual(@intFromPtr(&arguments.rows), @intFromPtr(table[3].?));
        arguments.rows = @intCast(index + 1);
        try std.testing.expectEqual(@as(u32, @intCast(index + 1)), @as(*u32, @ptrCast(@alignCast(table[3].?))).*);
    }
}
