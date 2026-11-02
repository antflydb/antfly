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
fn signature(comptime family: []const u8) []const u8 {
    const marker = "#define ANTFLY_CUDA_" ++ family ++ "_PARAMS ";
    const start = std.mem.indexOf(u8, native_signatures, marker) orelse @compileError("unknown CUDA ABI family: " ++ family);
    const begin = start + std.mem.indexOfScalar(u8, native_signatures[start..], '\n').? + 1;
    const end = std.mem.indexOf(u8, native_signatures[begin..], "\n\n") orelse native_signatures.len - begin;
    return native_signatures[begin .. begin + end];
}
/// Derive named value records directly from the native parameter list. Device
/// addresses use u64 and scalar wires use u32/i32/f32. fieldsFor validates
/// and order-packs the final storage.
/// This is compile-time construction, with no per-launch parser or allocation.
pub fn ArgumentsFor(comptime family: []const u8) type {
    @setEvalBranchQuota(20000);
    const declarations = signature(family);
    const count = comptime blk: {
        var lines = std.mem.splitScalar(u8, declarations, '\n');
        var count: usize = 0;
        while (lines.next()) |raw| if (std.mem.trim(u8, raw, " \t\r\\,").len != 0) {
            count += 1;
        };
        break :blk count;
    };
    var names: [count][]const u8 = undefined;
    var types: [count]type = undefined;
    var lines = std.mem.splitScalar(u8, declarations, '\n');
    var index: usize = 0;
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r\\,");
        if (line.len == 0) continue;
        const split = std.mem.lastIndexOfScalar(u8, line, ' ').?;
        names[index] = line[split + 1 ..];
        const declaration = line[0..split];
        types[index] = if (std.mem.indexOfScalar(u8, declaration, '*') != null) u64 else if (std.mem.eql(u8, declaration, "unsigned int")) u32 else if (std.mem.eql(u8, declaration, "unsigned long long")) u64 else if (std.mem.eql(u8, declaration, "int")) i32 else if (std.mem.eql(u8, declaration, "float")) f32 else @compileError("unsupported native CUDA value type: " ++ declaration);
        index += 1;
    }
    return @Struct(.auto, null, &names, &types, &@splat(.{}));
}

fn matchesWire(comptime declaration: []const u8, comptime T: type) bool {
    if (std.mem.indexOfScalar(u8, declaration, '*') != null)
        return @sizeOf(T) == 8 and (T == u64 or @typeInfo(T) == .pointer or (@typeInfo(T) == .optional and @typeInfo(@typeInfo(T).optional.child) == .pointer));
    return if (std.mem.eql(u8, declaration, "unsigned int")) T == u32 else if (std.mem.eql(u8, declaration, "unsigned long long")) T == u64 else if (std.mem.eql(u8, declaration, "int")) T == i32 else if (std.mem.eql(u8, declaration, "float")) T == f32 else false;
}
pub inline fn fieldsFor(comptime family: []const u8, storage: anytype) [@typeInfo(@TypeOf(storage.*)).@"struct".field_names.len]?*anyopaque {
    @setEvalBranchQuota(20000);
    var result: [@typeInfo(@TypeOf(storage.*)).@"struct".field_names.len]?*anyopaque = undefined;
    comptime var lines = std.mem.splitScalar(u8, signature(family), '\n');
    comptime var index: usize = 0;
    inline while (comptime lines.next()) |raw| {
        const line = comptime std.mem.trim(u8, raw, " \t\r\\,");
        if (comptime line.len == 0) continue;
        const split = comptime std.mem.lastIndexOfScalar(u8, line, ' ').?;
        const name = comptime line[split + 1 ..];
        const T = @TypeOf(@field(storage.*, name));
        if (comptime !matchesWire(line[0..split], T)) @compileError("CUDA wire type mismatch: " ++ family ++ "." ++ name);
        result[index] = @ptrCast(&@field(storage.*, name));
        index += 1;
    }
    if (index != result.len) @compileError("CUDA launch field count differs from native ABI");
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
