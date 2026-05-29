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

const std = @import("std");
const safetensors = @import("../models/safetensors.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const DType = @import("../backends/tensor.zig").DType;

pub fn convertToF32(
    allocator: std.mem.Allocator,
    dtype: DType,
    raw: []const u8,
    n_elements: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, n_elements);
    errdefer allocator.free(out);

    switch (dtype) {
        .f32 => {
            @memcpy(std.mem.sliceAsBytes(out), raw[0 .. n_elements * 4]);
        },
        .f16 => {
            for (0..n_elements) |i| {
                const bits = std.mem.readInt(u16, raw[i * 2 ..][0..2], .little);
                const f16_val: f16 = @bitCast(bits);
                out[i] = @floatCast(f16_val);
            }
        },
        .bf16 => {
            for (0..n_elements) |i| {
                const bits = std.mem.readInt(u16, raw[i * 2 ..][0..2], .little);
                out[i] = @bitCast(@as(u32, bits) << 16);
            }
        },
        else => return error.UnsupportedDType,
    }
    return out;
}

pub fn copyToF16(
    allocator: std.mem.Allocator,
    raw: []const u8,
    n_elements: usize,
) ![]f16 {
    const out = try allocator.alloc(f16, n_elements);
    errdefer allocator.free(out);
    for (0..n_elements) |i| {
        const bits = std.mem.readInt(u16, raw[i * 2 ..][0..2], .little);
        out[i] = @bitCast(bits);
    }
    return out;
}

pub fn registerSafetensorsWeight(
    allocator: std.mem.Allocator,
    compute: *wasm_compute.WasmCompute,
    name: []const u8,
    dtype: DType,
    raw: []const u8,
    n_elements: usize,
) !void {
    if (dtype == .f16) {
        const f16_data = try copyToF16(allocator, raw, n_elements);
        const owned_name = try allocator.dupe(u8, name);
        compute.registerF16Weight(owned_name, f16_data);
    } else {
        const f32_data = try convertToF32(allocator, dtype, raw, n_elements);
        const owned_name = try allocator.dupe(u8, name);
        compute.registerWeight(owned_name, f32_data);
    }
}

pub fn loadSafetensorsWeights(
    allocator: std.mem.Allocator,
    compute: *wasm_compute.WasmCompute,
    st_data: []const u8,
    prefix_strip: ?[]const u8,
) !void {
    const result = try safetensors.parseHeader(allocator, st_data);
    var header = result.header;
    defer header.deinit();
    const data_offset = result.data_offset;

    var it = header.tensors.iterator();
    while (it.next()) |entry| {
        const name = entry.key_ptr.*;
        const meta = entry.value_ptr.*;

        if (std.mem.endsWith(u8, name, ".position_ids")) continue;
        if (std.mem.endsWith(u8, name, ".token_type_ids")) continue;

        const abs_start: usize = @intCast(data_offset + meta.data_start);
        const abs_end: usize = @intCast(data_offset + meta.data_end);
        if (abs_end > st_data.len) continue;
        const raw = st_data[abs_start..abs_end];

        const n_elements = blk: {
            var count: usize = 1;
            for (meta.shape) |dim| count *= @intCast(dim);
            break :blk count;
        };

        const stripped = if (prefix_strip) |prefix|
            (if (std.mem.startsWith(u8, name, prefix)) name[prefix.len..] else name)
        else
            name;
        registerSafetensorsWeight(allocator, compute, stripped, meta.dtype, raw, n_elements) catch continue;
    }
}
