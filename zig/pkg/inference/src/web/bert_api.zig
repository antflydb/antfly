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
const gguf = @import("../gguf/root.zig");
const bert_arch = @import("../architectures/bert.zig");
const bert_config = @import("../models/bert.zig");
const safetensors = @import("../models/safetensors.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const web_runtime = @import("runtime_state.zig");
const web_weights = @import("weight_loader.zig");
const tensor_types = gguf.tensor_types;
const quant_codec = gguf.quant_codec;

pub fn loadGguf(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    gguf_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try bert_config.parseConfig(allocator, config_json);

    var parsed = try gguf.format.parse(allocator, gguf_data);
    defer parsed.deinit(allocator);

    var compute = wasm_compute.WasmCompute.init(allocator);

    for (parsed.tensors) |ti| {
        const byte_len = tensor_types.byteLen(ti.tensor_type, ti.dimensions) orelse continue;
        const data_offset: usize = @intCast(ti.data_offset);
        const data_len: usize = @intCast(byte_len);
        if (data_offset + data_len > gguf_data.len) continue;
        const raw = gguf_data[data_offset..][0..data_len];

        const n_elements: usize = @intCast(tensor_types.elementCount(ti.dimensions) orelse continue);
        const stripped = stripModelPrefix(ti.name);

        if (ti.tensor_type.isQuantized()) {
            const raw_copy = try allocator.dupe(u8, raw);
            const owned_name = try allocator.dupe(u8, stripped);
            compute.registerQuantizedWeight(owned_name, raw_copy, ti.tensor_type, n_elements);
        } else {
            const f32_data = try dequantize(allocator, ti.tensor_type, raw, n_elements);
            const owned_name = try allocator.dupe(u8, stripped);
            compute.registerWeight(owned_name, f32_data);
        }
    }

    return runtime.storeModel(compute, .{ .bert = config });
}

pub fn loadSafetensors(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    st_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try bert_config.parseConfig(allocator, config_json);

    const result = try safetensors.parseHeader(allocator, st_data);
    var header = result.header;
    defer header.deinit();
    const data_offset = result.data_offset;

    var compute = wasm_compute.WasmCompute.init(allocator);

    var it = header.tensors.iterator();
    while (it.next()) |entry| {
        const name = entry.key_ptr.*;
        const meta = entry.value_ptr.*;

        if (std.mem.endsWith(u8, name, ".position_ids")) continue;

        const abs_start: usize = @intCast(data_offset + meta.data_start);
        const abs_end: usize = @intCast(data_offset + meta.data_end);
        if (abs_end > st_data.len) continue;
        const raw = st_data[abs_start..abs_end];

        const n_elements = blk: {
            var count: usize = 1;
            for (meta.shape) |dim| count *= @intCast(dim);
            break :blk count;
        };

        try web_weights.registerSafetensorsWeight(allocator, &compute, stripModelPrefix(name), meta.dtype, raw, n_elements);
    }

    return runtime.storeModel(compute, .{ .bert = config });
}

pub fn embed(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .bert => |cfg| cfg,
        .clap, .clip, .deberta, .florence, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const result = try bert_arch.forward(
        &cb,
        allocator,
        config,
        input_ids,
        attention_mask,
        null,
        batch,
        seq_len,
    );
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

fn stripModelPrefix(name: []const u8) []const u8 {
    const prefixes = [_][]const u8{ "bert.", "roberta.", "distilbert." };
    for (prefixes) |prefix| {
        if (std.mem.startsWith(u8, name, prefix)) {
            return name[prefix.len..];
        }
    }
    return name;
}

fn dequantize(
    allocator: std.mem.Allocator,
    tensor_type: tensor_types.TensorType,
    raw: []const u8,
    n_elements: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, n_elements);
    errdefer allocator.free(out);

    switch (tensor_type) {
        .known => |known| switch (known) {
            .F32 => {
                const src = @as([*]const f32, @ptrCast(@alignCast(raw.ptr)))[0..n_elements];
                @memcpy(out, src);
            },
            .F16 => {
                const src = @as([*]const f16, @ptrCast(@alignCast(raw.ptr)))[0..n_elements];
                for (0..n_elements) |i| out[i] = @floatCast(src[i]);
            },
            else => {
                try quant_codec.dequantizeToFloat32(tensor_type, raw, out);
            },
        },
        .bitnet_tl2 => return error.UnsupportedQuantType,
        .unknown => return error.UnsupportedQuantType,
    }
    return out;
}
