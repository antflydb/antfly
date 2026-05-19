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
const clip_arch = @import("../architectures/clip.zig");
const clip_config = @import("../models/clip.zig");
const safetensors = @import("../models/safetensors.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const antfly_image = @import("antfly_image");
const image_processing = antfly_image.processing;
const web_runtime = @import("runtime_state.zig");
const web_weights = @import("weight_loader.zig");

pub fn load(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    st_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try clip_config.parseConfig(allocator, config_json);

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

        try web_weights.registerSafetensorsWeight(allocator, &compute, name, meta.dtype, raw, n_elements);
    }

    return runtime.storeModel(compute, .{ .clip = config });
}

pub fn embedText(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    input_ids: []const i64,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .clip => |cfg| cfg,
        .bert, .clap, .deberta, .florence, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const result = try clip_arch.textEncoderForward(
        &cb,
        allocator,
        config,
        input_ids,
        batch,
        seq_len,
    );
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn embedImage(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    pixel_values: []const f32,
    batch: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .clip => |cfg| cfg,
        .bert, .clap, .deberta, .florence, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const result = try clip_arch.visionEncoderForward(
        &cb,
        allocator,
        config,
        pixel_values,
        batch,
    );
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn preprocessImage(
    allocator: std.mem.Allocator,
    rgba_data: []const u8,
    width: u32,
    height: u32,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    out_ptr: [*]f32,
) !u32 {
    const img = image_processing.ImageU8{
        .data = rgba_data,
        .width = width,
        .height = height,
        .format = .rgba8,
    };
    const result = try image_processing.preprocessDecoded(
        allocator,
        img,
        target_size,
        mean,
        std_dev,
    );
    defer allocator.free(result);

    const n: u32 = @intCast(result.len);
    @memcpy(out_ptr[0..n], result);
    return n;
}
