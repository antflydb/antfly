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
const clap_arch = @import("../architectures/clap.zig");
const clap_config = @import("../models/clap.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const web_runtime = @import("runtime_state.zig");
const web_weights = @import("weight_loader.zig");

pub fn load(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    st_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try clap_config.parseConfig(allocator, config_json);
    var compute = wasm_compute.WasmCompute.init(allocator);
    try web_weights.loadSafetensorsWeights(allocator, &compute, st_data, null);
    return runtime.storeModel(compute, .{ .clap = config });
}

pub fn embedText(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .clap => |cfg| cfg,
        .bert, .clip, .deberta, .florence, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const result = try clap_arch.textEncoderForward(
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

pub fn embedAudio(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    input_features: []const f32,
    batch: u32,
    channels: u32,
    time_frames: u32,
    mel_bins: u32,
    is_longer: []const u8,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .clap => |cfg| cfg,
        .bert, .clip, .deberta, .florence, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const result = try clap_arch.audioEncoderForward(
        &cb,
        allocator,
        config,
        input_features,
        batch,
        channels,
        time_frames,
        mel_bins,
        is_longer,
    );
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}
