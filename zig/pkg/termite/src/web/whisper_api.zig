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
const whisper_arch = @import("../architectures/whisper.zig");
const whisper_config = @import("../models/whisper.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const web_runtime = @import("runtime_state.zig");
const web_weights = @import("weight_loader.zig");

pub fn load(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    st_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try whisper_config.parseConfig(allocator, config_json);
    var compute = wasm_compute.WasmCompute.init(allocator);
    try web_weights.loadSafetensorsWeights(allocator, &compute, st_data, null);
    return runtime.storeModel(compute, .{ .whisper = config });
}

pub fn encode(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    mel_features: []const f32,
    batch: u32,
    time_steps: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .whisper => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .florence, .gpt, .t5 => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const mel_ct = try cb.fromFloat32(mel_features);
    defer cb.free(mel_ct);

    const result = try whisper_arch.encoderForward(
        &cb,
        allocator,
        config,
        mel_ct,
        batch,
        time_steps,
    );
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn decode(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    decoder_ids: []const i64,
    enc_hidden: []const f32,
    enc_mask: []const i64,
    batch: u32,
    dec_seq: u32,
    enc_seq: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .whisper => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .florence, .gpt, .t5 => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const enc_ct = try cb.fromFloat32(enc_hidden);
    defer cb.free(enc_ct);

    const result = try whisper_arch.decoderForward(
        &cb,
        allocator,
        config,
        decoder_ids,
        enc_ct,
        enc_mask,
        batch,
        dec_seq,
        enc_seq,
    );
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}
