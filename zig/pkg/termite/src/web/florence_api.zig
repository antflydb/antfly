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
const florence_arch = @import("../architectures/florence.zig");
const florence_config = @import("../models/florence.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const web_runtime = @import("runtime_state.zig");
const web_weights = @import("weight_loader.zig");

pub fn load(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    st_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try florence_config.parseConfig(allocator, config_json);
    var compute = wasm_compute.WasmCompute.init(allocator);
    try web_weights.loadSafetensorsWeights(allocator, &compute, st_data, null);
    return runtime.storeModel(compute, .{ .florence = config });
}

pub fn encode(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    pixel_values: []const f32,
    prompt_ids: []const i64,
    batch: u32,
    out_ptr: [*]f32,
    out_enc_seq_ptr: [*]u32,
) !u32 {
    const config = switch (model.config) {
        .florence => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const prompt_seq_len = prompt_ids.len / batch;
    const enc_result = try florence_arch.encoderForward(
        &cb,
        allocator,
        config,
        pixel_values,
        batch,
        prompt_ids,
        prompt_seq_len,
    );
    defer allocator.free(enc_result.hidden);

    @memcpy(out_ptr[0..enc_result.hidden.len], enc_result.hidden);
    out_enc_seq_ptr[0] = @intCast(enc_result.seq_len);
    return @intCast(enc_result.hidden.len);
}

pub fn encodeText(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    input_ids: []const i64,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .florence => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const result = try florence_arch.textEncoderForward(
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
        .florence => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const enc_ct = try cb.fromFloat32(enc_hidden);
    defer cb.free(enc_ct);

    const result = try florence_arch.decoderForward(
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
