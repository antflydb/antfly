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
const t5_arch = @import("../architectures/t5.zig");
const t5_config = @import("../models/t5.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const web_cache = @import("cache_state.zig");
const web_runtime = @import("runtime_state.zig");
const web_weights = @import("weight_loader.zig");

pub fn load(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    st_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try t5_config.parseConfig(allocator, config_json);
    var compute = wasm_compute.WasmCompute.init(allocator);

    try web_weights.loadSafetensorsWeights(allocator, &compute, st_data, null);
    return runtime.storeModel(compute, .{ .t5 = config });
}

pub fn encode(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .t5 => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .florence, .gpt, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const result = try t5_arch.encoderForward(&cb, allocator, config, input_ids, attention_mask, batch, seq_len);
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn decode(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    encoder_output: []const f32,
    encoder_mask: []const i64,
    decoder_ids: []const i64,
    batch: u32,
    dec_seq: u32,
    enc_seq: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .t5 => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .florence, .gpt, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();

    const enc_shape = [_]i32{ @intCast(batch * enc_seq), @intCast(config.d_model) };
    const encoder_ct = try cb.fromFloat32Shape(encoder_output, &enc_shape);
    defer cb.free(encoder_ct);

    const result = try t5_arch.decoderForward(
        &cb,
        allocator,
        config,
        decoder_ids,
        encoder_ct,
        encoder_mask,
        batch,
        dec_seq,
        enc_seq,
    );
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn createKvCache(
    allocator: std.mem.Allocator,
    caches: *web_cache.CacheState,
    model: *web_runtime.Model,
    max_len: u32,
) !u32 {
    const config = switch (model.config) {
        .t5 => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .florence, .gpt, .whisper => return error.UnsupportedModelType,
    };

    return caches.createT5(
        allocator,
        model.compute.use_gpu,
        config.effectiveDecoderLayers(),
        config.num_heads,
        config.d_kv,
        max_len,
    );
}

pub fn forwardCached(
    allocator: std.mem.Allocator,
    caches: *web_cache.CacheState,
    model: *web_runtime.Model,
    cache_handle: u32,
    encoder_output: []const f32,
    encoder_mask: []const i64,
    decoder_ids: []const i64,
    batch: u32,
    dec_seq: u32,
    enc_seq: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .t5 => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .florence, .gpt, .whisper => return error.UnsupportedModelType,
    };

    var cache = try caches.getCache(cache_handle);
    const cross_cache = try caches.getT5CrossCache(cache_handle);

    const total_seq = cache.cached_len + dec_seq;
    if (total_seq > cache.max_len) return error.CacheFull;

    cache.step_tokens = dec_seq;
    model.compute.active_kv_cache = cache;
    defer model.compute.active_kv_cache = null;

    const gpu_cache_ptr = caches.getGpuCache(cache_handle);
    model.compute.active_gpu_kv_cache = gpu_cache_ptr;
    defer model.compute.active_gpu_kv_cache = null;

    var cb = model.compute.computeBackend();

    const enc_shape = [_]i32{ @intCast(batch * enc_seq), @intCast(config.d_model) };
    const encoder_ct = try cb.fromFloat32Shape(encoder_output, &enc_shape);
    defer cb.free(encoder_ct);

    const dc = t5_arch.T5DecodeContext{
        .cached_len = cache.cached_len,
        .total_kv_len = total_seq,
        .cross_cache = cross_cache,
    };

    const result = try t5_arch.decoderForwardCached(
        &cb,
        allocator,
        config,
        decoder_ids,
        encoder_ct,
        encoder_mask,
        batch,
        dec_seq,
        enc_seq,
        dc,
    );
    defer allocator.free(result);

    cache.commitStep();
    caches.syncGpuCachedLen(cache_handle, cache.cached_len);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}
