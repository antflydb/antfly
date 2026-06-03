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
const gguf_metadata = @import("../gguf/metadata.zig");
const gemma3_projector = @import("../architectures/gemma3_projector.zig");
const gemma3_vision = @import("../architectures/gemma3_vision.zig");
const gemma4_multimodal_mod = @import("../architectures/gemma4_multimodal.zig");
const gemma4_projector = @import("../architectures/gemma4_projector.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_config = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");
const safetensors = @import("../models/safetensors.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const gemma3_multimodal_mod = @import("../pipelines/gemma3_multimodal.zig");
const web_cache = @import("cache_state.zig");
const web_runtime = @import("runtime_state.zig");
const web_weights = @import("weight_loader.zig");
const tensor_types = gguf.tensor_types;
const quant_codec = gguf.quant_codec;
const DType = @import("../backends/tensor.zig").DType;

pub fn loadSafetensors(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    st_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try gpt_config.parseConfig(allocator, config_json);

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
        if (std.mem.endsWith(u8, name, ".attn.bias") or
            std.mem.endsWith(u8, name, ".attn.masked_bias")) continue;

        const abs_start: usize = @intCast(data_offset + meta.data_start);
        const abs_end: usize = @intCast(data_offset + meta.data_end);
        if (abs_end > st_data.len) continue;
        const raw = st_data[abs_start..abs_end];

        const n_elements = blk: {
            var count: usize = 1;
            for (meta.shape) |dim| count *= @intCast(dim);
            break :blk count;
        };

        const needs_transpose = config.family == .gpt2 and meta.shape.len == 2 and isConv1dWeight(name);

        if (meta.dtype == .f16 and !needs_transpose) {
            const f16_data = web_weights.copyToF16(allocator, raw, n_elements) catch continue;
            const owned_name = try allocator.dupe(u8, name);
            compute.registerF16Weight(owned_name, f16_data);
            continue;
        }

        var f32_data = web_weights.convertToF32(allocator, meta.dtype, raw, n_elements) catch continue;

        if (needs_transpose) {
            const rows: usize = @intCast(meta.shape[0]);
            const cols: usize = @intCast(meta.shape[1]);
            const transposed = try allocator.alloc(f32, n_elements);
            for (0..rows) |r| {
                for (0..cols) |c| {
                    transposed[c * rows + r] = f32_data[r * cols + c];
                }
            }
            allocator.free(f32_data);
            f32_data = transposed;
        }

        const owned_name = try allocator.dupe(u8, name);
        compute.registerWeight(owned_name, f32_data);
    }

    return runtime.storeModel(compute, .{ .gpt = config });
}

pub fn configJsonFromGguf(allocator: std.mem.Allocator, gguf_data: []const u8) ![]u8 {
    var parsed = try gguf.format.parse(allocator, gguf_data);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);
    const arch = view.getString("general.architecture") orelse return error.UnsupportedModelType;
    const config = gpt_config.parseGgufMetadata(view) orelse return error.UnsupportedModelType;

    const vision_config = if (config.vision_hidden_size > 0 or config.vision_embed_dim > 0 or config.vision_patch_size > 0)
        .{
            .hidden_size = if (config.vision_hidden_size > 0) config.vision_hidden_size else null,
            .embed_dim = if (config.vision_embed_dim > 0) config.vision_embed_dim else null,
            .num_hidden_layers = if (config.vision_num_hidden_layers > 0) config.vision_num_hidden_layers else null,
            .num_attention_heads = if (config.vision_num_attention_heads > 0) config.vision_num_attention_heads else null,
            .intermediate_size = if (config.vision_intermediate_size > 0) config.vision_intermediate_size else null,
            .mlp_ratio = if (config.vision_mlp_ratio > 0) config.vision_mlp_ratio else null,
            .image_size = if (config.vision_image_size > 0) config.vision_image_size else null,
            .patch_size = if (config.vision_patch_size > 0) config.vision_patch_size else null,
            .spatial_merge_size = if (config.vision_spatial_merge_size > 1) config.vision_spatial_merge_size else null,
            .temporal_patch_size = if (config.vision_temporal_patch_size > 1) config.vision_temporal_patch_size else null,
            .hidden_act = if (config.vision_use_quick_gelu) "quick_gelu" else null,
        }
    else
        null;

    const hidden_act: ?[]const u8 = switch (config.activation) {
        .gelu => "gelu",
        .gelu_new => "gelu_pytorch_tanh",
        .silu => "silu",
        .relu => "relu",
        .relu_squared => "relu_squared",
    };

    return std.json.Stringify.valueAlloc(allocator, .{
        .model_type = arch,
        .hidden_size = config.hidden_size,
        .num_hidden_layers = config.num_hidden_layers,
        .num_attention_heads = config.num_attention_heads,
        .num_key_value_heads = if (config.num_key_value_heads > 0) config.num_key_value_heads else null,
        .head_dim = if (config.attention_head_dim > 0) config.attention_head_dim else null,
        .intermediate_size = config.intermediate_size,
        .vocab_size = config.vocab_size,
        .max_position_embeddings = config.max_position_embeddings,
        .sliding_window = if (config.sliding_window > 0) config.sliding_window else null,
        .num_local_experts = if (config.num_local_experts > 0) config.num_local_experts else null,
        .num_experts_per_tok = if (config.num_experts_per_tok > 0) config.num_experts_per_tok else null,
        .num_shared_experts = if (config.num_shared_experts > 0) config.num_shared_experts else null,
        .shared_expert_intermediate_size = if (config.shared_expert_intermediate_size > 0) config.shared_expert_intermediate_size else null,
        .expert_intermediate_size = if (config.expert_intermediate_size > 0) config.expert_intermediate_size else null,
        .num_kv_shared_layers = if (config.num_kv_shared_layers > 0) config.num_kv_shared_layers else null,
        .global_head_dim = if (config.global_head_dim > 0) config.global_head_dim else null,
        .num_global_key_value_heads = if (config.num_global_key_value_heads > 0) config.num_global_key_value_heads else null,
        .hidden_size_per_layer_input = if (config.ple_hidden_size > 0) config.ple_hidden_size else null,
        .bos_token_id = if (config.bos_token_id >= 0) config.bos_token_id else null,
        .eos_token_id = if (config.eos_token_id >= 0) config.eos_token_id else null,
        .pad_token_id = if (config.pad_token_id >= 0) config.pad_token_id else null,
        .image_token_index = if (config.image_token_index >= 0) config.image_token_index else null,
        .boi_token_index = if (config.boi_token_index >= 0) config.boi_token_index else null,
        .eoi_token_index = if (config.eoi_token_index >= 0) config.eoi_token_index else null,
        .mm_tokens_per_image = if (config.mm_tokens_per_image > 0) config.mm_tokens_per_image else null,
        .rms_norm_eps = config.norm_eps,
        .rope_theta = config.rope_theta,
        .sliding_window_pattern = if (config.sliding_window_pattern != 6) config.sliding_window_pattern else null,
        .final_logit_softcapping = if (config.final_logit_softcapping > 0.0) config.final_logit_softcapping else null,
        .tie_word_embeddings = config.weight_tying,
        .hidden_act = hidden_act,
        .vision_config = vision_config,
    }, .{});
}

pub fn create(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    config_json: []const u8,
) !u32 {
    const config = try gpt_config.parseConfig(allocator, config_json);
    const compute = wasm_compute.WasmCompute.init(allocator);
    return runtime.storeModel(compute, .{ .gpt = config });
}

pub fn registerWeight(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    name: []const u8,
    raw: []const u8,
    rows: u32,
    cols: u32,
    dtype: u32,
) !u32 {
    const owned_name = try allocator.dupe(u8, name);
    const n_elements: usize = @as(usize, rows) * @as(usize, cols);
    const needs_transpose = dtype == 3 or dtype == 4;
    const base_dtype: u32 = switch (dtype) {
        3 => 1,
        4 => 0,
        else => dtype,
    };

    if (base_dtype == 1 and !needs_transpose) {
        const f16_data = web_weights.copyToF16(allocator, raw, n_elements) catch return error.ConvertFailed;
        model.compute.registerF16Weight(owned_name, f16_data);
        return 1;
    }

    const st_dtype: DType = switch (base_dtype) {
        0 => .f32,
        1 => .f16,
        2 => .bf16,
        else => return error.UnsupportedDtype,
    };
    var f32_data = web_weights.convertToF32(allocator, st_dtype, raw, n_elements) catch return error.ConvertFailed;

    if (needs_transpose and rows > 1 and cols > 1) {
        const r: usize = rows;
        const c: usize = cols;
        const transposed = try allocator.alloc(f32, n_elements);
        for (0..r) |ri| {
            for (0..c) |ci| {
                transposed[ci * r + ri] = f32_data[ri * c + ci];
            }
        }
        allocator.free(f32_data);
        f32_data = transposed;
    }

    model.compute.registerWeight(owned_name, f32_data);
    return 1;
}

pub fn registerGgufWeight(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    name: []const u8,
    raw: []const u8,
    tensor_type_raw: u32,
    n_elements: usize,
) !u32 {
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);

    const tensor_type = tensor_types.TensorType.fromRaw(tensor_type_raw);

    switch (tensor_type) {
        .known => |known| switch (known) {
            .F16 => {
                const f16_data = try web_weights.copyToF16(allocator, raw, n_elements);
                model.compute.registerF16Weight(owned_name, f16_data);
                return 1;
            },
            .F32 => {
                const f32_data = try web_weights.convertToF32(allocator, .f32, raw, n_elements);
                model.compute.registerWeight(owned_name, f32_data);
                return 1;
            },
            .BF16 => {
                const f32_data = try web_weights.convertToF32(allocator, .bf16, raw, n_elements);
                model.compute.registerWeight(owned_name, f32_data);
                return 1;
            },
            else => {
                if (!tensor_type.isQuantized()) return error.UnsupportedTensorType;
                const raw_copy = try allocator.dupe(u8, raw);
                model.compute.registerQuantizedWeight(owned_name, raw_copy, tensor_type, n_elements);
                return 1;
            },
        },
        .bitnet_tl2 => {
            const raw_copy = try allocator.dupe(u8, raw);
            model.compute.registerQuantizedWeight(owned_name, raw_copy, tensor_type, n_elements);
            return 1;
        },
        .unknown => return error.UnsupportedTensorType,
    }
}

pub fn registerGgufWeightOwned(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    name: []const u8,
    raw_owned: []u8,
    tensor_type_raw: u32,
    n_elements: usize,
) !u32 {
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);
    errdefer allocator.free(raw_owned);

    const tensor_type = tensor_types.TensorType.fromRaw(tensor_type_raw);

    switch (tensor_type) {
        .known => |known| switch (known) {
            .F16 => {
                const f16_data = try web_weights.copyToF16(allocator, raw_owned, n_elements);
                allocator.free(raw_owned);
                model.compute.registerF16Weight(owned_name, f16_data);
                return 1;
            },
            .F32 => {
                const f32_data = try web_weights.convertToF32(allocator, .f32, raw_owned, n_elements);
                allocator.free(raw_owned);
                model.compute.registerWeight(owned_name, f32_data);
                return 1;
            },
            .BF16 => {
                const f32_data = try web_weights.convertToF32(allocator, .bf16, raw_owned, n_elements);
                allocator.free(raw_owned);
                model.compute.registerWeight(owned_name, f32_data);
                return 1;
            },
            else => {
                if (!tensor_type.isQuantized()) return error.UnsupportedTensorType;
                model.compute.registerQuantizedWeight(owned_name, raw_owned, tensor_type, n_elements);
                return 1;
            },
        },
        .bitnet_tl2 => {
            model.compute.registerQuantizedWeight(owned_name, raw_owned, tensor_type, n_elements);
            return 1;
        },
        .unknown => return error.UnsupportedTensorType,
    }
}

pub fn registerGgufWeightGpu(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    name: []const u8,
    gpu_buf: u32,
    tensor_type_raw: u32,
    n_elements: usize,
) !u32 {
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);

    const tensor_type = tensor_types.TensorType.fromRaw(tensor_type_raw);
    if (!tensor_type.isQuantized()) return error.UnsupportedTensorType;

    model.compute.registerGpuQuantizedWeight(owned_name, gpu_buf, tensor_type, n_elements);
    return 1;
}

pub fn loadGguf(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    gguf_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try gpt_config.parseConfig(allocator, config_json);

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
        const owned_name = try allocator.dupe(u8, ti.name);

        if (ti.tensor_type.isQuantized()) {
            const raw_copy = try allocator.dupe(u8, raw);
            compute.registerQuantizedWeight(owned_name, raw_copy, ti.tensor_type, n_elements);
        } else {
            const f32_data = try dequantize(allocator, ti.tensor_type, raw, n_elements);
            compute.registerWeight(owned_name, f32_data);
        }
    }

    return runtime.storeModel(compute, .{ .gpt = config });
}

pub fn forward(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    input_ids: []const i64,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .gpt => |cfg| cfg,
        .bert, .clap, .clip, .deberta, .florence, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const result = try gpt_arch.forward(
        &cb,
        allocator,
        config,
        input_ids,
        batch,
        seq_len,
        null,
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
    key_format: web_cache.GpuKvKeyFormat,
    value_format: web_cache.GpuKvValueFormat,
) !u32 {
    const config = switch (model.config) {
        .gpt => |cfg| cfg,
        else => return error.UnsupportedModelType,
    };

    return caches.createGpt(
        allocator,
        model.compute.use_gpu,
        config.num_hidden_layers,
        config.effectiveKVHeads(),
        config.headDim(),
        max_len,
        key_format,
        value_format,
    );
}

pub fn forwardCached(
    allocator: std.mem.Allocator,
    caches: *web_cache.CacheState,
    model: *web_runtime.Model,
    cache_handle: u32,
    input_ids: []const i64,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .gpt => |cfg| cfg,
        else => return error.UnsupportedModelType,
    };

    var cache = try caches.getCache(cache_handle);

    const total_seq = cache.cached_len + seq_len;
    if (total_seq > cache.max_len) return error.CacheFull;

    cache.step_tokens = seq_len;
    model.compute.active_kv_cache = cache;
    defer model.compute.active_kv_cache = null;

    const gpu_cache_ptr = caches.getGpuCache(cache_handle);
    model.compute.active_gpu_kv_cache = gpu_cache_ptr;
    defer model.compute.active_gpu_kv_cache = null;

    const dc = gpt_arch.DecodeContext{
        .attention_mode = if (cache.cached_len == 0) .paged_prefill else .paged_decode,
        .total_sequence_len = total_seq,
        .query_sequence_len = seq_len,
        .kv_sequence_len = total_seq,
    };

    var cb = model.compute.computeBackend();
    const result = try gpt_arch.forward(
        &cb,
        allocator,
        config,
        input_ids,
        batch,
        seq_len,
        &dc,
    );
    defer allocator.free(result);

    cache.commitStep();
    caches.syncGpuCachedLen(cache_handle, cache.cached_len);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn visionEncode(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    pixel_values: []const f32,
    batch: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .gpt => |cfg| cfg,
        else => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const result = try gemma3_vision.encodeProjectedImageTokens(&cb, allocator, config, pixel_values, batch);
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn projectorVisionEncode(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    projector: *web_runtime.Projector,
    pixel_values: []const f32,
    batch: u32,
    out_ptr: [*]f32,
) !u32 {
    if (projector.kind != .antfly_gemma3) return error.UnsupportedProjector;
    const config = switch (model.config) {
        .gpt => |cfg| cfg,
        else => return error.UnsupportedModelType,
    };

    var compat_store = projector.store.asCompatGgufStore();
    var cb = model.compute.computeBackend();
    const result = try gemma3_projector.encodeProjectedImageTokensFromStore(
        &cb,
        allocator,
        &compat_store,
        config,
        pixel_values,
        batch,
    );
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn projectorImageEncode(
    allocator: std.mem.Allocator,
    projector: *web_runtime.Projector,
    image_bytes: []const u8,
    out_ptr: [*]f32,
    out_tokens_ptr: *u32,
) !u32 {
    switch (projector.kind) {
        .clip_gemma4_image, .clip_gemma4_image_audio => {},
        else => return error.UnsupportedProjector,
    }

    var compat_store = projector.store.asCompatGgufStore();
    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const images = [_][]const u8{image_bytes};
    var projected = try gemma4_projector.encodeProjectedImagesFromStore(&cb, allocator, &compat_store, &images);
    defer projected.deinit();
    if (projected.tokens_per_image.len != 1) return error.InvalidTensorShape;

    out_tokens_ptr.* = @intCast(projected.tokens_per_image[0]);
    @memcpy(out_ptr[0..projected.embeddings.len], projected.embeddings);
    return @intCast(projected.embeddings.len);
}

pub fn projectorAudioEncode(
    allocator: std.mem.Allocator,
    projector: *web_runtime.Projector,
    audio_bytes: []const u8,
    out_ptr: [*]f32,
    out_tokens_ptr: *u32,
) !u32 {
    switch (projector.kind) {
        .clip_gemma4_audio, .clip_gemma4_image_audio => {},
        else => return error.UnsupportedProjector,
    }

    var compat_store = projector.store.asCompatGgufStore();
    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const audio_clips = [_][]const u8{audio_bytes};
    var projected = try gemma4_projector.encodeProjectedAudioFromStore(&cb, allocator, &compat_store, &audio_clips);
    defer projected.deinit();
    if (projected.tokens_per_audio.len != 1) return error.InvalidTensorShape;

    out_tokens_ptr.* = @intCast(projected.tokens_per_audio[0]);
    @memcpy(out_ptr[0..projected.embeddings.len], projected.embeddings);
    return @intCast(projected.embeddings.len);
}

pub fn forwardMultimodal(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    expanded_ids: []const i64,
    image_embeddings: []const f32,
    image_offsets: []const u32,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .gpt => |cfg| cfg,
        else => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();
    const hidden_ct = try buildMultimodalHidden(
        &cb,
        allocator,
        config,
        expanded_ids,
        image_embeddings,
        image_offsets,
        batch,
        seq_len,
    );
    defer cb.free(hidden_ct);

    const attn_mask = try gemma3_multimodal_mod.buildImageAttentionOrMaskFromExpandedTokens(allocator, expanded_ids, config);
    defer if (attn_mask) |m| allocator.free(m);

    const dc = gpt_arch.DecodeContext{
        .total_sequence_len = seq_len,
        .query_sequence_len = seq_len,
        .kv_sequence_len = seq_len,
        .attn_or_mask = attn_mask,
    };

    const result = try gpt_arch.forwardFromEmbeddings(&cb, allocator, config, hidden_ct, batch, seq_len, &dc, null);
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn forwardCachedMultimodal(
    allocator: std.mem.Allocator,
    caches: *web_cache.CacheState,
    model: *web_runtime.Model,
    cache_handle: u32,
    expanded_ids: []const i64,
    image_embeddings: []const f32,
    image_offsets: []const u32,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .gpt => |cfg| cfg,
        else => return error.UnsupportedModelType,
    };

    var cache = try caches.getCache(cache_handle);

    const total_seq = cache.cached_len + seq_len;
    if (total_seq > cache.max_len) return error.CacheFull;

    cache.step_tokens = seq_len;
    model.compute.active_kv_cache = cache;
    defer model.compute.active_kv_cache = null;

    const gpu_cache_ptr = caches.getGpuCache(cache_handle);
    model.compute.active_gpu_kv_cache = gpu_cache_ptr;
    defer model.compute.active_gpu_kv_cache = null;

    var cb = model.compute.computeBackend();
    const hidden_ct = try buildMultimodalHidden(
        &cb,
        allocator,
        config,
        expanded_ids,
        image_embeddings,
        image_offsets,
        batch,
        seq_len,
    );
    defer cb.free(hidden_ct);

    const attn_mask = try gemma3_multimodal_mod.buildImageAttentionOrMaskFromExpandedTokens(allocator, expanded_ids, config);
    defer if (attn_mask) |m| allocator.free(m);

    const dc = gpt_arch.DecodeContext{
        .attention_mode = if (cache.cached_len == 0) .paged_prefill else .paged_decode,
        .total_sequence_len = total_seq,
        .query_sequence_len = seq_len,
        .kv_sequence_len = total_seq,
        .attn_or_mask = attn_mask,
    };

    const result = try gpt_arch.forwardFromEmbeddings(&cb, allocator, config, hidden_ct, batch, seq_len, &dc, null);
    defer allocator.free(result);

    cache.commitStep();
    caches.syncGpuCachedLen(cache_handle, cache.cached_len);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn forwardMultimodalGemma4(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    model: *web_runtime.Model,
    tok_handle: u32,
    expanded_ids: []const i64,
    image_embeddings: []const f32,
    image_token_counts: []const u32,
    audio_embeddings: []const f32,
    audio_token_counts: []const u32,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .gpt => |cfg| cfg,
        else => return error.UnsupportedModelType,
    };
    const hf_tok = try runtime.getTokenizer(tok_handle);
    const tokenizer = hf_tok.tokenizer();
    var projected = gemma4_projector.ProjectedImages{
        .allocator = allocator,
        .embeddings = @constCast(image_embeddings),
        .tokens_per_image = try allocator.alloc(usize, image_token_counts.len),
        .hidden_size = config.hidden_size,
    };
    defer allocator.free(projected.tokens_per_image);
    for (image_token_counts, 0..) |count, i| projected.tokens_per_image[i] = count;
    var projected_audio = gemma4_projector.ProjectedAudio{
        .allocator = allocator,
        .embeddings = @constCast(audio_embeddings),
        .tokens_per_audio = try allocator.alloc(usize, audio_token_counts.len),
        .hidden_size = config.hidden_size,
    };
    defer allocator.free(projected_audio.tokens_per_audio);
    for (audio_token_counts, 0..) |count, i| projected_audio.tokens_per_audio[i] = count;

    const expanded_i32 = try i32IdsFromI64(allocator, expanded_ids);
    defer allocator.free(expanded_i32);

    var cb = model.compute.computeBackend();
    var prepared = try gemma4_multimodal_mod.prepareExpandedPromptEmbeddings(
        &cb,
        allocator,
        tokenizer,
        config,
        expanded_i32,
        if (image_token_counts.len > 0) &projected else null,
        if (audio_token_counts.len > 0) &projected_audio else null,
    );
    defer prepared.deinit(&cb);

    const dc = gpt_arch.DecodeContext{
        .total_sequence_len = seq_len,
        .query_sequence_len = seq_len,
        .kv_sequence_len = seq_len,
    };
    const input_embeddings = prepared.input_embeddings orelse return error.InvalidPreparedPrompt;
    const ple_vectors = if (config.hasPle()) blk: {
        const ple_token_ids = prepared.ple_token_ids orelse prepared.token_ids;
        break :blk try gpt_arch.computePleVectors(&cb, allocator, config, ple_token_ids, input_embeddings, seq_len);
    } else null;
    defer if (ple_vectors) |vectors| cb.free(vectors);
    const result = try gpt_arch.forwardFromEmbeddings(&cb, allocator, config, input_embeddings, batch, seq_len, &dc, ple_vectors);
    defer allocator.free(result);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

pub fn forwardCachedMultimodalGemma4(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    caches: *web_cache.CacheState,
    model: *web_runtime.Model,
    tok_handle: u32,
    cache_handle: u32,
    expanded_ids: []const i64,
    image_embeddings: []const f32,
    image_token_counts: []const u32,
    audio_embeddings: []const f32,
    audio_token_counts: []const u32,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .gpt => |cfg| cfg,
        else => return error.UnsupportedModelType,
    };

    var cache = try caches.getCache(cache_handle);
    const total_seq = cache.cached_len + seq_len;
    if (total_seq > cache.max_len) return error.CacheFull;

    cache.step_tokens = seq_len;
    model.compute.active_kv_cache = cache;
    defer model.compute.active_kv_cache = null;

    const gpu_cache_ptr = caches.getGpuCache(cache_handle);
    model.compute.active_gpu_kv_cache = gpu_cache_ptr;
    defer model.compute.active_gpu_kv_cache = null;

    const hf_tok = try runtime.getTokenizer(tok_handle);
    const tokenizer = hf_tok.tokenizer();
    var projected = gemma4_projector.ProjectedImages{
        .allocator = allocator,
        .embeddings = @constCast(image_embeddings),
        .tokens_per_image = try allocator.alloc(usize, image_token_counts.len),
        .hidden_size = config.hidden_size,
    };
    defer allocator.free(projected.tokens_per_image);
    for (image_token_counts, 0..) |count, i| projected.tokens_per_image[i] = count;
    var projected_audio = gemma4_projector.ProjectedAudio{
        .allocator = allocator,
        .embeddings = @constCast(audio_embeddings),
        .tokens_per_audio = try allocator.alloc(usize, audio_token_counts.len),
        .hidden_size = config.hidden_size,
    };
    defer allocator.free(projected_audio.tokens_per_audio);
    for (audio_token_counts, 0..) |count, i| projected_audio.tokens_per_audio[i] = count;

    const expanded_i32 = try i32IdsFromI64(allocator, expanded_ids);
    defer allocator.free(expanded_i32);

    var cb = model.compute.computeBackend();
    var prepared = try gemma4_multimodal_mod.prepareExpandedPromptEmbeddings(
        &cb,
        allocator,
        tokenizer,
        config,
        expanded_i32,
        if (image_token_counts.len > 0) &projected else null,
        if (audio_token_counts.len > 0) &projected_audio else null,
    );
    defer prepared.deinit(&cb);

    const dc = gpt_arch.DecodeContext{
        .attention_mode = if (cache.cached_len == 0) .paged_prefill else .paged_decode,
        .total_sequence_len = total_seq,
        .query_sequence_len = seq_len,
        .kv_sequence_len = total_seq,
    };
    const input_embeddings = prepared.input_embeddings orelse return error.InvalidPreparedPrompt;
    const ple_vectors = if (config.hasPle()) blk: {
        const ple_token_ids = prepared.ple_token_ids orelse prepared.token_ids;
        break :blk try gpt_arch.computePleVectors(&cb, allocator, config, ple_token_ids, input_embeddings, seq_len);
    } else null;
    defer if (ple_vectors) |vectors| cb.free(vectors);
    const result = try gpt_arch.forwardFromEmbeddings(&cb, allocator, config, input_embeddings, batch, seq_len, &dc, ple_vectors);
    defer allocator.free(result);

    cache.commitStep();
    caches.syncGpuCachedLen(cache_handle, cache.cached_len);

    @memcpy(out_ptr[0..result.len], result);
    return @intCast(result.len);
}

fn isConv1dWeight(name: []const u8) bool {
    if (!std.mem.endsWith(u8, name, ".weight")) return false;
    if (!std.mem.startsWith(u8, name, "h.")) return false;
    return std.mem.indexOf(u8, name, ".attn.c_") != null or
        std.mem.indexOf(u8, name, ".mlp.c_") != null;
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

fn buildMultimodalHidden(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    expanded_ids: []const i64,
    image_embeddings: []const f32,
    image_offsets: []const u32,
    batch: u32,
    seq_len: u32,
) !@import("../ops/ops.zig").CT {
    const hidden_size = config.hidden_size;
    const total = batch * seq_len;

    const embed_w = try cb.getWeight("model.embed_tokens.weight");
    const embedded = try cb.embeddingLookup(embed_w, expanded_ids, total, hidden_size);

    const scale = config.tokenEmbeddingScale();
    var hidden = embedded;
    if (!std.math.approxEqAbs(f32, scale, 1.0, 1e-6)) {
        const data = try cb.toFloat32(embedded, allocator);
        defer allocator.free(data);
        const scaled = try allocator.alloc(f32, data.len);
        for (data, 0..) |v, i| scaled[i] = v * scale;
        const shape_s = [_]i32{ @intCast(total), @intCast(hidden_size) };
        hidden = try cb.fromFloat32Shape(scaled, &shape_s);
        allocator.free(scaled);
        cb.free(embedded);
    }

    const hidden_data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_data);

    const mm_tokens: usize = config.mm_tokens_per_image;
    for (image_offsets, 0..) |offset, img_idx| {
        const dst_start = @as(usize, offset) * hidden_size;
        const src_start = img_idx * mm_tokens * hidden_size;
        const count = mm_tokens * hidden_size;
        @memcpy(hidden_data[dst_start .. dst_start + count], image_embeddings[src_start .. src_start + count]);
    }

    const shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    const hidden_ct = try cb.fromFloat32Shape(hidden_data, &shape);
    cb.free(hidden);
    return hidden_ct;
}

fn i32IdsFromI64(allocator: std.mem.Allocator, ids: []const i64) ![]i32 {
    const out = try allocator.alloc(i32, ids.len);
    errdefer allocator.free(out);
    for (ids, 0..) |id, i| out[i] = @intCast(id);
    return out;
}
