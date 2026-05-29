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

const ctx = @import("entry_context.zig");
const web_gpt = @import("gpt_api.zig");
const web_host = @import("host_abi.zig");
const web_t5 = @import("t5_api.zig");

export fn load_model_gpt(
    st_ptr: [*]const u8,
    st_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_gpt.loadSafetensors(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, st_ptr, st_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn create_model_gpt(config_json_ptr: [*]const u8, config_json_len: web_host.HostLen) u32 {
    return web_gpt.create(ctx.allocator, &ctx.runtime, web_host.sliceConst(u8, config_json_ptr, config_json_len)) catch return 0;
}

export fn register_weight(
    model_handle: u32,
    name_ptr: [*]const u8,
    name_len: web_host.HostLen,
    data_ptr: [*]const u8,
    data_len: web_host.HostLen,
    rows: u32,
    cols: u32,
    dtype: u32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.registerWeight(
        ctx.allocator,
        model,
        web_host.sliceConst(u8, name_ptr, name_len),
        web_host.sliceConst(u8, data_ptr, data_len),
        rows,
        cols,
        dtype,
    ) catch return 0;
}

export fn register_weight_gguf(
    model_handle: u32,
    name_ptr: [*]const u8,
    name_len: web_host.HostLen,
    data_ptr: [*]const u8,
    data_len: web_host.HostLen,
    tensor_type_raw: u32,
    n_elements: web_host.HostLen,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.registerGgufWeight(
        ctx.allocator,
        model,
        web_host.sliceConst(u8, name_ptr, name_len),
        web_host.sliceConst(u8, data_ptr, data_len),
        tensor_type_raw,
        web_host.toLen(n_elements),
    ) catch return 0;
}

export fn register_weight_gguf_owned(
    model_handle: u32,
    name_ptr: [*]const u8,
    name_len: web_host.HostLen,
    data_ptr: [*]u8,
    data_len: web_host.HostLen,
    tensor_type_raw: u32,
    n_elements: web_host.HostLen,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.registerGgufWeightOwned(
        ctx.allocator,
        model,
        web_host.sliceConst(u8, name_ptr, name_len),
        web_host.sliceMut(u8, data_ptr, data_len),
        tensor_type_raw,
        web_host.toLen(n_elements),
    ) catch return 0;
}

export fn register_weight_gguf_gpu(
    model_handle: u32,
    name_ptr: [*]const u8,
    name_len: web_host.HostLen,
    gpu_buf: u32,
    tensor_type_raw: u32,
    n_elements: web_host.HostLen,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.registerGgufWeightGpu(
        ctx.allocator,
        model,
        web_host.sliceConst(u8, name_ptr, name_len),
        gpu_buf,
        tensor_type_raw,
        web_host.toLen(n_elements),
    ) catch return 0;
}

export fn load_model_gpt_gguf(
    gguf_ptr: [*]const u8,
    gguf_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_gpt.loadGguf(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, gguf_ptr, gguf_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn gpt_config_json_from_gguf(
    gguf_ptr: [*]const u8,
    gguf_len: web_host.HostLen,
    out_len_ptr: [*]u32,
) ?[*]u8 {
    const json_bytes = web_gpt.configJsonFromGguf(
        ctx.allocator,
        web_host.sliceConst(u8, gguf_ptr, gguf_len),
    ) catch return null;
    out_len_ptr[0] = @intCast(json_bytes.len);
    return json_bytes.ptr;
}

export fn gpt_forward(
    model_handle: u32,
    ids_ptr: [*]const i64,
    ids_len: web_host.HostLen,
    batch_size: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.forward(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, ids_ptr, ids_len),
        batch_size,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn gpt_create_kv_cache(model_handle: u32, max_len: u32) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.createKvCache(ctx.allocator, &ctx.caches, model, max_len, .f32, .f32) catch return 0;
}

export fn gpt_create_kv_cache_ex(model_handle: u32, max_len: u32, key_format_raw: u32, value_format_raw: u32) u32 {
    const key_format = ctx.parseGpuKvKeyFormat(key_format_raw) catch return 0;
    const value_format = ctx.parseGpuKvValueFormat(value_format_raw) catch return 0;
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.createKvCache(ctx.allocator, &ctx.caches, model, max_len, key_format, value_format) catch return 0;
}

export fn gpt_forward_cached(
    model_handle: u32,
    cache_handle: u32,
    ids_ptr: [*]const i64,
    ids_len: web_host.HostLen,
    batch_size: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.forwardCached(
        ctx.allocator,
        &ctx.caches,
        model,
        cache_handle,
        web_host.sliceConst(i64, ids_ptr, ids_len),
        batch_size,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn gpt_reset_kv_cache(cache_handle: u32) void {
    ctx.caches.resetGpt(cache_handle);
}

export fn gpt_free_kv_cache(cache_handle: u32) void {
    ctx.caches.freeGpt(cache_handle);
}

export fn gpt_truncate_kv_cache(cache_handle: u32, new_len: u32) void {
    ctx.caches.truncateGpt(cache_handle, new_len);
}

export fn load_model_t5(
    st_ptr: [*]const u8,
    st_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_t5.load(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, st_ptr, st_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn t5_encode(
    model_handle: u32,
    ids_ptr: [*]const i64,
    ids_len: web_host.HostLen,
    mask_ptr: [*]const i64,
    mask_len: web_host.HostLen,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_t5.encode(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, ids_ptr, ids_len),
        web_host.sliceConst(i64, mask_ptr, mask_len),
        batch,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn t5_decode(
    model_handle: u32,
    enc_out_ptr: [*]const f32,
    enc_out_len: web_host.HostLen,
    enc_mask_ptr: [*]const i64,
    enc_mask_len: web_host.HostLen,
    dec_ids_ptr: [*]const i64,
    dec_ids_len: web_host.HostLen,
    batch: u32,
    dec_seq: u32,
    enc_seq: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_t5.decode(
        ctx.allocator,
        model,
        web_host.sliceConst(f32, enc_out_ptr, enc_out_len),
        web_host.sliceConst(i64, enc_mask_ptr, enc_mask_len),
        web_host.sliceConst(i64, dec_ids_ptr, dec_ids_len),
        batch,
        dec_seq,
        enc_seq,
        out_ptr,
    ) catch return 0;
}

export fn t5_create_kv_cache(model_handle: u32, max_len: u32) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_t5.createKvCache(ctx.allocator, &ctx.caches, model, max_len) catch return 0;
}

export fn t5_forward_cached(
    model_handle: u32,
    cache_handle: u32,
    enc_out_ptr: [*]const f32,
    enc_out_len: web_host.HostLen,
    enc_mask_ptr: [*]const i64,
    enc_mask_len: web_host.HostLen,
    dec_ids_ptr: [*]const i64,
    dec_ids_len: web_host.HostLen,
    batch: u32,
    dec_seq: u32,
    enc_seq: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_t5.forwardCached(
        ctx.allocator,
        &ctx.caches,
        model,
        cache_handle,
        web_host.sliceConst(f32, enc_out_ptr, enc_out_len),
        web_host.sliceConst(i64, enc_mask_ptr, enc_mask_len),
        web_host.sliceConst(i64, dec_ids_ptr, dec_ids_len),
        batch,
        dec_seq,
        enc_seq,
        out_ptr,
    ) catch return 0;
}

export fn t5_reset_kv_cache(cache_handle: u32) void {
    ctx.caches.resetT5(cache_handle);
}

export fn t5_free_kv_cache(cache_handle: u32) void {
    ctx.caches.freeT5(cache_handle);
}

export fn gpt_vision_encode(
    model_handle: u32,
    pixel_values_ptr: [*]const f32,
    pixel_values_len: web_host.HostLen,
    batch: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.visionEncode(
        ctx.allocator,
        model,
        web_host.sliceConst(f32, pixel_values_ptr, pixel_values_len),
        batch,
        out_ptr,
    ) catch return 0;
}

export fn gpt_projector_vision_encode(
    model_handle: u32,
    projector_handle: u32,
    pixel_values_ptr: [*]const f32,
    pixel_values_len: web_host.HostLen,
    batch: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    const projector = ctx.runtime.getProjector(projector_handle) catch return 0;
    return web_gpt.projectorVisionEncode(
        ctx.allocator,
        model,
        projector,
        web_host.sliceConst(f32, pixel_values_ptr, pixel_values_len),
        batch,
        out_ptr,
    ) catch return 0;
}

export fn gpt_projector_image_encode(
    projector_handle: u32,
    image_ptr: [*]const u8,
    image_len: web_host.HostLen,
    out_tokens_ptr: *u32,
    out_ptr: [*]f32,
) u32 {
    const projector = ctx.runtime.getProjector(projector_handle) catch return 0;
    return web_gpt.projectorImageEncode(
        ctx.allocator,
        projector,
        web_host.sliceConst(u8, image_ptr, image_len),
        out_ptr,
        out_tokens_ptr,
    ) catch return 0;
}

export fn gpt_projector_audio_encode(
    projector_handle: u32,
    audio_ptr: [*]const u8,
    audio_len: web_host.HostLen,
    out_tokens_ptr: *u32,
    out_ptr: [*]f32,
) u32 {
    const projector = ctx.runtime.getProjector(projector_handle) catch return 0;
    return web_gpt.projectorAudioEncode(
        ctx.allocator,
        projector,
        web_host.sliceConst(u8, audio_ptr, audio_len),
        out_ptr,
        out_tokens_ptr,
    ) catch return 0;
}

export fn gpt_forward_multimodal(
    model_handle: u32,
    expanded_ids_ptr: [*]const i64,
    expanded_ids_len: web_host.HostLen,
    image_embeddings_ptr: [*]const f32,
    image_embeddings_len: web_host.HostLen,
    image_offsets_ptr: [*]const u32,
    num_images: u32,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.forwardMultimodal(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, expanded_ids_ptr, expanded_ids_len),
        web_host.sliceConst(f32, image_embeddings_ptr, image_embeddings_len),
        image_offsets_ptr[0..num_images],
        batch,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn gpt_forward_cached_multimodal(
    model_handle: u32,
    cache_handle: u32,
    expanded_ids_ptr: [*]const i64,
    expanded_ids_len: web_host.HostLen,
    image_embeddings_ptr: [*]const f32,
    image_embeddings_len: web_host.HostLen,
    image_offsets_ptr: [*]const u32,
    num_images: u32,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.forwardCachedMultimodal(
        ctx.allocator,
        &ctx.caches,
        model,
        cache_handle,
        web_host.sliceConst(i64, expanded_ids_ptr, expanded_ids_len),
        web_host.sliceConst(f32, image_embeddings_ptr, image_embeddings_len),
        image_offsets_ptr[0..num_images],
        batch,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn gpt_forward_multimodal_gemma4(
    model_handle: u32,
    tok_handle: u32,
    expanded_ids_ptr: [*]const i64,
    expanded_ids_len: web_host.HostLen,
    image_embeddings_ptr: [*]const f32,
    image_embeddings_len: web_host.HostLen,
    image_token_counts_ptr: [*]const u32,
    image_token_counts_len: web_host.HostLen,
    audio_embeddings_ptr: [*]const f32,
    audio_embeddings_len: web_host.HostLen,
    audio_token_counts_ptr: [*]const u32,
    audio_token_counts_len: web_host.HostLen,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.forwardMultimodalGemma4(
        ctx.allocator,
        &ctx.runtime,
        model,
        tok_handle,
        web_host.sliceConst(i64, expanded_ids_ptr, expanded_ids_len),
        web_host.sliceConst(f32, image_embeddings_ptr, image_embeddings_len),
        web_host.sliceConst(u32, image_token_counts_ptr, image_token_counts_len),
        web_host.sliceConst(f32, audio_embeddings_ptr, audio_embeddings_len),
        web_host.sliceConst(u32, audio_token_counts_ptr, audio_token_counts_len),
        batch,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn gpt_forward_cached_multimodal_gemma4(
    model_handle: u32,
    tok_handle: u32,
    cache_handle: u32,
    expanded_ids_ptr: [*]const i64,
    expanded_ids_len: web_host.HostLen,
    image_embeddings_ptr: [*]const f32,
    image_embeddings_len: web_host.HostLen,
    image_token_counts_ptr: [*]const u32,
    image_token_counts_len: web_host.HostLen,
    audio_embeddings_ptr: [*]const f32,
    audio_embeddings_len: web_host.HostLen,
    audio_token_counts_ptr: [*]const u32,
    audio_token_counts_len: web_host.HostLen,
    batch: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gpt.forwardCachedMultimodalGemma4(
        ctx.allocator,
        &ctx.runtime,
        &ctx.caches,
        model,
        tok_handle,
        cache_handle,
        web_host.sliceConst(i64, expanded_ids_ptr, expanded_ids_len),
        web_host.sliceConst(f32, image_embeddings_ptr, image_embeddings_len),
        web_host.sliceConst(u32, image_token_counts_ptr, image_token_counts_len),
        web_host.sliceConst(f32, audio_embeddings_ptr, audio_embeddings_len),
        web_host.sliceConst(u32, audio_token_counts_ptr, audio_token_counts_len),
        batch,
        seq_len,
        out_ptr,
    ) catch return 0;
}
