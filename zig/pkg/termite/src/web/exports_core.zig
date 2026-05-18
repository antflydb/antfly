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

const web_audio = @import("audio_api.zig");
const web_bert = @import("bert_api.zig");
const web_chat_template = @import("chat_template_api.zig");
const web_clap = @import("clap_api.zig");
const web_clip = @import("clip_api.zig");
const ctx = @import("entry_context.zig");
const web_florence = @import("florence_api.zig");
const web_gliner = @import("gliner_api.zig");
const web_host = @import("host_abi.zig");
const web_projector = @import("projector_api.zig");
const web_profile = @import("profile.zig");
const web_rerank = @import("rerank_api.zig");
const web_tokenizer = @import("tokenizer_api.zig");
const web_whisper = @import("whisper_api.zig");
const audio = @import("../pipelines/audio.zig");

export fn init() void {}

export fn wasm_alloc(size: web_profile.HostSize) ?[*]u8 {
    const slice = ctx.allocator.alloc(u8, web_profile.hostToLen(size)) catch return null;
    return slice.ptr;
}

export fn wasm_dealloc(ptr: [*]u8, size: web_profile.HostSize) void {
    ctx.allocator.free(ptr[0..web_profile.hostToLen(size)]);
}

export fn audio_whisper_mel_size() u32 {
    return audio.WHISPER_N_MELS * audio.WHISPER_N_FRAMES;
}

export fn audio_clap_feature_size(channels: u32) u32 {
    const requested = if (channels == 0) 1 else channels;
    const out_channels = if (requested >= 4) requested else 4;
    const time_frames = audio.CLAP_CONFIG.chunk_length_s * audio.CLAP_CONFIG.sample_rate / audio.CLAP_CONFIG.hop_length + 1;
    return out_channels * time_frames * audio.CLAP_CONFIG.n_mels;
}

export fn audio_whisper_mel(
    samples_ptr: [*]const f32,
    samples_len: web_host.HostLen,
    sample_rate: u32,
    out_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) u32 {
    return web_audio.whisperMel(
        ctx.allocator,
        web_host.sliceConst(f32, samples_ptr, samples_len),
        sample_rate,
        out_ptr,
        out_meta_ptr,
    ) catch return 0;
}

export fn audio_whisper_mel_interleaved(
    samples_ptr: [*]const f32,
    samples_len: web_host.HostLen,
    sample_rate: u32,
    input_channels: u32,
    out_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) u32 {
    return web_audio.whisperMelInterleaved(
        ctx.allocator,
        web_host.sliceConst(f32, samples_ptr, samples_len),
        sample_rate,
        input_channels,
        out_ptr,
        out_meta_ptr,
    ) catch return 0;
}

export fn audio_clap_features(
    samples_ptr: [*]const f32,
    samples_len: web_host.HostLen,
    sample_rate: u32,
    channels: u32,
    out_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) u32 {
    return web_audio.clapFeatures(
        ctx.allocator,
        web_host.sliceConst(f32, samples_ptr, samples_len),
        sample_rate,
        channels,
        out_ptr,
        out_meta_ptr,
    ) catch return 0;
}

export fn audio_clap_features_interleaved(
    samples_ptr: [*]const f32,
    samples_len: web_host.HostLen,
    sample_rate: u32,
    input_channels: u32,
    output_channels: u32,
    out_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) u32 {
    return web_audio.clapFeaturesInterleaved(
        ctx.allocator,
        web_host.sliceConst(f32, samples_ptr, samples_len),
        sample_rate,
        input_channels,
        output_channels,
        out_ptr,
        out_meta_ptr,
    ) catch return 0;
}

export fn load_model_gguf(
    gguf_ptr: [*]const u8,
    gguf_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_bert.loadGguf(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, gguf_ptr, gguf_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn load_projector_gguf(
    gguf_ptr: [*]const u8,
    gguf_len: web_host.HostLen,
) u32 {
    return web_projector.loadGguf(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, gguf_ptr, gguf_len),
    ) catch return 0;
}

export fn projector_kind(projector_handle: u32) u32 {
    const projector = ctx.runtime.getProjector(projector_handle) catch return 0;
    return @intFromEnum(projector.kind) + 1;
}

export fn unload_projector(projector_handle: u32) void {
    ctx.runtime.unloadProjector(projector_handle);
}

export fn load_model_safetensors(
    st_ptr: [*]const u8,
    st_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_bert.loadSafetensors(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, st_ptr, st_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn load_model_gliner(
    st_ptr: [*]const u8,
    st_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_gliner.load(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, st_ptr, st_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn embed(
    model_handle: u32,
    ids_ptr: [*]const i64,
    ids_len: web_host.HostLen,
    mask_ptr: [*]const i64,
    mask_len: web_host.HostLen,
    batch_size: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_bert.embed(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, ids_ptr, ids_len),
        web_host.sliceConst(i64, mask_ptr, mask_len),
        batch_size,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn unload_model(handle: u32) void {
    ctx.runtime.unloadModel(handle);
}

export fn load_tokenizer(json_ptr: [*]const u8, json_len: web_host.HostLen) u32 {
    return web_tokenizer.loadTokenizer(ctx.allocator, &ctx.runtime, web_host.sliceConst(u8, json_ptr, json_len)) catch return 0;
}

export fn load_tokenizer_gguf(gguf_ptr: [*]const u8, gguf_len: web_host.HostLen) u32 {
    return web_tokenizer.loadTokenizerFromGguf(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, gguf_ptr, gguf_len),
    ) catch return 0;
}

export fn gguf_chat_template(
    gguf_ptr: [*]const u8,
    gguf_len: web_host.HostLen,
    out_len_ptr: [*]u32,
) ?[*]u8 {
    const template = web_tokenizer.chatTemplateFromGguf(
        ctx.allocator,
        web_host.sliceConst(u8, gguf_ptr, gguf_len),
    ) catch return null;
    if (template) |value| {
        out_len_ptr[0] = @intCast(value.len);
        return value.ptr;
    }
    out_len_ptr[0] = 0;
    return null;
}

export fn tokenize(
    tok_handle: u32,
    text_ptr: [*]const u8,
    text_len: web_host.HostLen,
    max_len: u32,
    out_ids_ptr: [*]i32,
    out_mask_ptr: [*]i32,
) u32 {
    return web_tokenizer.tokenize(
        ctx.allocator,
        &ctx.runtime,
        tok_handle,
        web_host.sliceConst(u8, text_ptr, text_len),
        max_len,
        out_ids_ptr,
        out_mask_ptr,
    ) catch return 0;
}

export fn tokenize_pair(
    tok_handle: u32,
    a_ptr: [*]const u8,
    a_len: web_host.HostLen,
    b_ptr: [*]const u8,
    b_len: web_host.HostLen,
    max_len: u32,
    out_ids_ptr: [*]i32,
    out_mask_ptr: [*]i32,
) u32 {
    return web_tokenizer.tokenizePair(
        ctx.allocator,
        &ctx.runtime,
        tok_handle,
        web_host.sliceConst(u8, a_ptr, a_len),
        web_host.sliceConst(u8, b_ptr, b_len),
        max_len,
        out_ids_ptr,
        out_mask_ptr,
    ) catch return 0;
}

export fn unload_tokenizer(handle: u32) void {
    ctx.runtime.unloadTokenizer(handle);
}

export fn tokenize_raw(
    tok_handle: u32,
    text_ptr: [*]const u8,
    text_len: web_host.HostLen,
    out_ids_ptr: [*]i32,
    max_ids: u32,
) u32 {
    return web_tokenizer.tokenizeRaw(
        ctx.allocator,
        &ctx.runtime,
        tok_handle,
        web_host.sliceConst(u8, text_ptr, text_len),
        out_ids_ptr,
        max_ids,
    ) catch return 0;
}

export fn decode_tokens(
    tok_handle: u32,
    ids_ptr: [*]const i32,
    ids_len: web_host.HostLen,
    out_len_ptr: [*]u32,
) ?[*]u8 {
    const text = web_tokenizer.decode(
        ctx.allocator,
        &ctx.runtime,
        tok_handle,
        web_host.sliceConst(i32, ids_ptr, ids_len),
    ) catch return null;
    out_len_ptr[0] = @intCast(text.len);
    return text.ptr;
}

export fn render_chat_prompt(
    tok_handle: u32,
    template_ptr: [*]const u8,
    template_len: web_host.HostLen,
    system_ptr: [*]const u8,
    system_len: web_host.HostLen,
    user_ptr: [*]const u8,
    user_len: web_host.HostLen,
    add_generation_prompt: u32,
    out_len_ptr: [*]u32,
) ?[*]u8 {
    const prompt = web_chat_template.renderSingleTurn(
        ctx.allocator,
        &ctx.runtime,
        tok_handle,
        web_host.sliceConst(u8, template_ptr, template_len),
        web_host.sliceConst(u8, system_ptr, system_len),
        web_host.sliceConst(u8, user_ptr, user_len),
        add_generation_prompt != 0,
    ) catch return null;
    out_len_ptr[0] = @intCast(prompt.len);
    return prompt.ptr;
}

export fn rerank(
    model_handle: u32,
    ids_ptr: [*]const i64,
    ids_len: web_host.HostLen,
    mask_ptr: [*]const i64,
    mask_len: web_host.HostLen,
    batch_size: u32,
    seq_len: u32,
    num_labels: u32,
    out_scores_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_rerank.rerank(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, ids_ptr, ids_len),
        web_host.sliceConst(i64, mask_ptr, mask_len),
        batch_size,
        seq_len,
        num_labels,
        out_scores_ptr,
    ) catch return 0;
}

export fn gliner(
    model_handle: u32,
    ids_ptr: [*]const i64,
    ids_len: web_host.HostLen,
    mask_ptr: [*]const i64,
    mask_len: web_host.HostLen,
    words_mask_ptr: [*]const i64,
    words_mask_len: web_host.HostLen,
    span_idx_ptr: [*]const i64,
    span_idx_len: web_host.HostLen,
    batch_size: u32,
    seq_len: u32,
    out_logits_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_gliner.run(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, ids_ptr, ids_len),
        web_host.sliceConst(i64, mask_ptr, mask_len),
        web_host.sliceConst(i64, words_mask_ptr, words_mask_len),
        web_host.sliceConst(i64, span_idx_ptr, span_idx_len),
        batch_size,
        seq_len,
        out_logits_ptr,
        out_meta_ptr,
    ) catch return 0;
}

export fn load_model_clip(
    st_ptr: [*]const u8,
    st_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_clip.load(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, st_ptr, st_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn clip_embed_text(
    model_handle: u32,
    ids_ptr: [*]const i64,
    ids_len: web_host.HostLen,
    batch_size: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_clip.embedText(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, ids_ptr, ids_len),
        batch_size,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn clip_embed_image(
    model_handle: u32,
    pixel_ptr: [*]const f32,
    pixel_len: web_host.HostLen,
    batch_size: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_clip.embedImage(
        ctx.allocator,
        model,
        web_host.sliceConst(f32, pixel_ptr, pixel_len),
        batch_size,
        out_ptr,
    ) catch return 0;
}

export fn preprocess_image(
    rgba_ptr: [*]const u8,
    rgba_len: web_host.HostLen,
    width: u32,
    height: u32,
    target_size: u32,
    mean_ptr: [*]const f32,
    std_ptr: [*]const f32,
    out_ptr: [*]f32,
) u32 {
    return web_clip.preprocessImage(
        ctx.allocator,
        web_host.sliceConst(u8, rgba_ptr, rgba_len),
        width,
        height,
        target_size,
        mean_ptr[0..3].*,
        std_ptr[0..3].*,
        out_ptr,
    ) catch return 0;
}

export fn load_model_whisper(
    st_ptr: [*]const u8,
    st_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_whisper.load(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, st_ptr, st_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn whisper_encode(
    model_handle: u32,
    mel_ptr: [*]const f32,
    mel_len: web_host.HostLen,
    batch_size: u32,
    time_steps: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_whisper.encode(
        ctx.allocator,
        model,
        web_host.sliceConst(f32, mel_ptr, mel_len),
        batch_size,
        time_steps,
        out_ptr,
    ) catch return 0;
}

export fn whisper_decode(
    model_handle: u32,
    dec_ids_ptr: [*]const i64,
    dec_ids_len: web_host.HostLen,
    enc_hidden_ptr: [*]const f32,
    enc_hidden_len: web_host.HostLen,
    enc_mask_ptr: [*]const i64,
    enc_mask_len: web_host.HostLen,
    batch_size: u32,
    dec_seq: u32,
    enc_seq: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_whisper.decode(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, dec_ids_ptr, dec_ids_len),
        web_host.sliceConst(f32, enc_hidden_ptr, enc_hidden_len),
        web_host.sliceConst(i64, enc_mask_ptr, enc_mask_len),
        batch_size,
        dec_seq,
        enc_seq,
        out_ptr,
    ) catch return 0;
}

export fn load_model_clap(
    st_ptr: [*]const u8,
    st_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_clap.load(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, st_ptr, st_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn clap_embed_text(
    model_handle: u32,
    ids_ptr: [*]const i64,
    ids_len: web_host.HostLen,
    mask_ptr: [*]const i64,
    mask_len: web_host.HostLen,
    batch_size: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_clap.embedText(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, ids_ptr, ids_len),
        web_host.sliceConst(i64, mask_ptr, mask_len),
        batch_size,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn clap_embed_audio(
    model_handle: u32,
    features_ptr: [*]const f32,
    features_len: web_host.HostLen,
    batch_size: u32,
    channels: u32,
    time_frames: u32,
    mel_bins: u32,
    is_longer_ptr: [*]const u8,
    is_longer_len: web_host.HostLen,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_clap.embedAudio(
        ctx.allocator,
        model,
        web_host.sliceConst(f32, features_ptr, features_len),
        batch_size,
        channels,
        time_frames,
        mel_bins,
        web_host.sliceConst(u8, is_longer_ptr, is_longer_len),
        out_ptr,
    ) catch return 0;
}

export fn load_model_florence(
    st_ptr: [*]const u8,
    st_len: web_host.HostLen,
    config_json_ptr: [*]const u8,
    config_json_len: web_host.HostLen,
) u32 {
    return web_florence.load(
        ctx.allocator,
        &ctx.runtime,
        web_host.sliceConst(u8, st_ptr, st_len),
        web_host.sliceConst(u8, config_json_ptr, config_json_len),
    ) catch return 0;
}

export fn florence_encode(
    model_handle: u32,
    pixel_ptr: [*]const f32,
    pixel_len: web_host.HostLen,
    prompt_ids_ptr: [*]const i64,
    prompt_ids_len: web_host.HostLen,
    batch_size: u32,
    out_ptr: [*]f32,
    out_enc_seq_ptr: [*]u32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_florence.encode(
        ctx.allocator,
        model,
        web_host.sliceConst(f32, pixel_ptr, pixel_len),
        web_host.sliceConst(i64, prompt_ids_ptr, prompt_ids_len),
        batch_size,
        out_ptr,
        out_enc_seq_ptr,
    ) catch return 0;
}

export fn florence_encode_text(
    model_handle: u32,
    ids_ptr: [*]const i64,
    ids_len: web_host.HostLen,
    batch_size: u32,
    seq_len: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_florence.encodeText(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, ids_ptr, ids_len),
        batch_size,
        seq_len,
        out_ptr,
    ) catch return 0;
}

export fn florence_decode(
    model_handle: u32,
    dec_ids_ptr: [*]const i64,
    dec_ids_len: web_host.HostLen,
    enc_hidden_ptr: [*]const f32,
    enc_hidden_len: web_host.HostLen,
    enc_mask_ptr: [*]const i64,
    enc_mask_len: web_host.HostLen,
    batch_size: u32,
    dec_seq: u32,
    enc_seq: u32,
    out_ptr: [*]f32,
) u32 {
    const model = ctx.getModel(model_handle) catch return 0;
    return web_florence.decode(
        ctx.allocator,
        model,
        web_host.sliceConst(i64, dec_ids_ptr, dec_ids_len),
        web_host.sliceConst(f32, enc_hidden_ptr, enc_hidden_len),
        web_host.sliceConst(i64, enc_mask_ptr, enc_mask_len),
        batch_size,
        dec_seq,
        enc_seq,
        out_ptr,
    ) catch return 0;
}
