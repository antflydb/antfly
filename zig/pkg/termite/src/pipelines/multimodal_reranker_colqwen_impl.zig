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
const backends = @import("../backends/backends.zig");
const Tensor = backends.Tensor;
const tokenizer_mod = @import("termite_tokenizer");
const Tokenizer = tokenizer_mod.Tokenizer;
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;
const gpt_mod = @import("../models/gpt.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const qwen2vl = @import("multimodal_qwen_adapter.zig");
const qwen2vl_vision = @import("../architectures/qwen2vl_vision.zig");
const reranking = @import("reranking.zig");
const runtime = @import("../runtime/root.zig");

// Internal ColQwen/Qwen2-VL implementation for the task-oriented
// `multimodal_reranker.zig` surface.

pub const PromptConfig = struct {
    query_prefix: []const u8 = "Query -- ",
    document_prefix: []const u8 = "<|im_start|>user\n",
    image_marker_prefix: []const u8 = "<|vision_start|><|image_pad|><|vision_end|>",
    document_suffix: []const u8 = "<|im_end|><|endoftext|>",
};

pub const EncodedSequence = struct {
    allocator: std.mem.Allocator,
    input_ids: []i32,
    attention_mask: []i32,
    hidden_states: []f32,
    hidden_size: usize,

    pub fn deinit(self: *EncodedSequence) void {
        self.allocator.free(self.input_ids);
        self.allocator.free(self.attention_mask);
        self.allocator.free(self.hidden_states);
    }
};

pub const NativeImageEmbedding = struct {
    tensor: CT,
    token_count: usize,
};

pub const Config = struct {
    prompt: PromptConfig = .{},
    distributed: runtime.distributed.Config = .{},
};

pub const Pipeline = struct {
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    vision_session: ?backends.Session,
    tok: Tokenizer,
    gpt_cfg: gpt_mod.Config,
    prep_cfg: qwen2vl.PreprocessorConfig,
    max_length: usize,
    add_bos_token: bool,
    config: Config = .{},

    pub fn init(
        allocator: std.mem.Allocator,
        cb: *const ComputeBackend,
        vision_session: ?backends.Session,
        tok: Tokenizer,
        gpt_cfg: gpt_mod.Config,
        prep_cfg: qwen2vl.PreprocessorConfig,
        max_length: usize,
        add_bos_token: bool,
        config: Config,
    ) Pipeline {
        return .{
            .allocator = allocator,
            .cb = cb,
            .vision_session = vision_session,
            .tok = tok,
            .gpt_cfg = gpt_cfg,
            .prep_cfg = prep_cfg,
            .max_length = max_length,
            .add_bos_token = add_bos_token,
            .config = config,
        };
    }

    pub fn usesDistributedGpuHosted(self: *const Pipeline) bool {
        return self.config.distributed.enabled and
            self.config.distributed.world_size > 1 and
            self.cb.kind() == .metal;
    }

    pub fn usesTensorParallelGpuHosted(self: *const Pipeline) bool {
        return self.usesDistributedGpuHosted() and self.config.distributed.mode == .tensor_parallel;
    }

    pub fn encodeQueryText(self: *const Pipeline, query: []const u8) !EncodedSequence {
        return encodeQuery(
            self.cb,
            self.allocator,
            self.tok,
            self.config.prompt,
            self.gpt_cfg,
            query,
            self.max_length,
            self.add_bos_token,
        );
    }

    pub fn scoreDocumentText(self: *const Pipeline, query: EncodedSequence, document_text: []const u8, images: []const []const u8) !f32 {
        return scoreDocument(
            self.cb,
            self.allocator,
            self.vision_session,
            self.tok,
            self.config.prompt,
            self.gpt_cfg,
            self.prep_cfg,
            query,
            document_text,
            images,
            self.max_length,
            self.add_bos_token,
        );
    }
};

pub fn encodeQuery(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tok: Tokenizer,
    prompt_cfg: PromptConfig,
    gpt_cfg: gpt_mod.Config,
    query: []const u8,
    max_length: usize,
    add_bos_token: bool,
) !EncodedSequence {
    const full = try std.fmt.allocPrint(allocator, "{s}{s}", .{ prompt_cfg.query_prefix, query });
    defer allocator.free(full);

    var encoded = try tok.encodeForGenerationConfigured(allocator, full, max_length, add_bos_token);
    errdefer encoded.deinit();

    const ids_i64 = try allocator.alloc(i64, encoded.ids.len);
    defer allocator.free(ids_i64);
    for (encoded.ids, 0..) |id, idx| ids_i64[idx] = id;

    const hidden = try gpt_arch.hiddenForward(cb, allocator, gpt_cfg, ids_i64, 1, encoded.ids.len, null);
    const projected = try applyRetrievalProjection(cb, allocator, hidden, encoded.ids.len, @intCast(gpt_cfg.hidden_size));
    allocator.free(hidden);
    return .{
        .allocator = allocator,
        .input_ids = encoded.ids,
        .attention_mask = encoded.attention_mask,
        .hidden_states = projected.hidden_states,
        .hidden_size = projected.hidden_size,
    };
}

pub fn scoreDocument(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    vision_session: ?backends.Session,
    tok: Tokenizer,
    prompt_cfg: PromptConfig,
    gpt_cfg: gpt_mod.Config,
    prep_cfg: qwen2vl.PreprocessorConfig,
    query: EncodedSequence,
    document_text: []const u8,
    images: []const []const u8,
    max_length: usize,
    add_bos_token: bool,
) !f32 {
    if (images.len == 0) return error.NoImages;

    var prepared_images = std.ArrayListUnmanaged(qwen2vl.PreparedImage).empty;
    defer {
        for (prepared_images.items) |*img| img.deinit();
        prepared_images.deinit(allocator);
    }
    for (images) |img| {
        try prepared_images.append(allocator, try qwen2vl.prepareImage(allocator, img, prep_cfg));
    }

    var prepared_doc = try prepareDocumentPrompt(allocator, tok, prompt_cfg, gpt_cfg, prepared_images.items, document_text, max_length, add_bos_token);
    defer prepared_doc.deinit();

    var image_embeddings = std.ArrayListUnmanaged([]f32).empty;
    var native_image_embeddings = std.ArrayListUnmanaged(NativeImageEmbedding).empty;
    defer {
        for (image_embeddings.items) |emb| allocator.free(emb);
        image_embeddings.deinit(allocator);
        for (native_image_embeddings.items) |emb| cb.free(emb.tensor);
        native_image_embeddings.deinit(allocator);
    }
    if (vision_session == null) {
        for (prepared_images.items) |prepared| {
            try native_image_embeddings.append(allocator, .{
                .tensor = try qwen2vl_vision.encodePreparedImageTokensTensor(cb, allocator, gpt_cfg, prep_cfg, prepared),
                .token_count = prepared.image_token_count,
            });
        }
    } else {
        for (prepared_images.items) |prepared| {
            try image_embeddings.append(allocator, try encodeImageTokens(cb, allocator, vision_session, gpt_cfg, prep_cfg, prepared));
        }
    }

    var doc_encoded = if (vision_session == null)
        try encodeDocumentFromPreparedNative(cb, allocator, gpt_cfg, prepared_doc, native_image_embeddings.items)
    else
        try encodeDocumentFromPrepared(cb, allocator, gpt_cfg, prepared_doc, image_embeddings.items);
    defer doc_encoded.deinit();

    return reranking.lateInteractionScore(
        query.hidden_states,
        query.input_ids,
        query.attention_mask,
        doc_encoded.hidden_states,
        doc_encoded.input_ids,
        doc_encoded.attention_mask,
        query.hidden_size,
        tok.specialTokens(),
    );
}

pub fn prepareDocumentPrompt(
    allocator: std.mem.Allocator,
    tok: Tokenizer,
    prompt_cfg: PromptConfig,
    config: gpt_mod.Config,
    prepared_images: []const qwen2vl.PreparedImage,
    document_text: []const u8,
    max_length: usize,
    add_bos_token: bool,
) !qwen2vl.PreparedTextInput {
    const prompt = try buildDocumentPromptString(allocator, prompt_cfg, prepared_images.len, document_text);
    defer allocator.free(prompt);

    const counts = try allocator.alloc(usize, prepared_images.len);
    defer allocator.free(counts);
    for (prepared_images, 0..) |prepared, idx| counts[idx] = prepared.image_token_count;
    return encodeDocumentPromptWithImageExpansion(allocator, tok, prompt, config, counts, max_length, add_bos_token);
}

fn buildDocumentPromptString(
    allocator: std.mem.Allocator,
    prompt_cfg: PromptConfig,
    image_count: usize,
    document_text: []const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);

    try out.appendSlice(allocator, prompt_cfg.document_prefix);
    for (0..image_count) |_| {
        try out.appendSlice(allocator, prompt_cfg.image_marker_prefix);
        if (document_text.len > 0) try out.append(allocator, '\n');
    }
    if (document_text.len > 0) try out.appendSlice(allocator, document_text);
    try out.appendSlice(allocator, prompt_cfg.document_suffix);
    return try out.toOwnedSlice(allocator);
}

fn encodeDocumentPromptWithImageExpansion(
    allocator: std.mem.Allocator,
    tok: Tokenizer,
    prompt: []const u8,
    config: gpt_mod.Config,
    image_token_counts: []const usize,
    max_length: usize,
    add_bos_token: bool,
) !qwen2vl.PreparedTextInput {
    const marker = "<|image_pad|>";
    const prefix_bos: usize = if (add_bos_token and tok.specialTokens().cls_id >= 0 and max_length > 0) 1 else 0;

    const ids = try allocator.alloc(i32, max_length);
    errdefer allocator.free(ids);
    const mask = try allocator.alloc(i32, max_length);
    errdefer allocator.free(mask);

    var pos: usize = 0;
    if (prefix_bos == 1) {
        ids[0] = tok.specialTokens().cls_id;
        mask[0] = 1;
        pos = 1;
    }

    var remaining_prompt = prompt;
    var image_index: usize = 0;
    while (std.mem.indexOf(u8, remaining_prompt, marker)) |split| {
        const before = remaining_prompt[0..split];
        const before_ids = try tok.encode(allocator, before);
        defer allocator.free(before_ids);
        pos = appendSliceInto(ids, mask, pos, max_length, before_ids);
        if (image_index >= image_token_counts.len) return error.ImagePlaceholderCountMismatch;
        for (0..image_token_counts[image_index]) |_| {
            if (pos >= max_length) break;
            ids[pos] = config.image_token_index;
            mask[pos] = 1;
            pos += 1;
        }
        image_index += 1;
        remaining_prompt = remaining_prompt[split + marker.len ..];
    }

    const tail_ids = try tok.encode(allocator, remaining_prompt);
    defer allocator.free(tail_ids);
    pos = appendSliceInto(ids, mask, pos, max_length, tail_ids);
    if (image_index != image_token_counts.len) return error.ImagePlaceholderCountMismatch;

    for (pos..max_length) |i| {
        ids[i] = tok.specialTokens().pad_id;
        mask[i] = 0;
    }
    return .{ .allocator = allocator, .input_ids = ids, .attention_mask = mask };
}

pub fn encodeImageTokens(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    vision_session: ?backends.Session,
    gpt_cfg: gpt_mod.Config,
    prep_cfg: qwen2vl.PreprocessorConfig,
    prepared: qwen2vl.PreparedImage,
) ![]f32 {
    if (vision_session) |vs| {
        return runVisionSession(allocator, vs, prepared, gpt_cfg.hidden_size);
    }
    return qwen2vl_vision.encodePreparedImageTokens(cb, allocator, gpt_cfg, prep_cfg, prepared);
}

const appendSliceInto = qwen2vl.appendSliceInto;

fn runVisionSession(
    allocator: std.mem.Allocator,
    vision_session: backends.Session,
    prepared: qwen2vl.PreparedImage,
    expected_hidden_size: u32,
) ![]f32 {
    const input_info = vision_session.inputInfo();
    var needs_grid = false;
    for (input_info) |info| {
        if (std.mem.eql(u8, info.name, "image_grid_thw")) needs_grid = true;
    }

    const pixel_shape = [_]i64{ 1, 3, @intCast(prepared.resized_height), @intCast(prepared.resized_width) };
    var pixel_tensor = try Tensor.initFloat32(allocator, "pixel_values", &pixel_shape, prepared.pixel_values);
    defer pixel_tensor.deinit();

    var grid_tensor: ?Tensor = null;
    defer if (grid_tensor) |*tensor| tensor.deinit();
    var grid_values: [3]i64 = .{
        @intCast(prepared.image_grid_thw[0]),
        @intCast(prepared.image_grid_thw[1]),
        @intCast(prepared.image_grid_thw[2]),
    };
    const inputs = if (needs_grid) blk: {
        const grid_shape = [_]i64{ 1, 3 };
        grid_tensor = try Tensor.initInt64(allocator, "image_grid_thw", &grid_shape, grid_values[0..]);
        break :blk &[_]Tensor{ pixel_tensor, grid_tensor.? };
    } else &[_]Tensor{pixel_tensor};

    const outputs = try vision_session.run(inputs, allocator);
    defer {
        for (outputs) |*output| output.deinit();
        allocator.free(outputs);
    }
    if (outputs.len == 0) return error.NoOutputTensors;

    const first = outputs[0];
    const data = first.asFloat32();
    const shape = first.shape;

    var token_count: usize = 0;
    var hidden_size: usize = 0;
    if (shape.len == 3) {
        if (shape[0] != 1) return error.UnexpectedOutputShape;
        token_count = @intCast(shape[1]);
        hidden_size = @intCast(shape[2]);
    } else if (shape.len == 2) {
        token_count = @intCast(shape[0]);
        hidden_size = @intCast(shape[1]);
    } else {
        return error.UnexpectedOutputShape;
    }

    if (token_count != prepared.image_token_count) return error.ImageTokenLengthMismatch;
    if (hidden_size != expected_hidden_size) return error.ImageProjectionSizeMismatch;
    return try allocator.dupe(f32, data[0 .. token_count * hidden_size]);
}

pub fn encodeDocumentFromPrepared(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_cfg: gpt_mod.Config,
    prepared_doc: qwen2vl.PreparedTextInput,
    image_embeddings: []const []const f32,
) !EncodedSequence {
    const ids_i64 = try allocator.alloc(i64, prepared_doc.input_ids.len);
    defer allocator.free(ids_i64);
    for (prepared_doc.input_ids, 0..) |id, idx| ids_i64[idx] = id;

    const embed_w = try getEmbeddingWeight(cb, gpt_cfg);
    defer cb.free(embed_w);
    const base_embeddings = try cb.embeddingLookup(embed_w, ids_i64, ids_i64.len, gpt_cfg.hidden_size);
    defer cb.free(base_embeddings);

    const hidden_size: usize = @intCast(gpt_cfg.hidden_size);
    const prompt_embeddings = try cb.toFloat32(base_embeddings, allocator);
    errdefer allocator.free(prompt_embeddings);

    const embedding_scale = gpt_cfg.tokenEmbeddingScale();
    if (!std.math.approxEqAbs(f32, embedding_scale, 1.0, 1e-6)) {
        for (prompt_embeddings) |*value| value.* *= embedding_scale;
    }

    var image_index: usize = 0;
    var pos: usize = 0;
    while (pos < prepared_doc.input_ids.len) {
        if (prepared_doc.input_ids[pos] != gpt_cfg.image_token_index) {
            pos += 1;
            continue;
        }
        if (image_index >= image_embeddings.len) return error.ImagePlaceholderCountMismatch;

        const image_emb = image_embeddings[image_index];
        if (image_emb.len % hidden_size != 0) return error.ImageProjectionSizeMismatch;
        const tokens = image_emb.len / hidden_size;
        if (tokens == 0) return error.ImageTokenLengthMismatch;

        var run_len: usize = 0;
        while (pos + run_len < prepared_doc.input_ids.len and prepared_doc.input_ids[pos + run_len] == gpt_cfg.image_token_index) : (run_len += 1) {}
        if (run_len > tokens) return error.ImagePlaceholderCountMismatch;

        const copy_len = run_len * hidden_size;
        @memcpy(prompt_embeddings[pos * hidden_size ..][0..copy_len], image_emb[0..copy_len]);
        pos += run_len;
        image_index += 1;
    }
    if (image_index != image_embeddings.len) return error.ImagePlaceholderCountMismatch;

    const embedding_shape = [_]i32{ @intCast(prepared_doc.input_ids.len), @intCast(hidden_size) };
    const input_embeddings = try cb.fromFloat32Shape(prompt_embeddings, &embedding_shape);
    allocator.free(prompt_embeddings);

    const hidden = try gpt_arch.hiddenForwardFromEmbeddings(cb, allocator, gpt_cfg, input_embeddings, 1, prepared_doc.input_ids.len, null, null);
    const projected = try applyRetrievalProjection(cb, allocator, hidden, prepared_doc.input_ids.len, hidden_size);
    allocator.free(hidden);
    return .{
        .allocator = allocator,
        .input_ids = try allocator.dupe(i32, prepared_doc.input_ids),
        .attention_mask = try allocator.dupe(i32, prepared_doc.attention_mask),
        .hidden_states = projected.hidden_states,
        .hidden_size = projected.hidden_size,
    };
}

pub fn encodeDocumentFromPreparedNative(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_cfg: gpt_mod.Config,
    prepared_doc: qwen2vl.PreparedTextInput,
    image_embeddings: []const NativeImageEmbedding,
) !EncodedSequence {
    const embedding_scale = gpt_cfg.tokenEmbeddingScale();
    if (!std.math.approxEqAbs(f32, embedding_scale, 1.0, 1e-6)) return error.UnsupportedEmbeddingScale;

    const embed_w = try getEmbeddingWeight(cb, gpt_cfg);
    defer cb.free(embed_w);
    const hidden_size: usize = @intCast(gpt_cfg.hidden_size);

    var current: ?CT = null;
    var current_rows: usize = 0;
    defer if (current) |tensor| cb.free(tensor);

    var image_index: usize = 0;
    var pos: usize = 0;
    while (pos < prepared_doc.input_ids.len) {
        if (prepared_doc.input_ids[pos] == gpt_cfg.image_token_index) {
            if (image_index >= image_embeddings.len) return error.ImagePlaceholderCountMismatch;
            const image_emb = image_embeddings[image_index];
            var run_len: usize = 0;
            while (pos + run_len < prepared_doc.input_ids.len and prepared_doc.input_ids[pos + run_len] == gpt_cfg.image_token_index) : (run_len += 1) {}
            if (run_len > image_emb.token_count) return error.ImagePlaceholderCountMismatch;

            const image_chunk = if (run_len == image_emb.token_count)
                image_emb.tensor
            else
                try cb.sliceRows2D(allocator, image_emb.tensor, 0, run_len, hidden_size);
            var image_chunk_owned = run_len != image_emb.token_count;
            defer if (image_chunk_owned) cb.free(image_chunk);

            if (current == null) {
                current = if (image_chunk_owned)
                    image_chunk
                else
                    try cloneEmbeddingChunk(cb, allocator, image_chunk, run_len, hidden_size);
                current_rows = run_len;
                if (image_chunk_owned) image_chunk_owned = false;
            } else {
                current = try appendEmbeddingChunk(cb, allocator, current, current_rows, image_chunk, run_len, hidden_size);
                current_rows += run_len;
            }
            pos += run_len;
            image_index += 1;
            continue;
        }

        const start = pos;
        while (pos < prepared_doc.input_ids.len and prepared_doc.input_ids[pos] != gpt_cfg.image_token_index) : (pos += 1) {}
        const text_rows = pos - start;
        const text_chunk = try embedTextChunk(cb, allocator, embed_w, prepared_doc.input_ids[start..pos], hidden_size);
        var text_chunk_owned = true;
        defer if (text_chunk_owned) cb.free(text_chunk);
        if (current == null) {
            current = text_chunk;
            current_rows = text_rows;
            text_chunk_owned = false;
        } else {
            current = try appendEmbeddingChunk(cb, allocator, current, current_rows, text_chunk, text_rows, hidden_size);
            current_rows += text_rows;
        }
    }
    if (image_index != image_embeddings.len) return error.ImagePlaceholderCountMismatch;
    if (current_rows != prepared_doc.input_ids.len) return error.ImagePlaceholderCountMismatch;

    const input_embeddings = current orelse return error.ImagePlaceholderCountMismatch;
    current = null;
    const hidden = try gpt_arch.hiddenForwardFromEmbeddings(cb, allocator, gpt_cfg, input_embeddings, 1, prepared_doc.input_ids.len, null, null);
    const projected = try applyRetrievalProjection(cb, allocator, hidden, prepared_doc.input_ids.len, hidden_size);
    allocator.free(hidden);
    return .{
        .allocator = allocator,
        .input_ids = try allocator.dupe(i32, prepared_doc.input_ids),
        .attention_mask = try allocator.dupe(i32, prepared_doc.attention_mask),
        .hidden_states = projected.hidden_states,
        .hidden_size = projected.hidden_size,
    };
}

fn embedTextChunk(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    embed_w: CT,
    ids: []const i32,
    hidden_size: usize,
) !CT {
    const ids_i64 = try allocator.alloc(i64, ids.len);
    defer allocator.free(ids_i64);
    for (ids, 0..) |id, idx| ids_i64[idx] = id;
    return cb.embeddingLookup(embed_w, ids_i64, ids_i64.len, hidden_size);
}

fn appendEmbeddingChunk(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    current: ?CT,
    current_rows: usize,
    chunk: CT,
    chunk_rows: usize,
    hidden_size: usize,
) !CT {
    if (current == null) {
        return cb.reshape2D(allocator, chunk, chunk_rows, hidden_size, chunk_rows, hidden_size);
    }
    const lhs = current.?;
    const result = try cb.concatRows2D(allocator, lhs, chunk, current_rows, chunk_rows, hidden_size);
    errdefer cb.free(result);
    const materialized = try cloneEmbeddingChunk(cb, allocator, result, current_rows + chunk_rows, hidden_size);
    cb.free(result);
    cb.free(lhs);
    return materialized;
}

fn cloneEmbeddingChunk(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    chunk: CT,
    rows: usize,
    hidden_size: usize,
) !CT {
    const data = try cb.toFloat32(chunk, allocator);
    defer allocator.free(data);
    if (data.len != rows * hidden_size) return error.ImageProjectionSizeMismatch;
    const shape = [_]i32{ @intCast(rows), @intCast(hidden_size) };
    return cb.fromFloat32Shape(data, &shape);
}

const ProjectedStates = struct {
    hidden_states: []f32,
    hidden_size: usize,
};

fn applyRetrievalProjection(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden_states: []const f32,
    token_count: usize,
    input_hidden_size: usize,
) !ProjectedStates {
    const proj_w = cb.getWeight("embedding_proj_layer.weight") catch |err| switch (err) {
        error.MissingWeight => return .{
            .hidden_states = try allocator.dupe(f32, hidden_states),
            .hidden_size = input_hidden_size,
        },
        else => return err,
    };
    defer cb.free(proj_w);
    const proj_b = try cb.getWeight("embedding_proj_layer.bias");
    defer cb.free(proj_b);

    const bias_data = try cb.toFloat32(proj_b, allocator);
    defer allocator.free(bias_data);
    const output_hidden_size = bias_data.len;
    if (output_hidden_size == 0) return error.InvalidProjectionShape;
    if (hidden_states.len != token_count * input_hidden_size) return error.UnexpectedOutputShape;

    const shape = [_]i32{ @intCast(token_count), @intCast(input_hidden_size) };
    const input_ct = try cb.fromFloat32Shape(hidden_states, &shape);
    defer cb.free(input_ct);
    const projected_ct = try cb.linear(input_ct, proj_w, proj_b, token_count, input_hidden_size, output_hidden_size);
    defer cb.free(projected_ct);
    return .{
        .hidden_states = try cb.toFloat32(projected_ct, allocator),
        .hidden_size = output_hidden_size,
    };
}

fn getEmbeddingWeight(cb: *const ComputeBackend, config: gpt_mod.Config) !ops.CT {
    if (config.weight_prefix.len != 0) {
        var buf: [256]u8 = undefined;
        const prefixed = std.fmt.bufPrint(&buf, "{s}.embed_tokens.weight", .{config.weight_prefix}) catch return error.NameTooLong;
        return cb.getWeight(prefixed) catch try cb.getWeight("model.embed_tokens.weight");
    }
    return switch (config.family) {
        .gpt2 => cb.getWeight("wte.weight"),
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .phi => cb.getWeight("model.embed_tokens.weight"),
        else => cb.getWeight("model.embed_tokens.weight") catch try cb.getWeight("wte.weight"),
    };
}

test "encodeDocumentPromptWithImageExpansion truncates image placeholder run to max length" {
    const allocator = std.testing.allocator;

    const TestTokenizer = struct {
        fn encode(_: @This(), alloc: std.mem.Allocator, text: []const u8) ![]i32 {
            if (std.mem.eql(u8, text, "<|im_start|>user\n<|vision_start|>")) {
                return alloc.dupe(i32, &.{ 101, 102 });
            }
            if (std.mem.eql(u8, text, "<|vision_end|><|im_end|><|endoftext|>")) {
                return alloc.dupe(i32, &.{ 201, 202, 203 });
            }
            return alloc.dupe(i32, &.{999});
        }

        fn specialTokens(_: @This()) tokenizer_mod.SpecialTokens {
            return .{
                .pad_id = 0,
                .cls_id = -1,
                .sep_id = -1,
                .unk_id = -1,
                .mask_id = -1,
            };
        }

        fn encodeGeneration(self: @This(), alloc: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) !tokenizer_mod.EncodeResult {
            _ = max_length;
            _ = add_bos_token;
            return .{
                .ids = try self.encode(alloc, text),
                .attention_mask = try alloc.dupe(i32, &.{1}),
                .allocator = alloc,
            };
        }

        fn encodeForModel(self: @This(), alloc: std.mem.Allocator, text: []const u8, max_length: usize) !tokenizer_mod.EncodeResult {
            _ = max_length;
            return .{
                .ids = try self.encode(alloc, text),
                .attention_mask = try alloc.dupe(i32, &.{1}),
                .allocator = alloc,
            };
        }

        fn vocabSize(_: @This()) usize {
            return 1024;
        }

        fn deinit(_: @This()) void {}
    };

    const tok = Tokenizer{ .ptr = @constCast(&TestTokenizer{}), .vtable = &.{
        .encode = struct {
            fn f(ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8) anyerror![]i32 {
                const self: *TestTokenizer = @ptrCast(@alignCast(ptr));
                return self.encode(alloc, text);
            }
        }.f,
        .encodeInto = struct {
            fn f(ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) anyerror!void {
                const self: *TestTokenizer = @ptrCast(@alignCast(ptr));
                const ids = try self.encode(alloc, text);
                defer alloc.free(ids);
                try out.appendSlice(alloc, ids);
            }
        }.f,
        .encodeForModel = struct {
            fn f(ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8, max_length: usize) anyerror!tokenizer_mod.EncodeResult {
                const self: *TestTokenizer = @ptrCast(@alignCast(ptr));
                return self.encodeForModel(alloc, text, max_length);
            }
        }.f,
        .encodeGeneration = struct {
            fn f(ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) anyerror!tokenizer_mod.EncodeResult {
                const self: *TestTokenizer = @ptrCast(@alignCast(ptr));
                return self.encodeGeneration(alloc, text, max_length, add_bos_token);
            }
        }.f,
        .decode = struct {
            fn f(_: *anyopaque, _: std.mem.Allocator, _: []const i32) anyerror![]u8 {
                return error.UnsupportedOperation;
            }
        }.f,
        .specialTokens = struct {
            fn f(ptr: *anyopaque) tokenizer_mod.SpecialTokens {
                const self: *TestTokenizer = @ptrCast(@alignCast(ptr));
                return self.specialTokens();
            }
        }.f,
        .vocabSize = struct {
            fn f(ptr: *anyopaque) usize {
                const self: *TestTokenizer = @ptrCast(@alignCast(ptr));
                return self.vocabSize();
            }
        }.f,
        .deinit = struct {
            fn f(ptr: *anyopaque) void {
                const self: *TestTokenizer = @ptrCast(@alignCast(ptr));
                self.deinit();
            }
        }.f,
    } };

    const cfg = gpt_mod.Config{
        .family = .qwen2,
        .hidden_size = 128,
        .num_hidden_layers = 1,
        .num_attention_heads = 1,
        .intermediate_size = 128,
        .vocab_size = 1024,
        .max_position_embeddings = 4096,
        .image_token_index = 7,
    };

    var prepared = try encodeDocumentPromptWithImageExpansion(
        allocator,
        tok,
        "<|im_start|>user\n<|vision_start|><|image_pad|><|vision_end|><|im_end|><|endoftext|>",
        cfg,
        &.{729},
        512,
        false,
    );
    defer prepared.deinit();

    try std.testing.expectEqual(@as(usize, 512), prepared.input_ids.len);
    try std.testing.expectEqual(@as(i32, 101), prepared.input_ids[0]);
    try std.testing.expectEqual(@as(i32, 102), prepared.input_ids[1]);
    for (prepared.input_ids[2..]) |id| {
        try std.testing.expectEqual(@as(i32, 7), id);
    }
    for (prepared.attention_mask) |value| {
        try std.testing.expectEqual(@as(i32, 1), value);
    }
}
