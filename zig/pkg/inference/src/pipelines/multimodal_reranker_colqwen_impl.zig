// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
const backends = @import("../backends/backends.zig");
const Tensor = backends.Tensor;
const tokenizer_mod = @import("inference_tokenizer");
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
const decoder_prefill = @import("../execution/decoder_prefill.zig");

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
    execution_control: ?InferenceExecutionControl = null,

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
        if (self.execution_control) |control| try control.update(.tokenizing, 0, 1);
        return encodeQuery(
            self.cb,
            self.allocator,
            self.tok,
            self.config.prompt,
            self.gpt_cfg,
            query,
            self.max_length,
            self.add_bos_token,
            self.execution_control,
        );
    }

    /// Returns the largest exact non-padding text-model row used by a
    /// query/document score, including expanded visual placeholder tokens.
    pub fn maxInputTokensPerItem(
        self: *const Pipeline,
        query: []const u8,
        document_text: []const u8,
        images: []const []const u8,
    ) !usize {
        if (images.len == 0) return error.NoImages;
        const full_query = try std.fmt.allocPrint(
            self.allocator,
            "{s}{s}",
            .{ self.config.prompt.query_prefix, query },
        );
        defer self.allocator.free(full_query);
        var encoded_query = try self.tok.encodeForGenerationConfigured(
            self.allocator,
            full_query,
            self.max_length,
            self.add_bos_token,
        );
        defer encoded_query.deinit();

        var prepared_images = std.ArrayListUnmanaged(qwen2vl.PreparedImage).empty;
        defer {
            for (prepared_images.items) |*image| image.deinit();
            prepared_images.deinit(self.allocator);
        }
        try prepared_images.ensureTotalCapacityPrecise(self.allocator, images.len);
        for (images) |image| {
            prepared_images.appendAssumeCapacity(try qwen2vl.prepareImage(self.allocator, image, self.prep_cfg));
        }
        var prepared_document = try prepareDocumentPrompt(
            self.allocator,
            self.tok,
            self.config.prompt,
            self.gpt_cfg,
            prepared_images.items,
            document_text,
            self.max_length,
            self.add_bos_token,
        );
        defer prepared_document.deinit();

        return @max(
            activeTokenLength(encoded_query.attention_mask),
            activeTokenLength(prepared_document.attention_mask),
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
            self.execution_control,
        );
    }
};

fn activeTokenLength(mask: []const i32) usize {
    var last_active: usize = 0;
    var found = false;
    for (mask, 0..) |value, index| {
        if (value == 0) continue;
        last_active = index;
        found = true;
    }
    return if (found) last_active + 1 else 0;
}

pub fn encodeQuery(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tok: Tokenizer,
    prompt_cfg: PromptConfig,
    gpt_cfg: gpt_mod.Config,
    query: []const u8,
    max_length: usize,
    add_bos_token: bool,
    execution_control: ?InferenceExecutionControl,
) !EncodedSequence {
    if (execution_control) |control| try control.check();
    const full = try std.fmt.allocPrint(allocator, "{s}{s}", .{ prompt_cfg.query_prefix, query });
    defer allocator.free(full);

    var encoded = try tok.encodeForGenerationConfigured(allocator, full, max_length, add_bos_token);
    errdefer encoded.deinit();

    const ids_i64 = try allocator.alloc(i64, encoded.ids.len);
    defer allocator.free(ids_i64);
    for (encoded.ids, 0..) |id, idx| ids_i64[idx] = id;

    var hidden = try decoder_prefill.ResidentHiddenRows.fromTokens(cb, allocator, gpt_cfg, ids_i64);
    defer hidden.deinit();
    if (execution_control) |control| try control.check();
    const projected = try applyResidentRetrievalProjection(cb, allocator, &hidden);
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
    execution_control: ?InferenceExecutionControl,
) !f32 {
    if (execution_control) |control| try control.check();
    if (images.len == 0) return error.NoImages;

    var prepared_images = std.ArrayListUnmanaged(qwen2vl.PreparedImage).empty;
    defer {
        for (prepared_images.items) |*img| img.deinit();
        prepared_images.deinit(allocator);
    }
    // Reserve list metadata before producing any owner. Appending a completed
    // image cannot then fail between its producer and the list cleanup owner.
    try prepared_images.ensureTotalCapacityPrecise(allocator, images.len);
    for (images, 0..) |img, index| {
        if (execution_control) |control|
            try control.update(.tokenizing, @intCast(index), @intCast(images.len));
        prepared_images.appendAssumeCapacity(try qwen2vl.prepareImage(allocator, img, prep_cfg));
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
        try native_image_embeddings.ensureTotalCapacityPrecise(allocator, prepared_images.items.len);
        for (prepared_images.items, 0..) |prepared, index| {
            if (execution_control) |control|
                try control.update(.executing, @intCast(index), @intCast(prepared_images.items.len));
            native_image_embeddings.appendAssumeCapacity(.{
                .tensor = try qwen2vl_vision.encodePreparedImageTokensTensor(cb, allocator, gpt_cfg, prep_cfg, prepared),
                .token_count = prepared.image_token_count,
            });
        }
    } else {
        try image_embeddings.ensureTotalCapacityPrecise(allocator, prepared_images.items.len);
        for (prepared_images.items, 0..) |prepared, index| {
            if (execution_control) |control|
                try control.update(.executing, @intCast(index), @intCast(prepared_images.items.len));
            image_embeddings.appendAssumeCapacity(try encodeImageTokens(cb, allocator, vision_session, gpt_cfg, prep_cfg, prepared, execution_control));
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
    execution_control: ?InferenceExecutionControl,
) ![]f32 {
    if (vision_session) |vs| {
        return runVisionSession(allocator, vs, prepared, gpt_cfg.hidden_size, execution_control);
    }
    return qwen2vl_vision.encodePreparedImageTokens(cb, allocator, gpt_cfg, prep_cfg, prepared);
}

const appendSliceInto = qwen2vl.appendSliceInto;

fn runVisionSession(
    allocator: std.mem.Allocator,
    vision_session: backends.Session,
    prepared: qwen2vl.PreparedImage,
    expected_hidden_size: u32,
    execution_control: ?InferenceExecutionControl,
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

    const outputs = try vision_session.runWithControl(inputs, allocator, execution_control);
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
    return encodeDocumentFromPreparedImpl(true, cb, allocator, gpt_cfg, prepared_doc, image_embeddings);
}

fn encodeDocumentFromPreparedImpl(
    comptime resident_tail: bool,
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
    const input_embeddings = embeddings: {
        const prompt_embeddings = try cb.toFloat32(base_embeddings, allocator);
        defer allocator.free(prompt_embeddings);

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
        break :embeddings try cb.fromFloat32Shape(prompt_embeddings, &embedding_shape);
    };

    if (comptime resident_tail) {
        // Host image production, prompt scaling and overwrite order stay above.
        // The decoder consumes this owned input; only final retrieval vectors
        // cross the host boundary when its physical F32 contract is supported.
        var hidden = try decoder_prefill.ResidentHiddenRows.fromEmbeddings(cb, allocator, gpt_cfg, input_embeddings, prepared_doc.input_ids.len);
        defer hidden.deinit();
        return finishEncodedSequence(allocator, prepared_doc, try applyResidentRetrievalProjection(cb, allocator, &hidden));
    }
    // Original host tail retained as the independent test oracle.
    const hidden = try gpt_arch.hiddenForwardFromEmbeddings(cb, allocator, gpt_cfg, input_embeddings, 1, prepared_doc.input_ids.len, null, null);
    defer allocator.free(hidden);
    const projected = try applyRetrievalProjection(cb, allocator, hidden, prepared_doc.input_ids.len, hidden_size);
    return finishEncodedSequence(allocator, prepared_doc, projected);
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
    if (prepared_doc.input_ids.len == 0) return error.ImagePlaceholderCountMismatch;

    var resident = resident: {
        var assembly = (try decoder_prefill.EmbeddingRows.init(cb, allocator, prepared_doc.input_ids.len, hidden_size)) orelse break :resident null;
        var transferred = false;
        defer if (!transferred) assembly.deinit();
        // Every supplied image is checked before the first copy, including its
        // actual device namespace. Opaque/packed/foreign-provider sources keep
        // the original host-compatible path without replaying submitted math.
        for (image_embeddings) |image| if (!try assembly.acceptsSource(image.tensor, image.token_count)) break :resident null;
        transferred = true;
        break :resident assembly;
    };
    defer if (resident) |*assembly| assembly.deinit();
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

            if (resident) |*assembly| {
                try assembly.append(image_emb.tensor, 0, run_len);
                current_rows += run_len;
                pos += run_len;
                image_index += 1;
                continue;
            }
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
        if (resident) |*assembly| {
            try assembly.append(text_chunk, 0, text_rows);
            current_rows += text_rows;
        } else if (current == null) {
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

    const input_embeddings = if (resident) |*assembly| try assembly.take() else current orelse return error.ImagePlaceholderCountMismatch;
    current = null;
    var hidden = try decoder_prefill.ResidentHiddenRows.fromEmbeddings(cb, allocator, gpt_cfg, input_embeddings, prepared_doc.input_ids.len);
    defer hidden.deinit();
    const projected = try applyResidentRetrievalProjection(cb, allocator, &hidden);
    return finishEncodedSequence(allocator, prepared_doc, projected);
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

fn finishEncodedSequence(allocator: std.mem.Allocator, prepared: qwen2vl.PreparedTextInput, projected: ProjectedStates) !EncodedSequence {
    errdefer allocator.free(projected.hidden_states);
    const ids = try allocator.dupe(i32, prepared.input_ids);
    errdefer allocator.free(ids);
    const mask = try allocator.dupe(i32, prepared.attention_mask);
    return .{ .allocator = allocator, .input_ids = ids, .attention_mask = mask, .hidden_states = projected.hidden_states, .hidden_size = projected.hidden_size };
}

fn applyResidentRetrievalProjection(cb: *const ComputeBackend, allocator: std.mem.Allocator, hidden: *const decoder_prefill.ResidentHiddenRows) !ProjectedStates {
    const weight = cb.getWeight("embedding_proj_layer.weight") catch |err| switch (err) {
        error.MissingWeight => return .{ .hidden_states = try hidden.exportHost(), .hidden_size = hidden.width },
        else => return err,
    };
    defer cb.free(weight);
    const bias = try cb.getWeight("embedding_proj_layer.bias");
    defer cb.free(bias);
    const width = hidden.projectionWidth(bias) catch |err| switch (err) {
        // Opaque providers without shape metadata retain the original host
        // projection route. No projection has submitted when shape query declines.
        error.UnsupportedShape, error.UnsupportedTensorType => {
            const values = try hidden.exportHost();
            defer allocator.free(values);
            return applyRetrievalProjection(cb, allocator, values, hidden.rows, hidden.width);
        },
        else => return err,
    };
    var projected = try hidden.project(weight, bias, width);
    defer projected.deinit();
    return .{ .hidden_states = try projected.exportHost(), .hidden_size = projected.width };
}

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

const DocumentPromptTestTokenizer = struct {
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

        pub fn deinit(_: @This()) void {}
    };

    fn make() Tokenizer {
        return Tokenizer{ .ptr = @constCast(&TestTokenizer{}), .vtable = &.{
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
    }
};

test "encodeDocumentPromptWithImageExpansion truncates image placeholder run to max length" {
    const allocator = std.testing.allocator;

    const tok = DocumentPromptTestTokenizer.make();

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

const ResidentDocumentFixture = struct {
    const helper = @import("../ops/media_residency_test.zig");
    const native = @import("../ops/native_compute.zig");
    const first_image = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const second_image = [_]f32{ -1, -2, -3, -4, -5, -6, -7, -8 };

    fn config() gpt_mod.Config {
        return .{ .family = .qwen2, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .num_key_value_heads = 1, .intermediate_size = 4, .vocab_size = 8, .position_encoding = .rope, .norm_type = .rms_norm, .image_token_index = 7 };
    }
    fn populateBase(store: *native.WeightStore) !void {
        try helper.putWeight(store, "model.embed_tokens.weight", &.{ 8, 4 }, null);
        try helper.putWeight(store, "model.norm.weight", &.{4}, 1);
    }
    fn populateProjection(store: *native.WeightStore) !void {
        try populateBase(store);
        try helper.putWeight(store, "embedding_proj_layer.weight", &.{ 3, 4 }, null);
        try helper.putWeight(store, "embedding_proj_layer.bias", &.{3}, null);
    }
    fn check(backend_value: ComputeBackend, _: ComputeBackend) !void {
        const a = std.testing.allocator;
        const cb = &backend_value;
        var ids = [_]i32{ 1, 7, 7, 2, 7, 3 };
        var mask = [_]i32{ 1, 1, 1, 1, 1, 1 };
        const prepared = qwen2vl.PreparedTextInput{ .allocator = a, .input_ids = &ids, .attention_mask = &mask };
        const first = try cb.fromFloat32Shape(&first_image, &.{ 3, 4 });
        defer cb.free(first);
        const second = try cb.fromFloat32Shape(&second_image, &.{ 2, 4 });
        defer cb.free(second);
        const images = [_]NativeImageEmbedding{ .{ .tensor = first, .token_count = 3 }, .{ .tensor = second, .token_count = 2 } };
        // Original genuine-host feature path supplies an independent assembly
        // oracle, including partial image runs and separated text segments.
        var reference = try encodeDocumentFromPreparedImpl(false, cb, a, config(), prepared, &.{ &first_image, &second_image });
        defer reference.deinit();
        var imported = try encodeDocumentFromPrepared(cb, a, config(), prepared, &.{ &first_image, &second_image });
        defer imported.deinit();
        try std.testing.expectEqual(reference.hidden_size, imported.hidden_size);
        if (cb.kind() == .native) {
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(reference.hidden_states), std.mem.sliceAsBytes(imported.hidden_states));
        } else {
            for (reference.hidden_states, imported.hidden_states) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
        }
        if (cb.kind() == .metal or cb.kind() == .cuda) {
            // The individually executed original/public calls above warm the
            // same real provider and weights before these request-local scopes.
            const transfer = @import("../execution/transfer_audit.zig");
            var host_audit = transfer.Audit{};
            var observed_host = measured: {
                try transfer.begin(&host_audit);
                defer transfer.end();
                break :measured try encodeDocumentFromPreparedImpl(false, cb, a, config(), prepared, &.{ &first_image, &second_image });
            };
            defer observed_host.deinit();
            var resident_audit = transfer.Audit{};
            var observed_resident = measured: {
                try transfer.begin(&resident_audit);
                defer transfer.end();
                break :measured try encodeDocumentFromPrepared(cb, a, config(), prepared, &.{ &first_image, &second_image });
            };
            defer observed_resident.deinit();
            for (observed_host.hidden_states, observed_resident.hidden_states) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
            if (reference.hidden_size == 3) {
                // This zero-layer Metal fixture follows the actual rank-two
                // input storage: ordinary host inputs keep norm/linear on the
                // host, while the existing upload policy yields device rows.
                // Its cached bias remains host-backed in either configuration.
                // CUDA always downloads the bias as well as the hidden packet.
                const device_rows = cb.kind() == .cuda or @import("../ops/metal_compute.zig").MetalCompute.debugHasDeviceTensor(cb, first);
                const hidden_bytes: u64 = if (device_rows) prepared.input_ids.len * config().hidden_size * @sizeOf(f32) else 0;
                const bias_bytes: u64 = if (cb.kind() == .cuda) reference.hidden_size * @sizeOf(f32) else 0;
                try std.testing.expectEqual(resident_audit.upload_bytes + hidden_bytes, host_audit.upload_bytes);
                try std.testing.expectEqual(resident_audit.download_bytes + hidden_bytes + bias_bytes, host_audit.download_bytes);
            } else {
                // Missing projection still exports the requested hidden rows;
                // no hidden re-upload or bias read existed in the old path.
                try std.testing.expectEqual(@as(usize, config().hidden_size), reference.hidden_size);
                try std.testing.expectEqual(host_audit.upload_bytes, resident_audit.upload_bytes);
                try std.testing.expectEqual(host_audit.download_bytes, resident_audit.download_bytes);
            }
        }
        const imported_held = try a.dupe(f32, imported.hidden_states);
        defer a.free(imported_held);
        var scaled = config();
        scaled.family = .gemma;
        var scaled_reference = try encodeDocumentFromPreparedImpl(false, cb, a, scaled, prepared, &.{ &first_image, &second_image });
        defer scaled_reference.deinit();
        var scaled_imported = try encodeDocumentFromPrepared(cb, a, scaled, prepared, &.{ &first_image, &second_image });
        defer scaled_imported.deinit();
        if (cb.kind() == .native) {
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(scaled_reference.hidden_states), std.mem.sliceAsBytes(scaled_imported.hidden_states));
        } else {
            for (scaled_reference.hidden_states, scaled_imported.hidden_states) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
        }
        var encoded = try encodeDocumentFromPreparedNative(cb, a, config(), prepared, &images);
        defer encoded.deinit();
        try std.testing.expectEqual(reference.hidden_size, encoded.hidden_size);
        try std.testing.expectEqualSlices(i32, reference.input_ids, encoded.input_ids);
        try std.testing.expectEqualSlices(i32, reference.attention_mask, encoded.attention_mask);
        if (cb.kind() == .native) {
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(reference.hidden_states), std.mem.sliceAsBytes(encoded.hidden_states));
        } else {
            for (reference.hidden_states, encoded.hidden_states) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
        }
        const query_ids = [_]i64{ 1, 2 };
        const query_hidden = try gpt_arch.hiddenForward(cb, a, config(), &query_ids, 1, query_ids.len, null);
        defer a.free(query_hidden);
        const reference_query = try applyRetrievalProjection(cb, a, query_hidden, query_ids.len, config().hidden_size);
        defer a.free(reference_query.hidden_states);
        var query_rows = try decoder_prefill.ResidentHiddenRows.fromTokens(cb, a, config(), &query_ids);
        defer query_rows.deinit();
        const resident_query = try applyResidentRetrievalProjection(cb, a, &query_rows);
        defer a.free(resident_query.hidden_states);
        try std.testing.expectEqual(reference_query.hidden_size, resident_query.hidden_size);
        if (cb.kind() == .native) {
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(reference_query.hidden_states), std.mem.sliceAsBytes(resident_query.hidden_states));
        } else {
            for (reference_query.hidden_states, resident_query.hidden_states) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
        }
        const special = tokenizer_mod.SpecialTokens{ .pad_id = 0, .cls_id = -1, .sep_id = -1, .unk_id = -1, .mask_id = -1 };
        const expected_score = reranking.lateInteractionScore(reference_query.hidden_states, &.{ 1, 2 }, &.{ 1, 1 }, reference.hidden_states, reference.input_ids, reference.attention_mask, reference.hidden_size, special);
        const score = reranking.lateInteractionScore(resident_query.hidden_states, &.{ 1, 2 }, &.{ 1, 1 }, encoded.hidden_states, encoded.input_ids, encoded.attention_mask, encoded.hidden_size, special);
        try std.testing.expectApproxEqAbs(expected_score, score, 1e-5);
        const held = try a.dupe(f32, encoded.hidden_states);
        defer a.free(held);
        ids[0] = 4;
        var later = try encodeDocumentFromPreparedNative(cb, a, config(), prepared, &images);
        defer later.deinit();
        try std.testing.expectEqualSlices(f32, held, encoded.hidden_states);
        try std.testing.expectEqualSlices(f32, imported_held, imported.hidden_states);
        const image_after = try cb.toFloat32(first, a);
        defer a.free(image_after);
        try std.testing.expectEqualSlices(f32, &first_image, image_after);
    }
    fn runOom(a: std.mem.Allocator) !void {
        var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
        defer weights.deinitOwned();
        try populateProjection(&weights);
        var compute = native.NativeCompute.init(a, &weights, null);
        defer compute.deinit();
        const cb = compute.computeBackend();
        var ids = [_]i32{ 1, 7, 7, 2, 7, 3 };
        var mask = [_]i32{ 1, 1, 1, 1, 1, 1 };
        const prepared = qwen2vl.PreparedTextInput{ .allocator = a, .input_ids = &ids, .attention_mask = &mask };
        const first = try cb.fromFloat32Shape(&first_image, &.{ 3, 4 });
        defer cb.free(first);
        const second = try cb.fromFloat32Shape(&second_image, &.{ 2, 4 });
        defer cb.free(second);
        var encoded = try encodeDocumentFromPreparedNative(&cb, a, config(), prepared, &.{ .{ .tensor = first, .token_count = 3 }, .{ .tensor = second, .token_count = 2 } });
        defer encoded.deinit();
        try std.testing.expectEqual(@as(usize, 18), encoded.hidden_states.len);
        var imported = try encodeDocumentFromPrepared(&cb, a, config(), prepared, &.{ &first_image, &second_image });
        defer imported.deinit();
        try std.testing.expectEqualSlices(f32, encoded.hidden_states, imported.hidden_states);
    }
};

test "Native ColQwen resident document assembly preserves partial images projection vectors and score" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try ResidentDocumentFixture.helper.onNative(ResidentDocumentFixture.populateProjection, ResidentDocumentFixture.check);
}

test "Metal ColQwen resident document assembly preserves partial images projection vectors and score" {
    try ResidentDocumentFixture.helper.onMetal(ResidentDocumentFixture.populateProjection, ResidentDocumentFixture.check);
}

test "Native ColQwen resident document assembly preserves optional missing projection" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try ResidentDocumentFixture.helper.onNative(ResidentDocumentFixture.populateBase, ResidentDocumentFixture.check);
}

test "Metal ColQwen resident document assembly preserves optional missing projection" {
    try ResidentDocumentFixture.helper.onMetal(ResidentDocumentFixture.populateBase, ResidentDocumentFixture.check);
}

test "Native ColQwen resident document assembly owns every allocation failure and final outputs" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try ResidentDocumentFixture.runOom(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, ResidentDocumentFixture.runOom, .{});
}

test "Native ColQwen packed logical F32 image declines resident copy before preserving original document fallback" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Native = @import("../ops/native_compute.zig");
    var weights = Native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer weights.deinitOwned();
    const helper = @import("../ops/media_residency_test.zig");
    try helper.putWeight(&weights, "model.embed_tokens.weight", &.{ 8, 32 }, null);
    try helper.putWeight(&weights, "model.norm.weight", &.{32}, 1);
    try helper.putWeight(&weights, "embedding_proj_layer.weight", &.{ 3, 32 }, null);
    try helper.putWeight(&weights, "embedding_proj_layer.bias", &.{3}, null);
    {
        var dense: [64]f32 = undefined;
        for (&dense, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index)) * 0.025 - 0.5;
        var source = try Tensor.initFloat32(a, "source", &.{ 2, 32 }, &dense);
        defer source.deinit();
        var quantized_image = try @import("../models/weight_source.zig").quantizeDenseQ8_0(a, &source);
        errdefer quantized_image.deinit();
        const key = try a.dupe(u8, "packed.image");
        errdefer a.free(key);
        try weights.resident_weights.put(a, key, .{
            .tensor = .{ .name = key, .data = &.{}, .shape = &.{ 2, 32 }, .dtype = .f32, .allocator = a, .owns_data = false, .owns_shape = false },
            .quantized = true,
            .quantized_storage = quantized_image,
        });
    }
    var compute = Native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const image = try cb.getWeight("packed.image");
    defer cb.free(image);
    // This is the real logical/physical distinction, not a fake dtype callback.
    try std.testing.expectEqual(@import("../backends/tensor.zig").DType.f32, try cb.tensorDType(image));
    try std.testing.expect(!try decoder_prefill.EmbeddingRows.supportsSource(&cb, image, 2, 32));
    const decoded = try cb.toFloat32(image, a);
    defer a.free(decoded);
    var ids = [_]i32{ 7, 7, 1 };
    var mask = [_]i32{ 1, 1, 1 };
    const prepared = qwen2vl.PreparedTextInput{ .allocator = a, .input_ids = &ids, .attention_mask = &mask };
    var config = ResidentDocumentFixture.config();
    config.hidden_size = 32;
    config.intermediate_size = 32;
    var reference = try encodeDocumentFromPreparedImpl(false, &cb, a, config, prepared, &.{decoded});
    defer reference.deinit();
    var actual = try encodeDocumentFromPreparedNative(&cb, a, config, prepared, &.{.{ .tensor = image, .token_count = 2 }});
    defer actual.deinit();
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(reference.hidden_states), std.mem.sliceAsBytes(actual.hidden_states));
    try std.testing.expectEqualSlices(i32, &ids, actual.input_ids);
}

test "CUDA ColQwen imported vision tail preserves host oracle scaling projection and held vectors" {
    try ResidentDocumentFixture.helper.onCuda(ResidentDocumentFixture.populateProjection, ResidentDocumentFixture.check);
}

const DocumentProducerOomFixture = struct {
    const png_1x1 = [_]u8{
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
        0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
        0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41,
        0x54, 0x78, 0xda, 0x63, 0xfc, 0xff, 0x9f, 0xa1,
        0x1e, 0x00, 0x07, 0x82, 0x02, 0x7f, 0x3d, 0xc8,
        0x48, 0xef, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
        0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    };

    fn encode(_: *anyopaque, a: std.mem.Allocator, _: []const u8) ![]i32 {
        return a.dupe(i32, &.{1});
    }
    fn info(_: *anyopaque) []const backends.TensorInfo {
        return &.{};
    }
    fn backend(_: *anyopaque) backends.BackendType {
        return .native;
    }
    fn close(_: *anyopaque) void {}
    fn vision(_: *anyopaque, _: []const Tensor, a: std.mem.Allocator) ![]Tensor {
        const values = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
        const outputs = try a.alloc(Tensor, 1);
        errdefer a.free(outputs);
        outputs[0] = try Tensor.initFloat32(a, "image", &.{ 4, 4 }, &values);
        return outputs;
    }
    fn run(a: std.mem.Allocator) !void {
        const native = ResidentDocumentFixture.native;
        var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
        defer weights.deinitOwned();
        try ResidentDocumentFixture.populateProjection(&weights);
        var compute = native.NativeCompute.init(a, &weights, null);
        defer compute.deinit();
        const cb = compute.computeBackend();
        var token = DocumentPromptTestTokenizer.make();
        var table = token.vtable.*;
        table.encode = encode;
        token.vtable = &table;
        var context: u8 = 0;
        const imported = backends.Session{ .ptr = &context, .vtable = &.{ .run = vision, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close } };
        var config = ResidentDocumentFixture.config();
        config.vision_embed_dim = 4;
        config.vision_num_hidden_layers = 1;
        config.vision_num_attention_heads = 1;
        const prep = qwen2vl.PreprocessorConfig{ .min_pixels = 16, .max_pixels = 16, .patch_size = 2, .merge_size = 1, .temporal_patch_size = 1 };
        const query = EncodedSequence{ .allocator = a, .input_ids = @constCast(&[_]i32{1}), .attention_mask = @constCast(&[_]i32{1}), .hidden_states = @constCast(&[_]f32{ 1, 2, 3 }), .hidden_size = 3 };
        const score = try scoreDocument(&cb, a, imported, token, .{}, config, prep, query, "doc", &.{ &png_1x1, &png_1x1 }, 12, false, null);
        try std.testing.expect(std.math.isFinite(score));
        // The canonical Native branch reaches its real patch-weight lookup
        // after image preparation/list ownership, then fails without vision weights.
        _ = scoreDocument(&cb, a, null, token, .{}, config, prep, query, "doc", &.{ &png_1x1, &png_1x1 }, 12, false, null) catch |err| switch (err) {
            error.MissingWeight => return,
            else => return err,
        };
        return error.ExpectedMissingVisionWeight;
    }
};

test "Native ColQwen document producers preserve actual imported and native ownership through allocation failures" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try DocumentProducerOomFixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, DocumentProducerOomFixture.run, .{});
}
