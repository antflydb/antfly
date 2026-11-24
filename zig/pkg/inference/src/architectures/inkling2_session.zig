// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Resident Inkling CUDA model and request-local generation state.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const decoder = @import("inkling2_decoder_cuda.zig");
const dspark = @import("inkling2_dspark_cuda.zig");
const vision = @import("inkling2_vision_cuda.zig");
const images = @import("../pipelines/image.zig");
const generation = @import("../pipelines/generation.zig");
const backends = @import("../backends/backends.zig");
const Manifest = @import("../models/manifest.zig").ModelManifest;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Context = @import("../ops/cuda/context.zig").CudaContext;
const Tokenizer = @import("inference_tokenizer").Tokenizer;

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    ctx: Context,
    model: decoder.Decoder,
    encoder: vision.Vision,
    draft: ?dspark.Draft = null,
    eos: u32,
    fast_matvec: bool,
    resident_bytes: usize,

    pub fn load(allocator: std.mem.Allocator, manifest: *const Manifest) !backends.Session {
        var source = try checkpoint.Checkpoint.openFiles(
            allocator,
            manifest.config_path orelse return error.NoModelFileFound,
            manifest.inkling2_shard_path orelse return error.NoModelFileFound,
        );
        defer source.deinit();
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.ctx = try Context.initDefault();
        errdefer self.ctx.deinit();
        self.fast_matvec = self.ctx.info.compute_major == 12 and self.ctx.info.compute_minor == 1;
        self.eos = source.config.value.eos_token_id;
        self.draft = null;
        const draft_config = source.config.value.mtp_config;
        const has_draft = draft_config != null and manifest.inkling2_draft_path != null;
        self.model = try decoder.Decoder.load(allocator, &self.ctx, &source, .{
            .pack_attention_weights = self.fast_matvec,
            .prepare_greedy = true,
            .retain_raw_head = has_draft,
        });
        errdefer self.model.deinit();
        self.encoder = try vision.Vision.load(&self.ctx, &source);
        errdefer self.encoder.deinit();
        if (has_draft) self.draft = try dspark.Draft.loadWeights(
            allocator,
            &self.ctx,
            &self.model.blas,
            manifest.inkling2_draft_path.?,
            draft_config.?,
            self.model.sharedDraftWeights(),
        );
        self.resident_bytes = self.ctx.device_allocations.snapshot().physical_live;
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn fromSession(session: backends.Session) ?*Runtime {
        return if (session.vtable == &vtable) @ptrCast(@alignCast(session.ptr)) else null;
    }

    const vtable: backends.Session.VTable = .{
        .executionCapabilities = executionCapabilities,
        .run = run,
        .inputInfo = tensorInfo,
        .outputInfo = tensorInfo,
        .backend = backend,
        .close = close,
    };

    fn executionCapabilities(ptr: *anyopaque) @import("../backends/session.zig").ExecutionCapabilities {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        return .{ .measured_resident_weights = self.resident_bytes };
    }

    fn run(_: *anyopaque, _: []const backends.Tensor, _: std.mem.Allocator) anyerror![]backends.Tensor {
        return error.UnsupportedSessionRun;
    }

    fn tensorInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{};
    }

    fn backend(_: *anyopaque) backends.BackendType {
        return .cuda;
    }

    fn close(ptr: *anyopaque) void {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        self.ctx.makeCurrent() catch {};
        if (self.draft) |*draft| draft.deinit();
        self.encoder.deinit();
        self.model.deinit();
        self.ctx.deinit();
        self.allocator.destroy(self);
    }
};

const Prompt = struct {
    before_image: []i32,
    after_image: []i32,
    image: ?[]const u8,
    image_width: u32 = 0,
    image_height: u32 = 0,

    fn init(allocator: std.mem.Allocator, tok: Tokenizer, messages: []const generation.Message) !Prompt {
        const effort = "<|message_system|><|content_text|>Thinking effort level: 0<|end_message|>";
        var text: std.ArrayList(u8) = .empty;
        defer text.deinit(allocator);
        var effort_written = false;
        var image_data: ?[]const u8 = null;
        var image_offset: usize = 0;
        for (messages) |message| {
            const role = if (std.mem.eql(u8, message.role, "system"))
                "<|message_system|>"
            else if (std.mem.eql(u8, message.role, "user"))
                "<|message_user|>"
            else if (std.mem.eql(u8, message.role, "assistant"))
                "<|message_model|>"
            else
                return error.UnsupportedInkling2Request;
            if (message.tool_calls != null or message.audio_bytes != null or message.video_bytes != null)
                return error.UnsupportedInkling2Request;
            if (!effort_written and !std.mem.eql(u8, message.role, "system")) {
                try text.appendSlice(allocator, effort);
                effort_written = true;
            }
            if (message.content_parts) |parts| {
                for (parts) |part| switch (part) {
                    .text => |content| try appendText(&text, allocator, role, content),
                    .image => |index| {
                        const attached = message.image_bytes orelse return error.InvalidInkling2Image;
                        if (index >= attached.len or !std.mem.eql(u8, message.role, "user"))
                            return error.InvalidInkling2Image;
                        try appendImage(&text, allocator, role, attached[index], &image_data, &image_offset);
                    },
                    .audio => return error.UnsupportedInkling2Request,
                };
            } else {
                for (message.image_bytes orelse &.{}) |data| {
                    if (!std.mem.eql(u8, message.role, "user")) return error.InvalidInkling2Image;
                    try appendImage(&text, allocator, role, data, &image_data, &image_offset);
                }
                try appendText(&text, allocator, role, message.content);
            }
            if (std.mem.eql(u8, message.role, "assistant"))
                try text.appendSlice(allocator, "<|content_model_end_sampling|>");
        }
        if (!effort_written) try text.appendSlice(allocator, effort);
        try text.appendSlice(allocator, "<|message_model|><|content_text|>");
        const before = try tok.encode(allocator, text.items[0..image_offset]);
        errdefer allocator.free(before);
        const after = try tok.encode(allocator, text.items[image_offset..]);
        errdefer allocator.free(after);
        if (after.len == 0) return error.InvalidInkling2Tokenizer;
        var prompt = Prompt{ .before_image = before, .after_image = after, .image = image_data };
        if (image_data) |data| {
            const info = images.inspectEncodedForInference(data, 4096) catch return error.InvalidInkling2Image;
            prompt.image_width = info.width;
            prompt.image_height = info.height;
        }
        return prompt;
    }

    fn appendText(text: *std.ArrayList(u8), allocator: std.mem.Allocator, role: []const u8, content: []const u8) !void {
        try text.appendSlice(allocator, role);
        try text.appendSlice(allocator, "<|content_text|>");
        try text.appendSlice(allocator, content);
        try text.appendSlice(allocator, "<|end_message|>");
    }

    fn appendImage(text: *std.ArrayList(u8), allocator: std.mem.Allocator, role: []const u8, data: []const u8, image_data: *?[]const u8, offset: *usize) !void {
        if (image_data.* != null) return error.InvalidInkling2Image;
        try text.appendSlice(allocator, role);
        try text.appendSlice(allocator, "<|content_image|>");
        offset.* = text.items.len;
        image_data.* = data;
        try text.appendSlice(allocator, "<|end_message|>");
    }

    fn deinit(self: Prompt, allocator: std.mem.Allocator) void {
        allocator.free(self.before_image);
        allocator.free(self.after_image);
    }

    fn imageRows(self: Prompt) usize {
        return ((self.image_width + 39) / 40) * ((self.image_height + 39) / 40);
    }

    fn count(self: Prompt) usize {
        return self.before_image.len + self.after_image.len + self.imageRows();
    }
};

pub const Pipeline = struct {
    allocator: std.mem.Allocator,
    model: *Runtime,
    session: backends.Session,
    tokenizer: Tokenizer,
    control: Control,
    prompt: Prompt,
    context_tokens: u32,
    chunk_rows: u32,
    speculate: bool,
    continue_ctx: ?*anyopaque = null,
    continue_fn: ?*const fn (*anyopaque) bool = null,

    pub fn init(allocator: std.mem.Allocator, model: *Runtime, session: backends.Session, tokenizer: Tokenizer, control: Control, messages: []const generation.Message, config: generation.GenerationConfig, input_limit: ?usize) !Pipeline {
        if (config.grammar != null or config.enable_thinking == true or config.cache_dtype != null or
            config.kv_cache_mode != .exact_hybrid or config.cache_compaction_ratio != null or
            config.prompt_cache_enabled or config.draft_model != null or config.token_observer != null)
            return error.UnsupportedInkling2Request;
        const speculate = config.speculation_requested and config.speculation_policy != .off;
        if (speculate and (model.draft == null or !usesGreedy(config) or config.speculative_k == 0 or
            config.speculative_k > dspark.proposal_rows)) return error.UnsupportedInkling2Speculation;
        try control.check();
        const prompt = try Prompt.init(allocator, tokenizer, messages);
        errdefer prompt.deinit(allocator);
        if (input_limit) |limit| if (prompt.count() > limit) return error.InferenceInputTokensExceeded;
        const context = prompt.count() +| @as(usize, @intCast(config.max_tokens)) - 1;
        if (context > model.model.text.model_max_length or (speculate and context > model.draft.?.max_capacity))
            return error.InvalidInkling2Context;
        const batch_prompt = model.fast_matvec and prompt.count() > decoder.prefill_chunk_rows;
        return .{
            .allocator = allocator,
            .model = model,
            .session = session,
            .tokenizer = tokenizer,
            .control = control,
            .prompt = prompt,
            .context_tokens = @intCast(context),
            .chunk_rows = if (batch_prompt or speculate) @intCast(@min(decoder.prefill_chunk_rows, context)) else 1,
            .speculate = speculate,
        };
    }

    pub fn deinit(self: *Pipeline) void {
        self.prompt.deinit(self.allocator);
    }

    pub fn generate(self: *Pipeline, _: []const generation.Message, config: generation.GenerationConfig) !generation.GenerationResult {
        return self.decode(config, null, null);
    }

    pub fn generateStreaming(self: *Pipeline, _: []const generation.Message, config: generation.GenerationConfig, ctx: *anyopaque, callback: generation.TokenCallback) !generation.GenerationResult {
        return self.decode(config, ctx, callback);
    }

    fn check(self: *const Pipeline) !void {
        try self.control.check();
        if (self.continue_fn) |keep_going| if (!keep_going(self.continue_ctx.?)) return error.Cancelled;
    }

    fn prefill(self: *Pipeline, request: *decoder.Request, input: []const i32) !void {
        var offset: usize = 0;
        while (offset < input.len) {
            try self.check();
            var tokens: [decoder.prefill_chunk_rows]u32 = undefined;
            const count = @min(request.append_capacity, input.len - offset);
            for (input[offset..][0..count], tokens[0..count]) |id, *token| token.* = @intCast(id);
            if (count == 1) try request.append(tokens[0]) else try request.appendTokens(tokens[0..count]);
            offset += count;
        }
    }

    fn usesGreedy(config: generation.GenerationConfig) bool {
        return config.temperature == 0 and config.repetition_penalty == 1 and
            config.frequency_penalty == 0 and config.presence_penalty == 0;
    }

    fn next(self: *Pipeline, request: *decoder.Request, config: generation.GenerationConfig, history: *std.ArrayList(i64), token: u32) !u32 {
        if (usesGreedy(config)) return request.stepGreedy(token);
        try history.append(self.allocator, token);
        const logits = try request.step(token);
        return @intCast(generation.sampleTokenFromLogits(self.allocator, logits, config, history.items));
    }

    fn decode(self: *Pipeline, config: generation.GenerationConfig, callback_ctx: ?*anyopaque, callback: ?generation.TokenCallback) !generation.GenerationResult {
        try self.check();
        const model = self.model;
        try model.ctx.makeCurrent();
        const session = self.session.withAdmissionControl(self.control);
        const pixels = @as(usize, self.prompt.image_width) * self.prompt.image_height;
        const amount = try decoder.requestMemory(model.model.text, self.context_tokens, self.chunk_rows);
        const draft_kv = if (self.speculate) dspark.Draft.requestKvBytes(self.context_tokens) else 0;
        var state = try session.admitRetainedState(amount.host, amount.kv + draft_kv);
        defer state.deinit();
        var workspace = try session.admitRetainedMedia(pixels * 11, amount.workspace +
            if (self.prompt.image != null) vision.requestWorkspaceBytes(self.prompt.imageRows(), pixels) else @as(usize, 0));
        defer workspace.deinit();

        var request: decoder.Request = undefined;
        const draft: ?*dspark.Draft = if (self.speculate) &model.draft.? else null;
        defer if (draft) |active| active.endRequest();
        try request.initWithPrefill(self.allocator, &model.model, self.context_tokens, self.chunk_rows);
        defer request.deinit() catch @panic("unable to drain the Inkling CUDA stream");
        request.use_dense = model.fast_matvec;
        if (draft) |active| {
            try active.beginRequest(self.context_tokens, request.append_capacity);
            try request.attachDraft(active);
        }
        try self.prefill(&request, self.prompt.before_image);
        if (self.prompt.image) |data| {
            const rgb = try @import("../pipelines/inkling2_image.zig").decodeRgb(self.allocator, data);
            defer rgb.deinit(self.allocator);
            var embeddings = try model.encoder.encodeRgb(&model.model.blas, rgb.data, rgb.width, rgb.height);
            defer embeddings.deinit(&model.ctx);
            try self.check();
            try request.appendEmbeddingsWithControl(embeddings.device, embeddings.rows, self.control);
        }
        const after = self.prompt.after_image;
        try self.prefill(&request, after[0 .. after.len - 1]);
        var history: std.ArrayList(i64) = .empty;
        defer history.deinit(self.allocator);
        if (!usesGreedy(config)) {
            for (self.prompt.before_image) |id| try history.append(self.allocator, id);
            for (after[0 .. after.len - 1]) |id| try history.append(self.allocator, id);
        }
        const turn_end = try self.tokenizer.encode(self.allocator, "<|end_message|>");
        defer self.allocator.free(turn_end);
        if (turn_end.len != 1) return error.InvalidInkling2Tokenizer;
        const limit: usize = @intCast(config.max_tokens);
        var output: Output = .{ .allocator = self.allocator, .tokenizer = self.tokenizer, .ctx = callback_ctx, .callback = callback };
        defer output.deinit();
        var stats: generation.SpeculativeDecodeStats = .{ .speculation_policy = config.speculation_policy, .speculation_policy_decision = if (config.speculation_policy == .force) .forced else .active, .mtp_enabled = draft != null };
        var last = try self.next(&request, config, &history, @intCast(after[after.len - 1]));
        decode_loop: while (true) {
            try self.check();
            if (!config.ignore_eos and (last == model.eos or last == turn_end[0])) break;
            try output.append(last);
            const remaining = limit - output.tokens.items.len;
            if (remaining == 0) break;
            if (draft) |active| {
                if (remaining > 1 and request.append_capacity > 1) {
                    const count: u32 = @intCast(@min(config.speculative_k, remaining - 1));
                    const proposed = try active.propose(&model.model.blas, last, count);
                    const verified = try request.verifyDraft(last, proposed[0..count]);
                    stats.rounds += 1;
                    stats.drafted_tokens += count;
                    stats.matched_draft_tokens += verified.count - 1;
                    stats.accepted_tokens += verified.count - 1;
                    stats.correction_tokens += @intFromBool(verified.count <= count);
                    stats.bonus_tokens += @intFromBool(verified.count > count);
                    for (verified.ids[0 .. verified.count - 1]) |id| {
                        if (!config.ignore_eos and (id == model.eos or id == turn_end[0])) break :decode_loop;
                        try output.append(id);
                    }
                    last = verified.ids[verified.count - 1];
                    continue;
                }
            }
            last = try self.next(&request, config, &history, last);
        }
        const result = if (callback != null)
            try output.text.toOwnedSlice(self.allocator)
        else
            try self.tokenizer.decodeText(self.allocator, output.tokens.items);
        errdefer self.allocator.free(result);
        const ids = try output.tokens.toOwnedSlice(self.allocator);
        return .{
            .allocator = self.allocator,
            .text = result,
            .token_ids = ids,
            .prompt_tokens = self.prompt.count(),
            .tokens_used = ids.len,
            .finish_reason = if (ids.len == limit) "length" else "stop",
            .speculative = if (draft != null) stats else null,
        };
    }
};

const Output = struct {
    allocator: std.mem.Allocator,
    tokenizer: Tokenizer,
    tokens: std.ArrayList(i32) = .empty,
    text: std.ArrayList(u8) = .empty,
    pending_utf8: [4]u8 = undefined,
    pending_len: usize = 0,
    ctx: ?*anyopaque,
    callback: ?generation.TokenCallback,

    fn deinit(self: *Output) void {
        self.tokens.deinit(self.allocator);
        self.text.deinit(self.allocator);
    }

    fn append(self: *Output, token: u32) !void {
        const at_start = self.tokens.items.len == 0;
        try self.tokens.append(self.allocator, @intCast(token));
        if (self.callback) |emit| {
            // The released tokenizer has a byte-level decoder. Preserve UTF-8
            // sequences spanning tokens and match decodeText at the length limit.
            const bytes = try self.tokenizer.decodeToken(self.allocator, @intCast(token), at_start);
            defer self.allocator.free(bytes);
            const start = self.text.items.len;
            for (bytes) |byte| try self.appendByte(byte);
            if (self.text.items.len > start and !emit(self.ctx.?, self.text.items[start..]))
                return error.Cancelled;
        }
    }

    fn appendByte(self: *Output, byte: u8) !void {
        if (self.pending_len != 0 and byte & 0xc0 != 0x80) self.pending_len = 0;
        if (self.pending_len == 0 and byte < 0x80) {
            try self.text.append(self.allocator, byte);
            return;
        }
        if (self.pending_len == 0 and byte & 0xc0 == 0x80) return;
        self.pending_utf8[self.pending_len] = byte;
        self.pending_len += 1;
        const len = std.unicode.utf8ByteSequenceLength(self.pending_utf8[0]) catch {
            self.pending_len = 0;
            return;
        };
        if (self.pending_len == len) {
            const sequence = self.pending_utf8[0..len];
            if (std.unicode.utf8ValidateSlice(sequence)) try self.text.appendSlice(self.allocator, sequence);
            self.pending_len = 0;
        }
    }
};
