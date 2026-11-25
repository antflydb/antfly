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

//! Text and image generation over the resident Metal Inkling decoder.
const std = @import("std");
const decoder = @import("inkling2_decoder_metal.zig");
const chat = @import("inkling2_chat.zig");
const generation = @import("../pipelines/generation.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Tokenizer = @import("inference_tokenizer").Tokenizer;

const backends = @import("../backends/backends.zig");
const Manifest = @import("../models/manifest.zig").ModelManifest;
const checkpoint = @import("inkling2_checkpoint.zig");
const image = @import("../pipelines/inkling2_image.zig");
const vision_model = @import("inkling2_vision_metal.zig");
const MetalNativeProvider = @import("../backends/metal_native_provider.zig").MetalNativeProvider;
const State = @import("inkling2_state_metal.zig").State;
const Scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler;

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    provider: MetalNativeProvider,
    model: decoder.Decoder,
    vision: vision_model.Vision,
    eos: u32,
    resident_bytes: usize,
    scheduler: Scheduler = .{},
    gate: std.atomic.Mutex = .unlocked,

    pub fn load(a: std.mem.Allocator, manifest: *const Manifest) !backends.Session {
        var source = try checkpoint.Checkpoint.openFiles(a, manifest.config_path orelse return error.NoModelFileFound, manifest.inkling2_shard_path orelse return error.NoModelFileFound);
        defer source.deinit();
        return loadCheckpoint(a, &source);
    }

    pub fn loadCheckpoint(a: std.mem.Allocator, source: *const checkpoint.Checkpoint) !backends.Session {
        const self = try a.create(Runtime);
        errdefer a.destroy(self);
        self.* = .{
            .allocator = a,
            .provider = try MetalNativeProvider.create(),
            .model = undefined,
            .vision = undefined,
            .eos = source.config.value.eos_token_id,
            .resident_bytes = 0,
        };
        errdefer self.provider.deinitOwned();
        const raw = self.provider.raw_decode_runtime orelse return error.MetalUnavailable;
        self.model = try decoder.Decoder.load(a, raw, source);
        errdefer self.model.deinit();
        self.vision = try .load(a, raw, source);
        self.resident_bytes = self.model.residentBytes() + self.vision.residentBytes();
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Widen only the automatically chosen KV bucket, and only when the
    /// complete plan fits both of the existing physical envelopes.
    pub fn widenCacheBudget(self: *const Runtime, limits: @import("../runtime/tier/memory.zig").Limits) @TypeOf(limits) {
        if (limits.kv_limit_bytes == 0) return limits;
        const context = self.model.text.model_max_length;
        const plan = decoder.RequestPlan.initWithFormat(&self.model, context, @min(context, decoder.prefill_chunk_rows), .q4_head128) catch return limits;
        const retained = State.deviceBytes(plan.bank, plan.width) catch return limits;
        if (retained <= limits.kv_limit_bytes) return limits;
        const device = self.resident_bytes +| (plan.deviceBytes() catch return limits);
        const host = @as(usize, self.model.text.unpadded_vocab_size) * @sizeOf(f32);
        if ((limits.backend_limit_bytes != 0 and device > limits.backend_limit_bytes) or
            (limits.combined_limit_bytes != 0 and device +| host > limits.combined_limit_bytes) or
            (limits.host_limit_bytes != 0 and host > limits.host_limit_bytes))
            return limits;
        var widened = limits;
        widened.kv_limit_bytes = retained;
        return widened;
    }

    pub fn fromSession(session: backends.Session) ?*Runtime {
        return if (session.vtable == &vtable) @ptrCast(@alignCast(session.ptr)) else null;
    }

    pub fn generate(self: *Runtime, session: backends.Session, pipeline: *const TextPipeline, config: generation.GenerationConfig, callbacks: TextPipeline.Callbacks) !generation.GenerationResult {
        const plan = try pipeline.requestPlan(&self.model, config);
        const retained = try State.deviceBytes(plan.bank, plan.width);
        const physical = try plan.deviceBytes();
        const logit_bytes = @as(usize, self.model.text.unpadded_vocab_size) * @sizeOf(f32);
        const vision_bytes: vision_model.AdmissionBytes = if (pipeline.prompt.image != null)
            vision_model.admissionBytes(pipeline.prompt.image_width, pipeline.prompt.image_height)
        else
            .{ .host = 0, .device = 0 };
        var permit = try session.withAdmissionControl(pipeline.control).admitRetainedStateWithWorkspace(
            logit_bytes + vision_bytes.host,
            retained,
            physical - retained + vision_bytes.device,
        );
        defer permit.deinit();
        var waiter: Scheduler.Waiter = .{};
        try self.scheduler.enter(&waiter, &self.gate, pipeline.control);
        defer Scheduler.leave(&waiter);
        return pipeline.generateWithPlan(&self.model, &self.vision, self.eos, config, callbacks, plan);
    }

    const vtable: backends.Session.VTable = .{
        .executionCapabilities = capabilities,
        .run = run,
        .inputInfo = tensorInfo,
        .outputInfo = tensorInfo,
        .backend = backend,
        .close = close,
    };

    fn capabilities(ptr: *anyopaque) @import("../backends/session.zig").ExecutionCapabilities {
        const self: *const Runtime = @ptrCast(@alignCast(ptr));
        return .{ .measured_resident_weights = self.resident_bytes };
    }

    fn run(_: *anyopaque, _: []const backends.Tensor, _: std.mem.Allocator) anyerror![]backends.Tensor {
        return error.UnsupportedSessionRun;
    }

    fn tensorInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{};
    }

    fn backend(_: *anyopaque) backends.BackendType {
        return .metal;
    }

    fn close(ptr: *anyopaque) void {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        self.vision.deinit();
        self.model.deinit();
        self.provider.deinitOwned();
        self.allocator.destroy(self);
    }
};

pub const TextPipeline = struct {
    allocator: std.mem.Allocator,
    tokenizer: Tokenizer,
    control: Control,
    prompt: chat.Prompt,
    options: chat.ChatOptions,

    pub const Callbacks = struct {
        text_ctx: ?*anyopaque = null,
        text: ?generation.TokenCallback = null,
        reasoning_ctx: ?*anyopaque = null,
        reasoning: ?generation.TokenCallback = null,
    };

    pub fn init(a: std.mem.Allocator, tokenizer: Tokenizer, control: Control, messages: []const generation.Message, config: generation.GenerationConfig, input_limit: ?usize, options: chat.ChatOptions) !TextPipeline {
        try control.check();
        const prompt = try chat.Prompt.init(a, tokenizer, messages, options, config.enable_thinking);
        errdefer prompt.deinit(a);
        if (input_limit) |limit| if (prompt.count() > limit) return error.InferenceInputTokensExceeded;
        return .{ .allocator = a, .tokenizer = tokenizer, .control = control, .prompt = prompt, .options = options };
    }

    pub fn deinit(self: *TextPipeline) void {
        self.prompt.deinit(self.allocator);
    }

    pub fn requestPlan(self: *const TextPipeline, model: *const decoder.Decoder, config: generation.GenerationConfig) !decoder.RequestPlan {
        if (config.max_tokens <= 0 or self.prompt.after_image.len == 0 or
            config.grammar != null or config.kv_cache_mode != .exact_hybrid or
            config.cache_compaction_ratio != null or config.prompt_cache_enabled or config.draft_model != null or
            config.token_observer != null or (config.speculation_requested and config.speculation_policy == .force))
            return error.UnsupportedInkling2Request;
        const tokens = try std.math.add(usize, self.prompt.count(), @as(usize, @intCast(config.max_tokens)) - 1);
        const prefix = @max(self.prompt.before_image.len, self.prompt.after_image.len - 1, self.prompt.imageRows());
        const prefill_rows: u32 = if (prefix >= decoder.prefill_chunk_rows) decoder.prefill_chunk_rows else 1;
        const format: @import("../runtime/kv/hybrid_paged.zig").PlaneFormat = if (config.cache_dtype) |dtype| blk: {
            const parsed = @import("../runtime/kv/pool.zig").parseKvDType(dtype) orelse return error.UnsupportedInkling2Request;
            break :blk switch (parsed) {
                .bf16 => .bf16,
                .int4 => .q4_head128,
                else => return error.UnsupportedInkling2Request,
            };
        } else .bf16;
        return decoder.RequestPlan.initWithFormat(model, std.math.cast(u32, tokens) orelse return error.InvalidInkling2Context, prefill_rows, format);
    }

    /// The caller owns the runtime execution gate and memory admission for
    /// the complete request, including prompt prefill and streaming callbacks.
    pub fn generate(self: *const TextPipeline, model: *const decoder.Decoder, eos: u32, config: generation.GenerationConfig, callbacks: Callbacks) !generation.GenerationResult {
        return self.generateWithPlan(model, null, eos, config, callbacks, try self.requestPlan(model, config));
    }

    fn generateWithPlan(self: *const TextPipeline, model: *const decoder.Decoder, vision: ?*const vision_model.Vision, eos: u32, config: generation.GenerationConfig, callbacks: Callbacks, plan: decoder.RequestPlan) !generation.GenerationResult {
        try self.control.check();
        var request: decoder.Request = undefined;
        try request.initWithPlan(self.allocator, model, plan);
        defer request.deinit();

        var history: std.ArrayList(i64) = .empty;
        defer history.deinit(self.allocator);
        const greedy = chat.usesUnpenalizedGreedy(config);
        const after = self.prompt.after_image;
        const chunk_rows: u32 = @intCast(plan.bank.layout.max_append_tokens);
        try self.prefillTokens(&request, self.prompt.before_image, chunk_rows);
        if (self.prompt.image) |data| {
            const encoder = vision orelse return error.UnsupportedInkling2Request;
            try self.prefillImage(&request, model, encoder, data, chunk_rows);
        }
        try self.prefillTokens(&request, after[0 .. after.len - 1], chunk_rows);
        if (!greedy) {
            for ([_][]const i32{ self.prompt.before_image, after[0 .. after.len - 1] }) |prefix| {
                for (prefix) |id| try history.append(self.allocator, id);
            }
        }

        const turn_end = try chat.turnEndToken(self.allocator, self.tokenizer);
        const channels: ?chat.ChannelTokens = if (self.prompt.initial_channel != null)
            try .init(self.allocator, self.tokenizer, turn_end)
        else
            null;
        var output: chat.Output = .{
            .allocator = self.allocator,
            .tokenizer = self.tokenizer,
            .ctx = callbacks.text_ctx,
            .callback = callbacks.text,
            .reasoning_ctx = callbacks.reasoning_ctx,
            .reasoning_callback = callbacks.reasoning,
            .channels = channels,
            .channel = self.prompt.initial_channel orelse .text,
            .tools = self.options.tools,
        };
        defer output.deinit();
        const limit: usize = @intCast(config.max_tokens);
        var token = std.math.cast(u32, after[after.len - 1]) orelse return error.InvalidInkling2Token;
        while (output.tokens.items.len < limit) {
            try self.control.check();
            if (greedy) {
                token = try request.stepGreedy(token);
            } else {
                const logits = try request.step(token);
                try history.append(self.allocator, token);
                token = @intCast(generation.sampleTokenFromLogits(self.allocator, logits, config, history.items));
            }
            try self.control.check();
            if (!config.ignore_eos and (token == eos or (channels == null and token == turn_end))) break;
            try output.append(token);
        }
        return output.finalize(self.options, self.prompt.count(), limit);
    }

    fn prefillTokens(self: *const TextPipeline, request: *decoder.Request, tokens: []const i32, chunk_rows: u32) !void {
        var offset: usize = 0;
        while (chunk_rows > 1 and tokens.len - offset >= chunk_rows) : (offset += chunk_rows) {
            try self.control.check();
            try request.appendTokens(tokens[offset..][0..chunk_rows]);
        }
        for (tokens[offset..]) |id| {
            try self.control.check();
            try request.append(std.math.cast(u32, id) orelse return error.InvalidInkling2Token);
        }
    }

    fn prefillImage(self: *const TextPipeline, request: *decoder.Request, model: *const decoder.Decoder, encoder: *const vision_model.Vision, data: []const u8, chunk_rows: u32) !void {
        try self.control.check();
        const rgb = image.decodeRgb(self.allocator, data) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidInkling2Image,
        };
        defer rgb.deinit(self.allocator);
        if (rgb.width != self.prompt.image_width or rgb.height != self.prompt.image_height) return error.InvalidInkling2Image;
        var embeddings = try encoder.encodeRgb(self.allocator, model.runtime, rgb.data, rgb.width, rgb.height);
        defer embeddings.tensor.deinit();
        if (embeddings.rows != self.prompt.imageRows()) return error.InvalidInkling2Image;
        const row_bytes = @as(usize, model.text.hidden_size) * @sizeOf(u16);
        var offset: u32 = 0;
        while (offset < embeddings.rows) {
            try self.control.check();
            const count = if (embeddings.rows - offset >= chunk_rows) chunk_rows else 1;
            const bytes = @as(usize, count) * row_bytes;
            var rows = try embeddings.tensor.retainedView(@as(usize, offset) * row_bytes, bytes, &.{@intCast(bytes / @sizeOf(u16))});
            defer rows.deinit();
            try request.appendEmbeddings(&rows, count);
            offset += count;
        }
    }
};

pub const ServingPipeline = struct {
    resident: *Runtime,
    session: backends.Session,
    text: TextPipeline,
    reasoning_ctx: ?*anyopaque = null,
    reasoning_callback: ?generation.TokenCallback = null,

    pub fn generate(self: *ServingPipeline, _: []const generation.Message, config: generation.GenerationConfig) !generation.GenerationResult {
        return self.resident.generate(self.session, &self.text, config, .{});
    }

    pub fn generateStreaming(self: *ServingPipeline, _: []const generation.Message, config: generation.GenerationConfig, ctx: *anyopaque, callback: generation.TokenCallback) !generation.GenerationResult {
        return self.resident.generate(self.session, &self.text, config, .{
            .text_ctx = ctx,
            .text = callback,
            .reasoning_ctx = self.reasoning_ctx,
            .reasoning = self.reasoning_callback,
        });
    }
};
