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
const memory = @import("../runtime/tier/memory.zig");
const Scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler;
const observation = @import("../execution/invocation_observation.zig");
const diagnostics = @import("../execution/diagnostics.zig");

// The resident decoder records frames against a single Metal runtime.
pub const max_active_requests = 1;

const PrefillShape = struct {
    rows: u32,
    reduction_rows: u32,

    fn preferred(prefix: usize) PrefillShape {
        const rows: u32 = @intCast(@max(1, @min(prefix, decoder.preferredPrefillRows())));
        return .{ .rows = rows, .reduction_rows = rows };
    }

    fn smaller(self: PrefillShape) ?PrefillShape {
        if (self.reduction_rows > 16) {
            // Preserve the wider projection batch while there is still
            // attention workspace that can be reused across query tiles.
            return .{ .rows = self.rows, .reduction_rows = self.reduction_rows / 2 };
        }
        if (self.rows == 1) return null;
        const rows = self.rows / 2;
        return .{ .rows = rows, .reduction_rows = @min(rows, self.reduction_rows) };
    }
};

// Decoded RGB is transient. Only the projected rows used by the current
// decoder batch and a partially consumed preceding image can remain live.
fn imageAdmission(prompt: chat.Prompt, chunk_rows: usize, hidden_size: usize) !vision_model.AdmissionBytes {
    var peak: vision_model.AdmissionBytes = .{};
    var position: usize = 0;
    var batch_start: usize = 0;
    var live: usize = 0;
    var previous_bytes: usize = 0;
    for (prompt.media) |item| {
        const start = try std.math.add(usize, position, item.prefix.len);
        const next_batch = start - start % chunk_rows;
        if (next_batch != batch_start) {
            live = if (position > next_batch) previous_bytes else 0;
            batch_start = next_batch;
        }
        const need = vision_model.admissionBytes(item.image.width, item.image.height);
        peak.host = @max(peak.host, need.host);
        peak.device = @max(peak.device, try std.math.add(usize, live, need.device));
        const image_rows = item.image.rows();
        previous_bytes = try std.math.mul(usize, image_rows, hidden_size * @sizeOf(u16));
        live = try std.math.add(usize, live, previous_bytes);
        position = try std.math.add(usize, start, image_rows);
    }
    return peak;
}

test "Inkling chat frontend accounts for overlapping resident image rows" {
    const input: chat.Prompt.Image = .{ .bytes = &.{}, .width = 80, .height = 80 };
    var marker = [_]i32{0};
    var separator: [32]i32 = @splat(0);
    var media = [_]chat.Prompt.Media{
        .{ .prefix = &marker, .image = input },
        .{ .prefix = &marker, .image = input },
    };
    const prompt: chat.Prompt = .{ .media = &media, .suffix = &marker, .initial_channel = null };
    const width = @import("inkling2_vision.zig").text_width;
    const vision = vision_model.admissionBytes(input.width, input.height);
    const projected_bytes = input.rows() * width * @sizeOf(u16);
    try std.testing.expectEqual(vision.host, (try imageAdmission(prompt, 16, width)).host);
    try std.testing.expectEqual(vision.device + projected_bytes, (try imageAdmission(prompt, 16, width)).device);
    try std.testing.expectEqual(vision.device + projected_bytes, (try imageAdmission(prompt, 4, width)).device);
    media[1].prefix = &separator;
    try std.testing.expectEqual(vision.device, (try imageAdmission(prompt, 4, width)).device);
}

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    provider: MetalNativeProvider,
    model: decoder.Decoder,
    vision: vision_model.Vision,
    eos: u32,
    resident_bytes: usize,
    invocation_diagnostics: diagnostics.Diagnostics = .{},
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
        return .{
            .ptr = self,
            .vtable = &vtable,
            .invocation_diagnostics = .{
                .diagnostics = &self.invocation_diagnostics,
                .model_allocator = a,
                .context_runtime_scope = .unavailable,
            },
        };
    }

    /// Widen only the automatically chosen KV bucket, and only when the
    /// complete plan fits both of the existing physical envelopes.
    pub fn widenCacheBudget(self: *const Runtime, limits: memory.Limits) memory.Limits {
        if (limits.kv_limit_bytes == 0) return limits;
        const context = self.model.text.model_max_length;
        var shape = PrefillShape.preferred(context);
        while (true) {
            const plan = decoder.RequestPlan.initWithReductionRows(&self.model, context, shape.rows, shape.reduction_rows, .q4_head128) catch return limits;
            const retained = State.deviceBytes(plan.bank, plan.width) catch return limits;
            const physical = plan.deviceBytes() catch return limits;
            var candidate = limits;
            candidate.kv_limit_bytes = @max(limits.kv_limit_bytes, retained);
            const required: memory.AdmissionAmounts = .{
                .backend_weight_bytes = self.resident_bytes,
                .backend_kv_bytes = retained,
                .backend_scratch_bytes = physical - retained,
                .host_scratch_bytes = plan.hostLogitBytes(),
            };
            if (required.fitsLimits(candidate) catch false) return candidate;
            shape = shape.smaller() orelse return limits;
        }
    }

    pub fn fromSession(session: backends.Session) ?*Runtime {
        return if (session.vtable == &vtable) @ptrCast(@alignCast(session.ptr)) else null;
    }

    pub fn generate(self: *Runtime, session: backends.Session, pipeline: *const TextPipeline, config: generation.GenerationConfig, callbacks: TextPipeline.Callbacks) !generation.GenerationResult {
        const guarded = session.withAdmissionControl(pipeline.control);
        // Only the ready owner sizes and reserves workspace. Keep the ready
        // turn through retirement so the next request sees the available budget.
        var waiter: Scheduler.Waiter = .{};
        try self.scheduler.selectReady(&waiter, &self.gate, pipeline.control);
        defer Scheduler.leave(&waiter);
        var shape = PrefillShape.preferred(pipeline.prompt.count() -| 1);
        var plan: decoder.RequestPlan = undefined;
        const retained, const workspace, const host = while (true) {
            plan = try pipeline.requestPlanWithShape(&self.model, config, shape);
            const kv_bytes = try State.deviceBytes(plan.bank, plan.width);
            const device_bytes = try plan.deviceBytes();
            const vision_bytes = try imageAdmission(pipeline.prompt, shape.rows, self.model.text.hidden_size);
            const host_bytes = try std.math.add(usize, plan.hostLogitBytes(), vision_bytes.host);
            const scratch = try std.math.add(usize, device_bytes - kv_bytes, vision_bytes.device);
            if (shape.rows == 1 or try guarded.fitsRetainedStateWithWorkspace(host_bytes, kv_bytes, scratch))
                break .{ kv_bytes, scratch, host_bytes };
            shape = shape.smaller().?;
        };
        var permit = try guarded.admitRetainedStateWithWorkspace(host, retained, workspace);
        defer permit.deinit();
        try Scheduler.acquireSelected(&waiter, pipeline.control);
        defer Scheduler.releaseGate(&waiter);
        return pipeline.generateWithPlan(&self.model, &self.vision, self.eos, config, callbacks, plan, session.invocation_diagnostics);
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

fn requestCacheFormat(config: generation.GenerationConfig) !@import("../runtime/kv/hybrid_paged.zig").PlaneFormat {
    if (config.grammar != null or config.kv_cache_mode != .exact_hybrid or
        config.cache_compaction_ratio != null or config.prompt_cache_enabled or config.draft_model != null or
        config.token_observer != null or (config.speculation_requested and config.speculation_policy == .force))
        return error.UnsupportedInkling2Request;
    const requested = config.cache_dtype orelse return .bf16;
    const dtype = @import("../runtime/kv/pool.zig").parseKvDType(requested) orelse return error.UnsupportedInkling2Request;
    return switch (dtype) {
        .bf16 => .bf16,
        .int4 => .q4_head128,
        else => error.UnsupportedInkling2Request,
    };
}

test "Inkling Metal rejects unsupported streaming configuration during preflight" {
    try std.testing.expectEqual(.bf16, try requestCacheFormat(.{}));
    try std.testing.expectEqual(.q4_head128, try requestCacheFormat(.{ .cache_dtype = "int4" }));
    try std.testing.expectError(error.UnsupportedInkling2Request, requestCacheFormat(.{ .cache_dtype = "int8" }));
    try std.testing.expectError(error.UnsupportedInkling2Request, requestCacheFormat(.{ .prompt_cache_enabled = true }));
    try std.testing.expectError(error.UnsupportedInkling2Request, requestCacheFormat(.{ .speculation_requested = true, .speculation_policy = .force }));
}

pub const TextPipeline = struct {
    allocator: std.mem.Allocator,
    tokenizer: Tokenizer,
    control: Control,
    prompt: chat.Prompt,
    options: chat.ChatOptions,
    continue_ctx: ?*anyopaque = null,
    continue_fn: ?*const fn (*anyopaque) bool = null,

    pub const Callbacks = struct {
        text_ctx: ?*anyopaque = null,
        text: ?generation.TokenCallback = null,
        reasoning_ctx: ?*anyopaque = null,
        reasoning: ?generation.TokenCallback = null,
    };

    pub fn init(a: std.mem.Allocator, tokenizer: Tokenizer, control: Control, messages: []const generation.Message, config: generation.GenerationConfig, input_limit: ?usize, context_limit: u32, options: chat.ChatOptions) !TextPipeline {
        try control.check();
        _ = try requestCacheFormat(config);
        const prompt = try chat.Prompt.init(a, tokenizer, messages, options, config.enable_thinking);
        errdefer prompt.deinit(a);
        if (input_limit) |limit| if (prompt.count() > limit) return error.InferenceInputTokensExceeded;
        _ = try chat.contextCapacity(prompt.count(), config.max_tokens, context_limit);
        return .{ .allocator = a, .tokenizer = tokenizer, .control = control, .prompt = prompt, .options = options };
    }

    fn check(self: *const TextPipeline) !void {
        try self.control.check();
        if (self.continue_fn) |keep_going| {
            if (!keep_going(self.continue_ctx orelse return error.Cancelled)) return error.Cancelled;
        }
    }

    pub fn deinit(self: *TextPipeline) void {
        self.prompt.deinit(self.allocator);
    }

    pub fn requestPlan(self: *const TextPipeline, model: *const decoder.Decoder, config: generation.GenerationConfig) !decoder.RequestPlan {
        return self.requestPlanWithShape(model, config, PrefillShape.preferred(self.prompt.count() -| 1));
    }

    fn requestPlanWithShape(self: *const TextPipeline, model: *const decoder.Decoder, config: generation.GenerationConfig, shape: PrefillShape) !decoder.RequestPlan {
        if (self.prompt.suffix.len == 0) return error.UnsupportedInkling2Request;
        const format = try requestCacheFormat(config);
        const tokens = try chat.contextCapacity(self.prompt.count(), config.max_tokens, model.text.model_max_length);
        const prefix = self.prompt.count() - 1;
        var plan = try decoder.RequestPlan.initWithReductionRows(model, tokens, shape.rows, shape.reduction_rows, format);
        if (chat.usesUnpenalizedGreedy(config)) plan = plan.withDeviceSelection();
        return plan.withPrefillTail(model, @intCast(prefix % shape.rows));
    }

    /// The caller owns the runtime execution gate and memory admission for
    /// the complete request, including prompt prefill and streaming callbacks.
    pub fn generate(self: *const TextPipeline, model: *const decoder.Decoder, eos: u32, config: generation.GenerationConfig, callbacks: Callbacks, binding: ?observation.Binding) !generation.GenerationResult {
        return self.generateWithPlan(model, null, eos, config, callbacks, try self.requestPlan(model, config), binding);
    }

    fn generateWithPlan(self: *const TextPipeline, model: *const decoder.Decoder, vision: ?*const vision_model.Vision, eos: u32, config: generation.GenerationConfig, callbacks: Callbacks, plan: decoder.RequestPlan, binding: ?observation.Binding) !generation.GenerationResult {
        try self.check();
        var request: decoder.Request = undefined;
        try request.initWithPlan(self.allocator, model, plan);
        defer request.deinit();

        var history: std.ArrayList(i64) = .empty;
        defer history.deinit(self.allocator);
        const greedy = chat.usesUnpenalizedGreedy(config);
        const after = self.prompt.suffix;
        const chunk_rows: u32 = @intCast(plan.bank.layout.max_append_tokens);
        var prefill_observation = binding;
        if (prefill_observation) |*selected| {
            selected.planned_prefill = .{
                .max_append_rows = chunk_rows,
                .attention_workspace_rows = if (plan.prefill) |work| work.reduction_rows else 1,
                .cache_capacity_tokens = @intCast(plan.bank.layout.max_tokens),
            };
        }
        try observation.callOperation(prefill_observation, .prefill, prefillPrompt, .{ self, &request, model, vision, after[0 .. after.len - 1], chunk_rows, binding });
        request.releasePrefill();
        if (!greedy) {
            for (self.prompt.media) |item| {
                for (item.prefix) |id| try history.append(self.allocator, id);
            }
            for (after[0 .. after.len - 1]) |id| try history.append(self.allocator, id);
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
            try self.check();
            if (greedy) {
                token = try observation.callOperation(binding, .decode_step, decoder.Request.stepGreedy, .{ &request, token });
            } else {
                const logits = try observation.callOperation(binding, .decode_step, decoder.Request.step, .{ &request, token });
                try history.append(self.allocator, token);
                token = @intCast(generation.sampleTokenFromLogits(self.allocator, logits, config, history.items));
            }
            try self.check();
            if (!config.ignore_eos and (token == eos or (channels == null and token == turn_end))) break;
            try output.append(token);
        }
        return output.finalize(self.options, self.prompt.count(), limit);
    }

    fn prefillPrompt(self: *const TextPipeline, request: *decoder.Request, model: *const decoder.Decoder, vision: ?*const vision_model.Vision, after: []const i32, chunk_rows: u32, binding: ?observation.Binding) !void {
        const media = self.prompt.media;
        const count = media.len * 2 + 1;
        var local_images: [1]?vision_model.Vision.Embeddings = undefined;
        const image_rows = if (media.len <= local_images.len) local_images[0..media.len] else try self.allocator.alloc(?vision_model.Vision.Embeddings, media.len);
        defer if (media.len > local_images.len) self.allocator.free(image_rows);
        @memset(image_rows, null);
        defer for (image_rows) |*value| if (value.*) |*item| item.tensor.deinit();

        // Any nonempty part consumes at least one row of a batch.
        const part_capacity = @min(count, chunk_rows);
        var local_batch: [3]decoder.PrefixPart = undefined;
        const batch = if (part_capacity <= local_batch.len) local_batch[0..part_capacity] else try self.allocator.alloc(decoder.PrefixPart, part_capacity);
        defer if (part_capacity > local_batch.len) self.allocator.free(batch);

        const total = self.prompt.count() - 1;
        var position: usize = 0;
        var part_index: usize = 0;
        var part_position: usize = 0;
        var retired: usize = 0;
        while (position < total) {
            try self.check();
            const rows = @min(total - position, chunk_rows);
            var pending = rows;
            var active: usize = 0;
            while (pending > 0 and part_index < count) {
                const index = part_index / 2;
                const part: decoder.PrefixPart = if (index == media.len)
                    .{ .tokens = after }
                else if (part_index % 2 == 0)
                    .{ .tokens = media[index].prefix }
                else blk: {
                    const stored = &image_rows[index];
                    if (stored.* == null) {
                        const encoder = vision orelse return error.UnsupportedInkling2Request;
                        stored.* = try observation.callOperation(binding, .encoder, encodeImage, .{ self, model, encoder, media[index].image });
                    }
                    const values = &stored.*.?;
                    break :blk .{ .embeddings = .{ .tensor = &values.tensor, .start = 0, .count = values.rows } };
                };
                const take = @min(pending, part.rows() - part_position);
                if (take > 0) {
                    batch[active] = part.slice(part_position, take);
                    active += 1;
                    pending -= take;
                    part_position += take;
                }
                if (part_position == part.rows()) {
                    part_index += 1;
                    part_position = 0;
                }
            }
            if (pending != 0) return error.InvalidShape;
            try request.appendPrefix(batch[0..active]);
            while (retired < part_index / 2) : (retired += 1) {
                if (image_rows[retired]) |*item| item.tensor.deinit();
                image_rows[retired] = null;
            }
            position += rows;
        }
    }

    fn encodeImage(self: *const TextPipeline, model: *const decoder.Decoder, encoder: *const vision_model.Vision, input: chat.Prompt.Image) !vision_model.Vision.Embeddings {
        try self.check();
        const rgb = image.decodeRgb(self.allocator, input.bytes) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidInkling2Image,
        };
        defer rgb.deinit(self.allocator);
        if (rgb.width != input.width or rgb.height != input.height) return error.InvalidInkling2Image;
        var embeddings = try encoder.encodeRgb(self.allocator, model.runtime, rgb.data, rgb.width, rgb.height);
        errdefer embeddings.tensor.deinit();
        if (embeddings.rows != input.rows()) return error.InvalidInkling2Image;
        return embeddings;
    }
};

pub const ServingPipeline = struct {
    /// Generation and continuation callbacks run on the HTTP writer's thread.
    pub const serial_stream_checkpoints = true;

    resident: *Runtime,
    session: backends.Session,
    text: TextPipeline,
    reasoning_ctx: ?*anyopaque = null,
    reasoning_callback: ?generation.TokenCallback = null,
    continue_ctx: ?*anyopaque = null,
    continue_fn: ?*const fn (*anyopaque) bool = null,

    pub fn deinit(self: *ServingPipeline) void {
        self.text.deinit();
    }

    pub fn generate(self: *ServingPipeline, _: []const generation.Message, config: generation.GenerationConfig) !generation.GenerationResult {
        return self.resident.generate(self.session, &self.text, config, .{});
    }

    pub fn generateStreaming(self: *ServingPipeline, _: []const generation.Message, config: generation.GenerationConfig, ctx: *anyopaque, callback: generation.TokenCallback) !generation.GenerationResult {
        self.text.continue_ctx = self.continue_ctx;
        self.text.continue_fn = self.continue_fn;
        defer {
            self.text.continue_ctx = null;
            self.text.continue_fn = null;
        }
        return self.resident.generate(self.session, &self.text, config, .{
            .text_ctx = ctx,
            .text = callback,
            .reasoning_ctx = self.reasoning_ctx,
            .reasoning = self.reasoning_callback,
        });
    }
};
