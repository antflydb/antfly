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
const chat = @import("inkling2_chat.zig");
const Prompt = chat.Prompt;
const ChannelTokens = chat.ChannelTokens;
const Output = chat.Output;
pub const ChatOptions = chat.ChatOptions;
const StepScheduler = @import("../runtime/embedding_scheduler.zig").Scheduler;
const Embeddings = vision.Embeddings;
const sync = @import("antfly_platform").sync;

pub const max_active_requests = decoder.decode_batch_rows;

const observation = @import("../execution/invocation_observation.zig");
const diagnostics = @import("../execution/diagnostics.zig");
const batch_observation = @import("../execution/shared_batch_observation.zig");

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    ctx: Context,
    model: decoder.Decoder,
    encoder: vision.Vision,
    draft: ?dspark.Draft = null,
    eos: u32,
    fast_matvec: bool,
    resident_bytes: usize,
    invocation_diagnostics: diagnostics.Diagnostics,
    execution_gate: std.atomic.Mutex,
    steps: StepScheduler,
    batch_lock: std.atomic.Mutex,
    unmatched_decode: ?*DecodeStep,
    active_greedy: std.atomic.Value(u32),
    batch_steps: [max_active_requests - 1]std.atomic.Value(u64),

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
        self.execution_gate = .unlocked;
        self.steps = .{};
        self.batch_lock = .unlocked;
        self.unmatched_decode = null;
        self.active_greedy = .init(0);
        self.batch_steps = @splat(.init(0));
        self.invocation_diagnostics = .{};
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
        return .{
            .ptr = self,
            .vtable = &vtable,
            .invocation_diagnostics = .{
                .diagnostics = &self.invocation_diagnostics,
                .model_allocator = allocator,
                .context_runtime_scope = .unavailable,
            },
        };
    }

    pub fn fromSession(session: backends.Session) ?*Runtime {
        return if (session.vtable == &vtable) @ptrCast(@alignCast(session.ptr)) else null;
    }

    fn enterStep(self: *Runtime, waiter: *StepScheduler.Waiter, control: Control) !void {
        try self.steps.enter(waiter, &self.execution_gate, control);
        errdefer StepScheduler.leave(waiter);
        try self.ctx.makeCurrent();
    }

    fn enterCleanup(self: *Runtime) void {
        sync.lockYielding(&self.execution_gate);
        self.ctx.makeCurrent() catch @panic("unable to bind the Inkling CUDA context for cleanup");
    }

    const DecodeStep = struct {
        request: *decoder.Request,
        token: u32,
        io: std.Io,
        peers: [max_active_requests - 1]*DecodeStep = undefined,
        peer_count: usize = 0,
        peer_limit: usize = 0,
        result: anyerror!u32 = error.InklingDecodeStepIncomplete,
        done: std.Io.Event = .unset,
    };

    pub fn pairedDecodeSteps(self: *const Runtime) u64 {
        return self.batch_steps[0].load(.monotonic);
    }

    pub fn batchedDecodeTokens(self: *const Runtime) u64 {
        var tokens: u64 = 0;
        for (&self.batch_steps, 2..) |*steps, lanes| tokens +|= steps.load(.monotonic) *| lanes;
        return tokens;
    }

    fn describeDecode(request: *decoder.Request) diagnostics.Batch.Participant {
        return .{ .phase = .decode, .query_rows = 1, .total_rows = request.position() + 1 };
    }

    fn decodeGreedy(self: *Runtime, request: *decoder.Request, token: u32, control: Control, binding: ?observation.Binding) !u32 {
        try control.check();
        const lanes = @min(request.append_capacity, max_active_requests, self.active_greedy.load(.acquire));
        if (!self.fast_matvec or !request.canBatchGreedy() or control.io == null or lanes < 2) {
            var turn: StepScheduler.Waiter = .{};
            try self.enterStep(&turn, control);
            defer StepScheduler.leave(&turn);
            return observation.callOperation(binding, .decode_step, decoder.Request.stepGreedy, .{ request, token });
        }

        var step: DecodeStep = .{ .request = request, .token = token, .io = control.io.?, .peer_limit = lanes - 1 };
        sync.lockYielding(&self.batch_lock);
        if (self.unmatched_decode) |leader| {
            leader.peers[leader.peer_count] = &step;
            leader.peer_count += 1;
            if (leader.peer_count == leader.peer_limit) self.unmatched_decode = null;
            self.batch_lock.unlock();
            // Once claimed, the leader borrows this request's GPU state until
            // the stream drains. Cancellation must not unwind that storage.
            step.done.waitUncancelable(step.io);
            try control.check();
            return step.result;
        }
        self.unmatched_decode = &step;
        self.batch_lock.unlock();

        var turn: StepScheduler.Waiter = .{};
        const entered = self.enterStep(&turn, .{});
        defer StepScheduler.leave(&turn);
        if (entered) |_| {
            sync.lockYielding(&self.batch_lock);
            const waiting = step.peer_count < step.peer_limit;
            self.batch_lock.unlock();
            if (waiting) std.Io.sleep(step.io, .fromMicroseconds(150), .awake) catch {};
        } else |_| {}

        sync.lockYielding(&self.batch_lock);
        if (self.unmatched_decode == &step) self.unmatched_decode = null;
        self.batch_lock.unlock();
        const peers = step.peers[0..step.peer_count];
        const count = peers.len + 1;
        var results: [max_active_requests]u32 = undefined;
        const completed: anyerror!void = blk: {
            entered catch |err| break :blk err;
            if (peers.len == 0) {
                try control.check();
                return observation.callOperation(binding, .decode_step, decoder.Request.stepGreedy, .{ request, token });
            }
            var requests: [max_active_requests]*decoder.Request = undefined;
            var tokens: [max_active_requests]u32 = undefined;
            requests[0] = request;
            tokens[0] = token;
            for (peers, requests[1..count], tokens[1..count]) |follower, *member, *id| {
                member.* = follower.request;
                id.* = follower.token;
            }
            break :blk batch_observation.call(binding, null, .scheduled_step, requests[0..count], describeDecode, decoder.Request.stepGreedyBatch, .{ requests[0..count], tokens[0..count], results[0..count] });
        };
        if (completed) |_| {
            _ = self.batch_steps[count - 2].fetchAdd(1, .monotonic);
            for (peers, results[1..count]) |follower, result| {
                follower.result = result;
                follower.done.set(follower.io);
            }
            try control.check();
            return results[0];
        } else |err| {
            for (peers) |follower| {
                follower.result = err;
                follower.done.set(follower.io);
            }
            return err;
        }
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
        return .{ .measured_resident_weights = self.resident_bytes, .max_active_requests = max_active_requests };
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

pub const Pipeline = struct {
    allocator: std.mem.Allocator,
    model: *Runtime,
    session: backends.Session,
    tokenizer: Tokenizer,
    control: Control,
    prompt: Prompt,
    context_tokens: u32,
    prefill_rows: u32,
    chunk_rows: u32,
    speculate: bool,
    draft_verification: decoder.Request.DraftVerification,
    speculation_context_disabled: bool,
    options: ChatOptions,
    reasoning_ctx: ?*anyopaque = null,
    reasoning_callback: ?generation.TokenCallback = null,
    continue_ctx: ?*anyopaque = null,
    continue_fn: ?*const fn (*anyopaque) bool = null,

    pub fn init(allocator: std.mem.Allocator, model: *Runtime, session: backends.Session, tokenizer: Tokenizer, control: Control, messages: []const generation.Message, config: generation.GenerationConfig, input_limit: ?usize) !Pipeline {
        return initChat(allocator, model, session, tokenizer, control, messages, config, input_limit, .{});
    }

    pub fn initChat(allocator: std.mem.Allocator, model: *Runtime, session: backends.Session, tokenizer: Tokenizer, control: Control, messages: []const generation.Message, config: generation.GenerationConfig, input_limit: ?usize, options: ChatOptions) !Pipeline {
        if (config.grammar != null or config.cache_dtype != null or
            config.kv_cache_mode != .exact_hybrid or config.cache_compaction_ratio != null or
            config.prompt_cache_enabled or config.draft_model != null or config.token_observer != null)
            return error.UnsupportedInkling2Request;
        try control.check();
        const prompt = try Prompt.init(allocator, tokenizer, messages, options, config.enable_thinking);
        errdefer prompt.deinit(allocator);
        if (input_limit) |limit| if (prompt.count() > limit) return error.InferenceInputTokensExceeded;
        const context = try chat.contextCapacity(prompt.count(), config.max_tokens, model.model.text.model_max_length);
        const speculation_requested = config.speculation_requested and config.speculation_policy != .off;
        const block_verification = decoder.speculativeBlockContextSupported(model.model.text, context);
        const auto_context_disabled = speculation_requested and config.speculation_policy == .auto and
            !block_verification;
        const speculate = speculation_requested and !auto_context_disabled;
        if (speculate and (model.draft == null or !usesGreedy(config) or config.speculative_k == 0 or
            config.speculative_k > dspark.proposal_rows)) return error.UnsupportedInkling2Speculation;
        if (speculate and context > model.draft.?.max_capacity) return error.InvalidInkling2Context;
        const batch_prompt = model.fast_matvec and prompt.count() > decoder.prefill_chunk_rows;
        const prefill_rows: u32 = if (batch_prompt) @intCast(@min(decoder.prefill_chunk_rows, context)) else 1;
        const draft_verification: decoder.Request.DraftVerification = if (block_verification and
            @import("antfly_platform").env.getenvBool("ANTFLY_INFERENCE_INKLING_BATCHED_SPECULATION"))
            .batched
        else
            .exact_greedy;
        // The row-wise verifier must start from the same prefill as regular
        // decoding. Reserve its draft block capacity independently.
        const decode_rows: u32 = if (speculate) dspark.block_rows else if (model.fast_matvec and usesGreedy(config)) max_active_requests else 1;
        return .{
            .allocator = allocator,
            .model = model,
            .session = session,
            .tokenizer = tokenizer,
            .control = control,
            .prompt = prompt,
            .context_tokens = @intCast(context),
            .prefill_rows = prefill_rows,
            .chunk_rows = @max(prefill_rows, @min(decode_rows, context)),
            .speculate = speculate,
            .draft_verification = draft_verification,
            .speculation_context_disabled = auto_context_disabled,
            .options = options,
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

    fn prefillPrompt(self: *Pipeline, request: *decoder.Request, suffix: []const i32, progress: ?*diagnostics.PrefillProgress) !void {
        for (self.prompt.media) |item| {
            try self.prefill(request, item.prefix, progress);
            try self.prefillImage(request, item.image, progress);
        }
        try self.prefill(request, suffix, progress);
    }

    fn publishPrefill(self: *Pipeline, request: *decoder.Request, suffix: []const i32) !void {
        var binding = self.session.invocation_diagnostics orelse return self.prefillPrompt(request, suffix, null);
        if (!binding.diagnostics.isEnabled()) return self.prefillPrompt(request, suffix, null);
        var progress: diagnostics.PrefillProgress = .{};
        binding.planned_prefill = .{
            .max_append_rows = self.prefill_rows,
            .attention_workspace_rows = request.append_capacity,
            .cache_capacity_tokens = request.max_tokens,
        };
        binding.prefill_progress = &progress;
        return observation.callOperation(binding, .prefill, prefillPrompt, .{ self, request, suffix, &progress });
    }

    fn prefill(self: *Pipeline, request: *decoder.Request, input: []const i32, progress: ?*diagnostics.PrefillProgress) !void {
        var offset: usize = 0;
        while (offset < input.len) {
            try self.check();
            var tokens: [decoder.prefill_chunk_rows]u32 = undefined;
            const count = @min(self.prefill_rows, input.len - offset);
            for (input[offset..][0..count], tokens[0..count]) |id, *token| token.* = @intCast(id);
            var turn: StepScheduler.Waiter = .{};
            try self.model.enterStep(&turn, self.control);
            defer StepScheduler.leave(&turn);
            defer if (progress) |actual| actual.commitThrough(@intCast(request.position()));
            if (count == 1)
                try request.append(tokens[0])
            else
                try request.appendTokens(tokens[0..count]);
            offset += count;
        }
    }

    const usesGreedy = chat.usesUnpenalizedGreedy;

    fn initializeRequest(self: *Pipeline, request: *decoder.Request, plan: decoder.RequestPlan, draft: ?*dspark.Draft) !void {
        var turn: StepScheduler.Waiter = .{};
        try self.model.enterStep(&turn, self.control);
        defer StepScheduler.leave(&turn);
        try request.initWithPlan(self.allocator, plan);
        errdefer request.deinit() catch @panic("unable to drain the Inkling CUDA stream");
        request.use_dense = self.model.fast_matvec;
        if (draft) |active| {
            try active.beginRequest(self.context_tokens, request.append_capacity);
            errdefer active.endRequest();
            try request.attachDraftWithVerification(active, self.draft_verification);
        }
    }

    fn deinitializeRequest(self: *Pipeline, request: *decoder.Request, draft: ?*dspark.Draft) void {
        self.model.enterCleanup();
        defer self.model.execution_gate.unlock();
        request.deinit() catch @panic("unable to drain the Inkling CUDA stream");
        if (draft) |active| active.endRequest();
    }

    fn encodeImage(self: *Pipeline, rgb: images.Image) !Embeddings {
        var turn: StepScheduler.Waiter = .{};
        try self.model.enterStep(&turn, self.control);
        defer StepScheduler.leave(&turn);
        return observation.callOperation(self.session.invocation_diagnostics, .encoder, vision.Vision.encodeRgb, .{ &self.model.encoder, &self.model.model.blas, rgb.data, rgb.width, rgb.height });
    }

    fn prefillImage(self: *Pipeline, request: *decoder.Request, image: Prompt.Image, progress: ?*diagnostics.PrefillProgress) !void {
        const rgb = try @import("../pipelines/inkling2_image.zig").decodeRgb(self.allocator, image.bytes);
        defer rgb.deinit(self.allocator);
        if (rgb.width != image.width or rgb.height != image.height) return error.InvalidInkling2Image;
        var embeddings = try self.encodeImage(rgb);
        defer {
            self.model.enterCleanup();
            embeddings.deinit(&self.model.ctx);
            self.model.execution_gate.unlock();
        }
        if (embeddings.rows != image.rows()) return error.InvalidInkling2Image;
        const row_bytes = @as(usize, self.model.model.text.hidden_size) * 2;
        var offset: u32 = 0;
        while (offset < embeddings.rows) {
            try self.check();
            const rows = @min(self.prefill_rows, embeddings.rows - offset);
            var turn: StepScheduler.Waiter = .{};
            try self.model.enterStep(&turn, self.control);
            defer StepScheduler.leave(&turn);
            const input: @import("../ops/cuda/buffer.zig").DeviceBuffer = .{
                .ptr = embeddings.device.ptr + @as(usize, offset) * row_bytes,
                .len = @as(usize, rows) * row_bytes,
                .borrowed = true,
            };
            defer if (progress) |actual| actual.commitThrough(@intCast(request.position()));
            try request.appendEmbeddings(input, rows);
            offset += rows;
        }
    }

    fn next(self: *Pipeline, request: *decoder.Request, config: generation.GenerationConfig, history: *std.ArrayList(i64), token: u32) !u32 {
        if (usesGreedy(config)) return self.model.decodeGreedy(request, token, self.control, self.session.invocation_diagnostics);
        const logits = blk: {
            var turn: StepScheduler.Waiter = .{};
            try self.model.enterStep(&turn, self.control);
            defer StepScheduler.leave(&turn);
            break :blk try observation.callOperation(self.session.invocation_diagnostics, .decode_step, decoder.Request.step, .{ request, token });
        };
        try history.append(self.allocator, token);
        return @intCast(generation.sampleTokenFromLogits(self.allocator, logits, config, history.items));
    }

    fn verify(self: *Pipeline, request: *decoder.Request, draft: *dspark.Draft, anchor: u32, count: u32) !decoder.Request.Verification {
        var turn: StepScheduler.Waiter = .{};
        try self.model.enterStep(&turn, self.control);
        defer StepScheduler.leave(&turn);
        const proposed = try draft.propose(&self.model.model.blas, anchor, count);
        return request.verifyDraft(anchor, proposed[0..count]);
    }

    fn decode(self: *Pipeline, config: generation.GenerationConfig, callback_ctx: ?*anyopaque, callback: ?generation.TokenCallback) !generation.GenerationResult {
        try self.check();
        const model = self.model;
        const session = self.session.withAdmissionControl(self.control);
        var image_pixels: usize = 0;
        var image_workspace: usize = 0;
        for (self.prompt.media) |item| {
            const pixels = @as(usize, item.image.width) * item.image.height;
            image_pixels = @max(image_pixels, pixels);
            image_workspace = @max(image_workspace, vision.requestWorkspaceBytes(item.image.rows(), pixels));
        }
        const plan = try decoder.RequestPlan.init(&model.model, self.context_tokens, self.chunk_rows);
        const amount = try plan.memory(self.speculate, self.draft_verification == .batched);
        const draft_kv = if (self.speculate) dspark.Draft.requestKvBytes(self.context_tokens) else 0;
        var admission = try session.admitRetainedStateWithWorkspace(
            try std.math.add(usize, amount.host, try std.math.mul(usize, image_pixels, 11)),
            try std.math.add(usize, amount.kv, draft_kv),
            try std.math.add(usize, amount.workspace, image_workspace),
        );
        defer admission.deinit();

        var request: decoder.Request = undefined;
        const draft: ?*dspark.Draft = if (self.speculate) &model.draft.? else null;
        try self.initializeRequest(&request, plan, draft);
        defer self.deinitializeRequest(&request, draft);
        const after = self.prompt.suffix;
        try self.publishPrefill(&request, after[0 .. after.len - 1]);
        var history: std.ArrayList(i64) = .empty;
        defer history.deinit(self.allocator);
        if (!usesGreedy(config)) {
            for (self.prompt.media) |item| {
                for (item.prefix) |id| try history.append(self.allocator, id);
            }
            for (after[0 .. after.len - 1]) |id| try history.append(self.allocator, id);
        }
        const turn_end = try chat.turnEndToken(self.allocator, self.tokenizer);
        const channels: ?ChannelTokens = if (self.prompt.initial_channel != null) try .init(self.allocator, self.tokenizer, turn_end) else null;
        const limit: usize = @intCast(config.max_tokens);
        const batch_eligible = usesGreedy(config) and request.canBatchGreedy();
        if (batch_eligible) _ = model.active_greedy.fetchAdd(1, .acq_rel);
        defer if (batch_eligible) {
            _ = model.active_greedy.fetchSub(1, .acq_rel);
        };
        var output: Output = .{
            .allocator = self.allocator,
            .tokenizer = self.tokenizer,
            .ctx = callback_ctx,
            .callback = callback,
            .reasoning_ctx = self.reasoning_ctx,
            .reasoning_callback = self.reasoning_callback,
            .channels = channels,
            .channel = self.prompt.initial_channel orelse .text,
            .tools = self.options.tools,
        };
        defer output.deinit();
        var stats: generation.SpeculativeDecodeStats = .{ .speculation_policy = config.speculation_policy, .speculation_policy_decision = if (config.speculation_policy == .force) .forced else .active, .mtp_enabled = draft != null };
        var last = try self.next(&request, config, &history, @intCast(after[after.len - 1]));
        decode_loop: while (true) {
            try self.check();
            if (!config.ignore_eos and (last == model.eos or (channels == null and last == turn_end))) break;
            try output.append(last);
            const remaining = limit - output.tokens.items.len;
            if (remaining == 0) break;
            if (draft) |active| {
                if (remaining > 1 and request.append_capacity > 1) {
                    const count: u32 = @intCast(@min(config.speculative_k, remaining - 1));
                    const verified = try self.verify(&request, active, last, count);
                    stats.rounds += 1;
                    stats.drafted_tokens += count;
                    stats.matched_draft_tokens += verified.count - 1;
                    stats.accepted_tokens += verified.count - 1;
                    stats.correction_tokens += @intFromBool(verified.count <= count);
                    stats.bonus_tokens += @intFromBool(verified.count > count);
                    for (verified.ids[0 .. verified.count - 1]) |id| {
                        if (!config.ignore_eos and (id == model.eos or (channels == null and id == turn_end))) break :decode_loop;
                        try output.append(id);
                    }
                    last = verified.ids[verified.count - 1];
                    continue;
                }
            }
            last = try self.next(&request, config, &history, last);
        }
        var result = try output.finalize(self.options, self.prompt.count(), limit);
        result.speculative = if (draft != null) stats else if (self.speculation_context_disabled) .{
            .speculation_policy = config.speculation_policy,
            .speculation_policy_decision = .disabled_unavailable,
            .mtp_disabled_reason = "inkling_speculative_context_limit",
        } else null;
        return result;
    }
};
