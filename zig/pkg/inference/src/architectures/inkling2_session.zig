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
const jsonschema = @import("antfly_jsonschema");
const StepScheduler = @import("../runtime/embedding_scheduler.zig").Scheduler;
const Embeddings = vision.Embeddings;
const sync = @import("antfly_platform").sync;

pub const max_active_requests = decoder.decode_batch_rows;

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    ctx: Context,
    model: decoder.Decoder,
    encoder: vision.Vision,
    draft: ?dspark.Draft = null,
    eos: u32,
    fast_matvec: bool,
    resident_bytes: usize,
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

    fn decodeGreedy(self: *Runtime, request: *decoder.Request, token: u32, control: Control) !u32 {
        try control.check();
        const lanes = @min(request.append_capacity, max_active_requests, self.active_greedy.load(.acquire));
        if (!self.fast_matvec or !request.canBatchGreedy() or control.io == null or lanes < 2) {
            var turn: StepScheduler.Waiter = .{};
            try self.enterStep(&turn, control);
            defer StepScheduler.leave(&turn);
            return request.stepGreedy(token);
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
                return request.stepGreedy(token);
            }
            var requests: [max_active_requests]*decoder.Request = undefined;
            var tokens: [max_active_requests]u32 = undefined;
            requests[0] = request;
            tokens[0] = token;
            for (peers, requests[1..count], tokens[1..count]) |follower, *member, *id| {
                member.* = follower.request;
                id.* = follower.token;
            }
            break :blk decoder.Request.stepGreedyBatch(requests[0..count], tokens[0..count], results[0..count]);
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

const tooling = @import("../pipelines/tool_parser.zig");

pub const ChatOptions = struct {
    effort: ?f64 = null,
    tools: []const tooling.ToolDefinition = &.{},
    choice: tooling.ParsedToolChoice = .auto,

    pub fn parseEffort(value: std.json.Value) !?f64 {
        const amount: f64 = switch (value) {
            .null => return null,
            .integer => |n| @floatFromInt(n),
            .float => |n| n,
            .number_string => |n| std.fmt.parseFloat(f64, n) catch return error.InvalidInkling2Effort,
            .string => |name| blk: {
                const efforts = .{ .none = 0.0, .minimal = 0.1, .low = 0.2, .medium = 0.7, .high = 0.9, .xhigh = 0.99, .max = 0.99 };
                inline for (@typeInfo(@TypeOf(efforts)).@"struct".field_names) |key| {
                    if (std.mem.eql(u8, name, key)) break :blk @field(efforts, key);
                }
                return error.InvalidInkling2Effort;
            },
            else => return error.InvalidInkling2Effort,
        };
        if (!std.math.isFinite(amount) or amount < 0 or amount > 0.99) return error.InvalidInkling2Effort;
        return amount;
    }
};

fn toolNameValid(name: []const u8) bool {
    if (name.len == 0 or name.len > 64) return false;
    for (name) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_' and ch != '-') return false;
    return true;
}

const model_marker = "<|message_model|>";
const text_marker = "<|content_text|>";
const reasoning_marker = "<|content_thinking|>";
const tool_marker = "<|content_invoke_tool_json|>";
const end_marker = "<|end_message|>";
const sampling_end_marker = "<|content_model_end_sampling|>";

const Prompt = struct {
    before_image: []i32,
    after_image: []i32,
    image: ?[]const u8,
    image_width: u32 = 0,
    image_height: u32 = 0,
    initial_channel: ?Channel,

    fn init(allocator: std.mem.Allocator, tok: Tokenizer, messages: []const generation.Message, options: ChatOptions, enable_thinking: ?bool) !Prompt {
        const level = if (enable_thinking == false) 0 else options.effort orelse (if (enable_thinking == true) @as(f64, 0.9) else 0);
        const effort = try std.fmt.allocPrint(allocator, "<|message_system|>" ++ text_marker ++ "Thinking effort level: {d}" ++ end_marker, .{level});
        defer allocator.free(effort);
        var text: std.ArrayList(u8) = .empty;
        defer text.deinit(allocator);
        if (options.tools.len != 0) try appendTools(&text, allocator, options);
        var effort_written = false;
        var image_data: ?[]const u8 = null;
        var image_offset: usize = 0;
        for (messages, 0..) |message, message_index| {
            var tool_role: ?[]u8 = null;
            defer if (tool_role) |role| allocator.free(role);
            const role = if (std.mem.eql(u8, message.role, "system"))
                "<|message_system|>"
            else if (std.mem.eql(u8, message.role, "user"))
                "<|message_user|>"
            else if (std.mem.eql(u8, message.role, "assistant"))
                model_marker
            else if (std.mem.eql(u8, message.role, "tool")) blk: {
                const name = toolResultName(messages[0..message_index], message.tool_call_id orelse return error.InvalidInkling2ToolHistory) orelse
                    return error.InvalidInkling2ToolHistory;
                tool_role = try std.fmt.allocPrint(allocator, "<|message_tool|>{s}", .{name});
                break :blk tool_role.?;
            } else return error.UnsupportedInkling2Request;
            if (message.audio_bytes != null or message.video_bytes != null)
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
                if (!message.content_is_null) try appendText(&text, allocator, role, message.content);
            }
            if (message.tool_calls) |calls| {
                if (!std.mem.eql(u8, message.role, "assistant")) return error.InvalidInkling2ToolHistory;
                for (calls) |call| try appendToolHistory(&text, allocator, call);
            }
            if (std.mem.eql(u8, message.role, "assistant"))
                try text.appendSlice(allocator, sampling_end_marker);
        }
        if (!effort_written) try text.appendSlice(allocator, effort);
        const initial_channel: ?Channel = if (level > 0) .reasoning else if (options.tools.len > 0) .header else null;
        if (initial_channel != .header) {
            try text.appendSlice(allocator, model_marker);
            try text.appendSlice(allocator, if (initial_channel == .reasoning) reasoning_marker else text_marker);
        }
        const before = try tok.encode(allocator, text.items[0..image_offset]);
        errdefer allocator.free(before);
        const after = try tok.encode(allocator, text.items[image_offset..]);
        errdefer allocator.free(after);
        if (after.len == 0) return error.InvalidInkling2Tokenizer;
        var prompt = Prompt{ .before_image = before, .after_image = after, .image = image_data, .initial_channel = initial_channel };
        if (image_data) |data| {
            const info = images.inspectEncodedForInference(data, 4096) catch return error.InvalidInkling2Image;
            prompt.image_width = info.width;
            prompt.image_height = info.height;
        }
        return prompt;
    }

    fn appendTools(text: *std.ArrayList(u8), allocator: std.mem.Allocator, options: ChatOptions) !void {
        const Spec = struct { description: []const u8, name: []const u8, parameters: std.json.Value, type: []const u8 };
        const specs = try allocator.alloc(Spec, options.tools.len);
        defer allocator.free(specs);
        for (options.tools, specs) |tool, *spec| {
            if (!toolNameValid(tool.function.name)) return error.InvalidInkling2ToolHistory;
            spec.* = .{
                .description = tool.function.description,
                .name = tool.function.name,
                .parameters = if (tool.function.parameters) |schema| .{ .object = schema.map } else .{ .object = .{} },
                .type = tool.type,
            };
        }
        const encoded = try std.json.Stringify.valueAlloc(allocator, specs, .{});
        defer allocator.free(encoded);
        try text.appendSlice(allocator, "<|message_system|>tool_declare<|content_xml|>");
        try text.appendSlice(allocator, encoded);
        try text.appendSlice(allocator, end_marker);
        switch (options.choice) {
            .required => try appendText(text, allocator, "<|message_system|>", "Call one of the declared tools in your next response."),
            .function => |name| {
                const instruction = try std.fmt.allocPrint(allocator, "Call the {s} tool in your next response.", .{name});
                defer allocator.free(instruction);
                try appendText(text, allocator, "<|message_system|>", instruction);
            },
            else => {},
        }
    }

    fn appendToolHistory(text: *std.ArrayList(u8), allocator: std.mem.Allocator, call: generation.Message.ToolCall) !void {
        if (!toolNameValid(call.name) or !std.mem.eql(u8, call.type, "function")) return error.InvalidInkling2ToolHistory;
        const parsed = std.json.parseFromSlice(std.json.Value, allocator, call.arguments, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidInkling2ToolHistory,
        };
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidInkling2ToolHistory;
        const call_json = try std.json.Stringify.valueAlloc(allocator, .{ .name = call.name, .args = parsed.value }, .{});
        defer allocator.free(call_json);
        for ([_][]const u8{ model_marker, call.name, tool_marker, call_json, end_marker }) |part| try text.appendSlice(allocator, part);
    }

    fn toolResultName(messages: []const generation.Message, id: []const u8) ?[]const u8 {
        for (messages) |message| {
            for (message.tool_calls orelse &.{}) |call| {
                if (std.mem.eql(u8, call.id, id) and toolNameValid(call.name)) return call.name;
            }
        }
        return null;
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
    prefill_rows: u32,
    chunk_rows: u32,
    speculate: bool,
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
        const context = prompt.count() +| @as(usize, @intCast(config.max_tokens)) - 1;
        if (context > model.model.text.model_max_length) return error.InvalidInkling2Context;
        const speculation_requested = config.speculation_requested and config.speculation_policy != .off;
        const block_verification = decoder.speculativeBlockContextSupported(model.model.text, context);
        const auto_context_disabled = speculation_requested and config.speculation_policy == .auto and
            !block_verification;
        const speculate = speculation_requested and !auto_context_disabled;
        if (speculate and (model.draft == null or !usesGreedy(config) or config.speculative_k == 0 or
            config.speculative_k > dspark.proposal_rows)) return error.UnsupportedInkling2Speculation;
        if (speculate and context > model.draft.?.max_capacity) return error.InvalidInkling2Context;
        const batch_prompt = model.fast_matvec and prompt.count() > decoder.prefill_chunk_rows;
        const prefill_rows: u32 = if (batch_prompt or (speculate and block_verification)) @intCast(@min(decoder.prefill_chunk_rows, context)) else 1;
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

    fn prefill(self: *Pipeline, request: *decoder.Request, input: []const i32) !void {
        var offset: usize = 0;
        while (offset < input.len) {
            try self.check();
            var tokens: [decoder.prefill_chunk_rows]u32 = undefined;
            const count = @min(self.prefill_rows, input.len - offset);
            for (input[offset..][0..count], tokens[0..count]) |id, *token| token.* = @intCast(id);
            var turn: StepScheduler.Waiter = .{};
            try self.model.enterStep(&turn, self.control);
            defer StepScheduler.leave(&turn);
            if (count == 1) try request.append(tokens[0]) else try request.appendTokens(tokens[0..count]);
            offset += count;
        }
    }

    fn usesGreedy(config: generation.GenerationConfig) bool {
        return config.temperature == 0 and config.repetition_penalty == 1 and
            config.frequency_penalty == 0 and config.presence_penalty == 0;
    }

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
            try request.attachDraft(active);
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
        return self.model.encoder.encodeRgb(&self.model.model.blas, rgb.data, rgb.width, rgb.height);
    }

    fn prefillImage(self: *Pipeline, request: *decoder.Request, data: []const u8) !void {
        const rgb = try @import("../pipelines/inkling2_image.zig").decodeRgb(self.allocator, data);
        defer rgb.deinit(self.allocator);
        var embeddings = try self.encodeImage(rgb);
        defer {
            self.model.enterCleanup();
            embeddings.deinit(&self.model.ctx);
            self.model.execution_gate.unlock();
        }
        const row_bytes = @as(usize, self.model.model.text.hidden_size) * 2;
        var offset: u32 = 0;
        while (offset < embeddings.rows) {
            try self.check();
            const rows = @min(self.prefill_rows, embeddings.rows - offset);
            var turn: StepScheduler.Waiter = .{};
            try self.model.enterStep(&turn, self.control);
            defer StepScheduler.leave(&turn);
            try request.appendEmbeddings(.{
                .ptr = embeddings.device.ptr + @as(usize, offset) * row_bytes,
                .len = @as(usize, rows) * row_bytes,
                .borrowed = true,
            }, rows);
            offset += rows;
        }
    }

    fn next(self: *Pipeline, request: *decoder.Request, config: generation.GenerationConfig, history: *std.ArrayList(i64), token: u32) !u32 {
        if (usesGreedy(config)) return self.model.decodeGreedy(request, token, self.control);
        const logits = blk: {
            var turn: StepScheduler.Waiter = .{};
            try self.model.enterStep(&turn, self.control);
            defer StepScheduler.leave(&turn);
            break :blk try request.step(token);
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
        const pixels = @as(usize, self.prompt.image_width) * self.prompt.image_height;
        const plan = try decoder.RequestPlan.init(&model.model, self.context_tokens, self.chunk_rows);
        const amount = try plan.memory(self.speculate);
        const draft_kv = if (self.speculate) dspark.Draft.requestKvBytes(self.context_tokens) else 0;
        const image_workspace = if (self.prompt.image != null) vision.requestWorkspaceBytes(self.prompt.imageRows(), pixels) else 0;
        var admission = try session.admitRetainedStateWithWorkspace(
            try std.math.add(usize, amount.host, try std.math.mul(usize, pixels, 11)),
            try std.math.add(usize, amount.kv, draft_kv),
            try std.math.add(usize, amount.workspace, image_workspace),
        );
        defer admission.deinit();

        var request: decoder.Request = undefined;
        const draft: ?*dspark.Draft = if (self.speculate) &model.draft.? else null;
        try self.initializeRequest(&request, plan, draft);
        defer self.deinitializeRequest(&request, draft);
        try self.prefill(&request, self.prompt.before_image);
        if (self.prompt.image) |data| try self.prefillImage(&request, data);
        const after = self.prompt.after_image;
        try self.prefill(&request, after[0 .. after.len - 1]);
        var history: std.ArrayList(i64) = .empty;
        defer history.deinit(self.allocator);
        if (!usesGreedy(config)) {
            for (self.prompt.before_image) |id| try history.append(self.allocator, id);
            for (after[0 .. after.len - 1]) |id| try history.append(self.allocator, id);
        }
        const turn_end = try specialToken(self.allocator, self.tokenizer, end_marker);
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
        if (output.channel == .tool and output.tokens.items.len < limit) try output.finishTool();
        if ((self.options.choice == .required or self.options.choice == .function) and output.calls.items.len == 0 and output.tokens.items.len < limit)
            return error.InvalidInkling2ToolOutput;
        const result = if (callback != null or channels != null)
            try output.text.bytes.toOwnedSlice(self.allocator)
        else
            try self.tokenizer.decodeText(self.allocator, output.tokens.items);
        errdefer self.allocator.free(result);
        const reasoning = if (output.reasoning.bytes.items.len != 0) try output.reasoning.bytes.toOwnedSlice(self.allocator) else null;
        errdefer if (reasoning) |reasoning_text| self.allocator.free(reasoning_text);
        const ids = try output.tokens.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(ids);
        const calls = if (output.calls.items.len != 0) try output.calls.toOwnedSlice(self.allocator) else null;
        return .{
            .allocator = self.allocator,
            .text = result,
            .reasoning_text = reasoning,
            .native_tool_calls = calls,
            .token_ids = ids,
            .prompt_tokens = self.prompt.count(),
            .tokens_used = ids.len,
            .finish_reason = if (ids.len == limit) "length" else if (calls != null) "tool_calls" else "stop",
            .speculative = if (draft != null) stats else if (self.speculation_context_disabled) .{
                .speculation_policy = config.speculation_policy,
                .speculation_policy_decision = .disabled_unavailable,
                .mtp_disabled_reason = "inkling_speculative_context_limit",
            } else null,
        };
    }
};

const Channel = enum { header, text, reasoning, tool };

fn specialToken(allocator: std.mem.Allocator, tokenizer: Tokenizer, marker: []const u8) !u32 {
    const ids = try tokenizer.encode(allocator, marker);
    defer allocator.free(ids);
    if (ids.len != 1 or ids[0] < 0) return error.InvalidInkling2Tokenizer;
    return @intCast(ids[0]);
}

const ChannelTokens = struct {
    header: u32,
    end: u32,
    text: u32,
    reasoning: u32,
    tool: u32,

    fn init(allocator: std.mem.Allocator, tokenizer: Tokenizer, turn_end: u32) !ChannelTokens {
        return .{
            .header = try specialToken(allocator, tokenizer, model_marker),
            .end = turn_end,
            .text = try specialToken(allocator, tokenizer, text_marker),
            .reasoning = try specialToken(allocator, tokenizer, reasoning_marker),
            .tool = try specialToken(allocator, tokenizer, tool_marker),
        };
    }
};

const Output = struct {
    allocator: std.mem.Allocator,
    tokenizer: Tokenizer,
    tokens: std.ArrayList(i32) = .empty,
    text: Utf8Text = .{},
    reasoning: Utf8Text = .{},
    tool_tokens: std.ArrayList(i32) = .empty,
    calls: std.ArrayList(tooling.ToolCall) = .empty,
    ctx: ?*anyopaque,
    callback: ?generation.TokenCallback,
    reasoning_ctx: ?*anyopaque,
    reasoning_callback: ?generation.TokenCallback,
    channels: ?ChannelTokens,
    channel: Channel,
    tools: []const tooling.ToolDefinition,

    fn deinit(self: *Output) void {
        self.tokens.deinit(self.allocator);
        self.text.bytes.deinit(self.allocator);
        self.reasoning.bytes.deinit(self.allocator);
        self.tool_tokens.deinit(self.allocator);
        for (self.calls.items) |*call| call.deinit(self.allocator);
        self.calls.deinit(self.allocator);
    }

    fn append(self: *Output, token: u32) !void {
        const at_start = self.tokens.items.len == 0;
        try self.tokens.append(self.allocator, @intCast(token));
        if (self.channels) |channels| {
            const channel: ?Channel = if (token == channels.header or token == channels.end)
                .header
            else if (token == channels.text)
                .text
            else if (token == channels.reasoning)
                .reasoning
            else if (token == channels.tool)
                .tool
            else
                null;
            if (channel) |next| {
                if (self.channel == .tool) try self.finishTool();
                self.text.pending_len = 0;
                self.reasoning.pending_len = 0;
                self.channel = next;
                return;
            }
        } else if (self.callback == null) return;

        switch (self.channel) {
            .header => {},
            .tool => try self.tool_tokens.append(self.allocator, @intCast(token)),
            .text => try self.appendText(&self.text, self.ctx, self.callback, token, at_start),
            .reasoning => try self.appendText(&self.reasoning, self.reasoning_ctx, self.reasoning_callback, token, at_start),
        }
    }

    fn appendText(self: *Output, output: *Utf8Text, ctx: ?*anyopaque, callback: ?generation.TokenCallback, token: u32, at_start: bool) !void {
        const bytes = try self.tokenizer.decodeToken(self.allocator, @intCast(token), at_start);
        defer self.allocator.free(bytes);
        const start = output.bytes.items.len;
        for (bytes) |byte| try output.appendByte(self.allocator, byte);
        if (callback) |emit| {
            if (output.bytes.items.len > start and !emit(ctx.?, output.bytes.items[start..]))
                return error.Cancelled;
        }
    }

    fn finishTool(self: *Output) !void {
        const data = try self.tokenizer.decodeText(self.allocator, self.tool_tokens.items);
        defer self.allocator.free(data);
        const parsed = std.json.parseFromSlice(struct { name: []const u8, args: std.json.Value }, self.allocator, data, .{ .parse_numbers = false }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidInkling2ToolOutput,
        };
        defer parsed.deinit();
        if (parsed.value.args != .object) return error.InvalidInkling2ToolOutput;
        for (self.tools) |tool| {
            if (!std.mem.eql(u8, tool.function.name, parsed.value.name)) continue;
            if (tool.function.strict) {
                if (tool.function.parameters) |schema| {
                    jsonschema.validateJsonSchemaValue(self.allocator, .{ .object = schema.map }, parsed.value.args) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => return error.InvalidInkling2ToolOutput,
                    };
                }
            }
            const name = try self.allocator.dupe(u8, parsed.value.name);
            errdefer self.allocator.free(name);
            const arguments = try std.json.Stringify.valueAlloc(self.allocator, parsed.value.args, .{});
            errdefer self.allocator.free(arguments);
            const id = try tooling.generateCallId(self.allocator);
            errdefer self.allocator.free(id);
            try self.calls.append(self.allocator, .{ .id = id, .type = "function", .function = .{ .name = name, .arguments = arguments } });
            self.tool_tokens.clearRetainingCapacity();
            return;
        }
        return error.InvalidInkling2ToolOutput;
    }
};

const Utf8Text = struct {
    bytes: std.ArrayList(u8) = .empty,
    pending_utf8: [4]u8 = undefined,
    pending_len: usize = 0,

    fn appendByte(self: *Utf8Text, allocator: std.mem.Allocator, byte: u8) !void {
        if (self.pending_len != 0 and byte & 0xc0 != 0x80) self.pending_len = 0;
        if (self.pending_len == 0 and byte < 0x80) {
            try self.bytes.append(allocator, byte);
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
            if (std.unicode.utf8ValidateSlice(sequence)) try self.bytes.appendSlice(allocator, sequence);
            self.pending_len = 0;
        }
    }
};
