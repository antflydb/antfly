// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded, request-owned Qwen memory generation. Published model/feature
//! authority and resident-model admission remain the caller's responsibility.
const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const tok = @import("inference_tokenizer");
const ops = @import("../ops/ops.zig");
const execution = @import("../ops/execution_scope.zig");
const control_mod = @import("../execution_control.zig");
const runtime = @import("../runtime/root.zig");
const gpt_model = @import("../models/gpt.zig");
const gpt = @import("../architectures/gpt.zig");
const generation = @import("generation.zig");
const qualification = @import("../models/memorilla_qualification.zig");
const A = std.mem.Allocator;
const CB = ops.ComputeBackend;
const CT = ops.CT;
const Control = control_mod.InferenceExecutionControl;
const memory = runtime.tier.memory;
const PhysicalBudgetScope = if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig").CudaCompute.MemorillaDeviceBudgetScope else struct {
    fn deinit(_: *@This()) void {}
};

pub const default_system_prompt = "Answer the question using the provided memory.";
pub const marker = "<|memory|>";
pub const Limits = struct {
    max_prompt_bytes: usize = 128 * 1024,
    max_prompt_tokens: usize = 1024,
    max_memory_rows: usize = 64,
    max_new_tokens: usize = 256,
    max_output_bytes: usize = 64 * 1024,
    max_host_bytes: usize = 16 * 1024 * 1024,
    max_backend_bytes: usize = 4 * 1024 * 1024 * 1024,
    max_kv_bytes: usize = 2 * 1024 * 1024 * 1024,
};

pub const Recipe = struct {
    memory_token_id: i32 = 151669,
    memory_rows: usize = 16,
    system_prompt: []const u8 = default_system_prompt,
};

pub const PreparedText = struct {
    allocator: A,
    rendered: []u8,
    input_ids: []i64,
    memory_start: usize,
    recipe: Recipe,

    pub fn deinit(self: *PreparedText) void {
        self.allocator.free(self.rendered);
        self.allocator.free(self.input_ids);
        self.* = undefined;
    }
};

/// Ordinary, template-framed text has no virtual slots or reader tensors.
/// Keep its type distinct so a plain reservation cannot authorize injection.
pub const PreparedPlainText = struct {
    allocator: A,
    rendered: []u8,
    input_ids: []i64,
    mode: PlainMode = .plain,

    pub fn deinit(self: *PreparedPlainText) void {
        self.allocator.free(self.rendered);
        self.allocator.free(self.input_ids);
        self.* = undefined;
    }
};

/// Internal diagnostic protocol; it is never a public sampler override.
pub const PlainMode = enum { plain, bounded_thinking };
pub const think_open_id: i32 = 151667;
pub const think_close_id: i32 = 151668;
pub const ThinkingMetadata = struct {
    raw_tokens: usize = 0,
    reasoning_body_tokens: usize = 0,
    opening_count: usize = 0,
    closing_count: usize = 0,
    forced_format_tokens: usize = 0,
    first_close_position: ?usize = null,
    still_open: bool = false,
};

/// Counts exactly the intervened trajectory, including the private format
/// closure. The only token this policy can select instead of argmax is </think>.
pub const ThinkingState = struct {
    metadata: ThinkingMetadata = .{},

    pub fn select(self: *ThinkingState, predicted: i64, control: Control) !i64 {
        try control.check();
        if (self.metadata.still_open and self.metadata.reasoning_body_tokens >= 64) {
            if (predicted != think_close_id) self.metadata.forced_format_tokens += 1;
            return think_close_id;
        }
        return predicted;
    }

    pub fn observe(self: *ThinkingState, chosen: i64, eos: bool) void {
        const position = self.metadata.raw_tokens;
        self.metadata.raw_tokens += 1;
        if (chosen == think_open_id) {
            self.metadata.opening_count += 1;
            self.metadata.still_open = true;
        } else if (chosen == think_close_id) {
            self.metadata.closing_count += 1;
            if (self.metadata.first_close_position == null) self.metadata.first_close_position = position;
            self.metadata.still_open = false;
        } else if (self.metadata.still_open and !eos) {
            self.metadata.reasoning_body_tokens += 1;
        }
    }
};

fn plainLimits(limits: Limits) Limits {
    var bounded = limits;
    bounded.max_prompt_bytes = @min(bounded.max_prompt_bytes, 128 * 1024);
    bounded.max_prompt_tokens = @min(bounded.max_prompt_tokens, 1024);
    bounded.max_new_tokens = @min(bounded.max_new_tokens, 128);
    return bounded;
}

pub fn preparePlainText(a: A, tokenizer: tok.Tokenizer, template: *const generation.ChatTemplate, config: gpt_model.Config, system: []const u8, user: []const u8, limits: Limits, control: Control) !PreparedPlainText {
    return preparePlainTextWithMode(a, tokenizer, template, config, system, user, limits, control, .plain);
}

pub fn preparePlainTextWithMode(a: A, tokenizer: tok.Tokenizer, template: *const generation.ChatTemplate, config: gpt_model.Config, system: []const u8, user: []const u8, limits: Limits, control: Control, mode: PlainMode) !PreparedPlainText {
    try control.check();
    try validateConfig(config);
    const caps = plainLimits(limits);
    if (user.len == 0 or try add(system.len, user.len) > caps.max_prompt_bytes) return error.MemorillaPromptBudgetExceeded;
    _ = std.unicode.Utf8View.init(system) catch return error.InvalidMemorillaPromptRecipe;
    _ = std.unicode.Utf8View.init(user) catch return error.InvalidMemorillaPromptRecipe;
    if (std.mem.indexOfScalar(u8, system, 0) != null or std.mem.indexOfScalar(u8, user, 0) != null)
        return error.InvalidMemorillaPromptRecipe;
    const rendered = try template.applyWithOptions(a, &.{
        .{ .role = "system", .content = system },
        .{ .role = "user", .content = user },
    }, .{ .add_generation_prompt = true, .enable_thinking = mode == .bounded_thinking });
    errdefer a.free(rendered);
    if (mode == .bounded_thinking) {
        // The pinned HF true-thinking template leaves the opener to the model.
        if (!std.mem.endsWith(u8, rendered, "<|im_start|>assistant\n")) return error.InvalidMemorillaPromptRecipe;
        inline for (.{ .{ "<think>", think_open_id }, .{ "</think>", think_close_id } }) |entry| {
            var tag: std.ArrayListUnmanaged(i32) = .empty;
            defer tag.deinit(a);
            try tokenizer.encodeInto(a, entry[0], &tag);
            if (tag.items.len != 1 or tag.items[0] != entry[1] or tag.items[0] >= config.vocab_size) return error.InvalidMemorillaTokenizer;
        }
    }
    if (rendered.len > caps.max_prompt_bytes) return error.MemorillaPromptBudgetExceeded;
    var ids: std.ArrayListUnmanaged(i32) = .empty;
    defer ids.deinit(a);
    try control.check();
    try tokenizer.encodeInto(a, rendered, &ids);
    if (ids.items.len == 0 or ids.items.len > caps.max_prompt_tokens or ids.items.len > config.max_position_embeddings)
        return error.MemorillaPromptBudgetExceeded;
    const wide = try a.alloc(i64, ids.items.len);
    errdefer a.free(wide);
    for (ids.items, wide) |id, *out| {
        // No virtual-added-token encoding or vocabulary mutation in plain mode.
        if (id < 0 or id >= config.vocab_size or id >= tokenizer.vocabSize()) return error.InvalidMemorillaTokenizer;
        out.* = id;
    }
    try control.check();
    return .{ .allocator = a, .rendered = rendered, .input_ids = wide, .mode = mode };
}

/// Split the virtual added token before ordinary BPE, just as HF's added-token
/// matcher does. Neither the tokenizer nor the decoder vocabulary is mutated.
pub fn prepareText(a: A, tokenizer: tok.Tokenizer, template: *const generation.ChatTemplate, config: gpt_model.Config, canonical_question: []const u8, recipe: Recipe, limits: Limits, control: Control) !PreparedText {
    try control.check();
    try validateConfig(config);
    if (recipe.memory_rows == 0 or recipe.memory_rows > limits.max_memory_rows or
        recipe.memory_token_id < 0 or recipe.memory_token_id >= config.vocab_size or
        tokenizer.vocabSize() != @as(usize, @intCast(recipe.memory_token_id))) return error.InvalidMemorillaPromptRecipe;
    if (!std.mem.eql(u8, canonical_question, try qualification.canonicalQuestion(canonical_question))) return error.NonCanonicalMemorillaQuestion;
    if (std.mem.indexOf(u8, canonical_question, marker) != null or std.mem.indexOf(u8, recipe.system_prompt, marker) != null)
        return error.MemorillaPlaceholderInUserText;
    const minimum_bytes = try add(try add(recipe.system_prompt.len, canonical_question.len), try mul(recipe.memory_rows, marker.len));
    if (minimum_bytes > limits.max_prompt_bytes) return error.MemorillaPromptBudgetExceeded;
    var system: std.ArrayListUnmanaged(u8) = .empty;
    defer system.deinit(a);
    try system.appendSlice(a, recipe.system_prompt);
    try system.appendSlice(a, "\nMemory");
    for (0..recipe.memory_rows) |_| try system.appendSlice(a, marker);
    try system.appendSlice(a, "Memory");
    const rendered = try template.applyWithOptions(a, &.{
        .{ .role = "system", .content = system.items },
        .{ .role = "user", .content = canonical_question },
    }, .{ .add_generation_prompt = true, .enable_thinking = false });
    errdefer a.free(rendered);
    if (rendered.len > limits.max_prompt_bytes) return error.MemorillaPromptBudgetExceeded;
    var ids: std.ArrayListUnmanaged(i32) = .empty;
    defer ids.deinit(a);
    var offset: usize = 0;
    var found: usize = 0;
    var memory_start: usize = 0;
    while (std.mem.indexOfPos(u8, rendered, offset, marker)) |position| {
        try control.check();
        const before = ids.items.len;
        try tokenizer.encodeInto(a, rendered[offset..position], &ids);
        for (ids.items[before..]) |id| if (id < 0 or id >= recipe.memory_token_id) return error.InvalidMemorillaTokenizer;
        if (found == 0) memory_start = ids.items.len else if (ids.items.len != memory_start + found) return error.NonContiguousMemorillaSlots;
        try ids.append(a, recipe.memory_token_id);
        found += 1;
        offset = position + marker.len;
        if (found > recipe.memory_rows or ids.items.len > limits.max_prompt_tokens) return error.MemorillaPromptBudgetExceeded;
    }
    const tail = ids.items.len;
    try tokenizer.encodeInto(a, rendered[offset..], &ids);
    for (ids.items[tail..]) |id| if (id < 0 or id >= recipe.memory_token_id) return error.InvalidMemorillaTokenizer;
    if (found != recipe.memory_rows or ids.items.len == 0) return error.InvalidMemorillaPromptRecipe;
    if (ids.items.len > limits.max_prompt_tokens or ids.items.len > config.max_position_embeddings) return error.MemorillaPromptBudgetExceeded;
    const wide = try a.alloc(i64, ids.items.len);
    errdefer a.free(wide);
    for (ids.items, wide) |id, *dest| dest.* = id;
    try control.check();
    return .{ .allocator = a, .rendered = rendered, .input_ids = wide, .memory_start = memory_start, .recipe = recipe };
}

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.MemorillaResourceOverflow;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.MemorillaResourceOverflow;
}

fn validateConfig(config: gpt_model.Config) !void {
    if (!gpt.denseQwen3PrefillEligible(config) or config.tokenEmbeddingScale() != 1 or config.final_logit_softcapping != 0)
        return error.UnsupportedMemorillaDecoder;
}

pub const ResourcePlan = struct {
    mode: enum { memory, plain } = .memory,
    prompt_tokens: usize,
    memory_rows: usize,
    max_new_tokens: usize,
    hidden_size: usize,
    layers: usize,
    heads: usize,
    kv_heads: usize,
    head_dim: usize,
    intermediate: usize,
    vocab: usize,
    backend: runtime.kv.pool.BackendKind,
    host: memory.Estimate,
    decoder: memory.Estimate,
    composition_bytes: usize,

    /// Resident weights and cold-load conversion are deliberately separate.
    /// The managed owner adds those leases to this exact-prompt request plan.
    pub fn create(config: gpt_model.Config, backend: runtime.kv.pool.BackendKind, prompt_tokens: usize, prompt_bytes: usize, memory_rows: usize, max_new_tokens: usize, limits: Limits) !ResourcePlan {
        if (memory_rows == 0) return error.MemorillaPromptBudgetExceeded;
        return createInternal(config, backend, prompt_tokens, prompt_bytes, memory_rows, max_new_tokens, limits, .memory);
    }

    pub fn createPlain(config: gpt_model.Config, backend: runtime.kv.pool.BackendKind, prompt_tokens: usize, prompt_bytes: usize, max_new_tokens: usize, limits: Limits) !ResourcePlan {
        return createInternal(config, backend, prompt_tokens, prompt_bytes, 0, max_new_tokens, plainLimits(limits), .plain);
    }

    fn createInternal(config: gpt_model.Config, backend: runtime.kv.pool.BackendKind, prompt_tokens: usize, prompt_bytes: usize, memory_rows: usize, max_new_tokens: usize, limits: Limits, mode: @FieldType(ResourcePlan, "mode")) !ResourcePlan {
        try validateConfig(config);
        if (backend != .cuda and backend != .native) return error.UnsupportedMemorillaBackend;
        if (prompt_tokens == 0 or prompt_tokens > limits.max_prompt_tokens or prompt_bytes > limits.max_prompt_bytes or
            memory_rows > limits.max_memory_rows or max_new_tokens == 0 or max_new_tokens > limits.max_new_tokens or
            try add(prompt_tokens, max_new_tokens) > config.max_position_embeddings) return error.MemorillaPromptBudgetExceeded;
        var estimate = try memory.estimateGptGeneration(backend, .f32, config, prompt_tokens, max_new_tokens, prompt_tokens);
        // F32 CUDA storage doubles capacity and retires old buffers through
        // the shared compute cache. Reserve capacity plus old/new overlap.
        estimate.kv_bytes = try mul(estimate.kv_bytes, 4);
        const row_bytes = try mul(config.hidden_size, @sizeOf(f32));
        const composition = if (mode == .memory)
            try mul(try add(try mul(prompt_tokens, 3), try mul(memory_rows, 3)), row_bytes)
        else
            try mul(prompt_tokens, row_bytes);
        // Cover deferred per-layer FFN temporaries independently of the
        // ordinary decoder's reusable activation estimate.
        const ffn = try mul(try mul(try mul(prompt_tokens, config.intermediate_size), config.num_hidden_layers), @sizeOf(f32));
        estimate.scratch_bytes = try add(estimate.scratch_bytes, try add(composition, ffn));
        // Include simultaneous old/new logits, rounded pinned download storage
        // and bounded host descriptor/sequence metadata, all physically freed.
        const host_bytes = try add(try add(try mul(prompt_bytes, 16), try mul(prompt_tokens, 32)), try add(try add(4 * 1024 * 1024, try mul(config.vocab_size, 8 * @sizeOf(f32))), try add(limits.max_output_bytes, try mul(try add(prompt_tokens, max_new_tokens), 256))));
        const decoder_bytes = try add(estimate.kv_bytes, estimate.scratch_bytes);
        const total_host_bytes = try add(host_bytes, if (backend == .native) decoder_bytes else 0);
        if (estimate.kv_bytes > limits.max_kv_bytes or
            (backend == .cuda and decoder_bytes > limits.max_backend_bytes) or total_host_bytes > limits.max_host_bytes)
            return error.MemorillaResourceBudgetExceeded;
        return .{
            .mode = mode,
            .prompt_tokens = prompt_tokens,
            .memory_rows = memory_rows,
            .max_new_tokens = max_new_tokens,
            .hidden_size = config.hidden_size,
            .layers = config.num_hidden_layers,
            .heads = config.num_attention_heads,
            .kv_heads = config.effectiveKVHeads(),
            .head_dim = config.headDim(),
            .intermediate = config.intermediate_size,
            .vocab = config.vocab_size,
            .backend = backend,
            .host = .{ .prompt_tokens = prompt_tokens, .retained_tokens = prompt_tokens + max_new_tokens, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = host_bytes, .scratch_tier = .host },
            .decoder = estimate,
            .composition_bytes = composition,
        };
    }

    pub fn reserve(self: ResourcePlan, budget: *memory.RunBudget) !Reservation {
        try budget.reserveEstimate(self.decoder);
        errdefer budget.releaseEstimate(self.decoder);
        try budget.reserveEstimate(self.host);
        return .{ .plan = self, .budget = budget };
    }
};

pub const Reservation = struct {
    plan: ResourcePlan,
    budget: *memory.RunBudget,
    active: bool = true,
    in_use: std.atomic.Value(bool) = .init(false),
    pub fn deinit(self: *Reservation) void {
        if (!self.active) return;
        if (self.in_use.load(.acquire)) @panic("cannot refund an active Memorilla request reservation");
        self.budget.releaseEstimate(self.plan.host);
        self.budget.releaseEstimate(self.plan.decoder);
        self.active = false;
    }
};

pub const MemoryRows = struct {
    backend: *const CB,
    tensor: CT,
};
pub const PhysicalResources = struct {
    baseline: usize,
    held_transient_bytes: usize,
    limit: usize,
    live: usize,
    /// Lifetime DeviceBuffer high-water; never reset or presented as a
    /// request-local measurement after a larger earlier request.
    lifetime_peak: usize,
};
pub const Trace = struct {
    context: *anyopaque,
    input_embeddings: ?*const fn (*anyopaque, *const CB, CT) anyerror!void = null,
    /// step=0 is prefill; later values follow each teacher-forced/decode token.
    logits: ?*const fn (*anyopaque, usize, []const f32) anyerror!void = null,
    physical_resources: ?*const fn (*anyopaque, PhysicalResources) anyerror!void = null,
    sampled: ?*const fn (*anyopaque, usize, i64, i64, bool) anyerror!void = null,
};
pub const Request = struct {
    max_new_tokens: usize = 64,
    ignore_eos: bool = false,
    /// Internal numerical diagnostic; public requests must leave this null.
    teacher_forced_ids: ?[]const i64 = null,
    trace: ?Trace = null,
    mode: PlainMode = .plain,
    thinking_metadata: ?*ThinkingMetadata = null,
};
pub const Decoder = struct {
    backend: *const CB,
    config: gpt_model.Config,
    tokenizer: tok.Tokenizer,
    execution_mutex: *std.atomic.Mutex,
    execution_lane: ?*control_mod.ExecutionLane = null,
    policy: enum { cuda_f32, native_f32_reference } = .cuda_f32,

    pub fn fromQualifiedSession(session: @import("../backends/session.zig").Session, backend: *const CB, tokenizer: tok.Tokenizer, mutex: *std.atomic.Mutex) !Decoder {
        const factory = @import("../architectures/session_factory.zig");
        try factory.requireMemorillaBackend(session, .qwen3_decoder_f32_kv_f32_v1, backend);
        const config = try factory.requireMemorillaDecoderF32(session);
        return .{ .backend = backend, .config = config, .tokenizer = tokenizer, .execution_mutex = mutex };
    }

    fn validate(self: Decoder) !void {
        try validateConfig(self.config);
        if (self.policy == .native_f32_reference) {
            if (self.backend.kind() != .native) return error.UnsupportedMemorillaBackend;
            return;
        }
        if (self.backend.kind() != .cuda) return error.UnsupportedMemorillaBackend;
        if (comptime build_options.enable_cuda) {
            const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(self.backend.ptr));
            if (!compute.strict_f32_weights or compute.lazy_host_store != null or compute.capability_profile != .qwen3_generation or compute.capture_gates.persistent_replay or
                platform.env.getenv("ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY") != null or
                platform.env.getenvBool("ANTFLY_GEMMA4_MTP_UNSAFE_TARGET_REPLAY") or
                platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY") != null)
                return error.UnqualifiedMemorillaDecoderPrecision;
            var weights = compute.resident_weights.valueIterator();
            while (weights.next()) |weight| if (weight.dtype != .f32 or weight.quant_type != null) return error.UnqualifiedMemorillaDecoderPrecision;
        } else return error.UnsupportedMemorillaBackend;
    }
};

fn compose(cb: *const CB, a: A, config: gpt_model.Config, text: *const PreparedText, rows: MemoryRows) !CT {
    if (try rows.backend.tensorDType(rows.tensor) != .f32) return error.InvalidMemorillaMemoryRows;
    const shape = try rows.backend.tensorShape(rows.tensor, a);
    defer a.free(shape);
    if (shape.len != 2 or shape[0] != text.recipe.memory_rows or shape[1] != config.hidden_size) return error.InvalidMemorillaMemoryRows;
    const imported = if (cb.kind() == .cuda and rows.backend.kind() == .cuda)
        (try cb.copyTensorFromBackend(rows.backend, rows.tensor)) orelse return error.UnsupportedMemorillaDeviceTransfer
    else if (cb.kind() == .native and rows.backend.kind() == .native) blk: {
        const values = try rows.backend.toFloat32(rows.tensor, a);
        defer a.free(values);
        const imported_shape = [_]i32{ @intCast(text.recipe.memory_rows), @intCast(config.hidden_size) };
        break :blk try cb.fromFloat32Shape(values, &imported_shape);
    } else return error.UnsupportedMemorillaDeviceTransfer;
    defer cb.free(imported);
    const embedding_weight = try gpt.getEmbeddingWeight(cb, config);
    defer cb.free(embedding_weight);
    const base = try cb.embeddingLookup(embedding_weight, text.input_ids, text.input_ids.len, config.hidden_size);
    defer cb.free(base);
    const original_memory = try cb.sliceRows2D(a, base, text.memory_start, text.recipe.memory_rows, config.hidden_size);
    defer cb.free(original_memory);
    const delta = try cb.primSubtract(imported, original_memory);
    defer cb.free(delta);
    // Preserve upstream scatter_add(delta), including its rounding.
    const injected = try cb.add(original_memory, delta);
    defer cb.free(injected);
    const prefix = try cb.sliceRows2D(a, base, 0, text.memory_start, config.hidden_size);
    defer cb.free(prefix);
    const before_tail = try cb.concatRows2D(a, prefix, injected, text.memory_start, text.recipe.memory_rows, config.hidden_size);
    defer cb.free(before_tail);
    const end = text.memory_start + text.recipe.memory_rows;
    const suffix = try cb.sliceRows2D(a, base, end, text.input_ids.len - end, config.hidden_size);
    defer cb.free(suffix);
    return cb.concatRows2D(a, before_tail, suffix, end, text.input_ids.len - end, config.hidden_size);
}

fn argmax(values: []const f32) !i64 {
    if (values.len == 0) return error.InvalidMemorillaLogits;
    var best: usize = 0;
    for (values, 0..) |value, i| {
        if (!std.math.isFinite(value)) return error.NonFiniteMemorillaLogits;
        if (value > values[best]) best = i;
    }
    return @intCast(best);
}

/// The caller retains both model leases and memory rows until return. The
/// reservation must have been acquired before model construction. Every call
/// owns a new sequence/storage runtime; there is no reusable prompt/KV cache.
pub fn generate(a: A, decoder: Decoder, text: *const PreparedText, rows: MemoryRows, request: Request, reservation: *Reservation, limits: Limits, control: Control) !generation.GenerationResult {
    return generateInternal(a, decoder, .{ .memory = .{ .text = text, .rows = rows } }, request, reservation, limits, control);
}

pub fn generatePlain(a: A, decoder: Decoder, text: *const PreparedPlainText, request: Request, reservation: *Reservation, limits: Limits, control: Control) !generation.GenerationResult {
    return generateInternal(a, decoder, .{ .plain = text }, request, reservation, plainLimits(limits), control);
}

const Prompt = union(enum) {
    memory: struct { text: *const PreparedText, rows: MemoryRows },
    plain: *const PreparedPlainText,

    fn ids(self: Prompt) []const i64 {
        return switch (self) {
            .memory => |input| input.text.input_ids,
            .plain => |text| text.input_ids,
        };
    }

    fn memoryRows(self: Prompt) usize {
        return switch (self) {
            .memory => |input| input.text.recipe.memory_rows,
            .plain => 0,
        };
    }
};

fn generateInternal(a: A, decoder: Decoder, prompt: Prompt, request: Request, reservation: *Reservation, limits: Limits, control: Control) !generation.GenerationResult {
    try control.check();
    try decoder.validate();
    const input_ids = prompt.ids();
    if (reservation.in_use.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return error.MemorillaReservationBusy;
    defer reservation.in_use.store(false, .release);
    if (!reservation.active or reservation.plan.prompt_tokens != input_ids.len or reservation.plan.memory_rows != prompt.memoryRows() or
        reservation.plan.mode != (if (prompt == .memory) @as(@FieldType(ResourcePlan, "mode"), .memory) else .plain) or
        reservation.plan.hidden_size != decoder.config.hidden_size or reservation.plan.max_new_tokens != request.max_new_tokens or
        reservation.plan.layers != decoder.config.num_hidden_layers or reservation.plan.heads != decoder.config.num_attention_heads or
        reservation.plan.kv_heads != decoder.config.effectiveKVHeads() or reservation.plan.head_dim != decoder.config.headDim() or
        reservation.plan.intermediate != decoder.config.intermediate_size or reservation.plan.vocab != decoder.config.vocab_size or
        reservation.plan.backend != (if (decoder.backend.kind() == .cuda) runtime.kv.pool.BackendKind.cuda else .native) or
        request.max_new_tokens == 0 or request.max_new_tokens > limits.max_new_tokens) return error.InvalidMemorillaReservation;
    switch (prompt) {
        .memory => |input| {
            if (request.mode != .plain or request.thinking_metadata != null) return error.InvalidMemorillaReservation;
            const text = input.text;
            if (text.memory_start > input_ids.len or text.recipe.memory_rows > input_ids.len - text.memory_start) return error.InvalidMemorillaPromptRecipe;
            for (input_ids, 0..) |id, position| {
                if (id < 0 or id >= decoder.config.vocab_size or
                    (id == text.recipe.memory_token_id) != (position >= text.memory_start and position < text.memory_start + text.recipe.memory_rows))
                    return error.InvalidMemorillaPromptRecipe;
            }
        },
        .plain => |text| {
            if (text.mode != request.mode) return error.InvalidMemorillaReservation;
            if (request.mode == .bounded_thinking and (request.teacher_forced_ids != null or request.ignore_eos or request.max_new_tokens > 128))
                return error.InvalidMemorillaForcedTokens;
            for (input_ids) |id| if (id < 0 or id >= decoder.config.vocab_size or id >= decoder.tokenizer.vocabSize())
                return error.InvalidMemorillaPromptRecipe;
        },
    }
    if (request.teacher_forced_ids) |ids| {
        if (ids.len != request.max_new_tokens) return error.InvalidMemorillaForcedTokens;
        for (ids) |id| if (id < 0 or id >= decoder.config.vocab_size) return error.InvalidMemorillaForcedTokens;
    }
    var cb = decoder.backend.*;
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda) {
            const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(cb.ptr));
            cb = compute.computeBackendWithStepRunBudget(reservation.budget, @intFromPtr(reservation));
        }
    }
    cb.execution_control = control;
    cb.execution_budget = reservation.budget;
    var scope = try execution.Scope.acquire(control, .{ .mutex = decoder.execution_mutex, .lane = decoder.execution_lane });
    defer scope.deinit();
    var native_guard = try control.enterUninterruptible(.uninterruptible);
    defer native_guard.deinit();
    var physical_scope: ?PhysicalBudgetScope = null;
    defer if (physical_scope) |*held| held.deinit();
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda) {
            const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(cb.ptr));
            // Ordinary generation may have retired request buffers into its
            // cache. Quiesce/trim under this same ownership boundary before
            // narrowing the physical cap. A still-live peer KV owner fails
            // closed here, before this request constructs any device state.
            _ = try compute.releaseMemorillaRequestResources();
            physical_scope = try compute.beginMemorillaDeviceBudget(try add(reservation.plan.decoder.kv_bytes, reservation.plan.decoder.scratch_bytes));
        }
    }
    try scope.bind(cb);
    // Explicitly reset under the held lane/mutex even if a recycled request
    // address matches a previous cooperative owner's identifier.
    try cb.beginRequest();
    var released = false;
    defer if (!released) releaseTransientResources(&cb);
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda) try @import("../architectures/session_factory.zig").requireMemorillaCudaExecution(&cb);
    }
    // Request arenas do not physically reclaim retired full-vocabulary logits.
    // Use a reclaiming owner so the two-logit-buffer host reservation remains
    // valid across long continuations, and result.deinit returns real memory.
    const backing = platform.allocator.reclaimingScratch(a);
    var result = try run(backing, decoder, &cb, prompt, request, limits, control);
    errdefer result.deinit();
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda) try @import("../architectures/session_factory.zig").requireMemorillaCudaExecution(&cb);
    }
    if (comptime build_options.enable_cuda) {
        if (physical_scope) |held| if (request.trace) |trace| if (trace.physical_resources) |record| {
            const snapshot = held.compute.ctx.device_allocations.snapshot();
            try record(trace.context, .{ .baseline = held.baseline, .held_transient_bytes = try add(reservation.plan.decoder.kv_bytes, reservation.plan.decoder.scratch_bytes), .limit = snapshot.limit, .live = snapshot.live, .lifetime_peak = snapshot.peak });
        };
    }
    releaseTransientResources(&cb);
    released = true;
    try control.check();
    return result;
}

fn releaseTransientResources(cb: *const CB) void {
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda) {
            const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(cb.ptr));
            _ = compute.releaseMemorillaRequestResources() catch |err|
                std.debug.panic("Memorilla CUDA cleanup failed; retain admission and restart worker: {s}", .{@errorName(err)});
        }
    }
}

fn run(a: A, decoder: Decoder, cb: *const CB, prompt: Prompt, request: Request, limits: Limits, control: Control) !generation.GenerationResult {
    try control.check();
    const input_ids = prompt.ids();
    var manager = runtime.kv.manager.KvManager.init(a);
    defer manager.deinit();
    const backend: runtime.kv.pool.BackendKind = if (cb.kind() == .cuda) .cuda else .native;
    var pool_config = generation.kvPoolConfig(backend, .f32, decoder.config, false);
    if (backend == .cuda) pool_config.store_cpu_bytes = false;
    const pool = try manager.addPool(pool_config);
    var storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(a, pool_config);
    defer storage.deinit();
    try cb.provisionKvDeviceWriteHook(&storage);
    if (backend == .cuda and storage.device_write_hook == null) return error.UnsupportedMemorillaDeviceKv;
    var state = generation.NativeDecodeState.initPaged(a, &manager, pool, null);
    state.configureForGptConfig(decoder.config);
    state.kv_storage = &storage;
    defer state.deinit();
    const input = switch (prompt) {
        .memory => |injected| try compose(cb, a, decoder.config, injected.text, injected.rows),
        .plain => blk: {
            const weight = try gpt.getEmbeddingWeight(cb, decoder.config);
            defer cb.free(weight);
            break :blk try cb.embeddingLookup(weight, input_ids, input_ids.len, decoder.config.hidden_size);
        },
    };
    var input_owned = true;
    defer if (input_owned) cb.free(input);
    if (request.trace) |trace| if (trace.input_embeddings) |record| try record(trace.context, cb, input);
    try control.check();
    try state.notePrefill(input_ids.len);
    const prefill_context = state.gptDecodeContext(input_ids.len, input_ids.len);
    input_owned = false; // GPT consumes the tensor on success and error.
    var logits = try gpt.forwardLastLogitsLastRowFromEmbeddingsWithLayer0Overrides(cb, a, decoder.config, input, .{}, 1, input_ids.len, &prefill_context, null);
    defer a.free(logits);
    _ = try argmax(logits);
    if (request.trace) |trace| if (trace.logits) |record| try record(trace.context, 0, logits);
    var generated: std.ArrayListUnmanaged(i32) = .empty;
    defer generated.deinit(a);
    try generated.ensureTotalCapacity(a, request.max_new_tokens);
    var reason: []const u8 = "length";
    var thinking: ThinkingState = .{};
    defer {
        if (request.thinking_metadata) |out| out.* = thinking.metadata;
    }
    for (0..request.max_new_tokens) |step| {
        try control.update(.executing, step, request.max_new_tokens);
        const predicted = if (request.teacher_forced_ids) |ids| ids[step] else try argmax(logits);
        const token = if (request.mode == .bounded_thinking) try thinking.select(predicted, control) else predicted;
        generated.appendAssumeCapacity(@intCast(token));
        const eos = decoder.config.isEosToken(@intCast(token));
        if (request.mode == .bounded_thinking) thinking.observe(token, eos);
        if (request.trace) |trace| if (trace.sampled) |record| try record(trace.context, step, predicted, token, predicted != token);
        if (request.teacher_forced_ids == null and !request.ignore_eos and eos) {
            reason = "stop";
            break;
        }
        if (request.teacher_forced_ids == null and step + 1 == request.max_new_tokens) break;
        try state.appendGeneratedToken();
        const total = input_ids.len + step + 1;
        const context = state.gptDecodeContext(total, 1);
        const next = try gpt.forward(cb, a, decoder.config, &.{token}, 1, total, &context);
        a.free(logits);
        logits = next;
        _ = try argmax(logits);
        if (request.trace) |trace| if (trace.logits) |record| try record(trace.context, step + 1, logits);
    }
    try control.check();
    const output = try decoder.tokenizer.decodeText(a, generated.items);
    errdefer a.free(output);
    if (output.len > limits.max_output_bytes) return error.MemorillaOutputBudgetExceeded;
    const ids = try generated.toOwnedSlice(a);
    errdefer a.free(ids);
    try control.check();
    return .{ .text = output, .token_ids = ids, .prompt_tokens = input_ids.len, .tokens_used = ids.len, .finish_reason = reason, .allocator = a };
}

test "memorilla generation resource reservations reject overflow and unwind" {
    const config = gpt_model.Config{ .family = .qwen3, .hidden_size = 32, .intermediate_size = 64, .num_hidden_layers = 2, .num_attention_heads = 4, .num_key_value_heads = 2, .attention_head_dim = 8, .vocab_size = 80, .max_position_embeddings = 128, .norm_type = .rms_norm, .position_encoding = .rope, .activation = .silu };
    const plan = try ResourcePlan.create(config, .native, 12, 64, 2, 4, .{});
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 32 * 1024 * 1024, .combined_limit_bytes = 32 * 1024 * 1024 });
    var held = try plan.reserve(&budget);
    try std.testing.expect(budget.host_kv_bytes > 0 and budget.host_scratch_bytes > 0);
    held.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.host_kv_bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
    try std.testing.expectError(error.MemorillaPromptBudgetExceeded, ResourcePlan.create(config, .native, 125, 64, 2, 4, .{}));
    try std.testing.expectError(error.MemorillaResourceOverflow, mul(std.math.maxInt(usize), 2));
    const cuda_plan = try ResourcePlan.create(config, .cuda, 12, 64, 2, 4, .{});
    // A cap covering scratch alone must not admit additional KV bytes.
    try std.testing.expectError(error.MemorillaResourceBudgetExceeded, ResourcePlan.create(config, .cuda, 12, 64, 2, 4, .{ .max_backend_bytes = cuda_plan.decoder.scratch_bytes }));
}

const TestTokenizer = struct {
    var sentinel: u8 = 0;
    fn encode(_: *anyopaque, a: A, text: []const u8) ![]i32 {
        const out = try a.alloc(i32, text.len);
        for (text, out) |byte, *id| id.* = byte;
        return out;
    }
    fn append(raw: *anyopaque, a: A, text: []const u8, out: *std.ArrayListUnmanaged(i32)) !void {
        const ids = try encode(raw, a, text);
        defer a.free(ids);
        try out.appendSlice(a, ids);
    }
    fn decode(_: *anyopaque, a: A, ids: []const i32) ![]u8 {
        const out = try a.alloc(u8, ids.len);
        for (ids, out) |id, *byte| byte.* = @intCast(@mod(id, 128));
        return out;
    }
    fn unused(_: *anyopaque, _: A, _: []const u8, _: usize) !tok.EncodeResult {
        return error.NotUsed;
    }
    fn unusedGeneration(_: *anyopaque, _: A, _: []const u8, _: usize, _: bool) !tok.EncodeResult {
        return error.NotUsed;
    }
    fn specials(_: *anyopaque) tok.SpecialTokens {
        return .{};
    }
    fn size(_: *anyopaque) usize {
        return 256;
    }
    fn deinit(_: *anyopaque) void {}
    fn handle() tok.Tokenizer {
        return .{ .ptr = &sentinel, .vtable = &.{ .encode = encode, .encodeInto = append, .encodeForModel = unused, .encodeGeneration = unusedGeneration, .decode = decode, .specialTokens = specials, .vocabSize = size, .deinit = deinit } };
    }
};

fn testConfig() gpt_model.Config {
    return .{ .family = .qwen3, .hidden_size = 32, .intermediate_size = 64, .num_hidden_layers = 2, .num_attention_heads = 4, .num_key_value_heads = 2, .attention_head_dim = 8, .vocab_size = 512, .max_position_embeddings = 2048, .norm_type = .rms_norm, .position_encoding = .rope, .activation = .silu };
}

test "memorilla generation virtual added token preserves text and tokenizer identity" {
    const a = std.testing.allocator;
    var template = try generation.ChatTemplate.init(a, "{% for m in messages %}{{ '<|im_start|>' + m.role + '\\n' + m.content + '<|im_end|>\\n' }}{% endfor %}{{ '<|im_start|>assistant\\n<think>\\n\\n</think>\\n\\n' }}", "", "", "", "");
    defer template.deinit();
    const tokenizer = TestTokenizer.handle();
    var prompt = try prepareText(a, tokenizer, &template, testConfig(), "Café?", .{ .memory_token_id = 256, .memory_rows = 2, .system_prompt = "System" }, .{}, .{});
    defer prompt.deinit();
    try std.testing.expectEqualStrings("<|im_start|>system\nSystem\nMemory<|memory|><|memory|>Memory<|im_end|>\n<|im_start|>user\nCafé?<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n", prompt.rendered);
    try std.testing.expectEqual(@as(i64, 256), prompt.input_ids[prompt.memory_start]);
    try std.testing.expectEqual(@as(i64, 256), prompt.input_ids[prompt.memory_start + 1]);
    try std.testing.expectEqual(@as(usize, 256), tokenizer.vocabSize());
    try std.testing.expectEqual(@as(usize, prompt.rendered.len - 2 * marker.len + 2), prompt.input_ids.len);
    try std.testing.expectError(error.NonCanonicalMemorillaQuestion, prepareText(a, tokenizer, &template, testConfig(), "\xc2\xa0Question?\xc2\xa0", .{ .memory_token_id = 256, .memory_rows = 2 }, .{}, .{}));
    try std.testing.expectError(error.MemorillaPromptBudgetExceeded, prepareText(a, tokenizer, &template, testConfig(), "Question?", .{ .memory_token_id = 256, .memory_rows = 2 }, .{ .max_prompt_tokens = 1 }, .{}));
}

test "memorilla generation cancellation rejects before tokenizer model or KV work" {
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    const control = Control{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, prepareText(std.testing.allocator, undefined, undefined, undefined, "Question?", .{}, .{}, control));
    try std.testing.expectError(error.Cancelled, generate(std.testing.allocator, undefined, undefined, undefined, .{}, undefined, .{}, control));
    try std.testing.expectError(error.Cancelled, preparePlainText(std.testing.allocator, undefined, undefined, undefined, undefined, undefined, .{}, control));
    try std.testing.expectError(error.Cancelled, generatePlain(std.testing.allocator, undefined, undefined, .{}, undefined, .{}, control));
}

test "memorilla plain generation frames ordinary text without virtual memory slots" {
    const a = std.testing.allocator;
    var template = try generation.ChatTemplate.init(a, "{% for m in messages %}{{ '<|im_start|>' + m.role + '\\n' + m.content + '<|im_end|>\\n' }}{% endfor %}{{ '<|im_start|>assistant\\n' }}", "", "", "", "");
    defer template.deinit();
    var text = try preparePlainText(a, TestTokenizer.handle(), &template, testConfig(), "Sources", "Café?", .{}, .{});
    defer text.deinit();
    try std.testing.expectEqualStrings("<|im_start|>system\nSources<|im_end|>\n<|im_start|>user\nCafé?<|im_end|>\n<|im_start|>assistant\n", text.rendered);
    try std.testing.expectEqual(text.rendered.len, text.input_ids.len);
    for (text.input_ids, text.rendered) |id, byte| try std.testing.expectEqual(@as(i64, byte), id);
    try std.testing.expectError(error.MemorillaPromptBudgetExceeded, preparePlainText(a, TestTokenizer.handle(), &template, testConfig(), "s", "u", .{ .max_prompt_tokens = 1 }, .{}));
    try std.testing.expectError(error.InvalidMemorillaPromptRecipe, preparePlainText(a, TestTokenizer.handle(), &template, testConfig(), "s", "\xff", .{}, .{}));
}

test "memorilla plain reservations cannot authorize memory injection or change route" {
    const a = std.testing.allocator;
    const config = testConfig();
    try std.testing.expectError(error.MemorillaPromptBudgetExceeded, ResourcePlan.create(config, .native, 2, 2, 0, 1, .{}));
    const plan = try ResourcePlan.createPlain(config, .native, 2, 2, 1, .{});
    try std.testing.expectEqual(@as(usize, 0), plan.memory_rows);
    try std.testing.expectEqual(.plain, plan.mode);
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 32 * 1024 * 1024, .combined_limit_bytes = 32 * 1024 * 1024 });
    var reservation = try plan.reserve(&budget);
    defer reservation.deinit();
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    var mutex: std.atomic.Mutex = .unlocked;
    const decoder = Decoder{ .backend = &backend, .config = config, .tokenizer = TestTokenizer.handle(), .execution_mutex = &mutex, .policy = .native_f32_reference };
    var ids = [_]i64{ 1, 2 };
    var rendered = [_]u8{ 1, 2 };
    const text = PreparedPlainText{ .allocator = a, .input_ids = &ids, .rendered = &rendered };
    reservation.plan.mode = .memory;
    try std.testing.expectError(error.InvalidMemorillaReservation, generatePlain(a, decoder, &text, .{ .max_new_tokens = 1 }, &reservation, .{}, .{}));
    try std.testing.expect(!reservation.in_use.load(.acquire));
    reservation.plan.mode = .plain;
    const memory_text = PreparedText{ .allocator = a, .input_ids = &ids, .rendered = &rendered, .memory_start = 0, .recipe = .{ .memory_rows = 2 } };
    try std.testing.expectError(error.InvalidMemorillaReservation, generate(a, decoder, &memory_text, undefined, .{ .max_new_tokens = 1 }, &reservation, .{}, .{}));
}

test "memorilla bounded thinking closes only after64 actual body tokens" {
    var state: ThinkingState = .{};
    try std.testing.expectEqual(@as(i64, 42), try state.select(42, .{}));
    state.observe(think_open_id, false);
    for (0..64) |_| {
        try std.testing.expectEqual(@as(i64, 42), try state.select(42, .{}));
        state.observe(42, false);
    }
    try std.testing.expectEqual(@as(i64, think_close_id), try state.select(43, .{}));
    state.observe(think_close_id, false);
    try std.testing.expectEqual(@as(i64, 44), try state.select(44, .{}));
    state.observe(44, false);
    state.observe(151645, true);
    try std.testing.expectEqual(@as(usize, 64), state.metadata.reasoning_body_tokens);
    try std.testing.expectEqual(@as(usize, 1), state.metadata.forced_format_tokens);
    try std.testing.expectEqual(@as(?usize, 65), state.metadata.first_close_position);
    try std.testing.expectEqual(@as(usize, 68), state.metadata.raw_tokens);
    try std.testing.expect(!state.metadata.still_open);
}

test "memorilla bounded thinking respects natural close EOS and cancellation" {
    var state: ThinkingState = .{};
    state.observe(think_open_id, false);
    state.observe(42, false);
    try std.testing.expectEqual(@as(i64, think_close_id), try state.select(think_close_id, .{}));
    state.observe(think_close_id, false);
    try std.testing.expectEqual(@as(usize, 0), state.metadata.forced_format_tokens);
    var premature: ThinkingState = .{};
    premature.observe(think_open_id, false);
    premature.observe(151645, true);
    try std.testing.expect(premature.metadata.still_open);
    try std.testing.expectEqual(@as(usize, 0), premature.metadata.reasoning_body_tokens);
    var expired: ThinkingState = .{ .metadata = .{ .still_open = true, .reasoning_body_tokens = 64 } };
    try std.testing.expectError(error.Timeout, expired.select(42, .{ .deadline_ns = 0 }));
    try std.testing.expectEqual(@as(usize, 0), expired.metadata.forced_format_tokens);
    try std.testing.expectEqual(@as(usize, 0), expired.metadata.raw_tokens);
}

test "memorilla bounded thinking natural close at budget is not a forced token" {
    var state: ThinkingState = .{ .metadata = .{ .still_open = true, .reasoning_body_tokens = 64 } };
    try std.testing.expectEqual(@as(i64, think_close_id), try state.select(think_close_id, .{}));
    try std.testing.expectEqual(@as(usize, 0), state.metadata.forced_format_tokens);
}
