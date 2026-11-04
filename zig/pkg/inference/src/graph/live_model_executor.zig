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
const backends = @import("../backends/backends.zig");
const cache_mod = @import("cache.zig");
const decode_state_runtime = @import("decode_state_runtime.zig");
const session_factory = @import("../architectures/session_factory.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");
const runtime = @import("../runtime/root.zig");
const model_runtime = @import("model_runtime.zig");
const decoder_forward = @import("../execution/decoder_forward.zig");
const decoder_storage = @import("../execution/decoder_storage.zig");
const session_decoder = @import("session_decoder.zig");

/// Live whole-model executor for native sessions.
///
/// This is the first step toward treating live sessions and offline artifact
/// packages as the same model-level runtime concept. Unlike the older native
/// generation path, this runtime owns decode-state advancement itself: prefill
/// seeds the cache, decode consumes one token and advances the cache for the
/// next step.
const RuntimeContext = struct {
    allocator: std.mem.Allocator,
    cb: ops.ComputeBackend,
    definition: *session_decoder.Definition,
    kv_manager: runtime.kv.manager.KvManager,
    pool_id: runtime.kv.block.KvPoolId,
    decode_runtime: decode_state_runtime.DecodeStateRuntime,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,

    fn init(allocator: std.mem.Allocator, source: *session_decoder.Definition) !*RuntimeContext {
        const definition = try source.acquireForRuntime(allocator);
        errdefer definition.release();
        const session = definition.session;
        const gpt_config = definition.configuration.value;
        const kv_dtype_override = definition.kv_dtype;
        const shared_moe_cache = definition.shared_moe_cache;
        const cb = try session.acquireComputeBackend(allocator, .{});
        errdefer {
            var cb_mut = cb;
            cb_mut.deinit();
        }

        var kv_manager = runtime.kv.manager.KvManager.init(allocator);
        errdefer kv_manager.deinit();

        const backend_kind: runtime.kv.pool.BackendKind = switch (session.backend()) {
            .native => .native,
            .metal => .metal,
            .cuda => .cuda,
            .pjrt => return error.UnexpectedPjrtBackend,
            .wasm => return error.UnexpectedWasmBackend,
        };
        const kv_dtype = kv_dtype_override orelse session_factory.recommendedKvDTypeForSession(session, backend_kind);

        const pool_id = try kv_manager.addPool(decoder_storage.poolPlan(
            backend_kind,
            kv_dtype,
            &gpt_config,
            .{ .layer_layout = .architectural },
        ));

        const ctx = try allocator.create(RuntimeContext);
        errdefer allocator.destroy(ctx);
        ctx.* = .{
            .allocator = allocator,
            .cb = cb,
            .definition = definition,
            .kv_manager = kv_manager,
            .pool_id = pool_id,
            .decode_runtime = undefined,
            .shared_moe_cache = shared_moe_cache,
        };
        ctx.decode_runtime = decode_state_runtime.DecodeStateRuntime.initPaged(
            allocator,
            &ctx.kv_manager,
            pool_id,
            shared_moe_cache,
        );
        ctx.decode_runtime.configureForGptConfig(gpt_config);
        errdefer ctx.decode_runtime.deinit();
        try ctx.decode_runtime.state.ensureQwen35LinearCache(gpt_config);
        return ctx;
    }

    fn resetState(self: *RuntimeContext) !void {
        // Recurrent storage belongs to this immutable model/runtime, not one
        // prefill call. Reset its contents while retaining the same warmed
        // host/device backing; KV and other request state keep their teardown.
        const recurrent = self.decode_runtime.state.qwen35_linear_cache;
        self.decode_runtime.state.qwen35_linear_cache = null;
        self.decode_runtime.deinit();
        self.decode_runtime = decode_state_runtime.DecodeStateRuntime.initPaged(
            self.allocator,
            &self.kv_manager,
            self.pool_id,
            self.shared_moe_cache,
        );
        self.decode_runtime.configureForGptConfig(self.definition.configuration.value);
        self.decode_runtime.state.qwen35_linear_cache = recurrent;
        self.decode_runtime.state.resetQwen35LinearCache();
    }

    pub fn deinit(self: *RuntimeContext) void {
        var cleanup = self.definition.beginCleanup();
        defer cleanup.deinit();
        self.decode_runtime.deinit();
        self.kv_manager.deinit();
        self.cb.deinit();
        self.definition.release();
        self.allocator.destroy(self);
    }

    fn currentTokenCount(self: *const RuntimeContext) usize {
        return self.decode_runtime.currentTokenCount();
    }

    fn kvView(self: *const RuntimeContext) ?decode_state_runtime.KvView {
        return self.decode_runtime.kvView();
    }

    fn notePrefill(self: *RuntimeContext, token_count: usize) !void {
        try self.decode_runtime.notePrefill(token_count);
    }

    fn appendPrefillChunk(self: *RuntimeContext, token_count: usize) !void {
        try self.decode_runtime.appendPrefillChunk(token_count);
    }

    fn appendGeneratedToken(self: *RuntimeContext) !usize {
        return self.decode_runtime.appendGeneratedToken();
    }

    fn appendGeneratedTokens(self: *RuntimeContext, count: usize) !usize {
        return self.decode_runtime.appendGeneratedTokens(count);
    }

    fn truncateGeneratedTokens(self: *RuntimeContext, count: usize) !void {
        try self.decode_runtime.truncateGeneratedTokens(count);
    }

    fn compactKvCache(self: *RuntimeContext, config: runtime.kv.compaction.CompactionConfig) !void {
        try self.decode_runtime.compactKvCache(config);
    }

    fn validateDecodePosition(self: *const RuntimeContext, position: usize) !void {
        try self.decode_runtime.validateDecodePosition(position);
    }

    fn makeDecodeContext(
        self: *RuntimeContext,
        seq_len: usize,
        query_seq_len: usize,
        attention_mode: cache_mod.AttentionMode,
    ) gpt_arch.DecodeContext {
        return self.decode_runtime.makeDecodeContextForMode(seq_len, query_seq_len, attention_mode);
    }

    fn preparePrefill(self: *RuntimeContext, seq_len: usize, query_seq_len: usize, attention_mode: cache_mod.AttentionMode) !gpt_arch.DecodeContext {
        return self.decode_runtime.preparePrefillForMode(seq_len, query_seq_len, attention_mode);
    }

    fn beginDecodeStep(self: *RuntimeContext, position: usize, attention_mode: cache_mod.AttentionMode) !struct {
        seq_len: usize,
        decode_context: gpt_arch.DecodeContext,
    } {
        const step = try self.decode_runtime.beginDecodeStepForMode(position, attention_mode);
        return .{
            .seq_len = step.seq_len,
            .decode_context = step.decode_context,
        };
    }
};

fn acceptsOperationBackend(table: *const model_runtime.ModelRuntime.VTable) bool {
    return table == &runtime_vtable;
}
fn operationBackend(raw: *anyopaque) *const ops.ComputeBackend {
    const self: *RuntimeContext = @ptrCast(@alignCast(raw));
    return &self.cb;
}
const runtime_vtable = model_runtime.ModelRuntime.VTable{
    .operation_backend = operationBackend,
    .accepts_operation_backend = acceptsOperationBackend,
    .capabilities = runtimeCapabilities,
    .prefill = runtimePrefill,
    .decode = runtimeDecode,
    .decode_sample = runtimeDecodeSample,
    .decode_greedy = runtimeDecodeGreedy,
    .deinit = runtimeDeinit,
    .reset = runtimeReset,
};

fn supportsBackend(backend: backends.BackendType) bool {
    return switch (backend) {
        .native, .metal => true,
        else => false,
    };
}

pub fn supportsSession(session: backends.Session) bool {
    return supportsBackend(session.backend());
}

pub fn createModelExecutor(
    allocator: std.mem.Allocator,
    session: backends.Session,
    gpt_config: gpt_mod.Config,
    kv_dtype: ?runtime.kv.pool.KvDType,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
) !model_runtime.ModelExecutor {
    if (!supportsSession(session)) return error.UnsupportedCompileBackend;
    return session_decoder.createExecutor(allocator, session, &gpt_config, kv_dtype, shared_moe_cache, createRuntime);
}

fn createRuntime(definition: *session_decoder.Definition, allocator: std.mem.Allocator) !model_runtime.ModelRuntime {
    return .{ .ptr = try RuntimeContext.init(allocator, definition), .vtable = &runtime_vtable };
}

fn runtimeCapabilities(ctx: *anyopaque) model_runtime.RuntimeCapabilities {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    return .{
        .supports_decode = true,
        .supports_sample_decode = true,
        .supports_greedy_decode = true,
        .state_ownership = .runtime_owned_host_cache,
        .interruption = if (runtime_ctx.cb.kind() == .metal) .uninterruptible else .cooperative,
    };
}

fn runtimeReset(ctx: *anyopaque) !void {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    try runtime_ctx.resetState();
}

fn runtimeDeinit(ctx: *anyopaque) void {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    runtime_ctx.deinit();
}

fn runtimePrefill(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    request: model_runtime.PrefillRequest,
) !model_runtime.ModelOutput {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    try request.check();
    if (request.input_ids.len == 0 or request.query_seq_len == 0) return error.EmptyPrompt;
    if (request.input_ids.len != request.query_seq_len) return error.UnsupportedShape;
    if (request.query_seq_len > request.seq_len) return error.UnsupportedShape;
    const decode_context = try runtime_ctx.preparePrefill(request.seq_len, request.query_seq_len, request.attention_mode);
    try request.check();
    return .{
        .logits = try forwardLastLogits(
            runtime_ctx,
            allocator,
            request.input_ids,
            request.seq_len,
            request.query_seq_len,
            &decode_context,
        ),
    };
}

fn runtimeDecode(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    request: model_runtime.DecodeRequest,
) !model_runtime.ModelOutput {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    try request.check();
    const step = try runtime_ctx.beginDecodeStep(request.position, request.attention_mode);
    try request.check();
    const input_ids = [_]i64{request.token_id};
    return .{ .logits = try forwardLastLogits(runtime_ctx, allocator, input_ids[0..], step.seq_len, 1, &step.decode_context) };
}

fn runtimeDecodeSample(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    request: model_runtime.SampledDecodeRequest,
) !model_runtime.SampledDecodeOutput {
    try request.decode.check();
    if (request.sampling.isPureGreedy()) {
        const greedy = try runtimeDecodeGreedy(ctx, allocator, request.decode);
        return .{ .token_id = greedy.token_id };
    }

    var output = try runtimeDecode(ctx, allocator, request.decode);
    defer output.deinitWithCleanup(allocator, .{ .boundary = request.decode.cleanup_epoch, .operation = request.decode.backend_operation });
    return .{
        .token_id = @intCast(model_runtime.sampleTokenFromLogits(
            allocator,
            try output.hostLogitsWithCleanup(allocator, .{ .boundary = request.decode.cleanup_epoch, .operation = request.decode.backend_operation }),
            request.sampling,
            request.token_history,
        )),
    };
}

fn runtimeDecodeGreedy(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    request: model_runtime.DecodeRequest,
) !model_runtime.GreedyDecodeOutput {
    const runtime_ctx: *RuntimeContext = @ptrCast(@alignCast(ctx));
    try request.check();
    const step = try runtime_ctx.beginDecodeStep(request.position, request.attention_mode);
    try request.check();
    const input_ids = [_]i64{request.token_id};
    return .{
        .token_id = @intCast(try gpt_arch.forwardGreedyLastToken(
            &runtime_ctx.cb,
            allocator,
            runtime_ctx.definition.configuration.value,
            input_ids[0..],
            1,
            step.seq_len,
            &step.decode_context,
        )),
    };
}

fn forwardLastLogits(
    runtime_ctx: *RuntimeContext,
    allocator: std.mem.Allocator,
    input_ids: []const i64,
    seq_len: usize,
    query_seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) ![]f32 {
    std.debug.assert(query_seq_len == decode_context.query_sequence_len);
    var decoder = decoder_forward.Runtime{ .context = .{
        .allocator = allocator,
        .cb = &runtime_ctx.cb,
        .gpt_config = &runtime_ctx.definition.configuration.value,
    } };
    return decoder.forwardLastLogits(input_ids, 1, seq_len, decode_context);
}

test "live model executor supports native and metal backends" {
    try std.testing.expect(supportsBackend(.native));
    try std.testing.expect(supportsBackend(.metal));
    try std.testing.expect(!supportsBackend(.pjrt));
}

test "native whole model recurrent state preserves chunked prefill decode and warm reset backing" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    try directory.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data =
        \\{"model_type":"qwen3_5","tie_word_embeddings":true,"text_config":{"model_type":"qwen3_5_text","hidden_size":4,"intermediate_size":8,"vocab_size":16,"num_hidden_layers":1,"num_attention_heads":1,"num_key_value_heads":1,"head_dim":4,"max_position_embeddings":32,"tie_word_embeddings":true,"rms_norm_eps":0.000001,"full_attention_interval":4,"linear_conv_kernel_dim":4,"linear_key_head_dim":2,"linear_value_head_dim":2,"linear_num_key_heads":1,"linear_num_value_heads":1}}
    });
    const Spec = struct { name: []const u8, shape: []const i64, constant: ?f32 = null };
    const specs = [_]Spec{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 16, 4 } },
        .{ .name = "model.norm.weight", .shape = &.{4}, .constant = 0.125 },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .constant = 0.125 },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .constant = 0.125 },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 } },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 } },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 } },
        .{ .name = "model.layers.0.linear_attn.in_proj_qkv.weight", .shape = &.{ 6, 4 } },
        .{ .name = "model.layers.0.linear_attn.in_proj_z.weight", .shape = &.{ 2, 4 } },
        .{ .name = "model.layers.0.linear_attn.in_proj_a.weight", .shape = &.{ 1, 4 } },
        .{ .name = "model.layers.0.linear_attn.in_proj_b.weight", .shape = &.{ 1, 4 } },
        .{ .name = "model.layers.0.linear_attn.out_proj.weight", .shape = &.{ 4, 2 } },
        .{ .name = "model.layers.0.linear_attn.conv1d.weight", .shape = &.{ 6, 1, 4 }, .constant = 0.25 },
        .{ .name = "model.layers.0.linear_attn.A_log", .shape = &.{1}, .constant = -1 },
        .{ .name = "model.layers.0.linear_attn.dt_bias", .shape = &.{1}, .constant = 0.1 },
        .{ .name = "model.layers.0.linear_attn.norm.weight", .shape = &.{2}, .constant = 1 },
    };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const arena = scratch.allocator();
    var header = std.ArrayListUnmanaged(u8).empty;
    var payload = std.ArrayListUnmanaged(u8).empty;
    try header.append(arena, '{');
    for (specs, 0..) |spec, index| {
        var count: usize = 1;
        for (spec.shape) |dim| count *= @intCast(dim);
        const offset = payload.items.len;
        for (0..count) |i| {
            const value = spec.constant orelse @as(f32, @floatFromInt(@as(i32, @intCast((i * 7 + spec.name.len) % 23)) - 11)) / 32;
            var bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &bytes, @bitCast(value), .little);
            try payload.appendSlice(arena, &bytes);
        }
        const shape = try std.json.Stringify.valueAlloc(arena, spec.shape, .{});
        const entry = try std.fmt.allocPrint(arena, "{s}\"{s}\":{{\"dtype\":\"F32\",\"shape\":{s},\"data_offsets\":[{d},{d}]}}", .{ if (index == 0) "" else ",", spec.name, shape, offset, payload.items.len });
        try header.appendSlice(arena, entry);
    }
    try header.append(arena, '}');
    while (header.items.len % 8 != 0) try header.append(arena, ' ');
    const checkpoint = try arena.alloc(u8, 8 + header.items.len + payload.items.len);
    std.mem.writeInt(u64, checkpoint[0..8], header.items.len, .little);
    @memcpy(checkpoint[8..][0..header.items.len], header.items);
    @memcpy(checkpoint[8 + header.items.len ..], payload.items);
    try directory.dir.writeFile(std.testing.io, .{ .sub_path = "model.safetensors", .data = checkpoint });
    const path = try directory.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    const session = try session_factory.createNativeSession(a, path);
    defer session.close();
    const cfg = session_factory.getGptConfig(session) orelse return error.MissingDecoderConfig;
    var executor = try createModelExecutor(a, session, cfg, .f32, null);
    defer executor.deinit();
    var decoder = try executor.createRuntime(a);
    defer decoder.deinit();
    const ids = [_]i64{ 1, 2, 3, 4 };
    var full = try decoder.prefill(a, .{ .input_ids = &ids, .seq_len = 4, .query_seq_len = 4, .attention_mode = .paged_prefill });
    defer full.deinit(a);
    const expected = try a.dupe(f32, try full.hostLogits(a));
    defer a.free(expected);
    var full_next = try decoder.decode(a, .{ .token_id = 5, .position = 4 });
    defer full_next.deinit(a);
    const expected_next = try a.dupe(f32, try full_next.hostLogits(a));
    defer a.free(expected_next);
    try decoder.reset();
    var first = try decoder.prefill(a, .{ .input_ids = ids[0..2], .seq_len = 2, .query_seq_len = 2, .attention_mode = .paged_prefill });
    defer first.deinit(a);
    var second = try decoder.prefill(a, .{ .input_ids = ids[2..], .seq_len = 4, .query_seq_len = 2, .attention_mode = .paged_prefill });
    defer second.deinit(a);
    for (expected, try second.hostLogits(a)) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.00001);
    var next = try decoder.decode(a, .{ .token_id = 5, .position = 4 });
    defer next.deinit(a);
    for (expected_next, try next.hostLogits(a)) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.00001);
    const context: *RuntimeContext = @ptrCast(@alignCast(decoder.ptr));
    const cache = context.decode_runtime.state.qwen35_linear_cache orelse return error.MissingRetainedRecurrentState;
    const conv = cache.layers[0].conv.ptr;
    const recurrent = cache.layers[0].recurrent.ptr;
    try decoder.reset();
    const reset_cache = context.decode_runtime.state.qwen35_linear_cache.?;
    try std.testing.expectEqual(cache.layers.ptr, reset_cache.layers.ptr);
    try std.testing.expectEqual(conv, reset_cache.layers[0].conv.ptr);
    try std.testing.expectEqual(recurrent, reset_cache.layers[0].recurrent.ptr);
    for (reset_cache.layers[0].conv) |value| try std.testing.expectEqual(@as(f32, 0), value);
    for (reset_cache.layers[0].recurrent) |value| try std.testing.expectEqual(@as(f32, 0), value);
    const Construction = struct {
        fn run(allocator: std.mem.Allocator, source: *const model_runtime.ModelExecutor) !void {
            var instance = try source.createRuntime(allocator);
            defer instance.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Construction.run, .{&executor});
}
