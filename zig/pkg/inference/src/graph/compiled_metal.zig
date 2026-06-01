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
const build_options = @import("build_options");
const ml = @import("ml");

const cache_mod = @import("cache.zig");
const compiled_backend = @import("compiled_backend.zig");
const metal_capabilities = @import("metal_capabilities.zig");
const metal_executor = if (build_options.enable_metal) @import("metal_executor.zig") else struct {};
const metal_partition_executor = @import("metal_partition_executor.zig");
const model_runtime = @import("model_runtime.zig");
const multi_executor = @import("multi_executor.zig");
const partition_mod = @import("partition.zig");

const Graph = ml.graph.Graph;
const OpCode = ml.graph.OpCode;

fn supportsForMode(
    _: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) *const fn (op: OpCode) bool {
    return &metal_capabilities.supportsMetalEagerGraph;
}

fn decideForMode(
    _: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) ?*const fn (query: partition_mod.CapabilityQuery) partition_mod.CapabilityDecision {
    return &metal_capabilities.decideMetalEagerGraph;
}

fn shouldAttach(
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) bool {
    if (comptime !build_options.enable_metal) return false;
    if (context.requested_backend != .metal) return false;
    return context.cb.kind() == .metal;
}

fn partitionHasCompute(graph: *const Graph, part: partition_mod.Partition) bool {
    for (part.node_ids) |node_id| {
        const op = graph.node(node_id).op;
        if (op != .parameter and op != .constant) return true;
    }
    return false;
}

fn isMetalPartitionEligible(graph: *const Graph, part: partition_mod.Partition) bool {
    return part.backend == .metal and partitionHasCompute(graph, part);
}

fn hasCompilablePartition(
    graph: *const Graph,
    dpp: *const multi_executor.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) bool {
    if (!compiled_backend.attachmentTargetAllowsPlan(graph, dpp, .metal, context.attachment_target)) return false;
    for (dpp.base.partitions) |part| {
        if (isMetalPartitionEligible(graph, part)) return true;
    }
    return false;
}

fn attachExecutors(
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    graph: *const Graph,
    dpp: *multi_executor.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) !void {
    if (comptime !build_options.enable_metal) return;

    entry.selectCompiledPartitionsBackend(.metal, context.attachment_target);

    if (entry.compiled_partitions_status == .uninitialized) {
        if (!compiled_backend.attachmentTargetAllowsPlan(graph, dpp, .metal, context.attachment_target)) {
            entry.compiled_partitions = null;
            entry.compiled_partitions_status = .unavailable;
            return;
        }

        var compiled = std.ArrayListUnmanaged(cache_mod.CompiledPartition).empty;
        errdefer {
            for (compiled.items) |*cp| cp.executor.deinitExecutor();
            compiled.deinit(allocator);
        }

        for (dpp.base.partitions, 0..) |part, part_idx| {
            if (!isMetalPartitionEligible(graph, part)) continue;

            const exec = try metal_partition_executor.MetalPartitionExecutor.create(
                allocator,
                graph,
                context.cb,
            );
            var transferred = false;
            errdefer if (!transferred) exec.partitionExecutor().deinitExecutor();

            try compiled.append(allocator, .{
                .partition_idx = @intCast(part_idx),
                .executor = exec.partitionExecutor().*,
            });
            transferred = true;
        }

        if (compiled.items.len > 0) {
            entry.compiled_partitions = try compiled.toOwnedSlice(allocator);
            entry.compiled_partitions_status = .ready;
        } else {
            entry.compiled_partitions = null;
            entry.compiled_partitions_status = .unavailable;
        }
    }

    if (entry.compiled_partitions_status == .ready) {
        if (entry.compiled_partitions) |compiled| {
            for (compiled) |*cp| {
                dpp.base.partitions[cp.partition_idx].executor = &cp.executor;
            }
            dpp.base.owns_executors = false;
        }
    }
}

const CachedModelExecutor = struct {
    allocator: std.mem.Allocator,
    executor: model_runtime.ModelExecutor,
};

fn deinitModelExecutorPtr(ctx: *anyopaque) void {
    const cached: *CachedModelExecutor = @ptrCast(@alignCast(ctx));
    cached.executor.deinit();
    cached.allocator.destroy(cached);
}

fn ensureModelExecutor(
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    context: compiled_backend.AttachContext,
) !?*model_runtime.ModelExecutor {
    if (comptime !build_options.enable_metal) return null;
    if (context.attachment_target != .whole_model) return null;

    entry.selectCompiledModelExecutorBackend(.metal, context.attachment_target);
    if (entry.compiled_model_status == .ready) {
        const cached = entry.compiled_model_executor orelse return null;
        const executor: *CachedModelExecutor = @ptrCast(@alignCast(cached.ptr));
        return &executor.executor;
    }
    if (entry.compiled_model_status == .unavailable) return null;

    const session = context.session orelse {
        entry.compiled_model_status = .unavailable;
        return null;
    };
    const gpt_config = context.gpt_config orelse {
        entry.compiled_model_status = .unavailable;
        return null;
    };
    if (!metal_executor.supportsSession(session)) {
        entry.compiled_model_status = .unavailable;
        return null;
    }

    const cached = try allocator.create(CachedModelExecutor);
    errdefer allocator.destroy(cached);
    cached.* = .{
        .allocator = allocator,
        .executor = try metal_executor.createModelExecutor(
            allocator,
            session,
            gpt_config,
            context.kv_dtype,
            context.shared_moe_cache,
        ),
    };
    entry.compiled_model_executor = .{
        .ptr = cached,
        .deinit = &deinitModelExecutorPtr,
    };
    entry.compiled_model_status = .ready;
    return &cached.executor;
}

fn executeModelForward(
    allocator: std.mem.Allocator,
    cache: *cache_mod.GraphCache,
    entry: *cache_mod.CacheEntry,
    graph: *const Graph,
    dpp: *multi_executor.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
    request: model_runtime.ForwardRequest,
) !?[]f32 {
    if (comptime !build_options.enable_metal) return null;
    if (context.attachment_target != .whole_model) return null;
    if (!compiled_backend.attachmentTargetAllowsPlan(graph, dpp, .metal, .whole_model)) return null;

    const executor = try ensureModelExecutor(allocator, entry, context) orelse return null;
    const runtime = try compiled_backend.modelRuntimeForExecutor(
        allocator,
        cache,
        entry,
        .metal,
        context.attachment_target,
        executor,
    );

    var output = switch (request) {
        .prefill => |prefill_request| blk: {
            try runtime.reset();
            break :blk runtime.prefill(allocator, prefill_request) catch |err| switch (err) {
                error.UnsupportedShape,
                error.UnsupportedDecode,
                error.MissingPastKeyValue,
                error.MissingValue,
                => return null,
                else => return err,
            };
        },
        .decode => |decode_request| runtime.decode(allocator, decode_request) catch |err| switch (err) {
            error.UnsupportedShape,
            error.UnsupportedDecode,
            error.MissingPastKeyValue,
            error.MissingValue,
            => return null,
            else => return err,
        },
    };
    return try output.takeHostLogits(allocator);
}

fn executeRuntimeForward(
    allocator: std.mem.Allocator,
    runtime: *model_runtime.ModelRuntime,
    request: model_runtime.ForwardRequest,
) !?[]f32 {
    var output = (try executeRuntimeOutput(allocator, runtime, request)) orelse return null;
    return try output.takeHostLogits(allocator);
}

fn executeRuntimeOutput(
    allocator: std.mem.Allocator,
    runtime: *model_runtime.ModelRuntime,
    request: model_runtime.ForwardRequest,
) !?model_runtime.ModelOutput {
    const output = switch (request) {
        .prefill => |prefill_request| blk: {
            try runtime.reset();
            break :blk runtime.prefill(allocator, prefill_request) catch |err| switch (err) {
                error.UnsupportedShape,
                error.UnsupportedDecode,
                error.MissingPastKeyValue,
                error.MissingValue,
                => return null,
                else => return err,
            };
        },
        .decode => |decode_request| runtime.decode(allocator, decode_request) catch |err| switch (err) {
            error.UnsupportedShape,
            error.UnsupportedDecode,
            error.MissingPastKeyValue,
            error.MissingValue,
            => return null,
            else => return err,
        },
    };
    return output;
}

fn executeRuntimeGreedy(
    allocator: std.mem.Allocator,
    runtime: *model_runtime.ModelRuntime,
    request: model_runtime.ForwardRequest,
    vocab_size: usize,
) !?i64 {
    return switch (request) {
        .prefill => blk: {
            var output = (try executeRuntimeOutput(allocator, runtime, request)) orelse return null;
            defer output.deinit(allocator);
            break :blk try output.greedyToken(allocator, vocab_size);
        },
        .decode => |decode_request| blk: {
            const greedy = runtime.decodeGreedy(allocator, decode_request) catch |err| switch (err) {
                error.UnsupportedShape,
                error.UnsupportedDecode,
                error.UnsupportedGreedyDecode,
                error.MissingPastKeyValue,
                error.MissingValue,
                => return null,
                else => return err,
            };
            break :blk greedy.token_id;
        },
    };
}

fn executeModelForwardDirect(
    allocator: std.mem.Allocator,
    cache: *cache_mod.GraphCache,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
    request: model_runtime.ForwardRequest,
) !?[]f32 {
    if (comptime !build_options.enable_metal) return null;
    if (context.attachment_target != .whole_model) return null;
    if (!shouldAttach(context, .single_device)) return null;

    const session = context.session orelse return null;
    const gpt_config = context.gpt_config orelse return null;
    if (!metal_executor.supportsSession(session)) return null;

    var executor = try metal_executor.createModelExecutor(
        allocator,
        session,
        gpt_config,
        context.kv_dtype,
        context.shared_moe_cache,
    );
    defer executor.deinit();

    const runtime = try compiled_backend.modelRuntimeForSessionExecutor(
        allocator,
        cache,
        .metal,
        context.attachment_target,
        &executor,
    );
    return executeRuntimeForward(allocator, runtime, request);
}

fn executeModelForwardOutputDirect(
    allocator: std.mem.Allocator,
    cache: *cache_mod.GraphCache,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
    request: model_runtime.ForwardRequest,
) !?model_runtime.ModelOutput {
    if (comptime !build_options.enable_metal) return null;
    if (context.attachment_target != .whole_model) return null;
    if (!shouldAttach(context, .single_device)) return null;

    const session = context.session orelse return null;
    const gpt_config = context.gpt_config orelse return null;
    if (!metal_executor.supportsSession(session)) return null;

    var executor = try metal_executor.createModelExecutor(
        allocator,
        session,
        gpt_config,
        context.kv_dtype,
        context.shared_moe_cache,
    );
    defer executor.deinit();

    const runtime = try compiled_backend.modelRuntimeForSessionExecutor(
        allocator,
        cache,
        .metal,
        context.attachment_target,
        &executor,
    );
    return executeRuntimeOutput(allocator, runtime, request);
}

fn executeModelGreedyDirect(
    allocator: std.mem.Allocator,
    cache: *cache_mod.GraphCache,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
    request: model_runtime.ForwardRequest,
    vocab_size: usize,
) !?i64 {
    if (comptime !build_options.enable_metal) return null;
    if (context.attachment_target != .whole_model) return null;
    if (!shouldAttach(context, .single_device)) return null;

    const session = context.session orelse return null;
    const gpt_config = context.gpt_config orelse return null;
    if (!metal_executor.supportsSession(session)) return null;

    var executor = try metal_executor.createModelExecutor(
        allocator,
        session,
        gpt_config,
        context.kv_dtype,
        context.shared_moe_cache,
    );
    defer executor.deinit();

    const runtime = try compiled_backend.modelRuntimeForSessionExecutor(
        allocator,
        cache,
        .metal,
        context.attachment_target,
        &executor,
    );
    return executeRuntimeGreedy(allocator, runtime, request, vocab_size);
}

fn prepareModelRuntimeDirect(
    allocator: std.mem.Allocator,
    cache: *cache_mod.GraphCache,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
    request: model_runtime.PrepareRequest,
) !bool {
    if (comptime !build_options.enable_metal) return false;
    if (context.attachment_target != .whole_model) return false;
    if (!shouldAttach(context, .single_device)) return false;

    const session = context.session orelse return false;
    const gpt_config = context.gpt_config orelse return false;
    if (!metal_executor.supportsSession(session)) return false;

    var executor = try metal_executor.createModelExecutor(
        allocator,
        session,
        gpt_config,
        context.kv_dtype,
        context.shared_moe_cache,
    );
    defer executor.deinit();

    const runtime = try compiled_backend.modelRuntimeForSessionExecutor(
        allocator,
        cache,
        .metal,
        context.attachment_target,
        &executor,
    );
    return runtime.prepare(allocator, request);
}

pub const backend = compiled_backend.Definition{
    .kind = .metal,
    .priority = 10,
    .model_runtime_strategy = .direct_session,
    .supports_for_mode = &supportsForMode,
    .decide_for_mode = &decideForMode,
    .should_attach = &shouldAttach,
    .has_compilable_partition = &hasCompilablePartition,
    .attach_executors = &attachExecutors,
    .execute_model_forward = &executeModelForward,
    .execute_model_forward_direct = &executeModelForwardDirect,
    .execute_model_forward_output_direct = &executeModelForwardOutputDirect,
    .execute_model_greedy_direct = &executeModelGreedyDirect,
    .prepare_model_runtime_direct = &prepareModelRuntimeDirect,
};
