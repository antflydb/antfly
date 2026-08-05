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
const ops = @import("../ops.zig");
const tensor_mod = @import("../../backends/tensor.zig");
const buffer_mod = @import("buffer.zig");
const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");
const execution_plan = @import("execution_plan.zig");
const kernels_mod = @import("kernels.zig");
const cublaslt_mod = @import("cublaslt.zig");
const scratch_mod = @import("scratch.zig");
const weight_source_mod = @import("../../models/weight_source.zig");
const tensor_store_mod = @import("../../models/tensor_store.zig");
const c_file = @import("../../util/c_file.zig");
const native_compute_mod = @import("../native_compute.zig");
const run_memory = @import("../../runtime/tier/memory.zig");
const load_plan = @import("load_plan.zig");
const gguf_tensor_types = @import("../../gguf/tensor_types.zig");
const quant_codec = @import("../../gguf/quant_codec.zig");
const backend_contracts = @import("../../graph/backend_contracts.zig");
const quant_matmul = @import("../../graph/quant_matmul.zig");
const quant_kernel_compiler = @import("../../graph/quant_kernel_compiler.zig");
const kernel_jit = @import("../../graph/kernel_jit.zig");
const quant_kernel_catalog = @import("../../graph/quant_kernel_catalog.zig");
const quant_kernel_op = @import("../../graph/quant_kernel_op.zig");
const operator_plan = @import("../../graph/operator_plan.zig");
const kv_storage_runtime = @import("../../runtime/kv/storage_runtime.zig");
const kv_pool_mod = @import("../../runtime/kv/pool.zig");
const kv_block_mod = @import("../../runtime/kv/block.zig");
const prefetch_mod = @import("../../runtime/tier/prefetch.zig");
const gpt_arch = @import("../../architectures/gpt.zig");
const gemma4_runtime = @import("../../architectures/gemma4_runtime.zig");
const gpt_model = @import("../../models/gpt.zig");
const platform = @import("antfly_platform");
const linalg = @import("inference_linalg");

const CT = ops.CT;

pub const CudaTensor = struct {
    buffer: buffer_mod.DeviceBuffer,
    bf16_mirror: buffer_mod.DeviceBuffer = .{},
    /// Stable page-locked storage used by repeated training-input uploads.
    /// Keeping the source alive lets cuMemcpyHtoDAsync remain asynchronous;
    /// compiled training steps synchronize before the next overwrite.
    training_upload_host: buffer_mod.HostBuffer = .{},
    training_upload_generation: ?u64 = null,
    dtype: tensor_mod.DType,
    shape: []i64,
    elem_count: usize,
    quant_type: ?gguf_tensor_types.TensorType = null,
    tc_quant: ?CudaTensorCoreQuantBuffer = null,
    owns_buffer: bool = true,
    owns_bf16_mirror: bool = true,
    owns_training_upload_host: bool = true,
    owns_shape: bool = true,
    owned_by_tensor: bool = true,
};

pub const CudaTensorCoreQuantLayout = enum {
    q8_0_hmma,
    q4_k_hmma,
    q4_0_hmma,
};

pub const CudaTensorCoreQuantBuffer = struct {
    buffer: buffer_mod.DeviceBuffer,
    layout: CudaTensorCoreQuantLayout,
    row_blocks: usize,
    bytes: usize,
};

pub const CapabilityProfile = enum {
    clipclap,
    bert_encoder,
    deberta_reranker,
    gliner2,
    gliner2_training,
    florence2,
    gemma4,
};

pub const KernelJitRouteScope = kernels_mod.JitRouteScope;

fn jitModelProfile(profile: CapabilityProfile) kernels_mod.JitModelProfile {
    return switch (profile) {
        .clipclap => .clipclap,
        .bert_encoder => .bert_encoder,
        .deberta_reranker => .deberta_reranker,
        .gliner2, .gliner2_training => .gliner2,
        .florence2 => .florence2,
        .gemma4 => .gemma4,
    };
}

fn kernelJitRouteCount(routes: kernels_mod.JitProductionRoutes) usize {
    return @as(usize, @intFromBool(routes.mmv)) +
        @as(usize, @intFromBool(routes.mm)) +
        @as(usize, @intFromBool(routes.pair)) +
        @as(usize, @intFromBool(routes.pair_q8)) +
        @as(usize, @intFromBool(routes.down_q8));
}

fn logKernelJitCompletion(
    config: kernel_jit.Config,
    profile: CapabilityProfile,
    scope: KernelJitRouteScope,
    load_context: kernel_jit.LoadContext,
    kernels: *const kernels_mod.KernelModule,
) void {
    if (!config.mode.compiles()) return;
    const stats = kernels.runtime_jit_stats;
    const pending = kernels.runtime_jit_pending_routes;
    const outcome: []const u8 = if (stats.skipped_dynamic) "skipped_dynamic" else "ready";
    const reason: []const u8 = if (stats.skipped_dynamic) "dynamic_load" else "none";
    std.log.info(
        "cuda_jit_complete backend=cuda outcome={s} reason={s} mode={s} profile={s} load_context={s} scoped_routes={d} conformance_complete={} routes_mmv={} routes_mm={} routes_pair={} routes_pair_q8={} routes_down_q8={} qualified_mmv_shapes={d} qualified_mm_shapes={d} qualified_pair_shapes={d} qualification_cache_hits={d} qualification_cache_misses={d} compiled={d} qualified={d} active={d} pending={d} rejected={d} quarantined={d} sync_elapsed_ms={d} sync_budget_ms={d} budget_reached={} skipped_dynamic={} background_queued=false postpublication_work=false",
        .{
            outcome,
            reason,
            @tagName(config.mode),
            @tagName(profile),
            @tagName(load_context),
            kernelJitRouteCount(scope.production),
            scope.conformance_complete,
            scope.production.mmv,
            scope.production.mm,
            scope.production.pair,
            scope.production.pair_q8,
            scope.production.down_q8,
            kernels.runtime_jit_qualified_scope.observed_shape_count,
            kernels.runtime_jit_qualified_scope.prefill_shape_count,
            kernels.runtime_jit_qualified_scope.pair_shape_count,
            stats.qualification_cache_hits,
            stats.qualification_cache_misses,
            stats.compiled,
            stats.qualified,
            stats.active,
            kernelJitRouteCount(pending),
            stats.rejected,
            stats.quarantined,
            stats.sync_elapsed_ms,
            config.preload_budget_ms,
            stats.budget_reached,
            stats.skipped_dynamic,
        },
    );
}

fn logKernelJitFailure(
    config: kernel_jit.Config,
    profile: CapabilityProfile,
    scope: KernelJitRouteScope,
    load_context: kernel_jit.LoadContext,
    err: anyerror,
) void {
    if (!config.mode.compiles()) return;
    std.log.warn(
        "cuda_jit_complete backend=cuda outcome=failed mode={s} profile={s} load_context={s} scoped_routes={d} conformance_complete={} qualified=0 active=0 pending={d} rejected=0 quarantined=0 sync_elapsed_ms=0 sync_budget_ms={d} budget_reached=false skipped_dynamic=false background_queued=false postpublication_work=false error={s}",
        .{
            @tagName(config.mode),
            @tagName(profile),
            @tagName(load_context),
            kernelJitRouteCount(scope.production),
            scope.conformance_complete,
            kernelJitRouteCount(scope.production),
            config.preload_budget_ms,
            @errorName(err),
        },
    );
}

/// A resident 2-D tensor is not necessarily a linear route. Proven lookup-only
/// tables must not expand the JIT conformance domain merely because their
/// storage is quantized. Output heads and genuinely tied token embeddings are
/// retained because generic logits fallbacks can reach linearNoBias.
fn cudaKernelJitLinearWeightKey(key: []const u8, token_embedding_is_lookup_only: bool) bool {
    if (!std.mem.endsWith(u8, key, ".weight")) return false;
    const token_embedding = isTiedTokenEmbeddingWeightName(key) or
        std.mem.endsWith(u8, key, ".embed_tokens.weight") or
        std.mem.endsWith(u8, key, ".token_embd.weight");
    if (token_embedding and token_embedding_is_lookup_only) return false;
    if (std.mem.indexOf(u8, key, "per_layer_token_embd") != null or
        std.mem.indexOf(u8, key, "embed_tokens_per_layer") != null)
    {
        return false;
    }
    const non_linear_tables = [_][]const u8{
        "embedding",
        "embeddings",
        "position_embeddings",
        "token_type_embeddings",
        "word_embeddings",
        "rope_freqs",
        "layernorm",
        "layer_norm",
        ".norm",
        "wpe.weight",
        "wte.weight",
    };
    for (non_linear_tables) |needle| {
        if (std.mem.indexOf(u8, key, needle) != null) return false;
    }
    return true;
}

test "CUDA runtime JIT scope excludes lookup-only tables but retains reachable heads" {
    try std.testing.expectEqual(kernels_mod.JitModelProfile.gliner2, jitModelProfile(.gliner2_training));
    try std.testing.expect(cudaKernelJitLinearWeightKey(
        "model.layers.0.self_attn.q_proj.weight",
        true,
    ));
    try std.testing.expect(cudaKernelJitLinearWeightKey(
        "model.layers.0.mlp.down_proj.weight",
        true,
    ));
    try std.testing.expect(cudaKernelJitLinearWeightKey("model.embed_tokens.weight", false));
    try std.testing.expect(cudaKernelJitLinearWeightKey("token_embd.weight", false));
    try std.testing.expect(!cudaKernelJitLinearWeightKey("model.embed_tokens.weight", true));
    try std.testing.expect(!cudaKernelJitLinearWeightKey("model.per_layer_input.per_layer_token_embd.weight", false));
    try std.testing.expect(cudaKernelJitLinearWeightKey("lm_head.weight", true));
    try std.testing.expect(cudaKernelJitLinearWeightKey("model.language_model.lm_head.weight", true));
    try std.testing.expect(cudaKernelJitLinearWeightKey("output.weight", true));
    try std.testing.expect(!cudaKernelJitLinearWeightKey("position_embeddings.weight", true));
    try std.testing.expect(!cudaKernelJitLinearWeightKey("model.layers.0.input_layernorm.weight", true));
}

pub fn kernelJitRouteScopeForLoadedWeights(
    profile: CapabilityProfile,
    weights: *const std.StringHashMapUnmanaged(weight_source_mod.LoadedWeight),
) KernelJitRouteScope {
    var builder = kernels_mod.JitRouteScopeBuilder.init(jitModelProfile(profile));
    const has_separate_output_head = blk: {
        var names = weights.iterator();
        while (names.next()) |entry| {
            const key = entry.key_ptr.*;
            if (std.mem.eql(u8, key, "output.weight") or
                std.mem.eql(u8, key, "lm_head.weight") or
                std.mem.endsWith(u8, key, ".lm_head.weight")) break :blk true;
        }
        break :blk false;
    };
    var iterator = weights.iterator();
    while (iterator.next()) |entry| {
        const storage = entry.value_ptr.quantized_storage orelse continue;
        if (!cudaKernelJitLinearWeightKey(entry.key_ptr.*, has_separate_output_head) or
            !isKnownQuantStorage(storage, .Q4_0) or
            cudaDequantizeQuantWeightsOnUpload() or
            cudaShouldDequantizeQ4_0MatrixWeightToBf16OnUpload(entry.key_ptr.*, storage) or
            cudaShouldDequantizeWeightOnUpload(entry.key_ptr.*, storage) or
            storage.shape.len != 2)
        {
            continue;
        }
        const out_dim = std.math.cast(usize, storage.shape[0]) orelse continue;
        const in_dim = std.math.cast(usize, storage.shape[1]) orelse continue;
        builder.observeQ4_0Matrix(
            entry.key_ptr.*,
            out_dim,
            in_dim,
            !cudaQ4WeightBf16MirrorPolicyEnabled(entry.key_ptr.*, storage, true),
        );
    }
    return builder.finish();
}

const CudaDispatchOp = enum {
    linear,
    linear_no_bias,
    linear_gelu,
    linear_add,
    linear_quick_gelu,
    linear_relu,
    linear_pair,
    linear_triple,
    linear_pair_relu,
    linear_pair_inputs,
    tc_pack,
};

const CudaDispatchQuant = enum {
    none,
    q8_0,
    q4_0,
    q4_k,
    q6_k,
    f32,
    f16,
    bf16,
};

const CudaDispatchRoute = enum {
    tc_pack,
    q8_tc_hmma,
    q4_tc_hmma,
    q8_simt,
    q4_simt,
    q6_simt,
    q4_span_simt,
    dense_lt,
    dense_cuda,
    f32_cuda,
};

const CudaDispatchEpilogue = enum {
    none,
    bias,
    bias_gelu,
    bias_add,
    bias_quick_gelu,
    bias_relu,
    pair,
    pair_relu,
    pair_inputs,
};

const CudaDispatchFallback = enum {
    none,
    tc_not_requested,
    tc_no_packed_weight,
    tc_missing_symbol,
    tc_unsupported_shape,
    explicit_simt,
    specialized_span,
};

const cuda_dispatch_route_count = @typeInfo(CudaDispatchRoute).@"enum".fields.len;
const cuda_dispatch_max_entries: usize = 256;
const cuda_q4_route_census_max_entries: usize = 128;

/// Model-level route identity for the Q4 traffic census. Keep attention/GQA
/// distinct even though it has no Q4 weight bytes: a single schema can then
/// correlate the projection traffic with the attention phase that consumes it.
const CudaQ4RouteOp = enum {
    linear,
    lm_head,
    qkv,
    qkv_gqa,
    ffn_pair,
    ffn_down,
    attention_gqa_prefill,
};

const CudaQ4RouteBundle = enum {
    single,
    pair,
    qkv,
    gqa,
};

const CudaQ4RouteEpilogue = enum {
    none,
    gelu_new,
    activation_f32,
    activation_q8_zp,
    activation_ggml_q8_1,
};

const CudaQ4RouteProvider = enum {
    handwritten,
    generated,
    fallback,
};

const CudaQ4KernelLaunch = struct {
    provider: CudaQ4RouteProvider = .handwritten,
    kernel_id: []const u8,
    grid: [3]usize,
    block: [3]usize,
};

const cuda_q4_route_provider_count = @typeInfo(CudaQ4RouteProvider).@"enum".fields.len;

const CudaQ4RouteEntry = struct {
    op: CudaQ4RouteOp,
    rows: usize,
    k: usize,
    n: usize,
    bundle: CudaQ4RouteBundle,
    epilogue: CudaQ4RouteEpilogue,
    provider: CudaQ4RouteProvider,
    kernel_id: []const u8,
    grid: [3]u32,
    block: [3]u32,
    calls: u64 = 0,
    q4_bytes_per_call: u64 = 0,
    weighted_q4_bytes: u64 = 0,

    fn matches(
        self: CudaQ4RouteEntry,
        op: CudaQ4RouteOp,
        rows: usize,
        k: usize,
        n: usize,
        bundle: CudaQ4RouteBundle,
        epilogue: CudaQ4RouteEpilogue,
        provider: CudaQ4RouteProvider,
        kernel_id: []const u8,
        grid: [3]u32,
        block: [3]u32,
    ) bool {
        return self.op == op and self.rows == rows and self.k == k and self.n == n and
            self.bundle == bundle and self.epilogue == epilogue and self.provider == provider and
            std.mem.eql(u8, self.kernel_id, kernel_id) and
            std.mem.eql(u32, &self.grid, &grid) and std.mem.eql(u32, &self.block, &block);
    }
};

fn q4RouteBytesPerCall(rows: usize, k: usize, n: usize, bundle: CudaQ4RouteBundle) u64 {
    if (rows == 0 or k == 0 or n == 0 or k % 32 != 0) return 0;
    const projections: u64 = switch (bundle) {
        .single => 1,
        .pair => 2,
        // For QKV, n is the already-combined Q + K + V output width.
        .qkv => 1,
        .gqa => return 0,
    };
    return @as(u64, @intCast(rows)) *|
        @as(u64, @intCast(n)) *|
        @as(u64, @intCast(k / 32)) *|
        18 *| projections;
}

const CudaDispatchEntry = struct {
    op: CudaDispatchOp,
    quant: CudaDispatchQuant,
    route: CudaDispatchRoute,
    epilogue: CudaDispatchEpilogue,
    fallback: CudaDispatchFallback,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    calls: u64 = 0,
    tc_pack_bytes: u64 = 0,

    fn matches(
        self: CudaDispatchEntry,
        op: CudaDispatchOp,
        quant: CudaDispatchQuant,
        route: CudaDispatchRoute,
        epilogue: CudaDispatchEpilogue,
        fallback: CudaDispatchFallback,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) bool {
        return self.op == op and
            self.quant == quant and
            self.route == route and
            self.epilogue == epilogue and
            self.fallback == fallback and
            self.rows == rows and
            self.in_dim == in_dim and
            self.out_dim == out_dim;
    }
};

const CudaDispatchStats = struct {
    route_counts: [cuda_dispatch_route_count]u64 = [_]u64{0} ** cuda_dispatch_route_count,
    entries: std.ArrayListUnmanaged(CudaDispatchEntry) = .empty,
    dropped_entries: u64 = 0,
    q4_entries: std.ArrayListUnmanaged(CudaQ4RouteEntry) = .empty,
    q4_provider_calls: [cuda_q4_route_provider_count]u64 = [_]u64{0} ** cuda_q4_route_provider_count,
    q4_provider_weighted_bytes: [cuda_q4_route_provider_count]u64 = [_]u64{0} ** cuda_q4_route_provider_count,
    q4_dropped_entries: u64 = 0,

    fn note(
        self: *CudaDispatchStats,
        allocator: std.mem.Allocator,
        op: CudaDispatchOp,
        quant: CudaDispatchQuant,
        route: CudaDispatchRoute,
        epilogue: CudaDispatchEpilogue,
        fallback: CudaDispatchFallback,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        tc_pack_bytes: usize,
    ) void {
        self.route_counts[@intFromEnum(route)] += 1;
        if (!cudaDispatchStatsEnabled()) return;
        for (self.entries.items) |*entry| {
            if (entry.matches(op, quant, route, epilogue, fallback, rows, in_dim, out_dim)) {
                entry.calls += 1;
                entry.tc_pack_bytes += @intCast(tc_pack_bytes);
                return;
            }
        }
        if (self.entries.items.len >= cuda_dispatch_max_entries) {
            self.dropped_entries += 1;
            return;
        }
        self.entries.append(allocator, .{
            .op = op,
            .quant = quant,
            .route = route,
            .epilogue = epilogue,
            .fallback = fallback,
            .rows = rows,
            .in_dim = in_dim,
            .out_dim = out_dim,
            .calls = 1,
            .tc_pack_bytes = @intCast(tc_pack_bytes),
        }) catch {
            self.dropped_entries += 1;
            return;
        };
    }

    fn noteQ4(
        self: *CudaDispatchStats,
        allocator: std.mem.Allocator,
        enabled: bool,
        op: CudaQ4RouteOp,
        rows: usize,
        k: usize,
        n: usize,
        bundle: CudaQ4RouteBundle,
        epilogue: CudaQ4RouteEpilogue,
        provider: CudaQ4RouteProvider,
        kernel_id: []const u8,
        grid: [3]u32,
        block: [3]u32,
    ) void {
        // This census is physical launch traffic, not route-selection intent.
        // Call only after a launch succeeds. `.fallback` identifies an actual
        // fallback kernel that ran; failed candidate attempts belong solely in
        // their dedicated fallback counters.
        if (!enabled) return;
        const bytes_per_call = q4RouteBytesPerCall(rows, k, n, bundle);
        const provider_index = @intFromEnum(provider);
        self.q4_provider_calls[provider_index] +|= 1;
        self.q4_provider_weighted_bytes[provider_index] +|= bytes_per_call;
        for (self.q4_entries.items) |*entry| {
            if (entry.matches(op, rows, k, n, bundle, epilogue, provider, kernel_id, grid, block)) {
                entry.calls +|= 1;
                entry.weighted_q4_bytes +|= bytes_per_call;
                return;
            }
        }
        if (self.q4_entries.items.len >= cuda_q4_route_census_max_entries) {
            self.q4_dropped_entries +|= 1;
            return;
        }
        self.q4_entries.append(allocator, .{
            .op = op,
            .rows = rows,
            .k = k,
            .n = n,
            .bundle = bundle,
            .epilogue = epilogue,
            .provider = provider,
            .kernel_id = kernel_id,
            .grid = grid,
            .block = block,
            .calls = 1,
            .q4_bytes_per_call = bytes_per_call,
            .weighted_q4_bytes = bytes_per_call,
        }) catch {
            self.q4_dropped_entries +|= 1;
        };
    }

    fn printIfEnabled(self: *const CudaDispatchStats, q4_census_enabled: bool) void {
        if (!cudaDispatchStatsEnabled() and !q4_census_enabled) return;
        std.debug.print("ANTFLY_CUDA_DISPATCH_STATS {{\"routes\":{{", .{});
        inline for (@typeInfo(CudaDispatchRoute).@"enum".fields, 0..) |field, i| {
            if (i != 0) std.debug.print(",", .{});
            std.debug.print("\"{s}\":{d}", .{ field.name, self.route_counts[i] });
        }
        std.debug.print("}},\"entries\":[", .{});
        for (self.entries.items, 0..) |entry, i| {
            if (i != 0) std.debug.print(",", .{});
            std.debug.print(
                "{{\"op\":\"{s}\",\"quant\":\"{s}\",\"route\":\"{s}\",\"epilogue\":\"{s}\",\"fallback\":\"{s}\",\"rows\":{d},\"in_dim\":{d},\"out_dim\":{d},\"calls\":{d},\"tc_pack_bytes\":{d}}}",
                .{
                    @tagName(entry.op),
                    @tagName(entry.quant),
                    @tagName(entry.route),
                    @tagName(entry.epilogue),
                    @tagName(entry.fallback),
                    entry.rows,
                    entry.in_dim,
                    entry.out_dim,
                    entry.calls,
                    entry.tc_pack_bytes,
                },
            );
        }
        std.debug.print(
            "],\"dropped_entries\":{d},\"q4_route_census\":{{\"schema_version\":1,\"scope\":\"q4_0_weight_traffic_plus_gqa_phase_markers\",\"providers\":{{",
            .{self.dropped_entries},
        );
        inline for (@typeInfo(CudaQ4RouteProvider).@"enum".fields, 0..) |field, i| {
            if (i != 0) std.debug.print(",", .{});
            std.debug.print(
                "\"{s}\":{{\"calls\":{d},\"weighted_q4_bytes\":{d}}}",
                .{ field.name, self.q4_provider_calls[i], self.q4_provider_weighted_bytes[i] },
            );
        }
        std.debug.print("}},\"entries\":[", .{});
        for (self.q4_entries.items, 0..) |entry, i| {
            if (i != 0) std.debug.print(",", .{});
            std.debug.print(
                "{{\"op\":\"{s}\",\"rows\":{d},\"k\":{d},\"n\":{d},\"bundle\":\"{s}\",\"epilogue\":\"{s}\",\"provider\":\"{s}\",\"kernel_id\":\"{s}\",\"geometry_known\":{s},\"grid\":[{d},{d},{d}],\"block\":[{d},{d},{d}],\"calls\":{d},\"q4_bytes_per_call\":{d},\"weighted_q4_bytes\":{d}}}",
                .{
                    @tagName(entry.op),       entry.rows,               entry.k,                 entry.n,                                      @tagName(entry.bundle),
                    @tagName(entry.epilogue), @tagName(entry.provider), entry.kernel_id,         if (entry.block[0] != 0) "true" else "false", entry.grid[0],
                    entry.grid[1],            entry.grid[2],            entry.block[0],          entry.block[1],                               entry.block[2],
                    entry.calls,              entry.q4_bytes_per_call,  entry.weighted_q4_bytes,
                },
            );
        }
        std.debug.print("] ,\"dropped_entries\":{d}}}}}\n", .{self.q4_dropped_entries});
    }

    fn deinit(self: *CudaDispatchStats, allocator: std.mem.Allocator) void {
        self.entries.deinit(allocator);
        self.q4_entries.deinit(allocator);
        self.* = .{};
    }
};

const DenseStreamEntry = struct {
    const Kind = enum { attention, mlp };

    name: []u8,
    tensor: *CudaTensor,
    byte_len: usize,
    kind: Kind,
    last_access: u64 = 0,
};

const DenseHostRange = struct {
    path: []u8,
    byte_offset: u64,
    byte_len: usize,
    dtype: tensor_mod.DType,
    shape: []i64,

    fn deinit(self: *DenseHostRange, allocator: std.mem.Allocator) void {
        allocator.free(self.path);
        allocator.free(self.shape);
        self.* = .{
            .path = &.{},
            .byte_offset = 0,
            .byte_len = 0,
            .dtype = .f32,
            .shape = &.{},
        };
    }
};

const DenseHostPrefetchEntry = struct {
    name: []u8,
    range: DenseHostRange,
    kind: DenseStreamEntry.Kind,
    priority: u64 = 0,
    pending: bool = false,
    loading: bool = false,
    ready: bool = false,
    failed: bool = false,
    host: []u8 = &.{},
    read_ns: u64 = 0,
    last_access: u64 = 0,

    fn deinit(self: *DenseHostPrefetchEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        self.range.deinit(allocator);
        if (self.host.len != 0) allocator.free(self.host);
        self.* = undefined;
    }
};

const DenseHostPrefetchQueue = prefetch_mod.Queue(*DenseHostPrefetchEntry);

const TempPinnedSlot = struct {
    buffer: buffer_mod.DeviceBuffer = .{},
    requested_len: usize = 0,
    in_use: bool = false,
};

const PinnedScalarUploadRing = struct {
    host: buffer_mod.HostBuffer = .{},
    next_slot: usize = 0,
};

const PinnedUploadArena = struct {
    host: buffer_mod.HostBuffer = .{},
    next_offset: usize = 0,
};

const PinnedScalarDownloadBuffer = struct {
    host: buffer_mod.HostBuffer = .{},
};

const AsyncI32ScalarDownload = struct {
    host: buffer_mod.HostBuffer = .{},
    staging: buffer_mod.DeviceBuffer = .{},
    copy_stream: driver_mod.CUstream = null,
    ready_event: driver_mod.CUevent = null,
    event: driver_mod.CUevent = null,
    in_use: bool = false,
    handle: usize = 0,
};

const CudaDecoderRuntimeLinearSlot = struct {
    weight: CudaTensor,
    bias: ?CudaTensor = null,
    in_dim: usize = 0,
    out_dim: usize = 0,
};

const CudaDecoderRuntimeRmsNormSlot = struct {
    weight: CudaTensor,
    hidden_size: usize = 0,
};

const CudaDecoderRuntimeFamilyState = struct {
    prepared: bool = false,
    reserve_kv_tokens: usize = 0,
    configured_layer_count: usize = 0,
    hidden_size: u32 = 0,
    intermediate_size: u32 = 0,
    num_hidden_layers: u32 = 0,
    num_attention_heads: u32 = 0,
    num_key_value_heads: u32 = 0,
    num_global_key_value_heads: u32 = 0,
    attention_head_dim: u32 = 0,
    global_head_dim: u32 = 0,
    vocab_size: u32 = 0,
    sliding_window: u32 = 0,
    sliding_window_pattern: u32 = 0,
    ple_hidden_size: u32 = 0,
};

pub const RuntimeStats = struct {
    pub const top_transfer_size_count = 8;

    quant_ops: operator_plan.Stats = .{},
    quant_kernel_planned_ops: usize = 0,
    quant_kernel_handwritten_production: usize = 0,
    quant_kernel_generated_production: usize = 0,
    quant_kernel_unsupported_routes: usize = 0,
    quant_kernel_generated_candidates: usize = 0,
    quant_kernel_fallback_generated_artifact_missing: usize = 0,
    quant_kernel_fallback_generated_runtime_not_wired: usize = 0,
    quant_kernel_fallback_unsupported_format: usize = 0,
    quant_kernel_fallback_unsupported_shape: usize = 0,
    quant_kernel_fallback_unsupported_epilogue: usize = 0,
    quant_kernel_fallback_unsupported_backend: usize = 0,
    quant_kernel_fallback_tensor_core_repack_required: usize = 0,
    quant_kernel_fallback_unsupported: usize = 0,
    h2d_bytes: usize = 0,
    d2h_bytes: usize = 0,
    d2d_bytes: usize = 0,
    resident_weight_bytes: usize = 0,
    bf16_mirror_weight_count: usize = 0,
    bf16_mirror_weight_bytes: usize = 0,
    device_allocated_bytes: usize = 0,
    device_alloc_calls: usize = 0,
    device_free_calls: usize = 0,
    temp_buffer_hits: usize = 0,
    temp_buffer_misses: usize = 0,
    temp_buffer_releases: usize = 0,
    temp_buffer_evictions: usize = 0,
    temp_buffer_cached_bytes: usize = 0,
    deferred_free_queued: usize = 0,
    deferred_free_drains: usize = 0,
    deferred_free_forced_drains: usize = 0,
    deferred_free_pending_bytes: usize = 0,
    deferred_free_reclaimed_bytes: usize = 0,
    kernel_launches: usize = 0,
    stream_syncs: usize = 0,
    cross_backend_copies: usize = 0,
    cross_backend_copy_bytes: usize = 0,
    cross_backend_event_records: usize = 0,
    cross_backend_event_waits: usize = 0,
    cross_backend_sync_fallbacks: usize = 0,
    cuda_graph_capture_begins: usize = 0,
    cuda_graph_capture_replays: usize = 0,
    cuda_graph_capture_discards: usize = 0,
    cuda_graph_capture_instantiates: usize = 0,
    cuda_graph_capture_update_successes: usize = 0,
    cuda_graph_capture_update_failures: usize = 0,
    cuda_graph_capture_update_unavailable: usize = 0,
    cuda_graph_capture_scalar_updates: usize = 0,
    cuda_graph_capture_persistent_replays: usize = 0,
    cuda_graph_capture_capacity_skips: usize = 0,
    cuda_temp_arena_plan_observations: usize = 0,
    cuda_temp_arena_plan_activations: usize = 0,
    cuda_temp_arena_plan_invalidations: usize = 0,
    cuda_temp_arena_plan_slots: usize = 0,
    cuda_temp_arena_plan_physical_high_water_bytes: usize = 0,
    cuda_temp_arena_plan_peak_physical_high_water_bytes: usize = 0,
    cuda_temp_arena_plan_admission_budget_bytes: usize = 0,
    cuda_temp_arena_plan_admission_denials: usize = 0,
    upload_syncs: usize = 0,
    pinned_scalar_uploads: usize = 0,
    pinned_scalar_upload_bytes: usize = 0,
    pinned_scalar_upload_fallbacks: usize = 0,
    pinned_scalar_upload_wrap_syncs: usize = 0,
    pinned_scalar_downloads: usize = 0,
    pinned_scalar_download_bytes: usize = 0,
    pinned_scalar_download_fallbacks: usize = 0,
    async_i32_scalar_downloads: usize = 0,
    async_i32_scalar_download_bytes: usize = 0,
    async_i32_scalar_download_finishes: usize = 0,
    async_i32_scalar_download_cancels: usize = 0,
    async_i32_scalar_download_busy_fallbacks: usize = 0,
    download_syncs: usize = 0,
    eval_syncs: usize = 0,
    eval_requests: usize = 0,
    eval_skipped_eager: usize = 0,
    eval_forced_syncs: usize = 0,
    from_float32_calls: usize = 0,
    from_float32_bytes: usize = 0,
    to_float32_calls: usize = 0,
    to_float32_bytes: usize = 0,
    training_input_uploads: usize = 0,
    training_input_upload_bytes: usize = 0,
    packed_attention_forward_calls: usize = 0,
    packed_attention_backward_calls: usize = 0,
    exact_gelu_forward_calls: usize = 0,
    exact_gelu_backward_calls: usize = 0,
    upload_owned_host_calls: usize = 0,
    upload_owned_host_bytes: usize = 0,
    download_alloc_calls: usize = 0,
    download_alloc_bytes: usize = 0,
    upload_bucket_le_16: usize = 0,
    upload_bucket_le_1k: usize = 0,
    upload_bucket_le_8k: usize = 0,
    upload_bucket_le_32k: usize = 0,
    upload_bucket_le_256k: usize = 0,
    upload_bucket_gt_256k: usize = 0,
    download_bucket_le_16: usize = 0,
    download_bucket_le_1k: usize = 0,
    download_bucket_le_8k: usize = 0,
    download_bucket_le_32k: usize = 0,
    download_bucket_le_256k: usize = 0,
    download_bucket_gt_256k: usize = 0,
    add_scalar_calls: usize = 0,
    rms_norm_bare_calls: usize = 0,
    upload_top_sizes: [top_transfer_size_count]usize = [_]usize{0} ** top_transfer_size_count,
    upload_top_counts: [top_transfer_size_count]usize = [_]usize{0} ** top_transfer_size_count,
    download_top_sizes: [top_transfer_size_count]usize = [_]usize{0} ** top_transfer_size_count,
    download_top_counts: [top_transfer_size_count]usize = [_]usize{0} ** top_transfer_size_count,
    launch_embedding: usize = 0,
    launch_linear: usize = 0,
    launch_linear_qkv: usize = 0,
    deberta_fused_attention_calls: usize = 0,
    deberta_fused_attention_fallbacks: usize = 0,
    deberta_stream_f16_attention_calls: usize = 0,
    deberta_stream_f16_attention_fallbacks: usize = 0,
    deberta_stream_f16_staging_calls: usize = 0,
    deberta_materialized_f16_attention_calls: usize = 0,
    deberta_materialized_f16_attention_fallbacks: usize = 0,
    deberta_materialized_workspace_rejections: usize = 0,
    deberta_materialized_workspace_peak_bytes: usize = 0,
    deberta_generated_tc_attention_calls: usize = 0,
    deberta_generated_tc_m32_attention_calls: usize = 0,
    deberta_generated_tc_m16_attention_calls: usize = 0,
    deberta_generated_tc_attention_fallbacks: usize = 0,
    launch_norm: usize = 0,
    launch_norm_layer: usize = 0,
    launch_norm_add_layer: usize = 0,
    launch_norm_rms: usize = 0,
    launch_norm_rms_add: usize = 0,
    launch_norm_rms_add_mul_scalar: usize = 0,
    launch_norm_rms_add_output_scale: usize = 0,
    launch_norm_rms_bare: usize = 0,
    launch_norm_head_rope: usize = 0,
    launch_rope: usize = 0,
    launch_attention: usize = 0,
    launch_attention_gqa_decode: usize = 0,
    launch_attention_gqa_decode_generated: usize = 0,
    launch_attention_gqa_decode_splitk_online_sm89: usize = 0,
    launch_attention_gqa_decode_splitk_online_sm89_hd256: usize = 0,
    launch_attention_gqa_decode_splitk_online_sm89_hd512: usize = 0,
    launch_attention_gqa_decode_splitk_online_sm89_fallbacks: usize = 0,
    launch_attention_gqa_decode_splitk_online_sm89_ineligible_fallbacks: usize = 0,
    launch_attention_gqa_decode_splitk_online_sm89_symbol_fallbacks: usize = 0,
    launch_attention_gqa_decode_splitk_online_sm89_forbidden_routes: usize = 0,
    launch_attention_gqa_decode_score_prework: usize = 0,
    launch_attention_gqa_decode_score_prework_serial: usize = 0,
    launch_attention_gqa_decode_score_prework_serial_hd256: usize = 0,
    launch_attention_gqa_decode_score_prework_serial_hd512: usize = 0,
    launch_attention_gqa_decode_score_prework_tiled64: usize = 0,
    launch_attention_gqa_decode_score_prework_tiled64_hd256: usize = 0,
    launch_attention_gqa_decode_score_prework_tiled64_hd512: usize = 0,
    launch_attention_gqa_decode_score_prework_tiled64_fallbacks: usize = 0,
    launch_attention_gqa_decode_score_prework_tiled64_forbidden_routes: usize = 0,
    launch_attention_gqa_decode_score_prework_tiled64_symbol_fallbacks: usize = 0,
    launch_attention_gqa_decode_fast: usize = 0,
    launch_attention_gqa_decode_fast_fallbacks: usize = 0,
    launch_attention_gqa_prefill_fast: usize = 0,
    launch_attention_gqa_prefill_tiled: usize = 0,
    launch_attention_gqa_prefill_mma: usize = 0,
    launch_attention_gqa_prefill_mma_m32: usize = 0,
    launch_attention_gqa_prefill_tiled_f16_exact: usize = 0,
    launch_attention_gqa_prefill_tiled_f16_exact_hd256: usize = 0,
    launch_attention_gqa_prefill_tiled_f16_exact_hd512: usize = 0,
    launch_attention_gqa_prefill_tiled_f16_warp: usize = 0,
    launch_attention_gqa_prefill_tiled_f16_warp_hd256: usize = 0,
    launch_attention_gqa_prefill_tiled_f16_warp_hd512: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_hd256_q512: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_hd256_q3: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_hd512_q512: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_hd512_q3: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_fallbacks: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_ineligible_fallbacks: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_symbol_fallbacks: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd256_q512: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd256_q3: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd512_q512: usize = 0,
    launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd512_q3: usize = 0,
    launch_attention_gqa_scalar: usize = 0,
    launch_elementwise: usize = 0,
    launch_scalar: usize = 0,
    launch_scalar_multiply_immediate: usize = 0,
    launch_scalar_add_immediate: usize = 0,
    launch_scalar_device_broadcast: usize = 0,
    launch_argmax: usize = 0,
    launch_other: usize = 0,
    activation_multiply_fused: usize = 0,
    fused_gate_up_bf16_hits: usize = 0,
    add_mul_scalar_fused: usize = 0,
    rms_norm_add_fused: usize = 0,
    rms_norm_add_output_scale_fused: usize = 0,
    rms_norm_add_output_scale_fallbacks: usize = 0,
    rms_norm_add_weighted_embedding_fused_q6_k: usize = 0,
    host_attention_fallbacks: usize = 0,
    rope_host_fallbacks: usize = 0,
    rope_per_item_host_fallbacks: usize = 0,
    gqa_dense_host_fallbacks: usize = 0,
    paged_attention_host_fallbacks: usize = 0,
    device_kv_writes: usize = 0,
    device_kv_reads: usize = 0,
    device_kv_attempts: usize = 0,
    device_kv_successes: usize = 0,
    device_kv_fail_batch: usize = 0,
    device_kv_batch_steps: usize = 0,
    device_kv_batch_items: usize = 0,
    device_kv_fail_no_cache: usize = 0,
    device_kv_fail_no_storage: usize = 0,
    device_kv_fail_no_hook: usize = 0,
    device_kv_fail_write: usize = 0,
    device_kv_fail_read: usize = 0,
    device_kv_fail_shape: usize = 0,
    device_kv_paged_block_table_uploads: usize = 0,
    device_kv_paged_block_table_bytes: usize = 0,
    device_kv_paged_identity_attention_reads: usize = 0,
    device_kv_compressed_v_writes: usize = 0,
    device_kv_compressed_v_reads: usize = 0,
    device_kv_compressed_v_bytes: usize = 0,
    q4_0_q8_1_prefill_linear_hits: usize = 0,
    q4_0_q8_1_prefill_linear_rows2_hits: usize = 0,
    q4_0_q8_1_prefill_linear_rows4_hits: usize = 0,
    q4_0_q8_1_prefill_linear_rows8_c4_hits: usize = 0,
    q4_0_q8_1_prefill_linear_e4b_down_rows_hits: usize = 0,
    q4_0_q8_1_prefill_linear_generic_rows_hits: usize = 0,
    q4_0_q8_1_prefill_linear_tile8_rows_hits: usize = 0,
    q4_0_q8_1_prefill_qkv_hits: usize = 0,
    q4_0_q8_1_prefill_qkv_rows4_hits: usize = 0,
    q4_0_q8_1_prefill_qkv_tile8_rows_hits: usize = 0,
    q4_0_q8_1_prefill_qkv_tile8_w8_rows_hits: usize = 0,
    q4_0_q8_1_prefill_pair_hits: usize = 0,
    q4_0_q8_1_prefill_pair_rows2_hits: usize = 0,
    q4_0_q8_1_prefill_pair_rows4_hits: usize = 0,
    q4_0_q8_1_prefill_pair_rows8_c2_hits: usize = 0,
    q4_0_q8_1_prefill_pair_rows16_c1_hits: usize = 0,
    q4_0_q8_1_prefill_pair_generic_rows_hits: usize = 0,
    q4_0_q8_1_prefill_pair_tile8_rows_hits: usize = 0,
    q4_0_q8_1_prefill_gated_down_hits: usize = 0,
    q4_0_q8_1_prefill_gated_down_rows2_hits: usize = 0,
    q4_0_q8_1_prefill_gated_down_rows4_hits: usize = 0,
    q4_0_q8_1_prefill_gated_down_rows8_c4_hits: usize = 0,
    q4_0_q8_1_prefill_gated_down_e4b_down_rows_hits: usize = 0,
    q4_0_q8_1_prefill_gated_down_generic_rows_hits: usize = 0,
    q4_0_q8_1_prefill_gated_down_tile8_rows_hits: usize = 0,
    qkv_fused_q8: usize = 0,
    qkv_fused_q4_0: usize = 0,
    qkv_fused_q4_0_tile4: usize = 0,
    qkv_fused_q4_0_tile8: usize = 0,
    qkv_fused_q4: usize = 0,
    qkv_fused_q4_q4_f32: usize = 0,
    qkv_fused_f32: usize = 0,
    qkv_fused_f16: usize = 0,
    linear_pair_fused_q8: usize = 0,
    linear_pair_fused_q4_0: usize = 0,
    linear_pair_fused_q4_0_activation: usize = 0,
    linear_pair_fused_q4_0_tile4: usize = 0,
    linear_pair_fused_q4_0_tile8: usize = 0,
    linear_activation_slice_fused_q4_0: usize = 0,
    ple_gate_prefill_bf16_mirror_first_hits: usize = 0,
    ple_gate_prefill_bf16_mirror_first_ineligible: usize = 0,
    ple_gate_decode_q4_fused_preserved: usize = 0,
    linear_pair_fused_q4: usize = 0,
    linear_pair_fallbacks: usize = 0,
    lm_head_argmax_fused_q8: usize = 0,
    lm_head_argmax_fused_q4_0: usize = 0,
    lm_head_argmax_fused_q4_0_q8_1: usize = 0,
    lm_head_argmax_q4_0_q8_1_fallbacks: usize = 0,
    lm_head_argmax_fused_q4: usize = 0,
    lm_head_argmax_fused_q6: usize = 0,
    lm_head_argmax_generated_q6_k_q8_1_hits: usize = 0,
    lm_head_argmax_generated_q6_k_q8_1_fallbacks: usize = 0,
    lm_head_argmax_fallbacks: usize = 0,
    bf16_cublaslt_linear_calls: usize = 0,
    bf16_cublaslt_qkv_calls: usize = 0,
    bf16_cublaslt_activation_staging_calls: usize = 0,
    bf16_cublaslt_activation_mirror_hits: usize = 0,
    bf16_cublaslt_fallbacks: usize = 0,
    bf16_cublaslt_tuning_tuned_calls: usize = 0,
    bf16_cublaslt_tuning_heuristic_calls: usize = 0,
    bf16_cublaslt_tuning_api_fallbacks: usize = 0,
    bf16_scalar_linear_calls: usize = 0,
    bf16_scalar_qkv_calls: usize = 0,
    f16_cublaslt_linear_calls: usize = 0,
    f16_cublaslt_qkv_calls: usize = 0,
    f16_cublaslt_activation_staging_calls: usize = 0,
    f16_cublaslt_fallbacks: usize = 0,
    f16_scalar_linear_calls: usize = 0,
    rms_norm_bf16_mirror_hits: usize = 0,
    pinned_bulk_downloads: usize = 0,
    qkv_fallback_unsupported: usize = 0,
    qkv_kernel_unavailable: usize = 0,
    q4k_decode_fast_hits: usize = 0,
    q4k_decode_fast_fallbacks: usize = 0,
    q4_0_generated_mmv_hits: usize = 0,
    q4_0_generated_mmv_fallbacks: usize = 0,
    q4_0_generated_mm_hits: usize = 0,
    q4_0_generated_mm_fallbacks: usize = 0,
    q4_0_generated_pair_hits: usize = 0,
    q4_0_generated_pair_fallbacks: usize = 0,
    q4_0_generated_pair_q8_hits: usize = 0,
    q4_0_generated_pair_q8_fallbacks: usize = 0,
    q4_0_generated_down_q8_hits: usize = 0,
    q4_0_generated_down_q8_fallbacks: usize = 0,
    q4_0_generated_e2b_pair_q8_hits: usize = 0,
    q4_0_generated_e2b_pair_q8_fallbacks: usize = 0,
    q4_0_generated_e2b_down_q8_hits: usize = 0,
    q4_0_generated_e2b_down_q8_fallbacks: usize = 0,
    q4_0_generated_e2b_pair_only_hits: usize = 0,
    q4_0_generated_e2b_pair_only_fallbacks: usize = 0,
    q4_0_generated_e2b_exact_pair_f32_hits: usize = 0,
    q4_0_generated_e2b_exact_pair_f32_fallbacks: usize = 0,
    q4_0_generated_e2b_exact_down_f32_hits: usize = 0,
    q4_0_generated_e2b_exact_down_f32_fallbacks: usize = 0,
    q4_0_ggml_q8_1_e2b_ffn_hits: usize = 0,
    q4_0_ggml_q8_1_e2b_ffn_fallbacks: usize = 0,
    q4_0_ggml_q8_1_quantize_fallbacks: usize = 0,
    q4_0_ggml_q8_1_pair_fallbacks: usize = 0,
    q4_0_ggml_q8_1_down_fallbacks: usize = 0,
    generated_kernel_catalog_resolve_attempts: usize = 0,
    generated_kernel_catalog_resolve_misses: usize = 0,
    generated_kernel_catalog_hits: usize = 0,
    generated_kernel_catalog_fallbacks: usize = 0,
    head_norm_rope_fused_hits: usize = 0,
    head_norm_rope_fused_fallbacks: usize = 0,
    decoder_runtime_linear_slot_prepares: usize = 0,
    decoder_runtime_linear_slot_prepare_misses: usize = 0,
    decoder_runtime_rms_norm_slot_prepares: usize = 0,
    decoder_runtime_rms_norm_slot_prepare_misses: usize = 0,
    decoder_runtime_linear_apply_hits: usize = 0,
    decoder_runtime_linear_apply_misses: usize = 0,
    decoder_runtime_linear_pair_apply_hits: usize = 0,
    decoder_runtime_linear_qkv_apply_hits: usize = 0,
    decoder_runtime_rms_norm_apply_hits: usize = 0,
    decoder_runtime_rms_norm_apply_misses: usize = 0,
    decoder_runtime_attention_residual_attempts: usize = 0,
    decoder_runtime_attention_residual_hits: usize = 0,
    decoder_runtime_attention_residual_misses: usize = 0,
    decoder_runtime_gated_ffn_attempts: usize = 0,
    decoder_runtime_gated_ffn_hits: usize = 0,
    decoder_runtime_gated_ffn_misses: usize = 0,
    gated_down_fused_q8: usize = 0,
    gated_down_fused_q4_0: usize = 0,
    gated_down_fused_q4_0_precompute: usize = 0,
    gated_down_fused_q4_0_tile4: usize = 0,
    gated_down_fused_q4_0_tile8: usize = 0,
    gated_down_fused_q4_0_tile16: usize = 0,
    gated_down_fused_q4: usize = 0,
    gated_down_fallbacks: usize = 0,
    decode_profile_events: usize = 0,
    decode_profile_qkv_us: u64 = 0,
    decode_profile_gqa_attention_us: u64 = 0,
    decode_profile_attention_output_us: u64 = 0,
    decode_profile_attention_norm_residual_us: u64 = 0,
    decode_profile_ffn_gate_up_us: u64 = 0,
    decode_profile_ffn_gated_down_us: u64 = 0,
    decode_profile_ffn_post_norm_us: u64 = 0,
    decode_profile_lm_head_argmax_us: u64 = 0,
    decode_profile_graph_replay_us: u64 = 0,
    prefill_profile_events: usize = 0,
    prefill_profile_q4_linear_us: u64 = 0,
    prefill_profile_q4_qkv_us: u64 = 0,
    prefill_profile_q4_pair_us: u64 = 0,
    prefill_profile_q4_gated_down_us: u64 = 0,
    prefill_profile_bf16_linear_us: u64 = 0,
    prefill_profile_bf16_qkv_us: u64 = 0,
    prefill_profile_bf16_pair_us: u64 = 0,
    prefill_profile_attention_us: u64 = 0,
    prefill_profile_ple_dense_us: u64 = 0,
    prefill_profile_staging_us: u64 = 0,
    prefill_profile_norm_us: u64 = 0,
    prefill_profile_rope_us: u64 = 0,
    prefill_profile_kv_write_us: u64 = 0,
    prefill_profile_elementwise_us: u64 = 0,
    prefill_profile_embedding_us: u64 = 0,
    decoder_runtime_pinned_eviction_skips: usize = 0,
    mtp_preproject_fused_hits: usize = 0,
    mtp_preproject_fused_f32_weight_hits: usize = 0,
    mtp_preproject_fused_bf16_weight_hits: usize = 0,
    mtp_preproject_fused_f16_weight_hits: usize = 0,
    mtp_preproject_fused_fallbacks: usize = 0,
    mtp_masked_select_fused_hits: usize = 0,
    mtp_masked_select_fused_f32_weight_hits: usize = 0,
    mtp_masked_select_fused_bf16_weight_hits: usize = 0,
    mtp_masked_select_fused_f16_weight_hits: usize = 0,
    mtp_masked_select_fused_fallbacks: usize = 0,
    mtp_masked_select_hidden_fused_hits: usize = 0,
    mtp_masked_select_hidden_fused_bf16_hits: usize = 0,
    mtp_masked_select_hidden_multiblock_hits: usize = 0,
    mtp_masked_select_hidden_fused_fallbacks: usize = 0,
    mtp_masked_argmax_hits: usize = 0,
    mtp_masked_argmax_fallbacks: usize = 0,
    mtp_verify_commit_device_hits: usize = 0,
    mtp_verify_commit_device_fallbacks: usize = 0,
    mtp_verify_commit_result_downloads: usize = 0,
    mtp_verify_commit_choice_downloads: usize = 0,
    lazy_prefetch_enqueues: usize = 0,
    lazy_prefetch_duplicates: usize = 0,
    lazy_prefetch_missing: usize = 0,
    lazy_prefetch_cancelled_for_demand: usize = 0,
    lazy_prefetch_drain_calls: usize = 0,
    lazy_demand_loads: usize = 0,
    lazy_host_prefetch_hits: usize = 0,
    lazy_host_load_ns: u64 = 0,
    lazy_host_page_touch_ns: u64 = 0,
    lazy_upload_ns: u64 = 0,
    lazy_uploaded_bytes: usize = 0,
    ffn_stream_requests: usize = 0,
    ffn_stream_hits: usize = 0,
    ffn_stream_misses: usize = 0,
    ffn_stream_fallbacks: usize = 0,
    ffn_stream_evictions: usize = 0,
    ffn_stream_read_ns: u64 = 0,
    ffn_stream_h2d_ns: u64 = 0,
    ffn_stream_read_bytes: usize = 0,
    ffn_stream_uploaded_bytes: usize = 0,
    ffn_stream_resident_bytes: usize = 0,
    ffn_stream_fadvise_calls: usize = 0,
    dense_stream_requests: usize = 0,
    dense_stream_hits: usize = 0,
    dense_stream_misses: usize = 0,
    dense_stream_fallbacks: usize = 0,
    dense_stream_evictions: usize = 0,
    dense_stream_read_ns: u64 = 0,
    dense_stream_h2d_ns: u64 = 0,
    dense_stream_read_bytes: usize = 0,
    dense_stream_uploaded_bytes: usize = 0,
    dense_stream_resident_bytes: usize = 0,
    dense_stream_fadvise_calls: usize = 0,
    dense_stream_attention_loads: usize = 0,
    dense_stream_mlp_loads: usize = 0,
    dense_prefetch_enqueues: usize = 0,
    dense_prefetch_duplicates: usize = 0,
    dense_prefetch_ready_hits: usize = 0,
    dense_prefetch_inflight_steals: usize = 0,
    dense_prefetch_sync_reads: usize = 0,
    dense_prefetch_evictions: usize = 0,
    dense_prefetch_failures: usize = 0,
    dense_prefetch_host_read_ns: u64 = 0,
    dense_prefetch_demand_wait_ns: u64 = 0,
    dense_prefetch_upload_ns: u64 = 0,
    dense_prefetch_resident_bytes: usize = 0,
    dense_prefetch_read_bytes: usize = 0,
};

fn noteGqaFlashPrefillStats(
    stats: *RuntimeStats,
    launch_kind: kernels_mod.GqaAttentionLaunchKind,
    head_dim: usize,
    q_seq_len: usize,
) void {
    const fallback = launch_kind == .prefill_flash_f16_sm89_ineligible_fallback or
        launch_kind == .prefill_flash_f16_sm89_symbol_fallback;
    if (fallback) {
        stats.launch_attention_gqa_prefill_flash_f16_sm89_fallbacks += 1;
        if (launch_kind == .prefill_flash_f16_sm89_ineligible_fallback) {
            stats.launch_attention_gqa_prefill_flash_f16_sm89_ineligible_fallbacks += 1;
        } else {
            stats.launch_attention_gqa_prefill_flash_f16_sm89_symbol_fallbacks += 1;
        }
        if (head_dim == 256 and q_seq_len == 512) stats.launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd256_q512 += 1;
        if (head_dim == 256 and q_seq_len == 3) stats.launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd256_q3 += 1;
        if (head_dim == 512 and q_seq_len == 512) stats.launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd512_q512 += 1;
        if (head_dim == 512 and q_seq_len == 3) stats.launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd512_q3 += 1;
        return;
    }
    std.debug.assert(launch_kind == .prefill_flash_f16_sm89);
    stats.launch_attention_gqa_prefill_flash_f16_sm89 += 1;
    if (head_dim == 256 and q_seq_len == 512) stats.launch_attention_gqa_prefill_flash_f16_sm89_hd256_q512 += 1;
    if (head_dim == 256 and q_seq_len == 3) stats.launch_attention_gqa_prefill_flash_f16_sm89_hd256_q3 += 1;
    if (head_dim == 512 and q_seq_len == 512) stats.launch_attention_gqa_prefill_flash_f16_sm89_hd512_q512 += 1;
    if (head_dim == 512 and q_seq_len == 3) stats.launch_attention_gqa_prefill_flash_f16_sm89_hd512_q3 += 1;
}

fn noteGqaSplitkOnlineDecodeStats(
    stats: *RuntimeStats,
    launch_kind: kernels_mod.GqaAttentionLaunchKind,
    head_dim: usize,
) void {
    switch (launch_kind) {
        .decode_splitk_online_sm89 => {
            stats.launch_attention_gqa_decode_splitk_online_sm89 += 1;
            if (head_dim == 256) stats.launch_attention_gqa_decode_splitk_online_sm89_hd256 += 1;
            if (head_dim == 512) stats.launch_attention_gqa_decode_splitk_online_sm89_hd512 += 1;
        },
        .decode_splitk_online_sm89_ineligible_fallback => {
            stats.launch_attention_gqa_decode_splitk_online_sm89_fallbacks += 1;
            stats.launch_attention_gqa_decode_splitk_online_sm89_ineligible_fallbacks += 1;
            stats.launch_attention_gqa_decode_splitk_online_sm89_forbidden_routes += 1;
        },
        .decode_splitk_online_sm89_symbol_fallback => {
            stats.launch_attention_gqa_decode_splitk_online_sm89_fallbacks += 1;
            stats.launch_attention_gqa_decode_splitk_online_sm89_symbol_fallbacks += 1;
        },
        else => unreachable,
    }
}

fn noteTopTransferSize(sizes: *[RuntimeStats.top_transfer_size_count]usize, counts: *[RuntimeStats.top_transfer_size_count]usize, bytes: usize) void {
    if (bytes == 0) return;
    for (sizes, counts) |size, *count| {
        if (size == bytes) {
            count.* += 1;
            return;
        }
    }
    var empty_index: ?usize = null;
    var min_index: usize = 0;
    for (counts, 0..) |count, i| {
        if (sizes[i] == 0) {
            empty_index = i;
            break;
        }
        if (count < counts[min_index]) min_index = i;
    }
    const target = empty_index orelse min_index;
    sizes[target] = bytes;
    counts[target] = 1;
}

fn noteUploadBucket(stats: *RuntimeStats, bytes: usize) void {
    if (bytes <= 16) {
        stats.upload_bucket_le_16 += 1;
    } else if (bytes <= 1024) {
        stats.upload_bucket_le_1k += 1;
    } else if (bytes <= 8192) {
        stats.upload_bucket_le_8k += 1;
    } else if (bytes <= 32768) {
        stats.upload_bucket_le_32k += 1;
    } else if (bytes <= 262144) {
        stats.upload_bucket_le_256k += 1;
    } else {
        stats.upload_bucket_gt_256k += 1;
    }
}

fn noteDownloadBucket(stats: *RuntimeStats, bytes: usize) void {
    if (bytes <= 16) {
        stats.download_bucket_le_16 += 1;
    } else if (bytes <= 1024) {
        stats.download_bucket_le_1k += 1;
    } else if (bytes <= 8192) {
        stats.download_bucket_le_8k += 1;
    } else if (bytes <= 32768) {
        stats.download_bucket_le_32k += 1;
    } else if (bytes <= 262144) {
        stats.download_bucket_le_256k += 1;
    } else {
        stats.download_bucket_gt_256k += 1;
    }
}

fn elapsedNsSince(start_ns: u64) u64 {
    const end_ns = platform.time.monotonicNs();
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}

const max_cuda_graph_replay_slots = 8;

const CudaGraphReplayOutputKind = enum {
    tensor,
    greedy_token,
};

const CudaGraphReplaySlot = struct {
    exec: driver_mod.CUgraphExec = null,
    replay_key: u64 = 0,
    input_storage: buffer_mod.DeviceBuffer = .{},
    aux_input_storage: buffer_mod.DeviceBuffer = .{},
    output_storage: buffer_mod.DeviceBuffer = .{},
    input: buffer_mod.DeviceBuffer = .{},
    aux_input: buffer_mod.DeviceBuffer = .{},
    output: buffer_mod.DeviceBuffer = .{},
    shape: ?[]i64 = null,
    elem_count: usize = 0,
    dtype: tensor_mod.DType = .f32,
    input_valid: bool = false,
    aux_input_valid: bool = false,
    aux_input_required: bool = false,
    aux_input_prepared: bool = false,
    valid: bool = false,
    kv_replay_capacity_tokens: usize = 0,
    kv_replay_capacity_valid: bool = false,
    decode_scalars_auto_advance: bool = false,
    decode_scalars_auto_advance_delta: [5]u32 = .{ 1, 1, 1, 1, 0 },
    temp_arena_generation: u64 = 0,
    captured_temp_allocation_count: usize = 0,
};

const Bf16MirrorRowSelector = union(enum) {
    /// Production policy: BF16 mirrors serve multi-row work; single-token
    /// decode remains on native quantized kernels.
    production,
    /// Diagnostic-only minimum row count. For example, 4 keeps a 3-row
    /// prefill tail on Q4 while preserving BF16 routing for full chunks.
    diagnostic_min_rows: usize,

    fn resolve() Bf16MirrorRowSelector {
        const requested = platform.env.getenvUsize(
            "ANTFLY_INFERENCE_CUDA_BF16_MIRROR_MIN_ROWS_DIAGNOSTIC",
        ) orelse return .production;
        // Zero is never a meaningful matrix row threshold. Fail closed to the
        // production contract instead of silently widening BF16 dispatch.
        if (requested == 0) return .production;
        return .{ .diagnostic_min_rows = requested };
    }

    fn accepts(self: Bf16MirrorRowSelector, rows: usize) bool {
        return switch (self) {
            .production => rows > 1,
            .diagnostic_min_rows => |min_rows| rows >= min_rows,
        };
    }
};

/// Candidate route for Gemma 4 E2B's per-layer-input gate during prefill.
///
/// The existing fused PLE kernel quantizes the activation to Q8_1 and reads
/// the Q4_0 weight directly. When hybrid residency has already admitted a BF16
/// mirror, the generic PLE fallback can instead use the existing BF16
/// cuBLASLt linear, then the existing activation/slice operation. Keep this a
/// typed, default-off profile until locked end-to-end qualification promotes
/// it; the exact model and SM89 checks prevent an experimental E2B decision
/// from silently widening to other Gemma variants.
const PleGatePrefillProfile = enum {
    off,
    mirror_first_sm89_e2b,

    fn parse(value: []const u8) ?PleGatePrefillProfile {
        if (std.mem.eql(u8, value, "off")) return .off;
        if (std.mem.eql(u8, value, "mirror-first-sm89-e2b")) return .mirror_first_sm89_e2b;
        return null;
    }

    fn name(self: PleGatePrefillProfile) []const u8 {
        return switch (self) {
            .off => "off",
            .mirror_first_sm89_e2b => "mirror-first-sm89-e2b",
        };
    }

    fn resolve(compute_major: i32, compute_minor: i32) driver_mod.Error!PleGatePrefillProfile {
        const raw = platform.env.getenv("ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE") orelse
            return .off;
        const profile = parse(raw) orelse {
            std.log.err(
                "cuda_ple_gate_prefill: status=invalid value={s} expected=off|mirror-first-sm89-e2b",
                .{raw},
            );
            return error.InvalidCudaState;
        };
        if (profile == .mirror_first_sm89_e2b and
            (compute_major != 8 or compute_minor != 9))
        {
            std.log.err(
                "cuda_ple_gate_prefill: status=unsupported profile={s} compute={d}.{d}",
                .{ profile.name(), compute_major, compute_minor },
            );
            return error.InvalidCudaState;
        }
        std.log.info(
            "cuda_ple_gate_prefill: status=configured profile={s} compute={d}.{d}",
            .{ profile.name(), compute_major, compute_minor },
        );
        return profile;
    }

    fn eligibleFor(
        self: PleGatePrefillProfile,
        compute_major: i32,
        compute_minor: i32,
        model_is_e2b: bool,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        bf16_mirror_admitted: bool,
        cublaslt_available: bool,
    ) bool {
        return self == .mirror_first_sm89_e2b and
            compute_major == 8 and compute_minor == 9 and model_is_e2b and
            rows > 1 and in_dim == 1536 and out_dim == 256 and
            bf16_mirror_admitted and cublaslt_available;
    }
};

fn isGemma4E2bPleRuntimeState(state: CudaDecoderRuntimeFamilyState) bool {
    return state.prepared and state.configured_layer_count == 35 and
        state.hidden_size == 1536 and state.intermediate_size == 6144 and
        state.num_hidden_layers == 35 and
        state.num_attention_heads == 8 and state.num_key_value_heads == 1 and
        (state.num_global_key_value_heads == 0 or state.num_global_key_value_heads == 1) and
        state.attention_head_dim == 256 and
        state.global_head_dim == 512 and state.sliding_window == 512 and
        state.sliding_window_pattern == 5 and state.ple_hidden_size == 256;
}

const Sm89Q4_0GgmlQ8_1Gate = enum {
    off,
    ggml_ffn_v1,

    fn parse(raw: []const u8) !Sm89Q4_0GgmlQ8_1Gate {
        if (std.ascii.eqlIgnoreCase(raw, "off") or std.mem.eql(u8, raw, "0")) return .off;
        if (std.ascii.eqlIgnoreCase(raw, "ggml-ffn-v1") or
            std.ascii.eqlIgnoreCase(raw, "ggml_ffn_v1")) return .ggml_ffn_v1;
        return error.InvalidCudaSm89Q4_0GgmlQ8_1Gate;
    }

    fn resolve() !Sm89Q4_0GgmlQ8_1Gate {
        const raw = platform.env.getenv("ANTFLY_INFERENCE_CUDA_SM89_Q4_0_Q8_1") orelse return .off;
        return parse(raw) catch |err| {
            std.log.err(
                "invalid ANTFLY_INFERENCE_CUDA_SM89_Q4_0_Q8_1={s}; expected off or ggml-ffn-v1",
                .{raw},
            );
            return err;
        };
    }

    fn enabledFor(
        self: Sm89Q4_0GgmlQ8_1Gate,
        compute_major: i32,
        compute_minor: i32,
        rows: usize,
        hidden_size: usize,
        intermediate_size: usize,
    ) bool {
        return self == .ggml_ffn_v1 and compute_major == 8 and compute_minor == 9 and
            rows == 1 and hidden_size == 1536 and
            (intermediate_size == 6144 or intermediate_size == 12288);
    }
};

const CublasLtBf16TuningProfile = enum {
    off,
    sm89_prefill,

    const Resolved = struct {
        profile: CublasLtBf16TuningProfile,
        from_env: bool,
    };

    fn parse(value: []const u8) ?CublasLtBf16TuningProfile {
        if (std.ascii.eqlIgnoreCase(value, "off") or std.mem.eql(u8, value, "0")) return .off;
        if (std.ascii.eqlIgnoreCase(value, "sm89-prefill")) return .sm89_prefill;
        return null;
    }

    /// Promoted default: the SM89 prefill tuning profile is on when the device
    /// compute capability is exactly 8.9 and the env variable is unset. The env
    /// variable keeps working in both directions (explicit `off` disables).
    fn defaultForTarget(compute_major: i32, compute_minor: i32) CublasLtBf16TuningProfile {
        if (compute_major == 8 and compute_minor == 9) return .sm89_prefill;
        return .off;
    }

    fn resolve(compute_major: i32, compute_minor: i32) driver_mod.Error!Resolved {
        const raw = platform.env.getenv("ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE") orelse {
            const profile = defaultForTarget(compute_major, compute_minor);
            if (profile != .off) {
                std.log.info(
                    "cuda_cublaslt_bf16_tuning: status=configured source=default profile=sm89-prefill candidates=8 warmups=3 iterations=10 min_rows=32 compute={d}.{d}",
                    .{ compute_major, compute_minor },
                );
            }
            return .{ .profile = profile, .from_env = false };
        };
        const profile = parse(raw) orelse {
            std.log.err(
                "cuda_cublaslt_bf16_tuning: status=invalid value={s} expected=off|sm89-prefill",
                .{raw},
            );
            return error.InvalidCudaState;
        };
        if (profile == .sm89_prefill and (compute_major != 8 or compute_minor != 9)) {
            std.log.err(
                "cuda_cublaslt_bf16_tuning: status=unsupported profile=sm89-prefill compute={d}.{d}",
                .{ compute_major, compute_minor },
            );
            return error.InvalidCudaState;
        }
        std.log.info(
            "cuda_cublaslt_bf16_tuning: status=configured source=env profile={s} candidates=8 warmups=3 iterations=10 min_rows=32 compute={d}.{d}",
            .{ if (profile == .off) "off" else "sm89-prefill", compute_major, compute_minor },
        );
        return .{ .profile = profile, .from_env = true };
    }

    fn tuningForRows(self: CublasLtBf16TuningProfile, rows: usize) cublaslt_mod.MatmulTuning {
        if (self != .sm89_prefill or rows < 32) return .{};
        return .{
            .enabled = true,
            .candidate_count = 8,
            .warmups = 3,
            .iterations = 10,
        };
    }
};

test "CUDA BF16 mirror row selector preserves production and diagnostic boundaries" {
    const production: Bf16MirrorRowSelector = .production;
    try std.testing.expect(production.accepts(2));
    try std.testing.expect(!production.accepts(1));
    const diagnostic = Bf16MirrorRowSelector{ .diagnostic_min_rows = 4 };
    try std.testing.expect(!diagnostic.accepts(3));
    try std.testing.expect(diagnostic.accepts(4));
    try std.testing.expect(diagnostic.accepts(512));
}

test "CUDA PLE gate mirror-first profile is typed default-safe and E2B bounded" {
    try std.testing.expectEqual(PleGatePrefillProfile.off, PleGatePrefillProfile.parse("off").?);
    try std.testing.expectEqual(
        PleGatePrefillProfile.mirror_first_sm89_e2b,
        PleGatePrefillProfile.parse("mirror-first-sm89-e2b").?,
    );
    try std.testing.expect(PleGatePrefillProfile.parse("MIRROR-FIRST-SM89-E2B") == null);
    try std.testing.expect(PleGatePrefillProfile.parse("mirror_first_sm89_e2b") == null);

    const candidate = PleGatePrefillProfile.mirror_first_sm89_e2b;
    try std.testing.expect(candidate.eligibleFor(8, 9, true, 3, 1536, 256, true, true));
    try std.testing.expect(candidate.eligibleFor(8, 9, true, 512, 1536, 256, true, true));
    try std.testing.expect(!PleGatePrefillProfile.off.eligibleFor(8, 9, true, 512, 1536, 256, true, true));
    try std.testing.expect(!candidate.eligibleFor(8, 9, true, 1, 1536, 256, true, true));
    try std.testing.expect(!candidate.eligibleFor(8, 6, true, 512, 1536, 256, true, true));
    try std.testing.expect(!candidate.eligibleFor(8, 9, false, 512, 1536, 256, true, true));
    try std.testing.expect(!candidate.eligibleFor(8, 9, true, 512, 2560, 256, true, true));
    try std.testing.expect(!candidate.eligibleFor(8, 9, true, 512, 1536, 2560, true, true));
    try std.testing.expect(!candidate.eligibleFor(8, 9, true, 512, 1536, 256, false, true));
    try std.testing.expect(!candidate.eligibleFor(8, 9, true, 512, 1536, 256, true, false));
}

test "CUDA PLE gate E2B runtime identity requires the complete locked contract" {
    var state = CudaDecoderRuntimeFamilyState{
        .prepared = true,
        .configured_layer_count = 35,
        .hidden_size = 1536,
        .intermediate_size = 6144,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .num_global_key_value_heads = 1,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .ple_hidden_size = 256,
    };
    try std.testing.expect(isGemma4E2bPleRuntimeState(state));
    state.num_global_key_value_heads = 0;
    try std.testing.expect(isGemma4E2bPleRuntimeState(state));
    state.num_global_key_value_heads = 1;
    state.num_key_value_heads = 2;
    try std.testing.expect(!isGemma4E2bPleRuntimeState(state));
    state.num_key_value_heads = 1;
    state.ple_hidden_size = 2560;
    try std.testing.expect(!isGemma4E2bPleRuntimeState(state));
}

test "CUDA llama Q8_1 gate is typed narrow and default off" {
    try std.testing.expectEqual(Sm89Q4_0GgmlQ8_1Gate.off, try Sm89Q4_0GgmlQ8_1Gate.parse("off"));
    try std.testing.expectEqual(Sm89Q4_0GgmlQ8_1Gate.off, try Sm89Q4_0GgmlQ8_1Gate.parse("0"));
    try std.testing.expectEqual(Sm89Q4_0GgmlQ8_1Gate.ggml_ffn_v1, try Sm89Q4_0GgmlQ8_1Gate.parse("ggml-ffn-v1"));
    try std.testing.expectEqual(Sm89Q4_0GgmlQ8_1Gate.ggml_ffn_v1, try Sm89Q4_0GgmlQ8_1Gate.parse("GGML_FFN_V1"));
    try std.testing.expectError(error.InvalidCudaSm89Q4_0GgmlQ8_1Gate, Sm89Q4_0GgmlQ8_1Gate.parse("true"));
    try std.testing.expect(!Sm89Q4_0GgmlQ8_1Gate.off.enabledFor(8, 9, 1, 1536, 6144));
    try std.testing.expect(Sm89Q4_0GgmlQ8_1Gate.ggml_ffn_v1.enabledFor(8, 9, 1, 1536, 6144));
    try std.testing.expect(Sm89Q4_0GgmlQ8_1Gate.ggml_ffn_v1.enabledFor(8, 9, 1, 1536, 12288));
    try std.testing.expect(!Sm89Q4_0GgmlQ8_1Gate.ggml_ffn_v1.enabledFor(8, 6, 1, 1536, 6144));
    try std.testing.expect(!Sm89Q4_0GgmlQ8_1Gate.ggml_ffn_v1.enabledFor(8, 9, 2, 1536, 6144));
    try std.testing.expect(!Sm89Q4_0GgmlQ8_1Gate.ggml_ffn_v1.enabledFor(8, 9, 1, 2560, 10240));
}

test "CUDA cuBLASLt BF16 tuning profile is typed bounded and tail-safe" {
    try std.testing.expectEqual(CublasLtBf16TuningProfile.off, CublasLtBf16TuningProfile.parse("off").?);
    try std.testing.expectEqual(CublasLtBf16TuningProfile.sm89_prefill, CublasLtBf16TuningProfile.parse("sm89-prefill").?);
    try std.testing.expect(CublasLtBf16TuningProfile.parse("sm90-prefill") == null);
    try std.testing.expect(!CublasLtBf16TuningProfile.sm89_prefill.tuningForRows(3).enabled);
    try std.testing.expect(!CublasLtBf16TuningProfile.sm89_prefill.tuningForRows(31).enabled);
    const tuning = CublasLtBf16TuningProfile.sm89_prefill.tuningForRows(32);
    try std.testing.expect(tuning.enabled);
    try std.testing.expectEqual(@as(u8, 8), tuning.candidate_count);
    try std.testing.expectEqual(@as(u8, 3), tuning.warmups);
    try std.testing.expectEqual(@as(u8, 10), tuning.iterations);
    try std.testing.expect(CublasLtBf16TuningProfile.sm89_prefill.tuningForRows(128).enabled);
    try std.testing.expect(!CublasLtBf16TuningProfile.off.tuningForRows(128).enabled);
}

test "CUDA cuBLASLt BF16 tuning profile defaults to sm89-prefill only on compute 8.9" {
    try std.testing.expectEqual(
        CublasLtBf16TuningProfile.sm89_prefill,
        CublasLtBf16TuningProfile.defaultForTarget(8, 9),
    );
    try std.testing.expectEqual(CublasLtBf16TuningProfile.off, CublasLtBf16TuningProfile.defaultForTarget(8, 6));
    try std.testing.expectEqual(CublasLtBf16TuningProfile.off, CublasLtBf16TuningProfile.defaultForTarget(9, 0));
    try std.testing.expectEqual(CublasLtBf16TuningProfile.off, CublasLtBf16TuningProfile.defaultForTarget(7, 5));
}

test "CUDA Q4 route census computes actual E2B FFN bytes" {
    const small_pair = q4RouteBytesPerCall(1, 1536, 6144, .pair);
    const small_down = q4RouteBytesPerCall(1, 6144, 1536, .single);
    const large_pair = q4RouteBytesPerCall(1, 1536, 12288, .pair);
    const large_down = q4RouteBytesPerCall(1, 12288, 1536, .single);
    try std.testing.expectEqual(@as(u64, 10_616_832), small_pair);
    try std.testing.expectEqual(@as(u64, 5_308_416), small_down);
    try std.testing.expectEqual(@as(u64, 21_233_664), large_pair);
    try std.testing.expectEqual(@as(u64, 10_616_832), large_down);
    try std.testing.expectEqual(
        @as(u64, 875_888_640),
        15 * (small_pair + small_down) + 20 * (large_pair + large_down),
    );
    try std.testing.expectEqual(@as(u64, 0), q4RouteBytesPerCall(512, 2048, 2048, .gqa));
}

test "CUDA Q4 route census aggregates by full launch identity" {
    var stats = CudaDispatchStats{};
    defer stats.deinit(std.testing.allocator);
    stats.noteQ4(
        std.testing.allocator,
        true,
        .ffn_down,
        1,
        6144,
        1536,
        .single,
        .none,
        .generated,
        "candidate",
        .{ 1536, 1, 1 },
        .{ 128, 1, 1 },
    );
    stats.noteQ4(
        std.testing.allocator,
        true,
        .ffn_down,
        1,
        6144,
        1536,
        .single,
        .none,
        .generated,
        "candidate",
        .{ 1536, 1, 1 },
        .{ 128, 1, 1 },
    );
    stats.noteQ4(
        std.testing.allocator,
        true,
        .ffn_down,
        1,
        6144,
        1536,
        .single,
        .none,
        .fallback,
        "baseline",
        .{ 384, 1, 1 },
        .{ 256, 1, 1 },
    );
    try std.testing.expectEqual(@as(usize, 2), stats.q4_entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), stats.q4_entries.items[0].calls);
    try std.testing.expectEqual(@as(u64, 10_616_832), stats.q4_entries.items[0].weighted_q4_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.q4_provider_calls[@intFromEnum(CudaQ4RouteProvider.fallback)]);
}

pub const CudaCompute = struct {
    allocator: std.mem.Allocator,
    ctx: context_mod.CudaContext,
    kernels: kernels_mod.KernelModule,
    resident_weights: std.StringHashMapUnmanaged(CudaTensor) = .{},
    lazy_host_store: ?*native_compute_mod.WeightStore = null,
    lazy_device_epochs: std.StringHashMapUnmanaged(u64) = .{},
    lazy_device_bytes: usize = 0,
    lazy_device_budget_bytes: usize = 0,
    lazy_access_epoch: u64 = 1,
    decoder_runtime_linear_slots: std.AutoHashMapUnmanaged(usize, CudaDecoderRuntimeLinearSlot) = .empty,
    decoder_runtime_rms_norm_slots: std.AutoHashMapUnmanaged(usize, CudaDecoderRuntimeRmsNormSlot) = .empty,
    decoder_runtime_next_dynamic_slot: usize = 1,
    decoder_runtime_family_state: CudaDecoderRuntimeFamilyState = .{},
    run_budget: ?*run_memory.RunBudget = null,
    dense_stream_entries: std.ArrayListUnmanaged(DenseStreamEntry) = .empty,
    dense_stream_epoch: u64 = 1,
    dense_host_prefetch_entries: std.StringHashMapUnmanaged(*DenseHostPrefetchEntry) = .{},
    dense_host_prefetch: DenseHostPrefetchQueue = undefined,
    dense_host_prefetch_initialized: bool = false,
    dense_host_prefetch_epoch: u64 = 1,
    temp_buffer_mutex: std.atomic.Mutex = .unlocked,
    temp_buffers: std.ArrayListUnmanaged(buffer_mod.DeviceBuffer) = .empty,
    temp_pinned_slots: std.ArrayListUnmanaged(TempPinnedSlot) = .empty,
    temp_arena_planner: execution_plan.TempArenaPlanner = .{},
    temp_arena_generation: u64 = 1,
    deferred_device_frees: std.ArrayListUnmanaged(buffer_mod.DeviceBuffer) = .empty,
    deferred_device_free_bytes: usize = 0,
    temp_trace_seq: usize = 0,
    florence_tail_row_f32: ?CT = null,
    florence_tail_col_f32: ?CT = null,
    florence_tail_temporal_f32: ?CT = null,
    florence_image_projection_t_f32: ?CT = null,
    florence_tail_row_source_ptr: driver_mod.CUdeviceptr = 0,
    florence_tail_col_source_ptr: driver_mod.CUdeviceptr = 0,
    florence_tail_temporal_source_ptr: driver_mod.CUdeviceptr = 0,
    florence_image_projection_source_ptr: driver_mod.CUdeviceptr = 0,
    florence_image_projection_vision_dim: usize = 0,
    florence_image_projection_projection_dim: usize = 0,
    debug_cuda_graph_capture_active: bool = false,
    debug_cuda_graph_capture_disabled: bool = false,
    debug_cuda_graph_capture_inhibit_once: bool = false,
    debug_cuda_graph_slots: [max_cuda_graph_replay_slots]CudaGraphReplaySlot = [_]CudaGraphReplaySlot{.{}} ** max_cuda_graph_replay_slots,
    debug_cuda_graph_active_slot: ?usize = null,
    debug_cuda_graph_prepared_slot: ?usize = null,
    debug_cuda_graph_prepared_output_kind: CudaGraphReplayOutputKind = .tensor,
    debug_cuda_graph_next_evict_slot: usize = 0,
    debug_cuda_decode_scalars: buffer_mod.DeviceBuffer = .{},
    debug_cuda_decode_scalars_host: [5]u32 = .{ 0, 0, 0, 0, 0 },
    debug_cuda_decode_scalars_host_valid: bool = false,
    debug_cuda_decode_scalars_device: [5]u32 = .{ 0, 0, 0, 0, 0 },
    debug_cuda_decode_scalars_device_valid: bool = false,
    debug_cuda_decode_scalars_auto_advance_blocked: bool = false,
    debug_cuda_decode_scalars_upload_deferred: bool = false,
    debug_cuda_graph_decode_kv_seq_len: usize = 0,
    debug_cuda_graph_capture_temp_seq_begin: usize = 0,
    generated_gqa_score_prework_templates: std.ArrayListUnmanaged(kernels_mod.GeneratedGqaScorePreworkRequest) = .empty,
    debug_cuda_graph_prepared_attention_topology: [3]u8 = .{ 0, 0, 0 },
    decode_profile_gqa_attention_active: bool = false,
    // Non-blocking per-op profiler state. Allocated lazily on the first profiled
    // op when profiling is enabled; stays a zero-cost empty struct otherwise.
    profile_pool: CudaProfilePool = .{},
    pinned_scalar_upload_ring: PinnedScalarUploadRing = .{},
    pinned_upload_arena: PinnedUploadArena = .{},
    pinned_scalar_download_buffer: PinnedScalarDownloadBuffer = .{},
    pinned_bulk_download_buffer: buffer_mod.HostBuffer = .{},
    training_upload_generation: u64 = 0,
    async_i32_scalar_download: AsyncI32ScalarDownload = .{},
    temp_ids_masks: scratch_mod.DeviceScratch = .{},
    // A BERT forward shares one attention mask across every transformer
    // block. Keep that mask in a dedicated scratch allocation so padded
    // device-resident encodes make one H2D upload per request rather than one
    // per layer. The retained host copy prevents stale masks across requests.
    attention_mask_scratch: scratch_mod.DeviceScratch = .{},
    attention_mask_cache_host: std.ArrayListUnmanaged(i64) = .empty,
    bf16_activation_scratch: scratch_mod.DeviceScratch = .{},
    f16_activation_scratch: scratch_mod.DeviceScratch = .{},
    deberta_q_f16_scratch: scratch_mod.DeviceScratch = .{},
    deberta_k_f16_scratch: scratch_mod.DeviceScratch = .{},
    deberta_v_f16_scratch: scratch_mod.DeviceScratch = .{},
    deberta_qr_f16_scratch: scratch_mod.DeviceScratch = .{},
    deberta_kr_f16_scratch: scratch_mod.DeviceScratch = .{},
    // Materialized attention uses one preflighted arena. A single allocation
    // avoids ten independent grow/synchronize cycles and makes the complete
    // retained workspace visible to admission before any CUDA allocation.
    deberta_materialized_scratch: scratch_mod.DeviceScratch = .{},
    cublaslt_workspace_scratch: scratch_mod.DeviceScratch = .{},
    cublaslt: ?cublaslt_mod.CublasLt = null,
    stats: RuntimeStats = .{},
    dispatch_stats: CudaDispatchStats = .{},
    q4_route_census_enabled: bool = false,
    q4_route_op_override: ?CudaQ4RouteOp = null,
    bf16_mirror_row_selector: Bf16MirrorRowSelector = .production,
    ple_gate_prefill_profile: PleGatePrefillProfile = .off,
    cublaslt_bf16_tuning_profile: CublasLtBf16TuningProfile = .off,
    sm89_q4_0_ggml_q8_1_gate: Sm89Q4_0GgmlQ8_1Gate = .off,
    generated_q4_0_gates: GeneratedQ4_0Gates = .{},
    tuned_route_gates: CudaTunedRouteGates = .{},
    // Lazily-built concatenated [2*ffn, hidden] BF16 gate|up mirrors for the
    // fused gate/up prefill GEMM, keyed by the gate weight's mirror pointer.
    // Owned here; freed in deinit.
    bf16_pair_mirror_cache: std.AutoHashMapUnmanaged(usize, buffer_mod.DeviceBuffer) = .{},
    capture_gates: CudaCaptureGateConfig = .{},
    temp_slot_config: CudaTempSlotConfig = .{},
    runtime_jit_shape_gating: bool = false,
    owned_by_backend: bool = false,

    pub fn init(allocator: std.mem.Allocator) !CudaCompute {
        return initWithKernelJit(allocator, .{});
    }

    pub fn initWithKernelJit(allocator: std.mem.Allocator, jit_config: kernel_jit.Config) !CudaCompute {
        return initWithKernelJitProfile(allocator, jit_config, null, .{}, .dynamic);
    }

    /// Capability-only initialization is deliberately JIT-empty. Callers that
    /// own model weights must use initWithKernelJitForScope so required mode
    /// never demands routes absent from the loaded quantization and shapes.
    pub fn initWithKernelJitForProfile(
        allocator: std.mem.Allocator,
        jit_config: kernel_jit.Config,
        profile: CapabilityProfile,
    ) !CudaCompute {
        return initWithKernelJitProfile(allocator, jit_config, profile, .{}, .dynamic);
    }

    pub fn initWithKernelJitForScope(
        allocator: std.mem.Allocator,
        jit_config: kernel_jit.Config,
        profile: CapabilityProfile,
        scope: KernelJitRouteScope,
    ) !CudaCompute {
        return initWithKernelJitForScopeAndLoadContext(
            allocator,
            jit_config,
            profile,
            scope,
            .dynamic,
        );
    }

    pub fn initWithKernelJitForScopeAndLoadContext(
        allocator: std.mem.Allocator,
        jit_config: kernel_jit.Config,
        profile: CapabilityProfile,
        scope: KernelJitRouteScope,
        load_context: kernel_jit.LoadContext,
    ) !CudaCompute {
        return initWithKernelJitProfile(allocator, jit_config, profile, scope, load_context);
    }

    fn initWithKernelJitProfile(
        allocator: std.mem.Allocator,
        jit_config: kernel_jit.Config,
        profile: ?CapabilityProfile,
        scope: KernelJitRouteScope,
        load_context: kernel_jit.LoadContext,
    ) !CudaCompute {
        try jit_config.validate();
        if (jit_config.mode.failClosed() and
            (!scope.enabled() or !scope.conformance_complete or
                !load_context.allowsQualification()))
        {
            const err = error.CudaJitRequiredRouteFailed;
            if (profile) |value| logKernelJitFailure(jit_config, value, scope, load_context, err);
            return err;
        }
        var ctx = try context_mod.CudaContext.initDefault();
        errdefer ctx.deinit();
        const kernels = if (profile) |value|
            kernels_mod.KernelModule.loadWithKernelJitForScopeAndLoadContext(
                &ctx,
                allocator,
                jit_config,
                jitModelProfile(value),
                scope,
                load_context,
            ) catch |err| {
                logKernelJitFailure(jit_config, value, scope, load_context, err);
                return err;
            }
        else
            try kernels_mod.KernelModule.loadWithKernelJitAndLoadContext(
                &ctx,
                allocator,
                jit_config,
                load_context,
            );
        errdefer {
            var kernels_mut = kernels;
            kernels_mut.unload(&ctx);
        }
        const cublaslt_bf16_tuning = try CublasLtBf16TuningProfile.resolve(
            ctx.info.compute_major,
            ctx.info.compute_minor,
        );
        var cublaslt_bf16_tuning_profile = cublaslt_bf16_tuning.profile;
        const ple_gate_prefill_profile = try PleGatePrefillProfile.resolve(
            ctx.info.compute_major,
            ctx.info.compute_minor,
        );
        const sm89_q4_0_ggml_q8_1_gate = try Sm89Q4_0GgmlQ8_1Gate.resolve();
        var cublaslt = initCublasLtIfAvailable(allocator, &ctx);
        errdefer if (cublaslt) |*blas| blas.deinit();
        if (cublaslt_bf16_tuning_profile != .off and cublaslt == null) {
            // An explicit env request fails closed; the promoted SM89 default
            // degrades to off so capability-only inits keep working without
            // the cuBLASLt library.
            if (cublaslt_bf16_tuning.from_env) {
                std.log.err(
                    "cuda_cublaslt_bf16_tuning: status=unavailable profile=sm89-prefill",
                    .{},
                );
                return error.InvalidCudaState;
            }
            std.log.info(
                "cuda_cublaslt_bf16_tuning: status=disabled source=default reason=cublaslt_unavailable",
                .{},
            );
            cublaslt_bf16_tuning_profile = .off;
        }
        // Warm up only when a BF16 matmul path is actually enabled: the
        // ~100ms cuBLASLt library load should not tax every backend init
        // (parity harnesses construct dozens of CudaCompute instances).
        // The default-on Q4->BF16 prefill mirror only attaches to
        // encoder.layer.* weights on the qualified target, so its warmup is
        // scoped to the encoder profiles that carry them; profile-less
        // capability probes stay cold.
        const encoder_bf16_prefill_default = if (profile) |value| switch (value) {
            .bert_encoder, .deberta_reranker, .gliner2 => cudaBertQ4Bf16PrefillEnabled(
                cudaQualifiedPerfTarget(ctx.info.compute_major, ctx.info.compute_minor),
            ),
            else => false,
        } else false;
        if (cublaslt != null and cudaCublasLtWarmupEnabled() and
            (cudaDequantizeQ4_0MatrixWeightsToBf16OnUpload() or
                cudaHybridQ4Bf16WeightsEnabled() or
                cudaRmsNormBf16MirrorEnabled() or
                cudaPleModelProjectionBf16OnUpload() or
                encoder_bf16_prefill_default))
        {
            warmupCublasLtBf16(&cublaslt.?, &ctx);
        }
        const generated_q4_0_gates = if (jit_config.mode.activates()) blk: {
            var gates = GeneratedQ4_0Gates.fromJitRoutes(kernels.runtime_jit_routes);
            // The pair-only E2B route uses checked-in, shape-specific AOT
            // artifacts rather than a production JIT route. Keep its explicit
            // candidate gate available in JIT mode, but retain the same target
            // qualification and master-disable contract as bundled dispatch.
            gates.e2b_pair_only = (GeneratedQ4_0Gates{
                .e2b_pair_only = cudaQ4_0GeneratedE2BFfnPairOnlyEnabled(),
            }).restrictToPromotedTarget(
                ctx.info.compute_major,
                ctx.info.compute_minor,
                platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_ALLOW_UNPROMOTED_GENERATED_KERNELS", false),
            ).e2b_pair_only;
            break :blk gates.withExplicitDisables();
        } else GeneratedQ4_0Gates.resolveForTarget(ctx.info.compute_major, ctx.info.compute_minor);
        const tuned_route_gates = CudaTunedRouteGates.resolveForTarget(
            ctx.info.compute_major,
            ctx.info.compute_minor,
        );
        const capture_gates = CudaCaptureGateConfig.resolveForTarget(
            ctx.info.compute_major,
            ctx.info.compute_minor,
        );
        if (profile) |value| logKernelJitCompletion(jit_config, value, scope, load_context, &kernels);
        return .{
            .allocator = allocator,
            .ctx = ctx,
            .kernels = kernels,
            .cublaslt = cublaslt,
            .q4_route_census_enabled = cudaQ4RouteCensusEnabled(),
            .bf16_mirror_row_selector = Bf16MirrorRowSelector.resolve(),
            .ple_gate_prefill_profile = ple_gate_prefill_profile,
            .cublaslt_bf16_tuning_profile = cublaslt_bf16_tuning_profile,
            .sm89_q4_0_ggml_q8_1_gate = sm89_q4_0_ggml_q8_1_gate,
            .generated_q4_0_gates = generated_q4_0_gates,
            .tuned_route_gates = tuned_route_gates,
            .capture_gates = capture_gates,
            .temp_slot_config = CudaTempSlotConfig.resolve(capture_gates),
            .runtime_jit_shape_gating = jit_config.mode.activates(),
        };
    }

    pub fn create(allocator: std.mem.Allocator) !*CudaCompute {
        const self = try allocator.create(CudaCompute);
        errdefer allocator.destroy(self);
        self.* = try CudaCompute.init(allocator);
        self.owned_by_backend = true;
        return self;
    }

    pub fn deinit(self: *CudaCompute) void {
        self.dispatch_stats.printIfEnabled(self.q4_route_census_enabled);
        self.dispatch_stats.deinit(self.allocator);
        deinitProfilePool(self);
        deinitDenseHostPrefetch(self);
        if (self.lazy_host_store) |store| {
            native_compute_mod.stopPrefetchWorker(store);
        }
        self.decoder_runtime_linear_slots.deinit(self.allocator);
        self.decoder_runtime_rms_norm_slots.deinit(self.allocator);
        var pair_mirror_it = self.bf16_pair_mirror_cache.valueIterator();
        while (pair_mirror_it.next()) |buf| buf.free(&self.ctx);
        self.bf16_pair_mirror_cache.deinit(self.allocator);
        var it = self.resident_weights.iterator();
        while (it.next()) |entry| {
            var tensor = entry.value_ptr.*;
            tensor.owns_buffer = true;
            tensor.owns_shape = true;
            tensor.owns_bf16_mirror = true;
            freeCudaTensorStorage(self, &tensor);
            self.allocator.free(entry.key_ptr.*);
        }
        self.resident_weights.deinit(self.allocator);
        var lazy_it = self.lazy_device_epochs.iterator();
        while (lazy_it.next()) |entry| self.allocator.free(entry.key_ptr.*);
        self.lazy_device_epochs.deinit(self.allocator);
        deinitDenseStreamCache(self);
        for (&self.debug_cuda_graph_slots) |*slot| {
            deinitCudaGraphReplaySlot(self, slot);
        }
        self.debug_cuda_decode_scalars.free(&self.ctx);
        self.pinned_scalar_upload_ring.host.free(&self.ctx);
        self.pinned_upload_arena.host.free(&self.ctx);
        self.pinned_scalar_download_buffer.host.free(&self.ctx);
        self.pinned_bulk_download_buffer.free(&self.ctx);
        if (self.async_i32_scalar_download.in_use and self.async_i32_scalar_download.event != null) {
            self.ctx.driver.check(self.ctx.driver.fns.cuEventSynchronize(self.async_i32_scalar_download.event)) catch {};
            self.async_i32_scalar_download.in_use = false;
        }
        self.async_i32_scalar_download.staging.free(&self.ctx);
        self.async_i32_scalar_download.host.free(&self.ctx);
        self.ctx.destroyEvent(self.async_i32_scalar_download.ready_event);
        self.ctx.destroyEvent(self.async_i32_scalar_download.event);
        if (self.async_i32_scalar_download.copy_stream != null) {
            _ = self.ctx.driver.fns.cuStreamDestroy(self.async_i32_scalar_download.copy_stream);
            self.async_i32_scalar_download.copy_stream = null;
        }
        if (self.florence_tail_row_f32) |tensor| freeTensor(self, tensor);
        if (self.florence_tail_col_f32) |tensor| freeTensor(self, tensor);
        if (self.florence_tail_temporal_f32) |tensor| freeTensor(self, tensor);
        if (self.florence_image_projection_t_f32) |tensor| freeTensor(self, tensor);
        for (self.temp_pinned_slots.items) |*slot| slot.buffer.free(&self.ctx);
        self.temp_pinned_slots.deinit(self.allocator);
        self.temp_arena_planner.deinit(self.allocator);
        self.generated_gqa_score_prework_templates.deinit(self.allocator);
        for (self.temp_buffers.items) |*buffer| buffer.free(&self.ctx);
        self.temp_buffers.deinit(self.allocator);
        if (self.deferred_device_frees.items.len != 0) {
            synchronizeAndDrainDeferredDeviceFrees(self) catch {};
            drainDeferredDeviceFreesAfterSyncUnlocked(self);
        }
        self.deferred_device_frees.deinit(self.allocator);
        self.temp_ids_masks.deinit(&self.ctx);
        self.attention_mask_scratch.deinit(&self.ctx);
        self.attention_mask_cache_host.deinit(self.allocator);
        self.bf16_activation_scratch.deinit(&self.ctx);
        self.f16_activation_scratch.deinit(&self.ctx);
        self.deberta_q_f16_scratch.deinit(&self.ctx);
        self.deberta_k_f16_scratch.deinit(&self.ctx);
        self.deberta_v_f16_scratch.deinit(&self.ctx);
        self.deberta_qr_f16_scratch.deinit(&self.ctx);
        self.deberta_kr_f16_scratch.deinit(&self.ctx);
        self.deberta_materialized_scratch.deinit(&self.ctx);
        self.cublaslt_workspace_scratch.deinit(&self.ctx);
        if (self.cublaslt) |*blas| {
            blas.deinit();
            self.cublaslt = null;
        }
        self.kernels.unload(&self.ctx);
        self.ctx.deinit();
    }

    pub fn computeBackend(self: *CudaCompute) ops.ComputeBackend {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Bind a request-scoped RunBudget for the lifetime of the returned
    /// backend handle. CudaCompute is shared session state, so the budget
    /// pointer must not outlive the request: the handle's deinitBackend
    /// unbinds it. Requests on one session are serialized, matching every
    /// other unsynchronized per-request field on this struct.
    pub fn computeBackendWithScopedRunBudget(
        self: *CudaCompute,
        run_budget: *run_memory.RunBudget,
    ) ops.ComputeBackend {
        self.configureRunBudget(run_budget);
        return .{ .ptr = self, .vtable = &scoped_run_budget_vtable };
    }

    pub fn deviceMemoryInfo(self: *CudaCompute) !context_mod.DeviceMemoryInfo {
        return self.ctx.memoryInfo();
    }

    pub fn hasGemma4DecoderPrimitives(self: *const CudaCompute) bool {
        return self.kernels.hasGemma4DecoderPrimitives();
    }

    pub fn supportsProfile(self: *const CudaCompute, profile: CapabilityProfile) bool {
        return switch (profile) {
            .clipclap => self.kernels.hasClipClapPrimitives(),
            // BERT/XLM-R uses the same dense encoder primitives as CLIP text,
            // plus the Q4_0 biased-linear adapter in this compute backend.
            .bert_encoder => self.kernels.hasClipClapPrimitives(),
            .deberta_reranker => self.kernels.hasDebertaRerankerPrimitives(),
            .gliner2 => self.kernels.hasGliner2Primitives(),
            .gliner2_training => self.kernels.hasGliner2TrainingPrimitives(),
            .florence2 => self.kernels.hasFlorence2Primitives(),
            .gemma4 => self.kernels.hasQuantMatmulMvpPrimitives() and
                self.kernels.hasBf16WeightPrimitives() and
                (self.kernels.hasGemma4DecoderPrimitives() or cudaAllowHostAttentionFallback()),
        };
    }

    pub fn requireProfile(self: *const CudaCompute, profile: CapabilityProfile) !void {
        if (!self.supportsProfile(profile)) return error.CudaKernelUnavailable;
    }

    pub fn snapshotStats(self: *const CudaCompute) RuntimeStats {
        var stats = self.stats;
        stats.kernel_launches = self.ctx.stats.kernel_launches;
        stats.stream_syncs = self.ctx.stats.stream_syncs;
        for (self.temp_buffers.items) |buffer| {
            stats.temp_buffer_cached_bytes += buffer.len;
        }
        for (self.temp_pinned_slots.items) |slot| {
            stats.temp_buffer_cached_bytes += slot.buffer.len;
        }
        stats.cuda_temp_arena_plan_observations = self.temp_arena_planner.observations;
        stats.cuda_temp_arena_plan_activations = self.temp_arena_planner.activations;
        stats.cuda_temp_arena_plan_invalidations = self.temp_arena_planner.invalidations;
        stats.cuda_temp_arena_plan_slots = self.temp_arena_planner.period();
        stats.cuda_temp_arena_plan_physical_high_water_bytes = self.temp_arena_planner.planned_physical_high_water_bytes;
        stats.cuda_temp_arena_plan_peak_physical_high_water_bytes = self.temp_arena_planner.peak_planned_physical_high_water_bytes;
        stats.cuda_temp_arena_plan_admission_budget_bytes = self.temp_arena_planner.admission_budget_bytes;
        stats.cuda_temp_arena_plan_admission_denials = self.temp_arena_planner.admission_denials;
        stats.deferred_free_pending_bytes = self.deferred_device_free_bytes;
        return stats;
    }

    pub fn attachLazyHostStore(self: *CudaCompute, store: *native_compute_mod.WeightStore) !void {
        self.lazy_host_store = store;
        try self.resident_weights.ensureTotalCapacity(
            self.allocator,
            self.resident_weights.count() + store.lazy_weights.count() + 16,
        );
        try installCudaLazyHostPrefetch(self, store);
        try initDenseHostPrefetch(self);
    }

    pub fn configureRunBudget(self: *CudaCompute, run_budget: ?*run_memory.RunBudget) void {
        self.run_budget = run_budget;
        const default_lazy_budget: usize = 10 * 1024 * 1024 * 1024;
        // Passing null unbinds a request-scoped budget: restore the
        // unbudgeted field defaults (0 disables lazy eviction) rather than
        // leaving a stale cap on the shared compute.
        self.lazy_device_budget_bytes = if (run_budget) |budget| blk: {
            const backend_limit = budget.limits.backend_limit_bytes;
            if (backend_limit == 0) break :blk default_lazy_budget;
            const reserved = budget.backend_kv_bytes + budget.backend_scratch_bytes;
            break :blk if (backend_limit > reserved) backend_limit - reserved else 0;
        } else 0;
    }

    fn noteDeviceBytes(self: *CudaCompute, bytes: usize) void {
        self.stats.device_allocated_bytes += bytes;
    }

    fn insertBf16WeightFromQuantizedStorage(self: *CudaCompute, owned_key: []const u8, storage: weight_source_mod.QuantizedStorage) !void {
        const elem_count = try elementCountFromShape(storage.shape);
        const f32_data = try self.allocator.alloc(f32, elem_count);
        defer self.allocator.free(f32_data);
        try quant_codec.dequantizeToFloat32(storage.tensor_type, storage.raw_bytes, f32_data);

        const bf16_data = try self.allocator.alloc(u16, elem_count);
        defer self.allocator.free(bf16_data);
        for (f32_data, bf16_data) |value, *dst| dst.* = f32ToBf16BitsRoundNearestEven(value);

        const shape = try self.allocator.dupe(i64, storage.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, bf16_data.len * @sizeOf(u16));
        errdefer device.free(&self.ctx);
        try copyFromHostTracked(self, device, std.mem.sliceAsBytes(bf16_data));
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.resident_weight_bytes += bf16_data.len * @sizeOf(u16);
        errdefer self.allocator.free(owned_key);
        try self.resident_weights.put(self.allocator, owned_key, .{
            .buffer = device,
            .dtype = .bf16,
            .shape = shape,
            .elem_count = elem_count,
            .quant_type = null,
            .owns_buffer = false,
            .owns_shape = false,
            .owned_by_tensor = false,
        });
    }

    pub fn insertWeightFromLoaded(self: *CudaCompute, owned_key: []const u8, loaded: *const weight_source_mod.LoadedWeight) !void {
        if (loaded.quantized_storage) |storage| {
            if (cudaShouldDequantizeQ4_0MatrixWeightToBf16OnUpload(owned_key, storage)) {
                return self.insertBf16WeightFromQuantizedStorage(owned_key, storage);
            }
            if (cudaDequantizeQuantWeightsOnUpload() or cudaShouldDequantizeWeightOnUpload(owned_key, storage)) {
                const elem_count = try elementCountFromShape(storage.shape);
                const data = try self.allocator.alloc(f32, elem_count);
                defer self.allocator.free(data);
                try quant_codec.dequantizeToFloat32(storage.tensor_type, storage.raw_bytes, data);

                const shape = try self.allocator.dupe(i64, storage.shape);
                errdefer self.allocator.free(shape);
                var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
                errdefer device.free(&self.ctx);
                try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
                try synchronizeAndDrainDeferredDeviceFrees(self);
                self.stats.resident_weight_bytes += data.len * @sizeOf(f32);
                errdefer self.allocator.free(owned_key);
                try self.resident_weights.put(self.allocator, owned_key, .{
                    .buffer = device,
                    .dtype = .f32,
                    .shape = shape,
                    .elem_count = data.len,
                    .quant_type = null,
                    .owns_buffer = false,
                    .owns_shape = false,
                    .owned_by_tensor = false,
                });
                return;
            }
            const elem_count = try elementCountFromShape(storage.shape);
            const shape = try self.allocator.dupe(i64, storage.shape);
            errdefer self.allocator.free(shape);
            var device = try allocDeviceBuffer(self, storage.raw_bytes.len);
            errdefer device.free(&self.ctx);
            try copyFromHostTracked(self, device, storage.raw_bytes);
            var tc_quant = try self.prepareTensorCoreQuantOnUpload(storage.tensor_type, storage.shape, storage.raw_bytes);
            errdefer if (tc_quant) |*packed_quant| releaseDeviceBuffer(self, &packed_quant.buffer);
            var bf16_mirror = buffer_mod.DeviceBuffer{};
            errdefer bf16_mirror.free(&self.ctx);
            if (cudaShouldAttachBf16MirrorToQ4Weight(self, owned_key, storage)) {
                bf16_mirror = try allocDeviceBuffer(self, elem_count * @sizeOf(u16));
                // Dequantize on device from the raw Q4_0 bytes already
                // uploaded above — bit-identical to the host path and skips
                // both the single-threaded host dequant and a second PCIe
                // upload twice the size of the Q4 data.
                var device_dequantized = false;
                if (cudaDeviceMirrorDequantEnabled() and elem_count % 32 == 0) {
                    if (self.kernels.launchDequantQ4_0Bf16(&self.ctx, bf16_mirror, device, elem_count / 32)) {
                        device_dequantized = true;
                    } else |_| {}
                }
                if (!device_dequantized) {
                    const f32_data = try self.allocator.alloc(f32, elem_count);
                    defer self.allocator.free(f32_data);
                    try quant_codec.dequantizeToFloat32(storage.tensor_type, storage.raw_bytes, f32_data);
                    const bf16_data = try self.allocator.alloc(u16, elem_count);
                    defer self.allocator.free(bf16_data);
                    for (f32_data, bf16_data) |value, *dst| dst.* = f32ToBf16BitsRoundNearestEven(value);
                    try copyFromHostTracked(self, bf16_mirror, std.mem.sliceAsBytes(bf16_data));
                }
                self.stats.resident_weight_bytes += elem_count * @sizeOf(u16);
                self.stats.bf16_mirror_weight_count += 1;
                self.stats.bf16_mirror_weight_bytes += elem_count * @sizeOf(u16);
            }
            try synchronizeAndDrainDeferredDeviceFrees(self);
            self.stats.resident_weight_bytes += storage.raw_bytes.len;
            errdefer self.allocator.free(owned_key);
            try self.resident_weights.put(self.allocator, owned_key, .{
                .buffer = device,
                .dtype = .u8,
                .shape = shape,
                .elem_count = elem_count,
                .quant_type = storage.tensor_type,
                .tc_quant = tc_quant,
                .bf16_mirror = bf16_mirror,
                .owns_buffer = false,
                .owns_shape = false,
                .owns_bf16_mirror = false,
                .owned_by_tensor = false,
            });
            return;
        }
        if (loaded.quantized) return error.UnsupportedTensorType;
        if (loaded.tensor.dtype == .bf16 and loaded.tensor.shape.len >= 2) {
            return self.insertBf16WeightFromTensor(owned_key, &loaded.tensor);
        }
        // Keep matrix-shaped FP16 GGUF tensors in their native representation
        // only when the complete tensor-core route is available. Rank-one
        // parameters intentionally continue through F32: norms, biases, and
        // scalar graph operators retain their existing numerical behavior.
        if (loaded.tensor.dtype == .f16 and loaded.tensor.shape.len >= 2 and
            cudaShouldKeepF16WeightOnDevice(owned_key) and
            canUseF16TensorCoreWeights(self))
        {
            return self.insertF16WeightFromTensor(owned_key, &loaded.tensor);
        }
        if (loaded.tensor.dtype == .f32 and cudaShouldConvertF32WeightToBf16OnUpload(owned_key, &loaded.tensor)) {
            return self.insertBf16WeightFromF32Tensor(owned_key, &loaded.tensor);
        }
        if (loaded.tensor.dtype != .f32) {
            var converted = try weight_source_mod.convertToF32(self.allocator, &loaded.tensor);
            defer converted.deinit();
            if (cudaShouldConvertF32WeightToBf16OnUpload(owned_key, &converted)) {
                return self.insertBf16WeightFromF32Tensor(owned_key, &converted);
            }
            return self.insertWeightFromTensor(owned_key, &converted);
        }
        try self.insertWeightFromTensor(owned_key, &loaded.tensor);
    }

    fn prepareTensorCoreQuantOnUpload(
        self: *CudaCompute,
        tensor_type: gguf_tensor_types.TensorType,
        shape: []const i64,
        raw_bytes: []const u8,
    ) !?CudaTensorCoreQuantBuffer {
        if (!cudaTensorCoreQuantRequested()) return null;
        if (!cudaTensorCoreQuantAvailable(self)) return null;
        if (shape.len != 2 or shape[0] <= 0 or shape[1] <= 0) return null;

        const out_dim: usize = @intCast(shape[0]);
        const in_dim: usize = @intCast(shape[1]);

        const known = knownQuantTensorType(tensor_type) orelse return null;
        // Q4_0 W4A16 (opt-in) uses a broader WMMA-tile shape gate covering the
        // gemma4 decoder projections, which the encoder-only approved-shape
        // whitelist excludes. Other formats keep the whitelist.
        const q4_0_tc = known == .Q4_0 and self.tuned_route_gates.q4_0_tc_hmma_prefill;
        if (q4_0_tc) {
            if (!isQ4_0TcHmmaShape(in_dim, out_dim)) return null;
        } else if (!isTensorCoreQuantLinearShape(in_dim, out_dim)) {
            return null;
        }

        const packed_quant = switch (known) {
            .Q8_0 => try packQ8_0TensorCore(self.allocator, raw_bytes, in_dim, out_dim),
            .Q4_K => try packQ4_KTensorCore(self.allocator, raw_bytes, in_dim, out_dim),
            // Q4_0 tensor-core packing is opt-in (default off): it adds a packed
            // mirror alongside the raw weight, so only materialize it when the
            // W4A16 prefill route is enabled and being A/B'd against cuBLASLt.
            .Q4_0 => if (q4_0_tc)
                try packQ4_0TensorCore(self.allocator, raw_bytes, in_dim, out_dim)
            else
                return null,
            else => return null,
        };
        defer self.allocator.free(packed_quant.bytes);

        var device = try allocDeviceBuffer(self, packed_quant.bytes.len);
        errdefer device.free(&self.ctx);
        try copyFromHostTracked(self, device, packed_quant.bytes);
        self.dispatch_stats.note(
            self.allocator,
            .tc_pack,
            switch (known) {
                .Q8_0 => .q8_0,
                .Q4_K => .q4_k,
                .Q4_0 => .q4_0,
                else => .none,
            },
            .tc_pack,
            .none,
            .none,
            0,
            in_dim,
            out_dim,
            packed_quant.bytes.len,
        );
        return .{
            .buffer = device,
            .layout = packed_quant.layout,
            .row_blocks = packed_quant.row_blocks,
            .bytes = packed_quant.bytes.len,
        };
    }

    pub fn insertBf16WeightFromTensor(self: *CudaCompute, owned_key: []const u8, tensor: *const tensor_mod.Tensor) !void {
        if (tensor.dtype != .bf16) return error.UnsupportedTensorType;
        const elem_count = tensor.elementCount();
        if (tensor.data.len != elem_count * @sizeOf(u16)) return error.InvalidShape;
        const shape = try self.allocator.dupe(i64, tensor.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, tensor.data.len);
        errdefer device.free(&self.ctx);
        try copyFromHostTracked(self, device, tensor.data);
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.resident_weight_bytes += tensor.data.len;
        errdefer self.allocator.free(owned_key);
        try self.resident_weights.put(self.allocator, owned_key, .{
            .buffer = device,
            .dtype = .bf16,
            .shape = shape,
            .elem_count = elem_count,
            .quant_type = null,
            .owns_buffer = false,
            .owns_shape = false,
            .owned_by_tensor = false,
        });
    }

    pub fn insertF16WeightFromTensor(self: *CudaCompute, owned_key: []const u8, tensor: *const tensor_mod.Tensor) !void {
        if (tensor.dtype != .f16) return error.UnsupportedTensorType;
        const elem_count = tensor.elementCount();
        if (tensor.data.len != elem_count * @sizeOf(u16)) return error.InvalidShape;
        const shape = try self.allocator.dupe(i64, tensor.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, tensor.data.len);
        errdefer device.free(&self.ctx);
        try copyFromHostTracked(self, device, tensor.data);
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.resident_weight_bytes += tensor.data.len;
        errdefer self.allocator.free(owned_key);
        try self.resident_weights.put(self.allocator, owned_key, .{
            .buffer = device,
            .dtype = .f16,
            .shape = shape,
            .elem_count = elem_count,
            .quant_type = null,
            .owns_buffer = false,
            .owns_shape = false,
            .owned_by_tensor = false,
        });
    }

    pub fn insertBf16WeightFromF32Tensor(self: *CudaCompute, owned_key: []const u8, tensor: *const tensor_mod.Tensor) !void {
        if (tensor.dtype != .f32) return error.UnsupportedTensorType;
        const data = tensor.asFloat32();
        const bf16_data = try self.allocator.alloc(u16, data.len);
        defer self.allocator.free(bf16_data);
        for (data, bf16_data) |value, *dst| dst.* = f32ToBf16BitsRoundNearestEven(value);

        const shape = try self.allocator.dupe(i64, tensor.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, bf16_data.len * @sizeOf(u16));
        errdefer device.free(&self.ctx);
        try copyFromHostTracked(self, device, std.mem.sliceAsBytes(bf16_data));
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.resident_weight_bytes += bf16_data.len * @sizeOf(u16);
        errdefer self.allocator.free(owned_key);
        try self.resident_weights.put(self.allocator, owned_key, .{
            .buffer = device,
            .dtype = .bf16,
            .shape = shape,
            .elem_count = data.len,
            .quant_type = null,
            .owns_buffer = false,
            .owns_shape = false,
            .owned_by_tensor = false,
        });
    }

    pub fn insertWeightFromTensor(self: *CudaCompute, owned_key: []const u8, tensor: *const tensor_mod.Tensor) !void {
        if (tensor.dtype != .f32) return error.UnsupportedTensorType;
        const data = tensor.asFloat32();
        const shape = try self.allocator.dupe(i64, tensor.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
        errdefer device.free(&self.ctx);
        try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
        var bf16_mirror = buffer_mod.DeviceBuffer{};
        errdefer bf16_mirror.free(&self.ctx);
        if (cudaHybridQ4Bf16WeightsEnabled() and isPleModelProjectionWeightName(owned_key) and tensor.shape.len == 2) {
            // Hybrid residency for the F32 PLE projection: keep F32 for the
            // decode path (graph-capture friendly) and a BF16 copy for
            // prefill cuBLASLt. Converted on device from the F32 weight
            // uploaded above (bit-identical RNE); host fallback below.
            bf16_mirror = try allocDeviceBuffer(self, data.len * @sizeOf(u16));
            var device_converted = false;
            if (cudaDeviceMirrorDequantEnabled()) {
                if (self.kernels.launchF32ToBf16(&self.ctx, bf16_mirror, device, data.len)) {
                    device_converted = true;
                } else |_| {}
            }
            if (!device_converted) {
                const bf16_data = try self.allocator.alloc(u16, data.len);
                defer self.allocator.free(bf16_data);
                for (data, bf16_data) |value, *dst| dst.* = f32ToBf16BitsRoundNearestEven(value);
                try copyFromHostTracked(self, bf16_mirror, std.mem.sliceAsBytes(bf16_data));
            }
            self.stats.resident_weight_bytes += data.len * @sizeOf(u16);
        }
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.resident_weight_bytes += data.len * @sizeOf(f32);
        errdefer self.allocator.free(owned_key);
        try self.resident_weights.put(self.allocator, owned_key, .{
            .buffer = device,
            .dtype = .f32,
            .shape = shape,
            .elem_count = data.len,
            .quant_type = null,
            .bf16_mirror = bf16_mirror,
            .owns_buffer = false,
            .owns_shape = false,
            .owns_bf16_mirror = false,
            .owned_by_tensor = false,
        });
    }
};

fn expectApproxSlice(actual: []const f32, expected: []const f32, tolerance: f32) !void {
    if (actual.len != expected.len) return error.CudaParityMismatch;
    for (actual, expected) |got, want| {
        if (@abs(got - want) > tolerance) return error.CudaParityMismatch;
    }
}

pub fn smokeDecoderRuntimeSlots(allocator: std.mem.Allocator) !void {
    var compute = try CudaCompute.init(allocator);
    defer compute.deinit();
    var cb = compute.computeBackend();

    const linear_shape = [_]i64{ 2, 2 };
    const linear_weight_data = [_]f32{ 1.0, 0.0, 0.0, 1.0 };
    var linear_weight = try tensor_mod.Tensor.initFloat32(allocator, "slot.linear.identity", &linear_shape, &linear_weight_data);
    defer linear_weight.deinit();
    try compute.insertWeightFromTensor(try allocator.dupe(u8, "slot.linear.identity"), &linear_weight);

    const norm_shape = [_]i64{2};
    const norm_weight_data = [_]f32{ 1.0, 1.0 };
    var norm_weight = try tensor_mod.Tensor.initFloat32(allocator, "slot.norm.ones", &norm_shape, &norm_weight_data);
    defer norm_weight.deinit();
    try compute.insertWeightFromTensor(try allocator.dupe(u8, "slot.norm.ones"), &norm_weight);

    const linear_weight_ct = try cb.getWeight("slot.linear.identity");
    const norm_weight_ct = try cb.getWeight("slot.norm.ones");
    const linear_slot = (try cb.decoderRuntimeEnsureLinearSlot(&.{
        .weight = linear_weight_ct,
        .bias = null,
        .in_dim = 2,
        .out_dim = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    const norm_slot = (try cb.decoderRuntimeEnsureRmsNormSlot(&.{
        .weight = norm_weight_ct,
        .hidden_size = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;

    const input_shape = [_]i32{ 1, 2 };
    const input_data = [_]f32{ 3.0, 4.0 };
    const input = try cb.fromFloat32Shape(&input_data, &input_shape);
    defer cb.free(input);

    const linear_out = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = linear_slot,
        .input = input,
        .in_dim = 2,
        .out_dim = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    defer cb.free(linear_out);
    const linear_actual = try cb.toFloat32(linear_out, allocator);
    defer allocator.free(linear_actual);
    try expectApproxSlice(linear_actual, &input_data, 0.0001);

    const pair_out = (try cb.decoderRuntimeApplyLinearPair(&.{
        .slot_a = linear_slot,
        .slot_b = linear_slot,
        .input = input,
        .in_dim = 2,
        .out_dim = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    defer cb.free(pair_out.first);
    defer cb.free(pair_out.second);
    const pair_first = try cb.toFloat32(pair_out.first, allocator);
    defer allocator.free(pair_first);
    const pair_second = try cb.toFloat32(pair_out.second, allocator);
    defer allocator.free(pair_second);
    try expectApproxSlice(pair_first, &input_data, 0.0001);
    try expectApproxSlice(pair_second, &input_data, 0.0001);

    const qkv_out = (try cb.decoderRuntimeApplyLinearQkv(&.{
        .q_slot = linear_slot,
        .k_slot = linear_slot,
        .v_slot = linear_slot,
        .input = input,
        .in_dim = 2,
        .q_out_dim = 2,
        .kv_out_dim = 2,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    defer cb.free(qkv_out.first);
    defer cb.free(qkv_out.second);
    defer cb.free(qkv_out.third);
    const q_actual = try cb.toFloat32(qkv_out.first, allocator);
    defer allocator.free(q_actual);
    const k_actual = try cb.toFloat32(qkv_out.second, allocator);
    defer allocator.free(k_actual);
    const v_actual = try cb.toFloat32(qkv_out.third, allocator);
    defer allocator.free(v_actual);
    try expectApproxSlice(q_actual, &input_data, 0.0001);
    try expectApproxSlice(k_actual, &input_data, 0.0001);
    try expectApproxSlice(v_actual, &input_data, 0.0001);

    const norm_input_data = [_]f32{ 1.0, 1.0 };
    const norm_input = try cb.fromFloat32Shape(&norm_input_data, &input_shape);
    defer cb.free(norm_input);
    const norm_out = (try cb.decoderRuntimeApplyRmsNorm(&.{
        .slot = norm_slot,
        .input = norm_input,
        .hidden_size = 2,
        .eps = 0.000001,
    })) orelse return error.CudaRuntimeSlotUnavailable;
    defer cb.free(norm_out);
    const norm_actual = try cb.toFloat32(norm_out, allocator);
    defer allocator.free(norm_actual);
    try expectApproxSlice(norm_actual, &norm_input_data, 0.0001);

    const stats = compute.snapshotStats();
    if (stats.decoder_runtime_linear_slot_prepares == 0 or
        stats.decoder_runtime_rms_norm_slot_prepares == 0 or
        stats.decoder_runtime_linear_apply_hits < 6 or
        stats.decoder_runtime_rms_norm_apply_hits == 0)
    {
        return error.CudaRuntimeSlotUnavailable;
    }
}

const CudaKvLayer = struct {
    k: buffer_mod.DeviceBuffer = .{},
    v: buffer_mod.DeviceBuffer = .{},
    block_table: buffer_mod.DeviceBuffer = .{},
    capacity_tokens: usize = 0,
    token_count: usize = 0,
    row_width: usize = 0,
    key_row_bytes: usize = 0,
    base_key_row_bytes: usize = 0,
    value_row_bytes: usize = 0,
    compressed_format: ?u32 = null,
    value_format: u32 = cuda_kv_value_format_f32,
    page_size_tokens: u16 = 0,
    block_table_len: usize = 0,
    block_table_capacity: usize = 0,
    block_table_signature: u64 = 0,
    block_table_valid: bool = false,
    block_table_identity: bool = false,
    position_offset: usize = 0,

    fn deinit(self: *CudaKvLayer, compute: *CudaCompute) void {
        self.k.free(&compute.ctx);
        self.v.free(&compute.ctx);
        self.block_table.free(&compute.ctx);
        self.* = .{};
    }
};

fn uncompressedCudaKvRequiredCapacity(
    token_count: usize,
    persistent_replay: bool,
    forced_replay_capacity: ?usize,
) !usize {
    if (!persistent_replay) return token_count;
    const replay_capacity = try checkedAdd(token_count, 256);
    return @max(replay_capacity, forced_replay_capacity orelse 0);
}

fn cudaKvGrowthCapacity(
    required_capacity: usize,
    current_capacity: usize,
    page_size_tokens: u16,
) !usize {
    const doubled_capacity = try checkedMul(current_capacity, 2);
    const unaligned_capacity = @max(required_capacity, @max(@as(usize, 16), doubled_capacity));
    if (page_size_tokens == 0) return unaligned_capacity;

    // Compressed paged K/V addresses whole physical pages. Round the growth
    // policy after applying its minimum/doubling rules so every legal u16
    // page size—not only powers of two—preserves that allocation invariant.
    const page_size: usize = page_size_tokens;
    const page_count = std.math.divCeil(usize, unaligned_capacity, page_size) catch
        return error.InvalidShape;
    return checkedMul(page_count, page_size);
}

test "uncompressed CUDA KV capacity honors explicit replay force" {
    try std.testing.expectEqual(@as(usize, 300), try uncompressedCudaKvRequiredCapacity(300, false, 4096));
    try std.testing.expectEqual(@as(usize, 556), try uncompressedCudaKvRequiredCapacity(300, true, null));
    try std.testing.expectEqual(@as(usize, 556), try uncompressedCudaKvRequiredCapacity(300, true, 512));
    try std.testing.expectEqual(@as(usize, 4096), try uncompressedCudaKvRequiredCapacity(300, true, 4096));
    try std.testing.expectError(error.InvalidShape, uncompressedCudaKvRequiredCapacity(std.math.maxInt(usize), true, null));
}

test "compressed CUDA KV growth rounds arbitrary page sizes" {
    try std.testing.expectEqual(@as(usize, 18), try cudaKvGrowthCapacity(17, 0, 3));
    try std.testing.expectEqual(@as(usize, 20), try cudaKvGrowthCapacity(17, 0, 5));
    try std.testing.expectEqual(@as(usize, 27), try cudaKvGrowthCapacity(17, 13, 3));
    try std.testing.expectEqual(@as(usize, 30), try cudaKvGrowthCapacity(17, 13, 5));
    try std.testing.expectEqual(@as(usize, 26), try cudaKvGrowthCapacity(17, 13, 0));
    try std.testing.expectError(error.InvalidShape, cudaKvGrowthCapacity(1, std.math.maxInt(usize), 3));
}

fn traceCudaDeviceKvGatherFailure(
    reason: []const u8,
    gather: kv_storage_runtime.DeviceKvLayerGather,
    requested_row_width: usize,
    layer: ?*const CudaKvLayer,
) void {
    if (!platform.env.getenvBool("ANTFLY_CUDA_TRACE_DEVICE_KV")) return;
    if (layer) |existing| {
        std.debug.print(
            "cuda_device_kv_trace: gather_failed reason={s} sequence={d} layer={d} requested_tokens={d} requested_row_width={d} existing_tokens={d} existing_row_width={d} existing_capacity={d} existing_position_offset={d}\n",
            .{
                reason,
                gather.sequence_id,
                gather.layer_index,
                gather.token_count,
                requested_row_width,
                existing.token_count,
                existing.row_width,
                existing.capacity_tokens,
                existing.position_offset,
            },
        );
    } else {
        std.debug.print(
            "cuda_device_kv_trace: gather_failed reason={s} sequence={d} layer={d} requested_tokens={d} requested_row_width={d} existing_tokens=0 existing_row_width=0 existing_capacity=0 existing_position_offset=0\n",
            .{ reason, gather.sequence_id, gather.layer_index, gather.token_count, requested_row_width },
        );
    }
}

const CudaKvDeviceStorage = struct {
    allocator: std.mem.Allocator,
    compute: *CudaCompute,
    dtype: kv_pool_mod.KvDType,
    layers: std.AutoHashMapUnmanaged(u64, CudaKvLayer) = .{},

    fn create(allocator: std.mem.Allocator, compute: *CudaCompute, dtype: kv_pool_mod.KvDType) !*CudaKvDeviceStorage {
        const self = try allocator.create(CudaKvDeviceStorage);
        self.* = .{
            .allocator = allocator,
            .compute = compute,
            .dtype = dtype,
        };
        return self;
    }

    fn deinit(self: *CudaKvDeviceStorage) void {
        var it = self.layers.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.compute);
        }
        self.layers.deinit(self.allocator);
    }

    fn key(sequence_id: kv_storage_runtime.SequenceId, layer_index: usize) !u64 {
        if (layer_index > std.math.maxInt(u32)) return error.InvalidPagedKvState;
        return (@as(u64, sequence_id) << 32) | @as(u64, @intCast(layer_index));
    }

    fn layerKey(self: *const CudaKvDeviceStorage, sequence_id: kv_storage_runtime.SequenceId, layer_index: usize) !u64 {
        if (cudaPagedKvFormat(self.dtype) != null) return key(0, layer_index);
        return key(sequence_id, layer_index);
    }

    fn neededBlockCount(token_count: usize, page_size_tokens: u16) !usize {
        if (token_count == 0 or page_size_tokens == 0) return 0;
        return std.math.divCeil(usize, token_count, page_size_tokens) catch error.InvalidPagedKvState;
    }

    fn physicalCapacityTokens(logical_blocks: []const kv_block_mod.KvBlockId, needed_blocks: usize, page_size_tokens: u16) !usize {
        if (needed_blocks == 0) return 0;
        if (logical_blocks.len < needed_blocks or page_size_tokens == 0) return error.InvalidPagedKvState;
        var max_block: usize = 0;
        for (logical_blocks[0..needed_blocks]) |block_id| {
            max_block = @max(max_block, @as(usize, block_id));
        }
        return checkedMul(try checkedAdd(max_block, 1), page_size_tokens);
    }

    fn blockTableUploadBlockCount(
        token_count: usize,
        page_size_tokens: u16,
        logical_blocks: []const kv_block_mod.KvBlockId,
    ) !usize {
        const needed_blocks = try neededBlockCount(token_count, page_size_tokens);
        if (needed_blocks == 0) return 0;
        if (logical_blocks.len < needed_blocks) return error.InvalidPagedKvState;
        const forced_tokens = cudaDebugGraphForcedKvReplayCapacityTokens() orelse return needed_blocks;
        const forced_blocks = try neededBlockCount(@max(token_count, forced_tokens), page_size_tokens);
        if (forced_blocks == 0 or logical_blocks.len < forced_blocks) return needed_blocks;
        return @max(needed_blocks, forced_blocks);
    }

    fn blockTableSignature(logical_blocks: []const kv_block_mod.KvBlockId, needed_blocks: usize, page_size_tokens: u16) u64 {
        var hash = std.hash.Wyhash.init(0x9d2166f7_3b6a1b89);
        hash.update(std.mem.asBytes(&page_size_tokens));
        const needed_blocks_u64: u64 = @intCast(needed_blocks);
        hash.update(std.mem.asBytes(&needed_blocks_u64));
        hash.update(std.mem.sliceAsBytes(logical_blocks[0..needed_blocks]));
        return hash.final();
    }

    fn blockTableIsIdentity(logical_blocks: []const kv_block_mod.KvBlockId, needed_blocks: usize) bool {
        for (logical_blocks[0..needed_blocks], 0..) |block_id, idx| {
            if (block_id != @as(kv_block_mod.KvBlockId, @intCast(idx))) return false;
        }
        return true;
    }

    fn ensureLayerBlockTable(
        self: *CudaKvDeviceStorage,
        layer: *CudaKvLayer,
        logical_blocks: []const kv_block_mod.KvBlockId,
        token_count: usize,
        page_size_tokens: u16,
    ) !void {
        const upload_blocks = try blockTableUploadBlockCount(token_count, page_size_tokens, logical_blocks);
        if (upload_blocks == 0) return;
        const block_table_bytes = try checkedMul(upload_blocks, @sizeOf(kv_block_mod.KvBlockId));
        const signature = blockTableSignature(logical_blocks, upload_blocks, page_size_tokens);
        const identity = blockTableIsIdentity(logical_blocks, upload_blocks);
        if (layer.block_table_valid and
            layer.block_table_len == upload_blocks and
            layer.page_size_tokens == page_size_tokens and
            layer.block_table_signature == signature)
        {
            return;
        }
        if (layer.block_table_capacity < upload_blocks) {
            try preparePersistentCudaBufferReallocation(self.compute);
            var new_table = try buffer_mod.DeviceBuffer.alloc(&self.compute.ctx, block_table_bytes);
            errdefer new_table.free(&self.compute.ctx);
            self.compute.noteDeviceBytes(block_table_bytes);
            layer.block_table.free(&self.compute.ctx);
            layer.block_table = new_table;
            layer.block_table_capacity = upload_blocks;
        }
        try copyFromHostTracked(self.compute, layer.block_table, std.mem.sliceAsBytes(logical_blocks[0..upload_blocks]));
        layer.block_table_len = upload_blocks;
        layer.page_size_tokens = page_size_tokens;
        layer.block_table_signature = signature;
        layer.block_table_valid = true;
        layer.block_table_identity = identity;
        self.compute.stats.device_kv_paged_block_table_uploads += 1;
        self.compute.stats.device_kv_paged_block_table_bytes += block_table_bytes;
    }

    fn ensureLayer(self: *CudaKvDeviceStorage, write: kv_storage_runtime.KvSuffixWrite) !*CudaKvLayer {
        const row_width = try checkedMul(@as(usize, write.num_kv_heads), @as(usize, write.head_dim));
        const compressed_format = cudaPagedKvFormat(self.dtype);
        const key_row_bytes = if (compressed_format != null)
            self.dtype.bytesForKeyRow(write.num_kv_heads, write.head_dim)
        else
            try checkedMul(row_width, @sizeOf(f32));
        if (key_row_bytes == 0) return error.DeviceWriteFormatUnsupported;
        const base_key_row_bytes = switch (self.dtype) {
            .turbo3 => kv_pool_mod.KvDType.turbo3.bytesForKeyRow(write.num_kv_heads, write.head_dim) -
                @as(usize, write.num_kv_heads) * ((32 + 7) / 8),
            else => key_row_bytes,
        };
        const value_format: u32 = if (compressed_format != null and self.dtype == .f16)
            cuda_kv_value_format_f16
        else if (compressed_format != null and cudaTurboquantInt4ValuesEnabled())
            cuda_kv_value_format_int4_group
        else if (compressed_format != null and !cudaTurboquantCompressedVDisabled())
            cuda_kv_value_format_int8_per_head
        else
            cuda_kv_value_format_f32;
        const value_row_bytes = switch (value_format) {
            cuda_kv_value_format_int8_per_head, cuda_kv_value_format_f16 => self.dtype.bytesForValueRow(write.num_kv_heads, write.head_dim),
            cuda_kv_value_format_int4_group => kv_pool_mod.KvDType.int4.bytesForValueRow(write.num_kv_heads, write.head_dim),
            else => try checkedMul(row_width, @sizeOf(f32)),
        };
        if (value_row_bytes == 0) return error.DeviceWriteFormatUnsupported;
        const layer_key = try self.layerKey(write.sequence_id, write.layer_index);
        const gop = try self.layers.getOrPut(self.allocator, layer_key);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        }
        const layer = gop.value_ptr;
        if (layer.row_width != 0 and layer.row_width != row_width) return error.InvalidKvShape;
        if (layer.key_row_bytes != 0 and layer.key_row_bytes != key_row_bytes) return error.InvalidKvShape;
        if (layer.value_row_bytes != 0 and layer.value_row_bytes != value_row_bytes) return error.InvalidKvShape;
        if (layer.key_row_bytes != 0 and layer.compressed_format != compressed_format) return error.InvalidKvShape;
        if (layer.key_row_bytes != 0 and layer.value_format != value_format) return error.InvalidKvShape;
        layer.row_width = row_width;
        layer.key_row_bytes = key_row_bytes;
        layer.base_key_row_bytes = base_key_row_bytes;
        layer.value_row_bytes = value_row_bytes;
        layer.compressed_format = compressed_format;
        layer.value_format = value_format;
        layer.page_size_tokens = write.page_size_tokens;
        layer.position_offset = write.position_offset;

        const required_capacity = if (compressed_format != null and write.logical_blocks != null and write.page_size_tokens != 0) blk: {
            const logical_blocks = write.logical_blocks.?;
            const upload_blocks = try blockTableUploadBlockCount(write.total_token_count, write.page_size_tokens, logical_blocks);
            break :blk try physicalCapacityTokens(logical_blocks, upload_blocks, write.page_size_tokens);
            // F32 KV is contiguous and does not derive its device allocation from
            // reserved logical blocks, so honor the explicit replay reserve here.
        } else if (compressed_format == null) blk: {
            break :blk try uncompressedCudaKvRequiredCapacity(
                write.total_token_count,
                cudaDebugGraphPersistentReplayEnabled(self.compute),
                cudaDebugGraphForcedKvReplayCapacityTokens(),
            );
        } else if (cudaDebugGraphPersistentReplayEnabled(self.compute))
            try std.math.add(usize, write.total_token_count, 256)
        else
            write.total_token_count;
        if (layer.capacity_tokens < required_capacity) {
            try preparePersistentCudaBufferReallocation(self.compute);
            const allocation_page_size = if (compressed_format != null) write.page_size_tokens else 0;
            const new_capacity = try cudaKvGrowthCapacity(required_capacity, layer.capacity_tokens, allocation_page_size);
            const k_bytes = try checkedMul(new_capacity, key_row_bytes);
            const v_bytes = try checkedMul(new_capacity, value_row_bytes);
            var new_k = try buffer_mod.DeviceBuffer.alloc(&self.compute.ctx, k_bytes);
            errdefer new_k.free(&self.compute.ctx);
            var new_v = try buffer_mod.DeviceBuffer.alloc(&self.compute.ctx, v_bytes);
            errdefer new_v.free(&self.compute.ctx);
            self.compute.noteDeviceBytes(try checkedAdd(k_bytes, v_bytes));
            if (layer.capacity_tokens != 0) {
                const old_k_bytes = try checkedMul(layer.capacity_tokens, key_row_bytes);
                const old_v_bytes = try checkedMul(layer.capacity_tokens, value_row_bytes);
                try new_k.copyFromDevice(&self.compute.ctx, layer.k, old_k_bytes);
                try new_v.copyFromDevice(&self.compute.ctx, layer.v, old_v_bytes);
            }
            // The old-to-new copies are asynchronous on the CUDA stream.
            // Releasing through the compute cache/deferred-free path keeps
            // the source allocations alive until ordered stream work can no
            // longer reference them; cuMemFree here races the growth copy.
            releaseDeviceBuffer(self.compute, &layer.k);
            releaseDeviceBuffer(self.compute, &layer.v);
            layer.k = new_k;
            layer.v = new_v;
            layer.capacity_tokens = new_capacity;
        }
        if (cudaDebugGraphPersistentReplayEnabled(self.compute) and self.compute.debug_cuda_graph_capture_active) {
            const replay_capacity = if (cudaDebugGraphForcedKvReplayCapacityTokens()) |forced|
                @min(layer.capacity_tokens, forced)
            else
                layer.capacity_tokens;
            if (self.compute.debug_cuda_graph_active_slot) |slot_idx| {
                const slot = &self.compute.debug_cuda_graph_slots[slot_idx];
                if (!slot.kv_replay_capacity_valid or replay_capacity < slot.kv_replay_capacity_tokens) {
                    slot.kv_replay_capacity_tokens = replay_capacity;
                    slot.kv_replay_capacity_valid = true;
                }
            }
        }
        if (compressed_format != null) {
            const logical_blocks = write.logical_blocks orelse return error.InvalidPagedKvState;
            try self.ensureLayerBlockTable(layer, logical_blocks, write.total_token_count, write.page_size_tokens);
        }
        return layer;
    }

    fn writeLayerKvSuffix(
        ctx: *anyopaque,
        write: kv_storage_runtime.KvSuffixWrite,
        k_ref: kv_storage_runtime.DeviceKvRef,
        v_ref: kv_storage_runtime.DeviceKvRef,
    ) anyerror!void {
        const self: *CudaKvDeviceStorage = @ptrCast(@alignCast(ctx));
        if (write.suffix_token_count == 0) return;
        if (write.suffix_token_count > write.total_token_count) return error.InvalidKvShape;
        const row_width = try checkedMul(@as(usize, write.num_kv_heads), @as(usize, write.head_dim));
        const suffix_bytes = try checkedMul(try checkedMul(write.suffix_token_count, row_width), @sizeOf(f32));
        if (k_ref.byte_len < suffix_bytes or v_ref.byte_len < suffix_bytes) return error.InvalidKvShape;
        const layer = try self.ensureLayer(write);
        const token_start = write.total_token_count - write.suffix_token_count;
        // Prefill KV-cache writes were previously unattributed. `suffix_token_count`
        // is the number of tokens written (>1 at prefill, 1 at decode), so this
        // fires only during prefill and covers every write branch below.
        var kv_write_profile_scope = beginPrefillProfile(self.compute, .kv_write, write.suffix_token_count);
        defer if (kv_write_profile_scope) |*scope| scope.end();
        const k_src = buffer_mod.DeviceBuffer{ .ptr = @as(@TypeOf(layer.k.ptr), @intCast(@intFromPtr(k_ref.handle) + k_ref.byte_offset)), .len = suffix_bytes };
        const v_src = buffer_mod.DeviceBuffer{ .ptr = @as(@TypeOf(layer.v.ptr), @intCast(@intFromPtr(v_ref.handle) + v_ref.byte_offset)), .len = suffix_bytes };
        if (layer.compressed_format) |format| {
            const decode_scalars = if (cudaDebugDecodeScalarsReady(self.compute))
                self.compute.debug_cuda_decode_scalars
            else
                buffer_mod.DeviceBuffer{};
            try self.compute.kernels.launchKvWriteSuffixTurboquantF32(
                &self.compute.ctx,
                layer.k,
                layer.v,
                layer.block_table,
                k_src,
                v_src,
                decode_scalars,
                write.suffix_token_count,
                row_width,
                @intCast(write.num_kv_heads),
                @intCast(write.head_dim),
                layer.key_row_bytes,
                layer.base_key_row_bytes,
                layer.value_row_bytes,
                write.total_token_count,
                layer.block_table_len,
                write.page_size_tokens,
                format,
                layer.value_format,
                layer.capacity_tokens,
            );
            layer.token_count = @max(layer.token_count, write.total_token_count);
            layer.position_offset = write.position_offset;
            self.compute.stats.device_kv_writes += 1;
            if (cudaKvValueFormatCompressed(layer.value_format)) {
                self.compute.stats.device_kv_compressed_v_writes += 1;
                self.compute.stats.device_kv_compressed_v_bytes += try checkedMul(write.suffix_token_count, layer.value_row_bytes);
            }
            return;
        }
        const dst_offset = try checkedMul(token_start, layer.value_row_bytes);
        if (cudaDebugDecodeScalarsReady(self.compute)) {
            try self.compute.kernels.launchKvWriteSuffixDecodeScalarsF32(
                &self.compute.ctx,
                layer.k,
                layer.v,
                k_src,
                v_src,
                self.compute.debug_cuda_decode_scalars,
                write.suffix_token_count,
                row_width,
                write.total_token_count,
            );
            layer.token_count = write.total_token_count;
            layer.position_offset = write.position_offset;
            self.compute.stats.device_kv_writes += 1;
            return;
        }
        const k_dst = buffer_mod.DeviceBuffer{ .ptr = layer.k.ptr + dst_offset, .len = layer.k.len - dst_offset };
        const v_dst = buffer_mod.DeviceBuffer{ .ptr = layer.v.ptr + dst_offset, .len = layer.v.len - dst_offset };
        try k_dst.copyFromDevice(&self.compute.ctx, k_src, suffix_bytes);
        try v_dst.copyFromDevice(&self.compute.ctx, v_src, suffix_bytes);
        layer.token_count = write.total_token_count;
        layer.position_offset = write.position_offset;
        self.compute.stats.device_kv_writes += 1;
    }

    fn gatherLayerKvDevice(
        ctx: *anyopaque,
        gather: kv_storage_runtime.DeviceKvLayerGather,
    ) anyerror!kv_storage_runtime.DeviceKvLayer {
        const self: *CudaKvDeviceStorage = @ptrCast(@alignCast(ctx));
        const layer_key = try self.layerKey(gather.sequence_id, gather.layer_index);
        const row_width = try checkedMul(@as(usize, gather.num_kv_heads), @as(usize, gather.head_dim));
        const layer = self.layers.getPtr(layer_key) orelse {
            traceCudaDeviceKvGatherFailure("missing_layer", gather, row_width, null);
            return error.DeviceReadFallback;
        };
        if (layer.compressed_format != null) {
            traceCudaDeviceKvGatherFailure("compressed_layer", gather, row_width, layer);
            return error.DeviceReadFallback;
        }
        if (layer.row_width != row_width or layer.token_count < gather.token_count) {
            traceCudaDeviceKvGatherFailure("shape_or_tokens", gather, row_width, layer);
            return error.DeviceReadFallback;
        }
        const bytes = try checkedMul(try checkedMul(gather.token_count, row_width), @sizeOf(f32));
        self.compute.stats.device_kv_reads += 1;
        return .{
            .runtime = self,
            .k = .{ .handle = @ptrFromInt(layer.k.ptr), .byte_offset = 0, .byte_len = bytes },
            .v = .{ .handle = @ptrFromInt(layer.v.ptr), .byte_offset = 0, .byte_len = bytes },
            .token_count = gather.token_count,
            .row_width = row_width,
            .position_offset = layer.position_offset,
            .value_element_bytes = @sizeOf(f32),
        };
    }

    fn pagedLayerKvDevice(
        ctx: *anyopaque,
        gather: kv_storage_runtime.DeviceKvLayerGather,
    ) anyerror!kv_storage_runtime.DevicePagedKvLayer {
        const self: *CudaKvDeviceStorage = @ptrCast(@alignCast(ctx));
        const layer_key = try self.layerKey(gather.sequence_id, gather.layer_index);
        const row_width = try checkedMul(@as(usize, gather.num_kv_heads), @as(usize, gather.head_dim));
        const layer = self.layers.getPtr(layer_key) orelse {
            traceCudaDeviceKvGatherFailure("missing_paged_layer", gather, row_width, null);
            return error.DeviceReadFallback;
        };
        const format = layer.compressed_format orelse {
            traceCudaDeviceKvGatherFailure("not_compressed", gather, row_width, layer);
            return error.DeviceReadFallback;
        };
        if (layer.row_width != row_width or layer.token_count < gather.token_count) {
            traceCudaDeviceKvGatherFailure("paged_shape_or_tokens", gather, row_width, layer);
            return error.DeviceReadFallback;
        }
        if (layer_key > std.math.maxInt(usize)) return error.InvalidPagedKvState;
        self.compute.stats.device_kv_reads += 1;
        return .{
            .runtime = self,
            .slot = @intCast(layer_key),
            .format = format,
            .token_count = gather.token_count,
            .key_row_bytes = layer.key_row_bytes,
            .base_key_row_bytes = layer.base_key_row_bytes,
            .v_row_stride = layer.value_row_bytes,
            .page_size_tokens = layer.page_size_tokens,
            .position_offset = layer.position_offset,
        };
    }

    fn releaseSequence(ctx: *anyopaque, sequence_id: kv_storage_runtime.SequenceId) void {
        const self: *CudaKvDeviceStorage = @ptrCast(@alignCast(ctx));
        var doomed: std.ArrayList(u64) = .empty;
        defer doomed.deinit(self.allocator);
        var it = self.layers.iterator();
        while (it.next()) |entry| {
            if (@as(u32, @intCast(entry.key_ptr.* >> 32)) == sequence_id) {
                doomed.append(self.allocator, entry.key_ptr.*) catch {};
            }
        }
        for (doomed.items) |layer_key| {
            if (self.layers.fetchRemove(layer_key)) |entry| {
                var layer = entry.value;
                layer.deinit(self.compute);
            }
        }
    }

    fn hookDeinit(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *CudaKvDeviceStorage = @ptrCast(@alignCast(ctx));
        self.deinit();
        allocator.destroy(self);
    }

    const hook_vtable = kv_storage_runtime.DeviceWriteHook.VTable{
        .writeLayerKvSuffix = writeLayerKvSuffix,
        .gatherLayerKvDevice = gatherLayerKvDevice,
        .pagedLayerKvDevice = pagedLayerKvDevice,
        .releaseSequence = releaseSequence,
        .deinit = hookDeinit,
    };

    fn hook(self: *CudaKvDeviceStorage) kv_storage_runtime.DeviceWriteHook {
        return .{ .ctx = self, .vtable = &hook_vtable };
    }
};

fn cudaDequantizeQuantWeightsOnUpload() bool {
    return load_plan.dequantizeAllWeights();
}

fn cudaDequantizeQ4_0MatrixWeightsToBf16OnUpload() bool {
    return load_plan.dequantizeQ4_0ToBf16();
}

fn cudaPleModelProjectionBf16OnUpload() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PLE_MODEL_PROJ_BF16", false);
}

fn cudaCublasLtEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CUBLASLT", true);
}

/// FP16 dense residency is an all-or-nothing route. If either the runtime
/// library or the generated staging primitive is unavailable, retain the old
/// F32 upload path so a CUDA build with a partial/older artifact bundle stays
/// functional rather than accepting weights it cannot execute.
fn canUseF16TensorCoreWeights(self: *const CudaCompute) bool {
    return cudaCublasLtEnabled() and self.ctx.info.compute_major >= 8 and
        self.cublaslt != null and self.kernels.f32_to_f16 != null and
        self.kernels.linear_f16_weight_f32_tiled != null and
        self.kernels.embedding_lookup_f16_weight_f32 != null and
        self.kernels.embedding_lookup_i32_f16_weight_f32 != null;
}

/// Only retain FP16 tensors for operations with a fully device-resident FP16
/// implementation. GLiNER's DeBERTa token embedding, per-layer dense
/// projections, and span projection MLPs are consumed by FP16 tensor-core
/// GEMM routes. The relative position table and the CountLSTM/transformer head
/// still have F32-only consumers, so those exceptional tensors remain F32.
/// Keep this operation-capability policy explicit rather than shape-dependent:
/// silently promoting every two-dimensional head tensor can make an otherwise
/// supported model fail much later in an unrelated elementwise operation.
fn cudaShouldKeepF16WeightOnDevice(name: []const u8) bool {
    if (std.mem.eql(u8, name, "embeddings.word_embeddings.weight")) return true;
    if (std.mem.startsWith(u8, name, "encoder.layer.") and std.mem.endsWith(u8, name, ".weight")) return true;
    return std.mem.startsWith(u8, name, "span_rep.span_rep_layer.") and
        std.mem.endsWith(u8, name, ".weight");
}

fn cudaDeviceMirrorDequantEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DEVICE_MIRROR_DEQUANT", true);
}

fn cudaRmsNormBf16MirrorEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_RMS_NORM_BF16_MIRROR", false);
}

fn cudaCublasLtWorkspaceBytes() usize {
    const workspace_mb = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CUBLASLT_WORKSPACE_MB") orelse
        platform.env.getenvUsize("ANTFLY_CUDA_CUBLASLT_WORKSPACE_MB") orelse
        platform.env.getenvUsize("TERMITE_CUDA_CUBLASLT_WORKSPACE_MB") orelse
        0;
    return std.math.mul(usize, workspace_mb, 1024 * 1024) catch 0;
}

const cuda_kv_format_polar4: u32 = 0;
const cuda_kv_format_turbo3: u32 = 1;
const cuda_kv_format_f16: u32 = 2;
const cuda_kv_value_format_f32: u32 = 0;
const cuda_kv_value_format_int8_per_head: u32 = 1;
const cuda_kv_value_format_f16: u32 = 2;
const cuda_kv_value_format_int4_group: u32 = 3;

fn cudaPagedKvFormat(dtype: kv_pool_mod.KvDType) ?u32 {
    return switch (dtype) {
        .f16 => cuda_kv_format_f16,
        .polar4 => cuda_kv_format_polar4,
        .turbo3 => cuda_kv_format_turbo3,
        else => null,
    };
}

fn cudaTurboquantKvFormat(dtype: kv_pool_mod.KvDType) ?u32 {
    return switch (dtype) {
        .polar4 => cuda_kv_format_polar4,
        .turbo3 => cuda_kv_format_turbo3,
        else => null,
    };
}

fn cudaTurboquantKvDisabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_DISABLE_TURBOQUANT_KV", false);
}

fn cudaTurboquantCompressedVDisabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_DISABLE_TURBOQUANT_COMPRESSED_V", false);
}

fn cudaTurboquantInt4ValuesEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_TURBOQUANT_INT4_VALUES", false);
}

fn cudaKvValueFormatCompressed(value_format: u32) bool {
    return value_format == cuda_kv_value_format_int8_per_head or
        value_format == cuda_kv_value_format_int4_group;
}

fn cudaFfnStreamEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_FFN_STREAM");
}

fn cudaFfnStreamProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_FFN_STREAM_PROFILE");
}

fn cudaAutoDenseStreamEnabled(self: *const CudaCompute) bool {
    const store = self.lazy_host_store orelse return false;
    const tensor_store = store.tensor_store orelse return false;
    return tensor_store.kind() == .safetensors and store.lazy_weights.count() >= 128;
}

fn cudaDenseStreamEnabled(self: *const CudaCompute) bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DENSE_STREAM", cudaAutoDenseStreamEnabled(self)) or cudaFfnStreamEnabled();
}

fn cudaDenseStreamProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DENSE_STREAM_PROFILE") or cudaFfnStreamProfileEnabled();
}

fn cudaDenseStreamBudgetBytes(self: *const CudaCompute) usize {
    const mib: usize = 1024 * 1024;
    if (platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_DENSE_STREAM_BUDGET_MB")) |requested_mb| {
        const budget_mb = @max(@as(usize, 512), requested_mb);
        return budget_mb * mib;
    }

    const min_auto_mb: usize = 2048;
    const max_auto_mb: usize = 12000;
    const budget = self.run_budget orelse return min_auto_mb * mib;
    const backend_limit = budget.limits.backend_limit_bytes;
    if (backend_limit == 0) return min_auto_mb * mib;
    const reserved = budget.backend_kv_bytes + budget.backend_scratch_bytes;
    if (backend_limit <= reserved) return min_auto_mb * mib;
    const available_mb = (backend_limit - reserved) / mib;
    const derived_mb = (available_mb * 2) / 3;
    const budget_mb = @min(max_auto_mb, @max(min_auto_mb, derived_mb));
    return budget_mb * mib;
}

fn cudaDensePrefetchEnabled(self: *const CudaCompute) bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DENSE_PREFETCH", cudaAutoDenseStreamEnabled(self));
}

fn cudaDensePrefetchProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DENSE_PREFETCH_PROFILE") or cudaDenseStreamProfileEnabled();
}

fn cudaDenseHostPrefetchBudgetBytes() usize {
    const requested_mb = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_DENSE_HOST_PREFETCH_MB") orelse 4096;
    const budget_mb = @max(@as(usize, 512), requested_mb);
    return budget_mb * 1024 * 1024;
}

fn initCublasLtIfAvailable(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext) ?cublaslt_mod.CublasLt {
    if (!cudaCublasLtEnabled()) return null;
    if (ctx.info.compute_major < 8) return null;
    return cublaslt_mod.CublasLt.openWithAllocator(allocator, ctx) catch null;
}

fn cudaCublasLtWarmupEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CUBLASLT_WARMUP", true);
}

// cuBLASLt loads its kernel libraries lazily on the first BF16 matmul
// (~100ms of cuLibraryLoadData on an L4), which otherwise lands inside the
// first prefill. Run one tiny matmul at init so the cost moves to load time.
fn warmupCublasLtBf16(cublaslt: *cublaslt_mod.CublasLt, ctx: *context_mod.CudaContext) void {
    const rows: usize = 8;
    const dim: usize = 32;
    var input = buffer_mod.DeviceBuffer.alloc(ctx, rows * dim * @sizeOf(u16)) catch return;
    defer input.free(ctx);
    var weight = buffer_mod.DeviceBuffer.alloc(ctx, dim * dim * @sizeOf(u16)) catch return;
    defer weight.free(ctx);
    var dst = buffer_mod.DeviceBuffer.alloc(ctx, rows * dim * @sizeOf(f32)) catch return;
    defer dst.free(ctx);
    // Inputs stay uninitialized: the result is discarded, only the library
    // load and heuristic init matter.
    cublaslt.matmulBf16WeightF32Out(ctx, dst, input, weight, .{}, rows, dim, dim) catch return;
    ctx.driver.check(ctx.driver.fns.cuStreamSynchronize(ctx.stream)) catch {};
}

fn cudaShouldDequantizeWeightOnUpload(name: []const u8, storage: weight_source_mod.QuantizedStorage) bool {
    // Matrix weights stay quantized for CUDA matmul kernels. Small affine
    // parameters are consumed by f32 norm/elementwise kernels, so upload them
    // as f32 even if the bundle stored them in a quantized GGUF block.
    if (isKnownQuantStorage(storage, .Q6_K) and !cudaKeepQ6KQuantizedByName(name)) return true;
    if (storage.shape.len < 2) return true;
    if (std.mem.endsWith(u8, name, ".bias")) return true;
    if (std.mem.eql(u8, name, "count_embed.pos_embedding.weight")) return true;
    if (std.mem.eql(u8, name, "encoder.rel_embeddings.weight")) return true;
    if (std.mem.indexOf(u8, name, ".norm") != null) return true;
    if (std.mem.indexOf(u8, name, "layer_norm") != null) return true;
    return false;
}

fn cudaShouldDequantizeQ4_0MatrixWeightToBf16OnUpload(name: []const u8, storage: weight_source_mod.QuantizedStorage) bool {
    if (!cudaDequantizeQ4_0MatrixWeightsToBf16OnUpload()) return false;
    if (!isKnownQuantStorage(storage, .Q4_0)) return false;
    if (cudaShouldDequantizeWeightOnUpload(name, storage)) return false;
    if (storage.shape.len != 2) return false;
    if (storage.shape[0] <= 0 or storage.shape[1] <= 0) return false;
    if (isLmHeadOrTiedTokenEmbeddingWeightName(name)) return false;
    if (std.mem.indexOf(u8, name, "token_embd") != null) return false;
    if (std.mem.indexOf(u8, name, "embed_tokens") != null) return false;
    return true;
}

// Hybrid residency: keep Q4_0 matrix weights resident for the decode DP4A
// kernels AND attach a dequantized BF16 copy consumed by cuBLASLt when
// rows > 1 (prefill). Costs ~2 bytes/param extra device memory.
fn cudaHybridQ4Bf16WeightsEnabled() bool {
    return load_plan.retainQ4_0Bf16Mirror();
}

// Qualified-performance target: production parity and performance evidence
// for the default-on BF16 prefill mirrors and fused DeBERTa attention is
// exact to NVIDIA L4 / SM89 (see GLINER2_CUDA.md). Other architectures keep
// the conservative route by default and opt in through the env switches.
fn cudaQualifiedPerfTarget(compute_major: i32, compute_minor: i32) bool {
    return compute_major == 8 and compute_minor == 9;
}

fn cudaBertQ4Bf16PrefillEnabled(default_on: bool) bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_BERT_Q4_0_BF16_PREFILL", default_on);
}

// Name/env half of the mirror decision, usable before a CUDA context exists
// (kernel-JIT route scoping passes bert_default_on=true: an optimistic scope
// only costs generated prefill routes, never correctness). The attach
// decision itself must go through cudaShouldAttachBf16MirrorToQ4Weight,
// which also requires the BF16 fast path to exist on the device and applies
// the qualified-target default.
fn cudaQ4WeightBf16MirrorPolicyEnabled(name: []const u8, storage: weight_source_mod.QuantizedStorage, bert_default_on: bool) bool {
    // Encoder prefill has M=batch*sequence rows, so dequantizing the static
    // Q4_0 projection once on upload and dispatching the existing BF16
    // cuBLASLt path is materially faster than repeatedly running the scalar
    // SIMT Q4 kernel. Keep decoder behavior opt-in, and enable the
    // device-resident BERT/XLM-R profile by default only on the qualified
    // target.
    const bert_encoder_weight = std.mem.startsWith(u8, name, "encoder.layer.");
    if (!cudaHybridQ4Bf16WeightsEnabled() and
        !(bert_encoder_weight and cudaBertQ4Bf16PrefillEnabled(bert_default_on)))
    {
        return false;
    }
    if (cudaDequantizeQ4_0MatrixWeightsToBf16OnUpload()) return false;
    if (!isKnownQuantStorage(storage, .Q4_0)) return false;
    if (cudaShouldDequantizeWeightOnUpload(name, storage)) return false;
    if (storage.shape.len != 2) return false;
    if (storage.shape[0] <= 0 or storage.shape[1] <= 0) return false;
    if (isLmHeadOrTiedTokenEmbeddingWeightName(name)) return false;
    if (std.mem.indexOf(u8, name, "token_embd") != null) return false;
    if (std.mem.indexOf(u8, name, "embed_tokens") != null) return false;
    return true;
}

fn cudaShouldAttachBf16MirrorToQ4Weight(
    self: *const CudaCompute,
    name: []const u8,
    storage: weight_source_mod.QuantizedStorage,
) bool {
    // A mirror is only worth its ~2 bytes/param when the cuBLASLt tensor-core
    // path can consume it (cublaslt != null implies compute capability >= 8,
    // the env gate on, and the library loaded). Without it the mirror would
    // route prefill to the scalar BF16 fallback — slower than the retained
    // Q4 kernels — while still inflating VRAM at load admission.
    if (self.cublaslt == null) return false;
    return cudaQ4WeightBf16MirrorPolicyEnabled(
        name,
        storage,
        cudaQualifiedPerfTarget(self.ctx.info.compute_major, self.ctx.info.compute_minor),
    );
}

fn isPleModelProjectionWeightName(name: []const u8) bool {
    return std.mem.eql(u8, name, "model.per_layer_input.per_layer_model_proj.weight") or
        std.mem.eql(u8, name, "model.per_layer_model_projection.weight");
}

fn cudaShouldConvertF32WeightToBf16OnUpload(name: []const u8, tensor: *const tensor_mod.Tensor) bool {
    if (!cudaPleModelProjectionBf16OnUpload()) return false;
    if (!isPleModelProjectionWeightName(name)) return false;
    if (tensor.dtype != .f32 or tensor.shape.len != 2) return false;
    if (tensor.shape[0] <= 0 or tensor.shape[1] <= 0) return false;
    return true;
}

fn cudaDequantizeQ6KEmbeddingEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q6K_EMBED", false);
}

fn isTiedTokenEmbeddingWeightName(name: []const u8) bool {
    return std.mem.eql(u8, name, "token_embd.weight") or
        std.mem.eql(u8, name, "model.embed_tokens.weight");
}

fn isLmHeadOrTiedTokenEmbeddingWeightName(name: []const u8) bool {
    return isTiedTokenEmbeddingWeightName(name) or
        std.mem.eql(u8, name, "lm_head.weight") or
        std.mem.eql(u8, name, "model.lm_head.weight") or
        std.mem.eql(u8, name, "output.weight");
}

fn cudaKeepQ6KQuantizedByName(name: []const u8) bool {
    if (cudaDequantizeQ6KEmbeddingEnabled() and
        isTiedTokenEmbeddingWeightName(name))
    {
        return false;
    }
    if (isTiedTokenEmbeddingWeightName(name) or
        std.mem.eql(u8, name, "per_layer_token_embd.weight") or
        std.mem.eql(u8, name, "model.per_layer_input.per_layer_token_embd.weight"))
    {
        return true;
    }
    if (std.mem.endsWith(u8, name, ".ffn_down.weight")) return true;
    if (std.mem.indexOf(u8, name, ".mlp.down_proj.weight") != null) return true;
    return false;
}

fn isKnownQuantStorage(storage: weight_source_mod.QuantizedStorage, known: gguf_tensor_types.KnownTensorType) bool {
    return switch (storage.tensor_type) {
        .known => |actual| actual == known,
        else => false,
    };
}

fn f32ToBf16BitsRoundNearestEven(value: f32) u16 {
    const bits: u32 = @bitCast(value);
    const lsb = (bits >> 16) & 1;
    const rounded = bits +% (0x7fff + lsb);
    return @intCast(rounded >> 16);
}

fn cudaGlinerSpanQ4KernelsEnabled() bool {
    if (platform.env.getenvBool("TERMITE_CUDA_DISABLE_GLINER_SPAN_Q4_KERNELS")) return false;
    return platform.env.getenvBoolDefault("TERMITE_CUDA_ENABLE_GLINER_SPAN_Q4_KERNELS", true);
}

fn cudaDispatchStatsEnabled() bool {
    return platform.env.getenvBool("ANTFLY_CUDA_DISPATCH_STATS");
}

fn cudaQ4RouteCensusEnabled() bool {
    return cudaDispatchStatsEnabled() or
        platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DECODE_Q4_ROUTE_CENSUS");
}

fn cudaQ8TiledKernelsEnabled() bool {
    return !platform.env.getenvBool("ANTFLY_CUDA_DISABLE_Q8_TILED");
}

fn cudaQ4FusionKernelsEnabled() bool {
    if (platform.env.getenvBool("ANTFLY_CUDA_DISABLE_Q4_FUSIONS")) return false;
    return platform.env.getenvBool("ANTFLY_CUDA_ENABLE_Q4_FUSIONS");
}

const QMatmulVariantChoice = enum {
    auto,
    legacy,
    fast_r2c4,
    fast_r2c8,
    fast_r4c4,
    tc_hmma,
};

fn cudaQMatmulVariantChoice() QMatmulVariantChoice {
    const raw = platform.env.getenv("ANTFLY_CUDA_QMATMUL_VARIANT") orelse return .auto;
    if (std.ascii.eqlIgnoreCase(raw, "legacy")) return .legacy;
    if (std.ascii.eqlIgnoreCase(raw, "fast_r2c4")) return .fast_r2c4;
    if (std.ascii.eqlIgnoreCase(raw, "r2c4")) return .fast_r2c4;
    if (std.ascii.eqlIgnoreCase(raw, "fast_r2c8")) return .fast_r2c8;
    if (std.ascii.eqlIgnoreCase(raw, "r2c8")) return .fast_r2c8;
    if (std.ascii.eqlIgnoreCase(raw, "fast_r4c4")) return .fast_r4c4;
    if (std.ascii.eqlIgnoreCase(raw, "r4c4")) return .fast_r4c4;
    if (std.ascii.eqlIgnoreCase(raw, "tc_hmma")) return .tc_hmma;
    if (std.ascii.eqlIgnoreCase(raw, "hmma")) return .tc_hmma;
    if (std.ascii.eqlIgnoreCase(raw, "tensor_core")) return .tc_hmma;
    return .auto;
}

fn cudaQMatmulKernelVariant() ?kernels_mod.QMatmulVariant {
    return switch (cudaQMatmulVariantChoice()) {
        .legacy => null,
        .auto => .tc_hmma,
        .fast_r2c4 => .fast_r2c4,
        .fast_r2c8 => .fast_r2c8,
        .fast_r4c4 => .fast_r4c4,
        .tc_hmma => .tc_hmma,
    };
}

fn isApprovedQuantLinearShape(in_dim: usize, out_dim: usize) bool {
    if (in_dim == 512 and out_dim == 128) return true;
    if (in_dim == 256 and out_dim == 256) return true;
    if (in_dim == 256 and out_dim == 768) return true;
    if (in_dim == 256 and out_dim == 1024) return true;
    if (in_dim == 512 and out_dim == 512) return true;
    if (in_dim == 512 and out_dim == 1024) return true;
    if (in_dim == 512 and out_dim == 1536) return true;
    if (in_dim == 512 and out_dim == 2048) return true;
    if (in_dim == 768 and out_dim == 768) return true;
    if (in_dim == 768 and out_dim == 3072) return true;
    if (in_dim == 1024 and out_dim == 256) return true;
    if (in_dim == 1024 and out_dim == 512) return true;
    if (in_dim == 1024 and out_dim == 1024) return true;
    if (in_dim == 1024 and out_dim == 3072) return true;
    if (in_dim == 1024 and out_dim == 4096) return true;
    if (in_dim == 1536 and out_dim == 3072) return true;
    if (in_dim == 2048 and out_dim == 512) return true;
    if (in_dim == 2048 and out_dim == 1024) return true;
    if (in_dim == 3072 and out_dim == 768) return true;
    if (in_dim == 3072 and out_dim == 1536) return true;
    if (in_dim == 4096 and out_dim == 1024) return true;
    return false;
}

fn isTensorCoreQuantLinearShape(in_dim: usize, out_dim: usize) bool {
    if (out_dim == 1) return false;
    if (in_dim == 0 or in_dim % 256 != 0) return false;
    return isApprovedQuantLinearShape(in_dim, out_dim);
}

// Shape gate for the opt-in Q4_0 W4A16 tensor-core route. The WMMA kernel needs
// K a multiple of 256 (q4_0 block=32 values, tc K-tile=16) and N a multiple of
// the 16-wide column tile; unlike the encoder whitelist this admits the gemma4
// decoder projection shapes (1536×2048, 1536×8960, 2048×1536, 12288×1536, …).
fn isQ4_0TcHmmaShape(in_dim: usize, out_dim: usize) bool {
    return out_dim > 1 and in_dim != 0 and in_dim % 256 == 0 and out_dim % 16 == 0;
}

fn useMxbaiQ8TiledKernel(rows: usize, in_dim: usize, out_dim: usize) bool {
    return cudaQ8TiledKernelsEnabled() and rows > 0 and in_dim % 256 == 0 and isApprovedQuantLinearShape(in_dim, out_dim);
}

fn mxbaiQ8Variant(rows: usize, in_dim: usize, out_dim: usize) ?kernels_mod.QMatmulVariant {
    if (!useMxbaiQ8TiledKernel(rows, in_dim, out_dim)) return null;
    const variant = cudaQMatmulKernelVariant() orelse return null;
    if (variant == .fast_r2c8) return null;
    return variant;
}

fn useMxbaiQ4FusionKernel(rows: usize, in_dim: usize, out_dim: usize) bool {
    return (cudaQ4FusionKernelsEnabled() or cudaTensorCoreQuantRequested()) and rows >= 2 and in_dim % 256 == 0 and isApprovedQuantLinearShape(in_dim, out_dim);
}

fn mxbaiQ4Variant(rows: usize, in_dim: usize, out_dim: usize) ?kernels_mod.QMatmulVariant {
    if (!useMxbaiQ4FusionKernel(rows, in_dim, out_dim)) return null;
    return cudaQMatmulKernelVariant();
}

fn mxbaiQ4TiledVariant(rows: usize, in_dim: usize, out_dim: usize) ?kernels_mod.QMatmulVariant {
    if (rows == 0 or in_dim % 256 != 0 or !isApprovedQuantLinearShape(in_dim, out_dim)) return null;
    return cudaQMatmulKernelVariant();
}

fn cudaTensorCoreQuantRequested() bool {
    return load_plan.tensorCorePackedWeightsRequested();
}

fn cudaTensorCoreQuantAvailable(self: *const CudaCompute) bool {
    return self.ctx.info.compute_major >= 8;
}

fn knownQuantTensorType(tensor_type: gguf_tensor_types.TensorType) ?gguf_tensor_types.KnownTensorType {
    return switch (tensor_type) {
        .known => |known| known,
        else => null,
    };
}

const Q8_0_VALUES_PER_BLOCK: usize = 32;
const Q8_0_BLOCK_BYTES: usize = 34;
const Q8_0_TC_SCALE_BYTES: usize = 2;
const Q8_0_TC_Q_BYTES: usize = 32;
const Q4_0_VALUES_PER_BLOCK: usize = 32;
const Q4_0_BLOCK_BYTES: usize = 18;
const Q4_0_TC_SCALE_BYTES: usize = 2;
const Q4_0_TC_Q_BYTES: usize = 16;
const Q4_K_VALUES_PER_BLOCK: usize = 256;
const Q4_K_BLOCK_BYTES: usize = 144;
const Q4_K_TC_META_BYTES: usize = 20;
const Q4_K_TC_Q_BYTES: usize = 128;
const Q4_K_TC_BLOCK_BYTES: usize = Q4_K_TC_META_BYTES + Q4_K_TC_Q_BYTES;

const PackedTensorCoreQuant = struct {
    layout: CudaTensorCoreQuantLayout,
    row_blocks: usize,
    bytes: []u8,
};

fn packQ8_0TensorCore(allocator: std.mem.Allocator, raw: []const u8, in_dim: usize, out_dim: usize) !PackedTensorCoreQuant {
    if (in_dim == 0 or in_dim % Q8_0_VALUES_PER_BLOCK != 0) return error.InvalidShape;
    const row_blocks = in_dim / Q8_0_VALUES_PER_BLOCK;
    const block_count = try checkedMul(out_dim, row_blocks);
    const expected_raw = try checkedMul(block_count, Q8_0_BLOCK_BYTES);
    if (raw.len != expected_raw) return error.InvalidShape;

    const scales_bytes = try checkedMul(block_count, Q8_0_TC_SCALE_BYTES);
    const q_bytes = try checkedMul(block_count, Q8_0_TC_Q_BYTES);
    const out = try allocator.alloc(u8, try checkedAdd(scales_bytes, q_bytes));
    errdefer allocator.free(out);

    const q_base = scales_bytes;
    for (0..block_count) |block| {
        const src = raw[block * Q8_0_BLOCK_BYTES ..][0..Q8_0_BLOCK_BYTES];
        @memcpy(out[block * Q8_0_TC_SCALE_BYTES ..][0..Q8_0_TC_SCALE_BYTES], src[0..2]);
        @memcpy(out[q_base + block * Q8_0_TC_Q_BYTES ..][0..Q8_0_TC_Q_BYTES], src[2..34]);
    }

    return .{
        .layout = .q8_0_hmma,
        .row_blocks = row_blocks,
        .bytes = out,
    };
}

// Repack raw GGUF Q4_0 (18-byte blocks: f16 scale + 32 packed 4-bit quants) into
// the q4_0_hmma layout the tensor-core loader expects: a contiguous scales
// region (2 bytes/block) followed by a contiguous quants region (16 bytes/block).
// Bytes are copied verbatim (nibble order unchanged); only the two regions are
// separated, mirroring packQ8_0TensorCore. Total size is unchanged from raw.
fn packQ4_0TensorCore(allocator: std.mem.Allocator, raw: []const u8, in_dim: usize, out_dim: usize) !PackedTensorCoreQuant {
    if (in_dim == 0 or in_dim % Q4_0_VALUES_PER_BLOCK != 0) return error.InvalidShape;
    const row_blocks = in_dim / Q4_0_VALUES_PER_BLOCK;
    const block_count = try checkedMul(out_dim, row_blocks);
    const expected_raw = try checkedMul(block_count, Q4_0_BLOCK_BYTES);
    if (raw.len != expected_raw) return error.InvalidShape;

    const scales_bytes = try checkedMul(block_count, Q4_0_TC_SCALE_BYTES);
    const q_bytes = try checkedMul(block_count, Q4_0_TC_Q_BYTES);
    const out = try allocator.alloc(u8, try checkedAdd(scales_bytes, q_bytes));
    errdefer allocator.free(out);

    const q_base = scales_bytes;
    for (0..block_count) |block| {
        const src = raw[block * Q4_0_BLOCK_BYTES ..][0..Q4_0_BLOCK_BYTES];
        @memcpy(out[block * Q4_0_TC_SCALE_BYTES ..][0..Q4_0_TC_SCALE_BYTES], src[0..2]);
        @memcpy(out[q_base + block * Q4_0_TC_Q_BYTES ..][0..Q4_0_TC_Q_BYTES], src[2..18]);
    }

    return .{
        .layout = .q4_0_hmma,
        .row_blocks = row_blocks,
        .bytes = out,
    };
}

fn packQ4_KTensorCore(allocator: std.mem.Allocator, raw: []const u8, in_dim: usize, out_dim: usize) !PackedTensorCoreQuant {
    if (in_dim == 0 or in_dim % Q4_K_VALUES_PER_BLOCK != 0) return error.InvalidShape;
    const row_blocks = in_dim / Q4_K_VALUES_PER_BLOCK;
    const block_count = try checkedMul(out_dim, row_blocks);
    const expected_raw = try checkedMul(block_count, Q4_K_BLOCK_BYTES);
    if (raw.len != expected_raw) return error.InvalidShape;

    const meta_bytes = try checkedMul(block_count, Q4_K_TC_META_BYTES);
    const q_bytes = try checkedMul(block_count, Q4_K_TC_Q_BYTES);
    const out = try allocator.alloc(u8, try checkedAdd(meta_bytes, q_bytes));
    errdefer allocator.free(out);

    const q_base = meta_bytes;
    for (0..block_count) |block| {
        const src = raw[block * Q4_K_BLOCK_BYTES ..][0..Q4_K_BLOCK_BYTES];
        const meta = out[block * Q4_K_TC_META_BYTES ..][0..Q4_K_TC_META_BYTES];
        @memcpy(meta[0..4], src[0..4]);
        const scales = src[4..16];
        for (0..8) |sub| {
            meta[4 + sub] = q4KScale(scales, sub);
            meta[12 + sub] = q4KMin(scales, sub);
        }
        @memcpy(out[q_base + block * Q4_K_TC_Q_BYTES ..][0..Q4_K_TC_Q_BYTES], src[16..144]);
    }

    return .{
        .layout = .q4_k_hmma,
        .row_blocks = row_blocks,
        .bytes = out,
    };
}

fn q4KScale(scales: []const u8, sub: usize) u8 {
    std.debug.assert(scales.len >= 12);
    if (sub < 4) return scales[sub] & 63;
    return (scales[sub + 4] & 0x0f) | ((scales[sub - 4] >> 6) << 4);
}

fn q4KMin(scales: []const u8, sub: usize) u8 {
    std.debug.assert(scales.len >= 12);
    if (sub < 4) return scales[sub + 4] & 63;
    return (scales[sub + 4] >> 4) | ((scales[sub] >> 6) << 4);
}

fn isGlinerSpanQ4Shape(rows: usize, in_dim: usize, out_dim: usize) bool {
    if (rows < 2) return false;
    if (in_dim == 768 and out_dim == 3072) return true;
    if (in_dim == 3072 and out_dim == 768) return true;
    if (in_dim == 1536 and out_dim == 3072) return true;
    return false;
}

fn useGlinerSpanQ4Kernel(self: *const CudaCompute, rows: usize, in_dim: usize, out_dim: usize) bool {
    return cudaGlinerSpanQ4KernelsEnabled() and
        self.kernels.hasGlinerSpanQ4KPrimitives() and
        isGlinerSpanQ4Shape(rows, in_dim, out_dim);
}

fn tensorFromCt(tensor: CT) *CudaTensor {
    return @ptrCast(@alignCast(tensor));
}

fn unsupportedCt() anyerror!CT {
    return error.CudaOpUnsupported;
}

fn backendKind(_: *anyopaque) ops.BackendKind {
    return .cuda;
}

fn provisionKvDeviceWriteHook(ctx: *anyopaque, storage: *kv_storage_runtime.KvStorageRuntime) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (storage.device_write_hook != null) return;
    const config = storage.storage.config;
    if (config.dtype != .f32 and cudaPagedKvFormat(config.dtype) == null) return;
    if (config.num_kv_heads == 0 or config.head_dim == 0) return;
    const device_storage = try CudaKvDeviceStorage.create(self.allocator, self, storage.storage.config.dtype);
    errdefer {
        device_storage.deinit();
        self.allocator.destroy(device_storage);
    }
    storage.setDeviceWriteHook(device_storage.hook());
}

fn beginRequest(ctx: *anyopaque) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    // A graph launch may still reference its captured temporary addresses.
    // Establish stream completion before destroying execs or recycling any
    // pinned slot at a request boundary. This is normally already satisfied
    // by token readback, but making it explicit keeps the ownership invariant
    // independent of sampling and response plumbing.
    if (self.ctx.debug_graph_capture_active) return error.InvalidCudaState;
    try synchronizeAndDrainDeferredDeviceFrees(self);
    try resetDebugCudaGraphRequestState(self);
    if (self.temp_slot_config.autoplan_enabled or
        platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_SERVER_REQUEST_GRAPH_RESET", false))
    {
        // A server reuses CudaCompute across requests, unlike the CLI benchmark.
        // Start each request's pinned-temp capture schedule from a fresh
        // boundary. The learned planner retains only its shape candidate and
        // proves it again before capture; legacy fixed schedules retain the
        // synchronization required by their process-global allocation ABI.
        try resetTempPinnedSlotsForRequest(self);
    }
}

fn deinitBackend(ctx: *anyopaque) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const owned = self.owned_by_backend;
    if (owned) {
        self.deinit();
        self.allocator.destroy(self);
    }
}

fn deinitBackendClearRunBudget(ctx: *anyopaque) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.configureRunBudget(null);
    deinitBackend(ctx);
}

fn freeCudaTensorStorage(self: *CudaCompute, cuda_tensor: *CudaTensor) void {
    if (cuda_tensor.tc_quant) |*tc_quant| {
        var packed_buffer = tc_quant.buffer;
        releaseDeviceBuffer(self, &packed_buffer);
        cuda_tensor.tc_quant = null;
    }
    if (cuda_tensor.owns_bf16_mirror) releaseDeviceBuffer(self, &cuda_tensor.bf16_mirror);
    if (cuda_tensor.owns_training_upload_host) cuda_tensor.training_upload_host.free(&self.ctx);
    if (cuda_tensor.owns_buffer) releaseDeviceBuffer(self, &cuda_tensor.buffer);
    if (cuda_tensor.owns_shape) self.allocator.free(cuda_tensor.shape);
}

fn freeCudaTensorStorageUncached(self: *CudaCompute, cuda_tensor: *CudaTensor) void {
    if (cuda_tensor.tc_quant) |*tc_quant| {
        if (tc_quant.buffer.ptr != 0) {
            self.stats.device_free_calls += 1;
            tc_quant.buffer.free(&self.ctx);
        }
        cuda_tensor.tc_quant = null;
    }
    if (cuda_tensor.owns_bf16_mirror and cuda_tensor.bf16_mirror.ptr != 0) {
        self.stats.device_free_calls += 1;
        cuda_tensor.bf16_mirror.free(&self.ctx);
    }
    if (cuda_tensor.owns_training_upload_host) cuda_tensor.training_upload_host.free(&self.ctx);
    if (cuda_tensor.owns_buffer and cuda_tensor.buffer.ptr != 0) {
        self.stats.device_free_calls += 1;
        cuda_tensor.buffer.free(&self.ctx);
    }
    if (cuda_tensor.owns_shape) self.allocator.free(cuda_tensor.shape);
}

fn deinitDenseStreamCache(self: *CudaCompute) void {
    for (self.dense_stream_entries.items) |*entry| {
        freeCudaTensorStorageUncached(self, entry.tensor);
        self.allocator.destroy(entry.tensor);
        self.allocator.free(entry.name);
    }
    self.dense_stream_entries.deinit(self.allocator);
    self.stats.ffn_stream_resident_bytes = 0;
    self.stats.dense_stream_resident_bytes = 0;
}

fn initDenseHostPrefetch(self: *CudaCompute) !void {
    if (!cudaDensePrefetchEnabled(self)) return;
    if (self.dense_host_prefetch_initialized) return;
    if (self.lazy_host_store == null) return;
    self.dense_host_prefetch = DenseHostPrefetchQueue.initWithPriorityUnlocked(
        self.allocator,
        self,
        &denseHostPrefetchProcess,
        &denseHostPrefetchPriority,
    );
    self.dense_host_prefetch_initialized = true;
    if (!disableCudaLazyHostPrefetchWorker()) {
        try self.dense_host_prefetch.start();
    }
}

fn deinitDenseHostPrefetch(self: *CudaCompute) void {
    if (self.dense_host_prefetch_initialized) {
        self.dense_host_prefetch.deinit();
        self.dense_host_prefetch_initialized = false;
    }
    var it = self.dense_host_prefetch_entries.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.*.deinit(self.allocator);
        self.allocator.destroy(entry.value_ptr.*);
    }
    self.dense_host_prefetch_entries.deinit(self.allocator);
    self.stats.dense_prefetch_resident_bytes = 0;
}

fn cloneDenseHostRange(allocator: std.mem.Allocator, range: tensor_store_mod.TensorRangeRef) !DenseHostRange {
    const path = try allocator.dupe(u8, range.path);
    errdefer allocator.free(path);
    const shape = try allocator.dupe(i64, range.shape);
    errdefer allocator.free(shape);
    return .{
        .path = path,
        .byte_offset = range.byte_offset,
        .byte_len = range.byte_len,
        .dtype = range.dtype,
        .shape = shape,
    };
}

fn createDenseHostPrefetchEntry(self: *CudaCompute, name: []const u8, priority: u64) !*DenseHostPrefetchEntry {
    const store = self.lazy_host_store orelse return error.MissingWeight;
    const tensor_store = store.tensor_store orelse return error.MissingWeight;
    var tensor_range = (try tensor_store.describeTensorRange(self.allocator, name)) orelse return error.MissingWeight;
    defer tensor_range.deinit(self.allocator);
    if (tensor_range.dtype != .bf16 or tensor_range.shape.len < 2 or tensor_range.byte_len == 0) return error.UnsupportedTensorType;

    const entry = try self.allocator.create(DenseHostPrefetchEntry);
    errdefer self.allocator.destroy(entry);
    const owned_name = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(owned_name);
    const range = try cloneDenseHostRange(self.allocator, tensor_range);
    errdefer {
        var mutable_range = range;
        mutable_range.deinit(self.allocator);
    }
    entry.* = .{
        .name = owned_name,
        .range = range,
        .kind = if (isDenseAttentionStreamWeightName(name)) .attention else .mlp,
        .priority = priority,
        .pending = true,
        .last_access = self.dense_host_prefetch_epoch,
    };
    return entry;
}

fn denseHostPrefetchPriority(entry: *DenseHostPrefetchEntry) u64 {
    return entry.priority;
}

fn readDenseHostBytes(self: *CudaCompute, path: []const u8, byte_offset: u64, byte_len: usize) !struct { host: []u8, read_ns: u64 } {
    self.stats.dense_stream_fadvise_calls += 1;
    c_file.adviseFileRange(self.allocator, path, byte_offset, byte_len, .will_need);
    const start = platform.time.monotonicNs();
    const host = try self.allocator.alloc(u8, byte_len);
    errdefer self.allocator.free(host);
    try c_file.readRegionInto(self.allocator, path, byte_offset, host);
    const read_ns = elapsedNsSince(start);
    self.stats.dense_stream_fadvise_calls += 1;
    c_file.adviseFileRange(self.allocator, path, byte_offset, byte_len, .dont_need);
    return .{ .host = host, .read_ns = read_ns };
}

fn evictDenseHostPrefetchToBudgetLocked(self: *CudaCompute, protected_name: []const u8, incoming_bytes: usize) void {
    const budget = cudaDenseHostPrefetchBudgetBytes();
    while (self.stats.dense_prefetch_resident_bytes + incoming_bytes > budget) {
        var victim_name: ?[]const u8 = null;
        var victim_entry: ?*DenseHostPrefetchEntry = null;
        var oldest_epoch: u64 = std.math.maxInt(u64);
        var it = self.dense_host_prefetch_entries.iterator();
        while (it.next()) |map_entry| {
            const entry = map_entry.value_ptr.*;
            if (!entry.ready or entry.host.len == 0) continue;
            if (std.mem.eql(u8, entry.name, protected_name)) continue;
            if (entry.last_access < oldest_epoch) {
                oldest_epoch = entry.last_access;
                victim_name = map_entry.key_ptr.*;
                victim_entry = entry;
            }
        }
        const entry = victim_entry orelse return;
        _ = self.dense_host_prefetch_entries.remove(victim_name.?);
        self.stats.dense_prefetch_resident_bytes -|= entry.host.len;
        self.stats.dense_prefetch_evictions += 1;
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
    }
}

fn removeDenseHostPrefetchQueueItemLocked(self: *CudaCompute, entry: *DenseHostPrefetchEntry) bool {
    for (self.dense_host_prefetch.items.items, 0..) |queued, index| {
        if (queued == entry) {
            _ = self.dense_host_prefetch.items.orderedRemove(index);
            entry.pending = false;
            return true;
        }
    }
    return false;
}

fn denseHostPrefetchProcess(ctx: *anyopaque, entry: *DenseHostPrefetchEntry) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.dense_host_prefetch.lock();
    if (!entry.pending) {
        self.dense_host_prefetch.unlock();
        return;
    }
    entry.pending = false;
    entry.loading = true;
    self.dense_host_prefetch.unlock();

    const loaded = readDenseHostBytes(self, entry.range.path, entry.range.byte_offset, entry.range.byte_len) catch {
        self.dense_host_prefetch.lock();
        entry.loading = false;
        entry.failed = true;
        self.stats.dense_prefetch_failures += 1;
        self.dense_host_prefetch.unlock();
        return;
    };

    self.dense_host_prefetch.lock();
    defer self.dense_host_prefetch.unlock();
    entry.host = loaded.host;
    entry.read_ns = loaded.read_ns;
    entry.loading = false;
    entry.ready = true;
    self.stats.dense_prefetch_host_read_ns +|= loaded.read_ns;
    self.stats.dense_prefetch_read_bytes += loaded.host.len;
    evictDenseHostPrefetchToBudgetLocked(self, entry.name, loaded.host.len);
    if (self.dense_host_prefetch_entries.get(entry.name) == entry) {
        self.stats.dense_prefetch_resident_bytes += loaded.host.len;
        if (cudaDensePrefetchProfileEnabled()) {
            std.log.info("cuda_dense_prefetch: loaded name={s} mb={d} read_ms={d} resident_mb={d}", .{
                entry.name,
                loaded.host.len / (1024 * 1024),
                loaded.read_ns / 1_000_000,
                self.stats.dense_prefetch_resident_bytes / (1024 * 1024),
            });
        }
    }
}

fn isDenseFfnStreamWeightName(name: []const u8) bool {
    if (std.mem.indexOf(u8, name, ".mlp.") == null) return false;
    return std.mem.endsWith(u8, name, ".mlp.gate_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.up_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.down_proj.weight");
}

fn isDenseAttentionStreamWeightName(name: []const u8) bool {
    if (std.mem.indexOf(u8, name, ".self_attn.") == null) return false;
    return std.mem.endsWith(u8, name, ".self_attn.q_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.k_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.v_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.o_proj.weight");
}

fn isDenseStreamWeightName(self: *const CudaCompute, name: []const u8) bool {
    if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DENSE_STREAM", cudaAutoDenseStreamEnabled(self))) {
        return isDenseFfnStreamWeightName(name) or isDenseAttentionStreamWeightName(name);
    }
    return cudaFfnStreamEnabled() and isDenseFfnStreamWeightName(name);
}

fn findDenseStreamEntry(self: *CudaCompute, name: []const u8) ?*DenseStreamEntry {
    for (self.dense_stream_entries.items) |*entry| {
        if (std.mem.eql(u8, entry.name, name)) return entry;
    }
    return null;
}

fn evictOneDenseStreamEntry(self: *CudaCompute, protected_name: []const u8) !void {
    var victim_index: ?usize = null;
    var oldest_epoch: u64 = std.math.maxInt(u64);
    for (self.dense_stream_entries.items, 0..) |*entry, index| {
        if (std.mem.eql(u8, entry.name, protected_name)) continue;
        if (entry.last_access < oldest_epoch) {
            oldest_epoch = entry.last_access;
            victim_index = index;
        }
    }
    const index = victim_index orelse return error.MemoryBudgetExceeded;
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.stream_syncs += 1;
    const entry = self.dense_stream_entries.orderedRemove(index);
    const bytes = entry.byte_len;
    freeCudaTensorStorageUncached(self, entry.tensor);
    self.allocator.destroy(entry.tensor);
    if (entry.kind == .mlp) {
        self.stats.ffn_stream_resident_bytes -|= bytes;
        self.stats.ffn_stream_evictions += 1;
    }
    self.stats.dense_stream_resident_bytes -|= bytes;
    self.stats.resident_weight_bytes -|= bytes;
    self.stats.dense_stream_evictions += 1;
    self.allocator.free(entry.name);
}

fn ensureDenseStreamBudgetForInsert(self: *CudaCompute, protected_name: []const u8, byte_len: usize) !void {
    const budget = cudaDenseStreamBudgetBytes(self);
    if (byte_len > budget) return error.MemoryBudgetExceeded;
    while (self.stats.dense_stream_resident_bytes + byte_len > budget) {
        try evictOneDenseStreamEntry(self, protected_name);
    }
}

fn uploadDenseWeightFromHost(
    self: *CudaCompute,
    name: []const u8,
    dtype: tensor_mod.DType,
    shape_src: []const i64,
    byte_len: usize,
    host: []const u8,
    read_ns: u64,
    from_prefetch: bool,
) !*CudaTensor {
    if (dtype != .bf16) return error.UnsupportedTensorType;
    if (shape_src.len < 2) return error.InvalidShape;
    if (byte_len == 0 or host.len != byte_len) return error.InvalidShape;
    try ensureDenseStreamBudgetForInsert(self, name, byte_len);
    const stream_kind: DenseStreamEntry.Kind = if (isDenseAttentionStreamWeightName(name)) .attention else .mlp;

    if (!from_prefetch) {
        if (stream_kind == .mlp) self.stats.ffn_stream_read_ns +|= read_ns;
        self.stats.dense_stream_read_ns +|= read_ns;
        if (stream_kind == .mlp) self.stats.ffn_stream_read_bytes += host.len;
        self.stats.dense_stream_read_bytes += host.len;
    }

    const shape = try self.allocator.dupe(i64, shape_src);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, host.len);
    errdefer device.free(&self.ctx);
    const h2d_start = platform.time.monotonicNs();
    try copyFromHostTracked(self, device, host);
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.upload_syncs += 1;
    const h2d_ns = elapsedNsSince(h2d_start);
    if (stream_kind == .mlp) self.stats.ffn_stream_h2d_ns +|= h2d_ns;
    self.stats.dense_stream_h2d_ns +|= h2d_ns;
    if (from_prefetch) self.stats.dense_prefetch_upload_ns +|= h2d_ns;
    if (stream_kind == .mlp) self.stats.ffn_stream_uploaded_bytes += host.len;
    self.stats.dense_stream_uploaded_bytes += host.len;

    const owned_name = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(owned_name);
    const tensor = try self.allocator.create(CudaTensor);
    errdefer self.allocator.destroy(tensor);
    const elem_count = byte_len / @sizeOf(u16);
    tensor.* = .{
        .buffer = device,
        .dtype = .bf16,
        .shape = shape,
        .elem_count = elem_count,
        .quant_type = null,
        .owns_buffer = true,
        .owns_shape = true,
        .owned_by_tensor = false,
    };
    self.dense_stream_epoch +|= 1;
    try self.dense_stream_entries.append(self.allocator, .{
        .name = owned_name,
        .tensor = tensor,
        .byte_len = host.len,
        .kind = stream_kind,
        .last_access = self.dense_stream_epoch,
    });
    if (stream_kind == .mlp) self.stats.ffn_stream_resident_bytes += host.len;
    self.stats.dense_stream_resident_bytes += host.len;
    self.stats.resident_weight_bytes += host.len;
    if (stream_kind == .attention) {
        self.stats.dense_stream_attention_loads += 1;
    } else {
        self.stats.dense_stream_mlp_loads += 1;
    }
    if (cudaDenseStreamProfileEnabled()) {
        std.log.info("cuda_dense_stream: loaded name={s} mb={d} read_ms={d} h2d_ms={d} entries={d} resident_mb={d}", .{
            name,
            host.len / (1024 * 1024),
            read_ns / 1_000_000,
            h2d_ns / 1_000_000,
            self.dense_stream_entries.items.len,
            self.stats.dense_stream_resident_bytes / (1024 * 1024),
        });
    }
    return tensor;
}

fn streamDenseWeightFromRange(self: *CudaCompute, name: []const u8, range: tensor_store_mod.TensorRangeRef) !*CudaTensor {
    if (range.dtype != .bf16) return error.UnsupportedTensorType;
    if (range.shape.len < 2) return error.InvalidShape;
    if (range.byte_len == 0) return error.InvalidShape;
    const is_mlp = isDenseFfnStreamWeightName(name);
    if (is_mlp) self.stats.ffn_stream_fadvise_calls += 2;
    self.stats.dense_prefetch_sync_reads += 1;
    const loaded = try readDenseHostBytes(self, range.path, range.byte_offset, range.byte_len);
    defer self.allocator.free(loaded.host);
    return try uploadDenseWeightFromHost(self, name, range.dtype, range.shape, range.byte_len, loaded.host, loaded.read_ns, false);
}

fn takeDenseHostPrefetchedWeight(self: *CudaCompute, name: []const u8) !?*CudaTensor {
    if (!cudaDensePrefetchEnabled(self) or !self.dense_host_prefetch_initialized) return null;
    const wait_start = platform.time.monotonicNs();
    var waited = false;
    while (true) {
        self.dense_host_prefetch.lock();
        const entry = self.dense_host_prefetch_entries.get(name) orelse {
            self.dense_host_prefetch.unlock();
            if (waited) self.stats.dense_prefetch_demand_wait_ns +|= elapsedNsSince(wait_start);
            return null;
        };
        self.dense_host_prefetch_epoch +|= 1;
        entry.last_access = self.dense_host_prefetch_epoch;
        if (entry.ready) {
            _ = self.dense_host_prefetch_entries.remove(name);
            self.stats.dense_prefetch_resident_bytes -|= entry.host.len;
            const host = entry.host;
            entry.host = &.{};
            const read_ns = entry.read_ns;
            self.dense_host_prefetch.unlock();
            if (waited) self.stats.dense_prefetch_demand_wait_ns +|= elapsedNsSince(wait_start);
            self.stats.dense_prefetch_ready_hits += 1;
            defer self.allocator.free(host);
            defer {
                entry.deinit(self.allocator);
                self.allocator.destroy(entry);
            }
            return try uploadDenseWeightFromHost(self, name, entry.range.dtype, entry.range.shape, entry.range.byte_len, host, read_ns, true);
        }
        if (entry.pending) {
            _ = removeDenseHostPrefetchQueueItemLocked(self, entry);
            _ = self.dense_host_prefetch_entries.remove(name);
            self.dense_host_prefetch.unlock();
            if (waited) self.stats.dense_prefetch_demand_wait_ns +|= elapsedNsSince(wait_start);
            self.stats.dense_prefetch_inflight_steals += 1;
            self.stats.dense_prefetch_sync_reads += 1;
            if (entry.kind == .mlp) self.stats.ffn_stream_fadvise_calls += 2;
            const loaded = try readDenseHostBytes(self, entry.range.path, entry.range.byte_offset, entry.range.byte_len);
            defer self.allocator.free(loaded.host);
            defer {
                entry.deinit(self.allocator);
                self.allocator.destroy(entry);
            }
            return try uploadDenseWeightFromHost(self, name, entry.range.dtype, entry.range.shape, entry.range.byte_len, loaded.host, loaded.read_ns, false);
        }
        if (entry.failed) {
            _ = self.dense_host_prefetch_entries.remove(name);
            self.dense_host_prefetch.unlock();
            if (waited) self.stats.dense_prefetch_demand_wait_ns +|= elapsedNsSince(wait_start);
            entry.deinit(self.allocator);
            self.allocator.destroy(entry);
            return null;
        }
        self.dense_host_prefetch.unlock();
        waited = true;
        platform.time.yieldBriefly();
    }
}

fn enqueueDenseHostPrefetchHint(self: *CudaCompute, name: []const u8, hint: u32) void {
    if (!cudaDensePrefetchEnabled(self) or !self.dense_host_prefetch_initialized) return;
    if (self.resident_weights.contains(name)) return;
    self.dense_host_prefetch.lock();
    if (findDenseStreamEntry(self, name) != null) {
        self.dense_host_prefetch.unlock();
        return;
    }
    if (self.dense_host_prefetch_entries.get(name)) |entry| {
        entry.priority +|= hint;
        self.dense_host_prefetch_epoch +|= 1;
        entry.last_access = self.dense_host_prefetch_epoch;
        self.stats.dense_prefetch_duplicates += 1;
        self.dense_host_prefetch.signal();
        self.dense_host_prefetch.unlock();
        return;
    }
    self.dense_host_prefetch.unlock();

    const entry = createDenseHostPrefetchEntry(self, name, hint) catch {
        self.stats.dense_prefetch_failures += 1;
        return;
    };
    self.dense_host_prefetch.lock();
    if (self.dense_host_prefetch_entries.get(name)) |existing| {
        existing.priority +|= hint;
        self.stats.dense_prefetch_duplicates += 1;
        self.dense_host_prefetch.unlock();
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
        return;
    }
    self.dense_host_prefetch_entries.put(self.allocator, entry.name, entry) catch {
        self.dense_host_prefetch.unlock();
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
        self.stats.dense_prefetch_failures += 1;
        return;
    };
    self.dense_host_prefetch.items.append(self.allocator, entry) catch {
        _ = self.dense_host_prefetch_entries.remove(entry.name);
        self.dense_host_prefetch.unlock();
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
        self.stats.dense_prefetch_failures += 1;
        return;
    };
    self.stats.dense_prefetch_enqueues += 1;
    self.dense_host_prefetch.signal();
    self.dense_host_prefetch.unlock();
}

fn getDenseStreamWeight(self: *CudaCompute, name: []const u8) !?*CudaTensor {
    if (!cudaDenseStreamEnabled(self)) return null;
    if (!isDenseStreamWeightName(self, name)) return null;
    const store = self.lazy_host_store orelse return null;
    const tensor_store = store.tensor_store orelse return null;
    const is_mlp = isDenseFfnStreamWeightName(name);
    if (is_mlp) self.stats.ffn_stream_requests += 1;
    self.stats.dense_stream_requests += 1;
    self.dense_stream_epoch +|= 1;
    if (findDenseStreamEntry(self, name)) |entry| {
        entry.last_access = self.dense_stream_epoch;
        if (is_mlp) self.stats.ffn_stream_hits += 1;
        self.stats.dense_stream_hits += 1;
        return entry.tensor;
    }
    if (is_mlp) self.stats.ffn_stream_misses += 1;
    self.stats.dense_stream_misses += 1;
    if (try takeDenseHostPrefetchedWeight(self, name)) |tensor| return tensor;
    var range = (try tensor_store.describeTensorRange(self.allocator, name)) orelse return null;
    defer range.deinit(self.allocator);
    return try streamDenseWeightFromRange(self, name, range);
}

fn copyFromHostTracked(self: *CudaCompute, device: buffer_mod.DeviceBuffer, bytes: []const u8) !void {
    if (self.debug_cuda_graph_capture_active) {
        self.debug_cuda_graph_capture_disabled = true;
        std.log.warn("cuda_graph_capture_probe: unsafe_h2d_copy bytes={d} disabled=1", .{bytes.len});
        return error.CudaGraphCaptureUnsafeHostCopy;
    }
    try device.copyFromHost(&self.ctx, bytes);
    self.stats.h2d_bytes += bytes.len;
}

const pinned_scalar_upload_slot_size: usize = 64;
const pinned_scalar_upload_slot_count: usize = 2048;
const pinned_upload_arena_capacity: usize = 1024 * 1024;
const pinned_upload_arena_alignment: usize = 64;

fn cudaPinnedScalarUploadsDisabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_PINNED_SCALAR_UPLOADS", false);
}

fn cudaPinnedUploadArenaDisabled() bool {
    return platform.env.getenvBoolDefault(
        "ANTFLY_INFERENCE_CUDA_DISABLE_PINNED_UPLOAD_ARENA",
        cudaPinnedScalarUploadsDisabled(),
    );
}

fn cudaPinnedScalarDownloadsDisabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_PINNED_SCALAR_DOWNLOADS", false);
}

fn cudaAsyncI32ScalarDownloadStagingEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.async_i32_download_staging;
}

// Resolved once at CudaCompute.init: the SM89-promoted decode/prefill tuning
// battery frozen from the gemma4-e2b-sm89-flash-splitk-v1 execution profile.
// Every route here is exact-output (bit-identical to its fallback) and keeps
// its shape/quant eligibility guard at the dispatch site; only the meaning of
// an UNSET environment variable depends on the compute capability. Explicit
// env values keep working in both directions on every target.
pub const CudaTunedRouteGates = struct {
    linear_q8_1_dp4a: bool = false,
    qkv_q8_1_dp4a: bool = false,
    qkv_q8_1_tile8: bool = false,
    pair_q8_1_dp4a: bool = false,
    pair_activation_q8_1_dp4a: bool = false,
    activation_slice_q8_1_dp4a: bool = false,
    gated_down_q8_1_dp4a: bool = false,
    gate_up_activation_precompute: bool = false,
    q8_1_prefill_rows: bool = false,
    linear_q8_1_tile4_w8: bool = false,
    linear_q8_1_tile4_w8_min_in_dim: usize = 8192,
    pair_q8_1_tile4_w8: bool = false,
    q6_k_lm_head_q8_1: bool = false,
    lm_head_q8_1_argmax: bool = false,
    async_i32_download_staging: bool = false,
    // Opt-in (default off, never auto-promoted): route Q4_0 prefill projections
    // through the W4A16 tensor-core (tc_hmma) kernel, and fuse gate+up into a
    // single concatenated cuBLASLt GEMM. Kept out of promotedDefaultsForTarget
    // so they can be A/B'd against the cuBLASLt-BF16 mirror before promotion.
    q4_0_tc_hmma_prefill: bool = false,
    fused_gate_up_bf16: bool = false,

    /// Defaults when every env variable is unset. The promoted battery is
    /// default-on only on compute capability 8.9 (the qualified L4 target);
    /// every other target keeps the historical opt-in defaults.
    fn promotedDefaultsForTarget(compute_major: i32, compute_minor: i32) CudaTunedRouteGates {
        if (compute_major != 8 or compute_minor != 9) return .{};
        return .{
            .linear_q8_1_dp4a = true,
            .qkv_q8_1_dp4a = true,
            .qkv_q8_1_tile8 = true,
            .pair_q8_1_dp4a = true,
            .pair_activation_q8_1_dp4a = true,
            .activation_slice_q8_1_dp4a = true,
            .gated_down_q8_1_dp4a = true,
            .gate_up_activation_precompute = true,
            .q8_1_prefill_rows = true,
            .linear_q8_1_tile4_w8 = true,
            .linear_q8_1_tile4_w8_min_in_dim = 2048,
            .pair_q8_1_tile4_w8 = true,
            .q6_k_lm_head_q8_1 = true,
            .lm_head_q8_1_argmax = true,
            .async_i32_download_staging = true,
            // W4A16 bf16 tensor-core prefill projections: 1.81x faster than the
            // DP4A q8_1 path AND higher quality (96/96 F32-reference token match
            // vs DP4A's 30/96). Handles prefill projections (rows>1); DP4A stays
            // for decode (rows==1). Rollback: ANTFLY_INFERENCE_CUDA_Q4_0_TC_HMMA_PREFILL=0.
            .q4_0_tc_hmma_prefill = true,
        };
    }

    fn resolveForTarget(compute_major: i32, compute_minor: i32) CudaTunedRouteGates {
        const defaults = promotedDefaultsForTarget(compute_major, compute_minor);
        const env = platform.env;
        return .{
            .linear_q8_1_dp4a = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A", defaults.linear_q8_1_dp4a),
            .qkv_q8_1_dp4a = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_DP4A", defaults.qkv_q8_1_dp4a),
            .qkv_q8_1_tile8 = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_TILE8", defaults.qkv_q8_1_tile8),
            .pair_q8_1_dp4a = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A", defaults.pair_q8_1_dp4a),
            .pair_activation_q8_1_dp4a = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A", defaults.pair_activation_q8_1_dp4a),
            .activation_slice_q8_1_dp4a = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_ACTIVATION_SLICE_Q8_1_DP4A", defaults.activation_slice_q8_1_dp4a),
            .gated_down_q8_1_dp4a = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_Q8_1_DP4A", defaults.gated_down_q8_1_dp4a),
            .gate_up_activation_precompute = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_PRECOMPUTE", defaults.gate_up_activation_precompute),
            .q8_1_prefill_rows = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_Q8_1_PREFILL_ROWS", defaults.q8_1_prefill_rows),
            .linear_q8_1_tile4_w8 = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8", defaults.linear_q8_1_tile4_w8),
            .linear_q8_1_tile4_w8_min_in_dim = env.getenvUsize("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8_MIN_IN_DIM") orelse defaults.linear_q8_1_tile4_w8_min_in_dim,
            .pair_q8_1_tile4_w8 = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_TILE4_W8", defaults.pair_q8_1_tile4_w8),
            .q6_k_lm_head_q8_1 = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1", defaults.q6_k_lm_head_q8_1),
            .lm_head_q8_1_argmax = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX", defaults.lm_head_q8_1_argmax),
            .async_i32_download_staging = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING", defaults.async_i32_download_staging),
            .q4_0_tc_hmma_prefill = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_TC_HMMA_PREFILL", defaults.q4_0_tc_hmma_prefill),
            .fused_gate_up_bf16 = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_FUSED_GATE_UP_BF16", defaults.fused_gate_up_bf16),
        };
    }
};

// Resolved once at CudaCompute.init: the SM89-promoted graph-capture extras
// from the same tuned execution profile. These only change behavior on code
// paths that are separately gated on graph capture/replay being requested and
// batch-1 decode; the env variables keep overriding in both directions.
pub const CudaCaptureGateConfig = struct {
    persistent_replay: bool = false,
    update_exec: bool = false,
    device_scalars: bool = false,

    fn promotedDefaultsForTarget(compute_major: i32, compute_minor: i32) CudaCaptureGateConfig {
        if (compute_major != 8 or compute_minor != 9) return .{};
        return .{
            .persistent_replay = true,
            .update_exec = true,
            .device_scalars = true,
        };
    }

    fn resolveForTarget(compute_major: i32, compute_minor: i32) CudaCaptureGateConfig {
        const defaults = promotedDefaultsForTarget(compute_major, compute_minor);
        const env = platform.env;
        return .{
            .persistent_replay = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY", defaults.persistent_replay),
            .update_exec = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC", defaults.update_exec),
            .device_scalars = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_DEVICE_SCALARS", defaults.device_scalars),
        };
    }
};

extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern fn unsetenv(name: [*:0]const u8) c_int;

const TestEnvGuard = struct {
    name: [:0]const u8,
    saved: ?[:0]u8,

    fn captureAndClear(allocator: std.mem.Allocator, name: [:0]const u8) !TestEnvGuard {
        const saved: ?[:0]u8 = if (platform.env.getenv(name.ptr)) |value|
            try allocator.dupeZ(u8, value)
        else
            null;
        _ = unsetenv(name.ptr);
        return .{ .name = name, .saved = saved };
    }

    fn restore(self: *TestEnvGuard, allocator: std.mem.Allocator) void {
        if (self.saved) |value| {
            _ = setenv(self.name.ptr, value.ptr, 1);
            allocator.free(value);
        } else {
            _ = unsetenv(self.name.ptr);
        }
    }
};

const tuned_route_gate_test_env_names = [_][:0]const u8{
    "ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A",
    "ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_DP4A",
    "ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_TILE8",
    "ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A",
    "ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A",
    "ANTFLY_INFERENCE_CUDA_Q4_0_ACTIVATION_SLICE_Q8_1_DP4A",
    "ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_Q8_1_DP4A",
    "ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_PRECOMPUTE",
    "ANTFLY_INFERENCE_CUDA_Q4_0_Q8_1_PREFILL_ROWS",
    "ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8",
    "ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8_MIN_IN_DIM",
    "ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_TILE4_W8",
    "ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX",
    "ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING",
};

const capture_gate_test_env_names = [_][:0]const u8{
    "ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_DEVICE_SCALARS",
};

test "CUDA tuned route gates promote the SM89 battery only on compute 8.9" {
    const promoted = CudaTunedRouteGates.promotedDefaultsForTarget(8, 9);
    try std.testing.expect(promoted.linear_q8_1_dp4a);
    try std.testing.expect(promoted.qkv_q8_1_dp4a);
    try std.testing.expect(promoted.qkv_q8_1_tile8);
    try std.testing.expect(promoted.pair_q8_1_dp4a);
    try std.testing.expect(promoted.pair_activation_q8_1_dp4a);
    try std.testing.expect(promoted.activation_slice_q8_1_dp4a);
    try std.testing.expect(promoted.gated_down_q8_1_dp4a);
    try std.testing.expect(promoted.gate_up_activation_precompute);
    try std.testing.expect(promoted.q8_1_prefill_rows);
    try std.testing.expect(promoted.linear_q8_1_tile4_w8);
    try std.testing.expectEqual(@as(usize, 2048), promoted.linear_q8_1_tile4_w8_min_in_dim);
    try std.testing.expect(promoted.pair_q8_1_tile4_w8);
    try std.testing.expect(promoted.q6_k_lm_head_q8_1);
    try std.testing.expect(promoted.lm_head_q8_1_argmax);
    try std.testing.expect(promoted.async_i32_download_staging);
    // W4A16 bf16 tensor-core prefill projections are promoted default-on for
    // SM89 (1.81x faster than DP4A q8_1 and higher quality: 96/96 vs 30/96
    // F32-reference token match).
    try std.testing.expect(promoted.q4_0_tc_hmma_prefill);

    // Every other compute capability keeps the historical opt-in defaults.
    try std.testing.expect(std.meta.eql(
        CudaTunedRouteGates{},
        CudaTunedRouteGates.promotedDefaultsForTarget(8, 6),
    ));
    try std.testing.expect(std.meta.eql(
        CudaTunedRouteGates{},
        CudaTunedRouteGates.promotedDefaultsForTarget(9, 0),
    ));
    try std.testing.expect(std.meta.eql(
        CudaTunedRouteGates{},
        CudaTunedRouteGates.promotedDefaultsForTarget(7, 5),
    ));
    try std.testing.expect(!(CudaTunedRouteGates{}).linear_q8_1_dp4a);
    try std.testing.expectEqual(@as(usize, 8192), (CudaTunedRouteGates{}).linear_q8_1_tile4_w8_min_in_dim);
}

test "W4A16 prefill never preempts an available BF16 mirror route" {
    try std.testing.expect(shouldTryQ4_0TcHmmaPrefill(true, 2, false));
    try std.testing.expect(!shouldTryQ4_0TcHmmaPrefill(true, 2, true));
    try std.testing.expect(!shouldTryQ4_0TcHmmaPrefill(true, 1, false));
    try std.testing.expect(!shouldTryQ4_0TcHmmaPrefill(false, 512, false));
}

test "CUDA tuned route gates keep env overrides working in both directions" {
    const allocator = std.testing.allocator;
    var guards: [tuned_route_gate_test_env_names.len]TestEnvGuard = undefined;
    var guarded: usize = 0;
    defer for (guards[0..guarded]) |*guard| guard.restore(allocator);
    for (tuned_route_gate_test_env_names) |name| {
        guards[guarded] = try TestEnvGuard.captureAndClear(allocator, name);
        guarded += 1;
    }

    // Unset: the promoted SM89 defaults apply on 8.9, historical defaults elsewhere.
    try std.testing.expect(std.meta.eql(
        CudaTunedRouteGates.promotedDefaultsForTarget(8, 9),
        CudaTunedRouteGates.resolveForTarget(8, 9),
    ));
    try std.testing.expect(std.meta.eql(
        CudaTunedRouteGates{},
        CudaTunedRouteGates.resolveForTarget(8, 6),
    ));

    // Explicit 0 disables promoted defaults on SM89.
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A", "0", 1));
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_TILE8", "0", 1));
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1", "0", 1));
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8_MIN_IN_DIM", "8192", 1));
    const sm89_overridden = CudaTunedRouteGates.resolveForTarget(8, 9);
    try std.testing.expect(!sm89_overridden.linear_q8_1_dp4a);
    try std.testing.expect(!sm89_overridden.qkv_q8_1_tile8);
    try std.testing.expect(!sm89_overridden.q6_k_lm_head_q8_1);
    try std.testing.expectEqual(@as(usize, 8192), sm89_overridden.linear_q8_1_tile4_w8_min_in_dim);
    // Untouched knobs keep their promoted defaults.
    try std.testing.expect(sm89_overridden.pair_q8_1_dp4a);
    try std.testing.expect(sm89_overridden.async_i32_download_staging);

    // Explicit 1 still opts in on non-promoted targets.
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A", "1", 1));
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8", "1", 1));
    const sm86_overridden = CudaTunedRouteGates.resolveForTarget(8, 6);
    try std.testing.expect(sm86_overridden.pair_q8_1_dp4a);
    try std.testing.expect(sm86_overridden.linear_q8_1_tile4_w8);
    try std.testing.expect(!sm86_overridden.linear_q8_1_dp4a);
    try std.testing.expect(!sm86_overridden.gated_down_q8_1_dp4a);
    try std.testing.expectEqual(@as(usize, 8192), sm86_overridden.linear_q8_1_tile4_w8_min_in_dim);
}

test "CUDA capture gates promote SM89 defaults with bidirectional env overrides" {
    try std.testing.expect(std.meta.eql(
        CudaCaptureGateConfig{ .persistent_replay = true, .update_exec = true, .device_scalars = true },
        CudaCaptureGateConfig.promotedDefaultsForTarget(8, 9),
    ));
    try std.testing.expect(std.meta.eql(
        CudaCaptureGateConfig{},
        CudaCaptureGateConfig.promotedDefaultsForTarget(8, 6),
    ));
    try std.testing.expect(std.meta.eql(
        CudaCaptureGateConfig{},
        CudaCaptureGateConfig.promotedDefaultsForTarget(9, 0),
    ));

    const allocator = std.testing.allocator;
    var guards: [capture_gate_test_env_names.len]TestEnvGuard = undefined;
    var guarded: usize = 0;
    defer for (guards[0..guarded]) |*guard| guard.restore(allocator);
    for (capture_gate_test_env_names) |name| {
        guards[guarded] = try TestEnvGuard.captureAndClear(allocator, name);
        guarded += 1;
    }

    try std.testing.expect(std.meta.eql(
        CudaCaptureGateConfig.promotedDefaultsForTarget(8, 9),
        CudaCaptureGateConfig.resolveForTarget(8, 9),
    ));
    try std.testing.expect(std.meta.eql(
        CudaCaptureGateConfig{},
        CudaCaptureGateConfig.resolveForTarget(8, 6),
    ));

    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY", "0", 1));
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC", "1", 1));
    const sm89 = CudaCaptureGateConfig.resolveForTarget(8, 9);
    try std.testing.expect(!sm89.persistent_replay);
    try std.testing.expect(sm89.update_exec);
    try std.testing.expect(sm89.device_scalars);
    const sm86 = CudaCaptureGateConfig.resolveForTarget(8, 6);
    try std.testing.expect(!sm86.persistent_replay);
    try std.testing.expect(sm86.update_exec);
    try std.testing.expect(!sm86.device_scalars);
}

fn cudaQ4_0DecodeTile8GlobalEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_DECODE_TILE8", false);
}

fn cudaQ4_0LinearTile8Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_TILE8", cudaQ4_0DecodeTile8GlobalEnabled());
}

fn cudaQ4_0LinearTile4W4Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_TILE4_W4", false);
}

fn cudaQ4_0LinearQ8_1Dp4aEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.linear_q8_1_dp4a;
}

fn generatedQ4_0RouteDefaultEnabled(comptime kernel_id: []const u8) bool {
    const artifact = quant_kernel_compiler.generatedArtifactForKernel(.cuda, kernel_id) orelse
        @compileError("missing generated Q4_0 artifact: " ++ kernel_id);
    return artifact.runtime_default_enabled;
}

const generated_q4_0_mmv_default_enabled = generatedQ4_0RouteDefaultEnabled(quant_kernel_compiler.first_general_cuda_q4_0_mmv_kernel_id);
const generated_q4_0_mm_default_enabled = generatedQ4_0RouteDefaultEnabled(quant_kernel_compiler.first_general_cuda_q4_0_mm_kernel_id);
const generated_q4_0_pair_default_enabled = generatedQ4_0RouteDefaultEnabled(quant_kernel_compiler.first_general_cuda_q4_0_pair_kernel_id);
const generated_q4_0_pair_q8_default_enabled = generatedQ4_0RouteDefaultEnabled(quant_kernel_compiler.first_general_cuda_q4_0_pair_q8_kernel_id);
const generated_q4_0_down_q8_default_enabled = generatedQ4_0RouteDefaultEnabled(quant_kernel_compiler.first_general_cuda_q4_0_down_q8_kernel_id);

fn cudaQ4_0GeneratedMmvEnabled() bool {
    if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_MMV", false)) return false;
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MMV", generated_q4_0_mmv_default_enabled);
}

fn cudaQ4_0GeneratedMmEnabled() bool {
    if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_MM", false)) return false;
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MM", generated_q4_0_mm_default_enabled);
}

fn cudaQ4_0GeneratedPairEnabled() bool {
    if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_PAIR", false)) return false;
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR", generated_q4_0_pair_default_enabled);
}

fn cudaQ4_0GeneratedPairQ8Enabled() bool {
    if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_PAIR_Q8", false)) return false;
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR_Q8", generated_q4_0_pair_q8_default_enabled);
}

fn cudaQ4_0GeneratedDownQ8Enabled() bool {
    if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_DOWN_Q8", false)) return false;
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_DOWN_Q8", generated_q4_0_down_q8_default_enabled);
}

fn cudaQ4_0GeneratedCatalogFfnCandidatesEnabled() bool {
    const legacy = platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN", false);
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_CATALOG_FFN_CANDIDATES", legacy);
}

fn cudaQ4_0GeneratedExactFfnCandidatesEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT", false);
}

fn cudaQ4_0GeneratedE2BFfnPairOnlyEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY", false);
}

// Resolved once at CudaCompute.init: these gates sit on the per-op linear
// dispatch path and getenv is a linear scan of environ.
pub const GeneratedQ4_0Gates = struct {
    mmv: bool = generated_q4_0_mmv_default_enabled,
    mm: bool = generated_q4_0_mm_default_enabled,
    pair: bool = generated_q4_0_pair_default_enabled,
    pair_q8: bool = generated_q4_0_pair_q8_default_enabled,
    down_q8: bool = generated_q4_0_down_q8_default_enabled,
    catalog_ffn_candidates: bool = false,
    exact_ffn_candidates: bool = false,
    e2b_pair_only: bool = false,

    fn resolveForTarget(compute_major: i32, compute_minor: i32) GeneratedQ4_0Gates {
        const requested = GeneratedQ4_0Gates{
            .mmv = cudaQ4_0GeneratedMmvEnabled(),
            .mm = cudaQ4_0GeneratedMmEnabled(),
            .pair = cudaQ4_0GeneratedPairEnabled(),
            .pair_q8 = cudaQ4_0GeneratedPairQ8Enabled(),
            .down_q8 = cudaQ4_0GeneratedDownQ8Enabled(),
            .catalog_ffn_candidates = cudaQ4_0GeneratedCatalogFfnCandidatesEnabled(),
            .exact_ffn_candidates = cudaQ4_0GeneratedExactFfnCandidatesEnabled(),
            .e2b_pair_only = cudaQ4_0GeneratedE2BFfnPairOnlyEnabled(),
        };
        return requested.restrictToPromotedTarget(
            compute_major,
            compute_minor,
            platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_ALLOW_UNPROMOTED_GENERATED_KERNELS", false),
        ).withMasterDisable(platform.env.getenvBoolDefault(
            "ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0",
            false,
        ));
    }

    fn fromJitRoutes(routes: kernels_mod.JitProductionRoutes) GeneratedQ4_0Gates {
        return .{
            .mmv = routes.mmv,
            .mm = routes.mm,
            .pair = routes.pair,
            .pair_q8 = routes.pair_q8,
            .down_q8 = routes.down_q8,
        };
    }

    fn withExplicitDisables(self: GeneratedQ4_0Gates) GeneratedQ4_0Gates {
        var result = self;
        result.mmv = result.mmv and
            !platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_MMV", false);
        result.mm = result.mm and
            !platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_MM", false);
        result.pair = result.pair and
            !platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_PAIR", false);
        result.pair_q8 = result.pair_q8 and
            !platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_PAIR_Q8", false);
        result.down_q8 = result.down_q8 and
            !platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_DOWN_Q8", false);
        return result.withMasterDisable(platform.env.getenvBoolDefault(
            "ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0",
            false,
        ));
    }

    fn withMasterDisable(self: GeneratedQ4_0Gates, disabled: bool) GeneratedQ4_0Gates {
        return if (disabled) allDisabled() else self;
    }

    fn allDisabled() GeneratedQ4_0Gates {
        return .{
            .mmv = false,
            .mm = false,
            .pair = false,
            .pair_q8 = false,
            .down_q8 = false,
            .catalog_ffn_candidates = false,
            .exact_ffn_candidates = false,
            .e2b_pair_only = false,
        };
    }

    fn restrictToPromotedTarget(
        self: GeneratedQ4_0Gates,
        compute_major: i32,
        compute_minor: i32,
        allow_unpromoted: bool,
    ) GeneratedQ4_0Gates {
        if (allow_unpromoted or quant_kernel_catalog.targetForComputeCapability(compute_major, compute_minor) != null) {
            return self;
        }
        return allDisabled();
    }
};

const RuntimeJitShapeRoute = enum { mmv, mm, pair, pair_q8, down_q8 };

fn runtimeJitShapeAllows(
    enabled: bool,
    scope: kernels_mod.JitRouteScope,
    route: RuntimeJitShapeRoute,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) bool {
    if (!enabled) return true;
    return switch (route) {
        .mmv => rows == 1 and scope.production.mmv and scope.containsMmvShape(out_dim, in_dim),
        .mm => rows >= 9 and rows <= 64 and scope.production.mm and scope.containsMmShape(out_dim, in_dim),
        .pair => rows == 1 and scope.production.pair and scope.containsPairShape(out_dim, in_dim),
        .pair_q8 => scope.production.pair_q8 and
            kernels_mod.generatedQ4_0PairQ8E4BShapeEligible(rows, in_dim, out_dim),
        .down_q8 => scope.production.down_q8 and
            kernels_mod.generatedQ4_0DownQ8E4BShapeEligible(rows, in_dim, out_dim),
    };
}

fn runtimeJitShapeAllowsForCompute(
    self: *const CudaCompute,
    route: RuntimeJitShapeRoute,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) bool {
    return runtimeJitShapeAllows(
        self.runtime_jit_shape_gating,
        self.kernels.runtime_jit_qualified_scope,
        route,
        rows,
        in_dim,
        out_dim,
    );
}

test "generated Q4 routes require promoted CUDA target evidence" {
    const requested = GeneratedQ4_0Gates{
        .mmv = true,
        .mm = true,
        .pair = true,
        .pair_q8 = true,
        .down_q8 = true,
        .catalog_ffn_candidates = true,
        .exact_ffn_candidates = true,
        .e2b_pair_only = true,
    };
    try std.testing.expect(std.meta.eql(requested, requested.restrictToPromotedTarget(8, 9, false)));

    const blocked = requested.restrictToPromotedTarget(8, 0, false);
    try std.testing.expect(!blocked.mmv);
    try std.testing.expect(!blocked.mm);
    try std.testing.expect(!blocked.pair);
    try std.testing.expect(!blocked.pair_q8);
    try std.testing.expect(!blocked.down_q8);
    try std.testing.expect(!blocked.catalog_ffn_candidates);
    try std.testing.expect(!blocked.exact_ffn_candidates);
    try std.testing.expect(!blocked.e2b_pair_only);

    try std.testing.expect(std.meta.eql(requested, requested.restrictToPromotedTarget(9, 0, true)));
}

test "generated Q4 routes are opt in and master disable wins" {
    try std.testing.expect(!generated_q4_0_mmv_default_enabled);
    try std.testing.expect(!generated_q4_0_mm_default_enabled);
    try std.testing.expect(!generated_q4_0_pair_default_enabled);
    try std.testing.expect(!generated_q4_0_pair_q8_default_enabled);
    try std.testing.expect(!generated_q4_0_down_q8_default_enabled);
    const defaults = GeneratedQ4_0Gates{};
    try std.testing.expect(!defaults.mmv);
    try std.testing.expect(!defaults.mm);
    try std.testing.expect(!defaults.pair);
    try std.testing.expect(!defaults.pair_q8);
    try std.testing.expect(!defaults.down_q8);
    try std.testing.expect(!defaults.e2b_pair_only);

    const enabled = GeneratedQ4_0Gates{
        .mmv = true,
        .mm = true,
        .pair = true,
        .pair_q8 = true,
        .down_q8 = true,
        .catalog_ffn_candidates = true,
        .exact_ffn_candidates = true,
        .e2b_pair_only = true,
    };
    try std.testing.expect(std.meta.eql(enabled, enabled.withMasterDisable(false)));
    try std.testing.expect(std.meta.eql(GeneratedQ4_0Gates.allDisabled(), enabled.withMasterDisable(true)));
}

test "runtime JIT gates mirror only installed production routes" {
    const gates = GeneratedQ4_0Gates.fromJitRoutes(.{
        .mmv = true,
        .pair_q8 = true,
        .down_q8 = true,
    });
    try std.testing.expect(gates.mmv);
    try std.testing.expect(!gates.mm);
    try std.testing.expect(!gates.pair);
    try std.testing.expect(gates.pair_q8);
    try std.testing.expect(gates.down_q8);
    try std.testing.expect(!gates.catalog_ffn_candidates);
    try std.testing.expect(!gates.exact_ffn_candidates);
    try std.testing.expect(!gates.e2b_pair_only);
}

test "CUDA runtime JIT dispatch shape gate keeps oversized head on bundled kernels" {
    var qualified = kernels_mod.JitRouteScope{ .production = .{
        .mmv = true,
        .mm = true,
        .pair = true,
    } };
    qualified.observed_shapes[0] = .{ 10_240, 2_560 };
    qualified.observed_shape_count = 1;
    qualified.prefill_shapes[0] = .{ 10_240, 2_560 };
    qualified.prefill_shape_count = 1;
    qualified.pair_shapes[0] = .{ 10_240, 2_560 };
    qualified.pair_shape_count = 1;
    qualified.production.pair_q8 = true;
    qualified.production.down_q8 = true;

    try std.testing.expect(runtimeJitShapeAllows(true, qualified, .mmv, 1, 2_560, 10_240));
    try std.testing.expect(runtimeJitShapeAllows(true, qualified, .mm, 22, 2_560, 10_240));
    try std.testing.expect(runtimeJitShapeAllows(true, qualified, .pair, 1, 2_560, 10_240));
    try std.testing.expect(runtimeJitShapeAllows(true, qualified, .pair_q8, 1, 2_560, 10_240));
    try std.testing.expect(runtimeJitShapeAllows(true, qualified, .down_q8, 1, 10_240, 2_560));
    try std.testing.expect(!runtimeJitShapeAllows(true, qualified, .mmv, 1, 2_560, 262_144));
    try std.testing.expect(!runtimeJitShapeAllows(true, qualified, .mm, 22, 2_560, 262_144));
    try std.testing.expect(!runtimeJitShapeAllows(true, qualified, .pair_q8, 1, 1_536, 8_960));
    // Legacy explicitly-enabled generated kernels retain their pre-JIT behavior.
    try std.testing.expect(runtimeJitShapeAllows(false, .{}, .mm, 22, 2_560, 262_144));
}

fn generatedQ4_0E2BFfnPairOnlyEligible(
    enabled: bool,
    rows: usize,
    hidden_dim: usize,
    intermediate_dim: usize,
    activation: ops.DecoderRuntimeActivationKind,
) bool {
    return enabled and kernels_mod.generatedQ4_0PairQ8E2BShapeEligible(
        rows,
        hidden_dim,
        intermediate_dim,
        @intFromEnum(activation),
    );
}

test "generated CUDA E2B FFN pair-only route is narrow and opt-in" {
    try std.testing.expect(!(GeneratedQ4_0Gates{}).e2b_pair_only);
    try std.testing.expect(generatedQ4_0E2BFfnPairOnlyEligible(true, 1, 1536, 6144, .gelu_new));
    try std.testing.expect(generatedQ4_0E2BFfnPairOnlyEligible(true, 1, 1536, 12288, .gelu_new));
    try std.testing.expect(!generatedQ4_0E2BFfnPairOnlyEligible(false, 1, 1536, 6144, .gelu_new));
    try std.testing.expect(!generatedQ4_0E2BFfnPairOnlyEligible(true, 2, 1536, 6144, .gelu_new));
    try std.testing.expect(!generatedQ4_0E2BFfnPairOnlyEligible(true, 1, 2560, 10240, .gelu_new));
}

const GeneratedQ4_0CatalogFfnRoute = struct {
    pair_kernel_id: []const u8,
    down_kernel_id: []const u8,
};

const GeneratedQ4_0CatalogFfnResolution = struct {
    route: ?GeneratedQ4_0CatalogFfnRoute = null,
    attempts: usize = 0,
    misses: usize = 0,
};

fn catalogActivationFunction(activation: ops.DecoderRuntimeActivationKind) ?quant_kernel_op.ActivationFunction {
    return switch (activation) {
        .gelu => .gelu,
        .gelu_new => .gelu_new,
        .silu => .silu,
        .relu => .relu,
        .quick_gelu => .quick_gelu,
        .relu_squared => .relu_squared,
        .gelu_exact => null,
    };
}

fn generatedQ4_0CatalogFfnRoute(
    candidate_gate_enabled: bool,
    pair_q8_enabled: bool,
    down_q8_enabled: bool,
    rows: usize,
    hidden_dim: usize,
    intermediate_dim: usize,
    activation: ops.DecoderRuntimeActivationKind,
    compute_major: i32,
    compute_minor: i32,
) GeneratedQ4_0CatalogFfnResolution {
    var resolution = GeneratedQ4_0CatalogFfnResolution{};
    if (!pair_q8_enabled or !down_q8_enabled) return resolution;
    const runtime = quant_kernel_op.RuntimeShape{
        .rows = std.math.cast(u32, rows) orelse return resolution,
        .input_dim = std.math.cast(u32, hidden_dim) orelse return resolution,
        .output_dim = std.math.cast(u32, intermediate_dim) orelse return resolution,
    };
    const activation_function = catalogActivationFunction(activation) orelse return resolution;
    const pair_signature = quant_kernel_op.SpecializationSignature{ .small_batch_matmul = .{
        .format = .q4_0,
        .row_bucket = .rows_1,
        .dispatch = .mmv,
        .epilogue = .pair_activation,
        .activation = .q8_1,
        .function = activation_function,
        .output = .q8_1,
    } };
    resolution.attempts += 1;
    const pair = (quant_kernel_catalog.resolve(pair_signature, runtime, compute_major, compute_minor) catch {
        resolution.misses += 1;
        return resolution;
    }) orelse {
        resolution.misses += 1;
        return resolution;
    };
    const down_signature = quant_kernel_op.SpecializationSignature{ .small_batch_matmul = .{
        .format = .q4_0,
        .row_bucket = .rows_1,
        .dispatch = .mmv,
        .epilogue = .gated_down,
        .activation = .q8_1,
    } };
    resolution.attempts += 1;
    const down = (quant_kernel_catalog.resolve(down_signature, .{
        .rows = runtime.rows,
        .input_dim = runtime.output_dim,
        .output_dim = runtime.input_dim,
    }, compute_major, compute_minor) catch {
        resolution.misses += 1;
        return resolution;
    }) orelse {
        resolution.misses += 1;
        return resolution;
    };
    if ((!pair.production_enabled or !down.production_enabled) and !candidate_gate_enabled) return resolution;
    resolution.route = .{ .pair_kernel_id = pair.kernel_id, .down_kernel_id = down.kernel_id };
    return resolution;
}

const GeneratedQ4_0CatalogFfnStage = enum { pair_q8, down_q8 };

fn noteGeneratedQ4_0CatalogFfnResolution(stats: *RuntimeStats, resolution: GeneratedQ4_0CatalogFfnResolution) void {
    stats.generated_kernel_catalog_resolve_attempts += resolution.attempts;
    stats.generated_kernel_catalog_resolve_misses += resolution.misses;
}

fn noteGeneratedQ4_0CatalogFfnResult(stats: *RuntimeStats, stage: GeneratedQ4_0CatalogFfnStage, hit: bool) void {
    if (hit) {
        stats.generated_kernel_catalog_hits += 1;
    } else {
        stats.generated_kernel_catalog_fallbacks += 1;
    }
    switch (stage) {
        .pair_q8 => if (hit) {
            stats.q4_0_generated_e2b_pair_q8_hits += 1;
        } else {
            stats.q4_0_generated_e2b_pair_q8_fallbacks += 1;
        },
        .down_q8 => if (hit) {
            stats.q4_0_generated_e2b_down_q8_hits += 1;
        } else {
            stats.q4_0_generated_e2b_down_q8_fallbacks += 1;
        },
    }
}

test "generated CUDA FFN catalog resolves by shape and target" {
    try std.testing.expect(!(GeneratedQ4_0Gates{}).catalog_ffn_candidates);
    try std.testing.expect(!(GeneratedQ4_0Gates{}).exact_ffn_candidates);
    const gated_candidate = generatedQ4_0CatalogFfnRoute(false, true, true, 1, 1536, 6144, .gelu_new, 8, 9);
    try std.testing.expect(gated_candidate.route == null);
    try std.testing.expectEqual(@as(usize, 2), gated_candidate.attempts);
    try std.testing.expectEqual(@as(usize, 0), gated_candidate.misses);
    try std.testing.expect(generatedQ4_0CatalogFfnRoute(true, true, true, 1, 1536, 6144, .gelu_new, 8, 9).route != null);
    try std.testing.expect(generatedQ4_0CatalogFfnRoute(true, true, true, 1, 1536, 12288, .gelu_new, 8, 9).route != null);
    const wrong_rows = generatedQ4_0CatalogFfnRoute(true, true, true, 2, 1536, 6144, .gelu_new, 8, 9);
    try std.testing.expect(wrong_rows.route == null);
    try std.testing.expectEqual(@as(usize, 1), wrong_rows.attempts);
    try std.testing.expectEqual(@as(usize, 1), wrong_rows.misses);
    try std.testing.expect(generatedQ4_0CatalogFfnRoute(true, true, true, 1, 1536, 6144, .gelu_new, 8, 0).route == null);
    try std.testing.expect(generatedQ4_0CatalogFfnRoute(false, true, true, 1, 2560, 10240, .gelu_new, 8, 9).route != null);
    const disabled_pair = generatedQ4_0CatalogFfnRoute(true, false, true, 1, 1536, 6144, .gelu_new, 8, 9);
    try std.testing.expect(disabled_pair.route == null);
    try std.testing.expectEqual(@as(usize, 0), disabled_pair.attempts);
    try std.testing.expectEqual(@as(usize, 0), disabled_pair.misses);
    try std.testing.expect(generatedQ4_0CatalogFfnRoute(true, true, false, 1, 1536, 6144, .gelu_new, 8, 9).route == null);
    const exact_gelu = generatedQ4_0CatalogFfnRoute(true, true, true, 1, 1536, 6144, .gelu_exact, 8, 9);
    try std.testing.expect(exact_gelu.route == null);
    try std.testing.expectEqual(@as(usize, 0), exact_gelu.attempts);
    if (@bitSizeOf(usize) > @bitSizeOf(u32)) {
        const oversized = generatedQ4_0CatalogFfnRoute(true, true, true, @as(usize, std.math.maxInt(u32)) + 1, 1536, 6144, .gelu_new, 8, 9);
        try std.testing.expect(oversized.route == null);
        try std.testing.expectEqual(@as(usize, 0), oversized.attempts);
    }

    var stats = RuntimeStats{};
    noteGeneratedQ4_0CatalogFfnResolution(&stats, .{ .attempts = 2 });
    noteGeneratedQ4_0CatalogFfnResolution(&stats, .{ .attempts = 1, .misses = 1 });
    noteGeneratedQ4_0CatalogFfnResult(&stats, .pair_q8, true);
    noteGeneratedQ4_0CatalogFfnResult(&stats, .pair_q8, false);
    noteGeneratedQ4_0CatalogFfnResult(&stats, .down_q8, true);
    noteGeneratedQ4_0CatalogFfnResult(&stats, .down_q8, false);
    try std.testing.expectEqual(@as(usize, 1), stats.q4_0_generated_e2b_pair_q8_hits);
    try std.testing.expectEqual(@as(usize, 1), stats.q4_0_generated_e2b_pair_q8_fallbacks);
    try std.testing.expectEqual(@as(usize, 1), stats.q4_0_generated_e2b_down_q8_hits);
    try std.testing.expectEqual(@as(usize, 1), stats.q4_0_generated_e2b_down_q8_fallbacks);
    try std.testing.expectEqual(@as(usize, 0), stats.q4_0_generated_pair_q8_hits);
    try std.testing.expectEqual(@as(usize, 0), stats.q4_0_generated_down_q8_hits);
    try std.testing.expectEqual(@as(usize, 3), stats.generated_kernel_catalog_resolve_attempts);
    try std.testing.expectEqual(@as(usize, 1), stats.generated_kernel_catalog_resolve_misses);
    try std.testing.expectEqual(@as(usize, 2), stats.generated_kernel_catalog_hits);
    try std.testing.expectEqual(@as(usize, 2), stats.generated_kernel_catalog_fallbacks);
}

test "generated CUDA exact F32 E2B FFN route is narrow and opt-in" {
    try std.testing.expect(!(GeneratedQ4_0Gates{}).exact_ffn_candidates);
    try std.testing.expect(kernels_mod.generatedQ4_0ExactFfnPairE2BShapeEligible(1, 1536, 6144));
    try std.testing.expect(kernels_mod.generatedQ4_0ExactFfnPairE2BShapeEligible(1, 1536, 12288));
    try std.testing.expect(kernels_mod.generatedQ4_0ExactFfnDownE2BShapeEligible(1, 6144, 1536));
    try std.testing.expect(kernels_mod.generatedQ4_0ExactFfnDownE2BShapeEligible(1, 12288, 1536));
    try std.testing.expect(!kernels_mod.generatedQ4_0ExactFfnPairE2BShapeEligible(2, 1536, 6144));
    try std.testing.expect(!kernels_mod.generatedQ4_0ExactFfnDownE2BShapeEligible(1, 6144, 2560));
}

fn launchLinearQ4_0Tile4ThenBaseF32(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !CudaQ4KernelLaunch {
    self.kernels.launchLinearQ4_0Tile4F32(&self.ctx, dst, input, weight, rows, in_dim, out_dim) catch |tile4_err| switch (tile4_err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            try self.kernels.launchLinearQ4_0F32(&self.ctx, dst, input, weight, rows, in_dim, out_dim);
            return .{
                .provider = .fallback,
                .kernel_id = "termite_linear_q4_0_f32",
                .grid = .{ (rows * out_dim + 255) / 256, 1, 1 },
                .block = .{ 256, 1, 1 },
            };
        },
        else => return tile4_err,
    };
    return .{
        .kernel_id = "termite_linear_q4_0_f32_tile4",
        .grid = .{ (out_dim + 3) / 4, rows, 1 },
        .block = .{ 256, 1, 1 },
    };
}

fn cudaQ4_0Q8_1PrefillRowsEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.q8_1_prefill_rows;
}

fn cudaQ4_0Q8_1RowsEligible(self: *const CudaCompute, rows: usize) bool {
    return rows == 1 or (rows > 1 and cudaQ4_0Q8_1PrefillRowsEnabled(self));
}

fn noteQ4_0Q8_1PrefillLinear(stats: *RuntimeStats, variant: kernels_mod.Q4_0Q8_1PrefillRowsVariant) void {
    switch (variant) {
        .none, .single => {},
        .rows2 => {
            stats.q4_0_q8_1_prefill_linear_hits += 1;
            stats.q4_0_q8_1_prefill_linear_rows2_hits += 1;
        },
        .rows4, .rows8_c2, .rows16_c1 => {
            stats.q4_0_q8_1_prefill_linear_hits += 1;
            stats.q4_0_q8_1_prefill_linear_rows4_hits += 1;
        },
        .rows8_c4 => {
            stats.q4_0_q8_1_prefill_linear_hits += 1;
            stats.q4_0_q8_1_prefill_linear_rows8_c4_hits += 1;
        },
        .e4b_down_rows => {
            stats.q4_0_q8_1_prefill_linear_hits += 1;
            stats.q4_0_q8_1_prefill_linear_e4b_down_rows_hits += 1;
        },
        .generic_rows => {
            stats.q4_0_q8_1_prefill_linear_hits += 1;
            stats.q4_0_q8_1_prefill_linear_generic_rows_hits += 1;
        },
        .tile8_rows, .tile8_w8_rows => {
            stats.q4_0_q8_1_prefill_linear_hits += 1;
            stats.q4_0_q8_1_prefill_linear_tile8_rows_hits += 1;
        },
    }
}

fn noteQ4_0Q8_1PrefillQkv(stats: *RuntimeStats, variant: kernels_mod.Q4_0Q8_1PrefillRowsVariant) void {
    switch (variant) {
        .none, .single => {},
        .rows4, .rows8_c2, .rows8_c4, .rows16_c1 => {
            stats.q4_0_q8_1_prefill_qkv_hits += 1;
            stats.q4_0_q8_1_prefill_qkv_rows4_hits += 1;
        },
        .tile8_rows, .generic_rows, .rows2, .e4b_down_rows => {
            stats.q4_0_q8_1_prefill_qkv_hits += 1;
            stats.q4_0_q8_1_prefill_qkv_tile8_rows_hits += 1;
        },
        .tile8_w8_rows => {
            stats.q4_0_q8_1_prefill_qkv_hits += 1;
            stats.q4_0_q8_1_prefill_qkv_tile8_w8_rows_hits += 1;
        },
    }
}

fn noteQ4_0Q8_1PrefillPair(stats: *RuntimeStats, variant: kernels_mod.Q4_0Q8_1PrefillRowsVariant) void {
    switch (variant) {
        .none, .single => {},
        .rows2 => {
            stats.q4_0_q8_1_prefill_pair_hits += 1;
            stats.q4_0_q8_1_prefill_pair_rows2_hits += 1;
        },
        .rows4, .rows8_c4 => {
            stats.q4_0_q8_1_prefill_pair_hits += 1;
            stats.q4_0_q8_1_prefill_pair_rows4_hits += 1;
        },
        .rows8_c2 => {
            stats.q4_0_q8_1_prefill_pair_hits += 1;
            stats.q4_0_q8_1_prefill_pair_rows8_c2_hits += 1;
        },
        .rows16_c1 => {
            stats.q4_0_q8_1_prefill_pair_hits += 1;
            stats.q4_0_q8_1_prefill_pair_rows16_c1_hits += 1;
        },
        .generic_rows, .e4b_down_rows => {
            stats.q4_0_q8_1_prefill_pair_hits += 1;
            stats.q4_0_q8_1_prefill_pair_generic_rows_hits += 1;
        },
        .tile8_rows, .tile8_w8_rows => {
            stats.q4_0_q8_1_prefill_pair_hits += 1;
            stats.q4_0_q8_1_prefill_pair_tile8_rows_hits += 1;
        },
    }
}

fn noteQ4_0Q8_1PrefillGatedDown(stats: *RuntimeStats, variant: kernels_mod.Q4_0Q8_1PrefillRowsVariant) void {
    switch (variant) {
        .none, .single => {},
        .rows2 => {
            stats.q4_0_q8_1_prefill_gated_down_hits += 1;
            stats.q4_0_q8_1_prefill_gated_down_rows2_hits += 1;
        },
        .rows4, .rows8_c2, .rows16_c1 => {
            stats.q4_0_q8_1_prefill_gated_down_hits += 1;
            stats.q4_0_q8_1_prefill_gated_down_rows4_hits += 1;
        },
        .rows8_c4 => {
            stats.q4_0_q8_1_prefill_gated_down_hits += 1;
            stats.q4_0_q8_1_prefill_gated_down_rows8_c4_hits += 1;
        },
        .e4b_down_rows => {
            stats.q4_0_q8_1_prefill_gated_down_hits += 1;
            stats.q4_0_q8_1_prefill_gated_down_e4b_down_rows_hits += 1;
        },
        .generic_rows => {
            stats.q4_0_q8_1_prefill_gated_down_hits += 1;
            stats.q4_0_q8_1_prefill_gated_down_generic_rows_hits += 1;
        },
        .tile8_rows, .tile8_w8_rows => {
            stats.q4_0_q8_1_prefill_gated_down_hits += 1;
            stats.q4_0_q8_1_prefill_gated_down_tile8_rows_hits += 1;
        },
    }
}

fn cudaQ4_0LinearQ8_1Tile8Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE8", false);
}

fn cudaQ4_0LinearQ8_1Tile4W8Enabled(self: *const CudaCompute, in_dim: usize) bool {
    return in_dim >= self.tuned_route_gates.linear_q8_1_tile4_w8_min_in_dim and
        self.tuned_route_gates.linear_q8_1_tile4_w8;
}

fn cudaQ6KLmHeadQ8_1Enabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.q6_k_lm_head_q8_1;
}

fn cudaGeneratedQ6KQ8_1LmHeadArgmaxEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX", false);
}

fn generatedQ6KQ8_1LmHeadArgmaxEligible(rows: usize, in_dim: usize, out_dim: usize, suppress_count: usize) bool {
    return suppress_count == 0 and kernels_mod.linearQ6KQ8_1GeneratedArgmaxShapeEligible(rows, in_dim, out_dim);
}

fn cudaQ6KLmHeadTile16Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_TILE16", true);
}

fn cudaQ4_0QkvTile8Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_QKV_TILE8", cudaQ4_0DecodeTile8GlobalEnabled());
}

fn cudaQ4_0QkvTile4W4Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_QKV_TILE4_W4", false);
}

fn cudaQ4_0QkvQ8_1Dp4aEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.qkv_q8_1_dp4a;
}

fn cudaQ4_0QkvQ8_1Tile8Enabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.qkv_q8_1_tile8;
}

fn cudaQ4_0QkvQ8_1Tile8W8Enabled(in_dim: usize) bool {
    return in_dim >= 2048 and platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_TILE8_W8", false);
}

fn cudaQ4_0PairTile8Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_TILE8", cudaQ4_0DecodeTile8GlobalEnabled());
}

fn cudaQ4_0PairTile4W4Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_TILE4_W4", true);
}

fn cudaQ4_0PairQ8_1Dp4aEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.pair_q8_1_dp4a;
}

fn cudaQ4_0PairQ8_1Tile8Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_TILE8", false);
}

fn cudaQ4_0PairQ8_1Tile4W8Enabled(self: *const CudaCompute, in_dim: usize) bool {
    return in_dim >= 2048 and self.tuned_route_gates.pair_q8_1_tile4_w8;
}

fn cudaQ4_0PairActivationQ8_1Dp4aEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.pair_activation_q8_1_dp4a;
}

fn cudaQ4_0PairActivationQ8_1Tile8Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_TILE8", false);
}

fn cudaQ4_0PairActivationQ8_1Tile4W8Enabled(in_dim: usize) bool {
    return in_dim >= 2048 and platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_TILE4_W8", true);
}

fn cudaQ4_0ActivationSliceQ8_1Dp4aEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.activation_slice_q8_1_dp4a;
}

fn cudaQ4_0GatedDownTile8Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_TILE8", cudaQ4_0DecodeTile8GlobalEnabled());
}

fn cudaQ4_0GatedDownTile4W4Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_TILE4_W4", false);
}

fn cudaQ4_0GatedDownTile16Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_TILE16", false);
}

fn cudaQ4_0GatedDownQ8_1Dp4aEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.gated_down_q8_1_dp4a;
}

fn cudaQ4_0GatedDownPrecomputeEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_PRECOMPUTE", true);
}

fn cudaQ4_0GateUpActivationPrecomputeEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.gate_up_activation_precompute;
}

fn cudaQ4_0GateUpActivationQ8_1PrecomputeDisabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DISABLE_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE", false);
}

fn cudaQ4_0GateUpActivationQ8_1PrecomputeEnabled() bool {
    if (cudaQ4_0GateUpActivationQ8_1PrecomputeDisabled()) return false;
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE", false);
}

fn cudaQ4_0PleGateFusionEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PLE_GATE_FUSION", true);
}

fn cudaF32LinearTiledEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_F32_LINEAR_TILED", true);
}

fn cudaPleRmsEmbeddingFusionEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PLE_RMS_EMBED_FUSION", false);
}

fn cudaQ4_0LmHeadArgmaxEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_ARGMAX", false);
}

fn cudaQ4_0LmHeadQ8_1ArgmaxEnabled(self: *const CudaCompute) bool {
    return self.tuned_route_gates.lm_head_q8_1_argmax;
}

fn cudaQ4_0LmHeadTile16Enabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_TILE16", false);
}

fn pinnedScalarUploadEligible(self: *CudaCompute, bytes: []const u8) bool {
    return bytes.len != 0 and
        bytes.len <= pinned_scalar_upload_slot_size and
        !cudaPinnedScalarUploadsDisabled() and
        !self.debug_cuda_graph_capture_active;
}

fn pinnedScalarDownloadEligible(self: *CudaCompute, bytes: []const u8) bool {
    return bytes.len != 0 and
        bytes.len <= pinned_scalar_upload_slot_size and
        !cudaPinnedScalarDownloadsDisabled() and
        !self.debug_cuda_graph_capture_active;
}

fn copyFromPinnedScalarUploadRing(self: *CudaCompute, device: buffer_mod.DeviceBuffer, bytes: []const u8) !bool {
    if (bytes.len == 0) return true;
    if (!pinnedScalarUploadEligible(self, bytes)) return false;

    if (self.pinned_scalar_upload_ring.host.ptr == null) {
        self.pinned_scalar_upload_ring.host = buffer_mod.HostBuffer.alloc(
            &self.ctx,
            pinned_scalar_upload_slot_count * pinned_scalar_upload_slot_size,
        ) catch {
            return false;
        };
        self.pinned_scalar_upload_ring.next_slot = 0;
    }

    if (self.pinned_scalar_upload_ring.next_slot >= pinned_scalar_upload_slot_count) {
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.upload_syncs += 1;
        self.stats.pinned_scalar_upload_wrap_syncs += 1;
        self.pinned_scalar_upload_ring.next_slot = 0;
    }

    const slot_index = self.pinned_scalar_upload_ring.next_slot;
    self.pinned_scalar_upload_ring.next_slot += 1;
    const host_bytes = self.pinned_scalar_upload_ring.host.bytes();
    const start = slot_index * pinned_scalar_upload_slot_size;
    const slot = host_bytes[start..][0..bytes.len];
    @memcpy(slot, bytes);
    try device.copyFromHost(&self.ctx, slot);
    self.stats.h2d_bytes += bytes.len;
    self.stats.pinned_scalar_uploads += 1;
    self.stats.pinned_scalar_upload_bytes += bytes.len;
    return true;
}

fn copyFromPinnedUploadArena(self: *CudaCompute, device: buffer_mod.DeviceBuffer, bytes: []const u8) !bool {
    if (bytes.len <= pinned_scalar_upload_slot_size or
        bytes.len > pinned_upload_arena_capacity or
        cudaPinnedUploadArenaDisabled() or
        self.debug_cuda_graph_capture_active)
    {
        return false;
    }

    if (self.pinned_upload_arena.host.ptr == null) {
        self.pinned_upload_arena.host = buffer_mod.HostBuffer.alloc(
            &self.ctx,
            pinned_upload_arena_capacity,
        ) catch {
            return false;
        };
        self.pinned_upload_arena.next_offset = 0;
    }

    var start = std.mem.alignForward(
        usize,
        self.pinned_upload_arena.next_offset,
        pinned_upload_arena_alignment,
    );
    if (start > pinned_upload_arena_capacity - bytes.len) {
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.upload_syncs += 1;
        start = 0;
    }

    const slot = self.pinned_upload_arena.host.bytes()[start..][0..bytes.len];
    @memcpy(slot, bytes);
    try device.copyFromHost(&self.ctx, slot);
    self.stats.h2d_bytes += bytes.len;
    self.pinned_upload_arena.next_offset = start + bytes.len;
    return true;
}

fn uploadDecodeScalars(self: *CudaCompute, scalars: [5]u32) !void {
    if (self.debug_cuda_decode_scalars.ptr == 0) return error.InvalidCudaState;
    const scalar_bytes = std.mem.sliceAsBytes(&scalars);
    const tried_pinned_scalar_upload = pinnedScalarUploadEligible(self, scalar_bytes);
    if (!(try copyFromPinnedScalarUploadRing(self, self.debug_cuda_decode_scalars, scalar_bytes))) {
        if (tried_pinned_scalar_upload) self.stats.pinned_scalar_upload_fallbacks += 1;
        try copyFromHostTracked(self, self.debug_cuda_decode_scalars, scalar_bytes);
    }
    self.stats.cuda_graph_capture_scalar_updates += 1;
    self.debug_cuda_decode_scalars_device = scalars;
    self.debug_cuda_decode_scalars_device_valid = true;
    self.debug_cuda_decode_scalars_upload_deferred = false;
}

fn uploadCachedDecodeScalars(self: *CudaCompute) !void {
    if (!self.debug_cuda_decode_scalars_host_valid) return error.InvalidCudaState;
    try uploadDecodeScalars(self, self.debug_cuda_decode_scalars_host);
    self.debug_cuda_decode_scalars_upload_deferred = false;
}

fn decodeScalarsAutoAdvanceReplaySlot(self: *const CudaCompute) ?usize {
    if (!cudaDebugGraphAutoAdvanceDecodeScalarsEnabled()) return null;
    if (!cudaDebugGraphPersistentReplayEnabled(self)) return null;
    if (self.debug_cuda_decode_scalars.ptr == 0) return null;
    const slot_idx = self.debug_cuda_graph_prepared_slot orelse return null;
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    if (slot.exec == null or !slot.valid or !slot.decode_scalars_auto_advance) return null;
    return slot_idx;
}

fn decodeScalarsAutoAdvanceExpected(self: *const CudaCompute, slot_idx: usize) ?[5]u32 {
    if (!self.debug_cuda_decode_scalars_device_valid) return null;
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    var expected = self.debug_cuda_decode_scalars_device;
    inline for (0..5) |idx| {
        expected[idx] = std.math.add(u32, expected[idx], slot.decode_scalars_auto_advance_delta[idx]) catch return null;
    }
    return expected;
}

fn decodeScalarsEqual(a: [5]u32, b: [5]u32) bool {
    inline for (0..5) |idx| {
        if (a[idx] != b[idx]) return false;
    }
    return true;
}

fn decodeScalarsAutoAdvanceDeltaForScalars(scalars: [5]u32) [5]u32 {
    _ = scalars;
    return .{ 1, 1, 1, 1, 0 };
}

fn decodeScalarsAutoAdvanceDeltaSupported(delta: [5]u32) bool {
    return decodeScalarsEqual(delta, .{ 1, 1, 1, 1, 0 });
}

fn decodeScalarsAutoAdvanceDeltaBetween(previous: [5]u32, next: [5]u32) ?[5]u32 {
    var delta: [5]u32 = .{ 0, 0, 0, 0, 0 };
    inline for (0..5) |idx| {
        if (next[idx] < previous[idx]) return null;
        delta[idx] = next[idx] - previous[idx];
    }
    return delta;
}

fn decodeScalarsCanPreAdvance(scalars: [5]u32, delta: [5]u32) bool {
    inline for (0..5) |idx| {
        if (scalars[idx] < delta[idx]) return false;
    }
    return true;
}

fn markDecodeScalarsDeviceMatchesHost(self: *CudaCompute) void {
    if (!self.debug_cuda_decode_scalars_host_valid) return;
    self.debug_cuda_decode_scalars_device = self.debug_cuda_decode_scalars_host;
    self.debug_cuda_decode_scalars_device_valid = true;
    self.debug_cuda_decode_scalars_upload_deferred = false;
}

fn flushDeferredDecodeScalarUpload(self: *CudaCompute) !void {
    if (!self.debug_cuda_decode_scalars_upload_deferred) return;
    try uploadCachedDecodeScalars(self);
}

fn copyToHostTracked(self: *CudaCompute, device: buffer_mod.DeviceBuffer, bytes: []u8) !void {
    if (self.debug_cuda_graph_capture_active) {
        self.debug_cuda_graph_capture_disabled = true;
        std.log.warn("cuda_graph_capture_probe: unsafe_d2h_copy bytes={d} disabled=1", .{bytes.len});
        return error.CudaGraphCaptureUnsafeHostCopy;
    }
    try device.copyToHost(&self.ctx, bytes);
    self.stats.d2h_bytes += bytes.len;
}

fn copyToPinnedScalarDownloadAndSync(self: *CudaCompute, device: buffer_mod.DeviceBuffer, bytes: []u8) !bool {
    if (bytes.len == 0) return true;
    if (!pinnedScalarDownloadEligible(self, bytes)) return false;

    if (self.pinned_scalar_download_buffer.host.ptr == null) {
        self.pinned_scalar_download_buffer.host = buffer_mod.HostBuffer.alloc(
            &self.ctx,
            pinned_scalar_upload_slot_size,
        ) catch {
            return false;
        };
    }

    const host_bytes = self.pinned_scalar_download_buffer.host.bytes();
    const slot = host_bytes[0..bytes.len];
    try device.copyToHost(&self.ctx, slot);
    self.stats.d2h_bytes += bytes.len;
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.download_syncs += 1;
    @memcpy(bytes, slot);
    self.stats.pinned_scalar_downloads += 1;
    self.stats.pinned_scalar_download_bytes += bytes.len;
    return true;
}

fn copyToHostTrackedAndSync(self: *CudaCompute, device: buffer_mod.DeviceBuffer, bytes: []u8) !void {
    const tried_pinned_scalar_download = pinnedScalarDownloadEligible(self, bytes);
    if (try copyToPinnedScalarDownloadAndSync(self, device, bytes)) return;
    if (tried_pinned_scalar_download) self.stats.pinned_scalar_download_fallbacks += 1;
    if (try copyToPinnedBulkDownloadAndSync(self, device, bytes)) return;
    try copyToHostTracked(self, device, bytes);
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.download_syncs += 1;
}

// Large synchronous downloads into pageable memory run at a fraction of PCIe
// bandwidth (~180MB/s observed for the per-token 1MB logits row). Stage
// through a reusable pinned buffer instead: pinned DMA + host memcpy is
// ~10-20x faster for the sampled-decode logits path.
const pinned_bulk_download_min_bytes: usize = 64 * 1024;
const pinned_bulk_download_max_bytes: usize = 4 * 1024 * 1024;

fn copyToPinnedBulkDownloadAndSync(self: *CudaCompute, device: buffer_mod.DeviceBuffer, bytes: []u8) !bool {
    if (bytes.len < pinned_bulk_download_min_bytes or bytes.len > pinned_bulk_download_max_bytes) return false;
    if (self.debug_cuda_graph_capture_active) return false;
    if (self.pinned_bulk_download_buffer.ptr == null) {
        self.pinned_bulk_download_buffer = buffer_mod.HostBuffer.alloc(&self.ctx, pinned_bulk_download_max_bytes) catch return false;
    }
    const staging = self.pinned_bulk_download_buffer.bytes()[0..bytes.len];
    const logical = buffer_mod.DeviceBuffer{ .ptr = device.ptr, .len = bytes.len };
    try logical.copyToHost(&self.ctx, staging);
    try synchronizeAndDrainDeferredDeviceFrees(self);
    @memcpy(bytes, staging);
    self.stats.d2h_bytes += bytes.len;
    self.stats.download_syncs += 1;
    self.stats.pinned_bulk_downloads += 1;
    return true;
}

fn beginI32ScalarDownloadOp(ctx: *anyopaque, tensor: CT) anyerror!?ops.PendingI32ScalarDownload {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    if (cuda_tensor.quant_type != null) return null;
    if (cuda_tensor.dtype != .i32 or cuda_tensor.elem_count != 1) return null;

    const byte_len = @sizeOf(i32);
    var eligibility_probe: [byte_len]u8 = undefined;
    if (!pinnedScalarDownloadEligible(self, eligibility_probe[0..])) return null;

    if (self.async_i32_scalar_download.in_use) {
        self.stats.async_i32_scalar_download_busy_fallbacks += 1;
        return null;
    }
    if (byte_len > cuda_tensor.buffer.len) return error.InvalidTensorShape;

    if (self.async_i32_scalar_download.host.ptr == null) {
        self.async_i32_scalar_download.host = buffer_mod.HostBuffer.alloc(&self.ctx, pinned_scalar_upload_slot_size) catch {
            return null;
        };
    }
    if (self.async_i32_scalar_download.event == null) {
        self.async_i32_scalar_download.event = self.ctx.createStreamEvent() catch {
            return null;
        };
    }

    const host_bytes = self.async_i32_scalar_download.host.bytes();
    const slot = host_bytes[0..byte_len];
    const logical_buffer = buffer_mod.DeviceBuffer{
        .ptr = cuda_tensor.buffer.ptr,
        .len = byte_len,
    };
    var staged_download_started = false;
    if (cudaAsyncI32ScalarDownloadStagingEnabled(self)) staged: {
        if (self.ctx.debug_graph_capture_active) break :staged;
        if (self.async_i32_scalar_download.staging.ptr == 0) {
            self.async_i32_scalar_download.staging = buffer_mod.DeviceBuffer.alloc(&self.ctx, byte_len) catch {
                break :staged;
            };
            self.noteDeviceBytes(byte_len);
            self.stats.device_alloc_calls += 1;
        } else if (self.async_i32_scalar_download.staging.len < byte_len) {
            break :staged;
        }
        if (self.async_i32_scalar_download.copy_stream == null) {
            try self.ctx.makeCurrent();
            var copy_stream: driver_mod.CUstream = null;
            self.ctx.driver.check(self.ctx.driver.fns.cuStreamCreate(&copy_stream, 0)) catch {
                break :staged;
            };
            self.async_i32_scalar_download.copy_stream = copy_stream;
        }
        if (self.async_i32_scalar_download.ready_event == null) {
            self.async_i32_scalar_download.ready_event = self.ctx.createStreamEvent() catch {
                break :staged;
            };
        }

        try self.async_i32_scalar_download.staging.copyFromDevice(&self.ctx, logical_buffer, byte_len);
        self.stats.d2d_bytes += byte_len;
        try self.ctx.recordEvent(self.async_i32_scalar_download.ready_event);
        try self.ctx.driver.check(self.ctx.driver.fns.cuStreamWaitEvent(self.async_i32_scalar_download.copy_stream, self.async_i32_scalar_download.ready_event, 0));
        try self.ctx.driver.check(self.ctx.driver.fns.cuMemcpyDtoHAsync(slot.ptr, self.async_i32_scalar_download.staging.ptr, byte_len, self.async_i32_scalar_download.copy_stream));
        try self.ctx.driver.check(self.ctx.driver.fns.cuEventRecord(self.async_i32_scalar_download.event, self.async_i32_scalar_download.copy_stream));
        self.stats.d2h_bytes += byte_len;
        staged_download_started = true;
        break :staged;
    }
    if (!staged_download_started) {
        try logical_buffer.copyToHost(&self.ctx, slot);
        try self.ctx.recordEvent(self.async_i32_scalar_download.event);
        self.stats.d2h_bytes += byte_len;
    }
    self.async_i32_scalar_download.in_use = true;
    self.async_i32_scalar_download.handle +%= 1;
    if (self.async_i32_scalar_download.handle == 0) self.async_i32_scalar_download.handle = 1;
    self.stats.pinned_scalar_downloads += 1;
    self.stats.pinned_scalar_download_bytes += byte_len;
    self.stats.async_i32_scalar_downloads += 1;
    self.stats.async_i32_scalar_download_bytes += byte_len;
    return .{ .handle = self.async_i32_scalar_download.handle };
}

fn finishI32ScalarDownloadOp(ctx: *anyopaque, pending: ops.PendingI32ScalarDownload) anyerror!i32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!self.async_i32_scalar_download.in_use or
        self.async_i32_scalar_download.handle != pending.handle or
        self.async_i32_scalar_download.event == null)
    {
        return error.InvalidCudaState;
    }
    try self.ctx.driver.check(self.ctx.driver.fns.cuEventSynchronize(self.async_i32_scalar_download.event));
    self.async_i32_scalar_download.in_use = false;
    self.stats.async_i32_scalar_download_finishes += 1;
    const bytes = self.async_i32_scalar_download.host.bytes()[0..@sizeOf(i32)];
    return std.mem.readInt(i32, bytes, .little);
}

fn cancelI32ScalarDownloadOp(ctx: *anyopaque, pending: ops.PendingI32ScalarDownload) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!self.async_i32_scalar_download.in_use or
        self.async_i32_scalar_download.handle != pending.handle or
        self.async_i32_scalar_download.event == null)
    {
        return;
    }
    self.ctx.driver.check(self.ctx.driver.fns.cuEventSynchronize(self.async_i32_scalar_download.event)) catch return;
    self.async_i32_scalar_download.in_use = false;
    self.stats.async_i32_scalar_download_cancels += 1;
}

fn copyFromDeviceTracked(self: *CudaCompute, device: buffer_mod.DeviceBuffer, src: buffer_mod.DeviceBuffer, len: usize) !void {
    if (self.debug_cuda_graph_capture_active) {
        try self.kernels.launchCopyBytes(&self.ctx, device, src, len);
        self.stats.d2d_bytes += len;
        return;
    }
    try device.copyFromDevice(&self.ctx, src, len);
    self.stats.d2d_bytes += len;
}

fn stageBf16ActivationForCublasLt(
    self: *CudaCompute,
    input: *const CudaTensor,
    rows: usize,
    in_dim: usize,
) !?buffer_mod.DeviceBuffer {
    if (!cudaCublasLtEnabled()) return null;
    if (self.ctx.info.compute_major < 8) return null;
    if (self.cublaslt == null) return null;
    const count = try checkedMul(rows, in_dim);
    const bytes = try checkedMul(count, @sizeOf(u16));
    if (input.dtype == .f32 and input.elem_count == count and input.bf16_mirror.ptr != 0 and input.bf16_mirror.len >= bytes) {
        self.stats.bf16_cublaslt_activation_mirror_hits += 1;
        return .{ .ptr = input.bf16_mirror.ptr, .len = bytes };
    }
    const scratch = self.bf16_activation_scratch.acquire(&self.ctx, bytes) catch return null;
    var staging_profile_scope = beginPrefillProfile(self, .staging, rows);
    defer if (staging_profile_scope) |*scope| scope.end();
    self.kernels.launchF32ToBf16(&self.ctx, scratch, input.buffer, count) catch return null;
    self.stats.bf16_cublaslt_activation_staging_calls += 1;
    return scratch;
}

fn stageF16ActivationForCublasLt(
    self: *CudaCompute,
    input: *const CudaTensor,
    rows: usize,
    in_dim: usize,
) !?buffer_mod.DeviceBuffer {
    if (!canUseF16TensorCoreWeights(self)) return null;
    if (input.dtype != .f32) return null;
    const count = try checkedMul(rows, in_dim);
    if (input.elem_count != count) return null;
    const bytes = try checkedMul(count, @sizeOf(u16));
    const scratch = self.f16_activation_scratch.acquire(&self.ctx, bytes) catch return null;
    var staging_profile_scope = beginPrefillProfile(self, .staging, rows);
    defer if (staging_profile_scope) |*scope| scope.end();
    self.kernels.launchF32ToF16(&self.ctx, scratch, input.buffer, count) catch return null;
    self.stats.f16_cublaslt_activation_staging_calls += 1;
    return scratch;
}

// DeBERTa's attention consumes five tensors at once (Q/K/V and two relative
// projections), so it cannot reuse the one-buffer GEMM staging scratch. Keep
// a small set of model-session-owned buffers and convert each F32 graph tensor
// once per layer. The streaming attention kernel accumulates into F32 and
// writes F32 output, preserving the residual/norm contract around it.
fn stageDebertaAttentionF16(
    self: *CudaCompute,
    scratch: *scratch_mod.DeviceScratch,
    input: *const CudaTensor,
    count: usize,
) !?buffer_mod.DeviceBuffer {
    if (input.dtype != .f32 or input.quant_type != null or input.elem_count != count) return null;
    if (self.ctx.info.compute_major < 8 or self.kernels.deberta_attention_stream_f16 == null) return null;
    const bytes = try checkedMul(count, @sizeOf(u16));
    const staged = scratch.acquire(&self.ctx, bytes) catch return null;
    self.kernels.launchF32ToF16(&self.ctx, staged, input.buffer, count) catch return null;
    self.stats.deberta_stream_f16_staging_calls += 1;
    return staged;
}

const deberta_materialized_workspace_alignment: usize = 256;
const default_deberta_materialized_workspace_limit_bytes: usize = 512 * 1024 * 1024;

const DebertaMaterializedWorkspaceLayout = struct {
    q_f16: usize,
    k_f16: usize,
    v_f16: usize,
    q_r_f16: usize,
    k_r_f16: usize,
    content_scores: usize,
    c2p_scores: usize,
    p2c_scores: usize,
    probabilities_f16: usize,
    output_packed: usize,
    total_bytes: usize,
};

fn alignDebertaWorkspaceOffset(value: usize) !usize {
    const padded = try checkedAdd(value, deberta_materialized_workspace_alignment - 1);
    return padded & ~(deberta_materialized_workspace_alignment - 1);
}

fn appendDebertaWorkspaceRegion(cursor: *usize, bytes: usize) !usize {
    const offset = try alignDebertaWorkspaceOffset(cursor.*);
    cursor.* = try checkedAdd(offset, bytes);
    return offset;
}

fn debertaMaterializedWorkspaceLayout(
    f16_count_bytes: usize,
    f16_rel_bytes: usize,
    score_bytes: usize,
    rel_score_bytes: usize,
    probability_bytes: usize,
    output_bytes: usize,
) !DebertaMaterializedWorkspaceLayout {
    var cursor: usize = 0;
    const q_f16 = try appendDebertaWorkspaceRegion(&cursor, f16_count_bytes);
    const k_f16 = try appendDebertaWorkspaceRegion(&cursor, f16_count_bytes);
    const v_f16 = try appendDebertaWorkspaceRegion(&cursor, f16_count_bytes);
    const q_r_f16 = try appendDebertaWorkspaceRegion(&cursor, f16_rel_bytes);
    const k_r_f16 = try appendDebertaWorkspaceRegion(&cursor, f16_rel_bytes);
    const content_scores = try appendDebertaWorkspaceRegion(&cursor, score_bytes);
    const c2p_scores = try appendDebertaWorkspaceRegion(&cursor, rel_score_bytes);
    const p2c_scores = try appendDebertaWorkspaceRegion(&cursor, rel_score_bytes);
    const probabilities_f16 = try appendDebertaWorkspaceRegion(&cursor, probability_bytes);
    const output_packed = try appendDebertaWorkspaceRegion(&cursor, output_bytes);
    return .{
        .q_f16 = q_f16,
        .k_f16 = k_f16,
        .v_f16 = v_f16,
        .q_r_f16 = q_r_f16,
        .k_r_f16 = k_r_f16,
        .content_scores = content_scores,
        .c2p_scores = c2p_scores,
        .p2c_scores = p2c_scores,
        .probabilities_f16 = probabilities_f16,
        .output_packed = output_packed,
        .total_bytes = try alignDebertaWorkspaceOffset(cursor),
    };
}

fn debertaMaterializedWorkspaceLayoutForShape(batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) !DebertaMaterializedWorkspaceLayout {
    if (batch == 0 or seq_len == 0 or num_heads == 0 or head_dim == 0) return error.InvalidShape;
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    const rel_len = try checkedSub(try checkedMul(2, seq_len), 1);
    const packed_rel_count = try checkedMul(batch, try checkedMul(rel_len, hidden));
    const score_count = try checkedMul(try checkedMul(batch, num_heads), try checkedMul(seq_len, seq_len));
    const rel_score_count = try checkedMul(try checkedMul(batch, num_heads), try checkedMul(seq_len, rel_len));
    return debertaMaterializedWorkspaceLayout(
        try checkedMul(count, @sizeOf(u16)),
        try checkedMul(packed_rel_count, @sizeOf(u16)),
        try checkedMul(score_count, @sizeOf(f32)),
        try checkedMul(rel_score_count, @sizeOf(f32)),
        try checkedMul(score_count, @sizeOf(u16)),
        try checkedMul(count, @sizeOf(f32)),
    );
}

fn debertaWorkspaceRegion(arena: buffer_mod.DeviceBuffer, offset: usize, bytes: usize) buffer_mod.DeviceBuffer {
    std.debug.assert(offset <= arena.len and bytes <= arena.len - offset);
    return .{ .ptr = arena.ptr + @as(u64, @intCast(offset)), .len = bytes };
}

fn cudaDebertaMaterializedWorkspaceLimitBytes(self: *const CudaCompute) usize {
    const configured = if (platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_DEBERTA_MATERIALIZED_WORKSPACE_MB")) |mb|
        std.math.mul(usize, mb, 1024 * 1024) catch 0
    else
        default_deberta_materialized_workspace_limit_bytes;
    const budget = self.run_budget orelse return configured;
    const scratch_limit = budget.limits.scratch_limit_bytes;
    if (scratch_limit == 0) return configured;
    const remaining = scratch_limit -| budget.scratchTotalBytes();
    return @min(configured, remaining);
}

fn debertaMaterializedAutoTarget(compute_major: i32, compute_minor: i32) bool {
    // Current production evidence is exact to NVIDIA L4 / SM89. Keep other
    // architectures available through explicit diagnostic mode only.
    return cudaQualifiedPerfTarget(compute_major, compute_minor);
}

/// Materialized DeBERTa attention follows the same schedule as the fast CPU
/// encoder path: QK^T, QKr^T, and KQr^T are tensor-core GEMMs; a generated
/// kernel gathers relative-position diagonals and applies softmax; P*V is the
/// fourth GEMM. The auto dispatcher promotes it only for its qualified B4+
/// S128..256 envelope; explicit mode remains available for diagnostics.
fn tryDebertaMaterializedAttentionF16(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    q: *const CudaTensor,
    k: *const CudaTensor,
    v: *const CudaTensor,
    q_r: *const CudaTensor,
    k_r: *const CudaTensor,
    mask: buffer_mod.DeviceBuffer,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) !bool {
    if (self.ctx.info.compute_major < 8 or self.cublaslt == null) return false;
    if (seq_len == 0 or seq_len > 256 or head_dim != 64 or q.dtype != .f32 or k.dtype != .f32 or v.dtype != .f32 or q_r.dtype != .f32 or k_r.dtype != .f32) return false;
    if (self.kernels.deberta_pack_heads_f16 == null or self.kernels.deberta_scores_softmax_f32 == null or self.kernels.deberta_unpack_heads_f32 == null) return false;
    const matrices = checkedMul(batch, num_heads) catch return false;
    const hidden = checkedMul(num_heads, head_dim) catch return false;
    const count = checkedMul(checkedMul(batch, seq_len) catch return false, hidden) catch return false;
    const rel_len = checkedSub(checkedMul(2, seq_len) catch return false, 1) catch return false;
    const rel_count = checkedMul(rel_len, hidden) catch return false;
    const packed_rel_count = checkedMul(batch, rel_count) catch return false;
    const score_count = checkedMul(matrices, checkedMul(seq_len, seq_len) catch return false) catch return false;
    const rel_score_count = checkedMul(matrices, checkedMul(seq_len, rel_len) catch return false) catch return false;
    if (q.elem_count != count or k.elem_count != count or v.elem_count != count or q_r.elem_count != rel_count or k_r.elem_count != rel_count) return false;

    const f16_count_bytes = checkedMul(count, @sizeOf(u16)) catch return false;
    const f16_rel_bytes = checkedMul(packed_rel_count, @sizeOf(u16)) catch return false;
    const score_bytes = checkedMul(score_count, @sizeOf(f32)) catch return false;
    const rel_score_bytes = checkedMul(rel_score_count, @sizeOf(f32)) catch return false;
    const probability_bytes = checkedMul(score_count, @sizeOf(u16)) catch return false;
    const output_bytes = checkedMul(count, @sizeOf(f32)) catch return false;
    const layout = debertaMaterializedWorkspaceLayout(
        f16_count_bytes,
        f16_rel_bytes,
        score_bytes,
        rel_score_bytes,
        probability_bytes,
        output_bytes,
    ) catch return false;
    if (layout.total_bytes > cudaDebertaMaterializedWorkspaceLimitBytes(self)) {
        self.stats.deberta_materialized_workspace_rejections += 1;
        return false;
    }
    const arena = self.deberta_materialized_scratch.acquire(&self.ctx, layout.total_bytes) catch return false;
    self.stats.deberta_materialized_workspace_peak_bytes = @max(
        self.stats.deberta_materialized_workspace_peak_bytes,
        layout.total_bytes,
    );
    const q_f16 = debertaWorkspaceRegion(arena, layout.q_f16, f16_count_bytes);
    const k_f16 = debertaWorkspaceRegion(arena, layout.k_f16, f16_count_bytes);
    const v_f16 = debertaWorkspaceRegion(arena, layout.v_f16, f16_count_bytes);
    const q_r_f16 = debertaWorkspaceRegion(arena, layout.q_r_f16, f16_rel_bytes);
    const k_r_f16 = debertaWorkspaceRegion(arena, layout.k_r_f16, f16_rel_bytes);
    const content_scores = debertaWorkspaceRegion(arena, layout.content_scores, score_bytes);
    const c2p_scores = debertaWorkspaceRegion(arena, layout.c2p_scores, rel_score_bytes);
    const p2c_scores = debertaWorkspaceRegion(arena, layout.p2c_scores, rel_score_bytes);
    const probabilities_f16 = debertaWorkspaceRegion(arena, layout.probabilities_f16, probability_bytes);
    const output_packed = debertaWorkspaceRegion(arena, layout.output_packed, output_bytes);

    // Launch faults return false rather than propagating so the caller's
    // documented fallback chain runs and its fallback counter ticks; dst is
    // only written by the final unpack, so a partial pipeline leaves no state.
    if (!(self.kernels.launchDebertaPackHeadsF16(&self.ctx, q_f16, k_f16, v_f16, q_r_f16, k_r_f16, q.buffer, k.buffer, v.buffer, q_r.buffer, k_r.buffer, batch, seq_len, num_heads, head_dim) catch return false)) return false;
    const blas = &(self.cublaslt orelse return false);
    const workspace = cublasLtWorkspace(self);
    blas.matmulF16StridedBatchedF32Out(&self.ctx, content_scores, q_f16, k_f16, workspace, matrices, seq_len, head_dim, seq_len) catch return false;
    blas.matmulF16StridedBatchedF32Out(&self.ctx, c2p_scores, q_f16, k_r_f16, workspace, matrices, seq_len, head_dim, rel_len) catch return false;
    blas.matmulF16StridedBatchedF32Out(&self.ctx, p2c_scores, k_f16, q_r_f16, workspace, matrices, seq_len, head_dim, rel_len) catch return false;
    if (!(self.kernels.launchDebertaScoresSoftmaxF32(&self.ctx, content_scores, c2p_scores, p2c_scores, mask, batch, seq_len, num_heads, head_dim) catch return false)) return false;
    self.kernels.launchF32ToF16(&self.ctx, probabilities_f16, content_scores, score_count) catch return false;
    blas.matmulF16StridedBatchedF32Out(&self.ctx, output_packed, probabilities_f16, v_f16, workspace, matrices, seq_len, seq_len, head_dim) catch return false;
    if (!(self.kernels.launchDebertaUnpackHeadsF32(&self.ctx, dst, output_packed, batch, seq_len, num_heads, head_dim) catch return false)) return false;
    self.stats.deberta_materialized_f16_attention_calls += 1;
    return true;
}

// Q8_1 activation mirror: a norm kernel pre-quantized its output row, so a
// Q4/Q6 DP4A matmul can consume it directly instead of launching a separate
// quantize kernel. Blocks are position-independent per 32 contiguous values,
// so validity only needs the element count to match.
// Hybrid-residency dispatch: for a Q4- or F32-resident weight carrying a
// BF16 mirror, return the mirror when the matmul is prefill-shaped
// (rows > 1) so cuBLASLt handles it; decode (rows == 1) stays on the
// native Q4/F32 kernels.
fn weightBf16MirrorForRows(self: *const CudaCompute, weight: *const CudaTensor, rows: usize) ?buffer_mod.DeviceBuffer {
    if (!self.bf16_mirror_row_selector.accepts(rows)) return null;
    if (weight.bf16_mirror.ptr == 0) return null;
    if (weight.bf16_mirror.len < weight.elem_count * @sizeOf(u16)) return null;
    return weight.bf16_mirror;
}

fn cublasLtWorkspace(self: *CudaCompute) buffer_mod.DeviceBuffer {
    const bytes = cudaCublasLtWorkspaceBytes();
    if (bytes == 0) return .{};
    return self.cublaslt_workspace_scratch.acquire(&self.ctx, bytes) catch .{};
}

fn runCublasLtBf16Matmul(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    input_bf16: buffer_mod.DeviceBuffer,
    weight_bf16: buffer_mod.DeviceBuffer,
    workspace: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) bool {
    const blas = &(self.cublaslt orelse return false);
    const tuning = self.cublaslt_bf16_tuning_profile.tuningForRows(rows);
    const selection = blas.matmulBf16WeightF32OutWithTuning(
        &self.ctx,
        dst,
        input_bf16,
        weight_bf16,
        workspace,
        rows,
        in_dim,
        out_dim,
        tuning,
    ) catch {
        if (tuning.enabled) self.stats.bf16_cublaslt_tuning_api_fallbacks += 1;
        return false;
    };
    if (tuning.enabled) switch (selection) {
        .tuned => self.stats.bf16_cublaslt_tuning_tuned_calls += 1,
        .heuristic => self.stats.bf16_cublaslt_tuning_heuristic_calls += 1,
    };
    return true;
}

fn tryCublasLtBf16Linear(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    input: *const CudaTensor,
    weight: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const input_bf16 = try stageBf16ActivationForCublasLt(self, input, rows, in_dim) orelse return false;
    const workspace = cublasLtWorkspace(self);
    if (!runCublasLtBf16Matmul(self, dst, input_bf16, weight, workspace, rows, in_dim, out_dim)) return false;
    self.stats.bf16_cublaslt_linear_calls += 1;
    return true;
}

fn tryCublasLtF16Linear(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    input: *const CudaTensor,
    weight: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const input_f16 = try stageF16ActivationForCublasLt(self, input, rows, in_dim) orelse return false;
    const blas = &(self.cublaslt orelse return false);
    const workspace = cublasLtWorkspace(self);
    blas.matmulF16WeightF32Out(&self.ctx, dst, input_f16, weight, workspace, rows, in_dim, out_dim) catch return false;
    self.stats.f16_cublaslt_linear_calls += 1;
    return true;
}

fn tryCublasLtBf16LinearPair(
    self: *CudaCompute,
    dst_a: buffer_mod.DeviceBuffer,
    dst_b: buffer_mod.DeviceBuffer,
    input: *const CudaTensor,
    weight_a: buffer_mod.DeviceBuffer,
    weight_b: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const input_bf16 = try stageBf16ActivationForCublasLt(self, input, rows, in_dim) orelse return false;
    const workspace = cublasLtWorkspace(self);
    if (!runCublasLtBf16Matmul(self, dst_a, input_bf16, weight_a, workspace, rows, in_dim, out_dim)) return false;
    if (!runCublasLtBf16Matmul(self, dst_b, input_bf16, weight_b, workspace, rows, in_dim, out_dim)) return false;
    self.stats.bf16_cublaslt_linear_calls += 2;
    return true;
}

// Lazily build and cache the concatenated [2*ffn, in_dim] BF16 weight (gate
// rows then up rows) from the two per-weight mirrors, keyed by the gate
// mirror's device pointer. One device-to-device copy per half, once per layer.
fn ensureBf16PairMirror(
    self: *CudaCompute,
    gate_mirror: buffer_mod.DeviceBuffer,
    up_mirror: buffer_mod.DeviceBuffer,
    ffn: usize,
    in_dim: usize,
) !?buffer_mod.DeviceBuffer {
    const key: usize = @intCast(gate_mirror.ptr);
    if (self.bf16_pair_mirror_cache.get(key)) |buf| return buf;
    const half_elems = try checkedMul(ffn, in_dim);
    const half_bytes = try checkedMul(half_elems, @sizeOf(u16));
    if (gate_mirror.len < half_bytes or up_mirror.len < half_bytes) return null;
    var combined = try allocDeviceBuffer(self, try checkedMul(half_bytes, 2));
    // Both device copies are asynchronous. A later failure must retire the
    // destination through the stream-ordered temporary-buffer path.
    errdefer releaseDeviceBuffer(self, &combined);
    var lo = buffer_mod.DeviceBuffer{ .ptr = combined.ptr, .len = half_bytes };
    try lo.copyFromDevice(&self.ctx, gate_mirror, half_bytes);
    var hi = buffer_mod.DeviceBuffer{ .ptr = combined.ptr + half_bytes, .len = half_bytes };
    try hi.copyFromDevice(&self.ctx, up_mirror, half_bytes);
    try self.bf16_pair_mirror_cache.put(self.allocator, key, combined);
    return combined;
}

// Fused gate/up prefill: one cuBLASLt GEMM against the concatenated [2*ffn,
// in_dim] BF16 mirror producing [rows, 2*ffn] (gate cols then up cols per row),
// then the strided act(gate)*up epilogue in a single kernel → [rows, ffn].
// Returns null (fall through to the two-GEMM pair path) when either mirror is
// absent or staging is unavailable. Opt-in via fused_gate_up_bf16.
fn tryFusedGateUpBf16(
    self: *CudaCompute,
    input: CT,
    gate_weight: *const CudaTensor,
    up_weight: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    ffn: usize,
    activation: ops.DecoderRuntimeActivationKind,
) !?CT {
    if (!self.tuned_route_gates.fused_gate_up_bf16 or rows <= 1) return null;
    const gate_mirror = weightBf16MirrorForRows(self, gate_weight, rows) orelse return null;
    const up_mirror = weightBf16MirrorForRows(self, up_weight, rows) orelse return null;
    const combined = (try ensureBf16PairMirror(self, gate_mirror, up_mirror, ffn, in_dim)) orelse return null;

    const input_tensor = tensorFromCt(input);
    const input_bf16 = try stageBf16ActivationForCublasLt(self, input_tensor, rows, in_dim) orelse return null;
    const workspace = cublasLtWorkspace(self);
    const two_ffn = try checkedMul(ffn, 2);
    const combined_count = try checkedMul(rows, two_ffn);
    var combined_out = try allocDeviceBuffer(self, try checkedMul(combined_count, @sizeOf(f32)));
    // The activation kernel below consumes this buffer asynchronously.
    defer releaseDeviceBuffer(self, &combined_out);
    if (!runCublasLtBf16Matmul(self, combined_out, input_bf16, combined, workspace, rows, in_dim, two_ffn)) return null;
    self.stats.bf16_cublaslt_linear_calls += 1;

    const out_count = try checkedMul(rows, ffn);
    const shape = try allocShape2(self.allocator, rows, ffn);
    errdefer self.allocator.free(shape);
    var dst = try allocDeviceBuffer(self, try checkedMul(out_count, @sizeOf(f32)));
    errdefer dst.free(&self.ctx);
    try self.kernels.launchActivationMultiplyFusedGateUpF32(&self.ctx, dst, combined_out, rows, ffn, @intFromEnum(activation));
    self.stats.launch_elementwise += 1;
    if (self.stats.fused_gate_up_bf16_hits == 0) {
        std.log.info("cuda_fused_gate_up_bf16: status=active rows={d} in_dim={d} ffn={d}", .{ rows, in_dim, ffn });
    }
    self.stats.fused_gate_up_bf16_hits += 1;
    return createTensor(self, dst, shape, out_count);
}

fn tryCublasLtF16LinearPair(
    self: *CudaCompute,
    dst_a: buffer_mod.DeviceBuffer,
    dst_b: buffer_mod.DeviceBuffer,
    input: *const CudaTensor,
    weight_a: buffer_mod.DeviceBuffer,
    weight_b: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const input_f16 = try stageF16ActivationForCublasLt(self, input, rows, in_dim) orelse return false;
    const blas = &(self.cublaslt orelse return false);
    const workspace = cublasLtWorkspace(self);
    blas.matmulF16WeightF32Out(&self.ctx, dst_a, input_f16, weight_a, workspace, rows, in_dim, out_dim) catch return false;
    blas.matmulF16WeightF32Out(&self.ctx, dst_b, input_f16, weight_b, workspace, rows, in_dim, out_dim) catch return false;
    self.stats.f16_cublaslt_linear_calls += 2;
    return true;
}

fn tryCublasLtF32Linear(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (!cudaCublasLtEnabled()) return false;
    if (self.ctx.info.compute_major < 8) return false;
    if (rows < 128 or in_dim < 64 or out_dim < 64) return false;
    const blas = &(self.cublaslt orelse return false);
    blas.matmulF32WeightF32Out(&self.ctx, dst, input, weight, rows, in_dim, out_dim) catch return false;
    return true;
}

fn tryCublasLtBf16Qkv(
    self: *CudaCompute,
    dst_q: buffer_mod.DeviceBuffer,
    dst_k: buffer_mod.DeviceBuffer,
    dst_v: buffer_mod.DeviceBuffer,
    input: *const CudaTensor,
    weight_q: buffer_mod.DeviceBuffer,
    weight_k: buffer_mod.DeviceBuffer,
    weight_v: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
) !bool {
    const input_bf16 = try stageBf16ActivationForCublasLt(self, input, rows, in_dim) orelse return false;
    const workspace = cublasLtWorkspace(self);
    if (!runCublasLtBf16Matmul(self, dst_q, input_bf16, weight_q, workspace, rows, in_dim, q_out_dim)) return false;
    if (!runCublasLtBf16Matmul(self, dst_k, input_bf16, weight_k, workspace, rows, in_dim, kv_out_dim)) return false;
    if (!runCublasLtBf16Matmul(self, dst_v, input_bf16, weight_v, workspace, rows, in_dim, kv_out_dim)) return false;
    self.stats.bf16_cublaslt_qkv_calls += 1;
    return true;
}

fn tryCublasLtF16Qkv(
    self: *CudaCompute,
    dst_q: buffer_mod.DeviceBuffer,
    dst_k: buffer_mod.DeviceBuffer,
    dst_v: buffer_mod.DeviceBuffer,
    input: *const CudaTensor,
    weight_q: buffer_mod.DeviceBuffer,
    weight_k: buffer_mod.DeviceBuffer,
    weight_v: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
) !bool {
    const input_f16 = try stageF16ActivationForCublasLt(self, input, rows, in_dim) orelse return false;
    const blas = &(self.cublaslt orelse return false);
    const workspace = cublasLtWorkspace(self);
    blas.matmulF16WeightF32Out(&self.ctx, dst_q, input_f16, weight_q, workspace, rows, in_dim, q_out_dim) catch return false;
    blas.matmulF16WeightF32Out(&self.ctx, dst_k, input_f16, weight_k, workspace, rows, in_dim, kv_out_dim) catch return false;
    blas.matmulF16WeightF32Out(&self.ctx, dst_v, input_f16, weight_v, workspace, rows, in_dim, kv_out_dim) catch return false;
    self.stats.f16_cublaslt_qkv_calls += 1;
    return true;
}

const default_max_temp_buffers = 256;
const default_temp_arena_admission_budget_bytes: usize = 512 * 1024 * 1024;

fn cudaTempCacheMaxBuffers() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MAX_BUFFERS") orelse default_max_temp_buffers;
}

fn cudaTempCacheBudgetBytes() usize {
    const mb = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB") orelse 1024;
    return mb * 1024 * 1024;
}

fn cudaConfiguredTempArenaAdmissionBudgetBytes() ?usize {
    const mb = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_ARENA_MAX_MB") orelse return null;
    return std.math.mul(usize, mb, 1024 * 1024) catch 0;
}

/// Resolve a non-optional physical-footprint ceiling. An explicit arena limit
/// can narrow, but never widen, request-level scratch or backend/combined
/// memory policy. Without a request budget (diagnostic backends and unit
/// tests), retain a conservative bounded default instead of treating zero as
/// unlimited.
fn tempArenaAdmissionBudgetFor(
    configured_bytes: ?usize,
    run_budget: ?*const run_memory.RunBudget,
) usize {
    var admission_budget = configured_bytes orelse default_temp_arena_admission_budget_bytes;
    const budget = run_budget orelse return admission_budget;

    const scratch_limit = budget.limits.scratch_limit_bytes;
    if (configured_bytes == null and scratch_limit != 0) {
        admission_budget = scratch_limit;
    } else if (scratch_limit != 0) {
        admission_budget = @min(admission_budget, scratch_limit);
    }

    // `backend_scratch_bytes` is the request's estimate for this same scratch
    // domain and would double-count the arena. We only subtract independently
    // resident weight/KV reservations when calculating remaining headroom.
    const backend_non_scratch = std.math.add(
        usize,
        budget.backend_weight_bytes,
        budget.backend_kv_bytes,
    ) catch std.math.maxInt(usize);
    if (budget.limits.backend_limit_bytes != 0) {
        admission_budget = @min(
            admission_budget,
            budget.limits.backend_limit_bytes -| backend_non_scratch,
        );
    }
    if (budget.limits.combined_limit_bytes != 0) {
        const combined_non_arena = std.math.add(
            usize,
            budget.hostTotalBytes(),
            backend_non_scratch,
        ) catch std.math.maxInt(usize);
        admission_budget = @min(
            admission_budget,
            budget.limits.combined_limit_bytes -| combined_non_arena,
        );
    }
    return admission_budget;
}

fn cudaTempArenaAdmissionBudgetBytes(self: *const CudaCompute) usize {
    return tempArenaAdmissionBudgetFor(
        cudaConfiguredTempArenaAdmissionBudgetBytes(),
        self.run_budget,
    );
}

test "CUDA temp arena admission budget is bounded without a run budget" {
    try std.testing.expectEqual(
        default_temp_arena_admission_budget_bytes,
        tempArenaAdmissionBudgetFor(null, null),
    );
    try std.testing.expectEqual(@as(usize, 0), tempArenaAdmissionBudgetFor(0, null));
    try std.testing.expectEqual(@as(usize, 1234), tempArenaAdmissionBudgetFor(1234, null));
}

test "CUDA temp arena admission budget cannot widen request memory policy" {
    var budget = run_memory.RunBudget.init(.{
        .host_limit_bytes = 1_000,
        .backend_limit_bytes = 300,
        .combined_limit_bytes = 250,
        .scratch_limit_bytes = 200,
    });
    budget.host_kv_bytes = 30;
    budget.backend_weight_bytes = 100;
    budget.backend_kv_bytes = 50;
    // Combined non-arena bytes are 30 + 100 + 50, leaving 70. The scratch,
    // backend, and explicit ceilings are all wider and therefore cannot win.
    try std.testing.expectEqual(@as(usize, 70), tempArenaAdmissionBudgetFor(null, &budget));
    try std.testing.expectEqual(@as(usize, 70), tempArenaAdmissionBudgetFor(500, &budget));
    try std.testing.expectEqual(@as(usize, 40), tempArenaAdmissionBudgetFor(40, &budget));

    budget.limits.combined_limit_bytes = 0;
    // Backend non-arena bytes leave 150, narrower than the 200 scratch cap.
    try std.testing.expectEqual(@as(usize, 150), tempArenaAdmissionBudgetFor(null, &budget));
}

fn cudaDeferredFreeEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_DEFER_FREE", true);
}

fn cudaDeferredFreeBudgetBytes() usize {
    const mb = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_DEFER_FREE_BUDGET_MB") orelse 512;
    return mb * 1024 * 1024;
}

fn cudaTempTraceEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_TEMP_TRACE", false);
}

fn cudaTempTraceLimit() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_TRACE_LIMIT") orelse 4096;
}

fn cudaTempTraceSkip() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_TRACE_SKIP") orelse 0;
}

fn cudaTempStableReuseEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_TEMP_STABLE_REUSE", false);
}

const CudaDecodeGraphReplayMode = enum {
    off,
    auto,
    required,
};

fn cudaDecodeGraphReplayMode() CudaDecodeGraphReplayMode {
    const raw = platform.env.getenv("ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY") orelse return .off;
    if (std.mem.eql(u8, raw, "off") or std.mem.eql(u8, raw, "0") or std.mem.eql(u8, raw, "false") or std.mem.eql(u8, raw, "no")) return .off;
    if (std.mem.eql(u8, raw, "required") or std.mem.eql(u8, raw, "require") or std.mem.eql(u8, raw, "force")) return .required;
    if (std.mem.eql(u8, raw, "auto") or std.mem.eql(u8, raw, "on") or std.mem.eql(u8, raw, "1") or std.mem.eql(u8, raw, "true") or std.mem.eql(u8, raw, "yes")) return .auto;
    return .off;
}

fn cudaDecodeGraphReplayEnabled() bool {
    return cudaDecodeGraphReplayMode() != .off;
}

const max_temp_pinned_slots = 16_384;

fn cudaConfiguredTempSlotPeriod() usize {
    const period = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD") orelse 0;
    if (period > max_temp_pinned_slots) return 0;
    return period;
}

fn cudaConfiguredTempSlotSkip() usize {
    if (platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP")) |configured| return configured;
    if (cudaMtpTargetReplayEnabled() and cudaMtpUnsafeTargetReplayEnabled()) return 2500;
    return cudaTempTraceSkip();
}

fn cudaTempArenaAutoplanEnabled(capture_gates: CudaCaptureGateConfig) bool {
    const graph_capture_requested = cudaDecodeGraphReplayEnabled() or
        capture_gates.persistent_replay or
        capture_gates.update_exec;
    return graph_capture_requested and
        cudaConfiguredTempSlotPeriod() == 0 and
        platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN", true);
}

// Resolved once at CudaCompute.init: these settings gate every temporary
// allocation under temp_buffer_mutex and getenv is a linear scan of environ.
const CudaTempSlotConfig = struct {
    autoplan_enabled: bool = false,
    configured_period: usize = 0,
    configured_skip: usize = 0,

    fn resolve(capture_gates: CudaCaptureGateConfig) CudaTempSlotConfig {
        return .{
            .autoplan_enabled = cudaTempArenaAutoplanEnabled(capture_gates),
            .configured_period = cudaConfiguredTempSlotPeriod(),
            .configured_skip = cudaConfiguredTempSlotSkip(),
        };
    }
};

fn cudaEffectiveTempSlotPeriod(self: *const CudaCompute) usize {
    const configured = self.temp_slot_config.configured_period;
    if (configured != 0) return configured;
    if (!self.temp_slot_config.autoplan_enabled) return 0;
    return self.temp_arena_planner.period();
}

fn cudaEffectiveTempSlotSkip(self: *const CudaCompute) usize {
    if (self.temp_slot_config.configured_period != 0) return self.temp_slot_config.configured_skip;
    return self.temp_arena_planner.allocation_seq_base;
}

fn cudaDebugGraphCaptureMinAllocSeq(self: *const CudaCompute) usize {
    if (platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_MIN_ALLOC_SEQ")) |seq| return seq;
    const period = cudaEffectiveTempSlotPeriod(self);
    if (period != 0) return cudaEffectiveTempSlotSkip(self) + period;
    return 0;
}

fn cudaDebugGraphCaptureUpdateExecEnabled(self: *const CudaCompute) bool {
    return self.capture_gates.update_exec;
}

fn cudaMtpTargetReplayEnabled() bool {
    const raw = platform.env.getenv("ANTFLY_GEMMA4_MTP_TARGET_REPLAY") orelse return true;
    if (std.mem.eql(u8, raw, "off")) return false;
    if (std.mem.eql(u8, raw, "0")) return false;
    if (std.mem.eql(u8, raw, "false")) return false;
    return true;
}

fn cudaMtpUnsafeTargetReplayEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_UNSAFE_TARGET_REPLAY", false);
}

fn cudaMtpVerifyDeviceResultEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_VERIFY_DEVICE_RESULT", false);
}

fn cudaMtpPreprojectFusionEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_PREPROJECT_FUSION", true);
}

fn cudaMtpMaskedSelectFusionEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_MASKED_SELECT_FUSION", false);
}

fn cudaMtpMaskedSelectHiddenFusionEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_MASKED_SELECT_HIDDEN_FUSION", false);
}

fn cudaDebugGraphPersistentReplayEnabled(self: *const CudaCompute) bool {
    return cudaDecodeGraphReplayEnabled() or
        self.capture_gates.persistent_replay or
        (cudaMtpTargetReplayEnabled() and cudaMtpUnsafeTargetReplayEnabled());
}

fn cudaDebugGraphForcedKvReplayCapacityTokens() ?usize {
    const forced = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY") orelse return null;
    return if (forced == 0) null else forced;
}

pub fn cudaDecodeProfileEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PROFILE_DECODE", false);
}

pub fn cudaPrefillOpProfileEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS", false);
}

const CudaDecodeProfileCategory = enum {
    qkv,
    gqa_attention,
    attention_output,
    attention_norm_residual,
    ffn_gate_up,
    ffn_gated_down,
    ffn_post_norm,
    lm_head_argmax,
    graph_replay,
};

const CudaPrefillProfileCategory = enum {
    q4_linear,
    q4_qkv,
    q4_pair,
    q4_gated_down,
    bf16_linear,
    bf16_qkv,
    bf16_pair,
    attention,
    ple_dense,
    staging,
    norm,
    rope,
    kv_write,
    elementwise,
    embedding,
};

/// Bounded event-pair pool + pending ring backing the non-blocking op profiler.
/// Phase 0 observed ~3654 profiled prefill ops per request; 8192 gives headroom
/// so an entire request's timing events buffer between drains without a mid-run
/// sync. Allocated once, lazily, on the first profiled op; never allocated when
/// profiling is disabled.
const cuda_profile_pool_capacity: usize = 8192;

const CudaProfilePendingKind = union(enum) {
    prefill: CudaPrefillProfileCategory,
    decode: CudaDecodeProfileCategory,
};

const CudaProfilePendingEntry = struct {
    slot: u32 = 0,
    kind: CudaProfilePendingKind = .{ .prefill = .norm },
};

const CudaProfilePool = struct {
    // Reusable event pairs. Events are created lazily per slot on first acquire
    // and destroyed only at CudaCompute.deinit, so there is no per-op
    // cuEventCreate/cuEventDestroy churn.
    pairs: []context_mod.ProfileEventPair = &.{},
    // Stack of currently-free slot indices into `pairs`.
    free_stack: []u32 = &.{},
    free_count: usize = 0,
    // In-flight (recorded, not yet read) entries awaiting the next drain.
    pending: []CudaProfilePendingEntry = &.{},
    pending_count: usize = 0,
    allocated: bool = false,
};

fn ensureProfilePoolAllocated(self: *CudaCompute) !void {
    if (self.profile_pool.allocated) return;
    const cap = cuda_profile_pool_capacity;
    const pairs = try self.allocator.alloc(context_mod.ProfileEventPair, cap);
    errdefer self.allocator.free(pairs);
    @memset(pairs, .{});
    const free_stack = try self.allocator.alloc(u32, cap);
    errdefer self.allocator.free(free_stack);
    const pending = try self.allocator.alloc(CudaProfilePendingEntry, cap);
    errdefer self.allocator.free(pending);
    for (free_stack, 0..) |*entry, i| entry.* = @intCast(i);
    self.profile_pool = .{
        .pairs = pairs,
        .free_stack = free_stack,
        .free_count = cap,
        .pending = pending,
        .pending_count = 0,
        .allocated = true,
    };
}

fn deinitProfilePool(self: *CudaCompute) void {
    if (!self.profile_pool.allocated) return;
    for (self.profile_pool.pairs) |pair| self.ctx.destroyProfileEventPair(pair);
    self.allocator.free(self.profile_pool.pairs);
    self.allocator.free(self.profile_pool.free_stack);
    self.allocator.free(self.profile_pool.pending);
    self.profile_pool = .{};
}

fn recycleProfileSlot(self: *CudaCompute, slot: u32) void {
    self.profile_pool.free_stack[self.profile_pool.free_count] = slot;
    self.profile_pool.free_count += 1;
}

/// Acquire a pooled event pair and record its start event (async, no sync).
/// Returns null (profiling silently skips this op) if allocation fails or the
/// pool cannot be recycled. A free-stack pool hands out a distinct slot per open
/// scope, so it stays correct even where the existing code nests scopes (e.g.
/// the `.staging` scope opened inside a `.bf16_linear` GEMM).
fn acquireProfileSlot(self: *CudaCompute) ?u32 {
    ensureProfilePoolAllocated(self) catch return null;
    if (self.profile_pool.free_count == 0) {
        // Pool exhausted mid-request: drain in place with a single sync to
        // recycle every completed pair, then continue.
        drainCudaProfile(self);
        if (self.profile_pool.free_count == 0) return null;
    }
    self.profile_pool.free_count -= 1;
    const slot = self.profile_pool.free_stack[self.profile_pool.free_count];
    if (self.profile_pool.pairs[slot].start == null) {
        self.profile_pool.pairs[slot] = self.ctx.createProfileEventPair() catch {
            self.profile_pool.free_count += 1; // return the untouched slot
            return null;
        };
    }
    self.ctx.recordProfileStart(self.profile_pool.pairs[slot]) catch {
        self.profile_pool.free_count += 1;
        return null;
    };
    return slot;
}

/// Record a slot's end event (async, no sync) and buffer it for the next drain.
fn finishProfileSlot(self: *CudaCompute, slot: u32, kind: CudaProfilePendingKind) void {
    self.ctx.recordProfileEnd(self.profile_pool.pairs[slot]) catch {
        // End could not be recorded; recycle without counting a garbage interval.
        recycleProfileSlot(self, slot);
        return;
    };
    if (self.profile_pool.pending_count >= self.profile_pool.pending.len) {
        // Ring full: drain in place (one sync) before buffering this entry.
        drainCudaProfile(self);
    }
    self.profile_pool.pending[self.profile_pool.pending_count] = .{ .slot = slot, .kind = kind };
    self.profile_pool.pending_count += 1;
}

/// Drain all buffered profile entries with a SINGLE host sync, attributing each
/// recorded interval to its category, then return every pair to the pool. This
/// is the only place the profiler blocks the host and it runs off the hot path
/// (end of prefill / end of request). No-op when profiling is disabled or the
/// ring is empty.
pub fn drainCudaProfile(self: *CudaCompute) void {
    if (!self.profile_pool.allocated) return;
    if (self.profile_pool.pending_count == 0) return;
    // One stream sync guarantees every recorded start/end event has completed,
    // so the whole batch reads back without per-op synchronization.
    self.ctx.synchronize() catch {
        // Sync failed: timings are untrustworthy, so recycle without counting.
        for (self.profile_pool.pending[0..self.profile_pool.pending_count]) |entry| {
            recycleProfileSlot(self, entry.slot);
        }
        self.profile_pool.pending_count = 0;
        return;
    };
    for (self.profile_pool.pending[0..self.profile_pool.pending_count]) |entry| {
        const elapsed_us = self.ctx.readProfileEventPairUs(self.profile_pool.pairs[entry.slot]) catch 0;
        switch (entry.kind) {
            .prefill => |cat| notePrefillProfileUs(self, cat, elapsed_us),
            .decode => |cat| noteDecodeProfileUs(self, cat, elapsed_us),
        }
        recycleProfileSlot(self, entry.slot);
    }
    self.profile_pool.pending_count = 0;
}

const CudaDecodeProfileScope = struct {
    compute: *CudaCompute,
    category: CudaDecodeProfileCategory,
    slot: u32,

    fn end(self: *CudaDecodeProfileScope) void {
        defer {
            if (self.category == .gqa_attention) self.compute.decode_profile_gqa_attention_active = false;
        }
        finishProfileSlot(self.compute, self.slot, .{ .decode = self.category });
    }
};

const CudaPrefillProfileScope = struct {
    compute: *CudaCompute,
    category: CudaPrefillProfileCategory,
    slot: u32,

    fn end(self: *CudaPrefillProfileScope) void {
        finishProfileSlot(self.compute, self.slot, .{ .prefill = self.category });
    }
};

fn beginDecodeProfile(self: *CudaCompute, category: CudaDecodeProfileCategory, rows: usize) ?CudaDecodeProfileScope {
    if (rows != 1) return null;
    if (!cudaDecodeProfileEnabled()) return null;
    if (self.debug_cuda_graph_capture_active) return null;
    if (category == .gqa_attention) {
        if (self.decode_profile_gqa_attention_active) return null;
        self.decode_profile_gqa_attention_active = true;
    }
    const slot = acquireProfileSlot(self) orelse {
        if (category == .gqa_attention) self.decode_profile_gqa_attention_active = false;
        return null;
    };
    return .{ .compute = self, .category = category, .slot = slot };
}

fn beginPrefillProfile(self: *CudaCompute, category: ?CudaPrefillProfileCategory, rows: usize) ?CudaPrefillProfileScope {
    const resolved_category = category orelse return null;
    if (rows <= 1) return null;
    if (!cudaPrefillOpProfileEnabled()) return null;
    if (self.debug_cuda_graph_capture_active) return null;
    const slot = acquireProfileSlot(self) orelse return null;
    return .{ .compute = self, .category = resolved_category, .slot = slot };
}

fn noteDecodeProfileUs(self: *CudaCompute, category: CudaDecodeProfileCategory, elapsed_us: u64) void {
    self.stats.decode_profile_events += 1;
    switch (category) {
        .qkv => self.stats.decode_profile_qkv_us += elapsed_us,
        .gqa_attention => self.stats.decode_profile_gqa_attention_us += elapsed_us,
        .attention_output => self.stats.decode_profile_attention_output_us += elapsed_us,
        .attention_norm_residual => self.stats.decode_profile_attention_norm_residual_us += elapsed_us,
        .ffn_gate_up => self.stats.decode_profile_ffn_gate_up_us += elapsed_us,
        .ffn_gated_down => self.stats.decode_profile_ffn_gated_down_us += elapsed_us,
        .ffn_post_norm => self.stats.decode_profile_ffn_post_norm_us += elapsed_us,
        .lm_head_argmax => self.stats.decode_profile_lm_head_argmax_us += elapsed_us,
        .graph_replay => self.stats.decode_profile_graph_replay_us += elapsed_us,
    }
}

fn notePrefillProfileUs(self: *CudaCompute, category: CudaPrefillProfileCategory, elapsed_us: u64) void {
    self.stats.prefill_profile_events += 1;
    switch (category) {
        .q4_linear => self.stats.prefill_profile_q4_linear_us += elapsed_us,
        .q4_qkv => self.stats.prefill_profile_q4_qkv_us += elapsed_us,
        .q4_pair => self.stats.prefill_profile_q4_pair_us += elapsed_us,
        .q4_gated_down => self.stats.prefill_profile_q4_gated_down_us += elapsed_us,
        .bf16_linear => self.stats.prefill_profile_bf16_linear_us += elapsed_us,
        .bf16_qkv => self.stats.prefill_profile_bf16_qkv_us += elapsed_us,
        .bf16_pair => self.stats.prefill_profile_bf16_pair_us += elapsed_us,
        .attention => self.stats.prefill_profile_attention_us += elapsed_us,
        .ple_dense => self.stats.prefill_profile_ple_dense_us += elapsed_us,
        .staging => self.stats.prefill_profile_staging_us += elapsed_us,
        .norm => self.stats.prefill_profile_norm_us += elapsed_us,
        .rope => self.stats.prefill_profile_rope_us += elapsed_us,
        .kv_write => self.stats.prefill_profile_kv_write_us += elapsed_us,
        .elementwise => self.stats.prefill_profile_elementwise_us += elapsed_us,
        .embedding => self.stats.prefill_profile_embedding_us += elapsed_us,
    }
}

test "CUDA op-profile pool free-stack, drain no-op, and category mapping" {
    // Exercises the GPU-free parts of the non-blocking profiler: lazy pool
    // allocation, the LIFO free-stack that lets nested scopes hold distinct
    // slots, the empty-ring drain fast path (never touches the GPU context), and
    // the category -> stat-field mapping for the newly added prefill buckets.
    var compute: CudaCompute = undefined;
    compute.allocator = std.testing.allocator;
    compute.profile_pool = .{};
    compute.stats = .{};

    try ensureProfilePoolAllocated(&compute);
    defer {
        compute.allocator.free(compute.profile_pool.pairs);
        compute.allocator.free(compute.profile_pool.free_stack);
        compute.allocator.free(compute.profile_pool.pending);
    }
    try std.testing.expect(compute.profile_pool.allocated);
    try std.testing.expectEqual(cuda_profile_pool_capacity, compute.profile_pool.free_count);
    try std.testing.expectEqual(cuda_profile_pool_capacity, compute.profile_pool.pairs.len);
    // Lazy allocation must not have created any CUDA events yet.
    for (compute.profile_pool.pairs) |pair| try std.testing.expect(pair.start == null and pair.end == null);

    // Simulate acquiring three overlapping slots (the pop half of acquire) and
    // returning them; the free-stack must round-trip without loss or aliasing.
    var popped: [3]u32 = undefined;
    for (&popped) |*p| {
        compute.profile_pool.free_count -= 1;
        p.* = compute.profile_pool.free_stack[compute.profile_pool.free_count];
    }
    try std.testing.expectEqual(cuda_profile_pool_capacity - 3, compute.profile_pool.free_count);
    try std.testing.expect(popped[0] != popped[1] and popped[1] != popped[2] and popped[0] != popped[2]);
    for (popped) |slot| recycleProfileSlot(&compute, slot);
    try std.testing.expectEqual(cuda_profile_pool_capacity, compute.profile_pool.free_count);

    // Draining an empty ring is a no-op and must not require the GPU context.
    drainCudaProfile(&compute);
    try std.testing.expectEqual(@as(usize, 0), compute.profile_pool.pending_count);

    // New prefill buckets land in the right stat fields.
    notePrefillProfileUs(&compute, .rope, 5);
    notePrefillProfileUs(&compute, .kv_write, 7);
    notePrefillProfileUs(&compute, .elementwise, 11);
    notePrefillProfileUs(&compute, .embedding, 13);
    try std.testing.expectEqual(@as(u64, 5), compute.stats.prefill_profile_rope_us);
    try std.testing.expectEqual(@as(u64, 7), compute.stats.prefill_profile_kv_write_us);
    try std.testing.expectEqual(@as(u64, 11), compute.stats.prefill_profile_elementwise_us);
    try std.testing.expectEqual(@as(u64, 13), compute.stats.prefill_profile_embedding_us);
    try std.testing.expectEqual(@as(usize, 4), compute.stats.prefill_profile_events);
}

fn prefillProfileCategoryForLinearNoBias(weight: *const CudaTensor, rows: usize, in_dim: usize, out_dim: usize) ?CudaPrefillProfileCategory {
    if (rows <= 1) return null;
    if (isKnownQuant(weight, .Q4_0)) return if (weight.bf16_mirror.ptr != 0) .bf16_linear else .q4_linear;
    if (isBf16Weight(weight)) return .bf16_linear;
    if (weight.quant_type == null and weight.dtype == .f32 and in_dim == 2560 and out_dim == 10752) return .ple_dense;
    return null;
}

fn cudaDebugGraphCaptureDeviceScalarsEnabled(self: *const CudaCompute) bool {
    return cudaDecodeGraphReplayEnabled() or
        self.capture_gates.device_scalars or
        (cudaMtpTargetReplayEnabled() and cudaMtpUnsafeTargetReplayEnabled());
}

fn cudaTurboquantSplitAttentionEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION", false);
}

fn cudaTurboquantSplitAttentionChunkSize() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION_CHUNK") orelse 128;
}

fn cudaDebugGraphAutoAdvanceDecodeScalarsEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_AUTO_ADVANCE_SCALARS", true);
}

fn cudaDebugGraphCaptureAllowUnpinned() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_ALLOW_UNPINNED", false) or
        (cudaDecodeGraphReplayEnabled() and cudaTempStableReuseEnabled());
}

fn cudaDebugGraphCaptureTensorTraceEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_TENSOR_TRACE", false);
}

fn cudaDebugGraphCaptureProbeTraceEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GRAPH_CAPTURE_PROBE_TRACE", false);
}

fn cudaDebugGraphCaptureTensorTraceLimit() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_TENSOR_TRACE_LIMIT") orelse 64;
}

fn cudaDebugDecodeScalarsReady(self: *const CudaCompute) bool {
    return cudaDebugGraphCaptureDeviceScalarsEnabled(self) and self.debug_cuda_graph_capture_active and self.debug_cuda_decode_scalars.ptr != 0;
}

fn cudaGraphExecUpdateResultName(result: driver_mod.CUgraphExecUpdateResult) []const u8 {
    return switch (result) {
        driver_mod.CU_GRAPH_EXEC_UPDATE_SUCCESS => "success",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR => "error",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_TOPOLOGY_CHANGED => "topology_changed",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_NODE_TYPE_CHANGED => "node_type_changed",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_FUNCTION_CHANGED => "function_changed",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_PARAMETERS_CHANGED => "parameters_changed",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_NOT_SUPPORTED => "not_supported",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_UNSUPPORTED_FUNCTION_CHANGE => "unsupported_function_change",
        driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR_ATTRIBUTES_CHANGED => "attributes_changed",
        else => "unknown",
    };
}

fn traceCudaTempAlloc(
    self: *CudaCompute,
    seq: usize,
    comptime event: []const u8,
    requested_len: usize,
    buffer: buffer_mod.DeviceBuffer,
) void {
    if (!cudaTempTraceEnabled()) return;
    const skip = cudaTempTraceSkip();
    if (seq < skip) return;
    const trace_seq = seq - skip;
    const limit = cudaTempTraceLimit();
    if (trace_seq >= limit) return;
    if (trace_seq == 0) {
        std.log.info(
            "cuda_temp_trace: begin skip={d} limit={d} temp_cache_budget_mb={d} stable_reuse={} slot_period={d} slot_skip={d}",
            .{
                skip,
                limit,
                cudaTempCacheBudgetBytes() / (1024 * 1024),
                cudaTempStableReuseEnabled(),
                cudaEffectiveTempSlotPeriod(self),
                cudaEffectiveTempSlotSkip(self),
            },
        );
    }
    std.log.info(
        "cuda_temp_trace: seq={d} trace_seq={d} event={s} requested={d} buffer_len={d} ptr=0x{x} cache_count={d}",
        .{ seq, trace_seq, event, requested_len, buffer.len, buffer.ptr, self.temp_buffers.items.len },
    );
    if (trace_seq + 1 == limit) {
        std.log.info("cuda_temp_trace: limit_reached limit={d}", .{limit});
    }
}

fn ensureTempPinnedSlots(self: *CudaCompute, period: usize) !void {
    if (self.temp_pinned_slots.items.len >= period) return;
    try self.temp_pinned_slots.ensureTotalCapacity(self.allocator, period);
    while (self.temp_pinned_slots.items.len < period) {
        self.temp_pinned_slots.appendAssumeCapacity(.{});
    }
}

fn findExactTempBufferIndex(self: *const CudaCompute, len: usize) ?usize {
    for (self.temp_buffers.items, 0..) |buffer, i| {
        if (buffer.len == len) return i;
    }
    return null;
}

fn allocPinnedTempSlot(
    self: *CudaCompute,
    seq: usize,
    len: usize,
) !?buffer_mod.DeviceBuffer {
    const period = cudaEffectiveTempSlotPeriod(self);
    if (period == 0) return null;
    const skip = cudaEffectiveTempSlotSkip(self);
    if (seq < skip) return null;

    try ensureTempPinnedSlots(self, period);
    const slot_index = (seq - skip) % period;
    const slot = &self.temp_pinned_slots.items[slot_index];
    if (slot.in_use) {
        if (self.temp_slot_config.autoplan_enabled) self.temp_arena_planner.markPinnedValidationFailure();
        traceCudaTempAlloc(self, seq, "alloc_slot_busy_fallback", len, .{});
        return null;
    }
    if (slot.requested_len != 0 and slot.requested_len != len) {
        if (self.temp_slot_config.autoplan_enabled) self.temp_arena_planner.markPinnedValidationFailure();
        traceCudaTempAlloc(self, seq, "alloc_slot_shape_fallback", len, .{});
        return null;
    }
    if (slot.buffer.ptr == 0) {
        slot.requested_len = len;
        if (findExactTempBufferIndex(self, len)) |i| {
            slot.buffer = self.temp_buffers.orderedRemove(i);
            self.stats.temp_buffer_hits += 1;
            slot.in_use = true;
            traceCudaTempAlloc(self, seq, "alloc_slot_seed", len, slot.buffer);
            return slot.buffer;
        }
        self.stats.temp_buffer_misses += 1;
        slot.buffer = try buffer_mod.DeviceBuffer.alloc(&self.ctx, len);
        self.noteDeviceBytes(len);
        self.stats.device_alloc_calls += 1;
        slot.in_use = true;
        traceCudaTempAlloc(self, seq, "alloc_slot_miss", len, slot.buffer);
        return slot.buffer;
    }
    slot.in_use = true;
    self.stats.temp_buffer_hits += 1;
    traceCudaTempAlloc(self, seq, "alloc_slot_hit", len, slot.buffer);
    return slot.buffer;
}

fn releasePinnedTempSlot(self: *CudaCompute, buffer: *buffer_mod.DeviceBuffer) bool {
    if (buffer.ptr == 0 or self.temp_pinned_slots.items.len == 0) return false;
    for (self.temp_pinned_slots.items) |*slot| {
        if (slot.buffer.ptr == buffer.ptr) {
            slot.in_use = false;
            buffer.* = .{};
            return true;
        }
    }
    return false;
}

fn resetTempPinnedSlotsForRequest(self: *CudaCompute) !void {
    // Graph execs retain every captured device address. Destroy them before
    // returning any pinned arena allocation to the general temp cache.
    invalidateDebugFinalHiddenGraph(self);
    while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
    defer self.temp_buffer_mutex.unlock();

    try recycleTempPinnedSlotsUnlocked(self);
    self.temp_trace_seq = 0;
    self.temp_arena_planner.resetForRequest();
}

fn recycleTempPinnedSlotsUnlocked(self: *CudaCompute) !void {
    var reusable_count: usize = 0;
    for (self.temp_pinned_slots.items) |slot| {
        if (slot.in_use) return error.InvalidCudaState;
        reusable_count += @intFromBool(slot.buffer.ptr != 0);
    }
    try self.temp_buffers.ensureUnusedCapacity(self.allocator, reusable_count);
    const cache_budget = cudaTempCacheBudgetBytes();
    const max_buffers = cudaTempCacheMaxBuffers();
    var cached_bytes: usize = 0;
    for (self.temp_buffers.items) |cached| cached_bytes +|= cached.len;
    for (self.temp_pinned_slots.items) |*slot| {
        if (slot.buffer.ptr != 0) {
            var buffer = slot.buffer;
            if (self.temp_buffers.items.len < max_buffers and
                buffer.len <= cache_budget and
                cached_bytes <= cache_budget - buffer.len)
            {
                self.temp_buffers.appendAssumeCapacity(buffer);
                cached_bytes += buffer.len;
                self.stats.temp_buffer_releases += 1;
                buffer = .{};
            } else {
                evictTempBufferUnlocked(self, &buffer);
            }
        }
        slot.* = .{};
    }
    self.temp_arena_generation +%= 1;
    if (self.temp_arena_generation == 0) self.temp_arena_generation = 1;
}

fn installLearnedTempArenaPlan(self: *CudaCompute) !bool {
    const lengths = self.temp_arena_planner.candidate_lengths.items;
    if (!self.temp_arena_planner.active or lengths.len == 0 or lengths.len > max_temp_pinned_slots) {
        return error.InvalidCudaState;
    }
    const admission = self.temp_arena_planner.admitCandidate(cudaTempArenaAdmissionBudgetBytes(self));
    if (!admission.admitted) {
        // Admission is evaluated before installing or allocating a single new
        // pinned address. Destroy any older graph/arena state as well: its
        // pointers cannot remain reusable after this planner is disabled.
        try discardLearnedTempArenaSlots(self);
        std.log.warn(
            "cuda_temp_arena_plan: admission_denied slots={d} physical_high_water_bytes={d} largest_slot_bytes={d} budget_bytes={d} overflowed={}",
            .{
                lengths.len,
                admission.physical_high_water_bytes,
                admission.largest_slot_bytes,
                admission.budget_bytes,
                admission.overflowed,
            },
        );
        return false;
    }
    invalidateDebugFinalHiddenGraph(self);
    while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
    defer self.temp_buffer_mutex.unlock();

    try recycleTempPinnedSlotsUnlocked(self);
    try ensureTempPinnedSlots(self, lengths.len);
    for (lengths, 0..) |len, index| {
        self.temp_pinned_slots.items[index].requested_len = len;
    }
    return true;
}

fn discardLearnedTempArenaSlots(self: *CudaCompute) !void {
    invalidateDebugFinalHiddenGraph(self);
    while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
    defer self.temp_buffer_mutex.unlock();
    try recycleTempPinnedSlotsUnlocked(self);
}

fn noteLearnedTempArenaBoundary(self: *CudaCompute) !void {
    if (!self.temp_slot_config.autoplan_enabled) return;
    while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
    const transition = self.temp_arena_planner.beginDecodeIteration(
        self.allocator,
        self.temp_trace_seq,
    ) catch |err| {
        self.temp_buffer_mutex.unlock();
        return err;
    };
    self.temp_buffer_mutex.unlock();
    switch (transition) {
        .activated => {
            if (!try installLearnedTempArenaPlan(self)) return;
            std.log.info(
                "cuda_temp_arena_plan: activated slots={d} physical_high_water_bytes={d} budget_bytes={d} alloc_seq_base={d} observations={d}",
                .{
                    self.temp_arena_planner.period(),
                    self.temp_arena_planner.planned_physical_high_water_bytes,
                    self.temp_arena_planner.admission_budget_bytes,
                    self.temp_arena_planner.allocation_seq_base,
                    self.temp_arena_planner.observations,
                },
            );
        },
        .invalidated => {
            // `discardLearnedTempArenaSlots` destroys every graph before it
            // recycles the addresses those graph execs may still reference.
            try discardLearnedTempArenaSlots(self);
            if (cudaDebugGraphCaptureProbeTraceEnabled()) {
                std.log.warn("cuda_temp_arena_plan: invalidated observations={d} invalidations={d}", .{
                    self.temp_arena_planner.observations,
                    self.temp_arena_planner.invalidations,
                });
            }
        },
        .observing, .unchanged => {},
    }
}

fn retainPinnedTempSlot(self: *CudaCompute, buffer: buffer_mod.DeviceBuffer) bool {
    if (buffer.ptr == 0 or self.temp_pinned_slots.items.len == 0) return false;
    for (self.temp_pinned_slots.items) |*slot| {
        if (slot.buffer.ptr == buffer.ptr) {
            if (slot.in_use) return false;
            slot.in_use = true;
            return true;
        }
    }
    return false;
}

fn drainDeferredDeviceFreesAfterSyncUnlocked(self: *CudaCompute) void {
    if (self.deferred_device_frees.items.len == 0) return;
    var reclaimed: usize = 0;
    for (self.deferred_device_frees.items) |*buffer| {
        reclaimed += buffer.len;
        self.stats.device_free_calls += 1;
        buffer.free(&self.ctx);
    }
    self.deferred_device_frees.clearRetainingCapacity();
    self.deferred_device_free_bytes = 0;
    self.stats.deferred_free_drains += 1;
    self.stats.deferred_free_reclaimed_bytes += reclaimed;
}

fn synchronizeAndDrainDeferredDeviceFrees(self: *CudaCompute) !void {
    while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
    defer self.temp_buffer_mutex.unlock();
    try synchronizeAndDrainDeferredDeviceFreesUnlocked(self);
}

fn synchronizeAndDrainDeferredDeviceFreesUnlocked(self: *CudaCompute) !void {
    try self.ctx.synchronize();
    drainDeferredDeviceFreesAfterSyncUnlocked(self);
    self.training_upload_generation +%= 1;
    // Both page-locked upload staging areas are stream ordered. A completed
    // stream makes every previously submitted source slice reusable.
    self.pinned_scalar_upload_ring.next_slot = 0;
    self.pinned_upload_arena.next_offset = 0;
}

fn forceDrainDeferredDeviceFrees(self: *CudaCompute) !void {
    while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
    defer self.temp_buffer_mutex.unlock();
    try forceDrainDeferredDeviceFreesUnlocked(self);
}

fn forceDrainDeferredDeviceFreesUnlocked(self: *CudaCompute) !void {
    if (self.deferred_device_frees.items.len == 0) return;
    try synchronizeAndDrainDeferredDeviceFreesUnlocked(self);
    self.stats.deferred_free_forced_drains += 1;
}

fn disableCudaLazyHostPrefetchWorker() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_DISABLE_PREFETCH_WORKER");
}

fn installCudaLazyHostPrefetch(self: *CudaCompute, store: *native_compute_mod.WeightStore) !void {
    if (store.prefetch_initialized) {
        native_compute_mod.stopPrefetchWorker(store);
        native_compute_mod.deinitPrefetchQueue(store);
    }
    store.prefetch = @TypeOf(store.prefetch).initWithPriorityUnlocked(
        self.allocator,
        self,
        &cudaLazyHostPrefetchProcess,
        &cudaLazyHostPrefetchPriority,
    );
    store.prefetch_initialized = true;
    var lazy_it = store.lazy_weights.iterator();
    while (lazy_it.next()) |entry| {
        entry.value_ptr.guard = store.prefetch.mutexPtr();
    }
    if (store.lazy_weights.count() > 0 and !disableCudaLazyHostPrefetchWorker()) {
        try native_compute_mod.startPrefetchWorker(store);
    }
}

fn cudaLazyHostPrefetchProcess(ctx: *anyopaque, entry: *native_compute_mod.LazyWeightEntry) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const store = self.lazy_host_store orelse {
        entry.pending_prefetch = false;
        return;
    };
    var tensor_touch: []const u8 = &.{};
    var quant_touch: []const u8 = &.{};
    store.prefetch.lock();
    entry.pending_prefetch = false;
    native_compute_mod.ensureLazyWeightLoadedLocked(store, null, entry) catch {
        store.prefetch.unlock();
        return;
    };
    if (entry.loaded) |*loaded| {
        if (!loaded.tensor.owns_data) {
            tensor_touch = loaded.tensor.data;
        }
        if (loaded.quantized_storage) |*storage| {
            if (!storage.raw_owned or storage.raw_mmap_backed) {
                quant_touch = storage.raw_bytes;
            }
        }
    }
    store.prefetch.unlock();
    touchBytesForPageCache(tensor_touch);
    touchBytesForPageCache(quant_touch);
    store.prefetch.lock();
    defer store.prefetch.unlock();
    if (store.shared_prefetch) |shared_prefetch| {
        entry.prefetch_score = shared_prefetch.noteComplete(entry.tensor_ref.name) catch 0;
    } else {
        entry.prefetch_score = 0;
    }
}

fn cudaLazyHostPrefetchPriority(entry: *native_compute_mod.LazyWeightEntry) u64 {
    return entry.prefetch_score;
}

fn touchLoadedWeightPages(loaded: *const weight_source_mod.LoadedWeight) void {
    touchBytesForPageCache(loaded.tensor.data);
    if (loaded.quantized_storage) |storage| {
        touchBytesForPageCache(storage.raw_bytes);
    }
}

fn touchBytesForPageCache(bytes: []const u8) void {
    if (bytes.len == 0) return;
    const page_size = 4096;
    var checksum: u8 = 0;
    var index: usize = 0;
    while (index < bytes.len) : (index += page_size) {
        checksum +%= bytes[index];
    }
    checksum +%= bytes[bytes.len - 1];
    std.mem.doNotOptimizeAway(checksum);
}

const DeviceBufferLease = struct {
    buffer: buffer_mod.DeviceBuffer,
    owns_buffer: bool,
    prepared_greedy_slot: ?usize = null,
};

fn releaseDeviceBufferLease(self: *CudaCompute, lease: *DeviceBufferLease) void {
    if (lease.owns_buffer) releaseDeviceBuffer(self, &lease.buffer);
    lease.buffer = .{};
    lease.owns_buffer = false;
    lease.prepared_greedy_slot = null;
}

const PreparedGreedyTokenOutput = struct {
    buffer: buffer_mod.DeviceBuffer,
    slot_idx: usize,
};

fn preparedGreedyTokenOutputBuffer(self: *CudaCompute, byte_len: usize) !?PreparedGreedyTokenOutput {
    if (self.debug_cuda_graph_prepared_output_kind != .greedy_token) return null;
    const slot_idx = self.debug_cuda_graph_prepared_slot orelse return error.InvalidCudaState;
    if (self.debug_cuda_graph_capture_active and self.debug_cuda_graph_active_slot != slot_idx) {
        return error.InvalidCudaState;
    }
    const storage = self.debug_cuda_graph_slots[slot_idx].output_storage;
    if (storage.ptr == 0 or storage.len < byte_len) return error.InvalidCudaState;
    return .{
        .buffer = .{ .ptr = storage.ptr, .len = byte_len },
        .slot_idx = slot_idx,
    };
}

fn allocGreedyTokenDeviceBuffer(self: *CudaCompute, byte_len: usize) !DeviceBufferLease {
    if (try preparedGreedyTokenOutputBuffer(self, byte_len)) |prepared| {
        return .{
            .buffer = prepared.buffer,
            .owns_buffer = false,
            .prepared_greedy_slot = prepared.slot_idx,
        };
    }
    return .{ .buffer = try allocDeviceBuffer(self, byte_len), .owns_buffer = true };
}

test "CUDA greedy graph output borrows only the prepared replay slot" {
    var self: CudaCompute = undefined;
    self.debug_cuda_graph_slots = [_]CudaGraphReplaySlot{.{}} ** max_cuda_graph_replay_slots;
    self.debug_cuda_graph_prepared_slot = 3;
    self.debug_cuda_graph_active_slot = null;
    self.debug_cuda_graph_capture_active = false;
    self.debug_cuda_graph_capture_disabled = false;
    self.debug_cuda_graph_prepared_output_kind = .tensor;
    self.debug_cuda_graph_slots[3].output_storage = .{ .ptr = 0x1234, .len = 64 };

    try std.testing.expect((try preparedGreedyTokenOutputBuffer(&self, @sizeOf(i32))) == null);
    self.debug_cuda_graph_prepared_output_kind = .greedy_token;
    const borrowed = (try preparedGreedyTokenOutputBuffer(&self, @sizeOf(i32))).?;
    try std.testing.expectEqual(@as(driver_mod.CUdeviceptr, 0x1234), borrowed.buffer.ptr);
    try std.testing.expectEqual(@as(usize, @sizeOf(i32)), borrowed.buffer.len);
    try std.testing.expectEqual(@as(usize, 3), borrowed.slot_idx);
    try std.testing.expectError(error.InvalidCudaState, preparedGreedyTokenOutputBuffer(&self, 65));

    self.debug_cuda_graph_capture_active = true;
    self.debug_cuda_graph_active_slot = 2;
    try std.testing.expectError(error.InvalidCudaState, preparedGreedyTokenOutputBuffer(&self, @sizeOf(i32)));
}

test "CUDA greedy graph output lease remains armed for retry and consumes on tensor creation" {
    var self: CudaCompute = undefined;
    self.allocator = std.testing.allocator;
    self.debug_cuda_graph_slots = [_]CudaGraphReplaySlot{.{}} ** max_cuda_graph_replay_slots;
    self.debug_cuda_graph_prepared_slot = 1;
    self.debug_cuda_graph_active_slot = null;
    self.debug_cuda_graph_capture_active = false;
    self.debug_cuda_graph_prepared_output_kind = .greedy_token;
    self.debug_cuda_graph_slots[1].output_storage = .{ .ptr = 0x5678, .len = 64 };

    var failed_route_lease = try allocGreedyTokenDeviceBuffer(&self, @sizeOf(i32));
    try std.testing.expect(!failed_route_lease.owns_buffer);
    try std.testing.expectEqual(@as(?usize, 1), failed_route_lease.prepared_greedy_slot);
    releaseDeviceBufferLease(&self, &failed_route_lease);
    try std.testing.expectEqual(CudaGraphReplayOutputKind.greedy_token, self.debug_cuda_graph_prepared_output_kind);

    var successful_lease = try allocGreedyTokenDeviceBuffer(&self, @sizeOf(i32));
    const token = try createI32ScalarTensorFromLease(&self, &successful_lease);
    const token_tensor = tensorFromCt(token);
    try std.testing.expect(!token_tensor.owns_buffer);
    try std.testing.expectEqual(@as(driver_mod.CUdeviceptr, 0x5678), token_tensor.buffer.ptr);
    try std.testing.expectEqual(CudaGraphReplayOutputKind.tensor, self.debug_cuda_graph_prepared_output_kind);
    try std.testing.expect((try preparedGreedyTokenOutputBuffer(&self, @sizeOf(i32))) == null);

    freeTensor(&self, token);
    try std.testing.expectEqual(@as(driver_mod.CUdeviceptr, 0x5678), self.debug_cuda_graph_slots[1].output_storage.ptr);
}

fn allocDeviceBuffer(self: *CudaCompute, len: usize) !buffer_mod.DeviceBuffer {
    if (len == 0) return .{};
    while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
    defer self.temp_buffer_mutex.unlock();
    const seq = self.temp_trace_seq;
    self.temp_trace_seq = seq + 1;
    if (self.temp_slot_config.autoplan_enabled) {
        self.temp_arena_planner.noteAllocation(self.allocator, len, max_temp_pinned_slots) catch {
            self.temp_arena_planner.disabled = true;
            self.temp_arena_planner.active = false;
            self.temp_arena_planner.ready_for_capture = false;
        };
    }
    if (try allocPinnedTempSlot(self, seq, len)) |buffer| return buffer;
    if (self.debug_cuda_graph_capture_active and cudaEffectiveTempSlotPeriod(self) != 0) {
        // Falling through to the ordinary cache would let a graph retain an
        // address that can be reused immediately after capture. Captures with
        // a pinned arena therefore fail closed on any slot miss or ABI drift.
        if (self.temp_slot_config.autoplan_enabled) self.temp_arena_planner.markPinnedValidationFailure();
        self.debug_cuda_graph_capture_disabled = true;
        std.log.warn("cuda_graph_capture_probe: unpinned_temp_during_capture seq={d} bytes={d} disabled=1", .{ seq, len });
        return error.CudaGraphCaptureUnsafeTempAlloc;
    }
    const stable_reuse = cudaTempStableReuseEnabled();
    var best_index: ?usize = null;
    var best_len: usize = std.math.maxInt(usize);
    for (self.temp_buffers.items, 0..) |buffer, i| {
        if (stable_reuse) {
            if (buffer.len == len) {
                best_index = i;
                break;
            }
        } else if (buffer.len >= len and buffer.len < best_len) {
            best_index = i;
            best_len = buffer.len;
        }
    }
    if (best_index) |i| {
        const buffer = if (stable_reuse)
            self.temp_buffers.orderedRemove(i)
        else
            self.temp_buffers.swapRemove(i);
        self.stats.temp_buffer_hits += 1;
        traceCudaTempAlloc(self, seq, "alloc_hit", len, buffer);
        return buffer;
    }
    if (self.debug_cuda_graph_capture_active) {
        self.debug_cuda_graph_capture_disabled = true;
        std.log.warn("cuda_graph_capture_probe: unsafe_temp_alloc seq={d} bytes={d} disabled=1", .{ seq, len });
        return error.CudaGraphCaptureUnsafeTempAlloc;
    }
    self.stats.temp_buffer_misses += 1;
    const buffer = buffer_mod.DeviceBuffer.alloc(&self.ctx, len) catch |err| {
        if (self.deferred_device_frees.items.len != 0) {
            try forceDrainDeferredDeviceFreesUnlocked(self);
            const retry = try buffer_mod.DeviceBuffer.alloc(&self.ctx, len);
            self.noteDeviceBytes(len);
            self.stats.device_alloc_calls += 1;
            traceCudaTempAlloc(self, seq, "alloc_miss_retry", len, retry);
            return retry;
        }
        return err;
    };
    self.noteDeviceBytes(len);
    self.stats.device_alloc_calls += 1;
    traceCudaTempAlloc(self, seq, "alloc_miss", len, buffer);
    return buffer;
}

fn releaseDeviceBuffer(self: *CudaCompute, buffer: *buffer_mod.DeviceBuffer) void {
    if (buffer.ptr == 0) return;
    while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
    defer self.temp_buffer_mutex.unlock();
    if (releasePinnedTempSlot(self, buffer)) return;
    const cache_budget = cudaTempCacheBudgetBytes();
    var cached_bytes: usize = 0;
    for (self.temp_buffers.items) |cached| cached_bytes += cached.len;
    if (self.temp_buffers.items.len < cudaTempCacheMaxBuffers() and buffer.len <= cache_budget and cached_bytes + buffer.len <= cache_budget) {
        self.temp_buffers.append(self.allocator, buffer.*) catch {
            self.stats.temp_buffer_evictions += 1;
            self.stats.device_free_calls += 1;
            buffer.free(&self.ctx);
            return;
        };
        self.stats.temp_buffer_releases += 1;
        buffer.* = .{};
        return;
    }
    evictTempBufferUnlocked(self, buffer);
}

fn evictTempBufferUnlocked(self: *CudaCompute, buffer: *buffer_mod.DeviceBuffer) void {
    if (buffer.ptr == 0) return;
    self.stats.temp_buffer_evictions += 1;
    if (cudaDeferredFreeEnabled()) {
        self.deferred_device_frees.append(self.allocator, buffer.*) catch {
            self.stats.device_free_calls += 1;
            synchronizeAndDrainDeferredDeviceFreesUnlocked(self) catch {};
            buffer.free(&self.ctx);
            return;
        };
        self.deferred_device_free_bytes += buffer.len;
        self.stats.deferred_free_queued += 1;
        buffer.* = .{};
        if (self.deferred_device_free_bytes >= cudaDeferredFreeBudgetBytes()) {
            forceDrainDeferredDeviceFreesUnlocked(self) catch {};
        }
        return;
    }
    self.stats.device_free_calls += 1;
    synchronizeAndDrainDeferredDeviceFreesUnlocked(self) catch {};
    buffer.free(&self.ctx);
}

fn freeTensor(ctx: *anyopaque, tensor: CT) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    if (!cuda_tensor.owned_by_tensor) return;
    freeCudaTensorStorage(self, cuda_tensor);
    self.allocator.destroy(cuda_tensor);
}

fn borrowedSlotTensor(tensor: *const CudaTensor) CudaTensor {
    var borrowed = tensor.*;
    borrowed.owns_buffer = false;
    borrowed.owns_bf16_mirror = false;
    borrowed.owns_training_upload_host = false;
    borrowed.owns_shape = false;
    borrowed.owned_by_tensor = false;
    return borrowed;
}

fn residentTensorForSlot(self: *CudaCompute, tensor: *const CudaTensor) ?*CudaTensor {
    var it = self.resident_weights.iterator();
    while (it.next()) |entry| {
        const resident = entry.value_ptr;
        if (resident.buffer.ptr == tensor.buffer.ptr and
            resident.buffer.len == tensor.buffer.len and
            resident.elem_count == tensor.elem_count)
        {
            return resident;
        }
    }
    return null;
}

fn decoderRuntimeSlotPinsBuffer(self: *const CudaCompute, buffer: buffer_mod.DeviceBuffer) bool {
    if (buffer.ptr == 0) return false;
    var linear_it = self.decoder_runtime_linear_slots.iterator();
    while (linear_it.next()) |entry| {
        const slot = entry.value_ptr;
        if (slot.weight.buffer.ptr == buffer.ptr) return true;
        if (slot.bias) |bias| {
            if (bias.buffer.ptr == buffer.ptr) return true;
        }
    }
    var norm_it = self.decoder_runtime_rms_norm_slots.iterator();
    while (norm_it.next()) |entry| {
        if (entry.value_ptr.weight.buffer.ptr == buffer.ptr) return true;
    }
    return false;
}

fn cudaTensorDeviceBytes(tensor: *const CudaTensor) usize {
    const mirror_bytes = tensor.bf16_mirror.len;
    if (tensor.quant_type) |quant_type| {
        const block_size = gguf_tensor_types.bytesPerBlock(quant_type) orelse return tensor.buffer.len + mirror_bytes;
        const values_per_block = gguf_tensor_types.valuesPerBlock(quant_type) orelse return tensor.buffer.len + mirror_bytes;
        return ((tensor.elem_count + values_per_block - 1) / values_per_block) * block_size + mirror_bytes;
    }
    return tensor.elem_count * tensor.dtype.byteSize() + mirror_bytes;
}

fn noteLazyDeviceAccess(self: *CudaCompute, name: []const u8, bytes: usize) !void {
    self.lazy_access_epoch +|= 1;
    if (self.lazy_device_epochs.getPtr(name)) |epoch| {
        epoch.* = self.lazy_access_epoch;
        return;
    }
    if (bytes == 0) return;
    const owned_key = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(owned_key);
    try self.lazy_device_epochs.put(self.allocator, owned_key, self.lazy_access_epoch);
    self.lazy_device_bytes += bytes;
}

fn evictLazyDeviceWeightsToBudget(self: *CudaCompute, protected_name: []const u8) !void {
    if (self.lazy_device_budget_bytes == 0) return;
    const trace = cudaLazyTraceEnabled();
    var synchronized = false;
    while (self.lazy_device_bytes > self.lazy_device_budget_bytes) {
        var oldest_name: ?[]const u8 = null;
        var oldest_epoch: u64 = std.math.maxInt(u64);
        var it = self.lazy_device_epochs.iterator();
        while (it.next()) |entry| {
            const name = entry.key_ptr.*;
            if (std.mem.eql(u8, name, protected_name)) continue;
            if (self.resident_weights.getPtr(name)) |tensor| {
                if (decoderRuntimeSlotPinsBuffer(self, tensor.buffer)) {
                    self.stats.decoder_runtime_pinned_eviction_skips += 1;
                    continue;
                }
            }
            if (entry.value_ptr.* < oldest_epoch) {
                oldest_epoch = entry.value_ptr.*;
                oldest_name = name;
            }
        }
        const victim_name = oldest_name orelse break;
        if (trace) {
            std.log.info("cuda_lazy_trace: evict_start victim={s} protected={s} lazy_device_mb={d} budget_mb={d}", .{
                victim_name,
                protected_name,
                self.lazy_device_bytes / (1024 * 1024),
                self.lazy_device_budget_bytes / (1024 * 1024),
            });
        }
        if (!synchronized) {
            try synchronizeAndDrainDeferredDeviceFrees(self);
            synchronized = true;
        }
        if (self.resident_weights.fetchRemove(victim_name)) |removed| {
            var tensor = removed.value;
            tensor.owns_buffer = true;
            tensor.owns_shape = true;
            tensor.owns_bf16_mirror = true;
            const bytes = cudaTensorDeviceBytes(&tensor);
            freeCudaTensorStorageUncached(self, &tensor);
            self.lazy_device_bytes -|= bytes;
            self.stats.resident_weight_bytes -|= bytes;
            if (trace) {
                std.log.info("cuda_lazy_trace: evicted victim={s} evicted_mb={d} lazy_device_mb={d}", .{
                    victim_name,
                    bytes / (1024 * 1024),
                    self.lazy_device_bytes / (1024 * 1024),
                });
            }
            self.allocator.free(removed.key);
        }
        if (self.lazy_device_epochs.fetchRemove(victim_name)) |removed_epoch| {
            self.allocator.free(removed_epoch.key);
        }
    }
}

fn cancelPendingLazyHostPrefetchLocked(store: *native_compute_mod.WeightStore, entry: *native_compute_mod.LazyWeightEntry) bool {
    if (!entry.pending_prefetch) return false;
    for (store.prefetch.items.items, 0..) |queued, index| {
        if (queued == entry) {
            _ = store.prefetch.items.orderedRemove(index);
            entry.pending_prefetch = false;
            return true;
        }
    }
    return false;
}

fn materializeLazyWeight(self: *CudaCompute, name: []const u8) !*CudaTensor {
    const store = self.lazy_host_store orelse return error.MissingWeight;
    const trace = cudaLazyTraceEnabled();
    const demand_start = platform.time.monotonicNs();
    if (trace) {
        std.log.info("cuda_lazy_trace: demand_start name={s} resident_mb={d} lazy_device_mb={d} budget_mb={d}", .{
            name,
            self.stats.resident_weight_bytes / (1024 * 1024),
            self.lazy_device_bytes / (1024 * 1024),
            self.lazy_device_budget_bytes / (1024 * 1024),
        });
    }
    store.prefetch.lock();
    var store_locked = true;
    errdefer if (store_locked) store.prefetch.unlock();
    const loaded = blk: {
        const entry = store.lazy_weights.getPtr(name) orelse {
            store_locked = false;
            store.prefetch.unlock();
            return error.MissingWeight;
        };
        self.stats.lazy_demand_loads += 1;
        if (entry.loaded != null) {
            self.stats.lazy_host_prefetch_hits += 1;
            if (trace) {
                std.log.info("cuda_lazy_trace: host_hit name={s} loaded_mb={d}", .{ name, entry.loaded_bytes / (1024 * 1024) });
            }
        } else {
            if (cancelPendingLazyHostPrefetchLocked(store, entry)) {
                self.stats.lazy_prefetch_cancelled_for_demand += 1;
                if (trace) std.log.info("cuda_lazy_trace: cancelled_prefetch name={s}", .{name});
            }
            const host_start = platform.time.monotonicNs();
            try native_compute_mod.ensureLazyWeightLoadedLocked(store, self.run_budget, entry);
            const host_ns = elapsedNsSince(host_start);
            self.stats.lazy_host_load_ns +|= host_ns;
            if (trace) {
                std.log.info("cuda_lazy_trace: host_loaded name={s} loaded_mb={d} host_ms={d}", .{
                    name,
                    entry.loaded_bytes / (1024 * 1024),
                    host_ns / 1_000_000,
                });
            }
        }
        const moved_loaded = entry.loaded.?;
        entry.loaded = null;
        entry.active_tier = entry.placement.spill_tier;
        if (store.tier_cache) |*tier_cache| {
            tier_cache.noteRelease(.host, entry.loaded_bytes);
        }
        store_locked = false;
        store.prefetch.unlock();
        break :blk moved_loaded;
    };
    var owned_loaded = loaded;
    defer owned_loaded.deinit();
    const page_touch_start = platform.time.monotonicNs();
    touchLoadedWeightPages(&owned_loaded);
    const page_touch_ns = elapsedNsSince(page_touch_start);
    self.stats.lazy_host_page_touch_ns +|= page_touch_ns;
    if (trace and page_touch_ns > 0) {
        std.log.info("cuda_lazy_trace: page_touched name={s} touch_ms={d}", .{
            name,
            page_touch_ns / 1_000_000,
        });
    }
    const owned_key = try self.allocator.dupe(u8, name);
    const upload_start = platform.time.monotonicNs();
    self.insertWeightFromLoaded(owned_key, &owned_loaded) catch |err| {
        self.allocator.free(owned_key);
        return err;
    };
    const tensor = self.resident_weights.getPtr(name) orelse return error.MissingWeight;
    const uploaded_bytes = cudaTensorDeviceBytes(tensor);
    const upload_ns = elapsedNsSince(upload_start);
    self.stats.lazy_upload_ns +|= upload_ns;
    self.stats.lazy_uploaded_bytes += uploaded_bytes;
    if (trace) {
        std.log.info("cuda_lazy_trace: uploaded name={s} dtype={s} uploaded_mb={d} upload_ms={d}", .{
            name,
            @tagName(tensor.dtype),
            uploaded_bytes / (1024 * 1024),
            upload_ns / 1_000_000,
        });
    }
    try noteLazyDeviceAccess(self, name, uploaded_bytes);
    try evictLazyDeviceWeightsToBudget(self, name);
    if (trace) {
        std.log.info("cuda_lazy_trace: demand_done name={s} total_ms={d} lazy_device_mb={d} resident_mb={d}", .{
            name,
            elapsedNsSince(demand_start) / 1_000_000,
            self.lazy_device_bytes / (1024 * 1024),
            self.stats.resident_weight_bytes / (1024 * 1024),
        });
    }
    return self.resident_weights.getPtr(name) orelse error.MissingWeight;
}

fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.resident_weights.getPtr(name)) |tensor| {
        if (self.lazy_device_epochs.contains(name)) {
            try noteLazyDeviceAccess(self, name, 0);
        }
        return tensor;
    }
    if (getDenseStreamWeight(self, name)) |maybe_tensor| {
        if (maybe_tensor) |tensor| return tensor;
    } else |err| {
        if (isDenseFfnStreamWeightName(name)) self.stats.ffn_stream_fallbacks += 1;
        self.stats.dense_stream_fallbacks += 1;
        if (cudaDenseStreamProfileEnabled()) {
            std.log.warn("cuda_dense_stream: fallback name={s} err={s}", .{ name, @errorName(err) });
        }
    }
    return materializeLazyWeight(self, name) catch |err| switch (err) {
        error.MissingWeight => error.WeightNotFound,
        else => err,
    };
}

fn prefetchWeightHint(ctx: *anyopaque, name: []const u8, hint: u32) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.resident_weights.contains(name)) return;
    if (cudaDenseStreamEnabled(self) and isDenseStreamWeightName(self, name)) {
        const is_mlp = isDenseFfnStreamWeightName(name);
        if (findDenseStreamEntry(self, name)) |entry| {
            self.dense_stream_epoch +|= 1;
            entry.last_access = self.dense_stream_epoch;
            if (is_mlp) self.stats.ffn_stream_hits += 1;
            self.stats.dense_stream_hits += 1;
            return;
        }
        if (cudaDensePrefetchEnabled(self) and self.dense_host_prefetch_initialized) {
            enqueueDenseHostPrefetchHint(self, name, hint);
            return;
        }
        if (self.lazy_host_store) |store| {
            if (store.tensor_store) |tensor_store| {
                if (tensor_store.describeTensorRange(self.allocator, name)) |maybe_range| {
                    if (maybe_range) |range_value| {
                        var range = range_value;
                        if (is_mlp) self.stats.ffn_stream_fadvise_calls += 1;
                        self.stats.dense_stream_fadvise_calls += 1;
                        c_file.adviseFileRange(self.allocator, range.path, range.byte_offset, range.byte_len, .will_need);
                        range.deinit(self.allocator);
                    }
                } else |_| {}
            }
        }
        return;
    }
    const store = self.lazy_host_store orelse return;
    if (!store.prefetch_initialized) return;
    store.prefetch.lock();
    defer store.prefetch.unlock();
    const entry = store.lazy_weights.getPtr(name) orelse {
        self.stats.lazy_prefetch_missing += 1;
        return;
    };
    if (store.shared_prefetch) |shared_prefetch| {
        entry.prefetch_score = shared_prefetch.noteRequest(name, hint) catch entry.prefetch_score;
    } else {
        entry.prefetch_score +|= hint;
    }
    if (entry.pending_prefetch) {
        self.stats.lazy_prefetch_duplicates += 1;
        return;
    }
    store.prefetch.appendLocked(entry) catch return;
    entry.pending_prefetch = true;
    self.stats.lazy_prefetch_enqueues += 1;
    store.prefetch.signal();
}

fn drainPrefetchBudget(ctx: *anyopaque, max_items: usize) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const store = self.lazy_host_store orelse return;
    if (!store.prefetch_initialized) return;
    self.stats.lazy_prefetch_drain_calls += 1;
    store.prefetch.drainBudget(max_items);
}

fn debugProfileCheckpoint(ctx: *anyopaque, label: []const u8, layer: usize) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_PREFILL_PROFILE")) return;
    const stats = self.stats;
    std.debug.print(
        "cuda_prefill_profile: label={s} layer={d} lazy_demand={d} lazy_hits={d} lazy_host_ms={d} lazy_touch_ms={d} lazy_upload_ms={d} lazy_uploaded_mb={d} dense_req={d} dense_hits={d} dense_misses={d} dense_fallbacks={d} dense_read_ms={d} dense_h2d_ms={d} dense_read_mb={d} dense_uploaded_mb={d} dense_resident_mb={d} dense_prefetch_enq={d} dense_prefetch_ready={d} dense_prefetch_steals={d} dense_prefetch_sync_reads={d} dense_prefetch_wait_ms={d} dense_prefetch_host_read_ms={d} dense_prefetch_upload_ms={d} syncs={d} upload_syncs={d} alloc_calls={d} free_calls={d}\n",
        .{
            label,
            layer,
            stats.lazy_demand_loads,
            stats.lazy_host_prefetch_hits,
            stats.lazy_host_load_ns / 1_000_000,
            stats.lazy_host_page_touch_ns / 1_000_000,
            stats.lazy_upload_ns / 1_000_000,
            stats.lazy_uploaded_bytes / (1024 * 1024),
            stats.dense_stream_requests,
            stats.dense_stream_hits,
            stats.dense_stream_misses,
            stats.dense_stream_fallbacks,
            stats.dense_stream_read_ns / 1_000_000,
            stats.dense_stream_h2d_ns / 1_000_000,
            stats.dense_stream_read_bytes / (1024 * 1024),
            stats.dense_stream_uploaded_bytes / (1024 * 1024),
            stats.dense_stream_resident_bytes / (1024 * 1024),
            stats.dense_prefetch_enqueues,
            stats.dense_prefetch_ready_hits,
            stats.dense_prefetch_inflight_steals,
            stats.dense_prefetch_sync_reads,
            stats.dense_prefetch_demand_wait_ns / 1_000_000,
            stats.dense_prefetch_host_read_ns / 1_000_000,
            stats.dense_prefetch_upload_ns / 1_000_000,
            stats.stream_syncs,
            stats.upload_syncs,
            stats.device_alloc_calls,
            stats.device_free_calls,
        },
    );
}

fn debugCudaGraphCaptureBegin(ctx: *anyopaque, label: []const u8) !bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;
    if (self.debug_cuda_graph_capture_inhibit_once) {
        self.debug_cuda_graph_capture_inhibit_once = false;
        return false;
    }
    if (self.debug_cuda_graph_capture_disabled) {
        if (cudaDecodeGraphReplayMode() == .required) return error.CudaGraphReplayRequired;
        return false;
    }
    if (cudaEffectiveTempSlotPeriod(self) == 0 and !cudaDebugGraphCaptureAllowUnpinned()) {
        if (self.temp_slot_config.autoplan_enabled and !self.temp_arena_planner.disabled) {
            if (self.temp_arena_planner.captureWarmupExhausted() and cudaDecodeGraphReplayMode() == .required) {
                return error.CudaGraphReplayRequired;
            }
            return false;
        }
        if (cudaDecodeGraphReplayMode() == .required) return error.CudaGraphReplayRequired;
        return false;
    }
    if (self.temp_slot_config.autoplan_enabled and !self.temp_arena_planner.ready_for_capture) {
        if (self.temp_arena_planner.captureWarmupExhausted() and cudaDecodeGraphReplayMode() == .required) {
            return error.CudaGraphReplayRequired;
        }
        return false;
    }
    const min_alloc_seq = cudaDebugGraphCaptureMinAllocSeq(self);
    // Pinned temporary slots must first observe one complete allocation period.
    // Required mode permits this warmup because the planner bounds it: once
    // its per-request observation budget is exhausted without a stable trace,
    // the autoplan branches above fail closed with CudaGraphReplayRequired.
    if (self.temp_trace_seq < min_alloc_seq) return false;
    if (self.debug_cuda_decode_scalars_host_valid and self.debug_cuda_decode_scalars_host[4] != 0) {
        if (cudaDecodeGraphReplayMode() == .required) return error.CudaGraphReplayRequired;
        return false;
    }
    const active_slot_idx = self.debug_cuda_graph_prepared_slot orelse 0;
    const auto_decode_scalars_delta = if (self.debug_cuda_decode_scalars_host_valid)
        decodeScalarsAutoAdvanceDeltaForScalars(self.debug_cuda_decode_scalars_host)
    else
        [_]u32{ 1, 1, 1, 1, 0 };
    const use_auto_decode_scalars =
        cudaDebugGraphAutoAdvanceDecodeScalarsEnabled() and
        cudaDebugGraphPersistentReplayEnabled(self) and
        !self.debug_cuda_decode_scalars_auto_advance_blocked and
        self.kernels.supportsDecodeScalarsAdvance() and
        self.debug_cuda_decode_scalars.ptr != 0 and
        self.debug_cuda_decode_scalars_host_valid and
        self.debug_cuda_decode_scalars_host[0] != 0 and
        self.debug_cuda_decode_scalars_host[1] != 0 and
        self.debug_cuda_decode_scalars_host[2] != 0 and
        self.debug_cuda_decode_scalars_host[3] != 0 and
        decodeScalarsCanPreAdvance(self.debug_cuda_decode_scalars_host, auto_decode_scalars_delta);
    var decode_scalars_pre_advanced = false;
    var capture_started = false;
    errdefer {
        if (capture_started) {
            abortDebugCudaGraphCapture(self, active_slot_idx);
        } else if (decode_scalars_pre_advanced) {
            restoreDebugCudaGraphDecodeScalars(self, active_slot_idx);
        }
    }
    if (use_auto_decode_scalars) {
        var pre_advance_scalars = self.debug_cuda_decode_scalars_host;
        inline for (0..5) |idx| {
            pre_advance_scalars[idx] -= auto_decode_scalars_delta[idx];
        }
        try uploadDecodeScalars(self, pre_advance_scalars);
        decode_scalars_pre_advanced = true;
    }
    try self.ctx.beginStreamCapture(driver_mod.CU_STREAM_CAPTURE_MODE_RELAXED);
    capture_started = true;
    self.debug_cuda_graph_capture_active = true;
    self.debug_cuda_graph_capture_temp_seq_begin = self.temp_trace_seq;
    self.debug_cuda_graph_active_slot = active_slot_idx;
    self.debug_cuda_graph_slots[active_slot_idx].decode_scalars_auto_advance = false;
    self.debug_cuda_graph_slots[active_slot_idx].decode_scalars_auto_advance_delta = auto_decode_scalars_delta;
    self.debug_cuda_graph_slots[active_slot_idx].aux_input_valid = false;
    self.debug_cuda_graph_slots[active_slot_idx].aux_input_required = false;
    if (cudaDebugGraphPersistentReplayEnabled(self)) {
        const slot = &self.debug_cuda_graph_slots[self.debug_cuda_graph_active_slot.?];
        slot.kv_replay_capacity_tokens = 0;
        slot.kv_replay_capacity_valid = false;
    }
    if (use_auto_decode_scalars) {
        try self.kernels.launchDecodeScalarsAdvance(
            &self.ctx,
            self.debug_cuda_decode_scalars,
            auto_decode_scalars_delta[0],
            auto_decode_scalars_delta[1],
            auto_decode_scalars_delta[2],
            auto_decode_scalars_delta[3],
            auto_decode_scalars_delta[4],
        );
        self.debug_cuda_graph_slots[active_slot_idx].decode_scalars_auto_advance = true;
    }
    self.stats.cuda_graph_capture_begins += 1;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: begin label={s} slot={d} alloc_seq={d} min_alloc_seq={d} auto_decode_scalars={d}", .{
            label,
            self.debug_cuda_graph_active_slot.?,
            self.temp_trace_seq,
            min_alloc_seq,
            @intFromBool(use_auto_decode_scalars),
        });
    }
    capture_started = false;
    return true;
}

fn debugCudaGraphPrepareDecodeScalars(
    ctx: *anyopaque,
    position_offset: usize,
    query_position_offset: usize,
    kv_seq_len: usize,
    total_sequence_len: usize,
    kv_position_offset: usize,
) !bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphCaptureDeviceScalarsEnabled(self)) return false;
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;
    if (self.kernels.gqaSplitkOnlineDecodeProfileSelected()) {
        const splitk_profile_name = if (self.kernels.gqaSplitkOnlineDecodeProfileFailClosed())
            "required-splitk-online-sm89"
        else
            "splitk-online-sm89";
        const replay_capacity = cudaDebugGraphForcedKvReplayCapacityTokens() orelse {
            std.log.err(
                "cuda_gqa_decode_profile: status=rejected profile={s} reason=missing-forced-replay-capacity",
                .{splitk_profile_name},
            );
            disarmSplitkOnlineDecodeReplay(self);
            if (self.kernels.gqaSplitkOnlineDecodeProfileFailClosed()) return error.InvalidCudaState;
            return false;
        };
        if (!kernels_mod.gqaSplitkOnlineDecodeReplayScalarsEligible(
            query_position_offset,
            kv_seq_len,
            total_sequence_len,
            kv_position_offset,
            replay_capacity,
        )) {
            std.log.err(
                "cuda_gqa_decode_profile: status=rejected profile={s} reason=replay-scalars-outside-qualified-capacity query_position_offset={d} kv_seq_len={d} total_sequence_len={d} kv_position_offset={d} capacity={d}",
                .{ splitk_profile_name, query_position_offset, kv_seq_len, total_sequence_len, kv_position_offset, replay_capacity },
            );
            disarmSplitkOnlineDecodeReplay(self);
            if (self.kernels.gqaSplitkOnlineDecodeProfileFailClosed()) return error.InvalidCudaState;
            return false;
        }
    }
    self.debug_cuda_graph_decode_kv_seq_len = kv_seq_len;
    if (self.debug_cuda_decode_scalars.ptr == 0) {
        self.debug_cuda_decode_scalars = try buffer_mod.DeviceBuffer.alloc(&self.ctx, 5 * @sizeOf(u32));
    }
    const scalars = [_]u32{
        std.math.cast(u32, position_offset) orelse return error.InvalidCudaState,
        std.math.cast(u32, query_position_offset) orelse return error.InvalidCudaState,
        std.math.cast(u32, kv_seq_len) orelse return error.InvalidCudaState,
        std.math.cast(u32, total_sequence_len) orelse return error.InvalidCudaState,
        std.math.cast(u32, kv_position_offset) orelse return error.InvalidCudaState,
    };
    self.debug_cuda_decode_scalars_host = scalars;
    self.debug_cuda_decode_scalars_host_valid = true;
    if (decodeScalarsAutoAdvanceReplaySlot(self)) |slot_idx| {
        const expected_before_reset = decodeScalarsAutoAdvanceExpected(self, slot_idx) orelse [_]u32{ 0, 0, 0, 0, 0 };
        if (decodeScalarsAutoAdvanceExpected(self, slot_idx)) |expected| {
            if (decodeScalarsEqual(expected, scalars)) {
                self.debug_cuda_decode_scalars_upload_deferred = true;
                return true;
            }
        }
        const slot = &self.debug_cuda_graph_slots[slot_idx];
        // Scalar jump (e.g. a sliding-window block trim shifted the KV view):
        // resync by uploading the pre-advance values so the captured graph's
        // advance node lands exactly on this token's scalars. Keeps the exec
        // and auto-advance replay alive; costs one host upload per jump.
        if (decodeScalarsCanPreAdvance(scalars, slot.decode_scalars_auto_advance_delta)) {
            var pre_advance = scalars;
            inline for (0..5) |idx| pre_advance[idx] -= slot.decode_scalars_auto_advance_delta[idx];
            try uploadDecodeScalars(self, pre_advance);
            self.debug_cuda_decode_scalars_host = scalars;
            self.debug_cuda_decode_scalars_host_valid = true;
            self.debug_cuda_decode_scalars_upload_deferred = true;
            if (cudaDebugGraphCaptureProbeTraceEnabled()) {
                std.log.info("cuda_graph_capture_probe: auto_decode_scalars_resync slot={d} expected={any} actual={any}", .{
                    slot_idx,
                    expected_before_reset,
                    scalars,
                });
            }
            return true;
        }
        const mode_transition_supported = if (self.debug_cuda_decode_scalars_device_valid)
            if (decodeScalarsAutoAdvanceDeltaBetween(self.debug_cuda_decode_scalars_device, scalars)) |delta|
                decodeScalarsAutoAdvanceDeltaSupported(delta)
            else
                false
        else
            false;
        if (!mode_transition_supported) {
            self.debug_cuda_decode_scalars_auto_advance_blocked = true;
            resetCudaGraphReplaySlotMetadata(self, slot);
        } else {
            resetCudaGraphReplaySlotMetadataKeepExec(self, slot);
        }
        if (cudaDebugGraphCaptureProbeTraceEnabled()) {
            std.log.info("cuda_graph_capture_probe: auto_decode_scalars_mismatch slot={d} expected={any} actual={any} mode_transition_supported={d} fallback=scalar_upload", .{
                slot_idx,
                expected_before_reset,
                scalars,
                @intFromBool(mode_transition_supported),
            });
        }
    }
    try uploadDecodeScalars(self, scalars);
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: decode_scalars position_offset={d} query_position_offset={d} kv_seq_len={d} total_sequence_len={d} kv_position_offset={d} ptr=0x{x}", .{
            scalars[0],
            scalars[1],
            scalars[2],
            scalars[3],
            scalars[4],
            self.debug_cuda_decode_scalars.ptr,
        });
    }
    return true;
}

fn debugCudaTraceTensor(ctx: *anyopaque, label: []const u8, tensor: CT) !void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!self.ctx.debug_graph_capture_active) return;
    if (!cudaDebugGraphCaptureTensorTraceEnabled()) return;
    const limit = cudaDebugGraphCaptureTensorTraceLimit();
    if (self.ctx.debug_graph_capture_tensor_trace_count >= limit) return;
    const index = self.ctx.debug_graph_capture_tensor_trace_count;
    self.ctx.debug_graph_capture_tensor_trace_count += 1;

    const cuda_tensor = tensorFromCt(tensor);
    std.log.info("cuda_capture_tensor_trace: capture={d} index={d} label={s} tensor=0x{x} buffer=0x{x} buffer_len={d} logical_bytes={d} dtype={s} elem_count={d} shape={any} owns_buffer={d} owns_shape={d} owned_by_tensor={d}", .{
        self.ctx.debug_graph_capture_id,
        index,
        label,
        @intFromPtr(cuda_tensor),
        cuda_tensor.buffer.ptr,
        cuda_tensor.buffer.len,
        cudaTensorDeviceBytes(cuda_tensor),
        @tagName(cuda_tensor.dtype),
        cuda_tensor.elem_count,
        cuda_tensor.shape,
        @intFromBool(cuda_tensor.owns_buffer),
        @intFromBool(cuda_tensor.owns_shape),
        @intFromBool(cuda_tensor.owned_by_tensor),
    });
}

fn resetCudaGraphReplaySlotMetadata(self: *CudaCompute, slot: *CudaGraphReplaySlot) void {
    if (slot.exec) |exec| {
        self.ctx.destroyGraphExec(exec);
        slot.exec = null;
    }
    resetCudaGraphReplaySlotMetadataKeepExec(self, slot);
    slot.temp_arena_generation = 0;
    slot.captured_temp_allocation_count = 0;
}

fn resetCudaGraphReplaySlotMetadataKeepExec(self: *CudaCompute, slot: *CudaGraphReplaySlot) void {
    if (slot.shape) |shape| {
        self.allocator.free(shape);
        slot.shape = null;
    }
    slot.input = .{};
    slot.aux_input = .{};
    slot.output = .{};
    slot.elem_count = 0;
    slot.dtype = .f32;
    slot.input_valid = false;
    slot.aux_input_valid = false;
    slot.aux_input_required = false;
    slot.aux_input_prepared = false;
    slot.valid = false;
    slot.kv_replay_capacity_tokens = 0;
    slot.kv_replay_capacity_valid = false;
    slot.decode_scalars_auto_advance = false;
    slot.decode_scalars_auto_advance_delta = .{ 1, 1, 1, 1, 0 };
}

fn deinitCudaGraphReplaySlot(self: *CudaCompute, slot: *CudaGraphReplaySlot) void {
    resetCudaGraphReplaySlotMetadata(self, slot);
    slot.input_storage.free(&self.ctx);
    slot.aux_input_storage.free(&self.ctx);
    slot.output_storage.free(&self.ctx);
    slot.input_storage = .{};
    slot.aux_input_storage = .{};
    slot.output_storage = .{};
}

fn invalidateDebugFinalHiddenGraph(self: *CudaCompute) void {
    for (&self.debug_cuda_graph_slots) |*slot| {
        resetCudaGraphReplaySlotMetadata(self, slot);
    }
    self.debug_cuda_graph_active_slot = null;
    self.debug_cuda_graph_prepared_slot = null;
    self.debug_cuda_graph_prepared_output_kind = .tensor;
}

fn disarmSplitkOnlineDecodeReplay(self: *CudaCompute) void {
    // Optional candidate rejection must return to eager execution, not leave a
    // prepared exec that callers can replay with the previous token's device
    // scalars. Destroy all request-local graph slots and make scalar readiness
    // false before returning from the preparation hook.
    invalidateDebugFinalHiddenGraph(self);
    self.debug_cuda_graph_capture_disabled = true;
    self.debug_cuda_decode_scalars_host_valid = false;
    self.debug_cuda_decode_scalars_device_valid = false;
    self.debug_cuda_decode_scalars_upload_deferred = false;
    self.debug_cuda_decode_scalars_auto_advance_blocked = false;
    self.debug_cuda_graph_decode_kv_seq_len = 0;
    self.debug_cuda_graph_capture_inhibit_once = true;
}

test "split-K replay rejection cannot leave a prepared graph or stale scalars" {
    var self: CudaCompute = undefined;
    self.allocator = std.testing.allocator;
    self.debug_cuda_graph_slots = [_]CudaGraphReplaySlot{.{}} ** max_cuda_graph_replay_slots;
    self.debug_cuda_graph_slots[1].valid = true;
    self.debug_cuda_graph_slots[1].kv_replay_capacity_valid = true;
    self.debug_cuda_graph_slots[1].kv_replay_capacity_tokens = 2432;
    self.debug_cuda_graph_active_slot = 1;
    self.debug_cuda_graph_prepared_slot = 1;
    self.debug_cuda_decode_scalars_host_valid = true;
    self.debug_cuda_decode_scalars_device_valid = true;
    self.debug_cuda_decode_scalars_upload_deferred = true;
    self.debug_cuda_decode_scalars_auto_advance_blocked = false;
    self.debug_cuda_graph_decode_kv_seq_len = 2433;
    self.debug_cuda_graph_capture_inhibit_once = false;
    self.debug_cuda_graph_capture_disabled = false;

    disarmSplitkOnlineDecodeReplay(&self);

    try std.testing.expectEqual(@as(?usize, null), self.debug_cuda_graph_active_slot);
    try std.testing.expectEqual(@as(?usize, null), self.debug_cuda_graph_prepared_slot);
    try std.testing.expect(!self.debug_cuda_graph_slots[1].valid);
    try std.testing.expect(self.debug_cuda_graph_slots[1].exec == null);
    try std.testing.expect(!self.debug_cuda_decode_scalars_host_valid);
    try std.testing.expect(!self.debug_cuda_decode_scalars_device_valid);
    try std.testing.expect(!self.debug_cuda_decode_scalars_upload_deferred);
    try std.testing.expect(!self.debug_cuda_decode_scalars_auto_advance_blocked);
    try std.testing.expectEqual(@as(usize, 0), self.debug_cuda_graph_decode_kv_seq_len);
    try std.testing.expect(self.debug_cuda_graph_capture_inhibit_once);
    try std.testing.expect(self.debug_cuda_graph_capture_disabled);

    // The immediate capture attempt is consumed as an eager-only operation;
    // the request-scoped disabled latch prevents any later stale recapture.
    self.debug_cuda_graph_capture_active = false;
    try std.testing.expect(!(try debugCudaGraphCaptureBegin(&self, "splitk-rejected")));
    try std.testing.expect(!self.debug_cuda_graph_capture_inhibit_once);
    try std.testing.expect(self.debug_cuda_graph_capture_disabled);
}

fn preparePersistentCudaBufferReallocation(self: *CudaCompute) !void {
    if (self.debug_cuda_graph_capture_active) {
        self.debug_cuda_graph_capture_disabled = true;
        return error.CudaGraphCaptureUnsafeTempAlloc;
    }
    invalidateDebugFinalHiddenGraph(self);
}

test "CUDA persistent buffer reallocation invalidates graph pointers and rejects capture" {
    var self: CudaCompute = undefined;
    self.allocator = std.testing.allocator;
    self.debug_cuda_graph_slots = [_]CudaGraphReplaySlot{.{}} ** max_cuda_graph_replay_slots;
    self.debug_cuda_graph_slots[2].valid = true;
    self.debug_cuda_graph_capture_active = false;
    self.debug_cuda_graph_active_slot = 2;
    self.debug_cuda_graph_prepared_slot = 2;

    try preparePersistentCudaBufferReallocation(&self);

    try std.testing.expect(!self.debug_cuda_graph_slots[2].valid);
    try std.testing.expectEqual(@as(?usize, null), self.debug_cuda_graph_active_slot);
    try std.testing.expectEqual(@as(?usize, null), self.debug_cuda_graph_prepared_slot);

    self.debug_cuda_graph_capture_active = true;
    self.debug_cuda_graph_capture_disabled = false;
    try std.testing.expectError(error.CudaGraphCaptureUnsafeTempAlloc, preparePersistentCudaBufferReallocation(&self));
    try std.testing.expect(self.debug_cuda_graph_capture_disabled);
}

fn clearDebugCudaGraphCaptureState(self: *CudaCompute) void {
    self.debug_cuda_graph_capture_active = false;
    self.debug_cuda_graph_capture_disabled = false;
    self.debug_cuda_graph_active_slot = null;
    self.debug_cuda_graph_capture_temp_seq_begin = 0;
    self.debug_cuda_graph_prepared_output_kind = .tensor;
}

test "clearing CUDA graph capture state removes stale disabled latch" {
    var self: CudaCompute = undefined;
    self.debug_cuda_graph_capture_active = true;
    self.debug_cuda_graph_capture_disabled = true;
    self.debug_cuda_graph_active_slot = 3;
    self.debug_cuda_graph_prepared_output_kind = .greedy_token;

    clearDebugCudaGraphCaptureState(&self);

    try std.testing.expect(!self.debug_cuda_graph_capture_active);
    try std.testing.expect(!self.debug_cuda_graph_capture_disabled);
    try std.testing.expectEqual(@as(?usize, null), self.debug_cuda_graph_active_slot);
    try std.testing.expectEqual(CudaGraphReplayOutputKind.tensor, self.debug_cuda_graph_prepared_output_kind);
}

test "CUDA graph request reset clears stale capture and scalar state" {
    var self: CudaCompute = undefined;
    self.ctx.debug_graph_capture_active = false;
    self.generated_gqa_score_prework_templates = .empty;
    self.debug_cuda_graph_slots = [_]CudaGraphReplaySlot{.{}} ** max_cuda_graph_replay_slots;
    self.debug_cuda_graph_capture_active = true;
    self.debug_cuda_graph_capture_disabled = true;
    self.debug_cuda_graph_active_slot = 2;
    self.debug_cuda_graph_prepared_slot = 2;
    self.debug_cuda_decode_scalars_host_valid = true;
    self.debug_cuda_decode_scalars_device_valid = true;
    self.debug_cuda_decode_scalars_auto_advance_blocked = true;
    self.debug_cuda_decode_scalars_upload_deferred = true;
    self.debug_cuda_graph_decode_kv_seq_len = 17;

    try resetDebugCudaGraphRequestState(&self);

    try std.testing.expect(!self.debug_cuda_graph_capture_active);
    try std.testing.expect(!self.debug_cuda_graph_capture_disabled);
    try std.testing.expectEqual(@as(?usize, null), self.debug_cuda_graph_active_slot);
    try std.testing.expectEqual(@as(?usize, null), self.debug_cuda_graph_prepared_slot);
    try std.testing.expect(!self.debug_cuda_decode_scalars_host_valid);
    try std.testing.expect(!self.debug_cuda_decode_scalars_device_valid);
    try std.testing.expect(!self.debug_cuda_decode_scalars_auto_advance_blocked);
    try std.testing.expect(!self.debug_cuda_decode_scalars_upload_deferred);
    try std.testing.expectEqual(@as(usize, 0), self.debug_cuda_graph_decode_kv_seq_len);

    self.ctx.debug_graph_capture_active = true;
    self.debug_cuda_graph_capture_disabled = true;
    try std.testing.expectError(error.InvalidCudaState, resetDebugCudaGraphRequestState(&self));
    try std.testing.expect(self.debug_cuda_graph_capture_disabled);
}

test "CUDA request reset clears pinned temp slot ABI mappings" {
    var self: CudaCompute = undefined;
    self.allocator = std.testing.allocator;
    self.temp_buffer_mutex = .unlocked;
    self.temp_buffers = .empty;
    defer self.temp_buffers.deinit(std.testing.allocator);
    self.temp_pinned_slots = .empty;
    defer self.temp_pinned_slots.deinit(std.testing.allocator);
    self.temp_arena_planner = .{};
    defer self.temp_arena_planner.deinit(std.testing.allocator);
    self.debug_cuda_graph_slots = [_]CudaGraphReplaySlot{.{}} ** max_cuda_graph_replay_slots;
    self.debug_cuda_graph_active_slot = null;
    self.debug_cuda_graph_prepared_slot = null;
    self.temp_arena_generation = 1;
    try self.temp_pinned_slots.append(std.testing.allocator, .{
        .buffer = .{ .ptr = 0x1000, .len = 64 },
        .requested_len = 64,
    });
    try self.temp_pinned_slots.append(std.testing.allocator, .{
        .buffer = .{ .ptr = 0x2000, .len = 128 },
        .requested_len = 128,
    });
    self.temp_trace_seq = 99;

    try resetTempPinnedSlotsForRequest(&self);

    try std.testing.expectEqual(@as(usize, 0), self.temp_trace_seq);
    try std.testing.expectEqual(@as(u64, 2), self.temp_arena_generation);
    try std.testing.expectEqual(@as(usize, 2), self.temp_buffers.items.len);
    for (self.temp_pinned_slots.items) |slot| {
        try std.testing.expectEqual(@as(driver_mod.CUdeviceptr, 0), slot.buffer.ptr);
        try std.testing.expectEqual(@as(usize, 0), slot.requested_len);
        try std.testing.expect(!slot.in_use);
    }

    self.temp_pinned_slots.items[0] = .{
        .buffer = .{ .ptr = 0x3000, .len = 256 },
        .requested_len = 256,
        .in_use = true,
    };
    self.temp_trace_seq = 17;
    try std.testing.expectError(error.InvalidCudaState, resetTempPinnedSlotsForRequest(&self));
    try std.testing.expectEqual(@as(usize, 17), self.temp_trace_seq);
    try std.testing.expectEqual(@as(driver_mod.CUdeviceptr, 0x3000), self.temp_pinned_slots.items[0].buffer.ptr);
}

test "CUDA graph decode scalar helpers reject underflow and unsupported jumps" {
    const delta = [_]u32{ 1, 1, 1, 1, 0 };
    try std.testing.expect(decodeScalarsCanPreAdvance(.{ 1, 1, 1, 1, 0 }, delta));
    try std.testing.expect(!decodeScalarsCanPreAdvance(.{ 0, 1, 1, 1, 0 }, delta));
    try std.testing.expectEqual(delta, decodeScalarsAutoAdvanceDeltaBetween(
        .{ 4, 4, 8, 8, 0 },
        .{ 5, 5, 9, 9, 0 },
    ).?);
    try std.testing.expect(decodeScalarsAutoAdvanceDeltaBetween(
        .{ 5, 5, 9, 9, 0 },
        .{ 4, 6, 10, 10, 0 },
    ) == null);
    try std.testing.expect(!decodeScalarsAutoAdvanceDeltaSupported(.{ 1, 1, 2, 1, 0 }));
}

fn restoreDebugCudaGraphDecodeScalars(self: *CudaCompute, slot_idx: usize) void {
    self.debug_cuda_graph_slots[slot_idx].decode_scalars_auto_advance = false;
    self.debug_cuda_decode_scalars_upload_deferred = false;
    if (self.debug_cuda_decode_scalars_host_valid and self.debug_cuda_decode_scalars.ptr != 0) {
        uploadCachedDecodeScalars(self) catch {
            self.debug_cuda_decode_scalars_device_valid = false;
        };
    }
}

fn abortDebugCudaGraphCapture(self: *CudaCompute, slot_idx: usize) void {
    self.stats.cuda_graph_capture_discards += 1;
    if (self.ctx.debug_graph_capture_active) {
        if (self.ctx.endStreamCapture()) |graph| {
            self.ctx.destroyGraph(graph);
        } else |_| {}
    }
    resetCudaGraphReplaySlotMetadata(self, &self.debug_cuda_graph_slots[slot_idx]);
    clearDebugCudaGraphCaptureState(self);
    restoreDebugCudaGraphDecodeScalars(self, slot_idx);
}

fn resetDebugCudaGraphRequestState(self: *CudaCompute) !void {
    // Request boundaries must never inherit an invalidated capture latch. Do
    // not attempt recovery while CUDA still considers the stream captured.
    if (self.ctx.debug_graph_capture_active) return error.InvalidCudaState;
    clearDebugCudaGraphCaptureState(self);
    self.debug_cuda_graph_prepared_slot = null;
    self.debug_cuda_decode_scalars_host_valid = false;
    self.debug_cuda_decode_scalars_device_valid = false;
    self.debug_cuda_decode_scalars_auto_advance_blocked = false;
    self.debug_cuda_decode_scalars_upload_deferred = false;
    self.debug_cuda_graph_decode_kv_seq_len = 0;
    self.debug_cuda_graph_capture_inhibit_once = false;
    self.debug_cuda_graph_capture_temp_seq_begin = 0;
    self.debug_cuda_graph_prepared_attention_topology = .{ 0, 0, 0 };
    self.generated_gqa_score_prework_templates.clearRetainingCapacity();
    invalidateDebugFinalHiddenGraph(self);
}

fn cudaGraphReplayKey(label: []const u8) u64 {
    const key = std.hash.Wyhash.hash(0, label);
    return if (key == 0) 1 else key;
}

fn cudaGraphReplayKeyWithAttentionTopology(label: []const u8, topology: [3]u8) u64 {
    if (topology[0] == 0 and topology[1] == 0 and topology[2] == 0) return cudaGraphReplayKey(label);
    const key = std.hash.Wyhash.hash(cudaGraphReplayKey(label), &topology);
    return if (key == 0) 1 else key;
}

fn generatedGqaScorePreworkTemplateIdentity(
    request: kernels_mod.GeneratedGqaScorePreworkRequest,
) kernels_mod.GeneratedGqaScorePreworkRequest {
    var identity = request;
    identity.q_seq_len = 1;
    identity.kv_seq_len = 1;
    identity.mask_len = 0;
    identity.block_count = @intFromBool(request.block_count != 0);
    identity.physical_token_capacity = 1;
    return identity;
}

fn noteGeneratedGqaScorePreworkTemplate(
    self: *CudaCompute,
    request: kernels_mod.GeneratedGqaScorePreworkRequest,
) !void {
    var decode_template = request;
    decode_template.q_seq_len = 1;
    decode_template.mask_len = 0;
    const identity = generatedGqaScorePreworkTemplateIdentity(decode_template);
    for (self.generated_gqa_score_prework_templates.items) |*existing| {
        if (std.meta.eql(generatedGqaScorePreworkTemplateIdentity(existing.*), identity)) {
            existing.* = decode_template;
            return;
        }
    }
    try self.generated_gqa_score_prework_templates.append(self.allocator, decode_template);
}

fn generatedGqaAttentionTopologyForConsumerMode(
    templates: []const kernels_mod.GeneratedGqaScorePreworkRequest,
    kv_seq_len: usize,
    compute_major: i32,
    compute_minor: i32,
    score_prework_mode: kernels_mod.GeneratedGqaScorePreworkMode,
    consumer_mode: kernels_mod.GeneratedGqaScorePreworkConsumerMode,
) [3]u8 {
    var score_route_mask: u8 = 0;
    var score_consumer_mask: u8 = 0;
    var every_policy_uses_score_prework = templates.len != 0;
    for (templates) |template| {
        var request = template;
        request.q_seq_len = 1;
        request.kv_seq_len = kv_seq_len;
        request.mask_len = 0;
        request.physical_token_capacity = @max(request.physical_token_capacity, kv_seq_len);
        if (request.block_count != 0) {
            if (request.page_size_tokens == 0) {
                every_policy_uses_score_prework = false;
                continue;
            }
            const rounded = std.math.add(usize, kv_seq_len, request.page_size_tokens - 1) catch {
                every_policy_uses_score_prework = false;
                continue;
            };
            request.block_count = rounded / request.page_size_tokens;
        }
        const route = kernels_mod.generatedGqaScorePreworkSelectionWithConsumerFor(
            request,
            score_prework_mode,
            consumer_mode,
            compute_major,
            compute_minor,
        ) orelse {
            every_policy_uses_score_prework = false;
            continue;
        };
        const shift: u3 = @intCast(@intFromEnum(route.route));
        score_route_mask |= @as(u8, 1) << shift;
        const consumer_shift: u3 = @intCast(@intFromEnum(route.consumer));
        score_consumer_mask |= @as(u8, 1) << consumer_shift;
    }

    // Score-prework takes precedence over the split-summary launch only when
    // every observed attention policy selects it. Mixed models retain both
    // pieces of topology in the key.
    const split_schedule_tag = if (every_policy_uses_score_prework)
        0
    else
        kernels_mod.generatedGqaDecodeScheduleTag(kv_seq_len);
    return .{ split_schedule_tag, score_route_mask, score_consumer_mask };
}

fn generatedGqaAttentionTopology(self: *const CudaCompute, kv_seq_len: usize) [3]u8 {
    return generatedGqaAttentionTopologyForConsumerMode(
        self.generated_gqa_score_prework_templates.items,
        kv_seq_len,
        self.ctx.info.compute_major,
        self.ctx.info.compute_minor,
        self.kernels.gqa_score_prework_mode,
        self.kernels.gqa_score_prework_consumer_mode,
    );
}

fn cudaGraphReplayKeyForDecodeSchedule(self: *const CudaCompute, label: []const u8, kv_seq_len: usize) u64 {
    // CUDA graph execs encode launch topology. Both generated split schedules
    // and exact score-prework routes can cross thresholds as KV grows.
    return cudaGraphReplayKeyWithAttentionTopology(label, generatedGqaAttentionTopology(self, kv_seq_len));
}

test "CUDA graph replay key separates generated attention schedules" {
    const label = "gpt.final_hidden_decode";
    const base = cudaGraphReplayKey(label);
    const serial = cudaGraphReplayKeyWithAttentionTopology(label, .{ @intFromEnum(kernels_mod.GeneratedGqaDecodeSchedule.serial), 0, 0 });
    const split2 = cudaGraphReplayKeyWithAttentionTopology(label, .{ @intFromEnum(kernels_mod.GeneratedGqaDecodeSchedule.split2), 0, 0 });
    const split4 = cudaGraphReplayKeyWithAttentionTopology(label, .{ @intFromEnum(kernels_mod.GeneratedGqaDecodeSchedule.split4), 0, 0 });
    const split8 = cudaGraphReplayKeyWithAttentionTopology(label, .{ @intFromEnum(kernels_mod.GeneratedGqaDecodeSchedule.split8), 0, 0 });
    const local_route_mask = @as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkRoute.gemma4_f16_local);
    const global_route_mask = @as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkRoute.gemma4_f16_global);
    const serial_consumer_mask = @as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkConsumer.serial);
    const tiled64_consumer_mask = @as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkConsumer.tiled64);
    const score_local = cudaGraphReplayKeyWithAttentionTopology(label, .{ 0, local_route_mask, serial_consumer_mask });
    const score_local_tiled64 = cudaGraphReplayKeyWithAttentionTopology(label, .{ 0, local_route_mask, tiled64_consumer_mask });
    const score_global = cudaGraphReplayKeyWithAttentionTopology(label, .{ 0, global_route_mask, serial_consumer_mask });

    try std.testing.expectEqual(base, cudaGraphReplayKeyWithAttentionTopology(label, .{ 0, 0, 0 }));
    try std.testing.expect(base != serial);
    try std.testing.expect(base != split2);
    try std.testing.expect(base != split4);
    try std.testing.expect(base != split8);
    try std.testing.expect(serial != split2);
    try std.testing.expect(serial != split4);
    try std.testing.expect(serial != split8);
    try std.testing.expect(split2 != split4);
    try std.testing.expect(split2 != split8);
    try std.testing.expect(split4 != split8);
    try std.testing.expect(score_local != score_global);
    try std.testing.expect(score_local != score_local_tiled64);
    try std.testing.expect(score_local != split8);
    try std.testing.expect(score_global != split8);
}

test "CUDA graph topology retains score route and consumer masks" {
    const local = kernels_mod.GeneratedGqaScorePreworkRequest{
        .batch = 1,
        .q_seq_len = 1,
        .kv_seq_len = 512,
        .num_heads = 8,
        .num_kv_heads = 1,
        .head_dim = 256,
        .sliding_window = 512,
        .mask_len = 0,
        .bias_mode = 0,
        .key_row_bytes = 512,
        .base_key_row_bytes = 512,
        .value_row_bytes = 512,
        .block_count = 4,
        .page_size_tokens = 128,
        .format = 2,
        .value_format = 2,
        .physical_token_capacity = 512,
    };
    const global = kernels_mod.GeneratedGqaScorePreworkRequest{
        .batch = 1,
        .q_seq_len = 1,
        .kv_seq_len = 512,
        .num_heads = 8,
        .num_kv_heads = 1,
        .head_dim = 512,
        .sliding_window = 0,
        .mask_len = 0,
        .bias_mode = 0,
        .key_row_bytes = 1024,
        .base_key_row_bytes = 1024,
        .value_row_bytes = 1024,
        .block_count = 4,
        .page_size_tokens = 128,
        .format = 2,
        .value_format = 2,
        .physical_token_capacity = 512,
    };
    var templates = [_]kernels_mod.GeneratedGqaScorePreworkRequest{ local, global };
    const serial = generatedGqaAttentionTopologyForConsumerMode(&templates, 512, 8, 9, .automatic, .serial);
    const tiled64 = generatedGqaAttentionTopologyForConsumerMode(&templates, 512, 8, 9, .automatic, .tiled64);
    const route_mask = (@as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkRoute.gemma4_f16_local)) |
        (@as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkRoute.gemma4_f16_global));
    try std.testing.expectEqual(@as(u8, 0), serial[0]);
    try std.testing.expectEqual(route_mask, serial[1]);
    try std.testing.expectEqual(@as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkConsumer.serial), serial[2]);
    try std.testing.expectEqual(@as(u8, 0), tiled64[0]);
    try std.testing.expectEqual(route_mask, tiled64[1]);
    try std.testing.expectEqual(@as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkConsumer.tiled64), tiled64[2]);
    try std.testing.expect(!std.mem.eql(u8, &serial, &tiled64));

    // A mixed qualified/unqualified set retains both consumer bits rather than
    // collapsing to an aggregate score-prework topology.
    templates[0].num_kv_heads = 2;
    templates[0].key_row_bytes = 1024;
    templates[0].base_key_row_bytes = 1024;
    templates[0].value_row_bytes = 1024;
    const mixed = generatedGqaAttentionTopologyForConsumerMode(&templates, 512, 8, 9, .automatic, .tiled64);
    try std.testing.expectEqual(route_mask, mixed[1]);
    try std.testing.expectEqual(
        (@as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkConsumer.serial)) |
            (@as(u8, 1) << @intFromEnum(kernels_mod.GeneratedGqaScorePreworkConsumer.tiled64)),
        mixed[2],
    );
}

fn findCudaGraphReplaySlotForByteLen(self: *CudaCompute, replay_key: u64, byte_len: usize) usize {
    for (&self.debug_cuda_graph_slots, 0..) |*slot, idx| {
        if (slot.input_storage.ptr != 0 and slot.input_storage.len == byte_len and slot.replay_key == replay_key) return idx;
    }
    for (&self.debug_cuda_graph_slots, 0..) |*slot, idx| {
        if (slot.input_storage.ptr == 0) return idx;
    }
    for (&self.debug_cuda_graph_slots, 0..) |*slot, idx| {
        if (!slot.valid) return idx;
    }
    const idx = self.debug_cuda_graph_next_evict_slot % max_cuda_graph_replay_slots;
    self.debug_cuda_graph_next_evict_slot = (idx + 1) % max_cuda_graph_replay_slots;
    return idx;
}

fn findExistingCudaGraphReplaySlotForByteLen(self: *CudaCompute, replay_key: u64, byte_len: usize) ?usize {
    for (&self.debug_cuda_graph_slots, 0..) |*slot, idx| {
        if (slot.input_storage.ptr != 0 and
            slot.input_storage.len == byte_len and
            slot.replay_key == replay_key and
            slot.valid and
            slot.exec != null and
            slot.temp_arena_generation == self.temp_arena_generation)
        {
            return idx;
        }
    }
    return null;
}

fn ensureCudaGraphReplaySlotStorage(self: *CudaCompute, slot: *CudaGraphReplaySlot, byte_len: usize) !void {
    if (slot.input_storage.ptr != 0 and slot.input_storage.len == byte_len and
        slot.output_storage.ptr != 0 and slot.output_storage.len >= byte_len)
    {
        return;
    }
    resetCudaGraphReplaySlotMetadata(self, slot);
    if (slot.input_storage.ptr != 0) {
        self.stats.device_free_calls += 1;
        slot.input_storage.free(&self.ctx);
        slot.input_storage = .{};
    }
    if (slot.output_storage.ptr != 0) {
        self.stats.device_free_calls += 1;
        slot.output_storage.free(&self.ctx);
        slot.output_storage = .{};
    }
    slot.input_storage = try buffer_mod.DeviceBuffer.alloc(&self.ctx, byte_len);
    self.noteDeviceBytes(byte_len);
    self.stats.device_alloc_calls += 1;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: persistent_input_alloc ptr=0x{x} len={d}", .{
            slot.input_storage.ptr,
            slot.input_storage.len,
        });
    }
    slot.output_storage = try buffer_mod.DeviceBuffer.alloc(&self.ctx, byte_len);
    self.noteDeviceBytes(byte_len);
    self.stats.device_alloc_calls += 1;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: persistent_output_alloc ptr=0x{x} len={d}", .{
            slot.output_storage.ptr,
            slot.output_storage.len,
        });
    }
}

fn ensureCudaGraphReplayAuxInputStorage(self: *CudaCompute, slot: *CudaGraphReplaySlot, byte_len: usize) !void {
    if (slot.aux_input_storage.ptr != 0 and slot.aux_input_storage.len == byte_len) {
        return;
    }
    resetCudaGraphReplaySlotMetadata(self, slot);
    if (slot.aux_input_storage.ptr != 0) {
        self.stats.device_free_calls += 1;
        slot.aux_input_storage.free(&self.ctx);
        slot.aux_input_storage = .{};
    }
    slot.aux_input_storage = try buffer_mod.DeviceBuffer.alloc(&self.ctx, byte_len);
    self.noteDeviceBytes(byte_len);
    self.stats.device_alloc_calls += 1;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: persistent_aux_input_alloc ptr=0x{x} len={d}", .{
            slot.aux_input_storage.ptr,
            slot.aux_input_storage.len,
        });
    }
}

fn debugCudaGraphPrepareReplayInput(
    self: *CudaCompute,
    output_kind: CudaGraphReplayOutputKind,
    label: []const u8,
    input: CT,
    kv_seq_len: usize,
) !?CT {
    self.debug_cuda_graph_prepared_slot = null;
    self.debug_cuda_graph_prepared_output_kind = .tensor;
    if (!cudaDebugGraphPersistentReplayEnabled(self)) return null;
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;

    const input_tensor = tensorFromCt(input);
    if (input_tensor.dtype != .f32 or input_tensor.quant_type != null) return null;
    const byte_len = try checkedMul(input_tensor.elem_count, @sizeOf(f32));
    if (byte_len == 0) return null;
    if (input_tensor.buffer.len < byte_len) return error.InvalidCudaState;

    // This is the decode-iteration boundary for graph-managed execution.
    // Process it before arming the current replay slot: activating or
    // invalidating a learned arena destroys stale graph metadata, including
    // any prepared slot. Doing that later from decode-scalar preparation
    // would disarm the operation currently being prepared, route its greedy
    // result through an ordinary temporary, and perturb the very allocation
    // ABI being validated.
    try noteLearnedTempArenaBoundary(self);
    // Admission denial permanently disables this learned planner. Do not keep
    // paying the persistent-input D2D copy in automatic mode; returning null
    // selects the ordinary eager input, while graph-required callers retain
    // their existing `CudaGraphReplayRequired` failure at the architecture
    // boundary.
    if (self.temp_slot_config.autoplan_enabled and self.temp_arena_planner.disabled) return null;

    const capture_possible = self.temp_slot_config.configured_period != 0 or
        self.temp_slot_config.autoplan_enabled or
        cudaDebugGraphCaptureAllowUnpinned();
    const attention_topology = generatedGqaAttentionTopology(self, kv_seq_len);
    self.debug_cuda_graph_prepared_attention_topology = attention_topology;
    const replay_key = cudaGraphReplayKeyForDecodeSchedule(self, label, kv_seq_len);
    const slot_idx = if (capture_possible)
        findCudaGraphReplaySlotForByteLen(self, replay_key, byte_len)
    else
        findExistingCudaGraphReplaySlotForByteLen(self, replay_key, byte_len) orelse return null;
    self.debug_cuda_graph_prepared_slot = slot_idx;
    self.debug_cuda_graph_prepared_output_kind = output_kind;
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    if (slot.exec != null and slot.temp_arena_generation != self.temp_arena_generation) {
        resetCudaGraphReplaySlotMetadata(self, slot);
    }
    try ensureCudaGraphReplaySlotStorage(self, slot, byte_len);
    if (slot.replay_key != replay_key) {
        resetCudaGraphReplaySlotMetadata(self, slot);
        slot.replay_key = replay_key;
    }
    slot.aux_input_prepared = false;

    try copyFromDeviceTracked(self, slot.input_storage, input_tensor.buffer, byte_len);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = slot.input_storage,
        .dtype = .f32,
        .shape = shape,
        .elem_count = input_tensor.elem_count,
        .owns_buffer = false,
    };
    return tensor;
}

fn debugCudaGraphPrepareFinalHiddenReplayInput(ctx: *anyopaque, label: []const u8, input: CT, kv_seq_len: usize) !?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    return debugCudaGraphPrepareReplayInput(self, .tensor, label, input, kv_seq_len);
}

fn debugCudaGraphPrepareGreedyTokenReplayInput(ctx: *anyopaque, label: []const u8, input: CT, kv_seq_len: usize) !?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    return debugCudaGraphPrepareReplayInput(self, .greedy_token, label, input, kv_seq_len);
}

fn debugCudaGraphPrepareFinalHiddenReplayAuxInput(ctx: *anyopaque, input: CT) !?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphPersistentReplayEnabled(self)) return null;
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;
    const slot_idx = self.debug_cuda_graph_prepared_slot orelse return null;
    const slot = &self.debug_cuda_graph_slots[slot_idx];

    const input_tensor = tensorFromCt(input);
    if (input_tensor.dtype != .f32 or input_tensor.quant_type != null) return null;
    const byte_len = try checkedMul(input_tensor.elem_count, @sizeOf(f32));
    if (byte_len == 0) return null;
    if (input_tensor.buffer.len < byte_len) return error.InvalidCudaState;

    try ensureCudaGraphReplayAuxInputStorage(self, slot, byte_len);
    try copyFromDeviceTracked(self, slot.aux_input_storage, input_tensor.buffer, byte_len);
    slot.aux_input_prepared = true;

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = slot.aux_input_storage,
        .dtype = .f32,
        .shape = shape,
        .elem_count = input_tensor.elem_count,
        .owns_buffer = false,
    };
    return tensor;
}

fn debugCudaGraphRegisterFinalHiddenReplayInput(ctx: *anyopaque, input: CT) !void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphPersistentReplayEnabled(self)) return;
    if (!self.debug_cuda_graph_capture_active) return;
    const slot_idx = self.debug_cuda_graph_active_slot orelse return;
    const slot = &self.debug_cuda_graph_slots[slot_idx];

    const input_tensor = tensorFromCt(input);
    slot.input = input_tensor.buffer;
    slot.input_valid = true;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: persistent_input slot={d} input=0x{x} len={d}", .{
            slot_idx,
            input_tensor.buffer.ptr,
            input_tensor.buffer.len,
        });
    }
}

fn debugCudaGraphRegisterFinalHiddenReplayAuxInput(ctx: *anyopaque, input: CT) !void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphPersistentReplayEnabled(self)) return;
    if (!self.debug_cuda_graph_capture_active) return;
    const slot_idx = self.debug_cuda_graph_active_slot orelse return;
    const slot = &self.debug_cuda_graph_slots[slot_idx];

    const input_tensor = tensorFromCt(input);
    slot.aux_input = input_tensor.buffer;
    slot.aux_input_valid = true;
    slot.aux_input_required = true;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: persistent_aux_input slot={d} input=0x{x} len={d}", .{
            slot_idx,
            input_tensor.buffer.ptr,
            input_tensor.buffer.len,
        });
    }
}

fn debugCudaGraphRegisterFinalHiddenReplayBoundary(ctx: *anyopaque, input: CT, output: CT) !void {
    _ = input;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphPersistentReplayEnabled(self)) return;
    if (!self.debug_cuda_graph_capture_active) return;
    const slot_idx = self.debug_cuda_graph_active_slot orelse return;
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    if (!slot.input_valid) return;

    const output_tensor = tensorFromCt(output);
    if (output_tensor.quant_type != null) return error.UnsupportedTensorType;
    const byte_len = try checkedMul(output_tensor.elem_count, output_tensor.dtype.byteSize());
    if (byte_len == 0) return error.InvalidTensorShape;
    if (slot.output_storage.ptr == 0 or slot.output_storage.len < byte_len) return error.InvalidCudaState;
    if (slot.output_storage.ptr != output_tensor.buffer.ptr) {
        try self.kernels.launchCopyBytes(&self.ctx, slot.output_storage, output_tensor.buffer, byte_len);
    }

    const shape = try dupeShape(self.allocator, output_tensor.shape);
    errdefer self.allocator.free(shape);

    if (slot.shape) |old_shape| {
        self.allocator.free(old_shape);
    }
    slot.shape = shape;
    slot.output = .{ .ptr = slot.output_storage.ptr, .len = byte_len };
    slot.elem_count = output_tensor.elem_count;
    slot.dtype = output_tensor.dtype;
    slot.valid = true;

    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: persistent_boundary slot={d} input=0x{x} output=0x{x} temp_output=0x{x} elem_count={d} dtype={s} kv_capacity={d} shape={any}", .{
            slot_idx,
            slot.input.ptr,
            slot.output.ptr,
            output_tensor.buffer.ptr,
            output_tensor.elem_count,
            @tagName(output_tensor.dtype),
            if (slot.kv_replay_capacity_valid) slot.kv_replay_capacity_tokens else 0,
            output_tensor.shape,
        });
    }
}

fn cudaGraphReplayArenaReady(self: *CudaCompute, slot: *const CudaGraphReplaySlot) bool {
    if (slot.temp_arena_generation == 0 or slot.temp_arena_generation != self.temp_arena_generation) return false;
    if (self.temp_slot_config.autoplan_enabled) {
        while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.temp_buffer_mutex.unlock();
        return self.temp_arena_planner.canGraphReplay(
            self.temp_trace_seq,
            slot.captured_temp_allocation_count,
        );
    }
    return true;
}

fn noteCapturedTempAllocationsReplayed(self: *CudaCompute, allocation_count: usize) !void {
    // Host allocation calls inside the graph do not run during replay. Advance
    // the logical sequence and reconstruct their learned lengths so uncaptured
    // tail work maps to the same pinned slots and the complete iteration is
    // validated at its next boundary.
    while (!self.temp_buffer_mutex.tryLock()) std.atomic.spinLoopHint();
    defer self.temp_buffer_mutex.unlock();
    if (self.temp_slot_config.autoplan_enabled and
        !self.temp_arena_planner.markGraphReplay(self.temp_trace_seq, allocation_count))
    {
        return error.InvalidCudaState;
    }
    self.temp_trace_seq +%= allocation_count;
}

fn debugCudaGraphReplayFinalHidden(ctx: *anyopaque, input: CT) !?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphPersistentReplayEnabled(self)) {
        try flushDeferredDecodeScalarUpload(self);
        return null;
    }
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;
    const slot_idx = self.debug_cuda_graph_prepared_slot orelse {
        try flushDeferredDecodeScalarUpload(self);
        return null;
    };
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    const exec = slot.exec orelse {
        try flushDeferredDecodeScalarUpload(self);
        return null;
    };
    if (!slot.valid) {
        try flushDeferredDecodeScalarUpload(self);
        return null;
    }
    if (!cudaGraphReplayArenaReady(self, slot)) {
        if (slot.temp_arena_generation != self.temp_arena_generation) {
            std.log.warn("cuda_graph_capture_probe: stale_temp_arena_generation slot={d} captured={d} current={d}", .{
                slot_idx,
                slot.temp_arena_generation,
                self.temp_arena_generation,
            });
        }
        // A phase/count mismatch is just as unsafe as a stale arena
        // generation: never retain an exec whose captured addresses cannot be
        // proven against the current request's allocation ABI.
        resetCudaGraphReplaySlotMetadata(self, slot);
        try flushDeferredDecodeScalarUpload(self);
        return null;
    }
    const shape_src = slot.shape orelse {
        try flushDeferredDecodeScalarUpload(self);
        return null;
    };
    if (slot.kv_replay_capacity_valid and self.debug_cuda_graph_decode_kv_seq_len > slot.kv_replay_capacity_tokens) {
        self.stats.cuda_graph_capture_capacity_skips += 1;
        std.log.warn("cuda_graph_capture_probe: persistent_replay_kv_capacity_exceeded slot={d} kv_seq_len={d} capacity={d}", .{
            slot_idx,
            self.debug_cuda_graph_decode_kv_seq_len,
            slot.kv_replay_capacity_tokens,
        });
        try flushDeferredDecodeScalarUpload(self);
        return null;
    }

    const input_tensor = tensorFromCt(input);
    if (input_tensor.buffer.ptr != slot.input.ptr or input_tensor.buffer.len != slot.input.len) {
        std.log.warn("cuda_graph_capture_probe: persistent_replay_input_mismatch slot={d} expected=0x{x}/{d} actual=0x{x}/{d}", .{
            slot_idx,
            slot.input.ptr,
            slot.input.len,
            input_tensor.buffer.ptr,
            input_tensor.buffer.len,
        });
        try flushDeferredDecodeScalarUpload(self);
        return null;
    }
    if (slot.aux_input_required) {
        if (!slot.aux_input_valid or !slot.aux_input_prepared) {
            try flushDeferredDecodeScalarUpload(self);
            return null;
        }
        if (slot.aux_input_storage.ptr != slot.aux_input.ptr or slot.aux_input_storage.len != slot.aux_input.len) {
            std.log.warn("cuda_graph_capture_probe: persistent_replay_aux_input_mismatch slot={d} expected=0x{x}/{d} actual=0x{x}/{d}", .{
                slot_idx,
                slot.aux_input.ptr,
                slot.aux_input.len,
                slot.aux_input_storage.ptr,
                slot.aux_input_storage.len,
            });
            try flushDeferredDecodeScalarUpload(self);
            return null;
        }
    }

    const output = slot.output;
    if (output.ptr == 0) {
        try flushDeferredDecodeScalarUpload(self);
        return null;
    }

    const shape = try dupeShape(self.allocator, shape_src);
    errdefer self.allocator.free(shape);

    var profile_scope = beginDecodeProfile(self, .graph_replay, 1);
    defer if (profile_scope) |*scope| scope.end();
    try self.ctx.launchGraph(exec);
    self.debug_cuda_graph_prepared_output_kind = .tensor;
    try noteCapturedTempAllocationsReplayed(self, slot.captured_temp_allocation_count);
    if (slot.decode_scalars_auto_advance) markDecodeScalarsDeviceMatchesHost(self);
    self.stats.cuda_graph_capture_replays += 1;
    self.stats.cuda_graph_capture_persistent_replays += 1;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: persistent_replayed slot={d} input=0x{x} output=0x{x} kv_seq_len={d} kv_capacity={d}", .{
            slot_idx,
            input_tensor.buffer.ptr,
            output.ptr,
            self.debug_cuda_graph_decode_kv_seq_len,
            if (slot.kv_replay_capacity_valid) slot.kv_replay_capacity_tokens else 0,
        });
    }

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = output,
        .dtype = slot.dtype,
        .shape = shape,
        .elem_count = slot.elem_count,
        .owns_buffer = false,
    };
    return tensor;
}

fn debugCudaGraphReplayFinalHiddenDiscard(ctx: *anyopaque, input: CT) !bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaDebugGraphPersistentReplayEnabled(self)) {
        try flushDeferredDecodeScalarUpload(self);
        return false;
    }
    if (self.debug_cuda_graph_capture_active) return error.InvalidCudaState;
    const slot_idx = self.debug_cuda_graph_prepared_slot orelse {
        try flushDeferredDecodeScalarUpload(self);
        return false;
    };
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    const exec = slot.exec orelse {
        try flushDeferredDecodeScalarUpload(self);
        return false;
    };
    if (!slot.valid) {
        try flushDeferredDecodeScalarUpload(self);
        return false;
    }
    if (!cudaGraphReplayArenaReady(self, slot)) {
        if (slot.temp_arena_generation != self.temp_arena_generation) {
            std.log.warn("cuda_graph_capture_probe: stale_temp_arena_generation slot={d} captured={d} current={d}", .{
                slot_idx,
                slot.temp_arena_generation,
                self.temp_arena_generation,
            });
        }
        resetCudaGraphReplaySlotMetadata(self, slot);
        try flushDeferredDecodeScalarUpload(self);
        return false;
    }
    if (slot.shape == null) {
        try flushDeferredDecodeScalarUpload(self);
        return false;
    }
    if (slot.kv_replay_capacity_valid and self.debug_cuda_graph_decode_kv_seq_len > slot.kv_replay_capacity_tokens) {
        self.stats.cuda_graph_capture_capacity_skips += 1;
        std.log.warn("cuda_graph_capture_probe: persistent_replay_kv_capacity_exceeded slot={d} kv_seq_len={d} capacity={d}", .{
            slot_idx,
            self.debug_cuda_graph_decode_kv_seq_len,
            slot.kv_replay_capacity_tokens,
        });
        try flushDeferredDecodeScalarUpload(self);
        return false;
    }

    const input_tensor = tensorFromCt(input);
    if (input_tensor.buffer.ptr != slot.input.ptr or input_tensor.buffer.len != slot.input.len) {
        std.log.warn("cuda_graph_capture_probe: persistent_replay_input_mismatch slot={d} expected=0x{x}/{d} actual=0x{x}/{d}", .{
            slot_idx,
            slot.input.ptr,
            slot.input.len,
            input_tensor.buffer.ptr,
            input_tensor.buffer.len,
        });
        try flushDeferredDecodeScalarUpload(self);
        return false;
    }
    if (slot.aux_input_required) {
        if (!slot.aux_input_valid or !slot.aux_input_prepared) {
            try flushDeferredDecodeScalarUpload(self);
            return false;
        }
        if (slot.aux_input_storage.ptr != slot.aux_input.ptr or slot.aux_input_storage.len != slot.aux_input.len) {
            std.log.warn("cuda_graph_capture_probe: persistent_replay_aux_input_mismatch slot={d} expected=0x{x}/{d} actual=0x{x}/{d}", .{
                slot_idx,
                slot.aux_input.ptr,
                slot.aux_input.len,
                slot.aux_input_storage.ptr,
                slot.aux_input_storage.len,
            });
            try flushDeferredDecodeScalarUpload(self);
            return false;
        }
    }
    if (slot.output.ptr == 0) {
        try flushDeferredDecodeScalarUpload(self);
        return false;
    }

    var profile_scope = beginDecodeProfile(self, .graph_replay, 1);
    defer if (profile_scope) |*scope| scope.end();
    try self.ctx.launchGraph(exec);
    self.debug_cuda_graph_prepared_output_kind = .tensor;
    try noteCapturedTempAllocationsReplayed(self, slot.captured_temp_allocation_count);
    if (slot.decode_scalars_auto_advance) markDecodeScalarsDeviceMatchesHost(self);
    self.stats.cuda_graph_capture_replays += 1;
    self.stats.cuda_graph_capture_persistent_replays += 1;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: persistent_replayed_discard slot={d} input=0x{x} output=0x{x} kv_seq_len={d} kv_capacity={d}", .{
            slot_idx,
            input_tensor.buffer.ptr,
            slot.output.ptr,
            self.debug_cuda_graph_decode_kv_seq_len,
            if (slot.kv_replay_capacity_valid) slot.kv_replay_capacity_tokens else 0,
        });
    }
    return true;
}

fn replayCapturedDebugGraphOneShot(self: *CudaCompute, graph: driver_mod.CUgraph) !void {
    const exec = try self.ctx.instantiateGraph(graph);
    defer self.ctx.destroyGraphExec(exec);
    self.stats.cuda_graph_capture_instantiates += 1;
    try self.ctx.launchGraph(exec);
    self.stats.cuda_graph_capture_replays += 1;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: replayed", .{});
    }
}

fn replayCapturedDebugGraphWithUpdate(self: *CudaCompute, slot_idx: usize, graph: driver_mod.CUgraph) !void {
    const slot = &self.debug_cuda_graph_slots[slot_idx];
    update_existing: {
        if (slot.exec) |exec| {
            const outcome = self.ctx.updateGraphExec(exec, graph) catch |err| {
                if (err == error.CudaSymbolMissing) {
                    self.stats.cuda_graph_capture_update_unavailable += 1;
                    std.log.warn("cuda_graph_capture_probe: update_unavailable fallback=instantiate", .{});
                    self.ctx.destroyGraphExec(exec);
                    slot.exec = null;
                    break :update_existing;
                }
                return err;
            };
            if (outcome.success()) {
                self.stats.cuda_graph_capture_update_successes += 1;
                try self.ctx.launchGraph(exec);
                self.stats.cuda_graph_capture_replays += 1;
                if (cudaDebugGraphCaptureProbeTraceEnabled()) {
                    std.log.info("cuda_graph_capture_probe: updated_replayed", .{});
                }
                return;
            }

            self.stats.cuda_graph_capture_update_failures += 1;
            std.log.warn("cuda_graph_capture_probe: update_failed cuda={s} update_result={s} fallback=reinstantiate", .{
                self.ctx.driver.errorName(outcome.cuda_result),
                cudaGraphExecUpdateResultName(outcome.update_result),
            });
            self.ctx.destroyGraphExec(exec);
            slot.exec = null;
        }
    }

    const exec = try self.ctx.instantiateGraph(graph);
    slot.exec = exec;
    self.stats.cuda_graph_capture_instantiates += 1;
    try self.ctx.launchGraph(exec);
    self.stats.cuda_graph_capture_replays += 1;
    if (cudaDebugGraphCaptureProbeTraceEnabled()) {
        std.log.info("cuda_graph_capture_probe: instantiated_cached_replayed slot={d}", .{slot_idx});
    }
}

fn debugCudaGraphCaptureEnd(ctx: *anyopaque, replay: bool) !void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!self.debug_cuda_graph_capture_active) return;
    const slot_idx = self.debug_cuda_graph_active_slot orelse 0;
    const capture_sequence_valid = self.temp_trace_seq >= self.debug_cuda_graph_capture_temp_seq_begin;
    const captured_temp_allocation_count = if (capture_sequence_valid)
        self.temp_trace_seq - self.debug_cuda_graph_capture_temp_seq_begin
    else
        0;
    const captured_attention_topology = generatedGqaAttentionTopology(self, self.debug_cuda_graph_decode_kv_seq_len);
    const attention_topology_stable = std.mem.eql(
        u8,
        &self.debug_cuda_graph_prepared_attention_topology,
        &captured_attention_topology,
    );
    const capture_was_disabled = self.debug_cuda_graph_capture_disabled or
        !capture_sequence_valid or
        !attention_topology_stable;
    if (!attention_topology_stable) {
        std.log.warn("cuda_graph_capture_probe: attention_topology_changed prepared={any} captured={any} disabled=1", .{
            self.debug_cuda_graph_prepared_attention_topology,
            captured_attention_topology,
        });
    }
    var capture_executed = false;
    var capture_cached = false;
    defer {
        clearDebugCudaGraphCaptureState(self);
        if (!capture_cached) {
            resetCudaGraphReplaySlotMetadata(self, &self.debug_cuda_graph_slots[slot_idx]);
        }
        if (!capture_executed) {
            restoreDebugCudaGraphDecodeScalars(self, slot_idx);
        }
    }

    const graph = self.ctx.endStreamCapture() catch |err| {
        self.stats.cuda_graph_capture_discards += 1;
        // A caller requesting replay relies on this function to execute the
        // captured work. Never report success after end-capture failed: doing
        // so would expose an uninitialized/stale output tensor.
        if (!replay and capture_was_disabled) return;
        return err;
    };
    defer self.ctx.destroyGraph(graph);
    if (replay) {
        if (!capture_was_disabled) {
            const slot = &self.debug_cuda_graph_slots[slot_idx];
            if (cudaDebugGraphPersistentReplayEnabled(self) or cudaDebugGraphCaptureUpdateExecEnabled(self)) {
                slot.temp_arena_generation = self.temp_arena_generation;
                slot.captured_temp_allocation_count = captured_temp_allocation_count;
                try replayCapturedDebugGraphWithUpdate(self, slot_idx, graph);
                capture_cached = true;
            } else {
                try replayCapturedDebugGraphOneShot(self, graph);
            }
        } else {
            // Topology or arena validation can make a graph unsafe to retain,
            // but stream capture deferred all of its work. Execute it exactly
            // once before discarding its metadata so the caller still receives
            // the output it requested.
            try replayCapturedDebugGraphOneShot(self, graph);
            self.stats.cuda_graph_capture_discards += 1;
        }
        markDecodeScalarsDeviceMatchesHost(self);
        capture_executed = true;
    } else {
        self.stats.cuda_graph_capture_discards += 1;
        if (cudaDebugGraphCaptureProbeTraceEnabled()) {
            std.log.info("cuda_graph_capture_probe: discarded", .{});
        }
    }
}

fn fromFloat32Op(ctx: *anyopaque, data: []const f32) anyerror!CT {
    var shape = [_]i32{@intCast(data.len)};
    return fromFloat32ShapeOp(ctx, data, &shape);
}

fn fromFloat32ShapeOp(ctx: *anyopaque, data: []const f32, shape: []const i32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var elem_count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @intCast(dim));
    }
    if (elem_count != data.len) return error.InvalidShape;

    const shape_i64 = try self.allocator.alloc(i64, shape.len);
    errdefer self.allocator.free(shape_i64);
    for (shape, 0..) |dim, i| shape_i64[i] = dim;

    var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const data_bytes = std.mem.sliceAsBytes(data);
    const tried_pinned_scalar_upload = pinnedScalarUploadEligible(self, data_bytes);
    const copied_from_pinned = (try copyFromPinnedScalarUploadRing(self, device, data_bytes)) or
        (try copyFromPinnedUploadArena(self, device, data_bytes));
    if (!copied_from_pinned) {
        if (tried_pinned_scalar_upload) self.stats.pinned_scalar_upload_fallbacks += 1;
        try copyFromHostTracked(self, device, data_bytes);
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.upload_syncs += 1;
    }
    self.stats.from_float32_calls += 1;
    const byte_len = data.len * @sizeOf(f32);
    self.stats.from_float32_bytes += byte_len;
    noteUploadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.upload_top_sizes, &self.stats.upload_top_counts, byte_len);

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = .f32,
        .shape = shape_i64,
        .elem_count = elem_count,
    };
    return tensor;
}

fn fromInt32ShapeOp(ctx: *anyopaque, data: []const i32, shape: []const i32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var elem_count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @intCast(dim));
    }
    if (elem_count != data.len) return error.InvalidShape;

    const shape_i64 = try self.allocator.alloc(i64, shape.len);
    errdefer self.allocator.free(shape_i64);
    for (shape, 0..) |dim, i| shape_i64[i] = dim;

    var device = try allocDeviceBuffer(self, data.len * @sizeOf(i32));
    errdefer device.free(&self.ctx);
    const data_bytes = std.mem.sliceAsBytes(data);
    const tried_pinned_scalar_upload = pinnedScalarUploadEligible(self, data_bytes);
    const copied_from_pinned = (try copyFromPinnedScalarUploadRing(self, device, data_bytes)) or
        (try copyFromPinnedUploadArena(self, device, data_bytes));
    if (!copied_from_pinned) {
        if (tried_pinned_scalar_upload) self.stats.pinned_scalar_upload_fallbacks += 1;
        try copyFromHostTracked(self, device, data_bytes);
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.upload_syncs += 1;
    }
    self.stats.from_float32_calls += 1;
    const byte_len = data.len * @sizeOf(i32);
    self.stats.from_float32_bytes += byte_len;
    noteUploadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.upload_top_sizes, &self.stats.upload_top_counts, byte_len);

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = .i32,
        .shape = shape_i64,
        .elem_count = elem_count,
    };
    return tensor;
}

fn toFloat32Op(ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]f32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    if (platform.env.getenvBoolDefault("TERMITE_CUDA_TRACE_TRAINING_DOWNLOADS", false)) {
        std.debug.print(
            "cuda_training_download: dtype={s} shape={any} elements={} bytes={}\n",
            .{ @tagName(cuda_tensor.dtype), cuda_tensor.shape, cuda_tensor.elem_count, cuda_tensor.elem_count * @sizeOf(f32) },
        );
    }
    const out = try allocator.alloc(f32, cuda_tensor.elem_count);
    errdefer allocator.free(out);
    const byte_len = try downloadTensorToFloat32(self, cuda_tensor, allocator, out);
    self.stats.to_float32_calls += 1;
    self.stats.to_float32_bytes += byte_len;
    noteDownloadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.download_top_sizes, &self.stats.download_top_counts, byte_len);
    return out;
}

fn exportTensorDataOp(ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror!?ops.ExportTensorData {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    if (cuda_tensor.quant_type != null) return null;

    const byte_len = try checkedMul(cuda_tensor.elem_count, cuda_tensor.dtype.byteSize());
    if (byte_len > cuda_tensor.buffer.len) return error.InvalidTensorShape;
    const bytes = try allocator.alloc(u8, byte_len);
    errdefer allocator.free(bytes);
    if (byte_len > 0) {
        const logical_buffer = buffer_mod.DeviceBuffer{
            .ptr = cuda_tensor.buffer.ptr,
            .len = byte_len,
        };
        try copyToHostTrackedAndSync(self, logical_buffer, bytes);
    }
    noteDownloadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.download_top_sizes, &self.stats.download_top_counts, byte_len);
    return .{
        .dtype = cuda_tensor.dtype,
        .payload = .{ .bytes = bytes },
    };
}

fn downloadTensorToFloat32(
    self: *CudaCompute,
    cuda_tensor: *const CudaTensor,
    allocator: std.mem.Allocator,
    out: []f32,
) !usize {
    if (out.len != cuda_tensor.elem_count) return error.InvalidTensorShape;

    if (cuda_tensor.quant_type) |quant_type| {
        const raw = try allocator.alloc(u8, cuda_tensor.buffer.len);
        defer allocator.free(raw);
        try copyToHostTrackedAndSync(self, cuda_tensor.buffer, raw);
        try quant_codec.dequantizeToFloat32(quant_type, raw, out);
        return raw.len;
    }

    switch (cuda_tensor.dtype) {
        .f32 => {
            try copyToHostTrackedAndSync(self, cuda_tensor.buffer, std.mem.sliceAsBytes(out));
            return cuda_tensor.elem_count * @sizeOf(f32);
        },
        .i32 => {
            const ints = try allocator.alloc(i32, cuda_tensor.elem_count);
            defer allocator.free(ints);
            try copyToHostTrackedAndSync(self, cuda_tensor.buffer, std.mem.sliceAsBytes(ints));
            for (ints, out) |value, *dst| dst.* = @floatFromInt(value);
            return cuda_tensor.elem_count * @sizeOf(i32);
        },
        .f16 => {
            const halves = try allocator.alloc(u16, cuda_tensor.elem_count);
            defer allocator.free(halves);
            try copyToHostTrackedAndSync(self, cuda_tensor.buffer, std.mem.sliceAsBytes(halves));
            for (halves, out) |bits, *dst| {
                const value: f16 = @bitCast(bits);
                dst.* = @floatCast(value);
            }
            return cuda_tensor.elem_count * @sizeOf(u16);
        },
        .bf16 => {
            const halves = try allocator.alloc(u16, cuda_tensor.elem_count);
            defer allocator.free(halves);
            try copyToHostTrackedAndSync(self, cuda_tensor.buffer, std.mem.sliceAsBytes(halves));
            for (halves, out) |bits, *dst| dst.* = @bitCast(@as(u32, bits) << 16);
            return cuda_tensor.elem_count * @sizeOf(u16);
        },
        else => return error.UnsupportedTensorType,
    }
}

fn tensorDTypeOp(_: *anyopaque, tensor: CT) anyerror!tensor_mod.DType {
    return tensorFromCt(tensor).dtype;
}

fn tensorShapeOp(_: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]i64 {
    return allocator.dupe(i64, tensorFromCt(tensor).shape);
}

fn copyTensorFromBackendOp(ctx: *anyopaque, src_ctx: *anyopaque, src_kind: ops.BackendKind, src_tensor_ct: CT) anyerror!?CT {
    if (src_kind != .cuda) return null;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const src_self: *CudaCompute = @ptrCast(@alignCast(src_ctx));
    const src_tensor = tensorFromCt(src_tensor_ct);
    if (src_tensor.buffer.ptr == 0 and src_tensor.buffer.len != 0) return error.InvalidTensorShape;

    const shape = try dupeShape(self.allocator, src_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, src_tensor.buffer.len);
    errdefer device.free(&self.ctx);
    if (src_self == self) {
        try copyFromDeviceTracked(self, device, src_tensor.buffer, src_tensor.buffer.len);
    } else {
        self.stats.cross_backend_copies += 1;
        self.stats.cross_backend_copy_bytes += src_tensor.buffer.len;
        if (cudaForceCrossBackendCopySync()) {
            self.stats.cross_backend_sync_fallbacks += 1;
            try synchronizeAndDrainDeferredDeviceFrees(src_self);
            try copyFromDeviceTracked(self, device, src_tensor.buffer, src_tensor.buffer.len);
        } else {
            const source_ready = try src_self.ctx.createStreamEvent();
            defer src_self.ctx.destroyEvent(source_ready);
            const copy_done = try self.ctx.createStreamEvent();
            defer self.ctx.destroyEvent(copy_done);

            try src_self.ctx.recordEvent(source_ready);
            self.stats.cross_backend_event_records += 1;
            try self.ctx.waitEvent(source_ready);
            self.stats.cross_backend_event_waits += 1;
            try copyFromDeviceTracked(self, device, src_tensor.buffer, src_tensor.buffer.len);
            try self.ctx.recordEvent(copy_done);
            self.stats.cross_backend_event_records += 1;
            try src_self.ctx.waitEvent(copy_done);
            self.stats.cross_backend_event_waits += 1;
        }
    }

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = src_tensor.dtype,
        .shape = shape,
        .elem_count = src_tensor.elem_count,
        .quant_type = src_tensor.quant_type,
    };
    return tensor;
}

fn evalTensorOp(ctx: *anyopaque, _: CT) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.stats.eval_requests += 1;
    if (!cudaForceEvalSync()) {
        self.stats.eval_skipped_eager += 1;
        return;
    }
    try synchronizeAndDrainDeferredDeviceFrees(self);
    self.stats.eval_syncs += 1;
    self.stats.eval_forced_syncs += 1;
}

fn debugCudaDeviceWarmup(ctx: *anyopaque, bytes: usize, iterations: usize) !bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (bytes == 0 or iterations == 0) return false;
    const elem_count = @max(bytes / @sizeOf(f32), 1);
    var buffer = try buffer_mod.DeviceBuffer.alloc(&self.ctx, elem_count * @sizeOf(f32));
    self.noteDeviceBytes(buffer.len);
    self.stats.device_alloc_calls += 1;
    defer {
        self.stats.device_free_calls += 1;
        buffer.free(&self.ctx);
    }
    var iteration: usize = 0;
    while (iteration < iterations) : (iteration += 1) {
        const value: f32 = @as(f32, @floatFromInt(iteration % 251)) * 0.001;
        try self.kernels.launchFillF32(&self.ctx, buffer, elem_count, value);
    }
    try synchronizeAndDrainDeferredDeviceFrees(self);
    return true;
}

fn createTensor(
    self: *CudaCompute,
    device: buffer_mod.DeviceBuffer,
    shape: []i64,
    elem_count: usize,
) !CT {
    return createTensorWithDType(self, device, shape, elem_count, .f32);
}

fn createTensorWithDType(
    self: *CudaCompute,
    device: buffer_mod.DeviceBuffer,
    shape: []i64,
    elem_count: usize,
    dtype: tensor_mod.DType,
) !CT {
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = dtype,
        .shape = shape,
        .elem_count = elem_count,
    };
    return tensor;
}

fn createI32ScalarTensorFromLease(self: *CudaCompute, lease: *DeviceBufferLease) !CT {
    if (lease.prepared_greedy_slot) |slot_idx| {
        if (self.debug_cuda_graph_prepared_output_kind != .greedy_token or
            self.debug_cuda_graph_prepared_slot != slot_idx or
            self.debug_cuda_graph_slots[slot_idx].output_storage.ptr != lease.buffer.ptr)
        {
            return error.InvalidCudaState;
        }
    }
    const shape = try self.allocator.alloc(i64, 1);
    errdefer self.allocator.free(shape);
    shape[0] = 1;
    const tensor = try createTensorWithDType(self, lease.buffer, shape, 1, .i32);
    tensorFromCt(tensor).owns_buffer = lease.owns_buffer;
    if (lease.prepared_greedy_slot != null) {
        self.debug_cuda_graph_prepared_output_kind = .tensor;
    }
    lease.owns_buffer = false;
    lease.prepared_greedy_slot = null;
    return tensor;
}

fn createTensorWithDTypeAndBf16Mirror(
    self: *CudaCompute,
    device: buffer_mod.DeviceBuffer,
    bf16_mirror: buffer_mod.DeviceBuffer,
    shape: []i64,
    elem_count: usize,
    dtype: tensor_mod.DType,
) !CT {
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .bf16_mirror = bf16_mirror,
        .dtype = dtype,
        .shape = shape,
        .elem_count = elem_count,
    };
    return tensor;
}

fn convertDTypeOp(ctx: *anyopaque, tensor_ct: CT, target: ops.GraphDType) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const tensor = tensorFromCt(tensor_ct);
    try ensureF32(tensor);
    const shape = try dupeShape(self.allocator, tensor.shape);
    errdefer self.allocator.free(shape);
    switch (target) {
        .i32 => {
            var device = try allocDeviceBuffer(self, try checkedMul(tensor.elem_count, @sizeOf(i32)));
            errdefer device.free(&self.ctx);
            try self.kernels.launchF32ToI32(&self.ctx, device, tensor.buffer, tensor.elem_count);
            return createTensorWithDType(self, device, shape, tensor.elem_count, .i32);
        },
        // Match the interpreter's established representation for the other
        // integer-like graph dtypes: rounded values in an f32 device buffer.
        .i64, .u8, .bool_ => {
            var device = try allocDeviceBuffer(self, try checkedMul(tensor.elem_count, @sizeOf(f32)));
            errdefer device.free(&self.ctx);
            try self.kernels.launchRoundF32(&self.ctx, device, tensor.buffer, tensor.elem_count);
            return createTensor(self, device, shape, tensor.elem_count);
        },
        else => {
            self.allocator.free(shape);
            return null;
        },
    }
}

fn zeroTensorOp(ctx: *anyopaque, rows: usize, dim: usize) anyerror!?CT {
    if (rows == 0 or dim == 0) return null;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const elem_count = try checkedMul(rows, dim);
    const shape = try allocShape2(self.allocator, rows, dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchFillF32(&self.ctx, device, elem_count, 0.0);
    return createTensor(self, device, shape, elem_count);
}

fn trainingOverwriteF32Op(ctx: *anyopaque, tensor_ct: CT, data: []const f32, shape_i32: []const i32) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const tensor = tensorFromCt(tensor_ct);
    try ensureF32(tensor);
    if (data.len != tensor.elem_count or shape_i32.len != tensor.shape.len) return error.InvalidShape;
    var elem_count: usize = 1;
    for (shape_i32, 0..) |dim, idx| {
        if (dim < 0 or tensor.shape[idx] != @as(i64, dim)) return error.InvalidShape;
        elem_count = try checkedMul(elem_count, @intCast(dim));
    }
    if (elem_count != tensor.elem_count) return error.InvalidShape;
    if (tensor.training_upload_generation == self.training_upload_generation) {
        return error.TrainingUploadStillPending;
    }
    const data_bytes = std.mem.sliceAsBytes(data);
    if (tensor.training_upload_host.len != data_bytes.len) {
        tensor.training_upload_host.free(&self.ctx);
        tensor.training_upload_host = try buffer_mod.HostBuffer.alloc(&self.ctx, data_bytes.len);
    }
    @memcpy(tensor.training_upload_host.bytes(), data_bytes);
    try copyFromHostTracked(self, tensor.buffer, tensor.training_upload_host.constBytes());
    tensor.training_upload_generation = self.training_upload_generation;
    self.stats.training_input_uploads += 1;
    self.stats.training_input_upload_bytes += data_bytes.len;
}

fn trainingZeroF32Op(ctx: *anyopaque, elem_count: usize, shape_i32: []const i32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var shape_count: usize = 1;
    const shape = try self.allocator.alloc(i64, shape_i32.len);
    errdefer self.allocator.free(shape);
    for (shape_i32, 0..) |dim, idx| {
        if (dim < 0) return error.InvalidShape;
        shape[idx] = dim;
        shape_count = try checkedMul(shape_count, @intCast(dim));
    }
    if (shape_count != elem_count) return error.InvalidShape;
    var device = try allocDeviceBuffer(self, elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchFillF32(&self.ctx, device, elem_count, 0.0);
    return createTensor(self, device, shape, elem_count);
}

fn trainingAccumulateF32Op(ctx: *anyopaque, accum_ct: CT, grad_ct: CT, elem_count: usize, scale: f32, first: bool) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const accum = tensorFromCt(accum_ct);
    const grad = tensorFromCt(grad_ct);
    try ensureF32(accum);
    try ensureF32(grad);
    if (elem_count != accum.elem_count or elem_count != grad.elem_count) return error.InvalidShape;
    try self.kernels.launchTrainingAccumulateF32(&self.ctx, accum.buffer, grad.buffer, elem_count, scale, first);
}

fn trainingAdamWManyF32Op(ctx: *anyopaque, inputs: []const ops.TrainingAdamWBatchInput, opts: ops.TrainingAdamWBatchOptions) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    for (inputs) |input| {
        const weight = tensorFromCt(input.weight);
        const grad = tensorFromCt(input.grad);
        const m = tensorFromCt(input.m);
        const v = tensorFromCt(input.v);
        try ensureF32(weight);
        try ensureF32(grad);
        try ensureF32(m);
        try ensureF32(v);
        if (input.elem_count != weight.elem_count or
            input.elem_count != grad.elem_count or
            input.elem_count != m.elem_count or
            input.elem_count != v.elem_count) return error.InvalidShape;
        if (input.bias_correction1 <= 0.0 or input.bias_correction2 <= 0.0) return error.InvalidOptimizerState;
        try self.kernels.launchTrainingAdamWF32(
            &self.ctx,
            weight.buffer,
            grad.buffer,
            m.buffer,
            v.buffer,
            input.elem_count,
            opts.lr,
            opts.beta1,
            opts.beta2,
            opts.eps,
            opts.weight_decay,
            input.bias_correction1,
            input.bias_correction2,
            opts.grad_scale,
        );
    }
}

fn trainingSumSquaresManyF32Op(ctx: *anyopaque, inputs: []const ops.TrainingSumSquaresInput) anyerror!f32 {
    if (inputs.len == 0) return 0.0;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const scalar_shape = [_]i32{1};
    const output = try trainingZeroF32Op(ctx, 1, &scalar_shape);
    defer freeTensor(ctx, output);
    const output_tensor = tensorFromCt(output);
    for (inputs) |input| {
        const tensor = tensorFromCt(input.tensor);
        try ensureF32(tensor);
        if (input.elem_count != tensor.elem_count) return error.InvalidShape;
        try self.kernels.launchTrainingSumSquaresF32(&self.ctx, output_tensor.buffer, tensor.buffer, input.elem_count);
    }
    const host = try toFloat32Op(ctx, output, self.allocator);
    defer self.allocator.free(host);
    return if (host.len == 1) host[0] else error.InvalidShape;
}

fn trainingSynchronizeOp(ctx: *anyopaque) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try synchronizeAndDrainDeferredDeviceFrees(self);
}

fn maskedBceWithLogitsLossOp(ctx: *anyopaque, request: *const ops.MaskedBceWithLogitsRequest) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const logits = tensorFromCt(request.logits);
    const labels = tensorFromCt(request.labels);
    const mask = tensorFromCt(request.mask);
    try ensureF32(logits);
    try ensureF32(labels);
    try ensureF32(mask);
    if (logits.elem_count == 0 or labels.elem_count != logits.elem_count or mask.elem_count != logits.elem_count) return error.InvalidShape;
    var output_elements: usize = 1;
    for (request.output_shape) |dim| {
        if (dim <= 0) return error.InvalidShape;
        output_elements = try checkedMul(output_elements, @intCast(dim));
    }
    if (output_elements != 1) return error.InvalidShape;
    const shape = try self.allocator.dupe(i64, request.output_shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, 2 * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchFillF32(&self.ctx, device, 2, 0.0);
    try self.kernels.launchMaskedBceAccumulateF32(
        &self.ctx,
        device,
        logits.buffer,
        labels.buffer,
        mask.buffer,
        logits.elem_count,
        request.positive_weight,
        request.negative_weight,
    );
    try self.kernels.launchMaskedBceFinalizeF32(&self.ctx, device, request.eps, request.mean_reduction);
    return createTensor(self, device, shape, 1);
}

fn maskedBceWithLogitsBackwardOp(ctx: *anyopaque, request: *const ops.MaskedBceWithLogitsBackwardRequest) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const logits = tensorFromCt(request.logits);
    const labels = tensorFromCt(request.labels);
    const mask = tensorFromCt(request.mask);
    const upstream = tensorFromCt(request.upstream);
    try ensureF32(logits);
    try ensureF32(labels);
    try ensureF32(mask);
    try ensureF32(upstream);
    if (logits.elem_count == 0 or labels.elem_count != logits.elem_count or mask.elem_count != logits.elem_count or upstream.elem_count == 0) return error.InvalidShape;
    const shape = try dupeShape(self.allocator, logits.shape);
    errdefer self.allocator.free(shape);
    var accum = try allocDeviceBuffer(self, 2 * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &accum);
    try self.kernels.launchFillF32(&self.ctx, accum, 2, 0.0);
    if (request.mean_reduction) {
        try self.kernels.launchMaskedBceAccumulateF32(
            &self.ctx,
            accum,
            logits.buffer,
            labels.buffer,
            mask.buffer,
            logits.elem_count,
            request.positive_weight,
            request.negative_weight,
        );
    }
    var device = try allocDeviceBuffer(self, logits.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchMaskedBceBackwardF32(
        &self.ctx,
        device,
        logits.buffer,
        labels.buffer,
        mask.buffer,
        upstream.buffer,
        accum,
        logits.elem_count,
        request.positive_weight,
        request.negative_weight,
        request.eps,
        request.mean_reduction,
    );
    return createTensor(self, device, shape, logits.elem_count);
}

fn primitiveReduceF32(
    ctx: *anyopaque,
    input_ct: CT,
    axes: []const u8,
    input_shape: []const i64,
    mode: enum(u32) { sum = 0, max = 1, mean = 2 },
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input = tensorFromCt(input_ct);
    try ensureF32(input);
    const rank = input_shape.len;
    if (rank == 0 or rank > 8 or input.shape.len != rank) return error.InvalidShape;
    var dims = [_]u32{1} ** 8;
    var input_count: usize = 1;
    for (0..rank) |idx| {
        const dim_i64 = input.shape[idx];
        if (dim_i64 <= 0 or dim_i64 > std.math.maxInt(u32)) return error.InvalidShape;
        dims[idx] = @intCast(dim_i64);
        input_count = try checkedMul(input_count, @intCast(dim_i64));
    }
    if (input_count != input.elem_count) return error.InvalidShape;

    var reduce_mask: u32 = 0;
    var reduce_count: usize = 1;
    for (axes) |axis| {
        if (axis >= rank) return error.InvalidShape;
        const bit = @as(u32, 1) << @intCast(axis);
        if ((reduce_mask & bit) != 0) continue;
        reduce_mask |= bit;
        reduce_count = try checkedMul(reduce_count, dims[axis]);
    }
    if (reduce_mask == 0) return (try cloneTensorShapeOp(ctx, input_ct, blk: {
        var shape_buf: [8]i32 = undefined;
        for (input.shape, 0..) |dim, idx| shape_buf[idx] = @intCast(dim);
        break :blk shape_buf[0..rank];
    })) orelse error.InvalidShape;

    const output_shape = try self.allocator.alloc(i64, rank);
    errdefer self.allocator.free(output_shape);
    var output_count: usize = 1;
    for (0..rank) |idx| {
        output_shape[idx] = if ((reduce_mask & (@as(u32, 1) << @intCast(idx))) != 0) 1 else input.shape[idx];
        output_count = try checkedMul(output_count, @intCast(output_shape[idx]));
    }
    var device = try allocDeviceBuffer(self, output_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchPrimitiveReduceF32(
        &self.ctx,
        device,
        input.buffer,
        input_count,
        output_count,
        dims,
        rank,
        reduce_mask,
        @intFromEnum(mode),
        reduce_count,
    );
    return createTensor(self, device, output_shape, output_count);
}

fn primReduceSumOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
    return primitiveReduceF32(ctx, input, axes, input_shape, .sum);
}

fn primReduceMaxOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
    return primitiveReduceF32(ctx, input, axes, input_shape, .max);
}

fn primReduceMeanOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
    return primitiveReduceF32(ctx, input, axes, input_shape, .mean);
}

fn primBroadcastInDimOp(ctx: *anyopaque, input_ct: CT, target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input = tensorFromCt(input_ct);
    try ensureF32(input);
    // The graph uses rank-0 scalars, while CUDA storage represents them as a
    // one-element buffer with shape [1].  Broadcast semantics follow the
    // graph-declared logical shape, not that storage shape.
    const input_rank = input_shape.len;
    const output_rank = target_shape.len;
    if (input_rank > 8 or output_rank == 0 or output_rank > 8) return error.InvalidShape;
    if (broadcast_axes.len < input_rank) return error.InvalidShape;

    var input_dims = [_]u32{1} ** 8;
    var output_dims = [_]u32{1} ** 8;
    var axes = [_]u32{0} ** 8;
    var input_count: usize = 1;
    var seen_axes: u32 = 0;
    for (0..input_rank) |idx| {
        var dim = input_shape[idx];
        if (dim <= 0 and idx < input.shape.len) dim = input.shape[idx];
        const axis = broadcast_axes[idx];
        if (dim <= 0 or dim > std.math.maxInt(u32) or axis >= output_rank) return error.InvalidShape;
        const bit = @as(u32, 1) << @intCast(axis);
        if ((seen_axes & bit) != 0) return error.InvalidShape;
        seen_axes |= bit;
        input_dims[idx] = @intCast(dim);
        axes[idx] = axis;
        input_count = try checkedMul(input_count, @intCast(dim));
    }
    if (input_count != input.elem_count) return error.InvalidShape;

    const output_shape = try self.allocator.alloc(i64, output_rank);
    errdefer self.allocator.free(output_shape);
    var output_count: usize = 1;
    for (0..output_rank) |idx| {
        var dim = target_shape[idx];
        if (dim <= 0) {
            for (0..input_rank) |in_idx| {
                if (axes[in_idx] == idx) {
                    dim = @intCast(input_dims[in_idx]);
                    break;
                }
            }
        }
        if (dim <= 0) dim = 1;
        if (dim > std.math.maxInt(u32)) return error.InvalidShape;
        output_shape[idx] = dim;
        output_dims[idx] = @intCast(dim);
        output_count = try checkedMul(output_count, @intCast(dim));
    }
    for (0..input_rank) |idx| {
        const target_dim = output_dims[axes[idx]];
        if (input_dims[idx] != 1 and (target_dim < input_dims[idx] or target_dim % input_dims[idx] != 0)) return error.InvalidShape;
    }

    var device = try allocDeviceBuffer(self, output_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchPrimitiveBroadcastF32(
        &self.ctx,
        device,
        input.buffer,
        input_count,
        output_count,
        input_dims,
        output_dims,
        axes,
        input_rank,
        output_rank,
    );
    return createTensor(self, device, output_shape, output_count);
}

fn primDotGeneralOp(
    ctx: *anyopaque,
    lhs_ct: CT,
    rhs_ct: CT,
    lhs_shape: []const i64,
    rhs_shape: []const i64,
    lhs_contracting: []const u8,
    rhs_contracting: []const u8,
    lhs_batch: []const u8,
    rhs_batch: []const u8,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const lhs = tensorFromCt(lhs_ct);
    const rhs = tensorFromCt(rhs_ct);
    try ensureF32(lhs);
    try ensureF32(rhs);
    if (lhs_contracting.len != 1 or rhs_contracting.len != 1) return error.UnsupportedShape;

    if (lhs_batch.len != 0 or rhs_batch.len != 0) {
        if (lhs_batch.len == 0 or lhs_batch.len != rhs_batch.len) return error.UnsupportedShape;
        if (lhs.shape.len != rhs.shape.len or lhs.shape.len != lhs_batch.len + 2) return error.UnsupportedShape;
        if (lhs_shape.len != lhs.shape.len or rhs_shape.len != rhs.shape.len or lhs.shape.len > 8) return error.UnsupportedShape;

        const rank = lhs.shape.len;
        const m_axis = rank - 2;
        const k_axis = rank - 1;
        if (lhs_contracting[0] != k_axis) return error.UnsupportedShape;
        const rhs_contract_axis: usize = rhs_contracting[0];
        if (rhs_contract_axis != m_axis and rhs_contract_axis != k_axis) return error.UnsupportedShape;

        var batch_count: usize = 1;
        for (lhs_batch, 0..) |lhs_axis, idx| {
            const rhs_axis = rhs_batch[idx];
            if (lhs_axis != idx or rhs_axis != idx) return error.UnsupportedShape;
            const batch_dim = lhs.shape[idx];
            if (batch_dim <= 0 or rhs.shape[idx] != batch_dim) return error.InvalidShape;
            batch_count = try checkedMul(batch_count, @intCast(batch_dim));
        }

        const m_i64 = lhs.shape[m_axis];
        const k_i64 = lhs.shape[k_axis];
        const rhs_k_i64 = rhs.shape[rhs_contract_axis];
        const rhs_free_axis = if (rhs_contract_axis == k_axis) m_axis else k_axis;
        const n_i64 = rhs.shape[rhs_free_axis];
        if (m_i64 <= 0 or k_i64 <= 0 or n_i64 <= 0 or rhs_k_i64 != k_i64) return error.InvalidShape;
        const m: usize = @intCast(m_i64);
        const k: usize = @intCast(k_i64);
        const n: usize = @intCast(n_i64);
        const lhs_count = try checkedMul(try checkedMul(batch_count, m), k);
        const rhs_count = try checkedMul(try checkedMul(batch_count, n), k);
        if (lhs.elem_count != lhs_count or rhs.elem_count != rhs_count) return error.InvalidShape;
        const output_count = try checkedMul(try checkedMul(batch_count, m), n);

        const output_shape = try dupeShape(self.allocator, lhs.shape);
        errdefer self.allocator.free(output_shape);
        output_shape[k_axis] = n_i64;
        var output_device = try allocDeviceBuffer(self, output_count * @sizeOf(f32));
        errdefer output_device.free(&self.ctx);
        try self.kernels.launchPrimitiveBatchedDotF32(
            &self.ctx,
            output_device,
            lhs.buffer,
            rhs.buffer,
            batch_count,
            m,
            n,
            k,
            rhs_contract_axis == k_axis,
        );
        self.stats.launch_linear += 1;
        return createTensor(self, output_device, output_shape, output_count);
    }

    if (lhs.shape.len < 2 or lhs.shape.len > 8 or rhs.shape.len != 2 or lhs_shape.len != lhs.shape.len or rhs_shape.len != 2) return error.UnsupportedShape;
    const lhs_axis = lhs_contracting[0];
    const rhs_axis = rhs_contracting[0];
    if (lhs_axis != lhs.shape.len - 1 or rhs_axis > 1) return error.UnsupportedShape;
    const k_i64 = lhs.shape[lhs_axis];
    if (k_i64 <= 0 or rhs.shape[rhs_axis] != k_i64) return error.InvalidShape;
    const k: usize = @intCast(k_i64);
    const n_i64 = rhs.shape[1 - rhs_axis];
    if (n_i64 <= 0) return error.InvalidShape;
    const n: usize = @intCast(n_i64);
    var rows: usize = 1;
    for (lhs.shape[0..lhs_axis]) |dim| {
        if (dim <= 0) return error.InvalidShape;
        rows = try checkedMul(rows, @intCast(dim));
    }

    var transposed_rhs: ?CT = null;
    defer if (transposed_rhs) |tensor| freeTensor(ctx, tensor);
    const weight = if (rhs_axis == 1)
        rhs_ct
    else blk: {
        const t = try transposeOp(ctx, rhs_ct, &.{ 1, 0 }, rhs.shape);
        transposed_rhs = t;
        break :blk t;
    };
    const output = try linearNoBias(ctx, lhs_ct, weight, rows, k, n);
    errdefer freeTensor(ctx, output);
    const output_tensor = tensorFromCt(output);
    const output_shape = try self.allocator.alloc(i64, lhs_axis + 1);
    errdefer self.allocator.free(output_shape);
    for (lhs.shape[0..lhs_axis], 0..) |dim, idx| output_shape[idx] = dim;
    output_shape[lhs_axis] = n_i64;
    if (output_tensor.owns_shape) self.allocator.free(output_tensor.shape);
    output_tensor.shape = output_shape;
    output_tensor.owns_shape = true;
    return output;
}

fn primitiveSoftmaxOp(ctx: *anyopaque, input_ct: CT, last_dim: u32, log_softmax: bool) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input = tensorFromCt(input_ct);
    try ensureF32(input);
    if (last_dim == 0 or input.elem_count % last_dim != 0) return error.InvalidShape;
    const shape = try dupeShape(self.allocator, input.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchPrimitiveSoftmaxF32(&self.ctx, device, input.buffer, input.elem_count, last_dim, log_softmax);
    return createTensor(self, device, shape, input.elem_count);
}

fn primSoftmaxOp(ctx: *anyopaque, input: CT, last_dim: u32) anyerror!CT {
    return primitiveSoftmaxOp(ctx, input, last_dim, false);
}

fn primLogSoftmaxOp(ctx: *anyopaque, input: CT, last_dim: u32) anyerror!CT {
    return primitiveSoftmaxOp(ctx, input, last_dim, true);
}

fn uploadOwnedHost(self: *CudaCompute, data: []f32, shape_src: []const i64) !CT {
    errdefer self.allocator.free(data);
    const elem_count = data.len;
    const shape = try self.allocator.dupe(i64, shape_src);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const data_bytes = std.mem.sliceAsBytes(data);
    const copied_from_pinned = (try copyFromPinnedScalarUploadRing(self, device, data_bytes)) or
        (try copyFromPinnedUploadArena(self, device, data_bytes));
    if (!copied_from_pinned) {
        try copyFromHostTracked(self, device, data_bytes);
        try synchronizeAndDrainDeferredDeviceFrees(self);
        self.stats.upload_syncs += 1;
    }
    self.stats.upload_owned_host_calls += 1;
    const byte_len = elem_count * @sizeOf(f32);
    self.stats.upload_owned_host_bytes += byte_len;
    noteUploadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.upload_top_sizes, &self.stats.upload_top_counts, byte_len);
    self.allocator.free(data);
    return createTensor(self, device, shape, elem_count);
}

fn reshapeOp(ctx: *anyopaque, input: CT, new_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    var elem_count: usize = 1;
    for (new_shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @as(usize, @intCast(dim)));
    }
    if (elem_count != input_tensor.elem_count) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=reshape input_elems={d} requested_elems={d} new_rank={d}", .{ input_tensor.elem_count, elem_count, new_shape.len });
        return error.InvalidShape;
    }

    const shape = try self.allocator.dupe(i64, new_shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try copyFromDeviceTracked(self, device, input_tensor.buffer, input_tensor.elem_count * @sizeOf(f32));
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn reshape2DOp(
    ctx: *anyopaque,
    input: CT,
    old_rows: usize,
    old_cols: usize,
    new_rows: usize,
    new_cols: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const new_count = try std.math.mul(usize, new_rows, new_cols);
    if (new_count != input_tensor.elem_count) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=reshape2D input_elems={d} new_rows={d} new_cols={d}", .{ input_tensor.elem_count, new_rows, new_cols });
        return error.InvalidShape;
    }
    if (old_rows != 0 or old_cols != 0) {
        const old_count = try std.math.mul(usize, old_rows, old_cols);
        if (old_count != new_count) {
            if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=reshape2D old_rows={d} old_cols={d} new_rows={d} new_cols={d}", .{ old_rows, old_cols, new_rows, new_cols });
            return error.InvalidShape;
        }
    }
    const shape = try self.allocator.dupe(i64, &[_]i64{ @intCast(new_rows), @intCast(new_cols) });
    errdefer self.allocator.free(shape);
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = input_tensor.buffer,
        .dtype = input_tensor.dtype,
        .shape = shape,
        .elem_count = input_tensor.elem_count,
        .quant_type = input_tensor.quant_type,
        .owns_buffer = false,
        .owns_shape = true,
    };
    return tensor;
}

fn reshape2dOp(ctx: *anyopaque, input: CT, rows: usize, cols: usize) anyerror!CT {
    const input_tensor = tensorFromCt(input);
    const requested = try std.math.mul(usize, rows, cols);
    if (requested != input_tensor.elem_count) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=reshape2d input_elems={d} rows={d} cols={d}", .{ input_tensor.elem_count, rows, cols });
        return error.InvalidShape;
    }
    return reshape2DOp(ctx, input, input_tensor.elem_count, 1, rows, cols);
}

fn sliceRows2DOp(
    ctx: *anyopaque,
    input: CT,
    start_row: usize,
    row_count: usize,
    cols: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (cols == 0) return error.InvalidShape;
    if (input_tensor.elem_count % cols != 0) return error.InvalidShape;
    const total_rows = input_tensor.elem_count / cols;
    if (start_row > total_rows or row_count > total_rows - start_row) return error.InvalidShape;

    const out_count = try checkedMul(row_count, cols);
    const byte_offset = try checkedMul(try checkedMul(start_row, cols), @sizeOf(f32));
    const byte_len = try checkedMul(out_count, @sizeOf(f32));
    if (byte_offset > input_tensor.buffer.len or byte_len > input_tensor.buffer.len - byte_offset) return error.InvalidShape;

    const shape = try allocShape2(self.allocator, row_count, cols);
    errdefer self.allocator.free(shape);
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = .{
            .ptr = input_tensor.buffer.ptr + @as(u64, @intCast(byte_offset)),
            .len = byte_len,
        },
        .dtype = input_tensor.dtype,
        .shape = shape,
        .elem_count = out_count,
        .quant_type = input_tensor.quant_type,
        .owns_buffer = false,
        .owns_shape = true,
    };
    return tensor;
}

fn allocUninitF32ShapeOp(ctx: *anyopaque, shape_i32: []const i32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (shape_i32.len == 0) return error.InvalidShape;
    var elem_count: usize = 1;
    const shape = try self.allocator.alloc(i64, shape_i32.len);
    errdefer self.allocator.free(shape);
    for (shape_i32, 0..) |dim, idx| {
        if (dim <= 0) return error.InvalidShape;
        shape[idx] = @intCast(dim);
        elem_count = try checkedMul(elem_count, @as(usize, @intCast(dim)));
    }
    const byte_len = try checkedMul(elem_count, @sizeOf(f32));
    var device = try allocDeviceBuffer(self, byte_len);
    errdefer device.free(&self.ctx);
    return try createTensor(self, device, shape, elem_count);
}

fn copyRows2DOp(
    ctx: *anyopaque,
    dst: CT,
    dst_start_row: usize,
    src: CT,
    src_start_row: usize,
    row_count: usize,
    cols: usize,
) anyerror!bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const dst_tensor = tensorFromCt(dst);
    const src_tensor = tensorFromCt(src);
    try ensureF32(dst_tensor);
    try ensureF32(src_tensor);
    if (cols == 0) return error.InvalidShape;
    if (dst_tensor.elem_count % cols != 0 or src_tensor.elem_count % cols != 0) return error.InvalidShape;
    const dst_rows = dst_tensor.elem_count / cols;
    const src_rows = src_tensor.elem_count / cols;
    if (dst_start_row > dst_rows or row_count > dst_rows - dst_start_row) return error.InvalidShape;
    if (src_start_row > src_rows or row_count > src_rows - src_start_row) return error.InvalidShape;
    if (row_count == 0) return true;

    const row_bytes = try checkedMul(cols, @sizeOf(f32));
    const byte_len = try checkedMul(row_count, row_bytes);
    const dst_offset = try checkedMul(dst_start_row, row_bytes);
    const src_offset = try checkedMul(src_start_row, row_bytes);
    if (dst_offset > dst_tensor.buffer.len or byte_len > dst_tensor.buffer.len - dst_offset) return error.InvalidShape;
    if (src_offset > src_tensor.buffer.len or byte_len > src_tensor.buffer.len - src_offset) return error.InvalidShape;

    const dst_buffer = buffer_mod.DeviceBuffer{
        .ptr = dst_tensor.buffer.ptr + @as(u64, @intCast(dst_offset)),
        .len = byte_len,
    };
    const src_buffer = buffer_mod.DeviceBuffer{
        .ptr = src_tensor.buffer.ptr + @as(u64, @intCast(src_offset)),
        .len = byte_len,
    };
    try copyFromDeviceTracked(self, dst_buffer, src_buffer, byte_len);
    return true;
}

fn concatRows2DOp(
    ctx: *anyopaque,
    a: CT,
    b: CT,
    rows_a: usize,
    rows_b: usize,
    cols: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    if (cols == 0) return error.InvalidShape;

    const count_a = try checkedMul(rows_a, cols);
    const count_b = try checkedMul(rows_b, cols);
    if (a_tensor.elem_count != count_a or b_tensor.elem_count != count_b) return error.InvalidShape;

    const out_count = try checkedAdd(count_a, count_b);
    const byte_len_a = try checkedMul(count_a, @sizeOf(f32));
    const byte_len_b = try checkedMul(count_b, @sizeOf(f32));
    const out_byte_len = try checkedAdd(byte_len_a, byte_len_b);
    var device = try allocDeviceBuffer(self, out_byte_len);
    errdefer device.free(&self.ctx);

    if (byte_len_a > 0) try copyFromDeviceTracked(self, device, a_tensor.buffer, byte_len_a);
    if (byte_len_b > 0) {
        const dst_b = buffer_mod.DeviceBuffer{
            .ptr = device.ptr + @as(u64, @intCast(byte_len_a)),
            .len = byte_len_b,
        };
        try copyFromDeviceTracked(self, dst_b, b_tensor.buffer, byte_len_b);
    }

    const shape = try allocShape2(self.allocator, rows_a + rows_b, cols);
    errdefer self.allocator.free(shape);
    return try createTensor(self, device, shape, out_count);
}

const FlorenceTailCacheSlot = enum { row, col, temporal };

fn cachedFlorenceTailF32(
    self: *CudaCompute,
    slot: FlorenceTailCacheSlot,
    source: *const CudaTensor,
) !CT {
    const cached: *?CT = switch (slot) {
        .row => &self.florence_tail_row_f32,
        .col => &self.florence_tail_col_f32,
        .temporal => &self.florence_tail_temporal_f32,
    };
    const source_ptr: *driver_mod.CUdeviceptr = switch (slot) {
        .row => &self.florence_tail_row_source_ptr,
        .col => &self.florence_tail_col_source_ptr,
        .temporal => &self.florence_tail_temporal_source_ptr,
    };
    if (cached.*) |tensor| {
        if (source_ptr.* == source.buffer.ptr) return tensor;
        freeTensor(self, tensor);
        cached.* = null;
        source_ptr.* = 0;
    }

    const data = try downloadAlloc(self, source);
    defer self.allocator.free(data);
    const shape = [_]i32{@intCast(data.len)};
    const tensor = try fromFloat32ShapeOp(self, data, &shape);
    cached.* = tensor;
    source_ptr.* = source.buffer.ptr;
    return tensor;
}

fn cachedFlorenceImageProjectionTransposedF32(
    self: *CudaCompute,
    source: *const CudaTensor,
    vision_dim: usize,
    projection_dim: usize,
) !CT {
    if (self.florence_image_projection_t_f32) |tensor| {
        if (self.florence_image_projection_source_ptr == source.buffer.ptr and
            self.florence_image_projection_vision_dim == vision_dim and
            self.florence_image_projection_projection_dim == projection_dim)
        {
            return tensor;
        }
        freeTensor(self, tensor);
        self.florence_image_projection_t_f32 = null;
        self.florence_image_projection_source_ptr = 0;
        self.florence_image_projection_vision_dim = 0;
        self.florence_image_projection_projection_dim = 0;
    }

    const expected = try checkedMul(vision_dim, projection_dim);
    if (source.elem_count != expected) return error.InvalidShape;
    const data = try downloadAlloc(self, source);
    defer self.allocator.free(data);
    const transposed = try self.allocator.alloc(f32, expected);
    defer self.allocator.free(transposed);
    for (0..vision_dim) |i| {
        for (0..projection_dim) |j| {
            transposed[j * vision_dim + i] = data[i * projection_dim + j];
        }
    }
    const shape = [_]i32{ @intCast(projection_dim), @intCast(vision_dim) };
    const tensor = try fromFloat32ShapeOp(self, transposed, &shape);
    self.florence_image_projection_t_f32 = tensor;
    self.florence_image_projection_source_ptr = source.buffer.ptr;
    self.florence_image_projection_vision_dim = vision_dim;
    self.florence_image_projection_projection_dim = projection_dim;
    return tensor;
}

fn florenceVisionTailSourcesOp(
    ctx: *anyopaque,
    tokens: CT,
    row_embed: CT,
    col_embed: CT,
    temporal_embed: ?CT,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const token_tensor = tensorFromCt(tokens);
    var row_tensor = tensorFromCt(row_embed);
    var col_tensor = tensorFromCt(col_embed);
    try ensureF32(token_tensor);
    const row_dtype = florenceTailWeightDTypeCode(row_tensor) catch |err| switch (err) {
        error.UnsupportedTensorType => blk: {
            const cached = try cachedFlorenceTailF32(self, .row, row_tensor);
            row_tensor = tensorFromCt(cached);
            break :blk @as(u32, 0);
        },
    };
    const col_dtype = florenceTailWeightDTypeCode(col_tensor) catch |err| switch (err) {
        error.UnsupportedTensorType => blk: {
            const cached = try cachedFlorenceTailF32(self, .col, col_tensor);
            col_tensor = tensorFromCt(cached);
            break :blk @as(u32, 0);
        },
    };
    if (batch == 0 or height == 0 or width == 0 or dim == 0) return error.InvalidShape;

    const token_count = try checkedMul(height, width);
    const source_rows = try checkedMul(batch, token_count);
    if (token_tensor.elem_count != try checkedMul(source_rows, dim)) return error.InvalidShape;
    if (row_tensor.elem_count < try checkedMul(height, dim / 2)) return error.InvalidShape;
    if (col_tensor.elem_count < try checkedMul(width, dim - dim / 2)) return error.InvalidShape;

    var temporal_dtype: u32 = 0;
    const temporal_buffer = blk: {
        if (temporal_embed) |temporal| {
            var temporal_tensor = tensorFromCt(temporal);
            temporal_dtype = florenceTailWeightDTypeCode(temporal_tensor) catch |err| switch (err) {
                error.UnsupportedTensorType => cached_blk: {
                    const cached = try cachedFlorenceTailF32(self, .temporal, temporal_tensor);
                    temporal_tensor = tensorFromCt(cached);
                    break :cached_blk @as(u32, 0);
                },
            };
            if (temporal_tensor.elem_count < dim) return error.InvalidShape;
            break :blk temporal_tensor.buffer;
        }
        break :blk buffer_mod.DeviceBuffer{};
    };

    const out_seq = try checkedAdd(token_count, 1);
    const out_rows = try checkedMul(batch, out_seq);
    const out_count = try checkedMul(out_rows, dim);
    const out_byte_len = try checkedMul(out_count, @sizeOf(f32));
    var device = try allocDeviceBuffer(self, out_byte_len);
    errdefer device.free(&self.ctx);

    self.kernels.launchFlorenceVisionTailSourcesF32(
        &self.ctx,
        device,
        token_tensor.buffer,
        row_tensor.buffer,
        col_tensor.buffer,
        temporal_buffer,
        batch,
        height,
        width,
        dim,
        temporal_embed != null,
        row_dtype,
        col_dtype,
        temporal_dtype,
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable => {
            device.free(&self.ctx);
            return null;
        },
        else => return err,
    };

    const shape = try allocShape2(self.allocator, out_rows, dim);
    errdefer self.allocator.free(shape);
    return try createTensor(self, device, shape, out_count);
}

fn florenceProjectImageFeaturesOp(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    rows: usize,
    vision_dim: usize,
    projection_dim: usize,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    if (rows == 0 or vision_dim == 0 or projection_dim == 0) return error.InvalidShape;
    if (input_tensor.elem_count != try checkedMul(rows, vision_dim)) return error.InvalidShape;

    const transposed = try cachedFlorenceImageProjectionTransposedF32(self, weight_tensor, vision_dim, projection_dim);
    return try linearNoBias(ctx, input, transposed, rows, vision_dim, projection_dim);
}

fn sliceLastDimOp(ctx: *anyopaque, input: CT, start: usize, stop: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_tensor.shape.len != 2) return error.UnsupportedShape;
    const rows_i64 = input_tensor.shape[0];
    const cols_i64 = input_tensor.shape[1];
    if (rows_i64 < 0 or cols_i64 < 0) return error.InvalidShape;
    const rows: usize = @intCast(rows_i64);
    const cols: usize = @intCast(cols_i64);
    if (start > stop or stop > cols) return error.OutOfBounds;
    const out_cols = stop - start;
    try ensureCount(input_tensor, try checkedMul(rows, cols));

    const out_count = try checkedMul(rows, out_cols);
    const shape = try allocShape2(self.allocator, rows, out_cols);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchSliceLastDimF32(&self.ctx, device, input_tensor.buffer, rows, cols, start, out_cols);
    self.stats.launch_other += 1;
    return try createTensor(self, device, shape, out_count);
}

fn cloneTensorShapeOp(ctx: *anyopaque, input: CT, shape_i32: []const i32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    var elem_count: usize = 1;
    for (shape_i32) |dim| {
        if (dim <= 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @as(usize, @intCast(dim)));
    }
    if (elem_count != input_tensor.elem_count) return error.InvalidShape;

    const shape = try self.allocator.alloc(i64, shape_i32.len);
    errdefer self.allocator.free(shape);
    for (shape_i32, 0..) |dim, idx| shape[idx] = @intCast(dim);

    const byte_len = input_tensor.elem_count * @sizeOf(f32);
    var device = try allocDeviceBuffer(self, byte_len);
    errdefer device.free(&self.ctx);
    try copyFromDeviceTracked(self, device, input_tensor.buffer, byte_len);
    return try createTensor(self, device, shape, input_tensor.elem_count);
}

fn transposeOp(ctx: *anyopaque, input: CT, perm: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_shape.len == 0 or input_shape.len > 8 or perm.len != input_shape.len) return error.UnsupportedShape;

    var numel: usize = 1;
    var resolved_shape: [8]usize = undefined;
    for (input_shape, 0..) |dim, i| {
        if (dim <= 0) return error.UnsupportedShape;
        const value: usize = @intCast(dim);
        resolved_shape[i] = value;
        numel = try checkedMul(numel, value);
    }
    if (numel != input_tensor.elem_count) return error.InvalidShape;

    var seen = [_]bool{false} ** 8;
    var out_shape_usize: [8]usize = undefined;
    var out_shape_i64: [8]i64 = undefined;
    for (perm, 0..) |axis, i| {
        if (axis >= input_shape.len or seen[axis]) return error.InvalidShape;
        seen[axis] = true;
        out_shape_usize[i] = resolved_shape[axis];
        out_shape_i64[i] = @intCast(out_shape_usize[i]);
    }

    var dims = [_]u32{1} ** 8;
    var perm_u32 = [_]u32{0} ** 8;
    for (0..input_shape.len) |idx| {
        if (resolved_shape[idx] > std.math.maxInt(u32)) return error.InvalidShape;
        dims[idx] = @intCast(resolved_shape[idx]);
        perm_u32[idx] = perm[idx];
    }
    const output_shape = try self.allocator.dupe(i64, out_shape_i64[0..input_shape.len]);
    errdefer self.allocator.free(output_shape);
    var device = try allocDeviceBuffer(self, numel * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchPrimitiveTransposeF32(&self.ctx, device, input_tensor.buffer, numel, dims, perm_u32, input_shape.len);
    return createTensor(self, device, output_shape, numel);
}

fn primGatherOp(ctx: *anyopaque, input_ct: CT, indices_ct: CT, axis: u8, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input = tensorFromCt(input_ct);
    const indices = tensorFromCt(indices_ct);
    try ensureF32(input);
    try ensureF32(indices);
    const rank = input.shape.len;
    const axis_index: usize = axis;
    if (rank == 0 or rank > 8 or input_shape.len != rank or axis_index >= rank or indices.shape.len > 8) return error.InvalidShape;

    var prefix_count: usize = 1;
    for (input.shape[0..axis_index]) |dim| {
        if (dim <= 0) return error.InvalidShape;
        prefix_count = try checkedMul(prefix_count, @intCast(dim));
    }
    const axis_extent_i64 = input.shape[axis_index];
    if (axis_extent_i64 <= 0 or axis_extent_i64 > std.math.maxInt(u32)) return error.InvalidShape;
    const axis_extent: usize = @intCast(axis_extent_i64);
    var suffix_size: usize = 1;
    for (input.shape[axis_index + 1 ..]) |dim| {
        if (dim <= 0) return error.InvalidShape;
        suffix_size = try checkedMul(suffix_size, @intCast(dim));
    }
    const expected_input_count = try checkedMul(try checkedMul(prefix_count, axis_extent), suffix_size);
    if (expected_input_count != input.elem_count or indices.elem_count == 0) return error.InvalidShape;

    var normalize_scalar_indices = indices.shape.len > 1 and indices.elem_count == 1;
    if (normalize_scalar_indices) {
        for (indices.shape) |dim| {
            if (dim != 1) {
                normalize_scalar_indices = false;
                break;
            }
        }
    }
    const indices_rank: usize = if (normalize_scalar_indices) 1 else indices.shape.len;
    const output_rank = axis_index + indices_rank + (rank - axis_index - 1);
    if (output_rank == 0 or output_rank > 8) return error.UnsupportedShape;
    const output_shape = try self.allocator.alloc(i64, output_rank);
    errdefer self.allocator.free(output_shape);
    var out_dim: usize = 0;
    for (input.shape[0..axis_index]) |dim| {
        output_shape[out_dim] = dim;
        out_dim += 1;
    }
    if (normalize_scalar_indices) {
        output_shape[out_dim] = 1;
        out_dim += 1;
    } else {
        for (indices.shape) |dim| {
            if (dim <= 0) return error.InvalidShape;
            output_shape[out_dim] = dim;
            out_dim += 1;
        }
    }
    for (input.shape[axis_index + 1 ..]) |dim| {
        output_shape[out_dim] = dim;
        out_dim += 1;
    }

    const output_count = try checkedMul(try checkedMul(prefix_count, indices.elem_count), suffix_size);
    var device = try allocDeviceBuffer(self, output_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchPrimitiveGatherF32(
        &self.ctx,
        device,
        input.buffer,
        indices.buffer,
        output_count,
        input.elem_count,
        indices.elem_count,
        axis_extent,
        suffix_size,
    );
    return createTensor(self, device, output_shape, output_count);
}

fn primScatterAddOp(ctx: *anyopaque, input_ct: CT, indices_ct: CT, input_shape: []const i64, output_shape_declared: []const i64, axis: u8) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input = tensorFromCt(input_ct);
    const indices = tensorFromCt(indices_ct);
    try ensureF32(input);
    try ensureF32(indices);
    if (axis != 0 or input_shape.len != 2 or input.shape.len != 2 or output_shape_declared.len != 2) return error.UnsupportedShape;
    const rows_i64 = input.shape[0];
    const cols_i64 = input.shape[1];
    const output_rows_i64 = output_shape_declared[0];
    const output_cols_i64 = output_shape_declared[1];
    if (rows_i64 <= 0 or cols_i64 <= 0 or output_rows_i64 <= 0 or output_cols_i64 != cols_i64) return error.InvalidShape;
    const rows: usize = @intCast(rows_i64);
    const cols: usize = @intCast(cols_i64);
    const output_rows: usize = @intCast(output_rows_i64);
    if (input.elem_count != try checkedMul(rows, cols) or indices.elem_count != rows) return error.InvalidShape;
    const output_count = try checkedMul(output_rows, cols);
    const output_shape = try self.allocator.dupe(i64, output_shape_declared);
    errdefer self.allocator.free(output_shape);
    var device = try allocDeviceBuffer(self, output_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchFillF32(&self.ctx, device, output_count, 0.0);
    try self.kernels.launchPrimitiveScatterAddAxis0F32(&self.ctx, device, input.buffer, indices.buffer, rows, cols, output_rows);
    return createTensor(self, device, output_shape, output_count);
}

fn primConcatPrimOp(ctx: *anyopaque, a_ct: CT, b_ct: CT, axis: u8, a_shape: []const i64, b_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a = tensorFromCt(a_ct);
    const b = tensorFromCt(b_ct);
    try ensureF32(a);
    try ensureF32(b);
    const rank = a.shape.len;
    const axis_index: usize = axis;
    if (rank == 0 or rank > 8 or b.shape.len != rank or a_shape.len != rank or b_shape.len != rank or axis_index >= rank) return error.InvalidShape;
    var outer: usize = 1;
    var inner: usize = 1;
    for (0..rank) |idx| {
        const a_dim = a.shape[idx];
        const b_dim = b.shape[idx];
        if (a_dim <= 0 or b_dim <= 0) return error.InvalidShape;
        if (idx != axis_index and a_dim != b_dim) return error.InvalidShape;
        if (idx < axis_index) outer = try checkedMul(outer, @intCast(a_dim));
        if (idx > axis_index) inner = try checkedMul(inner, @intCast(a_dim));
    }
    const a_axis: usize = @intCast(a.shape[axis_index]);
    const b_axis: usize = @intCast(b.shape[axis_index]);
    const expected_a = try checkedMul(try checkedMul(outer, a_axis), inner);
    const expected_b = try checkedMul(try checkedMul(outer, b_axis), inner);
    if (a.elem_count != expected_a or b.elem_count != expected_b) return error.InvalidShape;
    const output_axis = try checkedAdd(a_axis, b_axis);
    const output_count = try checkedMul(try checkedMul(outer, output_axis), inner);
    const output_shape = try dupeShape(self.allocator, a.shape);
    errdefer self.allocator.free(output_shape);
    output_shape[axis_index] = @intCast(output_axis);
    var device = try allocDeviceBuffer(self, output_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchPrimitiveConcatF32(
        &self.ctx,
        device,
        a.buffer,
        b.buffer,
        output_count,
        a.elem_count,
        b.elem_count,
        a_axis,
        b_axis,
        inner,
    );
    return createTensor(self, device, output_shape, output_count);
}

fn primSliceOp(ctx: *anyopaque, input_ct: CT, starts_raw: []const i64, limits_raw: []const i64, strides_raw: []const i64, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input = tensorFromCt(input_ct);
    try ensureF32(input);
    const rank = input.shape.len;
    if (rank == 0 or rank > 8 or input_shape.len != rank or starts_raw.len < rank or limits_raw.len < rank or strides_raw.len < rank) return error.UnsupportedShape;
    var input_dims = [_]u32{1} ** 8;
    var output_dims = [_]u32{1} ** 8;
    var starts = [_]u32{0} ** 8;
    var strides = [_]u32{1} ** 8;
    const output_shape = try self.allocator.alloc(i64, rank);
    errdefer self.allocator.free(output_shape);
    var input_count: usize = 1;
    var output_count: usize = 1;
    for (0..rank) |idx| {
        const dim = input.shape[idx];
        const stride = strides_raw[idx];
        if (dim <= 0 or dim > std.math.maxInt(u32) or stride <= 0 or stride > std.math.maxInt(u32)) return error.UnsupportedShape;
        var start = starts_raw[idx];
        var limit = limits_raw[idx];
        if (start < 0) start += dim;
        if (limit < 0) limit += dim;
        start = std.math.clamp(start, 0, dim);
        limit = std.math.clamp(limit, 0, dim);
        const size = if (limit <= start) 0 else @divTrunc(limit - start + stride - 1, stride);
        if (size <= 0 or size > std.math.maxInt(u32)) return error.UnsupportedShape;
        input_dims[idx] = @intCast(dim);
        output_dims[idx] = @intCast(size);
        starts[idx] = @intCast(start);
        strides[idx] = @intCast(stride);
        output_shape[idx] = size;
        input_count = try checkedMul(input_count, @intCast(dim));
        output_count = try checkedMul(output_count, @intCast(size));
    }
    if (input_count != input.elem_count) return error.InvalidShape;
    var device = try allocDeviceBuffer(self, output_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchPrimitiveSliceF32(
        &self.ctx,
        device,
        input.buffer,
        output_count,
        input_count,
        input_dims,
        output_dims,
        starts,
        strides,
        rank,
    );
    return createTensor(self, device, output_shape, output_count);
}

fn downloadAlloc(self: *CudaCompute, tensor: *const CudaTensor) ![]f32 {
    const out = try self.allocator.alloc(f32, tensor.elem_count);
    errdefer self.allocator.free(out);
    const byte_len = try downloadTensorToFloat32(self, tensor, self.allocator, out);
    self.stats.download_alloc_calls += 1;
    self.stats.download_alloc_bytes += byte_len;
    noteDownloadBucket(&self.stats, byte_len);
    noteTopTransferSize(&self.stats.download_top_sizes, &self.stats.download_top_counts, byte_len);
    return out;
}

fn allocShape2(allocator: std.mem.Allocator, rows: usize, cols: usize) ![]i64 {
    const shape = try allocator.alloc(i64, 2);
    shape[0] = @intCast(rows);
    shape[1] = @intCast(cols);
    return shape;
}

fn dupeShape(allocator: std.mem.Allocator, shape: []const i64) ![]i64 {
    return allocator.dupe(i64, shape);
}

fn ensureF32(tensor: *const CudaTensor) !void {
    if (tensor.dtype != .f32 or tensor.quant_type != null) {
        if (cudaTensorTypeDebugEnabled()) {
            std.log.err("cuda_tensor_type: expected=f32 actual={s} quantized={} shape={any}", .{
                @tagName(tensor.dtype),
                tensor.quant_type != null,
                tensor.shape,
            });
        }
        return error.UnsupportedTensorType;
    }
}

fn florenceTailWeightDTypeCode(tensor: *const CudaTensor) !u32 {
    if (tensor.quant_type != null) return error.UnsupportedTensorType;
    return switch (tensor.dtype) {
        .f32 => 0,
        .f16 => 1,
        .bf16 => 2,
        else => error.UnsupportedTensorType,
    };
}

fn ensureF32OrQuantized(tensor: *const CudaTensor) !void {
    if (tensor.quant_type != null) return;
    try ensureF32(tensor);
}

fn ensureF32Bf16OrQuantized(tensor: *const CudaTensor) !void {
    if (tensor.quant_type != null) return;
    if (tensor.dtype != .f32 and tensor.dtype != .bf16) return error.UnsupportedTensorType;
}

fn ensureF32F16Bf16OrQuantized(tensor: *const CudaTensor) !void {
    if (tensor.quant_type != null) return;
    if (tensor.dtype != .f32 and tensor.dtype != .f16 and tensor.dtype != .bf16) return error.UnsupportedTensorType;
}

fn ensureF32F16OrQuantized(tensor: *const CudaTensor) !void {
    if (tensor.quant_type != null) return;
    if (tensor.dtype != .f32 and tensor.dtype != .f16) return error.UnsupportedTensorType;
}

fn isBf16Weight(tensor: *const CudaTensor) bool {
    return tensor.dtype == .bf16 and tensor.quant_type == null;
}

fn isF16Weight(tensor: *const CudaTensor) bool {
    return tensor.dtype == .f16 and tensor.quant_type == null;
}

fn isKnownQuant(tensor: *const CudaTensor, known: gguf_tensor_types.KnownTensorType) bool {
    const quant_type = tensor.quant_type orelse return false;
    return switch (quant_type) {
        .known => |actual| actual == known,
        else => false,
    };
}

fn cudaTensorQuantName(tensor: *const CudaTensor) []const u8 {
    const quant_type = tensor.quant_type orelse return "none";
    return quant_type.name();
}

fn quantPlanFormatForTensor(tensor: *const CudaTensor) quant_matmul.Format {
    const quant_type = tensor.quant_type orelse return .f32;
    return backend_contracts.quantFormatFromGgufTensorType(quant_type) orelse .unknown;
}

test "cuda quant plan format mirrors graph quant tensor mapping" {
    const cases = [_]struct {
        tensor_type: ?gguf_tensor_types.TensorType,
        format: quant_matmul.Format,
    }{
        .{ .tensor_type = null, .format = .f32 },
        .{ .tensor_type = .{ .known = .Q4_0 }, .format = .q4_0 },
        .{ .tensor_type = .{ .known = .Q4_1 }, .format = .q4_1 },
        .{ .tensor_type = .{ .known = .Q5_0 }, .format = .q5_0 },
        .{ .tensor_type = .{ .known = .Q5_1 }, .format = .q5_1 },
        .{ .tensor_type = .{ .known = .Q1_0 }, .format = .q1_0 },
        .{ .tensor_type = .{ .known = .Q2_K }, .format = .q2_k },
        .{ .tensor_type = .{ .known = .Q3_K }, .format = .q3_k },
        .{ .tensor_type = .{ .known = .Q4_K }, .format = .q4_k },
        .{ .tensor_type = .{ .known = .Q8_1 }, .format = .q8_1 },
        .{ .tensor_type = .{ .known = .Q8_K }, .format = .q8_k },
        .{ .tensor_type = .{ .known = .F16 }, .format = .unknown },
    };
    for (cases) |case| {
        const tensor = CudaTensor{
            .buffer = .{},
            .dtype = .f32,
            .shape = &.{},
            .elem_count = 0,
            .quant_type = case.tensor_type,
        };
        try std.testing.expectEqual(case.format, quantPlanFormatForTensor(&tensor));
    }
}

fn cudaAllowPlannedFallback() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_ALLOW_PLANNED_FALLBACK", false);
}

fn cudaForceEvalSync() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_FORCE_EVAL_SYNC", false) or
        platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_PROFILE_SYNC", false);
}

fn cudaForceCrossBackendCopySync() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_CROSS_BACKEND_COPY_SYNC", false);
}

fn cudaEnableQ4KDecodeFast() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_Q4K_DECODE_FAST", true);
}

fn cudaEnableQ4KDecodeTile8() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_Q4K_DECODE_TILE8", false);
}

fn cudaDisableHeadNormRopeFusion() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_DISABLE_HEAD_NORM_ROPE_FUSION", false);
}

fn cudaDisableFusedQkv() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_DISABLE_FUSED_QKV", false);
}

// Default-on only where the fused kernel's parity gate has run (L4/SM89);
// other architectures keep the reference elementwise kernel unless the env
// switch forces the fused route.
fn cudaDebertaFusedAttentionEnabled(self: *const CudaCompute) bool {
    return platform.env.getenvBoolDefault(
        "ANTFLY_CUDA_DEBERTA_FUSED_ATTENTION",
        cudaQualifiedPerfTarget(self.ctx.info.compute_major, self.ctx.info.compute_minor),
    );
}

const CudaDebertaAttentionMode = enum {
    auto,
    fused_f32,
    streaming_f16,
    materialized_f16,
    generated_tc,
};

fn parseCudaDebertaAttentionMode(value: []const u8) ?CudaDebertaAttentionMode {
    if (std.ascii.eqlIgnoreCase(value, "auto")) return .auto;
    if (std.ascii.eqlIgnoreCase(value, "f32") or std.ascii.eqlIgnoreCase(value, "fused") or std.ascii.eqlIgnoreCase(value, "fused-f32")) return .fused_f32;
    if (std.ascii.eqlIgnoreCase(value, "streaming") or std.ascii.eqlIgnoreCase(value, "streaming-f16")) return .streaming_f16;
    if (std.ascii.eqlIgnoreCase(value, "materialized") or std.ascii.eqlIgnoreCase(value, "materialized-f16") or std.ascii.eqlIgnoreCase(value, "tensor-core")) return .materialized_f16;
    if (std.ascii.eqlIgnoreCase(value, "generated") or std.ascii.eqlIgnoreCase(value, "generated-tc") or std.ascii.eqlIgnoreCase(value, "tc")) return .generated_tc;
    return null;
}

const CudaDebertaInvalidModeWarning = struct {
    var emitted = std.atomic.Value(bool).init(false);
};

fn cudaDebertaAttentionMode() CudaDebertaAttentionMode {
    if (platform.env.getenv("ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE")) |value| {
        if (parseCudaDebertaAttentionMode(value)) |mode| return mode;
        if (!CudaDebertaInvalidModeWarning.emitted.swap(true, .monotonic)) {
            std.log.warn("ignoring invalid ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE='{s}'; using auto", .{value});
        }
    }
    return .auto;
}

/// The generated tensor-core encoder route stays opt-in until its external
/// Fastino evidence is checked in. This avoids silently trading model quality
/// for a locally faster schedule while keeping the production auto policy
/// explicit and testable.
fn cudaDebertaGeneratedTcAutoEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO", false);
}

const CudaDebertaGeneratedTcVariant = enum { auto, m32, m16 };

fn parseCudaDebertaGeneratedTcVariant(value: []const u8) ?CudaDebertaGeneratedTcVariant {
    if (std.ascii.eqlIgnoreCase(value, "auto")) return .auto;
    if (std.ascii.eqlIgnoreCase(value, "m16") or std.ascii.eqlIgnoreCase(value, "m16n32")) return .m16;
    if (std.ascii.eqlIgnoreCase(value, "m32") or std.ascii.eqlIgnoreCase(value, "m32n16")) return .m32;
    return null;
}

const CudaDebertaInvalidVariantWarning = struct {
    var emitted = std.atomic.Value(bool).init(false);
};

fn cudaDebertaGeneratedTcVariant() CudaDebertaGeneratedTcVariant {
    if (platform.env.getenv("ANTFLY_INFERENCE_CUDA_DEBERTA_GENERATED_TC_VARIANT")) |value| {
        if (parseCudaDebertaGeneratedTcVariant(value)) |variant| return variant;
        if (!CudaDebertaInvalidVariantWarning.emitted.swap(true, .monotonic)) {
            std.log.warn("ignoring invalid ANTFLY_INFERENCE_CUDA_DEBERTA_GENERATED_TC_VARIANT='{s}'; using auto", .{value});
        }
    }
    return .auto;
}

fn recordQuantKernelCompilerPlan(
    self: *CudaCompute,
    plan: quant_matmul.Plan,
    epilogue: quant_kernel_compiler.Epilogue,
) void {
    const counters = quant_kernel_compiler.plannedCountersForPlan(.cuda, plan, epilogue);
    quant_kernel_compiler.addCountersToStats(&self.stats, counters);
}

fn recordQuantMatmulPlan(
    self: *CudaCompute,
    weight_tensor: *const CudaTensor,
    op_plan: operator_plan.OperatorPlan,
    epilogue: quant_kernel_compiler.Epilogue,
) !void {
    switch (op_plan) {
        .quant_matmul => |plan| {
            self.stats.quant_ops.add(plan.operator);
            recordQuantKernelCompilerPlan(self, plan, epilogue);
            if (plan.format != quantPlanFormatForTensor(weight_tensor)) return error.CudaPlanFormatMismatch;
            if (plan.operator == .fallback and !cudaAllowPlannedFallback()) return error.CudaPlannedFallbackDisabled;
        },
        else => {},
    }
}

fn ensureCount(tensor: *const CudaTensor, expected: usize) !void {
    if (tensor.elem_count != expected) return error.InvalidShape;
}

fn ensureLogicalWeightCount(tensor: *const CudaTensor, expected: usize) !void {
    if (tensor.quant_type == null) return ensureCount(tensor, expected);
    const logical_count = try elementCountFromShape(tensor.shape);
    if (logical_count != expected) return error.InvalidShape;
}

fn checkedMul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidShape;
}

fn computeStridesUsize(shape: []const usize, out: []usize) void {
    var stride: usize = 1;
    var i = shape.len;
    while (i > 0) {
        i -= 1;
        out[i] = stride;
        stride *= shape[i];
    }
}

fn checkedAdd(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.InvalidShape;
}

fn checkedSub(a: usize, b: usize) !usize {
    return std.math.sub(usize, a, b) catch error.InvalidShape;
}

fn elementCountFromShape(shape: []const i64) !usize {
    var count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        count = try checkedMul(count, @intCast(dim));
    }
    return count;
}

fn sameShape(a: []const i64, b: []const i64) bool {
    return std.mem.eql(i64, a, b);
}

fn cudaLazyProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_LAZY_PROFILE");
}

fn cudaTensorTypeDebugEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DEBUG_TENSOR_TYPES");
}

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn cudaProfileNsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e6;
}

fn logCudaFlorenceProfile(phase: []const u8, start_ns: u64) void {
    std.log.info("cuda-florence-profile phase={s} elapsed_ms={d:.3}", .{ phase, cudaProfileNsToMs(monotonicNowNs() - start_ns) });
}

fn cudaLazyTraceEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_LAZY_TRACE");
}

fn uploadTempI64(self: *CudaCompute, data: []const i64) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len * @sizeOf(i64));
    try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
    return device;
}

fn uploadCachedAttentionMaskI64(self: *CudaCompute, data: []const i64) !buffer_mod.DeviceBuffer {
    const bytes = try checkedMul(data.len, @sizeOf(i64));
    const device = try self.attention_mask_scratch.acquire(&self.ctx, bytes);
    // Compare against the retained host copy byte-for-byte: a 64-bit hash
    // alone would silently reuse a stale device mask on collision, and the
    // host compare is negligible next to the H2D upload it saves.
    if (!std.mem.eql(i64, self.attention_mask_cache_host.items, data)) {
        // Invalidate first so a failed upload cannot leave the cache
        // claiming the device buffer holds this mask.
        self.attention_mask_cache_host.clearRetainingCapacity();
        try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
        try self.attention_mask_cache_host.appendSlice(self.allocator, data);
    }
    return device;
}

fn allOnesI64(data: []const i64) bool {
    for (data) |value| {
        if (value != 1) return false;
    }
    return true;
}

fn uploadTempU32(self: *CudaCompute, data: []const u32) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len * @sizeOf(u32));
    try copyFromHostTracked(self, device, std.mem.sliceAsBytes(data));
    return device;
}

const TempBufferPair = struct {
    first: buffer_mod.DeviceBuffer,
    second: buffer_mod.DeviceBuffer,
};

fn uploadTempU32Pair(self: *CudaCompute, first: []const u32, second: []const u32) !TempBufferPair {
    const first_bytes = try checkedMul(first.len, @sizeOf(u32));
    const second_bytes = try checkedMul(second.len, @sizeOf(u32));
    const total_bytes = try checkedAdd(first_bytes, second_bytes);
    const device = try self.temp_ids_masks.acquire(&self.ctx, total_bytes);
    const first_device: buffer_mod.DeviceBuffer = .{ .ptr = device.ptr, .len = first_bytes };
    const second_device: buffer_mod.DeviceBuffer = .{ .ptr = device.ptr + first_bytes, .len = second_bytes };
    try copyFromHostTracked(self, first_device, std.mem.sliceAsBytes(first));
    try copyFromHostTracked(self, second_device, std.mem.sliceAsBytes(second));
    return .{ .first = first_device, .second = second_device };
}

fn uploadTempU8(self: *CudaCompute, data: []const u8) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len);
    try copyFromHostTracked(self, device, data);
    return device;
}

fn embeddingLookupScaledCommon(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize, scale: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(weight);
    try ensureF32F16Bf16OrQuantized(weight_tensor);
    if (ids.len != total) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=embedding ids_len={d} total={d}", .{ ids.len, total });
        return error.InvalidShape;
    }
    if (dim == 0 or weight_tensor.elem_count % dim != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=embedding weight_elems={d} dim={d}", .{ weight_tensor.elem_count, dim });
        return error.InvalidShape;
    }
    const vocab = weight_tensor.elem_count / dim;
    for (ids) |raw_id| {
        if (raw_id < 0) return error.InvalidTokenId;
        const id: usize = @intCast(raw_id);
        if (id >= vocab) return error.InvalidTokenId;
    }
    const ids_device = try uploadTempI64(self, ids);
    const out_count = try checkedMul(total, dim);
    const shape = try allocShape2(self.allocator, total, dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    // Token embedding lookup was previously unattributed at prefill. `total` is
    // the number of tokens gathered (>1 at prefill).
    var embedding_profile_scope = beginPrefillProfile(self, .embedding, total);
    defer if (embedding_profile_scope) |*scope| scope.end();
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => try self.kernels.launchEmbeddingLookupQ8_0F32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim, scale),
                .Q4_0 => try self.kernels.launchEmbeddingLookupQ4_0F32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim, scale),
                .Q4_K => try self.kernels.launchEmbeddingLookupQ4KF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim, scale),
                .Q6_K => try self.kernels.launchEmbeddingLookupQ6KF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim, scale),
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else if (isBf16Weight(weight_tensor)) {
        try self.kernels.launchEmbeddingLookupBf16WeightF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim, scale);
    } else if (isF16Weight(weight_tensor)) {
        try self.kernels.launchEmbeddingLookupF16WeightF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim, scale);
    } else {
        try self.kernels.launchEmbeddingLookupF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim, scale);
    }
    self.stats.launch_embedding += 1;
    return createTensor(self, device, shape, out_count);
}

fn embeddingLookup(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT {
    return embeddingLookupScaledCommon(ctx, weight, ids, total, dim, 1.0);
}

fn embeddingLookupScaled(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize, scale: f32) anyerror!?CT {
    return try embeddingLookupScaledCommon(ctx, weight, ids, total, dim, scale);
}

fn embeddingLookupTensorScaledCommon(ctx: *anyopaque, weight: CT, ids: CT, total: usize, dim: usize, scale: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(weight);
    const ids_tensor = tensorFromCt(ids);
    if (isBf16Weight(weight_tensor)) return null;
    try ensureF32F16OrQuantized(weight_tensor);
    if (ids_tensor.dtype != .i32 or ids_tensor.quant_type != null) return null;
    if (ids_tensor.elem_count != total) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=embedding_tensor ids_elems={d} total={d}", .{ ids_tensor.elem_count, total });
        return error.InvalidShape;
    }
    if (dim == 0 or weight_tensor.elem_count % dim != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=embedding_tensor weight_elems={d} dim={d}", .{ weight_tensor.elem_count, dim });
        return error.InvalidShape;
    }

    const out_count = try checkedMul(total, dim);
    const shape = try allocShape2(self.allocator, total, dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var embedding_profile_scope = beginPrefillProfile(self, .embedding, total);
    defer if (embedding_profile_scope) |*scope| scope.end();
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => try self.kernels.launchEmbeddingLookupI32Q8_0F32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim, scale),
                .Q4_0 => try self.kernels.launchEmbeddingLookupI32Q4_0F32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim, scale),
                .Q4_K => try self.kernels.launchEmbeddingLookupI32Q4KF32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim, scale),
                .Q6_K => try self.kernels.launchEmbeddingLookupI32Q6KF32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim, scale),
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else if (isF16Weight(weight_tensor)) {
        try self.kernels.launchEmbeddingLookupI32F16WeightF32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim, scale);
    } else {
        try self.kernels.launchEmbeddingLookupI32F32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, total, dim, scale);
    }
    self.stats.launch_embedding += 1;
    return createTensor(self, device, shape, out_count);
}

fn embeddingLookupTensor(ctx: *anyopaque, weight: CT, ids: CT, total: usize, dim: usize) anyerror!?CT {
    return embeddingLookupTensorScaledCommon(ctx, weight, ids, total, dim, 1.0);
}

fn embeddingLookupTensorScaled(ctx: *anyopaque, weight: CT, ids: CT, total: usize, dim: usize, scale: f32) anyerror!?CT {
    return embeddingLookupTensorScaledCommon(ctx, weight, ids, total, dim, scale);
}

fn addWeightedEmbeddingTensor(ctx: *anyopaque, weight: CT, ids: CT, rhs: CT, total: usize, dim: usize, lhs_scale: f32, rhs_scale: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(weight);
    const ids_tensor = tensorFromCt(ids);
    const rhs_tensor = tensorFromCt(rhs);
    try ensureF32(rhs_tensor);
    try ensureF32OrQuantized(weight_tensor);
    if (ids_tensor.dtype != .i32 or ids_tensor.quant_type != null) return null;
    if (ids_tensor.elem_count != total) return error.InvalidShape;
    if (dim == 0 or rhs_tensor.elem_count != total * dim or weight_tensor.elem_count % dim != 0) return error.InvalidShape;
    const quant_type = weight_tensor.quant_type orelse return null;
    switch (quant_type) {
        .known => |known| if (known != .Q6_K) return null,
        else => return null,
    }

    const shape = try dupeShape(self.allocator, rhs_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, rhs_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    // PLE per-layer embedding gather (weighted add). Attributed to `.embedding`.
    var embedding_profile_scope = beginPrefillProfile(self, .embedding, total);
    defer if (embedding_profile_scope) |*scope| scope.end();
    try self.kernels.launchEmbeddingAddWeightedI32Q6KF32(&self.ctx, device, weight_tensor.buffer, ids_tensor.buffer, rhs_tensor.buffer, total, dim, lhs_scale, rhs_scale);
    self.stats.launch_elementwise += 1;
    self.stats.add_mul_scalar_fused += 1;
    return createTensor(self, device, shape, rhs_tensor.elem_count);
}

fn rmsNormAddWeightedEmbeddingTensor(
    ctx: *anyopaque,
    input: CT,
    norm_weight: CT,
    embedding_weight: CT,
    ids: CT,
    total: usize,
    num_groups: usize,
    group_dim: usize,
    eps: f32,
    lhs_scale: f32,
    rhs_scale: f32,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaPleRmsEmbeddingFusionEnabled()) return null;
    const input_tensor = tensorFromCt(input);
    const norm_weight_tensor = tensorFromCt(norm_weight);
    const embedding_weight_tensor = tensorFromCt(embedding_weight);
    const ids_tensor = tensorFromCt(ids);
    try ensureF32(input_tensor);
    try ensureF32(norm_weight_tensor);
    try ensureF32OrQuantized(embedding_weight_tensor);
    if (ids_tensor.dtype != .i32 or ids_tensor.quant_type != null) return null;
    if (total == 0 or num_groups == 0 or group_dim == 0) return null;
    const dim = try checkedMul(num_groups, group_dim);
    if (ids_tensor.elem_count != total or input_tensor.elem_count != try checkedMul(total, dim)) return error.InvalidShape;
    try ensureCount(norm_weight_tensor, group_dim);
    if (embedding_weight_tensor.elem_count % dim != 0) return error.InvalidShape;
    const quant_type = embedding_weight_tensor.quant_type orelse return null;
    switch (quant_type) {
        .known => |known| if (known != .Q6_K) return null,
        else => return null,
    }

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    self.kernels.launchRmsNormAddWeightedEmbeddingI32Q6KF32(
        &self.ctx,
        device,
        input_tensor.buffer,
        norm_weight_tensor.buffer,
        embedding_weight_tensor.buffer,
        ids_tensor.buffer,
        total,
        num_groups,
        group_dim,
        eps,
        lhs_scale,
        rhs_scale,
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            device.free(&self.ctx);
            self.allocator.free(shape);
            return null;
        },
        else => return err,
    };
    self.stats.launch_norm += 1;
    self.stats.rms_norm_add_weighted_embedding_fused_q6_k += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn takeRows(ctx: *anyopaque, request: *const ops.TakeRowsRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(request.input);
    try ensureF32(input_tensor);
    if (request.dim == 0 or request.rows != request.row_ids.len) return error.InvalidShape;
    if (input_tensor.elem_count % request.dim != 0) return error.InvalidShape;
    const source_rows = input_tensor.elem_count / request.dim;
    for (request.row_ids) |row_id| {
        if (row_id >= source_rows) return error.InvalidShape;
    }
    const row_ids_device = try uploadTempU32(self, request.row_ids);

    const out_count = try checkedMul(request.rows, request.dim);
    const shape = try allocShape2(self.allocator, request.rows, request.dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchTakeRowsF32(&self.ctx, device, input_tensor.buffer, row_ids_device, source_rows, request.rows, request.dim);
    self.stats.launch_other += 1;
    return try createTensor(self, device, shape, out_count);
}

fn glinerWordEmbeddings(ctx: *anyopaque, request: *const ops.GlinerWordEmbeddingsRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const hidden_tensor = tensorFromCt(request.hidden);
    try ensureF32(hidden_tensor);
    if (request.batch == 0 or request.seq_len == 0 or request.hidden_size == 0) return error.InvalidShape;
    const token_count = try checkedMul(request.batch, request.seq_len);
    if (request.words_mask.len < token_count) return error.InvalidShape;
    try ensureCount(hidden_tensor, try checkedMul(token_count, request.hidden_size));

    const out_rows = try checkedMul(request.batch, request.num_words);
    const out_count = try checkedMul(out_rows, request.hidden_size);
    const shape = try allocShape2(self.allocator, out_rows, request.hidden_size);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (out_count == 0) return try createTensor(self, device, shape, out_count);

    const mask_device = try uploadTempI64(self, request.words_mask[0..token_count]);
    try self.kernels.launchGlinerWordEmbeddingsF32(
        &self.ctx,
        device,
        hidden_tensor.buffer,
        mask_device,
        request.batch,
        request.seq_len,
        request.hidden_size,
        request.num_words,
    );
    self.stats.launch_embedding += 1;
    return try createTensor(self, device, shape, out_count);
}

fn glinerLabelGruCombined(ctx: *anyopaque, request: *const ops.GlinerLabelGruCombinedRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const label_tensor = tensorFromCt(request.label_embeddings);
    try ensureF32(label_tensor);
    if (request.num_labels == 0 or request.hidden_size == 0) return error.InvalidShape;
    try ensureCount(label_tensor, try checkedMul(request.num_labels, request.hidden_size));

    const pos_w = try getWeight(ctx, "count_embed.pos_embedding.weight");
    const pos_tensor = tensorFromCt(pos_w);
    if (pos_tensor.elem_count < request.hidden_size) return error.InvalidShape;

    const label_count = try checkedMul(request.num_labels, request.hidden_size);
    const gate_dim = try checkedMul(request.hidden_size, 3);

    const pos_ct = if (pos_tensor.quant_type == null) blk: {
        if (pos_tensor.dtype != .f32) return null;
        const pos_shape = try allocShape2(self.allocator, request.num_labels, request.hidden_size);
        var pos_shape_owned = false;
        errdefer if (!pos_shape_owned) self.allocator.free(pos_shape);
        var pos_device = try allocDeviceBuffer(self, label_count * @sizeOf(f32));
        var pos_device_owned = false;
        errdefer if (!pos_device_owned) pos_device.free(&self.ctx);
        try self.kernels.launchRepeatFirstRowF32(&self.ctx, pos_device, pos_tensor.buffer, request.num_labels, request.hidden_size);
        self.stats.launch_other += 1;
        const repeated = try createTensor(self, pos_device, pos_shape, label_count);
        pos_shape_owned = true;
        pos_device_owned = true;
        break :blk repeated;
    } else blk: {
        if (!isKnownQuant(pos_tensor, .Q4_K)) return null;
        if (request.hidden_size == 0 or request.hidden_size % 256 != 0) return error.InvalidShape;
        const zero_ids = try self.allocator.alloc(i64, request.num_labels);
        defer self.allocator.free(zero_ids);
        @memset(zero_ids, 0);
        break :blk try embeddingLookup(ctx, pos_w, zero_ids, request.num_labels, request.hidden_size);
    };
    defer freeTensor(ctx, pos_ct);

    const w_ih = try getWeight(ctx, "count_embed.gru.weight_ih_l0");
    const b_ih = try getWeight(ctx, "count_embed.gru.bias_ih_l0");
    const gi = try linear(ctx, pos_ct, w_ih, b_ih, request.num_labels, request.hidden_size, gate_dim);
    defer freeTensor(ctx, gi);

    const w_hh = try getWeight(ctx, "count_embed.gru.weight_hh_l0");
    const b_hh = try getWeight(ctx, "count_embed.gru.bias_hh_l0");
    const gh = try linear(ctx, request.label_embeddings, w_hh, b_hh, request.num_labels, request.hidden_size, gate_dim);
    defer freeTensor(ctx, gh);

    const gi_tensor = tensorFromCt(gi);
    const gh_tensor = tensorFromCt(gh);
    const shape = try allocShape2(self.allocator, request.num_labels, request.hidden_size);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, label_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchGlinerGruCombineF32(
        &self.ctx,
        device,
        label_tensor.buffer,
        gi_tensor.buffer,
        gh_tensor.buffer,
        request.num_labels,
        request.hidden_size,
    );
    self.stats.launch_other += 1;
    return try createTensor(self, device, shape, label_count);
}

fn glinerGatherConcatRelu(ctx: *anyopaque, request: *const ops.GlinerGatherConcatReluRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.kernels.gliner_gather_concat_relu_f32 == null) return null;
    if (request.start_rows.len != request.rows or request.end_rows.len != request.rows) return error.InvalidShape;

    const start_tensor = tensorFromCt(request.start);
    const end_tensor = tensorFromCt(request.end);
    try ensureF32(start_tensor);
    try ensureF32(end_tensor);
    try ensureCount(start_tensor, try checkedMul(request.source_rows, request.dim));
    try ensureCount(end_tensor, try checkedMul(request.source_rows, request.dim));

    const row_devices = try uploadTempU32Pair(self, request.start_rows, request.end_rows);
    const out_dim = try checkedMul(request.dim, 2);
    const out_count = try checkedMul(request.rows, out_dim);
    const shape = try allocShape2(self.allocator, request.rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchGlinerGatherConcatReluF32(
        &self.ctx,
        device,
        start_tensor.buffer,
        end_tensor.buffer,
        row_devices.first,
        row_devices.second,
        request.source_rows,
        request.rows,
        request.dim,
    );
    self.stats.launch_other += 1;
    return try createTensor(self, device, shape, out_count);
}

fn tcQuantBuffer(tensor: *const CudaTensor, layout: CudaTensorCoreQuantLayout) ?buffer_mod.DeviceBuffer {
    const tc_quant = tensor.tc_quant orelse return null;
    if (tc_quant.layout != layout) return null;
    return tc_quant.buffer;
}

fn tcFallbackReason(tensor: *const CudaTensor, variant: kernels_mod.QMatmulVariant, layout: CudaTensorCoreQuantLayout) CudaDispatchFallback {
    if (variant != .tc_hmma) return .explicit_simt;
    if (tcQuantBuffer(tensor, layout) == null) return .tc_no_packed_weight;
    return .tc_missing_symbol;
}

fn tcPairFallbackReason(a: *const CudaTensor, b: *const CudaTensor, variant: kernels_mod.QMatmulVariant, layout: CudaTensorCoreQuantLayout) CudaDispatchFallback {
    if (variant != .tc_hmma) return .explicit_simt;
    if (tcQuantBuffer(a, layout) == null or tcQuantBuffer(b, layout) == null) return .tc_no_packed_weight;
    return .tc_missing_symbol;
}

fn tcTripleFallbackReason(a: *const CudaTensor, b: *const CudaTensor, c: *const CudaTensor, variant: kernels_mod.QMatmulVariant, layout: CudaTensorCoreQuantLayout) CudaDispatchFallback {
    if (variant != .tc_hmma) return .explicit_simt;
    if (tcQuantBuffer(a, layout) == null or tcQuantBuffer(b, layout) == null or tcQuantBuffer(c, layout) == null) return .tc_no_packed_weight;
    return .tc_missing_symbol;
}

fn tcUnavailableReason() CudaDispatchFallback {
    return if (cudaTensorCoreQuantRequested()) .tc_unsupported_shape else .tc_not_requested;
}

fn missingTcSymbolFallback(err: anyerror) anyerror!bool {
    return switch (err) {
        error.CudaSymbolMissing => false,
        else => err,
    };
}

fn shouldTryQ4_0TcHmmaPrefill(enabled: bool, rows: usize, bf16_mirror_route_available: bool) bool {
    return enabled and rows > 1 and !bf16_mirror_route_available;
}

fn launchQ8TcHmmaNoBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q8_0_hmma) orelse return false;
    self.kernels.launchLinearQ8_0TcHmmaF32(&self.ctx, dst, input, packed_buffer, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ8TcHmmaBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q8_0_hmma) orelse return false;
    self.kernels.launchLinearQ8_0BiasTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ8TcHmmaBiasGelu(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q8_0_hmma) orelse return false;
    self.kernels.launchLinearQ8_0BiasGeluTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ8TcHmmaBiasAdd(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    residual: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q8_0_hmma) orelse return false;
    self.kernels.launchLinearQ8_0BiasAddTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, residual, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaNoBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KTcHmmaF32(&self.ctx, dst, input, packed_buffer, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

// W4A16 Q4_0 tensor-core prefill route. Returns true if it consumed the launch
// (packed weight present + kernel launched); false to fall through to the
// standard dispatch (mirror/cuBLASLt/SIMT). Only fires for rows > 1: decode
// GEMVs keep the tuned fast Q4_0 kernels.
fn launchQ4_0TcHmmaNoBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_0_hmma) orelse return false;
    // Prefer the BF16-fragment kernel: bf16 has f32's exponent range, so it
    // stays accurate on Gemma's large activations where the f16 tc kernel loses
    // precision (diverges from the exact reference). Fall back to f16 only if
    // the bf16 symbol is unavailable (e.g. artifacts not yet regenerated).
    if (self.kernels.linear_q4_0_f32_tc_hmma_bf16 != null) {
        self.kernels.launchLinearQ4_0TcHmmaBf16F32(&self.ctx, dst, input, packed_buffer, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
        return true;
    }
    self.kernels.launchLinearQ4_0TcHmmaF32(&self.ctx, dst, input, packed_buffer, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBiasGelu(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasGeluTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBiasAdd(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    residual: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasAddTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, residual, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBiasQuickGelu(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasQuickGeluTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaBiasRelu(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: *const CudaTensor,
    bias: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_buffer = tcQuantBuffer(weight, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KBiasReluTcHmmaF32(&self.ctx, dst, input, packed_buffer, bias, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaTripleBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst_a: buffer_mod.DeviceBuffer,
    dst_b: buffer_mod.DeviceBuffer,
    dst_c: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight_a: *const CudaTensor,
    bias_a: buffer_mod.DeviceBuffer,
    weight_b: *const CudaTensor,
    bias_b: buffer_mod.DeviceBuffer,
    weight_c: *const CudaTensor,
    bias_c: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_a = tcQuantBuffer(weight_a, .q4_k_hmma) orelse return false;
    const packed_b = tcQuantBuffer(weight_b, .q4_k_hmma) orelse return false;
    const packed_c = tcQuantBuffer(weight_c, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KTripleBiasTcHmmaF32(&self.ctx, dst_a, dst_b, dst_c, input, packed_a, bias_a, packed_b, bias_b, packed_c, bias_c, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn launchQ4KTcHmmaPairBias(
    self: *CudaCompute,
    variant: kernels_mod.QMatmulVariant,
    dst_a: buffer_mod.DeviceBuffer,
    dst_b: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight_a: *const CudaTensor,
    bias_a: buffer_mod.DeviceBuffer,
    weight_b: *const CudaTensor,
    bias_b: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant != .tc_hmma) return false;
    const packed_a = tcQuantBuffer(weight_a, .q4_k_hmma) orelse return false;
    const packed_b = tcQuantBuffer(weight_b, .q4_k_hmma) orelse return false;
    self.kernels.launchLinearQ4KPairBiasTcHmmaF32(&self.ctx, dst_a, dst_b, input, packed_a, bias_a, packed_b, bias_b, rows, in_dim, out_dim) catch |err| return missingTcSymbolFallback(err);
    return true;
}

fn simtQMatmulFallbackVariant(variant: kernels_mod.QMatmulVariant) kernels_mod.QMatmulVariant {
    return if (variant == .tc_hmma) .fast_r4c4 else variant;
}

fn linear(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32F16OrQuantized(weight_tensor);
    try ensureF32(bias_tensor);
    const input_expected = try checkedMul(rows, in_dim);
    if (input_tensor.elem_count != input_expected) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear input_elems={d} rows={d} in_dim={d}", .{ input_tensor.elem_count, rows, in_dim });
        return error.InvalidShape;
    }
    const weight_expected = try checkedMul(out_dim, in_dim);
    if (weight_tensor.elem_count != weight_expected) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear weight_elems={d} out_dim={d} in_dim={d}", .{ weight_tensor.elem_count, out_dim, in_dim });
        return error.InvalidShape;
    }
    if (bias_tensor.elem_count != out_dim) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear bias_elems={d} out_dim={d}", .{ bias_tensor.elem_count, out_dim });
        return error.InvalidShape;
    }

    // Encoder GGUFs commonly use Q4_0 weights together with learned biases.
    // Reuse the proven generated/no-bias Q4_0 dispatch and apply the bias in
    // place instead of rejecting the model at every projection. This keeps
    // the entire encoder on CUDA now; specialized bias epilogues can replace
    // this two-launch fallback through the same route later.
    if (isKnownQuant(weight_tensor, .Q4_0)) {
        const result = try linearNoBias(ctx, input, weight, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, result);
        const result_tensor = tensorFromCt(result);
        try self.kernels.launchAddBiasRowsF32(&self.ctx, result_tensor.buffer, bias_tensor.buffer, rows, out_dim);
        self.dispatch_stats.note(self.allocator, .linear, .q4_0, .q4_simt, .bias, .none, rows, in_dim, out_dim, 0);
        return result;
    }
    if (isF16Weight(weight_tensor)) {
        const result = try linearNoBias(ctx, input, weight, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, result);
        const result_tensor = tensorFromCt(result);
        try self.kernels.launchAddBiasRowsF32(&self.ctx, result_tensor.buffer, bias_tensor.buffer, rows, out_dim);
        self.dispatch_stats.note(self.allocator, .linear, .f16, .dense_lt, .bias, .none, rows, in_dim, out_dim, 0);
        return result;
    }

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var prefill_profile_scope = beginPrefillProfile(self, prefillProfileCategoryForLinearNoBias(weight_tensor, rows, in_dim, out_dim), rows);
    defer if (prefill_profile_scope) |*scope| scope.end();
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => if (mxbaiQ8Variant(rows, in_dim, out_dim)) |variant| {
                    if (try launchQ8TcHmmaBias(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
                        self.dispatch_stats.note(self.allocator, .linear, .q8_0, .q8_tc_hmma, .bias, .none, rows, in_dim, out_dim, 0);
                    } else {
                        try self.kernels.launchLinearQ8_0BiasVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                        self.dispatch_stats.note(self.allocator, .linear, .q8_0, .q8_simt, .bias, tcFallbackReason(weight_tensor, variant, .q8_0_hmma), rows, in_dim, out_dim, 0);
                    }
                } else {
                    try self.kernels.launchLinearQ8_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    try self.kernels.launchAddBiasRowsF32(&self.ctx, device, bias_tensor.buffer, rows, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear, .q8_0, .q8_simt, .bias, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                .Q4_K => if (mxbaiQ4TiledVariant(rows, in_dim, out_dim)) |variant| {
                    if (try launchQ4KTcHmmaBias(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
                        self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_tc_hmma, .bias, .none, rows, in_dim, out_dim, 0);
                    } else {
                        try self.kernels.launchLinearQ4KBiasVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                        self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_simt, .bias, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
                    }
                } else if (useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim)) {
                    try self.kernels.launchLinearQ4KSpanBiasTile4Rows8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_span_simt, .bias, .specialized_span, rows, in_dim, out_dim, 0);
                } else if (rows >= 2) {
                    try self.kernels.launchLinearQ4KBiasTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_simt, .bias, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                } else {
                    try self.kernels.launchLinearQ4KBiasTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear, .q4_k, .q4_simt, .bias, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else {
        if (try tryCublasLtF32Linear(self, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim)) {
            try self.kernels.launchAddBiasRowsF32(&self.ctx, device, bias_tensor.buffer, rows, out_dim);
            self.dispatch_stats.note(self.allocator, .linear, .f32, .dense_lt, .bias, .none, rows, in_dim, out_dim, 0);
        } else if (rows >= 2 and in_dim >= 256 and out_dim >= 4) {
            try self.kernels.launchLinearBiasTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear, .f32, .f32_cuda, .bias, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearBiasF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear, .f32, .f32_cuda, .bias, .none, rows, in_dim, out_dim, 0);
        }
    }
    self.stats.launch_linear += 1;
    return createTensor(self, device, shape, out_count);
}

fn linearPlanned(ctx: *anyopaque, request: *const ops.LinearPlannedRequest) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(request.weight);
    try recordQuantMatmulPlan(self, weight_tensor, request.operator_plan, .bias);
    return linear(ctx, request.input, request.weight, request.bias, request.rows, request.in_dim, request.out_dim);
}

fn linearQuickGelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    if (!isKnownQuant(weight_tensor, .Q4_K)) return null;
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (mxbaiQ4TiledVariant(rows, in_dim, out_dim)) |variant| {
        if (try launchQ4KTcHmmaBiasQuickGelu(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_quick_gelu, .q4_k, .q4_tc_hmma, .bias_quick_gelu, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ4KBiasQuickGeluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_quick_gelu, .q4_k, .q4_simt, .bias_quick_gelu, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
        }
    } else {
        try self.kernels.launchLinearQ4KBiasQuickGeluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_quick_gelu, .q4_k, .q4_simt, .bias_quick_gelu, tcUnavailableReason(), rows, in_dim, out_dim, 0);
    }
    self.stats.launch_linear += 1;
    return try createTensor(self, device, shape, out_count);
}

fn linearRelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    if (isF16Weight(weight_tensor)) {
        // Older CUDA artifacts predate the fused bias+ReLU epilogue kernel;
        // decline so the generic path composes it from unfused ops.
        if (self.kernels.add_bias_relu_rows_f32 == null) return null;
        try ensureCount(input_tensor, try checkedMul(rows, in_dim));
        try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
        try ensureCount(bias_tensor, out_dim);

        const projected = try linearNoBias(ctx, input, weight, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, projected);
        const projected_tensor = tensorFromCt(projected);
        try self.kernels.launchAddBiasReluRowsF32(&self.ctx, projected_tensor.buffer, bias_tensor.buffer, rows, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_relu, .f16, .dense_lt, .bias_relu, .none, rows, in_dim, out_dim, 0);
        return projected;
    }
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    const use_dense = weight_tensor.quant_type == null and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (!use_q4 and !use_dense) return null;
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (use_q4) {
        if (mxbaiQ4TiledVariant(rows, in_dim, out_dim)) |variant| {
            if (try launchQ4KTcHmmaBiasRelu(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
                self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_tc_hmma, .bias_relu, .none, rows, in_dim, out_dim, 0);
            } else if (useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim)) {
                try self.kernels.launchLinearQ4KSpanBiasReluTile4Rows8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_span_simt, .bias_relu, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
            } else {
                try self.kernels.launchLinearQ4KBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
                self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_simt, .bias_relu, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
            }
        } else if (useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim)) {
            try self.kernels.launchLinearQ4KSpanBiasReluTile4Rows8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_span_simt, .bias_relu, .specialized_span, rows, in_dim, out_dim, 0);
        } else if (rows >= 2) {
            try self.kernels.launchLinearQ4KBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_simt, .bias_relu, tcUnavailableReason(), rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ4KBiasReluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_relu, .q4_k, .q4_simt, .bias_relu, tcUnavailableReason(), rows, in_dim, out_dim, 0);
        }
    } else {
        try self.kernels.launchLinearBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_relu, .f32, .f32_cuda, .bias_relu, .none, rows, in_dim, out_dim, 0);
    }
    self.stats.launch_linear += 1;
    return try createTensor(self, device, shape, out_count);
}

fn linearGelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    const q8_variant = if (isKnownQuant(weight_tensor, .Q8_0)) mxbaiQ8Variant(rows, in_dim, out_dim) else null;
    const q4_variant = if (isKnownQuant(weight_tensor, .Q4_K)) mxbaiQ4Variant(rows, in_dim, out_dim) else null;
    if (isF16Weight(weight_tensor)) return null;
    const use_dense = weight_tensor.quant_type == null and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (q8_variant == null and q4_variant == null and !use_dense) return null;
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (q8_variant) |variant| {
        if (try launchQ8TcHmmaBiasGelu(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_gelu, .q8_0, .q8_tc_hmma, .bias_gelu, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ8_0BiasGeluVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_gelu, .q8_0, .q8_simt, .bias_gelu, tcFallbackReason(weight_tensor, variant, .q8_0_hmma), rows, in_dim, out_dim, 0);
        }
    } else if (q4_variant) |variant| {
        if (try launchQ4KTcHmmaBiasGelu(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_gelu, .q4_k, .q4_tc_hmma, .bias_gelu, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ4KBiasGeluVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_gelu, .q4_k, .q4_simt, .bias_gelu, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
        }
    } else {
        try self.kernels.launchLinearBiasGeluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_gelu, .f32, .f32_cuda, .bias_gelu, .none, rows, in_dim, out_dim, 0);
    }
    self.stats.launch_linear += 1;
    return try createTensor(self, device, shape, out_count);
}

fn linearAdd(ctx: *anyopaque, input: CT, weight: CT, bias: CT, residual: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    const residual_tensor = tensorFromCt(residual);
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    try ensureF32(residual_tensor);
    const q8_variant = if (isKnownQuant(weight_tensor, .Q8_0)) mxbaiQ8Variant(rows, in_dim, out_dim) else null;
    const q4_variant = if (isKnownQuant(weight_tensor, .Q4_K)) mxbaiQ4Variant(rows, in_dim, out_dim) else null;
    if (isF16Weight(weight_tensor)) return null;
    const use_dense = weight_tensor.quant_type == null and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (q8_variant == null and q4_variant == null and !use_dense) return null;
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);
    const out_count = try checkedMul(rows, out_dim);
    try ensureCount(residual_tensor, out_count);

    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (q8_variant) |variant| {
        if (try launchQ8TcHmmaBiasAdd(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_add, .q8_0, .q8_tc_hmma, .bias_add, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ8_0BiasAddVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_add, .q8_0, .q8_simt, .bias_add, tcFallbackReason(weight_tensor, variant, .q8_0_hmma), rows, in_dim, out_dim, 0);
        }
    } else if (q4_variant) |variant| {
        if (try launchQ4KTcHmmaBiasAdd(self, variant, device, input_tensor.buffer, weight_tensor, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_add, .q4_k, .q4_tc_hmma, .bias_add, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearQ4KBiasAddVariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_add, .q4_k, .q4_simt, .bias_add, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
        }
    } else {
        try self.kernels.launchLinearBiasAddTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_add, .f32, .f32_cuda, .bias_add, .none, rows, in_dim, out_dim, 0);
    }
    self.stats.launch_linear += 1;
    return try createTensor(self, device, shape, out_count);
}

fn tryLaunchLinearQ4_0Q8_1Dp4a(
    self: *CudaCompute,
    device: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (!cudaQ4_0LinearQ8_1Dp4aEnabled(self)) return false;
    if (!cudaQ4_0Q8_1RowsEligible(self, rows) or in_dim == 0 or out_dim == 0 or in_dim % 32 != 0) return false;
    const row_blocks = in_dim / 32;
    const q8_blocks = try checkedMul(rows, row_blocks);
    const q8_bytes = try checkedMul(q8_blocks, 36);
    if (q8_bytes == 0) return false;

    var q8_input = allocDeviceBuffer(self, q8_bytes) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => return false,
        else => return err,
    };
    defer releaseDeviceBuffer(self, &q8_input);
    self.kernels.launchQuantizeF32Q8_1Rows(&self.ctx, q8_input, input, rows, in_dim) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => return false,
        else => return err,
    };
    var launched_tile8 = false;
    var prefill_variant: kernels_mod.Q4_0Q8_1PrefillRowsVariant = .none;
    if (cudaQ4_0LinearQ8_1Tile8Enabled()) tile8_linear: {
        self.kernels.launchLinearQ4_0Q8_1Tile8F32(&self.ctx, device, q8_input, weight, rows, in_dim, out_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => break :tile8_linear,
            else => return err,
        };
        launched_tile8 = true;
        prefill_variant = if (rows > 1) .tile8_rows else .single;
    }
    if (!launched_tile8) {
        if (cudaQ4_0LinearQ8_1Tile4W8Enabled(self, in_dim)) {
            const variant = self.kernels.q4_0Q8_1Tile4W8PrefillRowsVariant(rows, in_dim, out_dim);
            self.kernels.launchLinearQ4_0Q8_1Tile4W8F32(&self.ctx, device, q8_input, weight, rows, in_dim, out_dim) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => return false,
                else => return err,
            };
            prefill_variant = variant;
        } else {
            self.kernels.launchLinearQ4_0Q8_1Tile4F32(&self.ctx, device, q8_input, weight, rows, in_dim, out_dim) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => return false,
                else => return err,
            };
            prefill_variant = if (rows > 1) .generic_rows else .single;
        }
    }
    noteQ4_0Q8_1PrefillLinear(&self.stats, prefill_variant);
    return true;
}

fn tryLaunchLinearQ4_0PairQ8_1Dp4a(
    self: *CudaCompute,
    device_a: buffer_mod.DeviceBuffer,
    device_b: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight_a: buffer_mod.DeviceBuffer,
    weight_b: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (!cudaQ4_0PairQ8_1Dp4aEnabled(self)) return false;
    if (!cudaQ4_0Q8_1RowsEligible(self, rows) or in_dim == 0 or out_dim == 0 or in_dim % 32 != 0) return false;
    const row_blocks = in_dim / 32;
    const q8_blocks = try checkedMul(rows, row_blocks);
    const q8_bytes = try checkedMul(q8_blocks, 36);
    if (q8_bytes == 0) return false;

    var q8_input = allocDeviceBuffer(self, q8_bytes) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => return false,
        else => return err,
    };
    defer releaseDeviceBuffer(self, &q8_input);
    self.kernels.launchQuantizeF32Q8_1Rows(&self.ctx, q8_input, input, rows, in_dim) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => return false,
        else => return err,
    };
    var prefill_variant: kernels_mod.Q4_0Q8_1PrefillRowsVariant = .none;
    if (cudaQ4_0PairQ8_1Tile8Enabled()) {
        self.kernels.launchLinearQ4_0PairNoBiasQ8_1Tile8F32(&self.ctx, device_a, device_b, q8_input, weight_a, weight_b, rows, in_dim, out_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => return false,
            else => return err,
        };
        prefill_variant = if (rows > 1) .tile8_rows else .single;
    } else if (cudaQ4_0PairQ8_1Tile4W8Enabled(self, in_dim)) {
        self.kernels.launchLinearQ4_0PairNoBiasQ8_1Tile4W8F32(&self.ctx, device_a, device_b, q8_input, weight_a, weight_b, rows, in_dim, out_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => return false,
            else => return err,
        };
        prefill_variant = if (rows > 1) .generic_rows else .single;
    } else {
        self.kernels.launchLinearQ4_0PairNoBiasQ8_1Tile4F32(&self.ctx, device_a, device_b, q8_input, weight_a, weight_b, rows, in_dim, out_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => return false,
            else => return err,
        };
        prefill_variant = if (rows > 1) .generic_rows else .single;
    }
    noteQ4_0Q8_1PrefillPair(&self.stats, prefill_variant);
    return true;
}

fn tryLaunchLinearQ4_0QkvQ8_1Dp4a(
    self: *CudaCompute,
    q_device: buffer_mod.DeviceBuffer,
    k_device: buffer_mod.DeviceBuffer,
    v_device: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    q_weight: buffer_mod.DeviceBuffer,
    k_weight: buffer_mod.DeviceBuffer,
    v_weight: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
) !bool {
    if (!cudaQ4_0QkvQ8_1Dp4aEnabled(self)) return false;
    if (!cudaQ4_0Q8_1RowsEligible(self, rows) or in_dim == 0 or q_out_dim == 0 or kv_out_dim == 0 or in_dim % 32 != 0) return false;
    const row_blocks = in_dim / 32;
    const q8_blocks = try checkedMul(rows, row_blocks);
    const q8_bytes = try checkedMul(q8_blocks, 36);
    if (q8_bytes == 0) return false;

    var q8_input = allocDeviceBuffer(self, q8_bytes) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => return false,
        else => return err,
    };
    defer releaseDeviceBuffer(self, &q8_input);
    self.kernels.launchQuantizeF32Q8_1Rows(&self.ctx, q8_input, input, rows, in_dim) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => return false,
        else => return err,
    };
    var prefill_variant: kernels_mod.Q4_0Q8_1PrefillRowsVariant = .none;
    if (cudaQ4_0QkvQ8_1Tile8Enabled(self)) {
        if (cudaQ4_0QkvQ8_1Tile8W8Enabled(in_dim)) {
            self.kernels.launchLinearQ4_0QkvNoBiasQ8_1Tile8W8F32(&self.ctx, q_device, k_device, v_device, q8_input, q_weight, k_weight, v_weight, rows, in_dim, q_out_dim, kv_out_dim) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => return false,
                else => return err,
            };
            prefill_variant = if (rows > 1) .tile8_w8_rows else .single;
        } else {
            const variant = self.kernels.q4_0QkvNoBiasQ8_1Tile8PrefillRowsVariant(rows);
            self.kernels.launchLinearQ4_0QkvNoBiasQ8_1Tile8F32(&self.ctx, q_device, k_device, v_device, q8_input, q_weight, k_weight, v_weight, rows, in_dim, q_out_dim, kv_out_dim) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => return false,
                else => return err,
            };
            prefill_variant = variant;
        }
    } else {
        self.kernels.launchLinearQ4_0QkvNoBiasQ8_1Tile4F32(&self.ctx, q_device, k_device, v_device, q8_input, q_weight, k_weight, v_weight, rows, in_dim, q_out_dim, kv_out_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => return false,
            else => return err,
        };
        prefill_variant = if (rows > 1) .generic_rows else .single;
    }
    noteQ4_0Q8_1PrefillQkv(&self.stats, prefill_variant);
    return true;
}

fn tryLinearNoBiasGatedDownQ8_1Dp4a(
    ctx: *anyopaque,
    gate: CT,
    up: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    activation: ops.DecoderRuntimeActivationKind,
) !?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaQ4_0GatedDownQ8_1Dp4aEnabled(self)) return null;
    if (!cudaQ4_0Q8_1RowsEligible(self, rows) or in_dim == 0 or out_dim == 0 or in_dim % 32 != 0) return null;
    const gate_tensor = tensorFromCt(gate);
    const up_tensor = tensorFromCt(up);
    const weight_tensor = tensorFromCt(weight);
    if (!isKnownQuant(weight_tensor, .Q4_0)) return null;

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    var shape_owned = false;
    errdefer if (!shape_owned) self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_owned = false;
    errdefer if (!device_owned) device.free(&self.ctx);

    const row_blocks = in_dim / 32;
    const q8_blocks = try checkedMul(rows, row_blocks);
    const q8_bytes = try checkedMul(q8_blocks, 36);
    var q8_input = allocDeviceBuffer(self, q8_bytes) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => {
            releaseDeviceBuffer(self, &device);
            device_owned = true;
            self.allocator.free(shape);
            shape_owned = true;
            return null;
        },
        else => return err,
    };
    defer releaseDeviceBuffer(self, &q8_input);

    self.kernels.launchQuantizeGatedF32Q8_1Rows(&self.ctx, q8_input, gate_tensor.buffer, up_tensor.buffer, rows, in_dim, @intFromEnum(activation)) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            device.free(&self.ctx);
            device_owned = true;
            self.allocator.free(shape);
            shape_owned = true;
            return null;
        },
        else => return err,
    };
    var prefill_variant: kernels_mod.Q4_0Q8_1PrefillRowsVariant = .none;
    if (cudaQ4_0LinearQ8_1Tile8Enabled()) {
        self.kernels.launchLinearQ4_0Q8_1Tile8F32(&self.ctx, device, q8_input, weight_tensor.buffer, rows, in_dim, out_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                device.free(&self.ctx);
                device_owned = true;
                self.allocator.free(shape);
                shape_owned = true;
                return null;
            },
            else => return err,
        };
        prefill_variant = if (rows > 1) .tile8_rows else .single;
    } else if (cudaQ4_0LinearQ8_1Tile4W8Enabled(self, in_dim)) {
        const variant = self.kernels.q4_0Q8_1Tile4W8PrefillRowsVariant(rows, in_dim, out_dim);
        self.kernels.launchLinearQ4_0Q8_1Tile4W8F32(&self.ctx, device, q8_input, weight_tensor.buffer, rows, in_dim, out_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                device.free(&self.ctx);
                device_owned = true;
                self.allocator.free(shape);
                shape_owned = true;
                return null;
            },
            else => return err,
        };
        prefill_variant = variant;
    } else {
        self.kernels.launchLinearQ4_0Q8_1Tile4F32(&self.ctx, device, q8_input, weight_tensor.buffer, rows, in_dim, out_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                device.free(&self.ctx);
                device_owned = true;
                self.allocator.free(shape);
                shape_owned = true;
                return null;
            },
            else => return err,
        };
        prefill_variant = if (rows > 1) .generic_rows else .single;
    }

    self.stats.launch_linear += 1;
    noteQ4_0Q8_1PrefillGatedDown(&self.stats, prefill_variant);
    const result = try createTensor(self, device, shape, out_count);
    shape_owned = true;
    device_owned = true;
    return result;
}

fn linearNoBias(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32F16Bf16OrQuantized(weight_tensor);
    const input_expected = try checkedMul(rows, in_dim);
    if (input_tensor.elem_count != input_expected) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear_no_bias input_elems={d} rows={d} in_dim={d}", .{ input_tensor.elem_count, rows, in_dim });
        return error.InvalidShape;
    }
    const weight_expected = try checkedMul(out_dim, in_dim);
    if (weight_tensor.elem_count != weight_expected) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=linear_no_bias weight_elems={d} out_dim={d} in_dim={d}", .{ weight_tensor.elem_count, out_dim, in_dim });
        return error.InvalidShape;
    }

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var prefill_profile_scope = beginPrefillProfile(self, prefillProfileCategoryForLinearNoBias(weight_tensor, rows, in_dim, out_dim), rows);
    defer if (prefill_profile_scope) |*scope| scope.end();
    const bf16_mirror = weightBf16MirrorForRows(self, weight_tensor, rows);
    if (shouldTryQ4_0TcHmmaPrefill(self.tuned_route_gates.q4_0_tc_hmma_prefill, rows, bf16_mirror != null) and
        try launchQ4_0TcHmmaNoBias(self, .tc_hmma, device, input_tensor.buffer, weight_tensor, rows, in_dim, out_dim))
    {
        // W4A16 tensor-core prefill route (default on SM89). It DEFERS to the
        // BF16 mirror when present: cuBLASLt BF16 is faster than this WMMA kernel
        // AND equal quality, so the mirror wins when the user opted into its
        // +VRAM; W4A16 is the win over DP4A when no mirror is attached (default).
        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_0, .q4_tc_hmma, .none, .none, rows, in_dim, out_dim, 0);
    } else if (bf16_mirror) |mirror_buffer| {
        if (try tryCublasLtBf16Linear(self, device, input_tensor, mirror_buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .bf16, .dense_lt, .none, .none, rows, in_dim, out_dim, 0);
        } else {
            self.stats.bf16_cublaslt_fallbacks += 1;
            try self.kernels.launchLinearBf16WeightF32Tiled(&self.ctx, device, input_tensor.buffer, mirror_buffer, rows, in_dim, out_dim);
            self.stats.bf16_scalar_linear_calls += 1;
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .bf16, .dense_cuda, .none, .none, rows, in_dim, out_dim, 0);
        }
    } else if (isF16Weight(weight_tensor)) {
        if (try tryCublasLtF16Linear(self, device, input_tensor, weight_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .f16, .dense_lt, .none, .none, rows, in_dim, out_dim, 0);
        } else {
            self.stats.f16_cublaslt_fallbacks += 1;
            try self.kernels.launchLinearF16WeightF32Tiled(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
            self.stats.f16_scalar_linear_calls += 1;
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .f16, .dense_cuda, .none, .none, rows, in_dim, out_dim, 0);
        }
    } else if (rows == 1 and weight_tensor.quant_type == null and weight_tensor.dtype == .f32 and
        weight_tensor.bf16_mirror.ptr != 0 and
        weight_tensor.bf16_mirror.len >= weight_tensor.elem_count * @sizeOf(u16))
    {
        // Decode GEMV on an F32 weight carrying a BF16 mirror (hybrid PLE
        // projection): the BF16 read halves the bandwidth of the dominant
        // per-token eager matmul. Plain kernel launch, graph-capture safe.
        try self.kernels.launchLinearBf16WeightF32Tiled(&self.ctx, device, input_tensor.buffer, weight_tensor.bf16_mirror, rows, in_dim, out_dim);
        self.stats.bf16_scalar_linear_calls += 1;
        self.dispatch_stats.note(self.allocator, .linear_no_bias, .bf16, .dense_cuda, .none, .none, rows, in_dim, out_dim, 0);
    } else if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => if (mxbaiQ8Variant(rows, in_dim, out_dim)) |variant| {
                    if (try launchQ8TcHmmaNoBias(self, variant, device, input_tensor.buffer, weight_tensor, rows, in_dim, out_dim)) {
                        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q8_0, .q8_tc_hmma, .none, .none, rows, in_dim, out_dim, 0);
                    } else {
                        try self.kernels.launchLinearQ8_0VariantF32(simtQMatmulFallbackVariant(variant), &self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q8_0, .q8_simt, .none, tcFallbackReason(weight_tensor, variant, .q8_0_hmma), rows, in_dim, out_dim, 0);
                    }
                } else {
                    if (rows == 1 and cudaQ4_0DecodeTile8GlobalEnabled()) {
                        self.kernels.launchLinearQ8_0Tile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |err| switch (err) {
                            error.CudaKernelUnavailable, error.InvalidCudaState => try self.kernels.launchLinearQ8_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                            else => return err,
                        };
                    } else {
                        try self.kernels.launchLinearQ8_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    }
                    self.dispatch_stats.note(self.allocator, .linear_no_bias, .q8_0, .q8_simt, .none, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                .Q4_0 => {
                    var route = CudaQ4KernelLaunch{
                        .kernel_id = "termite_linear_q4_0_f32",
                        .grid = .{ (rows * out_dim + 255) / 256, 1, 1 },
                        .block = .{ 256, 1, 1 },
                    };
                    if (try tryLaunchLinearQ4_0Q8_1Dp4a(self, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim)) {
                        route = if (cudaQ4_0LinearQ8_1Tile8Enabled()) .{
                            .kernel_id = "termite_linear_q4_0_q8_1_f32_tile8",
                            .grid = .{ (out_dim + 7) / 8, rows, 1 },
                            .block = .{ 128, 1, 1 },
                        } else if (cudaQ4_0LinearQ8_1Tile4W8Enabled(self, in_dim)) .{
                            .kernel_id = "termite_linear_q4_0_q8_1_f32_tile4_w8",
                            .grid = .{ (out_dim + 3) / 4, rows, 1 },
                            .block = .{ 256, 1, 1 },
                        } else .{
                            .kernel_id = "termite_linear_q4_0_q8_1_f32_tile4",
                            .grid = .{ (out_dim + 3) / 4, rows, 1 },
                            .block = .{ 128, 1, 1 },
                        };
                    } else if (rows == 1 and cudaQ4_0LinearTile8Enabled()) {
                        self.kernels.launchLinearQ4_0Tile8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |tile8_err| switch (tile8_err) {
                            error.CudaKernelUnavailable, error.InvalidCudaState => {
                                if (cudaQ4_0LinearTile4W4Enabled()) {
                                    self.kernels.launchLinearQ4_0Tile4W4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |tile4w4_err| switch (tile4w4_err) {
                                        error.CudaKernelUnavailable, error.InvalidCudaState => {
                                            route = try launchLinearQ4_0Tile4ThenBaseF32(self, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                                            route.provider = .fallback;
                                        },
                                        else => return tile4w4_err,
                                    };
                                    if (route.provider == .handwritten and std.mem.eql(u8, route.kernel_id, "termite_linear_q4_0_f32")) route = .{
                                        .provider = .fallback,
                                        .kernel_id = "termite_linear_q4_0_f32_tile4_w4",
                                        .grid = .{ (out_dim + 3) / 4, rows, 1 },
                                        .block = .{ 128, 1, 1 },
                                    };
                                } else {
                                    route = try launchLinearQ4_0Tile4ThenBaseF32(self, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                                    route.provider = .fallback;
                                }
                            },
                            else => return tile8_err,
                        };
                        if (route.provider == .handwritten and std.mem.eql(u8, route.kernel_id, "termite_linear_q4_0_f32")) route = .{
                            .kernel_id = "termite_linear_q4_0_f32_tile8",
                            .grid = .{ (out_dim + 7) / 8, rows, 1 },
                            .block = .{ 256, 1, 1 },
                        };
                    } else if (rows == 1 and cudaQ4_0LinearTile4W4Enabled()) {
                        self.kernels.launchLinearQ4_0Tile4W4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |tile4w4_err| switch (tile4w4_err) {
                            error.CudaKernelUnavailable, error.InvalidCudaState => {
                                route = try launchLinearQ4_0Tile4ThenBaseF32(self, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                                route.provider = .fallback;
                            },
                            else => return tile4w4_err,
                        };
                        if (route.provider == .handwritten and std.mem.eql(u8, route.kernel_id, "termite_linear_q4_0_f32")) route = .{
                            .kernel_id = "termite_linear_q4_0_f32_tile4_w4",
                            .grid = .{ (out_dim + 3) / 4, rows, 1 },
                            .block = .{ 128, 1, 1 },
                        };
                    } else if (rows == 1 and self.generated_q4_0_gates.mmv and
                        runtimeJitShapeAllowsForCompute(self, .mmv, rows, in_dim, out_dim) and
                        quant_kernel_compiler.generatedArtifactSupportsPlan(
                            .cuda,
                            quant_matmul.plan(.{ .rows = rows, .in_dim = in_dim, .out_dim = out_dim, .format = .q4_0 }),
                            .none,
                        ))
                    {
                        if (self.kernels.launchLinearQ4_0GeneratedMmvF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim)) {
                            self.stats.q4_0_generated_mmv_hits += 1;
                            route = .{
                                .provider = .generated,
                                .kernel_id = quant_kernel_compiler.first_general_cuda_q4_0_mmv_kernel_id,
                                .grid = .{ (out_dim + 3) / 4, 1, 1 },
                                .block = .{ 256, 1, 1 },
                            };
                        } else |generated_err| switch (generated_err) {
                            error.CudaKernelUnavailable, error.InvalidCudaState => {
                                self.stats.q4_0_generated_mmv_fallbacks += 1;
                                route = try launchLinearQ4_0Tile4ThenBaseF32(self, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                                route.provider = .fallback;
                            },
                            else => return generated_err,
                        }
                    } else if (rows == 1) {
                        route = try launchLinearQ4_0Tile4ThenBaseF32(self, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    } else if (rows >= 9 and rows <= 64 and self.generated_q4_0_gates.mm and
                        runtimeJitShapeAllowsForCompute(self, .mm, rows, in_dim, out_dim) and
                        quant_kernel_compiler.generatedArtifactSupportsPlan(
                            .cuda,
                            quant_matmul.plan(.{ .rows = rows, .in_dim = in_dim, .out_dim = out_dim, .format = .q4_0 }),
                            .none,
                        ))
                    {
                        if (self.kernels.launchLinearQ4_0GeneratedMmF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim)) {
                            self.stats.q4_0_generated_mm_hits += 1;
                            route = .{
                                .provider = .generated,
                                .kernel_id = quant_kernel_compiler.first_general_cuda_q4_0_mm_kernel_id,
                                .grid = .{ (out_dim + 3) / 4, (rows + 7) / 8, 1 },
                                .block = .{ 256, 1, 1 },
                            };
                        } else |generated_err| switch (generated_err) {
                            error.CudaKernelUnavailable, error.InvalidCudaState => {
                                self.stats.q4_0_generated_mm_fallbacks += 1;
                                try self.kernels.launchLinearQ4_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                                route.provider = .fallback;
                            },
                            else => return generated_err,
                        }
                    } else {
                        try self.kernels.launchLinearQ4_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    }
                    noteQ4Route(
                        self,
                        self.q4_route_op_override orelse q4LinearRouteOp(out_dim),
                        rows,
                        in_dim,
                        out_dim,
                        .single,
                        .none,
                        route.provider,
                        route.kernel_id,
                        route.grid,
                        route.block,
                    );
                    self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_0, .q4_simt, .none, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                .Q4_K => if (mxbaiQ4TiledVariant(rows, in_dim, out_dim)) |variant| {
                    if (try launchQ4KTcHmmaNoBias(self, variant, device, input_tensor.buffer, weight_tensor, rows, in_dim, out_dim)) {
                        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_k, .q4_tc_hmma, .none, .none, rows, in_dim, out_dim, 0);
                    } else {
                        try self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_k, .q4_simt, .none, tcFallbackReason(weight_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
                    }
                } else {
                    if (rows == 1 and cudaEnableQ4KDecodeFast()) {
                        var used_fallback = false;
                        if (cudaEnableQ4KDecodeTile8()) {
                            self.kernels.launchLinearQ4KTile8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |err| switch (err) {
                                error.CudaKernelUnavailable, error.InvalidCudaState => {
                                    used_fallback = true;
                                    self.stats.q4k_decode_fast_fallbacks += 1;
                                    try self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                                },
                                else => return err,
                            };
                        } else {
                            self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |err| switch (err) {
                                error.CudaKernelUnavailable, error.InvalidCudaState => {
                                    used_fallback = true;
                                    self.stats.q4k_decode_fast_fallbacks += 1;
                                    try self.kernels.launchLinearQ4KF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                                },
                                else => return err,
                            };
                        }
                        if (!used_fallback) self.stats.q4k_decode_fast_hits += 1;
                    } else {
                        try self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    }
                    self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_k, .q4_simt, .none, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                .Q6_K => {
                    // Full-logits lm-head fast path: Q8_1 activations + DP4A,
                    // same math as the greedy argmax kernels. Feeds sampled
                    // decoding, which needs the whole distribution.
                    if (rows == 1 and in_dim == 2560 and out_dim >= 32768 and out_dim % 8 == 0 and cudaQ6KLmHeadQ8_1Enabled(self)) q6_q8_blk: {
                        var q8_input = allocDeviceBuffer(self, (in_dim / 32) * 36) catch |err| switch (err) {
                            error.CudaGraphCaptureUnsafeTempAlloc => break :q6_q8_blk,
                            else => return err,
                        };
                        defer releaseDeviceBuffer(self, &q8_input);
                        self.kernels.launchQuantizeF32Q8_1Rows(&self.ctx, q8_input, input_tensor.buffer, rows, in_dim) catch |err| switch (err) {
                            error.CudaKernelUnavailable, error.InvalidCudaState => break :q6_q8_blk,
                            else => return err,
                        };
                        self.kernels.launchLinearQ6KQ8_1F32Tile8E4B(&self.ctx, device, q8_input, weight_tensor.buffer, in_dim, out_dim) catch |err| switch (err) {
                            error.CudaKernelUnavailable, error.InvalidCudaState => break :q6_q8_blk,
                            else => return err,
                        };
                        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q6_k, .q6_simt, .none, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                        self.stats.launch_linear += 1;
                        return createTensor(self, device, shape, out_count);
                    }
                    try self.kernels.launchLinearQ6KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
                    self.dispatch_stats.note(self.allocator, .linear_no_bias, .q6_k, .q6_simt, .none, tcUnavailableReason(), rows, in_dim, out_dim, 0);
                },
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else if (isBf16Weight(weight_tensor)) {
        if (try tryCublasLtBf16Linear(self, device, input_tensor, weight_tensor.buffer, rows, in_dim, out_dim)) {
            // cuBLASLt counters are updated by the helper.
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .bf16, .dense_lt, .none, .none, rows, in_dim, out_dim, 0);
        } else {
            self.stats.bf16_cublaslt_fallbacks += 1;
            try self.kernels.launchLinearBf16WeightF32Tiled(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
            self.stats.bf16_scalar_linear_calls += 1;
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .bf16, .dense_cuda, .none, .none, rows, in_dim, out_dim, 0);
        }
    } else {
        if (try tryCublasLtF32Linear(self, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim)) {
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .f32, .dense_lt, .none, .none, rows, in_dim, out_dim, 0);
        } else if (cudaF32LinearTiledEnabled() and rows <= 16 and in_dim >= 512 and out_dim >= 512) {
            self.kernels.launchLinearF32Tiled(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim) catch |tiled_err| switch (tiled_err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => try self.kernels.launchLinearF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                else => return tiled_err,
            };
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .f32, .f32_cuda, .none, .none, rows, in_dim, out_dim, 0);
        } else {
            try self.kernels.launchLinearF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_no_bias, .f32, .f32_cuda, .none, .none, rows, in_dim, out_dim, 0);
        }
    }
    self.stats.launch_linear += 1;
    return createTensor(self, device, shape, out_count);
}

fn linearNoBiasForQ4Route(
    ctx: *anyopaque,
    route_op: CudaQ4RouteOp,
    input: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const previous = self.q4_route_op_override;
    self.q4_route_op_override = route_op;
    defer self.q4_route_op_override = previous;
    return linearNoBias(ctx, input, weight, rows, in_dim, out_dim);
}

fn linearNoBiasPlanned(ctx: *anyopaque, request: *const ops.LinearNoBiasPlannedRequest) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(request.weight);
    try recordQuantMatmulPlan(self, weight_tensor, request.operator_plan, .none);
    return linearNoBias(ctx, request.input, request.weight, request.rows, request.in_dim, request.out_dim);
}

fn argmaxLastRow(ctx: *anyopaque, tensor: CT, rows: usize, dim: usize) anyerror!?u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(tensor);
    try ensureF32(input_tensor);
    if (rows == 0 or dim == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, dim));

    var token_device = try allocDeviceBuffer(self, @sizeOf(u32));
    defer releaseDeviceBuffer(self, &token_device);
    try self.kernels.launchArgmaxLastRowF32(&self.ctx, token_device, input_tensor.buffer, rows, dim);
    self.stats.launch_argmax += 1;
    var token: u32 = 0;
    try copyToHostTrackedAndSync(self, token_device, std.mem.asBytes(&token));
    return token;
}

fn argmaxRows(ctx: *anyopaque, tensor: CT, row_start: usize, row_count: usize, dim: usize, allocator: std.mem.Allocator) anyerror!?[]u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(tensor);
    try ensureF32(input_tensor);
    if (row_count == 0 or dim == 0) return error.InvalidShape;
    const row_end = std.math.add(usize, row_start, row_count) catch return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(row_end, dim));

    var token_device = try allocDeviceBuffer(self, try checkedMul(row_count, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &token_device);
    const launched = try self.kernels.launchArgmaxRowsF32(&self.ctx, token_device, input_tensor.buffer, row_start, row_count, dim);
    if (!launched) return null;
    self.stats.launch_argmax += 1;

    const tokens = try allocator.alloc(u32, row_count);
    errdefer allocator.free(tokens);
    try copyToHostTrackedAndSync(self, token_device, std.mem.sliceAsBytes(tokens));
    return tokens;
}

fn argmaxRowsSuppress(
    ctx: *anyopaque,
    tensor: CT,
    row_start: usize,
    row_count: usize,
    dim: usize,
    suppress_token_ids: []const i32,
    allocator: std.mem.Allocator,
) anyerror!?[]u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (suppress_token_ids.len == 0) return argmaxRows(ctx, tensor, row_start, row_count, dim, allocator);
    const input_tensor = tensorFromCt(tensor);
    try ensureF32(input_tensor);
    if (row_count == 0 or dim == 0) return error.InvalidShape;
    const row_end = std.math.add(usize, row_start, row_count) catch return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(row_end, dim));

    var suppress_device = try allocDeviceBuffer(self, try checkedMul(suppress_token_ids.len, @sizeOf(i32)));
    defer releaseDeviceBuffer(self, &suppress_device);
    try copyFromHostTracked(self, suppress_device, std.mem.sliceAsBytes(suppress_token_ids));

    var token_device = try allocDeviceBuffer(self, try checkedMul(row_count, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &token_device);
    const launched = try self.kernels.launchArgmaxRowsSuppressF32(
        &self.ctx,
        token_device,
        input_tensor.buffer,
        suppress_device,
        row_start,
        row_count,
        dim,
        suppress_token_ids.len,
    );
    if (!launched) return null;
    self.stats.launch_argmax += 1;

    const tokens = try allocator.alloc(u32, row_count);
    errdefer allocator.free(tokens);
    try copyToHostTrackedAndSync(self, token_device, std.mem.sliceAsBytes(tokens));
    return tokens;
}

fn argmaxLastRowSuppressTensor(ctx: *anyopaque, tensor: CT, rows: usize, dim: usize, suppress_token_ids: []const i32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (suppress_token_ids.len == 0) return null;
    const input_tensor = tensorFromCt(tensor);
    try ensureF32(input_tensor);
    if (rows == 0 or dim == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, dim));

    var suppress_device = try allocDeviceBuffer(self, suppress_token_ids.len * @sizeOf(i32));
    defer releaseDeviceBuffer(self, &suppress_device);
    try copyFromHostTracked(self, suppress_device, std.mem.sliceAsBytes(suppress_token_ids));

    var token_device = try allocDeviceBuffer(self, @sizeOf(i32));
    errdefer token_device.free(&self.ctx);
    try self.kernels.launchArgmaxLastRowSuppressF32(
        &self.ctx,
        token_device,
        input_tensor.buffer,
        suppress_device,
        rows,
        dim,
        suppress_token_ids.len,
    );
    self.stats.launch_argmax += 1;
    const shape = try self.allocator.alloc(i64, 1);
    errdefer self.allocator.free(shape);
    shape[0] = 1;
    return createTensorWithDType(self, token_device, shape, 1, .i32);
}

fn linearNoBiasArgmaxLastRow(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?u32 {
    const logits = try linearNoBias(ctx, input, weight, rows, in_dim, out_dim);
    defer freeTensor(ctx, logits);
    return argmaxLastRow(ctx, logits, rows, out_dim);
}

fn noteQ4_0Q8_1ArgmaxFallback(self: *CudaCompute, reason: []const u8, rows: usize, in_dim: usize, out_dim: usize) void {
    self.stats.lm_head_argmax_q4_0_q8_1_fallbacks += 1;
    if (cudaLazyProfileEnabled()) std.log.err(
        "cuda_lm_argmax_rows_unsupported: reason=q4_0_q8_1_{s} rows={d} in_dim={d} out_dim={d}",
        .{ reason, rows, in_dim, out_dim },
    );
}

fn suppressedArgmaxRequiresEagerRetry(capture_active: bool, suppress_count: usize) bool {
    return capture_active and suppress_count != 0;
}

test "CUDA suppressed argmax defers host upload during graph capture" {
    try std.testing.expect(!suppressedArgmaxRequiresEagerRetry(false, 2));
    try std.testing.expect(!suppressedArgmaxRequiresEagerRetry(true, 0));
    try std.testing.expect(suppressedArgmaxRequiresEagerRetry(true, 2));
}

test "generated Q6_K Q8_1 LM head excludes suppression and non-exact shapes" {
    try std.testing.expect(generatedQ6KQ8_1LmHeadArgmaxEligible(1, 2560, 262144, 0));
    try std.testing.expect(generatedQ6KQ8_1LmHeadArgmaxEligible(1, 3840, 262144, 0));
    try std.testing.expect(!generatedQ6KQ8_1LmHeadArgmaxEligible(1, 2560, 262144, 1));
    try std.testing.expect(!generatedQ6KQ8_1LmHeadArgmaxEligible(2, 2560, 262144, 0));
    try std.testing.expect(!generatedQ6KQ8_1LmHeadArgmaxEligible(1, 4096, 262144, 0));
}

fn allocQ4_0Q8_1ArgmaxTemp(self: *CudaCompute, bytes: usize) !?buffer_mod.DeviceBuffer {
    return allocDeviceBuffer(self, bytes) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => null,
        else => return err,
    };
}

fn tryLinearQ4_0Q8_1ArgmaxE2BDevice(
    self: *CudaCompute,
    input_tensor: *const CudaTensor,
    weight_tensor: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    suppress_token_ids: []const i32,
) !?DeviceBufferLease {
    if (!kernels_mod.linearQ4_0Q8_1ArgmaxE2BShapeEligible(rows, in_dim, out_dim)) {
        if (cudaLazyProfileEnabled()) std.log.err(
            "cuda_lm_argmax_rows_unsupported: reason=q4_0_q8_1_shape rows={d} in_dim={d} out_dim={d}",
            .{ rows, in_dim, out_dim },
        );
        return null;
    }
    if (!self.kernels.hasLinearQ4_0Q8_1ArgmaxE2BPrimitives()) {
        noteQ4_0Q8_1ArgmaxFallback(self, "kernel_unavailable", rows, in_dim, out_dim);
        return null;
    }
    if (suppressedArgmaxRequiresEagerRetry(self.debug_cuda_graph_capture_active, suppress_token_ids.len)) {
        noteQ4_0Q8_1ArgmaxFallback(self, "capture_suppression_retry", rows, in_dim, out_dim);
        return null;
    }

    var profile_scope = beginDecodeProfile(self, .lm_head_argmax, rows);
    defer if (profile_scope) |*scope| scope.end();

    const row_blocks = in_dim / 32;
    const q8_blocks = try checkedMul(rows, row_blocks);
    const q8_bytes = try checkedMul(q8_blocks, 36);
    const col_tiles = (out_dim + 7) / 8;
    const partial_count = try checkedMul(rows, col_tiles);
    const partial_bytes = try checkedMul(partial_count, @sizeOf(f32));
    const partial_storage_bytes = try checkedMul(partial_bytes, 2);

    var q8_input = (try allocQ4_0Q8_1ArgmaxTemp(self, q8_bytes)) orelse {
        noteQ4_0Q8_1ArgmaxFallback(self, "capture_unsafe_q8_temp", rows, in_dim, out_dim);
        return null;
    };
    defer releaseDeviceBuffer(self, &q8_input);
    var partial_storage = (try allocQ4_0Q8_1ArgmaxTemp(self, partial_storage_bytes)) orelse {
        noteQ4_0Q8_1ArgmaxFallback(self, "capture_unsafe_partial_storage", rows, in_dim, out_dim);
        return null;
    };
    defer releaseDeviceBuffer(self, &partial_storage);
    const partial_values: buffer_mod.DeviceBuffer = .{
        .ptr = partial_storage.ptr,
        .len = partial_bytes,
    };
    const partial_indices: buffer_mod.DeviceBuffer = .{
        .ptr = partial_storage.ptr + @as(u64, @intCast(partial_bytes)),
        .len = partial_bytes,
    };

    var suppress_device: buffer_mod.DeviceBuffer = .{};
    defer if (suppress_device.len != 0) releaseDeviceBuffer(self, &suppress_device);
    if (suppress_token_ids.len != 0) {
        const suppress_bytes = try checkedMul(suppress_token_ids.len, @sizeOf(i32));
        suppress_device = (try allocQ4_0Q8_1ArgmaxTemp(self, suppress_bytes)) orelse {
            noteQ4_0Q8_1ArgmaxFallback(self, "capture_unsafe_suppress_temp", rows, in_dim, out_dim);
            return null;
        };
        copyFromHostTracked(self, suppress_device, std.mem.sliceAsBytes(suppress_token_ids)) catch |err| switch (err) {
            error.CudaGraphCaptureUnsafeHostCopy => {
                noteQ4_0Q8_1ArgmaxFallback(self, "capture_unsafe_suppress_copy", rows, in_dim, out_dim);
                return null;
            },
            else => return err,
        };
    }

    var token_device = allocGreedyTokenDeviceBuffer(self, @sizeOf(u32)) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => {
            noteQ4_0Q8_1ArgmaxFallback(self, "capture_unsafe_token_temp", rows, in_dim, out_dim);
            return null;
        },
        else => return err,
    };
    var token_device_transferred = false;
    defer if (!token_device_transferred) releaseDeviceBufferLease(self, &token_device);

    self.kernels.launchQuantizeF32Q8_1Rows(&self.ctx, q8_input, input_tensor.buffer, rows, in_dim) catch |err| switch (err) {
        error.CudaKernelUnavailable => {
            noteQ4_0Q8_1ArgmaxFallback(self, "quantize_unavailable", rows, in_dim, out_dim);
            return null;
        },
        else => return err,
    };
    self.kernels.launchLinearQ4_0Q8_1ArgmaxRowsTile8E2B(
        &self.ctx,
        token_device.buffer,
        partial_values,
        partial_indices,
        q8_input,
        weight_tensor.buffer,
        suppress_device,
        rows,
        in_dim,
        out_dim,
        suppress_token_ids.len,
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            noteQ4_0Q8_1ArgmaxFallback(self, @errorName(err), rows, in_dim, out_dim);
            return null;
        },
        else => return err,
    };

    noteQ4Route(
        self,
        .lm_head,
        rows,
        in_dim,
        out_dim,
        .single,
        .none,
        .generated,
        quant_kernel_compiler.first_e2b_cuda_q4_0_q8_1_argmax_kernel_id,
        .{ rows * ((out_dim + 7) / 8), 1, 1 },
        .{ 96, 1, 1 },
    );

    self.stats.lm_head_argmax_fused_q4_0_q8_1 += 1;
    self.stats.launch_linear += 1;
    self.stats.launch_argmax += 1;
    token_device_transferred = true;
    return token_device;
}

fn linearNoBiasArgmaxLastRowSuppressTensor(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize, suppress_token_ids: []const i32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32Bf16OrQuantized(weight_tensor);
    if (rows == 0 or in_dim == 0 or out_dim == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));

    const use_q8 = isKnownQuant(weight_tensor, .Q8_0);
    const use_q4_0 = isKnownQuant(weight_tensor, .Q4_0);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    const use_q6 = isKnownQuant(weight_tensor, .Q6_K);
    if (!use_q8 and !use_q4_0 and !use_q4 and !use_q6) {
        if (cudaLazyProfileEnabled()) std.log.err(
            "cuda_lm_argmax_rows_unsupported: reason=quant rows={d} in_dim={d} out_dim={d} input_dtype={s} weight_dtype={s} weight_quant={s} weight_elems={d} weight_bytes={d}",
            .{ rows, in_dim, out_dim, @tagName(input_tensor.dtype), @tagName(weight_tensor.dtype), cudaTensorQuantName(weight_tensor), weight_tensor.elem_count, weight_tensor.buffer.len },
        );
        return null;
    }
    if (use_q8 and in_dim % 32 != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_lm_argmax_rows_unsupported: reason=q8_in_dim rows={d} in_dim={d} out_dim={d}", .{ rows, in_dim, out_dim });
        return null;
    }
    if (use_q4_0 and in_dim % 32 != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_lm_argmax_rows_unsupported: reason=q4_0_in_dim rows={d} in_dim={d} out_dim={d}", .{ rows, in_dim, out_dim });
        return null;
    }
    if (use_q4 and in_dim % 256 != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_lm_argmax_rows_unsupported: reason=q4k_in_dim rows={d} in_dim={d} out_dim={d}", .{ rows, in_dim, out_dim });
        return null;
    }
    if (use_q6 and in_dim % 256 != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_lm_argmax_rows_unsupported: reason=q6k_in_dim rows={d} in_dim={d} out_dim={d}", .{ rows, in_dim, out_dim });
        return null;
    }

    if (use_q4_0) {
        var token_device = candidate: {
            if (cudaQ4_0LmHeadQ8_1ArgmaxEnabled(self)) {
                if (try tryLinearQ4_0Q8_1ArgmaxE2BDevice(self, input_tensor, weight_tensor, rows, in_dim, out_dim, suppress_token_ids)) |candidate_token| {
                    break :candidate candidate_token;
                }
            }
            if (!cudaQ4_0LmHeadArgmaxEnabled() or rows != 1) return null;
            break :candidate (try linearNoBiasArgmaxRowsSuppressDevice(ctx, input, weight, rows, in_dim, out_dim, suppress_token_ids, true)) orelse return null;
        };
        errdefer releaseDeviceBufferLease(self, &token_device);
        return createI32ScalarTensorFromLease(self, &token_device);
    }
    if (use_q6) {
        var last_row_shape = [_]i64{ 1, @intCast(in_dim) };
        var last_row_tensor: CudaTensor = undefined;
        const argmax_input: CT = if (rows == 1) input else blk: {
            const last_row_elems = try checkedMul(rows - 1, in_dim);
            const last_row_bytes = try checkedMul(last_row_elems, @sizeOf(f32));
            const row_bytes = try checkedMul(in_dim, @sizeOf(f32));
            if (last_row_bytes + row_bytes > input_tensor.buffer.len) return error.InvalidShape;
            last_row_tensor = input_tensor.*;
            last_row_tensor.buffer = .{
                .ptr = input_tensor.buffer.ptr + last_row_bytes,
                .len = row_bytes,
            };
            last_row_tensor.shape = last_row_shape[0..];
            last_row_tensor.elem_count = in_dim;
            last_row_tensor.owns_buffer = false;
            last_row_tensor.owns_shape = false;
            last_row_tensor.owned_by_tensor = false;
            break :blk @ptrCast(&last_row_tensor);
        };
        var token_device = (try linearNoBiasArgmaxRowsSuppressDevice(ctx, argmax_input, weight, 1, in_dim, out_dim, suppress_token_ids, true)) orelse return null;
        errdefer releaseDeviceBufferLease(self, &token_device);
        return createI32ScalarTensorFromLease(self, &token_device);
    }

    if (suppressedArgmaxRequiresEagerRetry(self.debug_cuda_graph_capture_active, suppress_token_ids.len)) {
        self.stats.lm_head_argmax_fallbacks += 1;
        return null;
    }

    const col_tiles = (out_dim + 3) / 4;
    var partial_values = try allocDeviceBuffer(self, try checkedMul(col_tiles, @sizeOf(f32)));
    defer releaseDeviceBuffer(self, &partial_values);
    var partial_indices = try allocDeviceBuffer(self, try checkedMul(col_tiles, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &partial_indices);

    var suppress_device: buffer_mod.DeviceBuffer = .{};
    defer if (suppress_device.len != 0) releaseDeviceBuffer(self, &suppress_device);
    if (suppress_token_ids.len != 0) {
        suppress_device = try allocDeviceBuffer(self, try checkedMul(suppress_token_ids.len, @sizeOf(i32)));
        try copyFromHostTracked(self, suppress_device, std.mem.sliceAsBytes(suppress_token_ids));
    }

    var token_device = try allocGreedyTokenDeviceBuffer(self, @sizeOf(i32));
    errdefer releaseDeviceBufferLease(self, &token_device);

    if (use_q8) {
        self.kernels.launchLinearQ8_0ArgmaxTile4F32(
            &self.ctx,
            token_device.buffer,
            partial_values,
            partial_indices,
            input_tensor.buffer,
            weight_tensor.buffer,
            suppress_device,
            rows,
            in_dim,
            out_dim,
            suppress_token_ids.len,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                if (cudaLazyProfileEnabled()) std.log.err("cuda_lm_argmax_rows_unsupported: reason=q8_kernel err={s} rows={d} in_dim={d} out_dim={d}", .{ @errorName(err), rows, in_dim, out_dim });
                self.stats.lm_head_argmax_fallbacks += 1;
                releaseDeviceBufferLease(self, &token_device);
                return null;
            },
            else => return err,
        };
        self.stats.lm_head_argmax_fused_q8 += 1;
    } else {
        self.kernels.launchLinearQ4KArgmaxTile4F32(
            &self.ctx,
            token_device.buffer,
            partial_values,
            partial_indices,
            input_tensor.buffer,
            weight_tensor.buffer,
            suppress_device,
            rows,
            in_dim,
            out_dim,
            suppress_token_ids.len,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                if (cudaLazyProfileEnabled()) std.log.err("cuda_lm_argmax_rows_unsupported: reason=q4_0_kernel err={s} rows={d} in_dim={d} out_dim={d}", .{ @errorName(err), rows, in_dim, out_dim });
                self.stats.lm_head_argmax_fallbacks += 1;
                releaseDeviceBufferLease(self, &token_device);
                return null;
            },
            else => return err,
        };
        self.stats.lm_head_argmax_fused_q4_0 += 1;
    }

    self.stats.launch_linear += 1;
    self.stats.launch_argmax += 1;
    return createI32ScalarTensorFromLease(self, &token_device);
}

fn linearNoBiasArgmaxRowsSuppressDevice(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    suppress_token_ids: []const i32,
    allow_prepared_greedy_output: bool,
) anyerror!?DeviceBufferLease {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32Bf16OrQuantized(weight_tensor);
    if (rows == 0 or in_dim == 0 or out_dim == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));

    const use_q8 = isKnownQuant(weight_tensor, .Q8_0);
    const use_q4_0 = isKnownQuant(weight_tensor, .Q4_0);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    const use_q6 = isKnownQuant(weight_tensor, .Q6_K);
    if (!use_q8 and !use_q4_0 and !use_q4 and !use_q6) {
        std.log.err(
            "cuda_lm_argmax_rows_unsupported: reason=quant rows={d} in_dim={d} out_dim={d} input_dtype={s} weight_dtype={s} weight_quant={s} weight_elems={d} weight_bytes={d}",
            .{ rows, in_dim, out_dim, @tagName(input_tensor.dtype), @tagName(weight_tensor.dtype), cudaTensorQuantName(weight_tensor), weight_tensor.elem_count, weight_tensor.buffer.len },
        );
        return null;
    }
    if (use_q8 and in_dim % 32 != 0) {
        std.log.err("cuda_lm_argmax_rows_unsupported: reason=q8_in_dim rows={d} in_dim={d} out_dim={d}", .{ rows, in_dim, out_dim });
        return null;
    }
    if (use_q4_0 and in_dim % 32 != 0) {
        std.log.err("cuda_lm_argmax_rows_unsupported: reason=q4_0_in_dim rows={d} in_dim={d} out_dim={d}", .{ rows, in_dim, out_dim });
        return null;
    }
    if (use_q4 and in_dim % 256 != 0) {
        std.log.err("cuda_lm_argmax_rows_unsupported: reason=q4k_in_dim rows={d} in_dim={d} out_dim={d}", .{ rows, in_dim, out_dim });
        return null;
    }
    if (use_q6 and in_dim % 256 != 0) {
        std.log.err("cuda_lm_argmax_rows_unsupported: reason=q6k_in_dim rows={d} in_dim={d} out_dim={d}", .{ rows, in_dim, out_dim });
        return null;
    }
    if (suppressedArgmaxRequiresEagerRetry(self.debug_cuda_graph_capture_active, suppress_token_ids.len)) {
        self.stats.lm_head_argmax_fallbacks += 1;
        return null;
    }

    var profile_scope = if (out_dim >= 100_000)
        beginDecodeProfile(self, .lm_head_argmax, rows)
    else
        null;
    defer if (profile_scope) |*scope| scope.end();

    const col_tiles = (out_dim + 3) / 4;
    const partial_count = try checkedMul(rows, col_tiles);
    var partial_values = try allocDeviceBuffer(self, try checkedMul(partial_count, @sizeOf(f32)));
    defer releaseDeviceBuffer(self, &partial_values);
    var partial_indices = try allocDeviceBuffer(self, try checkedMul(partial_count, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &partial_indices);

    var suppress_device: buffer_mod.DeviceBuffer = .{};
    defer if (suppress_device.len != 0) releaseDeviceBuffer(self, &suppress_device);
    if (suppress_token_ids.len != 0) {
        suppress_device = try allocDeviceBuffer(self, try checkedMul(suppress_token_ids.len, @sizeOf(i32)));
        try copyFromHostTracked(self, suppress_device, std.mem.sliceAsBytes(suppress_token_ids));
    }

    var token_device = if (rows == 1 and allow_prepared_greedy_output)
        try allocGreedyTokenDeviceBuffer(self, @sizeOf(u32))
    else
        DeviceBufferLease{
            .buffer = try allocDeviceBuffer(self, try checkedMul(rows, @sizeOf(u32))),
            .owns_buffer = true,
        };
    var token_device_transferred = false;
    defer if (!token_device_transferred) releaseDeviceBufferLease(self, &token_device);

    if (use_q8) {
        self.kernels.launchLinearQ8_0ArgmaxRowsTile4F32(
            &self.ctx,
            token_device.buffer,
            partial_values,
            partial_indices,
            input_tensor.buffer,
            weight_tensor.buffer,
            suppress_device,
            rows,
            in_dim,
            out_dim,
            suppress_token_ids.len,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                std.log.err("cuda_lm_argmax_rows_unsupported: reason=q8_kernel err={s} rows={d} in_dim={d} out_dim={d}", .{ @errorName(err), rows, in_dim, out_dim });
                self.stats.lm_head_argmax_fallbacks += 1;
                return null;
            },
            else => return err,
        };
        self.stats.lm_head_argmax_fused_q8 += 1;
    } else if (use_q4_0) {
        const launched_tile16 = blk: {
            if (!cudaQ4_0LmHeadTile16Enabled()) break :blk false;
            self.kernels.launchLinearQ4_0ArgmaxRowsTile16F32(
                &self.ctx,
                token_device.buffer,
                partial_values,
                partial_indices,
                input_tensor.buffer,
                weight_tensor.buffer,
                suppress_device,
                rows,
                in_dim,
                out_dim,
                suppress_token_ids.len,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => break :blk false,
                else => return err,
            };
            break :blk true;
        };
        if (!launched_tile16) {
            self.kernels.launchLinearQ4_0ArgmaxRowsTile4F32(
                &self.ctx,
                token_device.buffer,
                partial_values,
                partial_indices,
                input_tensor.buffer,
                weight_tensor.buffer,
                suppress_device,
                rows,
                in_dim,
                out_dim,
                suppress_token_ids.len,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => {
                    std.log.err("cuda_lm_argmax_rows_unsupported: reason=q4_0_kernel err={s} rows={d} in_dim={d} out_dim={d}", .{ @errorName(err), rows, in_dim, out_dim });
                    self.stats.lm_head_argmax_fallbacks += 1;
                    return null;
                },
                else => return err,
            };
        }
        noteQ4Route(
            self,
            .lm_head,
            rows,
            in_dim,
            out_dim,
            .single,
            .none,
            .handwritten,
            if (launched_tile16)
                "termite_linear_q4_0_argmax_rows_stage1_tile16"
            else
                "termite_linear_q4_0_argmax_rows_stage1_tile4",
            .{ rows * ((out_dim + (if (launched_tile16) @as(usize, 15) else 3)) / (if (launched_tile16) @as(usize, 16) else 4)), 1, 1 },
            .{ 256, 1, 1 },
        );
        self.stats.lm_head_argmax_fused_q4_0 += 1;
    } else if (use_q4) {
        self.kernels.launchLinearQ4KArgmaxRowsTile4F32(
            &self.ctx,
            token_device.buffer,
            partial_values,
            partial_indices,
            input_tensor.buffer,
            weight_tensor.buffer,
            suppress_device,
            rows,
            in_dim,
            out_dim,
            suppress_token_ids.len,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                std.log.err("cuda_lm_argmax_rows_unsupported: reason=q4k_kernel err={s} rows={d} in_dim={d} out_dim={d}", .{ @errorName(err), rows, in_dim, out_dim });
                self.stats.lm_head_argmax_fallbacks += 1;
                return null;
            },
            else => return err,
        };
        self.stats.lm_head_argmax_fused_q4 += 1;
    } else {
        // The generated candidates own only their two exact greedy shapes and
        // have no suppression loop. All other Q6 paths retain the existing
        // handwritten dispatch, including suppressed-token requests.
        const generated_q8_1_candidate = cudaGeneratedQ6KQ8_1LmHeadArgmaxEnabled() and
            generatedQ6KQ8_1LmHeadArgmaxEligible(rows, in_dim, out_dim, suppress_token_ids.len);
        const launched_q8_1 = if (cudaQ6KLmHeadQ8_1Enabled(self) or generated_q8_1_candidate) blk: {
            if (rows != 1 or in_dim % 256 != 0) break :blk false;
            const q8_row_blocks = in_dim / 32;
            const q8_blocks = checkedMul(rows, q8_row_blocks) catch break :blk false;
            const q8_bytes = checkedMul(q8_blocks, 36) catch break :blk false;
            var q8_input = allocDeviceBuffer(self, q8_bytes) catch |err| switch (err) {
                error.CudaGraphCaptureUnsafeTempAlloc => {
                    if (generated_q8_1_candidate) self.stats.lm_head_argmax_generated_q6_k_q8_1_fallbacks += 1;
                    break :blk false;
                },
                else => return err,
            };
            defer releaseDeviceBuffer(self, &q8_input);

            self.kernels.launchQuantizeF32Q8_1Rows(&self.ctx, q8_input, input_tensor.buffer, rows, in_dim) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => {
                    if (generated_q8_1_candidate) self.stats.lm_head_argmax_generated_q6_k_q8_1_fallbacks += 1;
                    break :blk false;
                },
                else => return err,
            };

            const launched_generated = if (generated_q8_1_candidate) generated: {
                self.kernels.launchLinearQ6KQ8_1GeneratedArgmaxRowsTile8F32(
                    &self.ctx,
                    token_device.buffer,
                    partial_values,
                    partial_indices,
                    q8_input,
                    weight_tensor.buffer,
                    rows,
                    in_dim,
                    out_dim,
                ) catch |err| switch (err) {
                    error.CudaKernelUnavailable, error.InvalidCudaState => {
                        self.stats.lm_head_argmax_generated_q6_k_q8_1_fallbacks += 1;
                        break :generated false;
                    },
                    else => return err,
                };
                self.stats.lm_head_argmax_generated_q6_k_q8_1_hits += 1;
                break :generated true;
            } else false;
            if (!launched_generated) {
                self.kernels.launchLinearQ6KQ8_1ArgmaxRowsTile8F32(
                    &self.ctx,
                    token_device.buffer,
                    partial_values,
                    partial_indices,
                    q8_input,
                    weight_tensor.buffer,
                    suppress_device,
                    rows,
                    in_dim,
                    out_dim,
                    suppress_token_ids.len,
                ) catch |err| switch (err) {
                    error.CudaKernelUnavailable, error.InvalidCudaState => break :blk false,
                    else => return err,
                };
            }
            break :blk true;
        } else false;
        const launched_tile16 = blk: {
            if (launched_q8_1) break :blk true;
            if (!cudaQ6KLmHeadTile16Enabled()) break :blk false;
            self.kernels.launchLinearQ6KArgmaxRowsTile16F32(
                &self.ctx,
                token_device.buffer,
                partial_values,
                partial_indices,
                input_tensor.buffer,
                weight_tensor.buffer,
                suppress_device,
                rows,
                in_dim,
                out_dim,
                suppress_token_ids.len,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => break :blk false,
                else => return err,
            };
            break :blk true;
        };
        const launched_tile8 = blk: {
            if (launched_q8_1 or launched_tile16) break :blk true;
            self.kernels.launchLinearQ6KArgmaxRowsTile8F32(
                &self.ctx,
                token_device.buffer,
                partial_values,
                partial_indices,
                input_tensor.buffer,
                weight_tensor.buffer,
                suppress_device,
                rows,
                in_dim,
                out_dim,
                suppress_token_ids.len,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => break :blk false,
                else => return err,
            };
            break :blk true;
        };
        if (!launched_tile8) {
            self.kernels.launchLinearQ6KArgmaxRowsTile4F32(
                &self.ctx,
                token_device.buffer,
                partial_values,
                partial_indices,
                input_tensor.buffer,
                weight_tensor.buffer,
                suppress_device,
                rows,
                in_dim,
                out_dim,
                suppress_token_ids.len,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => {
                    std.log.err("cuda_lm_argmax_rows_unsupported: reason=q6k_kernel err={s} rows={d} in_dim={d} out_dim={d}", .{ @errorName(err), rows, in_dim, out_dim });
                    self.stats.lm_head_argmax_fallbacks += 1;
                    return null;
                },
                else => return err,
            };
        }
        self.stats.lm_head_argmax_fused_q6 += 1;
    }

    self.stats.launch_linear += 1;
    self.stats.launch_argmax += 1;
    token_device_transferred = true;
    return token_device;
}

fn linearNoBiasArgmaxRowsSuppress(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    suppress_token_ids: []const i32,
    allocator: std.mem.Allocator,
) anyerror!?[]u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var token_device = (try linearNoBiasArgmaxRowsSuppressDevice(ctx, input, weight, rows, in_dim, out_dim, suppress_token_ids, false)) orelse return null;
    defer releaseDeviceBufferLease(self, &token_device);
    const tokens = try allocator.alloc(u32, rows);
    errdefer allocator.free(tokens);
    try copyToHostTrackedAndSync(self, token_device.buffer, std.mem.sliceAsBytes(tokens));
    self.stats.mtp_verify_commit_choice_downloads += 1;
    return tokens;
}

fn linearNoBiasArgmaxLastRowTensor(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (try linearNoBiasArgmaxLastRowSuppressTensor(ctx, input, weight, rows, in_dim, out_dim, &.{})) |token| {
        return token;
    }

    const logits = try linearNoBias(ctx, input, weight, rows, in_dim, out_dim);
    defer freeTensor(ctx, logits);

    var token_device = try allocGreedyTokenDeviceBuffer(self, @sizeOf(i32));
    errdefer releaseDeviceBufferLease(self, &token_device);
    try self.kernels.launchArgmaxLastRowF32(&self.ctx, token_device.buffer, tensorFromCt(logits).buffer, rows, out_dim);
    self.stats.launch_argmax += 1;
    return createI32ScalarTensorFromLease(self, &token_device);
}

fn gemma4MtpPreproject(ctx: *anyopaque, request: *const ops.Gemma4MtpPreprojectRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaMtpPreprojectFusionEnabled()) {
        self.stats.mtp_preproject_fused_fallbacks += 1;
        return null;
    }
    const target_embedding_tensor = tensorFromCt(request.target_embedding);
    const activation_tensor = tensorFromCt(request.activation);
    const weight_tensor = tensorFromCt(request.weight);
    try ensureF32(target_embedding_tensor);
    try ensureF32(activation_tensor);
    if (request.backbone_hidden == 0 or request.draft_hidden == 0) return error.InvalidShape;
    try ensureCount(target_embedding_tensor, request.backbone_hidden);
    try ensureCount(activation_tensor, request.backbone_hidden);
    if (weight_tensor.quant_type != null) {
        self.stats.mtp_preproject_fused_fallbacks += 1;
        return null;
    }
    const weight_dtype_code: u32 = switch (weight_tensor.dtype) {
        .f32 => 0,
        .bf16 => 1,
        .f16 => 2,
        else => {
            self.stats.mtp_preproject_fused_fallbacks += 1;
            return null;
        },
    };
    try ensureCount(weight_tensor, try checkedMul(request.draft_hidden, try checkedMul(request.backbone_hidden, 2)));

    const elem_count = request.draft_hidden;
    var output_device = try allocDeviceBuffer(self, try checkedMul(elem_count, @sizeOf(f32)));
    errdefer releaseDeviceBuffer(self, &output_device);
    const launched = try self.kernels.launchGemma4MtpPreprojectF32(
        &self.ctx,
        output_device,
        target_embedding_tensor.buffer,
        activation_tensor.buffer,
        weight_tensor.buffer,
        request.backbone_hidden,
        request.draft_hidden,
        @intFromEnum(request.concat_order),
        weight_dtype_code,
    );
    if (!launched) {
        self.stats.mtp_preproject_fused_fallbacks += 1;
        return null;
    }

    const shape = try allocShape2(self.allocator, 1, request.draft_hidden);
    errdefer self.allocator.free(shape);
    self.stats.launch_linear += 1;
    self.stats.mtp_preproject_fused_hits += 1;
    switch (weight_dtype_code) {
        0 => self.stats.mtp_preproject_fused_f32_weight_hits += 1,
        1 => self.stats.mtp_preproject_fused_bf16_weight_hits += 1,
        2 => self.stats.mtp_preproject_fused_f16_weight_hits += 1,
        else => {},
    }
    return createTensorWithDType(self, output_device, shape, elem_count, .f32);
}

fn gemma4MtpMaskedSelect(ctx: *anyopaque, request: *const ops.Gemma4MtpMaskedSelectRequest) anyerror!?u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaMtpMaskedSelectFusionEnabled()) {
        self.stats.mtp_masked_select_fused_fallbacks += 1;
        return null;
    }
    const hidden_tensor = tensorFromCt(request.assistant_hidden);
    const logits_tensor = tensorFromCt(request.logits);
    const centroid_weight_tensor = tensorFromCt(request.centroid_weight);
    const ordering_tensor = tensorFromCt(request.token_ordering);
    try ensureF32(hidden_tensor);
    try ensureF32(logits_tensor);
    try ensureF32(ordering_tensor);
    if (request.hidden_size == 0 or request.vocab_size == 0 or request.num_centroids == 0 or request.top_k == 0) return null;
    if (request.vocab_size % request.num_centroids != 0) return null;
    if (request.num_centroids > 4096) {
        self.stats.mtp_masked_select_fused_fallbacks += 1;
        return null;
    }
    try ensureCount(hidden_tensor, request.hidden_size);
    try ensureCount(logits_tensor, request.vocab_size);
    try ensureCount(ordering_tensor, request.vocab_size);
    if (centroid_weight_tensor.quant_type != null) {
        self.stats.mtp_masked_select_fused_fallbacks += 1;
        return null;
    }
    const weight_dtype_code: u32 = switch (centroid_weight_tensor.dtype) {
        .f32 => 0,
        .bf16 => 1,
        .f16 => 2,
        else => {
            self.stats.mtp_masked_select_fused_fallbacks += 1;
            return null;
        },
    };
    try ensureCount(centroid_weight_tensor, try checkedMul(request.num_centroids, request.hidden_size));

    var token_device = try allocDeviceBuffer(self, @sizeOf(u32));
    defer releaseDeviceBuffer(self, &token_device);
    const launched = try self.kernels.launchGemma4MtpMaskedSelectF32(
        &self.ctx,
        token_device,
        hidden_tensor.buffer,
        logits_tensor.buffer,
        centroid_weight_tensor.buffer,
        ordering_tensor.buffer,
        request.hidden_size,
        request.vocab_size,
        request.num_centroids,
        request.top_k,
        request.use_inverse_ordering,
        weight_dtype_code,
    );
    if (!launched) {
        self.stats.mtp_masked_select_fused_fallbacks += 1;
        return null;
    }
    self.stats.launch_linear += 1;
    self.stats.launch_argmax += 1;
    self.stats.mtp_masked_select_fused_hits += 1;
    switch (weight_dtype_code) {
        0 => self.stats.mtp_masked_select_fused_f32_weight_hits += 1,
        1 => self.stats.mtp_masked_select_fused_bf16_weight_hits += 1,
        2 => self.stats.mtp_masked_select_fused_f16_weight_hits += 1,
        else => {},
    }
    var token: u32 = 0;
    try copyToHostTrackedAndSync(self, token_device, std.mem.asBytes(&token));
    return token;
}

fn gemma4MtpMaskedSelectFromHidden(ctx: *anyopaque, request: *const ops.Gemma4MtpMaskedSelectFromHiddenRequest) anyerror!?u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaMtpMaskedSelectHiddenFusionEnabled()) {
        self.stats.mtp_masked_select_hidden_fused_fallbacks += 1;
        return null;
    }
    const hidden_tensor = tensorFromCt(request.assistant_hidden);
    const lm_head_tensor = tensorFromCt(request.lm_head_weight);
    const centroid_weight_tensor = tensorFromCt(request.centroid_weight);
    const ordering_tensor = tensorFromCt(request.token_ordering);
    try ensureF32(hidden_tensor);
    try ensureF32(ordering_tensor);
    if (request.hidden_size == 0 or request.vocab_size == 0 or request.num_centroids == 0 or request.top_k == 0) return null;
    if (request.vocab_size % request.num_centroids != 0) return null;
    if (request.num_centroids > 4096) {
        self.stats.mtp_masked_select_hidden_fused_fallbacks += 1;
        return null;
    }
    if (request.top_k > 128) {
        self.stats.mtp_masked_select_hidden_fused_fallbacks += 1;
        return null;
    }
    try ensureCount(hidden_tensor, request.hidden_size);
    try ensureCount(ordering_tensor, request.vocab_size);
    if (lm_head_tensor.quant_type != null or centroid_weight_tensor.quant_type != null) {
        self.stats.mtp_masked_select_hidden_fused_fallbacks += 1;
        return null;
    }
    const lm_head_dtype_code: u32 = switch (lm_head_tensor.dtype) {
        .f32 => 0,
        .bf16 => 1,
        .f16 => 2,
        else => {
            self.stats.mtp_masked_select_hidden_fused_fallbacks += 1;
            return null;
        },
    };
    const centroid_dtype_code: u32 = switch (centroid_weight_tensor.dtype) {
        .f32 => 0,
        .bf16 => 1,
        .f16 => 2,
        else => {
            self.stats.mtp_masked_select_hidden_fused_fallbacks += 1;
            return null;
        },
    };
    try ensureCount(lm_head_tensor, try checkedMul(request.vocab_size, request.hidden_size));
    try ensureCount(centroid_weight_tensor, try checkedMul(request.num_centroids, request.hidden_size));
    if (request.use_inverse_ordering) {
        self.stats.mtp_masked_select_hidden_fused_fallbacks += 1;
        return null;
    }
    const cluster_size = request.vocab_size / request.num_centroids;
    const candidate_count = try checkedMul(request.top_k, cluster_size);
    if (candidate_count == 0 or candidate_count > 65536) {
        self.stats.mtp_masked_select_hidden_fused_fallbacks += 1;
        return null;
    }

    var token_device = try allocDeviceBuffer(self, @sizeOf(u32));
    defer releaseDeviceBuffer(self, &token_device);
    var centroid_scores = try allocDeviceBuffer(self, try checkedMul(request.num_centroids, @sizeOf(f32)));
    defer releaseDeviceBuffer(self, &centroid_scores);
    var top_centroids = try allocDeviceBuffer(self, try checkedMul(request.top_k, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &top_centroids);
    var partial_values = try allocDeviceBuffer(self, try checkedMul(candidate_count, @sizeOf(f32)));
    defer releaseDeviceBuffer(self, &partial_values);
    var partial_tokens = try allocDeviceBuffer(self, try checkedMul(candidate_count, @sizeOf(u32)));
    defer releaseDeviceBuffer(self, &partial_tokens);
    const launched = try self.kernels.launchGemma4MtpMaskedSelectHiddenMultiF32(
        &self.ctx,
        token_device,
        centroid_scores,
        top_centroids,
        partial_values,
        partial_tokens,
        hidden_tensor.buffer,
        lm_head_tensor.buffer,
        centroid_weight_tensor.buffer,
        ordering_tensor.buffer,
        request.hidden_size,
        request.vocab_size,
        request.num_centroids,
        request.top_k,
        lm_head_dtype_code,
        centroid_dtype_code,
    );
    if (!launched) {
        self.stats.mtp_masked_select_hidden_fused_fallbacks += 1;
        return null;
    }
    self.stats.launch_linear += 1;
    self.stats.launch_argmax += 1;
    self.stats.mtp_masked_select_hidden_fused_hits += 1;
    self.stats.mtp_masked_select_hidden_multiblock_hits += 1;
    if (lm_head_dtype_code == 1 and centroid_dtype_code == 1) self.stats.mtp_masked_select_hidden_fused_bf16_hits += 1;
    var token: u32 = 0;
    try copyToHostTrackedAndSync(self, token_device, std.mem.asBytes(&token));
    return token;
}

fn gemma4MtpMaskedArgmax(ctx: *anyopaque, request: *const ops.Gemma4MtpMaskedArgmaxRequest) anyerror!?u32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const logits_tensor = tensorFromCt(request.logits);
    const centroid_tensor = tensorFromCt(request.centroid_logits);
    const ordering_tensor = tensorFromCt(request.token_ordering);
    try ensureF32(logits_tensor);
    try ensureF32(centroid_tensor);
    try ensureF32(ordering_tensor);
    if (request.vocab_size == 0 or request.num_centroids == 0 or request.top_k == 0) return null;
    try ensureCount(logits_tensor, request.vocab_size);
    try ensureCount(centroid_tensor, request.num_centroids);
    try ensureCount(ordering_tensor, request.vocab_size);

    var token_device = try allocDeviceBuffer(self, @sizeOf(u32));
    defer releaseDeviceBuffer(self, &token_device);
    const launched = try self.kernels.launchGemma4MtpMaskedArgmaxF32(
        &self.ctx,
        token_device,
        logits_tensor.buffer,
        centroid_tensor.buffer,
        ordering_tensor.buffer,
        request.vocab_size,
        request.num_centroids,
        request.top_k,
        request.use_inverse_ordering,
    );
    if (!launched) {
        self.stats.mtp_masked_argmax_fallbacks += 1;
        return null;
    }
    self.stats.launch_argmax += 1;
    self.stats.mtp_masked_argmax_hits += 1;
    var token: u32 = 0;
    try copyToHostTrackedAndSync(self, token_device, std.mem.asBytes(&token));
    return token;
}

fn gemma4MtpTokenIsEos(token: usize, eos_token_ids: []const i32) bool {
    for (eos_token_ids) |eos| {
        if (eos >= 0 and token == @as(usize, @intCast(eos))) return true;
    }
    return false;
}

const mtp_verify_commit_result_words = 12;

fn gemma4MtpVerifyCommitDeviceResult(
    self: *CudaCompute,
    request: *const ops.Gemma4MtpVerifyCommitRequest,
    verify_len: usize,
) anyerror!?ops.Gemma4MtpVerifyCommitResult {
    if (!cudaMtpVerifyDeviceResultEnabled()) return null;

    var target_choices_device = (try linearNoBiasArgmaxRowsSuppressDevice(
        self,
        request.input,
        request.weight,
        verify_len,
        request.in_dim,
        request.out_dim,
        request.suppress_token_ids,
        false,
    )) orelse {
        self.stats.mtp_verify_commit_device_fallbacks += 1;
        return null;
    };
    defer releaseDeviceBufferLease(self, &target_choices_device);

    var draft_tokens_device = try allocDeviceBuffer(self, try checkedMul(request.draft_tokens.len, @sizeOf(i64)));
    defer releaseDeviceBuffer(self, &draft_tokens_device);
    try copyFromHostTracked(self, draft_tokens_device, std.mem.sliceAsBytes(request.draft_tokens));

    var eos_token_ids_device: buffer_mod.DeviceBuffer = .{};
    if (request.eos_token_ids.len != 0) {
        eos_token_ids_device = try allocDeviceBuffer(self, try checkedMul(request.eos_token_ids.len, @sizeOf(i32)));
        try copyFromHostTracked(self, eos_token_ids_device, std.mem.sliceAsBytes(request.eos_token_ids));
    }
    defer if (eos_token_ids_device.len != 0) releaseDeviceBuffer(self, &eos_token_ids_device);

    var result_device = try allocDeviceBuffer(self, mtp_verify_commit_result_words * @sizeOf(u32));
    defer releaseDeviceBuffer(self, &result_device);
    const launched = try self.kernels.launchGemma4MtpVerifyCommitU32(
        &self.ctx,
        result_device,
        target_choices_device.buffer,
        draft_tokens_device,
        eos_token_ids_device,
        request.draft_tokens.len,
        request.eos_token_ids.len,
        request.accept_bonus,
    );
    if (!launched) {
        self.stats.mtp_verify_commit_device_fallbacks += 1;
        return null;
    }
    self.stats.launch_other += 1;

    var words: [mtp_verify_commit_result_words]u32 = undefined;
    try copyToHostTrackedAndSync(self, result_device, std.mem.sliceAsBytes(&words));
    self.stats.mtp_verify_commit_result_downloads += 1;
    if (words[11] != 0) return error.InvalidModelOutput;

    self.stats.mtp_verify_commit_device_hits += 1;
    return .{
        .target_choices = &.{},
        .target_choices_owned = false,
        .compact_device_result = true,
        .correction_token = if (words[2] != 0) words[9] else null,
        .bonus_token = if (words[3] != 0) words[10] else null,
        .matched_drafts = @intCast(words[0]),
        .accepted = @intCast(words[1]),
        .correction_added = words[2] != 0,
        .had_bonus = words[3] != 0,
        .bonus_skipped = words[4] != 0,
        .hit_eos = words[5] != 0,
        .commit_forward_required = words[6] != 0,
        .accepted_hidden_row = if (words[7] != 0) @intCast(words[8]) else null,
    };
}

fn gemma4MtpVerifyCommit(ctx: *anyopaque, request: *const ops.Gemma4MtpVerifyCommitRequest) anyerror!?ops.Gemma4MtpVerifyCommitResult {
    if (request.draft_tokens.len == 0) return null;
    const verify_len = request.draft_tokens.len + 1;
    if (request.rows < verify_len) return error.InvalidTensorShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));

    if (try gemma4MtpVerifyCommitDeviceResult(self, request, verify_len)) |result| {
        return result;
    }

    const target_choices = (try linearNoBiasArgmaxRowsSuppress(
        ctx,
        request.input,
        request.weight,
        verify_len,
        request.in_dim,
        request.out_dim,
        request.suppress_token_ids,
        request.allocator,
    )) orelse return null;
    errdefer request.allocator.free(target_choices);
    if (target_choices.len < verify_len) return error.InvalidTensorShape;

    var matched_drafts: usize = 0;
    var accepted: usize = 0;
    var correction_added = false;
    var hit_eos = false;

    for (request.draft_tokens, 0..) |draft_token_raw, i| {
        if (draft_token_raw < 0) return error.InvalidModelOutput;
        const draft_token: usize = @intCast(draft_token_raw);
        const target_choice: usize = @intCast(target_choices[i]);
        if (target_choice == draft_token) {
            matched_drafts += 1;
            accepted += 1;
            if (gemma4MtpTokenIsEos(target_choice, request.eos_token_ids)) {
                hit_eos = true;
                break;
            }
        } else {
            correction_added = true;
            accepted = matched_drafts + 1;
            if (gemma4MtpTokenIsEos(target_choice, request.eos_token_ids)) {
                hit_eos = true;
            }
            break;
        }
    }

    const can_bonus = matched_drafts == request.draft_tokens.len and !hit_eos;
    const had_bonus = can_bonus and request.accept_bonus;
    const bonus_skipped = can_bonus and !request.accept_bonus;
    if (had_bonus) {
        const bonus_token: usize = @intCast(target_choices[request.draft_tokens.len]);
        accepted += 1;
        if (gemma4MtpTokenIsEos(bonus_token, request.eos_token_ids)) {
            hit_eos = true;
        }
    }

    const commit_forward_required = correction_added or had_bonus;
    const accepted_hidden_row: ?usize = if (accepted != 0 and !commit_forward_required)
        accepted - 1
    else
        null;

    return .{
        .target_choices = target_choices,
        .target_choices_owned = true,
        .matched_drafts = matched_drafts,
        .accepted = accepted,
        .correction_added = correction_added,
        .had_bonus = had_bonus,
        .bonus_skipped = bonus_skipped,
        .hit_eos = hit_eos,
        .commit_forward_required = commit_forward_required,
        .accepted_hidden_row = accepted_hidden_row,
    };
}

fn linearNoBiasQkv(ctx: *anyopaque, input: CT, q_weight: CT, k_weight: CT, v_weight: CT, rows: usize, in_dim: usize, q_out_dim: usize, kv_out_dim: usize) anyerror!?ops.LinearNoBiasTripleResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (cudaDisableFusedQkv()) return null;
    const input_tensor = tensorFromCt(input);
    const q_weight_tensor = tensorFromCt(q_weight);
    const k_weight_tensor = tensorFromCt(k_weight);
    const v_weight_tensor = tensorFromCt(v_weight);
    try ensureF32(input_tensor);
    const q_mirror = weightBf16MirrorForRows(self, q_weight_tensor, rows);
    const k_mirror = weightBf16MirrorForRows(self, k_weight_tensor, rows);
    const v_mirror = weightBf16MirrorForRows(self, v_weight_tensor, rows);
    const use_hybrid_bf16 = q_mirror != null and k_mirror != null and v_mirror != null;
    // W4A16 QKV: when the tensor-core packs are present AND no BF16 mirror is
    // attached, defer to the individual linearNoBias path (which routes Q4_0
    // prefill through tc_hmma). When a mirror IS present, keep the fused
    // BF16-mirror triple (cuBLASLt is faster + equal quality).
    if (shouldTryQ4_0TcHmmaPrefill(self.tuned_route_gates.q4_0_tc_hmma_prefill, rows, use_hybrid_bf16) and
        tcQuantBuffer(q_weight_tensor, .q4_0_hmma) != null and
        tcQuantBuffer(k_weight_tensor, .q4_0_hmma) != null and
        tcQuantBuffer(v_weight_tensor, .q4_0_hmma) != null) return null;
    const q_weight_bf16 = if (use_hybrid_bf16) q_mirror.? else q_weight_tensor.buffer;
    const k_weight_bf16 = if (use_hybrid_bf16) k_mirror.? else k_weight_tensor.buffer;
    const v_weight_bf16 = if (use_hybrid_bf16) v_mirror.? else v_weight_tensor.buffer;
    const use_q8 = !use_hybrid_bf16 and isKnownQuant(q_weight_tensor, .Q8_0) and isKnownQuant(k_weight_tensor, .Q8_0) and isKnownQuant(v_weight_tensor, .Q8_0);
    const use_q4_0 = !use_hybrid_bf16 and isKnownQuant(q_weight_tensor, .Q4_0) and isKnownQuant(k_weight_tensor, .Q4_0) and isKnownQuant(v_weight_tensor, .Q4_0);
    const use_q4 = !use_hybrid_bf16 and isKnownQuant(q_weight_tensor, .Q4_K) and isKnownQuant(k_weight_tensor, .Q4_K) and isKnownQuant(v_weight_tensor, .Q4_K);
    const use_q4_q4_f32 = !use_hybrid_bf16 and isKnownQuant(q_weight_tensor, .Q4_K) and isKnownQuant(k_weight_tensor, .Q4_K) and
        v_weight_tensor.dtype == .f32 and v_weight_tensor.quant_type == null;
    const use_f32 = q_weight_tensor.dtype == .f32 and q_weight_tensor.quant_type == null and
        k_weight_tensor.dtype == .f32 and k_weight_tensor.quant_type == null and
        v_weight_tensor.dtype == .f32 and v_weight_tensor.quant_type == null;
    const use_f16 = isF16Weight(q_weight_tensor) and isF16Weight(k_weight_tensor) and isF16Weight(v_weight_tensor);
    const use_bf16 = use_hybrid_bf16 or (isBf16Weight(q_weight_tensor) and isBf16Weight(k_weight_tensor) and isBf16Weight(v_weight_tensor));
    if (!use_q8 and !use_q4_0 and !use_q4 and !use_q4_q4_f32 and !use_f32 and !use_f16 and !use_bf16) {
        self.stats.qkv_fallback_unsupported += 1;
        return null;
    }
    if (use_f32) {
        try ensureF32(q_weight_tensor);
        try ensureF32(k_weight_tensor);
        try ensureF32(v_weight_tensor);
    } else if (use_q4_q4_f32) {
        try ensureF32(v_weight_tensor);
    }
    if (in_dim == 0 or q_out_dim == 0 or kv_out_dim == 0) return null;
    if (use_q8 and in_dim % 32 != 0) return null;
    if (use_q4_0 and in_dim % 32 != 0) return null;
    if ((use_q4 or use_q4_q4_f32) and in_dim % 256 != 0) return null;
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(q_weight_tensor, try checkedMul(q_out_dim, in_dim));
    try ensureCount(k_weight_tensor, try checkedMul(kv_out_dim, in_dim));
    try ensureCount(v_weight_tensor, try checkedMul(kv_out_dim, in_dim));

    const q_count = try checkedMul(rows, q_out_dim);
    const kv_count = try checkedMul(rows, kv_out_dim);
    const q_shape = try allocShape2(self.allocator, rows, q_out_dim);
    var q_shape_owned = false;
    errdefer if (!q_shape_owned) self.allocator.free(q_shape);
    const k_shape = try allocShape2(self.allocator, rows, kv_out_dim);
    var k_shape_owned = false;
    errdefer if (!k_shape_owned) self.allocator.free(k_shape);
    const v_shape = try allocShape2(self.allocator, rows, kv_out_dim);
    var v_shape_owned = false;
    errdefer if (!v_shape_owned) self.allocator.free(v_shape);
    var q_device = try allocDeviceBuffer(self, q_count * @sizeOf(f32));
    var q_device_owned = false;
    errdefer if (!q_device_owned) q_device.free(&self.ctx);
    var k_device = try allocDeviceBuffer(self, kv_count * @sizeOf(f32));
    var k_device_owned = false;
    errdefer if (!k_device_owned) k_device.free(&self.ctx);
    var v_device = try allocDeviceBuffer(self, kv_count * @sizeOf(f32));
    var v_device_owned = false;
    errdefer if (!v_device_owned) v_device.free(&self.ctx);

    var prefill_profile_scope = beginPrefillProfile(self, if (use_q4_0) .q4_qkv else if (use_bf16) .bf16_qkv else null, rows);
    defer if (prefill_profile_scope) |*scope| scope.end();

    if (use_q8) {
        self.kernels.launchLinearQ8_0QkvNoBiasTile4F32(
            &self.ctx,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.qkv_kernel_unavailable += 1;
                return null;
            },
            else => return err,
        };
        self.stats.qkv_fused_q8 += 1;
        self.stats.launch_linear_qkv += 1;
    } else if (use_q4_0) {
        const combined_out_dim = q_out_dim + 2 * kv_out_dim;
        const qkv_op: CudaQ4RouteOp = if (q_out_dim == kv_out_dim) .qkv else .qkv_gqa;
        var route = CudaQ4KernelLaunch{
            .kernel_id = "termite_linear_q4_0_qkv_nobias_f32_tile4",
            .grid = .{ rows * ((q_out_dim + 3) / 4 + 2 * ((kv_out_dim + 3) / 4)), 1, 1 },
            .block = .{ 256, 1, 1 },
        };
        const used_q4_0_q8_1_dp4a = try tryLaunchLinearQ4_0QkvQ8_1Dp4a(
            self,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        );
        if (used_q4_0_q8_1_dp4a) {
            route = if (cudaQ4_0QkvQ8_1Tile8Enabled(self)) blk: {
                const w8 = cudaQ4_0QkvQ8_1Tile8W8Enabled(in_dim);
                break :blk .{
                    .kernel_id = if (w8)
                        "termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8"
                    else
                        "termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8",
                    .grid = .{ rows * ((q_out_dim + 7) / 8 + 2 * ((kv_out_dim + 7) / 8)), 1, 1 },
                    .block = .{ if (w8) 256 else 128, 1, 1 },
                };
            } else .{
                .kernel_id = "termite_linear_q4_0_qkv_nobias_q8_1_f32_tile4",
                .grid = .{ rows * ((q_out_dim + 3) / 4 + 2 * ((kv_out_dim + 3) / 4)), 1, 1 },
                .block = .{ 128, 1, 1 },
            };
        }
        const used_q4_0_tile8 = blk: {
            if (!used_q4_0_q8_1_dp4a and rows == 1 and cudaQ4_0QkvTile8Enabled()) {
                self.kernels.launchLinearQ4_0QkvNoBiasTile8F32(
                    &self.ctx,
                    q_device,
                    k_device,
                    v_device,
                    input_tensor.buffer,
                    q_weight_tensor.buffer,
                    k_weight_tensor.buffer,
                    v_weight_tensor.buffer,
                    rows,
                    in_dim,
                    q_out_dim,
                    kv_out_dim,
                ) catch |err| switch (err) {
                    error.CudaKernelUnavailable, error.InvalidCudaState => break :blk false,
                    else => return err,
                };
                route = .{
                    .kernel_id = "termite_linear_q4_0_qkv_nobias_f32_tile8",
                    .grid = .{ rows * ((q_out_dim + 7) / 8 + 2 * ((kv_out_dim + 7) / 8)), 1, 1 },
                    .block = .{ 256, 1, 1 },
                };
                break :blk true;
            }
            break :blk false;
        };
        if (!used_q4_0_q8_1_dp4a and !used_q4_0_tile8) {
            const used_q4_0_tile4_w4 = blk: {
                if (rows == 1 and cudaQ4_0QkvTile4W4Enabled()) {
                    self.kernels.launchLinearQ4_0QkvNoBiasTile4W4F32(
                        &self.ctx,
                        q_device,
                        k_device,
                        v_device,
                        input_tensor.buffer,
                        q_weight_tensor.buffer,
                        k_weight_tensor.buffer,
                        v_weight_tensor.buffer,
                        rows,
                        in_dim,
                        q_out_dim,
                        kv_out_dim,
                    ) catch |err| switch (err) {
                        error.CudaKernelUnavailable, error.InvalidCudaState => break :blk false,
                        else => return err,
                    };
                    route = .{
                        .kernel_id = "termite_linear_q4_0_qkv_nobias_f32_tile4_w4",
                        .grid = .{ rows * ((q_out_dim + 3) / 4 + 2 * ((kv_out_dim + 3) / 4)), 1, 1 },
                        .block = .{ 128, 1, 1 },
                    };
                    break :blk true;
                }
                break :blk false;
            };
            if (!used_q4_0_tile4_w4) {
                self.kernels.launchLinearQ4_0QkvNoBiasTile4F32(
                    &self.ctx,
                    q_device,
                    k_device,
                    v_device,
                    input_tensor.buffer,
                    q_weight_tensor.buffer,
                    k_weight_tensor.buffer,
                    v_weight_tensor.buffer,
                    rows,
                    in_dim,
                    q_out_dim,
                    kv_out_dim,
                ) catch |err| switch (err) {
                    error.CudaKernelUnavailable => {
                        self.stats.qkv_kernel_unavailable += 1;
                        return null;
                    },
                    else => return err,
                };
            }
        }
        noteQ4Route(
            self,
            qkv_op,
            rows,
            in_dim,
            combined_out_dim,
            .qkv,
            .none,
            route.provider,
            route.kernel_id,
            route.grid,
            route.block,
        );
        self.stats.qkv_fused_q4_0 += 1;
        if (used_q4_0_tile8 or (used_q4_0_q8_1_dp4a and cudaQ4_0QkvQ8_1Tile8Enabled(self))) {
            self.stats.qkv_fused_q4_0_tile8 += 1;
        } else {
            self.stats.qkv_fused_q4_0_tile4 += 1;
        }
        self.stats.launch_linear_qkv += 1;
    } else if (use_q4) {
        self.kernels.launchLinearQ4KQkvNoBiasTiledF32(
            &self.ctx,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.qkv_kernel_unavailable += 1;
                return null;
            },
            else => return err,
        };
        self.stats.qkv_fused_q4 += 1;
        self.stats.launch_linear_qkv += 1;
    } else if (use_q4_q4_f32) {
        self.kernels.launchLinearQ4KQ4KF32QkvNoBiasTiled(
            &self.ctx,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.qkv_kernel_unavailable += 1;
                return null;
            },
            else => return err,
        };
        self.stats.qkv_fused_q4_q4_f32 += 1;
        self.stats.launch_linear_qkv += 1;
    } else if (use_f16) {
        if (!try tryCublasLtF16Qkv(
            self,
            q_device,
            k_device,
            v_device,
            input_tensor,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        )) {
            self.stats.f16_cublaslt_fallbacks += 1;
            try self.kernels.launchLinearF16WeightF32Tiled(&self.ctx, q_device, input_tensor.buffer, q_weight_tensor.buffer, rows, in_dim, q_out_dim);
            try self.kernels.launchLinearF16WeightF32Tiled(&self.ctx, k_device, input_tensor.buffer, k_weight_tensor.buffer, rows, in_dim, kv_out_dim);
            try self.kernels.launchLinearF16WeightF32Tiled(&self.ctx, v_device, input_tensor.buffer, v_weight_tensor.buffer, rows, in_dim, kv_out_dim);
            self.stats.f16_scalar_linear_calls += 3;
        }
        self.stats.qkv_fused_f16 += 1;
        self.stats.launch_linear_qkv += 1;
    } else if (use_bf16) {
        if (try tryCublasLtBf16Qkv(
            self,
            q_device,
            k_device,
            v_device,
            input_tensor,
            q_weight_bf16,
            k_weight_bf16,
            v_weight_bf16,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        )) {
            // cuBLASLt counters are updated by the helper.
        } else {
            self.stats.bf16_cublaslt_fallbacks += 1;
            self.kernels.launchLinearBf16WeightF32QkvNoBiasTiled(
                &self.ctx,
                q_device,
                k_device,
                v_device,
                input_tensor.buffer,
                q_weight_bf16,
                k_weight_bf16,
                v_weight_bf16,
                rows,
                in_dim,
                q_out_dim,
                kv_out_dim,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable => {
                    self.stats.qkv_kernel_unavailable += 1;
                    return null;
                },
                else => return err,
            };
            self.stats.bf16_scalar_qkv_calls += 1;
        }
        self.stats.qkv_fused_f32 += 1;
        self.stats.launch_linear_qkv += 1;
    } else {
        self.kernels.launchLinearF32QkvNoBiasTiled(
            &self.ctx,
            q_device,
            k_device,
            v_device,
            input_tensor.buffer,
            q_weight_tensor.buffer,
            k_weight_tensor.buffer,
            v_weight_tensor.buffer,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.qkv_kernel_unavailable += 1;
                return null;
            },
            else => return err,
        };
        self.stats.qkv_fused_f32 += 1;
        self.stats.launch_linear_qkv += 1;
    }

    const q = try createTensor(self, q_device, q_shape, q_count);
    q_shape_owned = true;
    q_device_owned = true;
    errdefer freeTensor(ctx, q);
    const k = try createTensor(self, k_device, k_shape, kv_count);
    k_shape_owned = true;
    k_device_owned = true;
    errdefer freeTensor(ctx, k);
    const v = try createTensor(self, v_device, v_shape, kv_count);
    v_shape_owned = true;
    v_device_owned = true;
    return .{ .first = q, .second = k, .third = v };
}

fn linearTriple(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, weight_c: CT, bias_c: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearTripleResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const weight_c_tensor = tensorFromCt(weight_c);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);
    const bias_c_tensor = tensorFromCt(bias_c);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureF32(bias_c_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_c_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);
    try ensureCount(bias_c_tensor, out_dim);

    // BGE-M3/XLM-R uses three biased projections for every encoder attention
    // block. At prefill sizes BF16 mirrors and native FP16 weights are faster
    // with cuBLASLt, but the generic triple fallback stages the same F32
    // activation three times. Reuse the QKV route so input staging happens
    // once, then apply each learned bias in place.
    //
    // Decode and raw Q4_0 kernels retain their established dispatch and
    // numerical behavior. `linearNoBiasQkv` returns null when its fusion is
    // disabled or ineligible, preserving the three-linear fallback below.
    const use_bf16_triple = rows > 1 and
        weightBf16MirrorForRows(self, weight_a_tensor, rows) != null and
        weightBf16MirrorForRows(self, weight_b_tensor, rows) != null and
        weightBf16MirrorForRows(self, weight_c_tensor, rows) != null;
    const use_f16_triple = isF16Weight(weight_a_tensor) and
        isF16Weight(weight_b_tensor) and isF16Weight(weight_c_tensor);
    if (use_bf16_triple or use_f16_triple) {
        if (try linearNoBiasQkv(ctx, input, weight_a, weight_b, weight_c, rows, in_dim, out_dim, out_dim)) |qkv| {
            errdefer freeTensor(ctx, qkv.first);
            errdefer freeTensor(ctx, qkv.second);
            errdefer freeTensor(ctx, qkv.third);
            try self.kernels.launchAddBiasRowsF32(&self.ctx, tensorFromCt(qkv.first).buffer, bias_a_tensor.buffer, rows, out_dim);
            try self.kernels.launchAddBiasRowsF32(&self.ctx, tensorFromCt(qkv.second).buffer, bias_b_tensor.buffer, rows, out_dim);
            try self.kernels.launchAddBiasRowsF32(&self.ctx, tensorFromCt(qkv.third).buffer, bias_c_tensor.buffer, rows, out_dim);
            self.dispatch_stats.note(self.allocator, .linear_triple, if (use_f16_triple) .f16 else .bf16, .dense_lt, .bias, .none, rows, in_dim, out_dim, 0);
            return .{ .first = qkv.first, .second = qkv.second, .third = qkv.third };
        }
    }

    if (isKnownQuant(weight_a_tensor, .Q4_K) and isKnownQuant(weight_b_tensor, .Q4_K) and isKnownQuant(weight_c_tensor, .Q4_K)) {
        const out_count = try checkedMul(rows, out_dim);
        const shape_a = try allocShape2(self.allocator, rows, out_dim);
        var shape_a_owned = false;
        errdefer if (!shape_a_owned) self.allocator.free(shape_a);
        const shape_b = try allocShape2(self.allocator, rows, out_dim);
        var shape_b_owned = false;
        errdefer if (!shape_b_owned) self.allocator.free(shape_b);
        const shape_c = try allocShape2(self.allocator, rows, out_dim);
        var shape_c_owned = false;
        errdefer if (!shape_c_owned) self.allocator.free(shape_c);
        var device_a = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
        var device_a_owned = false;
        errdefer if (!device_a_owned) device_a.free(&self.ctx);
        var device_b = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
        var device_b_owned = false;
        errdefer if (!device_b_owned) device_b.free(&self.ctx);
        var device_c = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
        var device_c_owned = false;
        errdefer if (!device_c_owned) device_c.free(&self.ctx);
        if (mxbaiQ4TiledVariant(rows, in_dim, out_dim)) |variant| {
            if (try launchQ4KTcHmmaTripleBias(
                self,
                variant,
                device_a,
                device_b,
                device_c,
                input_tensor.buffer,
                weight_a_tensor,
                bias_a_tensor.buffer,
                weight_b_tensor,
                bias_b_tensor.buffer,
                weight_c_tensor,
                bias_c_tensor.buffer,
                rows,
                in_dim,
                out_dim,
            )) {
                self.dispatch_stats.note(self.allocator, .linear_triple, .q4_k, .q4_tc_hmma, .bias, .none, rows, in_dim, out_dim, 0);
            } else {
                try self.kernels.launchLinearQ4KTripleBiasTiledF32(
                    &self.ctx,
                    device_a,
                    device_b,
                    device_c,
                    input_tensor.buffer,
                    weight_a_tensor.buffer,
                    bias_a_tensor.buffer,
                    weight_b_tensor.buffer,
                    bias_b_tensor.buffer,
                    weight_c_tensor.buffer,
                    bias_c_tensor.buffer,
                    rows,
                    in_dim,
                    out_dim,
                );
                self.dispatch_stats.note(self.allocator, .linear_triple, .q4_k, .q4_simt, .bias, tcTripleFallbackReason(weight_a_tensor, weight_b_tensor, weight_c_tensor, variant, .q4_k_hmma), rows, in_dim, out_dim, 0);
            }
        } else {
            try self.kernels.launchLinearQ4KTripleBiasTiledF32(
                &self.ctx,
                device_a,
                device_b,
                device_c,
                input_tensor.buffer,
                weight_a_tensor.buffer,
                bias_a_tensor.buffer,
                weight_b_tensor.buffer,
                bias_b_tensor.buffer,
                weight_c_tensor.buffer,
                bias_c_tensor.buffer,
                rows,
                in_dim,
                out_dim,
            );
            self.dispatch_stats.note(self.allocator, .linear_triple, .q4_k, .q4_simt, .bias, tcUnavailableReason(), rows, in_dim, out_dim, 0);
        }
        self.stats.launch_linear += 1;
        const first = try createTensor(self, device_a, shape_a, out_count);
        shape_a_owned = true;
        device_a_owned = true;
        errdefer freeTensor(ctx, first);
        const second = try createTensor(self, device_b, shape_b, out_count);
        shape_b_owned = true;
        device_b_owned = true;
        errdefer freeTensor(ctx, second);
        const third = try createTensor(self, device_c, shape_c, out_count);
        shape_c_owned = true;
        device_c_owned = true;
        return .{ .first = first, .second = second, .third = third };
    }

    const first = try linear(ctx, input, weight_a, bias_a, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, first);
    const second = try linear(ctx, input, weight_b, bias_b, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, second);
    const third = try linear(ctx, input, weight_c, bias_c, rows, in_dim, out_dim);
    return .{ .first = first, .second = second, .third = third };
}

fn linearNoBiasPair(ctx: *anyopaque, input: CT, weight_a: CT, weight_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearNoBiasPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);

    try ensureF32(input_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));

    const mirror_a = weightBf16MirrorForRows(self, weight_a_tensor, rows);
    const mirror_b = weightBf16MirrorForRows(self, weight_b_tensor, rows);
    const use_hybrid_bf16 = mirror_a != null and mirror_b != null;
    const weight_a_bf16 = if (use_hybrid_bf16) mirror_a.? else weight_a_tensor.buffer;
    const weight_b_bf16 = if (use_hybrid_bf16) mirror_b.? else weight_b_tensor.buffer;
    const use_q8 = !use_hybrid_bf16 and isKnownQuant(weight_a_tensor, .Q8_0) and isKnownQuant(weight_b_tensor, .Q8_0);
    const use_q4_0 = !use_hybrid_bf16 and isKnownQuant(weight_a_tensor, .Q4_0) and isKnownQuant(weight_b_tensor, .Q4_0);
    const use_q4 = !use_hybrid_bf16 and isKnownQuant(weight_a_tensor, .Q4_K) and isKnownQuant(weight_b_tensor, .Q4_K);
    const use_f16 = isF16Weight(weight_a_tensor) and isF16Weight(weight_b_tensor);
    const use_bf16 = use_hybrid_bf16 or (isBf16Weight(weight_a_tensor) and isBf16Weight(weight_b_tensor));
    if (!use_q8 and !use_q4_0 and !use_q4 and !use_f16 and !use_bf16) {
        self.stats.linear_pair_fallbacks += 1;
        const first = try linearNoBias(ctx, input, weight_a, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, first);
        const second = try linearNoBias(ctx, input, weight_b, rows, in_dim, out_dim);
        return .{ .first = first, .second = second };
    }
    if (use_q8 and in_dim % 32 != 0) return error.InvalidShape;
    if (use_q4_0 and in_dim % 32 != 0) return error.InvalidShape;
    if (use_q4 and in_dim % 256 != 0) return error.InvalidShape;

    const out_count = try checkedMul(rows, out_dim);
    const shape_a = try allocShape2(self.allocator, rows, out_dim);
    var shape_a_owned = false;
    errdefer if (!shape_a_owned) self.allocator.free(shape_a);
    const shape_b = try allocShape2(self.allocator, rows, out_dim);
    var shape_b_owned = false;
    errdefer if (!shape_b_owned) self.allocator.free(shape_b);
    var device_a = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_a_owned = false;
    errdefer if (!device_a_owned) device_a.free(&self.ctx);
    var device_b = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_b_owned = false;
    errdefer if (!device_b_owned) device_b.free(&self.ctx);

    var prefill_profile_scope = beginPrefillProfile(self, if (use_bf16) .bf16_pair else null, rows);
    defer if (prefill_profile_scope) |*scope| scope.end();

    if (shouldTryQ4_0TcHmmaPrefill(self.tuned_route_gates.q4_0_tc_hmma_prefill, rows, use_hybrid_bf16) and
        tcQuantBuffer(weight_a_tensor, .q4_0_hmma) != null and
        tcQuantBuffer(weight_b_tensor, .q4_0_hmma) != null and
        try launchQ4_0TcHmmaNoBias(self, .tc_hmma, device_a, input_tensor.buffer, weight_a_tensor, rows, in_dim, out_dim) and
        try launchQ4_0TcHmmaNoBias(self, .tc_hmma, device_b, input_tensor.buffer, weight_b_tensor, rows, in_dim, out_dim))
    {
        // W4A16 tensor-core gate/up pair. Defers to the BF16 mirror pair when
        // present (use_hybrid_bf16): cuBLASLt is faster + equal quality.
        self.dispatch_stats.note(self.allocator, .linear_no_bias, .q4_0, .q4_tc_hmma, .pair, .none, rows, in_dim, out_dim, 0);
    } else if (use_f16) {
        if (!try tryCublasLtF16LinearPair(
            self,
            device_a,
            device_b,
            input_tensor,
            weight_a_tensor.buffer,
            weight_b_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        )) {
            self.stats.f16_cublaslt_fallbacks += 1;
            try self.kernels.launchLinearF16WeightF32Tiled(&self.ctx, device_a, input_tensor.buffer, weight_a_tensor.buffer, rows, in_dim, out_dim);
            try self.kernels.launchLinearF16WeightF32Tiled(&self.ctx, device_b, input_tensor.buffer, weight_b_tensor.buffer, rows, in_dim, out_dim);
            self.stats.f16_scalar_linear_calls += 2;
        }
    } else if (use_bf16) {
        if (try tryCublasLtBf16LinearPair(
            self,
            device_a,
            device_b,
            input_tensor,
            weight_a_bf16,
            weight_b_bf16,
            rows,
            in_dim,
            out_dim,
        )) {
            // cuBLASLt counters are updated by the helper.
        } else {
            self.stats.linear_pair_fallbacks += 1;
            self.stats.bf16_cublaslt_fallbacks += 1;
            try self.kernels.launchLinearBf16WeightF32Tiled(&self.ctx, device_a, input_tensor.buffer, weight_a_bf16, rows, in_dim, out_dim);
            try self.kernels.launchLinearBf16WeightF32Tiled(&self.ctx, device_b, input_tensor.buffer, weight_b_bf16, rows, in_dim, out_dim);
            self.stats.bf16_scalar_linear_calls += 2;
        }
    } else if (use_q8) {
        self.kernels.launchLinearQ8_0PairNoBiasTile4F32(
            &self.ctx,
            device_a,
            device_b,
            input_tensor.buffer,
            weight_a_tensor.buffer,
            weight_b_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.linear_pair_fallbacks += 1;
                device_a.free(&self.ctx);
                device_a_owned = true;
                device_b.free(&self.ctx);
                device_b_owned = true;
                self.allocator.free(shape_a);
                shape_a_owned = true;
                self.allocator.free(shape_b);
                shape_b_owned = true;
                const first = try linearNoBias(ctx, input, weight_a, rows, in_dim, out_dim);
                errdefer freeTensor(ctx, first);
                const second = try linearNoBias(ctx, input, weight_b, rows, in_dim, out_dim);
                return .{ .first = first, .second = second };
            },
            else => return err,
        };
        self.stats.linear_pair_fused_q8 += 1;
    } else if (use_q4_0) {
        var route = CudaQ4KernelLaunch{
            .kernel_id = "termite_linear_q4_0_pair_nobias_f32_tile4",
            .grid = .{ rows * ((out_dim + 3) / 4) * 2, 1, 1 },
            .block = .{ 256, 1, 1 },
        };
        const generated_pair_candidate = rows == 1 and self.generated_q4_0_gates.pair and
            runtimeJitShapeAllowsForCompute(self, .pair, rows, in_dim, out_dim);
        const used_q4_0_q8_1_dp4a = try tryLaunchLinearQ4_0PairQ8_1Dp4a(
            self,
            device_a,
            device_b,
            input_tensor.buffer,
            weight_a_tensor.buffer,
            weight_b_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        );
        if (used_q4_0_q8_1_dp4a) {
            route = if (cudaQ4_0PairQ8_1Tile8Enabled()) .{
                .kernel_id = "termite_linear_q4_0_pair_nobias_q8_1_f32_tile8",
                .grid = .{ rows * ((out_dim + 7) / 8) * 2, 1, 1 },
                .block = .{ 128, 1, 1 },
            } else if (cudaQ4_0PairQ8_1Tile4W8Enabled(self, in_dim)) .{
                .kernel_id = "termite_linear_q4_0_pair_nobias_q8_1_f32_tile4_w8",
                .grid = .{ rows * ((out_dim + 3) / 4) * 2, 1, 1 },
                .block = .{ 256, 1, 1 },
            } else .{
                .kernel_id = "termite_linear_q4_0_pair_nobias_q8_1_f32_tile4",
                .grid = .{ rows * ((out_dim + 3) / 4) * 2, 1, 1 },
                .block = .{ 128, 1, 1 },
            };
        }
        const used_q4_0_tile8 = blk: {
            if (!used_q4_0_q8_1_dp4a and rows == 1 and cudaQ4_0PairTile8Enabled()) {
                self.kernels.launchLinearQ4_0PairNoBiasTile8F32(
                    &self.ctx,
                    device_a,
                    device_b,
                    input_tensor.buffer,
                    weight_a_tensor.buffer,
                    weight_b_tensor.buffer,
                    rows,
                    in_dim,
                    out_dim,
                ) catch |err| switch (err) {
                    error.CudaKernelUnavailable, error.InvalidCudaState => break :blk false,
                    else => return err,
                };
                route = .{
                    .kernel_id = "termite_linear_q4_0_pair_nobias_f32_tile8",
                    .grid = .{ rows * ((out_dim + 7) / 8) * 2, 1, 1 },
                    .block = .{ 256, 1, 1 },
                };
                break :blk true;
            }
            break :blk false;
        };
        if (!used_q4_0_q8_1_dp4a and !used_q4_0_tile8) {
            const used_q4_0_generated_pair = blk: {
                if (generated_pair_candidate) {
                    self.kernels.launchLinearQ4_0GeneratedPairF32(
                        &self.ctx,
                        device_a,
                        device_b,
                        input_tensor.buffer,
                        weight_a_tensor.buffer,
                        weight_b_tensor.buffer,
                        rows,
                        in_dim,
                        out_dim,
                    ) catch |err| switch (err) {
                        error.CudaKernelUnavailable, error.InvalidCudaState => {
                            self.stats.q4_0_generated_pair_fallbacks += 1;
                            break :blk false;
                        },
                        else => return err,
                    };
                    self.stats.q4_0_generated_pair_hits += 1;
                    route = .{
                        .provider = .generated,
                        .kernel_id = quant_kernel_compiler.first_general_cuda_q4_0_pair_kernel_id,
                        .grid = .{ (out_dim + 3) / 4, 1, 1 },
                        .block = .{ 256, 1, 1 },
                    };
                    break :blk true;
                }
                break :blk false;
            };
            const used_q4_0_tile4_w4 = blk: {
                if (!used_q4_0_generated_pair and rows == 1 and cudaQ4_0PairTile4W4Enabled()) {
                    self.kernels.launchLinearQ4_0PairNoBiasTile4W4F32(
                        &self.ctx,
                        device_a,
                        device_b,
                        input_tensor.buffer,
                        weight_a_tensor.buffer,
                        weight_b_tensor.buffer,
                        rows,
                        in_dim,
                        out_dim,
                    ) catch |err| switch (err) {
                        error.CudaKernelUnavailable, error.InvalidCudaState => break :blk false,
                        else => return err,
                    };
                    route = .{
                        .provider = if (generated_pair_candidate) .fallback else .handwritten,
                        .kernel_id = "termite_linear_q4_0_pair_nobias_f32_tile4_w4",
                        .grid = .{ rows * ((out_dim + 3) / 4) * 2, 1, 1 },
                        .block = .{ 128, 1, 1 },
                    };
                    break :blk true;
                }
                break :blk false;
            };
            if (!used_q4_0_generated_pair and !used_q4_0_tile4_w4) {
                self.kernels.launchLinearQ4_0PairNoBiasTile4F32(
                    &self.ctx,
                    device_a,
                    device_b,
                    input_tensor.buffer,
                    weight_a_tensor.buffer,
                    weight_b_tensor.buffer,
                    rows,
                    in_dim,
                    out_dim,
                ) catch |err| switch (err) {
                    error.CudaKernelUnavailable => {
                        self.stats.linear_pair_fallbacks += 1;
                        device_a.free(&self.ctx);
                        device_a_owned = true;
                        device_b.free(&self.ctx);
                        device_b_owned = true;
                        self.allocator.free(shape_a);
                        shape_a_owned = true;
                        self.allocator.free(shape_b);
                        shape_b_owned = true;
                        const first = try linearNoBias(ctx, input, weight_a, rows, in_dim, out_dim);
                        errdefer freeTensor(ctx, first);
                        const second = try linearNoBias(ctx, input, weight_b, rows, in_dim, out_dim);
                        return .{ .first = first, .second = second };
                    },
                    else => return err,
                };
                route = .{
                    .provider = if (generated_pair_candidate) .fallback else .handwritten,
                    .kernel_id = "termite_linear_q4_0_pair_nobias_f32_tile4",
                    .grid = .{ rows * ((out_dim + 3) / 4) * 2, 1, 1 },
                    .block = .{ 256, 1, 1 },
                };
            }
        }
        noteQ4Route(
            self,
            .ffn_pair,
            rows,
            in_dim,
            out_dim,
            .pair,
            .none,
            route.provider,
            route.kernel_id,
            route.grid,
            route.block,
        );
        self.stats.linear_pair_fused_q4_0 += 1;
        if (used_q4_0_tile8 or (used_q4_0_q8_1_dp4a and cudaQ4_0PairQ8_1Tile8Enabled())) {
            self.stats.linear_pair_fused_q4_0_tile8 += 1;
        } else {
            self.stats.linear_pair_fused_q4_0_tile4 += 1;
        }
    } else {
        self.kernels.launchLinearQ4KPairNoBiasTile4F32(
            &self.ctx,
            device_a,
            device_b,
            input_tensor.buffer,
            weight_a_tensor.buffer,
            weight_b_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => {
                self.stats.linear_pair_fallbacks += 1;
                device_a.free(&self.ctx);
                device_a_owned = true;
                device_b.free(&self.ctx);
                device_b_owned = true;
                self.allocator.free(shape_a);
                shape_a_owned = true;
                self.allocator.free(shape_b);
                shape_b_owned = true;
                const first = try linearNoBias(ctx, input, weight_a, rows, in_dim, out_dim);
                errdefer freeTensor(ctx, first);
                const second = try linearNoBias(ctx, input, weight_b, rows, in_dim, out_dim);
                return .{ .first = first, .second = second };
            },
            else => return err,
        };
        self.stats.linear_pair_fused_q4 += 1;
    }

    self.stats.launch_linear += 1;
    const first = try createTensor(self, device_a, shape_a, out_count);
    shape_a_owned = true;
    device_a_owned = true;
    errdefer freeTensor(ctx, first);
    const second = try createTensor(self, device_b, shape_b, out_count);
    shape_b_owned = true;
    device_b_owned = true;
    return .{ .first = first, .second = second };
}

fn linearNoBiasPairActivationQ4_0(
    ctx: *anyopaque,
    input: CT,
    weight_gate: CT,
    weight_up: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    activation: ops.DecoderRuntimeActivationKind,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_gate_tensor = tensorFromCt(weight_gate);
    const weight_up_tensor = tensorFromCt(weight_up);
    try ensureF32(input_tensor);
    try ensureF32Bf16OrQuantized(weight_gate_tensor);
    try ensureF32Bf16OrQuantized(weight_up_tensor);
    if (rows != 1) return null;
    if (!isKnownQuant(weight_gate_tensor, .Q4_0) or !isKnownQuant(weight_up_tensor, .Q4_0)) return null;
    const input_expected = try checkedMul(rows, in_dim);
    if (input_tensor.elem_count != input_expected) return error.InvalidShape;
    const weight_expected = try checkedMul(out_dim, in_dim);
    if (weight_gate_tensor.elem_count != weight_expected or weight_up_tensor.elem_count != weight_expected) return error.InvalidShape;
    if (in_dim % 32 != 0) return error.InvalidShape;

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    var shape_owned = false;
    errdefer if (!shape_owned) self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_owned = false;
    errdefer if (!device_owned) device.free(&self.ctx);

    var used_q8_1_dp4a = false;
    var used_q8_1_tile8 = false;
    var route = CudaQ4KernelLaunch{
        .kernel_id = "termite_linear_q4_0_pair_activation_f32_tile4_w4",
        .grid = .{ rows * ((out_dim + 3) / 4), 1, 1 },
        .block = .{ 128, 1, 1 },
    };
    if (cudaQ4_0PairActivationQ8_1Dp4aEnabled(self)) q8_1_blk: {
        const row_blocks = in_dim / 32;
        const q8_blocks = try checkedMul(rows, row_blocks);
        const q8_bytes = try checkedMul(q8_blocks, 36);
        var q8_input = allocDeviceBuffer(self, q8_bytes) catch |err| switch (err) {
            error.CudaGraphCaptureUnsafeTempAlloc => break :q8_1_blk,
            else => return err,
        };
        defer releaseDeviceBuffer(self, &q8_input);

        self.kernels.launchQuantizeF32Q8_1Rows(&self.ctx, q8_input, input_tensor.buffer, rows, in_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => break :q8_1_blk,
            else => return err,
        };
        if (cudaQ4_0PairActivationQ8_1Tile8Enabled()) {
            self.kernels.launchLinearQ4_0PairActivationQ8_1Tile8F32(
                &self.ctx,
                device,
                q8_input,
                weight_gate_tensor.buffer,
                weight_up_tensor.buffer,
                rows,
                in_dim,
                out_dim,
                @intFromEnum(activation),
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => break :q8_1_blk,
                else => return err,
            };
            used_q8_1_tile8 = true;
            route = .{
                .kernel_id = "termite_linear_q4_0_pair_activation_q8_1_f32_tile8",
                .grid = .{ rows * ((out_dim + 7) / 8), 1, 1 },
                .block = .{ 128, 1, 1 },
            };
        } else if (cudaQ4_0PairActivationQ8_1Tile4W8Enabled(in_dim)) {
            self.kernels.launchLinearQ4_0PairActivationQ8_1Tile4W8F32(
                &self.ctx,
                device,
                q8_input,
                weight_gate_tensor.buffer,
                weight_up_tensor.buffer,
                rows,
                in_dim,
                out_dim,
                @intFromEnum(activation),
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => break :q8_1_blk,
                else => return err,
            };
            route = .{
                .kernel_id = "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8",
                .grid = .{ rows * ((out_dim + 3) / 4), 1, 1 },
                .block = .{ 256, 1, 1 },
            };
        } else {
            self.kernels.launchLinearQ4_0PairActivationQ8_1Tile4F32(
                &self.ctx,
                device,
                q8_input,
                weight_gate_tensor.buffer,
                weight_up_tensor.buffer,
                rows,
                in_dim,
                out_dim,
                @intFromEnum(activation),
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => break :q8_1_blk,
                else => return err,
            };
            route = .{
                .kernel_id = "termite_linear_q4_0_pair_activation_q8_1_f32_tile4",
                .grid = .{ rows * ((out_dim + 3) / 4), 1, 1 },
                .block = .{ 128, 1, 1 },
            };
        }
        used_q8_1_dp4a = true;
    }

    if (!used_q8_1_dp4a) self.kernels.launchLinearQ4_0PairActivationTile4W4F32(
        &self.ctx,
        device,
        input_tensor.buffer,
        weight_gate_tensor.buffer,
        weight_up_tensor.buffer,
        rows,
        in_dim,
        out_dim,
        @intFromEnum(activation),
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            device.free(&self.ctx);
            device_owned = true;
            self.allocator.free(shape);
            shape_owned = true;
            return null;
        },
        else => return err,
    };
    if (!used_q8_1_dp4a and cudaQ4_0PairActivationQ8_1Dp4aEnabled(self)) route.provider = .fallback;

    noteQ4Route(
        self,
        .ffn_pair,
        rows,
        in_dim,
        out_dim,
        .pair,
        .activation_f32,
        route.provider,
        route.kernel_id,
        route.grid,
        route.block,
    );

    self.stats.launch_linear += 1;
    self.stats.linear_pair_fused_q4_0 += 1;
    self.stats.linear_pair_fused_q4_0_activation += 1;
    if (used_q8_1_tile8) {
        self.stats.linear_pair_fused_q4_0_tile8 += 1;
    } else {
        self.stats.linear_pair_fused_q4_0_tile4 += 1;
    }
    return createTensor(self, device, shape, out_count);
}

const SpanQ4PairMode = enum {
    shared,
    shared_relu,
    separate,
};

fn spanQ4PairOp(mode: SpanQ4PairMode) CudaDispatchOp {
    return switch (mode) {
        .shared => .linear_pair,
        .shared_relu => .linear_pair_relu,
        .separate => .linear_pair_inputs,
    };
}

fn spanQ4PairEpilogue(mode: SpanQ4PairMode) CudaDispatchEpilogue {
    return switch (mode) {
        .shared => .pair,
        .shared_relu => .pair_relu,
        .separate => .pair_inputs,
    };
}

fn launchSpanQ4PairTc(
    self: *CudaCompute,
    mode: SpanQ4PairMode,
    variant: kernels_mod.QMatmulVariant,
    first_device: buffer_mod.DeviceBuffer,
    second_device: buffer_mod.DeviceBuffer,
    input_a_tensor: *const CudaTensor,
    input_b_tensor: ?*const CudaTensor,
    weight_a_tensor: *const CudaTensor,
    bias_a_tensor: *const CudaTensor,
    weight_b_tensor: *const CudaTensor,
    bias_b_tensor: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (variant == .tc_hmma and
        (tcQuantBuffer(weight_a_tensor, .q4_k_hmma) == null or tcQuantBuffer(weight_b_tensor, .q4_k_hmma) == null))
    {
        return false;
    }

    if (mode == .shared) {
        return try launchQ4KTcHmmaPairBias(
            self,
            variant,
            first_device,
            second_device,
            input_a_tensor.buffer,
            weight_a_tensor,
            bias_a_tensor.buffer,
            weight_b_tensor,
            bias_b_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        );
    }

    const first_tc = switch (mode) {
        .shared, .separate => try launchQ4KTcHmmaBias(self, variant, first_device, input_a_tensor.buffer, weight_a_tensor, bias_a_tensor.buffer, rows, in_dim, out_dim),
        .shared_relu => try launchQ4KTcHmmaBiasRelu(self, variant, first_device, input_a_tensor.buffer, weight_a_tensor, bias_a_tensor.buffer, rows, in_dim, out_dim),
    };
    if (!first_tc) return false;

    const second_input = switch (mode) {
        .shared, .shared_relu => input_a_tensor,
        .separate => input_b_tensor orelse return error.InvalidShape,
    };
    return switch (mode) {
        .shared, .separate => try launchQ4KTcHmmaBias(self, variant, second_device, second_input.buffer, weight_b_tensor, bias_b_tensor.buffer, rows, in_dim, out_dim),
        .shared_relu => try launchQ4KTcHmmaBiasRelu(self, variant, second_device, second_input.buffer, weight_b_tensor, bias_b_tensor.buffer, rows, in_dim, out_dim),
    };
}

fn linearPairSpanQ4(
    self: *CudaCompute,
    input_a_tensor: *const CudaTensor,
    input_b_tensor: ?*const CudaTensor,
    weight_a_tensor: *const CudaTensor,
    bias_a_tensor: *const CudaTensor,
    weight_b_tensor: *const CudaTensor,
    bias_b_tensor: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    mode: SpanQ4PairMode,
) anyerror!?ops.LinearPairResult {
    if (!isKnownQuant(weight_a_tensor, .Q4_K) or !isKnownQuant(weight_b_tensor, .Q4_K)) return null;

    const span_available = useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim);
    const tc_variant = mxbaiQ4TiledVariant(rows, in_dim, out_dim);
    if (tc_variant == null and !span_available) return null;

    const out_count = try checkedMul(rows, out_dim);
    const first_tensor = try self.allocator.create(CudaTensor);
    errdefer self.allocator.destroy(first_tensor);
    const second_tensor = try self.allocator.create(CudaTensor);
    errdefer self.allocator.destroy(second_tensor);
    const first_shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(first_shape);
    const second_shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(second_shape);
    var first_device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer first_device.free(&self.ctx);
    var second_device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer second_device.free(&self.ctx);

    const pair_op = spanQ4PairOp(mode);
    const pair_epilogue = spanQ4PairEpilogue(mode);
    var used_tc = false;
    var span_fallback: CudaDispatchFallback = if (tc_variant == null) tcUnavailableReason() else .specialized_span;
    if (tc_variant) |variant| {
        if (try launchSpanQ4PairTc(self, mode, variant, first_device, second_device, input_a_tensor, input_b_tensor, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim)) {
            used_tc = true;
            const tc_launch_count: usize = if (mode == .shared) 1 else 2;
            for (0..tc_launch_count) |_| self.dispatch_stats.note(self.allocator, pair_op, .q4_k, .q4_tc_hmma, pair_epilogue, .none, rows, in_dim, out_dim, 0);
        } else if (span_available) {
            span_fallback = tcPairFallbackReason(weight_a_tensor, weight_b_tensor, variant, .q4_k_hmma);
        } else {
            return null;
        }
    }

    if (!used_tc) {
        if (!span_available) return null;
        switch (mode) {
            .shared => try self.kernels.launchLinearQ4KSpanPairBiasTile8Rows2F32(
                &self.ctx,
                first_device,
                second_device,
                input_a_tensor.buffer,
                weight_a_tensor.buffer,
                bias_a_tensor.buffer,
                weight_b_tensor.buffer,
                bias_b_tensor.buffer,
                rows,
                in_dim,
                out_dim,
            ),
            .shared_relu => try self.kernels.launchLinearQ4KSpanPairBiasReluTile8Rows2F32(
                &self.ctx,
                first_device,
                second_device,
                input_a_tensor.buffer,
                weight_a_tensor.buffer,
                bias_a_tensor.buffer,
                weight_b_tensor.buffer,
                bias_b_tensor.buffer,
                rows,
                in_dim,
                out_dim,
            ),
            .separate => {
                const second_input = input_b_tensor orelse return error.InvalidShape;
                try self.kernels.launchLinearQ4KSpanPair2BiasTile8Rows2F32(
                    &self.ctx,
                    first_device,
                    second_device,
                    input_a_tensor.buffer,
                    second_input.buffer,
                    weight_a_tensor.buffer,
                    bias_a_tensor.buffer,
                    weight_b_tensor.buffer,
                    bias_b_tensor.buffer,
                    rows,
                    in_dim,
                    out_dim,
                );
            },
        }
        self.dispatch_stats.note(self.allocator, pair_op, .q4_k, .q4_span_simt, pair_epilogue, span_fallback, rows, in_dim, out_dim, 0);
    }

    first_tensor.* = .{
        .buffer = first_device,
        .dtype = .f32,
        .shape = first_shape,
        .elem_count = out_count,
    };
    second_tensor.* = .{
        .buffer = second_device,
        .dtype = .f32,
        .shape = second_shape,
        .elem_count = out_count,
    };
    return .{
        .first = first_tensor,
        .second = second_tensor,
    };
}

fn linearPair(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    if (isF16Weight(weight_a_tensor) and isF16Weight(weight_b_tensor)) {
        const pair = try linearNoBiasPair(ctx, input, weight_a, weight_b, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, pair.first);
        errdefer freeTensor(ctx, pair.second);
        try self.kernels.launchAddBiasRowsF32(&self.ctx, tensorFromCt(pair.first).buffer, bias_a_tensor.buffer, rows, out_dim);
        try self.kernels.launchAddBiasRowsF32(&self.ctx, tensorFromCt(pair.second).buffer, bias_b_tensor.buffer, rows, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_pair, .f16, .dense_lt, .bias, .none, rows, in_dim, out_dim, 0);
        return .{ .first = pair.first, .second = pair.second };
    }

    if (try linearPairSpanQ4(self, input_tensor, null, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim, .shared)) |span_pair| {
        return span_pair;
    }

    // The fused Q4_K pair kernel is block-per-output. Dispatch the two linears
    // separately so each uses the row/column-tiled Q4_K path.
    const first = try linear(ctx, input, weight_a, bias_a, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, first);
    const second = try linear(ctx, input, weight_b, bias_b, rows, in_dim, out_dim);
    return .{ .first = first, .second = second };
}

fn linearPairRelu(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    if (isF16Weight(weight_a_tensor) and isF16Weight(weight_b_tensor)) {
        // Older CUDA artifacts predate the fused bias+ReLU epilogue kernel;
        // decline so the generic path composes it from unfused ops.
        if (self.kernels.add_bias_relu_rows_f32 == null) return null;
        const projected = try linearNoBiasPair(ctx, input, weight_a, weight_b, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, projected.first);
        errdefer freeTensor(ctx, projected.second);
        try self.kernels.launchAddBiasReluRowsF32(&self.ctx, tensorFromCt(projected.first).buffer, bias_a_tensor.buffer, rows, out_dim);
        try self.kernels.launchAddBiasReluRowsF32(&self.ctx, tensorFromCt(projected.second).buffer, bias_b_tensor.buffer, rows, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_pair_relu, .f16, .dense_lt, .bias_relu, .none, rows, in_dim, out_dim, 0);
        return .{ .first = projected.first, .second = projected.second };
    }

    return try linearPairSpanQ4(self, input_tensor, null, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim, .shared_relu);
}

fn linearPairInputs(ctx: *anyopaque, input_a: CT, input_b: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_a_tensor = tensorFromCt(input_a);
    const input_b_tensor = tensorFromCt(input_b);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_a_tensor);
    try ensureF32(input_b_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_a_tensor, try checkedMul(rows, in_dim));
    try ensureCount(input_b_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    if (isF16Weight(weight_a_tensor) and isF16Weight(weight_b_tensor)) {
        const first = try linear(ctx, input_a, weight_a, bias_a, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, first);
        const second = try linear(ctx, input_b, weight_b, bias_b, rows, in_dim, out_dim);
        self.dispatch_stats.note(self.allocator, .linear_pair_inputs, .f16, .dense_lt, .bias, .none, rows, in_dim, out_dim, 0);
        return .{ .first = first, .second = second };
    }

    return try linearPairSpanQ4(self, input_a_tensor, input_b_tensor, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim, .separate);
}

fn layerNorm(ctx: *anyopaque, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const gamma_tensor = tensorFromCt(gamma);
    const beta_tensor = tensorFromCt(beta);
    try ensureF32(input_tensor);
    try ensureF32(gamma_tensor);
    try ensureF32(beta_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(gamma_tensor, dim);
    try ensureCount(beta_tensor, dim);
    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchLayerNormF32(&self.ctx, device, input_tensor.buffer, gamma_tensor.buffer, beta_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_layer += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn addLayerNorm(ctx: *anyopaque, a: CT, b: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    const gamma_tensor = tensorFromCt(gamma);
    const beta_tensor = tensorFromCt(beta);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureF32(gamma_tensor);
    try ensureF32(beta_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return null;
    if (dim == 0 or a_tensor.elem_count % dim != 0) return null;
    try ensureCount(gamma_tensor, dim);
    try ensureCount(beta_tensor, dim);

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAddLayerNormF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, gamma_tensor.buffer, beta_tensor.buffer, a_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_add_layer += 1;
    return try createTensor(self, device, shape, a_tensor.elem_count);
}

fn rmsNorm(ctx: *anyopaque, input: CT, weight: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=rms_norm input_elems={d} dim={d}", .{ input_tensor.elem_count, dim });
        return error.InvalidShape;
    }
    if (weight_tensor.elem_count != dim) {
        if (cudaLazyProfileEnabled()) std.log.err("cuda_invalid_shape: op=rms_norm weight_elems={d} dim={d}", .{ weight_tensor.elem_count, dim });
        return error.InvalidShape;
    }

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var norm_profile_scope = beginPrefillProfile(self, .norm, input_tensor.elem_count / dim);
    defer if (norm_profile_scope) |*scope| scope.end();
    if (input_tensor.elem_count / dim > 1 and cudaRmsNormBf16MirrorEnabled() and cudaCublasLtEnabled() and self.ctx.info.compute_major >= 8 and self.cublaslt != null) {
        var bf16_mirror = allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(u16)) catch buffer_mod.DeviceBuffer{};
        if (bf16_mirror.ptr != 0) {
            const launched_mirror = blk: {
                self.kernels.launchRmsNormF32Bf16(&self.ctx, device, bf16_mirror, input_tensor.buffer, weight_tensor.buffer, input_tensor.elem_count / dim, dim, eps) catch |err| {
                    bf16_mirror.free(&self.ctx);
                    switch (err) {
                        error.CudaKernelUnavailable => break :blk false,
                        else => return err,
                    }
                };
                break :blk true;
            };
            if (launched_mirror) {
                self.stats.launch_norm += 1;
                self.stats.launch_norm_rms += 1;
                self.stats.rms_norm_bf16_mirror_hits += 1;
                errdefer bf16_mirror.free(&self.ctx);
                return createTensorWithDTypeAndBf16Mirror(self, device, bf16_mirror, shape, input_tensor.elem_count, .f32);
            }
        }
    }
    try self.kernels.launchRmsNormF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormAddMultiplyScalarTensor(ctx: *anyopaque, input: CT, weight: CT, residual: CT, scalar: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const residual_tensor = tensorFromCt(residual);
    const scalar_tensor = tensorFromCt(scalar);
    if (!platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_RMSNORM_ADD_MUL_SCALAR_FUSION", false)) return null;
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(residual_tensor);
    try ensureF32(scalar_tensor);
    if (input_tensor.elem_count != residual_tensor.elem_count or !sameShape(input_tensor.shape, residual_tensor.shape)) return error.InvalidShape;
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(weight_tensor, dim);
    try ensureCount(scalar_tensor, 1);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchRmsNormAddMulScalarF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, residual_tensor.buffer, scalar_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms_add_mul_scalar += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormAddOutputScaleTensor(ctx: *anyopaque, input: CT, weight: CT, residual: CT, scalar: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_RMSNORM_ADD_OUTPUT_SCALE_FUSION", true)) {
        self.stats.rms_norm_add_output_scale_fallbacks += 1;
        return null;
    }
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const residual_tensor = tensorFromCt(residual);
    const scalar_tensor = tensorFromCt(scalar);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(residual_tensor);
    try ensureF32(scalar_tensor);
    if (input_tensor.elem_count != residual_tensor.elem_count or !sameShape(input_tensor.shape, residual_tensor.shape)) return error.InvalidShape;
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(weight_tensor, dim);
    try ensureCount(scalar_tensor, 1);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var norm_profile_scope = beginPrefillProfile(self, .norm, input_tensor.elem_count / dim);
    defer if (norm_profile_scope) |*scope| scope.end();
    self.kernels.launchRmsNormAddOutputScaleF32(
        &self.ctx,
        device,
        input_tensor.buffer,
        weight_tensor.buffer,
        residual_tensor.buffer,
        scalar_tensor.buffer,
        input_tensor.elem_count / dim,
        dim,
        eps,
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            device.free(&self.ctx);
            self.allocator.free(shape);
            self.stats.rms_norm_add_output_scale_fallbacks += 1;
            return null;
        },
        else => return err,
    };
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms_add_output_scale += 1;
    self.stats.rms_norm_add_output_scale_fused += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormAddTensor(ctx: *anyopaque, input: CT, weight: CT, residual: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const residual_tensor = tensorFromCt(residual);
    if (!platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_RMSNORM_ADD_FUSION", true)) return null;
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(residual_tensor);
    if (input_tensor.elem_count != residual_tensor.elem_count or !sameShape(input_tensor.shape, residual_tensor.shape)) return error.InvalidShape;
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(weight_tensor, dim);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var norm_profile_scope = beginPrefillProfile(self, .norm, input_tensor.elem_count / dim);
    defer if (norm_profile_scope) |*scope| scope.end();
    if (input_tensor.elem_count / dim > 1 and cudaRmsNormBf16MirrorEnabled() and cudaCublasLtEnabled() and self.ctx.info.compute_major >= 8 and self.cublaslt != null) {
        var bf16_mirror = allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(u16)) catch buffer_mod.DeviceBuffer{};
        if (bf16_mirror.ptr != 0) {
            const launched_mirror = blk: {
                self.kernels.launchRmsNormAddF32Bf16(&self.ctx, device, bf16_mirror, input_tensor.buffer, weight_tensor.buffer, residual_tensor.buffer, input_tensor.elem_count / dim, dim, eps) catch |err| {
                    bf16_mirror.free(&self.ctx);
                    switch (err) {
                        error.CudaKernelUnavailable => break :blk false,
                        else => return err,
                    }
                };
                break :blk true;
            };
            if (launched_mirror) {
                self.stats.launch_norm += 1;
                self.stats.launch_norm_rms_add += 1;
                self.stats.rms_norm_add_fused += 1;
                self.stats.rms_norm_bf16_mirror_hits += 1;
                errdefer bf16_mirror.free(&self.ctx);
                return createTensorWithDTypeAndBf16Mirror(self, device, bf16_mirror, shape, input_tensor.elem_count, .f32);
            }
        }
    }
    try self.kernels.launchRmsNormAddF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, residual_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms_add += 1;
    self.stats.rms_norm_add_fused += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormBare(ctx: *anyopaque, input: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var norm_profile_scope = beginPrefillProfile(self, .norm, input_tensor.elem_count / dim);
    defer if (norm_profile_scope) |*scope| scope.end();
    try self.kernels.launchRmsNormBareF32(&self.ctx, device, input_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    self.stats.rms_norm_bare_calls += 1;
    self.stats.launch_norm += 1;
    self.stats.launch_norm_rms_bare += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}
const UnaryOp = enum { gelu, relu, quick_gelu, sigmoid, tanh };

fn unaryHost(ctx: *anyopaque, input: CT, op: UnaryOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const kernel_op: kernels_mod.ElementwiseOp = switch (op) {
        .gelu => .gelu,
        .relu => .relu,
        .quick_gelu => .quick_gelu,
        .sigmoid => .sigmoid,
        .tanh => .tanh,
    };
    try self.kernels.launchElementwiseF32(&self.ctx, device, input_tensor.buffer, .{}, input_tensor.elem_count, kernel_op);
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn gelu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .gelu);
}

fn geluExact(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const result = try primitiveUnaryElementwise(ctx, input, .gelu_exact);
    self.stats.exact_gelu_forward_calls += 1;
    return result;
}

fn decoderRuntimeApplyGeluBackwardOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyGeluBackwardRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input = tensorFromCt(request.input);
    const upstream = tensorFromCt(request.upstream_grad);
    try ensureF32(input);
    try ensureF32(upstream);
    if (request.dim != input.elem_count or upstream.elem_count != input.elem_count) return error.InvalidShape;
    const result = try binaryElementwise(
        ctx,
        request.input,
        request.upstream_grad,
        if (request.exact) .gelu_exact_backward else .gelu_backward,
    );
    if (request.exact) self.stats.exact_gelu_backward_calls += 1;
    return result;
}

fn relu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .relu);
}

fn quickGelu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .quick_gelu);
}

fn sigmoid(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .sigmoid);
}

fn tanhAct(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .tanh);
}
fn concat(ctx: *anyopaque, a: CT, b: CT, total: usize, dim_a: usize, dim_b: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureCount(a_tensor, try checkedMul(total, dim_a));
    try ensureCount(b_tensor, try checkedMul(total, dim_b));
    const out_dim = try checkedAdd(dim_a, dim_b);
    const out_count = try checkedMul(total, out_dim);
    const shape = try allocShape2(self.allocator, total, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchConcatLastDimF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, total, dim_a, dim_b);
    self.stats.launch_other += 1;
    return createTensor(self, device, shape, out_count);
}

fn splitLastDim3(ctx: *anyopaque, input: CT, rows: usize, dim: usize) anyerror!ops.SplitLastDim3Result {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const part_count = try checkedMul(rows, dim);
    try ensureCount(input_tensor, try checkedMul(part_count, 3));
    const shape_first = try allocShape2(self.allocator, rows, dim);
    var shape_first_owned = false;
    errdefer if (!shape_first_owned) self.allocator.free(shape_first);
    const shape_second = try allocShape2(self.allocator, rows, dim);
    var shape_second_owned = false;
    errdefer if (!shape_second_owned) self.allocator.free(shape_second);
    const shape_third = try allocShape2(self.allocator, rows, dim);
    var shape_third_owned = false;
    errdefer if (!shape_third_owned) self.allocator.free(shape_third);
    var first_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var first_device_owned = false;
    errdefer if (!first_device_owned) first_device.free(&self.ctx);
    var second_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var second_device_owned = false;
    errdefer if (!second_device_owned) second_device.free(&self.ctx);
    var third_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var third_device_owned = false;
    errdefer if (!third_device_owned) third_device.free(&self.ctx);
    try self.kernels.launchSplitLastDim3F32(&self.ctx, first_device, second_device, third_device, input_tensor.buffer, rows, dim);
    self.stats.launch_other += 1;
    const first = try createTensor(self, first_device, shape_first, part_count);
    first_device_owned = true;
    shape_first_owned = true;
    errdefer freeTensor(ctx, first);
    const second = try createTensor(self, second_device, shape_second, part_count);
    second_device_owned = true;
    shape_second_owned = true;
    errdefer freeTensor(ctx, second);
    const third = try createTensor(self, third_device, shape_third, part_count);
    third_device_owned = true;
    shape_third_owned = true;
    return .{ .first = first, .second = second, .third = third };
}

fn binaryElementwise(ctx: *anyopaque, a: CT, b: CT, op: kernels_mod.ElementwiseOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    if (!sameShape(a_tensor.shape, b_tensor.shape) and (a_tensor.elem_count == 1 or b_tensor.elem_count == 1)) {
        const scalar_on_left = a_tensor.elem_count == 1 and (b_tensor.elem_count != 1 or b_tensor.shape.len > a_tensor.shape.len);
        const input_tensor = if (scalar_on_left) b_tensor else a_tensor;
        const scalar_tensor = if (scalar_on_left) a_tensor else b_tensor;
        const output_shape_source = if (a_tensor.elem_count > b_tensor.elem_count)
            a_tensor.shape
        else if (b_tensor.elem_count > a_tensor.elem_count)
            b_tensor.shape
        else if (a_tensor.shape.len >= b_tensor.shape.len)
            a_tensor.shape
        else
            b_tensor.shape;
        const shape = try dupeShape(self.allocator, output_shape_source);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
        errdefer device.free(&self.ctx);
        try self.kernels.launchBinaryScalarF32(&self.ctx, device, input_tensor.buffer, scalar_tensor.buffer, input_tensor.elem_count, op, scalar_on_left);
        self.stats.launch_scalar += 1;
        self.stats.launch_scalar_device_broadcast += 1;
        return createTensor(self, device, shape, input_tensor.elem_count);
    }
    if (!sameShape(a_tensor.shape, b_tensor.shape)) {
        const output_rank = @max(a_tensor.shape.len, b_tensor.shape.len);
        if (output_rank == 0 or output_rank > 8) return error.InvalidShape;
        var a_dims = [_]u32{1} ** 8;
        var b_dims = [_]u32{1} ** 8;
        var output_dims = [_]u32{1} ** 8;
        for (a_tensor.shape, 0..) |dim, idx| {
            if (dim <= 0 or dim > std.math.maxInt(u32)) return error.InvalidShape;
            a_dims[idx] = @intCast(dim);
        }
        for (b_tensor.shape, 0..) |dim, idx| {
            if (dim <= 0 or dim > std.math.maxInt(u32)) return error.InvalidShape;
            b_dims[idx] = @intCast(dim);
        }
        const shape = try self.allocator.alloc(i64, output_rank);
        errdefer self.allocator.free(shape);
        var output_count: usize = 1;
        for (0..output_rank) |out_idx| {
            const a_dim: u32 = if (out_idx + a_tensor.shape.len >= output_rank)
                a_dims[out_idx + a_tensor.shape.len - output_rank]
            else
                1;
            const b_dim: u32 = if (out_idx + b_tensor.shape.len >= output_rank)
                b_dims[out_idx + b_tensor.shape.len - output_rank]
            else
                1;
            if (a_dim != b_dim and a_dim != 1 and b_dim != 1) return error.InvalidShape;
            const out_dim = @max(a_dim, b_dim);
            output_dims[out_idx] = out_dim;
            shape[out_idx] = out_dim;
            output_count = try checkedMul(output_count, out_dim);
        }
        var device = try allocDeviceBuffer(self, output_count * @sizeOf(f32));
        errdefer device.free(&self.ctx);
        try self.kernels.launchElementwiseBroadcastF32(
            &self.ctx,
            device,
            a_tensor.buffer,
            b_tensor.buffer,
            output_count,
            a_tensor.elem_count,
            b_tensor.elem_count,
            a_dims,
            b_dims,
            output_dims,
            a_tensor.shape.len,
            b_tensor.shape.len,
            output_rank,
            op,
        );
        self.stats.launch_elementwise += 1;
        return createTensor(self, device, shape, output_count);
    }
    if (a_tensor.elem_count != b_tensor.elem_count) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    // Same-shape elementwise binary op (residual add / multiply). Leading dim of
    // the 2-D residual-stream tensor is the token count (>1 at prefill).
    const bin_rows: usize = if (a_tensor.shape.len == 2 and a_tensor.shape[0] > 1) @intCast(a_tensor.shape[0]) else 1;
    var elementwise_profile_scope = beginPrefillProfile(self, .elementwise, bin_rows);
    defer if (elementwise_profile_scope) |*scope| scope.end();
    try self.kernels.launchElementwiseF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, a_tensor.elem_count, op);
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, a_tensor.elem_count);
}

fn add(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .add);
}

fn addBiasRowsConsume(ctx: *anyopaque, input: CT, bias: CT, rows: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    if (input_tensor.elem_count != try checkedMul(rows, out_dim)) return error.InvalidShape;
    if (bias_tensor.elem_count != out_dim) return error.InvalidShape;
    try self.kernels.launchAddBiasRowsF32(&self.ctx, input_tensor.buffer, bias_tensor.buffer, rows, out_dim);
    self.stats.launch_elementwise += 1;
    return input;
}

fn multiply(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .multiply);
}

fn primitiveUnaryElementwise(ctx: *anyopaque, input_ct: CT, op: kernels_mod.ElementwiseOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input = tensorFromCt(input_ct);
    try ensureF32(input);
    const shape = try dupeShape(self.allocator, input.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchElementwiseF32(&self.ctx, device, input.buffer, .{}, input.elem_count, op);
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, input.elem_count);
}

fn primSubtractOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .subtract);
}
fn primDivideOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .divide);
}
fn primNegateOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .negate);
}
fn primSqrtOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .sqrt);
}
fn primRsqrtOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .rsqrt);
}
fn primExpOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .exp);
}
fn primLogOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .log);
}
fn primSinOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .sin);
}
fn primCosOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .cos);
}
fn primTanhOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .tanh);
}
fn primErfOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .erf);
}
fn primAbsOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return primitiveUnaryElementwise(ctx, a, .abs);
}
fn primLessThanOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .less_than);
}

fn broadcastRightAligned(ctx: *anyopaque, input_ct: CT, target_shape: []const i64) anyerror!CT {
    const input = tensorFromCt(input_ct);
    if (input.shape.len > target_shape.len or input.shape.len > 8) return error.InvalidShape;

    var broadcast_axes = [_]u8{0} ** 8;
    const leading_dims = target_shape.len - input.shape.len;
    for (0..input.shape.len) |idx| {
        broadcast_axes[idx] = @intCast(leading_dims + idx);
    }
    return primBroadcastInDimOp(
        ctx,
        input_ct,
        target_shape,
        broadcast_axes[0..input.shape.len],
        input.shape,
    );
}

fn primWhereSelectOp(ctx: *anyopaque, cond_ct: CT, true_ct: CT, false_ct: CT) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cond = tensorFromCt(cond_ct);
    const on_true = tensorFromCt(true_ct);
    const on_false = tensorFromCt(false_ct);
    try ensureF32(cond);
    try ensureF32(on_true);
    try ensureF32(on_false);

    const output_rank = @max(cond.shape.len, @max(on_true.shape.len, on_false.shape.len));
    if (output_rank == 0 or output_rank > 8) return error.InvalidShape;
    var target_shape_storage = [_]i64{1} ** 8;
    const input_shapes = [_][]const i64{ cond.shape, on_true.shape, on_false.shape };
    for (0..output_rank) |output_axis| {
        var output_dim: i64 = 1;
        for (input_shapes) |input_shape| {
            const leading_dims = output_rank - input_shape.len;
            const input_dim: i64 = if (output_axis < leading_dims) 1 else input_shape[output_axis - leading_dims];
            if (input_dim <= 0) return error.InvalidShape;
            if (input_dim != 1 and output_dim != 1 and input_dim != output_dim) return error.InvalidShape;
            if (input_dim != 1) output_dim = input_dim;
        }
        target_shape_storage[output_axis] = output_dim;
    }
    const target_shape = target_shape_storage[0..output_rank];
    const output_count = try elementCountFromShape(target_shape);

    var broadcast_cond: ?CT = null;
    defer if (broadcast_cond) |tensor| freeTensor(ctx, tensor);
    var broadcast_true: ?CT = null;
    defer if (broadcast_true) |tensor| freeTensor(ctx, tensor);
    var broadcast_false: ?CT = null;
    defer if (broadcast_false) |tensor| freeTensor(ctx, tensor);

    const launch_cond = if (sameShape(cond.shape, target_shape)) cond_ct else blk: {
        broadcast_cond = try broadcastRightAligned(ctx, cond_ct, target_shape);
        break :blk broadcast_cond.?;
    };
    const launch_true = if (sameShape(on_true.shape, target_shape)) true_ct else blk: {
        broadcast_true = try broadcastRightAligned(ctx, true_ct, target_shape);
        break :blk broadcast_true.?;
    };
    const launch_false = if (sameShape(on_false.shape, target_shape)) false_ct else blk: {
        broadcast_false = try broadcastRightAligned(ctx, false_ct, target_shape);
        break :blk broadcast_false.?;
    };

    const shape = try dupeShape(self.allocator, target_shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, output_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchPrimitiveWhereF32(
        &self.ctx,
        device,
        tensorFromCt(launch_cond).buffer,
        tensorFromCt(launch_true).buffer,
        tensorFromCt(launch_false).buffer,
        output_count,
    );
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, output_count);
}

fn multiplyScalar(ctx: *anyopaque, input: CT, scale: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchScaleF32(&self.ctx, device, input_tensor.buffer, input_tensor.elem_count, scale);
    self.stats.launch_scalar += 1;
    self.stats.launch_scalar_multiply_immediate += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn addScalar(ctx: *anyopaque, input: CT, value: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAddScalarF32(&self.ctx, device, input_tensor.buffer, input_tensor.elem_count, value);
    self.stats.add_scalar_calls += 1;
    self.stats.launch_scalar += 1;
    self.stats.launch_scalar_add_immediate += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn siluMultiply(ctx: *anyopaque, gate: CT, up: CT) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const gate_tensor = tensorFromCt(gate);
    const up_tensor = tensorFromCt(up);
    try ensureF32(gate_tensor);
    try ensureF32(up_tensor);
    if (gate_tensor.elem_count != up_tensor.elem_count or !sameShape(gate_tensor.shape, up_tensor.shape)) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, gate_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, gate_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    // SwiGLU activation-multiply. Leading dim of the 2-D activation is the token
    // count (>1 at prefill), which gates this to prefill-only attribution.
    const silu_rows: usize = if (gate_tensor.shape.len == 2 and gate_tensor.shape[0] > 1) @intCast(gate_tensor.shape[0]) else 1;
    var elementwise_profile_scope = beginPrefillProfile(self, .elementwise, silu_rows);
    defer if (elementwise_profile_scope) |*scope| scope.end();
    try self.kernels.launchSiluMultiplyF32(&self.ctx, device, gate_tensor.buffer, up_tensor.buffer, gate_tensor.elem_count);
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, gate_tensor.elem_count);
}

fn activationMultiply(ctx: *anyopaque, gate: CT, up: CT, activation: ops.DecoderRuntimeActivationKind) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const gate_tensor = tensorFromCt(gate);
    const up_tensor = tensorFromCt(up);
    try ensureF32(gate_tensor);
    try ensureF32(up_tensor);
    if (gate_tensor.elem_count != up_tensor.elem_count or !sameShape(gate_tensor.shape, up_tensor.shape)) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, gate_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, gate_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const act_rows: usize = if (gate_tensor.shape.len == 2 and gate_tensor.shape[0] > 1) @intCast(gate_tensor.shape[0]) else 1;
    var elementwise_profile_scope = beginPrefillProfile(self, .elementwise, act_rows);
    defer if (elementwise_profile_scope) |*scope| scope.end();
    try self.kernels.launchActivationMultiplyF32(&self.ctx, device, gate_tensor.buffer, up_tensor.buffer, gate_tensor.elem_count, @intFromEnum(activation));
    self.stats.launch_elementwise += 1;
    self.stats.activation_multiply_fused += 1;
    return createTensor(self, device, shape, gate_tensor.elem_count);
}

fn activationMultiplySliceLastDim(ctx: *anyopaque, gate: CT, source: CT, start: usize, stop: usize, activation: ops.DecoderRuntimeActivationKind) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const gate_tensor = tensorFromCt(gate);
    const source_tensor = tensorFromCt(source);
    try ensureF32(gate_tensor);
    try ensureF32(source_tensor);
    if (gate_tensor.shape.len != 2 or source_tensor.shape.len != 2) return null;
    if (gate_tensor.shape[0] < 0 or gate_tensor.shape[1] < 0 or source_tensor.shape[0] < 0 or source_tensor.shape[1] < 0) return error.InvalidShape;
    const rows: usize = @intCast(gate_tensor.shape[0]);
    const out_cols: usize = @intCast(gate_tensor.shape[1]);
    const source_rows: usize = @intCast(source_tensor.shape[0]);
    const source_cols: usize = @intCast(source_tensor.shape[1]);
    if (rows != source_rows or start > stop or stop > source_cols or stop - start != out_cols) return null;
    try ensureCount(gate_tensor, try checkedMul(rows, out_cols));
    try ensureCount(source_tensor, try checkedMul(source_rows, source_cols));

    const shape = try dupeShape(self.allocator, gate_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, gate_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var elementwise_profile_scope = beginPrefillProfile(self, .elementwise, rows);
    defer if (elementwise_profile_scope) |*scope| scope.end();
    self.kernels.launchActivationMultiplySliceLastDimF32(
        &self.ctx,
        device,
        gate_tensor.buffer,
        source_tensor.buffer,
        rows,
        source_cols,
        start,
        out_cols,
        @intFromEnum(activation),
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            device.free(&self.ctx);
            self.allocator.free(shape);
            return null;
        },
        else => return err,
    };
    self.stats.launch_elementwise += 1;
    self.stats.activation_multiply_fused += 1;
    return createTensor(self, device, shape, gate_tensor.elem_count);
}

fn linearNoBiasActivationSliceLastDim(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    source: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    start: usize,
    activation: ops.DecoderRuntimeActivationKind,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!cudaQ4_0PleGateFusionEnabled()) return null;
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const source_tensor = tensorFromCt(source);
    try ensureF32(input_tensor);
    try ensureF32Bf16OrQuantized(weight_tensor);
    try ensureF32(source_tensor);
    if (!isKnownQuant(weight_tensor, .Q4_0)) return null;
    if (in_dim == 0 or in_dim % 32 != 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    if (weight_tensor.elem_count != try checkedMul(out_dim, in_dim)) return error.InvalidShape;
    if (source_tensor.shape.len != 2) return null;
    if (source_tensor.shape[0] < 0 or source_tensor.shape[1] < 0) return error.InvalidShape;
    const source_rows: usize = @intCast(source_tensor.shape[0]);
    const source_cols: usize = @intCast(source_tensor.shape[1]);
    if (source_rows != rows or start > source_cols or out_dim > source_cols - start) return null;
    try ensureCount(source_tensor, try checkedMul(source_rows, source_cols));

    // With an admitted hybrid BF16 mirror, this E2B prefill candidate evaluates
    // two existing semantic operations in place of the fused Q8_1+Q4_0 PLE
    // kernel: returning null asks the architecture fallback to run
    // linearNoBias (BF16 cuBLASLt), sliceLastDim, and activationMultiply.
    // Decode is deliberately excluded before this decision, so rows == 1
    // continues through the byte-for-byte existing Q4_0 fused path below.
    if (self.ple_gate_prefill_profile != .off) {
        const model_is_e2b = isGemma4E2bPleRuntimeState(self.decoder_runtime_family_state);
        const bf16_mirror_admitted = weightBf16MirrorForRows(self, weight_tensor, rows) != null;
        if (self.ple_gate_prefill_profile.eligibleFor(
            self.ctx.info.compute_major,
            self.ctx.info.compute_minor,
            model_is_e2b,
            rows,
            in_dim,
            out_dim,
            bf16_mirror_admitted,
            self.cublaslt != null,
        )) {
            self.stats.ple_gate_prefill_bf16_mirror_first_hits += 1;
            return null;
        }
        if (model_is_e2b and rows == 1 and in_dim == 1536 and out_dim == 256) {
            self.stats.ple_gate_decode_q4_fused_preserved += 1;
        } else {
            self.stats.ple_gate_prefill_bf16_mirror_first_ineligible += 1;
        }
    }

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    var shape_owned = false;
    errdefer if (!shape_owned) self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_owned = false;
    errdefer if (!device_owned) device.free(&self.ctx);

    var used_q8_1_dp4a = false;
    if (cudaQ4_0ActivationSliceQ8_1Dp4aEnabled(self)) q8_1_blk: {
        const row_blocks = in_dim / 32;
        const q8_blocks = try checkedMul(rows, row_blocks);
        const q8_bytes = try checkedMul(q8_blocks, 36);
        var q8_input = allocDeviceBuffer(self, q8_bytes) catch |err| switch (err) {
            error.CudaGraphCaptureUnsafeTempAlloc => break :q8_1_blk,
            else => return err,
        };
        defer releaseDeviceBuffer(self, &q8_input);

        self.kernels.launchQuantizeF32Q8_1Rows(&self.ctx, q8_input, input_tensor.buffer, rows, in_dim) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => break :q8_1_blk,
            else => return err,
        };
        self.kernels.launchLinearQ4_0ActivationSliceLastDimQ8_1Tile4F32(
            &self.ctx,
            device,
            q8_input,
            weight_tensor.buffer,
            source_tensor.buffer,
            rows,
            in_dim,
            out_dim,
            source_cols,
            start,
            @intFromEnum(activation),
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => break :q8_1_blk,
            else => return err,
        };
        used_q8_1_dp4a = true;
    }

    if (!used_q8_1_dp4a) self.kernels.launchLinearQ4_0ActivationSliceLastDimTile4F32(
        &self.ctx,
        device,
        input_tensor.buffer,
        weight_tensor.buffer,
        source_tensor.buffer,
        rows,
        in_dim,
        out_dim,
        source_cols,
        start,
        @intFromEnum(activation),
    ) catch |tile4_err| switch (tile4_err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => self.kernels.launchLinearQ4_0ActivationSliceLastDimTile4W4F32(
            &self.ctx,
            device,
            input_tensor.buffer,
            weight_tensor.buffer,
            source_tensor.buffer,
            rows,
            in_dim,
            out_dim,
            source_cols,
            start,
            @intFromEnum(activation),
        ) catch |tile4_w4_err| switch (tile4_w4_err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                device.free(&self.ctx);
                device_owned = true;
                self.allocator.free(shape);
                shape_owned = true;
                return null;
            },
            else => return tile4_w4_err,
        },
        else => return tile4_err,
    };

    self.stats.launch_linear += 1;
    self.stats.linear_activation_slice_fused_q4_0 += 1;
    const result = try createTensor(self, device, shape, out_count);
    shape_owned = true;
    device_owned = true;
    return result;
}

fn linearNoBiasGatedDown(
    ctx: *anyopaque,
    gate: CT,
    up: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    activation: ops.DecoderRuntimeActivationKind,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const gate_tensor = tensorFromCt(gate);
    const up_tensor = tensorFromCt(up);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(gate_tensor);
    try ensureF32(up_tensor);
    try ensureF32Bf16OrQuantized(weight_tensor);
    const input_expected = try checkedMul(rows, in_dim);
    if (gate_tensor.elem_count != input_expected or up_tensor.elem_count != input_expected or
        !sameShape(gate_tensor.shape, up_tensor.shape))
    {
        return error.InvalidShape;
    }
    if (weight_tensor.elem_count != try checkedMul(out_dim, in_dim)) return error.InvalidShape;

    const use_q8 = isKnownQuant(weight_tensor, .Q8_0);
    const use_q4_0 = isKnownQuant(weight_tensor, .Q4_0);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    const use_q6 = isKnownQuant(weight_tensor, .Q6_K);
    if (!use_q8 and !use_q4_0 and !use_q4 and !use_q6) {
        self.stats.gated_down_fallbacks += 1;
        return null;
    }
    // Hybrid residency: prefill-shaped gated-down goes through the caller's
    // activation-multiply + BF16-mirror linear fallback instead of Q8_1 kernels.
    if (weightBf16MirrorForRows(self, weight_tensor, rows) != null) {
        self.stats.gated_down_fallbacks += 1;
        return null;
    }

    if (use_q4_0 and cudaQ4_0Q8_1RowsEligible(self, rows)) {
        if (try tryLinearNoBiasGatedDownQ8_1Dp4a(ctx, gate, up, weight, rows, in_dim, out_dim, activation)) |projected| {
            self.stats.gated_down_fused_q4_0 += 1;
            self.stats.gated_down_fused_q4_0_tile4 += 1;
            return projected;
        }
    }

    if (use_q4_0 and rows == 1 and cudaQ4_0GatedDownPrecomputeEnabled()) {
        if (try activationMultiply(ctx, gate, up, activation)) |activated| {
            defer freeTensor(ctx, activated);
            const projected = try linearNoBias(ctx, activated, weight, rows, in_dim, out_dim);
            self.stats.gated_down_fused_q4_0 += 1;
            self.stats.gated_down_fused_q4_0_precompute += 1;
            return projected;
        }
    }

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    var shape_owned = false;
    errdefer if (!shape_owned) self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_owned = false;
    errdefer if (!device_owned) device.free(&self.ctx);

    if (use_q8) {
        self.kernels.launchLinearQ8_0GatedDownTile4F32(
            &self.ctx,
            device,
            gate_tensor.buffer,
            up_tensor.buffer,
            weight_tensor.buffer,
            rows,
            in_dim,
            out_dim,
            @intFromEnum(activation),
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.gated_down_fallbacks += 1;
                device.free(&self.ctx);
                self.allocator.free(shape);
                return null;
            },
            else => return err,
        };
        self.stats.gated_down_fused_q8 += 1;
    } else if (use_q4_0) {
        const q4_0_tile_kind: enum { tile4, tile4_w4, tile8, tile16 } = blk: {
            if (rows == 1 and cudaQ4_0GatedDownTile16Enabled()) {
                self.kernels.launchLinearQ4_0GatedDownTile16F32(
                    &self.ctx,
                    device,
                    gate_tensor.buffer,
                    up_tensor.buffer,
                    weight_tensor.buffer,
                    rows,
                    in_dim,
                    out_dim,
                    @intFromEnum(activation),
                ) catch |err| switch (err) {
                    error.CudaKernelUnavailable, error.InvalidCudaState => break :blk .tile4,
                    else => return err,
                };
                break :blk .tile16;
            }
            if (rows == 1 and cudaQ4_0GatedDownTile8Enabled()) {
                self.kernels.launchLinearQ4_0GatedDownTile8F32(
                    &self.ctx,
                    device,
                    gate_tensor.buffer,
                    up_tensor.buffer,
                    weight_tensor.buffer,
                    rows,
                    in_dim,
                    out_dim,
                    @intFromEnum(activation),
                ) catch |err| switch (err) {
                    error.CudaKernelUnavailable, error.InvalidCudaState => break :blk .tile4,
                    else => return err,
                };
                break :blk .tile8;
            }
            if (rows == 1 and cudaQ4_0GatedDownTile4W4Enabled()) {
                self.kernels.launchLinearQ4_0GatedDownTile4W4F32(
                    &self.ctx,
                    device,
                    gate_tensor.buffer,
                    up_tensor.buffer,
                    weight_tensor.buffer,
                    rows,
                    in_dim,
                    out_dim,
                    @intFromEnum(activation),
                ) catch |err| switch (err) {
                    error.CudaKernelUnavailable, error.InvalidCudaState => break :blk .tile4,
                    else => return err,
                };
                break :blk .tile4_w4;
            }
            break :blk .tile4;
        };
        if (q4_0_tile_kind == .tile4) {
            self.kernels.launchLinearQ4_0GatedDownTile4F32(
                &self.ctx,
                device,
                gate_tensor.buffer,
                up_tensor.buffer,
                weight_tensor.buffer,
                rows,
                in_dim,
                out_dim,
                @intFromEnum(activation),
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => {
                    self.stats.gated_down_fallbacks += 1;
                    device.free(&self.ctx);
                    self.allocator.free(shape);
                    return null;
                },
                else => return err,
            };
        }
        self.stats.gated_down_fused_q4_0 += 1;
        switch (q4_0_tile_kind) {
            .tile4, .tile4_w4 => self.stats.gated_down_fused_q4_0_tile4 += 1,
            .tile8 => self.stats.gated_down_fused_q4_0_tile8 += 1,
            .tile16 => self.stats.gated_down_fused_q4_0_tile16 += 1,
        }
    } else if (use_q4) {
        self.kernels.launchLinearQ4KGatedDownTile4F32(
            &self.ctx,
            device,
            gate_tensor.buffer,
            up_tensor.buffer,
            weight_tensor.buffer,
            rows,
            in_dim,
            out_dim,
            @intFromEnum(activation),
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.gated_down_fallbacks += 1;
                device.free(&self.ctx);
                self.allocator.free(shape);
                return null;
            },
            else => return err,
        };
        self.stats.gated_down_fused_q4 += 1;
        if (rows == 1) self.stats.q4k_decode_fast_hits += 1;
    } else {
        self.kernels.launchLinearQ6KGatedDownTile4F32(
            &self.ctx,
            device,
            gate_tensor.buffer,
            up_tensor.buffer,
            weight_tensor.buffer,
            rows,
            in_dim,
            out_dim,
            @intFromEnum(activation),
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.gated_down_fallbacks += 1;
                device.free(&self.ctx);
                self.allocator.free(shape);
                return null;
            },
            else => return err,
        };
    }

    self.stats.launch_linear += 1;
    const result = try createTensor(self, device, shape, out_count);
    shape_owned = true;
    device_owned = true;
    return result;
}

fn addMultiplyScalarTensor(ctx: *anyopaque, a: CT, b: CT, scalar: CT) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    const scalar_tensor = tensorFromCt(scalar);
    if (!platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_ADD_MUL_SCALAR_FUSION", true)) return null;
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureF32(scalar_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return error.InvalidShape;
    try ensureCount(scalar_tensor, 1);

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAddMulScalarF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, scalar_tensor.buffer, a_tensor.elem_count);
    self.stats.launch_elementwise += 1;
    self.stats.add_mul_scalar_fused += 1;
    return createTensor(self, device, shape, a_tensor.elem_count);
}

fn addWeightedScalars(ctx: *anyopaque, a: CT, b: CT, scale_a: f32, scale_b: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    if (!platform.env.getenvBoolDefault("ANTFLY_CUDA_ENABLE_ADD_MUL_SCALAR_FUSION", true)) return null;
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAddWeightedScalarsF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, scale_a, scale_b, a_tensor.elem_count);
    self.stats.launch_elementwise += 1;
    self.stats.add_mul_scalar_fused += 1;
    return createTensor(self, device, shape, a_tensor.elem_count);
}

fn silu(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchElementwiseF32(&self.ctx, device, input_tensor.buffer, .{}, input_tensor.elem_count, .silu);
    self.stats.launch_elementwise += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}
fn sdpaLaunch(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, mask: ?[]const i64, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    const token_count = try checkedMul(batch, seq_len);
    const has_mask = mask != null;
    if (mask) |mask_values| {
        if (mask_values.len < token_count) return error.InvalidShape;
    }

    const mask_device = if (mask) |mask_values| try uploadCachedAttentionMaskI64(self, mask_values) else buffer_mod.DeviceBuffer{};
    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const bias_mode: u32 = if (bias_tensor) |bt| blk: {
        const shared = try checkedMul(num_heads, try checkedMul(seq_len, seq_len));
        const batched = try checkedMul(batch, shared);
        break :blk if (bt.elem_count == batched) 2 else if (bt.elem_count == shared) 1 else return error.InvalidShape;
    } else 0;

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var prefill_profile_scope = beginPrefillProfile(self, .attention, token_count);
    defer if (prefill_profile_scope) |*scope| scope.end();
    try self.kernels.launchAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, mask_device, bias_buffer, batch, seq_len, num_heads, head_dim, false, has_mask, bias_mode, true);
    self.stats.launch_attention += 1;
    return createTensor(self, device, shape, count);
}

fn sdpa(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, mask: []const i64, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    return sdpaLaunch(ctx, q_ct, k_ct, v_ct, mask, attn_bias_ct, batch, seq_len, num_heads, head_dim);
}

fn sdpaFull(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!?CT {
    return try sdpaLaunch(ctx, q_ct, k_ct, v_ct, null, attn_bias_ct, batch, seq_len, num_heads, head_dim);
}

fn causalSelfAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const bias_mode: u32 = if (bias_tensor) |bt| blk: {
        const shared = try checkedMul(num_heads, try checkedMul(seq_len, seq_len));
        const batched = try checkedMul(batch, shared);
        break :blk if (bt.elem_count == batched) 2 else if (bt.elem_count == shared) 1 else return error.InvalidShape;
    } else 0;

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var prefill_profile_scope = beginPrefillProfile(self, .attention, seq_len);
    defer if (prefill_profile_scope) |*scope| scope.end();
    try self.kernels.launchAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, .{}, bias_buffer, batch, seq_len, num_heads, head_dim, true, false, bias_mode, false);
    self.stats.launch_attention += 1;
    return createTensor(self, device, shape, count);
}

fn cudaAllowHostAttentionFallback() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_ALLOW_HOST_ATTENTION_FALLBACK", false);
}

fn cudaForceHostGqaAttention() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_FORCE_HOST_GQA_ATTENTION", false);
}

fn usizeSliceToU32(allocator: std.mem.Allocator, values: []const usize) ![]u32 {
    const out = try allocator.alloc(u32, values.len);
    errdefer allocator.free(out);
    for (values, 0..) |value, i| {
        if (value > std.math.maxInt(u32)) return error.InvalidShape;
        out[i] = @intCast(value);
    }
    return out;
}

fn ropeHostFallback(
    self: *CudaCompute,
    input_tensor: *const CudaTensor,
    seq_len: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    position_offset: usize,
    consecutive_pairs: bool,
) !CT {
    self.stats.host_attention_fallbacks += 1;
    self.stats.rope_host_fallbacks += 1;
    const data = try downloadAlloc(self, input_tensor);
    var data_owned = true;
    errdefer if (data_owned) self.allocator.free(data);
    const total_chunks = data.len / head_dim;
    const chunks_per_position = total_chunks / seq_len;
    const positions = try self.allocator.alloc(usize, total_chunks);
    defer self.allocator.free(positions);
    for (positions, 0..) |*position, chunk| {
        position.* = position_offset + ((chunk / chunks_per_position) % seq_len);
    }
    linalg.ropeCore(data, positions, head_dim, rope_dim, theta, freq_scale, consecutive_pairs);
    data_owned = false;
    return uploadOwnedHost(self, data, input_tensor.shape);
}

fn ropePerItemHostFallback(
    self: *CudaCompute,
    input_tensor: *const CudaTensor,
    batch: usize,
    max_seq_len: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    query_lengths: []const usize,
    position_offsets: []const usize,
    consecutive_pairs: bool,
) !CT {
    self.stats.host_attention_fallbacks += 1;
    self.stats.rope_per_item_host_fallbacks += 1;
    const data = try downloadAlloc(self, input_tensor);
    var data_owned = true;
    errdefer if (data_owned) self.allocator.free(data);
    const row_count = try checkedMul(batch, max_seq_len);
    const row_dim = data.len / row_count;
    const num_heads = row_dim / head_dim;
    const total_chunks = try checkedMul(row_count, num_heads);
    const positions = try self.allocator.alloc(usize, total_chunks);
    defer self.allocator.free(positions);
    @memset(positions, 0);
    for (0..batch) |b| {
        for (0..query_lengths[b]) |pos| {
            const row_base = (b * max_seq_len + pos) * num_heads;
            for (0..num_heads) |h| {
                positions[row_base + h] = position_offsets[b] + pos;
            }
        }
    }
    linalg.ropeCore(data, positions, head_dim, rope_dim, theta, freq_scale, consecutive_pairs);
    data_owned = false;
    return uploadOwnedHost(self, data, input_tensor.shape);
}

fn biasModeFor(bias_tensor: ?*CudaTensor, batch: usize, num_heads: usize, q_seq_len: usize, kv_seq_len: usize) !u32 {
    const tensor = bias_tensor orelse return 0;
    try ensureF32(tensor);
    const shared = try checkedMul(num_heads, try checkedMul(q_seq_len, kv_seq_len));
    const batched = try checkedMul(batch, shared);
    if (tensor.elem_count == batched) return 2;
    if (tensor.elem_count == shared) return 1;
    return error.InvalidShape;
}

fn gqaDenseAttentionHostFallback(
    self: *CudaCompute,
    q_tensor: *const CudaTensor,
    k_tensor: *const CudaTensor,
    v_tensor: *const CudaTensor,
    bias_tensor: ?*CudaTensor,
    attn_or_mask: ?[]const u8,
    sliding_window: usize,
    batch: usize,
    q_seq_len: usize,
    kv_seq_len: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) !CT {
    self.stats.host_attention_fallbacks += 1;
    self.stats.gqa_dense_host_fallbacks += 1;
    const q_host = try downloadAlloc(self, q_tensor);
    defer self.allocator.free(q_host);
    const k_host = try downloadAlloc(self, k_tensor);
    defer self.allocator.free(k_host);
    const v_host = try downloadAlloc(self, v_tensor);
    defer self.allocator.free(v_host);
    const bias_host = if (bias_tensor) |tensor| try downloadAlloc(self, tensor) else null;
    defer if (bias_host) |data| self.allocator.free(data);
    const output = try linalg.flashCausalAttentionHost(
        self.allocator,
        q_host,
        k_host,
        v_host,
        bias_host,
        attn_or_mask,
        sliding_window,
        batch,
        q_seq_len,
        kv_seq_len,
        query_position_offset,
        kv_position_offset,
        num_heads,
        num_kv_heads,
        head_dim,
    );
    var output_owned = true;
    errdefer if (output_owned) self.allocator.free(output);
    output_owned = false;
    return uploadOwnedHost(self, output, q_tensor.shape);
}

fn gqaDenseAttention(
    ctx: *anyopaque,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    attn_bias_ct: ?CT,
    attn_or_mask: ?[]const u8,
    batch: usize,
    q_seq_len: usize,
    kv_seq_len: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    total_sequence_len: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    if (num_kv_heads == 0 or num_heads % num_kv_heads != 0) return error.InvalidShape;
    const q_hidden = try checkedMul(num_heads, head_dim);
    const kv_hidden = try checkedMul(num_kv_heads, head_dim);
    const q_count = try checkedMul(try checkedMul(batch, q_seq_len), q_hidden);
    const kv_count = try checkedMul(try checkedMul(batch, kv_seq_len), kv_hidden);
    try ensureCount(q_tensor, q_count);
    try ensureCount(k_tensor, kv_count);
    try ensureCount(v_tensor, kv_count);
    if (total_sequence_len < q_seq_len or query_position_offset > total_sequence_len) return error.InvalidShape;
    const query_end = try checkedAdd(query_position_offset, q_seq_len);
    const kv_end = try checkedAdd(kv_position_offset, kv_seq_len);
    const mask_sequence_len = @max(query_end, kv_end);

    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_mode = try biasModeFor(bias_tensor, batch, num_heads, q_seq_len, kv_seq_len);
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    if (cudaForceHostGqaAttention()) {
        return gqaDenseAttentionHostFallback(
            self,
            q_tensor,
            k_tensor,
            v_tensor,
            bias_tensor,
            attn_or_mask,
            sliding_window,
            batch,
            q_seq_len,
            kv_seq_len,
            query_position_offset,
            kv_position_offset,
            num_heads,
            num_kv_heads,
            head_dim,
        );
    }

    const mask_device = if (attn_or_mask) |mask| try uploadTempU8(self, mask) else buffer_mod.DeviceBuffer{};
    const mask_len = if (attn_or_mask) |mask| mask.len else 0;
    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, q_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var prefill_profile_scope = beginPrefillProfile(self, .attention, q_seq_len);
    defer if (prefill_profile_scope) |*scope| scope.end();

    const attention_launch = launch: {
        if (cudaDebugDecodeScalarsReady(self)) {
            const scalar_launch = self.kernels.launchGqaAttentionDecodeScalarsF32(
                &self.ctx,
                device,
                q_tensor.buffer,
                k_tensor.buffer,
                v_tensor.buffer,
                mask_device,
                bias_buffer,
                self.debug_cuda_decode_scalars,
                batch,
                q_seq_len,
                kv_seq_len,
                num_heads,
                num_kv_heads,
                head_dim,
                query_position_offset,
                kv_position_offset,
                sliding_window,
                mask_sequence_len,
                mask_len,
                bias_mode,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable => null,
                else => return err,
            };
            if (scalar_launch) |kind| break :launch kind;
        }
        break :launch self.kernels.launchGqaAttentionF32(
            &self.ctx,
            device,
            q_tensor.buffer,
            k_tensor.buffer,
            v_tensor.buffer,
            mask_device,
            bias_buffer,
            batch,
            q_seq_len,
            kv_seq_len,
            num_heads,
            num_kv_heads,
            head_dim,
            query_position_offset,
            kv_position_offset,
            sliding_window,
            mask_sequence_len,
            mask_len,
            bias_mode,
        );
    } catch |err| {
        if (err == error.CudaKernelUnavailable and cudaAllowHostAttentionFallback()) {
            return gqaDenseAttentionHostFallback(
                self,
                q_tensor,
                k_tensor,
                v_tensor,
                bias_tensor,
                attn_or_mask,
                sliding_window,
                batch,
                q_seq_len,
                kv_seq_len,
                query_position_offset,
                kv_position_offset,
                num_heads,
                num_kv_heads,
                head_dim,
            );
        }
        return err;
    };
    switch (attention_launch) {
        .decode => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
        },
        .decode_generated => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
        },
        .decode_splitk_online_sm89,
        .decode_splitk_online_sm89_ineligible_fallback,
        .decode_splitk_online_sm89_symbol_fallback,
        => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            noteGqaSplitkOnlineDecodeStats(&self.stats, attention_launch, head_dim);
        },
        .decode_generated_score_prework => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
            self.stats.launch_attention_gqa_decode_score_prework += 1;
            self.stats.launch_attention_gqa_decode_score_prework_serial += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_decode_score_prework_serial_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_decode_score_prework_serial_hd512 += 1;
        },
        .decode_generated_score_prework_tiled64 => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
            self.stats.launch_attention_gqa_decode_score_prework += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64 += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_decode_score_prework_tiled64_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_decode_score_prework_tiled64_hd512 += 1;
        },
        .decode_generated_score_prework_tiled64_ineligible_fallback => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
            self.stats.launch_attention_gqa_decode_score_prework += 1;
            self.stats.launch_attention_gqa_decode_score_prework_serial += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64_fallbacks += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64_forbidden_routes += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_decode_score_prework_serial_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_decode_score_prework_serial_hd512 += 1;
        },
        .decode_generated_score_prework_tiled64_symbol_fallback => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
            self.stats.launch_attention_gqa_decode_score_prework += 1;
            self.stats.launch_attention_gqa_decode_score_prework_serial += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64_fallbacks += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64_symbol_fallbacks += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_decode_score_prework_serial_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_decode_score_prework_serial_hd512 += 1;
        },
        .decode_fast => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_fast += 1;
        },
        .decode_fast_fallback => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_fast_fallbacks += 1;
        },
        .prefill_fast => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_fast += 1;
        },
        .prefill_tiled => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_tiled += 1;
        },
        .prefill_mma => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_mma += 1;
        },
        .prefill_mma_m32 => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_mma_m32 += 1;
        },
        .prefill_tiled_f16_exact => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_tiled_f16_exact += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_prefill_tiled_f16_exact_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_prefill_tiled_f16_exact_hd512 += 1;
        },
        .prefill_tiled_f16_warp => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_tiled_f16_warp += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_prefill_tiled_f16_warp_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_prefill_tiled_f16_warp_hd512 += 1;
        },
        .prefill_flash_f16_sm89,
        .prefill_flash_f16_sm89_ineligible_fallback,
        .prefill_flash_f16_sm89_symbol_fallback,
        => {
            self.stats.launch_attention += 1;
            noteGqaFlashPrefillStats(&self.stats, attention_launch, head_dim, q_seq_len);
        },
        .scalar => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_scalar += 1;
        },
        .none => {},
    }
    if (q_seq_len > 1) {
        const phase_kernel_id: []const u8 = switch (attention_launch) {
            .prefill_fast => "termite_gqa_attention_prefill_fast_f32",
            .prefill_tiled => "termite_gqa_attention_prefill_tiled_f32",
            .prefill_mma => "termite_gqa_attention_prefill_mma_f32",
            .prefill_mma_m32 => "termite_gqa_attention_prefill_mma_m32_f32",
            .prefill_tiled_f16_exact => "termite_gqa_attention_prefill_tiled_f16_exact_f32",
            .prefill_tiled_f16_warp => "termite_gqa_attention_prefill_tiled_f16_warp_f32",
            .prefill_flash_f16_sm89 => if (head_dim == 256)
                quant_kernel_compiler.first_prefill_flash_cuda_hd256_kernel_id
            else
                quant_kernel_compiler.first_prefill_flash_cuda_hd512_kernel_id,
            .prefill_flash_f16_sm89_ineligible_fallback => "termite_gqa_attention_decode_turboquant_f32",
            .prefill_flash_f16_sm89_symbol_fallback => "termite_gqa_attention_decode_turboquant_f32",
            .scalar => "termite_gqa_attention_scalar_f32",
            .decode => "termite_gqa_attention_decode_f32",
            .decode_generated => "antfly_gqa_attention_decode_generated_f32",
            .decode_splitk_online_sm89 => if (head_dim == 256)
                quant_kernel_compiler.first_decode_splitk_online_cuda_hd256_kernel_id
            else
                quant_kernel_compiler.first_decode_splitk_online_cuda_hd512_kernel_id,
            .decode_splitk_online_sm89_ineligible_fallback => "termite_gqa_attention_decode_turboquant_f32",
            .decode_splitk_online_sm89_symbol_fallback => "termite_gqa_attention_decode_turboquant_f32",
            .decode_generated_score_prework => "antfly_gqa_attention_decode_generated_score_prework_serial_f32",
            .decode_generated_score_prework_tiled64 => "antfly_gqa_attention_decode_generated_score_prework_tiled64_f32",
            .decode_generated_score_prework_tiled64_ineligible_fallback => "antfly_gqa_attention_decode_generated_score_prework_tiled64_ineligible_fallback_f32",
            .decode_generated_score_prework_tiled64_symbol_fallback => "antfly_gqa_attention_decode_generated_score_prework_tiled64_symbol_fallback_f32",
            .decode_fast => "termite_gqa_attention_decode_fast_f32",
            .decode_fast_fallback => "termite_gqa_attention_decode_fast_fallback_f32",
            .none => "none",
        };
        // Attention launch topology is selected inside KernelModule and is not
        // surfaced through this API. Zero geometry is an explicit unknown
        // sentinel (`geometry_known=false`), not a fabricated launch shape.
        noteQ4Route(
            self,
            .attention_gqa_prefill,
            batch * q_seq_len,
            kv_seq_len,
            q_hidden,
            .gqa,
            .none,
            .handwritten,
            phase_kernel_id,
            .{ 0, 0, 0 },
            .{ 0, 0, 0 },
        );
    }
    return createTensor(self, device, shape, q_count);
}

fn compactKvRows(
    allocator: std.mem.Allocator,
    data: []const f32,
    token_count: usize,
    target_width: usize,
) ![]f32 {
    if (token_count == 0 or target_width == 0) return error.InvalidShape;
    if (data.len % token_count != 0) return error.InvalidShape;
    const source_width = data.len / token_count;
    if (source_width < target_width) return error.InvalidShape;
    if (source_width == target_width) return allocator.dupe(f32, data);
    const compact = try allocator.alloc(f32, token_count * target_width);
    errdefer allocator.free(compact);
    for (0..token_count) |token_idx| {
        @memcpy(
            compact[token_idx * target_width ..][0..target_width],
            data[token_idx * source_width ..][0..target_width],
        );
    }
    return compact;
}

fn gqaPagedAttentionWithHostKv(
    ctx: *anyopaque,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    attn_bias_ct: ?CT,
    attention: ops.AttentionContext,
    batch: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (gqaPagedAttentionWithDeviceKv(self, q_ct, k_ct, v_ct, attn_bias_ct, attention, batch, num_heads, num_kv_heads, head_dim)) |result| {
        return result;
    } else |err| switch (err) {
        error.DeviceReadUnsupported,
        error.DeviceReadFallback,
        error.DeviceWriteUnsupported,
        error.DeviceWriteFormatUnsupported,
        error.CudaPagedKvUnsupported,
        => {},
        else => return err,
    }

    self.stats.host_attention_fallbacks += 1;
    self.stats.paged_attention_host_fallbacks += 1;
    if (batch != 1 or attention.kv_batch != null) return error.CudaPagedKvBatchUnsupported;
    const manager = attention.kv_manager orelse return error.CudaPagedKvUnsupported;
    const kv = attention.kv_cache orelse return error.CudaPagedKvUnsupported;
    if (attention.total_sequence_len < attention.query_sequence_len) return error.InvalidShape;
    if (attention.kv_sequence_len == 0) return error.InvalidShape;
    const h_kv = try checkedMul(num_kv_heads, head_dim);

    if (!attention.skip_kv_write) {
        const k_tensor = tensorFromCt(k_ct);
        const v_tensor = tensorFromCt(v_ct);
        try ensureF32(k_tensor);
        try ensureF32(v_tensor);
        const suffix_count = try checkedMul(attention.query_sequence_len, h_kv);
        try ensureCount(k_tensor, suffix_count);
        try ensureCount(v_tensor, suffix_count);
        const k_suffix = try downloadAlloc(self, k_tensor);
        defer self.allocator.free(k_suffix);
        const v_suffix = try downloadAlloc(self, v_tensor);
        defer self.allocator.free(v_suffix);
        try manager.writeLayerKvSuffix(kv.sequence_id, attention.layer_index, attention.kv_sequence_len, attention.query_sequence_len, k_suffix, v_suffix);
    }

    const gathered = try manager.gatherLayerKv(self.allocator, kv.sequence_id, attention.layer_index, attention.kv_sequence_len);
    defer self.allocator.free(gathered.k);
    defer self.allocator.free(gathered.v);
    const compact_k = try compactKvRows(self.allocator, gathered.k, attention.kv_sequence_len, h_kv);
    defer self.allocator.free(compact_k);
    const compact_v = try compactKvRows(self.allocator, gathered.v, attention.kv_sequence_len, h_kv);
    defer self.allocator.free(compact_v);

    const kv_shape = [_]i32{ @intCast(attention.kv_sequence_len), @intCast(h_kv) };
    const gathered_k_ct = try fromFloat32ShapeOp(ctx, compact_k, &kv_shape);
    defer freeTensor(ctx, gathered_k_ct);
    const gathered_v_ct = try fromFloat32ShapeOp(ctx, compact_v, &kv_shape);
    defer freeTensor(ctx, gathered_v_ct);

    const query_position_offset = attention.total_sequence_len - attention.query_sequence_len;
    return gqaDenseAttention(
        ctx,
        q_ct,
        gathered_k_ct,
        gathered_v_ct,
        attn_bias_ct,
        attention.attn_or_mask,
        batch,
        attention.query_sequence_len,
        attention.kv_sequence_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        attention.kv_position_offset,
        attention.sliding_window,
        attention.total_sequence_len,
    );
}

fn gqaPagedAttentionWithCompressedDeviceKv(
    self: *CudaCompute,
    q_ct: CT,
    attn_bias_ct: ?CT,
    attention: ops.AttentionContext,
    paged: kv_storage_runtime.DevicePagedKvLayer,
    batch: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) anyerror!CT {
    const q_tensor = tensorFromCt(q_ct);
    try ensureF32(q_tensor);
    if (num_kv_heads == 0 or num_heads % num_kv_heads != 0) return error.InvalidShape;
    const q_hidden = try checkedMul(num_heads, head_dim);
    const h_kv = try checkedMul(num_kv_heads, head_dim);
    const q_count = try checkedMul(try checkedMul(batch, attention.query_sequence_len), q_hidden);
    const kv_count = try checkedMul(attention.kv_sequence_len, h_kv);
    try ensureCount(q_tensor, q_count);
    if (paged.token_count < attention.kv_sequence_len) return error.DeviceReadFallback;
    if (paged.key_row_bytes == 0 or paged.base_key_row_bytes == 0 or paged.base_key_row_bytes > paged.key_row_bytes) return error.DeviceReadFallback;
    if (paged.page_size_tokens == 0) return error.DeviceReadFallback;

    const runtime = paged.runtime orelse return error.DeviceReadFallback;
    const device_storage: *CudaKvDeviceStorage = @ptrCast(@alignCast(runtime));
    const layer_key: u64 = @intCast(paged.slot);
    const layer = device_storage.layers.getPtr(layer_key) orelse return error.DeviceReadFallback;
    if (layer.compressed_format == null or layer.compressed_format.? != paged.format) return error.DeviceReadFallback;
    if (layer.key_row_bytes != paged.key_row_bytes or layer.value_row_bytes != paged.v_row_stride) return error.DeviceReadFallback;
    if (layer.value_format == cuda_kv_value_format_f32 and layer.value_row_bytes < h_kv * @sizeOf(f32)) return error.DeviceReadFallback;
    if (layer.value_format == cuda_kv_value_format_int8_per_head and layer.value_row_bytes != kv_pool_mod.KvDType.int8.bytesForValueRow(@intCast(num_kv_heads), @intCast(head_dim))) return error.DeviceReadFallback;
    if (layer.value_format == cuda_kv_value_format_int4_group and layer.value_row_bytes != kv_pool_mod.KvDType.int4.bytesForValueRow(@intCast(num_kv_heads), @intCast(head_dim))) return error.DeviceReadFallback;
    if (layer.value_format == cuda_kv_value_format_f16 and layer.value_row_bytes != kv_pool_mod.KvDType.f16.bytesForValueRow(@intCast(num_kv_heads), @intCast(head_dim))) return error.DeviceReadFallback;
    const logical_blocks = if (attention.kv_cache) |kv| blk: {
        if (kv.logical_blocks) |blocks| break :blk blocks;
        const storage = attention.kv_storage orelse return error.DeviceReadFallback;
        const table = storage.blockTable(kv.sequence_id) orelse return error.DeviceReadFallback;
        break :blk table.blocks.items;
    } else return error.DeviceReadFallback;
    try device_storage.ensureLayerBlockTable(layer, logical_blocks, attention.kv_sequence_len, paged.page_size_tokens);

    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_mode = try biasModeFor(bias_tensor, batch, num_heads, attention.query_sequence_len, attention.kv_sequence_len);
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const mask_device = if (attention.attn_or_mask) |mask| try uploadTempU8(self, mask) else buffer_mod.DeviceBuffer{};
    const mask_len = if (attention.attn_or_mask) |mask| mask.len else 0;
    const query_position_offset = attention.total_sequence_len - attention.query_sequence_len;
    const query_end = try checkedAdd(query_position_offset, attention.query_sequence_len);
    const kv_end = try checkedAdd(attention.kv_position_offset, attention.kv_sequence_len);
    const mask_sequence_len = @max(query_end, kv_end);

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, q_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);

    const attention_block_table = if (layer.block_table_identity) buffer_mod.DeviceBuffer{} else layer.block_table;
    const attention_block_table_len: usize = if (layer.block_table_identity) 0 else layer.block_table_len;
    if (layer.block_table_identity) {
        self.stats.device_kv_paged_identity_attention_reads += 1;
    }
    const decode_scalars = if (cudaDebugDecodeScalarsReady(self))
        self.debug_cuda_decode_scalars
    else
        buffer_mod.DeviceBuffer{};

    var profile_scope = beginDecodeProfile(self, .gqa_attention, attention.query_sequence_len);
    defer if (profile_scope) |*scope| scope.end();
    var prefill_profile_scope = beginPrefillProfile(self, .attention, attention.query_sequence_len);
    defer if (prefill_profile_scope) |*scope| scope.end();
    // The exact generated score-prework candidate owns precedence whenever
    // the typed selector accepts this concrete storage/layout policy. The
    // split-summary experiment remains available only outside that exact
    // route; it must never shadow the chronological F16 Gemma 4 path.
    const score_prework_request = kernels_mod.GeneratedGqaScorePreworkRequest{
        .batch = batch,
        .q_seq_len = attention.query_sequence_len,
        .kv_seq_len = attention.kv_sequence_len,
        .num_heads = num_heads,
        .num_kv_heads = num_kv_heads,
        .head_dim = head_dim,
        .sliding_window = attention.sliding_window,
        .mask_len = mask_len,
        .bias_mode = bias_mode,
        .key_row_bytes = paged.key_row_bytes,
        .base_key_row_bytes = paged.base_key_row_bytes,
        .value_row_bytes = paged.v_row_stride,
        .block_count = attention_block_table_len,
        .page_size_tokens = paged.page_size_tokens,
        .format = paged.format,
        .value_format = layer.value_format,
        .physical_token_capacity = layer.capacity_tokens,
    };
    try noteGeneratedGqaScorePreworkTemplate(self, score_prework_request);
    try kernels_mod.validateGeneratedGqaScorePreworkConsumerRequirementFor(
        score_prework_request,
        self.kernels.gqa_score_prework_mode,
        self.kernels.gqa_score_prework_consumer_mode,
        self.ctx.info.compute_major,
        self.ctx.info.compute_minor,
    );
    const score_prework_selected = kernels_mod.generatedGqaScorePreworkSelectionWithConsumerFor(
        score_prework_request,
        self.kernels.gqa_score_prework_mode,
        self.kernels.gqa_score_prework_consumer_mode,
        self.ctx.info.compute_major,
        self.ctx.info.compute_minor,
    ) != null;
    if (!self.kernels.gqaSplitkOnlineDecodeProfileSelected() and
        !score_prework_selected and
        cudaTurboquantSplitAttentionEnabled() and
        batch == 1 and
        attention.query_sequence_len == 1 and
        bias_mode == 0 and
        mask_len == 0 and
        (paged.format == cuda_kv_format_polar4 or paged.format == cuda_kv_format_f16) and
        paged.base_key_row_bytes == paged.key_row_bytes)
    split_attention_blk: {
        const configured_chunk_size = cudaTurboquantSplitAttentionChunkSize();
        if (configured_chunk_size == 0) break :split_attention_blk;
        const split_capacity = @max(layer.capacity_tokens, attention.kv_sequence_len);
        // Grow the chunk size as capacity grows instead of abandoning the
        // split kernel: falling back to the serial per-key decode kernel is
        // a multi-x cliff for long-generation runs with large forced KV
        // capacities.
        const chunk_size = @max(configured_chunk_size, (split_capacity + 127) / 128);
        const chunk_count = (split_capacity + chunk_size - 1) / chunk_size;
        if (chunk_count <= 1 or chunk_count > 128) break :split_attention_blk;
        const partial_acc_count = checkedMul(try checkedMul(num_heads, chunk_count), head_dim) catch break :split_attention_blk;
        const partial_meta_count = checkedMul(try checkedMul(num_heads, chunk_count), @as(usize, 2)) catch break :split_attention_blk;
        var partial_acc = allocDeviceBuffer(self, partial_acc_count * @sizeOf(f32)) catch |err| switch (err) {
            error.CudaGraphCaptureUnsafeTempAlloc => break :split_attention_blk,
            else => return err,
        };
        defer releaseDeviceBuffer(self, &partial_acc);
        var partial_meta = allocDeviceBuffer(self, partial_meta_count * @sizeOf(f32)) catch |err| switch (err) {
            error.CudaGraphCaptureUnsafeTempAlloc => break :split_attention_blk,
            else => return err,
        };
        defer releaseDeviceBuffer(self, &partial_meta);
        const split_launched = self.kernels.launchGqaAttentionDecodeTurboquantSplitF32(
            &self.ctx,
            device,
            partial_acc,
            partial_meta,
            q_tensor.buffer,
            layer.k,
            layer.v,
            attention_block_table,
            q_count,
            attention.kv_sequence_len,
            num_heads,
            num_kv_heads,
            head_dim,
            query_position_offset,
            attention.kv_position_offset,
            attention.sliding_window,
            paged.key_row_bytes,
            paged.v_row_stride,
            attention_block_table_len,
            paged.page_size_tokens,
            paged.format,
            layer.value_format,
            layer.capacity_tokens,
            decode_scalars,
            chunk_size,
            chunk_count,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => break :split_attention_blk,
            else => return err,
        };
        if (split_launched) {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_fast += 1;
            if (cudaKvValueFormatCompressed(layer.value_format)) {
                self.stats.device_kv_compressed_v_reads += 1;
                self.stats.device_kv_compressed_v_bytes += try checkedMul(attention.kv_sequence_len, layer.value_row_bytes);
            }
            _ = kv_count;
            return createTensor(self, device, shape, q_count);
        }
    }
    const launch_result = try self.kernels.launchGqaAttentionDecodeTurboquantF32(
        &self.ctx,
        device,
        q_tensor.buffer,
        layer.k,
        layer.v,
        attention_block_table,
        mask_device,
        bias_buffer,
        batch,
        attention.query_sequence_len,
        attention.kv_sequence_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        attention.kv_position_offset,
        attention.sliding_window,
        mask_sequence_len,
        mask_len,
        bias_mode,
        paged.key_row_bytes,
        paged.base_key_row_bytes,
        paged.v_row_stride,
        attention_block_table_len,
        paged.page_size_tokens,
        paged.format,
        layer.value_format,
        layer.capacity_tokens,
        decode_scalars,
    );
    const launch_kind = launch_result.kind;
    if (launch_result.splitk_online_fallback) |fallback| {
        noteGqaSplitkOnlineDecodeStats(&self.stats, fallback, head_dim);
    }
    switch (launch_kind) {
        .decode => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
        },
        .decode_generated => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
        },
        .decode_splitk_online_sm89 => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            noteGqaSplitkOnlineDecodeStats(&self.stats, launch_kind, head_dim);
        },
        .decode_splitk_online_sm89_ineligible_fallback,
        .decode_splitk_online_sm89_symbol_fallback,
        => unreachable,
        .decode_generated_score_prework => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
            self.stats.launch_attention_gqa_decode_score_prework += 1;
            self.stats.launch_attention_gqa_decode_score_prework_serial += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_decode_score_prework_serial_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_decode_score_prework_serial_hd512 += 1;
        },
        .decode_generated_score_prework_tiled64 => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
            self.stats.launch_attention_gqa_decode_score_prework += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64 += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_decode_score_prework_tiled64_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_decode_score_prework_tiled64_hd512 += 1;
        },
        .decode_generated_score_prework_tiled64_ineligible_fallback => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
            self.stats.launch_attention_gqa_decode_score_prework += 1;
            self.stats.launch_attention_gqa_decode_score_prework_serial += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64_fallbacks += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64_forbidden_routes += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_decode_score_prework_serial_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_decode_score_prework_serial_hd512 += 1;
        },
        .decode_generated_score_prework_tiled64_symbol_fallback => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_generated += 1;
            self.stats.launch_attention_gqa_decode_score_prework += 1;
            self.stats.launch_attention_gqa_decode_score_prework_serial += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64_fallbacks += 1;
            self.stats.launch_attention_gqa_decode_score_prework_tiled64_symbol_fallbacks += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_decode_score_prework_serial_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_decode_score_prework_serial_hd512 += 1;
        },
        .decode_fast => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_fast += 1;
        },
        .decode_fast_fallback => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_decode += 1;
            self.stats.launch_attention_gqa_decode_fast_fallbacks += 1;
        },
        .prefill_fast => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_fast += 1;
        },
        .prefill_tiled => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_tiled += 1;
        },
        .prefill_mma => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_mma += 1;
        },
        .prefill_mma_m32 => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_mma_m32 += 1;
        },
        .prefill_tiled_f16_exact => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_tiled_f16_exact += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_prefill_tiled_f16_exact_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_prefill_tiled_f16_exact_hd512 += 1;
        },
        .prefill_tiled_f16_warp => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_prefill_tiled_f16_warp += 1;
            if (head_dim == 256) self.stats.launch_attention_gqa_prefill_tiled_f16_warp_hd256 += 1;
            if (head_dim == 512) self.stats.launch_attention_gqa_prefill_tiled_f16_warp_hd512 += 1;
        },
        .prefill_flash_f16_sm89,
        .prefill_flash_f16_sm89_ineligible_fallback,
        .prefill_flash_f16_sm89_symbol_fallback,
        => {
            self.stats.launch_attention += 1;
            noteGqaFlashPrefillStats(&self.stats, launch_kind, head_dim, attention.query_sequence_len);
        },
        .scalar => {
            self.stats.launch_attention += 1;
            self.stats.launch_attention_gqa_scalar += 1;
        },
        .none => {},
    }
    if (cudaKvValueFormatCompressed(layer.value_format)) {
        self.stats.device_kv_compressed_v_reads += 1;
        self.stats.device_kv_compressed_v_bytes += try checkedMul(attention.kv_sequence_len, layer.value_row_bytes);
    }
    _ = kv_count;
    return createTensor(self, device, shape, q_count);
}

fn gqaPagedAttentionWithDeviceKv(
    self: *CudaCompute,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    attn_bias_ct: ?CT,
    attention: ops.AttentionContext,
    batch: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) anyerror!CT {
    if (attention.kv_batch) |kv_batch| {
        return gqaPagedAttentionBatchWithDeviceKv(
            self,
            q_ct,
            k_ct,
            v_ct,
            attn_bias_ct,
            attention,
            kv_batch,
            batch,
            num_heads,
            num_kv_heads,
            head_dim,
        );
    }
    self.stats.device_kv_attempts += 1;
    if (batch != 1) {
        self.stats.device_kv_fail_batch += 1;
        return error.CudaPagedKvBatchUnsupported;
    }
    const kv = attention.kv_cache orelse {
        self.stats.device_kv_fail_no_cache += 1;
        return error.CudaPagedKvUnsupported;
    };
    const storage = attention.kv_storage orelse {
        self.stats.device_kv_fail_no_storage += 1;
        return error.CudaPagedKvUnsupported;
    };
    const hook = storage.device_write_hook orelse {
        self.stats.device_kv_fail_no_hook += 1;
        return error.DeviceWriteUnsupported;
    };
    if (attention.total_sequence_len < attention.query_sequence_len) {
        self.stats.device_kv_fail_shape += 1;
        return error.InvalidShape;
    }
    if (attention.kv_sequence_len == 0) {
        self.stats.device_kv_fail_shape += 1;
        return error.InvalidShape;
    }
    const h_kv = try checkedMul(num_kv_heads, head_dim);
    const compressed_format = cudaPagedKvFormat(storage.storage.config.dtype);
    if (cudaTurboquantKvFormat(storage.storage.config.dtype) != null and cudaTurboquantKvDisabled()) {
        self.stats.device_kv_fail_read += 1;
        return error.CudaPagedKvUnsupported;
    }

    if (!attention.skip_kv_write) {
        const k_tensor = tensorFromCt(k_ct);
        const v_tensor = tensorFromCt(v_ct);
        try ensureF32(k_tensor);
        try ensureF32(v_tensor);
        const suffix_count = try checkedMul(attention.query_sequence_len, h_kv);
        try ensureCount(k_tensor, suffix_count);
        try ensureCount(v_tensor, suffix_count);
        const suffix_bytes = try checkedMul(suffix_count, @sizeOf(f32));
        storage.writeLayerKvSuffixDevice(
            .{
                .sequence_id = kv.sequence_id,
                .layer_index = attention.layer_index,
                .total_token_count = attention.kv_sequence_len,
                .suffix_token_count = attention.query_sequence_len,
                .position_offset = attention.kv_position_offset,
                .num_kv_heads = @intCast(num_kv_heads),
                .head_dim = @intCast(head_dim),
            },
            .{ .handle = @ptrFromInt(k_tensor.buffer.ptr), .byte_offset = 0, .byte_len = suffix_bytes },
            .{ .handle = @ptrFromInt(v_tensor.buffer.ptr), .byte_offset = 0, .byte_len = suffix_bytes },
        ) catch |err| {
            self.stats.device_kv_fail_write += 1;
            return err;
        };
    }

    if (compressed_format) |expected_format| {
        const paged = hook.pagedLayerKvDevice(.{
            .sequence_id = kv.sequence_id,
            .layer_index = attention.layer_index,
            .token_count = attention.kv_sequence_len,
            .num_kv_heads = @intCast(num_kv_heads),
            .head_dim = @intCast(head_dim),
        }) catch |err| {
            self.stats.device_kv_fail_read += 1;
            return err;
        };
        if (paged.format != expected_format) {
            self.stats.device_kv_fail_shape += 1;
            return error.DeviceReadFallback;
        }
        const result = gqaPagedAttentionWithCompressedDeviceKv(
            self,
            q_ct,
            attn_bias_ct,
            attention,
            paged,
            batch,
            num_heads,
            num_kv_heads,
            head_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable,
            error.DeviceReadFallback,
            error.DeviceWriteFormatUnsupported,
            => {
                self.stats.device_kv_fail_read += 1;
                return error.DeviceReadFallback;
            },
            else => return err,
        };
        self.stats.device_kv_successes += 1;
        return result;
    }

    const gathered = hook.gatherLayerKvDevice(.{
        .sequence_id = kv.sequence_id,
        .layer_index = attention.layer_index,
        .token_count = attention.kv_sequence_len,
        .num_kv_heads = @intCast(num_kv_heads),
        .head_dim = @intCast(head_dim),
    }) catch |err| {
        self.stats.device_kv_fail_read += 1;
        return err;
    };
    if (gathered.value_element_bytes != @sizeOf(f32) or gathered.row_width < h_kv) {
        self.stats.device_kv_fail_shape += 1;
        return error.DeviceReadFallback;
    }
    const kv_count = try checkedMul(attention.kv_sequence_len, h_kv);
    const kv_bytes = try checkedMul(kv_count, @sizeOf(f32));
    if (gathered.k.byte_len < kv_bytes or gathered.v.byte_len < kv_bytes) {
        self.stats.device_kv_fail_shape += 1;
        return error.DeviceReadFallback;
    }

    const k_shape = try allocShape2(self.allocator, attention.kv_sequence_len, h_kv);
    var k_shape_owned = false;
    errdefer if (!k_shape_owned) self.allocator.free(k_shape);
    const v_shape = try allocShape2(self.allocator, attention.kv_sequence_len, h_kv);
    var v_shape_owned = false;
    errdefer if (!v_shape_owned) self.allocator.free(v_shape);
    const k_tensor = try self.allocator.create(CudaTensor);
    var k_tensor_owned = false;
    errdefer if (!k_tensor_owned) self.allocator.destroy(k_tensor);
    const q_tensor_for_ptr_type = tensorFromCt(q_ct);
    const k_device_ptr: @TypeOf(q_tensor_for_ptr_type.buffer.ptr) = @intCast(@intFromPtr(gathered.k.handle) + gathered.k.byte_offset);
    k_tensor.* = .{
        .buffer = .{ .ptr = k_device_ptr, .len = gathered.k.byte_len },
        .dtype = .f32,
        .shape = k_shape,
        .elem_count = kv_count,
        .owns_buffer = false,
    };
    k_shape_owned = true;
    const v_tensor = try self.allocator.create(CudaTensor);
    var v_tensor_owned = false;
    errdefer if (!v_tensor_owned) self.allocator.destroy(v_tensor);
    const v_device_ptr: @TypeOf(q_tensor_for_ptr_type.buffer.ptr) = @intCast(@intFromPtr(gathered.v.handle) + gathered.v.byte_offset);
    v_tensor.* = .{
        .buffer = .{ .ptr = v_device_ptr, .len = gathered.v.byte_len },
        .dtype = .f32,
        .shape = v_shape,
        .elem_count = kv_count,
        .owns_buffer = false,
    };
    v_shape_owned = true;

    k_tensor_owned = true;
    v_tensor_owned = true;
    defer freeTensor(self, k_tensor);
    defer freeTensor(self, v_tensor);

    const query_position_offset = attention.total_sequence_len - attention.query_sequence_len;
    const result = try gqaDenseAttention(
        self,
        q_ct,
        k_tensor,
        v_tensor,
        attn_bias_ct,
        attention.attn_or_mask,
        batch,
        attention.query_sequence_len,
        attention.kv_sequence_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        attention.kv_position_offset,
        attention.sliding_window,
        attention.total_sequence_len,
    );
    self.stats.device_kv_successes += 1;
    return result;
}

/// Correctness-first CUDA batch path. Quantized projections and the rest of
/// the transformer execute as one row batch; paged KV ownership remains per
/// sequence and each item uses the same generated decode-attention route as a
/// singleton. This is also the reference path for the packed descriptor
/// kernel, and avoids all host tensor materialization.
fn gqaPagedAttentionBatchWithDeviceKv(
    self: *CudaCompute,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    attn_bias_ct: ?CT,
    attention: ops.AttentionContext,
    kv_batch: []const ops.KvBatchView,
    batch: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) anyerror!CT {
    self.stats.device_kv_attempts += 1;
    if (batch < 2 or kv_batch.len != batch) {
        self.stats.device_kv_fail_batch += 1;
        return error.InvalidPagedKvBatch;
    }
    if (attention.attn_or_mask != null or attn_bias_ct != null) {
        return error.AttentionOrMaskBatchUnsupported;
    }

    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);

    const max_q_len = attention.query_sequence_len;
    const h_q = try checkedMul(num_heads, head_dim);
    const h_kv = try checkedMul(num_kv_heads, head_dim);
    const q_rows = try checkedMul(batch, max_q_len);
    try ensureCount(q_tensor, try checkedMul(q_rows, h_q));
    try ensureCount(k_tensor, try checkedMul(q_rows, h_kv));
    try ensureCount(v_tensor, try checkedMul(q_rows, h_kv));

    // Validate every descriptor before the first stream operation. This keeps
    // malformed multi-row requests from entering an asynchronous error path.
    for (kv_batch) |view| {
        const item_q_len = view.per_item_query_len orelse max_q_len;
        const item_total_len = view.per_item_total_len orelse attention.total_sequence_len;
        const item_kv_len = view.per_item_kv_len orelse attention.kv_sequence_len;
        if (item_q_len == 0 or item_q_len > max_q_len or item_total_len < item_q_len or item_kv_len < item_q_len) {
            self.stats.device_kv_fail_shape += 1;
            return error.InvalidShape;
        }
    }

    const output_count = try checkedMul(q_rows, h_q);
    const output_shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(output_shape);
    var output = try allocDeviceBuffer(self, try checkedMul(output_count, @sizeOf(f32)));
    errdefer releaseDeviceBuffer(self, &output);
    try self.kernels.launchFillF32(&self.ctx, output, output_count, 0.0);

    for (kv_batch, 0..) |view, batch_index| {
        const item_q_len = view.per_item_query_len orelse max_q_len;
        const item_total_len = view.per_item_total_len orelse attention.total_sequence_len;
        const item_kv_len = view.per_item_kv_len orelse attention.kv_sequence_len;
        const item_kv_position_offset = view.per_item_kv_position_offset orelse attention.kv_position_offset;
        const row_offset = try checkedMul(batch_index, max_q_len);
        const item_q = try sliceRows2DOp(self, q_ct, row_offset, item_q_len, h_q);
        defer freeTensor(self, item_q);
        const item_k = try sliceRows2DOp(self, k_ct, row_offset, item_q_len, h_kv);
        defer freeTensor(self, item_k);
        const item_v = try sliceRows2DOp(self, v_ct, row_offset, item_q_len, h_kv);
        defer freeTensor(self, item_v);

        const item_attention = ops.AttentionContext{
            .mode = view.per_item_mode orelse attention.mode,
            .total_sequence_len = item_total_len,
            .query_sequence_len = item_q_len,
            .kv_sequence_len = item_kv_len,
            .kv_position_offset = item_kv_position_offset,
            .decoder_runtime_resident_kv_sequence_len = attention.decoder_runtime_resident_kv_sequence_len,
            .decoder_runtime_resident_kv_position_offset = attention.decoder_runtime_resident_kv_position_offset,
            .sliding_window = attention.sliding_window,
            .kv_cache = view.kv_cache,
            .kv_manager = view.kv_manager,
            .kv_storage = view.kv_storage,
            .kv_batch = null,
            .layer_index = attention.layer_index,
            .skip_kv_write = attention.skip_kv_write,
            .attention_sink = attention.attention_sink,
        };
        const item_output = try gqaPagedAttentionWithDeviceKv(
            self,
            item_q,
            item_k,
            item_v,
            null,
            item_attention,
            1,
            num_heads,
            num_kv_heads,
            head_dim,
        );
        defer freeTensor(self, item_output);

        const item_output_tensor = tensorFromCt(item_output);
        const item_count = try checkedMul(item_q_len, h_q);
        try ensureCount(item_output_tensor, item_count);
        const byte_offset = try checkedMul(try checkedMul(row_offset, h_q), @sizeOf(f32));
        const byte_len = try checkedMul(item_count, @sizeOf(f32));
        const dst = buffer_mod.DeviceBuffer{
            .ptr = output.ptr + @as(u64, @intCast(byte_offset)),
            .len = byte_len,
        };
        try copyFromDeviceTracked(self, dst, item_output_tensor.buffer, byte_len);
    }

    self.stats.device_kv_batch_steps += 1;
    self.stats.device_kv_batch_items += batch;
    self.stats.device_kv_successes += 1;
    return createTensor(self, output, output_shape, output_count);
}
fn crossAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, enc_mask: []const i64, batch: usize, dec_seq: usize, enc_seq: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    const hidden = try checkedMul(num_heads, head_dim);
    const q_count = try checkedMul(try checkedMul(batch, dec_seq), hidden);
    const kv_count = try checkedMul(try checkedMul(batch, enc_seq), hidden);
    try ensureCount(q_tensor, q_count);
    try ensureCount(k_tensor, kv_count);
    try ensureCount(v_tensor, kv_count);
    if (enc_mask.len < try checkedMul(batch, enc_seq)) return error.InvalidShape;

    const mask_values = enc_mask[0 .. batch * enc_seq];
    const mask_device = if (allOnesI64(mask_values)) buffer_mod.DeviceBuffer{} else try uploadTempI64(self, mask_values);
    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, q_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchCrossAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, mask_device, batch, dec_seq, enc_seq, num_heads, head_dim);
    self.stats.launch_attention += 1;
    return createTensor(self, device, shape, q_count);
}
fn relativePositionBias(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: bool) anyerror!CT {
    return unsupportedCt();
}
fn debertaDisentangledAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, q_r_ct: CT, k_r_ct: CT, mask: []const i64, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    const q_r_tensor = tensorFromCt(q_r_ct);
    const k_r_tensor = tensorFromCt(k_r_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    try ensureF32(q_r_tensor);
    try ensureF32(k_r_tensor);
    if (seq_len == 0) return error.InvalidShape;
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    const rel_positions = try checkedSub(try checkedMul(2, seq_len), 1);
    const rel_count = try checkedMul(rel_positions, hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    try ensureCount(q_r_tensor, rel_count);
    try ensureCount(k_r_tensor, rel_count);
    if (mask.len < try checkedMul(batch, seq_len)) return error.InvalidShape;

    const mask_device = try uploadTempI64(self, mask);
    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const attention_mode = cudaDebertaAttentionMode();
    if ((attention_mode == .generated_tc or (attention_mode == .auto and cudaDebertaGeneratedTcAutoEnabled())) and seq_len <= 256 and head_dim == 64) {
        const requested_variant = cudaDebertaGeneratedTcVariant();
        const launched_m32 = if (requested_variant != .m16)
            self.kernels.launchDebertaAttentionTcF16M32N16(
                &self.ctx,
                device,
                q_tensor.buffer,
                k_tensor.buffer,
                v_tensor.buffer,
                q_r_tensor.buffer,
                k_r_tensor.buffer,
                mask_device,
                batch,
                seq_len,
                num_heads,
                head_dim,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable => false,
                else => return err,
            }
        else
            false;
        // Keep the smaller schedule as a binary-compatibility fallback for
        // deployments whose CUDA artifact predates the preferred M32 kernel.
        var launched_m16 = false;
        if (!launched_m32 and requested_variant != .m32) {
            launched_m16 = self.kernels.launchDebertaAttentionTcF16M16N32(
                &self.ctx,
                device,
                q_tensor.buffer,
                k_tensor.buffer,
                v_tensor.buffer,
                q_r_tensor.buffer,
                k_r_tensor.buffer,
                mask_device,
                batch,
                seq_len,
                num_heads,
                head_dim,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable => false,
                else => return err,
            };
        }
        if (launched_m32 or launched_m16) {
            self.stats.deberta_generated_tc_attention_calls += 1;
            if (launched_m32) {
                self.stats.deberta_generated_tc_m32_attention_calls += 1;
            } else {
                self.stats.deberta_generated_tc_m16_attention_calls += 1;
            }
            self.stats.launch_attention += 1;
            return createTensor(self, device, shape, count);
        }
        self.stats.deberta_generated_tc_attention_fallbacks += 1;
    }
    // Qualification on SM89/L4 shows the cuBLASLt materialized schedule is
    // the best current B4+ production route. Do not generalize that evidence
    // to other tensor-core generations; explicit mode remains available for
    // collecting qualification data there. Workspace admission inside the
    // route falls through to bounded fused attention before allocating.
    const auto_materialized_f16 = attention_mode == .auto and
        debertaMaterializedAutoTarget(self.ctx.info.compute_major, self.ctx.info.compute_minor) and
        batch >= 4 and seq_len >= 128 and seq_len <= 256 and head_dim == 64;
    if (attention_mode == .materialized_f16 or auto_materialized_f16) {
        if (try tryDebertaMaterializedAttentionF16(self, device, q_tensor, k_tensor, v_tensor, q_r_tensor, k_r_tensor, mask_device, batch, seq_len, num_heads, head_dim)) {
            self.stats.launch_attention += 1;
            return createTensor(self, device, shape, count);
        }
        self.stats.deberta_materialized_f16_attention_fallbacks += 1;
    }
    if (attention_mode == .streaming_f16 and seq_len <= 256 and head_dim == 64) {
        const q_f16 = try stageDebertaAttentionF16(self, &self.deberta_q_f16_scratch, q_tensor, count);
        const k_f16 = try stageDebertaAttentionF16(self, &self.deberta_k_f16_scratch, k_tensor, count);
        const v_f16 = try stageDebertaAttentionF16(self, &self.deberta_v_f16_scratch, v_tensor, count);
        const qr_f16 = try stageDebertaAttentionF16(self, &self.deberta_qr_f16_scratch, q_r_tensor, rel_count);
        const kr_f16 = try stageDebertaAttentionF16(self, &self.deberta_kr_f16_scratch, k_r_tensor, rel_count);
        if (q_f16 != null and k_f16 != null and v_f16 != null and qr_f16 != null and kr_f16 != null) {
            const launched = self.kernels.launchDebertaAttentionStreamF16(
                &self.ctx,
                device,
                q_f16.?,
                k_f16.?,
                v_f16.?,
                qr_f16.?,
                kr_f16.?,
                mask_device,
                batch,
                seq_len,
                num_heads,
                head_dim,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable => false,
                else => return err,
            };
            if (launched) {
                self.stats.deberta_stream_f16_attention_calls += 1;
                self.stats.launch_attention += 1;
                return createTensor(self, device, shape, count);
            }
        }
        self.stats.deberta_stream_f16_attention_fallbacks += 1;
    }
    if (cudaDebertaFusedAttentionEnabled(self) and seq_len <= 512) {
        const launched = self.kernels.launchDebertaAttentionFusedF32(
            &self.ctx,
            device,
            q_tensor.buffer,
            k_tensor.buffer,
            v_tensor.buffer,
            q_r_tensor.buffer,
            k_r_tensor.buffer,
            mask_device,
            batch,
            seq_len,
            num_heads,
            head_dim,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable => false,
            else => return err,
        };
        if (launched) {
            self.stats.deberta_fused_attention_calls += 1;
            self.stats.launch_attention += 1;
            return createTensor(self, device, shape, count);
        }
        // Count only attempted-but-failed fused launches: disabled or
        // ineligible shapes reaching the base kernel are not fallbacks.
        self.stats.deberta_fused_attention_fallbacks += 1;
    }
    try self.kernels.launchDebertaAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, q_r_tensor.buffer, k_r_tensor.buffer, mask_device, batch, seq_len, num_heads, head_dim);
    self.stats.launch_attention += 1;
    return createTensor(self, device, shape, count);
}

fn debertaDisentangledAttentionBackward(
    ctx: *anyopaque,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    q_r_ct: CT,
    k_r_ct: CT,
    mask: []const i64,
    d_out_ct: CT,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    const q_r_tensor = tensorFromCt(q_r_ct);
    const k_r_tensor = tensorFromCt(k_r_ct);
    const d_out_tensor = tensorFromCt(d_out_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    try ensureF32(q_r_tensor);
    try ensureF32(k_r_tensor);
    try ensureF32(d_out_tensor);
    if (seq_len == 0 or num_heads == 0 or head_dim == 0) return error.InvalidShape;
    const hidden = try checkedMul(num_heads, head_dim);
    const token_rows = try checkedMul(batch, seq_len);
    const token_count = try checkedMul(token_rows, hidden);
    const rel_rows = try checkedSub(try checkedMul(2, seq_len), 1);
    const rel_count = try checkedMul(rel_rows, hidden);
    try ensureCount(q_tensor, token_count);
    try ensureCount(k_tensor, token_count);
    try ensureCount(v_tensor, token_count);
    try ensureCount(d_out_tensor, token_count);
    try ensureCount(q_r_tensor, rel_count);
    try ensureCount(k_r_tensor, rel_count);
    if (mask.len < try checkedMul(batch, seq_len)) return error.InvalidShape;

    const packed_rows = try checkedAdd(try checkedMul(3, token_rows), try checkedMul(2, rel_rows));
    const packed_count = try checkedMul(packed_rows, hidden);
    const pair_count = try checkedMul(try checkedMul(try checkedMul(batch, num_heads), seq_len), seq_len);
    const mask_device = try uploadTempI64(self, mask);
    const shape = try allocShape2(self.allocator, packed_rows, hidden);
    errdefer self.allocator.free(shape);
    var probabilities = try allocDeviceBuffer(self, pair_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &probabilities);
    var d_scores = try allocDeviceBuffer(self, pair_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &d_scores);
    var device = try allocDeviceBuffer(self, packed_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchDebertaAttentionBackwardScoresF32(
        &self.ctx,
        probabilities,
        d_scores,
        q_tensor.buffer,
        k_tensor.buffer,
        v_tensor.buffer,
        q_r_tensor.buffer,
        k_r_tensor.buffer,
        mask_device,
        d_out_tensor.buffer,
        batch,
        seq_len,
        num_heads,
        head_dim,
    );
    try self.kernels.launchDebertaAttentionBackwardF32(
        &self.ctx,
        device,
        probabilities,
        d_scores,
        q_tensor.buffer,
        k_tensor.buffer,
        q_r_tensor.buffer,
        k_r_tensor.buffer,
        mask_device,
        d_out_tensor.buffer,
        batch,
        seq_len,
        num_heads,
        head_dim,
    );
    return createTensor(self, device, shape, packed_count);
}

fn debertaDisentangledAttentionPacked(
    ctx: *anyopaque,
    qkv_ct: CT,
    qr_kr_ct: CT,
    attn_bias_ct: CT,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const qkv = tensorFromCt(qkv_ct);
    const qr_kr = tensorFromCt(qr_kr_ct);
    const attn_bias = tensorFromCt(attn_bias_ct);
    try ensureF32(qkv);
    try ensureF32(qr_kr);
    try ensureF32(attn_bias);
    if (seq_len == 0 or num_heads == 0 or head_dim == 0) return error.InvalidShape;

    const hidden = try checkedMul(num_heads, head_dim);
    const token_rows = try checkedMul(batch, seq_len);
    const token_count = try checkedMul(token_rows, hidden);
    const rel_rows = try checkedSub(try checkedMul(2, seq_len), 1);
    const rel_count = try checkedMul(rel_rows, hidden);
    const pair_count = try checkedMul(try checkedMul(try checkedMul(batch, num_heads), seq_len), seq_len);
    try ensureCount(qkv, try checkedMul(3, token_count));
    try ensureCount(qr_kr, try checkedMul(2, rel_count));
    if (attn_bias.elem_count < pair_count) return error.InvalidShape;

    const token_bytes = try checkedMul(token_count, @sizeOf(f32));
    const rel_bytes = try checkedMul(rel_count, @sizeOf(f32));
    const q = buffer_mod.DeviceBuffer{ .ptr = qkv.buffer.ptr, .len = token_bytes };
    const k = buffer_mod.DeviceBuffer{ .ptr = qkv.buffer.ptr + token_bytes, .len = token_bytes };
    const v = buffer_mod.DeviceBuffer{ .ptr = qkv.buffer.ptr + 2 * token_bytes, .len = token_bytes };
    const q_r = buffer_mod.DeviceBuffer{ .ptr = qr_kr.buffer.ptr, .len = rel_bytes };
    const k_r = buffer_mod.DeviceBuffer{ .ptr = qr_kr.buffer.ptr + rel_bytes, .len = rel_bytes };

    const shape = try allocShape2(self.allocator, token_rows, hidden);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, token_bytes);
    errdefer device.free(&self.ctx);
    try self.kernels.launchDebertaAttentionBiasF32(
        &self.ctx,
        device,
        q,
        k,
        v,
        q_r,
        k_r,
        attn_bias.buffer,
        batch,
        seq_len,
        num_heads,
        head_dim,
    );
    self.stats.launch_attention += 1;
    self.stats.packed_attention_forward_calls += 1;
    return createTensor(self, device, shape, token_count);
}

fn debertaDisentangledAttentionBackwardPacked(
    ctx: *anyopaque,
    qkv_ct: CT,
    qr_kr_ct: CT,
    attn_bias_ct: CT,
    d_out_ct: CT,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const qkv = tensorFromCt(qkv_ct);
    const qr_kr = tensorFromCt(qr_kr_ct);
    const attn_bias = tensorFromCt(attn_bias_ct);
    const d_out = tensorFromCt(d_out_ct);
    try ensureF32(qkv);
    try ensureF32(qr_kr);
    try ensureF32(attn_bias);
    try ensureF32(d_out);
    if (seq_len == 0 or num_heads == 0 or head_dim == 0) return error.InvalidShape;

    const hidden = try checkedMul(num_heads, head_dim);
    const token_rows = try checkedMul(batch, seq_len);
    const token_count = try checkedMul(token_rows, hidden);
    const rel_rows = try checkedSub(try checkedMul(2, seq_len), 1);
    const rel_count = try checkedMul(rel_rows, hidden);
    const pair_count = try checkedMul(try checkedMul(try checkedMul(batch, num_heads), seq_len), seq_len);
    const packed_rows = try checkedAdd(try checkedMul(3, token_rows), try checkedMul(2, rel_rows));
    const packed_count = try checkedMul(packed_rows, hidden);
    try ensureCount(qkv, try checkedMul(3, token_count));
    try ensureCount(qr_kr, try checkedMul(2, rel_count));
    try ensureCount(d_out, token_count);
    if (attn_bias.elem_count < pair_count) return error.InvalidShape;

    const token_bytes = try checkedMul(token_count, @sizeOf(f32));
    const rel_bytes = try checkedMul(rel_count, @sizeOf(f32));
    const q = buffer_mod.DeviceBuffer{ .ptr = qkv.buffer.ptr, .len = token_bytes };
    const k = buffer_mod.DeviceBuffer{ .ptr = qkv.buffer.ptr + token_bytes, .len = token_bytes };
    const v = buffer_mod.DeviceBuffer{ .ptr = qkv.buffer.ptr + 2 * token_bytes, .len = token_bytes };
    const q_r = buffer_mod.DeviceBuffer{ .ptr = qr_kr.buffer.ptr, .len = rel_bytes };
    const k_r = buffer_mod.DeviceBuffer{ .ptr = qr_kr.buffer.ptr + rel_bytes, .len = rel_bytes };

    const shape = try allocShape2(self.allocator, packed_rows, hidden);
    errdefer self.allocator.free(shape);
    var probabilities = try allocDeviceBuffer(self, try checkedMul(pair_count, @sizeOf(f32)));
    defer releaseDeviceBuffer(self, &probabilities);
    var d_scores = try allocDeviceBuffer(self, try checkedMul(pair_count, @sizeOf(f32)));
    defer releaseDeviceBuffer(self, &d_scores);
    var device = try allocDeviceBuffer(self, try checkedMul(packed_count, @sizeOf(f32)));
    errdefer device.free(&self.ctx);
    try self.kernels.launchDebertaAttentionBackwardScoresBiasF32(
        &self.ctx,
        probabilities,
        d_scores,
        q,
        k,
        v,
        q_r,
        k_r,
        attn_bias.buffer,
        d_out.buffer,
        batch,
        seq_len,
        num_heads,
        head_dim,
    );
    try self.kernels.launchDebertaAttentionBackwardBiasF32(
        &self.ctx,
        device,
        probabilities,
        d_scores,
        q,
        k,
        q_r,
        k_r,
        attn_bias.buffer,
        d_out.buffer,
        batch,
        seq_len,
        num_heads,
        head_dim,
    );
    self.stats.packed_attention_backward_calls += 1;
    return createTensor(self, device, shape, packed_count);
}

fn trainingRuntimeStatsOp(ctx: *anyopaque) ops.TrainingRuntimeStats {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const stats = self.snapshotStats();
    var largest_d2h_transfer: usize = 0;
    for (stats.download_top_sizes) |bytes| largest_d2h_transfer = @max(largest_d2h_transfer, bytes);
    return .{
        .device_allocations = @intCast(stats.device_alloc_calls),
        .device_frees = @intCast(stats.device_free_calls),
        .h2d_bytes = @intCast(stats.h2d_bytes),
        .d2h_bytes = @intCast(stats.d2h_bytes),
        .largest_d2h_transfer_bytes = @intCast(largest_d2h_transfer),
        .to_float32_calls = @intCast(stats.to_float32_calls),
        .download_alloc_calls = @intCast(stats.download_alloc_calls),
        .stream_synchronizations = @intCast(stats.stream_syncs),
        .upload_synchronizations = @intCast(stats.upload_syncs),
        .temp_cache_hits = @intCast(stats.temp_buffer_hits),
        .temp_cache_misses = @intCast(stats.temp_buffer_misses),
        .kernel_launches = @intCast(stats.kernel_launches),
        .packed_attention_forward_calls = @intCast(stats.packed_attention_forward_calls),
        .packed_attention_backward_calls = @intCast(stats.packed_attention_backward_calls),
        .exact_gelu_forward_calls = @intCast(stats.exact_gelu_forward_calls),
        .exact_gelu_backward_calls = @intCast(stats.exact_gelu_backward_calls),
        .training_input_uploads = @intCast(stats.training_input_uploads),
        .training_input_upload_bytes = @intCast(stats.training_input_upload_bytes),
    };
}

fn layerNormBackwardOp(ctx: *anyopaque, input_ct: CT, gamma_ct: CT, beta_ct: CT, d_y_ct: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input = tensorFromCt(input_ct);
    const gamma = tensorFromCt(gamma_ct);
    const beta = tensorFromCt(beta_ct);
    const d_y = tensorFromCt(d_y_ct);
    try ensureF32(input);
    try ensureF32(gamma);
    try ensureF32(beta);
    try ensureF32(d_y);
    if (dim == 0 or input.elem_count % dim != 0 or d_y.elem_count != input.elem_count or gamma.elem_count != dim or beta.elem_count != dim) return error.InvalidShape;
    const rows = input.elem_count / dim;
    const packed_rows = try checkedAdd(rows, 2);
    const packed_count = try checkedMul(packed_rows, dim);
    const shape = try allocShape2(self.allocator, packed_rows, dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, packed_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchFillF32(&self.ctx, device, packed_count, 0.0);
    try self.kernels.launchLayerNormBackwardF32(&self.ctx, device, input.buffer, gamma.buffer, d_y.buffer, rows, dim, eps);
    return createTensor(self, device, shape, packed_count);
}
fn windowedSelfAttention(
    ctx: *anyopaque,
    input: CT,
    norm_weight: CT,
    norm_bias: CT,
    qkv_weight: CT,
    qkv_bias: CT,
    proj_weight: CT,
    proj_bias: CT,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    num_heads: usize,
    window_size: usize,
) anyerror!CT {
    if (num_heads == 0 or dim % num_heads != 0 or window_size == 0) return error.InvalidShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    try ensureCount(input_tensor, try checkedMul(try checkedMul(batch, height * width), dim));

    const normed = try layerNorm(ctx, input, norm_weight, norm_bias, dim, 1e-5);
    defer freeTensor(ctx, normed);
    const normed_tensor = tensorFromCt(normed);

    const pad_h = (window_size - (height % window_size)) % window_size;
    const pad_w = (window_size - (width % window_size)) % window_size;
    const padded_h = try checkedAdd(height, pad_h);
    const padded_w = try checkedAdd(width, pad_w);
    const windows_h = padded_h / window_size;
    const windows_w = padded_w / window_size;
    const window_area = try checkedMul(window_size, window_size);
    const window_count = try checkedMul(try checkedMul(batch, windows_h), windows_w);
    const rows = try checkedMul(window_count, window_area);
    const packed_count = try checkedMul(rows, dim);

    var packed_device = try allocDeviceBuffer(self, packed_count * @sizeOf(f32));
    var packed_device_owned = false;
    errdefer if (!packed_device_owned) packed_device.free(&self.ctx);
    try self.kernels.launchPackWindowsF32(&self.ctx, packed_device, normed_tensor.buffer, batch, height, width, dim, window_size, padded_h, padded_w);
    self.stats.launch_other += 1;
    const packed_shape = try allocShape2(self.allocator, rows, dim);
    var packed_shape_owned = false;
    errdefer if (!packed_shape_owned) self.allocator.free(packed_shape);
    const packed_tokens = try createTensor(self, packed_device, packed_shape, packed_count);
    packed_device_owned = true;
    packed_shape_owned = true;
    defer freeTensor(ctx, packed_tokens);

    const qkv = try linear(ctx, packed_tokens, qkv_weight, qkv_bias, rows, dim, dim * 3);
    defer freeTensor(ctx, qkv);
    const split = try splitLastDim3(ctx, qkv, rows, dim);
    defer freeTensor(ctx, split.first);
    defer freeTensor(ctx, split.second);
    defer freeTensor(ctx, split.third);

    const attn_shape = try allocShape2(self.allocator, rows, dim);
    errdefer self.allocator.free(attn_shape);
    var attn_device = try allocDeviceBuffer(self, packed_count * @sizeOf(f32));
    errdefer attn_device.free(&self.ctx);
    try self.kernels.launchAttentionF32(
        &self.ctx,
        attn_device,
        tensorFromCt(split.first).buffer,
        tensorFromCt(split.second).buffer,
        tensorFromCt(split.third).buffer,
        .{},
        .{},
        window_count,
        window_area,
        num_heads,
        dim / num_heads,
        false,
        false,
        0,
        false,
    );
    self.stats.launch_attention += 1;
    const attn = try createTensor(self, attn_device, attn_shape, packed_count);
    defer freeTensor(ctx, attn);

    const projected = try linear(ctx, attn, proj_weight, proj_bias, rows, dim, dim);
    defer freeTensor(ctx, projected);
    const projected_tensor = tensorFromCt(projected);

    const out_count = try checkedMul(try checkedMul(batch, height * width), dim);
    var out_device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer out_device.free(&self.ctx);
    try self.kernels.launchUnpadWindowsF32(&self.ctx, out_device, projected_tensor.buffer, batch, height, width, dim, window_size, padded_h, padded_w);
    self.stats.launch_other += 1;
    const out_shape = try allocShape2(self.allocator, batch * height * width, dim);
    errdefer self.allocator.free(out_shape);
    return createTensor(self, out_device, out_shape, out_count);
}
fn channelSelfAttention(
    ctx: *anyopaque,
    input: CT,
    norm_weight: CT,
    norm_bias: CT,
    qkv_weight: CT,
    qkv_bias: CT,
    proj_weight: CT,
    proj_bias: CT,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
) anyerror!CT {
    if (groups == 0 or dim % groups != 0) return error.InvalidShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const channels_per_group = dim / groups;
    if (channels_per_group > 256) return error.UnsupportedShape;
    const total_rows = try checkedMul(batch, seq_len);
    const total_count = try checkedMul(total_rows, dim);
    try ensureCount(tensorFromCt(input), total_count);

    const profile = platform.env.getenvBool("ANTFLY_INFERENCE_READ_PROFILE");
    var op_start = monotonicNowNs();
    const normed = try layerNorm(ctx, input, norm_weight, norm_bias, dim, 1e-5);
    if (profile) logCudaFlorenceProfile("channel_layer_norm", op_start);
    defer freeTensor(ctx, normed);
    op_start = monotonicNowNs();
    const qkv = try linear(ctx, normed, qkv_weight, qkv_bias, total_rows, dim, dim * 3);
    if (profile) logCudaFlorenceProfile("channel_qkv_linear", op_start);
    defer freeTensor(ctx, qkv);
    const qkv_tensor = tensorFromCt(qkv);

    const score_count = try checkedMul(try checkedMul(batch, groups), try checkedMul(channels_per_group, channels_per_group));
    var score_device = try allocDeviceBuffer(self, score_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &score_device);
    op_start = monotonicNowNs();
    try self.kernels.launchChannelScoresSoftmaxF32(&self.ctx, score_device, qkv_tensor.buffer, batch, seq_len, dim, groups);
    if (profile) logCudaFlorenceProfile("channel_scores_softmax", op_start);
    self.stats.launch_attention += 1;

    const attended_shape = try allocShape2(self.allocator, total_rows, dim);
    errdefer self.allocator.free(attended_shape);
    var attended_device = try allocDeviceBuffer(self, total_count * @sizeOf(f32));
    errdefer attended_device.free(&self.ctx);
    op_start = monotonicNowNs();
    try self.kernels.launchChannelApplyF32(&self.ctx, attended_device, qkv_tensor.buffer, score_device, batch, seq_len, dim, groups);
    if (profile) logCudaFlorenceProfile("channel_apply", op_start);
    self.stats.launch_attention += 1;
    const attended = try createTensor(self, attended_device, attended_shape, total_count);
    defer freeTensor(ctx, attended);

    op_start = monotonicNowNs();
    const projected = try linear(ctx, attended, proj_weight, proj_bias, total_rows, dim, dim);
    if (profile) logCudaFlorenceProfile("channel_proj_linear", op_start);
    return projected;
}
fn tokenGridConv2d(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    bias: CT,
    batch: usize,
    in_channels: usize,
    out_channels: usize,
    height: usize,
    width: usize,
    kernel_h: usize,
    kernel_w: usize,
    stride_h: usize,
    stride_w: usize,
    padding_h: usize,
    padding_w: usize,
    groups: usize,
) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    try ensureCount(input_tensor, try checkedMul(try checkedMul(batch, height * width), in_channels));

    const image_count = try checkedMul(try checkedMul(batch, in_channels), height * width);
    var image_device = try allocDeviceBuffer(self, image_count * @sizeOf(f32));
    var image_device_owned = false;
    errdefer if (!image_device_owned) image_device.free(&self.ctx);
    try self.kernels.launchTokenToNchwF32(&self.ctx, image_device, input_tensor.buffer, batch, in_channels, height, width);
    self.stats.launch_other += 1;
    const image_shape = try self.allocator.dupe(i64, &.{
        @as(i64, @intCast(batch)),
        @as(i64, @intCast(in_channels)),
        @as(i64, @intCast(height)),
        @as(i64, @intCast(width)),
    });
    var image_shape_owned = false;
    errdefer if (!image_shape_owned) self.allocator.free(image_shape);
    const image_ct = try createTensor(self, image_device, image_shape, image_count);
    image_device_owned = true;
    image_shape_owned = true;
    defer freeTensor(ctx, image_ct);

    const out_ct = try conv2d(ctx, image_ct, weight, bias, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups);
    defer freeTensor(ctx, out_ct);
    const out_tensor = tensorFromCt(out_ct);
    const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
    const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
    const out_count = try checkedMul(try checkedMul(batch, out_h * out_w), out_channels);
    var out_device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer out_device.free(&self.ctx);
    try self.kernels.launchNchwToTokenF32(&self.ctx, out_device, out_tensor.buffer, batch, out_channels, out_h, out_w);
    self.stats.launch_other += 1;
    const out_shape = try allocShape2(self.allocator, batch * out_h * out_w, out_channels);
    errdefer self.allocator.free(out_shape);
    return createTensor(self, out_device, out_shape, out_count);
}
fn conv1d(_: *anyopaque, _: CT, _: CT, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
    return unsupportedCt();
}
fn conv2d(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    bias: CT,
    batch: usize,
    in_channels: usize,
    out_channels: usize,
    height: usize,
    width: usize,
    kernel_h: usize,
    kernel_w: usize,
    stride_h: usize,
    stride_w: usize,
    padding_h: usize,
    padding_w: usize,
    groups: usize,
) anyerror!CT {
    if (groups == 0 or in_channels % groups != 0 or out_channels % groups != 0) return error.InvalidShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(bias_tensor);
    const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
    const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
    const out_count = try checkedMul(try checkedMul(batch, out_channels), try checkedMul(out_h, out_w));
    try ensureCount(input_tensor, try checkedMul(try checkedMul(batch, in_channels), try checkedMul(height, width)));
    try ensureCount(weight_tensor, try checkedMul(try checkedMul(out_channels, in_channels / groups), try checkedMul(kernel_h, kernel_w)));
    try ensureCount(bias_tensor, out_channels);
    const shape = try self.allocator.dupe(i64, &.{ @as(i64, @intCast(batch)), @as(i64, @intCast(out_channels)), @as(i64, @intCast(out_h)), @as(i64, @intCast(out_w)) });
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchConv2dF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups, out_h, out_w);
    return createTensor(self, device, shape, out_count);
}
fn rope(ctx: *anyopaque, input: CT, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (seq_len == 0 or head_dim == 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0) return error.InvalidShape;
    if (input_tensor.elem_count % head_dim != 0) return error.InvalidShape;
    const total_chunks = input_tensor.elem_count / head_dim;
    if (total_chunks % seq_len != 0) return error.InvalidShape;
    const chunks_per_position = total_chunks / seq_len;
    if (chunks_per_position == 0) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var rope_profile_scope = beginPrefillProfile(self, .rope, seq_len);
    defer if (rope_profile_scope) |*scope| scope.end();
    const launch_result = if (cudaDebugDecodeScalarsReady(self))
        self.kernels.launchRopeDecodeScalarsF32(&self.ctx, device, input_tensor.buffer, self.debug_cuda_decode_scalars, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs)
    else
        self.kernels.launchRopeF32(&self.ctx, device, input_tensor.buffer, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs);
    launch_result catch |err| {
        if (err == error.CudaKernelUnavailable and cudaAllowHostAttentionFallback()) {
            return ropeHostFallback(self, input_tensor, seq_len, head_dim, rope_dim, theta, freq_scale, position_offset, consecutive_pairs);
        }
        return err;
    };
    self.stats.launch_rope += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn rmsNormHeadsRope(ctx: *anyopaque, input: CT, weight: CT, rows: usize, total_dim: usize, head_dim: usize, rope_dim: usize, eps: f32, theta: f32, freq_scale: f32, position_offset: usize, seq_len: usize, consecutive_pairs: bool, scale: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (cudaDisableHeadNormRopeFusion()) {
        self.stats.head_norm_rope_fused_fallbacks += 1;
        return null;
    }
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    if (rows == 0 or total_dim == 0 or head_dim == 0 or total_dim % head_dim != 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0 or seq_len == 0) return error.InvalidShape;
    try ensureCount(input_tensor, try checkedMul(rows, total_dim));
    try ensureCount(weight_tensor, head_dim);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    // Fused head-RMSNorm + RoPE kernel. Attributed to `.rope` (the rotary path);
    // standalone RMSNorm is timed separately under `.norm`.
    var rope_profile_scope = beginPrefillProfile(self, .rope, rows);
    defer if (rope_profile_scope) |*scope| scope.end();
    const launch_result = if (cudaDebugDecodeScalarsReady(self))
        self.kernels.launchRmsNormHeadsRopeDecodeScalarsF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, self.debug_cuda_decode_scalars, rows, total_dim, head_dim, rope_dim, eps, theta, freq_scale, position_offset, seq_len, consecutive_pairs, scale)
    else
        self.kernels.launchRmsNormHeadsRopeF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, total_dim, head_dim, rope_dim, eps, theta, freq_scale, position_offset, seq_len, consecutive_pairs, scale);
    launch_result catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            self.stats.head_norm_rope_fused_fallbacks += 1;
            return null;
        },
        else => return err,
    };
    self.stats.head_norm_rope_fused_hits += 1;
    self.stats.launch_norm += 1;
    self.stats.launch_norm_head_rope += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn ropeScaled(ctx: *anyopaque, input: CT, scale: f32, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (seq_len == 0 or head_dim == 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0) return error.InvalidShape;
    if (input_tensor.elem_count % head_dim != 0) return error.InvalidShape;
    const total_chunks = input_tensor.elem_count / head_dim;
    if (total_chunks % seq_len != 0) return error.InvalidShape;
    const chunks_per_position = total_chunks / seq_len;
    if (chunks_per_position == 0) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var rope_profile_scope = beginPrefillProfile(self, .rope, seq_len);
    defer if (rope_profile_scope) |*scope| scope.end();
    const launch_result = if (cudaDebugDecodeScalarsReady(self))
        self.kernels.launchRopeScaledDecodeScalarsF32(&self.ctx, device, input_tensor.buffer, self.debug_cuda_decode_scalars, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs, scale)
    else
        self.kernels.launchRopeScaledF32(&self.ctx, device, input_tensor.buffer, total_chunks, head_dim, rope_dim, theta, freq_scale, position_offset, seq_len, chunks_per_position, consecutive_pairs, scale);
    launch_result catch |err| switch (err) {
        error.CudaKernelUnavailable => return null,
        else => return err,
    };
    self.stats.launch_rope += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn ropePerItem(ctx: *anyopaque, input: CT, batch: usize, max_seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, query_lengths: []const usize, position_offsets: []const usize, consecutive_pairs: bool) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (batch == 0 or max_seq_len == 0 or head_dim == 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0) return error.InvalidShape;
    if (query_lengths.len != batch or position_offsets.len != batch) return error.InvalidShape;
    const row_count = try checkedMul(batch, max_seq_len);
    if (input_tensor.elem_count % row_count != 0) return error.InvalidShape;
    const row_dim = input_tensor.elem_count / row_count;
    if (row_dim % head_dim != 0) return error.InvalidShape;
    const num_heads = row_dim / head_dim;
    if (num_heads == 0) return error.InvalidShape;
    for (query_lengths) |len| if (len > max_seq_len) return error.InvalidShape;

    const query_lengths_u32 = try usizeSliceToU32(self.allocator, query_lengths);
    defer self.allocator.free(query_lengths_u32);
    const position_offsets_u32 = try usizeSliceToU32(self.allocator, position_offsets);
    defer self.allocator.free(position_offsets_u32);
    const query_lengths_device = try uploadTempU32(self, query_lengths_u32);
    const position_offsets_device = try uploadTempU32(self, position_offsets_u32);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    var rope_profile_scope = beginPrefillProfile(self, .rope, row_count);
    defer if (rope_profile_scope) |*scope| scope.end();
    self.kernels.launchRopePerItemF32(&self.ctx, device, input_tensor.buffer, query_lengths_device, position_offsets_device, batch, max_seq_len, num_heads, head_dim, rope_dim, theta, freq_scale, consecutive_pairs) catch |err| {
        if (err == error.CudaKernelUnavailable and cudaAllowHostAttentionFallback()) {
            return ropePerItemHostFallback(self, input_tensor, batch, max_seq_len, head_dim, rope_dim, theta, freq_scale, query_lengths, position_offsets, consecutive_pairs);
        }
        return err;
    };
    self.stats.launch_rope += 1;
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn cudaDecoderRuntimeReserveKvTokens(config: gpt_model.Config, current_kv_tokens: usize) usize {
    if (config.sliding_window > 0) return @intCast(config.sliding_window);
    if (config.max_position_embeddings > 0) return @intCast(config.max_position_embeddings);
    return if (current_kv_tokens > 0) current_kv_tokens else 1;
}

fn cudaDecoderRuntimeOverrideLevel() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_DECODER_SLOT_OVERRIDE_LEVEL") orelse 4;
}

fn cudaGemmaPreparedLayers(configured_layers: usize) usize {
    const configured = platform.env.getenvUsize("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN_GATED_LAYERS") orelse configured_layers;
    return @min(configured_layers, configured);
}

fn cudaGemmaNormSlot(layer: usize, comptime kind: enum { attn_pre, attn_post, ffn_pre, ffn_post }) usize {
    return switch (kind) {
        .attn_pre => layer * 4,
        .attn_post => layer * 4 + 1,
        .ffn_pre => layer * 4 + 2,
        .ffn_post => layer * 4 + 3,
    };
}

fn cudaGemmaLinearSlot(layer: usize, comptime kind: enum { attn_q, attn_k, attn_v, attn_out_proj, mlp_gate, mlp_up, mlp_down }) usize {
    return switch (kind) {
        .attn_q => layer * 7,
        .attn_k => layer * 7 + 1,
        .attn_v => layer * 7 + 2,
        .attn_out_proj => layer * 7 + 3,
        .mlp_gate => layer * 7 + 4,
        .mlp_up => layer * 7 + 5,
        .mlp_down => layer * 7 + 6,
    };
}

fn cudaFamilyStateMatches(self: *const CudaCompute, config: gpt_model.Config, configured_layer_count: usize) bool {
    const state = self.decoder_runtime_family_state;
    return state.prepared and
        state.configured_layer_count == configured_layer_count and
        state.hidden_size == config.hidden_size and
        state.intermediate_size == config.intermediate_size and
        state.num_hidden_layers == config.num_hidden_layers and
        state.num_attention_heads == config.num_attention_heads and
        state.num_key_value_heads == config.num_key_value_heads and
        state.num_global_key_value_heads == config.num_global_key_value_heads and
        state.attention_head_dim == config.attention_head_dim and
        state.global_head_dim == config.global_head_dim and
        state.vocab_size == config.vocab_size and
        state.sliding_window == config.sliding_window and
        state.sliding_window_pattern == config.sliding_window_pattern and
        state.ple_hidden_size == config.ple_hidden_size;
}

fn cudaNoteFamilyPrepared(self: *CudaCompute, config: gpt_model.Config, reserve_kv_tokens: usize, configured_layer_count: usize) void {
    self.decoder_runtime_family_state = .{
        .prepared = true,
        .reserve_kv_tokens = reserve_kv_tokens,
        .configured_layer_count = configured_layer_count,
        .hidden_size = config.hidden_size,
        .intermediate_size = config.intermediate_size,
        .num_hidden_layers = config.num_hidden_layers,
        .num_attention_heads = config.num_attention_heads,
        .num_key_value_heads = config.num_key_value_heads,
        .num_global_key_value_heads = config.num_global_key_value_heads,
        .attention_head_dim = config.attention_head_dim,
        .global_head_dim = config.global_head_dim,
        .vocab_size = config.vocab_size,
        .sliding_window = config.sliding_window,
        .sliding_window_pattern = config.sliding_window_pattern,
        .ple_hidden_size = config.ple_hidden_size,
    };
}

fn cudaPrepareRmsNormSlotByName(
    self: *CudaCompute,
    cb: *const ops.ComputeBackend,
    config: gpt_model.Config,
    slot: usize,
    name: []const u8,
    hidden_size: usize,
) !bool {
    const weight = try gpt_arch.getModelWeight(cb, config, name);
    defer cb.free(weight);
    return decoderRuntimePrepareRmsNormOp(self, &.{
        .slot = slot,
        .weight = weight,
        .hidden_size = hidden_size,
    });
}

fn cudaPrepareLinearNoBiasSlotFromWeight(
    self: *CudaCompute,
    slot: usize,
    weight: CT,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const resident = residentTensorForSlot(self, tensorFromCt(weight)) orelse {
        self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
        return false;
    };
    try decoderRuntimePrepareLinearSlot(self, slot, resident, null, in_dim, out_dim);
    return true;
}

fn cudaPrepareLinearNoBiasSlotByName(
    self: *CudaCompute,
    cb: *const ops.ComputeBackend,
    config: gpt_model.Config,
    slot: usize,
    name: []const u8,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const weight = try gpt_arch.getModelWeight(cb, config, name);
    defer cb.free(weight);
    return cudaPrepareLinearNoBiasSlotFromWeight(self, slot, weight, in_dim, out_dim);
}

fn cudaPrepareGemmaDecoderRuntimeFamily(
    self: *CudaCompute,
    allocator: std.mem.Allocator,
    config: gpt_model.Config,
    configured_layer_count: usize,
) !bool {
    _ = allocator;
    if (config.family != .gemma or config.usesMoe()) return false;
    const layer_count: usize = @intCast(config.num_hidden_layers);
    if (layer_count == 0 or layer_count > 256) return false;

    const cb = self.computeBackend();
    const prepared_layer_count = cudaGemmaPreparedLayers(@min(configured_layer_count, layer_count));
    const override_level = cudaDecoderRuntimeOverrideLevel();
    if (override_level == 0) return false;
    const prepare_gated_block_slots = platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_GATED_BLOCK");

    for (0..prepared_layer_count) |layer| {
        var name_buf: [256]u8 = undefined;
        const shares_kv = config.layerSharesKv(layer);
        const layer_head_dim: usize = @intCast(config.effectiveHeadDimForLayer(layer));
        const layer_kv_heads: usize = @intCast(config.effectiveKVHeadsForLayer(layer));
        const attention_input_size: usize = @as(usize, @intCast(config.num_attention_heads)) * layer_head_dim;
        const kv_dim = layer_kv_heads * layer_head_dim;

        const attn_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.input_layernorm.weight", .{layer});
        if (!(try cudaPrepareRmsNormSlotByName(self, &cb, config, cudaGemmaNormSlot(layer, .attn_pre), attn_norm_name, config.hidden_size))) return false;

        if (override_level >= 2) {
            const q_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.q_proj.weight", .{layer});
            if (!(try cudaPrepareLinearNoBiasSlotByName(self, &cb, config, cudaGemmaLinearSlot(layer, .attn_q), q_name, config.hidden_size, attention_input_size))) return false;

            if (!shares_kv) {
                const k_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.k_proj.weight", .{layer});
                const k_weight = try gpt_arch.getModelWeight(&cb, config, k_name);
                defer cb.free(k_weight);
                if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .attn_k), k_weight, config.hidden_size, kv_dim))) return false;

                const v_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.v_proj.weight", .{layer});
                const v_weight = gpt_arch.getModelWeight(&cb, config, v_name) catch |err| switch (err) {
                    error.MissingWeight, error.WeightNotFound => if (config.layerOmitsVProj(layer)) k_weight else return err,
                    else => return err,
                };
                const owns_v_weight = v_weight != k_weight;
                defer if (owns_v_weight) cb.free(v_weight);
                if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .attn_v), v_weight, config.hidden_size, kv_dim))) return false;
            }

            const attn_out_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.o_proj.weight", .{layer});
            if (!(try cudaPrepareLinearNoBiasSlotByName(self, &cb, config, cudaGemmaLinearSlot(layer, .attn_out_proj), attn_out_name, attention_input_size, config.hidden_size))) return false;

            if (prepare_gated_block_slots and config.position_encoding == .rope) {
                const q_head_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.q_norm.weight", .{layer});
                if (!(try cudaPrepareRmsNormSlotByName(self, &cb, config, gemma4_runtime.qHeadNormSlot(configured_layer_count, layer), q_head_norm_name, layer_head_dim))) return false;
                if (!shares_kv) {
                    const k_head_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.k_norm.weight", .{layer});
                    if (!(try cudaPrepareRmsNormSlotByName(self, &cb, config, gemma4_runtime.kHeadNormSlot(configured_layer_count, layer), k_head_norm_name, layer_head_dim))) return false;
                }
            }
        }

        if (override_level >= 3) {
            const attn_post_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer});
            if (!(try cudaPrepareRmsNormSlotByName(self, &cb, config, cudaGemmaNormSlot(layer, .attn_post), attn_post_norm_name, config.hidden_size))) return false;

            const ffn_pre_norm_name = std.fmt.bufPrint(&name_buf, "model.layers.{d}.pre_feedforward_layernorm.weight", .{layer}) catch return error.NameTooLong;
            const ffn_pre_norm = gpt_arch.getModelWeight(&cb, config, ffn_pre_norm_name) catch |err| switch (err) {
                error.MissingWeight, error.WeightNotFound => null,
                else => return err,
            };
            if (ffn_pre_norm) |weight| {
                defer cb.free(weight);
                if (!(try decoderRuntimePrepareRmsNormOp(self, &.{
                    .slot = cudaGemmaNormSlot(layer, .ffn_pre),
                    .weight = weight,
                    .hidden_size = config.hidden_size,
                }))) return false;
            }

            const ffn_post_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.post_feedforward_layernorm.weight", .{layer});
            if (!(try cudaPrepareRmsNormSlotByName(self, &cb, config, cudaGemmaNormSlot(layer, .ffn_post), ffn_post_norm_name, config.hidden_size))) return false;
        }

        if (override_level >= 4) {
            const gate_w = try gpt_arch.getFFNWeight(&cb, config, layer, "gate", &name_buf);
            defer cb.free(gate_w);
            if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .mlp_gate), gate_w, config.hidden_size, config.intermediateSize(layer)))) return false;

            const up_w = try gpt_arch.getFFNWeight(&cb, config, layer, "up", &name_buf);
            defer cb.free(up_w);
            if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .mlp_up), up_w, config.hidden_size, config.intermediateSize(layer)))) return false;

            const down_w = try gpt_arch.getFFNWeight(&cb, config, layer, "down", &name_buf);
            defer cb.free(down_w);
            if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, cudaGemmaLinearSlot(layer, .mlp_down), down_w, config.intermediateSize(layer), config.hidden_size))) return false;

            if (prepare_gated_block_slots and config.hasPle()) {
                const ple_gate_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input.inp_gate.weight", .{layer});
                const ple_gate_w = gpt_arch.getModelWeight(&cb, config, ple_gate_name) catch |err| switch (err) {
                    error.MissingWeight, error.WeightNotFound => blk: {
                        const fallback = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input_gate.weight", .{layer});
                        break :blk try gpt_arch.getModelWeight(&cb, config, fallback);
                    },
                    else => return err,
                };
                defer cb.free(ple_gate_w);
                if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, gemma4_runtime.pleGateSlot(configured_layer_count, layer), ple_gate_w, config.hidden_size, config.ple_hidden_size))) return false;

                const ple_proj_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input.proj.weight", .{layer});
                const ple_proj_w = gpt_arch.getModelWeight(&cb, config, ple_proj_name) catch |err| switch (err) {
                    error.MissingWeight, error.WeightNotFound => blk: {
                        const fallback = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_projection.weight", .{layer});
                        break :blk try gpt_arch.getModelWeight(&cb, config, fallback);
                    },
                    else => return err,
                };
                defer cb.free(ple_proj_w);
                if (!(try cudaPrepareLinearNoBiasSlotFromWeight(self, gemma4_runtime.pleProjSlot(configured_layer_count, layer), ple_proj_w, config.ple_hidden_size, config.hidden_size))) return false;

                const ple_post_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input.post_norm.weight", .{layer});
                const ple_post_norm_w = gpt_arch.getModelWeight(&cb, config, ple_post_norm_name) catch |err| switch (err) {
                    error.MissingWeight, error.WeightNotFound => blk: {
                        const fallback = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.post_per_layer_input_norm.weight", .{layer});
                        break :blk try gpt_arch.getModelWeight(&cb, config, fallback);
                    },
                    else => return err,
                };
                defer cb.free(ple_post_norm_w);
                if (!(try decoderRuntimePrepareRmsNormOp(self, &.{
                    .slot = gemma4_runtime.plePostNormSlot(configured_layer_count, layer),
                    .weight = ple_post_norm_w,
                    .hidden_size = config.hidden_size,
                }))) return false;
            }
        }
    }

    return true;
}

fn decoderRuntimePrepareGreedyOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeGreedyRequest) anyerror!bool {
    _ = ctx;
    if (request.hidden_size == 0 or request.num_layers == 0 or request.num_heads == 0 or
        request.num_kv_heads == 0 or request.head_dim == 0 or request.vocab_size == 0 or
        request.kv_tokens == 0)
    {
        return false;
    }
    return true;
}

fn decoderRuntimePrepareOrReuseFamilyOp(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    config: gpt_model.Config,
    current_kv_tokens: usize,
    configured_layer_count: usize,
) anyerror!ops.DecoderRuntimePrepareReuseResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const reserve_kv_tokens = cudaDecoderRuntimeReserveKvTokens(config, current_kv_tokens);
    if (cudaFamilyStateMatches(self, config, configured_layer_count)) {
        if (reserve_kv_tokens <= self.decoder_runtime_family_state.reserve_kv_tokens) {
            return .{
                .prepared = true,
                .reserve_kv_tokens = reserve_kv_tokens,
                .fast_hit = true,
            };
        }
        const prepared = try decoderRuntimePrepareGreedyOp(ctx, &.{
            .hidden_size = config.hidden_size,
            .intermediate_size = config.intermediate_size,
            .num_layers = config.num_hidden_layers,
            .num_heads = config.num_attention_heads,
            .num_kv_heads = config.effectiveKVHeads(),
            .head_dim = config.headDim(),
            .vocab_size = config.vocab_size,
            .kv_tokens = reserve_kv_tokens,
        });
        if (prepared) self.decoder_runtime_family_state.reserve_kv_tokens = reserve_kv_tokens;
        return .{
            .prepared = prepared,
            .reserve_kv_tokens = reserve_kv_tokens,
            .used_greedy = prepared,
        };
    }

    if (!(try decoderRuntimePrepareGreedyOp(ctx, &.{
        .hidden_size = config.hidden_size,
        .intermediate_size = config.intermediate_size,
        .num_layers = config.num_hidden_layers,
        .num_heads = config.num_attention_heads,
        .num_kv_heads = config.effectiveKVHeads(),
        .head_dim = config.headDim(),
        .vocab_size = config.vocab_size,
        .kv_tokens = reserve_kv_tokens,
    }))) {
        return .{ .reserve_kv_tokens = reserve_kv_tokens };
    }

    const prepared = try cudaPrepareGemmaDecoderRuntimeFamily(self, allocator, config, configured_layer_count);
    if (prepared) cudaNoteFamilyPrepared(self, config, reserve_kv_tokens, configured_layer_count);
    return .{
        .prepared = prepared,
        .reserve_kv_tokens = reserve_kv_tokens,
    };
}

fn decoderRuntimeReadyOp(ctx: *anyopaque) bool {
    const self: *const CudaCompute = @ptrCast(@alignCast(ctx));
    return self.decoder_runtime_family_state.prepared or
        self.decoder_runtime_linear_slots.count() != 0 or
        self.decoder_runtime_rms_norm_slots.count() != 0;
}

fn decoderRuntimePrepareRmsNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareRmsNormRequest) anyerror!bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(request.weight);
    const resident = residentTensorForSlot(self, weight_tensor) orelse {
        self.stats.decoder_runtime_rms_norm_slot_prepare_misses += 1;
        return false;
    };
    try ensureF32(resident);
    try ensureCount(resident, request.hidden_size);
    try self.decoder_runtime_rms_norm_slots.put(self.allocator, request.slot, .{
        .weight = borrowedSlotTensor(resident),
        .hidden_size = request.hidden_size,
    });
    self.stats.decoder_runtime_rms_norm_slot_prepares += 1;
    return true;
}

fn decoderRuntimeEnsureRmsNormSlotOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeEnsureRmsNormSlotRequest) anyerror!?usize {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(request.weight);
    const resident = residentTensorForSlot(self, weight_tensor) orelse {
        self.stats.decoder_runtime_rms_norm_slot_prepare_misses += 1;
        return null;
    };
    try ensureF32(resident);
    try ensureCount(resident, request.hidden_size);
    var it = self.decoder_runtime_rms_norm_slots.iterator();
    while (it.next()) |entry| {
        const slot = entry.value_ptr;
        if (slot.weight.buffer.ptr == resident.buffer.ptr and slot.hidden_size == request.hidden_size) {
            return entry.key_ptr.*;
        }
    }
    const slot_id = self.decoder_runtime_next_dynamic_slot;
    self.decoder_runtime_next_dynamic_slot += 1;
    try self.decoder_runtime_rms_norm_slots.put(self.allocator, slot_id, .{
        .weight = borrowedSlotTensor(resident),
        .hidden_size = request.hidden_size,
    });
    self.stats.decoder_runtime_rms_norm_slot_prepares += 1;
    return slot_id;
}

fn decoderRuntimeApplyRmsNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const slot = self.decoder_runtime_rms_norm_slots.getPtr(request.slot) orelse {
        self.stats.decoder_runtime_rms_norm_apply_misses += 1;
        return null;
    };
    if (slot.hidden_size != request.hidden_size) return error.UnexpectedOutputShape;
    self.stats.decoder_runtime_rms_norm_apply_hits += 1;
    return try rmsNorm(ctx, request.input, &slot.weight, request.hidden_size, request.eps);
}

fn decoderRuntimeInputRows(input: CT, in_dim: usize) !usize {
    if (in_dim == 0) return error.InvalidShape;
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_tensor.elem_count % in_dim != 0) return error.InvalidShape;
    const rows = input_tensor.elem_count / in_dim;
    if (rows == 0) return error.InvalidShape;
    return rows;
}

fn sameOptionalSlotBias(slot_bias: ?CudaTensor, request_bias: ?*CudaTensor) bool {
    if (slot_bias) |bias| {
        const requested = request_bias orelse return false;
        return bias.buffer.ptr == requested.buffer.ptr and bias.buffer.len == requested.buffer.len and bias.elem_count == requested.elem_count;
    }
    return request_bias == null;
}

fn decoderRuntimePrepareLinearSlot(
    self: *CudaCompute,
    slot_id: usize,
    weight: *CudaTensor,
    bias: ?*CudaTensor,
    in_dim: usize,
    out_dim: usize,
) !void {
    try ensureF32Bf16OrQuantized(weight);
    try ensureLogicalWeightCount(weight, try checkedMul(out_dim, in_dim));
    if (bias) |bias_tensor| {
        try ensureF32(bias_tensor);
        try ensureCount(bias_tensor, out_dim);
    }
    try self.decoder_runtime_linear_slots.put(self.allocator, slot_id, .{
        .weight = borrowedSlotTensor(weight),
        .bias = if (bias) |bias_tensor| borrowedSlotTensor(bias_tensor) else null,
        .in_dim = in_dim,
        .out_dim = out_dim,
    });
    self.stats.decoder_runtime_linear_slot_prepares += 1;
}

fn decoderRuntimePrepareLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareLinearRequest) anyerror!bool {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    _ = request.dense_fallback_max_bytes;
    const weight = residentTensorForSlot(self, tensorFromCt(request.weight)) orelse {
        self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
        return false;
    };
    const bias = residentTensorForSlot(self, tensorFromCt(request.bias)) orelse {
        self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
        return false;
    };
    try decoderRuntimePrepareLinearSlot(self, request.slot, weight, bias, request.in_dim, request.out_dim);
    return true;
}

fn decoderRuntimeEnsureLinearSlotOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeEnsureLinearSlotRequest) anyerror!?usize {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight = residentTensorForSlot(self, tensorFromCt(request.weight)) orelse {
        self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
        return null;
    };
    const bias = if (request.bias) |bias_ct| blk: {
        break :blk residentTensorForSlot(self, tensorFromCt(bias_ct)) orelse {
            self.stats.decoder_runtime_linear_slot_prepare_misses += 1;
            return null;
        };
    } else null;

    var it = self.decoder_runtime_linear_slots.iterator();
    while (it.next()) |entry| {
        const slot = entry.value_ptr;
        if (slot.weight.buffer.ptr == weight.buffer.ptr and
            slot.in_dim == request.in_dim and
            slot.out_dim == request.out_dim and
            sameOptionalSlotBias(slot.bias, bias))
        {
            return entry.key_ptr.*;
        }
    }

    const slot_id = self.decoder_runtime_next_dynamic_slot;
    self.decoder_runtime_next_dynamic_slot += 1;
    try decoderRuntimePrepareLinearSlot(self, slot_id, weight, bias, request.in_dim, request.out_dim);
    return slot_id;
}

fn decoderRuntimeApplyLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const slot = self.decoder_runtime_linear_slots.getPtr(request.slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    if (slot.in_dim != request.in_dim or slot.out_dim != request.out_dim) return error.UnexpectedOutputShape;
    self.stats.decoder_runtime_linear_apply_hits += 1;
    const rows = try decoderRuntimeInputRows(request.input, request.in_dim);
    if (slot.bias) |*bias| {
        return try linear(ctx, request.input, &slot.weight, bias, rows, request.in_dim, request.out_dim);
    }
    return try linearNoBias(ctx, request.input, &slot.weight, rows, request.in_dim, request.out_dim);
}

fn decoderRuntimeApplyLinearArgmaxOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearArgmaxRequest) anyerror!?usize {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const slot = self.decoder_runtime_linear_slots.getPtr(request.slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    if (slot.in_dim != request.in_dim or slot.out_dim != request.out_dim) return error.UnexpectedOutputShape;
    self.stats.decoder_runtime_linear_apply_hits += 1;
    const rows = try decoderRuntimeInputRows(request.input, request.in_dim);
    if (slot.bias == null) {
        if (try linearNoBiasArgmaxRowsSuppress(
            ctx,
            request.input,
            &slot.weight,
            rows,
            request.in_dim,
            request.out_dim,
            &.{},
            self.allocator,
        )) |tokens| {
            defer self.allocator.free(tokens);
            return @intCast(tokens[rows - 1]);
        }
        const token = (try linearNoBiasArgmaxLastRow(ctx, request.input, &slot.weight, rows, request.in_dim, request.out_dim)) orelse return null;
        return @intCast(token);
    }
    const logits = try linear(ctx, request.input, &slot.weight, &slot.bias.?, rows, request.in_dim, request.out_dim);
    defer freeTensor(ctx, logits);
    const token = (try argmaxLastRow(ctx, logits, rows, request.out_dim)) orelse return null;
    return @intCast(token);
}

fn decoderRuntimeApplyLinearPairOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearPairRequest) anyerror!?ops.LinearNoBiasPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const slot_a = self.decoder_runtime_linear_slots.getPtr(request.slot_a) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    const slot_b = self.decoder_runtime_linear_slots.getPtr(request.slot_b) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    if (slot_a.in_dim != request.in_dim or slot_b.in_dim != request.in_dim or
        slot_a.out_dim != request.out_dim or slot_b.out_dim != request.out_dim)
    {
        return error.UnexpectedOutputShape;
    }
    if (slot_a.bias != null or slot_b.bias != null) return null;
    self.stats.decoder_runtime_linear_apply_hits += 2;
    self.stats.decoder_runtime_linear_pair_apply_hits += 1;
    const rows = try decoderRuntimeInputRows(request.input, request.in_dim);
    var profile_scope = beginDecodeProfile(self, .ffn_gate_up, rows);
    defer if (profile_scope) |*scope| scope.end();
    return try linearNoBiasPair(ctx, request.input, &slot_a.weight, &slot_b.weight, rows, request.in_dim, request.out_dim);
}

fn decoderRuntimeApplyLinearQkvOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearQkvRequest) anyerror!?ops.LinearNoBiasTripleResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_slot = self.decoder_runtime_linear_slots.getPtr(request.q_slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    const k_slot = self.decoder_runtime_linear_slots.getPtr(request.k_slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    const v_slot = self.decoder_runtime_linear_slots.getPtr(request.v_slot) orelse {
        self.stats.decoder_runtime_linear_apply_misses += 1;
        return null;
    };
    if (q_slot.in_dim != request.in_dim or k_slot.in_dim != request.in_dim or v_slot.in_dim != request.in_dim or
        q_slot.out_dim != request.q_out_dim or k_slot.out_dim != request.kv_out_dim or v_slot.out_dim != request.kv_out_dim)
    {
        return error.UnexpectedOutputShape;
    }
    if (q_slot.bias != null or k_slot.bias != null or v_slot.bias != null) return null;
    self.stats.decoder_runtime_linear_apply_hits += 3;
    self.stats.decoder_runtime_linear_qkv_apply_hits += 1;
    const rows = try decoderRuntimeInputRows(request.input, request.in_dim);
    var profile_scope = beginDecodeProfile(self, .qkv, rows);
    defer if (profile_scope) |*scope| scope.end();
    if (try linearNoBiasQkv(ctx, request.input, &q_slot.weight, &k_slot.weight, &v_slot.weight, rows, request.in_dim, request.q_out_dim, request.kv_out_dim)) |fused| {
        return fused;
    }
    const q = try linearNoBias(ctx, request.input, &q_slot.weight, rows, request.in_dim, request.q_out_dim);
    errdefer freeTensor(ctx, q);
    const k = try linearNoBias(ctx, request.input, &k_slot.weight, rows, request.in_dim, request.kv_out_dim);
    errdefer freeTensor(ctx, k);
    const v = try linearNoBias(ctx, request.input, &v_slot.weight, rows, request.in_dim, request.kv_out_dim);
    return .{ .first = q, .second = k, .third = v };
}

fn decoderRuntimeApplyScaledAddScaleOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyScaledAddScaleRequest) anyerror!?CT {
    const lhs_tensor = tensorFromCt(request.lhs);
    const rhs_tensor = tensorFromCt(request.rhs);
    if (request.dim == 0 or lhs_tensor.elem_count != request.dim or rhs_tensor.elem_count != request.dim) return error.InvalidShape;
    return try addWeightedScalars(ctx, request.lhs, request.rhs, request.lhs_scale * request.output_scale, request.output_scale);
}

fn decoderRuntimeApplyBlockRmsNorm(
    ctx: *anyopaque,
    input: CT,
    slot_id: ?usize,
    hidden_size: usize,
    eps: f32,
) anyerror!?CT {
    const slot = slot_id orelse return input;
    return decoderRuntimeApplyRmsNormOp(ctx, &.{
        .slot = slot,
        .input = input,
        .hidden_size = hidden_size,
        .eps = eps,
    });
}

fn noteQ4Route(
    self: *CudaCompute,
    op: CudaQ4RouteOp,
    rows: usize,
    k: usize,
    n: usize,
    bundle: CudaQ4RouteBundle,
    epilogue: CudaQ4RouteEpilogue,
    provider: CudaQ4RouteProvider,
    kernel_id: []const u8,
    grid: [3]usize,
    block: [3]usize,
) void {
    self.dispatch_stats.noteQ4(
        self.allocator,
        self.q4_route_census_enabled,
        op,
        rows,
        k,
        n,
        bundle,
        epilogue,
        provider,
        kernel_id,
        .{
            std.math.cast(u32, grid[0]) orelse std.math.maxInt(u32),
            std.math.cast(u32, grid[1]) orelse std.math.maxInt(u32),
            std.math.cast(u32, grid[2]) orelse std.math.maxInt(u32),
        },
        .{
            std.math.cast(u32, block[0]) orelse std.math.maxInt(u32),
            std.math.cast(u32, block[1]) orelse std.math.maxInt(u32),
            std.math.cast(u32, block[2]) orelse std.math.maxInt(u32),
        },
    );
}

fn noteQ4FfnRoute(
    self: *CudaCompute,
    op: CudaQ4RouteOp,
    rows: usize,
    k: usize,
    n: usize,
    bundle: CudaQ4RouteBundle,
    epilogue: CudaQ4RouteEpilogue,
    provider: CudaQ4RouteProvider,
    kernel_id: []const u8,
    grid_x: usize,
    block_x: usize,
) void {
    noteQ4Route(
        self,
        op,
        rows,
        k,
        n,
        bundle,
        epilogue,
        provider,
        kernel_id,
        .{ grid_x, 1, 1 },
        .{ block_x, 1, 1 },
    );
}

fn q4LinearRouteOp(out_dim: usize) CudaQ4RouteOp {
    // This is the same vocabulary-width boundary already used by the CUDA
    // decode profiler. Keeping it centralized makes sampled (full logits) and
    // greedy (fused argmax) LM-head traffic reconcile under one census op.
    return if (out_dim >= 100_000) .lm_head else .linear;
}

fn generatedExactE2BPairKernelId(intermediate_size: usize) []const u8 {
    return switch (intermediate_size) {
        6144 => "antfly_q4_0_pair_activation_f32_e2b_6144_exact_v1",
        12288 => "antfly_q4_0_pair_activation_f32_e2b_12288_exact_v1",
        else => "antfly_q4_0_pair_activation_f32_e2b_unknown_exact_v1",
    };
}

fn generatedExactE2BDownKernelId(intermediate_size: usize) []const u8 {
    return switch (intermediate_size) {
        6144 => "antfly_q4_0_down_f32_e2b_6144_exact_v1",
        12288 => "antfly_q4_0_down_f32_e2b_12288_exact_v1",
        else => "antfly_q4_0_down_f32_e2b_unknown_exact_v1",
    };
}

fn generatedE2BPairQ8ZpKernelId(intermediate_size: usize) []const u8 {
    return switch (intermediate_size) {
        6144 => "antfly_q4_0_pair_activation_q8_1_e2b_6144_mmv_v1",
        12288 => "antfly_q4_0_pair_activation_q8_1_e2b_12288_mmv_v1",
        else => "antfly_q4_0_pair_activation_q8_1_mmv_v1",
    };
}

fn generatedE2BDownQ8ZpKernelId(intermediate_size: usize) []const u8 {
    return switch (intermediate_size) {
        6144 => "antfly_q4_0_down_q8_1_e2b_6144_mmv_v1",
        12288 => "antfly_q4_0_down_q8_1_e2b_12288_mmv_v1",
        else => "antfly_q4_0_down_q8_1_mmv_v1",
    };
}

fn generatedE2BDownQ8BlockSize(intermediate_size: usize) usize {
    return if (intermediate_size == 6144) 128 else 256;
}

fn generatedE2BPairGgmlQ8_1KernelId(intermediate_size: usize) []const u8 {
    return switch (intermediate_size) {
        6144 => quant_kernel_compiler.first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_kernel_id,
        12288 => quant_kernel_compiler.first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_kernel_id,
        else => "antfly_q4_0_pair_activation_ggml_q8_1_e2b_unknown_mmv_v1",
    };
}

fn generatedE2BDownGgmlQ8_1KernelId(intermediate_size: usize) []const u8 {
    return switch (intermediate_size) {
        6144 => quant_kernel_compiler.first_e2b_cuda_q4_0_down_ggml_q8_1_6144_kernel_id,
        12288 => quant_kernel_compiler.first_e2b_cuda_q4_0_down_ggml_q8_1_12288_kernel_id,
        else => "antfly_q4_0_down_ggml_q8_1_e2b_unknown_mmv_v1",
    };
}

fn tryRunQ4_0Sm89GgmlQ8_1E2BFfn(
    ctx: *anyopaque,
    request: *const ops.RunGatedFfnResidualRequest,
    rows: usize,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!self.sm89_q4_0_ggml_q8_1_gate.enabledFor(
        self.ctx.info.compute_major,
        self.ctx.info.compute_minor,
        rows,
        request.hidden_size,
        request.intermediate_size,
    )) return null;
    if (request.post_gate_rms_norm_slot != null or
        !kernels_mod.generatedQ4_0PairQ8E2BShapeEligible(
            rows,
            request.hidden_size,
            request.intermediate_size,
            @intFromEnum(request.activation),
        ) or
        !kernels_mod.generatedQ4_0DownQ8E2BShapeEligible(
            rows,
            request.intermediate_size,
            request.hidden_size,
        ))
    {
        return null;
    }

    const gate_slot = self.decoder_runtime_linear_slots.getPtr(request.gate_linear_slot) orelse return null;
    const up_slot = self.decoder_runtime_linear_slots.getPtr(request.up_linear_slot) orelse return null;
    const down_slot = self.decoder_runtime_linear_slots.getPtr(request.down_linear_slot) orelse return null;
    if (gate_slot.in_dim != request.hidden_size or up_slot.in_dim != request.hidden_size or
        gate_slot.out_dim != request.intermediate_size or up_slot.out_dim != request.intermediate_size or
        down_slot.in_dim != request.intermediate_size or down_slot.out_dim != request.hidden_size)
    {
        return error.UnexpectedOutputShape;
    }
    if (gate_slot.bias != null or up_slot.bias != null or down_slot.bias != null) return null;
    if (!isKnownQuant(&gate_slot.weight, .Q4_0) or !isKnownQuant(&up_slot.weight, .Q4_0) or
        !isKnownQuant(&down_slot.weight, .Q4_0)) return null;
    if (weightBf16MirrorForRows(self, &gate_slot.weight, rows) != null or
        weightBf16MirrorForRows(self, &up_slot.weight, rows) != null or
        weightBf16MirrorForRows(self, &down_slot.weight, rows) != null) return null;

    const input_tensor = tensorFromCt(request.input);
    try ensureF32(input_tensor);
    if (input_tensor.elem_count != rows * request.hidden_size) return error.InvalidShape;

    const hidden_q8_bytes = try checkedMul(try checkedMul(rows, request.hidden_size / 32), 36);
    const activated_q8_bytes = try checkedMul(try checkedMul(rows, request.intermediate_size / 32), 36);
    const output_count = try checkedMul(rows, request.hidden_size);
    const shape = try allocShape2(self.allocator, rows, request.hidden_size);
    var shape_transferred = false;
    defer if (!shape_transferred) self.allocator.free(shape);
    var projected = allocDeviceBuffer(self, output_count * @sizeOf(f32)) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => return null,
        else => return err,
    };
    var projected_transferred = false;
    defer if (!projected_transferred) releaseDeviceBuffer(self, &projected);
    var hidden_storage = allocDeviceBuffer(self, hidden_q8_bytes) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => return null,
        else => return err,
    };
    defer releaseDeviceBuffer(self, &hidden_storage);
    var activated_storage = allocDeviceBuffer(self, activated_q8_bytes) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => return null,
        else => return err,
    };
    defer releaseDeviceBuffer(self, &activated_storage);
    const hidden_q8 = kernels_mod.GgmlQ8_1Buffer.init(hidden_storage);
    const activated_q8 = kernels_mod.GgmlQ8_1Buffer.init(activated_storage);

    self.kernels.launchQuantizeF32GgmlQ8_1Rows(
        &self.ctx,
        hidden_q8,
        input_tensor.buffer,
        rows,
        request.hidden_size,
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            self.stats.q4_0_ggml_q8_1_quantize_fallbacks += 1;
            self.stats.q4_0_ggml_q8_1_e2b_ffn_fallbacks += 1;
            return null;
        },
        else => return err,
    };

    var pair_profile_scope = beginDecodeProfile(self, .ffn_gate_up, rows);
    self.kernels.launchLinearQ4_0GeneratedPairGgmlQ8_1E2B(
        &self.ctx,
        activated_q8,
        hidden_q8,
        gate_slot.weight.buffer,
        up_slot.weight.buffer,
        rows,
        request.hidden_size,
        request.intermediate_size,
        @intFromEnum(request.activation),
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            if (pair_profile_scope) |*scope| scope.end();
            self.stats.q4_0_ggml_q8_1_pair_fallbacks += 1;
            self.stats.q4_0_ggml_q8_1_e2b_ffn_fallbacks += 1;
            return null;
        },
        else => {
            if (pair_profile_scope) |*scope| scope.end();
            return err;
        },
    };
    if (pair_profile_scope) |*scope| scope.end();
    noteQ4FfnRoute(
        self,
        .ffn_pair,
        rows,
        request.hidden_size,
        request.intermediate_size,
        .pair,
        .activation_ggml_q8_1,
        .generated,
        generatedE2BPairGgmlQ8_1KernelId(request.intermediate_size),
        rows * (request.intermediate_size / 32),
        384,
    );

    var down_profile_scope = beginDecodeProfile(self, .ffn_gated_down, rows);
    self.kernels.launchLinearQ4_0GeneratedDownGgmlQ8_1E2B(
        &self.ctx,
        projected,
        activated_q8,
        down_slot.weight.buffer,
        rows,
        request.intermediate_size,
        request.hidden_size,
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            if (down_profile_scope) |*scope| scope.end();
            self.stats.q4_0_ggml_q8_1_down_fallbacks += 1;
            self.stats.q4_0_ggml_q8_1_e2b_ffn_fallbacks += 1;
            return null;
        },
        else => {
            if (down_profile_scope) |*scope| scope.end();
            return err;
        },
    };
    if (down_profile_scope) |*scope| scope.end();
    noteQ4FfnRoute(
        self,
        .ffn_down,
        rows,
        request.intermediate_size,
        request.hidden_size,
        .single,
        .none,
        .generated,
        generatedE2BDownGgmlQ8_1KernelId(request.intermediate_size),
        rows * request.hidden_size,
        128,
    );

    self.stats.q4_0_ggml_q8_1_e2b_ffn_hits += 1;
    self.stats.launch_linear += 2;
    self.stats.linear_pair_fused_q4_0 += 1;
    self.stats.linear_pair_fused_q4_0_activation += 1;
    self.stats.decoder_runtime_linear_apply_hits += 3;
    self.stats.decoder_runtime_linear_pair_apply_hits += 1;
    self.stats.gated_down_fused_q4_0 += 1;
    self.stats.gated_down_fused_q4_0_precompute += 1;
    const result = try createTensor(self, projected, shape, output_count);
    projected_transferred = true;
    shape_transferred = true;
    return result;
}

fn tryRunQ4_0GeneratedExactE2BFfn(
    ctx: *anyopaque,
    request: *const ops.RunGatedFfnResidualRequest,
    rows: usize,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!self.generated_q4_0_gates.exact_ffn_candidates) return null;
    if (request.post_gate_rms_norm_slot != null or
        !kernels_mod.generatedQ4_0ExactFfnPairE2BShapeEligible(rows, request.hidden_size, request.intermediate_size) or
        !kernels_mod.generatedQ4_0ExactFfnDownE2BShapeEligible(rows, request.intermediate_size, request.hidden_size))
    {
        return null;
    }

    const gate_slot = self.decoder_runtime_linear_slots.getPtr(request.gate_linear_slot) orelse return null;
    const up_slot = self.decoder_runtime_linear_slots.getPtr(request.up_linear_slot) orelse return null;
    const down_slot = self.decoder_runtime_linear_slots.getPtr(request.down_linear_slot) orelse return null;
    if (gate_slot.in_dim != request.hidden_size or up_slot.in_dim != request.hidden_size or
        gate_slot.out_dim != request.intermediate_size or up_slot.out_dim != request.intermediate_size or
        down_slot.in_dim != request.intermediate_size or down_slot.out_dim != request.hidden_size)
    {
        return error.UnexpectedOutputShape;
    }
    if (gate_slot.bias != null or up_slot.bias != null or down_slot.bias != null) return null;
    if (!isKnownQuant(&gate_slot.weight, .Q4_0) or !isKnownQuant(&up_slot.weight, .Q4_0) or !isKnownQuant(&down_slot.weight, .Q4_0)) return null;
    if (weightBf16MirrorForRows(self, &gate_slot.weight, rows) != null or
        weightBf16MirrorForRows(self, &up_slot.weight, rows) != null or
        weightBf16MirrorForRows(self, &down_slot.weight, rows) != null)
    {
        return null;
    }
    const input_tensor = tensorFromCt(request.input);
    try ensureF32(input_tensor);
    if (input_tensor.elem_count != rows * request.hidden_size) return error.InvalidShape;

    const activated_count = try checkedMul(rows, request.intermediate_size);
    const output_count = try checkedMul(rows, request.hidden_size);
    const shape = try allocShape2(self.allocator, rows, request.hidden_size);
    var shape_owned = false;
    errdefer if (!shape_owned) self.allocator.free(shape);
    var projected_device = allocDeviceBuffer(self, output_count * @sizeOf(f32)) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => return null,
        else => return err,
    };
    var projected_owned = false;
    errdefer if (!projected_owned) releaseDeviceBuffer(self, &projected_device);
    var activated_device = allocDeviceBuffer(self, activated_count * @sizeOf(f32)) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => return null,
        else => return err,
    };
    defer releaseDeviceBuffer(self, &activated_device);

    var pair_profile_scope = beginDecodeProfile(self, .ffn_gate_up, rows);
    self.kernels.launchLinearQ4_0GeneratedExactFfnPairF32(
        &self.ctx,
        activated_device,
        input_tensor.buffer,
        gate_slot.weight.buffer,
        up_slot.weight.buffer,
        rows,
        request.hidden_size,
        request.intermediate_size,
        @intFromEnum(request.activation),
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            if (pair_profile_scope) |*scope| scope.end();
            self.stats.q4_0_generated_e2b_exact_pair_f32_fallbacks += 1;
            return null;
        },
        else => {
            if (pair_profile_scope) |*scope| scope.end();
            return err;
        },
    };
    if (pair_profile_scope) |*scope| scope.end();
    self.stats.q4_0_generated_e2b_exact_pair_f32_hits += 1;
    noteQ4FfnRoute(
        self,
        .ffn_pair,
        rows,
        request.hidden_size,
        request.intermediate_size,
        .pair,
        .activation_f32,
        .generated,
        generatedExactE2BPairKernelId(request.intermediate_size),
        (request.intermediate_size + 3) / 4,
        128,
    );

    var down_profile_scope = beginDecodeProfile(self, .ffn_gated_down, rows);
    self.kernels.launchLinearQ4_0GeneratedExactFfnDownF32(
        &self.ctx,
        projected_device,
        activated_device,
        down_slot.weight.buffer,
        rows,
        request.intermediate_size,
        request.hidden_size,
    ) catch |err| switch (err) {
        error.CudaKernelUnavailable, error.InvalidCudaState => {
            if (down_profile_scope) |*scope| scope.end();
            self.stats.q4_0_generated_e2b_exact_down_f32_fallbacks += 1;
            return null;
        },
        else => {
            if (down_profile_scope) |*scope| scope.end();
            return err;
        },
    };
    if (down_profile_scope) |*scope| scope.end();
    self.stats.q4_0_generated_e2b_exact_down_f32_hits += 1;
    noteQ4FfnRoute(
        self,
        .ffn_down,
        rows,
        request.intermediate_size,
        request.hidden_size,
        .single,
        .none,
        .generated,
        generatedExactE2BDownKernelId(request.intermediate_size),
        (request.hidden_size + 3) / 4,
        256,
    );

    self.stats.launch_linear += 2;
    self.stats.linear_pair_fused_q4_0 += 1;
    self.stats.linear_pair_fused_q4_0_activation += 1;
    self.stats.linear_pair_fused_q4_0_tile4 += 1;
    self.stats.decoder_runtime_linear_apply_hits += 3;
    self.stats.decoder_runtime_linear_pair_apply_hits += 1;
    self.stats.gated_down_fused_q4_0 += 1;
    self.stats.gated_down_fused_q4_0_precompute += 1;
    const result = try createTensor(self, projected_device, shape, output_count);
    shape_owned = true;
    projected_owned = true;
    return result;
}

fn tryRunQ4_0GateUpActivationQ8_1Precompute(
    ctx: *anyopaque,
    request: *const ops.RunGatedFfnResidualRequest,
    rows: usize,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (cudaQ4_0GateUpActivationQ8_1PrecomputeDisabled()) return null;
    if (rows == 0 or request.post_gate_rms_norm_slot != null) return null;
    if (request.hidden_size == 0 or request.intermediate_size == 0 or
        request.hidden_size % 32 != 0 or request.intermediate_size % 32 != 0)
    {
        return null;
    }

    const generated_e2b_pair_only = generatedQ4_0E2BFfnPairOnlyEligible(
        self.generated_q4_0_gates.e2b_pair_only,
        rows,
        request.hidden_size,
        request.intermediate_size,
        request.activation,
    );
    // Pair-only is deliberately distinct from the catalog's coupled pair/down
    // candidate. The generated pair produces the same Q8_1 bytes as the
    // existing F32-pair + quantize boundary; retaining the handwritten down
    // projection therefore avoids changing its reduction order.
    const generated_catalog_ffn_resolution: GeneratedQ4_0CatalogFfnResolution = if (generated_e2b_pair_only)
        .{}
    else
        generatedQ4_0CatalogFfnRoute(
            self.generated_q4_0_gates.catalog_ffn_candidates,
            self.generated_q4_0_gates.pair_q8 and runtimeJitShapeAllowsForCompute(
                self,
                .pair_q8,
                rows,
                request.hidden_size,
                request.intermediate_size,
            ),
            self.generated_q4_0_gates.down_q8 and runtimeJitShapeAllowsForCompute(
                self,
                .down_q8,
                rows,
                request.intermediate_size,
                request.hidden_size,
            ),
            rows,
            request.hidden_size,
            request.intermediate_size,
            request.activation,
            self.ctx.info.compute_major,
            self.ctx.info.compute_minor,
        );
    noteGeneratedQ4_0CatalogFfnResolution(&self.stats, generated_catalog_ffn_resolution);
    const generated_catalog_ffn = generated_catalog_ffn_resolution.route;
    if (!generated_e2b_pair_only and generated_catalog_ffn == null and !cudaQ4_0GateUpActivationQ8_1PrecomputeEnabled()) return null;

    const gate_slot = self.decoder_runtime_linear_slots.getPtr(request.gate_linear_slot) orelse return null;
    const up_slot = self.decoder_runtime_linear_slots.getPtr(request.up_linear_slot) orelse return null;
    const down_slot = self.decoder_runtime_linear_slots.getPtr(request.down_linear_slot) orelse return null;
    if (gate_slot.in_dim != request.hidden_size or up_slot.in_dim != request.hidden_size or
        gate_slot.out_dim != request.intermediate_size or up_slot.out_dim != request.intermediate_size or
        down_slot.in_dim != request.intermediate_size or down_slot.out_dim != request.hidden_size)
    {
        return error.UnexpectedOutputShape;
    }
    if (gate_slot.bias != null or up_slot.bias != null or down_slot.bias != null) return null;
    const input_tensor = tensorFromCt(request.input);
    try ensureF32(input_tensor);
    if (input_tensor.elem_count != rows * request.hidden_size) return error.InvalidShape;
    if (!isKnownQuant(&gate_slot.weight, .Q4_0) or !isKnownQuant(&up_slot.weight, .Q4_0) or !isKnownQuant(&down_slot.weight, .Q4_0)) return null;
    // Hybrid residency: prefill-shaped FFN runs go through the BF16 mirror
    // pair+linear path instead of the Q8_1 fused kernels.
    if (weightBf16MirrorForRows(self, &gate_slot.weight, rows) != null and
        weightBf16MirrorForRows(self, &up_slot.weight, rows) != null and
        weightBf16MirrorForRows(self, &down_slot.weight, rows) != null) return null;

    const hidden_q8_blocks = request.hidden_size / 32;
    const hidden_q8_bytes = try checkedMul(try checkedMul(rows, hidden_q8_blocks), 36);
    const activated_q8_blocks = request.intermediate_size / 32;
    const activated_q8_bytes = try checkedMul(try checkedMul(rows, activated_q8_blocks), 36);
    const out_count = try checkedMul(rows, request.hidden_size);
    const shape = try allocShape2(self.allocator, rows, request.hidden_size);
    var shape_owned = false;
    errdefer if (!shape_owned) self.allocator.free(shape);
    var projected_device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var projected_owned = false;
    errdefer if (!projected_owned) projected_device.free(&self.ctx);

    var q8_hidden = allocDeviceBuffer(self, hidden_q8_bytes) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => {
            releaseDeviceBuffer(self, &projected_device);
            projected_owned = true;
            self.allocator.free(shape);
            shape_owned = true;
            return null;
        },
        else => return err,
    };
    defer releaseDeviceBuffer(self, &q8_hidden);

    var q8_activated = allocDeviceBuffer(self, activated_q8_bytes) catch |err| switch (err) {
        error.CudaGraphCaptureUnsafeTempAlloc => {
            releaseDeviceBuffer(self, &projected_device);
            projected_owned = true;
            self.allocator.free(shape);
            shape_owned = true;
            return null;
        },
        else => return err,
    };
    defer releaseDeviceBuffer(self, &q8_activated);

    {
        self.kernels.launchQuantizeF32Q8_1Rows(&self.ctx, q8_hidden, input_tensor.buffer, rows, request.hidden_size) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                projected_device.free(&self.ctx);
                projected_owned = true;
                self.allocator.free(shape);
                shape_owned = true;
                return null;
            },
            else => return err,
        };
    }
    const pair_prefill_variant = self.kernels.q4_0PairActivationQ8_1Tile32W5E4BPrefillRowsVariant(rows);
    var pair_profile_scope = beginPrefillProfile(self, .q4_pair, rows);
    const used_generated_pair_q8 = blk: {
        if (generated_e2b_pair_only) {
            self.kernels.launchLinearQ4_0GeneratedPairQ8E2B(
                &self.ctx,
                q8_activated,
                q8_hidden,
                gate_slot.weight.buffer,
                up_slot.weight.buffer,
                rows,
                request.hidden_size,
                request.intermediate_size,
                @intFromEnum(request.activation),
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => {
                    if (pair_profile_scope) |*scope| scope.end();
                    self.stats.q4_0_generated_e2b_pair_only_fallbacks += 1;
                    projected_device.free(&self.ctx);
                    projected_owned = true;
                    self.allocator.free(shape);
                    shape_owned = true;
                    return null;
                },
                else => {
                    if (pair_profile_scope) |*scope| scope.end();
                    return err;
                },
            };
            self.stats.q4_0_generated_e2b_pair_only_hits += 1;
            noteQ4FfnRoute(
                self,
                .ffn_pair,
                rows,
                request.hidden_size,
                request.intermediate_size,
                .pair,
                .activation_q8_zp,
                .generated,
                generatedE2BPairQ8ZpKernelId(request.intermediate_size),
                rows * (request.intermediate_size / 32),
                384,
            );
            break :blk true;
        }
        if (generated_catalog_ffn) |route| {
            self.kernels.launchLinearQ4_0GeneratedPairQ8Catalog(
                &self.ctx,
                route.pair_kernel_id,
                q8_activated,
                q8_hidden,
                gate_slot.weight.buffer,
                up_slot.weight.buffer,
                rows,
                request.hidden_size,
                request.intermediate_size,
                @intFromEnum(request.activation),
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => {
                    noteGeneratedQ4_0CatalogFfnResult(&self.stats, .pair_q8, false);
                    break :blk false;
                },
                else => return err,
            };
            noteGeneratedQ4_0CatalogFfnResult(&self.stats, .pair_q8, true);
            noteQ4FfnRoute(
                self,
                .ffn_pair,
                rows,
                request.hidden_size,
                request.intermediate_size,
                .pair,
                .activation_q8_zp,
                .generated,
                route.pair_kernel_id,
                rows * (request.intermediate_size / 32),
                if (request.hidden_size == 1536) 384 else 640,
            );
            break :blk true;
        }
        if (rows != 1 or !self.generated_q4_0_gates.pair_q8 or
            !runtimeJitShapeAllowsForCompute(
                self,
                .pair_q8,
                rows,
                request.hidden_size,
                request.intermediate_size,
            )) break :blk false;
        self.kernels.launchLinearQ4_0GeneratedPairQ8(
            &self.ctx,
            q8_activated,
            q8_hidden,
            gate_slot.weight.buffer,
            up_slot.weight.buffer,
            rows,
            request.hidden_size,
            request.intermediate_size,
            @intFromEnum(request.activation),
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.q4_0_generated_pair_q8_fallbacks += 1;
                break :blk false;
            },
            else => return err,
        };
        self.stats.q4_0_generated_pair_q8_hits += 1;
        noteQ4FfnRoute(
            self,
            .ffn_pair,
            rows,
            request.hidden_size,
            request.intermediate_size,
            .pair,
            .activation_q8_zp,
            .generated,
            "antfly_q4_0_pair_activation_q8_1_mmv_v1",
            rows * (request.intermediate_size / 32),
            640,
        );
        break :blk true;
    };
    if (!used_generated_pair_q8) {
        self.kernels.launchLinearQ4_0PairActivationQ8_1Tile32W5E4BFfnQ8_1(
            &self.ctx,
            q8_activated,
            q8_hidden,
            gate_slot.weight.buffer,
            up_slot.weight.buffer,
            rows,
            request.hidden_size,
            request.intermediate_size,
            @intFromEnum(request.activation),
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                if (pair_profile_scope) |*scope| scope.end();
                projected_device.free(&self.ctx);
                projected_owned = true;
                self.allocator.free(shape);
                shape_owned = true;
                return null;
            },
            else => {
                if (pair_profile_scope) |*scope| scope.end();
                return err;
            },
        };
        noteQ4FfnRoute(
            self,
            .ffn_pair,
            rows,
            request.hidden_size,
            request.intermediate_size,
            .pair,
            .activation_q8_zp,
            .handwritten,
            "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn",
            rows * (request.intermediate_size / 32),
            640,
        );
    }
    if (pair_profile_scope) |*scope| scope.end();
    const down_prefill_variant = self.kernels.q4_0Q8_1Tile4W8PrefillRowsVariant(rows, request.intermediate_size, request.hidden_size);
    var down_profile_scope = beginPrefillProfile(self, .q4_gated_down, rows);
    const used_generated_down_q8 = blk: {
        // The pair-only candidate's numerical contract ends at its exact Q8_1
        // boundary. Always preserve the existing handwritten down reduction.
        if (generated_e2b_pair_only) break :blk false;
        if (generated_catalog_ffn) |route| {
            self.kernels.launchLinearQ4_0GeneratedDownQ8Catalog(
                &self.ctx,
                route.down_kernel_id,
                projected_device,
                q8_activated,
                down_slot.weight.buffer,
                rows,
                request.intermediate_size,
                request.hidden_size,
            ) catch |err| switch (err) {
                error.CudaKernelUnavailable, error.InvalidCudaState => {
                    noteGeneratedQ4_0CatalogFfnResult(&self.stats, .down_q8, false);
                    break :blk false;
                },
                else => return err,
            };
            noteGeneratedQ4_0CatalogFfnResult(&self.stats, .down_q8, true);
            noteQ4FfnRoute(
                self,
                .ffn_down,
                rows,
                request.intermediate_size,
                request.hidden_size,
                .single,
                .none,
                .generated,
                route.down_kernel_id,
                rows * ((request.hidden_size + 3) / 4),
                generatedE2BDownQ8BlockSize(request.intermediate_size),
            );
            break :blk true;
        }
        if (rows != 1 or !self.generated_q4_0_gates.down_q8 or
            !runtimeJitShapeAllowsForCompute(
                self,
                .down_q8,
                rows,
                request.intermediate_size,
                request.hidden_size,
            )) break :blk false;
        self.kernels.launchLinearQ4_0GeneratedDownQ8(
            &self.ctx,
            projected_device,
            q8_activated,
            down_slot.weight.buffer,
            rows,
            request.intermediate_size,
            request.hidden_size,
        ) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                self.stats.q4_0_generated_down_q8_fallbacks += 1;
                break :blk false;
            },
            else => return err,
        };
        self.stats.q4_0_generated_down_q8_hits += 1;
        noteQ4FfnRoute(
            self,
            .ffn_down,
            rows,
            request.intermediate_size,
            request.hidden_size,
            .single,
            .none,
            .generated,
            "antfly_q4_0_down_q8_1_mmv_v1",
            rows * ((request.hidden_size + 3) / 4),
            256,
        );
        break :blk true;
    };
    if (!used_generated_down_q8) {
        self.kernels.launchLinearQ4_0Q8_1Tile4W8F32(&self.ctx, projected_device, q8_activated, down_slot.weight.buffer, rows, request.intermediate_size, request.hidden_size) catch |err| switch (err) {
            error.CudaKernelUnavailable, error.InvalidCudaState => {
                if (down_profile_scope) |*scope| scope.end();
                projected_device.free(&self.ctx);
                projected_owned = true;
                self.allocator.free(shape);
                shape_owned = true;
                return null;
            },
            else => {
                if (down_profile_scope) |*scope| scope.end();
                return err;
            },
        };
        noteQ4FfnRoute(
            self,
            .ffn_down,
            rows,
            request.intermediate_size,
            request.hidden_size,
            .single,
            .none,
            .handwritten,
            "termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down",
            rows * ((request.hidden_size + 3) / 4),
            256,
        );
    }
    if (down_profile_scope) |*scope| scope.end();

    self.stats.launch_linear += 2;
    self.stats.linear_pair_fused_q4_0 += 1;
    self.stats.linear_pair_fused_q4_0_activation += 1;
    self.stats.linear_pair_fused_q4_0_tile4 += 1;
    self.stats.decoder_runtime_linear_apply_hits += 3;
    self.stats.decoder_runtime_linear_pair_apply_hits += 1;
    self.stats.gated_down_fused_q4_0 += 1;
    self.stats.gated_down_fused_q4_0_precompute += 1;
    noteQ4_0Q8_1PrefillPair(&self.stats, pair_prefill_variant);
    noteQ4_0Q8_1PrefillGatedDown(&self.stats, down_prefill_variant);
    const result = try createTensor(self, projected_device, shape, out_count);
    shape_owned = true;
    projected_owned = true;
    return result;
}

fn runGatedFfnResidualOp(ctx: *anyopaque, request: *const ops.RunGatedFfnResidualRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.stats.decoder_runtime_gated_ffn_attempts += 1;

    const rows = decoderRuntimeInputRows(request.input, request.hidden_size) catch {
        self.stats.decoder_runtime_gated_ffn_misses += 1;
        return null;
    };
    const residual_tensor = tensorFromCt(request.residual);
    ensureF32(residual_tensor) catch {
        self.stats.decoder_runtime_gated_ffn_misses += 1;
        return null;
    };
    if (residual_tensor.elem_count != rows * request.hidden_size) {
        self.stats.decoder_runtime_gated_ffn_misses += 1;
        return null;
    }

    var current: CT = undefined;
    var current_is_down_projection = false;

    if (try tryRunQ4_0Sm89GgmlQ8_1E2BFfn(ctx, request, rows)) |projected| {
        current = projected;
        current_is_down_projection = true;
    }

    if (!current_is_down_projection) {
        if (try tryRunQ4_0GeneratedExactE2BFfn(ctx, request, rows)) |projected| {
            current = projected;
            current_is_down_projection = true;
        }
    }

    if (!current_is_down_projection) {
        if (try tryRunQ4_0GateUpActivationQ8_1Precompute(ctx, request, rows)) |projected| {
            current = projected;
            current_is_down_projection = true;
        }
    }

    if (request.post_gate_rms_norm_slot == null and cudaQ4_0GateUpActivationPrecomputeEnabled(self)) pair_activation_blk: {
        if (current_is_down_projection) break :pair_activation_blk;
        const gate_slot = self.decoder_runtime_linear_slots.getPtr(request.gate_linear_slot) orelse break :pair_activation_blk;
        const up_slot = self.decoder_runtime_linear_slots.getPtr(request.up_linear_slot) orelse break :pair_activation_blk;
        const down_slot = self.decoder_runtime_linear_slots.getPtr(request.down_linear_slot) orelse break :pair_activation_blk;
        if (gate_slot.in_dim != request.hidden_size or up_slot.in_dim != request.hidden_size or
            gate_slot.out_dim != request.intermediate_size or up_slot.out_dim != request.intermediate_size or
            down_slot.in_dim != request.intermediate_size or down_slot.out_dim != request.hidden_size)
        {
            return error.UnexpectedOutputShape;
        }
        if (gate_slot.bias != null or up_slot.bias != null or down_slot.bias != null) break :pair_activation_blk;

        var gate_profile_scope = beginDecodeProfile(self, .ffn_gate_up, rows);
        const activated = (linearNoBiasPairActivationQ4_0(
            ctx,
            request.input,
            &gate_slot.weight,
            &up_slot.weight,
            rows,
            request.hidden_size,
            request.intermediate_size,
            request.activation,
        ) catch |err| {
            if (gate_profile_scope) |*scope| scope.end();
            return err;
        }) orelse {
            if (gate_profile_scope) |*scope| scope.end();
            break :pair_activation_blk;
        };
        if (gate_profile_scope) |*scope| scope.end();
        defer freeTensor(ctx, activated);
        self.stats.decoder_runtime_linear_apply_hits += 2;
        self.stats.decoder_runtime_linear_pair_apply_hits += 1;

        var down_profile_scope = beginDecodeProfile(self, .ffn_gated_down, rows);
        const projected = linearNoBiasForQ4Route(ctx, .ffn_down, activated, &down_slot.weight, rows, request.intermediate_size, request.hidden_size) catch |err| {
            if (down_profile_scope) |*scope| scope.end();
            return err;
        };
        if (down_profile_scope) |*scope| scope.end();
        self.stats.decoder_runtime_linear_apply_hits += 1;
        self.stats.gated_down_fused_q4_0 += 1;
        self.stats.gated_down_fused_q4_0_precompute += 1;
        current = projected;
        current_is_down_projection = true;
    }

    if (!current_is_down_projection and request.post_gate_rms_norm_slot == null and
        self.tuned_route_gates.fused_gate_up_bf16 and rows > 1)
    bf16_fused_blk: {
        const gate_slot = self.decoder_runtime_linear_slots.getPtr(request.gate_linear_slot) orelse break :bf16_fused_blk;
        const up_slot = self.decoder_runtime_linear_slots.getPtr(request.up_linear_slot) orelse break :bf16_fused_blk;
        const down_slot = self.decoder_runtime_linear_slots.getPtr(request.down_linear_slot) orelse break :bf16_fused_blk;
        if (gate_slot.in_dim != request.hidden_size or up_slot.in_dim != request.hidden_size or
            gate_slot.out_dim != request.intermediate_size or up_slot.out_dim != request.intermediate_size or
            down_slot.in_dim != request.intermediate_size or down_slot.out_dim != request.hidden_size)
        {
            return error.UnexpectedOutputShape;
        }
        if (gate_slot.bias != null or up_slot.bias != null or down_slot.bias != null) break :bf16_fused_blk;

        var gate_profile_scope = beginDecodeProfile(self, .ffn_gate_up, rows);
        const activated = (tryFusedGateUpBf16(
            self,
            request.input,
            &gate_slot.weight,
            &up_slot.weight,
            rows,
            request.hidden_size,
            request.intermediate_size,
            request.activation,
        ) catch |err| {
            if (gate_profile_scope) |*scope| scope.end();
            return err;
        }) orelse {
            if (gate_profile_scope) |*scope| scope.end();
            break :bf16_fused_blk;
        };
        if (gate_profile_scope) |*scope| scope.end();
        defer freeTensor(ctx, activated);
        self.stats.decoder_runtime_linear_apply_hits += 2;
        self.stats.decoder_runtime_linear_pair_apply_hits += 1;

        var down_profile_scope = beginDecodeProfile(self, .ffn_gated_down, rows);
        const projected = linearNoBiasForQ4Route(ctx, .ffn_down, activated, &down_slot.weight, rows, request.intermediate_size, request.hidden_size) catch |err| {
            if (down_profile_scope) |*scope| scope.end();
            return err;
        };
        if (down_profile_scope) |*scope| scope.end();
        self.stats.decoder_runtime_linear_apply_hits += 1;
        current = projected;
        current_is_down_projection = true;
    }

    if (!current_is_down_projection) {
        const gate_up = (try decoderRuntimeApplyLinearPairOp(ctx, &.{
            .slot_a = request.gate_linear_slot,
            .slot_b = request.up_linear_slot,
            .input = request.input,
            .in_dim = request.hidden_size,
            .out_dim = request.intermediate_size,
        })) orelse {
            self.stats.decoder_runtime_gated_ffn_misses += 1;
            return null;
        };
        defer freeTensor(ctx, gate_up.first);
        defer freeTensor(ctx, gate_up.second);

        if (request.post_gate_rms_norm_slot == null) fused_down_blk: {
            const down_slot = self.decoder_runtime_linear_slots.getPtr(request.down_linear_slot) orelse break :fused_down_blk;
            if (down_slot.in_dim != request.intermediate_size or down_slot.out_dim != request.hidden_size) return error.UnexpectedOutputShape;
            if (down_slot.bias != null) break :fused_down_blk;
            var profile_scope = beginDecodeProfile(self, .ffn_gated_down, rows);
            defer if (profile_scope) |*scope| scope.end();
            if (try linearNoBiasGatedDown(
                ctx,
                gate_up.first,
                gate_up.second,
                &down_slot.weight,
                rows,
                request.intermediate_size,
                request.hidden_size,
                request.activation,
            )) |projected| {
                self.stats.decoder_runtime_linear_apply_hits += 1;
                current = projected;
                current_is_down_projection = true;
            }
        }

        if (!current_is_down_projection) {
            current = (try activationMultiply(ctx, gate_up.first, gate_up.second, request.activation)) orelse {
                self.stats.decoder_runtime_gated_ffn_misses += 1;
                return null;
            };

            if (request.post_gate_rms_norm_slot) |slot_id| {
                const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
                    .slot = slot_id,
                    .input = current,
                    .hidden_size = request.intermediate_size,
                    .eps = request.eps,
                })) orelse {
                    freeTensor(ctx, current);
                    self.stats.decoder_runtime_gated_ffn_misses += 1;
                    return null;
                };
                freeTensor(ctx, current);
                current = normed;
            }

            const projected = projected_blk: {
                const previous_route_op = self.q4_route_op_override;
                self.q4_route_op_override = .ffn_down;
                defer self.q4_route_op_override = previous_route_op;
                break :projected_blk (try decoderRuntimeApplyLinearOp(ctx, &.{
                    .slot = request.down_linear_slot,
                    .input = current,
                    .in_dim = request.intermediate_size,
                    .out_dim = request.hidden_size,
                })) orelse {
                    freeTensor(ctx, current);
                    self.stats.decoder_runtime_gated_ffn_misses += 1;
                    return null;
                };
            };
            freeTensor(ctx, current);
            current = projected;
        }
    }
    errdefer freeTensor(ctx, current);

    const output_scale = request.output_scale;
    if (request.post_down_rms_norm_slot) |slot_id| {
        var profile_scope = beginDecodeProfile(self, .ffn_post_norm, rows);
        defer if (profile_scope) |*scope| scope.end();
        if (self.decoder_runtime_rms_norm_slots.getPtr(slot_id)) |slot| {
            if (output_scale) |scale| {
                if (try rmsNormAddOutputScaleTensor(ctx, current, &slot.weight, request.residual, scale, request.hidden_size, request.eps)) |fused| {
                    freeTensor(ctx, current);
                    self.stats.decoder_runtime_gated_ffn_hits += 1;
                    return fused;
                }
                if (try rmsNormAddTensor(ctx, current, &slot.weight, request.residual, request.hidden_size, request.eps)) |unscaled| {
                    defer freeTensor(ctx, unscaled);
                    const scaled = try multiply(ctx, unscaled, scale);
                    freeTensor(ctx, current);
                    self.stats.decoder_runtime_gated_ffn_hits += 1;
                    return scaled;
                }
            } else if (try rmsNormAddTensor(ctx, current, &slot.weight, request.residual, request.hidden_size, request.eps)) |fused| {
                freeTensor(ctx, current);
                self.stats.decoder_runtime_gated_ffn_hits += 1;
                return fused;
            }
        }
        const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = slot_id,
            .input = current,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse {
            freeTensor(ctx, current);
            self.stats.decoder_runtime_gated_ffn_misses += 1;
            return null;
        };
        freeTensor(ctx, current);
        current = normed;
    }

    const added = try add(ctx, current, request.residual);
    freeTensor(ctx, current);
    const result = if (output_scale) |scale| scaled: {
        defer freeTensor(ctx, added);
        break :scaled try multiply(ctx, added, scale);
    } else added;
    self.stats.decoder_runtime_gated_ffn_hits += 1;
    return result;
}

fn decoderRuntimeApplyHeadNormRopeSlot(
    ctx: *anyopaque,
    input: CT,
    slot_id: usize,
    rows: usize,
    total_dim: usize,
    head_dim: usize,
    rope_dim: usize,
    eps: f32,
    theta: f32,
    freq_scale: f32,
    position_offset: usize,
    seq_len: usize,
    consecutive_pairs: bool,
    scale: f32,
) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const slot = self.decoder_runtime_rms_norm_slots.getPtr(slot_id) orelse {
        traceCudaGatedBlockDecline("headnorm-slot-unprepared");
        return null;
    };
    if (slot.hidden_size != head_dim) {
        traceCudaGatedBlockDecline("headnorm-slot-shape");
        return null;
    }
    return (try rmsNormHeadsRope(
        ctx,
        input,
        &slot.weight,
        rows,
        total_dim,
        head_dim,
        rope_dim,
        eps,
        theta,
        freq_scale,
        position_offset,
        seq_len,
        consecutive_pairs,
        scale,
    )) orelse {
        traceCudaGatedBlockDecline("headnorm-kernel");
        return null;
    };
}

fn traceCudaGatedBlockDecline(comptime reason: []const u8) void {
    if (platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_TRACE_GATED_BLOCK_DECLINE")) {
        std.debug.print("cuda_gated_block_decline: reason={s}\n", .{reason});
    }
}

fn inferCudaGemmaHeadNormSlotFromQSlot(self: *const CudaCompute, q_slot: usize, comptime kind: enum { q, k }) ?usize {
    const state = self.decoder_runtime_family_state;
    if (!state.prepared or state.configured_layer_count == 0) return null;
    if (q_slot % 7 != 0) return null;
    const layer = q_slot / 7;
    if (layer >= state.configured_layer_count or layer >= @as(usize, @intCast(state.num_hidden_layers))) return null;
    return switch (kind) {
        .q => gemma4_runtime.qHeadNormSlot(state.configured_layer_count, layer),
        .k => gemma4_runtime.kHeadNormSlot(state.configured_layer_count, layer),
    };
}

fn inferCudaGemmaPleSlotFromQSlot(self: *const CudaCompute, q_slot: usize, comptime kind: enum { gate, proj, post_norm }) ?usize {
    const state = self.decoder_runtime_family_state;
    if (!state.prepared or state.configured_layer_count == 0) return null;
    if (q_slot % 7 != 0) return null;
    const layer = q_slot / 7;
    if (layer >= state.configured_layer_count or layer >= @as(usize, @intCast(state.num_hidden_layers))) return null;
    return switch (kind) {
        .gate => gemma4_runtime.pleGateSlot(state.configured_layer_count, layer),
        .proj => gemma4_runtime.pleProjSlot(state.configured_layer_count, layer),
        .post_norm => gemma4_runtime.plePostNormSlot(state.configured_layer_count, layer),
    };
}

fn runGatedDecoderBlockOp(ctx: *anyopaque, request: *const ops.RunGatedDecoderBlockRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_GATED_BLOCK")) {
        traceCudaGatedBlockDecline("disabled");
        return null;
    }
    if (self.debug_cuda_graph_capture_active) {
        traceCudaGatedBlockDecline("graph-capture");
        return null;
    }
    if (request.ffn_layer_norm_slot != null) {
        traceCudaGatedBlockDecline("layer-norm-ffn");
        return null;
    }

    var q_for_attention: CT = undefined;
    var k_for_attention: CT = undefined;
    var v_for_attention: CT = undefined;
    var owns_q_for_attention = false;
    var owns_k_for_attention = false;
    var owns_v_for_attention = false;
    var v_normed_flat: ?CT = null;
    var qkv_first_owner: ?CT = null;
    var qkv_second_owner: ?CT = null;
    var qkv_third_owner: ?CT = null;
    defer if (qkv_third_owner) |tensor| freeTensor(ctx, tensor);
    defer if (qkv_second_owner) |tensor| freeTensor(ctx, tensor);
    defer if (qkv_first_owner) |tensor| freeTensor(ctx, tensor);
    defer if (v_normed_flat) |flat| freeTensor(ctx, flat);
    defer if (owns_v_for_attention) freeTensor(ctx, v_for_attention);
    defer if (owns_k_for_attention) freeTensor(ctx, k_for_attention);
    defer if (owns_q_for_attention) freeTensor(ctx, q_for_attention);

    if (request.attention_input) |attention_input| {
        const q_slot = request.q_linear_slot orelse {
            traceCudaGatedBlockDecline("missing-q-slot");
            return null;
        };
        const k_slot = request.k_linear_slot orelse {
            traceCudaGatedBlockDecline("missing-k-slot");
            return null;
        };
        const v_slot = request.v_linear_slot orelse {
            traceCudaGatedBlockDecline("missing-v-slot");
            return null;
        };
        const q_head_norm_slot = request.q_head_norm_slot orelse inferCudaGemmaHeadNormSlotFromQSlot(self, q_slot, .q) orelse {
            traceCudaGatedBlockDecline("missing-q-head-norm");
            return null;
        };
        const k_head_norm_slot = request.k_head_norm_slot orelse inferCudaGemmaHeadNormSlotFromQSlot(self, q_slot, .k) orelse {
            traceCudaGatedBlockDecline("missing-k-head-norm");
            return null;
        };
        if (request.rope_active_dim == 0) {
            traceCudaGatedBlockDecline("missing-rope");
            return null;
        }

        const rows = decoderRuntimeInputRows(attention_input, request.hidden_size) catch return null;
        if (rows == 0 or rows != request.attention.query_sequence_len) {
            traceCudaGatedBlockDecline("rows-mismatch");
            return null;
        }
        const q_dim = try checkedMul(request.num_heads, request.head_dim);
        const kv_dim = try checkedMul(request.num_kv_heads, request.head_dim);
        const qkv = (try decoderRuntimeApplyLinearQkvOp(ctx, &.{
            .q_slot = q_slot,
            .k_slot = k_slot,
            .v_slot = v_slot,
            .input = attention_input,
            .in_dim = request.hidden_size,
            .q_out_dim = q_dim,
            .kv_out_dim = kv_dim,
        })) orelse {
            traceCudaGatedBlockDecline("qkv");
            return null;
        };
        qkv_first_owner = qkv.first;
        qkv_second_owner = qkv.second;
        qkv_third_owner = qkv.third;

        const position_offset = if (request.attention.total_sequence_len >= request.attention.query_sequence_len)
            request.attention.total_sequence_len - request.attention.query_sequence_len
        else
            request.attention.kv_position_offset;
        const q_scale: f32 = if (request.global_head_dim != 0)
            @sqrt(@as(f32, @floatFromInt(request.head_dim)))
        else
            1.0;
        q_for_attention = (try decoderRuntimeApplyHeadNormRopeSlot(
            ctx,
            qkv.first,
            q_head_norm_slot,
            rows,
            q_dim,
            request.head_dim,
            request.rope_active_dim,
            request.eps,
            request.rope_theta,
            request.rope_freq_scale,
            position_offset,
            rows,
            request.rope_consecutive_pairs,
            q_scale,
        )) orelse {
            traceCudaGatedBlockDecline("q-head-norm-rope");
            return null;
        };
        owns_q_for_attention = true;

        k_for_attention = (try decoderRuntimeApplyHeadNormRopeSlot(
            ctx,
            qkv.second,
            k_head_norm_slot,
            rows,
            kv_dim,
            request.head_dim,
            request.rope_active_dim,
            request.eps,
            request.rope_theta,
            request.rope_freq_scale,
            position_offset,
            rows,
            request.rope_consecutive_pairs,
            1.0,
        )) orelse {
            traceCudaGatedBlockDecline("k-head-norm-rope");
            return null;
        };
        owns_k_for_attention = true;

        if (request.global_head_dim != 0) {
            const reshaped_v = try reshape2dOp(ctx, qkv.third, rows * request.num_kv_heads, request.head_dim);
            defer freeTensor(ctx, reshaped_v);
            const normed_flat = (try rmsNormBare(ctx, reshaped_v, request.head_dim, request.eps)) orelse {
                traceCudaGatedBlockDecline("v-rms-bare");
                return null;
            };
            v_normed_flat = normed_flat;
            v_for_attention = try reshape2dOp(ctx, normed_flat, rows, kv_dim);
            owns_v_for_attention = true;
        } else {
            v_for_attention = qkv.third;
        }
    } else {
        q_for_attention = request.q orelse {
            traceCudaGatedBlockDecline("missing-q");
            return null;
        };
        k_for_attention = request.k orelse {
            traceCudaGatedBlockDecline("missing-k");
            return null;
        };
        v_for_attention = request.v orelse {
            traceCudaGatedBlockDecline("missing-v");
            return null;
        };
    }

    const attention_residual = (try runAttentionResidualOp(ctx, &.{
        .q = q_for_attention,
        .k = k_for_attention,
        .v = v_for_attention,
        .residual = request.residual,
        .attention = request.attention,
        .attention_sink = request.attention.attention_sink,
        .num_heads = request.num_heads,
        .num_kv_heads = request.num_kv_heads,
        .head_dim = request.head_dim,
        .linear_slot = request.attention_linear_slot,
        .pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
        .post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
    })) orelse return null;
    errdefer freeTensor(ctx, attention_residual);

    var ffn_input = attention_residual;
    var owns_ffn_input = false;
    if (request.ffn_rms_norm_slot) |slot_id| {
        const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = slot_id,
            .input = attention_residual,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse {
            freeTensor(ctx, attention_residual);
            return null;
        };
        ffn_input = normed;
        owns_ffn_input = true;
    }
    defer if (owns_ffn_input) freeTensor(ctx, ffn_input);

    const block_output = (try runGatedFfnResidualOp(ctx, &.{
        .gate_linear_slot = request.gate_ffn_linear_slot,
        .up_linear_slot = request.up_ffn_linear_slot,
        .down_linear_slot = request.down_ffn_linear_slot,
        .input = ffn_input,
        .residual = attention_residual,
        .post_gate_rms_norm_slot = request.ffn_post_gate_rms_norm_slot,
        .post_down_rms_norm_slot = request.ffn_post_down_rms_norm_slot,
        .output_scale = if (request.ple == null) request.output_scale else null,
        .hidden_size = request.hidden_size,
        .intermediate_size = request.intermediate_size,
        .eps = request.eps,
        .activation = request.activation,
        .planned_layer_contract = request.planned_layer_contract,
    })) orelse {
        freeTensor(ctx, attention_residual);
        return null;
    };
    freeTensor(ctx, attention_residual);
    var block_output_owned = true;
    defer if (block_output_owned) freeTensor(ctx, block_output);

    if (request.ple) |ple| {
        const q_slot = request.q_linear_slot orelse {
            traceCudaGatedBlockDecline("ple-missing-q-slot");
            return null;
        };
        const ple_dim = request.ple_hidden_size;
        if (ple_dim == 0) {
            traceCudaGatedBlockDecline("ple-missing-dim");
            return null;
        }
        const gate_slot_id = request.ple_gate_linear_slot orelse inferCudaGemmaPleSlotFromQSlot(self, q_slot, .gate) orelse {
            traceCudaGatedBlockDecline("ple-missing-gate-slot");
            return null;
        };
        const proj_slot_id = request.ple_proj_linear_slot orelse inferCudaGemmaPleSlotFromQSlot(self, q_slot, .proj) orelse {
            traceCudaGatedBlockDecline("ple-missing-proj-slot");
            return null;
        };
        const post_norm_slot_id = request.ple_post_norm_slot orelse inferCudaGemmaPleSlotFromQSlot(self, q_slot, .post_norm) orelse {
            traceCudaGatedBlockDecline("ple-missing-post-norm-slot");
            return null;
        };
        const gate_slot = self.decoder_runtime_linear_slots.getPtr(gate_slot_id) orelse {
            traceCudaGatedBlockDecline("ple-gate-slot-unprepared");
            return null;
        };
        const proj_slot = self.decoder_runtime_linear_slots.getPtr(proj_slot_id) orelse {
            traceCudaGatedBlockDecline("ple-proj-slot-unprepared");
            return null;
        };
        const post_norm_slot = self.decoder_runtime_rms_norm_slots.getPtr(post_norm_slot_id) orelse {
            traceCudaGatedBlockDecline("ple-post-norm-unprepared");
            return null;
        };
        if (gate_slot.in_dim != request.hidden_size or gate_slot.out_dim != ple_dim or
            proj_slot.in_dim != ple_dim or proj_slot.out_dim != request.hidden_size or
            post_norm_slot.hidden_size != request.hidden_size or gate_slot.bias != null or proj_slot.bias != null)
        {
            traceCudaGatedBlockDecline("ple-slot-shape");
            return null;
        }
        const rows = decoderRuntimeInputRows(block_output, request.hidden_size) catch {
            traceCudaGatedBlockDecline("ple-rows");
            return null;
        };
        const gated = (try linearNoBiasActivationSliceLastDim(
            ctx,
            block_output,
            &gate_slot.weight,
            ple,
            rows,
            request.hidden_size,
            ple_dim,
            0,
            request.activation,
        )) orelse fallback: {
            const gate_projected = try linearNoBias(ctx, block_output, &gate_slot.weight, rows, request.hidden_size, ple_dim);
            defer freeTensor(ctx, gate_projected);
            break :fallback (try activationMultiply(ctx, gate_projected, ple, request.activation)) orelse {
                traceCudaGatedBlockDecline("ple-gate-fallback");
                return null;
            };
        };
        defer freeTensor(ctx, gated);
        const projected = (try linearNoBias(ctx, gated, &proj_slot.weight, rows, ple_dim, request.hidden_size));
        defer freeTensor(ctx, projected);
        const result = if (request.output_scale) |scale|
            (try rmsNormAddOutputScaleTensor(ctx, projected, &post_norm_slot.weight, block_output, scale, request.hidden_size, request.eps)) orelse {
                traceCudaGatedBlockDecline("ple-post-scale");
                return null;
            }
        else
            (try rmsNormAddTensor(ctx, projected, &post_norm_slot.weight, block_output, request.hidden_size, request.eps)) orelse {
                traceCudaGatedBlockDecline("ple-post");
                return null;
            };
        freeTensor(ctx, block_output);
        block_output_owned = false;
        return result;
    }

    block_output_owned = false;
    return block_output;
}

fn runAttentionOutputResidualOp(ctx: *anyopaque, request: *const ops.RunAttentionOutputResidualRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.stats.decoder_runtime_attention_residual_attempts += 1;

    if (request.rows == 0 or request.attention_input_size == 0 or request.hidden_size == 0) {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    }
    const rows = decoderRuntimeInputRows(request.attention_output, request.attention_input_size) catch {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    };
    if (rows != request.rows) {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    }
    const residual_tensor = tensorFromCt(request.residual);
    ensureF32(residual_tensor) catch {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    };
    if (residual_tensor.elem_count != rows * request.hidden_size) {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    }

    var current = request.attention_output;
    var owns_current = false;
    errdefer if (owns_current) freeTensor(ctx, current);

    if (try decoderRuntimeApplyBlockRmsNorm(ctx, current, request.pre_linear_rms_norm_slot, request.attention_input_size, request.eps)) |normed| {
        if (normed != current) {
            current = normed;
            owns_current = true;
        }
    } else {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    }

    const projected = projected_blk: {
        var profile_scope = beginDecodeProfile(self, .attention_output, rows);
        defer if (profile_scope) |*scope| scope.end();
        break :projected_blk (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.linear_slot,
            .input = current,
            .in_dim = request.attention_input_size,
            .out_dim = request.hidden_size,
        })) orelse {
            self.stats.decoder_runtime_attention_residual_misses += 1;
            return null;
        };
    };
    if (owns_current) {
        freeTensor(ctx, current);
        owns_current = false;
    }
    current = projected;
    owns_current = true;

    var post_profile_scope = beginDecodeProfile(self, .attention_norm_residual, rows);
    defer if (post_profile_scope) |*scope| scope.end();
    if (request.post_linear_rms_norm_slot) |slot_id| {
        if (self.decoder_runtime_rms_norm_slots.getPtr(slot_id)) |slot| {
            if (try rmsNormAddTensor(ctx, current, &slot.weight, request.residual, request.hidden_size, request.eps)) |fused| {
                freeTensor(ctx, current);
                owns_current = false;
                self.stats.decoder_runtime_attention_residual_hits += 1;
                return fused;
            }
        }
        const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = slot_id,
            .input = current,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse {
            self.stats.decoder_runtime_attention_residual_misses += 1;
            return null;
        };
        freeTensor(ctx, current);
        current = normed;
    }

    const result = try add(ctx, current, request.residual);
    freeTensor(ctx, current);
    owns_current = false;
    self.stats.decoder_runtime_attention_residual_hits += 1;
    return result;
}

fn runAttentionResidualOp(ctx: *anyopaque, request: *const ops.RunAttentionResidualRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    self.stats.decoder_runtime_attention_residual_attempts += 1;
    if (request.attention_sink.hasMetadata()) {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    }
    const attention_input_size = request.num_heads * request.head_dim;
    var current = gqa_blk: {
        var profile_scope = beginDecodeProfile(self, .gqa_attention, 1);
        defer if (profile_scope) |*scope| scope.end();
        break :gqa_blk try gqaPagedAttention(
            ctx,
            request.q,
            request.k,
            request.v,
            null,
            request.attention,
            1,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
        );
    };
    errdefer freeTensor(ctx, current);

    if (try decoderRuntimeApplyBlockRmsNorm(ctx, current, request.pre_linear_rms_norm_slot, attention_input_size, request.eps)) |normed| {
        if (normed != current) {
            freeTensor(ctx, current);
            current = normed;
        }
    } else {
        self.stats.decoder_runtime_attention_residual_misses += 1;
        return null;
    }

    const projected = projected_blk: {
        var profile_scope = beginDecodeProfile(self, .attention_output, 1);
        defer if (profile_scope) |*scope| scope.end();
        break :projected_blk (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.linear_slot,
            .input = current,
            .in_dim = attention_input_size,
            .out_dim = request.hidden_size,
        })) orelse {
            freeTensor(ctx, current);
            self.stats.decoder_runtime_attention_residual_misses += 1;
            return null;
        };
    };
    freeTensor(ctx, current);
    current = projected;

    var post_profile_scope = beginDecodeProfile(self, .attention_norm_residual, 1);
    defer if (post_profile_scope) |*scope| scope.end();
    if (request.post_linear_rms_norm_slot) |slot_id| {
        if (self.decoder_runtime_rms_norm_slots.getPtr(slot_id)) |slot| {
            if (try rmsNormAddTensor(ctx, current, &slot.weight, request.residual, request.hidden_size, request.eps)) |fused| {
                freeTensor(ctx, current);
                self.stats.decoder_runtime_attention_residual_hits += 1;
                return fused;
            }
        }
        const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = slot_id,
            .input = current,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse {
            freeTensor(ctx, current);
            self.stats.decoder_runtime_attention_residual_misses += 1;
            return null;
        };
        freeTensor(ctx, current);
        current = normed;
    }

    const result = try add(ctx, current, request.residual);
    freeTensor(ctx, current);
    self.stats.decoder_runtime_attention_residual_hits += 1;
    return result;
}

fn gqaCausalAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
    if (num_heads == num_kv_heads) {
        const self: *CudaCompute = @ptrCast(@alignCast(ctx));
        if (self.kernels.gqa_attention_f32 == null) {
            return causalSelfAttention(ctx, q_ct, k_ct, v_ct, attn_bias_ct, batch, seq_len, num_heads, head_dim);
        }
    }
    return gqaDenseAttention(ctx, q_ct, k_ct, v_ct, attn_bias_ct, null, batch, seq_len, seq_len, num_heads, num_kv_heads, head_dim, 0, 0, 0, seq_len);
}

fn gqaPagedAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, attention: ops.AttentionContext, batch: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
    if (attention.kv_batch != null or attention.kv_cache != null or attention.kv_manager != null or attention.kv_storage != null) {
        return gqaPagedAttentionWithHostKv(ctx, q_ct, k_ct, v_ct, attn_bias_ct, attention, batch, num_heads, num_kv_heads, head_dim);
    }
    if (attention.total_sequence_len < attention.query_sequence_len) return error.InvalidShape;
    const query_position_offset = attention.total_sequence_len - attention.query_sequence_len;
    return gqaDenseAttention(ctx, q_ct, k_ct, v_ct, attn_bias_ct, attention.attn_or_mask, batch, attention.query_sequence_len, attention.kv_sequence_len, num_heads, num_kv_heads, head_dim, query_position_offset, attention.kv_position_offset, attention.sliding_window, attention.total_sequence_len);
}

const vtable = ops.ComputeBackend.VTable{
    .backendKind = &backendKind,
    .deinitBackend = &deinitBackend,
    .freeTensor = &freeTensor,
    .beginRequest = &beginRequest,
    .convertDType = &convertDTypeOp,
    .provisionKvDeviceWriteHook = &provisionKvDeviceWriteHook,
    .getWeight = &getWeight,
    .prefetchWeightHint = &prefetchWeightHint,
    .drainPrefetchBudget = &drainPrefetchBudget,
    .debugProfileCheckpoint = &debugProfileCheckpoint,
    .trainingRuntimeStats = &trainingRuntimeStatsOp,
    .debugCudaGraphCaptureBegin = &debugCudaGraphCaptureBegin,
    .debugCudaGraphPrepareDecodeScalars = &debugCudaGraphPrepareDecodeScalars,
    .debugCudaTraceTensor = &debugCudaTraceTensor,
    .debugCudaGraphRegisterFinalHiddenReplayBoundary = &debugCudaGraphRegisterFinalHiddenReplayBoundary,
    .debugCudaGraphRegisterFinalHiddenReplayInput = &debugCudaGraphRegisterFinalHiddenReplayInput,
    .debugCudaGraphRegisterFinalHiddenReplayAuxInput = &debugCudaGraphRegisterFinalHiddenReplayAuxInput,
    .debugCudaGraphPrepareFinalHiddenReplayInput = &debugCudaGraphPrepareFinalHiddenReplayInput,
    .debugCudaGraphPrepareGreedyTokenReplayInput = &debugCudaGraphPrepareGreedyTokenReplayInput,
    .debugCudaGraphPrepareFinalHiddenReplayAuxInput = &debugCudaGraphPrepareFinalHiddenReplayAuxInput,
    .debugCudaGraphReplayFinalHidden = &debugCudaGraphReplayFinalHidden,
    .debugCudaGraphReplayFinalHiddenDiscard = &debugCudaGraphReplayFinalHiddenDiscard,
    .debugCudaGraphCaptureEnd = &debugCudaGraphCaptureEnd,
    .embeddingLookup = &embeddingLookup,
    .embeddingLookupScaled = &embeddingLookupScaled,
    .embeddingLookupTensor = &embeddingLookupTensor,
    .embeddingLookupTensorScaled = &embeddingLookupTensorScaled,
    .addWeightedEmbeddingTensor = &addWeightedEmbeddingTensor,
    .rmsNormAddWeightedEmbeddingTensor = &rmsNormAddWeightedEmbeddingTensor,
    .takeRows = &takeRows,
    .glinerWordEmbeddings = &glinerWordEmbeddings,
    .glinerLabelGruCombined = &glinerLabelGruCombined,
    .glinerGatherConcatRelu = &glinerGatherConcatRelu,
    .linear = &linear,
    .linearQuickGelu = &linearQuickGelu,
    .linearRelu = &linearRelu,
    .linearGelu = &linearGelu,
    .linearAdd = &linearAdd,
    .linearPlanned = &linearPlanned,
    .linearNoBias = &linearNoBias,
    .linearNoBiasPlanned = &linearNoBiasPlanned,
    .linearNoBiasPair = &linearNoBiasPair,
    .argmaxLastRow = &argmaxLastRow,
    .argmaxRows = &argmaxRows,
    .argmaxRowsSuppress = &argmaxRowsSuppress,
    .argmaxLastRowSuppressTensor = &argmaxLastRowSuppressTensor,
    .linearNoBiasArgmaxLastRow = &linearNoBiasArgmaxLastRow,
    .linearNoBiasArgmaxLastRowTensor = &linearNoBiasArgmaxLastRowTensor,
    .linearNoBiasArgmaxLastRowSuppressTensor = &linearNoBiasArgmaxLastRowSuppressTensor,
    .linearNoBiasArgmaxRowsSuppress = &linearNoBiasArgmaxRowsSuppress,
    .gemma4MtpMaskedArgmax = &gemma4MtpMaskedArgmax,
    .gemma4MtpMaskedSelect = &gemma4MtpMaskedSelect,
    .gemma4MtpMaskedSelectFromHidden = &gemma4MtpMaskedSelectFromHidden,
    .gemma4MtpPreproject = &gemma4MtpPreproject,
    .gemma4MtpVerifyCommit = &gemma4MtpVerifyCommit,
    .linearNoBiasQkv = &linearNoBiasQkv,
    .linearPair = &linearPair,
    .linearPairRelu = &linearPairRelu,
    .linearPairInputs = &linearPairInputs,
    .linearTriple = &linearTriple,
    .decoderRuntimePrepareGreedy = &decoderRuntimePrepareGreedyOp,
    .decoderRuntimePrepareOrReuseFamily = &decoderRuntimePrepareOrReuseFamilyOp,
    .decoderRuntimeReady = &decoderRuntimeReadyOp,
    .decoderRuntimePrepareRmsNorm = &decoderRuntimePrepareRmsNormOp,
    .decoderRuntimeEnsureRmsNormSlot = &decoderRuntimeEnsureRmsNormSlotOp,
    .decoderRuntimeApplyRmsNorm = &decoderRuntimeApplyRmsNormOp,
    .decoderRuntimeApplyGeluBackward = &decoderRuntimeApplyGeluBackwardOp,
    .decoderRuntimePrepareLinear = &decoderRuntimePrepareLinearOp,
    .decoderRuntimeEnsureLinearSlot = &decoderRuntimeEnsureLinearSlotOp,
    .decoderRuntimeApplyLinear = &decoderRuntimeApplyLinearOp,
    .decoderRuntimeApplyLinearArgmax = &decoderRuntimeApplyLinearArgmaxOp,
    .decoderRuntimeApplyLinearPair = &decoderRuntimeApplyLinearPairOp,
    .decoderRuntimeApplyLinearQkv = &decoderRuntimeApplyLinearQkvOp,
    .decoderRuntimeApplyScaledAddScale = &decoderRuntimeApplyScaledAddScaleOp,
    .runGatedFfnResidual = &runGatedFfnResidualOp,
    .runGatedDecoderBlock = &runGatedDecoderBlockOp,
    .runAttentionOutputResidual = &runAttentionOutputResidualOp,
    .multiplyScalar = &multiplyScalar,
    .addScalar = &addScalar,
    .siluMultiply = &siluMultiply,
    .activationMultiply = &activationMultiply,
    .activationMultiplySliceLastDim = &activationMultiplySliceLastDim,
    .linearNoBiasActivationSliceLastDim = &linearNoBiasActivationSliceLastDim,
    .addMultiplyScalarTensor = &addMultiplyScalarTensor,
    .addWeightedScalars = &addWeightedScalars,
    .layerNorm = &layerNorm,
    .layerNormBackward = &layerNormBackwardOp,
    .addLayerNorm = &addLayerNorm,
    .rmsNorm = &rmsNorm,
    .rmsNormAddMultiplyScalarTensor = &rmsNormAddMultiplyScalarTensor,
    .rmsNormAddTensor = &rmsNormAddTensor,
    .rmsNormAddOutputScaleTensor = &rmsNormAddOutputScaleTensor,
    .rmsNormHeadsRope = &rmsNormHeadsRope,
    .rmsNormBare = &rmsNormBare,
    .gelu = &gelu,
    .geluExact = &geluExact,
    .relu = &relu,
    .silu = &silu,
    .quickGelu = &quickGelu,
    .sigmoid = &sigmoid,
    .tanh_act = &tanhAct,
    .reshapeOp = &reshapeOp,
    .reshape2D = &reshape2DOp,
    .reshape2d = &reshape2dOp,
    .allocUninitF32Shape = &allocUninitF32ShapeOp,
    .copyRows2D = &copyRows2DOp,
    .concatRows2D = &concatRows2DOp,
    .sliceRows2D = &sliceRows2DOp,
    .florenceVisionTailSources = &florenceVisionTailSourcesOp,
    .florenceProjectImageFeatures = &florenceProjectImageFeaturesOp,
    .sliceLastDim = &sliceLastDimOp,
    .cloneTensorShape = &cloneTensorShapeOp,
    .transposeOp = &transposeOp,
    .splitLastDim3 = &splitLastDim3,
    .concat = &concat,
    .add = &add,
    .addBiasRowsConsume = &addBiasRowsConsume,
    .scaledDotProductAttention = &sdpa,
    .scaledDotProductAttentionFull = &sdpaFull,
    .causalSelfAttention = &causalSelfAttention,
    .crossAttention = &crossAttention,
    .relativePositionBias = &relativePositionBias,
    .disentangledRelativeAttention = &debertaDisentangledAttention,
    .disentangledRelativeAttentionPacked = &debertaDisentangledAttentionPacked,
    .disentangledRelativeAttentionBackward = &debertaDisentangledAttentionBackward,
    .disentangledRelativeAttentionBackwardPacked = &debertaDisentangledAttentionBackwardPacked,
    .windowedSelfAttention = &windowedSelfAttention,
    .channelSelfAttention = &channelSelfAttention,
    .tokenGridConv2d = &tokenGridConv2d,
    .multiply = &multiply,
    .conv1d = &conv1d,
    .conv2d = &conv2d,
    .rope = &rope,
    .ropeScaled = &ropeScaled,
    .ropePerItem = &ropePerItem,
    .runAttentionResidual = &runAttentionResidualOp,
    .gqaCausalAttention = &gqaCausalAttention,
    .gqaPagedAttention = &gqaPagedAttention,
    .fromFloat32 = &fromFloat32Op,
    .fromFloat32Shape = &fromFloat32ShapeOp,
    .fromInt32Shape = &fromInt32ShapeOp,
    .toFloat32 = &toFloat32Op,
    .exportTensorData = &exportTensorDataOp,
    .beginI32ScalarDownload = &beginI32ScalarDownloadOp,
    .finishI32ScalarDownload = &finishI32ScalarDownloadOp,
    .cancelI32ScalarDownload = &cancelI32ScalarDownloadOp,
    .copyTensorFromBackend = &copyTensorFromBackendOp,
    .tensorDType = &tensorDTypeOp,
    .tensorShape = &tensorShapeOp,
    .evalTensor = &evalTensorOp,
    .trainingOverwriteF32 = &trainingOverwriteF32Op,
    .trainingZeroF32 = &trainingZeroF32Op,
    .trainingAccumulateF32 = &trainingAccumulateF32Op,
    .trainingAdamWManyF32 = &trainingAdamWManyF32Op,
    .trainingSumSquaresManyF32 = &trainingSumSquaresManyF32Op,
    .trainingSynchronize = &trainingSynchronizeOp,
    .subtract = &primSubtractOp,
    .divide = &primDivideOp,
    .negate = &primNegateOp,
    .sqrtOp = &primSqrtOp,
    .rsqrtOp = &primRsqrtOp,
    .expOp = &primExpOp,
    .logOp = &primLogOp,
    .sinOp = &primSinOp,
    .cosOp = &primCosOp,
    .tanhOp = &primTanhOp,
    .erfOp = &primErfOp,
    .absOp = &primAbsOp,
    .lessThan = &primLessThanOp,
    .whereSelect = &primWhereSelectOp,
    .reduceSumOp = &primReduceSumOp,
    .reduceMaxOp = &primReduceMaxOp,
    .reduceMeanOp = &primReduceMeanOp,
    .broadcastInDimOp = &primBroadcastInDimOp,
    .dotGeneralOp = &primDotGeneralOp,
    .scatterAddOp = &primScatterAddOp,
    .gatherOp = &primGatherOp,
    .sliceOp = &primSliceOp,
    .concatPrimOp = &primConcatPrimOp,
    .softmaxOp = &primSoftmaxOp,
    .logSoftmaxOp = &primLogSoftmaxOp,
    .maskedBceWithLogitsLoss = &maskedBceWithLogitsLossOp,
    .maskedBceWithLogitsBackward = &maskedBceWithLogitsBackwardOp,
    .debugCudaDeviceWarmup = &debugCudaDeviceWarmup,
    .zeroTensor = &zeroTensorOp,
};

// Identical to `vtable` except deinit also unbinds the request-scoped
// RunBudget; see computeBackendWithScopedRunBudget.
const scoped_run_budget_vtable = blk: {
    var copied = vtable;
    copied.deinitBackend = &deinitBackendClearRunBudget;
    break :blk copied;
};

test "cuda compute vtable is type checked" {
    const backend_kind_fn: *const fn (*anyopaque) ops.BackendKind = &backendKind;
    const linear_fn: *const fn (*anyopaque, CT, CT, CT, usize, usize, usize) anyerror!CT = &linear;
    const linear_no_bias_fn: *const fn (*anyopaque, CT, CT, usize, usize, usize) anyerror!CT = &linearNoBias;
    const rms_norm_fn: *const fn (*anyopaque, CT, CT, usize, f32) anyerror!CT = &rmsNorm;
    const rope_per_item_fn: *const fn (*anyopaque, CT, usize, usize, usize, usize, f32, f32, []const usize, []const usize, bool) anyerror!CT = &ropePerItem;
    const graph_prepare_input_fn: *const fn (*anyopaque, []const u8, CT, usize) anyerror!?CT = &debugCudaGraphPrepareFinalHiddenReplayInput;
    const graph_prepare_greedy_input_fn: *const fn (*anyopaque, []const u8, CT, usize) anyerror!?CT = &debugCudaGraphPrepareGreedyTokenReplayInput;
    _ = backend_kind_fn;
    _ = linear_fn;
    _ = linear_no_bias_fn;
    _ = rms_norm_fn;
    _ = rope_per_item_fn;
    _ = graph_prepare_input_fn;
    _ = graph_prepare_greedy_input_fn;
    _ = vtable;
}

test "cuda shape helpers reject incompatible shapes" {
    try std.testing.expect(try checkedMul(2, 3) == 6);
    try std.testing.expect(sameShape(&.{ 2, 3 }, &.{ 2, 3 }));
    try std.testing.expect(!sameShape(&.{ 2, 3 }, &.{ 3, 2 }));
}

test "deberta materialized workspace is aligned bounded and scales linearly with batch" {
    const b8 = try debertaMaterializedWorkspaceLayoutForShape(8, 256, 12, 64);
    const b32 = try debertaMaterializedWorkspaceLayoutForShape(32, 256, 12, 64);
    inline for (.{
        b8.q_f16,
        b8.k_f16,
        b8.v_f16,
        b8.q_r_f16,
        b8.k_r_f16,
        b8.content_scores,
        b8.c2p_scores,
        b8.p2c_scores,
        b8.probabilities_f16,
        b8.output_packed,
        b8.total_bytes,
    }) |offset| try std.testing.expectEqual(@as(usize, 0), offset % deberta_materialized_workspace_alignment);
    try std.testing.expect(b8.total_bytes < default_deberta_materialized_workspace_limit_bytes);
    try std.testing.expect(b32.total_bytes > default_deberta_materialized_workspace_limit_bytes);
    try std.testing.expect(b32.total_bytes >= b8.total_bytes * 4);
}

test "deberta materialized auto promotion is exact to qualified sm89" {
    try std.testing.expect(debertaMaterializedAutoTarget(8, 9));
    try std.testing.expect(!debertaMaterializedAutoTarget(8, 0));
    try std.testing.expect(!debertaMaterializedAutoTarget(9, 0));
}

test "deberta attention mode aliases select the advertised routes" {
    try std.testing.expectEqual(CudaDebertaAttentionMode.auto, parseCudaDebertaAttentionMode("auto").?);
    try std.testing.expectEqual(CudaDebertaAttentionMode.generated_tc, parseCudaDebertaAttentionMode("generated").?);
    try std.testing.expectEqual(CudaDebertaAttentionMode.generated_tc, parseCudaDebertaAttentionMode("generated-tc").?);
    try std.testing.expectEqual(CudaDebertaAttentionMode.streaming_f16, parseCudaDebertaAttentionMode("streaming-f16").?);
    try std.testing.expectEqual(@as(?CudaDebertaAttentionMode, null), parseCudaDebertaAttentionMode("typo"));
}

test "deberta generated attention variants parse strictly" {
    try std.testing.expectEqual(CudaDebertaGeneratedTcVariant.auto, parseCudaDebertaGeneratedTcVariant("auto").?);
    try std.testing.expectEqual(CudaDebertaGeneratedTcVariant.m32, parseCudaDebertaGeneratedTcVariant("m32n16").?);
    try std.testing.expectEqual(CudaDebertaGeneratedTcVariant.m16, parseCudaDebertaGeneratedTcVariant("m16").?);
    try std.testing.expectEqual(@as(?CudaDebertaGeneratedTcVariant, null), parseCudaDebertaGeneratedTcVariant("m64"));
}

test "cuda Flash prefill stats preserve head and query buckets" {
    var stats = RuntimeStats{};
    noteGqaFlashPrefillStats(&stats, .prefill_flash_f16_sm89, 256, 512);
    noteGqaFlashPrefillStats(&stats, .prefill_flash_f16_sm89, 512, 3);
    noteGqaFlashPrefillStats(&stats, .prefill_flash_f16_sm89_ineligible_fallback, 256, 3);
    noteGqaFlashPrefillStats(&stats, .prefill_flash_f16_sm89_symbol_fallback, 512, 512);
    try std.testing.expectEqual(@as(usize, 2), stats.launch_attention_gqa_prefill_flash_f16_sm89);
    try std.testing.expectEqual(@as(usize, 1), stats.launch_attention_gqa_prefill_flash_f16_sm89_hd256_q512);
    try std.testing.expectEqual(@as(usize, 1), stats.launch_attention_gqa_prefill_flash_f16_sm89_hd512_q3);
    try std.testing.expectEqual(@as(usize, 2), stats.launch_attention_gqa_prefill_flash_f16_sm89_fallbacks);
    try std.testing.expectEqual(@as(usize, 1), stats.launch_attention_gqa_prefill_flash_f16_sm89_ineligible_fallbacks);
    try std.testing.expectEqual(@as(usize, 1), stats.launch_attention_gqa_prefill_flash_f16_sm89_symbol_fallbacks);
    try std.testing.expectEqual(@as(usize, 1), stats.launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd256_q3);
    try std.testing.expectEqual(@as(usize, 1), stats.launch_attention_gqa_prefill_flash_f16_sm89_fallback_hd512_q512);
}

test "cuda lazy demand cancels pending host prefetch item" {
    const allocator = std.testing.allocator;
    var store = native_compute_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    native_compute_mod.initPrefetchQueue(&store, allocator);
    defer native_compute_mod.deinitPrefetchQueue(&store);

    var entry = native_compute_mod.LazyWeightEntry{
        .tensor_ref = .{ .name = "weight", .byte_len = 4 },
        .pending_prefetch = true,
    };
    try store.prefetch.items.append(allocator, &entry);

    try std.testing.expect(cancelPendingLazyHostPrefetchLocked(&store, &entry));
    try std.testing.expect(!entry.pending_prefetch);
    try std.testing.expectEqual(@as(usize, 0), store.prefetch.items.items.len);
    try std.testing.expect(!cancelPendingLazyHostPrefetchLocked(&store, &entry));
}

test "cuda dense stream weight classifier covers gemma attention and mlp matrices" {
    try std.testing.expect(isDenseFfnStreamWeightName("model.language_model.layers.0.mlp.gate_proj.weight"));
    try std.testing.expect(isDenseFfnStreamWeightName("model.language_model.layers.0.mlp.up_proj.weight"));
    try std.testing.expect(isDenseFfnStreamWeightName("model.language_model.layers.0.mlp.down_proj.weight"));
    try std.testing.expect(!isDenseFfnStreamWeightName("model.language_model.layers.0.self_attn.q_proj.weight"));

    try std.testing.expect(isDenseAttentionStreamWeightName("model.language_model.layers.0.self_attn.q_proj.weight"));
    try std.testing.expect(isDenseAttentionStreamWeightName("model.language_model.layers.0.self_attn.k_proj.weight"));
    try std.testing.expect(isDenseAttentionStreamWeightName("model.language_model.layers.0.self_attn.v_proj.weight"));
    try std.testing.expect(isDenseAttentionStreamWeightName("model.language_model.layers.0.self_attn.o_proj.weight"));
    try std.testing.expect(!isDenseAttentionStreamWeightName("model.language_model.layers.0.input_layernorm.weight"));
}

test "cuda dense host prefetch queue removal clears pending item" {
    const allocator = std.testing.allocator;
    const Dummy = struct {
        fn process(_: *anyopaque, _: *DenseHostPrefetchEntry) void {}
    };
    var dummy_ctx: u8 = 0;
    var compute = CudaCompute{
        .allocator = allocator,
        .ctx = undefined,
        .kernels = undefined,
        .dense_host_prefetch = DenseHostPrefetchQueue.init(allocator, &dummy_ctx, &Dummy.process),
        .dense_host_prefetch_initialized = true,
    };
    defer compute.dense_host_prefetch.deinit();

    var entry = DenseHostPrefetchEntry{
        .name = try allocator.dupe(u8, "model.language_model.layers.0.self_attn.q_proj.weight"),
        .range = .{
            .path = try allocator.dupe(u8, "/tmp/model.safetensors"),
            .byte_offset = 8,
            .byte_len = 16,
            .dtype = .bf16,
            .shape = try allocator.dupe(i64, &.{ 2, 4 }),
        },
        .kind = .attention,
        .pending = true,
    };
    defer entry.deinit(allocator);

    compute.dense_host_prefetch.lock();
    defer compute.dense_host_prefetch.unlock();
    try compute.dense_host_prefetch.items.append(allocator, &entry);
    try std.testing.expect(removeDenseHostPrefetchQueueItemLocked(&compute, &entry));
    try std.testing.expect(!entry.pending);
    try std.testing.expectEqual(@as(usize, 0), compute.dense_host_prefetch.items.items.len);
    try std.testing.expect(!removeDenseHostPrefetchQueueItemLocked(&compute, &entry));
}
