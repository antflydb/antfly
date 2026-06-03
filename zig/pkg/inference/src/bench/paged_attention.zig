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
const ops = @import("../ops/ops.zig");
const NativeCompute = @import("../ops/native_compute.zig").NativeCompute;
const NativeWeightStore = @import("../ops/native_compute.zig").WeightStore;
const MlxCompute = if (build_options.enable_mlx) @import("../ops/mlx_compute.zig").MlxCompute else void;
const MlxWeightStore = if (build_options.enable_mlx) @import("../ops/mlx_compute.zig").WeightStore else void;
const mlx = if (build_options.enable_mlx) @import("../backends/mlx.zig") else struct {};
const mlx_quant = if (build_options.enable_mlx) @import("../backends/mlx_quant.zig") else struct {};
const runtime = @import("../runtime/root.zig");
const LoadedWeight = @import("../models/weight_source.zig").LoadedWeight;

const BackendKind = enum {
    native,
    mlx,
};

const BenchConfig = struct {
    backend: BackendKind = .native,
    prompt_len: usize = 256,
    decode_steps: usize = 64,
    page_size: u16 = 16,
    num_heads: usize = 32,
    num_kv_heads: usize = 8,
    head_dim: usize = 128,
    warmup_iters: usize = 2,
    measure_iters: usize = 5,
    cache_dtype: ?runtime.kv.pool.KvDType = null,
    cache_dtype_sweep: bool = false,
};

const cache_dtype_sweep_dtypes = [_]runtime.kv.pool.KvDType{ .f32, .f16, .int8, .fp8, .int4, .polar4, .turbo3 };

const BenchResult = struct {
    prompt_dense_ns: u64,
    prompt_paged_ns: u64,
    decode_dense_ns: u64,
    decode_paged_ns: u64,
};

const BackendResources = union(BackendKind) {
    native: struct {
        weights: *NativeWeightStore,
        compute: *NativeCompute,
    },
    mlx: if (build_options.enable_mlx) struct {
        weights: *MlxWeightStore,
        stream: mlx.c.mlx_stream,
        compute: *MlxCompute,
    } else void,

    fn deinit(self: *BackendResources, allocator: std.mem.Allocator) void {
        if (!build_options.enable_mlx) {
            const native_backend = &self.native;
            native_backend.compute.computeBackend().deinit();
            native_backend.weights.resident_weights.deinit(allocator);
            native_backend.weights.lazy_weights.deinit(allocator);
            allocator.destroy(native_backend.weights);
            return;
        }
        switch (self.*) {
            .native => |*native_backend| {
                native_backend.compute.computeBackend().deinit();
                native_backend.weights.resident_weights.deinit(allocator);
                native_backend.weights.lazy_weights.deinit(allocator);
                allocator.destroy(native_backend.weights);
            },
            .mlx => |*mlx_backend| {
                mlx_backend.compute.computeBackend().deinit();
                mlx_backend.weights.native_quant.deinit();
                _ = mlx.c.mlx_map_string_to_array_free(mlx_backend.weights.resident_weights);
                allocator.destroy(mlx_backend.weights);
                _ = mlx.c.mlx_stream_free(mlx_backend.stream);
            },
        }
    }

    fn computeBackend(self: *BackendResources) ops.ComputeBackend {
        if (!build_options.enable_mlx) return self.native.compute.computeBackend();
        return switch (self.*) {
            .native => |*native_backend| native_backend.compute.computeBackend(),
            .mlx => |*mlx_backend| mlx_backend.compute.computeBackend(),
        };
    }

    fn kvBackendKind(self: *const BackendResources) runtime.kv.pool.BackendKind {
        if (!build_options.enable_mlx) return .native;
        return switch (self.*) {
            .native => .native,
            .mlx => .mlx,
        };
    }

    fn defaultKvDType(self: *const BackendResources) runtime.kv.pool.KvDType {
        if (!build_options.enable_mlx) return .f32;
        return switch (self.*) {
            .native => .f32,
            .mlx => .f16,
        };
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;

    const cfg = try parseArgs(init);
    var backend = try initBackend(allocator, cfg.backend);
    defer backend.deinit(allocator);

    if (cfg.cache_dtype_sweep) {
        try runBenchSweep(allocator, &backend, cfg);
        return;
    }

    const result = try runBench(allocator, &backend, cfg);
    const kv_dtype = resolvedKvDType(&backend, cfg);

    printBenchResult(cfg, kv_dtype, result);
}

fn printBenchResult(cfg: BenchConfig, kv_dtype: runtime.kv.pool.KvDType, result: BenchResult) void {
    std.debug.print(
        \\backend={s}
        \\cache_dtype={s}
        \\prompt_len={}
        \\decode_steps={}
        \\page_size={}
        \\num_heads={}
        \\num_kv_heads={}
        \\head_dim={}
        \\kv_key_row_bytes={}
        \\kv_value_row_bytes={}
        \\kv_pair_bytes={}
        \\warmup_iters={}
        \\measure_iters={}
        \\prompt_dense_ms={d:.3}
        \\prompt_paged_ms={d:.3}
        \\decode_dense_ms_total={d:.3}
        \\decode_paged_ms_total={d:.3}
        \\decode_dense_ms_per_token={d:.3}
        \\decode_paged_ms_per_token={d:.3}
        \\
    , .{
        @tagName(cfg.backend),
        @tagName(kv_dtype),
        cfg.prompt_len,
        cfg.decode_steps,
        cfg.page_size,
        cfg.num_heads,
        cfg.num_kv_heads,
        cfg.head_dim,
        kv_dtype.bytesForKeyRow(@intCast(cfg.num_kv_heads), @intCast(cfg.head_dim)),
        kv_dtype.bytesForValueRow(@intCast(cfg.num_kv_heads), @intCast(cfg.head_dim)),
        kv_dtype.bytesForTokenPair(@intCast(cfg.num_kv_heads), @intCast(cfg.head_dim)),
        cfg.warmup_iters,
        cfg.measure_iters,
        nsToMs(result.prompt_dense_ns),
        nsToMs(result.prompt_paged_ns),
        nsToMs(result.decode_dense_ns),
        nsToMs(result.decode_paged_ns),
        nsToMs(divNs(result.decode_dense_ns, cfg.decode_steps)),
        nsToMs(divNs(result.decode_paged_ns, cfg.decode_steps)),
    });
}

fn parseArgs(init: std.process.Init) !BenchConfig {
    var cfg = BenchConfig{};
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args_buf: [64][]const u8 = undefined;
    var args_len: usize = 0;
    while (args_iter.next()) |arg| {
        if (args_len < args_buf.len) {
            args_buf[args_len] = arg;
            args_len += 1;
        }
    }
    const args = args_buf[0..args_len];

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.backend = try parseBackend(args[i]);
        } else if (std.mem.eql(u8, arg, "--prompt-len")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.prompt_len = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--decode-steps")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.decode_steps = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--page-size")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.page_size = try std.fmt.parseInt(u16, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--num-heads")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.num_heads = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--num-kv-heads")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.num_kv_heads = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--head-dim")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.head_dim = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.warmup_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.measure_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--cache-dtype")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.cache_dtype = runtime.kv.pool.parseKvDType(args[i]) orelse return error.InvalidCacheDtype;
        } else if (std.mem.eql(u8, arg, "--cache-dtype-sweep")) {
            cfg.cache_dtype_sweep = true;
        } else if (std.mem.eql(u8, arg, "--help")) {
            try printUsage();
            std.process.exit(0);
        } else {
            return error.UnknownArgument;
        }
    }

    if (cfg.prompt_len == 0 or cfg.decode_steps == 0) return error.InvalidBenchShape;
    if (cfg.num_heads == 0 or cfg.num_kv_heads == 0 or cfg.head_dim == 0) return error.InvalidBenchShape;
    if (cfg.num_heads % cfg.num_kv_heads != 0) return error.InvalidBenchShape;
    if (cfg.cache_dtype_sweep and cfg.cache_dtype != null) return error.ConflictingCacheDtypeOptions;
    if (cfg.cache_dtype) |dtype| {
        if (!isBenchDTypeSupported(dtype, cfg)) return error.UnsupportedKvHeadDim;
    }
    return cfg;
}

fn parseBackend(arg: []const u8) !BackendKind {
    if (std.mem.eql(u8, arg, "native")) return .native;
    if (std.mem.eql(u8, arg, "mlx")) {
        if (!build_options.enable_mlx) return error.MlxNotEnabled;
        return .mlx;
    }
    return error.InvalidBackend;
}

fn printUsage() !void {
    std.debug.print(
        \\Usage: termite-paged-attention-bench [options]
        \\  --backend native|mlx
        \\  --prompt-len N
        \\  --decode-steps N
        \\  --page-size N
        \\  --num-heads N
        \\  --num-kv-heads N
        \\  --head-dim N
        \\  --cache-dtype f16|f32|int8|fp8|int4|polar4|turbo3
        \\  --cache-dtype-sweep
        \\  --warmup-iters N
        \\  --measure-iters N
        \\
    , .{});
}

fn initBackend(allocator: std.mem.Allocator, backend: BackendKind) !BackendResources {
    switch (backend) {
        .native => {
            const weights = try allocator.create(NativeWeightStore);
            weights.* = .{
                .allocator = allocator,
                .resident_weights = .empty,
                .lazy_weights = .empty,
            };
            const compute = try allocator.create(NativeCompute);
            compute.* = NativeCompute.init(allocator, weights, null);
            return .{ .native = .{ .weights = weights, .compute = compute } };
        },
        .mlx => {
            if (!build_options.enable_mlx) return error.MlxNotEnabled;
            const stream = mlx.gpuStream();
            const weights = try allocator.create(MlxWeightStore);
            weights.* = .{
                .allocator = allocator,
                .resident_weights = mlx.c.mlx_map_string_to_array_new(),
                .stream = stream,
                .prefix = "",
                .lazy_weights = .empty,
                .native_quant = mlx_quant.defaultProvider(),
            };
            const compute = try allocator.create(MlxCompute);
            compute.* = try MlxCompute.init(allocator, weights, null);
            return .{ .mlx = .{ .weights = weights, .stream = stream, .compute = compute } };
        },
    }
}

fn resolvedKvDType(backend: *const BackendResources, cfg: BenchConfig) runtime.kv.pool.KvDType {
    return cfg.cache_dtype orelse backend.defaultKvDType();
}

fn configWithDType(cfg: BenchConfig, dtype: runtime.kv.pool.KvDType) BenchConfig {
    var next = cfg;
    next.cache_dtype = dtype;
    next.cache_dtype_sweep = false;
    return next;
}

fn isBenchDTypeSupported(dtype: runtime.kv.pool.KvDType, cfg: BenchConfig) bool {
    return switch (dtype) {
        .bf16 => false,
        .polar4, .turbo3 => runtime.kv.turboquant.isSupportedHeadDim(@intCast(cfg.head_dim)),
        else => true,
    };
}

fn runBenchSweep(allocator: std.mem.Allocator, backend: *BackendResources, cfg: BenchConfig) !void {
    std.debug.print(
        \\backend={s}
        \\cache_dtype_sweep=true
        \\prompt_len={}
        \\decode_steps={}
        \\page_size={}
        \\num_heads={}
        \\num_kv_heads={}
        \\head_dim={}
        \\warmup_iters={}
        \\measure_iters={}
        \\sweep_columns=cache_dtype,kv_key_row_bytes,kv_value_row_bytes,kv_pair_bytes,prompt_dense_ms,prompt_paged_ms,decode_dense_ms_total,decode_paged_ms_total,decode_dense_ms_per_token,decode_paged_ms_per_token
        \\
    , .{
        @tagName(cfg.backend),
        cfg.prompt_len,
        cfg.decode_steps,
        cfg.page_size,
        cfg.num_heads,
        cfg.num_kv_heads,
        cfg.head_dim,
        cfg.warmup_iters,
        cfg.measure_iters,
    });

    for (cache_dtype_sweep_dtypes) |dtype| {
        if (!isBenchDTypeSupported(dtype, cfg)) {
            std.debug.print("sweep_row cache_dtype={s} status=unsupported\n", .{@tagName(dtype)});
            continue;
        }
        const dtype_cfg = configWithDType(cfg, dtype);
        const result = try runBench(allocator, backend, dtype_cfg);
        std.debug.print(
            "sweep_row cache_dtype={s} kv_key_row_bytes={} kv_value_row_bytes={} kv_pair_bytes={} prompt_dense_ms={d:.3} prompt_paged_ms={d:.3} decode_dense_ms_total={d:.3} decode_paged_ms_total={d:.3} decode_dense_ms_per_token={d:.3} decode_paged_ms_per_token={d:.3}\n",
            .{
                @tagName(dtype),
                dtype.bytesForKeyRow(@intCast(cfg.num_kv_heads), @intCast(cfg.head_dim)),
                dtype.bytesForValueRow(@intCast(cfg.num_kv_heads), @intCast(cfg.head_dim)),
                dtype.bytesForTokenPair(@intCast(cfg.num_kv_heads), @intCast(cfg.head_dim)),
                nsToMs(result.prompt_dense_ns),
                nsToMs(result.prompt_paged_ns),
                nsToMs(result.decode_dense_ns),
                nsToMs(result.decode_paged_ns),
                nsToMs(divNs(result.decode_dense_ns, cfg.decode_steps)),
                nsToMs(divNs(result.decode_paged_ns, cfg.decode_steps)),
            },
        );
    }
}

fn runBench(allocator: std.mem.Allocator, backend: *BackendResources, cfg: BenchConfig) !BenchResult {
    var cb = backend.computeBackend();

    for (0..cfg.warmup_iters) |_| {
        _ = try benchPromptDense(allocator, &cb, cfg);
        _ = try benchPromptPaged(allocator, &cb, backend, cfg);
        _ = try benchDecodeDense(allocator, &cb, cfg);
        _ = try benchDecodePaged(allocator, &cb, backend, cfg);
    }

    var prompt_dense_total: u128 = 0;
    var prompt_paged_total: u128 = 0;
    var decode_dense_total: u128 = 0;
    var decode_paged_total: u128 = 0;

    for (0..cfg.measure_iters) |_| {
        prompt_dense_total += try benchPromptDense(allocator, &cb, cfg);
        prompt_paged_total += try benchPromptPaged(allocator, &cb, backend, cfg);
        decode_dense_total += try benchDecodeDense(allocator, &cb, cfg);
        decode_paged_total += try benchDecodePaged(allocator, &cb, backend, cfg);
    }

    return .{
        .prompt_dense_ns = @intCast(prompt_dense_total / cfg.measure_iters),
        .prompt_paged_ns = @intCast(prompt_paged_total / cfg.measure_iters),
        .decode_dense_ns = @intCast(decode_dense_total / cfg.measure_iters),
        .decode_paged_ns = @intCast(decode_paged_total / cfg.measure_iters),
    };
}

fn benchPromptDense(allocator: std.mem.Allocator, cb: *const ops.ComputeBackend, cfg: BenchConfig) !u64 {
    const q_data = try makeRows(allocator, cfg.prompt_len, cfg.num_heads * cfg.head_dim, 1);
    defer allocator.free(q_data);
    const k_data = try makeRows(allocator, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim, 2);
    defer allocator.free(k_data);
    const v_data = try makeRows(allocator, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim, 3);
    defer allocator.free(v_data);

    const q = try makeBenchTensor(cfg.backend, cb, q_data, cfg.prompt_len, cfg.num_heads * cfg.head_dim);
    defer cb.free(q);
    const k = try makeBenchTensor(cfg.backend, cb, k_data, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim);
    defer cb.free(k);
    const v = try makeBenchTensor(cfg.backend, cb, v_data, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim);
    defer cb.free(v);

    const start_ns = nowNs();
    const out = try cb.gqaCausalAttention(q, k, v, null, 1, cfg.prompt_len, cfg.num_heads, cfg.num_kv_heads, cfg.head_dim);
    defer cb.free(out);
    return nowNs() - start_ns;
}

fn benchPromptPaged(allocator: std.mem.Allocator, cb: *const ops.ComputeBackend, backend: *const BackendResources, cfg: BenchConfig) !u64 {
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{
        .backend = backend.kvBackendKind(),
        .dtype = resolvedKvDType(backend, cfg),
        .page_size_tokens = cfg.page_size,
        .num_layers_packed = 1,
        .num_kv_heads = @intCast(cfg.num_kv_heads),
        .head_dim = @intCast(cfg.head_dim),
    });
    const sequence_id = try manager.attachSequence(pool_id);
    try manager.appendTokens(sequence_id, @intCast(cfg.prompt_len));

    const q_data = try makeRows(allocator, cfg.prompt_len, cfg.num_heads * cfg.head_dim, 11);
    defer allocator.free(q_data);
    const k_data = try makeRows(allocator, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim, 12);
    defer allocator.free(k_data);
    const v_data = try makeRows(allocator, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim, 13);
    defer allocator.free(v_data);

    const q = try makeBenchTensor(cfg.backend, cb, q_data, cfg.prompt_len, cfg.num_heads * cfg.head_dim);
    defer cb.free(q);
    const k = try makeBenchTensor(cfg.backend, cb, k_data, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim);
    defer cb.free(k);
    const v = try makeBenchTensor(cfg.backend, cb, v_data, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim);
    defer cb.free(v);

    const start_ns = nowNs();
    const out = try cb.gqaPagedAttention(q, k, v, null, .{
        .mode = .paged_prefill,
        .total_sequence_len = cfg.prompt_len,
        .query_sequence_len = cfg.prompt_len,
        .kv_sequence_len = cfg.prompt_len,
        .kv_cache = kvCacheView(&manager, sequence_id, pool_id),
        .kv_manager = &manager,
        .layer_index = 0,
    }, 1, cfg.num_heads, cfg.num_kv_heads, cfg.head_dim);
    defer cb.free(out);
    return nowNs() - start_ns;
}

fn benchDecodeDense(allocator: std.mem.Allocator, cb: *const ops.ComputeBackend, cfg: BenchConfig) !u64 {
    var total_ns: u64 = 0;
    for (0..cfg.decode_steps) |step| {
        const seq_len = cfg.prompt_len + step + 1;
        const q_data = try makeRows(allocator, seq_len, cfg.num_heads * cfg.head_dim, 21 + step);
        defer allocator.free(q_data);
        const k_data = try makeRows(allocator, seq_len, cfg.num_kv_heads * cfg.head_dim, 42 + step);
        defer allocator.free(k_data);
        const v_data = try makeRows(allocator, seq_len, cfg.num_kv_heads * cfg.head_dim, 84 + step);
        defer allocator.free(v_data);

        const q = try makeBenchTensor(cfg.backend, cb, q_data, seq_len, cfg.num_heads * cfg.head_dim);
        defer cb.free(q);
        const k = try makeBenchTensor(cfg.backend, cb, k_data, seq_len, cfg.num_kv_heads * cfg.head_dim);
        defer cb.free(k);
        const v = try makeBenchTensor(cfg.backend, cb, v_data, seq_len, cfg.num_kv_heads * cfg.head_dim);
        defer cb.free(v);

        const start_ns = nowNs();
        const out = try cb.gqaCausalAttention(q, k, v, null, 1, seq_len, cfg.num_heads, cfg.num_kv_heads, cfg.head_dim);
        defer cb.free(out);
        total_ns += nowNs() - start_ns;
    }
    return total_ns;
}

fn benchDecodePaged(allocator: std.mem.Allocator, cb: *const ops.ComputeBackend, backend: *const BackendResources, cfg: BenchConfig) !u64 {
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{
        .backend = backend.kvBackendKind(),
        .dtype = resolvedKvDType(backend, cfg),
        .page_size_tokens = cfg.page_size,
        .num_layers_packed = 1,
        .num_kv_heads = @intCast(cfg.num_kv_heads),
        .head_dim = @intCast(cfg.head_dim),
    });
    const sequence_id = try manager.attachSequence(pool_id);
    try manager.appendTokens(sequence_id, @intCast(cfg.prompt_len));

    const prefill_q_data = try makeRows(allocator, cfg.prompt_len, cfg.num_heads * cfg.head_dim, 101);
    defer allocator.free(prefill_q_data);
    const prefill_k_data = try makeRows(allocator, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim, 102);
    defer allocator.free(prefill_k_data);
    const prefill_v_data = try makeRows(allocator, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim, 103);
    defer allocator.free(prefill_v_data);

    const prefill_q = try makeBenchTensor(cfg.backend, cb, prefill_q_data, cfg.prompt_len, cfg.num_heads * cfg.head_dim);
    defer cb.free(prefill_q);
    const prefill_k = try makeBenchTensor(cfg.backend, cb, prefill_k_data, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim);
    defer cb.free(prefill_k);
    const prefill_v = try makeBenchTensor(cfg.backend, cb, prefill_v_data, cfg.prompt_len, cfg.num_kv_heads * cfg.head_dim);
    defer cb.free(prefill_v);

    const prefill_out = try cb.gqaPagedAttention(prefill_q, prefill_k, prefill_v, null, .{
        .mode = .paged_prefill,
        .total_sequence_len = cfg.prompt_len,
        .query_sequence_len = cfg.prompt_len,
        .kv_sequence_len = cfg.prompt_len,
        .kv_cache = kvCacheView(&manager, sequence_id, pool_id),
        .kv_manager = &manager,
        .layer_index = 0,
    }, 1, cfg.num_heads, cfg.num_kv_heads, cfg.head_dim);
    cb.free(prefill_out);

    var total_ns: u64 = 0;
    for (0..cfg.decode_steps) |step| {
        const seq_len = cfg.prompt_len + step + 1;
        try manager.appendTokens(sequence_id, 1);
        const kv_tokens = manager.tokenCount(sequence_id) orelse return error.InvalidSequenceId;

        const q_data = try makeRows(allocator, 1, cfg.num_heads * cfg.head_dim, 201 + step);
        defer allocator.free(q_data);
        const k_data = try makeRows(allocator, 1, cfg.num_kv_heads * cfg.head_dim, 301 + step);
        defer allocator.free(k_data);
        const v_data = try makeRows(allocator, 1, cfg.num_kv_heads * cfg.head_dim, 401 + step);
        defer allocator.free(v_data);

        const q = try makeBenchTensor(cfg.backend, cb, q_data, 1, cfg.num_heads * cfg.head_dim);
        defer cb.free(q);
        const k = try makeBenchTensor(cfg.backend, cb, k_data, 1, cfg.num_kv_heads * cfg.head_dim);
        defer cb.free(k);
        const v = try makeBenchTensor(cfg.backend, cb, v_data, 1, cfg.num_kv_heads * cfg.head_dim);
        defer cb.free(v);

        const start_ns = nowNs();
        const out = try cb.gqaPagedAttention(q, k, v, null, .{
            .mode = .paged_decode,
            .total_sequence_len = seq_len,
            .query_sequence_len = 1,
            .kv_sequence_len = kv_tokens,
            .kv_position_offset = seq_len - kv_tokens,
            .kv_cache = kvCacheView(&manager, sequence_id, pool_id),
            .kv_manager = &manager,
            .layer_index = 0,
        }, 1, cfg.num_heads, cfg.num_kv_heads, cfg.head_dim);
        defer cb.free(out);
        total_ns += nowNs() - start_ns;
    }
    return total_ns;
}

fn kvCacheView(manager: *runtime.kv.manager.KvManager, sequence_id: runtime.kv.manager.SequenceId, pool_id: runtime.kv.block.KvPoolId) ops.KvCacheView {
    const table = manager.blockTable(sequence_id) orelse unreachable;
    return .{
        .sequence_id = sequence_id,
        .pool_id = pool_id,
        .logical_block_count = table.len(),
        .tail_tokens = table.tail_tokens,
        .position_offset = 0,
    };
}

fn makeBenchTensor(backend: BackendKind, cb: *const ops.ComputeBackend, data: []const f32, rows: usize, cols: usize) !ops.CT {
    if (backend == .mlx and build_options.enable_mlx) {
        const mlx_compute: *MlxCompute = @ptrCast(@alignCast(cb.ptr));
        const shape = [_]i32{ @intCast(rows), @intCast(cols) };
        return mlx_compute.fromFloat32Shape(data, &shape);
    }
    return cb.fromFloat32(data);
}

fn makeRows(allocator: std.mem.Allocator, rows: usize, cols: usize, seed: usize) ![]f32 {
    const data = try allocator.alloc(f32, rows * cols);
    for (data, 0..) |*value, idx| {
        const mixed = (idx * 131 + seed * 17) % 1024;
        value.* = @as(f32, @floatFromInt(mixed)) / 1024.0;
    }
    return data;
}

fn divNs(total_ns: u64, count: usize) u64 {
    return if (count == 0) 0 else total_ns / count;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}

fn nowNs() u64 {
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return 0,
    }
}
