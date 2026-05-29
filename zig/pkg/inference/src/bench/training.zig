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
const ml = @import("ml");
const optimizers = ml.graph.optimizers;
const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;
const training_loop_mod = @import("../graph/training_loop.zig");
const training_mod = @import("../graph/training.zig");
const checkpoint_mod = ml.graph.checkpoint;
const BlasCompute = @import("../ops/blas_compute.zig").BlasCompute;
const WeightStore = @import("../ops/blas_compute.zig").WeightStore;

const BenchMode = enum {
    optimizer,
    graph,
    both,
};

const BenchConfig = struct {
    mode: BenchMode = .both,
    optimizer_len: usize = 1_000_000,
    optimizer_steps: usize = 50,
    graph_batch: usize = 8,
    graph_width: usize = 256,
    graph_depth: usize = 8,
    graph_steps: usize = 20,
    graph_use_gelu: bool = true,
    checkpoint_interval: u32 = 2,
    checkpoint_sweep: bool = false,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    const cfg = try parseArgs(init);

    if (cfg.mode == .optimizer or cfg.mode == .both) {
        const optimizer_result = try benchOptimizer(cfg);
        std.debug.print(
            \\optimizer_len={}
            \\optimizer_steps={}
            \\optimizer_scalar_ref_ms={d:.3}
            \\optimizer_fused_ms={d:.3}
            \\optimizer_scalar_ns_per_elem={d:.3}
            \\optimizer_fused_ns_per_elem={d:.3}
            \\optimizer_speedup={d:.3}
            \\
        , .{
            cfg.optimizer_len,
            cfg.optimizer_steps,
            nsToMs(optimizer_result.scalar_ns),
            nsToMs(optimizer_result.fused_ns),
            nsPerElem(optimizer_result.scalar_ns, cfg.optimizer_len, cfg.optimizer_steps),
            nsPerElem(optimizer_result.fused_ns, cfg.optimizer_len, cfg.optimizer_steps),
            speedup(optimizer_result.scalar_ns, optimizer_result.fused_ns),
        });
    }

    if (cfg.mode == .graph or cfg.mode == .both) {
        if (cfg.checkpoint_sweep) {
            const sweep = [_]?u32{ null, 2, 4, 8 };
            var baseline_total_ns: ?u64 = null;
            for (sweep) |interval| {
                const result = try runGraphSweepVariant(allocator, cfg, interval);
                printGraphResult(intervalLabel(interval), cfg, result);
                if (interval == null) {
                    baseline_total_ns = result.avg_total_ns;
                } else if (baseline_total_ns) |baseline| {
                    std.debug.print("{s}_speed_ratio_vs_off={d:.3}\n", .{
                        intervalLabel(interval),
                        speedup(baseline, result.avg_total_ns),
                    });
                }
            }
        } else {
            const graph_result = try benchGraphTraining(allocator, cfg);
            printGraphResult("off", cfg, graph_result.off);
            printGraphResult("checkpointed", cfg, graph_result.checkpointed);
            std.debug.print("graph_total_speed_ratio={d:.3}\n", .{
                speedup(graph_result.off.avg_total_ns, graph_result.checkpointed.avg_total_ns),
            });
            if (graph_result.checkpointed.last_checkpoint_summary) |summary| {
                std.debug.print(
                    "checkpoint_summary strategy={s} interval={} recomputable={} checkpointed={} ratio={d:.3}\n",
                    .{
                        @tagName(summary.strategy),
                        summary.layer_interval,
                        summary.recomputable_activations,
                        summary.checkpointed_activations,
                        summary.savings_ratio,
                    },
                );
            }
        }
    }
}

const OptimizerBenchResult = struct {
    scalar_ns: u64,
    fused_ns: u64,
};

fn benchOptimizer(cfg: BenchConfig) !OptimizerBenchResult {
    const allocator = std.heap.page_allocator;
    const len = cfg.optimizer_len;

    const grad = try allocator.alloc(f32, len);
    defer allocator.free(grad);
    for (grad, 0..) |*item, i| item.* = @as(f32, @floatFromInt((i % 17) + 1)) * 0.001;

    const adamw_cfg = optimizers.AdamWConfig{
        .beta1 = 0.9,
        .beta2 = 0.999,
        .eps = 1e-8,
        .weight_decay = 0.01,
    };

    const scalar_ns = try runOptimizerBenchLoop(allocator, len, grad, cfg.optimizer_steps, adamw_cfg, true);
    const fused_ns = try runOptimizerBenchLoop(allocator, len, grad, cfg.optimizer_steps, adamw_cfg, false);
    return .{ .scalar_ns = scalar_ns, .fused_ns = fused_ns };
}

fn runOptimizerBenchLoop(
    allocator: std.mem.Allocator,
    len: usize,
    grad: []const f32,
    steps: usize,
    cfg: optimizers.AdamWConfig,
    scalar_ref: bool,
) !u64 {
    const param = try allocator.alloc(f32, len);
    defer allocator.free(param);
    const m = try allocator.alloc(f32, len);
    defer allocator.free(m);
    const v = try allocator.alloc(f32, len);
    defer allocator.free(v);

    for (param, 0..) |*item, i| item.* = 1.0 + @as(f32, @floatFromInt(i % 13)) * 0.01;
    @memset(m, 0.0);
    @memset(v, 0.0);

    const start = nowNs();
    for (0..steps) |i| {
        const step_count: u32 = @intCast(i + 1);
        if (scalar_ref) {
            scalarAdamWReference(cfg, step_count, 0.001, param, grad, m, v);
        } else {
            optimizers.stepSlices(.{ .adamw = cfg }, step_count, 0.001, param, grad, m, v);
        }
    }
    const end = nowNs();
    return end - start;
}

const GraphBenchPair = struct {
    off: GraphBenchResult,
    checkpointed: GraphBenchResult,
};

const GraphBenchResult = struct {
    avg_loss: f64,
    avg_total_ns: u64,
    avg_runtime_input_ns: u64,
    avg_optimizer_ns: u64,
    avg_autodiff_ns: u64,
    avg_checkpoint_ns: u64,
    avg_execute_ns: u64,
    avg_extract_ns: u64,
    avg_peak_resident_bytes: usize,
    last_checkpoint_summary: ?training_mod.CheckpointSummary = null,
};

fn benchGraphTraining(allocator: std.mem.Allocator, cfg: BenchConfig) !GraphBenchPair {
    var built = try buildBenchGraph(
        allocator,
        cfg.graph_batch,
        cfg.graph_width,
        cfg.graph_depth,
        cfg.graph_use_gelu,
    );
    defer built.graph.deinit();

    const off = try runGraphVariant(allocator, &built.graph, built.loss, cfg, null, false);
    const checkpointed = try runGraphVariant(
        allocator,
        &built.graph,
        built.loss,
        cfg,
        .{ .strategy = .every_n_layers, .layer_interval = cfg.checkpoint_interval },
        true,
    );
    return .{ .off = off, .checkpointed = checkpointed };
}

fn runGraphSweepVariant(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    interval: ?u32,
) !GraphBenchResult {
    var built = try buildBenchGraph(allocator, cfg.graph_batch, cfg.graph_width, cfg.graph_depth, cfg.graph_use_gelu);
    defer built.graph.deinit();

    return runGraphVariant(
        allocator,
        &built.graph,
        built.loss,
        cfg,
        if (interval) |value| .{ .strategy = .every_n_layers, .layer_interval = value } else null,
        interval != null,
    );
}

fn runGraphVariant(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    loss: ml.graph.NodeId,
    cfg: BenchConfig,
    checkpoint_config: ?checkpoint_mod.CheckpointConfig,
    emit_checkpoint_analysis: bool,
) !GraphBenchResult {
    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer {
        ws.resident_weights.deinit(allocator);
        ws.lazy_weights.deinit(allocator);
    }
    var compute = BlasCompute.init(allocator, &ws, null);
    var cb = compute.computeBackend();

    var loop = training_loop_mod.TrainingLoop.init(allocator, .{
        .optimizer = .{ .adamw = .{} },
        .lr_schedule = .{ .constant = 0.001 },
        .checkpoint_config = checkpoint_config,
        .emit_checkpoint_analysis = emit_checkpoint_analysis,
    });
    defer loop.deinit();

    try seedGraphWeights(&loop.weight_store, graph, cfg.graph_width);

    var loss_sum: f64 = 0;
    var total_sum: u128 = 0;
    var runtime_sum: u128 = 0;
    var optimizer_sum: u128 = 0;
    var autodiff_sum: u128 = 0;
    var checkpoint_sum: u128 = 0;
    var execute_sum: u128 = 0;
    var extract_sum: u128 = 0;
    var peak_resident_sum: u128 = 0;

    for (0..cfg.graph_steps) |_| {
        const step_loss = try loop.step(graph, loss, &cb);
        loss_sum += step_loss;
        total_sum += loop.last_step_metrics.total_ns;
        runtime_sum += loop.last_step_metrics.runtime_input_build_ns;
        optimizer_sum += loop.last_step_metrics.optimizer_ns;
        autodiff_sum += loop.last_step_metrics.train_step.autodiff_ns;
        checkpoint_sum += loop.last_step_metrics.train_step.checkpoint_ns;
        execute_sum += loop.last_step_metrics.train_step.execute_ns;
        extract_sum += loop.last_step_metrics.train_step.extract_ns;
        peak_resident_sum += loop.last_step_metrics.train_step.peak_resident_bytes;
    }

    return .{
        .avg_loss = loss_sum / @as(f64, @floatFromInt(cfg.graph_steps)),
        .avg_total_ns = @intCast(total_sum / cfg.graph_steps),
        .avg_runtime_input_ns = @intCast(runtime_sum / cfg.graph_steps),
        .avg_optimizer_ns = @intCast(optimizer_sum / cfg.graph_steps),
        .avg_autodiff_ns = @intCast(autodiff_sum / cfg.graph_steps),
        .avg_checkpoint_ns = @intCast(checkpoint_sum / cfg.graph_steps),
        .avg_execute_ns = @intCast(execute_sum / cfg.graph_steps),
        .avg_extract_ns = @intCast(extract_sum / cfg.graph_steps),
        .avg_peak_resident_bytes = @intCast(peak_resident_sum / cfg.graph_steps),
        .last_checkpoint_summary = loop.last_step_metrics.checkpoint_summary,
    };
}

fn buildBenchGraph(
    allocator: std.mem.Allocator,
    batch: usize,
    width: usize,
    depth: usize,
    use_gelu: bool,
) !struct { graph: Graph, loss: ml.graph.NodeId } {
    var g = Graph.init(allocator);
    var b = Builder.init(&g);
    const batch_dim: i64 = @intCast(batch);
    const width_dim: i64 = @intCast(width);
    const batch_u32: u32 = @intCast(batch);
    const width_u32: u32 = @intCast(width);

    const x = try b.parameter("x", Shape.init(.f32, &.{ batch_dim, width_dim }));
    var current = x;
    for (0..depth) |i| {
        const weight_name = try std.fmt.allocPrint(allocator, "w{}", .{i});
        defer allocator.free(weight_name);
        const w = try b.parameter(weight_name, Shape.init(.f32, &.{ width_dim, width_dim }));
        current = try b.linearNoBias(current, w, batch_u32, width_u32, width_u32);
        if (use_gelu and i + 1 < depth) {
            current = try b.gelu(current);
        }
    }
    const loss = try b.reduceSum(current, &.{ 0, 1 });
    try g.markOutput(loss);
    return .{ .graph = g, .loss = loss };
}

fn seedGraphWeights(weight_store: *training_loop_mod.TrainingWeightStore, graph: *const Graph, width: usize) !void {
    for (graph.parameters.items) |param_id| {
        const node = graph.node(param_id);
        const name = graph.parameterName(node);
        const numel = node.output_shape.numElements() orelse return error.InvalidShape;
        const buf = try weight_store.allocator.alloc(f32, @intCast(numel));
        errdefer weight_store.allocator.free(buf);
        if (std.mem.eql(u8, name, "x")) {
            for (buf, 0..) |*item, i| item.* = @as(f32, @floatFromInt((i % width) + 1)) * 0.01;
        } else {
            for (buf, 0..) |*item, i| item.* = @as(f32, @floatFromInt((i % 11) + 1)) * 0.001;
        }
        const owned_name = try weight_store.allocator.dupe(u8, name);
        try weight_store.putOwned(owned_name, buf);
    }
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
        if (std.mem.eql(u8, arg, "--mode")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.mode = try parseMode(args[i]);
        } else if (std.mem.eql(u8, arg, "--optimizer-len")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.optimizer_len = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--optimizer-steps")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.optimizer_steps = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--graph-batch")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.graph_batch = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--graph-width")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.graph_width = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--graph-depth")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.graph_depth = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--graph-steps")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.graph_steps = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--graph-activation")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.graph_use_gelu = std.mem.eql(u8, args[i], "gelu");
        } else if (std.mem.eql(u8, arg, "--checkpoint-interval")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.checkpoint_interval = try std.fmt.parseInt(u32, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--checkpoint-sweep")) {
            cfg.checkpoint_sweep = true;
        } else if (std.mem.eql(u8, arg, "--help")) {
            try printUsage();
            std.process.exit(0);
        } else {
            return error.UnknownArgument;
        }
    }

    return cfg;
}

fn parseMode(value: []const u8) !BenchMode {
    if (std.mem.eql(u8, value, "optimizer")) return .optimizer;
    if (std.mem.eql(u8, value, "graph")) return .graph;
    if (std.mem.eql(u8, value, "both")) return .both;
    return error.InvalidMode;
}

fn printUsage() !void {
    std.debug.print(
        \\Usage: termite-training-bench [options]
        \\  --mode optimizer|graph|both
        \\  --optimizer-len N
        \\  --optimizer-steps N
        \\  --graph-batch N
        \\  --graph-width N
        \\  --graph-depth N
        \\  --graph-steps N
        \\  --graph-activation none|gelu
        \\  --checkpoint-interval N
        \\  --checkpoint-sweep
        \\
    , .{});
}

fn printGraphResult(label: []const u8, cfg: BenchConfig, result: GraphBenchResult) void {
    std.debug.print(
        "{s} graph_batch={} graph_width={} graph_depth={} graph_steps={} graph_activation={s} avg_loss={d:.6} avg_total_ms={d:.3} avg_runtime_input_ms={d:.3} avg_optimizer_ms={d:.3} avg_autodiff_ms={d:.3} avg_checkpoint_ms={d:.3} avg_execute_ms={d:.3} avg_extract_ms={d:.3} avg_peak_resident_mb={d:.3}\n",
        .{
            label,
            cfg.graph_batch,
            cfg.graph_width,
            cfg.graph_depth,
            cfg.graph_steps,
            if (cfg.graph_use_gelu) "gelu" else "none",
            result.avg_loss,
            nsToMs(result.avg_total_ns),
            nsToMs(result.avg_runtime_input_ns),
            nsToMs(result.avg_optimizer_ns),
            nsToMs(result.avg_autodiff_ns),
            nsToMs(result.avg_checkpoint_ns),
            nsToMs(result.avg_execute_ns),
            nsToMs(result.avg_extract_ns),
            bytesToMb(result.avg_peak_resident_bytes),
        },
    );
    if (result.last_checkpoint_summary) |summary| {
        std.debug.print(
            "{s}_checkpoint_summary strategy={s} interval={} recomputable={} checkpointed={} ratio={d:.3}\n",
            .{
                label,
                @tagName(summary.strategy),
                summary.layer_interval,
                summary.recomputable_activations,
                summary.checkpointed_activations,
                summary.savings_ratio,
            },
        );
    }
}

fn scalarAdamWReference(
    cfg: optimizers.AdamWConfig,
    step_count: u32,
    current_lr: f32,
    param: []f32,
    grad: []const f32,
    m: []f32,
    v: []f32,
) void {
    const t: f32 = @floatFromInt(step_count);
    const bias_correction1 = 1.0 - std.math.pow(f32, cfg.beta1, t);
    const bias_correction2 = 1.0 - std.math.pow(f32, cfg.beta2, t);

    for (param, grad, m, v) |*p, g, *m_item, *v_item| {
        p.* -= cfg.weight_decay * current_lr * p.*;
        m_item.* = cfg.beta1 * m_item.* + (1.0 - cfg.beta1) * g;
        v_item.* = cfg.beta2 * v_item.* + (1.0 - cfg.beta2) * g * g;
        const m_hat = m_item.* / bias_correction1;
        const v_hat = v_item.* / bias_correction2;
        p.* -= current_lr * m_hat / (@sqrt(v_hat) + cfg.eps);
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn bytesToMb(bytes: usize) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}

fn nowNs() u64 {
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return 0,
    }
}

fn nsPerElem(ns: u64, len: usize, steps: usize) f64 {
    const denom = @as(f64, @floatFromInt(len * steps));
    return @as(f64, @floatFromInt(ns)) / denom;
}

fn speedup(baseline_ns: u64, candidate_ns: u64) f64 {
    if (candidate_ns == 0) return 0;
    return @as(f64, @floatFromInt(baseline_ns)) / @as(f64, @floatFromInt(candidate_ns));
}

fn intervalLabel(interval: ?u32) []const u8 {
    return switch (interval orelse 0) {
        0 => "off",
        2 => "ckpt2",
        4 => "ckpt4",
        8 => "ckpt8",
        else => "ckpt",
    };
}
