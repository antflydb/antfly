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

const cuda_buffer = if (build_options.enable_cuda) @import("../ops/cuda/buffer.zig") else struct {};
const cuda_context = if (build_options.enable_cuda) @import("../ops/cuda/context.zig") else struct {};
const cuda_driver = if (build_options.enable_cuda) @import("../ops/cuda/driver.zig") else struct {};
const native_embed = @import("../native_embed.zig");
const quant_codec = @import("../gguf/quant_codec.zig");

const print = std.debug.print;
const cuda_ptx = if (build_options.enable_cuda) @embedFile("../ops/cuda/artifacts/termite_cuda_kernels.ptx") else "";
const cuda_ptx_z = cuda_ptx ++ "\x00";

const q4_k_values_per_block: usize = 256;
const q4_k_block_bytes: usize = 144;

const Shape = struct {
    label: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

const shapes = [_]Shape{
    .{ .label = "CLIP text proj/QKV", .rows = 77, .in_dim = 768, .out_dim = 768 },
    .{ .label = "CLIP vision proj/QKV", .rows = 257, .in_dim = 768, .out_dim = 768 },
    .{ .label = "CLIP text MLP up", .rows = 77, .in_dim = 768, .out_dim = 3072 },
    .{ .label = "CLIP text MLP down", .rows = 77, .in_dim = 3072, .out_dim = 768 },
    .{ .label = "CLAP audio MLP up", .rows = 256, .in_dim = 768, .out_dim = 3072 },
    .{ .label = "pooled projection", .rows = 1, .in_dim = 768, .out_dim = 768 },
};

const Config = struct {
    warmup_iters: usize = 5,
    measure_iters: usize = 50,
    model_path: ?[]const u8 = null,
    text: []const u8 = "a photo of a document with audio metadata",
    full_iters: usize = 1,
};

const BenchModule = if (build_options.enable_cuda) struct {
    module: cuda_driver.CUmodule = null,
    linear_q4_k_f32: cuda_driver.CUfunction = null,
    linear_q4_k_bias_f32: cuda_driver.CUfunction = null,
    linear_q4_k_f32_tiled: cuda_driver.CUfunction = null,
    linear_q4_k_bias_f32_tiled: cuda_driver.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tiled: cuda_driver.CUfunction = null,
    linear_q4_k_f32_tile4: cuda_driver.CUfunction = null,
    linear_q4_k_bias_f32_tile4: cuda_driver.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tile4: cuda_driver.CUfunction = null,
    linear_q4_k_triple_bias_f32: cuda_driver.CUfunction = null,
    linear_q4_k_triple_bias_f32_tiled: cuda_driver.CUfunction = null,

    fn load(ctx: *cuda_context.CudaContext) cuda_driver.Error!BenchModule {
        try ctx.makeCurrent();
        var module: cuda_driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, cuda_ptx_z.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);

        var linear_q4_k_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32, module, "termite_linear_q4_k_f32"));
        var linear_q4_k_bias_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32, module, "termite_linear_q4_k_bias_f32"));
        var linear_q4_k_f32_tiled: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32_tiled, module, "termite_linear_q4_k_f32_tiled"));
        var linear_q4_k_bias_f32_tiled: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32_tiled, module, "termite_linear_q4_k_bias_f32_tiled"));
        var linear_q4_k_bias_quick_gelu_f32_tiled: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_quick_gelu_f32_tiled, module, "termite_linear_q4_k_bias_quick_gelu_f32_tiled"));
        var linear_q4_k_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32_tile4, module, "termite_linear_q4_k_f32_tile4"));
        var linear_q4_k_bias_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32_tile4, module, "termite_linear_q4_k_bias_f32_tile4"));
        var linear_q4_k_bias_quick_gelu_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_quick_gelu_f32_tile4, module, "termite_linear_q4_k_bias_quick_gelu_f32_tile4"));
        var linear_q4_k_triple_bias_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32, module, "termite_linear_q4_k_triple_bias_f32"));
        var linear_q4_k_triple_bias_f32_tiled: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32_tiled, module, "termite_linear_q4_k_triple_bias_f32_tiled"));

        return .{
            .module = module,
            .linear_q4_k_f32 = linear_q4_k_f32,
            .linear_q4_k_bias_f32 = linear_q4_k_bias_f32,
            .linear_q4_k_f32_tiled = linear_q4_k_f32_tiled,
            .linear_q4_k_bias_f32_tiled = linear_q4_k_bias_f32_tiled,
            .linear_q4_k_bias_quick_gelu_f32_tiled = linear_q4_k_bias_quick_gelu_f32_tiled,
            .linear_q4_k_f32_tile4 = linear_q4_k_f32_tile4,
            .linear_q4_k_bias_f32_tile4 = linear_q4_k_bias_f32_tile4,
            .linear_q4_k_bias_quick_gelu_f32_tile4 = linear_q4_k_bias_quick_gelu_f32_tile4,
            .linear_q4_k_triple_bias_f32 = linear_q4_k_triple_bias_f32,
            .linear_q4_k_triple_bias_f32_tiled = linear_q4_k_triple_bias_f32_tiled,
        };
    }

    fn unload(self: *BenchModule, ctx: *cuda_context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.module = null;
            self.linear_q4_k_f32 = null;
            self.linear_q4_k_bias_f32 = null;
            self.linear_q4_k_f32_tiled = null;
            self.linear_q4_k_bias_f32_tiled = null;
            self.linear_q4_k_bias_quick_gelu_f32_tiled = null;
            self.linear_q4_k_f32_tile4 = null;
            self.linear_q4_k_bias_f32_tile4 = null;
            self.linear_q4_k_bias_quick_gelu_f32_tile4 = null;
            self.linear_q4_k_triple_bias_f32 = null;
            self.linear_q4_k_triple_bias_f32_tiled = null;
        }
    }
} else struct {};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    if (wantsHelp(args)) {
        printUsage();
        return;
    }
    const cfg = try parseArgs(args);
    if (!build_options.enable_cuda) {
        print("bench-cuda requires a build with -Dcuda=true\n", .{});
        return error.CudaUnavailable;
    }

    try runKernelBench(allocator, cfg);
    if (cfg.model_path) |model_path| {
        try runFullTextEmbedBench(allocator, io, cfg, model_path);
    }
}

fn parseArgs(args: []const []const u8) !Config {
    var cfg = Config{};
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--warmup-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingWarmupIters;
            cfg.warmup_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingMeasureIters;
            cfg.measure_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--model")) {
            i += 1;
            if (i >= args.len) return error.MissingModelPath;
            cfg.model_path = args[i];
        } else if (std.mem.eql(u8, arg, "--text")) {
            i += 1;
            if (i >= args.len) return error.MissingText;
            cfg.text = args[i];
        } else if (std.mem.eql(u8, arg, "--full-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingFullIters;
            cfg.full_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else {
            print("unknown bench-cuda argument: {s}\n", .{arg});
            printUsage();
            return error.InvalidArgument;
        }
    }
    if (cfg.measure_iters == 0) return error.InvalidArgument;
    if (cfg.full_iters == 0) return error.InvalidArgument;
    return cfg;
}

fn wantsHelp(args: []const []const u8) bool {
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) return true;
    }
    return false;
}

fn printUsage() void {
    print(
        \\usage: termite bench-cuda [--warmup-iters N] [--measure-iters N]
        \\                         [--model <clipclap-model-dir>] [--text <prompt>] [--full-iters N]
        \\
        \\Benchmarks CUDA Q4_K linear kernels on CLIP/CLAP-sized shapes. If --model is
        \\provided, also runs full ClipCLAP text embedding through the CUDA backend.
        \\
    , .{});
}

fn runKernelBench(allocator: std.mem.Allocator, cfg: Config) !void {
    var ctx = try cuda_context.CudaContext.initDefault();
    defer ctx.deinit();

    var module = try BenchModule.load(&ctx);
    defer module.unload(&ctx);

    print("CUDA Q4_K microbench: device={s} cc={d}.{d} warmup={d} measure={d}\n", .{
        ctx.info.nameSlice(),
        ctx.info.compute_major,
        ctx.info.compute_minor,
        cfg.warmup_iters,
        cfg.measure_iters,
    });
    print("{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>14} {s:>14} {s:>14} {s:>12}\n", .{
        "shape",
        "rows",
        "in",
        "out",
        "scalar ns",
        "tiled ns",
        "scalar+b ns",
        "tiled+b ns",
        "qgelu ns",
        "checksum",
    });
    print("{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>14} {s:>14} {s:>14} {s:>12}\n", .{
        "-----",
        "----",
        "--",
        "---",
        "------",
        "------",
        "-----------",
        "---------",
        "--------",
        "--------",
    });

    for (shapes) |shape| {
        try benchShape(allocator, &ctx, &module, cfg, shape);
    }

    print("\n{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>12}\n", .{
        "triple shape",
        "rows",
        "in",
        "out",
        "scalar ns",
        "tiled ns",
        "checksum",
    });
    print("{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>12}\n", .{
        "------------",
        "----",
        "--",
        "---",
        "---------",
        "--------",
        "--------",
    });
    try benchTripleShape(allocator, &ctx, &module, cfg, .{ .label = "CLIP text QKV", .rows = 77, .in_dim = 768, .out_dim = 768 });
    try benchTripleShape(allocator, &ctx, &module, cfg, .{ .label = "CLIP vision QKV", .rows = 257, .in_dim = 768, .out_dim = 768 });
}

fn benchShape(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    shape: Shape,
) !void {
    if (shape.in_dim % q4_k_values_per_block != 0) return error.InvalidArgument;

    const input_count = try std.math.mul(usize, shape.rows, shape.in_dim);
    const output_count = try std.math.mul(usize, shape.rows, shape.out_dim);
    const row_blocks = shape.in_dim / q4_k_values_per_block;
    const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q4_k_block_bytes);

    const input_host = try allocator.alloc(f32, input_count);
    defer allocator.free(input_host);
    const bias_host = try allocator.alloc(f32, shape.out_dim);
    defer allocator.free(bias_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillBias(bias_host);
    fillQ4KWeights(weight_host);

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_host.len);
    defer weight.free(ctx);
    var bias = try cuda_buffer.DeviceBuffer.alloc(ctx, bias_host.len * @sizeOf(f32));
    defer bias.free(ctx);
    var output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try bias.copyFromHost(ctx, std.mem.sliceAsBytes(bias_host));
    try ctx.synchronize();

    const no_bias_ns = try timeCudaStep(ctx, cfg, launchQ4K, .{ module, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
    const no_bias_tiled_ns = try timeCudaStep(ctx, cfg, launchQ4KTiled, .{ module, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
    const bias_ns = try timeCudaStep(ctx, cfg, launchQ4KBias, .{ module, ctx, output, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });
    const bias_tiled_ns = try timeCudaStep(ctx, cfg, launchQ4KBiasTiled, .{ module, ctx, output, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });
    const quick_gelu_ns = try timeCudaStep(ctx, cfg, launchQ4KBiasQuickGeluTiled, .{ module, ctx, output, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });

    var sample: [1]f32 = undefined;
    try output.copyToHost(ctx, std.mem.sliceAsBytes(&sample));
    try ctx.synchronize();

    print("{s:<24} {d:>8} {d:>8} {d:>8} {d:>14} {d:>14} {d:>14} {d:>14} {d:>14} {d:>12.4}\n", .{
        shape.label,
        shape.rows,
        shape.in_dim,
        shape.out_dim,
        no_bias_ns,
        no_bias_tiled_ns,
        bias_ns,
        bias_tiled_ns,
        quick_gelu_ns,
        sample[0],
    });
}

fn timeCudaStep(
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    comptime step: anytype,
    args: anytype,
) !u64 {
    for (0..cfg.warmup_iters) |_| {
        try @call(.auto, step, args);
        try ctx.synchronize();
    }

    var total_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const started = nowNs();
        try @call(.auto, step, args);
        try ctx.synchronize();
        total_ns += nowNs() - started;
    }
    return total_ns / cfg.measure_iters;
}

fn benchTripleShape(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    shape: Shape,
) !void {
    const input_count = try std.math.mul(usize, shape.rows, shape.in_dim);
    const output_count = try std.math.mul(usize, shape.rows, shape.out_dim);
    const row_blocks = shape.in_dim / q4_k_values_per_block;
    const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q4_k_block_bytes);

    const input_host = try allocator.alloc(f32, input_count);
    defer allocator.free(input_host);
    const bias_host = try allocator.alloc(f32, shape.out_dim);
    defer allocator.free(bias_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillBias(bias_host);
    fillQ4KWeights(weight_host);

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_host.len);
    defer weight.free(ctx);
    var bias = try cuda_buffer.DeviceBuffer.alloc(ctx, bias_host.len * @sizeOf(f32));
    defer bias.free(ctx);
    var output_a = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output_a.free(ctx);
    var output_b = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output_b.free(ctx);
    var output_c = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output_c.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try bias.copyFromHost(ctx, std.mem.sliceAsBytes(bias_host));
    try ctx.synchronize();

    const scalar_ns = try timeCudaStep(ctx, cfg, launchQ4KTripleBias, .{ module, ctx, output_a, output_b, output_c, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });
    const tiled_ns = try timeCudaStep(ctx, cfg, launchQ4KTripleBiasTiled, .{ module, ctx, output_a, output_b, output_c, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });

    var sample: [1]f32 = undefined;
    try output_a.copyToHost(ctx, std.mem.sliceAsBytes(&sample));
    try ctx.synchronize();
    print("{s:<24} {d:>8} {d:>8} {d:>8} {d:>14} {d:>14} {d:>12.4}\n", .{
        shape.label,
        shape.rows,
        shape.in_dim,
        shape.out_dim,
        scalar_ns,
        tiled_ns,
        sample[0],
    });
}

fn launchQ4K(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KRaw(ctx, module.linear_q4_k_f32, output, input, weight, rows, in_dim, out_dim);
}

fn launchQ4KBias(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KBiasRaw(ctx, module.linear_q4_k_bias_f32, output, input, weight, bias, rows, in_dim, out_dim);
}

fn launchQ4KTiled(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTile4(ctx, module.linear_q4_k_f32_tile4, output, input, weight, .{}, rows, in_dim, out_dim, false, false);
}

fn launchQ4KBiasTiled(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTile4(ctx, module.linear_q4_k_bias_f32_tile4, output, input, weight, bias, rows, in_dim, out_dim, true, false);
}

fn launchQ4KBiasQuickGeluTiled(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTile4(ctx, module.linear_q4_k_bias_quick_gelu_f32_tile4, output, input, weight, bias, rows, in_dim, out_dim, true, false);
}

fn launchLinearQ4KTile4(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    has_bias: bool,
    has_residual: bool,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, if (has_bias) bias else null, rows, in_dim, out_dim);
    if (has_residual) return error.InvalidCudaState;
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var bias_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&bias_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    if (!has_bias) {
        params[3] = @ptrCast(&rows_u32);
        params[4] = @ptrCast(&in_dim_u32);
        params[5] = @ptrCast(&out_dim_u32);
    }
    try launch2d(ctx, function, (out_dim + 3) / 4, rows, 256, &params);
}

fn launchQ4KTripleBias(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output_a: cuda_buffer.DeviceBuffer,
    output_b: cuda_buffer.DeviceBuffer,
    output_c: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTripleBiasRaw(ctx, module.linear_q4_k_triple_bias_f32, output_a, output_b, output_c, input, weight, bias, rows, in_dim, out_dim, false);
}

fn launchQ4KTripleBiasTiled(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output_a: cuda_buffer.DeviceBuffer,
    output_b: cuda_buffer.DeviceBuffer,
    output_c: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTripleBiasRaw(ctx, module.linear_q4_k_triple_bias_f32_tiled, output_a, output_b, output_c, input, weight, bias, rows, in_dim, out_dim, true);
}

fn launchLinearQ4KRaw(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, null, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch1d(ctx, function, try checkedMul(rows, out_dim), &params);
}

fn launchLinearQ4KBiasRaw(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, bias, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var bias_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&bias_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch1d(ctx, function, try checkedMul(rows, out_dim), &params);
}

fn launchLinearQ4KBlocks(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, null, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launchBlocks(ctx, function, try checkedMul(rows, out_dim), 256, &params);
}

fn launchLinearQ4KBiasBlocks(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, bias, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var bias_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&bias_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launchBlocks(ctx, function, try checkedMul(rows, out_dim), 256, &params);
}

fn launchLinearQ4KTripleBiasRaw(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output_a: cuda_buffer.DeviceBuffer,
    output_b: cuda_buffer.DeviceBuffer,
    output_c: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    tiled: bool,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output_a, input, weight, bias, rows, in_dim, out_dim);
    try validateQ4KBuffers(output_b, input, weight, bias, rows, in_dim, out_dim);
    try validateQ4KBuffers(output_c, input, weight, bias, rows, in_dim, out_dim);
    var dst_a_ptr = output_a.ptr;
    var dst_b_ptr = output_b.ptr;
    var dst_c_ptr = output_c.ptr;
    var input_ptr = input.ptr;
    var weight_a_ptr = weight.ptr;
    var bias_a_ptr = bias.ptr;
    var weight_b_ptr = weight.ptr;
    var bias_b_ptr = bias.ptr;
    var weight_c_ptr = weight.ptr;
    var bias_c_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_a_ptr),
        @ptrCast(&dst_b_ptr),
        @ptrCast(&dst_c_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_a_ptr),
        @ptrCast(&bias_a_ptr),
        @ptrCast(&weight_b_ptr),
        @ptrCast(&bias_b_ptr),
        @ptrCast(&weight_c_ptr),
        @ptrCast(&bias_c_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    const count = try checkedMul(try checkedMul(rows, out_dim), 3);
    if (tiled) {
        try launchBlocks(ctx, function, count, 256, &params);
    } else {
        try launch1d(ctx, function, count, &params);
    }
}

fn validateQ4KBuffers(
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: ?cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
    const row_blocks = in_dim / q4_k_values_per_block;
    try checkF32Bytes(output, try checkedMul(rows, out_dim));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    try checkRawBytes(weight, try checkedMul(try checkedMul(out_dim, row_blocks), q4_k_block_bytes));
    if (bias) |bias_buf| try checkF32Bytes(bias_buf, out_dim);
}

fn launch1d(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    count: usize,
    params: [*]?*anyopaque,
) cuda_driver.Error!void {
    const block: c_uint = 256;
    const grid = try toU32((count + block - 1) / block);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid,
        1,
        1,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
}

fn launchBlocks(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    blocks: usize,
    threads: usize,
    params: [*]?*anyopaque,
) cuda_driver.Error!void {
    const grid = try toU32(blocks);
    const block = try toU32(threads);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid,
        1,
        1,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
}

fn launch2d(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    grid_x: usize,
    grid_y: usize,
    threads: usize,
    params: [*]?*anyopaque,
) cuda_driver.Error!void {
    const gx = try toU32(grid_x);
    const gy = try toU32(grid_y);
    const block = try toU32(threads);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        gx,
        gy,
        1,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
}

fn checkedMul(a: usize, b: usize) cuda_driver.Error!usize {
    return std.math.mul(usize, a, b) catch error.InvalidCudaState;
}

fn toU32(value: usize) cuda_driver.Error!u32 {
    if (value > std.math.maxInt(u32)) return error.InvalidCudaState;
    return @intCast(value);
}

fn checkF32Bytes(buffer: cuda_buffer.DeviceBuffer, count: usize) cuda_driver.Error!void {
    try checkRawBytes(buffer, try checkedMul(count, @sizeOf(f32)));
}

fn checkRawBytes(buffer: cuda_buffer.DeviceBuffer, byte_count: usize) cuda_driver.Error!void {
    if (byte_count > buffer.len) return error.InvalidCudaState;
}

fn fillInput(values: []f32) void {
    for (values, 0..) |*value, i| {
        const lane: f32 = @floatFromInt(i % 97);
        value.* = (lane - 48.0) * 0.005;
    }
}

fn fillBias(values: []f32) void {
    for (values, 0..) |*value, i| {
        const lane: f32 = @floatFromInt(i % 23);
        value.* = (lane - 11.0) * 0.001;
    }
}

fn fillQ4KWeights(bytes: []u8) void {
    var block_input: [q4_k_values_per_block]f32 = undefined;
    var offset: usize = 0;
    var block_index: usize = 0;
    while (offset < bytes.len) : ({
        offset += q4_k_block_bytes;
        block_index += 1;
    }) {
        for (&block_input, 0..) |*value, i| {
            const lane: f32 = @floatFromInt((i + block_index * 13) % 31);
            value.* = (lane - 15.0) * 0.01;
        }
        quant_codec.quantizeQ4_KBlock(&block_input, bytes[offset..][0..q4_k_block_bytes]);
    }
}

fn runFullTextEmbedBench(allocator: std.mem.Allocator, io: std.Io, cfg: Config, model_path: []const u8) !void {
    print("\nfull ClipCLAP text embed via termite embed --backend cuda: model={s} iters={d}\n", .{ model_path, cfg.full_iters });

    var total_ns: u64 = 0;
    for (0..cfg.full_iters) |iter| {
        const embed_args = [_][]const u8{
            model_path,
            "--backend",
            "cuda",
            "--text",
            cfg.text,
            "--print-timing",
        };
        const started = nowNs();
        try native_embed.main(allocator, io, &embed_args);
        const elapsed = nowNs() - started;
        total_ns += elapsed;
        print("full_text_embed_iter={d} elapsed_ms={d}\n", .{ iter, elapsed / std.time.ns_per_ms });
    }
    print("full_text_embed_avg_ms={d}\n", .{(total_ns / cfg.full_iters) / std.time.ns_per_ms});
}

fn nowNs() u64 {
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return 0,
    }
}
