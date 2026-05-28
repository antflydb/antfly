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

// CLIPCLAP native kernel micro-benchmark.
//
// Times the per-kernel hot paths used by the pure-Zig native compute backend
// for CLIP/CLAP/CLIPCLAP on representative shapes (CLIP-B-Patch16 sized).
// Each kernel ships in two flavours:
//
//   *Baseline   — the implementation as it stood before this optimization
//                 pass, copied verbatim so the bench is reproducible without
//                 needing git checkouts.
//   *Optimized  — the current production implementation, either pulled
//                 directly from `activations.zig` / `clip.zig` / `clap.zig`
//                 or inlined here when the production version lives inside
//                 a larger function.
//
// We report wall-clock ns/call for both, the speedup ratio, and a checksum
// so the optimizer can't notice the result is unused and elide the call.

const std = @import("std");
const activations = @import("../backends/activations.zig");
const native_backend = @import("../backends/native.zig");
const native_compute = @import("../ops/native_compute.zig");
const QuantizedStorage = @import("../models/weight_source.zig").QuantizedStorage;
const quant_codec = @import("../gguf/quant_codec.zig");
const tensor_types = @import("../gguf/tensor_types.zig");
const linalg = @import("termite_linalg");

const VEC_LEN = 8;
const F32xN = @Vector(VEC_LEN, f32);

const quant_parity_kinds = [_]tensor_types.KnownTensorType{
    .Q1_0,
    .Q4_0,
    .Q4_1,
    .Q5_0,
    .Q5_1,
    .Q8_0,
    .Q8_1,
    .Q2_K,
    .Q3_K,
    .Q4_K,
    .Q5_K,
    .Q6_K,
    .Q8_K,
};

const quant_policy_kinds = quant_parity_kinds;

// --- CLI ----------------------------------------------------------------

const BenchConfig = struct {
    warmup_iters: usize = 5,
    measure_iters: usize = 50,
    only_gliner_quant: bool = false,
    only_clipclap_quant_policy: bool = false,
    only_clipclap_audio_quant: bool = false,
    only_native_quant_buckets: bool = false,
    only_packed_qkv: bool = false,
    include_direct_variants: bool = false,
    filter_kind: ?tensor_types.KnownTensorType = null,
    filter_kinds: [64]bool = [_]bool{false} ** 64,
    has_filter_kinds: bool = false,
    filter_rows: ?usize = null,
    filter_in_dim: ?usize = null,
    filter_out_dim: ?usize = null,
    io: ?std.Io = null,
};

fn hasCompleteShapeFilter(cfg: BenchConfig) bool {
    return cfg.filter_rows != null and
        cfg.filter_in_dim != null and
        cfg.filter_out_dim != null;
}

fn filterMatchesKind(cfg: BenchConfig, kind: tensor_types.KnownTensorType) bool {
    if (cfg.filter_kind == null and !cfg.has_filter_kinds) return true;
    if (cfg.filter_kind) |value| {
        if (value == kind) return true;
    }
    const idx: usize = @intFromEnum(kind);
    return idx < cfg.filter_kinds.len and cfg.filter_kinds[idx];
}

fn filterMatchesShape(cfg: BenchConfig, rows: usize, in_dim: usize, out_dim: usize) bool {
    if (cfg.filter_rows) |value| {
        if (rows != value) return false;
    }
    if (cfg.filter_in_dim) |value| {
        if (in_dim != value) return false;
    }
    if (cfg.filter_out_dim) |value| {
        if (out_dim != value) return false;
    }
    return true;
}

fn parseBenchKind(value: []const u8) !tensor_types.KnownTensorType {
    inline for (@typeInfo(tensor_types.KnownTensorType).@"enum".fields) |field| {
        if (std.mem.eql(u8, value, field.name)) {
            return @enumFromInt(field.value);
        }
    }
    return error.InvalidArgument;
}

fn addBenchKindFilter(cfg: *BenchConfig, value: []const u8) !void {
    const kind = try parseBenchKind(value);
    const idx: usize = @intFromEnum(kind);
    if (idx >= cfg.filter_kinds.len) return error.InvalidArgument;
    cfg.filter_kinds[idx] = true;
    cfg.has_filter_kinds = true;
}

fn parseBenchKindList(cfg: *BenchConfig, value: []const u8) !void {
    var iter = std.mem.splitScalar(u8, value, ',');
    var saw_kind = false;
    while (iter.next()) |part| {
        if (part.len == 0) return error.InvalidArgument;
        try addBenchKindFilter(cfg, part);
        saw_kind = true;
    }
    if (!saw_kind) return error.InvalidArgument;
}

fn parseArgs(init: std.process.Init) !BenchConfig {
    var cfg = BenchConfig{};
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--warmup-iters")) {
            cfg.warmup_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            cfg.measure_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--only-gliner-quant")) {
            cfg.only_gliner_quant = true;
        } else if (std.mem.eql(u8, arg, "--only-clipclap-quant-policy")) {
            cfg.only_clipclap_quant_policy = true;
        } else if (std.mem.eql(u8, arg, "--only-clipclap-audio-quant")) {
            cfg.only_clipclap_audio_quant = true;
        } else if (std.mem.eql(u8, arg, "--only-native-quant-buckets")) {
            cfg.only_native_quant_buckets = true;
        } else if (std.mem.eql(u8, arg, "--only-packed-qkv")) {
            cfg.only_packed_qkv = true;
        } else if (std.mem.eql(u8, arg, "--include-direct-variants")) {
            cfg.include_direct_variants = true;
        } else if (std.mem.eql(u8, arg, "--kind")) {
            cfg.filter_kind = try parseBenchKind(args_iter.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--types") or std.mem.eql(u8, arg, "--kinds")) {
            try parseBenchKindList(&cfg, args_iter.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--rows")) {
            cfg.filter_rows = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--in-dim")) {
            cfg.filter_in_dim = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--out-dim")) {
            cfg.filter_out_dim = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingValue, 10);
        } else {
            return error.InvalidArgument;
        }
    }
    return cfg;
}

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cfg = try parseArgs(init);
    cfg.io = init.io;
    cfg.include_direct_variants = cfg.include_direct_variants or
        cfg.only_clipclap_audio_quant or
        cfg.only_packed_qkv;

    std.debug.print("CLIPCLAP kernel microbench (warmup={d}, measure={d})\n", .{ cfg.warmup_iters, cfg.measure_iters });
    printHeader(cfg.only_gliner_quant or cfg.only_clipclap_quant_policy);
    if (cfg.only_gliner_quant) {
        try benchQuantizedGlinerKernelCompare(allocator, cfg);
        return;
    }
    if (cfg.only_clipclap_quant_policy) {
        try benchQuantizedClipClapPolicyCompare(allocator, cfg);
        return;
    }
    if (cfg.only_clipclap_audio_quant) {
        try benchQuantizedClipClapAudioCompare(allocator, cfg);
        return;
    }
    if (cfg.only_native_quant_buckets) {
        try benchNativeQuantBuckets(allocator, cfg);
        return;
    }
    if (cfg.only_packed_qkv) {
        try benchPackedQkvDirectCompare(allocator, cfg);
        return;
    }

    try benchSoftmax(allocator, cfg, "softmax CLIP-text 12x77x77 attn", 12 * 77, 77);
    try benchSoftmax(allocator, cfg, "softmax CLIP-vision 12x257x257 attn", 12 * 257, 257);

    try benchLayerNorm(allocator, cfg, "layerNorm CLIP-text 12x(2x77) H=768", 12 * 2 * 77, 768);
    try benchLayerNorm(allocator, cfg, "layerNorm CLIP-vision 12x(2x257) H=768", 12 * 2 * 257, 768);

    try benchLogSoftmax(allocator, cfg, "logSoftmax 32x257 H=257", 32, 257);

    try benchEmbeddingLookupBf16(allocator, cfg, "embeddingLookup bf16 vocab=49408 H=768 ids=77", 49408, 768, 77);
    try benchEmbeddingLookupF16(allocator, cfg, "embeddingLookup f16  vocab=49408 H=768 ids=77", 49408, 768, 77);

    try benchPatchExtract(allocator, cfg, "patchExtract CLIP-B-P16 224x224 b=1", 1, 224, 16);
    try benchPatchExtract(allocator, cfg, "patchExtract CLIP-L-P14 224x224 b=4", 4, 224, 14);

    try benchWindowPartition(allocator, cfg, "windowPartition CLAP 64x64 ws=8 dim=128", 1, 64, 64, 128, 8);
    try benchCyclicShift(allocator, cfg, "cyclicShift CLAP 64x64 ws/2=4 dim=128", 1, 64, 64, 128, 4);
    try benchAvgPool(allocator, cfg, "avgPoolTokens CLAP 64x64 H=768 b=1", 1, 64 * 64, 768);

    // Flash-attention end-to-end: a single block of streaming softmax + V
    // projection on CLIP-B-sized shapes.  Compares scalar-@exp baseline vs
    // the new linalg primitives.expSubtractAndSum.
    try benchFlashSoftmaxBlock(allocator, cfg, "flash softmax block bkv=77 head_dim=64 (CLIP-text)", 64, 77, 64);
    try benchFlashSoftmaxBlock(allocator, cfg, "flash softmax block bkv=257 head_dim=64 (CLIP-vis)", 64, 257, 64);
    try benchFlashSoftmaxBlock(allocator, cfg, "flash softmax block bkv=512 head_dim=64 (BERT-base)", 64, 512, 64);

    // SGEMM register-tile expansion (sgemmTransB) — old MR=4 NR=2 baseline
    // inlined here, compared against the production single-threaded path
    // which now uses the comptime-selected tile (MR=6 NR=2 on AVX-512/AVX2/
    // aarch64).  We bypass the threading dispatch by calling the production
    // kernel on shapes below `sgemm_thread_flops_threshold` (4M FMAs) so
    // both versions run on a single core.
    try benchSgemmTransB(allocator, cfg, "sgemmTransB 77x768x768 (CLIP-B QKV/out proj)", 77, 768, 768);
    try benchSgemmTransB(allocator, cfg, "sgemmTransB 257x768x768 (CLIP-B vision proj)", 257, 768, 768);
    try benchSgemmTransB(allocator, cfg, "sgemmTransB 77x3072x768 (CLIP-B text MLP up)", 77, 3072, 768);
    try benchSgemmTransB(allocator, cfg, "sgemmTransB 77x768x3072 (CLIP-B text MLP down)", 77, 768, 3072);

    // Quantized linear: representative CLIP/CLAP projection shapes using the
    // same public native quant path GLiNER2 reaches through quantized_storage.
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q8_0 77x768x768 (CLIP text proj)", 77, 768, 768, .Q8_0);
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q4_K 77x768x768 (CLIP text proj)", 77, 768, 768, .Q4_K);
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q8_0 257x768x768 (CLIP vision proj)", 257, 768, 768, .Q8_0);
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q4_K 257x768x768 (CLIP vision proj)", 257, 768, 768, .Q4_K);
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q8_0 256x768x3072 (CLAP audio MLP up)", 256, 768, 3072, .Q8_0);
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q4_K 256x768x3072 (CLAP audio MLP up)", 256, 768, 3072, .Q4_K);
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q8_0 1x768x768 (pooled projection)", 1, 768, 768, .Q8_0);
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q4_K 1x768x768 (pooled projection)", 1, 768, 768, .Q4_K);
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q8_0 1x768x3072 (small-row MLP up)", 1, 768, 3072, .Q8_0);
    try benchQuantizedLinear(allocator, cfg, "quantLinear Q4_K 1x768x3072 (small-row MLP up)", 1, 768, 3072, .Q4_K);
    try benchQuantizedLinearPair(allocator, cfg, "quantPair Q8_0 77x768x768 (CLIP text Q/K)", 77, 768, 768, .Q8_0);
    try benchQuantizedLinearPair(allocator, cfg, "quantPair Q4_K 77x768x768 (CLIP text Q/K)", 77, 768, 768, .Q4_K);
    try benchQuantizedLinearPair(allocator, cfg, "quantPair Q8_0 257x768x768 (CLIP vision Q/K)", 257, 768, 768, .Q8_0);
    try benchQuantizedLinearPair(allocator, cfg, "quantPair Q4_K 257x768x768 (CLIP vision Q/K)", 257, 768, 768, .Q4_K);
    try benchQuantizedLinearTriple(allocator, cfg, "quantTriple Q8_0 77x768x768 (CLIP text QKV)", 77, 768, 768, .Q8_0);
    try benchQuantizedLinearTriple(allocator, cfg, "quantTriple Q4_K 77x768x768 (CLIP text QKV)", 77, 768, 768, .Q4_K);
    try benchQuantizedLinearTriple(allocator, cfg, "quantTriple Q8_0 257x768x768 (CLIP vision QKV)", 257, 768, 768, .Q8_0);
    try benchQuantizedLinearTriple(allocator, cfg, "quantTriple Q4_K 257x768x768 (CLIP vision QKV)", 257, 768, 768, .Q4_K);
    try benchQuantizedGlinerKernelCompare(allocator, cfg);
    try benchQuantizedParitySweep(allocator, cfg);
    try benchQuantizedSmallRowKSweep(allocator, cfg);

    // End-to-end flash attention: head-major (CLIP vision / BERT) and
    // token-major (CLIP text causal) layouts.  Compares scalar-dotPtrs/axpy
    // baseline against the GEMM-based production paths.
    try benchFlashAttn(allocator, cfg, "flash attn token-major seq=77 h=12 d=64 (CLIP-text causal)", 1, 77, 12, 64, true);
    try benchFlashAttn(allocator, cfg, "flash attn head-major seq=256 h=12 d=64", 1, 256, 12, 64, false);
    try benchFlashAttn(allocator, cfg, "flash attn head-major seq=257 h=12 d=64 (CLIP-vision)", 1, 257, 12, 64, false);
    try benchFlashAttn(allocator, cfg, "flash attn head-major seq=512 h=12 d=64 (BERT-base)", 1, 512, 12, 64, false);

    std.debug.print("\n", .{});
}

fn benchQuantizedGlinerKernelCompare(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
) !void {
    const shapes = [_]struct {
        label: []const u8,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    }{
        .{ .label = "36x768x768", .rows = 36, .in_dim = 768, .out_dim = 768 },
        .{ .label = "36x768x3072", .rows = 36, .in_dim = 768, .out_dim = 3072 },
        .{ .label = "36x3072x768", .rows = 36, .in_dim = 3072, .out_dim = 768 },
        .{ .label = "128x768x768", .rows = 128, .in_dim = 768, .out_dim = 768 },
        .{ .label = "128x768x3072", .rows = 128, .in_dim = 768, .out_dim = 3072 },
        .{ .label = "128x3072x768", .rows = 128, .in_dim = 3072, .out_dim = 768 },
    };
    for (quant_policy_kinds) |kind| {
        if (!filterMatchesKind(cfg, kind)) continue;
        const name = quantTypeName(kind);
        for (shapes) |shape| {
            if (!filterMatchesShape(cfg, shape.rows, shape.in_dim, shape.out_dim)) continue;
            const label = try std.fmt.allocPrint(allocator, "nativeGliner directQuant_vs_dequantSgemm {s} {s}", .{ name, shape.label });
            defer allocator.free(label);
            try benchQuantizedLinearDequantSgemm(allocator, cfg, label, shape.rows, shape.in_dim, shape.out_dim, kind);
            if (cfg.include_direct_variants and hasDirectVariants(kind)) {
                const variant_label = try std.fmt.allocPrint(allocator, "nativeGliner directVariants {s} {s}", .{ name, shape.label });
                defer allocator.free(variant_label);
                try benchQuantizedLinearDirectVariants(allocator, cfg, variant_label, shape.rows, shape.in_dim, shape.out_dim, kind);
            }
        }
    }
}

fn benchQuantizedClipClapPolicyCompare(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
) !void {
    const Shape = struct {
        label: []const u8,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    };
    const shapes = [_]Shape{
        .{ .label = "CLIP text hidden512 self-attn/out", .rows = 77, .in_dim = 512, .out_dim = 512 },
        .{ .label = "CLIP text hidden512 MLP up", .rows = 77, .in_dim = 512, .out_dim = 2048 },
        .{ .label = "CLIP text hidden512 MLP down", .rows = 77, .in_dim = 2048, .out_dim = 512 },
        .{ .label = "CLIP text hidden512 pooled proj", .rows = 1, .in_dim = 512, .out_dim = 512 },
        .{ .label = "CLIP/CLAP text hidden768 self-attn/out", .rows = 77, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLIP/CLAP text hidden768 MLP up", .rows = 77, .in_dim = 768, .out_dim = 3072 },
        .{ .label = "CLIP/CLAP text hidden768 MLP down", .rows = 77, .in_dim = 3072, .out_dim = 768 },
        .{ .label = "CLAP text max-pos self-attn/out", .rows = 514, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP text max-pos MLP up", .rows = 514, .in_dim = 768, .out_dim = 3072 },
        .{ .label = "CLAP text max-pos MLP down", .rows = 514, .in_dim = 3072, .out_dim = 768 },
        .{ .label = "CLIP vision P32 self-attn/out", .rows = 50, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLIP vision P16 self-attn/out", .rows = 197, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLIP vision P14 self-attn/out", .rows = 257, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLIP vision P14 MLP up", .rows = 257, .in_dim = 768, .out_dim = 3072 },
        .{ .label = "CLIP vision P14 MLP down", .rows = 257, .in_dim = 3072, .out_dim = 768 },
        .{ .label = "CLAP audio tiny stage self-attn/out", .rows = 64, .in_dim = 32, .out_dim = 32 },
        .{ .label = "CLAP audio tiny stage MLP up", .rows = 64, .in_dim = 32, .out_dim = 128 },
        .{ .label = "CLAP audio tiny stage MLP down", .rows = 64, .in_dim = 128, .out_dim = 32 },
        .{ .label = "CLAP audio small stage self-attn/out", .rows = 16, .in_dim = 64, .out_dim = 64 },
        .{ .label = "CLAP audio small stage MLP up", .rows = 16, .in_dim = 64, .out_dim = 256 },
        .{ .label = "CLAP audio small stage MLP down", .rows = 16, .in_dim = 256, .out_dim = 64 },
        .{ .label = "CLAP audio mid stage self-attn/out", .rows = 4, .in_dim = 128, .out_dim = 128 },
        .{ .label = "CLAP audio mid stage MLP up", .rows = 4, .in_dim = 128, .out_dim = 512 },
        .{ .label = "CLAP audio mid stage MLP down", .rows = 4, .in_dim = 512, .out_dim = 128 },
        .{ .label = "CLAP audio large stage self-attn/out", .rows = 1, .in_dim = 256, .out_dim = 256 },
        .{ .label = "CLAP audio large stage MLP up", .rows = 1, .in_dim = 256, .out_dim = 1024 },
        .{ .label = "CLAP audio large stage MLP down", .rows = 1, .in_dim = 1024, .out_dim = 256 },
    };
    if (hasCompleteShapeFilter(cfg)) {
        const rows = cfg.filter_rows.?;
        const in_dim = cfg.filter_in_dim.?;
        const out_dim = cfg.filter_out_dim.?;
        for (quant_policy_kinds) |kind| {
            if (!filterMatchesKind(cfg, kind)) continue;
            const tensor_type: tensor_types.TensorType = .{ .known = kind };
            const block_values = tensor_types.valuesPerBlock(tensor_type) orelse continue;
            if (in_dim % block_values != 0) continue;
            const label = try std.fmt.allocPrint(
                allocator,
                "clipclapPolicy {s} custom {d}x{d}x{d}",
                .{ quantTypeName(kind), rows, in_dim, out_dim },
            );
            defer allocator.free(label);
            try benchQuantizedLinearDequantSgemmIfSupported(allocator, cfg, label, rows, in_dim, out_dim, kind);
            if (hasDirectVariants(kind)) {
                const direct_label = try std.fmt.allocPrint(
                    allocator,
                    "clipclapDirectVariants {s} custom {d}x{d}x{d}",
                    .{ quantTypeName(kind), rows, in_dim, out_dim },
                );
                defer allocator.free(direct_label);
                try benchQuantizedLinearDirectVariants(allocator, cfg, direct_label, rows, in_dim, out_dim, kind);
            }
        }
        return;
    }

    for (shapes) |shape| {
        if (!filterMatchesShape(cfg, shape.rows, shape.in_dim, shape.out_dim)) continue;
        for (quant_policy_kinds) |kind| {
            if (!filterMatchesKind(cfg, kind)) continue;
            const tensor_type: tensor_types.TensorType = .{ .known = kind };
            const block_values = tensor_types.valuesPerBlock(tensor_type) orelse continue;
            if (shape.in_dim % block_values != 0) continue;
            const label = try std.fmt.allocPrint(
                allocator,
                "clipclapPolicy {s} {s} {d}x{d}x{d}",
                .{ quantTypeName(kind), shape.label, shape.rows, shape.in_dim, shape.out_dim },
            );
            defer allocator.free(label);
            try benchQuantizedLinearDequantSgemmIfSupported(allocator, cfg, label, shape.rows, shape.in_dim, shape.out_dim, kind);
            if (hasDirectVariants(kind)) {
                const direct_label = try std.fmt.allocPrint(
                    allocator,
                    "clipclapDirectVariants {s} {s} {d}x{d}x{d}",
                    .{ quantTypeName(kind), shape.label, shape.rows, shape.in_dim, shape.out_dim },
                );
                defer allocator.free(direct_label);
                try benchQuantizedLinearDirectVariants(allocator, cfg, direct_label, shape.rows, shape.in_dim, shape.out_dim, kind);
            }
        }
    }
}

fn benchQuantizedClipClapAudioCompare(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
) !void {
    const LinearShape = struct {
        label: []const u8,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    };
    const linear_shapes = [_]LinearShape{
        .{ .label = "CLAP audio encoder short self-attn/out", .rows = 64, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio encoder short MLP up", .rows = 64, .in_dim = 768, .out_dim = 3072 },
        .{ .label = "CLAP audio encoder short MLP down", .rows = 64, .in_dim = 3072, .out_dim = 768 },
        .{ .label = "CLAP audio encoder mid self-attn/out", .rows = 128, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio encoder mid MLP up", .rows = 128, .in_dim = 768, .out_dim = 3072 },
        .{ .label = "CLAP audio encoder mid MLP down", .rows = 128, .in_dim = 3072, .out_dim = 768 },
        .{ .label = "CLAP audio encoder long self-attn/out", .rows = 256, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio encoder long MLP up", .rows = 256, .in_dim = 768, .out_dim = 3072 },
        .{ .label = "CLAP audio encoder long MLP down", .rows = 256, .in_dim = 3072, .out_dim = 768 },
        .{ .label = "CLAP audio pooled hidden768 projection", .rows = 1, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio pooled hidden256 projection", .rows = 1, .in_dim = 256, .out_dim = 256 },
    };
    const qkv_shapes = [_]LinearShape{
        .{ .label = "CLAP audio encoder short QKV", .rows = 64, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio encoder mid QKV", .rows = 128, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio encoder long QKV", .rows = 256, .in_dim = 768, .out_dim = 768 },
    };
    if (hasCompleteShapeFilter(cfg)) {
        const rows = cfg.filter_rows.?;
        const in_dim = cfg.filter_in_dim.?;
        const out_dim = cfg.filter_out_dim.?;
        for (quant_policy_kinds) |kind| {
            if (!filterMatchesKind(cfg, kind)) continue;
            const tensor_type: tensor_types.TensorType = .{ .known = kind };
            const block_values = tensor_types.valuesPerBlock(tensor_type) orelse continue;
            if (in_dim % block_values != 0) continue;

            const base_label = try std.fmt.allocPrint(
                allocator,
                "clipclapAudio directQuant_vs_dequantSgemm {s} custom {d}x{d}x{d}",
                .{ quantTypeName(kind), rows, in_dim, out_dim },
            );
            defer allocator.free(base_label);
            try benchQuantizedLinearDequantSgemmIfSupported(allocator, cfg, base_label, rows, in_dim, out_dim, kind);

            if (hasDirectVariants(kind)) {
                const variant_label = try std.fmt.allocPrint(
                    allocator,
                    "clipclapAudio directVariants {s} custom {d}x{d}x{d}",
                    .{ quantTypeName(kind), rows, in_dim, out_dim },
                );
                defer allocator.free(variant_label);
                try benchQuantizedLinearDirectVariants(allocator, cfg, variant_label, rows, in_dim, out_dim, kind);
            }

            const qkv_label = try std.fmt.allocPrint(
                allocator,
                "clipclapAudio packedQKV {s} custom {d}x{d}x{d}",
                .{ quantTypeName(kind), rows, in_dim, out_dim },
            );
            defer allocator.free(qkv_label);
            try benchQuantizedLinearTriple(allocator, cfg, qkv_label, rows, in_dim, out_dim, kind);
        }
        return;
    }

    for (quant_policy_kinds) |kind| {
        if (!filterMatchesKind(cfg, kind)) continue;
        for (linear_shapes) |shape| {
            if (!filterMatchesShape(cfg, shape.rows, shape.in_dim, shape.out_dim)) continue;
            const tensor_type: tensor_types.TensorType = .{ .known = kind };
            const block_values = tensor_types.valuesPerBlock(tensor_type) orelse continue;
            if (shape.in_dim % block_values != 0) continue;

            const base_label = try std.fmt.allocPrint(
                allocator,
                "clipclapAudio directQuant_vs_dequantSgemm {s} {s} {d}x{d}x{d}",
                .{ quantTypeName(kind), shape.label, shape.rows, shape.in_dim, shape.out_dim },
            );
            defer allocator.free(base_label);
            try benchQuantizedLinearDequantSgemmIfSupported(allocator, cfg, base_label, shape.rows, shape.in_dim, shape.out_dim, kind);

            if (hasDirectVariants(kind)) {
                const variant_label = try std.fmt.allocPrint(
                    allocator,
                    "clipclapAudio directVariants {s} {s} {d}x{d}x{d}",
                    .{ quantTypeName(kind), shape.label, shape.rows, shape.in_dim, shape.out_dim },
                );
                defer allocator.free(variant_label);
                try benchQuantizedLinearDirectVariants(allocator, cfg, variant_label, shape.rows, shape.in_dim, shape.out_dim, kind);
            }
        }

        for (qkv_shapes) |shape| {
            if (!filterMatchesShape(cfg, shape.rows, shape.in_dim, shape.out_dim)) continue;
            const tensor_type: tensor_types.TensorType = .{ .known = kind };
            const block_values = tensor_types.valuesPerBlock(tensor_type) orelse continue;
            if (shape.in_dim % block_values != 0) continue;

            const label = try std.fmt.allocPrint(
                allocator,
                "clipclapAudio packedQKV {s} {s} {d}x{d}x{d}",
                .{ quantTypeName(kind), shape.label, shape.rows, shape.in_dim, shape.out_dim },
            );
            defer allocator.free(label);
            try benchQuantizedLinearTriple(allocator, cfg, label, shape.rows, shape.in_dim, shape.out_dim, kind);
        }
    }
}

fn printHeader(gliner_quant_only: bool) void {
    if (gliner_quant_only) {
        std.debug.print("\n{s:<58} {s:>16} {s:>18} {s:>16} {s:>14}\n", .{ "kernel", "direct quant ns", "dequant SGEMM ns", "dequant speedup", "checksum" });
        std.debug.print("{s:<58} {s:>16} {s:>18} {s:>16} {s:>14}\n", .{ "------", "---------------", "----------------", "------------", "--------" });
        return;
    }
    std.debug.print("\n{s:<58} {s:>16} {s:>18} {s:>16} {s:>14}\n", .{ "kernel", "baseline ns/it", "opt ns/it", "speedup", "checksum" });
    std.debug.print("{s:<58} {s:>16} {s:>18} {s:>16} {s:>14}\n", .{ "------", "--------------", "----------", "-------", "--------" });
}

fn printRow(label: []const u8, base_ns: u64, opt_ns: u64, checksum: f64) void {
    const speedup = if (opt_ns == 0) 0.0 else @as(f64, @floatFromInt(base_ns)) / @as(f64, @floatFromInt(opt_ns));
    std.debug.print("{s:<58} {d:>16} {d:>18} {d:>15.2}x {d:>14.4}\n", .{ label, base_ns, opt_ns, speedup, checksum });
}

fn averageNativeQuantPhaseStats(stats: native_compute.NativeQuantDispatchStats, iterations: usize) native_compute.NativeQuantDispatchStats {
    var averaged = stats;
    const divisor = @max(iterations, 1);
    averaged.q8k_activation_alloc_ns /= divisor;
    averaged.q8k_activation_quant_ns /= divisor;
    averaged.q8_0_activation_alloc_ns /= divisor;
    averaged.q8_0_activation_quant_ns /= divisor;
    averaged.q8_0_compute_ns /= divisor;
    averaged.legacy_activation_alloc_ns /= divisor;
    averaged.legacy_activation_quant_ns /= divisor;
    averaged.legacy_compute_ns /= divisor;
    averaged.q4q5_q8k_compute_ns /= divisor;
    averaged.q4q5_q8k_pair_compute_ns /= divisor;
    averaged.q4q5_q8k_triple_compute_ns /= divisor;
    averaged.dequant_fetch_ns /= divisor;
    averaged.dequant_sgemm_compute_ns /= divisor;
    return averaged;
}

fn printQuantTriplePhaseRow(label: []const u8, stats: native_compute.NativeQuantDispatchStats, measured_ns: u64) void {
    std.debug.print(
        "quantTriplePhase {s} measured_ns={} q8k_alloc_ns={} q8k_quant_ns={} q4q5_compute_ns={} q4q5_pair_compute_ns={} q4q5_triple_compute_ns={} dequant_fetch_ns={} dequant_sgemm_compute_ns={} packed_mr8={} packed_mr4={} packed_mr2={} packed_mr1={}\n",
        .{
            label,
            measured_ns,
            stats.q8k_activation_alloc_ns,
            stats.q8k_activation_quant_ns,
            stats.q4q5_q8k_compute_ns,
            stats.q4q5_q8k_pair_compute_ns,
            stats.q4q5_q8k_triple_compute_ns,
            stats.dequant_fetch_ns,
            stats.dequant_sgemm_compute_ns,
            stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr8,
            stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4,
            stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2,
            stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr1,
        },
    );
}

fn printQuantLinearPhaseRow(label: []const u8, stats: native_compute.NativeQuantDispatchStats, measured_ns: u64) void {
    std.debug.print(
        "quantLinearPhase {s} measured_ns={} q8k_alloc_ns={} q8k_quant_ns={} q8_0_alloc_ns={} q8_0_quant_ns={} q8_0_compute_ns={} legacy_alloc_ns={} legacy_quant_ns={} legacy_compute_ns={} q4q5_compute_ns={} dequant_fetch_ns={} dequant_sgemm_compute_ns={}\n",
        .{
            label,
            measured_ns,
            stats.q8k_activation_alloc_ns,
            stats.q8k_activation_quant_ns,
            stats.q8_0_activation_alloc_ns,
            stats.q8_0_activation_quant_ns,
            stats.q8_0_compute_ns,
            stats.legacy_activation_alloc_ns,
            stats.legacy_activation_quant_ns,
            stats.legacy_compute_ns,
            stats.q4q5_q8k_compute_ns,
            stats.dequant_fetch_ns,
            stats.dequant_sgemm_compute_ns,
        },
    );
}

fn nowNs() u64 {
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return 0,
    }
}

fn fillRandom(rand: std.Random, dst: []f32) void {
    for (dst) |*v| v.* = (rand.float(f32) - 0.5) * 6.0;
}

/// Fold a single f32 sample into the running f64 checksum, ignoring NaN
/// so a rare FP edge case (random Q/K/V occasionally producing NaN
/// through underflow + rescale + division) doesn't poison the column for
/// every subsequent iteration of the bench.  Bench correctness comes from
/// the kernel parity tests, not the checksum -- this just keeps the
/// optimizer from eliding the call.
inline fn foldChecksum(checksum: *f64, sample: f32) void {
    if (!std.math.isNan(sample)) checksum.* += @floatCast(sample);
}

/// Run a single kernel `step` (which encompasses any per-iter setup plus
/// the kernel call) under the standard warmup + measure schedule, return
/// median-ish ns-per-iter.  `args` is the tuple of arguments forwarded to
/// `step`.  After the timing window, `checksum_byte.*` is folded into
/// `checksum.*` so the optimizer can't elide the kernel call.
fn timeStep(
    cfg: BenchConfig,
    comptime step: anytype,
    args: anytype,
    checksum_byte: *const f32,
    checksum: *f64,
) u64 {
    for (0..cfg.warmup_iters) |_| @call(.auto, step, args);
    var ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        @call(.auto, step, args);
        ns += nowNs() - t;
        foldChecksum(checksum, checksum_byte.*);
    }
    return ns / cfg.measure_iters;
}

/// Run baseline and optimized kernel steps with the same warmup + measure
/// schedule, then print the comparison row.  See `timeStep` for the
/// `step` contract.  Helps every bench function shed the per-bench
/// duplication of the warmup/measure/timer pattern.
fn runPair(
    cfg: BenchConfig,
    label: []const u8,
    comptime baseline_step: anytype,
    comptime opt_step: anytype,
    args: anytype,
    checksum_byte: *const f32,
    checksum: *f64,
) void {
    const base_ns = timeStep(cfg, baseline_step, args, checksum_byte, checksum);
    const opt_ns = timeStep(cfg, opt_step, args, checksum_byte, checksum);
    printRow(label, base_ns, opt_ns, checksum.*);
}

/// Like `timeStep` but runs `setup` outside the measured window before
/// every iteration.  Use for kernels that destructively transform their
/// inputs (softmax / layerNorm / logSoftmax) so the per-iter buffer reset
/// doesn't pollute the kernel timing.
fn timeStepWithSetup(
    cfg: BenchConfig,
    comptime setup: anytype,
    setup_args: anytype,
    comptime kernel: anytype,
    kernel_args: anytype,
    checksum_byte: *const f32,
    checksum: *f64,
) u64 {
    for (0..cfg.warmup_iters) |_| {
        @call(.auto, setup, setup_args);
        @call(.auto, kernel, kernel_args);
    }
    var ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        @call(.auto, setup, setup_args);
        const t = nowNs();
        @call(.auto, kernel, kernel_args);
        ns += nowNs() - t;
        foldChecksum(checksum, checksum_byte.*);
    }
    return ns / cfg.measure_iters;
}

/// `runPair` for kernels that need a per-iteration setup step (typically
/// `@memcpy(work, seed)` for kernels that mutate their input).  The
/// setup runs outside the measured window so it doesn't bias either side.
fn runPairWithSetup(
    cfg: BenchConfig,
    label: []const u8,
    comptime setup: anytype,
    setup_args: anytype,
    comptime baseline_kernel: anytype,
    comptime opt_kernel: anytype,
    kernel_args: anytype,
    checksum_byte: *const f32,
    checksum: *f64,
) void {
    const base_ns = timeStepWithSetup(cfg, setup, setup_args, baseline_kernel, kernel_args, checksum_byte, checksum);
    const opt_ns = timeStepWithSetup(cfg, setup, setup_args, opt_kernel, kernel_args, checksum_byte, checksum);
    printRow(label, base_ns, opt_ns, checksum.*);
}

/// Standard "memcpy work ← seed" setup for kernels that mutate the work
/// buffer.  Used by softmax / layerNorm / logSoftmax benches.
fn copyWorkBuf(work: []f32, seed: []const f32) void {
    @memcpy(work, seed);
}

// --- Softmax ------------------------------------------------------------

fn softmaxBaseline(data: []f32, dim: usize) void {
    // The pre-optimization version: vectorized but uses scalar @exp(@Vector)
    // which lowers to per-lane libm expf.
    const batch = data.len / dim;
    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];
        var max_val: f32 = -std.math.inf(f32);
        for (row) |v| max_val = @max(max_val, v);
        const max_splat: F32xN = @splat(max_val);
        var sum: f32 = 0.0;
        var i: usize = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            const e = @exp(v - max_splat);
            row[i..][0..VEC_LEN].* = e;
            sum += @reduce(.Add, e);
        }
        while (i < dim) : (i += 1) {
            row[i] = @exp(row[i] - max_val);
            sum += row[i];
        }
        if (sum > 0.0) {
            const inv_sum: F32xN = @splat(1.0 / sum);
            i = 0;
            while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
                const v: F32xN = row[i..][0..VEC_LEN].*;
                row[i..][0..VEC_LEN].* = v * inv_sum;
            }
            const inv_sum_scalar = 1.0 / sum;
            while (i < dim) : (i += 1) row[i] *= inv_sum_scalar;
        }
    }
}

fn benchSoftmax(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, batch: usize, dim: usize) !void {
    const seed_buf = try allocator.alloc(f32, batch * dim);
    defer allocator.free(seed_buf);
    const work = try allocator.alloc(f32, batch * dim);
    defer allocator.free(work);
    var prng = std.Random.DefaultPrng.init(0xC0FE_5F71);
    fillRandom(prng.random(), seed_buf);

    var checksum: f64 = 0;
    runPairWithSetup(cfg, label, copyWorkBuf, .{ work, seed_buf }, softmaxBaseline, activations.softmax, .{ work, dim }, &work[0], &checksum);
}

// --- LayerNorm ----------------------------------------------------------

fn layerNormBaseline(data: []f32, gamma: []const f32, beta: []const f32, dim: usize, eps: f32) void {
    const batch = data.len / dim;
    const dim_f: f32 = @floatFromInt(dim);
    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];
        // First pass: mean.
        var sum_acc: F32xN = @splat(0.0);
        var i: usize = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            sum_acc += v;
        }
        var sum: f32 = @reduce(.Add, sum_acc);
        while (i < dim) : (i += 1) sum += row[i];
        const mean = sum / dim_f;
        // Second pass: variance.
        var variance: f32 = 0.0;
        const mean_splat: F32xN = @splat(mean);
        i = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            const d = v - mean_splat;
            variance += @reduce(.Add, d * d);
        }
        while (i < dim) : (i += 1) {
            const d = row[i] - mean;
            variance += d * d;
        }
        variance /= dim_f;
        const inv_std = 1.0 / @sqrt(variance + eps);
        const inv_std_splat: F32xN = @splat(inv_std);
        // Third pass: normalize.
        i = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            const g: F32xN = gamma[i..][0..VEC_LEN].*;
            const bt: F32xN = beta[i..][0..VEC_LEN].*;
            row[i..][0..VEC_LEN].* = g * (v - mean_splat) * inv_std_splat + bt;
        }
        while (i < dim) : (i += 1) {
            row[i] = gamma[i] * (row[i] - mean) * inv_std + beta[i];
        }
    }
}

fn benchLayerNorm(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, batch: usize, dim: usize) !void {
    const seed_buf = try allocator.alloc(f32, batch * dim);
    defer allocator.free(seed_buf);
    const work = try allocator.alloc(f32, batch * dim);
    defer allocator.free(work);
    const gamma = try allocator.alloc(f32, dim);
    defer allocator.free(gamma);
    const beta = try allocator.alloc(f32, dim);
    defer allocator.free(beta);
    var prng = std.Random.DefaultPrng.init(0xBEEF_F00D);
    fillRandom(prng.random(), seed_buf);
    for (gamma) |*g| g.* = 0.5 + prng.random().float(f32);
    for (beta) |*bt| bt.* = (prng.random().float(f32) - 0.5) * 0.5;

    var checksum: f64 = 0;
    runPairWithSetup(cfg, label, copyWorkBuf, .{ work, seed_buf }, layerNormBaseline, activations.layerNorm, .{ work, gamma, beta, dim, @as(f32, 1e-5) }, &work[0], &checksum);
}

// --- LogSoftmax ---------------------------------------------------------

fn logSoftmaxBaseline(data: []f32, dim: usize) void {
    const batch = data.len / dim;
    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];
        var max_val: f32 = -std.math.inf(f32);
        for (row) |v| max_val = @max(max_val, v);
        var sum_exp: f32 = 0;
        for (row) |v| sum_exp += @exp(v - max_val);
        const lse = @log(sum_exp);
        for (row) |*v| v.* = (v.* - max_val) - lse;
    }
}

fn benchLogSoftmax(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, batch: usize, dim: usize) !void {
    const seed_buf = try allocator.alloc(f32, batch * dim);
    defer allocator.free(seed_buf);
    const work = try allocator.alloc(f32, batch * dim);
    defer allocator.free(work);
    var prng = std.Random.DefaultPrng.init(0xC0DE_F00D);
    fillRandom(prng.random(), seed_buf);

    var checksum: f64 = 0;
    runPairWithSetup(cfg, label, copyWorkBuf, .{ work, seed_buf }, logSoftmaxBaseline, activations.logSoftmaxInPlace, .{ work, dim }, &work[0], &checksum);
}

// --- Embedding lookup ---------------------------------------------------

fn embedBf16Baseline(out: []f32, src_bytes: [*]const u8, ids: []const i64, dim: usize) void {
    for (0..ids.len) |i| {
        const idx: usize = @intCast(ids[i]);
        const row_offset = idx * dim * 2;
        for (0..dim) |j| {
            const offset = row_offset + j * 2;
            const bits: u16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
            out[i * dim + j] = @bitCast(@as(u32, bits) << 16);
        }
    }
}

fn embedBf16Optimized(out: []f32, src_bytes: [*]const u8, ids: []const i64, dim: usize) void {
    const VEC_BF16 = 8;
    const U32xV = @Vector(VEC_BF16, u32);
    const F32xV = @Vector(VEC_BF16, f32);
    const shift_v: U32xV = @splat(16);
    const src_bf16: [*]const u16 = @ptrCast(@alignCast(src_bytes));
    for (0..ids.len) |i| {
        const idx: usize = @intCast(ids[i]);
        const src_row = src_bf16[idx * dim ..][0..dim];
        const dst_row = out[i * dim ..][0..dim];
        var j: usize = 0;
        while (j + VEC_BF16 <= dim) : (j += VEC_BF16) {
            var bits: U32xV = undefined;
            inline for (0..VEC_BF16) |lane| bits[lane] = src_row[j + lane];
            const shifted = bits << shift_v;
            const f: F32xV = @bitCast(shifted);
            dst_row[j..][0..VEC_BF16].* = f;
        }
        while (j < dim) : (j += 1) {
            const bits: u32 = @as(u32, src_row[j]) << 16;
            dst_row[j] = @bitCast(bits);
        }
    }
}

fn embedF16Baseline(out: []f32, src_bytes: [*]const u8, ids: []const i64, dim: usize) void {
    for (0..ids.len) |i| {
        const idx: usize = @intCast(ids[i]);
        const row_offset = idx * dim * 2;
        for (0..dim) |j| {
            const offset = row_offset + j * 2;
            const half: f16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
            out[i * dim + j] = @floatCast(half);
        }
    }
}

fn embedF16Optimized(out: []f32, src_bytes: [*]const u8, ids: []const i64, dim: usize) void {
    const src_f16: [*]const f16 = @ptrCast(@alignCast(src_bytes));
    for (0..ids.len) |i| {
        const idx: usize = @intCast(ids[i]);
        const src_row = src_f16[idx * dim ..][0..dim];
        const dst_row = out[i * dim ..][0..dim];
        for (dst_row, src_row) |*o, h| o.* = @floatCast(h);
    }
}

fn benchEmbeddingLookupBf16(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, vocab: usize, dim: usize, n_ids: usize) !void {
    const tbl_bytes = try allocator.alignedAlloc(u8, .of(u16), vocab * dim * 2);
    defer allocator.free(tbl_bytes);
    var prng = std.Random.DefaultPrng.init(0xBF16_AAAA);
    // Fill with sane bf16 values (top 16 bits of normal-range f32) — random
    // bytes would produce NaN-soup that pollutes the checksum.
    const tbl_u16: [*]u16 = @ptrCast(@alignCast(tbl_bytes.ptr));
    for (0..vocab * dim) |i| {
        const f: f32 = (prng.random().float(f32) - 0.5) * 2.0;
        const bits: u32 = @bitCast(f);
        tbl_u16[i] = @intCast(bits >> 16);
    }
    const ids = try allocator.alloc(i64, n_ids);
    defer allocator.free(ids);
    for (ids) |*id| id.* = @intCast(prng.random().intRangeLessThan(usize, 0, vocab));
    const out = try allocator.alloc(f32, n_ids * dim);
    defer allocator.free(out);

    var checksum: f64 = 0;
    runPair(cfg, label, embedBf16Baseline, embedBf16Optimized, .{ out, tbl_bytes.ptr, ids, dim }, &out[0], &checksum);
}

fn benchEmbeddingLookupF16(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, vocab: usize, dim: usize, n_ids: usize) !void {
    const tbl_bytes = try allocator.alignedAlloc(u8, .of(f16), vocab * dim * 2);
    defer allocator.free(tbl_bytes);
    var prng = std.Random.DefaultPrng.init(0xF16_AAAAA);
    // Fill with reasonable f16 values (avoid NaN soup).
    const f16_view: [*]f16 = @ptrCast(@alignCast(tbl_bytes.ptr));
    for (0..vocab * dim) |i| f16_view[i] = @floatCast((prng.random().float(f32) - 0.5) * 2.0);
    const ids = try allocator.alloc(i64, n_ids);
    defer allocator.free(ids);
    for (ids) |*id| id.* = @intCast(prng.random().intRangeLessThan(usize, 0, vocab));
    const out = try allocator.alloc(f32, n_ids * dim);
    defer allocator.free(out);

    var checksum: f64 = 0;
    runPair(cfg, label, embedF16Baseline, embedF16Optimized, .{ out, tbl_bytes.ptr, ids, dim }, &out[0], &checksum);
}

// --- Patch extraction (CLIP vision) ------------------------------------

fn patchExtractBaseline(out: []f32, in: []const f32, batch: usize, img_size: usize, P: usize) void {
    const grid = img_size / P;
    const num_patches = grid * grid;
    const patch_dim = 3 * P * P;
    for (0..batch) |b| {
        for (0..grid) |ph| {
            for (0..grid) |pw| {
                const pidx = b * num_patches + ph * grid + pw;
                for (0..3) |ch| {
                    for (0..P) |y| {
                        for (0..P) |x| {
                            out[pidx * patch_dim + ch * P * P + y * P + x] =
                                in[b * 3 * img_size * img_size + ch * img_size * img_size + (ph * P + y) * img_size + (pw * P + x)];
                        }
                    }
                }
            }
        }
    }
}

fn patchExtractOptimized(out: []f32, in: []const f32, batch: usize, img_size: usize, P: usize) void {
    const grid = img_size / P;
    const num_patches = grid * grid;
    const patch_dim = 3 * P * P;
    const channel_stride = img_size * img_size;
    const image_stride = 3 * channel_stride;
    for (0..batch) |b| {
        const img_base = b * image_stride;
        for (0..grid) |ph| {
            const row_base = ph * P;
            for (0..grid) |pw| {
                const pidx = b * num_patches + ph * grid + pw;
                const patch_base = pidx * patch_dim;
                const col_base = pw * P;
                for (0..3) |ch| {
                    const ch_dst_base = patch_base + ch * P * P;
                    const ch_src_base = img_base + ch * channel_stride;
                    for (0..P) |y| {
                        const dst = out[ch_dst_base + y * P ..][0..P];
                        const src = in[ch_src_base + (row_base + y) * img_size + col_base ..][0..P];
                        @memcpy(dst, src);
                    }
                }
            }
        }
    }
}

fn benchPatchExtract(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, batch: usize, img_size: usize, P: usize) !void {
    const grid = img_size / P;
    const num_patches = grid * grid;
    const patch_dim = 3 * P * P;
    const in = try allocator.alloc(f32, batch * 3 * img_size * img_size);
    defer allocator.free(in);
    const out = try allocator.alloc(f32, batch * num_patches * patch_dim);
    defer allocator.free(out);
    var prng = std.Random.DefaultPrng.init(0xCAFE_BABE);
    for (in) |*v| v.* = prng.random().float(f32);

    var checksum: f64 = 0;
    runPair(cfg, label, patchExtractBaseline, patchExtractOptimized, .{ out, in, batch, img_size, P }, &out[0], &checksum);
}

// --- CLAP audio: window partition, cyclic shift, avg pool --------------

fn windowPartitionBaseline(out: []f32, in: []const f32, batch: usize, height: usize, width: usize, dim: usize, ws: usize) void {
    const wh = height / ws;
    const ww_n = width / ws;
    const window_area = ws * ws;
    var window_index: usize = 0;
    for (0..batch) |b| {
        for (0..wh) |whi| {
            for (0..ww_n) |wwi| {
                for (0..ws) |dy| {
                    for (0..ws) |dx| {
                        const src_y = whi * ws + dy;
                        const src_x = wwi * ws + dx;
                        const src_base = ((b * height * width + src_y * width + src_x) * dim);
                        const dst_base = ((window_index * window_area + dy * ws + dx) * dim);
                        @memcpy(out[dst_base..][0..dim], in[src_base..][0..dim]);
                    }
                }
                window_index += 1;
            }
        }
    }
}

fn windowPartitionOptimized(out: []f32, in: []const f32, batch: usize, height: usize, width: usize, dim: usize, ws: usize) void {
    const wh = height / ws;
    const ww_n = width / ws;
    const window_area = ws * ws;
    const row_floats = ws * dim;
    var window_index: usize = 0;
    for (0..batch) |b| {
        for (0..wh) |whi| {
            for (0..ww_n) |wwi| {
                for (0..ws) |dy| {
                    const src_y = whi * ws + dy;
                    const src_x_start = wwi * ws;
                    const src_off = (b * height * width + src_y * width + src_x_start) * dim;
                    const dst_off = (window_index * window_area + dy * ws) * dim;
                    @memcpy(out[dst_off..][0..row_floats], in[src_off..][0..row_floats]);
                }
                window_index += 1;
            }
        }
    }
}

fn benchWindowPartition(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, batch: usize, height: usize, width: usize, dim: usize, ws: usize) !void {
    const total = batch * height * width * dim;
    const in = try allocator.alloc(f32, total);
    defer allocator.free(in);
    const out = try allocator.alloc(f32, total);
    defer allocator.free(out);
    var prng = std.Random.DefaultPrng.init(0xFEED_FACE);
    for (in) |*v| v.* = prng.random().float(f32);

    var checksum: f64 = 0;
    runPair(cfg, label, windowPartitionBaseline, windowPartitionOptimized, .{ out, in, batch, height, width, dim, ws }, &out[0], &checksum);
}

fn cyclicShiftBaseline(out: []f32, in: []const f32, batch: usize, height: usize, width: usize, dim: usize, shift: usize) void {
    for (0..batch) |b| {
        for (0..height) |y| {
            for (0..width) |x| {
                const src_y = (y + shift) % height;
                const src_x = (x + shift) % width;
                const dst_base = ((b * height * width + y * width + x) * dim);
                const src_base = ((b * height * width + src_y * width + src_x) * dim);
                @memcpy(out[dst_base..][0..dim], in[src_base..][0..dim]);
            }
        }
    }
}

fn cyclicShiftOptimized(out: []f32, in: []const f32, batch: usize, height: usize, width: usize, dim: usize, shift: usize) void {
    if (shift == 0) {
        @memcpy(out, in);
        return;
    }
    const head_x = width - shift;
    const head_floats = head_x * dim;
    const tail_floats = shift * dim;
    for (0..batch) |b| {
        for (0..height) |y| {
            const src_y = (y + shift) % height;
            const row_dst_off = (b * height * width + y * width) * dim;
            const row_src_off = (b * height * width + src_y * width) * dim;
            @memcpy(out[row_dst_off..][0..head_floats], in[row_src_off + shift * dim ..][0..head_floats]);
            @memcpy(out[row_dst_off + head_floats ..][0..tail_floats], in[row_src_off..][0..tail_floats]);
        }
    }
}

fn benchCyclicShift(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, batch: usize, height: usize, width: usize, dim: usize, shift: usize) !void {
    const total = batch * height * width * dim;
    const in = try allocator.alloc(f32, total);
    defer allocator.free(in);
    const out = try allocator.alloc(f32, total);
    defer allocator.free(out);
    var prng = std.Random.DefaultPrng.init(0xDEAD_F00D);
    for (in) |*v| v.* = prng.random().float(f32);

    var checksum: f64 = 0;
    runPair(cfg, label, cyclicShiftBaseline, cyclicShiftOptimized, .{ out, in, batch, height, width, dim, shift }, &out[0], &checksum);
}

fn avgPoolBaseline(out: []f32, in: []const f32, batch: usize, seq_len: usize, dim: usize) void {
    @memset(out, 0);
    const scale = 1.0 / @as(f32, @floatFromInt(seq_len));
    for (0..batch) |b| {
        for (0..seq_len) |t| {
            const src = (b * seq_len + t) * dim;
            const dst = b * dim;
            for (0..dim) |i| out[dst + i] += in[src + i] * scale;
        }
    }
}

fn avgPoolOptimized(out: []f32, in: []const f32, batch: usize, seq_len: usize, dim: usize) void {
    @memset(out, 0);
    const scale = 1.0 / @as(f32, @floatFromInt(seq_len));
    const VEC = 8;
    const Vec = @Vector(VEC, f32);
    for (0..batch) |b| {
        const dst = out[b * dim ..][0..dim];
        for (0..seq_len) |t| {
            const src = in[(b * seq_len + t) * dim ..][0..dim];
            var i: usize = 0;
            while (i + VEC <= dim) : (i += VEC) {
                const dv: Vec = dst[i..][0..VEC].*;
                const sv: Vec = src[i..][0..VEC].*;
                dst[i..][0..VEC].* = dv + sv;
            }
            while (i < dim) : (i += 1) dst[i] += src[i];
        }
        const scale_v: Vec = @splat(scale);
        var i: usize = 0;
        while (i + VEC <= dim) : (i += VEC) {
            const dv: Vec = dst[i..][0..VEC].*;
            dst[i..][0..VEC].* = dv * scale_v;
        }
        while (i < dim) : (i += 1) dst[i] *= scale;
    }
}

// --- End-to-end flash attention --------------------------------------

/// Token-major causal flash-attention baseline: scalar dotPtrs + per-row
/// axpy, mirrors the previous flashCausalAttentionHost body before the
/// GEMM rewrite.  Q/K/V layout is [batch, seq, num_heads * head_dim].
fn flashCausalAttnTokenMajorBaseline(
    alloc: std.mem.Allocator,
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    output: []f32,
) !void {
    const H = num_heads * head_dim;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const bq = @min(BLOCK_Q_BENCH, seq_len);
    const bkv = @min(BLOCK_KV_BENCH, seq_len);
    const score_tile = try alloc.alloc(f32, bq * bkv);
    defer alloc.free(score_tile);
    const row_max = try alloc.alloc(f32, bq);
    defer alloc.free(row_max);
    const row_sum = try alloc.alloc(f32, bq);
    defer alloc.free(row_sum);
    @memset(output, 0);

    for (0..batch) |b| {
        for (0..num_heads) |h| {
            var q_start: usize = 0;
            while (q_start < seq_len) {
                const q_end = @min(q_start + BLOCK_Q_BENCH, seq_len);
                const cur_bq = q_end - q_start;
                for (0..cur_bq) |r| {
                    row_max[r] = -std.math.inf(f32);
                    row_sum[r] = 0.0;
                }
                var kv_start: usize = 0;
                while (kv_start < seq_len) {
                    const kv_end = @min(kv_start + BLOCK_KV_BENCH, seq_len);
                    const cur_bkv = kv_end - kv_start;
                    for (0..cur_bq) |qi_local| {
                        const qi = q_start + qi_local;
                        const q_row = Q[(b * seq_len + qi) * H + h * head_dim ..][0..head_dim];
                        for (0..cur_bkv) |ki_local| {
                            const ki = kv_start + ki_local;
                            const k_row = K[(b * seq_len + ki) * H + h * head_dim ..][0..head_dim];
                            var dot: f32 = 0;
                            for (0..head_dim) |d| dot += q_row[d] * k_row[d];
                            score_tile[qi_local * cur_bkv + ki_local] = dot * scale;
                        }
                    }
                    for (0..cur_bq) |qi_local| {
                        const qi = q_start + qi_local;
                        for (0..cur_bkv) |ki_local| {
                            const ki = kv_start + ki_local;
                            if (ki > qi) score_tile[qi_local * cur_bkv + ki_local] = -std.math.inf(f32);
                        }
                    }
                    for (0..cur_bq) |qi_local| {
                        const qi = q_start + qi_local;
                        const out_ptr = output[(b * seq_len + qi) * H + h * head_dim ..][0..head_dim];
                        var block_max: f32 = -std.math.inf(f32);
                        for (0..cur_bkv) |ki_local| {
                            const s = score_tile[qi_local * cur_bkv + ki_local];
                            if (s > block_max) block_max = s;
                        }
                        const old_max = row_max[qi_local];
                        const new_max = @max(old_max, block_max);
                        if (new_max == -std.math.inf(f32)) continue;
                        if (row_sum[qi_local] != 0.0) {
                            const rescale = @exp(old_max - new_max);
                            if (rescale != 1.0) {
                                for (out_ptr) |*ov| ov.* *= rescale;
                                row_sum[qi_local] *= rescale;
                            }
                        }
                        var block_sum: f32 = 0;
                        for (0..cur_bkv) |ki_local| {
                            const s = score_tile[qi_local * cur_bkv + ki_local];
                            const w = @exp(s - new_max);
                            if (w == 0.0) continue;
                            block_sum += w;
                            const ki = kv_start + ki_local;
                            const v_row = V[(b * seq_len + ki) * H + h * head_dim ..][0..head_dim];
                            for (out_ptr, v_row) |*o, vv| o.* += w * vv;
                        }
                        row_max[qi_local] = new_max;
                        row_sum[qi_local] += block_sum;
                    }
                    kv_start = kv_end;
                }
                for (0..cur_bq) |qi_local| {
                    const qi = q_start + qi_local;
                    if (row_sum[qi_local] != 0.0) {
                        const inv = 1.0 / row_sum[qi_local];
                        const out_ptr = output[(b * seq_len + qi) * H + h * head_dim ..][0..head_dim];
                        for (out_ptr) |*ov| ov.* *= inv;
                    }
                }
                q_start = q_end;
            }
        }
    }
}

const BLOCK_Q_BENCH: usize = 64;
const BLOCK_KV_BENCH: usize = 256;

/// Pre-optimization head-major flash attention: per-Q-row scalar dotPtrs +
/// per-Q-row axpy V projection.  Mirrors the previous lib/linalg
/// flashAttentionHost body (sans bias/mask, which CLIP vision doesn't use).
fn flashAttnHeadMajorBaseline(
    allocator: std.mem.Allocator,
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    output: []f32,
) !void {
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const bq = @min(BLOCK_Q_BENCH, seq_len);
    const bkv = @min(BLOCK_KV_BENCH, seq_len);
    const score_tile = try allocator.alloc(f32, bq * bkv);
    defer allocator.free(score_tile);
    const row_max = try allocator.alloc(f32, bq);
    defer allocator.free(row_max);
    const row_sum = try allocator.alloc(f32, bq);
    defer allocator.free(row_sum);
    @memset(output, 0);

    for (0..batch) |b| {
        for (0..num_heads) |h| {
            const bh_offset = (b * num_heads + h) * seq_len * head_dim;
            var q_start: usize = 0;
            while (q_start < seq_len) {
                const q_end = @min(q_start + BLOCK_Q_BENCH, seq_len);
                const cur_bq = q_end - q_start;
                for (0..cur_bq) |r| {
                    row_max[r] = -std.math.inf(f32);
                    row_sum[r] = 0.0;
                }
                var kv_start: usize = 0;
                while (kv_start < seq_len) {
                    const kv_end = @min(kv_start + BLOCK_KV_BENCH, seq_len);
                    const cur_bkv = kv_end - kv_start;
                    for (0..cur_bq) |qi_local| {
                        const qi = q_start + qi_local;
                        const q_row = Q[bh_offset + qi * head_dim ..][0..head_dim];
                        for (0..cur_bkv) |ki_local| {
                            const ki = kv_start + ki_local;
                            const k_row = K[bh_offset + ki * head_dim ..][0..head_dim];
                            var dot: f32 = 0;
                            for (0..head_dim) |d| dot += q_row[d] * k_row[d];
                            score_tile[qi_local * cur_bkv + ki_local] = dot * scale;
                        }
                    }
                    for (0..cur_bq) |qi_local| {
                        const qi = q_start + qi_local;
                        const out_ptr = output[bh_offset + qi * head_dim ..][0..head_dim];
                        var block_max: f32 = -std.math.inf(f32);
                        for (0..cur_bkv) |ki_local| {
                            const s = score_tile[qi_local * cur_bkv + ki_local];
                            if (s > block_max) block_max = s;
                        }
                        const old_max = row_max[qi_local];
                        const new_max = @max(old_max, block_max);
                        if (new_max == -std.math.inf(f32)) continue;
                        if (row_sum[qi_local] != 0.0) {
                            const rescale = @exp(old_max - new_max);
                            if (rescale != 1.0) {
                                for (out_ptr) |*v| v.* *= rescale;
                                row_sum[qi_local] *= rescale;
                            }
                        }
                        var block_sum: f32 = 0;
                        for (0..cur_bkv) |ki_local| {
                            const s = score_tile[qi_local * cur_bkv + ki_local];
                            const w = @exp(s - new_max);
                            if (w == 0.0) continue;
                            block_sum += w;
                            const ki = kv_start + ki_local;
                            const v_row = V[bh_offset + ki * head_dim ..][0..head_dim];
                            for (out_ptr, v_row) |*o, vv| o.* += w * vv;
                        }
                        row_max[qi_local] = new_max;
                        row_sum[qi_local] += block_sum;
                    }
                    kv_start = kv_end;
                }
                for (0..cur_bq) |qi_local| {
                    const qi = q_start + qi_local;
                    if (row_sum[qi_local] != 0.0) {
                        const inv = 1.0 / row_sum[qi_local];
                        const out_ptr = output[bh_offset + qi * head_dim ..][0..head_dim];
                        for (out_ptr) |*v| v.* *= inv;
                    }
                }
                q_start = q_end;
            }
        }
    }
}

fn benchFlashAttn(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize, causal: bool) !void {
    const H: usize = num_heads * head_dim;
    const Q = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(Q);
    const K = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(K);
    const V = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(V);
    const out_baseline = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(out_baseline);
    var prng = std.Random.DefaultPrng.init(0xF1A5_AA00 + seq_len);
    for (Q) |*v| v.* = (prng.random().float(f32) - 0.5) * 0.5;
    for (K) |*v| v.* = (prng.random().float(f32) - 0.5) * 0.5;
    for (V) |*v| v.* = (prng.random().float(f32) - 0.5);

    var checksum: f64 = 0;

    if (causal) {
        // Token-major layout for the causal path.
        const ones_attn_or_mask: ?[]const u8 = null;
        for (0..cfg.warmup_iters) |_| try flashCausalAttnTokenMajorBaseline(allocator, Q, K, V, batch, seq_len, num_heads, head_dim, out_baseline);
        var base_ns: u64 = 0;
        for (0..cfg.measure_iters) |_| {
            const t = nowNs();
            try flashCausalAttnTokenMajorBaseline(allocator, Q, K, V, batch, seq_len, num_heads, head_dim, out_baseline);
            base_ns += nowNs() - t;
            foldChecksum(&checksum, out_baseline[0]);
        }
        for (0..cfg.warmup_iters) |_| {
            const opt_out = linalg.attention.flashCausalAttentionHost(allocator, Q, K, V, null, ones_attn_or_mask, 0, batch, seq_len, seq_len, 0, 0, num_heads, num_heads, head_dim) catch unreachable;
            allocator.free(opt_out);
        }
        var opt_ns: u64 = 0;
        for (0..cfg.measure_iters) |_| {
            const t = nowNs();
            const opt_out = linalg.attention.flashCausalAttentionHost(allocator, Q, K, V, null, ones_attn_or_mask, 0, batch, seq_len, seq_len, 0, 0, num_heads, num_heads, head_dim) catch unreachable;
            opt_ns += nowNs() - t;
            foldChecksum(&checksum, opt_out[0]);
            allocator.free(opt_out);
        }
        printRow(label, base_ns / cfg.measure_iters, opt_ns / cfg.measure_iters, checksum);
    } else {
        // Head-major layout (CLIP vision / BERT).
        for (0..cfg.warmup_iters) |_| try flashAttnHeadMajorBaseline(allocator, Q, K, V, batch, seq_len, num_heads, head_dim, out_baseline);
        var base_ns: u64 = 0;
        for (0..cfg.measure_iters) |_| {
            const t = nowNs();
            try flashAttnHeadMajorBaseline(allocator, Q, K, V, batch, seq_len, num_heads, head_dim, out_baseline);
            base_ns += nowNs() - t;
            foldChecksum(&checksum, out_baseline[0]);
        }
        const ones = try allocator.alloc(i64, batch * seq_len);
        defer allocator.free(ones);
        @memset(ones, 1);
        for (0..cfg.warmup_iters) |_| {
            const opt_out = linalg.attention.flashAttentionHost(allocator, Q, K, V, null, ones, batch, seq_len, num_heads, head_dim) catch unreachable;
            allocator.free(opt_out);
        }
        var opt_ns: u64 = 0;
        for (0..cfg.measure_iters) |_| {
            const t = nowNs();
            const opt_out = linalg.attention.flashAttentionHost(allocator, Q, K, V, null, ones, batch, seq_len, num_heads, head_dim) catch unreachable;
            opt_ns += nowNs() - t;
            foldChecksum(&checksum, opt_out[0]);
            allocator.free(opt_out);
        }
        printRow(label, base_ns / cfg.measure_iters, opt_ns / cfg.measure_iters, checksum);
    }
}

// --- SGEMM register-tile expansion (lib/linalg/mod.zig hot path) -------

/// Pre-optimization baseline: sgemmTransB with the previous MR=4, NR=2
/// tile, single-threaded.  Inlined here verbatim from the original kernel
/// so the bench measures the tile change in isolation (no threading, no
/// f16, no Kc blocking).
fn sgemmTransBBaselineMR4(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    c_out: []f32,
) void {
    const V = 8;
    const Vec = @Vector(V, f32);
    const MR = 4;
    const NR = 2;
    @memset(c_out, 0.0);

    const m_main = (m / MR) * MR;
    var j: usize = 0;
    while (j + NR <= n) : (j += NR) {
        var i: usize = 0;
        while (i < m_main) : (i += MR) {
            var acc: [MR][NR]Vec = undefined;
            inline for (0..MR) |r| inline for (0..NR) |c| {
                acc[r][c] = @splat(0.0);
            };

            var l: usize = 0;
            while (l + V <= k) : (l += V) {
                var av: [MR]Vec = undefined;
                inline for (0..MR) |r| av[r] = a[(i + r) * k + l ..][0..V].*;
                var bv: [NR]Vec = undefined;
                inline for (0..NR) |c| bv[c] = b[(j + c) * k + l ..][0..V].*;
                inline for (0..MR) |r| inline for (0..NR) |c| {
                    acc[r][c] = @mulAdd(Vec, av[r], bv[c], acc[r][c]);
                };
            }
            var sum: [MR][NR]f32 = undefined;
            inline for (0..MR) |r| inline for (0..NR) |c| {
                sum[r][c] = @reduce(.Add, acc[r][c]);
            };
            while (l < k) : (l += 1) {
                inline for (0..MR) |r| {
                    const a_val = a[(i + r) * k + l];
                    inline for (0..NR) |c| sum[r][c] += a_val * b[(j + c) * k + l];
                }
            }
            inline for (0..MR) |r| inline for (0..NR) |c| {
                c_out[(i + r) * n + j + c] += alpha * sum[r][c];
            };
        }
        // M-tail.
        while (i < m) : (i += 1) {
            const a_row = a[i * k ..][0..k];
            inline for (0..NR) |c| {
                const b_row = b[(j + c) * k ..][0..k];
                var acc: Vec = @splat(0.0);
                var l: usize = 0;
                while (l + V <= k) : (l += V) {
                    const av: Vec = a_row[l..][0..V].*;
                    const bv: Vec = b_row[l..][0..V].*;
                    acc = @mulAdd(Vec, av, bv, acc);
                }
                var sum: f32 = @reduce(.Add, acc);
                while (l < k) : (l += 1) sum += a_row[l] * b_row[l];
                c_out[i * n + j + c] += alpha * sum;
            }
        }
    }
    // N-tail.
    while (j < n) : (j += 1) {
        const b_row = b[j * k ..][0..k];
        var irow: usize = 0;
        while (irow < m) : (irow += 1) {
            const a_row = a[irow * k ..][0..k];
            var acc: Vec = @splat(0.0);
            var l: usize = 0;
            while (l + V <= k) : (l += V) {
                const av: Vec = a_row[l..][0..V].*;
                const bv: Vec = b_row[l..][0..V].*;
                acc = @mulAdd(Vec, av, bv, acc);
            }
            var sum: f32 = @reduce(.Add, acc);
            while (l < k) : (l += 1) sum += a_row[l] * b_row[l];
            c_out[irow * n + j] += alpha * sum;
        }
    }
}

fn benchSgemmTransB(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, m: usize, n: usize, k: usize) !void {
    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, n * k);
    defer allocator.free(b);
    const c_baseline = try allocator.alloc(f32, m * n);
    defer allocator.free(c_baseline);
    const c_opt = try allocator.alloc(f32, m * n);
    defer allocator.free(c_opt);
    var prng = std.Random.DefaultPrng.init(0x5_F2D6_E1AD);
    for (a) |*v| v.* = (prng.random().float(f32) - 0.5);
    for (b) |*v| v.* = (prng.random().float(f32) - 0.5);

    var checksum: f64 = 0;

    // Baseline: MR=4 NR=2, single-threaded, inlined.
    for (0..cfg.warmup_iters) |_| sgemmTransBBaselineMR4(m, n, k, 1.0, a, b, c_baseline);
    var base_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        sgemmTransBBaselineMR4(m, n, k, 1.0, a, b, c_baseline);
        base_ns += nowNs() - t;
        foldChecksum(&checksum, c_baseline[0]);
    }

    // Optimized: production sgemmTransBSync with the new comptime tile.
    // For shapes large enough that the production path goes parallel
    // (>4M FMAs), the speedup combines tile + threading; that's the
    // realistic deployment we care about.
    for (0..cfg.warmup_iters) |_| linalg.sgemmTransBSync(m, n, k, 1.0, a, b, 0.0, c_opt);
    var opt_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        linalg.sgemmTransBSync(m, n, k, 1.0, a, b, 0.0, c_opt);
        opt_ns += nowNs() - t;
        foldChecksum(&checksum, c_opt[0]);
    }

    // Spot-check correctness — drift would be a regression.
    var max_diff: f32 = 0;
    for (c_baseline, c_opt) |x, y| {
        const d = @abs(x - y);
        if (d > max_diff) max_diff = d;
    }
    if (max_diff > 1e-2) std.debug.print("  WARN: sgemm baseline vs opt max diff = {d:.6}\n", .{max_diff});

    printRow(label, base_ns / cfg.measure_iters, opt_ns / cfg.measure_iters, checksum);
}

// --- Quantized linear (native CPU production path) ----------------------

fn quantizeLinearWeight(
    allocator: std.mem.Allocator,
    weight: []const f32,
    kind: tensor_types.KnownTensorType,
) ![]u8 {
    return switch (kind) {
        .Q1_0 => try quant_codec.quantizeQ1_0FromF32(allocator, weight),
        .Q4_0 => try quant_codec.quantizeQ4_0FromF32(allocator, weight),
        .Q4_1 => try quant_codec.quantizeQ4_1FromF32(allocator, weight),
        .Q5_0 => try quant_codec.quantizeQ5_0FromF32(allocator, weight),
        .Q5_1 => try quant_codec.quantizeQ5_1FromF32(allocator, weight),
        .Q8_0 => try quant_codec.quantizeQ8_0FromF32(allocator, weight),
        .Q8_1 => try quant_codec.quantizeQ8_1FromF32(allocator, weight),
        .Q2_K => try quant_codec.quantizeQ2_KFromF32(allocator, weight),
        .Q3_K => try quant_codec.quantizeQ3_KFromF32(allocator, weight),
        .Q4_K => try quant_codec.quantizeQ4_KFromF32(allocator, weight),
        .Q5_K => try quant_codec.quantizeQ5_KFromF32(allocator, weight),
        .Q6_K => try quant_codec.quantizeQ6_KFromF32(allocator, weight),
        .Q8_K => try quant_codec.quantizeQ8_KFromF32(allocator, weight),
        else => error.UnsupportedQuantLinearBenchType,
    };
}

fn quantTypeName(kind: tensor_types.KnownTensorType) []const u8 {
    return switch (kind) {
        .Q1_0 => "Q1_0",
        .Q4_0 => "Q4_0",
        .Q4_1 => "Q4_1",
        .Q5_0 => "Q5_0",
        .Q5_1 => "Q5_1",
        .Q8_0 => "Q8_0",
        .Q8_1 => "Q8_1",
        .Q2_K => "Q2_K",
        .Q3_K => "Q3_K",
        .Q4_K => "Q4_K",
        .Q5_K => "Q5_K",
        .Q6_K => "Q6_K",
        .Q8_K => "Q8_K",
        else => "unsupported",
    };
}

fn hasDirectVariants(kind: tensor_types.KnownTensorType) bool {
    return kind == .Q4_K or kind == .Q5_K or kind == .Q6_K or kind == .Q8_K;
}

fn benchQuantizedParitySweep(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
) !void {
    for (quant_parity_kinds) |kind| {
        const name = quantTypeName(kind);
        const linear_label = try std.fmt.allocPrint(allocator, "quantParity linear {s} 77x768x768 (CLIP text proj)", .{name});
        try benchQuantizedLinear(allocator, cfg, linear_label, 77, 768, 768, kind);

        const dequant_label = try std.fmt.allocPrint(allocator, "quantParity cachedDequantSgemm {s} 77x768x768", .{name});
        try benchQuantizedLinearDequantSgemm(allocator, cfg, dequant_label, 77, 768, 768, kind);

        const pair_label = try std.fmt.allocPrint(allocator, "quantParity pair {s} 77x768x768 (CLIP text Q/K)", .{name});
        try benchQuantizedLinearPair(allocator, cfg, pair_label, 77, 768, 768, kind);

        const triple_label = try std.fmt.allocPrint(allocator, "quantParity triple {s} 77x768x768 (CLIP text QKV)", .{name});
        try benchQuantizedLinearTriple(allocator, cfg, triple_label, 77, 768, 768, kind);
    }
}

fn benchQuantizedSmallRowKSweep(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
) !void {
    const small_row_kinds = [_]tensor_types.KnownTensorType{ .Q2_K, .Q3_K, .Q4_K, .Q5_K, .Q6_K, .Q8_K };
    for (small_row_kinds) |kind| {
        const name = quantTypeName(kind);
        const linear_label = try std.fmt.allocPrint(allocator, "quantSmallRow linear {s} 1x768x768 (pooled projection)", .{name});
        defer allocator.free(linear_label);
        try benchQuantizedLinear(allocator, cfg, linear_label, 1, 768, 768, kind);

        const mlp_label = try std.fmt.allocPrint(allocator, "quantSmallRow linear {s} 1x768x3072 (small-row MLP up)", .{name});
        defer allocator.free(mlp_label);
        try benchQuantizedLinear(allocator, cfg, mlp_label, 1, 768, 3072, kind);

        const pair_label = try std.fmt.allocPrint(allocator, "quantSmallRow pair {s} 1x768x768 (pooled Q/K)", .{name});
        defer allocator.free(pair_label);
        try benchQuantizedLinearPair(allocator, cfg, pair_label, 1, 768, 768, kind);

        const triple_label = try std.fmt.allocPrint(allocator, "quantSmallRow triple {s} 1x768x768 (pooled QKV)", .{name});
        defer allocator.free(triple_label);
        try benchQuantizedLinearTriple(allocator, cfg, triple_label, 1, 768, 768, kind);

        const triple_mr2_label = try std.fmt.allocPrint(allocator, "quantSmallRow triple {s} 2x768x768 (pooled QKV MR2)", .{name});
        defer allocator.free(triple_mr2_label);
        try benchQuantizedLinearTriple(allocator, cfg, triple_mr2_label, 2, 768, 768, kind);
    }
}

fn benchNativeQuantBuckets(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
) !void {
    const rows_buckets = [_]usize{ 1, 2, 3, 4, 5, 6, 8, 9, 64 };
    for (quant_policy_kinds) |kind| {
        if (!filterMatchesKind(cfg, kind)) continue;
        const tensor_type: tensor_types.TensorType = .{ .known = kind };
        const block_values = tensor_types.valuesPerBlock(tensor_type) orelse continue;
        const default_in_dim: usize = if (block_values >= 128) 768 else 96;
        const default_out_dim: usize = 512;
        const row_values: []const usize = if (hasCompleteShapeFilter(cfg)) &[_]usize{cfg.filter_rows.?} else &rows_buckets;
        for (row_values) |rows| {
            const in_dim: usize = cfg.filter_in_dim orelse default_in_dim;
            const out_dim: usize = cfg.filter_out_dim orelse default_out_dim;
            if (!filterMatchesShape(cfg, rows, in_dim, out_dim)) continue;
            if (in_dim % block_values != 0) continue;
            const linear_label = try std.fmt.allocPrint(
                allocator,
                "nativeQuantBucket linear {s} {d}x{d}x{d}",
                .{ quantTypeName(kind), rows, in_dim, out_dim },
            );
            defer allocator.free(linear_label);
            try benchQuantizedLinear(allocator, cfg, linear_label, rows, in_dim, out_dim, kind);

            const pair_label = try std.fmt.allocPrint(
                allocator,
                "nativeQuantBucket pair {s} {d}x{d}x{d}",
                .{ quantTypeName(kind), rows, in_dim, out_dim },
            );
            defer allocator.free(pair_label);
            try benchQuantizedLinearPair(allocator, cfg, pair_label, rows, in_dim, out_dim, kind);

            const triple_label = try std.fmt.allocPrint(
                allocator,
                "nativeQuantBucket triple {s} {d}x{d}x{d}",
                .{ quantTypeName(kind), rows, in_dim, out_dim },
            );
            defer allocator.free(triple_label);
            try benchQuantizedLinearTriple(allocator, cfg, triple_label, rows, in_dim, out_dim, kind);
        }
    }
}

fn addBiasRowsBench(output: []f32, bias: []const f32, rows: usize, out_dim: usize) void {
    for (0..rows) |r| {
        const out_row = output[r * out_dim ..][0..out_dim];
        for (out_row, bias[0..out_dim]) |*value, bias_value| {
            value.* += bias_value;
        }
    }
}

fn productionSgemmTransBSync(
    rows: usize,
    out_dim: usize,
    in_dim: usize,
    input: []const f32,
    weight: []const f32,
    beta: f32,
    output: []f32,
) void {
    native_backend.sgemmTransBSync(rows, out_dim, in_dim, 1.0, input, weight, beta, output);
}

fn benchPackedQkvDirectCompare(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
) !void {
    const Shape = struct {
        label: []const u8,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    };
    const shapes = [_]Shape{
        .{ .label = "CLIP text hidden512 QKV", .rows = 77, .in_dim = 512, .out_dim = 512 },
        .{ .label = "CLIP/CLAP text hidden768 QKV", .rows = 77, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP text max-pos QKV", .rows = 514, .in_dim = 768, .out_dim = 768 },
        .{ .label = "small MR2 hidden512 QKV", .rows = 2, .in_dim = 512, .out_dim = 512 },
        .{ .label = "small MR4 hidden512 QKV", .rows = 4, .in_dim = 512, .out_dim = 512 },
        .{ .label = "small MR4+MR2 hidden512 QKV", .rows = 6, .in_dim = 512, .out_dim = 512 },
        .{ .label = "small MR2 hidden768 QKV", .rows = 2, .in_dim = 768, .out_dim = 768 },
        .{ .label = "small MR4 hidden768 QKV", .rows = 4, .in_dim = 768, .out_dim = 768 },
        .{ .label = "small MR4+MR2 hidden768 QKV", .rows = 6, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLIP vision P32 QKV", .rows = 50, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLIP vision P16 QKV", .rows = 197, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLIP vision P14 QKV", .rows = 257, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio encoder short QKV", .rows = 64, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio encoder mid QKV", .rows = 128, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio encoder long QKV", .rows = 256, .in_dim = 768, .out_dim = 768 },
        .{ .label = "CLAP audio pooled QKV", .rows = 1, .in_dim = 256, .out_dim = 256 },
        .{ .label = "pooled hidden768 QKV", .rows = 1, .in_dim = 768, .out_dim = 768 },
    };
    const kinds = [_]tensor_types.KnownTensorType{ .Q4_K, .Q5_K };

    if (hasCompleteShapeFilter(cfg)) {
        const rows = cfg.filter_rows.?;
        const in_dim = cfg.filter_in_dim.?;
        const out_dim = cfg.filter_out_dim.?;
        for (kinds) |kind| {
            if (!filterMatchesKind(cfg, kind)) continue;
            const tensor_type: tensor_types.TensorType = .{ .known = kind };
            const block_values = tensor_types.valuesPerBlock(tensor_type) orelse continue;
            if (in_dim % block_values != 0) continue;
            const label = try std.fmt.allocPrint(
                allocator,
                "packedQKVDirect {s} custom {d}x{d}x{d}",
                .{ quantTypeName(kind), rows, in_dim, out_dim },
            );
            defer allocator.free(label);
            try benchQuantizedLinearTriple(allocator, cfg, label, rows, in_dim, out_dim, kind);
        }
        return;
    }

    for (kinds) |kind| {
        if (!filterMatchesKind(cfg, kind)) continue;
        for (shapes) |shape| {
            if (!filterMatchesShape(cfg, shape.rows, shape.in_dim, shape.out_dim)) continue;
            const tensor_type: tensor_types.TensorType = .{ .known = kind };
            const block_values = tensor_types.valuesPerBlock(tensor_type) orelse continue;
            if (shape.in_dim % block_values != 0) continue;
            const label = try std.fmt.allocPrint(
                allocator,
                "packedQKVDirect {s} {s} {d}x{d}x{d}",
                .{ quantTypeName(kind), shape.label, shape.rows, shape.in_dim, shape.out_dim },
            );
            defer allocator.free(label);
            try benchQuantizedLinearTriple(allocator, cfg, label, shape.rows, shape.in_dim, shape.out_dim, kind);
        }
    }
}

fn benchQuantizedLinear(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    label: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    kind: tensor_types.KnownTensorType,
) !void {
    const tensor_type: tensor_types.TensorType = .{ .known = kind };
    const block_values = tensor_types.valuesPerBlock(tensor_type) orelse return error.UnsupportedQuantLinearBenchType;
    if (in_dim % block_values != 0) return;

    const input = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input);
    const weight = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight);
    const dense_out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(dense_out);
    const quant_out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(quant_out);

    var prng = std.Random.DefaultPrng.init(0xC11C_C1A9);
    const rand = prng.random();
    for (input) |*v| v.* = (rand.float(f32) - 0.5) * 2.0;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(@max(in_dim, 1))));
    for (weight) |*v| v.* = (rand.float(f32) - 0.5) * 2.0 * scale;

    const raw_bytes = try quantizeLinearWeight(allocator, weight, kind);
    errdefer allocator.free(raw_bytes);
    const shape = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) });
    errdefer allocator.free(shape);
    var storage = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_bytes,
        .shape = shape,
        .raw_owned = true,
        .allocator = allocator,
    };
    defer storage.deinit();
    try native_compute.prepareNativeQuantizedStorage(&storage);

    var checksum: f64 = 0;

    for (0..cfg.warmup_iters) |_| {
        linalg.sgemmTransBSync(rows, out_dim, in_dim, 1.0, input, weight, 0.0, dense_out);
    }
    var dense_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        linalg.sgemmTransBSync(rows, out_dim, in_dim, 1.0, input, weight, 0.0, dense_out);
        dense_ns += nowNs() - t;
        foldChecksum(&checksum, dense_out[0]);
    }

    for (0..cfg.warmup_iters) |_| {
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage, input, quant_out, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
    }
    var quant_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage, input, quant_out, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
        quant_ns += nowNs() - t;
        foldChecksum(&checksum, quant_out[0]);
    }

    var max_abs: f32 = 0;
    for (dense_out, quant_out) |dense, quant| {
        const diff = @abs(dense - quant);
        if (diff > max_abs) max_abs = diff;
    }
    if (!std.math.isFinite(max_abs)) {
        std.debug.print("  WARN: quantLinear {s} produced non-finite diff\n", .{quantTypeName(kind)});
    }

    printRow(label, dense_ns / cfg.measure_iters, quant_ns / cfg.measure_iters, checksum);
}

fn benchQuantizedLinearDequantSgemm(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    label: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    kind: tensor_types.KnownTensorType,
) !void {
    const tensor_type: tensor_types.TensorType = .{ .known = kind };
    const block_values = tensor_types.valuesPerBlock(tensor_type) orelse return error.UnsupportedQuantLinearBenchType;
    if (in_dim % block_values != 0) return;

    const input = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input);
    const weight = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight);
    const direct_out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(direct_out);
    const dequant_out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(dequant_out);
    const dequant_weight = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(dequant_weight);

    var prng = std.Random.DefaultPrng.init(0xC11C_D6E9);
    const rand = prng.random();
    for (input) |*v| v.* = (rand.float(f32) - 0.5) * 2.0;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(@max(in_dim, 1))));
    for (weight) |*v| v.* = (rand.float(f32) - 0.5) * 2.0 * scale;

    const raw_bytes = try quantizeLinearWeight(allocator, weight, kind);
    errdefer allocator.free(raw_bytes);
    const shape = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) });
    errdefer allocator.free(shape);
    var storage = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_bytes,
        .shape = shape,
        .raw_owned = true,
        .allocator = allocator,
    };
    defer storage.deinit();
    try native_compute.prepareNativeQuantizedStorage(&storage);
    try quant_codec.dequantizeToFloat32(tensor_type, raw_bytes, dequant_weight);

    var checksum: f64 = 0;

    for (0..cfg.warmup_iters) |_| {
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage, input, direct_out, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
    }
    native_compute.resetNativeQuantDispatchStats();
    var direct_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage, input, direct_out, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
        direct_ns += nowNs() - t;
        foldChecksum(&checksum, direct_out[0]);
    }
    const direct_stats = averageNativeQuantPhaseStats(native_compute.nativeQuantDispatchStats(), cfg.measure_iters);

    for (0..cfg.warmup_iters) |_| {
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_weight, 0.0, dequant_out);
    }
    var dequant_sgemm_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_weight, 0.0, dequant_out);
        dequant_sgemm_ns += nowNs() - t;
        foldChecksum(&checksum, dequant_out[0]);
    }

    printRow(label, direct_ns / cfg.measure_iters, dequant_sgemm_ns / cfg.measure_iters, checksum);
    if (kind == .Q4_0 or kind == .Q4_K or kind == .Q5_K or kind == .Q8_0) {
        printQuantLinearPhaseRow(label, direct_stats, direct_ns / cfg.measure_iters);
    }
}

fn benchQuantizedLinearDequantSgemmIfSupported(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    label: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    kind: tensor_types.KnownTensorType,
) !void {
    const tensor_type: tensor_types.TensorType = .{ .known = kind };
    const block_values = tensor_types.valuesPerBlock(tensor_type) orelse return;
    if (in_dim % block_values != 0) return;
    try benchQuantizedLinearDequantSgemm(allocator, cfg, label, rows, in_dim, out_dim, kind);
}

const DirectLinearVariant = enum {
    default,
    q4q5_panel16,
    q6_panel16,
    q8_panel8,
    q8_panel16,
    q8_sdot_off,
};

fn directLinearVariantName(variant: DirectLinearVariant) []const u8 {
    return switch (variant) {
        .default => "directDefault",
        .q4q5_panel16 => "panel16MR4",
        .q6_panel16 => "panel16",
        .q8_panel8 => "panel8",
        .q8_panel16 => "panel16",
        .q8_sdot_off => "sdotOff",
    };
}

fn directLinearVariantNameForShape(variant: DirectLinearVariant, kind: tensor_types.KnownTensorType, rows: usize, in_dim: usize, out_dim: usize) []const u8 {
    if (variant == .q4q5_panel16 and (kind == .Q4_K or kind == .Q5_K) and in_dim % 256 == 0) {
        const row_blocks = in_dim / 256;
        if (rows >= 77 and rows <= 308 and (rows % 77 == 0 or rows % 8 == 0)) {
            if (row_blocks == 3 and (out_dim == 768 or out_dim == 3072)) return "panel16MR8";
            if (row_blocks == 12 and rows % 8 == 0 and out_dim == 768) return "panel16MR8";
        }
    }
    return directLinearVariantName(variant);
}

fn setDirectLinearVariantOverrides(kind: tensor_types.KnownTensorType, variant: DirectLinearVariant) void {
    native_compute.setClipClapDequantSgemmOverrideForBench(false);
    native_compute.setQ4Q5Panel16DirectOverrideForBench(if (kind == .Q4_K or kind == .Q5_K) switch (variant) {
        .q4q5_panel16 => true,
        .default => null,
        else => false,
    } else null);
    native_compute.setQ6KPanel16DirectOverrideForBench(if (kind == .Q6_K) switch (variant) {
        .q6_panel16 => true,
        .default => null,
        else => false,
    } else null);
    native_compute.setQ8KPanel8DirectOverrideForBench(if (kind == .Q8_K) switch (variant) {
        .q8_panel8 => true,
        .default => null,
        else => false,
    } else null);
    native_compute.setQ8KPanel16DirectOverrideForBench(if (kind == .Q8_K) switch (variant) {
        .q8_panel16 => true,
        .default => null,
        else => false,
    } else null);
    native_compute.setQ8KFusedSdotOverrideForBench(if (kind == .Q8_K and variant == .q8_sdot_off) false else null);
}

fn resetDirectLinearVariantOverrides() void {
    native_compute.setClipClapDequantSgemmOverrideForBench(null);
    native_compute.setQ4Q5Panel16DirectOverrideForBench(null);
    native_compute.setQ6KPanel16DirectOverrideForBench(null);
    native_compute.setQ8KPanel8DirectOverrideForBench(null);
    native_compute.setQ8KPanel16DirectOverrideForBench(null);
    native_compute.setQ8KFusedSdotOverrideForBench(null);
}

const DirectLinearTiming = struct {
    ns: u64,
    stats: native_compute.NativeQuantDispatchStats,
};

fn timeQuantizedLinearDirect(
    cfg: BenchConfig,
    storage: *QuantizedStorage,
    input: []const f32,
    output: []f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    kind: tensor_types.KnownTensorType,
    variant: DirectLinearVariant,
) !DirectLinearTiming {
    setDirectLinearVariantOverrides(kind, variant);
    defer resetDirectLinearVariantOverrides();
    try native_compute.prepareNativeQuantizedStorage(storage);

    for (0..cfg.warmup_iters) |_| {
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, storage, input, output, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
    }
    native_compute.resetNativeQuantDispatchStats();
    var elapsed_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, storage, input, output, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
        elapsed_ns += nowNs() - t;
    }
    return .{
        .ns = elapsed_ns / cfg.measure_iters,
        .stats = averageNativeQuantPhaseStats(native_compute.nativeQuantDispatchStats(), cfg.measure_iters),
    };
}

fn timeQuantizedLinearTripleDirect(
    cfg: BenchConfig,
    storage_a: *QuantizedStorage,
    storage_b: *const QuantizedStorage,
    storage_c: *const QuantizedStorage,
    input: []const f32,
    bias_a: []const f32,
    bias_b: []const f32,
    bias_c: []const f32,
    output_a: []f32,
    output_b: []f32,
    output_c: []f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    panel16_enabled: bool,
    fused_triple_enabled: bool,
    triple_panel16_dot_enabled: bool,
    packed_qkv_panel16_enabled: bool,
) !u64 {
    native_compute.setClipClapDequantSgemmOverrideForBench(false);
    native_compute.setQ4Q5Panel16DirectOverrideForBench(panel16_enabled);
    native_compute.setQ4Q5FusedTripleDirectOverrideForBench(fused_triple_enabled);
    native_compute.setQ4Q5TriplePanel16DotOverrideForBench(triple_panel16_dot_enabled);
    native_compute.setQ4Q5PackedQKVPanel16OverrideForBench(packed_qkv_panel16_enabled);
    defer native_compute.setClipClapDequantSgemmOverrideForBench(null);
    defer native_compute.setQ4Q5Panel16DirectOverrideForBench(null);
    defer native_compute.setQ4Q5FusedTripleDirectOverrideForBench(null);
    defer native_compute.setQ4Q5TriplePanel16DotOverrideForBench(null);
    defer native_compute.setQ4Q5PackedQKVPanel16OverrideForBench(null);

    if (packed_qkv_panel16_enabled) {
        try native_compute.prepareQ4Q5QKVPanel16PackedStorageForBench(storage_a, storage_b, storage_c, "bench_qkv_b", "bench_qkv_c");
    }

    for (0..cfg.warmup_iters) |_| {
        if (!(try native_compute.linearQuantizedTriple(cfg.io, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, output_a, output_b, output_c, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
    }
    var elapsed_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        if (!(try native_compute.linearQuantizedTriple(cfg.io, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, output_a, output_b, output_c, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
        elapsed_ns += nowNs() - t;
    }
    return elapsed_ns / cfg.measure_iters;
}

const TripleAutoSelectorTiming = struct {
    ns: u64,
    stats: native_compute.NativeQuantDispatchStats,
    route_name: []const u8,
};

fn packedQKVAutoSelectorRouteName(stats: native_compute.NativeQuantDispatchStats, fallback: []const u8) []const u8 {
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr8 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr1 > 0) return "packedMR8+MR4+MR2+MR1";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr8 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2 > 0) return "packedMR8+MR4+MR2";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr8 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr1 > 0) return "packedMR8+MR4+MR1";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr8 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4 > 0) return "packedMR8+MR4";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr8 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr1 > 0) return "packedMR8+MR2+MR1";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr8 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2 > 0) return "packedMR8+MR2";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr8 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr1 > 0) return "packedMR8+MR1";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr8 > 0) return "packedMR8";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr1 > 0) return "packedMR4+MR2+MR1";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2 > 0) return "packedMR4+MR2";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr1 > 0) return "packedMR4+MR1";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4 > 0) return "packedMR4";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2 > 0 and stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr1 > 0) return "packedMR2+MR1";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2 > 0) return "packedMR2";
    if (stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr1 > 0) return "packedMR1";
    if (stats.q4_q5_k_q8k_activation_triple > 0) return "panel16Split";
    if (stats.legacy_activation_triple > 0) return "legacyTriple";
    if (stats.dequant_sgemm_triple > 0) return "dequantSgemm";
    return fallback;
}

fn timeQuantizedLinearTripleAutoSelector(
    cfg: BenchConfig,
    storage_a: *QuantizedStorage,
    storage_b: *const QuantizedStorage,
    storage_c: *const QuantizedStorage,
    input: []const f32,
    bias_a: []const f32,
    bias_b: []const f32,
    bias_c: []const f32,
    output_a: []f32,
    output_b: []f32,
    output_c: []f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !TripleAutoSelectorTiming {
    native_compute.setClipClapDequantSgemmOverrideForBench(false);
    native_compute.setQ4Q5Panel16DirectOverrideForBench(true);
    native_compute.setQ4Q5FusedTripleDirectOverrideForBench(false);
    native_compute.setQ4Q5TriplePanel16DotOverrideForBench(false);
    native_compute.setQ4Q5PackedQKVPanel16AutoOverrideForBench();
    defer native_compute.setClipClapDequantSgemmOverrideForBench(null);
    defer native_compute.setQ4Q5Panel16DirectOverrideForBench(null);
    defer native_compute.setQ4Q5FusedTripleDirectOverrideForBench(null);
    defer native_compute.setQ4Q5TriplePanel16DotOverrideForBench(null);
    defer native_compute.setQ4Q5PackedQKVPanel16OverrideForBench(null);

    try native_compute.prepareQ4Q5QKVPanel16PackedStorageForBench(storage_a, storage_b, storage_c, "bench_qkv_b", "bench_qkv_c");

    for (0..cfg.warmup_iters) |_| {
        if (!(try native_compute.linearQuantizedTriple(cfg.io, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, output_a, output_b, output_c, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
    }
    native_compute.resetNativeQuantDispatchStats();
    var elapsed_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        if (!(try native_compute.linearQuantizedTriple(cfg.io, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, output_a, output_b, output_c, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
        elapsed_ns += nowNs() - t;
    }
    return .{
        .ns = elapsed_ns / cfg.measure_iters,
        .stats = averageNativeQuantPhaseStats(native_compute.nativeQuantDispatchStats(), cfg.measure_iters),
        .route_name = native_compute.q4q5PackedQKVPanel16AutoRouteNameForBench(rows, in_dim, out_dim, switch (storage_a.tensor_type) {
            .known => |value| value,
            else => return error.UnsupportedQuantLinearBenchType,
        }),
    };
}

fn benchQuantizedLinearVariant(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    label: []const u8,
    storage: *QuantizedStorage,
    input: []const f32,
    direct_out: []const f32,
    variant_out: []f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    kind: tensor_types.KnownTensorType,
    direct_timing: DirectLinearTiming,
    variant: DirectLinearVariant,
    checksum: *f64,
) !void {
    const variant_timing = try timeQuantizedLinearDirect(cfg, storage, input, variant_out, rows, in_dim, out_dim, kind, variant);
    foldChecksum(checksum, variant_out[0]);
    var max_abs: f32 = 0;
    for (direct_out, variant_out) |direct, variant_value| {
        const diff = @abs(direct - variant_value);
        if (diff > max_abs) max_abs = diff;
    }
    if (max_abs > 1e-3 and !std.math.isNan(max_abs)) {
        std.debug.print("  WARN: directVariant {s} {s} max diff = {d:.6}\n", .{ directLinearVariantName(variant), quantTypeName(kind), max_abs });
    }

    const variant_label = try std.fmt.allocPrint(allocator, "{s} directDefault_vs_{s}", .{ label, directLinearVariantNameForShape(variant, kind, rows, in_dim, out_dim) });
    defer allocator.free(variant_label);
    printRow(variant_label, direct_timing.ns, variant_timing.ns, checksum.*);
    if (kind == .Q4_K or kind == .Q5_K or kind == .Q8_0) {
        printQuantLinearPhaseRow(variant_label, variant_timing.stats, variant_timing.ns);
    }
}

fn benchQuantizedLinearDirectVariants(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    label: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    kind: tensor_types.KnownTensorType,
) !void {
    const tensor_type: tensor_types.TensorType = .{ .known = kind };
    const block_values = tensor_types.valuesPerBlock(tensor_type) orelse return error.UnsupportedQuantLinearBenchType;
    if (in_dim % block_values != 0) return error.InvalidQuantLinearBenchShape;

    const input = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input);
    const weight = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight);
    const direct_out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(direct_out);
    const variant_out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(variant_out);
    const dequant_out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(dequant_out);
    const dequant_weight = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(dequant_weight);

    var prng = std.Random.DefaultPrng.init(0xC11C_D171);
    const rand = prng.random();
    for (input) |*v| v.* = (rand.float(f32) - 0.5) * 2.0;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(@max(in_dim, 1))));
    for (weight) |*v| v.* = (rand.float(f32) - 0.5) * 2.0 * scale;

    const raw_bytes = try quantizeLinearWeight(allocator, weight, kind);
    errdefer allocator.free(raw_bytes);
    const shape = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) });
    errdefer allocator.free(shape);
    var storage = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_bytes,
        .shape = shape,
        .raw_owned = true,
        .allocator = allocator,
    };
    defer storage.deinit();
    try quant_codec.dequantizeToFloat32(tensor_type, raw_bytes, dequant_weight);

    var checksum: f64 = 0;
    const direct_timing = try timeQuantizedLinearDirect(cfg, &storage, input, direct_out, rows, in_dim, out_dim, kind, .default);
    foldChecksum(&checksum, direct_out[0]);

    switch (kind) {
        .Q4_K, .Q5_K => try benchQuantizedLinearVariant(allocator, cfg, label, &storage, input, direct_out, variant_out, rows, in_dim, out_dim, kind, direct_timing, .q4q5_panel16, &checksum),
        .Q6_K => {
            try benchQuantizedLinearVariant(allocator, cfg, label, &storage, input, direct_out, variant_out, rows, in_dim, out_dim, kind, direct_timing, .q6_panel16, &checksum);
        },
        .Q8_K => {
            try benchQuantizedLinearVariant(allocator, cfg, label, &storage, input, direct_out, variant_out, rows, in_dim, out_dim, kind, direct_timing, .q8_sdot_off, &checksum);
            try benchQuantizedLinearVariant(allocator, cfg, label, &storage, input, direct_out, variant_out, rows, in_dim, out_dim, kind, direct_timing, .q8_panel8, &checksum);
            try benchQuantizedLinearVariant(allocator, cfg, label, &storage, input, direct_out, variant_out, rows, in_dim, out_dim, kind, direct_timing, .q8_panel16, &checksum);
        },
        else => {},
    }

    for (0..cfg.warmup_iters) |_| {
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_weight, 0.0, dequant_out);
    }
    var dequant_sgemm_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_weight, 0.0, dequant_out);
        dequant_sgemm_ns += nowNs() - t;
        foldChecksum(&checksum, dequant_out[0]);
    }
    dequant_sgemm_ns /= cfg.measure_iters;

    const dequant_label = try std.fmt.allocPrint(allocator, "{s} directDefault_vs_dequantSgemm", .{label});
    defer allocator.free(dequant_label);
    printRow(dequant_label, direct_timing.ns, dequant_sgemm_ns, checksum);
    if (kind == .Q4_K or kind == .Q5_K or kind == .Q8_0) {
        printQuantLinearPhaseRow(dequant_label, direct_timing.stats, direct_timing.ns);
    }
}

fn benchQuantizedLinearTripleDirectVariants(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    label: []const u8,
    storage_a: *QuantizedStorage,
    storage_b: *const QuantizedStorage,
    storage_c: *const QuantizedStorage,
    input: []const f32,
    bias_a: []const f32,
    bias_b: []const f32,
    bias_c: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    kind: tensor_types.KnownTensorType,
) !void {
    if (kind != .Q4_K and kind != .Q5_K) return;

    const direct_a = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(direct_a);
    const direct_b = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(direct_b);
    const direct_c = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(direct_c);
    const variant_a = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(variant_a);
    const variant_b = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(variant_b);
    const variant_c = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(variant_c);
    const dequant_a = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(dequant_a);
    const dequant_b = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(dequant_b);
    const dequant_c = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(dequant_c);
    const dequant_out_a = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(dequant_out_a);
    const dequant_out_b = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(dequant_out_b);
    const dequant_out_c = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(dequant_out_c);

    try quant_codec.dequantizeToFloat32(storage_a.tensor_type, storage_a.raw_bytes, dequant_a);
    try quant_codec.dequantizeToFloat32(storage_b.tensor_type, storage_b.raw_bytes, dequant_b);
    try quant_codec.dequantizeToFloat32(storage_c.tensor_type, storage_c.raw_bytes, dequant_c);

    var checksum: f64 = 0;
    const variant_diff_tolerance: f32 = if (rows < 16 and in_dim / 256 < 3) 2e-2 else 1e-3;
    const default_ns = try timeQuantizedLinearTripleDirect(cfg, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, direct_a, direct_b, direct_c, rows, in_dim, out_dim, false, false, false, false);
    foldChecksum(&checksum, direct_a[0]);
    foldChecksum(&checksum, direct_b[0]);
    foldChecksum(&checksum, direct_c[0]);

    const panel16_split_ns = try timeQuantizedLinearTripleDirect(cfg, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, variant_a, variant_b, variant_c, rows, in_dim, out_dim, true, false, false, false);
    foldChecksum(&checksum, variant_a[0]);
    foldChecksum(&checksum, variant_b[0]);
    foldChecksum(&checksum, variant_c[0]);
    var max_abs: f32 = 0;
    for (direct_a, variant_a) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_b, variant_b) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_c, variant_c) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    if (max_abs > variant_diff_tolerance and !std.math.isNan(max_abs)) {
        std.debug.print("  WARN: quantTriple panel16Split {s} max diff = {d:.6}\n", .{ quantTypeName(kind), max_abs });
    }

    const panel16_fused_ns = try timeQuantizedLinearTripleDirect(cfg, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, variant_a, variant_b, variant_c, rows, in_dim, out_dim, true, true, false, false);
    foldChecksum(&checksum, variant_a[0]);
    foldChecksum(&checksum, variant_b[0]);
    foldChecksum(&checksum, variant_c[0]);
    max_abs = 0;
    for (direct_a, variant_a) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_b, variant_b) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_c, variant_c) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    if (max_abs > variant_diff_tolerance and !std.math.isNan(max_abs)) {
        std.debug.print("  WARN: quantTriple panel16Fused {s} max diff = {d:.6}\n", .{ quantTypeName(kind), max_abs });
    }

    const panel16_triple_dot_ns = try timeQuantizedLinearTripleDirect(cfg, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, variant_a, variant_b, variant_c, rows, in_dim, out_dim, true, true, true, false);
    foldChecksum(&checksum, variant_a[0]);
    foldChecksum(&checksum, variant_b[0]);
    foldChecksum(&checksum, variant_c[0]);
    max_abs = 0;
    for (direct_a, variant_a) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_b, variant_b) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_c, variant_c) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    if (max_abs > variant_diff_tolerance and !std.math.isNan(max_abs)) {
        std.debug.print("  WARN: quantTriple triplePanel16Dot {s} max diff = {d:.6}\n", .{ quantTypeName(kind), max_abs });
    }

    const packed_qkv_panel16_ns = try timeQuantizedLinearTripleDirect(cfg, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, variant_a, variant_b, variant_c, rows, in_dim, out_dim, true, true, true, true);
    foldChecksum(&checksum, variant_a[0]);
    foldChecksum(&checksum, variant_b[0]);
    foldChecksum(&checksum, variant_c[0]);
    max_abs = 0;
    for (direct_a, variant_a) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_b, variant_b) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_c, variant_c) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    if (max_abs > variant_diff_tolerance and !std.math.isNan(max_abs)) {
        std.debug.print("  WARN: quantTriple packedQKVPanel16 {s} max diff = {d:.6}\n", .{ quantTypeName(kind), max_abs });
    }

    const auto_selector = try timeQuantizedLinearTripleAutoSelector(cfg, storage_a, storage_b, storage_c, input, bias_a, bias_b, bias_c, variant_a, variant_b, variant_c, rows, in_dim, out_dim);
    foldChecksum(&checksum, variant_a[0]);
    foldChecksum(&checksum, variant_b[0]);
    foldChecksum(&checksum, variant_c[0]);
    max_abs = 0;
    for (direct_a, variant_a) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_b, variant_b) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    for (direct_c, variant_c) |direct, variant| max_abs = @max(max_abs, @abs(direct - variant));
    if (max_abs > variant_diff_tolerance and !std.math.isNan(max_abs)) {
        std.debug.print("  WARN: quantTriple autoSelector {s} max diff = {d:.6}\n", .{ quantTypeName(kind), max_abs });
    }

    for (0..cfg.warmup_iters) |_| {
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_a, 0.0, dequant_out_a);
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_b, 0.0, dequant_out_b);
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_c, 0.0, dequant_out_c);
        addBiasRowsBench(dequant_out_a, bias_a, rows, out_dim);
        addBiasRowsBench(dequant_out_b, bias_b, rows, out_dim);
        addBiasRowsBench(dequant_out_c, bias_c, rows, out_dim);
    }
    var dequant_sgemm_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_a, 0.0, dequant_out_a);
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_b, 0.0, dequant_out_b);
        productionSgemmTransBSync(rows, out_dim, in_dim, input, dequant_c, 0.0, dequant_out_c);
        addBiasRowsBench(dequant_out_a, bias_a, rows, out_dim);
        addBiasRowsBench(dequant_out_b, bias_b, rows, out_dim);
        addBiasRowsBench(dequant_out_c, bias_c, rows, out_dim);
        dequant_sgemm_ns += nowNs() - t;
        foldChecksum(&checksum, dequant_out_a[0]);
        foldChecksum(&checksum, dequant_out_b[0]);
        foldChecksum(&checksum, dequant_out_c[0]);
    }
    dequant_sgemm_ns /= cfg.measure_iters;

    const split_label = try std.fmt.allocPrint(allocator, "{s} tripleDefault_vs_panel16Split", .{label});
    defer allocator.free(split_label);
    printRow(split_label, default_ns, panel16_split_ns, checksum);

    const fused_label = try std.fmt.allocPrint(allocator, "{s} tripleDefault_vs_panel16Fused", .{label});
    defer allocator.free(fused_label);
    printRow(fused_label, default_ns, panel16_fused_ns, checksum);

    const fused_split_label = try std.fmt.allocPrint(allocator, "{s} panel16Split_vs_panel16Fused", .{label});
    defer allocator.free(fused_split_label);
    printRow(fused_split_label, panel16_split_ns, panel16_fused_ns, checksum);

    const triple_dot_label = try std.fmt.allocPrint(allocator, "{s} panel16Fused_vs_triplePanel16Dot", .{label});
    defer allocator.free(triple_dot_label);
    printRow(triple_dot_label, panel16_fused_ns, panel16_triple_dot_ns, checksum);

    const triple_dot_split_label = try std.fmt.allocPrint(allocator, "{s} panel16Split_vs_triplePanel16Dot", .{label});
    defer allocator.free(triple_dot_split_label);
    printRow(triple_dot_split_label, panel16_split_ns, panel16_triple_dot_ns, checksum);

    const packed_variant_name =
        if (rows >= 77 and rows <= 308 and (rows % 77 == 0 or rows % 8 == 0) and in_dim == 768 and (out_dim == 768 or out_dim == 3072)) "packedQKVPanel16MR8" else if (rows >= 4) "packedQKVPanel16MR4" else if (rows >= 2 and kind == .Q5_K) "packedQKVPanel16Q5MR2HeuristicNR16" else if (rows >= 2) "packedQKVPanel16MR2NR16" else "packedQKVPanel16ScalarTail";

    const packed_label = try std.fmt.allocPrint(allocator, "{s} triplePanel16Dot_vs_{s}", .{ label, packed_variant_name });
    defer allocator.free(packed_label);
    printRow(packed_label, panel16_triple_dot_ns, packed_qkv_panel16_ns, checksum);

    const packed_split_label = try std.fmt.allocPrint(allocator, "{s} panel16Split_vs_{s}", .{ label, packed_variant_name });
    defer allocator.free(packed_split_label);
    printRow(packed_split_label, panel16_split_ns, packed_qkv_panel16_ns, checksum);

    const best_forced_ns = @min(panel16_split_ns, @min(panel16_fused_ns, @min(panel16_triple_dot_ns, packed_qkv_panel16_ns)));
    const auto_route = packedQKVAutoSelectorRouteName(auto_selector.stats, auto_selector.route_name);
    const auto_best_label = try std.fmt.allocPrint(allocator, "{s} bestForcedQKV_vs_autoSelector[{s}]", .{ label, auto_route });
    defer allocator.free(auto_best_label);
    printRow(auto_best_label, best_forced_ns, auto_selector.ns, checksum);

    const auto_fused_label = try std.fmt.allocPrint(allocator, "{s} panel16Fused_vs_autoSelector[{s}]", .{ label, auto_route });
    defer allocator.free(auto_fused_label);
    printRow(auto_fused_label, panel16_fused_ns, auto_selector.ns, checksum);

    const dequant_label = try std.fmt.allocPrint(allocator, "{s} panel16Fused_vs_dequantSgemmTriple", .{label});
    defer allocator.free(dequant_label);
    printRow(dequant_label, panel16_fused_ns, dequant_sgemm_ns, checksum);

    const triple_dot_dequant_label = try std.fmt.allocPrint(allocator, "{s} triplePanel16Dot_vs_dequantSgemmTriple", .{label});
    defer allocator.free(triple_dot_dequant_label);
    printRow(triple_dot_dequant_label, panel16_triple_dot_ns, dequant_sgemm_ns, checksum);

    const packed_dequant_label = try std.fmt.allocPrint(allocator, "{s} {s}_vs_dequantSgemmTriple", .{ label, packed_variant_name });
    defer allocator.free(packed_dequant_label);
    printRow(packed_dequant_label, packed_qkv_panel16_ns, dequant_sgemm_ns, checksum);

    const auto_dequant_label = try std.fmt.allocPrint(allocator, "{s} autoSelector[{s}]_vs_dequantSgemmTriple", .{ label, auto_route });
    defer allocator.free(auto_dequant_label);
    printRow(auto_dequant_label, auto_selector.ns, dequant_sgemm_ns, checksum);
    printQuantTriplePhaseRow(auto_dequant_label, auto_selector.stats, auto_selector.ns);
}

fn benchQuantizedLinearPair(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    label: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    kind: tensor_types.KnownTensorType,
) !void {
    const tensor_type: tensor_types.TensorType = .{ .known = kind };
    const block_values = tensor_types.valuesPerBlock(tensor_type) orelse return error.UnsupportedQuantLinearBenchType;
    if (in_dim % block_values != 0) return error.InvalidQuantLinearBenchShape;

    const input = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input);
    const weight_a = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight_a);
    const weight_b = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight_b);
    const sep_a = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(sep_a);
    const sep_b = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(sep_b);
    const pair_a = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(pair_a);
    const pair_b = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(pair_b);

    var prng = std.Random.DefaultPrng.init(0xC11C_9A1F);
    const rand = prng.random();
    for (input) |*v| v.* = (rand.float(f32) - 0.5) * 2.0;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(@max(in_dim, 1))));
    for (weight_a) |*v| v.* = (rand.float(f32) - 0.5) * 2.0 * scale;
    for (weight_b) |*v| v.* = (rand.float(f32) - 0.5) * 2.0 * scale;

    const raw_a = try quantizeLinearWeight(allocator, weight_a, kind);
    errdefer allocator.free(raw_a);
    const shape_a = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) });
    errdefer allocator.free(shape_a);
    var storage_a = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_a,
        .shape = shape_a,
        .raw_owned = true,
        .allocator = allocator,
    };
    defer storage_a.deinit();

    const raw_b = try quantizeLinearWeight(allocator, weight_b, kind);
    errdefer allocator.free(raw_b);
    const shape_b = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) });
    errdefer allocator.free(shape_b);
    var storage_b = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_b,
        .shape = shape_b,
        .raw_owned = true,
        .allocator = allocator,
    };
    defer storage_b.deinit();
    try native_compute.prepareNativeQuantizedStorage(&storage_a);
    try native_compute.prepareNativeQuantizedStorage(&storage_b);

    var checksum: f64 = 0;

    for (0..cfg.warmup_iters) |_| {
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_a, input, sep_a, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_b, input, sep_b, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
    }
    var separate_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_a, input, sep_a, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_b, input, sep_b, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
        separate_ns += nowNs() - t;
        foldChecksum(&checksum, sep_a[0]);
        foldChecksum(&checksum, sep_b[0]);
    }

    for (0..cfg.warmup_iters) |_| {
        if (!(try native_compute.linearNoBiasQuantizedPair(cfg.io, allocator, &storage_a, &storage_b, input, pair_a, pair_b, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
    }
    var pair_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        if (!(try native_compute.linearNoBiasQuantizedPair(cfg.io, allocator, &storage_a, &storage_b, input, pair_a, pair_b, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
        pair_ns += nowNs() - t;
        foldChecksum(&checksum, pair_a[0]);
        foldChecksum(&checksum, pair_b[0]);
    }

    var max_abs: f32 = 0;
    for (sep_a, pair_a) |separate, pair| {
        const diff = @abs(separate - pair);
        if (diff > max_abs) max_abs = diff;
    }
    for (sep_b, pair_b) |separate, pair| {
        const diff = @abs(separate - pair);
        if (diff > max_abs) max_abs = diff;
    }
    if (max_abs > 1e-3 and !std.math.isNan(max_abs)) {
        std.debug.print("  WARN: quantPair {s} max diff = {d:.6}\n", .{ quantTypeName(kind), max_abs });
    }

    printRow(label, separate_ns / cfg.measure_iters, pair_ns / cfg.measure_iters, checksum);
}

fn benchQuantizedLinearTriple(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    label: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    kind: tensor_types.KnownTensorType,
) !void {
    const tensor_type: tensor_types.TensorType = .{ .known = kind };
    const block_values = tensor_types.valuesPerBlock(tensor_type) orelse return error.UnsupportedQuantLinearBenchType;
    if (in_dim % block_values != 0) return error.InvalidQuantLinearBenchShape;

    const input = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input);
    const weight_a = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight_a);
    const weight_b = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight_b);
    const weight_c = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight_c);
    const bias_a = try allocator.alloc(f32, out_dim);
    defer allocator.free(bias_a);
    const bias_b = try allocator.alloc(f32, out_dim);
    defer allocator.free(bias_b);
    const bias_c = try allocator.alloc(f32, out_dim);
    defer allocator.free(bias_c);
    const sep_a = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(sep_a);
    const sep_b = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(sep_b);
    const sep_c = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(sep_c);
    const triple_a = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(triple_a);
    const triple_b = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(triple_b);
    const triple_c = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(triple_c);

    var prng = std.Random.DefaultPrng.init(0xC11C_9A20);
    const rand = prng.random();
    for (input) |*v| v.* = (rand.float(f32) - 0.5) * 2.0;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(@max(in_dim, 1))));
    for (weight_a) |*v| v.* = (rand.float(f32) - 0.5) * 2.0 * scale;
    for (weight_b) |*v| v.* = (rand.float(f32) - 0.5) * 2.0 * scale;
    for (weight_c) |*v| v.* = (rand.float(f32) - 0.5) * 2.0 * scale;
    for (bias_a) |*v| v.* = (rand.float(f32) - 0.5) * 0.25;
    for (bias_b) |*v| v.* = (rand.float(f32) - 0.5) * 0.25;
    for (bias_c) |*v| v.* = (rand.float(f32) - 0.5) * 0.25;

    const raw_a = try quantizeLinearWeight(allocator, weight_a, kind);
    errdefer allocator.free(raw_a);
    const shape_a = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) });
    errdefer allocator.free(shape_a);
    var storage_a = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_a,
        .shape = shape_a,
        .raw_owned = true,
        .allocator = allocator,
    };
    defer storage_a.deinit();

    const raw_b = try quantizeLinearWeight(allocator, weight_b, kind);
    errdefer allocator.free(raw_b);
    const shape_b = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) });
    errdefer allocator.free(shape_b);
    var storage_b = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_b,
        .shape = shape_b,
        .raw_owned = true,
        .allocator = allocator,
    };
    defer storage_b.deinit();

    const raw_c = try quantizeLinearWeight(allocator, weight_c, kind);
    errdefer allocator.free(raw_c);
    const shape_c = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) });
    errdefer allocator.free(shape_c);
    var storage_c = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_c,
        .shape = shape_c,
        .raw_owned = true,
        .allocator = allocator,
    };
    defer storage_c.deinit();
    try native_compute.prepareNativeQuantizedStorage(&storage_a);
    try native_compute.prepareNativeQuantizedStorage(&storage_b);
    try native_compute.prepareNativeQuantizedStorage(&storage_c);

    var checksum: f64 = 0;

    for (0..cfg.warmup_iters) |_| {
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_a, input, sep_a, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
        addBiasRowsBench(sep_a, bias_a, rows, out_dim);
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_b, input, sep_b, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
        addBiasRowsBench(sep_b, bias_b, rows, out_dim);
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_c, input, sep_c, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
        addBiasRowsBench(sep_c, bias_c, rows, out_dim);
    }
    var separate_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_a, input, sep_a, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
        addBiasRowsBench(sep_a, bias_a, rows, out_dim);
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_b, input, sep_b, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
        addBiasRowsBench(sep_b, bias_b, rows, out_dim);
        if (!(try native_compute.linearNoBiasQuantized(cfg.io, &storage_c, input, sep_c, rows, in_dim, out_dim))) return error.UnsupportedQuantLinearBenchType;
        addBiasRowsBench(sep_c, bias_c, rows, out_dim);
        separate_ns += nowNs() - t;
        foldChecksum(&checksum, sep_a[0]);
        foldChecksum(&checksum, sep_b[0]);
        foldChecksum(&checksum, sep_c[0]);
    }

    for (0..cfg.warmup_iters) |_| {
        if (!(try native_compute.linearQuantizedTriple(cfg.io, &storage_a, &storage_b, &storage_c, input, bias_a, bias_b, bias_c, triple_a, triple_b, triple_c, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
    }
    var triple_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        if (!(try native_compute.linearQuantizedTriple(cfg.io, &storage_a, &storage_b, &storage_c, input, bias_a, bias_b, bias_c, triple_a, triple_b, triple_c, rows, in_dim, out_dim))) {
            return error.UnsupportedQuantLinearBenchType;
        }
        triple_ns += nowNs() - t;
        foldChecksum(&checksum, triple_a[0]);
        foldChecksum(&checksum, triple_b[0]);
        foldChecksum(&checksum, triple_c[0]);
    }

    var max_abs: f32 = 0;
    for (sep_a, triple_a) |separate, triple| {
        const diff = @abs(separate - triple);
        if (diff > max_abs) max_abs = diff;
    }
    for (sep_b, triple_b) |separate, triple| {
        const diff = @abs(separate - triple);
        if (diff > max_abs) max_abs = diff;
    }
    for (sep_c, triple_c) |separate, triple| {
        const diff = @abs(separate - triple);
        if (diff > max_abs) max_abs = diff;
    }
    if (max_abs > 1e-3 and !std.math.isNan(max_abs)) {
        std.debug.print("  WARN: quantTriple {s} max diff = {d:.6}\n", .{ quantTypeName(kind), max_abs });
    }

    printRow(label, separate_ns / cfg.measure_iters, triple_ns / cfg.measure_iters, checksum);
    if (cfg.include_direct_variants) {
        try benchQuantizedLinearTripleDirectVariants(allocator, cfg, label, &storage_a, &storage_b, &storage_c, input, bias_a, bias_b, bias_c, rows, in_dim, out_dim, kind);
    }
}

// --- Flash softmax block (lib/linalg attention.zig hot path) -----------

fn flashSoftmaxBlockBaseline(scores: []f32, V: []const f32, out: []f32, new_max: f32, cur_bkv: usize, head_dim: usize) f32 {
    // Mirrors the original `for (0..cur_bkv) |ki_local| { w = @exp(s - max);
    // axpy(w, V, out) }` body in flashAttentionHost / flashCausalAttentionHost
    // before the optimization.
    var block_sum: f32 = 0.0;
    for (0..cur_bkv) |ki_local| {
        const s = scores[ki_local];
        const w = @exp(s - new_max);
        if (w == 0.0) continue;
        block_sum += w;
        const v_ptr = V[ki_local * head_dim ..].ptr;
        // Inline the axpy so the bench doesn't get distorted by linalg
        // function-call overhead — both versions hit the same axpy.
        const a_splat: F32xN = @splat(w);
        var d: usize = 0;
        while (d + VEC_LEN <= head_dim) : (d += VEC_LEN) {
            const xv: F32xN = v_ptr[d..][0..VEC_LEN].*;
            const yv: F32xN = out[d..][0..VEC_LEN].*;
            out[d..][0..VEC_LEN].* = yv + a_splat * xv;
        }
        while (d < head_dim) : (d += 1) {
            out[d] += w * v_ptr[d];
        }
    }
    return block_sum;
}

fn flashSoftmaxBlockOptimized(scores: []f32, V: []const f32, out: []f32, new_max: f32, cur_bkv: usize, head_dim: usize) f32 {
    // What the optimized flash attention now does.
    const block_sum = linalg.primitives.expSubtractAndSum(scores[0..cur_bkv], new_max);
    for (0..cur_bkv) |ki_local| {
        const w = scores[ki_local];
        if (w == 0.0) continue;
        const v_ptr = V[ki_local * head_dim ..].ptr;
        const a_splat: F32xN = @splat(w);
        var d: usize = 0;
        while (d + VEC_LEN <= head_dim) : (d += VEC_LEN) {
            const xv: F32xN = v_ptr[d..][0..VEC_LEN].*;
            const yv: F32xN = out[d..][0..VEC_LEN].*;
            out[d..][0..VEC_LEN].* = yv + a_splat * xv;
        }
        while (d < head_dim) : (d += 1) {
            out[d] += w * v_ptr[d];
        }
    }
    return block_sum;
}

fn benchFlashSoftmaxBlock(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, q_rows: usize, cur_bkv: usize, head_dim: usize) !void {
    // Simulate q_rows iterations of the inner KV loop, which is what one
    // Q-block of the flash attention does.  Each iteration reads the same
    // score row from a fresh seed buffer to keep the bench isolated.
    const scores_seed = try allocator.alloc(f32, cur_bkv);
    defer allocator.free(scores_seed);
    const scores = try allocator.alloc(f32, cur_bkv);
    defer allocator.free(scores);
    const V = try allocator.alloc(f32, cur_bkv * head_dim);
    defer allocator.free(V);
    const out = try allocator.alloc(f32, head_dim);
    defer allocator.free(out);
    var prng = std.Random.DefaultPrng.init(0xF1A5_BAD0);
    for (scores_seed) |*v| v.* = (prng.random().float(f32) - 0.5) * 6.0;
    for (V) |*v| v.* = (prng.random().float(f32) - 0.5);
    var max_val: f32 = -std.math.inf(f32);
    for (scores_seed) |v| max_val = @max(max_val, v);

    var checksum: f64 = 0;

    for (0..cfg.warmup_iters) |_| {
        for (0..q_rows) |_| {
            @memcpy(scores, scores_seed);
            @memset(out, 0);
            _ = flashSoftmaxBlockBaseline(scores, V, out, max_val, cur_bkv, head_dim);
        }
    }
    var base_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        for (0..q_rows) |_| {
            @memcpy(scores, scores_seed);
            @memset(out, 0);
            const bs = flashSoftmaxBlockBaseline(scores, V, out, max_val, cur_bkv, head_dim);
            foldChecksum(&checksum, bs);
        }
        base_ns += nowNs() - t;
    }

    for (0..cfg.warmup_iters) |_| {
        for (0..q_rows) |_| {
            @memcpy(scores, scores_seed);
            @memset(out, 0);
            _ = flashSoftmaxBlockOptimized(scores, V, out, max_val, cur_bkv, head_dim);
        }
    }
    var opt_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const t = nowNs();
        for (0..q_rows) |_| {
            @memcpy(scores, scores_seed);
            @memset(out, 0);
            const bs = flashSoftmaxBlockOptimized(scores, V, out, max_val, cur_bkv, head_dim);
            foldChecksum(&checksum, bs);
        }
        opt_ns += nowNs() - t;
    }

    printRow(label, base_ns / cfg.measure_iters, opt_ns / cfg.measure_iters, checksum);
}

fn benchAvgPool(allocator: std.mem.Allocator, cfg: BenchConfig, label: []const u8, batch: usize, seq_len: usize, dim: usize) !void {
    const in = try allocator.alloc(f32, batch * seq_len * dim);
    defer allocator.free(in);
    const out = try allocator.alloc(f32, batch * dim);
    defer allocator.free(out);
    var prng = std.Random.DefaultPrng.init(0xABAD_C0DE);
    for (in) |*v| v.* = prng.random().float(f32);

    var checksum: f64 = 0;
    runPair(cfg, label, avgPoolBaseline, avgPoolOptimized, .{ out, in, batch, seq_len, dim }, &out[0], &checksum);
}
