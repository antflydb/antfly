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
const linalg = @import("termite_linalg");

const BenchConfig = struct {
    warmup_iters: usize = 2,
    measure_iters: usize = 5,
    dot_len: usize = 4096,
    attn_seq: usize = 64,
    num_heads: usize = 8,
    head_dim: usize = 64,
    channel_groups: usize = 4,
    primitive_repeats: usize = 2048,
};

const BenchResult = struct {
    dot_ns: u64,
    axpy_ns: u64,
    cross_ns: u64,
    deberta_ns: u64,
    channel_ns: u64,
    sgemm_qkv_ns: u64,
    sgemm_mlp_up_ns: u64,
    sgemm_transb_proj_ns: u64,
    sgemm_transb_qk_ns: u64,
    sgemm_transb_f16_proj_ns: u64,
    flash_attn_clip_text_ns: u64,
    flash_attn_bert_base_ns: u64,
    checksum: f64,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    const cfg = try parseArgs(init);
    const result = try runBench(allocator, cfg);

    std.debug.print(
        \\warmup_iters={}
        \\measure_iters={}
        \\dot_len={}
        \\attn_seq={}
        \\num_heads={}
        \\head_dim={}
        \\channel_groups={}
        \\primitive_repeats={}
        \\dot_ms={d:.3}
        \\axpy_ms={d:.3}
        \\cross_attention_ms={d:.3}
        \\deberta_attention_ms={d:.3}
        \\channel_attention_ms={d:.3}
        \\sgemm_qkv_77x512x512_ms={d:.3}
        \\sgemm_mlp_up_77x2048x512_ms={d:.3}
        \\sgemm_transb_proj_50x768x768_ms={d:.3}
        \\sgemm_transb_qk_77x77x512_ms={d:.3}
        \\sgemm_transb_f16_proj_50x768x768_ms={d:.3}
        \\flash_attn_clip_text_b1_seq77_h12_d64_ms={d:.3}
        \\flash_attn_bert_base_b1_seq512_h12_d64_ms={d:.3}
        \\checksum={d:.6}
        \\
    , .{
        cfg.warmup_iters,
        cfg.measure_iters,
        cfg.dot_len,
        cfg.attn_seq,
        cfg.num_heads,
        cfg.head_dim,
        cfg.channel_groups,
        cfg.primitive_repeats,
        nsToMs(result.dot_ns),
        nsToMs(result.axpy_ns),
        nsToMs(result.cross_ns),
        nsToMs(result.deberta_ns),
        nsToMs(result.channel_ns),
        nsToMs(result.sgemm_qkv_ns),
        nsToMs(result.sgemm_mlp_up_ns),
        nsToMs(result.sgemm_transb_proj_ns),
        nsToMs(result.sgemm_transb_qk_ns),
        nsToMs(result.sgemm_transb_f16_proj_ns),
        nsToMs(result.flash_attn_clip_text_ns),
        nsToMs(result.flash_attn_bert_base_ns),
        result.checksum,
    });
}

fn parseArgs(init: std.process.Init) !BenchConfig {
    var cfg = BenchConfig{};
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args_buf: [32][]const u8 = undefined;
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
        if (std.mem.eql(u8, arg, "--warmup-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.warmup_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.measure_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--dot-len")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.dot_len = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--attn-seq")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.attn_seq = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--num-heads")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.num_heads = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--head-dim")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.head_dim = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--channel-groups")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.channel_groups = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--primitive-repeats")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.primitive_repeats = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--help")) {
            try printUsage();
            std.process.exit(0);
        } else {
            return error.UnknownArgument;
        }
    }

    if (cfg.dot_len == 0 or cfg.attn_seq == 0 or cfg.num_heads == 0 or cfg.head_dim == 0) return error.InvalidBenchShape;
    if (cfg.channel_groups == 0 or cfg.head_dim % cfg.channel_groups != 0) return error.InvalidBenchShape;
    if (cfg.primitive_repeats == 0) return error.InvalidBenchShape;
    return cfg;
}

fn printUsage() !void {
    std.debug.print(
        \\Usage: termite-linalg-bench [options]
        \\  --warmup-iters N
        \\  --measure-iters N
        \\  --dot-len N
        \\  --attn-seq N
        \\  --num-heads N
        \\  --head-dim N
        \\  --channel-groups N
        \\  --primitive-repeats N
        \\
    , .{});
}

fn runBench(allocator: std.mem.Allocator, cfg: BenchConfig) !BenchResult {
    var prng = std.Random.DefaultPrng.init(0xdecafbad);
    const rand = prng.random();

    const hidden = cfg.num_heads * cfg.head_dim;
    const dot_a = try allocator.alloc(f32, cfg.dot_len);
    defer allocator.free(dot_a);
    const dot_b = try allocator.alloc(f32, cfg.dot_len);
    defer allocator.free(dot_b);
    const axpy_y = try allocator.alloc(f32, cfg.dot_len);
    defer allocator.free(axpy_y);

    const q = try allocator.alloc(f32, cfg.attn_seq * hidden);
    defer allocator.free(q);
    const k = try allocator.alloc(f32, cfg.attn_seq * hidden);
    defer allocator.free(k);
    const v = try allocator.alloc(f32, cfg.attn_seq * hidden);
    defer allocator.free(v);
    const mask = try allocator.alloc(i64, cfg.attn_seq);
    defer allocator.free(mask);

    const num_rel = 2 * cfg.attn_seq - 1;
    const q_r = try allocator.alloc(f32, num_rel * hidden);
    defer allocator.free(q_r);
    const k_r = try allocator.alloc(f32, num_rel * hidden);
    defer allocator.free(k_r);

    const qkv = try allocator.alloc(f32, cfg.attn_seq * hidden * 3);
    defer allocator.free(qkv);
    const channel_out = try allocator.alloc(f32, cfg.attn_seq * hidden);
    defer allocator.free(channel_out);

    fillRandom(rand, dot_a);
    fillRandom(rand, dot_b);
    fillRandom(rand, axpy_y);
    fillRandom(rand, q);
    fillRandom(rand, k);
    fillRandom(rand, v);
    fillRandom(rand, q_r);
    fillRandom(rand, k_r);
    fillRandom(rand, qkv);
    @memset(channel_out, 0.0);
    for (mask, 0..) |*entry, i| {
        entry.* = if (i + 7 < cfg.attn_seq) 1 else @as(i64, @intCast((i & 1) ^ 1));
    }

    const dot_result = try benchDot(cfg, dot_a, dot_b);
    const axpy_result = try benchAxpy(cfg, dot_a, axpy_y);
    const cross_result = try benchCrossAttention(allocator, cfg, q, k, v, mask);
    const deberta_result = try benchDebertaAttention(allocator, cfg, q, k, v, q_r, k_r, mask);
    const channel_result = try benchChannelAttention(allocator, cfg, channel_out, qkv, hidden);

    // Representative dense matmul shapes from CLIP+CLAP transformer encoders.
    const sgemm_qkv = try benchSgemm(allocator, cfg, 77, 512, 512);
    const sgemm_mlp_up = try benchSgemm(allocator, cfg, 77, 2048, 512);
    const sgemm_transb_proj = try benchSgemmTransB(allocator, cfg, 50, 768, 768);
    const sgemm_transb_qk = try benchSgemmTransB(allocator, cfg, 77, 77, 512);
    const sgemm_transb_f16_proj = try benchSgemmTransBF16(allocator, cfg, 50, 768, 768);
    // Flash attention covers the encoder-style bidirectional path used by
    // sentence-transformers, BERT, rerankers, Whisper-encoder, and CLIP/CLAP
    // text/vision.  Two representative shapes: CLIP text (small, where
    // materialized softmax was traditionally faster) and BERT-base (where
    // flash wins clearly).
    const flash_clip_text = try benchFlashAttn(allocator, cfg, 1, 77, 12, 64);
    const flash_bert_base = try benchFlashAttn(allocator, cfg, 1, 512, 12, 64);

    return .{
        .dot_ns = dot_result.ns,
        .axpy_ns = axpy_result.ns,
        .cross_ns = cross_result.ns,
        .deberta_ns = deberta_result.ns,
        .channel_ns = channel_result.ns,
        .sgemm_qkv_ns = sgemm_qkv.ns,
        .sgemm_mlp_up_ns = sgemm_mlp_up.ns,
        .sgemm_transb_proj_ns = sgemm_transb_proj.ns,
        .sgemm_transb_qk_ns = sgemm_transb_qk.ns,
        .sgemm_transb_f16_proj_ns = sgemm_transb_f16_proj.ns,
        .flash_attn_clip_text_ns = flash_clip_text.ns,
        .flash_attn_bert_base_ns = flash_bert_base.ns,
        .checksum = dot_result.checksum + axpy_result.checksum + cross_result.checksum +
            deberta_result.checksum + channel_result.checksum +
            sgemm_qkv.checksum + sgemm_mlp_up.checksum + sgemm_transb_proj.checksum + sgemm_transb_qk.checksum +
            sgemm_transb_f16_proj.checksum +
            flash_clip_text.checksum + flash_bert_base.checksum,
    };
}

fn benchSgemmTransBF16(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    m: usize,
    n: usize,
    k: usize,
) !TimedChecksum {
    var prng = std.Random.DefaultPrng.init(0xc5c5c5c5);
    const rand = prng.random();
    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b = try allocator.alloc(f16, n * k);
    defer allocator.free(b);
    const c = try allocator.alloc(f32, m * n);
    defer allocator.free(c);
    fillRandom(rand, a);
    for (b) |*v| v.* = @floatCast(rand.float(f32) * 2.0 - 1.0);
    @memset(c, 0.0);

    var checksum: f64 = 0.0;
    for (0..cfg.warmup_iters) |_| {
        linalg.sgemmTransBF16WeightsSync(m, n, k, 1.0, a, b, 0.0, c);
        checksum += c[0];
    }
    const start_ns = nowNs();
    for (0..cfg.measure_iters) |_| {
        linalg.sgemmTransBF16WeightsSync(m, n, k, 1.0, a, b, 0.0, c);
        checksum += c[0];
    }
    return .{ .ns = nowNs() - start_ns, .checksum = checksum };
}

fn benchFlashAttn(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    batch: usize,
    seq: usize,
    num_heads: usize,
    head_dim: usize,
) !TimedChecksum {
    var prng = std.Random.DefaultPrng.init(0xfa57feed);
    const rand = prng.random();
    const total = batch * seq * num_heads * head_dim;
    const Q = try allocator.alloc(f32, total);
    defer allocator.free(Q);
    const K = try allocator.alloc(f32, total);
    defer allocator.free(K);
    const V = try allocator.alloc(f32, total);
    defer allocator.free(V);
    fillRandom(rand, Q);
    fillRandom(rand, K);
    fillRandom(rand, V);
    const mask = try allocator.alloc(i64, batch * seq);
    defer allocator.free(mask);
    for (mask) |*m| m.* = 1;

    var checksum: f64 = 0.0;
    for (0..cfg.warmup_iters) |_| {
        const out = try linalg.flashAttentionHost(allocator, Q, K, V, null, mask, batch, seq, num_heads, head_dim);
        checksum += out[0];
        allocator.free(out);
    }
    const start_ns = nowNs();
    for (0..cfg.measure_iters) |_| {
        const out = try linalg.flashAttentionHost(allocator, Q, K, V, null, mask, batch, seq, num_heads, head_dim);
        checksum += out[0];
        allocator.free(out);
    }
    return .{ .ns = nowNs() - start_ns, .checksum = checksum };
}

fn benchSgemm(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    m: usize,
    n: usize,
    k: usize,
) !TimedChecksum {
    var prng = std.Random.DefaultPrng.init(0xa5a5a5a5);
    const rand = prng.random();
    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, k * n);
    defer allocator.free(b);
    const c = try allocator.alloc(f32, m * n);
    defer allocator.free(c);
    fillRandom(rand, a);
    fillRandom(rand, b);
    @memset(c, 0.0);

    var checksum: f64 = 0.0;
    for (0..cfg.warmup_iters) |_| {
        linalg.sgemmSync(m, n, k, 1.0, a, b, 0.0, c);
        checksum += c[0];
    }
    const start_ns = nowNs();
    for (0..cfg.measure_iters) |_| {
        linalg.sgemmSync(m, n, k, 1.0, a, b, 0.0, c);
        checksum += c[0];
    }
    return .{ .ns = nowNs() - start_ns, .checksum = checksum };
}

fn benchSgemmTransB(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    m: usize,
    n: usize,
    k: usize,
) !TimedChecksum {
    var prng = std.Random.DefaultPrng.init(0xb5b5b5b5);
    const rand = prng.random();
    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, n * k);
    defer allocator.free(b);
    const c = try allocator.alloc(f32, m * n);
    defer allocator.free(c);
    fillRandom(rand, a);
    fillRandom(rand, b);
    @memset(c, 0.0);

    var checksum: f64 = 0.0;
    for (0..cfg.warmup_iters) |_| {
        linalg.sgemmTransBSync(m, n, k, 1.0, a, b, 0.0, c);
        checksum += c[0];
    }
    const start_ns = nowNs();
    for (0..cfg.measure_iters) |_| {
        linalg.sgemmTransBSync(m, n, k, 1.0, a, b, 0.0, c);
        checksum += c[0];
    }
    return .{ .ns = nowNs() - start_ns, .checksum = checksum };
}

const TimedChecksum = struct {
    ns: u64,
    checksum: f64,
};

fn benchDot(cfg: BenchConfig, a: []const f32, b: []const f32) !TimedChecksum {
    var checksum: f64 = 0.0;
    for (0..cfg.warmup_iters) |_| {
        for (0..cfg.primitive_repeats) |_| {
            checksum += linalg.dot(a, b);
        }
    }

    const start_ns = nowNs();
    for (0..cfg.measure_iters) |_| {
        for (0..cfg.primitive_repeats) |_| {
            checksum += linalg.dot(a, b);
        }
    }
    return .{ .ns = nowNs() - start_ns, .checksum = checksum };
}

fn benchAxpy(cfg: BenchConfig, x: []const f32, y: []f32) !TimedChecksum {
    var checksum: f64 = 0.0;
    for (0..cfg.warmup_iters) |_| {
        linalg.axpy(0.25, x, y);
    }

    const start_ns = nowNs();
    for (0..cfg.measure_iters) |_| {
        for (0..cfg.primitive_repeats) |_| {
            linalg.axpy(0.25, x, y);
        }
        checksum += y[0];
    }
    return .{ .ns = nowNs() - start_ns, .checksum = checksum };
}

fn benchCrossAttention(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    q: []const f32,
    k: []const f32,
    v: []const f32,
    mask: []const i64,
) !TimedChecksum {
    var checksum: f64 = 0.0;
    for (0..cfg.warmup_iters) |_| {
        const out = try linalg.crossAttentionHost(allocator, q, k, v, mask, 1, cfg.attn_seq, cfg.attn_seq, cfg.num_heads, cfg.head_dim);
        checksum += out[0];
        allocator.free(out);
    }

    const start_ns = nowNs();
    for (0..cfg.measure_iters) |_| {
        const out = try linalg.crossAttentionHost(allocator, q, k, v, mask, 1, cfg.attn_seq, cfg.attn_seq, cfg.num_heads, cfg.head_dim);
        checksum += out[0];
        allocator.free(out);
    }
    return .{ .ns = nowNs() - start_ns, .checksum = checksum };
}

fn benchDebertaAttention(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    q: []const f32,
    k: []const f32,
    v: []const f32,
    q_r: []const f32,
    k_r: []const f32,
    mask: []const i64,
) !TimedChecksum {
    var checksum: f64 = 0.0;
    for (0..cfg.warmup_iters) |_| {
        const out = try linalg.debertaDisentangledAttentionHost(allocator, q, k, v, q_r, k_r, mask, 1, cfg.attn_seq, cfg.num_heads, cfg.head_dim);
        checksum += out[0];
        allocator.free(out);
    }

    const start_ns = nowNs();
    for (0..cfg.measure_iters) |_| {
        const out = try linalg.debertaDisentangledAttentionHost(allocator, q, k, v, q_r, k_r, mask, 1, cfg.attn_seq, cfg.num_heads, cfg.head_dim);
        checksum += out[0];
        allocator.free(out);
    }
    return .{ .ns = nowNs() - start_ns, .checksum = checksum };
}

fn benchChannelAttention(
    allocator: std.mem.Allocator,
    cfg: BenchConfig,
    out: []f32,
    qkv: []const f32,
    dim: usize,
) !TimedChecksum {
    var checksum: f64 = 0.0;
    for (0..cfg.warmup_iters) |_| {
        try linalg.channelAttention(allocator, out, qkv, 1, cfg.attn_seq, dim, cfg.channel_groups);
        checksum += out[0];
    }

    const start_ns = nowNs();
    for (0..cfg.measure_iters) |_| {
        try linalg.channelAttention(allocator, out, qkv, 1, cfg.attn_seq, dim, cfg.channel_groups);
        checksum += out[0];
    }
    return .{ .ns = nowNs() - start_ns, .checksum = checksum };
}

fn fillRandom(rand: std.Random, data: []f32) void {
    for (data) |*value| {
        value.* = rand.float(f32) * 2.0 - 1.0;
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn nowNs() u64 {
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return 0,
    }
}
