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

//! Whisper on tile kernels: every tensor operation in the encoder and
//! decoder is one of the tile kernels in `kernels` below, so the same
//! descriptions render the Metal and CUDA sources (`gpu_model.zig`).
//! This file runs them through the CPU lowerings.
//!
//! Weights are the Hugging Face `model.safetensors` (f32) and
//! `config.json`, loaded as-is: `nn.Linear` weights are already the
//! [out, in] layout the tile GEMM takes, and conv weights [out, in, k]
//! flatten to the [out, in * k] the im2col GEMM takes.

const std = @import("std");
const fexpr = @import("fexpr.zig");
const gemm = @import("gemm.zig");
const norm = @import("norm.zig");
const attention = @import("attention.zig");
const layout = @import("layout.zig");
const tile = @import("tile.zig");
const cpu = @import("cpu.zig");
const whisper_logits = @import("whisper_logits.zig");

const acc = fexpr.acc;

/// Per-kernel-kind wall time, printed by `printProfile` (CPU lowering).
pub var profile_enabled = false;
var profile_ns: [5]u64 = @splat(0);
const Kind = enum { gemm, attention, layer_norm, im2col, other };

fn clockNs() u64 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn timed(kind: Kind, start: u64) void {
    if (profile_enabled) profile_ns[@backingInt(kind)] += clockNs() - start;
}

pub fn printProfile(label: []const u8) void {
    if (!profile_enabled) return;
    std.debug.print("  {s}: gemm {d} ms, attention {d} ms, layer_norm {d} ms, im2col {d} ms\n", .{ label, profile_ns[0] / std.time.ns_per_ms, profile_ns[1] / std.time.ns_per_ms, profile_ns[2] / std.time.ns_per_ms, profile_ns[3] / std.time.ns_per_ms });
    profile_ns = @splat(0);
}

/// Whisper's head_dim is 64 for every released size, so q * 64^-0.5 = q / 8
/// fuses into the q projection (as `WhisperAttention` scales q_proj output).
pub const head_dim = 64;

/// Every kernel the model launches.
pub const kernels = struct {
    pub const im2col_mel: layout.Im2col = .{ .name = "antfly_tile_whisper_im2col_mel", .kernel = 3, .stride = 1, .pad = 1, .layout = .channel_major };
    pub const im2col_hidden: layout.Im2col = .{ .name = "antfly_tile_whisper_im2col_hidden", .kernel = 3, .stride = 2, .pad = 1, .layout = .time_major };
    pub const linear: gemm.Gemm = .{ .name = "antfly_tile_gemm" };
    pub const linear_bias: gemm.Gemm = .{ .name = "antfly_tile_gemm_bias", .epilogue = acc.plus(.bias) };
    pub const linear_query: gemm.Gemm = .{ .name = "antfly_tile_gemm_bias_scale", .epilogue = acc.plus(.bias).times(fexpr.lit(0.125)) };
    pub const linear_gelu: gemm.Gemm = .{ .name = "antfly_tile_gemm_bias_gelu", .epilogue = acc.plus(.bias).geluOf() };
    pub const linear_residual: gemm.Gemm = .{ .name = "antfly_tile_gemm_bias_residual", .epilogue = acc.plus(.bias).plus(.residual) };
    pub const conv_positions: gemm.Gemm = .{ .name = "antfly_tile_gemm_bias_gelu_residual", .epilogue = acc.plus(.bias).geluOf().plus(.residual) };
    pub const layer_norm: norm.LayerNorm = .{ .name = "antfly_tile_layer_norm", .eps = 1e-5 };
    pub const attend: attention.Attention = .{ .name = "antfly_tile_attention_hd64", .head_dim = head_dim };
    pub const embed: layout.Embed = .{ .name = "antfly_tile_whisper_embed" };
    pub const logits = whisper_logits.kernel;
};

pub const Config = struct {
    d_model: usize,
    encoder_layers: usize,
    decoder_layers: usize,
    encoder_heads: usize,
    decoder_heads: usize,
    encoder_ffn: usize,
    decoder_ffn: usize,
    num_mel_bins: usize,
    vocab_size: usize,
    audio_ctx: usize,
    text_ctx: usize,
    eot: u32,

    pub fn parse(allocator: std.mem.Allocator, json: []const u8) !Config {
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json, .{});
        defer parsed.deinit();
        const o = parsed.value.object;
        const get = struct {
            fn f(obj: std.json.ObjectMap, key: []const u8) !usize {
                const v = obj.get(key) orelse return error.MissingConfigField;
                return @intCast(v.integer);
            }
        }.f;
        return .{
            .d_model = try get(o, "d_model"),
            .encoder_layers = try get(o, "encoder_layers"),
            .decoder_layers = try get(o, "decoder_layers"),
            .encoder_heads = try get(o, "encoder_attention_heads"),
            .decoder_heads = try get(o, "decoder_attention_heads"),
            .encoder_ffn = try get(o, "encoder_ffn_dim"),
            .decoder_ffn = try get(o, "decoder_ffn_dim"),
            .num_mel_bins = try get(o, "num_mel_bins"),
            .vocab_size = try get(o, "vocab_size"),
            .audio_ctx = try get(o, "max_source_positions"),
            .text_ctx = try get(o, "max_target_positions"),
            .eot = @intCast(try get(o, "eos_token_id")),
        };
    }

    pub fn melFrames(c: Config) usize {
        return 2 * c.audio_ctx;
    }
};

/// f32 tensors from a safetensors file, borrowed from one aligned buffer.
pub const SafeTensors = struct {
    bytes: []align(64) u8,
    header: std.json.Parsed(std.json.Value),
    data_start: usize,

    pub fn load(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !SafeTensors {
        const bytes = try std.Io.Dir.cwd().readFileAllocOptions(io, path, allocator, .limited(8 << 30), .@"64", null);
        errdefer allocator.free(bytes);
        if (bytes.len < 8) return error.InvalidSafeTensors;
        const header_len = std.mem.readInt(u64, bytes[0..8], .little);
        if (8 + header_len > bytes.len) return error.InvalidSafeTensors;
        const header = try std.json.parseFromSlice(std.json.Value, allocator, bytes[8..][0..header_len], .{});
        return .{ .bytes = bytes, .header = header, .data_start = 8 + header_len };
    }

    pub fn deinit(self: *SafeTensors, allocator: std.mem.Allocator) void {
        self.header.deinit();
        allocator.free(self.bytes);
    }

    pub fn get(self: *const SafeTensors, name: []const u8, expected_len: usize) ![]const f32 {
        const entry = self.header.value.object.get(name) orelse {
            std.debug.print("missing tensor {s}\n", .{name});
            return error.MissingTensor;
        };
        if (!std.mem.eql(u8, entry.object.get("dtype").?.string, "F32")) return error.UnsupportedDtype;
        const offsets = entry.object.get("data_offsets").?.array.items;
        const begin = self.data_start + @as(usize, @intCast(offsets[0].integer));
        const end = self.data_start + @as(usize, @intCast(offsets[1].integer));
        if (end > self.bytes.len or (end - begin) != expected_len * 4) {
            std.debug.print("tensor {s}: {d} bytes, expected {d}\n", .{ name, end - begin, expected_len * 4 });
            return error.TensorShapeMismatch;
        }
        if (begin % 4 != 0) return error.MisalignedTensor;
        return @as([*]const f32, @ptrCast(@alignCast(self.bytes.ptr + begin)))[0..expected_len];
    }
};

const Norm = struct { gamma: []const f32, beta: []const f32 };
const Linear = struct { w: []const f32, b: []const f32 = &.{} };
const Attn = struct { q: Linear, k: Linear, v: Linear, o: Linear };

const EncoderLayer = struct { ln1: Norm, attn: Attn, ln2: Norm, fc1: Linear, fc2: Linear };
const DecoderLayer = struct { ln1: Norm, self_attn: Attn, ln2: Norm, cross: Attn, ln3: Norm, fc1: Linear, fc2: Linear };

pub const Model = struct {
    allocator: std.mem.Allocator,
    cfg: Config,
    tensors: SafeTensors,
    conv1: Linear,
    conv2: Linear,
    enc_pos: []const f32,
    enc_layers: []EncoderLayer,
    enc_ln: Norm,
    tok_emb: []const f32,
    dec_pos: []const f32,
    dec_layers: []DecoderLayer,
    dec_ln: Norm,

    pub fn load(allocator: std.mem.Allocator, io: std.Io, model_dir: []const u8) !Model {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const config_json = try std.Io.Dir.cwd().readFileAlloc(io, try std.fmt.bufPrint(&path_buf, "{s}/config.json", .{model_dir}), allocator, .limited(1 << 20));
        defer allocator.free(config_json);
        const cfg = try Config.parse(allocator, config_json);
        var st = try SafeTensors.load(allocator, io, try std.fmt.bufPrint(&path_buf, "{s}/model.safetensors", .{model_dir}));
        errdefer st.deinit(allocator);
        const d = cfg.d_model;

        var name_buf: [256]u8 = undefined;
        const W = struct {
            st: *const SafeTensors,
            buf: *[256]u8,
            fn get(w: @This(), comptime fmt: []const u8, args: anytype, len: usize) ![]const f32 {
                return w.st.get(try std.fmt.bufPrint(w.buf, fmt, args), len);
            }
            fn norm(w: @This(), comptime prefix: []const u8, args: anytype, dim: usize) !Norm {
                return .{ .gamma = try w.get(prefix ++ ".weight", args, dim), .beta = try w.get(prefix ++ ".bias", args, dim) };
            }
            fn linear(w: @This(), comptime prefix: []const u8, args: anytype, out: usize, in: usize, bias: bool) !Linear {
                return .{ .w = try w.get(prefix ++ ".weight", args, out * in), .b = if (bias) try w.get(prefix ++ ".bias", args, out) else &.{} };
            }
            fn attn(w: @This(), comptime prefix: []const u8, args: anytype, dim: usize) !Attn {
                return .{
                    .q = try w.linear(prefix ++ ".q_proj", args, dim, dim, true),
                    .k = try w.linear(prefix ++ ".k_proj", args, dim, dim, false),
                    .v = try w.linear(prefix ++ ".v_proj", args, dim, dim, true),
                    .o = try w.linear(prefix ++ ".out_proj", args, dim, dim, true),
                };
            }
        };
        const w: W = .{ .st = &st, .buf = &name_buf };

        const enc_layers = try allocator.alloc(EncoderLayer, cfg.encoder_layers);
        errdefer allocator.free(enc_layers);
        for (enc_layers, 0..) |*l, i| l.* = .{
            .ln1 = try w.norm("model.encoder.layers.{d}.self_attn_layer_norm", .{i}, d),
            .attn = try w.attn("model.encoder.layers.{d}.self_attn", .{i}, d),
            .ln2 = try w.norm("model.encoder.layers.{d}.final_layer_norm", .{i}, d),
            .fc1 = try w.linear("model.encoder.layers.{d}.fc1", .{i}, cfg.encoder_ffn, d, true),
            .fc2 = try w.linear("model.encoder.layers.{d}.fc2", .{i}, d, cfg.encoder_ffn, true),
        };
        const dec_layers = try allocator.alloc(DecoderLayer, cfg.decoder_layers);
        errdefer allocator.free(dec_layers);
        for (dec_layers, 0..) |*l, i| l.* = .{
            .ln1 = try w.norm("model.decoder.layers.{d}.self_attn_layer_norm", .{i}, d),
            .self_attn = try w.attn("model.decoder.layers.{d}.self_attn", .{i}, d),
            .ln2 = try w.norm("model.decoder.layers.{d}.encoder_attn_layer_norm", .{i}, d),
            .cross = try w.attn("model.decoder.layers.{d}.encoder_attn", .{i}, d),
            .ln3 = try w.norm("model.decoder.layers.{d}.final_layer_norm", .{i}, d),
            .fc1 = try w.linear("model.decoder.layers.{d}.fc1", .{i}, cfg.decoder_ffn, d, true),
            .fc2 = try w.linear("model.decoder.layers.{d}.fc2", .{i}, d, cfg.decoder_ffn, true),
        };
        return .{
            .allocator = allocator,
            .cfg = cfg,
            .tensors = st,
            .conv1 = try w.linear("model.encoder.conv1", .{}, d, cfg.num_mel_bins * 3, true),
            .conv2 = try w.linear("model.encoder.conv2", .{}, d, d * 3, true),
            .enc_pos = try w.get("model.encoder.embed_positions.weight", .{}, cfg.audio_ctx * d),
            .enc_layers = enc_layers,
            .enc_ln = try w.norm("model.encoder.layer_norm", .{}, d),
            .tok_emb = try w.get("model.decoder.embed_tokens.weight", .{}, cfg.vocab_size * d),
            .dec_pos = try w.get("model.decoder.embed_positions.weight", .{}, cfg.text_ctx * d),
            .dec_layers = dec_layers,
            .dec_ln = try w.norm("model.decoder.layer_norm", .{}, d),
        };
    }

    pub fn deinit(self: *Model) void {
        self.allocator.free(self.enc_layers);
        self.allocator.free(self.dec_layers);
        self.tensors.deinit(self.allocator);
    }

    fn linear(comptime g: gemm.Gemm, x: []const f32, l: Linear, out: []f32, residual: ?[]const f32, m: usize, n: usize, k: usize) void {
        const start = if (profile_enabled) clockNs() else 0;
        defer timed(.gemm, start);
        gemm.run(g, .{ .x = x, .w = l.w, .bias = l.b, .residual = if (residual) |r| r.ptr else null, .out = out.ptr, .m = m, .n = n, .k = k });
    }

    fn layerNorm(x: []const f32, n: Norm, out: []f32, rows: usize, dim: usize) void {
        const start = if (profile_enabled) clockNs() else 0;
        defer timed(.layer_norm, start);
        norm.run(kernels.layer_norm, .{ .x = x, .gamma = n.gamma, .beta = n.beta, .out = out, .rows = rows, .dim = dim });
    }

    /// mel: [num_mel_bins, 2 * audio_ctx] (the feature extractor layout).
    /// out: [audio_ctx, d_model].
    fn attend(args: attention.Args) void {
        const start = if (profile_enabled) clockNs() else 0;
        defer timed(.attention, start);
        attention.run(kernels.attend, args);
    }

    pub fn encode(self: *const Model, mel: []const f32, out: []f32) !void {
        const c = self.cfg;
        const d = c.d_model;
        const t_mel = c.melFrames();
        const t = c.audio_ctx;
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        // Conv stem: two im2col GEMMs; the second adds positions in its epilogue.
        const cols1 = try a.alloc(f32, t_mel * c.num_mel_bins * 3);
        const im2col_start = if (profile_enabled) clockNs() else 0;
        layout.im2col(kernels.im2col_mel, .{ .x = mel, .out = cols1, .channels = c.num_mel_bins, .t_in = t_mel });
        timed(.im2col, im2col_start);
        const h1 = try a.alloc(f32, t_mel * d);
        linear(kernels.linear_gelu, cols1, self.conv1, h1, null, t_mel, d, c.num_mel_bins * 3);
        const cols2 = try a.alloc(f32, t * d * 3);
        layout.im2col(kernels.im2col_hidden, .{ .x = h1, .out = cols2, .channels = d, .t_in = t_mel });
        const h = out;
        linear(kernels.conv_positions, cols2, self.conv2, h, self.enc_pos, t, d, d * 3);

        const xn = try a.alloc(f32, t * d);
        const q = try a.alloc(f32, t * d);
        const k = try a.alloc(f32, t * d);
        const v = try a.alloc(f32, t * d);
        const ctx = try a.alloc(f32, t * d);
        const ff = try a.alloc(f32, t * c.encoder_ffn);
        for (self.enc_layers) |l| {
            layerNorm(h, l.ln1, xn, t, d);
            linear(kernels.linear_query, xn, l.attn.q, q, null, t, d, d);
            linear(kernels.linear, xn, l.attn.k, k, null, t, d, d);
            linear(kernels.linear_bias, xn, l.attn.v, v, null, t, d, d);
            attend(.{ .q = q, .k = k, .v = v, .out = ctx, .tq = t, .tk = t, .heads = c.encoder_heads, .ld_q = d, .ld_k = d, .ld_v = d, .ld_o = d, .scale = 1 });
            linear(kernels.linear_residual, ctx, l.attn.o, h, h, t, d, d);
            layerNorm(h, l.ln2, xn, t, d);
            linear(kernels.linear_gelu, xn, l.fc1, ff, null, t, c.encoder_ffn, d);
            linear(kernels.linear_residual, ff, l.fc2, h, h, t, d, c.encoder_ffn);
        }
        layerNorm(h, self.enc_ln, h, t, d);
    }

    pub const Decoder = struct {
        model: *const Model,
        arena: std.heap.ArenaAllocator,
        cross_k: [][]f32,
        cross_v: [][]f32,
        self_k: [][]f32,
        self_v: [][]f32,
        len: usize = 0,
        logits: []f32,

        pub fn init(model: *const Model, encoded: []const f32) !Decoder {
            const c = model.cfg;
            const d = c.d_model;
            var arena = std.heap.ArenaAllocator.init(model.allocator);
            errdefer arena.deinit();
            const a = arena.allocator();
            var dec: Decoder = .{
                .model = model,
                .arena = undefined,
                .cross_k = try a.alloc([]f32, c.decoder_layers),
                .cross_v = try a.alloc([]f32, c.decoder_layers),
                .self_k = try a.alloc([]f32, c.decoder_layers),
                .self_v = try a.alloc([]f32, c.decoder_layers),
                .logits = try a.alloc(f32, c.vocab_size),
            };
            // Cross-attention keys and values depend only on the audio.
            for (model.dec_layers, 0..) |l, i| {
                dec.cross_k[i] = try a.alloc(f32, c.audio_ctx * d);
                dec.cross_v[i] = try a.alloc(f32, c.audio_ctx * d);
                linear(kernels.linear, encoded, l.cross.k, dec.cross_k[i], null, c.audio_ctx, d, d);
                linear(kernels.linear_bias, encoded, l.cross.v, dec.cross_v[i], null, c.audio_ctx, d, d);
                dec.self_k[i] = try a.alloc(f32, c.text_ctx * d);
                dec.self_v[i] = try a.alloc(f32, c.text_ctx * d);
            }
            dec.arena = arena;
            return dec;
        }

        pub fn deinit(self: *Decoder) void {
            self.arena.deinit();
        }

        /// Run `tokens` at positions [len, len + tokens.len); leaves the last
        /// token's logits in `self.logits`.
        pub fn step(self: *Decoder, tokens: []const u32) !void {
            const m = self.model;
            const c = m.cfg;
            const d = c.d_model;
            const n = tokens.len;
            if (self.len + n > c.text_ctx) return error.ContextFull;
            var scratch = std.heap.ArenaAllocator.init(m.allocator);
            defer scratch.deinit();
            const a = scratch.allocator();
            const x = try a.alloc(f32, n * d);
            const xn = try a.alloc(f32, n * d);
            const q = try a.alloc(f32, n * d);
            const ctx = try a.alloc(f32, n * d);
            const ff = try a.alloc(f32, n * c.decoder_ffn);
            layout.embed(kernels.embed, .{ .ids = tokens, .table = m.tok_emb, .pos = m.dec_pos, .out = x, .dim = d, .pos_offset = self.len });
            const total = self.len + n;
            for (m.dec_layers, 0..) |l, i| {
                // Self-attention; new keys/values go straight into the cache.
                layerNorm(x, l.ln1, xn, n, d);
                linear(kernels.linear_query, xn, l.self_attn.q, q, null, n, d, d);
                linear(kernels.linear, xn, l.self_attn.k, self.self_k[i][self.len * d ..][0 .. n * d], null, n, d, d);
                linear(kernels.linear_bias, xn, l.self_attn.v, self.self_v[i][self.len * d ..][0 .. n * d], null, n, d, d);
                attend(.{ .q = q, .k = self.self_k[i], .v = self.self_v[i], .out = ctx, .tq = n, .tk = total, .heads = c.decoder_heads, .ld_q = d, .ld_k = d, .ld_v = d, .ld_o = d, .scale = 1, .causal = true, .q_offset = self.len });
                linear(kernels.linear_residual, ctx, l.self_attn.o, x, x, n, d, d);
                // Cross-attention over the encoder output.
                layerNorm(x, l.ln2, xn, n, d);
                linear(kernels.linear_query, xn, l.cross.q, q, null, n, d, d);
                attend(.{ .q = q, .k = self.cross_k[i], .v = self.cross_v[i], .out = ctx, .tq = n, .tk = c.audio_ctx, .heads = c.decoder_heads, .ld_q = d, .ld_k = d, .ld_v = d, .ld_o = d, .scale = 1 });
                linear(kernels.linear_residual, ctx, l.cross.o, x, x, n, d, d);
                // Feed-forward.
                layerNorm(x, l.ln3, xn, n, d);
                linear(kernels.linear_gelu, xn, l.fc1, ff, null, n, c.decoder_ffn, d);
                linear(kernels.linear_residual, ff, l.fc2, x, x, n, d, c.decoder_ffn);
            }
            self.len = total;
            // Logits for the last token against the tied embedding.
            const last = x[(n - 1) * d ..][0..d];
            layerNorm(last, m.dec_ln, xn[0..d], 1, d);
            linear(kernels.linear, xn[0..d], .{ .w = m.tok_emb }, self.logits, null, 1, c.vocab_size, d);
        }

        /// Greedy choice over `self.logits` with suppressed ids, through the
        /// Whisper logits RowReduce (no timestamp tokens).
        pub fn choose(self: *const Decoder, suppress: []const i32) u32 {
            const c = self.model.cfg;
            const vocab: u32 = @intCast(c.vocab_size);
            const grammar: [8]u32 = @splat(0);
            const params = .{
                .out_dim = vocab,
                .suppress_count = @as(u32, @intCast(suppress.len)),
                .ts_begin = vocab,
                .text_allowed = @as(u32, 1),
                .ts_min = vocab,
                .ts_max = vocab,
                .eot = c.eot,
                .probe_id = vocab,
                .mode = @as(u32, 0),
                .token_slot = @as(u32, 0),
                .stats_slot = @as(u32, 0),
                .reserved = @as(u32, 0),
            };
            const args = whisper_logits.args(self.logits, params, suppress, &grammar);
            const stats = cpu.RowReduce(kernels.logits, 16).run(&args);
            return stats[0].id();
        }
    };
};

/// GPT-2 byte-level BPE decoding for Whisper's `vocab.json`.
pub const Detokenizer = struct {
    allocator: std.mem.Allocator,
    tokens: [][]u8,

    pub fn load(allocator: std.mem.Allocator, io: std.Io, model_dir: []const u8) !Detokenizer {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const json = try std.Io.Dir.cwd().readFileAlloc(io, try std.fmt.bufPrint(&path_buf, "{s}/vocab.json", .{model_dir}), allocator, .limited(16 << 20));
        defer allocator.free(json);
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json, .{});
        defer parsed.deinit();
        var max_id: usize = 0;
        var it = parsed.value.object.iterator();
        while (it.next()) |e| max_id = @max(max_id, @as(usize, @intCast(e.value_ptr.integer)));
        const tokens = try allocator.alloc([]u8, max_id + 1);
        @memset(tokens, &.{});
        // bytes_to_unicode: printable bytes map to themselves, the rest to 256+.
        var unicode_to_byte: [512]?u8 = @splat(null);
        var next: u16 = 256;
        for (0..256) |b| {
            const printable = (b >= 33 and b <= 126) or (b >= 161 and b <= 172) or (b >= 174);
            if (printable) unicode_to_byte[b] = @intCast(b) else {
                unicode_to_byte[next] = @intCast(b);
                next += 1;
            }
        }
        it = parsed.value.object.iterator();
        while (it.next()) |e| {
            var out: std.ArrayList(u8) = .empty;
            var view = try std.unicode.Utf8View.init(e.key_ptr.*);
            var cps = view.iterator();
            while (cps.nextCodepoint()) |cp| {
                if (cp < unicode_to_byte.len) if (unicode_to_byte[cp]) |b| {
                    try out.append(allocator, b);
                };
            }
            tokens[@intCast(e.value_ptr.integer)] = try out.toOwnedSlice(allocator);
        }
        return .{ .allocator = allocator, .tokens = tokens };
    }

    pub fn deinit(self: *Detokenizer) void {
        for (self.tokens) |t| self.allocator.free(t);
        self.allocator.free(self.tokens);
    }

    /// Text tokens only (ids below `first_special`).
    pub fn decode(self: *const Detokenizer, ids: []const u32, first_special: u32, out: *std.ArrayList(u8)) !void {
        for (ids) |id| {
            if (id >= first_special or id >= self.tokens.len) continue;
            try out.appendSlice(self.allocator, self.tokens[id]);
        }
    }
};
