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

//! GLiDE decoder backbone (Qwen3.5 text). The six full-attention layers attend
//! bidirectionally over the row's real tokens; the Gated-DeltaNet layers keep
//! their causal recurrence. Pad positions are zeroed before every linear layer
//! in the reference, so each row equals its unpadded forward.
//!
//! Rows are packed without padding into one [N, hidden] activation: every
//! dense projection and FFN runs once over all N real tokens, attention is
//! segment-restricted to each row's own keys, and each DeltaNet recurrence
//! runs over its row's contiguous token range from a zero history (all rows
//! in one packed-core dispatch). The whole stack is one Metal frame, and the
//! final layer computes queries, attention and FFN only for the readout rows.
//! Activations stay F32 end to end (BF16 weights, F32 accumulation).
//!
//! Debug knobs: ANTFLY_GLIDE_DECODER_PROFILE (timings),
//! ANTFLY_GLIDE_DECODER_DUMP=<file> (per-layer readout rows, raw f32),
//! ANTFLY_GLIDE_DECODER_ATTENTION=gemm|auto|flash,
//! ANTFLY_GLIDE_DECODER_FRAME_LAYERS=<n>, ANTFLY_GLIDE_DECODER_SERIAL_CORE,
//! ANTFLY_GLIDE_DECODER_NO_PRUNE.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const gpt_mod = @import("../models/gpt.zig");
const gpt_arch = @import("gpt.zig");
const metal_compute_mod = @import("../ops/metal_compute.zig");

const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const Config = gpt_mod.Config;

/// Final-norm hidden states at `readout_index[b]` for every row: [batch * hidden] f32.
pub fn readoutHidden(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    readout_index: []const i64,
) ![]f32 {
    if (!config.isQwen35() or !config.qwen35_has_linear_attention) return error.UnsupportedGlideBackbone;
    if (batch == 0 or seq_len == 0 or readout_index.len != batch or
        input_ids.len != batch * seq_len or attention_mask.len != batch * seq_len) return error.InvalidInputShape;

    var plan = try Plan.init(allocator, input_ids, attention_mask, batch, seq_len, readout_index);
    defer plan.deinit(allocator);

    var forward = Forward{ .cb = cb, .allocator = allocator, .config = config, .plan = &plan };
    return forward.run();
}

/// Packed-row layout: real tokens of every row concatenated in row order.
const Plan = struct {
    batch: usize,
    tokens: usize,
    lengths: []usize,
    starts: []usize,
    ids: []i64,
    /// Axis-major [3, tokens] text M-RoPE positions (identical axes).
    mrope_positions: []u32,
    /// Segment-attention visibility: each query sees its own row's keys.
    ranges: []u32,
    positions: []i32,
    readout_rows: []u32,
    /// Readout-row query set for the pruned final layer.
    readout_mrope: []u32,
    readout_ranges: []u32,
    readout_positions: []i32,
    /// Packed DeltaNet metadata: each token's row position, then every row's
    /// (start, length).
    core_meta: []u32,

    fn all(self: *const Plan) Queries {
        return .{ .count = self.tokens, .mrope_positions = self.mrope_positions, .ranges = self.ranges, .positions = self.positions };
    }

    fn readout(self: *const Plan) Queries {
        return .{ .count = self.batch, .mrope_positions = self.readout_mrope, .ranges = self.readout_ranges, .positions = self.readout_positions };
    }

    fn init(allocator: std.mem.Allocator, input_ids: []const i64, mask: []const i64, batch: usize, seq_len: usize, readout_index: []const i64) !Plan {
        const lengths = try allocator.alloc(usize, batch);
        errdefer allocator.free(lengths);
        const starts = try allocator.alloc(usize, batch);
        errdefer allocator.free(starts);
        var tokens: usize = 0;
        for (0..batch) |row| {
            var len: usize = 0;
            for (mask[row * seq_len ..][0..seq_len]) |m| {
                if (m != 0) len += 1;
            }
            // Right padding: the row's real tokens are its prefix.
            for (mask[row * seq_len ..][0..len]) |m| if (m == 0) return error.InvalidAttentionMask;
            if (len == 0) return error.InvalidAttentionMask;
            const index = readout_index[row];
            if (index < 0 or @as(usize, @intCast(index)) >= len) return error.InvalidGlideReadoutIndex;
            lengths[row] = len;
            starts[row] = tokens;
            tokens += len;
        }
        if (tokens > std.math.maxInt(u32) / 8) return error.InvalidInputShape;
        const ids = try allocator.alloc(i64, tokens);
        errdefer allocator.free(ids);
        const mrope = try allocator.alloc(u32, tokens * 3);
        errdefer allocator.free(mrope);
        const ranges = try allocator.alloc(u32, tokens * 6);
        errdefer allocator.free(ranges);
        const positions = try allocator.alloc(i32, tokens);
        errdefer allocator.free(positions);
        const readout_rows = try allocator.alloc(u32, batch);
        errdefer allocator.free(readout_rows);
        const readout_mrope = try allocator.alloc(u32, batch * 3);
        errdefer allocator.free(readout_mrope);
        const readout_ranges = try allocator.alloc(u32, batch * 6);
        errdefer allocator.free(readout_ranges);
        const readout_positions = try allocator.alloc(i32, batch);
        errdefer allocator.free(readout_positions);
        const core_meta = try allocator.alloc(u32, tokens + batch * 2);
        errdefer allocator.free(core_meta);
        for (0..batch) |row| {
            const start = starts[row];
            const len = lengths[row];
            @memcpy(ids[start..][0..len], input_ids[row * seq_len ..][0..len]);
            for (0..len) |t| {
                const at = start + t;
                for (0..3) |axis| mrope[axis * tokens + at] = @intCast(t);
                ranges[at * 6 ..][0..6].* = .{ @intCast(start), @intCast(start + len), 0, 0, 0, 0 };
                positions[at] = @intCast(t);
                core_meta[at] = @intCast(t);
            }
            core_meta[tokens + row * 2] = @intCast(start);
            core_meta[tokens + row * 2 + 1] = @intCast(len);
            const index: usize = @intCast(readout_index[row]);
            readout_rows[row] = @intCast(start + index);
            for (0..3) |axis| readout_mrope[axis * batch + row] = @intCast(index);
            readout_ranges[row * 6 ..][0..6].* = .{ @intCast(start), @intCast(start + len), 0, 0, 0, 0 };
            readout_positions[row] = @intCast(index);
        }
        return .{
            .batch = batch,
            .tokens = tokens,
            .lengths = lengths,
            .starts = starts,
            .ids = ids,
            .mrope_positions = mrope,
            .ranges = ranges,
            .positions = positions,
            .readout_rows = readout_rows,
            .readout_mrope = readout_mrope,
            .readout_ranges = readout_ranges,
            .readout_positions = readout_positions,
            .core_meta = core_meta,
        };
    }

    fn deinit(self: *Plan, allocator: std.mem.Allocator) void {
        allocator.free(self.lengths);
        allocator.free(self.starts);
        allocator.free(self.ids);
        allocator.free(self.mrope_positions);
        allocator.free(self.ranges);
        allocator.free(self.positions);
        allocator.free(self.readout_rows);
        allocator.free(self.readout_mrope);
        allocator.free(self.readout_ranges);
        allocator.free(self.readout_positions);
        allocator.free(self.core_meta);
    }
};

/// Attention query rows: every packed token, or only the readout rows.
/// Keys always span every packed token of the query's own row.
const Queries = struct {
    count: usize,
    /// Axis-major [3, count] M-RoPE positions.
    mrope_positions: []const u32,
    /// [count * 6] segment ranges into the packed keys.
    ranges: []const u32,
    positions: []const i32,
};

const Profile = struct {
    enabled: bool,
    linear_ns: u64 = 0,
    full_ns: u64 = 0,
    embed_ns: u64 = 0,
    tail_ns: u64 = 0,
    wait_ns: u64 = 0,
    started: u64 = 0,

    fn now() u64 {
        if (comptime @import("builtin").os.tag == .freestanding) return 0;
        var ts: std.posix.timespec = undefined;
        switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
            .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
            else => return 0,
        }
    }
};

/// Dense GEMM-softmax-GEMM attention is several times faster than the flash
/// kernel on long bidirectional rows (5k tokens: 290 vs 650 ms per layer on
/// an M4), and matches it on short rows. Overridable for qualification.
fn attentionStrategy() @FieldType(ops.SegmentAttentionGrouped, "metal_strategy") {
    const raw = platform.env.getenv("ANTFLY_GLIDE_DECODER_ATTENTION") orelse return .gemm;
    if (std.mem.eql(u8, raw, "auto")) return .automatic;
    if (std.mem.eql(u8, raw, "flash")) return .flash;
    return .gemm;
}

/// Mirrors the Metal grouped-attention planner: one query tile of scores
/// over the widest row, packed K/V copies, and the query/output tiles.
pub const gemm_attention_workspace_cap: usize = 256 * 1024 * 1024;

/// Packed token count above which a controlled forward submits its Metal frame
/// every `long_forward_layers_per_frame` layers (an 8k-token 0.8B forward
/// takes several seconds on an M4).
const long_forward_frame_tokens: usize = 8192;
const long_forward_layers_per_frame: usize = 4;

pub fn gemmAttentionWorkspace(tile: usize, span: usize, keys: usize, heads: usize, kv_heads: usize, head_dim: usize) usize {
    return (tile * span * heads + keys * kv_heads * head_dim * 2 + tile * heads * head_dim * 2) * @sizeOf(f32);
}

const Forward = struct {
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    plan: *const Plan,
    name_buf: [256]u8 = undefined,

    fn weight(self: *Forward, layer: usize, comptime suffix: []const u8) !CT {
        const name = std.fmt.bufPrint(&self.name_buf, "model.layers.{d}." ++ suffix, .{layer}) catch return error.NameTooLong;
        return gpt_arch.getModelWeight(self.cb, self.config, name);
    }

    /// Qwen3.5 decoder RMSNorm scales by (1 + weight).
    fn normPlusOne(self: *Forward, input: CT, base: CT, dim: usize) !CT {
        const adjusted = try gpt_arch.maybeAdjustNormWeight(self.cb, self.allocator, self.config, base, dim);
        defer if (adjusted != base) self.cb.free(adjusted);
        return self.cb.rmsNorm(input, adjusted, dim, self.config.norm_eps);
    }

    fn layerNorm(self: *Forward, input: CT, layer: usize, comptime suffix: []const u8) !CT {
        const w = try self.weight(layer, suffix);
        defer self.cb.free(w);
        return self.normPlusOne(input, w, self.config.hidden_size);
    }

    /// The tuned F32 projection kernels qualify at most this many rows;
    /// longer packed inputs run as row chunks written into one output.
    const max_projection_rows = 16384;

    fn linear(self: *Forward, input: CT, rows: usize, layer: usize, comptime suffix: []const u8, in_dim: usize, out_dim: usize) !CT {
        const cb = self.cb;
        const w = try self.weight(layer, suffix);
        defer cb.free(w);
        if (rows <= max_projection_rows) return cb.linearNoBias(input, w, rows, in_dim, out_dim);
        const output = (try cb.allocUninitF32Shape(&.{ @intCast(rows), @intCast(out_dim) })) orelse
            return cb.linearNoBias(input, w, rows, in_dim, out_dim);
        errdefer cb.free(output);
        var start: usize = 0;
        while (start < rows) : (start += max_projection_rows) {
            const len = @min(max_projection_rows, rows - start);
            const in_view = try cb.sliceRows2D(self.allocator, input, start, len, in_dim);
            defer cb.free(in_view);
            const out_view = try cb.sliceRows2D(self.allocator, output, start, len, out_dim);
            defer cb.free(out_view);
            try cb.linearNoBiasInto(self.allocator, in_view, w, out_view, len, in_dim, out_dim);
        }
        return output;
    }

    fn gatherReadout(self: *Forward, input: CT, width: usize) !CT {
        const cb = self.cb;
        const rows = self.plan.batch;
        if (try cb.takeRows(input, self.plan.readout_rows, rows, width)) |gathered| return gathered;
        const ids = try self.allocator.alloc(i64, rows);
        defer self.allocator.free(ids);
        for (ids, self.plan.readout_rows) |*id, row| id.* = row;
        return cb.embeddingLookup(input, ids, rows, width);
    }

    fn run(self: *Forward) ![]f32 {
        const cb = self.cb;
        const n = self.plan.tokens;
        const hidden_size: usize = self.config.hidden_size;
        var profile = Profile{ .enabled = platform.env.getenvBool("ANTFLY_GLIDE_DECODER_PROFILE") };
        const run_started = Profile.now();

        var hidden = blk: {
            const embed_w = try gpt_arch.getEmbeddingWeight(cb, self.config);
            defer cb.free(embed_w);
            break :blk try cb.embeddingLookup(embed_w, self.plan.ids, n, hidden_size);
        };
        defer cb.free(hidden);
        if (profile.enabled) profile.embed_ns = Profile.now() - run_started;

        var linear_states: [64]metal_compute_mod.Qwen35LinearState = @splat(.{});
        defer for (&linear_states) |*state| state.deinit();
        if (self.config.num_hidden_layers > linear_states.len) return error.UnsupportedGlideBackbone;

        // One command-buffer frame for the whole stack: per-layer submit/wait
        // lets the GPU drop clocks between layers (2-4x slower, erratic).
        // Long packed batches (seconds per forward) under an execution control
        // submit every few layers instead, so cancellation and deadlines are
        // observed mid-forward; the extra submits are noise at that size.
        const default_layers_per_frame: usize = if (cb.execution_control != null and n >= long_forward_frame_tokens)
            long_forward_layers_per_frame
        else
            self.config.num_hidden_layers;
        const layers_per_frame = platform.env.getenvUsize("ANTFLY_GLIDE_DECODER_FRAME_LAYERS") orelse default_layers_per_frame;
        var frame = false;
        defer if (frame) cb.decoderRuntimeCancelFrame() catch {};
        // Debug: append the first row's readout hidden state after every layer
        // (raw f32) for layer-by-layer comparison against the reference.
        const dump_file: ?*std.c.FILE = if (platform.env.getenvZ("ANTFLY_GLIDE_DECODER_DUMP")) |path| std.c.fopen(path, "ab") else null;
        defer if (dump_file) |file| {
            _ = std.c.fclose(file);
        };
        var hidden_rows = n;
        if (dump_file) |file| try self.dumpRow(file, hidden, hidden_rows);

        for (0..self.config.num_hidden_layers) |layer| {
            const layer_started = if (profile.enabled) Profile.now() else 0;
            if (cb.kind() == .metal and !frame and layers_per_frame != 0) {
                frame = try cb.decoderRuntimeBeginFrame();
                if (frame) try cb.decoderRuntimeSetActiveFrameRegime(.prefill);
            }
            const last = layer + 1 == self.config.num_hidden_layers;
            // The readout needs only the final layer's readout rows: its
            // keys/values (or recurrence) still span every token.
            const prune = last and !platform.env.getenvBool("ANTFLY_GLIDE_DECODER_NO_PRUNE");
            const next = try self.decoderLayer(hidden, layer, &linear_states[layer], prune);
            cb.free(hidden);
            hidden = next;
            hidden_rows = if (prune) self.plan.batch else n;
            if (frame and (last or (layer + 1) % layers_per_frame == 0)) {
                const wait_started = if (profile.enabled) Profile.now() else 0;
                try cb.decoderRuntimeSubmitAndWaitFrame();
                frame = false;
                if (profile.enabled) profile.wait_ns += Profile.now() - wait_started;
                if (!last) try cb.checkExecutionControl();
            }
            if (dump_file) |file| try self.dumpRow(file, hidden, hidden_rows);
            if (profile.enabled) {
                if (!frame) {
                    const elapsed = Profile.now() - layer_started;
                    if (self.config.layerUsesQwen35LinearAttention(layer)) profile.linear_ns += elapsed else profile.full_ns += elapsed;
                }
            }
        }

        const tail_started = Profile.now();
        const rows = self.plan.batch;
        const gathered = if (hidden_rows == rows and rows != n) hidden else try self.gatherReadout(hidden, hidden_size);
        defer if (gathered != hidden) cb.free(gathered);
        const final = blk: {
            const w = try gpt_arch.getModelWeight(cb, self.config, "model.norm.weight");
            defer cb.free(w);
            break :blk try self.normPlusOne(gathered, w, hidden_size);
        };
        defer cb.free(final);
        const out = try cb.toFloat32(final, self.allocator);
        if (dump_file) |file| {
            const bytes = std.mem.sliceAsBytes(out[0..hidden_size]);
            _ = std.c.fwrite(bytes.ptr, 1, bytes.len, file);
        }
        if (profile.enabled) {
            profile.tail_ns = Profile.now() - tail_started;
            std.debug.print("glide_decoder_profile: rows={d} tokens={d} embed_ms={d:.2} linear_ms={d:.2} full_ms={d:.2} wait_ms={d:.2} tail_ms={d:.2} total_ms={d:.2}\n", .{
                rows,
                n,
                @as(f64, @floatFromInt(profile.embed_ns)) / 1e6,
                @as(f64, @floatFromInt(profile.linear_ns)) / 1e6,
                @as(f64, @floatFromInt(profile.full_ns)) / 1e6,
                @as(f64, @floatFromInt(profile.wait_ns)) / 1e6,
                @as(f64, @floatFromInt(profile.tail_ns)) / 1e6,
                @as(f64, @floatFromInt(Profile.now() - run_started)) / 1e6,
            });
        }
        return out;
    }

    fn dumpRow(self: *Forward, file: *std.c.FILE, hidden: CT, hidden_rows: usize) !void {
        const cb = self.cb;
        const hidden_size: usize = self.config.hidden_size;
        if (cb.decoderRuntimeHasActiveFrame()) try cb.decoderRuntimeFlushActiveFrame();
        const first = [_]u32{0};
        const index = if (hidden_rows == self.plan.tokens) self.plan.readout_rows[0..1] else first[0..];
        const row = (try cb.takeRows(hidden, index, 1, hidden_size)) orelse return error.UnsupportedOperation;
        defer cb.free(row);
        const values = try cb.toFloat32(row, self.allocator);
        defer self.allocator.free(values);
        const bytes = std.mem.sliceAsBytes(values);
        _ = std.c.fwrite(bytes.ptr, 1, bytes.len, file);
    }

    /// One decoder layer over all packed tokens. With `prune`, only the
    /// readout rows leave the token mixer: the result is [batch, hidden].
    fn decoderLayer(self: *Forward, hidden: CT, layer: usize, state: *metal_compute_mod.Qwen35LinearState, prune: bool) !CT {
        const cb = self.cb;
        const hidden_size: usize = self.config.hidden_size;
        const intermediate: usize = self.config.intermediate_size;
        const rows = if (prune) self.plan.batch else self.plan.tokens;

        const normed = try self.layerNorm(hidden, layer, "input_layernorm.weight");
        const mixed = blk: {
            defer cb.free(normed);
            if (!self.config.layerUsesQwen35LinearAttention(layer))
                break :blk try self.fullAttention(normed, layer, if (prune) self.plan.readout() else self.plan.all());
            const all_rows = try self.linearAttention(normed, layer, state);
            if (!prune) break :blk all_rows;
            defer cb.free(all_rows);
            break :blk try self.gatherReadout(all_rows, hidden_size);
        };
        const residual = if (prune) try self.gatherReadout(hidden, hidden_size) else hidden;
        defer if (residual != hidden) cb.free(residual);
        const attn_res = blk: {
            defer cb.free(mixed);
            break :blk try cb.add(mixed, residual);
        };
        defer cb.free(attn_res);

        const ffn_in = try self.layerNorm(attn_res, layer, "post_attention_layernorm.weight");
        const activated = blk: {
            defer cb.free(ffn_in);
            const gate = try self.linear(ffn_in, rows, layer, "mlp.gate_proj.weight", hidden_size, intermediate);
            defer cb.free(gate);
            const up = try self.linear(ffn_in, rows, layer, "mlp.up_proj.weight", hidden_size, intermediate);
            defer cb.free(up);
            if (try cb.siluMultiply(gate, up)) |fused| break :blk fused;
            const act = try cb.silu(gate);
            defer cb.free(act);
            break :blk try cb.multiply(act, up);
        };
        const down = blk: {
            defer cb.free(activated);
            break :blk try self.linear(activated, rows, layer, "mlp.down_proj.weight", intermediate, hidden_size);
        };
        defer cb.free(down);
        return cb.add(down, attn_res);
    }

    /// Bidirectional full attention over each row's tokens. `queries` selects
    /// every packed token or only the readout rows (keys always span all).
    fn fullAttention(self: *Forward, normed: CT, layer: usize, queries: Queries) !CT {
        const cb = self.cb;
        const n = self.plan.tokens;
        const m = queries.count;
        const hidden_size: usize = self.config.hidden_size;
        const heads: usize = self.config.num_attention_heads;
        const kv_heads: usize = self.config.effectiveKVHeads();
        const head_dim: usize = self.config.headDim();
        const q_dim = heads * head_dim;
        const kv_dim = kv_heads * head_dim;
        const sections = self.config.mrope_section;
        const rotary_dim = 2 * (@as(usize, sections[0]) + sections[1] + sections[2]);
        const expected_rotary: usize = @intFromFloat(@as(f32, @floatFromInt(head_dim)) * self.config.rope_partial_factor);
        if (rotary_dim == 0 or rotary_dim != expected_rotary) return error.UnsupportedGlideRope;

        const query_input = if (m == n) normed else try self.gatherReadout(normed, hidden_size);
        defer if (query_input != normed) cb.free(query_input);
        const projected = try self.linear(query_input, m, layer, "self_attn.q_proj.weight", hidden_size, q_dim * 2);
        const split = blk: {
            defer cb.free(projected);
            break :blk try cb.splitPerHeadGate(projected, m, heads, head_dim);
        };
        defer cb.free(split.gate);
        const q = blk: {
            defer cb.free(split.query);
            break :blk try self.headNormRope(split.query, layer, "self_attn.q_norm.weight", heads, head_dim, m, queries.mrope_positions);
        };
        defer cb.free(q);
        const k = blk: {
            const raw = try self.linear(normed, n, layer, "self_attn.k_proj.weight", hidden_size, kv_dim);
            defer cb.free(raw);
            break :blk try self.headNormRope(raw, layer, "self_attn.k_norm.weight", kv_heads, head_dim, n, self.plan.mrope_positions);
        };
        defer cb.free(k);
        const v = try self.linear(normed, n, layer, "self_attn.v_proj.weight", hidden_size, kv_dim);
        defer cb.free(v);

        const attended = try self.segmentAttention(q, k, v, heads, kv_heads, head_dim, queries);
        const gated = blk: {
            defer cb.free(attended);
            const sig = try cb.sigmoid(split.gate);
            defer cb.free(sig);
            break :blk try cb.multiply(attended, sig);
        };
        defer cb.free(gated);
        return self.linear(gated, m, layer, "self_attn.o_proj.weight", q_dim, hidden_size);
    }

    /// Per-head (1 + w) RMSNorm, then partial M-RoPE over the rotary prefix
    /// (text positions restart at zero in every packed row).
    fn headNormRope(self: *Forward, input: CT, layer: usize, comptime norm_suffix: []const u8, heads: usize, head_dim: usize, n: usize, mrope_positions: []const u32) !CT {
        const cb = self.cb;
        const per_head = (try cb.reshape2d(input, n * heads, head_dim)) orelse return error.UnsupportedOperation;
        const normed = blk: {
            defer cb.free(per_head);
            const w = try self.weight(layer, norm_suffix);
            defer cb.free(w);
            break :blk try self.normPlusOne(per_head, w, head_dim);
        };
        const flat = blk: {
            defer cb.free(normed);
            break :blk (try cb.reshape2d(normed, n, heads * head_dim)) orelse return error.UnsupportedOperation;
        };
        defer cb.free(flat);
        return (try cb.partialMrope(flat, n, heads, head_dim, self.config.rope_theta, 1.0, mrope_positions, self.config.mrope_section)) orelse
            error.UnsupportedGlideRope;
    }

    fn segmentAttention(self: *Forward, q: CT, k: CT, v: CT, heads: usize, kv_heads: usize, head_dim: usize, queries: Queries) !CT {
        const cb = self.cb;
        const n = self.plan.tokens;
        const visibility = ops.SegmentAttention{
            .ranges = queries.ranges,
            .query_positions = queries.positions,
            .key_positions = self.plan.positions,
            .window = std.math.maxInt(u32),
            .queries = queries.count,
            .keys = n,
            .num_heads = heads,
            .head_dim = head_dim,
        };
        var grouped = ops.SegmentAttentionGrouped{
            .visibility = visibility,
            .num_kv_heads = kv_heads,
            .score_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))),
            .metal_strategy = attentionStrategy(),
            .control = cb.execution_control,
        };
        // Keep the dense route inside its 256 MiB workspace: a 128-query tile
        // halves the score block (rows up to ~32k tokens), and splitting by
        // KV head halves it again (~64k). Beyond that the flash route, which
        // needs no N^2 workspace, covers any key span.
        var span: usize = 0;
        for (self.plan.lengths) |len| span = @max(span, len);
        const default_tile: usize = if (queries.count <= 512) 512 else 256;
        const narrow_tile = @min(queries.count, 128);
        if (gemmAttentionWorkspace(@min(queries.count, default_tile), span, n, heads, kv_heads, head_dim) > gemm_attention_workspace_cap)
            grouped.metal_query_tile = 128;
        const op = cb.vtable.segmentAttentionGrouped orelse return error.GlideAttentionUnavailable;
        if (grouped.metal_strategy == .gemm and kv_heads > 1 and
            gemmAttentionWorkspace(narrow_tile, span, n, heads, kv_heads, head_dim) > gemm_attention_workspace_cap and
            gemmAttentionWorkspace(narrow_tile, span, n, heads / kv_heads, 1, head_dim) <= gemm_attention_workspace_cap)
        {
            if (try self.attentionPerKvHead(op, q, k, v, grouped, heads, kv_heads, head_dim)) |result| return result;
        }
        if (try op(cb.ptr, q, k, v, &grouped)) |result| return result;
        // The GEMM route declines when a row's score tile exceeds its
        // workspace budget; the flash route covers any key span.
        if (grouped.metal_strategy == .gemm) {
            grouped.metal_strategy = .automatic;
            grouped.metal_query_tile = null;
            if (try op(cb.ptr, q, k, v, &grouped)) |result| return result;
        }
        return error.GlideAttentionUnavailable;
    }

    /// Dense attention one KV head at a time: each call scores only that
    /// head's query group, so its workspace is 1/kv_heads of the full call.
    fn attentionPerKvHead(
        self: *Forward,
        op: @typeInfo(@FieldType(ComputeBackend.VTable, "segmentAttentionGrouped")).optional.child,
        q: CT,
        k: CT,
        v: CT,
        base: ops.SegmentAttentionGrouped,
        heads: usize,
        kv_heads: usize,
        head_dim: usize,
    ) !?CT {
        const cb = self.cb;
        const rows = base.visibility.queries;
        const group = heads / kv_heads;
        const group_width = group * head_dim;
        var joined: ?CT = null;
        errdefer if (joined) |tensor| cb.free(tensor);
        for (0..kv_heads) |g| {
            const q_g = try cb.sliceLastDim(q, g * group_width, (g + 1) * group_width);
            defer cb.free(q_g);
            const k_g = try cb.sliceLastDim(k, g * head_dim, (g + 1) * head_dim);
            defer cb.free(k_g);
            const v_g = try cb.sliceLastDim(v, g * head_dim, (g + 1) * head_dim);
            defer cb.free(v_g);
            var request = base;
            request.visibility.num_heads = group;
            request.num_kv_heads = 1;
            request.metal_query_tile = 128;
            const part = (try op(cb.ptr, q_g, k_g, v_g, &request)) orelse {
                if (joined) |tensor| cb.free(tensor);
                joined = null;
                return null;
            };
            if (joined) |previous| {
                defer cb.free(part);
                const wider = try cb.concat(previous, part, rows, g * group_width, group_width);
                cb.free(previous);
                joined = wider;
            } else joined = part;
        }
        return joined;
    }

    fn linearAttention(self: *Forward, normed: CT, layer: usize, state: *metal_compute_mod.Qwen35LinearState) !CT {
        const cb = self.cb;
        const hidden_size: usize = self.config.hidden_size;
        const key_heads: usize = self.config.qwen35_linear_num_key_heads;
        const value_heads: usize = self.config.qwen35_linear_num_value_heads;
        const key_head_dim: usize = self.config.qwen35_linear_key_head_dim;
        const value_head_dim: usize = self.config.qwen35_linear_value_head_dim;
        const conv_kernel: usize = self.config.qwen35_linear_conv_kernel_dim;
        const key_dim = key_heads * key_head_dim;
        const value_dim = value_heads * value_head_dim;
        const conv_dim = key_dim * 2 + value_dim;

        if (comptime build_options.enable_metal) {
            if (cb.kind() == .metal) return self.linearAttentionMetal(normed, layer, state, .{
                .hidden_size = hidden_size,
                .key_heads = key_heads,
                .value_heads = value_heads,
                .key_head_dim = key_head_dim,
                .value_head_dim = value_head_dim,
                .conv_kernel = conv_kernel,
                .value_dim = value_dim,
                .conv_dim = conv_dim,
            });
        }
        return error.GlideDecoderRequiresMetal;
    }

    const LinearDims = struct {
        hidden_size: usize,
        key_heads: usize,
        value_heads: usize,
        key_head_dim: usize,
        value_head_dim: usize,
        conv_kernel: usize,
        value_dim: usize,
        conv_dim: usize,
    };

    fn linearAttentionMetal(self: *Forward, normed: CT, layer: usize, state: *metal_compute_mod.Qwen35LinearState, dims: LinearDims) !CT {
        const cb = self.cb;
        const n = self.plan.tokens;
        const hidden_size = dims.hidden_size;
        const key_heads = dims.key_heads;
        const value_heads = dims.value_heads;
        const key_head_dim = dims.key_head_dim;
        const value_head_dim = dims.value_head_dim;
        const conv_kernel = dims.conv_kernel;
        const value_dim = dims.value_dim;
        const conv_dim = dims.conv_dim;

        const mixed = try self.linear(normed, n, layer, "linear_attn.in_proj_qkv.weight", hidden_size, conv_dim);
        defer cb.free(mixed);
        const z = try self.linear(normed, n, layer, "linear_attn.in_proj_z.weight", hidden_size, value_dim);
        defer cb.free(z);
        const beta = try self.linear(normed, n, layer, "linear_attn.in_proj_b.weight", hidden_size, value_heads);
        defer cb.free(beta);
        const a = try self.linear(normed, n, layer, "linear_attn.in_proj_a.weight", hidden_size, value_heads);
        defer cb.free(a);

        const conv_w = try self.weight(layer, "linear_attn.conv1d.weight");
        defer cb.free(conv_w);
        const a_log = try self.weight(layer, "linear_attn.A_log");
        defer cb.free(a_log);
        const dt_bias = try self.weight(layer, "linear_attn.dt_bias");
        defer cb.free(dt_bias);
        const norm_w = try self.weight(layer, "linear_attn.norm.weight");
        defer cb.free(norm_w);

        // All rows in one dispatch per stage; the per-row path remains for
        // devices without the packed kernels.
        if (!platform.env.getenvBool("ANTFLY_GLIDE_DECODER_SERIAL_CORE")) {
            if (try metal_compute_mod.MetalCompute.glidePackedCore(cb, .{
                .mixed = mixed,
                .z = z,
                .beta = beta,
                .a = a,
                .conv_weight = conv_w,
                .a_log = a_log,
                .dt_bias = dt_bias,
                .norm_weight = norm_w,
                .meta = @as([]const u32, self.plan.core_meta),
                .tokens = n,
                .rows = self.plan.batch,
                .key_heads = key_heads,
                .value_heads = value_heads,
                .dim = key_head_dim,
                .value_dim = value_head_dim,
                .conv_kernel = conv_kernel,
                .eps = self.config.norm_eps,
            })) |packed_core| {
                defer cb.free(packed_core);
                return self.linear(packed_core, n, layer, "linear_attn.out_proj.weight", value_dim, hidden_size);
            }
        }
        const core = (try cb.allocUninitF32Shape(&.{ @intCast(n), @intCast(value_dim) })) orelse return error.GlideDecoderRequiresMetal;
        defer cb.free(core);
        for (self.plan.starts, self.plan.lengths) |start, len| {
            const mixed_row = try cb.sliceRows2D(self.allocator, mixed, start, len, conv_dim);
            defer cb.free(mixed_row);
            const z_row = try cb.sliceRows2D(self.allocator, z, start, len, value_dim);
            defer cb.free(z_row);
            const beta_row = try cb.sliceRows2D(self.allocator, beta, start, len, value_heads);
            defer cb.free(beta_row);
            const a_row = try cb.sliceRows2D(self.allocator, a, start, len, value_heads);
            defer cb.free(a_row);
            const destination = try cb.sliceRows2D(self.allocator, core, start, len, value_dim);
            defer cb.free(destination);
            // Every row starts from a zero history.
            state.reset();
            const out = (try metal_compute_mod.MetalCompute.qwen35LinearCore(cb, state, .{
                .destination = @as(?CT, destination),
                .mixed = mixed_row,
                .z = z_row,
                .beta = beta_row,
                .a = a_row,
                .conv_weight = conv_w,
                .a_log = a_log,
                .dt_bias = dt_bias,
                .norm_weight = norm_w,
                .seq_len = len,
                .key_heads = key_heads,
                .value_heads = value_heads,
                .dim = key_head_dim,
                .value_dim = value_head_dim,
                .conv_kernel = conv_kernel,
                .eps = self.config.norm_eps,
            })) orelse return error.GlideDeltaNetCoreUnavailable;
            cb.free(out);
        }
        return self.linear(core, n, layer, "linear_attn.out_proj.weight", value_dim, hidden_size);
    }
};

test "GLiDE decoder plan packs right-padded rows without padding" {
    const allocator = std.testing.allocator;
    const ids = [_]i64{ 5, 6, 7, 0, 8, 9, 0, 0 };
    const mask = [_]i64{ 1, 1, 1, 0, 1, 1, 0, 0 };
    const readout = [_]i64{ 2, 1 };
    var plan = try Plan.init(allocator, &ids, &mask, 2, 4, &readout);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 5), plan.tokens);
    try std.testing.expectEqualSlices(i64, &.{ 5, 6, 7, 8, 9 }, plan.ids);
    try std.testing.expectEqualSlices(u32, &.{ 2, 4 }, plan.readout_rows);
    // Positions restart at zero in every row, on all three M-RoPE axes.
    try std.testing.expectEqualSlices(i32, &.{ 0, 1, 2, 0, 1 }, plan.positions);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2, 0, 1, 0, 1, 2, 0, 1, 0, 1, 2, 0, 1 }, plan.mrope_positions);
    try std.testing.expectEqualSlices(u32, &.{ 2, 1, 2, 1, 2, 1 }, plan.readout_mrope);
    // Each query sees exactly its own row's keys (bidirectional).
    try std.testing.expectEqualSlices(u32, &.{ 0, 3, 0, 0, 0, 0 }, plan.ranges[0..6]);
    try std.testing.expectEqualSlices(u32, &.{ 3, 5, 0, 0, 0, 0 }, plan.ranges[4 * 6 ..][0..6]);
    try std.testing.expectEqualSlices(u32, &.{ 3, 5, 0, 0, 0, 0 }, plan.readout_ranges[6..12]);
    // DeltaNet metadata: token positions, then (start, length) per row.
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2, 0, 1, 0, 3, 3, 2 }, plan.core_meta);
}

test "GLiDE decoder plan rejects left padding and out-of-row readouts" {
    const allocator = std.testing.allocator;
    const ids = [_]i64{ 0, 6, 7, 8 };
    try std.testing.expectError(error.InvalidAttentionMask, Plan.init(allocator, &ids, &.{ 0, 1, 1, 1 }, 1, 4, &.{3}));
    try std.testing.expectError(error.InvalidGlideReadoutIndex, Plan.init(allocator, &ids, &.{ 1, 1, 0, 0 }, 1, 4, &.{2}));
}

test "GLiDE dense attention workspace stays within the Metal cap for long rows" {
    // 8 query heads, 2 KV heads, head_dim 256 (GLiDE-0.8B).
    try std.testing.expect(gemmAttentionWorkspace(256, 20000, 20000, 8, 2, 256) <= gemm_attention_workspace_cap);
    try std.testing.expect(gemmAttentionWorkspace(256, 30000, 30000, 8, 2, 256) > gemm_attention_workspace_cap);
    try std.testing.expect(gemmAttentionWorkspace(128, 30000, 30000, 8, 2, 256) <= gemm_attention_workspace_cap);
    try std.testing.expect(gemmAttentionWorkspace(128, 40000, 40000, 8, 2, 256) > gemm_attention_workspace_cap);
}
