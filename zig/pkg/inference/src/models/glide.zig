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

//! Fastino GLiDE decision checkpoints.
//!
//! A GLiDE directory is an ordinary backbone checkpoint (ModernBERT encoder for
//! `backbone: "encoder"`, Qwen3.5 text model otherwise) plus two sidecars:
//! `decision_config.json` (label codes, pooling, prompt format) and
//! `readout.safetensors` (`weight`: [255, hidden], no bias). One forward pass
//! per question prompt produces the backbone's final hidden state at a single
//! readout row; the readout maps it to 255 option logits, and a softmax over
//! the first `n` logits (scaled by `temperature`) gives the option
//! distribution. Nothing is generated.
//!
//! Session contract (`ArchSession.glide_head != null`): inputs `input_ids` and
//! `attention_mask` ([B, T] i64, RIGHT padded) plus `readout_index` ([B] i64,
//! the row-local token index to read); output `logits` ([B, 255] f32). Right
//! padding with explicit positions 0..len-1 is exactly the reference's
//! left-padded `position_ids = cumsum(mask) - 1` layout.

const std = @import("std");
const safetensors = @import("safetensors.zig");
const tensor_mod = @import("../backends/tensor.zig");
const c_file = @import("../util/c_file.zig");

/// Opt-in per-phase wall-clock trace on stderr (`ANTFLY_GLIDE_TRACE=1`).
/// Process-global and unsynchronized: a diagnostic for one request at a time.
pub const trace = struct {
    threadlocal var enabled: bool = false;
    threadlocal var start: u64 = 0;
    threadlocal var last: u64 = 0;

    pub fn begin() void {
        enabled = @import("antfly_platform").env.getenvBoolDefault("ANTFLY_GLIDE_TRACE", false);
        if (!enabled) return;
        start = @import("antfly_platform").time.monotonicNs();
        last = start;
    }

    pub fn mark(label: []const u8) void {
        if (!enabled) return;
        const now = @import("antfly_platform").time.monotonicNs();
        std.debug.print("glide-trace {s}: {d:.3} ms (at {d:.3})\n", .{ label, ms(now - last), ms(now - start) });
        last = now;
    }

    fn ms(ns: u64) f64 {
        return @as(f64, @floatFromInt(ns)) / std.time.ns_per_ms;
    }
};

pub const decision_config_file = "decision_config.json";
pub const readout_file = "readout.safetensors";
pub const max_options: usize = 255;
pub const supported_prompt_format = "quick-empty-think-medium-v1";

pub const Backbone = enum { encoder, decoder };
pub const Pooling = enum { last, cls };

pub const DecisionConfig = struct {
    backbone: Backbone,
    pooling: Pooling,
    /// Maximum tokens per question prompt (special tokens included).
    max_len: usize,
    /// Encoder truncation keeps this many head tokens plus the tail.
    trunc_head: usize,
    temperature: f64,
    /// Owned. Empty when the checkpoint does not name itself.
    model_name: []const u8,
    /// Owned option codes ("A".."Z", "AA", ...), exactly `max_options`.
    codes: []const []const u8,
    /// Owned; encoder checkpoints render prompt text with this tokenizer dir.
    render_tokenizer: []const u8,

    pub fn deinit(self: *DecisionConfig, allocator: std.mem.Allocator) void {
        for (self.codes) |code| allocator.free(code);
        allocator.free(self.codes);
        allocator.free(self.model_name);
        allocator.free(self.render_tokenizer);
        self.* = undefined;
    }

    /// Row-local readout index for a prompt of `len` tokens (special tokens
    /// included). Encoder prompts end in [SEP]; the readout reads the final
    /// "(" before it. Decoder prompts end in the "(" itself.
    pub fn readoutIndex(self: DecisionConfig, len: usize) !usize {
        return switch (self.pooling) {
            .cls => 0,
            .last => switch (self.backbone) {
                .encoder => if (len >= 2) len - 2 else error.InvalidGlidePromptLength,
                .decoder => if (len >= 1) len - 1 else error.InvalidGlidePromptLength,
            },
        };
    }
};

pub const default_encoder_max_len: usize = 7999;
pub const default_decoder_max_len: usize = 40000;
pub const default_trunc_head: usize = 1024;

fn jsonUsize(value: std.json.Value) ?usize {
    return switch (value) {
        .integer => |v| if (v >= 0) @intCast(v) else null,
        else => null,
    };
}

fn jsonFloat(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |v| @floatFromInt(v),
        .float => |v| v,
        else => null,
    };
}

pub fn parseDecisionConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !DecisionConfig {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{}) catch return error.InvalidGlideDecisionConfig;
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |o| o,
        else => return error.InvalidGlideDecisionConfig,
    };
    const version = jsonUsize(obj.get("format_version") orelse return error.InvalidGlideDecisionConfig) orelse return error.InvalidGlideDecisionConfig;
    if (version != 1) return error.UnsupportedGlideDecisionConfig;

    const backbone: Backbone = if (obj.get("backbone")) |v| switch (v) {
        .string => |s| if (std.mem.eql(u8, s, "encoder")) .encoder else if (std.mem.eql(u8, s, "decoder")) .decoder else return error.UnsupportedGlideBackbone,
        else => return error.InvalidGlideDecisionConfig,
    } else .decoder;
    const pooling: Pooling = if (obj.get("pooling")) |v| switch (v) {
        .string => |s| if (std.mem.eql(u8, s, "last")) .last else if (std.mem.eql(u8, s, "cls")) .cls else return error.UnsupportedGlidePooling,
        else => return error.InvalidGlideDecisionConfig,
    } else .last;
    if (pooling == .cls and backbone != .encoder) return error.UnsupportedGlidePooling;
    if (obj.get("prompt_format")) |v| switch (v) {
        .string => |s| if (!std.mem.eql(u8, s, supported_prompt_format)) return error.UnsupportedGlidePromptFormat,
        else => return error.InvalidGlideDecisionConfig,
    };
    if (obj.get("prompt_layout")) |v| switch (v) {
        .string => |s| if (!std.mem.eql(u8, s, "std")) return error.UnsupportedGlidePromptFormat,
        else => return error.InvalidGlideDecisionConfig,
    };
    // Looped variants carry extra parameter files and backbone surgery.
    if (obj.get("loop") != null or obj.get("midloop") != null) return error.UnsupportedGlideLoopedCheckpoint;
    if (obj.get("attention_mode")) |v| switch (v) {
        .string => |s| {
            const ok = switch (backbone) {
                .encoder => std.mem.eql(u8, s, "noncausal_bidirectional_encoder"),
                .decoder => std.mem.eql(u8, s, "noncausal_full_attention"),
            };
            if (!ok) return error.UnsupportedGlideAttentionMode;
        },
        else => return error.InvalidGlideDecisionConfig,
    };

    const max_len = if (obj.get("max_len")) |v| jsonUsize(v) orelse return error.InvalidGlideDecisionConfig else switch (backbone) {
        .encoder => default_encoder_max_len,
        .decoder => default_decoder_max_len,
    };
    const trunc_head = if (obj.get("trunc_head")) |v| jsonUsize(v) orelse return error.InvalidGlideDecisionConfig else default_trunc_head;
    if (max_len < 16 or (backbone == .encoder and trunc_head + 2 >= max_len)) return error.InvalidGlideDecisionConfig;
    const temperature = if (obj.get("temperature")) |v| jsonFloat(v) orelse return error.InvalidGlideDecisionConfig else 1.0;
    if (!std.math.isFinite(temperature) or temperature <= 0) return error.InvalidGlideDecisionConfig;

    const codes_value = switch (obj.get("codes") orelse return error.InvalidGlideDecisionConfig) {
        .array => |a| a,
        else => return error.InvalidGlideDecisionConfig,
    };
    if (codes_value.items.len != max_options) return error.InvalidGlideDecisionConfig;
    const codes = try allocator.alloc([]const u8, max_options);
    var filled: usize = 0;
    errdefer {
        for (codes[0..filled]) |code| allocator.free(code);
        allocator.free(codes);
    }
    for (codes_value.items, 0..) |item, index| {
        const code = switch (item) {
            .string => |s| s,
            else => return error.InvalidGlideDecisionConfig,
        };
        if (code.len == 0 or code.len > 4) return error.InvalidGlideDecisionConfig;
        for (code) |c| if (c < 'A' or c > 'Z') return error.InvalidGlideDecisionConfig;
        for (codes[0..index]) |previous| if (std.mem.eql(u8, previous, code)) return error.InvalidGlideDecisionConfig;
        codes[index] = try allocator.dupe(u8, code);
        filled += 1;
    }
    const model_name = try allocator.dupe(u8, if (obj.get("model_name")) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidGlideDecisionConfig,
    } else "");
    errdefer allocator.free(model_name);
    const render_tokenizer = try allocator.dupe(u8, if (obj.get("render_tokenizer")) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidGlideDecisionConfig,
    } else if (backbone == .encoder) "render_tokenizer" else "");
    return .{
        .backbone = backbone,
        .pooling = pooling,
        .max_len = max_len,
        .trunc_head = trunc_head,
        .temperature = temperature,
        .model_name = model_name,
        .codes = codes,
        .render_tokenizer = render_tokenizer,
    };
}

/// A directory is a GLiDE checkpoint when both decision sidecars exist and the
/// decision config declares the 255-code GLiDE label set.
pub fn declaredInDir(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    var config = loadDecisionConfig(allocator, model_dir) catch return false;
    config.deinit(allocator);
    return c_file.fileExistsInDir(allocator, model_dir, readout_file);
}

pub fn loadDecisionConfig(allocator: std.mem.Allocator, model_dir: []const u8) !DecisionConfig {
    const path = try std.fs.path.join(allocator, &.{ model_dir, decision_config_file });
    defer allocator.free(path);
    if (!c_file.fileExists(allocator, path)) return error.GlideDecisionConfigNotFound;
    const bytes = try c_file.readFileMax(allocator, path, 1 << 20);
    defer allocator.free(bytes);
    return parseDecisionConfig(allocator, bytes);
}

/// Session-resident decision head: the decision config plus the readout
/// widened to f32 ([max_options, hidden] row-major).
pub const Head = struct {
    allocator: std.mem.Allocator,
    config: DecisionConfig,
    hidden_size: usize,
    readout: []f32,

    pub fn deinit(self: *Head) void {
        self.allocator.free(self.readout);
        self.config.deinit(self.allocator);
    }

    /// logits[b, o] = sum_h hidden[b, h] * readout[o, h], f32 accumulation.
    pub fn apply(self: *const Head, hidden_rows: []const f32, rows: usize, logits: []f32) !void {
        const h = self.hidden_size;
        if (hidden_rows.len != rows * h or logits.len != rows * max_options) return error.InvalidInputShape;
        for (0..rows) |r| {
            const x = hidden_rows[r * h ..][0..h];
            for (0..max_options) |o| {
                const w = self.readout[o * h ..][0..h];
                var acc: @Vector(16, f32) = @splat(0);
                var i: usize = 0;
                while (i + 16 <= h) : (i += 16) {
                    const xv: @Vector(16, f32) = x[i..][0..16].*;
                    const wv: @Vector(16, f32) = w[i..][0..16].*;
                    acc += xv * wv;
                }
                var sum = @reduce(.Add, acc);
                while (i < h) : (i += 1) sum += x[i] * w[i];
                logits[r * max_options + o] = sum;
            }
        }
    }
};

pub fn loadHead(allocator: std.mem.Allocator, model_dir: []const u8, hidden_size: usize) !*Head {
    var config = try loadDecisionConfig(allocator, model_dir);
    errdefer config.deinit(allocator);
    const path = try std.fs.path.join(allocator, &.{ model_dir, readout_file });
    defer allocator.free(path);
    if (!c_file.fileExists(allocator, path)) return error.GlideReadoutNotFound;
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, path);
    defer reader.deinit();
    var weight = reader.readTensor("weight") catch return error.InvalidGlideReadout;
    defer weight.deinit();
    if (weight.shape.len != 2 or weight.shape[0] != max_options or weight.shape[1] != @as(i64, @intCast(hidden_size)))
        return error.InvalidGlideReadout;
    const count = max_options * hidden_size;
    const readout = try allocator.alloc(f32, count);
    errdefer allocator.free(readout);
    switch (weight.dtype) {
        .bf16 => tensor_mod.widenBf16LittleEndian(weight.data, readout),
        .f32 => @memcpy(std.mem.sliceAsBytes(readout), weight.data[0 .. count * 4]),
        .f16 => {
            for (readout, 0..) |*out, i| out.* = @floatCast(@as(f16, @bitCast(std.mem.readInt(u16, weight.data[i * 2 ..][0..2], .little))));
        },
        else => return error.InvalidGlideReadout,
    }
    for (readout) |v| if (!std.math.isFinite(v)) return error.InvalidGlideReadout;
    const head = try allocator.create(Head);
    head.* = .{ .allocator = allocator, .config = config, .hidden_size = hidden_size, .readout = readout };
    return head;
}

/// Softmax over the first `n` logits divided by `temperature` (f64, matching
/// the Python reference's float math).
pub fn optionProbabilities(logits: []const f32, n: usize, temperature: f64, out: []f64) !void {
    if (n == 0 or n > logits.len or n > max_options or out.len != n) return error.InvalidInputShape;
    var max_value = -std.math.inf(f64);
    for (logits[0..n]) |v| max_value = @max(max_value, @as(f64, v) / temperature);
    var total: f64 = 0;
    for (logits[0..n], out) |v, *p| {
        p.* = @exp(@as(f64, v) / temperature - max_value);
        total += p.*;
    }
    for (out) |*p| p.* /= total;
}

test "GLiDE decision config parses encoder and decoder checkpoints" {
    const allocator = std.testing.allocator;
    var codes_json: std.ArrayList(u8) = .empty;
    defer codes_json.deinit(allocator);
    try codes_json.appendSlice(allocator, "[");
    for (0..max_options) |i| {
        if (i != 0) try codes_json.appendSlice(allocator, ",");
        var buf: [4]u8 = undefined;
        const code = if (i < 26) try std.fmt.bufPrint(&buf, "{c}", .{@as(u8, @intCast('A' + i))}) else try std.fmt.bufPrint(&buf, "{c}{c}", .{ @as(u8, @intCast('A' + (i - 26) / 26)), @as(u8, @intCast('A' + (i - 26) % 26)) });
        try codes_json.print(allocator, "\"{s}\"", .{code});
    }
    try codes_json.appendSlice(allocator, "]");

    const encoder_json = try std.fmt.allocPrint(allocator,
        \\{{"format_version": 1, "backbone": "encoder", "attention_mode": "noncausal_bidirectional_encoder", "pooling": "last",
        \\ "max_len": 7999, "trunc_head": 1024, "render_tokenizer": "render_tokenizer", "prompt_format": "quick-empty-think-medium-v1",
        \\ "prompt_layout": "std", "temperature": 1.0, "codes": {s}, "model_name": "glide-0.4b"}}
    , .{codes_json.items});
    defer allocator.free(encoder_json);
    var encoder = try parseDecisionConfig(allocator, encoder_json);
    defer encoder.deinit(allocator);
    try std.testing.expectEqual(Backbone.encoder, encoder.backbone);
    try std.testing.expectEqual(@as(usize, 7999), encoder.max_len);
    try std.testing.expectEqual(@as(usize, 98), try encoder.readoutIndex(100));
    try std.testing.expectEqualStrings("glide-0.4b", encoder.model_name);
    try std.testing.expectEqualStrings("AA", encoder.codes[26]);

    const decoder_json = try std.fmt.allocPrint(allocator,
        \\{{"format_version": 1, "attention_mode": "noncausal_full_attention", "pooling": "last",
        \\ "prompt_format": "quick-empty-think-medium-v1", "codes": {s}, "temperature": 1.0, "model_name": "glide-0.8b"}}
    , .{codes_json.items});
    defer allocator.free(decoder_json);
    var decoder = try parseDecisionConfig(allocator, decoder_json);
    defer decoder.deinit(allocator);
    try std.testing.expectEqual(Backbone.decoder, decoder.backbone);
    try std.testing.expectEqual(default_decoder_max_len, decoder.max_len);
    try std.testing.expectEqual(@as(usize, 99), try decoder.readoutIndex(100));

    try std.testing.expectError(error.UnsupportedGlideDecisionConfig, parseDecisionConfig(allocator, "{\"format_version\": 2}"));
    try std.testing.expectError(error.InvalidGlideDecisionConfig, parseDecisionConfig(allocator, "{\"format_version\": 1, \"codes\": [\"A\"]}"));
}

test "GLiDE option softmax honours option count and temperature" {
    const logits = [_]f32{ 2.0, 1.0, 0.0, 100.0 };
    var probs: [3]f64 = undefined;
    try optionProbabilities(&logits, 3, 1.0, &probs);
    try std.testing.expectApproxEqAbs(@as(f64, 0.6652409557748219), probs[0], 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), probs[0] + probs[1] + probs[2], 1e-12);
    try optionProbabilities(&logits, 3, 2.0, &probs);
    try std.testing.expectApproxEqAbs(@as(f64, 0.506480391055654), probs[0], 1e-12);
}
