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

// YaRN RoPE scaling for context extension (Peng et al., 2023).
//
// `attn_scale` is stored as the pre-sqrt value; `sqrt(attn_scale)` is applied
// to both cos and sin tables so the effective attention logit scale is linear.

const std = @import("std");

pub const YaRNConfig = struct {
    rope_dim: usize,
    original_max_position: u32,
    extended_max_position: u32,
    rope_base: f32 = 10000.0,
    beta_fast: f32 = 32.0,
    beta_slow: f32 = 1.0,
    scale_override: f32 = 0.0,
};

pub const YaRNTable = struct {
    allocator: std.mem.Allocator,
    inv_freq: []f32,
    attn_scale: f32,

    pub fn deinit(self: *YaRNTable) void {
        self.allocator.free(self.inv_freq);
        self.* = undefined;
    }
};

fn computeScale(config: YaRNConfig) f32 {
    if (config.scale_override > 0.0) return config.scale_override;
    const orig: f32 = @floatFromInt(config.original_max_position);
    const ext: f32 = @floatFromInt(config.extended_max_position);
    if (orig <= 0.0) return 1.0;
    return ext / orig;
}

pub fn buildYaRN(allocator: std.mem.Allocator, config: YaRNConfig) !YaRNTable {
    std.debug.assert(config.rope_dim % 2 == 0);
    const half = config.rope_dim / 2;
    const inv_freq = try allocator.alloc(f32, half);
    errdefer allocator.free(inv_freq);

    const s = computeScale(config);
    const dim_f: f32 = @floatFromInt(config.rope_dim);
    const orig_f: f32 = @floatFromInt(config.original_max_position);
    const two_pi: f32 = 2.0 * std.math.pi;

    const beta_fast = config.beta_fast;
    const beta_slow = config.beta_slow;
    const denom = beta_fast - beta_slow;

    var i: usize = 0;
    while (i < half) : (i += 1) {
        const i_f: f32 = @floatFromInt(i);
        const exponent = (2.0 * i_f) / dim_f;
        const base_inv_freq = 1.0 / std.math.pow(f32, config.rope_base, exponent);

        if (s == 1.0) {
            inv_freq[i] = base_inv_freq;
            continue;
        }

        const lambda = two_pi / base_inv_freq;
        const num_rotations = orig_f / lambda;

        var r: f32 = 0.0;
        if (denom != 0.0) {
            r = (num_rotations - beta_slow) / denom;
        }
        if (r < 0.0) r = 0.0;
        if (r > 1.0) r = 1.0;

        const interp = base_inv_freq / s;
        const extrap = base_inv_freq;
        inv_freq[i] = (1.0 - r) * interp + r * extrap;
    }

    var attn_scale: f32 = 1.0;
    if (s > 1.0) {
        attn_scale = 0.1 * @log(s) + 1.0;
    }

    return YaRNTable{
        .allocator = allocator,
        .inv_freq = inv_freq,
        .attn_scale = attn_scale,
    };
}

pub fn fillCosSinTables(
    table: *const YaRNTable,
    rope_dim: usize,
    seq_len: usize,
    cos_out: []f32,
    sin_out: []f32,
) void {
    const half = rope_dim / 2;
    std.debug.assert(table.inv_freq.len == half);
    std.debug.assert(cos_out.len == seq_len * half);
    std.debug.assert(sin_out.len == seq_len * half);

    const mscale = std.math.sqrt(table.attn_scale);

    var pos: usize = 0;
    while (pos < seq_len) : (pos += 1) {
        const pos_f: f32 = @floatFromInt(pos);
        const row = pos * half;
        var i: usize = 0;
        while (i < half) : (i += 1) {
            const theta = pos_f * table.inv_freq[i];
            cos_out[row + i] = std.math.cos(theta) * mscale;
            sin_out[row + i] = std.math.sin(theta) * mscale;
        }
    }
}

test "yarn: s=1 matches base rope inv_freq" {
    const allocator = std.testing.allocator;
    const config = YaRNConfig{
        .rope_dim = 64,
        .original_max_position = 2048,
        .extended_max_position = 2048,
    };
    var table = try buildYaRN(allocator, config);
    defer table.deinit();

    try std.testing.expectEqual(@as(usize, 32), table.inv_freq.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), table.attn_scale, 1e-6);

    const dim_f: f32 = @floatFromInt(config.rope_dim);
    var i: usize = 0;
    while (i < 32) : (i += 1) {
        const i_f: f32 = @floatFromInt(i);
        const expected = 1.0 / std.math.pow(f32, 10000.0, (2.0 * i_f) / dim_f);
        try std.testing.expectApproxEqRel(expected, table.inv_freq[i], 1e-5);
    }
}

test "yarn: s=2 extrapolates high-freq, interpolates low-freq" {
    const allocator = std.testing.allocator;
    const config = YaRNConfig{
        .rope_dim = 64,
        .original_max_position = 2048,
        .extended_max_position = 4096,
    };
    var table = try buildYaRN(allocator, config);
    defer table.deinit();

    const dim_f: f32 = @floatFromInt(config.rope_dim);

    // i == 0 -> highest frequency, inv_freq == 1.0, many rotations fit -> extrapolate
    const base_high = 1.0 / std.math.pow(f32, 10000.0, 0.0);
    try std.testing.expectApproxEqRel(base_high, table.inv_freq[0], 1e-5);

    // i == half-1 -> lowest frequency, very few rotations -> interpolated (divided by s=2)
    const last = 31;
    const i_f: f32 = @floatFromInt(last);
    const base_low = 1.0 / std.math.pow(f32, 10000.0, (2.0 * i_f) / dim_f);
    const expected_low = base_low / 2.0;
    try std.testing.expectApproxEqRel(expected_low, table.inv_freq[last], 1e-4);
}

test "yarn: attn_scale for s=4" {
    const allocator = std.testing.allocator;
    const config = YaRNConfig{
        .rope_dim = 64,
        .original_max_position = 2048,
        .extended_max_position = 8192,
    };
    var table = try buildYaRN(allocator, config);
    defer table.deinit();

    const expected: f32 = 0.1 * @log(@as(f32, 4.0)) + 1.0;
    try std.testing.expectApproxEqAbs(expected, table.attn_scale, 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 1.1386), table.attn_scale, 1e-3);
}

test "yarn: fillCosSinTables pos=0 gives cos=1 sin=0 when s=1" {
    const allocator = std.testing.allocator;
    const config = YaRNConfig{
        .rope_dim = 32,
        .original_max_position = 2048,
        .extended_max_position = 2048,
    };
    var table = try buildYaRN(allocator, config);
    defer table.deinit();

    const seq_len: usize = 4;
    const half = config.rope_dim / 2;
    const cos_out = try allocator.alloc(f32, seq_len * half);
    defer allocator.free(cos_out);
    const sin_out = try allocator.alloc(f32, seq_len * half);
    defer allocator.free(sin_out);

    fillCosSinTables(&table, config.rope_dim, seq_len, cos_out, sin_out);

    var i: usize = 0;
    while (i < half) : (i += 1) {
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), cos_out[i], 1e-6);
        try std.testing.expectApproxEqAbs(@as(f32, 0.0), sin_out[i], 1e-6);
    }

    // Spot-check pos=1: cos^2 + sin^2 == 1 (mscale == 1 for s=1)
    i = 0;
    while (i < half) : (i += 1) {
        const c = cos_out[half + i];
        const s_v = sin_out[half + i];
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), c * c + s_v * s_v, 1e-5);
    }
}

test "yarn: deinit frees cleanly" {
    const allocator = std.testing.allocator;
    const config = YaRNConfig{
        .rope_dim = 128,
        .original_max_position = 4096,
        .extended_max_position = 16384,
    };
    var table = try buildYaRN(allocator, config);
    table.deinit();
}
