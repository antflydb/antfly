// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Exact requested host cache storage. The producer and cold admission share
//! the same checked dimensions and allocation order; allocator overhead is not
//! payload credit.
const std = @import("std");
const Config = @import("gpt.zig").Config;
pub const Layout = struct {
    layers: usize,
    linear_layers: usize,
    full_attention_interval: usize,
    conv_elements: usize,
    recurrent_elements: usize,
    backing_bytes: usize,

    pub fn init(config: Config, header_size: usize, header_alignment: usize) !Layout {
        if (header_alignment == 0 or !std.math.isPowerOfTwo(header_alignment) or header_size % header_alignment != 0) return error.InvalidModelConfig;
        var linear: usize = 0;
        for (0..config.num_hidden_layers) |layer| if (config.layerUsesQwen35LinearAttention(layer) and !config.qwen35_device_only_state) {
            linear += 1;
        };
        const key = try std.math.mul(usize, config.qwen35_linear_num_key_heads, config.qwen35_linear_key_head_dim);
        const value = try std.math.mul(usize, config.qwen35_linear_num_value_heads, config.qwen35_linear_value_head_dim);
        const channels = try std.math.add(usize, try std.math.mul(usize, key, 2), value);
        const conv = try std.math.mul(usize, channels, config.qwen35_linear_conv_kernel_dim);
        const recurrent = try std.math.mul(usize, try std.math.mul(usize, config.qwen35_linear_num_value_heads, config.qwen35_linear_key_head_dim), config.qwen35_linear_value_head_dim);
        if (linear != 0 and (conv == 0 or recurrent == 0)) return error.InvalidModelConfig;
        var cursor = try std.math.mul(usize, config.num_hidden_layers, header_size);
        for (0..linear) |_| {
            cursor = (try std.math.add(usize, cursor, @alignOf(f32) - 1)) & ~@as(usize, @alignOf(f32) - 1);
            cursor = try std.math.add(usize, cursor, try std.math.mul(usize, conv, @sizeOf(f32)));
            cursor = (try std.math.add(usize, cursor, @alignOf(f32) - 1)) & ~@as(usize, @alignOf(f32) - 1);
            cursor = try std.math.add(usize, cursor, try std.math.mul(usize, recurrent, @sizeOf(f32)));
        }
        return .{ .layers = config.num_hidden_layers, .linear_layers = linear, .full_attention_interval = if (config.qwen35_full_attention_interval == 0) 4 else config.qwen35_full_attention_interval, .conv_elements = conv, .recurrent_elements = recurrent, .backing_bytes = cursor };
    }
};

test "Native recurrent host layout includes all layer headers and checked exact original arrays" {
    const cfg = Config{ .family = .qwen3_5, .num_hidden_layers = 5, .qwen35_has_linear_attention = true, .qwen35_full_attention_interval = 4, .qwen35_linear_num_key_heads = 1, .qwen35_linear_num_value_heads = 1, .qwen35_linear_key_head_dim = 2, .qwen35_linear_value_head_dim = 2, .qwen35_linear_conv_kernel_dim = 4 };
    const layout = try Layout.init(cfg, 32, 8);
    try std.testing.expectEqual(@as(usize, 4), layout.linear_layers);
    try std.testing.expectEqual(@as(usize, 24), layout.conv_elements);
    try std.testing.expectEqual(@as(usize, 4), layout.recurrent_elements);
    try std.testing.expectEqual(@as(usize, 5 * 32 + 4 * (24 + 4) * 4), layout.backing_bytes);
    var invalid = cfg;
    invalid.qwen35_linear_num_key_heads = std.math.maxInt(u32);
    invalid.qwen35_linear_key_head_dim = std.math.maxInt(u32);
    try std.testing.expectError(error.Overflow, Layout.init(invalid, 32, 8));
    try std.testing.expectError(error.InvalidModelConfig, Layout.init(cfg, 32, 3));
}
