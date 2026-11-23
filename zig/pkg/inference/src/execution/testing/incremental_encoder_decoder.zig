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

//! Deterministic real encoder, cross-attention and incremental KV fixture.
const std = @import("std");
const NativeData = @import("../../ops/native_compute.zig").WeightStore;
const Tensor = @import("../../backends/tensor.zig").Tensor;
pub const config: @import("../../models/whisper.zig").Config = .{ .d_model = 4, .num_mel_bins = 2, .encoder_layers = 0, .decoder_layers = 1, .encoder_attention_heads = 2, .decoder_attention_heads = 2, .encoder_ffn_dim = 8, .decoder_ffn_dim = 8, .max_source_positions = 2, .max_target_positions = 8, .vocab_size = 4 };
fn put(store: *NativeData, name: []const u8, shape: []const i64, norm: bool) !void {
    var count: usize = 1;
    for (shape) |dim| count *= @intCast(dim);
    const a = store.allocator;
    const values = try a.alloc(f32, count);
    defer a.free(values);
    for (values, 0..) |*value, i| value.* = if (norm) 1 else if (std.mem.endsWith(u8, name, ".bias")) 0 else @as(f32, @floatFromInt(@as(i32, @intCast(i % 13)) - 6)) / 32;
    const key = try a.dupe(u8, name);
    errdefer a.free(key);
    var tensor = try Tensor.initFloat32(a, key, shape, values);
    errdefer tensor.deinit();
    try store.resident_weights.put(a, key, .{ .tensor = tensor });
}

pub fn populate(store: *NativeData) !void {
    try put(store, "model.encoder.conv1.weight", &.{ 4, 2, 3 }, false);
    try put(store, "model.encoder.conv1.bias", &.{4}, false);
    try put(store, "model.encoder.conv2.weight", &.{ 4, 4, 3 }, false);
    try put(store, "model.encoder.conv2.bias", &.{4}, false);
    try put(store, "model.encoder.embed_positions.weight", &.{ 2, 4 }, false);
    try put(store, "model.encoder.layer_norm.weight", &.{4}, true);
    try put(store, "model.encoder.layer_norm.bias", &.{4}, false);
    try put(store, "model.decoder.embed_tokens.weight", &.{ 4, 4 }, false);
    try put(store, "model.decoder.embed_positions.weight", &.{ 8, 4 }, false);
    try put(store, "model.decoder.layer_norm.weight", &.{4}, true);
    try put(store, "model.decoder.layer_norm.bias", &.{4}, false);
    for ([_][]const u8{ "self_attn_layer_norm", "encoder_attn_layer_norm", "final_layer_norm" }) |name| {
        var buf: [128]u8 = undefined;
        try put(store, try std.fmt.bufPrint(&buf, "model.decoder.layers.0.{s}.weight", .{name}), &.{4}, true);
        try put(store, try std.fmt.bufPrint(&buf, "model.decoder.layers.0.{s}.bias", .{name}), &.{4}, false);
    }
    for ([_][]const u8{ "self_attn.q_proj", "self_attn.k_proj", "self_attn.v_proj", "self_attn.out_proj", "encoder_attn.q_proj", "encoder_attn.k_proj", "encoder_attn.v_proj", "encoder_attn.out_proj", "fc1", "fc2" }) |name| {
        const output: i64 = if (std.mem.eql(u8, name, "fc1")) 8 else 4;
        const input: i64 = if (std.mem.eql(u8, name, "fc2")) 8 else 4;
        var buf: [128]u8 = undefined;
        try put(store, try std.fmt.bufPrint(&buf, "model.decoder.layers.0.{s}.weight", .{name}), &.{ output, input }, false);
        try put(store, try std.fmt.bufPrint(&buf, "model.decoder.layers.0.{s}.bias", .{name}), &.{output}, false);
    }
}
