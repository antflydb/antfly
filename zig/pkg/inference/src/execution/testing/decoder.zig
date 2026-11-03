// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Real dense decoder weights shared by physical-runtime parity/lifetime tests.
const std = @import("std");
const ComputeBackend = @import("../../ops/ops.zig").ComputeBackend;
const graph_mod = @import("../../graph/root.zig");
const gpt_arch = @import("../../architectures/gpt.zig");
const BorrowedDecodeStateRuntime = graph_mod.decode_state_runtime.BorrowedDecodeStateRuntime;
const control = @import("../../execution_control.zig");
pub const Config = @import("../../models/gpt.zig").Config;
const gpt = @import("../../models/gpt.zig");
const native = @import("../../ops/native_compute.zig");
const Tensor = @import("../../backends/tensor.zig").Tensor;
pub fn config() gpt.Config {
    return .{ .family = .llama, .hidden_size = 64, .num_hidden_layers = 1, .num_attention_heads = 1, .intermediate_size = 128, .vocab_size = 2, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope, .max_position_embeddings = 64 };
}
fn put(store: *native.WeightStore, name: []const u8, shape: []const i64, norm: bool) !void {
    const a = store.allocator;
    var elements: usize = 1;
    for (shape) |dim| elements *= @intCast(dim);
    const values = try a.alloc(f32, elements);
    defer a.free(values);
    for (values, 0..) |*value, i| value.* = if (norm) 1 else if (std.mem.eql(u8, name, "model.embed_tokens.weight")) (if ((i / 64 == 0) == (i % 64 < 32)) @as(f32, 1.0 / 64.0) else @as(f32, -1.0 / 64.0)) else @as(f32, @floatFromInt(@as(i32, @intCast(i % 7)) - 3)) / 64;
    const key = try a.dupe(u8, name);
    errdefer a.free(key);
    var tensor = try Tensor.initFloat32(a, key, shape, values);
    errdefer tensor.deinit();
    try store.resident_weights.put(a, key, .{ .tensor = tensor });
}
pub fn populate(store: *native.WeightStore) !void {
    try put(store, "model.embed_tokens.weight", &.{ 2, 64 }, false);
    try put(store, "model.norm.weight", &.{64}, true);
    try put(store, "model.layers.0.input_layernorm.weight", &.{64}, true);
    try put(store, "model.layers.0.post_attention_layernorm.weight", &.{64}, true);
    for ([_][]const u8{ "q_proj", "k_proj", "v_proj", "o_proj" }) |name| {
        var key: [128]u8 = undefined;
        try put(store, try std.fmt.bufPrint(&key, "model.layers.0.self_attn.{s}.weight", .{name}), &.{ 64, 64 }, false);
    }
    try put(store, "model.layers.0.mlp.gate_proj.weight", &.{ 128, 64 }, false);
    try put(store, "model.layers.0.mlp.up_proj.weight", &.{ 128, 64 }, false);
    try put(store, "model.layers.0.mlp.down_proj.weight", &.{ 64, 128 }, false);
}
pub fn eager(cb: ComputeBackend, cfg: gpt.Config, ids: []const i64) ![]f32 {
    const a = std.testing.allocator;
    var state = graph_mod.decode_state_runtime.NativeDecodeState.initContiguous(a);
    defer state.deinit();
    var logical = BorrowedDecodeStateRuntime.init(&state);
    try logical.appendPrefillChunk(ids.len);
    const ctx = logical.makeDecodeContext(ids.len, ids.len);
    const all = try gpt_arch.forward(&cb, a, cfg, ids, 1, ids.len, &ctx);
    defer a.free(all);
    return a.dupe(f32, all[(ids.len - 1) * cfg.vocab_size ..][0..cfg.vocab_size]);
}
pub fn checkUnlocked(mutex: *std.atomic.Mutex, lane: *control.ExecutionLane) !void {
    try std.testing.expect(!lane.owned);
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
}
