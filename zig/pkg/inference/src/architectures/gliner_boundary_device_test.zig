// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const model = @import("../models/gliner_boundary.zig");
const device = @import("gliner_boundary_device.zig");
const ops = @import("../ops/ops.zig");
const parity = @import("gliner_boundary_parity_test.zig");
const native = @import("../ops/native_compute.zig");
const gpu_store = @import("../ops/gpu_hosted_store.zig");

fn tinyConfig(a: std.mem.Allocator) !model.Config {
    const config_bytes = try parity.fixtureBytes(a, "models/base/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try parity.fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder_bytes);
    var config = try model.parseConfig(a, config_bytes, encoder_bytes);
    config.head.boundary_dim = 16;
    config.head.pair_dim = 16;
    config.head.content_dim = 8;
    config.head.record_dim = 16;
    config.head.record_instance_queries = 4;
    config.head.boundary_attention_window = 4;
    config.head.start_top_k = 4;
    config.head.end_top_k = 4;
    config.head.ends_per_start = 2;
    config.head.starts_per_end = 2;
    config.head.candidate_budget = 12;
    config.head.training_candidate_budget = 16;
    config.head.max_gold_per_query = 4;
    config.head.end_block_size = 8;
    config.head.pool_boundary_top_k = 4;
    config.head.pool_size = 12;
    config.head.min_pool_per_query = 2;
    config.head.relation_heads_per_type = 4;
    config.head.relation_tails_per_type = 4;
    config.head.relation_pair_cap = 8;
    config.head.multihead_pair_compat_heads = 4;
    config.head.dropout = 0;
    config.encoder.hidden_size = 32;
    config.encoder.num_attention_heads = 4;
    config.encoder.intermediate_size = 64;
    return config;
}

test "gliner boundary device admission separates transfer and query chunk budgets" {
    const config = try tinyConfig(std.testing.allocator);
    const input = device.Input{ .batch = 2, .text_length = 7, .queries = 3, .text_states = @ptrFromInt(8), .query_states = @ptrFromInt(16), .text_lengths = &.{ 7, 3 }, .query_mask = &.{ true, true, true, true, false, true } };
    const plan = try device.plan(&config, input, .{ .query_chunk = 2 });
    try std.testing.expectEqual(@as(usize, 2 * 2 * 8 * (3 + 16) * 4), plan.proposal_download_bytes);
    try std.testing.expectEqual(@as(usize, 2 * 12 * 2 * 16), plan.film_chunk_elements);
    try std.testing.expectError(error.ResourceLimitExceeded, device.plan(&config, input, .{ .max_proposal_download_bytes = plan.proposal_download_bytes - 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, device.plan(&config, input, .{ .max_device_bytes = 4, .query_chunk = 1 }));
    var invalid = input;
    invalid.text_lengths = &.{ 8, 3 };
    try std.testing.expectError(error.InvalidInputShape, device.plan(&config, invalid, .{}));
    invalid = input;
    invalid.query_mask = &.{true};
    try std.testing.expectError(error.InvalidInputShape, device.plan(&config, invalid, .{}));
}

fn unavailableDevice(_: *anyopaque, _: *const ops.gliner_boundary_device.Request) anyerror!ops.CT {
    return error.UnsupportedGlinerBoundaryDevice;
}

fn metalKind(_: *anyopaque) ops.BackendKind {
    return .metal;
}

fn failedPrepare(a: std.mem.Allocator, cb: *const ops.ComputeBackend, config: *const model.Config, input: device.Input) !void {
    var prepared = device.prepare(cb, a, config, input, .{}) catch |err| switch (err) {
        error.UnsupportedGlinerBoundaryDevice => return,
        else => return err,
    };
    defer prepared.deinit();
    return error.ExpectedUnsupportedDevice;
}

test "gliner boundary device rejects fallback and cleans failed preparation allocations" {
    const a = std.testing.allocator;
    const config = try tinyConfig(a);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    var cb = backend.computeBackend();
    const text = try cb.fromFloat32Shape(&(@as([2 * 7 * 32]f32, @splat(0))), &.{ 2, 7, 32 });
    defer cb.free(text);
    const queries = try cb.fromFloat32Shape(&(@as([2 * 3 * 32]f32, @splat(0))), &.{ 2, 3, 32 });
    defer cb.free(queries);
    const input = device.Input{ .batch = 2, .text_length = 7, .queries = 3, .text_states = text, .query_states = queries, .text_lengths = &.{ 7, 3 }, .query_mask = &.{ true, true, true, true, false, true } };
    try std.testing.expectError(error.UnsupportedGlinerBoundaryDevice, device.prepare(&cb, a, &config, input, .{}));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryDevice, cb.glinerBoundaryDevice(&.{ .resident_f32 = .{ .input = text } }));
    // Backend-independent rejection after owner construction proves that
    // absence of a device primitive cannot enter CPU math as a fallback.
    var vt = cb.vtable.*;
    vt.backendKind = metalKind;
    vt.glinerBoundaryDevice = unavailableDevice;
    cb.vtable = &vt;
    try std.testing.checkAllAllocationFailures(a, failedPrepare, .{ &cb, &config, input });
    cb.execution_control = .{ .check_fn = struct {
        fn cancelled(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    }.cancelled };
    try std.testing.expectError(error.Cancelled, device.prepare(&cb, a, &config, input, .{}));
}

pub fn loadMetalWeights(a: std.mem.Allocator, fixture: *const parity.TensorFixture) !gpu_store.WeightStore {
    var store = gpu_store.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty, .prefer_f32_dense_tensors = true };
    errdefer store.lazy_weights.deinit(a);
    var it = fixture.reader.header.tensors.iterator();
    while (it.next()) |entry| {
        const name = entry.key_ptr.*;
        const key = if (std.mem.startsWith(u8, name, "encoder.embeddings.") or std.mem.startsWith(u8, name, "encoder.encoder.")) name["encoder.".len..] else name;
        if (!std.mem.startsWith(u8, key, "boundary_head.") and !std.mem.startsWith(u8, key, "embeddings.") and
            !std.mem.startsWith(u8, key, "encoder.") and !std.mem.startsWith(u8, key, "classifier.") and
            !std.mem.startsWith(u8, key, "record_decoder.") and !std.mem.startsWith(u8, key, "relation_scorer.")) continue;
        const tensor = try fixture.tensor(name);
        try store.lazy_weights.put(a, key, .{ .tensor_ref = .{ .name = name }, .host_loaded = .{ .tensor = tensor }, .active_tier = .host, .loaded_bytes = tensor.data.len, .prefer_dense = true });
    }
    return store;
}
