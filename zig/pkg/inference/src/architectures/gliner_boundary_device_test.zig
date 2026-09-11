// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const build_options = @import("build_options");
const model = @import("../models/gliner_boundary.zig");
const device = @import("gliner_boundary_device.zig");
const ops = @import("../ops/ops.zig");
const parity = @import("gliner_boundary_parity_test.zig");
const native = @import("../ops/native_compute.zig");
const metal = @import("../ops/metal_compute.zig");
const gpu_store = @import("../ops/gpu_hosted_store.zig");

fn tinyConfig(a: std.mem.Allocator) !model.Config {
    const config_bytes = try parity.fixtureBytes(a, "models/base/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try parity.fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder_bytes);
    var config = try model.parseConfig(a, config_bytes, encoder_bytes);
    const capture_bytes = try parity.fixtureBytes(a, "tiny/capture.json");
    defer a.free(capture_bytes);
    const capture = try std.json.parseFromSlice(struct { boundary_head: struct { settings: model.HeadConfig } }, a, capture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    config.head = capture.value.boundary_head.settings;
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

fn expectTensor(a: std.mem.Allocator, result: *device.Result, tensor: ops.CT, expected: []const f32, tolerance: f32) !void {
    const actual = try result.download(tensor, expected.len);
    defer a.free(actual);
    try parity.expectFloats(expected, actual, tolerance, tolerance);
}

test "gliner boundary device Metal tiny shared pool parity with explicit transfer accounting" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const config = try tinyConfig(a);
    var weights = try parity.TensorFixture.init(a, "tiny/boundary_weights.safetensors");
    defer weights.deinit();
    var reference = try parity.TensorFixture.init(a, "tiny/boundary_tensors.safetensors");
    defer reference.deinit();
    var store = try loadMetalWeights(a, &weights);
    defer store.lazy_weights.deinit(a);
    metal.initPrefetchQueue(&store, a);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var backend = try metal.MetalCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const token_states = try reference.floats("input.token_states");
    const query_states = try reference.floats("input.query_states");
    const text = try cb.glinerBoundaryDevice(&.{ .upload_f32 = .{ .values = token_states, .shape = &.{ 2, 7, 32 } } });
    defer cb.free(text);
    const queries = try cb.glinerBoundaryDevice(&.{ .upload_f32 = .{ .values = query_states, .shape = &.{ 2, 3, 32 } } });
    defer cb.free(queries);
    const query_mask = try reference.booleans(a, "input.query_mask");
    defer a.free(query_mask);
    const input = device.Input{ .batch = 2, .text_length = 7, .queries = 3, .text_states = text, .query_states = queries, .text_lengths = &.{ 7, 3 }, .query_mask = query_mask };
    var prepared = try device.prepare(&cb, a, &config, input, .{ .query_chunk = 2, .explicit_chunk = 4 });
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 0), prepared.stats().proposal_download_calls);
    _ = try prepared.propose();
    try std.testing.expectEqual(@as(usize, 4), prepared.stats().proposal_download_calls);
    var result = try device.score(&prepared);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 4), result.stats().proposal_download_calls);
    try std.testing.expectEqual(@as(usize, 0), result.stats().result_download_calls);
    try std.testing.expectError(error.InvalidBoundaryDeviceShape, result.download(result.boundary_states, 2 * 8 * 16 + 1));
    try std.testing.expectEqual(@as(usize, 0), result.stats().result_download_calls);
    try std.testing.expectEqual((try device.plan(&config, input, .{})).proposal_download_bytes, result.stats().proposal_download_bytes);
    try std.testing.expect(metal.MetalCompute.debugHasDeviceTensor(&cb, result.boundary_states));
    try std.testing.expect(metal.MetalCompute.debugHasDeviceTensor(&cb, result.pair_logits));
    try expectTensor(a, &result, result.boundary_states, try reference.floats("boundary.states"), 3e-5);
    try expectTensor(a, &result, result.marginals.start_logits, try reference.floats("marginal.start"), 3e-5);
    try expectTensor(a, &result, result.marginals.end_logits, try reference.floats("marginal.end"), 3e-5);
    try expectTensor(a, &result, result.marginals.inside_logits, try reference.floats("marginal.inside"), 3e-5);
    const packed_prefix = try result.download(result.marginals.inside_prefix_and_mean, 2 * 3 * 9);
    defer a.free(packed_prefix);
    const prefixes = try reference.floats("marginal.inside_prefix");
    const means = try reference.floats("marginal.inside_mean");
    for (0..6) |row| {
        try parity.expectFloats(prefixes[row * 8 ..][0..8], packed_prefix[row * 9 ..][0..8], 3e-5, 3e-5);
        try std.testing.expectApproxEqAbs(means[row], packed_prefix[row * 9 + 8], 3e-5);
    }
    const indices = try reference.tensor("pool.indices");
    const valid = try reference.booleans(a, "pool.mask");
    defer a.free(valid);
    const pool = result.pool();
    try std.testing.expectEqualSlices(bool, valid, pool.valid);
    for (pool.indices, 0..) |span, i| {
        try std.testing.expectEqual(@as(usize, @intCast(std.mem.readInt(i64, indices.data[i * 16 ..][0..8], .little))), span.start);
        try std.testing.expectEqual(@as(usize, @intCast(std.mem.readInt(i64, indices.data[i * 16 + 8 ..][0..8], .little))), span.end);
    }
    try expectTensor(a, &result, result.candidate_features, try reference.floats("shared.features"), 4e-5);
    try expectTensor(a, &result, result.pair_logits, try reference.floats("candidate.logits"), 8e-5);
    // Python broadcasts the shared [B,C,H] states to [B,Q,C,H]. Keep the
    // native/device representation shared and compare every oracle view.
    const candidate_states = try result.download(result.candidate_states.?, 2 * pool.capacity * 32);
    defer a.free(candidate_states);
    const expected_states = try reference.floats("candidate.states");
    for (0..2) |b| for (0..3) |q| {
        try parity.expectFloats(expected_states[(b * 3 + q) * pool.capacity * 32 ..][0 .. pool.capacity * 32], candidate_states[b * pool.capacity * 32 ..][0 .. pool.capacity * 32], 4e-5, 4e-5);
    };
    try expectTensor(a, &result, result.null_logits.?, try reference.floats("query.abstention_logits"), 3e-5);
    try expectTensor(a, &result, result.count_log_rates.?, try reference.floats("query.count_log_rates"), 3e-5);
    const explicit_tensor = try reference.tensor("input.explicit_spans");
    var explicit_spans: [18]@import("gliner_boundary_head.zig").SignedSpan = undefined;
    for (&explicit_spans, 0..) |*span, i| span.* = .{
        .start = std.mem.readInt(i64, explicit_tensor.data[i * 16 ..][0..8], .little),
        .end = std.mem.readInt(i64, explicit_tensor.data[i * 16 + 8 ..][0..8], .little),
    };
    const before_explicit = result.stats();
    var explicit_result = try result.scoreExplicit(.{ .capacity = 3, .indices = &explicit_spans });
    defer explicit_result.deinit();
    try std.testing.expectEqual(@as(usize, 5), explicit_result.chunks.len);
    try std.testing.expectEqual(before_explicit.result_download_calls, result.stats().result_download_calls);
    try std.testing.expectEqual(before_explicit.proposal_download_calls, result.stats().proposal_download_calls);
    const explicit_logits = try explicit_result.download();
    defer a.free(explicit_logits);
    try parity.expectFloats(try reference.floats("explicit.logits"), explicit_logits, 8e-5, 4e-5);
    const subset_spans = explicit_spans[15..18].* ++ explicit_spans[9..12].*;
    const before_subset = result.stats();
    var subset = try result.scoreExplicitSubset(1, &.{ 2, 0 }, .{ .capacity = 3, .indices = &subset_spans });
    defer subset.deinit();
    try std.testing.expectEqual(before_subset.result_download_calls, result.stats().result_download_calls);
    try std.testing.expectEqual(before_subset.proposal_download_calls, result.stats().proposal_download_calls);
    const subset_logits = try subset.download();
    defer a.free(subset_logits);
    try parity.expectFloats(explicit_logits[15..18], subset_logits[0..3], 8e-5, 4e-5);
    try parity.expectFloats(explicit_logits[9..12], subset_logits[3..6], 8e-5, 4e-5);
    // Physical integer metadata cannot be substituted with float-valued masks.
    const float_mask = try cb.glinerBoundaryDevice(&.{ .upload_f32 = .{ .values = &.{1}, .shape = &.{1} } });
    defer cb.free(float_mask);
    var malformed = ops.gliner_boundary_device.Kernel{ .kind = .mask_rows, .dims = .{ 1, 1, 0, 0, 0, 0, 0, 0 } };
    malformed.inputs[0] = float_mask;
    malformed.inputs[1] = float_mask;
    try std.testing.expectError(error.InvalidBoundaryDeviceShape, cb.glinerBoundaryDevice(&.{ .kernel = malformed }));
}

test "gliner boundary device Metal pinned small checkpoint shared head parity" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const path = try std.fmt.allocPrint(a, "{s}/model.safetensors", .{directory});
    defer a.free(path);
    var weights = parity.TensorFixture{ .allocator = a, .reader = try @import("../models/safetensors.zig").MMapReader.openFileAbsolute(a, path) };
    defer weights.deinit();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(weights.reader.file_bytes, &digest, .{});
    try std.testing.expectEqualStrings("4ee982787ace270d4bf15dbcb28ced38e0aa201372347114ceedd6336055de2b", &std.fmt.bytesToHex(digest, .lower));
    const config_bytes = try parity.fixtureBytes(a, "models/small/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try parity.fixtureBytes(a, "models/small/encoder_config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    var store = try loadMetalWeights(a, &weights);
    defer store.lazy_weights.deinit(a);
    metal.initPrefetchQueue(&store, a);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var backend = try metal.MetalCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    for ([_][]const u8{ "mixed_tasks", "entity_attributes", "enum_field", "legacy_structure", "record_anchorless", "record_latent", "record_natural", "unicode_offsets" }) |name| {
        errdefer std.debug.print("small checkpoint device head fixture: {s}\n", .{name});
        const fixture_name = try std.fmt.allocPrint(a, "small_reference/{s}.safetensors", .{name});
        defer a.free(fixture_name);
        var reference = try parity.TensorFixture.init(a, fixture_name);
        defer reference.deinit();
        const text_tensor = try reference.tensor("encoded.text");
        const query_tensor = try reference.tensor("encoded.query");
        const batch: usize = @intCast(text_tensor.shape[0]);
        const length: usize = @intCast(text_tensor.shape[1]);
        const queries: usize = @intCast(query_tensor.shape[1]);
        const query_mask = try reference.booleans(a, "encoded.query_mask");
        defer a.free(query_mask);
        const text_mask = try reference.booleans(a, "encoded.text_mask");
        defer a.free(text_mask);
        const lengths = try a.alloc(usize, batch);
        defer a.free(lengths);
        @memset(lengths, 0);
        for (0..batch) |b| for (text_mask[b * length ..][0..length]) |valid| {
            lengths[b] += @intFromBool(valid);
        };
        const text = try cb.glinerBoundaryDevice(&.{ .upload_f32 = .{ .values = try reference.floats("encoded.text"), .shape = &.{ @intCast(batch), @intCast(length), @intCast(config.encoder.hidden_size) } } });
        defer cb.free(text);
        const query = try cb.glinerBoundaryDevice(&.{ .upload_f32 = .{ .values = try reference.floats("encoded.query"), .shape = &.{ @intCast(batch), @intCast(queries), @intCast(config.encoder.hidden_size) } } });
        defer cb.free(query);
        var result = try device.forwardDevice(&cb, a, &config, .{ .batch = batch, .text_length = length, .queries = queries, .text_states = text, .query_states = query, .text_lengths = lengths, .query_mask = query_mask }, .{ .query_chunk = 3 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 4), result.stats().proposal_download_calls);
        try std.testing.expectEqual(@as(usize, 0), result.stats().result_download_calls);
        try expectTensor(a, &result, result.marginals.start_logits, try reference.floats("marginal.start"), 1e-4);
        try expectTensor(a, &result, result.marginals.end_logits, try reference.floats("marginal.end"), 1e-4);
        try expectTensor(a, &result, result.marginals.inside_logits, try reference.floats("marginal.inside"), 1e-4);
        try expectTensor(a, &result, result.pair_logits, try reference.floats("candidate.logits"), 3e-4);
        const indices = try reference.tensor("candidate.indices");
        const valid = try reference.booleans(a, "candidate.mask");
        defer a.free(valid);
        const pool = result.pool();
        try std.testing.expectEqual(batch * queries * pool.capacity, valid.len);
        for (0..batch) |b| for (0..queries) |q| for (0..pool.capacity) |c| {
            const i = (b * queries + q) * pool.capacity + c;
            const span = pool.indices[b * pool.capacity + c];
            try std.testing.expectEqual(valid[i], pool.valid[b * pool.capacity + c]);
            try std.testing.expectEqual(@as(usize, @intCast(std.mem.readInt(i64, indices.data[i * 16 ..][0..8], .little))), span.start);
            try std.testing.expectEqual(@as(usize, @intCast(std.mem.readInt(i64, indices.data[i * 16 + 8 ..][0..8], .little))), span.end);
        };
    }
}
