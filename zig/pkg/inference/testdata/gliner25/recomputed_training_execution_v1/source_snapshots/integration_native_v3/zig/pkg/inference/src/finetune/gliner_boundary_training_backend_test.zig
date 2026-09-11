// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const backend = @import("gliner_boundary_training_backend.zig");
const native = @import("../ops/native_compute.zig");
const run = @import("gliner_boundary_run.zig");
const controller = @import("seeded_gradient_trainer.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const ml = @import("ml").graph;
const build_options = @import("build_options");
const metal_runtime = @import("../backends/metal_runtime.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const Allocator = std.mem.Allocator;

const originals = [_]run.Parameter{
    .{ .name = "encoder.weight", .canonical_name = "encoder.encoder.weight", .dimensions = &.{2}, .values = &.{ 1, 2 }, .kind = .original },
    .{ .name = "classifier.bias", .canonical_name = "classifier.bias", .dimensions = &.{2}, .values = &.{ 3, 4 }, .kind = .original },
};
const selected = [_]controller.Parameter{.{ .name = "classifier.bias", .dimensions = &.{2}, .values = &.{ 3, 4 }, .group = 0 }};
const adapters = [_]controller.Parameter{.{ .name = "encoder.lora_A.default.weight", .dimensions = &.{ 1, 2 }, .values = &.{ 0.1, 0.2 }, .group = 0 }};

fn source(a: Allocator) !native.WeightStore {
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    errdefer store.deinitOwned();
    for (originals) |parameter| {
        const name = try a.dupe(u8, parameter.name);
        errdefer a.free(name);
        var tensor = try Tensor.initFloat32(a, name, &.{2}, parameter.values);
        errdefer tensor.deinit();
        try store.resident_weights.put(a, name, .{ .tensor = tensor });
    }
    return store;
}
fn graph(a: Allocator) !ml.Graph {
    var value = ml.Graph.init(a);
    errdefer value.deinit();
    var builder = ml.Builder.init(&value);
    const frozen = try builder.parameter("encoder.weight", ml.Shape.init(.f32, &.{2}));
    _ = try builder.parameter("classifier.bias", ml.Shape.init(.f32, &.{2}));
    _ = try builder.parameter("__gliner25.runtime", ml.Shape.init(.i32, &.{2}));
    try value.markOutput(frozen);
    return value;
}

test "boundary training backend metadata preflight validates selection and rejects unadmitted payloads" {
    const cpu = try backend.estimate(&originals, &selected, .native, .{});
    try std.testing.expectEqual(@as(usize, 1), cpu.frozen_parameters);
    try std.testing.expectEqual(@as(usize, 0), cpu.frozen_device_bytes);
    try std.testing.expectEqual(@as(usize, 0), cpu.upload_staging_bytes);
    try std.testing.expectError(error.BoundaryTrainingBackendLimitExceeded, backend.estimate(&originals, &selected, .native, .{ .max_combined_bytes = 1 }));
    var invalid = selected;
    invalid[0].dimensions = &.{ 1, 2 };
    try std.testing.expectError(error.TrainingBindingShapeMismatch, backend.estimate(&originals, &invalid, .native, .{}));
    var repeated = originals;
    repeated[1] = originals[0];
    try std.testing.expectError(error.DuplicateBoundaryTrainingBackendParameter, backend.estimate(&repeated, &selected, .native, .{}));
    invalid = selected;
    invalid[0].name = "__runtime";
    try std.testing.expectError(error.InvalidBoundaryTrainingBackendParameter, backend.estimate(&originals, &invalid, .native, .{}));
    if (comptime build_options.enable_metal) {
        const heads = try backend.estimate(&originals, &selected, .resident_metal, .{});
        try std.testing.expectEqual(@as(usize, 8), heads.frozen_device_bytes);
        try std.testing.expectEqual(@as(usize, 8), heads.upload_staging_bytes);
        const peft = try backend.estimate(&originals, &adapters, .resident_metal, .{});
        try std.testing.expectEqual(@as(usize, 2), peft.frozen_parameters);
        try std.testing.expectEqual(@as(usize, 16), peft.frozen_device_bytes);
        const full = [_]controller.Parameter{
            .{ .name = originals[0].name, .dimensions = originals[0].dimensions, .values = originals[0].values, .group = 0 },
            selected[0],
        };
        const all = try backend.estimate(&originals, &full, .resident_metal, .{});
        try std.testing.expectEqual(@as(usize, 0), all.frozen_parameters);
        try std.testing.expectEqual(@as(usize, 0), all.frozen_device_bytes);
        try std.testing.expectError(error.BoundaryTrainingBackendLimitExceeded, backend.estimate(&originals, &selected, .resident_metal, .{ .max_frozen_device_bytes = 7 }));
        try std.testing.expectError(error.BoundaryTrainingBackendLimitExceeded, backend.estimate(&originals, &selected, .resident_metal, .{ .max_upload_staging_bytes = 7 }));
    } else {
        var store = native.WeightStore{ .allocator = std.testing.allocator, .resident_weights = .empty, .lazy_weights = .empty };
        defer store.deinitOwned();
        var failed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
        try std.testing.expectError(error.UnsupportedBoundaryTrainingBackend, backend.Owner.init(failed.allocator(), &store, &originals, &selected, .resident_metal, .{}, null));
    }
}

fn cpuExercise(a: Allocator) !void {
    var store = try source(a);
    defer store.deinitOwned();
    const owner = try backend.Owner.init(a, &store, &originals, &selected, .native, .{}, null);
    var live = true;
    defer if (live) owner.deinit();
    try std.testing.expectEqual(.native, owner.cb.kind());
    try std.testing.expectEqual(native.QuantizedActivationPolicy.strict_f32, owner.native_backend.?.quantized_activation_policy);
    try std.testing.expectEqual(@as(usize, 0), owner.receipt.upload_bytes);
    var g = try graph(a);
    defer g.deinit();
    {
        var bindings = try owner.bindFrozen(&g, null);
        defer bindings.deinit();
        try std.testing.expectEqual(@as(usize, 0), bindings.inputs.len);
        try std.testing.expectError(error.TrainingBackendBindingStillLive, owner.bindFrozen(&g, null));
        const value = try owner.cb.acquireWeight("encoder.weight");
        defer owner.cb.free(value);
        const values = try owner.cb.toFloat32(value, a);
        defer a.free(values);
        try std.testing.expectEqualSlices(f32, originals[0].values, values);
    }
    owner.deinit();
    live = false;
    // The source owns the original tensor and remains usable after its
    // backend/provider owner has released every weight handle.
    try std.testing.expectEqualSlices(f32, originals[0].values, store.resident_weights.get("encoder.weight").?.tensor.asFloat32());
}

test "boundary training backend preserves native strict math source lifetime and empty frozen leases" {
    try cpuExercise(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cpuExercise, .{});
}

const Cancel = struct {
    calls: usize = 0,
    at: usize,
    fn check(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        if (self.calls >= self.at) return error.Cancelled;
    }
};
test "boundary training backend native cancellation leaves the source owner intact" {
    const a = std.testing.allocator;
    var store = try source(a);
    defer store.deinitOwned();
    for ([_]usize{ 1, 2 }) |at| {
        var cancel = Cancel{ .at = at };
        try std.testing.expectError(error.Cancelled, backend.Owner.init(a, &store, &originals, &selected, .native, .{}, .{ .ptr = &cancel, .check_fn = Cancel.check }));
        try std.testing.expectEqualSlices(f32, originals[0].values, store.resident_weights.get("encoder.weight").?.tensor.asFloat32());
    }
}

fn deviceAvailable() !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
}

test "boundary training backend Metal uploads frozen weights once and owns temporary selection metadata" {
    try deviceAvailable();
    const a = std.testing.allocator;
    var store = try source(a);
    defer store.deinitOwned();
    var temporary = std.heap.ArenaAllocator.init(a);
    var temporary_live = true;
    defer if (temporary_live) temporary.deinit();
    const selected_name = try temporary.allocator().dupe(u8, selected[0].name);
    const selected_dims = try temporary.allocator().dupe(i32, selected[0].dimensions);
    const selection = [_]controller.Parameter{.{ .name = selected_name, .dimensions = selected_dims, .values = selected[0].values, .group = 0 }};
    const owner = try backend.Owner.init(a, &store, &originals, &selection, .resident_metal, .{}, null);
    defer owner.deinit();
    temporary.deinit();
    temporary_live = false;
    try std.testing.expectEqual(@as(usize, 1), owner.receipt.upload_tensors);
    try std.testing.expectEqual(@as(usize, 8), owner.receipt.upload_bytes);
    try std.testing.expectEqual(@as(usize, 1), owner.frozen.len);
    var g = try graph(a);
    defer g.deinit();
    for (0..3) |_| {
        const before = metal_tensor.memoryStatsSnapshot();
        var bindings = try owner.bindFrozen(&g, null);
        defer bindings.deinit();
        const after = metal_tensor.memoryStatsSnapshot();
        try std.testing.expectEqual(before.device_owned_buffers_created, after.device_owned_buffers_created);
        try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
        try std.testing.expectEqual(@as(usize, 1), bindings.inputs.len);
        try std.testing.expectEqual(@as(usize, 1), owner.receipt.upload_tensors);
        try std.testing.expectError(error.TrainingBackendBindingStillLive, owner.bindFrozen(&g, null));
        var actual: [2]f32 = undefined;
        try owner.cb.glinerBoundaryDownload(bindings.inputs[0].value, &actual);
        try std.testing.expectEqualSlices(f32, originals[0].values, &actual);
    }
}

fn bindAllocation(a: Allocator, owner: *backend.Owner, g: *const ml.Graph) !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const old_metadata = owner.metadata.backing;
    const old_backend = owner.metal_backend.?.backend.allocator;
    owner.metadata.backing = a;
    owner.metal_backend.?.backend.allocator = a;
    defer {
        owner.metadata.backing = old_metadata;
        owner.metal_backend.?.backend.allocator = old_backend;
    }
    var bindings = try owner.bindFrozen(g, null);
    defer bindings.deinit();
    try std.testing.expectEqual(@as(usize, 1), bindings.inputs.len);
}

test "boundary training backend Metal validates bindings and recovers cancellation and allocation failures" {
    try deviceAvailable();
    const a = std.testing.allocator;
    var store = try source(a);
    defer store.deinitOwned();
    const owner = try backend.Owner.init(a, &store, &originals, &selected, .resident_metal, .{}, null);
    defer owner.deinit();
    var g = try graph(a);
    defer g.deinit();
    const baseline = owner.metadata.live;
    try std.testing.checkAllAllocationFailures(a, bindAllocation, .{ owner, &g });
    try std.testing.expectEqual(@as(usize, 0), owner.active_bindings);
    try std.testing.expectEqual(baseline, owner.metadata.live);
    var probe = Cancel{ .at = std.math.maxInt(usize) };
    {
        var binding = try owner.bindFrozen(&g, .{ .ptr = &probe, .check_fn = Cancel.check });
        binding.deinit();
    }
    for (1..probe.calls + 1) |at| {
        var cancel = Cancel{ .at = at };
        try std.testing.expectError(error.Cancelled, owner.bindFrozen(&g, .{ .ptr = &cancel, .check_fn = Cancel.check }));
        try std.testing.expectEqual(@as(usize, 0), owner.active_bindings);
        try std.testing.expectEqual(baseline, owner.metadata.live);
    }
    g.nodes.items[g.parameters.items[0]].output_shape = ml.Shape.init(.f32, &.{ 1, 2 });
    try std.testing.expectError(error.TrainingBindingShapeMismatch, owner.bindFrozen(&g, null));
    g.nodes.items[g.parameters.items[0]].output_shape = ml.Shape.init(.f32, &.{2});
    try g.parameters.append(a, g.parameters.items[0]);
    try std.testing.expectError(error.DuplicateBoundaryTrainingBinding, owner.bindFrozen(&g, null));
    g.parameters.items.len -= 1;
    var builder = ml.Builder.init(&g);
    _ = try builder.parameter("unknown.weight", ml.Shape.init(.f32, &.{2}));
    try std.testing.expectError(error.UnknownBoundaryTrainingParameter, owner.bindFrozen(&g, null));
    try std.testing.expectEqual(baseline, owner.metadata.live);
}

test "boundary training backend Metal preflight finite checks and cancelled initialization release uploads" {
    try deviceAvailable();
    const a = std.testing.allocator;
    var store = try source(a);
    defer store.deinitOwned();
    var invalid = originals;
    invalid[0].values = &.{ std.math.nan(f32), 2 };
    const before = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectError(error.NonFiniteTrainingParameter, backend.Owner.init(a, &store, &invalid, &selected, .resident_metal, .{}, null));
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.device_owned_buffers_created, after.device_owned_buffers_created);
    for ([_]usize{ 1, 2, 4, 5, 6 }) |at| {
        var cancel = Cancel{ .at = at };
        const prior = metal_tensor.memoryStatsSnapshot();
        try std.testing.expectError(error.Cancelled, backend.Owner.init(a, &store, &originals, &selected, .resident_metal, .{}, .{ .ptr = &cancel, .check_fn = Cancel.check }));
        const following = metal_tensor.memoryStatsSnapshot();
        try std.testing.expectEqual(prior.device_owned_live_bytes, following.device_owned_live_bytes);
    }
    const owner = try backend.Owner.init(a, &store, &originals, &adapters, .resident_metal, .{}, null);
    defer owner.deinit();
    try std.testing.expectEqual(@as(usize, 2), owner.receipt.upload_tensors);
    try std.testing.expectEqual(@as(usize, 16), owner.receipt.upload_bytes);
}
