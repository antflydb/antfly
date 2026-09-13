// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const build_options = @import("build_options");
const ops = @import("../ops/ops.zig");
const math_mod = @import("gliner_boundary_device_math.zig");
const tasks = @import("gliner_boundary_tasks_device.zig");
const native_tasks = @import("gliner_boundary_tasks.zig");
const fixtures = @import("gliner_boundary_parity_test.zig");
const task_tests = @import("gliner_boundary_tasks_test.zig");
const metal_tests = @import("gliner_boundary_device_test.zig");
const native = @import("../ops/native_compute.zig");
const metal = @import("../ops/metal_compute.zig");

fn unavailable(_: *anyopaque, _: *const ops.gliner_boundary_device.Request) anyerror!ops.CT {
    return error.UnsupportedGlinerBoundaryDevice;
}
fn metalKind(_: *anyopaque) ops.BackendKind {
    return .metal;
}

test "gliner boundary device tasks reject limits and clean failed child ownership" {
    const a = std.testing.allocator;
    const capture = try task_tests.loadCapture(a);
    defer capture.deinit();
    const config = try task_tests.testConfig(a, capture.value);
    var weights = try fixtures.TensorFixture.init(a, "tasks/weights.safetensors");
    defer weights.deinit();
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    var cb = backend.computeBackend();
    var vt = cb.vtable.*;
    vt.backendKind = metalKind;
    vt.glinerBoundaryDevice = unavailable;
    cb.vtable = &vt;
    const math = try math_mod.Context.create(a, &cb, .{}, null);
    defer math.destroy();
    const input = tasks.ClassificationInput{ .choices = 2, .choice_states = @ptrFromInt(8) };
    try std.testing.expectError(error.ResourceLimitExceeded, tasks.classify(math, &config, input, .{ .max_classification_choices = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, tasks.scoreRelations(math, &config, .{ .batch = 1, .text_length = 1, .relations = 1, .text_states = @ptrFromInt(8), .relation_query_states = @ptrFromInt(16), .pairs = &.{.{ .batch_index = 0, .relation_index = 0, .head_span = .{ .start = 0, .end = 1 }, .tail_span = .{ .start = 0, .end = 1 } }} }, .{ .max_relation_pairs = 0 }));
    const record = tasks.RecordInput{ .mode = .natural, .fields = &.{}, .query_states = @ptrFromInt(8), .query_rows = 1, .candidate_states = null, .candidate_rows = 0 };
    try std.testing.expectError(error.InvalidGlinerRecordRouting, tasks.scoreRecord(math, &config, record, .{}));
    const Check = struct {
        fn run(allocator: std.mem.Allocator, backend_: *const ops.ComputeBackend, config_: *const @TypeOf(config), input_: tasks.ClassificationInput) !void {
            const owner = try math_mod.Context.create(allocator, backend_, .{}, null);
            defer owner.destroy();
            var result = tasks.classify(owner, config_, input_, .{}) catch |err| switch (err) {
                error.UnsupportedGlinerBoundaryDevice => return,
                else => return err,
            };
            defer result.deinit();
            return error.ExpectedUnsupportedDevice;
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ &cb, &config, input });
}

fn upload(math: *math_mod.Context, values: []const f32) !ops.CT {
    return math.execute(.{ .upload_f32 = .{ .values = values, .shape = &.{@intCast(values.len)} } }, values.len);
}
fn expectTensor(math: *math_mod.Context, tensor: ops.CT, expected: []const f32) !void {
    const actual = try math.download(tensor, expected.len, false);
    defer math.allocator.free(actual);
    try fixtures.expectFloats(expected, actual, 3e-5, 3e-5);
}

test "gliner boundary device Metal learned classification relation and all record modes parity" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const capture = try task_tests.loadCapture(a);
    defer capture.deinit();
    const config = try task_tests.testConfig(a, capture.value);
    var weights = try fixtures.TensorFixture.init(a, "tasks/weights.safetensors");
    defer weights.deinit();
    var reference = try fixtures.TensorFixture.init(a, "tasks/tensors.safetensors");
    defer reference.deinit();
    var store = try metal_tests.loadMetalWeights(a, &weights);
    defer store.lazy_weights.deinit(a);
    metal.initPrefetchQueue(&store, a);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var backend = try metal.MetalCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const math = try math_mod.Context.create(a, &cb, .{}, null);
    defer math.destroy();
    {
        const states = try upload(math, try reference.floats("classification.input.states"));
        defer math.drop(states);
        const expected = try reference.floats("classification.expected.logits");
        const before = math.stats.result_download_calls;
        var result = try tasks.classify(math, &config, .{ .choices = expected.len, .choice_states = states }, .{});
        defer result.deinit();
        try std.testing.expectEqual(before, math.stats.result_download_calls);
        try expectTensor(math, result.logits.?, expected);
        try expectTensor(math, result.hidden.?, try reference.floats("classification.intermediate.1"));
    }
    {
        const text = try upload(math, try reference.floats("relation.input.token_states"));
        defer math.drop(text);
        const queries = try upload(math, try reference.floats("relation.input.query_states"));
        defer math.drop(queries);
        const valid = try reference.booleans(a, "relation.input.pair_mask");
        defer a.free(valid);
        const pairs = try a.alloc(native_tasks.RelationPair, valid.len);
        defer a.free(pairs);
        for (pairs, 0..) |*pair, i| pair.* = .{ .batch_index = try task_tests.i64At(&reference, "relation.input.batch_index", i), .relation_index = try task_tests.i64At(&reference, "relation.input.relation_index", i), .head_span = .{ .start = try task_tests.i64At(&reference, "relation.input.head_start", i), .end = try task_tests.i64At(&reference, "relation.input.head_end", i) }, .tail_span = .{ .start = try task_tests.i64At(&reference, "relation.input.tail_start", i), .end = try task_tests.i64At(&reference, "relation.input.tail_end", i) }, .valid = valid[i] };
        const input = tasks.RelationInput{ .batch = 2, .text_length = 5, .relations = 2, .text_states = text, .relation_query_states = queries, .pairs = pairs };
        const before = math.stats.result_download_calls;
        var result = try tasks.scoreRelations(math, &config, input, .{});
        defer result.deinit();
        try std.testing.expectEqual(before, math.stats.result_download_calls);
        try expectTensor(math, result.logits.?, try reference.floats("relation.expected.logits"));
        try expectTensor(math, result.hidden.?, try reference.floats("relation.intermediate.mlp.1"));
        try expectTensor(math, result.mlp_logits.?, try reference.floats("relation.intermediate.mlp.3"));
        try expectTensor(math, result.head_content.?, try reference.floats("relation.intermediate.head_content_projection"));
        try expectTensor(math, result.tail_content.?, try reference.floats("relation.intermediate.tail_content_projection"));
        try std.testing.expectEqualSlices(bool, valid, result.valid);
        pairs[0].batch_index = -1;
        pairs[1].relation_index = 2;
        var masked = try tasks.scoreRelations(math, &config, input, .{});
        defer masked.deinit();
        const masked_logits = try masked.download();
        defer a.free(masked_logits);
        try std.testing.expect(!masked.valid[0] and !masked.valid[1]);
        try std.testing.expectEqual(@as(f32, 0), masked_logits[0]);
        try std.testing.expectEqual(@as(f32, 0), masked_logits[1]);
    }
    for (capture.value.records) |case| {
        errdefer std.debug.print("resident record fixture: {s}\n", .{case.id});
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const fields = try sa.alloc(tasks.RecordField, case.fields.len);
        var all_states: std.ArrayList(f32) = .empty;
        const query_states = try sa.alloc(f32, case.fields.len * capture.value.hidden);
        var anchor: ?usize = null;
        var name: [256]u8 = undefined;
        for (case.fields, 0..) |field, f| {
            const values = try reference.floats(try std.fmt.bufPrint(&name, "{s}.input.candidate_states", .{field.tensor_prefix}));
            const spans = try task_tests.readSpans(sa, &reference, try std.fmt.bufPrint(&name, "{s}.input.spans", .{field.tensor_prefix}));
            const indices = try sa.alloc(usize, spans.len);
            for (indices, 0..) |*index, c| index.* = all_states.items.len / capture.value.hidden + c;
            try all_states.appendSlice(sa, values);
            @memcpy(query_states[f * capture.value.hidden ..][0..capture.value.hidden], try reference.floats(try std.fmt.bufPrint(&name, "{s}.input.query_state", .{field.tensor_prefix})));
            fields[f] = .{ .query_index = f, .candidate_indices = indices, .candidate_spans = spans, .candidate_logits = try reference.floats(try std.fmt.bufPrint(&name, "{s}.input.logits", .{field.tensor_prefix})) };
            if (field.is_anchor) anchor = f;
        }
        const queries = try upload(math, query_states);
        defer math.drop(queries);
        const candidates = if (all_states.items.len > 0) try upload(math, all_states.items) else null;
        defer if (candidates) |tensor| math.drop(tensor);
        const before = math.stats.result_download_calls;
        var result = try tasks.scoreRecord(math, &config, .{ .mode = case.mode, .fields = fields, .anchor_field = anchor, .query_states = queries, .query_rows = fields.len, .candidate_states = candidates, .candidate_rows = all_states.items.len / capture.value.hidden }, .{});
        defer result.deinit();
        try std.testing.expectEqual(before, math.stats.result_download_calls);
        try std.testing.expectEqual(case.instances, result.instances);
        try expectTensor(math, result.object_logits.?, try reference.floats(try std.fmt.bufPrint(&name, "{s}.expected.object_logits", .{case.tensor_prefix})));
        try expectTensor(math, result.instance_states.?, try reference.floats(try std.fmt.bufPrint(&name, "{s}.expected.instance_states", .{case.tensor_prefix})));
        const assignments = try result.downloadAssignments();
        defer a.free(assignments);
        for (case.fields, result.fields, 0..) |field, actual, f| {
            try fixtures.expectFloats(try reference.floats(try std.fmt.bufPrint(&name, "{s}.expected.assignment", .{field.tensor_prefix})), assignments[actual.assignment_offset..][0 .. result.instances * actual.columns], 3e-5, 3e-5);
            try std.testing.expectEqualSlices(@import("gliner_boundary_ops.zig").Span, fields[f].candidate_spans, actual.candidate_spans);
        }
        for (case.instance_seed, result.instance_seeds) |expected, actual| {
            if (expected) |seed| {
                try std.testing.expect(actual != null);
                try std.testing.expectEqual(seed[0], actual.?.field);
                try std.testing.expectEqual(seed[1], actual.?.candidate);
            } else try std.testing.expect(actual == null);
        }
    }
}
