// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differential learned-task tests against the pinned CPU PyTorch capture.
const std = @import("std");
const fixtures = @import("gliner_boundary_parity_test.zig");
const tasks = @import("gliner_boundary_tasks.zig");
const boundary = @import("../models/gliner_boundary.zig");
const compute = @import("../ops/ops.zig");
const native = @import("../ops/native_compute.zig");
const Span = @import("gliner_boundary_ops.zig").Span;
const head = @import("gliner_boundary_head.zig");

pub const Capture = struct {
    format_version: u32,
    hidden: u32,
    record_dim: u32,
    instance_queries: u32,
    relation_query_dim: u32,
    relation_biaffine_content: bool,
    provenance: struct { commit: []const u8 },
    records: []RecordCase,
};

pub const RecordCase = struct {
    id: []const u8,
    tensor_prefix: []const u8,
    mode: tasks.RecordMode,
    sample: usize,
    instances: usize,
    instance_seed: []?[2]usize,
    fields: []struct { tensor_prefix: []const u8, is_anchor: bool },
};

pub fn loadCapture(a: std.mem.Allocator) !std.json.Parsed(Capture) {
    const bytes = try fixtures.fixtureBytes(a, "tasks/capture.json");
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(Capture, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer parsed.deinit();
    try std.testing.expectEqual(@as(u32, 1), parsed.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", parsed.value.provenance.commit);
    return parsed;
}

pub fn testConfig(a: std.mem.Allocator, capture: Capture) !boundary.Config {
    const bytes = try fixtures.fixtureBytes(a, "models/base/config.json");
    defer a.free(bytes);
    const encoder = try fixtures.fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder);
    var config = try boundary.parseConfig(a, bytes, encoder);
    config.encoder.hidden_size = capture.hidden;
    config.head.record_dim = capture.record_dim;
    config.head.record_instance_queries = capture.instance_queries;
    config.head.dropout = 0; // Capture uses create_mlp with no Dropout module.
    config.head.directional_relation_states = capture.relation_query_dim == 2 * capture.hidden;
    config.head.relation_biaffine_content = capture.relation_biaffine_content;
    return config;
}

pub fn i64At(reference: *const fixtures.TensorFixture, name: []const u8, index: usize) !i64 {
    const tensor = try reference.tensor(name);
    if (tensor.dtype != .i64 or tensor.data.len % 8 != 0 or index >= tensor.data.len / 8) return error.InvalidFixtureTensor;
    return std.mem.readInt(i64, tensor.data[index * 8 ..][0..8], .little);
}

pub fn readSpans(a: std.mem.Allocator, reference: *const fixtures.TensorFixture, name: []const u8) ![]Span {
    const tensor = try reference.tensor(name);
    if (tensor.dtype != .i64 or tensor.data.len % 16 != 0) return error.InvalidFixtureTensor;
    const spans = try a.alloc(Span, tensor.data.len / 16);
    errdefer a.free(spans);
    for (spans, 0..) |*span, i| {
        const start = try i64At(reference, name, i * 2);
        const end = try i64At(reference, name, i * 2 + 1);
        if (start < 0 or end <= start) return error.InvalidFixtureTensor;
        span.* = .{ .start = @intCast(start), .end = @intCast(end) };
    }
    return spans;
}

test "gliner boundary Python parity learned classification and allocation cleanup" {
    const a = std.testing.allocator;
    const capture = try loadCapture(a);
    defer capture.deinit();
    const config = try testConfig(a, capture.value);
    var weights = try fixtures.TensorFixture.init(a, "tasks/weights.safetensors");
    defer weights.deinit();
    var reference = try fixtures.TensorFixture.init(a, "tasks/tensors.safetensors");
    defer reference.deinit();
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const states = try reference.floats("classification.input.states");
    const expected = try reference.floats("classification.expected.logits");
    const input = tasks.ClassificationInput{ .choices = expected.len, .choice_states = states };
    var result = try tasks.classifyNative(&cb, a, &config, input, .{});
    defer result.deinit();
    try fixtures.expectFloats(expected, result.logits, 2e-5, 2e-5);
    try fixtures.expectFloats(try reference.floats("classification.intermediate.1"), result.hidden, 2e-5, 2e-5);
    const Check = struct {
        fn run(allocator: std.mem.Allocator, backend_: *const compute.ComputeBackend, config_: *const boundary.Config, input_: tasks.ClassificationInput) !void {
            var output = try tasks.classifyNative(backend_, allocator, config_, input_, .{});
            defer output.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ &cb, &config, input });
}

test "gliner boundary Python parity directional biaffine relation scoring" {
    const a = std.testing.allocator;
    const capture = try loadCapture(a);
    defer capture.deinit();
    const config = try testConfig(a, capture.value);
    var weights = try fixtures.TensorFixture.init(a, "tasks/weights.safetensors");
    defer weights.deinit();
    var reference = try fixtures.TensorFixture.init(a, "tasks/tensors.safetensors");
    defer reference.deinit();
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const mask = try reference.booleans(a, "relation.input.pair_mask");
    defer a.free(mask);
    const pairs = try a.alloc(tasks.RelationPair, mask.len);
    defer a.free(pairs);
    for (pairs, 0..) |*pair, i| pair.* = .{
        .batch_index = try i64At(&reference, "relation.input.batch_index", i),
        .relation_index = try i64At(&reference, "relation.input.relation_index", i),
        .head_span = .{ .start = try i64At(&reference, "relation.input.head_start", i), .end = try i64At(&reference, "relation.input.head_end", i) },
        .tail_span = .{ .start = try i64At(&reference, "relation.input.tail_start", i), .end = try i64At(&reference, "relation.input.tail_end", i) },
        .valid = mask[i],
    };
    const input = tasks.RelationInput{
        .batch = 2,
        .sequence_length = 5,
        .relations = 2,
        .text_states = try reference.floats("relation.input.token_states"),
        .relation_query_states = try reference.floats("relation.input.query_states"),
        .pairs = pairs,
    };
    var result = try tasks.scoreRelationsNative(&cb, a, &config, input, .{});
    defer result.deinit();
    try fixtures.expectFloats(try reference.floats("relation.expected.logits"), result.logits, 2e-5, 2e-5);
    try fixtures.expectFloats(try reference.floats("relation.intermediate.mlp.1"), result.hidden, 2e-5, 2e-5);
    try fixtures.expectFloats(try reference.floats("relation.intermediate.mlp.3"), result.mlp_logits, 2e-5, 2e-5);
    try fixtures.expectFloats(try reference.floats("relation.intermediate.head_content_projection"), result.head_content.?, 2e-5, 2e-5);
    try fixtures.expectFloats(try reference.floats("relation.intermediate.tail_content_projection"), result.tail_content.?, 2e-5, 2e-5);
    try std.testing.expectEqualSlices(bool, mask, result.valid);
    for (mask, result.logits) |valid, logit| if (!valid) try std.testing.expectEqual(@as(f32, 0), logit);

    // Routing errors never gather a negative index or leak a valid pair score.
    pairs[0].batch_index = -1;
    pairs[1].relation_index = 2;
    var invalid = try tasks.scoreRelationsNative(&cb, a, &config, input, .{});
    defer invalid.deinit();
    try std.testing.expect(!invalid.valid[0] and !invalid.valid[1]);
    try std.testing.expectEqual(@as(f32, 0), invalid.logits[0]);
    try std.testing.expectEqual(@as(f32, 0), invalid.logits[1]);
}

test "gliner boundary Python parity natural latent and anchorless record scoring" {
    const a = std.testing.allocator;
    const capture = try loadCapture(a);
    defer capture.deinit();
    try std.testing.expectEqual(@as(usize, 6), capture.value.records.len);
    const config = try testConfig(a, capture.value);
    var weights = try fixtures.TensorFixture.init(a, "tasks/weights.safetensors");
    defer weights.deinit();
    var reference = try fixtures.TensorFixture.init(a, "tasks/tensors.safetensors");
    defer reference.deinit();
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    for (capture.value.records) |case| {
        errdefer std.debug.print("record scoring case: {s}\n", .{case.id});
        var prepared = std.heap.ArenaAllocator.init(a);
        defer prepared.deinit();
        const pa = prepared.allocator();
        const fields = try pa.alloc(tasks.RecordFieldInput, case.fields.len);
        var anchor: ?usize = null;
        var name: [256]u8 = undefined;
        for (case.fields, 0..) |field, i| {
            fields[i] = .{
                .query_state = try reference.floats(try std.fmt.bufPrint(&name, "{s}.input.query_state", .{field.tensor_prefix})),
                .candidate_states = try reference.floats(try std.fmt.bufPrint(&name, "{s}.input.candidate_states", .{field.tensor_prefix})),
                .candidate_spans = try readSpans(pa, &reference, try std.fmt.bufPrint(&name, "{s}.input.spans", .{field.tensor_prefix})),
                .candidate_logits = try reference.floats(try std.fmt.bufPrint(&name, "{s}.input.logits", .{field.tensor_prefix})),
            };
            if (field.is_anchor) anchor = i;
        }
        const input = tasks.RecordInput{ .mode = case.mode, .fields = fields, .anchor_field = anchor };
        var result = try tasks.scoreRecordNative(&cb, a, &config, input, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.instances, result.object_logits.len);
        try fixtures.expectFloats(try reference.floats(try std.fmt.bufPrint(&name, "{s}.expected.object_logits", .{case.tensor_prefix})), result.object_logits, 2e-5, 2e-5);
        try fixtures.expectFloats(try reference.floats(try std.fmt.bufPrint(&name, "{s}.expected.instance_states", .{case.tensor_prefix})), result.instance_states, 2e-5, 2e-5);
        try std.testing.expectEqual(case.instance_seed.len, result.instance_seeds.len);
        for (case.instance_seed, result.instance_seeds) |expected, actual| {
            if (expected) |seed| {
                try std.testing.expect(actual != null);
                try std.testing.expectEqual(seed[0], actual.?.field);
                try std.testing.expectEqual(seed[1], actual.?.candidate);
            } else try std.testing.expect(actual == null);
        }
        for (case.fields, result.fields, 0..) |field, actual, i| {
            try fixtures.expectFloats(try reference.floats(try std.fmt.bufPrint(&name, "{s}.expected.assignment", .{field.tensor_prefix})), actual.assign_logits, 2e-5, 2e-5);
            try std.testing.expectEqualSlices(Span, fields[i].candidate_spans, actual.candidate_spans);
            try std.testing.expectEqualSlices(f32, fields[i].candidate_logits, actual.candidate_logits);
        }
        if (case.mode == .anchorless and case.sample == 0) {
            const Check = struct {
                fn run(allocator: std.mem.Allocator, backend_: *const compute.ComputeBackend, config_: *const boundary.Config, input_: tasks.RecordInput) !void {
                    var output = try tasks.scoreRecordNative(backend_, allocator, config_, input_, .{});
                    defer output.deinit();
                }
            };
            try std.testing.checkAllAllocationFailures(a, Check.run, .{ &cb, &config, input });
        }
    }
}

test "gliner boundary full head and explicit scoring clean up every allocation failure" {
    const a = std.testing.allocator;
    const config_bytes = try fixtures.fixtureBytes(a, "models/base/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtures.fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder_bytes);
    var config = try boundary.parseConfig(a, config_bytes, encoder_bytes);
    const capture_bytes = try fixtures.fixtureBytes(a, "tiny/capture.json");
    defer a.free(capture_bytes);
    const capture = try std.json.parseFromSlice(struct { boundary_head: struct { settings: boundary.HeadConfig } }, a, capture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    config.head = capture.value.boundary_head.settings;
    config.encoder.hidden_size = 32;
    var weights = try fixtures.TensorFixture.init(a, "tiny/boundary_weights.safetensors");
    defer weights.deinit();
    var reference = try fixtures.TensorFixture.init(a, "tiny/boundary_tensors.safetensors");
    defer reference.deinit();
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const query_mask = try reference.booleans(a, "input.query_mask");
    defer a.free(query_mask);
    const spans = try a.alloc(head.SignedSpan, 18);
    defer a.free(spans);
    for (spans, 0..) |*span, i| span.* = .{
        .start = try i64At(&reference, "input.explicit_spans", 2 * i),
        .end = try i64At(&reference, "input.explicit_spans", 2 * i + 1),
    };
    const input = head.Input{
        .batch = 2,
        .text_length = 7,
        .queries = 3,
        .text_states = try reference.floats("input.token_states"),
        .query_states = try reference.floats("input.query_states"),
        .text_lengths = &.{ 7, 3 },
        .query_mask = query_mask,
    };
    const explicit = head.ExplicitInput{ .capacity = 3, .indices = spans };
    const Check = struct {
        fn run(allocator: std.mem.Allocator, backend_: *const compute.ComputeBackend, config_: *const boundary.Config, input_: head.Input, explicit_: head.ExplicitInput) !void {
            var shared = try head.forwardNative(backend_, allocator, config_, input_, .{});
            defer shared.deinit();
            var explicit_result = try head.scoreExplicitSpansNative(backend_, allocator, config_, input_, explicit_, .{});
            defer explicit_result.deinit();
            try std.testing.expectEqual(@as(usize, 2 * 3 * 12), shared.pair_logits.len);
            try std.testing.expectEqual(@as(usize, 18), explicit_result.logits.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ &cb, &config, input, explicit });
    try std.testing.expectError(error.ResourceLimitExceeded, head.forwardNative(&cb, a, &config, input, .{ .max_intermediate_bytes = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, head.scoreExplicitSpansNative(&cb, a, &config, input, explicit, .{ .max_intermediate_bytes = 1 }));
}
