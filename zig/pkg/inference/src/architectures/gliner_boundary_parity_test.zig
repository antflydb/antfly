// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differential tests consume captured outputs from the pinned Python source.
//! No Python installation or checkpoint download is required to run them.
const std = @import("std");
const primitive = @import("gliner_boundary_ops.zig");
const decode = @import("../pipelines/gliner_boundary_decode.zig");
const c_file = @import("../util/c_file.zig");
const safetensors = @import("../models/safetensors.zig");
const tensor_mod = @import("../backends/tensor.zig");
const native = @import("../ops/native_compute.zig");
const boundary_model = @import("../models/gliner_boundary.zig");
const head = @import("gliner_boundary_head.zig");

test "gliner boundary Python parity typed relation proposals with ragged masks and caps" {
    const a = std.testing.allocator;
    const relations = @import("../pipelines/gliner_boundary_relations.zig");
    const bytes = try fixtureBytes(a, "relations.json");
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(struct {
        cases: []struct { id: []const u8, input: relations.Input, routes: []relations.Route, options: struct { heads_per_relation: usize, tails_per_relation: usize, pair_cap: usize, argument_threshold: f32 }, expected: []relations.Proposal },
    }, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed.value.cases.len);
    for (parsed.value.cases) |case| {
        errdefer std.debug.print("relation proposal fixture: {s}\n", .{case.id});
        const result = try relations.generate(a, case.input, case.routes, .{
            .heads_per_relation = case.options.heads_per_relation,
            .tails_per_relation = case.options.tails_per_relation,
            .pair_cap = case.options.pair_cap,
            .argument_threshold = case.options.argument_threshold,
        });
        defer a.free(result);
        try std.testing.expectEqual(case.expected.len, result.len);
        for (case.expected, result) |e, r| {
            inline for (.{ "batch_index", "relation_index", "head_query", "tail_query", "head_span", "tail_span" }) |field|
                try std.testing.expectEqual(@field(e, field), @field(r, field));
            try std.testing.expectApproxEqAbs(e.head_probability, r.head_probability, 1e-6);
            try std.testing.expectApproxEqAbs(e.tail_probability, r.tail_probability, 1e-6);
        }
    }
}

test "gliner boundary Python parity relation containment repeated mentions and Unicode semantic dedup" {
    const a = std.testing.allocator;
    const relations = @import("../pipelines/gliner_boundary_relations.zig");
    const bytes = try fixtureBytes(a, "relation_dedup.json");
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(struct {
        cases: []struct { id: []const u8, edges: []relations.Edge, expected: []relations.Edge },
    }, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 39), parsed.value.cases.len);
    for (parsed.value.cases) |case| {
        errdefer std.debug.print("relation dedup fixture: {s}\n", .{case.id});
        var result = try relations.deduplicate(a, case.edges, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.expected.len, result.edges.len);
        for (case.expected, result.edges) |e, r| {
            try std.testing.expectEqualDeep(e, r);
        }
    }
}

test "gliner boundary Python parity global record decoding all modes" {
    const a = std.testing.allocator;
    const records = @import("../pipelines/gliner_boundary_records.zig");
    const bytes = try fixtureBytes(a, "records.json");
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(struct {
        cases: []struct {
            id: []const u8,
            group: records.Group,
            expected: []struct { probability: f32, anchor: ?records.Span, fields: []records.Selection },
        },
    }, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 18), parsed.value.cases.len);
    for (parsed.value.cases) |case| {
        errdefer std.debug.print("record decode fixture: {s}\n", .{case.id});
        var result = try records.decode(a, case.group, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.expected.len, result.records.len);
        for (case.expected, result.records) |expected, actual| {
            try std.testing.expectEqual(expected.anchor, actual.anchor);
            try std.testing.expectApproxEqAbs(expected.probability, actual.probability, 1e-6);
            try std.testing.expectEqual(expected.fields.len, actual.fields.len);
            for (expected.fields, actual.fields) |ef, af| {
                try std.testing.expectEqual(ef.query_id, af.query_id);
                try std.testing.expectEqual(ef.values.len, af.values.len);
                for (ef.values, af.values) |ev, av| {
                    try std.testing.expectEqual(ev.span, av.span);
                    try std.testing.expectApproxEqAbs(ev.probability, av.probability, 1e-6);
                }
            }
        }
    }
}

test "gliner boundary Python parity rectangular assignment scipy tie profile" {
    const a = std.testing.allocator;
    const assignment = @import("../pipelines/extraction_assignment.zig");
    const bytes = try fixtureBytes(a, "assignment.json");
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(struct {
        cases: []struct { rows: usize, columns: usize, costs: []f64, pairs: []assignment.Pair },
    }, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 28), parsed.value.cases.len);
    for (parsed.value.cases, 0..) |case, i| {
        errdefer std.debug.print("assignment fixture {d}\n", .{i});
        const result = try assignment.solve(a, case.costs, case.rows, case.columns, .{});
        defer a.free(result);
        try std.testing.expectEqualSlices(assignment.Pair, case.pairs, result);
    }
}

pub fn fixtureBytes(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    for ([_][]const u8{ "", "pkg/inference/", "zig/pkg/inference/" }) |prefix| {
        const path = try std.fmt.allocPrint(allocator, "{s}testdata/gliner25/{s}", .{ prefix, name });
        defer allocator.free(path);
        return c_file.readFile(allocator, path) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
    }
    return error.FileNotFound;
}

pub const TensorFixture = struct {
    allocator: std.mem.Allocator,
    reader: safetensors.MMapReader,

    pub fn init(a: std.mem.Allocator, name: []const u8) !TensorFixture {
        const bytes = try fixtureBytes(a, name);
        errdefer a.free(bytes);
        return .{ .allocator = a, .reader = try safetensors.MMapReader.fromBytes(a, bytes) };
    }

    pub fn deinit(self: *TensorFixture) void {
        self.reader.deinit();
    }

    pub fn tensor(self: *const TensorFixture, name: []const u8) !tensor_mod.Tensor {
        const meta = self.reader.header.tensors.get(name) orelse return error.TensorNotFound;
        const start = try std.math.add(u64, self.reader.data_offset, meta.data_start);
        const end = try std.math.add(u64, self.reader.data_offset, meta.data_end);
        if (start > end or end > self.reader.file_bytes.len) return error.InvalidFixtureTensor;
        return .{ .allocator = self.allocator, .name = name, .shape = meta.shape, .data = @constCast(self.reader.file_bytes[@intCast(start)..@intCast(end)]), .dtype = meta.dtype, .owns_data = false, .owns_shape = false };
    }

    pub fn floats(self: *const TensorFixture, name: []const u8) ![]const f32 {
        const value = try self.tensor(name);
        if (value.dtype != .f32) return error.InvalidFixtureTensor;
        return value.asFloat32IfAligned() orelse error.InvalidFixtureAlignment;
    }

    pub fn booleans(self: *const TensorFixture, a: std.mem.Allocator, name: []const u8) ![]bool {
        const value = try self.tensor(name);
        // The shared SafeTensors parser normalizes BOOL storage to u8.
        if (value.dtype != .bool_ and value.dtype != .u8) return error.InvalidFixtureTensor;
        const result = try a.alloc(bool, value.data.len);
        errdefer a.free(result);
        for (value.data, result) |byte, *boolean| {
            if (byte > 1) return error.InvalidFixtureTensor;
            boolean.* = byte != 0;
        }
        return result;
    }

    pub fn loadWeights(self: *const TensorFixture) !native.WeightStore {
        // Complete checkpoints get strict inventory/shape/precision validation
        // before any prefix transformation. Tiny task fixtures remain partial.
        if (self.reader.header.tensors.get("encoder.embeddings.word_embeddings.weight")) |embedding| {
            const dimensions = embedding.shape;
            if (dimensions.len != 2) return error.InvalidGlinerBoundaryTensorShape;
            const backbone: boundary_model.Backbone = if (dimensions[0] == 250112) .multi else if (dimensions[1] == 384) .small else .base;
            const artifact = @import("../models/gliner_boundary_artifact.zig");
            const descriptors = try self.allocator.alloc(@import("../models/tensor_access.zig").Descriptor, self.reader.header.tensors.count());
            defer self.allocator.free(descriptors);
            var tensor_iter = self.reader.header.tensors.iterator();
            var index: usize = 0;
            while (tensor_iter.next()) |entry| {
                const value = try self.tensor(entry.key_ptr.*);
                descriptors[index] = .{ .name = entry.key_ptr.*, .shape = value.shape, .encoding = .{ .dense = value.dtype }, .byte_len = value.data.len, .quantized = false };
                index += 1;
            }
            _ = try artifact.validate(self.allocator, backbone, .fp32, descriptors, null);
        }
        var store = native.WeightStore{ .allocator = self.allocator, .resident_weights = .empty, .lazy_weights = .empty };
        errdefer store.deinitOwned();
        var iter = self.reader.header.tensors.iterator();
        while (iter.next()) |entry| {
            const original = entry.key_ptr.*;
            const is_encoder = std.mem.startsWith(u8, original, "encoder.embeddings.") or std.mem.startsWith(u8, original, "encoder.encoder.");
            if (!std.mem.startsWith(u8, original, "boundary_head.") and
                !std.mem.startsWith(u8, original, "classifier.") and
                !std.mem.startsWith(u8, original, "record_decoder.") and
                !std.mem.startsWith(u8, original, "relation_scorer.") and !is_encoder) continue;
            const name = try self.allocator.dupe(u8, if (is_encoder) original["encoder.".len..] else original);
            errdefer self.allocator.free(name);
            var value = try self.tensor(original);
            value.name = name;
            try store.resident_weights.put(self.allocator, name, .{ .tensor = value });
        }
        if (store.resident_weights.count() == 0) return error.MissingBoundaryHeadWeights;
        return store;
    }
};

test "gliner boundary Python parity complete FP32 CPU head on ragged batch" {
    const a = std.testing.allocator;
    const config_bytes = try fixtureBytes(a, "models/base/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder_bytes);
    var config = try boundary_model.parseConfig(a, config_bytes, encoder_bytes);
    const capture_bytes = try fixtureBytes(a, "tiny/capture.json");
    defer a.free(capture_bytes);
    const capture = try std.json.parseFromSlice(struct { boundary_head: struct { settings: boundary_model.HeadConfig } }, a, capture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    config.head = capture.value.boundary_head.settings;
    config.encoder.hidden_size = 32;
    config.encoder.num_attention_heads = 4;
    config.encoder.intermediate_size = 64;
    var weights = try TensorFixture.init(a, "tiny/boundary_weights.safetensors");
    defer weights.deinit();
    var reference = try TensorFixture.init(a, "tiny/boundary_tensors.safetensors");
    defer reference.deinit();
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const query_mask = try reference.booleans(a, "input.query_mask");
    defer a.free(query_mask);
    var result = try head.forwardNative(&cb, a, &config, .{
        .batch = 2,
        .text_length = 7,
        .queries = 3,
        .text_states = try reference.floats("input.token_states"),
        .query_states = try reference.floats("input.query_states"),
        .text_lengths = &.{ 7, 3 },
        .query_mask = query_mask,
    }, .{});
    defer result.deinit();
    try expectFloats(try reference.floats("boundary.states"), result.boundary_states, 2e-5, 2e-5);
    try expectFloats(try reference.floats("marginal.start"), result.marginals.start_logits, 2e-5, 2e-5);
    try expectFloats(try reference.floats("marginal.end"), result.marginals.end_logits, 2e-5, 2e-5);
    try expectFloats(try reference.floats("marginal.inside"), result.marginals.inside_logits, 2e-5, 2e-5);
    try expectFloats(try reference.floats("marginal.inside_prefix"), result.marginals.inside_prefix, 3e-5, 2e-5);
    try expectFloats(try reference.floats("marginal.inside_mean"), result.marginals.inside_mean, 2e-5, 2e-5);
    const candidate_indices = try reference.tensor("candidate.indices");
    try std.testing.expectEqual(tensor_mod.DType.i64, candidate_indices.dtype);
    const expected_valid = try reference.booleans(a, "candidate.mask");
    defer a.free(expected_valid);
    const capacity = result.pool.capacity;
    try std.testing.expectEqual(@as(usize, 2 * 3 * capacity), expected_valid.len);
    for (0..2) |b| for (0..3) |q| {
        for (0..capacity) |c| {
            const i = (b * 3 + q) * capacity + c;
            try std.testing.expectEqual(expected_valid[i], result.pool.valid[b * capacity + c]);
            for (0..2) |endpoint| {
                const offset = (i * 2 + endpoint) * 8;
                const expected = std.mem.readInt(i64, candidate_indices.data[offset..][0..8], .little);
                const actual = if (endpoint == 0) result.pool.indices[b * capacity + c].start else result.pool.indices[b * capacity + c].end;
                try std.testing.expectEqual(@as(usize, @intCast(expected)), actual);
            }
        }
    };
    try expectFloats(try reference.floats("candidate.logits"), result.pair_logits, 5e-5, 2e-5);
    try expectFloats(try reference.floats("query.abstention_logits"), result.null_logits.?, 2e-5, 2e-5);
    try expectFloats(try reference.floats("query.count_log_rates"), result.count_log_rates.?, 2e-5, 2e-5);
    const explicit_tensor = try reference.tensor("input.explicit_spans");
    const explicit = try a.alloc(head.SignedSpan, 2 * 3 * 3);
    defer a.free(explicit);
    for (explicit, 0..) |*span, i| span.* = .{
        .start = std.mem.readInt(i64, explicit_tensor.data[i * 16 ..][0..8], .little),
        .end = std.mem.readInt(i64, explicit_tensor.data[i * 16 + 8 ..][0..8], .little),
    };
    var explicit_result = try head.scoreExplicitSpansNative(&cb, a, &config, .{
        .batch = 2,
        .text_length = 7,
        .queries = 3,
        .text_states = try reference.floats("input.token_states"),
        .query_states = try reference.floats("input.query_states"),
        .text_lengths = &.{ 7, 3 },
        .query_mask = query_mask,
    }, .{ .capacity = 3, .indices = explicit }, .{});
    defer explicit_result.deinit();
    try expectFloats(try reference.floats("explicit.logits"), explicit_result.logits, 5e-5, 2e-5);
}

test "gliner boundary Python parity pinned small checkpoint CPU head" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const path = try std.fmt.allocPrint(a, "{s}/model.safetensors", .{directory});
    defer a.free(path);
    var weights = TensorFixture{ .allocator = a, .reader = try safetensors.MMapReader.openFileAbsolute(a, path) };
    defer weights.deinit();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(weights.reader.file_bytes, &digest, .{});
    const actual_hash = std.fmt.bytesToHex(digest, .lower);
    try std.testing.expectEqualStrings("4ee982787ace270d4bf15dbcb28ced38e0aa201372347114ceedd6336055de2b", &actual_hash);
    const config_bytes = try fixtureBytes(a, "models/small/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtureBytes(a, "models/small/encoder_config.json");
    defer a.free(encoder_bytes);
    const config = try boundary_model.parseConfig(a, config_bytes, encoder_bytes);
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    for ([_][]const u8{ "mixed_tasks", "entity_attributes", "enum_field", "legacy_structure", "unicode_offsets" }) |name| {
        errdefer std.debug.print("small checkpoint head fixture: {s}\n", .{name});
        const fixture_name = try std.fmt.allocPrint(a, "small_reference/{s}.safetensors", .{name});
        defer a.free(fixture_name);
        var reference = try TensorFixture.init(a, fixture_name);
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
        var result = try head.forwardNative(&cb, a, &config, .{
            .batch = batch,
            .text_length = length,
            .queries = queries,
            .text_states = try reference.floats("encoded.text"),
            .query_states = try reference.floats("encoded.query"),
            .text_lengths = lengths,
            .query_mask = query_mask,
        }, .{});
        defer result.deinit();
        try expectFloats(try reference.floats("marginal.start"), result.marginals.start_logits, 1e-4, 3e-5);
        try expectFloats(try reference.floats("marginal.end"), result.marginals.end_logits, 1e-4, 3e-5);
        try expectFloats(try reference.floats("marginal.inside"), result.marginals.inside_logits, 1e-4, 3e-5);
        const indices = try reference.tensor("candidate.indices");
        const valid = try reference.booleans(a, "candidate.mask");
        defer a.free(valid);
        const capacity = result.pool.capacity;
        try std.testing.expectEqual(batch * queries * capacity, valid.len);
        for (0..batch) |b| for (0..queries) |q| {
            for (0..capacity) |c| {
                const i = (b * queries + q) * capacity + c;
                const span = result.pool.indices[b * capacity + c];
                try std.testing.expectEqual(valid[i], result.pool.valid[b * capacity + c]);
                try std.testing.expectEqual(@as(i64, @intCast(span.start)), std.mem.readInt(i64, indices.data[i * 16 ..][0..8], .little));
                try std.testing.expectEqual(@as(i64, @intCast(span.end)), std.mem.readInt(i64, indices.data[i * 16 + 8 ..][0..8], .little));
            }
        };
        try expectFloats(try reference.floats("candidate.logits"), result.pair_logits, 3e-4, 3e-5);
    }
}

const PoolCase = struct {
    id: []const u8,
    batch: usize,
    boundaries: usize,
    queries: usize,
    dim: usize,
    lengths: []usize,
    query_mask: []bool,
    start_logits: []f32,
    end_logits: []f32,
    projected_starts: []f32,
    projected_ends: []f32,
    config: primitive.PoolConfig,
    expected: struct { indices: []usize, valid: []bool, proposal_logits: []f32, compat_logits: []f32 },
};
const InsideCase = struct {
    id: []const u8,
    batch: usize,
    queries: usize,
    length: usize,
    query_mask: []bool,
    text_mask: []bool,
    inside_logits: []f32,
    grad_prefix: []f32,
    expected: struct { mean: []f32, prefix: []f32, input_gradient: []f32 },
};
const OverlapCase = struct {
    id: []const u8,
    canonical_policy: enum { allow, nested, disallow, longest },
    items: []struct { id: usize, start: usize, end: usize, score: f32 },
    expected_ids: []usize,
};
const Primitives = struct {
    format_version: u32,
    provenance: struct { commit: []const u8 },
    pool_cases: []PoolCase,
    inside_cases: []InsideCase,
    overlap_cases: []OverlapCase,
};

fn loadPrimitives(a: std.mem.Allocator) !std.json.Parsed(Primitives) {
    const bytes = try fixtureBytes(a, "primitives.json");
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(Primitives, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer parsed.deinit();
    try std.testing.expectEqual(@as(u32, 1), parsed.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", parsed.value.provenance.commit);
    return parsed;
}

pub fn expectFloats(expected: []const f32, actual: []const f32, absolute: f32, relative: f32) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual, 0..) |e, a, i| {
        if (!std.math.isFinite(a) or @abs(e - a) > absolute + relative * @abs(e)) {
            std.debug.print("boundary parity element {d}: expected {d}, actual {d}\n", .{ i, e, a });
            return error.TestExpectedApproxEqAbs;
        }
    }
}

test "gliner boundary Python parity shared pool quotas masks sentinel and stable ordering" {
    const a = std.testing.allocator;
    const fixtures = try loadPrimitives(a);
    defer fixtures.deinit();
    try std.testing.expect(fixtures.value.pool_cases.len >= 4);
    for (fixtures.value.pool_cases) |case| {
        errdefer std.debug.print("pool fixture: {s}\n", .{case.id});
        var pool = try primitive.buildSharedPool(a, .{
            .batch = case.batch,
            .boundaries = case.boundaries,
            .queries = case.queries,
            .dim = case.dim,
            .lengths = case.lengths,
            .query_mask = case.query_mask,
            .start_logits = case.start_logits,
            .end_logits = case.end_logits,
            .projected_starts = case.projected_starts,
            .projected_ends = case.projected_ends,
        }, case.config);
        defer pool.deinit();
        try std.testing.expectEqual(pool.indices.len * 2, case.expected.indices.len);
        for (pool.indices, 0..) |span, i| {
            try std.testing.expectEqual(case.expected.indices[2 * i], span.start);
            try std.testing.expectEqual(case.expected.indices[2 * i + 1], span.end);
        }
        try std.testing.expectEqualSlices(bool, case.expected.valid, pool.valid);
        try expectFloats(case.expected.proposal_logits, pool.proposal_logits, 1e-5, 0);
        try expectFloats(case.expected.compat_logits, pool.compat_logits, 1e-6, 0);
    }
}

test "gliner boundary Python parity centered inside prefixes and detached mean gradients" {
    const a = std.testing.allocator;
    const fixtures = try loadPrimitives(a);
    defer fixtures.deinit();
    try std.testing.expect(fixtures.value.inside_cases.len > 0);
    for (fixtures.value.inside_cases) |case| {
        errdefer std.debug.print("inside fixture: {s}\n", .{case.id});
        const rows = case.batch * case.queries;
        const mask = try a.alloc(bool, rows * case.length);
        defer a.free(mask);
        for (0..case.batch) |b| for (0..case.queries) |q| {
            for (0..case.length) |w| mask[(b * case.queries + q) * case.length + w] = case.query_mask[b * case.queries + q] and case.text_mask[b * case.length + w];
        };
        const prefix = try a.alloc(f32, rows * (case.length + 1));
        defer a.free(prefix);
        const means = try a.alloc(f32, rows);
        defer a.free(means);
        try primitive.centeredInsidePrefix(case.inside_logits, mask, rows, case.length, prefix, means);
        try expectFloats(case.expected.prefix, prefix, 1e-5, 0);
        try expectFloats(case.expected.mean, means, 1e-5, 0);
        const gradient = try a.alloc(f32, rows * case.length);
        defer a.free(gradient);
        try primitive.centeredInsidePrefixBackward(case.grad_prefix, mask, rows, case.length, gradient);
        try expectFloats(case.expected.input_gradient, gradient, 1e-6, 0);
    }
}

test "gliner boundary Python parity all overlap policies and global scheduling ties" {
    const a = std.testing.allocator;
    const fixtures = try loadPrimitives(a);
    defer fixtures.deinit();
    try std.testing.expect(fixtures.value.overlap_cases.len >= 24);
    for (fixtures.value.overlap_cases) |case| {
        errdefer std.debug.print("overlap fixture: {s}\n", .{case.id});
        const candidates = try a.alloc(decode.Candidate, case.items.len);
        defer a.free(candidates);
        for (case.items, candidates) |item, *candidate| candidate.* = .{ .start = item.start, .end = item.end, .probability = item.score, .source_index = item.id };
        const policy: decode.OverlapPolicy = switch (case.canonical_policy) {
            .allow => .allow,
            .nested => .nested,
            .disallow => .flat,
            .longest => .longest,
        };
        const result = try decode.resolveOverlaps(a, candidates, policy, .{});
        defer a.free(result);
        try std.testing.expectEqual(case.expected_ids.len, result.len);
        for (case.expected_ids, result) |expected, actual| try std.testing.expectEqual(expected, actual.source_index);
    }
}
