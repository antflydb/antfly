// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A ModernBERT GLiNER2.5 boundary encoder against the pinned upstream
//! (scripts/gliner25/modernbert_reference.py, models/antenna/ANTENNA.md).
//!
//! The processor test uses the checked-in tokenizer and upstream token ids
//! (testdata/gliner25/modernbert_tokenizer). The parity and training tests
//! read a generated reference directory from
//! ANTFLY_GLINER25_MODERNBERT_REFERENCE; ANTFLY_GLINER25_MODERNBERT_BACKEND
//! selects `native` (default) or `metal` for the parity test.
const std = @import("std");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const ml = @import("ml").graph;
const encoder_graph = @import("boundary_encoder_graph.zig");
const sources = @import("boundary_training_source.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const schema_mod = @import("../../pipelines/extraction_schema.zig");
const fixtures = @import("../../architectures/gliner/boundary_parity_test.zig");
const safetensors = @import("../../models/safetensors.zig");
const native = @import("../../ops/native_compute.zig");
const ops = @import("../../ops/ops.zig");
const interpreter = @import("../../graph/interpreter.zig");
const compat = @import("../../io/compat.zig");
const HfTokenizer = @import("inference_hf_tokenizer").HfTokenizer;
const Allocator = std.mem.Allocator;

const Pin = struct {
    format_version: u32,
    upstream_commit: []const u8,
    generator_sha256: []const u8,
    cases: []const struct { id: []const u8, text: []const u8, native_schema: []const u8 },
    tokenization: std.json.Value,
    input_ids: []const []const i64,
    attention_mask: []const []const i64,
    routes: struct {
        text: Route,
        query: Route,
        cls: Route,
    },
    const Route = struct { indices: []const []const i64, mask: []const []const bool };
};

const Prepared = struct {
    schemas: []schema_mod.CompiledSchema,
    batch: processor.PreparedBatch,

    fn init(a: Allocator, tokenizer: @import("inference_tokenizer").Tokenizer, pin: *const Pin) !Prepared {
        const schemas = try a.alloc(schema_mod.CompiledSchema, pin.cases.len);
        var compiled: usize = 0;
        errdefer {
            for (schemas[0..compiled]) |*schema| schema.deinit();
            a.free(schemas);
        }
        for (pin.cases, schemas) |case, *schema| {
            schema.* = try schema_mod.compile(a, case.native_schema, .{});
            compiled += 1;
        }
        const requests = try a.alloc(processor.Item, pin.cases.len);
        defer a.free(requests);
        for (pin.cases, schemas, requests) |case, *schema, *request| request.* = .{ .text = case.text, .schema = schema };
        return .{ .schemas = schemas, .batch = try processor.prepare(a, tokenizer, requests, .{}) };
    }

    fn deinit(self: *Prepared, a: Allocator) void {
        self.batch.deinit();
        for (self.schemas) |*schema| schema.deinit();
        a.free(self.schemas);
    }
};

fn loadPin(a: Allocator, bytes: []const u8) !std.json.Parsed(Pin) {
    const pin = try std.json.parseFromSlice(Pin, a, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
    errdefer pin.deinit();
    try std.testing.expectEqual(@as(u32, 1), pin.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", pin.value.upstream_commit);
    return pin;
}

fn expectRoute(expected: Pin.Route, width: usize, indices: []const i64, mask: []const bool) !void {
    try std.testing.expectEqual(expected.indices.len * width, indices.len);
    for (expected.indices, expected.mask, 0..) |row_indices, row_mask, row| {
        try std.testing.expectEqual(width, row_indices.len);
        for (row_indices, row_mask, 0..) |index, present, column| {
            try std.testing.expectEqual(present, mask[row * width + column]);
            if (present) try std.testing.expectEqual(index, indices[row * width + column]);
        }
    }
}

/// Upstream tokenizes each word and schema fragment alone, so a byte-level
/// BPE sees every word at the start of a text ("apple" -> "a" "pple", never
/// "Ġapple"). The native processor must reproduce those ids and routes.
fn expectProcessorPin(pin: *const Pin, prepared: *const processor.PreparedBatch) !void {
    const rows = pin.input_ids.len;
    try std.testing.expectEqual(rows, prepared.samples.len);
    const sequence = prepared.sequence_length;
    for (pin.input_ids, pin.attention_mask, 0..) |ids, mask, row| {
        try std.testing.expectEqual(sequence, ids.len);
        for (ids, mask, 0..) |id, valid, column| {
            try std.testing.expectEqual(valid, prepared.attention_mask[row * sequence + column]);
            // Both pad with id 0.
            try std.testing.expectEqual(id, prepared.input_ids[row * sequence + column]);
        }
    }
    try expectRoute(pin.routes.text, prepared.word_width, prepared.text_word_indices, prepared.text_word_mask);
    try expectRoute(pin.routes.query, prepared.query_width, prepared.query_marker_indices, prepared.query_marker_mask);
    try expectRoute(pin.routes.cls, prepared.classification_width, prepared.cls_marker_indices, prepared.cls_marker_mask);
}

test "GLiNER2.5 ModernBERT processor tokenizes each word alone as upstream does" {
    const a = std.testing.allocator;
    const pin_bytes = try fixtures.fixtureBytes(a, "modernbert_tokenizer/processor.json");
    defer a.free(pin_bytes);
    var pin = try loadPin(a, pin_bytes);
    defer pin.deinit();
    const tokenizer_bytes = try fixtures.fixtureBytes(a, "modernbert_tokenizer/tokenizer.json");
    defer a.free(tokenizer_bytes);
    const loaded = try HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    const tokenizer = loaded.tokenizer();
    defer tokenizer.deinitTokenizer();
    var prepared = try Prepared.init(a, tokenizer, &pin.value);
    defer prepared.deinit(a);
    try expectProcessorPin(&pin.value, &prepared.batch);
    // The prefix space matters for this tokenizer, so the pin is not vacuous.
    const alone = try tokenizer.encode(a, "apple");
    defer a.free(alone);
    const spaced = try tokenizer.encode(a, " apple");
    defer a.free(spaced);
    try std.testing.expect(!std.mem.eql(i32, alone, spaced));
    try std.testing.expectEqual(@as(usize, 2), alone.len);
}

const Reference = struct {
    root: []const u8,
    source: *sources.Source,
    pin: std.json.Parsed(Pin),
    tensors: fixtures.TensorFixture,

    fn open(a: Allocator) !Reference {
        const root = platform.env.getenv("ANTFLY_GLINER25_MODERNBERT_REFERENCE") orelse return error.SkipZigTest;
        const checkpoint = try std.fs.path.join(a, &.{ root, "checkpoint" });
        defer a.free(checkpoint);
        const source = try sources.Source.open(a, compat.io(), checkpoint, .{}, null);
        errdefer source.deinit();
        const pin_path = try std.fs.path.join(a, &.{ root, "processor.json" });
        defer a.free(pin_path);
        const pin_bytes = try @import("../../util/c_file.zig").readFile(a, pin_path);
        defer a.free(pin_bytes);
        var pin = try loadPin(a, pin_bytes);
        errdefer pin.deinit();
        const tensor_path = try std.fs.path.join(a, &.{ root, "reference.safetensors" });
        defer a.free(tensor_path);
        return .{ .root = root, .source = source, .pin = pin, .tensors = .{ .allocator = a, .reader = try safetensors.MMapReader.openFileAbsolute(a, tensor_path) } };
    }

    fn deinit(self: *Reference) void {
        self.tensors.deinit();
        self.pin.deinit();
        self.source.deinit();
    }
};

fn upload(a: Allocator, cb: *const ops.ComputeBackend, list: *std.ArrayListUnmanaged(interpreter.RuntimeInput), node: ml.NodeId, shape: ml.Shape, values: encoder_graph.Values) !void {
    var dims: [8]i32 = undefined;
    for (shape.dims[0..shape.rank_], dims[0..shape.rank_]) |dim, *out| out.* = @intCast(dim);
    const value = switch (values) {
        .f32 => |data| try cb.fromFloat32Shape(data, dims[0..shape.rank_]),
        .i32 => |data| (try cb.fromInt32Shape(data, dims[0..shape.rank_])) orelse return error.UnsupportedTestBackend,
    };
    list.append(a, .{ .node_id = node, .value = value }) catch |err| {
        cb.free(value);
        return err;
    };
}

fn parity(a: Allocator, cb: *const ops.ComputeBackend) !void {
    var reference = try Reference.open(a);
    defer reference.deinit();
    const config = reference.source.config;
    try std.testing.expectEqual(@import("../../models/gliner_boundary.zig").Backbone.modern_bert, config.backbone);
    var prepared = try Prepared.init(a, reference.source.tokenizer(), &reference.pin.value);
    defer prepared.deinit(a);
    try expectProcessorPin(&reference.pin.value, &prepared.batch);

    const layout = try encoder_graph.layoutFromPrepared(&config, &prepared.batch, .{});
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    var built = try encoder_graph.build(&builder, &config, layout, .eval, .{});
    defer built.deinit();
    const routed = [_]ml.NodeId{ built.nodes.text, built.nodes.queries, built.nodes.classifications };
    const kinds = [_][]const u8{ "text", "query", "cls" };
    var seeds: [routed.len]ml.autodiff.Seed = undefined;
    for (routed, kinds, &seeds) |node, kind, *seed| {
        try std.testing.expect(node != ml.null_node);
        try graph.markOutput(node);
        var name: [64]u8 = undefined;
        seed.* = .{ .output = node, .cotangent = try builder.parameter(try std.fmt.bufPrint(&name, "__test_cotangent_{s}", .{kind}), graph.node(node).output_shape) };
    }
    var wrt: std.ArrayListUnmanaged(ml.NodeId) = .empty;
    defer wrt.deinit(a);
    for (graph.parameters.items) |id| if (!std.mem.startsWith(u8, graph.parameterName(graph.node(id)), "__")) try wrt.append(a, id);
    try std.testing.expectEqual(@as(usize, 20), wrt.items.len);
    var gradients = try ml.autodiff.gradientWithSeeds(a, &graph, &seeds, wrt.items, .{ .require_all_gradients = true });
    defer gradients.deinit();
    gradients.graph.outputs.clearRetainingCapacity();
    for (gradients.param_grads) |id| try gradients.graph.markOutput(id);

    var inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer {
        for (inputs.items) |input| cb.free(input.value);
        inputs.deinit(a);
    }
    var bound = try encoder_graph.bindPrepared(a, &built, &config, &prepared.batch, .{ .seed = 0, .micro_batch = 0 });
    defer bound.deinit();
    for (bound.bindings) |binding| try upload(a, cb, &inputs, binding.node, binding.shape, binding.values);
    for (wrt.items) |id| {
        const name = graph.parameterName(graph.node(id));
        const weight = reference.source.store.resident_weights.get(name) orelse return error.MissingTestWeight;
        try upload(a, cb, &inputs, id, graph.node(id).output_shape, .{ .f32 = weight.tensor.asFloat32() });
    }
    const forward_input_count = inputs.items.len;
    for (seeds, kinds) |seed, kind| {
        var name: [64]u8 = undefined;
        const values = try reference.tensors.floats(try std.fmt.bufPrint(&name, "cotangent.{s}", .{kind}));
        const shape = graph.node(seed.cotangent).output_shape;
        if (cb.kind() == .native) {
            try upload(a, cb, &inputs, seed.cotangent, shape, .{ .f32 = values });
            continue;
        }
        // Metal's gather VJP scatters only device-resident training tensors,
        // and these cotangents feed the routing gathers directly.
        var dims: [8]i32 = undefined;
        for (shape.dims[0..shape.rank_], dims[0..shape.rank_]) |dim, *out| out.* = @intCast(dim);
        const value = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = dims[0..shape.rank_] } }, .{});
        inputs.append(a, .{ .node_id = seed.cotangent, .value = value }) catch |err| {
            cb.free(value);
            return err;
        };
    }

    var forward = try interpreter.execute(a, &graph, cb, .{ .runtime_inputs = inputs.items[0..forward_input_count], .strict_integer_constants = true });
    defer forward.deinit(cb);
    for (forward.outputs, kinds) |output, kind| {
        errdefer std.debug.print("ModernBERT boundary encoder parity: {s} states\n", .{kind});
        var name: [64]u8 = undefined;
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        try fixtures.expectFloats(try reference.tensors.floats(try std.fmt.bufPrint(&name, "encoded.{s}", .{kind})), actual, 2e-5, 1e-4);
    }

    const backward_inputs = try a.alloc(interpreter.RuntimeInput, inputs.items.len);
    defer a.free(backward_inputs);
    for (inputs.items, backward_inputs) |input, *mapped| mapped.* = .{ .node_id = gradients.id_map[input.node_id], .value = input.value };
    var backward = try interpreter.execute(a, &gradients.graph, cb, .{ .runtime_inputs = backward_inputs, .strict_integer_constants = true });
    defer backward.deinit(cb);
    var worst: f32 = 0;
    for (wrt.items, backward.outputs) |id, output| {
        const name = graph.parameterName(graph.node(id));
        errdefer std.debug.print("ModernBERT boundary encoder parity: gradient {s}\n", .{name});
        var key: [256]u8 = undefined;
        const expected = try reference.tensors.floats(try std.fmt.bufPrint(&key, "gradient.{s}", .{name}));
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        var largest: f32 = 0;
        for (expected) |value| largest = @max(largest, @abs(value));
        // The Laya trunk's gradient bound: absolute 5e-5 plus 0.2% of the tensor's scale.
        try fixtures.expectFloats(expected, actual, 5e-5 + 0.002 * largest, 0);
        for (expected, actual) |want, got| worst = @max(worst, @abs(want - got));
    }
    std.debug.print("ModernBERT boundary encoder ({s}): 20 gradient tensors, max absolute error={d}\n", .{ @tagName(cb.kind()), worst });
}

test "GLiNER2.5 ModernBERT encoder states and every encoder gradient match PyTorch" {
    const a = std.testing.allocator;
    const backend = platform.env.getenv("ANTFLY_GLINER25_MODERNBERT_BACKEND") orelse "native";
    if (std.mem.eql(u8, backend, "metal")) {
        if (comptime !build_options.enable_metal) return error.SkipZigTest;
        var device = try @import("../../graph/resident_training_fixture.zig").Device.init(a);
        defer device.deinit();
        const cb = device.backend.computeBackend();
        return parity(a, &cb);
    }
    if (!std.mem.eql(u8, backend, "native")) return error.InvalidTestBackend;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    return parity(a, &cb);
}

test "GLiNER2.5 ModernBERT checkpoint trains full and heads jobs on resident Metal with exact durable resume" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var reference = try Reference.open(a);
    defer reference.deinit();
    const trainer = @import("boundary_native_trainer.zig");
    const observed = @import("boundary_native_trainer_test.zig");
    const data = @import("boundary_dataset.zig");
    const source = reference.source;
    var bytes = std.Io.Writer.Allocating.init(a);
    defer bytes.deinit();
    for (0..4) |i| try bytes.writer.print("{{\"version\":1,\"id\":\"{d}\",\"text\":\"John works at Apple. Alice works at Google.\",\"schema\":{{\"entities\":[\"person\",\"organization\"],\"classifications\":[{{\"name\":\"sentiment\",\"labels\":[\"positive\",\"negative\"]}}]}},\"entities\":[{{\"id\":\"john\",\"type\":\"person\",\"span\":{{\"start\":0,\"end\":4}}}},{{\"id\":\"apple\",\"type\":\"organization\",\"span\":{{\"start\":14,\"end\":19}}}}],\"classifications\":[{{\"task\":\"sentiment\",\"labels\":[\"positive\"]}}]}}\n", .{i});
    var samples = try data.Dataset.fromBytes(a, bytes.written(), .{ .limits = .{ .max_host_bytes = 16 * 1024 * 1024 } }, null, null);
    defer samples.deinit();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/modernbert.safetensors", .{temporary.sub_path});
    defer a.free(path);
    const parameters = source.parameters[0..source.parameter_count];
    for ([_]@import("boundary_run.zig").Mode{ .full, .heads }) |mode| {
        errdefer std.debug.print("ModernBERT checkpoint job: {s}\n", .{@tagName(mode)});
        const options = trainer.Options{ .execution = .resident_metal, .run = .{ .mode = mode, .epochs = 2, .batch_size = 2, .accumulation = 1, .seed = 2509, .encoder_lr = 0.001, .task_lr = 0.002 }, .source_reserved_bytes = source.reserved_source_bytes, .limits = .{ .max_host_bytes = 512 * 1024 * 1024, .max_backend_bytes = 512 * 1024 * 1024, .max_combined_bytes = 2048 * 1024 * 1024 } };
        var expected = try trainer.Trainer.init(a, &source.store, source.tokenizer(), source.identity, source.config, &samples, parameters, options, null);
        defer expected.deinit();
        var steps: usize = 0;
        var first_loss: ?f32 = null;
        while (try observed.nextObserved(expected)) |report| {
            if (report.terms) |terms| {
                try std.testing.expect(std.math.isFinite(terms.total) and terms.total > 0);
                if (first_loss == null) first_loss = terms.total;
            }
            steps += 1;
        }
        try std.testing.expect(steps >= 4);
        // Interrupt after one step, checkpoint, resume in a fresh owner, finish.
        var actual = try trainer.Trainer.init(a, &source.store, source.tokenizer(), source.identity, source.config, &samples, parameters, options, null);
        _ = (try observed.nextObserved(actual)) orelse return error.TestUnexpectedResult;
        try actual.optimizer.ensureHostState(null);
        const checkpoint_state = try actual.optimizer.stateFingerprint(actual.fingerprint, null);
        try actual.save(path, null);
        actual.deinit();
        var resumed = try trainer.Trainer.init(a, &source.store, source.tokenizer(), source.identity, source.config, &samples, parameters, options, null);
        defer resumed.deinit();
        _ = try resumed.restorePinned(path, checkpoint_state, null);
        while (try observed.nextObserved(resumed)) |_| {}
        try observed.expectSameState(expected, resumed);
        std.debug.print("ModernBERT checkpoint {s} job on resident Metal: {d} steps, first loss={d}\n", .{ @tagName(mode), steps, first_loss.? });
    }
}
