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

    pub fn deinit(self: *Prepared, a: Allocator) void {
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

    pub fn deinit(self: *Reference) void {
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

/// Both encoder attention profiles: materialized scores, and the fused trunk
/// attention (`replay_tiled_v1` for ModernBERT).
fn parity(a: Allocator, cb: *const ops.ComputeBackend) !void {
    for ([_]encoder_graph.AttentionProfile{ .materialized_v1, .replay_tiled_v1 }) |profile| try parityProfile(a, cb, profile);
}

fn parityProfile(a: Allocator, cb: *const ops.ComputeBackend, profile: encoder_graph.AttentionProfile) !void {
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
    var built = try encoder_graph.buildWithProfile(&builder, &config, layout, .eval, profile, .{});
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
    // Embeddings and final norm, five tensors per layer plus attn_norm after
    // layer 0, and a neck's weight and bias.
    const layers: usize = config.encoder.num_hidden_layers;
    try std.testing.expectEqual(6 * layers + 2 + @as(usize, if (config.neck == .linear) 2 else 0), wrt.items.len);
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
    var compared: usize = 0;
    for (wrt.items, backward.outputs) |id, output| {
        const name = graph.parameterName(graph.node(id));
        errdefer std.debug.print("ModernBERT boundary encoder parity: gradient {s}\n", .{name});
        var key: [256]u8 = undefined;
        // A real-checkpoint capture keeps a representative subset.
        const expected = reference.tensors.floats(try std.fmt.bufPrint(&key, "gradient.{s}", .{name})) catch |err| switch (err) {
            error.TensorNotFound => if (layers > 3) continue else return err,
            else => return err,
        };
        compared += 1;
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        var largest: f32 = 0;
        for (expected) |value| largest = @max(largest, @abs(value));
        // The Laya trunk's gradient bound: absolute 5e-5 plus 0.2% of the tensor's scale.
        try fixtures.expectFloats(expected, actual, 5e-5 + 0.002 * largest, 0);
        for (expected, actual) |want, got| worst = @max(worst, @abs(want - got));
    }
    try std.testing.expect(compared >= 3);
    std.debug.print("ModernBERT boundary encoder ({s}, {s}): {d} gradient tensors, max absolute error={d}\n", .{ @tagName(cb.kind()), @tagName(profile), compared, worst });
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
    var temporary = platform.testing.tmpDir(.{});
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

// A real student checkpoint (scripts/antenna/init_student.py): the training
// source accepts it, and the native processor reproduces upstream's ids and
// routes with the pretrained tokenizer (NFC, full byte-level BPE vocabulary).
test "GLiNER2.5 ModernBERT student checkpoint loads and tokenizes as upstream" {
    const a = std.testing.allocator;
    const directory = platform.env.getenv("ANTFLY_GLINER25_MODERNBERT_STUDENT") orelse return error.SkipZigTest;
    const source = try sources.Source.open(a, compat.io(), directory, .{}, null);
    defer source.deinit();
    try std.testing.expectEqual(@import("../../models/gliner_boundary.zig").Backbone.modern_bert, source.config.backbone);
    const pin_path = try std.fs.path.join(a, &.{ directory, "processor.json" });
    defer a.free(pin_path);
    const pin_bytes = try @import("../../util/c_file.zig").readFile(a, pin_path);
    defer a.free(pin_bytes);
    var pin = try loadPin(a, pin_bytes);
    defer pin.deinit();
    var prepared = try Prepared.init(a, source.tokenizer(), &pin.value);
    defer prepared.deinit(a);
    try expectProcessorPin(&pin.value, &prepared.batch);
    std.debug.print("ModernBERT student: {d} tensors, vocabulary {d}, hidden {d}, {d} layers\n", .{ source.parameter_count, source.config.encoder.vocab_size, source.config.encoder.hidden_size, source.config.encoder.num_hidden_layers });
}

/// A distillation "teacher" that returns the PyTorch reference's own final
/// states at the student's routes, so the distillation term measures how far
/// the trainer's encoder output is from PyTorch.
const ReferenceTeacher = struct {
    reference: *Reference,
    pin: *const Pin,

    const Owned = struct {
        allocator: Allocator,
        values: [4][]f32,
        fn release(raw: ?*anyopaque) void {
            const self: *Owned = @ptrCast(@alignCast(raw.?));
            for (self.values) |values| self.allocator.free(values);
            self.allocator.destroy(self);
        }
    };

    fn teacher(self: *ReferenceTeacher) @import("boundary_distillation.zig").Teacher {
        return .{ .ptr = self, .identity = @splat(7), .encode = encode };
    }

    fn encode(raw: *anyopaque, a: Allocator, items: []const processor.Item, student: *const processor.PreparedBatch, _: ?@import("../../execution_control.zig").InferenceExecutionControl) !@import("boundary_distillation.zig").TeacherStates {
        const self: *ReferenceTeacher = @ptrCast(@alignCast(raw));
        const hidden_tensor = try self.reference.tensors.tensor("encoded.hidden");
        const hidden = try self.reference.tensors.floats("encoded.hidden");
        const sequence: usize = @intCast(hidden_tensor.shape[1]);
        const h: usize = @intCast(hidden_tensor.shape[2]);
        const rows = try a.alloc(usize, items.len);
        defer a.free(rows);
        for (items, rows) |item, *row| row.* = for (self.pin.cases, 0..) |case, index| {
            if (std.mem.eql(u8, case.text, item.text)) break index;
        } else return error.MissingReferenceCase;
        const owned = try a.create(Owned);
        owned.allocator = a;
        const routes = [_]struct { []const i64, []const bool }{
            .{ student.text_word_indices, student.text_word_mask },
            .{ student.query_marker_indices, student.query_marker_mask },
            .{ student.cls_marker_indices, student.cls_marker_mask },
            .{ student.parent_marker_indices, student.parent_marker_mask },
        };
        for (routes, &owned.values) |route, *values| {
            values.* = try a.alloc(f32, route[0].len * h);
            @memset(values.*, 0);
            const width = route[0].len / items.len;
            for (route[0], route[1], 0..) |position, valid, index| {
                if (!valid) continue;
                const row = rows[index / width];
                const at: usize = @intCast(position);
                if (at >= sequence) return error.InvalidReferenceRoute;
                @memcpy(values.*[index * h ..][0..h], hidden[(row * sequence + at) * h ..][0..h]);
            }
        }
        return .{ .text = owned.values[0], .queries = owned.values[1], .classifications = owned.values[2], .parents = owned.values[3], .context = owned, .release = Owned.release };
    }
};

fn distillationToReference(a: Allocator, reference: *Reference, execution: @import("../seeded_gradient_trainer.zig").Execution) !f32 {
    const trainer = @import("boundary_native_trainer.zig");
    const data = @import("boundary_dataset.zig");
    var bytes = std.Io.Writer.Allocating.init(a);
    defer bytes.deinit();
    for (reference.pin.value.cases, 0..) |case, i| {
        try bytes.writer.print("{{\"version\":1,\"id\":\"{d}\",\"text\":", .{i});
        try std.json.Stringify.value(case.text, .{}, &bytes.writer);
        try bytes.writer.print(",\"schema\":{s}}}\n", .{case.native_schema});
    }
    var samples = try data.Dataset.fromBytes(a, bytes.written(), .{ .limits = .{ .max_host_bytes = 16 * 1024 * 1024 } }, null, null);
    defer samples.deinit();
    var teacher = ReferenceTeacher{ .reference = reference, .pin = &reference.pin.value };
    const source = reference.source;
    const gib: usize = 1024 * 1024 * 1024;
    // The ModernBERT-base job limits (scripts/antenna/README.md).
    const limits_mod = @import("boundary_training_limits.zig");
    const parsed = try std.json.parseFromSlice(limits_mod.Config, a,
        \\{"encoder":{"max_forward_tensor_bytes":17179869184},"differentiation":{"max_tape_bytes":6442450944,"max_cotangent_bytes":1073741824},"resident":{"program":{"max_working_bytes":6442450944,"max_capture_bytes":6442450944}}}
    , .{});
    defer parsed.deinit();
    const limits = try limits_mod.apply(parsed.value, .{ .max_host_bytes = 8 * gib, .max_backend_bytes = 12 * gib, .max_backend_host_bytes = 512 * 1024 * 1024, .max_combined_bytes = 24 * gib, .optimizer = .{ .max_state_bytes = 8 * gib, .max_transaction_bytes = 8 * gib } });
    const options = trainer.Options{ .execution = execution, .run = .{ .mode = .full, .epochs = 1, .batch_size = @intCast(reference.pin.value.cases.len), .accumulation = 1, .seed = 2509, .encoder_lr = 1e-12, .task_lr = 1e-12 }, .source_reserved_bytes = source.reserved_source_bytes, .distillation = .{ .teacher = teacher.teacher() }, .limits = limits };
    var owner = try trainer.Trainer.init(a, &source.store, source.tokenizer(), source.identity, source.config, &samples, source.parameters[0..source.parameter_count], options, null);
    defer owner.deinit();
    while (try owner.next(null)) |report| if (report.terms) |terms| return terms.distillation;
    return error.TestUnexpectedResult;
}

test "GLiNER2.5 ModernBERT training program reproduces the reference encoder states" {
    const a = std.testing.allocator;
    var reference = try Reference.open(a);
    defer reference.deinit();
    const cpu = try distillationToReference(a, &reference, .native);
    std.debug.print("ModernBERT trainer encoder vs PyTorch (z-space MSE): native={d}\n", .{cpu});
    try std.testing.expect(cpu < 1e-3);
    if (comptime !build_options.enable_metal) return;
    if (!@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return;
    const metal = try distillationToReference(a, &reference, .resident_metal);
    std.debug.print("ModernBERT trainer encoder vs PyTorch (z-space MSE): resident Metal={d}\n", .{metal});
    try std.testing.expect(metal < 1e-3);
}
