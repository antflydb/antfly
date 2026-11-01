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

//! One deterministic checkpoint, real format parsers, one preparation contract.
const std = @import("std");
const ml = @import("ml");
const onnx = @import("onnx_graph");
const Tensor = @import("../../backends/tensor.zig").Tensor;
const Context = @import("../../backends/compute_context.zig").BackendContext;
const Backend = @import("../../backends/backends.zig").BackendType;
const safetensors = @import("../../models/safetensors.zig");
const store_mod = @import("../../models/tensor_store.zig");
const gguf = @import("../../gguf/root.zig");
const writer = @import("../../gguf/writer.zig");
const executable = @import("../executable.zig");
const Program = @import("../program.zig").ModelProgram;
const Initializer = onnx.ParameterInitializer;
const a = std.testing.allocator;

fn weightValues(arena: std.mem.Allocator, graph: *const ml.graph.Graph) ![]Initializer {
    var result: std.ArrayListUnmanaged(Initializer) = .empty;
    for (graph.parameters.items) |id| {
        const node = graph.node(id);
        const name = graph.parameterName(node);
        if (node.output_shape.dtype != .f32 or std.mem.eql(u8, name, "mask") or std.mem.eql(u8, name, "x") or std.mem.startsWith(u8, name, "state.")) continue;
        const values = try arena.alloc(f32, @intCast(node.output_shape.numElements().?));
        for (values, 0..) |*value, i| value.* = if (std.mem.endsWith(u8, name, "LayerNorm.weight")) 1 else if (std.mem.endsWith(u8, name, ".bias")) 0 else @as(f32, @floatFromInt(@as(i32, @intCast(i % 19)) - 9)) / 100;
        try result.append(arena, .{ .name = name, .shape = node.output_shape, .data = .{ .f32 = values } });
    }
    return result.toOwnedSlice(arena);
}

fn safetensorsBytes(arena: std.mem.Allocator, weights: []const Initializer) ![]u8 {
    var header: std.ArrayListUnmanaged(u8) = .empty;
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    try header.append(arena, '{');
    for (weights, 0..) |weight, index| {
        if (index != 0) try header.append(arena, ',');
        const dims = try std.json.Stringify.valueAlloc(arena, weight.shape.dims[0..weight.shape.rank()], .{});
        const bytes = std.mem.sliceAsBytes(weight.data.f32);
        const item = try std.fmt.allocPrint(arena, "\"{s}\":{{\"dtype\":\"F32\",\"shape\":{s},\"data_offsets\":[{d},{d}]}}", .{ weight.name, dims, raw.items.len, raw.items.len + bytes.len });
        try header.appendSlice(arena, item);
        try raw.appendSlice(arena, bytes);
    }
    try header.append(arena, '}');
    while (header.items.len % 8 != 0) try header.append(arena, ' ');
    const result = try arena.alloc(u8, 8 + header.items.len + raw.items.len);
    std.mem.writeInt(u64, result[0..8], header.items.len, .little);
    @memcpy(result[8..][0..header.items.len], header.items);
    @memcpy(result[8 + header.items.len ..], raw.items);
    return result;
}

fn ggufBytes(arena: std.mem.Allocator, weights: []const Initializer, quantized: bool, architecture: []const u8) ![]u8 {
    const specs = try arena.alloc(writer.TensorSpec, weights.len);
    const payloads = try arena.alloc([]const u8, weights.len);
    for (weights, specs, payloads) |weight, *spec, *payload| {
        const dims = try arena.alloc(u64, weight.shape.rank());
        for (dims, 0..) |*dim, i| dim.* = @intCast(weight.shape.dims[dims.len - i - 1]);
        const q8 = quantized and dims.len == 2;
        spec.* = .{ .name = weight.name, .dimensions = dims, .tensor_type = .{ .known = if (q8) .Q8_0 else .F32 } };
        if (q8) {
            const values = weight.data.f32;
            const data = try arena.alloc(u8, values.len / 32 * 34);
            for (0..values.len / 32) |block| {
                // Deliberately lossy, independently decoded by the reference.
                std.mem.writeInt(u16, data[block * 34 ..][0..2], @bitCast(@as(f16, 1.0 / 1024.0)), .little);
                for (0..32) |i| data[block * 34 + 2 + i] = @bitCast(@as(i8, @intFromFloat(@round(values[block * 32 + i] * 1024))));
            }
            payload.* = data;
        } else payload.* = std.mem.sliceAsBytes(weight.data.f32);
    }
    var layout = try writer.buildLayout(arena, &.{.{ .key = "general.architecture", .value = .{ .string = architecture } }}, specs);
    defer layout.deinit(arena);
    const start = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    const last = payloads.len - 1;
    const result = try arena.alloc(u8, start + @as(usize, @intCast(layout.offsets[last])) + payloads[last].len);
    @memset(result, 0);
    @memcpy(result[0..layout.header_bytes.len], layout.header_bytes);
    for (payloads, layout.offsets) |payload, offset| @memcpy(result[start + @as(usize, @intCast(offset)) ..][0..payload.len], payload);
    return result;
}

fn inputTensor(allocator: std.mem.Allocator, name: []const u8) !Tensor {
    if (std.mem.eql(u8, name, "mask")) {
        var values: [16]f32 = undefined;
        for (&values, 0..) |*value, i| value.* = if (i % 4 == 3) -10000 else 0;
        return Tensor.initFloat32(allocator, name, &.{ 1, 4, 4 }, &values);
    }
    return Tensor.initInt64(allocator, name, &.{4}, if (std.mem.eql(u8, name, "types")) &.{ 0, 0, 0, 0 } else &.{ 0, 1, 2, 3 });
}

fn prepareArtifact(graph: *const ml.graph.Graph, weights: []const Initializer, reader: ?*safetensors.MMapReader, store: ?store_mod.TensorStore, kind: Backend, quant_reference: bool) ![]f32 {
    var context = try Context.init(a, kind, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var inputs: std.ArrayListUnmanaged(@import("../../graph/interpreter.zig").RuntimeInput) = .empty;
    defer {
        for (inputs.items) |input| cb.free(input.value);
        inputs.deinit(a);
    }
    for (graph.parameters.items) |id| {
        const node = graph.node(id);
        const name = graph.parameterName(node);
        var tensor: Tensor = undefined;
        if (reader) |source| {
            tensor = source.readTensor(name) catch |err| switch (err) {
                error.TensorNotFound => try inputTensor(a, name),
                else => return err,
            };
        } else if (store) |source| {
            var ref = source.describeTensor(a, name) catch |err| switch (err) {
                error.TensorNotFound => {
                    tensor = try inputTensor(a, name);
                    const value = try context.importHostTensor(a, &tensor);
                    tensor.deinit();
                    try inputs.append(a, .{ .node_id = id, .value = value });
                    continue;
                },
                else => return err,
            };
            defer ref.deinit(a);
            var loaded = try source.loadTensorRef(&ref);
            defer loaded.deinit();
            const value = try context.importHostTensor(a, &loaded.tensor);
            try inputs.append(a, .{ .node_id = id, .value = value });
            continue;
        } else blk: {
            for (weights) |weight| if (std.mem.eql(u8, weight.name, name)) {
                if (quant_reference and weight.shape.rank() == 2) {
                    const values = try a.dupe(f32, weight.data.f32);
                    defer a.free(values);
                    for (values) |*value| value.* = @round(value.* * 1024) / 1024;
                    tensor = try Tensor.initFloat32(a, name, node.output_shape.dims[0..node.output_shape.rank()], values);
                } else tensor = try Tensor.initFloat32(a, name, node.output_shape.dims[0..node.output_shape.rank()], weight.data.f32);
                break :blk;
            };
            tensor = try inputTensor(a, name);
        }
        defer tensor.deinit();
        const value = try context.importHostTensor(a, &tensor);
        errdefer cb.free(value);
        try inputs.append(a, .{ .node_id = id, .value = value });
    }
    var program = try Program.init(a, graph, &.{});
    defer program.deinit();
    const bound = try a.dupe(@import("../../graph/interpreter.zig").RuntimeInput, inputs.items);
    defer a.free(bound);
    const bindings = try program.remapInputs(bound);
    var prepared = try executable.prepare(a, &program, &cb, .partitioned);
    defer prepared.deinit();
    var result = try prepared.run(a, .{ .runtime_inputs = bindings, .output_placement = .producer, .collect_partition_stats = true });
    defer result.deinit(&prepared.runtime);
    try std.testing.expectEqual(cb.kind(), result.backendFor(&prepared.runtime, 0).kind());
    return result.backendFor(&prepared.runtime, 0).toFloat32(result.outputs[0], a);
}

fn compare(expected: []const f32, actual: []const f32) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| {
        try std.testing.expect(std.math.isFinite(got));
        try std.testing.expectApproxEqAbs(want, got, 2e-4);
    }
}

pub fn run(kind: Backend) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var graph = ml.graph.Graph.init(scratch);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const ids = try b.parameter("ids", ml.graph.Shape.init(.i64, &.{4}));
    const positions = try b.parameter("positions", ml.graph.Shape.init(.i64, &.{4}));
    const types = try b.parameter("types", ml.graph.Shape.init(.i64, &.{4}));
    const mask = try b.parameter("mask", ml.graph.Shape.init(.f32, &.{ 1, 4, 4 }));
    const built = try @import("../../architectures/bert_graph.zig").buildForwardGraph(&b, .{ .vocab_size = 8, .hidden_size = 64, .num_hidden_layers = 1, .num_attention_heads = 1, .intermediate_size = 128, .max_position_embeddings = 4 }, 1, 4, .{ .input_ids = ids, .position_ids = positions, .token_type_ids = types, .attn_bias = mask });
    try graph.markOutput(built.output_node);
    const weights = try weightValues(scratch, &graph);
    const expected = try prepareArtifact(&graph, weights, null, null, .native, false);
    defer a.free(expected);
    const quant_expected = try prepareArtifact(&graph, weights, null, null, .native, true);
    defer a.free(quant_expected);
    var changed = false;
    for (expected, quant_expected) |x, y| {
        changed = changed or @abs(x - y) > 1e-5;
        try std.testing.expect(@abs(x - y) < 0.1);
    }
    try std.testing.expect(changed);
    var sf = try safetensors.MMapReader.fromBorrowedBytesLimited(a, try safetensorsBytes(scratch, weights), safetensors.max_header_size);
    defer sf.deinit();
    const actual_sf = try prepareArtifact(&graph, weights, &sf, null, kind, false);
    defer a.free(actual_sf);
    try compare(expected, actual_sf);
    for ([_]bool{ false, true }) |q8| {
        const source = try store_mod.GgufStore.initOwnedBytes(a, "equivalent-bert", try ggufBytes(scratch, weights, q8, "bert"));
        const store = source.tensorStore();
        defer store.deinit();
        const actual = try prepareArtifact(&graph, weights, null, store, kind, false);
        defer a.free(actual);
        try compare(if (q8) quant_expected else expected, actual);
    }
    const encoded = try onnx.exportGraph(scratch, &graph, .{ .lower_fused = true, .parameter_initializers = weights });
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "equivalent.onnx", .data = encoded });
    const path = try dir.dir.realPathFileAlloc(std.testing.io, "equivalent.onnx", a);
    defer a.free(path);
    var session = try @import("../../backends/imported_onnx_session.zig").createSessionWithOptions(a, path, kind, .{ .graph_runtime_strategy = .partitioned });
    defer session.close();
    var tensors: [4]Tensor = undefined;
    var count: usize = 0;
    defer for (tensors[0..count]) |*tensor| tensor.deinit();
    for (session.inputInfo(), 0..) |info, i| {
        tensors[i] = try inputTensor(a, info.name);
        count += 1;
    }
    var result = (try session.runResident(&tensors, a)) orelse return error.TestUnexpectedResult;
    defer result.deinit();
    const actual = try result.backend.toFloat32(result.outputs[0], a);
    defer a.free(actual);
    try compare(expected, actual);
}

// Sliding-window decoder block with explicit, functional K/V bindings. A fixed
// window deliberately reuses one physical plan while successive states differ.
fn decoderGraph(graph: *ml.graph.Graph) !void {
    const S = ml.graph.Shape;
    var b = ml.graph.Builder.init(graph);
    const x = try b.parameter("x", S.init(.f32, &.{ 1, 64 }));
    const mask = try b.parameter("mask", S.init(.f32, &.{ 1, 3 }));
    const past_k = try b.parameter("state.k", S.init(.f32, &.{ 64, 2 }));
    const past_v = try b.parameter("state.v", S.init(.f32, &.{ 64, 2 }));
    const q = try b.matmul(x, try b.parameter("weights.q", S.init(.f32, &.{ 64, 64 })));
    const k = try b.matmul(x, try b.parameter("weights.k", S.init(.f32, &.{ 64, 64 })));
    const v = try b.matmul(x, try b.parameter("weights.v", S.init(.f32, &.{ 64, 64 })));
    const keys = try b.concat(past_k, try b.transpose(k, &.{ 1, 0 }), 1);
    const values = try b.concat(past_v, try b.transpose(v, &.{ 1, 0 }), 1);
    const scores = try b.add(try b.mul(try b.matmul(q, keys), try b.scalarConst(.f32, 0.125)), mask);
    const attended = try b.matmul(try b.softmax(scores), try b.transpose(values, &.{ 1, 0 }));
    const residual = try b.add(x, attended);
    const up = try b.matmul(residual, try b.parameter("weights.up", S.init(.f32, &.{ 64, 128 })));
    const down = try b.matmul(try b.gelu(up), try b.parameter("weights.down", S.init(.f32, &.{ 128, 64 })));
    const logits = try b.matmul(try b.add(residual, down), try b.parameter("weights.head", S.init(.f32, &.{ 64, 8 })));
    try graph.markOutput(logits);
    try graph.markOutput(try b.sliceLastDim(keys, 1, 3));
    try graph.markOutput(try b.sliceLastDim(values, 1, 3));
}

fn decoderInput(name: []const u8, step: usize) !Tensor {
    if (std.mem.eql(u8, name, "x")) {
        var values: [64]f32 = undefined;
        for (&values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i + step * 7) % 23)) - 11)) / 23;
        return Tensor.initFloat32(a, name, &.{ 1, 64 }, &values);
    }
    if (std.mem.eql(u8, name, "mask")) {
        var values: [3]f32 = @splat(0);
        for (&values, 0..) |*value, i| if (i + @min(step, 2) < 2) {
            value.* = -10000;
        };
        return Tensor.initFloat32(a, name, &.{ 1, 3 }, &values);
    }
    return Tensor.initFloat32(a, name, &.{ 64, 2 }, &@as([128]f32, @splat(0)));
}

fn decoderWeight(context: *Context, weight: Initializer, reader: ?*safetensors.MMapReader, store: ?store_mod.TensorStore, quant_reference: bool) !@import("../../ops/ops.zig").CT {
    if (reader) |source| {
        var tensor = try source.readTensor(weight.name);
        defer tensor.deinit();
        return context.importHostTensor(a, &tensor);
    }
    if (store) |source| {
        var ref = try source.describeTensor(a, weight.name);
        defer ref.deinit(a);
        var loaded = try source.loadTensorRef(&ref);
        defer loaded.deinit();
        return context.importHostTensor(a, &loaded.tensor);
    }
    const values = try a.dupe(f32, weight.data.f32);
    defer a.free(values);
    if (quant_reference) for (values) |*value| {
        value.* = @round(value.* * 1024) / 1024;
    };
    var tensor = try Tensor.initFloat32(a, weight.name, weight.shape.dims[0..weight.shape.rank()], values);
    defer tensor.deinit();
    return context.importHostTensor(a, &tensor);
}

fn decoderPrepared(graph: *const ml.graph.Graph, weights: []const Initializer, reader: ?*safetensors.MMapReader, store: ?store_mod.TensorStore, kind: Backend, quant_reference: bool) ![]f32 {
    var context = try Context.init(a, kind, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var program = try Program.init(a, graph, &.{
        .{ .name = "k", .node_id = graph.parameters.items[2], .kind = .state_input },
        .{ .name = "v", .node_id = graph.parameters.items[3], .kind = .state_input },
        .{ .name = "k", .node_id = graph.outputs.items[1], .kind = .state_output },
        .{ .name = "v", .node_id = graph.outputs.items[2], .kind = .state_output },
    });
    defer program.deinit();
    var prepared = try executable.prepare(a, &program, &cb, .partitioned);
    defer prepared.deinit();
    var static: std.ArrayListUnmanaged(@import("../../graph/interpreter.zig").RuntimeInput) = .empty;
    defer {
        for (static.items) |input| cb.free(input.value);
        static.deinit(a);
    }
    for (graph.parameters.items) |id| {
        const name = graph.parameterName(graph.node(id));
        for (weights) |weight| if (std.mem.eql(u8, weight.name, name)) {
            const value = try decoderWeight(&context, weight, reader, store, quant_reference);
            errdefer cb.free(value);
            try static.append(a, .{ .node_id = id, .value = value });
            break;
        };
    }
    var held: [4]executable.runtime_module.Result = undefined;
    var count: usize = 0;
    defer for (held[0..count]) |*result| result.deinit(&prepared.runtime);
    for (0..held.len) |step| {
        var inputs: std.ArrayListUnmanaged(@import("../../graph/interpreter.zig").RuntimeInput) = .empty;
        defer inputs.deinit(a);
        try inputs.appendSlice(a, static.items);
        var owned: std.ArrayListUnmanaged(@import("../../ops/ops.zig").CT) = .empty;
        defer {
            for (owned.items) |value| cb.free(value);
            owned.deinit(a);
        }
        for (graph.parameters.items[0..4], 0..) |id, index| {
            const value = if (step > 0 and index >= 2) held[step - 1].outputs[index - 1] else blk: {
                var tensor = try decoderInput(graph.parameterName(graph.node(id)), step);
                defer tensor.deinit();
                const imported = try context.importHostTensor(a, &tensor);
                errdefer cb.free(imported);
                try owned.append(a, imported);
                break :blk imported;
            };
            try inputs.append(a, .{ .node_id = id, .value = value });
        }
        const bound = try program.remapInputs(inputs.items);
        if (step == 0) {
            // Cold packing may inspect immutable weights. The functional state
            // graph has no side effects, so discard this warm-up result before
            // the four strictly resident state transitions below.
            var warm = try prepared.run(a, .{ .runtime_inputs = bound, .output_placement = .default_backend });
            warm.deinit(&prepared.runtime);
        }
        held[step] = try prepared.run(a, .{ .runtime_inputs = bound, .output_placement = .default_backend, .activation_readbacks = .forbidden });
        count += 1;
        for (0..3) |output| try std.testing.expectEqual(cb.kind(), held[step].backendFor(&prepared.runtime, output).kind());
    }
    var snapshots: std.ArrayListUnmanaged(f32) = .empty;
    errdefer snapshots.deinit(a);
    // Read only after all steps, so overwriting an earlier published frame is
    // visible. The K/V chain itself never crosses a host output boundary.
    for (&held) |*result| for (result.outputs, 0..) |value, i| {
        const values = try result.backendFor(&prepared.runtime, i).toFloat32(value, a);
        defer a.free(values);
        try snapshots.appendSlice(a, values);
    };
    return snapshots.toOwnedSlice(a);
}

fn decoderOnnx(graph: *const ml.graph.Graph, weights: []const Initializer, kind: Backend, scratch: std.mem.Allocator) ![]f32 {
    const encoded = try onnx.exportGraph(scratch, graph, .{ .lower_fused = true, .parameter_initializers = weights });
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "decoder.onnx", .data = encoded });
    const path = try dir.dir.realPathFileAlloc(std.testing.io, "decoder.onnx", a);
    defer a.free(path);
    var session = try @import("../../backends/imported_onnx_session.zig").createSessionWithOptions(a, path, kind, .{ .graph_runtime_strategy = .partitioned });
    defer session.close();
    var held: [4]@import("../../backends/session.zig").ResidentOutputs = undefined;
    var count: usize = 0;
    defer for (held[0..count]) |*result| result.deinit();
    for (0..held.len) |step| {
        var audit = @import("../transfer_audit.zig").Audit{ .forbid_device_host_access = true };
        if (step > 0) try @import("../transfer_audit.zig").begin(&audit);
        defer if (step > 0) @import("../transfer_audit.zig").end();
        var tensors: [4]Tensor = undefined;
        var initialized: usize = 0;
        defer for (tensors[0..initialized]) |*tensor| tensor.deinit();
        for (session.inputInfo(), 0..) |info, i| {
            tensors[i] = try decoderInput(info.name, step);
            initialized += 1;
        }
        if (step == 0) {
            held[step] = (try session.runResident(&tensors, a)) orelse return error.TestUnexpectedResult;
        } else {
            const cb = held[step - 1].backend;
            var inputs: [4]@import("../../backends/session.zig").ResidentInput = undefined;
            var owned: std.ArrayListUnmanaged(@import("../../ops/ops.zig").CT) = .empty;
            defer {
                for (owned.items) |value| cb.free(value);
                owned.deinit(a);
            }
            for (session.inputInfo(), 0..) |info, i| {
                const state_index: ?usize = if (std.mem.eql(u8, info.name, "state.k")) 1 else if (std.mem.eql(u8, info.name, "state.v")) 2 else null;
                const value = if (state_index) |index| held[step - 1].outputs[index] else blk: {
                    var dims: [2]i32 = undefined;
                    for (tensors[i].shape, 0..) |dim, axis| dims[axis] = @intCast(dim);
                    const uploaded = try cb.fromFloat32Shape(tensors[i].asFloat32(), dims[0..tensors[i].shape.len]);
                    errdefer cb.free(uploaded);
                    try owned.append(a, uploaded);
                    break :blk uploaded;
                };
                inputs[i] = .{ .value = value, .backend = cb, .shape = tensors[i].shape, .estimated_bytes = tensors[i].data.len };
            }
            held[step] = (try session.runResidentInputs(&inputs, a)) orelse return error.TestUnexpectedResult;
        }
        count += 1;
        try std.testing.expectEqual(kind, @as(Backend, switch (held[step].backend.kind()) {
            .native => .native,
            .metal => .metal,
            .cuda => .cuda,
            else => return error.TestUnexpectedResult,
        }));
    }
    var snapshots: std.ArrayListUnmanaged(f32) = .empty;
    errdefer snapshots.deinit(a);
    for (&held) |*result| for (result.outputs) |value| {
        const values = try result.backend.toFloat32(value, a);
        defer a.free(values);
        try snapshots.appendSlice(a, values);
    };
    return snapshots.toOwnedSlice(a);
}

pub fn runDecoder(kind: Backend) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var graph = ml.graph.Graph.init(scratch);
    defer graph.deinit();
    try decoderGraph(&graph);
    const weights = try weightValues(scratch, &graph);
    const expected = try decoderPrepared(&graph, weights, null, null, .native, false);
    defer a.free(expected);
    const quant_expected = try decoderPrepared(&graph, weights, null, null, .native, true);
    defer a.free(quant_expected);
    var sf = try safetensors.MMapReader.fromBorrowedBytesLimited(a, try safetensorsBytes(scratch, weights), safetensors.max_header_size);
    defer sf.deinit();
    const actual_sf = try decoderPrepared(&graph, weights, &sf, null, kind, false);
    defer a.free(actual_sf);
    try compare(expected, actual_sf);
    for ([_]bool{ false, true }) |q8| {
        const source = try store_mod.GgufStore.initOwnedBytes(a, "equivalent-decoder", try ggufBytes(scratch, weights, q8, "generic_decoder"));
        const store = source.tensorStore();
        defer store.deinit();
        const actual = try decoderPrepared(&graph, weights, null, store, kind, false);
        defer a.free(actual);
        try compare(if (q8) quant_expected else expected, actual);
    }
    const actual_onnx = try decoderOnnx(&graph, weights, kind, scratch);
    defer a.free(actual_onnx);
    try compare(expected, actual_onnx);
}
