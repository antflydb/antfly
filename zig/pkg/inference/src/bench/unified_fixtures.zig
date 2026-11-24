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

//! Export one pinned BERT checkpoint into equivalent encoder acceptance fixtures.
const std = @import("std");
const ml = @import("ml");
const onnx = @import("onnx_graph");
const inference = @import("inference_internal");

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    var source: ?[]const u8 = null;
    var onnx_dir: ?[]const u8 = null;
    var gguf_dir: ?[]const u8 = null;
    var batch: u32 = 1;
    var seq: u32 = 16;
    while (args.next()) |arg| {
        const value = args.next() orelse return error.MissingArgument;
        if (std.mem.eql(u8, arg, "--model-dir")) source = value else if (std.mem.eql(u8, arg, "--onnx-dir")) onnx_dir = value else if (std.mem.eql(u8, arg, "--gguf-dir")) gguf_dir = value else if (std.mem.eql(u8, arg, "--batch")) batch = try std.fmt.parseInt(u32, value, 10) else if (std.mem.eql(u8, arg, "--seq-len")) seq = try std.fmt.parseInt(u32, value, 10) else return error.InvalidArgument;
    }
    const model_dir = source orelse return error.MissingModelDir;
    if (batch == 0 or seq == 0 or (onnx_dir == null and gguf_dir == null)) return error.InvalidGeometry;
    if (onnx_dir) |path| {
        try std.Io.Dir.cwd().createDirPath(init.io, path);
        try exportOnnx(allocator, init.io, model_dir, path, batch, seq);
        try copyMetadata(allocator, init.io, model_dir, path);
    }
    if (gguf_dir) |path| {
        try std.Io.Dir.cwd().createDirPath(init.io, path);
        const output = try std.fs.path.join(allocator, &.{ path, "model.gguf" });
        defer allocator.free(output);
        try inference.native_export_gguf.exportModelDirToGguf(allocator, model_dir, output, .none);
        try copyMetadata(allocator, init.io, model_dir, path);
    }
}

fn copyMetadata(allocator: std.mem.Allocator, io: std.Io, source: []const u8, destination: []const u8) !void {
    for ([_][]const u8{ "config.json", "model_manifest.json", "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json", "vocab.txt", "modules.json", "1_Pooling/config.json", "fixture_revision.json" }) |name| {
        const input = try std.fs.path.join(allocator, &.{ source, name });
        defer allocator.free(input);
        const data = std.Io.Dir.cwd().readFileAlloc(io, input, allocator, .limited(16 * 1024 * 1024)) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer allocator.free(data);
        const output = try std.fs.path.join(allocator, &.{ destination, name });
        defer allocator.free(output);
        if (std.fs.path.dirname(output)) |parent| try std.Io.Dir.cwd().createDirPath(io, parent);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output, .data = data });
    }
}

fn exportOnnx(allocator: std.mem.Allocator, io: std.Io, source: []const u8, destination: []const u8, batch: u32, seq: u32) !void {
    var model = try inference.execution.LoadedModel.open(allocator, source);
    defer model.deinit();
    if (model.architecture != .bert) return error.UnsupportedBenchmarkArchitecture;
    const config = model.architecture.bert;
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const ids = try b.parameter("input_ids", ml.graph.Shape.init(.i64, &.{batch * seq}));
    const positions = try b.parameter("position_ids", ml.graph.Shape.init(.i64, &.{batch * seq}));
    const types = if (config.model_type != .distilbert) try b.parameter("token_type_ids", ml.graph.Shape.init(.i64, &.{batch * seq})) else null;
    const bias = try b.parameter("attn_bias", ml.graph.Shape.init(.f32, &.{ batch * config.num_attention_heads, seq, seq }));
    const forward = try inference.architectures.bert_graph.buildForwardGraph(&b, .{
        .vocab_size = config.vocab_size,
        .hidden_size = config.hidden_size,
        .num_hidden_layers = config.num_hidden_layers,
        .num_attention_heads = config.num_attention_heads,
        .intermediate_size = config.intermediate_size,
        .max_position_embeddings = config.max_position_embeddings,
        .type_vocab_size = config.type_vocab_size,
        .hidden_act = config.hidden_act,
        .layer_norm_eps = config.layer_norm_eps,
        .use_token_type = config.model_type != .distilbert,
    }, batch, seq, .{ .input_ids = ids, .position_ids = positions, .token_type_ids = types, .attn_bias = bias });
    try graph.markOutput(forward.output_node);
    const weights = (try model.weights.?.weightSource()).?;
    var initializers: std.ArrayListUnmanaged(onnx.ParameterInitializer) = .empty;
    defer initializers.deinit(allocator);
    var loaded: std.ArrayListUnmanaged(inference.models.weight_source.LoadedWeight) = .empty;
    defer {
        for (loaded.items) |*weight| weight.deinit();
        loaded.deinit(allocator);
    }
    for (graph.parameters.items) |id| {
        if (id == ids or id == positions or id == bias or (types != null and id == types.?)) continue;
        const node = graph.node(id);
        const name = graph.parameterName(node);
        var weight = try weights.getTensor(name);
        var adopted = false;
        errdefer if (!adopted) weight.deinit();
        if (weight.tensor.dtype != .f32 or weight.tensor.data.len != node.output_shape.numElements().? * @sizeOf(f32)) return error.InvalidCheckpointRepresentation;
        try loaded.append(allocator, weight);
        adopted = true;
        try initializers.append(allocator, .{ .name = name, .shape = node.output_shape, .data = .{ .raw_bytes = weight.tensor.data } });
    }
    const sidecar = try std.fs.path.join(allocator, &.{ destination, "weights.bin" });
    defer allocator.free(sidecar);
    var exported = try onnx.exportGraphWithExternalDataToPath(allocator, &graph, .{ .parameter_initializers = initializers.items, .lower_fused = true }, "weights.bin", sidecar);
    defer exported.deinit(allocator);
    const path = try std.fs.path.join(allocator, &.{ destination, "model.onnx" });
    defer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = exported.model_bytes });
    std.debug.print("unified_fixture format=onnx nodes={d} weights={d} batch={d} seq={d} output={s}\n", .{ graph.nodeCount(), initializers.items.len, batch, seq, path });
}
