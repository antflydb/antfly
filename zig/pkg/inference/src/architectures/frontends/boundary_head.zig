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

//! Reuse the differentiable task graph as the inference semantic owner. Runtime
//! state and candidate routing are operands, and checkpoint tensors are named
//! parameters shared by all specializations of the loaded session.
const std = @import("std");
const ml = @import("ml").graph;
const capture = @import("../../execution/capture.zig");
const program = @import("../../execution/program.zig");
const core = @import("../gliner/boundary_graph.zig");
const candidates = @import("../gliner/boundary_graph_candidates.zig");
const tasks = @import("../gliner/boundary_graph_tasks.zig");
const Config = @import("../../models/gliner_boundary.zig").Config;

pub fn resolve(config: *Config) capture.Forward {
    return .{ .context = config, .fingerprint = 8, .author = author, .validate = validate, .output_abi = outputNames };
}

fn outputNames(raw: *anyopaque, inputs: []const capture.Input) ![]const []const u8 {
    const config: *Config = @ptrCast(@alignCast(raw));
    if (find(inputs, "head.classification_states") != null) return &.{"classification_logits"};
    if (find(inputs, "head.relation.valid") != null) return &.{"relation_logits"};
    if (find(inputs, "head.record.field_membership") != null) return &.{ "record_objects", "record_assignments" };
    if (find(inputs, "head.explicit.valid") != null) return &.{"explicit_logits"};
    if (find(inputs, "head.pool.valid") != null) return if (config.head.enable_records) &.{ "pair_logits", "candidate_features", "candidate_states" } else &.{ "pair_logits", "candidate_features" };
    const base = [_][]const u8{ "boundary_states", "boundary_mask", "start_logits", "end_logits", "inside_logits", "pool_start", "pool_end" };
    if (config.head.enable_abstention and config.head.enable_count_head) return &(base ++ [_][]const u8{ "null_logits", "count_log_rates" });
    if (config.head.enable_abstention) return &(base ++ [_][]const u8{"null_logits"});
    if (config.head.enable_count_head) return &(base ++ [_][]const u8{"count_log_rates"});
    return &base;
}

const find = capture.findInput;

fn validate(raw: *anyopaque, inputs: []const capture.Input) !void {
    const config: *Config = @ptrCast(@alignCast(raw));
    try config.head.validate();
    try capture.validateHostInputs(inputs);
    for (inputs) |input| {
        switch (input.tensor.dtype) {
            .f32 => if (!input.tensor.isAlignedFor(f32)) return error.InvalidInputShape,
            .i32 => if (!input.tensor.isAlignedFor(i32)) return error.InvalidInputShape,
            .i64 => if (!input.tensor.isAlignedFor(i64)) return error.InvalidInputShape,
            else => return error.InvalidInputShape,
        }
        if (!std.mem.startsWith(u8, input.tensor.name, "head.")) return error.UnexpectedModelProgramInput;
    }
    const source = (find(inputs, "head.text_states") orelse (find(inputs, "head.classification_states") orelse return error.MissingInputs)).tensor;
    if (source.dtype != .f32 or source.shape.len != 3 or source.shape[0] <= 0 or source.shape[1] <= 0 or source.shape[2] != config.encoder.hidden_size) return error.InvalidInputShape;
}

const Author = struct {
    builder: *ml.Builder,
    inputs: []const capture.Input,
    nodes: []const ml.NodeId,
    bindings: *std.ArrayList(program.Binding),
    allocator: std.mem.Allocator,

    fn input(self: @This(), name: []const u8) !ml.NodeId {
        for (self.inputs, self.nodes) |value, node| if (std.mem.eql(u8, value.tensor.name, name)) return node;
        return error.MissingInputs;
    }
    fn flat(self: @This(), name: []const u8) !ml.NodeId {
        const id = try self.input(name);
        const shape = self.builder.graph.node(id).output_shape;
        if (shape.rank() != 3) return error.InvalidInputShape;
        return self.builder.reshape(id, ml.Shape.init(shape.dtype, &.{ try std.math.mul(i64, shape.dim(0), shape.dim(1)), shape.dim(2) }));
    }
    fn output(self: @This(), name: []const u8, id: ml.NodeId) !void {
        try self.builder.graph.outputs.append(self.allocator, id);
        try self.bindings.append(self.allocator, .{ .name = name, .node_id = id, .kind = .output });
    }
};

fn author(raw: *anyopaque, a: std.mem.Allocator, inputs: []const capture.Input) !program.ModelProgram {
    try validate(raw, inputs);
    const config: *Config = @ptrCast(@alignCast(raw));
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    var bindings: std.ArrayList(program.Binding) = .empty;
    defer bindings.deinit(a);
    const nodes = try a.alloc(ml.NodeId, inputs.len);
    defer a.free(nodes);
    for (inputs, nodes) |input, *node| {
        node.* = try builder.parameter(input.tensor.name, try capture.inputShape(input.tensor));
        try bindings.append(a, .{ .name = input.tensor.name, .node_id = node.*, .kind = .input });
    }
    const state = Author{ .builder = &builder, .inputs = inputs, .nodes = nodes, .bindings = &bindings, .allocator = a };
    if (find(inputs, "head.classification_states")) |source| {
        const batch: u32 = @intCast(source.tensor.shape[0]);
        const count: u32 = @intCast(source.tensor.shape[1]);
        var g = try core.GraphBuilder.init(a, &builder, config.*, .{ .batch = batch, .words = 1, .queries = 0, .classifications = count }, .evaluation, .{});
        defer g.deinit();
        const logits = try g.buildClassification(try state.flat("head.classification_states"), try std.math.mul(u32, batch, count));
        try state.output("classification_logits", try g.reshape(logits, &.{ batch, count }));
    } else {
        const source = find(inputs, "head.text_states").?.tensor;
        const query = (find(inputs, "head.query_states") orelse return error.MissingInputs).tensor;
        if (query.dtype != .f32 or query.shape.len != 3 or query.shape[0] != source.shape[0] or query.shape[1] <= 0 or query.shape[2] != config.encoder.hidden_size) return error.InvalidInputShape;
        const b: u32 = @intCast(source.shape[0]);
        const w: u32 = @intCast(source.shape[1]);
        const q: u32 = @intCast(query.shape[1]);
        var g = try core.GraphBuilder.init(a, &builder, config.*, .{ .batch = b, .words = w, .queries = q }, .evaluation, .{});
        defer g.deinit();
        const routed = core.Input{ .text = try state.flat("head.text_states"), .queries = try state.flat("head.query_states"), .text_mask = try state.input("head.text_mask"), .query_mask = try state.input("head.query_mask") };
        if (find(inputs, "head.relation.valid")) |valid| {
            const relations = find(inputs, "head.relation.queries") orelse return error.MissingInputs;
            if (valid.tensor.shape.len != 1 or valid.tensor.shape[0] <= 0 or relations.tensor.shape.len != 3 or relations.tensor.shape[0] != b) return error.InvalidInputShape;
            const result = try tasks.buildRelations(&g, .{
                .pairs = @intCast(valid.tensor.shape[0]),
                .relations = @intCast(relations.tensor.shape[1]),
                .text = routed.text,
                .relation_queries = try state.flat("head.relation.queries"),
                .query_indices = try state.input("head.relation.query_indices"),
                .text_indices = .{ try state.input("head.relation.head_first"), try state.input("head.relation.head_last"), try state.input("head.relation.tail_first"), try state.input("head.relation.tail_last") },
                .head_prefix_start = try state.input("head.relation.head_start"),
                .head_prefix_end = try state.input("head.relation.head_end"),
                .tail_prefix_start = try state.input("head.relation.tail_start"),
                .tail_prefix_end = try state.input("head.relation.tail_end"),
                .head_length = try state.input("head.relation.head_length"),
                .tail_length = try state.input("head.relation.tail_length"),
                .geometry = try state.input("head.relation.geometry"),
                .valid = try state.input("head.relation.valid"),
            });
            try state.output("relation_logits", result.logits);
        } else if (find(inputs, "head.record.field_membership")) |membership| {
            const mode = (find(inputs, "head.record.mode") orelse return error.MissingInputs).tensor;
            if (mode.dtype != .i64 or !mode.isAlignedFor(i64) or mode.asInt64().len != 1 or mode.asInt64()[0] < 0 or mode.asInt64()[0] > 2 or membership.tensor.shape.len != 2 or membership.tensor.shape[0] <= 0 or membership.tensor.shape[1] <= 0) return error.InvalidInputShape;
            const record_mode: @import("../../pipelines/extraction_schema.zig").RecordMode = @fromBackingInt(@as(u2, @intCast(mode.asInt64()[0])));
            const result = try tasks.buildRecordGroupDense(&g, .{
                .mode = record_mode,
                .fields = @intCast(membership.tensor.shape[0]),
                .candidates = @intCast(membership.tensor.shape[1]),
                .candidate_states = try state.input("head.record.candidate_states"),
                .field_queries = try state.input("head.record.field_queries"),
                .candidate_mask = try state.input("head.record.candidate_mask"),
                .field_membership = try state.input("head.record.field_membership"),
                .instance_mask = try state.input("head.record.instance_mask"),
                .natural_object_logits = if (record_mode == .natural) try state.input("head.record.object_logits") else null,
            });
            try state.output("record_objects", result.object_logits);
            try state.output("record_assignments", result.assignment_logits);
        } else {
            const proposals: core.Proposals = if (find(inputs, "head.boundary_states") != null) .{
                .input = routed,
                .boundary_states = try state.input("head.boundary_states"),
                .boundary_mask = try state.input("head.boundary_mask"),
                .start_logits = try state.input("head.start_logits"),
                .end_logits = try state.input("head.end_logits"),
                .inside_logits = try state.input("head.inside_logits"),
                .pool_start = try state.input("head.pool_start"),
                .pool_end = try state.input("head.pool_end"),
                .null_logits = null,
                .count_logits = null,
            } else try g.buildProposals(routed);
            if (find(inputs, "head.explicit.valid")) |valid| {
                if (valid.tensor.shape.len != 3 or valid.tensor.shape[0] != b or valid.tensor.shape[1] != q or valid.tensor.shape[2] <= 0) return error.InvalidInputShape;
                const result = try candidates.buildExplicitSpans(&g, proposals, .{
                    .capacity = @intCast(valid.tensor.shape[2]),
                    .starts = try state.input("head.explicit.starts"),
                    .ends = try state.input("head.explicit.ends"),
                    .marginal_starts = try state.input("head.explicit.marginal_starts"),
                    .marginal_ends = try state.input("head.explicit.marginal_ends"),
                    .valid = try state.input("head.explicit.valid"),
                    .lengths = try state.input("head.explicit.lengths"),
                    .length_features = try state.input("head.explicit.length_features"),
                    .inside_mean = if (config.head.use_inside_evidence) try state.input("head.explicit.inside_mean") else null,
                });
                try state.output("explicit_logits", result.logits);
            } else if (find(inputs, "head.pool.valid")) |pool| {
                if (pool.tensor.shape.len != 2 or pool.tensor.shape[0] != b or pool.tensor.shape[1] <= 0) return error.InvalidInputShape;
                const result = try candidates.buildSharedPool(&g, proposals, .{ .capacity = @intCast(pool.tensor.shape[1]), .starts = try state.input("head.pool.starts"), .ends = try state.input("head.pool.ends"), .valid = try state.input("head.pool.valid"), .lengths = try state.input("head.pool.lengths"), .length_features = try state.input("head.pool.length_features"), .inside_mean = if (config.head.use_inside_evidence) try state.input("head.pool.inside_mean") else null });
                try state.output("pair_logits", result.pair_logits);
                try state.output("candidate_features", result.candidate_features);
                if (result.candidate_states) |value| try state.output("candidate_states", value);
            } else {
                try state.output("boundary_states", proposals.boundary_states);
                try state.output("boundary_mask", proposals.boundary_mask);
                try state.output("start_logits", proposals.start_logits);
                try state.output("end_logits", proposals.end_logits);
                try state.output("inside_logits", proposals.inside_logits);
                try state.output("pool_start", proposals.pool_start);
                try state.output("pool_end", proposals.pool_end);
                if (proposals.null_logits) |value| try state.output("null_logits", value);
                if (proposals.count_logits) |value| try state.output("count_log_rates", value);
            }
        }
    }
    for (graph.parameters.items) |id| {
        var external = false;
        for (nodes) |input| if (id == input) {
            external = true;
            break;
        };
        if (!external) try bindings.append(a, .{ .name = graph.parameterName(graph.node(id)), .node_id = id, .kind = .weight });
    }
    return program.ModelProgram.init(a, &graph, bindings.items);
}
