// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Query-control planner surface. Physical index planning remains in
//! `planner.zig`; this module owns only plans constructible without an Index.

const std = @import("std");
const ir = @import("ir.zig");

pub const TensorProgramQueryPlan = struct {
    program_id: []u8,
    inputs: []ir.TensorExpr = &.{},
    access_paths: []ir.PhysicalAccessPath,
    steps: []ir.TensorProgramStep,
    output: ir.TensorProgramRef,
    outputs: []ir.TensorProgramRef = &.{},
    owned_metadata: []?[]u8 = &.{},

    pub fn deinit(self: *TensorProgramQueryPlan, alloc: std.mem.Allocator) void {
        alloc.free(self.program_id);
        if (self.inputs.len > 0) alloc.free(self.inputs);
        if (self.access_paths.len > 0) alloc.free(self.access_paths);
        for (self.owned_metadata) |metadata| if (metadata) |bytes| alloc.free(bytes);
        if (self.owned_metadata.len > 0) alloc.free(self.owned_metadata);
        if (self.steps.len > 0) alloc.free(self.steps);
        if (self.outputs.len > 0) alloc.free(self.outputs);
        self.* = undefined;
    }

    pub fn asProgram(self: *const TensorProgramQueryPlan) ir.TensorProgram {
        return .{
            .inputs = self.inputs,
            .steps = self.steps,
            .output = self.output,
            .outputs = self.outputs,
        };
    }
};

const native_doc_id_constraint_dims = [_]ir.Dimension{.doc};
const vector_search_output_dims = [_]ir.Dimension{ .doc, .score };
const vector_doc_id_constraint_inputs = [_]ir.TensorProgramRef{.{ .input = 0 }};

pub fn planVectorSearchTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    layout: ir.PhysicalLayout,
    constrained: bool,
) !?TensorProgramQueryPlan {
    if (layout != .dense_vector and layout != .sparse_vector) return null;
    const access_path = ir.vectorAccessPath(index_name, layout);
    const access_paths = try alloc.alloc(ir.PhysicalAccessPath, 1);
    errdefer alloc.free(access_paths);
    access_paths[0] = access_path;

    var inputs: []ir.TensorExpr = if (constrained) try alloc.alloc(ir.TensorExpr, 1) else &.{};
    errdefer if (inputs.len > 0) alloc.free(inputs);
    if (constrained) inputs[0] = .{
        .fragment = .slice,
        .output_dims = &native_doc_id_constraint_dims,
        .semantic_id = "native_doc_id_constraints",
    };

    const steps = try alloc.alloc(ir.TensorProgramStep, 1);
    errdefer alloc.free(steps);
    steps[0] = .{ .expr = .{
        .fragment = .vector_search,
        .output_dims = &vector_search_output_dims,
        .owner = access_path.owner,
        .layout = access_path.layout,
    }, .inputs = if (constrained) &vector_doc_id_constraint_inputs else &.{} };

    const program = ir.TensorProgram{ .inputs = inputs, .steps = steps, .output = .{ .step = 0 } };
    const proof = try ir.tensorProgramProof(alloc, access_paths, program);
    if (!proof.safe()) {
        alloc.free(steps);
        if (inputs.len > 0) alloc.free(inputs);
        alloc.free(access_paths);
        return null;
    }
    const program_id = try ir.tensorProgramIdAlloc(alloc, program);
    errdefer alloc.free(program_id);
    return .{
        .program_id = program_id,
        .inputs = inputs,
        .access_paths = access_paths,
        .steps = steps,
        .output = .{ .step = 0 },
    };
}
