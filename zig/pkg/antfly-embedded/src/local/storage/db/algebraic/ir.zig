// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const algebra = @import("algebra.zig");
const join_mod = @import("join.zig");
const law_mod = @import("law.zig");
const lexical = @import("lexical.zig");
const token = @import("token.zig");

pub const QueryKind = enum {
    metric,
    terms,
    histogram,
    date_histogram,
};

pub const Constraint = struct {
    field: []const u8,
    value: []const u8,
};

pub const Metric = struct {
    name: []const u8,
    op: algebra.Op,
    field: []const u8 = "",
};

pub const JoinRef = struct {
    name: []const u8,
    kind: join_mod.TemporalMode = .none,
    group_side: ?[]const u8 = null,
    measure_side: ?[]const u8 = null,
};

pub const Query = struct {
    kind: QueryKind,
    aggregation_name: []const u8,
    bucket_field: ?[]const u8 = null,
    bucket_fields: []const []const u8 = &.{},
    bucket_interval: f64 = 0,
    time_field: ?[]const u8 = null,
    time_bucket: ?[]const u8 = null,
    constraints: []const Constraint = &.{},
    metric: ?Metric = null,
    child_metrics: []const Metric = &.{},
    join: ?JoinRef = null,
};

pub fn joinKind(bucket: ?[]const u8, window_seconds: ?i64) join_mod.TemporalMode {
    return join_mod.temporalMode(bucket, window_seconds);
}

pub const Dimension = enum {
    doc,
    path,
    field,
    kind,
    scalar,
    term,
    time,
    bucket,
    dim,
    src,
    dst,
    score,
};

pub const TensorFragment = enum {
    slice,
    join,
    reduce,
    map,
    merge,
    automaton_select,
    vector_search,
    graph_traverse,
};

pub const PhysicalLayout = enum {
    docfact_rows,
    join_fact_rows,
    pathfact_rows,
    path_lookup_rows,
    dictionary_postings,
    full_text_postings,
    sparse_token_postings,
    dense_vector,
    sparse_vector,
    graph_edges,
    materialized_tensor,
    materialized_expr,
};

pub const TensorExpr = struct {
    fragment: TensorFragment,
    input_dims: []const Dimension = &.{},
    output_dims: []const Dimension = &.{},
    semantic_id: ?[]const u8 = null,
    owner: ?[]const u8 = null,
    layout: ?PhysicalLayout = null,
    dictionary: ?lexical.DictionaryIdentity = null,
    law_id: ?law_mod.Id = null,
    metadata: ?[]const u8 = null,
};

pub const PhysicalAccessPath = struct {
    owner: []const u8,
    layout: PhysicalLayout,
    fragments: []const TensorFragment,
    output_dims: []const Dimension,
    dictionary: ?lexical.DictionaryIdentity = null,
    law_ids: []const law_mod.Id = &.{},
};

pub const AccessPathRejectReason = enum {
    owner_mismatch,
    layout_mismatch,
    missing_fragment,
    missing_output_dimension,
    dictionary_required,
    dictionary_mismatch,
    law_required,
    law_mismatch,
};

pub const AccessPathProof = union(enum) {
    proven,
    rejected: AccessPathRejectReason,

    pub fn safe(self: AccessPathProof) bool {
        return self == .proven;
    }
};

pub const AccessPathPlan = struct {
    index: usize,
    access_path: PhysicalAccessPath,
};

pub const MaterializedExpressionPlan = struct {
    expr_id: []u8,
    law_ids: []law_mod.Id,
    access_path: PhysicalAccessPath,

    pub fn deinit(self: *MaterializedExpressionPlan, alloc: std.mem.Allocator) void {
        alloc.free(self.expr_id);
        alloc.free(self.law_ids);
        self.* = undefined;
    }
};

pub const TensorProgramRef = union(enum) {
    input: usize,
    step: usize,
};

pub const TensorProgramStep = struct {
    expr: TensorExpr,
    inputs: []const TensorProgramRef = &.{},
};

pub const TensorProgram = struct {
    inputs: []const TensorExpr = &.{},
    steps: []const TensorProgramStep = &.{},
    output: TensorProgramRef,
    outputs: []const TensorProgramRef = &.{},
};

pub const TensorProgramRejectReason = enum {
    invalid_output_ref,
    invalid_input_ref,
    invalid_step_ref,
    unplannable_leaf,
    unsupported_derived_fragment,
    derived_law_required,
    derived_output_dimension_required,
};

pub const TensorProgramReject = struct {
    reason: TensorProgramRejectReason,
    step_index: ?usize = null,
};

pub const TensorProgramProof = union(enum) {
    proven,
    rejected: TensorProgramReject,

    pub fn safe(self: TensorProgramProof) bool {
        return self == .proven;
    }
};

pub fn dimensionListTokenAlloc(alloc: std.mem.Allocator, dims: []const Dimension) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    try parts.append(alloc, "dims:v1");
    for (dims) |dim| try parts.append(alloc, @tagName(dim));
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn tensorExprFingerprintAlloc(alloc: std.mem.Allocator, expr: TensorExpr) ![]u8 {
    const input_dims = try dimensionListTokenAlloc(alloc, expr.input_dims);
    defer alloc.free(input_dims);
    const output_dims = try dimensionListTokenAlloc(alloc, expr.output_dims);
    defer alloc.free(output_dims);
    const dictionary_key = if (expr.dictionary) |dictionary| try dictionary.keyAlloc(alloc) else try alloc.dupe(u8, "");
    defer alloc.free(dictionary_key);
    return try token.canonicalTupleAlloc(alloc, &.{
        "tensor-expr:v1",
        @tagName(expr.fragment),
        input_dims,
        output_dims,
        expr.semantic_id orelse "",
        expr.owner orelse "",
        if (expr.layout) |layout| @tagName(layout) else "",
        dictionary_key,
        if (expr.law_id) |law_id| @tagName(law_id) else "",
        expr.metadata orelse "",
    });
}

pub fn tensorExprIdAlloc(alloc: std.mem.Allocator, expr: TensorExpr) ![]u8 {
    const fingerprint = try tensorExprFingerprintAlloc(alloc, expr);
    defer alloc.free(fingerprint);
    return try std.fmt.allocPrint(alloc, "expr:{}", .{token.hash128(fingerprint)});
}

pub fn tensorProgramFingerprintAlloc(alloc: std.mem.Allocator, program: TensorProgram) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    var owned = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned.items) |item| alloc.free(item);
        owned.deinit(alloc);
    }

    try parts.append(alloc, "tensor-program:v1");
    const input_count = try std.fmt.allocPrint(alloc, "inputs:{d}", .{program.inputs.len});
    try appendOwnedProgramToken(alloc, &parts, &owned, input_count);
    for (program.inputs) |expr| {
        const expr_fingerprint = try tensorExprFingerprintAlloc(alloc, expr);
        try appendOwnedProgramToken(alloc, &parts, &owned, expr_fingerprint);
    }
    const step_count = try std.fmt.allocPrint(alloc, "steps:{d}", .{program.steps.len});
    try appendOwnedProgramToken(alloc, &parts, &owned, step_count);
    for (program.steps) |step| {
        const step_fingerprint = try tensorProgramStepFingerprintAlloc(alloc, step);
        try appendOwnedProgramToken(alloc, &parts, &owned, step_fingerprint);
    }
    const output_token = try tensorProgramRefTokenAlloc(alloc, program.output);
    try appendOwnedProgramToken(alloc, &parts, &owned, output_token);
    const output_count = try std.fmt.allocPrint(alloc, "outputs:{d}", .{program.outputs.len});
    try appendOwnedProgramToken(alloc, &parts, &owned, output_count);
    for (program.outputs) |output_ref| {
        const extra_output_token = try tensorProgramRefTokenAlloc(alloc, output_ref);
        try appendOwnedProgramToken(alloc, &parts, &owned, extra_output_token);
    }
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn tensorProgramIdAlloc(alloc: std.mem.Allocator, program: TensorProgram) ![]u8 {
    const fingerprint = try tensorProgramFingerprintAlloc(alloc, program);
    defer alloc.free(fingerprint);
    return try std.fmt.allocPrint(alloc, "program:{}", .{token.hash128(fingerprint)});
}

fn tensorProgramStepFingerprintAlloc(alloc: std.mem.Allocator, step: TensorProgramStep) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    var owned = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned.items) |item| alloc.free(item);
        owned.deinit(alloc);
    }

    try parts.append(alloc, "tensor-program-step:v1");
    const expr_fingerprint = try tensorExprFingerprintAlloc(alloc, step.expr);
    try appendOwnedProgramToken(alloc, &parts, &owned, expr_fingerprint);
    for (step.inputs) |input| {
        const input_token = try tensorProgramRefTokenAlloc(alloc, input);
        try appendOwnedProgramToken(alloc, &parts, &owned, input_token);
    }
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

fn appendOwnedProgramToken(
    alloc: std.mem.Allocator,
    parts: *std.ArrayListUnmanaged([]const u8),
    owned: *std.ArrayListUnmanaged([]u8),
    item: []u8,
) !void {
    var owned_by_list = false;
    errdefer if (!owned_by_list) alloc.free(item);
    try owned.append(alloc, item);
    owned_by_list = true;
    try parts.append(alloc, item);
}

fn tensorProgramRefTokenAlloc(alloc: std.mem.Allocator, ref: TensorProgramRef) ![]u8 {
    return switch (ref) {
        .input => |idx| try std.fmt.allocPrint(alloc, "input:{d}", .{idx}),
        .step => |idx| try std.fmt.allocPrint(alloc, "step:{d}", .{idx}),
    };
}

pub fn accessPathCanSatisfy(path: PhysicalAccessPath, expr: TensorExpr) AccessPathProof {
    if (expr.owner) |owner| {
        if (!std.mem.eql(u8, path.owner, owner)) return .{ .rejected = .owner_mismatch };
    }
    if (expr.layout) |layout| {
        if (path.layout != layout) return .{ .rejected = .layout_mismatch };
    }
    if (!containsFragment(path.fragments, expr.fragment)) return .{ .rejected = .missing_fragment };
    for (expr.output_dims) |dim| {
        if (!containsDimension(path.output_dims, dim)) return .{ .rejected = .missing_output_dimension };
    }
    if (expr.dictionary) |needed| {
        const provided = path.dictionary orelse return .{ .rejected = .dictionary_required };
        if (!provided.eql(needed)) return .{ .rejected = .dictionary_mismatch };
    }
    if (expr.law_id) |needed| {
        if (!containsLaw(path.law_ids, needed)) return .{ .rejected = .law_required };
    }
    return .proven;
}

pub fn tensorProgramProof(alloc: std.mem.Allocator, paths: []const PhysicalAccessPath, program: TensorProgram) !TensorProgramProof {
    if (!tensorProgramRefValid(program, program.output, program.steps.len)) {
        return .{ .rejected = .{ .reason = .invalid_output_ref } };
    }
    for (program.outputs) |output_ref| {
        if (!tensorProgramRefValid(program, output_ref, program.steps.len)) {
            return .{ .rejected = .{ .reason = .invalid_output_ref } };
        }
    }
    for (program.steps, 0..) |step, i| {
        for (step.inputs) |input_ref| {
            if (!tensorProgramRefValid(program, input_ref, i)) {
                return .{ .rejected = .{
                    .reason = switch (input_ref) {
                        .input => .invalid_input_ref,
                        .step => .invalid_step_ref,
                    },
                    .step_index = i,
                } };
            }
        }
        if (step.inputs.len == 0) {
            if (selectUniqueAccessPath(paths, step.expr) != null) continue;
            if (try planMaterializedExpressionAlloc(alloc, step.expr)) |planned_value| {
                var planned = planned_value;
                planned.deinit(alloc);
                continue;
            }
            return .{ .rejected = .{ .reason = .unplannable_leaf, .step_index = i } };
        }
        switch (step.expr.fragment) {
            .reduce, .join, .merge => {
                if (step.expr.law_id == null) return .{ .rejected = .{ .reason = .derived_law_required, .step_index = i } };
                if (step.expr.output_dims.len == 0) return .{ .rejected = .{ .reason = .derived_output_dimension_required, .step_index = i } };
            },
            .map => {
                if (step.expr.output_dims.len == 0) return .{ .rejected = .{ .reason = .derived_output_dimension_required, .step_index = i } };
            },
            .vector_search, .graph_traverse => {
                if (step.expr.output_dims.len == 0) return .{ .rejected = .{ .reason = .derived_output_dimension_required, .step_index = i } };
                if (selectUniqueAccessPath(paths, step.expr) == null) return .{ .rejected = .{ .reason = .unplannable_leaf, .step_index = i } };
            },
            else => return .{ .rejected = .{ .reason = .unsupported_derived_fragment, .step_index = i } },
        }
    }
    return .proven;
}

pub fn vectorSearchProgramMatchesTarget(
    program: TensorProgram,
    owner: []const u8,
    layout: PhysicalLayout,
    constrained: bool,
) bool {
    if (layout != .dense_vector and layout != .sparse_vector) return false;
    const output_step_idx = switch (program.output) {
        .step => |idx| idx,
        .input => return false,
    };
    if (output_step_idx >= program.steps.len) return false;
    const output_step = program.steps[output_step_idx];
    const output_expr = output_step.expr;
    if (output_expr.fragment != .vector_search or
        output_expr.layout == null or output_expr.layout.? != layout or
        output_expr.owner == null or !std.mem.eql(u8, output_expr.owner.?, owner) or
        !std.mem.eql(Dimension, output_expr.output_dims, &.{ .doc, .score }))
    {
        return false;
    }
    const consumes_constraints = tensorProgramStepUsesSemanticInput(program, output_step_idx, "native_doc_id_constraints", &.{.doc});
    return consumes_constraints == constrained;
}

pub fn graphTraversalProgramMatchesTarget(
    program: TensorProgram,
    owner: []const u8,
    constrained: bool,
) bool {
    const output_step_idx = switch (program.output) {
        .step => |idx| idx,
        .input => return false,
    };
    if (output_step_idx >= program.steps.len) return false;
    const output_step = program.steps[output_step_idx];
    const output_expr = output_step.expr;
    if (output_expr.fragment != .graph_traverse or
        output_expr.layout == null or output_expr.layout.? != .graph_edges or
        output_expr.owner == null or !std.mem.eql(u8, output_expr.owner.?, owner) or
        output_expr.law_id == null or output_expr.law_id.? != .provenance_semiring or
        !std.mem.eql(Dimension, output_expr.output_dims, &.{.doc}))
    {
        return false;
    }
    const consumes_constraints = tensorProgramStepUsesSemanticInput(program, output_step_idx, "graph_target_constraints", &.{.doc});
    return consumes_constraints == constrained;
}

pub fn graphEdgesProgramMatchesTarget(
    program: TensorProgram,
    owner: []const u8,
) bool {
    const output_step_idx = switch (program.output) {
        .step => |idx| idx,
        .input => return false,
    };
    if (output_step_idx >= program.steps.len) return false;
    const output_expr = program.steps[output_step_idx].expr;
    return output_expr.fragment == .graph_traverse and
        output_expr.layout != null and output_expr.layout.? == .graph_edges and
        output_expr.owner != null and std.mem.eql(u8, output_expr.owner.?, owner) and
        output_expr.law_id != null and output_expr.law_id.? == .provenance_semiring and
        std.mem.eql(Dimension, output_expr.output_dims, &.{ .src, .dst });
}

pub fn tensorProgramStepUsesSemanticInput(
    program: TensorProgram,
    step_idx: usize,
    semantic_id: []const u8,
    output_dims: []const Dimension,
) bool {
    if (step_idx >= program.steps.len) return false;
    for (program.steps[step_idx].inputs) |input_ref| {
        switch (input_ref) {
            .input => |input_idx| {
                if (input_idx < program.inputs.len and tensorExprMatchesSemanticInput(program.inputs[input_idx], semantic_id, output_dims)) return true;
            },
            .step => {},
        }
    }
    return false;
}

pub fn tensorExprMatchesSemanticInput(expr: TensorExpr, semantic_id: []const u8, output_dims: []const Dimension) bool {
    return expr.semantic_id != null and
        std.mem.eql(u8, expr.semantic_id.?, semantic_id) and
        expr.fragment == .slice and
        std.mem.eql(Dimension, expr.output_dims, output_dims);
}

fn tensorProgramRefValid(program: TensorProgram, ref: TensorProgramRef, current_step: usize) bool {
    return switch (ref) {
        .input => |idx| idx < program.inputs.len,
        .step => |idx| idx < current_step,
    };
}

pub fn selectUniqueAccessPath(paths: []const PhysicalAccessPath, expr: TensorExpr) ?AccessPathPlan {
    var selected: ?AccessPathPlan = null;
    var selected_count: usize = 0;
    for (paths, 0..) |path, i| {
        if (!accessPathCanSatisfy(path, expr).safe()) continue;
        selected = .{ .index = i, .access_path = path };
        selected_count += 1;
    }
    if (selected_count == 1) return selected;
    return null;
}

pub fn planMaterializedExpressionAlloc(alloc: std.mem.Allocator, expr: TensorExpr) !?MaterializedExpressionPlan {
    const law_id = expr.law_id orelse return null;
    if (expr.layout) |layout| {
        if (layout != .materialized_expr) return null;
    } else return null;

    const expr_id = try tensorExprIdAlloc(alloc, expr);
    errdefer alloc.free(expr_id);
    const law_ids = try alloc.alloc(law_mod.Id, 1);
    errdefer alloc.free(law_ids);
    law_ids[0] = law_id;
    const access_path = materializedExpressionAccessPath(expr_id, expr.output_dims, law_ids, expr.dictionary);
    if (!accessPathCanSatisfy(access_path, expr).safe()) {
        alloc.free(law_ids);
        alloc.free(expr_id);
        return null;
    }
    return .{
        .expr_id = expr_id,
        .law_ids = law_ids,
        .access_path = access_path,
    };
}

const lexical_slice_fragments = [_]TensorFragment{.slice};
const lexical_select_fragments = [_]TensorFragment{ .slice, .automaton_select };
const vector_search_fragments = [_]TensorFragment{ .vector_search, .slice };
const graph_traverse_fragments = [_]TensorFragment{ .graph_traverse, .slice };
const materialized_expr_fragments = [_]TensorFragment{ .slice, .reduce, .merge };
const doc_output_dims = [_]Dimension{.doc};
const doc_score_output_dims = [_]Dimension{ .doc, .score };
const doc_term_score_output_dims = [_]Dimension{ .doc, .term, .score };
const graph_edge_output_dims = [_]Dimension{ .src, .dst };
const graph_reachability_output_dims = [_]Dimension{.doc};
const doc_fact_output_dims = [_]Dimension{ .doc, .field, .scalar };
const path_fact_output_dims = [_]Dimension{ .doc, .path, .kind, .scalar };

pub fn lexicalAccessPath(owner: []const u8, layout: PhysicalLayout, dictionary: lexical.DictionaryIdentity, supports_automata: bool) PhysicalAccessPath {
    return .{
        .owner = owner,
        .layout = layout,
        .fragments = if (supports_automata) &lexical_select_fragments else &lexical_slice_fragments,
        .output_dims = &doc_output_dims,
        .dictionary = dictionary,
    };
}

pub fn vectorAccessPath(owner: []const u8, layout: PhysicalLayout) PhysicalAccessPath {
    return .{
        .owner = owner,
        .layout = layout,
        .fragments = &vector_search_fragments,
        .output_dims = &doc_score_output_dims,
    };
}

pub fn sparseTokenAccessPath(
    owner: []const u8,
    scope: []const u8,
    field: []const u8,
    token_space: []const u8,
    value_kind: []const u8,
    supports_automata: bool,
) PhysicalAccessPath {
    return .{
        .owner = owner,
        .layout = .sparse_token_postings,
        .fragments = if (supports_automata) &lexical_select_fragments else &lexical_slice_fragments,
        .output_dims = &doc_term_score_output_dims,
        .dictionary = lexical.DictionaryIdentity.sparseToken(scope, field, token_space, value_kind),
    };
}

pub fn graphEdgeAccessPath(owner: []const u8) PhysicalAccessPath {
    return .{
        .owner = owner,
        .layout = .graph_edges,
        .fragments = &graph_traverse_fragments,
        .output_dims = &graph_edge_output_dims,
        .law_ids = &.{.provenance_semiring},
    };
}

pub fn graphReachabilityAccessPath(owner: []const u8) PhysicalAccessPath {
    return .{
        .owner = owner,
        .layout = .graph_edges,
        .fragments = &graph_traverse_fragments,
        .output_dims = &graph_reachability_output_dims,
        .law_ids = &.{.provenance_semiring},
    };
}

pub fn docFactAccessPath(owner: []const u8) PhysicalAccessPath {
    return .{
        .owner = owner,
        .layout = .docfact_rows,
        .fragments = &lexical_slice_fragments,
        .output_dims = &doc_fact_output_dims,
    };
}

pub fn pathFactAccessPath(owner: []const u8) PhysicalAccessPath {
    return .{
        .owner = owner,
        .layout = .pathfact_rows,
        .fragments = &lexical_slice_fragments,
        .output_dims = &path_fact_output_dims,
    };
}

pub fn materializedExpressionAccessPath(owner: []const u8, output_dims: []const Dimension, law_ids: []const law_mod.Id, dictionary: ?lexical.DictionaryIdentity) PhysicalAccessPath {
    return .{
        .owner = owner,
        .layout = .materialized_expr,
        .fragments = &materialized_expr_fragments,
        .output_dims = output_dims,
        .law_ids = law_ids,
        .dictionary = dictionary,
    };
}

fn containsFragment(haystack: []const TensorFragment, needle: TensorFragment) bool {
    for (haystack) |item| {
        if (item == needle) return true;
    }
    return false;
}

fn containsDimension(haystack: []const Dimension, needle: Dimension) bool {
    for (haystack) |item| {
        if (item == needle) return true;
    }
    return false;
}

fn containsLaw(haystack: []const law_mod.Id, needle: law_mod.Id) bool {
    for (haystack) |item| {
        if (item == needle) return true;
    }
    return false;
}

test "algebraic IR can represent temporal join aggregation query" {
    const constraints = [_]Constraint{.{ .field = "tenant", .value = "t1" }};
    const metrics = [_]Metric{.{ .name = "amount_by_segment", .op = .sum, .field = "amount" }};
    const query = Query{
        .kind = .terms,
        .aggregation_name = "count_by_segment",
        .bucket_field = "segment",
        .constraints = constraints[0..],
        .child_metrics = metrics[0..],
        .join = .{
            .name = "orders_customers",
            .kind = joinKind("day", 3600),
            .group_side = "right",
            .measure_side = "left",
        },
    };

    try std.testing.expectEqual(QueryKind.terms, query.kind);
    try std.testing.expectEqual(join_mod.TemporalMode.bucket_window, query.join.?.kind);
    try std.testing.expectEqual(@as(usize, 1), query.child_metrics.len);
}

test "typed tensor IR proves dictionary access paths by semantic identity" {
    const canonical_customer = lexical.DictionaryIdentity.canonicalScalar("docs", "/customer", .string, "json-scalar-v1", "kind-qualified");
    const path = lexicalAccessPath("algebraic-path-promotion", .dictionary_postings, canonical_customer, true);
    const prefix_expr = TensorExpr{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = canonical_customer,
    };
    try std.testing.expect(accessPathCanSatisfy(path, prefix_expr).safe());

    const analyzed_text_expr = TensorExpr{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = lexical.DictionaryIdentity.analyzedText("docs", "customer", "default"),
    };
    try std.testing.expectEqual(
        AccessPathRejectReason.dictionary_mismatch,
        accessPathCanSatisfy(path, analyzed_text_expr).rejected,
    );
}

test "typed tensor IR gives expressions stable semantic ids" {
    const alloc = std.testing.allocator;
    const dictionary = lexical.DictionaryIdentity.canonicalScalar("docs", "/customer", .string, "json-scalar-v1", "kind-qualified");
    const expr = TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .layout = .materialized_expr,
        .dictionary = dictionary,
        .law_id = .sum,
    };

    const first = try tensorExprIdAlloc(alloc, expr);
    defer alloc.free(first);
    const second = try tensorExprIdAlloc(alloc, expr);
    defer alloc.free(second);
    try std.testing.expectEqualStrings(first, second);

    var different_law = expr;
    different_law.law_id = .count;
    const law_id = try tensorExprIdAlloc(alloc, different_law);
    defer alloc.free(law_id);
    try std.testing.expect(!std.mem.eql(u8, first, law_id));

    var different_dictionary = expr;
    different_dictionary.dictionary = lexical.DictionaryIdentity.analyzedText("docs", "customer", "standard");
    const dictionary_id = try tensorExprIdAlloc(alloc, different_dictionary);
    defer alloc.free(dictionary_id);
    try std.testing.expect(!std.mem.eql(u8, first, dictionary_id));

    var different_semantic = expr;
    different_semantic.semantic_id = "tax_by_customer";
    const semantic_id = try tensorExprIdAlloc(alloc, different_semantic);
    defer alloc.free(semantic_id);
    try std.testing.expect(!std.mem.eql(u8, first, semantic_id));
    try std.testing.expect(accessPathCanSatisfy(
        materializedExpressionAccessPath(semantic_id, &.{.bucket}, &.{.sum}, different_semantic.dictionary),
        different_semantic,
    ).safe());

    const path = materializedExpressionAccessPath(first, &.{.bucket}, &.{.sum}, expr.dictionary);
    try std.testing.expect(accessPathCanSatisfy(path, expr).safe());
    var planned = (try planMaterializedExpressionAlloc(alloc, expr)).?;
    defer planned.deinit(alloc);
    try std.testing.expectEqualStrings(first, planned.expr_id);
    try std.testing.expect(accessPathCanSatisfy(planned.access_path, expr).safe());

    var missing_layout = expr;
    missing_layout.layout = null;
    try std.testing.expect((try planMaterializedExpressionAlloc(alloc, missing_layout)) == null);

    try std.testing.expectEqual(
        AccessPathRejectReason.owner_mismatch,
        accessPathCanSatisfy(path, .{
            .fragment = .reduce,
            .output_dims = &.{.bucket},
            .owner = law_id,
            .layout = .materialized_expr,
            .law_id = .sum,
        }).rejected,
    );
    try std.testing.expectEqual(
        AccessPathRejectReason.layout_mismatch,
        accessPathCanSatisfy(path, .{
            .fragment = .reduce,
            .output_dims = &.{.bucket},
            .layout = .materialized_tensor,
            .law_id = .sum,
        }).rejected,
    );
}

test "typed tensor IR gives expression programs stable ids" {
    const alloc = std.testing.allocator;
    const dictionary = lexical.DictionaryIdentity.analyzedText("docs", "body", "default");
    const input = TensorExpr{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = dictionary,
    };
    const reduce = TensorProgramStep{
        .expr = .{
            .fragment = .reduce,
            .input_dims = &.{.doc},
            .output_dims = &.{.bucket},
            .law_id = .count,
        },
        .inputs = &.{.{ .input = 0 }},
    };
    const program = TensorProgram{
        .inputs = &.{input},
        .steps = &.{reduce},
        .output = .{ .step = 0 },
    };

    const first = try tensorProgramIdAlloc(alloc, program);
    defer alloc.free(first);
    const second = try tensorProgramIdAlloc(alloc, program);
    defer alloc.free(second);
    try std.testing.expectEqualStrings(first, second);

    var different_reduce = reduce;
    different_reduce.expr.law_id = .sum;
    const different = try tensorProgramIdAlloc(alloc, .{
        .inputs = &.{input},
        .steps = &.{different_reduce},
        .output = .{ .step = 0 },
    });
    defer alloc.free(different);
    try std.testing.expect(!std.mem.eql(u8, first, different));
}

test "typed tensor IR proves bounded expression programs conservatively" {
    const alloc = std.testing.allocator;
    const dictionary = lexical.DictionaryIdentity.analyzedText("docs", "body", "default");
    const lexical_expr = TensorExpr{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = dictionary,
    };
    const paths = [_]PhysicalAccessPath{
        lexicalAccessPath("full-text-body-default", .full_text_postings, dictionary, true),
    };
    const accepted = TensorProgram{
        .steps = &.{
            .{ .expr = lexical_expr },
            .{
                .expr = .{
                    .fragment = .reduce,
                    .input_dims = &.{.doc},
                    .output_dims = &.{.bucket},
                    .law_id = .count,
                },
                .inputs = &.{.{ .step = 0 }},
            },
        },
        .output = .{ .step = 1 },
        .outputs = &.{ .{ .step = 0 }, .{ .step = 1 } },
    };
    try std.testing.expect((try tensorProgramProof(alloc, &paths, accepted)).safe());
    const accepted_id = try tensorProgramIdAlloc(alloc, accepted);
    defer alloc.free(accepted_id);
    var single_output_program = accepted;
    single_output_program.outputs = &.{};
    const single_output_id = try tensorProgramIdAlloc(alloc, single_output_program);
    defer alloc.free(single_output_id);
    try std.testing.expect(!std.mem.eql(u8, accepted_id, single_output_id));

    var forward_ref = accepted;
    forward_ref.steps = &.{
        accepted.steps[0],
        .{
            .expr = accepted.steps[1].expr,
            .inputs = &.{.{ .step = 2 }},
        },
    };
    const forward_proof = try tensorProgramProof(alloc, &paths, forward_ref);
    try std.testing.expectEqual(TensorProgramRejectReason.invalid_step_ref, forward_proof.rejected.reason);
    try std.testing.expectEqual(@as(?usize, 1), forward_proof.rejected.step_index);
    var bad_extra_output = accepted;
    bad_extra_output.outputs = &.{.{ .step = 2 }};
    const bad_extra_output_proof = try tensorProgramProof(alloc, &paths, bad_extra_output);
    try std.testing.expectEqual(TensorProgramRejectReason.invalid_output_ref, bad_extra_output_proof.rejected.reason);

    var no_law = accepted;
    no_law.steps = &.{
        accepted.steps[0],
        .{
            .expr = .{
                .fragment = .reduce,
                .input_dims = &.{.doc},
                .output_dims = &.{.bucket},
            },
            .inputs = &.{.{ .step = 0 }},
        },
    };
    const no_law_proof = try tensorProgramProof(alloc, &paths, no_law);
    try std.testing.expectEqual(TensorProgramRejectReason.derived_law_required, no_law_proof.rejected.reason);

    const unplannable_leaf = TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .layout = .dense_vector,
                .output_dims = &.{ .doc, .score },
            },
        }},
        .output = .{ .step = 0 },
    };
    const unplannable_proof = try tensorProgramProof(alloc, &paths, unplannable_leaf);
    try std.testing.expectEqual(TensorProgramRejectReason.unplannable_leaf, unplannable_proof.rejected.reason);

    const vector_path = vectorAccessPath("dense-v1", .dense_vector);
    const candidate_input = TensorExpr{
        .fragment = .slice,
        .output_dims = &.{.doc},
        .semantic_id = "native_doc_id_constraints",
    };
    const constrained_vector = TensorProgram{
        .inputs = &.{candidate_input},
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .input_dims = &.{.doc},
                .output_dims = &.{ .doc, .score },
                .owner = "dense-v1",
                .layout = .dense_vector,
            },
            .inputs = &.{.{ .input = 0 }},
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expect((try tensorProgramProof(alloc, &.{vector_path}, constrained_vector)).safe());
    try std.testing.expect(vectorSearchProgramMatchesTarget(constrained_vector, "dense-v1", .dense_vector, true));
    try std.testing.expect(!vectorSearchProgramMatchesTarget(constrained_vector, "dense-v1", .dense_vector, false));
    const missing_vector_proof = try tensorProgramProof(alloc, &paths, constrained_vector);
    try std.testing.expectEqual(TensorProgramRejectReason.unplannable_leaf, missing_vector_proof.rejected.reason);

    const unconstrained_vector = TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "dense-v1",
                .layout = .dense_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expect(vectorSearchProgramMatchesTarget(unconstrained_vector, "dense-v1", .dense_vector, false));
    try std.testing.expect(!vectorSearchProgramMatchesTarget(unconstrained_vector, "dense-v1", .dense_vector, true));

    const graph_path = graphReachabilityAccessPath("graph-v1");
    const target_input = TensorExpr{
        .fragment = .slice,
        .output_dims = &.{.doc},
        .semantic_id = "graph_target_constraints",
    };
    const constrained_graph = TensorProgram{
        .inputs = &.{target_input},
        .steps = &.{.{
            .expr = .{
                .fragment = .graph_traverse,
                .input_dims = &.{.doc},
                .output_dims = &.{.doc},
                .owner = "graph-v1",
                .layout = .graph_edges,
                .law_id = .provenance_semiring,
            },
            .inputs = &.{.{ .input = 0 }},
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expect((try tensorProgramProof(alloc, &.{graph_path}, constrained_graph)).safe());
    try std.testing.expect(graphTraversalProgramMatchesTarget(constrained_graph, "graph-v1", true));
    try std.testing.expect(!graphTraversalProgramMatchesTarget(constrained_graph, "graph-v1", false));

    const unconstrained_graph = TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .graph_traverse,
                .output_dims = &.{.doc},
                .owner = "graph-v1",
                .layout = .graph_edges,
                .law_id = .provenance_semiring,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expect(graphTraversalProgramMatchesTarget(unconstrained_graph, "graph-v1", false));
    try std.testing.expect(!graphTraversalProgramMatchesTarget(unconstrained_graph, "graph-v1", true));

    var wrong_graph_law = unconstrained_graph;
    wrong_graph_law.steps = &.{.{
        .expr = .{
            .fragment = .graph_traverse,
            .output_dims = &.{.doc},
            .owner = "graph-v1",
            .layout = .graph_edges,
            .law_id = .count,
        },
    }};
    try std.testing.expect(!graphTraversalProgramMatchesTarget(wrong_graph_law, "graph-v1", false));

    const graph_edges = TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .graph_traverse,
                .output_dims = &.{ .src, .dst },
                .owner = "graph-v1",
                .layout = .graph_edges,
                .law_id = .provenance_semiring,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expect((try tensorProgramProof(alloc, &.{graphEdgeAccessPath("graph-v1")}, graph_edges)).safe());
    try std.testing.expect(graphEdgesProgramMatchesTarget(graph_edges, "graph-v1"));
    try std.testing.expect(!graphEdgesProgramMatchesTarget(graph_edges, "other-graph"));
    try std.testing.expect(!graphTraversalProgramMatchesTarget(graph_edges, "graph-v1", false));
}

test "typed tensor IR lexical access paths advertise exact and automaton candidates" {
    const body_terms = lexical.DictionaryIdentity.analyzedText("docs", "body", "default");
    const full_text = lexicalAccessPath("full-text-body-default", .full_text_postings, body_terms, true);
    try std.testing.expect(accessPathCanSatisfy(full_text, .{
        .fragment = .slice,
        .output_dims = &.{.doc},
        .dictionary = body_terms,
    }).safe());
    try std.testing.expect(accessPathCanSatisfy(full_text, .{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = body_terms,
    }).safe());

    const exact_only = lexicalAccessPath("scalar-row-lookup", .path_lookup_rows, body_terms, false);
    try std.testing.expectEqual(
        AccessPathRejectReason.missing_fragment,
        accessPathCanSatisfy(exact_only, .{
            .fragment = .automaton_select,
            .output_dims = &.{.doc},
            .dictionary = body_terms,
        }).rejected,
    );

    const sparse_tokens = lexical.DictionaryIdentity.sparseToken("sparse-v1", "body", "splade-v1", "u32");
    const sparse_path = sparseTokenAccessPath("sparse-v1", "sparse-v1", "body", "splade-v1", "u32", false);
    try std.testing.expectEqual(PhysicalLayout.sparse_token_postings, sparse_path.layout);
    try std.testing.expect(accessPathCanSatisfy(sparse_path, .{
        .fragment = .slice,
        .layout = .sparse_token_postings,
        .output_dims = &.{ .doc, .term, .score },
        .dictionary = sparse_tokens,
    }).safe());
    try std.testing.expectEqual(
        AccessPathRejectReason.missing_fragment,
        accessPathCanSatisfy(sparse_path, .{
            .fragment = .automaton_select,
            .layout = .sparse_token_postings,
            .output_dims = &.{.doc},
            .dictionary = sparse_tokens,
        }).rejected,
    );
}

test "typed tensor IR selects only unambiguous physical access paths" {
    const customer = lexical.DictionaryIdentity.canonicalScalar("docs", "/customer", .string, "json-scalar-v1", "kind-qualified");
    const region = lexical.DictionaryIdentity.canonicalScalar("docs", "/region", .string, "json-scalar-v1", "kind-qualified");
    const paths = [_]PhysicalAccessPath{
        lexicalAccessPath("customer-fst", .dictionary_postings, customer, true),
        lexicalAccessPath("region-fst", .dictionary_postings, region, true),
    };
    const customer_plan = selectUniqueAccessPath(&paths, .{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = customer,
    }).?;
    try std.testing.expectEqual(@as(usize, 0), customer_plan.index);
    try std.testing.expectEqualStrings("customer-fst", customer_plan.access_path.owner);

    const ambiguous = [_]PhysicalAccessPath{
        lexicalAccessPath("customer-fst-a", .dictionary_postings, customer, true),
        lexicalAccessPath("customer-fst-b", .dictionary_postings, customer, true),
    };
    try std.testing.expect(selectUniqueAccessPath(&ambiguous, .{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = customer,
    }) == null);

    const named_plan = selectUniqueAccessPath(&ambiguous, .{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .owner = "customer-fst-b",
        .dictionary = customer,
    }).?;
    try std.testing.expectEqual(@as(usize, 1), named_plan.index);
    try std.testing.expectEqual(
        AccessPathRejectReason.owner_mismatch,
        accessPathCanSatisfy(ambiguous[0], .{
            .fragment = .automaton_select,
            .output_dims = &.{.doc},
            .owner = "customer-fst-b",
            .dictionary = customer,
        }).rejected,
    );
}

test "typed tensor IR advertises vector and graph physical capabilities" {
    const dense = vectorAccessPath("dense-embedding", .dense_vector);
    try std.testing.expect(accessPathCanSatisfy(dense, .{
        .fragment = .vector_search,
        .layout = .dense_vector,
        .output_dims = &.{ .doc, .score },
    }).safe());
    try std.testing.expectEqual(
        AccessPathRejectReason.layout_mismatch,
        accessPathCanSatisfy(dense, .{
            .fragment = .vector_search,
            .layout = .sparse_vector,
            .output_dims = &.{ .doc, .score },
        }).rejected,
    );
    try std.testing.expectEqual(
        AccessPathRejectReason.missing_output_dimension,
        accessPathCanSatisfy(dense, .{
            .fragment = .vector_search,
            .output_dims = &.{.src},
        }).rejected,
    );

    const graph = graphReachabilityAccessPath("citations");
    try std.testing.expect(accessPathCanSatisfy(graph, .{
        .fragment = .graph_traverse,
        .output_dims = &.{.doc},
        .law_id = .provenance_semiring,
    }).safe());
    try std.testing.expectEqual(
        AccessPathRejectReason.law_required,
        accessPathCanSatisfy(graph, .{
            .fragment = .graph_traverse,
            .output_dims = &.{.doc},
            .law_id = .sum,
        }).rejected,
    );
}

test "typed tensor IR gates reductions by declared law and output dimensions" {
    const aggregate_tensor = PhysicalAccessPath{
        .owner = "adaptive-tensor",
        .layout = .materialized_tensor,
        .fragments = &.{ .slice, .reduce, .merge },
        .output_dims = &.{ .bucket, .scalar },
        .law_ids = &.{ .count, .sum },
    };

    try std.testing.expect(accessPathCanSatisfy(aggregate_tensor, .{
        .fragment = .reduce,
        .output_dims = &.{.bucket},
        .law_id = .sum,
    }).safe());
    try std.testing.expectEqual(
        AccessPathRejectReason.law_required,
        accessPathCanSatisfy(aggregate_tensor, .{
            .fragment = .reduce,
            .output_dims = &.{.bucket},
            .law_id = .max,
        }).rejected,
    );
    try std.testing.expectEqual(
        AccessPathRejectReason.missing_output_dimension,
        accessPathCanSatisfy(aggregate_tensor, .{
            .fragment = .reduce,
            .output_dims = &.{.doc},
            .law_id = .count,
        }).rejected,
    );
}
