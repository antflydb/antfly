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

//! Local-query request and response contracts shared by routing and storage.

const std = @import("std");
const graph_mod = @import("../graph/graph.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const distributed_graph = @import("distributed_graph.zig");
const platform_time = @import("antfly_platform").time;
const distributed_stats_mod = @import("../search/distributed_stats.zig");
const db_mod = @import("../storage/db/control_root.zig");

pub const algebraic_ir = db_mod.algebraic.ir;

pub fn checkQueryDeadline(req: db_mod.types.SearchRequest) !void {
    if (req.cancellation) |value| {
        if (value.isCancelled()) return error.Cancelled;
    }
    const deadline_ns = req.execution_deadline_ns orelse return;
    if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
}

pub const algebraic_law = db_mod.algebraic.law;

pub const algebraic_planner = db_mod.algebraic.planner;

pub const TextStatsRequestMode = enum {
    query_request,
    explicit_fields,
    background_fields,
};

pub const OwnedTextStatsFieldRequest = struct {
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: [][]const u8 = &.{},

    pub fn deinit(self: *OwnedTextStatsFieldRequest, alloc: std.mem.Allocator) void {
        if (self.index_name) |index_name| alloc.free(index_name);
        alloc.free(self.field);
        for (self.terms) |term| alloc.free(term);
        if (self.terms.len > 0) alloc.free(self.terms);
        self.* = undefined;
    }
};

pub const OwnedBackgroundTextStatsFieldRequest = struct {
    aggregation_name: []const u8,
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: [][]const u8 = &.{},
    background_query: db_mod.aggregations.BackgroundQuery,

    pub fn deinit(self: *OwnedBackgroundTextStatsFieldRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.aggregation_name);
        if (self.index_name) |index_name| alloc.free(index_name);
        alloc.free(self.field);
        for (self.terms) |term| alloc.free(term);
        if (self.terms.len > 0) alloc.free(self.terms);
        switch (self.background_query) {
            .match_all => {},
            .match => |query| {
                alloc.free(query.field);
                alloc.free(query.text);
            },
            .term => |query| {
                alloc.free(query.field);
                alloc.free(query.term);
            },
        }
        self.* = undefined;
    }
};

pub const TextStatsFieldRequestInput = struct {
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: []const []const u8,
};

pub const BackgroundTextStatsFieldRequestInput = struct {
    aggregation_name: []const u8,
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: []const []const u8,
    background_query: std.json.Value,
};

pub const TextStatsRequestInput = struct {
    _identity_read_generation: ?u64 = null,
    _resolved_doc_filter: ?std.json.Value = null,
    query_request: ?std.json.Value = null,
    fields: ?[]const TextStatsFieldRequestInput = null,
    background_fields: ?[]const BackgroundTextStatsFieldRequestInput = null,
};

pub const AlgebraicPartialsRequestInput = struct {
    index_name: ?[]const u8 = null,
    _identity_read_generation: ?u64 = null,
    tensor_access_paths: ?[]const AlgebraicTensorAccessPathInput = null,
    tensor_exprs: ?[]const AlgebraicTensorExprInput = null,
    tensor_program: ?AlgebraicTensorProgramInput = null,
    cardinality: ?std.json.Value = null,
    terms_cardinality: ?std.json.Value = null,
    range_cardinality: ?std.json.Value = null,
    histogram_cardinality: ?std.json.Value = null,
};

pub const AlgebraicTensorAccessPathInput = query_contract.AlgebraicTensorAccessPathEnvelopeInput;

pub const AlgebraicTensorExprInput = query_contract.AlgebraicTensorExprEnvelopeInput;

pub const AlgebraicTensorProgramInput = query_contract.AlgebraicTensorProgramEnvelopeInput;

pub const ParsedAlgebraicPartialsRequest = struct {
    index_name: ?[]u8 = null,
    identity_read_generation: ?u64 = null,
    tensor_access_paths: []OwnedAlgebraicTensorAccessPath = &.{},
    tensor_exprs: []OwnedAlgebraicTensorExpr = &.{},
    tensor_program: ?OwnedAlgebraicTensorProgram = null,

    pub fn deinit(self: *ParsedAlgebraicPartialsRequest, alloc: std.mem.Allocator) void {
        if (self.index_name) |value| alloc.free(value);
        for (self.tensor_access_paths) |*item| item.deinit(alloc);
        if (self.tensor_access_paths.len > 0) alloc.free(self.tensor_access_paths);
        for (self.tensor_exprs) |*item| item.deinit(alloc);
        if (self.tensor_exprs.len > 0) alloc.free(self.tensor_exprs);
        if (self.tensor_program) |*program| program.deinit(alloc);
        self.* = undefined;
    }
};

pub const OwnedAlgebraicTensorAccessPath = query_contract.OwnedAlgebraicTensorAccessPathEnvelope;

pub const OwnedAlgebraicTensorExpr = query_contract.OwnedAlgebraicTensorExprEnvelope;

pub const OwnedAlgebraicTensorProgram = query_contract.OwnedAlgebraicTensorProgramEnvelope;

pub const ParsedExplicitTextStatsRequest = struct {
    identity_read_generation: ?u64 = null,
    resolved_doc_filter: ?db_mod.doc_filter_wire.ParsedResolvedDocFilter = null,
    items: []OwnedTextStatsFieldRequest = &.{},

    pub fn deinit(self: *ParsedExplicitTextStatsRequest, alloc: std.mem.Allocator) void {
        if (self.resolved_doc_filter) |*filter| filter.deinit(alloc);
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

pub const ParsedBackgroundTextStatsRequest = struct {
    identity_read_generation: ?u64 = null,
    resolved_doc_filter: ?db_mod.doc_filter_wire.ParsedResolvedDocFilter = null,
    items: []OwnedBackgroundTextStatsFieldRequest = &.{},

    pub fn deinit(self: *ParsedBackgroundTextStatsRequest, alloc: std.mem.Allocator) void {
        if (self.resolved_doc_filter) |*filter| filter.deinit(alloc);
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

pub const ParsedTextStatsRequest = union(TextStatsRequestMode) {
    query_request: query_api.OwnedQueryRequest,
    explicit_fields: ParsedExplicitTextStatsRequest,
    background_fields: ParsedBackgroundTextStatsRequest,

    pub fn deinit(self: *ParsedTextStatsRequest, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .query_request => |*request| request.deinit(alloc),
            .explicit_fields => |*request| request.deinit(alloc),
            .background_fields => |*request| request.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub fn graphHydrateRequestHasResolvedDocFilter(req: distributed_graph.GraphHydrateRequest) bool {
    return req.resolved_doc_filter != null;
}

pub fn graphHydrateSearchRequest(req: distributed_graph.GraphHydrateRequest) db_mod.types.SearchRequest {
    return .{
        .query = .{ .match_all = {} },
        .filter_query_json = req.filter_query_json,
        .exclusion_query_json = req.exclusion_query_json,
        .include_stored = req.include_stored,
        .fields = req.fields,
        .include_all_fields = req.include_all_fields,
        .resolved_doc_filter = req.resolved_doc_filter,
        .resolved_doc_filter_wire_context = req.resolved_doc_filter_wire_context,
        .identity_read_generation = req.identity_read_generation,
        .execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms),
        .cancellation = req.cancellation,
    };
}

pub fn parseTextStatsRequest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
) !ParsedTextStatsRequest {
    var parsed = try std.json.parseFromSlice(TextStatsRequestInput, alloc, body, .{});
    defer parsed.deinit();
    if (parsed.value.query_request) |query_value| {
        if (parsed.value._resolved_doc_filter != null) return error.InvalidQueryRequest;
        const encoded_query = try std.json.Stringify.valueAlloc(alloc, query_value, .{});
        defer alloc.free(encoded_query);
        return .{ .query_request = try query_api.parseQueryRequest(alloc, null, table_name, encoded_query) };
    }
    if (parsed.value.fields) |fields_value| {
        var resolved_doc_filter = if (parsed.value._resolved_doc_filter) |filter_value|
            try db_mod.doc_filter_wire.parseFilterEnvelopeAlloc(alloc, filter_value)
        else
            null;
        errdefer if (resolved_doc_filter) |*filter| filter.deinit(alloc);
        const identity_read_generation = try identityGenerationFromTextStatsResolvedFilter(parsed.value._identity_read_generation, if (resolved_doc_filter) |*filter| filter else null);
        const items = try alloc.alloc(OwnedTextStatsFieldRequest, fields_value.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            if (items.len > 0) alloc.free(items);
        }
        for (fields_value, 0..) |field_value, i| {
            const terms = try alloc.alloc([]const u8, field_value.terms.len);
            var initialized_terms: usize = 0;
            errdefer {
                for (terms[0..initialized_terms]) |term| alloc.free(term);
                if (terms.len > 0) alloc.free(terms);
            }
            for (field_value.terms, 0..) |term_value, term_idx| {
                terms[term_idx] = try alloc.dupe(u8, term_value);
                initialized_terms += 1;
            }
            items[i] = .{
                .index_name = if (field_value.index_name) |index_name_value| try alloc.dupe(u8, index_name_value) else null,
                .field = try alloc.dupe(u8, field_value.field),
                .terms = terms,
            };
            initialized += 1;
        }
        return .{ .explicit_fields = .{
            .identity_read_generation = identity_read_generation,
            .resolved_doc_filter = resolved_doc_filter,
            .items = items,
        } };
    }
    if (parsed.value.background_fields) |fields_value| {
        var resolved_doc_filter = if (parsed.value._resolved_doc_filter) |filter_value|
            try db_mod.doc_filter_wire.parseFilterEnvelopeAlloc(alloc, filter_value)
        else
            null;
        errdefer if (resolved_doc_filter) |*filter| filter.deinit(alloc);
        const identity_read_generation = try identityGenerationFromTextStatsResolvedFilter(parsed.value._identity_read_generation, if (resolved_doc_filter) |*filter| filter else null);
        const items = try alloc.alloc(OwnedBackgroundTextStatsFieldRequest, fields_value.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            if (items.len > 0) alloc.free(items);
        }
        for (fields_value, 0..) |field_value, i| {
            const terms = try alloc.alloc([]const u8, field_value.terms.len);
            var initialized_terms: usize = 0;
            errdefer {
                for (terms[0..initialized_terms]) |term| alloc.free(term);
                if (terms.len > 0) alloc.free(terms);
            }
            for (field_value.terms, 0..) |term_value, term_idx| {
                terms[term_idx] = try alloc.dupe(u8, term_value);
                initialized_terms += 1;
            }
            items[i] = .{
                .aggregation_name = try alloc.dupe(u8, field_value.aggregation_name),
                .index_name = if (field_value.index_name) |index_name_value| try alloc.dupe(u8, index_name_value) else null,
                .field = try alloc.dupe(u8, field_value.field),
                .terms = terms,
                .background_query = try parseBackgroundQueryRequestAlloc(alloc, field_value.background_query),
            };
            initialized += 1;
        }
        return .{ .background_fields = .{
            .identity_read_generation = identity_read_generation,
            .resolved_doc_filter = resolved_doc_filter,
            .items = items,
        } };
    }
    return error.InvalidQueryRequest;
}

pub fn identityGenerationFromTextStatsResolvedFilter(
    explicit_generation: ?u64,
    resolved_doc_filter: ?*const db_mod.doc_filter_wire.ParsedResolvedDocFilter,
) !?u64 {
    const filter = resolved_doc_filter orelse return explicit_generation;
    if (explicit_generation) |generation| {
        if (generation != filter.context.identity_read_generation) return error.InvalidQueryRequest;
        return generation;
    }
    return filter.context.identity_read_generation;
}

pub fn parseAlgebraicPartialsRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !ParsedAlgebraicPartialsRequest {
    var parsed = try std.json.parseFromSlice(AlgebraicPartialsRequestInput, alloc, body, .{});
    defer parsed.deinit();
    const exprs_value = parsed.value.tensor_exprs orelse &.{};
    const has_program = parsed.value.tensor_program != null;
    const has_legacy_request = parsed.value.cardinality != null or
        parsed.value.terms_cardinality != null or
        parsed.value.range_cardinality != null or
        parsed.value.histogram_cardinality != null;
    if (has_legacy_request) return error.InvalidQueryRequest;
    if (exprs_value.len == 0 and !has_program) return error.InvalidQueryRequest;
    if (has_program and exprs_value.len > 0) return error.InvalidQueryRequest;
    const paths_value = parsed.value.tensor_access_paths orelse return error.InvalidQueryRequest;
    const expected_proof_count = if (exprs_value.len > 0) exprs_value.len else paths_value.len;
    if (paths_value.len != expected_proof_count) return error.InvalidQueryRequest;
    const tensor_access_paths = blk: {
        const paths = try alloc.alloc(OwnedAlgebraicTensorAccessPath, paths_value.len);
        var paths_initialized: usize = 0;
        errdefer {
            for (paths[0..paths_initialized]) |*item| item.deinit(alloc);
            if (paths.len > 0) alloc.free(paths);
        }
        for (paths_value, 0..) |path_value, i| {
            paths[i] = try parseAlgebraicTensorAccessPathAlloc(alloc, path_value);
            paths_initialized += 1;
        }
        break :blk paths;
    };
    errdefer {
        for (tensor_access_paths) |*item| item.deinit(alloc);
        if (tensor_access_paths.len > 0) alloc.free(tensor_access_paths);
    }
    const tensor_exprs = blk: {
        const exprs = try alloc.alloc(OwnedAlgebraicTensorExpr, exprs_value.len);
        var exprs_initialized: usize = 0;
        errdefer {
            for (exprs[0..exprs_initialized]) |*item| item.deinit(alloc);
            if (exprs.len > 0) alloc.free(exprs);
        }
        for (exprs_value, 0..) |expr_value, i| {
            exprs[i] = try query_contract.parseAlgebraicTensorExprEnvelopeInputAlloc(alloc, expr_value);
            exprs_initialized += 1;
        }
        break :blk exprs;
    };
    errdefer {
        for (tensor_exprs) |*item| item.deinit(alloc);
        if (tensor_exprs.len > 0) alloc.free(tensor_exprs);
    }
    var tensor_program: ?OwnedAlgebraicTensorProgram = null;
    errdefer if (tensor_program) |*program| program.deinit(alloc);
    if (parsed.value.tensor_program) |program_value| {
        tensor_program = try query_contract.parseAlgebraicTensorProgramEnvelopeInputAlloc(alloc, program_value);
        try validateAlgebraicProgramPartialsProof(alloc, tensor_access_paths, &tensor_program.?);
    }
    return .{
        .index_name = if (parsed.value.index_name) |name| try alloc.dupe(u8, name) else null,
        .identity_read_generation = parsed.value._identity_read_generation,
        .tensor_access_paths = tensor_access_paths,
        .tensor_exprs = tensor_exprs,
        .tensor_program = tensor_program,
    };
}

pub fn parseAlgebraicTensorAccessPathAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicTensorAccessPathInput,
) !OwnedAlgebraicTensorAccessPath {
    return try query_contract.parseAlgebraicTensorAccessPathEnvelopeInputAlloc(alloc, input);
}

pub fn encodeTextStatsResponse(alloc: std.mem.Allocator, stats: []const distributed_stats_mod.TextFieldStats) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"fields\":[");
    for (stats, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldU32(alloc, &out, &first, "global_doc_count", item.global_doc_count);
        try appendJsonFieldU64(alloc, &out, &first, "global_total_field_len", item.global_total_field_len);
        try appendJsonFieldName(alloc, &out, &first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, &out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, &out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn encodeBackgroundTextStatsResponse(
    alloc: std.mem.Allocator,
    stats: []const db_mod.aggregations.DistributedBackgroundTextStats,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"background_fields\":[");
    for (stats, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "aggregation_name", item.aggregation_name);
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldU32(alloc, &out, &first, "background_doc_count", item.background_doc_count);
        try appendJsonFieldName(alloc, &out, &first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, &out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, &out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn encodeAlgebraicPartialsResponse(
    alloc: std.mem.Allocator,
    partials: []const db_mod.algebraic.distributed.Partial,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"partials\":[");
    for (partials, 0..) |partial, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "canonical_axis", partial.canonical_axis);
        try appendJsonFieldString(alloc, &out, &first, "metric", partial.metric);
        try appendJsonFieldString(alloc, &out, &first, "law", @tagName(partial.law_id));
        const encoded_len = std.base64.standard.Encoder.calcSize(partial.value.len);
        const encoded = try alloc.alloc(u8, encoded_len);
        defer alloc.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, partial.value);
        try appendJsonFieldString(alloc, &out, &first, "value_base64", encoded);
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn parseBackgroundQueryRequestAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !db_mod.aggregations.BackgroundQuery {
    if (value == .object) {
        if (value.object.get("match_all") != null) return .{ .match_all = {} };
        if (value.object.get("match")) |match| {
            if (match == .object and match.object.count() == 1) {
                var it = match.object.iterator();
                const entry = it.next() orelse return error.InvalidQueryRequest;
                if (entry.value_ptr.* != .string) return error.InvalidQueryRequest;
                return .{ .match = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .text = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
        if (value.object.get("term")) |term| {
            if (term == .object and term.object.count() == 1) {
                var it = term.object.iterator();
                const entry = it.next() orelse return error.InvalidQueryRequest;
                if (entry.value_ptr.* != .string) return error.InvalidQueryRequest;
                return .{ .term = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .term = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
    }
    return error.InvalidQueryRequest;
}

pub fn parsedAlgebraicTensorExpressionsAlloc(
    alloc: std.mem.Allocator,
    items: []const OwnedAlgebraicTensorExpr,
) ![]algebraic_ir.TensorExpr {
    const exprs = try alloc.alloc(algebraic_ir.TensorExpr, items.len);
    errdefer if (exprs.len > 0) alloc.free(exprs);
    for (items, 0..) |*item, i| exprs[i] = item.asExpr();
    return exprs;
}

pub fn validateAlgebraicPartialsAccessPaths(
    alloc: std.mem.Allocator,
    access_paths: anytype,
    tensor_exprs: anytype,
) !void {
    if (access_paths.len == 0 or access_paths.len != tensor_exprs.len) return error.InvalidQueryRequest;
    for (access_paths, tensor_exprs) |access_path, tensor_expr| {
        const expr = algebraicTensorExprValue(tensor_expr);
        var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)) orelse return error.InvalidQueryRequest;
        defer plan.deinit(alloc);
        if (!algebraicTensorAccessPathMatches(plan.access_path, access_path)) return error.InvalidQueryRequest;
    }
}

pub fn validateAlgebraicProgramPartialsProof(
    alloc: std.mem.Allocator,
    access_paths: []OwnedAlgebraicTensorAccessPath,
    program: *const OwnedAlgebraicTensorProgram,
) !void {
    const path_values = try algebraicTensorAccessPathValuesAlloc(alloc, access_paths);
    defer if (path_values.len > 0) alloc.free(path_values);
    var view = try program.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    const proof = try algebraic_ir.tensorProgramProof(alloc, path_values, view.program);
    if (!proof.safe()) return error.InvalidQueryRequest;
}

pub fn algebraicTensorAccessPathListHas(paths: []const algebraic_ir.PhysicalAccessPath, expected: algebraic_ir.PhysicalAccessPath) bool {
    for (paths) |path| {
        if (algebraicTensorAccessPathMatches(expected, path)) return true;
    }
    return false;
}

pub fn algebraicTensorAccessPathValuesAlloc(
    alloc: std.mem.Allocator,
    access_paths: []OwnedAlgebraicTensorAccessPath,
) ![]algebraic_ir.PhysicalAccessPath {
    const out = try alloc.alloc(algebraic_ir.PhysicalAccessPath, access_paths.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (access_paths, 0..) |path, i| out[i] = path.asAccessPath();
    return out;
}

pub fn algebraicTensorAccessPathMatches(
    expected: algebraic_ir.PhysicalAccessPath,
    actual: anytype,
) bool {
    const actual_path = algebraicTensorAccessPathValue(actual);
    return std.mem.eql(u8, expected.owner, actual_path.owner) and
        expected.layout == actual_path.layout and
        optionalDictionaryEqual(expected.dictionary, actual_path.dictionary) and
        tensorFragmentSlicesEqual(expected.fragments, actual_path.fragments) and
        tensorDimensionSlicesEqual(expected.output_dims, actual_path.output_dims) and
        lawIdSlicesEqual(expected.law_ids, actual_path.law_ids);
}

pub fn algebraicTensorAccessPathValue(actual: anytype) algebraic_ir.PhysicalAccessPath {
    if (@TypeOf(actual) == algebraic_ir.PhysicalAccessPath) return actual;
    return actual.asAccessPath();
}

pub fn optionalDictionaryEqual(
    left: ?db_mod.algebraic.lexical.DictionaryIdentity,
    right: ?db_mod.algebraic.lexical.DictionaryIdentity,
) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return left.?.eql(right.?);
}

pub fn algebraicTensorExprValue(actual: anytype) algebraic_ir.TensorExpr {
    if (@TypeOf(actual) == algebraic_ir.TensorExpr) return actual;
    return actual.asExpr();
}

pub fn tensorFragmentSlicesEqual(left: []const algebraic_ir.TensorFragment, right: []const algebraic_ir.TensorFragment) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

pub fn tensorDimensionSlicesEqual(left: []const algebraic_ir.Dimension, right: []const algebraic_ir.Dimension) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

pub fn lawIdSlicesEqual(left: []const algebraic_law.Id, right: []const algebraic_law.Id) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

pub const StorageKernelPreflightWireRequest = struct {
    query_json: []const u8,
    max_work: u32 = 0,
};

pub fn appendJsonFieldU64(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: u64,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    var buf: [32]u8 = undefined;
    const rendered = try std.fmt.bufPrint(&buf, "{d}", .{value});
    try out.appendSlice(alloc, rendered);
}

pub fn appendJsonFieldName(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

pub fn appendJsonFieldString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonString(alloc, out, value);
}

pub fn appendJsonFieldU32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

pub fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

pub const indexes_openapi = @import("antfly_indexes_openapi");

pub const metadata_openapi = @import("antfly_metadata_openapi");

pub const storage_schema = @import("../storage/schema.zig");

pub const dynamic_field_capability = @import("../storage/db/dynamic_field_capability.zig");

pub const graph_edge_type = @import("../graph/edge_type.zig");

pub const graph_edge_weight = @import("../graph/edge_weight.zig");

pub const graph_node_identity = @import("../graph/node_identity.zig");

pub const graph_pattern_mod = @import("../graph/pattern.zig");

pub const graph_paths = @import("../graph/paths.zig");

pub const graph_query_mod = @import("../graph/query.zig");

pub const public_limits = @import("public_limits.zig");

pub const table_read_source = @import("table_read_source.zig");

pub const fusion_mod = @import("../search/fusion.zig");

pub const regex_mod = @import("../search/regex.zig");

pub const ObservedDynamicFieldCapabilitySet = table_read_source.ObservedDynamicFieldCapabilitySet;

pub const DynamicFieldObservationQuery = table_read_source.DynamicFieldObservationQuery;

pub const backend_current_root_generation: u64 = 0;

pub const GroupVisibleRootGenerationSource = struct {
    ptr: *anyopaque,
    visible_root_generation_for_group: *const fn (ptr: *anyopaque, group_id: u64) u64,
    reserve_root_generation_for_group: ?*const fn (ptr: *anyopaque, group_id: u64) anyerror!void = null,
    finish_root_generation_reservation: ?*const fn (ptr: *anyopaque, group_id: u64, advance: bool) void = null,

    pub const Reservation = struct {
        source: GroupVisibleRootGenerationSource,
        group_id: u64,
        active: bool = true,

        pub fn advance(self: *Reservation) void {
            if (!self.active) return;
            self.source.finish_root_generation_reservation.?(self.source.ptr, self.group_id, true);
            self.active = false;
        }

        pub fn deinit(self: *Reservation) void {
            if (!self.active) return;
            self.source.finish_root_generation_reservation.?(self.source.ptr, self.group_id, false);
            self.active = false;
        }
    };

    /// Shared LSM/HBC cache namespace for the currently visible replica root.
    /// This is advanced when local root/catalog visibility is reconciled; it is
    /// not the storage engine's physical per-write generation.
    pub fn visibleRootGenerationForGroup(self: GroupVisibleRootGenerationSource, group_id: u64) u64 {
        return self.visible_root_generation_for_group(self.ptr, group_id);
    }

    /// Reserves generation bookkeeping before a fallible publication starts.
    pub fn reserveRootGenerationForGroup(self: GroupVisibleRootGenerationSource, group_id: u64) !?Reservation {
        const reserve = self.reserve_root_generation_for_group orelse {
            if (self.finish_root_generation_reservation != null) return error.InvalidRootGenerationSource;
            return null;
        };
        if (self.finish_root_generation_reservation == null) return error.InvalidRootGenerationSource;
        try reserve(self.ptr, group_id);
        return .{ .source = self, .group_id = group_id };
    }
};

pub const AlgebraicVectorWorkerCandidate = struct {
    index_name: []const u8,
    layout: algebraic_ir.PhysicalLayout,
    query: query_contract.AlgebraicVectorWorkerQuery,
    k: u32,
};

pub fn algebraicVectorWorkerCandidateForSearchRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ?AlgebraicVectorWorkerCandidate {
    if (req.aggregations_json.len != 0 or
        req.full_text != null or
        req.filter_text != null or
        req.exclusion_text != null or
        req.full_text_queries.len != 0 or
        req.dense_queries.len != 0 or
        req.sparse_queries.len != 0 or
        req.graph_queries.len != 0 or
        req.merge_config != null or
        req.reranker != null or
        req.pruner != null or
        req.expand_strategy != null or
        req.distributed_text_stats.len != 0 or
        searchRequestHasUnserializableResolvedDocFilter(req))
    {
        return null;
    }
    if (req.filter_query_json.len != 0 and !algebraicVectorWorkerFilterJsonSupported(alloc, req.filter_query_json)) return null;
    if (req.exclusion_query_json.len != 0 and !algebraicVectorWorkerFilterJsonSupported(alloc, req.exclusion_query_json)) return null;

    if (req.dense) |dense| {
        if (req.sparse != null) return null;
        if (req.query != .match_all) return null;
        const index_name = req.index_name orelse return null;
        return .{ .index_name = index_name, .layout = .dense_vector, .query = .{ .dense = dense }, .k = dense.k };
    }
    if (req.sparse) |sparse| {
        if (req.query != .match_all) return null;
        const index_name = req.index_name orelse return null;
        return .{ .index_name = index_name, .layout = .sparse_vector, .query = .{ .sparse = sparse }, .k = sparse.k };
    }

    switch (req.query) {
        .dense_knn => |dense| {
            const index_name = req.index_name orelse return null;
            return .{ .index_name = index_name, .layout = .dense_vector, .query = .{ .dense = dense }, .k = dense.k };
        },
        .sparse_knn => |sparse| {
            const index_name = req.index_name orelse return null;
            return .{ .index_name = index_name, .layout = .sparse_vector, .query = .{ .sparse = sparse }, .k = sparse.k };
        },
        else => return null,
    }
}

pub fn algebraicVectorWorkerFilterJsonSupported(alloc: std.mem.Allocator, filter_query_json: []const u8) bool {
    if (filter_query_json.len == 0) return true;
    const constraints = algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .match_all = {} },
        .filter_query_json = filter_query_json,
    }) catch return false;
    const owned = constraints orelse return false;
    defer freeAlgebraicConstraints(alloc, owned);
    return true;
}

pub fn annotateVectorWorkerPreflight(
    alloc: std.mem.Allocator,
    summary: *db_mod.RuntimePreflightSummary,
    req: db_mod.types.SearchRequest,
) void {
    if (!searchRequestHasSingleVectorWorkerKnn(req)) return;
    summary.vector_worker_filter_constraint_count +|= vectorWorkerFilterConstraintCount(req);
    if (req.filter_text != null or
        req.exclusion_text != null or
        req.filter_query_json.len > 0 or
        req.exclusion_query_json.len > 0)
    {
        summary.vector_worker_requires_algebraic_filter_resolution = true;
    }
    if (algebraicVectorWorkerCandidateForSearchRequest(alloc, req) != null) {
        summary.vector_worker_candidate_count +|= 1;
    } else {
        summary.vector_worker_fallback_count +|= 1;
    }
}

pub fn searchRequestHasSingleVectorWorkerKnn(req: db_mod.types.SearchRequest) bool {
    var count: u32 = 0;
    if (req.dense != null) count += 1;
    if (req.sparse != null) count += 1;
    switch (req.query) {
        .dense_knn, .sparse_knn => count += 1,
        else => {},
    }
    return count == 1;
}

pub fn vectorWorkerFilterConstraintCount(req: db_mod.types.SearchRequest) u32 {
    var count: u32 = 0;
    if (req.filter_text != null) count += 1;
    if (req.exclusion_text != null) count += 1;
    if (req.filter_query_json.len > 0) count += 1;
    if (req.exclusion_query_json.len > 0) count += 1;
    if (req.filter_ids.len > 0) count += 1;
    if (req.exclude_ids.len > 0) count += 1;
    if (req.filter_doc_ids_positive or req.filter_doc_ids.len > 0) count += 1;
    if (req.exclude_doc_ids.len > 0) count += 1;
    if (searchRequestHasResolvedDocFilter(req)) count += 1;
    return count;
}

pub fn freeObservedDynamicFieldCapabilitySets(
    alloc: std.mem.Allocator,
    sets: []ObservedDynamicFieldCapabilitySet,
) void {
    for (sets) |*set| set.deinit(alloc);
    if (sets.len > 0) alloc.free(sets);
}

pub fn mergeObservedDynamicFieldCapabilitySet(
    alloc: std.mem.Allocator,
    merged: *std.ArrayListUnmanaged(ObservedDynamicFieldCapabilitySet),
    incoming: ObservedDynamicFieldCapabilitySet,
) !void {
    for (merged.items) |*existing| {
        if (!std.mem.eql(u8, existing.index_name, incoming.index_name)) continue;
        for (incoming.field_capabilities) |capability| {
            if (mergeObservedFieldCapabilityIntoSet(existing.field_capabilities, capability)) continue;
            const cloned = try storage_schema.cloneFieldCapabilityAlloc(alloc, capability);
            const old_len = existing.field_capabilities.len;
            const expanded = alloc.realloc(existing.field_capabilities, old_len + 1) catch |err| {
                storage_schema.freeOwnedFieldCapability(alloc, cloned);
                return err;
            };
            existing.field_capabilities = expanded;
            existing.field_capabilities[old_len] = cloned;
        }
        return;
    }

    {
        var new_set = ObservedDynamicFieldCapabilitySet{
            .index_name = try alloc.dupe(u8, incoming.index_name),
            .field_capabilities = &.{},
        };
        errdefer new_set.deinit(alloc);
        new_set.field_capabilities = try storage_schema.cloneFieldCapabilitiesAlloc(alloc, incoming.field_capabilities);
        try merged.append(alloc, new_set);
    }
}

pub fn mergeObservedFieldCapabilityIntoSet(
    capabilities: []storage_schema.FieldCapability,
    needle: storage_schema.FieldCapability,
) bool {
    for (capabilities) |*capability| {
        if (!fieldCapabilityAggregationKeyEqual(capability.*, needle)) continue;
        mergeObservedFieldCapability(capability, needle);
        return true;
    }
    return false;
}

pub fn fieldCapabilityAggregationKeyEqual(left: storage_schema.FieldCapability, right: storage_schema.FieldCapability) bool {
    return optionalStringsEqual(left.name, right.name) and
        optionalStringsEqual(left.field, right.field) and
        optionalStringsEqual(left.path_pattern, right.path_pattern) and
        optionalStringsEqual(left.field_pattern, right.field_pattern) and
        optionalStringsEqual(left.match_mapping_type, right.match_mapping_type) and
        optionalStringsEqual(left.emitted_name, right.emitted_name) and
        optionalStringsEqual(left.document_schema, right.document_schema) and
        left.field_type == right.field_type and
        std.mem.eql(u8, left.provenance, right.provenance) and
        optionalStringsEqual(left.analyzer, right.analyzer);
}

pub fn mergeObservedFieldCapability(
    existing: *storage_schema.FieldCapability,
    incoming: storage_schema.FieldCapability,
) void {
    existing.searchable = existing.searchable and incoming.searchable;
    existing.filterable = existing.filterable and incoming.filterable;
    existing.aggregatable = existing.aggregatable and incoming.aggregatable;
    existing.doc_values = existing.doc_values and incoming.doc_values;
    existing.sortable = existing.sortable and incoming.sortable;
    existing.doc_value_coverage = storage_schema.conservativeDocValueCoverage(existing.doc_value_coverage, incoming.doc_value_coverage);
    existing.queryability_state = storage_schema.conservativeQueryabilityState(existing.queryability_state, incoming.queryability_state);
    existing.sort_lifecycle_state = storage_schema.conservativeSortLifecycleState(existing.sort_lifecycle_state, incoming.sort_lifecycle_state);
    if (!std.mem.eql(u8, existing.missing_null_policy, incoming.missing_null_policy)) {
        existing.missing_null_policy = "mixed";
    }
    if (!indexSortMembershipEqual(existing.index_sort, incoming.index_sort)) {
        existing.index_sort = null;
    }
}

pub fn optionalStringsEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?, right.?);
}

pub fn indexSortMembershipEqual(left: ?storage_schema.IndexSortMembership, right: ?storage_schema.IndexSortMembership) bool {
    if (left == null or right == null) return left == null and right == null;
    return left.?.position == right.?.position and left.?.desc == right.?.desc;
}

pub fn cloneRuntimePreflightSummary(
    alloc: std.mem.Allocator,
    summary: db_mod.RuntimePreflightSummary,
) !db_mod.RuntimePreflightSummary {
    var cloned: db_mod.RuntimePreflightSummary = .{};
    errdefer cloned.deinit(alloc);
    try mergeRuntimePreflightSummaryNoFree(alloc, &cloned, summary);
    return cloned;
}

pub fn searchRequestHasResolvedDocFilter(req: db_mod.types.SearchRequest) bool {
    if (comptime @hasField(db_mod.types.SearchRequest, "resolved_doc_filter")) {
        return req.resolved_doc_filter != null;
    }
    return false;
}

pub fn searchRequestHasUnserializableResolvedDocFilter(req: db_mod.types.SearchRequest) bool {
    return searchRequestHasResolvedDocFilter(req) and req.resolved_doc_filter_wire_context == null;
}

pub fn mergeRuntimePreflightSummaryNoFree(
    alloc: std.mem.Allocator,
    target: *db_mod.RuntimePreflightSummary,
    extra: db_mod.RuntimePreflightSummary,
) !void {
    try mergeRuntimePreflightStrings(alloc, &target.result_refs, extra.result_refs);
    try mergeRuntimePreflightStrings(alloc, &target.graph_query_order, extra.graph_query_order);
    try mergeRuntimePreflightTextEstimates(alloc, &target.text_indexes, extra.text_indexes);
    try mergeRuntimePreflightEmbeddingEstimates(alloc, &target.embedding_indexes, extra.embedding_indexes);
    try mergeRuntimePreflightGraphEstimates(alloc, &target.graph_indexes, extra.graph_indexes);
    try mergeRuntimePreflightTextQueryStats(alloc, &target.text_query_stats, extra.text_query_stats);
    target.doc_id_value_count = @max(target.doc_id_value_count, extra.doc_id_value_count);
    target.filter_id_count = @max(target.filter_id_count, extra.filter_id_count);
    target.exclude_id_count = @max(target.exclude_id_count, extra.exclude_id_count);
    target.numeric_range_clause_count = @max(target.numeric_range_clause_count, extra.numeric_range_clause_count);
    target.term_range_clause_count = @max(target.term_range_clause_count, extra.term_range_clause_count);
    target.ip_range_clause_count = @max(target.ip_range_clause_count, extra.ip_range_clause_count);
    target.bool_field_clause_count = @max(target.bool_field_clause_count, extra.bool_field_clause_count);
    target.geo_filter_clause_count = @max(target.geo_filter_clause_count, extra.geo_filter_clause_count);
    target.positive_id_result_upper_bound = if (target.positive_id_result_upper_bound) |existing|
        if (extra.positive_id_result_upper_bound) |incoming|
            @min(existing, incoming)
        else
            existing
    else
        extra.positive_id_result_upper_bound;
    const target_pre_merge_lower_bound = if (target.structured_filter_doc_count_lower_bound) |value|
        value
    else if (target.structured_filter_count_exact)
        target.structured_filter_doc_count_estimate
    else
        target.structured_filter_doc_count_estimate;
    const extra_pre_merge_lower_bound = if (extra.structured_filter_doc_count_lower_bound) |value|
        value
    else if (extra.structured_filter_count_exact)
        extra.structured_filter_doc_count_estimate
    else
        extra.structured_filter_doc_count_estimate;
    if (target.structured_filter_count_exact and extra.structured_filter_count_exact) {
        if (target.structured_filter_doc_count_estimate) |existing| {
            if (extra.structured_filter_doc_count_estimate) |incoming| {
                target.structured_filter_doc_count_estimate = existing + incoming;
                target.structured_filter_count_exact = true;
            } else {
                target.structured_filter_doc_count_estimate = null;
                target.structured_filter_count_exact = false;
            }
        } else if (extra.structured_filter_doc_count_estimate) |incoming| {
            target.structured_filter_doc_count_estimate = incoming;
            target.structured_filter_count_exact = true;
        } else {
            target.structured_filter_doc_count_estimate = null;
            target.structured_filter_count_exact = false;
        }
    } else {
        target.structured_filter_doc_count_estimate = null;
        target.structured_filter_count_exact = false;
    }
    target.structured_filter_doc_count_sample_estimate = if (target.structured_filter_doc_count_sample_estimate) |existing|
        if (extra.structured_filter_doc_count_sample_estimate) |incoming|
            existing + incoming
        else
            existing
    else
        extra.structured_filter_doc_count_sample_estimate;
    target.structured_filter_count_sample_size += extra.structured_filter_count_sample_size;
    if (target.structured_filter_count_exact) {
        target.structured_filter_doc_count_sample_estimate = null;
        target.structured_filter_count_sample_size = 0;
    }
    if (target.structured_filter_count_exact) {
        target.structured_filter_doc_count_lower_bound = null;
    } else {
        target.structured_filter_doc_count_lower_bound = if (target_pre_merge_lower_bound != null or extra_pre_merge_lower_bound != null)
            (target_pre_merge_lower_bound orelse 0) + (extra_pre_merge_lower_bound orelse 0)
        else
            null;
    }
    target.structured_filter_count_budget_limit = if (target.structured_filter_count_budget_limit) |existing|
        if (extra.structured_filter_count_budget_limit) |incoming|
            @max(existing, incoming)
        else
            existing
    else
        extra.structured_filter_count_budget_limit;
    target.shard_result_window = @max(target.shard_result_window, extra.shard_result_window);
    target.shard_result_window_total += extra.shard_result_window_total;
    target.stored_projection_doc_upper_bound_total += extra.stored_projection_doc_upper_bound_total;
    target.rerank_doc_upper_bound = @max(target.rerank_doc_upper_bound, extra.rerank_doc_upper_bound);
    target.aggregation_may_scan_full_results = target.aggregation_may_scan_full_results or extra.aggregation_may_scan_full_results;
    target.shard_count += extra.shard_count;
    target.remote_shard_count += extra.remote_shard_count;
    target.dense_query_count += extra.dense_query_count;
    target.vector_worker_candidate_count += extra.vector_worker_candidate_count;
    target.vector_worker_fallback_count += extra.vector_worker_fallback_count;
    target.vector_worker_filter_constraint_count += extra.vector_worker_filter_constraint_count;
    target.vector_worker_requires_algebraic_filter_resolution = target.vector_worker_requires_algebraic_filter_resolution or
        extra.vector_worker_requires_algebraic_filter_resolution;
    target.dense_effective_k_total += extra.dense_effective_k_total;
    target.dense_search_width_total += extra.dense_search_width_total;
    target.dense_search_width_max = @max(target.dense_search_width_max, extra.dense_search_width_max);
    target.dense_epsilon_max = @max(target.dense_epsilon_max, extra.dense_epsilon_max);
    db_mod.deriveRuntimePreflightEstimates(target);
}

pub fn mergeRuntimePreflightTextQueryStats(
    alloc: std.mem.Allocator,
    target: *[]const distributed_stats_mod.TextFieldStats,
    extra: []const distributed_stats_mod.TextFieldStats,
) !void {
    const merged = try mergeDistributedTextStats(alloc, &[_][]const distributed_stats_mod.TextFieldStats{
        target.*,
        extra,
    });
    distributed_stats_mod.deinitTextFieldStats(alloc, target.*);
    target.* = merged;
}

pub fn mergeRuntimePreflightStrings(
    alloc: std.mem.Allocator,
    target: *[]const []const u8,
    extra: []const []const u8,
) !void {
    var items = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (items.items) |item| alloc.free(item);
        items.deinit(alloc);
    }
    for (target.*) |item| try appendUniqueRuntimePreflightString(alloc, &items, item);
    for (extra) |item| try appendUniqueRuntimePreflightString(alloc, &items, item);
    freeRuntimePreflightStringSlice(alloc, target.*);
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

pub fn appendUniqueRuntimePreflightString(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try items.append(alloc, try alloc.dupe(u8, value));
}

pub fn freeRuntimePreflightStringSlice(alloc: std.mem.Allocator, items: []const []const u8) void {
    for (items) |item| alloc.free(@constCast(item));
    if (items.len > 0) alloc.free(@constCast(items));
}

pub fn mergeRuntimePreflightTextEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.TextIndexEstimate,
    extra: []const db_mod.TextIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.TextIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .doc_count = item.doc_count,
        .chunk_backed = item.chunk_backed,
        .group_chunk_parents = item.group_chunk_parents,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name)) continue;
            existing.doc_count += item.doc_count;
            existing.chunk_backed = existing.chunk_backed or item.chunk_backed;
            existing.group_chunk_parents = existing.group_chunk_parents or item.group_chunk_parents;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .doc_count = item.doc_count,
                .chunk_backed = item.chunk_backed,
                .group_chunk_parents = item.group_chunk_parents,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

pub fn mergeRuntimePreflightEmbeddingEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.EmbeddingIndexEstimate,
    extra: []const db_mod.EmbeddingIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.EmbeddingIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .sparse = item.sparse,
        .doc_count = item.doc_count,
        .dims = item.dims,
        .chunk_backed = item.chunk_backed,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name) or existing.sparse != item.sparse) continue;
            existing.doc_count += item.doc_count;
            existing.chunk_backed = existing.chunk_backed or item.chunk_backed;
            if (existing.dims == 0) existing.dims = item.dims;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .sparse = item.sparse,
                .doc_count = item.doc_count,
                .dims = item.dims,
                .chunk_backed = item.chunk_backed,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

pub fn mergeRuntimePreflightGraphEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.GraphIndexEstimate,
    extra: []const db_mod.GraphIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.GraphIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .edge_count = item.edge_count,
        .node_count = item.node_count,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name)) continue;
            existing.edge_count += item.edge_count;
            existing.node_count += item.node_count;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .edge_count = item.edge_count,
                .node_count = item.node_count,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

pub fn algebraicConstraintsForRequestAlloc(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
) !?[]db_mod.aggregations.FixedConstraint {
    var out = std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint).empty;
    errdefer freeAlgebraicConstraints(alloc, out.items);

    switch (req.query) {
        .match_all => {},
        .term => |term| {
            if (!std.mem.startsWith(u8, term.field, "/")) return null;
            const value_text = db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term }) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, term.field, value_text) catch return null;
        },
        .match => |match| {
            if (!std.mem.startsWith(u8, match.field, "/") or match.analyzer != null or match.text.len == 0) return null;
            const value_text = db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, match.text) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, match.field, value_text) catch return null;
        },
        .fuzzy => |fuzzy| {
            if (!std.mem.startsWith(u8, fuzzy.field, "/") or fuzzy.auto_fuzzy or fuzzy.prefix_len == 0) return null;
            const prefix_len: usize = @intCast(fuzzy.prefix_len);
            if (fuzzy.term.len < prefix_len) return null;
            const value_text = db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
                alloc,
                fuzzy.term,
                fuzzy.max_edits,
                fuzzy.prefix_len,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, fuzzy.field, value_text) catch return null;
        },
        .prefix => |prefix| {
            if (!std.mem.startsWith(u8, prefix.field, "/")) return null;
            const value_text = db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, prefix.prefix) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, prefix.field, value_text) catch return null;
        },
        .wildcard => |wildcard| {
            if (!std.mem.startsWith(u8, wildcard.field, "/")) return null;
            const literal_prefix = algebraicWildcardLiteralPrefix(wildcard.pattern);
            if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(wildcard.pattern)) return null;
            const value_text = db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, wildcard.pattern) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, wildcard.field, value_text) catch return null;
        },
        .regexp => |regexp| {
            if (!std.mem.startsWith(u8, regexp.field, "/")) return null;
            if (algebraicRegexpLiteralPrefix(regexp.pattern).len == 0) return null;
            var compiled = regex_mod.compile(alloc, regexp.pattern) catch return null;
            defer compiled.deinit();
            const value_text = db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, regexp.pattern) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, regexp.field, value_text) catch return null;
        },
        .bool_field => |field| {
            const value_text = algebraicConstraintBoolValueAlloc(alloc, field.field, field.value) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, field.field, value_text) catch return null;
        },
        .numeric_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return null;
            const value_text = db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .term_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or (range.min == null and range.max == null)) return null;
            const value_text = db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .ip_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or !algebraicValidIpRange(range.cidr)) return null;
            const value_text = db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, range.cidr) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .geo_bbox => |bbox| {
            if (!std.mem.startsWith(u8, bbox.field, "/") or !algebraicValidGeoBBox(bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
                alloc,
                bbox.min_lat,
                bbox.min_lon,
                bbox.max_lat,
                bbox.max_lon,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, bbox.field, value_text) catch return null;
        },
        .geo_distance => |distance| {
            if (!std.mem.startsWith(u8, distance.field, "/") or !algebraicValidGeoDistance(distance.lat, distance.lon, distance.radius_meters)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
                alloc,
                distance.lat,
                distance.lon,
                distance.radius_meters,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, distance.field, value_text) catch return null;
        },
        .geo_shape => |shape| {
            if (!std.mem.startsWith(u8, shape.field, "/") or !algebraicGeoShapeRelationSupported(shape.relation)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
                alloc,
                @tagName(shape.relation),
                shape.polygons,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, shape.field, value_text) catch return null;
        },
        .date_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return null;
            const start_text = if (range.start_ns) |ns| std.fmt.allocPrint(alloc, "{d}", .{ns}) catch return null else null;
            defer if (start_text) |value| alloc.free(value);
            const end_text = if (range.end_ns) |ns| std.fmt.allocPrint(alloc, "{d}", .{ns}) catch return null else null;
            defer if (end_text) |value| alloc.free(value);
            const value_text = db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                range.inclusive_start,
                range.inclusive_end,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        else => return null,
    }

    if (req.full_text) |text_query| {
        if (!(collectAlgebraicTextQueryConstraints(alloc, text_query, &out) catch return null)) return null;
    }

    if (req.filter_query_json.len > 0) {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, req.filter_query_json, .{}) catch return null;
        defer parsed.deinit();
        if (!(collectAlgebraicFilterConstraints(alloc, parsed.value, &out) catch return null)) return null;
    }

    return try out.toOwnedSlice(alloc);
}

pub fn freeAlgebraicConstraints(
    alloc: std.mem.Allocator,
    constraints: []db_mod.aggregations.FixedConstraint,
) void {
    for (constraints) |constraint| {
        alloc.free(@constCast(constraint.field));
        alloc.free(@constCast(constraint.value));
    }
    if (constraints.len > 0) alloc.free(constraints);
}

pub const AlgebraicConstraintCollectError = std.mem.Allocator.Error || error{UnsupportedQueryRequest};

pub fn collectAlgebraicTextBoolQueryConstraints(
    alloc: std.mem.Allocator,
    bool_query: db_mod.types.TextBoolQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (bool_query.must_not.len > 0) return false;
    if (bool_query.should.len > 0) {
        if (bool_query.must.len > 0) {
            if (bool_query.min_should != 0) return false;
        } else {
            if (bool_query.min_should != 0 and bool_query.min_should != 1) return false;
            return try collectAlgebraicTextShouldTermConstraint(alloc, bool_query.should, out);
        }
    }
    if (bool_query.min_should > 0) return false;
    for (bool_query.must) |query| {
        if (!(try collectAlgebraicTextQueryConstraints(alloc, query, out))) return false;
    }
    return true;
}

pub fn collectAlgebraicTextShouldTermConstraint(
    alloc: std.mem.Allocator,
    queries: []const db_mod.types.TextQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (queries.len == 0) return false;
    var field: ?[]const u8 = null;
    const typed_values = try alloc.alloc([]const u8, queries.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }

    for (queries, 0..) |query, i| {
        const term = switch (query) {
            .term => |term| term,
            else => return false,
        };
        if (!std.mem.startsWith(u8, term.field, "/")) return false;
        if (field) |existing| {
            if (!std.mem.eql(u8, existing, term.field)) return false;
        } else {
            field = term.field;
        }
        typed_values[i] = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term });
        initialized += 1;
    }

    try appendAlgebraicTypedAnyConstraint(out, alloc, field orelse return false, typed_values);
    return true;
}

pub fn collectAlgebraicTextQueryConstraints(
    alloc: std.mem.Allocator,
    query: db_mod.types.TextQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    switch (query) {
        .match_all => return true,
        .term => |term| {
            if (!std.mem.startsWith(u8, term.field, "/")) return false;
            const value_text = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term });
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, term.field, value_text);
            return true;
        },
        .match => |match| {
            if (!std.mem.startsWith(u8, match.field, "/") or match.analyzer != null or match.text.len == 0) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, match.text);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, match.field, value_text);
            return true;
        },
        .fuzzy => |fuzzy| {
            if (!std.mem.startsWith(u8, fuzzy.field, "/") or fuzzy.auto_fuzzy or fuzzy.prefix_len == 0) return false;
            const prefix_len: usize = @intCast(fuzzy.prefix_len);
            if (fuzzy.term.len < prefix_len) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
                alloc,
                fuzzy.term,
                fuzzy.max_edits,
                fuzzy.prefix_len,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, fuzzy.field, value_text);
            return true;
        },
        .prefix => |prefix| {
            if (!std.mem.startsWith(u8, prefix.field, "/")) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, prefix.prefix);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, prefix.field, value_text);
            return true;
        },
        .wildcard => |wildcard| {
            if (!std.mem.startsWith(u8, wildcard.field, "/")) return false;
            const literal_prefix = algebraicWildcardLiteralPrefix(wildcard.pattern);
            if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(wildcard.pattern)) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, wildcard.pattern);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, wildcard.field, value_text);
            return true;
        },
        .regexp => |regexp| {
            if (!std.mem.startsWith(u8, regexp.field, "/")) return false;
            if (algebraicRegexpLiteralPrefix(regexp.pattern).len == 0) return false;
            var compiled = regex_mod.compile(alloc, regexp.pattern) catch return false;
            defer compiled.deinit();
            const value_text = try db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, regexp.pattern);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, regexp.field, value_text);
            return true;
        },
        .bool_field => |field| {
            const value_text = try algebraicConstraintBoolValueAlloc(alloc, field.field, field.value);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, field.field, value_text);
            return true;
        },
        .numeric_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return false;
            const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .date_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return false;
            const start_text = if (range.start_ns) |ns| try std.fmt.allocPrint(alloc, "{d}", .{ns}) else null;
            defer if (start_text) |value| alloc.free(value);
            const end_text = if (range.end_ns) |ns| try std.fmt.allocPrint(alloc, "{d}", .{ns}) else null;
            defer if (end_text) |value| alloc.free(value);
            const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                range.inclusive_start,
                range.inclusive_end,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .term_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or (range.min == null and range.max == null)) return false;
            const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .ip_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or !algebraicValidIpRange(range.cidr)) return false;
            const value_text = try db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, range.cidr);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .geo_bbox => |bbox| {
            if (!std.mem.startsWith(u8, bbox.field, "/") or !algebraicValidGeoBBox(bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
                alloc,
                bbox.min_lat,
                bbox.min_lon,
                bbox.max_lat,
                bbox.max_lon,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, bbox.field, value_text);
            return true;
        },
        .geo_distance => |distance| {
            if (!std.mem.startsWith(u8, distance.field, "/") or !algebraicValidGeoDistance(distance.lat, distance.lon, distance.radius_meters)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
                alloc,
                distance.lat,
                distance.lon,
                distance.radius_meters,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, distance.field, value_text);
            return true;
        },
        .geo_shape => |shape| {
            if (!std.mem.startsWith(u8, shape.field, "/") or !algebraicGeoShapeRelationSupported(shape.relation)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
                alloc,
                @tagName(shape.relation),
                shape.polygons,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, shape.field, value_text);
            return true;
        },
        .bool_query => |nested| return try collectAlgebraicTextBoolQueryConstraints(alloc, nested, out),
        else => return false,
    }
}

pub fn appendAlgebraicConstraint(
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
    alloc: std.mem.Allocator,
    field: []const u8,
    value: []const u8,
) AlgebraicConstraintCollectError!void {
    for (out.items) |existing| {
        if (!std.mem.eql(u8, existing.field, field)) continue;
        if (std.mem.eql(u8, existing.value, value)) return;
        return error.UnsupportedQueryRequest;
    }
    try out.append(alloc, .{
        .field = try alloc.dupe(u8, field),
        .value = try alloc.dupe(u8, value),
    });
}

pub fn collectAlgebraicFilterConstraints(
    alloc: std.mem.Allocator,
    filter: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (filter != .object) return false;
    if (filter.object.get("match_all") != null) return true;
    if (filter.object.get("term")) |term| {
        const predicate = algebraicFilterTermPredicate(term, filter.object.get("field") orelse filter.object.get("path")) orelse return false;
        const value_text = try algebraicConstraintValueTextAlloc(alloc, predicate.field, predicate.value);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.field, value_text);
        return true;
    }
    if (filter.object.get("terms")) |terms| {
        if (!(try collectSingleValueTermsConstraint(alloc, terms, out))) return false;
        return true;
    }
    if (filter.object.get("match")) |match| {
        const predicate = algebraicPathMatchPredicate(match, filter.object.get("field")) orelse return false;
        if (predicate.text.len == 0) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("bool_field")) |bool_field| {
        if (bool_field != .object) return false;
        const field = bool_field.object.get("field") orelse return false;
        const value = bool_field.object.get("value") orelse return false;
        if (field != .string or value != .bool) return false;
        const value_text = try algebraicConstraintBoolValueAlloc(alloc, field.string, value.bool);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field.string, value_text);
        return true;
    }
    if (filter.object.get("exists")) |exists| {
        const path = algebraicExistsPath(exists) orelse return false;
        if (!std.mem.startsWith(u8, path, "/")) return false;
        try appendAlgebraicConstraint(out, alloc, path, db_mod.algebraic.index.path_fact_exists_constraint_value);
        return true;
    }
    if (filter.object.get("prefix")) |prefix| {
        const predicate = algebraicPathPrefixPredicate(prefix, filter.object.get("field")) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, predicate.prefix);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("wildcard")) |wildcard| {
        const predicate = algebraicPathPatternPredicate(wildcard, "pattern") orelse return false;
        const literal_prefix = algebraicWildcardLiteralPrefix(predicate.text);
        if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(predicate.text)) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("regexp")) |regexp| {
        const predicate = algebraicPathPatternPredicate(regexp, "pattern") orelse return false;
        if (algebraicRegexpLiteralPrefix(predicate.text).len == 0) return false;
        var compiled = regex_mod.compile(alloc, predicate.text) catch return false;
        defer compiled.deinit();
        const value_text = try db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("fuzzy")) |fuzzy| {
        const predicate = algebraicPathFuzzyPredicate(fuzzy) orelse return false;
        if (predicate.query.prefix_len == 0) return false;
        const prefix_len: usize = @intCast(predicate.query.prefix_len);
        if (predicate.query.term.len < prefix_len) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
            alloc,
            predicate.query.term,
            predicate.query.max_edits,
            predicate.query.prefix_len,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("numeric_range")) |range| {
        const predicate = algebraicPathNumericRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
            alloc,
            predicate.min,
            predicate.max,
            predicate.inclusive_min,
            predicate.inclusive_max,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("date_range")) |range| {
        const predicate = algebraicPathDateRangePredicate(range) orelse return false;
        const start_text = try algebraicDateBoundTextAlloc(alloc, predicate.start);
        defer if (start_text) |value| alloc.free(value);
        const end_text = try algebraicDateBoundTextAlloc(alloc, predicate.end);
        defer if (end_text) |value| alloc.free(value);
        const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
            alloc,
            start_text,
            end_text,
            predicate.inclusive_start,
            predicate.inclusive_end,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("ip_range")) |range| {
        const predicate = algebraicPathIpRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, predicate.cidr);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_bbox")) |bbox| {
        const predicate = algebraicPathGeoBBoxPredicate(bbox) orelse return false;
        const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
            alloc,
            predicate.min_lat,
            predicate.min_lon,
            predicate.max_lat,
            predicate.max_lon,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_distance")) |distance| {
        const predicate = algebraicPathGeoDistancePredicate(distance) orelse return false;
        const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
            alloc,
            predicate.lat,
            predicate.lon,
            predicate.radius_meters,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_shape")) |shape| {
        var predicate = (try algebraicPathGeoShapePredicateAlloc(alloc, shape)) orelse return false;
        defer predicate.deinit(alloc);
        const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
            alloc,
            @tagName(predicate.relation),
            predicate.polygons,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("term_range")) |range| {
        const predicate = algebraicPathTermRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
            alloc,
            predicate.min,
            predicate.max,
            predicate.inclusive_min,
            predicate.inclusive_max,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("range")) |range| {
        if (algebraicPathStandardNumericRangePredicate(range)) |predicate| {
            const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                predicate.min,
                predicate.max,
                predicate.inclusive_min,
                predicate.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        if (algebraicPathStandardDateRangePredicate(range)) |predicate| {
            const start_text = try algebraicDateBoundTextAlloc(alloc, predicate.start);
            defer if (start_text) |value| alloc.free(value);
            const end_text = try algebraicDateBoundTextAlloc(alloc, predicate.end);
            defer if (end_text) |value| alloc.free(value);
            const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                predicate.inclusive_start,
                predicate.inclusive_end,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        if (algebraicPathStandardTermRangePredicate(range)) |predicate| {
            const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                predicate.min,
                predicate.max,
                predicate.inclusive_min,
                predicate.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        return false;
    }
    if (filter.object.get("conjuncts")) |conjuncts| {
        if (conjuncts != .array) return false;
        for (conjuncts.array.items) |item| {
            if (!(try collectAlgebraicFilterConstraints(alloc, item, out))) return false;
        }
        return true;
    }
    if (filter.object.get("disjuncts")) |disjuncts| {
        return try collectAlgebraicFilterShouldTermConstraint(alloc, disjuncts, out);
    }
    if (filter.object.get("bool")) |bool_query| {
        if (bool_query != .object) return false;
        if (bool_query.object.get("must_not") != null) return false;
        const must = bool_query.object.get("must");
        const filter_clause = bool_query.object.get("filter");
        const should = bool_query.object.get("should");
        if (should) |clause| {
            const min_should_value = bool_query.object.get("minimum_should_match") orelse bool_query.object.get("min_should");
            if (must != null or filter_clause != null) {
                if (!algebraicBoolShouldMinIsOptional(min_should_value)) return false;
            } else {
                if (!algebraicBoolShouldMinIsOne(min_should_value)) return false;
                return try collectAlgebraicFilterShouldTermConstraint(alloc, clause, out);
            }
        }
        if (must == null and filter_clause == null) return false;
        if (must) |clause| {
            if (!(try collectAlgebraicFilterConstraintClause(alloc, clause, out))) return false;
        }
        if (filter_clause) |clause| {
            if (!(try collectAlgebraicFilterConstraintClause(alloc, clause, out))) return false;
        }
        return true;
    }
    return false;
}

pub fn collectAlgebraicFilterConstraintClause(
    alloc: std.mem.Allocator,
    clause: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (clause == .array) {
        if (clause.array.items.len == 0) return false;
        for (clause.array.items) |item| {
            if (!(try collectAlgebraicFilterConstraints(alloc, item, out))) return false;
        }
        return true;
    }
    return try collectAlgebraicFilterConstraints(alloc, clause, out);
}

pub fn algebraicBoolShouldMinIsOptional(value: ?std.json.Value) bool {
    const actual = value orelse return true;
    return switch (actual) {
        .integer => |number| number == 0,
        .float => |number| number == 0.0,
        .string => |text| std.mem.eql(u8, text, "0"),
        else => false,
    };
}

pub fn algebraicBoolShouldMinIsOne(value: ?std.json.Value) bool {
    const actual = value orelse return true;
    return switch (actual) {
        .integer => |number| number == 1,
        .float => |number| number == 1.0,
        .string => |text| std.mem.eql(u8, text, "1"),
        else => false,
    };
}

pub fn collectAlgebraicFilterShouldTermConstraint(
    alloc: std.mem.Allocator,
    clause: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    const items = switch (clause) {
        .array => |array| array.items,
        else => return try collectAlgebraicFilterShouldTermItemsConstraint(alloc, &.{clause}, out),
    };
    return try collectAlgebraicFilterShouldTermItemsConstraint(alloc, items, out);
}

pub fn collectAlgebraicFilterShouldTermItemsConstraint(
    alloc: std.mem.Allocator,
    items: []const std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (items.len == 0) return false;
    var field: ?[]const u8 = null;
    const typed_values = try alloc.alloc([]const u8, items.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }

    for (items, 0..) |item, i| {
        const object = switch (item) {
            .object => |object| object,
            else => return false,
        };
        const predicate = algebraicFilterTermPredicate(object.get("term") orelse return false, object.get("field") orelse object.get("path")) orelse return false;
        if (!std.mem.startsWith(u8, predicate.field, "/")) return false;
        if (field) |existing| {
            if (!std.mem.eql(u8, existing, predicate.field)) return false;
        } else {
            field = predicate.field;
        }
        typed_values[i] = try algebraicConstraintValueTextAlloc(alloc, predicate.field, predicate.value);
        initialized += 1;
    }

    try appendAlgebraicTypedAnyConstraint(out, alloc, field orelse return false, typed_values);
    return true;
}

pub const AlgebraicFilterTermPredicate = struct {
    field: []const u8,
    value: std.json.Value,
};

pub fn algebraicFilterTermPredicate(term: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicFilterTermPredicate {
    if (term == .object) {
        if (term.object.get("field") orelse term.object.get("path")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            const value = term.object.get("term") orelse term.object.get("value") orelse return null;
            return .{ .field = field, .value = value };
        }
        if (term.object.count() == 1) {
            var it = term.object.iterator();
            const entry = it.next() orelse return null;
            return .{ .field = entry.key_ptr.*, .value = entry.value_ptr.* };
        }
    }
    const field = algebraicJsonString(sibling_field_value orelse return null) orelse return null;
    return .{ .field = field, .value = term };
}

pub fn algebraicExistsPath(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |field| field,
        .object => |object| blk: {
            const path = object.get("path") orelse object.get("field") orelse break :blk null;
            if (path != .string) break :blk null;
            break :blk path.string;
        },
        else => null,
    };
}

pub const AlgebraicPathPrefixPredicate = struct {
    path: []const u8,
    prefix: []const u8,
};

pub const AlgebraicPathTextPredicate = struct {
    path: []const u8,
    text: []const u8,
};

pub const AlgebraicFuzzyQuery = struct {
    term: []const u8,
    max_edits: u8,
    prefix_len: u8,
};

pub const AlgebraicPathFuzzyPredicate = struct {
    path: []const u8,
    query: AlgebraicFuzzyQuery,
};

pub const AlgebraicPathNumericRangePredicate = struct {
    path: []const u8,
    min: ?f64 = null,
    max: ?f64 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

pub const AlgebraicPathTermRangePredicate = struct {
    path: []const u8,
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

pub const AlgebraicPathDateRangePredicate = struct {
    path: []const u8,
    start: ?std.json.Value = null,
    end: ?std.json.Value = null,
    inclusive_start: bool = true,
    inclusive_end: bool = false,
};

pub const AlgebraicPathIpRangePredicate = struct {
    path: []const u8,
    cidr: []const u8,
};

pub const AlgebraicPathGeoBBoxPredicate = struct {
    path: []const u8,
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
};

pub const AlgebraicPathGeoDistancePredicate = struct {
    path: []const u8,
    lat: f64,
    lon: f64,
    radius_meters: f64,
};

pub const AlgebraicPathGeoShapePredicate = struct {
    path: []const u8,
    relation: db_mod.types.GeoShapeRelation,
    polygons: []const []const db_mod.types.GeoPoint,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.polygons) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (self.polygons.len > 0) alloc.free(@constCast(self.polygons));
        self.* = undefined;
    }
};

pub fn algebraicJsonString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

pub fn algebraicPathPrefixPredicate(prefix_value: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicPathPrefixPredicate {
    return switch (prefix_value) {
        .object => |object| blk: {
            if (object.get("path")) |path_value| {
                const path = algebraicJsonString(path_value) orelse break :blk null;
                if (!std.mem.startsWith(u8, path, "/")) break :blk null;
                const prefix = algebraicJsonString(object.get("value") orelse object.get("prefix") orelse break :blk null) orelse break :blk null;
                break :blk .{ .path = path, .prefix = prefix };
            }
            if (object.get("role") == null) {
                if (object.get("field")) |field_value| {
                    const field = algebraicJsonString(field_value) orelse break :blk null;
                    if (std.mem.startsWith(u8, field, "/")) {
                        const prefix = algebraicJsonString(object.get("value") orelse object.get("prefix") orelse break :blk null) orelse break :blk null;
                        break :blk .{ .path = field, .prefix = prefix };
                    }
                }
            }
            if (object.count() == 1) {
                var it = object.iterator();
                const entry = it.next() orelse break :blk null;
                if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) break :blk null;
                const prefix = algebraicJsonString(entry.value_ptr.*) orelse break :blk null;
                break :blk .{ .path = entry.key_ptr.*, .prefix = prefix };
            }
            break :blk null;
        },
        .string => |prefix| blk: {
            const field = algebraicJsonString(sibling_field_value orelse break :blk null) orelse break :blk null;
            break :blk if (std.mem.startsWith(u8, field, "/")) .{ .path = field, .prefix = prefix } else null;
        },
        else => null,
    };
}

pub fn algebraicPathFromPredicateObject(object: anytype) ?[]const u8 {
    if (object.get("path")) |path_value| {
        const path = algebraicJsonString(path_value) orelse return null;
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return path;
    }
    return null;
}

pub fn algebraicPathPatternPredicate(value: std.json.Value, pattern_field: []const u8) ?AlgebraicPathTextPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (algebraicPathFromPredicateObject(object)) |path| {
        const text = algebraicJsonString(object.get(pattern_field) orelse object.get("value") orelse return null) orelse return null;
        return .{ .path = path, .text = text };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) {
                const text = algebraicJsonString(object.get(pattern_field) orelse object.get("value") orelse return null) orelse return null;
                return .{ .path = field, .text = text };
            }
        }
    }
    if (object.count() == 1) {
        var it = object.iterator();
        const entry = it.next() orelse return null;
        if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
        const text = algebraicJsonString(entry.value_ptr.*) orelse return null;
        return .{ .path = entry.key_ptr.*, .text = text };
    }
    return null;
}

pub fn algebraicPathMatchPredicate(value: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicPathTextPredicate {
    return switch (value) {
        .object => algebraicPathPatternPredicate(value, "query"),
        .string => |text| blk: {
            const field = algebraicJsonString(sibling_field_value orelse break :blk null) orelse break :blk null;
            break :blk if (std.mem.startsWith(u8, field, "/")) .{ .path = field, .text = text } else null;
        },
        else => null,
    };
}

pub fn algebraicWildcardLiteralPrefix(pattern: []const u8) []const u8 {
    for (pattern, 0..) |ch, i| {
        if (ch == '*' or ch == '?') return pattern[0..i];
    }
    return pattern;
}

pub fn algebraicWildcardPatternHasMeta(pattern: []const u8) bool {
    return std.mem.indexOfAny(u8, pattern, "*?") != null;
}

pub fn algebraicRegexpLiteralPrefix(pattern: []const u8) []const u8 {
    for (pattern, 0..) |ch, i| {
        switch (ch) {
            '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\' => return pattern[0..i],
            else => {},
        }
    }
    return pattern;
}

pub fn algebraicJsonU8(value: std.json.Value) ?u8 {
    return switch (value) {
        .integer => |number| std.math.cast(u8, number),
        .float => |number| blk: {
            if (!std.math.isFinite(number) or @round(number) != number) break :blk null;
            const parsed: i64 = @intFromFloat(number);
            break :blk std.math.cast(u8, parsed);
        },
        else => null,
    };
}

pub fn algebraicParseFuzzyOptions(object: anytype, out: *AlgebraicFuzzyQuery) bool {
    if (object.get("max_edits")) |edits| {
        out.max_edits = algebraicJsonU8(edits) orelse return false;
    }
    if (object.get("prefix_length")) |prefix| {
        out.prefix_len = algebraicJsonU8(prefix) orelse return false;
    }
    if (object.get("auto_fuzzy")) |auto| {
        if (auto != .bool) return false;
        if (auto.bool) out.max_edits = if (out.term.len > 5) 2 else if (out.term.len > 2) 1 else 0;
    }
    return true;
}

pub fn algebraicParseFuzzyQuery(value: std.json.Value) ?AlgebraicFuzzyQuery {
    return switch (value) {
        .string => |text| .{ .term = text, .max_edits = 1, .prefix_len = 0 },
        .object => |object| blk: {
            var out = AlgebraicFuzzyQuery{
                .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse break :blk null) orelse break :blk null,
                .max_edits = 1,
                .prefix_len = 0,
            };
            if (!algebraicParseFuzzyOptions(object, &out)) break :blk null;
            break :blk out;
        },
        else => null,
    };
}

pub fn algebraicPathFuzzyPredicate(value: std.json.Value) ?AlgebraicPathFuzzyPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (algebraicPathFromPredicateObject(object)) |path| {
        var query = AlgebraicFuzzyQuery{
            .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse return null) orelse return null,
            .max_edits = 1,
            .prefix_len = 0,
        };
        if (!algebraicParseFuzzyOptions(object, &query)) return null;
        return .{ .path = path, .query = query };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) {
                var query = AlgebraicFuzzyQuery{
                    .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse return null) orelse return null,
                    .max_edits = 1,
                    .prefix_len = 0,
                };
                if (!algebraicParseFuzzyOptions(object, &query)) return null;
                return .{ .path = field, .query = query };
            }
        }
    }
    if (object.count() == 1) {
        var it = object.iterator();
        const entry = it.next() orelse return null;
        if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
        const query = algebraicParseFuzzyQuery(entry.value_ptr.*) orelse return null;
        return .{ .path = entry.key_ptr.*, .query = query };
    }
    return null;
}

pub fn algebraicOptionalBool(value: ?std.json.Value) ?bool {
    const actual = value orelse return null;
    return switch (actual) {
        .bool => |flag| flag,
        .null => null,
        else => null,
    };
}

pub fn algebraicOptionalF64(value: ?std.json.Value) ?f64 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .null => null,
        else => null,
    };
}

pub fn algebraicOptionalString(value: ?std.json.Value) ?[]const u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .string => |text| text,
        .null => null,
        else => null,
    };
}

pub fn algebraicNumericJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .integer, .float => true,
        else => false,
    };
}

pub fn algebraicStringJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .string => true,
        else => false,
    };
}

pub fn algebraicDateJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .integer => |number| number >= 0,
        .string => |text| (algebraicParseDateTimeOptionalToNs(text) catch null) != null,
        else => false,
    };
}

pub fn algebraicDateBoundTextAlloc(alloc: std.mem.Allocator, value: ?std.json.Value) !?[]u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |number| if (number >= 0) try std.fmt.allocPrint(alloc, "{d}", .{number}) else error.UnsupportedQueryRequest,
        .string => |text| if ((try algebraicParseDateTimeOptionalToNs(text)) != null) try alloc.dupe(u8, text) else error.UnsupportedQueryRequest,
        .null => null,
        else => error.UnsupportedQueryRequest,
    };
}

pub fn algebraicValidIpRange(text: []const u8) bool {
    return algebraicParseIpCidr(text) != null or algebraicParseIPv4(text) != null;
}

pub fn algebraicValidLatitude(lat: f64) bool {
    return std.math.isFinite(lat) and lat >= -90.0 and lat <= 90.0;
}

pub fn algebraicValidLongitude(lon: f64) bool {
    return std.math.isFinite(lon) and lon >= -180.0 and lon <= 180.0;
}

pub fn algebraicValidGeoBBox(min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) bool {
    return algebraicValidLatitude(min_lat) and
        algebraicValidLatitude(max_lat) and
        algebraicValidLongitude(min_lon) and
        algebraicValidLongitude(max_lon) and
        min_lat <= max_lat;
}

pub fn algebraicValidGeoDistance(lat: f64, lon: f64, radius_meters: f64) bool {
    return algebraicValidLatitude(lat) and
        algebraicValidLongitude(lon) and
        std.math.isFinite(radius_meters) and
        radius_meters >= 0;
}

pub fn algebraicGeoShapeRelationSupported(relation: db_mod.types.GeoShapeRelation) bool {
    return switch (relation) {
        .intersects, .within => true,
        .contains => false,
    };
}

pub const AlgebraicIpCidr = struct {
    network: [4]u8,
    prefix_len: u8,
};

pub fn algebraicParseIpCidr(text: []const u8) ?AlgebraicIpCidr {
    const slash_pos = std.mem.indexOfScalar(u8, text, '/') orelse return null;
    const ip = algebraicParseIPv4(text[0..slash_pos]) orelse return null;
    const prefix_len = std.fmt.parseInt(u8, text[slash_pos + 1 ..], 10) catch return null;
    if (prefix_len > 32) return null;
    const mask = algebraicIpMask(prefix_len);
    return .{
        .network = .{ ip[0] & mask[0], ip[1] & mask[1], ip[2] & mask[2], ip[3] & mask[3] },
        .prefix_len = prefix_len,
    };
}

pub fn algebraicParseIPv4(text: []const u8) ?[4]u8 {
    var parts = std.mem.splitScalar(u8, text, '.');
    var out: [4]u8 = undefined;
    var i: usize = 0;
    while (parts.next()) |part| {
        if (i >= 4 or part.len == 0) return null;
        out[i] = std.fmt.parseInt(u8, part, 10) catch return null;
        i += 1;
    }
    if (i != 4) return null;
    return out;
}

pub fn algebraicIpMask(prefix_len: u8) [4]u8 {
    var mask = [_]u8{ 0, 0, 0, 0 };
    var remaining = prefix_len;
    for (&mask) |*byte| {
        if (remaining >= 8) {
            byte.* = 0xff;
            remaining -= 8;
        } else if (remaining > 0) {
            byte.* = @as(u8, 0xff) << @intCast(8 - remaining);
            remaining = 0;
        }
    }
    return mask;
}

pub fn algebraicParseDateTimeOptionalToNs(text: []const u8) !?u64 {
    if (try algebraicParseRfc3339ToNs(text)) |ts| return ts;
    if (text.len != 10 or text[4] != '-' or text[7] != '-') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    return algebraicCivilDateTimeToNs(year, month, day, 0, 0, 0, 0);
}

pub fn algebraicParseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;
    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;
    return algebraicCivilDateTimeToNs(year, month, day, hour, minute, second, nanos);
}

pub fn algebraicCivilDateTimeToNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?u64 {
    if (month < 1 or month > 12 or day < 1 or day > 31 or hour < 0 or hour > 23 or minute < 0 or minute > 59 or second < 0 or second > 60) return null;
    const days = algebraicDaysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

pub fn algebraicDaysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) 1 else 0;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

pub fn algebraicPathIpRangePredicate(value: std.json.Value) ?AlgebraicPathIpRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const cidr = algebraicJsonString(object.get("cidr") orelse return null) orelse return null;
    if (!algebraicValidIpRange(cidr)) return null;
    if (algebraicPathFromPredicateObject(object)) |path| {
        return .{ .path = path, .cidr = cidr };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) return .{ .path = field, .cidr = cidr };
        }
    }
    return null;
}

pub fn algebraicPathGeoBBoxPredicate(value: std.json.Value) ?AlgebraicPathGeoBBoxPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const min_lat = algebraicOptionalF64(object.get("min_lat")) orelse return null;
    const min_lon = algebraicOptionalF64(object.get("min_lon")) orelse return null;
    const max_lat = algebraicOptionalF64(object.get("max_lat")) orelse return null;
    const max_lon = algebraicOptionalF64(object.get("max_lon")) orelse return null;
    if (!algebraicValidGeoBBox(min_lat, min_lon, max_lat, max_lon)) return null;
    return .{
        .path = path,
        .min_lat = min_lat,
        .min_lon = min_lon,
        .max_lat = max_lat,
        .max_lon = max_lon,
    };
}

pub fn algebraicPathGeoDistancePredicate(value: std.json.Value) ?AlgebraicPathGeoDistancePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const lat = algebraicOptionalF64(object.get("lat")) orelse return null;
    const lon = algebraicOptionalF64(object.get("lon")) orelse return null;
    const radius_meters = algebraicOptionalF64(object.get("radius_meters")) orelse return null;
    if (!algebraicValidGeoDistance(lat, lon, radius_meters)) return null;
    return .{
        .path = path,
        .lat = lat,
        .lon = lon,
        .radius_meters = radius_meters,
    };
}

pub fn algebraicPathGeoShapePredicateAlloc(alloc: std.mem.Allocator, value: std.json.Value) !?AlgebraicPathGeoShapePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const relation_text = if (object.get("relation")) |relation_value|
        algebraicJsonString(relation_value) orelse return null
    else
        "intersects";
    const relation = std.meta.stringToEnum(db_mod.types.GeoShapeRelation, relation_text) orelse return null;
    if (!algebraicGeoShapeRelationSupported(relation)) return null;
    const polygons_value = object.get("polygons") orelse object.get("polygon") orelse return null;
    const polygons = try algebraicGeoShapePolygonsAlloc(alloc, polygons_value);
    errdefer {
        for (polygons) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (polygons.len > 0) alloc.free(polygons);
    }
    return .{
        .path = path,
        .relation = relation,
        .polygons = polygons,
    };
}

pub fn algebraicGeoShapePolygonsAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const []const db_mod.types.GeoPoint {
    const array = switch (value) {
        .array => |array| array,
        else => return error.UnsupportedQueryRequest,
    };
    if (array.items.len == 0) return error.UnsupportedQueryRequest;
    const first_is_point = array.items[0] == .object;
    const polygon_count: usize = if (first_is_point) 1 else array.items.len;
    var out = try alloc.alloc([]const db_mod.types.GeoPoint, polygon_count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (out.len > 0) alloc.free(out);
    }
    if (first_is_point) {
        out[0] = try algebraicGeoShapePolygonAlloc(alloc, value);
        initialized = 1;
    } else {
        for (array.items, 0..) |item, i| {
            out[i] = try algebraicGeoShapePolygonAlloc(alloc, item);
            initialized += 1;
        }
    }
    return out;
}

pub fn algebraicGeoShapePolygonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.GeoPoint {
    const array = switch (value) {
        .array => |array| array,
        else => return error.UnsupportedQueryRequest,
    };
    if (array.items.len < 3) return error.UnsupportedQueryRequest;
    var out = try alloc.alloc(db_mod.types.GeoPoint, array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (array.items, 0..) |item, i| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.UnsupportedQueryRequest,
        };
        const lat = algebraicOptionalF64(object.get("lat")) orelse return error.UnsupportedQueryRequest;
        const lon = algebraicOptionalF64(object.get("lon")) orelse return error.UnsupportedQueryRequest;
        if (!algebraicValidLatitude(lat) or !algebraicValidLongitude(lon)) return error.UnsupportedQueryRequest;
        out[i] = .{ .lat = lat, .lon = lon };
    }
    return out;
}

pub fn algebraicPathNumericRangePredicate(value: std.json.Value) ?AlgebraicPathNumericRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const min = algebraicOptionalF64(object.get("min"));
    const max = algebraicOptionalF64(object.get("max"));
    if (min == null and max == null) return null;
    return .{
        .path = path,
        .min = min,
        .max = max,
        .inclusive_min = algebraicOptionalBool(object.get("inclusive_min")) orelse true,
        .inclusive_max = algebraicOptionalBool(object.get("inclusive_max")) orelse false,
    };
}

pub fn algebraicPathTermRangePredicate(value: std.json.Value) ?AlgebraicPathTermRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("path") != null or object.get("field") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const min = algebraicOptionalString(object.get("min"));
        const max = algebraicOptionalString(object.get("max"));
        if (min == null and max == null) return null;
        return .{
            .path = path,
            .min = min,
            .max = max,
            .inclusive_min = algebraicOptionalBool(object.get("inclusive_min")) orelse true,
            .inclusive_max = algebraicOptionalBool(object.get("inclusive_max")) orelse false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    return algebraicTermRangeFromBounds(entry.key_ptr.*, switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    });
}

pub fn algebraicPathDateRangePredicate(value: std.json.Value) ?AlgebraicPathDateRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("path") != null or object.get("field") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const start = object.get("start_ns") orelse object.get("start");
        const end = object.get("end_ns") orelse object.get("end");
        if (start == null and end == null) return null;
        if (start) |bound| if (!algebraicDateJsonValue(bound)) return null;
        if (end) |bound| if (!algebraicDateJsonValue(bound)) return null;
        return .{
            .path = path,
            .start = start,
            .end = end,
            .inclusive_start = algebraicOptionalBool(object.get("inclusive_start")) orelse true,
            .inclusive_end = algebraicOptionalBool(object.get("inclusive_end")) orelse false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicDateRangeFromBounds(entry.key_ptr.*, range_object);
}

pub const AlgebraicRangeBound = struct {
    value: std.json.Value,
    inclusive: bool,
};

pub fn algebraicSetRangeBound(found: *?AlgebraicRangeBound, value: std.json.Value, inclusive: bool) ?void {
    if (found.* != null) return null;
    found.* = .{ .value = value, .inclusive = inclusive };
}

pub fn algebraicStandardRangeLowerBound(object: std.json.ObjectMap) ?AlgebraicRangeBound {
    var found: ?AlgebraicRangeBound = null;
    if (object.get("gt")) |value| algebraicSetRangeBound(&found, value, false) orelse return null;
    if (object.get("gte")) |value| algebraicSetRangeBound(&found, value, true) orelse return null;
    return found;
}

pub fn algebraicStandardRangeUpperBound(object: std.json.ObjectMap) ?AlgebraicRangeBound {
    var found: ?AlgebraicRangeBound = null;
    if (object.get("lt")) |value| algebraicSetRangeBound(&found, value, false) orelse return null;
    if (object.get("lte")) |value| algebraicSetRangeBound(&found, value, true) orelse return null;
    return found;
}

pub fn algebraicTermRangeFromBounds(path: []const u8, object: std.json.ObjectMap) ?AlgebraicPathTermRangePredicate {
    const lower = algebraicStandardRangeLowerBound(object);
    const upper = algebraicStandardRangeUpperBound(object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicStringJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicStringJsonValue(bound.value)) return null;
    return .{
        .path = path,
        .min = if (lower) |bound| algebraicOptionalString(bound.value) else null,
        .max = if (upper) |bound| algebraicOptionalString(bound.value) else null,
        .inclusive_min = if (lower) |bound| bound.inclusive else true,
        .inclusive_max = if (upper) |bound| bound.inclusive else false,
    };
}

pub fn algebraicDateRangeFromBounds(path: []const u8, object: std.json.ObjectMap) ?AlgebraicPathDateRangePredicate {
    const lower = algebraicStandardRangeLowerBound(object);
    const upper = algebraicStandardRangeUpperBound(object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicDateJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicDateJsonValue(bound.value)) return null;
    return .{
        .path = path,
        .start = if (lower) |bound| bound.value else null,
        .end = if (upper) |bound| bound.value else null,
        .inclusive_start = if (lower) |bound| bound.inclusive else true,
        .inclusive_end = if (upper) |bound| bound.inclusive else false,
    };
}

pub fn algebraicPathStandardNumericRangePredicate(value: std.json.Value) ?AlgebraicPathNumericRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const lower = algebraicStandardRangeLowerBound(object);
        const upper = algebraicStandardRangeUpperBound(object);
        if (lower == null and upper == null) return null;
        if (lower) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
        if (upper) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
        return .{
            .path = path,
            .min = if (lower) |bound| algebraicOptionalF64(bound.value) else null,
            .max = if (upper) |bound| algebraicOptionalF64(bound.value) else null,
            .inclusive_min = if (lower) |bound| bound.inclusive else true,
            .inclusive_max = if (upper) |bound| bound.inclusive else false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    const lower = algebraicStandardRangeLowerBound(range_object);
    const upper = algebraicStandardRangeUpperBound(range_object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
    return .{
        .path = entry.key_ptr.*,
        .min = if (lower) |bound| algebraicOptionalF64(bound.value) else null,
        .max = if (upper) |bound| algebraicOptionalF64(bound.value) else null,
        .inclusive_min = if (lower) |bound| bound.inclusive else true,
        .inclusive_max = if (upper) |bound| bound.inclusive else false,
    };
}

pub fn algebraicPathStandardDateRangePredicate(value: std.json.Value) ?AlgebraicPathDateRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return algebraicDateRangeFromBounds(path, object);
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicDateRangeFromBounds(entry.key_ptr.*, range_object);
}

pub fn algebraicPathStandardTermRangePredicate(value: std.json.Value) ?AlgebraicPathTermRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return algebraicTermRangeFromBounds(path, object);
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicTermRangeFromBounds(entry.key_ptr.*, range_object);
}

pub fn collectSingleValueTermsConstraint(
    alloc: std.mem.Allocator,
    terms: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (terms != .object) return false;
    if (terms.object.count() == 1) {
        var it = terms.object.iterator();
        const entry = it.next() orelse return false;
        return try collectTermsValuesConstraint(alloc, entry.key_ptr.*, entry.value_ptr.*, out);
    }
    const field = terms.object.get("path") orelse terms.object.get("field") orelse return false;
    const values = terms.object.get("values") orelse terms.object.get("terms") orelse return false;
    if (field != .string) return false;
    return try collectTermsValuesConstraint(alloc, field.string, values, out);
}

pub fn collectTermsValuesConstraint(
    alloc: std.mem.Allocator,
    field: []const u8,
    values: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (values != .array or values.array.items.len == 0) return false;
    if (!std.mem.startsWith(u8, field, "/")) {
        if (values.array.items.len != 1) return false;
        const value_text = try algebraicConstraintValueTextAlloc(alloc, field, values.array.items[0]);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field, value_text);
        return true;
    }
    if (values.array.items.len == 1) {
        const value_text = try algebraicConstraintValueTextAlloc(alloc, field, values.array.items[0]);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field, value_text);
        return true;
    }
    const typed_values = try alloc.alloc([]const u8, values.array.items.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }
    for (values.array.items, 0..) |item, i| {
        typed_values[i] = try algebraicConstraintValueTextAlloc(alloc, field, item);
        initialized += 1;
    }
    try appendAlgebraicTypedAnyConstraint(out, alloc, field, typed_values);
    return true;
}

pub fn appendAlgebraicTypedAnyConstraint(
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
    alloc: std.mem.Allocator,
    field: []const u8,
    typed_values: []const []const u8,
) AlgebraicConstraintCollectError!void {
    if (typed_values.len == 0) return error.UnsupportedQueryRequest;
    if (typed_values.len == 1) {
        try appendAlgebraicConstraint(out, alloc, field, typed_values[0]);
        return;
    }
    const any_value = db_mod.algebraic.index.pathFactAnyConstraintValueAlloc(alloc, typed_values) catch return error.UnsupportedQueryRequest;
    defer alloc.free(any_value);
    try appendAlgebraicConstraint(out, alloc, field, any_value);
}

pub fn algebraicConstraintValueTextAlloc(alloc: std.mem.Allocator, field: []const u8, value: std.json.Value) AlgebraicConstraintCollectError![]u8 {
    const raw = switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .bool => |flag| try alloc.dupe(u8, if (flag) "true" else "false"),
        .null => if (std.mem.startsWith(u8, field, "/")) try alloc.dupe(u8, "") else return error.UnsupportedQueryRequest,
        else => return error.UnsupportedQueryRequest,
    };
    errdefer alloc.free(raw);
    if (!std.mem.startsWith(u8, field, "/")) return raw;
    const kind = switch (value) {
        .string => "string",
        .integer, .float => "number",
        .bool => "bool",
        .null => "null",
        else => unreachable,
    };
    const typed = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ kind, raw });
    alloc.free(raw);
    return typed;
}

pub fn algebraicConstraintBoolValueAlloc(alloc: std.mem.Allocator, field: []const u8, value: bool) AlgebraicConstraintCollectError![]u8 {
    const raw = if (value) "true" else "false";
    if (!std.mem.startsWith(u8, field, "/")) return try alloc.dupe(u8, raw);
    return try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "bool", raw });
}

pub fn mergeDistributedTextStats(
    alloc: std.mem.Allocator,
    groups: []const []const distributed_stats_mod.TextFieldStats,
) ![]const distributed_stats_mod.TextFieldStats {
    var fields = std.StringHashMapUnmanaged(struct {
        doc_count: u32 = 0,
        total_field_len: u64 = 0,
        terms: std.StringHashMapUnmanaged(u32) = .{},
    }){};
    defer {
        var it = fields.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.terms.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.terms.deinit(alloc);
            alloc.free(entry.key_ptr.*);
        }
        fields.deinit(alloc);
    }

    for (groups) |items| {
        for (items) |item| {
            const gop = try fields.getOrPut(alloc, item.field);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, item.field);
                gop.value_ptr.* = .{};
            }
            gop.value_ptr.doc_count +|= item.global_doc_count;
            gop.value_ptr.total_field_len +|= item.global_total_field_len;
            for (item.term_doc_freqs) |term| {
                const term_gop = try gop.value_ptr.terms.getOrPut(alloc, term.term);
                if (!term_gop.found_existing) {
                    term_gop.key_ptr.* = try alloc.dupe(u8, term.term);
                    term_gop.value_ptr.* = 0;
                }
                term_gop.value_ptr.* +|= term.doc_freq;
            }
        }
    }

    const out = try alloc.alloc(distributed_stats_mod.TextFieldStats, fields.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = fields.iterator();
    while (it.next()) |entry| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.value_ptr.terms.count());
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        var term_it = entry.value_ptr.terms.iterator();
        while (term_it.next()) |term_entry| {
            term_doc_freqs[initialized_terms] = .{
                .term = try alloc.dupe(u8, term_entry.key_ptr.*),
                .doc_freq = term_entry.value_ptr.*,
            };
            initialized_terms += 1;
        }
        out[initialized] = .{
            .field = try alloc.dupe(u8, entry.key_ptr.*),
            .global_doc_count = entry.value_ptr.doc_count,
            .global_total_field_len = entry.value_ptr.total_field_len,
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }
    return out;
}

pub const RemoteDocumentArtifactManifest = struct {
    const ChildRange = struct {
        range_id: []const u8 = "",
        range_kind: []const u8 = "",
        artifact_name: []const u8 = "",
        split_boundary: []const u8 = "",
        placement: []const u8 = "",
        owner_group_id: ?u64 = null,
        placement_generation: ?u64 = null,
        route_status: ?[]const u8 = null,
        split_eligible: ?bool = null,
        start_key: []const u8 = "",
        end_key_exclusive: []const u8 = "",
        last_key: []const u8 = "",
        child_count: usize = 0,
        text_bytes: ?usize = null,
    };

    document_id: []const u8,
    artifact_name: []const u8,
    artifact_id: []const u8,
    manifest_version: u64 = 0,
    generation: u64 = 0,
    source_url: []const u8 = "",
    source_fingerprint: []const u8 = "",
    content_type: []const u8 = "",
    route_type: []const u8 = "",
    unsupported_reason: ?[]const u8 = null,
    unit_count: usize = 0,
    chunk_count: usize = 0,
    ocr_attempted_count: usize = 0,
    ocr_selected_count: usize = 0,
    ocr_retained_embedded_count: usize = 0,
    ocr_failed_count: usize = 0,
    ocr_failed_page_numbers: []const i64 = &.{},
    ocr_failed_pages_truncated: bool = false,
    child_ranges: []const ChildRange = &.{},
    child_range_count: usize = 0,
    merge_status: []const u8 = "",
    merge_from_generation: u64 = 0,
    merge_to_generation: u64 = 0,
    merge_operation_granularity: []const u8 = "",
    merge_operation_count: usize = 0,
    last_error_code: ?[]const u8 = null,
    last_error_message: ?[]const u8 = null,
    manifest_json: []const u8,
    state_json: ?[]const u8 = null,
};

pub const RemoteDocumentArtifactManifests = struct {
    document_id: []const u8,
    artifacts: []const RemoteDocumentArtifactManifest,
};

pub fn remoteDocumentArtifactChildRangesAlloc(alloc: std.mem.Allocator, remote: []const RemoteDocumentArtifactManifest.ChildRange) ![]db_mod.types.DocumentArtifactChildRange {
    const child_ranges = try alloc.alloc(db_mod.types.DocumentArtifactChildRange, remote.len);
    var initialized_child_ranges: usize = 0;
    errdefer {
        for (child_ranges[0..initialized_child_ranges]) |*child_range| child_range.deinit(alloc);
        if (child_ranges.len > 0) alloc.free(child_ranges);
    }
    for (remote, child_ranges) |remote_range, *out| {
        out.* = .{
            .range_id = try alloc.dupe(u8, remote_range.range_id),
            .range_kind = try alloc.dupe(u8, remote_range.range_kind),
            .artifact_name = try alloc.dupe(u8, remote_range.artifact_name),
            .split_boundary = try alloc.dupe(u8, remote_range.split_boundary),
            .placement = try alloc.dupe(u8, remote_range.placement),
            .owner_group_id = remote_range.owner_group_id,
            .placement_generation = remote_range.placement_generation,
            .route_status = if (remote_range.route_status) |value| try alloc.dupe(u8, value) else null,
            .split_eligible = remote_range.split_eligible,
            .start_key = try alloc.dupe(u8, remote_range.start_key),
            .end_key_exclusive = try alloc.dupe(u8, remote_range.end_key_exclusive),
            .last_key = try alloc.dupe(u8, remote_range.last_key),
            .child_count = remote_range.child_count,
            .text_bytes = remote_range.text_bytes,
        };
        initialized_child_ranges += 1;
    }
    return child_ranges;
}

pub fn remoteDocumentArtifactManifestAlloc(alloc: std.mem.Allocator, remote: RemoteDocumentArtifactManifest) !db_mod.types.DocumentArtifactManifest {
    const child_ranges = try remoteDocumentArtifactChildRangesAlloc(alloc, remote.child_ranges);
    errdefer {
        for (child_ranges) |*child_range| child_range.deinit(alloc);
        if (child_ranges.len > 0) alloc.free(child_ranges);
    }

    const document_id = try alloc.dupe(u8, remote.document_id);
    errdefer alloc.free(document_id);
    const artifact_name = try alloc.dupe(u8, remote.artifact_name);
    errdefer alloc.free(artifact_name);
    const artifact_id = try alloc.dupe(u8, remote.artifact_id);
    errdefer alloc.free(artifact_id);
    const manifest_json = try alloc.dupe(u8, remote.manifest_json);
    errdefer alloc.free(manifest_json);
    const state_json = if (remote.state_json) |value| try alloc.dupe(u8, value) else null;
    errdefer if (state_json) |value| alloc.free(value);
    const source_url: []u8 = if (remote.source_url.len > 0) try alloc.dupe(u8, remote.source_url) else @constCast("");
    errdefer if (source_url.len > 0) alloc.free(source_url);
    const source_fingerprint: []u8 = if (remote.source_fingerprint.len > 0) try alloc.dupe(u8, remote.source_fingerprint) else @constCast("");
    errdefer if (source_fingerprint.len > 0) alloc.free(source_fingerprint);
    const content_type: []u8 = if (remote.content_type.len > 0) try alloc.dupe(u8, remote.content_type) else @constCast("");
    errdefer if (content_type.len > 0) alloc.free(content_type);
    const route_type: []u8 = if (remote.route_type.len > 0) try alloc.dupe(u8, remote.route_type) else @constCast("");
    errdefer if (route_type.len > 0) alloc.free(route_type);
    const unsupported_reason = if (remote.unsupported_reason) |value| try alloc.dupe(u8, value) else null;
    errdefer if (unsupported_reason) |value| alloc.free(value);
    const merge_status: []u8 = if (remote.merge_status.len > 0) try alloc.dupe(u8, remote.merge_status) else @constCast("");
    errdefer if (merge_status.len > 0) alloc.free(merge_status);
    const merge_operation_granularity: []u8 = if (remote.merge_operation_granularity.len > 0) try alloc.dupe(u8, remote.merge_operation_granularity) else @constCast("");
    errdefer if (merge_operation_granularity.len > 0) alloc.free(merge_operation_granularity);
    const last_error_code = if (remote.last_error_code) |value| try alloc.dupe(u8, value) else null;
    errdefer if (last_error_code) |value| alloc.free(value);
    const last_error_message = if (remote.last_error_message) |value| try alloc.dupe(u8, value) else null;
    errdefer if (last_error_message) |value| alloc.free(value);
    const ocr_failed_page_numbers: []i64 = if (remote.ocr_failed_page_numbers.len > 0)
        try alloc.dupe(i64, remote.ocr_failed_page_numbers)
    else
        @constCast(&.{});
    errdefer if (ocr_failed_page_numbers.len > 0) alloc.free(ocr_failed_page_numbers);

    return .{
        .document_id = document_id,
        .artifact_name = artifact_name,
        .artifact_id = artifact_id,
        .manifest_json = manifest_json,
        .state_json = state_json,
        .manifest_version = remote.manifest_version,
        .generation = remote.generation,
        .source_url = source_url,
        .source_fingerprint = source_fingerprint,
        .content_type = content_type,
        .route_type = route_type,
        .unsupported_reason = unsupported_reason,
        .unit_count = remote.unit_count,
        .chunk_count = remote.chunk_count,
        .ocr_attempted_count = remote.ocr_attempted_count,
        .ocr_selected_count = remote.ocr_selected_count,
        .ocr_retained_embedded_count = remote.ocr_retained_embedded_count,
        .ocr_failed_count = remote.ocr_failed_count,
        .ocr_failed_page_numbers = ocr_failed_page_numbers,
        .ocr_failed_pages_truncated = remote.ocr_failed_pages_truncated,
        .child_ranges = child_ranges,
        .child_range_count = if (remote.child_range_count > 0) remote.child_range_count else child_ranges.len,
        .merge_status = merge_status,
        .merge_from_generation = remote.merge_from_generation,
        .merge_to_generation = remote.merge_to_generation,
        .merge_operation_granularity = merge_operation_granularity,
        .merge_operation_count = remote.merge_operation_count,
        .last_error_code = last_error_code,
        .last_error_message = last_error_message,
    };
}

pub fn parseStorageKernelDocumentArtifactManifestResponse(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.DocumentArtifactManifest {
    var parsed = try std.json.parseFromSlice(RemoteDocumentArtifactManifest, alloc, body, .{});
    defer parsed.deinit();

    return try remoteDocumentArtifactManifestAlloc(alloc, parsed.value);
}

pub fn parseStorageKernelDocumentArtifactManifestsResponse(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.DocumentArtifactManifestList {
    var parsed = try std.json.parseFromSlice(RemoteDocumentArtifactManifests, alloc, body, .{});
    defer parsed.deinit();

    var artifacts = try alloc.alloc(db_mod.types.DocumentArtifactManifest, parsed.value.artifacts.len);
    errdefer alloc.free(artifacts);
    var initialized: usize = 0;
    errdefer {
        for (artifacts[0..initialized]) |*artifact| artifact.deinit(alloc);
    }
    for (parsed.value.artifacts, artifacts) |remote, *out| {
        out.* = try remoteDocumentArtifactManifestAlloc(alloc, remote);
        initialized += 1;
    }

    return .{
        .document_id = try alloc.dupe(u8, parsed.value.document_id),
        .artifacts = artifacts,
    };
}

pub fn encodeQueryRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ![]u8 {
    return try encodeQueryRequestWithGraphWireMode(alloc, req, false);
}

pub fn encodeQueryRequestWithGraphWireMode(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    allow_legacy_graph: bool,
) ![]u8 {
    if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
    if (req.dense != null and req.dense_queries.len > 0) return error.UnsupportedQueryRequest;
    if (req.sparse != null and req.sparse_queries.len > 0) return error.UnsupportedQueryRequest;
    // Cross-table authorization is a request-local callback and has no trusted
    // generic JSON representation. Supported graph requests execute through
    // the coordinator; unsupported authenticated modes are rejected during
    // routing. Keep this last-line guard so a future route cannot proxy them
    // without their target-table authorization policy.
    if (req.graph_queries.len > 0 and req.graph_table_read_authorizer != null)
        return error.UnsupportedQueryRequest;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    const has_named_embeddings = req.dense_queries.len > 0 or req.sparse_queries.len > 0;

    if (!req.include_all_fields) {
        try appendJsonFieldNames(alloc, &out, &first, "fields", req.fields);
    }
    if (req.hierarchy_children != null or
        req.hierarchy_grouped_matches or
        req.hierarchy_group_level == .unit)
    {
        try appendQueryHierarchyField(alloc, &out, &first, req);
    }
    if (req.limit != 10) {
        try appendJsonFieldU32(alloc, &out, &first, "limit", req.limit);
    }
    if (req.offset != 0) {
        try appendJsonFieldU32(alloc, &out, &first, "offset", req.offset);
    }
    if (req.order_by.len > 0) try appendQueryOrderByField(alloc, &out, &first, req.order_by);
    if (req.search_after.len > 0) try appendQueryCursorField(alloc, &out, &first, "search_after", req.search_after);
    if (req.search_before.len > 0) try appendQueryCursorField(alloc, &out, &first, "search_before", req.search_before);
    if (req.count_only) {
        try appendJsonFieldBool(alloc, &out, &first, "count", true);
    }
    if (req.profile) {
        try appendJsonFieldBool(alloc, &out, &first, "profile", true);
    }
    if (req.index_name) |index_name| {
        // The public `indexes` selector does not reconstruct the legacy
        // singleton binding for every query family. Carry the exact resolved
        // identity separately; a hybrid query's primary text index may differ.
        try appendJsonFieldString(alloc, &out, &first, "_index_name", index_name);
        if (!has_named_embeddings and (req.full_text != null or req.dense != null or req.sparse != null)) {
            const index_names = [_][]const u8{index_name};
            try appendJsonFieldNames(alloc, &out, &first, "indexes", &index_names);
        }
    }
    if (req.primary_text_index_name orelse if (req.full_text != null or req.filter_text != null or req.exclusion_text != null)
        req.index_name
    else
        null) |index_name|
    {
        try appendJsonFieldString(alloc, &out, &first, "_primary_text_index_name", index_name);
    }
    if (req.filter_prefix.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "filter_prefix", req.filter_prefix);
    }
    if (req.distance_over) |value| {
        try appendJsonFieldF32(alloc, &out, &first, "distance_over", value);
    }
    if (req.distance_under) |value| {
        try appendJsonFieldF32(alloc, &out, &first, "distance_under", value);
    }
    if (req.merge_config) |merge_config| {
        try appendMergeConfigField(alloc, &out, &first, merge_config);
    }
    if (req.pruner) |pruner| {
        try appendPrunerField(alloc, &out, &first, pruner);
    }
    if (req.distributed_text_stats.len > 0) {
        try appendDistributedTextStatsField(alloc, &out, &first, req.distributed_text_stats);
    }
    if (req.identity_read_generation) |generation| {
        try appendJsonFieldU64(alloc, &out, &first, "_identity_read_generation", generation);
    }
    if (req.hierarchy_children != null or req.defer_hierarchy_child_hydration) {
        try appendJsonFieldBool(alloc, &out, &first, "_defer_hierarchy_child_hydration", true);
    }
    if (req.resolved_doc_filter != null) {
        try db_mod.doc_filter_wire.appendSearchRequestFieldAlloc(alloc, &out, &first, req);
    }
    const native_doc_id_constraints = query_contract.nativeDocIdConstraintEnvelopeFromSearchRequest(req);
    if (native_doc_id_constraints.hasConstraints()) {
        try appendNativeDocIdConstraintsField(alloc, &out, &first, native_doc_id_constraints);
    }
    if (req.doc_filter_bindings.len > 0) {
        try appendDocFilterBindingsField(alloc, &out, &first, req.doc_filter_bindings);
    }
    if (req.filter_query_json.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "_filter_query_json", req.filter_query_json);
    }
    if (req.exclusion_query_json.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "_exclusion_query_json", req.exclusion_query_json);
    }
    if (req.require_algebraic_filter_resolution) {
        try appendJsonFieldBool(alloc, &out, &first, "_require_algebraic_filter_resolution", true);
    }
    if (req.filter_text) |filter_text| {
        try appendTextQueryField(alloc, &out, &first, "filter_query", filter_text);
    }
    if (req.exclusion_text) |exclusion_text| {
        try appendTextQueryField(alloc, &out, &first, "exclusion_query", exclusion_text);
    }
    if (req.graph_queries.len > 0) {
        try appendGraphQueriesField(
            alloc,
            &out,
            &first,
            req.graph_queries,
            req.graph_query_transport,
            allow_legacy_graph,
        );
    }
    if (req.expand_strategy) |expand_strategy| {
        try appendJsonFieldString(alloc, &out, &first, "expand_strategy", switch (expand_strategy) {
            .@"union" => "union",
            .intersection => "intersection",
        });
    }
    var singleton_dense: [1]db_mod.types.NamedDenseQuery = undefined;
    const dense_queries = if (req.dense) |query| blk: {
        const index_name = req.index_name orelse return error.UnsupportedQueryRequest;
        singleton_dense[0] = .{ .name = index_name, .index_name = index_name, .query = query };
        break :blk singleton_dense[0..];
    } else req.dense_queries;
    var singleton_sparse: [1]db_mod.types.NamedSparseQuery = undefined;
    const sparse_queries = if (req.sparse) |query| blk: {
        const index_name = req.index_name orelse return error.UnsupportedQueryRequest;
        singleton_sparse[0] = .{ .name = index_name, .index_name = index_name, .query = query };
        break :blk singleton_sparse[0..];
    } else req.sparse_queries;
    if (dense_queries.len > 0 or sparse_queries.len > 0) {
        try appendEmbeddingsField(alloc, &out, &first, dense_queries, sparse_queries);
    }
    if (req.hierarchy_children != null) {
        // Child traversal is an ordered hierarchy scan rather than a relevance
        // query. Keeping the query clause out of the internal wire request also
        // lets the public parser reject accidental mixed-mode requests.
    } else if (req.full_text_queries.len > 0) {
        // The public selector is intentionally singular. Preserve its stable
        // result name while forwarding coordinator requests to data shards;
        // arbitrary internal multi-query plans cannot be represented by the
        // public wire contract without losing result-set identity.
        if (req.full_text != null or
            req.full_text_queries.len != 1 or
            !std.mem.eql(u8, req.full_text_queries[0].name, "$full_text_results"))
        {
            return error.UnsupportedQueryRequest;
        }
        try appendJsonFieldString(
            alloc,
            &out,
            &first,
            "full_text_index",
            req.full_text_queries[0].index_name,
        );
        try appendTextQueryField(
            alloc,
            &out,
            &first,
            "full_text_search",
            req.full_text_queries[0].query,
        );
    } else if (req.full_text) |full_text| {
        try appendTextQueryField(alloc, &out, &first, "full_text_search", full_text);
    } else {
        try appendQueryField(alloc, &out, &first, req.query, req.limit);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeStorageKernelQueryRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ![]u8 {
    // The compiled storage boundary remains in-process and must preserve the
    // deprecated public graph dialect for single-group compatibility. Generic
    // inter-node shard forwarding continues to reject that stateful dialect.
    return try encodeQueryRequestWithGraphWireMode(alloc, req, true);
}

pub const StorageKernelLookupWireRequest = struct {
    key: []const u8,
    fields: []const []const u8 = &.{},
    include_all_fields: bool = true,
};

pub fn encodeStorageKernelLookupRequest(
    alloc: std.mem.Allocator,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, StorageKernelLookupWireRequest{
        .key = key,
        .fields = opts.fields,
        .include_all_fields = opts.include_all_fields,
    }, .{});
}

pub const StorageKernelDocumentArtifactManifestWireRequest = struct {
    doc_key: []const u8,
    artifact_name: []const u8,
};

pub const StorageKernelDocumentArtifactManifestsWireRequest = struct {
    doc_key: []const u8,
};

pub fn encodeStorageKernelDocumentArtifactManifestRequest(
    alloc: std.mem.Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, StorageKernelDocumentArtifactManifestWireRequest{
        .doc_key = doc_key,
        .artifact_name = artifact_name,
    }, .{});
}

pub fn encodeStorageKernelDocumentArtifactManifestsRequest(
    alloc: std.mem.Allocator,
    doc_key: []const u8,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, StorageKernelDocumentArtifactManifestsWireRequest{
        .doc_key = doc_key,
    }, .{});
}

pub fn encodeStorageKernelDocumentArtifactManifestResponse(
    alloc: std.mem.Allocator,
    manifest: db_mod.types.DocumentArtifactManifest,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, manifest, .{});
}

pub fn encodeStorageKernelDocumentArtifactManifestsResponse(
    alloc: std.mem.Allocator,
    manifests: db_mod.types.DocumentArtifactManifestList,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, manifests, .{});
}

pub const StorageKernelScanWireRequest = struct {
    from_key: []const u8 = "",
    to_key: []const u8 = "",
    inclusive_from: bool = false,
    exclusive_to: bool = false,
    include_documents: bool = false,
    limit: u32 = 0,
    fields: []const []const u8 = &.{},
    include_all_fields: bool = true,
    filter_query_json: []const u8 = "",
    include_content_hashes: bool = false,
};

pub fn encodeStorageKernelScanRequest(
    alloc: std.mem.Allocator,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, StorageKernelScanWireRequest{
        .from_key = from_key,
        .to_key = to_key,
        .inclusive_from = opts.inclusive_from,
        .exclusive_to = opts.exclusive_to,
        .include_documents = opts.include_documents,
        .limit = opts.limit,
        .fields = opts.fields,
        .include_all_fields = opts.include_all_fields,
        .filter_query_json = opts.filter_query_json,
        .include_content_hashes = opts.include_content_hashes,
    }, .{});
}

pub fn encodeStorageKernelScanNdjson(
    alloc: std.mem.Allocator,
    result: db_mod.types.ScanResult,
    include_documents: bool,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (result.hashes, 0..) |entry, i| {
        const json = if (include_documents) result.documents[i].json else null;
        try appendScanLine(alloc, &out, entry.id, json, entry.content_hash);
    }
    return try out.toOwnedSlice(alloc);
}

pub const StorageKernelDynamicFieldObservationWireRequest = struct {
    index_name: ?[]const u8 = null,
    fields: []const []const u8 = &.{},
    coverage_read_mode: dynamic_field_capability.CoverageReadMode = .cached_only,
};

pub fn encodeStorageKernelDynamicFieldObservationRequest(
    alloc: std.mem.Allocator,
    observation: DynamicFieldObservationQuery,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, StorageKernelDynamicFieldObservationWireRequest{
        .index_name = observation.index_name,
        .fields = observation.fields,
        .coverage_read_mode = observation.coverage_read_mode,
    }, .{});
}

pub fn encodeStorageKernelPreflightRequest(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    max_work: u32,
) ![]u8 {
    const query_json = try encodeQueryRequest(alloc, req);
    defer alloc.free(query_json);
    return try std.json.Stringify.valueAlloc(alloc, StorageKernelPreflightWireRequest{
        .query_json = query_json,
        .max_work = max_work,
    }, .{});
}

pub fn parseStorageKernelPreflightSummary(
    alloc: std.mem.Allocator,
    response_json: []const u8,
) !db_mod.RuntimePreflightSummary {
    var parsed = try std.json.parseFromSlice(
        db_mod.RuntimePreflightSummary,
        alloc,
        response_json,
        .{ .allocate = .alloc_always },
    );
    defer parsed.deinit();
    return try cloneRuntimePreflightSummary(alloc, parsed.value);
}

pub fn appendGraphQueriesField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    graph_queries: []const db_mod.types.NamedGraphQuery,
    graph_query_transport: ?db_mod.types.GraphQueryTransport,
    allow_legacy_graph: bool,
) !void {
    if (graph_queries.len == 0) return;
    const transport = graph_query_transport orelse return error.UnsupportedQueryRequest;
    if (!transport.matchesOperations(graph_queries) or
        transport.operations_json.len < 2 or
        transport.operations_json[0] != '{' or
        transport.operations_json[transport.operations_json.len - 1] != '}')
        return error.UnsupportedQueryRequest;
    const field_name: []const u8 = switch (transport.dialect) {
        .canonical => "graph_queries",
        .legacy => if (allow_legacy_graph)
            "graph_searches"
        else
            return error.UnsupportedQueryRequest,
    };
    try appendJsonFieldName(alloc, out, first, field_name);
    try out.appendSlice(alloc, transport.operations_json);
}

pub fn appendQueryHierarchyField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    req: db_mod.types.SearchRequest,
) !void {
    try appendJsonFieldName(alloc, out, first, "hierarchy");
    try out.append(alloc, '{');
    if (req.hierarchy_children) |children| {
        try out.appendSlice(alloc, "\"children\":{\"parent\":{\"level\":\"source\",\"id\":");
        try appendJsonString(alloc, out, children.parent_id);
        try out.appendSlice(alloc, "},\"level\":\"unit\"}");
    } else {
        try out.appendSlice(alloc, "\"group_by\":{\"level\":");
        try appendJsonString(alloc, out, @tagName(req.hierarchy_group_level));
        if (req.hierarchy_grouped_matches) {
            try out.appendSlice(alloc, ",\"matches\":{");
            try out.appendSlice(alloc, "\"limit\":");
            try out.print(alloc, "{d}", .{req.max_chunks_per_parent});
            try out.appendSlice(alloc, ",\"fields\":");
            try appendJsonStringArray(alloc, out, req.hierarchy_match_fields);
            try out.append(alloc, '}');
        }
        try out.append(alloc, '}');
        if (req.hierarchy_include_source or req.hierarchy_include_unit) {
            try out.appendSlice(alloc, ",\"ancestors\":{");
            var first_ancestor = true;
            if (req.hierarchy_include_source) {
                try out.appendSlice(alloc, "\"source\":{\"fields\":");
                try appendJsonStringArray(alloc, out, req.hierarchy_source_fields);
                try out.append(alloc, '}');
                first_ancestor = false;
            }
            if (req.hierarchy_include_unit) {
                if (!first_ancestor) try out.append(alloc, ',');
                try out.appendSlice(alloc, "\"unit\":{\"fields\":");
                try appendJsonStringArray(alloc, out, req.hierarchy_unit_fields);
                try out.append(alloc, '}');
            }
            try out.append(alloc, '}');
        }
    }
    try out.append(alloc, '}');
}

pub fn appendQueryOrderByField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    order_by: []const db_mod.types.SortField,
) !void {
    try appendJsonFieldName(alloc, out, first, "order_by");
    try out.append(alloc, '[');
    for (order_by, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "{\"field\":");
        try appendJsonString(alloc, out, field.field);
        if (field.desc) try out.appendSlice(alloc, ",\"desc\":true");
        try out.append(alloc, '}');
    }
    try out.append(alloc, ']');
}

pub fn appendQueryCursorField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    values: []const std.json.Value,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, ']');
}

pub fn appendDocFilterBindingsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    bindings: []const db_mod.types.NamedDocFilterBinding,
) !void {
    if (bindings.len == 0) return;

    var seen = std.StringHashMap(void).init(alloc);
    defer seen.deinit();
    const binding_count = std.math.cast(u32, bindings.len) orelse {
        return error.InvalidQueryRequest;
    };
    try seen.ensureTotalCapacity(binding_count);

    try appendJsonFieldName(alloc, out, first, "with");
    try out.append(alloc, '{');
    for (bindings, 0..) |binding, index| {
        if (binding.name.len == 0 or binding.filter_query_json.len == 0) {
            return error.InvalidQueryRequest;
        }
        const normalized_filter = std.mem.trim(
            u8,
            binding.filter_query_json,
            &std.ascii.whitespace,
        );
        if (normalized_filter.len < 2 or
            normalized_filter[0] != '{' or
            normalized_filter[normalized_filter.len - 1] != '}' or
            !(try std.json.validate(alloc, normalized_filter)))
        {
            return error.InvalidQueryRequest;
        }
        const entry = try seen.getOrPut(binding.name);
        if (entry.found_existing) return error.InvalidQueryRequest;

        if (index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, binding.name);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, normalized_filter);
    }
    try out.append(alloc, '}');
}

pub fn appendNativeDocIdConstraintsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    constraints: query_contract.NativeDocIdConstraintEnvelope,
) !void {
    const encoded = try query_contract.encodeNativeDocIdConstraintEnvelopeAlloc(alloc, constraints);
    defer alloc.free(encoded);
    try appendJsonFieldName(alloc, out, first, "native_doc_id_constraints");
    try out.appendSlice(alloc, encoded);
}

pub fn appendDistributedTextStatsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    items: []const distributed_stats_mod.TextFieldStats,
) !void {
    try appendJsonFieldName(alloc, out, first, "_distributed_text_stats");
    try out.append(alloc, '[');
    for (items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var field_first = true;
        try appendJsonFieldString(alloc, out, &field_first, "field", item.field);
        try appendJsonFieldU32(alloc, out, &field_first, "global_doc_count", item.global_doc_count);
        try appendJsonFieldU64(alloc, out, &field_first, "global_total_field_len", item.global_total_field_len);
        try appendJsonFieldName(alloc, out, &field_first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.append(alloc, ']');
}

pub fn appendMergeConfigField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    merge_config: db_mod.types.MergeConfig,
) !void {
    try appendJsonFieldName(alloc, out, first, "merge_config");
    try out.append(alloc, '{');
    var merge_first = true;
    try appendJsonFieldString(alloc, out, &merge_first, "strategy", switch (merge_config.strategy) {
        .rrf => "rrf",
        .rsf => "rsf",
    });
    if (merge_config.rank_constant != 60.0) {
        try appendJsonFieldF64(alloc, out, &merge_first, "rank_constant", merge_config.rank_constant);
    }
    if (merge_config.window_size != 0) {
        try appendJsonFieldU32(alloc, out, &merge_first, "window_size", merge_config.window_size);
    }
    if (merge_config.weights.len > 0) {
        try appendJsonFieldName(alloc, out, &merge_first, "weights");
        try out.append(alloc, '{');
        for (merge_config.weights, 0..) |weight, i| {
            if (i > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, weight.name);
            try out.append(alloc, ':');
            var weight_buf: [32]u8 = undefined;
            const rendered = try std.fmt.bufPrint(&weight_buf, "{d}", .{weight.weight});
            try out.appendSlice(alloc, rendered);
        }
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');
}

pub fn appendPrunerField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    pruner: @import("../search/fusion.zig").Pruner,
) !void {
    try appendJsonFieldName(alloc, out, first, "pruner");
    try out.append(alloc, '{');
    var pruner_first = true;
    if (pruner.min_score_ratio > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "min_score_ratio", pruner.min_score_ratio);
    }
    if (pruner.max_score_gap_percent > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "max_score_gap_percent", pruner.max_score_gap_percent);
    }
    if (pruner.min_absolute_score > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "min_absolute_score", pruner.min_absolute_score);
    }
    if (pruner.require_multi_index) {
        try appendJsonFieldBool(alloc, out, &pruner_first, "require_multi_index", true);
    }
    if (pruner.std_dev_threshold > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "std_dev_threshold", pruner.std_dev_threshold);
    }
    try out.append(alloc, '}');
}

pub fn appendEmbeddingsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    dense_queries: []const db_mod.types.NamedDenseQuery,
    sparse_queries: []const db_mod.types.NamedSparseQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, "embeddings");
    try out.append(alloc, '{');
    var entry_index: usize = 0;
    for (dense_queries) |dense_query| {
        if (entry_index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, dense_query.index_name);
        try out.appendSlice(alloc, ":[");
        for (dense_query.query.vector, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.append(alloc, ']');
        entry_index += 1;
    }
    for (sparse_queries) |sparse_query| {
        if (entry_index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, sparse_query.index_name);
        try out.appendSlice(alloc, ":{\"indices\":[");
        for (sparse_query.query.indices, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.appendSlice(alloc, "],\"values\":[");
        for (sparse_query.query.values, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.appendSlice(alloc, "]}");
        entry_index += 1;
    }
    try out.append(alloc, '}');
}

pub fn appendQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    query: db_mod.types.Query,
    default_k: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, "full_text_search");
    switch (query) {
        .dense_knn => |dense| {
            try out.appendSlice(alloc, "{\"dense_knn\":{\"vector\":[");
            for (dense.vector, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"k\":");
            try out.print(alloc, "{d}", .{if (dense.k == 0) default_k else dense.k});
            try out.appendSlice(alloc, "}}");
        },
        .sparse_knn => |sparse| {
            try out.appendSlice(alloc, "{\"sparse_knn\":{\"indices\":[");
            for (sparse.indices, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"values\":[");
            for (sparse.values, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"k\":");
            try out.print(alloc, "{d}", .{if (sparse.k == 0) default_k else sparse.k});
            try out.appendSlice(alloc, "}}");
        },
        .graph => return error.UnsupportedQueryRequest,
        else => try appendTextQueryValue(
            alloc,
            out,
            try borrowedTextQueryFromQuery(query),
        ),
    }
}

pub fn borrowedTextQueryFromQuery(
    query: db_mod.types.Query,
) !db_mod.types.TextQuery {
    return switch (query) {
        .match_none => .{ .match_none = {} },
        .match_all => .{ .match_all = {} },
        .phrase => |value| .{ .phrase = .{
            .field = value.field,
            .terms = value.terms,
            .max_edits = value.max_edits,
            .auto_fuzzy = value.auto_fuzzy,
            .boost = value.boost,
        } },
        .multi_phrase => |value| .{ .multi_phrase = .{
            .field = value.field,
            .terms = value.terms,
            .max_edits = value.max_edits,
            .auto_fuzzy = value.auto_fuzzy,
            .boost = value.boost,
        } },
        .term => |value| .{ .term = .{
            .field = value.field,
            .term = value.term,
            .boost = value.boost,
        } },
        .match => |value| .{ .match = .{
            .field = value.field,
            .text = value.text,
            .analyzer = value.analyzer,
            .boost = value.boost,
        } },
        .match_phrase => |value| .{ .match_phrase = .{
            .field = value.field,
            .text = value.text,
            .analyzer = value.analyzer,
            .max_edits = value.max_edits,
            .auto_fuzzy = value.auto_fuzzy,
            .boost = value.boost,
        } },
        .fuzzy => |value| .{ .fuzzy = .{
            .field = value.field,
            .term = value.term,
            .max_edits = value.max_edits,
            .prefix_len = value.prefix_len,
            .auto_fuzzy = value.auto_fuzzy,
            .boost = value.boost,
        } },
        .numeric_range => |value| .{ .numeric_range = .{
            .field = value.field,
            .min = value.min,
            .max = value.max,
            .inclusive_min = value.inclusive_min,
            .inclusive_max = value.inclusive_max,
            .boost = value.boost,
        } },
        .date_range => |value| .{ .date_range = .{
            .field = value.field,
            .start_ns = value.start_ns,
            .end_ns = value.end_ns,
            .inclusive_start = value.inclusive_start,
            .inclusive_end = value.inclusive_end,
            .boost = value.boost,
        } },
        .doc_id => |value| .{ .doc_id = .{
            .ids = value.ids,
            .boost = value.boost,
        } },
        .bool_field => |value| .{ .bool_field = .{
            .field = value.field,
            .value = value.value,
            .boost = value.boost,
        } },
        .geo_distance => |value| .{ .geo_distance = .{
            .field = value.field,
            .lon = value.lon,
            .lat = value.lat,
            .radius_meters = value.radius_meters,
            .boost = value.boost,
        } },
        .geo_bbox => |value| .{ .geo_bbox = .{
            .field = value.field,
            .min_lat = value.min_lat,
            .min_lon = value.min_lon,
            .max_lat = value.max_lat,
            .max_lon = value.max_lon,
            .boost = value.boost,
        } },
        .prefix => |value| .{ .prefix = .{
            .field = value.field,
            .prefix = value.prefix,
            .boost = value.boost,
        } },
        .wildcard => |value| .{ .wildcard = .{
            .field = value.field,
            .pattern = value.pattern,
            .boost = value.boost,
        } },
        .regexp => |value| .{ .regexp = .{
            .field = value.field,
            .pattern = value.pattern,
            .boost = value.boost,
        } },
        .term_range => |value| .{ .term_range = .{
            .field = value.field,
            .min = value.min,
            .max = value.max,
            .inclusive_min = value.inclusive_min,
            .inclusive_max = value.inclusive_max,
            .boost = value.boost,
        } },
        .ip_range => |value| .{ .ip_range = .{
            .field = value.field,
            .cidr = value.cidr,
            .boost = value.boost,
        } },
        .geo_shape => |value| .{ .geo_shape = .{
            .field = value.field,
            .relation = value.relation,
            .polygons = value.polygons,
            .boost = value.boost,
        } },
        .dense_knn, .sparse_knn, .graph => error.UnsupportedQueryRequest,
    };
}

pub fn appendTextQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    query: db_mod.types.TextQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendTextQueryValue(alloc, out, query);
}

pub fn appendTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: db_mod.types.TextQuery,
) !void {
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .match_none => try out.appendSlice(alloc, "{\"match_none\":{}}"),
        .phrase => |phrase| try appendPhraseTextQueryValue(alloc, out, phrase),
        .multi_phrase => |phrase| try appendMultiPhraseTextQueryValue(alloc, out, phrase),
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, term.field);
            try appendTextQueryBoost(alloc, out, term.boost);
            try out.append(alloc, '}');
        },
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":");
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, match.field);
            if (match.analyzer) |analyzer| {
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, analyzer);
            }
            try appendTextQueryBoost(alloc, out, match.boost);
            try out.append(alloc, '}');
        },
        .multi_match_bool_prefix => |multi_match| {
            if (!std.math.isFinite(multi_match.boost)) {
                return error.InvalidQueryRequest;
            }
            try out.appendSlice(alloc, "{\"multi_match\":{\"query\":");
            try appendJsonString(alloc, out, multi_match.query);
            try out.appendSlice(alloc, ",\"type\":\"bool_prefix\",\"fields\":[");
            for (multi_match.fields, 0..) |field, i| {
                if (i > 0) try out.append(alloc, ',');
                if (!std.math.isFinite(field.boost) or field.boost <= 0) {
                    return error.InvalidQueryRequest;
                }
                if (field.boost == 1.0) {
                    try appendJsonString(alloc, out, field.field);
                } else {
                    const boosted_field = try std.fmt.allocPrint(alloc, "{s}^{d}", .{ field.field, field.boost });
                    defer alloc.free(boosted_field);
                    try appendJsonString(alloc, out, boosted_field);
                }
            }
            try out.append(alloc, ']');
            try appendTextQueryBoost(alloc, out, multi_match.boost);
            try out.appendSlice(alloc, "}}");
        },
        .match_phrase => |phrase| {
            try out.appendSlice(alloc, "{\"match_phrase\":");
            try appendJsonString(alloc, out, phrase.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, phrase.field);
            if (phrase.analyzer) |analyzer| {
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, analyzer);
            }
            if (phrase.auto_fuzzy) {
                try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
            } else if (phrase.max_edits > 0) {
                try out.appendSlice(alloc, ",\"fuzziness\":");
                try out.print(alloc, "{d}", .{phrase.max_edits});
            }
            try appendTextQueryBoost(alloc, out, phrase.boost);
            try out.append(alloc, '}');
        },
        .fuzzy => |fuzzy| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, fuzzy.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, fuzzy.field);
            if (fuzzy.prefix_len > 0) {
                try out.appendSlice(alloc, ",\"prefix_length\":");
                try out.print(alloc, "{d}", .{fuzzy.prefix_len});
            }
            if (fuzzy.auto_fuzzy) {
                try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
            } else {
                try out.appendSlice(alloc, ",\"fuzziness\":");
                try out.print(alloc, "{d}", .{fuzzy.max_edits});
            }
            try appendTextQueryBoost(alloc, out, fuzzy.boost);
            try out.append(alloc, '}');
        },
        .prefix => |prefix| {
            try out.appendSlice(alloc, "{\"prefix\":");
            try appendJsonString(alloc, out, prefix.prefix);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, prefix.field);
            try appendTextQueryBoost(alloc, out, prefix.boost);
            try out.append(alloc, '}');
        },
        .wildcard => |wildcard| {
            try out.appendSlice(alloc, "{\"wildcard\":");
            try appendJsonString(alloc, out, wildcard.pattern);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, wildcard.field);
            try appendTextQueryBoost(alloc, out, wildcard.boost);
            try out.append(alloc, '}');
        },
        .regexp => |regexp| {
            try out.appendSlice(alloc, "{\"regexp\":");
            try appendJsonString(alloc, out, regexp.pattern);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, regexp.field);
            try appendTextQueryBoost(alloc, out, regexp.boost);
            try out.append(alloc, '}');
        },
        .numeric_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.min) |min| {
                try appendJsonFieldName(alloc, out, &first, "min");
                try out.print(alloc, "{d}", .{min});
            }
            if (range_query.max) |max| {
                try appendJsonFieldName(alloc, out, &first, "max");
                try out.print(alloc, "{d}", .{max});
            }
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &first, "inclusive_max", true);
            try appendOptionalTextQueryBoostField(alloc, out, &first, range_query.boost);
            try out.append(alloc, '}');
        },
        .date_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.start_ns) |start_ns| {
                const text = try formatRfc3339Ns(alloc, start_ns);
                defer alloc.free(text);
                try appendJsonFieldString(alloc, out, &first, "start", text);
            }
            if (range_query.end_ns) |end_ns| {
                const text = try formatRfc3339Ns(alloc, end_ns);
                defer alloc.free(text);
                try appendJsonFieldString(alloc, out, &first, "end", text);
            }
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_start) try appendJsonFieldBool(alloc, out, &first, "inclusive_start", false);
            if (range_query.inclusive_end) try appendJsonFieldBool(alloc, out, &first, "inclusive_end", true);
            try appendOptionalTextQueryBoostField(alloc, out, &first, range_query.boost);
            try out.append(alloc, '}');
        },
        .term_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.min) |min| try appendJsonFieldString(alloc, out, &first, "min", min);
            if (range_query.max) |max| try appendJsonFieldString(alloc, out, &first, "max", max);
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &first, "inclusive_max", true);
            try appendOptionalTextQueryBoostField(alloc, out, &first, range_query.boost);
            try out.append(alloc, '}');
        },
        .doc_id => |doc_id| {
            try out.appendSlice(alloc, "{\"ids\":[");
            for (doc_id.ids, 0..) |id, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, id);
            }
            try out.append(alloc, ']');
            try appendTextQueryBoost(alloc, out, doc_id.boost);
            try out.append(alloc, '}');
        },
        .bool_field => |bool_field| {
            try out.appendSlice(alloc, "{\"bool\":");
            try out.appendSlice(alloc, if (bool_field.value) "true" else "false");
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, bool_field.field);
            try appendTextQueryBoost(alloc, out, bool_field.boost);
            try out.append(alloc, '}');
        },
        .bool_query => |bool_query| {
            try out.append(alloc, '{');
            var first = true;
            if (bool_query.must.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must");
                try out.appendSlice(alloc, "{\"conjuncts\":[");
                for (bool_query.must, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.appendSlice(alloc, "]}");
            }
            if (bool_query.should.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "should");
                try out.appendSlice(alloc, "{\"disjuncts\":[");
                for (bool_query.should, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
                if (bool_query.min_should > 0 or bool_query.pure_should_optional) {
                    try out.appendSlice(alloc, ",\"min\":");
                    try out.print(alloc, "{d}", .{bool_query.min_should});
                }
                try out.append(alloc, '}');
            }
            if (bool_query.must_not.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must_not");
                try out.appendSlice(alloc, "{\"disjuncts\":[");
                for (bool_query.must_not, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.appendSlice(alloc, "]}");
            }
            try appendOptionalTextQueryBoostField(alloc, out, &first, bool_query.boost);
            try out.append(alloc, '}');
        },
        .geo_distance => |distance| try appendGeoDistanceTextQueryValue(alloc, out, distance),
        .geo_bbox => |bbox| try appendGeoBBoxTextQueryValue(alloc, out, bbox),
        .ip_range => |range| try appendIpRangeTextQueryValue(alloc, out, range),
        .geo_shape => |shape| try appendGeoShapeTextQueryValue(alloc, out, shape),
    }
}

pub fn appendPhraseTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    phrase: anytype,
) !void {
    if (phrase.field.len == 0 or phrase.terms.len == 0 or phrase.max_edits > 2) {
        return error.InvalidQueryRequest;
    }
    try out.appendSlice(alloc, "{\"terms\":[");
    for (phrase.terms, 0..) |term, index| {
        if (term.len == 0) return error.InvalidQueryRequest;
        if (index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, term);
    }
    try out.appendSlice(alloc, "],\"field\":");
    try appendJsonString(alloc, out, phrase.field);
    if (phrase.auto_fuzzy) {
        try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
    } else if (phrase.max_edits > 0) {
        try out.appendSlice(alloc, ",\"fuzziness\":");
        try out.print(alloc, "{d}", .{phrase.max_edits});
    }
    try appendTextQueryBoost(alloc, out, phrase.boost);
    try out.append(alloc, '}');
}

pub fn appendMultiPhraseTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    phrase: anytype,
) !void {
    if (phrase.field.len == 0 or phrase.terms.len == 0 or phrase.max_edits > 2) {
        return error.InvalidQueryRequest;
    }
    try out.appendSlice(alloc, "{\"terms\":[");
    for (phrase.terms, 0..) |alternatives, position| {
        if (alternatives.len == 0) return error.InvalidQueryRequest;
        if (position > 0) try out.append(alloc, ',');
        try out.append(alloc, '[');
        for (alternatives, 0..) |term, alternative| {
            if (term.len == 0) return error.InvalidQueryRequest;
            if (alternative > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, term);
        }
        try out.append(alloc, ']');
    }
    try out.appendSlice(alloc, "],\"field\":");
    try appendJsonString(alloc, out, phrase.field);
    if (phrase.auto_fuzzy) {
        try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
    } else if (phrase.max_edits > 0) {
        try out.appendSlice(alloc, ",\"fuzziness\":");
        try out.print(alloc, "{d}", .{phrase.max_edits});
    }
    try appendTextQueryBoost(alloc, out, phrase.boost);
    try out.append(alloc, '}');
}

pub fn appendGeoDistanceTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    distance: anytype,
) !void {
    if (distance.field.len == 0 or
        !std.math.isFinite(distance.lon) or distance.lon < -180 or distance.lon > 180 or
        !std.math.isFinite(distance.lat) or distance.lat < -90 or distance.lat > 90 or
        !std.math.isFinite(distance.radius_meters) or distance.radius_meters < 0)
    {
        return error.InvalidQueryRequest;
    }
    try out.appendSlice(alloc, "{\"location\":[");
    try out.print(alloc, "{d},{d}", .{ distance.lon, distance.lat });
    try out.appendSlice(alloc, "],\"distance\":\"");
    try out.print(alloc, "{d}m\",\"field\":", .{distance.radius_meters});
    try appendJsonString(alloc, out, distance.field);
    try appendTextQueryBoost(alloc, out, distance.boost);
    try out.append(alloc, '}');
}

pub fn appendGeoBBoxTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    bbox: anytype,
) !void {
    if (bbox.field.len == 0 or
        !std.math.isFinite(bbox.min_lat) or bbox.min_lat < -90 or bbox.min_lat > 90 or
        !std.math.isFinite(bbox.max_lat) or bbox.max_lat < -90 or bbox.max_lat > 90 or
        bbox.min_lat > bbox.max_lat or
        !std.math.isFinite(bbox.min_lon) or bbox.min_lon < -180 or bbox.min_lon > 180 or
        !std.math.isFinite(bbox.max_lon) or bbox.max_lon < -180 or bbox.max_lon > 180)
    {
        return error.InvalidQueryRequest;
    }
    try out.appendSlice(alloc, "{\"field\":");
    try appendJsonString(alloc, out, bbox.field);
    try out.print(
        alloc,
        ",\"min_lat\":{d},\"min_lon\":{d},\"max_lat\":{d},\"max_lon\":{d}",
        .{ bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon },
    );
    try appendTextQueryBoost(alloc, out, bbox.boost);
    try out.append(alloc, '}');
}

pub fn appendIpRangeTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    range: anytype,
) !void {
    if (range.field.len == 0 or !algebraicValidIpRange(range.cidr)) {
        return error.InvalidQueryRequest;
    }
    try out.appendSlice(alloc, "{\"cidr\":");
    try appendJsonString(alloc, out, range.cidr);
    try out.appendSlice(alloc, ",\"field\":");
    try appendJsonString(alloc, out, range.field);
    try appendTextQueryBoost(alloc, out, range.boost);
    try out.append(alloc, '}');
}

pub fn appendGeoShapeTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    shape: anytype,
) !void {
    if (shape.field.len == 0 or shape.polygons.len == 0) {
        return error.InvalidQueryRequest;
    }
    try out.appendSlice(
        alloc,
        "{\"geometry\":{\"shape\":{\"type\":\"MultiPolygon\",\"coordinates\":[",
    );
    for (shape.polygons, 0..) |polygon, polygon_index| {
        if (polygon.len < 3) return error.InvalidQueryRequest;
        if (polygon_index > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "[[");
        for (polygon, 0..) |point, point_index| {
            if (!std.math.isFinite(point.lat) or point.lat < -90 or point.lat > 90 or
                !std.math.isFinite(point.lon) or point.lon < -180 or point.lon > 180)
            {
                return error.InvalidQueryRequest;
            }
            if (point_index > 0) try out.append(alloc, ',');
            try out.print(alloc, "[{d},{d}]", .{ point.lon, point.lat });
        }
        const first = polygon[0];
        const last = polygon[polygon.len - 1];
        if (first.lat != last.lat or first.lon != last.lon) {
            try out.print(alloc, ",[{d},{d}]", .{ first.lon, first.lat });
        }
        try out.appendSlice(alloc, "]]");
    }
    try out.appendSlice(alloc, "]},\"relation\":");
    try appendJsonString(alloc, out, switch (shape.relation) {
        .intersects => "intersects",
        .within => "within",
        .contains => "contains",
    });
    try out.appendSlice(alloc, "},\"field\":");
    try appendJsonString(alloc, out, shape.field);
    try appendTextQueryBoost(alloc, out, shape.boost);
    try out.append(alloc, '}');
}

pub fn appendTextQueryBoost(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    boost: f32,
) !void {
    if (!std.math.isFinite(boost)) return error.InvalidQueryRequest;
    if (boost == 1.0) return;
    try out.appendSlice(alloc, ",\"boost\":");
    try out.print(alloc, "{d}", .{boost});
}

pub fn appendOptionalTextQueryBoostField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    boost: f32,
) !void {
    if (!std.math.isFinite(boost)) return error.InvalidQueryRequest;
    if (boost != 1.0) try appendJsonFieldF32(alloc, out, first, "boost", boost);
}

pub fn parseRemoteSearchResult(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.SearchResult {
    return parseRemoteSearchResultInner(alloc, body) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidRemoteResponse,
    };
}

pub fn parseRemoteSearchResultInner(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.SearchResult {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, body, .{});
    defer parsed.deinit();
    const responses = parsed.value.responses orelse return error.InvalidQueryRequest;
    if (responses.len == 0) return error.InvalidQueryRequest;
    const response = responses[0];
    const hits_obj = response.hits orelse return error.InvalidQueryRequest;
    const total_obj = hits_obj.total orelse return error.InvalidQueryRequest;
    const hits_value = hits_obj.hits orelse return error.InvalidQueryRequest;
    const total_hits = try query_contract.queryHitsTotalValueToU32(total_obj);
    const total_hits_relation = try query_contract.parseTotalHitsRelation(total_obj.relation);

    const hits = try alloc.alloc(db_mod.types.SearchHit, hits_value.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    for (hits_value, 0..) |item, i| {
        var hit: db_mod.types.SearchHit = .{ .id = try alloc.dupe(u8, item._id) };
        errdefer hit.deinit(alloc);
        hit.score = item._score;
        hit.distance = item._distance;
        hit.index_scores = try parseRemoteIndexScoresAlloc(alloc, item._index_scores);
        hit.sort_values = try db_mod.types.cloneJsonValues(alloc, item._sort orelse &.{});
        hit.stored_data = if (item._source) |value| try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})}) else null;
        hit.ancestor_source_data = try remoteHierarchyAncestorDocumentAlloc(alloc, item.hierarchy, .source);
        hit.ancestor_unit_data = try remoteHierarchyAncestorDocumentAlloc(alloc, item.hierarchy, .unit);
        hit.artifact_ref = try parseRemoteHierarchyArtifactRefAlloc(alloc, item.hierarchy);
        hit.chunk_hits = try parseRemoteHierarchyMatchesAlloc(alloc, item.hierarchy);
        hits[i] = hit;
        initialized += 1;
    }

    const graph_results: []db_mod.types.GraphSearchResult = if (response.graph_results) |graph_results_value|
        try parseRemoteGraphResults(alloc, graph_results_value)
    else
        @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]);

    // The hit errdefer above already owns its cleanup until we return.
    errdefer {
        for (graph_results) |*graph_result| graph_result.deinit(alloc);
        if (graph_results.len > 0) alloc.free(graph_results);
    }
    var result: db_mod.types.SearchResult = .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = total_hits,
        .total_hits_relation = total_hits_relation,
        .graph_results = graph_results,
    };
    if (response.profile) |profile| {
        if (profile != .object) return error.InvalidRemoteResponse;
        if (profile.object.get("sort")) |value| {
            if (value != .null) {
                var sort = try std.json.parseFromValue(metadata_openapi.SortProfile, alloc, value, .{ .ignore_unknown_fields = true });
                defer sort.deinit();
                try result.setOwnedSortProfile(try remoteSortProfile(sort.value));
            }
        }
    }
    return result;
}

/// Restore the public diagnostics that the provider actually returned. All
/// strings are cloned by SearchResult before the response arena is released.
fn remoteSortProfile(wire: metadata_openapi.SortProfile) !db_mod.types.SortProfile {
    var profile: db_mod.types.SortProfile = .{};
    inline for (@typeInfo(db_mod.types.SortProfile).@"struct".fields) |field| {
        if (@hasField(metadata_openapi.SortProfile, field.name)) {
            if (@field(wire, field.name)) |value| {
                if (field.type == []const u8) {
                    @field(profile, field.name) = if (@typeInfo(@TypeOf(value)) == .@"enum") @tagName(value) else value;
                } else if (field.type == db_mod.types.SortProfileField) {
                    if (value.len > profile.sort_rejection_field.bytes.len) return error.InvalidRemoteResponse;
                    @field(profile, field.name) = .init(value);
                } else if (@typeInfo(field.type) == .int) {
                    @field(profile, field.name) = std.math.cast(field.type, value) orelse return error.InvalidRemoteResponse;
                } else {
                    @field(profile, field.name) = value;
                }
            }
        }
    }
    return profile;
}

pub fn parseStorageKernelSearchResult(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.SearchResult {
    return try parseRemoteSearchResult(alloc, body);
}

pub fn parseRemoteHierarchyMatchesAlloc(
    alloc: std.mem.Allocator,
    hierarchy: ?metadata_openapi.QueryHitHierarchy,
) ![]db_mod.types.ChunkHit {
    const value = hierarchy orelse return &.{};
    const matches = value.matches orelse value.chunks orelse return &.{};
    const out = try alloc.alloc(db_mod.types.ChunkHit, matches.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(out);
    }
    for (matches, 0..) |match, i| {
        var hit: db_mod.types.ChunkHit = .{ .id = try alloc.dupe(u8, match._id) };
        errdefer hit.deinit(alloc);
        hit.score = match._score;
        hit.distance = match._distance;
        hit.stored_data = if (match._source) |source| try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(source, .{})}) else null;
        hit.ancestor_source_data = try remoteHierarchyAncestorDocumentAlloc(alloc, match.hierarchy, .source);
        hit.ancestor_unit_data = try remoteHierarchyAncestorDocumentAlloc(alloc, match.hierarchy, .unit);
        hit.artifact_ref = try parseRemoteHierarchyArtifactRefAlloc(alloc, match.hierarchy);
        out[i] = hit;
        initialized += 1;
    }
    return out;
}

pub const RemoteHierarchyAncestorLevel = enum { source, unit };

pub fn remoteHierarchyAncestorDocumentAlloc(
    alloc: std.mem.Allocator,
    hierarchy: anytype,
    comptime level: RemoteHierarchyAncestorLevel,
) !?[]u8 {
    const ancestors = (hierarchy orelse return null).ancestors orelse return null;
    const ancestor = switch (level) {
        .source => ancestors.source,
        .unit => ancestors.unit orelse return null,
    };
    const document = ancestor.document orelse return null;
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})});
}

pub fn parseRemoteHierarchyArtifactRefAlloc(
    alloc: std.mem.Allocator,
    hierarchy: anytype,
) !?db_mod.types.ArtifactRef {
    const context = hierarchy orelse return null;
    const artifact = context.artifact orelse return null;
    const document_id = context.parent_doc_key orelse return error.InvalidQueryRequest;
    const kind = try parseRemoteArtifactKind(artifact.kind);
    const chunk_id = if (artifact.chunk_id) |value| std.math.cast(u32, value) orelse return error.InvalidQueryRequest else null;
    const owned_document_id = try alloc.dupe(u8, document_id);
    errdefer alloc.free(owned_document_id);
    const owned_name = try alloc.dupe(u8, artifact.name);
    errdefer alloc.free(owned_name);
    const owned_unit_id = if (artifact.unit_id) |unit_id| try alloc.dupe(u8, unit_id) else null;
    errdefer if (owned_unit_id) |unit_id| alloc.free(unit_id);
    const source = if (artifact.source) |source| try parseRemoteArtifactSourceRefAlloc(alloc, source) else null;
    return db_mod.types.ArtifactRef{
        .document_id = owned_document_id,
        .name = owned_name,
        .kind = kind,
        .chunk_id = chunk_id,
        .unit_id = owned_unit_id,
        .source = source,
    };
}

pub fn parseRemoteArtifactSourceRefAlloc(
    alloc: std.mem.Allocator,
    source: metadata_openapi.HierarchyArtifactSource,
) !db_mod.types.ArtifactSourceRef {
    const kind = try parseRemoteArtifactKind(source.kind);
    const chunk_id = if (source.chunk_id) |value| std.math.cast(u32, value) orelse return error.InvalidQueryRequest else null;
    const owned_name = try alloc.dupe(u8, source.name);
    errdefer alloc.free(owned_name);
    const owned_unit_id = if (source.unit_id) |unit_id| try alloc.dupe(u8, unit_id) else null;
    errdefer if (owned_unit_id) |unit_id| alloc.free(unit_id);
    return .{
        .name = owned_name,
        .kind = kind,
        .chunk_id = chunk_id,
        .unit_id = owned_unit_id,
    };
}

pub fn parseRemoteArtifactKind(value: []const u8) !db_mod.types.ArtifactKind {
    if (std.mem.eql(u8, value, "chunk")) return .chunk;
    if (std.mem.eql(u8, value, "asset")) return .asset;
    if (std.mem.eql(u8, value, "embedding")) return .embedding;
    return error.InvalidQueryRequest;
}

pub fn parseRemoteIndexScoresAlloc(
    alloc: std.mem.Allocator,
    maybe_value: ?std.json.ArrayHashMap(f64),
) ![]fusion_mod.IndexScore {
    const value = maybe_value orelse return &.{};
    const object = value.map;
    if (object.count() == 0) return &.{};

    var scores = try alloc.alloc(fusion_mod.IndexScore, object.count());
    var initialized: usize = 0;
    errdefer {
        for (scores[0..initialized]) |score| alloc.free(score.index_name);
        alloc.free(scores);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        scores[initialized] = .{
            .index_name = try alloc.dupe(u8, entry.key_ptr.*),
            .score = entry.value_ptr.*,
        };
        initialized += 1;
    }

    if (initialized == 0) {
        alloc.free(scores);
        return &.{};
    }
    if (initialized == scores.len) return scores;
    const trimmed = try alloc.realloc(scores, initialized);
    return trimmed;
}

pub fn parseRemoteGraphResults(
    alloc: std.mem.Allocator,
    value: std.json.ArrayHashMap(indexes_openapi.GraphResult),
) ![]db_mod.types.GraphSearchResult {
    if (value.map.count() > graph_query_mod.max_named_queries)
        return error.InvalidRemoteResponse;
    const results = try alloc.alloc(db_mod.types.GraphSearchResult, value.map.count());
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*graph_result| graph_result.deinit(alloc);
        alloc.free(results);
    }

    var it = value.map.iterator();
    while (it.next()) |entry| {
        const result_value = entry.value_ptr.*;
        // Inter-node responses are always canonical, even when the coordinator
        // is serving a legacy stateful client. Compatibility conversion occurs
        // once at public egress and never widens the trusted shard protocol.
        if (!graph_query_mod.isValidQueryName(entry.key_ptr.*))
            return error.InvalidRemoteResponse;
        const ResultView = struct {
            canonical_nodes: ?[]const indexes_openapi.GraphResultNode = null,
            canonical_path_results: ?[]const indexes_openapi.GraphPathResult = null,
            rows: ?[]const indexes_openapi.GraphResultRow = null,
            aggregates: ?std.json.ArrayHashMap(indexes_openapi.GraphAggregateValue) = null,
            truncated: bool = false,
        };
        const view: ResultView = switch (result_value) {
            .graph_nodes_result => |result| blk: {
                if (result.nodes.len > public_limits.max_graph_result_items)
                    return error.InvalidRemoteResponse;
                if (!remoteGraphReturnedItemsMatch(result.stats.returned_items, result.nodes.len))
                    return error.InvalidRemoteResponse;
                break :blk .{
                    .canonical_nodes = result.nodes,
                    .truncated = result.stats.truncated,
                };
            },
            .graph_paths_result => |result| blk: {
                if (result.paths.len > public_limits.max_graph_result_items or
                    !remoteGraphReturnedItemsMatch(result.stats.returned_items, result.paths.len))
                    return error.InvalidRemoteResponse;
                break :blk .{ .canonical_path_results = result.paths };
            },
            .graph_bindings_result => |result| blk: {
                if (result.rows.len > public_limits.max_graph_result_items or
                    !remoteGraphReturnedItemsMatch(result.stats.returned_items, result.rows.len))
                    return error.InvalidRemoteResponse;
                break :blk .{
                    .rows = result.rows,
                    .truncated = result.stats.truncated,
                };
            },
            .graph_aggregates_result => |result| blk: {
                if (result.aggregates.map.count() == 0 or
                    result.aggregates.map.count() > graph_pattern_mod.max_count_aggregates or
                    !remoteGraphReturnedItemsMatch(result.stats.returned_items, result.aggregates.map.count()))
                    return error.InvalidRemoteResponse;
                break :blk .{
                    .aggregates = result.aggregates,
                    .truncated = false,
                };
            },
        };
        var parsed_nodes = if (view.canonical_nodes) |nodes_value|
            try parseRemoteGraphNodes(alloc, nodes_value)
        else if (view.canonical_path_results) |path_results|
            try parseRemoteGraphPathResultNodes(alloc, path_results)
        else
            ParsedRemoteGraphNodes{};
        errdefer parsed_nodes.deinit(alloc);
        var parsed_matches = if (view.rows) |rows_value|
            try parseRemoteGraphRows(alloc, rows_value)
        else
            ParsedRemoteGraphMatches{};
        errdefer parsed_matches.deinit(alloc);
        const paths: []graph_paths.Path = if (view.canonical_path_results) |path_results|
            try parseRemoteCanonicalGraphPathResults(alloc, path_results)
        else
            @constCast((&[_]graph_paths.Path{})[0..]);
        errdefer {
            for (paths) |path| graph_paths.freePath(alloc, path);
            if (paths.len > 0) alloc.free(paths);
        }
        const aggregates = if (view.aggregates) |aggregates_value|
            try parseRemoteGraphAggregates(alloc, aggregates_value)
        else
            @constCast((&[_]db_mod.types.GraphAggregateResult{})[0..]);
        errdefer {
            for (aggregates) |*aggregate| aggregate.deinit(alloc);
            if (aggregates.len > 0) alloc.free(aggregates);
        }

        const joined_hits = try concatGraphResultHits(alloc, parsed_nodes.hits, parsed_matches.hits);
        errdefer {
            for (joined_hits) |*hit| hit.deinit(alloc);
            if (joined_hits.len > 0) alloc.free(joined_hits);
        }
        for (parsed_nodes.hits) |*hit| hit.deinit(alloc);
        if (parsed_nodes.hits.len > 0) alloc.free(parsed_nodes.hits);
        parsed_nodes.hits = &.{};
        for (parsed_matches.hits) |*hit| hit.deinit(alloc);
        if (parsed_matches.hits.len > 0) alloc.free(parsed_matches.hits);
        parsed_matches.hits = &.{};

        results[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .nodes = parsed_nodes.nodes,
            .paths = paths,
            .matches = parsed_matches.matches,
            .aggregates = aggregates,
            .hits = joined_hits,
            .total_hits = @intCast(@max(parsed_nodes.nodes.len, @max(paths.len, parsed_matches.matches.len))),
            .truncated = view.truncated,
        };
        initialized += 1;
    }

    return results;
}

pub fn remoteGraphReturnedItemsMatch(value: i64, actual: usize) bool {
    const parsed = std.math.cast(usize, value) orelse return false;
    return parsed == actual;
}

pub fn parseRemoteGraphAggregates(
    alloc: std.mem.Allocator,
    value: std.json.ArrayHashMap(indexes_openapi.GraphAggregateValue),
) ![]db_mod.types.GraphAggregateResult {
    const aggregates = try alloc.alloc(db_mod.types.GraphAggregateResult, value.map.count());
    var initialized: usize = 0;
    errdefer {
        for (aggregates[0..initialized]) |*aggregate| aggregate.deinit(alloc);
        alloc.free(aggregates);
    }
    var it = value.map.iterator();
    while (it.next()) |entry| {
        if (!graph_query_mod.isValidIdentifier(entry.key_ptr.*) or !entry.value_ptr.exact)
            return error.InvalidRemoteResponse;
        const parsed_value = std.fmt.parseInt(u128, entry.value_ptr.value, 10) catch return error.InvalidRemoteResponse;
        aggregates[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .value = parsed_value,
            .exact = entry.value_ptr.exact,
        };
        initialized += 1;
    }
    return aggregates;
}

pub const ParsedRemoteGraphNodes = struct {
    nodes: []graph_query_mod.GraphResultNode = &.{},
    hits: []db_mod.types.SearchHit = &.{},

    pub fn deinit(self: ParsedRemoteGraphNodes, alloc: std.mem.Allocator) void {
        for (self.nodes) |*node| node.deinit(alloc);
        if (self.nodes.len > 0) alloc.free(self.nodes);
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
    }
};

pub const ParsedRemoteGraphMatches = struct {
    matches: []db_mod.types.GraphPatternMatch = &.{},
    hits: []db_mod.types.SearchHit = &.{},

    pub fn deinit(self: ParsedRemoteGraphMatches, alloc: std.mem.Allocator) void {
        for (self.matches) |*match| match.deinit(alloc);
        if (self.matches.len > 0) alloc.free(self.matches);
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
    }
};

pub fn parseRemoteGraphNodes(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphResultNode,
) !ParsedRemoteGraphNodes {
    if (value.len > public_limits.max_graph_result_items)
        return error.InvalidRemoteResponse;
    const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, value.len);
    var initialized: usize = 0;
    errdefer {
        for (nodes[0..initialized]) |*node| node.deinit(alloc);
        alloc.free(nodes);
    }
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (value, 0..) |item, i| {
        nodes[i] = try parseRemoteGraphNodeWithKey(alloc, item.key, item);
        initialized += 1;
        if (item.document) |document| {
            try appendRemoteGraphDocumentHit(alloc, &hits, item.key, item.table, document);
        }
    }
    return .{
        .nodes = nodes,
        .hits = try hits.toOwnedSlice(alloc),
    };
}

pub fn parseRemoteGraphPathResultNodes(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphPathResult,
) !ParsedRemoteGraphNodes {
    const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, value.len);
    var initialized: usize = 0;
    errdefer {
        for (nodes[0..initialized]) |*node| node.deinit(alloc);
        if (nodes.len > 0) alloc.free(nodes);
    }
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }
    for (value, 0..) |item, i| {
        if (item.path.nodes.len == 0) return error.InvalidRemoteResponse;
        const terminal = item.path.nodes[item.path.nodes.len - 1];
        nodes[i] = try parseRemoteGraphNodeWithKey(alloc, terminal.key, .{
            .key = terminal.key,
            .table = terminal.table,
            .depth = item.path.length,
            .document = item.document,
        });
        initialized += 1;
        if (item.document) |document|
            try appendRemoteGraphDocumentHit(alloc, &hits, terminal.key, terminal.table, document);
    }
    return .{ .nodes = nodes, .hits = try hits.toOwnedSlice(alloc) };
}

pub const OwnedRemoteGraphNodePath = struct {
    nodes: [][]const u8,
    tables: ?[]const ?[]const u8,

    pub fn deinit(self: OwnedRemoteGraphNodePath, alloc: std.mem.Allocator) void {
        freeRemoteGraphNodePath(alloc, self.nodes);
        if (self.tables) |tables| {
            for (tables) |table| if (table) |value| alloc.free(value);
            if (tables.len > 0) alloc.free(tables);
        }
    }
};

pub fn cloneRemoteCanonicalGraphNodePath(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphPathEndpoint,
) !OwnedRemoteGraphNodePath {
    const nodes = try alloc.alloc([]const u8, value.len);
    var initialized_nodes: usize = 0;
    errdefer {
        for (nodes[0..initialized_nodes]) |node| alloc.free(node);
        if (nodes.len > 0) alloc.free(nodes);
    }
    const tables = try alloc.alloc(?[]const u8, value.len);
    @memset(tables, null);
    var initialized_tables: usize = 0;
    errdefer {
        for (tables[0..initialized_tables]) |table| if (table) |item| alloc.free(item);
        if (tables.len > 0) alloc.free(tables);
    }
    var has_qualified_identity = false;
    for (value, 0..) |endpoint, i| {
        nodes[i] = try alloc.dupe(u8, endpoint.key);
        initialized_nodes += 1;
        tables[i] = if (endpoint.table) |table| blk: {
            has_qualified_identity = true;
            break :blk try alloc.dupe(u8, table);
        } else null;
        initialized_tables += 1;
    }
    if (!has_qualified_identity) {
        if (tables.len > 0) alloc.free(tables);
        return .{ .nodes = nodes, .tables = null };
    }
    return .{ .nodes = nodes, .tables = tables };
}

pub fn parseRemoteGraphNodeWithKey(
    alloc: std.mem.Allocator,
    key: []const u8,
    item: indexes_openapi.GraphResultNode,
) !graph_query_mod.GraphResultNode {
    try validateRemoteCanonicalGraphIdentity(key, item.table);
    const depth = std.math.cast(u32, item.depth) orelse return error.InvalidRemoteResponse;
    if (depth > graph_pattern_mod.max_pattern_hops) return error.InvalidRemoteResponse;
    const distance: f64 = @floatFromInt(depth);
    if (item.path) |path| {
        try validateRemoteCanonicalGraphPathNodes(path);
        if (depth != path.len - 1 or !graphPathEndpointEql(path[path.len - 1], .{
            .key = key,
            .table = item.table,
        })) return error.InvalidRemoteResponse;
    }
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (item.table) |table| try alloc.dupe(u8, table) else null;
    errdefer if (owned_table) |table| alloc.free(table);
    const owned_path = if (item.path) |value| try cloneRemoteCanonicalGraphNodePath(alloc, value) else null;
    errdefer if (owned_path) |value| value.deinit(alloc);
    const path_edges = if (item.path_edges) |value| blk: {
        const path = item.path orelse return error.InvalidRemoteResponse;
        try validateRemoteCanonicalGraphPathEdges(path, value);
        break :blk try cloneRemoteCanonicalGraphNodePathEdges(alloc, value);
    } else null;
    errdefer if (path_edges) |value| freeRemoteGraphNodePathEdges(alloc, value);
    const provenance = if (item.provenance) |value| try cloneRemoteGraphNodePath(alloc, value) else null;
    errdefer if (provenance) |value| freeRemoteGraphNodePath(alloc, value);
    return .{
        .key = owned_key,
        .table = owned_table,
        .depth = depth,
        .distance = distance,
        .path = if (owned_path) |value| value.nodes else null,
        .path_tables = if (owned_path) |value| value.tables else null,
        .path_edges = path_edges,
        .provenance = provenance,
    };
}

pub fn remoteGraphDocumentHit(
    alloc: std.mem.Allocator,
    key: []const u8,
    table: ?[]const u8,
    document: std.json.ArrayHashMap(std.json.Value),
) !db_mod.types.SearchHit {
    const id = try alloc.dupe(u8, key);
    errdefer alloc.free(id);
    const source_table = if (table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (source_table) |value| alloc.free(value);
    const stored_data = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})});
    errdefer alloc.free(stored_data);
    return .{ .id = id, .source_table = source_table, .score = null, .stored_data = stored_data };
}

pub fn appendRemoteGraphDocumentHit(
    alloc: std.mem.Allocator,
    hits: *std.ArrayListUnmanaged(db_mod.types.SearchHit),
    key: []const u8,
    table: ?[]const u8,
    document: std.json.ArrayHashMap(std.json.Value),
) !void {
    var hit = try remoteGraphDocumentHit(alloc, key, table, document);
    errdefer hit.deinit(alloc);
    try hits.append(alloc, hit);
}

pub fn appendRemoteGraphBinding(
    alloc: std.mem.Allocator,
    bindings: *std.ArrayListUnmanaged(db_mod.types.GraphPatternBinding),
    alias: []const u8,
    node_value: indexes_openapi.GraphBindingNode,
) !void {
    const owned_alias = try alloc.dupe(u8, alias);
    errdefer alloc.free(owned_alias);
    try validateRemoteCanonicalGraphIdentity(node_value.key, node_value.table);
    const owned_key = try alloc.dupe(u8, node_value.key);
    errdefer alloc.free(owned_key);
    const owned_table = if (node_value.table) |table| try alloc.dupe(u8, table) else null;
    errdefer if (owned_table) |table| alloc.free(table);
    var node = graph_query_mod.GraphResultNode{
        .key = owned_key,
        .table = owned_table,
        .depth = 0,
        .distance = 0,
    };
    errdefer node.deinit(alloc);
    try bindings.append(alloc, .{ .alias = owned_alias, .node = node });
}

pub fn parseRemoteGraphRows(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphResultRow,
) !ParsedRemoteGraphMatches {
    if (value.len > public_limits.max_graph_result_items)
        return error.InvalidRemoteResponse;
    const matches = try alloc.alloc(db_mod.types.GraphPatternMatch, value.len);
    var initialized_matches: usize = 0;
    errdefer {
        for (matches[0..initialized_matches]) |*match| match.deinit(alloc);
        if (matches.len > 0) alloc.free(matches);
    }
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }
    for (value, 0..) |row, i| {
        if (row.map.count() > graph_pattern_mod.max_conjunctive_nodes)
            return error.InvalidRemoteResponse;
        var bindings = std.ArrayListUnmanaged(db_mod.types.GraphPatternBinding).empty;
        errdefer {
            for (bindings.items) |*binding| binding.deinit(alloc);
            bindings.deinit(alloc);
        }
        var null_aliases = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (null_aliases.items) |alias| alloc.free(alias);
            null_aliases.deinit(alloc);
        }
        var it = row.map.iterator();
        while (it.next()) |entry| {
            if (!graph_query_mod.isValidIdentifier(entry.key_ptr.*))
                return error.InvalidRemoteResponse;
            const node_value = entry.value_ptr.* orelse {
                const alias = try alloc.dupe(u8, entry.key_ptr.*);
                errdefer alloc.free(alias);
                try null_aliases.append(alloc, alias);
                continue;
            };
            try appendRemoteGraphBinding(alloc, &bindings, entry.key_ptr.*, node_value);
            if (node_value.document) |document| {
                try appendRemoteGraphDocumentHit(alloc, &hits, node_value.key, node_value.table, document);
            }
        }
        const owned_bindings = try bindings.toOwnedSlice(alloc);
        errdefer {
            for (owned_bindings) |*binding| binding.deinit(alloc);
            if (owned_bindings.len > 0) alloc.free(owned_bindings);
        }
        const owned_null_aliases = try null_aliases.toOwnedSlice(alloc);
        errdefer {
            for (owned_null_aliases) |alias| alloc.free(alias);
            if (owned_null_aliases.len > 0) alloc.free(owned_null_aliases);
        }
        matches[i] = .{
            .bindings = owned_bindings,
            .path = &.{},
            .null_aliases = owned_null_aliases,
        };
        initialized_matches += 1;
    }
    return .{ .matches = matches, .hits = try hits.toOwnedSlice(alloc) };
}

pub fn concatGraphResultHits(
    alloc: std.mem.Allocator,
    left: []db_mod.types.SearchHit,
    right: []db_mod.types.SearchHit,
) ![]db_mod.types.SearchHit {
    const out = try alloc.alloc(db_mod.types.SearchHit, left.len + right.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*hit| hit.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (left) |hit| {
        out[initialized] = try hit.clone(alloc);
        initialized += 1;
    }
    for (right) |hit| {
        out[initialized] = try hit.clone(alloc);
        initialized += 1;
    }
    return out;
}

pub fn cloneRemoteGraphNodePath(alloc: std.mem.Allocator, value: []const []const u8) ![][]const u8 {
    const out = try alloc.alloc([]const u8, value.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }
    for (value, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return out;
}

pub fn freeRemoteGraphNodePath(alloc: std.mem.Allocator, value: []const []const u8) void {
    for (value) |item| alloc.free(item);
    if (value.len > 0) alloc.free(value);
}

pub fn cloneRemoteCanonicalGraphNodePathEdges(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphPathEdge,
) ![]graph_query_mod.PathEdgeInfo {
    const edges = try alloc.alloc(graph_query_mod.PathEdgeInfo, value.len);
    var initialized: usize = 0;
    errdefer freeRemoteGraphNodePathEdgeItems(alloc, edges, initialized);
    for (value, 0..) |item, i| {
        const source_key = if (item.direction == .out) item.from.key else item.to.key;
        const target_key = if (item.direction == .out) item.to.key else item.from.key;
        const source = try alloc.dupe(u8, source_key);
        errdefer alloc.free(source);
        const target = try alloc.dupe(u8, target_key);
        errdefer alloc.free(target);
        const edge_type = try alloc.dupe(u8, item.type);
        errdefer alloc.free(edge_type);
        const metadata = if (item.metadata) |metadata| try std.json.Stringify.valueAlloc(alloc, metadata, .{}) else "";
        errdefer if (metadata.len > 0) alloc.free(metadata);
        edges[i] = .{
            .source = source,
            .target = target,
            .edge_type = edge_type,
            .weight = item.weight,
            .metadata = metadata,
            .traversal_direction = switch (item.direction) {
                .out => .out,
                .in => .in,
            },
        };
        initialized += 1;
    }
    return edges;
}

pub fn freeRemoteGraphNodePathEdgeItems(
    alloc: std.mem.Allocator,
    edges: []const graph_query_mod.PathEdgeInfo,
    initialized: usize,
) void {
    for (edges[0..initialized]) |edge| {
        alloc.free(edge.source);
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
        if (edge.metadata.len > 0) alloc.free(edge.metadata);
    }
    if (edges.len > 0) alloc.free(edges);
}

pub fn freeRemoteGraphNodePathEdges(alloc: std.mem.Allocator, edges: []const graph_query_mod.PathEdgeInfo) void {
    freeRemoteGraphNodePathEdgeItems(alloc, edges, edges.len);
}

pub fn parseRemoteCanonicalGraphPathResults(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphPathResult,
) ![]graph_paths.Path {
    const paths = try alloc.alloc(graph_paths.Path, value.len);
    var initialized: usize = 0;
    errdefer {
        for (paths[0..initialized]) |path| graph_paths.freePath(alloc, path);
        if (paths.len > 0) alloc.free(paths);
    }
    for (value, 0..) |item, i| {
        paths[i] = try parseRemoteCanonicalGraphPath(alloc, item.path);
        initialized += 1;
    }
    return paths;
}

pub fn parseRemoteCanonicalGraphPath(
    alloc: std.mem.Allocator,
    item: indexes_openapi.GraphPath,
) !graph_paths.Path {
    if (item.length < 0 or item.length > graph_pattern_mod.max_pattern_hops or
        @as(u64, @intCast(item.length)) != item.edges.len)
        return error.InvalidRemoteResponse;
    try validateRemoteCanonicalGraphPathScores(item);
    const nodes = try alloc.alloc([]const u8, item.nodes.len);
    var initialized_nodes: usize = 0;
    errdefer {
        for (nodes[0..initialized_nodes]) |node| alloc.free(node);
        alloc.free(nodes);
    }
    const has_qualified_node = for (item.nodes) |node| {
        if (node.table != null) break true;
    } else false;
    const node_tables: []?[]const u8 = if (has_qualified_node)
        try alloc.alloc(?[]const u8, item.nodes.len)
    else
        @constCast((&[_]?[]const u8{})[0..]);
    if (node_tables.len > 0) @memset(node_tables, null);
    var initialized_tables: usize = 0;
    errdefer {
        for (node_tables[0..initialized_tables]) |table| if (table) |value| alloc.free(value);
        if (node_tables.len > 0) alloc.free(node_tables);
    }
    for (item.nodes, 0..) |node, i| {
        nodes[i] = try alloc.dupe(u8, node.key);
        initialized_nodes += 1;
        if (has_qualified_node) {
            node_tables[i] = if (node.table) |table| try alloc.dupe(u8, table) else null;
            initialized_tables += 1;
        }
    }
    try validateRemoteCanonicalGraphPathEdges(item.nodes, item.edges);
    const edges = try parseRemoteCanonicalPathEdges(alloc, item.edges);
    errdefer {
        for (edges) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.metadata.len > 0) alloc.free(edge.metadata);
        }
        alloc.free(edges);
    }
    return .{
        .nodes = nodes,
        .node_tables = node_tables,
        .edges = edges,
        .total_weight = item.weight_sum,
        .length = std.math.cast(u32, item.length) orelse return error.InvalidRemoteResponse,
    };
}

pub fn validateRemoteCanonicalGraphPathScores(item: indexes_openapi.GraphPath) !void {
    if (!std.math.isFinite(item.weight_sum) or !std.math.isFinite(item.objective_value))
        return error.InvalidRemoteResponse;
    var sum: f64 = 0;
    var product: f64 = 1;
    for (item.edges) |edge| {
        graph_edge_weight.validateStored(edge.weight) catch return error.InvalidRemoteResponse;
        switch (item.objective) {
            .min_hops => {},
            .min_weight_sum => _ = graph_paths.pathEdgeCost(.min_weight, edge.weight) catch
                return error.InvalidRemoteResponse,
            .max_weight_product => _ = graph_paths.pathEdgeCost(.max_weight, edge.weight) catch
                return error.InvalidRemoteResponse,
        }
        sum += edge.weight;
        if (!std.math.isFinite(sum)) return error.InvalidRemoteResponse;
        if (item.objective == .max_weight_product) {
            product *= edge.weight;
            if (!std.math.isFinite(product)) return error.InvalidRemoteResponse;
        }
    }
    if (!graphPathScoreEql(item.weight_sum, sum)) return error.InvalidRemoteResponse;
    const objective: f64 = switch (item.objective) {
        .min_hops => @floatFromInt(item.edges.len),
        .min_weight_sum => sum,
        .max_weight_product => product,
    };
    if (!std.math.isFinite(objective) or !graphPathScoreEql(item.objective_value, objective))
        return error.InvalidRemoteResponse;
}

pub fn graphPathScoreEql(left: f64, right: f64) bool {
    if (!std.math.isFinite(left) or !std.math.isFinite(right)) return false;
    const scale = @max(@as(f64, 1), @max(@abs(left), @abs(right)));
    return @abs(left - right) <= 1e-12 * scale;
}

pub fn parseRemoteCanonicalPathEdges(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphPathEdge,
) ![]graph_paths.PathEdge {
    const edges = try alloc.alloc(graph_paths.PathEdge, value.len);
    var initialized: usize = 0;
    errdefer {
        for (edges[0..initialized]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.metadata.len > 0) alloc.free(edge.metadata);
        }
        if (edges.len > 0) alloc.free(edges);
    }
    for (value, 0..) |item, i| {
        const source_key = if (item.direction == .out) item.from.key else item.to.key;
        const target_key = if (item.direction == .out) item.to.key else item.from.key;
        const source = try alloc.dupe(u8, source_key);
        errdefer alloc.free(source);
        const target = try alloc.dupe(u8, target_key);
        errdefer alloc.free(target);
        const edge_type = try alloc.dupe(u8, item.type);
        errdefer alloc.free(edge_type);
        const metadata = if (item.metadata) |metadata| try std.json.Stringify.valueAlloc(alloc, metadata, .{}) else "";
        errdefer if (metadata.len > 0) alloc.free(metadata);
        edges[i] = .{
            .source = source,
            .target = target,
            .edge_type = edge_type,
            .weight = item.weight,
            .metadata = metadata,
            .traversal_direction = switch (item.direction) {
                .out => .out,
                .in => .in,
            },
        };
        initialized += 1;
    }
    return edges;
}

pub fn validateRemoteCanonicalGraphPathEdges(
    nodes: []const indexes_openapi.GraphPathEndpoint,
    edges: []const indexes_openapi.GraphPathEdge,
) !void {
    try validateRemoteCanonicalGraphPathNodes(nodes);
    if (edges.len != nodes.len - 1) return error.InvalidRemoteResponse;
    for (edges, 0..) |edge, i| {
        graph_edge_type.validateStored(edge.type) catch return error.InvalidRemoteResponse;
        graph_edge_weight.validateStored(edge.weight) catch return error.InvalidRemoteResponse;
        try validateRemoteCanonicalGraphIdentity(edge.from.key, edge.from.table);
        try validateRemoteCanonicalGraphIdentity(edge.to.key, edge.to.table);
        if (!graphPathEndpointEql(edge.from, nodes[i]) or
            !graphPathEndpointEql(edge.to, nodes[i + 1]))
            return error.InvalidRemoteResponse;
    }
}

pub fn validateRemoteCanonicalGraphPathNodes(
    nodes: []const indexes_openapi.GraphPathEndpoint,
) !void {
    if (nodes.len == 0 or nodes.len > graph_pattern_mod.max_pattern_hops + 1)
        return error.InvalidRemoteResponse;
    for (nodes) |node| try validateRemoteCanonicalGraphIdentity(node.key, node.table);
}

pub fn validateRemoteCanonicalGraphIdentity(key: []const u8, table: ?[]const u8) !void {
    if (key.len == 0) return error.InvalidRemoteResponse;
    if (table) |value| if (value.len == 0) return error.InvalidRemoteResponse;
}

pub fn graphPathEndpointEql(
    left: indexes_openapi.GraphPathEndpoint,
    right: indexes_openapi.GraphPathEndpoint,
) bool {
    return graph_node_identity.equal(
        .{ .table = left.table, .key = left.key },
        .{ .table = right.table, .key = right.key },
    );
}

pub fn appendJsonFieldF32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f32,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

pub fn appendJsonFieldF64(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f64,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

pub fn appendJsonFieldBool(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: bool,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

pub fn appendJsonFieldNames(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    fields: []const []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '[');
    for (fields, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, field);
    }
    try out.append(alloc, ']');
}

pub fn appendJsonStringArray(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const []const u8,
) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, value);
    }
    try out.append(alloc, ']');
}

pub fn appendScanLine(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    projected_json: ?[]const u8,
    content_hash: ?db_mod.types.DocumentContentHash,
) !void {
    const escaped_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(key, .{})});
    defer alloc.free(escaped_key);

    try out.appendSlice(alloc, "{\"_id\":");
    try out.appendSlice(alloc, escaped_key);
    if (content_hash) |digest| {
        const encoded = std.fmt.bytesToHex(digest, .lower);
        try out.appendSlice(alloc, ",\"_content_hash\":\"");
        try out.appendSlice(alloc, &encoded);
        try out.append(alloc, '\"');
    }
    if (projected_json) |json| {
        try appendScanProjectedFields(alloc, out, json);
    } else {
        try out.append(alloc, '}');
    }
    try out.append(alloc, '\n');
}

pub fn appendScanProjectedFields(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    projected_json: []const u8,
) !void {
    if (projected_json.len < 2 or projected_json[0] != '{' or projected_json[projected_json.len - 1] != '}') return error.InvalidProjectedDocumentJson;
    if (projected_json.len == 2) {
        try out.append(alloc, '}');
        return;
    }

    if (std.mem.indexOf(u8, projected_json, "\"_id\"") == null) {
        try out.append(alloc, ',');
        try out.appendSlice(alloc, projected_json[1..]);
        return;
    }

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, projected_json, .{}) catch return error.InvalidProjectedDocumentJson;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidProjectedDocumentJson;

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "_id")) continue;
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded_value = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.*, .{});
        defer alloc.free(encoded_value);
        try out.appendSlice(alloc, encoded_value);
    }
    try out.append(alloc, '}');
}

pub const CivilDate = struct {
    year: i64,
    month: i64,
    day: i64,
};

pub fn formatRfc3339Ns(alloc: std.mem.Allocator, value_ns: u64) ![]u8 {
    const secs_total: u64 = @divFloor(value_ns, std.time.ns_per_s);
    const nanos: u64 = @mod(value_ns, std.time.ns_per_s);
    const days: i64 = @intCast(@divFloor(secs_total, 86_400));
    const secs_of_day: u64 = @mod(secs_total, 86_400);
    const date = civilFromDays(days);
    const year: u64 = @intCast(date.year);
    const month: u64 = @intCast(date.month);
    const day: u64 = @intCast(date.day);
    const hour: u64 = secs_of_day / 3_600;
    const minute: u64 = (secs_of_day % 3_600) / 60;
    const second: u64 = secs_of_day % 60;
    if (nanos == 0) {
        return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
            year, month, day, hour, minute, second,
        });
    }
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
        year, month, day, hour, minute, second, nanos,
    });
}

pub fn civilFromDays(days_since_epoch: i64) CivilDate {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1_460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const day = doy - @divFloor(153 * mp + 2, 5) + 1;
    const month = mp + (if (mp < 10) @as(i64, 3) else @as(i64, -9));
    const year = y + (if (month <= 2) @as(i64, 1) else @as(i64, 0));
    return .{ .year = year, .month = month, .day = day };
}
