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

//! Explicit bag-preserving relational adapters. Prove the whole pipeline before
//! negotiating its physical cursor; no producer is advanced during planning.
const std = @import("std");
const binding = @import("relation_binding.zig");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const describe = @import("describe.zig");
const A = std.mem.Allocator;
const Names = std.StringHashMapUnmanaged(void);

pub const Plan = struct {
    const Stage = struct { node: *const binding.Node, fields: []const []const []const u8 };
    pub const Terminal = union(enum) {
        scan,
        ordinary,
        replay: struct { child: *Plan },
        window: struct { child: *Plan },
        union_all: struct { left: *Plan, right: *Plan },
        cross: struct { child: *Plan, build: *Plan, singleton: *const binding.Node, graph_left: bool, factor_only: bool },
    };
    source: *const binding.Node,
    terminal: Terminal = .scan,
    requests: []const catalog.AggregateWeight,
    stages: []const Stage,
    physical: []const catalog.AggregateWeight,
    width: usize,

    pub fn init(a: A, root: *const binding.Node, requests: []const catalog.AggregateWeight, cursors: []const catalog.Cursor) anyerror!?Plan {
        // Adapters may negotiate with a temporary request descriptor. The
        // operator tree retains it across pulls, so own both descriptors and
        // field vectors before returning to the caller.
        const owned = try a.alloc(catalog.AggregateWeight, requests.len);
        for (requests, owned) |request, *out| {
            const fields = try a.alloc([]const u8, request.fields.len);
            for (request.fields, fields) |field, *name| name.* = try a.dupe(u8, field);
            const invariants = try a.alloc([]const u8, request.invariants.len);
            for (request.invariants, invariants) |field, *name| name.* = try a.dupe(u8, field);
            out.* = .{ .fields = fields, .multiplicity = request.multiplicity, .summary = if (request.summary) |name| try a.dupe(u8, name) else null, .invariants = invariants, .summaries = try ownCertificates(a, request.summaries) };
        }
        return initDepth(a, root, owned, cursors, 0);
    }
    fn ownCertificates(a: A, input: []const catalog.SummaryCertificate) ![]const catalog.SummaryCertificate {
        const result = try a.alloc(catalog.SummaryCertificate, input.len);
        for (input, result) |certificate, *out| {
            const stable = try a.alloc([]const u8, certificate.invariants.len);
            for (certificate.invariants, stable) |field, *owned| owned.* = try a.dupe(u8, field);
            out.* = .{ .field = try a.dupe(u8, certificate.field), .invariants = stable, .joint = certificate.joint, .linear = certificate.linear };
        }
        return result;
    }
    fn certificates(a: A, request: catalog.AggregateWeight) ![]const catalog.SummaryCertificate {
        const field = request.summary orelse return request.summaries;
        // A joint proof must not hide a legacy single-input alternative. A
        // projection may be affine in one varying input times an invariant,
        // while its all-input shared-marginal proof correctly declines.
        const result = try a.alloc(catalog.SummaryCertificate, request.summaries.len + 1);
        @memcpy(result[0..request.summaries.len], request.summaries);
        result[request.summaries.len] = .{ .field = field, .invariants = request.invariants };
        return result;
    }
    fn projectionInputs(a: A, fields: *Names, node: *const binding.Node, ordinal: usize) !void {
        const query = node.operation.query;
        const bound = query.binding.scalars;
        if (ordinal < bound.projections.len) if (bound.projections[ordinal]) |program| {
            try programFields(a, fields, program, bound.columns);
            return;
        };
        try fields.put(a, query.statement.columns[ordinal].field, {});
    }
    const LinearCheck = @import("cypher").linear_bounds.Check;
    const FieldBounds = @import("cypher").linear_bounds.Field;
    fn fieldBounds(fields: []const FieldBounds, name: []const u8) ?@import("cypher").linear_bounds.Interval {
        for (fields) |field| if (std.mem.eql(u8, field.name, name)) return field.bounds;
        return null;
    }
    const RenamedCheck = struct {
        next: LinearCheck,
        from: []const []const u8,
        to: []const []const u8,
        fn validate(ptr: *const anyopaque, a: A, fields: []const FieldBounds) !bool {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const renamed = try a.alloc(FieldBounds, self.from.len);
            for (self.from, self.to, renamed) |from, to, *out| {
                out.* = .{ .name = from, .bounds = fieldBounds(fields, to) };
                for (fields) |field| if (std.mem.eql(u8, field.name, to)) {
                    out.numeric = field.numeric;
                    out.polynomial = field.polynomial;
                    break;
                };
            }
            return self.next.accepts(a, renamed);
        }
        fn statistic(ptr: *const anyopaque, a: A, fields: []const FieldBounds) !?@import("cypher").statistic.Form {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const renamed = try a.alloc(FieldBounds, self.from.len);
            for (self.from, self.to, renamed) |from, to, *out| {
                out.* = .{ .name = from, .bounds = fieldBounds(fields, to) };
                for (fields) |field| if (std.mem.eql(u8, field.name, to)) {
                    out.numeric = field.numeric;
                    out.polynomial = field.polynomial;
                    break;
                };
            }
            return self.next.statisticProof(a, renamed);
        }
    };
    fn renameCheck(a: A, check: ?LinearCheck, from: []const []const u8, to: []const []const u8) !?LinearCheck {
        const original = check orelse return null;
        const owned = try a.create(RenamedCheck);
        owned.* = .{ .next = original, .from = from, .to = to };
        return .{ .ptr = owned, .validate = RenamedCheck.validate, .statistic = if (original.statistic != null) RenamedCheck.statistic else null, .statistic_kind = original.statistic_kind, .statistic_global = original.statistic_global, .integers = original.integers, .scalars = original.scalars, .total_only = original.total_only, .numeric_required = original.numeric_required };
    }
    const ProjectedCheck = struct {
        next: LinearCheck,
        node: *const binding.Node,
        fields: []const []const u8,
        fn validate(ptr: *const anyopaque, a: A, fields: []const FieldBounds) !bool {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const query = self.node.operation.query;
            const bound = query.binding.scalars;
            const projected = try a.alloc(FieldBounds, self.fields.len);
            for (self.fields, projected) |field, *out| {
                const ordinal = columnIndex(self.node, field) orelse return false;
                const program = if (ordinal < bound.projections.len) bound.projections[ordinal] else null;
                const interval = if (program) |p| p.linearBounds(bound.columns, fields, self.next.integers) else fieldBounds(fields, query.statement.columns[ordinal].field);
                // A demanded or otherwise observed arithmetic projection must
                // retain every intermediate error, even if a reducer ignores it.
                if (program) |p| if (p.affineTogether() and interval == null) return false;
                const numeric = if (program) |p| try p.numericProof(a, bound.columns, fields, self.next.scalars) else blk: {
                    for (fields) |input| if (std.mem.eql(u8, input.name, query.statement.columns[ordinal].field)) break :blk input.numeric;
                    break :blk null;
                };
                if (program) |p| if (p.numericLinearCandidate() and numeric == null and (interval == null or self.next.total_only)) return false;
                out.* = .{ .name = field, .bounds = interval, .numeric = numeric };
            }
            return self.next.accepts(a, projected);
        }
        fn statistic(ptr: *const anyopaque, a: A, fields: []const FieldBounds) !?@import("cypher").statistic.Form {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const query = self.node.operation.query;
            const bound = query.binding.scalars;
            const projected = try a.alloc(FieldBounds, self.fields.len);
            for (self.fields, projected) |field, *out| {
                const ordinal = columnIndex(self.node, field) orelse return null;
                const program = if (ordinal < bound.projections.len) bound.projections[ordinal] else null;
                const polynomial = if (program) |p| try p.statisticProof(a, bound.columns, fields, self.next.integers) else blk: {
                    for (fields) |input| if (std.mem.eql(u8, input.name, query.statement.columns[ordinal].field)) break :blk input.polynomial;
                    break :blk null;
                };
                // The source has marked every observed varying input. A failed
                // projection proof cannot disappear behind an aggregate override.
                if (program) |p| if (polynomial == null) for (p.required_columns) |input| {
                    for (fields) |source| if (std.mem.eql(u8, source.name, bound.columns[input].name) and source.polynomial != null) return null;
                };
                out.* = .{ .name = field, .bounds = if (polynomial) |f| f.range else null, .polynomial = polynomial };
            }
            return self.next.statisticProof(a, projected);
        }
    };
    fn projectCheck(a: A, check: ?LinearCheck, node: *const binding.Node, wanted: Names) !?LinearCheck {
        const original = check orelse return null;
        const owned = try a.create(ProjectedCheck);
        owned.* = .{ .next = original, .node = node, .fields = try names(a, wanted) };
        return .{ .ptr = owned, .validate = ProjectedCheck.validate, .statistic = if (original.statistic != null) ProjectedCheck.statistic else null, .statistic_kind = original.statistic_kind, .statistic_global = original.statistic_global, .integers = original.integers, .scalars = original.scalars, .total_only = original.total_only, .numeric_required = original.numeric_required };
    }
    fn projectCertificates(a: A, node: *const binding.Node, request: catalog.AggregateWeight, wanted: Names) ![]const catalog.SummaryCertificate {
        const query = node.operation.query;
        const bound = query.binding.scalars;
        var result: std.ArrayList(catalog.SummaryCertificate) = .empty;
        for (try certificates(a, request)) |certificate| {
            if (certificate.joint) {
                var stable: Names = .empty;
                if (bound.predicate) |predicate| try programFields(a, &stable, predicate, bound.columns);
                for (certificate.invariants) |field| try projectionInputs(a, &stable, node, columnIndex(node, field) orelse return error.InvalidSqlBackendResponse);
                var inputs: Names = .empty;
                for (node.columns, 0..) |column, ordinal| {
                    if (!wanted.contains(column.internal)) continue;
                    try projectionInputs(a, &inputs, node, ordinal);
                    if (ordinal < bound.projections.len) if (bound.projections[ordinal]) |program| {
                        if (!program.affineTogether() and !program.numericLinearCandidate() and !(certificate.linear != null and certificate.linear.?.statistic != null and program.statisticCandidate())) try programFields(a, &stable, program, bound.columns);
                    };
                }
                var input_keys = inputs.keyIterator();
                if (inputs.count() != 0 and result.items.len < 256) try result.append(a, .{ .field = input_keys.next().?.*, .invariants = try names(a, stable), .joint = true, .linear = try projectCheck(a, certificate.linear, node, wanted) });
                continue;
            }
            const target = columnIndex(node, certificate.field) orelse continue;
            const program = if (target < bound.projections.len) bound.projections[target] else null;
            if (query.binding.columns[target].type != .integer) continue;
            var common: Names = .empty;
            if (bound.predicate) |predicate| try programFields(a, &common, predicate, bound.columns);
            for (certificate.invariants) |field| try projectionInputs(a, &common, node, columnIndex(node, field) orelse return error.InvalidSqlBackendResponse);
            // Unobserved computed outputs still run for their errors. Other
            // demanded computations cannot observe this marginal either.
            for (node.columns, 0..) |column, ordinal| {
                if (ordinal == target or !wanted.contains(column.internal)) continue;
                if (ordinal < bound.projections.len) if (bound.projections[ordinal]) |other| {
                    const operation = other.instructions[other.root].operation;
                    if (operation != .column and operation != .literal) try programFields(a, &common, other, bound.columns);
                };
            }
            const direct = if (program == null) columnIndex(query.source, query.statement.columns[target].field) else null;
            if (program == null and direct == null) continue;
            const candidates: []const u32 = if (program) |p| p.required_columns else &.{@intCast(direct.?)};
            if (candidates.len > 256) continue;
            for (candidates) |candidate| {
                const field = if (program) |p| blk: {
                    if (!p.affineRelativeTo(candidate)) continue;
                    break :blk bound.columns[candidate].name;
                } else query.source.columns[candidate].internal;
                var stable = try common.clone(a);
                if (program) |p| for (p.required_columns) |ordinal| if (ordinal != candidate) try stable.put(a, bound.columns[ordinal].name, {});
                if (stable.contains(field)) continue;
                // Bound proof growth rather than building an exponential
                // disjunction through nested projections. Declining is exact.
                if (result.items.len >= 256) return &.{};
                try result.append(a, .{ .field = field, .invariants = try names(a, stable) });
            }
        }
        return result.items;
    }
    fn initDepth(a: A, root: *const binding.Node, requests: []const catalog.AggregateWeight, cursors: []const catalog.Cursor, depth: usize) anyerror!?Plan {
        if (depth >= 64 or requests.len == 0) return null;
        var node = root;
        var mapped = requests;
        var stages: std.ArrayList(Stage) = .empty;
        var width: usize = 0;
        while (node.operation == .query) {
            if (depth + stages.items.len >= 64) return null;
            const query = node.operation.query;
            if (query.statement.limit != null and query.binding.aggregate == null and query.binding.window == null and
                !query.statement.count_all and !query.statement.scalar_cardinality_limit and !query.statement.selection_prefix and query.constant_refs.len == 0 and
                query.statement.columns.len == node.columns.len and query.binding.columns.len == node.columns.len)
            window: {
                const bound = query.binding.scalars;
                if (bound.predicate) |p| if (!p.repeatable()) break :window;
                for (bound.projections) |p| if (p) |program| if (!program.repeatable()) break :window;
                for (bound.orders) |p| if (p) |program| if (!program.repeatable()) break :window;
                var needed: Names = .empty;
                if (bound.predicate) |p| try programFields(a, &needed, p, bound.columns);
                for (query.statement.columns, 0..) |column, i| {
                    if (i < bound.projections.len and bound.projections[i] != null) {
                        try programFields(a, &needed, bound.projections[i].?, bound.columns);
                    } else try needed.put(a, column.field, {});
                }
                for (bound.orders) |p| if (p) |program| try programFields(a, &needed, program, bound.columns);
                for (query.binding.order_keys) |key| if (key.source == .column) try needed.put(a, key.source.column.path, {});
                const input = try a.alloc(catalog.AggregateWeight, 1);
                input[0] = .{ .fields = try names(a, needed), .multiplicity = .capped };
                const nested = try child(a, query.source, input, cursors, depth + stages.items.len + 1);
                return .{ .source = node, .terminal = .{ .window = .{ .child = nested } }, .requests = requests, .stages = stages.items, .physical = &.{}, .width = width + node.columns.len + nested.width };
            }
            if (query.binding.aggregate != null or query.binding.window != null or query.statement.count_all or
                !unobservedOrder(query) or query.statement.limit != null or query.statement.offset != null or
                query.statement.scalar_cardinality_limit or query.constant_refs.len != 0 or
                query.statement.columns.len != node.columns.len or query.binding.columns.len != node.columns.len) return null;
            const bound = query.binding.scalars;
            if (bound.predicate) |p| if (!p.repeatable()) return null;
            for (bound.projections) |optional| if (optional) |p| if (!p.repeatable()) return null;
            var covered: Names = .empty;
            for (mapped) |request| for (request.fields) |field| try covered.put(a, field, {});
            const inputs = try a.alloc(catalog.AggregateWeight, mapped.len);
            const outputs = try a.alloc([]const []const u8, mapped.len);
            for (mapped, inputs, outputs) |request, *input, *output| {
                var wanted: Names = .empty;
                for (request.fields) |field| {
                    if (columnIndex(node, field) == null) return null;
                    try wanted.put(a, field, {});
                }
                var dependencies: Names = .empty;
                if (bound.predicate) |p| {
                    try programFields(a, &dependencies, p, bound.columns);
                }
                for (node.columns, query.statement.columns, query.binding.columns, 0..) |column, projection, definition, index| {
                    const program = if (index < bound.projections.len) bound.projections[index] else null;
                    const direct = if (program) |p| blk: {
                        const instruction = p.instructions[p.root];
                        break :blk !instruction.input_function and (instruction.operation == .column or instruction.operation == .literal) and
                            p.output_type.kind == definition.type and p.output_type.element_type == definition.element_type and
                            scalar.NumericModifier.eql(p.output_type.numeric_modifier, definition.numeric_modifier);
                    } else blk: {
                        const source_index = columnIndex(query.source, projection.field) orelse return null;
                        const source = query.source.columns[source_index];
                        break :blk source.type == definition.type and source.element_type == definition.element_type and
                            scalar.NumericModifier.eql(source.numeric_modifier, definition.numeric_modifier);
                    };
                    // A demanded computed output is checked by its assigned
                    // reducer pass. Truly unused computations must still run;
                    // keep their error dependencies in every candidate pass.
                    if (!direct and !covered.contains(column.internal)) try wanted.put(a, column.internal, {});
                    if (!wanted.contains(column.internal)) continue;
                    if (program) |p| {
                        try programFields(a, &dependencies, p, bound.columns);
                    } else {
                        if (columnIndex(query.source, projection.field) == null) return null;
                        try dependencies.put(a, projection.field, {});
                    }
                }
                output.* = try names(a, wanted);
                input.* = .{ .fields = try names(a, dependencies), .multiplicity = request.multiplicity, .summaries = try projectCertificates(a, node, request, wanted) };
            }
            try stages.append(a, .{ .node = node, .fields = outputs });
            width += node.columns.len;
            mapped = inputs;
            node = query.source;
        }
        const terminal: Terminal = switch (node.operation) {
            .scan => .scan,
            .materialized_ref => |source| blk: {
                // Capture one complete exact tuple for every reader, never a
                // consumer-specific dependency partition or presence count.
                const fields = try a.alloc([]const u8, source.columns.len);
                for (source.columns, fields) |column, *field| field.* = column.internal;
                const input = try a.alloc(catalog.AggregateWeight, 1);
                input[0] = .{ .fields = fields, .multiplicity = .exact };
                const nested = try child(a, source, input, cursors, depth + 1);
                width += nested.width;
                break :blk .{ .replay = .{ .child = nested } };
            },
            .set => |set| blk: {
                if (set.kind != .@"union" or !set.all) return null;
                const left = try child(a, set.left, try positional(a, node, set.left, mapped), cursors, depth + stages.items.len + 1);
                const right = try child(a, set.right, try positional(a, node, set.right, mapped), cursors, depth + stages.items.len + 1);
                width += @max(left.width, right.width);
                break :blk .{ .union_all = .{ .left = left, .right = right } };
            },
            .join => |join| blk: {
                if (join.correlation) return null;
                if (join.condition) |program| if (!program.repeatable()) return null;
                const graph_left = captureSafe(join.right, cursors, 0, false) or
                    (!captureSafe(join.left, cursors, 0, false) and captureSafe(join.right, cursors, 0, true));
                const graph = if (graph_left) join.left else if (captureSafe(join.left, cursors, 0, true)) join.right else return null;
                const single = if (graph_left) join.right else join.left;
                if (join.membership != null and !graph_left) return null;
                var factor_only = join.condition == null and join.membership == null and (join.kind == .inner or join.kind == .cross);
                for (mapped) |request| for (request.fields) |field| if (columnIndex(single, field) != null) {
                    factor_only = false;
                };
                const input = try a.alloc(catalog.AggregateWeight, mapped.len);
                for (mapped, input) |request, *out| {
                    var fields: Names = .empty;
                    for (request.fields) |field| if (columnIndex(graph, field) != null) try fields.put(a, field, {});
                    if (join.condition) |program| for (program.required_columns) |ordinal| {
                        if (ordinal >= node.columns.len) return error.InvalidSqlBackendResponse;
                        const name = node.columns[ordinal].internal;
                        if (columnIndex(graph, name) != null) try fields.put(a, name, {});
                    };
                    if (join.membership != null) for (join.left_keys) |program| for (program.required_columns) |ordinal| {
                        if (ordinal >= graph.columns.len) return error.InvalidSqlBackendResponse;
                        try fields.put(a, graph.columns[ordinal].internal, {});
                    };
                    out.* = .{ .fields = try names(a, fields), .multiplicity = request.multiplicity };
                }
                const nested = try child(a, graph, input, cursors, depth + stages.items.len + 1);
                // Build one dependency partition shared by every probe slot. A
                // build tuple carries its own bag weight through memory/spill.
                var build_fields: Names = .empty;
                var multiplicity: catalog.Multiplicity = .presence;
                for (mapped) |request| {
                    for (request.fields) |field| if (columnIndex(single, field) != null) try build_fields.put(a, field, {});
                    if (request.multiplicity == .exact or (request.multiplicity == .capped and multiplicity == .presence)) multiplicity = request.multiplicity;
                }
                if (join.condition) |program| for (program.required_columns) |ordinal| {
                    const name = node.columns[ordinal].internal;
                    if (columnIndex(single, name) != null) try build_fields.put(a, name, {});
                };
                const keys = if (graph_left) join.right_keys else join.left_keys;
                for (keys) |program| for (program.required_columns) |ordinal| try build_fields.put(a, single.columns[ordinal].internal, {});
                const build_requests = try a.alloc(catalog.AggregateWeight, 1);
                build_requests[0] = .{ .fields = try names(a, build_fields), .multiplicity = multiplicity };
                const build = try child(a, single, build_requests, cursors, depth + stages.items.len + 1);
                width += nested.width + build.width;
                break :blk .{ .cross = .{ .child = nested, .build = build, .singleton = single, .graph_left = graph_left, .factor_only = factor_only } };
            },
            else => return null,
        };
        if (terminal != .scan) return .{ .source = node, .terminal = terminal, .requests = requests, .stages = stages.items, .physical = &.{}, .width = width + node.columns.len };
        const physical = try a.alloc(catalog.AggregateWeight, mapped.len);
        for (mapped, physical) |request, *out| {
            const fields = try a.alloc([]const u8, request.fields.len);
            for (request.fields, fields) |field, *name| {
                const index = columnIndex(node, field) orelse return null;
                name.* = node.operation.scan.source_columns[index];
            }
            const invariants = try a.alloc([]const u8, request.invariants.len);
            for (request.invariants, invariants) |field, *name| name.* = node.operation.scan.source_columns[columnIndex(node, field) orelse return null];
            const summaries = try a.alloc(catalog.SummaryCertificate, request.summaries.len);
            for (request.summaries, summaries) |certificate, *mapped_certificate| {
                const stable = try a.alloc([]const u8, certificate.invariants.len);
                for (certificate.invariants, stable) |field, *name| name.* = node.operation.scan.source_columns[columnIndex(node, field) orelse return null];
                mapped_certificate.* = .{ .field = node.operation.scan.source_columns[columnIndex(node, certificate.field) orelse return null], .invariants = stable, .joint = certificate.joint, .linear = try renameCheck(a, certificate.linear, request.fields, fields) };
            }
            out.* = .{ .fields = fields, .multiplicity = request.multiplicity, .summary = if (request.summary) |name| node.operation.scan.source_columns[columnIndex(node, name) orelse return null] else null, .invariants = invariants, .summaries = summaries };
        }
        return .{ .source = node, .requests = requests, .stages = stages.items, .physical = physical, .width = width + node.columns.len };
    }

    fn child(a: A, node: *const binding.Node, requests: []const catalog.AggregateWeight, cursors: []const catalog.Cursor, depth: usize) !*Plan {
        const result = try a.create(Plan);
        result.* = (try initDepth(a, node, requests, cursors, depth)) orelse .{ .source = node, .terminal = .ordinary, .requests = requests, .stages = &.{}, .physical = &.{}, .width = node.columns.len };
        return result;
    }
    fn positional(a: A, parent: *const binding.Node, nested: *const binding.Node, requests: []const catalog.AggregateWeight) ![]const catalog.AggregateWeight {
        const result = try a.alloc(catalog.AggregateWeight, requests.len);
        for (requests, result) |request, *out| {
            const fields = try a.alloc([]const u8, request.fields.len);
            for (request.fields, fields) |field, *name| name.* = nested.columns[columnIndex(parent, field) orelse return error.InvalidSqlBackendResponse].internal;
            const invariants = try a.alloc([]const u8, request.invariants.len);
            for (request.invariants, invariants) |field, *name| name.* = nested.columns[columnIndex(parent, field) orelse return error.InvalidSqlBackendResponse].internal;
            const summaries = try a.alloc(catalog.SummaryCertificate, request.summaries.len);
            for (request.summaries, summaries) |certificate, *mapped_certificate| {
                const stable = try a.alloc([]const u8, certificate.invariants.len);
                for (certificate.invariants, stable) |field, *name| name.* = nested.columns[columnIndex(parent, field) orelse return error.InvalidSqlBackendResponse].internal;
                mapped_certificate.* = .{ .field = nested.columns[columnIndex(parent, certificate.field) orelse return error.InvalidSqlBackendResponse].internal, .invariants = stable, .joint = certificate.joint, .linear = try renameCheck(a, certificate.linear, request.fields, fields) };
            }
            out.* = .{ .fields = fields, .multiplicity = request.multiplicity, .summary = if (request.summary) |name| nested.columns[columnIndex(parent, name) orelse return error.InvalidSqlBackendResponse].internal else null, .invariants = invariants, .summaries = summaries };
        }
        return result;
    }
    /// Ordinary pinned scans may be captured into a bounded, spillable build.
    /// Capability inspection never advances a graph producer. Repeated or
    /// correlated evaluation and volatile expressions remain barriers.
    fn captureSafe(node: *const binding.Node, cursors: []const catalog.Cursor, depth: usize, weighted: bool) bool {
        if (depth >= 64) return false;
        return switch (node.operation) {
            .singleton, .literal_rows => true,
            .scan => |scan| weighted or (cursors[scan.index].enable_weights == null and cursors[scan.index].enable_aggregate_weights == null),
            .materialized_ref => |source| captureSafe(source, cursors, depth + 1, weighted),
            .query => |q| blk: {
                if (q.constant_refs.len != 0) break :blk false;
                if (q.binding.scalars.predicate) |p| if (!p.repeatable()) break :blk false;
                for (q.binding.scalars.projections) |p| if (p) |program| if (!program.repeatable()) break :blk false;
                for (q.binding.scalars.orders) |p| if (p) |program| if (!program.repeatable()) break :blk false;
                if (q.binding.window != null) break :blk false;
                if (q.binding.aggregate) |aggregate| {
                    if (aggregate.constant_count != 0 or !repeatableScalars(aggregate.input)) break :blk false;
                    for (aggregate.specs) |spec| if (spec.kind == .pattern_set) break :blk false;
                    for (aggregate.outputs) |program| if (!program.repeatable()) break :blk false;
                    for (aggregate.orders) |program| if (!program.repeatable()) break :blk false;
                    if (aggregate.having) |program| if (!program.repeatable()) break :blk false;
                    for (aggregate.ordered) |ordered| if (ordered.direct) |program| if (!program.repeatable()) break :blk false;
                }
                break :blk captureSafe(q.source, cursors, depth + 1, weighted);
            },
            .join => |join| !join.correlation and
                (if (join.condition) |program| program.repeatable() else true) and
                captureSafe(join.left, cursors, depth + 1, weighted) and captureSafe(join.right, cursors, depth + 1, weighted),
            .set => |set| captureSafe(set.left, cursors, depth + 1, weighted) and captureSafe(set.right, cursors, depth + 1, weighted),
            .values => |arms| for (arms) |arm| {
                if (!captureSafe(arm, cursors, depth + 1, weighted)) break false;
            } else true,
            else => false,
        };
    }
    fn repeatableScalars(bound: @import("bound_scalars.zig").Bound) bool {
        if (bound.predicate) |program| if (!program.repeatable()) return false;
        for (bound.projections) |optional| if (optional) |program| if (!program.repeatable()) return false;
        for (bound.orders) |optional| if (optional) |program| if (!program.repeatable()) return false;
        return true;
    }

    /// Captures can feed ordinary row consumers, which may observe a source's
    /// ordering. Aggregate-only projection proofs may discard that ordering;
    /// shared replay must use ordinary capture when that proof would do so.
    pub fn replayOrderSafe(node: *const binding.Node, depth: usize) bool {
        if (depth >= 64) return false;
        return switch (node.operation) {
            .query => |q| (q.binding.order_keys.len == 0 or q.statement.limit != null) and replayOrderSafe(q.source, depth + 1),
            .join => |j| replayOrderSafe(j.left, depth + 1) and replayOrderSafe(j.right, depth + 1),
            .set => |set| replayOrderSafe(set.left, depth + 1) and replayOrderSafe(set.right, depth + 1),
            .materialized_ref => true, // Its own shared capture enforces this boundary.
            .scan, .singleton, .literal_rows => true,
            else => false,
        };
    }
    fn totalOrderProgram(program: scalar.Program) bool {
        const root = program.instructions[program.root];
        return !root.input_function and (root.operation == .column or root.operation == .literal) and
            orderedScalar(program.output_type.kind);
    }
    fn orderedScalar(kind: ?@import("ast.zig").ColumnType) bool {
        return if (kind) |k| k == .integer or k == .number or k == .boolean or k == .string else true;
    }
    fn unobservedOrder(query: anytype) bool {
        for (query.binding.order_keys) |key| switch (key.source) {
            .column => |column| if (!orderedScalar(column.type)) return false,
            .output => |index| {
                if (index >= query.binding.columns.len or !orderedScalar(query.binding.columns[index].type)) return false;
                if (index < query.binding.scalars.projections.len) if (query.binding.scalars.projections[index]) |program| {
                    if (!totalOrderProgram(program)) return false;
                };
            },
            .expression => return false,
        };
        return true;
    }

    pub fn project(self: Plan, a: A, original_context: anytype, initial: catalog.Row, slots: ?[]const usize) !?catalog.Row {
        var row = initial;
        var remaining = self.stages.len;
        while (remaining != 0) {
            remaining -= 1;
            const stage = self.stages[remaining];
            const query = stage.node.operation.query;
            var context = original_context;
            context.binding = query.binding;
            context.binding.relation = null;
            const bound = query.binding.scalars;
            const cells = try bound.cells(a, row);
            if (!try bound.matchesWithLimits(a, cells, context.parameters, context.backend.decision_provider, @import("decision_eval.zig").limitsFor(context.backend))) return null;
            const values = try a.alloc(scalar.Datum, stage.node.columns.len);
            @memset(values, .{});
            const fields = try a.alloc([]const u8, values.len);
            for (stage.node.columns, query.statement.columns, query.binding.columns, values, fields, 0..) |column, projection, definition, *value, *field, index| {
                field.* = column.internal;
                const active = if (slots) |active_slots| blk: {
                    for (active_slots) |slot| {
                        if (slot >= stage.fields.len) return error.InvalidSqlBackendResponse;
                        if (contains(stage.fields[slot], column.internal)) break :blk true;
                    }
                    break :blk false;
                } else contains(stage.fields[0], column.internal);
                if (!active) continue;
                const program = if (index < bound.projections.len) bound.projections[index] else null;
                const input = if (program) |p| try context.evaluate(a, p, cells) else try row.cell(projection.field);
                value.* = try describe.coerceDatum(a, input, definition.type, definition.element_type);
            }
            row = try catalog.Row.fromDatums(a, row.id, try catalog.Row.TypedLayout.init(a, fields), values);
        }
        return row;
    }
};

fn columnIndex(node: *const binding.Node, name: []const u8) ?usize {
    for (node.columns, 0..) |column, index| if (std.mem.eql(u8, column.internal, name)) return index;
    return null;
}
fn contains(fields: []const []const u8, name: []const u8) bool {
    for (fields) |field| if (std.mem.eql(u8, field, name)) return true;
    return false;
}
fn programFields(a: A, fields: *Names, program: scalar.Program, columns: []const scalar.Column) !void {
    for (program.required_columns) |index| {
        if (index >= columns.len) return error.InvalidSqlBackendResponse;
        try fields.put(a, columns[index].name, {});
    }
}
fn names(a: A, fields: Names) ![]const []const u8 {
    const result = try a.alloc([]const u8, fields.count());
    var keys = fields.keyIterator();
    for (result) |*name| name.* = keys.next().?.*;
    return result;
}
