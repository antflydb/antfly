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

//! Final, cardinality-preserving projection. JSON expressions lower to the
//! existing SQL scalar IR/VM; CASE and NULL semantics have one implementation.
//! Binding discovers all dependencies for authorization without loading them.
const std = @import("std");
const sql = @import("../sql/scalar.zig");
const ast = @import("../sql/ast.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
/// Normalize the stable string field mask and named scalar expressions once.
/// The encoded plan is internal request ownership, not a new public protocol.
pub fn encode(a: A, fields: []const []const u8, selected: V, params: V) ![]u8 {
    if (selected != .object or selected.object.count() == 0 or selected.object.count() > 64 or params != .object or params.object.count() > 1024) return error.InvalidQueryRequest;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    var counter: std.Io.Writer.Discarding = .init(&.{});
    try std.json.Stringify.value(params, .{}, &counter.writer);
    if (counter.fullCount() > 1024 * 1024) return error.DecisionLimitExceeded;
    var values: std.ArrayList(V) = .empty;
    for (fields) |field| try values.append(temp, .{ .string = field });
    var entries = selected.object.iterator();
    while (entries.next()) |entry| {
        var object: std.json.ObjectMap = .empty;
        try object.put(temp, "name", .{ .string = entry.key_ptr.* });
        try object.put(temp, "expression", entry.value_ptr.*);
        try values.append(temp, .{ .object = object });
    }
    const request: Request = .{ .fields = values.items, .params = params };
    var checked = try Plan.init(temp, request);
    defer checked.deinit();
    return std.json.Stringify.valueAlloc(a, request, .{});
}
pub fn decode(a: A, encoded: []const u8) !Plan {
    var request = try std.json.parseFromSlice(Request, a, encoded, .{});
    defer request.deinit();
    return Plan.init(a, request.value);
}
pub fn selection(a: A, request: Request) !V {
    var output: std.json.ObjectMap = .empty;
    for (request.fields) |field| if (field == .object) {
        try output.put(a, field.object.get("name").?.string, field.object.get("expression").?);
    };
    return .{ .object = output };
}
/// Borrowed JSON fallback for complete provider/older-worker afterimages.
/// Native storage supplies a lazy source instead; identity caching is never a
/// substitute for selecting and authorizing this exact current source.
pub const JsonSource = struct {
    id: []const u8,
    root: V,
    pub fn source(self: *@This()) Source {
        return .{ .context = self, .read = read };
    }
    pub fn at(root: V, path: []const u8) ?V {
        var current = root;
        var parts = std.mem.splitScalar(u8, path, '.');
        while (parts.next()) |part| {
            current = switch (current) {
                .object => |object| object.get(part) orelse return null,
                .array => |items| blk: {
                    const index = std.fmt.parseInt(usize, part, 10) catch return null;
                    if (index >= items.items.len) return null;
                    break :blk items.items[index];
                },
                else => return null,
            };
        }
        return current;
    }
    fn resolve(a: A, root: V, path: []const u8, depth: usize) anyerror!?V {
        if (depth >= 256) return error.DecisionLimitExceeded;
        if (path.len == 0) return root;
        const dot = std.mem.indexOfScalar(u8, path, '.') orelse path.len;
        const head = path[0..dot];
        const tail = if (dot < path.len) path[dot + 1 ..] else "";
        return switch (root) {
            .object => |object| if (object.get(head)) |value| try resolve(a, value, tail, depth + 1) else null,
            .array => |array| blk: {
                if (std.fmt.parseInt(usize, head, 10)) |index| {
                    break :blk if (index < array.items.len) try resolve(a, array.items[index], tail, depth + 1) else null;
                } else |_| {}
                var values: std.ArrayList(V) = .empty;
                for (array.items) |item| if (try resolve(a, item, path, depth + 1)) |value| try values.append(a, value);
                break :blk if (values.items.len == 0) null else V{ .array = .fromOwnedSlice(a, try values.toOwnedSlice(a)) };
            },
            else => null,
        };
    }
    fn read(raw: *anyopaque, a: A, path: []const u8, kind: sql.ColumnRead) !sql.Datum {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const value = if (std.mem.eql(u8, path, "_id")) V{ .string = self.id } else try resolve(a, self.root, path, 0);
        return switch (kind) {
            .missing => sql.Datum.json(.{ .bool = value == null }),
            .identity => if (value) |present| sql.Datum.json(.{ .string = try a.dupe(u8, &(try @import("../common/value_identity.zig").identity(present))) }) else .{},
            .value => if (value) |present| sql.Datum.json(present) else .{ .missing = true },
        };
    }
};
/// Trusted hydration already selected the global page and retained its read cut.
pub fn hydrationEligible(req: anytype) bool {
    var delivery = req;
    if (req.document_authority_mode != .hydrate) return false;
    delivery.document_authority_mode = .none;
    delivery.include_stored = true;
    return storageEligible(delivery);
}
/// Poll the same deadline/cancellation state used by native retrieval.
pub fn requestLimits(request: *const @import("../storage/db/types.zig").SearchRequest) sql.EvalLimits {
    const Guard = struct {
        fn poll(raw: ?*anyopaque) !void {
            const req: *const @import("../storage/db/types.zig").SearchRequest = @ptrCast(@alignCast(raw.?));
            try @import("../storage/db/query/search_exec.zig").checkSearchRequestDeadline(req.*);
        }
    };
    return .{ .checkpoint = Guard.poll, .checkpoint_context = @constCast(request) };
}
/// Output-only plans can run after local retrieval without changing membership.
/// Source-dependent operators retain complete afterimages for final evaluation.
pub fn storageEligible(req: anytype) bool {
    return req.field_projection_json.len != 0 and req.include_stored and
        req.document_authority_mode == .none and req.return_mode == .parent and
        !req.hasHitEvaluation() and req.evaluation_json.len == 0 and
        req.reranker == null and req.pruner == null and req.highlight == null and
        req.graph_queries.len == 0 and req.graph_metric_queries.len == 0 and req.graph_metric_rerank == null and
        req.hierarchy_children == null and !req.hierarchy_grouped_matches and
        req.hierarchy_group_level == .source and !req.hierarchy_include_source and !req.hierarchy_include_unit;
}
/// Keep scalar input/type failures in the existing query dependency contract;
/// source corruption, cancellation and allocation failures retain their identity.
fn scalarFailure(err: anyerror) anyerror {
    const name = @errorName(err);
    if (std.mem.startsWith(u8, name, "Sql")) {
        if (std.mem.indexOf(u8, name, "Limit") != null or std.mem.indexOf(u8, name, "TooLarge") != null) return error.DecisionLimitExceeded;
        return error.FunctionTypeMismatch;
    }
    return err;
}
pub const marker = "__antfly_projection";
pub const Entry = struct { name: []const u8, expression: V };
pub const Request = struct { fields: []const V, params: V = .{ .object = .empty } };
pub const Source = struct {
    context: *anyopaque,
    read: *const fn (*anyopaque, A, []const u8, sql.ColumnRead) anyerror!sql.Datum,
};
const Dependency = union(enum) { field: []const u8, binding: usize };
const Binding = struct { name: []const u8, expression: V, state: enum { unseen, visiting, ready } = .unseen, program: ?sql.Program = null, dependencies: []const Dependency = &.{}, height: usize = 0 };
pub const Plan = struct {
    arena: *std.heap.ArenaAllocator,
    source_fields: []const []const u8 = &.{},
    required_fields: std.StringHashMapUnmanaged(void) = .empty,
    bindings: []Binding = &.{},
    evaluation_order: []const usize = &.{},
    parameters: std.ArrayList(V) = .empty,
    hints: std.ArrayList(?ast.ColumnType) = .empty,
    params: V = .null,
    nodes: usize = 0,
    fingerprint: [64]u8 = undefined,

    pub fn init(backing: A, input: Request) !Plan {
        const arena = try backing.create(std.heap.ArenaAllocator);
        arena.* = .init(backing);
        var self: Plan = .{ .arena = arena };
        errdefer self.deinit();
        const a = self.arena.allocator();
        const owned = try std.json.Stringify.valueAlloc(a, input, .{});
        const request = try std.json.parseFromSliceLeaky(Request, a, owned, .{});
        if (request.fields.len > 4160 or request.params != .object) return error.InvalidQueryRequest;
        self.params = request.params;
        var hasher = std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256).init(&.{});
        try std.json.Stringify.value(request, .{}, &hasher.writer);
        try hasher.writer.flush();
        self.fingerprint = std.fmt.bytesToHex(hasher.hasher.finalResult(), .lower);
        var fields: std.ArrayList([]const u8) = .empty;
        var bindings: std.ArrayList(Binding) = .empty;
        for (request.fields) |field| switch (field) {
            .string => |name| try fields.append(a, name),
            .object => |object| {
                if (object.count() != 2 or !object.contains("name") or !object.contains("expression") or bindings.items.len == 64) return error.InvalidQueryRequest;
                const name = object.get("name").?;
                if (name != .string or name.string.len == 0 or name.string.len > 1024 or std.mem.startsWith(u8, name.string, "__antfly_") or std.mem.indexOfAny(u8, name.string, ".\x00") != null) return error.InvalidQueryRequest;
                for (bindings.items) |binding| if (std.mem.eql(u8, name.string, binding.name)) return error.InvalidQueryRequest;
                try bindings.append(a, .{ .name = name.string, .expression = object.get("expression").? });
            },
            else => return error.InvalidQueryRequest,
        };
        self.source_fields = fields.items;
        self.bindings = bindings.items;
        // Resolve aliases before compiling expressions. This worklist never
        // nests compiler frames through references, even for adversarial DAGs.
        var edges: [64]u64 = @splat(0);
        var visited: usize = 0;
        for (self.bindings, 0..) |binding, i| {
            var pending: std.ArrayList(V) = .empty;
            try pending.append(a, binding.expression);
            while (pending.pop()) |value| {
                visited += 1;
                if (visited > 65536) return error.DecisionLimitExceeded;
                switch (value) {
                    .array => |array| try pending.appendSlice(a, array.items),
                    .object => |object| {
                        // Literal JSON is data, including any nested ref keys.
                        if (object.contains("literal")) continue;
                        if (object.get("ref")) |name| {
                            if (name != .string) return error.InvalidFunctionExpression;
                            const target = for (self.bindings, 0..) |other, j| {
                                if (std.mem.eql(u8, other.name, name.string)) break j;
                            } else return error.UnknownFunctionBinding;
                            edges[i] |= @as(u64, 1) << @intCast(target);
                        }
                        var members = object.iterator();
                        while (members.next()) |member| try pending.append(a, member.value_ptr.*);
                    },
                    else => {},
                }
            }
        }
        var order: std.ArrayList(usize) = .empty;
        var ready: u64 = 0;
        var remaining = self.bindings.len;
        while (remaining != 0) {
            const before = remaining;
            for (0..self.bindings.len) |i| {
                const bit = @as(u64, 1) << @intCast(i);
                if (ready & bit != 0 or edges[i] & ~ready != 0) continue;
                try self.compileBinding(i);
                try order.append(a, i);
                ready |= bit;
                remaining -= 1;
            }
            if (before == remaining) return error.CyclicFunctionBinding;
        }
        self.evaluation_order = order.items;
        return self;
    }
    pub fn deinit(self: *Plan) void {
        for (self.bindings) |*binding| if (binding.program) |*program| program.deinit();
        const backing = self.arena.child_allocator;
        self.arena.deinit();
        backing.destroy(self.arena);
        self.* = undefined;
    }
    fn compileBinding(self: *Plan, index: usize) anyerror!void {
        switch (self.bindings[index].state) {
            .ready => return,
            .visiting => return error.CyclicFunctionBinding,
            .unseen => {},
        }
        self.bindings[index].state = .visiting;
        var compiler: Compiler = .{ .plan = self };
        const node = try compiler.expression(self.bindings[index].expression, 0);
        self.bindings[index].program = sql.bind(self.arena.allocator(), node, compiler.columns.items, self.hints.items, .{ .constant_bytes = 0, .constant_steps = 0 }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidFunctionExpression,
        };
        self.bindings[index].height = compiler.height;
        self.bindings[index].dependencies = compiler.dependencies.items;
        self.bindings[index].state = .ready;
    }
    pub fn hasOutputs(self: *const Plan, previous: ?V) bool {
        const value = previous orelse return false;
        if (value != .object) return false;
        const token = value.object.get(marker) orelse return false;
        if (token != .string or !std.mem.eql(u8, token.string, &self.fingerprint)) return false;
        for (self.bindings) |binding| if (!value.object.contains(binding.name)) return false;
        return true;
    }
    /// Validate worker output and keep selected values as lexical spans. No
    /// payload DOM, string unescaping/re-escaping, or numeric coercion is needed.
    /// Receipts attest execution only, never source access or authorization.
    pub fn wireOutput(self: *const Plan, a: A, bytes: []const u8, transport: bool) !?@import("antfly-json").RawObject {
        const json = @import("antfly-json");
        const Observer = struct {
            plan: *const Plan,
            writer: std.Io.Writer.Allocating,
            transport: bool,
            seen: u64 = 0,
            matched: bool = false,
            has_marker: bool = false,
            count: usize = 0,
            fn member(raw: *anyopaque, name: []const u8, value: []const u8) !void {
                const ctx: *@This() = @ptrCast(@alignCast(raw));
                if (std.mem.eql(u8, name, marker)) {
                    ctx.has_marker = true;
                    const fingerprint = try std.json.parseFromSlice(V, ctx.writer.allocator, value, .{});
                    defer fingerprint.deinit();
                    ctx.matched = fingerprint.value == .string and std.mem.eql(u8, fingerprint.value.string, &ctx.plan.fingerprint);
                    if (!ctx.transport) return;
                }
                for (ctx.plan.bindings, 0..) |binding, i| if (std.mem.eql(u8, name, binding.name)) {
                    ctx.seen |= @as(u64, 1) << @intCast(i);
                };
                if (!ctx.transport) {
                    if (ctx.count != 0) try ctx.writer.writer.writeByte(',');
                    try std.json.Stringify.value(name, .{}, &ctx.writer.writer);
                    try ctx.writer.writer.writeByte(':');
                    try ctx.writer.writer.writeAll(value);
                    ctx.count += 1;
                }
            }
        };
        const scratch = @import("antfly_platform").allocator.reclaimingScratch(a);
        var observer: Observer = .{ .plan = self, .writer = .init(scratch), .transport = transport };
        defer observer.writer.deinit();
        if (!transport) try observer.writer.writer.writeByte('{');
        var scanner = json.Scanner.initCompleteInput(scratch, bytes);
        defer scanner.deinit();
        if (try scanner.peekNextTokenType() != .object_begin) return error.InvalidQueryResponse;
        try json.scanValue(scratch, &scanner, bytes.len, .{ .ctx = &observer, .member_fn = Observer.member });
        if (try scanner.next() != .end_of_document) return error.InvalidQueryResponse;
        const expected = if (self.bindings.len == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(self.bindings.len)) - 1;
        if (observer.has_marker and (!observer.matched or observer.seen != expected)) return error.InvalidQueryResponse;
        if (!observer.has_marker) return null;
        if (transport) return .{ .bytes = bytes };
        try observer.writer.writer.writeByte('}');
        return .{ .bytes = try a.dupe(u8, observer.writer.written()) };
    }
    pub fn evaluate(self: *const Plan, a: A, source: Source, previous: ?V, limits: sql.EvalLimits) !V {
        if (limits.checkpoint) |poll| try poll(limits.checkpoint_context);
        if (self.hasOutputs(previous)) return previous.?;
        if (previous) |value| if (value == .object and value.object.contains(marker)) return error.InvalidQueryResponse;
        const values = try a.alloc(?sql.Datum, self.bindings.len);
        @memset(values, null);
        var row: Row = .{ .plan = self, .a = a, .source = source, .values = values, .limits = limits };
        var output: std.json.ObjectMap = if (previous) |value| if (value == .object) value.object else return error.InvalidQueryResponse else .empty;
        for (self.bindings) |binding| if (output.contains(binding.name)) return error.InvalidQueryRequest;
        var output_bytes: usize = 0;
        // Every binding is a requested output. Evaluate in dependency order so
        // references only load memoized values, never recursively enter VMs.
        for (self.evaluation_order) |i| {
            if (limits.checkpoint) |poll| try poll(limits.checkpoint_context);
            const value = try row.binding(i);
            output_bytes +|= try @import("../sql/operators.zig").datumBytes(value);
            if (output_bytes > limits.output_bytes) return error.DecisionLimitExceeded;
        }
        // Preserve the caller's alias order at the public output boundary.
        for (self.bindings, 0..) |binding, i| {
            const value = row.values[i].?;
            try output.put(a, binding.name, binding.program.?.resultJson(a, value, limits) catch |err| return scalarFailure(err));
        }
        try output.put(a, marker, .{ .string = try a.dupe(u8, &self.fingerprint) });
        return .{ .object = output };
    }
};
const Compiler = struct {
    plan: *Plan,
    height: usize = 0,
    columns: std.ArrayList(sql.Column) = .empty,
    dependencies: std.ArrayList(Dependency) = .empty,
    fn node(self: *@This(), value: ast.Scalar) !*const ast.Scalar {
        const out = try self.plan.arena.allocator().create(ast.Scalar);
        out.* = value;
        return out;
    }
    fn column(self: *@This(), dependency: Dependency, kind: sql.Type) !*const ast.Scalar {
        const a = self.plan.arena.allocator();
        const name = try std.fmt.allocPrint(a, "projection${d}", .{self.columns.items.len});
        try self.columns.append(a, .{ .name = name, .type = kind.kind orelse .json, .nullable = kind.nullable, .element_type = kind.element_type, .numeric_modifier = kind.numeric_modifier });
        try self.dependencies.append(a, dependency);
        return self.node(.{ .column = name });
    }
    fn parameter(self: *@This(), value: V) !*const ast.Scalar {
        const a = self.plan.arena.allocator();
        if (self.plan.parameters.items.len >= 1024) return error.DecisionLimitExceeded;
        try self.plan.parameters.append(a, value);
        try self.plan.hints.append(a, switch (value) {
            .null => null,
            .bool => .boolean,
            .integer => .integer,
            .float, .number_string => .number,
            .string => .string,
            .array, .object => .json,
        });
        return self.node(.{ .literal = .{ .parameter = @intCast(self.plan.parameters.items.len) } });
    }
    fn expression(self: *@This(), value: V, depth: usize) anyerror!*const ast.Scalar {
        self.plan.nodes += 1;
        if (depth >= 64 or self.plan.nodes > 8192) return error.DecisionLimitExceeded;
        self.height = @max(self.height, depth + 1);
        if (value != .object) return error.InvalidFunctionExpression;
        const object = value.object;
        if (object.get("literal")) |literal| {
            if (object.count() != 1) return error.InvalidFunctionExpression;
            return switch (literal) {
                .null => self.node(.{ .literal = .null }),
                .bool => |v| self.node(.{ .literal = .{ .boolean = v } }),
                .integer => |v| self.node(.{ .literal = .{ .integer = v } }),
                .float => |v| self.node(.{ .literal = .{ .number = v } }),
                .number_string => |v| self.node(.{ .literal = .{ .numeric = v } }),
                .string => |v| self.node(.{ .literal = .{ .string = v } }),
                .array, .object => self.parameter(literal),
            };
        }
        if (object.get("field")) |field| {
            if (object.count() != 1 or field != .string or !validPath(field.string)) return error.InvalidFunctionExpression;
            try self.plan.required_fields.put(self.plan.arena.allocator(), field.string, {});
            return self.column(.{ .field = field.string }, .{ .kind = if (std.mem.eql(u8, field.string, "_id")) .string else .json });
        }
        if (object.get("ref")) |name| {
            if (object.count() != 1 or name != .string) return error.InvalidFunctionExpression;
            for (self.plan.bindings, 0..) |binding, i| if (std.mem.eql(u8, binding.name, name.string)) {
                std.debug.assert(self.plan.bindings[i].state == .ready);
                const height = depth + 1 + self.plan.bindings[i].height;
                if (height > 64) return error.DecisionLimitExceeded;
                self.height = @max(self.height, height);
                return self.column(.{ .binding = i }, self.plan.bindings[i].program.?.output_type);
            };
            return error.UnknownFunctionBinding;
        }
        if (object.get("param")) |name| {
            if (object.count() != 1 or name != .string) return error.InvalidFunctionExpression;
            return self.parameter(self.plan.params.object.get(name.string) orelse return error.InvalidSqlParameters);
        }
        if (object.get("case")) |cases| {
            if (object.count() > 2 or (object.count() == 2 and !object.contains("else")) or cases != .array or cases.array.items.len == 0 or cases.array.items.len > 64) return error.InvalidFunctionExpression;
            const branches = try self.plan.arena.allocator().alloc(ast.Scalar.Branch, cases.array.items.len);
            for (cases.array.items, branches) |branch, *out| {
                if (branch != .object or branch.object.count() != 2) return error.InvalidFunctionExpression;
                out.* = .{ .condition = try self.expression(branch.object.get("when") orelse return error.InvalidFunctionExpression, depth + 1), .value = try self.expression(branch.object.get("then") orelse return error.InvalidFunctionExpression, depth + 1) };
            }
            return self.node(.{ .case_when = .{ .branches = branches, .otherwise = if (object.get("else")) |other| try self.expression(other, depth + 1) else null } });
        }
        if (object.get("call")) |call| {
            if (call != .string or object.count() != 2) return error.InvalidFunctionExpression;
            const function = std.meta.stringToEnum(sql.Function, call.string) orelse return error.UnknownQueryFunction;
            if (std.mem.startsWith(u8, call.string, "$") or std.mem.startsWith(u8, call.string, "ai_") or function == .current_setting) return error.UnsupportedQueryRequest;
            const args = object.get("args") orelse return error.InvalidFunctionExpression;
            if (args != .array or args.array.items.len > 64) return error.InvalidFunctionExpression;
            const nodes = try self.plan.arena.allocator().alloc(*const ast.Scalar, args.array.items.len);
            for (args.array.items, nodes) |arg, *out| out.* = try self.expression(arg, depth + 1);
            return self.node(.{ .call = .{ .name = call.string, .args = nodes } });
        }
        if (object.get("cast")) |cast| {
            if (object.count() != 1 or cast != .object or cast.object.count() != 2) return error.InvalidFunctionExpression;
            const kind = cast.object.get("type") orelse return error.InvalidFunctionExpression;
            if (kind != .string) return error.InvalidFunctionExpression;
            return self.node(.{ .cast = .{ .operand = try self.expression(cast.object.get("expression") orelse return error.InvalidFunctionExpression, depth + 1), .type = std.meta.stringToEnum(ast.ColumnType, kind.string) orelse return error.InvalidFunctionExpression } });
        }
        if (object.count() != 1) return error.InvalidFunctionExpression;
        const name = object.keys()[0];
        const op: ast.Scalar.Binary = if (std.mem.eql(u8, name, "is_distinct_from")) .is_distinct else if (std.mem.eql(u8, name, "is_not_distinct_from")) .is_not_distinct else std.meta.stringToEnum(ast.Scalar.Binary, name) orelse return error.InvalidFunctionExpression;
        const args = object.values()[0];
        if (args != .array or args.array.items.len != 2) return error.InvalidFunctionExpression;
        return self.node(.{ .binary = .{ .op = op, .left = try self.expression(args.array.items[0], depth + 1), .right = try self.expression(args.array.items[1], depth + 1) } });
    }
};
pub fn validPath(path: []const u8) bool {
    if (std.mem.eql(u8, path, "_id")) return true;
    if (path.len == 0 or path.len > 1024 or std.mem.indexOfAny(u8, path, "*?[]\x00") != null) return false;
    var parts = std.mem.splitScalar(u8, path, '.');
    const root = parts.next().?;
    if (root.len == 0 or root[0] == '_' or root[0] == '-') return false;
    while (parts.next()) |part| if (part.len == 0) return false;
    return true;
}
const Row = struct {
    plan: *const Plan,
    a: A,
    source: Source,
    values: []?sql.Datum,
    limits: sql.EvalLimits,
    const Frame = struct {
        row: *Row,
        index: usize,
        fn read(raw: *anyopaque, _: A, ordinal: u32, kind: sql.ColumnRead) !sql.Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const dependency = self.row.plan.bindings[self.index].dependencies[ordinal];
            return switch (dependency) {
                .field => |path| self.row.source.read(self.row.source.context, self.row.a, path, kind),
                .binding => |index| blk: {
                    const value = self.row.values[index] orelse return error.InvalidQueryResponse;
                    break :blk switch (kind) {
                        .value => value,
                        .missing => sql.Datum.json(.{ .bool = value.missing }),
                        .identity => if (value.missing) .{} else sql.Datum.json(.{ .string = try self.row.a.dupe(u8, &(try @import("../common/value_identity.zig").identity(try self.row.plan.bindings[index].program.?.resultJson(self.row.a, value, self.row.limits)))) }),
                    };
                },
            };
        }
    };
    fn binding(self: *Row, index: usize) anyerror!sql.Datum {
        if (self.values[index]) |value| return value;
        var frame: Frame = .{ .row = self, .index = index };
        var limits = self.limits;
        limits.column_loader = .{ .context = &frame, .read = Frame.read };
        const value = self.plan.bindings[index].program.?.evaluate(self.a, &.{}, self.plan.parameters.items, limits) catch |err| return scalarFailure(err);
        self.values[index] = value;
        return value;
    }
};

test "api query contract field projection shares SQL CASE and skips untaken payload reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, temp,
        \\[ {"name":"identity","expression":{"call":"value_identity","args":[{"field":"image"}]}},
        \\  {"name":"changed","expression":{"is_distinct_from":[{"ref":"identity"},{"param":"cached"}]}},
        \\  {"name":"image","expression":{"case":[{"when":{"ref":"changed"},"then":{"field":"image"}}],"else":{"literal":null}}} ]
    , .{});
    var params: std.json.ObjectMap = .empty;
    try params.put(temp, "cached", .{ .string = "opaque" });
    var plan = try Plan.init(a, .{ .fields = fields, .params = .{ .object = params } });
    defer plan.deinit();
    const Mock = struct {
        identities: usize = 0,
        payloads: usize = 0,
        fn read(raw: *anyopaque, _: A, path: []const u8, kind: sql.ColumnRead) !sql.Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualStrings("image", path);
            switch (kind) {
                .identity => {
                    self.identities += 1;
                    return sql.Datum.json(.{ .string = "opaque" });
                },
                .missing => return sql.Datum.json(.{ .bool = false }),
                .value => {
                    self.payloads += 1;
                    return error.UnexpectedPayloadRead;
                },
            }
        }
    };
    var mock: Mock = .{};
    const output = try plan.evaluate(temp, .{ .context = &mock, .read = Mock.read }, null, .{});
    try std.testing.expectEqual(@as(usize, 1), mock.identities);
    try std.testing.expectEqual(@as(usize, 0), mock.payloads);
    try std.testing.expect(!output.object.get("changed").?.bool);
    try std.testing.expect(output.object.get("image").? == .null);
}

test "api query contract field projection validates every branch refs parameters and limits" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    for ([_][]const u8{
        \\[{"name":"x","expression":{"ref":"x"}}]
        ,
        \\[{"name":"x","expression":{"ref":"y"}},{"name":"y","expression":{"ref":"x"}}]
        ,
        \\[{"name":"x","expression":{"case":[{"when":{"literal":false},"then":{"call":"ai_probability","args":[]}}],"else":{"literal":null}}}]
        ,
        \\[{"name":"x","expression":{"literal":1}},{"name":"x","expression":{"literal":2}}]
    }) |json| {
        const fields = try std.json.parseFromSliceLeaky([]const V, temp, json, .{});
        if (Plan.init(a, .{ .fields = fields })) |value| {
            var unexpected = value;
            unexpected.deinit();
            return error.ExpectedProjectionRejection;
        } else |_| {}
    }
    const fields = try std.json.parseFromSliceLeaky([]const V, temp,
        \\[{"name":"x","expression":{"param":"absent"}}]
    , .{});
    try std.testing.expectError(error.InvalidSqlParameters, Plan.init(a, .{ .fields = fields }));
}

test "api query contract projection preserves null missing typed numbers and lazy errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const selected = try std.json.parseFromSliceLeaky(V, a,
        \\{"present":{"call":"value_identity","args":[{"field":"null"}]},"missing":{"call":"value_identity","args":[{"field":"absent"}]},"is_missing":{"call":"is_missing","args":[{"field":"absent"}]},"integer":{"field":"integer"},"lazy":{"case":[{"when":{"literal":false},"then":{"divide":[{"literal":1},{"literal":0}]}}],"else":{"literal":7}},"sum":{"add":[{"literal":3},{"literal":4}]}}
    , .{});
    const encoded = try encode(a, &.{}, selected, .{ .object = .empty });
    var plan = try decode(std.testing.allocator, encoded);
    defer plan.deinit();
    const root = try std.json.parseFromSliceLeaky(V, a, "{\"null\":null,\"integer\":9007199254740993}", .{});
    var source: JsonSource = .{ .id = "a", .root = root };
    const value = try plan.evaluate(a, source.source(), null, .{});
    try std.testing.expect(value.object.get("present").? == .string);
    try std.testing.expect(value.object.get("missing").? == .null);
    try std.testing.expect(value.object.get("is_missing").?.bool);
    try std.testing.expectEqual(@as(i64, 9007199254740993), value.object.get("integer").?.integer);
    try std.testing.expectEqual(@as(i64, 7), value.object.get("sum").?.integer);
    try std.testing.expectEqual(@as(i64, 7), value.object.get("lazy").?.integer);
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    try std.testing.expectError(error.Canceled, plan.evaluate(a, source.source(), null, .{ .checkpoint = Cancel.check }));
}

test "api query contract projection owns its compilation under allocation failures" {
    const Check = struct {
        fn run(a: A) !void {
            const body = "{\"fields\":[{\"name\":\"x\",\"expression\":{\"literal\":1}}]}";
            var plan = try decode(a, body);
            defer plan.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "api query contract projection bounds dependency depth before recursive compilation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fields: std.ArrayList(V) = .empty;
    for (0..64) |i| {
        var expr = try std.json.parseFromSliceLeaky(V, a, if (i == 63) "{\"literal\":1}" else try std.fmt.allocPrint(a, "{{\"ref\":\"x{d}\"}}", .{i + 1}), .{});
        for (0..40) |_| {
            var branch: std.json.ObjectMap = .empty;
            try branch.put(a, "when", try std.json.parseFromSliceLeaky(V, a, "{\"literal\":true}", .{}));
            try branch.put(a, "then", expr);
            var branches: std.ArrayList(V) = .empty;
            try branches.append(a, .{ .object = branch });
            var object: std.json.ObjectMap = .empty;
            try object.put(a, "case", .{ .array = .fromOwnedSlice(a, try branches.toOwnedSlice(a)) });
            expr = .{ .object = object };
        }
        var field: std.json.ObjectMap = .empty;
        try field.put(a, "name", .{ .string = try std.fmt.allocPrint(a, "x{d}", .{i}) });
        try field.put(a, "expression", expr);
        try fields.append(a, .{ .object = field });
    }
    try std.testing.expectError(error.DecisionLimitExceeded, Plan.init(std.testing.allocator, .{ .fields = fields.items }));
    // The maximum legal alias chain remains usable and evaluates once per
    // binding; unrelated literal JSON never creates graph dependencies.
    fields.clearRetainingCapacity();
    for (0..64) |i| {
        var field: std.json.ObjectMap = .empty;
        try field.put(a, "name", .{ .string = try std.fmt.allocPrint(a, "x{d}", .{i}) });
        try field.put(a, "expression", try std.json.parseFromSliceLeaky(V, a, if (i == 63) "{\"literal\":1}" else try std.fmt.allocPrint(a, "{{\"ref\":\"x{d}\"}}", .{i + 1}), .{}));
        try fields.append(a, .{ .object = field });
    }
    var chain = try Plan.init(std.testing.allocator, .{ .fields = fields.items });
    defer chain.deinit();
    var source: JsonSource = .{ .id = "x", .root = .null };
    const values = try chain.evaluate(a, source.source(), null, .{});
    try std.testing.expectEqual(@as(i64, 1), values.object.get("x0").?.integer);
    // Literal JSON with a ref member is data, not a dependency edge.
    var literal = try decode(std.testing.allocator, "{\"fields\":[{\"name\":\"x\",\"expression\":{\"literal\":{\"ref\":\"x\"}}}]}");
    defer literal.deinit();
}

test "api query contract projection resolves original array indexes and nested user keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var source: JsonSource = .{ .id = "x", .root = try std.json.parseFromSliceLeaky(V, a, "{\"arr\":[{\"n\":1},{\"n\":2}],\"metadata\":{\"_type\":\"image\"}}", .{}) };
    const value = try source.source().read(&source, a, "arr.1.n", .value);
    try std.testing.expectEqual(@as(i64, 2), value.value.integer);
    try std.testing.expect(!(try source.source().read(&source, a, "arr.1.n", .missing)).value.bool);
    try std.testing.expect((try source.source().read(&source, a, "arr.9.n", .missing)).value.bool);
    const all = try source.source().read(&source, a, "arr.n", .value);
    try std.testing.expectEqual(@as(usize, 2), all.value.array.items.len);
    try std.testing.expect(validPath("metadata._type"));
    try std.testing.expect(!validPath("_private.n"));
}

test "api query contract projection wire spans validate receipts and preserve lexical values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var plan = try decode(std.testing.allocator, "{\"fields\":[{\"name\":\"n\",\"expression\":{\"literal\":1}}]}");
    defer plan.deinit();
    const bytes = try std.fmt.allocPrint(a, "{{\"n\":9007199254740993,\"extra\":\"quote\\\"\\n\\u03bb\",\"{s}\":\"{s}\"}}", .{ marker, plan.fingerprint });
    const public = (try plan.wireOutput(a, bytes, false)).?;
    try std.testing.expect(std.mem.indexOf(u8, public.bytes, "9007199254740993") != null);
    try std.testing.expect(std.mem.indexOf(u8, public.bytes, "\\u03bb") != null);
    try std.testing.expect(std.mem.indexOf(u8, public.bytes, marker) == null);
    const transport = (try plan.wireOutput(a, bytes, true)).?;
    try std.testing.expect(transport.bytes.ptr == bytes.ptr);
    try std.testing.expectError(error.InvalidQueryResponse, plan.wireOutput(a, "{\"__antfly_projection\":\"wrong\"}", false));
    const missing = try std.fmt.allocPrint(a, "{{\"{s}\":\"{s}\"}}", .{ marker, plan.fingerprint });
    try std.testing.expectError(error.InvalidQueryResponse, plan.wireOutput(a, missing, false));
    try std.testing.expect((try plan.wireOutput(a, "{\"unrelated\":true}", false)) == null);
    try std.testing.expectError(error.DuplicateField, plan.wireOutput(a, "{\"n\":1,\"n\":2}", false));
}
