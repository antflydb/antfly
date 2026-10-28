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
/// Leave room for the internal request envelope in std.json's fixed-depth
/// writer. Data inside literals/parameters is not an expression AST, but must
/// pass this iterative budget before any recursive copy, hash or stringify.
pub fn validateInput(a: A, value: V) !void {
    const Item = struct { value: V, depth: usize };
    var pending: std.ArrayList(Item) = .empty;
    defer pending.deinit(a);
    try pending.append(a, .{ .value = value, .depth = 0 });
    var scheduled: usize = 1;
    while (pending.pop()) |item| {
        switch (item.value) {
            .array, .object => {
                if (item.depth >= 240) return error.DecisionLimitExceeded;
                const count = if (item.value == .array) item.value.array.items.len else item.value.object.count();
                if (count > 65536 -| scheduled) return error.DecisionLimitExceeded;
                scheduled += count;
                if (item.value == .array) {
                    for (item.value.array.items) |child| try pending.append(a, .{ .value = child, .depth = item.depth + 1 });
                } else {
                    var members = item.value.object.iterator();
                    while (members.next()) |member| try pending.append(a, .{ .value = member.value_ptr.*, .depth = item.depth + 1 });
                }
            },
            else => {},
        }
    }
}
/// Normalize the stable string field mask and named scalar expressions once.
/// The encoded plan is internal request ownership, not a new public protocol.
pub const Encoding = struct { json: []u8, plan: *Plan };
pub fn encode(a: A, fields: []const []const u8, selected: V, params: V) ![]u8 {
    const encoded = try encodePrepared(a, fields, selected, params);
    defer {
        encoded.plan.deinit();
        a.destroy(encoded.plan);
    }
    return encoded.json;
}
/// Admission owns the program; synchronous consumers borrow it with an exact
/// encoded-input witness. Transported or cloned inputs prepare their own plan.
pub fn encodePrepared(a: A, fields: []const []const u8, selected: V, params: V) !Encoding {
    if (selected != .object or selected.object.count() == 0 or selected.object.count() > 64 or params != .object or params.object.count() > 1024) return error.InvalidQueryRequest;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    try validateInput(temp, selected);
    try validateInput(temp, params);
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
    return prepareRequest(a, request);
}
/// Public source delivery uses the same read/projection program with no named
/// scalar outputs. The completion marker remains private transport metadata.
pub fn encodeSourcePrepared(a: A, fields: []const []const u8) !Encoding {
    const values = try a.alloc(V, fields.len);
    defer a.free(values);
    for (fields, values) |field, *value| value.* = .{ .string = field };
    return prepareRequest(a, .{ .fields = values, .params = .{ .object = .empty } });
}
fn prepareRequest(a: A, request: Request) !Encoding {
    const checked = try a.create(Plan);
    errdefer a.destroy(checked);
    checked.* = try Plan.init(a, request);
    errdefer checked.deinit();
    return .{ .json = try std.json.Stringify.valueAlloc(a, request, .{}), .plan = checked };
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
    return req.field_projection_json.len != 0 and sourceStorageEligible(req);
}
pub fn sourceStorageEligible(req: anytype) bool {
    return req.include_stored and
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
    read_raw: ?*const fn (*anyopaque, A, []const u8) anyerror!?[]const u8 = null,
    /// Only immutable native extents with strict syntax/value-shape evidence.
    /// Unqualified ports retain the scanner and typed-number normalizer.
    read_normalized: ?*const fn (*anyopaque, []const u8) anyerror!?sql.NormalizedOutput = null,
    prefetch: ?*const fn (*anyopaque, []const []const u8) anyerror!void = null,
};
const Dependency = union(enum) { field: []const u8, binding: usize };
const Binding = struct { name: []const u8, expression: V, state: enum { unseen, visiting, ready } = .unseen, program: ?sql.Program = null, dependencies: []const Dependency = &.{}, height: usize = 0 };
pub const BorrowedPlan = struct {
    encoded: []const u8,
    plan: *const Plan,
    pub fn get(self: BorrowedPlan, encoded: []const u8) ?*const Plan {
        return if (self.encoded.ptr == encoded.ptr and self.encoded.len == encoded.len) self.plan else null;
    }
};
/// Native-producer evidence, never a wire field or an authorization grant.
/// Replacing bytes or changing the program invalidates it. An exact owned
/// copy may rebind locally; transport must validate before establishing one.
pub const Receipt = struct {
    bytes: []const u8,
    fingerprint: [64]u8,
    members_end: usize,
    pub fn matches(self: @This(), bytes: []const u8, fingerprint: [64]u8) bool {
        return self.bytes.ptr == bytes.ptr and self.bytes.len == bytes.len and std.mem.eql(u8, &self.fingerprint, &fingerprint);
    }
    /// Caller has just copied the complete immutable extent into owned bytes.
    pub fn afterCopy(self: @This(), previous: []const u8, bytes: []const u8) ?Receipt {
        if (!self.matches(previous, self.fingerprint) or previous.len != bytes.len) return null;
        var copied = self;
        copied.bytes = bytes;
        return copied;
    }
    pub fn output(self: @This(), transport: bool) @import("antfly-json").RawObject {
        return @import("antfly-json").RawObject.fromImmutableValidated(self.bytes, if (transport) null else self.members_end);
    }
};
pub const Plan = struct {
    arena: *std.heap.ArenaAllocator,
    source_fields: []const []const u8 = &.{},
    required_fields: std.StringHashMapUnmanaged(void) = .empty,
    bindings: []Binding = &.{},
    evaluation_order: []const usize = &.{},
    unconditional_fields: []const []const u8 = &.{},
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
        if (input.fields.len > 4160 or input.params != .object) return error.InvalidQueryRequest;
        // Validate the complete data tree, including literal/parameter payloads,
        // before the first recursive serialization below.
        for (input.fields) |field| try validateInput(a, field);
        try validateInput(a, input.params);
        var counter: std.Io.Writer.Discarding = .init(&.{});
        try std.json.Stringify.value(input, .{}, &counter.writer);
        if (counter.fullCount() > 16 * 1024 * 1024) return error.DecisionLimitExceeded;
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
        // Eligibility and deduplication are immutable program facts. Prepare
        // once per request, never once per row or selected output.
        var unconditional: std.ArrayList([]const u8) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (self.bindings) |binding| if (binding.program.?.directOutputColumn()) |ordinal| {
            if (binding.dependencies[ordinal] == .field) {
                const path = binding.dependencies[ordinal].field;
                if (std.mem.eql(u8, path, "_id")) continue;
                if (!(try seen.getOrPut(a, path)).found_existing) try unconditional.append(a, path);
            }
        };
        self.unconditional_fields = unconditional.items;
        return self;
    }
    /// Caller has strictly validated output members and the plan fingerprint.
    /// Nonstandard worker marker placement retains the general wire fallback.
    pub fn receiptForValidated(self: *const Plan, bytes: []const u8) ?Receipt {
        const prefix = if (self.bindings.len == 0) "\"" ++ marker ++ "\":\"" else ",\"" ++ marker ++ "\":\"";
        if (bytes.len < prefix.len + 64 + 3) return null;
        const end = bytes.len - prefix.len - 64 - 2;
        if (!std.mem.startsWith(u8, bytes[end..], prefix) or !std.mem.eql(u8, bytes[end + prefix.len ..][0..64], &self.fingerprint) or !std.mem.endsWith(u8, bytes, "\"}")) return null;
        return .{ .bytes = bytes, .fingerprint = self.fingerprint, .members_end = end };
    }
    /// Receipts allocate only for selected projected hits, not every candidate.
    pub fn ownValidatedReceipt(self: *const Plan, a: A, bytes: []const u8) !?*Receipt {
        const value = self.receiptForValidated(bytes) orelse return null;
        const owned = try a.create(Receipt);
        owned.* = value;
        return owned;
    }
    /// Only for fresh, complete native evaluateEncoded output.
    pub fn receipt(self: *const Plan, bytes: []const u8) Receipt {
        return self.receiptForValidated(bytes).?;
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
        if (transport) return @import("antfly-json").RawObject.fromImmutableValidated(bytes, null);
        try observer.writer.writer.writeByte('}');
        return @import("antfly-json").RawObject.fromImmutableValidated(try a.dupe(u8, observer.writer.written()), null);
    }
    /// Native delivery keeps borrowed JSON through pure output expressions;
    /// aliases materialize once only when a consuming operator requires Datum.
    pub fn evaluateEncoded(self: *const Plan, a: A, output: A, source: Source, previous: ?V, limits: sql.EvalLimits) ![]u8 {
        if (previous != null) return std.json.Stringify.valueAlloc(output, try self.evaluate(a, source, previous, limits), .{});
        if (limits.checkpoint) |poll| try poll(limits.checkpoint_context);
        if (source.prefetch) |prefetch| try prefetch(source.context, self.unconditional_fields);
        const values = try a.alloc(?sql.Datum, self.bindings.len);
        @memset(values, null);
        const raw_values = try a.alloc(?sql.NormalizedOutput, self.bindings.len);
        @memset(raw_values, null);
        const origins = try a.alloc(?[]const u8, self.bindings.len);
        @memset(origins, null);
        var row: Row = .{ .plan = self, .a = a, .source = source, .values = values, .raw_values = raw_values, .origins = origins, .limits = limits };
        var bytes: usize = 0;
        for (self.evaluation_order) |i| {
            var frame: Row.Frame = .{ .row = &row, .index = i };
            var local = limits;
            local.column_loader = .{ .context = &frame, .read = Row.Frame.read };
            local.output_column_loader = .{ .context = &frame, .read = Row.Frame.readRaw, .normalized = Row.Frame.readNormalized };
            const result = self.bindings[i].program.?.evaluateOutput(a, &.{}, self.parameters.items, local) catch |err| return scalarFailure(err);
            switch (result) {
                .raw => |raw| {
                    raw_values[i] = raw.value;
                    try row.normalized.put(a, .{ .ptr = @intFromPtr(raw.value.original.ptr), .len = raw.value.original.len }, raw.value);
                    origins[i] = switch (self.bindings[i].dependencies[raw.column]) {
                        .field => |path| path,
                        .binding => |index| origins[index],
                    };
                    bytes +|= raw.value.bytes.len;
                },
                .datum => |datum| {
                    values[i] = datum;
                    bytes +|= try @import("../sql/operators.zig").datumBytes(datum);
                },
            }
            if (bytes > limits.output_bytes) return error.DecisionLimitExceeded;
        }
        var encoded: std.Io.Writer.Allocating = .init(@import("antfly_platform").allocator.reclaimingScratch(a));
        defer encoded.deinit();
        try encoded.writer.writeByte('{');
        for (self.bindings, 0..) |binding, i| {
            if (i != 0) try encoded.writer.writeByte(',');
            try std.json.Stringify.value(binding.name, .{}, &encoded.writer);
            try encoded.writer.writeByte(':');
            if (raw_values[i]) |raw| try encoded.writer.writeAll(raw.bytes) else {
                const value = binding.program.?.resultJson(a, values[i].?, limits) catch |err| return scalarFailure(err);
                try std.json.Stringify.value(value, .{}, &encoded.writer);
            }
            if (encoded.written().len > limits.output_bytes) return error.DecisionLimitExceeded;
        }
        if (self.bindings.len != 0) try encoded.writer.writeByte(',');
        try std.json.Stringify.value(marker, .{}, &encoded.writer);
        try encoded.writer.writeByte(':');
        try std.json.Stringify.value(&self.fingerprint, .{}, &encoded.writer);
        try encoded.writer.writeByte('}');
        if (encoded.written().len > limits.output_bytes) return error.DecisionLimitExceeded;
        return output.dupe(u8, encoded.written());
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
    raw_values: ?[]?sql.NormalizedOutput = null,
    origins: ?[]?[]const u8 = null,
    normalized: std.AutoHashMapUnmanaged(struct { ptr: usize, len: usize }, sql.NormalizedOutput) = .empty,
    loaded_raws: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    identities: ?[]?sql.Datum = null,
    limits: sql.EvalLimits,
    const Frame = struct {
        row: *Row,
        index: usize,
        fn fieldRaw(self: *@This(), path: []const u8) !?[]const u8 {
            const slot = try self.row.loaded_raws.getOrPut(self.row.a, path);
            if (!slot.found_existing) slot.value_ptr.* = if (self.row.source.read_raw) |read_raw| try read_raw(self.row.source.context, self.row.a, path) else null;
            return slot.value_ptr.*;
        }
        fn readNormalized(raw: *anyopaque, ordinal: u32) !?sql.NormalizedOutput {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return switch (self.row.plan.bindings[self.index].dependencies[ordinal]) {
                .binding => |index| if (self.row.raw_values) |values| values[index] else null,
                .field => |path| cached: {
                    if (self.row.source.read_normalized) |prepared| if (try prepared(self.row.source.context, path)) |value| break :cached value;
                    const bytes = (try self.fieldRaw(path)) orelse break :cached null;
                    break :cached self.row.normalized.get(.{ .ptr = @intFromPtr(bytes.ptr), .len = bytes.len });
                },
            };
        }
        fn readRaw(raw: *anyopaque, _: A, ordinal: u32) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return switch (self.row.plan.bindings[self.index].dependencies[ordinal]) {
                .field => |path| self.fieldRaw(path),
                .binding => |index| if (self.row.raw_values) |values| (if (values[index]) |value| value.bytes else null) else null,
            };
        }
        fn read(raw: *anyopaque, _: A, ordinal: u32, kind: sql.ColumnRead) !sql.Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const dependency = self.row.plan.bindings[self.index].dependencies[ordinal];
            return switch (dependency) {
                .field => |path| self.row.source.read(self.row.source.context, self.row.a, path, kind),
                .binding => |index| blk: {
                    if (kind == .identity) if (self.row.identities) |identities| if (identities[index]) |identity| break :blk identity;
                    if (self.row.raw_values) |raws| if (raws[index] != null) {
                        if (kind == .missing) break :blk sql.Datum.json(.{ .bool = false });
                        if (kind == .identity) if (self.row.origins) |origins| if (origins[index]) |path| {
                            const identity = try self.row.source.read(self.row.source.context, self.row.a, path, .identity);
                            if (self.row.identities == null) {
                                self.row.identities = try self.row.a.alloc(?sql.Datum, self.row.plan.bindings.len);
                                @memset(self.row.identities.?, null);
                            }
                            self.row.identities.?[index] = identity;
                            break :blk identity;
                        };
                    };
                    if (self.row.values[index] == null) {
                        const raws = self.row.raw_values orelse return error.InvalidQueryResponse;
                        const bytes = (raws[index] orelse return error.InvalidQueryResponse).bytes;
                        self.row.values[index] = sql.Datum.json(try @import("antfly-json").parseFromSliceLeaky(V, self.row.a, bytes, .{ .allocate = .alloc_if_needed }));
                    }
                    const value = self.row.values[index].?;
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

test "api query contract raw CASE outputs match VM and materialize alias consumers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a,
        \\[ {"name":"body","expression":{"case":[{"when":{"literal":true},"then":{"field":"body"}}],"else":{"field":"absent"}}},
        \\  {"name":"copy","expression":{"ref":"body"}},
        \\  {"name":"kind","expression":{"call":"jsonb_typeof","args":[{"ref":"copy"}]}},
        \\  {"name":"null","expression":{"field":"null"}},
        \\  {"name":"missing","expression":{"field":"absent"}} ]
    , .{});
    var plan = try Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    const Mock = struct {
        json: JsonSource,
        raw_reads: usize = 0,
        fn raw(ptr: *anyopaque, _: A, path: []const u8) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, "body")) {
                self.raw_reads += 1;
                return "{\"arr\":[1,null,\"escaped\\n\"],\"n\":9007199254740993}";
            }
            if (std.mem.eql(u8, path, "null")) return "null";
            return null;
        }
        fn read(ptr: *anyopaque, a2: A, path: []const u8, kind: sql.ColumnRead) !sql.Datum {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.json.source().read(&self.json, a2, path, kind);
        }
    };
    var source: Mock = .{ .json = .{ .id = "a", .root = try std.json.parseFromSliceLeaky(V, a, "{\"body\":{\"arr\":[1,null,\"escaped\\n\"],\"n\":9007199254740993},\"null\":null}", .{}) } };
    const normal = try plan.evaluate(a, .{ .context = &source, .read = Mock.read }, null, .{});
    const encoded = try plan.evaluateEncoded(a, a, .{ .context = &source, .read = Mock.read, .read_raw = Mock.raw }, null, .{});
    const expected = try std.json.Stringify.valueAlloc(a, normal, .{});
    try std.testing.expectEqualStrings(expected, encoded);
    try std.testing.expectEqual(@as(usize, 1), source.raw_reads);
    try std.testing.expectError(error.DecisionLimitExceeded, plan.evaluateEncoded(a, a, .{ .context = &source, .read = Mock.read, .read_raw = Mock.raw }, null, .{ .output_bytes = 8 }));
}

test "api query contract literal and parameter depth bounded before stringify" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var value: V = .null;
    for (0..300) |_| {
        var items: std.array_list.Managed(V) = .init(a);
        try items.append(value);
        value = .{ .array = items };
    }
    try std.testing.expectError(error.DecisionLimitExceeded, validateInput(a, value));
    var params: std.json.ObjectMap = .empty;
    try params.put(a, "unused", value);
    try std.testing.expectError(error.DecisionLimitExceeded, Plan.init(a, .{ .fields = &.{}, .params = .{ .object = params } }));
}

test "api query contract producer receipts omit only the private suffix and retire on replacement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a, "[{\"name\":\"n\",\"expression\":{\"field\":\"n\"}}]", .{});
    var plan = try Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    var source: JsonSource = .{ .id = "a", .root = try std.json.parseFromSliceLeaky(V, a, "{\"n\":5}", .{}) };
    const bytes = try plan.evaluateEncoded(a, a, source.source(), null, .{});
    const receipt = plan.receipt(bytes);
    try std.testing.expect(receipt.matches(bytes, plan.fingerprint));
    try std.testing.expect(!receipt.matches(try a.dupe(u8, bytes), plan.fingerprint));
    try std.testing.expectEqualStrings("{\"n\":5}", try std.json.Stringify.valueAlloc(a, receipt.output(false), .{}));
    try std.testing.expectEqualStrings(bytes, try std.json.Stringify.valueAlloc(a, receipt.output(true), .{}));
}
test "api query contract batches unconditional fields without speculative CASE reads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a,
        \\[ {"name":"a","expression":{"field":"a"}}, {"name":"b","expression":{"field":"b"}},
        \\  {"name":"conditional","expression":{"case":[{"when":{"literal":false},"then":{"field":"secret"}}],"else":{"literal":null}}} ]
    , .{});
    var plan = try Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    const Mock = struct {
        batches: usize = 0,
        fn prefetch(ptr: *anyopaque, paths: []const []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batches += 1;
            try std.testing.expectEqual(@as(usize, 2), paths.len);
            for (paths) |path| try std.testing.expect(!std.mem.eql(u8, path, "secret"));
        }
        fn read(_: *anyopaque, _: A, path: []const u8, _: sql.ColumnRead) !sql.Datum {
            if (std.mem.eql(u8, path, "secret")) return error.SpeculativeRead;
            return sql.Datum.json(.{ .integer = 1 });
        }
    };
    var mock: Mock = .{};
    _ = try plan.evaluateEncoded(a, a, .{ .context = &mock, .read = Mock.read, .prefetch = Mock.prefetch }, null, .{});
    try std.testing.expectEqual(@as(usize, 1), mock.batches);
}

test "api query contract optional producer receipts clone independently and release allocation failures" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, temp, "[{\"name\":\"n\",\"expression\":{\"field\":\"n\"}}]", .{});
    var plan = try Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    var source: JsonSource = .{ .id = "a", .root = try std.json.parseFromSliceLeaky(V, temp, "{\"n\":5}", .{}) };
    const Hit = @import("../storage/db/types.zig").SearchHit;
    var hit: Hit = .{ .id = try a.dupe(u8, "a") };
    defer hit.deinit(a);
    hit.computed_json = try plan.evaluateEncoded(temp, a, source.source(), null, .{});
    hit.computed_receipt = try plan.ownValidatedReceipt(a, hit.computed_json.?);
    const Copy = struct {
        fn run(alloc: A, original: *const Hit, fingerprint: [64]u8) !void {
            var copied = try original.clone(alloc);
            defer copied.deinit(alloc);
            try std.testing.expect(copied.computed_receipt != original.computed_receipt);
            try std.testing.expect(copied.computed_receipt.?.matches(copied.computed_json.?, fingerprint));
            try std.testing.expect(!copied.computed_receipt.?.matches(original.computed_json.?, fingerprint));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Copy.run, .{ &hit, plan.fingerprint });
}

test "api query contract raw numeric provenance preserves aliases and identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a,
        \\[ {"name":"n","expression":{"field":"n"}}, {"name":"copy","expression":{"ref":"n"}},
        \\  {"name":"from_field","expression":{"call":"value_identity","args":[{"field":"n"}]}},
        \\  {"name":"from_alias","expression":{"call":"value_identity","args":[{"ref":"n"}]}},
        \\  {"name":"from_copy","expression":{"call":"value_identity","args":[{"ref":"copy"}]}} ]
    , .{});
    var plan = try Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    const Mock = struct {
        json: JsonSource,
        original: []const u8,
        fn raw(ptr: *anyopaque, _: A, _: []const u8) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.original;
        }
        fn read(ptr: *anyopaque, alloc: A, path: []const u8, kind: sql.ColumnRead) !sql.Datum {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.json.source().read(&self.json, alloc, path, kind);
        }
    };
    for ([_][]const u8{ "-0.0", "1e-400", "1.234567890123456789", "{\"a\":[-0.0,1e-400,1.234567890123456789]}" }) |original| {
        var source: Mock = .{ .original = original, .json = .{ .id = "a", .root = try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"n\":{s}}}", .{original}), .{}) } };
        const normal = try plan.evaluate(a, .{ .context = &source, .read = Mock.read }, null, .{});
        const encoded = try plan.evaluateEncoded(a, a, .{ .context = &source, .read = Mock.read, .read_raw = Mock.raw }, null, .{});
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, normal, .{}), encoded);
    }
}

test "api query contract raw aliases retain selected provenance without DOM reads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a,
        \\[ {"name":"v","expression":{"case":[{"when":{"literal":true},"then":{"field":"payload"}}],"else":{"field":"secret"}}},
        \\ {"name":"copy","expression":{"ref":"v"}},
        \\ {"name":"missing","expression":{"call":"is_missing","args":[{"ref":"copy"}]}},
        \\ {"name":"token","expression":{"call":"value_identity","args":[{"ref":"copy"}]}},
        \\ {"name":"token_again","expression":{"call":"value_identity","args":[{"ref":"copy"}]}} ]
    , .{});
    var plan = try Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    const Mock = struct {
        identity_reads: usize = 0,
        fn raw(_: *anyopaque, _: A, path: []const u8) !?[]const u8 {
            if (!std.mem.eql(u8, path, "payload")) return error.SpeculativeRead;
            return "{\"items\":[1e-400,-0.0,null,\"escaped\\n\"]}";
        }
        fn read(ptr: *anyopaque, _: A, path: []const u8, kind: sql.ColumnRead) !sql.Datum {
            if (!std.mem.eql(u8, path, "payload") or kind != .identity) return error.UnnecessaryMaterialization;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.identity_reads += 1;
            return sql.Datum.json(.{ .string = "qualified-source-identity" });
        }
    };
    var mock: Mock = .{};
    const encoded = try plan.evaluateEncoded(a, a, .{ .context = &mock, .read = Mock.read, .read_raw = Mock.raw }, null, .{});
    const parsed = try std.json.parseFromSliceLeaky(V, a, encoded, .{});
    try std.testing.expect(!parsed.object.get("missing").?.bool);
    try std.testing.expectEqualStrings("qualified-source-identity", parsed.object.get("token").?.string);
    try std.testing.expectEqualStrings("qualified-source-identity", parsed.object.get("token_again").?.string);
    try std.testing.expectEqual(@as(usize, 1), mock.identity_reads);
    try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, parsed.object.get("v").?, .{}), try std.json.Stringify.valueAlloc(a, parsed.object.get("copy").?, .{}));
}

test "api query contract normalized receipts preserve work depth bytes and cancellation" {
    const budget = @import("../sql/raw_json_budget.zig");
    const Work = @import("../sql/numeric_value.zig").Context;
    const a = std.testing.allocator;
    var work: Work = .{ .alloc = a, .remaining = 10 };
    const value = try budget.normalizePrepared(a, "{\"n\":[1,null]}", .{ .depth = 3, .wire_bytes = 1024 }, &work);
    defer if (value.bytes.ptr != value.original.ptr) a.free(value.bytes);
    try std.testing.expectEqual(@as(usize, 4), value.nodes);
    try std.testing.expectEqual(@as(usize, 3), value.height);
    var small: Work = .{ .alloc = a, .remaining = 3 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, value.charge(.{ .depth = 3, .wire_bytes = 1024 }, &small));
    var shallow: Work = .{ .alloc = a };
    try std.testing.expectError(error.SqlProgramLimitExceeded, value.charge(.{ .depth = 2, .wire_bytes = 1024 }, &shallow));
    var narrow: Work = .{ .alloc = a };
    try std.testing.expectError(error.SqlProgramLimitExceeded, value.charge(.{ .depth = 3, .wire_bytes = 2 }, &narrow));
    const Guard = struct {
        fn poll(_: ?*anyopaque) !void {
            return error.QueryCanceled;
        }
    };
    var cancelled: Work = .{ .alloc = a };
    try std.testing.expectError(error.QueryCanceled, value.charge(.{ .depth = 3, .wire_bytes = 1024, .checkpoint = Guard.poll }, &cancelled));
}
