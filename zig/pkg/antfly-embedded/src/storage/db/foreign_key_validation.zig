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

//! Typed FK validation projections and composable reference-set witnesses.
//! These are read-side facts, not schema-publication or parent-root authority.
const std = @import("std");
const contract = @import("relational_integrity_contract.zig");
const registry = @import("schema_registry.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const keys = @import("relational_index_keys.zig");
const schema = @import("../schema.zig");
const native = @import("../relational_index.zig");
const expressions = @import("../../schema/relational_expression.zig");
const A = std.mem.Allocator;
pub const Control = @import("relational_nullability.zig").Control;

/// Count plus addition modulo 2^256 of domain-separated key hashes. Unlike
/// XOR, duplicate pairs do not disappear. Source and parent owners may combine
/// disjoint ranges in any order. Full cohort/root/epoch verification is separate.
pub const Witness = struct {
    count: u64 = 0,
    sum: [32]u8 = @splat(0),

    pub fn identity(key: []const u8) ![32]u8 {
        if ((try contract.parseKey(key)).kind != .reference) return error.InvalidSchemaValidationJob;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/fk-validation-reference-set/v1");
        state.update(key);
        var result: [32]u8 = undefined;
        state.final(&result);
        return result;
    }
    pub fn validate(self: Witness) !void {
        if (self.count == 0 and !std.mem.allEqual(u8, &self.sum, 0)) return error.InvalidSchemaValidationJob;
    }
    pub fn combine(self: *Witness, other: Witness) !void {
        try self.validate();
        try other.validate();
        const count = std.math.add(u64, self.count, other.count) catch return error.InvalidSchemaValidationJob;
        const sum = std.mem.readInt(u256, &self.sum, .little) +% std.mem.readInt(u256, &other.sum, .little);
        std.mem.writeInt(u256, &self.sum, sum, .little);
        self.count = count;
    }
    pub fn addIdentity(self: *Witness, identity_hash: [32]u8) !void {
        try self.combine(.{ .count = 1, .sum = identity_hash });
    }
    pub fn addReference(self: *Witness, key: []const u8) !void {
        try self.addIdentity(try identity(key));
    }
    pub fn eql(self: Witness, other: Witness) bool {
        return self.count == other.count and std.mem.eql(u8, &self.sum, &other.sum);
    }
};

pub const Rule = struct {
    name: []const u8,
    columns: []const []const u8,
    generation: contract.Generation,
    parent_generation: contract.Generation,
    match: native.ForeignKeyMatch = .simple,
};

/// Owns an immutable schema epoch and heap-stable arenas. Historical ordinal
/// bindings use retained scratch, never an arena created/destroyed per row.
pub const ChildPlan = struct {
    const Bound = struct { rule: Rule, tuple: keys.TuplePlan };
    alloc: A,
    arena: *std.heap.ArenaAllocator,
    scratch: *std.heap.ArenaAllocator,
    view: registry.SchemaView,
    table_name: []const u8,
    rules: []const Bound,
    names: []const []const u8,
    identity: [32]u8,
    tuple_bytes: std.ArrayList(u8) = .empty,

    pub fn init(a: A, table_name: []const u8, table: schema.TableSchema, rules: []const Rule) !ChildPlan {
        if (table_name.len == 0 or table.storage_mode != .relational or rules.len == 0 or rules.len > 256) return error.InvalidSchemaValidationJob;
        const arena = try a.create(std.heap.ArenaAllocator);
        errdefer a.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = try a.create(std.heap.ArenaAllocator);
        errdefer a.destroy(scratch);
        scratch.* = std.heap.ArenaAllocator.init(a);
        errdefer scratch.deinit();
        var view = registry.SchemaView{ .epoch = try registry.Epoch.createCloned(a, table) };
        errdefer view.release();
        const owned = arena.allocator();
        const bound = try owned.alloc(Bound, rules.len);
        var names_set: std.StringArrayHashMapUnmanaged(void) = .empty;
        var columns: usize = 0;
        var name_bytes: usize = table_name.len;
        if (name_bytes > 1024 * 1024) return error.InvalidSchemaValidationJob;
        for (rules, bound, 0..) |rule, *out, i| {
            columns = std.math.add(usize, columns, rule.columns.len) catch return error.InvalidSchemaValidationJob;
            if (rule.name.len == 0 or rule.columns.len == 0 or columns > 4096 or
                rule.match == .partial or std.mem.allEqual(u8, &rule.generation, 0) or
                std.mem.allEqual(u8, &rule.parent_generation, 0)) return error.InvalidSchemaValidationJob;
            for (rules[0..i]) |prior| if (std.mem.eql(u8, rule.name, prior.name)) return error.InvalidSchemaValidationJob;
            name_bytes = std.math.add(usize, name_bytes, rule.name.len) catch return error.InvalidSchemaValidationJob;
            if (name_bytes > 1024 * 1024 or std.mem.indexOfScalar(u8, rule.name, 0) != null) return error.InvalidSchemaValidationJob;
            var copy = rule;
            copy.name = try owned.dupe(u8, rule.name);
            const names = try owned.alloc([]const u8, rule.columns.len);
            const columns_keys = try owned.alloc(native.RelationalIndexKey, rule.columns.len);
            for (rule.columns, names, columns_keys) |name, *target, *key| {
                name_bytes = std.math.add(usize, name_bytes, name.len) catch return error.InvalidSchemaValidationJob;
                if (name.len == 0 or name_bytes > 1024 * 1024 or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidSchemaValidationJob;
                target.* = try owned.dupe(u8, name);
                key.* = .{ .column = target.* };
                try names_set.put(owned, target.*, {});
            }
            copy.columns = names;
            out.* = .{ .rule = copy, .tuple = try keys.TuplePlan.init(owned, view.tableSchema().*, view.physicalLayout(), columns_keys) };
        }
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/fk-validation-child-projection/v1");
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(state, &.{});
        try std.json.Stringify.value(.{ .table_name = table_name, .schema = table, .rules = rules }, .{}, &writer.writer);
        var identity: [32]u8 = undefined;
        writer.hasher.final(&identity);
        return .{ .alloc = a, .arena = arena, .scratch = scratch, .view = view, .table_name = try owned.dupe(u8, table_name), .rules = bound, .names = names_set.keys(), .identity = identity };
    }
    pub fn deinit(self: *ChildPlan) void {
        self.tuple_bytes.deinit(self.alloc);
        self.view.release();
        self.scratch.deinit();
        self.alloc.destroy(self.scratch);
        self.arena.deinit();
        self.alloc.destroy(self.arena);
        self.* = undefined;
    }

    /// Failure leaves the caller's accumulated witness unchanged. Only FK
    /// columns are decoded; all-null keys and SIMPLE partial-null keys are
    /// exempt, whereas FULL partial-null keys violate the constraint.
    pub fn addRow(self: *ChildPlan, row_key: []const u8, row: codec.OrdinalRowView, witness: *Witness) !void {
        if (try self.inspectRow(row_key, row, witness, .{}) != null) return error.ForeignKeyMatchFullViolation;
    }

    /// A violation is the selected rule ordinal. The owner can persist that
    /// bounded diagnostic without retaining or publishing the offending row.
    pub fn inspectRow(self: *ChildPlan, row_key: []const u8, row: codec.OrdinalRowView, witness: *Witness, control: Control) !?usize {
        try control.ensureActive();
        _ = self.scratch.reset(.retain_capacity);
        const scratch = self.scratch.allocator();
        var byte_budget: usize = expressions.max_allocated_bytes;
        var execution = expressions.Execution.init(scratch, &byte_budget);
        var delta: Witness = .{};
        for (self.rules, 0..) |bound, rule_index| {
            try control.ensureActive();
            var nulls: usize = 0;
            for (bound.rule.columns) |name| {
                const ordinal = row.ordinalForName(name) orelse {
                    nulls += 1;
                    continue;
                };
                if (try row.isSqlNull(ordinal)) nulls += 1;
            }
            if (nulls != 0) {
                if (bound.rule.match == .full and nulls != bound.rule.columns.len) return rule_index;
                continue;
            }
            var projected = if (row.layout == bound.tuple.layout) bound.tuple else try bound.tuple.projectSource(scratch, row.table_schema, row.layout);
            defer if (row.layout != bound.tuple.layout) projected.deinit();
            self.tuple_bytes.clearRetainingCapacity();
            if (try projected.appendWithExecution(self.alloc, &self.tuple_bytes, row, &execution)) return error.RelationalRowSchemaMismatch;
            const address = try contract.Address.init(bound.rule.parent_generation, self.tuple_bytes.items);
            const reference: contract.Reference = .{ .child_table = self.table_name, .child_key = row_key, .constraint_name = bound.rule.name, .constraint_generation = bound.rule.generation };
            const key = try reference.key(address);
            try delta.addReference(&key);
        }
        try control.ensureActive();
        try witness.combine(delta);
        return null;
    }
};

/// Immutable target-side scope. Neither AFR2 nor a caller-supplied table name
/// proves a parent exists. The owner must authenticate the schema/generation
/// cohort and freeze every child source before using these read-side facts.
pub const ParentRule = struct {
    child_table: []const u8,
    constraint_name: []const u8,
    constraint_generation: contract.Generation,
    parent_table: []const u8,
    parent_generation: contract.Generation,
};

pub const ParentPlan = struct {
    arena: std.heap.ArenaAllocator,
    rules: []const ParentRule,

    /// Owns only fixed key identities and a rule ordinal, so subsequent point
    /// reads may invalidate all cursor buffers without corrupting the witness.
    pub const Probe = struct {
        address: contract.Address,
        identity: [32]u8,
        rule: usize,
    };

    pub fn init(a: A, rules: []const ParentRule) !ParentPlan {
        if (rules.len == 0 or rules.len > 256) return error.InvalidSchemaValidationJob;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const copy = try owned.alloc(ParentRule, rules.len);
        var name_bytes: usize = 0;
        for (rules, copy, 0..) |rule, *out, i| {
            if (std.mem.allEqual(u8, &rule.constraint_generation, 0) or std.mem.allEqual(u8, &rule.parent_generation, 0)) return error.InvalidSchemaValidationJob;
            for (rules[0..i]) |prior| if (std.mem.eql(u8, &rule.constraint_generation, &prior.constraint_generation)) return error.InvalidSchemaValidationJob;
            out.* = rule;
            inline for (.{ "child_table", "constraint_name", "parent_table" }) |field| {
                const name = @field(rule, field);
                name_bytes = std.math.add(usize, name_bytes, name.len) catch return error.InvalidSchemaValidationJob;
                if (name.len == 0 or name_bytes > 1024 * 1024 or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidSchemaValidationJob;
                @field(out, field) = try owned.dupe(u8, name);
            }
        }
        return .{ .arena = arena, .rules = copy };
    }
    pub fn deinit(self: *ParentPlan) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn probe(self: *const ParentPlan, key: []const u8, value: []const u8) !?Probe {
        const parsed = try contract.parseKey(key);
        if (parsed.kind != .reference) return error.InvalidSchemaValidationJob;
        const reference = try contract.Reference.decode(key, value);
        for (self.rules, 0..) |rule, i| {
            if (!std.mem.eql(u8, &reference.constraint_generation, &rule.constraint_generation)) continue;
            // A generation match with different semantics is corruption, not
            // an unrelated reference that can safely be omitted from coverage.
            if (!std.mem.eql(u8, reference.child_table, rule.child_table) or
                !std.mem.eql(u8, reference.constraint_name, rule.constraint_name) or
                !std.mem.eql(u8, &parsed.address.generation, &rule.parent_generation)) return error.InvalidSchemaValidationJob;
            return .{ .address = parsed.address, .identity = try Witness.identity(key), .rule = i };
        }
        return null;
    }
    pub fn inspectParent(self: *const ParentPlan, selected: Probe, value: ?[]const u8, witness: *Witness) !void {
        if (selected.rule >= self.rules.len) return error.InvalidSchemaValidationJob;
        const rule = self.rules[selected.rule];
        if (!std.mem.eql(u8, &selected.address.generation, &rule.parent_generation)) return error.InvalidSchemaValidationJob;
        const parent = try contract.Claim.decode(&selected.address.claimKey(), value orelse return error.ForeignKeyViolation);
        if (!std.mem.eql(u8, parent.parent_table, rule.parent_table)) return error.InvalidSchemaValidationJob;
        // A draining claim still owns its visible parent. Frozen child sources
        // and their references prevent the destructive action from finishing.
        try witness.addIdentity(selected.identity);
    }
};

test "foreign key validation witnesses combine disjoint owners and detect duplicate pairs" {
    const first = try contract.Address.init(@splat(1), "first");
    const second = try contract.Address.init(@splat(1), "second");
    const reference: contract.Reference = .{ .child_table = "child", .child_key = "a", .constraint_name = "fk", .constraint_generation = @splat(2) };
    const a = try reference.key(first);
    const b = try reference.key(second);
    var together: Witness = .{};
    try together.addReference(&a);
    try together.addReference(&b);
    var reverse: Witness = .{};
    try reverse.addReference(&b);
    var part: Witness = .{};
    try part.addReference(&a);
    try reverse.combine(part);
    try std.testing.expect(together.eql(reverse));
    var duplicate_a: Witness = .{};
    try duplicate_a.addReference(&a);
    try duplicate_a.addReference(&a);
    var duplicate_b: Witness = .{};
    try duplicate_b.addReference(&b);
    try duplicate_b.addReference(&b);
    try std.testing.expect(!duplicate_a.eql(duplicate_b));
    const before = together;
    try std.testing.expectError(error.InvalidSchemaValidationJob, together.combine(.{ .count = std.math.maxInt(u64), .sum = @splat(1) }));
    try std.testing.expect(before.eql(together));
}

test "foreign key validation typed child witness matches the actual write-side reference identity" {
    const a = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "tenant", .path = "tenant", .column_type = .string },
        .{ .name = "id", .path = "id", .column_type = .integer },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var view = registry.SchemaView{ .epoch = try registry.Epoch.createCloned(a, table) };
    defer view.release();
    const bytes = try codec.serializeOrdinal(a, 1, &columns, &.{
        .{ .ordinal = 0, .path = "tenant", .value_type = .bytes_val, .value = .{ .bytes_val = "Acme" } },
        .{ .ordinal = 1, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = 9_007_199_254_740_993 } },
    }, @splat(0));
    defer a.free(bytes);
    const row = try codec.ordinalRowViewSelective(bytes, view.tableSchema().*, view.physicalLayout());
    const fk: native.ForeignKey = .{ .name = "fk", .child_columns = &.{ "tenant", "id" }, .parent_table = "parent", .parent_columns = &.{ "tenant", "id" }, .match = .full, .not_valid = true };
    const writer = @import("../../api/relational_integrity.zig");
    var write_plan = try writer.Plan.init(a, "child", view, &.{}, &.{.{ .generation = @splat(2), .parent_generation = @splat(1), .definition = fk, .parent = view, .parent_unique = .{ .name = "parent_key", .columns = &.{ "tenant", "id" } } }});
    defer write_plan.deinit();
    var written = try write_plan.expandHistorical(a, &.{.{ .key = "row", .after = row }});
    defer written.deinit();
    var actual: Witness = .{};
    const command = written.commands[0].command;
    try actual.addReference(&try command.operation.attach_historical.key(command.address));
    var plan = try ChildPlan.init(a, "child", table, &.{.{ .name = "fk", .columns = &.{ "tenant", "id" }, .generation = @splat(2), .parent_generation = @splat(1), .match = .full }});
    defer plan.deinit();
    var expected: Witness = .{};
    try plan.addRow("row", row, &expected);
    try std.testing.expect(actual.eql(expected));
}

test "foreign key validation MATCH FULL failure cannot partially advance a source witness" {
    const a = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "a", .path = "a", .column_type = .integer, .allows_null = true },
        .{ .name = "b", .path = "b", .column_type = .integer, .allows_null = true },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var plan = try ChildPlan.init(a, "child", table, &.{
        .{ .name = "simple", .columns = &.{"a"}, .generation = @splat(2), .parent_generation = @splat(1) },
        .{ .name = "full", .columns = &.{ "a", "b" }, .generation = @splat(4), .parent_generation = @splat(3), .match = .full },
    });
    defer plan.deinit();
    var view = registry.SchemaView{ .epoch = try registry.Epoch.createCloned(a, table) };
    defer view.release();
    const bytes = try codec.serializeOrdinal(a, 1, &columns, &.{.{ .ordinal = 0, .path = "a", .value_type = .i64_val, .value = .{ .i64_val = 1 } }}, @splat(0));
    defer a.free(bytes);
    const row = try codec.ordinalRowViewSelective(bytes, table, view.physicalLayout());
    var witness: Witness = .{};
    try std.testing.expectError(error.ForeignKeyMatchFullViolation, plan.addRow("row", row, &witness));
    try std.testing.expect(witness.eql(.{}));
}

test "foreign key validation parent probes own identities across borrowed buffer invalidation and AFR upgrades" {
    const a = std.testing.allocator;
    var plan = try ParentPlan.init(a, &.{.{ .child_table = "child", .constraint_name = "fk", .constraint_generation = @splat(2), .parent_table = "parent", .parent_generation = @splat(1) }});
    defer plan.deinit();
    const address = try contract.Address.init(@splat(1), "tuple");
    const reference: contract.Reference = .{ .child_table = "child", .child_key = "row", .constraint_name = "fk", .constraint_generation = @splat(2), .historical_unvalidated = true };
    var key = try reference.key(address);
    const bytes = try reference.encode(a, address);
    defer a.free(bytes);
    const selected = (try plan.probe(&key, bytes)).?;
    const claim: contract.Claim = .{ .tuple = "tuple", .parent_table = "parent", .parent_key = "owner", .schema_version = 1 };
    const parent = try claim.encode(a, address);
    defer a.free(parent);
    var expected: Witness = .{};
    try expected.addReference(&key);
    @memset(&key, 0);
    @memset(bytes, 0);
    var actual: Witness = .{};
    try plan.inspectParent(selected, parent, &actual);
    try std.testing.expect(expected.eql(actual));
    const before = actual;
    try std.testing.expectError(error.ForeignKeyViolation, plan.inspectParent(selected, null, &actual));
    try std.testing.expect(before.eql(actual));
    var ordinary = reference;
    ordinary.historical_unvalidated = false;
    const strong = try ordinary.encode(a, address);
    defer a.free(strong);
    const upgraded = (try plan.probe(&try ordinary.key(address), strong)).?;
    try std.testing.expectEqualDeep(selected, upgraded);
    var wrong = claim;
    wrong.parent_table = "other";
    const wrong_bytes = try wrong.encode(a, address);
    defer a.free(wrong_bytes);
    try std.testing.expectError(error.InvalidSchemaValidationJob, plan.inspectParent(selected, wrong_bytes, &actual));
    try std.testing.expect(before.eql(actual));
    var draining = claim;
    draining.state = .draining;
    draining.action = .parent_delete;
    draining.action_id = @splat(3);
    const draining_bytes = try draining.encode(a, address);
    defer a.free(draining_bytes);
    var draining_witness: Witness = .{};
    try plan.inspectParent(selected, draining_bytes, &draining_witness);
    try std.testing.expect(expected.eql(draining_witness));
}

test "foreign key validation parent scopes reject corrupt generation aliasing and ignore unrelated references" {
    const a = std.testing.allocator;
    var plan = try ParentPlan.init(a, &.{.{ .child_table = "child", .constraint_name = "fk", .constraint_generation = @splat(2), .parent_table = "parent", .parent_generation = @splat(1) }});
    defer plan.deinit();
    const address = try contract.Address.init(@splat(1), "tuple");
    const reference: contract.Reference = .{ .child_table = "child", .child_key = "row", .constraint_name = "fk", .constraint_generation = @splat(2) };
    var unrelated = reference;
    unrelated.constraint_generation = @splat(3);
    const unrelated_bytes = try unrelated.encode(a, address);
    defer a.free(unrelated_bytes);
    try std.testing.expect(try plan.probe(&try unrelated.key(address), unrelated_bytes) == null);
    var alias = reference;
    alias.child_table = "other";
    const alias_bytes = try alias.encode(a, address);
    defer a.free(alias_bytes);
    try std.testing.expectError(error.InvalidSchemaValidationJob, plan.probe(&try alias.key(address), alias_bytes));
    const wrong_address = try contract.Address.init(@splat(4), "tuple");
    const wrong_generation = try reference.encode(a, wrong_address);
    defer a.free(wrong_generation);
    try std.testing.expectError(error.InvalidSchemaValidationJob, plan.probe(&try reference.key(wrong_address), wrong_generation));
}

fn allocationPlanCase(a: A) !void {
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "id", .path = "id", .column_type = .integer }} };
    var child = try ChildPlan.init(a, "child", table, &.{.{ .name = "fk", .columns = &.{"id"}, .generation = @splat(2), .parent_generation = @splat(1) }});
    defer child.deinit();
    var parent = try ParentPlan.init(a, &.{.{ .child_table = "child", .constraint_name = "fk", .constraint_generation = @splat(2), .parent_table = "parent", .parent_generation = @splat(1) }});
    defer parent.deinit();
}
test "foreign key validation source and parent plan allocation failures release all immutable epochs" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, allocationPlanCase, .{});
}

pub const ParentBudget = struct {
    records: u32 = 1024,
    time_ns: u64 = 5 * std.time.ns_per_ms,

    fn validate(self: @This()) !void {
        if (self.records == 0 or self.records > 65536 or self.time_ns == 0 or self.time_ns > std.time.ns_per_s) return error.InvalidSchemaValidationJob;
    }
};

/// Fixed-size, read-side target progress. A cursor is an exclusive physical
/// boundary, never permission to skip source coverage or to publish a schema.
/// The owner must join it with its authenticated cohort and durable predecessor.
pub const ParentPage = struct {
    witness: Witness = .{},
    records_examined: u32 = 0,
    more: bool = false,
    failure_rule: ?u16 = null,
    last_key: [contract.key_len + 32]u8 = @splat(0),
    has_cursor: bool = false,

    pub fn after(self: *const @This()) []const u8 {
        return if (self.has_cursor) &self.last_key else "";
    }
    pub fn validate(self: *const @This()) !void {
        try self.witness.validate();
        if (self.records_examined > 65536 or self.witness.count > self.records_examined or
            (self.more and self.records_examined == 0) or
            (self.records_examined != 0 and !self.has_cursor) or
            (!self.has_cursor and !std.mem.allEqual(u8, &self.last_key, 0))) return error.InvalidSchemaValidationJob;
        if (self.has_cursor and (try contract.parseKey(&self.last_key)).kind != .reference) return error.InvalidSchemaValidationJob;
    }
};

/// Iterate only the private reference namespace. Primary rows, claim records
/// and action jobs are never decoded as references. Point probes share the
/// caller's pinned read cut. No page, reference or row-sized arena is allocated.
pub fn readParentPage(plan: *const ParentPlan, read: anytype, after: []const u8, budget: ParentBudget, control: Control) !ParentPage {
    return readParentPageMode(plan, read, after, budget, control, null);
}

/// Re-execution compares the independently read prefix, including the actual
/// parent claim and witness. Transport assertions cannot advance an owner cut.
pub fn replayParentPage(plan: *const ParentPlan, read: anytype, after: []const u8, expected: *const ParentPage, control: Control) !ParentPage {
    try expected.validate();
    if (expected.failure_rule) |rule| if (rule >= plan.rules.len) return error.InvalidSchemaValidationJob;
    return readParentPageMode(plan, read, after, .{ .records = 65536, .time_ns = 1 }, control, expected);
}

fn readParentPageMode(plan: *const ParentPlan, read: anytype, after: []const u8, budget: ParentBudget, control: Control, expected: ?*const ParentPage) !ParentPage {
    try control.ensureActive();
    try budget.validate();
    if (after.len != 0 and (try contract.parseKey(after)).kind != .reference) return error.InvalidSchemaValidationJob;
    var result: ParentPage = .{};
    if (after.len != 0) {
        if (after.len != result.last_key.len) return error.InvalidSchemaValidationJob;
        @memcpy(&result.last_key, after);
        result.has_cursor = true;
    }
    var cursor = try read.openCursor();
    defer cursor.close();
    cursor.setUpperBound(contract.namespace ++ "\x03");
    var entry = try cursor.seekAtOrAfter(if (after.len != 0) after else contract.namespace ++ "\x02");
    if (entry) |kv| if (std.mem.eql(u8, kv.key, after)) {
        entry = try cursor.next();
    };
    const started = if (expected == null) @import("antfly_platform").time.monotonicNs() else 0;
    while (entry) |kv| : (entry = try cursor.next()) {
        try control.ensureActive();
        if (!std.mem.startsWith(u8, kv.key, contract.namespace ++ "\x02")) break;
        if (kv.key.len != result.last_key.len) return error.InvalidSchemaValidationJob;
        // Capture both the continuation and hash before a point probe may
        // overwrite the cursor's borrowed key/value backing storage.
        @memcpy(&result.last_key, kv.key);
        result.has_cursor = true;
        result.records_examined += 1;
        if (try plan.probe(&result.last_key, kv.value)) |selected| {
            const parent: ?[]const u8 = read.get(&selected.address.claimKey()) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            plan.inspectParent(selected, parent, &result.witness) catch |err| switch (err) {
                error.ForeignKeyViolation => {
                    result.failure_rule = @intCast(selected.rule);
                },
                else => return err,
            };
        }
        const stop = if (expected) |boundary| result.failure_rule != null or
            (boundary.more and result.records_examined >= boundary.records_examined) else result.failure_rule != null or result.records_examined >= budget.records or
            @import("antfly_platform").time.monotonicNs() - started >= budget.time_ns;
        if (stop) {
            result.more = true;
            break;
        }
        if (result.records_examined == 65536) return error.InvalidSchemaValidationJob;
    }
    try control.ensureActive();
    try result.validate();
    if (expected) |boundary| if (!std.meta.eql(result, boundary.*)) return error.InvalidSchemaValidationJob;
    return result;
}

test "foreign key validation parent pages bound cold LSM work and reject forged or orphan coverage" {
    const a = std.testing.allocator;
    const DocStore = @import("../docstore.zig").DocStore;
    const lsm = @import("../lsm_backend.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("fk-validation-parent");
    defer directory.cleanup();
    var plan = try ParentPlan.init(a, &.{.{ .child_table = "child", .constraint_name = "fk", .constraint_generation = @splat(2), .parent_table = "parent", .parent_generation = @splat(1) }});
    defer plan.deinit();
    const address = try contract.Address.init(@splat(1), "tuple");
    for (0..2) |epoch| {
        var backend = try lsm.Backend.open(a, directory.path(), .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        if (epoch == 0) {
            const parent = try (contract.Claim{ .tuple = "tuple", .parent_table = "parent", .parent_key = "p", .schema_version = 1 }).encode(a, address);
            defer a.free(parent);
            try store.put(&address.claimKey(), parent);
            for ([_][]const u8{ "a", "b", "other" }, 0..) |key, i| {
                const reference: contract.Reference = .{ .child_table = "child", .child_key = key, .constraint_name = "fk", .constraint_generation = @splat(if (i == 2) @as(u8, 3) else 2), .historical_unvalidated = true };
                const value = try reference.encode(a, address);
                defer a.free(value);
                try store.put(&try reference.key(address), value);
            }
            continue;
        }
        var read = try store.beginReadTxn();
        defer read.abort();
        var current: ParentPage = .{};
        var combined: Witness = .{};
        var pages: usize = 0;
        while (pages < 8) : (pages += 1) {
            const page = try readParentPage(&plan, &read, current.after(), .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
            try std.testing.expect(page.records_examined <= 1);
            var forged = page;
            forged.witness.count += 1;
            forged.witness.sum[0] +%= 1;
            try std.testing.expectError(error.InvalidSchemaValidationJob, replayParentPage(&plan, &read, current.after(), &forged, .{}));
            const replayed = try replayParentPage(&plan, &read, current.after(), &page, .{});
            try std.testing.expectEqualDeep(page, replayed);
            try combined.combine(page.witness);
            current = page;
            if (!page.more) break;
        }
        try std.testing.expect(pages < 8);
        try std.testing.expectEqual(@as(u64, 2), combined.count);
        const whole = try readParentPage(&plan, &read, "", .{ .time_ns = std.time.ns_per_s }, .{});
        try std.testing.expect(!whole.more and whole.witness.eql(combined));
        // The retained snapshot still has its parent; a fresh cut must reject
        // the same AFR2 references after the parent is removed.
        try store.delete(&address.claimKey());
        var fresh = try store.beginReadTxn();
        defer fresh.abort();
        const failed = try readParentPage(&plan, &fresh, "", .{ .time_ns = std.time.ns_per_s }, .{});
        try std.testing.expectEqual(@as(?u16, 0), failed.failure_rule);
        try std.testing.expect(failed.more);
        try std.testing.expectError(error.InvalidSchemaValidationJob, replayParentPage(&plan, &fresh, "", &whole, .{}));
        const retained = try readParentPage(&plan, &read, "", .{ .time_ns = std.time.ns_per_s }, .{});
        try std.testing.expect(whole.witness.eql(retained.witness));
    }
}

test "foreign key validation repeated historical rows retain scratch without heap allocation" {
    const a = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{.{ .name = "id", .path = "id", .column_type = .integer }};
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var source = registry.SchemaView{ .epoch = try registry.Epoch.createCloned(a, table) };
    defer source.release();
    const bytes = try codec.serializeOrdinal(a, 1, &columns, &.{.{ .ordinal = 0, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = 123 } }}, @splat(0));
    defer a.free(bytes);
    const row = try codec.ordinalRowViewSelective(bytes, source.tableSchema().*, source.physicalLayout());
    var failure = std.testing.FailingAllocator.init(a, .{});
    var plan = try ChildPlan.init(failure.allocator(), "child", table, &.{.{ .name = "fk", .columns = &.{"id"}, .generation = @splat(2), .parent_generation = @splat(1) }});
    defer plan.deinit();
    var witness: Witness = .{};
    try plan.addRow("row", row, &witness);
    failure.fail_index = failure.alloc_index;
    for (0..10000) |_| try plan.addRow("row", row, &witness);
    try std.testing.expectEqual(@as(u64, 10001), witness.count);
    try std.testing.expect(!failure.has_induced_failure);
}
