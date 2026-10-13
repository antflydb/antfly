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

//! Immutable conjunctive partial-index membership. Logical comparison identity
//! is shared by writes, cold historical scans, and conservative query proofs.
const std = @import("std");
const predicate = @import("relational_predicate.zig");
const implication = @import("relational_predicate_implication.zig");
const native = @import("../relational_index.zig");
const schema = @import("../schema.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const Allocator = std.mem.Allocator;
pub const max_conditions = implication.max_conditions;

fn identityLess(_: void, a: [32]u8, b: [32]u8) bool {
    return std.mem.order(u8, &a, &b) == .lt;
}

pub const ImplicationWork = struct {
    query_hashes: usize = 0,
    operand_bytes_hashed: usize = 0,
    comparisons: usize = 0,
    semantic_domains: usize = 0,
};

pub fn conditionIdentity(plan: predicate.Plan) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly partial index conjunct v1");
    hash.update(&plan.tuple.fingerprint);
    hash.update(&.{ @backingInt(plan.op), @intFromBool(plan.operand_null) });
    hash.update(plan.operand);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub const Plan = struct {
    alloc: Allocator,
    conditions: []predicate.Plan,
    /// Sorted once at compilation. Predicate operands can be large; proofs
    /// must never rehash them once per query conjunct.
    condition_identities: [][32]u8,
    implication: implication.Compiled,
    identity: [32]u8,
    active: Source,
    always_false: bool = false,

    pub fn init(alloc: Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout, definitions: []const native.UniquePredicate) !Plan {
        if (definitions.len == 0 or definitions.len > max_conditions) return error.InvalidRelationalPredicate;
        var count: usize = 0;
        var always_false = false;
        for (definitions) |definition| {
            if (definition.constant) |value| {
                if (definition.field.len != 0 or definition.op != .eq or definition.value_json != null or definition.collation != null)
                    return error.InvalidRelationalPredicate;
                always_false = always_false or !value;
            } else count += 1;
        }
        const conditions = try alloc.alloc(predicate.Plan, count);
        var initialized: usize = 0;
        errdefer {
            for (conditions[0..initialized]) |*condition| condition.deinit();
            alloc.free(conditions);
        }
        const identities = try alloc.alloc([32]u8, count);
        errdefer alloc.free(identities);
        for (definitions) |definition| {
            if (definition.constant != null) continue;
            const condition = &conditions[initialized];
            const identity = &identities[initialized];
            const ordinal = layout.ordinalForName(table.relational_columns, definition.field) orelse return error.RelationalIndexColumnNotFound;
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const parsed = if (definition.value_json) |json| try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), json, .{ .parse_numbers = false }) else .null;
            const value = try @import("../../schema/relational_checks.zig").valueFromJson(arena.allocator(), table.relational_columns[ordinal].column_type, parsed, true);
            condition.* = try predicate.Plan.init(alloc, table, layout, .{ .column = definition.field, .op = definition.op, .value = value, .collation = definition.collation });
            initialized += 1;
            identity.* = conditionIdentity(condition.*);
        }
        std.mem.sort([32]u8, identities, {}, identityLess);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly partial index conjunction v1");
        for (identities, 0..) |identity, i| {
            if (i != 0 and std.mem.eql(u8, &identity, &identities[i - 1])) continue;
            hash.update(&identity);
        }
        var compiled_implication = try implication.Compiled.init(alloc, conditions);
        errdefer compiled_implication.deinit(alloc);
        compiled_implication.impossible = compiled_implication.impossible or always_false;
        var result: Plan = .{ .alloc = alloc, .conditions = conditions, .condition_identities = identities, .implication = compiled_implication, .identity = undefined, .active = undefined, .always_false = always_false };
        if (always_false) {
            std.crypto.hash.Blake3.hash("antfly partial index constant false v1", &result.identity, .{});
        } else if (count == 0) {
            result.identity = @splat(0);
        } else hash.final(&result.identity);
        result.active = try result.projectSource(alloc, table, layout);
        return result;
    }

    pub fn deinit(self: *Plan) void {
        self.active.deinit();
        self.implication.deinit(self.alloc);
        for (self.conditions) |*condition| condition.deinit();
        self.alloc.free(self.conditions);
        self.alloc.free(self.condition_identities);
        self.* = undefined;
    }

    pub fn bindFingerprint(self: Plan, fingerprint: *[32]u8) void {
        if (self.conditions.len == 0 and !self.always_false) return;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(fingerprint);
        hash.update(&self.identity);
        hash.final(fingerprint);
    }

    /// Prove typed WHERE implication using exact containment first, then
    /// canonical per-column intervals, exclusions and SQL NULL truth. No
    /// textual coercion, arbitrary expression algebra, or scan-bound guesses.
    pub fn impliedBy(self: Plan, query: []const predicate.Plan) bool {
        return self.impliedByWithWork(query, null);
    }

    /// Bounded scratch, one operand-hashing pass, then O(N log N) fixed-width
    /// sorting and a linear containment merge. Duplicate conjuncts add no
    /// requirements, and no per-request heap allocation is needed for proofs.
    pub fn impliedByWithWork(self: Plan, query: []const predicate.Plan, work: ?*ImplicationWork) bool {
        return self.prove(query, null, work);
    }

    /// Mark query conjuncts already certified by ready index membership.
    /// Other conditions remain residual filters; masks are usable only after
    /// the complete implication proof succeeds.
    pub fn impliedByAndMark(self: Plan, query: []const predicate.Plan, proven: []bool) bool {
        if (proven.len != query.len) return false;
        return self.prove(query, proven, null);
    }

    fn containsIdentity(self: Plan, identity: [32]u8) bool {
        var start: usize = 0;
        var end = self.condition_identities.len;
        while (start < end) {
            const middle = start + (end - start) / 2;
            switch (std.mem.order(u8, &self.condition_identities[middle], &identity)) {
                .lt => start = middle + 1,
                .gt => end = middle,
                .eq => return true,
            }
        }
        return false;
    }

    fn prove(self: Plan, query: []const predicate.Plan, proven: ?[]bool, work: ?*ImplicationWork) bool {
        if (query.len > max_conditions) return false;
        if (self.always_false) {
            // An empty index can answer only a provably empty query. Merely
            // having no required column conjuncts must never authorize it.
            const proof = implication.Proof.init(query);
            if (work) |counts| counts.semantic_domains += proof.domain_count;
            if (!proof.impossible) return false;
            if (proven) |mask| @memset(mask, true);
            return true;
        }
        var scratch: [max_conditions][32]u8 = undefined;
        const identities = scratch[0..query.len];
        for (query, identities, 0..) |condition, *identity, i| {
            identity.* = conditionIdentity(condition);
            if (proven) |mask| mask[i] = self.containsIdentity(identity.*);
            if (work) |counts| {
                counts.query_hashes += 1;
                counts.operand_bytes_hashed +|= condition.operand.len;
            }
        }
        std.mem.sort([32]u8, identities, {}, identityLess);
        if (!self.containsAll(identities, work)) {
            const proof = implication.Proof.init(query);
            if (work) |counts| counts.semantic_domains += proof.domain_count;
            for (self.conditions) |required| if (!proof.implies(required)) return false;
        }
        // Query => index permits choosing the index. Only the reverse proof
        // index => query permits dropping a residual filter, including when
        // the query range is stricter than index membership.
        if (proven) |mask| for (query, mask) |condition, *covered| {
            if (!covered.*) covered.* = self.implication.implies(condition);
        };
        return true;
    }

    fn containsAll(self: Plan, identities: []const [32]u8, work: ?*ImplicationWork) bool {
        var position: usize = 0;
        for (self.condition_identities) |required| {
            while (position < identities.len) {
                if (work) |counts| counts.comparisons += 1;
                switch (std.mem.order(u8, &identities[position], &required)) {
                    .lt => position += 1,
                    .eq => break,
                    .gt => return false,
                }
            }
            if (position == identities.len) return false;
        }
        return true;
    }

    pub fn projectSource(self: *const Plan, alloc: Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout) !Source {
        if (self.always_false) return .{ .alloc = alloc, .conditions = try alloc.alloc(predicate.Source, 0), .always_false = true };
        const conditions = try alloc.alloc(predicate.Source, self.conditions.len);
        var initialized: usize = 0;
        errdefer {
            for (conditions[0..initialized]) |*condition| condition.deinit();
            alloc.free(conditions);
        }
        for (self.conditions, conditions) |*condition, *source| {
            source.* = try condition.projectSource(alloc, table, layout);
            initialized += 1;
        }
        return .{ .alloc = alloc, .conditions = conditions };
    }
};

pub const Source = struct {
    alloc: Allocator,
    conditions: []predicate.Source,
    always_false: bool = false,
    pub fn deinit(self: *Source) void {
        for (self.conditions) |*condition| condition.deinit();
        self.alloc.free(self.conditions);
        self.* = undefined;
    }
    pub fn matches(self: *const Source, alloc: Allocator, scratch: *std.ArrayList(u8), row: codec.OrdinalRowView) !bool {
        if (self.always_false) return false;
        for (self.conditions) |condition| if (!(try condition.evaluate(alloc, scratch, row)).matches()) return false;
        return true;
    }
};

test "relational index system SQL immutable index constant membership preserves empty-index implication and avoids row decoding" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer, .allows_null = true }} };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var empty = try Plan.init(a, table, &layout, &.{.{ .constant = false }});
    defer empty.deinit();
    var full = try Plan.init(a, table, &layout, &.{.{ .constant = true }});
    defer full.deinit();
    var lower = try predicate.Plan.init(a, table, &layout, .{ .column = "n", .op = .gt, .value = .{ .integer = 0 } });
    defer lower.deinit();
    var upper = try predicate.Plan.init(a, table, &layout, .{ .column = "n", .op = .lte, .value = .{ .integer = 0 } });
    defer upper.deinit();
    try std.testing.expect(!empty.impliedBy(&.{}));
    try std.testing.expect(!empty.impliedBy(&.{lower}));
    var proven = [_]bool{ false, false };
    try std.testing.expect(empty.impliedByAndMark(&.{ lower, upper }, &proven));
    try std.testing.expectEqualSlices(bool, &.{ true, true }, &proven);
    try std.testing.expect(full.impliedBy(&.{lower}));
    proven[0] = true;
    try std.testing.expect(full.impliedByAndMark(&.{lower}, proven[0..1]));
    try std.testing.expect(!proven[0]);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(a);
    // An undefined row is intentional: these answers must not read a layout,
    // decode a cell, allocate a tuple, or invent any column dependency.
    try std.testing.expect(!try empty.active.matches(a, &scratch, undefined));
    try std.testing.expect(try full.active.matches(a, &scratch, undefined));
    try std.testing.expectEqual(@as(usize, 0), scratch.capacity);
    try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(0)), &full.identity);
    var fingerprint: [32]u8 = @splat(7);
    full.bindFingerprint(&fingerprint);
    try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(7)), &fingerprint);
    empty.bindFingerprint(&fingerprint);
    try std.testing.expect(!std.mem.eql(u8, &@as([32]u8, @splat(7)), &fingerprint));
    var normal = try Plan.init(a, table, &layout, &.{.{ .field = "n", .op = .gt, .value_json = "0" }});
    defer normal.deinit();
    var neutral = try Plan.init(a, table, &layout, &.{ .{ .constant = true }, .{ .field = "n", .op = .gt, .value_json = "0" } });
    defer neutral.deinit();
    try std.testing.expectEqualSlices(u8, &normal.identity, &neutral.identity);
    var absorbing = try Plan.init(a, table, &layout, &.{ .{ .constant = false }, .{ .field = "n", .op = .gt, .value_json = "0" } });
    defer absorbing.deinit();
    try std.testing.expectEqualSlices(u8, &empty.identity, &absorbing.identity);
    try std.testing.expectEqual(@as(usize, 0), absorbing.active.conditions.len);
    try std.testing.expectError(error.InvalidRelationalPredicate, Plan.init(a, table, &layout, &.{.{ .constant = false, .field = "n" }}));
    // Binding validation still precedes an absorbing constant.
    try std.testing.expectError(error.RelationalIndexColumnNotFound, Plan.init(a, table, &layout, &.{ .{ .constant = false }, .{ .field = "missing", .op = .is_null } }));
}

test "relational index system SQL immutable index constant predicates retain durable spelling and unwind allocation cuts" {
    const a = std.testing.allocator;
    const original: native.UniquePredicate = .{ .field = "n", .op = .gt, .value_json = "0" };
    const bytes = try std.json.Stringify.valueAlloc(a, original, .{});
    defer a.free(bytes);
    try std.testing.expectEqualStrings("{\"field\":\"n\",\"op\":\"gt\",\"value_json\":\"0\",\"collation\":null}", bytes);
    const encoded = try std.json.Stringify.valueAlloc(a, native.UniquePredicate{ .constant = false }, .{});
    defer a.free(encoded);
    try std.testing.expectEqualStrings("{\"constant\":false}", encoded);
    const decoded = try std.json.parseFromSlice(native.UniquePredicate, a, encoded, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(?bool, false), decoded.value.constant);
    const Faults = struct {
        fn run(alloc: Allocator) !void {
            const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer }} };
            var layout = try codec.PhysicalLayout.init(alloc, table);
            defer layout.deinit();
            var plan = try Plan.init(alloc, table, &layout, &.{ .{ .constant = false }, .{ .field = "n", .op = .gt, .value_json = "0" } });
            defer plan.deinit();
            try std.testing.expect(plan.always_false);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{});
}
