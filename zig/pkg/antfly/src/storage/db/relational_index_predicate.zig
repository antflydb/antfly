// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
    hash.update(&.{ @intFromEnum(plan.op), @intFromBool(plan.operand_null) });
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

    pub fn init(alloc: Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout, definitions: []const native.UniquePredicate) !Plan {
        if (definitions.len == 0 or definitions.len > max_conditions) return error.InvalidRelationalPredicate;
        const conditions = try alloc.alloc(predicate.Plan, definitions.len);
        var initialized: usize = 0;
        errdefer {
            for (conditions[0..initialized]) |*condition| condition.deinit();
            alloc.free(conditions);
        }
        const identities = try alloc.alloc([32]u8, definitions.len);
        errdefer alloc.free(identities);
        for (definitions, conditions, identities) |definition, *condition, *identity| {
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
        const compiled_implication = try implication.Compiled.init(alloc, conditions);
        errdefer compiled_implication.deinit(alloc);
        var result: Plan = .{ .alloc = alloc, .conditions = conditions, .condition_identities = identities, .implication = compiled_implication, .identity = undefined, .active = undefined };
        hash.final(&result.identity);
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
    pub fn deinit(self: *Source) void {
        for (self.conditions) |*condition| condition.deinit();
        self.alloc.free(self.conditions);
        self.* = undefined;
    }
    pub fn matches(self: *const Source, alloc: Allocator, scratch: *std.ArrayList(u8), row: codec.OrdinalRowView) !bool {
        for (self.conditions) |condition| if (!(try condition.evaluate(alloc, scratch, row)).matches()) return false;
        return true;
    }
};
