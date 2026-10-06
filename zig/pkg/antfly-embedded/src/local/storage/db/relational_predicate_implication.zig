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

//! Bounded implication for conjunctions in the typed relational predicate
//! language. Domains describe WHERE TRUE, not CHECK's TRUE-or-UNKNOWN. The
//! tuple fingerprint binds column path, type and collation; comparisons use
//! the same canonical ordered operands as row evaluation, never JSON/coercion.
const std = @import("std");
const predicate = @import("relational_predicate.zig");
pub const max_conditions = 256;

const Bound = struct { value: []const u8, inclusive: bool };
const Domain = struct {
    identity: [32]u8,
    lower: ?Bound = null,
    upper: ?Bound = null,
    allows_null: bool = true,
    nonnull: bool = true,
    exclusion_start: usize,
    exclusion_count: usize = 0,

    fn restrictLower(self: *Domain, bound: Bound) void {
        if (self.lower) |old| switch (std.mem.order(u8, bound.value, old.value)) {
            .gt => self.lower = bound,
            .eq => self.lower.?.inclusive = old.inclusive and bound.inclusive,
            .lt => {},
        } else self.lower = bound;
    }

    fn restrictUpper(self: *Domain, bound: Bound) void {
        if (self.upper) |old| switch (std.mem.order(u8, bound.value, old.value)) {
            .lt => self.upper = bound,
            .eq => self.upper.?.inclusive = old.inclusive and bound.inclusive,
            .gt => {},
        } else self.upper = bound;
    }

    fn intersect(self: *Domain, condition: predicate.Plan) void {
        switch (condition.op) {
            .is_null => self.nonnull = false,
            .is_not_null => self.allows_null = false,
            .is_distinct => if (condition.operand_null) {
                self.allows_null = false;
            },
            .is_not_distinct => if (condition.operand_null) {
                self.nonnull = false;
            } else {
                self.allows_null = false;
                self.restrictLower(.{ .value = condition.operand, .inclusive = true });
                self.restrictUpper(.{ .value = condition.operand, .inclusive = true });
            },
            else => {
                self.allows_null = false;
                if (condition.operand_null) {
                    // Ordinary comparisons to NULL are never WHERE TRUE.
                    self.nonnull = false;
                    return;
                }
                switch (condition.op) {
                    .eq => {
                        self.restrictLower(.{ .value = condition.operand, .inclusive = true });
                        self.restrictUpper(.{ .value = condition.operand, .inclusive = true });
                    },
                    .gt, .gte => self.restrictLower(.{ .value = condition.operand, .inclusive = condition.op == .gte }),
                    .lt, .lte => self.restrictUpper(.{ .value = condition.operand, .inclusive = condition.op == .lte }),
                    .ne => {}, // Recorded separately in the sorted exclusions.
                    else => unreachable,
                }
            },
        }
    }

    fn excludes(self: Domain, value: []const u8, exclusions: []const []const u8) bool {
        if (self.lower) |bound| switch (std.mem.order(u8, value, bound.value)) {
            .lt => return true,
            .eq => if (!bound.inclusive) return true,
            .gt => {},
        };
        if (self.upper) |bound| switch (std.mem.order(u8, value, bound.value)) {
            .gt => return true,
            .eq => if (!bound.inclusive) return true,
            .lt => {},
        };
        var start = self.exclusion_start;
        var end = start + self.exclusion_count;
        while (start < end) {
            const middle = start + (end - start) / 2;
            switch (std.mem.order(u8, exclusions[middle], value)) {
                .lt => start = middle + 1,
                .gt => end = middle,
                .eq => return true,
            }
        }
        return false;
    }

    fn implies(self: Domain, required: predicate.Plan, exclusions: []const []const u8) bool {
        if (required.op == .is_null) return !self.nonnull;
        if (required.op == .is_not_null) return !self.allows_null;
        if (required.operand_null) return switch (required.op) {
            .is_distinct => !self.allows_null,
            .is_not_distinct => !self.nonnull,
            else => !self.allows_null and !self.nonnull,
        };
        if (self.allows_null and required.op != .is_distinct) return false;
        if (!self.nonnull) return true;
        return switch (required.op) {
            .eq, .is_not_distinct => blk: {
                const lower = self.lower orelse break :blk false;
                const upper = self.upper orelse break :blk false;
                break :blk lower.inclusive and upper.inclusive and
                    std.mem.eql(u8, lower.value, required.operand) and std.mem.eql(u8, upper.value, required.operand);
            },
            .ne, .is_distinct => self.excludes(required.operand, exclusions),
            .gt, .gte => blk: {
                const lower = self.lower orelse break :blk false;
                break :blk switch (std.mem.order(u8, lower.value, required.operand)) {
                    .gt => true,
                    .eq => required.op == .gte or !lower.inclusive,
                    .lt => false,
                };
            },
            .lt, .lte => blk: {
                const upper = self.upper orelse break :blk false;
                break :blk switch (std.mem.order(u8, upper.value, required.operand)) {
                    .lt => true,
                    .eq => required.op == .lte or !upper.inclusive,
                    .gt => false,
                };
            },
            .is_null, .is_not_null => unreachable,
        };
    }
};

/// O(N) bounded scratch, O(N log N) sorting, then binary domain/exclusion
/// probes. Operand slices are borrowed from immutable predicate plans. No
/// per-request heap allocation, pairwise operand rehashing, or unbounded DNF.
pub const Proof = struct {
    domains: [max_conditions]Domain = undefined,
    domain_count: usize = 0,
    exclusions: [max_conditions][]const u8 = undefined,
    exclusion_count: usize = 0,
    impossible: bool = false,

    pub fn init(conditions: []const predicate.Plan) Proof {
        std.debug.assert(conditions.len <= max_conditions);
        var result: Proof = .{};
        var indices: [max_conditions]usize = undefined;
        for (indices[0..conditions.len], 0..) |*index, i| index.* = i;
        const Order = struct {
            fn less(plans: []const predicate.Plan, a: usize, b: usize) bool {
                const order = std.mem.order(u8, &plans[a].tuple.fingerprint, &plans[b].tuple.fingerprint);
                if (order != .eq) return order == .lt;
                return std.mem.order(u8, plans[a].operand, plans[b].operand) == .lt;
            }
        };
        std.mem.sort(usize, indices[0..conditions.len], conditions, Order.less);
        for (indices[0..conditions.len]) |index| {
            const condition = conditions[index];
            if (result.domain_count == 0 or !std.mem.eql(u8, &result.domains[result.domain_count - 1].identity, &condition.tuple.fingerprint)) {
                result.domains[result.domain_count] = .{ .identity = condition.tuple.fingerprint, .exclusion_start = result.exclusion_count };
                result.domain_count += 1;
            }
            const domain = &result.domains[result.domain_count - 1];
            domain.intersect(condition);
            if (!condition.operand_null and (condition.op == .ne or condition.op == .is_distinct)) {
                if (domain.exclusion_count != 0 and std.mem.eql(u8, result.exclusions[result.exclusion_count - 1], condition.operand)) continue;
                result.exclusions[result.exclusion_count] = condition.operand;
                result.exclusion_count += 1;
                domain.exclusion_count += 1;
            }
        }
        for (result.domains[0..result.domain_count]) |*domain| {
            if (domain.lower) |lower| if (domain.upper) |upper| switch (std.mem.order(u8, lower.value, upper.value)) {
                .gt => domain.nonnull = false,
                .eq => if (!lower.inclusive or !upper.inclusive or domain.excludes(lower.value, &result.exclusions)) {
                    domain.nonnull = false;
                },
                .lt => {},
            };
            if (!domain.nonnull and !domain.allows_null) result.impossible = true;
        }
        return result;
    }

    pub fn implies(self: *const Proof, required: predicate.Plan) bool {
        return impliesCondition(self.domains[0..self.domain_count], self.exclusions[0..self.exclusion_count], self.impossible, required);
    }
};

/// Immutable index predicates retain only their populated domains. They do
/// not reserve max_conditions worth of space for every one-column index.
pub const Compiled = struct {
    domains: []Domain,
    exclusions: [][]const u8,
    impossible: bool,

    pub fn init(alloc: std.mem.Allocator, conditions: []const predicate.Plan) !Compiled {
        const proof = Proof.init(conditions);
        const domains = try alloc.dupe(Domain, proof.domains[0..proof.domain_count]);
        errdefer alloc.free(domains);
        return .{ .domains = domains, .exclusions = try alloc.dupe([]const u8, proof.exclusions[0..proof.exclusion_count]), .impossible = proof.impossible };
    }

    pub fn deinit(self: Compiled, alloc: std.mem.Allocator) void {
        alloc.free(self.domains);
        alloc.free(self.exclusions);
    }

    pub fn implies(self: Compiled, required: predicate.Plan) bool {
        return impliesCondition(self.domains, self.exclusions, self.impossible, required);
    }
};

fn impliesCondition(domains: []const Domain, exclusions: []const []const u8, impossible: bool, required: predicate.Plan) bool {
    if (impossible) return true;
    var start: usize = 0;
    var end = domains.len;
    while (start < end) {
        const middle = start + (end - start) / 2;
        switch (std.mem.order(u8, &domains[middle].identity, &required.tuple.fingerprint)) {
            .lt => start = middle + 1,
            .gt => end = middle,
            .eq => return domains[middle].implies(required, exclusions),
        }
    }
    return false;
}

test "relational index system partial implication SQL truth exhaustive conjunction soundness" {
    const alloc = std.testing.allocator;
    const schema = @import("../schema.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const native = @import("../relational_index.zig");
    const Value = @import("relational_index_keys.zig").Value;
    const columns = [_]schema.RelationalColumn{.{ .name = "x", .path = "x", .column_type = .integer, .allows_null = true }};
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(alloc, table);
    defer layout.deinit();
    var conditions = std.ArrayList(predicate.Plan).empty;
    defer {
        for (conditions.items) |*condition| condition.deinit();
        conditions.deinit(alloc);
    }
    const operands = [_]Value{ .null, .{ .integer = -2 }, .{ .integer = 0 }, .{ .integer = 2 } };
    for (std.enums.values(native.RelationalCheckOp)) |op| for (operands) |operand| {
        if ((op == .is_null or op == .is_not_null) and operand != .null) continue;
        var condition = try predicate.Plan.init(alloc, table, &layout, .{ .column = "x", .op = op, .value = operand });
        errdefer condition.deinit();
        try conditions.append(alloc, condition);
    };
    const samples = [_]Value{ .null, .{ .integer = -3 }, .{ .integer = -2 }, .{ .integer = -1 }, .{ .integer = 0 }, .{ .integer = 1 }, .{ .integer = 2 }, .{ .integer = 3 } };
    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(alloc);
    const truth = try alloc.alloc([samples.len]bool, conditions.items.len);
    defer alloc.free(truth);
    for (conditions.items, truth) |*condition, *values| for (samples, values) |sample, *matches| {
        matches.* = (try condition.evaluateValue(alloc, &scratch, sample)).matches();
    };
    var proofs: usize = 0;
    for (conditions.items, 0..) |left, l| for (conditions.items, 0..) |right, r| {
        const premise = [_]predicate.Plan{ left, right };
        const proof = Proof.init(&premise);
        for (conditions.items, 0..) |required, p| {
            if (!proof.implies(required)) continue;
            proofs += 1;
            for (0..samples.len) |sample| if (truth[l][sample] and truth[r][sample]) {
                try std.testing.expect(truth[p][sample]);
            };
        }
    };
    try std.testing.expect(proofs > 1000);
}

test "relational index system partial implication stronger ranges null exclusions and residual direction" {
    const alloc = std.testing.allocator;
    const schema = @import("../schema.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const partial = @import("relational_index_predicate.zig");
    const columns = [_]schema.RelationalColumn{
        .{ .name = "x", .path = "x", .column_type = .integer, .allows_null = true },
        .{ .name = "label", .path = "label", .column_type = .string, .allows_null = true },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(alloc, table);
    defer layout.deinit();
    var index = try partial.Plan.init(alloc, table, &layout, &.{
        .{ .field = "x", .op = .gte, .value_json = "9007199254740993" },
        .{ .field = "x", .op = .lte, .value_json = "9007199254741000" },
        .{ .field = "label", .op = .is_not_null },
    });
    defer index.deinit();
    var query = try partial.Plan.init(alloc, table, &layout, &.{
        .{ .field = "x", .op = .gt, .value_json = "9007199254740993" },
        .{ .field = "x", .op = .lt, .value_json = "9007199254741000" },
        .{ .field = "x", .op = .ne, .value_json = "9007199254740992" },
        .{ .field = "label", .op = .eq, .value_json = "\"ok\"" },
    });
    defer query.deinit();
    var proven: [4]bool = undefined;
    try std.testing.expect(index.impliedByAndMark(query.conditions, &proven));
    try std.testing.expectEqualSlices(bool, &.{ false, false, true, false }, &proven);
    try std.testing.expect(!query.impliedBy(index.conditions));
    var equality = try partial.Plan.init(alloc, table, &layout, &.{.{ .field = "x", .op = .eq, .value_json = "4" }});
    defer equality.deinit();
    var singleton = try partial.Plan.init(alloc, table, &layout, &.{
        .{ .field = "x", .op = .gte, .value_json = "4" },
        .{ .field = "x", .op = .lte, .value_json = "4" },
    });
    defer singleton.deinit();
    try std.testing.expect(equality.impliedBy(singleton.conditions));
    var distinct = try partial.Plan.init(alloc, table, &layout, &.{.{ .field = "x", .op = .is_distinct, .value_json = "4" }});
    defer distinct.deinit();
    var not_null = try partial.Plan.init(alloc, table, &layout, &.{.{ .field = "x", .op = .is_not_null }});
    defer not_null.deinit();
    try std.testing.expect(!not_null.impliedBy(distinct.conditions));
    var ci = try partial.Plan.init(alloc, table, &layout, &.{.{ .field = "label", .op = .gte, .value_json = "\"a\"", .collation = "ci" }});
    defer ci.deinit();
    var binary = try partial.Plan.init(alloc, table, &layout, &.{.{ .field = "label", .op = .eq, .value_json = "\"b\"" }});
    defer binary.deinit();
    try std.testing.expect(!ci.impliedBy(binary.conditions));
    var folded = try partial.Plan.init(alloc, table, &layout, &.{.{ .field = "label", .op = .eq, .value_json = "\"B\"", .collation = "ci" }});
    defer folded.deinit();
    try std.testing.expect(ci.impliedBy(folded.conditions));
}
