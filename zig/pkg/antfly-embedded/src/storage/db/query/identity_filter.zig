// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exact primary-identity Boolean algebra. Finite and cofinite domains keep
//! NOT and minimum-should-match exact without assuming a table universe. Only
//! finite final domains may bypass secondary indexes; stored predicates never
//! become an approximate identity selection.
const std = @import("std");
const Filter = @import("graph_exec.zig").CompiledPatternFilter;
const A = std.mem.Allocator;

pub const Domain = struct {
    /// Membership outside the finite exception directory.
    complement: bool = false,
    ids: std.StringHashMapUnmanaged(void) = .empty,

    pub fn deinit(self: *Domain, a: A) void {
        self.ids.deinit(a);
        self.* = .{};
    }
    pub fn contains(self: Domain, key: []const u8) bool {
        return self.complement != self.ids.contains(key);
    }
    const Operation = enum { intersection, merge };
    fn apply(operation: Operation, left: bool, right: bool) bool {
        return if (operation == .intersection) left and right else left or right;
    }
    fn combine(self: *Domain, a: A, other: Domain, operation: Operation) !void {
        var result: Domain = .{ .complement = apply(operation, self.complement, other.complement) };
        errdefer result.deinit(a);
        var left = self.ids.keyIterator();
        while (left.next()) |key| if (apply(operation, self.contains(key.*), other.contains(key.*)) != result.complement) try result.ids.put(a, key.*, {});
        var right = other.ids.keyIterator();
        while (right.next()) |key| if (apply(operation, self.contains(key.*), other.contains(key.*)) != result.complement) try result.ids.put(a, key.*, {});
        self.deinit(a);
        self.* = result;
    }
};

/// Map keys borrow the parsed filter, while directory allocations are
/// reclaimed after each algebraic operation rather than accumulated in an
/// arena. This bounds retained state by live domains, not Boolean tree size.
pub fn resolve(a: A, filter: Filter) !?Domain {
    return resolveDepth(a, filter, 0);
}
fn resolveDepth(a: A, filter: Filter, depth: usize) anyerror!?Domain {
    if (depth >= 64) return null;
    var result: Domain = .{};
    errdefer result.deinit(a);
    switch (filter) {
        .match_all => result.complement = true,
        .match_none => {},
        .doc_id => |ids| for (ids) |key| {
            try result.ids.put(a, key, {});
        },
        .field_matcher, .logical_value => return null,
        .conjuncts, .disjuncts => |children| {
            const operation: Domain.Operation = if (filter == .conjuncts) .intersection else .merge;
            result.complement = operation == .intersection;
            for (children) |child| {
                var domain = (try resolveDepth(a, child, depth + 1)) orelse {
                    result.deinit(a);
                    return null;
                };
                defer domain.deinit(a);
                try result.combine(a, domain, operation);
            }
        },
        .bool_query => |query| {
            result.complement = true;
            for (query.must) |child| {
                var domain = (try resolveDepth(a, child, depth + 1)) orelse {
                    result.deinit(a);
                    return null;
                };
                defer domain.deinit(a);
                try result.combine(a, domain, .intersection);
            }
            var should = (try threshold(a, query.should, query.min_should, depth + 1)) orelse {
                result.deinit(a);
                return null;
            };
            defer should.deinit(a);
            try result.combine(a, should, .intersection);
            for (query.must_not) |child| {
                var domain = (try resolveDepth(a, child, depth + 1)) orelse {
                    result.deinit(a);
                    return null;
                };
                defer domain.deinit(a);
                domain.complement = !domain.complement;
                try result.combine(a, domain, .intersection);
            }
        },
    }
    return result;
}

fn threshold(a: A, children: []const Filter, minimum: usize, depth: usize) !?Domain {
    var deltas: std.StringHashMapUnmanaged(i64) = .empty;
    defer deltas.deinit(a);
    var baseline: i64 = 0;
    for (children) |child| {
        var domain = (try resolveDepth(a, child, depth)) orelse return null;
        defer domain.deinit(a);
        if (domain.complement) baseline += 1;
        const delta: i64 = if (domain.complement) -1 else 1;
        var keys = domain.ids.keyIterator();
        while (keys.next()) |key| {
            const entry = try deltas.getOrPut(a, key.*);
            if (!entry.found_existing) entry.value_ptr.* = 0;
            entry.value_ptr.* += delta;
        }
    }
    const required: i64 = @intCast(@min(minimum, children.len + 1));
    var result: Domain = .{ .complement = baseline >= required };
    errdefer result.deinit(a);
    var entries = deltas.iterator();
    while (entries.next()) |entry| if ((baseline + entry.value_ptr.* >= required) != result.complement) try result.ids.put(a, entry.key_ptr.*, {});
    return result;
}
