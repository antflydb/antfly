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
const Allocator = std.mem.Allocator;
const law = @import("law.zig");
const token = @import("token.zig");

pub const Term = struct {
    basis: []u8,
    coefficient: []u8,

    pub fn deinit(self: *Term, alloc: Allocator) void {
        alloc.free(self.basis);
        alloc.free(self.coefficient);
        self.* = undefined;
    }
};

pub const SparseVector = struct {
    law_id: law.Id,
    terms: std.ArrayListUnmanaged(Term) = .empty,

    pub fn init(law_id: law.Id) SparseVector {
        return .{ .law_id = law_id };
    }

    pub fn deinit(self: *SparseVector, alloc: Allocator) void {
        for (self.terms.items) |*term| term.deinit(alloc);
        self.terms.deinit(alloc);
        self.* = undefined;
    }

    pub fn addTerm(self: *SparseVector, alloc: Allocator, basis: []const u8, coefficient: []const u8) !void {
        for (self.terms.items, 0..) |*term, i| {
            if (!std.mem.eql(u8, term.basis, basis)) continue;
            const next = try law.combineAlloc(alloc, self.law_id, term.coefficient, coefficient);
            if (next) |bytes| {
                alloc.free(term.coefficient);
                term.coefficient = bytes;
            } else {
                term.deinit(alloc);
                _ = self.terms.orderedRemove(i);
            }
            return;
        }
        const next = (try law.combineAlloc(alloc, self.law_id, null, coefficient)) orelse return;
        errdefer alloc.free(next);
        const owned_basis = try alloc.dupe(u8, basis);
        errdefer alloc.free(owned_basis);
        try self.terms.append(alloc, .{
            .basis = owned_basis,
            .coefficient = next,
        });
    }

    pub fn addVector(self: *SparseVector, alloc: Allocator, other: SparseVector) !void {
        if (self.law_id != other.law_id) return error.IncompatibleAlgebraicLaw;
        for (other.terms.items) |term| try self.addTerm(alloc, term.basis, term.coefficient);
    }

    pub fn coefficientAlloc(self: SparseVector, alloc: Allocator, basis: []const u8) !?[]u8 {
        for (self.terms.items) |term| {
            if (std.mem.eql(u8, term.basis, basis)) return try alloc.dupe(u8, term.coefficient);
        }
        return null;
    }

    pub fn projectPrefixAlloc(self: SparseVector, alloc: Allocator, prefix_components: []const []const u8) !SparseVector {
        var out = SparseVector.init(self.law_id);
        errdefer out.deinit(alloc);
        for (self.terms.items) |term| {
            if (try tupleStartsWith(alloc, term.basis, prefix_components)) {
                try out.addTerm(alloc, term.basis, term.coefficient);
            }
        }
        return out;
    }
};

pub fn basisAlloc(alloc: Allocator, components: []const []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, components);
}

pub const Projection = struct {
    left: []const usize = &.{},
    right: []const usize = &.{},
};

pub fn contractEquiJoinAlloc(
    alloc: Allocator,
    left: SparseVector,
    right: SparseVector,
    left_match_component: usize,
    right_match_component: usize,
    projection: Projection,
) !SparseVector {
    if (left.law_id != .provenance_semiring or right.law_id != .provenance_semiring) {
        return error.UnsupportedAlgebraicContraction;
    }
    var out = SparseVector.init(.provenance_semiring);
    errdefer out.deinit(alloc);

    for (left.terms.items) |left_term| {
        const left_parts = try token.decodeTupleAlloc(alloc, left_term.basis);
        defer freeParts(alloc, left_parts);
        if (left_match_component >= left_parts.len) continue;
        for (right.terms.items) |right_term| {
            const right_parts = try token.decodeTupleAlloc(alloc, right_term.basis);
            defer freeParts(alloc, right_parts);
            if (right_match_component >= right_parts.len) continue;
            if (!std.mem.eql(u8, left_parts[left_match_component], right_parts[right_match_component])) continue;
            const joined_basis = try projectedBasisAlloc(alloc, left_parts, right_parts, projection);
            defer alloc.free(joined_basis);
            const product = (try law.multiplyAlloc(alloc, .provenance_semiring, left_term.coefficient, right_term.coefficient)) orelse continue;
            defer alloc.free(product);
            try out.addTerm(alloc, joined_basis, product);
        }
    }
    return out;
}

fn projectedBasisAlloc(alloc: Allocator, left_parts: []const []const u8, right_parts: []const []const u8, projection: Projection) ![]u8 {
    var components = std.ArrayListUnmanaged([]const u8).empty;
    defer components.deinit(alloc);
    for (projection.left) |idx| {
        if (idx >= left_parts.len) return error.InvalidAlgebraicProjection;
        try components.append(alloc, left_parts[idx]);
    }
    for (projection.right) |idx| {
        if (idx >= right_parts.len) return error.InvalidAlgebraicProjection;
        try components.append(alloc, right_parts[idx]);
    }
    return try basisAlloc(alloc, components.items);
}

fn tupleStartsWith(alloc: Allocator, encoded: []const u8, prefix_components: []const []const u8) !bool {
    const parts = try token.decodeTupleAlloc(alloc, encoded);
    defer freeParts(alloc, parts);
    if (prefix_components.len > parts.len) return false;
    for (prefix_components, 0..) |component, i| {
        if (!std.mem.eql(u8, component, parts[i])) return false;
    }
    return true;
}

fn freeParts(alloc: Allocator, parts: [][]u8) void {
    for (parts) |part| alloc.free(part);
    if (parts.len > 0) alloc.free(parts);
}

test "sparse vector folds group coefficients and removes zero terms" {
    const alloc = std.testing.allocator;
    var vec = SparseVector.init(.count);
    defer vec.deinit(alloc);
    const basis = try basisAlloc(alloc, &.{ "orders", "region:west" });
    defer alloc.free(basis);
    try vec.addTerm(alloc, basis, "1");
    try vec.addTerm(alloc, basis, "2");
    const count = (try vec.coefficientAlloc(alloc, basis)).?;
    defer alloc.free(count);
    try std.testing.expectEqualStrings("3", count);
    try vec.addTerm(alloc, basis, "-3");
    try std.testing.expect((try vec.coefficientAlloc(alloc, basis)) == null);
}

test "sparse vector projects tuple-basis prefixes" {
    const alloc = std.testing.allocator;
    var vec = SparseVector.init(.sum);
    defer vec.deinit(alloc);
    const west = try basisAlloc(alloc, &.{ "orders", "west", "p1" });
    defer alloc.free(west);
    const east = try basisAlloc(alloc, &.{ "orders", "east", "p1" });
    defer alloc.free(east);
    try vec.addTerm(alloc, west, "2.5");
    try vec.addTerm(alloc, east, "9.0");
    var projected = try vec.projectPrefixAlloc(alloc, &.{ "orders", "west" });
    defer projected.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), projected.terms.items.len);
    try std.testing.expectEqualStrings(west, projected.terms.items[0].basis);
}

test "sparse vector contracts provenance join facts exactly" {
    const alloc = std.testing.allocator;
    var orders = SparseVector.init(.provenance_semiring);
    defer orders.deinit(alloc);
    var customers = SparseVector.init(.provenance_semiring);
    defer customers.deinit(alloc);

    const order_basis = try basisAlloc(alloc, &.{ "order", "c1", "100" });
    defer alloc.free(order_basis);
    const customer_basis = try basisAlloc(alloc, &.{ "customer", "c1", "west" });
    defer alloc.free(customer_basis);
    const order_prov = try basisAlloc(alloc, &.{"o1"});
    defer alloc.free(order_prov);
    const customer_prov = try basisAlloc(alloc, &.{"c1"});
    defer alloc.free(customer_prov);
    try orders.addTerm(alloc, order_basis, order_prov);
    try customers.addTerm(alloc, customer_basis, customer_prov);

    var joined = try contractEquiJoinAlloc(alloc, orders, customers, 1, 1, .{
        .left = &.{2},
        .right = &.{2},
    });
    defer joined.deinit(alloc);
    const joined_basis = try basisAlloc(alloc, &.{ "100", "west" });
    defer alloc.free(joined_basis);
    const coefficient = (try joined.coefficientAlloc(alloc, joined_basis)).?;
    defer alloc.free(coefficient);
    const parts = try token.decodeTupleAlloc(alloc, coefficient);
    defer freeParts(alloc, parts);
    try std.testing.expectEqual(@as(usize, 2), parts.len);
    try std.testing.expectEqualStrings("c1", parts[0]);
    try std.testing.expectEqualStrings("o1", parts[1]);
}
