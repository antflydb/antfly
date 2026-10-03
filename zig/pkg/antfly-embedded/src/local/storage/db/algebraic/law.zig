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
const algebra = @import("algebra.zig");
const token = @import("token.zig");
const hll = @import("hll.zig");

pub const Structure = enum {
    monoid,
    group,
    semiring,
    lattice,
};

pub const Id = enum {
    count,
    sum,
    sumsquares,
    avg,
    min,
    max,
    bool_any,
    bool_all,
    set_union,
    max_timestamp,
    provenance_semiring,
    hll,

    pub fn parse(text: []const u8) ?Id {
        inline for (std.meta.fields(Id)) |field| {
            if (std.mem.eql(u8, text, field.name)) return @enumFromInt(field.value);
        }
        return null;
    }
};

pub const Descriptor = struct {
    id: Id,
    structure: Structure,
    invertible: bool,
    support_required_for_delete: bool = false,
    exact_merge: bool = true,
};

pub fn descriptor(id: Id) Descriptor {
    return switch (id) {
        .count => .{ .id = id, .structure = .group, .invertible = true },
        .sum => .{ .id = id, .structure = .group, .invertible = true },
        .sumsquares => .{ .id = id, .structure = .group, .invertible = true },
        .avg => .{ .id = id, .structure = .group, .invertible = true },
        .min => .{ .id = id, .structure = .lattice, .invertible = false, .support_required_for_delete = true },
        .max => .{ .id = id, .structure = .lattice, .invertible = false, .support_required_for_delete = true },
        .bool_any => .{ .id = id, .structure = .lattice, .invertible = false },
        .bool_all => .{ .id = id, .structure = .lattice, .invertible = false },
        .set_union => .{ .id = id, .structure = .lattice, .invertible = false },
        .max_timestamp => .{ .id = id, .structure = .lattice, .invertible = false },
        .provenance_semiring => .{ .id = id, .structure = .semiring, .invertible = false },
        // HyperLogLog sketches union via register-wise max: a join-semilattice
        // that cannot be inverted, and whose merge is approximate (not exact).
        .hll => .{ .id = id, .structure = .lattice, .invertible = false, .support_required_for_delete = true, .exact_merge = false },
    };
}

pub fn fromOp(op: algebra.Op) Id {
    return switch (op) {
        .count => .count,
        .sum => .sum,
        .sumsquares => .sumsquares,
        .avg => .avg,
        .min => .min,
        .max => .max,
    };
}

pub fn identityAlloc(alloc: Allocator, id: Id) ![]u8 {
    return switch (id) {
        .count, .sum, .sumsquares => try alloc.dupe(u8, "0"),
        .avg => try algebra.encodeAvgAlloc(alloc, .{}),
        .bool_any => try alloc.dupe(u8, "false"),
        .bool_all => try alloc.dupe(u8, "true"),
        .set_union, .provenance_semiring => try token.canonicalTupleAlloc(alloc, &.{}),
        // An empty payload is the HLL identity (an absent / all-zero sketch).
        .min, .max, .max_timestamp, .hll => try alloc.dupe(u8, ""),
    };
}

pub fn combineAlloc(alloc: Allocator, id: Id, left: ?[]const u8, right: ?[]const u8) !?[]u8 {
    return switch (id) {
        .count => blk: {
            const lhs = if (left) |bytes| try algebra.parseI64(bytes) else 0;
            const rhs = if (right) |bytes| try algebra.parseI64(bytes) else 0;
            const next = lhs + rhs;
            break :blk if (next == 0) null else try algebra.encodeI64Alloc(alloc, next);
        },
        .sum, .sumsquares => blk: {
            const lhs = if (left) |bytes| try algebra.parseF64(bytes) else 0;
            const rhs = if (right) |bytes| try algebra.parseF64(bytes) else 0;
            const next = lhs + rhs;
            break :blk if (next == 0) null else try algebra.encodeF64Alloc(alloc, next);
        },
        .avg => blk: {
            var lhs = if (left) |bytes| try algebra.parseAvg(bytes) else algebra.AvgState{};
            const rhs = if (right) |bytes| try algebra.parseAvg(bytes) else algebra.AvgState{};
            lhs.sum += rhs.sum;
            lhs.count += rhs.count;
            break :blk if (lhs.count == 0) null else try algebra.encodeAvgAlloc(alloc, lhs);
        },
        .min => try minMaxAlloc(alloc, true, left, right),
        .max => try minMaxAlloc(alloc, false, left, right),
        .bool_any => try boolFoldAlloc(alloc, false, left, right),
        .bool_all => try boolFoldAlloc(alloc, true, left, right),
        .max_timestamp => try lexicalMaxAlloc(alloc, left, right),
        .set_union, .provenance_semiring => try tupleUnionAlloc(alloc, left, right),
        .hll => try hllUnionAlloc(alloc, left, right),
    };
}

pub fn multiplyAlloc(alloc: Allocator, id: Id, left: ?[]const u8, right: ?[]const u8) !?[]u8 {
    return switch (id) {
        .provenance_semiring => try tupleUnionAlloc(alloc, left, right),
        else => error.AlgebraicLawNotSemiring,
    };
}

pub fn invertAlloc(alloc: Allocator, id: Id, value: []const u8) ![]u8 {
    return switch (id) {
        .count => try algebra.encodeI64Alloc(alloc, -(try algebra.parseI64(value))),
        .sum, .sumsquares => try algebra.encodeF64Alloc(alloc, -(try algebra.parseF64(value))),
        .avg => blk: {
            const avg = try algebra.parseAvg(value);
            break :blk try algebra.encodeAvgAlloc(alloc, .{ .sum = -avg.sum, .count = -avg.count });
        },
        else => error.AlgebraicLawNotInvertible,
    };
}

fn minMaxAlloc(alloc: Allocator, comptime is_min: bool, left: ?[]const u8, right: ?[]const u8) !?[]u8 {
    if (left == null) return if (right) |bytes| try alloc.dupe(u8, bytes) else null;
    if (right == null) return try alloc.dupe(u8, left.?);
    const lhs = try algebra.parseF64(left.?);
    const rhs = try algebra.parseF64(right.?);
    if ((is_min and rhs < lhs) or (!is_min and rhs > lhs)) return try alloc.dupe(u8, right.?);
    return try alloc.dupe(u8, left.?);
}

fn boolFoldAlloc(alloc: Allocator, comptime all: bool, left: ?[]const u8, right: ?[]const u8) !?[]u8 {
    const lhs = if (left) |bytes| std.mem.eql(u8, bytes, "true") else all;
    const rhs = if (right) |bytes| std.mem.eql(u8, bytes, "true") else all;
    return try alloc.dupe(u8, if (if (all) lhs and rhs else lhs or rhs) "true" else "false");
}

fn lexicalMaxAlloc(alloc: Allocator, left: ?[]const u8, right: ?[]const u8) !?[]u8 {
    if (left == null) return if (right) |bytes| try alloc.dupe(u8, bytes) else null;
    if (right == null) return try alloc.dupe(u8, left.?);
    return try alloc.dupe(u8, if (std.mem.order(u8, left.?, right.?) == .lt) right.? else left.?);
}

fn hllUnionAlloc(alloc: Allocator, left: ?[]const u8, right: ?[]const u8) !?[]u8 {
    // Empty payloads are the identity sketch; normalize them to null so the
    // union of two absent sketches stays absent.
    const lhs = if (left) |bytes| (if (bytes.len == 0) null else bytes) else null;
    const rhs = if (right) |bytes| (if (bytes.len == 0) null else bytes) else null;
    if (lhs == null and rhs == null) return null;
    return try hll.mergeEncodedAlloc(alloc, lhs, rhs);
}

fn tupleUnionAlloc(alloc: Allocator, left: ?[]const u8, right: ?[]const u8) !?[]u8 {
    var values = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = values.iterator();
        while (it.next()) |entry| alloc.free(entry.key_ptr.*);
        values.deinit(alloc);
    }
    try addTupleValues(alloc, &values, left);
    try addTupleValues(alloc, &values, right);
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    var it = values.iterator();
    while (it.next()) |entry| try parts.append(alloc, entry.key_ptr.*);
    std.mem.sort([]const u8, parts.items, {}, lessString);
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

fn addTupleValues(alloc: Allocator, values: *std.StringHashMapUnmanaged(void), encoded: ?[]const u8) !void {
    const bytes = encoded orelse return;
    if (bytes.len == 0) return;
    const parts = try token.decodeTupleAlloc(alloc, bytes);
    defer {
        for (parts) |part| alloc.free(part);
        if (parts.len > 0) alloc.free(parts);
    }
    for (parts) |part| {
        if (values.contains(part)) continue;
        try values.put(alloc, try alloc.dupe(u8, part), {});
    }
}

fn lessString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

test "law descriptors expose algebraic structure and invertibility" {
    try std.testing.expectEqual(Structure.group, descriptor(.sum).structure);
    try std.testing.expect(descriptor(.sum).invertible);
    try std.testing.expectEqual(Structure.lattice, descriptor(.max).structure);
    try std.testing.expect(descriptor(.max).support_required_for_delete);
    try std.testing.expectEqual(Structure.semiring, descriptor(.provenance_semiring).structure);
}

test "law combines and inverts group coefficients" {
    const alloc = std.testing.allocator;
    const combined = (try combineAlloc(alloc, .sum, "3.5", "2.5")).?;
    defer alloc.free(combined);
    try std.testing.expectEqual(@as(f64, 6), try algebra.parseF64(combined));
    const inverse = try invertAlloc(alloc, .sum, combined);
    defer alloc.free(inverse);
    try std.testing.expectEqual(@as(f64, -6), try algebra.parseF64(inverse));
}

test "law merges set-like semiring payloads deterministically" {
    const alloc = std.testing.allocator;
    const a = try token.canonicalTupleAlloc(alloc, &.{ "b", "a" });
    defer alloc.free(a);
    const b = try token.canonicalTupleAlloc(alloc, &.{ "c", "a" });
    defer alloc.free(b);
    const merged = (try combineAlloc(alloc, .provenance_semiring, a, b)).?;
    defer alloc.free(merged);
    const parts = try token.decodeTupleAlloc(alloc, merged);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    try std.testing.expectEqual(@as(usize, 3), parts.len);
    try std.testing.expectEqualStrings("a", parts[0]);
    try std.testing.expectEqualStrings("b", parts[1]);
    try std.testing.expectEqualStrings("c", parts[2]);
}

test "law merges set union lattice payloads deterministically and idempotently" {
    const alloc = std.testing.allocator;
    const first = try token.canonicalTupleAlloc(alloc, &.{ "tag:b", "tag:a" });
    defer alloc.free(first);
    const second = try token.canonicalTupleAlloc(alloc, &.{ "tag:c", "tag:a", "tag:b" });
    defer alloc.free(second);

    const merged = (try combineAlloc(alloc, .set_union, first, second)).?;
    defer alloc.free(merged);
    const merged_again = (try combineAlloc(alloc, .set_union, merged, second)).?;
    defer alloc.free(merged_again);

    try std.testing.expectEqualStrings(merged, merged_again);
    const parts = try token.decodeTupleAlloc(alloc, merged);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    try std.testing.expectEqual(@as(usize, 3), parts.len);
    try std.testing.expectEqualStrings("tag:a", parts[0]);
    try std.testing.expectEqualStrings("tag:b", parts[1]);
    try std.testing.expectEqualStrings("tag:c", parts[2]);
    try std.testing.expectError(error.AlgebraicLawNotInvertible, invertAlloc(alloc, .set_union, merged));
}

test "law multiplies provenance semiring payloads explicitly" {
    const alloc = std.testing.allocator;
    const left = try token.canonicalTupleAlloc(alloc, &.{"order:o1"});
    defer alloc.free(left);
    const right = try token.canonicalTupleAlloc(alloc, &.{"customer:c1"});
    defer alloc.free(right);
    const product = (try multiplyAlloc(alloc, .provenance_semiring, left, right)).?;
    defer alloc.free(product);
    const parts = try token.decodeTupleAlloc(alloc, product);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    try std.testing.expectEqual(@as(usize, 2), parts.len);
    try std.testing.expectEqualStrings("customer:c1", parts[0]);
    try std.testing.expectEqualStrings("order:o1", parts[1]);
    try std.testing.expectError(error.AlgebraicLawNotSemiring, multiplyAlloc(alloc, .sum, "2", "3"));
}

test "hll law unions sketches as an idempotent, non-invertible lattice" {
    const alloc = std.testing.allocator;
    try std.testing.expectEqual(Structure.lattice, descriptor(.hll).structure);
    try std.testing.expect(!descriptor(.hll).invertible);
    try std.testing.expect(!descriptor(.hll).exact_merge);

    const identity = try identityAlloc(alloc, .hll);
    defer alloc.free(identity);
    try std.testing.expectEqual(@as(usize, 0), identity.len);

    // Build two single-value sketches and union them through the law.
    const a = try hll.singletonEncodedAlloc(alloc, hll.default_precision, "alpha");
    defer alloc.free(a);
    const b = try hll.singletonEncodedAlloc(alloc, hll.default_precision, "beta");
    defer alloc.free(b);

    const union_ab = (try combineAlloc(alloc, .hll, a, b)).?;
    defer alloc.free(union_ab);
    try std.testing.expectEqual(@as(u64, 2), try hll.estimateEncoded(union_ab));

    // Identity element leaves the sketch unchanged, and the union is idempotent.
    const with_identity = (try combineAlloc(alloc, .hll, union_ab, identity)).?;
    defer alloc.free(with_identity);
    try std.testing.expectEqualSlices(u8, union_ab, with_identity);

    const again = (try combineAlloc(alloc, .hll, union_ab, union_ab)).?;
    defer alloc.free(again);
    try std.testing.expectEqualSlices(u8, union_ab, again);

    try std.testing.expectError(error.AlgebraicLawNotInvertible, invertAlloc(alloc, .hll, union_ab));
}
