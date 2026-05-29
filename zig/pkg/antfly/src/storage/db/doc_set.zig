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
const roaring = @import("../../encoding/roaring.zig");

pub const DocOrdinal = u32;
pub const small_set_threshold: usize = 64;
pub const bitmap_min_cardinality: usize = 4096;
const bitmap_min_density_numerator: usize = 1;
const bitmap_min_density_denominator: usize = 8;

pub const ResolvedDocSet = union(enum) {
    all,
    none,
    doc_keys: []const []const u8,
    ordinals: []const DocOrdinal,
    ordinal_bitmap: roaring.RoaringBitmap,

    pub fn deinit(self: *ResolvedDocSet, alloc: Allocator) void {
        switch (self.*) {
            .all, .none => {},
            .doc_keys => |keys| {
                for (keys) |key| alloc.free(@constCast(key));
                if (keys.len > 0) alloc.free(keys);
            },
            .ordinals => |ordinals| if (ordinals.len > 0) alloc.free(ordinals),
            .ordinal_bitmap => |*bitmap| bitmap.deinit(),
        }
        self.* = .none;
    }

    pub fn estimatedCardinality(self: *const ResolvedDocSet) ?usize {
        return switch (self.*) {
            .all => null,
            .none => 0,
            .doc_keys => |keys| keys.len,
            .ordinals => |ordinals| ordinals.len,
            .ordinal_bitmap => |*bitmap| bitmap.cardinality(),
        };
    }

    pub fn containsOrdinal(self: *const ResolvedDocSet, ordinal: DocOrdinal) bool {
        return switch (self.*) {
            .all => true,
            .none, .doc_keys => false,
            .ordinals => |ordinals| std.sort.binarySearch(DocOrdinal, ordinals, ordinal, compareOrdinal) != null,
            .ordinal_bitmap => |*bitmap| bitmap.contains(ordinal),
        };
    }
};

pub const ResolvedDocFilter = struct {
    include: ResolvedDocSet = .all,
    exclude: ResolvedDocSet = .none,

    pub fn deinit(self: *ResolvedDocFilter, alloc: Allocator) void {
        self.include.deinit(alloc);
        self.exclude.deinit(alloc);
        self.* = .{};
    }
};

pub fn fromOrdinalsAlloc(alloc: Allocator, ordinals_in: []const DocOrdinal) !ResolvedDocSet {
    if (ordinals_in.len == 0) return .none;

    const ordinals = try alloc.dupe(DocOrdinal, ordinals_in);
    errdefer alloc.free(ordinals);
    std.mem.sort(DocOrdinal, ordinals, {}, ordinalLessThan);
    const unique_len = uniqueSortedOrdinals(ordinals);

    if (!shouldUseOrdinalBitmap(ordinals[0..unique_len])) {
        return .{ .ordinals = try alloc.realloc(ordinals, unique_len) };
    }

    var bitmap = roaring.RoaringBitmap.init(alloc);
    errdefer bitmap.deinit();
    try bitmap.addSortedAscending(ordinals[0..unique_len]);
    alloc.free(ordinals);
    return .{ .ordinal_bitmap = bitmap };
}

pub fn cloneDocKeysAlloc(alloc: Allocator, doc_keys: []const []const u8) !ResolvedDocSet {
    if (doc_keys.len == 0) return .none;
    const out = try alloc.alloc([]const u8, doc_keys.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| alloc.free(@constCast(key));
        alloc.free(out);
    }
    for (doc_keys, 0..) |key, i| {
        out[i] = try alloc.dupe(u8, key);
        initialized += 1;
    }
    return .{ .doc_keys = out };
}

pub fn cloneAlloc(alloc: Allocator, set: *const ResolvedDocSet) !ResolvedDocSet {
    return switch (set.*) {
        .all => .all,
        .none => .none,
        .doc_keys => |keys| try cloneDocKeysAlloc(alloc, keys),
        .ordinals => |ordinals| if (ordinals.len == 0)
            .none
        else
            .{ .ordinals = try alloc.dupe(DocOrdinal, ordinals) },
        .ordinal_bitmap => |*bitmap| .{ .ordinal_bitmap = try bitmap.clone(alloc) },
    };
}

pub fn unionAlloc(alloc: Allocator, left: *const ResolvedDocSet, right: *const ResolvedDocSet) !?ResolvedDocSet {
    return switch (left.*) {
        .all => .all,
        .none => try cloneAlloc(alloc, right),
        .doc_keys => |left_keys| switch (right.*) {
            .all => .all,
            .none => try cloneAlloc(alloc, left),
            .doc_keys => |right_keys| docKeysSetAlloc(try unionDocKeysAlloc(alloc, left_keys, right_keys)),
            .ordinals, .ordinal_bitmap => null,
        },
        .ordinals, .ordinal_bitmap => switch (right.*) {
            .all => .all,
            .none => try cloneAlloc(alloc, left),
            .doc_keys => null,
            .ordinals, .ordinal_bitmap => try unionOrdinalSetsAlloc(alloc, left, right),
        },
    };
}

pub fn intersectAlloc(alloc: Allocator, left: *const ResolvedDocSet, right: *const ResolvedDocSet) !?ResolvedDocSet {
    return switch (left.*) {
        .none => .none,
        .all => try cloneAlloc(alloc, right),
        .doc_keys => |left_keys| switch (right.*) {
            .none => .none,
            .all => try cloneAlloc(alloc, left),
            .doc_keys => |right_keys| docKeysSetAlloc(try intersectDocKeysAlloc(alloc, left_keys, right_keys)),
            .ordinals, .ordinal_bitmap => null,
        },
        .ordinals, .ordinal_bitmap => switch (right.*) {
            .none => .none,
            .all => try cloneAlloc(alloc, left),
            .doc_keys => null,
            .ordinals, .ordinal_bitmap => try intersectOrdinalSetsAlloc(alloc, left, right),
        },
    };
}

pub fn differenceAlloc(alloc: Allocator, left: *const ResolvedDocSet, right: *const ResolvedDocSet) !?ResolvedDocSet {
    return switch (left.*) {
        .none => .none,
        .all => switch (right.*) {
            .none => .all,
            .all => .none,
            .doc_keys, .ordinals, .ordinal_bitmap => null,
        },
        .doc_keys => |left_keys| switch (right.*) {
            .none => try cloneAlloc(alloc, left),
            .all => .none,
            .doc_keys => |right_keys| docKeysSetAlloc(try differenceDocKeysAlloc(alloc, left_keys, right_keys)),
            .ordinals, .ordinal_bitmap => null,
        },
        .ordinals, .ordinal_bitmap => switch (right.*) {
            .none => try cloneAlloc(alloc, left),
            .all => .none,
            .doc_keys => null,
            .ordinals, .ordinal_bitmap => try differenceOrdinalSetsAlloc(alloc, left, right),
        },
    };
}

pub fn intersectFiltersAlloc(alloc: Allocator, left: *const ResolvedDocFilter, right: *const ResolvedDocFilter) !?ResolvedDocFilter {
    var include = (try intersectAlloc(alloc, &left.include, &right.include)) orelse return null;
    errdefer include.deinit(alloc);

    var exclude = (try unionAlloc(alloc, &left.exclude, &right.exclude)) orelse return null;
    errdefer exclude.deinit(alloc);

    return .{
        .include = include,
        .exclude = exclude,
    };
}

fn docKeysSetAlloc(keys: []const []const u8) ResolvedDocSet {
    if (keys.len > 0) return .{ .doc_keys = keys };
    return .none;
}

fn unionDocKeysAlloc(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |key| alloc.free(@constCast(key));
        out.deinit(alloc);
    }
    for (left) |key| try appendUniqueDocKeyAlloc(alloc, &out, key);
    for (right) |key| try appendUniqueDocKeyAlloc(alloc, &out, key);
    return try out.toOwnedSlice(alloc);
}

fn intersectDocKeysAlloc(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |key| alloc.free(@constCast(key));
        out.deinit(alloc);
    }
    for (left) |key| {
        if (!containsDocKey(right, key)) continue;
        try appendUniqueDocKeyAlloc(alloc, &out, key);
    }
    return try out.toOwnedSlice(alloc);
}

fn differenceDocKeysAlloc(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |key| alloc.free(@constCast(key));
        out.deinit(alloc);
    }
    for (left) |key| {
        if (containsDocKey(right, key)) continue;
        try appendUniqueDocKeyAlloc(alloc, &out, key);
    }
    return try out.toOwnedSlice(alloc);
}

fn appendUniqueDocKeyAlloc(alloc: Allocator, out: *std.ArrayListUnmanaged([]const u8), key: []const u8) !void {
    if (containsDocKey(out.items, key)) return;
    try out.append(alloc, try alloc.dupe(u8, key));
}

fn containsDocKey(keys: []const []const u8, needle: []const u8) bool {
    for (keys) |key| {
        if (std.mem.eql(u8, key, needle)) return true;
    }
    return false;
}

fn unionOrdinalSetsAlloc(alloc: Allocator, left: *const ResolvedDocSet, right: *const ResolvedDocSet) !ResolvedDocSet {
    return switch (left.*) {
        .ordinals => |left_ordinals| switch (right.*) {
            .ordinals => |right_ordinals| try unionSortedOrdinalsAlloc(alloc, left_ordinals, right_ordinals),
            .ordinal_bitmap => |*right_bitmap| try unionListBitmapAlloc(alloc, left_ordinals, right_bitmap),
            .all, .none, .doc_keys => unreachable,
        },
        .ordinal_bitmap => |*left_bitmap| switch (right.*) {
            .ordinals => |right_ordinals| try unionListBitmapAlloc(alloc, right_ordinals, left_bitmap),
            .ordinal_bitmap => |*right_bitmap| try unionBitmapsAlloc(alloc, left_bitmap, right_bitmap),
            .all, .none, .doc_keys => unreachable,
        },
        .all, .none, .doc_keys => unreachable,
    };
}

fn intersectOrdinalSetsAlloc(alloc: Allocator, left: *const ResolvedDocSet, right: *const ResolvedDocSet) !ResolvedDocSet {
    return switch (left.*) {
        .ordinals => |left_ordinals| switch (right.*) {
            .ordinals => |right_ordinals| try intersectSortedOrdinalsAlloc(alloc, left_ordinals, right_ordinals),
            .ordinal_bitmap => |*right_bitmap| try intersectListBitmapAlloc(alloc, left_ordinals, right_bitmap),
            .all, .none, .doc_keys => unreachable,
        },
        .ordinal_bitmap => |*left_bitmap| switch (right.*) {
            .ordinals => |right_ordinals| try intersectListBitmapAlloc(alloc, right_ordinals, left_bitmap),
            .ordinal_bitmap => |*right_bitmap| try intersectBitmapsAlloc(alloc, left_bitmap, right_bitmap),
            .all, .none, .doc_keys => unreachable,
        },
        .all, .none, .doc_keys => unreachable,
    };
}

fn differenceOrdinalSetsAlloc(alloc: Allocator, left: *const ResolvedDocSet, right: *const ResolvedDocSet) !ResolvedDocSet {
    return switch (left.*) {
        .ordinals => |left_ordinals| switch (right.*) {
            .ordinals => |right_ordinals| try differenceSortedOrdinalsAlloc(alloc, left_ordinals, right_ordinals),
            .ordinal_bitmap => |*right_bitmap| try differenceListBitmapAlloc(alloc, left_ordinals, right_bitmap),
            .all, .none, .doc_keys => unreachable,
        },
        .ordinal_bitmap => |*left_bitmap| switch (right.*) {
            .ordinals => |right_ordinals| try differenceBitmapListAlloc(alloc, left_bitmap, right_ordinals),
            .ordinal_bitmap => |*right_bitmap| try differenceBitmapsAlloc(alloc, left_bitmap, right_bitmap),
            .all, .none, .doc_keys => unreachable,
        },
        .all, .none, .doc_keys => unreachable,
    };
}

fn sortedOrdinalSetFromOwnedAlloc(alloc: Allocator, ordinals: []DocOrdinal) !ResolvedDocSet {
    if (ordinals.len == 0) {
        alloc.free(ordinals);
        return .none;
    }
    if (!shouldUseOrdinalBitmap(ordinals)) {
        return .{ .ordinals = ordinals };
    }
    var bitmap = roaring.RoaringBitmap.init(alloc);
    errdefer bitmap.deinit();
    try bitmap.addSortedAscending(ordinals);
    alloc.free(ordinals);
    return .{ .ordinal_bitmap = bitmap };
}

fn normalizedBitmapSetAlloc(alloc: Allocator, bitmap: roaring.RoaringBitmap) !ResolvedDocSet {
    const cardinality = bitmap.cardinality();
    if (cardinality == 0) {
        var owned = bitmap;
        owned.deinit();
        return .none;
    }
    var owned = bitmap;
    errdefer owned.deinit();
    if (shouldUseBitmapStats(owned.cardinality(), approximateBitmapSpan(&owned))) return .{ .ordinal_bitmap = bitmap };

    const ordinals = try alloc.alloc(DocOrdinal, cardinality);
    var i: usize = 0;
    var iter = owned.iterator();
    while (iter.next()) |ordinal| : (i += 1) ordinals[i] = ordinal;
    owned.deinit();
    return .{ .ordinals = ordinals };
}

fn shouldUseOrdinalBitmap(ordinals: []const DocOrdinal) bool {
    if (ordinals.len < bitmap_min_cardinality) return false;
    return shouldUseBitmapStats(ordinals.len, ordinalSpan(ordinals));
}

fn shouldUseBitmapStats(cardinality: usize, span: usize) bool {
    if (cardinality < bitmap_min_cardinality) return false;
    if (span == 0) return false;
    return cardinality * bitmap_min_density_denominator >= span * bitmap_min_density_numerator;
}

fn ordinalSpan(ordinals: []const DocOrdinal) usize {
    if (ordinals.len == 0) return 0;
    return @as(usize, ordinals[ordinals.len - 1]) - @as(usize, ordinals[0]) + 1;
}

fn approximateBitmapSpan(bitmap: *const roaring.RoaringBitmap) usize {
    if (bitmap.keys.items.len == 0) return 0;
    const first = @as(u32, bitmap.keys.items[0]) << 16;
    const last = (@as(u32, bitmap.keys.items[bitmap.keys.items.len - 1]) << 16) | 0xffff;
    return @as(usize, last) - @as(usize, first) + 1;
}

fn unionSortedOrdinalsAlloc(alloc: Allocator, left: []const DocOrdinal, right: []const DocOrdinal) !ResolvedDocSet {
    var out = try std.ArrayListUnmanaged(DocOrdinal).initCapacity(alloc, left.len + right.len);
    errdefer out.deinit(alloc);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len or j < right.len) {
        if (j >= right.len or (i < left.len and left[i] < right[j])) {
            out.appendAssumeCapacity(left[i]);
            i += 1;
        } else if (i >= left.len or right[j] < left[i]) {
            out.appendAssumeCapacity(right[j]);
            j += 1;
        } else {
            out.appendAssumeCapacity(left[i]);
            i += 1;
            j += 1;
        }
    }
    return try sortedOrdinalSetFromOwnedAlloc(alloc, try out.toOwnedSlice(alloc));
}

fn intersectSortedOrdinalsAlloc(alloc: Allocator, left: []const DocOrdinal, right: []const DocOrdinal) !ResolvedDocSet {
    var out = try std.ArrayListUnmanaged(DocOrdinal).initCapacity(alloc, @min(left.len, right.len));
    errdefer out.deinit(alloc);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len and j < right.len) {
        if (left[i] < right[j]) {
            i += 1;
        } else if (right[j] < left[i]) {
            j += 1;
        } else {
            out.appendAssumeCapacity(left[i]);
            i += 1;
            j += 1;
        }
    }
    return try sortedOrdinalSetFromOwnedAlloc(alloc, try out.toOwnedSlice(alloc));
}

fn differenceSortedOrdinalsAlloc(alloc: Allocator, left: []const DocOrdinal, right: []const DocOrdinal) !ResolvedDocSet {
    var out = try std.ArrayListUnmanaged(DocOrdinal).initCapacity(alloc, left.len);
    errdefer out.deinit(alloc);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len) {
        while (j < right.len and right[j] < left[i]) j += 1;
        if (j >= right.len or left[i] != right[j]) out.appendAssumeCapacity(left[i]);
        i += 1;
    }
    return try sortedOrdinalSetFromOwnedAlloc(alloc, try out.toOwnedSlice(alloc));
}

fn unionListBitmapAlloc(alloc: Allocator, ordinals: []const DocOrdinal, bitmap: *const roaring.RoaringBitmap) !ResolvedDocSet {
    var out = try bitmap.clone(alloc);
    errdefer out.deinit();
    try out.addSortedAscending(ordinals);
    return try normalizedBitmapSetAlloc(alloc, out);
}

fn intersectListBitmapAlloc(alloc: Allocator, ordinals: []const DocOrdinal, bitmap: *const roaring.RoaringBitmap) !ResolvedDocSet {
    var out = try std.ArrayListUnmanaged(DocOrdinal).initCapacity(alloc, ordinals.len);
    errdefer out.deinit(alloc);
    for (ordinals) |ordinal| {
        if (bitmap.contains(ordinal)) out.appendAssumeCapacity(ordinal);
    }
    return try sortedOrdinalSetFromOwnedAlloc(alloc, try out.toOwnedSlice(alloc));
}

fn differenceListBitmapAlloc(alloc: Allocator, ordinals: []const DocOrdinal, bitmap: *const roaring.RoaringBitmap) !ResolvedDocSet {
    var out = try std.ArrayListUnmanaged(DocOrdinal).initCapacity(alloc, ordinals.len);
    errdefer out.deinit(alloc);
    for (ordinals) |ordinal| {
        if (!bitmap.contains(ordinal)) out.appendAssumeCapacity(ordinal);
    }
    return try sortedOrdinalSetFromOwnedAlloc(alloc, try out.toOwnedSlice(alloc));
}

fn differenceBitmapListAlloc(alloc: Allocator, bitmap: *const roaring.RoaringBitmap, ordinals: []const DocOrdinal) !ResolvedDocSet {
    var out = try bitmap.clone(alloc);
    errdefer out.deinit();
    for (ordinals) |ordinal| try out.remove(ordinal);
    return try normalizedBitmapSetAlloc(alloc, out);
}

fn unionBitmapsAlloc(alloc: Allocator, left: *const roaring.RoaringBitmap, right: *const roaring.RoaringBitmap) !ResolvedDocSet {
    var out = try left.clone(alloc);
    errdefer out.deinit();
    try out.orWith(right);
    return try normalizedBitmapSetAlloc(alloc, out);
}

fn intersectBitmapsAlloc(alloc: Allocator, left: *const roaring.RoaringBitmap, right: *const roaring.RoaringBitmap) !ResolvedDocSet {
    var out = try left.clone(alloc);
    errdefer out.deinit();
    out.andWith(right);
    return try normalizedBitmapSetAlloc(alloc, out);
}

fn differenceBitmapsAlloc(alloc: Allocator, left: *const roaring.RoaringBitmap, right: *const roaring.RoaringBitmap) !ResolvedDocSet {
    var out = try left.clone(alloc);
    errdefer out.deinit();
    out.andNotWith(right);
    return try normalizedBitmapSetAlloc(alloc, out);
}

test "doc set differences compatible representations" {
    const alloc = std.testing.allocator;

    var left = try fromOrdinalsAlloc(alloc, &.{ 1, 2, 3, 4 });
    defer left.deinit(alloc);
    var right = try fromOrdinalsAlloc(alloc, &.{ 2, 4 });
    defer right.deinit(alloc);

    var difference = (try differenceAlloc(alloc, &left, &right)).?;
    defer difference.deinit(alloc);
    try std.testing.expect(difference.containsOrdinal(1));
    try std.testing.expect(!difference.containsOrdinal(2));
    try std.testing.expect(difference.containsOrdinal(3));
    try std.testing.expect(!difference.containsOrdinal(4));

    var doc_keys = try cloneDocKeysAlloc(alloc, &.{ "doc:a", "doc:b", "doc:c" });
    defer doc_keys.deinit(alloc);
    var excluded_keys = try cloneDocKeysAlloc(alloc, &.{"doc:b"});
    defer excluded_keys.deinit(alloc);
    var doc_key_difference = (try differenceAlloc(alloc, &doc_keys, &excluded_keys)).?;
    defer doc_key_difference.deinit(alloc);
    switch (doc_key_difference) {
        .doc_keys => |keys| {
            try std.testing.expectEqual(@as(usize, 2), keys.len);
            try std.testing.expectEqualStrings("doc:a", keys[0]);
            try std.testing.expectEqualStrings("doc:c", keys[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    const all: ResolvedDocSet = .all;
    try std.testing.expect((try differenceAlloc(alloc, &all, &right)) == null);

    var all_doc_keys_excluded = try cloneDocKeysAlloc(alloc, &.{ "doc:a", "doc:b", "doc:c" });
    defer all_doc_keys_excluded.deinit(alloc);
    var empty_doc_key_difference = (try differenceAlloc(alloc, &doc_keys, &all_doc_keys_excluded)).?;
    defer empty_doc_key_difference.deinit(alloc);
    switch (empty_doc_key_difference) {
        .none => {},
        else => return error.TestUnexpectedResult,
    }
}

test "doc filter intersection intersects includes and unions excludes" {
    const alloc = std.testing.allocator;

    var left = ResolvedDocFilter{
        .include = try fromOrdinalsAlloc(alloc, &.{ 1, 2, 3 }),
        .exclude = try fromOrdinalsAlloc(alloc, &.{3}),
    };
    defer left.deinit(alloc);

    var right = ResolvedDocFilter{
        .include = try fromOrdinalsAlloc(alloc, &.{ 2, 3, 4 }),
        .exclude = try fromOrdinalsAlloc(alloc, &.{4}),
    };
    defer right.deinit(alloc);

    var intersection = (try intersectFiltersAlloc(alloc, &left, &right)).?;
    defer intersection.deinit(alloc);

    try std.testing.expect(!intersection.include.containsOrdinal(1));
    try std.testing.expect(intersection.include.containsOrdinal(2));
    try std.testing.expect(intersection.include.containsOrdinal(3));
    try std.testing.expect(!intersection.include.containsOrdinal(4));
    try std.testing.expect(!intersection.exclude.containsOrdinal(2));
    try std.testing.expect(intersection.exclude.containsOrdinal(3));
    try std.testing.expect(intersection.exclude.containsOrdinal(4));
}

fn uniqueSortedOrdinals(ordinals: []DocOrdinal) usize {
    if (ordinals.len == 0) return 0;
    var write: usize = 1;
    for (ordinals[1..]) |ordinal| {
        if (ordinal == ordinals[write - 1]) continue;
        ordinals[write] = ordinal;
        write += 1;
    }
    return write;
}

fn ordinalLessThan(_: void, lhs: DocOrdinal, rhs: DocOrdinal) bool {
    return lhs < rhs;
}

fn compareOrdinal(needle: DocOrdinal, item: DocOrdinal) std.math.Order {
    return std.math.order(needle, item);
}

test "doc set normalizes small ordinal sets into sorted unique lists" {
    const alloc = std.testing.allocator;
    var set = try fromOrdinalsAlloc(alloc, &.{ 7, 3, 7, 1 });
    defer set.deinit(alloc);

    try std.testing.expectEqual(@as(?usize, 3), set.estimatedCardinality());
    try std.testing.expect(set.containsOrdinal(1));
    try std.testing.expect(set.containsOrdinal(3));
    try std.testing.expect(set.containsOrdinal(7));
    try std.testing.expect(!set.containsOrdinal(2));
}

test "doc set keeps medium ordinal sets as sorted unique lists" {
    const alloc = std.testing.allocator;
    var ordinals: [small_set_threshold + 2]DocOrdinal = undefined;
    for (&ordinals, 0..) |*ordinal, i| ordinal.* = @intCast(i);

    var set = try fromOrdinalsAlloc(alloc, &ordinals);
    defer set.deinit(alloc);

    try std.testing.expectEqual(@as(?usize, small_set_threshold + 2), set.estimatedCardinality());
    try std.testing.expect(set.containsOrdinal(0));
    try std.testing.expect(set.containsOrdinal(small_set_threshold + 1));
    try std.testing.expect(!set.containsOrdinal(small_set_threshold + 2));
    switch (set) {
        .ordinals => {},
        else => return error.TestUnexpectedResult,
    }
}

test "doc set promotes large dense ordinal sets into bitmaps" {
    const alloc = std.testing.allocator;
    var ordinals: [bitmap_min_cardinality]DocOrdinal = undefined;
    for (&ordinals, 0..) |*ordinal, i| ordinal.* = @intCast(i);

    var set = try fromOrdinalsAlloc(alloc, &ordinals);
    defer set.deinit(alloc);

    try std.testing.expectEqual(@as(?usize, bitmap_min_cardinality), set.estimatedCardinality());
    try std.testing.expect(set.containsOrdinal(0));
    try std.testing.expect(set.containsOrdinal(bitmap_min_cardinality - 1));
    try std.testing.expect(!set.containsOrdinal(bitmap_min_cardinality));
    switch (set) {
        .ordinal_bitmap => {},
        else => return error.TestUnexpectedResult,
    }
}

test "doc set keeps large sparse ordinal sets as sorted unique lists" {
    const alloc = std.testing.allocator;
    var ordinals: [bitmap_min_cardinality]DocOrdinal = undefined;
    for (&ordinals, 0..) |*ordinal, i| ordinal.* = @intCast(i * 97);

    var set = try fromOrdinalsAlloc(alloc, &ordinals);
    defer set.deinit(alloc);

    try std.testing.expectEqual(@as(?usize, bitmap_min_cardinality), set.estimatedCardinality());
    try std.testing.expect(set.containsOrdinal(0));
    try std.testing.expect(set.containsOrdinal((bitmap_min_cardinality - 1) * 97));
    try std.testing.expect(!set.containsOrdinal(1));
    switch (set) {
        .ordinals => {},
        else => return error.TestUnexpectedResult,
    }
}

test "doc set clones all owned representations" {
    const alloc = std.testing.allocator;

    var doc_keys = try cloneDocKeysAlloc(alloc, &.{ "doc:a", "doc:b" });
    defer doc_keys.deinit(alloc);
    var doc_keys_clone = try cloneAlloc(alloc, &doc_keys);
    defer doc_keys_clone.deinit(alloc);
    try std.testing.expectEqual(@as(?usize, 2), doc_keys_clone.estimatedCardinality());
    switch (doc_keys_clone) {
        .doc_keys => |keys| {
            try std.testing.expectEqualStrings("doc:a", keys[0]);
            try std.testing.expectEqualStrings("doc:b", keys[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var ordinals = try fromOrdinalsAlloc(alloc, &.{ 9, 3, 9 });
    defer ordinals.deinit(alloc);
    var ordinals_clone = try cloneAlloc(alloc, &ordinals);
    defer ordinals_clone.deinit(alloc);
    try std.testing.expect(ordinals_clone.containsOrdinal(3));
    try std.testing.expect(ordinals_clone.containsOrdinal(9));
    try std.testing.expect(!ordinals_clone.containsOrdinal(4));

    var wide_ordinals: [bitmap_min_cardinality]DocOrdinal = undefined;
    for (&wide_ordinals, 0..) |*ordinal, i| ordinal.* = @intCast(i + 100);
    var bitmap = try fromOrdinalsAlloc(alloc, &wide_ordinals);
    defer bitmap.deinit(alloc);
    var bitmap_clone = try cloneAlloc(alloc, &bitmap);
    defer bitmap_clone.deinit(alloc);
    try std.testing.expectEqual(@as(?usize, bitmap_min_cardinality), bitmap_clone.estimatedCardinality());
    try std.testing.expect(bitmap_clone.containsOrdinal(100));
    try std.testing.expect(bitmap_clone.containsOrdinal(100 + bitmap_min_cardinality - 1));
}

test "doc set unions and intersects compatible representations" {
    const alloc = std.testing.allocator;

    var left = try fromOrdinalsAlloc(alloc, &.{ 1, 3, 5 });
    defer left.deinit(alloc);
    var right = try fromOrdinalsAlloc(alloc, &.{ 3, 4, 5 });
    defer right.deinit(alloc);

    var union_set = (try unionAlloc(alloc, &left, &right)).?;
    defer union_set.deinit(alloc);
    try std.testing.expect(union_set.containsOrdinal(1));
    try std.testing.expect(union_set.containsOrdinal(3));
    try std.testing.expect(union_set.containsOrdinal(4));
    try std.testing.expect(union_set.containsOrdinal(5));
    try std.testing.expect(!union_set.containsOrdinal(2));

    var intersection = (try intersectAlloc(alloc, &left, &right)).?;
    defer intersection.deinit(alloc);
    try std.testing.expect(!intersection.containsOrdinal(1));
    try std.testing.expect(intersection.containsOrdinal(3));
    try std.testing.expect(intersection.containsOrdinal(5));

    const all: ResolvedDocSet = .all;
    var all_intersection = (try intersectAlloc(alloc, &all, &left)).?;
    defer all_intersection.deinit(alloc);
    try std.testing.expect(all_intersection.containsOrdinal(1));
    try std.testing.expect(all_intersection.containsOrdinal(5));

    var doc_keys = try cloneDocKeysAlloc(alloc, &.{ "doc:a", "doc:b" });
    defer doc_keys.deinit(alloc);
    try std.testing.expect((try unionAlloc(alloc, &doc_keys, &left)) == null);

    var doc_keys_right = try cloneDocKeysAlloc(alloc, &.{ "doc:b", "doc:c" });
    defer doc_keys_right.deinit(alloc);
    var doc_key_intersection = (try intersectAlloc(alloc, &doc_keys, &doc_keys_right)).?;
    defer doc_key_intersection.deinit(alloc);
    switch (doc_key_intersection) {
        .doc_keys => |keys| {
            try std.testing.expectEqual(@as(usize, 1), keys.len);
            try std.testing.expectEqualStrings("doc:b", keys[0]);
        },
        else => return error.TestUnexpectedResult,
    }
}
