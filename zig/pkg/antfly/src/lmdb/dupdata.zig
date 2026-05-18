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
const format = @import("format.zig");
const node = @import("node.zig");
const page = @import("page.zig");

pub const Error = node.Error || page.Error || std.mem.Allocator.Error || error{
    Corrupted,
    Incompatible,
    MapFull,
    UnsupportedNodeFlags,
};

pub fn subpageView(leaf: node.View) Error!page.View {
    if (!leaf.isDupdata() or leaf.isSubdata()) return error.UnsupportedNodeFlags;

    const subpage = try page.View.init(leaf.inlineValue());
    if (!subpage.hasFlag(format.PageFlags.subp)) return error.Corrupted;
    if (!subpage.hasFlag(format.PageFlags.leaf)) return error.Corrupted;
    return subpage;
}

pub fn count(leaf: node.View) Error!usize {
    return (try subpageView(leaf)).nodeCount();
}

pub fn firstValue(leaf: node.View) Error![]const u8 {
    return valueAt(leaf, 0);
}

pub fn valueAt(leaf: node.View, index: usize) Error![]const u8 {
    const subpage = try subpageView(leaf);
    if (subpage.kind() == .leaf2) return leaf2ValueAt(subpage, index);
    const dup = try node.View.fromPage(subpage, index);
    if (dup.flags() != 0 or dup.dataSize() != 0 or dup.storedValueLen() != 0) return error.Corrupted;
    return dup.key();
}

pub fn leaf2ValueAt(subpage: page.View, index: usize) Error![]const u8 {
    if (!subpage.hasFlag(format.PageFlags.leaf2)) return error.Incompatible;
    const key_size = subpage.pad();
    const node_count = subpage.nodeCount();
    if (index >= node_count) return error.InvalidNodeIndex;
    const start = format.page_header_size + index * key_size;
    const end = start + key_size;
    if (end > subpage.bytes.len) return error.Corrupted;
    return subpage.bytes[start..end];
}

pub fn appendClonedValues(
    allocator: std.mem.Allocator,
    leaf: node.View,
    out: *std.ArrayListUnmanaged([]u8),
) Error!void {
    const dup_count = try count(leaf);
    for (0..dup_count) |i| {
        try out.append(allocator, try allocator.dupe(u8, try valueAt(leaf, i)));
    }
}

pub fn appendClonedValuesFromBytes(
    allocator: std.mem.Allocator,
    subpage_bytes: []const u8,
    out: *std.ArrayListUnmanaged([]u8),
) Error!void {
    const subpage = try page.View.init(subpage_bytes);
    if (!subpage.hasFlag(format.PageFlags.subp)) return error.Corrupted;
    if (!subpage.hasFlag(format.PageFlags.leaf)) return error.Corrupted;

    if (subpage.kind() == .leaf2) {
        for (0..subpage.nodeCount()) |i| {
            try out.append(allocator, try allocator.dupe(u8, try leaf2ValueAt(subpage, i)));
        }
        return;
    }

    for (0..subpage.nodeCount()) |i| {
        const dup = try node.View.fromPage(subpage, i);
        if (dup.flags() != 0 or dup.dataSize() != 0 or dup.storedValueLen() != 0) return error.Corrupted;
        try out.append(allocator, try allocator.dupe(u8, dup.key()));
    }
}

pub fn encodeSubpage(
    allocator: std.mem.Allocator,
    db_flags: u16,
    values: []const []const u8,
) Error![]u8 {
    if ((db_flags & format.DbFlags.dup_fixed) != 0) {
        if (values.len == 0 or values[0].len == 0) return error.Incompatible;
        const value_size = values[0].len;
        for (values) |value| {
            if (value.len != value_size) return error.Incompatible;
        }
        const total = format.page_header_size + values.len * value_size;
        const subpage = try allocator.alloc(u8, total);
        try writeLeaf2Subpage(subpage, 0, @intCast(value_size), values);
        return subpage;
    }

    var total: usize = format.page_header_size;
    for (values) |value| {
        total += @sizeOf(format.Indx) + node.header_size + value.len;
    }
    const subpage = try allocator.alloc(u8, total);
    try writeSubpage(subpage, 0, values);
    return subpage;
}

pub fn writeSubpage(page_bytes: []u8, pgno: format.Pgno, values: []const []const u8) !void {
    @memset(page_bytes, 0);

    var upper = page_bytes.len;
    for (values, 0..) |value, i| {
        const node_len = node.header_size + value.len;
        const ptr_offset = format.page_header_size + i * @sizeOf(format.Indx);
        const lower = ptr_offset + @sizeOf(format.Indx);

        if (node_len > upper or lower > upper - node_len) return error.MapFull;
        upper -= node_len;

        const hdr = node.Header{
            .mn_lo = 0,
            .mn_hi = 0,
            .mn_flags = 0,
            .mn_ksize = @intCast(value.len),
        };
        format.writeStruct(node.Header, page_bytes[upper..][0..node.header_size], hdr);
        @memcpy(page_bytes[upper + node.header_size .. upper + node.header_size + value.len], value);
        format.writeNativeInt(format.Indx, page_bytes[ptr_offset..][0..@sizeOf(format.Indx)], @intCast(upper));
    }

    const lower = format.page_header_size + values.len * @sizeOf(format.Indx);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.leaf | format.PageFlags.subp,
        .mp_lower = @intCast(lower),
        .mp_upper = @intCast(upper),
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);
}

pub fn writeLeaf2Subpage(page_bytes: []u8, pgno: format.Pgno, value_size: u16, values: []const []const u8) !void {
    @memset(page_bytes, 0);

    for (values, 0..) |value, i| {
        if (value.len != value_size) return error.Incompatible;
        const start = format.page_header_size + i * value_size;
        const end = start + value_size;
        if (end > page_bytes.len) return error.MapFull;
        @memcpy(page_bytes[start..end], value);
    }

    const lower = format.page_header_size + values.len * @sizeOf(format.Indx);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = value_size,
        .mp_flags = format.PageFlags.leaf | format.PageFlags.leaf2 | format.PageFlags.subp,
        .mp_lower = @intCast(lower),
        .mp_upper = @intCast(lower),
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);
}

pub fn compareValues(db_flags: u16, left: []const u8, right: []const u8) std.math.Order {
    if ((db_flags & format.DbFlags.integer_dup) != 0) return compareInteger(left, right);
    if ((db_flags & format.DbFlags.reverse_dup) != 0) return compareReverse(left, right);
    return std.mem.order(u8, left, right);
}

fn compareInteger(left: []const u8, right: []const u8) std.math.Order {
    if (left.len != right.len) return std.math.order(left.len, right.len);
    return switch (left.len) {
        @sizeOf(u32) => std.math.order(format.readNativeInt(u32, left), format.readNativeInt(u32, right)),
        @sizeOf(u64) => std.math.order(format.readNativeInt(u64, left), format.readNativeInt(u64, right)),
        else => std.mem.order(u8, left, right),
    };
}

fn compareReverse(left: []const u8, right: []const u8) std.math.Order {
    var i: usize = 0;
    const common = @min(left.len, right.len);
    while (i < common) : (i += 1) {
        const lb = left[left.len - 1 - i];
        const rb = right[right.len - 1 - i];
        if (lb < rb) return .lt;
        if (lb > rb) return .gt;
    }
    return std.math.order(left.len, right.len);
}

test "dupdata subpage round-trips duplicate values" {
    var subpage_bytes = std.mem.zeroes([128]u8);
    try writeSubpage(&subpage_bytes, 0, &.{ "a", "b", "c" });

    var parent_bytes = std.mem.zeroes([node.header_size + 1 + subpage_bytes.len]u8);
    const hdr = node.Header{
        .mn_lo = @intCast(subpage_bytes.len & 0xffff),
        .mn_hi = @intCast((subpage_bytes.len >> 16) & 0xffff),
        .mn_flags = format.NodeFlags.dupdata,
        .mn_ksize = 1,
    };
    format.writeStruct(node.Header, parent_bytes[0..node.header_size], hdr);
    parent_bytes[node.header_size] = 'k';
    @memcpy(parent_bytes[node.header_size + 1 ..], &subpage_bytes);
    const parent = node.View{
        .page_bytes = parent_bytes[0..],
        .offset = 0,
        .header = hdr,
    };
    try std.testing.expectEqual(@as(usize, 3), try count(parent));
    try std.testing.expectEqualStrings("a", try firstValue(parent));
    try std.testing.expectEqualStrings("b", try valueAt(parent, 1));
    try std.testing.expectEqualStrings("c", try valueAt(parent, 2));
}

test "dupdata leaf2 subpage round-trips duplicate values" {
    var subpage_bytes = std.mem.zeroes([64]u8);
    try writeLeaf2Subpage(&subpage_bytes, 0, 2, &.{ "aa", "bb", "cc" });

    var parent_bytes = std.mem.zeroes([node.header_size + 1 + subpage_bytes.len]u8);
    const hdr = node.Header{
        .mn_lo = @intCast(subpage_bytes.len & 0xffff),
        .mn_hi = @intCast((subpage_bytes.len >> 16) & 0xffff),
        .mn_flags = format.NodeFlags.dupdata,
        .mn_ksize = 1,
    };
    format.writeStruct(node.Header, parent_bytes[0..node.header_size], hdr);
    parent_bytes[node.header_size] = 'k';
    @memcpy(parent_bytes[node.header_size + 1 ..], &subpage_bytes);
    const parent = node.View{
        .page_bytes = parent_bytes[0..],
        .offset = 0,
        .header = hdr,
    };
    try std.testing.expectEqual(@as(usize, 3), try count(parent));
    try std.testing.expectEqualStrings("aa", try firstValue(parent));
    try std.testing.expectEqualStrings("bb", try valueAt(parent, 1));
    try std.testing.expectEqualStrings("cc", try valueAt(parent, 2));
}
