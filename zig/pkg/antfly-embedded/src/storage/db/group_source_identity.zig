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

//! Private logical source identities. The hash prefix distributes groups in
//! the existing key-range router. Partition anchors remain below their group
//! prefix; split boundaries round back to the group root so ordered production
//! and prior-artifact reads retain one owner across topology changes.
const std = @import("std");
pub const primary_kind: u8 = 0x14;
pub const reverse_kind: u8 = 0x54;
pub const outbox_kind: u8 = 0x55;
pub const metadata_kind: u8 = 0x56;
pub const marker = "/\x00antfly/group/v1/";
pub fn anchor(a: std.mem.Allocator, table: u128, configuration: [32]u8, group: [32]u8) ![]u8 {
    if (table == 0) return error.InvalidGroupedSourceMutation;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:group-owner-routing:v1");
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &bytes, table, .big);
    hash.update(&bytes);
    hash.update(&configuration);
    hash.update(&group);
    return std.fmt.allocPrint(a, "{x}" ++ marker ++ "{x}/{x}/{x}", .{ &hash.finalResult(), table, &configuration, &group });
}
pub fn root(document: []const u8) ?[]const u8 {
    if (document.len < 64 + marker.len + 1 + 1 + 64 + 1 + 64 or !std.mem.eql(u8, document[64..][0..marker.len], marker)) return null;
    for (document[0..64]) |byte| if (!std.ascii.isHex(byte)) return null;
    const start = 64 + marker.len;
    const table_end = std.mem.indexOfScalarPos(u8, document, start, '/') orelse return null;
    if (table_end == start or table_end - start > 32) return null;
    const table = std.fmt.parseInt(u128, document[start..table_end], 16) catch return null;
    if (table == 0) return null;
    const end = table_end + 1 + 64 + 1 + 64;
    if (document.len < end or document[table_end + 65] != '/') return null;
    for (document[table_end + 1 .. table_end + 65]) |byte| if (!std.ascii.isHex(byte)) return null;
    for (document[table_end + 66 .. end]) |byte| if (!std.ascii.isHex(byte)) return null;
    if (document.len != end and !std.mem.startsWith(u8, document[end..], "/p/")) return null;
    return document[0..end];
}
pub fn isAnchor(document: []const u8) bool {
    return root(document) != null;
}
pub fn partition(a: std.mem.Allocator, group_anchor: []const u8, segment: []const u8, cell: i64) ![]u8 {
    const selected = root(group_anchor) orelse return error.InvalidGroupedSourceMutation;
    if (selected.len != group_anchor.len) return error.InvalidGroupedSourceMutation;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(segment, &digest, .{});
    return std.fmt.allocPrint(a, "{s}/p/{x}/{d}", .{ group_anchor, &digest, cell });
}
pub fn splitBoundary(document: []const u8) []const u8 {
    return root(document) orelse document;
}

test "group anchors distribute by hash and partition split boundaries retain the group owner" {
    const a = std.testing.allocator;
    const group = try anchor(a, 7, @splat(1), @splat(2));
    defer a.free(group);
    const other = try anchor(a, 7, @splat(1), @splat(3));
    defer a.free(other);
    try std.testing.expect(!std.mem.eql(u8, group[0..64], other[0..64]));
    const child = try partition(a, group, "turn", -4);
    defer a.free(child);
    try std.testing.expectEqualStrings(group, splitBoundary(child));
    try std.testing.expect(isAnchor(child));
    try std.testing.expect(!isAnchor("user-document"));
}

pub fn configurationOf(document: []const u8) ?[32]u8 {
    const selected = root(document) orelse return null;
    const table_end = std.mem.indexOfScalarPos(u8, selected, 64 + marker.len, '/') orelse return null;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, selected[table_end + 1 .. table_end + 65]) catch return null;
    return result;
}

pub fn documentMetadataKey(a: std.mem.Allocator, document: []const u8, suffix: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.append(a, 1);
    for (document) |byte| {
        try out.append(a, byte);
        if (byte == 0) try out.append(a, 255);
    }
    try out.appendSlice(a, &.{ 0, 0, metadata_kind });
    try out.appendSlice(a, suffix);
    return out.toOwnedSlice(a);
}
pub fn unitContextEpochKey(a: std.mem.Allocator, document: []const u8) ![]u8 {
    if (!isAnchor(document)) return error.InvalidGroupedSourceMutation;
    return documentMetadataKey(a, document, &.{11});
}
pub fn isUnitContextEpochKey(selected: []const u8) bool {
    const encoded_marker = "/\x00\xffantfly/group/v1/";
    return selected.len > 1 + 64 + encoded_marker.len + 4 and selected[0] == 1 and
        std.mem.eql(u8, selected[65..][0..encoded_marker.len], encoded_marker) and
        std.mem.eql(u8, selected[selected.len - 4 ..], &.{ 0, 0, metadata_kind, 11 });
}
pub fn tableOf(document: []const u8) ?u128 {
    const selected = root(document) orelse return null;
    const start = 64 + marker.len;
    const end = std.mem.indexOfScalarPos(u8, selected, start, '/') orelse return null;
    return std.fmt.parseInt(u128, selected[start..end], 16) catch null;
}
pub fn cellOf(document: []const u8) ?i64 {
    const selected = root(document) orelse return null;
    if (document.len < selected.len + 3 + 64 + 1 + 1 or !std.mem.startsWith(u8, document[selected.len..], "/p/")) return null;
    const start = selected.len + 3 + 64 + 1;
    const end = std.mem.indexOfScalarPos(u8, document, start, '/') orelse document.len;
    return std.fmt.parseInt(i64, document[start..end], 10) catch null;
}

pub fn unitMembershipKey(a: std.mem.Allocator, document: []const u8) ![]u8 {
    if (!isAnchor(document)) return error.InvalidGroupedSourceMutation;
    return documentMetadataKey(a, document, &.{13});
}
