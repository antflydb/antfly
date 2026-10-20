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

//! Reconstructible, point-coalesced work directories. The document-owned
//! outbox/group state remains authoritative across range transfer; these
//! indexes avoid rescanning the source table on every append or worker tick.
const std = @import("std");
const A = std.mem.Allocator;
pub const source_prefix = "\x00\x00G\x03";
pub const group_prefix = "\x00\x00G\x04";
fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
pub fn sourceKey(a: A, outbox_key: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ source_prefix, &digest(outbox_key) });
}
pub fn groupKey(a: A, anchor: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ group_prefix, &digest(anchor) });
}
pub fn checksum(selected: []const u8, bytes: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(selected);
    hash.update(bytes);
    return hash.finalResult();
}
pub fn encode(a: A, selected: []const u8, pointer: []const u8) ![]u8 {
    if (pointer.len == 0 or pointer.len > 32768) return error.GroupedSourceCorrupt;
    return std.mem.concat(a, u8, &.{ "GSQ1", pointer, &checksum(selected, pointer) });
}
pub fn decode(selected: []const u8, raw: []const u8) ![]const u8 {
    if (raw.len <= 36 or raw.len > 32804 or !std.mem.eql(u8, raw[0..4], "GSQ1") or !std.mem.eql(u8, raw[raw.len - 32 ..], &checksum(selected, raw[4 .. raw.len - 32]))) return error.GroupedSourceCorrupt;
    return raw[4 .. raw.len - 32];
}
pub fn stageSource(a: A, txn: anytype, outbox_key: []const u8) !void {
    const selected = try sourceKey(a, outbox_key);
    defer a.free(selected);
    const encoded = try encode(a, selected, outbox_key);
    defer a.free(encoded);
    try txn.put(selected, encoded);
}
pub fn stageGroup(a: A, txn: anytype, anchor: []const u8, descriptor: anytype) !void {
    const selected = try groupKey(a, anchor);
    defer a.free(selected);
    const bytes = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(bytes);
    const encoded = try encode(a, selected, bytes);
    defer a.free(encoded);
    try txn.put(selected, encoded);
}
pub fn removeSource(a: A, txn: anytype, outbox_key: []const u8) !void {
    const selected = try sourceKey(a, outbox_key);
    defer a.free(selected);
    try txn.delete(selected);
}
pub fn removeGroup(a: A, txn: anytype, anchor: []const u8) !void {
    const selected = try groupKey(a, anchor);
    defer a.free(selected);
    try txn.delete(selected);
}
