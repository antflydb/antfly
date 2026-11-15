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

//! Private continuation for ROOT-LOCAL projected transactions. A partial frame
//! remembers whether a preceding page exposed any authorized row. This permits
//! a later empty page to complete a relevant transaction without manufacturing
//! completion markers for entirely hidden transactions. Coordinated transaction
//! completion requires a separate qualified participant/outcome contract.
const std = @import("std");
const control = @import("changefeed_control.zig");
const A = std.mem.Allocator;
const Digest = control.Digest;
const base64 = std.base64.url_safe_no_pad;
const fixed_bytes = 4 + 32 + 32 + 2;
const max_raw_bytes = fixed_bytes + (control.max_roots + 7) / 8 + 32;
pub const max_text_bytes = base64.Encoder.calcSize(max_raw_bytes);
fn digest(raw: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &out, .{});
    return out;
}
fn positions(roots: []const control.Root) !Digest {
    if (roots.len == 0 or roots.len > control.max_roots) return error.InvalidChangefeed;
    var sha = std.crypto.hash.sha2.Sha256.init(.{});
    sha.update("antfly:local-projection-positions:v1");
    for (roots, 0..) |root, index| {
        if (std.mem.allEqual(u8, &root.namespace, 0) or root.delivered.event >= 65536 or
            (root.delivered.event != 0 and root.delivered.sequence == std.math.maxInt(u64)) or
            (index != 0 and std.mem.order(u8, &roots[index - 1].namespace, &root.namespace) != .lt)) return error.InvalidChangefeed;
        sha.update(&root.namespace);
        var position: [12]u8 = undefined;
        std.mem.writeInt(u64, position[0..8], root.delivered.sequence, .little);
        std.mem.writeInt(u32, position[8..12], root.delivered.event, .little);
        sha.update(&position);
    }
    return sha.finalResult();
}
fn partial(roots: []const control.Root) bool {
    for (roots) |root| if (root.delivered.event != 0) return true;
    return false;
}
/// Scope binds the immutable feed definition/consumer epoch. The returned text
/// belongs in the private delivery receipt; never expose these bits publicly.
pub fn encode(a: A, scope: Digest, roots: []const control.Root, visible: []const bool) ![]u8 {
    const frontier = try positions(roots);
    if (std.mem.allEqual(u8, &scope, 0) or visible.len != roots.len) return error.InvalidChangefeed;
    for (roots, visible) |root, seen| if (root.delivered.event == 0 and seen) return error.InvalidChangefeed;
    if (!partial(roots)) return a.dupe(u8, "");
    const raw = try a.alloc(u8, fixed_bytes + (roots.len + 7) / 8 + 32);
    defer a.free(raw);
    @memset(raw, 0);
    @memcpy(raw[0..4], "CFV1");
    @memcpy(raw[4..36], &scope);
    @memcpy(raw[36..68], &frontier);
    std.mem.writeInt(u16, raw[68..70], @intCast(roots.len), .little);
    for (visible, 0..) |seen, index| if (seen) {
        raw[fixed_bytes + index / 8] |= @as(u8, 1) << @intCast(index % 8);
    };
    @memcpy(raw[raw.len - 32 ..], &digest(raw[0 .. raw.len - 32]));
    const out = try a.alloc(u8, base64.Encoder.calcSize(raw.len));
    _ = base64.Encoder.encode(out, raw);
    return out;
}
pub fn decode(a: A, scope: Digest, roots: []const control.Root, text: []const u8) ![]bool {
    const frontier = try positions(roots);
    if (std.mem.allEqual(u8, &scope, 0)) return error.InvalidChangefeed;
    if (text.len == 0) {
        // A legacy cursor in the middle of a frame proves no visibility state.
        if (partial(roots)) return error.ChangefeedResynchronizationRequired;
        const out = try a.alloc(bool, roots.len);
        @memset(out, false);
        return out;
    }
    if (!partial(roots) or text.len > max_text_bytes) return error.ChangefeedResynchronizationRequired;
    const size = base64.Decoder.calcSizeForSlice(text) catch return error.ChangefeedResynchronizationRequired;
    if (size != fixed_bytes + (roots.len + 7) / 8 + 32) return error.ChangefeedResynchronizationRequired;
    var raw: [max_raw_bytes]u8 = undefined;
    const bytes = raw[0..size];
    base64.Decoder.decode(bytes, text) catch return error.ChangefeedResynchronizationRequired;
    if (!std.mem.eql(u8, bytes[0..4], "CFV1") or !std.mem.eql(u8, bytes[4..36], &scope) or !std.mem.eql(u8, bytes[36..68], &frontier) or
        std.mem.readInt(u16, bytes[68..70], .little) != roots.len or !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.ChangefeedResynchronizationRequired;
    var canonical: [max_text_bytes]u8 = undefined;
    if (!std.mem.eql(u8, text, base64.Encoder.encode(canonical[0..text.len], bytes))) return error.ChangefeedResynchronizationRequired;
    const out = try a.alloc(bool, roots.len);
    errdefer a.free(out);
    for (roots, out, 0..) |root, *seen, index| {
        seen.* = bytes[fixed_bytes + index / 8] & (@as(u8, 1) << @intCast(index % 8)) != 0;
        if (root.delivered.event == 0 and seen.*) return error.ChangefeedResynchronizationRequired;
    }
    const tail = roots.len % 8;
    if (tail != 0 and bytes[bytes.len - 33] >> @intCast(tail) != 0) return error.ChangefeedResynchronizationRequired;
    return out;
}
fn testRoot(table: u64, event: u32) control.Root {
    var ns: control.Namespace = @splat(0);
    std.mem.writeInt(u64, ns[0..8], table, .big);
    return .{ .namespace = ns, .ownership_epoch = 1, .grant_sequence = 1, .consumer_epoch = 1, .delivered = .{ .sequence = 7, .event = event } };
}
test "projection continuation distinguishes hidden partial frames and binds scope and exact frontier" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const roots = [_]control.Root{ testRoot(1, 3), testRoot(2, 5), testRoot(3, 0) };
    const encoded = try encode(a, @splat(1), &roots, &.{ true, false, false });
    const flags = try decode(a, @splat(1), &roots, encoded);
    try std.testing.expectEqualSlices(bool, &.{ true, false, false }, flags);
    const hidden = try encode(a, @splat(1), &roots, &.{ false, false, false });
    try std.testing.expect(hidden.len != 0);
    try std.testing.expect(!(try decode(a, @splat(1), &roots, hidden))[0]);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, decode(a, @splat(1), &roots, ""));
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, decode(a, @splat(2), &roots, encoded));
    var moved = roots;
    moved[0].delivered.event += 1;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, decode(a, @splat(1), &moved, encoded));
    var renamed = roots;
    renamed[0].ownership_epoch = 2;
    renamed[0].grant_sequence = 100;
    try std.testing.expect((try decode(a, @splat(1), &renamed, encoded))[0]);
}
test "projection continuation uses fixed space for every root and clears complete-frame visibility" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const roots = try a.alloc(control.Root, control.max_roots);
    const flags = try a.alloc(bool, control.max_roots);
    @memset(flags, false);
    for (roots, 0..) |*root, i| root.* = testRoot(i + 1, 1);
    flags[control.max_roots - 1] = true;
    const text = try encode(a, @splat(1), roots, flags);
    try std.testing.expectEqual(max_text_bytes, text.len);
    try std.testing.expect((try decode(a, @splat(1), roots, text))[control.max_roots - 1]);
    roots[control.max_roots - 1].delivered = .{ .sequence = 8 };
    try std.testing.expectError(error.InvalidChangefeed, encode(a, @splat(1), roots, flags));
    const complete = [_]control.Root{testRoot(1, 0)};
    try std.testing.expectEqualStrings("", try encode(a, @splat(1), &complete, &.{false}));
    try std.testing.expect(!(try decode(a, @splat(1), &complete, ""))[0]);
    var corrupt = try a.dupe(u8, text);
    corrupt[corrupt.len / 2] = if (corrupt[corrupt.len / 2] == 'A') 'B' else 'A';
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, decode(a, @splat(1), roots, corrupt));
}
