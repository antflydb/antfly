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

//! Bounded first-page admission. Large payloads decline their cache slot; they
//! retain physical work and remain readable through ordinary scalar pages.
const std = @import("std");
const b = @import("backend.zig");
const v = @import("value.zig");
const wire = @import("wire.zig");
pub const max_nodes = 16;
pub const max_bytes = 1 << 20;
pub fn pageBytes(page: b.Page(*const v.Relationship)) !usize {
    var size = std.Io.Writer.Discarding.init(&.{});
    try size.writer.writeAll("[");
    for (page.items) |item| try std.json.Stringify.value(wire.Value{ .value = .{ .relationship = item }, .include_native = true }, .{}, &size.writer);
    try std.json.Stringify.value(page.next, .{}, &size.writer);
    return size.fullCount() + 256;
}
pub fn clone(a: std.mem.Allocator, page: b.Page(*const v.Relationship)) !b.Page(*const v.Relationship) {
    const items = try a.alloc(*const v.Relationship, page.items.len);
    for (page.items, items) |item, *out| out.* = (try (v.Value{ .relationship = item }).clone(a)).relationship;
    return .{ .items = items, .next = if (page.next) |next| try a.dupe(u8, next) else null, .examined = page.examined };
}
pub fn run(a: std.mem.Allocator, ptr: *anyopaque, mapping: @import("catalog.zig").Relationship, identities: []const v.Identity, direction: @import("ast.zig").Direction, limit: usize, maximum: usize, work: ?b.Work, comptime read: anytype) !?b.ExpansionBatch {
    if (identities.len == 0 or identities.len > max_nodes or limit == 0 or limit > 128 or maximum < identities.len) return null;
    const pages = try a.alloc(?b.Page(*const v.Relationship), identities.len);
    @memset(pages, null);
    var bytes: usize = 0;
    var examined: usize = 0;
    for (identities, pages, 0..) |identity, *out, index| {
        var arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer arena.deinit();
        const allowance = (maximum - examined) / (identities.len - index);
        const page = try read(ptr, arena.allocator(), mapping, identity, direction, limit, allowance);
        const used = page.examined orelse page.items.len;
        if (used > allowance or used < page.items.len or page.items.len > limit) return error.CypherInvalidBackendPage;
        examined += used;
        for (page.items) |item| if (work) |meter| try meter.value(meter.ptr, .{ .relationship = item });
        const needed = try pageBytes(page);
        if (needed > max_bytes - bytes) continue;
        out.* = try clone(a, page);
        bytes += needed;
    }
    return .{ .pages = pages, .examined = examined };
}
