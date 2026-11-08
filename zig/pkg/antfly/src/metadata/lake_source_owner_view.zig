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

//! Compact native owner projection. Fresh queries must not transfer or decode
//! retained publication history just to obtain the accepted serving watermark.
const std = @import("std");
const admission = @import("lake_source_admission.zig");
const A = std.mem.Allocator;
pub const max_bytes = 32 * 1024;
pub const View = struct { version: u8 = 1, dropped: bool = false, owner: ?admission.State = null };
pub fn encode(a: A, view: View) ![]u8 {
    if (view.version != 1) return error.InvalidLakeIndexLifecycle;
    if (view.owner) |owner| try owner.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, view, .{ .emit_null_optional_fields = false });
    errdefer a.free(bytes);
    if (bytes.len > max_bytes) return error.InvalidLakeIndexLifecycle;
    return bytes;
}
pub fn parse(a: A, bytes: []const u8) !View {
    if (bytes.len > max_bytes) return error.InvalidLakeIndexLifecycle;
    const view = try std.json.parseFromSliceLeaky(View, a, bytes, .{ .allocate = .alloc_always });
    if (view.version != 1) return error.InvalidLakeIndexLifecycle;
    if (view.owner) |owner| try owner.validate();
    return view;
}
/// Restored old checkpoints can lack the materialized projection. Extract the
/// small view on the owner; never return publication history to the query node.
pub fn fromLifecycle(a: A, bytes: []const u8) !View {
    const state = try std.json.parseFromSliceLeaky(struct { version: u16 = 1, dropped: bool = false, source_owner: ?admission.State = null }, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    if ((state.version != 1 and state.version != 2) or (state.source_owner != null and state.version != 2)) return error.InvalidLakeIndexLifecycle;
    if (state.source_owner) |owner| try owner.validate();
    return .{ .dropped = state.dropped, .owner = state.source_owner };
}
pub fn readOnService(svc: anytype, a: A, table_id: u64, request: anytype) ![]u8 {
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.getLakeSourceOwner(a, svc.metadata_group_id, table_id);
}
test "native source owner projection excludes retained publication history" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .version = 2, .dropped = false, .source_owner = admission.State{ .kind = .managed_iceberg, .owner = @splat(1), .signing_key = @splat(2), .object_generation = 3, .source = @splat(4) }, .publications = [_][]const u8{"large-history-is-not-a-query-result"} }, .{});
    const view = try fromLifecycle(a, bytes);
    const projected = try encode(a, view);
    try std.testing.expect(std.mem.indexOf(u8, projected, "publications") == null);
    try std.testing.expectEqual(@as(u64, 3), (try parse(a, projected)).owner.?.object_generation);
    try std.testing.expect((try fromLifecycle(a, "{\"version\":1,\"publications\":[]}")).owner == null);
}

test "native source owner projection rejects owners in legacy lifecycle records" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .version = 1, .source_owner = admission.State{ .kind = .managed_iceberg, .owner = @splat(1), .signing_key = @splat(2), .object_generation = 3, .source = @splat(4) } }, .{});
    try std.testing.expectError(error.InvalidLakeIndexLifecycle, fromLifecycle(a, bytes));
    try std.testing.expectError(error.InvalidLakeIndexLifecycle, parse(a, "{\"version\":2}"));
}
