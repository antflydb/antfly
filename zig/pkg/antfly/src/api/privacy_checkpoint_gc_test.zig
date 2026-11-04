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

const std = @import("std");
const Store = @import("../metadata/storage/raft_apply_store.zig").RaftApplyStore;
const checkpoints = @import("antfly_local_sources").privacy_catalog.checkpoints;

test "privacy checkpoint GC page does not copy a large table for each header" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/privacy-gc-budget", .{tmp.sub_path});
    var counter = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var store = try Store.init(counter.allocator(), .{ .root_dir = root });
    defer store.deinit();
    const description = try a.alloc(u8, 8 * 1024 * 1024);
    @memset(description, 'x');
    var refs: [32]checkpoints.Ref = undefined;
    var tokens: [32][101]u8 = undefined;
    for (&refs, &tokens, 0..) |*ref, *token, i| {
        var generation: [16]u8 = @splat(0);
        generation[0] = @intCast(i);
        ref.* = .{ .digest = @splat('a'), .generation = std.fmt.bytesToHex(generation, .lower) };
        token.* = ref.token();
    }
    var token_views: [32][]const u8 = undefined;
    for (&tokens, &token_views) |*token, *view| view.* = token[0..];
    const indexes = try std.json.Stringify.valueAlloc(a, .{ .hold = token_views }, .{});
    try store.applyStandaloneCommand(7303, .{ .upsert_table = .{ .table_id = 1, .name = "gc", .description = description, .indexes_json = indexes } });
    const header = try std.json.Stringify.valueAlloc(a, checkpoints.Header{ .length = 1, .created_at_millis = 1 }, .{});
    for (refs) |ref| {
        var buffer: [128]u8 = undefined;
        try store.applyStandaloneCommand(7303, .{ .compare_and_set_job_state = .{ .key = try ref.storageKey(&buffer, 1, null), .expected_hash = "", .value = header } });
    }
    const before = counter.allocated_bytes;
    const bytes = try store.scanJobState(counter.allocator(), 7303, "privacy/m/");
    defer counter.allocator().free(bytes);
    const allocated = counter.allocated_bytes - before;
    // One pinned table read plus bounded page framing, rather than 32 deep
    // copies of the 8 MiB description retained until the GC page completes.
    try std.testing.expect(allocated < 32 * 1024 * 1024);
    const page = try std.json.parseFromSlice(@import("../metadata/job_state.zig").Page, a, bytes, .{});
    try std.testing.expectEqual(@as(usize, 0), page.value.rows.len);
    try std.testing.expect(page.value.next != null);
}
