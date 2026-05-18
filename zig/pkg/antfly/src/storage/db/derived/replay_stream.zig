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
const backend_types = @import("../../backend_types.zig");
const docstore_mod = @import("../../docstore.zig");
const mem_backend_mod = @import("../../mem_backend.zig");

pub const Entry = backend_types.ReplayEntry;

pub fn nextReplaySequence(store: *docstore_mod.DocStore, fallback_next: u64) u64 {
    return store.nextReplaySequence(fallback_next);
}

pub fn lastReplaySequence(store: *docstore_mod.DocStore, fallback_last: u64) u64 {
    return store.lastReplaySequence(fallback_last);
}

pub fn appendReplayOpaque(alloc: Allocator, store: *docstore_mod.DocStore, sequence: u64, payload: []const u8) !void {
    try store.appendReplayOpaque(alloc, sequence, payload);
}

pub fn truncateReplayUpTo(alloc: Allocator, store: *docstore_mod.DocStore, up_to_sequence: u64) !void {
    try store.truncateReplayUpTo(alloc, up_to_sequence);
}

pub fn iterateReplayFrom(alloc: Allocator, store: *docstore_mod.DocStore, from_sequence: u64) ![]Entry {
    return try store.iterateReplayFrom(alloc, from_sequence);
}

pub fn nextAppendSequence(store: *docstore_mod.DocStore, fallback_next: u64) u64 {
    return nextReplaySequence(store, fallback_next);
}

pub fn lastSequence(store: *docstore_mod.DocStore, fallback_last: u64) u64 {
    return lastReplaySequence(store, fallback_last);
}

pub fn appendOpaque(alloc: Allocator, store: *docstore_mod.DocStore, sequence: u64, payload: []const u8) !void {
    try appendReplayOpaque(alloc, store, sequence, payload);
}

pub fn truncate(alloc: Allocator, store: *docstore_mod.DocStore, up_to_sequence: u64) !void {
    try truncateReplayUpTo(alloc, store, up_to_sequence);
}

pub fn iterateFrom(alloc: Allocator, store: *docstore_mod.DocStore, from_sequence: u64) ![]Entry {
    return try iterateReplayFrom(alloc, store, from_sequence);
}

test "replay stream append iterate and truncate" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    try appendReplayOpaque(alloc, &store, 1, "one");
    try appendReplayOpaque(alloc, &store, 2, "two");
    try std.testing.expectEqual(@as(u64, 2), lastReplaySequence(&store, 0));
    try std.testing.expectEqual(@as(u64, 3), nextReplaySequence(&store, 1));

    const entries = try iterateReplayFrom(alloc, &store, 2);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 2), entries[0].sequence);
    try std.testing.expectEqualStrings("two", entries[0].payload);

    try truncateReplayUpTo(alloc, &store, 1);
    const after = try iterateReplayFrom(alloc, &store, 1);
    defer {
        for (after) |*entry| entry.deinit(alloc);
        alloc.free(after);
    }
    try std.testing.expectEqual(@as(usize, 1), after.len);
    try std.testing.expectEqual(@as(u64, 2), after[0].sequence);
}
