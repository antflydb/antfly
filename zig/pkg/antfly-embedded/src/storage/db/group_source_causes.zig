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

//! Per-correction dependency rows. A cause can cover arbitrarily many ordinal
//! cells without growing one record or depending on the group's global clock.
//! Rows belong to the group anchor and travel with its ordinary typed metadata.
const std = @import("std");
const state = @import("group_source_state.zig");
const index = @import("group_source_index.zig");
const groups = @import("group_sources.zig");
const A = std.mem.Allocator;
pub const Dependency = struct {
    generation: u64,
    partition: index.Partition,
    revision: u64,
    horizons: []const u16 = &.{},
};
pub const Entry = struct { key: []const u8, dependency: Dependency };
fn prefix(a: A, group: state.Group, cause: groups.Digest) ![]u8 {
    return state.metadataKey(a, group, 14, &cause);
}
fn key(a: A, group: state.Group, cause: groups.Digest, partition: index.Partition) ![]u8 {
    const raw = try std.json.Stringify.valueAlloc(a, partition, .{});
    return std.mem.concat(a, u8, &.{ try prefix(a, group, cause), &groups.digest(raw) });
}
pub fn missingHorizons(a: A, txn: anytype, group: state.Group, cause: groups.Digest, generation: u64, partition: index.Partition, horizons: []const u16) ![]const u16 {
    const old = try state.readRecord(Dependency, a, txn, try key(a, group, cause, partition));
    var missing: std.ArrayList(u16) = .empty;
    for (horizons) |horizon| {
        if (old) |value| if (value.generation == generation and std.mem.indexOfScalar(u16, value.horizons, horizon) != null) continue;
        try missing.append(a, horizon);
    }
    return missing.toOwnedSlice(a);
}
pub fn enroll(a: A, txn: anytype, group: state.Group, cause: groups.Digest, generation: u64, partition: index.Partition, revision: u64, horizons: []const u16) !void {
    const selected = try key(a, group, cause, partition);
    const old = try state.readRecord(Dependency, a, txn, selected);
    var dependency = Dependency{ .generation = generation, .partition = partition, .revision = revision };
    var policies: std.ArrayList(u16) = .empty;
    if (old) |value| {
        // Coalescing a newer source event preserves its older outstanding
        // obligations, while policy deduplication starts afresh for this edit.
        dependency.revision = @max(dependency.revision, value.revision);
        if (value.generation == generation) try policies.appendSlice(a, value.horizons);
    }
    for (horizons) |horizon| if (std.mem.indexOfScalar(u16, policies.items, horizon) == null) try policies.append(a, horizon);
    dependency.horizons = policies.items;
    try state.writeRecord(a, txn, selected, dependency);
}
pub const Readiness = struct { empty: bool, retire_ready: bool };
pub fn readiness(a: A, txn: anytype, group: state.Group, cause: groups.Digest) !Readiness {
    const selected = try prefix(a, group, cause);
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    const first = (try cursor.seekAtOrAfter(selected)) orelse return .{ .empty = true, .retire_ready = false };
    if (!std.mem.startsWith(u8, first.key, selected)) return .{ .empty = true, .retire_ready = false };
    const dependency = (try state.readRecord(Dependency, a, txn, first.key)) orelse return error.GroupedSourceCorrupt;
    return .{ .empty = false, .retire_ready = try state.settled(a, txn, group, dependency.partition, dependency.revision) };
}
/// Bound both visits and decoded bytes. Copy the page before mutations so the
/// same implementation works with physical snapshots and writer overlays.
pub fn page(a: A, txn: anytype, group: state.Group, cause: groups.Digest) ![]const Entry {
    const selected = try prefix(a, group, cause);
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(selected);
    var entries: std.ArrayList(Entry) = .empty;
    var bytes: usize = 0;
    while (row) |current| {
        if (!std.mem.startsWith(u8, current.key, selected) or entries.items.len == 128) break;
        if (current.value.len > 262144) return error.GroupedSourceCorrupt;
        if (current.value.len > 262144 - bytes) break;
        bytes += current.value.len;
        const dependency = (try state.readRecord(Dependency, a, txn, current.key)) orelse return error.GroupedSourceCorrupt;
        if (dependency.generation == 0 or dependency.revision == 0 or dependency.horizons.len > 128 or !std.mem.eql(u8, dependency.partition.group, group.key) or !std.mem.eql(u8, current.key, try key(a, group, cause, dependency.partition))) return error.GroupedSourceCorrupt;
        try entries.append(a, .{ .key = try a.dupe(u8, current.key), .dependency = dependency });
        row = try cursor.next();
    }
    return entries.toOwnedSlice(a);
}
pub fn retirePage(a: A, txn: anytype, group: state.Group, cause: groups.Digest) !void {
    for (try page(a, txn, group, cause)) |entry| {
        const dependency = entry.dependency;
        if (try state.settled(a, txn, group, dependency.partition, dependency.revision)) try txn.delete(entry.key);
    }
}
