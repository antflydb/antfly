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

//! Original-event context propagation. A durable cursor pages even a very
//! large segment; recomputed outputs never create another propagation seed.
const std = @import("std");
const groups = @import("group_sources.zig");
const index = @import("group_source_index.zig");
const state = @import("group_source_state.zig");
const reads = @import("group_source_reads.zig");
const A = std.mem.Allocator;
pub const Progress = struct {
    source_revision: u64,
    event_digest: groups.Digest,
    cursor: []const u8 = "",
    segments: []const groups.Digest = &.{},
    done: bool = false,
    frontier: u64 = 0,
    enrollment: u64 = 0,
    ordinal: ?i64 = null,
};
pub const Status = struct { source_revision: u64, propagated: bool, frontier: u64, settled: bool };
fn selectedKey(a: A, group: state.Group, owner: state.Owner, event: index.Invalidation) ![]u8 {
    const identity = try std.json.Stringify.valueAlloc(a, .{ owner, event.document, event.partition }, .{});
    return state.metadataKey(a, group, 12, &groups.digest(identity));
}
fn fingerprint(a: A, event: index.Invalidation) !groups.Digest {
    return index.eventDigest(a, event);
}
pub fn status(a: A, txn: anytype, owner: state.Owner, event: index.Invalidation) !?Status {
    const group = state.Group{ .definition = event.definition, .key = event.partition.group };
    const value = (try state.readRecord(Progress, a, txn, try selectedKey(a, group, owner, event))) orelse return null;
    if (value.source_revision != event.source_revision or !std.mem.eql(u8, &value.event_digest, &(try fingerprint(a, event)))) return null;
    if (try state.readSnapshot(a, txn, group, event.partition) == null and try state.pendingFor(a, txn, group, event.partition) == null) return null;
    return .{ .source_revision = value.source_revision, .propagated = value.done, .frontier = value.frontier, .settled = value.done and try state.frontierSettled(a, txn, group, value.frontier) };
}
pub fn advance(a: A, txn: anytype, owner: state.Owner, event: index.Invalidation, horizon: u16) !void {
    if (horizon > 128) return error.InvalidGroupedPriorContext;
    const revision = try state.invalidate(a, txn, owner, event);
    const group = state.Group{ .definition = event.definition, .key = event.partition.group };
    const selected = try selectedKey(a, group, owner, event);
    const digest = try fingerprint(a, event);
    var progress = (try state.readRecord(Progress, a, txn, selected)) orelse Progress{ .source_revision = event.source_revision, .event_digest = digest };
    if (progress.source_revision > event.source_revision) return;
    if (progress.source_revision < event.source_revision) progress = .{ .source_revision = event.source_revision, .event_digest = digest };
    if (!std.mem.eql(u8, &progress.event_digest, &digest)) return error.GroupedSourceCorrupt;
    if (progress.enrollment != revision) progress = .{ .source_revision = event.source_revision, .event_digest = digest, .enrollment = revision };
    if (progress.done) return;
    if (horizon != 0 and !event.repair and progress.ordinal == null) {
        // New segments acquire their global first-member ordinal from fan-in.
        // Enroll first, then bind the finite interval after materialization;
        // delivery order must not choose a different replay horizon.
        const snapshot = (try state.readSnapshot(a, txn, group, event.partition)) orelse {
            try state.writeRecord(a, txn, selected, progress);
            return;
        };
        progress.ordinal = if (snapshot.units.len != 0 or snapshot.revision == revision) @min(event.ordinal, snapshot.ordinal) else event.ordinal;
    }
    if (horizon == 0 or event.repair) {
        progress.done = true;
        progress.frontier = revision;
    } else {
        const prefix = try state.directoryPrefix(a, group);
        const start = if (progress.cursor.len == 0) try state.directoryOrdinalBound(a, group, progress.ordinal orelse event.ordinal, false) else try reads.decode(a, progress.cursor);
        if (!std.mem.startsWith(u8, start, prefix)) return error.GroupedSourceCorrupt;
        var cursor = try txn.openPhysicalCursorAdapter();
        defer cursor.close();
        var row = try cursor.seekAtOrAfter(start);
        if (progress.cursor.len > 0 and row != null and std.mem.eql(u8, row.?.key, start)) row = try cursor.next();
        var seen: std.ArrayList(groups.Digest) = .empty;
        try seen.appendSlice(a, progress.segments);
        var visits: usize = 0;
        while (row) |current| {
            if (!std.mem.startsWith(u8, current.key, prefix)) break;
            if (visits == 128) break;
            visits += 1;
            const entry = try state.directoryEntry(a, txn, group, current.key);
            progress.cursor = try reads.encode(a, current.key);
            if (@import("group_prior_context.zig").Position.before(.{ .ordinal = progress.ordinal orelse event.ordinal, .segment = event.partition.segment }, .{ .ordinal = entry.snapshot.ordinal, .segment = entry.partition.segment }) and !std.mem.eql(u8, entry.partition.segment, event.partition.segment)) {
                const segment = groups.digest(entry.partition.segment);
                var known = false;
                for (seen.items) |previous| if (std.mem.eql(u8, &previous, &segment)) {
                    known = true;
                    break;
                };
                if (!known) {
                    if (seen.items.len == horizon) {
                        progress.done = true;
                        break;
                    }
                    try seen.append(a, segment);
                }
                try state.invalidateContext(a, txn, group, &.{.{ .partition = entry.partition, .ordinal = entry.snapshot.ordinal }}, 1);
            }
            row = try cursor.next();
        }
        progress.segments = try seen.toOwnedSlice(a);
        if (row == null or !std.mem.startsWith(u8, row.?.key, prefix)) progress.done = true;
        if (progress.done) progress.frontier = (try state.currentHead(a, txn, group)).revision;
    }
    try state.writeRecord(a, txn, selected, progress);
}
