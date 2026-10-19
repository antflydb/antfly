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
const causes = @import("group_source_causes.zig");
pub const Progress = struct {
    source_revision: u64,
    event_digest: groups.Digest,
    cursor: []const u8 = "",
    segments: []const groups.Digest = &.{},
    done: bool = false,
    frontier: u64 = 0,
    enrollment: u64 = 0,
    ordinal: ?i64 = null,
    new_ordinal: ?i64 = null,
    anchors_bound: bool = false,
    scan: u8 = 0,
    causal_version: u8 = 0,
    /// An admitted correction keeps its policy bounds across catalog changes.
    horizons: ?[]const u16 = null,
};
pub const Status = struct { source_revision: u64, propagated: bool, frontier: u64, settled: bool, retire_ready: bool = false };
fn selectedKey(a: A, group: state.Group, owner: state.Owner, event: index.Invalidation) ![]u8 {
    const identity = try std.json.Stringify.valueAlloc(a, .{ owner, event.document, event.partition }, .{});
    return state.metadataKey(a, group, 12, &groups.digest(identity));
}
fn fingerprint(a: A, event: index.Invalidation) !groups.Digest {
    return index.eventDigest(a, event);
}
pub fn causeId(a: A, owner: state.Owner, event: index.Invalidation) !groups.Digest {
    return groups.digest(try selectedKey(a, .{ .definition = event.definition, .key = event.partition.group }, owner, event));
}
pub fn currentProgress(a: A, txn: anytype, owner: state.Owner, event: index.Invalidation) !?Progress {
    const group = state.Group{ .definition = event.definition, .key = event.partition.group };
    const value = (try state.readRecord(Progress, a, txn, try selectedKey(a, group, owner, event))) orelse return null;
    if (value.source_revision != event.source_revision or !std.mem.eql(u8, &value.event_digest, &(try fingerprint(a, event)))) return null;
    if (try state.readSnapshot(a, txn, group, event.partition) == null and try state.pendingFor(a, txn, group, event.partition) == null) return null;
    return value;
}
pub fn status(a: A, txn: anytype, owner: state.Owner, event: index.Invalidation) !?Status {
    const value = (try currentProgress(a, txn, owner, event)) orelse return null;
    const group = state.Group{ .definition = event.definition, .key = event.partition.group };
    const propagated = value.done and value.causal_version == 1;
    const readiness = if (propagated) try causes.readiness(a, txn, group, try causeId(a, owner, event)) else causes.Readiness{ .empty = false, .retire_ready = false };
    return .{ .source_revision = value.source_revision, .propagated = propagated, .frontier = value.frontier, .settled = propagated and readiness.empty, .retire_ready = readiness.retire_ready };
}
pub fn advance(a: A, txn: anytype, owner: state.Owner, event: index.Invalidation, horizon: u16) !void {
    return advanceHorizons(a, txn, owner, event, if (horizon == 0) &.{} else &.{horizon});
}
pub fn advanceHorizons(a: A, txn: anytype, owner: state.Owner, event: index.Invalidation, horizons: []const u16) !void {
    if (horizons.len > 128) return error.InvalidGroupedPriorContext;
    for (horizons) |value| {
        if (value == 0 or value > 128) return error.InvalidGroupedPriorContext;
    }
    const group = state.Group{ .definition = event.definition, .key = event.partition.group };
    const selected = try selectedKey(a, group, owner, event);
    const digest = try fingerprint(a, event);
    var progress = (try state.readRecord(Progress, a, txn, selected)) orelse Progress{ .source_revision = event.source_revision, .event_digest = digest };
    if (progress.source_revision > event.source_revision) return;
    const before = try state.readSnapshot(a, txn, group, event.partition);
    const published = progress.anchors_bound or (before != null and before.?.revision >= progress.enrollment);
    if (progress.source_revision < event.source_revision and progress.causal_version == 1 and !progress.done and published) {
        // Drain published neighborhoods before replacing their cursor. An
        // unmaterialized event can coalesce immediately: no intermediate
        // position became visible, and deferring its newer delivery could
        // block the fan-in needed to materialize the original event itself.
        var original = event;
        original.repair = false;
        try advanceProgress(a, txn, owner, original, horizons, group, selected, &progress);
        return;
    }
    const revision = try state.invalidate(a, txn, owner, event);
    if (progress.source_revision < event.source_revision) progress = .{ .source_revision = event.source_revision, .event_digest = digest };
    if (!std.mem.eql(u8, &progress.event_digest, &digest)) return error.GroupedSourceCorrupt;
    if (progress.enrollment != revision or progress.causal_version != 1) progress = .{ .source_revision = event.source_revision, .event_digest = digest, .enrollment = revision, .causal_version = 1 };
    if (progress.ordinal == null) {
        // Queue ordinals are scheduling lower bounds and can retain much older
        // moves. Propagation needs the actual last published position instead.
        progress.ordinal = if (before) |snapshot| (if (snapshot.units.len > 0) snapshot.ordinal else event.ordinal) else event.ordinal;
    }
    try advanceProgress(a, txn, owner, event, horizons, group, selected, &progress);
}
fn advanceProgress(a: A, txn: anytype, owner: state.Owner, event: index.Invalidation, horizons: []const u16, group: state.Group, selected: []const u8, progress: *Progress) !void {
    const policies = progress.horizons orelse horizons;
    progress.horizons = policies;
    var horizon: u16 = 0;
    for (policies) |value| horizon = @max(horizon, value);
    const revision = progress.enrollment;
    const cause = try causeId(a, owner, event);
    if (progress.done) return causes.retirePage(a, txn, group, cause);
    try causes.enroll(a, txn, group, cause, revision, event.partition, revision, &.{});
    progress.frontier = @max(progress.frontier, revision); // Diagnostic only.
    if (horizon != 0 and !event.repair and !progress.anchors_bound) {
        const snapshot = try state.readSnapshot(a, txn, group, event.partition);
        if (snapshot == null or snapshot.?.revision < revision) {
            try state.writeRecord(a, txn, selected, progress.*);
            return;
        }
        progress.new_ordinal = if (snapshot.?.units.len > 0) snapshot.?.ordinal else null;
        progress.anchors_bound = true;
    }
    if (horizon == 0 or event.repair) {
        progress.done = true;
        progress.frontier = revision;
    } else {
        const prefix = try state.directoryPrefix(a, group);
        // Enroll the earlier neighborhood first. A worker may run between
        // bounded scans; a backward move must make its new predecessor work
        // visible before allowing the old, later successors to publish.
        const old_ordinal = progress.ordinal.?;
        const new_ordinal = progress.new_ordinal orelse old_ordinal;
        const ordinal = if (progress.scan == 0) @min(old_ordinal, new_ordinal) else @max(old_ordinal, new_ordinal);
        const start = if (progress.cursor.len == 0) try state.directoryOrdinalBound(a, group, ordinal, false) else try reads.decode(a, progress.cursor);
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
            if (@import("group_prior_context.zig").Position.before(.{ .ordinal = ordinal, .segment = event.partition.segment }, .{ .ordinal = entry.snapshot.ordinal, .segment = entry.partition.segment }) and !std.mem.eql(u8, entry.partition.segment, event.partition.segment)) {
                const segment = groups.digest(entry.partition.segment);
                var distance = seen.items.len + 1;
                var known = false;
                for (seen.items, 0..) |previous, i| if (std.mem.eql(u8, &previous, &segment)) {
                    distance = i + 1;
                    known = true;
                    break;
                };
                if (!known) {
                    if (seen.items.len >= horizon) {
                        progress.done = true;
                        break;
                    }
                    try seen.append(a, segment);
                }
                var affected: [128]u16 = undefined;
                var count: usize = 0;
                for (policies) |value| if (value >= distance) {
                    affected[count] = value;
                    count += 1;
                };
                const missing = try causes.missingHorizons(a, txn, group, cause, revision, entry.partition, affected[0..count]);
                if (missing.len > 0) {
                    const required = try state.invalidateContextHorizons(a, txn, group, entry.partition, entry.snapshot.ordinal, missing);
                    try causes.enroll(a, txn, group, cause, revision, entry.partition, required, missing);
                    progress.frontier = @max(progress.frontier, required);
                }
            }
            row = try cursor.next();
        }
        progress.segments = try seen.toOwnedSlice(a);
        if (row == null or !std.mem.startsWith(u8, row.?.key, prefix)) progress.done = true;
        if (progress.done and progress.scan == 0 and progress.new_ordinal != null and progress.new_ordinal.? != progress.ordinal.?) {
            // A move has two independently bounded neighborhoods. Persist the
            // second cursor; dependency rows deduplicate overlapping policies.
            progress.scan = 1;
            progress.cursor = "";
            progress.segments = &.{};
            progress.done = false;
        }
    }
    try state.writeRecord(a, txn, selected, progress.*);
}
