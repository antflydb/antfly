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
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const persistent_mod = @import("../../persistent.zig");
const merger_mod = @import("../../../merger.zig");
const index_mod = @import("../../../index.zig");

pub fn needsMerge(
    alloc: Allocator,
    index: *persistent_mod.PersistentIndex,
    policy: merger_mod.MergePolicy,
) !bool {
    const snap = index.snapshot();
    if (snap.segments.len < 2) return false;

    const infos = try buildSegmentInfosAlloc(alloc, snap);
    defer alloc.free(infos);

    const planned = (try policy.plan(alloc, infos)) orelse return false;
    alloc.free(planned);
    return true;
}

pub fn planPolicyMergeAlloc(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    policy: merger_mod.MergePolicy,
) !?[]usize {
    if (snap.segments.len < 2) return null;

    const infos = try buildSegmentInfosAlloc(alloc, snap);
    defer alloc.free(infos);
    return try policy.plan(alloc, infos);
}

pub fn planForceCompactAlloc(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    max_segments_at_once: usize,
) ![]usize {
    const plan_len = @min(snap.segments.len, max_segments_at_once);
    const candidates = try buildSegmentInfosAlloc(alloc, snap);
    defer alloc.free(candidates);

    std.mem.sort(merger_mod.SegmentInfo, candidates, {}, struct {
        fn lessThan(_: void, a: merger_mod.SegmentInfo, b: merger_mod.SegmentInfo) bool {
            if (a.has_deletions != b.has_deletions) return a.has_deletions;
            if (a.size != b.size) return a.size < b.size;
            return a.index < b.index;
        }
    }.lessThan);

    const planned = try alloc.alloc(usize, plan_len);
    for (planned, 0..) |*seg_idx, i| seg_idx.* = candidates[i].index;
    return planned;
}

pub fn applyPlannedMerge(
    alloc: Allocator,
    index: *persistent_mod.PersistentIndex,
    snap: *const index_mod.IndexSnapshot,
    planned: []const usize,
    target_segment_bytes: u64,
    merge_error_prefix: []const u8,
    apply_error_prefix: []const u8,
) !bool {
    const old_ids = try alloc.alloc(u64, planned.len);
    defer alloc.free(old_ids);
    for (planned, 0..) |seg_idx, i| {
        old_ids[i] = snap.segments[seg_idx].id;
    }

    if (index.prepareMergedSegmentToFile(snap, planned)) |prepared| {
        return index.replaceSegmentsIfActiveManyPrepared(old_ids, prepared) catch |err| switch (err) {
            error.EmptySegment => try index.removeSegmentsIfActive(old_ids),
            else => {
                logErr(apply_error_prefix, err);
                return err;
            },
        };
    } else |err| switch (err) {
        error.Unsupported => {},
        else => {
            logErr(merge_error_prefix, err);
            return err;
        },
    }

    var merged = merger_mod.mergeSegmentsBounded(alloc, snap, planned, .{
        .target_segment_bytes = @intCast(target_segment_bytes),
    }) catch |err| {
        logErr(merge_error_prefix, err);
        return err;
    };
    errdefer merger_mod.freeMergedSegments(alloc, merged);

    const applied = index.replaceSegmentsIfActiveManyOwned(old_ids, merged) catch |err| {
        merged = &.{};
        if (err == error.EmptySegment) {
            return try index.removeSegmentsIfActive(old_ids);
        }
        logErr(apply_error_prefix, err);
        return err;
    };
    merged = &.{};
    return applied;
}

fn buildSegmentInfosAlloc(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
) ![]merger_mod.SegmentInfo {
    const infos = try alloc.alloc(merger_mod.SegmentInfo, snap.segments.len);
    for (snap.segments, 0..) |seg, i| {
        infos[i] = .{
            .index = i,
            .size = seg.data.bytes().len,
            .doc_count = seg.reader.doc_count,
            .deleted_count = if (seg.deleted) |deleted| @intCast(deleted.cardinality()) else 0,
            .has_deletions = seg.deleted != null,
        };
    }
    return infos;
}

fn logErr(prefix: []const u8, err: anyerror) void {
    if (builtin.os.tag != .freestanding) {
        std.log.err("{s}: {s}", .{ prefix, @errorName(err) });
    }
}
