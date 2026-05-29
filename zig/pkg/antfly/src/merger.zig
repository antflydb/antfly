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

//! Background tiered merge for multi-segment indexes.
//!
//! Implements a tiered merge policy (smallest segments first) with
//! direct-copy optimization: segments without deletions can have their
//! section bytes copied verbatim, only renumbering doc IDs in bitmaps.

const std = @import("std");
const Allocator = std.mem.Allocator;
const index_mod = @import("index.zig");
const persistent_mod = @import("storage/persistent.zig");
const segment_mod = @import("segment.zig");
const typed_dv = @import("section/typed_doc_values.zig");
const query_mod = @import("search/query.zig");
const roaring = @import("encoding/roaring.zig");

/// Merge policy configuration.
pub const MergePolicy = struct {
    max_segments_per_tier: u32 = 8,
    max_merge_at_once: u32 = 10,
    max_segment_size: u64 = 5 * 1024 * 1024 * 1024,
    floor_segment_size: u64 = 16 * 1024 * 1024,
    skew_weight: f64 = 1.0,
    size_weight: f64 = 1.0,
    delete_reclaim_weight: f64 = 2.0,

    /// Plan a merge: pick the best set of segments to merge.
    /// Returns owned segment indices, or null if no merge is needed.
    pub fn plan(self: MergePolicy, alloc: Allocator, segments: []const SegmentInfo) !?[]usize {
        if (segments.len < 2) return null;

        var has_deletions = false;
        var has_floor_segments = false;
        for (segments) |seg| {
            has_deletions = has_deletions or seg.has_deletions;
            has_floor_segments = has_floor_segments or seg.size <= self.floor_segment_size;
        }

        if (segments.len <= self.max_segments_per_tier and !has_deletions and !has_floor_segments) {
            return null;
        }

        const candidates = try alloc.dupe(SegmentInfo, segments);
        defer alloc.free(candidates);

        std.mem.sort(SegmentInfo, candidates, {}, struct {
            fn lessThan(_: void, a: SegmentInfo, b: SegmentInfo) bool {
                if (a.size != b.size) return a.size < b.size;
                if (a.has_deletions != b.has_deletions) return a.has_deletions;
                return a.index < b.index;
            }
        }.lessThan);

        const max_merge_at_once: usize = @max(@as(usize, 2), @as(usize, self.max_merge_at_once));
        var best: ?struct {
            start: usize,
            len: usize,
            score: f64,
        } = null;

        for (0..candidates.len) |start| {
            var total_size: u64 = 0;
            var total_docs: u64 = 0;
            var total_deleted: u64 = 0;
            var largest_size: u64 = 0;
            var smallest_size: u64 = std.math.maxInt(u64);
            var has_candidate_deletions = false;
            var has_candidate_floor_segments = false;

            const max_len = @min(max_merge_at_once, candidates.len - start);
            for (0..max_len) |offset| {
                const candidate = candidates[start + offset];
                const effective_size = @max(candidate.size, self.floor_segment_size);
                if (candidate.size > self.max_segment_size) break;
                if (offset > 0 and total_size + effective_size > self.max_segment_size) break;

                total_size += effective_size;
                total_docs += candidate.doc_count;
                total_deleted += candidate.deleted_count;
                largest_size = @max(largest_size, effective_size);
                smallest_size = @min(smallest_size, effective_size);
                has_candidate_deletions = has_candidate_deletions or candidate.has_deletions;
                has_candidate_floor_segments = has_candidate_floor_segments or candidate.size <= self.floor_segment_size;

                const len = offset + 1;
                if (len < 2) continue;
                if (segments.len <= self.max_segments_per_tier and !has_candidate_deletions and !has_candidate_floor_segments) continue;

                const skew = if (smallest_size == 0)
                    1.0
                else
                    @as(f64, @floatFromInt(largest_size)) / @as(f64, @floatFromInt(smallest_size));
                const size_ratio = if (self.max_segment_size == 0)
                    1.0
                else
                    @as(f64, @floatFromInt(total_size)) / @as(f64, @floatFromInt(self.max_segment_size));
                const delete_ratio = if (total_docs == 0)
                    0.0
                else
                    @as(f64, @floatFromInt(total_deleted)) / @as(f64, @floatFromInt(total_docs));
                const floor_bonus: f64 = if (largest_size <= self.floor_segment_size) 0.25 else 0.0;
                const max_segments_per_tier: usize = @max(@as(usize, 1), @as(usize, self.max_segments_per_tier));
                const budget_pressure: f64 = if (segments.len > max_segments_per_tier)
                    @as(f64, @floatFromInt(segments.len - max_segments_per_tier)) / @as(f64, @floatFromInt(max_segments_per_tier))
                else
                    0.0;
                const width_ratio = @as(f64, @floatFromInt(len)) / @as(f64, @floatFromInt(max_merge_at_once));
                const backlog_width_bonus = if (budget_pressure > 1.0) (budget_pressure - 1.0) * width_ratio else 0.0;
                const score = (skew * self.skew_weight) +
                    (size_ratio * self.size_weight) -
                    (delete_ratio * self.delete_reclaim_weight) -
                    floor_bonus -
                    budget_pressure -
                    backlog_width_bonus;

                if (best == null or score < best.?.score) {
                    best = .{
                        .start = start,
                        .len = len,
                        .score = score,
                    };
                }
            }
        }

        const selected = best orelse return null;
        const planned = try alloc.alloc(usize, selected.len);
        for (planned, 0..) |*seg_idx, i| {
            seg_idx.* = candidates[selected.start + i].index;
        }
        return planned;
    }
};

pub const SegmentInfo = struct {
    index: usize,
    size: u64,
    doc_count: u32,
    deleted_count: u32 = 0,
    has_deletions: bool,
};

pub const MergeOutputOptions = struct {
    target_segment_bytes: usize = 256 * 1024 * 1024,
};

/// Merge multiple segments from the snapshot into one.
/// `segment_indices`: which segments in the snapshot to merge.
/// Returns the merged segment bytes. Caller owns result.
pub fn mergeSegments(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    segment_indices: []const usize,
) ![]u8 {
    if (segment_indices.len == 0) return error.NoSegments;

    var inputs = try alloc.alloc(segment_mod.MergeInput, segment_indices.len);
    defer alloc.free(inputs);
    for (segment_indices, 0..) |si, i| {
        const seg = &snap.segments[si];
        inputs[i] = .{
            .reader = &seg.reader,
            .deleted = seg.deleted,
        };
    }
    return try segment_mod.mergeSegmentInputs(alloc, inputs);
}

/// Merge multiple segments from a snapshot into bounded output segments.
/// Deleted documents are omitted and live documents retain their relative order.
pub fn mergeSegmentsBounded(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    segment_indices: []const usize,
    options: MergeOutputOptions,
) ![][]u8 {
    if (segment_indices.len == 0) return error.NoSegments;

    var inputs = try alloc.alloc(segment_mod.MergeInput, segment_indices.len);
    defer alloc.free(inputs);

    var total_input_bytes: u64 = 0;
    for (segment_indices, 0..) |si, i| {
        const seg = &snap.segments[si];
        total_input_bytes += seg.data.bytes().len;
        inputs[i] = .{
            .reader = &seg.reader,
            .deleted = seg.deleted,
        };
    }

    const live_docs = countLiveDocs(inputs);
    if (live_docs == 0) return try alloc.alloc([]u8, 0);

    const target_bytes = @max(@as(usize, 1), options.target_segment_bytes);
    if (live_docs <= 1) {
        const segments = try alloc.alloc([]u8, 1);
        errdefer alloc.free(segments);
        segments[0] = try segment_mod.mergeSegmentInputs(alloc, inputs);
        return segments;
    }

    const docs_per_segment_u64 = @max(
        @as(u64, 1),
        (@as(u64, live_docs) * @as(u64, @intCast(target_bytes))) / @max(@as(u64, 1), total_input_bytes),
    );
    const docs_per_segment: u32 = @intCast(@min(@as(u64, live_docs), docs_per_segment_u64));

    var outputs = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (outputs.items) |segment| alloc.free(segment);
        outputs.deinit(alloc);
    }

    var window_start: u32 = 0;
    while (window_start < live_docs) {
        var window_len = @min(docs_per_segment, live_docs - window_start);
        const segment = while (true) {
            const window_end = window_start + window_len;
            const candidate = try mergeLiveDocWindow(alloc, inputs, window_start, window_end);
            if (candidate.len <= target_bytes or window_len == 1) break candidate;
            alloc.free(candidate);
            window_len = @max(@as(u32, 1), window_len / 2);
        };
        try outputs.append(alloc, segment);
        window_start += window_len;
    }

    return try outputs.toOwnedSlice(alloc);
}

pub fn freeMergedSegments(alloc: Allocator, segments: [][]u8) void {
    for (segments) |segment| alloc.free(segment);
    if (segments.len > 0) alloc.free(segments);
}

/// Replace merged segments in the index with a single merged segment.
pub fn applyMerge(
    writer: *index_mod.IndexWriter,
    segment_indices: []const usize,
    merged_bytes: []const u8,
) !void {
    const snap = writer.snapshot();
    var old_ids = try writer.alloc.alloc(u64, segment_indices.len);
    defer writer.alloc.free(old_ids);

    for (segment_indices, 0..) |seg_idx, i| {
        old_ids[i] = snap.segments[seg_idx].id;
    }

    try writer.replaceSegments(old_ids, writer.next_segment_id, merged_bytes);
}

/// Replace merged segments in a persistent index, updating both on-disk and
/// in-memory state.
pub fn applyPersistentMerge(
    index: *persistent_mod.PersistentIndex,
    segment_indices: []const usize,
    merged_bytes: []const u8,
) !void {
    const snap = index.snapshot();
    var old_ids = try index.alloc.alloc(u64, segment_indices.len);
    defer index.alloc.free(old_ids);

    for (segment_indices, 0..) |seg_idx, i| {
        old_ids[i] = snap.segments[seg_idx].id;
    }

    try index.replaceSegments(old_ids, merged_bytes);
}

/// Like applyPersistentMerge(), but takes ownership of merged_bytes.
pub fn applyPersistentMergeOwned(
    index: *persistent_mod.PersistentIndex,
    segment_indices: []const usize,
    merged_bytes: []u8,
) !void {
    const snap = index.snapshot();
    var old_ids = try index.alloc.alloc(u64, segment_indices.len);
    defer index.alloc.free(old_ids);

    for (segment_indices, 0..) |seg_idx, i| {
        old_ids[i] = snap.segments[seg_idx].id;
    }

    try index.replaceSegmentsOwned(old_ids, merged_bytes);
}

pub fn applyPersistentMergeManyOwned(
    index: *persistent_mod.PersistentIndex,
    segment_indices: []const usize,
    merged_segments: [][]u8,
) !void {
    const snap = index.snapshot();
    var old_ids = try index.alloc.alloc(u64, segment_indices.len);
    defer index.alloc.free(old_ids);

    for (segment_indices, 0..) |seg_idx, i| {
        old_ids[i] = snap.segments[seg_idx].id;
    }

    _ = try index.replaceSegmentsIfActiveManyOwned(old_ids, merged_segments);
}

fn countLiveDocs(inputs: []const segment_mod.MergeInput) u32 {
    var total: u32 = 0;
    for (inputs) |input| {
        for (0..input.reader.doc_count) |doc_id_usize| {
            if (!isDeleted(input, @intCast(doc_id_usize))) total += 1;
        }
    }
    return total;
}

fn mergeLiveDocWindow(
    alloc: Allocator,
    inputs: []const segment_mod.MergeInput,
    window_start: u32,
    window_end: u32,
) ![]u8 {
    const window_inputs = try alloc.alloc(segment_mod.MergeInput, inputs.len);
    defer alloc.free(window_inputs);

    const masks = try alloc.alloc(roaring.RoaringBitmap, inputs.len);
    var masks_initialized: usize = 0;
    defer {
        for (masks[0..masks_initialized]) |*mask| mask.deinit();
        alloc.free(masks);
    }

    var live_ordinal: u32 = 0;
    for (inputs, 0..) |input, input_idx| {
        masks[input_idx] = roaring.RoaringBitmap.init(alloc);
        masks_initialized += 1;

        for (0..input.reader.doc_count) |doc_id_usize| {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (isDeleted(input, doc_id)) {
                try masks[input_idx].add(doc_id);
                continue;
            }
            const include = live_ordinal >= window_start and live_ordinal < window_end;
            live_ordinal += 1;
            if (!include) try masks[input_idx].add(doc_id);
        }

        window_inputs[input_idx] = .{
            .reader = input.reader,
            .deleted = if (masks[input_idx].isEmpty()) null else masks[input_idx],
        };
    }

    return try segment_mod.mergeSegmentInputs(alloc, window_inputs);
}

fn isDeleted(input: segment_mod.MergeInput, doc_id: u32) bool {
    return if (input.deleted) |deleted| deleted.contains(doc_id) else false;
}

// ============================================================================
// Tests
// ============================================================================

test "merge two segments" {
    const alloc = std.testing.allocator;
    const introducer = @import("introducer.zig");

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    var intro = introducer.Introducer.init(alloc, &writer);

    // Create two segments
    try intro.submit(.{ .docs = &.{
        .{ .id = "a", .stored_data = "data_a", .fields = &.{.{
            .field_name = "body",
            .hits = &.{.{ .term = "hello", .freq = 1, .norm = 10 }},
        }} },
    } });
    try intro.submit(.{ .docs = &.{
        .{ .id = "b", .stored_data = "data_b", .fields = &.{.{
            .field_name = "body",
            .hits = &.{ .{ .term = "hello", .freq = 2, .norm = 15 }, .{ .term = "world", .freq = 1, .norm = 15 } },
        }} },
    } });

    try std.testing.expectEqual(@as(usize, 2), writer.snapshot().segments.len);

    // Merge both segments
    const merged = try mergeSegments(alloc, writer.snapshot(), &.{ 0, 1 });
    defer alloc.free(merged);

    // Verify merged segment is valid
    var reader = try segment_mod.SegmentReader.init(alloc, merged);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);

    // Verify inverted index in merged segment
    var inv = (try reader.invertedIndex("body")).?;
    const hello = inv.lookup("hello") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), hello.docFreq());
    const world = inv.lookup("world") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), world.docFreq());
}

test "bounded merge splits output and preserves live documents" {
    const alloc = std.testing.allocator;
    const mapper = @import("storage/db/document_mapper.zig");

    const seg1 = (try mapper.buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"body\":\"alpha one\"}" },
    }, .{}, null)).?;
    defer alloc.free(seg1);
    const seg2 = (try mapper.buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:2", .value = "{\"body\":\"beta two\"}" },
    }, .{}, null)).?;
    defer alloc.free(seg2);
    const seg3 = (try mapper.buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:3", .value = "{\"body\":\"gamma three\"}" },
    }, .{}, null)).?;
    defer alloc.free(seg3);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg1);
    try writer.addSegment(seg2);
    try writer.addSegment(seg3);

    const merged = try mergeSegmentsBounded(alloc, writer.snapshot(), &.{ 0, 1, 2 }, .{ .target_segment_bytes = 1 });
    defer freeMergedSegments(alloc, merged);

    try std.testing.expect(merged.len > 1);
    var total_docs: u32 = 0;
    for (merged) |segment| {
        var reader = try segment_mod.SegmentReader.init(alloc, segment);
        defer reader.deinit();
        try std.testing.expect(reader.doc_count > 0);
        total_docs += reader.doc_count;
    }
    try std.testing.expectEqual(@as(u32, 3), total_docs);
}

test "merge policy picks small segments and applyMerge replaces them" {
    const alloc = std.testing.allocator;
    const introducer = @import("introducer.zig");

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    var intro = introducer.Introducer.init(alloc, &writer);

    try intro.submit(.{ .docs = &.{
        .{ .id = "a", .stored_data = "data_a", .fields = &.{.{
            .field_name = "body",
            .hits = &.{.{ .term = "hello", .freq = 1, .norm = 10 }},
        }} },
    } });
    try intro.submit(.{ .docs = &.{
        .{ .id = "b", .stored_data = "data_b", .fields = &.{.{
            .field_name = "body",
            .hits = &.{.{ .term = "world", .freq = 1, .norm = 10 }},
        }} },
    } });
    try intro.submit(.{ .docs = &.{
        .{ .id = "c", .stored_data = "data_c", .fields = &.{.{
            .field_name = "body",
            .hits = &.{.{ .term = "third", .freq = 1, .norm = 10 }},
        }} },
    } });

    const snap = writer.snapshot();
    var infos = try alloc.alloc(SegmentInfo, snap.segments.len);
    defer alloc.free(infos);

    for (snap.segments, 0..) |seg, i| {
        infos[i] = .{
            .index = i,
            .size = seg.data.bytes().len,
            .doc_count = seg.reader.doc_count,
            .deleted_count = if (seg.deleted) |deleted| @intCast(deleted.cardinality()) else 0,
            .has_deletions = seg.deleted != null,
        };
    }

    const policy = MergePolicy{
        .max_segments_per_tier = 2,
        .max_segment_size = 1024 * 1024,
        .floor_segment_size = 4096,
    };
    const planned = (try policy.plan(alloc, infos)).?;
    defer alloc.free(planned);

    try std.testing.expectEqual(@as(usize, 2), planned.len);

    const merged = try mergeSegments(alloc, writer.snapshot(), planned);
    defer alloc.free(merged);

    try applyMerge(&writer, planned, merged);

    try std.testing.expectEqual(@as(usize, 2), writer.snapshot().segments.len);

    const results = try writer.snapshot().search(alloc, "body", &.{ "hello", "world" }, 10);
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 2), results.hits.len);
}

test "merge policy skips idle index within tier budget" {
    const alloc = std.testing.allocator;
    const policy = MergePolicy{
        .max_segments_per_tier = 4,
        .max_segment_size = 1024 * 1024,
        .floor_segment_size = 64,
    };
    const infos = [_]SegmentInfo{
        .{ .index = 0, .size = 256, .doc_count = 10, .has_deletions = false },
        .{ .index = 1, .size = 512, .doc_count = 10, .has_deletions = false },
        .{ .index = 2, .size = 768, .doc_count = 10, .has_deletions = false },
    };

    const planned = try policy.plan(alloc, &infos);
    if (planned) |owned| alloc.free(owned);
    try std.testing.expect(planned == null);
}

test "merge policy reclaims deleted docs within tier budget" {
    const alloc = std.testing.allocator;
    const policy = MergePolicy{
        .max_segments_per_tier = 8,
        .max_merge_at_once = 3,
        .max_segment_size = 1024 * 1024,
        .floor_segment_size = 64,
        .delete_reclaim_weight = 4.0,
    };
    const infos = [_]SegmentInfo{
        .{ .index = 0, .size = 10_000, .doc_count = 100, .deleted_count = 0, .has_deletions = false },
        .{ .index = 1, .size = 11_000, .doc_count = 100, .deleted_count = 0, .has_deletions = false },
        .{ .index = 2, .size = 12_000, .doc_count = 100, .deleted_count = 90, .has_deletions = true },
        .{ .index = 3, .size = 13_000, .doc_count = 100, .deleted_count = 0, .has_deletions = false },
    };

    const planned = (try policy.plan(alloc, &infos)).?;
    defer alloc.free(planned);

    try std.testing.expect(planned.len >= 2);
    try std.testing.expect(std.mem.indexOfScalar(usize, planned, 2) != null);
}

test "merge policy compacts floor segments within tier budget" {
    const alloc = std.testing.allocator;
    const policy = MergePolicy{
        .max_segments_per_tier = 8,
        .max_merge_at_once = 4,
        .max_segment_size = 1024 * 1024,
        .floor_segment_size = 2048,
    };
    const infos = [_]SegmentInfo{
        .{ .index = 0, .size = 512, .doc_count = 2, .has_deletions = false },
        .{ .index = 1, .size = 768, .doc_count = 2, .has_deletions = false },
        .{ .index = 2, .size = 128 * 1024, .doc_count = 100, .has_deletions = false },
        .{ .index = 3, .size = 256 * 1024, .doc_count = 100, .has_deletions = false },
    };

    const planned = (try policy.plan(alloc, &infos)).?;
    defer alloc.free(planned);

    try std.testing.expectEqual(@as(usize, 2), planned.len);
    try std.testing.expect(std.mem.indexOfScalar(usize, planned, 0) != null);
    try std.testing.expect(std.mem.indexOfScalar(usize, planned, 1) != null);
}

test "merge direct-copies single-source field sections when eligible" {
    const alloc = std.testing.allocator;
    const introducer = @import("introducer.zig");

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    var intro = introducer.Introducer.init(alloc, &writer);

    try intro.submit(.{ .docs = &.{
        .{ .id = "title-doc", .stored_data = "{\"title\":\"alpha\"}", .fields = &.{.{
            .field_name = "title",
            .hits = &.{.{ .term = "alpha", .freq = 1, .norm = 8 }},
        }} },
    } });
    try intro.submit(.{ .docs = &.{
        .{ .id = "body-doc", .stored_data = "{\"body\":\"beta\"}", .fields = &.{.{
            .field_name = "body",
            .hits = &.{.{ .term = "beta", .freq = 1, .norm = 7 }},
        }} },
    } });

    const snap = writer.snapshot();
    const original_title = snap.segments[0].reader.getSection("title", .inverted_text).?;

    const merged = try mergeSegments(alloc, snap, &.{ 0, 1 });
    defer alloc.free(merged);

    var merged_reader = try segment_mod.SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();

    const merged_title = merged_reader.getSection("title", .inverted_text).?;
    try std.testing.expectEqualStrings(original_title, merged_title);

    const stored0 = (try merged_reader.storedDocDecompressed(0)).?;
    defer alloc.free(stored0.data);
    const stored1 = (try merged_reader.storedDocDecompressed(1)).?;
    defer alloc.free(stored1.data);

    try std.testing.expectEqualStrings("title-doc", stored0.id);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", stored0.data);
    try std.testing.expectEqualStrings("body-doc", stored1.id);
    try std.testing.expectEqualStrings("{\"body\":\"beta\"}", stored1.data);
}

test "merge mapper-built multi-field text segments" {
    const alloc = std.testing.allocator;
    const mapper = @import("storage/db/document_mapper.zig");
    const text_analysis = @import("introducer.zig").TextAnalysisConfig{};

    const seg1 = (try mapper.buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc1", .value = "{\"content\":\"alpha beta\",\"title\":\"both\"}" },
    }, text_analysis, null)).?;
    defer alloc.free(seg1);

    const seg2 = (try mapper.buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc2", .value = "{\"content\":\"alpha only\",\"title\":\"alpha\"}" },
    }, text_analysis, null)).?;
    defer alloc.free(seg2);

    const seg3 = (try mapper.buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc3", .value = "{\"content\":\"beta only\",\"title\":\"beta\"}" },
    }, text_analysis, null)).?;
    defer alloc.free(seg3);

    const merged = try segment_mod.mergeSegments(alloc, &.{ seg1, seg2, seg3 });
    defer alloc.free(merged);

    var reader = try segment_mod.SegmentReader.init(alloc, merged);
    defer reader.deinit();

    try std.testing.expectEqual(@as(u32, 3), reader.doc_count);

    var content = (try reader.invertedIndex("content")).?;
    const alpha = content.lookup("alpha") orelse return error.TestExpectedEqual;
    const beta = content.lookup("beta") orelse return error.TestExpectedEqual;

    try std.testing.expectEqual(@as(u32, 2), alpha.docFreq());
    try std.testing.expectEqual(@as(u32, 2), beta.docFreq());

    const doc0 = (try reader.storedDocDecompressed(0)).?;
    defer alloc.free(doc0.data);
    const doc1 = (try reader.storedDocDecompressed(1)).?;
    defer alloc.free(doc1.data);
    const doc2 = (try reader.storedDocDecompressed(2)).?;
    defer alloc.free(doc2.data);

    try std.testing.expectEqualStrings("doc1", doc0.id);
    try std.testing.expectEqualStrings("doc2", doc1.id);
    try std.testing.expectEqualStrings("doc3", doc2.id);

    var seg_entry: index_mod.SegmentEntry = .{
        .id = 0,
        .data = index_mod.SegmentData.fromOwnedHeap(try alloc.dupe(u8, merged)),
        .reader = reader,
        .deleted = null,
    };
    defer seg_entry.data.deinit(alloc);
    const phrase = query_mod.Filter{ .phrase = .{
        .field = "content",
        .terms = &.{ "alpha", "beta" },
        .slop = 0,
    } };
    var phrase_hits = try phrase.execute(alloc, &seg_entry);
    defer phrase_hits.deinit();
    try std.testing.expect(phrase_hits.contains(0));
    try std.testing.expect(!phrase_hits.contains(1));
    try std.testing.expect(!phrase_hits.contains(2));
}

test "merge mapper-built segments preserves typed doc values" {
    const alloc = std.testing.allocator;
    const mapper = @import("storage/db/document_mapper.zig");
    const text_analysis = @import("introducer.zig").TextAnalysisConfig{};

    const seg1 = (try mapper.buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "a", .value = "{\"content\":\"alpha\",\"price\":10,\"published_at\":\"2026-01-01T00:00:00Z\",\"location\":{\"lat\":37.7749,\"lon\":-122.4194}}" },
    }, text_analysis, null)).?;
    defer alloc.free(seg1);

    const seg2 = (try mapper.buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "b", .value = "{\"content\":\"beta\",\"price\":20,\"published_at\":\"2026-01-02T00:00:00Z\",\"location\":{\"lat\":40.7128,\"lon\":-74.0060}}" },
    }, text_analysis, null)).?;
    defer alloc.free(seg2);

    const merged = try segment_mod.mergeSegments(alloc, &.{ seg1, seg2 });
    defer alloc.free(merged);

    var reader = try segment_mod.SegmentReader.init(alloc, merged);
    defer reader.deinit();

    const price_section = reader.getSection("price", .typed_doc_values) orelse return error.TestExpectedEqual;
    var price_reader = try typed_dv.TypedDocValuesReader.init(alloc, price_section);
    try std.testing.expectEqual(@as(?f64, 10.0), try price_reader.getF64(0));
    try std.testing.expectEqual(@as(?f64, 20.0), try price_reader.getF64(1));

    const ts_section = reader.getSection("published_at", .typed_doc_values) orelse return error.TestExpectedEqual;
    var ts_reader = try typed_dv.TypedDocValuesReader.init(alloc, ts_section);
    try std.testing.expect((try ts_reader.getU64(0)) != null);
    try std.testing.expect((try ts_reader.getU64(1)) != null);

    const geo_section = reader.getSection("location", .typed_doc_values) orelse return error.TestExpectedEqual;
    var geo_reader = try typed_dv.TypedDocValuesReader.init(alloc, geo_section);
    try std.testing.expect((try geo_reader.getGeoPoint(0)) != null);
    try std.testing.expect((try geo_reader.getGeoPoint(1)) != null);
}
