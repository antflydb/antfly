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

//! Bounded immutable projection runs. Recency is manifest order, not wall time.
//! Merge checkpoints retain their immutable inputs until atomic root publication.
const std = @import("std");
const A = std.mem.Allocator;
const tree = @import("../graph_segment/page_tree.zig");
const common = @import("../paged_projection.zig");
const blocks = @import("blocks.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
pub const max_runs = 4;
pub const Merge = struct {
    start: u8,
    left: tree.Ref,
    right: tree.Ref,
    output: ?tree.Ref = null,
    after: []const u8 = "",
};
pub fn records(root: common.Root) u64 {
    return if (root.format == 3) root.vector_records else if (root.docs) |r| r.records else 0;
}
pub fn pending(root: common.Root) bool {
    return root.vector_merge != null or root.vector_runs.len + @as(usize, if (root.docs != null) 1 else 0) >= max_runs;
}
pub fn references(root: common.Root, out: *[max_runs]tree.Ref) ![]tree.Ref {
    const n = root.vector_runs.len + @as(usize, if (root.docs != null) 1 else 0);
    if (n > max_runs) return error.VectorRunBackpressure;
    var i: usize = 0;
    if (root.docs) |ref| {
        out[0] = ref;
        i = 1;
    }
    @memcpy(out[i..][0..root.vector_runs.len], root.vector_runs);
    return out[0..n];
}

pub const Cursor = struct {
    a: A,
    store: tree.Store,
    cursors: [max_runs]?tree.Cursor = @splat(null),
    heads: [max_runs]?tree.Cursor.Record = @splat(null),
    advance: [max_runs]bool = @splat(true),
    count: usize = 0,
    keep_tombstones: bool = false,
    pub fn init(a: A, store: tree.Store, root: common.Root, lower: []const u8, upper: ?[]const u8) !Cursor {
        var refs: [max_runs]tree.Ref = undefined;
        return initRefs(a, store, try references(root, &refs), lower, upper, false);
    }
    fn initRefs(a: A, store: tree.Store, refs: []const tree.Ref, lower: []const u8, upper: ?[]const u8, keep_tombstones: bool) !Cursor {
        if (refs.len > max_runs) return error.VectorRunBackpressure;
        var self = Cursor{ .a = a, .store = store, .keep_tombstones = keep_tombstones };
        errdefer self.deinit();
        for (refs, 0..) |ref, i| {
            self.cursors[i] = try tree.Cursor.init(a, store, ref, lower, upper);
            self.count += 1;
        }
        return self;
    }
    pub fn deinit(self: *Cursor) void {
        for (self.cursors[0..self.count]) |*c| if (c.*) |*cursor| cursor.deinit();
    }
    pub fn readAhead(self: *Cursor, remaining: u64, bytes: usize) !void {
        // Multiple runs are merged lazily; prefetch only the single-run case
        // where every requested sibling is guaranteed to belong to this slice.
        if (self.count == 1) try self.cursors[0].?.readAhead(remaining, bytes);
    }
    pub fn next(self: *Cursor) !?tree.Cursor.Record {
        while (true) {
            try self.store.check(self.store.ptr);
            for (0..self.count) |i| if (self.advance[i]) {
                self.heads[i] = try self.cursors[i].?.next();
                self.advance[i] = false;
            };
            var best: ?usize = null;
            for (self.heads[0..self.count], 0..) |head, i| if (head) |row| {
                if (best == null or std.mem.order(u8, row.key, self.heads[best.?].?.key) != .gt) best = i;
            };
            const winner = best orelse return null;
            const record = self.heads[winner].?;
            for (self.heads[0..self.count], 0..) |head, i| if (head) |row| {
                if (std.mem.eql(u8, row.key, record.key)) self.advance[i] = true;
            };
            if (record.value.len != 0 or self.keep_tombstones) return record;
        }
    }
};

/// Publish a sorted compact delta without rewriting the existing vector base.
/// Tombstones are values in a run; only manifest merging resolves their age.
pub fn append(a: A, scratch: A, store: tree.Store, root: *common.Root, changes: std.StringHashMapUnmanaged(?[]const u8)) !void {
    if (changes.count() == 0) return;
    var refs: [max_runs]tree.Ref = undefined;
    const old = try references(root.*, &refs);
    if (old.len == max_runs) return error.VectorRunBackpressure;
    const sorted = try scratch.alloc(tree.Mutation, changes.count());
    var it = changes.iterator();
    var i: usize = 0;
    while (it.next()) |entry| : (i += 1) sorted[i] = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* orelse "" };
    std.mem.sort(tree.Mutation, sorted, {}, struct {
        fn less(_: void, x: tree.Mutation, y: tree.Mutation) bool {
            return std.mem.lessThan(u8, x.key, y.key);
        }
    }.less);
    var builder = tree.SortedBuilder{ .alloc = a, .store = store, .target_bytes = 128 * 1024 };
    defer builder.deinit();
    for (sorted) |change| try builder.add(change.key, change.value.?);
    const fresh = (try builder.finish()) orelse return error.InvalidVectorRun;
    if (old.len == 0) root.docs = fresh else {
        const runs = try scratch.alloc(tree.Ref, root.vector_runs.len + 1);
        @memcpy(runs[0..root.vector_runs.len], root.vector_runs);
        runs[runs.len - 1] = fresh;
        root.vector_runs = runs;
    }
}

pub const Maintenance = struct { ref: @import("../manifest/mod.zig").ArtifactRef, has_more: bool };
/// One bounded merge step. Older source references and private output remain
/// reachable through this checkpoint until the final atomic manifest swap.
pub fn maintain(a: A, pages: *PageStore, source: @import("../manifest/mod.zig").ArtifactRef) !?Maintenance {
    pages.prefix_keys = true;
    pages.value_prefix = .{ .offset = 20, .len = 56 };
    pages.pack_pages = true;
    if (source.byte_len > pages.remaining_read_bytes.*) return error.ArtifactReadBudgetExceeded;
    pages.remaining_read_bytes.* -= source.byte_len;
    const parsed = try common.load(a, pages, source);
    defer parsed.deinit();
    var root = parsed.value;
    if (root.format != 3 or !pending(root)) return null;
    var refs: [max_runs]tree.Ref = undefined;
    const inputs = try references(root, &refs);
    var job: Merge = if (root.vector_merge) |merge| merge else blk: {
        var start: usize = 0;
        var numerator: u64 = std.math.maxInt(u64);
        var denominator: u64 = 1;
        var cost: u64 = std.math.maxInt(u64);
        for (0..inputs.len - 1) |i| {
            const larger = @max(inputs[i].records, inputs[i + 1].records);
            const smaller = @min(inputs[i].records, inputs[i + 1].records);
            const next = inputs[i].records +| inputs[i + 1].records;
            const ratio_left = @as(u128, larger) * denominator;
            const ratio_right = @as(u128, numerator) * smaller;
            if (ratio_left < ratio_right or (ratio_left == ratio_right and next < cost)) {
                start = i;
                numerator = larger;
                denominator = smaller;
                cost = next;
            }
        }
        break :blk .{ .start = @intCast(start), .left = inputs[start], .right = inputs[start + 1] };
    };
    if (job.start + 1 >= inputs.len or !inputs[job.start].eql(job.left) or !inputs[job.start + 1].eql(job.right)) return error.InvalidVectorMergeCheckpoint;
    var after: [36]u8 = undefined;
    if (job.after.len != 0) {
        if (job.after.len != 72) return error.InvalidVectorMergeCheckpoint;
        _ = std.fmt.hexToBytes(&after, job.after) catch return error.InvalidVectorMergeCheckpoint;
    }
    const lower = if (job.after.len == 0) null else try common.upperBound(a, &after);
    defer if (lower) |key| a.free(key);
    // Surface tombstones so a deletion-only prefix still consumes bounded
    // work and advances its durable exclusive checkpoint.
    var cursor = try Cursor.initRefs(a, pages.store(), &.{ job.left, job.right }, lower orelse "", null, true);
    defer cursor.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var mutations: std.ArrayList(tree.Mutation) = .empty;
    var bytes: usize = 0;
    var visited: usize = 0;
    var complete = false;
    var packs: [64]common.Blob = undefined;
    var pack_count: usize = 0;
    var residual_output_bytes: u64 = 0;
    var reserved: u64 = 0;
    defer pages.remaining_read_bytes.* += reserved;
    while (visited < 1024 and bytes < 1024 * 1024) {
        const row = (cursor.next() catch |err| switch (err) {
            error.ArtifactReadBudgetExceeded => if (visited != 0) break else return err,
            else => return err,
        }) orelse {
            complete = true;
            break;
        };
        if (row.key.len != 36) return error.InvalidVectorRun;
        if (row.value.len == 0 and job.start == 0) {
            visited += 1;
            bytes += row.key.len;
            @memcpy(&after, row.key);
            continue;
        }
        var fresh: ?common.Blob = null;
        if (blocks.isCompact(row.value)) {
            const c = try blocks.control(row.value, root.dims);
            if (residual_output_bytes + c.length > 1024 * 1024 and visited != 0) break;
            residual_output_bytes += c.length;
            const found = for (packs[0..pack_count]) |blob| {
                if (std.meta.eql(blob, c.residual)) break true;
            } else false;
            if (!found) {
                if (pack_count == packs.len or c.residual.bytes > pages.remaining_read_bytes.*) {
                    if (visited == 0) return error.ArtifactReadBudgetExceeded;
                    break;
                }
                fresh = c.residual;
            }
        }
        try mutations.append(scratch, .{ .key = try scratch.dupe(u8, row.key), .value = try scratch.dupe(u8, row.value) });
        if (fresh) |blob| {
            pages.remaining_read_bytes.* -= blob.bytes;
            reserved += blob.bytes;
            packs[pack_count] = blob;
            pack_count += 1;
        }
        bytes += row.key.len + row.value.len;
        visited += 1;
        @memcpy(&after, row.key);
    }
    if (!complete and visited == 0) return error.VectorMergeMadeNoProgress;
    const Source = struct {
        entries: []const tree.Mutation,
        pub fn next(self: *@This()) !?tree.Mutation {
            if (self.entries.len == 0) return null;
            const result = self.entries[0];
            self.entries = self.entries[1..];
            return result;
        }
    };
    pages.remaining_read_bytes.* += reserved;
    reserved = 0;
    const values = try scratch.alloc([]const u8, mutations.items.len);
    for (mutations.items, values) |mutation, *value| value.* = mutation.value.?;
    const repacked = try blocks.repackAlloc(a, pages, values, root.dims);
    defer {
        for (repacked) |value| a.free(value);
        a.free(repacked);
    }
    for (mutations.items, repacked) |*mutation, value| mutation.value = value;
    var stream = Source{ .entries = mutations.items };
    job.output = try tree.applySortedWithOptions(a, pages.store(), job.output, &stream, .{ .target_bytes = 128 * 1024, .inherit_page_size = false });
    if (complete) {
        var output: [max_runs]tree.Ref = undefined;
        var n: usize = 0;
        for (inputs, 0..) |ref, i| {
            if (i == job.start) {
                if (job.output) |fresh| {
                    output[n] = fresh;
                    n += 1;
                }
            } else if (i != job.start + 1) {
                output[n] = ref;
                n += 1;
            }
        }
        root.docs = if (n == 0) null else output[0];
        root.vector_runs = if (n <= 1) &.{} else output[1..n];
        root.vector_merge = null;
        return .{ .ref = try common.publish(a, pages, root), .has_more = pending(root) };
    }
    const after_hex = std.fmt.bytesToHex(after, .lower);
    job.after = &after_hex;
    root.vector_merge = job;
    return .{ .ref = try common.publish(a, pages, root), .has_more = true };
}

test "serverless vector run checkpoints preserve recency tombstones and source snapshots" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/runs", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var backend = fs.artifactStore();
    defer backend.deinit();
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 32 * 1024 * 1024;
    var pages = PageStore{ .domain = @splat(9), .attempt = @splat(1), .artifacts = &backend, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .prefix_keys = true, .pack_pages = true };
    const old_value = try @import("paged.zig").vectorValue(a, .{ .doc_id = @constCast("old"), .vector = @constCast(&[_]f32{ 1, 0 }) });
    defer a.free(old_value);
    const updated_value = try @import("paged.zig").vectorValue(a, .{ .doc_id = @constCast("updated"), .vector = @constCast(&[_]f32{ 0, 1 }) });
    defer a.free(updated_value);
    const inserted_value = try @import("paged.zig").vectorValue(a, .{ .doc_id = @constCast("inserted"), .vector = @constCast(&[_]f32{ 1, 1 }) });
    defer a.free(inserted_value);
    const latest_value = try @import("paged.zig").vectorValue(a, .{ .doc_id = @constCast("latest"), .vector = @constCast(&[_]f32{ 1, -1 }) });
    defer a.free(latest_value);
    var base = tree.SortedBuilder{ .alloc = a, .store = pages.store(), .target_bytes = 32 * 1024 };
    defer base.deinit();
    var keys: [1501][36]u8 = @splat(@splat(0));
    for (&keys, 0..) |*key, i| std.mem.writeInt(u32, key[32..36], @intCast(i), .big);
    for (keys[0..1500]) |key| try base.add(&key, old_value);
    const original = (try base.finish()).?;
    const delta = (try tree.apply(a, pages.store(), null, &.{
        .{ .key = &keys[5], .value = "" },
        .{ .key = &keys[10], .value = updated_value },
        .{ .key = &keys[1500], .value = inserted_value },
    })).?;
    const newest = (try tree.apply(a, pages.store(), null, &.{.{ .key = &keys[10], .value = latest_value }})).?;
    var root = common.Root{ .format = 3, .kind = .vector_segment, .domain = pages.domain, .index_name = "dense", .source_fingerprint = @splat(1), .docs = original, .vector_runs = &.{ delta, newest }, .vector_records = 1500, .vector_merge = .{ .start = 0, .left = original, .right = delta }, .directory = original, .centroids = original, .dims = 2 };
    const initial = try common.publish(a, &pages, root);
    defer @import("../build/builder.zig").freeArtifactRef(a, initial);
    var current = initial;
    defer if (!std.mem.eql(u8, current.artifact_id, initial.artifact_id)) @import("../build/builder.zig").freeArtifactRef(a, current);
    var steps: usize = 0;
    while (true) {
        reads = 4 * 1024 * 1024;
        writes = 4 * 1024 * 1024;
        const step = (try maintain(a, &pages, current)).?;
        if (!std.mem.eql(u8, current.artifact_id, initial.artifact_id)) @import("../build/builder.zig").freeArtifactRef(a, current);
        current = step.ref;
        steps += 1;
        const parsed = try common.load(a, &pages, current);
        defer parsed.deinit();
        root = parsed.value;
        if (steps == 1) {
            try std.testing.expect(root.vector_merge != null and root.vector_merge.?.output != null);
            try std.testing.expect(root.docs.?.eql(original));
        }
        var cursor = try Cursor.init(a, pages.store(), root, "", null);
        defer cursor.deinit();
        var count: u64 = 0;
        var source_blocks = blocks.Reader{ .a = a, .pages = &pages };
        defer source_blocks.deinit();
        while (try cursor.next()) |row| {
            const i = std.mem.readInt(u32, row.key[32..36], .big);
            try std.testing.expect(i != 5);
            if (i == 10 or i == 1500) {
                var vector = try source_blocks.read(row.value, 2);
                defer vector.deinit(a);
                try std.testing.expectEqualStrings(if (i == 10) "latest" else "inserted", vector.doc_id);
            }
            count += 1;
        }
        try std.testing.expectEqual(@as(u64, 1500), count);
        if (!step.has_more) break;
        try std.testing.expect(steps < 4);
    }
    try std.testing.expect(steps > 1);
    var before = try tree.Cursor.init(a, pages.store(), original, &keys[5], null);
    defer before.deinit();
    try std.testing.expectEqualSlices(u8, old_value, (try before.next()).?.value);
    const old = try common.load(a, &pages, initial);
    defer old.deinit();
    try std.testing.expect(old.value.vector_merge != null);
    const Run = struct {
        fn run(alloc: A, store: tree.Store, snapshot: common.Root) !void {
            var c = try Cursor.init(alloc, store, snapshot, "", null);
            defer c.deinit();
            _ = try c.next();
            _ = try c.next();
        }
    };
    reads = std.math.maxInt(u64);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{ pages.store(), old.value });

    // No retained rows in a prefix must still yield after 1,024 visited keys.
    // The newer serving run revives one document outside the merged pair.
    var deleted = tree.SortedBuilder{ .alloc = a, .store = pages.store() };
    defer deleted.deinit();
    for (keys[0..1500]) |key| try deleted.add(&key, "");
    const deletions = (try deleted.finish()).?;
    root = old.value;
    root.vector_runs = &.{ deletions, newest };
    root.vector_records = 1;
    root.vector_merge = .{ .start = 0, .left = original, .right = deletions };
    const deletion_root = try common.publish(a, &pages, root);
    defer @import("../build/builder.zig").freeArtifactRef(a, deletion_root);
    reads = 4 * 1024 * 1024;
    writes = 4 * 1024 * 1024;
    const first = (try maintain(a, &pages, deletion_root)).?;
    defer @import("../build/builder.zig").freeArtifactRef(a, first.ref);
    try std.testing.expect(first.has_more);
    const checkpoint = try common.load(a, &pages, first.ref);
    defer checkpoint.deinit();
    try std.testing.expect(checkpoint.value.vector_merge.?.output == null);
    try std.testing.expectEqual(@as(usize, 72), checkpoint.value.vector_merge.?.after.len);
    reads = 4 * 1024 * 1024;
    writes = 4 * 1024 * 1024;
    const last = (try maintain(a, &pages, first.ref)).?;
    defer @import("../build/builder.zig").freeArtifactRef(a, last.ref);
    try std.testing.expect(!last.has_more);
    const finished = try common.load(a, &pages, last.ref);
    defer finished.deinit();
    try std.testing.expect(finished.value.vector_merge == null);
    var live = try Cursor.init(a, pages.store(), finished.value, "", null);
    defer live.deinit();
    try std.testing.expectEqualSlices(u8, latest_value, (try live.next()).?.value);
    try std.testing.expect((try live.next()) == null);
}
