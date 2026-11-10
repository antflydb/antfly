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

//! Transactional retained aggregate state. Callers MUST use the same serialized
//! write transaction for source rows, this state, and source replay progress.
//! No wall clock, network I/O, or transaction commit occurs in this module.
//! Any error poisons the caller transaction: it MUST abort, never commit a
//! partially staged operation. Source decoding/authorization belong to the host.
const std = @import("std");
const algebra = @import("db/algebraic/algebra.zig");
const A = std.mem.Allocator;
const prefix = "\x00\x00__retained_series_v1__:";
pub const max_changes = 1024;
pub const max_measures = 32;
pub const max_feed_frames = 256;
pub const max_frame_bytes = 1024 * 1024;
pub const max_feed_bytes = 4 * max_frame_bytes;
pub const Id = @import("../retained_dataset_types.zig").Id;
pub const Law = @import("../retained_dataset_types.zig").Law;
pub const Spec = struct {
    id: Id,
    generation: u64,
    /// Binds the complete immutable definition, including authorization/schema.
    definition: Id,
    source_partition: Id,
    source_incarnation: u64,
    interval_ns: u64,
    origin_ns: i128 = 0,
    mutable_for_ns: u64,
    retention_ns: u64 = std.math.maxInt(u64),
    laws: []const Law,
    /// Empty means every count is COUNT(*); true positions are COUNT(field).
    count_nonnull: []const bool = &.{},

    pub fn countsNonNull(s: Spec, i: usize) bool {
        return s.count_nonnull.len != 0 and s.count_nonnull[i];
    }
    pub fn validate(s: Spec) !void {
        if (s.generation == 0 or s.source_incarnation == 0 or s.interval_ns == 0 or
            s.laws.len == 0 or s.laws.len > max_measures or s.retention_ns <= s.mutable_for_ns or
            std.mem.allEqual(u8, &s.id, 0) or std.mem.allEqual(u8, &s.definition, 0) or std.mem.allEqual(u8, &s.source_partition, 0)) return error.InvalidRetainedSeries;
        if (s.count_nonnull.len != 0 and s.count_nonnull.len != s.laws.len) return error.InvalidRetainedSeries;
        for (s.laws, 0..) |law, i| if (s.countsNonNull(i) and law != .count) return error.InvalidRetainedSeries;
    }
    pub fn bucket(s: Spec, timestamp: i128) !i128 {
        if (s.interval_ns == 0) return error.InvalidRetainedSeries;
        const start = @import("../datetime.zig").binSigned(timestamp, s.interval_ns, s.origin_ns) orelse return error.RetainedSeriesTimeOverflow;
        _ = std.math.add(i128, start, s.interval_ns) catch return error.RetainedSeriesTimeOverflow;
        return start;
    }
    pub fn validateBucket(self: Spec, b: Bucket) !void {
        if (b.rows <= 0 or b.metrics.len != self.laws.len or b.dimensions.len > 4096 or !std.unicode.utf8ValidateSlice(b.dimensions) or
            try self.bucket(b.start_ns) != b.start_ns) return error.RetainedSeriesCorrupt;
        for (self.laws, b.metrics, 0..) |law, m, i| {
            if (m.count < 0 or m.count > b.rows or !std.math.isFinite(m.sum)) return error.RetainedSeriesCorrupt;
            if (m.minimum) |n| if (!std.math.isFinite(n)) return error.RetainedSeriesCorrupt;
            if (m.maximum) |n| if (!std.math.isFinite(n)) return error.RetainedSeriesCorrupt;
            if ((law == .count and !self.countsNonNull(i) and m.count != b.rows) or (law == .min and (m.count == 0) != (m.minimum == null)) or
                (law == .max and (m.count == 0) != (m.maximum == null))) return error.RetainedSeriesCorrupt;
        }
    }
    fn expired(s: Spec, timestamp: i128, now: i128) !bool {
        const end = try std.math.add(i128, try s.bucket(timestamp), s.interval_ns);
        const expiry = std.math.add(i128, end, s.retention_ns) catch return false;
        return expiry <= now;
    }
    pub fn finalized(s: Spec, timestamp: i128, now: i128) !bool {
        const end = try std.math.add(i128, try s.bucket(timestamp), s.interval_ns);
        const deadline = std.math.add(i128, end, s.mutable_for_ns) catch return false;
        return deadline <= now;
    }
};
/// A decoder supplies typed, canonical dimensions and numeric inputs. Missing
/// numeric inputs are null (SQL aggregate null semantics); count counts rows.
pub const Row = struct { timestamp_ns: i128, dimensions: []const u8, values: []const ?f64 };
pub const Change = struct { key: []const u8, before: ?Row = null, after: ?Row = null };
pub const Cause = enum { mutation, expiration };
pub const Progress = struct {
    definition: Id,
    source_incarnation: u64,
    source_sequence: u64,
    admitted_ns: i128,
    complete_from_ns: i128,
    ready: bool = false,
    backfill_cursor: []const u8 = "",
    feed_latest: u64 = 0,
    feed_floor: u64 = 0,
    feed_bytes: usize = 0,
};
pub const Scalar = union(enum) { null, integer: i64, number: f64 };
pub const Metric = struct {
    count: i64 = 0,
    // A finite f64 input times an i64 row count fits in f128. Keep the
    // accumulation domain wider than the SQL result to avoid rejecting an
    // already-decided transaction merely because its float result overflows.
    sum: f128 = 0,
    minimum: ?f64 = null,
    maximum: ?f64 = null,

    /// SQL adapters use this projection to preserve counts beyond 2^53.
    pub fn finalize(self: Metric, op: Law) !Scalar {
        if (op == .count) return .{ .integer = self.count };
        const number = self.value(op) orelse return .null;
        if (!std.math.isFinite(number)) return error.RetainedSeriesNumericOverflow;
        return .{ .number = number };
    }
    /// Numeric display projection. Use finalize for typed query results.
    pub fn value(self: Metric, op: Law) ?f64 {
        return switch (op) {
            .count => @floatFromInt(self.count),
            .sum => if (self.count == 0) null else @floatCast(self.sum),
            .avg => if (self.count == 0) null else @floatCast(self.sum / @as(f128, @floatFromInt(self.count))),
            .min => self.minimum,
            .max => self.maximum,
        };
    }
    /// AVG remains a mergeable (sum,count) state, never an average of averages.
    pub fn merge(left: Metric, right: Metric) !Metric {
        if (left.count < 0 or right.count < 0) return error.RetainedSeriesCorrupt;
        const sum = left.sum + right.sum;
        if (!std.math.isFinite(sum)) return error.RetainedSeriesNumericOverflow;
        return .{
            .count = std.math.add(i64, left.count, right.count) catch return error.RetainedSeriesNumericOverflow,
            .sum = sum,
            .minimum = if (left.minimum) |l| if (right.minimum) |r| @min(l, r) else l else right.minimum,
            .maximum = if (left.maximum) |l| if (right.maximum) |r| @max(l, r) else l else right.maximum,
        };
    }
};
pub const Bucket = struct { start_ns: i128, dimensions: []const u8, rows: i64 = 0, metrics: []Metric };
pub const Edit = struct { key: []const u8, row: ?Bucket };
pub const Frame = struct { sequence: u64, source_sequence: u64, edits: []const Edit };
pub const Page = struct { rows: []const Bucket, after: ?[]const u8, cursor: Cursor };
pub const Cursor = struct { definition: Id, partition: Id, incarnation: u64, sequence: u64 };

pub const Engine = struct {
    alloc: A,
    spec: Spec,

    fn key(self: Engine, a: A, suffix: []const u8) ![]u8 {
        return std.fmt.allocPrint(a, "{s}{s}:{x:0>16}:{s}", .{ prefix, std.fmt.bytesToHex(self.spec.id, .lower), self.spec.generation, suffix });
    }
    fn load(self: Engine, a: A, txn: anytype) !Progress {
        try self.spec.validate();
        const configured = try txn.get(try self.key(a, "definition"));
        if (!std.mem.eql(u8, configured, try std.json.Stringify.valueAlloc(a, self.spec, .{}))) return error.RetainedSeriesDefinitionMismatch;
        const k = try self.key(a, "progress");
        const raw = try txn.get(k);
        const p = (try std.json.parseFromSlice(Progress, a, raw, .{ .allocate = .alloc_always })).value;
        if (!std.mem.eql(u8, &p.definition, &self.spec.definition) or p.source_incarnation != self.spec.source_incarnation) return error.RetainedSeriesDefinitionMismatch;
        if (p.feed_floor > p.feed_latest or p.feed_latest - p.feed_floor > max_feed_frames or p.feed_bytes > max_feed_bytes or
            try self.spec.bucket(p.complete_from_ns) != p.complete_from_ns) return error.RetainedSeriesCorrupt;
        return p;
    }
    pub fn status(self: Engine, a: A, txn: anytype) !Progress {
        return self.load(a, txn);
    }
    fn save(self: Engine, a: A, txn: anytype, p: Progress) !void {
        try txn.put(try self.key(a, "progress"), try std.json.Stringify.valueAlloc(a, p, .{}));
    }
    pub fn install(self: Engine, txn: anytype, sequence: u64, admitted_ns: i128, complete_from_ns: i128) !void {
        try self.spec.validate();
        if (try self.spec.bucket(complete_from_ns) != complete_from_ns) return error.RetainedSeriesResolutionMismatch;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const k = try self.key(a, "progress");
        if ((try optional(txn, k)) != null) return error.RetainedSeriesAlreadyExists;
        try txn.put(try self.key(a, "definition"), try std.json.Stringify.valueAlloc(a, self.spec, .{}));
        try self.save(a, txn, .{ .definition = self.spec.definition, .source_incarnation = self.spec.source_incarnation, .source_sequence = sequence, .admitted_ns = admitted_ns, .complete_from_ns = complete_from_ns });
    }
    fn bucketKey(self: Engine, a: A, row: Row) ![]u8 {
        var digest: Id = undefined;
        std.crypto.hash.sha2.Sha256.hash(row.dimensions, &digest, .{});
        const biased = @as(u128, @bitCast(try self.spec.bucket(row.timestamp_ns))) ^ (@as(u128, 1) << 127);
        return self.key(a, try std.fmt.allocPrint(a, "bucket:{x:0>32}:{s}", .{ biased, std.fmt.bytesToHex(digest, .lower) }));
    }
    fn markerKey(self: Engine, a: A, document: []const u8) ![]u8 {
        return self.key(a, try std.fmt.allocPrint(a, "backfill:{s}", .{try hex(a, document)}));
    }
    fn decodeBucket(self: Engine, a: A, k: []const u8, raw: []const u8) !Bucket {
        const b = (try std.json.parseFromSlice(Bucket, a, raw, .{ .allocate = .alloc_always })).value;
        try self.spec.validateBucket(b);
        if (!std.mem.eql(u8, k, try self.bucketKey(a, .{ .timestamp_ns = b.start_ns, .dimensions = b.dimensions, .values = &.{} }))) return error.RetainedSeriesCorrupt;
        return b;
    }
    fn readBucket(self: Engine, a: A, txn: anytype, k: []const u8) !?Bucket {
        const raw = (try optional(txn, k)) orelse return null;
        return try self.decodeBucket(a, k, raw);
    }
    fn validateRow(self: Engine, r: Row) !void {
        if (!std.unicode.utf8ValidateSlice(r.dimensions) or r.dimensions.len > 4096 or r.values.len != self.spec.laws.len) return error.InvalidRetainedSeriesRow;
        for (r.values) |v| if (v) |n| if (!std.math.isFinite(n)) return error.InvalidRetainedSeriesRow;
        _ = try self.spec.bucket(r.timestamp_ns);
    }
    fn supportPrefix(self: Engine, a: A, k: []const u8, measure: usize) ![]u8 {
        return std.fmt.allocPrint(a, "{s}{s}:{x:0>4}:", .{ try self.key(a, "support:"), k, measure });
    }
    fn supportKey(self: Engine, a: A, k: []const u8, measure: usize, value: f64, document: []const u8) ![]u8 {
        // Normalize signed zero before ordering the IEEE representation.
        const bits: u64 = @bitCast(if (value == 0) @as(f64, 0) else value);
        const ordered = if (bits >> 63 != 0) ~bits else bits ^ (@as(u64, 1) << 63);
        var identity: Id = undefined;
        std.crypto.hash.sha2.Sha256.hash(document, &identity, .{});
        return std.fmt.allocPrint(a, "{s}{x:0>16}:{s}", .{ try self.supportPrefix(a, k, measure), ordered, std.fmt.bytesToHex(identity, .lower) });
    }
    fn extremum(self: Engine, a: A, txn: anytype, k: []const u8, measure: usize, maximum: bool) !?f64 {
        const p = try self.supportPrefix(a, k, measure);
        var cur = try txn.openCursor();
        defer cur.close();
        const entry = (if (maximum) try cur.seekAtOrBefore(try std.mem.concat(a, u8, &.{ p, "~" })) else try cur.seekAtOrAfter(p)) orelse return null;
        if (!std.mem.startsWith(u8, entry.key, p)) return null;
        return try algebra.parseF64(entry.value);
    }
    fn adjust(self: Engine, a: A, txn: anytype, document: []const u8, r: Row, sign: i64, touched: *std.StringHashMap(Bucket)) !void {
        try self.validateRow(r);
        const k = try self.bucketKey(a, r);
        const cached = try touched.getOrPut(k);
        if (!cached.found_existing) {
            cached.value_ptr.* = (try self.readBucket(a, txn, k)) orelse Bucket{ .start_ns = try self.spec.bucket(r.timestamp_ns), .dimensions = r.dimensions, .metrics = try a.alloc(Metric, self.spec.laws.len) };
            if (cached.value_ptr.rows == 0) @memset(cached.value_ptr.metrics, .{});
        }
        const b = cached.value_ptr;
        if (!std.mem.eql(u8, b.dimensions, r.dimensions) or b.metrics.len != self.spec.laws.len) return error.RetainedSeriesCorrupt;
        b.rows = std.math.add(i64, b.rows, sign) catch return error.RetainedSeriesNumericOverflow;
        if (b.rows < 0) return error.RetainedSeriesCorrupt;
        for (self.spec.laws, b.metrics, r.values, 0..) |op, *m, input, i| {
            const n = if (op == .count) value: {
                if (self.spec.countsNonNull(i) and input == null) continue;
                break :value @as(f64, 1);
            } else input orelse continue;
            m.count = std.math.add(i64, m.count, sign) catch return error.RetainedSeriesNumericOverflow;
            if (m.count < 0) return error.RetainedSeriesCorrupt;
            switch (op) {
                .count => {},
                .sum, .avg => {
                    m.sum += @as(f128, @floatFromInt(sign)) * @as(f128, n);
                    if (!std.math.isFinite(m.sum)) return error.RetainedSeriesNumericOverflow;
                    if (m.count == 0) m.sum = 0;
                },
                .min, .max => {
                    const support = try self.supportKey(a, k, i, n, document);
                    if (sign > 0) try txn.put(support, try algebra.encodeF64Alloc(a, n)) else try txn.delete(support);
                },
            }
        }
    }
    /// Save each affected bucket once and resolve extrema after all support
    /// changes. A thousand observations in one bucket need one row write and
    /// one ordered seek per extrema law, not a thousand rewrites/seeks.
    fn flushBuckets(self: Engine, a: A, txn: anytype, touched: *std.StringHashMap(Bucket)) !void {
        var iterator = touched.iterator();
        while (iterator.next()) |entry| {
            const b = entry.value_ptr;
            if (b.rows == 0) {
                try txn.delete(entry.key_ptr.*);
                continue;
            }
            for (self.spec.laws, b.metrics, 0..) |op, *metric, i| {
                if (op == .min) metric.minimum = if (metric.count == 0) null else (try self.extremum(a, txn, entry.key_ptr.*, i, false)) orelse return error.RetainedSeriesCorrupt;
                if (op == .max) metric.maximum = if (metric.count == 0) null else (try self.extremum(a, txn, entry.key_ptr.*, i, true)) orelse return error.RetainedSeriesCorrupt;
            }
            try txn.put(entry.key_ptr.*, try std.json.Stringify.valueAlloc(a, b.*, .{}));
        }
    }
    /// Synchronous maintenance makes the primary row and maintained progress
    /// share one cut. Outside declared coverage/retention there is no hold.
    pub fn expirationReady(self: Engine, a: A, txn: anytype, progress: Progress, row: Row, admitted_ns: i128) !bool {
        try self.validateRow(row);
        if (try self.spec.bucket(row.timestamp_ns) < progress.complete_from_ns) return true;
        const now = @max(progress.admitted_ns, admitted_ns);
        if (try self.spec.expired(row.timestamp_ns, now)) return true;
        if (!progress.ready) return false;
        if (!try self.spec.finalized(row.timestamp_ns, now)) return false;
        return (try self.readBucket(a, txn, try self.bucketKey(a, row))) != null;
    }
    /// Exactly one call per committed source command. A replay at/below the
    /// checkpoint is rejected, never silently applied twice. Expiration is a
    /// distinct operation: it cannot retract historical aggregates.
    pub fn apply(self: Engine, txn: anytype, sequence: u64, admitted_ns: i128, cause: Cause, changes: []const Change) !void {
        if (changes.len > max_changes) return error.RetainedSeriesBatchTooLarge;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var p = try self.load(a, txn);
        if (sequence <= p.source_sequence) return error.RetainedSeriesSequenceMismatch;
        const now = @max(admitted_ns, p.admitted_ns);
        var touched = std.StringHashMap(Bucket).init(a);
        var seen = std.StringHashMap(void).init(a);
        for (changes) |change| {
            if (change.key.len == 0 or change.key.len > 4096) return error.InvalidRetainedSeriesRow;
            // Caller must coalesce a transaction's multiple writes to one row.
            const entry = try seen.getOrPut(change.key);
            if (entry.found_existing) return error.RetainedSeriesDuplicateRow;
            if (change.before) |r| try self.validateRow(r);
            if (change.after) |r| try self.validateRow(r);
            if (cause == .expiration) {
                if (change.after != null) return error.RetainedSeriesNotReady;
                if (change.before) |r| {
                    if (try self.spec.bucket(r.timestamp_ns) < p.complete_from_ns or try self.spec.expired(r.timestamp_ns, now)) continue;
                    if (!p.ready) return error.RetainedSeriesNotReady;
                    if (!try self.spec.finalized(r.timestamp_ns, now)) return error.RetainedSeriesCorrectionWindow;
                    // Drop mutable extrema support only after finality. The
                    // cached aggregate survives source physical reclamation.
                    const k = try self.bucketKey(a, r);
                    if (!try self.spec.expired(r.timestamp_ns, now) and try self.readBucket(a, txn, k) == null) return error.RetainedSeriesCoverageMissing;
                    for (self.spec.laws, r.values, 0..) |law, input, m| if ((law == .min or law == .max) and input != null) {
                        try txn.delete(try self.supportKey(a, k, m, input.?, change.key));
                    };
                }
            } else {
                if (change.before != null and change.after != null and try sameContribution(self.spec, change.before.?, change.after.?)) continue;
                // Logical expiry removes this dataset's dependency on old
                // contributions. A move between expired and current history
                // adjusts only the current side, even before physical cleanup.
                const before = if (change.before) |r| if (try self.spec.expired(r.timestamp_ns, now)) null else r else null;
                const after = if (change.after) |r| if (try self.spec.expired(r.timestamp_ns, now)) null else r else null;
                if (before) |r| if (try self.spec.bucket(r.timestamp_ns) < p.complete_from_ns) return error.RetainedSeriesCoverageMissing;
                if (after) |r| if (try self.spec.bucket(r.timestamp_ns) < p.complete_from_ns) return error.RetainedSeriesCoverageMissing;
                if (before) |r| if (try self.spec.finalized(r.timestamp_ns, now)) return error.RetainedSeriesCorrectionWindow;
                if (after) |r| if (try self.spec.finalized(r.timestamp_ns, now)) return error.RetainedSeriesCorrectionWindow;
                const marker = try self.markerKey(a, change.key);
                const incorporated = p.ready or (try optional(txn, marker)) != null;
                if (incorporated) if (before) |r| try self.adjust(a, txn, change.key, r, -1, &touched);
                if (after) |r| try self.adjust(a, txn, change.key, r, 1, &touched);
                if (!p.ready) try txn.put(marker, "1");
            }
        }
        p.source_sequence = sequence;
        p.admitted_ns = now;
        try self.flushBuckets(a, txn, &touched);
        try self.publish(a, txn, &p, &touched);
        try self.save(a, txn, p);
    }
    /// The caller scans a bounded current-source page under the same writer
    /// fence. Markers make an intervening mutation/backfill race idempotent.
    /// Cursor monotonicity is part of the durable command, not worker memory.
    pub fn backfill(self: Engine, txn: anytype, expected_cursor: []const u8, rows: []const Change, complete: bool) !void {
        const through = if (rows.len == 0) expected_cursor else rows[rows.len - 1].key;
        return self.backfillScanned(txn, expected_cursor, rows, through, complete);
    }
    pub fn backfillScanned(self: Engine, txn: anytype, expected_cursor: []const u8, rows: []const Change, through: []const u8, complete: bool) !void {
        if (rows.len > max_changes or (!complete and std.mem.order(u8, through, expected_cursor) != .gt)) return error.RetainedSeriesBatchTooLarge;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var p = try self.load(a, txn);
        if (p.ready or !std.mem.eql(u8, expected_cursor, p.backfill_cursor)) return error.RetainedSeriesBackfillFence;
        var touched = std.StringHashMap(Bucket).init(a);
        var previous = expected_cursor;
        for (rows) |row| {
            if (row.before != null or row.after == null or row.key.len == 0 or row.key.len > 4096 or std.mem.order(u8, previous, row.key) != .lt) return error.RetainedSeriesBackfillFence;
            const marker = try self.markerKey(a, row.key);
            if ((try optional(txn, marker)) == null and try self.spec.bucket(row.after.?.timestamp_ns) >= p.complete_from_ns and !try self.spec.expired(row.after.?.timestamp_ns, p.admitted_ns)) {
                try self.adjust(a, txn, row.key, row.after.?, 1, &touched);
                try txn.put(marker, "1");
            }
            previous = row.key;
        }
        if (std.mem.order(u8, through, previous) == .lt) return error.RetainedSeriesBackfillFence;
        p.backfill_cursor = through;
        p.ready = complete;
        try self.flushBuckets(a, txn, &touched);
        try self.publish(a, txn, &p, &touched);
        try self.save(a, txn, p);
    }
    fn frameKey(self: Engine, a: A, sequence: u64) ![]u8 {
        return self.key(a, try std.fmt.allocPrint(a, "feed:{x:0>16}", .{sequence}));
    }
    fn publish(self: Engine, a: A, txn: anytype, p: *Progress, touched: anytype) !void {
        if (touched.count() == 0) return;
        const edits = try a.alloc(Edit, touched.count());
        var it = touched.keyIterator();
        var i: usize = 0;
        while (it.next()) |k| : (i += 1) edits[i] = .{ .key = k.*, .row = try self.readBucket(a, txn, k.*) };
        std.mem.sort(Edit, edits, {}, struct {
            fn less(_: void, l: Edit, r: Edit) bool {
                return std.mem.order(u8, l.key, r.key) == .lt;
            }
        }.less);
        // Split one atomic source command into bounded keyed-upsert frames.
        // Feed pressure may evict earlier frames in this command; consumers
        // then reset to a current snapshot rather than blocking source commits.
        var offset: usize = 0;
        while (offset < edits.len) {
            const end = @min(offset + 8, edits.len);
            p.feed_latest = std.math.add(u64, p.feed_latest, 1) catch return error.RetainedSeriesSequenceMismatch;
            const raw = try std.json.Stringify.valueAlloc(a, Frame{ .sequence = p.feed_latest, .source_sequence = p.source_sequence, .edits = edits[offset..end] }, .{});
            if (raw.len > max_frame_bytes) return error.RetainedSeriesBatchTooLarge;
            try txn.put(try self.frameKey(a, p.feed_latest), raw);
            p.feed_bytes += raw.len;
            while (p.feed_latest - p.feed_floor > max_feed_frames or p.feed_bytes > max_feed_bytes) {
                p.feed_floor += 1;
                const k = try self.frameKey(a, p.feed_floor);
                const previous = try txn.get(k);
                p.feed_bytes = std.math.sub(usize, p.feed_bytes, previous.len) catch return error.RetainedSeriesCorrupt;
                try txn.delete(k);
            }
            offset = end;
        }
    }

    /// Replicate canonical clock ticks even on an idle source. Logical expiry
    /// produces keyed removals; mutable extrema support is reclaimed separately
    /// within the same work bound. This does not depend on raw TTL being enabled.
    pub fn tick(self: Engine, txn: anytype, admitted_ns: i128, limit: usize) !usize {
        if (limit == 0 or limit > max_changes) return error.RetainedSeriesBatchTooLarge;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var state = try self.load(a, txn);
        if (!state.ready) return error.RetainedSeriesNotReady;
        state.admitted_ns = @max(state.admitted_ns, admitted_ns);
        var touched = std.StringHashMap(void).init(a);
        const expired = try a.alloc([]const u8, limit);
        var n: usize = 0;
        {
            const p = try self.key(a, "bucket:");
            var cur = try txn.openCursor();
            defer cur.close();
            var next = try cur.seekAtOrAfter(p);
            while (next) |entry| {
                if (n == limit or !std.mem.startsWith(u8, entry.key, p)) break;
                const b = try self.decodeBucket(a, entry.key, entry.value);
                if (!try self.spec.expired(b.start_ns, state.admitted_ns)) break;
                expired[n] = try a.dupe(u8, entry.key);
                n += 1;
                next = try cur.next();
            }
        }
        for (expired[0..n]) |k| {
            try txn.delete(k);
            try touched.put(k, {});
        }
        const support = try self.key(a, "support:");
        const buckets = try self.key(a, "bucket:");
        var supports: std.ArrayList([]const u8) = .empty;
        {
            var cur = try txn.openCursor();
            defer cur.close();
            var next = try cur.seekAtOrAfter(support);
            while (next) |entry| {
                if (n + supports.items.len == limit or !std.mem.startsWith(u8, entry.key, support)) break;
                const offset = support.len + buckets.len;
                if (entry.key.len < offset + 32 or !std.mem.startsWith(u8, entry.key[support.len..], buckets)) return error.RetainedSeriesCorrupt;
                const biased = std.fmt.parseInt(u128, entry.key[offset..][0..32], 16) catch return error.RetainedSeriesCorrupt;
                const start: i128 = @bitCast(biased ^ (@as(u128, 1) << 127));
                if (!try self.spec.finalized(start, state.admitted_ns)) break;
                try supports.append(a, try a.dupe(u8, entry.key));
                next = try cur.next();
            }
        }
        for (supports.items) |k| try txn.delete(k);
        try self.publish(a, txn, &state, &touched);
        try self.save(a, txn, state);
        return n + supports.items.len;
    }
    /// Indexed half-open bucket range. Input boundaries must align with the
    /// definition; partial buckets cannot be presented as exact raw aggregates.
    /// Pagination must retain this immutable transaction (or restart a snapshot).
    pub fn scan(self: Engine, a: A, txn: anytype, start_ns: i128, end_ns: i128, admitted_ns: i128, after: ?[]const u8, limit: usize) !Page {
        if (limit == 0 or limit > max_changes or start_ns >= end_ns or try self.spec.bucket(start_ns) != start_ns or try self.spec.bucket(end_ns) != end_ns) return error.RetainedSeriesResolutionMismatch;
        const cut = try self.snapshotCursor(txn);
        const state = try self.load(a, txn);
        if (start_ns < state.complete_from_ns) return error.RetainedSeriesCoverageMissing;
        if (try self.spec.expired(start_ns, @max(admitted_ns, state.admitted_ns))) return error.RetainedSeriesHistoryExpired;
        const p = try self.key(a, "bucket:");
        const biased = @as(u128, @bitCast(start_ns)) ^ (@as(u128, 1) << 127);
        const lower = try std.fmt.allocPrint(a, "{s}{x:0>32}:", .{ p, biased });
        var seek = lower;
        if (after) |previous| {
            if (!std.mem.startsWith(u8, previous, p) or previous.len != p.len + 32 + 1 + 64 or std.mem.order(u8, previous, lower) == .lt) return error.RetainedSeriesResetRequired;
            seek = try std.mem.concat(a, u8, &.{ previous, "\x00" });
        }
        const rows = try a.alloc(Bucket, limit);
        var count: usize = 0;
        var last: ?[]const u8 = null;
        var cur = try txn.openCursor();
        defer cur.close();
        var entry = try cur.seekAtOrAfter(seek);
        while (entry) |e| {
            if (!std.mem.startsWith(u8, e.key, p)) break;
            const b = try self.decodeBucket(a, e.key, e.value);
            if (b.start_ns >= end_ns) break;
            if (b.start_ns < start_ns) return error.RetainedSeriesCorrupt;
            if (count == limit) return .{ .rows = rows[0..count], .after = last, .cursor = cut };
            rows[count] = b;
            count += 1;
            last = try a.dupe(u8, e.key);
            entry = try cur.next();
        }
        return .{ .rows = rows[0..count], .after = null, .cursor = cut };
    }
    /// Temporary race markers are bounded by the initial source/backfill, not
    /// retained forever per historical document. Each command removes one page.
    pub fn cleanupBackfill(self: Engine, txn: anytype, limit: usize) !usize {
        if (limit == 0 or limit > max_changes) return error.RetainedSeriesBatchTooLarge;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const p = try self.load(a, txn);
        if (!p.ready) return error.RetainedSeriesNotReady;
        const marker = try self.key(a, "backfill:");
        const keys = try a.alloc([]const u8, limit);
        var n: usize = 0;
        {
            var cur = try txn.openCursor();
            defer cur.close();
            var e = try cur.seekAtOrAfter(marker);
            while (e) |item| {
                if (n == limit or !std.mem.startsWith(u8, item.key, marker)) break;
                keys[n] = try a.dupe(u8, item.key);
                n += 1;
                e = try cur.next();
            }
        }
        for (keys[0..n]) |k| try txn.delete(k);
        return n;
    }
    /// Read this and snapshot rows in ONE immutable read transaction for the
    /// snapshot/replay handoff. The transport authenticates/binds the token.
    pub fn snapshotCursor(self: Engine, txn: anytype) !Cursor {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const p = try self.load(arena.allocator(), txn);
        if (!p.ready) return error.RetainedSeriesNotReady;
        return .{ .definition = p.definition, .partition = self.spec.source_partition, .incarnation = p.source_incarnation, .sequence = p.feed_latest };
    }
    /// Returned frames belong to the caller arena. No unbounded replay scan.
    pub fn poll(self: Engine, a: A, txn: anytype, cursor: Cursor, limit: usize) ![]const Frame {
        if (limit == 0 or limit > max_feed_frames) return error.RetainedSeriesBatchTooLarge;
        const p = try self.load(a, txn);
        if (!p.ready) return error.RetainedSeriesNotReady;
        if (!std.mem.eql(u8, &cursor.definition, &p.definition) or !std.mem.eql(u8, &cursor.partition, &self.spec.source_partition) or cursor.incarnation != p.source_incarnation or cursor.sequence < p.feed_floor or cursor.sequence > p.feed_latest) return error.RetainedSeriesResetRequired;
        const count: usize = @intCast(@min(p.feed_latest - cursor.sequence, limit));
        const frames = try a.alloc(Frame, count);
        var total_bytes: usize = 0;
        const bucket_prefix = try self.key(a, "bucket:");
        for (frames, 0..) |*frame, i| {
            const raw = try txn.get(try self.frameKey(a, cursor.sequence + i + 1));
            if (raw.len > max_frame_bytes or raw.len > max_feed_bytes - total_bytes) return error.RetainedSeriesCorrupt;
            total_bytes += raw.len;
            frame.* = (try std.json.parseFromSlice(Frame, a, raw, .{ .allocate = .alloc_always })).value;
            if (frame.edits.len == 0 or frame.edits.len > 8) return error.RetainedSeriesCorrupt;
            var previous: ?[]const u8 = null;
            for (frame.edits) |edit| {
                if (!std.mem.startsWith(u8, edit.key, bucket_prefix) or edit.key.len != bucket_prefix.len + 97 or edit.key[bucket_prefix.len + 32] != ':' or
                    (previous != null and std.mem.order(u8, previous.?, edit.key) != .lt)) return error.RetainedSeriesCorrupt;
                const suffix = edit.key[bucket_prefix.len..];
                for (suffix, 0..) |c, n| if (n != 32 and !std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.RetainedSeriesCorrupt;
                const biased = std.fmt.parseInt(u128, suffix[0..32], 16) catch return error.RetainedSeriesCorrupt;
                const instant: i128 = @bitCast(biased ^ (@as(u128, 1) << 127));
                if (try self.spec.bucket(instant) != instant) return error.RetainedSeriesCorrupt;
                if (edit.row) |row| {
                    try self.spec.validateBucket(row);
                    if (!std.mem.eql(u8, edit.key, try self.bucketKey(a, .{ .timestamp_ns = row.start_ns, .dimensions = row.dimensions, .values = &.{} }))) return error.RetainedSeriesCorrupt;
                }
                previous = edit.key;
            }
            if (frame.sequence != cursor.sequence + i + 1 or frame.source_sequence > p.source_sequence) return error.RetainedSeriesCorrupt;
        }
        return frames;
    }
};
fn optional(txn: anytype, k: []const u8) !?[]const u8 {
    return txn.get(k) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
fn hex(a: A, bytes: []const u8) ![]u8 {
    const result = try a.alloc(u8, bytes.len * 2);
    const alphabet = "0123456789abcdef";
    for (bytes, 0..) |byte, i| {
        result[i * 2] = alphabet[byte >> 4];
        result[i * 2 + 1] = alphabet[byte & 15];
    }
    return result;
}

test {
    _ = @import("retained_series_tests.zig");
}

fn sameContribution(spec: Spec, left: Row, right: Row) !bool {
    if (try spec.bucket(left.timestamp_ns) != try spec.bucket(right.timestamp_ns) or !std.mem.eql(u8, left.dimensions, right.dimensions)) return false;
    for (left.values, right.values) |l, r| if (l != r) return false;
    return true;
}
