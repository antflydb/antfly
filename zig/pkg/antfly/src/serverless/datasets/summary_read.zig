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

//! Shared archive scan for retained relations. Plans certify complete coverage;
//! pins protect physical files only during decoding. Consumers own copied state
//! and do not pin source TTL or hold archive leases while streaming to clients.
const std = @import("std");
const t = @import("types.zig");
const catalog = @import("catalog.zig");
const series = @import("antfly_local_sources").storage_retained_series;
const codec = @import("antfly_local_sources").storage_retained_series_archive;
const A = std.mem.Allocator;
const closure = @import("closure_manifest.zig");
pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    rows: []const series.Bucket,
    next: ?t.RowKey = null,
    bytes_read: usize = 0,
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Limits = struct { segments: usize = 32, rows: usize = 65536, bytes: usize = 64 * 1024 * 1024, read_clock_ns: i128 = 0, after: ?t.RowKey = null, dimensions_json: ?[]const u8 = null, page: bool = false };

pub fn successor(key: t.RowKey) !t.RowKey {
    var result = key;
    var i = result.len;
    const digits = "0123456789abcdef";
    while (i > 0) {
        i -= 1;
        const value = std.mem.indexOfScalar(u8, digits, result[i]) orelse return error.InvalidDatasetCoverage;
        if (value < 15) {
            result[i] = digits[value + 1];
            return result;
        }
        result[i] = '0';
    }
    return error.InvalidDatasetCoverage;
}
pub fn predecessor(key: t.RowKey) !t.RowKey {
    var result = key;
    var i = result.len;
    const digits = "0123456789abcdef";
    while (i > 0) {
        i -= 1;
        const value = std.mem.indexOfScalar(u8, digits, result[i]) orelse return error.InvalidDatasetCoverage;
        if (value > 0) {
            result[i] = digits[value - 1];
            return result;
        }
        result[i] = 'f';
    }
    return error.InvalidDatasetCoverage;
}

/// `resolve` is a host capability with resolve.location(id) returning Location.
/// The caller authorizes the independent dataset resource before invoking this.
/// This bounded materialization is also used beneath streaming query operators;
/// a result that exceeds admission limits fails explicitly, never truncates.
pub fn read(a: A, store: catalog.Catalog, definition: t.Definition, spec: series.Spec, range: t.Range, limits: Limits, resolve: anytype) !Result {
    return readImpl(a, store, definition, spec, range, limits, null, resolve);
}

/// The caller supplies the receipt selected by metadata authority. A missing
/// manifest or object fails the read, including after the native owner is gone.
pub fn readClosed(a: A, store: catalog.Catalog, definition: t.Definition, spec: series.Spec, range: t.Range, limits: Limits, cut: closure.Cut, receipt: t.Id, resolve: anytype) !Result {
    const manifest = (try store.getClosedExport(spec, cut)) orelse return error.DatasetDependencyPending;
    if (!std.mem.eql(u8, &try manifest.receipt(), &receipt)) return error.InvalidDatasetCoverage;
    return readImpl(a, store, definition, spec, range, limits, manifest, resolve);
}

// One immutable file is retained between monotone seeks. A file spanning many
// buckets is decoded and charged once, and tuple seeks binary-search its keys.
const DecodeCache = struct {
    arena: std.heap.ArenaAllocator,
    id: ?t.Id = null,
    checksum: t.Id = undefined,
    decoded: codec.Decoded = undefined,
    keys: []const t.RowKey = &.{},
    dimensions: ?[]const u8 = null,
    tuple_times: []const i128 = &.{},
    read_clock: i128 = 0,
    fn matches(self: *const DecodeCache, segment: t.Segment) bool {
        return if (self.id) |id| std.mem.eql(u8, &id, &segment.id) and std.mem.eql(u8, &self.checksum, &segment.checksum) else false;
    }
    fn load(self: *DecodeCache, store: catalog.Catalog, spec: series.Spec, segment: t.Segment, resolve: anytype) !void {
        if (self.matches(segment)) return;
        self.id = null;
        _ = self.arena.reset(.free_all);
        const lease = try store.acquire(segment.id);
        defer store.release(lease) catch {};
        const location = try resolve.location(lease.pin.location);
        const bytes = try store.read(lease, location);
        defer store.context.alloc.free(bytes);
        if (try codec.formatEncoding(bytes) != segment.state_encoding) return error.InvalidDatasetSegment;
        const owned_bytes = try self.arena.allocator().dupe(u8, bytes);
        self.decoded = try codec.decode(self.arena.allocator(), spec, owned_bytes);
        const keys = try self.arena.allocator().alloc(t.RowKey, self.decoded.rows.len);
        var times = std.ArrayList(i128).empty;
        for (keys, self.decoded.rows) |*key, row| {
            key.* = codec.key(row);
            if (self.dimensions) |dimensions| if (std.mem.eql(u8, dimensions, row.dimensions)) try times.append(self.arena.allocator(), row.start_ns);
        }
        self.tuple_times = times.items;
        self.keys = keys;
        self.checksum = segment.checksum;
        self.id = segment.id;
    }
    fn nextTupleBucket(self: *const DecodeCache, start: i128) !i128 {
        var lo: usize = 0;
        var hi = self.tuple_times.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.tuple_times[mid] < start) lo = mid + 1 else hi = mid;
        }
        // Absence is certified only through this immutable file's coverage.
        const end = try @import("antfly_local_sources").retained_dataset_types.rowKeyTimestamp(self.decoded.receipt.span.end);
        return @max(start, @min(end, if (lo < self.tuple_times.len) self.tuple_times[lo] else end));
    }
    fn lowerBound(self: *const DecodeCache, key: t.RowKey) usize {
        var lo: usize = 0;
        var hi = self.keys.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, &self.keys[mid], &key) == .lt) lo = mid + 1 else hi = mid;
        }
        return lo;
    }
};

fn validateInputs(a: A, store: catalog.Catalog, definition: t.Definition, spec: series.Spec, range: t.Range, limits: Limits, allow_empty_budget: bool) !void {
    try definition.validate();
    try spec.validate();
    try range.validate();
    const summary = definition.summary orelse return error.InvalidDatasetDefinition;
    if (!std.mem.eql(u8, &definition.id, &store.dataset) or !std.mem.eql(u8, &definition.id, &spec.id) or
        definition.generation != spec.generation or !std.mem.eql(u8, &try definition.fingerprint(a), &spec.definition) or
        summary.interval_ns != spec.interval_ns or summary.mutable_for_ns != spec.mutable_for_ns or definition.retention_ns != spec.retention_ns or
        try std.fmt.parseInt(i128, summary.origin_ns, 10) != spec.origin_ns or summary.measures.len != spec.laws.len) return error.RetainedSeriesDefinitionMismatch;
    for (summary.measures, spec.laws, 0..) |measure, law, i| {
        if (measure.law != law or (law == .count and (measure.field != null) != spec.countsNonNull(i))) return error.RetainedSeriesDefinitionMismatch;
    }
    if (limits.segments == 0 or limits.segments > catalog.max_page or limits.rows == 0 or limits.rows > 128 * 1024 or (limits.bytes == 0 and !allow_empty_budget) or limits.bytes > 256 * 1024 * 1024) return error.DatasetPageBudgetExceeded;
    if (try spec.bucket(try range.start()) != try range.start() or try spec.bucket(try range.end()) != try range.end()) return error.RetainedSeriesResolutionMismatch;
}

// Complete consumers traverse bounded pages. Exact tuples seek one key per
// bucket, avoiding scans of unrelated dimension extents after archive handoff.
fn readImpl(a: A, store: catalog.Catalog, definition: t.Definition, spec: series.Spec, range: t.Range, limits: Limits, closed: ?closure.Manifest, resolve: anytype) !Result {
    try validateInputs(a, store, definition, spec, range, limits, false);
    // Validate the original range before clipping it to a continuation or
    // individual bucket. A later cursor never bypasses expired history.
    const now = store.clock.unix();
    if (now == 0) return error.DatasetLeaseExpired;
    const clock = try store.admitRetentionClock(spec, @max(@max(@as(i128, now), limits.read_clock_ns), if (closed) |manifest| manifest.cut.admitted_ns else 0));
    if (try spec.expired(try range.start(), clock)) return error.RetainedSeriesHistoryExpired;
    if (closed) |manifest| {
        if (manifest.expired_before_ns) |floor| if (try range.start() < floor) return error.RetainedSeriesHistoryExpired;
        if (try range.start() < try @import("antfly_local_sources").retained_dataset_types.rowKeyTimestamp(manifest.first)) return error.RetainedSeriesCoverageMissing;
    }
    var cache: DecodeCache = .{ .arena = std.heap.ArenaAllocator.init(a), .dimensions = limits.dimensions_json, .read_clock = clock };
    defer cache.arena.deinit();
    if (limits.page and limits.dimensions_json == null) return readPage(a, store, definition, spec, range, limits, closed, resolve, &cache);
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const temp = arena.allocator();
    var rows = std.ArrayList(series.Bucket).empty;
    var row_bytes: usize = 0;
    var bytes_read: usize = 0;
    var after = limits.after;
    var start = try range.start();
    var end = try range.end();
    if (closed) |manifest| end = @min(end, try @import("antfly_local_sources").retained_dataset_types.rowKeyTimestamp(manifest.next));
    if (after) |key| start = @max(start, try spec.bucket(try @import("antfly_local_sources").retained_dataset_types.rowKeyTimestamp(key)));
    // A complete materialization also has an operation bound, independent of
    // file/row sizes; empty or very sparse histories cannot monopolize a worker.
    const work_limit = if (limits.page) limits.segments else 8192;
    var steps: usize = 0;
    var next: ?t.RowKey = null;
    while (start < end) {
        if (store.context.cancellation) |token| try token.check();
        if (steps == work_limit) {
            if (!limits.page) return error.DatasetPageBudgetExceeded;
            next = after orelse return error.DatasetPageBudgetExceeded;
            break;
        }
        steps += 1;
        var start_buffer: [40]u8 = undefined;
        var end_buffer: [40]u8 = undefined;
        const part_end = if (limits.dimensions_json != null) @min(end, try std.math.add(i128, start, spec.interval_ns)) else end;
        var part = readPage(a, store, definition, spec, .{
            .start_ns = try std.fmt.bufPrint(&start_buffer, "{d}", .{start}),
            .end_ns = try std.fmt.bufPrint(&end_buffer, "{d}", .{part_end}),
        }, .{ .segments = limits.segments, .rows = @max(1, limits.rows - rows.items.len), .bytes = limits.bytes - bytes_read, .read_clock_ns = limits.read_clock_ns, .after = after, .dimensions_json = limits.dimensions_json, .page = true }, closed, resolve, &cache) catch |err| {
            if (err == error.DatasetPageBudgetExceeded and limits.page and bytes_read != 0) {
                next = after orelse return err;
                break;
            }
            return err;
        };
        defer part.deinit();
        bytes_read += part.bytes_read;
        for (part.rows) |row| {
            if (rows.items.len == limits.rows) return error.DatasetPageBudgetExceeded;
            const cost = @sizeOf(series.Bucket) + row.dimensions.len + row.metrics.len * @sizeOf(series.Metric);
            if (cost > 32 * 1024 * 1024 - row_bytes) return error.DatasetPageBudgetExceeded;
            row_bytes += cost;
            try rows.append(temp, .{ .start_ns = row.start_ns, .dimensions = try temp.dupe(u8, row.dimensions), .rows = row.rows, .metrics = try temp.dupe(series.Metric, row.metrics) });
        }
        if (part.next) |key| {
            if (after) |previous| if (std.mem.order(u8, &key, &previous) != .gt) return error.InvalidDatasetCoverage;
            after = key;
        } else {
            start = part_end;
            if (limits.dimensions_json != null and cache.id != null) start = @min(end, try cache.nextTupleBucket(start));
            if (start < end) after = try predecessor(t.rowKey(start, @splat(0)));
        }
        if (limits.page and rows.items.len == limits.rows) {
            next = if (part.next != null or start < end) after else null;
            break;
        }
    }
    // Preserve range/retention validation for empty tails as well.
    if (steps == 0) {
        const empty = try readPage(a, store, definition, spec, range, limits, closed, resolve, &cache);
        arena.deinit();
        return empty;
    }
    if (try spec.expired(try range.start(), cache.read_clock)) return error.RetainedSeriesHistoryExpired;
    return .{ .arena = arena, .rows = rows.items, .next = next, .bytes_read = bytes_read };
}

fn readPage(a: A, store: catalog.Catalog, definition: t.Definition, spec: series.Spec, range: t.Range, limits: Limits, closed: ?closure.Manifest, resolve: anytype, cache: *DecodeCache) !Result {
    try validateInputs(a, store, definition, spec, range, limits, true);
    const now = store.clock.unix();
    if (now == 0) return error.DatasetLeaseExpired;
    // File retention follows its newest covered bucket. Older rows in that
    // same file still obey their own bucket lifetime, exactly as native scans.
    var read_clock = try store.admitRetentionClock(spec, @max(@max(@as(i128, now), limits.read_clock_ns), cache.read_clock));
    if (try spec.expired(try range.start(), read_clock)) return error.RetainedSeriesHistoryExpired;
    var scan_range = range;
    var tail_buffer: [40]u8 = undefined;
    if (closed) |manifest| {
        const keyTimestamp = @import("antfly_local_sources").retained_dataset_types.rowKeyTimestamp;
        read_clock = @max(read_clock, manifest.cut.admitted_ns);
        const start = try range.start();
        if (manifest.expired_before_ns) |floor| {
            if (start < floor) return error.RetainedSeriesHistoryExpired;
        }
        if (try spec.expired(start, read_clock)) return error.RetainedSeriesHistoryExpired;
        if (start < try keyTimestamp(manifest.first)) return error.RetainedSeriesCoverageMissing;
        const tail = try keyTimestamp(manifest.next);
        if (start >= tail) return .{ .arena = std.heap.ArenaAllocator.init(a), .rows = &.{} };
        if (try range.end() > tail) scan_range.end_ns = try std.fmt.bufPrint(&tail_buffer, "{d}", .{tail});
    }
    var requested = try t.KeySpan.fromRange(scan_range);
    if (limits.after) |after| {
        const next = try successor(after);
        if (std.mem.order(u8, &next, &requested.start) == .gt) requested.start = next;
    }
    // A point dimension tuple in one bucket maps directly to one coverage
    // extent, even when the bucket spans thousands of immutable files.
    if (limits.dimensions_json) |dimensions| if ((std.math.add(i128, try range.start(), spec.interval_ns) catch return error.InvalidDatasetCoverage) == try range.end()) {
        const key = codec.key(.{ .start_ns = try range.start(), .dimensions = dimensions, .rows = 1, .metrics = &.{} });
        if (std.mem.order(u8, &key, &requested.start) == .lt) return .{ .arena = std.heap.ArenaAllocator.init(a), .rows = &.{} };
        requested.start = key;
        requested.end = try successor(key);
    };
    if (std.mem.order(u8, &requested.start, &requested.end) != .lt) return .{ .arena = std.heap.ArenaAllocator.init(a), .rows = &.{} };
    var plan = try store.planCoveringKeys(spec.generation, spec.source_partition, spec.source_incarnation, requested, limits.segments, limits.page);
    defer plan.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const temp = arena.allocator();
    var rows = std.ArrayList(series.Bucket).empty;
    var total_bytes: usize = 0;
    var next_key: ?t.RowKey = if (plan.next) |end| try predecessor(end) else null;
    segment_loop: for (plan.segments) |segment| {
        if ((segment.state_encoding != 1 and segment.state_encoding != codec.encoding) or !segment.finalized or !std.mem.eql(u8, &segment.schema, &definition.schema)) return error.InvalidDatasetSegment;
        const cost: usize = if (cache.matches(segment)) 0 else @intCast(segment.byte_len);
        if (cost > limits.bytes - total_bytes) {
            if (!limits.page or total_bytes == 0) return error.DatasetPageBudgetExceeded;
            next_key = try predecessor((try segment.span()).start);
            break;
        }
        total_bytes += cost;
        try cache.load(store, spec, segment, resolve);
        const decoded = cache.decoded;
        if (decoded.receipt.source_sequence != segment.source.sequence or !std.meta.eql(decoded.receipt.span, try segment.span())) return error.InvalidDatasetCoverage;
        if (closed) |manifest| {
            if (!decoded.receipt.source_closed or decoded.receipt.source_sequence != manifest.cut.sequence or
                decoded.receipt.complete_from_ns != manifest.cut.complete_from_ns or decoded.receipt.admitted_ns != manifest.cut.admitted_ns or
                !std.mem.eql(u8, &segment.id, &closure.segmentId(manifest.cut.closure, decoded.receipt.span))) return error.InvalidDatasetCoverage;
        }
        // Only a receipt verified against catalog authority may advance the
        // durable watermark; rejected objects must not expire other history.
        if (decoded.receipt.admitted_ns > read_clock)
            read_clock = try store.admitRetentionClock(spec, decoded.receipt.admitted_ns);
        const offset = cache.lowerBound(requested.start);
        for (decoded.rows[offset..], cache.keys[offset..]) |row, key| {
            if (std.mem.order(u8, &key, &requested.end) != .lt) break;
            if (limits.dimensions_json) |dimensions| if (!std.mem.eql(u8, dimensions, row.dimensions)) continue;
            if (rows.items.len == limits.rows) {
                if (!limits.page) return error.DatasetPageBudgetExceeded;
                next_key = codec.key(rows.items[rows.items.len - 1]);
                break :segment_loop;
            }
            var owned = row;
            owned.dimensions = try temp.dupe(u8, row.dimensions);
            owned.metrics = try temp.dupe(series.Metric, row.metrics);
            try rows.append(temp, owned);
        }
    }
    cache.read_clock = @max(cache.read_clock, read_clock);
    if (try spec.expired(try range.start(), read_clock)) return error.RetainedSeriesHistoryExpired;
    return .{ .arena = arena, .rows = rows.items, .next = next_key, .bytes_read = total_bytes };
}
