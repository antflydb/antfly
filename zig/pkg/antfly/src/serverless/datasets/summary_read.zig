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
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Limits = struct { segments: usize = 32, rows: usize = 65536, bytes: usize = 64 * 1024 * 1024, read_clock_ns: i128 = 0 };

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

fn readImpl(a: A, store: catalog.Catalog, definition: t.Definition, spec: series.Spec, range: t.Range, limits: Limits, closed: ?closure.Manifest, resolve: anytype) !Result {
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
    if (limits.segments == 0 or limits.segments > catalog.max_page or limits.rows == 0 or limits.rows > codec.max_rows or limits.bytes == 0 or limits.bytes > 256 * 1024 * 1024) return error.DatasetPageBudgetExceeded;
    if (try spec.bucket(try range.start()) != try range.start() or try spec.bucket(try range.end()) != try range.end()) return error.RetainedSeriesResolutionMismatch;
    const now = store.clock.unix();
    if (now == 0) return error.DatasetLeaseExpired;
    // File retention follows its newest covered bucket. Older rows in that
    // same file still obey their own bucket lifetime, exactly as native scans.
    var read_clock = @max(@as(i128, now), limits.read_clock_ns);
    if (try spec.expired(try range.start(), read_clock)) return error.RetainedSeriesHistoryExpired;
    var scan_range = range;
    var tail_buffer: [40]u8 = undefined;
    if (closed) |manifest| {
        const keyTimestamp = @import("antfly_local_sources").retained_dataset_types.rowKeyTimestamp;
        read_clock = @max(read_clock, manifest.cut.admitted_ns);
        const start = try range.start();
        if (try spec.expired(start, read_clock)) return error.RetainedSeriesHistoryExpired;
        if (start < try keyTimestamp(manifest.first)) return error.RetainedSeriesCoverageMissing;
        const tail = try keyTimestamp(manifest.next);
        if (start >= tail) return .{ .arena = std.heap.ArenaAllocator.init(a), .rows = &.{} };
        if (try range.end() > tail) scan_range.end_ns = try std.fmt.bufPrint(&tail_buffer, "{d}", .{tail});
    }
    var plan = try store.planCoveringRange(spec.generation, spec.source_partition, spec.source_incarnation, scan_range, limits.segments);
    defer plan.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const temp = arena.allocator();
    var rows = std.ArrayList(series.Bucket).empty;
    var total_bytes: usize = 0;
    const requested = try t.KeySpan.fromRange(range);
    for (plan.segments) |segment| {
        if ((segment.state_encoding != 1 and segment.state_encoding != codec.encoding) or !segment.finalized or !std.mem.eql(u8, &segment.schema, &definition.schema)) return error.InvalidDatasetSegment;
        if (segment.byte_len > limits.bytes - total_bytes) return error.DatasetPageBudgetExceeded;
        total_bytes += @intCast(segment.byte_len);
        // Placement may change after planning. acquire() atomically pins the
        // current location; segment identity/content/coverage remain immutable.
        const lease = try store.acquire(segment.id);
        defer store.release(lease) catch {};
        const location = try resolve.location(lease.pin.location);
        const bytes = try store.read(lease, location);
        defer store.context.alloc.free(bytes);
        if (try codec.formatEncoding(bytes) != segment.state_encoding) return error.InvalidDatasetSegment;
        const decoded = try codec.decode(temp, spec, bytes);
        read_clock = @max(read_clock, decoded.receipt.admitted_ns);
        if (decoded.receipt.source_sequence != segment.source.sequence or !std.meta.eql(decoded.receipt.span, try segment.span())) return error.InvalidDatasetCoverage;
        if (closed) |manifest| {
            if (!decoded.receipt.source_closed or decoded.receipt.source_sequence != manifest.cut.sequence or
                decoded.receipt.complete_from_ns != manifest.cut.complete_from_ns or decoded.receipt.admitted_ns != manifest.cut.admitted_ns or
                !std.mem.eql(u8, &segment.id, &closure.segmentId(manifest.cut.closure, decoded.receipt.span))) return error.InvalidDatasetCoverage;
        }
        for (decoded.rows) |row| {
            const key = codec.key(row);
            if (std.mem.order(u8, &key, &requested.start) == .lt or std.mem.order(u8, &key, &requested.end) != .lt) continue;
            if (rows.items.len == limits.rows) return error.DatasetPageBudgetExceeded;
            var owned = row;
            owned.dimensions = try temp.dupe(u8, row.dimensions);
            try rows.append(temp, owned);
        }
    }
    if (try spec.expired(try range.start(), read_clock)) return error.RetainedSeriesHistoryExpired;
    return .{ .arena = arena, .rows = rows.items };
}
