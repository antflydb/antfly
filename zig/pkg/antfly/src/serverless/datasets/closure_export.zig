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

//! Encode one bounded page of a closed native owner. A page publication only
//! proves its own span; even a complete page never authorizes source removal.
const std = @import("std");
const t = @import("types.zig");
const catalog = @import("catalog.zig");
const local = @import("antfly_local_sources");
const series = local.storage_retained_series;
const codec = local.storage_retained_series_archive;
const A = std.mem.Allocator;

const manifest = @import("closure_manifest.zig");
pub const Cut = manifest.Cut;

pub const Page = struct {
    bytes: []const u8,
    segment: t.Segment,
};

fn successor(spec: series.Spec, row: series.Bucket, same_bucket: bool) !t.RowKey {
    const end = std.math.add(i128, row.start_ns, spec.interval_ns) catch return error.RetainedSeriesTimeOverflow;
    if (same_bucket) {
        var key = codec.key(row);
        const digits = "0123456789abcdef";
        var pos = key.len;
        while (pos > 32) {
            pos -= 1;
            const index = std.mem.indexOfScalar(u8, digits, key[pos]).?;
            if (index != 15) {
                key[pos] = digits[index + 1];
                return key;
            }
            key[pos] = '0';
        }
    }
    return t.rowKey(end, @splat(0));
}

/// `start` is the exact prior committed page end, or the first retained
/// bucket boundary. The caller obtains rows and `has_more` from one native
/// snapshot with the admitted cut. An empty snapshot has no segment; its
/// empty coverage belongs in the separate partition handoff record.
pub fn encode(a: A, definition: t.Definition, spec: series.Spec, cut: Cut, start: t.RowKey, rows: []const series.Bucket, has_more: bool, location: catalog.Location) !?Page {
    if (rows.len > series.max_changes or (rows.len == 0 and has_more)) return error.RetainedSeriesArchiveBudgetExceeded;
    if (t.zero(cut.closure) or location.access != .queryable or !std.mem.eql(u8, &spec.id, &definition.id) or
        spec.generation != definition.generation or !std.mem.eql(u8, &spec.definition, &try definition.fingerprint(a))) return error.InvalidDatasetBinding;
    if (rows.len == 0) return null;
    const end = try successor(spec, rows[rows.len - 1], has_more);
    const span: t.KeySpan = .{ .start = start, .end = end };
    const start_ns = try local.retained_dataset_types.rowKeyTimestamp(start);
    const end_bucket = try local.retained_dataset_types.rowKeyTimestamp(end);
    const end_ns = if (std.mem.allEqual(u8, end[32..], '0')) end_bucket else std.math.add(i128, end_bucket, spec.interval_ns) catch return error.RetainedSeriesTimeOverflow;
    const bytes = try codec.encode(a, spec, .{ .source_sequence = cut.sequence, .source_closed = true, .admitted_ns = cut.admitted_ns, .complete_from_ns = cut.complete_from_ns, .span = span }, rows);
    errdefer a.free(bytes);
    const start_text = try std.fmt.allocPrint(a, "{d}", .{start_ns});
    errdefer a.free(start_text);
    const end_text = try std.fmt.allocPrint(a, "{d}", .{end_ns});
    errdefer a.free(end_text);
    var checksum: t.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &checksum, .{});
    const segment: t.Segment = .{ .id = manifest.segmentId(cut.closure, span), .dataset = definition.id, .definition_generation = definition.generation, .schema = definition.schema, .state_encoding = codec.encoding, .coverage = .{ .start_ns = start_text, .end_ns = end_text }, .key_span = span, .source = .{ .partition = spec.source_partition, .incarnation = spec.source_incarnation, .sequence = cut.sequence }, .finalized = true, .checksum = checksum, .byte_len = bytes.len, .location = location.id, .access = location.access };
    try segment.validate(definition);
    return .{ .bytes = bytes, .segment = segment };
}

pub fn deinit(a: A, page: Page) void {
    a.free(page.bytes);
    a.free(page.segment.coverage.start_ns);
    a.free(page.segment.coverage.end_ns);
}

/// The verified page and the resumed export frontier become visible in the
/// same conditional catalog commit.
pub fn publish(store: catalog.Catalog, spec: series.Spec, cut: Cut, page: Page, location: catalog.Location) !void {
    try store.publishClosedPage(spec, cut, page.segment, location, page.bytes);
}
