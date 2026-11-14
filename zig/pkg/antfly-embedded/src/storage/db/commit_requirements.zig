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

//! Stable admission classification, shared by preparation and capability checks.
//! This module performs no IO and never depends on a visibility wait level.
const std = @import("std");

pub const Eligibility = enum {
    eligible,
    protected_producer,
    committed_context,
    artifact_source,
    page_images,
    grouped_source,
};

pub fn directEligibility(request: anytype) Eligibility {
    for ([_][]const u8{ request.artifact_name, request.embedding_name, request.upstream_artifact_name }) |name| {
        if (std.mem.startsWith(u8, name, "_privacy_")) return .protected_producer;
    }
    if (request.group_source_json.len != 0 or request.group_artifact_name.len != 0) return .grouped_source;
    if (request.requires_committed_graph or request.neighbor_context_json.len != 0 or request.prior_context_json.len != 0) return .committed_context;
    if (request.upstream_artifact_name.len != 0 or request.input_kind == .materialized_chunks) return .artifact_source;
    if (request.embedding_input == .pdf_page_images) return .page_images;
    return .eligible;
}

/// Catalog-level admission remains stable when a conditional row or an authored
/// vector removes provider work from the row plan. Group-only catalogs have no
/// eligible document-local producer and must not turn a gate into a no-op.
pub fn catalogSupported(plan: anytype) bool {
    for (plan.generated_templates) |request| if (directEligibility(request) == .eligible) return true;
    if (plan.generated_templates.len != 0) return false;
    if (plan.group_sources) |groups| if (groups.entries.len != 0) return false;
    return true;
}

/// Admission budget for the source envelope and the complete atomic effect set.
/// The final physical check includes metadata amplification and replay bytes.
pub const max_output_bytes: usize = 8 * 1024 * 1024;
pub const max_output_rows: usize = 16 * 1024;

/// Bound speculative dispatch without reducing bulk ingestion to one provider
/// call per document. A large single source remains one bounded work item.
pub const max_window_documents: usize = 32;
pub const max_window_source_bytes: usize = 256 * 1024;
pub fn windowEnd(writes: anytype, start: usize) usize {
    var end = start;
    var bytes: usize = 0;
    while (end < writes.len and end - start < max_window_documents) {
        const next = writes[end].key.len +| writes[end].value.len;
        if (end != start and next > max_window_source_bytes -| bytes) break;
        bytes +|= next;
        end += 1;
    }
    return end;
}

/// Incremental accounting is monotonic: callers check only newly appended
/// effects before dispatching the next producer, avoiding quadratic rescans.
pub const OutputBudget = struct {
    bytes: usize = 0,
    rows: usize = 0,
    writes_seen: usize = 0,
    deletes_seen: usize = 0,

    pub fn reserve(self: *OutputBudget, bytes: usize, rows: usize) !void {
        if (bytes > max_output_bytes - self.bytes or rows > max_output_rows - self.rows)
            return error.CommitRequirementTooLarge;
        self.bytes += bytes;
        self.rows += rows;
    }

    pub fn checkpoint(self: *OutputBudget, writes: anytype, deletes: []const []const u8) !void {
        std.debug.assert(writes.len >= self.writes_seen and deletes.len >= self.deletes_seen);
        for (writes[self.writes_seen..]) |write| {
            try self.reserve(write.key.len, 1);
            try self.reserve(write.value.len, 0);
        }
        for (deletes[self.deletes_seen..]) |key| try self.reserve(key.len, 1);
        self.writes_seen = writes.len;
        self.deletes_seen = deletes.len;
    }
};

/// Reserve the complete transaction envelope before voting yes. Bounds include
/// both retained journal encoding and receiver-owned metadata: source clocks,
/// identities, TTL, readiness/work records, receipts, proof/count/reference
/// replacement, artifact revisions, scope clocks and acceptance certificates.
/// These deliberately cover replacement's delete-and-put paths together; phase
/// two must never turn an irrevocable decision into a size rejection.
pub fn validateTransactionReservation(packet: anytype, intents: anytype, predicates: anytype, relational_indexes: usize) !void {
    var budget = OutputBudget{};
    // Transaction record, admission/read manifests, watermark and replay head.
    try budget.reserve(4096, 32);
    for (intents) |intent| {
        const payload = @max(if (intent.value) |value| value.len else @as(usize, 0), if (intent.prepared_row) |row| row.len else @as(usize, 0));
        if (intent.key.len > max_output_bytes or payload > max_output_bytes) return error.CommitRequirementTooLarge;
        // Primary bytes plus their journal bound. Existing vector/retirement
        // reservations are authored by the pinned native materializer.
        try budget.reserve(4096 + 16 * intent.key.len + 2 * payload, 16);
        try budget.reserve(std.math.cast(usize, intent.retained_artifact_bytes) orelse return error.CommitRequirementTooLarge, std.math.cast(usize, intent.retained_artifact_keys) orelse return error.CommitRequirementTooLarge);
        for (0..relational_indexes) |_| try budget.reserve(1024 + 8 * intent.key.len + 4 * payload, 8);
    }
    for (predicates) |predicate| {
        if (predicate.key.len > max_output_bytes) return error.CommitRequirementTooLarge;
        try budget.reserve(512 + 8 * predicate.key.len, 8);
    }
    for (packet.artifact_writes) |write| try budget.reserve(2048 + 12 * write.key.len + 2 * write.value.len, 12);
    for (packet.artifact_deletes) |key| try budget.reserve(2048 + 12 * key.len, 12);
    for (packet.producers) |producer| try budget.reserve(4096 + 8 * producer.doc_key.len, 16);
}

pub fn validateOutputBounds(writes: anytype, deletes: []const []const u8) !void {
    var budget = OutputBudget{};
    try budget.checkpoint(writes, deletes);
}

// The policy consumes values, not the enrichment runtime or its import graph.
// Keep standalone contract fixtures local to this module.
const TestRequest = struct {
    artifact_name: []const u8 = "",
    embedding_name: []const u8 = "",
    upstream_artifact_name: []const u8 = "",
    group_source_json: []const u8 = "",
    group_artifact_name: []const u8 = "",
    requires_committed_graph: bool = false,
    neighbor_context_json: []const u8 = "",
    prior_context_json: []const u8 = "",
    input_kind: enum { document, inline_chunks, materialized_chunks } = .document,
    embedding_input: enum { text, pdf_page_images } = .text,
};

test "commit requirements direct admission excludes committed and artifact dependencies" {
    var request = TestRequest{};
    try std.testing.expectEqual(Eligibility.eligible, directEligibility(request));
    request.upstream_artifact_name = "extracted";
    try std.testing.expectEqual(Eligibility.artifact_source, directEligibility(request));
    request.upstream_artifact_name = "";
    request.requires_committed_graph = true;
    try std.testing.expectEqual(Eligibility.committed_context, directEligibility(request));
    request.requires_committed_graph = false;
    request.embedding_input = .pdf_page_images;
    try std.testing.expectEqual(Eligibility.page_images, directEligibility(request));
    request.embedding_input = .text;
    request.artifact_name = "_privacy_protected";
    try std.testing.expectEqual(Eligibility.protected_producer, directEligibility(request));
    request.artifact_name = "";
    request.group_source_json = "{}";
    try std.testing.expectEqual(Eligibility.grouped_source, directEligibility(request));
}

test "commit requirements catalog capability includes grouped-only and empty catalogs" {
    const Group = struct { entries: []const u8 };
    const Plan = struct { generated_templates: []const TestRequest = &.{}, group_sources: ?Group = null };
    try std.testing.expect(catalogSupported(Plan{}));
    try std.testing.expect(!catalogSupported(Plan{ .group_sources = .{ .entries = &.{1} } }));
    const excluded: TestRequest = .{ .upstream_artifact_name = "extraction" };
    try std.testing.expect(!catalogSupported(Plan{ .generated_templates = &.{excluded} }));
    var eligible = excluded;
    eligible.upstream_artifact_name = "";
    try std.testing.expect(catalogSupported(Plan{ .generated_templates = &.{ excluded, eligible } }));
}

test "commit requirements prepared output bounds include deletes" {
    const Write = struct { key: []const u8, value: []const u8 };
    try validateOutputBounds(&[_]Write{.{ .key = "artifact", .value = "value" }}, &.{"retired"});
    const oversized = try std.testing.allocator.alloc(u8, max_output_bytes);
    defer std.testing.allocator.free(oversized);
    try std.testing.expectError(error.CommitRequirementTooLarge, validateOutputBounds(&[_]Write{.{ .key = "artifact", .value = oversized }}, &.{}));
}

test "commit requirements incremental output budget counts each effect once" {
    const Write = struct { key: []const u8, value: []const u8 };
    const writes = [_]Write{ .{ .key = "a", .value = "bc" }, .{ .key = "de", .value = "f" } };
    var budget = OutputBudget{};
    try budget.checkpoint(writes[0..1], &.{});
    try budget.checkpoint(&writes, &.{"gh"});
    try budget.checkpoint(&writes, &.{"gh"});
    try std.testing.expectEqual(@as(usize, 8), budget.bytes);
    try std.testing.expectEqual(@as(usize, 3), budget.rows);
    try budget.reserve(max_output_bytes - 8, max_output_rows - 3);
    try std.testing.expectError(error.CommitRequirementTooLarge, budget.reserve(1, 0));
    try std.testing.expectError(error.CommitRequirementTooLarge, budget.reserve(0, 1));
}

test "commit requirements dispatch windows bound documents and source bytes" {
    const Write = struct { key: []const u8 = "k", value: []const u8 = "v" };
    const writes: [max_window_documents + 1]Write = @splat(.{});
    try std.testing.expectEqual(max_window_documents, windowEnd(&writes, 0));
    try std.testing.expectEqual(writes.len, windowEnd(&writes, max_window_documents));
    const large = try std.testing.allocator.alloc(u8, max_window_source_bytes);
    defer std.testing.allocator.free(large);
    const by_bytes = [_]Write{ .{ .value = large }, .{} };
    try std.testing.expectEqual(@as(usize, 1), windowEnd(&by_bytes, 0));
}
