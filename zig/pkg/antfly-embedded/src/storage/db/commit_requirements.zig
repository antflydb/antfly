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

/// Bound the physical output before it enters a primary transaction. Transport
/// adapters must separately account for their encoding overhead and source bytes.
pub const max_output_bytes: usize = 8 * 1024 * 1024;
pub const max_output_rows: usize = 16 * 1024;

pub fn validateOutputBounds(writes: anytype, deletes: []const []const u8) !void {
    if (writes.len > max_output_rows or deletes.len > max_output_rows - writes.len)
        return error.CommitRequirementTooLarge;
    var remaining = max_output_bytes;
    for (writes) |write| {
        if (write.key.len > remaining) return error.CommitRequirementTooLarge;
        remaining -= write.key.len;
        if (write.value.len > remaining) return error.CommitRequirementTooLarge;
        remaining -= write.value.len;
    }
    for (deletes) |key| {
        if (key.len > remaining) return error.CommitRequirementTooLarge;
        remaining -= key.len;
    }
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
