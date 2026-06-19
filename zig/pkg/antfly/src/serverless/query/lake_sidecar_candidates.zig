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

//! Candidate production from lake sidecar artifacts.
//!
//! Sidecar segments use the existing Antfly document-id field to store
//! `source_binding.rowRefKeyAlloc` keys. This module is the query-side bridge
//! from sidecar hits back into validated lake `RowRef` candidates.

const std = @import("std");
const Allocator = std.mem.Allocator;
const text_segment = @import("../text_segment/mod.zig");
const artifact_ref = @import("../manifest/artifact_ref.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const indexed_reader = @import("indexed_reader.zig");
const query_request = @import("request.zig");
const lake_rows = @import("lake_rows.zig");

pub const TextCandidateRequest = struct {
    text: []const u8,
    operator: query_request.QueryOperator = .any_terms,
    offset: usize = 0,
    limit: usize = std.math.maxInt(usize),
    min_score: u32 = 0,
};

pub const OwnedCandidateSet = struct {
    sidecar_name: []u8,
    row_refs: []rowsource.RowRef,

    pub fn asLakeRowsCandidateSet(self: OwnedCandidateSet) lake_rows.SidecarCandidateSet {
        return .{
            .sidecar_name = self.sidecar_name,
            .row_refs = self.row_refs,
        };
    }

    pub fn deinit(self: *OwnedCandidateSet, alloc: Allocator) void {
        alloc.free(self.sidecar_name);
        source_binding.freeOwnedRowRefs(alloc, self.row_refs);
        self.* = undefined;
    }
};

pub fn textCandidateSetFromPayloadAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: TextCandidateRequest,
) !OwnedCandidateSet {
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .text) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.text_segment) return error.UnsupportedLakeSidecarCandidateSource;

    var segment = try text_segment.decodeAlloc(alloc, payload);
    defer text_segment.freeSegment(alloc, &segment);

    const hits = try indexed_reader.searchTextSegmentDocIdsAlloc(
        alloc,
        segment,
        request.text,
        request.operator,
        request.offset,
        request.limit,
        request.min_score,
    );
    defer indexed_reader.freeScoredDocs(alloc, hits);

    const keys = try alloc.alloc([]const u8, hits.len);
    defer alloc.free(keys);
    for (hits, 0..) |hit, idx| keys[idx] = hit.doc_id;

    const refs = try source_binding.rowRefsFromKeysAlloc(alloc, declaration.binding, keys);
    errdefer source_binding.freeOwnedRowRefs(alloc, refs);
    return .{
        .sidecar_name = try alloc.dupe(u8, declaration.name),
        .row_refs = refs,
    };
}

test "lake text sidecar candidate producer decodes external row refs from hits" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 0,
        } },
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 1,
        } },
    };
    const key_a = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key_a);
    const key_b = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key_b);
    const docs = [_]text_segment.DocumentEntry{
        .{ .doc_id = key_a, .normalized_text = @constCast("alpha beta"), .token_count = 2 },
        .{ .doc_id = key_b, .normalized_text = @constCast("beta gamma"), .token_count = 2 },
    };
    const beta_postings = [_]text_segment.Posting{
        .{ .doc_index = 0, .term_freq = 1 },
        .{ .doc_index = 1, .term_freq = 1 },
    };
    const gamma_postings = [_]text_segment.Posting{.{ .doc_index = 1, .term_freq = 1 }};
    const terms = [_]text_segment.TermEntry{
        .{ .term = @constCast("beta"), .postings = @constCast(beta_postings[0..]) },
        .{ .term = @constCast("gamma"), .postings = @constCast(gamma_postings[0..]) },
    };
    const segment = text_segment.Segment{
        .index_name = @constCast("events.body.text"),
        .source_name = @constCast("body"),
        .config_json = @constCast("{}"),
        .docs = @constCast(docs[0..]),
        .terms = @constCast(terms[0..]),
    };
    const payload = try text_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.body.text",
        .binding = binding,
        .artifact = .{
            .kind = .text_segment,
            .name = "events.body.text",
            .artifact_id = "artifact:text",
            .byte_len = payload.len,
            .checksum = "sha256:text",
        },
    };

    var candidates = try textCandidateSetFromPayloadAlloc(alloc, declaration, payload, .{
        .text = "gamma",
        .operator = .any_terms,
        .limit = 10,
    });
    defer candidates.deinit(alloc);

    try std.testing.expectEqualStrings("events.body.text", candidates.sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), candidates.row_refs.len);
    try std.testing.expectEqualStrings("file-a.parquet", candidates.row_refs[0].external.file_id);
    try std.testing.expectEqual(@as(u64, 1), candidates.row_refs[0].external.row_ordinal);
    try source_binding.validateCandidateRowRefsAgainstBinding(binding, candidates.asLakeRowsCandidateSet().row_refs);
}

test "lake text sidecar candidate producer rejects stale sidecar doc ids" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const stale_ref = rowsource.RowRef{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-8",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 0,
        .row_ordinal = 0,
    } };
    const stale_key = try source_binding.rowRefKeyAlloc(alloc, stale_ref);
    defer alloc.free(stale_key);
    const docs = [_]text_segment.DocumentEntry{
        .{ .doc_id = stale_key, .normalized_text = @constCast("alpha"), .token_count = 1 },
    };
    const postings = [_]text_segment.Posting{.{ .doc_index = 0, .term_freq = 1 }};
    const terms = [_]text_segment.TermEntry{
        .{ .term = @constCast("alpha"), .postings = @constCast(postings[0..]) },
    };
    const segment = text_segment.Segment{
        .index_name = @constCast("events.body.text"),
        .source_name = @constCast("body"),
        .config_json = @constCast("{}"),
        .docs = @constCast(docs[0..]),
        .terms = @constCast(terms[0..]),
    };
    const payload = try text_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        textCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.body.text",
            .binding = binding,
            .artifact = .{
                .kind = .text_segment,
                .name = "events.body.text",
                .artifact_id = "artifact:text",
                .byte_len = payload.len,
                .checksum = "sha256:text",
            },
        }, payload, .{ .text = "alpha" }),
    );
}
