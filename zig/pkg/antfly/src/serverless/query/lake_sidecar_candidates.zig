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
const sparse_segment = @import("../sparse_segment/mod.zig");
const artifacts_mod = @import("../artifacts/mod.zig");
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

pub const TextCandidatePlan = struct {
    request: TextCandidateRequest,
    sidecar_names: ?[]const []const u8 = null,
};

pub const SparseCandidateRequest = struct {
    terms: []const query_request.SparseTermWeight,
    offset: usize = 0,
    limit: usize = std.math.maxInt(usize),
    min_score: u32 = 0,
};

pub const SparseCandidatePlan = struct {
    request: SparseCandidateRequest,
    sidecar_names: ?[]const []const u8 = null,
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

pub const OwnedCandidateSets = struct {
    sets: []OwnedCandidateSet,

    pub fn asLakeRowsCandidateSetsAlloc(self: OwnedCandidateSets, alloc: Allocator) ![]lake_rows.SidecarCandidateSet {
        const out = try alloc.alloc(lake_rows.SidecarCandidateSet, self.sets.len);
        for (self.sets, 0..) |set, idx| out[idx] = set.asLakeRowsCandidateSet();
        return out;
    }

    pub fn deinit(self: *OwnedCandidateSets, alloc: Allocator) void {
        for (self.sets) |*set| set.deinit(alloc);
        if (self.sets.len > 0) alloc.free(self.sets);
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

pub fn textCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: TextCandidatePlan,
) !OwnedCandidateSets {
    if (plan.request.text.len == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };

    const selected = try selectedTextDeclarationsAlloc(alloc, declarations, plan.sidecar_names);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try artifacts.getAlloc(declaration.artifact.artifact_id);
        defer artifacts.allocator.free(payload);
        sets.appendAssumeCapacity(try textCandidateSetFromPayloadAlloc(alloc, declaration, payload, plan.request));
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

pub fn sparseCandidateSetFromPayloadAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: SparseCandidateRequest,
) !OwnedCandidateSet {
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .sparse) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.sparse_segment) return error.UnsupportedLakeSidecarCandidateSource;

    var segment = try sparse_segment.decodeAlloc(alloc, payload);
    defer sparse_segment.freeSegment(alloc, &segment);

    const hits = try indexed_reader.searchSparseSegmentDocIdsAlloc(
        alloc,
        segment,
        request.terms,
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

pub fn sparseCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: SparseCandidatePlan,
) !OwnedCandidateSets {
    if (plan.request.terms.len == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };

    const selected = try selectedSparseDeclarationsAlloc(alloc, declarations, plan.sidecar_names);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try artifacts.getAlloc(declaration.artifact.artifact_id);
        defer artifacts.allocator.free(payload);
        sets.appendAssumeCapacity(try sparseCandidateSetFromPayloadAlloc(alloc, declaration, payload, plan.request));
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

fn selectedTextDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    maybe_names: ?[]const []const u8,
) ![]sidecar_manifest.DeclaredArtifact {
    var selected = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    errdefer selected.deinit(alloc);

    if (maybe_names) |names| {
        for (names) |name| {
            if (name.len == 0) return error.InvalidLakeSidecarCandidateRequest;
            const declaration = findTextDeclaration(declarations, name) orelse return error.MissingLakeSidecarCandidateSource;
            try selected.append(alloc, declaration);
        }
        return try selected.toOwnedSlice(alloc);
    }

    for (declarations) |declaration| {
        if (!isTextDeclaration(declaration)) continue;
        if (selected.items.len != 0) return error.AmbiguousLakeSidecarCandidateSource;
        try selected.append(alloc, declaration);
    }
    return try selected.toOwnedSlice(alloc);
}

fn selectedSparseDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    maybe_names: ?[]const []const u8,
) ![]sidecar_manifest.DeclaredArtifact {
    var selected = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    errdefer selected.deinit(alloc);

    if (maybe_names) |names| {
        for (names) |name| {
            if (name.len == 0) return error.InvalidLakeSidecarCandidateRequest;
            const declaration = findSparseDeclaration(declarations, name) orelse return error.MissingLakeSidecarCandidateSource;
            try selected.append(alloc, declaration);
        }
        return try selected.toOwnedSlice(alloc);
    }

    for (declarations) |declaration| {
        if (!isSparseDeclaration(declaration)) continue;
        if (selected.items.len != 0) return error.AmbiguousLakeSidecarCandidateSource;
        try selected.append(alloc, declaration);
    }
    return try selected.toOwnedSlice(alloc);
}

fn findTextDeclaration(
    declarations: []const sidecar_manifest.DeclaredArtifact,
    name: []const u8,
) ?sidecar_manifest.DeclaredArtifact {
    for (declarations) |declaration| {
        if (!isTextDeclaration(declaration)) continue;
        if (std.mem.eql(u8, declaration.name, name)) return declaration;
    }
    return null;
}

fn findSparseDeclaration(
    declarations: []const sidecar_manifest.DeclaredArtifact,
    name: []const u8,
) ?sidecar_manifest.DeclaredArtifact {
    for (declarations) |declaration| {
        if (!isSparseDeclaration(declaration)) continue;
        if (std.mem.eql(u8, declaration.name, name)) return declaration;
    }
    return null;
}

fn isTextDeclaration(declaration: sidecar_manifest.DeclaredArtifact) bool {
    return declaration.binding.sidecar_kind == .text and declaration.artifact.kind == artifact_ref.ArtifactKind.text_segment;
}

fn isSparseDeclaration(declaration: sidecar_manifest.DeclaredArtifact) bool {
    return declaration.binding.sidecar_kind == .sparse and declaration.artifact.kind == artifact_ref.ArtifactKind.sparse_segment;
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

test "lake text sidecar candidate producer loads payloads from artifact store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const artifact_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer alloc.free(artifact_path);
    var fs = try artifacts_mod.FsStore.init(alloc, artifact_path);
    var store = fs.artifactStore();
    defer store.deinit();

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
    var meta = try store.put(payload);
    defer meta.deinit(alloc);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.body.text",
        .binding = binding,
        .artifact = .{
            .kind = .text_segment,
            .name = "events.body.text",
            .artifact_id = meta.artifact_id,
            .byte_len = meta.byte_len,
            .checksum = meta.checksum,
        },
    };
    const sidecar_names = [_][]const u8{"events.body.text"};
    var candidates = try textCandidateSetsFromArtifactStoreAlloc(alloc, &store, &[_]sidecar_manifest.DeclaredArtifact{declaration}, .{
        .request = .{
            .text = "gamma",
            .operator = .any_terms,
            .limit = 10,
        },
        .sidecar_names = &sidecar_names,
    });
    defer candidates.deinit(alloc);
    const lake_candidate_sets = try candidates.asLakeRowsCandidateSetsAlloc(alloc);
    defer alloc.free(lake_candidate_sets);

    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets.len);
    try std.testing.expectEqualStrings("events.body.text", lake_candidate_sets[0].sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets[0].row_refs.len);
    try std.testing.expectEqualStrings("file-a.parquet", lake_candidate_sets[0].row_refs[0].external.file_id);
    try std.testing.expectEqual(@as(u64, 1), lake_candidate_sets[0].row_refs[0].external.row_ordinal);
}

test "lake text sidecar candidate producer rejects ambiguous implicit sidecars" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const artifact_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer alloc.free(artifact_path);
    var fs = try artifacts_mod.FsStore.init(alloc, artifact_path);
    var store = fs.artifactStore();
    defer store.deinit();

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
    const declarations = [_]sidecar_manifest.DeclaredArtifact{
        .{
            .name = "events.body.text",
            .binding = binding,
            .artifact = .{
                .kind = .text_segment,
                .name = "events.body.text",
                .artifact_id = "artifact:text-a",
                .byte_len = 1,
                .checksum = "sha256:text-a",
            },
        },
        .{
            .name = "events.title.text",
            .binding = binding,
            .artifact = .{
                .kind = .text_segment,
                .name = "events.title.text",
                .artifact_id = "artifact:text-b",
                .byte_len = 1,
                .checksum = "sha256:text-b",
            },
        },
    };

    try std.testing.expectError(
        error.AmbiguousLakeSidecarCandidateSource,
        textCandidateSetsFromArtifactStoreAlloc(alloc, &store, &declarations, .{
            .request = .{ .text = "gamma" },
        }),
    );
}

test "lake sparse sidecar candidate producer loads payloads from artifact store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const artifact_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer alloc.free(artifact_path);
    var fs = try artifacts_mod.FsStore.init(alloc, artifact_path);
    var store = fs.artifactStore();
    defer store.deinit();

    const binding = source_binding.Binding{
        .sidecar_kind = .sparse,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"features"},
        .index_config_hash = "sha256:sparse",
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
    const docs = [_]sparse_segment.DocumentEntry{
        .{ .doc_id = key_a, .feature_count = 1 },
        .{ .doc_id = key_b, .feature_count = 2 },
    };
    const beta_postings = [_]sparse_segment.Posting{
        .{ .doc_index = 0, .weight = 0.25 },
        .{ .doc_index = 1, .weight = 0.5 },
    };
    const gamma_postings = [_]sparse_segment.Posting{.{ .doc_index = 1, .weight = 1.0 }};
    const terms = [_]sparse_segment.TermEntry{
        .{ .term = @constCast("beta"), .postings = @constCast(beta_postings[0..]) },
        .{ .term = @constCast("gamma"), .postings = @constCast(gamma_postings[0..]) },
    };
    const segment = sparse_segment.Segment{
        .docs = @constCast(docs[0..]),
        .terms = @constCast(terms[0..]),
    };
    const payload = try sparse_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    var meta = try store.put(payload);
    defer meta.deinit(alloc);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.features.sparse",
        .binding = binding,
        .artifact = .{
            .kind = .sparse_segment,
            .name = "events.features.sparse",
            .artifact_id = meta.artifact_id,
            .byte_len = meta.byte_len,
            .checksum = meta.checksum,
        },
    };
    const terms_query = [_]query_request.SparseTermWeight{.{ .term = @constCast("gamma"), .weight = 1.0 }};
    const sidecar_names = [_][]const u8{"events.features.sparse"};
    var candidates = try sparseCandidateSetsFromArtifactStoreAlloc(alloc, &store, &[_]sidecar_manifest.DeclaredArtifact{declaration}, .{
        .request = .{
            .terms = &terms_query,
            .limit = 10,
        },
        .sidecar_names = &sidecar_names,
    });
    defer candidates.deinit(alloc);
    const lake_candidate_sets = try candidates.asLakeRowsCandidateSetsAlloc(alloc);
    defer alloc.free(lake_candidate_sets);

    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets.len);
    try std.testing.expectEqualStrings("events.features.sparse", lake_candidate_sets[0].sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets[0].row_refs.len);
    try std.testing.expectEqualStrings("file-a.parquet", lake_candidate_sets[0].row_refs[0].external.file_id);
    try std.testing.expectEqual(@as(u64, 1), lake_candidate_sets[0].row_refs[0].external.row_ordinal);
}

test "lake sparse sidecar candidate producer rejects stale sidecar doc ids" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .sparse,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"features"},
        .index_config_hash = "sha256:sparse",
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
    const docs = [_]sparse_segment.DocumentEntry{
        .{ .doc_id = stale_key, .feature_count = 1 },
    };
    const postings = [_]sparse_segment.Posting{.{ .doc_index = 0, .weight = 1.0 }};
    const terms = [_]sparse_segment.TermEntry{
        .{ .term = @constCast("gamma"), .postings = @constCast(postings[0..]) },
    };
    const segment = sparse_segment.Segment{
        .docs = @constCast(docs[0..]),
        .terms = @constCast(terms[0..]),
    };
    const payload = try sparse_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    const terms_query = [_]query_request.SparseTermWeight{.{ .term = @constCast("gamma"), .weight = 1.0 }};

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        sparseCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.features.sparse",
            .binding = binding,
            .artifact = .{
                .kind = .sparse_segment,
                .name = "events.features.sparse",
                .artifact_id = "artifact:sparse",
                .byte_len = payload.len,
                .checksum = "sha256:sparse",
            },
        }, payload, .{ .terms = &terms_query }),
    );
}
