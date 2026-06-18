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

//! Source binding metadata for serverless sidecar segments built over RowSource.

const std = @import("std");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const SidecarKind = enum(u8) {
    text = 1,
    vector = 2,
    sparse = 3,
    graph = 4,
    algebraic = 5,
};

pub const RowRefKind = enum(u8) {
    relational_key = 1,
    serverless = 2,
    external = 3,
};

pub const Binding = struct {
    sidecar_kind: SidecarKind,
    source_kind: rowsource.SourceKind,
    row_ref_kind: RowRefKind,
    source_id: []const u8 = &.{},
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    column_bindings: []const []const u8 = &.{},
    index_config_hash: []const u8,

    pub fn validate(self: Binding) !void {
        if (self.snapshot_id.len == 0) return error.InvalidSidecarSourceBinding;
        if (self.schema_fingerprint.len == 0) return error.InvalidSidecarSourceBinding;
        if (self.index_config_hash.len == 0) return error.InvalidSidecarSourceBinding;
        for (self.column_bindings) |column| {
            if (column.len == 0) return error.InvalidSidecarSourceBinding;
        }
        switch (self.source_kind) {
            .external_parquet, .external_iceberg, .external_lance => {
                if (self.row_ref_kind != .external) return error.InvalidSidecarSourceBinding;
                if (self.source_id.len == 0) return error.InvalidSidecarSourceBinding;
            },
            .serverless_fragment => {
                if (self.row_ref_kind != .serverless) return error.InvalidSidecarSourceBinding;
            },
            .relational_store, .json_materialized => {
                if (self.row_ref_kind != .relational_key) return error.InvalidSidecarSourceBinding;
            },
        }
    }
};

pub fn rowRefKindForSourceKind(source_kind: rowsource.SourceKind) RowRefKind {
    return switch (source_kind) {
        .relational_store, .json_materialized => .relational_key,
        .serverless_fragment => .serverless,
        .external_parquet, .external_iceberg, .external_lance => .external,
    };
}

pub fn bindingFromSnapshot(
    sidecar_kind: SidecarKind,
    source_kind: rowsource.SourceKind,
    snapshot: rowsource.SnapshotRef,
    schema_fingerprint: []const u8,
    column_bindings: []const []const u8,
    index_config_hash: []const u8,
) Binding {
    return .{
        .sidecar_kind = sidecar_kind,
        .source_kind = source_kind,
        .row_ref_kind = rowRefKindForSourceKind(source_kind),
        .source_id = snapshot.table_id,
        .snapshot_id = snapshot.snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .column_bindings = column_bindings,
        .index_config_hash = index_config_hash,
    };
}

pub fn validateBatchAgainstBinding(binding: Binding, batch: rowsource.ColumnBatch) !void {
    try validateBatchSnapshotAgainstBinding(binding, batch);
    for (binding.column_bindings) |column| {
        if (batch.findColumn(column) == null) return error.SidecarSourceBindingMismatch;
    }
}

pub fn validateBatchSnapshotAgainstBinding(binding: Binding, batch: rowsource.ColumnBatch) !void {
    try binding.validate();
    try batch.validate();
    if (binding.source_id.len != 0 and !std.mem.eql(u8, binding.source_id, batch.snapshot.table_id)) {
        return error.SidecarSourceBindingMismatch;
    }
    if (!std.mem.eql(u8, binding.snapshot_id, batch.snapshot.snapshot_id)) {
        return error.SidecarSourceBindingMismatch;
    }
    for (batch.row_refs) |row_ref| {
        try validateRowRefAgainstBinding(binding, row_ref);
    }
}

pub fn validateCandidateRowRefsAgainstBinding(
    binding: Binding,
    row_refs: []const rowsource.RowRef,
) !void {
    try binding.validate();
    for (row_refs) |row_ref| try validateRowRefAgainstBinding(binding, row_ref);
}

pub fn sameSourceSnapshot(a: Binding, b: Binding) bool {
    return a.source_kind == b.source_kind and
        a.row_ref_kind == b.row_ref_kind and
        std.mem.eql(u8, a.source_id, b.source_id) and
        std.mem.eql(u8, a.snapshot_id, b.snapshot_id) and
        std.mem.eql(u8, a.schema_fingerprint, b.schema_fingerprint);
}

pub fn rowRefMatchesKind(row_ref: rowsource.RowRef, kind: RowRefKind) bool {
    return switch (row_ref) {
        .relational_key => kind == .relational_key,
        .serverless => kind == .serverless,
        .external => kind == .external,
    };
}

pub fn validateRowRefAgainstBinding(binding: Binding, row_ref: rowsource.RowRef) !void {
    if (!rowRefMatchesKind(row_ref, binding.row_ref_kind)) return error.SidecarSourceBindingMismatch;
    switch (row_ref) {
        .external => |value| {
            if (!std.mem.eql(u8, value.source_id, binding.source_id)) return error.SidecarSourceBindingMismatch;
            if (!std.mem.eql(u8, value.snapshot_id, binding.snapshot_id)) return error.SidecarSourceBindingMismatch;
        },
        .serverless, .relational_key => {},
    }
}

pub fn rowRefKeyAlloc(alloc: std.mem.Allocator, row_ref: rowsource.RowRef) ![]u8 {
    return switch (row_ref) {
        .relational_key => |key| std.fmt.allocPrint(alloc, "rel:{d}:{s}", .{ key.len, key }),
        .serverless => |value| std.fmt.allocPrint(
            alloc,
            "srv:{d}:{s}:{d}",
            .{ value.fragment_id.len, value.fragment_id, value.row_ordinal },
        ),
        .external => |value| std.fmt.allocPrint(
            alloc,
            "ext:{d}:{s}:{d}:{s}:{d}:{s}:{d}:{d}",
            .{
                value.source_id.len,
                value.source_id,
                value.snapshot_id.len,
                value.snapshot_id,
                value.file_id.len,
                value.file_id,
                value.row_group_ordinal,
                value.row_ordinal,
            },
        ),
    };
}

test "sidecar source binding validates serverless and external sources" {
    const serverless_binding = Binding{
        .sidecar_kind = .text,
        .source_kind = .serverless_fragment,
        .row_ref_kind = .serverless,
        .snapshot_id = "manifest-1",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    try serverless_binding.validate();

    const external_binding = Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-123",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    try external_binding.validate();
    try std.testing.expect(!sameSourceSnapshot(serverless_binding, external_binding));

    var invalid = external_binding;
    invalid.row_ref_kind = .serverless;
    try std.testing.expectError(error.InvalidSidecarSourceBinding, invalid.validate());

    const invalid_relational = Binding{
        .sidecar_kind = .algebraic,
        .source_kind = .relational_store,
        .row_ref_kind = .external,
        .snapshot_id = "relational-1",
        .schema_fingerprint = "schema-v3",
        .column_bindings = &[_][]const u8{"tenant"},
        .index_config_hash = "sha256:algebraic",
    };
    try std.testing.expectError(error.InvalidSidecarSourceBinding, invalid_relational.validate());
}

test "sidecar source binding validates batches and creates stable row-ref keys" {
    const alloc = std.testing.allocator;

    const row_refs = [_]rowsource.RowRef{
        .{ .serverless = .{ .fragment_id = "frag-a", .row_ordinal = 0 } },
        .{ .serverless = .{ .fragment_id = "frag-a", .row_ordinal = 1 } },
    };
    const bodies = [_][]const u8{ "alpha", "bravo" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "body", .values = .{ .bytes = &bodies } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-9" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    const binding = bindingFromSnapshot(
        .text,
        .serverless_fragment,
        batch.snapshot,
        "schema-v1",
        &[_][]const u8{"body"},
        "sha256:text",
    );

    try validateBatchAgainstBinding(binding, batch);
    const key = try rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key);
    try std.testing.expectEqualStrings("srv:6:frag-a:1", key);

    var wrong_snapshot = batch;
    wrong_snapshot.snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-10" };
    try std.testing.expectError(error.SidecarSourceBindingMismatch, validateBatchAgainstBinding(binding, wrong_snapshot));
}

test "sidecar source binding validates external row-ref batches" {
    const alloc = std.testing.allocator;
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 2,
            .row_ordinal = 42,
        } },
    };
    const embeddings = [_][]const f32{&[_]f32{ 1.0, 0.0 }};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "embedding", .values = .{ .vector_f32 = &embeddings } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "events", .snapshot_id = "iceberg-7" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    const binding = bindingFromSnapshot(
        .vector,
        .external_iceberg,
        batch.snapshot,
        "schema-v2",
        &[_][]const u8{"embedding"},
        "sha256:vector",
    );

    try validateBatchAgainstBinding(binding, batch);
    const key = try rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key);
    try std.testing.expectEqualStrings("ext:6:events:9:iceberg-7:14:file-a.parquet:2:42", key);

    var missing_column = batch;
    missing_column.columns = &.{};
    try std.testing.expectError(error.SidecarSourceBindingMismatch, validateBatchAgainstBinding(binding, missing_column));

    const stale_refs = [_]rowsource.RowRef{.{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-8",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 2,
        .row_ordinal = 42,
    } }};
    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        validateCandidateRowRefsAgainstBinding(binding, &stale_refs),
    );
}
