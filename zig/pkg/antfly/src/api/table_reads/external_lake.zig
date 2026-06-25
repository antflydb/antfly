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

const std = @import("std");

const storage_schema = @import("../../storage/schema.zig");
const external_binding_api = @import("../../serverless/external_source/catalog_binding.zig");
const external_source_api = @import("../../serverless/external_source/mod.zig");
const artifact_ref_api = @import("../../serverless/manifest/artifact_ref.zig");
const sidecar_manifest_api = @import("../../serverless/segment/sidecar_manifest.zig");
const source_binding_api = @import("../../serverless/segment/source_binding.zig");
const serverless_query = @import("../../serverless/query/mod.zig");

pub const PinnedExternalLakeRowsScanner = struct {
    inventory: external_source_api.Inventory,
    reader: serverless_query.LakeParquetObjectRangeReader,
    cache: ?*serverless_query.LakeParquetObjectRangeCache = null,
    coalesce_options: serverless_query.LakeRangeCoalesceOptions = .{},
    sidecar_context: PinnedExternalLakeSidecarContext = .{},

    pub fn scanAlloc(
        self: PinnedExternalLakeRowsScanner,
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        request: serverless_query.LakeRowsScanRequest,
    ) !serverless_query.LakeRowsScanResult {
        return try executePinnedExternalLakeRowsScanAlloc(
            alloc,
            runtime_schema,
            self.inventory,
            self.reader,
            self.cache,
            self.coalesce_options,
            self.sidecar_context,
            request,
        );
    }
};

pub const PinnedExternalLakeSidecarContext = struct {
    sidecars: []const sidecar_manifest_api.DeclaredArtifact = &.{},
    desired_sidecars: []const serverless_query.LakeSidecarDesired = &.{},
    sidecar_policy: serverless_query.LakeSidecarSelectionPolicy = .{},
    candidates: []const serverless_query.LakeRowsSidecarCandidateSet = &.{},
};

pub fn executePinnedExternalLakeRowsScanAlloc(
    alloc: std.mem.Allocator,
    runtime_schema: storage_schema.TableSchema,
    inventory: external_source_api.Inventory,
    reader: serverless_query.LakeParquetObjectRangeReader,
    cache: ?*serverless_query.LakeParquetObjectRangeCache,
    coalesce_options: serverless_query.LakeRangeCoalesceOptions,
    sidecar_context: PinnedExternalLakeSidecarContext,
    request: serverless_query.LakeRowsScanRequest,
) !serverless_query.LakeRowsScanResult {
    if (runtime_schema.storage_mode != .relational) return error.InvalidRowsRequest;
    const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
    const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
    if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;

    return try serverless_query.queryLakeParquetSupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = binding,
        .reader = reader,
        .cache = cache,
        .inventory = inventory,
        .projected_columns = request.projected_columns,
        .predicate = request.predicate,
        .limit = request.limit,
        .deleted_row_refs = request.deleted_row_refs,
        .coalesce_options = coalesce_options,
        .sidecars = sidecar_context.sidecars,
        .desired_sidecars = sidecar_context.desired_sidecars,
        .sidecar_policy = sidecar_context.sidecar_policy,
        .candidates = sidecar_context.candidates,
    });
}

test "pinned external lake rows scanner validates schema binding against inventory" {
    const alloc = std.testing.allocator;
    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .external_base_source = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:expected" },
            .schema_fingerprint = "schema-v1",
        },
    };

    var inventory = external_source_api.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:stale"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source_api.types.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 128,
        .row_count = 0,
        .row_groups = &.{},
    };

    const NeverRead = struct {
        fn readRangeAlloc(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: u64,
            _: usize,
        ) ![]u8 {
            return error.UnexpectedLakeRangeRead;
        }
    };
    var ctx: u8 = 0;
    const reader = serverless_query.LakeParquetObjectRangeReader{
        .ctx = &ctx,
        .read_range_alloc = NeverRead.readRangeAlloc,
    };
    const projection = [_][]const u8{"amount"};

    try std.testing.expectError(
        error.ExternalLakeSnapshotMismatch,
        executePinnedExternalLakeRowsScanAlloc(
            alloc,
            schema,
            inventory,
            reader,
            null,
            .{},
            .{},
            .{ .projected_columns = projection[0..] },
        ),
    );
}

test "pinned external lake rows scanner forwards sidecar freshness policy" {
    const alloc = std.testing.allocator;
    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .external_base_source = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:expected" },
            .schema_fingerprint = "schema-v1",
        },
    };

    var inventory = external_source_api.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:expected"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source_api.types.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 128,
        .row_count = 1,
        .row_groups = try alloc.dupe(external_source_api.types.RowGroup, &[_]external_source_api.types.RowGroup{.{
            .ordinal = 0,
            .row_count = 1,
            .file_offset = 0,
            .total_byte_len = 1,
            .column_chunks = try alloc.dupe(external_source_api.types.ColumnChunk, &[_]external_source_api.types.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 0,
                .compressed_len = 1,
                .uncompressed_len = 1,
                .encoding = try alloc.dupe(u8, "plain"),
                .physical_type = try alloc.dupe(u8, "int64"),
            }}),
        }}),
    };

    const NeverRead = struct {
        fn readRangeAlloc(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: u64,
            _: usize,
        ) ![]u8 {
            return error.UnexpectedLakeRangeRead;
        }
    };
    var ctx: u8 = 0;
    const reader = serverless_query.LakeParquetObjectRangeReader{
        .ctx = &ctx,
        .read_range_alloc = NeverRead.readRangeAlloc,
    };

    const stale_sidecars = [_]sidecar_manifest_api.DeclaredArtifact{.{
        .name = "events.amount.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_parquet,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "sha256:old",
            .schema_fingerprint = "schema-v1",
            .column_bindings = &[_][]const u8{"amount"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = artifact_ref_api.ArtifactKind.vector_segment,
            .name = "events.amount.vector",
            .artifact_id = "artifact:vector:old",
            .byte_len = 1,
            .checksum = "sha256:artifact",
        },
    }};
    const desired = [_]serverless_query.LakeSidecarDesired{.{ .kind = source_binding_api.SidecarKind.vector }};
    const projection = [_][]const u8{"amount"};

    try std.testing.expectError(
        error.StaleLakeSidecar,
        executePinnedExternalLakeRowsScanAlloc(
            alloc,
            schema,
            inventory,
            reader,
            null,
            .{},
            .{
                .sidecars = stale_sidecars[0..],
                .desired_sidecars = desired[0..],
            },
            .{ .projected_columns = projection[0..] },
        ),
    );
}
