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

//! Focused compile/test root for the Antfly-owned lake-native scaffold.

pub const rowsource = @import("storage/rowsource/mod.zig");
pub const local_rowsource = @import("storage/rowsource/local.zig");
pub const external_rowsource = @import("storage/rowsource/external.zig");
pub const row_fragment = @import("serverless/row_fragment/mod.zig");
pub const row_fragment_stats = @import("serverless/row_fragment/stats.zig");
pub const row_fragment_build = @import("serverless/build/row_fragments.zig");
pub const row_fragment_manifest = @import("serverless/build/row_fragment_manifest.zig");
pub const row_fragment_publish = @import("serverless/build/row_fragment_publish.zig");
pub const external_source_manifest = @import("serverless/build/external_source_manifest.zig");
pub const external_source_plan_resolver = @import("serverless/build/external_source_plan_resolver.zig");
pub const external_source_plan_resolver_api = @import("serverless/build/external_source_plan_resolver_api.zig");
pub const algebraic_manifest = @import("serverless/build/algebraic_manifest.zig");
pub const algebraic_publish = @import("serverless/build/algebraic_publish.zig");
pub const lake_gc = @import("serverless/build/lake_gc.zig");
pub const lake_promotion = @import("serverless/build/lake_promotion.zig");
pub const lake_rebuild = @import("serverless/build/lake_rebuild.zig");
pub const algebraic_segment = @import("serverless/algebraic_segment/mod.zig");
pub const external_source = @import("serverless/external_source/mod.zig");
pub const external_source_catalog_binding = @import("serverless/external_source/catalog_binding.zig");
pub const external_source_object_snapshot = @import("serverless/external_source/object_snapshot.zig");
pub const external_source_iceberg_metadata = @import("serverless/external_source/iceberg_metadata.zig");
pub const lake_rows_query = @import("serverless/query/lake_rows.zig");
pub const lake_sidecar_candidates = @import("serverless/query/lake_sidecar_candidates.zig");
pub const lake_explain_query = @import("serverless/query/lake_explain.zig");
pub const lake_cache_query = @import("serverless/query/lake_cache.zig");
pub const lake_range_io = @import("serverless/query/lake_range_io.zig");
pub const lake_parquet_footer = @import("serverless/query/lake_parquet_footer.zig");
pub const lake_parquet_metadata = @import("serverless/query/lake_parquet_metadata.zig");
pub const lake_parquet_page = @import("serverless/query/lake_parquet_page.zig");
pub const lake_parquet_rowgroup = @import("serverless/query/lake_parquet_rowgroup.zig");
pub const lake_scan_plan = @import("serverless/query/lake_scan_plan.zig");
pub const lake_object_reader = @import("serverless/query/lake_object_reader.zig");
pub const lake_iceberg_deletes = @import("serverless/query/lake_iceberg_deletes.zig");
pub const sidecar_source_binding = @import("serverless/segment/source_binding.zig");
pub const sidecar_manifest = @import("serverless/segment/sidecar_manifest.zig");
pub const manifest_artifact_ref = @import("serverless/manifest/artifact_ref.zig");
pub const manifest_base_source = @import("serverless/manifest/base_source.zig");
pub const manifest_compatibility = @import("serverless/manifest/compatibility.zig");

test {
    _ = rowsource;
    _ = local_rowsource;
    _ = external_rowsource;
    _ = row_fragment;
    _ = row_fragment_stats;
    _ = row_fragment_build;
    _ = row_fragment_manifest;
    _ = row_fragment_publish;
    _ = external_source_manifest;
    _ = external_source_plan_resolver;
    _ = external_source_plan_resolver_api;
    _ = algebraic_manifest;
    _ = algebraic_publish;
    _ = lake_gc;
    _ = lake_promotion;
    _ = lake_rebuild;
    _ = algebraic_segment;
    _ = external_source;
    _ = external_source_catalog_binding;
    _ = external_source_object_snapshot;
    _ = external_source_iceberg_metadata;
    _ = lake_rows_query;
    _ = lake_sidecar_candidates;
    _ = lake_explain_query;
    _ = lake_cache_query;
    _ = lake_range_io;
    _ = lake_parquet_footer;
    _ = lake_parquet_metadata;
    _ = lake_parquet_page;
    _ = lake_parquet_rowgroup;
    _ = lake_scan_plan;
    _ = lake_object_reader;
    _ = lake_iceberg_deletes;
    _ = sidecar_source_binding;
    _ = sidecar_manifest;
    _ = manifest_artifact_ref;
    _ = manifest_base_source;
    _ = manifest_compatibility;
}
