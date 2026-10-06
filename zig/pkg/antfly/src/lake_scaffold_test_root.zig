// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
pub const local_rowsource = @import("antfly_local_sources").storage_rowsource_local;
pub const external_rowsource = @import("antfly_local_sources").storage_rowsource_external;
pub const row_fragment = @import("antfly_local_sources").serverless_row_fragment_mod;
pub const row_fragment_stats = @import("antfly_local_sources").serverless_row_fragment_stats;
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
pub const algebraic_segment = @import("antfly_local_sources").serverless_algebraic_segment_mod;
pub const external_source = @import("antfly_local_sources").serverless_external_source_mod;
pub const external_source_catalog_binding = @import("antfly_local_sources").serverless_external_source_catalog_binding;
pub const external_source_object_snapshot = @import("antfly_local_sources").serverless_external_source_object_snapshot;
pub const external_source_iceberg_metadata = @import("antfly_local_sources").serverless_external_source_iceberg_metadata;
pub const lake_rows_query = @import("antfly_local_sources").serverless_query_lake_rows;
pub const lake_sidecar_candidates = @import("serverless/query/lake_sidecar_candidates.zig");
pub const lake_explain_query = @import("serverless/query/lake_explain.zig");
pub const lake_cache_query = @import("serverless/query/lake_cache.zig");
pub const lake_range_io = @import("antfly_local_sources").serverless_query_lake_range_io;
pub const lake_parquet_footer = @import("antfly_local_sources").serverless_query_lake_parquet_footer;
pub const lake_parquet_metadata = @import("antfly_local_sources").serverless_query_lake_parquet_metadata;
pub const lake_parquet_page = @import("antfly_local_sources").serverless_query_lake_parquet_page;
pub const lake_parquet_rowgroup = @import("antfly_local_sources").serverless_query_lake_parquet_rowgroup;
pub const lake_scan_plan = @import("antfly_local_sources").serverless_query_lake_scan_plan;
pub const lake_object_reader = @import("antfly_local_sources").serverless_query_lake_object_reader;
pub const lake_iceberg_deletes = @import("antfly_local_sources").serverless_query_lake_iceberg_deletes;
pub const sidecar_source_binding = @import("antfly_local_sources").serverless_segment_source_binding;
pub const sidecar_manifest = @import("antfly_local_sources").serverless_segment_sidecar_manifest;
pub const manifest_artifact_ref = @import("antfly_local_sources").serverless_manifest_artifact_ref;
pub const manifest_base_source = @import("antfly_local_sources").serverless_manifest_base_source;
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

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
test {
    _ = @import("antfly_local_sources").serverless_query_lake_serving;
    _ = @import("antfly_local_sources").serverless_external_source_schema_binding;
}
