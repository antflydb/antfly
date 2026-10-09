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

pub const antfly_sources = @import("source_owner_physical.zig");

test {
    _ = @import("serverless/lake_wal.zig");
    _ = @import("serverless/lake_ingestion.zig");
    _ = @import("api/sql_execution.zig");
    _ = @import("api/tables.zig");
    _ = @import("api/table_contract.zig");
    _ = @import("serverless/build/lake_sidecar_text.zig");
    _ = @import("api/lake_sql_cursor.zig");
    _ = @import("api/lake_sql_overlay.zig");
    _ = @import("api/lake_maintenance_scheduler.zig");
    _ = @import("api/composed_query.zig");
    _ = @import("api/lake_retained_cut.zig");
    _ = @import("api/native_retained_cut_test.zig");
    _ = @import("api/native_public_repartition_test.zig");
    _ = @import("api/lake_enrichment_units.zig");
    _ = @import("api/lake_recent_vectors.zig");
    _ = @import("api/lake_vector_enrichment.zig");
    _ = @import("api/lake_expiring_objects.zig");
    _ = @import("api/lake_vector_enrichment_test.zig");
    _ = @import("api/lake_index_row_source.zig");
    _ = @import("api/lake_index_publication.zig");
    _ = @import("api/lake_index_store.zig");
    _ = @import("api/lake_index_coordinator.zig");
    _ = @import("api/lake_index_selection.zig");
    _ = @import("api/lake_index_reader_lease.zig");
    _ = @import("api/lake_index_gc.zig");
    _ = @import("api/lake_index_ordered_rows.zig");
    _ = @import("api/lake_index_native_files.zig");
    _ = @import("api/lake_index_native_rows.zig");
    _ = @import("api/lake_index_native_state.zig");
    _ = @import("api/lake_index_incremental_test.zig");
    _ = @import("api/lake_index_refinements_test.zig");
    _ = @import("api/lake_index_sql_rows.zig");
    _ = @import("api/lake_index_text_query.zig");
    _ = @import("api/lake_index_search_filter.zig");
    _ = @import("metadata/lake_index_lifecycle.zig");
    _ = @import("api/lake_index_aggregate_artifact.zig");
    _ = @import("api/lake_index_aggregate_composition.zig");
    _ = @import("api/lake_index_names.zig");
    _ = @import("api/lake_index_native_aggregates.zig");
    _ = @import("api/lake_schema_detection.zig");
    _ = @import("api/lake_table_reads.zig");
    _ = @import("antfly_local_sources").serverless_query_lake_schema;
    _ = @import("antfly_local_sources").schema_mod;
    _ = @import("antfly_local_sources").storage_db_relational_index_keys;
    _ = @import("antfly_local_sources").serverless_query_lake_read_context;
    _ = @import("antfly_local_sources").serverless_query_lake_serving_cache;
}

comptime {
    _ = @import("serverless/lake_snapshot_pins.zig");
    _ = @import("serverless/lake_compaction.zig");
    _ = @import("serverless/lake_vacuum.zig");
    _ = @import("api/lake_search_overlay.zig");
}
