// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
test {
    _ = @import("antfly_local_sources").storage_db_graph_mutation_scopes;
    _ = @import("antfly_local_sources").storage_db_online_graph_artifacts;
    _ = @import("antfly_local_sources").storage_db_artifact_catalog_view;
    _ = @import("antfly_local_sources").storage_db_source_artifact_batch;
    _ = @import("antfly_local_sources").storage_db_merge_artifact_catalog;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
