// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
test {
    _ = @import("storage/db/graph_mutation_scopes.zig");
    _ = @import("storage/db/online_graph_artifacts.zig");
    _ = @import("storage/db/artifact_catalog_view.zig");
    _ = @import("storage/db/source_artifact_batch.zig");
    _ = @import("storage/db/merge_artifact_catalog.zig");
}
