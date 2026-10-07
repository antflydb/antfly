// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

test {
    _ = @import("antfly_local_sources").storage_db_artifact_producer_readiness;
    _ = @import("api/indexes.zig");
    _ = @import("metadata/storage/raft_apply_store.zig");
}
