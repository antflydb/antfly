// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
pub const antfly_sources = @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const local_test_sources = @import("local_test_sources.zig");
test {
    _ = @import("api/distributed_graph.zig");
    _ = @import("storage/local_query_provider.zig");
}
