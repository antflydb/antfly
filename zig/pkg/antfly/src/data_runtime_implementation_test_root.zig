// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

pub const antfly_sources = @import("source_owner_physical.zig");
pub const implementation_tests_only = true;
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
comptime {
    _ = @import("data/runtime.zig").implementation_tests;
    _ = @import("antfly_local_sources").storage_db_enrichment_enrichment_runtime;
    _ = @import("storage/hot_standby/restore_terminal_ledger.zig");
    _ = @import("data/storage/shard_state_store.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
