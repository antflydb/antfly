// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

pub const antfly_sources = @import("source_owner_physical.zig");
pub const implementation_tests_only = true;
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");
comptime {
    _ = @import("data/runtime.zig").implementation_tests;
    _ = @import("storage/db/enrichment/enrichment_runtime.zig");
    _ = @import("storage/hot_standby/restore_terminal_ledger.zig");
    _ = @import("data/storage/shard_state_store.zig");
}
