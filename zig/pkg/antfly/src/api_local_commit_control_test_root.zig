// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Explicit physical owner for commit controls and accepted-BEGIN cleanup.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = @import("local_test_sources.zig");
test {
    @import("std").testing.refAllDecls(@import("metadata/storage/raft_apply_store.zig"));
    @import("std").testing.refAllDecls(@import("api/table_catalog.zig").consumer_tests);
    @import("std").testing.refAllDecls(@import("api/table_writes.zig").consumer_tests);
}
