// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2
pub const antfly_sources = @import("source_owner_physical.zig");
test {
    _ = @import("antfly_local_sources").storage_db_maintenance_transaction_runtime;
    _ = @import("storage/server_db_integration_test.zig");
    _ = @import("storage/server_transaction_recovery.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
