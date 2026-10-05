// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

pub const antfly_sources = @import("source_owner_physical.zig");

test {
    _ = @import("storage/restore_owner.zig");
    _ = @import("antfly_local_sources").storage_restore_decoder_cache;
    _ = @import("antfly_local_sources").storage_db_relational_integrity_integration_test;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
