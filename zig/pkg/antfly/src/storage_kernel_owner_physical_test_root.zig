// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

//! Physical-owner regressions that need the DB implementation as well as the
//! linked storage kernel. Keep the contract-only owner root lightweight.

test {
    _ = @import("storage/kernel_owner_handoff_reopen_test.zig");
}

pub const antfly_sources = @import("source_owner_storage.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
