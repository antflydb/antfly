// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

//! Physical-owner regressions that need the DB implementation as well as the
//! linked storage kernel. Keep the contract-only owner root lightweight.

test {
    _ = @import("storage/kernel_owner_handoff_reopen_test.zig");
}

pub const antfly_sources = @import("source_owner_storage.zig");
