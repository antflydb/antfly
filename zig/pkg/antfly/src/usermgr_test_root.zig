// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
// Keep the standalone suite rooted at src so UserManager can share the
// production executor ABI without introducing a second module identity.
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = @import("usermgr/mod.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
