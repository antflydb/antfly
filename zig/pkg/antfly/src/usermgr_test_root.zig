// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
// Keep the standalone suite rooted at src so UserManager can share the
// production executor ABI without introducing a second module identity.
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");

test {
    _ = @import("usermgr/mod.zig");
}
