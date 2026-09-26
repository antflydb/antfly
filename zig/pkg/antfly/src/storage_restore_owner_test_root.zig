// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

pub const antfly_sources = @import("source_owner_physical.zig");

test {
    _ = @import("storage/restore_owner.zig");
    _ = @import("storage/restore_decoder_cache.zig");
    _ = @import("storage/db/relational_integrity_integration_test.zig");
}
