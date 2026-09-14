// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Qualification executable; worker controls are never linked into Antfly.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const main = @import("testing/maintenance_process.zig").main;

// The linked API kernel and this physical fixture share one activity counter.
comptime {
    @export(&@import("storage/db/enrichment/enrichment_types.zig").interactiveActivity, .{ .name = "antfly_storage_interactive_activity" });
}
