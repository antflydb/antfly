// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Actual provisioned activity gates with control-side storage ownership.
pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
test {
    @import("std").testing.refAllDecls(@import("antfly_local_sources").common_seed_snapshot_lifetime);
    @import("std").testing.refAllDecls(@import("api/routed_write_admission.zig"));
    @import("std").testing.refAllDecls(@import("api/runnable_reentry.zig"));
    @import("std").testing.refAllDecls(@import("api/table_writes.zig").consumer_tests);
}
