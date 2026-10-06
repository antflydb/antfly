// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Linked-control hosted initial-FK owner integration composition.

const hosted_initial_fk_integration_test = @import("api/hosted_initial_fk_integration_test.zig");

pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
pub const linked_owner_fixture = @import("api/linked_owner_test_fixture.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = hosted_initial_fk_integration_test;
    _ = @import("api/hosted_initial_fk_fault_integration_test.zig");
    _ = @import("api/hosted_initial_fk_transfer_integration_test.zig");
    _ = @import("api/hosted_initial_fk_offline_integration_test.zig");
    _ = @import("api/hosted_initial_fk_capabilities_test.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
