// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Linked-control owner composition for mounted SQL primary-key rewrites.

const sql_primary_key_rewrite_e2e = @import("api/sql_primary_key_rewrite_e2e.zig");

// The private online-merge donor port is only installed by the linked-control
// runtime. A physical/monolithic API root cannot exercise this protocol.
pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
pub const linked_owner_fixture = @import("api/linked_owner_test_fixture.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");

test {
    _ = sql_primary_key_rewrite_e2e;
}
