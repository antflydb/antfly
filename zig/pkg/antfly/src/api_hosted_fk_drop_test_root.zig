// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Linked metadata/data composition for mounted external-parent FK DROP.
const fixture = @import("api/hosted_fk_drop_e2e.zig");
const graph_fixture = @import("api/hosted_graph_truncate_e2e.zig");

pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
pub const linked_owner_fixture = @import("api/linked_owner_test_fixture.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");

test {
    _ = fixture;
    _ = graph_fixture;
}
