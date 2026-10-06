// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Hidden initial-child Raft failover, distinct from ALTER publication.
test "mounted initial FK hidden three voter owner transfers before release" {
    try @import("hosted_initial_fk_fault_integration_test.zig").mountedInitialFault(true);
}
