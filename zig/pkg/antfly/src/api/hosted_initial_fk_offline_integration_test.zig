// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Physical cleanup after an offline initial child misses cancellation.
test "mounted initial FK canceled offline replica returns and signs exact unlink ACK" {
    try @import("hosted_initial_fk_fault_integration_test.zig").mountedInitialScenario(.cancel_offline);
}

test "mounted initial FK published obsolete released replica retires without canceling current owner" {
    try @import("hosted_initial_fk_fault_integration_test.zig").mountedInitialScenario(.published_obsolete);
}
