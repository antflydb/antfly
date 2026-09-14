// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

pub const antfly_sources = @import("source_owner_physical.zig");
pub const implementation_tests_only = true;
comptime {
    _ = @import("api/table_reads.zig").implementation_tests;
    _ = @import("metadata/table_provisioner.zig").implementation_tests;
    _ = @import("api/distributed_txn.zig").implementation_tests;
    _ = @import("api/provisioned_storage.zig").implementation_tests;
    _ = @import("api/table_writes.zig").implementation_tests;
}
