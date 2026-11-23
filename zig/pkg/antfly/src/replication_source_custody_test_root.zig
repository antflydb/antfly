// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

pub const antfly_sources = @import("source_owner_physical.zig");

test {
    _ = @import("antfly_local_sources").storage_db_replication_source_apply_store;
    _ = @import("antfly_local_sources").storage_db_replication_source_commit_participant;
    _ = @import("metadata/replication_digest.zig");
    _ = @import("metadata/replication_initialization_attestation.zig");
    _ = @import("system_catalog/replication/payload.zig");
    _ = @import("system_catalog/replication/snapshot_codec.zig");
    _ = @import("api/replication_subscription_capture.zig");
    _ = @import("system_catalog/prepared_transactions.zig");
    _ = @import("metadata/replication_subscription_work.zig");
}
