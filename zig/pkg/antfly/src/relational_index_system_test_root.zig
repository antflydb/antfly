// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
pub const antfly_sources = @import("source_owner_physical.zig");
test {
    _ = @import("storage/db/relational_index_system_test.zig");
    _ = @import("storage/db/relational_index_cover_system_test.zig");
    _ = @import("storage/db/relational_expression_system_test.zig");
    _ = @import("storage/db/relational_row_transform_test.zig");
    _ = @import("storage/db/relational_rewrite_staging_test.zig");
    _ = @import("storage/db/merge_page_system_test.zig");
    _ = @import("storage/db/native_raft_snapshot.zig");
    _ = @import("storage/db/online_merge_receiver.zig");
    _ = @import("storage/db/online_merge_io.zig");
    _ = @import("storage/db/source_publication_job.zig");
}
