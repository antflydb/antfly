// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

pub const antfly_sources = @import("source_owner_physical.zig");
test {
    _ = @import("storage/rewrite_tail_spool.zig");
    _ = @import("storage/db/relational_integrity.zig");
    _ = @import("storage/db/relational_index_system_test.zig");
    _ = @import("storage/db/relational_index_cover_system_test.zig");
    _ = @import("storage/db/relational_expression_system_test.zig");
    _ = @import("storage/db/relational_row_transform_test.zig");
    _ = @import("storage/db/relational_rewrite_staging_test.zig");
    _ = @import("storage/db/merge_page_system_test.zig");
    _ = @import("raft/storage/native_snapshot.zig");
    _ = @import("storage/db/online_merge_receiver.zig");
    _ = @import("storage/db/online_merge_io.zig");
    _ = @import("storage/db/source_publication_job.zig");
}
