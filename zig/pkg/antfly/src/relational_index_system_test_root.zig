// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub const antfly_sources = @import("source_owner_physical.zig");
test {
    _ = @import("antfly_local_sources").storage_rewrite_tail_spool;
    _ = @import("antfly_local_sources").storage_db_relational_integrity;
    _ = @import("antfly_local_sources").storage_db_relational_index_system_test;
    _ = @import("antfly_local_sources").storage_db_relational_index_cover_system_test;
    _ = @import("antfly_local_sources").storage_db_relational_expression_system_test;
    _ = @import("antfly_local_sources").storage_db_relational_row_transform_test;
    _ = @import("antfly_local_sources").storage_db_relational_rewrite_staging_test;
    _ = @import("antfly_local_sources").storage_db_merge_page_system_test;
    _ = @import("raft/storage/native_snapshot.zig");
    _ = @import("antfly_local_sources").storage_db_online_merge_receiver;
    _ = @import("antfly_local_sources").storage_db_online_merge_io;
    _ = @import("antfly_local_sources").storage_db_source_publication_job;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
