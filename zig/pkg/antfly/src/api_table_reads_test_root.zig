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

const table_reads = @import("antfly_source_root").antfly_sources.table_reads;
const table_router = @import("api/table_router.zig");
const internal_query_operations = @import("api/internal_query_operations.zig");
const internal_group_operations = @import("api/internal_group_operations.zig");
const http_client = @import("api/http_client.zig");
const storage_db = @import("antfly_source_root").antfly_sources.selected_db;

test {
    _ = @import("api/internal_query_owner.zig");
    _ = @import("api/retained_read_owner.zig");
    _ = @import("api/retained_read_rpc.zig");
    _ = @import("api/owned_retained_read.zig");
    _ = @import("api/retained_read_client.zig");
    _ = table_reads;
    _ = table_router;
    _ = @import("api/table_catalog.zig").consumer_tests;
    _ = internal_query_operations;
    _ = internal_group_operations;
    _ = http_client;
    _ = storage_db;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_control.zig");

pub const consumer_tests_only = true;

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
