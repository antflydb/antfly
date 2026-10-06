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
    _ = @import("antfly_local_sources").storage_db_maintenance_transaction_runtime;
    _ = @import("storage/server_db_integration_test.zig");
    _ = @import("storage/server_transaction_recovery.zig");
    _ = @import("storage/artifact_upload_recovery.zig");
    _ = @import("storage/server_coordinated_ttl.zig");
    _ = @import("storage/server_query_visibility.zig");
    _ = @import("storage/server_document_child_range.zig");
    _ = @import("storage/server_group_metadata.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
