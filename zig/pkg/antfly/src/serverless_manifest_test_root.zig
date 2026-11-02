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

const manifest_object_store = @import("serverless/manifest/object_store.zig");
const object_storage = @import("antfly_local_sources").storage_object_storage;

test {
    _ = manifest_object_store;
    _ = @import("serverless/artifacts/store.zig");
    _ = @import("api/lake_index_aggregate_artifact.zig");
    _ = @import("api/lake_index_aggregate_composition.zig");
    _ = @import("api/lake_index_reader_lease.zig");
    _ = @import("api/lake_index_seekable_text.zig");
    _ = @import("api/lake_index_ordered_rows.zig");
    _ = @import("api/sql_worker_lease.zig");
    _ = @import("serverless/manifest/fs_store.zig");
    _ = @import("serverless/manifest/remote_store.zig");
    _ = object_storage;
    _ = @import("serverless/build/retention.zig");
    _ = @import("serverless/catalog/fs_progress_store.zig");
    _ = @import("serverless/catalog/progress_store.zig");
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
