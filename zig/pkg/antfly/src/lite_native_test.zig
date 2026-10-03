// Copyright 2026 Antfly, Inc.
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

test {
    _ = @import("antfly_local_sources").storage_lite_backend;
    _ = @import("antfly_local_sources").storage_lite_conformance_test;
    _ = @import("antfly_local_sources").storage_lite_docstore;
    _ = @import("antfly_local_sources").storage_lite_index_storage;
    _ = @import("antfly_local_sources").storage_lite_native;
    _ = @import("antfly_local_sources").storage_lite_benchmark;
    _ = @import("antfly_local_sources").storage_lite_snapshot_test;
    _ = @import("antfly_local_sources").storage_lite_secret_store;
    _ = @import("antfly_local_sources").storage_lite_paths;
    _ = @import("antfly_local_sources").storage_lite_restore_staging;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
