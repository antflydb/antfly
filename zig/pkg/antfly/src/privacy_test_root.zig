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

// Metadata store tests and publication regressions also own physical DB
// fixtures; keep their restore decoder and source identities in this root.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = @import("local_test_sources.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;
test "privacy API and transport contracts" {
    _ = @import("api/privacy.zig");
    _ = @import("api/privacy_mutation_test.zig");
    _ = @import("metadata/catalog_projection_reader.zig");
    _ = @import("api/kernel_bridge.zig");
    _ = @import("api/privacy_checkpoints.zig");
    _ = @import("antfly_local_sources").storage_db_relational_integrity_json;
    _ = @import("api/http_routes.zig");
    _ = @import("antfly_local_sources").privacy_catalog;
    _ = @import("metadata/storage/raft_apply_store.zig");
    _ = @import("metadata/service.zig");
    _ = @import("api/privacy_transport_test.zig");
    _ = @import("api/privacy_checkpoint_gc_test.zig");
    _ = @import("api/http_client.zig");
    _ = @import("storage/hot_standby/mutation_inventory.zig");
    _ = @import("storage/artifact_publication_dispatch.zig");
}
