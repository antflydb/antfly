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

//! Focused collector/publication ownership and bounded embedding contracts. The normal
//! storage shards still own these tests; this root avoids HTTP discovery when
//! qualifying allocation failures or provider batch behavior independently.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = @import("local_test_sources.zig");
test {
    _ = antfly_sources.physical_db;
    // Explicit ownership keeps native publication/repair feedback available
    // here without compiling the C API's discovery and consumer fixture roots.
    _ = @import("antfly_local_sources").storage_db_db;
    _ = @import("antfly_local_sources").storage_persistent;
    _ = @import("antfly_local_sources").storage_wal;
    _ = @import("antfly_local_sources").storage_lite_index_storage;
    _ = @import("antfly_local_sources").storage_lite_native;
    _ = @import("antfly_local_sources").storage_db_catalog_index_manager;
    _ = @import("antfly_local_sources").index;
    _ = @import("antfly_local_sources").storage_db_result_collectors;
    _ = @import("antfly_local_sources").storage_db_enrichment_enrichment_runtime;
    _ = @import("antfly_local_sources").storage_db_artifact_chunk_vector_publication;
    _ = @import("antfly_local_sources").storage_db_artifact_completion_progress;
    _ = @import("antfly_local_sources").storage_db_artifact_publication;
    _ = @import("antfly_local_sources").storage_db_artifact_producer_input;
}
