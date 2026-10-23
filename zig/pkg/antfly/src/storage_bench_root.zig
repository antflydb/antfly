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

pub const platform_time = @import("antfly_platform").time;
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const mem_backend = @import("antfly_local_sources").storage_mem_backend;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;
pub const paths = @import("antfly_local_sources").graph_paths;

pub const resource_manager = @import("antfly_local_sources").storage_resource_manager;
pub const roaring = @import("antfly_local_sources").encoding_roaring;

pub const db = struct {
    pub const OpenMode = @import("antfly_source_root").antfly_sources.physical_db.OpenMode;
    pub const ReplayProgress = @import("antfly_source_root").antfly_sources.physical_db.ReplayProgress;
    pub const embedder = @import("antfly_local_sources").storage_db_enrichment_embedder;
    pub const replay_stream = @import("antfly_local_sources").storage_db_derived_replay_stream;
    pub const backfill_state = @import("antfly_local_sources").storage_db_backfill_state;
    pub const freeDBStats = @import("antfly_local_sources").storage_db_types.freeDBStats;
    pub const doc_identity = @import("antfly_local_sources").storage_db_doc_identity;
    pub const doc_set = @import("antfly_local_sources").storage_db_doc_set;
    pub const BatchProfile = @import("antfly_source_root").antfly_sources.physical_db.BatchProfile;
    pub const OpenOptions = @import("antfly_source_root").antfly_sources.physical_db.OpenOptions;
    pub const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
    pub const IndexManager = @import("antfly_local_sources").storage_db_catalog_index_manager.IndexManager;
    pub const aggregations = @import("antfly_local_sources").storage_db_aggregations;
    pub const algebraic = @import("antfly_local_sources").storage_db_algebraic_mod;
    pub const derived_types = @import("antfly_local_sources").storage_db_derived_derived_types;
    pub const docstore = @import("antfly_local_sources").storage_docstore;
    pub const types = @import("antfly_local_sources").storage_db_types;
};

pub const hbc = @import("antfly_local_sources").storage_hbc_adapter;
pub const vectorindex = @import("antfly_vectorindex");
pub const vector = @import("antfly_vector").vector;
pub const storage_lsm = @import("storage/lsm/mod.zig");
pub const metadata_api = @import("metadata/api.zig");
pub const metadata = @import("metadata/mod.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
