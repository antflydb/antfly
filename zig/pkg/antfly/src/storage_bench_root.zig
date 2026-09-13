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

pub const platform_time = @import("antfly_platform").time;
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lmdb_backend = @import("storage/lmdb_backend.zig");
pub const mem_backend = @import("storage/mem_backend.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const paths = @import("graph/paths.zig");

pub const resource_manager = @import("storage/resource_manager.zig");
pub const roaring = @import("encoding/roaring.zig");

pub const db = struct {
    pub const OpenMode = @import("antfly_source_root").antfly_sources.physical_db.OpenMode;
    pub const ReplayProgress = @import("antfly_source_root").antfly_sources.physical_db.ReplayProgress;
    pub const embedder = @import("storage/db/enrichment/embedder.zig");
    pub const replay_stream = @import("storage/db/derived/replay_stream.zig");
    pub const backfill_state = @import("storage/db/backfill_state.zig");
    pub const freeDBStats = @import("storage/db/types.zig").freeDBStats;
    pub const doc_identity = @import("storage/db/doc_identity.zig");
    pub const doc_set = @import("storage/db/doc_set.zig");
    pub const BatchProfile = @import("antfly_source_root").antfly_sources.physical_db.BatchProfile;
    pub const OpenOptions = @import("antfly_source_root").antfly_sources.physical_db.OpenOptions;
    pub const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
    pub const IndexManager = @import("storage/db/catalog/index_manager.zig").IndexManager;
    pub const aggregations = @import("storage/db/aggregations.zig");
    pub const algebraic = @import("storage/db/algebraic/mod.zig");
    pub const derived_types = @import("storage/db/derived/derived_types.zig");
    pub const docstore = @import("storage/docstore.zig");
    pub const types = @import("storage/db/types.zig");
};

pub const hbc = @import("storage/hbc_adapter.zig");
pub const vectorindex = @import("antfly_vectorindex");
pub const vector = @import("antfly_vector").vector;
pub const storage_lsm = @import("storage/lsm/mod.zig");
pub const metadata_api = @import("metadata/api.zig");
pub const metadata = @import("metadata/mod.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");
