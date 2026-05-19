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

pub const platform_time = @import("platform/time.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lmdb_backend = @import("storage/lmdb_backend.zig");
pub const mem_backend = @import("storage/mem_backend.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const paths = @import("graph/paths.zig");

pub const db = struct {
    pub const DB = @import("storage/db/db.zig").DB;
    pub const IndexManager = @import("storage/db/catalog/index_manager.zig").IndexManager;
    pub const aggregations = @import("storage/db/aggregations.zig");
    pub const algebraic = @import("storage/db/algebraic/mod.zig");
    pub const derived_types = @import("storage/db/derived/derived_types.zig");
    pub const docstore = @import("storage/docstore.zig");
    pub const types = @import("storage/db/types.zig");
};
