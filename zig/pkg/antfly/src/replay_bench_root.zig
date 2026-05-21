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
pub const resource_manager = @import("storage/resource_manager.zig");
pub const db = struct {
    pub const types = @import("storage/db/types.zig");
    pub const freeDBStats = @import("storage/db/types.zig").freeDBStats;
    pub const doc_identity = @import("storage/db/doc_identity.zig");
    pub const doc_set = @import("storage/db/doc_set.zig");
    pub const embedder = @import("storage/db/enrichment/embedder.zig");
    pub const replay_stream = @import("storage/db/derived/replay_stream.zig");
    pub const BatchProfile = @import("storage/db/db.zig").BatchProfile;
    pub const OpenOptions = @import("storage/db/db.zig").OpenOptions;
    pub const DB = @import("storage/db/db.zig").DB;
};
