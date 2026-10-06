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

//! Internal dependency surface for the production standalone runtime.

pub const admin = @import("../admin/mod.zig");
pub const common = @import("../common/mod.zig");
pub const data = @import("../data/mod.zig");
pub const db = @import("antfly_local_sources").storage_db_selected_root.db;
pub const extensions = @import("../extensions/mod.zig");
pub const extracting = @import("antfly_extracting");
pub const hot_standby = @import("../storage/hot_standby/mod.zig");
pub const inference = @import("antfly_local_sources").inference_mod;
pub const inference_runtime = @import("../inference_runtime/runtime.zig");
pub const internal = @import("../internal/mod.zig");
pub const lite = @import("antfly_local_sources").storage_lite_mod;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;
pub const metadata = @import("../metadata/mod.zig");
pub const metadata_api = @import("../metadata/api.zig");
pub const metadata_service = @import("../metadata/service.zig");
pub const public_api = @import("../api/runtime.zig");
pub const casbin = @import("antfly_casbin");
pub const raft = @import("../raft/mod.zig");
pub const readers = @import("antfly_readers");
pub const resource_manager = @import("antfly_local_sources").storage_resource_manager;
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const storage_maintenance = @import("antfly_local_sources").storage_maintenance;
pub const synthesizing = @import("antfly_synthesizing");
pub const template = @import("antfly_local_sources").template;
pub const transcribing = @import("antfly_transcribing");
pub const usermgr = @import("../usermgr/mod.zig");
