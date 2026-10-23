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

//! Internal dependency surface for the production data runtime.

pub const admin = @import("../admin/mod.zig");
pub const common = @import("../common/mod.zig");
pub const data = @import("domain.zig");
pub const db = @import("antfly_local_sources").storage_db_selected_root.db;
pub const extensions = @import("../extensions/mod.zig");
pub const hot_standby = @import("../storage/hot_standby/mod.zig");
pub const inference = @import("antfly_local_sources").inference_mod;
pub const internal = @import("../internal/mod.zig");
pub const internal_keys = @import("antfly_local_sources").storage_internal_keys;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;
pub const metadata = @import("../metadata/domain.zig");
pub const metadata_api = @import("../metadata/api.zig");
pub const metadata_http_client = @import("../metadata/http_client.zig");
pub const metadata_http_routes = @import("../metadata/http_routes.zig");
pub const metadata_service = @import("../metadata/service.zig");
pub const public_api = @import("../api/runtime.zig");
pub const raft = @import("../raft/mod.zig");
pub const readers = @import("antfly_readers");
pub const shard = @import("antfly_local_sources").storage_shard;
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const synthesizing = @import("antfly_synthesizing");
pub const transcribing = @import("antfly_transcribing");
pub const usermgr = @import("../usermgr/mod.zig");
