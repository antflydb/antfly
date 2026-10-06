// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Focused facade for the installed Antfly CLI. Keep this list limited to the
//! namespaces referenced by main.zig, serverless_main.zig, and cmd/ so the
//! compiler does not have to load the entire public library root.

pub const build_options = @import("build_options");

pub const admin = @import("admin/mod.zig");
pub const common = @import("common/mod.zig");
pub const data = @import("data/mod.zig");
pub const graph = @import("antfly_local_sources").graph_graph;
pub const graph_query = @import("antfly_local_sources").graph_query;
pub const metadata = @import("metadata/mod.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");
pub const serverless = @import("serverless/mod.zig");

pub const hot_standby = @import("storage/hot_standby/mod.zig");
pub const db = @import("antfly_local_sources").storage_db_selected_root.db;
pub const lite = @import("antfly_local_sources").storage_lite_mod;
pub const backup_codec = @import("antfly_local_sources").storage_backup_codec;
pub const backup_bundle = @import("antfly_local_sources").storage_backup_bundle;
pub const backup_bundle_io = @import("antfly_local_sources").storage_backup_bundle_io;
pub const backup_repository = @import("storage/backup_repository.zig");
pub const portable_backup = @import("antfly_local_sources").storage_portable_backup;
pub const platform_clock = @import("antfly_platform").clock;
pub const platform_time = @import("antfly_platform").time;

// usermgr/storage_imports.zig depends back on these through antfly_root.
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
