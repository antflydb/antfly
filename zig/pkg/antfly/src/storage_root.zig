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

pub const backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const backend_types = @import("antfly_local_sources").storage_backend_types;
pub const hot_standby = @import("storage/hot_standby/mod.zig");
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const resource_manager = @import("antfly_local_sources").storage_resource_manager;
pub const rowsource = @import("storage/rowsource/mod.zig");
pub const sim_runtime = @import("antfly_local_sources").storage_sim_runtime;

pub const antfly_sources = struct {
    pub const physical_db = @import("antfly_local_sources").storage_db_db;
};

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
