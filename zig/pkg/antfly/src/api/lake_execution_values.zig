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

//! Publication control values; no physical index builders.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const A = std.mem.Allocator;
pub const Clock = struct {
    ptr: *const anyopaque,
    now_ms: *const fn (*const anyopaque) anyerror!u64,
};

pub const Lease = struct {
    ptr: *anyopaque,
    snapshot: *const fn (*anyopaque, A) anyerror!local.common_topology_records.TableRecord,
    save_checkpoint: ?*const fn (*anyopaque, catalog.Publication) anyerror!void = null,
    discard_checkpoint: ?*const fn (*anyopaque) anyerror!void = null,
};

pub const BuildPolicy = struct { allow_reuse: bool = true, identity_spill_bytes: u64 = 1024 * 1024 * 1024 };

/// Reopen physical clients in the execution owner. No reader, scanner, object
/// store vtable, allocator state, or scheduler handle crosses this contract.
pub const StoreDescriptor = struct {
    config: ?*const local.common_config.Config,
    secrets: ?*local.common_secrets.FileStore,
    deployment: local.common_config.DeploymentMode,
    local_base_dir: ?[]const u8,
    locator: catalog.StoreLocator,
    identity: catalog.Digest,
    cloud_vacuum: ?@import("../serverless/artifacts/cloud_generations.zig").VacuumCut,
};
