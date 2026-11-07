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

//! Indexed catalog preparation for native restart. This contains no row
//! payload, SQL executor identity, fabricated source fence or physical proof.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const Request = struct {
    table_id: ?u64 = null,
    storage_name: ?[]const u8 = null,
    expected_revision: ?u64 = null,
    pub fn validate(self: Request) !void {
        if ((self.table_id == null) == (self.storage_name == null)) return error.InvalidSqlRelationPersistence;
        if (self.table_id) |id| if (id == 0) return error.InvalidSqlRelationPersistence;
        if (self.storage_name) |name| if (name.len == 0 or name.len > 512) return error.InvalidSqlRelationPersistence;
    }
};
pub const Capture = struct {
    relation: @import("../system_catalog/trigger_snapshot.zig").Snapshot,
    resource: local.system_catalog_domain.Resource,
    ranges: []const local.common_topology_records.RangeRecord,
    boot: ?@import("sql_relation_boots.zig").Observation = null,
};
