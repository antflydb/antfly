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

//! Authority-neutral immutable identity. No position, allocation or execution grant.
const std = @import("std");

/// Metadata's immutable owner identity, without physical allocation choices or
/// schema-sized inputs. Status is information, never publication permission.
pub const Identity = struct {
    pub const max_json_bytes = 4096;
    id: [16]u8,
    plan_digest: [32]u8,
    owner_digest: [32]u8,
    ordinal: u16,
    namespace: @import("doc_identity.zig").Namespace,
    owner_group_id: u64,

    pub fn validate(self: Identity) !void {
        if (std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.owner_digest, 0) or self.owner_group_id == 0 or
            self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.ordinal >= @import("relational_index_publication_progress.zig").max_owners) return error.InvalidPrivateIndexOwnerJob;
    }
};
