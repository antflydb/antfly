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

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const db_mod = @import("../db.zig");
const DB = db_mod.DB;
const Indexes = @import("mod.zig").Indexes;

/// Algebraic index state owned by `DB.indexes`: the HyperLogLog maintenance lane owner.
pub const AlgebraicIndexes = struct {
    hll_owner_id: u64 = 0,

    fn dbPtr(self: *AlgebraicIndexes) *DB {
        const parent: *Indexes = @alignCast(@fieldParentPtr("algebraic", self));
        return parent.dbPtr();
    }


    pub fn attachHllMaintenanceLane(self: *AlgebraicIndexes) !void {
        const db = self.dbPtr();
        const owner_id = try db.backend_runtime.allocOwnerId();
        self.hll_owner_id = owner_id;
        db.core.index_manager.attachHllMaintenance(db.backend_runtime.durable_jobs, owner_id);
    }
};
