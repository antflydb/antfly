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

//! Transaction-local proof of bounded native lifetime recovery. The ordinary
//! table publication paths own retirement/adoption/empty-generation receipts;
//! this verifier only advances after those exact catalog effects are durable.
const std = @import("std");
const boots = @import("boots.zig");
const work = @import("recovery_work.zig");

pub const Verifier = struct {
    alloc: std.mem.Allocator,
    group: u64,

    pub fn requireRecoveryCut(self: Verifier, txn: anytype, expected: boots.Record, after: ?u64) !void {
        if (expected.phase != .recovering or (after != null and after.? == 0)) return error.InvalidSqlRelationPersistence;
        const page = try work.list(self.alloc, txn, self.group, expected.node_id, expected.recovery_after, work.max_page);
        defer self.alloc.free(page.entries);
        for (page.entries) |candidate| {
            const entry = candidate.table;
            if (after) |cut| if (entry.table_id > cut) break;
            if (entry.descriptor.kind == .temporary) {
                if (candidate.hidden != null or !try @import("../../temporary_relation_retirement.zig").requireEntryRetired(self.alloc, txn, self.group, entry, expected)) return error.SqlRelationRecoveryIncomplete;
                continue;
            }
            // TEMP retirement removes the entry. UNLOGGED adoption/reset
            // atomically publishes the authenticated current process epoch.
            // A cursor cannot skip an extant old-process source of either kind.
            if (entry.initialized == null or
                !std.mem.eql(u8, &entry.initialized.?.binding.native_instance, &expected.instance) or
                entry.initialized.?.boot_generation != expected.generation)
                return error.SqlRelationRecoveryIncomplete;
        }
        if (page.next != null and (after == null or after.? > page.entries[page.entries.len - 1].table.table_id))
            return error.SqlRelationRecoveryIncomplete;
    }
};
