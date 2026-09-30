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

//! Validate immutable row-policy programs before staging a portable owner.
const std = @import("std");

pub fn validatePolicyPrograms(alloc: std.mem.Allocator, publications: []const @import("policies.zig").Publication, programs: []const @import("policies.zig").InstallSnapshot) !void {
    const policies = @import("policies.zig");
    if (programs.len > publications.len) return error.RowPolicyCatalogChanged;
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer seen.deinit(alloc);
    var active: std.AutoHashMapUnmanaged(u64, *const policies.Publication) = .empty;
    defer active.deinit(alloc);
    for (publications) |*publication| {
        try publication.validateShape();
        const entry = try seen.getOrPut(alloc, publication.table_id);
        if (entry.found_existing) return error.RowPolicyCatalogChanged;
        switch (publication.phase) {
            .active => try active.put(alloc, publication.table_id, publication),
            .disabled => if (publication.disabled_acknowledged_owners.len != publication.required_owners.len) return error.RowPolicyCatalogChanged,
            else => return error.RowPolicyPublicationInProgress,
        }
    }
    for (programs) |program| {
        try program.validateShape();
        const publication = active.get(program.table_id) orelse return error.RowPolicyCatalogChanged;
        if (program.phase != .active or program.policy_generation != publication.generation or
            program.catalog_epoch != publication.catalog_epoch or program.schema_version != publication.schema_version or
            !std.mem.eql(u8, &program.schema_digest, &publication.schema_digest)) return error.RowPolicyCatalogChanged;
        _ = active.remove(program.table_id);
    }
    if (active.count() != 0) return error.RowPolicyCatalogChanged;
}
