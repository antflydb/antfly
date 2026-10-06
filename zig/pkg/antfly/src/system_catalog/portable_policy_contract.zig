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

//! Validate immutable row-policy programs before staging a portable owner.
const std = @import("std");

pub fn validatePolicyPrograms(alloc: std.mem.Allocator, publications: []const @import("antfly_local_sources").system_catalog_policies.Publication, programs: []const @import("antfly_local_sources").system_catalog_policies.InstallSnapshot) !void {
    const policies = @import("antfly_local_sources").system_catalog_policies;
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
