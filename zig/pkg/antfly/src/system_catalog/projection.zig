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

//! Owned wire projections. Producers capture every row in one metadata read
//! transaction (or under the standalone metadata mutex).
const std = @import("std");
const domain = @import("domain.zig");
const metadata = @import("../metadata/table_manager.zig");
const api = @import("../metadata/api.zig");
const extensions = @import("../extensions/mod.zig");
const raft = @import("../raft/reconciler.zig");

/// One immutable metadata read: the table schema and all applicable extension
/// constraints. No placement, runtime, or unrelated-table inventory is copied.
pub const WriteValidation = struct {
    schema_json: []const u8,
    data_shapes: []const []const u8 = &.{},
};

pub const TableEntry = struct { name: []const u8, table: metadata.TableRecord };
pub const TableListing = struct {
    revision: u64,
    next_after: ?[]const u8 = null,
    next_table_id: ?u64 = null,
    legacy_membership: [32]u8 = @splat(0),
    entries: []const TableEntry,
    ranges: []metadata.RangeRecord = &.{},
    stores: []metadata.StoreRecord = &.{},
    placement_intents: []raft.PlacementIntent = &.{},
    replication_source_statuses: []metadata.ReplicationSourceStatusRecord = &.{},

    /// Borrow projection storage and allocate only the table reference array.
    pub fn adminSnapshot(self: @This(), alloc: std.mem.Allocator) !api.AdminSnapshot {
        const tables = try alloc.alloc(metadata.TableRecord, self.entries.len);
        for (self.entries, tables) |entry, *table| table.* = entry.table;
        return .{ .status = .{ .metadata_group_id = 1, .metadata_epoch = self.revision, .metrics = .{} }, .tables = tables, .ranges = self.ranges, .stores = self.stores, .placement_intents = self.placement_intents, .replication_source_statuses = self.replication_source_statuses, .replication_source_action_hints = try api.deriveReplicationSourceActionHints(alloc, tables, self.replication_source_statuses), .split_transitions = &.{}, .merge_transitions = &.{} };
    }
};

/// Portable primary state, with logical identities and physical topology
/// observed together. Derived indexes are reconstructed by the destination.
pub const Export = struct {
    epoch: u64,
    tables: []metadata.TableRecord,
    ranges: []metadata.RangeRecord,
    system_catalog: domain.State = .{},
    /// Immutable programs for active row-policy generations. Draft policy
    /// definitions may already differ, so they cannot reconstruct these
    /// programs during restore. An in-flight publication is not exportable.
    policy_install_snapshots: []const @import("policies.zig").InstallSnapshot = &.{},
    extension_packages: []extensions.PackageManifest = &.{},
    installed_extensions: []extensions.InstalledExtension = &.{},
    extension_members: []extensions.ExtensionMember = &.{},
    extension_dependencies: []extensions.ExtensionDependency = &.{},
};

/// Validate the portable policy manifest before staging any owner. Draft
/// records are intentionally not used here: an active generation is defined
/// by its immutable compiled install snapshot, not the current draft.
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

/// Keyset pagination concerns logical membership, not changing runtime counters.
pub fn selectPage(entries: []TableEntry, request: domain.TableList, revision: u64) !struct { entries: []TableEntry, next: ?[]const u8 } {
    if (request.revision) |expected| if (expected != revision) return error.CatalogGenerationChanged;
    if (request.limit) |limit| if (limit == 0 or limit > 1000) return error.InvalidCatalogName;
    std.mem.sort(TableEntry, entries, {}, struct {
        fn less(_: void, left: TableEntry, right: TableEntry) bool {
            return std.mem.lessThan(u8, left.name, right.name);
        }
    }.less);
    var start: usize = 0;
    if (request.after) |after| while (start < entries.len and !std.mem.lessThan(u8, after, entries[start].name)) : (start += 1) {};
    const count = @min(entries.len - start, request.limit orelse std.math.maxInt(u32));
    const page = entries[start..][0..count];
    return .{ .entries = page, .next = if (start + count < entries.len and count != 0) page[count - 1].name else null };
}

/// Order-independent fingerprint of unbound physical table identities. Unlike
/// runtime epochs this survives heartbeats and can be rebuilt after restore.
pub fn legacyIdentity(id: u64, name: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, id, .little);
    hash.update(&bytes);
    hash.update(name);
    return hash.finalResult();
}
pub fn checkMembership(request: domain.TableList, current: [32]u8) !void {
    if (request.legacy_membership) |expected| if (!std.mem.eql(u8, &expected, &current)) return error.CatalogGenerationChanged;
}

test "system catalog pages sort literal names and fence membership independently of runtime" {
    var entries = [_]TableEntry{
        .{ .name = "z", .table = .{ .table_id = 1, .name = "one" } },
        .{ .name = "a/b", .table = .{ .table_id = 2, .name = "two" } },
        .{ .name = "a*", .table = .{ .table_id = 3, .name = "three" } },
    };
    const first = try selectPage(&entries, .{ .limit = 2 }, 7);
    try std.testing.expectEqualStrings("a*", first.entries[0].name);
    try std.testing.expectEqualStrings("a/b", first.next.?);
    const last = try selectPage(&entries, .{ .limit = 2, .after = first.next, .revision = 7 }, 7);
    try std.testing.expectEqualStrings("z", last.entries[0].name);
    try std.testing.expect(last.next == null);
    try std.testing.expectError(error.CatalogGenerationChanged, selectPage(&entries, .{ .revision = 6 }, 7));
    try std.testing.expectError(error.InvalidCatalogName, selectPage(&entries, .{ .limit = 0 }, 7));
    try std.testing.expectError(error.CatalogGenerationChanged, checkMembership(.{ .legacy_membership = legacyIdentity(1, "old") }, legacyIdentity(1, "new")));
}
