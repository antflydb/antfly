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

pub const TableEntry = struct { name: []const u8, table: metadata.TableRecord };
pub const TableListing = struct {
    revision: u64,
    entries: []const TableEntry,
    ranges: []metadata.RangeRecord = &.{},
    stores: []metadata.StoreRecord = &.{},
    placement_intents: []raft.PlacementIntent = &.{},

    /// Borrow projection storage and allocate only the table reference array.
    pub fn adminSnapshot(self: @This(), alloc: std.mem.Allocator) !api.AdminSnapshot {
        const tables = try alloc.alloc(metadata.TableRecord, self.entries.len);
        for (self.entries, tables) |entry, *table| table.* = entry.table;
        return .{ .status = .{ .metadata_group_id = 1, .metadata_epoch = self.revision, .metrics = .{} }, .tables = tables, .ranges = self.ranges, .stores = self.stores, .placement_intents = self.placement_intents, .split_transitions = &.{}, .merge_transitions = &.{} };
    }
};

/// Portable primary state, with logical identities and physical topology
/// observed together. Derived indexes are reconstructed by the destination.
pub const Export = struct {
    epoch: u64,
    tables: []metadata.TableRecord,
    ranges: []metadata.RangeRecord,
    system_catalog: domain.State = .{},
    extension_packages: []extensions.PackageManifest = &.{},
    installed_extensions: []extensions.InstalledExtension = &.{},
    extension_members: []extensions.ExtensionMember = &.{},
    extension_dependencies: []extensions.ExtensionDependency = &.{},
};
