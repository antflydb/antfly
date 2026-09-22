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

//! Authenticated HA-created owners which do not belong to public Raft placement.
//! Immutable per-owner records survive replay restart and offline reseeding.
const std = @import("std");
const native = @import("../db/restore_staging_contract.zig");
const backup = @import("../db/native_backup.zig");
const fs = @import("../../common/fs_paths.zig");
const records = @import("../../common/topology_records.zig");
pub const directory = "native-restore-owners";
pub const max_owners = @import("restore_owner_contract.zig").max_owners;
pub const Owner = native.OwnerBootstrap;
pub const OwnerRef = @import("restore_owner_contract.zig").OwnerRef;
pub const OwnerTable = @import("restore_owner_contract.zig").OwnerTable;
pub const Compact = @import("restore_owner_contract.zig").Compact;
pub const compact = @import("restore_owner_contract.zig").compact;

pub const expand = @import("restore_owner_contract.zig").expand;

pub fn record(alloc: std.mem.Allocator, io: std.Io, metadata_root: []const u8, owner: Owner) !void {
    try owner.validate();
    const encoded = try owner.encode(alloc);
    defer alloc.free(encoded);
    const root = try std.fs.path.join(alloc, &.{ metadata_root, directory });
    defer alloc.free(root);
    try fs.createDirPathPortable(io, root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}.owner", .{ root, owner.scope.target_namespace.shard_id });
    defer alloc.free(path);
    const previous = backup.readFileAlloc(alloc, io, path, 64 * 1024 * 1024) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (previous) |bytes| alloc.free(bytes);
    if (previous) |bytes| {
        if (!std.mem.eql(u8, bytes, encoded)) return error.RestoreStagingScopeChanged;
        try fs.syncDirPortable(io, root);
        try fs.syncDirPortable(io, metadata_root);
        if (std.fs.path.dirname(metadata_root)) |parent| try fs.syncDirPortable(io, parent);
        return;
    }
    const pending = try std.fmt.allocPrint(alloc, "{s}.next", .{path});
    defer alloc.free(pending);
    _ = try backup.writeFileDurable(io, pending, encoded);
    try std.Io.Dir.rename(.cwd(), pending, .cwd(), path, io);
    try fs.syncDirPortable(io, root);
    try fs.syncDirPortable(io, metadata_root);
    if (std.fs.path.dirname(metadata_root)) |parent| try fs.syncDirPortable(io, parent);
}

/// The permanent terminal ledger must commit before calling this function.
/// Retry the directory barrier even if the previous deletion already happened.
pub fn retireTerminal(alloc: std.mem.Allocator, io: std.Io, metadata_root: []const u8, group_id: u64, scope: [32]u8) !void {
    const root = try std.fs.path.join(alloc, &.{ metadata_root, directory });
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}.owner", .{ root, group_id });
    defer alloc.free(path);
    const bytes = backup.readFileAlloc(alloc, io, path, 64 * 1024 * 1024) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    if (bytes) |encoded| {
        defer alloc.free(encoded);
        var owner = try Owner.decode(alloc, encoded);
        defer owner.deinit();
        if (owner.value.scope.target_namespace.shard_id != group_id or !std.mem.eql(u8, &owner.value.scope.digest(), &scope)) return error.RestoreStagingScopeChanged;
        try std.Io.Dir.cwd().deleteFile(io, path);
    }
    fs.syncDirPortable(io, root) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    try fs.syncDirPortable(io, metadata_root);
}

pub const Inventory = struct {
    arena: std.heap.ArenaAllocator,
    owners: []const Owner,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn load(alloc: std.mem.Allocator, io: std.Io, metadata_root: []const u8) !Inventory {
    return loadWithPublicTables(alloc, io, metadata_root, &.{});
}

/// Public table IDs are durable incarnation authority, not names. Once a
/// table is published, ordinary placement owns all its current ranges; old
/// native-only descriptors must not resurrect retired split/merge owners.
pub fn loadWithPublicTables(alloc: std.mem.Allocator, io: std.Io, metadata_root: []const u8, public_tables: []const records.TableRecord) !Inventory {
    return loadWithTerminalSnapshot(alloc, io, metadata_root, public_tables, null);
}

pub fn loadWithTerminalSnapshot(alloc: std.mem.Allocator, io: std.Io, metadata_root: []const u8, public_tables: []const records.TableRecord, terminals: ?*@import("restore_terminal_ledger.zig").Snapshot) !Inventory {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const path = try std.fs.path.join(owned, &.{ metadata_root, directory });
    var dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return .{ .arena = arena, .owners = &.{} },
        else => return err,
    };
    defer dir.close(io);
    var owners: std.ArrayList(Owner) = .empty;
    var iterator = dir.iterate();
    var public_ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer public_ids.deinit(alloc);
    for (public_tables) |table| try public_ids.put(alloc, table.table_id, {});
    var table_payloads: std.AutoHashMapUnmanaged(u64, Owner) = .empty;
    defer table_payloads.deinit(alloc);
    var retained_bytes: usize = 0;
    var reclaimed = false;
    defer if (reclaimed) fs.syncDirPortable(io, path) catch {};
    while (try iterator.next(io)) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".next")) continue;
        if (entry.kind != .file or !std.mem.startsWith(u8, entry.name, "group-") or !std.mem.endsWith(u8, entry.name, ".owner")) return error.InvalidRestoreStagingRecord;
        const group_id = std.fmt.parseInt(u64, entry.name[6 .. entry.name.len - 6], 10) catch return error.InvalidRestoreStagingRecord;
        const full = try std.fs.path.join(alloc, &.{ path, entry.name });
        defer alloc.free(full);
        const encoded = try backup.readFileAlloc(alloc, io, full, 64 * 1024 * 1024);
        defer alloc.free(encoded);
        var parsed = try Owner.decode(alloc, encoded);
        defer parsed.deinit();
        if (parsed.value.scope.target_namespace.shard_id != group_id) return error.RestoreStagingScopeChanged;
        if (terminals) |snapshot| if (try snapshot.get(group_id)) |terminal| {
            if (terminal.table_id != parsed.value.scope.target_namespace.table_id or !std.mem.eql(u8, &terminal.scope, &parsed.value.scope.digest())) return error.RestoreStagingScopeChanged;
            try std.Io.Dir.cwd().deleteFile(io, full);
            reclaimed = true;
            continue;
        };
        if (public_ids.contains(parsed.value.scope.target_namespace.table_id)) {
            try std.Io.Dir.cwd().deleteFile(io, full);
            reclaimed = true;
            continue;
        }
        if (owners.items.len == max_owners) return error.InvalidRestoreStagingRecord;
        var owner = parsed.value;
        if (table_payloads.get(owner.scope.target_namespace.table_id)) |shared| {
            if (!std.mem.eql(u8, shared.table_name, owner.table_name) or !std.mem.eql(u8, shared.schema_json, owner.schema_json) or !std.mem.eql(u8, shared.read_schema_json, owner.read_schema_json) or !std.mem.eql(u8, shared.indexes_json, owner.indexes_json)) return error.RestoreStagingScopeChanged;
            owner.table_name = shared.table_name;
            owner.schema_json = shared.schema_json;
            owner.read_schema_json = shared.read_schema_json;
            owner.indexes_json = shared.indexes_json;
        } else {
            retained_bytes = std.math.add(usize, retained_bytes, owner.table_name.len +| owner.schema_json.len +| owner.read_schema_json.len +| owner.indexes_json.len) catch return error.InvalidRestoreStagingRecord;
            if (retained_bytes > 64 * 1024 * 1024) return error.InvalidRestoreStagingRecord;
            owner.table_name = try owned.dupe(u8, owner.table_name);
            owner.schema_json = try owned.dupe(u8, owner.schema_json);
            owner.read_schema_json = try owned.dupe(u8, owner.read_schema_json);
            owner.indexes_json = try owned.dupe(u8, owner.indexes_json);
            try table_payloads.put(alloc, owner.scope.target_namespace.table_id, owner);
        }
        retained_bytes = std.math.add(usize, retained_bytes, @sizeOf(Owner) +| owner.byte_range.start.len +| owner.byte_range.end.len) catch return error.InvalidRestoreStagingRecord;
        if (retained_bytes > 64 * 1024 * 1024) return error.InvalidRestoreStagingRecord;
        owner.byte_range = .{ .start = try owned.dupe(u8, owner.byte_range.start), .end = try owned.dupe(u8, owner.byte_range.end) };
        try owners.append(owned, owner);
    }
    if (reclaimed) {
        try fs.syncDirPortable(io, path);
        reclaimed = false;
    }
    std.mem.sort(Owner, owners.items, {}, struct {
        fn less(_: void, left: Owner, right: Owner) bool {
            return left.scope.target_namespace.shard_id < right.scope.target_namespace.shard_id;
        }
    }.less);
    return .{ .arena = arena, .owners = owners.items };
}

pub const Projection = @import("restore_owner_contract.zig").Projection;
pub const project = @import("restore_owner_contract.zig").project;
