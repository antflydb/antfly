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
const native = @import("../db/restore_staging.zig");
const backup = @import("../db/native_backup.zig");
const fs = @import("../../common/fs_paths.zig");
const records = @import("../../common/topology_records.zig");
pub const directory = "native-restore-owners";
pub const max_owners = 65_536;
pub const Owner = native.OwnerBootstrap;
pub const OwnerRef = struct {
    scope: native.Scope,
    byte_range: @import("../db/types.zig").ByteRange,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../db/relational_integrity_json.zig").write(self, jw);
    }
};
pub const OwnerTable = struct { table_id: u64, name: []const u8, schema_json: []const u8, read_schema_json: []const u8 = "", indexes_json: []const u8 };
pub const Compact = struct { tables: []const OwnerTable, owners: []const OwnerRef };

/// Keep schema payload proportional to tables, not table×owner count.
pub fn compact(alloc: std.mem.Allocator, owners: []const Owner) !Compact {
    var tables: std.ArrayList(OwnerTable) = .empty;
    errdefer tables.deinit(alloc);
    const refs = try alloc.alloc(OwnerRef, owners.len);
    errdefer alloc.free(refs);
    var seen: std.AutoHashMapUnmanaged(u64, OwnerTable) = .empty;
    defer seen.deinit(alloc);
    for (owners, refs) |owner, *ref| {
        const table: OwnerTable = .{ .table_id = owner.scope.target_namespace.table_id, .name = owner.table_name, .schema_json = owner.schema_json, .read_schema_json = owner.read_schema_json, .indexes_json = owner.indexes_json };
        if (seen.get(table.table_id)) |existing| {
            if (!std.mem.eql(u8, existing.name, table.name) or !std.mem.eql(u8, existing.schema_json, table.schema_json) or !std.mem.eql(u8, existing.read_schema_json, table.read_schema_json) or !std.mem.eql(u8, existing.indexes_json, table.indexes_json)) return error.RestoreStagingScopeChanged;
        } else {
            try seen.put(alloc, table.table_id, table);
            try tables.append(alloc, table);
        }
        ref.* = .{ .scope = owner.scope, .byte_range = owner.byte_range };
    }
    std.mem.sort(OwnerTable, tables.items, {}, struct {
        fn less(_: void, left: OwnerTable, right: OwnerTable) bool {
            return left.table_id < right.table_id;
        }
    }.less);
    return .{ .tables = tables.items, .owners = refs };
}

pub fn expand(alloc: std.mem.Allocator, tables: []const OwnerTable, refs: []const OwnerRef) ![]const Owner {
    if (refs.len > max_owners or tables.len > refs.len) return error.InvalidRestoreStagingRecord;
    var lookup: std.AutoHashMapUnmanaged(u64, struct { value: OwnerTable, used: bool = false }) = .empty;
    defer lookup.deinit(alloc);
    for (tables, 0..) |table, index| {
        if (table.table_id == 0 or (index != 0 and tables[index - 1].table_id >= table.table_id)) return error.InvalidRestoreStagingRecord;
        try lookup.put(alloc, table.table_id, .{ .value = table });
    }
    const owners = try alloc.alloc(Owner, refs.len);
    errdefer alloc.free(owners);
    for (refs, owners) |ref, *owner| {
        const table = lookup.getPtr(ref.scope.target_namespace.table_id) orelse return error.RestoreStagingScopeChanged;
        table.used = true;
        owner.* = .{ .scope = ref.scope, .table_name = table.value.name, .schema_json = table.value.schema_json, .read_schema_json = table.value.read_schema_json, .indexes_json = table.value.indexes_json, .byte_range = ref.byte_range };
        try owner.validate();
    }
    var iterator = lookup.valueIterator();
    while (iterator.next()) |entry| if (!entry.used) return error.InvalidRestoreStagingRecord;
    return owners;
}

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

pub const Projection = struct { owners: []const Owner, tables: []const records.TableRecord, ranges: []const records.RangeRecord };

/// Catalog overlaps are deduplicated by immutable owner identity, never by
/// table name. An unpublished replacement may intentionally reuse that name.
pub fn project(alloc: std.mem.Allocator, owners: []const Owner, public_tables: []const records.TableRecord, existing_tables: []const records.TableRecord, existing_ranges: []const records.RangeRecord) !Projection {
    if (owners.len > max_owners) return error.InvalidRestoreStagingRecord;
    var tables: std.ArrayList(records.TableRecord) = .empty;
    errdefer tables.deinit(alloc);
    var ranges: std.ArrayList(records.RangeRecord) = .empty;
    errdefer ranges.deinit(alloc);
    var result: std.ArrayList(Owner) = .empty;
    errdefer result.deinit(alloc);
    var table_ids: std.AutoHashMapUnmanaged(u64, records.TableRecord) = .empty;
    defer table_ids.deinit(alloc);
    var group_ids: std.AutoHashMapUnmanaged(u64, records.RangeRecord) = .empty;
    defer group_ids.deinit(alloc);
    for (existing_tables) |table| try table_ids.put(alloc, table.table_id, table);
    for (existing_ranges) |range| try group_ids.put(alloc, range.group_id, range);
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer seen.deinit(alloc);
    var public_ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer public_ids.deinit(alloc);
    for (public_tables) |table| try public_ids.put(alloc, table.table_id, {});
    for (owners) |owner| {
        try owner.validate();
        const namespace = owner.scope.target_namespace;
        if (namespace.shard_id != namespace.range_id) return error.RestoreStagingScopeChanged;
        const unique = try seen.getOrPut(alloc, namespace.shard_id);
        if (unique.found_existing) return error.InvalidRestoreStagingRecord;
        if (public_ids.contains(namespace.table_id)) continue;
        if (group_ids.get(namespace.shard_id)) |range| {
            const effective_shard = if (range.doc_identity_shard_id != 0) range.doc_identity_shard_id else range.group_id;
            const effective_range = if (range.doc_identity_range_id != 0) range.doc_identity_range_id else if (range.range_id != 0) range.range_id else range.group_id;
            if (range.table_id != namespace.table_id or effective_shard != namespace.shard_id or effective_range != namespace.range_id) return error.RestoreStagingScopeChanged;
            continue;
        }
        if (table_ids.get(namespace.table_id)) |table| {
            if (!std.mem.eql(u8, table.name, owner.table_name)) return error.RestoreStagingScopeChanged;
        } else {
            const table: records.TableRecord = .{ .table_id = namespace.table_id, .name = owner.table_name, .schema_json = owner.schema_json, .read_schema_json = owner.read_schema_json, .indexes_json = owner.indexes_json };
            try table_ids.put(alloc, namespace.table_id, table);
            try tables.append(alloc, table);
        }
        try result.append(alloc, owner);
        try ranges.append(alloc, .{ .group_id = namespace.shard_id, .table_id = namespace.table_id, .range_id = namespace.range_id, .doc_identity_shard_id = namespace.shard_id, .doc_identity_range_id = namespace.range_id, .start_key = owner.byte_range.start, .end_key = owner.byte_range.end });
    }
    return .{ .owners = result.items, .tables = tables.items, .ranges = ranges.items };
}
