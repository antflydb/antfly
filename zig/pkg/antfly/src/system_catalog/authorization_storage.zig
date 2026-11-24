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

//! Catalog-transaction SQL principals and ACL records. Authoritative rows and
//! derived membership/dependency directories share the caller's transaction.
const std = @import("std");
const local = @import("antfly_local_sources");
const records = local.system_catalog_privileges;
const storage = @import("storage.zig");
const A = std.mem.Allocator;
const Principal = records.Principal;
const Scope = records.Scope;
const Record = records.Record;
pub const Member = struct { role_id: u64, instance: [16]u8, inherit: bool, set: bool, admin: bool };

fn eraseKey(txn: anytype, key: []const u8) !void {
    txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

pub fn objectPrefix(a: A, group: u64) ![]u8 {
    if (group == 0) return error.InvalidCatalogMutation;
    return std.fmt.allocPrint(a, "\x00\x00__metadata__:system_catalog:{d}:privilege:", .{group});
}
pub fn objectKey(a: A, group: u64, scope: Scope) ![]u8 {
    try scope.validate();
    const prefix = try objectPrefix(a, group);
    defer a.free(prefix);
    const suffix_len: usize = if (scope == .defaults) 26 else 9;
    const key = try a.alloc(u8, prefix.len + suffix_len);
    @memcpy(key[0..prefix.len], prefix);
    const suffix = key[prefix.len..];
    switch (scope) {
        .table, .role => |id| {
            suffix[0] = if (scope == .table) 't' else 'r';
            std.mem.writeInt(u64, suffix[1..9], id, .big);
        },
        .defaults => |value| {
            suffix[0] = 'd';
            std.mem.writeInt(u64, suffix[1..9], value.namespace_id, .big);
            suffix[9] = @intCast(@backingInt(value.owner.kind));
            @memcpy(suffix[10..26], &value.owner.instance);
        },
    }
    return key;
}
fn derivedPrefix(a: A, group: u64, name: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "\x00\x00__metadata_derived__:system_catalog_name:{d}:authorization_{s}:", .{ group, name });
}
pub fn roleOriginKey(a: A, group: u64, instance: [16]u8) ![]u8 {
    const prefix = try derivedPrefix(a, group, "role_origin");
    defer a.free(prefix);
    return std.fmt.allocPrint(a, "{s}{s}", .{ prefix, &std.fmt.bytesToHex(instance, .lower) });
}
fn principalPrefix(a: A, group: u64, name: []const u8, principal: Principal) ![]u8 {
    try principal.validate();
    const prefix = try derivedPrefix(a, group, name);
    defer a.free(prefix);
    return std.fmt.allocPrint(a, "{s}{t}:{s}:", .{ prefix, principal.kind, &std.fmt.bytesToHex(principal.instance, .lower) });
}
fn memberKey(a: A, group: u64, principal: Principal, role_id: u64) ![]u8 {
    const prefix = try principalPrefix(a, group, "member", principal);
    defer a.free(prefix);
    const result = try a.alloc(u8, prefix.len + 8);
    @memcpy(result[0..prefix.len], prefix);
    std.mem.writeInt(u64, result[prefix.len..][0..8], role_id, .big);
    return result;
}
fn referenceKey(a: A, group: u64, principal: Principal, scope: Scope) ![]u8 {
    const prefix = try principalPrefix(a, group, "reference", principal);
    defer a.free(prefix);
    const suffix = try objectKey(a, group, scope);
    defer a.free(suffix);
    return std.mem.concat(a, u8, &.{ prefix, suffix });
}
pub fn roleForName(a: A, txn: anytype, group: u64, name: []const u8) !?records.Role {
    const resource = (try storage.find(a, txn, group, .role, 0, name)) orelse return null;
    defer resource.deinit();
    const role = try std.json.parseFromSliceLeaky(records.Role, a, resource.value.source_json, .{ .allocate = .alloc_always });
    try role.validate();
    if (role.id != resource.value.id or !std.mem.eql(u8, role.name, resource.value.name) or resource.value.parent_id != 0) return error.InvalidCatalogRecord;
    return role;
}
pub fn roleById(a: A, txn: anytype, group: u64, id: u64) !?records.Role {
    const resource = (try storage.getById(a, txn, group, .role, id)) orelse return null;
    defer resource.deinit();
    const role = try std.json.parseFromSliceLeaky(records.Role, a, resource.value.source_json, .{ .allocate = .alloc_always });
    try role.validate();
    if (role.id != id or !std.mem.eql(u8, role.name, resource.value.name) or resource.value.parent_id != 0) return error.InvalidCatalogRecord;
    return role;
}
pub fn roleByPrincipal(a: A, txn: anytype, group: u64, principal: Principal) !?records.Role {
    try principal.validate();
    if (principal.kind != .role) return error.InvalidCatalogMutation;
    const key = try roleOriginKey(a, group, principal.instance);
    defer a.free(key);
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != 8) return error.InvalidCatalogRecord;
    const role = (try roleById(a, txn, group, std.mem.readInt(u64, raw[0..8], .little))) orelse return error.InvalidCatalogRecord;
    if (!role.principal().eql(principal)) return error.InvalidCatalogRecord;
    return role;
}
pub fn read(a: A, txn: anytype, group: u64, scope: Scope) !?Record {
    const key = try objectKey(a, group, scope);
    defer a.free(key);
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const record = try std.json.parseFromSliceLeaky(Record, a, raw, .{ .allocate = .alloc_always });
    record.validate(a) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogRecord;
    if (!record.scope.eql(scope)) return error.InvalidCatalogRecord;
    return record;
}
const SubjectSet = std.AutoHashMapUnmanaged(Principal, void);
const Members = std.AutoHashMapUnmanaged(Principal, Member);
fn subjects(a: A, record: Record) !SubjectSet {
    var result: SubjectSet = .empty;
    errdefer result.deinit(a);
    try result.put(a, record.owner, {});
    for (record.grants) |grant| {
        try result.put(a, grant.grantor, {});
        try result.put(a, grant.grantee, {});
    }
    return result;
}
fn members(a: A, record: Record, role: records.Role) !Members {
    var result: Members = .empty;
    errdefer result.deinit(a);
    for (record.grants) |grant| {
        const slot = try result.getOrPut(a, grant.grantee);
        if (!slot.found_existing) slot.value_ptr.* = .{ .role_id = role.id, .instance = role.instance, .inherit = false, .set = false, .admin = false };
        slot.value_ptr.inherit = slot.value_ptr.inherit or grant.inherit;
        slot.value_ptr.set = slot.value_ptr.set or grant.set;
        slot.value_ptr.admin = slot.value_ptr.admin or grant.option;
    }
    return result;
}
fn removeIndexes(a: A, txn: anytype, group: u64, record: Record) !void {
    var principals = try subjects(a, record);
    defer principals.deinit(a);
    var iterator = principals.keyIterator();
    while (iterator.next()) |principal| {
        if (principal.kind != .public and principal.kind != .bootstrap) {
            const key = try referenceKey(a, group, principal.*, record.scope);
            defer a.free(key);
            try eraseKey(txn, key);
        }
        if (record.scope == .role) {
            const membership = try memberKey(a, group, principal.*, record.scope.role);
            defer a.free(membership);
            try eraseKey(txn, membership);
        }
    }
}
/// Change only affected membership/dependency rows. A one-grantee edit does
/// not tombstone and rebuild every reverse edge of a large existing ACL.
pub fn write(backing: A, txn: anytype, group: u64, record: Record) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    try record.validate(a);
    const previous = try read(a, txn, group, record.scope);
    try updateIndexes(a, txn, group, previous, record);
    const encoded = try std.json.Stringify.valueAlloc(a, record, .{});
    const key = try objectKey(a, group, record.scope);
    try txn.put(key, encoded);
}
/// Recreate derived rows from authoritative records without modifying the
/// authoritative catalog or relying on the previous derived index contents.
pub fn rebuildIndexes(backing: A, txn: anytype, group: u64, record: Record) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    try record.validate(a);
    try updateIndexes(a, txn, group, null, record);
}
fn updateIndexes(a: A, txn: anytype, group: u64, previous: ?Record, record: Record) !void {
    var old_subjects: SubjectSet = if (previous) |old| try subjects(a, old) else .empty;
    defer old_subjects.deinit(a);
    var new_subjects = try subjects(a, record);
    defer new_subjects.deinit(a);
    const scope = try std.json.Stringify.valueAlloc(a, record.scope, .{});
    defer a.free(scope);
    var removed = old_subjects.keyIterator();
    while (removed.next()) |principal| {
        if (new_subjects.contains(principal.*) or principal.kind == .public or principal.kind == .bootstrap) continue;
        const reference = try referenceKey(a, group, principal.*, record.scope);
        defer a.free(reference);
        try eraseKey(txn, reference);
    }
    var added = new_subjects.keyIterator();
    while (added.next()) |principal| {
        if (old_subjects.contains(principal.*) or principal.kind == .public or principal.kind == .bootstrap) continue;
        const reference = try referenceKey(a, group, principal.*, record.scope);
        defer a.free(reference);
        try txn.put(reference, scope);
    }
    if (record.scope == .role) {
        const role = (try roleById(a, txn, group, record.scope.role)) orelse return error.InvalidCatalogRecord;
        var before: Members = if (previous) |old| try members(a, old, role) else .empty;
        defer before.deinit(a);
        var after = try members(a, record, role);
        defer after.deinit(a);
        var old_rows = before.keyIterator();
        while (old_rows.next()) |principal| {
            if (after.contains(principal.*)) continue;
            const membership = try memberKey(a, group, principal.*, role.id);
            defer a.free(membership);
            try eraseKey(txn, membership);
        }
        var new_rows = after.iterator();
        while (new_rows.next()) |row| {
            if (before.get(row.key_ptr.*)) |old| if (std.meta.eql(old, row.value_ptr.*)) continue;
            const membership = try memberKey(a, group, row.key_ptr.*, role.id);
            defer a.free(membership);
            const raw = try std.json.Stringify.valueAlloc(a, row.value_ptr.*, .{});
            defer a.free(raw);
            try txn.put(membership, raw);
        }
    }
}
pub fn erase(backing: A, txn: anytype, group: u64, scope: Scope) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    if (try read(a, txn, group, scope)) |previous| try removeIndexes(a, txn, group, previous);
    const key = try objectKey(a, group, scope);
    defer a.free(key);
    try eraseKey(txn, key);
}
/// Capture defaults in the same native transaction that publishes the new
/// table. Subsequent ALTER DEFAULT PRIVILEGES cannot change existing ACLs.
pub fn initializeTable(a: A, txn: anytype, group: u64, table: local.system_catalog_domain.Resource) !void {
    const owner = table.sql_owner orelse return;
    try owner.validate();
    if (table.kind != .table or owner.kind == .public) return error.InvalidCatalogMutation;
    if (owner.kind == .role and try roleByPrincipal(a, txn, group, owner) == null) return error.SqlRoleNotFound;
    if (try read(a, txn, group, .{ .table = table.id }) != null) return error.InvalidCatalogRecord;
    const global = try read(a, txn, group, .{ .defaults = .{ .owner = owner } });
    const scoped = try read(a, txn, group, .{ .defaults = .{ .owner = owner, .namespace_id = table.parent_id } });
    try write(a, txn, group, try records.tableDefaults(a, table.id, table.parent_id, owner, global, scoped));
}
/// Namespace defaults are contiguous by namespace ID. Retire them with a
/// bounded temporary region per record, without loading unrelated ACLs.
pub fn eraseNamespace(backing: A, txn: anytype, group: u64, namespace: u64) !void {
    const representative = try objectKey(backing, group, .{ .defaults = .{ .owner = .bootstrap, .namespace_id = namespace } });
    defer backing.free(representative);
    const prefix = representative[0 .. representative.len - 17];
    var cursor = try txn.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(prefix);
    while (row) |entry| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        const record = try std.json.parseFromSliceLeaky(Record, a, entry.value, .{ .allocate = .alloc_always });
        try record.validate(a);
        const canonical = try objectKey(a, group, record.scope);
        if (!std.mem.eql(u8, canonical, entry.key)) return error.InvalidCatalogRecord;
        if (record.scope != .defaults or record.scope.defaults.namespace_id != namespace) return error.InvalidCatalogRecord;
        try erase(a, txn, group, record.scope);
    }
}
pub fn loadAll(a: A, txn: anytype, group: u64) ![]Record {
    const prefix = try objectPrefix(a, group);
    defer a.free(prefix);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var result: std.ArrayList(Record) = .empty;
    errdefer result.deinit(a);
    var entry = try cursor.seekAtOrAfter(prefix);
    while (entry) |row| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, row.key, prefix)) break;
        const record = try std.json.parseFromSliceLeaky(Record, a, row.value, .{ .allocate = .alloc_always });
        try record.validate(a);
        const canonical = try objectKey(a, group, record.scope);
        defer a.free(canonical);
        if (!std.mem.eql(u8, canonical, row.key)) return error.InvalidCatalogRecord;
        try result.append(a, record);
    }
    return result.toOwnedSlice(a);
}
/// Native metadata readers walk only memberships reachable from this principal.
/// Unrelated role definitions and table ACLs are never enumerated on reads.
pub fn closure(a: A, txn: anytype, group: u64, principal: Principal, mode: records.Closure) ![]records.Role {
    var seen = std.AutoHashMapUnmanaged([16]u8, void).empty;
    defer seen.deinit(a);
    var queue: std.ArrayList(Principal) = .empty;
    defer queue.deinit(a);
    try queue.append(a, principal);
    if (principal.kind == .role) try seen.put(a, principal.instance, {});
    var result: std.ArrayList(records.Role) = .empty;
    errdefer result.deinit(a);
    var position: usize = 0;
    while (position < queue.items.len) : (position += 1) {
        const prefix = try principalPrefix(a, group, "member", queue.items[position]);
        defer a.free(prefix);
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(prefix);
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, prefix)) break;
            const member = try std.json.parseFromSliceLeaky(Member, a, row.value, .{});
            if (mode == .inherit and !member.inherit or mode == .set and !member.set) continue;
            const current = (try roleById(a, txn, group, member.role_id)) orelse return error.InvalidCatalogRecord;
            if (!std.mem.eql(u8, &current.instance, &member.instance)) return error.InvalidCatalogRecord;
            if ((try seen.getOrPut(a, current.instance)).found_existing) continue;
            if (result.items.len == records.max_principals) return error.SqlProgramLimitExceeded;
            try result.append(a, current);
            try queue.append(a, current.principal());
        }
    }
    return result.toOwnedSlice(a);
}
pub fn references(a: A, txn: anytype, group: u64, principal: Principal) ![]Scope {
    const prefix = try principalPrefix(a, group, "reference", principal);
    defer a.free(prefix);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var result: std.ArrayList(Scope) = .empty;
    errdefer result.deinit(a);
    var entry = try cursor.seekAtOrAfter(prefix);
    while (entry) |row| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, row.key, prefix)) break;
        if (result.items.len == 4096) return error.SqlProgramLimitExceeded;
        const scope = try std.json.parseFromSliceLeaky(Scope, a, row.value, .{});
        try scope.validate();
        try result.append(a, scope);
    }
    return result.toOwnedSlice(a);
}

fn dependencyKey(a: A, group: u64, principal: Principal, kind: []const u8, id: u64, generation: u64) ![]u8 {
    const prefix = try principalPrefix(a, group, "dependency", principal);
    defer a.free(prefix);
    return std.fmt.allocPrint(a, "{s}{s}:{d}:{d}", .{ prefix, kind, id, generation });
}
fn dependency(a: A, txn: anytype, group: u64, principal: Principal, kind: []const u8, id: u64, generation: u64, adding: bool) !void {
    const key = try dependencyKey(a, group, principal, kind, id, generation);
    defer a.free(key);
    if (adding) try txn.put(key, "") else try eraseKey(txn, key);
}
pub fn resourceDependency(a: A, txn: anytype, group: u64, resource: local.system_catalog_domain.Resource, adding: bool) !void {
    const owner = (try records.resourceOwner(a, resource)) orelse return;
    if (owner.kind != .role) return;
    try dependency(a, txn, group, owner, @tagName(resource.kind), resource.id, 0, adding);
}
pub fn policyDependency(a: A, txn: anytype, group: u64, record: local.system_catalog_policies.Record, adding: bool) !void {
    for (record.roles) |role| if (try records.aliasPrincipal(role)) |principal| try dependency(a, txn, group, principal, "policy", record.id, 0, adding);
}
pub fn policyInstallDependency(a: A, txn: anytype, group: u64, snapshot: local.system_catalog_policies.InstallSnapshot, adding: bool) !void {
    for (snapshot.records) |record| for (record.roles) |role| if (try records.aliasPrincipal(role)) |principal| try dependency(a, txn, group, principal, "policy-install", snapshot.table_id, snapshot.policy_generation, adding);
}
pub fn hasDependencies(a: A, txn: anytype, group: u64, principal: Principal) !bool {
    const prefix = try principalPrefix(a, group, "dependency", principal);
    defer a.free(prefix);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const found = (try cursor.seekAtOrAfter(prefix)) orelse return false;
    return std.mem.startsWith(u8, found.key, prefix);
}
