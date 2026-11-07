// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! First TEMP namespace admission pins its actual metadata backend. Neither a
//! connection token nor an API process epoch is a backend registration.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const namespaces = @import("temporary_namespace_index.zig");
const authority = @import("lock_authority.zig");
const scopes = authority.scopes;
const A = std.mem.Allocator;
const max_bytes = 4096;

pub const Binding = struct {
    namespace: namespaces.Reference,
    group: u64,
    backend: authority.locks.Backend,
    registration_nonce: [16]u8,
    source_principal: [32]u8,
    standalone: bool,
    creation_command: scopes.Command,

    pub fn validate(self: Binding) !void {
        try self.namespace.validate();
        try self.backend.validate();
        try self.creation_command.validate();
        if (self.group == 0 or std.mem.allEqual(u8, &self.registration_nonce, 0) or
            std.mem.allEqual(u8, &self.source_principal, 0) or
            !std.mem.eql(u8, &self.creation_command.executor, &self.namespace.owner.execution_instance) or
            ((self.namespace.owner.issuer_node_id == 0) != self.standalone)) return error.InvalidSqlTemporaryNamespace;
    }
    pub fn requireNamespace(self: Binding, value: namespaces.Reference) !void {
        try self.validate();
        if (!std.meta.eql(self.namespace, value)) return error.InvalidSqlTemporaryNamespace;
    }
    /// Lease renewals preserve this authority. Reincarnated registrations and
    /// commands from a successor API process cannot be closed by old cleanup.
    pub fn shouldFence(self: Binding, source: scopes.Record) !bool {
        try self.validate();
        const ns: scopes.ledger.Namespace = .{ .home = .metadata, .group = self.group, .incarnation = self.backend.incarnation };
        try source.validate(ns);
        const registration = source.registration;
        if (registration.group != self.group or !registration.lease.backend.eql(self.backend) or
            !std.mem.eql(u8, &registration.nonce, &self.registration_nonce) or
            !std.mem.eql(u8, &registration.connection, &self.namespace.owner.connection_id) or
            registration.lease.node != self.namespace.owner.issuer_node_id or
            registration.lease.database != self.namespace.database_id or
            !std.mem.eql(u8, &registration.lease.principal, &self.source_principal) or
            registration.standalone != self.standalone) return error.InvalidSqlTemporaryNamespace;
        if (source.state != .active) return false;
        const latest = source.command orelse source.completed orelse return error.InvalidSqlTemporaryNamespace;
        if (latest.sequence < self.creation_command.sequence) return error.InvalidSqlTemporaryNamespace;
        return std.mem.eql(u8, &latest.executor, &self.namespace.owner.execution_instance);
    }
};
fn namespaceKey(buffer: []u8, group: u64, namespace: u64) ![]const u8 {
    if (group == 0 or namespace == 0) return error.InvalidSqlTemporaryNamespace;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:temporary_source:{d}:namespace:{x:0>16}", .{ group, namespace });
}
fn backendKey(buffer: []u8, group: u64, backend: authority.locks.Backend) ![]const u8 {
    try backend.validate();
    if (group == 0) return error.InvalidSqlTemporaryNamespace;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:temporary_source:{d}:backend:{s}:{s}:{x:0>16}", .{
        group, std.fmt.bytesToHex(backend.incarnation, .lower), std.fmt.bytesToHex(backend.id, .lower), backend.fence,
    });
}
fn optional(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| return if (err == error.NotFound) null else err;
}
pub fn read(a: A, reader: anytype, group: u64, namespace: u64) !?Binding {
    var buffer: [256]u8 = undefined;
    const bytes = (try optional(reader, try namespaceKey(&buffer, group, namespace))) orelse return null;
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidSqlTemporaryNamespace;
    var parsed = std.json.parseFromSlice(Binding, a, bytes, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSqlTemporaryNamespace;
    defer parsed.deinit();
    const value = parsed.value;
    try value.validate();
    if (value.group != group or value.namespace.namespace_id != namespace) return error.InvalidSqlTemporaryNamespace;
    const encoded = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(encoded);
    if (!std.mem.eql(u8, bytes, encoded)) return error.InvalidSqlTemporaryNamespace;
    var inverse: [256]u8 = undefined;
    const stored = (try optional(reader, try backendKey(&inverse, group, value.backend))) orelse return error.InvalidSqlTemporaryNamespace;
    if (stored.len != 8 or std.mem.readInt(u64, stored[0..8], .little) != namespace) return error.InvalidSqlTemporaryNamespace;
    return value;
}
/// Prepare before the first catalog effect. The accepting writer checks the
/// real command fence independently; this guard rejoins its exact source.
pub fn prepare(a: A, txn: anytype, group: u64, resource: domain.Resource, fence: scopes.CommandFence) !Binding {
    const namespace = (namespaces.reference(resource) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogMutation) orelse return error.InvalidCatalogMutation;
    if (fence.group != group or fence.database != namespace.database_id or
        !std.mem.eql(u8, &fence.command.executor, &namespace.owner.execution_instance)) return error.InvalidCatalogMutation;
    const ns: scopes.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = fence.backend.incarnation };
    const source = scopes.guard(a, txn, ns, fence.backend, fence.command, fence) catch |err| return authority.semanticError(err);
    if (source.registration.lease.node != namespace.owner.issuer_node_id or
        !std.mem.eql(u8, &source.registration.connection, &namespace.owner.connection_id)) return error.InvalidCatalogMutation;
    const value: Binding = .{
        .namespace = namespace,
        .group = group,
        .backend = fence.backend,
        .registration_nonce = source.registration.nonce,
        .source_principal = source.registration.lease.principal,
        .standalone = source.registration.standalone,
        .creation_command = fence.command,
    };
    if (!try value.shouldFence(source)) return error.InvalidSqlTemporaryNamespace;
    if (try read(a, txn, group, namespace.namespace_id)) |before| {
        if (!std.meta.eql(before, value)) return error.InvalidSqlTemporaryNamespace;
    }
    var inverse: [256]u8 = undefined;
    if (try optional(txn, try backendKey(&inverse, group, value.backend))) |existing| {
        if (existing.len != 8) return error.InvalidSqlTemporaryNamespace;
        // One registration has one immutable database and at most one live
        // TEMP namespace. DISCARD must retire it before admitting a new Owner.
        if (std.mem.readInt(u64, existing[0..8], .little) != namespace.namespace_id) return error.CatalogPublicationProofPending;
    }
    return value;
}
pub fn stage(a: A, txn: anytype, value: Binding) !void {
    try value.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > max_bytes) return error.InvalidSqlTemporaryNamespace;
    var key: [256]u8 = undefined;
    try txn.put(try namespaceKey(&key, value.group, value.namespace.namespace_id), bytes);
    var namespace: [8]u8 = undefined;
    std.mem.writeInt(u64, &namespace, value.namespace.namespace_id, .little);
    try txn.put(try backendKey(&key, value.group, value.backend), &namespace);
}
/// Called only after the existing scope receiver has verified its canonical
/// namespace/Owner checkpoint, in the SAME metadata transaction as that step.
/// Missing old source state authorizes no lookup or fence of another backend.
pub fn fenceOwner(a: A, txn: anytype, group: u64, incarnation: [16]u8, namespace: namespaces.Reference) !void {
    const binding = (try read(a, txn, group, namespace.namespace_id)) orelse return error.InvalidSqlTemporaryNamespace;
    try binding.requireNamespace(namespace);
    if (!std.mem.eql(u8, &binding.backend.incarnation, &incarnation)) return error.InvalidScopeRetirement;
    const ns: scopes.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = incarnation };
    const source = (try scopes.read(a, txn, ns, binding.backend)) orelse return;
    if (!try binding.shouldFence(source)) return;
    const context: authority.Context = .{ .a = a, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = source.registration.lease.database, .standalone = source.registration.standalone, .maintenance = true };
    try scopes.fence(a, txn, ns, binding.backend, .close, context);
}

pub fn remove(a: A, txn: anytype, group: u64, resource: domain.Resource) !void {
    const namespace = (try namespaces.reference(resource)) orelse return;
    const value = (try read(a, txn, group, namespace.namespace_id)) orelse return error.InvalidSqlTemporaryNamespace;
    try value.requireNamespace(namespace);
    var key: [256]u8 = undefined;
    try txn.delete(try backendKey(&key, group, value.backend));
    try txn.delete(try namespaceKey(&key, group, namespace.namespace_id));
}
pub fn requireResource(a: A, reader: anytype, group: u64, resource: domain.Resource) !void {
    const namespace = (try namespaces.reference(resource)) orelse return;
    const value = (try read(a, reader, group, namespace.namespace_id)) orelse return error.InvalidSqlTemporaryNamespace;
    try value.requireNamespace(namespace);
}
/// Stream both indexes, rejoining actual namespaces. The original source may
/// already be GC'd; its exact binding lives until ordinary namespace removal.
pub fn validate(a: A, reader: anytype, group: u64, view: anytype) !void {
    var buffer: [256]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:temporary_source:{d}:", .{group});
    var cursor = try reader.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(prefix);
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, prefix)) break;
        if (item.key.len > 256 or item.value.len > max_bytes) return error.InvalidSqlTemporaryNamespace;
        var key_copy: [256]u8 = undefined;
        @memcpy(key_copy[0..item.key.len], item.key);
        const key = key_copy[0..item.key.len];
        const rest = key[prefix.len..];
        const namespace = if (std.mem.startsWith(u8, rest, "namespace:")) blk: {
            if (rest.len != "namespace:".len + 16) return error.InvalidSqlTemporaryNamespace;
            break :blk std.fmt.parseInt(u64, rest["namespace:".len..], 16) catch return error.InvalidSqlTemporaryNamespace;
        } else if (std.mem.startsWith(u8, rest, "backend:")) blk: {
            if (item.value.len != 8) return error.InvalidSqlTemporaryNamespace;
            break :blk std.mem.readInt(u64, item.value[0..8], .little);
        } else return error.InvalidSqlTemporaryNamespace;
        const value = (try read(a, reader, group, namespace)) orelse return error.InvalidSqlTemporaryNamespace;
        var expected: [256]u8 = undefined;
        const canonical = if (std.mem.startsWith(u8, rest, "namespace:")) try namespaceKey(&expected, group, namespace) else try backendKey(&expected, group, value.backend);
        if (!std.mem.eql(u8, key, canonical)) return error.InvalidSqlTemporaryNamespace;
        try view.requireNamespace(value.namespace);
    }
}

/// A metadata cut of the admitted backend, never a missing API-local file or
/// an expiry guess. It is archived by the existing namespace retirement job.
pub const Abandonment = struct {
    binding: Binding,
    source: ?scopes.Record,

    pub fn eligible(self: Abandonment) !bool {
        try self.binding.validate();
        const source = self.source orelse return true;
        // Validate the exact original registration before considering state or
        // executor. A recycled connection/backend cannot lend this authority.
        const still_owned = try self.binding.shouldFence(source);
        return source.state != .active or !still_owned;
    }
    pub fn digest(self: Abandonment) ![32]u8 {
        if (!try self.eligible()) return error.ScopeRetirementChanged;
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
        try writer.writer.writeAll("antfly/abandoned-temporary-source/v1\x00");
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var result: [32]u8 = undefined;
        writer.hasher.final(&result);
        return result;
    }
    pub fn guard(self: Abandonment) !AbandonmentGuard {
        return .{ .namespace = self.binding.namespace, .digest = try self.digest() };
    }
};
pub const AbandonmentGuard = struct {
    namespace: namespaces.Reference,
    digest: [32]u8,
    pub fn validate(self: AbandonmentGuard) !void {
        try self.namespace.validate();
        if (std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidScopeRetirement;
    }
};

pub fn captureAbandoned(a: A, reader: anytype, group: u64, namespace: u64) !?Abandonment {
    const binding = (try read(a, reader, group, namespace)) orelse return error.InvalidSqlTemporaryNamespace;
    const ns: scopes.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = binding.backend.incarnation };
    const captured: Abandonment = .{ .binding = binding, .source = try scopes.read(a, reader, ns, binding.backend) };
    return if (try captured.eligible()) captured else null;
}
pub fn requireAbandoned(a: A, reader: anytype, group: u64, guard: AbandonmentGuard) !Abandonment {
    try guard.validate();
    const captured = (try captureAbandoned(a, reader, group, guard.namespace.namespace_id)) orelse return error.ScopeRetirementChanged;
    try captured.binding.requireNamespace(guard.namespace);
    if (!std.mem.eql(u8, &try captured.digest(), &guard.digest)) return error.ScopeRetirementChanged;
    return captured;
}

/// One keyset step over TEMP namespace bindings, not a table/catalog scan.
/// The returned POD survives subsequent source/catalog point reads.
pub fn nextNamespace(a: A, reader: anytype, group: u64, after: ?u64) !?Binding {
    if (group == 0) return error.InvalidSqlTemporaryNamespace;
    var prefix_buffer: [128]u8 = undefined;
    const selected_prefix = try std.fmt.bufPrint(&prefix_buffer, "\x00\x00__metadata__:temporary_source:{d}:namespace:", .{group});
    var seek_buffer: [256]u8 = undefined;
    const seek = if (after) |id| try namespaceKey(&seek_buffer, group, id) else selected_prefix;
    var cursor = try reader.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(seek);
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, selected_prefix)) return null;
        if (item.key.len != selected_prefix.len + 16) return error.InvalidSqlTemporaryNamespace;
        const id = std.fmt.parseInt(u64, item.key[selected_prefix.len..], 16) catch return error.InvalidSqlTemporaryNamespace;
        if (after != null and id <= after.?) continue;
        return (try read(a, reader, group, id)) orelse error.InvalidSqlTemporaryNamespace;
    }
    return null;
}

const Testing = if (@import("builtin").is_test) struct {
    const Entry = struct { key: []u8, value: []u8 };
    const Memory = struct {
        a: A,
        rows: std.ArrayList(Entry) = .empty,
        fn deinit(self: *@This()) void {
            for (self.rows.items) |item| {
                self.a.free(item.key);
                self.a.free(item.value);
            }
            self.rows.deinit(self.a);
        }
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            for (self.rows.items) |item| if (std.mem.eql(u8, item.key, key)) return item.value;
            return error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const copy = try self.a.dupe(u8, value);
            errdefer self.a.free(copy);
            for (self.rows.items) |*item| if (std.mem.eql(u8, item.key, key)) {
                self.a.free(item.value);
                item.value = copy;
                return;
            };
            const key_copy = try self.a.dupe(u8, key);
            errdefer self.a.free(key_copy);
            try self.rows.append(self.a, .{ .key = key_copy, .value = copy });
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            for (self.rows.items, 0..) |item, i| if (std.mem.eql(u8, item.key, key)) {
                self.a.free(item.key);
                self.a.free(item.value);
                _ = self.rows.swapRemove(i);
                return;
            };
            return error.NotFound;
        }
        const Cursor = struct {
            memory: *Memory,
            after: ?[]const u8 = null,
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?Entry {
                self.after = null;
                return self.find(key, true);
            }
            pub fn next(self: *@This()) !?Entry {
                return self.find(self.after orelse return null, false);
            }
            fn find(self: *@This(), key: []const u8, inclusive: bool) ?Entry {
                var found: ?Entry = null;
                for (self.memory.rows.items) |item| {
                    const cmp = std.mem.order(u8, item.key, key);
                    if (cmp == .lt or (!inclusive and cmp == .eq)) continue;
                    if (found == null or std.mem.order(u8, item.key, found.?.key) == .lt) found = item;
                }
                if (found) |value| self.after = value.key;
                return found;
            }
        };
        pub fn openCursor(self: *@This()) !Cursor {
            return .{ .memory = self };
        }
    };
    const group: u64 = 2188;
    const ns: scopes.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = @splat(1) };
    const owner: local.common_topology_records.RelationPersistence.Owner = .{
        .issuer_node_id = 7,
        .connection_id = @splat('a'),
        .wire_epoch = @splat(8),
        .execution_instance = @splat(6),
        .connection_generation = 1,
        .principal_digest = @splat(9),
    };
    const registration: scopes.Registration = .{ .group = group, .connection = owner.connection_id, .nonce = @splat(2), .standalone = false, .lease = .{
        .backend = .{ .incarnation = ns.incarnation, .id = @splat(3), .fence = 1 },
        .database = 1,
        .node = owner.issuer_node_id,
        .principal = @splat(4),
        .expires_at_ms = 1000,
    } };
    const command: scopes.Command = .{ .sequence = 1, .nonce = @splat(5), .executor = owner.execution_instance };
    const fence: scopes.CommandFence = .{ .group = group, .backend = registration.lease.backend, .command = command, .database = 1, .principal = registration.lease.principal, .standalone = false };
    const Registrar = struct {
        pub fn requireRegistration(_: @This(), _: anytype, _: scopes.ledger.Namespace, _: scopes.Registration) !void {}
    };
    fn resource(buffer: *[72]u8, value: @TypeOf(owner), id: u64) !domain.Resource {
        return .{ .kind = .namespace, .id = id, .parent_id = 1, .name = try value.namespace(buffer), .sql_temporary_owner = value };
    }
    fn start(memory: *Memory) !void {
        try scopes.register(memory.a, memory, ns, registration, Registrar{});
        _ = try scopes.begin(memory.a, memory, ns, registration.lease.backend, command, fence);
    }
    const View = struct {
        expected: namespaces.Reference,
        pub fn requireNamespace(self: @This(), actual: namespaces.Reference) !void {
            if (!std.meta.eql(self.expected, actual)) return error.InvalidSqlTemporaryNamespace;
        }
    };
} else struct {};

test "SQL relation lifetime TEMP source binding preserves exact registration and all Owner fields" {
    const T = Testing;
    const a = std.testing.allocator;
    var memory: T.Memory = .{ .a = a };
    defer memory.deinit();
    try T.start(&memory);
    var name: [72]u8 = undefined;
    const resource = try T.resource(&name, T.owner, 22);
    const value = try prepare(a, &memory, T.group, resource, T.fence);
    try stage(a, &memory, value);
    const cold = (try read(a, &memory, T.group, 22)).?;
    try std.testing.expectEqualDeep(value, cold);
    const source = (try scopes.read(a, &memory, T.ns, T.registration.lease.backend)).?;
    try std.testing.expect(try cold.shouldFence(source));
    var renewed = source;
    renewed.registration.lease.expires_at_ms += 100;
    try std.testing.expect(try cold.shouldFence(renewed));
    var successor = source;
    successor.command.?.executor = @splat(30);
    try std.testing.expect(!try cold.shouldFence(successor));
    var completed = source;
    completed.completed = completed.command;
    completed.command = null;
    completed.next_command += 1;
    try std.testing.expect(try cold.shouldFence(completed));
    completed.completed.?.executor = @splat(30);
    try std.testing.expect(!try cold.shouldFence(completed));
    inline for (.{ "nonce", "connection" }) |field| {
        var forged = source;
        if (comptime std.mem.eql(u8, field, "connection")) forged.registration.connection[0] = 'b' else forged.registration.nonce[0] ^= 1;
        try std.testing.expectError(error.InvalidSqlTemporaryNamespace, cold.shouldFence(forged));
    }
    var forged = source;
    forged.registration.lease.principal[0] ^= 1;
    try std.testing.expectError(error.InvalidSqlTemporaryNamespace, cold.shouldFence(forged));
    forged = source;
    forged.registration.lease.backend.id[0] ^= 1;
    try std.testing.expectError(error.InvalidSqlTemporaryNamespace, cold.shouldFence(forged));
    inline for (.{ "wire_epoch", "execution_instance", "principal_digest", "connection_id" }) |field| {
        var other = cold.namespace;
        @field(other.owner, field)[0] ^= 1;
        try std.testing.expectError(error.InvalidSqlTemporaryNamespace, cold.requireNamespace(other));
    }
    var other = cold.namespace;
    other.owner.connection_generation += 1;
    try std.testing.expectError(error.InvalidSqlTemporaryNamespace, cold.requireNamespace(other));
    other = cold.namespace;
    other.owner.issuer_node_id += 1;
    try std.testing.expectError(error.InvalidSqlTemporaryNamespace, cold.requireNamespace(other));
}

test "SQL relation lifetime TEMP source binding rejects successor replacement and cold index corruption" {
    const T = Testing;
    const a = std.testing.allocator;
    var memory: T.Memory = .{ .a = a };
    defer memory.deinit();
    try T.start(&memory);
    var name: [72]u8 = undefined;
    const resource = try T.resource(&name, T.owner, 22);
    const value = try prepare(a, &memory, T.group, resource, T.fence);
    try stage(a, &memory, value);
    try requireResource(a, &memory, T.group, resource);
    try validate(a, &memory, T.group, T.View{ .expected = value.namespace });
    var successor = T.owner;
    successor.connection_generation += 1;
    var successor_name: [72]u8 = undefined;
    const next = try T.resource(&successor_name, successor, 23);
    try std.testing.expectError(error.CatalogPublicationProofPending, prepare(a, &memory, T.group, next, T.fence));
    var key: [256]u8 = undefined;
    var bad_id: [8]u8 = undefined;
    std.mem.writeInt(u64, &bad_id, 23, .little);
    try memory.put(try backendKey(&key, T.group, value.backend), &bad_id);
    try std.testing.expectError(error.InvalidSqlTemporaryNamespace, read(a, &memory, T.group, 22));
    try stage(a, &memory, value);
    try remove(a, &memory, T.group, resource);
    try std.testing.expectEqual(@as(?Binding, null), try read(a, &memory, T.group, 22));
    // After exact namespace removal the same registration can admit DISCARD's
    // next generation; no durable alias or stale backend marker remains.
    const replacement = try prepare(a, &memory, T.group, next, T.fence);
    try stage(a, &memory, replacement);
    try validate(a, &memory, T.group, T.View{ .expected = replacement.namespace });
    try std.testing.expectEqualDeep(replacement, (try read(a, &memory, T.group, 23)).?);
}

test "SQL relation lifetime TEMP source cleanup fences only its admitted backend across reincarnation" {
    const T = Testing;
    const a = std.testing.allocator;
    var memory: T.Memory = .{ .a = a };
    defer memory.deinit();
    try T.start(&memory);
    var name: [72]u8 = undefined;
    const resource = try T.resource(&name, T.owner, 22);
    const binding = try prepare(a, &memory, T.group, resource, T.fence);
    try stage(a, &memory, binding);
    var wrong_owner = binding.namespace;
    wrong_owner.owner.wire_epoch[0] ^= 1;
    try std.testing.expectError(error.InvalidSqlTemporaryNamespace, fenceOwner(a, &memory, T.group, T.ns.incarnation, wrong_owner));
    try std.testing.expectEqual(.active, (try scopes.read(a, &memory, T.ns, binding.backend)).?.state);
    try fenceOwner(a, &memory, T.group, T.ns.incarnation, binding.namespace);
    try std.testing.expectEqual(.fenced, (try scopes.read(a, &memory, T.ns, binding.backend)).?.state);
    // Exact reply-loss replay has no further effects.
    try fenceOwner(a, &memory, T.group, T.ns.incarnation, binding.namespace);
    // Model completed native source GC, retaining the catalog namespace and
    // its immutable binding. The same public connection ID is then recycled.
    var i: usize = 0;
    while (i < memory.rows.items.len) {
        const item = memory.rows.items[i];
        if (std.mem.startsWith(u8, item.key, "\x00\x00__metadata__:temporary_source:")) {
            i += 1;
            continue;
        }
        memory.a.free(item.key);
        memory.a.free(item.value);
        _ = memory.rows.swapRemove(i);
    }
    var successor = T.registration;
    successor.lease.backend.id = @splat(41);
    successor.nonce = @splat(42);
    try scopes.register(a, &memory, T.ns, successor, T.Registrar{});
    var successor_command = T.command;
    successor_command.executor = @splat(43);
    var successor_fence = T.fence;
    successor_fence.backend = successor.lease.backend;
    successor_fence.command = successor_command;
    _ = try scopes.begin(a, &memory, T.ns, successor.lease.backend, successor_command, successor_fence);
    try fenceOwner(a, &memory, T.group, T.ns.incarnation, binding.namespace);
    const after = (try scopes.read(a, &memory, T.ns, successor.lease.backend)).?;
    try std.testing.expectEqual(.active, after.state);
    try std.testing.expectEqualDeep(successor_command, after.command.?);
    try std.testing.expect((try scopes.read(a, &memory, T.ns, binding.backend)) == null);
}

test "SQL relation lifetime abandoned TEMP discovery uses exact completed source and never fences successor" {
    const T = Testing;
    const a = std.testing.allocator;
    var memory: T.Memory = .{ .a = a };
    defer memory.deinit();
    try T.start(&memory);
    var name: [72]u8 = undefined;
    const resource = try T.resource(&name, T.owner, 22);
    const binding = try prepare(a, &memory, T.group, resource, T.fence);
    try stage(a, &memory, binding);
    try std.testing.expectEqualDeep(binding, (try nextNamespace(a, &memory, T.group, null)).?);
    try std.testing.expect(try nextNamespace(a, &memory, T.group, 22) == null);
    try std.testing.expect(try captureAbandoned(a, &memory, T.group, 22) == null);
    try scopes.finish(a, &memory, T.ns, T.registration.lease.backend, T.command, T.fence);
    // New API execution may recover the backend; its current command is never
    // terminated just because the prior process's TEMP ownership ended.
    var next = T.command;
    next.sequence += 1;
    next.nonce = @splat(61);
    next.executor = @splat(62);
    var next_fence = T.fence;
    next_fence.command = next;
    _ = try scopes.begin(a, &memory, T.ns, T.registration.lease.backend, next, next_fence);
    const captured = (try captureAbandoned(a, &memory, T.group, 22)).?;
    const guard = try captured.guard();
    try std.testing.expectEqualDeep(captured, try requireAbandoned(a, &memory, T.group, guard));
    try fenceOwner(a, &memory, T.group, T.ns.incarnation, binding.namespace);
    try std.testing.expectEqual(.active, (try scopes.read(a, &memory, T.ns, binding.backend)).?.state);
    var forged = guard;
    forged.namespace.owner.wire_epoch[0] ^= 1;
    try std.testing.expectError(error.InvalidSqlTemporaryNamespace, requireAbandoned(a, &memory, T.group, forged));
    forged = guard;
    forged.digest[0] ^= 1;
    try std.testing.expectError(error.ScopeRetirementChanged, requireAbandoned(a, &memory, T.group, forged));
    // A changed command cut invalidates the old proof before any scope effect.
    try scopes.finish(a, &memory, T.ns, binding.backend, next, next_fence);
    try std.testing.expectError(error.ScopeRetirementChanged, requireAbandoned(a, &memory, T.group, guard));
    const refreshed = (try captureAbandoned(a, &memory, T.group, 22)).?;
    try std.testing.expect(try refreshed.eligible());
}
