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

//! Ordered notification blocks share the SQL decision writer. Reservations
//! precede the irreversible decision; immutable payloads become deliverable
//! only after the existing native visibility/retirement barrier. These helpers
//! carry no caller authority: apply must join the frozen SQL owner itself.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const types = local.sql_ast.notifications;
const A = std.mem.Allocator;
pub const max_queue_bytes: u64 = 8 * 1024 * 1024;
pub const max_group_queue_bytes: u64 = 64 * 1024 * 1024;
pub const max_channels = 256;
const max_record_bytes = 512 * 1024;
const Channel = struct { name: []const u8, since: u64 };
pub const Scope = struct {
    group: u64,
    database: u64,
    incarnation: [16]u8,
    pub fn validate(self: Scope) !void {
        if (self.group == 0 or self.database == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidCatalogRecord;
    }
    pub fn id(self: Scope) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-notifications/v1\x00");
        var ids: [16]u8 = undefined;
        std.mem.writeInt(u64, ids[0..8], self.group, .little);
        std.mem.writeInt(u64, ids[8..16], self.database, .little);
        hash.update(&ids);
        hash.update(&self.incarnation);
        var out: [32]u8 = undefined;
        hash.final(&out);
        return out;
    }
};
const Gc = struct { cut: u64, minimum: u64, revision: u64, retention_revision: u64, after: ?[32]u8 = null, deleting: bool = false };
const GcMemo = struct { revision: u64, retention_revision: u64 };
pub const Member = struct { scope: [32]u8, connection: [32]u8, generation: u64, acknowledged: types.Cursor };
pub const Head = struct { scope: ?Scope = null, next: u64 = 1, oldest: u64 = 1, used: u64 = 0, reserved: u64 = 0, revision: u64 = 1, retention_revision: u64 = 1, gc: ?Gc = null, gc_memo: ?GcMemo = null };
pub const Listener = struct {
    scope: Scope,
    connection: [32]u8,
    generation: u64,
    channels: []const Channel,
    acknowledged: types.Cursor,
    offered: types.Cursor,
};
pub const Ref = struct { transaction: [16]u8, digest: [32]u8, reserved: u64, committed: bool = false, sequence: ?u64 = null };
pub const Slot = struct { sequence: u64, operation: [16]u8, transaction: [16]u8, digest: [32]u8, checksum: [32]u8, bytes: u64, ready: bool = false, event_count: u16, bloom: [4]u64 };
pub const Payload = struct { sender_pid: i32, events: []const struct { channel: []const u8, payload: []const u8 } };
pub const Page = types.Page;
pub const Binding = struct { backend: local.storage_pessimistic_lock_contract.Backend, connection: [32]u8, generation: u64, sender_pid: i32 };
pub fn prefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_notifications:{x:0>16}:", .{group});
}
fn groupPrefix(a: A, group: u64) ![]u8 {
    var buffer: [128]u8 = undefined;
    return a.dupe(u8, try prefixForGroup(&buffer, group));
}
pub const Budget = struct { used: u64 = 0, reserved: u64 = 0 };
fn budgetKey(a: A, group: u64) ![]u8 {
    return std.mem.concat(a, u8, &.{ try groupPrefix(a, group), "budget" });
}
fn groupBudget(a: A, txn: anytype, group: u64) !Budget {
    const value = (try get(Budget, a, txn, try budgetKey(a, group))) orelse Budget{};
    if (value.used > max_group_queue_bytes or value.reserved > max_group_queue_bytes - value.used) return error.InvalidCatalogRecord;
    return value;
}

fn pidKey(a: A, group: u64, pid: i32) ![]u8 {
    if (pid <= 0) return error.InvalidSqlConnection;
    return std.fmt.allocPrint(a, "{s}pid:{x:0>8}", .{ try groupPrefix(a, group), @as(u32, @intCast(pid)) });
}

fn bindingKey(a: A, group: u64, backend: local.storage_pessimistic_lock_contract.Backend) ![]u8 {
    return std.fmt.allocPrint(a, "{s}binding:{s}:{s}:{x:0>16}", .{ try groupPrefix(a, group), std.fmt.bytesToHex(backend.incarnation, .lower), std.fmt.bytesToHex(backend.id, .lower), backend.fence });
}
pub fn readBinding(a: A, txn: anytype, group: u64, backend: local.storage_pessimistic_lock_contract.Backend) !?Binding {
    return get(Binding, a, txn, try bindingKey(a, group, backend));
}
pub fn bind(a: A, txn: anytype, group: u64, value: Binding) !void {
    try value.backend.validate();
    if (value.generation == 0 or value.sender_pid <= 0) return error.InvalidSqlConnection;
    if (try readBinding(a, txn, group, value.backend)) |prior| {
        if (!std.meta.eql(prior, value)) return error.CatalogGenerationChanged;
        return;
    }
    if (try get(Binding, a, txn, try pidKey(a, group, value.sender_pid))) |_| return error.SqlBackendPidInUse;
    try put(a, txn, try pidKey(a, group, value.sender_pid), value);
    try put(a, txn, try bindingKey(a, group, value.backend), value);
}
/// DISCARD keeps the PID lease but retires subscriptions with its exact
/// generation transition, atomically with native command completion.
pub fn resetBinding(a: A, txn: anytype, group: u64, backend: local.storage_pessimistic_lock_contract.Backend, generation: u64) !void {
    var prior = (try readBinding(a, txn, group, backend)) orelse return error.InvalidSqlConnection;
    if (generation == prior.generation) return;
    if (prior.generation == std.math.maxInt(u64) or generation != prior.generation + 1) return error.CatalogGenerationChanged;
    const pid_owner = (try get(Binding, a, txn, try pidKey(a, group, prior.sender_pid))) orelse return error.InvalidCatalogRecord;
    if (!std.meta.eql(prior, pid_owner)) return error.InvalidCatalogRecord;
    try removeListener(a, txn, group, prior.connection);
    prior.generation = generation;
    try put(a, txn, try pidKey(a, group, prior.sender_pid), prior);
    try put(a, txn, try bindingKey(a, group, backend), prior);
}
pub fn retireBinding(a: A, txn: anytype, group: u64, backend: local.storage_pessimistic_lock_contract.Backend) !void {
    if (try readBinding(a, txn, group, backend)) |prior| {
        const owner = (try get(Binding, a, txn, try pidKey(a, group, prior.sender_pid))) orelse return error.InvalidCatalogRecord;
        if (!std.meta.eql(owner, prior)) return error.InvalidCatalogRecord;
        try erase(txn, try pidKey(a, group, prior.sender_pid));
        try removeListener(a, txn, group, prior.connection);
        try erase(txn, try bindingKey(a, group, backend));
    }
}

pub fn decisionScope(_: A, group: u64, incarnation: [16]u8, identity: anytype) !Scope {
    // Notifications are database-scoped independently of table storage services.
    return .{ .group = group, .database = identity.database_id, .incarnation = incarnation };
}

fn key(a: A, scope: Scope, suffix: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}:{s}", .{ try groupPrefix(a, scope.group), std.fmt.bytesToHex(scope.id(), .lower), suffix });
}
fn referenceKey(a: A, scope: Scope, operation: [16]u8) ![]u8 {
    return key(a, scope, try std.fmt.allocPrint(a, "operation/{s}", .{std.fmt.bytesToHex(operation, .lower)}));
}
fn slotKey(a: A, scope: Scope, sequence: u64, body: bool) ![]u8 {
    return key(a, scope, try std.fmt.allocPrint(a, "slot/{x:0>16}/{s}", .{ sequence, if (body) "body" else "head" }));
}
fn listenerKey(a: A, group: u64, connection: [32]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}listener:{s}", .{ try groupPrefix(a, group), std.fmt.bytesToHex(connection, .lower) });
}
fn workPrefix(a: A, group: u64, incarnation: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}work:{s}:", .{ try groupPrefix(a, group), std.fmt.bytesToHex(incarnation, .lower) });
}
fn workKey(a: A, scope: Scope) ![]u8 {
    return std.mem.concat(a, u8, &.{ try workPrefix(a, scope.group, scope.incarnation), &std.fmt.bytesToHex(scope.id(), .lower) });
}
fn member(a: A, txn: anytype, value: Listener) !void {
    try put(a, txn, try memberKey(a, value.scope, value.connection), Member{ .scope = value.scope.id(), .connection = value.connection, .generation = value.generation, .acknowledged = value.acknowledged });
}
fn needsGc(state: Head) bool {
    if (state.used == 0 and state.reserved == 0) return true;
    const memo = state.gc_memo orelse return true;
    return memo.revision != state.revision or memo.retention_revision != state.retention_revision;
}
pub const Work = struct { scope: ?Scope = null, next_after: ?[32]u8 = null };
/// At most sixteen compact heads, one returned objective, and no row payloads.
pub fn readWork(a: A, txn: anytype, group: u64, incarnation: [16]u8, after: ?[32]u8) !Work {
    const prefix = try workPrefix(a, group, incarnation);
    const lower = if (after) |id| try std.mem.concat(a, u8, &.{ prefix, &std.fmt.bytesToHex(id, .lower), "\x00" }) else prefix;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(lower);
    var result: Work = .{};
    var inspected: usize = 0;
    while (entry) |row| : (inspected += 1) {
        if (!std.mem.startsWith(u8, row.key, prefix)) return .{};
        if (inspected == 16) return result;
        if (row.key.len != prefix.len + 64) return error.InvalidCatalogRecord;
        var id: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, row.key[prefix.len..]) catch return error.InvalidCatalogRecord;
        const scope = (try get(Scope, a, txn, row.key)) orelse return error.InvalidCatalogRecord;
        if (scope.group != group or !std.mem.eql(u8, &scope.incarnation, &incarnation) or !std.mem.eql(u8, &scope.id(), &id)) return error.InvalidCatalogRecord;
        result.next_after = id;
        if (needsGc(try head(a, txn, scope))) {
            result.scope = scope;
            return result;
        }
        entry = try cursor.next();
    }
    return .{};
}

fn memberKey(a: A, scope: Scope, connection: [32]u8) ![]u8 {
    return key(a, scope, try std.fmt.allocPrint(a, "listener/{s}", .{std.fmt.bytesToHex(connection, .lower)}));
}
fn get(comptime T: type, a: A, txn: anytype, address: []const u8) !?T {
    const raw = txn.get(address) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try decodeRecord(T, a, raw);
}
pub fn decodeRecord(comptime T: type, a: A, raw: []const u8) !T {
    if (raw.len <= 32 or raw.len > max_record_bytes) return error.InvalidCatalogRecord;
    const json = raw[0 .. raw.len - 32];
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(json, &checksum, .{});
    if (!std.mem.eql(u8, &checksum, raw[json.len..])) return error.InvalidCatalogRecord;
    return std.json.parseFromSliceLeaky(T, a, json, .{ .allocate = .alloc_always }) catch |err| if (err == error.OutOfMemory) err else error.InvalidCatalogRecord;
}
fn put(a: A, txn: anytype, address: []const u8, value: anytype) !void {
    const json = try std.json.Stringify.valueAlloc(a, value, .{});
    if (json.len + 32 > max_record_bytes) return error.InvalidCatalogRecord;
    const raw = try a.alloc(u8, json.len + 32);
    @memcpy(raw[0..json.len], json);
    std.crypto.hash.Blake3.hash(json, raw[json.len..][0..32], .{});
    try txn.put(address, raw);
}
fn erase(txn: anytype, address: []const u8) !void {
    txn.delete(address) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}
pub fn head(a: A, txn: anytype, scope: Scope) !Head {
    try scope.validate();
    const result = (try get(Head, a, txn, try key(a, scope, "head"))) orelse Head{ .scope = scope };
    if (result.next == 0 or result.oldest == 0 or result.oldest > result.next or result.revision == 0 or result.used > max_queue_bytes or result.reserved > max_queue_bytes - result.used) return error.InvalidCatalogRecord;
    if (result.scope == null or !std.meta.eql(result.scope.?, scope) or result.retention_revision == 0) return error.InvalidCatalogRecord;
    if (result.gc) |progress| {
        if (progress.cut < result.oldest or progress.cut > result.next or progress.minimum < result.oldest or progress.minimum > progress.cut or progress.revision == 0 or progress.revision > result.revision or progress.retention_revision == 0 or progress.retention_revision > result.retention_revision) return error.InvalidCatalogRecord;
        if (progress.after) |after| for (after) |byte| {
            if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.InvalidCatalogRecord;
        };
    }
    if (result.gc_memo) |memo| if (memo.revision == 0 or memo.revision > result.revision or memo.retention_revision == 0 or memo.retention_revision > result.retention_revision) return error.InvalidCatalogRecord;
    return result;
}
pub fn listener(a: A, txn: anytype, group: u64, connection: [32]u8, generation: u64) !?Listener {
    const found = (try get(Listener, a, txn, try listenerKey(a, group, connection))) orelse return null;
    if (!std.mem.eql(u8, &found.connection, &connection) or found.generation != generation or found.channels.len == 0 or found.channels.len > max_channels or
        found.acknowledged.sequence == 0 or types.Cursor.order(found.acknowledged, found.offered) == .gt) return error.InvalidCatalogRecord;
    try found.scope.validate();
    for (found.channels, 0..) |channel, i| {
        try (types.Command{ .listen = channel.name }).validate();
        if (channel.since == 0 or (i != 0 and std.mem.order(u8, found.channels[i - 1].name, channel.name) != .lt)) return error.InvalidCatalogRecord;
    }
    return found;
}
fn digest(a: A, plan: types.Plan) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, plan, .{});
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn requireRef(a: A, txn: anytype, scope: Scope, operation: [16]u8, transaction: [16]u8, plan: types.Plan) !Ref {
    const value = (try get(Ref, a, txn, try referenceKey(a, scope, operation))) orelse return error.ExternalTransactionDecisionChanged;
    if (!std.mem.eql(u8, &value.transaction, &transaction) or !std.mem.eql(u8, &value.digest, &try digest(a, plan))) return error.ExternalTransactionDecisionChanged;
    return value;
}
fn channels(a: A, before: ?Listener, plan: types.Plan, since: u64) ![]const Channel {
    var selected: std.StringArrayHashMapUnmanaged(u64) = .empty;
    if (before) |prior| for (prior.channels) |channel| try selected.put(a, channel.name, channel.since);
    for (plan.commands) |command| switch (command) {
        .listen => |name| {
            if (!selected.contains(name)) {
                if (selected.count() == max_channels) return error.SqlProgramLimitExceeded;
                try selected.put(a, name, since);
            }
        },
        .unlisten => |name| _ = selected.orderedRemove(name),
        .unlisten_all => selected.clearRetainingCapacity(),
        .notify => {},
    };
    const result = try a.alloc(Channel, selected.count());
    for (selected.keys(), selected.values(), result) |name, start, *out| out.* = .{ .name = name, .since = start };
    std.mem.sortUnstable(Channel, result, {}, struct {
        fn less(_: void, left: Channel, right: Channel) bool {
            return std.mem.order(u8, left.name, right.name) == .lt;
        }
    }.less);
    return result;
}
fn bloomAdd(bits: *[4]u64, name: []const u8) void {
    const hash = std.hash.Wyhash.hash(0, name);
    inline for (0..3) |i| {
        const bit: u8 = @truncate(hash >> (i * 16));
        bits[bit / 64] |= @as(u64, 1) << @intCast(bit % 64);
    }
}
fn bloomMayContain(bits: [4]u64, name: []const u8) bool {
    var required: [4]u64 = @splat(0);
    bloomAdd(&required, name);
    for (bits, required) |present, needed| if (present & needed != needed) return false;
    return true;
}
fn interested(prior: Listener, sequence: u64, name: []const u8) bool {
    var lower: usize = 0;
    var upper = prior.channels.len;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        const channel = prior.channels[middle];
        switch (std.mem.order(u8, channel.name, name)) {
            .lt => lower = middle + 1,
            .gt => upper = middle,
            .eq => return channel.since <= sequence,
        }
    }
    return false;
}

const Reservation = struct { state: Head, budget: Budget, value: Ref };
fn reservation(a: A, txn: anytype, scope: Scope, operation: [16]u8, transaction: [16]u8, plan: types.Plan) !?Reservation {
    try plan.validate();
    const expected = try digest(a, plan);
    if (try get(Ref, a, txn, try referenceKey(a, scope, operation))) |_| {
        _ = try requireRef(a, txn, scope, operation, transaction, plan);
        return null;
    }
    var state = try head(a, txn, scope);
    if (!plan.discard) {
        const prior = try listener(a, txn, scope.group, plan.connection, plan.generation);
        if (prior) |value| if (!std.meta.eql(value.scope, scope)) return error.CatalogGenerationChanged;
        _ = try channels(a, prior, plan, state.next);
    }
    var bytes: u64 = 0;
    if (!plan.discard) for (plan.commands) |command| {
        if (command == .notify) bytes += 64 + command.encodedBytes();
    };
    if (bytes != 0) bytes += 4096;
    if (bytes > max_queue_bytes - state.used - state.reserved) return error.SqlProgramLimitExceeded;
    var budget = try groupBudget(a, txn, scope.group);
    if (bytes > max_group_queue_bytes - budget.used - budget.reserved) return error.SqlProgramLimitExceeded;
    state.reserved += bytes;
    budget.reserved += bytes;
    return .{ .state = state, .budget = budget, .value = .{ .transaction = transaction, .digest = expected, .reserved = bytes } };
}
/// Pure preflight: native outcome planning must reject capacity before writes.
pub fn validateReserve(a: A, txn: anytype, scope: Scope, operation: [16]u8, transaction: [16]u8, plan: types.Plan) !void {
    _ = try reservation(a, txn, scope, operation, transaction, plan);
}
pub fn reserve(a: A, txn: anytype, scope: Scope, operation: [16]u8, transaction: [16]u8, plan: types.Plan) !void {
    const prepared = (try reservation(a, txn, scope, operation, transaction, plan)) orelse return;
    try put(a, txn, try key(a, scope, "head"), prepared.state);
    try put(a, txn, try budgetKey(a, scope.group), prepared.budget);
    try put(a, txn, try referenceKey(a, scope, operation), prepared.value);
    if (prepared.value.reserved != 0) try put(a, txn, try workKey(a, scope), scope);
}

pub fn abort(a: A, txn: anytype, scope: Scope, operation: [16]u8, transaction: [16]u8, plan: types.Plan) !void {
    const value = try requireRef(a, txn, scope, operation, transaction, plan);
    if (value.committed) return error.ExternalTransactionDecisionChanged;
    var state = try head(a, txn, scope);
    if (state.reserved < value.reserved) return error.InvalidCatalogRecord;
    var budget = try groupBudget(a, txn, scope.group);
    if (budget.reserved < value.reserved) return error.InvalidCatalogRecord;
    budget.reserved -= value.reserved;
    try put(a, txn, try budgetKey(a, scope.group), budget);
    state.reserved -= value.reserved;
    try put(a, txn, try key(a, scope, "head"), state);
    try erase(txn, try referenceKey(a, scope, operation));
}
pub fn commit(a: A, txn: anytype, scope: Scope, operation: [16]u8, transaction: [16]u8, plan: types.Plan) !void {
    if (plan.discard) return error.InvalidPreparedTransactionPhase;
    var reference = try requireRef(a, txn, scope, operation, transaction, plan);
    if (reference.committed) return;
    var state = try head(a, txn, scope);
    if (state.reserved < reference.reserved or state.next == std.math.maxInt(u64) or state.revision == std.math.maxInt(u64)) return error.InvalidCatalogRecord;
    const prior = try listener(a, txn, scope.group, plan.connection, plan.generation);
    const selected = try channels(a, prior, plan, state.next);
    if (selected.len == 0) {
        if (prior) |value| {
            try erase(txn, try memberKey(a, value.scope, plan.connection));
            try erase(txn, try listenerKey(a, scope.group, plan.connection));
        }
    } else {
        const initial: types.Cursor = .{ .sequence = state.next };
        const updated: Listener = Listener{ .scope = scope, .connection = plan.connection, .generation = plan.generation, .channels = selected, .acknowledged = if (prior) |v| v.acknowledged else initial, .offered = if (prior) |v| v.offered else initial };
        try put(a, txn, try listenerKey(a, scope.group, plan.connection), updated);
        try member(a, txn, updated);
    }
    var events = std.ArrayList(@typeInfo(@FieldType(Payload, "events")).pointer.child).empty;
    var bloom: [4]u64 = @splat(0);
    var distinct: types.EventSet = .empty;
    for (plan.commands) |command| if (command == .notify) {
        const entry = try distinct.getOrPut(a, .{ .channel = command.notify.channel, .payload = command.notify.payload });
        if (entry.found_existing) continue;
        try events.append(a, .{ .channel = command.notify.channel, .payload = command.notify.payload });
        bloomAdd(&bloom, command.notify.channel);
    };
    if (events.items.len != 0) {
        const body = try std.json.Stringify.valueAlloc(a, Payload{ .sender_pid = plan.sender_pid, .events = events.items }, .{});
        const bytes: u64 = @intCast(body.len + 2048);
        if (bytes > reference.reserved or body.len > max_record_bytes) return error.InvalidCatalogRecord;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(body, &checksum, .{});
        const slot: Slot = .{ .sequence = state.next, .operation = operation, .transaction = transaction, .digest = reference.digest, .checksum = checksum, .bytes = bytes, .event_count = @intCast(events.items.len), .bloom = bloom };
        try txn.put(try slotKey(a, scope, state.next, true), body);
        try put(a, txn, try slotKey(a, scope, state.next, false), slot);
        reference.sequence = state.next;
        state.next += 1;
        state.used += bytes;
    }
    var budget = try groupBudget(a, txn, scope.group);
    if (budget.reserved < reference.reserved) return error.InvalidCatalogRecord;
    budget.reserved -= reference.reserved;
    if (reference.sequence) |sequence| {
        const slot = (try get(Slot, a, txn, try slotKey(a, scope, sequence, false))) orelse return error.InvalidCatalogRecord;
        budget.used += slot.bytes;
    }
    try put(a, txn, try budgetKey(a, scope.group), budget);
    state.reserved -= reference.reserved;
    state.revision += 1;
    state.retention_revision = try std.math.add(u64, state.retention_revision, 1);
    reference.committed = true;
    try put(a, txn, try referenceKey(a, scope, operation), reference);
    try put(a, txn, try key(a, scope, "head"), state);
}
pub fn ready(a: A, txn: anytype, scope: Scope, operation: [16]u8, transaction: [16]u8, plan: types.Plan) !void {
    const reference = try requireRef(a, txn, scope, operation, transaction, plan);
    if (!reference.committed) return error.ExternalTransactionDecisionChanged;
    const sequence = reference.sequence orelse {
        try erase(txn, try referenceKey(a, scope, operation));
        return;
    };
    var slot = (try get(Slot, a, txn, try slotKey(a, scope, sequence, false))) orelse return error.InvalidCatalogRecord;
    if (slot.sequence != sequence or !std.mem.eql(u8, &slot.operation, &operation) or !std.mem.eql(u8, &slot.transaction, &transaction) or !std.mem.eql(u8, &slot.digest, &reference.digest)) return error.InvalidCatalogRecord;
    if (slot.ready) return;
    slot.ready = true;
    var state = try head(a, txn, scope);
    state.revision = try std.math.add(u64, state.revision, 1);
    try put(a, txn, try slotKey(a, scope, sequence, false), slot);
    try put(a, txn, try key(a, scope, "head"), state);
}
pub fn removeListener(a: A, txn: anytype, group: u64, connection: [32]u8) !void {
    if (try get(Listener, a, txn, try listenerKey(a, group, connection))) |prior| {
        try erase(txn, try memberKey(a, prior.scope, connection));
        try erase(txn, try listenerKey(a, group, connection));
        var state = try head(a, txn, prior.scope);
        state.retention_revision = try std.math.add(u64, state.retention_revision, 1);
        try put(a, txn, try key(a, prior.scope, "head"), state);
    }
}
pub fn acknowledge(a: A, txn: anytype, group: u64, connection: [32]u8, generation: u64, cursor: types.Cursor) !void {
    var prior = (try listener(a, txn, group, connection, generation)) orelse return error.InvalidSqlConnection;
    if (cursor.sequence == 0 or types.Cursor.order(cursor, prior.offered) == .gt) return error.SqlInvalidParameterValue;
    // Delayed ACKs remain successful after a newer ACK and prefix reclamation.
    if (types.Cursor.order(cursor, prior.acknowledged) != .gt) return;
    const state = try head(a, txn, prior.scope);
    if (cursor.sequence == state.next) {
        if (cursor.ordinal != 0) return error.SqlInvalidParameterValue;
    } else {
        const slot = (try get(Slot, a, txn, try slotKey(a, prior.scope, cursor.sequence, false))) orelse return error.SqlInvalidParameterValue;
        if (cursor.ordinal > slot.event_count) return error.SqlInvalidParameterValue;
    }
    if (types.Cursor.order(cursor, prior.acknowledged) == .eq) return;
    prior.acknowledged = cursor;
    try put(a, txn, try listenerKey(a, group, connection), prior);
    try member(a, txn, prior);
    var updated = state;
    updated.retention_revision = try std.math.add(u64, updated.retention_revision, 1);
    try put(a, txn, try key(a, prior.scope, "head"), updated);
}
/// Shared physical block validation for reads and snapshot publication.
pub fn validatePayload(a: A, slot: Slot, body: []const u8, comptime canonical: bool) !Payload {
    if (body.len > max_record_bytes or slot.bytes != body.len + 2048 or slot.event_count == 0 or slot.event_count > types.max_commands) return error.InvalidCatalogRecord;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(body, &checksum, .{});
    if (!std.mem.eql(u8, &checksum, &slot.checksum)) return error.InvalidCatalogRecord;
    const payload = std.json.parseFromSliceLeaky(Payload, a, body, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) error.OutOfMemory else error.InvalidCatalogRecord;
    if (payload.sender_pid <= 0 or payload.events.len != slot.event_count) return error.InvalidCatalogRecord;
    var bloom: [4]u64 = @splat(0);
    var bytes: usize = 0;
    var distinct: types.EventSet = .empty;
    for (payload.events) |event| {
        const command: types.Command = .{ .notify = .{ .channel = event.channel, .payload = event.payload } };
        command.validate() catch return error.InvalidCatalogRecord;
        bytes += command.bytes();
        if (bytes > types.max_pending_bytes) return error.InvalidCatalogRecord;
        if (canonical) {
            if ((try distinct.getOrPut(a, .{ .channel = event.channel, .payload = event.payload })).found_existing) return error.InvalidCatalogRecord;
            bloomAdd(&bloom, event.channel);
        }
    }
    if (canonical) {
        if (!std.mem.eql(u64, &bloom, &slot.bloom) or !std.mem.eql(u8, body, try std.json.Stringify.valueAlloc(a, payload, .{}))) return error.InvalidCatalogRecord;
    }
    return payload;
}

pub fn readPage(a: A, txn: anytype, group: u64, connection: [32]u8, generation: u64, maximum: usize) !Page {
    if (maximum == 0 or maximum > types.max_commands) return error.SqlInvalidParameterValue;
    const prior = (try listener(a, txn, group, connection, generation)) orelse return .{};
    const state = try head(a, txn, prior.scope);
    var cursor = prior.acknowledged;
    if (cursor.sequence < state.oldest or cursor.sequence > state.next) return error.InvalidCatalogRecord;
    var events: std.ArrayList(types.Event) = .empty;
    var bytes: usize = 0;
    var pending = false;
    var waiting = false;
    var inspected: usize = 0;
    while (cursor.sequence < state.next and inspected < 8) : (inspected += 1) {
        const slot = (try get(Slot, a, txn, try slotKey(a, prior.scope, cursor.sequence, false))) orelse return error.InvalidCatalogRecord;
        if (slot.sequence != cursor.sequence or slot.event_count == 0 or slot.event_count > types.max_commands) return error.InvalidCatalogRecord;
        const possible = for (prior.channels) |channel| {
            if (channel.since <= cursor.sequence and bloomMayContain(slot.bloom, channel.name)) break true;
        } else false;
        if (!possible) {
            cursor.sequence += 1;
            cursor.ordinal = 0;
            continue;
        }
        if (!slot.ready) {
            waiting = true;
            pending = true;
            break;
        }
        const body = try txn.get(try slotKey(a, prior.scope, cursor.sequence, true));
        const payload = try validatePayload(a, slot, body, false);
        if (cursor.ordinal > payload.events.len) return error.InvalidCatalogRecord;
        while (cursor.ordinal < payload.events.len) {
            const event = payload.events[cursor.ordinal];
            try (types.Command{ .notify = .{ .channel = event.channel, .payload = event.payload } }).validate();
            if (interested(prior, cursor.sequence, event.channel)) {
                const size = event.channel.len + event.payload.len;
                if (events.items.len == maximum or size > types.max_pending_bytes -| bytes) break;
                try events.append(a, .{ .sequence = cursor.sequence, .ordinal = cursor.ordinal, .channel = event.channel, .payload = event.payload, .sender_pid = payload.sender_pid });
                bytes += size;
            }
            cursor.ordinal += 1;
        }
        if (cursor.ordinal < payload.events.len) break;
        cursor.sequence += 1;
        cursor.ordinal = 0;
    }

    return .{ .events = events.items, .cursor = cursor, .acknowledged = prior.acknowledged, .offered = prior.offered, .subscribed = true, .revision = state.revision, .waiting = waiting, .pending = pending or cursor.sequence < state.next };
}

/// A persistent minimum scan is conservative under concurrent registrations:
/// new listeners start at/after cut, old cursors only advance, and deletions
/// cannot lower the minimum. No global scan or payload decode holds the writer.
/// Run in a separate maintenance transaction, never after a user's commit in
/// that same writer: corruption/failure of GC must not change its outcome.
pub fn offer(a: A, txn: anytype, group: u64, connection: [32]u8, generation: u64, cursor: types.Cursor, maximum: usize) !void {
    var prior = (try listener(a, txn, group, connection, generation)) orelse return error.InvalidSqlConnection;
    if (types.Cursor.order(cursor, prior.offered) != .gt) return;
    const page = try readPage(a, txn, group, connection, generation, maximum);
    if (types.Cursor.order(cursor, prior.acknowledged) == .lt or types.Cursor.order(cursor, page.cursor) == .gt) return error.SqlInvalidParameterValue;
    const state = try head(a, txn, prior.scope);
    if (cursor.sequence == state.next) {
        if (cursor.ordinal != 0) return error.SqlInvalidParameterValue;
    } else {
        const slot = (try get(Slot, a, txn, try slotKey(a, prior.scope, cursor.sequence, false))) orelse return error.SqlInvalidParameterValue;
        if (cursor.ordinal > slot.event_count) return error.SqlInvalidParameterValue;
    }
    prior.offered = cursor;
    try put(a, txn, try listenerKey(a, group, connection), prior);
}
pub fn poll(a: A, txn: anytype, group: u64, connection: [32]u8, generation: u64, maximum: usize) !Page {
    var page = try readPage(a, txn, group, connection, generation, maximum);
    if (try listener(a, txn, group, connection, generation)) |_| {
        try offer(a, txn, group, connection, generation, page.cursor, maximum);
        if (types.Cursor.order(page.cursor, page.offered) == .gt) page.offered = page.cursor;
    }
    return page;
}

pub fn gcStep(a: A, txn: anytype, scope: Scope) !bool {
    var state = try head(a, txn, scope);
    if (state.oldest == state.next and state.reserved == 0) {
        try erase(txn, try workKey(a, scope));
        return false;
    }
    if (!needsGc(state)) return false;
    var progress = state.gc orelse Gc{ .cut = state.next, .minimum = state.next, .revision = state.revision, .retention_revision = state.retention_revision };
    if (progress.cut == 0 or progress.cut > state.next or progress.minimum > progress.cut) return error.InvalidCatalogRecord;
    if (!progress.deleting) {
        const prefix = try key(a, scope, "listener/");
        const lower = if (progress.after) |id| try std.mem.concat(a, u8, &.{ try memberKey(a, scope, id), "\x00" }) else prefix;
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(lower);
        var inspected: usize = 0;
        while (entry) |value| {
            if (!std.mem.startsWith(u8, value.key, prefix) or inspected == 16) break;
            if (value.key.len != prefix.len + 64) return error.InvalidCatalogRecord;
            var id: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&id, value.key[prefix.len..]) catch return error.InvalidCatalogRecord;
            const projection = (try get(Member, a, txn, value.key)) orelse return error.InvalidCatalogRecord;
            if (!std.mem.eql(u8, &projection.scope, &scope.id()) or !std.mem.eql(u8, &projection.connection, &id) or projection.generation == 0 or projection.acknowledged.sequence < state.oldest or projection.acknowledged.sequence > state.next) return error.InvalidCatalogRecord;
            progress.minimum = @min(progress.minimum, projection.acknowledged.sequence);
            progress.after = id;
            inspected += 1;
            entry = try cursor.next();
        }
        progress.deleting = entry == null or !std.mem.startsWith(u8, entry.?.key, prefix);
        state.gc = progress;
    } else {
        var removed: usize = 0;
        var blocked = false;
        while (state.oldest < progress.minimum and removed < 8) : (removed += 1) {
            const slot = (try get(Slot, a, txn, try slotKey(a, scope, state.oldest, false))) orelse return error.InvalidCatalogRecord;
            if (slot.sequence != state.oldest or slot.bytes > state.used) return error.InvalidCatalogRecord;
            if (!slot.ready) {
                blocked = true;
                break;
            }
            try erase(txn, try slotKey(a, scope, state.oldest, true));
            try erase(txn, try slotKey(a, scope, state.oldest, false));
            try erase(txn, try referenceKey(a, scope, slot.operation));
            var budget = try groupBudget(a, txn, scope.group);
            if (budget.used < slot.bytes) return error.InvalidCatalogRecord;
            budget.used -= slot.bytes;
            try put(a, txn, try budgetKey(a, scope.group), budget);
            state.used -= slot.bytes;
            state.oldest += 1;
        }
        if (state.oldest >= progress.minimum) {
            state.gc = null;
            state.gc_memo = .{ .revision = progress.revision, .retention_revision = progress.retention_revision };
        } else {
            state.gc = progress;
            if (blocked) state.gc_memo = .{ .revision = state.revision, .retention_revision = state.retention_revision };
        }
    }
    try put(a, txn, try key(a, scope, "head"), state);
    if (state.oldest == state.next and state.reserved == 0) {
        try erase(txn, try workKey(a, scope));
        return false;
    }
    return needsGc(state);
}

const Test = struct {
    const scope: Scope = .{ .group = 1, .database = 2, .incarnation = @splat(3) };
    const alice: [32]u8 = @splat('a');
    const bob: [32]u8 = @splat('b');
    fn plan(connection: [32]u8, commands: []const types.Command) types.Plan {
        return .{ .connection = connection, .generation = 1, .sender_pid = 42, .commands = commands };
    }
    fn publish(a: A, store: anytype, operation: u8, input: types.Plan, complete: bool) !void {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try reserve(a, &txn, scope, @splat(operation), @splat(operation + 1), input);
        try commit(a, &txn, scope, @splat(operation), @splat(operation + 1), input);
        if (complete) try ready(a, &txn, scope, @splat(operation), @splat(operation + 1), input);
        try txn.commit();
    }
};

test "SQL notification journal orders visibility barriers bounds pages preserves replay and reclaims acknowledged blocks" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    var backend = local.storage_mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "notification-journal" });
    defer store.deinit();
    try Test.publish(work, &store, 1, Test.plan(Test.alice, &.{.{ .listen = "events" }}), true);
    const first = Test.plan(Test.bob, &.{ .{ .notify = .{ .channel = "events", .payload = "updated" } }, .{ .notify = .{ .channel = "events", .payload = "updated" } }, .{ .notify = .{ .channel = "other", .payload = "hidden" } } });
    try Test.publish(work, &store, 3, first, false);
    try Test.publish(work, &store, 5, Test.plan(Test.bob, &.{.{ .notify = .{ .channel = "events", .payload = "updated" } }}), true);
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        const page = try poll(work, &txn, Test.scope.group, Test.alice, 1, 1);
        try std.testing.expectEqual(@as(usize, 0), page.events.len);
        try std.testing.expect(page.pending);
        try ready(work, &txn, Test.scope, @splat(3), @splat(4), first);
        try txn.commit();
    }
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        const page = try poll(work, &txn, Test.scope.group, Test.alice, 1, 1);
        try std.testing.expectEqual(@as(usize, 1), page.events.len);
        try std.testing.expectEqual(@as(u64, 1), page.events[0].sequence);
        try std.testing.expectEqualStrings("updated", page.events[0].payload);
        try std.testing.expectEqual(@as(i32, 42), page.events[0].sender_pid);
        const replay = try poll(work, &txn, Test.scope.group, Test.alice, 1, 1);
        try std.testing.expectEqualDeep(page, replay);
        try acknowledge(work, &txn, Test.scope.group, Test.alice, 1, page.cursor);
        const second = try poll(work, &txn, Test.scope.group, Test.alice, 1, 1);
        try std.testing.expectEqual(@as(usize, 1), second.events.len);
        try std.testing.expectEqual(@as(u64, 2), second.events[0].sequence);
        try std.testing.expectEqualStrings("updated", second.events[0].payload);
        // Same payload in another transaction must not be folded. A forged
        // middle-block ordinal cannot poison a listener's durable cursor.
        try std.testing.expectError(error.SqlInvalidParameterValue, acknowledge(work, &txn, Test.scope.group, Test.alice, 1, .{ .sequence = 2, .ordinal = 1000 }));
        try acknowledge(work, &txn, Test.scope.group, Test.alice, 1, second.cursor);
        try txn.commit();
    }
    for (0..4) |_| {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        _ = try gcStep(work, &txn, Test.scope);
        try txn.commit();
    }
    {
        var txn = try store.beginRead();
        defer txn.abort();
        const state = try head(work, &txn, Test.scope);
        try std.testing.expectEqual(@as(u64, 0), state.used);
        try std.testing.expectEqual(@as(u64, 0), state.reserved);
        try std.testing.expectEqual(@as(u64, 3), state.next);
        try std.testing.expectEqual(state.next, state.oldest);
        try std.testing.expectError(error.NotFound, txn.get(try slotKey(work, Test.scope, 1, true)));
    }
}

test "SQL notification journal reservations abort atomically and exclude future listen channels from old blocks" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    var backend = local.storage_mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "notification-listeners" });
    defer store.deinit();
    try Test.publish(work, &store, 1, Test.plan(Test.alice, &.{.{ .listen = "old" }}), true);
    try Test.publish(work, &store, 3, Test.plan(Test.bob, &.{.{ .notify = .{ .channel = "new", .payload = "before LISTEN" } }}), true);
    const changed = Test.plan(Test.alice, &.{.{ .listen = "new" }});
    {
        var txn = try store.beginWrite();
        try reserve(work, &txn, Test.scope, @splat(5), @splat(6), changed);
        txn.abort();
    }
    {
        var txn = try store.beginRead();
        defer txn.abort();
        try std.testing.expectEqual(@as(u64, 0), (try head(work, &txn, Test.scope)).reserved);
        try std.testing.expectEqual(@as(usize, 1), (try listener(work, &txn, Test.scope.group, Test.alice, 1)).?.channels.len);
    }
    try Test.publish(work, &store, 5, changed, true);
    try Test.publish(work, &store, 7, Test.plan(Test.bob, &.{.{ .notify = .{ .channel = "new", .payload = "after LISTEN" } }}), true);
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        const page = try poll(work, &txn, Test.scope.group, Test.alice, 1, 128);
        try std.testing.expectEqual(@as(usize, 1), page.events.len);
        try std.testing.expectEqualStrings("after LISTEN", page.events[0].payload);
        try reserve(work, &txn, Test.scope, @splat(9), @splat(10), changed);
        try abort(work, &txn, Test.scope, @splat(9), @splat(10), changed);
        try std.testing.expectEqual(@as(u64, 0), (try head(work, &txn, Test.scope)).reserved);
        try removeListener(work, &txn, Test.scope.group, Test.alice);
        try txn.commit();
    }
}

test "SQL notification journal compact GC objectives sleep when pinned wake on ACK and reclaim group escrow after source retirement" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    var backend = local.storage_mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "notification-gc-objectives" });
    defer store.deinit();
    const source: local.storage_pessimistic_lock_contract.Backend = .{ .incarnation = Test.scope.incarnation, .id = @splat(8), .fence = 1 };
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try bind(work, &txn, Test.scope.group, .{ .backend = source, .connection = Test.alice, .generation = 1, .sender_pid = 42 });
        try txn.commit();
    }
    try Test.publish(work, &store, 1, Test.plan(Test.alice, &.{.{ .listen = "events" }}), true);
    try Test.publish(work, &store, 3, Test.plan(Test.bob, &.{.{ .notify = .{ .channel = "events", .payload = "pending" } }}), true);
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try std.testing.expect((try readWork(work, &txn, Test.scope.group, Test.scope.incarnation, null)).scope != null);
        for (0..3) |_| _ = try gcStep(work, &txn, Test.scope);
        try std.testing.expect((try readWork(work, &txn, Test.scope.group, Test.scope.incarnation, null)).scope == null);
        try std.testing.expect((try groupBudget(work, &txn, Test.scope.group)).used > 0);
        const current = try poll(work, &txn, Test.scope.group, Test.alice, 1, 128);
        try acknowledge(work, &txn, Test.scope.group, Test.alice, 1, current.cursor);
        try std.testing.expect((try readWork(work, &txn, Test.scope.group, Test.scope.incarnation, null)).scope != null);
        for (0..3) |_| _ = try gcStep(work, &txn, Test.scope);
        try std.testing.expectEqualDeep(Budget{}, try groupBudget(work, &txn, Test.scope.group));
        try std.testing.expect((try readWork(work, &txn, Test.scope.group, Test.scope.incarnation, null)).scope == null);
        try txn.commit();
    }
    try Test.publish(work, &store, 5, Test.plan(Test.bob, &.{.{ .notify = .{ .channel = "events", .payload = "retired" } }}), true);
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try retireBinding(work, &txn, Test.scope.group, source);
        try std.testing.expect((try listener(work, &txn, Test.scope.group, Test.alice, 1)) == null);
        try std.testing.expect((try readBinding(work, &txn, Test.scope.group, source)) == null);
        for (0..4) |_| _ = try gcStep(work, &txn, Test.scope);
        try std.testing.expectEqualDeep(Budget{}, try groupBudget(work, &txn, Test.scope.group));
        try std.testing.expectEqual(@as(u64, 3), (try head(work, &txn, Test.scope)).next);
        try txn.commit();
    }
}

test "SQL notification journal rejects group snapshot headroom exhaustion before admission but permits rollback" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    var backend = local.storage_mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "notification-snapshot-headroom" });
    defer store.deinit();
    var txn = try store.beginWrite();
    defer txn.abort();
    try put(work, &txn, try budgetKey(work, Test.scope.group), Budget{ .used = max_group_queue_bytes });
    var plan_value = Test.plan(Test.bob, &.{.{ .notify = .{ .channel = "events", .payload = "capacity" } }});
    try std.testing.expectError(error.SqlProgramLimitExceeded, validateReserve(work, &txn, Test.scope, @splat(1), @splat(2), plan_value));
    try std.testing.expectEqual(@as(u64, 0), (try head(work, &txn, Test.scope)).reserved);
    try std.testing.expectEqualDeep(Budget{ .used = max_group_queue_bytes }, try groupBudget(work, &txn, Test.scope.group));
    plan_value.discard = true;
    try reserve(work, &txn, Test.scope, @splat(1), @splat(2), plan_value);
    try abort(work, &txn, Test.scope, @splat(1), @splat(2), plan_value);
    try std.testing.expectEqualDeep(Budget{ .used = max_group_queue_bytes }, try groupBudget(work, &txn, Test.scope.group));
}

test "SQL notification journal validates canonical blocks and retains PID identity across discard generations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var backend = local.storage_mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var store = try backend.runtimeStore(std.testing.allocator, .{ .name = "notification-discard-integrity" });
    defer store.deinit();
    const source: local.storage_pessimistic_lock_contract.Backend = .{ .incarnation = Test.scope.incarnation, .id = @splat(9), .fence = 1 };
    try Test.publish(a, &store, 1, Test.plan(Test.alice, &.{.{ .listen = "events" }}), true);
    try Test.publish(a, &store, 3, Test.plan(Test.bob, &.{.{ .notify = .{ .channel = "events", .payload = "body" } }}), true);
    var txn = try store.beginWrite();
    defer txn.abort();
    try bind(a, &txn, Test.scope.group, .{ .backend = source, .connection = Test.alice, .generation = 1, .sender_pid = 42 });
    const slot = (try get(Slot, a, &txn, try slotKey(a, Test.scope, 1, false))).?;
    const body = try txn.get(try slotKey(a, Test.scope, 1, true));
    _ = try validatePayload(a, slot, body, true);
    const padded = try std.mem.concat(a, u8, &.{ " ", body });
    var alternate = slot;
    alternate.bytes += 1;
    std.crypto.hash.Blake3.hash(padded, &alternate.checksum, .{});
    _ = try validatePayload(a, alternate, padded, false);
    try std.testing.expectError(error.InvalidCatalogRecord, validatePayload(a, alternate, padded, true));
    alternate = slot;
    alternate.bloom = @splat(0);
    try std.testing.expectError(error.InvalidCatalogRecord, validatePayload(a, alternate, body, true));
    const offered = try poll(a, &txn, Test.scope.group, Test.alice, 1, 128);
    try acknowledge(a, &txn, Test.scope.group, Test.alice, 1, offered.cursor);
    for (0..4) |_| _ = try gcStep(a, &txn, Test.scope);
    try acknowledge(a, &txn, Test.scope.group, Test.alice, 1, .{ .sequence = 1 });
    try resetBinding(a, &txn, Test.scope.group, source, 2);
    try resetBinding(a, &txn, Test.scope.group, source, 2);
    try std.testing.expect((try listener(a, &txn, Test.scope.group, Test.alice, 2)) == null);
    const binding = (try readBinding(a, &txn, Test.scope.group, source)).?;
    try std.testing.expectEqual(@as(i32, 42), binding.sender_pid);
    try std.testing.expectEqual(@as(u64, 2), binding.generation);
    try std.testing.expectError(error.CatalogGenerationChanged, resetBinding(a, &txn, Test.scope.group, source, 4));
}

test "SQL notification journal bounds escaped intents before sealing and restores encoded headroom on rollback" {
    const a = std.testing.allocator;
    var pending: types.Pending = .{};
    defer pending.deinit(a);
    var payload: [types.max_payload_bytes]u8 = @splat(1);
    const first: types.Command = .{ .notify = .{ .channel = "events", .payload = &payload } };
    try pending.append(a, first);
    payload[payload.len - 1] = 2;
    try pending.append(a, .{ .notify = .{ .channel = "events", .payload = &payload } });
    const frontier = pending.encoded_bytes;
    payload[payload.len - 1] = 3;
    try std.testing.expectError(error.SqlProgramLimitExceeded, pending.append(a, .{ .notify = .{ .channel = "events", .payload = &payload } }));
    try std.testing.expectEqual(@as(usize, 2), pending.commands.items.len);
    try std.testing.expectEqual(frontier, pending.encoded_bytes);
    pending.truncate(a, 1);
    try pending.append(a, .{ .notify = .{ .channel = "events", .payload = &payload } });
    var cloned = try pending.clone(a);
    defer cloned.deinit(a);
    try std.testing.expectEqual(pending.encoded_bytes, cloned.encoded_bytes);
    try (types.Plan{ .connection = Test.alice, .generation = 1, .sender_pid = 1, .commands = pending.commands.items }).validate();
}
