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

//! Validate queue snapshots before publication: one block of decode scratch,
//! linear row visits, compact per-scope totals and exact forward/reverse edges.
const std = @import("std");
const local = @import("antfly_local_sources");
const N = @import("sql_notifications.zig");
const P = @import("prepared_transactions.zig");
const A = std.mem.Allocator;
const Totals = struct { head: ?N.Head = null, used: u64 = 0, reserved: u64 = 0, blocks: u64 = 0, minimum: u64 = std.math.maxInt(u64), scanned_minimum: u64 = std.math.maxInt(u64) };
fn record(comptime T: type, a: A, raw: []const u8) !T {
    const value = try N.decodeRecord(T, a, raw);
    const canonical = try std.json.Stringify.valueAlloc(a, value, .{});
    if (!std.mem.eql(u8, raw[0 .. raw.len - 32], canonical)) return error.InvalidCatalogRecord;
    return value;
}
fn linked(comptime T: type, a: A, reader: anytype, address: []const u8) !T {
    return record(T, a, try reader.get(address));
}
fn bindingAddress(a: A, prefix: []const u8, binding: N.Binding) ![]u8 {
    return std.fmt.allocPrint(a, "{s}binding:{s}:{s}:{x:0>16}", .{ prefix, std.fmt.bytesToHex(binding.backend.incarnation, .lower), std.fmt.bytesToHex(binding.backend.id, .lower), binding.backend.fence });
}
fn listenerAddress(a: A, prefix: []const u8, connection: [32]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}listener:{s}", .{ prefix, std.fmt.bytesToHex(connection, .lower) });
}
fn checkBinding(a: A, reader: anytype, group: u64, prefix: []const u8, binding: N.Binding) !void {
    try binding.backend.validate();
    if (binding.generation == 0 or binding.sender_pid <= 0) return error.InvalidCatalogRecord;
    for (binding.connection) |byte| if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.InvalidCatalogRecord;
    const source = (try local.storage_pessimistic_lock_authority.read(a, reader, .{ .home = .metadata, .group = group, .incarnation = binding.backend.incarnation }, binding.backend)) orelse return error.InvalidCatalogRecord;
    if (source.state == .retired or !std.mem.eql(u8, &source.registration.connection, &binding.connection)) return error.InvalidCatalogRecord;
    const pid = try linked(N.Binding, a, reader, try std.fmt.allocPrint(a, "{s}pid:{x:0>8}", .{ prefix, @as(u32, @intCast(binding.sender_pid)) }));
    if (!std.meta.eql(pid, binding)) return error.InvalidCatalogRecord;
}
pub fn verify(backing: A, group: u64, reader: anytype, entries: anytype, cancelled: ?*const std.atomic.Value(bool)) !void {
    var prefix_buffer: [128]u8 = undefined;
    const prefix = try N.prefixForGroup(&prefix_buffer, group);
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var bindings: std.AutoHashMapUnmanaged([32]u8, N.Binding) = .empty;
    defer bindings.deinit(backing);
    var totals: std.AutoHashMapUnmanaged([32]u8, Totals) = .empty;
    defer totals.deinit(backing);
    // Connection reverse edges are bounded by the existing native source quota.
    for (entries) |row| {
        if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, prefix) or !std.mem.startsWith(u8, row.key[prefix.len..], "binding:")) continue;
        defer _ = arena.reset(.retain_capacity);
        const binding = try record(N.Binding, a, row.value);
        if (!std.mem.eql(u8, row.key, try bindingAddress(a, prefix, binding))) return error.InvalidCatalogRecord;
        try checkBinding(a, reader, group, prefix, binding);
        const entry = try bindings.getOrPut(backing, binding.connection);
        if (entry.found_existing) return error.InvalidCatalogRecord;
        entry.value_ptr.* = binding;
    }
    var budget: ?N.Budget = null;
    for (entries) |row| {
        if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, prefix)) continue;
        defer _ = arena.reset(.retain_capacity);
        const suffix = row.key[prefix.len..];
        if (std.mem.eql(u8, suffix, "budget")) {
            budget = try record(N.Budget, a, row.value);
            if (budget.?.used > N.max_group_queue_bytes or budget.?.reserved > N.max_group_queue_bytes - budget.?.used) return error.InvalidCatalogRecord;
        } else if (std.mem.startsWith(u8, suffix, "binding:")) {
            continue;
        } else if (std.mem.startsWith(u8, suffix, "pid:")) {
            const binding = try record(N.Binding, a, row.value);
            if (binding.sender_pid <= 0 or !std.mem.eql(u8, row.key, try std.fmt.allocPrint(a, "{s}pid:{x:0>8}", .{ prefix, @as(u32, @intCast(binding.sender_pid)) }))) return error.InvalidCatalogRecord;
            const owner = bindings.get(binding.connection) orelse return error.InvalidCatalogRecord;
            if (!std.meta.eql(owner, binding)) return error.InvalidCatalogRecord;
        } else if (std.mem.startsWith(u8, suffix, "work:")) {
            const scope = try record(N.Scope, a, row.value);
            try scope.validate();
            if (scope.group != group or !std.mem.eql(u8, row.key, try std.fmt.allocPrint(a, "{s}work:{s}:{s}", .{ prefix, std.fmt.bytesToHex(scope.incarnation, .lower), std.fmt.bytesToHex(scope.id(), .lower) }))) return error.InvalidCatalogRecord;
            _ = try N.head(a, reader, scope);
        } else if (std.mem.startsWith(u8, suffix, "listener:")) {
            const value = try record(N.Listener, a, row.value);
            const binding = bindings.get(value.connection) orelse return error.InvalidCatalogRecord;
            if (value.scope.group != group or binding.generation != value.generation or !std.mem.eql(u8, &binding.backend.incarnation, &value.scope.incarnation) or !std.mem.eql(u8, row.key, try listenerAddress(a, prefix, value.connection))) return error.InvalidCatalogRecord;
            _ = (try N.listener(a, reader, group, value.connection, value.generation)) orelse return error.InvalidCatalogRecord;
            const state = try N.head(a, reader, value.scope);
            const source = (try local.storage_pessimistic_lock_authority.read(a, reader, .{ .home = .metadata, .group = group, .incarnation = binding.backend.incarnation }, binding.backend)) orelse return error.InvalidCatalogRecord;
            if (source.registration.lease.database != value.scope.database) return error.InvalidCatalogRecord;
            if (value.acknowledged.sequence < state.oldest or value.offered.sequence > state.next) return error.InvalidCatalogRecord;
            for ([_]N.types.Cursor{ value.acknowledged, value.offered }) |cursor| {
                if (cursor.sequence == state.next) {
                    if (cursor.ordinal != 0) return error.InvalidCatalogRecord;
                } else {
                    const block = try linked(N.Slot, a, reader, try std.fmt.allocPrint(a, "{s}{s}:slot/{x:0>16}/head", .{ prefix, std.fmt.bytesToHex(value.scope.id(), .lower), cursor.sequence }));
                    if (cursor.ordinal > block.event_count) return error.InvalidCatalogRecord;
                }
            }
            const membership = try linked(N.Member, a, reader, try std.fmt.allocPrint(a, "{s}{s}:listener/{s}", .{ prefix, std.fmt.bytesToHex(value.scope.id(), .lower), std.fmt.bytesToHex(value.connection, .lower) }));
            if (!std.mem.eql(u8, &membership.scope, &value.scope.id()) or !std.mem.eql(u8, &membership.connection, &value.connection) or membership.generation != value.generation or !std.meta.eql(membership.acknowledged, value.acknowledged)) return error.InvalidCatalogRecord;
        } else {
            if (suffix.len < 66 or suffix[64] != ':') return error.InvalidCatalogRecord;
            var scope_id: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&scope_id, suffix[0..64]) catch return error.InvalidCatalogRecord;
            if (!std.mem.eql(u8, suffix[0..64], &std.fmt.bytesToHex(scope_id, .lower))) return error.InvalidCatalogRecord;
            const entry = try totals.getOrPut(backing, scope_id);
            if (!entry.found_existing) entry.value_ptr.* = .{};
            const item = suffix[65..];
            const head_address = try std.fmt.allocPrint(a, "{s}{s}:head", .{ prefix, std.fmt.bytesToHex(scope_id, .lower) });
            const head = try linked(N.Head, a, reader, head_address);
            const scope = head.scope orelse return error.InvalidCatalogRecord;
            if (scope.group != group or !std.mem.eql(u8, &scope.id(), &scope_id)) return error.InvalidCatalogRecord;
            _ = try N.head(a, reader, scope);
            if (std.mem.eql(u8, item, "head")) {
                entry.value_ptr.head = head;
                if (head.used != 0 or head.reserved != 0) {
                    const work_scope = try linked(N.Scope, a, reader, try std.fmt.allocPrint(a, "{s}work:{s}:{s}", .{ prefix, std.fmt.bytesToHex(scope.incarnation, .lower), std.fmt.bytesToHex(scope_id, .lower) }));
                    if (!std.meta.eql(work_scope, scope)) return error.InvalidCatalogRecord;
                }
            } else if (std.mem.startsWith(u8, item, "listener/")) {
                const membership = try record(N.Member, a, row.value);
                const value = try linked(N.Listener, a, reader, try listenerAddress(a, prefix, membership.connection));
                if (!std.mem.eql(u8, &membership.scope, &scope_id) or !std.mem.eql(u8, &value.scope.id(), &scope_id) or membership.generation != value.generation or !std.meta.eql(membership.acknowledged, value.acknowledged) or !std.mem.eql(u8, item, try std.fmt.allocPrint(a, "listener/{s}", .{std.fmt.bytesToHex(membership.connection, .lower)}))) return error.InvalidCatalogRecord;
                entry.value_ptr.minimum = @min(entry.value_ptr.minimum, membership.acknowledged.sequence);
                if (head.gc) |progress| if (progress.after) |after| {
                    if (std.mem.order(u8, &membership.connection, &after) != .gt) entry.value_ptr.scanned_minimum = @min(entry.value_ptr.scanned_minimum, membership.acknowledged.sequence);
                };
            } else if (std.mem.startsWith(u8, item, "slot/")) {
                if (item.len != 26 or item[21] != '/') return error.InvalidCatalogRecord;
                const sequence = try std.fmt.parseInt(u64, item[5..21], 16);
                if (!std.mem.eql(u8, item[5..21], try std.fmt.allocPrint(a, "{x:0>16}", .{sequence}))) return error.InvalidCatalogRecord;
                if (sequence < head.oldest or sequence >= head.next) return error.InvalidCatalogRecord;
                const block = try linked(N.Slot, a, reader, try std.fmt.allocPrint(a, "{s}{s}:slot/{x:0>16}/head", .{ prefix, std.fmt.bytesToHex(scope_id, .lower), sequence }));
                if (block.sequence != sequence) return error.InvalidCatalogRecord;
                if (std.mem.endsWith(u8, item, "/head")) {
                    const body = try reader.get(try std.fmt.allocPrint(a, "{s}{s}:slot/{x:0>16}/body", .{ prefix, std.fmt.bytesToHex(scope_id, .lower), sequence }));
                    _ = try N.validatePayload(a, block, body, true);
                    const reference = try linked(N.Ref, a, reader, try std.fmt.allocPrint(a, "{s}{s}:operation/{s}", .{ prefix, std.fmt.bytesToHex(scope_id, .lower), std.fmt.bytesToHex(block.operation, .lower) }));
                    if (!reference.committed or reference.sequence != sequence or !std.mem.eql(u8, &reference.transaction, &block.transaction) or !std.mem.eql(u8, &reference.digest, &block.digest)) return error.InvalidCatalogRecord;
                    entry.value_ptr.used = try std.math.add(u64, entry.value_ptr.used, block.bytes);
                    entry.value_ptr.blocks += 1;
                } else if (!std.mem.endsWith(u8, item, "/body")) return error.InvalidCatalogRecord;
            } else if (std.mem.startsWith(u8, item, "operation/")) {
                const reference = try record(N.Ref, a, row.value);
                if (item.len != 42 or reference.reserved > N.max_queue_bytes or std.mem.allEqual(u8, &reference.transaction, 0) or std.mem.allEqual(u8, &reference.digest, 0)) return error.InvalidCatalogRecord;
                var operation: [16]u8 = undefined;
                _ = std.fmt.hexToBytes(&operation, item[10..]) catch return error.InvalidCatalogRecord;
                if (!std.mem.eql(u8, item[10..], &std.fmt.bytesToHex(operation, .lower))) return error.InvalidCatalogRecord;
                var needs_owner = !reference.committed or reference.sequence == null;
                if (reference.sequence) |sequence| {
                    if (!reference.committed) return error.InvalidCatalogRecord;
                    const block = try linked(N.Slot, a, reader, try std.fmt.allocPrint(a, "{s}{s}:slot/{x:0>16}/head", .{ prefix, std.fmt.bytesToHex(scope_id, .lower), sequence }));
                    needs_owner = !block.ready;
                }
                const owner = try P.readOperation(a, reader, group, operation);
                if (needs_owner and owner == null) return error.InvalidCatalogRecord;
                if (owner) |value| {
                    if (!needs_owner or (reference.committed and (value.decision == null or value.decision.?.status != .committed)) or (!reference.committed and value.decision != null)) return error.InvalidCatalogRecord;
                    const plan = value.identity.notification_plan orelse return error.InvalidCatalogRecord;
                    const owned_scope = try N.decisionScope(a, group, scope.incarnation, value.identity);
                    if (!std.meta.eql(owned_scope, scope) or !std.mem.eql(u8, &value.identity.txn_id, &reference.transaction)) return error.InvalidCatalogRecord;
                    const encoded = try std.json.Stringify.valueAlloc(a, plan, .{});
                    var digest: [32]u8 = undefined;
                    std.crypto.hash.Blake3.hash(encoded, &digest, .{});
                    if (!std.mem.eql(u8, &digest, &reference.digest)) return error.InvalidCatalogRecord;
                }
                if (!reference.committed) entry.value_ptr.reserved = try std.math.add(u64, entry.value_ptr.reserved, reference.reserved);
            } else return error.InvalidCatalogRecord;
        }
    }
    var used: u64 = 0;
    var reserved: u64 = 0;
    var iterator = totals.valueIterator();
    while (iterator.next()) |value| {
        const head = value.head orelse return error.InvalidCatalogRecord;
        if (head.used != value.used or head.reserved != value.reserved or head.next - head.oldest != value.blocks) return error.InvalidCatalogRecord;
        if (head.gc) |progress| {
            const minimum = if (progress.deleting) value.minimum else value.scanned_minimum;
            if (progress.minimum > minimum) return error.InvalidCatalogRecord;
        }
        used = try std.math.add(u64, used, value.used);
        reserved = try std.math.add(u64, reserved, value.reserved);
    }
    if (budget) |value| {
        if (value.used != used or value.reserved != reserved) return error.InvalidCatalogRecord;
    } else if (totals.count() != 0) return error.InvalidCatalogRecord;
}
