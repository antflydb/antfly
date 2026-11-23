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

//! The metadata decision writer assigns addresses. A terminal handoff changes
//! availability at that address, so delayed owners cannot reorder commits.
const std = @import("std");
const publisher = @import("antfly_local_sources").system_catalog_logical_replication_publisher;
const A = std.mem.Allocator;
const subscription = @import("../../metadata/replication_subscription_work.zig");

pub const Write = struct { key: []const u8, value: []const u8 };
pub const Row = struct {
    entry: publisher.JournalEntry,
    ready: bool = false,
};

pub fn groupPrefix(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:logical_decision_journal:{x:0>16}:", .{group});
}

pub fn key(a: A, realm: publisher.Realm, suffix: []const u8) ![]const u8 {
    try realm.validate();
    var buffer: [96]u8 = undefined;
    return std.fmt.allocPrint(a, "{s}{s}:{x:0>16}:{s}", .{ try groupPrefix(&buffer, realm.group_id), realm.incarnation, realm.database_id, suffix });
}

fn publisherSystemKey(a: A, group: u64, incarnation: [32]u8) ![]const u8 {
    var buffer: [96]u8 = undefined;
    return std.fmt.allocPrint(a, "{s}publisher_system:{s}", .{ try groupPrefix(&buffer, group), incarnation });
}

pub const IdentityView = struct {
    identity: publisher.PublisherIdentity,
    lsn: u64,
};

pub fn readIdentity(a: A, txn: anytype, realm: publisher.Realm) !?IdentityView {
    try realm.validate();
    const system = (try readNumber(txn, try publisherSystemKey(a, realm.group_id, realm.incarnation))) orelse return null;
    if (system == 0) return error.InvalidCatalogRecord;
    const identity: publisher.PublisherIdentity = .{
        .realm = realm,
        .system_identifier = system,
        .timeline = 1,
        .database_oid = std.math.cast(u32, realm.database_id) orelse return error.InvalidLogicalPublisher,
    };
    return .{ .identity = identity, .lsn = try reservedHead(a, txn, realm) };
}

fn rowKey(a: A, realm: publisher.Realm, begin: u64) ![]const u8 {
    return key(a, realm, try std.fmt.allocPrint(a, "row/{x:0>16}", .{begin}));
}

fn operationKey(a: A, realm: publisher.Realm, operation: [16]u8) ![]const u8 {
    return key(a, realm, try std.fmt.allocPrint(a, "operation/{s}", .{std.fmt.bytesToHex(operation, .lower)}));
}

fn get(txn: anytype, address: []const u8) !?[]const u8 {
    return txn.get(address) catch |err| if (err == error.NotFound) null else err;
}

fn encodeNumber(a: A, number: u64) ![]const u8 {
    const raw = try a.alloc(u8, 8);
    std.mem.writeInt(u64, raw[0..8], number, .big);
    return raw;
}

fn readNumber(txn: anytype, address: []const u8) !?u64 {
    const raw = (try get(txn, address)) orelse return null;
    if (raw.len != 8) return error.InvalidCatalogRecord;
    return std.mem.readInt(u64, raw[0..8], .big);
}

const max_journal_row_bytes = 1024 * 1024;
const max_feed_scan_bytes = 4 * max_journal_row_bytes;

fn encodeRow(a: A, row: Row) ![]const u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, row, .{});
    if (bytes.len > max_journal_row_bytes) return error.CatalogCommandTooLarge;
    return bytes;
}

fn readRow(a: A, txn: anytype, realm: publisher.Realm, begin: u64) !?Row {
    const raw = (try get(txn, try rowKey(a, realm, begin))) orelse return null;
    return try decodeRow(a, realm, begin, raw);
}

fn decodeRow(a: A, realm: publisher.Realm, begin: u64, raw: []const u8) !Row {
    if (raw.len > max_journal_row_bytes) return error.InvalidCatalogRecord;
    const row = std.json.parseFromSliceLeaky(Row, a, raw, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCatalogRecord,
    };
    try row.entry.validate();
    if (!row.entry.transaction.realm.eql(realm) or row.entry.begin_lsn != begin or row.entry.end_lsn - begin != 2 or row.entry.xid != transactionXid(begin))
        return error.InvalidCatalogRecord;
    return row;
}

fn transactionXid(begin: u64) u32 {
    return @intCast((begin / 2) % (@as(u64, std.math.maxInt(u32)) - 2) + 3);
}

pub const Read = union(enum) { caught_up, pending, ready: publisher.JournalEntry };

/// The feed consults one exact address at a time. A pending predecessor fences
/// the stream even if a later owner already handed off.
pub fn readNext(a: A, txn: anytype, realm: publisher.Realm, begin: u64) !Read {
    const head = (try readNumber(txn, try key(a, realm, "head"))) orelse 1;
    if (begin == 0 or begin > head or (begin & 1) == 0 or (head & 1) == 0) return error.InvalidCatalogRecord;
    if (begin == head) return .caught_up;
    const row = (try readRow(a, txn, realm, begin)) orelse return error.InvalidCatalogRecord;
    if (row.entry.end_lsn > head) return error.InvalidCatalogRecord;
    return if (row.ready) .{ .ready = row.entry } else .pending;
}

pub fn reserve(a: A, txn: anytype, realm: publisher.Realm, record: anytype, commit_time_unix_us: u64) ![4]Write {
    const decision = record.decision orelse return error.InvalidCatalogRecord;
    if (decision.status != .committed or record.publication_captures.len == 0 or !record.was_prepared) return error.InvalidCatalogRecord;
    const index_key = try operationKey(a, realm, record.identity.operation_id);
    if (try get(txn, index_key) != null) return error.ExternalTransactionDecisionChanged;
    const head_key = try key(a, realm, "head");
    const begin = (try readNumber(txn, head_key)) orelse 1;
    if (begin == 0 or (begin & 1) == 0) return error.InvalidCatalogRecord;
    const end = std.math.add(u64, begin, 2) catch return error.CatalogRevisionExhausted;
    const address = try rowKey(a, realm, begin);
    if (try get(txn, address) != null) return error.InvalidCatalogRecord;

    const captures = try a.dupe(publisher.CaptureRef, record.publication_captures);
    std.mem.sort(publisher.CaptureRef, captures, {}, struct {
        fn less(_: void, left: publisher.CaptureRef, right: publisher.CaptureRef) bool {
            return left.participant < right.participant;
        }
    }.less);
    const Route = std.meta.Child(@FieldType(publisher.JournalEntry, "routes"));
    const routes = try a.alloc(Route, captures.len);
    for (captures, routes) |capture, *route| {
        const part = record.participants[capture.participant];
        if (!part.publication_capture.?.realm.eql(realm)) return error.InvalidCatalogRecord;
        route.* = .{ .participant = capture.participant, .table_name = part.table_name };
    }
    // Two logical positions represent the transaction span. XIDs wrap
    // independently of the 64-bit cursor and skip PostgreSQL-reserved IDs.
    const row: Row = .{ .entry = .{
        .xid = transactionXid(begin),
        .begin_lsn = begin,
        .end_lsn = end,
        .transaction = .{
            .owner = try record.identity.owner(a),
            .transaction_id = record.identity.txn_id,
            .realm = realm,
            .commit_version = decision.version,
            .commit_time_unix_us = commit_time_unix_us,
            .captures = captures,
        },
        .routes = routes,
    } };
    try row.entry.validate();
    return .{
        .{ .key = head_key, .value = try encodeNumber(a, end) },
        .{ .key = index_key, .value = try encodeNumber(a, begin) },
        .{ .key = address, .value = try encodeRow(a, row) },
        try planPendingChange(a, txn, realm, .reserve),
    };
}

pub fn handoff(a: A, txn: anytype, realm: publisher.Realm, record: anytype) ![2]Write {
    const index_key = try operationKey(a, realm, record.identity.operation_id);
    const begin = (try readNumber(txn, index_key)) orelse return error.CatalogPublicationProofPending;
    var row = (try readRow(a, txn, realm, begin)) orelse return error.InvalidCatalogRecord;
    const transaction = row.entry.transaction;
    const decision = record.decision orelse return error.InvalidCatalogRecord;
    if (row.ready or decision.status != .committed or transaction.commit_version != decision.version or
        !transaction.owner.eql(try record.identity.owner(a)) or !std.mem.eql(u8, &transaction.transaction_id, &record.identity.txn_id))
        return error.InvalidCatalogRecord;
    if (transaction.captures.len != record.publication_terminals.len) return error.CatalogPublicationProofPending;
    for (transaction.captures, record.publication_terminals) |capture, terminal| {
        try record.requirePublicationTerminal(a, terminal);
        if (!capture.sameLogical(terminal.capture_ref)) return error.ExternalTransactionDecisionChanged;
    }
    row.ready = true;
    return .{
        .{ .key = try rowKey(a, realm, begin), .value = try encodeRow(a, row) },
        try planPendingChange(a, txn, realm, .handoff),
    };
}

pub fn verifyPending(a: A, txn: anytype, realm: publisher.Realm, operation: [16]u8) !void {
    const begin = (try readNumber(txn, try operationKey(a, realm, operation))) orelse return error.InvalidCatalogRecord;
    const row = (try readRow(a, txn, realm, begin)) orelse return error.InvalidCatalogRecord;
    if (row.ready or !std.mem.eql(u8, &row.entry.transaction.owner.operation_id, &operation)) return error.InvalidCatalogRecord;
}

pub fn verifyKey(a: A, txn: anytype, group: u64, incarnation: [32]u8, tail: []const u8, prepared: type) !void {
    if (std.mem.startsWith(u8, tail, "publisher_system:")) {
        if (!std.mem.eql(u8, tail["publisher_system:".len..], &incarnation)) return error.InvalidCatalogRecord;
        const system = (try readNumber(txn, try publisherSystemKey(a, group, incarnation))) orelse return error.InvalidCatalogRecord;
        if (system == 0) return error.InvalidCatalogRecord;
        return;
    }
    if (std.mem.startsWith(u8, tail, "cleanup:")) {
        const cursor = try parseCollectionTail(incarnation, tail["cleanup:".len..]);
        const address = try collectionKey(a, group, incarnation, cursor);
        var prefix_buffer: [96]u8 = undefined;
        const prefix = try groupPrefix(&prefix_buffer, group);
        if (!std.mem.eql(u8, address[prefix.len..], tail)) return error.InvalidCatalogRecord;
        const raw = (try get(txn, address)) orelse return error.InvalidCatalogRecord;
        const permission = try decodeCollection(a, group, incarnation, cursor, raw);
        if (try prepared.readOperation(a, txn, group, permission.debt.reference.owner.operation_id) != null)
            return error.InvalidCatalogRecord;
        return;
    }
    var it = std.mem.splitScalar(u8, tail, ':');
    const identity = it.next() orelse return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, identity, &incarnation)) return error.InvalidCatalogRecord;
    const database = try exactHex(u64, it.next() orelse return error.InvalidCatalogRecord);
    const family = it.next() orelse return error.InvalidCatalogRecord;
    if (it.next() != null) return error.InvalidCatalogRecord;
    try verifyAddress(a, txn, .{ .group_id = group, .database_id = database, .incarnation = incarnation }, family, prepared);
}

fn exactHex(comptime T: type, bytes: []const u8) !T {
    if (bytes.len != @bitSizeOf(T) / 4) return error.InvalidCatalogRecord;
    for (bytes) |byte| {
        if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidCatalogRecord;
    }
    return std.fmt.parseInt(T, bytes, 16) catch error.InvalidCatalogRecord;
}

/// Check every durable address against its neighbors, reverse index and live
/// decision. This is a linear, order-independent check over the snapshot map.
pub fn verifyAddress(a: A, txn: anytype, realm: publisher.Realm, suffix: []const u8, prepared: type) !void {
    const lease_prefix = "subscription_lease/";
    if (std.mem.startsWith(u8, suffix, lease_prefix)) {
        const id = try exactHex(u64, suffix[lease_prefix.len..]);
        _ = (try subscription.observe(a, txn, realm.group_id, realm.incarnation, .{ .logical_subscription = .{ .database_id = realm.database_id, .subscription_id = id } })) orelse return error.InvalidCatalogRecord;
        return;
    }
    if (std.mem.startsWith(u8, suffix, "slot_control_")) return verifyControlAddress(a, txn, realm, suffix);
    if (std.mem.eql(u8, suffix, "slot_births") or std.mem.eql(u8, suffix, "slot_count") or std.mem.startsWith(u8, suffix, "slot/")) {
        const births = (try readNumber(txn, try key(a, realm, "slot_births"))) orelse return error.InvalidCatalogRecord;
        const count = (try readNumber(txn, try key(a, realm, "slot_count"))) orelse 0;
        if (births == 0 or count > births or count > max_slots_per_database or (count == 0 and std.mem.eql(u8, suffix, "slot_count")))
            return error.InvalidCatalogRecord;
        if (std.mem.startsWith(u8, suffix, "slot/")) {
            const slot = (try slotByName(a, txn, realm, suffix["slot/".len..])) orelse return error.InvalidCatalogRecord;
            if (count == 0 or slot.reference.birth > births or slot.offered_lsn > try reservedHead(a, txn, realm) or
                (slot.confirmed_flush_lsn & 1) == 0 or (slot.offered_lsn & 1) == 0 or
                (slot.lease == null and slot.offered_lsn != slot.confirmed_flush_lsn)) return error.InvalidCatalogRecord;
        }
        return;
    }
    const head = (try readNumber(txn, try key(a, realm, "head"))) orelse return error.InvalidCatalogRecord;
    if (head < 3 or (head & 1) == 0) return error.InvalidCatalogRecord;
    if (std.mem.eql(u8, suffix, "pending")) {
        const count = (try readNumber(txn, try key(a, realm, suffix))) orelse return error.InvalidCatalogRecord;
        if (count > head / 2) return error.InvalidCatalogRecord;
        return;
    }
    if (std.mem.eql(u8, suffix, "head")) {
        if (try get(txn, try rowKey(a, realm, head - 2)) == null) return error.InvalidCatalogRecord;
        return;
    }
    if (std.mem.startsWith(u8, suffix, "operation/")) {
        var encoded: [16]u8 = undefined;
        std.mem.writeInt(u128, &encoded, try exactHex(u128, suffix["operation/".len..]), .big);
        const begin = (try readNumber(txn, try operationKey(a, realm, encoded))) orelse return error.InvalidCatalogRecord;
        const row = (try readRow(a, txn, realm, begin)) orelse return error.InvalidCatalogRecord;
        if (!std.mem.eql(u8, &row.entry.transaction.owner.operation_id, &encoded)) return error.InvalidCatalogRecord;
        return;
    }
    if (!std.mem.startsWith(u8, suffix, "row/")) return error.InvalidCatalogRecord;
    const begin = try exactHex(u64, suffix["row/".len..]);
    if (begin == 0 or (begin & 1) == 0 or begin >= head) return error.InvalidCatalogRecord;
    if (begin > 1 and try get(txn, try rowKey(a, realm, begin - 2)) == null) return error.InvalidCatalogRecord;
    const row = (try readRow(a, txn, realm, begin)) orelse return error.InvalidCatalogRecord;
    const transaction = row.entry.transaction;
    const index = try readNumber(txn, try operationKey(a, realm, transaction.owner.operation_id));
    const live = try prepared.readOperation(a, txn, realm.group_id, transaction.owner.operation_id);
    if (row.ready) {
        if (index == null or index.? != begin or live != null) return error.InvalidCatalogRecord;
        return;
    }
    const record = live orelse return error.InvalidCatalogRecord;
    const decision = record.decision orelse return error.InvalidCatalogRecord;
    if (index == null or index.? != begin or record.identity.database_id != realm.database_id or
        decision.status != .committed or decision.version != transaction.commit_version or
        !transaction.owner.eql(try record.identity.owner(a)) or !std.mem.eql(u8, &transaction.transaction_id, &record.identity.txn_id) or
        transaction.captures.len != record.publication_captures.len)
        return error.InvalidCatalogRecord;
    for (transaction.captures, row.entry.routes) |capture, route| {
        if (capture.participant >= record.participants.len or !std.mem.eql(u8, route.table_name, record.participants[capture.participant].table_name))
            return error.InvalidCatalogRecord;
        for (record.publication_captures) |accepted| {
            if (capture.sameLogical(accepted)) break;
        } else return error.InvalidCatalogRecord;
    }
}

/// ABORT consumes no stream address. Retain custody of every captured body
/// in the same write that removes the live metadata decision.
pub fn handoffAbort(a: A, txn: anytype, realm: publisher.Realm, record: anytype, writes: *std.ArrayListUnmanaged(Write)) !void {
    const decision = record.decision orelse return error.InvalidCatalogRecord;
    if (decision.status != .aborted or record.publication_terminals.len + record.publication_abort_fences.len != record.publication_capacities.len)
        return error.CatalogPublicationProofPending;
    for (record.publication_terminals) |terminal| {
        try record.requirePublicationTerminal(a, terminal);
        const ref = terminal.capture_ref;
        const address = try collectionKey(a, realm.group_id, realm.incarnation, CollectionCursor.fromDebt(realm, ref));
        if (try get(txn, address) != null) return error.InvalidCatalogRecord;
        const debt: publisher.GcDebt = .{
            .reference = ref,
            .table_name = record.participants[ref.participant].table_name,
            .reason = .{ .aborted = decision.version },
            .decision_version = decision.version,
        };
        try debt.validate();
        try writes.append(a, .{ .key = address, .value = try std.json.Stringify.valueAlloc(a, CollectionRecord{ .debt = debt }, .{}) });
    }
}

pub const CollectionCursor = struct {
    database: u64,
    operation: [16]u8,
    member: u16,

    fn fromDebt(realm: publisher.Realm, ref: publisher.CaptureRef) @This() {
        return .{ .database = realm.database_id, .operation = ref.owner.operation_id, .member = ref.participant };
    }
};

pub const CollectionRead = union(enum) {
    next: ?CollectionCursor,
    authorize: publisher.GcPermission,
};
pub const CollectionWork = struct { cursor: CollectionCursor, permission: publisher.GcPermission };
const CollectionRecord = struct { debt: publisher.GcDebt, purged: ?publisher.GcObservation = null };

fn collectionPrefix(buffer: []u8, group: u64) ![]const u8 {
    const n = (try groupPrefix(buffer, group)).len;
    const suffix = try std.fmt.bufPrint(buffer[n..], "cleanup:", .{});
    return buffer[0 .. n + suffix.len];
}

fn collectionKey(a: A, group: u64, incarnation: [32]u8, cursor: CollectionCursor) ![]const u8 {
    if (cursor.database == 0 or cursor.member >= publisher.max_capture_participants or std.mem.allEqual(u8, &cursor.operation, 0)) return error.InvalidCatalogRecord;
    var buffer: [96]u8 = undefined;
    return std.fmt.allocPrint(a, "{s}{s}:{x:0>16}:{s}/{x:0>4}", .{
        try collectionPrefix(&buffer, group),
        incarnation,
        cursor.database,
        std.fmt.bytesToHex(cursor.operation, .lower),
        cursor.member,
    });
}

fn parseCollectionTail(incarnation: [32]u8, tail: []const u8) !CollectionCursor {
    var it = std.mem.tokenizeAny(u8, tail, ":/");
    const identity = it.next() orelse return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, identity, &incarnation)) return error.InvalidCatalogRecord;
    const database = try exactHex(u64, it.next() orelse return error.InvalidCatalogRecord);
    const operation = try exactHex(u128, it.next() orelse return error.InvalidCatalogRecord);
    const member = try exactHex(u16, it.next() orelse return error.InvalidCatalogRecord);
    if (it.next() != null) return error.InvalidCatalogRecord;
    var encoded: [16]u8 = undefined;
    std.mem.writeInt(u128, &encoded, operation, .big);
    return .{ .database = database, .operation = encoded, .member = member };
}

fn decodeCollection(a: A, group: u64, incarnation: [32]u8, cursor: CollectionCursor, raw: []const u8) !publisher.GcPermission {
    if (raw.len > 32 * 1024) return error.InvalidCatalogRecord;
    const value = try std.json.parseFromSliceLeaky(CollectionRecord, a, raw, .{ .allocate = .alloc_always });
    var permission: publisher.GcPermission = .{
        .realm = .{ .group_id = group, .incarnation = incarnation, .database_id = cursor.database },
        .debt = value.debt,
        .action = .purge,
    };
    try permission.validate();
    if (value.debt.reason != .aborted or !std.meta.eql(cursor, CollectionCursor.fromDebt(permission.realm, value.debt.reference))) return error.InvalidCatalogRecord;
    if (value.purged) |purged| {
        try purged.require(permission, a);
        permission.action = .clear_marker;
        permission.purged = purged;
        permission.purged_digest = try purged.digest(a);
    }
    return permission;
}

pub fn nextCollection(a: A, txn: anytype, group: u64, incarnation: [32]u8, after: ?CollectionCursor) !?CollectionWork {
    var buffer: [96]u8 = undefined;
    const prefix = try collectionPrefix(&buffer, group);
    const start = if (after) |cursor| try collectionKey(a, group, incarnation, cursor) else prefix;
    var scan = try txn.openCursor();
    defer scan.close();
    var entry = try scan.seekAtOrAfter(start);
    if (entry != null and after != null and std.mem.eql(u8, entry.?.key, start)) entry = try scan.next();
    const row = entry orelse return null;
    if (!std.mem.startsWith(u8, row.key, prefix)) return null;
    const cursor = try parseCollectionTail(incarnation, row.key[prefix.len..]);
    if (!std.mem.eql(u8, row.key, try collectionKey(a, group, incarnation, cursor))) return error.InvalidCatalogRecord;
    return .{ .cursor = cursor, .permission = try decodeCollection(a, group, incarnation, cursor, row.value) };
}

pub fn authorizeCollection(a: A, txn: anytype, group: u64, incarnation: [32]u8, request: publisher.GcPermission) !?publisher.GcPermission {
    try request.validate();
    if (request.realm.group_id != group or !std.mem.eql(u8, &request.realm.incarnation, &incarnation)) return null;
    const cursor = CollectionCursor.fromDebt(request.realm, request.debt.reference);
    const raw = (try get(txn, try collectionKey(a, group, incarnation, cursor))) orelse return null;
    const current = try decodeCollection(a, group, incarnation, cursor, raw);
    if (!std.mem.eql(u8, &try request.digest(a), &try current.digest(a))) return null;
    return current;
}

/// Called in the accepting metadata writer after verifying the current owner.
pub fn acceptCollection(a: A, txn: anytype, permission: publisher.GcPermission, evidence: publisher.GcObservation) !void {
    const realm = permission.realm;
    const canonical = (try authorizeCollection(a, txn, realm.group_id, realm.incarnation, permission)) orelse return;
    try evidence.require(canonical, a);
    const cursor = CollectionCursor.fromDebt(realm, canonical.debt.reference);
    const address = try collectionKey(a, realm.group_id, realm.incarnation, cursor);
    switch (canonical.action) {
        .purge => {
            const record: CollectionRecord = .{ .debt = canonical.debt, .purged = evidence };
            try txn.put(address, try std.json.Stringify.valueAlloc(a, record, .{}));
        },
        .clear_marker => try txn.delete(address),
    }
}

const max_slot_lease_ns = 60 * std.time.ns_per_s;

const PendingChange = enum { reserve, handoff };
fn planPendingChange(a: A, txn: anytype, realm: publisher.Realm, change: PendingChange) !Write {
    const address = try key(a, realm, "pending");
    const count = (try readNumber(txn, address)) orelse 0;
    const next = switch (change) {
        .reserve => std.math.add(u64, count, 1) catch return error.CatalogRevisionExhausted,
        .handoff => std.math.sub(u64, count, 1) catch return error.InvalidCatalogRecord,
    };
    return .{ .key = address, .value = try encodeNumber(a, next) };
}

pub fn snapshotPoint(a: A, txn: anytype, realm: publisher.Realm) !u64 {
    if (((try readNumber(txn, try key(a, realm, "pending"))) orelse 0) != 0)
        return error.SqlSnapshotCaptureChanged;
    return reservedHead(a, txn, realm);
}

fn reservedHead(a: A, txn: anytype, realm: publisher.Realm) !u64 {
    const head = (try readNumber(txn, try key(a, realm, "head"))) orelse 1;
    if ((head & 1) == 0) return error.InvalidCatalogRecord;
    return head;
}

fn slotKey(a: A, realm: publisher.Realm, name: []const u8) ![]const u8 {
    try publisher.validateSlotName(name);
    return key(a, realm, try std.fmt.allocPrint(a, "slot/{s}", .{name}));
}

fn slotByName(a: A, txn: anytype, realm: publisher.Realm, name: []const u8) !?publisher.Slot {
    const raw = (try get(txn, try slotKey(a, realm, name))) orelse return null;
    if (raw.len > 16 * 1024) return error.InvalidCatalogRecord;
    const slot = try std.json.parseFromSliceLeaky(publisher.Slot, a, raw, .{ .allocate = .alloc_always });
    try slot.validate();
    if (slot.reference.database_id != realm.database_id or !std.mem.eql(u8, slot.reference.name, name)) return error.InvalidCatalogRecord;
    return slot;
}

pub const SlotView = struct {
    control_head: u64,
    slot: ?publisher.Slot,
};

pub fn readSlotView(a: A, txn: anytype, realm: publisher.Realm, name: []const u8) !SlotView {
    return .{
        .control_head = try controlHead(a, txn, realm),
        .slot = try slotByName(a, txn, realm, name),
    };
}

fn exactSlot(a: A, txn: anytype, realm: publisher.Realm, ref: publisher.SlotRef) !publisher.Slot {
    const slot = (try slotByName(a, txn, realm, ref.name)) orelse return error.ReplicationSlotNotFound;
    if (!slot.reference.eql(ref)) return error.ReplicationSlotLeaseChanged;
    return slot;
}

fn writeSlot(a: A, txn: anytype, realm: publisher.Realm, slot: publisher.Slot) !void {
    try slot.validate();
    try txn.put(try slotKey(a, realm, slot.reference.name), try std.json.Stringify.valueAlloc(a, slot, .{}));
}

pub fn createSlot(a: A, txn: anytype, realm: publisher.Realm, request: publisher.CreateSlot) !publisher.Slot {
    if (request.database_id != realm.database_id or std.mem.allEqual(u8, &request.connection, 0) or
        std.mem.allEqual(u8, &request.nonce, 0)) return error.InvalidLogicalPublisher;
    if (request.snapshot != .nothing) return error.UnsupportedOperation;
    if (try slotByName(a, txn, realm, request.name) != null) return error.ReplicationSlotAlreadyExists;
    const count_key = try key(a, realm, "slot_count");
    const count = (try readNumber(txn, count_key)) orelse 0;
    if (count >= max_slots_per_database) return error.ReplicationSlotCapacityExceeded;
    const births = try key(a, realm, "slot_births");
    const birth = std.math.add(u64, (try readNumber(txn, births)) orelse 0, 1) catch return error.CatalogRevisionExhausted;
    const start = if (request.expected_cut) |cut| point: {
        const current = try snapshotPoint(a, txn, realm);
        if (current != cut.lsn) return error.SqlSnapshotCaptureChanged;
        break :point current;
    } else try reservedHead(a, txn, realm);
    const slot: publisher.Slot = .{
        .reference = .{ .database_id = realm.database_id, .name = request.name, .birth = birth },
        .temporary_connection = if (request.temporary) request.connection else null,
        .restart_lsn = start,
        .confirmed_flush_lsn = start,
        .applied_lsn = 0,
        .offered_lsn = start,
    };
    try writeSlot(a, txn, realm, slot);
    try txn.put(births, try encodeNumber(a, birth));
    try txn.put(count_key, try encodeNumber(a, count + 1));
    return slot;
}

pub fn acquireSlot(a: A, txn: anytype, realm: publisher.Realm, request: @FieldType(publisher.Control, "acquire")) !publisher.Slot {
    var slot = try exactSlot(a, txn, realm, request.slot);
    if (request.database_id != realm.database_id or request.observed_at_unix_ns == 0 or
        request.expires_at_unix_ns <= request.observed_at_unix_ns or
        request.expires_at_unix_ns - request.observed_at_unix_ns > max_slot_lease_ns)
        return error.InvalidLogicalPublisher;
    if (slot.temporary_connection) |owner| if (!std.mem.eql(u8, &owner, &request.connection)) return error.ReplicationSlotActive;
    if (slot.lease) |lease| {
        if (request.observed_at_unix_ns < slot.lease_observed_at_unix_ns) return error.ReplicationSlotLeaseChanged;
        if (request.observed_at_unix_ns < slot.lease_expires_at_unix_ns) {
            if (!std.mem.eql(u8, &lease.connection, &request.connection) or !std.mem.eql(u8, &lease.nonce, &request.nonce))
                return error.ReplicationSlotActive;
            if (request.expires_at_unix_ns < slot.lease_expires_at_unix_ns) return error.ReplicationSlotLeaseChanged;
            slot.lease_expires_at_unix_ns = request.expires_at_unix_ns;
            slot.lease_observed_at_unix_ns = request.observed_at_unix_ns;
            try writeSlot(a, txn, realm, slot);
            return slot;
        }
    }
    slot.lease_highwater = std.math.add(u64, slot.lease_highwater, 1) catch return error.CatalogRevisionExhausted;
    slot.lease = .{ .slot = slot.reference, .generation = slot.lease_highwater, .connection = request.connection, .nonce = request.nonce };
    slot.lease_observed_at_unix_ns = request.observed_at_unix_ns;
    slot.lease_expires_at_unix_ns = request.expires_at_unix_ns;
    slot.offered_lsn = slot.confirmed_flush_lsn;
    try writeSlot(a, txn, realm, slot);
    return slot;
}

fn requireLease(slot: publisher.Slot, lease: publisher.Lease, now: u64) !void {
    try lease.validate();
    if (slot.lease == null or !slot.lease.?.eql(lease) or now < slot.lease_observed_at_unix_ns or now >= slot.lease_expires_at_unix_ns)
        return error.ReplicationSlotLeaseChanged;
}

pub const FeedProbe = struct { realm: publisher.Realm, lease: publisher.Lease, after: u64 };
pub const FeedReadiness = struct { control_head: u64, available: bool };

pub fn feedReadiness(a: A, txn: anytype, input: FeedProbe, now: u64) !FeedReadiness {
    const slot = try exactSlot(a, txn, input.realm, input.lease.slot);
    try requireLease(slot, input.lease, now);
    if (input.after < slot.confirmed_flush_lsn or input.after > slot.offered_lsn)
        return error.InvalidReplicationAcknowledgement;
    return .{
        .control_head = try controlHead(a, txn, input.realm),
        .available = (try readNext(a, txn, input.realm, input.after)) == .ready,
    };
}

pub fn offerFeed(a: A, txn: anytype, realm: publisher.Realm, request: publisher.FeedRequest, publications: []const publisher.Publication) !publisher.FeedPage {
    var slot = try exactSlot(a, txn, realm, request.lease.slot);
    try requireLease(slot, request.lease, request.observed_at_unix_ns);
    if (request.protocol_version != 1 or request.limit == 0 or request.limit > publisher.max_feed_page or
        publications.len == 0 or publications.len != request.publication_names.len or publications.len > @import("antfly_local_sources").system_catalog_logical_replication.max_members)
        return error.InvalidLogicalPublisher;
    if (request.after < slot.confirmed_flush_lsn or request.after > slot.offered_lsn or (request.after & 1) == 0)
        return error.InvalidReplicationAcknowledgement;
    const head = try reservedHead(a, txn, realm);
    if (slot.offered_lsn > head) return error.InvalidCatalogRecord;
    const entries = try a.alloc(publisher.JournalEntry, request.limit);
    var count: usize = 0;
    var after = request.after;
    var scanned: usize = 0;
    for (0..request.limit) |_| {
        if (after == head) break;
        const raw = (try get(txn, try rowKey(a, realm, after))) orelse return error.InvalidCatalogRecord;
        if (raw.len > max_journal_row_bytes) return error.InvalidCatalogRecord;
        if (raw.len > max_feed_scan_bytes - scanned) break;
        scanned += raw.len;
        const row = try decodeRow(a, realm, after, raw);
        if (row.entry.end_lsn > head) return error.InvalidCatalogRecord;
        if (!row.ready) break;
        after = row.entry.end_lsn;
        entries[count] = row.entry;
        count += 1;
    }
    if (after > slot.offered_lsn) {
        slot.offered_lsn = after;
        slot.lease_observed_at_unix_ns = request.observed_at_unix_ns;
        try writeSlot(a, txn, realm, slot);
    }
    // Even empty and filtered pages report only the complete offered boundary.
    return .{ .realm = realm, .lease = request.lease, .head_lsn = after, .offered_lsn = after, .entries = entries[0..count], .publications = publications };
}

pub fn acknowledgeFeed(a: A, txn: anytype, realm: publisher.Realm, request: publisher.Acknowledge) !publisher.Slot {
    var slot = try exactSlot(a, txn, realm, request.lease.slot);
    try requireLease(slot, request.lease, request.observed_at_unix_ns);
    if (request.applied > request.flushed or request.flushed > request.received or request.received > slot.offered_lsn)
        return error.InvalidReplicationAcknowledgement;
    if (request.flushed == 0 or
        (request.flushed == slot.confirmed_flush_lsn and request.applied == slot.applied_lsn)) return slot;
    if (request.flushed < slot.confirmed_flush_lsn or (request.flushed & 1) == 0 or request.applied < slot.applied_lsn)
        return error.InvalidReplicationAcknowledgement;
    slot.confirmed_flush_lsn = request.flushed;
    slot.restart_lsn = request.flushed;
    slot.applied_lsn = request.applied;
    slot.lease_observed_at_unix_ns = request.observed_at_unix_ns;
    try writeSlot(a, txn, realm, slot);
    return slot;
}

pub fn authorizeFeedCapture(a: A, txn: anytype, request: publisher.OutboxRead, now: u64) !bool {
    try request.validate();
    const realm = request.realm;
    const slot = try exactSlot(a, txn, realm, request.lease.slot);
    try requireLease(slot, request.lease, now);
    const begin = (try readNumber(txn, try operationKey(a, realm, request.reference.owner.operation_id))) orelse return false;
    if (begin < slot.restart_lsn or begin >= slot.offered_lsn) return false;
    const row = (try readRow(a, txn, realm, begin)) orelse return error.InvalidCatalogRecord;
    if (!row.ready or row.entry.end_lsn > slot.offered_lsn) return false;
    for (row.entry.transaction.captures, row.entry.routes) |ref, route| {
        if (ref.sameLogical(request.reference) and std.mem.eql(u8, route.table_name, request.table_name)) return true;
    }
    return false;
}

fn clearLease(slot: *publisher.Slot) void {
    slot.lease = null;
    slot.lease_observed_at_unix_ns = 0;
    slot.lease_expires_at_unix_ns = 0;
    slot.offered_lsn = slot.confirmed_flush_lsn;
}

pub fn releaseSlot(a: A, txn: anytype, realm: publisher.Realm, lease: publisher.Lease) !void {
    var slot = try exactSlot(a, txn, realm, lease.slot);
    if (slot.lease == null or !slot.lease.?.eql(lease)) return error.ReplicationSlotLeaseChanged;
    clearLease(&slot);
    try writeSlot(a, txn, realm, slot);
}

pub fn expireSlot(a: A, txn: anytype, realm: publisher.Realm, request: @FieldType(publisher.Control, "expire")) !void {
    var slot = try exactSlot(a, txn, realm, request.lease.slot);
    if (slot.lease == null or !slot.lease.?.eql(request.lease) or slot.lease_expires_at_unix_ns != request.expected_expiry or
        request.observed_at_unix_ns < slot.lease_expires_at_unix_ns) return error.ReplicationSlotLeaseChanged;
    if (slot.temporary_connection != null) return removeSlot(a, txn, realm, slot.reference.name);
    clearLease(&slot);
    try writeSlot(a, txn, realm, slot);
}

pub fn dropSlot(a: A, txn: anytype, realm: publisher.Realm, request: @FieldType(publisher.Control, "drop_slot")) !void {
    if (request.database_id != realm.database_id) return error.InvalidLogicalPublisher;
    const slot = try exactSlot(a, txn, realm, request.slot);
    if (slot.lease != null) return error.ReplicationSlotActive;
    try removeSlot(a, txn, realm, slot.reference.name);
}

const max_slots_per_database = 1024;

fn removeSlot(a: A, txn: anytype, realm: publisher.Realm, name: []const u8) !void {
    const count_key = try key(a, realm, "slot_count");
    const count = (try readNumber(txn, count_key)) orelse return error.InvalidCatalogRecord;
    if (count == 0 or count > max_slots_per_database) return error.InvalidCatalogRecord;
    try txn.delete(try slotKey(a, realm, name));
    if (count == 1) try txn.delete(count_key) else try txn.put(count_key, try encodeNumber(a, count - 1));
}

/// One ordered attempt. The issuing writer checks the authenticated database
/// and authors time before submitting this immutable frame through Raft.
pub const ControlRequest = struct {
    pub fn minimumDecoderVersion(self: @This()) u16 {
        const protocol = @import("../../metadata/topology_protocol.zig");
        return if (self.action == .finish_subscription_disable or self.action == .finish_subscription_drop or self.action == .bind_subscription_export or self.action == .transition_subscription_export or
            (self.action == .start_subscription and self.action.start_subscription.provider_preflight_identity != null))
            protocol.sql_subscription_apply_version
        else
            protocol.sql_publication_slot_control_version;
    }
    realm: publisher.Realm,
    nonce: [16]u8,
    observed_head: u64,
    action: publisher.Control,

    pub fn validate(self: @This()) !void {
        try self.realm.validate();
        if (std.mem.allEqual(u8, &self.nonce, 0)) return error.InvalidLogicalPublisher;
    }

    fn digest(self: @This(), a: A) ![32]u8 {
        try self.validate();
        return @import("antfly_local_sources").system_catalog_logical_replication_capture.canonicalDigest(a, "antfly/logical-slot-control/v1\x00", self);
    }
};

pub const OfferedWindow = struct {
    after: u64,
    end: u64,
    publications: []const publisher.Publication,
};

pub const ControlRefusal = enum {
    InvalidLogicalPublisher,
    InvalidReplicationAcknowledgement,
    ReplicationSlotActive,
    ReplicationSlotAlreadyExists,
    ReplicationSlotNotFound,
    ReplicationSlotLeaseChanged,
    ReplicationSlotCapacityExceeded,
    PublicationNotFound,
    SqlSnapshotCaptureChanged,
    InvalidReplicationCutoverIntent,
    ReplicationCutoverAuthorityLost,
    LogicalReplicationLifecyclePending,
    ForeignProviderIdentityMismatch,
    UnsupportedOperation,
};

pub const ControlResult = union(enum) {
    none,
    identity: IdentityView,
    subscription_lease: subscription.Lease,
    slot: publisher.Slot,
    offered: OfferedWindow,
    refused: ControlRefusal,
};

const controls_retained: u64 = 64;
// The maximum publication set fits even with decimal-byte JSON digests.
const max_control_receipt_bytes = 1024 * 1024;
const ControlReceipt = struct {
    ordinal: u64,
    nonce: [16]u8,
    digest: [32]u8,
    result: ControlResult,
};

pub fn controlHead(a: A, txn: anytype, realm: publisher.Realm) !u64 {
    return (try readNumber(txn, try key(a, realm, "slot_control_head"))) orelse 0;
}

fn controlNonceKey(a: A, realm: publisher.Realm, nonce: [16]u8) ![]const u8 {
    return key(a, realm, try std.fmt.allocPrint(a, "slot_control_nonce/{s}", .{std.fmt.bytesToHex(nonce, .lower)}));
}

fn controlRingKey(a: A, realm: publisher.Realm, ordinal: u64) ![]const u8 {
    return key(a, realm, try std.fmt.allocPrint(a, "slot_control_ring/{x:0>2}", .{ordinal % controls_retained}));
}

fn decodeControlReceipt(a: A, raw: []const u8) !ControlReceipt {
    if (raw.len > max_control_receipt_bytes) return error.InvalidCatalogRecord;
    const receipt = try std.json.parseFromSliceLeaky(ControlReceipt, a, raw, .{ .allocate = .alloc_always });
    if (receipt.ordinal == 0 or std.mem.allEqual(u8, &receipt.nonce, 0) or std.mem.allEqual(u8, &receipt.digest, 0))
        return error.InvalidCatalogRecord;
    switch (receipt.result) {
        .identity => |view| {
            try view.identity.validate();
            if (view.lsn == 0 or (view.lsn & 1) == 0) return error.InvalidCatalogRecord;
        },
        .subscription_lease => |lease| try lease.validate(),
        .slot => |slot| try slot.validate(),
        .offered => |window| if (window.after == 0 or (window.after & 1) == 0 or (window.end & 1) == 0 or window.end < window.after or window.end - window.after > 2 * publisher.max_feed_page or window.publications.len == 0)
            return error.InvalidCatalogRecord,
        .none, .refused => {},
    }
    return receipt;
}

pub fn readControlOutcome(a: A, txn: anytype, request: ControlRequest) !?ControlResult {
    const digest = try request.digest(a);
    const realm = request.realm;
    const ordinal = (try readNumber(txn, try controlNonceKey(a, realm, request.nonce))) orelse return null;
    const raw = (try get(txn, try controlRingKey(a, realm, ordinal))) orelse return error.InvalidCatalogRecord;
    const receipt = try decodeControlReceipt(a, raw);
    const head = try controlHead(a, txn, realm);
    if (receipt.ordinal != ordinal or !std.mem.eql(u8, &receipt.nonce, &request.nonce) or ordinal > head or head - ordinal >= controls_retained) return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, &receipt.digest, &digest)) return error.SequenceAdmissionConflict;
    if (receipt.result == .identity) try verifyIdentityReceipt(a, txn, realm, receipt.result.identity);
    return receipt.result;
}

fn verifyIdentityReceipt(a: A, txn: anytype, realm: publisher.Realm, offered: IdentityView) !void {
    const retained = (try readIdentity(a, txn, realm)) orelse return error.InvalidCatalogRecord;
    if (!std.meta.eql(retained.identity, offered.identity) or offered.lsn > retained.lsn) return error.InvalidCatalogRecord;
}

fn ControlOverlay(comptime Reader: type) type {
    return struct {
        backing: Reader,
        alloc: A,
        writes: std.ArrayListUnmanaged(Write) = .empty,
        deletes: std.ArrayListUnmanaged([]const u8) = .empty,

        pub fn get(self: *@This(), address: []const u8) ![]const u8 {
            for (self.writes.items) |write| if (std.mem.eql(u8, write.key, address)) return write.value;
            for (self.deletes.items) |removed| if (std.mem.eql(u8, removed, address)) return error.NotFound;
            return self.backing.get(address);
        }

        pub fn put(self: *@This(), address: []const u8, value: []const u8) !void {
            try self.writes.append(self.alloc, .{ .key = address, .value = value });
        }

        pub fn delete(self: *@This(), address: []const u8) !void {
            try self.deletes.append(self.alloc, address);
        }
    };
}

pub const ControlPlan = struct {
    result: ControlResult,
    writes: []const Write = &.{},
    deletes: []const []const u8 = &.{},

    pub fn apply(self: @This(), txn: anytype) !void {
        for (self.writes) |write| try txn.put(write.key, write.value);
        for (self.deletes) |address| try txn.delete(address);
    }
};

fn executeControl(a: A, txn: anytype, request: ControlRequest, publications: anyerror![]const publisher.Publication) !ControlResult {
    const realm = request.realm;
    const stamps = try publications;
    switch (request.action) {
        .finish_subscription_drop => |input| {
            try subscription.requireDatabase(input.item, realm.database_id);
            const definition = subscription.cleanupDefinition(a, txn, realm.group_id, realm.incarnation, input.item, false) catch |err| switch (err) {
                error.Forbidden => return error.ReplicationCutoverAuthorityLost,
                else => return err,
            };
            if (definition.checkpoint != input.checkpoint) return error.ReplicationCutoverAuthorityLost;
            return .none;
        },
        .finish_subscription_disable => |input| {
            try subscription.requireDatabase(input.item, realm.database_id);
            if (input.checkpoint) |checkpoint| {
                try subscription.finishDisableExport(a, txn, realm.group_id, realm.incarnation, input.item, checkpoint);
            } else {
                try subscription.finishDisable(a, txn, realm.group_id, realm.incarnation, input.item);
            }
            return .none;
        },
        .claim_subscription => |input| {
            try subscription.requireDatabase(input.item, realm.database_id);
            return .{ .subscription_lease = try subscription.claim(a, txn, realm.group_id, realm.incarnation, input.item, input.worker, input.observed_at_unix_ns, input.expires_at_unix_ns) };
        },
        .transition_subscription_export => |input| {
            try subscription.requireDatabase(input.lease.item, realm.database_id);
            return .{ .subscription_lease = try subscription.transitionExport(a, txn, realm.group_id, realm.incarnation, input.lease, input.action, input.observed_at_unix_ns) };
        },
        .bind_subscription_export => |input| {
            try subscription.requireDatabase(input.lease.item, realm.database_id);
            return .{ .subscription_lease = try subscription.bindExport(a, txn, realm.group_id, realm.incarnation, input.lease, input.attempt, input.origin, input.receipt, input.observed_at_unix_ns) };
        },
        .bind_subscription_origin => |input| {
            try subscription.requireDatabase(input.lease.item, realm.database_id);
            return .{ .subscription_lease = try subscription.bindOrigin(a, txn, realm.group_id, realm.incarnation, input.lease, input.attempt, input.origin, input.observed_at_unix_ns) };
        },
        .start_subscription => |input| {
            try subscription.requireDatabase(input.lease.item, realm.database_id);
            // A retained reply proves intent custody. The worker must check
            // that attempt against the provider before any external effect.
            return .{ .subscription_lease = try subscription.start(a, txn, realm.group_id, realm.incarnation, input.lease, input.attempt, input.provider_preflight_identity, input.observed_at_unix_ns) };
        },
        .initialize => |candidate| {
            if (candidate == 0 or realm.database_id > std.math.maxInt(u32)) return error.InvalidLogicalPublisher;
            if (try readIdentity(a, txn, realm)) |view| return .{ .identity = view };
            try txn.put(try publisherSystemKey(a, realm.group_id, realm.incarnation), try encodeNumber(a, candidate));
            return .{ .identity = (try readIdentity(a, txn, realm)).? };
        },
        .create_slot => |input| return .{ .slot = try createSlot(a, txn, realm, input) },
        .acquire => |input| return .{ .slot = try acquireSlot(a, txn, realm, input) },
        .acknowledge => |input| return .{ .slot = try acknowledgeFeed(a, txn, realm, input) },
        .feed => |input| {
            const page = try offerFeed(a, txn, realm, input, stamps);
            return .{ .offered = .{ .after = input.after, .end = page.offered_lsn, .publications = page.publications } };
        },
        .drop_slot => |input| try dropSlot(a, txn, realm, input),
        .release => |input| try releaseSlot(a, txn, realm, input),
        .expire => |input| try expireSlot(a, txn, realm, input),
    }
    return .none;
}

/// Plan effects and the original replay response together. The caller has
/// already joined the publication stamps to the metadata catalog cut.
pub fn planControl(a: A, txn: anytype, request: ControlRequest, publications: anyerror![]const publisher.Publication) !ControlPlan {
    if (try readControlOutcome(a, txn, request)) |result| return .{ .result = result };
    const realm = request.realm;
    const head = try controlHead(a, txn, realm);
    if (request.observed_head > head or head - request.observed_head >= controls_retained)
        return error.SequenceAdmissionExpired;
    const ordinal = std.math.add(u64, head, 1) catch return error.CatalogRevisionExhausted;
    var overlay: ControlOverlay(@TypeOf(txn)) = .{ .backing = txn, .alloc = a };
    const result: ControlResult = executeControl(a, &overlay, request, publications) catch |err| rejected: {
        const refusal = std.meta.stringToEnum(ControlRefusal, @errorName(err)) orelse return err;
        overlay.writes.clearRetainingCapacity();
        overlay.deletes.clearRetainingCapacity();
        break :rejected .{ .refused = refusal };
    };

    const ring_key = try controlRingKey(a, realm, ordinal);
    if (try get(txn, ring_key)) |raw| {
        const old = try decodeControlReceipt(a, raw);
        if (ordinal <= controls_retained or old.ordinal != ordinal - controls_retained) return error.InvalidCatalogRecord;
        const old_nonce = try controlNonceKey(a, realm, old.nonce);
        if ((try readNumber(txn, old_nonce)) != old.ordinal) return error.InvalidCatalogRecord;
        try overlay.delete(old_nonce);
    } else if (ordinal > controls_retained) return error.InvalidCatalogRecord;
    const receipt: ControlReceipt = .{ .ordinal = ordinal, .nonce = request.nonce, .digest = try request.digest(a), .result = result };
    const bytes = try std.json.Stringify.valueAlloc(a, receipt, .{});
    if (bytes.len > max_control_receipt_bytes) return error.CatalogCommandTooLarge;
    try overlay.put(ring_key, bytes);
    try overlay.put(try controlNonceKey(a, realm, request.nonce), try encodeNumber(a, ordinal));
    try overlay.put(try key(a, realm, "slot_control_head"), try encodeNumber(a, ordinal));
    return .{ .result = result, .writes = overlay.writes.items, .deletes = overlay.deletes.items };
}

/// Materialize the exact window retained by native acceptance, with no
/// writable transaction or new offer. Retry uses the same publication IDs.
pub fn readOfferedPage(a: A, txn: anytype, request: ControlRequest, now: u64) !publisher.FeedPage {
    const control = (try readControlOutcome(a, txn, request)) orelse return error.CatalogPublicationProofPending;
    if (request.action != .feed or control != .offered) return error.InvalidLogicalPublisher;
    const realm = request.realm;
    const feed = request.action.feed;
    const window = control.offered;
    const slot = try exactSlot(a, txn, realm, feed.lease.slot);
    try requireLease(slot, feed.lease, now);
    if (window.after != feed.after or window.after < slot.confirmed_flush_lsn or window.end > slot.offered_lsn or
        window.end - window.after > 2 * @as(u64, feed.limit)) return error.InvalidReplicationAcknowledgement;
    const entries = try a.alloc(publisher.JournalEntry, @intCast((window.end - window.after) / 2));
    var count: usize = 0;
    var after = window.after;
    var budget: usize = max_feed_scan_bytes;
    while (after < window.end) {
        const raw = (try get(txn, try rowKey(a, realm, after))) orelse return error.InvalidCatalogRecord;
        if (raw.len > budget) return error.InvalidCatalogRecord;
        budget -= raw.len;
        const row = try decodeRow(a, realm, after, raw);
        if (!row.ready or row.entry.end_lsn > window.end) return error.InvalidCatalogRecord;
        after = row.entry.end_lsn;
        entries[count] = row.entry;
        count += 1;
    }
    return .{ .realm = realm, .lease = feed.lease, .head_lsn = window.end, .offered_lsn = window.end, .publications = window.publications, .entries = entries[0..count] };
}

fn verifyControlAddress(a: A, txn: anytype, realm: publisher.Realm, suffix: []const u8) !void {
    const head = try controlHead(a, txn, realm);
    if (head == 0) return error.InvalidCatalogRecord;
    const ring_prefix = "slot_control_ring/";
    const nonce_prefix = "slot_control_nonce/";
    const ordinal = if (std.mem.startsWith(u8, suffix, nonce_prefix)) nonce: {
        var nonce: [16]u8 = undefined;
        std.mem.writeInt(u128, &nonce, try exactHex(u128, suffix[nonce_prefix.len..]), .big);
        break :nonce (try readNumber(txn, try controlNonceKey(a, realm, nonce))) orelse return error.InvalidCatalogRecord;
    } else if (std.mem.startsWith(u8, suffix, ring_prefix)) ring: {
        const index = try exactHex(u8, suffix[ring_prefix.len..]);
        if (index >= controls_retained) return error.InvalidCatalogRecord;
        const delta = (head % controls_retained + controls_retained - index) % controls_retained;
        if (delta >= head) return error.InvalidCatalogRecord;
        break :ring head - delta;
    } else if (std.mem.eql(u8, suffix, "slot_control_head")) latest: {
        const retained: usize = @intCast(@min(head, controls_retained));
        for (0..retained) |offset| {
            if (try get(txn, try controlRingKey(a, realm, head - @as(u64, @intCast(offset)))) == null) return error.InvalidCatalogRecord;
        }
        break :latest head;
    } else return error.InvalidCatalogRecord;
    const raw = (try get(txn, try controlRingKey(a, realm, ordinal))) orelse return error.InvalidCatalogRecord;
    const receipt = try decodeControlReceipt(a, raw);
    const nonce_key = try controlNonceKey(a, realm, receipt.nonce);
    if (receipt.ordinal != ordinal or ordinal > head or head - ordinal >= controls_retained or
        (try readNumber(txn, nonce_key)) != ordinal) return error.InvalidCatalogRecord;
    if (std.mem.startsWith(u8, suffix, nonce_prefix) and !std.mem.endsWith(u8, nonce_key, suffix)) return error.InvalidCatalogRecord;
    if (receipt.result == .identity) try verifyIdentityReceipt(a, txn, realm, receipt.result.identity);
    if (receipt.result == .slot and receipt.result.slot.reference.database_id != realm.database_id) return error.InvalidCatalogRecord;
    if (receipt.result == .subscription_lease) subscription.requireDatabase(receipt.result.subscription_lease.item, realm.database_id) catch return error.InvalidCatalogRecord;
    if (receipt.result == .offered and receipt.result.offered.end > try reservedHead(a, txn, realm)) return error.InvalidCatalogRecord;
}
