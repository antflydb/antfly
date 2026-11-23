// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Destructive collection is a native-authorized physical metadata effect.
//! The caller owns the accepting transaction/DB apply fence. Payload removal,
//! exact dedup evidence and the real source clock are one atomic mutation.
const std = @import("std");
const wire = @import("../../system_catalog/logical_replication_publisher.zig");
const contract = @import("logical_replication_outbox_contract.zig");
const outbox = @import("logical_replication_outbox.zig");
const positions = @import("receipt_position.zig");
const authority = @import("../source_authority.zig");
const operation = @import("../../api/operation.zig");
const A = std.mem.Allocator;
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const max_marker_bytes = contract.max_metadata_record_bytes;

pub fn markerKey(a: A, reference: wire.CaptureRef) ![]u8 {
    return std.fmt.allocPrint(a, contract.family ++ "collected:{s}:{x:0>4}", .{ std.fmt.bytesToHex(try contract.ownerDigest(reference.owner), .lower), reference.participant });
}
pub fn readObservation(a: A, read: anytype, permission: wire.GcPermission) !?wire.GcObservation {
    const raw = read.get(try markerKey(a, permission.debt.reference)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const value = try decode(a, raw);
    // The stored marker records the purge; clear permission pins its exact
    // logical digest after native acceptance, independent of a new reporter.
    var purge = permission;
    purge.action = .purge;
    purge.purged_digest = null;
    purge.purged = null;
    try value.require(purge, a);
    try requireNoPayload(a, read, permission.debt.reference);
    return value;
}

/// Reconciliation after a lost clear reply. Native permission includes the
/// actual purge already accepted by metadata. The caller cannot supply the
/// observation clock: derive it from this SAME pinned physical source cut.
pub fn observeCleared(a: A, namespace: Namespace, read: anytype, permission: wire.GcPermission, root: u128) !?wire.GcObservation {
    try permission.validate();
    if (permission.action != .clear_marker or root == 0 or !namespace.eql(permission.debt.reference.namespace)) return error.InvalidLogicalOutbox;
    const source_namespace = @import("online_source_contract.zig").namespaceBytes(permission.debt.reference.namespace);
    const actual = (try authority.load(read)) orelse return error.OnlineSourceScopeChanged;
    _ = try authority.require(read, actual.kind, source_namespace);
    if (try readObservation(a, read, permission) != null) return null;
    try requireNoPayload(a, read, permission.debt.reference);
    if (actual.kind != .raft) return error.InvalidLogicalOutbox;
    const current = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(read)) orelse return null;
    const position: positions.Position = .{ .raft = .{ .term = current.term, .index = current.index } };
    const prior = permission.purged.?;
    if (!position.strictlyAfter(prior.position)) return null;
    const result: wire.GcObservation = .{
        .action = .clear_marker,
        .evidence = .absence,
        .debt_digest = try permission.debt.digest(a),
        .reference_digest = try wire.captureReferenceDigest(a, permission.debt.reference),
        .header_digest = prior.header_digest,
        .physical_root_incarnation = root,
        .position = position,
        .payload_bytes = permission.debt.reference.encoded_bytes,
        .purged_digest = permission.purged_digest,
    };
    try result.require(permission, a);
    return result;
}

pub fn stage(a: A, namespace: Namespace, txn: anytype, permission: wire.GcPermission, root: u128, ordered: @FieldType(positions.Position, "raft"), context: operation.RequestContext) !wire.GcObservation {
    try permission.validate();
    try context.ensureActive();
    const reference = permission.debt.reference;
    if (root == 0 or !namespace.eql(reference.namespace)) return error.IdentityNamespaceMismatch;
    const source_namespace = @import("online_source_contract.zig").namespaceBytes(reference.namespace);
    _ = try authority.require(txn, .raft, source_namespace);
    if (reference.position != .raft) return error.InvalidLogicalOutbox;
    const marker = try readObservation(a, txn, permission);
    if (permission.action == .purge) if (marker) |prior| return prior;
    if (permission.action == .clear_marker and marker == null)
        return (try observeCleared(a, namespace, txn, permission, root)) orelse error.LogicalCaptureReceiptPending;
    try @import("../transactions.zig").requireCollectedExternalParticipant(txn, reference.transaction_id, reference.owner, if (permission.debt.reason == .aborted) .aborted else .committed, permission.debt.decision_version);
    var header_digest: [32]u8 = undefined;
    var terminal_position: ?positions.Position = null;
    var keys: std.ArrayList([]const u8) = .empty;
    if (permission.action == .purge) {
        const terminal = (try outbox.readOrderedTerminal(a, txn, reference.transaction_id)) orelse return error.InvalidLogicalOutbox;
        if (!terminal.capture_ref.sameLogical(reference) or
            (terminal.status == .aborted) != (permission.debt.reason == .aborted) or
            terminal.commit_version != permission.debt.decision_version) return error.InvalidLogicalOutbox;
        terminal_position = .{ .raft = terminal.position };
        try keys.append(a, try outbox.terminalKey(a, reference.transaction_id));
        const header = try outbox.collectionHeader(a, txn, reference);
        const raw_header = try txn.get(try contract.headerKey(a, reference.owner, reference.participant));
        const locator = (try outbox.forTransaction(a, txn, reference.transaction_id, reference.owner, reference.namespace)) orelse return error.InvalidLogicalOutbox;
        if (!locator.reference.sameLogical(reference)) return error.InvalidLogicalOutbox;
        try keys.append(a, try contract.transactionKey(a, reference.transaction_id));
        std.crypto.hash.Blake3.hash(raw_header, &header_digest, .{});
        const prefix = try contract.prefix(a, reference.owner, reference.participant);
        var cursor = try txn.openCursor();
        {
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(prefix);
            var payload_bytes: u64 = 0;
            var row_count: u64 = 0;
            var roster_count: u32 = 0;
            var header_count: u32 = 0;
            const SchemaIdentity = struct { version: u32, digest: [32]u8 };
            var schemas: std.ArrayList(SchemaIdentity) = .empty;
            var rows_hash = std.crypto.hash.Blake3.init(.{});
            rows_hash.update("antfly/logical-outbox-changes/v1\x00");
            while (entry) |item| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, item.key, prefix)) break;
                try context.ensureActive();
                const suffix = item.key[prefix.len..];
                if (std.mem.eql(u8, suffix, "head")) {
                    header_count += 1;
                } else if (std.mem.eql(u8, suffix, "roster")) {
                    roster_count += 1;
                    if (item.value.len != header.roster_bytes) return error.InvalidLogicalOutbox;
                    _ = try outbox.collectionRoster(a, item.value, reference);
                } else if (std.mem.startsWith(u8, suffix, "row:")) {
                    row_count += 1;
                    const change = try contract.Change.decode(item.value, outbox.max_capture_bytes);
                    if (change.ordinal != row_count or !std.mem.eql(u8, item.key, try contract.rowKey(a, reference.owner, reference.participant, row_count))) return error.InvalidLogicalOutbox;
                    var length: [8]u8 = undefined;
                    std.mem.writeInt(u64, &length, item.value.len, .little);
                    rows_hash.update(&length);
                    rows_hash.update(item.value);
                } else if (std.mem.startsWith(u8, suffix, "schema:")) {
                    if (suffix.len != "schema:".len + 64) return error.InvalidLogicalOutbox;
                    var digest: [32]u8 = undefined;
                    _ = std.fmt.hexToBytes(&digest, suffix["schema:".len..]) catch return error.InvalidLogicalOutbox;
                    if (!std.mem.eql(u8, item.key, try contract.schemaKey(a, reference.owner, reference.participant, digest))) return error.InvalidLogicalOutbox;
                    try (contract.Schema{ .digest = digest, .encoded = item.value }).validate();
                    const schema = try @import("../schema.zig").deserializeSchema(a, item.value);
                    if (schema.storage_mode != .relational) return error.InvalidLogicalOutbox;
                    try schemas.append(a, .{ .version = schema.version, .digest = digest });
                } else return error.InvalidLogicalOutbox;
                if (!std.mem.eql(u8, suffix, "head")) payload_bytes = try std.math.add(u64, payload_bytes, item.value.len);
                if (payload_bytes > reference.encoded_bytes or row_count > reference.change_count or schemas.items.len > header.schema_count) return error.InvalidLogicalOutbox;
                // Cursor keys cannot escape its next call; payloads are never cloned.
                try keys.append(a, try a.dupe(u8, item.key));
            }
            if (header_count != 1 or roster_count != 1 or payload_bytes != reference.encoded_bytes or row_count != reference.change_count or schemas.items.len != header.schema_count) return error.InvalidLogicalOutbox;
            var actual: [32]u8 = undefined;
            rows_hash.final(&actual);
            if (!std.mem.eql(u8, &actual, &reference.changes_digest)) return error.InvalidLogicalOutbox;
            std.mem.sort(SchemaIdentity, schemas.items, {}, struct {
                fn less(_: void, left: SchemaIdentity, right: SchemaIdentity) bool {
                    return left.version < right.version;
                }
            }.less);
            var schema_hash = std.crypto.hash.Blake3.init(.{});
            schema_hash.update("antfly/logical-outbox-schemas/v1\x00");
            for (schemas.items, 0..) |schema, i| {
                if (i > 0 and schemas.items[i - 1].version >= schema.version) return error.InvalidLogicalOutbox;
                schema_hash.update(&schema.digest);
            }
            schema_hash.final(&actual);
            if (!std.mem.eql(u8, &actual, &header.schemas_digest)) return error.InvalidLogicalOutbox;
        }
    } else {
        const prior = marker.?;
        if (!std.mem.eql(u8, &try prior.digest(a), &permission.purged_digest.?)) return error.InvalidLogicalOutbox;
        header_digest = prior.header_digest;
    }
    const position: positions.Position = .{ .raft = ordered };
    const before = if (marker) |prior| prior.position else terminal_position.?;
    if (!position.strictlyAfter(before)) return error.InvalidControlReceiptPosition;
    const result: wire.GcObservation = .{ .action = permission.action, .debt_digest = try permission.debt.digest(a), .reference_digest = try wire.captureReferenceDigest(a, reference), .header_digest = header_digest, .physical_root_incarnation = root, .position = position, .payload_bytes = reference.encoded_bytes, .purged_digest = permission.purged_digest };
    try result.require(permission, a);
    // No destructive write precedes complete structural/decision validation.
    if (permission.action == .purge) {
        for (keys.items) |key| try txn.delete(key);
        try txn.put(try markerKey(a, reference), try encode(a, result));
    } else try txn.delete(try markerKey(a, reference));
    try context.ensureActive();
    return result;
}

fn requireNoPayload(a: A, read: anytype, reference: wire.CaptureRef) !void {
    if (try outbox.readOrderedTerminal(a, read, reference.transaction_id) != null) return error.InvalidLogicalOutbox;
    if (read.get(try contract.transactionKey(a, reference.transaction_id))) |_| return error.InvalidLogicalOutbox else |err| if (err != error.NotFound) return err;
    const prefix = try contract.prefix(a, reference.owner, reference.participant);
    var cursor = try read.openCursor();
    defer cursor.close();
    if (try cursor.seekAtOrAfter(prefix)) |entry| {
        if (std.mem.startsWith(u8, entry.key, prefix)) return error.InvalidLogicalOutbox;
    }
}

fn encode(a: A, value: wire.GcObservation) ![]u8 {
    const body = try std.json.Stringify.valueAlloc(a, value, .{});
    if (body.len + 32 > max_marker_bytes) return error.InvalidLogicalOutbox;
    const raw = try a.alloc(u8, body.len + 32);
    @memcpy(raw[0..body.len], body);
    std.crypto.hash.Blake3.hash(body, raw[body.len..][0..32], .{});
    return raw;
}
fn decode(a: A, raw: []const u8) !wire.GcObservation {
    if (raw.len <= 32 or raw.len > max_marker_bytes) return error.InvalidLogicalOutbox;
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.InvalidLogicalOutbox;
    const result = try std.json.parseFromSliceLeaky(wire.GcObservation, a, raw[0 .. raw.len - 32], .{ .allocate = .alloc_always });
    try result.validate();
    return result;
}
