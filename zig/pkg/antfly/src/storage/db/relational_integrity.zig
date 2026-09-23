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

//! Generation-bound, globally routed relational integrity records.
//!
//! Claims and all their child references explicitly share one routing digest;
//! routing their common private prefix would incorrectly concentrate every
//! constraint in one shard. Child writers retain shared claim guards and write
//! separate reference keys. Parent transitions take an exclusive claim intent,
//! so popular parents do not acquire a shared counter/write hotspot.
//!
//! Prepared effects MUST join the primary rows in the existing durable 2PC
//! prepare/decision. This module never commits independently. Raw effects are
//! trusted-node internals, not a client-controlled public mutation API.
const std = @import("std");
const time = @import("antfly_platform").time;
const transactions = @import("../transactions.zig");
const generation_retirement = @import("relational_integrity_generation_retirement.zig");
const Allocator = std.mem.Allocator;
pub const Operation = @import("types.zig").TransactionIntegrityOperation;
pub const namespace = @import("relational_integrity_contract.zig").namespace;
pub const Generation = @import("relational_integrity_contract.zig").Generation;
pub const Digest = @import("relational_integrity_contract.zig").Digest;
pub const key_len = @import("relational_integrity_contract.zig").key_len;
pub const max_record_bytes = @import("relational_integrity_contract.zig").max_record_bytes;
pub const max_commands = @import("relational_integrity_contract.zig").max_commands;
pub const max_preparation_bytes = @import("relational_integrity_contract.zig").max_preparation_bytes;
pub const Kind = @import("relational_integrity_contract.zig").Kind;
pub const CurrentView = struct {
    probe: @import("../docstore.zig").DocStore.Txn,
    scan: @import("../docstore.zig").DocStore.Txn,
    pub fn init(store: *@import("../docstore.zig").DocStore) !CurrentView {
        var probe = try store.beginProbeTxn();
        errdefer probe.abort();
        const scan = try store.beginCurrentScanTxn();
        return .{ .probe = probe, .scan = scan };
    }
    pub fn deinit(self: *CurrentView) void {
        self.scan.abort();
        self.probe.abort();
        self.* = undefined;
    }
    pub fn get(self: *CurrentView, key: []const u8) ![]const u8 {
        return self.probe.get(key);
    }
    pub fn openCursor(self: *CurrentView) !@import("../docstore.zig").DocStore.Txn.CursorAdapter {
        return self.scan.openCursor();
    }
};

const hash = @import("relational_integrity_contract.zig").hash;

const checksum = @import("relational_integrity_contract.zig").checksum;

pub const Address = @import("relational_integrity_contract.zig").Address;

pub const ParsedKey = @import("relational_integrity_contract.zig").ParsedKey;

pub const isKey = @import("relational_integrity_contract.zig").isKey;

pub const parseKey = @import("relational_integrity_contract.zig").parseKey;

pub const routingKey = @import("relational_integrity_contract.zig").routingKey;
pub const validateTransferRecord = @import("relational_integrity_contract.zig").validateTransferRecord;
pub fn validateTransferredCompanions(txn: anytype, key: []const u8, value: []const u8) !void {
    const address = try validateTransferRecord(key, value);
    const parsed = try parseKey(key);
    const claim_key = address.claimKey();
    const claim_raw = if (parsed.kind == .claim) value else try optional(txn, &claim_key) orelse return error.IntegrityMissingCompanion;
    const claim = try Claim.decode(&claim_key, claim_raw);
    if (claim.state == .draining) {
        const job_raw = if (parsed.kind == .job) value else try optional(txn, &address.jobKey()) orelse return error.IntegrityMissingCompanion;
        const job = try Job.decode(&address.jobKey(), job_raw);
        if (!std.mem.eql(u8, &job.action_id, &claim.action_id)) return error.ForeignKeyActionMismatch;
    } else if (parsed.kind == .job) return error.InvalidIntegrityRecord;
}

const appendField = @import("relational_integrity_contract.zig").appendField;

const Decoder = @import("relational_integrity_contract.zig").Decoder;

const finishRecord = @import("relational_integrity_contract.zig").finishRecord;

const recordBody = @import("relational_integrity_contract.zig").recordBody;
pub const Action = @import("relational_integrity_contract.zig").Action;
pub const Claim = @import("relational_integrity_contract.zig").Claim;

pub const Reference = @import("relational_integrity_contract.zig").Reference;

pub const ClaimOwner = @import("relational_integrity_contract.zig").ClaimOwner;

pub const Command = @import("relational_integrity_contract.zig").Command;
pub const commandAdmissionBytes = @import("relational_integrity_contract.zig").commandAdmissionBytes;

pub const validateCommandAdmission = @import("relational_integrity_contract.zig").validateCommandAdmission;
pub const Effects = struct {
    arena: std.heap.ArenaAllocator,
    operations: []const Operation,
    pub fn deinit(self: *Effects) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

const Builder = struct {
    alloc: Allocator,
    retained_bytes: usize,
    operations: std.ArrayList(Operation) = .empty,
    positions: std.StringHashMapUnmanaged(usize) = .empty,

    fn current(self: *Builder, txn: anytype, key: []const u8) !?[]const u8 {
        if (self.positions.get(key)) |position| {
            const op = self.operations.items[position];
            return if (op.kind == .guard) op.expected_value else op.value;
        }
        const value = try optional(txn, key);
        if (value) |bytes| try self.charge(bytes.len);
        return value;
    }

    fn charge(self: *Builder, bytes: usize) !void {
        self.retained_bytes = std.math.add(usize, self.retained_bytes, bytes) catch return error.TransactionTooLarge;
        if (self.retained_bytes > max_preparation_bytes) return error.TransactionTooLarge;
    }

    fn add(self: *Builder, txn: anytype, address: Address, key_bytes: []const u8, kind: @FieldType(Operation, "kind"), value: ?[]const u8) !void {
        if (self.positions.get(key_bytes)) |position| {
            if (kind == .guard) return;
            if (value) |bytes| try self.charge(bytes.len);
            self.operations.items[position].kind = kind;
            self.operations.items[position].value = if (value) |bytes| try self.alloc.dupe(u8, bytes) else null;
            return;
        }
        const key_copy = try self.alloc.dupe(u8, key_bytes);
        const old = try optional(txn, key_bytes);
        if (old) |bytes| try self.charge(std.math.mul(usize, bytes.len, 3) catch return error.TransactionTooLarge);
        if (value) |bytes| try self.charge(bytes.len);
        const op: Operation = .{ .routing_key = try self.alloc.dupe(u8, &address.routing), .key = key_copy, .kind = kind, .value = if (value) |bytes| try self.alloc.dupe(u8, bytes) else null, .expected_value = if (old) |bytes| try self.alloc.dupe(u8, bytes) else null };
        try self.positions.put(self.alloc, key_copy, self.operations.items.len);
        try self.operations.append(self.alloc, op);
    }

    fn requireEmpty(self: *Builder, txn: anytype, address: Address) !void {
        const prefix = address.referencePrefix();
        for (self.operations.items) |op| if (op.kind == .put and std.mem.startsWith(u8, op.key, &prefix)) return error.ForeignKeyReferenced;
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(&prefix);
        while (entry) |item| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, item.key, &prefix)) break;
            if (self.positions.get(item.key)) |position| if (self.operations.items[position].kind == .delete) continue;
            const reference = try Reference.decode(item.key, item.value);
            if (try generation_retirement.isRetired(txn, reference)) continue;
            return error.ForeignKeyReferenced;
        }
    }
};

fn requireOwner(claim: Claim, table: []const u8, key: []const u8) !void {
    if (!std.mem.eql(u8, claim.parent_table, table) or !std.mem.eql(u8, claim.parent_key, key)) return error.UniqueConstraintViolation;
}

pub const Job = @import("relational_integrity_contract.zig").Job;

pub fn prepare(alloc: Allocator, txn: anytype, commands: []const Command) !Effects {
    const admission_bytes = try validateCommandAdmission(commands);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var builder: Builder = .{ .alloc = owned, .retained_bytes = admission_bytes };
    // Detaches first make RESTRICT independent of caller row ordering. They
    // may not create a nonexistent parent; creation/attachment remain ordered.
    for (commands) |command| if (command.operation == .detach or command.operation == .repair_detach) {
        const claim_key = command.address.claimKey();
        const raw = try builder.current(txn, &claim_key);
        if (raw) |bytes| {
            _ = try Claim.decode(&claim_key, bytes);
        } else if (command.operation == .detach) return error.ForeignKeyParentMissing;
        try builder.add(txn, command.address, &claim_key, .guard, null);
        const reference = if (command.operation == .detach) command.operation.detach else command.operation.repair_detach;
        const reference_key = try reference.key(command.address);
        if (try builder.current(txn, &reference_key)) |old| _ = try Reference.decode(&reference_key, old);
        try builder.add(txn, command.address, &reference_key, .delete, null);
    };
    // Row input order must not decide whether same-transaction parents exist.
    // Release old unique owners before establishing new owners, but only
    // after all explicit reference detaches have entered the final overlay.
    for (0..5) |phase| {
        for (commands) |command| {
            const command_phase: usize = switch (command.operation) {
                .detach, .repair_detach => continue,
                .compare_claim, .check_owner => 0,
                .release, .repair_release => 1,
                .establish => 2,
                .attach => 3,
                else => 4,
            };
            if (command_phase != phase) continue;
            const address = command.address;
            const claim_key = address.claimKey();
            switch (command.operation) {
                .detach, .repair_detach => {},
                .compare_claim => |expected| {
                    const observed = try builder.current(txn, &claim_key);
                    const encoded = if (expected) |claim| try claim.encode(owned, address) else null;
                    if (!optionalEqual(observed, encoded)) return error.PreparedReadSetChanged;
                    try builder.add(txn, address, &claim_key, .guard, null);
                },
                .check_owner => |owner| {
                    const claim = try Claim.decode(&claim_key, try builder.current(txn, &claim_key) orelse return error.ForeignKeyParentMissing);
                    try requireOwner(claim, owner.parent_table, owner.parent_key);
                    if (claim.state != .live) return error.ForeignKeyActionInProgress;
                    try builder.add(txn, address, &claim_key, .guard, null);
                },
                .establish => |claim| {
                    if (claim.state != .live) return error.InvalidIntegrityCommand;
                    if (try builder.current(txn, &claim_key)) |raw| {
                        const old = try Claim.decode(&claim_key, raw);
                        try requireOwner(old, claim.parent_table, claim.parent_key);
                        if (!std.mem.eql(u8, old.tuple, claim.tuple)) return error.IntegrityAddressMismatch;
                        if (old.state != .live) return error.ForeignKeyActionInProgress;
                        try builder.add(txn, address, &claim_key, .guard, null);
                        continue;
                    }
                    try builder.add(txn, address, &claim_key, .put, try claim.encode(owned, address));
                },
                .attach => |reference| {
                    if (try generation_retirement.isRetired(txn, reference)) return error.GenerationRetired;
                    const raw = try builder.current(txn, &claim_key) orelse return error.ForeignKeyParentMissing;
                    const claim = try Claim.decode(&claim_key, raw);
                    if (claim.state != .live) return error.ForeignKeyActionInProgress;
                    try builder.add(txn, address, &claim_key, .guard, null);
                    const reference_key = try reference.key(address);
                    const encoded_reference = try reference.encode(owned, address);
                    if (try builder.current(txn, &reference_key)) |old| {
                        _ = try Reference.decode(&reference_key, old);
                        if (std.mem.eql(u8, old, encoded_reference)) {
                            try builder.add(txn, address, &reference_key, .guard, null);
                            continue;
                        }
                        return error.IntegrityAddressMismatch;
                    }
                    try builder.add(txn, address, &reference_key, .put, encoded_reference);
                },
                .release, .repair_release => |owner| {
                    const raw = try builder.current(txn, &claim_key) orelse {
                        if (command.operation != .repair_release) return error.ForeignKeyParentMissing;
                        try builder.add(txn, address, &claim_key, .guard, null);
                        continue;
                    };
                    const claim = try Claim.decode(&claim_key, raw);
                    if (command.operation == .repair_release and
                        (!std.mem.eql(u8, claim.parent_table, owner.parent_table) or !std.mem.eql(u8, claim.parent_key, owner.parent_key)))
                    {
                        // Partial activation may have assigned this duplicate's
                        // tuple to another row. Repair must preserve that owner.
                        try builder.add(txn, address, &claim_key, .guard, null);
                        continue;
                    }
                    try requireOwner(claim, owner.parent_table, owner.parent_key);
                    if (claim.state != .live) return error.ForeignKeyActionInProgress;
                    try builder.requireEmpty(txn, address);
                    try builder.add(txn, address, &claim_key, .delete, null);
                },
                .start_action => |action| {
                    const raw = try builder.current(txn, &claim_key) orelse return error.ForeignKeyParentMissing;
                    var claim = try Claim.decode(&claim_key, raw);
                    try requireOwner(claim, action.parent_table, action.parent_key);
                    if (action.action == .restrict or std.mem.allEqual(u8, &action.action_id, 0)) return error.InvalidIntegrityCommand;
                    if (action.parent_version == 0 or (action.action == .parent_delete and action.parent_value != null) or (action.action == .parent_update and action.parent_value == null)) return error.InvalidIntegrityCommand;
                    if (claim.state == .draining) {
                        if (!std.mem.eql(u8, &claim.action_id, &action.action_id) or claim.action != action.action or !optionalEqual(claim.target_tuple, action.target_tuple)) return error.ForeignKeyActionInProgress;
                        const job = try Job.decode(&address.jobKey(), try builder.current(txn, &address.jobKey()) orelse return error.InvalidIntegrityRecord);
                        if (job.parent_version != action.parent_version or job.parent_schema_version != action.parent_schema_version or !optionalEqual(job.parent_value, action.parent_value)) return error.ForeignKeyActionMismatch;
                        try builder.add(txn, address, &claim_key, .guard, null);
                        continue;
                    }
                    claim.state = .draining;
                    claim.action = action.action;
                    claim.action_id = action.action_id;
                    claim.target_tuple = action.target_tuple;
                    try builder.add(txn, address, &claim_key, .put, try claim.encode(owned, address));
                    try builder.add(txn, address, &address.jobKey(), .put, try (Job{ .action_id = action.action_id, .parent_version = action.parent_version, .parent_schema_version = action.parent_schema_version, .parent_value = action.parent_value }).encode(owned, address));
                },
                .finish_action => |action_id| {
                    const raw = try builder.current(txn, &claim_key) orelse return error.ForeignKeyParentMissing;
                    const claim = try Claim.decode(&claim_key, raw);
                    if (claim.state != .draining or !std.mem.eql(u8, &claim.action_id, &action_id)) return error.ForeignKeyActionMismatch;
                    const job = try Job.decode(&address.jobKey(), try builder.current(txn, &address.jobKey()) orelse return error.InvalidIntegrityRecord);
                    if (job.phase != .applying or !std.mem.eql(u8, &job.action_id, &action_id)) return error.ForeignKeyActionNotValidated;
                    try builder.requireEmpty(txn, address);
                    try builder.add(txn, address, &claim_key, .delete, null);
                    try builder.add(txn, address, &address.jobKey(), .delete, null);
                },
                .advance_validation => |advance| {
                    const claim = try Claim.decode(&claim_key, try builder.current(txn, &claim_key) orelse return error.ForeignKeyParentMissing);
                    var job = try Job.decode(&address.jobKey(), try builder.current(txn, &address.jobKey()) orelse return error.InvalidIntegrityRecord);
                    if (claim.state != .draining or job.phase != .validating or !std.mem.eql(u8, &claim.action_id, &advance.action_id) or !std.mem.eql(u8, &job.action_id, &advance.action_id)) return error.ForeignKeyActionMismatch;
                    if (!std.mem.eql(u8, job.cursor, advance.expected_cursor) or job.rows_validated != advance.expected_rows) return error.InvalidIntegrityContinuation;
                    if (!advance.complete and advance.failure == null and std.mem.order(u8, advance.cursor, job.cursor) != .gt) return error.InvalidIntegrityContinuation;
                    job.cursor = if (advance.complete and advance.failure == null) "" else advance.cursor;
                    job.rows_validated = std.math.add(u64, job.rows_validated, advance.rows) catch return error.InvalidIntegrityRecord;
                    job.phase = if (advance.failure != null) .failed else if (advance.complete) .applying else .validating;
                    job.failure = advance.failure orelse "";
                    try builder.add(txn, address, &claim_key, .guard, null);
                    try builder.add(txn, address, &address.jobKey(), .put, try job.encode(owned, address));
                },
                .retry_action, .cancel_action => |action_id| {
                    var claim = try Claim.decode(&claim_key, try builder.current(txn, &claim_key) orelse return error.ForeignKeyParentMissing);
                    const job = try Job.decode(&address.jobKey(), try builder.current(txn, &address.jobKey()) orelse return error.InvalidIntegrityRecord);
                    if (claim.state != .draining or !std.mem.eql(u8, &claim.action_id, &action_id) or !std.mem.eql(u8, &job.action_id, &action_id) or job.phase == .applying) return error.ForeignKeyActionMismatch;
                    if (command.operation == .retry_action) {
                        if (job.phase != .failed) return error.ForeignKeyActionMismatch;
                        try builder.add(txn, address, &claim_key, .guard, null);
                        try builder.add(txn, address, &address.jobKey(), .put, try (Job{ .action_id = action_id, .parent_version = job.parent_version, .parent_schema_version = job.parent_schema_version, .parent_value = job.parent_value }).encode(owned, address));
                    } else {
                        claim.state = .live;
                        claim.action = .restrict;
                        claim.action_id = @splat(0);
                        claim.target_tuple = null;
                        try builder.add(txn, address, &claim_key, .put, try claim.encode(owned, address));
                        try builder.add(txn, address, &address.jobKey(), .delete, null);
                    }
                },
            }
        }
    }
    const operations = try builder.operations.toOwnedSlice(owned);
    return .{ .arena = arena, .operations = operations };
}

fn optionalEqual(left: ?[]const u8, right: ?[]const u8) bool {
    return if (left) |bytes| if (right) |other| std.mem.eql(u8, bytes, other) else false else right == null;
}

/// Structural validation runs again on the receiving participant. Semantic
/// ownership/declaration checks belong to the typed coordinator that enlisted
/// the primary row and generated these effects; raw effects are never public.
pub const validateOperation = @import("relational_integrity_contract.zig").validateOperation;

pub fn predicate(op: Operation) transactions.VersionPredicate {
    return .{ .key = op.key, .comparison = .exact_value, .expected_value = op.expected_value };
}

fn requireEmptyOperations(txn: anytype, address: Address, operations: []const Operation, overlay: *const std.StringHashMapUnmanaged(usize)) !void {
    const prefix = address.referencePrefix();
    for (operations) |candidate| if (candidate.kind == .put and std.mem.startsWith(u8, candidate.key, &prefix)) return error.ForeignKeyReferenced;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(&prefix);
    while (entry) |item| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, &prefix)) break;
        if (overlay.get(item.key)) |position| if (operations[position].kind == .delete) continue;
        const reference = try Reference.decode(item.key, item.value);
        if (try generation_retirement.isRetired(txn, reference)) continue;
        return error.ForeignKeyReferenced;
    }
}

fn requireReboundReferences(txn: anytype, address: Address, operations: []const Operation, overlay: *const std.StringHashMapUnmanaged(usize)) !void {
    const prefix = address.referencePrefix();
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(&prefix);
    while (entry) |item| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, &prefix)) break;
        const reference = try Reference.decode(item.key, item.value);
        if (try generation_retirement.isRetired(txn, reference)) continue;
        const position = overlay.get(item.key) orelse return error.ForeignKeyReferenced;
        // A guard-only attach is not a reference rebind. Only a detached old
        // relationship, optionally reattached to the final tuple owner, may
        // survive a handoff. Newly committed unseen children still reject it.
        if (operations[position].kind == .guard) return error.ForeignKeyReferenced;
    }
}

/// Recheck semantic transitions and prefix proofs at participant prepare,
/// under the SAME apply fence as acquisition of the claim intent. Checking
/// emptiness only at coordinator planning is unsafe: another child can commit
/// a reference while leaving the shared claim's bytes unchanged.
pub fn validatePreparedEffects(alloc: Allocator, txn: anytype, operations: []const Operation) !void {
    if (operations.len > max_commands * 3) return error.TransactionTooLarge;
    var overlay = std.StringHashMapUnmanaged(usize).empty;
    defer overlay.deinit(alloc);
    for (operations, 0..) |op, index| {
        try validateOperation(op);
        const entry = try overlay.getOrPut(alloc, op.key);
        if (entry.found_existing) return error.InvalidIntegrityOperation;
        entry.value_ptr.* = index;
        if (!optionalEqual(try optional(txn, op.key), op.expected_value)) return error.VersionConflict;
    }
    for (operations) |op| {
        const parsed = try parseKey(op.key);
        const claim_key = parsed.address.claimKey();
        switch (parsed.kind) {
            .reference => {
                if (op.kind == .put and try generation_retirement.isRetired(txn, try Reference.decode(op.key, op.value.?))) return error.GenerationRetired;
                const claim_op = operations[overlay.get(&claim_key) orelse return error.IntegrityClaimGuardRequired];
                const current_claim = if (claim_op.kind == .put) claim_op.value else claim_op.expected_value;
                const claim = try Claim.decode(&claim_key, current_claim orelse return error.ForeignKeyParentMissing);
                if (op.kind == .put and (claim_op.kind == .delete or claim.state != .live)) return error.ForeignKeyActionInProgress;
            },
            .claim => {
                if (op.kind == .guard) continue;
                const old: ?Claim = if (op.expected_value) |bytes| try Claim.decode(op.key, bytes) else null;
                const next: ?Claim = if (op.value) |bytes| try Claim.decode(op.key, bytes) else null;
                if (old) |before| {
                    if (next) |after| {
                        if (!std.mem.eql(u8, after.parent_table, before.parent_table) or !std.mem.eql(u8, after.parent_key, before.parent_key)) {
                            if (before.state != .live or after.state != .live) return error.UniqueConstraintViolation;
                            // Preparation already proves old references empty
                            // after explicit detaches and before establishes.
                            // The final overlay can legitimately reattach a
                            // NO ACTION child to the replacement tuple owner.
                            // Its live exact-tuple claim and exclusive intent
                            // preserve existence across the atomic decision.
                            try requireReboundReferences(txn, parsed.address, operations, &overlay);
                        }
                        if (!std.mem.eql(u8, before.tuple, after.tuple)) return error.IntegrityAddressMismatch;
                        if (before.state == .draining) {
                            if (after.state == .live) {
                                const job_op = operations[overlay.get(&parsed.address.jobKey()) orelse return error.IntegrityActionJobRequired];
                                const old_job = try Job.decode(job_op.key, job_op.expected_value orelse return error.InvalidIntegrityRecord);
                                if (job_op.kind != .delete or old_job.phase == .applying or !std.mem.eql(u8, &old_job.action_id, &before.action_id)) return error.ForeignKeyActionMismatch;
                            } else if (before.action != after.action or !std.mem.eql(u8, &before.action_id, &after.action_id) or !optionalEqual(before.target_tuple, after.target_tuple)) return error.ForeignKeyActionMismatch;
                        }
                    }
                } else if (next) |after| {
                    if (after.state != .live) return error.InvalidIntegrityOperation;
                } else return error.ForeignKeyParentMissing;
                if (op.kind == .delete) {
                    try requireEmptyOperations(txn, parsed.address, operations, &overlay);
                }
                const needs_job_put = if (next) |after| after.state == .draining and (old == null or old.?.state == .live) else false;
                const needs_job_delete = next == null and old != null and old.?.state == .draining;
                if (needs_job_put or needs_job_delete) {
                    const job = operations[overlay.get(&parsed.address.jobKey()) orelse return error.IntegrityActionJobRequired];
                    const expected_kind: @FieldType(Operation, "kind") = if (needs_job_put) .put else .delete;
                    if (job.kind != expected_kind) return error.InvalidIntegrityOperation;
                    const decoded = try Job.decode(job.key, if (needs_job_put) job.value.? else job.expected_value orelse return error.InvalidIntegrityRecord);
                    if (!std.mem.eql(u8, &decoded.action_id, if (next) |after| &after.action_id else &old.?.action_id)) return error.ForeignKeyActionMismatch;
                    if (needs_job_put and (decoded.phase != .validating or decoded.cursor.len != 0 or decoded.rows_validated != 0)) return error.InvalidIntegrityOperation;
                    if (needs_job_delete and decoded.phase != .applying) return error.ForeignKeyActionNotValidated;
                }
            },
            .job => {
                const claim_op = operations[overlay.get(&claim_key) orelse return error.IntegrityClaimGuardRequired];
                if (op.kind == .put) {
                    if (claim_op.kind == .delete) return error.InvalidIntegrityOperation;
                    const claim = try Claim.decode(&claim_key, (if (claim_op.kind == .put) claim_op.value else claim_op.expected_value) orelse return error.ForeignKeyParentMissing);
                    const next_job = try Job.decode(op.key, op.value.?);
                    if (claim.state != .draining or !std.mem.eql(u8, &claim.action_id, &next_job.action_id)) return error.InvalidIntegrityOperation;
                    if (op.expected_value) |old_bytes| {
                        const old_job = try Job.decode(op.key, old_bytes);
                        if (!std.mem.eql(u8, &old_job.action_id, &next_job.action_id) or old_job.phase == .applying) return error.ForeignKeyActionMismatch;
                        if (old_job.parent_version != next_job.parent_version or old_job.parent_schema_version != next_job.parent_schema_version or !optionalEqual(old_job.parent_value, next_job.parent_value)) return error.ForeignKeyActionMismatch;
                        if (old_job.phase == .failed) {
                            if (next_job.phase != .validating or next_job.cursor.len != 0 or next_job.rows_validated != 0) return error.InvalidIntegrityOperation;
                        } else if (next_job.rows_validated < old_job.rows_validated or (next_job.phase == .validating and std.mem.order(u8, next_job.cursor, old_job.cursor) != .gt)) return error.InvalidIntegrityContinuation;
                    }
                } else if (op.kind == .delete) {
                    if (claim_op.kind == .put) {
                        const before = try Claim.decode(&claim_key, claim_op.expected_value orelse return error.InvalidIntegrityRecord);
                        const after = try Claim.decode(&claim_key, claim_op.value.?);
                        const job = try Job.decode(op.key, op.expected_value orelse return error.InvalidIntegrityRecord);
                        if (before.state != .draining or after.state != .live or job.phase == .applying or !std.mem.eql(u8, &before.action_id, &job.action_id)) return error.InvalidIntegrityOperation;
                    } else if (claim_op.kind != .delete) return error.InvalidIntegrityOperation;
                }
            },
        }
    }
}

pub const ActionPage = struct {
    arena: std.heap.ArenaAllocator,
    claim: Claim,
    job: Job,
    next_cursor: []const u8,
    references: []const Reference,
    /// The fence prohibits new references. Processed references are removed
    /// atomically with child mutations, making the first remaining key the
    /// durable continuation. No growing journal or full-prefix scan is needed.
    complete: bool,
    pub fn deinit(self: *ActionPage) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn actionPage(alloc: Allocator, txn: anytype, address: Address, action_id: Generation, max_rows: usize, max_bytes: usize) !ActionPage {
    return actionPageWithBudget(alloc, null, txn, address, action_id, .{ .rows = max_rows, .bytes = max_bytes });
}

pub const ActionBudget = struct { rows: usize = 256, bytes: usize = 1024 * 1024, time_ns: u64 = 5 * std.time.ns_per_ms };

pub fn actionPageWithBudget(alloc: Allocator, io: ?std.Io, txn: anytype, address: Address, action_id: Generation, budget: ActionBudget) !ActionPage {
    const max_rows = budget.rows;
    const max_bytes = budget.bytes;
    if (max_rows == 0 or max_rows > 4096 or max_bytes == 0 or max_bytes > 16 * 1024 * 1024) return error.InvalidIntegrityBudget;
    if (budget.time_ns == 0 or budget.time_ns > std.time.ns_per_s) return error.InvalidIntegrityBudget;
    if (io) |runtime_io| try runtime_io.checkCancel();
    const started = time.monotonicNs();
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const claim_key = address.claimKey();
    const raw = try optional(txn, &claim_key) orelse return error.ForeignKeyParentMissing;
    const claim = try Claim.decode(&claim_key, try owned.dupe(u8, raw));
    if (claim.state != .draining or !std.mem.eql(u8, &claim.action_id, &action_id)) return error.ForeignKeyActionMismatch;
    const raw_job = try optional(txn, &address.jobKey()) orelse return error.InvalidIntegrityRecord;
    const job = try Job.decode(&address.jobKey(), try owned.dupe(u8, raw_job));
    if (!std.mem.eql(u8, &job.action_id, &action_id)) return error.ForeignKeyActionMismatch;
    if (job.phase == .failed) return error.ForeignKeyActionFailed;
    const prefix = address.referencePrefix();
    var cursor = try txn.openCursor();
    defer cursor.close();
    var references: std.ArrayList(Reference) = .empty;
    var bytes: usize = 0;
    var inspected: usize = 0;
    var entry = try cursor.seekAtOrAfter(if (job.phase == .validating and job.cursor.len != 0) job.cursor else &prefix);
    if (entry) |item| if (job.phase == .validating and std.mem.eql(u8, item.key, job.cursor)) {
        entry = try cursor.next();
    };
    var complete = true;
    var next_cursor: []const u8 = job.cursor;
    while (entry) |item| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, &prefix)) break;
        if (io) |runtime_io| try runtime_io.checkCancel();
        const item_bytes = std.math.add(usize, item.key.len, item.value.len) catch return error.IntegrityRecordTooLarge;
        if (inspected == max_rows or item_bytes > max_bytes - bytes or
            (inspected != 0 and time.monotonicNs() -| started >= budget.time_ns))
        {
            if (inspected == 0) return error.IntegrityRecordTooLarge;
            complete = false;
            break;
        }
        const reference = try Reference.decode(item.key, item.value);
        if (try generation_retirement.isRetired(txn, reference)) {
            next_cursor = try owned.dupe(u8, item.key);
            bytes += item_bytes;
            inspected += 1;
            continue;
        }
        const value = try owned.dupe(u8, item.value);
        try references.append(owned, try Reference.decode(item.key, value));
        next_cursor = try owned.dupe(u8, item.key);
        bytes += item_bytes;
        inspected += 1;
    }
    const reference_items = try references.toOwnedSlice(owned);
    return .{ .arena = arena, .claim = claim, .job = job, .next_cursor = next_cursor, .references = reference_items, .complete = complete };
}

test "relational integrity keys are generation bound explicitly routed and checksummed" {
    const alloc = std.testing.allocator;
    const address = try Address.init(@splat(1), "typed composite tuple");
    const other = try Address.init(@splat(2), "typed composite tuple");
    try std.testing.expect(!std.mem.eql(u8, &address.routing, &other.routing));
    const claim: Claim = .{ .tuple = "typed composite tuple", .parent_table = "parents", .parent_key = "42", .schema_version = 1 };
    const encoded = try claim.encode(alloc, address);
    defer alloc.free(encoded);
    const parsed = try Claim.decode(&address.claimKey(), encoded);
    try std.testing.expectEqualStrings("42", parsed.parent_key);
    try std.testing.expectError(error.IntegrityChecksumMismatch, Claim.decode(&other.claimKey(), encoded));
    const reference: Reference = .{ .child_table = "children", .child_key = "9", .constraint_name = "parent_id", .constraint_generation = @splat(3) };
    const ref_key = try reference.key(address);
    const ref_value = try reference.encode(alloc, address);
    defer alloc.free(ref_value);
    try std.testing.expectEqualStrings(&address.routing, &(try routingKey(&ref_key)));
    try std.testing.expectEqualStrings("9", (try Reference.decode(&ref_key, ref_value)).child_key);
    encoded[encoded.len - 1] ^= 1;
    try std.testing.expectError(error.IntegrityChecksumMismatch, Claim.decode(&address.claimKey(), encoded));
}

test "relational integrity preparation cleans up every allocation failure and bounds admission" {
    const Harness = struct {
        const Empty = struct {
            const Cursor = struct {
                const Entry = struct { key: []const u8, value: []const u8 };
                pub fn close(_: *@This()) void {}
                pub fn seekAtOrAfter(_: *@This(), _: []const u8) !?Entry {
                    return null;
                }
                pub fn next(_: *@This()) !?Entry {
                    return null;
                }
            };
            pub fn get(_: *@This(), _: []const u8) ![]const u8 {
                return error.NotFound;
            }
            pub fn openCursor(_: *@This()) !Cursor {
                return .{};
            }
        };
        fn run(alloc: Allocator) !void {
            const address = try Address.init(@splat(1), "tuple");
            const claim: Claim = .{ .tuple = "tuple", .parent_table = "parent", .parent_key = "p", .schema_version = 1 };
            const reference: Reference = .{ .child_table = "child", .child_key = "c", .constraint_name = "fk", .constraint_generation = @splat(2) };
            var empty: Empty = .{};
            var effects = try prepare(alloc, &empty, &.{ .{ .address = address, .operation = .{ .attach = reference } }, .{ .address = address, .operation = .{ .establish = claim } } });
            defer effects.deinit();
            try validatePreparedEffects(alloc, &empty, effects.operations);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
    const oversized = try std.testing.allocator.alloc(u8, max_preparation_bytes / 16);
    defer std.testing.allocator.free(oversized);
    @memset(oversized, 'x');
    const address = try Address.init(@splat(1), "tuple");
    try std.testing.expectError(error.TransactionTooLarge, validateCommandAdmission(&.{.{ .address = address, .operation = .{ .establish = .{ .tuple = oversized, .parent_table = "parent", .parent_key = "p", .schema_version = 1 } } }}));
}

fn testPrepareEffects(store: *@import("../docstore.zig").DocStore, manager: *transactions.TxnManager, id: transactions.TxnId, effects: []const Operation) !void {
    const alloc = std.testing.allocator;
    var read = try CurrentView.init(store);
    defer read.deinit();
    try validatePreparedEffects(alloc, &read, effects);
    var intents: std.ArrayList(transactions.WriteIntent) = .empty;
    defer intents.deinit(alloc);
    var predicates: std.ArrayList(transactions.VersionPredicate) = .empty;
    defer predicates.deinit(alloc);
    for (effects) |op| {
        try predicates.append(alloc, predicate(op));
        if (op.kind != .guard) try intents.append(alloc, .{ .key = op.key, .value = op.value });
    }
    try manager.writeIntents(id, intents.items, predicates.items);
}

test "relational index system integrity compare claim fences absence and exact owner before combined writes" {
    const Fake = struct {
        value: ?[]const u8 = null,
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
        const Cursor = struct {
            const Entry = struct { key: []const u8, value: []const u8 };
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(_: *@This(), _: []const u8) !?Entry {
                return null;
            }
            pub fn next(_: *@This()) !?Entry {
                return null;
            }
        };
        pub fn openCursor(_: *@This()) !Cursor {
            return .{};
        }
    };
    const alloc = std.testing.allocator;
    const address = try Address.init(@splat(7), "tuple");
    const claim: Claim = .{ .tuple = "tuple", .parent_table = "items", .parent_key = "first", .schema_version = 1 };
    const encoded = try claim.encode(alloc, address);
    defer alloc.free(encoded);
    var empty: Fake = .{};
    var insertion = try prepare(alloc, &empty, &.{
        .{ .address = address, .operation = .{ .establish = claim } },
        .{ .address = address, .operation = .{ .compare_claim = null } },
    });
    defer insertion.deinit();
    try std.testing.expectEqual(@as(usize, 1), insertion.operations.len);
    try std.testing.expect(insertion.operations[0].kind == .put);
    try std.testing.expect(insertion.operations[0].expected_value == null);
    var occupied: Fake = .{ .value = encoded };
    try std.testing.expectError(error.PreparedReadSetChanged, prepare(alloc, &occupied, &.{.{ .address = address, .operation = .{ .compare_claim = null } }}));
    var guard = try prepare(alloc, &occupied, &.{.{ .address = address, .operation = .{ .compare_claim = claim } }});
    defer guard.deinit();
    try std.testing.expectEqual(@as(usize, 1), guard.operations.len);
    try std.testing.expectEqualStrings(encoded, guard.operations[0].expected_value.?);
    var changed = claim;
    changed.schema_version += 1;
    try std.testing.expectError(error.PreparedReadSetChanged, prepare(alloc, &occupied, &.{.{ .address = address, .operation = .{ .compare_claim = changed } }}));
    try std.testing.expectError(error.VersionConflict, validatePreparedEffects(alloc, &empty, guard.operations));
}

fn testCommands(store: *@import("../docstore.zig").DocStore, manager: *transactions.TxnManager, id: transactions.TxnId, commands: []const Command, commit: bool) !void {
    var read = try CurrentView.init(store);
    defer read.deinit();
    var effects = try prepare(std.testing.allocator, &read, commands);
    defer effects.deinit();
    try manager.initTransaction(id, 1);
    try testPrepareEffects(store, manager, id, effects.operations);
    if (commit) try manager.resolveIntents(id, .committed, 2);
}

test "relational integrity shared parent guards and fenced bounded action recovery" {
    const alloc = std.testing.allocator;
    const DocStore = @import("../docstore.zig").DocStore;
    const lsm = @import("../lsm_backend.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const address = try Address.init(@splat(1), "parent tuple");
    const claim: Claim = .{ .tuple = "parent tuple", .parent_table = "parent", .parent_key = "p", .schema_version = 1 };
    const first: Reference = .{ .child_table = "child", .child_key = "a", .constraint_name = "fk", .constraint_generation = @splat(2) };
    const second: Reference = .{ .child_table = "child", .child_key = "b", .constraint_name = "fk", .constraint_generation = @splat(2) };
    const action_id: Generation = @splat(90);
    {
        var backend = try lsm.Backend.open(alloc, path, .{});
        defer backend.close();
        const runtime = try backend.runtimeStore(alloc, .{});
        var store = try DocStore.openRuntime(alloc, runtime);
        defer store.close();
        var manager = try transactions.TxnManager.init(alloc, &store);
        defer manager.deinit();
        try testCommands(&store, &manager, @splat(1), &.{.{ .address = address, .operation = .{ .establish = claim } }}, true);
        var read_before = try store.beginReadTxn();
        defer read_before.abort();
        var stale_release = try prepare(alloc, &read_before, &.{.{ .address = address, .operation = .{ .release = .{ .parent_table = "parent", .parent_key = "p" } } }});
        defer stale_release.deinit();
        try testCommands(&store, &manager, @splat(2), &.{.{ .address = address, .operation = .{ .attach = first } }}, false);
        // Independent child prepares retain compatible shared claim guards.
        try testCommands(&store, &manager, @splat(3), &.{.{ .address = address, .operation = .{ .attach = second } }}, false);
        try manager.initTransaction(@splat(4), 1);
        try std.testing.expectError(error.IntentConflict, testPrepareEffects(&store, &manager, @splat(4), stale_release.operations));
        try manager.resolveIntents(@splat(2), .committed, 2);
        try manager.resolveIntents(@splat(3), .committed, 2);
        // A staged TRUNCATE parent tombstone is not a publication receipt.
        // Native RESTRICT planning and participant validation still see the
        // committed child references.
        const topology = @import("relational_integrity_topology.zig");
        const retirement = @import("relational_integrity_generation_retirement.zig");
        const fence: topology.Fence = .{ .role = .truncate_parent, .transition_id = 11, .attempt = 1, .peer_group_id = 21, .owner_group_id = 31, .namespace = .{ .table_id = 41, .shard_id = 31, .range_id = 31 }, .catalog_digest = @splat(4) };
        const encoded_fence = try fence.encode();
        try store.put(topology.fence_key, &encoded_fence);
        const pending = try retirement.encodePending(alloc, fence, @splat(5), &.{.{ .child_table_id = 51, .child_table_name = first.child_table, .constraint_name = first.constraint_name, .generation = first.constraint_generation }});
        defer alloc.free(pending);
        try store.put(retirement.key, pending);
        var pending_read = try store.beginReadTxn();
        defer pending_read.abort();
        try std.testing.expectError(error.ForeignKeyReferenced, prepare(alloc, &pending_read, &.{
            .{ .address = address, .operation = .{ .release = .{ .parent_table = "parent", .parent_key = "p" } } },
        }));
        // Claim bytes did not change. Participant prefix proof must still
        // reject the release planned before the two child references existed.
        try std.testing.expectError(error.ForeignKeyReferenced, testPrepareEffects(&store, &manager, @splat(4), stale_release.operations));
        try testCommands(&store, &manager, @splat(5), &.{.{ .address = address, .operation = .{ .start_action = .{ .parent_table = "parent", .parent_key = "p", .action = .parent_delete, .action_id = action_id, .parent_version = 2, .parent_schema_version = 1 } } }}, true);
        try std.testing.expectError(error.ForeignKeyActionInProgress, testCommands(&store, &manager, @splat(6), &.{.{ .address = address, .operation = .{ .attach = first } }}, true));
        var scan = try store.beginReadTxn();
        defer scan.abort();
        var page = try actionPage(alloc, &scan, address, action_id, 1, 4096);
        defer page.deinit();
        try std.testing.expectEqual(.validating, page.job.phase);
        try std.testing.expectEqual(@as(usize, 1), page.references.len);
        try std.testing.expect(!page.complete);
        try testCommands(&store, &manager, @splat(7), &.{.{ .address = address, .operation = .{ .advance_validation = .{ .action_id = action_id, .expected_cursor = page.job.cursor, .expected_rows = page.job.rows_validated, .cursor = page.next_cursor, .rows = page.references.len, .complete = page.complete } } }}, true);
    }
    {
        var backend = try lsm.Backend.open(alloc, path, .{});
        defer backend.close();
        const runtime = try backend.runtimeStore(alloc, .{});
        var store = try DocStore.openRuntime(alloc, runtime);
        defer store.close();
        var manager = try transactions.TxnManager.init(alloc, &store);
        defer manager.deinit();
        var scan = try store.beginReadTxn();
        defer scan.abort();
        var page = try actionPage(alloc, &scan, address, action_id, 1, 4096);
        defer page.deinit();
        try std.testing.expectEqual(@as(u64, 1), page.job.rows_validated);
        try std.testing.expectEqual(@as(usize, 1), page.references.len);
        try std.testing.expect(page.complete);
        try testCommands(&store, &manager, @splat(8), &.{.{ .address = address, .operation = .{ .advance_validation = .{ .action_id = action_id, .expected_cursor = page.job.cursor, .expected_rows = page.job.rows_validated, .cursor = page.next_cursor, .rows = page.references.len, .complete = true } } }}, true);
        try std.testing.expectError(error.ForeignKeyActionMismatch, testCommands(&store, &manager, @splat(9), &.{.{ .address = address, .operation = .{ .cancel_action = action_id } }}, true));
        // Deleting reference records is atomic with the caller's corresponding
        // child row action; finishing needs the same-transaction final overlay.
        try testCommands(&store, &manager, @splat(10), &.{
            .{ .address = address, .operation = .{ .finish_action = action_id } },
            .{ .address = address, .operation = .{ .detach = first } },
            .{ .address = address, .operation = .{ .detach = second } },
        }, true);
        var final_read = try store.beginReadTxn();
        defer final_read.abort();
        try std.testing.expectError(error.NotFound, final_read.get(&address.claimKey()));
        try std.testing.expectError(error.NotFound, final_read.get(&address.jobKey()));
        // Same-batch parents are established before child admission, even if
        // the caller's coalesced/hash iteration presents the child first.
        try testCommands(&store, &manager, @splat(11), &.{
            .{ .address = address, .operation = .{ .attach = first } },
            .{ .address = address, .operation = .{ .establish = claim } },
        }, true);
        var unchanged_read = try CurrentView.init(&store);
        defer unchanged_read.deinit();
        var unchanged = try prepare(alloc, &unchanged_read, &.{
            .{ .address = address, .operation = .{ .attach = first } },
            .{ .address = address, .operation = .{ .establish = claim } },
        });
        defer unchanged.deinit();
        try std.testing.expectEqual(@as(usize, 2), unchanged.operations.len);
        for (unchanged.operations) |operation| try std.testing.expectEqual(.guard, operation.kind);
    }
}

test "distributed txn deferred unique swaps fence concurrent claims and survive prepared restart" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const a = try Address.init(@splat(9), "one");
    const b = try Address.init(@splat(9), "two");
    const first: Claim = .{ .tuple = "one", .parent_table = "rows", .parent_key = "a", .schema_version = 1 };
    const second: Claim = .{ .tuple = "two", .parent_table = "rows", .parent_key = "b", .schema_version = 1 };
    var swapped_first = first;
    swapped_first.parent_key = "b";
    var swapped_second = second;
    swapped_second.parent_key = "a";
    const commands = [_]Command{
        .{ .address = a, .operation = .{ .establish = swapped_first } },
        .{ .address = b, .operation = .{ .establish = swapped_second } },
        .{ .address = a, .operation = .{ .release = .{ .parent_table = "rows", .parent_key = "a" } } },
        .{ .address = b, .operation = .{ .release = .{ .parent_table = "rows", .parent_key = "b" } } },
    };
    {
        var backend = try @import("../lsm_backend.zig").Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try @import("../docstore.zig").DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try transactions.TxnManager.init(alloc, &store);
        defer manager.deinit();
        try testCommands(&store, &manager, @splat(1), &.{ .{ .address = a, .operation = .{ .establish = first } }, .{ .address = b, .operation = .{ .establish = second } } }, true);
        try testCommands(&store, &manager, @splat(2), &commands, false);
        try std.testing.expectError(error.IntentConflict, testCommands(&store, &manager, @splat(3), &commands, false));
    }
    {
        var backend = try @import("../lsm_backend.zig").Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try @import("../docstore.zig").DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try transactions.TxnManager.init(alloc, &store);
        defer manager.deinit();
        try manager.resolveIntents(@splat(2), .committed, 3);
        var read = try CurrentView.init(&store);
        defer read.deinit();
        try std.testing.expectEqualStrings("b", (try Claim.decode(&a.claimKey(), try read.get(&a.claimKey()))).parent_key);
        try std.testing.expectEqualStrings("a", (try Claim.decode(&b.claimKey(), try read.get(&b.claimKey()))).parent_key);
        try std.testing.expectError(error.UniqueConstraintViolation, prepare(alloc, &read, &.{.{ .address = a, .operation = .{ .establish = first } }}));
    }
}

test "distributed txn deferred reference handoff commits atomically and rejects unseen children" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(path);
    var backend = try @import("../lsm_backend.zig").Backend.open(alloc, path, .{});
    defer backend.close();
    var store = try @import("../docstore.zig").DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var manager = try transactions.TxnManager.init(alloc, &store);
    defer manager.deinit();
    const address = try Address.init(@splat(1), "tuple");
    const old: Claim = .{ .tuple = "tuple", .parent_table = "p", .parent_key = "old", .schema_version = 1 };
    var replacement = old;
    replacement.parent_key = "replacement";
    const child: Reference = .{ .child_table = "c", .child_key = "child", .constraint_name = "fk", .constraint_generation = @splat(2) };
    try testCommands(&store, &manager, @splat(1), &.{ .{ .address = address, .operation = .{ .establish = old } }, .{ .address = address, .operation = .{ .attach = child } } }, true);
    const owner: ClaimOwner = .{ .parent_table = "p", .parent_key = "old" };
    var read = try CurrentView.init(&store);
    defer read.deinit();
    try std.testing.expectError(error.ForeignKeyReferenced, prepare(alloc, &read, &.{ .{ .address = address, .operation = .{ .release = owner } }, .{ .address = address, .operation = .{ .establish = replacement } } }));
    try std.testing.expectError(error.ForeignKeyParentMissing, prepare(alloc, &read, &.{ .{ .address = address, .operation = .{ .detach = child } }, .{ .address = address, .operation = .{ .release = owner } }, .{ .address = address, .operation = .{ .attach = child } } }));
    const commands = [_]Command{
        .{ .address = address, .operation = .{ .attach = child } },
        .{ .address = address, .operation = .{ .establish = replacement } },
        .{ .address = address, .operation = .{ .release = owner } },
        .{ .address = address, .operation = .{ .detach = child } },
    };
    var effects = try prepare(alloc, &read, &commands);
    defer effects.deinit();
    var unseen = child;
    unseen.child_key = "concurrent";
    try testCommands(&store, &manager, @splat(2), &.{.{ .address = address, .operation = .{ .attach = unseen } }}, true);
    try manager.initTransaction(@splat(3), 1);
    try std.testing.expectError(error.ForeignKeyReferenced, testPrepareEffects(&store, &manager, @splat(3), effects.operations));
    try testCommands(&store, &manager, @splat(4), &.{.{ .address = address, .operation = .{ .detach = unseen } }}, true);
    try testPrepareEffects(&store, &manager, @splat(3), effects.operations);
    // The durable prepared decision fences new children until resolution.
    try std.testing.expectError(error.IntentConflict, testCommands(&store, &manager, @splat(5), &.{.{ .address = address, .operation = .{ .attach = unseen } }}, false));
    try manager.resolveIntents(@splat(3), .committed, 3);
    var committed = try CurrentView.init(&store);
    defer committed.deinit();
    try std.testing.expectEqualStrings("replacement", (try Claim.decode(&address.claimKey(), try committed.get(&address.claimKey()))).parent_key);
    _ = try Reference.decode(&try child.key(address), try committed.get(&try child.key(address)));
}
