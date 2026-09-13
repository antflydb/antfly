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
const Allocator = std.mem.Allocator;
pub const Operation = @import("types.zig").TransactionIntegrityOperation;
pub const namespace = "\x00\x00__metadata__:relational_integrity:";
pub const Generation = [16]u8;
pub const Digest = [32]u8;
pub const key_len = namespace.len + 1 + 32 + 16 + 32;
pub const max_record_bytes = 1024 * 1024;
pub const max_commands = 4096;
pub const max_preparation_bytes = 16 * 1024 * 1024;
pub const Kind = enum(u8) { claim = 1, reference = 2, job = 3 };

/// Apply-fenced point probes plus a current cursor avoid cloning the mutable
/// LSM memtable just to validate one claim or the first remaining reference.
/// The caller must retain its apply fence until this view is closed.
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

fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

fn checksum(key: []const u8, bytes: []const u8) Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update(key);
    state.update(bytes);
    var result: Digest = undefined;
    state.final(&result);
    return result;
}

pub const Address = struct {
    routing: Digest,
    generation: Generation,
    tuple_digest: Digest,

    pub fn init(generation: Generation, tuple: []const u8) !Address {
        if (std.mem.allEqual(u8, &generation, 0) or tuple.len == 0 or tuple.len > max_record_bytes) return error.InvalidIntegrityAddress;
        return fromDigest(generation, hash(tuple));
    }

    fn fromDigest(generation: Generation, tuple_digest: Digest) Address {
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly relational integrity routing v1");
        state.update(&generation);
        state.update(&tuple_digest);
        var routing: Digest = undefined;
        state.final(&routing);
        return .{ .routing = routing, .generation = generation, .tuple_digest = tuple_digest };
    }

    pub fn routingKey(self: Address) Digest {
        return self.routing;
    }

    pub fn key(self: Address, kind: Kind) [key_len]u8 {
        var out: [key_len]u8 = undefined;
        @memcpy(out[0..namespace.len], namespace);
        out[namespace.len] = @intFromEnum(kind);
        @memcpy(out[namespace.len + 1 ..][0..32], &self.routing);
        @memcpy(out[namespace.len + 33 ..][0..16], &self.generation);
        @memcpy(out[namespace.len + 49 ..][0..32], &self.tuple_digest);
        return out;
    }

    pub fn claimKey(self: Address) [key_len]u8 {
        return self.key(.claim);
    }
    pub fn referencePrefix(self: Address) [key_len]u8 {
        return self.key(.reference);
    }
    pub fn jobKey(self: Address) [key_len]u8 {
        return self.key(.job);
    }

    pub fn verifyTuple(self: Address, tuple: []const u8) !void {
        const expected = try init(self.generation, tuple);
        if (!std.mem.eql(u8, &self.routing, &expected.routing) or !std.mem.eql(u8, &self.tuple_digest, &expected.tuple_digest)) return error.IntegrityAddressMismatch;
    }
};

pub const ParsedKey = struct { address: Address, kind: Kind };

pub fn isKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, namespace);
}

pub fn parseKey(key: []const u8) !ParsedKey {
    if (!isKey(key) or key.len < key_len) return error.InvalidIntegrityKey;
    const kind: Kind = switch (key[namespace.len]) {
        1 => .claim,
        2 => .reference,
        3 => .job,
        else => return error.InvalidIntegrityKey,
    };
    if (key.len != key_len + @as(usize, if (kind == .reference) 32 else 0)) return error.InvalidIntegrityKey;
    const generation = key[namespace.len + 33 ..][0..16].*;
    if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidIntegrityKey;
    const address = Address.fromDigest(generation, key[namespace.len + 49 ..][0..32].*);
    if (!std.mem.eql(u8, &address.routing, key[namespace.len + 1 ..][0..32])) return error.InvalidIntegrityKey;
    return .{ .address = address, .kind = kind };
}

pub fn routingKey(key: []const u8) !Digest {
    return (try parseKey(key)).address.routing;
}

/// Range movement and restore must validate private records, then assign them
/// through the same explicit logical routing key as transaction admission.
/// Never assign these records by their physical metadata-prefix ordering.
pub fn validateTransferRecord(key: []const u8, value: []const u8) !Address {
    const parsed = try parseKey(key);
    switch (parsed.kind) {
        .claim => _ = try Claim.decode(key, value),
        .reference => _ = try Reference.decode(key, value),
        .job => _ = try Job.decode(key, value),
    }
    return parsed.address;
}

/// Execute once the whole staged ownership range has arrived. Page callers
/// check each record without reconstructing any JSON row. Cross-table primary
/// dependencies still require the distributed restore/activation barrier.
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

fn appendField(alloc: Allocator, out: *std.ArrayList(u8), value: []const u8) !void {
    if (value.len > max_record_bytes) return error.IntegrityRecordTooLarge;
    var length: [4]u8 = undefined;
    std.mem.writeInt(u32, &length, @intCast(value.len), .little);
    try out.appendSlice(alloc, &length);
    try out.appendSlice(alloc, value);
}

const Decoder = struct {
    bytes: []const u8,
    offset: usize = 0,
    fn field(self: *Decoder) ![]const u8 {
        if (self.bytes.len - self.offset < 4) return error.InvalidIntegrityRecord;
        const length = std.mem.readInt(u32, self.bytes[self.offset..][0..4], .little);
        self.offset += 4;
        if (length > self.bytes.len - self.offset) return error.InvalidIntegrityRecord;
        const result = self.bytes[self.offset..][0..length];
        self.offset += length;
        return result;
    }
    fn finish(self: Decoder) !void {
        if (self.offset != self.bytes.len) return error.InvalidIntegrityRecord;
    }
};

fn finishRecord(alloc: Allocator, out: *std.ArrayList(u8), key: []const u8) ![]u8 {
    if (out.items.len > max_record_bytes - 32) return error.IntegrityRecordTooLarge;
    try out.appendSlice(alloc, &checksum(key, out.items));
    return out.toOwnedSlice(alloc);
}

fn recordBody(key: []const u8, value: []const u8, magic: []const u8) ![]const u8 {
    if (value.len < magic.len + 32 or value.len > max_record_bytes or !std.mem.startsWith(u8, value, magic)) return error.InvalidIntegrityRecord;
    const body = value[0 .. value.len - 32];
    if (!std.mem.eql(u8, value[value.len - 32 ..], &checksum(key, body))) return error.IntegrityChecksumMismatch;
    return body[magic.len..];
}

/// The job describes the parent event. Individual referencing constraints may
/// have different RESTRICT/CASCADE/SET NULL policies; those are resolved from
/// each reference's immutable constraint generation during fenced preflight.
pub const Action = enum(u8) { restrict = 0, parent_delete = 1, parent_update = 2 };
pub const Claim = struct {
    tuple: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    schema_version: u32,
    state: enum(u8) { live = 0, draining = 1 } = .live,
    action: Action = .restrict,
    action_id: Generation = @splat(0),
    /// NULL means delete; an update keeps the new canonical tuple here.
    target_tuple: ?[]const u8 = null,

    pub fn encode(self: Claim, alloc: Allocator, address: Address) ![]u8 {
        try address.verifyTuple(self.tuple);
        if (self.parent_table.len == 0 or self.parent_key.len == 0 or
            (self.state == .live and (self.action != .restrict or self.target_tuple != null or !std.mem.allEqual(u8, &self.action_id, 0))) or
            (self.state == .draining and (self.action == .restrict or std.mem.allEqual(u8, &self.action_id, 0))) or
            (self.action == .parent_update and self.target_tuple == null) or
            (self.action == .parent_delete and self.target_tuple != null)) return error.InvalidIntegrityRecord;
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(alloc);
        try out.appendSlice(alloc, "AFC1");
        try out.appendSlice(alloc, &.{ @intFromEnum(self.state), @intFromEnum(self.action), @intFromBool(self.target_tuple != null), 0 });
        var version: [4]u8 = undefined;
        std.mem.writeInt(u32, &version, self.schema_version, .little);
        try out.appendSlice(alloc, &version);
        try out.appendSlice(alloc, &self.action_id);
        for ([_][]const u8{ self.tuple, self.parent_table, self.parent_key, self.target_tuple orelse "" }) |field| try appendField(alloc, &out, field);
        return finishRecord(alloc, &out, &address.claimKey());
    }

    pub fn decode(key: []const u8, value: []const u8) !Claim {
        const parsed = try parseKey(key);
        if (parsed.kind != .claim) return error.InvalidIntegrityKey;
        const body = try recordBody(key, value, "AFC1");
        if (body.len < 24 or body[0] > 1 or body[1] > 2 or body[2] > 1 or body[3] != 0) return error.InvalidIntegrityRecord;
        var reader: Decoder = .{ .bytes = body[24..] };
        const result: Claim = .{
            .state = if (body[0] == 0) .live else .draining,
            .action = switch (body[1]) {
                0 => .restrict,
                1 => .parent_delete,
                2 => .parent_update,
                else => unreachable,
            },
            .schema_version = std.mem.readInt(u32, body[4..8], .little),
            .action_id = body[8..24].*,
            .tuple = try reader.field(),
            .parent_table = try reader.field(),
            .parent_key = try reader.field(),
            .target_tuple = blk: {
                const field = try reader.field();
                if (body[2] == 0 and field.len != 0) return error.InvalidIntegrityRecord;
                break :blk if (body[2] != 0) field else null;
            },
        };
        try reader.finish();
        try parsed.address.verifyTuple(result.tuple);
        if (result.parent_table.len == 0 or result.parent_key.len == 0 or
            (result.state == .live and (result.action != .restrict or result.target_tuple != null or !std.mem.allEqual(u8, &result.action_id, 0))) or
            (result.state == .draining and (result.action == .restrict or std.mem.allEqual(u8, &result.action_id, 0))) or
            (result.action == .parent_update and result.target_tuple == null) or
            (result.action == .parent_delete and result.target_tuple != null)) return error.InvalidIntegrityRecord;
        return result;
    }
};

pub const Reference = struct {
    child_table: []const u8,
    child_key: []const u8,
    constraint_name: []const u8,
    constraint_generation: Generation,

    pub fn key(self: Reference, address: Address) ![key_len + 32]u8 {
        if (self.child_table.len == 0 or self.child_key.len == 0 or self.constraint_name.len == 0 or std.mem.allEqual(u8, &self.constraint_generation, 0)) return error.InvalidIntegrityRecord;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update(&self.constraint_generation);
        for ([_][]const u8{ self.child_table, self.child_key, self.constraint_name }) |field| {
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, field.len, .little);
            state.update(&length);
            state.update(field);
        }
        var out: [key_len + 32]u8 = undefined;
        @memcpy(out[0..key_len], &address.referencePrefix());
        state.final(out[key_len..]);
        return out;
    }

    pub fn encode(self: Reference, alloc: Allocator, address: Address) ![]u8 {
        const physical = try self.key(address);
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(alloc);
        try out.appendSlice(alloc, "AFR1");
        try out.appendSlice(alloc, &self.constraint_generation);
        for ([_][]const u8{ self.child_table, self.child_key, self.constraint_name }) |field| try appendField(alloc, &out, field);
        return finishRecord(alloc, &out, &physical);
    }

    pub fn decode(key_bytes: []const u8, value: []const u8) !Reference {
        const parsed = try parseKey(key_bytes);
        if (parsed.kind != .reference) return error.InvalidIntegrityKey;
        const body = try recordBody(key_bytes, value, "AFR1");
        if (body.len < 16) return error.InvalidIntegrityRecord;
        var reader: Decoder = .{ .bytes = body[16..] };
        const result: Reference = .{ .constraint_generation = body[0..16].*, .child_table = try reader.field(), .child_key = try reader.field(), .constraint_name = try reader.field() };
        try reader.finish();
        if (!std.mem.eql(u8, key_bytes, &(try result.key(parsed.address)))) return error.IntegrityAddressMismatch;
        return result;
    }
};

pub const Command = struct {
    address: Address,
    operation: union(enum) {
        establish: Claim,
        check_owner: struct { parent_table: []const u8, parent_key: []const u8 },
        attach: Reference,
        detach: Reference,
        release: struct { parent_table: []const u8, parent_key: []const u8 },
        start_action: struct { parent_table: []const u8, parent_key: []const u8, action: Action, action_id: Generation, target_tuple: ?[]const u8 = null, parent_version: u64, parent_schema_version: u32, parent_value: ?[]const u8 = null },
        finish_action: Generation,
        advance_validation: struct { action_id: Generation, expected_cursor: []const u8, expected_rows: u64, cursor: []const u8, rows: u64, complete: bool, failure: ?[]const u8 = null },
        retry_action: Generation,
        cancel_action: Generation,
    },

    pub fn jsonStringify(self: Command, stream: anytype) @TypeOf(stream.*).Error!void {
        return @import("relational_integrity_json.zig").write(self, stream);
    }
};

/// Conservative credits include encoded records, ownership copies, key maps,
/// predicates and temporary tuple/JSON envelopes. Checked before allocating
/// the preparation arena; fetched existing values receive additional credits.
pub fn commandAdmissionBytes(command: Command) !usize {
    var bytes: usize = 0;
    switch (command.operation) {
        .establish => |claim| for ([_][]const u8{ claim.tuple, claim.parent_table, claim.parent_key }) |field| {
            bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
        },
        .attach, .detach => |reference| for ([_][]const u8{ reference.child_table, reference.child_key, reference.constraint_name }) |field| {
            bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
        },
        .check_owner => |owner| {
            bytes = std.math.add(usize, owner.parent_table.len, owner.parent_key.len) catch return error.TransactionTooLarge;
        },
        .release => |owner| {
            bytes = std.math.add(usize, owner.parent_table.len, owner.parent_key.len) catch return error.TransactionTooLarge;
        },
        .start_action => |action| for ([_][]const u8{ action.parent_table, action.parent_key, action.target_tuple orelse "", action.parent_value orelse "" }) |field| {
            bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
        },
        .advance_validation => |advance| for ([_][]const u8{ advance.expected_cursor, advance.cursor, advance.failure orelse "" }) |field| {
            bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
        },
        else => {},
    }
    return std.math.add(usize, 4096, std.math.mul(usize, bytes, 16) catch return error.TransactionTooLarge) catch error.TransactionTooLarge;
}

pub fn validateCommandAdmission(commands: []const Command) !usize {
    if (commands.len > max_commands) return error.TransactionTooLarge;
    var bytes: usize = 0;
    for (commands) |command| bytes = std.math.add(usize, bytes, try commandAdmissionBytes(command)) catch return error.TransactionTooLarge;
    if (bytes > max_preparation_bytes) return error.TransactionTooLarge;
    return bytes;
}

/// Owned, coalesced physical intents and read dependencies. The caller must
/// hold its mutation fence through prepare so the prefix proof cannot race.
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
            _ = try Reference.decode(item.key, item.value);
            return error.ForeignKeyReferenced;
        }
    }
};

fn requireOwner(claim: Claim, table: []const u8, key: []const u8) !void {
    if (!std.mem.eql(u8, claim.parent_table, table) or !std.mem.eql(u8, claim.parent_key, key)) return error.UniqueConstraintViolation;
}

pub const Job = struct {
    action_id: Generation,
    parent_version: u64,
    parent_schema_version: u32,
    parent_value: ?[]const u8 = null,
    phase: enum(u8) { validating = 0, applying = 1, failed = 2 } = .validating,
    cursor: []const u8 = "",
    rows_validated: u64 = 0,
    failure: []const u8 = "",

    pub fn encode(self: Job, alloc: Allocator, address: Address) ![]u8 {
        if (self.parent_version == 0 or std.mem.allEqual(u8, &self.action_id, 0) or self.failure.len > 4096 or
            (self.phase == .applying and self.cursor.len != 0) or (self.phase != .failed and self.failure.len != 0) or
            (self.cursor.len != 0 and (!std.mem.startsWith(u8, self.cursor, &address.referencePrefix()) or self.cursor.len != key_len + 32))) return error.InvalidIntegrityRecord;
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(alloc);
        try out.appendSlice(alloc, "AFJ1");
        try out.appendSlice(alloc, &self.action_id);
        try out.appendSlice(alloc, &.{ @intFromEnum(self.phase), 0, 0, 0 });
        var rows: [8]u8 = undefined;
        std.mem.writeInt(u64, &rows, self.rows_validated, .little);
        try out.appendSlice(alloc, &rows);
        std.mem.writeInt(u64, &rows, self.parent_version, .little);
        try out.appendSlice(alloc, &rows);
        var schema_version: [4]u8 = undefined;
        std.mem.writeInt(u32, &schema_version, self.parent_schema_version, .little);
        try out.appendSlice(alloc, &schema_version);
        try out.appendSlice(alloc, &.{ @intFromBool(self.parent_value != null), 0, 0, 0 });
        try appendField(alloc, &out, self.cursor);
        try appendField(alloc, &out, self.failure);
        try appendField(alloc, &out, self.parent_value orelse "");
        return finishRecord(alloc, &out, &address.jobKey());
    }

    pub fn decode(key: []const u8, value: []const u8) !Job {
        const parsed = try parseKey(key);
        if (parsed.kind != .job) return error.InvalidIntegrityKey;
        const body = try recordBody(key, value, "AFJ1");
        if (body.len < 44 or body[16] > 2 or !std.mem.allEqual(u8, body[17..20], 0) or std.mem.allEqual(u8, body[0..16], 0) or body[40] > 1 or !std.mem.allEqual(u8, body[41..44], 0)) return error.InvalidIntegrityRecord;
        var reader: Decoder = .{ .bytes = body[44..] };
        const result: Job = .{ .action_id = body[0..16].*, .phase = switch (body[16]) {
            0 => .validating,
            1 => .applying,
            2 => .failed,
            else => unreachable,
        }, .rows_validated = std.mem.readInt(u64, body[20..28], .little), .parent_version = std.mem.readInt(u64, body[28..36], .little), .parent_schema_version = std.mem.readInt(u32, body[36..40], .little), .cursor = try reader.field(), .failure = try reader.field(), .parent_value = blk: {
            const field = try reader.field();
            if (body[40] == 0 and field.len != 0) return error.InvalidIntegrityRecord;
            break :blk if (body[40] != 0) field else null;
        } };
        try reader.finish();
        if (result.parent_version == 0 or result.failure.len > 4096 or (result.phase == .applying and result.cursor.len != 0) or (result.phase != .failed and result.failure.len != 0) or
            (result.cursor.len != 0 and (!std.mem.startsWith(u8, result.cursor, &parsed.address.referencePrefix()) or result.cursor.len != key_len + 32))) return error.InvalidIntegrityRecord;
        return result;
    }
};

pub fn prepare(alloc: Allocator, txn: anytype, commands: []const Command) !Effects {
    const admission_bytes = try validateCommandAdmission(commands);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var builder: Builder = .{ .alloc = owned, .retained_bytes = admission_bytes };
    // Detaches first make RESTRICT independent of caller row ordering. They
    // may not create a nonexistent parent; creation/attachment remain ordered.
    for (commands) |command| if (command.operation == .detach) {
        const claim_key = command.address.claimKey();
        const raw = try builder.current(txn, &claim_key) orelse return error.ForeignKeyParentMissing;
        _ = try Claim.decode(&claim_key, raw);
        try builder.add(txn, command.address, &claim_key, .guard, null);
        const reference = command.operation.detach;
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
                .detach => continue,
                .check_owner => 0,
                .release => 1,
                .establish => 2,
                .attach => 3,
                else => 4,
            };
            if (command_phase != phase) continue;
            const address = command.address;
            const claim_key = address.claimKey();
            switch (command.operation) {
                .detach => {},
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
                .release => |owner| {
                    const raw = try builder.current(txn, &claim_key) orelse return error.ForeignKeyParentMissing;
                    const claim = try Claim.decode(&claim_key, raw);
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
pub fn validateOperation(op: Operation) !void {
    const parsed = try parseKey(op.key);
    if (!std.mem.eql(u8, op.routing_key, &parsed.address.routing) or (op.kind != .put and op.value != null)) return error.InvalidIntegrityOperation;
    for ([_]?[]const u8{ op.expected_value, op.value }) |maybe| if (maybe) |raw| switch (parsed.kind) {
        .claim => _ = try Claim.decode(op.key, raw),
        .reference => _ = try Reference.decode(op.key, raw),
        .job => {
            _ = try Job.decode(op.key, raw);
        },
    };
    if (op.kind == .put and op.value == null) return error.InvalidIntegrityOperation;
}

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
        _ = try Reference.decode(item.key, item.value);
        return error.ForeignKeyReferenced;
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
                            try requireEmptyOperations(txn, parsed.address, operations, &overlay);
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
        if (references.items.len == max_rows or item_bytes > max_bytes - bytes or
            (references.items.len != 0 and time.monotonicNs() -| started >= budget.time_ns))
        {
            if (references.items.len == 0) return error.IntegrityRecordTooLarge;
            complete = false;
            break;
        }
        const value = try owned.dupe(u8, item.value);
        try references.append(owned, try Reference.decode(item.key, value));
        next_cursor = try owned.dupe(u8, item.key);
        bytes += item_bytes;
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
