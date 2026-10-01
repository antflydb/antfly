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

//! Data-only relational lifecycle contract. Physical execution stays in its owner.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Operation = struct {
    routing_key: []const u8,
    key: []const u8,
    kind: enum { guard, put, delete },
    value: ?[]const u8 = null,
    /// Exact previous physical bytes; null asserts absence. Guards retain this
    /// comparison through the durable transaction decision, not just preflight.
    expected_value: ?[]const u8 = null,

    /// HA uses the native JSON envelope. Integrity keys and checksummed values
    /// are arbitrary bytes, not UTF-8; encode only these fields as byte arrays.
    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("routing_key");
        try writeBytes(jw, self.routing_key);
        try jw.objectField("key");
        try writeBytes(jw, self.key);
        try jw.objectField("kind");
        try jw.write(@tagName(self.kind));
        try jw.objectField("value");
        if (self.value) |value| try writeBytes(jw, value) else try jw.write(null);
        try jw.objectField("expected_value");
        if (self.expected_value) |value| try writeBytes(jw, value) else try jw.write(null);
        try jw.endObject();
    }

    fn writeBytes(jw: anytype, bytes: []const u8) !void {
        try jw.beginArray();
        for (bytes) |byte| try jw.write(byte);
        try jw.endArray();
    }
};

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
pub fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

pub fn checksum(key: []const u8, bytes: []const u8) Digest {
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
pub fn appendField(alloc: Allocator, out: *std.ArrayList(u8), value: []const u8) !void {
    if (value.len > max_record_bytes) return error.IntegrityRecordTooLarge;
    var length: [4]u8 = undefined;
    std.mem.writeInt(u32, &length, @intCast(value.len), .little);
    try out.appendSlice(alloc, &length);
    try out.appendSlice(alloc, value);
}

pub const Decoder = struct {
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

pub fn finishRecord(alloc: Allocator, out: *std.ArrayList(u8), key: []const u8) ![]u8 {
    if (out.items.len > max_record_bytes - 32) return error.IntegrityRecordTooLarge;
    try out.appendSlice(alloc, &checksum(key, out.items));
    return out.toOwnedSlice(alloc);
}

pub fn recordBody(key: []const u8, value: []const u8, magic: []const u8) ![]const u8 {
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

pub const ClaimOwner = struct { parent_table: []const u8, parent_key: []const u8 };

pub const Command = struct {
    address: Address,
    operation: union(enum) {
        /// Exact pre-state guard for a conflict arbiter. null proves absence;
        /// unlike check_owner this also fences schema/action/tuple state.
        compare_claim: ?Claim,
        establish: Claim,
        check_owner: ClaimOwner,
        attach: Reference,
        detach: Reference,
        repair_detach: Reference,
        release: ClaimOwner,
        repair_release: ClaimOwner,
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
        .compare_claim => |optional| if (optional) |claim| {
            for ([_][]const u8{ claim.tuple, claim.parent_table, claim.parent_key, claim.target_tuple orelse "" }) |field| {
                bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
            }
        },
        .establish => |claim| for ([_][]const u8{ claim.tuple, claim.parent_table, claim.parent_key }) |field| {
            bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
        },
        .attach, .detach, .repair_detach => |reference| for ([_][]const u8{ reference.child_table, reference.child_key, reference.constraint_name }) |field| {
            bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
        },
        .check_owner => |owner| {
            bytes = std.math.add(usize, owner.parent_table.len, owner.parent_key.len) catch return error.TransactionTooLarge;
        },
        .release, .repair_release => |owner| {
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

test "pure integrity contract preserves binary claims and operation ownership" {
    const alloc = std.testing.allocator;
    const tuple = "\x00\xffvalue";
    const address = try Address.init(@splat(17), tuple);
    const primary_key = "\xff\x00parent";
    const encoded = try (Claim{ .tuple = tuple, .parent_table = "parents", .parent_key = primary_key, .schema_version = 1 }).encode(alloc, address);
    defer alloc.free(encoded);
    const key = address.claimKey();
    const decoded = try Claim.decode(&key, encoded);
    try std.testing.expectEqualStrings(primary_key, decoded.parent_key);
    const operation: Operation = .{ .routing_key = &address.routing, .key = &key, .kind = .put, .value = encoded };
    try validateOperation(operation);
    const json = try std.json.Stringify.valueAlloc(alloc, operation, .{});
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(Operation, alloc, json, .{});
    defer parsed.deinit();
    try validateOperation(parsed.value);
    try std.testing.expectEqualStrings(encoded, parsed.value.value.?);
}
