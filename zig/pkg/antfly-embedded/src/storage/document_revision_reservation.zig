// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Precommit document revisions. A reservation burns allocator numbers, never
//! publishes rows, votes PREPARE, or authorizes a mutation. The transaction
//! manager takes existing native row locks with the allocation, preventing a
//! create/delete ABA before the final immutable intent is prepared.
//! The transaction manager owns its pending record and publishes this plan
//! under the same primary apply fence as ordinary document mutations.
const std = @import("std");
const ttl = @import("ttl.zig");
const internal = @import("internal_keys.zig");
const KV = @import("docstore.zig").KVPair;
const Pin = @import("../common/relation_persistence.zig").PhysicalPin;
const External = @import("transaction_decision_owner.zig").External;

pub const header_prefix = "\x00\x00__txn_document_revision_v1__:";
pub const rows_prefix = "\x00\x00__txn_document_revision_rows_v1__:";
pub const max_rows = 4096;
pub const max_key_bytes = 4 * 1024 * 1024;
pub const max_lease_ns = 5 * std.time.ns_per_min;
pub const max_version = std.math.maxInt(i64);

/// Logical SQL ownership, independent of a not-yet-admitted metadata P. These
/// bytes are identity only: the ordinary execution policy remains mandatory.
pub const Scope = struct {
    metadata_incarnation: [32]u8,
    transaction_id: [16]u8,
    principal_digest: [32]u8,
    begin_timestamp: u64,
    table_id: u64,
    shard_id: u64,
    range_id: u64,
    native_pin: ?Pin = null,

    pub const encoded_len = 32 + 16 + 32 + 8 + 24 + 1 + Pin.encoded_len;

    pub fn validate(self: Scope) !void {
        if (self.begin_timestamp == 0 or self.table_id == 0 or self.shard_id == 0 or self.range_id == 0 or
            std.mem.allEqual(u8, &self.transaction_id, 0) or std.mem.allEqual(u8, &self.principal_digest, 0) or
            std.mem.allEqual(u8, &self.metadata_incarnation, '0')) return error.InvalidTxnRecord;
        for (self.metadata_incarnation) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidTxnRecord;
        if (self.native_pin) |pin| {
            try pin.validate();
            if (pin.table_id != self.table_id or pin.shard_id != self.shard_id or pin.range_id != self.range_id) return error.InvalidTxnRecord;
        }
    }

    pub fn encode(self: Scope) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..32], &self.metadata_incarnation);
        @memcpy(bytes[32..48], &self.transaction_id);
        @memcpy(bytes[48..80], &self.principal_digest);
        std.mem.writeInt(u64, bytes[80..88], self.begin_timestamp, .little);
        std.mem.writeInt(u64, bytes[88..96], self.table_id, .little);
        std.mem.writeInt(u64, bytes[96..104], self.shard_id, .little);
        std.mem.writeInt(u64, bytes[104..112], self.range_id, .little);
        if (self.native_pin) |pin| {
            bytes[112] = 1;
            @memcpy(bytes[113..], &try pin.encode());
        }
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Scope {
        if (bytes.len != encoded_len or bytes[112] > 1) return error.InvalidTxnRecord;
        if (bytes[112] == 0 and !std.mem.allEqual(u8, bytes[113..], 0)) return error.InvalidTxnRecord;
        const value: Scope = .{
            .metadata_incarnation = bytes[0..32].*,
            .transaction_id = bytes[32..48].*,
            .principal_digest = bytes[48..80].*,
            .begin_timestamp = std.mem.readInt(u64, bytes[80..88], .little),
            .table_id = std.mem.readInt(u64, bytes[88..96], .little),
            .shard_id = std.mem.readInt(u64, bytes[96..104], .little),
            .range_id = std.mem.readInt(u64, bytes[104..112], .little),
            .native_pin = if (bytes[112] == 1) try Pin.decode(bytes[113..]) else null,
        };
        try value.validate();
        return value;
    }

    pub fn eql(self: Scope, other: Scope) bool {
        const left = self.encode() catch return false;
        const right = other.encode() catch return false;
        return std.mem.eql(u8, &left, &right);
    }
};

/// A compare-only reference retained by SQL before RETURNING. It is not a
/// caller-chosen stamp: PREPARE must rejoin it to native allocation custody.
pub const Reference = struct {
    key: []const u8,
    version: u64,
    pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        try stream.objectField("key");
        try stream.beginArray();
        for (self.key) |byte| try stream.write(byte);
        try stream.endArray();
        try stream.objectField("version");
        try stream.write(self.version);
        try stream.endObject();
    }
};

/// Private immutable preparation demand. Admission must bind these exact
/// compare-only references; the receiving store still loads every stamp.
pub const Handoff = struct {
    scope: Scope,
    references: []const Reference,
    pub fn validate(self: Handoff) !void {
        try self.scope.validate();
        if (self.references.len > max_rows) return error.TransactionTooLarge;
        var bytes: usize = 0;
        for (self.references, 0..) |ref, i| {
            if (ref.key.len == 0 or ref.version == 0 or ref.version > max_version or
                (i != 0 and std.mem.order(u8, self.references[i - 1].key, ref.key) != .lt)) return error.InvalidTxnRecord;
            bytes = std.math.add(usize, bytes, ref.key.len) catch return error.TransactionTooLarge;
            if (bytes > max_key_bytes) return error.TransactionTooLarge;
        }
    }
};

pub const Header = struct {
    scope: Scope,
    expires_at: u64,
    count: u32 = 0,
    key_bytes: u32 = 0,
    /// Once admitted, only the actual metadata decision can release custody.
    owner: ?External = null,
    pub const encoded_len = 4 + Scope.encoded_len + 8 + 4 + 4 + 1 + External.encoded_len;

    pub fn encode(self: Header) ![encoded_len]u8 {
        if (self.expires_at == 0 or self.expires_at == std.math.maxInt(u64) or self.count > max_rows or self.key_bytes > max_key_bytes or
            (self.count == 0) != (self.key_bytes == 0) or self.key_bytes < self.count) return error.InvalidTxnRecord;
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..4], "ADVR");
        @memcpy(bytes[4..][0..Scope.encoded_len], &try self.scope.encode());
        const offset = 4 + Scope.encoded_len;
        std.mem.writeInt(u64, bytes[offset..][0..8], self.expires_at, .little);
        std.mem.writeInt(u32, bytes[offset + 8 ..][0..4], self.count, .little);
        std.mem.writeInt(u32, bytes[offset + 12 ..][0..4], self.key_bytes, .little);
        if (self.owner) |owner| {
            if (!std.mem.eql(u8, &owner.metadata_incarnation, &self.scope.metadata_incarnation)) return error.InvalidTxnRecord;
            bytes[offset + 16] = 1;
            @memcpy(bytes[offset + 17 ..], &try owner.encode());
        }
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Header {
        const offset = 4 + Scope.encoded_len;
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "ADVR") or bytes[offset + 16] > 1) return error.InvalidTxnRecord;
        if (bytes[offset + 16] == 0 and !std.mem.allEqual(u8, bytes[offset + 17 ..], 0)) return error.InvalidTxnRecord;
        const value: Header = .{
            .scope = try Scope.decode(bytes[4..][0..Scope.encoded_len]),
            .expires_at = std.mem.readInt(u64, bytes[offset..][0..8], .little),
            .count = std.mem.readInt(u32, bytes[offset + 8 ..][0..4], .little),
            .key_bytes = std.mem.readInt(u32, bytes[offset + 12 ..][0..4], .little),
            .owner = if (bytes[offset + 16] == 1) try External.decode(bytes[offset + 17 ..]) else null,
        };
        _ = try value.encode();
        return value;
    }
};

pub fn headerKey(id: [16]u8) [header_prefix.len + 16]u8 {
    return header_prefix.* ++ id;
}
pub fn rowPrefix(id: [16]u8) [rows_prefix.len + 17]u8 {
    return rows_prefix.* ++ id ++ [_]u8{':'};
}
pub fn rowKey(a: std.mem.Allocator, id: [16]u8, key: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ &rowPrefix(id), key });
}
fn optional(view: anytype, key: []const u8) !?[]const u8 {
    return view.get(key) catch |err| return if (err == error.NotFound) null else err;
}
pub fn load(view: anytype, id: [16]u8) !?Header {
    return if (try optional(view, &headerKey(id))) |bytes| try Header.decode(bytes) else null;
}
pub fn decodeVersion(bytes: []const u8) !u64 {
    if (bytes.len != 8) return error.InvalidTxnRecord;
    const version = std.mem.readInt(u64, bytes[0..8], .little);
    if (version == 0 or version > max_version) return error.InvalidTxnRecord;
    return version;
}

/// Root-scope rows use eight bytes; nested first touches retain their numeric
/// subtransaction in a further eight. PREPARE appends one canonical used tag.
pub fn rowVersion(bytes: []const u8) !u64 {
    if (bytes.len != 8 and bytes.len != 9 and bytes.len != 16 and bytes.len != 17) return error.InvalidTxnRecord;
    if ((bytes.len == 9 or bytes.len == 17) and bytes[bytes.len - 1] != 1) return error.InvalidTxnRecord;
    if (bytes.len >= 16 and std.mem.readInt(u64, bytes[8..16], .little) == 0) return error.InvalidTxnRecord;
    return decodeVersion(bytes[0..8]);
}
pub fn rowUsed(bytes: []const u8) !bool {
    _ = try rowVersion(bytes);
    return bytes.len == 9 or bytes.len == 17;
}
pub fn rowSubtransaction(bytes: []const u8) !u64 {
    _ = try rowVersion(bytes);
    return if (bytes.len >= 16) std.mem.readInt(u64, bytes[8..16], .little) else 0;
}
pub fn encodeRow(a: std.mem.Allocator, version: u64, subtransaction: u64, used: bool) ![]u8 {
    if (version == 0 or version > max_version) return error.InvalidTxnRecord;
    const base: usize = if (subtransaction == 0) 8 else 16;
    const bytes = try a.alloc(u8, base + @intFromBool(used));
    std.mem.writeInt(u64, bytes[0..8], version, .little);
    if (subtransaction != 0) std.mem.writeInt(u64, bytes[8..16], subtransaction, .little);
    if (used) bytes[base] = 1;
    return bytes;
}

/// Owned finite plan. Only NEW keys are written, plus a fixed-size header and
/// watermark. Repeated statements never rewrite a growing key vector. Values
/// correspond to the caller's strictly increasing key window.
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    writes: []const KV,
    versions: []const u64,
    fresh_indices: []const usize,
    header: Header,
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn prepare(a: std.mem.Allocator, view: anytype, scope: Scope, keys: []const []const u8, now: u64, expires_at: u64) !Prepared {
    return prepareScoped(a, view, scope, keys, now, expires_at, 0);
}
pub fn prepareScoped(a: std.mem.Allocator, view: anytype, scope: Scope, keys: []const []const u8, now: u64, expires_at: u64, subtransaction: u64) !Prepared {
    try scope.validate();
    if (now == 0 or expires_at <= now or expires_at - now > max_lease_ns) return error.ExternalTransactionAdmissionExpired;
    if (keys.len == 0 or keys.len > max_rows) return error.TransactionTooLarge;
    var input_bytes: usize = 0;
    for (keys, 0..) |key, i| {
        if (key.len == 0 or internal.isInternalUserKey(key) or std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or
            std.mem.startsWith(u8, key, "splitstate:") or std.mem.startsWith(u8, key, "splitdelta:") or
            (i != 0 and std.mem.order(u8, keys[i - 1], key) != .lt)) return error.InvalidTxnRecord;
        input_bytes = std.math.add(usize, input_bytes, key.len) catch return error.TransactionTooLarge;
        if (input_bytes > max_key_bytes) return error.TransactionTooLarge;
    }
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    var header = (try load(view, scope.transaction_id)) orelse Header{ .scope = scope, .expires_at = expires_at };
    if (!header.scope.eql(scope)) return error.ExternalTransactionDecisionChanged;
    if (header.owner != null) return error.ExternalTransactionDecisionRequired;
    if (header.expires_at <= now) return error.ExternalTransactionAdmissionExpired;
    // Renewal is explicit and monotone, bounded by the receiver's clock.
    header.expires_at = @max(header.expires_at, expires_at);
    const versions = try alloc.alloc(u64, keys.len);
    const ledger_keys = try alloc.alloc([]const u8, keys.len);
    const values = try alloc.alloc(?[]const u8, keys.len);
    for (keys, ledger_keys) |key, *destination| destination.* = try rowKey(alloc, scope.transaction_id, key);
    try view.getManySorted(ledger_keys, values);
    var fresh = std.ArrayList(usize).empty;
    var probes = std.ArrayList([]const u8).empty;
    try probes.append(alloc, ttl.document_version_watermark_key);
    for (keys, values, 0..) |key, existing, i| {
        if (existing) |bytes| {
            if (try rowUsed(bytes)) return error.ExternalTransactionDecisionRequired;
            versions[i] = try rowVersion(bytes);
        } else {
            header.count = std.math.add(u32, header.count, 1) catch return error.TransactionTooLarge;
            header.key_bytes = std.math.add(u32, header.key_bytes, @intCast(key.len)) catch return error.TransactionTooLarge;
            if (header.count > max_rows or header.key_bytes > max_key_bytes) return error.TransactionTooLarge;
            try fresh.append(alloc, i);
            try probes.append(alloc, try internal.ttlKeyAlloc(alloc, key));
        }
    }
    var writes = std.ArrayList(KV).empty;
    if (fresh.items.len != 0) {
        std.mem.sort([]const u8, probes.items, {}, struct {
            fn less(_: void, left: []const u8, right: []const u8) bool {
                return std.mem.order(u8, left, right) == .lt;
            }
        }.less);
        const found = try alloc.alloc(?[]const u8, probes.items.len);
        try view.getManySorted(probes.items, found);
        const assigned = try ttl.nextDocumentVersionFromMetadata(now, probes.items, found);
        if (assigned > max_version) return error.InvalidTimestamp;
        const encoded = try alloc.alloc(u8, 8);
        std.mem.writeInt(u64, encoded[0..8], assigned, .little);
        try writes.append(alloc, .{ .key = ttl.document_version_watermark_key, .value = encoded });
        const row_bytes = try encodeRow(alloc, assigned, subtransaction, false);
        for (fresh.items) |i| {
            versions[i] = assigned;
            try writes.append(alloc, .{ .key = ledger_keys[i], .value = row_bytes });
        }
    }
    const encoded_header = try alloc.create([Header.encoded_len]u8);
    encoded_header.* = try header.encode();
    try writes.append(alloc, .{ .key = try alloc.dupe(u8, &headerKey(scope.transaction_id)), .value = encoded_header });
    return .{ .arena = arena, .writes = writes.items, .versions = versions, .fresh_indices = fresh.items, .header = header };
}

pub const Cleanup = struct {
    arena: std.heap.ArenaAllocator,
    keys: []const []const u8,
    pub fn deinit(self: *Cleanup) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Key-only, bounded retirement. A corrupt/orphan prefix is refused before
/// publication, without cloning an unbounded prefix of stored row values.
pub fn prepareCleanup(a: std.mem.Allocator, read: anytype, header: Header) !Cleanup {
    const id = header.scope.transaction_id;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    var result = std.ArrayList([]const u8).empty;
    try result.append(alloc, try alloc.dupe(u8, &headerKey(id)));
    const prefix = rowPrefix(id);
    var cursor = try read.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(&prefix);
    var key_bytes: usize = 0;
    while (row) |entry| {
        if (!std.mem.startsWith(u8, entry.key, &prefix)) break;
        const held = header;
        if (result.items.len > held.count or entry.key.len == prefix.len) return error.InvalidTxnRecord;
        key_bytes = std.math.add(usize, key_bytes, entry.key.len - prefix.len) catch return error.InvalidTxnRecord;
        if (key_bytes > held.key_bytes) return error.InvalidTxnRecord;
        _ = try rowVersion(entry.value);
        try result.append(alloc, try alloc.dupe(u8, entry.key));
        row = try cursor.next();
    }
    if (result.items.len - 1 != header.count or key_bytes != header.key_bytes) return error.InvalidTxnRecord;
    return .{ .arena = arena, .keys = result.items };
}

test "document revision reservation codecs reject noncanonical identities bounds and nested revision envelopes" {
    const scope: Scope = .{ .metadata_incarnation = @splat('1'), .transaction_id = @splat(2), .principal_digest = @splat(3), .begin_timestamp = 4, .table_id = 5, .shard_id = 6, .range_id = 7 };
    const encoded = try scope.encode();
    try std.testing.expect(scope.eql(try Scope.decode(&encoded)));
    for (0..encoded.len) |len| try std.testing.expectError(error.InvalidTxnRecord, Scope.decode(encoded[0..len]));
    var corrupt = encoded;
    corrupt[113] = 1;
    try std.testing.expectError(error.InvalidTxnRecord, Scope.decode(&corrupt));
    corrupt = encoded;
    corrupt[0] = 'G';
    try std.testing.expectError(error.InvalidTxnRecord, Scope.decode(&corrupt));
    const header: Header = .{ .scope = scope, .expires_at = 100, .count = 2, .key_bytes = 3 };
    const bytes = try header.encode();
    try std.testing.expectEqualDeep(header, try Header.decode(&bytes));
    for (0..bytes.len) |len| try std.testing.expectError(error.InvalidTxnRecord, Header.decode(bytes[0..len]));
    var invalid = header;
    invalid.count = max_rows + 1;
    try std.testing.expectError(error.InvalidTxnRecord, invalid.encode());
    invalid = header;
    invalid.key_bytes = 1;
    try std.testing.expectError(error.InvalidTxnRecord, invalid.encode());
    var wrong_owner = header;
    wrong_owner.owner = .{ .metadata_incarnation = @splat('2'), .operation_id = @splat(3), .admission_digest = @splat(4), .expires_at_unix_ns = 100 };
    try std.testing.expectError(error.InvalidTxnRecord, wrong_owner.encode());
    var row: [9]u8 = @splat(0);
    try std.testing.expectError(error.InvalidTxnRecord, rowVersion(&row));
    std.mem.writeInt(u64, row[0..8], 1, .little);
    row[8] = 2;
    try std.testing.expectError(error.InvalidTxnRecord, rowVersion(&row));
    row[8] = 1;
    try std.testing.expectEqual(@as(u64, 1), try rowVersion(&row));
}

test "document revision reservation bounds full windows and writes only new keys without growing vector rewrites" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const View = struct {
        values: std.StringHashMap([]const u8),
        alloc: std.mem.Allocator,
        batch_probes: usize = 0,
        probed_keys: usize = 0,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            self.batch_probes += 1;
            self.probed_keys += keys.len;
            for (keys, values, 0..) |key, *value, i| {
                if (i != 0 and std.mem.order(u8, keys[i - 1], key) != .lt) return error.InvalidTxnRecord;
                value.* = self.values.get(key);
            }
        }
        fn apply(self: *@This(), writes: []const KV) !void {
            for (writes) |write| try self.values.put(try self.alloc.dupe(u8, write.key), try self.alloc.dupe(u8, write.value));
        }
    };
    var view: View = .{ .values = .init(alloc), .alloc = alloc };
    const scope: Scope = .{ .metadata_incarnation = @splat('1'), .transaction_id = @splat(2), .principal_digest = @splat(3), .begin_timestamp = 4, .table_id = 5, .shard_id = 6, .range_id = 7 };
    const keys = try alloc.alloc([]const u8, max_rows);
    for (keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(alloc, "row:{d:0>5}", .{i});
    var first = try prepare(a, &view, scope, keys, 20, 100);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, max_rows + 2), first.writes.len);
    try std.testing.expectEqual(@as(usize, 2), view.batch_probes);
    try std.testing.expectEqual(@as(usize, 2 * max_rows + 1), view.probed_keys);
    try view.apply(first.writes);
    view.batch_probes = 0;
    view.probed_keys = 0;
    var retry = try prepare(a, &view, scope, keys, 30, 100);
    defer retry.deinit();
    try std.testing.expectEqual(@as(usize, 1), retry.writes.len);
    try std.testing.expectEqual(@as(usize, 1), view.batch_probes);
    try std.testing.expectEqual(@as(usize, max_rows), view.probed_keys);
    try std.testing.expectEqualSlices(u64, first.versions, retry.versions);
    try std.testing.expectError(error.TransactionTooLarge, prepare(a, &view, scope, &.{"z-over-capacity"}, 30, 100));
    // A rejected new row cannot prevent exact replay of old assignments.
    var after = try prepare(a, &view, scope, keys[0..1], 40, 100);
    defer after.deinit();
    try std.testing.expectEqual(@as(usize, 1), after.writes.len);
    try std.testing.expectEqual(@as(u64, 20), after.versions[0]);
    const huge = try alloc.alloc(u8, max_key_bytes + 1);
    @memset(huge, 'x');
    try std.testing.expectError(error.TransactionTooLarge, prepare(a, &view, scope, &.{huge}, 30, 100));
}

test "document revision reservation handoff JSON preserves opaque reference bytes and exact signed SQL versions" {
    const a = std.testing.allocator;
    const handoff: Handoff = .{ .scope = .{ .metadata_incarnation = @splat('1'), .transaction_id = @splat(2), .principal_digest = @splat(3), .begin_timestamp = 4, .table_id = 5, .shard_id = 6, .range_id = 7 }, .references = &.{.{ .key = "row\x00\xff", .version = 9007199254740993 }} };
    const encoded = try std.json.Stringify.valueAlloc(a, handoff, .{});
    defer a.free(encoded);
    var parsed = try std.json.parseFromSlice(Handoff, a, encoded, .{});
    defer parsed.deinit();
    try parsed.value.validate();
    try std.testing.expectEqualStrings(handoff.references[0].key, parsed.value.references[0].key);
    try std.testing.expectEqual(handoff.references[0].version, parsed.value.references[0].version);
}
