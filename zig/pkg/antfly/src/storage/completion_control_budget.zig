// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Shared physical shapes used by transaction compilation and native admission.
//! Keeping sizing here avoids a backend dependency on the transaction manager.
const std = @import("std");
const TxnId = [16]u8;

pub const intent_keys_prefix = "\x00\x00__txn_intent_keys__:";
pub const intent_admission_prefix = "\x00\x00__txn_intent_admission__:";
pub const completion_prefix = "\x00\x00__txn_completion_v1__:";
pub const completion_summary_key = "\x00\x00__metadata__:txn_completion_v1";
pub const schema_leases_prefix = "\x00\x00__txn_schema_leases__:";
pub const records_prefix = "\x00\x00__txn_records__:";
pub const participants_prefix = "\x00\x00__txn_participants__:";
pub const resolved_participants_prefix = "\x00\x00__txn_resolved_participants__:";
pub const txn_record_v6_size = 53;

/// Physical metadata traffic for one begin, one decision, and every unique
/// participant acknowledgement. Intent application and native ownership rows
/// are additional costs. This is a sizing input, not a native resource lease.
/// Duplicate commands assigned fresh consensus indices need separate admission;
/// they cannot spend the finite budget for unique lifecycle transitions.
pub const Budget = struct {
    participant_list_bytes: u64,
    acknowledgement_list_bytes: u64,
    mutations: u64,
    operations: u64,
    payload_bytes: u64,
    max_mutation_payload_bytes: u64,
    max_key_bytes: u64,
    max_record_payload_bytes: u64,
    wal_bytes: u64,

    fn add(a: u64, b: u64) !u64 {
        return std.math.add(u64, a, b) catch error.TransactionTooLarge;
    }

    fn mul(a: u64, b: u64) !u64 {
        return std.math.mul(u64, a, b) catch error.TransactionTooLarge;
    }

    pub fn measure(participants: []const []const u8) !Budget {
        if (participants.len > std.math.maxInt(u32)) return error.TransactionTooLarge;
        var list_bytes: u64 = @sizeOf(u32);
        for (participants) |participant| {
            if (participant.len > std.math.maxInt(u32)) return error.TransactionTooLarge;
            list_bytes = try add(list_bytes, try add(@sizeOf(u32), participant.len));
        }
        return fromEncodedList(participants.len, list_bytes);
    }

    pub fn measureEncodedList(encoded: []const u8) !Budget {
        var iterator = try ParticipantIterator.init(encoded);
        while (try iterator.next()) |_| {}
        return fromEncodedList(iterator.count, encoded.len);
    }

    pub fn fromEncodedList(count: u64, list_bytes: u64) !Budget {
        if (count > std.math.maxInt(u32) or list_bytes > std.math.maxInt(u32)) return error.TransactionTooLarge;
        const record = records_prefix.len + @sizeOf(TxnId) + txn_record_v6_size;
        const participants_key = participants_prefix.len + @sizeOf(TxnId);
        const resolved_key = resolved_participants_prefix.len + @sizeOf(TxnId);
        const credit_key = completion_prefix.len + @sizeOf(TxnId);
        const summary = completion_summary_key.len + 16;
        // Begin always has five rows, including the resolved-set tombstone.
        // An empty participant set deletes its sidecar instead of storing a list.
        const begin = try add(record + participants_key + resolved_key + credit_key + 16 + summary, if (count == 0) 0 else list_bytes);
        // Metadata-only resolve writes the record and retires three intent/schema
        // sidecars. Charging ledger retirement here AND each ACK is conservative
        // across follower, coordinator, and empty-participant lifecycles.
        const decision = record + intent_admission_prefix.len + @sizeOf(TxnId) +
            intent_keys_prefix.len + @sizeOf(TxnId) + schema_leases_prefix.len + @sizeOf(TxnId) + credit_key + summary;
        const ack_fixed = resolved_key + credit_key + summary;
        // ACK order is unconstrained. Every prefix is at most the complete
        // encoded list, so N*L bounds all N rewrites in linear sizing time.
        // A fixed multiplier of participant-name bytes does not bound this.
        const acknowledgement_lists = try mul(count, list_bytes);
        const acknowledgements = try add(try mul(count, ack_fixed), acknowledgement_lists);
        const payload = try add(try add(begin, decision), acknowledgements);
        const mutations = try add(2, count);
        const operations = try add(5 + 6, try mul(count, 3));
        // Native WAL v1: 16-byte record header, four-byte row count, and
        // 16-byte row headers. Include the longest supported namespace, docs.
        const wal_bytes = try add(payload, try add(try mul(mutations, 20), try mul(operations, 16 + "docs".len)));
        var max_key_bytes: u64 = 0;
        inline for (.{ records_prefix, participants_prefix, resolved_participants_prefix, completion_prefix, intent_admission_prefix, intent_keys_prefix, schema_leases_prefix }) |prefix|
            max_key_bytes = @max(max_key_bytes, prefix.len + @sizeOf(TxnId));
        max_key_bytes = @max(max_key_bytes, completion_summary_key.len);
        // Keep the largest key distinct from cumulative/list payload sizes:
        // SST bound metadata stores keys, not the participant-list values.
        const max_record_payload_bytes = @max(max_key_bytes, @max(record, @max(summary, @max(credit_key + 16, try add(@max(participants_key, resolved_key), list_bytes)))));
        return .{
            .participant_list_bytes = list_bytes,
            .acknowledgement_list_bytes = acknowledgement_lists,
            .mutations = mutations,
            .operations = operations,
            .payload_bytes = payload,
            .max_mutation_payload_bytes = @max(begin, @max(decision, if (count == 0) 0 else try add(ack_fixed, list_bytes))),
            .max_key_bytes = max_key_bytes,
            .max_record_payload_bytes = max_record_payload_bytes,
            .wal_bytes = wal_bytes,
        };
    }
};

/// Borrowed, allocation-free traversal of the actual transaction sidecar.
/// A caller must consume through null: only then has trailing data been checked.
pub const ParticipantIterator = struct {
    encoded: []const u8,
    count: u32,
    remaining: u32,
    offset: usize = 4,

    pub fn init(encoded: []const u8) !ParticipantIterator {
        if (encoded.len < 4) return error.InvalidTxnRecord;
        if (encoded.len > std.math.maxInt(u32)) return error.TransactionTooLarge;
        const count = std.mem.readInt(u32, encoded[0..4], .little);
        if (@as(u64, count) * 4 > encoded.len - 4) return error.InvalidTxnRecord;
        return .{ .encoded = encoded, .count = count, .remaining = count };
    }

    pub fn next(self: *ParticipantIterator) !?[]const u8 {
        if (self.remaining == 0) {
            if (self.offset != self.encoded.len) return error.InvalidTxnRecord;
            return null;
        }
        if (self.encoded.len - self.offset < 4) return error.InvalidTxnRecord;
        const length = std.mem.readInt(u32, self.encoded[self.offset..][0..4], .little);
        const start = self.offset + 4;
        if (length > self.encoded.len - start) return error.InvalidTxnRecord;
        self.offset = start + length;
        self.remaining -= 1;
        return self.encoded[start..self.offset];
    }
};

test "workload admission completion compiler control budget parses exact physical participant encoding" {
    const bytes = "\x02\x00\x00\x00\x01\x00\x00\x00a\x03\x00\x00\x00b\x00c";
    try std.testing.expectEqualDeep(try Budget.measure(&.{ "a", "b\x00c" }), try Budget.measureEncodedList(bytes));
    var iterator = try ParticipantIterator.init(bytes);
    try std.testing.expectEqualStrings("a", (try iterator.next()).?);
    try std.testing.expectEqualStrings("b\x00c", (try iterator.next()).?);
    try std.testing.expectEqual(@as(?[]const u8, null), try iterator.next());
    try std.testing.expectEqualDeep(try Budget.measure(&.{}), try Budget.measureEncodedList("\x00\x00\x00\x00"));
    for (0..bytes.len) |length|
        try std.testing.expectError(error.InvalidTxnRecord, Budget.measureEncodedList(bytes[0..length]));
    try std.testing.expectError(error.InvalidTxnRecord, Budget.measureEncodedList(bytes ++ "trailing"));
    try std.testing.expectError(error.InvalidTxnRecord, Budget.measureEncodedList("\xff\xff\xff\xff"));
    try std.testing.expectError(error.InvalidTxnRecord, Budget.measureEncodedList("\x01\x00\x00\x00\xff\xff\xff\xff"));
}
