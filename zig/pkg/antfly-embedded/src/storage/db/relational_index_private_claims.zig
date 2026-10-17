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

//! Generation-owned inventory for unpublished UNIQUE claims. Callers retain
//! apply exclusion and commit these primitives with their ordered durable
//! effect/outbox. No API here authenticates metadata or acknowledges a cohort.
const std = @import("std");
const jobs = @import("relational_index_owner_job.zig");
const catalog = @import("relational_integrity_catalog.zig");
const integrity = @import("relational_integrity_contract.zig");
const Allocator = std.mem.Allocator;
pub const prefix = jobs.key ++ ":claim-inventory:";
pub const registration_prefix = jobs.key ++ ":claim-registration:";
const authority = @import("../source_authority.zig");
const Native = @import("receipt_position.zig").Native;
const Position = @import("receipt_position.zig").Position;
pub const max_claims = 128;
pub const delivery = @import("relational_index_claim_delivery.zig");

/// Metadata-owned frozen input, not a physical inventory certificate. The
/// receiver obtains this independently; core must still validate its actual
/// generation, consumed source and claim companions before sealing anything.
pub const TargetExpectation = struct {
    metadata_incarnation: integrity.Digest,
    binding_digest: integrity.Digest,
    generation: integrity.Generation,
    completion_revision: u64,
    source_consumption_digest: integrity.Digest,
    source_progress_digest: integrity.Digest,
    table_name_digest: integrity.Digest,
    rows: u64,
    inventory_xor: integrity.Digest,
    provenance: integrity.Digest,

    pub fn validate(self: TargetExpectation) !void {
        for (self.metadata_incarnation) |byte| if (!(byte >= '0' and byte <= '9') and !(byte >= 'a' and byte <= 'f')) return error.InvalidPrivateIndexClaimInventory;
        inline for (.{ "binding_digest", "generation", "source_consumption_digest", "source_progress_digest", "table_name_digest" }) |field| if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidPrivateIndexClaimInventory;
        if (self.completion_revision == 0 or
            (self.rows == 0 and (!std.mem.allEqual(u8, &self.inventory_xor, 0) or !std.mem.allEqual(u8, &self.provenance, 0))) or
            (self.rows != 0 and std.mem.allEqual(u8, &self.provenance, 0))) return error.InvalidPrivateIndexClaimInventory;
    }
    pub fn requireBinding(self: TargetExpectation, binding: jobs.Binding) !void {
        try self.validate();
        if (binding.authority != .native or binding.phase != .reserved or !std.mem.eql(u8, &self.binding_digest, &try bindingDigest(binding))) return error.PrivateIndexOwnerJobChanged;
    }
    pub fn digest(self: TargetExpectation) !integrity.Digest {
        try self.validate();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-target-expectation/v1");
        inline for (.{ "metadata_incarnation", "binding_digest", "generation", "source_consumption_digest", "source_progress_digest", "table_name_digest", "inventory_xor", "provenance" }) |field| state.update(&@field(self, field));
        var numbers: [16]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.completion_revision, .little);
        std.mem.writeInt(u64, numbers[8..16], self.rows, .little);
        state.update(&numbers);
        var result: integrity.Digest = undefined;
        state.final(&result);
        return result;
    }
};

/// Durable source pages, independent of mirror transport acknowledgments.
/// Headers stay compact for recovery; payloads are fetched only for delivery.
/// This queue grants neither target ownership nor publication authority.
pub const SourceStream = struct {
    pub const head_key = jobs.key ++ ":claim-source-head";
    pub const consumption_key = jobs.key ++ ":claim-source-consumption";
    pub const batch_prefix = jobs.key ++ ":claim-source-batch:";
    pub const max_pending = 64;
    pub const max_pending_bytes = 8 * 1024 * 1024;
    pub const max_payload_bytes = 1024 * 1024;
    pub const header_len = 320;
    pub const Key = [batch_prefix.len + 25]u8;
    pub const State = enum(u8) { building, ready, failed };
    pub const Row = struct { key: []const u8, tuple: []const u8 };

    /// Metadata-owned decision data, not a caller verification flag. The
    /// receiving server obtains it independently from current metadata before
    /// invoking native consumption. Core still verifies the exact local batch.
    pub const Accepted = struct {
        pub const encoded_len = 304;
        metadata_incarnation: integrity.Digest,
        binding_digest: integrity.Digest,
        scope_digest: integrity.Digest,
        ordinal: u64,
        rows: u64,
        batch_rows: u64,
        source_sequence: u64,
        batch_digest: integrity.Digest,
        logical_digest: integrity.Digest,
        previous_decision: integrity.Digest,
        decision_digest: integrity.Digest,
        state: State,
        pub fn computedDigest(self: Accepted) integrity.Digest {
            var state = std.crypto.hash.Blake3.init(.{});
            state.update("antfly/private-unique-accepted-decision/v1");
            inline for (.{ "metadata_incarnation", "binding_digest", "scope_digest", "batch_digest", "logical_digest", "previous_decision" }) |field| state.update(&@field(self, field));
            var numbers: [32]u8 = undefined;
            inline for (.{ "ordinal", "rows", "batch_rows", "source_sequence" }, 0..) |field, i| std.mem.writeInt(u64, numbers[i * 8 ..][0..8], @field(self, field), .little);
            state.update(&numbers);
            state.update(&.{@backingInt(self.state)});
            var result: integrity.Digest = undefined;
            state.final(&result);
            return result;
        }
        pub fn validate(self: Accepted) !void {
            inline for (.{ "metadata_incarnation", "binding_digest", "scope_digest", "batch_digest", "logical_digest" }) |field| if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidPrivateIndexClaimReceipt;
            if (self.ordinal == 0 or self.source_sequence == 0 or self.batch_rows > max_claims or self.rows < self.batch_rows or
                @as(u128, self.rows) > @as(u128, self.ordinal) * max_claims or self.state == .failed or
                (self.ordinal == 1 and (self.rows != self.batch_rows or !std.mem.allEqual(u8, &self.previous_decision, 0))) or
                (self.ordinal > 1 and std.mem.allEqual(u8, &self.previous_decision, 0)) or
                !std.mem.eql(u8, &self.computedDigest(), &self.decision_digest)) return error.InvalidPrivateIndexClaimReceipt;
        }
        pub fn requireFor(self: Accepted, binding: jobs.Binding, header: Header) !void {
            try self.validate();
            _ = try Header.decode(&header.bytes, binding, self.ordinal);
            if (binding.phase != .reserved or !std.mem.eql(u8, &self.binding_digest, &try bindingDigest(binding)) or
                !std.mem.eql(u8, &self.scope_digest, header.bytes[40..72]) or self.batch_rows != std.mem.readInt(u32, header.bytes[96..100], .little) or
                self.source_sequence != try orderedSequence(binding, try header.orderedPosition()) or !std.mem.eql(u8, &self.batch_digest, &header.digest()) or
                @backingInt(self.state) != header.bytes[184]) return error.InvalidPrivateIndexClaimReceipt;
        }
        pub fn encode(self: Accepted) ![encoded_len]u8 {
            try self.validate();
            var bytes: [encoded_len]u8 = @splat(0);
            @memcpy(bytes[0..8], "AICA\x01\x00\x00\x00");
            inline for (.{ "metadata_incarnation", "binding_digest", "scope_digest" }, 0..) |field, i| @memcpy(bytes[8 + i * 32 ..][0..32], &@field(self, field));
            inline for (.{ "ordinal", "rows", "batch_rows", "source_sequence" }, 0..) |field, i| std.mem.writeInt(u64, bytes[104 + i * 8 ..][0..8], @field(self, field), .little);
            inline for (.{ "batch_digest", "logical_digest", "previous_decision", "decision_digest" }, 0..) |field, i| @memcpy(bytes[136 + i * 32 ..][0..32], &@field(self, field));
            bytes[264] = @backingInt(self.state);
            @memcpy(bytes[272..304], &hash(bytes[0..272]));
            return bytes;
        }
        pub fn decode(bytes: []const u8) !Accepted {
            if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AICA\x01\x00\x00\x00") or !std.mem.allEqual(u8, bytes[265..272], 0) or !std.mem.eql(u8, bytes[272..304], &hash(bytes[0..272]))) return error.InvalidPrivateIndexClaimReceipt;
            var result: Accepted = undefined;
            inline for (.{ "metadata_incarnation", "binding_digest", "scope_digest" }, 0..) |field, i| @field(result, field) = bytes[8 + i * 32 ..][0..32].*;
            inline for (.{ "ordinal", "rows", "batch_rows", "source_sequence" }, 0..) |field, i| @field(result, field) = std.mem.readInt(u64, bytes[104 + i * 8 ..][0..8], .little);
            inline for (.{ "batch_digest", "logical_digest", "previous_decision", "decision_digest" }, 0..) |field, i| @field(result, field) = bytes[136 + i * 32 ..][0..32].*;
            result.state = std.enums.fromInt(State, bytes[264]) orelse return error.InvalidPrivateIndexClaimReceipt;
            try result.validate();
            return result;
        }
    };

    pub const ConsumedPrefix = struct {
        metadata_incarnation: integrity.Digest,
        binding_digest: integrity.Digest,
        ordinal: u64,
        batch_digest: integrity.Digest,
        decision_digest: integrity.Digest,
        rows: u64,
        position: Position,
        pub fn digest(self: ConsumedPrefix) !integrity.Digest {
            try self.position.validate();
            if (self.ordinal == 0 or std.mem.allEqual(u8, &self.metadata_incarnation, 0) or std.mem.allEqual(u8, &self.binding_digest, 0) or std.mem.allEqual(u8, &self.batch_digest, 0) or std.mem.allEqual(u8, &self.decision_digest, 0)) return error.InvalidPrivateIndexClaimReceipt;
            var state = std.crypto.hash.Blake3.init(.{});
            state.update("antfly/private-unique-source-consumed/v1");
            inline for (.{ "metadata_incarnation", "binding_digest", "batch_digest", "decision_digest" }) |field| state.update(&@field(self, field));
            var numbers: [16]u8 = undefined;
            std.mem.writeInt(u64, numbers[0..8], self.ordinal, .little);
            std.mem.writeInt(u64, numbers[8..16], self.rows, .little);
            state.update(&numbers);
            state.update(&try self.position.encode());
            var result: integrity.Digest = undefined;
            state.final(&result);
            return result;
        }
    };

    /// Independent retained anchor: deleting a source header never removes
    /// the decision, original source cut, or later atomic consumption cut.
    pub const Consumption = struct {
        pub const encoded_len = 736;
        accepted: Accepted,
        header: Header,
        position: Position,
        previous_digest: integrity.Digest,
        pub fn prefix(self: Consumption) ConsumedPrefix {
            return .{ .metadata_incarnation = self.accepted.metadata_incarnation, .binding_digest = self.accepted.binding_digest, .ordinal = self.accepted.ordinal, .batch_digest = self.accepted.batch_digest, .decision_digest = self.accepted.decision_digest, .rows = self.accepted.rows, .position = self.position };
        }
        pub fn requireFor(self: Consumption, binding: jobs.Binding) !void {
            try self.accepted.requireFor(binding, self.header);
            const sequence = try orderedSequence(binding, self.position);
            if (sequence <= self.accepted.source_sequence or self.position.term() < (try self.header.orderedPosition()).term() or
                (self.accepted.ordinal == 1) != std.mem.allEqual(u8, &self.previous_digest, 0)) return error.InvalidPrivateIndexClaimInventory;
        }
        pub fn encode(self: Consumption, binding: jobs.Binding) ![encoded_len]u8 {
            try self.requireFor(binding);
            var bytes: [encoded_len]u8 = @splat(0);
            @memcpy(bytes[0..8], "AICC\x01\x00\x00\x00");
            @memcpy(bytes[8..312], &try self.accepted.encode());
            @memcpy(bytes[312..632], &self.header.bytes);
            @memcpy(bytes[632..665], &try self.position.encode());
            @memcpy(bytes[672..704], &self.previous_digest);
            @memcpy(bytes[704..736], &hash(bytes[0..704]));
            return bytes;
        }
        pub fn decode(bytes: []const u8, binding: jobs.Binding) !Consumption {
            if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AICC\x01\x00\x00\x00") or !std.mem.allEqual(u8, bytes[665..672], 0) or !std.mem.eql(u8, bytes[704..736], &hash(bytes[0..704]))) return error.InvalidPrivateIndexClaimInventory;
            const accepted = try Accepted.decode(bytes[8..312]);
            const cut = try @import("receipt_position.zig").Position.decode(bytes[632..665]);
            const result: Consumption = .{ .accepted = accepted, .header = try Header.decode(bytes[312..632], binding, accepted.ordinal), .position = cut, .previous_digest = bytes[672..704].* };
            try result.requireFor(binding);
            return result;
        }
    };
    /// Exact target inventory charge, not merely source-body framing. Check
    /// before persisting a row that no destination request could ever admit.
    pub fn targetBytes(table_bytes: usize, key_bytes: usize, tuple_bytes: usize) !usize {
        if (table_bytes == 0 or key_bytes == 0 or tuple_bytes == 0) return error.InvalidPrivateIndexClaimInventory;
        var result: usize = claim_entry_overhead;
        for ([_]usize{ table_bytes, key_bytes, tuple_bytes }) |size|
            result = std.math.add(usize, result, size) catch return error.RelationalIndexKeyTooLarge;
        if (result > max_bytes) return error.RelationalIndexKeyTooLarge;
        return result;
    }
    pub const Scope = struct {
        table_name: []const u8,
        generation: integrity.Generation,
        schema_version: u32,
        tuple_fingerprint: integrity.Digest,
        nulls_not_distinct: bool,

        pub fn validate(self: Scope) !void {
            if (self.table_name.len == 0 or self.table_name.len > 1024 or !std.unicode.utf8ValidateSlice(self.table_name) or std.mem.indexOfScalar(u8, self.table_name, 0) != null or
                self.schema_version == 0 or std.mem.allEqual(u8, &self.generation, 0) or std.mem.allEqual(u8, &self.tuple_fingerprint, 0)) return error.InvalidPrivateIndexClaimInventory;
        }
        pub fn digest(self: Scope) !integrity.Digest {
            try self.validate();
            var state = std.crypto.hash.Blake3.init(.{});
            state.update("antfly/index-claim-source-scope/v1");
            state.update(&self.generation);
            state.update(&self.tuple_fingerprint);
            var numbers: [9]u8 = undefined;
            std.mem.writeInt(u32, numbers[0..4], self.schema_version, .little);
            std.mem.writeInt(u32, numbers[4..8], @intCast(self.table_name.len), .little);
            numbers[8] = @intFromBool(self.nulls_not_distinct);
            state.update(&numbers);
            state.update(self.table_name);
            var result: integrity.Digest = undefined;
            state.final(&result);
            return result;
        }
    };

    pub fn key(id: [16]u8, ordinal: u64, payload: bool) Key {
        var result: Key = undefined;
        @memcpy(result[0..batch_prefix.len], batch_prefix);
        @memcpy(result[batch_prefix.len..][0..16], &id);
        std.mem.writeInt(u64, result[batch_prefix.len + 16 ..][0..8], ordinal, .big);
        result[result.len - 1] = @intFromBool(payload);
        return result;
    }

    pub const Head = struct {
        pub const encoded_len = 296;
        binding_digest: integrity.Digest,
        scope_digest: integrity.Digest,
        emitted: u64 = 0,
        consumed: u64 = 0,
        pending_bytes: u64 = 0,
        emitted_rows: u64 = 0,
        last_sequence: u64 = 0,
        last_digest: integrity.Digest = @splat(0),
        consumed_rows: u64 = 0,
        consumed_sequence: u64 = 0,
        consumed_digest: integrity.Digest = @splat(0),
        state: State = .building,
        progress_digest: integrity.Digest = @splat(0),
        consumption_digest: integrity.Digest = @splat(0),

        pub fn pending(self: Head) u64 {
            return self.emitted - self.consumed;
        }
        fn validate(self: Head) !void {
            if (std.mem.allEqual(u8, &self.binding_digest, 0) or std.mem.allEqual(u8, &self.scope_digest, 0) or
                self.consumed > self.emitted or self.emitted - self.consumed > max_pending or
                self.pending_bytes > max_pending_bytes or (self.emitted == self.consumed) != (self.pending_bytes == 0) or
                self.emitted == 0 or self.last_sequence == 0 or self.consumed_rows > self.emitted_rows or
                self.consumed_sequence > self.last_sequence or std.mem.allEqual(u8, &self.last_digest, 0) or
                std.mem.allEqual(u8, &self.progress_digest, 0) or
                (self.consumed == 0) != std.mem.allEqual(u8, &self.consumption_digest, 0) or
                (self.consumed != 0 and (self.consumed_sequence == 0 or std.mem.allEqual(u8, &self.consumed_digest, 0))) or
                (self.consumed == 0 and (self.consumed_rows != 0 or self.consumed_sequence != 0 or !std.mem.allEqual(u8, &self.consumed_digest, 0)))) return error.InvalidPrivateIndexClaimInventory;
        }
        pub fn encode(self: Head) ![encoded_len]u8 {
            try self.validate();
            var bytes: [encoded_len]u8 = @splat(0);
            @memcpy(bytes[0..8], "AICQ\x02\x00\x00\x00");
            @memcpy(bytes[8..40], &self.binding_digest);
            @memcpy(bytes[40..72], &self.scope_digest);
            inline for (.{ "emitted", "consumed", "pending_bytes", "emitted_rows", "last_sequence" }, 0..) |field, i| std.mem.writeInt(u64, bytes[72 + i * 8 ..][0..8], @field(self, field), .little);
            @memcpy(bytes[112..144], &self.last_digest);
            std.mem.writeInt(u64, bytes[144..152], self.consumed_rows, .little);
            std.mem.writeInt(u64, bytes[152..160], self.consumed_sequence, .little);
            @memcpy(bytes[160..192], &self.consumed_digest);
            bytes[192] = @backingInt(self.state);
            @memcpy(bytes[200..232], &self.progress_digest);
            @memcpy(bytes[232..264], &self.consumption_digest);
            @memcpy(bytes[264..296], &hash(bytes[0..264]));
            return bytes;
        }
        pub fn decode(bytes: []const u8) !Head {
            if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AICQ\x02\x00\x00\x00") or
                !std.mem.allEqual(u8, bytes[193..200], 0) or !std.mem.eql(u8, bytes[264..296], &hash(bytes[0..264]))) return error.InvalidPrivateIndexClaimInventory;
            var result: Head = .{ .binding_digest = bytes[8..40].*, .scope_digest = bytes[40..72].*, .last_digest = bytes[112..144].*, .consumed_rows = std.mem.readInt(u64, bytes[144..152], .little), .consumed_sequence = std.mem.readInt(u64, bytes[152..160], .little), .consumed_digest = bytes[160..192].*, .state = std.enums.fromInt(State, bytes[192]) orelse return error.InvalidPrivateIndexClaimInventory, .progress_digest = bytes[200..232].* };
            inline for (.{ "emitted", "consumed", "pending_bytes", "emitted_rows", "last_sequence" }, 0..) |field, i| @field(result, field) = std.mem.readInt(u64, bytes[72 + i * 8 ..][0..8], .little);
            result.consumption_digest = bytes[232..264].*;
            try result.validate();
            return result;
        }
    };

    pub fn load(txn: anytype, binding: jobs.Binding) !?Head {
        const raw = (try optional(txn, head_key)) orelse {
            if (try optional(txn, consumption_key) != null) return error.InvalidPrivateIndexClaimInventory;
            return null;
        };
        const head = try Head.decode(raw);
        if (!std.mem.eql(u8, &head.binding_digest, &try bindingDigest(binding))) return error.PrivateIndexOwnerJobChanged;
        _ = try loadConsumption(txn, binding, head);
        return head;
    }

    pub fn loadConsumption(txn: anytype, binding: jobs.Binding, head: Head) !?Consumption {
        const raw = try optional(txn, consumption_key);
        if (head.consumed == 0) {
            if (raw != null) return error.InvalidPrivateIndexClaimInventory;
            return null;
        }
        const bytes = raw orelse return error.PrivateIndexRootRecoveryRequired;
        if (!std.mem.eql(u8, &hash(bytes), &head.consumption_digest)) return error.InvalidPrivateIndexClaimInventory;
        const record = try Consumption.decode(bytes, binding);
        if (record.accepted.ordinal != head.consumed or record.accepted.rows != head.consumed_rows or record.accepted.source_sequence != head.consumed_sequence or
            !std.mem.eql(u8, &record.accepted.batch_digest, &head.consumed_digest) or !std.mem.eql(u8, &record.accepted.scope_digest, &head.scope_digest)) return error.InvalidPrivateIndexClaimInventory;
        if (head.pending() == 0 and (head.consumed_rows != head.emitted_rows or head.consumed_sequence != head.last_sequence or !std.mem.eql(u8, &head.last_digest, &head.consumed_digest) or
            record.header.bytes[184] != @backingInt(head.state) or !std.mem.eql(u8, record.header.bytes[152..184], &head.progress_digest))) return error.InvalidPrivateIndexClaimInventory;
        const current = try authority.require(txn, if (binding.authority == .native) .native else .raft, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
        switch (record.position) {
            .native => |cut| if (cut.sequence > current.sequence) return error.InvalidControlReceiptPosition,
            .raft => |cut| {
                const applied = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
                if (cut.index > applied.index or cut.term > applied.term or (cut.index == applied.index and cut.term != applied.term)) return error.InvalidControlReceiptPosition;
            },
        }
        return record;
    }

    pub const Header = struct {
        bytes: [header_len]u8,
        pub fn decode(raw: []const u8, binding: jobs.Binding, ordinal: u64) !Header {
            if (raw.len != header_len or !std.mem.eql(u8, raw[0..8], "AICB\x01\x00\x00\x00") or
                !std.mem.eql(u8, raw[8..40], &try bindingDigest(binding)) or !std.mem.allEqual(u8, raw[145..152], 0) or
                !std.mem.allEqual(u8, raw[186..192], 0) or raw[185] > 1 or raw[184] > 2 or
                std.mem.readInt(u64, raw[104..112], .little) != ordinal or ordinal == 0 or
                !std.mem.eql(u8, raw[288..320], &hash(raw[0..288]))) return error.InvalidPrivateIndexClaimInventory;
            const cut_position = try @import("receipt_position.zig").Position.decode(raw[112..145]);
            if ((cut_position == .native) != (binding.authority == .native)) return error.InvalidPrivateIndexClaimInventory;
            try cut_position.requireNamespace(binding.fence.namespace);
            const name_len = std.mem.readInt(u32, raw[92..96], .little);
            const count = std.mem.readInt(u32, raw[96..100], .little);
            const size = std.mem.readInt(u32, raw[100..104], .little);
            if (name_len == 0 or name_len > 1024 or count > max_claims or size > max_payload_bytes or size < name_len + count * 8 or
                std.mem.readInt(u32, raw[88..92], .little) != binding.physical_candidate.schema_version or
                std.mem.allEqual(u8, raw[40..72], 0) or std.mem.allEqual(u8, raw[72..88], 0) or
                std.mem.allEqual(u8, raw[152..184], 0) or std.mem.allEqual(u8, raw[192..224], 0) or std.mem.allEqual(u8, raw[224..256], 0)) return error.InvalidPrivateIndexClaimInventory;
            return .{ .bytes = raw[0..header_len].* };
        }
        pub fn position(self: Header) !Native {
            const cut = try self.orderedPosition();
            if (cut != .native) return error.InvalidControlReceiptPosition;
            return cut.native;
        }
        pub fn orderedPosition(self: Header) !Position {
            return Position.decode(self.bytes[112..145]);
        }
        pub fn progressDigest(self: Header) integrity.Digest {
            return self.bytes[152..184].*;
        }
        pub fn digest(self: Header) integrity.Digest {
            return self.bytes[288..320].*;
        }
        pub fn payloadLen(self: Header) u32 {
            return std.mem.readInt(u32, self.bytes[100..104], .little);
        }
        pub fn storedBytes(self: Header) u64 {
            return @as(u64, self.payloadLen()) + header_len + 2 * @sizeOf(Key);
        }
    };

    pub const PreparedSource = struct {
        alloc: Allocator,
        scope: Scope,
        header: [header_len]u8,
        payload: []u8,

        pub fn init(alloc: Allocator, binding: jobs.Binding, source: Scope, progress_digest: integrity.Digest, state: State, rows: []const Row) !PreparedSource {
            try source.validate();
            if (rows.len > max_claims or source.schema_version != binding.physical_candidate.schema_version or std.mem.allEqual(u8, &progress_digest, 0)) return error.InvalidPrivateIndexClaimInventory;
            var size: usize = source.table_name.len;
            var previous: ?[]const u8 = null;
            for (rows) |row| {
                if (previous) |prior| if (std.mem.order(u8, prior, row.key) != .lt) return error.InvalidPrivateIndexClaimInventory;
                previous = row.key;
                if (row.key.len == 0 or row.key.len > @import("relational_index_limits.zig").max_cursor_key_bytes or row.tuple.len == 0) return error.InvalidPrivateIndexClaimInventory;
                _ = try targetBytes(source.table_name.len, row.key.len, row.tuple.len);
                const fields = std.math.add(usize, row.key.len, row.tuple.len) catch return error.TransactionTooLarge;
                const framed = std.math.add(usize, fields, 8) catch return error.TransactionTooLarge;
                size = std.math.add(usize, size, framed) catch return error.TransactionTooLarge;
                if (size > max_payload_bytes) return error.TransactionTooLarge;
            }
            const payload = try alloc.alloc(u8, size);
            errdefer alloc.free(payload);
            @memcpy(payload[0..source.table_name.len], source.table_name);
            var offset = source.table_name.len;
            for (rows) |row| {
                std.mem.writeInt(u32, payload[offset..][0..4], @intCast(row.key.len), .little);
                std.mem.writeInt(u32, payload[offset + 4 ..][0..4], @intCast(row.tuple.len), .little);
                offset += 8;
                @memcpy(payload[offset..][0..row.key.len], row.key);
                offset += row.key.len;
                @memcpy(payload[offset..][0..row.tuple.len], row.tuple);
                offset += row.tuple.len;
            }
            var header: [header_len]u8 = @splat(0);
            @memcpy(header[0..8], "AICB\x01\x00\x00\x00");
            @memcpy(header[8..40], &try bindingDigest(binding));
            @memcpy(header[40..72], &try source.digest());
            @memcpy(header[72..88], &source.generation);
            std.mem.writeInt(u32, header[88..92], source.schema_version, .little);
            std.mem.writeInt(u32, header[92..96], @intCast(source.table_name.len), .little);
            std.mem.writeInt(u32, header[96..100], @intCast(rows.len), .little);
            std.mem.writeInt(u32, header[100..104], @intCast(size), .little);
            @memcpy(header[152..184], &progress_digest);
            header[184] = @backingInt(state);
            header[185] = @intFromBool(source.nulls_not_distinct);
            @memcpy(header[192..224], &source.tuple_fingerprint);
            @memcpy(header[224..256], &hash(payload));
            return .{ .alloc = alloc, .scope = source, .header = header, .payload = payload };
        }
        pub fn deinit(self: *PreparedSource) void {
            self.alloc.free(self.payload);
            self.* = undefined;
        }
        /// Stage with the physical page, progress, native cut and mirror outbox.
        /// Capacity rejection aborts that whole transaction, never just claims.
        pub fn stage(self: *PreparedSource, txn: anytype, binding: jobs.Binding, position: Native) !void {
            return self.stageOrdered(txn, binding, .{ .native = position });
        }
        /// The caller commits this batch with its physical page and actual
        /// authority watermark. Raft indexes remain Raft positions on disk.
        pub fn stageOrdered(self: *PreparedSource, txn: anytype, binding: jobs.Binding, position: Position) !void {
            const sequence = try orderedSequence(binding, position);
            const allocation = try requireRegistered(txn, binding);
            if (!std.mem.eql(u8, &allocation.generation, &self.scope.generation)) return error.InvalidPrivateIndexClaimInventory;
            var head = (try load(txn, binding)) orelse Head{ .binding_digest = try bindingDigest(binding), .scope_digest = try self.scope.digest() };
            if (!std.mem.eql(u8, &head.scope_digest, &try self.scope.digest()) or head.state != .building or sequence <= head.last_sequence) return error.PrivateIndexOwnerJobChanged;
            if (head.emitted != 0 and binding.authority == .raft) {
                const previous = try latestHeader(txn, binding, head);
                if ((try previous.orderedPosition()).raft.term > position.raft.term) return error.InvalidControlReceiptPosition;
            }
            const bytes: u64 = self.payload.len + header_len + 2 * @sizeOf(Key);
            if (head.emitted - head.consumed >= max_pending or head.pending_bytes + bytes > max_pending_bytes) return error.PrivateIndexClaimBacklogFull;
            head.emitted = try std.math.add(u64, head.emitted, 1);
            std.mem.writeInt(u64, self.header[104..112], head.emitted, .little);
            @memcpy(self.header[112..145], &try position.encode());
            @memcpy(self.header[256..288], &head.last_digest);
            @memcpy(self.header[288..320], &hash(self.header[0..288]));
            const header = try Header.decode(&self.header, binding, head.emitted);
            head.pending_bytes += bytes;
            head.emitted_rows = try std.math.add(u64, head.emitted_rows, std.mem.readInt(u32, self.header[96..100], .little));
            head.last_sequence = sequence;
            head.last_digest = header.digest();
            head.progress_digest = self.header[152..184].*;
            head.state = @fromBackingInt(@intCast(self.header[184]));
            try txn.put(&key(binding.id, head.emitted, false), &self.header);
            try txn.put(&key(binding.id, head.emitted, true), self.payload);
            try txn.put(head_key, &try head.encode());
        }
    };

    pub fn requireLatest(txn: anytype, binding: jobs.Binding, source: Scope, position: Native, progress_digest: integrity.Digest, batch_digest: integrity.Digest) !Head {
        return requireLatestOrdered(txn, binding, source, .{ .native = position }, progress_digest, batch_digest);
    }

    fn orderedSequence(binding: jobs.Binding, position: Position) !u64 {
        try position.requireNamespace(binding.fence.namespace);
        if ((position == .native) != (binding.authority == .native)) return error.InvalidControlReceiptPosition;
        return switch (position) {
            .native => |value| value.sequence,
            .raft => |value| value.index,
        };
    }

    fn latestHeader(txn: anytype, binding: jobs.Binding, head: Head) !Header {
        if (head.pending() == 0) return ((try loadConsumption(txn, binding, head)) orelse return error.InvalidPrivateIndexClaimInventory).header;
        return Header.decode((try optional(txn, &key(binding.id, head.emitted, false))) orelse return error.InvalidPrivateIndexClaimInventory, binding, head.emitted);
    }

    pub fn requireLatestOrdered(txn: anytype, binding: jobs.Binding, source: Scope, position: Position, progress_digest: integrity.Digest, batch_digest: integrity.Digest) !Head {
        const sequence = try orderedSequence(binding, position);
        const head = (try load(txn, binding)) orelse return error.InvalidPrivateIndexClaimInventory;
        if (!std.mem.eql(u8, &head.last_digest, &batch_digest) or !std.mem.eql(u8, &head.scope_digest, &try source.digest()) or head.last_sequence != sequence or
            !std.mem.eql(u8, &head.progress_digest, &progress_digest)) return error.InvalidPrivateIndexClaimInventory;
        {
            const header = try latestHeader(txn, binding, head);
            if (!std.meta.eql(try header.orderedPosition(), position) or
                !std.mem.eql(u8, header.bytes[40..72], &head.scope_digest) or
                !std.mem.eql(u8, header.bytes[152..184], &head.progress_digest) or header.bytes[184] != @backingInt(head.state) or
                !std.mem.eql(u8, &header.digest(), &head.last_digest) or !std.mem.eql(u8, header.bytes[72..88], &source.generation) or
                !std.mem.eql(u8, header.bytes[192..224], &source.tuple_fingerprint) or header.bytes[185] != @intFromBool(source.nulls_not_distinct)) return error.InvalidPrivateIndexClaimInventory;
        }
        return head;
    }

    /// Metadata-only bounded recovery; payload integrity is checked on peek.
    /// Pending debt prevents publication even if a payload needs root recovery.
    pub fn verifyRecovery(txn: anytype, binding: jobs.Binding, expected: ?Native, progress_digest: ?integrity.Digest, batch_digest: ?integrity.Digest) !void {
        return verifyRecoveryOrdered(txn, binding, if (expected) |value| .{ .native = value } else null, progress_digest, batch_digest);
    }

    pub fn verifyRecoveryOrdered(txn: anytype, binding: jobs.Binding, expected: ?Position, progress_digest: ?integrity.Digest, batch_digest: ?integrity.Digest) !void {
        const head = (try load(txn, binding)) orelse {
            if (expected != null) return error.InvalidPrivateIndexClaimInventory;
            return;
        };
        const cut = expected orelse return error.InvalidPrivateIndexClaimInventory;
        const allocation = try requireRegistered(txn, binding);
        if (try orderedSequence(binding, cut) != head.last_sequence or progress_digest == null or batch_digest == null or
            !std.mem.eql(u8, &head.last_digest, &batch_digest.?) or !std.mem.eql(u8, &head.progress_digest, &progress_digest.?)) return error.InvalidPrivateIndexClaimInventory;
        if (head.pending() == 0 and !std.meta.eql(try (try latestHeader(txn, binding, head)).orderedPosition(), cut)) return error.InvalidPrivateIndexClaimInventory;
        var previous = head.consumed_digest;
        var previous_sequence = head.consumed_sequence;
        // A later consumption may retire an earlier batch while other pages
        // are already pending. Compare producer cuts, not consumption time.
        var previous_term: u64 = if (try loadConsumption(txn, binding, head)) |record| (try record.header.orderedPosition()).term() else 0;
        var bytes: u64 = 0;
        var rows: u64 = 0;
        for (0..@intCast(head.pending())) |i| {
            const ordinal = head.consumed + i + 1;
            const raw = (try optional(txn, &key(binding.id, ordinal, false))) orelse return error.InvalidPrivateIndexClaimInventory;
            const header = try Header.decode(raw, binding, ordinal);
            const position = try header.orderedPosition();
            const sequence = try orderedSequence(binding, position);
            if (!std.mem.eql(u8, header.bytes[40..72], &head.scope_digest) or
                !std.mem.eql(u8, header.bytes[72..88], &allocation.generation) or
                !std.mem.eql(u8, header.bytes[256..288], &previous) or sequence <= previous_sequence or position.term() < previous_term or
                (ordinal != head.emitted and header.bytes[184] != @backingInt(State.building)) or
                (ordinal == head.emitted and (!std.meta.eql(position, cut) or
                    !std.mem.eql(u8, header.bytes[152..184], &head.progress_digest) or header.bytes[184] != @backingInt(head.state)))) return error.InvalidPrivateIndexClaimInventory;
            previous = header.digest();
            previous_sequence = sequence;
            previous_term = position.term();
            bytes += header.storedBytes();
            rows += std.mem.readInt(u32, header.bytes[96..100], .little);
        }
        if (bytes != head.pending_bytes or rows != head.emitted_rows - head.consumed_rows or previous_sequence != head.last_sequence or !std.mem.eql(u8, &previous, &head.last_digest)) return error.InvalidPrivateIndexClaimInventory;
    }

    pub const OwnedBatch = struct {
        alloc: Allocator,
        payload: []u8,
        header: Header,
        scope: Scope,
        entries: [max_claims]Row,
        count: usize,
        pub fn rows(self: *const OwnedBatch) []const Row {
            return self.entries[0..self.count];
        }
        pub fn deinit(self: *OwnedBatch) void {
            self.alloc.free(self.payload);
            self.* = undefined;
        }

        /// Transfer one owned canonical body on success only. Reused by the
        /// storage snapshot and compiled-owner wire decoder; no row JSON parse.
        pub fn takePayload(alloc: Allocator, binding: jobs.Binding, header: Header, payload: []u8) !OwnedBatch {
            _ = try Header.decode(&header.bytes, binding, std.mem.readInt(u64, header.bytes[104..112], .little));
            if (payload.len != header.payloadLen() or !std.mem.eql(u8, header.bytes[224..256], &hash(payload))) return error.InvalidPrivateIndexClaimInventory;
            const name_len = std.mem.readInt(u32, header.bytes[92..96], .little);
            const scope_value: Scope = .{ .table_name = payload[0..name_len], .generation = header.bytes[72..88].*, .schema_version = std.mem.readInt(u32, header.bytes[88..92], .little), .tuple_fingerprint = header.bytes[192..224].*, .nulls_not_distinct = header.bytes[185] != 0 };
            if (!std.mem.eql(u8, &try scope_value.digest(), header.bytes[40..72])) return error.InvalidPrivateIndexClaimInventory;
            var result: OwnedBatch = .{ .alloc = alloc, .payload = payload, .header = header, .scope = scope_value, .entries = undefined, .count = std.mem.readInt(u32, header.bytes[96..100], .little) };
            var offset: usize = name_len;
            var previous: ?[]const u8 = null;
            for (result.entries[0..result.count]) |*entry| {
                if (payload.len - offset < 8) return error.InvalidPrivateIndexClaimInventory;
                const row_key_len = std.mem.readInt(u32, payload[offset..][0..4], .little);
                const tuple_len = std.mem.readInt(u32, payload[offset + 4 ..][0..4], .little);
                offset += 8;
                if (row_key_len == 0 or row_key_len > @import("relational_index_limits.zig").max_cursor_key_bytes or row_key_len > payload.len - offset) return error.InvalidPrivateIndexClaimInventory;
                entry.key = payload[offset..][0..row_key_len];
                offset += row_key_len;
                if (tuple_len == 0 or tuple_len > payload.len - offset) return error.InvalidPrivateIndexClaimInventory;
                entry.tuple = payload[offset..][0..tuple_len];
                offset += tuple_len;
                _ = try targetBytes(name_len, row_key_len, tuple_len);
                if (previous) |prior| if (std.mem.order(u8, prior, entry.key) != .lt) return error.InvalidPrivateIndexClaimInventory;
                previous = entry.key;
            }
            if (offset != payload.len) return error.InvalidPrivateIndexClaimInventory;
            return result;
        }
    };

    /// Read one immutable bounded batch. A mirror ACK cannot retire it; a
    /// separate, authenticated complete delivery closure must do that later.
    pub fn peek(alloc: Allocator, txn: anytype, binding: jobs.Binding) !?OwnedBatch {
        try jobs.requireReserved(txn, binding);
        const head = (try load(txn, binding)) orelse return null;
        if (head.pending() == 0) return null;
        const ordinal = head.consumed + 1;
        const raw_header = (try optional(txn, &key(binding.id, ordinal, false))) orelse return error.PrivateIndexRootRecoveryRequired;
        const header = try Header.decode(raw_header, binding, ordinal);
        const allocation = try requireRegistered(txn, binding);
        if (!std.mem.eql(u8, header.bytes[40..72], &head.scope_digest) or !std.mem.eql(u8, header.bytes[72..88], &allocation.generation)) return error.InvalidPrivateIndexClaimInventory;
        const raw = (try optional(txn, &key(binding.id, ordinal, true))) orelse return error.PrivateIndexRootRecoveryRequired;
        if (raw.len != header.payloadLen()) return error.InvalidPrivateIndexClaimInventory;
        const payload = try alloc.dupe(u8, raw);
        errdefer alloc.free(payload);
        return try OwnedBatch.takePayload(alloc, binding, header, payload);
    }

    pub const ConsumeResult = struct { record: Consumption, duplicate: bool };
    pub const PreparedConsumption = struct {
        binding: jobs.Binding,
        accepted: Accepted,
        batch: ?OwnedBatch,
        pub fn init(alloc: Allocator, txn: anytype, binding: jobs.Binding, accepted: Accepted) !PreparedConsumption {
            try jobs.requireReserved(txn, binding);
            try accepted.validate();
            const head = (try load(txn, binding)) orelse return error.PrivateIndexRootRecoveryRequired;
            const previous = try loadConsumption(txn, binding, head);
            if (head.consumed == accepted.ordinal) {
                const retained = previous orelse return error.InvalidPrivateIndexClaimInventory;
                if (!std.mem.eql(u8, &retained.accepted.decision_digest, &accepted.decision_digest)) return error.InvalidPrivateIndexClaimReceipt;
                return .{ .binding = binding, .accepted = accepted, .batch = null };
            }
            if (head.consumed == std.math.maxInt(u64) or accepted.ordinal != head.consumed + 1) return error.InvalidPrivateIndexClaimReceipt;
            const batch = (try peek(alloc, txn, binding)) orelse return error.PrivateIndexRootRecoveryRequired;
            errdefer {
                var owned = batch;
                owned.deinit();
            }
            try accepted.requireFor(binding, batch.header);
            try requirePrefix(accepted, head, previous);
            return .{ .binding = binding, .accepted = accepted, .batch = batch };
        }
        pub fn deinit(self: *PreparedConsumption) void {
            if (self.batch) |*batch| batch.deinit();
            self.* = undefined;
        }
        fn requirePrefix(accepted: Accepted, head: Head, previous: ?Consumption) !void {
            if ((std.math.add(u64, head.consumed_rows, accepted.batch_rows) catch return error.InvalidPrivateIndexClaimReceipt) != accepted.rows) return error.InvalidPrivateIndexClaimReceipt;
            const expected = if (previous) |record| record.accepted.decision_digest else @as(integrity.Digest, @splat(0));
            if (!std.mem.eql(u8, &accepted.previous_decision, &expected)) return error.InvalidPrivateIndexClaimReceipt;
            if (previous) |record| if (!std.mem.eql(u8, &record.accepted.metadata_incarnation, &accepted.metadata_incarnation)) return error.InvalidPrivateIndexClaimReceipt;
        }
        /// All validation/encoding precedes the first write. The caller must
        /// abort on any stage/outbox error and commit this whole effect once.
        pub fn stage(self: *const PreparedConsumption, txn: anytype, replay: ?Native) !ConsumeResult {
            if (self.binding.authority != .native) return error.InvalidControlReceiptPosition;
            return self.stageOrdered(txn, if (replay) |cut| .{ .native = cut } else null);
        }
        pub fn stageOrdered(self: *const PreparedConsumption, txn: anytype, replay: ?Position) !ConsumeResult {
            if (self.binding.authority == .raft and replay == null) return error.InvalidControlReceiptPosition;
            try jobs.requireReserved(txn, self.binding);
            var head = (try load(txn, self.binding)) orelse return error.PrivateIndexRootRecoveryRequired;
            const previous = try loadConsumption(txn, self.binding, head);
            if (head.consumed == self.accepted.ordinal) {
                const retained = previous orelse return error.InvalidPrivateIndexClaimInventory;
                if (!std.mem.eql(u8, &retained.accepted.decision_digest, &self.accepted.decision_digest)) return error.InvalidPrivateIndexClaimReceipt;
                if (replay) |cut| {
                    _ = try orderedSequence(self.binding, cut);
                    if (self.binding.authority == .native and !std.meta.eql(cut, retained.position)) return error.InvalidControlReceiptPosition;
                    if (cut == .raft and (cut.raft.index < retained.position.raft.index or cut.raft.term < retained.position.raft.term or
                        (cut.raft.index == retained.position.raft.index and cut.raft.term != retained.position.raft.term))) return error.InvalidControlReceiptPosition;
                }
                return .{ .record = retained, .duplicate = true };
            }
            if (head.consumed == std.math.maxInt(u64) or self.accepted.ordinal != head.consumed + 1) return error.InvalidPrivateIndexClaimReceipt;
            if (self.binding.authority == .raft) {
                const cut = replay.?;
                _ = try orderedSequence(self.binding, cut);
                const prior = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
                if (cut.raft.index <= prior.index or cut.raft.term < prior.term) return error.InvalidControlReceiptPosition;
            }
            const batch = if (self.batch) |*value| value else return error.PrivateIndexRootRecoveryRequired;
            try self.accepted.requireFor(self.binding, batch.header);
            try requirePrefix(self.accepted, head, previous);
            const raw_header = try txn.get(&key(self.binding.id, self.accepted.ordinal, false));
            if (!std.mem.eql(u8, raw_header, &batch.header.bytes)) return error.PrivateIndexOwnerJobChanged;
            const raw_body = try txn.get(&key(self.binding.id, self.accepted.ordinal, true));
            if (raw_body.len != batch.header.payloadLen() or !std.mem.eql(u8, &hash(raw_body), batch.header.bytes[224..256])) return error.InvalidPrivateIndexClaimInventory;
            const ns = @import("online_source_contract.zig").namespaceBytes(self.binding.fence.namespace);
            const current = try authority.require(txn, if (self.binding.authority == .native) .native else .raft, ns);
            const position: Position = if (self.binding.authority == .raft) replay orelse return error.InvalidControlReceiptPosition else .{ .native = .{ .namespace = self.binding.fence.namespace, .sequence = std.math.add(u64, current.sequence, 1) catch return error.OnlineSourceCorrupt } };
            _ = try orderedSequence(self.binding, position);
            if (replay) |cut| if (!std.meta.eql(cut, position)) return error.InvalidControlReceiptPosition;
            const record: Consumption = .{ .accepted = self.accepted, .header = batch.header, .position = position, .previous_digest = head.consumption_digest };
            const record_bytes = try record.encode(self.binding);
            const charge = batch.header.storedBytes();
            if (head.pending_bytes < charge) return error.InvalidPrivateIndexClaimInventory;
            head.consumed = self.accepted.ordinal;
            head.consumed_rows = self.accepted.rows;
            head.consumed_sequence = self.accepted.source_sequence;
            head.consumed_digest = self.accepted.batch_digest;
            head.consumption_digest = hash(&record_bytes);
            head.pending_bytes -= charge;
            const head_bytes = try head.encode();
            try txn.delete(&key(self.binding.id, self.accepted.ordinal, false));
            try txn.delete(&key(self.binding.id, self.accepted.ordinal, true));
            try txn.put(consumption_key, &record_bytes);
            try txn.put(head_key, &head_bytes);
            if (position == .native) _ = try authority.advance(txn, ns, position.native.sequence);
            return .{ .record = record, .duplicate = false };
        }
    };

    /// Cancellation owns these exact bounded ordinal keys. Do not fetch large
    /// payloads, decode claims or scan the table to retire unpublished work.
    pub fn stageCanceled(txn: anytype, binding: jobs.Binding) !void {
        const head = (try load(txn, binding)) orelse return;
        for (0..@intCast(head.pending())) |i| {
            const ordinal = head.consumed + i + 1;
            try txn.delete(&key(binding.id, ordinal, false));
            try txn.delete(&key(binding.id, ordinal, true));
        }
        try txn.delete(head_key);
        try txn.delete(consumption_key);
    }

    /// stageCanceled retires the bounded pending window. A completion/release
    /// additionally proves absence of orphan companions with one prefix seek;
    /// it must never silently abandon source history after retiring the job.
    pub fn requireCanceledRetired(txn: anytype, binding: jobs.Binding) !void {
        if (try optional(txn, head_key) != null or try optional(txn, consumption_key) != null) return error.PrivateIndexCancellationPending;
        const start = key(binding.id, 0, false);
        const job_prefix = start[0 .. batch_prefix.len + 16];
        var cursor = try txn.openCursor();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(job_prefix)) |entry| if (std.mem.startsWith(u8, entry.key, job_prefix)) return error.PrivateIndexCancellationPending;
    }
};

pub const max_bytes = 1024 * 1024;
const key_len = prefix.len + 16 + 32 + 32;
// AFC1 fixed fields/checksum, both paired keys, and the provenance value.
// Count the actual bytes GC must carry, not just request string lengths.
pub const claim_entry_overhead = 76 + key_len + integrity.key_len + max_inventory_value_len;

fn hash(bytes: []const u8) integrity.Digest {
    return integrity.hash(bytes);
}

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

fn scope(txn: anytype, binding: jobs.Binding) !catalog.PrivateIndexAllocation {
    return catalog.privateIndexAllocationFromReservation(try txn.get(catalog.key), binding.integrity_reserved, binding.fence.namespace.table_id);
}

pub fn registrationKey(generation: integrity.Generation) [registration_prefix.len + 16]u8 {
    var key: [registration_prefix.len + 16]u8 = undefined;
    @memcpy(key[0..registration_prefix.len], registration_prefix);
    @memcpy(key[registration_prefix.len..], &generation);
    return key;
}

fn registration(binding: jobs.Binding, generation: integrity.Generation) !integrity.Digest {
    var current = binding;
    current.phase = .reserved;
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly private UNIQUE claim registration v1");
    state.update(&try current.encode());
    state.update(&generation);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

fn generationPrefix(generation: integrity.Generation) [prefix.len + 16]u8 {
    var key: [prefix.len + 16]u8 = undefined;
    @memcpy(key[0..prefix.len], prefix);
    @memcpy(key[prefix.len..], &generation);
    return key;
}

pub fn inventoryKey(address: integrity.Address) [key_len]u8 {
    var key: [key_len]u8 = undefined;
    @memcpy(key[0 .. prefix.len + 16], &generationPrefix(address.generation));
    @memcpy(key[prefix.len + 16 ..][0..32], &address.routing);
    @memcpy(key[prefix.len + 48 ..], &address.tuple_digest);
    return key;
}

fn addressFromInventory(key: []const u8) !integrity.Address {
    if (key.len != key_len or !std.mem.startsWith(u8, key, prefix)) return error.InvalidPrivateIndexClaimInventory;
    const address: integrity.Address = .{
        .generation = key[prefix.len..][0..16].*,
        .routing = key[prefix.len + 16 ..][0..32].*,
        .tuple_digest = key[prefix.len + 48 ..][0..32].*,
    };
    _ = integrity.parseKey(&address.claimKey()) catch return error.InvalidPrivateIndexClaimInventory;
    return address;
}

/// Reservation creates the empty inventory before private claim writes become
/// admissible. This is staged with both allocation burns, not lazily inferred
/// from the absence of inventory entries after a restart.
pub fn stageRegistration(txn: anytype, binding: jobs.Binding) !void {
    const allocation = try scope(txn, binding);
    const key = registrationKey(allocation.generation);
    if (try optional(txn, &key) != null) return error.InvalidPrivateIndexClaimInventory;
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        const start = generationPrefix(allocation.generation);
        if (try cursor.seekAtOrAfter(&start)) |entry| if (std.mem.startsWith(u8, entry.key, &start)) return error.InvalidPrivateIndexClaimInventory;
    }
    try txn.put(&key, &try registration(binding, allocation.generation));
}

pub fn requireRegistered(txn: anytype, binding: jobs.Binding) !catalog.PrivateIndexAllocation {
    const allocation = try scope(txn, binding);
    const expected = try registration(binding, allocation.generation);
    const actual = (try optional(txn, &registrationKey(allocation.generation))) orelse return error.InvalidPrivateIndexClaimInventory;
    if (!std.mem.eql(u8, actual, &expected)) return error.InvalidPrivateIndexClaimInventory;
    return allocation;
}

const Entry = struct {
    address: integrity.Address,
    value: []const u8,
    expected: ?[]const u8,
    sequence: u64 = 0,
    position: ?Position = null,
};

fn bindingDigest(binding: jobs.Binding) !integrity.Digest {
    var normalized = binding;
    normalized.phase = .reserved;
    return hash(&try normalized.encode());
}

// One immutable cut plus a binding/claim/key/cut digest. The shared generation
// registration already owns binding identity; duplicating that identity and a
// claim hash as plaintext in every row adds bytes without additional integrity.
pub const inventory_value_len = 40;
pub const max_inventory_value_len = 48;

fn positionNumber(position: Position) u64 {
    return switch (position) {
        .native => |cut| cut.sequence,
        .raft => |cut| cut.index,
    };
}

fn inventoryChecksum(key: []const u8, binding_digest: integrity.Digest, value_digest: integrity.Digest, cut: []const u8) integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly private claim provenance v1");
    state.update(key);
    state.update(&binding_digest);
    state.update(&value_digest);
    state.update(cut);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

fn encodeInventory(key: []const u8, binding_digest: integrity.Digest, value_digest: integrity.Digest, position: Position, buffer: *[max_inventory_value_len]u8) ![]const u8 {
    try position.validate();
    const cut_len: usize = if (position == .native) 8 else 16;
    if (position == .native) {
        std.mem.writeInt(u64, buffer[0..8], position.native.sequence, .little);
    } else {
        std.mem.writeInt(u64, buffer[0..8], position.raft.term, .little);
        std.mem.writeInt(u64, buffer[8..16], position.raft.index, .little);
    }
    @memcpy(buffer[cut_len..][0..32], &inventoryChecksum(key, binding_digest, value_digest, buffer[0..cut_len]));
    return buffer[0 .. cut_len + 32];
}

fn decodeInventory(key: []const u8, value: []const u8, provenance: Provenance, value_digest: integrity.Digest) !u64 {
    return positionNumber(try decodeInventoryPosition(key, value, provenance, value_digest));
}

fn decodeInventoryPosition(key: []const u8, value: []const u8, provenance: Provenance, value_digest: integrity.Digest) !Position {
    _ = try addressFromInventory(key);
    const cut_len: usize = if (provenance.reserved_cut == .native) 8 else 16;
    if (value.len != cut_len + 32 or
        !std.mem.eql(u8, value[cut_len..], &inventoryChecksum(key, provenance.binding_digest, value_digest, value[0..cut_len]))) return error.InvalidPrivateIndexClaimInventory;
    const position: Position = if (provenance.reserved_cut == .native)
        .{ .native = .{ .namespace = provenance.reserved_cut.native.namespace, .sequence = std.mem.readInt(u64, value[0..8], .little) } }
    else
        .{ .raft = .{ .term = std.mem.readInt(u64, value[0..8], .little), .index = std.mem.readInt(u64, value[8..16], .little) } };
    const sequence = positionNumber(position);
    if (sequence <= provenance.reserved or sequence > provenance.maximum) return error.InvalidPrivateIndexClaimInventory;
    if (position.term() < provenance.reserved_cut.term() or position.term() > provenance.maximum_cut.term() or
        (sequence == provenance.maximum and position.term() != provenance.maximum_cut.term())) return error.InvalidPrivateIndexClaimInventory;
    return position;
}

test "relational index system compact claim cuts preserve authority and committed bounds" {
    const binding_digest: integrity.Digest = @splat(2);
    const value_digest: integrity.Digest = @splat(3);
    const address = try integrity.Address.init(@splat(1), "opaque-tuple");
    const key = inventoryKey(address);
    const ns: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 };
    const native_provenance: Provenance = .{ .binding_digest = binding_digest, .reserved = 3, .maximum = 10, .reserved_cut = .{ .native = .{ .namespace = ns, .sequence = 3 } }, .maximum_cut = .{ .native = .{ .namespace = ns, .sequence = 10 } } };
    const raft_provenance: Provenance = .{ .binding_digest = binding_digest, .reserved = 30, .maximum = 80, .reserved_cut = .{ .raft = .{ .term = 2, .index = 30 } }, .maximum_cut = .{ .raft = .{ .term = 4, .index = 80 } } };
    var storage: [max_inventory_value_len]u8 = undefined;
    const native: Position = .{ .native = .{ .namespace = ns, .sequence = 7 } };
    const native_bytes = try encodeInventory(&key, binding_digest, value_digest, native, &storage);
    try std.testing.expectEqual(inventory_value_len, native_bytes.len);
    try std.testing.expectEqualDeep(native, try decodeInventoryPosition(&key, native_bytes, native_provenance, value_digest));
    try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, decodeInventoryPosition(&key, native_bytes, raft_provenance, value_digest));
    const raft: Position = .{ .raft = .{ .term = 3, .index = 50 } };
    const raft_bytes = try encodeInventory(&key, binding_digest, value_digest, raft, &storage);
    try std.testing.expectEqual(max_inventory_value_len, raft_bytes.len);
    try std.testing.expectEqualDeep(raft, try decodeInventoryPosition(&key, raft_bytes, raft_provenance, value_digest));
    try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, decodeInventoryPosition(&key, raft_bytes, native_provenance, value_digest));
    storage[storage.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, decodeInventoryPosition(&key, &storage, raft_provenance, value_digest));
    for ([_]Position{
        .{ .raft = .{ .term = 4, .index = 81 } },
        .{ .raft = .{ .term = 1, .index = 50 } },
        .{ .raft = .{ .term = 3, .index = 80 } },
        .{ .raft = .{ .term = 2, .index = 30 } },
    }) |uncommitted| {
        const bytes = try encodeInventory(&key, binding_digest, value_digest, uncommitted, &storage);
        try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, decodeInventoryPosition(&key, bytes, raft_provenance, value_digest));
    }
}

const Provenance = struct {
    binding_digest: integrity.Digest,
    reserved: u64,
    maximum: u64,
    reserved_cut: Position,
    maximum_cut: Position,

    fn init(txn: anytype, binding: jobs.Binding) !Provenance {
        const namespace = @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace);
        if (binding.authority == .raft) {
            _ = try authority.require(txn, .raft, namespace);
            const reserved = (try @import("relational_index_raft_control.zig").receipt(txn, binding, .reserved)) orelse return error.InvalidControlReceiptPosition;
            const applied = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
            return .{ .binding_digest = try bindingDigest(binding), .reserved = reserved.position.raft.index, .maximum = applied.index, .reserved_cut = reserved.position, .maximum_cut = .{ .raft = .{ .term = applied.term, .index = applied.index } } };
        }
        const current = try authority.require(txn, .native, namespace);
        const reserved = (try jobs.nativeEffectPosition(txn, binding, .reserved)) orelse return error.InvalidPrivateIndexNativeReceipt;
        var maximum = current.sequence;
        const cancellation = @import("relational_index_cancellation_intent.zig");
        if (try cancellation.load(txn)) |canceled| {
            if (canceled.intent.fence.admission_epoch < binding.fence.admission_epoch) {
                try cancellation.requireOpen(txn, binding);
            } else {
                try cancellation.requireCanceled(txn, binding);
                maximum = @min(maximum, canceled.position.sequence -| 1);
            }
        }
        if (maximum < reserved.sequence) return error.InvalidPrivateIndexClaimInventory;
        return .{ .binding_digest = try bindingDigest(binding), .reserved = reserved.sequence, .maximum = maximum, .reserved_cut = .{ .native = reserved }, .maximum_cut = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = maximum } } };
    }
};

pub const OrderedOutcome = struct { position: Native, duplicate: bool, claims_added: usize };

/// Own all tuple/claim bytes before serialized apply. A prepared batch does not
/// retain a store snapshot or borrow request memory. Cancellation is checked
/// again in stage, including batches prepared before the intent committed.
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    binding: jobs.Binding,
    entries: []const Entry,

    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn init(alloc: Allocator, txn: anytype, binding: jobs.Binding, claims: []const integrity.Claim) !Prepared {
        if (claims.len == 0 or claims.len > max_claims) return error.InvalidPrivateIndexClaimInventory;
        var bytes: usize = 0;
        for (claims) |claim| {
            if (claim.state != .live or claim.schema_version != binding.physical_candidate.schema_version) return error.InvalidPrivateIndexClaimInventory;
            bytes = std.math.add(usize, bytes, claim_entry_overhead) catch return error.TransactionTooLarge;
            for ([_][]const u8{ claim.tuple, claim.parent_table, claim.parent_key }) |field| bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
        }
        if (bytes > max_bytes) return error.TransactionTooLarge;
        try jobs.requireReserved(txn, binding);
        const allocation = try requireRegistered(txn, binding);
        const provenance = try Provenance.init(txn, binding);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var entries: std.ArrayList(Entry) = .empty;
        var seen: std.AutoHashMapUnmanaged(integrity.Digest, usize) = .empty;
        defer seen.deinit(alloc);
        for (claims) |claim| {
            const address = try integrity.Address.init(allocation.generation, claim.tuple);
            const value = try claim.encode(owned, address);
            const slot = try seen.getOrPut(alloc, address.routing);
            if (slot.found_existing) {
                if (!std.mem.eql(u8, entries.items[slot.value_ptr.*].value, value)) return error.UniqueConstraintViolation;
                continue;
            }
            slot.value_ptr.* = entries.items.len;
            // Validate before owning any store bytes. An exact duplicate can
            // retain the already-owned encoding, avoiding a second allocation
            // and rejecting oversized corrupt records before cloning them.
            const expected = if (try optional(txn, &address.claimKey())) |raw| blk: {
                _ = try integrity.Claim.decode(&address.claimKey(), raw);
                if (!std.mem.eql(u8, raw, value)) return error.UniqueConstraintViolation;
                break :blk value;
            } else null;
            const position = try checkInventoryPosition(txn, address, expected, provenance);
            try entries.append(owned, .{ .address = address, .value = value, .expected = expected, .sequence = if (position) |cut| positionNumber(cut) else 0, .position = position });
        }
        // Stable transport order, independent of request order/coalescing.
        std.mem.sort(Entry, entries.items, {}, struct {
            fn less(_: void, left: Entry, right: Entry) bool {
                return std.mem.order(u8, &left.address.claimKey(), &right.address.claimKey()) == .lt;
            }
        }.less);
        return .{ .arena = arena, .binding = binding, .entries = entries.items };
    }

    /// Caller commits claim pairs, immutable creation cuts, transport marker
    /// and outbox together. The batch cut is the maximum creation cut of its
    /// members: new members receive this effect's cut, existing members never
    /// change. Retries and regrouping therefore add no metadata and recover an
    /// exact cut without a per-batch ledger or a global-clock approximation.
    pub fn stageOrdered(self: *const Prepared, txn: anytype, replay: ?Native) !OrderedOutcome {
        if (self.binding.authority != .native) return error.InvalidControlReceiptPosition;
        const result = try self.stageAt(txn, if (replay) |cut| .{ .native = cut } else null);
        return .{ .position = result.position.native, .duplicate = result.duplicate, .claims_added = result.claims_added };
    }

    pub fn stageAt(self: *const Prepared, txn: anytype, replay: ?Position) !struct { position: Position, duplicate: bool, claims_added: usize } {
        if (self.binding.authority == .raft and replay == null) return error.InvalidControlReceiptPosition;
        try jobs.requireReserved(txn, self.binding);
        const allocation = try requireRegistered(txn, self.binding);
        const namespace = @import("online_source_contract.zig").namespaceBytes(self.binding.fence.namespace);
        const current = try authority.require(txn, if (self.binding.authority == .native) .native else .raft, namespace);
        const provenance = try Provenance.init(txn, self.binding);
        if (replay) |position| {
            try position.validate();
            try position.requireNamespace(self.binding.fence.namespace);
            if ((position == .native) != (self.binding.authority == .native)) return error.InvalidControlReceiptPosition;
        }
        var changed: usize = 0;
        var maximum: ?Position = null;
        for (self.entries) |entry| {
            if (!std.mem.eql(u8, &entry.address.generation, &allocation.generation)) return error.InvalidPrivateIndexClaimInventory;
            const actual = try optional(txn, &entry.address.claimKey());
            if (actual) |value| {
                if (!std.mem.eql(u8, value, entry.value)) return error.UniqueConstraintViolation;
                const position = (try checkInventoryPosition(txn, entry.address, entry.value, provenance)).?;
                if (entry.expected != null and !std.meta.eql(position, entry.position.?)) return error.VersionConflict;
                if (maximum) |cut| {
                    if (positionNumber(position) == positionNumber(cut) and !std.meta.eql(position, cut)) return error.InvalidPrivateIndexClaimInventory;
                }
                if (maximum == null or positionNumber(position) > positionNumber(maximum.?)) maximum = position;
            } else {
                if (entry.expected != null) return error.VersionConflict;
                _ = try checkInventory(txn, entry.address, null, provenance);
                changed += 1;
            }
        }
        if (changed == 0) {
            const cut = maximum orelse return error.InvalidPrivateIndexClaimInventory;
            if (replay) |position| {
                if (position == .native and !std.meta.eql(position, cut)) return error.InvalidPrivateIndexClaimReceipt;
                if (positionNumber(position) < positionNumber(cut) or position.term() < cut.term() or
                    (positionNumber(position) == positionNumber(cut) and position.term() != cut.term())) return error.InvalidControlReceiptPosition;
            }
            return .{ .position = cut, .duplicate = true, .claims_added = 0 };
        }
        if (try optional(txn, &InventoryValidation.key(self.binding)) != null) return error.PrivateIndexTargetSealed;
        const position: Position = if (self.binding.authority == .raft) replay.? else .{ .native = .{ .namespace = self.binding.fence.namespace, .sequence = std.math.add(u64, current.sequence, 1) catch return error.OnlineSourceCorrupt } };
        if (replay) |cut| if (!std.meta.eql(cut, position)) return error.OnlineSourceScopeChanged;
        if (position == .raft and (position.raft.index <= provenance.maximum or position.raft.term < provenance.maximum_cut.raft.term)) return error.InvalidControlReceiptPosition;
        try txn.retained.bindPrivateClaims(allocation.generation, prefix);
        for (self.entries) |entry| {
            if (try optional(txn, &entry.address.claimKey()) != null) continue;
            const key = inventoryKey(entry.address);
            try txn.put(&entry.address.claimKey(), entry.value);
            var inventory_bytes: [max_inventory_value_len]u8 = undefined;
            try txn.put(&key, try encodeInventory(&key, provenance.binding_digest, hash(entry.value), position, &inventory_bytes));
        }
        if (position == .native) _ = try authority.advance(txn, namespace, position.native.sequence);
        return .{ .position = position, .duplicate = false, .claims_added = changed };
    }
};

fn optionalEqual(a: ?[]const u8, b: ?[]const u8) bool {
    return if (a) |left| if (b) |right| std.mem.eql(u8, left, right) else false else b == null;
}

fn checkInventory(txn: anytype, address: integrity.Address, expected: ?[]const u8, provenance: Provenance) !u64 {
    return if (try checkInventoryPosition(txn, address, expected, provenance)) |position| positionNumber(position) else 0;
}

fn checkInventoryPosition(txn: anytype, address: integrity.Address, expected: ?[]const u8, provenance: Provenance) !?Position {
    const actual = try optional(txn, &inventoryKey(address));
    if (expected) |value| {
        return try decodeInventoryPosition(&inventoryKey(address), actual orelse return error.InvalidPrivateIndexClaimInventory, provenance, hash(value));
    } else if (actual != null) return error.InvalidPrivateIndexClaimInventory;
    return null;
}

/// Durable, bounded claim-inventory verification. This freezes NEW claim
/// writes, not exact duplicates, and never claims that the physical secondary
/// index or the complete distributed publication has been validated.
pub const InventoryValidation = struct {
    const validation_prefix = jobs.key ++ ":target-inventory-validation:";
    pub const max_wire_bytes = 16 * 1024;
    const max_record_bytes = 8 * 1024;
    pub const Phase = enum { scanning, claims, ready, failed };
    pub const Failure = enum { none, inventory_mismatch, claim_companion };
    pub const Fields = struct {
        phase: Phase = .scanning,
        failure: Failure = .none,
        rows: u64 = 0,
        inventory_xor: integrity.Digest = @splat(0),
        claim_rows: u64 = 0,
        claim_xor: integrity.Digest = @splat(0),
        claim_cursor: ?[integrity.key_len]u8 = null,
        chain: integrity.Digest,
        cursor: ?[key_len]u8 = null,
        pub fn validate(self: Fields, expected: TargetExpectation) !void {
            try expected.validate();
            if (std.mem.allEqual(u8, &self.chain, 0) or (self.rows == 0) != (self.cursor == null)) return error.InvalidPrivateIndexClaimInventory;
            if ((self.phase == .failed) != (self.failure != .none) or (self.phase == .scanning and self.rows > expected.rows) or
                (self.rows == 0 and !std.mem.allEqual(u8, &self.inventory_xor, 0)) or
                (self.phase == .scanning and (self.claim_rows != 0 or self.claim_cursor != null or !std.mem.allEqual(u8, &self.claim_xor, 0))) or
                (self.phase == .scanning and self.rows == 0 and !std.mem.eql(u8, &self.chain, &(try initial(expected)).chain)) or
                (self.claim_rows == 0 and !std.mem.allEqual(u8, &self.claim_xor, 0)) or (self.claim_rows != 0 and self.claim_cursor == null)) return error.InvalidPrivateIndexClaimInventory;
            if (self.cursor) |cursor| {
                const address = try addressFromInventory(&cursor);
                if (!std.mem.eql(u8, &address.generation, &expected.generation)) return error.InvalidPrivateIndexClaimInventory;
            }
            if (self.claim_cursor) |cursor| if ((try integrity.parseKey(&cursor)).kind != .claim) return error.InvalidPrivateIndexClaimInventory;
            const matches = self.rows == expected.rows and std.mem.eql(u8, &self.inventory_xor, &expected.inventory_xor);
            const reverse_matches = self.claim_rows == expected.rows and std.mem.eql(u8, &self.claim_xor, &expected.inventory_xor);
            if (((self.phase == .claims or self.phase == .ready) and !matches) or
                (self.phase == .claims and self.claim_rows > expected.rows) or
                (self.phase == .ready and !reverse_matches) or
                (self.failure == .inventory_mismatch and matches and reverse_matches)) return error.InvalidPrivateIndexClaimInventory;
        }
    };
    pub const Record = struct {
        expected: TargetExpectation,
        fields: Fields,
        position: Native,
        page_digest: integrity.Digest,
        pub fn validate(self: Record) !void {
            try self.fields.validate(self.expected);
            try self.position.validate();
            if (std.mem.allEqual(u8, &self.page_digest, 0)) return error.InvalidPrivateIndexClaimInventory;
        }
        pub fn encode(self: Record, alloc: Allocator) ![]u8 {
            try self.validate();
            const json = try std.json.Stringify.valueAlloc(alloc, self, .{});
            defer alloc.free(json);
            if (json.len > max_record_bytes - 40) return error.InvalidPrivateIndexClaimInventory;
            return std.mem.concat(alloc, u8, &.{ "AICV\x02\x00\x00\x00", json, &hash(json) });
        }
        pub fn decode(alloc: Allocator, bytes: []const u8) !Record {
            if (bytes.len <= 40 or bytes.len > max_record_bytes or !std.mem.eql(u8, bytes[0..8], "AICV\x02\x00\x00\x00") or
                !std.mem.eql(u8, bytes[bytes.len - 32 ..], &hash(bytes[8 .. bytes.len - 32]))) return error.InvalidPrivateIndexClaimInventory;
            var parsed = try std.json.parseFromSlice(Record, alloc, bytes[8 .. bytes.len - 32], .{ .ignore_unknown_fields = false, .max_value_len = 128 });
            defer parsed.deinit();
            try parsed.value.validate();
            const canonical = try parsed.value.encode(alloc);
            defer alloc.free(canonical);
            if (!std.mem.eql(u8, bytes, canonical)) return error.InvalidPrivateIndexClaimInventory;
            return parsed.value;
        }
    };
    pub const Command = struct {
        binding: jobs.Binding,
        expected: TargetExpectation,
        previous: integrity.Digest,
        from: Phase,
        scanned: u16,
        visited: u16,
        finish: bool,
        next: Fields,
        pub fn validate(self: Command) !void {
            try self.expected.requireBinding(self.binding);
            try self.next.validate(self.expected);
            if ((self.from != .scanning and self.from != .claims) or self.visited > max_claims or self.scanned > self.visited or
                (self.from == .scanning and (self.visited != self.scanned or self.next.rows < self.scanned)) or
                (self.from == .claims and self.next.claim_rows < self.scanned) or
                (std.mem.allEqual(u8, &self.previous, 0) and (self.from != .scanning or self.visited != 0 or self.finish or self.next.rows != 0 or self.next.phase != .scanning)) or
                (self.finish != (self.next.phase != self.from))) return error.InvalidPrivateIndexClaimInventory;
        }
        pub fn digest(self: Command) !integrity.Digest {
            try self.validate();
            var state = std.crypto.hash.Blake3.init(.{});
            state.update("antfly/private-unique-target-page/v2");
            state.update(&try self.binding.encode());
            state.update(&try self.expected.digest());
            state.update(&self.previous);
            var numbers: [22]u8 = undefined;
            std.mem.writeInt(u16, numbers[0..2], self.scanned, .little);
            std.mem.writeInt(u16, numbers[2..4], self.visited, .little);
            numbers[4] = @intFromBool(self.finish);
            numbers[5] = @backingInt(self.from);
            std.mem.writeInt(u64, numbers[6..14], self.next.rows, .little);
            std.mem.writeInt(u64, numbers[14..22], self.next.claim_rows, .little);
            state.update(&numbers);
            state.update(&self.next.inventory_xor);
            state.update(&self.next.claim_xor);
            state.update(&self.next.chain);
            state.update(&.{ @intFromBool(self.next.cursor != null), @intFromBool(self.next.claim_cursor != null), @backingInt(self.next.phase), @backingInt(self.next.failure) });
            if (self.next.cursor) |cursor| state.update(&cursor);
            if (self.next.claim_cursor) |cursor| state.update(&cursor);
            var result: integrity.Digest = undefined;
            state.final(&result);
            return result;
        }
    };
    pub fn key(binding: jobs.Binding) [validation_prefix.len + 16]u8 {
        var result: [validation_prefix.len + 16]u8 = undefined;
        @memcpy(result[0..validation_prefix.len], validation_prefix);
        @memcpy(result[validation_prefix.len..], &binding.id);
        return result;
    }
    fn initial(expected: TargetExpectation) !Fields {
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-target-inventory/v1");
        state.update(&try expected.digest());
        var chain: integrity.Digest = undefined;
        state.final(&chain);
        return .{ .chain = chain };
    }
    /// Claim keys are routing-first in the established integrity format. Audit
    /// that authoritative namespace without inventing a generation directory
    /// whose missing entries would hide the very orphans being checked.
    /// Unrelated values are neither decoded nor hashed. Replay verifies the
    /// bounded cursor interval, allowing unrelated retired claims to disappear.
    fn scanClaims(txn: anytype, binding: jobs.Binding, expected: TargetExpectation, provenance: Provenance, before: Fields, budget: @import("relational_index_jobs.zig").Budget, context: @import("../../api/operation.zig").RequestContext, replay_command: ?Command) !struct { next: Fields, scanned: u16, visited: u16 } {
        var next = before;
        const start = integrity.namespace ++ "\x01";
        var upper: [start.len]u8 = start.*;
        upper[upper.len - 1] += 1;
        var cursor = try txn.openCursor();
        defer cursor.close();
        cursor.setUpperBound(&upper);
        var entry = if (before.claim_cursor) |after| try cursor.seekAtOrAfter(&after) else try cursor.seekAtOrAfter(start);
        if (before.claim_cursor) |after| if (entry) |item| if (std.mem.eql(u8, item.key, &after)) {
            entry = try cursor.next();
        };
        if (replay_command) |command| {
            if (command.from != .claims or (command.visited != 0 and command.next.claim_cursor == null)) return error.InvalidPrivateIndexClaimInventory;
            if (before.claim_cursor) |old| if (command.next.claim_cursor) |end| if (std.mem.order(u8, &end, &old) == .lt) return error.InvalidPrivateIndexClaimInventory;
        }
        var visited: u16 = 0;
        var scanned: u16 = 0;
        var bytes: usize = 0;
        const byte_limit = @min(budget.bytes, max_bytes);
        const begun = @import("antfly_platform").time.monotonicNs();
        while (entry) |item| {
            if (!std.mem.startsWith(u8, item.key, start)) break;
            if (replay_command) |command| {
                const end = command.next.claim_cursor orelse break;
                if (std.mem.order(u8, item.key, &end) == .gt) break;
                if (visited >= command.visited) return error.InvalidPrivateIndexClaimInventory;
            }
            try context.ensureActive();
            const parsed = try integrity.parseKey(item.key);
            if (parsed.kind != .claim or item.value.len > integrity.max_record_bytes) return error.InvalidPrivateIndexClaimInventory;
            const own = std.mem.eql(u8, &parsed.address.generation, &expected.generation);
            const cost = item.value.len + integrity.key_len + (if (own) key_len + inventory_value_len else @as(usize, 0));
            // Existing unrelated claims can reach the established record
            // limit. Permit one indivisible foreign record, not a new smaller
            // storage-format limit imposed by this audit's byte quantum.
            if (own and cost > max_bytes) return error.InvalidPrivateIndexClaimInventory;
            if (cost > byte_limit and (own or visited != 0)) {
                if (visited == 0) return error.InvalidRelationalIndexBudget;
                break;
            }
            if (visited != 0 and cost > byte_limit - bytes) break;
            next.claim_cursor = parsed.address.claimKey();
            visited += 1;
            bytes += cost;
            if (own) {
                scanned += 1;
                next.claim_rows = std.math.add(u64, next.claim_rows, 1) catch return error.InvalidPrivateIndexClaimInventory;
                const value_hash = hash(item.value);
                var chain = std.crypto.hash.Blake3.init(.{});
                chain.update("antfly/private-unique-claim-reverse/v1");
                chain.update(&next.chain);
                chain.update(&parsed.address.claimKey());
                chain.update(&value_hash);
                const claim = integrity.Claim.decode(&parsed.address.claimKey(), item.value) catch null;
                var valid = false;
                if (claim) |value| {
                    const member = value.memberDigest();
                    for (&next.claim_xor, member) |*byte, contribution| byte.* ^= contribution;
                    if (value.state == .live and value.schema_version == binding.physical_candidate.schema_version and std.mem.eql(u8, &hash(value.parent_table), &expected.table_name_digest)) {
                        if (try optional(txn, &inventoryKey(parsed.address))) |inventory| {
                            if (inventory.len == inventory_value_len) {
                                chain.update(inventory);
                                valid = if (decodeInventory(&inventoryKey(parsed.address), inventory, provenance, value_hash)) |_| true else |_| false;
                            }
                        }
                    }
                }
                chain.final(&next.chain);
                if (!valid) {
                    next.phase = .failed;
                    next.failure = .claim_companion;
                    break;
                }
                if (next.claim_rows > expected.rows) {
                    next.phase = .failed;
                    next.failure = .inventory_mismatch;
                    break;
                }
            }
            entry = try cursor.next();
            if (visited >= @min(budget.records, max_claims) or bytes >= byte_limit or (replay_command == null and @import("antfly_platform").time.monotonicNs() -| begun >= budget.time_ns)) break;
        }
        if (replay_command) |command| {
            if (visited > command.visited or scanned != command.scanned) return error.InvalidPrivateIndexClaimInventory;
            if (next.phase != .failed) {
                if (entry) |item| if (std.mem.startsWith(u8, item.key, start)) {
                    if (command.next.claim_cursor) |end| if (std.mem.order(u8, item.key, &end) != .gt) return error.InvalidPrivateIndexClaimInventory;
                };
                // The endpoint itself may have been an unrelated retired
                // claim deleted since preparation. Coverage, not its continued
                // existence or diagnostic visited count, is the invariant.
                next.claim_cursor = command.next.claim_cursor;
            }
            visited = command.visited;
        }
        const exhausted = next.phase != .failed and (entry == null or !std.mem.startsWith(u8, entry.?.key, start));
        if (exhausted and (replay_command == null or replay_command.?.finish)) {
            next.phase = if (next.claim_rows == expected.rows and std.mem.eql(u8, &next.claim_xor, &expected.inventory_xor)) .ready else .failed;
            if (next.phase == .failed) next.failure = .inventory_mismatch;
        }
        return .{ .next = next, .scanned = scanned, .visited = visited };
    }
    fn requireLocal(txn: anytype, binding: jobs.Binding, expected: TargetExpectation) !Provenance {
        try expected.requireBinding(binding);
        try jobs.requireReserved(txn, binding);
        const allocation = try requireRegistered(txn, binding);
        if (!std.mem.eql(u8, &allocation.generation, &expected.generation)) return error.PrivateIndexOwnerJobChanged;
        const source = (try SourceStream.load(txn, binding)) orelse return error.InvalidPrivateIndexClaimInventory;
        if (source.state != .ready or source.pending() != 0 or !std.mem.eql(u8, &source.progress_digest, &expected.source_progress_digest)) return error.PrivateIndexSourceConsumptionRequired;
        const consumed = (try SourceStream.loadConsumption(txn, binding, source)) orelse return error.PrivateIndexSourceConsumptionRequired;
        if (!std.mem.eql(u8, &consumed.accepted.metadata_incarnation, &expected.metadata_incarnation) or
            !std.mem.eql(u8, &try consumed.prefix().digest(), &expected.source_consumption_digest)) return error.PrivateIndexOwnerJobChanged;
        const pages = @import("relational_index_jobs.zig");
        const owner = try pages.ownership(txn);
        const raw = try txn.get(&pages.progressKey(binding.index));
        if (!std.mem.eql(u8, &hash(raw), &expected.source_progress_digest)) return error.InvalidRelationalIndexProgress;
        const progress = try pages.Progress.decode(raw);
        if (progress.phase != .primary or progress.state != .ready or progress.id.mapKey() != binding.index.mapKey() or !std.mem.eql(u8, &progress.owner, &owner)) return error.InvalidRelationalIndexProgress;
        return Provenance.init(txn, binding);
    }
    pub fn load(alloc: Allocator, txn: anytype, binding: jobs.Binding) !?Record {
        const bytes = (try optional(txn, &key(binding))) orelse return null;
        const record = try Record.decode(alloc, bytes);
        try record.expected.requireBinding(binding);
        const provenance = try Provenance.init(txn, binding);
        if (!record.position.namespace.eql(binding.fence.namespace) or record.position.sequence <= provenance.reserved or record.position.sequence > provenance.maximum) return error.InvalidPrivateIndexClaimInventory;
        return record;
    }
    pub fn requireReady(alloc: Allocator, txn: anytype, binding: jobs.Binding, expected: TargetExpectation) !struct { digest: integrity.Digest, source_members: u64, position: Native } {
        _ = try requireLocal(txn, binding, expected);
        const record = (try load(alloc, txn, binding)) orelse return error.PrivateIndexTargetValidationRequired;
        if (record.fields.phase != .ready or !std.mem.eql(u8, &try record.expected.digest(), &try expected.digest())) return error.PrivateIndexTargetValidationRequired;
        const encoded = try record.encode(alloc);
        defer alloc.free(encoded);
        const head = (try SourceStream.load(txn, binding)).?;
        const consumed = (try SourceStream.loadConsumption(txn, binding, head)).?;
        return .{ .digest = hash(encoded), .source_members = consumed.accepted.rows, .position = record.position };
    }
    /// Root-local audit state must never be imported as canonical job state or
    /// submitted through the native mutation/outbox interface. Its owner keeps
    /// it outside the replicated snapshot, under the physical root's lease.
    /// This component grants no adoption, signing or publication authority.
    pub const Audit = struct {
        root_incarnation: u128,
        expectation_digest: integrity.Digest,
        canonical_digest: integrity.Digest,
        fields: Fields,
        page_digest: integrity.Digest,
    };
    /// Recompute physical coverage from scratch without rewinding canonical
    /// cursors, advancing native authority, or perturbing replication markers.
    /// Ordinary restart may resume a root-local checkpoint; a different root
    /// or canonical seal must start a new audit rather than adopt its prefix.
    pub fn auditPage(alloc: Allocator, txn: anytype, binding: jobs.Binding, expected: TargetExpectation, root_incarnation: u128, previous: ?Audit, budget: @import("relational_index_jobs.zig").Budget, context: @import("../../api/operation.zig").RequestContext) !Audit {
        try context.ensureActive();
        try budget.validate();
        if (root_incarnation == 0) return error.PrivateIndexAuditScopeChanged;
        const provenance = try requireLocal(txn, binding, expected);
        const canonical = (try load(alloc, txn, binding)) orelse return error.PrivateIndexTargetValidationRequired;
        if (!std.mem.eql(u8, &try canonical.expected.digest(), &try expected.digest())) return error.PrivateIndexAuditScopeChanged;
        const canonical_bytes = try canonical.encode(alloc);
        defer alloc.free(canonical_bytes);
        const canonical_digest = hash(canonical_bytes);
        const expectation_digest = try expected.digest();
        var local: ?Record = null;
        var previous_digest: integrity.Digest = @splat(0);
        if (previous) |state| {
            if (state.root_incarnation != root_incarnation or !std.mem.eql(u8, &state.expectation_digest, &expectation_digest) or
                !std.mem.eql(u8, &state.canonical_digest, &canonical_digest)) return error.PrivateIndexAuditScopeChanged;
            try state.fields.validate(expected);
            if (std.mem.allEqual(u8, &state.page_digest, 0)) return error.InvalidPrivateIndexClaimInventory;
            if (state.fields.phase == .ready or state.fields.phase == .failed) return state;
            local = .{ .expected = expected, .fields = state.fields, .position = canonical.position, .page_digest = state.page_digest };
            const encoded = try local.?.encode(alloc);
            defer alloc.free(encoded);
            previous_digest = hash(encoded);
        }
        const page = (try InventoryValidation.Prepared.preparePage(txn, binding, expected, budget, context, provenance, local, previous_digest, null)) orelse return error.InvalidPrivateIndexClaimInventory;
        var digest = std.crypto.hash.Blake3.init(.{});
        digest.update("antfly/private-unique-root-local-audit/v1");
        var root_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &root_bytes, root_incarnation, .little);
        digest.update(&root_bytes);
        digest.update(&canonical_digest);
        digest.update(&try page.command.digest());
        var page_digest: integrity.Digest = undefined;
        digest.final(&page_digest);
        return .{ .root_incarnation = root_incarnation, .expectation_digest = expectation_digest, .canonical_digest = canonical_digest, .fields = page.command.next, .page_digest = page_digest };
    }
    pub const Prepared = struct {
        command: Command,
        context: @import("../../api/operation.zig").RequestContext,
        /// Preparation owns only fixed fields. No snapshot, row buffer or
        /// request slice survives the short read transaction.
        pub fn prepare(alloc: Allocator, txn: anytype, binding: jobs.Binding, expected: TargetExpectation, budget: @import("relational_index_jobs.zig").Budget, context: @import("../../api/operation.zig").RequestContext) !?@This() {
            return prepareImpl(alloc, txn, binding, expected, budget, context, null);
        }
        pub fn replay(alloc: Allocator, txn: anytype, command: Command) !@This() {
            try command.validate();
            const value = (try prepareImpl(alloc, txn, command.binding, command.expected, .{ .records = @max(1, command.visited), .bytes = max_bytes, .time_ns = std.time.ns_per_s }, .{}, command)) orelse return error.InvalidPrivateIndexClaimInventory;
            if (!std.mem.eql(u8, &try value.command.digest(), &try command.digest())) return error.InvalidPrivateIndexClaimInventory;
            return value;
        }
        fn prepareImpl(alloc: Allocator, txn: anytype, binding: jobs.Binding, expected: TargetExpectation, budget: @import("relational_index_jobs.zig").Budget, context: @import("../../api/operation.zig").RequestContext, replay_command: ?Command) !?@This() {
            try context.ensureActive();
            try budget.validate();
            const provenance = try requireLocal(txn, binding, expected);
            const record = try load(alloc, txn, binding);
            const previous = if (try optional(txn, &key(binding))) |bytes| hash(bytes) else @as(integrity.Digest, @splat(0));
            return preparePage(txn, binding, expected, budget, context, provenance, record, previous, replay_command);
        }
        fn preparePage(txn: anytype, binding: jobs.Binding, expected: TargetExpectation, budget: @import("relational_index_jobs.zig").Budget, context: @import("../../api/operation.zig").RequestContext, provenance: Provenance, record: ?Record, previous: integrity.Digest, replay_command: ?Command) !?@This() {
            if (record) |current| {
                if (!std.mem.eql(u8, &try current.expected.digest(), &try expected.digest())) return error.PrivateIndexOwnerJobChanged;
                if (replay_command) |command| if (std.mem.eql(u8, &current.page_digest, &try command.digest())) return .{ .command = command, .context = context };
                if (current.fields.phase != .scanning and current.fields.phase != .claims) return null;
            }
            var next = if (record) |current| current.fields else try initial(expected);
            const from = next.phase;
            var scanned: u16 = 0;
            var visited: u16 = 0;
            var finish = false;
            if (record != null and from == .scanning) {
                const start = generationPrefix(expected.generation);
                var cursor = try txn.openCursor();
                defer cursor.close();
                var entry = if (next.cursor) |after| try cursor.seekAtOrAfter(&after) else try cursor.seekAtOrAfter(&start);
                if (next.cursor) |after| if (entry) |item| if (std.mem.eql(u8, item.key, &after)) {
                    entry = try cursor.next();
                };
                const begun = @import("antfly_platform").time.monotonicNs();
                // Replay uses this fixed ceiling too. A larger caller budget
                // must not produce a page that its deterministic replayer
                // cannot consume in one bounded turn.
                const byte_limit = @min(budget.bytes, max_bytes);
                var bytes: usize = 0;
                while (entry) |item| {
                    if (!std.mem.startsWith(u8, item.key, &start)) break;
                    if (replay_command) |command| if (scanned >= command.scanned) break;
                    try context.ensureActive();
                    const address = try addressFromInventory(item.key);
                    if (item.value.len != inventory_value_len) return error.InvalidPrivateIndexClaimInventory;
                    const inventory = item.value[0..inventory_value_len].*;
                    const raw = (try optional(txn, &address.claimKey())) orelse return error.InvalidPrivateIndexClaimInventory;
                    if (raw.len > integrity.max_record_bytes) return error.InvalidPrivateIndexClaimInventory;
                    const cost = raw.len + key_len + integrity.key_len + inventory_value_len;
                    if (cost > max_bytes) return error.InvalidPrivateIndexClaimInventory;
                    if (cost > byte_limit) return error.InvalidRelationalIndexBudget;
                    if (cost > byte_limit - bytes) break;
                    const value_hash = hash(raw);
                    const claim = try integrity.Claim.decode(&address.claimKey(), raw);
                    if (claim.state != .live or claim.schema_version != binding.physical_candidate.schema_version or !std.mem.eql(u8, &hash(claim.parent_table), &expected.table_name_digest)) return error.InvalidPrivateIndexClaimInventory;
                    const member = claim.memberDigest();
                    _ = try decodeInventory(&inventoryKey(address), &inventory, provenance, value_hash);
                    var chain = std.crypto.hash.Blake3.init(.{});
                    chain.update(&next.chain);
                    chain.update(&inventoryKey(address));
                    chain.update(&value_hash);
                    chain.update(&inventory);
                    chain.final(&next.chain);
                    for (&next.inventory_xor, member) |*byte, contribution| byte.* ^= contribution;
                    next.rows = std.math.add(u64, next.rows, 1) catch return error.InvalidPrivateIndexClaimInventory;
                    next.cursor = inventoryKey(address);
                    scanned += 1;
                    visited += 1;
                    bytes += cost;
                    if (next.rows > expected.rows) {
                        next.phase = .failed;
                        next.failure = .inventory_mismatch;
                        finish = true;
                        break;
                    }
                    entry = try cursor.next();
                    if (scanned >= @min(budget.records, max_claims) or bytes >= byte_limit or (replay_command == null and @import("antfly_platform").time.monotonicNs() -| begun >= budget.time_ns)) break;
                }
                if (replay_command) |command| {
                    if (scanned != command.scanned or (command.finish and !finish and entry != null and std.mem.startsWith(u8, entry.?.key, &start))) return error.InvalidPrivateIndexClaimInventory;
                }
                if (!finish and (replay_command == null or replay_command.?.finish) and (entry == null or !std.mem.startsWith(u8, entry.?.key, &start))) {
                    finish = true;
                    next.phase = if (next.rows == expected.rows and std.mem.eql(u8, &next.inventory_xor, &expected.inventory_xor)) .claims else .failed;
                    if (next.phase == .failed) next.failure = .inventory_mismatch;
                }
            }
            if (record != null and from == .claims) {
                const result = try scanClaims(txn, binding, expected, provenance, next, budget, context, replay_command);
                next = result.next;
                scanned = result.scanned;
                visited = result.visited;
                finish = next.phase != from;
            }
            const command: Command = .{ .binding = binding, .expected = expected, .previous = previous, .from = from, .scanned = scanned, .visited = visited, .finish = finish, .next = next };
            try command.validate();
            return .{ .command = command, .context = context };
        }
        /// The enclosing mutation pipeline commits this record with the native
        /// authority and durable outbox, and MUST abort on every staging error.
        pub fn stage(self: *const @This(), alloc: Allocator, txn: anytype, replay_position: ?Native) !struct { record: Record, duplicate: bool } {
            try self.context.ensureActive();
            try self.command.validate();
            _ = try requireLocal(txn, self.command.binding, self.command.expected);
            const digest = try self.command.digest();
            const current = try load(alloc, txn, self.command.binding);
            if (current) |record| if (std.mem.eql(u8, &record.page_digest, &digest)) {
                if (replay_position) |position| if (!std.meta.eql(record.position, position)) return error.InvalidPrivateIndexClaimReceipt;
                return .{ .record = record, .duplicate = true };
            };
            const previous = if (try optional(txn, &key(self.command.binding))) |bytes| hash(bytes) else @as(integrity.Digest, @splat(0));
            if (!std.mem.eql(u8, &previous, &self.command.previous)) return error.VersionConflict;
            if (current) |record| if (record.fields.phase != self.command.from or !std.mem.eql(u8, &try record.expected.digest(), &try self.command.expected.digest())) return error.PrivateIndexOwnerJobChanged;
            if (replay_position) |position| if (!position.namespace.eql(self.command.binding.fence.namespace)) return error.IdentityNamespaceMismatch;
            try self.context.ensureActive();
            const position: Native = .{ .namespace = self.command.binding.fence.namespace, .sequence = try authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(self.command.binding.fence.namespace), if (replay_position) |value| value.sequence else null) };
            const record: Record = .{ .expected = self.command.expected, .fields = self.command.next, .position = position, .page_digest = digest };
            const bytes = try record.encode(alloc);
            defer alloc.free(bytes);
            try txn.put(&key(self.command.binding), bytes);
            return .{ .record = record, .duplicate = false };
        }
    };
    pub fn verifyRecovery(alloc: Allocator, txn: anytype, binding: jobs.Binding) !void {
        const record = (try load(alloc, txn, binding)) orelse return;
        const cancellation = @import("relational_index_cancellation_intent.zig");
        if (try cancellation.load(txn)) |closed| if (closed.intent.fence.admission_epoch == binding.fence.admission_epoch) return;
        _ = try requireLocal(txn, binding, record.expected);
    }
};

/// Pin once in the same source/staging transaction as a transfer page. This
/// avoids decoding the job or hashing its catalog once per claim. The view is
/// information for physical validation, never execution/publication authority.
pub const TransferView = struct {
    generation: ?integrity.Generation = null,
    provenance: ?Provenance = null,

    pub fn init(txn: anytype) !TransferView {
        const binding = (try jobs.load(txn)) orelse return .{};
        if (binding.phase != .reserved) return .{};
        return .{ .generation = (try requireRegistered(txn, binding)).generation, .provenance = try Provenance.init(txn, binding) };
    }

    pub fn isPrivate(self: TransferView, generation: integrity.Generation) bool {
        return if (self.generation) |current| std.mem.eql(u8, &current, &generation) else false;
    }

    pub fn validateCompanion(self: TransferView, txn: anytype, parsed: integrity.ParsedKey, value_digest: integrity.Digest) !void {
        if (!self.isPrivate(parsed.address.generation)) return;
        // An unpublished UNIQUE generation cannot have FK references/action
        // jobs. Those records must not acquire legitimacy through restoration.
        if (parsed.kind != .claim) return error.InvalidPrivateIndexClaimInventory;
        const actual = (try optional(txn, &inventoryKey(parsed.address))) orelse return error.InvalidPrivateIndexClaimInventory;
        _ = try decodeInventory(&inventoryKey(parsed.address), actual, self.provenance orelse return error.InvalidPrivateIndexClaimInventory, value_digest);
    }
};

/// Local emptiness only; metadata must still prove complete cohort retirement
/// and persist the final cancellation cut before any owner acknowledgement.
pub fn requireRetired(txn: anytype, binding: jobs.Binding) !void {
    _ = try jobs.requireCancellationCleanup(txn, binding);
    const allocation = try requireRegistered(txn, binding);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const start = generationPrefix(allocation.generation);
    if (try cursor.seekAtOrAfter(&start)) |entry| if (std.mem.startsWith(u8, entry.key, &start)) return error.PrivateIndexCancellationPending;
    try SourceStream.requireCanceledRetired(txn, binding);
}

/// Deleting the first bounded inventory page is itself the durable continuation:
/// abort leaves every pair present, commit advances both keys atomically. No
/// whole-claim scan, RAM cursor or generation-global rewrite is required.
pub const GcPage = struct {
    arena: std.heap.ArenaAllocator,
    binding: jobs.Binding,
    entries: []const Entry,

    pub fn deinit(self: *GcPage) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn prepare(alloc: Allocator, io: ?std.Io, txn: anytype, binding: jobs.Binding) !?GcPage {
        _ = try jobs.requireCancellationCleanup(txn, binding);
        const allocation = try requireRegistered(txn, binding);
        const provenance = try Provenance.init(txn, binding);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var entries: std.ArrayList(Entry) = .empty;
        var cursor = try txn.openCursor();
        defer cursor.close();
        const start = generationPrefix(allocation.generation);
        const time = @import("antfly_platform").time;
        const started = time.monotonicNs();
        var bytes: usize = 0;
        var next = try cursor.seekAtOrAfter(&start);
        while (next) |entry| : (next = try cursor.next()) {
            if (!std.mem.startsWith(u8, entry.key, &start)) break;
            if (io) |runtime| try runtime.checkCancel();
            const address = try addressFromInventory(entry.key);
            if (entry.value.len != inventory_value_len) return error.InvalidPrivateIndexClaimInventory;
            // Copy before the claim read can invalidate borrowed cursor bytes.
            const inventory = entry.value[0..inventory_value_len].*;
            const raw = (try optional(txn, &address.claimKey())) orelse return error.InvalidPrivateIndexClaimInventory;
            if (raw.len > integrity.max_record_bytes) return error.InvalidPrivateIndexClaimInventory;
            const record_bytes = raw.len + key_len + integrity.key_len + inventory_value_len;
            if (record_bytes > max_bytes) return error.InvalidPrivateIndexClaimInventory;
            // Stop BEFORE owning another row; checking after append can admit
            // almost two page budgets when the next claim is large.
            if (record_bytes > max_bytes - bytes) break;
            const sequence = try decodeInventory(&inventoryKey(address), &inventory, provenance, hash(raw));
            const value = try owned.dupe(u8, raw);
            const claim = try integrity.Claim.decode(&address.claimKey(), value);
            if (claim.state != .live or claim.schema_version != binding.physical_candidate.schema_version) return error.InvalidPrivateIndexClaimInventory;
            try entries.append(owned, .{ .address = address, .value = value, .expected = value, .sequence = sequence });
            bytes += record_bytes;
            if (entries.items.len >= 256 or bytes >= max_bytes or time.monotonicNs() -| started >= 5 * std.time.ns_per_ms) break;
        }
        if (entries.items.len == 0) {
            arena.deinit();
            return null;
        }
        return .{ .arena = arena, .binding = binding, .entries = entries.items };
    }

    pub fn stage(self: *const GcPage, txn: anytype) !void {
        _ = try jobs.requireCancellationCleanup(txn, self.binding);
        const allocation = try requireRegistered(txn, self.binding);
        const provenance = try Provenance.init(txn, self.binding);
        try txn.retained.bindPrivateClaims(allocation.generation, prefix);
        for (self.entries) |entry| {
            if (!std.mem.eql(u8, &entry.address.generation, &allocation.generation)) return error.InvalidPrivateIndexClaimInventory;
            if (!optionalEqual(try optional(txn, &entry.address.claimKey()), entry.expected)) return error.VersionConflict;
            if (try checkInventory(txn, entry.address, entry.expected, provenance) != entry.sequence) return error.VersionConflict;
            if (try optional(txn, &entry.address.jobKey()) != null) return error.InvalidPrivateIndexClaimInventory;
            var cursor = try txn.openCursor();
            defer cursor.close();
            const reference = entry.address.referencePrefix();
            if (try cursor.seekAtOrAfter(&reference)) |record| if (std.mem.startsWith(u8, record.key, &reference)) return error.InvalidPrivateIndexClaimInventory;
        }
        for (self.entries) |entry| {
            try txn.delete(&entry.address.claimKey());
            try txn.delete(&inventoryKey(entry.address));
        }
    }
};
