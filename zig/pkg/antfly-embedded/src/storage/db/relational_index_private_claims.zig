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
pub const max_claims = 128;
pub const delivery = @import("relational_index_claim_delivery.zig");

/// Durable source pages, independent of mirror transport acknowledgments.
/// Headers stay compact for recovery; payloads are fetched only for delivery.
/// This queue grants neither target ownership nor publication authority.
pub const SourceStream = struct {
    pub const head_key = jobs.key ++ ":claim-source-head";
    pub const batch_prefix = jobs.key ++ ":claim-source-batch:";
    pub const max_pending = 64;
    pub const max_pending_bytes = 8 * 1024 * 1024;
    pub const max_payload_bytes = 1024 * 1024;
    pub const header_len = 320;
    pub const Key = [batch_prefix.len + 25]u8;
    pub const State = enum(u8) { building, ready, failed };
    pub const Row = struct { key: []const u8, tuple: []const u8 };
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
        pub const encoded_len = 264;
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

        pub fn pending(self: Head) u64 {
            return self.emitted - self.consumed;
        }
        fn validate(self: Head) !void {
            // No consumption is authorized until ordered, authenticated
            // complete-delivery closures exist. A mirror ACK is insufficient.
            if (self.consumed != 0) return error.InvalidPrivateIndexClaimInventory;
            if (std.mem.allEqual(u8, &self.binding_digest, 0) or std.mem.allEqual(u8, &self.scope_digest, 0) or
                self.consumed > self.emitted or self.emitted - self.consumed > max_pending or
                self.pending_bytes > max_pending_bytes or (self.emitted == self.consumed) != (self.pending_bytes == 0) or
                self.emitted == 0 or self.last_sequence == 0 or self.consumed_rows > self.emitted_rows or
                self.consumed_sequence > self.last_sequence or std.mem.allEqual(u8, &self.last_digest, 0) or
                std.mem.allEqual(u8, &self.progress_digest, 0) or
                (self.consumed == 0 and (self.consumed_rows != 0 or self.consumed_sequence != 0 or !std.mem.allEqual(u8, &self.consumed_digest, 0)))) return error.InvalidPrivateIndexClaimInventory;
        }
        pub fn encode(self: Head) ![encoded_len]u8 {
            try self.validate();
            var bytes: [encoded_len]u8 = @splat(0);
            @memcpy(bytes[0..8], "AICQ\x01\x00\x00\x00");
            @memcpy(bytes[8..40], &self.binding_digest);
            @memcpy(bytes[40..72], &self.scope_digest);
            inline for (.{ "emitted", "consumed", "pending_bytes", "emitted_rows", "last_sequence" }, 0..) |field, i| std.mem.writeInt(u64, bytes[72 + i * 8 ..][0..8], @field(self, field), .little);
            @memcpy(bytes[112..144], &self.last_digest);
            std.mem.writeInt(u64, bytes[144..152], self.consumed_rows, .little);
            std.mem.writeInt(u64, bytes[152..160], self.consumed_sequence, .little);
            @memcpy(bytes[160..192], &self.consumed_digest);
            bytes[192] = @backingInt(self.state);
            @memcpy(bytes[200..232], &self.progress_digest);
            @memcpy(bytes[232..264], &hash(bytes[0..232]));
            return bytes;
        }
        pub fn decode(bytes: []const u8) !Head {
            if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AICQ\x01\x00\x00\x00") or
                !std.mem.allEqual(u8, bytes[193..200], 0) or !std.mem.eql(u8, bytes[232..264], &hash(bytes[0..232]))) return error.InvalidPrivateIndexClaimInventory;
            var result: Head = .{ .binding_digest = bytes[8..40].*, .scope_digest = bytes[40..72].*, .last_digest = bytes[112..144].*, .consumed_rows = std.mem.readInt(u64, bytes[144..152], .little), .consumed_sequence = std.mem.readInt(u64, bytes[152..160], .little), .consumed_digest = bytes[160..192].*, .state = std.enums.fromInt(State, bytes[192]) orelse return error.InvalidPrivateIndexClaimInventory, .progress_digest = bytes[200..232].* };
            inline for (.{ "emitted", "consumed", "pending_bytes", "emitted_rows", "last_sequence" }, 0..) |field, i| @field(result, field) = std.mem.readInt(u64, bytes[72 + i * 8 ..][0..8], .little);
            try result.validate();
            return result;
        }
    };

    pub fn load(txn: anytype, binding: jobs.Binding) !?Head {
        const raw = (try optional(txn, head_key)) orelse return null;
        const head = try Head.decode(raw);
        if (!std.mem.eql(u8, &head.binding_digest, &try bindingDigest(binding))) return error.PrivateIndexOwnerJobChanged;
        return head;
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
            if (cut_position != .native) return error.InvalidPrivateIndexClaimInventory;
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
            return (try @import("receipt_position.zig").Position.decode(self.bytes[112..145])).native;
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
            const allocation = try requireRegistered(txn, binding);
            if (!std.mem.eql(u8, &allocation.generation, &self.scope.generation)) return error.InvalidPrivateIndexClaimInventory;
            var head = (try load(txn, binding)) orelse Head{ .binding_digest = try bindingDigest(binding), .scope_digest = try self.scope.digest() };
            if (!std.mem.eql(u8, &head.scope_digest, &try self.scope.digest()) or head.state != .building or position.sequence <= head.last_sequence) return error.PrivateIndexOwnerJobChanged;
            const bytes: u64 = self.payload.len + header_len + 2 * @sizeOf(Key);
            if (head.emitted - head.consumed >= max_pending or head.pending_bytes + bytes > max_pending_bytes) return error.PrivateIndexClaimBacklogFull;
            head.emitted = try std.math.add(u64, head.emitted, 1);
            std.mem.writeInt(u64, self.header[104..112], head.emitted, .little);
            @memcpy(self.header[112..145], &try (@import("receipt_position.zig").Position{ .native = position }).encode());
            @memcpy(self.header[256..288], &head.last_digest);
            @memcpy(self.header[288..320], &hash(self.header[0..288]));
            const header = try Header.decode(&self.header, binding, head.emitted);
            head.pending_bytes += bytes;
            head.emitted_rows = try std.math.add(u64, head.emitted_rows, std.mem.readInt(u32, self.header[96..100], .little));
            head.last_sequence = position.sequence;
            head.last_digest = header.digest();
            head.progress_digest = self.header[152..184].*;
            head.state = @fromBackingInt(@intCast(self.header[184]));
            try txn.put(&key(binding.id, head.emitted, false), &self.header);
            try txn.put(&key(binding.id, head.emitted, true), self.payload);
            try txn.put(head_key, &try head.encode());
        }
    };

    pub fn requireLatest(txn: anytype, binding: jobs.Binding, source: Scope, position: Native, progress_digest: integrity.Digest, batch_digest: integrity.Digest) !Head {
        const head = (try load(txn, binding)) orelse return error.InvalidPrivateIndexClaimInventory;
        if (!std.mem.eql(u8, &head.last_digest, &batch_digest) or !std.mem.eql(u8, &head.scope_digest, &try source.digest()) or head.last_sequence != position.sequence or
            !std.mem.eql(u8, &head.progress_digest, &progress_digest)) return error.InvalidPrivateIndexClaimInventory;
        if (head.pending() != 0) {
            const raw = (try optional(txn, &key(binding.id, head.emitted, false))) orelse return error.InvalidPrivateIndexClaimInventory;
            const header = try Header.decode(raw, binding, head.emitted);
            if (!std.meta.eql(try header.position(), position) or
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
        const head = (try load(txn, binding)) orelse {
            if (expected != null) return error.InvalidPrivateIndexClaimInventory;
            return;
        };
        const cut = expected orelse return error.InvalidPrivateIndexClaimInventory;
        const allocation = try requireRegistered(txn, binding);
        if (cut.sequence != head.last_sequence or progress_digest == null or batch_digest == null or
            !std.mem.eql(u8, &head.last_digest, &batch_digest.?) or !std.mem.eql(u8, &head.progress_digest, &progress_digest.?)) return error.InvalidPrivateIndexClaimInventory;
        var previous = head.consumed_digest;
        var previous_sequence = head.consumed_sequence;
        var bytes: u64 = 0;
        var rows: u64 = 0;
        for (0..@intCast(head.pending())) |i| {
            const ordinal = head.consumed + i + 1;
            const raw = (try optional(txn, &key(binding.id, ordinal, false))) orelse return error.InvalidPrivateIndexClaimInventory;
            const header = try Header.decode(raw, binding, ordinal);
            const position = try header.position();
            if (!std.mem.eql(u8, header.bytes[40..72], &head.scope_digest) or
                !std.mem.eql(u8, header.bytes[72..88], &allocation.generation) or
                !std.mem.eql(u8, header.bytes[256..288], &previous) or position.sequence <= previous_sequence or
                (ordinal != head.emitted and header.bytes[184] != @backingInt(State.building)) or
                (ordinal == head.emitted and (!std.meta.eql(position, cut) or
                    !std.mem.eql(u8, header.bytes[152..184], &head.progress_digest) or header.bytes[184] != @backingInt(head.state)))) return error.InvalidPrivateIndexClaimInventory;
            previous = header.digest();
            previous_sequence = position.sequence;
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
        if (raw.len != header.payloadLen() or !std.mem.eql(u8, header.bytes[224..256], &hash(raw))) return error.InvalidPrivateIndexClaimInventory;
        const payload = try alloc.dupe(u8, raw);
        errdefer alloc.free(payload);
        const name_len = std.mem.readInt(u32, header.bytes[92..96], .little);
        const scope_value: Scope = .{ .table_name = payload[0..name_len], .generation = header.bytes[72..88].*, .schema_version = std.mem.readInt(u32, header.bytes[88..92], .little), .tuple_fingerprint = header.bytes[192..224].*, .nulls_not_distinct = header.bytes[185] != 0 };
        if (!std.mem.eql(u8, &try scope_value.digest(), &head.scope_digest)) return error.InvalidPrivateIndexClaimInventory;
        var result: OwnedBatch = .{ .alloc = alloc, .payload = payload, .header = header, .scope = scope_value, .entries = undefined, .count = std.mem.readInt(u32, header.bytes[96..100], .little) };
        var offset: usize = name_len;
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
        }
        if (offset != payload.len) return error.InvalidPrivateIndexClaimInventory;
        return result;
    }

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
    }
};

pub const max_bytes = 1024 * 1024;
const key_len = prefix.len + 16 + 32 + 32;
// AFC1 fixed fields/checksum, both paired keys, and the provenance value.
// Count the actual bytes GC must carry, not just request string lengths.
pub const claim_entry_overhead = 76 + key_len + integrity.key_len + inventory_value_len;

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

fn encodeInventory(key: []const u8, binding_digest: integrity.Digest, value_digest: integrity.Digest, sequence: u64) [inventory_value_len]u8 {
    var result: [inventory_value_len]u8 = undefined;
    std.mem.writeInt(u64, result[0..8], sequence, .little);
    @memcpy(result[8..], &inventoryChecksum(key, binding_digest, value_digest, result[0..8]));
    return result;
}

fn decodeInventory(key: []const u8, value: []const u8, provenance: Provenance, value_digest: integrity.Digest) !u64 {
    _ = try addressFromInventory(key);
    if (value.len != inventory_value_len or
        !std.mem.eql(u8, value[8..], &inventoryChecksum(key, provenance.binding_digest, value_digest, value[0..8]))) return error.InvalidPrivateIndexClaimInventory;
    const sequence = std.mem.readInt(u64, value[0..8], .little);
    if (sequence <= provenance.reserved or sequence > provenance.maximum) return error.InvalidPrivateIndexClaimInventory;
    return sequence;
}

const Provenance = struct {
    binding_digest: integrity.Digest,
    reserved: u64,
    maximum: u64,

    fn init(txn: anytype, binding: jobs.Binding) !Provenance {
        const namespace = @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace);
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
        return .{ .binding_digest = try bindingDigest(binding), .reserved = reserved.sequence, .maximum = maximum };
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
        if (claims.len == 0 or claims.len > max_claims or binding.authority != .native) return error.InvalidPrivateIndexClaimInventory;
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
            const sequence = try checkInventory(txn, address, expected, provenance);
            try entries.append(owned, .{ .address = address, .value = value, .expected = expected, .sequence = sequence });
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
        try jobs.requireReserved(txn, self.binding);
        const allocation = try requireRegistered(txn, self.binding);
        const namespace = @import("online_source_contract.zig").namespaceBytes(self.binding.fence.namespace);
        const current = try authority.require(txn, .native, namespace);
        const provenance = try Provenance.init(txn, self.binding);
        if (replay) |position| {
            try position.validate();
            if (!position.namespace.eql(self.binding.fence.namespace)) return error.IdentityNamespaceMismatch;
        }
        var changed: usize = 0;
        var maximum: u64 = 0;
        for (self.entries) |entry| {
            if (!std.mem.eql(u8, &entry.address.generation, &allocation.generation)) return error.InvalidPrivateIndexClaimInventory;
            const actual = try optional(txn, &entry.address.claimKey());
            if (actual) |value| {
                if (!std.mem.eql(u8, value, entry.value)) return error.UniqueConstraintViolation;
                const sequence = try checkInventory(txn, entry.address, entry.value, provenance);
                if (entry.expected != null and sequence != entry.sequence) return error.VersionConflict;
                maximum = @max(maximum, sequence);
            } else {
                if (entry.expected != null) return error.VersionConflict;
                _ = try checkInventory(txn, entry.address, null, provenance);
                changed += 1;
            }
        }
        if (changed == 0) {
            if (replay) |position| if (position.sequence != maximum) return error.InvalidPrivateIndexClaimReceipt;
            return .{ .position = .{ .namespace = self.binding.fence.namespace, .sequence = maximum }, .duplicate = true, .claims_added = 0 };
        }
        const sequence = std.math.add(u64, current.sequence, 1) catch return error.OnlineSourceCorrupt;
        if (replay) |position| if (position.sequence != sequence) return error.OnlineSourceScopeChanged;
        try txn.retained.bindPrivateClaims(allocation.generation, prefix);
        for (self.entries) |entry| {
            if (try optional(txn, &entry.address.claimKey()) != null) continue;
            const key = inventoryKey(entry.address);
            try txn.put(&entry.address.claimKey(), entry.value);
            try txn.put(&key, &encodeInventory(&key, provenance.binding_digest, hash(entry.value), sequence));
        }
        _ = try authority.advance(txn, namespace, sequence);
        return .{ .position = .{ .namespace = self.binding.fence.namespace, .sequence = sequence }, .duplicate = false, .claims_added = changed };
    }
};

fn optionalEqual(a: ?[]const u8, b: ?[]const u8) bool {
    return if (a) |left| if (b) |right| std.mem.eql(u8, left, right) else false else b == null;
}

fn checkInventory(txn: anytype, address: integrity.Address, expected: ?[]const u8, provenance: Provenance) !u64 {
    const actual = try optional(txn, &inventoryKey(address));
    if (expected) |value| {
        return decodeInventory(&inventoryKey(address), actual orelse return error.InvalidPrivateIndexClaimInventory, provenance, hash(value));
    } else if (actual != null) return error.InvalidPrivateIndexClaimInventory;
    return 0;
}

/// Pin once in the same source/staging transaction as a transfer page. This
/// avoids decoding the job or hashing its catalog once per claim. The view is
/// information for physical validation, never execution/publication authority.
pub const TransferView = struct {
    generation: ?integrity.Generation = null,
    provenance: ?Provenance = null,

    pub fn init(txn: anytype) !TransferView {
        const binding = (try jobs.load(txn)) orelse return .{};
        if (binding.phase != .reserved) return .{};
        return .{ .generation = (try requireRegistered(txn, binding)).generation, .provenance = if (binding.authority == .native) try Provenance.init(txn, binding) else null };
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
