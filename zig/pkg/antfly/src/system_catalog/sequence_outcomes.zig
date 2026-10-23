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

//! Bounded native DDL admission/outcomes, not another coordinator. A request
//! may cross unrelated mutations without a global revision CAS. Retained
//! outcomes replay exactly; evicted requests expire rather than execute again.
const std = @import("std");
const A = std.mem.Allocator;
const Meta = @import("antfly_local_sources").system_catalog_domain.Meta;
pub const retained: u64 = 64;

pub const Identity = struct {
    id: u64,
    generation: u64,
    pub fn validate(self: Identity) !void {
        if (self.id == 0 or self.generation == 0) return error.InvalidCatalogMutation;
    }
};
pub const Result = struct {
    revision: u64,
    identity: ?Identity,
    removed: usize,
    pub fn validate(self: Result) !void {
        if (self.identity) |value| try value.validate();
        if (self.removed > 1 or (self.removed != 0 and self.identity != null) or
            (self.revision == 0 and (self.removed != 0 or self.identity != null))) return error.InvalidSequenceOutcome;
    }
};
pub const Admission = struct {
    nonce: [16]u8,
    head: u64,
    pub fn validate(self: Admission) !void {
        if (std.mem.allEqual(u8, &self.nonce, 0)) return error.InvalidCatalogMutation;
    }
};
pub const Rejection = enum {
    CatalogGenerationChanged,
    CatalogAlreadyExists,
    SequenceNotFound,
    DatabaseNotFound,
    NamespaceNotFound,
    SqlWrongObjectType,
    InvalidSequenceDefinition,
    SequenceCatalogChanged,
    CatalogOccupancyNotReady,
    CatalogPublicationProofPending,
    SequenceCounterChanged,
    SequenceGeneratorLimitExceeded,
    SequenceValueOutOfRange,
    CatalogGenerationExhausted,
};
pub const Outcome = union(enum) {
    success: Result,
    rejected: Rejection,

    pub fn fromError(err: anyerror) ?Outcome {
        return .{ .rejected = std.meta.stringToEnum(Rejection, @errorName(err)) orelse return null };
    }
    pub fn result(self: Outcome) anyerror!Result {
        return switch (self) {
            .success => |value| value,
            .rejected => |reason| switch (reason) {
                inline else => |tag| @field(anyerror, @tagName(tag)),
            },
        };
    }
    fn validate(self: Outcome) !void {
        if (self == .success) try self.success.validate();
    }
    fn validateCatalog(self: Outcome, catalog: Meta) !void {
        try self.validate();
        if (self == .success) {
            if (self.success.revision > catalog.revision) return error.InvalidSequenceOutcome;
            if (self.success.identity) |value| if (value.id >= catalog.next_id) return error.InvalidSequenceOutcome;
        }
    }
};
pub const Decision = union(enum) { replay: Outcome, append: u64 };
const Record = struct {
    format: u8,
    ordinal: u64,
    incarnation: [16]u8,
    admission: Admission,
    digest: [32]u8,
    outcome: Outcome,

    fn validate(self: Record, incarnation: [16]u8) !void {
        try self.admission.validate();
        try self.outcome.validate();
        if (self.format != 1 or !std.mem.eql(u8, &self.incarnation, &incarnation) or
            self.ordinal == 0 or self.admission.head >= self.ordinal or
            self.ordinal - self.admission.head > retained or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidSequenceOutcome;
    }
};

pub fn prefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidSequenceOutcome;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sequence-outcomes:", .{group});
}

/// One catalog-owned head, 64 slots and 64 nonce indexes. Lookup and eviction are point
/// operations. All errors abort the caller's one metadata transaction.
pub fn Store(comptime Txn: type) type {
    return struct {
        txn: *Txn,
        group: u64,
        incarnation: [16]u8,

        fn key(self: @This(), buffer: []u8, suffix: []const u8) ![]const u8 {
            const prefix = try prefixForGroup(buffer, self.group);
            if (buffer.len - prefix.len < suffix.len) return error.NoSpaceLeft;
            @memcpy(buffer[prefix.len..][0..suffix.len], suffix);
            return buffer[0 .. prefix.len + suffix.len];
        }
        fn slotKey(self: @This(), buffer: []u8, ordinal: u64) ![]const u8 {
            const suffix = [_]u8{ 's', @intCast(ordinal % retained) };
            return self.key(buffer, &suffix);
        }
        fn nonceKey(self: @This(), buffer: []u8, nonce: [16]u8) ![]const u8 {
            var suffix: [17]u8 = undefined;
            suffix[0] = 'n';
            @memcpy(suffix[1..], &nonce);
            return self.key(buffer, &suffix);
        }
        fn optional(self: @This(), bytes: []const u8) !?[]const u8 {
            return self.txn.get(bytes) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
        }
        fn number(bytes: []const u8) !u64 {
            if (bytes.len != 8) return error.InvalidSequenceOutcome;
            return std.mem.readInt(u64, bytes[0..8], .big);
        }
        fn metaKey(self: @This(), buffer: []u8) ![]const u8 {
            if (self.group == 0) return error.InvalidSequenceOutcome;
            return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:meta", .{self.group});
        }
        fn meta(self: @This(), a: A) !Meta {
            var buffer: [192]u8 = undefined;
            const bytes = (try self.optional(try self.metaKey(&buffer))) orelse return .{};
            if (bytes.len > 4096) return error.InvalidSequenceOutcome;
            var value = try std.json.parseFromSlice(Meta, a, bytes, .{});
            defer value.deinit();
            if (value.value.version != 1 or value.value.next_id < 3 or value.value.sequence_outcome_head == 0) return error.InvalidSequenceOutcome;
            return value.value;
        }
        pub fn head(self: @This(), a: A) !u64 {
            return (try self.meta(a)).sequence_outcome_head orelse 0;
        }
        fn record(self: @This(), a: A, ordinal: u64, catalog: Meta) !Record {
            const latest = catalog.sequence_outcome_head orelse 0;
            if (ordinal == 0 or ordinal > latest or latest - ordinal >= retained) return error.InvalidSequenceOutcome;
            var buffer: [192]u8 = undefined;
            const bytes = (try self.optional(try self.slotKey(&buffer, ordinal))) orelse return error.InvalidSequenceOutcome;
            if (bytes.len > 4096) return error.InvalidSequenceOutcome;
            var parsed = std.json.parseFromSlice(Record, a, bytes, .{}) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidSequenceOutcome,
            };
            defer parsed.deinit();
            try parsed.value.validate(self.incarnation);
            try parsed.value.outcome.validateCatalog(catalog);
            if (parsed.value.ordinal != ordinal) return error.InvalidSequenceOutcome;
            return parsed.value;
        }
        pub fn lookup(self: @This(), a: A, admission: Admission, digest: [32]u8) !?Outcome {
            try admission.validate();
            var buffer: [192]u8 = undefined;
            const encoded = (try self.optional(try self.nonceKey(&buffer, admission.nonce))) orelse return null;
            const value = try self.record(a, try number(encoded), try self.meta(a));
            if (!std.mem.eql(u8, &value.admission.nonce, &admission.nonce)) return error.InvalidSequenceOutcome;
            if (value.admission.head != admission.head or !std.mem.eql(u8, &value.digest, &digest)) return error.SequenceAdmissionConflict;
            return value.outcome;
        }
        pub fn prepare(self: @This(), a: A, admission: Admission, digest: [32]u8) !Decision {
            if (std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &digest, 0)) return error.InvalidSequenceOutcome;
            if (try self.lookup(a, admission, digest)) |value| return .{ .replay = value };
            const latest = try self.head(a);
            if (admission.head > latest or latest - admission.head >= retained) return error.SequenceAdmissionExpired;
            return .{ .append = std.math.add(u64, latest, 1) catch return error.CatalogRevisionExhausted };
        }
        pub fn append(self: @This(), a: A, ordinal: u64, admission: Admission, digest: [32]u8, outcome: Outcome) !void {
            var catalog = try self.meta(a);
            const latest = catalog.sequence_outcome_head orelse 0;
            if (latest == std.math.maxInt(u64) or ordinal != latest + 1) return error.InvalidSequenceOutcome;
            const next: Record = .{ .format = 1, .ordinal = ordinal, .incarnation = self.incarnation, .admission = admission, .digest = digest, .outcome = outcome };
            try next.validate(self.incarnation);
            try outcome.validateCatalog(catalog);
            var buffer: [192]u8 = undefined;
            // prepare already proved this nonce absent in the same txn. An
            // unexpected collision here is corruption, not semantic rejection:
            // the native mutation may already have provisional writes.
            if (try self.optional(try self.nonceKey(&buffer, admission.nonce)) != null) return error.InvalidSequenceOutcome;
            const old = if (ordinal > retained) try self.record(a, ordinal - retained, catalog) else null;
            if (old) |value| {
                const index = (try self.optional(try self.nonceKey(&buffer, value.admission.nonce))) orelse return error.InvalidSequenceOutcome;
                if (try number(index) != value.ordinal) return error.InvalidSequenceOutcome;
            }
            const bytes = try std.json.Stringify.valueAlloc(a, next, .{});
            defer a.free(bytes);
            catalog.sequence_outcome_head = ordinal;
            const catalog_bytes = try std.json.Stringify.valueAlloc(a, catalog, .{});
            defer a.free(catalog_bytes);
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, ordinal, .big);
            // All structural checks precede the first provisional write.
            if (old) |value| try self.txn.delete(try self.nonceKey(&buffer, value.admission.nonce));
            try self.txn.put(try self.slotKey(&buffer, ordinal), bytes);
            try self.txn.put(try self.nonceKey(&buffer, admission.nonce), &encoded);
            try self.txn.put(try self.metaKey(&buffer), catalog_bytes);
        }
        pub fn validateCut(self: @This(), a: A) !void {
            const catalog = try self.meta(a);
            const latest = catalog.sequence_outcome_head orelse 0;
            const count = @min(latest, retained);
            var offset: u64 = 0;
            while (offset < count) : (offset += 1) {
                const value = try self.record(a, latest - offset, catalog);
                var buffer: [192]u8 = undefined;
                const index = (try self.optional(try self.nonceKey(&buffer, value.admission.nonce))) orelse return error.InvalidSequenceOutcome;
                if (try number(index) != value.ordinal) return error.InvalidSequenceOutcome;
            }
        }
        /// Pair with validateCut when validating a manifest: reject extra,
        /// malformed or orphan rows as well as missing expected rows.
        pub fn validateRow(self: @This(), a: A, row_key: []const u8, bytes: []const u8) !void {
            var buffer: [192]u8 = undefined;
            const prefix = try prefixForGroup(&buffer, self.group);
            if (!std.mem.startsWith(u8, row_key, prefix)) return error.InvalidSequenceOutcome;
            const suffix = row_key[prefix.len..];
            const catalog = try self.meta(a);
            const latest = catalog.sequence_outcome_head orelse 0;
            if (suffix.len == 2 and suffix[0] == 's' and suffix[1] < retained) {
                if (latest == 0) return error.InvalidSequenceOutcome;
                const distance = (latest % retained + retained - suffix[1]) % retained;
                if (distance >= latest) return error.InvalidSequenceOutcome;
                _ = try self.record(a, latest - distance, catalog);
            } else if (suffix.len == 17 and suffix[0] == 'n') {
                const value = try self.record(a, try number(bytes), catalog);
                if (!std.mem.eql(u8, suffix[1..], &value.admission.nonce)) return error.InvalidSequenceOutcome;
            } else return error.InvalidSequenceOutcome;
        }
    };
}

test "sequence catalog outcome requires persisted counts and rejects impossible catalog success" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.MissingField, std.json.parseFromSlice(Result, a, "{\"revision\":0,\"identity\":null}", .{}));
    const value: Outcome = .{ .success = .{ .revision = 2, .identity = .{ .id = 4, .generation = 1 }, .removed = 0 } };
    try std.testing.expectError(error.InvalidSequenceOutcome, value.validateCatalog(.{ .revision = 1, .next_id = 5 }));
    try std.testing.expectError(error.InvalidSequenceOutcome, value.validateCatalog(.{ .revision = 2, .next_id = 4 }));
    try value.validateCatalog(.{ .revision = 2, .next_id = 5 });
    try std.testing.expectError(error.InvalidSequenceOutcome, (Result{ .revision = 0, .identity = null, .removed = 1 }).validate());
    try std.testing.expectError(error.InvalidSequenceOutcome, (Result{ .revision = 0, .identity = .{ .id = 4, .generation = 1 }, .removed = 0 }).validate());
}
