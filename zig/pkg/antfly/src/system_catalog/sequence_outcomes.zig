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
// A valid 1024-byte notice may expand sixfold when JSON escapes controls.
// Include fixed command-fence identities in the same bounded record budget.
pub const max_record_bytes = 8192;

pub const Identity = struct {
    id: u64,
    generation: u64,
    pub fn validate(self: Identity) !void {
        if (self.id == 0 or self.generation == 0) return error.InvalidCatalogMutation;
    }
};
/// Owned bounded text: results outlive JSON parser arenas and native read
/// scratch. JSON keeps the existing string/null representation.
pub const Notice = struct {
    bytes: [1024]u8 = @splat(0),
    len: u16 = 0,
    pub fn init(text: []const u8) !Notice {
        if (text.len > 1024) return error.SqlProgramLimitExceeded;
        if (text.len == 0 or !std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidSequenceOutcome;
        var result: Notice = .{ .len = @intCast(text.len) };
        @memcpy(result.bytes[0..text.len], text);
        return result;
    }
    pub fn slice(self: *const Notice) []const u8 {
        return self.bytes[0..self.len];
    }
    pub fn jsonParse(allocator: A, source: anytype, options: std.json.ParseOptions) !Notice {
        return init(try std.json.innerParse([]const u8, allocator, source, options)) catch return error.UnexpectedToken;
    }
    pub fn jsonParseFromValue(_: A, source: std.json.Value, _: std.json.ParseOptions) !Notice {
        if (source != .string) return error.UnexpectedToken;
        return init(source.string) catch return error.UnexpectedToken;
    }
    pub fn jsonStringify(self: Notice, writer: anytype) !void {
        try writer.write(self.slice());
    }
    fn validate(self: *const Notice) !void {
        if (self.len == 0 or self.len > self.bytes.len or !std.mem.allEqual(u8, self.bytes[self.len..], 0)) return error.InvalidSequenceOutcome;
        const text = self.slice();
        if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidSequenceOutcome;
    }
};

pub const Result = struct {
    revision: u64,
    identity: ?Identity,
    removed: usize,
    cascade_notice: ?Notice = null,
    no_op: bool = false,
    pub fn jsonStringify(self: Result, writer: anytype) !void {
        if (self.no_op) return writer.write(.{ .revision = self.revision, .identity = self.identity, .removed = self.removed, .cascade_notice = self.cascade_notice, .no_op = true });
        // New type-only no-op results are protected by decoder capability 111.
        // Existing callable/sequence frames keep their exact field shape.
        try writer.write(.{ .revision = self.revision, .identity = self.identity, .removed = self.removed, .cascade_notice = self.cascade_notice });
    }
    pub fn validate(self: Result) !void {
        if (self.no_op and (self.removed != 0 or self.cascade_notice != null)) return error.InvalidSequenceOutcome;
        if (self.cascade_notice) |notice| {
            if (self.removed != 1) return error.InvalidSequenceOutcome;
            try notice.validate();
        }
        if (self.identity) |value| try value.validate();
        if (self.removed > 1 or (self.removed != 0 and self.identity != null) or
            (self.revision == 0 and (self.removed != 0 or self.identity != null))) return error.InvalidSequenceOutcome;
    }
};
pub const Admission = struct {
    nonce: [16]u8,
    head: u64,
    command_fence: ?@import("command_fence.zig").Fence = null,
    pub fn jsonStringify(self: Admission, writer: anytype) !void {
        if (self.command_fence) |fence| return writer.write(.{ .nonce = self.nonce, .head = self.head, .command_fence = fence });
        try writer.write(.{ .nonce = self.nonce, .head = self.head });
    }
    pub fn validate(self: Admission) !void {
        if (std.mem.allEqual(u8, &self.nonce, 0)) return error.InvalidCatalogMutation;
        if (self.command_fence) |fence| try fence.validate();
    }
};
pub const Rejection = enum {
    SqlIndexAlreadyExists,
    SqlIndexNotFound,
    InvalidCatalogMutation,
    CatalogCommandTooLarge,
    TableTransitionActive,
    TableNotFound,
    InvalidTableDefinitionReplacement,
    ExternalTransactionDecisionChanged,
    ExternalTransactionAdmissionExpired,
    InvalidExternalDecisionOwner,
    InvalidPreparedTransactionId,
    InvalidPreparedTransactionPhase,
    SqlPreparedTransactionAlreadyExists,
    SqlPreparedTransactionNotFound,
    SqlPreparedTransactionWrongDatabase,
    SqlPreparedTransactionCapacityExceeded,
    Canceled,
    SqlRoleNotFound,
    SqlDuplicateRole,
    SqlDependentRole,
    SqlDependentGrant,
    SqlRoleMembershipCycle,
    SqlReservedRoleName,
    InvalidSqlGrant,
    Forbidden,
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
    SqlDuplicateFunction,
    SqlDuplicateOperator,
    SqlOperatorNotFound,
    SqlDependentCallable,
    SqlFunctionReturnTypeChanged,
    SqlUndefinedFunction,
    SqlUndefinedProcedure,
    SqlInvalidFunctionDefinition,
    SqlInvalidCastDefinition,
    SqlDuplicateCast,
    SqlCastNotFound,
    SqlNamedTypeNotFound,
    SqlDuplicateType,
    SqlDuplicateTypeOrigin,
    SqlDuplicateEnumLabel,
    SqlInvalidEnumLabel,
    SqlEnumLabelNotFound,
    InvalidSqlNamedType,
    InvalidSqlEnumSymbol,
    InvalidSqlEnumOrder,
    SqlTypeActivationRequired,
    SqlDependentType,
    SqlDependentObjectsExist,
    SqlIdentitySequenceOwnership,
    SqlNotNullViolation,

    InvalidSqlCastRecord,
    SqlInvalidTextRepresentation,
    SqlNumericOutOfRange,
    SqlNumericFieldOverflow,
    SqlDateTimeFieldOverflow,
    SqlInvalidDateTimeFormat,
    SqlUndefinedParameter,
    SqlProgramLimitExceeded,
    InvalidSqlSyntax,
    UnsupportedSqlShape,
    UnknownColumn,
    SqlTypeMismatch,
    SqlAssignmentTypeMismatch,
    CatalogRevisionExhausted,
};
/// Compact reference to the existing all-owner schema job. The catalog
/// journal never duplicates table schemas or owner arrays into retained slots.
pub const RestoreWork = struct {
    group: u64,
    id: [16]u8,
    intent_digest: [32]u8,
    incarnation: [16]u8,
    identity: Identity,
    pub fn validate(self: @This()) !void {
        try self.identity.validate();
        if (self.group == 0 or std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.intent_digest, 0) or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSequenceOutcome;
    }
};
pub const Pending = struct {
    revision: u64,
    identity: Identity,
    work: union(enum) {
        schema: @import("../metadata/schema_validation_read.zig").Lookup,
        sequence_schema: @import("../metadata/schema_validation_read.zig").Lookup,
        restore: RestoreWork,
    },
    pub fn validate(self: @This()) !void {
        try self.identity.validate();
        switch (self.work) {
            .schema, .sequence_schema => |lookup| try (@import("../metadata/schema_validation_read.zig").OwnerReadRequest{ .lookup = lookup, .ordinal = 0 }).validate(),
            .restore => |lookup| {
                try lookup.validate();
                if (!std.meta.eql(self.identity, lookup.identity)) return error.InvalidSequenceOutcome;
            },
        }
    }
    pub fn matchesIncarnation(self: @This(), incarnation: [16]u8) bool {
        return switch (self.work) {
            .schema, .sequence_schema => |lookup| std.mem.eql(u8, &lookup.metadata_incarnation, &std.fmt.bytesToHex(incarnation, .lower)),
            .restore => |lookup| std.mem.eql(u8, &lookup.incarnation, &incarnation),
        };
    }
};
pub const Outcome = union(enum) {
    pending: Pending,
    success: Result,
    rejected: Rejection,

    pub fn fromError(err: anyerror) ?Outcome {
        return .{ .rejected = std.meta.stringToEnum(Rejection, @errorName(err)) orelse return null };
    }
    pub fn result(self: Outcome) anyerror!Result {
        return switch (self) {
            .success => |value| value,
            .pending => error.CatalogPublicationProofPending,
            .rejected => |reason| switch (reason) {
                inline else => |tag| @field(anyerror, @tagName(tag)),
            },
        };
    }
    fn validate(self: Outcome) !void {
        if (self == .success) try self.success.validate();
        if (self == .pending) try self.pending.validate();
    }
    fn validateCatalog(self: Outcome, catalog: Meta) !void {
        try self.validate();
        if (self == .pending and (self.pending.revision > catalog.revision or self.pending.identity.id >= catalog.next_id)) return error.InvalidSequenceOutcome;
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
        if (self.admission.command_fence) |fence| if (!std.mem.eql(u8, &fence.backend.incarnation, &incarnation)) return error.InvalidSequenceOutcome;
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
            if (value.value.version != 1 or value.value.next_id < 3) return error.InvalidSequenceOutcome;
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
            if (bytes.len > max_record_bytes) return error.InvalidSequenceOutcome;
            var parsed = std.json.parseFromSlice(Record, a, bytes, .{}) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidSequenceOutcome,
            };
            defer parsed.deinit();
            try parsed.value.validate(self.incarnation);
            try parsed.value.outcome.validateCatalog(catalog);
            if (parsed.value.ordinal != ordinal) return error.InvalidSequenceOutcome;
            if (parsed.value.outcome == .pending) {
                const pending = parsed.value.outcome.pending;
                if (!pending.matchesIncarnation(self.incarnation)) return error.InvalidSequenceOutcome;
                if (pending.work == .restore) {
                    parsed.value.outcome = (try self.restoreOutcomeAtCatalog(a, pending.work.restore, parsed.value.digest, catalog)) orelse return error.InvalidSequenceOutcome;
                    try parsed.value.outcome.validateCatalog(catalog);
                    return parsed.value;
                }
                const retiring = pending.work == .sequence_schema;
                const schema_lookup = if (retiring) pending.work.sequence_schema else pending.work.schema;
                if (schema_lookup.group != self.group) return error.InvalidSequenceOutcome;
                const job_head = try @import("../metadata/schema_validation_ledger.zig").readHead(self.txn, schema_lookup);
                if (retiring) {
                    const retired = job_head.sequence_retirement orelse return error.InvalidSequenceOutcome;
                    if (retired.id != pending.identity.id or retired.generation != pending.identity.generation or
                        !std.mem.eql(u8, &retired.admission_digest, &parsed.value.digest)) return error.InvalidSequenceOutcome;
                } else if (job_head.type_id != pending.identity.id or job_head.type_generation != pending.identity.generation or
                    !std.mem.eql(u8, &job_head.type_admission_digest, &parsed.value.digest)) return error.InvalidSequenceOutcome;
                // Project a completed job at this same read cut. The retained
                // slot stays immutable and owns only its compact reference;
                // callers need neither another head read nor the full plan.
                parsed.value.outcome = switch (job_head.phase) {
                    .succeeded => .{ .success = .{ .revision = job_head.catalog_revision, .identity = if (retiring) null else pending.identity, .removed = if (retiring) 1 else 0 } },
                    .canceled => .{ .rejected = switch (job_head.failure) {
                        .requested => .Canceled,
                        .owner_failed => .CatalogGenerationChanged,
                        .not_null_violation => .SqlNotNullViolation,
                        else => return error.InvalidSequenceOutcome,
                    } },
                    else => parsed.value.outcome,
                };
                try parsed.value.outcome.validateCatalog(catalog);
            }
            if (parsed.value.ordinal != ordinal) return error.InvalidSequenceOutcome;
            return parsed.value;
        }
        pub fn restoreOutcome(self: @This(), a: A, work_scope: RestoreWork, digest: [32]u8) !?Outcome {
            return self.restoreOutcomeAtCatalog(a, work_scope, digest, try self.meta(a));
        }
        fn restoreOutcomeAtCatalog(self: @This(), a: A, work_scope: RestoreWork, digest: [32]u8, catalog: Meta) !?Outcome {
            try work_scope.validate();
            if (work_scope.group != self.group or !std.mem.eql(u8, &work_scope.incarnation, &self.incarnation)) return error.InvalidSequenceOutcome;
            const staging = @import("../metadata/restore_staging.zig");
            var buffer: [256]u8 = undefined;
            const encoded = (try self.optional(try staging.progressKey(&buffer, self.group, work_scope.id))) orelse return null;
            if (encoded.len > 16 * 1024) return error.InvalidSequenceOutcome;
            var progress = try std.json.parseFromSlice(staging.Progress, a, encoded, .{});
            defer progress.deinit();
            const proof = progress.value.type_retirement orelse return null;
            if (progress.value.revision == 0 or !std.meta.eql(proof.work, work_scope) or !std.mem.eql(u8, &proof.admission_digest, &digest)) return error.InvalidSequenceOutcome;
            if ((progress.value.state == .published) != (proof.catalog_revision != 0)) return error.InvalidSequenceOutcome;
            if (proof.observed_revision > catalog.revision or (proof.catalog_revision != 0 and proof.catalog_revision < proof.observed_revision)) return error.InvalidSequenceOutcome;
            const outcome: Outcome = switch (progress.value.state) {
                .published => .{ .success = .{ .revision = proof.catalog_revision, .identity = null, .removed = 1 } },
                .canceled => .{ .rejected = proof.failure orelse .Canceled },
                else => if (proof.failure != null) return error.InvalidSequenceOutcome else .{ .pending = .{ .revision = proof.observed_revision, .identity = work_scope.identity, .work = .{ .restore = work_scope } } },
            };
            try outcome.validateCatalog(catalog);
            return outcome;
        }
        pub fn lookup(self: @This(), a: A, admission: Admission, digest: [32]u8) !?Outcome {
            try admission.validate();
            var buffer: [192]u8 = undefined;
            const encoded = (try self.optional(try self.nonceKey(&buffer, admission.nonce))) orelse {
                if (admission.command_fence) |fence| {
                    const latest = try self.head(a);
                    // Outside the bounded admission window, absence cannot
                    // prove non-execution: preserve the existing unknown result.
                    if (admission.head > latest or latest - admission.head >= retained) return null;
                    @import("command_fence.zig").check(a, self.txn, self.group, self.incarnation, fence) catch |err| return switch (err) {
                        // These fences are irreversible. The nonce is absent in
                        // the same native cut, so this proves definite nonexecution.
                        error.CatalogGenerationChanged, error.Forbidden => Outcome.fromError(err),
                        else => err,
                    };
                }
                return null;
            };
            // Native read scratch may be reused by the next point lookup.
            const ordinal = try number(encoded);
            const catalog = try self.meta(a);
            const value = try self.record(a, ordinal, catalog);
            if (!std.mem.eql(u8, &value.admission.nonce, &admission.nonce)) return error.InvalidSequenceOutcome;
            if (!std.meta.eql(value.admission, admission) or !std.mem.eql(u8, &value.digest, &digest)) return error.SequenceAdmissionConflict;
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
            if (bytes.len > max_record_bytes) return error.InvalidSequenceOutcome;
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

test "sequence catalog outcome retains compact pending type jobs without reporting committed success" {
    const pending: Pending = .{ .revision = 1, .identity = .{ .id = 17, .generation = 2 }, .work = .{ .schema = .{ .group = 1, .table = 7, .id = @splat(1), .metadata_incarnation = @splat('1'), .plan_digest = @splat(2) } } };
    const value: Outcome = .{ .pending = pending };
    try value.validateCatalog(.{ .revision = 1, .next_id = 18 });
    try std.testing.expectError(error.CatalogPublicationProofPending, value.result());
    try std.testing.expectError(error.InvalidSequenceOutcome, value.validateCatalog(.{ .revision = 0, .next_id = 18 }));
    try std.testing.expectError(error.InvalidSequenceOutcome, value.validateCatalog(.{ .revision = 1, .next_id = 17 }));
    const a = std.testing.allocator;
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    try std.testing.expect(bytes.len < 1024);
    var parsed = try std.json.parseFromSlice(Outcome, a, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqualDeep(value, parsed.value);
}

test "SQL routine catalog pending type outcomes reject cross job substitution and retain exact cold completion" {
    try pendingTypeOutcome(false);
}

test "schema validation publication domain NOT NULL rejection remains observable through the original cold type outcome" {
    try pendingTypeOutcome(true);
}

fn pendingTypeOutcome(rejected: bool) !void {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const DocStore = local.storage_docstore.DocStore;
    const ledger = @import("../metadata/schema_validation_ledger.zig");
    const Head = @import("../metadata/schema_validation_publication.zig").Head;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/pending-type", .{temporary.sub_path}, 0);
    defer a.free(path);
    const group = 1;
    const incarnation: [16]u8 = @splat(0x11);
    const admission: Admission = .{ .nonce = @splat(3), .head = 0 };
    const digest: [32]u8 = @splat(4);
    // Shape fixtures only; a journal reference never grants owner execution.
    var head: Head = .{ .metadata_group_id = group, .metadata_incarnation = @splat('1'), .table_id = 7, .id = @splat(1), .plan_digest = @splat(5), .owner_count = 1, .type_id = 17, .type_generation = 2, .type_admission_digest = digest, .participants_digest = @splat(8) };
    const pending: Pending = .{ .revision = 1, .identity = .{ .id = 17, .generation = 2 }, .work = .{ .schema = .fromHead(head) } };
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const journal: Store(DocStore.Txn) = .{ .txn = &txn, .group = group, .incarnation = incarnation };
        var buffer: [192]u8 = undefined;
        const meta = try std.json.Stringify.valueAlloc(a, Meta{ .revision = 1, .next_id = 18 }, .{});
        defer a.free(meta);
        try txn.put(try journal.metaKey(&buffer), meta);
        const encoded = try head.encode();
        try txn.put(try (ledger.Key{ .group = group, .table = head.table_id, .id = head.id, .kind = .head }).encode(&buffer), encoded.slice());
        try journal.append(a, 1, admission, digest, .{ .pending = pending });
        try txn.commit();
    }
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const journal: Store(DocStore.Txn) = .{ .txn = &txn, .group = group, .incarnation = incarnation };
        try std.testing.expectEqualDeep(Outcome{ .pending = pending }, (try journal.lookup(a, admission, digest)).?);
        var other = head;
        other.id = @splat(2);
        other.plan_digest = @splat(6);
        other.type_admission_digest = @splat(7);
        var buffer: [192]u8 = undefined;
        const other_head = try other.encode();
        try txn.put(try (ledger.Key{ .group = group, .table = other.table_id, .id = other.id, .kind = .head }).encode(&buffer), other_head.slice());
        var forged = pending;
        forged.work = .{ .schema = .fromHead(other) };
        const swapped = try std.json.Stringify.valueAlloc(a, Record{ .format = 1, .ordinal = 1, .incarnation = incarnation, .admission = admission, .digest = digest, .outcome = .{ .pending = forged } }, .{});
        defer a.free(swapped);
        try txn.put(try journal.slotKey(&buffer, 1), swapped);
        try std.testing.expectError(error.InvalidSequenceOutcome, journal.lookup(a, admission, digest));
        try std.testing.expectError(error.InvalidSequenceOutcome, journal.validateCut(a));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const journal: Store(DocStore.Txn) = .{ .txn = &txn, .group = group, .incarnation = incarnation };
        var buffer: [192]u8 = undefined;
        var meta = try journal.meta(a);
        meta.revision = 2;
        const bytes = try std.json.Stringify.valueAlloc(a, meta, .{});
        defer a.free(bytes);
        try txn.put(try journal.metaKey(&buffer), bytes);
        if (rejected) {
            head.phase = .canceled;
            head.revision = 4;
            head.canceled = 1;
            head.cancellation_revision = 2;
            head.failure = .not_null_violation;
            head.failure_digest = @splat(14);
            head.terminal_chain = @splat(13);
        } else {
            head.phase = .succeeded;
            head.revision = 6;
            head.ready = 1;
            head.installed = 1;
            head.released = 1;
            head.install_revision = 3;
            head.publication_revision = 5;
            head.catalog_revision = 2;
            head.ready_chain = @splat(11);
            head.installed_chain = @splat(12);
            head.terminal_chain = @splat(13);
        }
        const encoded = try head.encode();
        try txn.put(try (ledger.Key{ .group = group, .table = head.table_id, .id = head.id, .kind = .head }).encode(&buffer), encoded.slice());
        const outcome = (try journal.lookup(a, admission, digest)).?;
        if (rejected) {
            try std.testing.expectEqual(Rejection.SqlNotNullViolation, outcome.rejected);
            try std.testing.expectError(error.SqlNotNullViolation, outcome.result());
        } else {
            try std.testing.expectEqual(@as(u64, 2), outcome.success.revision);
            try std.testing.expectEqualDeep(pending.identity, outcome.success.identity.?);
        }
        try journal.validateCut(a);
        try txn.commit();
    }
}

test "SQL routine catalog type retirement projects exact cold restore progress and rejects substituted jobs" {
    const a = std.testing.allocator;
    const DocStore = @import("antfly_local_sources").storage_docstore.DocStore;
    const staging = @import("../metadata/restore_staging.zig");
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/retiring-type", .{directory.sub_path}, 0);
    defer a.free(path);
    const work: RestoreWork = .{ .group = 1, .id = @splat(3), .incarnation = @splat(7), .intent_digest = @splat(5), .identity = .{ .id = 17, .generation = 2 } };
    const admission: Admission = .{ .nonce = @splat(9), .head = 0 };
    const digest: [32]u8 = @splat(8);
    const pending: Pending = .{ .revision = 1, .identity = work.identity, .work = .{ .restore = work } };
    var progress: staging.Progress = .{ .state = .preparing_sources, .type_retirement = .{ .work = work, .admission_digest = digest, .observed_revision = 1 } };
    // Protocol-shaped journal fixtures only. Physical restore authority and
    // type/schema publication are independently checked by the real writer.
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const journal: Store(DocStore.Txn) = .{ .txn = &txn, .group = work.group, .incarnation = work.incarnation };
        var buffer: [256]u8 = undefined;
        const meta = try std.json.Stringify.valueAlloc(a, Meta{ .revision = 1, .next_id = 18 }, .{});
        defer a.free(meta);
        try txn.put(try journal.metaKey(&buffer), meta);
        const bytes = try std.json.Stringify.valueAlloc(a, progress, .{});
        defer a.free(bytes);
        try txn.put(try staging.progressKey(&buffer, work.group, work.id), bytes);
        try journal.append(a, 1, admission, digest, .{ .pending = pending });
        try txn.commit();
    }
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const journal: Store(DocStore.Txn) = .{ .txn = &txn, .group = work.group, .incarnation = work.incarnation };
        try std.testing.expectEqualDeep(Outcome{ .pending = pending }, (try journal.lookup(a, admission, digest)).?);
        var swapped = work;
        swapped.intent_digest[0] ^= 1;
        try std.testing.expectError(error.InvalidSequenceOutcome, journal.restoreOutcome(a, swapped, digest));
        try std.testing.expectError(error.InvalidSequenceOutcome, journal.restoreOutcome(a, work, @splat(4)));
        var buffer: [256]u8 = undefined;
        progress.state = .published;
        progress.type_retirement.?.catalog_revision = 2;
        const meta = try std.json.Stringify.valueAlloc(a, Meta{ .revision = 3, .next_id = 18, .sequence_outcome_head = 1 }, .{});
        defer a.free(meta);
        try txn.put(try journal.metaKey(&buffer), meta);
        const bytes = try std.json.Stringify.valueAlloc(a, progress, .{});
        defer a.free(bytes);
        try txn.put(try staging.progressKey(&buffer, work.group, work.id), bytes);
        try std.testing.expectEqualDeep(Outcome{ .success = .{ .revision = 2, .identity = null, .removed = 1 } }, (try journal.lookup(a, admission, digest)).?);
        progress.state = .canceled;
        progress.type_retirement.?.catalog_revision = 0;
        progress.type_retirement.?.failure = .CatalogGenerationChanged;
        const canceled = try std.json.Stringify.valueAlloc(a, progress, .{});
        defer a.free(canceled);
        try txn.put(try staging.progressKey(&buffer, work.group, work.id), canceled);
        try std.testing.expectEqualDeep(Outcome{ .rejected = .CatalogGenerationChanged }, (try journal.lookup(a, admission, digest)).?);
    }
}
