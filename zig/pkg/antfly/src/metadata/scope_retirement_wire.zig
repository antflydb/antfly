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

//! Body-bound native scope reservation commands and exact observations.
//! Reservation does not certify physical retirement or final DROP publication.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.system_catalog_domain.scope_retirement;
const ledger = @import("scope_retirement_ledger.zig");
const records = @import("schema_validation_publication.zig");
const A = std.mem.Allocator;
pub const max_command_bytes = contract.max_manifest_bytes + 8192;
pub const ProgressRequest = struct { lookup: ledger.Lookup, expected_revision: u64 };
pub const Request = struct {
    admission: @import("../system_catalog/sequence_outcomes.zig").Admission,
    operation: union(enum) {
        reserve: contract.Manifest,
        cancel: ProgressRequest,
        drain: ProgressRequest,
        publish: ProgressRequest,
        child_ready: struct { lookup: ledger.Lookup, expected_revision: u64, ordinal: u16 },
    },
    pub fn validate(self: Request, a: A) !void {
        try self.admission.validate();
        switch (self.operation) {
            .reserve => |manifest| {
                try manifest.validate(a);
                if (self.admission.head != manifest.catalog_revision) return error.CatalogGenerationChanged;
            },
            inline .cancel, .drain, .child_ready, .publish => |value| {
                try value.lookup.validate();
                if (value.expected_revision == 0) return error.InvalidScopeRetirement;
            },
        }
    }
    pub fn encodeAlloc(self: Request, a: A) ![]u8 {
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(self, .{}, &count.writer);
        if (count.fullCount() > max_command_bytes) return error.CatalogCommandTooLarge;
        return std.json.Stringify.valueAlloc(a, self, .{});
    }
};
pub const Query = struct {
    lookup: ledger.Lookup,
    nonce: [16]u8,
    digest: [32]u8,
    pub fn validate(self: Query) !void {
        try self.lookup.validate();
        if (std.mem.allEqual(u8, &self.nonce, 0) or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidScopeRetirement;
    }
};
pub const QueryRecord = records.Record(Query, "SRTL", 2048);
pub const Input = struct {
    arena: std.heap.ArenaAllocator,
    request: Request,
    query: Query,
    manifest_json: ?[]const u8,
    pub fn parse(backing: A, group: u64, bytes: []const u8) !Input {
        if (bytes.len == 0 or bytes.len > max_command_bytes) return error.CatalogCommandTooLarge;
        var arena: std.heap.ArenaAllocator = .init(backing);
        errdefer arena.deinit();
        const a = arena.allocator();
        const request = try std.json.parseFromSliceLeaky(Request, a, bytes, .{ .allocate = .alloc_always });
        try request.validate(a);
        const lookup: ledger.Lookup = switch (request.operation) {
            .reserve => |manifest| blk: {
                if (manifest.metadata_group_id != group) return error.CatalogGenerationChanged;
                break :blk .{ .group = group, .reference = try manifest.reference(a, request.admission.nonce) };
            },
            inline .cancel, .drain, .child_ready, .publish => |value| value.lookup,
        };
        if (lookup.group != group) return error.CatalogGenerationChanged;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes, &digest, .{});
        const query: Query = .{ .lookup = lookup, .nonce = request.admission.nonce, .digest = digest };
        try query.validate();
        const manifest_json = if (request.operation == .reserve) try std.json.Stringify.valueAlloc(a, request.operation.reserve, .{}) else null;
        return .{ .arena = arena, .request = request, .query = query, .manifest_json = manifest_json };
    }
    pub fn deinit(self: *Input) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Rejection = enum {
    CatalogGenerationChanged,
    MetadataIncarnationMismatch,
    CatalogPublicationProofPending,
    CatalogOccupancyNotReady,
    TableTransitionActive,
    ProtectedCatalogResource,
    ExtensionOwnedObject,
    DatabaseNotFound,
    NamespaceNotFound,
    ScopeRetirementChanged,
    SqlDatabaseInUse,
    Forbidden,
    pub fn fromError(err: anyerror) ?Rejection {
        inline for (comptime std.meta.tags(Rejection).*) |tag| {
            if (std.mem.eql(u8, @errorName(err), @tagName(tag))) return tag;
        }
        return null;
    }
    pub fn toError(self: Rejection) anyerror {
        return switch (self) {
            inline else => |tag| @field(anyerror, @tagName(tag)),
        };
    }
};
pub const Outcome = struct {
    query: Query,
    result: union(enum) { accepted: ledger.Head, rejected: Rejection },
    pub fn validate(self: Outcome) !void {
        try self.query.validate();
        if (self.result == .accepted) {
            try self.result.accepted.validate();
            if (!std.meta.eql(self.query.lookup, self.result.accepted.lookup)) return error.InvalidScopeRetirement;
        }
    }
};
pub const OutcomeRecord = records.Record(Outcome, "SROT", 4096);
pub const Observation = struct {
    outcome: ?Outcome,
    current: ?ledger.Head,
    pub fn requireFor(self: Observation, query: Query) !void {
        try query.validate();
        if (self.outcome) |outcome| {
            try outcome.validate();
            if (!std.meta.eql(outcome.query, query)) return error.SequenceAdmissionConflict;
        }
        if (self.current) |head| {
            try head.validate();
            if (!std.meta.eql(head.lookup, query.lookup)) return error.CatalogGenerationChanged;
        }
        if (self.outcome) |outcome| if (outcome.result == .accepted) {
            const current = self.current orelse return error.InvalidScopeRetirement;
            const accepted = outcome.result.accepted;
            if (current.sequence != accepted.sequence or !std.mem.eql(u8, &current.metadata_incarnation, &accepted.metadata_incarnation) or
                !current.creator.eql(accepted.creator) or !std.mem.eql(u8, &current.target_digest, &accepted.target_digest) or current.force != accepted.force or current.progress.revision < accepted.progress.revision) return error.InvalidScopeRetirement;
            if (!accepted.active() and !std.meta.eql(current, accepted)) return error.InvalidScopeRetirement;
        };
    }
};
pub fn outcomeKey(buffer: []u8, query: Query) ![]const u8 {
    try query.validate();
    return outcomeNonceKey(buffer, query.lookup.group, query.nonce);
}
pub fn outcomeNonceKey(buffer: []u8, group: u64, nonce: [16]u8) ![]const u8 {
    if (std.mem.allEqual(u8, &nonce, 0)) return error.InvalidScopeRetirement;
    var prefix: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}outcome:{s}", .{ try ledger.prefix(&prefix, group), std.fmt.bytesToHex(nonce, .lower) });
}
pub fn observation(reader: anytype, query: Query) !Observation {
    var buffer: [320]u8 = undefined;
    const bytes = reader.get(try outcomeKey(&buffer, query)) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const outcome = if (bytes) |value| try OutcomeRecord.decode(value) else null;
    if (outcome) |value| {
        try value.validate();
        if (!std.meta.eql(value.query, query)) return error.SequenceAdmissionConflict;
    }
    const result: Observation = .{ .outcome = outcome, .current = try ledger.head(reader, query.lookup) };
    try result.requireFor(query);
    return result;
}
pub fn stageOutcome(txn: anytype, value: Outcome) !void {
    try value.validate();
    const encoded = try OutcomeRecord.init(value);
    var buffer: [320]u8 = undefined;
    if (value.result == .accepted and value.result.accepted.progress.revision == 2 and
        (value.result.accepted.progress.phase == .retiring_tables or value.result.accepted.progress.phase == .publishing_catalog))
    {
        if (std.mem.eql(u8, &value.query.nonce, &value.query.lookup.reference.id)) return error.InvalidScopeRetirement;
        const link = try QueryRecord.init(value.query);
        try txn.put(try ledger.key(&buffer, value.query.lookup, .drain_outcome), link.slice());
    }
    if (value.result == .accepted and value.result.accepted.progress.completed_tables != 0 and
        (value.result.accepted.progress.phase == .retiring_tables or value.result.accepted.progress.phase == .publishing_catalog))
    {
        const link = try QueryRecord.init(value.query);
        try txn.put(try childOutcomeKey(&buffer, value.query.lookup, value.result.accepted.progress.completed_tables - 1), link.slice());
    }
    if (value.result == .accepted and (value.result.accepted.progress.phase == .canceled or value.result.accepted.progress.phase == .published)) {
        if (std.mem.eql(u8, &value.query.nonce, &value.query.lookup.reference.id)) return error.InvalidScopeRetirement;
        const link = try QueryRecord.init(value.query);
        try txn.put(try ledger.key(&buffer, value.query.lookup, .terminal_outcome), link.slice());
    }
    try txn.put(try outcomeKey(&buffer, value.query), encoded.slice());
}

pub fn childOutcomeKey(buffer: []u8, lookup: ledger.Lookup, ordinal: u16) ![]const u8 {
    try lookup.validate();
    if (ordinal >= lookup.reference.tables) return error.InvalidScopeRetirement;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{s}:{x:0>16}:{s}:child_outcome:{x:0>4}", .{ try ledger.prefix(&base, lookup.group), @tagName(lookup.reference.kind), lookup.reference.root, std.fmt.bytesToHex(lookup.reference.id, .lower), ordinal });
}
pub fn childQuery(reader: anytype, lookup: ledger.Lookup, ordinal: u16) !Query {
    var buffer: [320]u8 = undefined;
    const query = try QueryRecord.decode(try reader.get(try childOutcomeKey(&buffer, lookup, ordinal)));
    try query.validate();
    if (!std.meta.eql(query.lookup, lookup) or std.mem.eql(u8, &query.nonce, &lookup.reference.id)) return error.InvalidScopeRetirement;
    return query;
}

/// A terminal head names exactly one retained command, independent of how many
/// other canceled jobs exist. Counts alone cannot prove per-job receipt custody.
pub fn terminalQuery(reader: anytype, lookup: ledger.Lookup) !Query {
    var buffer: [320]u8 = undefined;
    const result = try QueryRecord.decode(try reader.get(try ledger.key(&buffer, lookup, .terminal_outcome)));
    try result.validate();
    if (!std.meta.eql(result.lookup, lookup) or std.mem.eql(u8, &result.nonce, &lookup.reference.id)) return error.InvalidScopeRetirement;
    return result;
}

/// Root-bound accepting-writer receipt, distinct from admission/cancellation.
/// Its exact accepted postimage proves drainage; a phase alone grants nothing.
pub fn drainQuery(reader: anytype, lookup: ledger.Lookup) !Query {
    var buffer: [320]u8 = undefined;
    const result = try QueryRecord.decode(try reader.get(try ledger.key(&buffer, lookup, .drain_outcome)));
    try result.validate();
    if (!std.meta.eql(result.lookup, lookup) or std.mem.eql(u8, &result.nonce, &lookup.reference.id)) return error.InvalidScopeRetirement;
    return result;
}

/// Only after transport has proved that no proposal occurred. Matching an
/// error-looking body on an unknown/committed delivery never authorizes retry.
pub fn definiteHttpFailure(status: u16, body: []const u8) ?anyerror {
    inline for (comptime std.meta.tags(Rejection).*) |tag| {
        const err = tag.toError();
        if (status == local.system_catalog_domain.httpStatus(err) and std.mem.eql(u8, body, @tagName(tag))) return err;
    }
    inline for (.{ error.InvalidScopeRetirement, error.InvalidCatalogMutation, error.SequenceAdmissionConflict, error.CatalogCommandTooLarge }) |err| {
        if (status == local.system_catalog_domain.httpStatus(err) and std.mem.eql(u8, body, @errorName(err))) return err;
    }
    return null;
}
