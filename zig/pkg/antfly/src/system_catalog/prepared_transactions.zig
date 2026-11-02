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

//! Metadata-owned external transaction identities and monotone phase changes.
//! Native votes and immutable postimages remain in the existing data journals.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
pub const Principal = local.system_catalog_privileges.Principal;
pub const Owner = local.storage_transactions.ExternalDecisionOwner;
pub const max_participants = 64;
pub const max_plan_bytes = 8 * 1024 * 1024;
pub const max_named = local.storage_transactions.max_external_prepared_transactions;
pub const max_session_commits = 128;
pub const max_active = max_named + max_session_commits;
pub const Kind = enum { named, session_commit };
pub const max_bytes = 256 * 1024;
pub const outcomes = @import("sequence_outcomes.zig");
pub const Actor = @import("authorization.zig").Actor;

pub fn validateGid(gid: []const u8) !void {
    if (gid.len >= 200 or !std.unicode.utf8ValidateSlice(gid) or std.mem.indexOfScalar(u8, gid, 0) != null) return error.InvalidPreparedTransactionId;
}

pub const Participant = struct {
    table_name: []const u8,
    route: local.metadata_catalog_route_contract.CatalogRouteFence,
    schema_version: ?u32 = null,

    pub fn validate(self: Participant, incarnation: [32]u8) !void {
        try self.route.validate();
        if (self.table_name.len == 0 or self.table_name.len > 1024 or !std.unicode.utf8ValidateSlice(self.table_name) or std.mem.indexOfScalar(u8, self.table_name, 0) != null or
            self.route.table_id == 0 or self.route.route.group_id == 0 or self.route.topology_epoch == 0 or self.route.metadata_group_id == 0 or
            self.route.route.identity_namespace.table_id != self.route.table_id or
            self.route.metadata_incarnation == null or !std.mem.eql(u8, &self.route.metadata_incarnation.?, &incarnation) or
            (self.schema_version != null and self.schema_version.? == 0)) return error.InvalidCatalogMutation;
    }
};

pub const SessionRef = struct {
    owner_node_id: u64,
    connection_id: ?[32]u8 = null,
    /// The local sentinel is meaningful only in the standalone catalog realm.
    /// The containing identity independently binds its catalog incarnation.
    standalone: bool = false,
    pub fn jsonStringify(self: SessionRef, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(SessionRef).@"struct".field_names) |field| {
            if (!(comptime std.mem.eql(u8, field, "standalone")) or self.standalone) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: SessionRef) !void {
        if ((self.owner_node_id == 0) != self.standalone) return error.InvalidCatalogMutation;
        if (self.connection_id) |id| {
            for (id) |byte| if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.InvalidCatalogMutation;
            if (std.mem.allEqual(u8, &id, '0')) return error.InvalidCatalogMutation;
        }
    }
    pub fn requireRealm(self: SessionRef, standalone: bool) !void {
        try self.validate();
        if (self.standalone != standalone) return error.CatalogGenerationChanged;
    }
};

pub const Identity = struct {
    kind: Kind = .named,
    commit_sync_level: local.storage_db_types.SyncLevel = .write,
    session: ?SessionRef = null,
    gid: []const u8,
    database_id: u64,
    native_service: ?local.system_catalog_domain.ServiceOwner = null,
    principal: Principal,
    txn_id: [16]u8,
    operation_id: [16]u8,
    metadata_incarnation: [32]u8,
    begin_timestamp: u64,
    expires_at_unix_ns: u64,
    plan_digest: [32]u8,
    manifest_digest: [32]u8,

    pub fn requiresConnectionDecoder(self: Identity) bool {
        return self.kind == .session_commit or (self.session != null and self.session.?.standalone);
    }

    pub fn validate(self: Identity) !void {
        if (self.session) |session| try session.validate();
        try validateGid(self.gid);
        if (self.kind == .session_commit and (self.gid.len != 0 or self.session == null or self.session.?.connection_id == null)) return error.InvalidCatalogMutation;
        if (self.kind == .session_commit and self.commit_sync_level == .propose) return error.InvalidCatalogMutation;
        if (self.kind == .named and self.commit_sync_level != .write) return error.InvalidCatalogMutation;
        try self.principal.validate();
        if (self.principal.kind == .public or self.database_id == 0 or self.begin_timestamp == 0 or
            std.mem.allEqual(u8, &self.txn_id, 0) or std.mem.allEqual(u8, &self.operation_id, 0) or
            std.mem.allEqual(u8, &self.plan_digest, 0) or std.mem.allEqual(u8, &self.manifest_digest, 0)) return error.InvalidCatalogMutation;
        try (Owner{ .metadata_incarnation = self.metadata_incarnation, .operation_id = self.operation_id, .admission_digest = @splat(1), .expires_at_unix_ns = self.expires_at_unix_ns }).validate();
    }

    pub fn jsonStringify(self: Identity, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Identity).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "kind") or std.mem.eql(u8, field, "commit_sync_level")) {
                if (self.kind == .session_commit) {
                    try writer.objectField(field);
                    try writer.write(@field(self, field));
                }
            } else {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn owner(self: Identity, a: A) !Owner {
        try self.validate();
        return .{ .metadata_incarnation = self.metadata_incarnation, .operation_id = self.operation_id, .admission_digest = try digest(a, "antfly:sql-prepared-admission:v1\x00", self), .expires_at_unix_ns = self.expires_at_unix_ns };
    }
};

fn digest(a: A, domain: []const u8, value: anytype) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(domain);
    hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn planDigest(bytes: []const u8) ![32]u8 {
    if (bytes.len == 0 or bytes.len > max_plan_bytes or !std.unicode.utf8ValidateSlice(bytes)) return error.CatalogCommandTooLarge;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:sql-prepared-execution-plan:v1\x00");
    hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn manifestDigest(a: A, manifest: []const Participant, incarnation: [32]u8) ![32]u8 {
    if (manifest.len > max_participants) return error.CatalogCommandTooLarge;
    for (manifest, 0..) |participant, index| {
        try participant.validate(incarnation);
        for (manifest[0..index]) |previous| if (previous.route.route.group_id == participant.route.route.group_id) return error.InvalidCatalogMutation;
    }
    return digest(a, "antfly:sql-prepared-participants:v1\x00", manifest);
}

pub const Phase = enum { preparing, prepared, decided, active, resolved, retired };
pub const DecisionInput = struct { version: u64, sync_level: local.storage_db_types.SyncLevel = .write };
pub const Decision = struct { status: enum { committed, aborted }, version: u64, sync_level: local.storage_db_types.SyncLevel = .write };
pub const Record = struct {
    identity: Identity,
    revision: u64 = 1,
    phase: Phase = .preparing,
    was_prepared: bool = false,
    participants: []const Participant,
    votes: u64 = 0,
    activations: u64 = 0,
    resolutions: u64 = 0,
    decision: ?Decision = null,

    pub fn mask(self: Record) u64 {
        return if (self.participants.len == 64) std.math.maxInt(u64) else (@as(u64, 1) << @as(u6, @intCast(self.participants.len))) - 1;
    }

    pub fn validate(self: Record, a: A) !void {
        try self.identity.validate();
        if (self.revision == 0 or !std.mem.eql(u8, &try manifestDigest(a, self.participants, self.identity.metadata_incarnation), &self.identity.manifest_digest)) return error.InvalidCatalogRecord;
        const all = self.mask();
        if ((self.votes | self.activations | self.resolutions) & ~all != 0) return error.InvalidCatalogRecord;
        switch (self.phase) {
            .preparing, .prepared => {
                if (self.decision != null or self.activations != 0 or self.resolutions != 0) return error.InvalidCatalogRecord;
                if (self.phase == .prepared and (!self.was_prepared or self.votes != all)) return error.InvalidCatalogRecord;
                if (self.phase == .preparing and self.was_prepared) return error.InvalidCatalogRecord;
            },
            .decided, .active, .resolved, .retired => {
                const decision = self.decision orelse return error.InvalidCatalogRecord;
                if (decision.version <= self.identity.begin_timestamp) return error.InvalidCatalogRecord;
                if (decision.status == .committed and (!self.was_prepared or self.votes != all)) return error.InvalidCatalogRecord;
                if (self.identity.kind == .session_commit and ((decision.status == .aborted and self.was_prepared) or (decision.status == .committed and decision.sync_level != self.identity.commit_sync_level))) return error.InvalidCatalogRecord;
                if (decision.status == .aborted and (self.activations != 0 or self.phase == .active)) return error.InvalidCatalogRecord;
                if (decision.status == .committed and self.phase != .decided and self.activations != all) return error.InvalidCatalogRecord;
                if ((self.phase == .resolved or self.phase == .retired) and self.resolutions != all) return error.InvalidCatalogRecord;
                if (self.phase == .decided and (all == 0 or self.resolutions == all or (decision.status == .committed and (self.resolutions != 0 or self.activations == all)))) return error.InvalidCatalogRecord;
                if (self.phase == .active and self.resolutions == all) return error.InvalidCatalogRecord;
            },
        }
    }
};

/// The metadata apply lane receipts the whole command before returning. These
/// transitions never contact native owners and never sample a process clock.
pub const Action = union(enum) {
    vote: u64,
    prepare,
    commit: DecisionInput,
    abort: u64,
    activate: u64,
    resolve: u64,
    handoff,
};

pub fn advance(a: A, record: Record, expected_revision: u64, owner: Owner, action: Action) !Record {
    try record.validate(a);
    if (!(try record.identity.owner(a)).eql(owner)) return error.ExternalTransactionDecisionChanged;
    if (record.revision != expected_revision) return error.CatalogGenerationChanged;
    var next = record;
    next.revision = std.math.add(u64, record.revision, 1) catch return error.CatalogRevisionExhausted;
    const all = record.mask();
    switch (action) {
        .vote => |members| {
            if (record.phase != .preparing or members == 0 or members & ~all != 0) return error.InvalidPreparedTransactionPhase;
            next.votes |= members;
        },
        .prepare => {
            if (record.phase != .preparing) return error.InvalidPreparedTransactionPhase;
            // One trusted coordinator attests its complete durable vote cohort.
            // Partial progress is not a prerequisite for recovery, which aborts
            // every frozen participant when preparation cannot finish.
            next.votes = all;
            next.was_prepared = true;
            next.phase = .prepared;
        },
        .commit => |decision| {
            if (record.phase != .prepared or decision.version <= record.identity.begin_timestamp or (record.identity.kind == .session_commit and decision.sync_level != record.identity.commit_sync_level)) return error.InvalidPreparedTransactionPhase;
            next.decision = .{ .status = .committed, .version = decision.version, .sync_level = decision.sync_level };
            next.phase = if (all == 0) .resolved else .decided;
        },
        .abort => |version| {
            if ((record.phase != .preparing and record.phase != .prepared) or (record.identity.kind == .session_commit and record.phase == .prepared) or version <= record.identity.begin_timestamp) return error.InvalidPreparedTransactionPhase;
            next.decision = .{ .status = .aborted, .version = version };
            next.phase = if (all == 0) .resolved else .decided;
        },
        .activate => |members| {
            if ((record.phase != .decided and record.phase != .active) or record.decision.?.status != .committed or members == 0 or members & ~all != 0) return error.InvalidPreparedTransactionPhase;
            next.activations |= members;
            if (next.activations == all) next.phase = .active;
        },
        .resolve => |members| {
            if ((record.phase != .decided and record.phase != .active) or (record.decision.?.status == .committed and record.phase != .active) or members == 0 or members & ~all != 0) return error.InvalidPreparedTransactionPhase;
            next.resolutions |= members;
            if (next.resolutions == all) next.phase = .resolved;
        },
        .handoff => {
            if (record.phase != .resolved) return error.InvalidPreparedTransactionPhase;
            next.phase = .retired;
        },
    }
    try next.validate(a);
    return next;
}

/// Decision intent for an already validated immutable registry record. No
/// clock sampling, I/O, metadata mutation or participant work occurs here.
/// A caller must obtain a fresh decision version before applying the action.
pub fn recoveryDecision(record: Record, now_ns: u64) ?enum { abort, commit } {
    return switch (record.phase) {
        .preparing => if (now_ns >= record.identity.expires_at_unix_ns) .abort else null,
        .prepared => if (record.identity.kind == .session_commit) .commit else null,
        .decided, .active, .resolved, .retired => null,
    };
}

pub const AdmissionInput = struct { identity: Identity, participants: []const Participant, admitted_at_ns: u64 };

pub const Mutation = struct {
    admission: outcomes.Admission,
    incarnation: [16]u8,
    actor: Actor,
    native_admin: bool = false,
    standalone: bool = false,
    database_id: u64,
    operation: union(enum) {
        admit: AdmissionInput,
        /// Competes for the original admission nonce and canonical digest.
        /// The first durable representation wins; no new GID is reserved.
        cancel_admission: struct { original_actor: Actor, original: AdmissionInput },
        advance: struct { owner: Owner, expected_revision: u64, action: Action },
        acknowledge_receipt: struct { owner: Owner, txn_id: [16]u8 },
    },

    pub fn validate(self: Mutation) !void {
        try self.admission.validate();
        try self.actor.validate();
        if (self.native_admin and self.actor.current_role != null) return error.InvalidCatalogMutation;
        if (self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidCatalogMutation;
        switch (self.operation) {
            .acknowledge_receipt => |receipt| {
                try receipt.owner.validate();
                if (self.actor.principal().kind != .bootstrap or std.mem.allEqual(u8, &receipt.txn_id, 0)) return error.Forbidden;
            },
            .admit => |request| try validateAdmission(self, request, self.actor),
            .cancel_admission => |cancel| {
                if (self.actor.principal().kind != .bootstrap or self.native_admin) return error.Forbidden;
                try validateAdmission(self, cancel.original, cancel.original_actor);
            },
            .advance => |request| {
                try request.owner.validate();
                if (request.expected_revision == 0) return error.InvalidCatalogMutation;
                switch (request.action) {
                    .commit, .abort => {},
                    // Only the trusted recovery/coordinator worker can attest
                    // native votes, activations, resolutions and handoffs.
                    else => if (self.actor.principal().kind != .bootstrap) return error.Forbidden,
                }
            },
        }
    }
};

fn validateAdmission(self: Mutation, request: AdmissionInput, actor: Actor) !void {
    try actor.validate();
    try request.identity.validate();
    if (request.identity.session) |session| try session.requireRealm(self.standalone);
    if (request.identity.database_id != self.database_id or !request.identity.principal.eql(actor.principal()) or
        !std.mem.eql(u8, &request.identity.operation_id, &self.admission.nonce) or
        !std.mem.eql(u8, &request.identity.metadata_incarnation, &std.fmt.bytesToHex(self.incarnation, .lower))) return error.InvalidCatalogMutation;
    try (Owner{ .metadata_incarnation = request.identity.metadata_incarnation, .operation_id = request.identity.operation_id, .admission_digest = @splat(1), .expires_at_unix_ns = request.identity.expires_at_unix_ns }).requireFreshVoteAt(request.admitted_at_ns);
    if (request.participants.len > max_participants) return error.CatalogCommandTooLarge;
    for (request.participants) |participant| try participant.validate(request.identity.metadata_incarnation);
}

/// Cancellation is an alternative execution of the exact same admission, not
/// an unrelated mutation that could race a later original proposal. The
/// command envelope still requires trusted bootstrap authority to cancel.
pub fn admissionRequest(request: Mutation) Mutation {
    if (request.operation != .cancel_admission) return request;
    var original = request;
    original.actor = request.operation.cancel_admission.original_actor;
    original.operation = .{ .admit = request.operation.cancel_admission.original };
    return original;
}

pub fn commandDigest(a: A, request: Mutation) ![32]u8 {
    try request.validate();
    return digest(a, "antfly:sql-prepared-command:v1\x00", admissionRequest(request));
}
pub const ReceiptWorkRequest = struct {
    owner_node_id: ?u64 = null,
    after: ?[16]u8 = null,
    standalone: bool = false,
    pub fn jsonStringify(self: ReceiptWorkRequest, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(ReceiptWorkRequest).@"struct".field_names) |field| {
            if (!(comptime std.mem.eql(u8, field, "standalone")) or self.standalone) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: ReceiptWorkRequest) !void {
        if (self.standalone) {
            if (self.owner_node_id == null or self.owner_node_id.? != 0) return error.InvalidCatalogMutation;
        } else if (self.owner_node_id != null and self.owner_node_id.? == 0) return error.InvalidCatalogMutation;
    }
};

pub const Read = union(enum) {
    gid: []const u8,
    operation: [16]u8,
    work: ?[16]u8,
    receipt: [16]u8,
    receipt_work: ReceiptWorkRequest,
    pub fn validate(self: Read) !void {
        switch (self) {
            .gid => |gid| try validateGid(gid),
            .operation => |id| if (std.mem.allEqual(u8, &id, 0)) return error.InvalidCatalogMutation,
            .work => {},
            .receipt => |id| if (std.mem.allEqual(u8, &id, 0)) return error.InvalidCatalogMutation,
            .receipt_work => |work| try work.validate(),
        }
    }
};
pub const Snapshot = struct {
    standalone: bool = false,
    catalog_revision: u64 = 0,
    incarnation: [16]u8,
    head: u64,
    active: u64,
    records: []const Record,
    receipts: []const Receipt = &.{},
    outstanding_receipts: u64 = 0,
    next_after: ?[16]u8 = null,
};
pub const Write = struct { key: []const u8, value: []const u8 };
pub const Plan = struct {
    writes: []const Write,
    deletes: []const []const u8,
    result: outcomes.Result,

    /// The caller commits this together with its retained command receipt.
    pub fn apply(self: Plan, txn: anytype) !void {
        for (self.writes) |write| try txn.put(write.key, write.value);
        for (self.deletes) |key| try txn.delete(key);
    }
};

fn prefix(a: A, group: u64, family: []const u8) ![]u8 {
    if (group == 0) return error.InvalidCatalogMutation;
    return std.fmt.allocPrint(a, "\x00\x00__metadata__:sql_prepared:{d}:{s}:", .{ group, family });
}
fn operationKey(a: A, group: u64, id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "operation"), &std.fmt.bytesToHex(id, .lower) });
}
fn workKey(a: A, group: u64, id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "work"), &std.fmt.bytesToHex(id, .lower) });
}
fn gidKey(a: A, group: u64, gid: []const u8) ![]u8 {
    var bytes: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(gid, &bytes, .{});
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "gid"), &std.fmt.bytesToHex(bytes, .lower) });
}
fn countKey(a: A, group: u64, family: []const u8, id: u64) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{d}", .{ try prefix(a, group, family), id });
}
fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
fn number(txn: anytype, key: []const u8) !u64 {
    const value = (try optional(txn, key)) orelse return 0;
    if (value.len != 8) return error.InvalidCatalogRecord;
    return std.mem.readInt(u64, value[0..8], .big);
}
pub fn activeCount(a: A, txn: anytype, group: u64) !u64 {
    _ = a;
    if (group == 0) return error.InvalidCatalogMutation;
    var buffer: [160]u8 = undefined;
    const key = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:sql_prepared:{d}:active:{d}", .{ group, 0 });
    const count = try number(txn, key);
    if (count > max_active) return error.InvalidCatalogRecord;
    return count;
}
pub fn databaseCount(a: A, txn: anytype, group: u64, id: u64) !u64 {
    _ = a;
    if (group == 0) return error.InvalidCatalogMutation;
    var buffer: [160]u8 = undefined;
    const key = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:sql_prepared:{d}:database:{d}", .{ group, id });
    const count = try number(txn, key);
    if (count > max_active) return error.InvalidCatalogRecord;
    return count;
}
pub fn tableCount(a: A, txn: anytype, group: u64, id: u64) !u64 {
    _ = a;
    if (group == 0) return error.InvalidCatalogMutation;
    var buffer: [160]u8 = undefined;
    const key = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:sql_prepared:{d}:table:{d}", .{ group, id });
    const count = try number(txn, key);
    if (count > max_active) return error.InvalidCatalogRecord;
    return count;
}
pub fn readOperation(a: A, txn: anytype, group: u64, id: [16]u8) !?Record {
    const bytes = (try optional(txn, try operationKey(a, group, id))) orelse return null;
    if (bytes.len > max_bytes) return error.InvalidCatalogRecord;
    const record = try std.json.parseFromSliceLeaky(Record, a, bytes, .{ .allocate = .alloc_always });
    try record.validate(a);
    if (!std.mem.eql(u8, &record.identity.operation_id, &id) or record.phase == .retired) return error.InvalidCatalogRecord;
    const work = try optional(txn, try workKey(a, group, id));
    if ((record.phase != .prepared or record.identity.kind == .session_commit) != (work != null) or (work != null and work.?.len != 0)) return error.InvalidCatalogRecord;
    return record;
}
pub fn readGid(a: A, txn: anytype, group: u64, gid: []const u8) !?Record {
    try validateGid(gid);
    const bytes = (try optional(txn, try gidKey(a, group, gid))) orelse return null;
    if (bytes.len != 16) return error.InvalidCatalogRecord;
    const record = (try readOperation(a, txn, group, bytes[0..16].*)) orelse return error.InvalidCatalogRecord;
    if (record.identity.kind != .named or !std.mem.eql(u8, record.identity.gid, gid)) return error.InvalidCatalogRecord;
    return record;
}
fn changeCount(a: A, txn: anytype, writes: *std.ArrayListUnmanaged(Write), deletes: *std.ArrayListUnmanaged([]const u8), key: []const u8, adding: bool) !void {
    const old = try number(txn, key);
    if (old > max_active or (adding and old == max_active) or (!adding and old == 0)) return error.InvalidCatalogRecord;
    const value = if (adding) old + 1 else old - 1;
    if (value == 0) return deletes.append(a, key);
    const bytes = try a.alloc(u8, 8);
    std.mem.writeInt(u64, bytes[0..8], value, .big);
    try writes.append(a, .{ .key = key, .value = bytes });
}
pub fn kindCount(a: A, txn: anytype, group: u64, kind: Kind) !u64 {
    const count = try number(txn, try countKey(a, group, if (kind == .named) "named_active" else "session_active", 0));
    if (count > (if (kind == .named) @as(u64, max_named) else max_session_commits)) return error.InvalidCatalogRecord;
    return count;
}
fn reservations(a: A, txn: anytype, group: u64, record: Record, adding: bool, writes: *std.ArrayListUnmanaged(Write), deletes: *std.ArrayListUnmanaged([]const u8)) !void {
    try changeCount(a, txn, writes, deletes, try countKey(a, group, "active", 0), adding);
    try changeCount(a, txn, writes, deletes, try countKey(a, group, if (record.identity.kind == .named) "named_active" else "session_active", 0), adding);
    try changeCount(a, txn, writes, deletes, try countKey(a, group, "database", record.identity.database_id), adding);
    for (record.participants, 0..) |participant, index| {
        var seen = false;
        for (record.participants[0..index]) |previous| if (previous.route.table_id == participant.route.table_id) {
            seen = true;
            break;
        };
        if (!seen) try changeCount(a, txn, writes, deletes, try countKey(a, group, "table", participant.route.table_id), adding);
    }
}

/// Arena-owned planning performs every rejection before the first native write.
/// Metadata catalog/topology validation runs in the caller's same transaction.
pub fn plan(a: A, txn: anytype, group: u64, catalog_revision: u64, request: Mutation) !Plan {
    try request.validate();
    var writes = std.ArrayListUnmanaged(Write).empty;
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    const record: Record = switch (request.operation) {
        .cancel_admission => |cancel| {
            if (try readOperation(a, txn, group, cancel.original.identity.operation_id) != null or
                try readReceipt(a, txn, group, cancel.original.identity.operation_id) != null) return error.InvalidPreparedTransactionPhase;
            // The caller journals this rejection under the original admission
            // digest in the same transaction. A delayed BEGIN producer can
            // therefore never obtain a successful admission acknowledgement.
            return error.ExternalTransactionAdmissionExpired;
        },
        .acknowledge_receipt => |ack| {
            const receipt = (try readReceipt(a, txn, group, ack.owner.operation_id)) orelse return .{ .writes = &.{}, .deletes = &.{}, .result = .{ .revision = catalog_revision, .identity = null, .removed = 0, .no_op = true } };
            if (!receipt.owner.eql(ack.owner) or !std.mem.eql(u8, &receipt.txn_id, &ack.txn_id)) return error.ExternalTransactionDecisionChanged;
            if (receipt.status == .pending) return error.InvalidPreparedTransactionPhase;
            try changeReceiptCount(a, txn, group, false, &writes, &deletes);
            try deletes.append(a, try receiptKey(a, group, ack.owner.operation_id));
            try deletes.append(a, try receiptNodeKey(a, group, receipt.session.owner_node_id, ack.owner.operation_id));
            return .{ .writes = writes.items, .deletes = deletes.items, .result = .{ .revision = catalog_revision, .identity = null, .removed = 0 } };
        },
        .admit => |admission| blk: {
            if (admission.identity.kind == .named and try readGid(a, txn, group, admission.identity.gid) != null) return error.SqlPreparedTransactionAlreadyExists;
            if (try readOperation(a, txn, group, admission.identity.operation_id) != null) return error.ExternalTransactionDecisionChanged;
            if (try activeCount(a, txn, group) == max_active or try kindCount(a, txn, group, admission.identity.kind) == (if (admission.identity.kind == .named) @as(u64, max_named) else max_session_commits)) return error.SqlPreparedTransactionCapacityExceeded;
            const candidate: Record = .{ .identity = admission.identity, .participants = admission.participants };
            try candidate.validate(a);
            try reservations(a, txn, group, candidate, true, &writes, &deletes);
            if (candidate.identity.session) |session| {
                if (try receiptCount(a, txn, group) == max_receipts) return error.SqlPreparedTransactionCapacityExceeded;
                if (try readReceipt(a, txn, group, candidate.identity.operation_id) != null) return error.ExternalTransactionDecisionChanged;
                try changeReceiptCount(a, txn, group, true, &writes, &deletes);
                try writeReceipt(a, group, .{ .owner = try candidate.identity.owner(a), .txn_id = candidate.identity.txn_id, .gid = candidate.identity.gid, .kind = candidate.identity.kind, .session = session, .status = .pending }, true, &writes);
            }
            if (admission.identity.kind == .named) try writes.append(a, .{ .key = try gidKey(a, group, admission.identity.gid), .value = try a.dupe(u8, &admission.identity.operation_id) });
            try writes.append(a, .{ .key = try workKey(a, group, admission.identity.operation_id), .value = "" });
            break :blk candidate;
        },
        .advance => |change| blk: {
            const previous = (try readOperation(a, txn, group, change.owner.operation_id)) orelse return error.SqlPreparedTransactionNotFound;
            if (previous.identity.kind == .named) {
                const indexed = (try readGid(a, txn, group, previous.identity.gid)) orelse return error.InvalidCatalogRecord;
                if (!std.mem.eql(u8, &indexed.identity.operation_id, &previous.identity.operation_id)) return error.InvalidCatalogRecord;
            }
            if (previous.identity.database_id != request.database_id) return error.SqlPreparedTransactionWrongDatabase;
            if (!request.native_admin and request.actor.principal().kind != .bootstrap and !previous.identity.principal.eql(request.actor.principal())) return error.Forbidden;
            const next = try advance(a, previous, change.expected_revision, change.owner, change.action);
            if ((next.phase == .prepared and next.identity.kind == .named) or next.phase == .retired) {
                try deletes.append(a, try workKey(a, group, next.identity.operation_id));
            } else if (previous.phase == .prepared and previous.identity.kind == .named) {
                try writes.append(a, .{ .key = try workKey(a, group, next.identity.operation_id), .value = "" });
            }
            // Publication of PREPARED and its API handoff proof are one commit.
            // A later GID resolution cannot erase the original session result.
            const named_handoff = next.identity.kind == .named and ((!previous.was_prepared and next.was_prepared) or (!next.was_prepared and previous.phase != .resolved and next.phase == .resolved));
            const session_handoff = next.identity.kind == .session_commit and previous.phase != .resolved and next.phase == .resolved;
            if (next.identity.session != null and (named_handoff or session_handoff)) {
                var receipt = (try readReceipt(a, txn, group, next.identity.operation_id)) orelse return error.InvalidCatalogRecord;
                if (receipt.status != .pending or !receipt.owner.eql(change.owner) or !std.mem.eql(u8, &receipt.txn_id, &next.identity.txn_id)) return error.InvalidCatalogRecord;
                if (receipt.kind != next.identity.kind) return error.InvalidCatalogRecord;
                receipt.status = if (next.identity.kind == .session_commit) (if (next.decision.?.status == .committed) .committed else .aborted) else if (next.was_prepared) .prepared else .aborted;
                if (next.identity.kind == .session_commit) receipt.decision = next.decision;
                try writeReceipt(a, group, receipt, false, &writes);
            }
            if (next.phase == .retired) {
                try reservations(a, txn, group, next, false, &writes, &deletes);
                if (next.identity.kind == .named) try deletes.append(a, try gidKey(a, group, next.identity.gid));
                try deletes.append(a, try operationKey(a, group, next.identity.operation_id));
                return .{ .writes = writes.items, .deletes = deletes.items, .result = .{ .revision = catalog_revision, .identity = null, .removed = 0 } };
            }
            break :blk next;
        },
    };
    const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
    if (bytes.len > max_bytes) return error.CatalogCommandTooLarge;
    try writes.append(a, .{ .key = try operationKey(a, group, record.identity.operation_id), .value = bytes });
    return .{ .writes = writes.items, .deletes = deletes.items, .result = .{ .revision = catalog_revision, .identity = null, .removed = 0 } };
}

pub fn prefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidCatalogMutation;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_prepared:{d}:", .{group});
}
pub const Work = struct { records: []const Record, next_after: ?[16]u8 = null };
pub fn readWork(a: A, txn: anytype, group: u64, after: ?[16]u8) !Work {
    const range_prefix = try prefix(a, group, "work");
    const seek = if (after) |id| try workKey(a, group, id) else range_prefix;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(seek);
    const records = try a.alloc(Record, 8);
    var used: usize = 0;
    while (entry) |row| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, row.key, range_prefix)) break;
        if (after != null and std.mem.eql(u8, row.key, seek)) continue;
        if (used == records.len) return .{ .records = records, .next_after = records[used - 1].identity.operation_id };
        if (row.key.len != range_prefix.len + 32 or row.value.len != 0) return error.InvalidCatalogRecord;
        var id: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, row.key[range_prefix.len..]) catch return error.InvalidCatalogRecord;
        const record = (try readOperation(a, txn, group, id)) orelse return error.InvalidCatalogRecord;
        if ((record.phase == .prepared and record.identity.kind == .named) or record.phase == .retired) return error.InvalidCatalogRecord;
        records[used] = record;
        used += 1;
    }
    return .{ .records = records[0..used] };
}

/// Slots are reserved before any native BEGIN. A lost API reply therefore
/// cannot consume unbounded metadata or make a durable PREPARE lose its proof.
/// Completion requires the receiving session store to confirm durable adoption
/// or canonical absence; wall-clock expiry never fabricates that acknowledgement.
pub const max_receipts = 4096;
pub const Receipt = struct {
    owner: Owner,
    txn_id: [16]u8,
    gid: []const u8,
    session: SessionRef,
    kind: Kind = .named,
    status: enum { pending, prepared, committed, aborted },
    /// Private COMMIT recovery must retain its exact version and visibility
    /// contract after the metadata job and native coordinator are retired.
    decision: ?Decision = null,
    pub fn jsonStringify(self: Receipt, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Receipt).@"struct".field_names) |field| {
            if (!(comptime std.mem.eql(u8, field, "kind") or std.mem.eql(u8, field, "decision")) or self.kind == .session_commit) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: Receipt) !void {
        try self.owner.validate();
        try self.session.validate();
        try validateGid(self.gid);
        if ((self.kind == .named and self.status == .committed) or (self.kind == .session_commit and (self.status == .prepared or self.gid.len != 0 or self.session.connection_id == null))) return error.InvalidCatalogRecord;
        if (self.kind == .named and self.decision != null) return error.InvalidCatalogRecord;
        if (self.kind == .session_commit) {
            if ((self.status == .pending) != (self.decision == null)) return error.InvalidCatalogRecord;
            if (self.decision) |decision| {
                if (decision.version == 0 or (self.status == .committed) != (decision.status == .committed) or (decision.status == .committed and decision.sync_level == .propose)) return error.InvalidCatalogRecord;
            }
        }
        if (std.mem.allEqual(u8, &self.txn_id, 0)) return error.InvalidCatalogRecord;
    }
};
pub fn receiptCount(a: A, txn: anytype, group: u64) !u64 {
    const count = try number(txn, try countKey(a, group, "receipt_active", 0));
    if (count > max_receipts) return error.InvalidCatalogRecord;
    return count;
}
fn receiptKey(a: A, group: u64, id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "receipt"), &std.fmt.bytesToHex(id, .lower) });
}
fn receiptNodePrefix(a: A, group: u64, node: u64) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{d}:", .{ try prefix(a, group, "receipt_node"), node });
}
fn receiptNodeKey(a: A, group: u64, node: u64, id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try receiptNodePrefix(a, group, node), &std.fmt.bytesToHex(id, .lower) });
}
fn changeReceiptCount(a: A, txn: anytype, group: u64, adding: bool, writes: *std.ArrayListUnmanaged(Write), deletes: *std.ArrayListUnmanaged([]const u8)) !void {
    const key = try countKey(a, group, "receipt_active", 0);
    const old = try number(txn, key);
    if (old > max_receipts or (adding and old == max_receipts) or (!adding and old == 0)) return error.InvalidCatalogRecord;
    const next = if (adding) old + 1 else old - 1;
    if (next == 0) return deletes.append(a, key);
    const bytes = try a.alloc(u8, 8);
    std.mem.writeInt(u64, bytes[0..8], next, .big);
    try writes.append(a, .{ .key = key, .value = bytes });
}
fn writeReceipt(a: A, group: u64, receipt: Receipt, initial: bool, writes: *std.ArrayListUnmanaged(Write)) !void {
    try receipt.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, receipt, .{});
    try writes.append(a, .{ .key = try receiptKey(a, group, receipt.owner.operation_id), .value = bytes });
    // This membership is immutable and readReceipt already verifies it on
    // transitions. Do not rewrite its LSM key for every completion update.
    if (initial) try writes.append(a, .{ .key = try receiptNodeKey(a, group, receipt.session.owner_node_id, receipt.owner.operation_id), .value = "1" });
}
pub fn readReceipt(a: A, txn: anytype, group: u64, id: [16]u8) !?Receipt {
    const bytes = (try optional(txn, try receiptKey(a, group, id))) orelse return null;
    if (bytes.len > 4096) return error.InvalidCatalogRecord;
    const receipt = try std.json.parseFromSliceLeaky(Receipt, a, bytes, .{ .allocate = .alloc_always });
    try receipt.validate();
    if (!std.mem.eql(u8, &receipt.owner.operation_id, &id)) return error.InvalidCatalogRecord;
    const node = (try optional(txn, try receiptNodeKey(a, group, receipt.session.owner_node_id, id))) orelse return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, node, "1")) return error.InvalidCatalogRecord;
    return receipt;
}
pub const ReceiptWork = struct { receipts: []const Receipt, next_after: ?[16]u8 = null };
pub fn readReceiptWork(a: A, txn: anytype, group: u64, node: ?u64, after: ?[16]u8) !ReceiptWork {
    const range_prefix = if (node) |id| try receiptNodePrefix(a, group, id) else try prefix(a, group, "receipt");
    const seek = if (after) |id| if (node) |node_id| try receiptNodeKey(a, group, node_id, id) else try receiptKey(a, group, id) else range_prefix;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(seek);
    const receipts = try a.alloc(Receipt, 8);
    var used: usize = 0;
    while (entry) |row| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, row.key, range_prefix)) break;
        if (after != null and std.mem.eql(u8, row.key, seek)) continue;
        if (used == receipts.len) return .{ .receipts = receipts, .next_after = receipts[used - 1].owner.operation_id };
        if (row.key.len != range_prefix.len + 32) return error.InvalidCatalogRecord;
        var id: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, row.key[range_prefix.len..]) catch return error.InvalidCatalogRecord;
        const receipt = (try readReceipt(a, txn, group, id)) orelse return error.InvalidCatalogRecord;
        if (node) |node_id| if (receipt.session.owner_node_id != node_id or !std.mem.eql(u8, row.value, "1")) return error.InvalidCatalogRecord;
        receipts[used] = receipt;
        used += 1;
    }
    return .{ .receipts = receipts[0..used] };
}

test "SQL prepared transaction session realms preserve clustered bytes and fence standalone sentinels" {
    const a = std.testing.allocator;
    const clustered: SessionRef = .{ .owner_node_id = 9 };
    const legacy = try std.json.Stringify.valueAlloc(a, clustered, .{});
    defer a.free(legacy);
    try std.testing.expectEqualStrings("{\"owner_node_id\":9,\"connection_id\":null}", legacy);
    try clustered.requireRealm(false);
    try std.testing.expectError(error.CatalogGenerationChanged, clustered.requireRealm(true));
    const standalone_ref: SessionRef = .{ .owner_node_id = 0, .connection_id = @splat('a'), .standalone = true };
    try standalone_ref.requireRealm(true);
    try std.testing.expectError(error.CatalogGenerationChanged, standalone_ref.requireRealm(false));
    try std.testing.expectError(error.InvalidCatalogMutation, (SessionRef{ .owner_node_id = 0 }).validate());
    try std.testing.expectError(error.InvalidCatalogMutation, (SessionRef{ .owner_node_id = 9, .standalone = true }).validate());
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const identity: Identity = .{ .kind = .session_commit, .session = standalone_ref, .gid = "", .database_id = 1, .principal = .bootstrap, .txn_id = @splat(2), .operation_id = @splat(3), .metadata_incarnation = @splat('1'), .begin_timestamp = 10, .expires_at_unix_ns = 1000, .plan_digest = try planDigest("[]"), .manifest_digest = try manifestDigest(owned, &.{}, @splat('1')) };
    const request: Mutation = .{ .admission = .{ .nonce = identity.operation_id, .head = 0 }, .incarnation = @splat(0x11), .actor = .{ .session_user = .bootstrap, .credential = "" }, .standalone = true, .database_id = 1, .operation = .{ .admit = .{ .identity = identity, .participants = &.{}, .admitted_at_ns = 20 } } };
    try request.validate();
    var wrong = request;
    wrong.standalone = false;
    try std.testing.expectError(error.CatalogGenerationChanged, wrong.validate());
    wrong = request;
    wrong.operation.admit.identity.session = .{ .owner_node_id = 9, .connection_id = @splat('a') };
    try std.testing.expectError(error.CatalogGenerationChanged, wrong.validate());
    wrong.standalone = false;
    try wrong.validate();
    try std.testing.expect(!(try identity.owner(owned)).eql(try wrong.operation.admit.identity.owner(owned)));
    const encoded = try std.json.Stringify.valueAlloc(owned, request, .{});
    const decoded = try std.json.parseFromSliceLeaky(Mutation, owned, encoded, .{});
    try decoded.validate();
    try std.testing.expectEqualDeep(request, decoded);
}

test "SQL prepared transaction receipt work uses an explicit standalone realm without changing clustered selectors" {
    const a = std.testing.allocator;
    const clustered: Read = .{ .receipt_work = .{ .owner_node_id = 9 } };
    const encoded = try std.json.Stringify.valueAlloc(a, clustered, .{});
    defer a.free(encoded);
    try std.testing.expectEqualStrings("{\"receipt_work\":{\"owner_node_id\":9,\"after\":null}}", encoded);
    try clustered.validate();
    try (Read{ .receipt_work = .{ .owner_node_id = 0, .standalone = true } }).validate();
    try std.testing.expectError(error.InvalidCatalogMutation, (Read{ .receipt_work = .{ .owner_node_id = 0 } }).validate());
    try std.testing.expectError(error.InvalidCatalogMutation, (Read{ .receipt_work = .{ .standalone = true } }).validate());
    try std.testing.expectError(error.InvalidCatalogMutation, (Read{ .receipt_work = .{ .owner_node_id = 9, .standalone = true } }).validate());
}
