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

//! Transactional validation publication ledger. These helpers never commit:
//! errors require aborting the writer or fully restoring its command savepoint
//! before classifying a losing guard. Storage/corruption failures remain fatal.
//! Production admission/dispatch additionally requires the
//! coordinated decoder, standby, SQL/catalog and authenticated owner adapters.
//! Signed preparation is retained separately from progress, so restore can
//! verify the original physical root instead of trusting the first ACK.
const std = @import("std");
const local = @import("antfly_local_sources");
const plan_mod = @import("schema_validation_plan.zig");
const barriers = @import("schema_validation_publication.zig");
const evidence = @import("schema_validation_attestation.zig");
pub const parent = @import("schema_validation_parent.zig");
pub const roots = @import("schema_validation_root_epoch.zig");
const lifecycle = @import("table_lifecycle_lock.zig");
const contract = local.storage_db_relational_schema_owner_contract;
const A = std.mem.Allocator;
pub const all_prefix = "\x00\x00__metadata__:schema_validation:";
pub const Kind = enum(u8) { plan, head, owner, proof, preparation, active, input, parent, root_epoch, proof_history, parent_image, owner_namespace, participants, type_active, sequence_active, sequence_namespace, sequence_database, sequence_outcome, object_rename_source, retirement_active };
const OwnerAddress = @import("schema_validation_read.zig").OwnerAddress;
const ParticipantAlias = barriers.Record(@import("schema_validation_read.zig").Lookup, "SVPA", 1024);
const NamespaceOwner = struct { address: OwnerAddress, ordinal: u16 };
/// Canonical reverse-owner bytes shared by admission and native authority
/// fixtures. Encoding or decoding this record grants no lifecycle authority.
pub const NamespaceOwnerRecord = barriers.Record(NamespaceOwner, "SVNO", 256);
const ParentImage = struct { ordinal: u16, grant: [32]u8 };
const ParentImageRecord = barriers.Record(ParentImage, "SVPI", 256);
pub const PreparationRecord = barriers.Record(evidence.SignedPreparation, "SVPP", evidence.SignedPreparation.max_wire_bytes);
pub const ProofRecord = barriers.Record(evidence.SignedObservation, "SVPE", 48 * 1024);
pub const max_command_bytes = 64 * 1024 * 1024;
pub const max_control_command_bytes = 64 * 1024;
const begin_magic = "SVBI\x01\x00\x00\x00";
pub fn requiresAdmissionAuthority(bytes: []const u8) bool {
    // JSON begin is never accepted by parseCommand; only this binary frame
    // carries immutable inputs. Keep permission classification with its codec.
    return std.mem.startsWith(u8, bytes, begin_magic);
}
/// Project the manifest with fixed memory before decoding raw preparation
/// inputs. Malformed binary headers conservatively require the newest decoder;
/// parseCommand still rejects them before any mutation is staged.
pub fn requiredDecoderVersion(bytes: []const u8) u16 {
    const protocol = @import("topology_protocol.zig");
    if (!requiresAdmissionAuthority(bytes)) return protocol.schema_validation_publication_version;
    if (bytes.len < 48) return protocol.sql_sequence_retirement_version;
    const size: usize = std.mem.readInt(u32, bytes[8..12], .little);
    if (size == 0 or size > bytes.len - 48 or size > plan_mod.max_bytes * 2) return protocol.sql_sequence_retirement_version;
    if (@import("schema_reader_wire.zig").requiresRetirementCohort(bytes[16..][0..size])) return protocol.table_retirement_survivors_version;
    if (@import("schema_reader_wire.zig").requiresObjectRenames(bytes[16..][0..size])) return protocol.sql_object_annotations_version;
    if (@import("schema_reader_wire.zig").requiresSequenceRetirement(bytes[16..][0..size])) return protocol.sql_sequence_retirement_version;
    if (@import("schema_reader_wire.zig").requiresSqlDomainEpoch(bytes[16..][0..size])) return protocol.sql_domain_epoch_version;
    return if (@import("schema_reader_wire.zig").requiresTypeCohort(bytes[16..][0..size])) protocol.sql_type_epoch_version else protocol.schema_validation_publication_version;
}

pub const Decision = struct { lookup: Lookup, revision: u64 };
pub const Begin = struct {
    plan: plan_mod.Plan,
    preparations: []const evidence.SignedPreparation,
    /// Borrowed raw ASVI frames, in dense first-owner digest order. They are
    /// NEVER serialized as JSON byte arrays or base64.
    inputs: []const []const u8 = &.{},
    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(.{ .plan = self.plan, .preparations = self.preparations });
    }
};
pub const Command = union(enum) {
    begin: Begin,
    ack: struct { lookup: Lookup, ordinal: u16, revision: u64, signed: evidence.SignedObservation },
    authorize_parent: parent.Request,
    adopt_root: roots.Request,
    install: Decision,
    publish: Decision,
    cancel: struct { decision: Decision, failure: barriers.Failure },
};
pub const ParsedCommand = struct {
    alloc: A,
    parsed: std.json.Parsed(Command),
    value: Command,
    input_views: []const []const u8 = &.{},

    pub fn deinit(self: *@This()) void {
        self.parsed.deinit();
        if (self.input_views.len != 0) self.alloc.free(self.input_views);
        self.* = undefined;
    }
};

pub fn encodeBegin(alloc: A, request: Begin) ![]u8 {
    if (request.inputs.len == 0 or request.inputs.len > barriers.max_owners or request.plan.owners.len == 0 or request.plan.owners.len > barriers.max_owners or
        request.plan.selection.count() == 0 or request.plan.selection.count() > contract.max_columns or request.preparations.len != request.plan.owners.len) return error.InvalidGenerationPublication;
    var count = std.Io.Writer.Discarding.init(&.{});
    try std.json.Stringify.value(Command{ .begin = request }, .{}, &count.writer);
    if (count.fullCount() > plan_mod.max_bytes * 2) return error.InvalidGenerationPublication;
    const manifest_len: usize = @intCast(count.fullCount());
    var total = try std.math.add(usize, 48, manifest_len);
    for (request.inputs) |input| {
        _ = try contract.inputParts(input);
        total = try std.math.add(usize, total, try std.math.add(usize, 4, input.len));
        if (total > max_command_bytes) return error.InvalidGenerationPublication;
    }
    const bytes = try alloc.alloc(u8, total);
    errdefer alloc.free(bytes);
    @memcpy(bytes[0..8], begin_magic);
    std.mem.writeInt(u32, bytes[8..12], @intCast(manifest_len), .little);
    std.mem.writeInt(u32, bytes[12..16], @intCast(request.inputs.len), .little);
    var writer = std.Io.Writer.fixed(bytes[16..][0..manifest_len]);
    try std.json.Stringify.value(Command{ .begin = request }, .{}, &writer);
    var offset = 16 + manifest_len;
    for (request.inputs) |input| {
        std.mem.writeInt(u32, bytes[offset..][0..4], @intCast(input.len), .little);
        offset += 4;
        @memcpy(bytes[offset..][0..input.len], input);
        offset += input.len;
    }
    @memcpy(bytes[offset..][0..32], &contract.hash(bytes[0..offset]));
    return bytes;
}

pub fn parseCommand(alloc: A, bytes: []const u8) !ParsedCommand {
    if (bytes.len == 0 or bytes.len > max_command_bytes) return error.InvalidGenerationPublication;
    const framed = std.mem.startsWith(u8, bytes, begin_magic);
    if (!framed and bytes.len > max_control_command_bytes) return error.InvalidGenerationPublication;
    var body = bytes;
    var input_count: usize = 0;
    if (framed) {
        if (bytes.len < 48 or !std.mem.eql(u8, &contract.hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..])) return error.InvalidGenerationPublication;
        const manifest_len: usize = std.mem.readInt(u32, bytes[8..12], .little);
        input_count = std.mem.readInt(u32, bytes[12..16], .little);
        if (manifest_len == 0 or manifest_len > plan_mod.max_bytes * 2 or manifest_len > bytes.len - 48 or input_count == 0 or input_count > barriers.max_owners) return error.InvalidGenerationPublication;
        body = bytes[16..][0..manifest_len];
    }
    var parsed = std.json.parseFromSlice(Command, alloc, body, .{ .allocate = .alloc_always }) catch |err|
        return if (err == error.OutOfMemory) err else error.InvalidGenerationPublication;
    errdefer parsed.deinit();
    var value = parsed.value;
    switch (value) {
        .begin => |request| if (!framed or request.inputs.len != 0 or request.preparations.len == 0 or request.preparations.len > barriers.max_owners) return error.InvalidGenerationPublication,
        .ack => |ack| if (framed or ack.ordinal >= barriers.max_owners or ack.revision == 0) return error.InvalidGenerationPublication,
        .authorize_parent => |request| {
            if (framed) return error.InvalidGenerationPublication;
            try request.validateShape();
        },
        .adopt_root => |request| {
            if (framed) return error.InvalidGenerationPublication;
            try request.validateShape();
        },
        .install, .publish => |decision| if (framed or decision.revision == 0) return error.InvalidGenerationPublication,
        .cancel => |request| if (framed or request.decision.revision == 0 or request.failure == .none or request.failure.isConstraintViolation()) return error.InvalidGenerationPublication,
    }
    var input_views: []const []const u8 = &.{};
    if (framed) {
        var canonical = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
        try std.json.Stringify.value(value, .{}, &canonical.writer);
        var digest: [32]u8 = undefined;
        canonical.hasher.final(&digest);
        if (!std.mem.eql(u8, &digest, &contract.hash(body))) return error.InvalidGenerationPublication;
        const inputs = try alloc.alloc([]const u8, input_count);
        errdefer alloc.free(inputs);
        var offset = 16 + body.len;
        const end = bytes.len - 32;
        for (inputs) |*input| {
            if (offset > end or end - offset < 4) return error.InvalidGenerationPublication;
            const len: usize = std.mem.readInt(u32, bytes[offset..][0..4], .little);
            offset += 4;
            if (len > end - offset) return error.InvalidGenerationPublication;
            input.* = bytes[offset..][0..len];
            _ = try contract.inputParts(input.*);
            offset += len;
        }
        if (offset != end) return error.InvalidGenerationPublication;
        value.begin.inputs = inputs;
        input_views = inputs;
    }
    return .{ .alloc = alloc, .parsed = parsed, .value = value, .input_views = input_views };
}

pub const Key = struct {
    group: u64,
    table: u64,
    id: [16]u8,
    kind: Kind,
    ordinal: u16 = 0,
    stage: barriers.Stage = .ready,
    grant: ?[32]u8 = null,

    pub fn encode(self: Key, buf: []u8) ![]const u8 {
        if (self.group == 0 or self.table == 0 or
            (self.kind == .active or self.kind == .retirement_active or self.kind == .type_active or self.kind == .sequence_active or self.kind == .sequence_namespace or self.kind == .sequence_database or self.kind == .object_rename_source) != std.mem.allEqual(u8, &self.id, 0) or
            (self.kind != .owner and self.kind != .proof and self.kind != .preparation and self.kind != .input and self.kind != .parent and self.kind != .root_epoch and self.kind != .proof_history and self.ordinal != 0) or
            (self.kind != .proof and self.kind != .proof_history and self.stage != .ready) or self.ordinal >= barriers.max_owners)
            return error.InvalidGenerationPublication;
        if ((self.kind == .parent or self.kind == .root_epoch or self.kind == .proof_history or self.kind == .parent_image or self.kind == .owner_namespace or self.kind == .sequence_namespace or self.kind == .sequence_database) != (self.grant != null)) return error.InvalidGenerationPublication;
        if (self.grant) |grant| if (std.mem.allEqual(u8, &grant, 0)) return error.InvalidGenerationPublication;
        if ((self.kind == .root_epoch or self.kind == .sequence_namespace or self.kind == .sequence_database) and !std.mem.allEqual(u8, self.grant.?[0..24], 0)) return error.InvalidGenerationPublication;
        const base = try std.fmt.bufPrint(buf, all_prefix ++ "{x:0>16}:{x:0>16}:{s}:{x:0>2}:{x:0>4}:{x:0>2}", .{
            self.group, self.table, std.fmt.bytesToHex(self.id, .lower), @backingInt(self.kind), self.ordinal, @backingInt(self.stage),
        });
        if (self.grant) |grant| {
            const suffix = try std.fmt.bufPrint(buf[base.len..], ":{s}", .{std.fmt.bytesToHex(grant, .lower)});
            return buf[0 .. base.len + suffix.len];
        }
        return base;
    }
    pub fn parse(bytes: []const u8) !?Key {
        if (!std.mem.startsWith(u8, bytes, all_prefix)) return null;
        var parts = std.mem.splitScalar(u8, bytes[all_prefix.len..], ':');
        const group = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidCatalogRecord, 16) catch return error.InvalidCatalogRecord;
        const table = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidCatalogRecord, 16) catch return error.InvalidCatalogRecord;
        const hex = parts.next() orelse return error.InvalidCatalogRecord;
        var id: [16]u8 = undefined;
        if (hex.len != 32) return error.InvalidCatalogRecord;
        _ = std.fmt.hexToBytes(&id, hex) catch return error.InvalidCatalogRecord;
        const kind = std.fmt.parseInt(u8, parts.next() orelse return error.InvalidCatalogRecord, 16) catch return error.InvalidCatalogRecord;
        const ordinal = std.fmt.parseInt(u16, parts.next() orelse return error.InvalidCatalogRecord, 16) catch return error.InvalidCatalogRecord;
        const stage = std.fmt.parseInt(u8, parts.next() orelse return error.InvalidCatalogRecord, 16) catch return error.InvalidCatalogRecord;
        var grant: ?[32]u8 = null;
        if (parts.next()) |hex_grant| {
            if (hex_grant.len != 64) return error.InvalidCatalogRecord;
            var decoded: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&decoded, hex_grant) catch return error.InvalidCatalogRecord;
            grant = decoded;
        }
        if (parts.next() != null) return error.InvalidCatalogRecord;
        const result: Key = .{ .group = group, .table = table, .id = id, .kind = std.enums.fromInt(Kind, kind) orelse return error.InvalidCatalogRecord, .ordinal = ordinal, .stage = std.enums.fromInt(barriers.Stage, stage) orelse return error.InvalidCatalogRecord, .grant = grant };
        var buf: [192]u8 = undefined;
        const canonical = result.encode(&buf) catch return error.InvalidCatalogRecord;
        if (!std.mem.eql(u8, canonical, bytes)) return error.InvalidCatalogRecord;
        return result;
    }
};

pub fn prefix(buf: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidGenerationPublication;
    return std.fmt.bufPrint(buf, all_prefix ++ "{x:0>16}:", .{group});
}

pub const Lookup = @import("schema_validation_read.zig").Lookup;

fn storageKey(lookup: Lookup, kind: Kind, ordinal: u16, stage: barriers.Stage) Key {
    return .{ .group = lookup.group, .table = lookup.table, .id = if (kind == .active) @splat(0) else lookup.id, .kind = kind, .ordinal = ordinal, .stage = stage };
}

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
fn terminal(head: barriers.Head) bool {
    return head.phase == .succeeded or head.phase == .canceled;
}
/// A secondary participant retains only a compact immutable anchor. Returning
/// it is discovery, not execution permission; owner lookup still rejoins the
/// exact physical namespace and current enrolled root.
pub fn resolveActiveHead(txn: anytype, group: u64, table: u64) !?barriers.Head {
    var buffer: [192]u8 = undefined;
    const value = (try optional(txn, try (Key{ .group = group, .table = table, .id = @splat(0), .kind = .active }).encode(&buffer))) orelse return null;
    return if (value.len == 16) block: {
        const id = value[0..16].*;
        const head = try barriers.Head.decode(try txn.get(try (Key{ .group = group, .table = table, .id = id, .kind = .head }).encode(&buffer)));
        if (head.metadata_group_id != group or head.table_id != table or !std.mem.eql(u8, &head.id, &id)) return error.InvalidCatalogRecord;
        break :block head;
    } else block: {
        const alias = try ParticipantAlias.decode(value);
        try (OwnerReadRequest{ .lookup = alias, .ordinal = 0 }).validate();
        if (alias.group != group or alias.table == table) return error.InvalidCatalogRecord;
        const head = try readHead(txn, alias);
        if (std.mem.allEqual(u8, &head.participants_digest, 0)) return error.InvalidCatalogRecord;
        break :block head;
    };
}

pub fn resolveActive(txn: anytype, group: u64, table: u64) !?Lookup {
    return if (try resolveActiveHead(txn, group, table)) |head| Lookup.fromHead(head) else null;
}

/// Resolve the job anchor from a physical participant, then rejoin the exact
/// retained owner binding. An alias alone never authorizes owner work or
/// cancellation; this remains bounded independently of table and plan size.
pub fn activeHeadForBinding(txn: anytype, group: u64, incarnation: [32]u8, binding: contract.Binding) !barriers.Head {
    try binding.validate();
    const head = (try resolveActiveHead(txn, group, binding.fence.namespace.table_id)) orelse return error.GenerationPublicationChanged;
    const lookup = Lookup.fromHead(head);
    if (!std.mem.eql(u8, &lookup.metadata_incarnation, &incarnation) or
        !std.mem.eql(u8, &lookup.id, &binding.id) or !std.mem.eql(u8, &lookup.plan_digest, &binding.plan_digest)) return error.SchemaValidationJobChanged;
    try requireParticipantLock(txn, head, binding.fence.namespace.table_id);
    const control = (try readReplayControl(txn, lookup, OwnerAddress.fromBinding(binding))) orelse return error.SchemaValidationJobChanged;
    if (!std.meta.eql(control.owner.binding, binding)) return error.SchemaValidationJobChanged;
    return head;
}

fn typeActiveKey(group: u64, type_id: u64) Key {
    return .{ .group = group, .table = type_id, .id = @splat(0), .kind = .type_active };
}

const SequenceAcceptance = struct {
    lookup: Lookup,
    admission: @import("../system_catalog/sequence_catalog.zig").Admission,
    digest: [32]u8,
    revision: u64,
    fn validate(self: @This()) !void {
        try (@import("schema_validation_read.zig").OwnerReadRequest{ .lookup = self.lookup, .ordinal = 0 }).validate();
        try self.admission.validate();
        if (self.revision == 0 or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidSequenceOutcome;
    }
};
const SequenceAcceptanceRecord = barriers.Record(SequenceAcceptance, "SVSQ", 2048);
fn sequenceOutcomeKey(group: u64, sequence: u64, nonce: [16]u8) Key {
    return .{ .group = group, .table = sequence, .id = nonce, .kind = .sequence_outcome };
}
fn requireSequenceAcceptance(value: SequenceAcceptance, head: barriers.Head) !void {
    try value.validate();
    const retired = head.sequence_retirement orelse return error.InvalidSequenceOutcome;
    if (!std.meta.eql(value.lookup, Lookup.fromHead(head)) or !std.mem.eql(u8, &value.digest, &retired.admission_digest)) return error.InvalidSequenceOutcome;
}

/// The compact acceptance belongs to the durable job, not the short ordinary
/// catalog outcome ring. Unrelated DDL churn cannot evict an in-flight SQL
/// operation or its eventual terminal decision. Its lifetime follows job history.
pub fn retainedSequenceOutcome(txn: anytype, group: u64, request: @import("../system_catalog/sequence_catalog.zig").Mutation, digest: [32]u8, catalog_revision: u64) !?@import("../system_catalog/sequence_catalog.zig").Outcome {
    if (request.operation != .drop or !request.operation.drop.cascade) return null;
    const expected = request.operation.drop.expected orelse return null;
    var buffer: [192]u8 = undefined;
    const bytes = (try optional(txn, try sequenceOutcomeKey(group, expected.id, request.admission.nonce).encode(&buffer))) orelse return null;
    const accepted = try SequenceAcceptanceRecord.decode(bytes);
    try accepted.validate();
    if (!std.meta.eql(accepted.admission, request.admission) or !std.mem.eql(u8, &accepted.digest, &digest)) return error.SequenceAdmissionConflict;
    const head = try readHead(txn, accepted.lookup);
    try requireSequenceAcceptance(accepted, head);
    const retired = head.sequence_retirement.?;
    if (head.metadata_group_id != group or retired.id != expected.id or retired.generation != expected.generation or
        !std.mem.eql(u8, &head.metadata_incarnation, &std.fmt.bytesToHex(request.incarnation, .lower)) or
        accepted.revision > catalog_revision or head.catalog_revision > catalog_revision) return error.InvalidSequenceOutcome;
    return switch (head.phase) {
        .succeeded => .{ .success = .{ .revision = head.catalog_revision, .identity = null, .removed = 1 } },
        .canceled => .{ .rejected = switch (head.failure) {
            .requested => .Canceled,
            .owner_failed => .CatalogGenerationChanged,
            else => return error.InvalidSequenceOutcome,
        } },
        else => .{ .pending = .{ .revision = accepted.revision, .identity = expected, .work = .{ .sequence_schema = accepted.lookup } } },
    };
}

fn sequenceActiveKey(group: u64, sequence_id: u64) Key {
    return .{ .group = group, .table = sequence_id, .id = @splat(0), .kind = .sequence_active };
}
fn resourceActiveKey(head: barriers.Head) Key {
    if (head.retirement_root) |root| return .{ .group = head.metadata_group_id, .table = root.root.table, .id = @splat(0), .kind = .retirement_active };
    return if (head.type_id != 0) typeActiveKey(head.metadata_group_id, head.type_id) else sequenceActiveKey(head.metadata_group_id, head.sequence_retirement.?.id);
}

/// The common publication head owns sequence exclusion until durable release
/// or cancellation. An alias is never independently sufficient authority.
pub fn activeSequence(txn: anytype, group: u64, sequence_id: u64) !?Lookup {
    var buffer: [192]u8 = undefined;
    const bytes = (try optional(txn, try sequenceActiveKey(group, sequence_id).encode(&buffer))) orelse return null;
    const lookup = try ParticipantAlias.decode(bytes);
    if (lookup.group != group) return error.InvalidCatalogRecord;
    const head = try readHead(txn, lookup);
    const retirement = head.sequence_retirement orelse return error.InvalidCatalogRecord;
    if (retirement.id != sequence_id or terminal(head)) return error.InvalidCatalogRecord;
    try requireLock(txn, head);
    return lookup;
}

fn sequenceContainerKey(head: barriers.Head, kind: Kind) Key {
    const retired = head.sequence_retirement.?;
    var grant: [32]u8 = @splat(0);
    std.mem.writeInt(u64, grant[24..32], retired.id, .big);
    return .{ .group = head.metadata_group_id, .table = if (kind == .sequence_namespace) retired.namespace_id else retired.database_id, .id = @splat(0), .kind = kind, .grant = grant };
}
fn requireContainerReservation(txn: anytype, head: barriers.Head, kind: Kind) !void {
    var buffer: [192]u8 = undefined;
    const bytes = try txn.get(try sequenceContainerKey(head, kind).encode(&buffer));
    if (!std.meta.eql(try ParticipantAlias.decode(bytes), Lookup.fromHead(head))) return error.InvalidCatalogRecord;
}

/// Container lifetimes stay pinned after the counter itself is published away.
/// A covering alias seek touches only active retirements for this container;
/// independent sequences share the namespace without serializing their jobs.
pub fn requireSequenceContainerFree(txn: anytype, group: u64, kind: enum { namespace, database }, container: u64) !void {
    var buffer: [192]u8 = undefined;
    const lane: Kind = if (kind == .namespace) .sequence_namespace else .sequence_database;
    var grant: [32]u8 = @splat(0);
    grant[31] = 1;
    const probe = try (Key{ .group = group, .table = container, .id = @splat(0), .kind = lane, .grant = grant }).encode(&buffer);
    const container_prefix = probe[0 .. probe.len - 64];
    var cursor = try txn.openCursor();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(container_prefix)) orelse return;
    if (!std.mem.startsWith(u8, row.key, container_prefix)) return;
    const key = (try Key.parse(row.key)) orelse return error.InvalidCatalogRecord;
    const lookup = try ParticipantAlias.decode(row.value);
    const head = try readHead(txn, lookup);
    if (head.sequence_retirement == null or terminal(head) or
        !std.meta.eql(key, sequenceContainerKey(head, lane))) return error.InvalidCatalogRecord;
    const active = (try activeSequence(txn, group, head.sequence_retirement.?.id)) orelse return error.InvalidCatalogRecord;
    if (!std.meta.eql(active, lookup)) return error.InvalidCatalogRecord;
    return error.CatalogPublicationProofPending;
}

/// A type reservation blocks new dependency edges and competing executable
/// mutations until every owner releases the coordinated epoch. Annotations
/// remain independent and are preserved by publication.
pub fn activeType(txn: anytype, group: u64, type_id: u64) !?Lookup {
    var buffer: [192]u8 = undefined;
    const bytes = (try optional(txn, try typeActiveKey(group, type_id).encode(&buffer))) orelse return null;
    const lookup = try ParticipantAlias.decode(bytes);
    if (lookup.group != group) return error.InvalidCatalogRecord;
    const head = try readHead(txn, lookup);
    if (head.type_id != type_id or terminal(head)) return error.InvalidCatalogRecord;
    try requireLock(txn, head);
    return lookup;
}

fn retirementCohort(head: barriers.Head) @import("table_retirement_authority.zig").Cohort {
    return .{ .table = head.table_id, .id = head.id, .digest = head.plan_digest };
}
fn activeRetirement(txn: anytype, head: barriers.Head) !?Lookup {
    var buffer: [192]u8 = undefined;
    const bytes = (try optional(txn, try resourceActiveKey(head).encode(&buffer))) orelse return null;
    const selected = try ParticipantAlias.decode(bytes);
    const current = try readHead(txn, selected);
    if (current.retirement_root == null or !std.meta.eql(current.retirement_root, head.retirement_root) or terminal(current)) return error.InvalidCatalogRecord;
    _ = try @import("table_retirement_ledger.zig").requireCohort(txn, current.retirement_root.?, retirementCohort(current));
    return selected;
}

fn requireCatalogReservation(txn: anytype, head: barriers.Head) !void {
    if (!head.catalogEffect()) return;
    const current = (if (head.retirement_root != null) try activeRetirement(txn, head) else if (head.type_id != 0) try activeType(txn, head.metadata_group_id, head.type_id) else try activeSequence(txn, head.metadata_group_id, head.sequence_retirement.?.id)) orelse return error.InvalidCatalogRecord;
    if (!std.meta.eql(current, Lookup.fromHead(head))) return error.InvalidCatalogRecord;
    if (head.sequence_retirement != null) {
        try requireContainerReservation(txn, head, .sequence_namespace);
        try requireContainerReservation(txn, head, .sequence_database);
    }
}

pub fn discoverActive(txn: anytype, request: @import("schema_validation_read.zig").DiscoveryRequest) !?Lookup {
    try request.validate();
    const lock = (try lifecycle.read(txn, request.group, request.table)) orelse return null;
    if (lock.kind == .table_retirement) {
        const root_mod = @import("table_retirement_ledger.zig");
        const selected = (try root_mod.member(txn, request.group, request.table)) orelse return error.InvalidCatalogRecord;
        const root = (try root_mod.head(txn, selected.root)) orelse return error.InvalidCatalogRecord;
        if (!std.mem.eql(u8, &lock.id, &root.lookup.id)) return error.InvalidCatalogRecord;
        const candidate = root.survivor orelse return null;
        if (root.phase != .validating_survivors) return error.InvalidCatalogRecord;
        // Root and external parents also hold kind-6 locks, but only actual
        // survivor participants have a common validation dispatch alias.
        const survivor = (try resolveActiveHead(txn, request.group, request.table)) orelse return null;
        if (terminal(survivor) or survivor.table_id != candidate.table or
            !std.mem.eql(u8, &survivor.id, &candidate.id) or !std.mem.eql(u8, &survivor.plan_digest, &candidate.digest) or
            !std.mem.eql(u8, &survivor.metadata_incarnation, &request.metadata_incarnation)) return error.GenerationPublicationChanged;
        try requireParticipantLock(txn, survivor, request.table);
        return .fromHead(survivor);
    }
    if (lock.kind != .schema_validation) return null;
    const head = (try resolveActiveHead(txn, request.group, request.table)) orelse return error.GenerationPublicationChanged;
    if (terminal(head) or !std.mem.eql(u8, &head.id, &lock.id) or
        !std.mem.eql(u8, &head.metadata_incarnation, &request.metadata_incarnation)) return error.GenerationPublicationChanged;
    if (head.table_id != request.table) try requireLock(txn, head);
    return .fromHead(head);
}

pub fn readHead(txn: anytype, lookup: Lookup) !barriers.Head {
    var buf: [192]u8 = undefined;
    const bytes = (try optional(txn, try storageKey(lookup, .head, 0, .ready).encode(&buf))) orelse return error.GenerationPublicationNotFound;
    const head = try barriers.Head.decode(bytes);
    try lookup.require(head);
    return head;
}
pub fn readOwner(txn: anytype, head: barriers.Head, ordinal: u16) !barriers.Owner {
    if (ordinal >= head.owner_count) return error.InvalidGenerationPublication;
    var buf: [192]u8 = undefined;
    const bytes = try txn.get(try storageKey(Lookup.fromHead(head), .owner, ordinal, .ready).encode(&buf));
    const owner = try barriers.Owner.decode(bytes, head);
    if (owner.ordinal != ordinal) return error.InvalidGenerationPublication;
    return owner;
}
pub const OwnerControlRead = @import("schema_validation_read.zig").OwnerControlRead;
pub const OwnerReadRequest = @import("schema_validation_read.zig").OwnerReadRequest;
pub const ownerReadHttpFailure = @import("schema_validation_read.zig").ownerReadHttpFailure;

/// Compact point-read path for authenticated owner dispatch. The immutable
/// plan and cohort are deliberately not decoded for each page/heartbeat.
/// Context.authorize MUST independently rejoin current enrollment, physical
/// root, placement/source authority and reader/standby barriers in THIS read
/// transaction, even for acknowledged/terminal work. No default exists.
/// Returning acknowledged suppresses physical execution, not authentication.
pub fn readOwnerControl(txn: anytype, lookup: Lookup, ordinal: u16, action: contract.Action, context: anytype) !OwnerControlRead {
    const head = try readHead(txn, lookup);
    const owner = try readOwner(txn, head, ordinal);
    if (!terminal(head)) try requireParticipantLock(txn, head, owner.binding.fence.namespace.table_id);
    try context.authorize(txn, head, owner, action);
    return .{ .head = head, .owner = owner, .action = action, .disposition = try barriers.ownerPermission(head, owner, action) };
}

/// Admission input projection. The returned ASVI frame borrows this read
/// transaction: transport must copy/stream it before releasing that snapshot.
/// Re-authenticate before touching the blob; a caller cannot supply a saved
/// OwnerControlRead to bypass a newer cancellation or authority change.
pub fn readOwnerInputs(txn: anytype, lookup: Lookup, ordinal: u16, context: anytype) ![]const u8 {
    const control = try readOwnerControl(txn, lookup, ordinal, .begin, context);
    if (control.disposition != .execute) return error.GenerationPublicationChanged;
    var buf: [192]u8 = undefined;
    const input = try txn.get(try storageKey(lookup, .input, control.owner.input_ordinal, .ready).encode(&buf));
    _ = try contract.inputParts(input);
    if (!std.mem.eql(u8, &contract.hash(input), &control.owner.binding.inputs)) return error.SchemaValidationJobChanged;
    return input;
}

fn requireLock(txn: anytype, head: barriers.Head) !void {
    if (head.retirement_root) |root| {
        try @import("table_retirement_ledger.zig").requireBorrowedLock(txn, root, retirementCohort(head), head.table_id);
    } else {
        const lock = (try lifecycle.read(txn, head.metadata_group_id, head.table_id)) orelse return error.GenerationPublicationChanged;
        if (lock.kind != .schema_validation or !std.mem.eql(u8, &lock.id, &head.id)) return error.GenerationPublicationChanged;
    }
    var buf: [192]u8 = undefined;
    const active = (try optional(txn, try storageKey(Lookup.fromHead(head), .active, 0, .ready).encode(&buf))) orelse return error.GenerationPublicationChanged;
    if (!std.mem.eql(u8, active, &head.id)) return error.GenerationPublicationChanged;
}
fn writeHead(txn: anytype, head: barriers.Head) !void {
    const encoded = try head.encode();
    var buf: [192]u8 = undefined;
    try txn.put(try storageKey(Lookup.fromHead(head), .head, 0, .ready).encode(&buf), encoded.slice());
}
pub fn readParticipants(txn: anytype, head: barriers.Head) !plan_mod.Participants {
    if (std.mem.allEqual(u8, &head.participants_digest, 0)) {
        var single: plan_mod.Participants = .{ .len = 1 };
        single.tables[0] = head.table_id;
        try single.validate();
        return single;
    }
    var buffer: [192]u8 = undefined;
    const result = try plan_mod.ParticipantsRecord.decode(try txn.get(try storageKey(Lookup.fromHead(head), .participants, 0, .ready).encode(&buffer)));
    if (result.len != head.participant_count or result.tables[0] != head.table_id or
        !std.mem.eql(u8, &try result.digest(), &head.participants_digest)) return error.InvalidCatalogRecord;
    return result;
}

fn requireParticipantLock(txn: anytype, head: barriers.Head, table: u64) !void {
    try requireLock(txn, head);
    if (table == head.table_id) return;
    if (std.mem.allEqual(u8, &head.participants_digest, 0)) return error.InvalidGenerationPublication;
    if (head.retirement_root) |root| {
        try @import("table_retirement_ledger.zig").requireBorrowedLock(txn, root, retirementCohort(head), table);
    } else {
        const lock = (try lifecycle.read(txn, head.metadata_group_id, table)) orelse return error.GenerationPublicationChanged;
        if (lock.kind != .schema_validation or !std.mem.eql(u8, &lock.id, &head.id)) return error.GenerationPublicationChanged;
    }
    var buffer: [192]u8 = undefined;
    const alias = try ParticipantAlias.decode(try txn.get(try (Key{ .group = head.metadata_group_id, .table = table, .id = @splat(0), .kind = .active }).encode(&buffer)));
    if (!std.meta.eql(alias, Lookup.fromHead(head))) return error.InvalidCatalogRecord;
}

fn stageTerminal(txn: anytype, head: barriers.Head, foreign_constraints: u16) !void {
    if (!terminal(head)) return;
    try requireLock(txn, head);
    var buf: [192]u8 = undefined;
    if (foreign_constraints != 0)
        try @import("schema_validation_dependencies.zig").releaseJob(txn, head, foreign_constraints);
    if (head.catalogEffect()) {
        // The terminal head is staged before cleanup; validate the retained
        // identity directly rather than treating terminal state as active work.
        const bytes = try txn.get(try resourceActiveKey(head).encode(&buf));
        if (!std.meta.eql(try ParticipantAlias.decode(bytes), Lookup.fromHead(head))) return error.InvalidCatalogRecord;
        try txn.delete(try resourceActiveKey(head).encode(&buf));
    }
    if (head.sequence_retirement != null) {
        for ([_]Kind{ .sequence_namespace, .sequence_database }) |kind| {
            try requireContainerReservation(txn, head, kind);
            try txn.delete(try sequenceContainerKey(head, kind).encode(&buf));
        }
    }
    const participants = try readParticipants(txn, head);
    for (participants.tables[0..participants.len]) |table| {
        if (table == head.table_id) continue;
        try requireParticipantLock(txn, head, table);
        try txn.delete(try (Key{ .group = head.metadata_group_id, .table = table, .id = @splat(0), .kind = .active }).encode(&buf));
        if (head.retirement_root == null or head.phase != .canceled) try lifecycle.release(txn, head.metadata_group_id, table, .{ .kind = .schema_validation, .id = head.id });
    }
    const rename_key = try (Key{ .group = head.metadata_group_id, .table = head.table_id, .id = @splat(0), .kind = .object_rename_source }).encode(&buf);
    if (try optional(txn, rename_key)) |pointer| {
        if (!std.mem.eql(u8, pointer, &head.id)) return error.InvalidGenerationPublication;
        try txn.delete(rename_key);
    }
    try txn.delete(try storageKey(Lookup.fromHead(head), .active, 0, .ready).encode(&buf));
    if (head.retirement_root) |root| {
        if (head.phase == .canceled) try @import("table_retirement_ledger.zig").finishCohort(txn, root, retirementCohort(head), false) else try lifecycle.release(txn, head.metadata_group_id, head.table_id, .{ .kind = .schema_validation, .id = head.id });
    } else try lifecycle.release(txn, head.metadata_group_id, head.table_id, .{ .kind = .schema_validation, .id = head.id });
}

/// Context.admit MUST compare the live catalog/complete owner cover, SQL/RLS
/// authority and capabilities, and verify each preparation against current
/// root enrollment in THIS writer. No validation-only callback default exists.
pub fn checkBegin(alloc: A, txn: anytype, prepared: *const plan_mod.Prepared, preparations: []const evidence.SignedPreparation, inputs: []const []const u8, context: anytype) !bool {
    const lookup = Lookup.fromHead(prepared.head);
    if (preparations.len != prepared.head.owner_count or inputs.len != prepared.head.input_count) return error.InvalidGenerationPublication;
    var decoded = try plan_mod.parse(alloc, prepared.encoded);
    defer decoded.deinit();
    try decoded.value.requireProgress(alloc, prepared.head, prepared.owners);
    if (prepared.head.revision != 1 or prepared.head.phase != .validating) return error.InvalidGenerationPublication;
    const input_digests = try alloc.alloc([32]u8, inputs.len);
    defer alloc.free(input_digests);
    const catalog_digests = try alloc.alloc([32]u8, inputs.len);
    defer alloc.free(catalog_digests);
    const cohort = decoded.value.cohort();
    if (cohort) {
        const participants = prepared.participants orelse return error.InvalidGenerationPublication;
        if (!std.meta.eql(participants, try decoded.value.participantDirectory())) return error.InvalidGenerationPublication;
        if (decoded.value.type_change) |actual| {
            if (prepared.sequence_request != null) return error.InvalidGenerationPublication;
            const types = @import("../system_catalog/named_types.zig");
            const cached = (prepared.type_request orelse return error.InvalidGenerationPublication).value;
            if (!std.mem.eql(u8, &try types.boundDigest(alloc, try types.observation(alloc, cached)), &try types.boundDigest(alloc, try types.observation(alloc, actual.request))) or
                prepared.type_after == null or prepared.type_after.?.id != actual.after.identity.id or prepared.type_after.?.generation != actual.after.identity.generation) return error.InvalidGenerationPublication;
        } else if (decoded.value.retirement_root != null) {
            // The immutable root authority was compared by requireProgress;
            // this cohort has no independent type/sequence mutation intent.
            if (prepared.type_request != null or prepared.type_after != null or prepared.sequence_request != null) return error.InvalidGenerationPublication;
        } else {
            if (prepared.type_request != null or prepared.type_after != null) return error.InvalidGenerationPublication;
            const sequences = @import("../system_catalog/sequence_catalog.zig");
            const cached = (prepared.sequence_request orelse return error.InvalidGenerationPublication).value;
            if (!std.mem.eql(u8, &try sequences.commandDigest(alloc, cached), &try sequences.commandDigest(alloc, decoded.value.sequence_change.?.request))) return error.InvalidGenerationPublication;
        }
    } else if (prepared.participants != null or prepared.type_request != null or prepared.type_after != null or prepared.sequence_request != null) return error.InvalidGenerationPublication;
    const before_public = try alloc.alloc([32]u8, if (cohort) inputs.len else 0);
    defer alloc.free(before_public);
    const after_public = try alloc.alloc([32]u8, if (cohort) inputs.len else 0);
    defer alloc.free(after_public);
    const columns = try decoded.value.selection.encodePayload(alloc);
    defer alloc.free(columns);
    for (inputs, input_digests, catalog_digests, 0..) |input, *input_digest, *catalog_digest, index| {
        const parts = try contract.inputParts(input);
        if (!std.mem.eql(u8, parts[3], columns)) return error.InvalidGenerationPublication;
        if (cohort) {
            before_public[index] = contract.hash(parts[0]);
            after_public[index] = contract.hash(parts[1]);
        } else if (!std.mem.eql(u8, parts[0], decoded.value.before.schema_json) or !std.mem.eql(u8, parts[1], decoded.value.after.schema_json)) return error.InvalidGenerationPublication;
        input_digest.* = contract.hash(input);
        catalog_digest.* = contract.hash(parts[2]);
    }
    for (prepared.owners) |owner| {
        if (owner.root_epoch != 0) return error.InvalidGenerationPublication;
        if (!std.mem.eql(u8, &owner.binding.inputs, &input_digests[owner.input_ordinal]) or
            !std.mem.eql(u8, &owner.binding.fence.catalog_digest, &catalog_digests[owner.input_ordinal])) return error.InvalidGenerationPublication;
        if (cohort and (!std.mem.eql(u8, &owner.binding.before_public, &before_public[owner.input_ordinal]) or
            !std.mem.eql(u8, &owner.binding.after_public, &after_public[owner.input_ordinal]))) return error.InvalidGenerationPublication;
    }
    var buf: [192]u8 = undefined;
    if (try optional(txn, try storageKey(lookup, .head, 0, .ready).encode(&buf)) != null) {
        var retained = try recover(alloc, txn, lookup);
        defer retained.deinit();
        const original = try txn.get(try storageKey(lookup, .plan, 0, .ready).encode(&buf));
        if (!std.mem.eql(u8, original, prepared.encoded)) return error.GenerationPublicationChanged;
        // Exact lost-begin reply. Never rerun old DDL admission against a later
        // catalog or overwrite its original preparation with a newer reporter.
        // This is a no-op observation, not permission to execute owner work.
        return false;
    }
    if (try optional(txn, try storageKey(lookup, .active, 0, .ready).encode(&buf)) != null) return error.GenerationPublicationChanged;
    for (preparations, prepared.owners) |signed, owner| {
        try signed.verifyFor(owner.binding, owner.physical_root_incarnation);
        if (!std.mem.eql(u8, &signed.proof.identity.metadata_incarnation, &prepared.head.metadata_incarnation)) return error.MetadataIncarnationMismatch;
    }
    if (decoded.value.sequence_change) |change| {
        if (try optional(txn, try sequenceOutcomeKey(lookup.group, change.before.id, change.request.admission.nonce).encode(&buf)) != null) return error.GenerationPublicationChanged;
    }
    try context.admit(txn, decoded.value, preparations);
    if (prepared.head.type_id != 0 and try activeType(txn, lookup.group, prepared.head.type_id) != null) return error.GenerationPublicationChanged;
    if (prepared.head.sequence_retirement) |retirement| if (try activeSequence(txn, lookup.group, retirement.id) != null) return error.GenerationPublicationChanged;
    if (prepared.participants) |directory| {
        for (directory.tables[0..directory.len]) |table| {
            const lock = try lifecycle.read(txn, lookup.group, table);
            if (prepared.head.retirement_root) |root| {
                const selected = (try @import("table_retirement_ledger.zig").member(txn, lookup.group, table)) orelse return error.GenerationPublicationChanged;
                if (!std.meta.eql(selected.root, root.root) or lock == null or lock.?.kind != .table_retirement or !std.mem.eql(u8, &lock.?.id, &root.root.id)) return error.GenerationPublicationChanged;
            } else if (lock != null) return error.GenerationPublicationChanged;
        }
    } else if (try lifecycle.read(txn, lookup.group, lookup.table) != null) return error.GenerationPublicationChanged;
    return true;
}

/// Preflight is read-only. Replicated application repeats it in the writer;
/// its result must never be retained as authority across transaction snapshots.
pub fn begin(alloc: A, txn: anytype, prepared: *const plan_mod.Prepared, preparations: []const evidence.SignedPreparation, inputs: []const []const u8, context: anytype) !void {
    if (!try checkBegin(alloc, txn, prepared, preparations, inputs, context)) return;
    const lookup = Lookup.fromHead(prepared.head);
    var buf: [192]u8 = undefined;
    // No expected admission/CAS rejection may be swallowed after this point.
    if (prepared.type_request) |request| {
        const Context = switch (@typeInfo(@TypeOf(context))) {
            .pointer => |pointer| pointer.child,
            else => @TypeOf(context),
        };
        if (comptime @hasDecl(Context, "stageTypeAdmission")) {
            try context.stageTypeAdmission(txn, request.value, prepared.type_after.?, prepared.head);
        } else return error.InvalidGenerationPublication;
    }
    var sequence_revision: u64 = 0;
    if (prepared.sequence_request) |request| {
        const Context = switch (@typeInfo(@TypeOf(context))) {
            .pointer => |pointer| pointer.child,
            else => @TypeOf(context),
        };
        if (comptime @hasDecl(Context, "stageSequenceAdmission")) {
            sequence_revision = try context.stageSequenceAdmission(txn, request.value, prepared.head);
        } else return error.InvalidGenerationPublication;
    }
    if (prepared.sequence_request) |request| {
        const accepted: SequenceAcceptance = .{ .lookup = lookup, .admission = request.value.admission, .digest = prepared.head.sequence_retirement.?.admission_digest, .revision = sequence_revision };
        try accepted.validate();
        const encoded = try SequenceAcceptanceRecord.init(accepted);
        try txn.put(try sequenceOutcomeKey(lookup.group, prepared.head.sequence_retirement.?.id, request.value.admission.nonce).encode(&buf), encoded.slice());
    }
    if (prepared.head.catalogEffect()) {
        const reservation = try ParticipantAlias.init(lookup);
        try txn.put(try resourceActiveKey(prepared.head).encode(&buf), reservation.slice());
    }
    if (prepared.head.sequence_retirement != null) {
        const reservation = try ParticipantAlias.init(lookup);
        for ([_]Kind{ .sequence_namespace, .sequence_database }) |kind|
            try txn.put(try sequenceContainerKey(prepared.head, kind).encode(&buf), reservation.slice());
    }
    if (prepared.head.retirement_root) |root| {
        try @import("table_retirement_ledger.zig").stageCohort(txn, root, retirementCohort(prepared.head));
    } else try lifecycle.acquire(txn, lookup.group, lookup.table, .{ .kind = .schema_validation, .id = lookup.id });
    if (prepared.participants) |directory| {
        const encoded = try plan_mod.ParticipantsRecord.init(directory);
        try txn.put(try storageKey(lookup, .participants, 0, .ready).encode(&buf), encoded.slice());
        for (directory.tables[0..directory.len]) |table| {
            if (table == lookup.table) continue;
            if (prepared.head.retirement_root == null) try lifecycle.acquire(txn, lookup.group, table, .{ .kind = .schema_validation, .id = lookup.id });
            const alias = try ParticipantAlias.init(lookup);
            try txn.put(try (Key{ .group = lookup.group, .table = table, .id = @splat(0), .kind = .active }).encode(&buf), alias.slice());
        }
    }
    if (prepared.owners[0].binding.purpose == .foreign_key_source) {
        const parts = try contract.inputParts(inputs[0]);
        var source = try std.json.parseFromSlice(contract.ForeignKeySource, alloc, parts[3], .{ .ignore_unknown_fields = false });
        defer source.deinit();
        try @import("schema_validation_dependencies.zig").acquire(alloc, txn, prepared.head, source.value);
    }
    try txn.put(try storageKey(lookup, .plan, 0, .ready).encode(&buf), prepared.encoded);
    for (inputs, 0..) |input, ordinal| try txn.put(try storageKey(lookup, .input, @intCast(ordinal), .ready).encode(&buf), input);
    for (prepared.owners, preparations) |owner, signed| {
        const encoded = try owner.encode(prepared.head);
        try txn.put(try storageKey(lookup, .owner, owner.ordinal, .ready).encode(&buf), encoded.slice());
        const address = OwnerAddress.fromBinding(owner.binding);
        const index = try NamespaceOwnerRecord.init(.{ .address = address, .ordinal = owner.ordinal });
        const index_key = try namespaceOwnerKey(lookup, address).encode(&buf);
        if (try optional(txn, index_key) != null) return error.InvalidGenerationPublication;
        try txn.put(index_key, index.slice());
        const preparation = try PreparationRecord.init(signed);
        try txn.put(try storageKey(lookup, .preparation, owner.ordinal, .ready).encode(&buf), preparation.slice());
    }
    if (prepared.owners[0].binding.purpose == .object_rename) try txn.put(try (Key{ .group = lookup.group, .table = lookup.table, .id = @splat(0), .kind = .object_rename_source }).encode(&buf), &lookup.id);
    try writeHead(txn, prepared.head);
    try txn.put(try storageKey(lookup, .active, 0, .ready).encode(&buf), &lookup.id);
}

fn stageFor(header: contract.Header) !barriers.Stage {
    return switch (header.phase) {
        .ready => .ready,
        .installed => .installed,
        .released => .released,
        .canceled => .canceled,
        .failed => .failed,
        else => error.SchemaValidationNotReady,
    };
}

/// Stable expected acceptance before submission. A caller retains this across
/// lost replies; neither it nor a matching historical receipt grants execution.
pub fn acknowledgementReceipt(lookup: Lookup, ordinal: u16, signed: evidence.SignedObservation) !@import("schema_validation_receipt.zig").Request {
    const header = switch (signed) {
        inline else => |value| value.rooted.observation.header orelse return error.InvalidGenerationPublication,
    };
    const proof = try ProofRecord.init(signed);
    const result: @import("schema_validation_receipt.zig").Request = .{ .lookup = lookup, .expected = .{ .acknowledgement = .{
        .ordinal = ordinal,
        .stage = try stageFor(header),
        .header_digest = try header.digest(),
        .proof_digest = contract.hash(proof.slice()),
    } } };
    try result.validate();
    return result;
}

pub fn commandLookup(command: Command) !Lookup {
    const lookup = switch (command) {
        .begin => |request| Lookup{
            .group = request.plan.metadata_group_id,
            .table = request.plan.before.table_id,
            .id = request.plan.id,
            .metadata_incarnation = request.plan.metadata_incarnation,
            .plan_digest = try request.plan.digest(),
        },
        .ack => |request| request.lookup,
        .authorize_parent => |request| request.lookup,
        .adopt_root => |request| request.lookup,
        .install, .publish => |decision| decision.lookup,
        .cancel => |request| request.decision.lookup,
    };
    try (@import("schema_validation_read.zig").OwnerReadRequest{ .lookup = lookup, .ordinal = 0 }).validate();
    return lookup;
}

fn parentKey(request: parent.Request) !Key {
    var key = storageKey(request.lookup, .parent, request.ordinal, .ready);
    key.grant = try request.id();
    return key;
}

fn rootKey(lookup: Lookup, ordinal: u16, epoch: u64) !Key {
    if (epoch == 0) return error.InvalidGenerationPublication;
    var result = storageKey(lookup, .root_epoch, ordinal, .ready);
    var number: [32]u8 = @splat(0);
    std.mem.writeInt(u64, number[24..32], epoch, .big);
    result.grant = number;
    return result;
}

fn historyKey(lookup: Lookup, ordinal: u16, stage: barriers.Stage, digest: [32]u8) Key {
    var result = storageKey(lookup, .proof_history, ordinal, stage);
    result.grant = digest;
    return result;
}

fn readRawParent(txn: anytype, lookup: Lookup, ordinal: u16, grant: [32]u8) !?parent.Request {
    var key = storageKey(lookup, .parent, ordinal, .ready);
    key.grant = grant;
    var buf: [192]u8 = undefined;
    const bytes = (try optional(txn, try key.encode(&buf))) orelse return null;
    const result = try parent.Record.decode(bytes);
    if (!std.meta.eql(result.lookup, lookup) or result.ordinal != ordinal or
        !std.mem.eql(u8, &try result.id(), &grant)) return error.InvalidCatalogRecord;
    return result;
}

fn readRawEpoch(txn: anytype, lookup: Lookup, ordinal: u16, epoch: u64) !roots.Epoch {
    var buf: [192]u8 = undefined;
    const result = try roots.EpochRecord.decode(try txn.get(try (try rootKey(lookup, ordinal, epoch)).encode(&buf)));
    try result.request.validateShape();
    if (!std.meta.eql(result.request.lookup, lookup) or result.request.ordinal != ordinal or result.number() != epoch)
        return error.InvalidCatalogRecord;
    return result;
}

/// One predecessor point read, never a walk proportional to failover history.
/// The checkpoint reverse pass independently verifies EVERY retained epoch.
pub fn readRootEpoch(txn: anytype, head: barriers.Head, owner: barriers.Owner, epoch: u64) !roots.Epoch {
    try owner.validate(head);
    if (epoch == 0 or epoch > owner.root_epoch) return error.InvalidCatalogRecord;
    const lookup = Lookup.fromHead(head);
    const result = try readRawEpoch(txn, lookup, owner.ordinal, epoch);
    const retained: ?parent.Request = switch (result.request.lineage) {
        .retained_parent => |grant| (try readRawParent(txn, lookup, owner.ordinal, grant)) orelse return error.InvalidCatalogRecord,
        .raft_replay => null,
        .native_recovery => null,
    };
    try result.validate(head, retained);
    if (result.source_owner.initialRoot() != owner.initialRoot()) return error.InvalidCatalogRecord;
    if (epoch == 1) {
        var buf: [192]u8 = undefined;
        const preparation = try PreparationRecord.decode(try txn.get(try storageKey(lookup, .preparation, owner.ordinal, .ready).encode(&buf)));
        try preparation.verifyFor(owner.binding, result.source_owner.physical_root_incarnation);
        if (!std.mem.eql(u8, &preparation.proof.identity.metadata_incarnation, &head.metadata_incarnation)) return error.MetadataIncarnationMismatch;
        if (result.source_owner.root_epoch != 0) return error.InvalidCatalogRecord;
    } else {
        const previous = try readRawEpoch(txn, lookup, owner.ordinal, epoch - 1);
        if (result.source_owner.root_epoch != epoch - 1 or result.request.revision <= previous.request.revision or
            result.source_owner.physical_root_incarnation != previous.request.physicalRoot() or
            !std.mem.eql(u8, &result.source_owner.root_digest, &try previous.digest())) return error.InvalidCatalogRecord;
    }
    const digest = try result.digest();
    if (epoch == owner.root_epoch) {
        if (owner.physical_root_incarnation != result.request.physicalRoot() or
            !std.mem.eql(u8, &owner.root_digest, &digest)) return error.InvalidCatalogRecord;
    } else {
        const next = try readRawEpoch(txn, lookup, owner.ordinal, epoch + 1);
        if (next.source_owner.root_epoch != epoch or next.source_owner.physical_root_incarnation != result.request.physicalRoot() or
            !std.mem.eql(u8, &next.source_owner.root_digest, &digest)) return error.InvalidCatalogRecord;
    }
    return result;
}

/// Root-only historical projection. It deliberately carries NO accepted
/// phases and must never be used as execution or phase-publication authority.
fn historicalRootOwner(txn: anytype, head: barriers.Head, owner: barriers.Owner, epoch: u64) !barriers.Owner {
    if (epoch > owner.root_epoch) return error.InvalidCatalogRecord;
    var result: barriers.Owner = .{ .ordinal = owner.ordinal, .input_ordinal = owner.input_ordinal, .binding = owner.binding, .physical_root_incarnation = owner.initialRoot() };
    if (epoch == 0) {
        if (owner.root_epoch != 0) {
            var buf: [192]u8 = undefined;
            const preparation = try PreparationRecord.decode(try txn.get(try storageKey(Lookup.fromHead(head), .preparation, owner.ordinal, .ready).encode(&buf)));
            try preparation.verifyFor(owner.binding, owner.initialRoot());
            if (!std.mem.eql(u8, &preparation.proof.identity.metadata_incarnation, &head.metadata_incarnation)) return error.MetadataIncarnationMismatch;
        }
        return result;
    }
    // Resolve once: root identity and commitment come from the SAME owned
    // epoch record, without repeating its parent/predecessor point reads.
    const retained = try readRootEpoch(txn, head, owner, epoch);
    result.physical_root_incarnation = retained.request.physicalRoot();
    result.root_epoch = epoch;
    result.root_origin = owner.initialRoot();
    result.root_digest = try retained.digest();
    return result;
}

pub fn verifiedStage(txn: anytype, head: barriers.Head, owner: barriers.Owner, stage: barriers.Stage, signed: evidence.SignedObservation) !evidence.VerifiedObservation {
    // The normal, current-root path keeps its original three point reads.
    // Only a retained old-root phase needs the immutable epoch resolver.
    if (owner.proof_epochs[@backingInt(stage)] == owner.root_epoch) return verified(signed, owner);
    const historic = try historicalRootOwner(txn, head, owner, owner.proof_epochs[@backingInt(stage)]);
    return verified(signed, historic);
}

fn verifyEpochArchives(txn: anytype, head: barriers.Head, owner: barriers.Owner, epoch: roots.Epoch) !void {
    const before = epoch.source_owner;
    var required = if (epoch.request.lineage != .retained_parent) try originalReplayCut(txn, head, before) else null;
    var buf: [192]u8 = undefined;
    for (std.meta.tags(barriers.Stage)) |stage| {
        if (!before.has(stage)) continue;
        const i = @backingInt(stage);
        const history = try roots.HistoricalProofRecord.decode(try txn.get(try historyKey(epoch.request.lookup, before.ordinal, stage, before.proofs[i]).encode(&buf)));
        if (history.epoch != before.proof_epochs[i] or history.first_saved_epoch == 0 or history.first_saved_epoch > epoch.number() or
            history.epoch > owner.root_epoch) return error.InvalidCatalogRecord;
        const checked = try verifiedStage(txn, head, before, stage, history.signed);
        if (!std.mem.eql(u8, &signedIdentity(history.signed).metadata_incarnation, &head.metadata_incarnation)) return error.MetadataIncarnationMismatch;
        if (try stageFor(checked.header) != stage or !std.mem.eql(u8, &checked.root_commitment, &before.proofs[i]) or
            !std.mem.eql(u8, &checked.logical_digest, &before.logical[i]) or !std.mem.eql(u8, &try checked.header.digest(), &before.headers[i]))
            return error.InvalidCatalogRecord;
        if (epoch.request.lineage != .retained_parent) required = try roots.includeCut(required, try replayCut(checked.header.updated));
    }
    if (epoch.request.lineage != .retained_parent and !std.meta.eql(required, epoch.required_cut)) return error.InvalidCatalogRecord;
}

fn replayCut(position: local.storage_db_receipt_position.Position) !roots.Cut {
    try position.validate();
    return position;
}

fn originalReplayCut(txn: anytype, head: barriers.Head, owner: barriers.Owner) !?roots.Cut {
    var buf: [192]u8 = undefined;
    const preparation = try PreparationRecord.decode(try txn.get(try storageKey(Lookup.fromHead(head), .preparation, owner.ordinal, .ready).encode(&buf)));
    try preparation.verifyFor(owner.binding, owner.initialRoot());
    if (!std.mem.eql(u8, &preparation.proof.identity.metadata_incarnation, &head.metadata_incarnation)) return error.MetadataIncarnationMismatch;
    return if (preparation.value.position) |cut| try replayCut(cut) else null;
}

/// Fixed work per owner (at most five signed phases), never a scan of the
/// immutable cohort or Raft log. Epoch staging archives the exact same proofs.
fn replayRequiredCut(txn: anytype, head: barriers.Head, owner: barriers.Owner) !?roots.Cut {
    var required = try originalReplayCut(txn, head, owner);
    var buf: [192]u8 = undefined;
    for (std.meta.tags(barriers.Stage)) |stage| {
        if (!owner.has(stage)) continue;
        const signed = try ProofRecord.decode(try txn.get(try storageKey(Lookup.fromHead(head), .proof, owner.ordinal, stage).encode(&buf)));
        const checked = try verifiedStage(txn, head, owner, stage, signed);
        const i = @backingInt(stage);
        if (try stageFor(checked.header) != stage or !std.mem.eql(u8, &checked.root_commitment, &owner.proofs[i]) or
            !std.mem.eql(u8, &checked.logical_digest, &owner.logical[i]) or
            !std.mem.eql(u8, &try checked.header.digest(), &owner.headers[i]) or
            !std.mem.eql(u8, &signedIdentity(signed).metadata_incarnation, &head.metadata_incarnation)) return error.InvalidCatalogRecord;
        required = try roots.includeCut(required, try replayCut(checked.header.updated));
    }
    return required;
}

fn validateRetainedParent(txn: anytype, head: barriers.Head, owner: barriers.Owner, request: parent.Request) !void {
    try request.requireScope(head, try historicalRootOwner(txn, head, owner, request.root_epoch));
    try requireParentPreparationPrefix(txn, owner, request);
}

fn requireParentPreparationPrefix(txn: anytype, owner: barriers.Owner, request: parent.Request) !void {
    var buf: [192]u8 = undefined;
    const preparation = try PreparationRecord.decode(try txn.get(try storageKey(request.lookup, .preparation, owner.ordinal, .ready).encode(&buf)));
    try preparation.verifyFor(owner.binding, owner.initialRoot());
    if (!std.mem.eql(u8, &preparation.proof.identity.metadata_incarnation, &request.lookup.metadata_incarnation))
        return error.MetadataIncarnationMismatch;
    if (preparation.value.position) |position| {
        if (position != .raft or position.raft.index > request.signed.value.through_index or
            position.raft.term > request.signed.value.completed.term) return error.InvalidControlReceiptPosition;
    }
}

pub fn readParentAuthorization(txn: anytype, lookup: Lookup, ordinal: u16, grant: [32]u8) !?parent.Request {
    const head = try readHead(txn, lookup);
    const owner = try readOwner(txn, head, ordinal);
    const retained = (try readRawParent(txn, lookup, ordinal, grant)) orelse return null;
    try validateRetainedParent(txn, head, owner, retained);
    return retained;
}

fn parentImageKey(lookup: Lookup, image: @import("schema_validation_read.zig").ImageIdentity) !Key {
    return .{ .group = lookup.group, .table = lookup.table, .id = lookup.id, .kind = .parent_image, .grant = try image.digest() };
}

fn namespaceOwnerKey(lookup: Lookup, address: OwnerAddress) Key {
    return .{ .group = lookup.group, .table = lookup.table, .id = lookup.id, .kind = .owner_namespace, .grant = address.digest() };
}

pub fn readReplayControl(txn: anytype, lookup: Lookup, address: OwnerAddress) !?@import("schema_validation_read.zig").ReplayControl {
    try (@import("schema_validation_read.zig").OwnerReadRequest{ .lookup = lookup, .ordinal = 0, .projection = .replay, .source_owner = address }).validate();
    const head = try readHead(txn, lookup);
    if (std.mem.allEqual(u8, &head.participants_digest, 0) and std.mem.readInt(u64, address.namespace[0..8], .big) != head.table_id)
        return error.InvalidGenerationPublication;
    var buf: [192]u8 = undefined;
    const bytes = (try optional(txn, try namespaceOwnerKey(lookup, address).encode(&buf))) orelse return null;
    const index = try NamespaceOwnerRecord.decode(bytes);
    if (!std.meta.eql(index.address, address)) return error.InvalidCatalogRecord;
    const result: @import("schema_validation_read.zig").ReplayControl = .{ .head = head, .owner = try readOwner(txn, head, index.ordinal) };
    try result.requireFor(lookup, address);
    return result;
}

pub fn readReplayInputs(txn: anytype, lookup: Lookup, address: OwnerAddress, ordinal: u16) ![]const u8 {
    const control = (try readReplayControl(txn, lookup, address)) orelse return error.SchemaValidationNotReady;
    if (control.owner.ordinal != ordinal) return error.SchemaValidationJobChanged;
    if (terminal(control.head)) return error.GenerationPublicationChanged;
    try requireLock(txn, control.head);
    var buf: [192]u8 = undefined;
    const bytes = try txn.get(try storageKey(lookup, .input, control.owner.input_ordinal, .ready).encode(&buf));
    _ = try contract.inputParts(bytes);
    if (!std.mem.eql(u8, &contract.hash(bytes), &control.owner.binding.inputs)) return error.InvalidCatalogRecord;
    return bytes;
}

/// Bounded historical lookup. The image index and its accepted parent are read
/// from the same metadata snapshot and checked against their complete identity.
/// Returning a retained grant does not authorize destination execution/adoption.
pub fn readParentByImage(txn: anytype, lookup: Lookup, image: @import("schema_validation_read.zig").ImageIdentity) !?parent.Request {
    try (@import("schema_validation_read.zig").OwnerReadRequest{ .lookup = lookup, .ordinal = 0, .projection = .parent, .source_image = image }).validate();
    _ = try readHead(txn, lookup);
    var buf: [192]u8 = undefined;
    const bytes = (try optional(txn, try (try parentImageKey(lookup, image)).encode(&buf))) orelse return null;
    const index = try ParentImageRecord.decode(bytes);
    if (index.ordinal >= barriers.max_owners or std.mem.allEqual(u8, &index.grant, 0)) return error.InvalidCatalogRecord;
    const request = (try readParentAuthorization(txn, lookup, index.ordinal, index.grant)) orelse return error.InvalidCatalogRecord;
    if (!std.meta.eql(try request.image(), image)) return error.InvalidCatalogRecord;
    return request;
}

pub fn readRestorationControl(txn: anytype, lookup: Lookup, image: @import("schema_validation_read.zig").ImageIdentity) !?@import("schema_validation_read.zig").RestorationControl {
    const retained = (try readParentByImage(txn, lookup, image)) orelse return null;
    const head = try readHead(txn, lookup);
    const result: @import("schema_validation_read.zig").RestorationControl = .{
        .head = head,
        .owner = try readOwner(txn, head, retained.ordinal),
        .parent = retained,
    };
    try result.requireFor(lookup, image);
    return result;
}

/// Immutable restore inputs are available after source loss even when that
/// owner phase was already acknowledged. The retained image authorization
/// supplies the exact input owner; this is NOT ordinary execution permission.
pub fn readRecoveryInputs(txn: anytype, lookup: Lookup, ordinal: u16, image: @import("schema_validation_read.zig").ImageIdentity) ![]const u8 {
    const head = try readHead(txn, lookup);
    if (terminal(head)) return error.GenerationPublicationChanged;
    try requireLock(txn, head);
    const request = (try readParentByImage(txn, lookup, image)) orelse return error.SchemaValidationNotReady;
    if (request.ordinal != ordinal) return error.SchemaValidationJobChanged;
    const owner = try readOwner(txn, head, ordinal);
    var buf: [192]u8 = undefined;
    const input = try txn.get(try storageKey(lookup, .input, owner.input_ordinal, .ready).encode(&buf));
    _ = try contract.inputParts(input);
    if (!std.mem.eql(u8, &contract.hash(input), &request.signed.value.binding.inputs)) return error.InvalidCatalogRecord;
    return input;
}

pub const PreparedRootAdoption = struct {
    head: barriers.Head,
    epoch: ?roots.Epoch = null,
    owner: ?barriers.OwnerRecord = null,

    /// Authentication, predecessor checks and staging share THIS writer.
    /// Any failed put aborts the whole outer native transaction.
    pub fn stage(self: *const @This(), txn: anytype) !barriers.Head {
        const epoch = self.epoch orelse return self.head;
        const lookup = epoch.request.lookup;
        const before = epoch.source_owner;
        var buf: [192]u8 = undefined;
        for (std.meta.tags(barriers.Stage)) |phase_stage| {
            if (!before.has(phase_stage)) continue;
            const signed = try ProofRecord.decode(try txn.get(try storageKey(lookup, .proof, before.ordinal, phase_stage).encode(&buf)));
            const checked = try verifiedStage(txn, self.head, before, phase_stage, signed);
            const i = @backingInt(phase_stage);
            if (!std.mem.eql(u8, &checked.root_commitment, &before.proofs[i]) or
                !std.mem.eql(u8, &try checked.header.digest(), &before.headers[i])) return error.InvalidCatalogRecord;
            const record = try roots.HistoricalProofRecord.init(.{ .epoch = before.proof_epochs[i], .first_saved_epoch = epoch.number(), .signed = signed });
            const key = try historyKey(lookup, before.ordinal, phase_stage, before.proofs[i]).encode(&buf);
            if (try optional(txn, key)) |prior| {
                const stored = try roots.HistoricalProofRecord.decode(prior);
                if (stored.epoch != before.proof_epochs[i] or stored.first_saved_epoch == 0 or stored.first_saved_epoch > epoch.number() or
                    !std.meta.eql(stored.signed, signed)) return error.InvalidCatalogRecord;
            } else try txn.put(key, record.slice());
        }
        const record = try roots.EpochRecord.init(epoch);
        try txn.put(try (try rootKey(lookup, before.ordinal, epoch.number())).encode(&buf), record.slice());
        try txn.put(try storageKey(lookup, .owner, before.ordinal, .ready).encode(&buf), self.owner.?.slice());
        try writeHead(txn, self.head);
        return self.head;
    }
};

/// Exact retries observe the immutable accepted record without requiring the
/// old source or a currently live destination. A stale missing epoch remains
/// unknown; it cannot allocate a new epoch or reset the accepted phase counts.
pub fn prepareRootAdoption(txn: anytype, request: roots.Request, context: anytype) !PreparedRootAdoption {
    try request.validateShape();
    const head = try readHead(txn, request.lookup);
    const owner = try readOwner(txn, head, request.ordinal);
    const number = request.expected_epoch + 1;
    if (owner.root_epoch >= number) {
        var buf: [192]u8 = undefined;
        if (try optional(txn, try (try rootKey(request.lookup, request.ordinal, number)).encode(&buf)) == null) return error.MetadataMutationOutcomeUnknown;
        const prior = try readRootEpoch(txn, head, owner, number);
        try verifyEpochArchives(txn, head, owner, prior);
        if (!std.mem.eql(u8, &try prior.request.digest(), &try request.digest())) return error.GenerationPublicationChanged;
        return .{ .head = head };
    }
    try requireLock(txn, head);
    if (head.revision != request.revision or owner.root_epoch != request.expected_epoch or
        !std.mem.eql(u8, &request.owner_digest, &try parent.ownerDigest(owner, head))) return error.GenerationPublicationChanged;
    const retained: ?parent.Request = switch (request.lineage) {
        .retained_parent => |grant| (try readParentAuthorization(txn, request.lookup, request.ordinal, grant)) orelse return error.GenerationPublicationChanged,
        .raft_replay => null,
        .native_recovery => null,
    };
    try request.requireDestination(head, owner, retained);
    switch (request.proposal) {
        .audited => |value| {
            const checked = try context.verifyAdoption(txn, head, owner, evidence.SignedObservation{ .raft = value.observation });
            if (checked.physical_root_incarnation != value.destination.value.physical_root or
                !std.mem.eql(u8, &try checked.header.digest(), &try value.destination.value.rows.header.digest())) return error.SchemaValidationJobChanged;
        },
        .bootstrap => |value| try context.verifyBootstrap(txn, head, owner, value),
        .audited_native => |value| {
            const checked = try context.verifyAdoption(txn, head, owner, evidence.SignedObservation{ .native = value.observation });
            if (checked.physical_root_incarnation != value.destination.value.physical_root or
                !std.meta.eql(checked.header, value.destination.value.rows.header)) return error.SchemaValidationJobChanged;
        },
        .native_bootstrap => |value| try context.verifyNativeBootstrap(txn, head, owner, value),
    }
    const epoch: roots.Epoch = .{ .request = request, .source_owner = owner, .required_cut = if (request.lineage != .retained_parent) try replayRequiredCut(txn, head, owner) else null };
    var next = head;
    next.revision = try std.math.add(u64, next.revision, 1);
    try epoch.validate(next, retained);
    const successor = try epoch.successor(next);
    return .{ .head = next, .epoch = epoch, .owner = try successor.encode(next) };
}

pub fn adoptRoot(txn: anytype, request: roots.Request, context: anytype) !barriers.Head {
    const prepared = try prepareRootAdoption(txn, request, context);
    return prepared.stage(txn);
}

pub const PreparedParentAuthorization = struct {
    head: barriers.Head,
    key: Key,
    record: ?parent.Record = null,
    image: ?struct { key: Key, record: ParentImageRecord } = null,

    /// Same writer snapshot only. Read-only preflight must discard this value;
    /// replicated application repeats all current authority checks afresh.
    pub fn stage(self: *const @This(), txn: anytype) !barriers.Head {
        if (self.record) |record| {
            var buf: [192]u8 = undefined;
            try txn.put(try self.key.encode(&buf), record.slice());
        }
        if (self.image) |image| {
            var buf: [192]u8 = undefined;
            try txn.put(try image.key.encode(&buf), image.record.slice());
        }
        return self.head;
    }
};

/// One immutable authorization per exact request digest. New acceptance joins
/// live source facts to the owner/head CAS in THIS transaction. Lost replies
/// are historical observations: they need no live source and never grant
/// execution permission. No plan/corpus scan or head-counter mutation occurs.
pub fn prepareParentAuthorization(txn: anytype, request: parent.Request, context: anytype) !PreparedParentAuthorization {
    try request.validateShape();
    const head = try readHead(txn, request.lookup);
    const owner = try readOwner(txn, head, request.ordinal);
    const key = try parentKey(request);
    const record = try parent.Record.init(request);
    var buf: [192]u8 = undefined;
    if (try optional(txn, try key.encode(&buf))) |prior| {
        if (!std.mem.eql(u8, prior, record.slice())) return error.InvalidCatalogRecord;
        try validateRetainedParent(txn, head, owner, request);
        if (try readParentByImage(txn, request.lookup, try request.image()) == null) return error.InvalidCatalogRecord;
        return .{ .head = head, .key = key };
    }
    // Terminal retirement may have discharged an unobserved producer attempt.
    // Preserve historical exact receipts above, but never create a new parent
    // after the all-owner terminal barrier, even if a stale lock is present.
    if (terminal(head)) return error.GenerationPublicationChanged;
    try requireLock(txn, head);
    try request.requireCurrent(head, owner);
    try requireParentPreparationPrefix(txn, owner, request);
    try context.verifyParent(txn, owner.binding, owner.physical_root_incarnation, request.signed, head.reader_requirements);
    const image_key = try parentImageKey(request.lookup, try request.image());
    const image: @FieldType(PreparedParentAuthorization, "image") = if (try readParentByImage(txn, request.lookup, try request.image()) != null) null else .{ .key = image_key, .record = try ParentImageRecord.init(.{ .ordinal = request.ordinal, .grant = try request.id() }) };
    return .{ .head = head, .key = key, .record = record, .image = image };
}

pub fn authorizeParent(txn: anytype, request: parent.Request, context: anytype) !barriers.Head {
    const prepared = try prepareParentAuthorization(txn, request, context);
    return prepared.stage(txn);
}

/// Derive exact acceptance from the retained command and returned durable
/// head, not from a server-selected effect. Decisions keep their original
/// revision/digest even when subsequent owner phases have already advanced.
pub fn commandReceipt(command: Command, head: barriers.Head) !@import("schema_validation_receipt.zig").Request {
    const lookup = try commandLookup(command);
    try lookup.require(head);
    const result: @import("schema_validation_receipt.zig").Request = .{
        .lookup = lookup,
        .expected = switch (command) {
            .begin => .admission,
            .ack => |request| (try acknowledgementReceipt(lookup, request.ordinal, request.signed)).expected,
            .authorize_parent => |request| .{ .parent_authorization = .{ .ordinal = request.ordinal, .grant = try request.id() } },
            .adopt_root => |request| blk: {
                const digest = try request.digest();
                break :blk .{ .root_adoption = .{ .ordinal = request.ordinal, .epoch = request.expected_epoch + 1, .request_digest = digest } };
            },
            .install => |decision| .{ .installation = .{ .revision = decision.revision, .digest = try head.installDecision() } },
            .publish => |decision| .{ .publication = .{ .revision = decision.revision, .digest = try head.publicationDecision() } },
            .cancel => |request| .{ .cancellation = .{ .decision = .{ .revision = request.decision.revision, .digest = try head.cancellationDecision() }, .failure = request.failure } },
        },
    };
    if (!try (@import("schema_validation_receipt.zig").Observation{ .lookup = lookup, .head = head, .observed = result.expected }).isAcknowledged(result)) return error.InvalidGenerationPublication;
    return result;
}

/// Resolve an uncertain coordinator submission from one durable snapshot.
/// No native liveness, root adoption, plan decode, or execution permission is
/// involved. Admission is idempotent by immutable plan; ACKs require the exact
/// retained signed proof. A later re-attestation may conservatively leave an
/// old ACK unknown, but cannot make a different witness look accepted.
pub fn observeReceipt(txn: anytype, request: @import("schema_validation_receipt.zig").Request) !@import("schema_validation_receipt.zig").Observation {
    try request.validate();
    var result: @import("schema_validation_receipt.zig").Observation = .{ .lookup = request.lookup };
    const head = readHead(txn, request.lookup) catch |err| switch (err) {
        error.GenerationPublicationNotFound => return result,
        else => return err,
    };
    result.head = head;
    switch (request.expected) {
        .admission => result.observed = .admission,
        .parent_authorization => |wanted| {
            if (try readParentAuthorization(txn, request.lookup, wanted.ordinal, wanted.grant) != null)
                result.observed = .{ .parent_authorization = wanted };
        },
        .root_adoption => |wanted| {
            const owner = try readOwner(txn, head, wanted.ordinal);
            if (wanted.epoch > owner.root_epoch) return result;
            var buf: [192]u8 = undefined;
            if (try optional(txn, try (try rootKey(request.lookup, wanted.ordinal, wanted.epoch)).encode(&buf)) == null) return result;
            const epoch = try readRootEpoch(txn, head, owner, wanted.epoch);
            try verifyEpochArchives(txn, head, owner, epoch);
            result.observed = .{ .root_adoption = .{ .ordinal = wanted.ordinal, .epoch = wanted.epoch, .request_digest = try epoch.request.digest() } };
        },
        .acknowledgement => |ack| {
            const owner = try readOwner(txn, head, ack.ordinal);
            var buf: [192]u8 = undefined;
            const key = try storageKey(request.lookup, .proof, ack.ordinal, ack.stage).encode(&buf);
            const bytes = try optional(txn, key);
            if (!owner.has(ack.stage)) {
                if (bytes != null) return error.InvalidGenerationPublication;
                return result;
            }
            const retained = bytes orelse return error.InvalidGenerationPublication;
            const signed = try ProofRecord.decode(retained);
            const checked = try verifiedStage(txn, head, owner, ack.stage, signed);
            const index = @backingInt(ack.stage);
            if (try stageFor(checked.header) != ack.stage or
                !std.mem.eql(u8, &try checked.header.digest(), &owner.headers[index]) or
                !std.mem.eql(u8, &checked.root_commitment, &owner.proofs[index])) return error.InvalidGenerationPublication;
            result.observed = .{ .acknowledgement = .{ .ordinal = ack.ordinal, .stage = ack.stage, .header_digest = owner.headers[index], .proof_digest = contract.hash(retained) } };
        },
        .installation => if (head.install_revision != 0) {
            result.observed = .{ .installation = .{ .revision = head.install_revision - 1, .digest = try head.installDecision() } };
        },
        .publication => if (head.publication_revision != 0) {
            result.observed = .{ .publication = .{ .revision = head.publication_revision - 1, .digest = try head.publicationDecision() } };
        },
        .cancellation => if (head.cancellation_revision != 0 and !head.failure.isConstraintViolation()) {
            result.observed = .{ .cancellation = .{ .decision = .{ .revision = head.cancellation_revision - 1, .digest = try head.cancellationDecision() }, .failure = head.failure } };
        },
    }
    return result;
}

/// Verifies the retained bytes themselves, not just a caller-supplied digest.
/// This is suitable for checkpoint/historical verification; current physical
/// enrollment is additionally rechecked by the accepting ACK/decision adapter.
pub fn verified(signed: evidence.SignedObservation, owner: barriers.Owner) !evidence.VerifiedObservation {
    switch (signed) {
        inline else => |value| {
            const observed = value.rooted.observation;
            try observed.validateFor(owner.binding);
            if (value.rooted.root_generation == 0 or value.rooted.root_generation != value.proof.root_generation or
                (owner.binding.authority == .native) != (value.proof.authority == .native)) return error.SchemaValidationJobChanged;
            try value.proof.verify(try value.evidenceDigest());
            if (observed.physical_root_incarnation != owner.physical_root_incarnation) return error.SchemaValidationRuntimeRecoveryRequired;
            return .{ .header = observed.header.?, .logical_digest = try value.logicalDigest(), .root_commitment = try value.commitment(), .physical_root_incarnation = observed.physical_root_incarnation };
        },
    }
}

/// O(1) point reads/writes, independent of immutable plan size and owner count.
/// Context.verify repeats enrollment/report/capability verification in THIS txn.
/// Exact accepted retries are no-ops, including after terminal unlock; retries
/// never infer whether an unmirrored metadata commit has reached standby.
pub const PreparedAcknowledgement = struct {
    lookup: Lookup,
    head: barriers.Head,
    changed: ?struct { owner: barriers.OwnerRecord, proof: ProofRecord, ordinal: u16, stage: barriers.Stage, foreign_constraints: u16 } = null,

    /// Stage only in the SAME writer snapshot that authenticated this value.
    /// Preflight callers discard it; consensus application prepares it afresh.
    pub fn stage(self: *const @This(), txn: anytype) !barriers.Head {
        const changed = self.changed orelse return self.head;
        var buf: [192]u8 = undefined;
        try txn.put(try storageKey(self.lookup, .proof, changed.ordinal, changed.stage).encode(&buf), changed.proof.slice());
        try txn.put(try storageKey(self.lookup, .owner, changed.ordinal, .ready).encode(&buf), changed.owner.slice());
        try writeHead(txn, self.head);
        try stageTerminal(txn, self.head, changed.foreign_constraints);
        return self.head;
    }
};

pub fn prepareAcknowledgement(txn: anytype, lookup: Lookup, ordinal: u16, expected_revision: u64, signed: evidence.SignedObservation, context: anytype) !PreparedAcknowledgement {
    const head = try readHead(txn, lookup);
    const owner = try readOwner(txn, head, ordinal);
    if (!std.mem.eql(u8, &signedIdentity(signed).metadata_incarnation, &head.metadata_incarnation)) return error.MetadataIncarnationMismatch;
    const header = switch (signed) {
        inline else => |value| value.rooted.observation.header orelse return error.SchemaValidationJobRequired,
    };
    const stage = try stageFor(header);
    var buf: [192]u8 = undefined;
    const proof_key = try storageKey(lookup, .proof, ordinal, stage).encode(&buf);
    const proof = try ProofRecord.init(signed);
    const prior = try optional(txn, proof_key);
    if (prior) |bytes| {
        if (std.mem.eql(u8, bytes, proof.slice())) {
            const authenticated = try verifiedStage(txn, head, owner, stage, signed);
            const i = @backingInt(stage);
            if (!owner.has(stage) or !std.mem.eql(u8, &owner.headers[i], &try authenticated.header.digest()) or
                !std.mem.eql(u8, &owner.proofs[i], &authenticated.root_commitment)) return error.InvalidGenerationPublication;
            return .{ .lookup = lookup, .head = head };
        }
    }
    const authenticated = try verified(signed, owner);
    try requireParticipantLock(txn, head, owner.binding.fence.namespace.table_id);
    const checked = try context.verify(txn, owner.binding, signed, head.reader_requirements);
    if (!std.meta.eql(checked, authenticated)) return error.SchemaValidationJobChanged;
    const update = try barriers.acknowledge(head, owner, expected_revision, checked);
    if (!update.changed) return .{ .lookup = lookup, .head = head };
    const encoded_owner = try update.owner.encode(update.head);
    // Retain the complete signed evidence with BOTH compact postimages. A
    // failed put or terminal delete is fatal to the whole writer, never a
    // recoverable stale-ACK result after partial progress has been staged.
    return .{ .lookup = lookup, .head = update.head, .changed = .{ .owner = encoded_owner, .proof = proof, .ordinal = ordinal, .stage = stage, .foreign_constraints = if (owner.binding.purpose == .foreign_key_source) owner.binding.columns else 0 } };
}

pub fn acknowledge(txn: anytype, lookup: Lookup, ordinal: u16, expected_revision: u64, signed: evidence.SignedObservation, context: anytype) !barriers.Head {
    const prepared = try prepareAcknowledgement(txn, lookup, ordinal, expected_revision, signed, context);
    return prepared.stage(txn);
}

pub const PreparedHead = struct {
    head: barriers.Head,
    changed: bool,

    pub fn stage(self: @This(), txn: anytype) !barriers.Head {
        if (self.changed) try writeHead(txn, self.head);
        return self.head;
    }
};

pub fn checkInstall(txn: anytype, lookup: Lookup, revision: u64) !PreparedHead {
    const head = try readHead(txn, lookup);
    if (revision != std.math.maxInt(u64) and head.install_revision == revision + 1) return .{ .head = head, .changed = false };
    try requireLock(txn, head);
    return .{ .head = try head.decideInstall(revision), .changed = true };
}
pub fn decideInstall(txn: anytype, lookup: Lookup, revision: u64) !barriers.Head {
    return (try checkInstall(txn, lookup, revision)).stage(txn);
}
pub fn checkCancellation(txn: anytype, lookup: Lookup, revision: u64, failure: barriers.Failure) !PreparedHead {
    const head = try readHead(txn, lookup);
    if (revision != std.math.maxInt(u64) and head.cancellation_revision == revision + 1 and head.failure == failure) return .{ .head = head, .changed = false };
    try requireLock(txn, head);
    return .{ .head = try head.cancel(revision, failure), .changed = true };
}
pub fn cancel(txn: anytype, lookup: Lookup, revision: u64, failure: barriers.Failure) !barriers.Head {
    return (try checkCancellation(txn, lookup, revision, failure)).stage(txn);
}

pub const Recovery = struct {
    alloc: A,
    plan: std.json.Parsed(plan_mod.Plan),
    head: barriers.Head,
    owners: []barriers.Owner,
    proof_count: usize = 0,

    pub fn deinit(self: *Recovery) void {
        self.plan.deinit();
        self.alloc.free(self.owners);
        self.* = undefined;
    }
};

/// Reconstruct the entire admitted cut, including every historical phase
/// signature and initial physical preparation. Never invoked on the hot ACK
/// path. The caller separately validates live catalog/placement/restore scope.
pub fn recover(alloc: A, txn: anytype, lookup: Lookup) !Recovery {
    const head = try readHead(txn, lookup);
    var buf: [192]u8 = undefined;
    var plan = try plan_mod.parse(alloc, try txn.get(try storageKey(lookup, .plan, 0, .ready).encode(&buf)));
    errdefer plan.deinit();
    const owners = try alloc.alloc(barriers.Owner, head.owner_count);
    errdefer alloc.free(owners);
    const input_digests = try alloc.alloc([32]u8, head.input_count);
    defer alloc.free(input_digests);
    const catalog_digests = try alloc.alloc([32]u8, head.input_count);
    defer alloc.free(catalog_digests);
    const cohort = plan.value.cohort();
    const before_public = try alloc.alloc([32]u8, if (cohort) head.input_count else 0);
    defer alloc.free(before_public);
    const after_public = try alloc.alloc([32]u8, if (cohort) head.input_count else 0);
    defer alloc.free(after_public);
    const columns = try plan.value.selection.encodePayload(alloc);
    defer alloc.free(columns);
    for (input_digests, catalog_digests, 0..) |*input_digest, *catalog_digest, ordinal| {
        const bytes = try txn.get(try storageKey(lookup, .input, @intCast(ordinal), .ready).encode(&buf));
        const parts = try contract.inputParts(bytes);
        if (!std.mem.eql(u8, parts[3], columns)) return error.InvalidGenerationPublication;
        if (cohort) {
            before_public[ordinal] = contract.hash(parts[0]);
            after_public[ordinal] = contract.hash(parts[1]);
        } else if (!std.mem.eql(u8, parts[0], plan.value.before.schema_json) or !std.mem.eql(u8, parts[1], plan.value.after.schema_json)) return error.InvalidGenerationPublication;
        input_digest.* = contract.hash(bytes);
        catalog_digest.* = contract.hash(parts[2]);
    }
    var count: usize = 0;
    for (owners, 0..) |*owner, i| {
        owner.* = try readOwner(txn, head, @intCast(i));
        const address = OwnerAddress.fromBinding(owner.binding);
        const owner_index = try NamespaceOwnerRecord.decode(try txn.get(try namespaceOwnerKey(lookup, address).encode(&buf)));
        if (owner_index.ordinal != owner.ordinal or !std.meta.eql(owner_index.address, address)) return error.InvalidCatalogRecord;
        if (!std.mem.eql(u8, &owner.binding.inputs, &input_digests[owner.input_ordinal]) or
            !std.mem.eql(u8, &owner.binding.fence.catalog_digest, &catalog_digests[owner.input_ordinal])) return error.InvalidGenerationPublication;
        if (cohort and (!std.mem.eql(u8, &owner.binding.before_public, &before_public[owner.input_ordinal]) or
            !std.mem.eql(u8, &owner.binding.after_public, &after_public[owner.input_ordinal]))) return error.InvalidGenerationPublication;
        const preparation = try PreparationRecord.decode(try txn.get(try storageKey(lookup, .preparation, @intCast(i), .ready).encode(&buf)));
        try preparation.verifyFor(owner.binding, owner.initialRoot());
        if (owner.root_epoch != 0) _ = try readRootEpoch(txn, head, owner.*, owner.root_epoch);
        var epoch: u64 = 1;
        while (epoch <= owner.root_epoch) {
            const retained = try readRootEpoch(txn, head, owner.*, epoch);
            try verifyEpochArchives(txn, head, owner.*, retained);
            if (epoch == owner.root_epoch) break;
            epoch += 1;
        }
        if (!std.mem.eql(u8, &preparation.proof.identity.metadata_incarnation, &head.metadata_incarnation)) return error.MetadataIncarnationMismatch;
        for (std.meta.tags(barriers.Stage)) |stage| {
            const proof_bytes = try optional(txn, try storageKey(lookup, .proof, @intCast(i), stage).encode(&buf));
            if (owner.has(stage) != (proof_bytes != null)) return error.InvalidGenerationPublication;
            if (proof_bytes) |bytes| {
                const signed = try ProofRecord.decode(bytes);
                const checked = try verifiedStage(txn, head, owner.*, stage, signed);
                const index = @backingInt(stage);
                if (try stageFor(checked.header) != stage or !std.mem.eql(u8, &checked.logical_digest, &owner.logical[index]) or
                    !std.mem.eql(u8, &try checked.header.digest(), &owner.headers[index]) or
                    !std.mem.eql(u8, &checked.root_commitment, &owner.proofs[index]) or
                    !std.mem.eql(u8, &signedIdentity(signed).metadata_incarnation, &head.metadata_incarnation)) return error.InvalidGenerationPublication;
                // Check decision and predecessor relationships independently
                // of the aggregate chain/counter reconstruction below.
                if (stage == .installed or stage == .released) {
                    const installed = checked.header.installation.?;
                    if (!std.mem.eql(u8, &installed.ready_digest, &owner.headers[@backingInt(barriers.Stage.ready)]) or
                        !std.mem.eql(u8, &installed.decision, &try head.installDecision())) return error.InvalidGenerationPublication;
                }
                if (stage == .released) {
                    const released = checked.header.release.?;
                    if (!std.mem.eql(u8, &released.installed_digest, &owner.headers[@backingInt(barriers.Stage.installed)]) or
                        !std.mem.eql(u8, &released.publication, &try head.publicationDecision())) return error.InvalidGenerationPublication;
                }
                count += 1;
            }
        }
    }
    try plan.value.requireProgress(alloc, head, owners);
    if (plan.value.sequence_change) |change| {
        const accepted = try SequenceAcceptanceRecord.decode(try txn.get(try sequenceOutcomeKey(lookup.group, change.before.id, change.request.admission.nonce).encode(&buf)));
        try requireSequenceAcceptance(accepted, head);
        if (!std.meta.eql(accepted.admission, change.request.admission)) return error.InvalidSequenceOutcome;
    }
    if (plan.value.selection == .foreign_keys) try @import("schema_validation_dependencies.zig").requireSource(alloc, txn, head, plan.value.selection.foreign_keys, terminal(head));
    if (head.catalogEffect()) {
        if (head.publication_revision != 0 and head.catalog_revision == 0) return error.InvalidCatalogRecord;
        if (!terminal(head)) {
            try requireCatalogReservation(txn, head);
        } else if (try optional(txn, try resourceActiveKey(head).encode(&buf))) |bytes| {
            if (std.meta.eql(try ParticipantAlias.decode(bytes), lookup)) return error.InvalidCatalogRecord;
            _ = if (head.retirement_root != null) try activeRetirement(txn, head) else if (head.type_id != 0) try activeType(txn, lookup.group, head.type_id) else try activeSequence(txn, lookup.group, head.sequence_retirement.?.id);
        }
    }
    const participants = try readParticipants(txn, head);
    for (participants.tables[0..participants.len]) |table| {
        if (!terminal(head)) {
            try requireParticipantLock(txn, head, table);
        } else {
            const active = try optional(txn, try (Key{ .group = lookup.group, .table = table, .id = @splat(0), .kind = .active }).encode(&buf));
            if (active) |_| {
                const current = (try resolveActive(txn, lookup.group, table)) orelse return error.InvalidGenerationPublication;
                if (std.meta.eql(current, lookup)) return error.InvalidGenerationPublication;
            }
            if (try lifecycle.read(txn, lookup.group, table)) |lock|
                if (lock.kind == .schema_validation and std.mem.eql(u8, &lock.id, &head.id)) return error.InvalidGenerationPublication;
        }
    }
    return .{ .alloc = alloc, .plan = plan, .head = head, .owners = owners, .proof_count = count };
}
fn signedIdentity(signed: evidence.SignedObservation) @import("store_root_enrollment.zig").Identity {
    return switch (signed) {
        inline else => |value| value.proof.identity,
    };
}

/// Publication rechecks every installed owner's current physical authority,
/// then stages the EXACT successor through the metadata table writer callback.
/// The table postimage and publication decision must share this transaction.
pub const PreparedPublication = struct {
    head: barriers.Head,
    recovery: ?Recovery = null,

    pub fn deinit(self: *@This()) void {
        if (self.recovery) |*value| value.deinit();
        self.* = undefined;
    }

    /// The catalog postimage and decision are inseparable. The callback still
    /// rechecks live catalog authority in this SAME writer before staging it.
    pub fn stage(self: *const @This(), txn: anytype, context: anytype) !barriers.Head {
        if (self.recovery) |value| {
            try context.publish(txn, value.plan.value);
            var head = self.head;
            if (head.catalogEffect()) {
                const Context = switch (@typeInfo(@TypeOf(context))) {
                    .pointer => |pointer| pointer.child,
                    else => @TypeOf(context),
                };
                if (comptime @hasDecl(Context, "publicationCatalogRevision")) {
                    head.catalog_revision = try context.publicationCatalogRevision(txn);
                    if (head.catalog_revision == 0) return error.InvalidCatalogRecord;
                } else return error.InvalidGenerationPublication;
            }
            try writeHead(txn, head);
            return head;
        }
        return self.head;
    }
};

pub fn checkPublication(alloc: A, txn: anytype, lookup: Lookup, revision: u64, context: anytype) !PreparedPublication {
    const head = try readHead(txn, lookup);
    if (revision != std.math.maxInt(u64) and head.publication_revision == revision + 1) return .{ .head = head };
    try requireLock(txn, head);
    const next = try head.publish(revision);
    var recovered = try recover(alloc, txn, lookup);
    errdefer recovered.deinit();
    for (recovered.owners) |owner| {
        try requireCurrentStageAuthority(txn, head, owner, .installed, context);
    }
    return .{ .head = next, .recovery = recovered };
}

/// Join historical logical acceptance to the CURRENT physical owner. An old
/// source need not remain live, and a replacement never fabricates another
/// ready/installed ACK. A stale runtime generation requires re-attestation.
pub fn requireCurrentStageAuthority(txn: anytype, head: barriers.Head, owner: barriers.Owner, stage: barriers.Stage, context: anytype) !void {
    var buf: [192]u8 = undefined;
    const signed = try ProofRecord.decode(try txn.get(try storageKey(Lookup.fromHead(head), .proof, owner.ordinal, stage).encode(&buf)));
    const historic = try verifiedStage(txn, head, owner, stage, signed);
    const i = @backingInt(stage);
    if (!owner.has(stage) or !std.mem.eql(u8, &historic.root_commitment, &owner.proofs[i]) or
        !std.mem.eql(u8, &try historic.header.digest(), &owner.headers[i])) return error.InvalidCatalogRecord;
    if (owner.proof_epochs[i] == owner.root_epoch) {
        if (!std.meta.eql(try context.verify(txn, owner.binding, signed, head.reader_requirements), historic)) return error.SchemaValidationJobChanged;
        return;
    }
    const epoch = try readRootEpoch(txn, head, owner, owner.root_epoch);
    // A zero-progress adoption cannot inherit an older accepted ACK. New ACKs
    // are signed on its own epoch and take the exact-current branch above.
    const Current = struct { destination: local.storage_db_relational_schema_root_contract.Evidence, signed: evidence.SignedObservation };
    const current: Current = switch (epoch.request.proposal) {
        .audited => |value| .{ .destination = value.destination, .signed = .{ .raft = value.observation } },
        .audited_native => |value| .{ .destination = value.destination, .signed = .{ .native = value.observation } },
        .bootstrap, .native_bootstrap => return error.InvalidCatalogRecord,
    };
    try parent.requireHeaderLineage(historic.header, current.destination.value.rows.header);
    try parent.requireHeaderDecisions(head, current.destination.value.rows.header);
    const checked = try context.verify(txn, owner.binding, current.signed, head.reader_requirements);
    if (checked.physical_root_incarnation != owner.physical_root_incarnation or
        !std.mem.eql(u8, &try checked.header.digest(), &try current.destination.value.rows.header.digest())) return error.SchemaValidationJobChanged;
}

pub fn publish(alloc: A, txn: anytype, lookup: Lookup, revision: u64, context: anytype) !barriers.Head {
    var prepared = try checkPublication(alloc, txn, lookup, revision, context);
    defer prepared.deinit();
    return prepared.stage(txn, context);
}

/// Reverse-count verification rejects extra/orphan records that forward plan
/// lookup alone cannot discover. Keys have a single canonical representation.
/// Context.restore checks the exact live table/owner/SQL cut at this checkpoint;
/// historical terminal jobs may outlive later DDL and physical root replacement.
pub fn verifyGroup(alloc: A, txn: anytype, group: u64, context: anytype) !void {
    var cursor = try txn.openCursor();
    defer cursor.close();
    var buf: [192]u8 = undefined;
    const owned_prefix = try prefix(&buf, group);
    var expected: [std.meta.tags(Kind).len]usize = @splat(0);
    var received: [std.meta.tags(Kind).len]usize = @splat(0);
    var row = try cursor.seekAtOrAfter(owned_prefix);
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, owned_prefix)) break;
        const key = (try Key.parse(item.key)) orelse return error.InvalidCatalogRecord;
        if (key.group != group) return error.InvalidCatalogRecord;
        received[@backingInt(key.kind)] = try std.math.add(usize, received[@backingInt(key.kind)], 1);
        if (key.kind == .plan) {
            // Recover copies the head before any further borrowed get. Plan
            // bodies are owned by their parsed arena throughout the audit.
            var head_buf: [192]u8 = undefined;
            var head_key = key;
            head_key.kind = .head;
            const head = try barriers.Head.decode(try txn.get(try head_key.encode(&head_buf)));
            const lookup = Lookup.fromHead(head);
            if (lookup.group != key.group or lookup.table != key.table or !std.mem.eql(u8, &lookup.id, &key.id)) return error.InvalidCatalogRecord;
            var recovery = try recover(alloc, txn, lookup);
            defer recovery.deinit();
            try context.restore(txn, &recovery);
            expected[@backingInt(Kind.plan)] += 1;
            expected[@backingInt(Kind.head)] += 1;
            if (head.sequence_retirement != null) expected[@backingInt(Kind.sequence_outcome)] += 1;
            expected[@backingInt(Kind.input)] = try std.math.add(usize, expected[@backingInt(Kind.input)], head.input_count);
            expected[@backingInt(Kind.owner)] = try std.math.add(usize, expected[@backingInt(Kind.owner)], head.owner_count);
            expected[@backingInt(Kind.owner_namespace)] = try std.math.add(usize, expected[@backingInt(Kind.owner_namespace)], head.owner_count);
            expected[@backingInt(Kind.preparation)] = try std.math.add(usize, expected[@backingInt(Kind.preparation)], head.owner_count);
            expected[@backingInt(Kind.proof)] = try std.math.add(usize, expected[@backingInt(Kind.proof)], recovery.proof_count);
            for (recovery.owners) |owner| {
                expected[@backingInt(Kind.root_epoch)] = try std.math.add(usize, expected[@backingInt(Kind.root_epoch)], std.math.cast(usize, owner.root_epoch) orelse return error.InvalidCatalogRecord);
            }
            const participants = try readParticipants(txn, head);
            if (!std.mem.allEqual(u8, &head.participants_digest, 0)) expected[@backingInt(Kind.participants)] += 1;
            if (!terminal(head)) {
                if (recovery.plan.value.selection == .object_rename) expected[@backingInt(Kind.object_rename_source)] += 1;
                if (head.catalogEffect()) {
                    expected[@backingInt(resourceActiveKey(head).kind)] += 1;
                    if (head.sequence_retirement != null) {
                        expected[@backingInt(Kind.sequence_namespace)] += 1;
                        expected[@backingInt(Kind.sequence_database)] += 1;
                    }
                    try requireCatalogReservation(txn, head);
                }
                expected[@backingInt(Kind.active)] = try std.math.add(usize, expected[@backingInt(Kind.active)], participants.len);
                for (participants.tables[0..participants.len]) |table| try requireParticipantLock(txn, head, table);
            }
        } else if (key.kind == .root_epoch) {
            const value = try roots.EpochRecord.decode(item.value);
            const head = try readHead(txn, value.request.lookup);
            const owner = try readOwner(txn, head, key.ordinal);
            if (value.request.lookup.group != key.group or value.request.lookup.table != key.table or
                !std.mem.eql(u8, &value.request.lookup.id, &key.id)) return error.InvalidCatalogRecord;
            _ = try readRootEpoch(txn, head, owner, std.mem.readInt(u64, key.grant.?[24..32], .big));
        } else if (key.kind == .proof_history) {
            const value = try roots.HistoricalProofRecord.decode(item.value);
            const head_key = Key{ .group = key.group, .table = key.table, .id = key.id, .kind = .head };
            var head_buf: [192]u8 = undefined;
            const head = try barriers.Head.decode(try txn.get(try head_key.encode(&head_buf)));
            const owner = try readOwner(txn, head, key.ordinal);
            const epoch = try readRootEpoch(txn, head, owner, value.first_saved_epoch);
            const i = @backingInt(key.stage);
            if (!epoch.source_owner.has(key.stage) or epoch.source_owner.proof_epochs[i] != value.epoch or
                !std.mem.eql(u8, &epoch.source_owner.proofs[i], &key.grant.?)) return error.InvalidCatalogRecord;
            const checked = try verifiedStage(txn, head, epoch.source_owner, key.stage, value.signed);
            if (!std.mem.eql(u8, &checked.root_commitment, &key.grant.?)) return error.InvalidCatalogRecord;
            expected[@backingInt(Kind.proof_history)] = try std.math.add(usize, expected[@backingInt(Kind.proof_history)], 1);
        } else if (key.kind == .owner_namespace) {
            const index = try NamespaceOwnerRecord.decode(item.value);
            const head_key = Key{ .group = key.group, .table = key.table, .id = key.id, .kind = .head };
            var head_buf: [192]u8 = undefined;
            const head = try barriers.Head.decode(try txn.get(try head_key.encode(&head_buf)));
            if (!std.mem.eql(u8, &index.address.digest(), &key.grant.?)) return error.InvalidCatalogRecord;
            const control = (try readReplayControl(txn, .fromHead(head), index.address)) orelse return error.InvalidCatalogRecord;
            if (control.owner.ordinal != index.ordinal) return error.InvalidCatalogRecord;
        } else if (key.kind == .parent) {
            const retained = try parent.Record.decode(item.value);
            if (retained.lookup.group != key.group or retained.lookup.table != key.table or
                retained.ordinal != key.ordinal or !std.mem.eql(u8, &retained.lookup.id, &key.id) or
                !std.mem.eql(u8, &try retained.id(), &key.grant.?)) return error.InvalidCatalogRecord;
            _ = (try readParentAuthorization(txn, retained.lookup, key.ordinal, key.grant.?)) orelse return error.InvalidCatalogRecord;
            if (try readParentByImage(txn, retained.lookup, try retained.image()) == null) return error.InvalidCatalogRecord;
            expected[@backingInt(Kind.parent)] = try std.math.add(usize, expected[@backingInt(Kind.parent)], 1);
        } else if (key.kind == .parent_image) {
            const index = try ParentImageRecord.decode(item.value);
            const head_key = Key{ .group = key.group, .table = key.table, .id = key.id, .kind = .head };
            var head_buf: [192]u8 = undefined;
            const head = try barriers.Head.decode(try txn.get(try head_key.encode(&head_buf)));
            const lookup = Lookup.fromHead(head);
            const retained = (try readParentAuthorization(txn, lookup, index.ordinal, index.grant)) orelse return error.InvalidCatalogRecord;
            if (!std.mem.eql(u8, &try (try retained.image()).digest(), &key.grant.?)) return error.InvalidCatalogRecord;
            expected[@backingInt(Kind.parent_image)] = try std.math.add(usize, expected[@backingInt(Kind.parent_image)], 1);
        } else if (key.kind == .sequence_outcome) {
            const accepted = try SequenceAcceptanceRecord.decode(item.value);
            const head = try readHead(txn, accepted.lookup);
            try requireSequenceAcceptance(accepted, head);
            if (!std.meta.eql(key, sequenceOutcomeKey(head.metadata_group_id, head.sequence_retirement.?.id, accepted.admission.nonce))) return error.InvalidSequenceOutcome;
        } else if (key.kind == .sequence_namespace or key.kind == .sequence_database) {
            const lookup = try ParticipantAlias.decode(item.value);
            const head = try readHead(txn, lookup);
            if (head.sequence_retirement == null or terminal(head) or !std.meta.eql(key, sequenceContainerKey(head, key.kind))) return error.InvalidCatalogRecord;
            try requireCatalogReservation(txn, head);
        } else if (key.kind == .retirement_active) {
            const selected = try ParticipantAlias.decode(item.value);
            const head = try readHead(txn, selected);
            if (head.retirement_root == null or terminal(head) or !std.meta.eql(key, resourceActiveKey(head))) return error.InvalidCatalogRecord;
            try requireCatalogReservation(txn, head);
        } else if (key.kind == .sequence_active) {
            _ = (try activeSequence(txn, key.group, key.table)) orelse return error.InvalidCatalogRecord;
        } else if (key.kind == .type_active) {
            _ = (try activeType(txn, key.group, key.table)) orelse return error.InvalidCatalogRecord;
        } else if (key.kind == .object_rename_source) {
            if (item.value.len != 16 or std.mem.allEqual(u8, item.value, 0)) return error.InvalidCatalogRecord;
            const lookup = (try resolveActive(txn, key.group, key.table)) orelse return error.InvalidCatalogRecord;
            const head = try readHead(txn, lookup);
            if (terminal(head) or !std.mem.eql(u8, item.value, &head.id)) return error.InvalidCatalogRecord;
            const owner = try readOwner(txn, head, 0);
            if (owner.binding.purpose != .object_rename) return error.InvalidCatalogRecord;
        } else if (key.kind == .active) {
            const lookup = (try resolveActive(txn, key.group, key.table)) orelse return error.InvalidCatalogRecord;
            const head = try readHead(txn, lookup);
            if (terminal(head)) return error.InvalidCatalogRecord;
            try requireParticipantLock(txn, head, key.table);
        }
    }
    if (!std.meta.eql(expected, received)) return error.InvalidCatalogRecord;
}

const TestContext = struct {
    verifies: usize = 0,
    parent_verifies: usize = 0,
    publishes: usize = 0,
    permission_reads: usize = 0,
    reject: bool = false,
    const table_key = "fixture:logical-schema";
    const type_key = "fixture:named-type";
    const sequence_key = "fixture:sequence";
    const sequence_meta_key = "\x00\x00__metadata__:system_catalog:1:meta";
    fn participantKey(buffer: []u8, table: u64) ![]const u8 {
        return std.fmt.bufPrint(buffer, "fixture:cohort-table:{d}", .{table});
    }
    fn requireCohort(txn: anytype, plan: plan_mod.Plan, published: bool) !void {
        if (plan.type_change) |change| {
            const expected = try (if (published) change.after else change.before).encode(std.testing.allocator);
            defer std.testing.allocator.free(expected);
            if (!std.mem.eql(u8, try txn.get(type_key), expected)) return error.CatalogGenerationChanged;
        }
        if (plan.sequence_change) |change| {
            if (published) {
                if (try optional(txn, sequence_key) != null) return error.CatalogGenerationChanged;
            } else {
                const expected = try change.before.encode(std.testing.allocator);
                defer std.testing.allocator.free(expected);
                if (!std.mem.eql(u8, try txn.get(sequence_key), expected)) return error.CatalogGenerationChanged;
            }
        }
        if (plan.cohort()) {
            var buffer: [96]u8 = undefined;
            for (plan.additional_tables) |pair| {
                const table = if (published) pair.after else pair.before;
                if (!std.mem.eql(u8, try txn.get(try participantKey(&buffer, table.table_id)), table.schema_json)) return error.CatalogGenerationChanged;
            }
        }
    }
    pub fn admit(self: *@This(), txn: anytype, plan: plan_mod.Plan, _: []const evidence.SignedPreparation) !void {
        if (self.reject) return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, try txn.get(table_key), plan.before.schema_json)) return error.CatalogGenerationChanged;
        try requireCohort(txn, plan, false);
    }
    pub fn verify(self: *@This(), _: anytype, binding: contract.Binding, signed: evidence.SignedObservation, _: ?local.common_schema_readers.ReaderRequirements) !evidence.VerifiedObservation {
        self.verifies += 1;
        if (self.reject) return error.StoreRootEnrollmentChanged;
        const root = switch (signed) {
            inline else => |value| value.rooted.observation.physical_root_incarnation,
        };
        return verified(signed, .{ .ordinal = 0, .binding = binding, .physical_root_incarnation = root });
    }
    pub fn authorize(self: *@This(), _: anytype, _: barriers.Head, _: barriers.Owner, _: contract.Action) !void {
        self.permission_reads += 1;
        // Fixture authority only; never a production root/enrollment grant.
        if (self.reject) return error.Forbidden;
    }
    pub fn verifyParent(self: *@This(), _: anytype, binding: contract.Binding, physical_root: u128, signed: evidence.SignedSnapshotParent, _: ?local.common_schema_readers.ReaderRequirements) !void {
        self.parent_verifies += 1;
        if (self.reject) return error.StoreRootEnrollmentChanged;
        try signed.verifyFor(binding, physical_root);
    }
    pub fn verifyAdoption(self: *@This(), txn: anytype, head: barriers.Head, owner: barriers.Owner, signed: evidence.SignedObservation) !evidence.VerifiedObservation {
        return self.verify(txn, owner.binding, signed, head.reader_requirements);
    }
    pub fn verifyBootstrap(self: *@This(), _: anytype, head: barriers.Head, owner: barriers.Owner, signed: @import("schema_validation_bootstrap.zig").CertifiedPreparation) !void {
        self.verifies += 1;
        if (self.reject) return error.StoreRootEnrollmentChanged;
        _ = try signed.attestedScope(owner.binding, head.metadata_incarnation);
    }
    pub fn verifyNativeBootstrap(self: *@This(), _: anytype, head: barriers.Head, owner: barriers.Owner, signed: evidence.SignedPreparation) !void {
        self.verifies += 1;
        if (self.reject) return error.StoreRootEnrollmentChanged;
        try signed.verifyFor(owner.binding, signed.value.physical_root_incarnation);
        if (!std.mem.eql(u8, &head.metadata_incarnation, &signed.proof.identity.metadata_incarnation)) return error.MetadataIncarnationMismatch;
    }
    pub fn publicationCatalogRevision(_: *@This(), _: anytype) !u64 {
        return 3;
    }

    pub fn stageTypeAdmission(_: *@This(), txn: anytype, request: @import("../system_catalog/named_types.zig").Mutation, identity: @import("../system_catalog/named_types.zig").Identity, head: barriers.Head) !void {
        const pending: @import("../system_catalog/sequence_outcomes.zig").Pending = .{ .revision = 1, .identity = identity, .work = .{ .schema = .fromHead(head) } };
        try pending.validate();
        if (request.operation != .add_enum and request.operation != .set_domain_not_null) return error.InvalidGenerationPublication;
        const record = try barriers.Record(@TypeOf(pending), "SVTA", 2048).init(pending);
        try txn.put("fixture:type-acceptance", record.slice());
    }

    pub fn stageSequenceAdmission(_: *@This(), txn: anytype, request: @import("../system_catalog/sequence_catalog.zig").Mutation, head: barriers.Head) !u64 {
        const sequences = @import("../system_catalog/sequence_catalog.zig");
        const retirement = head.sequence_retirement.?;
        const journal: sequences.outcomes.Store(@typeInfo(@TypeOf(txn)).pointer.child) = .{ .txn = txn, .group = head.metadata_group_id, .incarnation = request.incarnation };
        const digest = try sequences.commandDigest(std.testing.allocator, request);
        const ordinal = switch (try journal.prepare(std.testing.allocator, request.admission, digest)) {
            .append => |value| value,
            .replay => return error.GenerationPublicationChanged,
        };
        try journal.append(std.testing.allocator, ordinal, request.admission, digest, .{ .pending = .{ .revision = 1, .identity = .{ .id = retirement.id, .generation = retirement.generation }, .work = .{ .sequence_schema = .fromHead(head) } } });
        return 1;
    }

    pub fn publish(self: *@This(), txn: anytype, plan: plan_mod.Plan) !void {
        if (self.reject or !std.mem.eql(u8, try txn.get(table_key), plan.before.schema_json)) return error.CatalogGenerationChanged;
        try requireCohort(txn, plan, false);
        const type_bytes = if (plan.type_change) |change| try change.after.encode(std.testing.allocator) else null;
        defer if (type_bytes) |bytes| std.testing.allocator.free(bytes);
        try txn.put(table_key, plan.after.schema_json);
        var buffer: [96]u8 = undefined;
        for (plan.additional_tables) |pair| try txn.put(try participantKey(&buffer, pair.after.table_id), pair.after.schema_json);
        if (type_bytes) |bytes| try txn.put(type_key, bytes);
        if (plan.sequence_change != null) {
            try txn.delete(sequence_key);
            var meta = try std.json.parseFromSlice(local.system_catalog_domain.Meta, std.testing.allocator, try txn.get(sequence_meta_key), .{});
            defer meta.deinit();
            meta.value.revision = 3;
            const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, meta.value, .{});
            defer std.testing.allocator.free(encoded);
            try txn.put(sequence_meta_key, encoded);
        }
        self.publishes += 1;
    }
    pub fn restore(_: *@This(), txn: anytype, recovery: *const Recovery) !void {
        if (terminal(recovery.head)) return;
        const published = recovery.head.phase == .releasing;
        const expected = if (published) recovery.plan.value.after else recovery.plan.value.before;
        if (!std.mem.eql(u8, try txn.get(table_key), expected.schema_json)) return error.InvalidCatalogRecord;
        requireCohort(txn, recovery.plan.value, published) catch return error.InvalidCatalogRecord;
    }
};

const TestProofs = struct {
    fn parentRequest(head: barriers.Head, owner: barriers.Owner, header: ?contract.Header) !parent.Request {
        var source_header: ?[32]u8 = null;
        const through_index = if (header) |value| @max(@as(u64, 3), value.updated.raft.index) else 3;
        if (header) |value| {
            var buffer: [contract.max_header + 44]u8 = undefined;
            var fixed = std.heap.FixedBufferAllocator.init(&buffer);
            source_header = contract.hash(try value.encode(fixed.allocator()));
        }
        return .{
            .lookup = Lookup.fromHead(head),
            .ordinal = owner.ordinal,
            .revision = head.revision,
            .owner_digest = try parent.ownerDigest(owner, head),
            .root_epoch = owner.root_epoch,
            .source_header = header,
            .signed = try evidence.SignedSnapshotParent.sign(.{
                .binding = owner.binding,
                .origin = .{ .physical_root_incarnation = owner.physical_root_incarnation, .namespace = owner.binding.fence.namespace, .authority = null, .applied = .{ .term = 2, .index = 1 }, .schema_header = source_header, .transaction_directory = null, .schema_outcomes = null },
                .stream_digest = @splat(23),
                .through_index = through_index,
                .completed = .{ .incarnation = 77, .term = 2, .applied_index = through_index },
            }, try identity(head), 4, 42, 6, try root()),
        };
    }
    fn ackRequest(lookup: Lookup, ordinal: u16, signed_value: evidence.SignedObservation) !@import("schema_validation_receipt.zig").Request {
        return acknowledgementReceipt(lookup, ordinal, signed_value);
    }

    // Ledger/cryptographic fixture only. Production evidence is issued by the
    // native full-image auditor, never synthesized by the coordinator.
    fn adoptionRequest(head: barriers.Head, owner: barriers.Owner, retained: parent.Request, destination_root: u128) !roots.Request {
        const header = retained.source_header.?;
        const Directory = @typeInfo(@FieldType(local.storage_db_relational_schema_root_contract.Qualification, "directory_head")).optional.child;
        const directory: Directory = .{ .scope = try @FieldType(Directory, "scope").fromBinding(owner.binding), .phase = .ready };
        const header_bytes = try header.encode(std.testing.allocator);
        defer std.testing.allocator.free(header_bytes);
        var image: local.storage_db_relational_schema_root_contract.Evidence = .{
            .value = .{
                .signing_root = 7,
                .physical_root = destination_root,
                .checkpoint_digest = @splat(91),
                .rows = .{ .header = header, .snapshot_cut = header.updated, .coverage = .full, .records = header.records, .rows = header.rows_scanned, .cursor_digest = header.cursor_digest },
                .qualification = .{
                    .origin = .{
                        .physical_root_incarnation = destination_root,
                        .namespace = owner.binding.fence.namespace,
                        .authority = .{ .kind = .raft, .namespace = local.storage_db_online_source_contract.namespaceBytes(owner.binding.fence.namespace) },
                        .applied = .{ .term = header.updated.raft.term, .index = header.updated.raft.index },
                        .schema_header = contract.hash(header_bytes),
                        .transaction_directory = contract.hash(&try directory.encode()),
                        .schema_outcomes = null,
                    },
                    .directory_head = directory,
                    .directory = .{},
                    .transactions = .{},
                    .history_head = null,
                    .history = .{},
                    .installation = .{ .group_id = owner.binding.fence.owner_group_id, .through_index = retained.signed.value.through_index, .destination_root_incarnation = destination_root, .namespace = local.storage_db_online_source_contract.namespaceBytes(owner.binding.fence.namespace), .source = retained.signed.value.origin, .stream_digest = retained.signed.value.stream_digest },
                    .source_history_head = null,
                },
            },
            .signature = undefined,
        };
        image.signature = try (try root()).sign(&try image.signatureDigest());
        var destination_owner = owner;
        destination_owner.physical_root_incarnation = destination_root;
        return .{ .lookup = .fromHead(head), .ordinal = owner.ordinal, .revision = head.revision, .owner_digest = try parent.ownerDigest(owner, head), .expected_epoch = owner.root_epoch, .lineage = .{ .retained_parent = try retained.id() }, .proposal = .{ .audited = .{ .destination = image, .observation = (try signed(head, destination_owner, header)).raft } } };
    }

    fn nativeAdoptionRequest(head: barriers.Head, owner: barriers.Owner, destination_root: u128) !roots.Request {
        return nativeAdoptionRequestForHeader(head, owner, destination_root, ready(owner));
    }

    fn nativeAdoptionRequestForHeader(head: barriers.Head, owner: barriers.Owner, destination_root: u128, header: contract.Header) !roots.Request {
        const historical = header.phase == .released and header.transaction_coverage != null;
        const history_only = header.phase == .canceled or historical;
        const Directory = @typeInfo(@FieldType(local.storage_db_relational_schema_root_contract.Qualification, "directory_head")).optional.child;
        const directory: Directory = .{ .scope = try @FieldType(Directory, "scope").fromBinding(owner.binding), .phase = if (history_only) .retired else .ready };
        const encoded = try header.encode(std.testing.allocator);
        defer std.testing.allocator.free(encoded);
        var image: local.storage_db_relational_schema_root_contract.Evidence = .{
            .value = .{
                .signing_root = 7,
                .physical_root = destination_root,
                .checkpoint_digest = @splat(91),
                .rows = .{ .header = header, .snapshot_cut = header.updated, .coverage = if (historical) .history else if (history_only) .none else .full, .records = if (history_only) 0 else header.records, .rows = if (history_only) 0 else header.rows_scanned, .cursor_digest = if (history_only) contract.hash("") else header.cursor_digest, .reference_coverage = if (!owner.binding.purpose.references() or historical) null else .{} },
                .qualification = .{
                    .origin = .{ .physical_root_incarnation = destination_root, .namespace = owner.binding.fence.namespace, .authority = .{ .kind = .native, .namespace = local.storage_db_online_source_contract.namespaceBytes(owner.binding.fence.namespace), .sequence = header.updated.native.sequence }, .applied = null, .schema_header = contract.hash(encoded), .transaction_directory = contract.hash(&try directory.encode()), .schema_outcomes = null },
                    .directory_head = directory,
                    .directory = .{},
                    .transactions = .{},
                    .history_head = null,
                    .history = .{},
                    .installation = null,
                    .source_history_head = null,
                },
            },
            .signature = undefined,
        };
        image.signature = try (try root()).sign(&try image.signatureDigest());
        var receiver = owner;
        receiver.physical_root_incarnation = destination_root;
        return .{ .lookup = .fromHead(head), .ordinal = owner.ordinal, .revision = head.revision, .owner_digest = try parent.ownerDigest(owner, head), .expected_epoch = owner.root_epoch, .lineage = .native_recovery, .proposal = .{ .audited_native = .{ .destination = image, .observation = (try signed(head, receiver, header)).native } } };
    }

    fn bootstrapRequest(head: barriers.Head, owner: barriers.Owner, destination_root: u128) !roots.Request {
        const key = try root();
        if (owner.binding.authority == .native) {
            const prepared = try evidence.SignedPreparation.sign(.{ .binding = owner.binding, .physical_root_incarnation = destination_root, .position = position(owner, 2) }, try identity(head), 4, try authority(head, owner), 6, key);
            return .{ .lookup = .fromHead(head), .ordinal = owner.ordinal, .revision = head.revision, .owner_digest = try parent.ownerDigest(owner, head), .expected_epoch = owner.root_epoch, .lineage = .native_recovery, .proposal = .{ .native_bootstrap = prepared } };
        }
        const scope: evidence.RaftScope = .{
            .metadata_incarnation = head.metadata_incarnation,
            .binding_digest = try evidence.bindingDigest(owner.binding),
            .identity = try identity(head),
            .reporter_incarnation = 4,
            .replica_id = 42,
            .group_id = owner.binding.fence.owner_group_id,
            .root_generation = 6,
            .physical_root_incarnation = destination_root,
            .reported_term = 2,
            .reported_applied_index = 3,
        };
        const prepared = try evidence.SignedPreparation.sign(.{ .binding = owner.binding, .physical_root_incarnation = destination_root, .position = position(owner, 2) }, scope.identity, 4, .{ .raft = 42 }, 6, key);
        const proof = try @import("schema_validation_bootstrap.zig").CertifiedPreparation.sign(prepared, .{ .incarnation = 77, .term = 2, .applied_index = 3 }, scope, key);
        return .{ .lookup = .fromHead(head), .ordinal = owner.ordinal, .revision = head.revision, .owner_digest = try parent.ownerDigest(owner, head), .expected_epoch = owner.root_epoch, .lineage = .raft_replay, .proposal = .{ .bootstrap = proof } };
    }

    fn root() !local.storage_db_root_signing_identity.State {
        const seed: [32]u8 = @splat(9);
        return .{ .seed = seed, .root_incarnation = 7, .public_key = (try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed)).public_key.toBytes() };
    }
    fn identity(head: barriers.Head) !@import("store_root_enrollment.zig").Identity {
        return .{ .metadata_incarnation = head.metadata_incarnation, .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = (try root()).public_key };
    }
    fn authority(head: barriers.Head, owner: barriers.Owner) !@import("store_root_attestation.zig").Authority {
        return if (owner.binding.authority == .raft) .{ .raft = 42 } else .{ .native = try (@import("standalone_native_owner.zig").Binding{
            .metadata_incarnation = head.metadata_incarnation,
            .node_id = 2,
            .store_id = 3,
            .root_incarnation = 7,
        }).digest() };
    }
    fn position(owner: barriers.Owner, sequence: u64) local.storage_db_receipt_position.Position {
        return if (owner.binding.authority == .native)
            .{ .native = .{ .namespace = owner.binding.fence.namespace, .sequence = sequence } }
        else
            .{ .raft = .{ .term = 2, .index = sequence } };
    }
    fn preparation(head: barriers.Head, owner: barriers.Owner) !evidence.SignedPreparation {
        return evidence.SignedPreparation.sign(.{ .binding = owner.binding, .physical_root_incarnation = owner.physical_root_incarnation, .position = position(owner, 1) }, try identity(head), 4, try authority(head, owner), 6, try root());
    }
    fn ready(owner: barriers.Owner) contract.Header {
        return .{ .binding = owner.binding, .phase = .ready, .admitted = position(owner, 1), .frozen = position(owner, 2), .updated = position(owner, 3), .revision = 3, .records = if (owner.binding.purpose.schemaOnly()) 0 else 2, .rows_scanned = if (owner.binding.purpose.schemaOnly()) 0 else 2, .cursor_digest = contract.hash(""), .chain = @splat(14), .last_effect = @splat(15) };
    }
    fn installed(head: barriers.Head, owner: barriers.Owner) !contract.Header {
        var header = ready(owner);
        const ready_digest = try header.digest();
        header.installation = .{ .decision = try head.installDecision(), .ready_digest = ready_digest, .ready_position = header.updated, .ready_last_effect = header.last_effect, .physical_catalog = @splat(16), .activation = @splat(17), .position = position(owner, 4) };
        header.phase = .installed;
        header.revision += 1;
        header.updated = position(owner, 4);
        header.last_effect = @splat(18);
        return header;
    }
    fn released(head: barriers.Head, owner: barriers.Owner) !contract.Header {
        var header = try installed(head, owner);
        const installed_digest = try header.digest();
        header.release = .{ .publication = try head.publicationDecision(), .installed_digest = installed_digest, .installed_last_effect = header.last_effect };
        header.phase = .released;
        header.revision += 1;
        header.updated = position(owner, 5);
        header.last_effect = @splat(19);
        return header;
    }
    fn signed(head: barriers.Head, owner: barriers.Owner, header: contract.Header) !evidence.SignedObservation {
        const signer = try identity(head);
        const domain = try authority(head, owner);
        return if (owner.binding.authority == .native) .{ .native = try evidence.SignedNativeObservation.sign(.{
            .root_generation = 6,
            .observation = .{ .binding = owner.binding, .physical_root_incarnation = owner.physical_root_incarnation, .sequence = header.updated.native.sequence, .header = header, .replication_pending = false },
        }, .{
            .metadata_incarnation = head.metadata_incarnation,
            .binding_digest = try evidence.bindingDigest(owner.binding),
            .signing = .{ .identity = signer, .reporter_incarnation = 4, .binding_digest = domain.native },
            .root_generation = 6,
            .physical_root_incarnation = owner.physical_root_incarnation,
        }, try root()) } else .{ .raft = try evidence.SignedRaftObservation.sign(.{
            .root_generation = 6,
            .observation = .{ .binding = owner.binding, .physical_root_incarnation = owner.physical_root_incarnation, .applied = header.updated.raft, .header = header },
        }, .{
            .metadata_incarnation = head.metadata_incarnation,
            .binding_digest = try evidence.bindingDigest(owner.binding),
            .identity = signer,
            .reporter_incarnation = 4,
            .replica_id = 42,
            .group_id = owner.binding.fence.owner_group_id,
            .root_generation = 6,
            .physical_root_incarnation = owner.physical_root_incarnation,
            .reported_term = 2,
            .reported_applied_index = 1,
        }, try root()) };
    }
};

const FaultTxn = struct {
    base: *local.storage_docstore.DocStore.Txn,
    fail_write: usize = std.math.maxInt(usize),
    after: bool = false,
    writes: usize = 0,
    reads: usize = 0,
    plan_reads: usize = 0,
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        self.reads += 1;
        if (try Key.parse(key)) |parsed| if (parsed.kind == .plan) {
            self.plan_reads += 1;
        };
        return self.base.get(key);
    }
    pub fn openCursor(self: *@This()) !local.storage_docstore.DocStore.Txn.CursorAdapter {
        return self.base.openCursor();
    }
    fn checkpoint(self: *@This()) !void {
        self.writes += 1;
        if (!self.after and self.writes == self.fail_write) return error.InjectedStorageFailure;
    }
    fn afterWrite(self: *@This()) !void {
        if (self.after and self.writes == self.fail_write) return error.InjectedStorageFailure;
    }
    pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
        try self.checkpoint();
        try self.base.put(key, value);
        try self.afterWrite();
    }
    pub fn delete(self: *@This(), key: []const u8) !void {
        try self.checkpoint();
        try self.base.delete(key);
        try self.afterWrite();
    }
};

test "schema validation publication root adoption preserves acknowledgements atomically across replacement and cold recovery" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/root-epochs", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preps = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    var first: roots.Request = undefined;
    var second: roots.Request = undefined;
    var first_receipt: @import("schema_validation_receipt.zig").Request = undefined;
    var accepted: barriers.Owner = undefined;
    const original_ack = try TestProofs.signed(prepared.head, prepared.owners[1], TestProofs.ready(prepared.owners[1]));
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        try store.put(TestContext.table_key, fixture.plan().before.schema_json);
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            try begin(a, &write, &prepared, &preps, &.{input}, &context);
            _ = try acknowledge(&write, lookup, 1, prepared.head.revision, original_ack, &context);
            try write.commit();
        }
        var before: barriers.Head = undefined;
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            before = try readHead(&write, lookup);
            accepted = try readOwner(&write, before, 1);
            const source = try TestProofs.parentRequest(before, accepted, TestProofs.ready(accepted));
            _ = try authorizeParent(&write, source, &context);
            first = try TestProofs.adoptionRequest(before, accepted, source, 99);
            const control = (try readRestorationControl(&write, lookup, try source.image())).?;
            try std.testing.expectEqualDeep(first, (try control.prepareAdoption(first.proposal.audited.destination, first.proposal.audited.observation)).?);
            try std.testing.expectEqualDeep(first, (try control.prepareAdoptionOrReplay(first.proposal.audited.destination, first.proposal.audited.observation)).?);
            const command_bytes = try std.json.Stringify.valueAlloc(a, Command{ .adopt_root = first }, .{});
            defer a.free(command_bytes);
            var parsed = try parseCommand(a, command_bytes);
            defer parsed.deinit();
            try std.testing.expectEqualDeep(first, parsed.value.adopt_root);
            try std.testing.expectEqualDeep(lookup, try commandLookup(parsed.value));
            try std.testing.expect(!requiresAdmissionAuthority(command_bytes));
            try write.commit();
        }
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            var counted: FaultTxn = .{ .base = &read };
            const pending = try prepareRootAdoption(&counted, first, &context);
            first_receipt = try commandReceipt(.{ .adopt_root = first }, pending.head);
            try std.testing.expect(!try (try observeReceipt(&read, first_receipt)).isAcknowledged(first_receipt));
            try std.testing.expect(pending.epoch != null);
            try std.testing.expectEqual(@as(usize, 0), counted.writes);
            try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
            context.reject = true;
            try std.testing.expectError(error.StoreRootEnrollmentChanged, prepareRootAdoption(&counted, first, &context));
            context.reject = false;
            var stale = first;
            stale.owner_digest[0] ^= 1;
            try std.testing.expectError(error.GenerationPublicationChanged, prepareRootAdoption(&counted, stale, &context));
        }
        for ([_]bool{ false, true }) |after| {
            for (1..5) |write_index| {
                var write = try store.beginWriteTxn();
                var fault: FaultTxn = .{ .base = &write, .fail_write = write_index, .after = after };
                try std.testing.expectError(error.InjectedStorageFailure, adoptRoot(&fault, first, &context));
                write.abort();
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectEqualDeep(before, try readHead(&read, lookup));
                try std.testing.expectEqualDeep(accepted, try readOwner(&read, before, 1));
                try verifyGroup(a, &read, lookup.group, &context);
            }
        }
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            const head = try adoptRoot(&write, first, &context);
            const owner = try readOwner(&write, head, 1);
            try std.testing.expectEqual(before.revision + 1, head.revision);
            var unchanged = head;
            unchanged.revision = before.revision;
            try std.testing.expectEqualDeep(before, unchanged);
            var restored = owner;
            restored.root_epoch = 0;
            restored.root_origin = 0;
            restored.root_digest = @splat(0);
            restored.physical_root_incarnation = accepted.physical_root_incarnation;
            try std.testing.expectEqualDeep(accepted, restored);
            try std.testing.expectEqual(@as(u64, 1), owner.root_epoch);
            try std.testing.expectEqual(@as(u128, 99), owner.physical_root_incarnation);
            const retained = (try readParentAuthorization(&write, lookup, 1, first.lineage.retained_parent)).?;
            const control = (try readRestorationControl(&write, lookup, try retained.image())).?;
            try std.testing.expect((try control.prepareAdoption(first.proposal.audited.destination, first.proposal.audited.observation)) == null);
            context.reject = true;
            const calls = context.verifies;
            _ = try acknowledge(&write, lookup, 1, head.revision, original_ack, &context);
            try std.testing.expectEqual(calls, context.verifies);
            try std.testing.expect((try readParentAuthorization(&write, lookup, 1, first.lineage.retained_parent)) != null);
            context.reject = false;
            // Re-attesting the same phase changes only its root commitment,
            // never the logical ACK count or the immutable first preparation.
            const next = try acknowledge(&write, lookup, 1, head.revision, try TestProofs.signed(head, owner, TestProofs.ready(owner)), &context);
            const refreshed = try readOwner(&write, next, 1);
            try std.testing.expectEqual(@as(u64, 1), refreshed.proof_epochs[@backingInt(barriers.Stage.ready)]);
            try std.testing.expectEqual(head.ready, next.ready);
            const source = try TestProofs.parentRequest(next, refreshed, TestProofs.ready(refreshed));
            _ = try authorizeParent(&write, source, &context);
            second = try TestProofs.adoptionRequest(next, refreshed, source, 100);
            _ = try adoptRoot(&write, second, &context);
            const old_parent = (try readParentAuthorization(&write, lookup, 1, first.lineage.retained_parent)).?;
            const old_image = (try readRestorationControl(&write, lookup, try old_parent.image())).?;
            try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, old_image.prepareAdoption(first.proposal.audited.destination, first.proposal.audited.observation));
            try std.testing.expectError(error.InvalidControlReceiptPosition, old_image.prepareAdoptionOrReplay(first.proposal.audited.destination, first.proposal.audited.observation));
            var replay_observation = first.proposal.audited.observation;
            replay_observation.rooted.completed = .{ .incarnation = 77, .term = 2, .applied_index = 3 };
            replay_observation.proof = try replay_observation.proof.sign(try TestProofs.root(), try replay_observation.evidenceDigest());
            const replay = (try old_image.prepareAdoptionOrReplay(first.proposal.audited.destination, replay_observation)).?;
            try std.testing.expect(replay.lineage == .raft_replay);
            try std.testing.expectEqual(old_image.owner.root_epoch, replay.expected_epoch);
            // The accepting writer still verifies current live authority and
            // all retained ACK cuts. An old grant supplies no replay authority.
            const qualified = try prepareRootAdoption(&write, replay, &context);
            try std.testing.expect(qualified.epoch != null);
            try write.commit();
        }
    }
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    context.reject = true;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const calls = context.verifies;
        var counted: FaultTxn = .{ .base = &read };
        try std.testing.expect((try prepareRootAdoption(&counted, first, &context)).epoch == null);
        try std.testing.expectEqual(@as(usize, 8), counted.reads);
        try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        try std.testing.expect((try prepareRootAdoption(&read, second, &context)).epoch == null);
        try std.testing.expectEqual(calls, context.verifies);
        try std.testing.expect(try (try observeReceipt(&read, first_receipt)).isAcknowledged(first_receipt));
        var wrong_receipt = first_receipt;
        wrong_receipt.expected.root_adoption.request_digest[0] ^= 1;
        try std.testing.expect(!try (try observeReceipt(&read, wrong_receipt)).isAcknowledged(wrong_receipt));
        var collision = first;
        collision.proposal.audited.destination.value.checkpoint_digest[0] ^= 1;
        try std.testing.expectError(error.GenerationPublicationChanged, prepareRootAdoption(&read, collision, &context));
        var recovered = try recover(a, &read, lookup);
        defer recovered.deinit();
        try std.testing.expectEqual(@as(u64, 2), recovered.owners[1].root_epoch);
        try std.testing.expectEqual(accepted.initialRoot(), recovered.owners[1].initialRoot());
        try verifyGroup(a, &read, lookup.group, &context);
    }
    // An archive is mandatory recovery evidence, not an optional cache.
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var key_buf: [192]u8 = undefined;
        try write.delete(try historyKey(lookup, 1, .ready, accepted.proofs[@backingInt(barriers.Stage.ready)]).encode(&key_buf));
        try std.testing.expectError(error.NotFound, recover(a, &write, lookup));
    }
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var key_buf: [192]u8 = undefined;
        try write.delete(try (try rootKey(lookup, 1, 1)).encode(&key_buf));
        try std.testing.expect((try observeReceipt(&write, first_receipt)).observed == null);
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, prepareRootAdoption(&write, first, &context));
        try std.testing.expectError(error.NotFound, recover(a, &write, lookup));
    }
}

test "schema validation publication pre-begin root recovery preserves zero progress through faults restart and later installation" {
    try testBootstrapRootRecovery(.raft);
    try testBootstrapRootRecovery(.native);
}

fn testBootstrapRootRecovery(kind: local.storage_source_authority.Kind) !void {
    // Real durable ledger/cryptographic faults. The compiled-owner gate
    // independently issues this proof from an actual native preparation.
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/bootstrap-root", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.authority = kind;
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preparations = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    var request: roots.Request = undefined;
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        try store.put(TestContext.table_key, fixture.plan().before.schema_json);
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            try begin(a, &write, &prepared, &preparations, &.{input}, &context);
            try write.commit();
        }
        const owner = prepared.owners[0];
        request = try TestProofs.bootstrapRequest(prepared.head, owner, 87);
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            const control = (try readReplayControl(&read, lookup, .fromBinding(owner.binding))).?;
            const derived = if (kind == .native)
                try @import("schema_validation_read.zig").prepareNativeAdoption(control, request.proposal)
            else
                try @import("schema_validation_read.zig").prepareBootstrapAdoption(control, request.proposal.bootstrap);
            try std.testing.expectEqualDeep(request, derived.?);
            var counted: FaultTxn = .{ .base = &read };
            try std.testing.expect((try prepareRootAdoption(&counted, request, &context)).epoch != null);
            try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
            try std.testing.expectEqual(@as(usize, 0), counted.writes);
            var corrupt = request;
            if (kind == .native) corrupt.proposal.native_bootstrap.proof.signature[0] ^= 1 else corrupt.proposal.bootstrap.proof.signature[0] ^= 1;
            try std.testing.expectError(error.InvalidGenerationPublication, prepareRootAdoption(&read, corrupt, &context));
            corrupt = request;
            corrupt.lineage = .{ .retained_parent = @splat(1) };
            try std.testing.expectError(error.InvalidGenerationPublication, prepareRootAdoption(&read, corrupt, &context));
            context.reject = true;
            try std.testing.expectError(error.StoreRootEnrollmentChanged, prepareRootAdoption(&read, request, &context));
            context.reject = false;
        }
        for ([_]bool{ false, true }) |after| for (1..4) |failure| {
            var write = try store.beginWriteTxn();
            var fault: FaultTxn = .{ .base = &write, .fail_write = failure, .after = after };
            try std.testing.expectError(error.InjectedStorageFailure, adoptRoot(&fault, request, &context));
            write.abort();
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualDeep(prepared.head, try readHead(&read, lookup));
            try std.testing.expectEqualDeep(owner, try readOwner(&read, prepared.head, 0));
            try verifyGroup(a, &read, lookup.group, &context);
        };
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        _ = try adoptRoot(&write, request, &context);
        try write.commit();
    }
    // The old reporter disappeared and the adoption reply was lost. Recovery
    // reads historical acceptance without asking it to sign anything again.
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    context.reject = true;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const head = try readHead(&read, lookup);
        const owner = try readOwner(&read, head, 0);
        try std.testing.expectEqual(@as(u64, 1), owner.root_epoch);
        try std.testing.expectEqual(prepared.owners[0].initialRoot(), owner.initialRoot());
        try std.testing.expectEqual(@as(u128, 87), owner.physical_root_incarnation);
        try std.testing.expectEqual(@as(u16, 0), head.ready);
        try std.testing.expect((try prepareRootAdoption(&read, request, &context)).epoch == null);
        const receipt = try commandReceipt(.{ .adopt_root = request }, head);
        try std.testing.expect(try (try observeReceipt(&read, receipt)).isAcknowledged(receipt));
    }
    context.reject = false;
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        var head = try readHead(&write, lookup);
        const original = try readOwner(&write, head, 0);
        const retained = try TestProofs.bootstrapRequest(head, original, 88);
        const peer = try readOwner(&write, head, 1);
        head = try acknowledge(&write, lookup, 1, head.revision, try TestProofs.signed(head, peer, TestProofs.ready(peer)), &context);
        const rebased = try retained.rebaseForReplay(head, original);
        try std.testing.expectEqualDeep(retained.proposal, rebased.proposal);
        try std.testing.expectEqual(head.revision, rebased.revision);
        head = try acknowledge(&write, lookup, 0, head.revision, try TestProofs.signed(head, original, TestProofs.ready(original)), &context);
        const owner = try readOwner(&write, head, 0);
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, retained.rebaseForReplay(head, owner));
        const forbidden = try TestProofs.bootstrapRequest(head, owner, 88);
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, prepareRootAdoption(&write, forbidden, &context));
        _ = try decideInstall(&write, lookup, head.revision);
        try write.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    var recovered = try recover(a, &read, lookup);
    defer recovered.deinit();
    try std.testing.expect(recovered.head.install_revision > request.revision);
    try verifyGroup(a, &read, lookup.group, &context);
}

test "schema validation publication sparse recovery discovery bounds live locks and never loads plans" {
    const a = std.testing.allocator;
    const work = @import("schema_validation_work.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/sparse-work", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const preparations = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    try store.put(TestContext.table_key, fixture.plan().before.schema_json);
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try begin(a, &write, &prepared, &preparations, &.{input}, &context);
        // Unrelated active lifecycle kinds precede table 7 in canonical key
        // order. No index/FK head or terminal-history scan may be requested.
        for (100..150) |table| try lifecycle.acquire(&write, prepared.head.metadata_group_id, table, .{ .kind = .index_build, .id = @splat(1) });
        try write.commit();
    }
    const Txn = local.storage_docstore.DocStore.Txn;
    const Counted = struct {
        base: *Txn,
        points: usize = 0,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.points += 1;
            if (try Key.parse(key)) |decoded| if (decoded.kind == .plan or decoded.kind == .input) return error.UnexpectedImmutablePlanRead;
            return self.base.get(key);
        }
        pub fn openCursor(self: *@This()) @TypeOf(@as(*Txn, undefined).openCursor()) {
            return self.base.openCursor();
        }
    };
    var read = try store.beginReadTxn();
    defer read.abort();
    var counted: Counted = .{ .base = &read };
    var query: work.Query = .{ .group = prepared.head.metadata_group_id, .metadata_incarnation = prepared.head.metadata_incarnation };
    var tasks: usize = 0;
    var pages: usize = 0;
    while (pages < 16) : (pages += 1) {
        counted.points = 0;
        const page = try work.read(&counted, query);
        try page.validate(query);
        try std.testing.expect(counted.points <= work.max_tasks + 3);
        for (page.tasks[0..page.len]) |slot| {
            const task = slot.?;
            try std.testing.expectEqual(tasks, task.ordinal);
            try std.testing.expectEqualDeep(Lookup.fromHead(prepared.head), task.lookup);
            try std.testing.expectEqual(prepared.owners[tasks].binding.fence.owner_group_id, task.group_id);
            tasks += 1;
        }
        if (page.next) |next| query.cursor = next else break;
    }
    try std.testing.expect(pages >= 4 and pages < 16);
    try std.testing.expectEqual(@as(usize, 2), tasks);
    query.cursor = .{ .owners = .{ .lookup = .fromHead(prepared.head), .ordinal = 1 } };
    const resumed = try work.read(&counted, query);
    try std.testing.expectEqual(@as(u8, 1), resumed.len);
    try std.testing.expectEqual(@as(u16, 1), resumed.tasks[0].?.ordinal);
    try std.testing.expectEqualDeep(work.Cursor{ .scan = prepared.head.table_id }, resumed.next.?);
    var malformed = resumed;
    malformed.tasks[1] = resumed.tasks[0];
    try std.testing.expectError(error.InvalidCatalogRecord, malformed.validate(query));
    malformed = resumed;
    malformed.next = .{ .owners = .{ .lookup = .fromHead(prepared.head), .ordinal = 1 } };
    try std.testing.expectError(error.InvalidCatalogRecord, malformed.validate(query));
    query.cursor.owners.lookup.plan_digest[0] ^= 1;
    try std.testing.expectError(error.GenerationPublicationChanged, work.read(&counted, query));
    query.cursor.owners.lookup = .fromHead(prepared.head);
    query.cursor.owners.lookup.id[0] ^= 1;
    try std.testing.expectError(error.GenerationPublicationChanged, work.read(&counted, query));
}

test "schema validation publication recovery work pages large compact cohorts in canonical key order" {
    const a = std.testing.allocator;
    const work = @import("schema_validation_work.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/owner-pages", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    // Compact scheduling fixture only: deliberately retain no immutable plan,
    // preparation, data or signatures. This is not admission/restore evidence.
    var head = prepared.head;
    head.owner_count = 17;
    head.input_count = 1;
    var cohort: [17]barriers.Owner = undefined;
    for (&cohort, 0..) |*owner, ordinal| {
        owner.* = prepared.owners[0];
        owner.ordinal = @intCast(ordinal);
        owner.input_ordinal = 0;
        owner.physical_root_incarnation = 17 + ordinal;
        owner.binding.fence.owner_group_id = 1000 + ordinal;
        owner.binding.fence.peer_group_id = 1000 + ordinal;
        owner.binding.fence.namespace.shard_id = 2000 + ordinal;
        owner.binding.fence.namespace.range_id = 3000 + ordinal;
    }
    try barriers.validateCohort(head, &cohort);
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        var buf: [192]u8 = undefined;
        var key: Key = .{ .group = head.metadata_group_id, .table = head.table_id, .id = head.id, .kind = .head };
        const encoded = try head.encode();
        try write.put(try key.encode(&buf), encoded.slice());
        for (cohort) |owner| {
            key.kind = .owner;
            key.ordinal = owner.ordinal;
            const record = try owner.encode(head);
            try write.put(try key.encode(&buf), record.slice());
        }
        key.kind = .active;
        key.id = @splat(0);
        key.ordinal = 0;
        try write.put(try key.encode(&buf), &head.id);
        try lifecycle.acquire(&write, head.metadata_group_id, head.table_id, .{ .kind = .schema_validation, .id = head.id });
        try write.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    var query: work.Query = .{ .group = head.metadata_group_id, .metadata_incarnation = head.metadata_incarnation };
    var seen: usize = 0;
    var first: ?work.Page = null;
    for ([_]u8{ 8, 8, 1, 0 }) |count| {
        const page = try work.read(&read, query);
        try page.validate(query);
        try std.testing.expectEqual(count, page.len);
        if (first == null) first = page;
        for (page.tasks[0..page.len]) |slot| {
            try std.testing.expectEqual(seen, slot.?.ordinal);
            try std.testing.expectEqual(cohort[seen].binding.fence.owner_group_id, slot.?.group_id);
            seen += 1;
        }
        const bytes = try std.json.Stringify.valueAlloc(a, page, .{});
        defer a.free(bytes);
        try std.testing.expect(bytes.len <= work.max_wire_bytes);
        if (page.next) |next| query.cursor = next else try std.testing.expectEqual(@as(u8, 0), count);
    }
    try std.testing.expectEqual(@as(usize, 17), seen);
    query.cursor = .{ .scan = 0 };
    var malformed = first.?;
    malformed.head = null;
    try std.testing.expectError(error.InvalidCatalogRecord, malformed.validate(query));
    malformed = first.?;
    malformed.head.?.plan_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidCatalogRecord, malformed.validate(query));
    malformed = first.?;
    malformed.head.?.owner_count = 1;
    try std.testing.expectError(error.InvalidCatalogRecord, malformed.validate(query));
    malformed = first.?;
    malformed.len = work.max_tasks + 1;
    try std.testing.expectError(error.InvalidCatalogRecord, malformed.validate(query));
    malformed = first.?;
    malformed.next = .{ .scan = 0 };
    try std.testing.expectError(error.InvalidCatalogRecord, malformed.validate(query));
    var empty: work.Page = .{ .next = .{ .scan = 9 } };
    query.cursor = .{ .scan = 10 };
    try empty.validate(query); // canonical decimal keys: "10" precedes "9"
    empty.next = .{ .scan = 10 };
    query.cursor = .{ .scan = 9 };
    try std.testing.expectError(error.InvalidCatalogRecord, empty.validate(query));
    query.cursor = .{ .owners = .{ .lookup = .fromHead(head), .ordinal = 17 } };
    try std.testing.expectError(error.GenerationPublicationChanged, work.read(&read, query));
}

test "schema validation publication native root recovery keeps real namespace clocks and accepted ACKs across cold faults" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/native-root", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preparations = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    var request: roots.Request = undefined;
    var accepted: barriers.Owner = undefined;
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        try store.put(TestContext.table_key, fixture.plan().before.schema_json);
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            try begin(a, &write, &prepared, &preparations, &.{input}, &context);
            var head = try readHead(&write, lookup);
            const owner = try readOwner(&write, head, 0);
            head = try acknowledge(&write, lookup, 0, head.revision, try TestProofs.signed(head, owner, TestProofs.ready(owner)), &context);
            accepted = try readOwner(&write, head, 0);
            request = try TestProofs.nativeAdoptionRequest(head, accepted, 97);
            try write.commit();
        }
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            var counted: FaultTxn = .{ .base = &read };
            const pending = try prepareRootAdoption(&counted, request, &context);
            try std.testing.expectEqualDeep(@as(?roots.Cut, .{ .native = .{ .namespace = accepted.binding.fence.namespace, .sequence = 3 } }), pending.epoch.?.required_cut);
            try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
            try std.testing.expectEqual(@as(usize, 0), counted.writes);
            const control = (try readReplayControl(&read, lookup, .fromBinding(accepted.binding))).?;
            try std.testing.expectEqualDeep(request, (try @import("schema_validation_read.zig").prepareNativeAdoption(control, request.proposal)).?);
            var mixed = request;
            mixed.lineage = .raft_replay;
            try std.testing.expectError(error.InvalidControlReceiptPosition, prepareRootAdoption(&read, mixed, &context));
        }
        for ([_]bool{ false, true }) |after| for (1..5) |failure| {
            var write = try store.beginWriteTxn();
            var fault: FaultTxn = .{ .base = &write, .fail_write = failure, .after = after };
            try std.testing.expectError(error.InjectedStorageFailure, adoptRoot(&fault, request, &context));
            write.abort();
            var read = try store.beginReadTxn();
            defer read.abort();
            const head = try readHead(&read, lookup);
            try std.testing.expectEqualDeep(accepted, try readOwner(&read, head, 0));
            try verifyGroup(a, &read, lookup.group, &context);
        };
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        _ = try adoptRoot(&write, request, &context);
        try write.commit();
    }
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    var read = try store.beginReadTxn();
    defer read.abort();
    context.reject = true;
    try std.testing.expect((try prepareRootAdoption(&read, request, &context)).epoch == null);
    const head = try readHead(&read, lookup);
    const owner = try readOwner(&read, head, 0);
    try std.testing.expectEqual(accepted.initialRoot(), owner.initialRoot());
    try std.testing.expectEqualDeep(accepted.headers, owner.headers);
    try std.testing.expectEqualDeep(accepted.proofs, owner.proofs);
    try std.testing.expectEqual(@as(u128, 97), owner.physical_root_incarnation);
    const receipt = try commandReceipt(.{ .adopt_root = request }, head);
    try std.testing.expect(try (try observeReceipt(&read, receipt)).isAcknowledged(receipt));
    context.reject = false;
    try requireCurrentStageAuthority(&read, head, owner, .ready, &context);
    try verifyGroup(a, &read, lookup.group, &context);
}

test "schema validation publication replay discovery separates split owners sharing an identity namespace" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/shared-namespace", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    fixture.intents[1].binding.fence.namespace = fixture.intents[0].binding.fence.namespace;
    fixture.intents[1].range.doc_identity_shard_id = fixture.intents[0].range.doc_identity_shard_id;
    fixture.intents[1].range.doc_identity_range_id = fixture.intents[0].range.doc_identity_range_id;
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preparations = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        try store.put(TestContext.table_key, fixture.plan().before.schema_json);
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try begin(a, &write, &prepared, &preparations, &.{input}, &context);
        try write.commit();
    }
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    var read = try store.beginReadTxn();
    defer read.abort();
    context.reject = true;
    const first = OwnerAddress.fromBinding(prepared.owners[0].binding);
    const discovery: @import("schema_validation_read.zig").DiscoveryRequest = .{ .group = lookup.group, .table = lookup.table, .metadata_incarnation = lookup.metadata_incarnation };
    var discovered: FaultTxn = .{ .base = &read };
    try std.testing.expectEqualDeep(lookup, (try discoverActive(&discovered, discovery)).?);
    try std.testing.expectEqual(@as(usize, 3), discovered.reads);
    try std.testing.expectEqual(@as(usize, 0), discovered.plan_reads);
    try std.testing.expectEqual(@as(usize, 0), discovered.writes);
    var absent = discovery;
    absent.table += 1;
    try std.testing.expect((try discoverActive(&read, absent)) == null);
    var foreign = discovery;
    foreign.metadata_incarnation = @splat('f');
    try std.testing.expectError(error.GenerationPublicationChanged, discoverActive(&read, foreign));
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var key_buf: [192]u8 = undefined;
        try write.delete(try storageKey(lookup, .active, 0, .ready).encode(&key_buf));
        try std.testing.expectError(error.GenerationPublicationChanged, discoverActive(&write, discovery));
    }
    const second = OwnerAddress.fromBinding(prepared.owners[1].binding);
    try std.testing.expectEqualSlices(u8, &first.namespace, &second.namespace);
    try std.testing.expect(first.group_id != second.group_id);
    for ([_]OwnerAddress{ first, second }, prepared.owners) |address, expected| {
        var counted: FaultTxn = .{ .base = &read };
        const found = (try readReplayControl(&counted, lookup, address)).?;
        try std.testing.expectEqualDeep(expected, found.owner);
        try std.testing.expectEqual(@as(usize, 3), counted.reads);
        try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        try std.testing.expectEqualSlices(u8, input, try readReplayInputs(&read, lookup, address, expected.ordinal));
    }
    var missing = first;
    missing.group_id = 999;
    try std.testing.expect((try readReplayControl(&read, lookup, missing)) == null);
    try verifyGroup(a, &read, lookup.group, &context);
}

test "schema validation publication Raft replay adopts promoted and pre-job snapshot roots without a source grant" {
    // Real LSM ledger/cryptographic fault campaign, not a native-image or
    // quorum simulator. Native full audit and actual prefix production are
    // independently exercised at their owning runtime boundaries.
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/replay-root", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preparations = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    var accepted: barriers.Owner = undefined;
    var before: barriers.Head = undefined;
    var first: roots.Request = undefined;
    var second: roots.Request = undefined;
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        try store.put(TestContext.table_key, fixture.plan().before.schema_json);
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            try begin(a, &write, &prepared, &preparations, &.{input}, &context);
            before = try acknowledge(&write, lookup, 1, prepared.head.revision, try TestProofs.signed(prepared.head, prepared.owners[1], TestProofs.ready(prepared.owners[1])), &context);
            accepted = try readOwner(&write, before, 1);
            try write.commit();
        }
        // This promoted replica replayed the complete log; it never installed
        // a snapshot and has no source artifact/grant to consult or fabricate.
        const absent_parent = try TestProofs.parentRequest(before, accepted, TestProofs.ready(accepted));
        first = try TestProofs.adoptionRequest(before, accepted, absent_parent, 99);
        first.lineage = .raft_replay;
        first.proposal.audited.destination.value.qualification.installation = null;
        first.proposal.audited.destination.signature = try (try TestProofs.root()).sign(&try first.proposal.audited.destination.signatureDigest());
        first.proposal.audited.observation.rooted.completed = .{ .incarnation = 77, .term = 2, .applied_index = 3 };
        first.proposal.audited.observation.proof = try first.proposal.audited.observation.proof.sign(try TestProofs.root(), try first.proposal.audited.observation.evidenceDigest());
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try readParentAuthorization(&read, lookup, 1, try absent_parent.id())) == null);
            const address = OwnerAddress.fromBinding(accepted.binding);
            var discovery: FaultTxn = .{ .base = &read };
            const control = (try readReplayControl(&discovery, lookup, address)).?;
            try std.testing.expectEqualDeep(accepted, control.owner);
            try std.testing.expectEqual(@as(usize, 0), discovery.plan_reads);
            try std.testing.expectEqual(@as(usize, 0), discovery.writes);
            try std.testing.expectEqualDeep(first, (try control.prepareAdoption(first.proposal.audited.destination, first.proposal.audited.observation)).?);
            try std.testing.expectEqualSlices(u8, input, try readReplayInputs(&read, lookup, address, 1));
            try std.testing.expectError(error.SchemaValidationJobChanged, readReplayInputs(&read, lookup, address, 0));
            var counted: FaultTxn = .{ .base = &read };
            const candidate = try prepareRootAdoption(&counted, first, &context);
            try std.testing.expectEqual(@as(usize, 0), counted.writes);
            try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
            try std.testing.expectEqualDeep(@as(?roots.Cut, .{ .raft = .{ .term = 2, .index = 3 } }), candidate.epoch.?.required_cut);
            var no_cut = candidate.epoch.?;
            no_cut.required_cut = null;
            try std.testing.expectError(error.InvalidGenerationPublication, no_cut.validate(candidate.head, null));
            var missing = first;
            missing.proposal.audited.observation.rooted.completed = null;
            try std.testing.expectError(error.InvalidControlReceiptPosition, prepareRootAdoption(&counted, missing, &context));
            var bad_image = first;
            bad_image.proposal.audited.destination.signature[0] ^= 1;
            try std.testing.expectError(error.InvalidSchemaValidationJob, prepareRootAdoption(&counted, bad_image, &context));
            var stale = first;
            stale.owner_digest[0] ^= 1;
            try std.testing.expectError(error.GenerationPublicationChanged, prepareRootAdoption(&counted, stale, &context));
            context.reject = true;
            try std.testing.expectError(error.StoreRootEnrollmentChanged, prepareRootAdoption(&counted, first, &context));
            context.reject = false;
        }
        {
            // A validly signed but uncovered preparation cannot be hidden by
            // an otherwise exact ready header or a caller-provided high cut.
            var write = try store.beginWriteTxn();
            defer write.abort();
            var future = preparations[1];
            future.value.position.?.raft.index = 4;
            future.proof = try future.proof.sign(try TestProofs.root(), try future.evidenceDigest());
            const encoded = try PreparationRecord.init(future);
            var buf: [192]u8 = undefined;
            try write.put(try storageKey(lookup, .preparation, 1, .ready).encode(&buf), encoded.slice());
            try std.testing.expectError(error.InvalidControlReceiptPosition, prepareRootAdoption(&write, first, &context));
        }
        for ([_]bool{ false, true }) |after| {
            for (1..5) |failure| {
                var write = try store.beginWriteTxn();
                var fault: FaultTxn = .{ .base = &write, .fail_write = failure, .after = after };
                try std.testing.expectError(error.InjectedStorageFailure, adoptRoot(&fault, first, &context));
                write.abort();
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectEqualDeep(before, try readHead(&read, lookup));
                try std.testing.expectEqualDeep(accepted, try readOwner(&read, before, 1));
                try verifyGroup(a, &read, lookup.group, &context);
            }
        }
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            const head = try adoptRoot(&write, first, &context);
            const owner = try readOwner(&write, head, 1);
            try std.testing.expectEqual(before.ready, head.ready);
            try std.testing.expectEqualDeep(accepted.headers, owner.headers);
            try std.testing.expectEqualDeep(accepted.proofs, owner.proofs);
            try std.testing.expectEqualDeep(accepted.accepted_revisions, owner.accepted_revisions);
            try std.testing.expectEqual(@as(u64, 1), owner.root_epoch);
            try write.commit();
        }
    }
    // The adoption reply was lost. Reopen independently with the former source
    // unavailable: exact receipt/retry observation does not revalidate liveness.
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    context.reject = true;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try prepareRootAdoption(&read, first, &context)).epoch == null);
        const address = OwnerAddress.fromBinding(accepted.binding);
        const control = (try readReplayControl(&read, lookup, address)).?;
        try std.testing.expect((try control.prepareAdoption(first.proposal.audited.destination, first.proposal.audited.observation)) == null);
        const receipt = try commandReceipt(.{ .adopt_root = first }, try readHead(&read, lookup));
        try std.testing.expect(try (try observeReceipt(&read, receipt)).isAcknowledged(receipt));
        try verifyGroup(a, &read, lookup.group, &context);
    }
    context.reject = false;
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        const head = try readHead(&write, lookup);
        const owner = try readOwner(&write, head, 1);
        const placeholder = try TestProofs.parentRequest(head, owner, TestProofs.ready(owner));
        second = try TestProofs.adoptionRequest(head, owner, placeholder, 100);
        second.lineage = .raft_replay;
        // An ordinary image predates this job and came from a different old
        // replica. Replayed native state, not that absent source, is audited.
        const installation = &second.proposal.audited.destination.value.qualification.installation.?;
        installation.source.physical_root_incarnation = 12345;
        installation.source.schema_header = null;
        installation.source.applied = .{ .term = 2, .index = 1 };
        installation.through_index = 1;
        second.proposal.audited.destination.signature = try (try TestProofs.root()).sign(&try second.proposal.audited.destination.signatureDigest());
        second.proposal.audited.observation.rooted.completed = .{ .incarnation = 78, .term = 2, .applied_index = 3 };
        second.proposal.audited.observation.proof = try second.proposal.audited.observation.proof.sign(try TestProofs.root(), try second.proposal.audited.observation.evidenceDigest());
        const adopted = try adoptRoot(&write, second, &context);
        const destination = try readOwner(&write, adopted, 1);
        try std.testing.expectEqual(@as(u64, 2), destination.root_epoch);
        try std.testing.expectEqual(@as(u128, 100), destination.physical_root_incarnation);
        try std.testing.expectEqual(accepted.initialRoot(), destination.initialRoot());
        try std.testing.expectEqualDeep(accepted.proofs, destination.proofs);
        try write.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        var recovered = try recover(a, &read, lookup);
        defer recovered.deinit();
        try std.testing.expectEqual(@as(u64, 2), recovered.owners[1].root_epoch);
        try verifyGroup(a, &read, lookup.group, &context);
    }
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var buf: [192]u8 = undefined;
        const address = OwnerAddress.fromBinding(accepted.binding);
        try write.delete(try namespaceOwnerKey(lookup, address).encode(&buf));
        try std.testing.expectError(error.NotFound, recover(a, &write, lookup));
    }
}

test "schema validation publication parent authorization survives lost replies source loss and cold restart" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/parent-grants", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preps = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    const request = try TestProofs.parentRequest(prepared.head, prepared.owners[1], null);
    var unscoped = request;
    unscoped.signed.value.origin.namespace = null;
    unscoped.signed.proof = try unscoped.signed.proof.sign(try TestProofs.root(), try unscoped.signed.evidenceDigest());
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, unscoped.validate());
    const grant_id = try request.id();
    var peer_progress = prepared.head;
    peer_progress.revision += 1;
    try request.requirePredecessor(peer_progress, prepared.owners[1]);
    try std.testing.expectError(error.GenerationPublicationChanged, request.requireCurrent(peer_progress, prepared.owners[1]));
    const receipt = try commandReceipt(.{ .authorize_parent = request }, prepared.head);
    const command = try std.json.Stringify.valueAlloc(a, Command{ .authorize_parent = request }, .{});
    defer a.free(command);
    var parsed = try parseCommand(a, command);
    defer parsed.deinit();
    try std.testing.expectEqualDeep(request, parsed.value.authorize_parent);
    try std.testing.expect(!requiresAdmissionAuthority(command));
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        try store.put(TestContext.table_key, fixture.plan().before.schema_json);
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            try begin(a, &write, &prepared, &preps, &.{input}, &context);
            try write.commit();
        }
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            var counted: FaultTxn = .{ .base = &read };
            const pending = try prepareParentAuthorization(&counted, request, &context);
            try std.testing.expect(pending.record != null);
            try std.testing.expectEqual(@as(usize, 0), counted.writes);
            try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
            try std.testing.expect(!try (try observeReceipt(&read, receipt)).isAcknowledged(receipt));
        }
        for (1..3) |write_number| for ([_]bool{ false, true }) |after| {
            var write = try store.beginWriteTxn();
            var fault: FaultTxn = .{ .base = &write, .fail_write = write_number, .after = after };
            try std.testing.expectError(error.InjectedStorageFailure, authorizeParent(&fault, request, &context));
            write.abort();
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try readParentAuthorization(&read, lookup, 1, grant_id)) == null);
            try std.testing.expect((try readParentByImage(&read, lookup, try request.image())) == null);
        };
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            _ = try authorizeParent(&write, request, &context);
            try write.commit();
        }
    }
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    context.reject = true;
    const calls = context.parent_verifies;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const repeated = try prepareParentAuthorization(&read, request, &context);
        try std.testing.expect(repeated.record == null);
        try std.testing.expectEqual(calls, context.parent_verifies);
        try std.testing.expect(try (try observeReceipt(&read, receipt)).isAcknowledged(receipt));
        const restored_parent = (try readParentByImage(&read, lookup, try request.image())).?;
        try std.testing.expectEqualDeep(request, restored_parent);
        const restoration = (try readRestorationControl(&read, lookup, try request.image())).?;
        try std.testing.expectEqualDeep(try readHead(&read, lookup), restoration.head);
        try std.testing.expectEqualDeep(try readOwner(&read, restoration.head, request.ordinal), restoration.owner);
        try std.testing.expectEqualDeep(request, restoration.parent);
        try std.testing.expectEqualSlices(u8, input, try readRecoveryInputs(&read, lookup, request.ordinal, try request.image()));
        var wrong_image = try request.image();
        wrong_image.origin_digest[0] ^= 1;
        try std.testing.expect((try readParentByImage(&read, lookup, wrong_image)) == null);
        try verifyGroup(a, &read, lookup.group, &context);
    }
    context.reject = false;
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        _ = try acknowledge(&write, lookup, 1, prepared.head.revision, try TestProofs.signed(prepared.head, prepared.owners[1], TestProofs.ready(prepared.owners[1])), &context);
        try write.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const head = try readHead(&read, lookup);
        const owner = try readOwner(&read, head, 1);
        try std.testing.expectError(error.GenerationPublicationChanged, request.requireCurrent(head, owner));
        try std.testing.expect(try (try observeReceipt(&read, receipt)).isAcknowledged(receipt));
        const missing_phase = try TestProofs.parentRequest(head, owner, null);
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, prepareParentAuthorization(&read, missing_phase, &context));
        const current = try TestProofs.parentRequest(head, owner, TestProofs.ready(owner));
        var refreshed = owner;
        refreshed.proofs[@backingInt(barriers.Stage.ready)][0] ^= 1;
        var later_head = head;
        later_head.revision += 1;
        try current.requirePredecessor(later_head, refreshed);
        try std.testing.expectError(error.GenerationPublicationChanged, current.requireCurrent(later_head, refreshed));
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, request.requirePredecessor(head, owner));
        const pending = try prepareParentAuthorization(&read, current, &context);
        try std.testing.expect(pending.record != null);
        var wrong_image = current;
        wrong_image.source_header.?.last_effect[0] ^= 1;
        try std.testing.expectError(error.SchemaValidationJobChanged, wrong_image.validate());
        context.reject = true;
        try std.testing.expectError(error.StoreRootEnrollmentChanged, prepareParentAuthorization(&read, current, &context));
        const repeated = try prepareParentAuthorization(&read, request, &context);
        try std.testing.expect(repeated.record == null);
        try verifyGroup(a, &read, lookup.group, &context);
    }
    context.reject = false;
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        var head = try readHead(&write, lookup);
        const owner = try readOwner(&write, head, 0);
        head = try acknowledge(&write, lookup, 0, head.revision, try TestProofs.signed(head, owner, TestProofs.ready(owner)), &context);
        _ = try decideInstall(&write, lookup, head.revision);
        try write.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const head = try readHead(&read, lookup);
        const owner = try readOwner(&read, head, 1);
        const installed = try TestProofs.installed(head, owner);
        const ahead = try TestProofs.parentRequest(head, owner, installed);
        try std.testing.expect((try prepareParentAuthorization(&read, ahead, &context)).record != null);
        var wrong_decision = installed;
        wrong_decision.installation.?.decision[0] ^= 1;
        const wrong = try TestProofs.parentRequest(head, owner, wrong_decision);
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, prepareParentAuthorization(&read, wrong, &context));
    }
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        var head = try readHead(&write, lookup);
        for (0..head.owner_count) |ordinal| {
            const owner = try readOwner(&write, head, @intCast(ordinal));
            head = try acknowledge(&write, lookup, @intCast(ordinal), head.revision, try TestProofs.signed(head, owner, try TestProofs.installed(head, owner)), &context);
        }
        _ = try publish(a, &write, lookup, head.revision, &context);
        try write.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const head = try readHead(&read, lookup);
        const owner = try readOwner(&read, head, 1);
        const released = try TestProofs.released(head, owner);
        const ahead = try TestProofs.parentRequest(head, owner, released);
        try std.testing.expect((try prepareParentAuthorization(&read, ahead, &context)).record != null);
        var wrong_decision = released;
        wrong_decision.release.?.publication[0] ^= 1;
        const wrong = try TestProofs.parentRequest(head, owner, wrong_decision);
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, prepareParentAuthorization(&read, wrong, &context));
        try std.testing.expect(try (try observeReceipt(&read, receipt)).isAcknowledged(receipt));
        try verifyGroup(a, &read, lookup.group, &context);
    }
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var key = try parentKey(request);
        key.grant.?[0] ^= 1;
        var buf: [192]u8 = undefined;
        const record = try parent.Record.init(request);
        try write.put(try key.encode(&buf), record.slice());
        try std.testing.expectError(error.InvalidCatalogRecord, verifyGroup(a, &write, lookup.group, &context));
    }
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var foreign = preps[1];
        foreign.proof.identity.metadata_incarnation[0] ^= 1;
        foreign.proof = try foreign.proof.sign(try TestProofs.root(), try foreign.evidenceDigest());
        const record = try PreparationRecord.init(foreign);
        var buf: [192]u8 = undefined;
        try write.put(try storageKey(lookup, .preparation, 1, .ready).encode(&buf), record.slice());
        try std.testing.expectError(error.MetadataIncarnationMismatch, readParentAuthorization(&write, lookup, 1, grant_id));
    }
}

test "schema validation publication coordinator completes durable barriers across leader changes lost replies and cold restart" {
    // Actual LSM ledger and complete signed cohort, with fixture physical
    // authority only. This qualifies coordinator durability, not a Raft quorum
    // or end-to-end public SQL admission.
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/schema-coordinator", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preps = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    const Store = local.storage_docstore.DocStore;
    var store = try Store.open(a, path, .{});
    var open = true;
    defer if (open) store.close();
    try store.put(TestContext.table_key, fixture.plan().before.schema_json);
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try begin(a, &write, &prepared, &preps, &.{input}, &context);
        try write.commit();
    }
    const Host = struct {
        store: *Store,
        context: *TestContext,
        lookup: Lookup,
        generation: u64 = 1,
        submissions: usize = 0,
        lose_reply: bool = false,
        change_authority: bool = false,
        const Authority = struct { metadata_incarnation: [32]u8, generation: u64 };
        pub fn currentAuthority(self: *@This()) !?Authority {
            return .{ .metadata_incarnation = self.lookup.metadata_incarnation, .generation = self.generation };
        }
        pub fn systemCatalog(self: *@This(), alloc: A, ctx: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(ctx.schema_validation_authority);
            switch (call) {
                .schema_validation_work => |query| {
                    var read = try self.store.beginReadTxn();
                    defer read.abort();
                    const page = try @import("schema_validation_work.zig").read(&read, query);
                    if (self.change_authority) {
                        self.change_authority = false;
                        self.generation += 1;
                    }
                    return std.json.Stringify.valueAlloc(alloc, page, .{});
                },
                .schema_validation_submit => |bytes| {
                    var parsed = try parseCommand(alloc, bytes);
                    defer parsed.deinit();
                    var write = try self.store.beginWriteTxn();
                    var write_live = true;
                    defer if (write_live) write.abort();
                    const head = switch (parsed.value) {
                        .install => |decision| try decideInstall(&write, decision.lookup, decision.revision),
                        .publish => |decision| try publish(alloc, &write, decision.lookup, decision.revision, self.context),
                        else => return error.UnexpectedCoordinatorCommand,
                    };
                    const receipt = try commandReceipt(parsed.value, head);
                    try write.commit();
                    write_live = false;
                    self.submissions += 1;
                    if (self.lose_reply) {
                        self.lose_reply = false;
                        return error.MetadataMutationOutcomeUnknown;
                    }
                    return std.json.Stringify.valueAlloc(alloc, @import("schema_validation_receipt.zig").Observation{ .lookup = self.lookup, .head = head, .observed = receipt.expected }, .{});
                },
                else => return error.UnexpectedCoordinatorRead,
            }
        }
    };
    var host: Host = .{ .store = &store, .context = &context, .lookup = lookup };
    const Coordinator = @import("schema_validation_coordinator.zig").Coordinator;
    var coordinator: Coordinator = .{};
    var now: u64 = 1;
    for (0..2) |ordinal| {
        // No metadata install decision while even one owner is unvalidated.
        coordinator = .{};
        _ = try coordinator.step(a, lookup.group, &host, now);
        try std.testing.expectEqual(@as(usize, 0), host.submissions);
        var write = try store.beginWriteTxn();
        var write_live = true;
        defer if (write_live) write.abort();
        const head = try readHead(&write, lookup);
        const owner = try readOwner(&write, head, @intCast(ordinal));
        _ = try acknowledge(&write, lookup, owner.ordinal, head.revision, try TestProofs.signed(head, owner, TestProofs.ready(owner)), &context);
        try write.commit();
        write_live = false;
        store.close();
        open = false;
        store = try Store.open(a, path, .{});
        open = true;
        now += 2 * std.time.ns_per_s;
    }
    coordinator = .{};
    host.change_authority = true;
    try std.testing.expectError(error.NotLeader, coordinator.step(a, lookup.group, &host, now));
    try std.testing.expectEqual(@as(usize, 0), host.submissions);
    coordinator = .{};
    host.lose_reply = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, coordinator.step(a, lookup.group, &host, now));
    try std.testing.expectEqual(@as(usize, 1), host.submissions);
    store.close();
    open = false;
    store = try Store.open(a, path, .{});
    open = true;
    coordinator = .{};
    _ = try coordinator.step(a, lookup.group, &host, now);
    try std.testing.expectEqual(@as(usize, 1), host.submissions);
    for (0..2) |ordinal| {
        var write = try store.beginWriteTxn();
        var write_live = true;
        defer if (write_live) write.abort();
        const head = try readHead(&write, lookup);
        const owner = try readOwner(&write, head, @intCast(ordinal));
        _ = try acknowledge(&write, lookup, owner.ordinal, head.revision, try TestProofs.signed(head, owner, try TestProofs.installed(head, owner)), &context);
        try write.commit();
        write_live = false;
        if (ordinal == 0) {
            coordinator = .{};
            _ = try coordinator.step(a, lookup.group, &host, now);
            try std.testing.expectEqual(@as(usize, 1), host.submissions);
            try std.testing.expectEqual(@as(usize, 0), context.publishes);
        }
    }
    coordinator = .{};
    host.lose_reply = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, coordinator.step(a, lookup.group, &host, now));
    store.close();
    open = false;
    store = try Store.open(a, path, .{});
    open = true;
    coordinator = .{};
    _ = try coordinator.step(a, lookup.group, &host, now);
    try std.testing.expectEqual(@as(usize, 2), host.submissions);
    try std.testing.expectEqual(@as(usize, 1), context.publishes);
    for (0..2) |ordinal| {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        const head = try readHead(&write, lookup);
        const owner = try readOwner(&write, head, @intCast(ordinal));
        _ = try acknowledge(&write, lookup, owner.ordinal, head.revision, try TestProofs.signed(head, owner, try TestProofs.released(head, owner)), &context);
        try write.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    const terminal_head = try readHead(&read, lookup);
    try std.testing.expectEqual(barriers.Phase.succeeded, terminal_head.phase);
    try std.testing.expectEqual(@as(u16, 2), terminal_head.released);
    try std.testing.expect((try lifecycle.read(&read, lookup.group, lookup.table)) == null);
    try std.testing.expectEqualStrings(fixture.plan().after.schema_json, try read.get(TestContext.table_key));
    try verifyGroup(a, &read, lookup.group, &context);
}

test "schema validation publication durable ledger atomic ACKs decisions evidence and cold recovery" {
    // Real LSM transaction/reopen campaign. TestContext deliberately supplies
    // fixture catalog/auth callbacks; this does not claim production command,
    // current enrollment, standby or public SQL activation qualification.
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/schema-publication", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preps = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    const Store = local.storage_docstore.DocStore;
    var store = try Store.open(a, path, .{});
    var open = true;
    defer if (open) store.close();
    try store.put(TestContext.table_key, fixture.plan().before.schema_json);

    {
        var read = try store.beginReadTxn();
        defer read.abort();
        var counted: FaultTxn = .{ .base = &read };
        try std.testing.expect(try checkBegin(a, &counted, &prepared, &preps, &.{input}, &context));
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        try std.testing.expectError(error.GenerationPublicationNotFound, readHead(&read, lookup));
        try std.testing.expectEqual(@as(?lifecycle.Lock, null), try lifecycle.read(&read, lookup.group, lookup.table));
    }

    for (1..12) |fail_write| for ([_]bool{ false, true }) |after| {
        var write = try store.beginWriteTxn();
        var failing: FaultTxn = .{ .base = &write, .fail_write = fail_write, .after = after };
        try std.testing.expectError(error.InjectedStorageFailure, begin(a, &failing, &prepared, &preps, &.{input}, &context));
        write.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.GenerationPublicationNotFound, readHead(&read, lookup));
        try std.testing.expectEqual(@as(?lifecycle.Lock, null), try lifecycle.read(&read, lookup.group, lookup.table));
        const status_request: @import("schema_validation_receipt.zig").Request = .{ .lookup = lookup, .expected = .admission };
        try std.testing.expect(!try (try observeReceipt(&read, status_request)).isAcknowledged(status_request));
    };
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try begin(a, &write, &prepared, &preps, &.{input}, &context);
        try write.commit();
    }
    store.close();
    open = false;
    store = try Store.open(a, path, .{});
    open = true;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try verifyGroup(a, &read, lookup.group, &context);
        var counted: FaultTxn = .{ .base = &read };
        const admission: @import("schema_validation_receipt.zig").Request = .{ .lookup = lookup, .expected = .admission };
        try std.testing.expect(try (try observeReceipt(&counted, admission)).isAcknowledged(admission));
        try std.testing.expectEqual(@as(usize, 1), counted.reads);
        try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        counted = .{ .base = &read };
        const control = try readOwnerControl(&counted, lookup, 0, .begin, &context);
        try std.testing.expectEqual(barriers.OwnerPermission.execute, control.disposition);
        try std.testing.expectEqualDeep(prepared.head, control.head);
        try std.testing.expectEqualDeep(prepared.owners[0], control.owner);
        try std.testing.expectEqual(@as(usize, 4), counted.reads);
        try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        counted = .{ .base = &read };
        const inputs = try readOwnerInputs(&counted, lookup, 0, &context);
        try std.testing.expectEqualSlices(u8, input, inputs);
        try std.testing.expectEqual(@as(usize, 5), counted.reads);
        try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        context.reject = true;
        try std.testing.expectError(error.Forbidden, readOwnerControl(&read, lookup, 0, .begin, &context));
        counted = .{ .base = &read };
        try std.testing.expectError(error.Forbidden, readOwnerInputs(&counted, lookup, 0, &context));
        try std.testing.expectEqual(@as(usize, 4), counted.reads);
        context.reject = false;
        var changed = lookup;
        changed.plan_digest[0] ^= 1;
        try std.testing.expectError(error.GenerationPublicationChanged, readOwnerControl(&read, changed, 0, .begin, &context));
        changed = lookup;
        changed.metadata_incarnation[0] ^= 1;
        try std.testing.expectError(error.GenerationPublicationChanged, readOwnerControl(&read, changed, 0, .begin, &context));
        try std.testing.expectError(error.InvalidGenerationPublication, readOwnerControl(&read, lookup, prepared.head.owner_count, .begin, &context));
        try std.testing.expectError(error.SchemaValidationPublicationRequired, readOwnerControl(&read, lookup, 0, .install, &context));
    }
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        // Point reads cannot treat a retained but unlocked/spliced head as
        // authority to restart physical work. This writer is never committed.
        var key_buf: [192]u8 = undefined;
        try write.delete(try storageKey(lookup, .active, 0, .ready).encode(&key_buf));
        try std.testing.expectError(error.GenerationPublicationChanged, readOwnerControl(&write, lookup, 0, .begin, &context));
    }
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var counted: FaultTxn = .{ .base = &write };
        context.reject = true;
        try begin(a, &counted, &prepared, &preps, &.{input}, &context);
        context.reject = false;
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
    }
    var head = prepared.head;
    for (prepared.owners) |owner| {
        const signed = try TestProofs.signed(head, owner, TestProofs.ready(owner));
        const status_request = try TestProofs.ackRequest(lookup, owner.ordinal, signed);
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            var counted: FaultTxn = .{ .base = &read };
            const candidate = try prepareAcknowledgement(&counted, lookup, owner.ordinal, head.revision, signed, &context);
            try std.testing.expect(candidate.changed != null);
            try std.testing.expectEqual(head.revision + 1, candidate.head.revision);
            try std.testing.expectEqual(@as(usize, 0), counted.writes);
            try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
            try std.testing.expect(counted.reads <= 6);
            try std.testing.expectEqualDeep(head, try readHead(&read, lookup));
            context.reject = true;
            defer context.reject = false;
            try std.testing.expectError(error.StoreRootEnrollmentChanged, prepareAcknowledgement(&read, lookup, owner.ordinal, head.revision, signed, &context));
            var accepted: TestContext = .{};
            try std.testing.expectError(error.GenerationPublicationChanged, prepareAcknowledgement(&read, lookup, owner.ordinal, head.revision + 1, signed, &accepted));
        }
        for (1..4) |fail_write| for ([_]bool{ false, true }) |after| {
            var write = try store.beginWriteTxn();
            var failing: FaultTxn = .{ .base = &write, .fail_write = fail_write, .after = after };
            try std.testing.expectError(error.InjectedStorageFailure, acknowledge(&failing, lookup, owner.ordinal, head.revision, signed, &context));
            try std.testing.expectEqual(@as(usize, 0), failing.plan_reads);
            try std.testing.expect(failing.reads <= 6);
            write.abort();
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualDeep(head, try readHead(&read, lookup));
            try verifyGroup(a, &read, lookup.group, &context);
            try std.testing.expect(!try (try observeReceipt(&read, status_request)).isAcknowledged(status_request));
        };
        {
            var write = try store.beginWriteTxn();
            errdefer write.abort();
            var counted: FaultTxn = .{ .base = &write };
            head = try acknowledge(&counted, lookup, owner.ordinal, head.revision, signed, &context);
            try std.testing.expectEqual(@as(usize, 3), counted.writes);
            try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
            try write.commit();
        }
        var committed = try store.beginReadTxn();
        defer committed.abort();
        var observed: FaultTxn = .{ .base = &committed };
        try std.testing.expect(try (try observeReceipt(&observed, status_request)).isAcknowledged(status_request));
        try std.testing.expectEqual(@as(usize, 3), observed.reads);
        try std.testing.expectEqual(@as(usize, 0), observed.plan_reads);
        try std.testing.expectEqual(@as(usize, 0), observed.writes);
    }
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        var counted: FaultTxn = .{ .base = &write };
        const prospective = try checkInstall(&counted, lookup, head.revision);
        try std.testing.expect(prospective.changed);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        head = try decideInstall(&write, lookup, head.revision);
        counted = .{ .base = &write };
        try std.testing.expectEqualDeep(head, try decideInstall(&counted, lookup, head.install_revision - 1));
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        try std.testing.expectError(error.SchemaValidationPublicationRequired, cancel(&write, lookup, head.revision, .requested));
        try write.commit();
    }
    for (prepared.owners) |owner| {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        const control = try readOwnerControl(&write, lookup, owner.ordinal, .install, &context);
        try std.testing.expectEqual(barriers.OwnerPermission.execute, control.disposition);
        try std.testing.expectError(error.SchemaValidationPublicationRequired, readOwnerControl(&write, lookup, owner.ordinal, .release, &context));
        head = try acknowledge(&write, lookup, owner.ordinal, head.revision, try TestProofs.signed(head, owner, try TestProofs.installed(head, owner)), &context);
        try std.testing.expectEqual(barriers.OwnerPermission.acknowledged, (try readOwnerControl(&write, lookup, owner.ordinal, .install, &context)).disposition);
        try write.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        var counted: FaultTxn = .{ .base = &read };
        var candidate = try checkPublication(a, &counted, lookup, head.revision, &context);
        defer candidate.deinit();
        try std.testing.expect(candidate.recovery != null);
        try std.testing.expectEqual(head.revision + 1, candidate.head.revision);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        try std.testing.expectEqualDeep(head, try readHead(&read, lookup));
    }
    for (1..3) |fail_write| for ([_]bool{ false, true }) |after| {
        var write = try store.beginWriteTxn();
        var failing: FaultTxn = .{ .base = &write, .fail_write = fail_write, .after = after };
        // publish's complete audit needs an actual cursor only in verifyGroup,
        // not recover; the write wrapper retains the exact same LSM writer.
        try std.testing.expectError(error.InjectedStorageFailure, publish(a, &failing, lookup, head.revision, &context));
        write.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(head, try readHead(&read, lookup));
        try std.testing.expectEqualStrings(fixture.plan().before.schema_json, try read.get(TestContext.table_key));
    };
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        head = try publish(a, &write, lookup, head.revision, &context);
        try std.testing.expectEqualDeep(head, try publish(a, &write, lookup, head.publication_revision - 1, &context));
        try write.commit();
    }
    for (prepared.owners, 0..) |owner, i| {
        const signed = try TestProofs.signed(head, owner, try TestProofs.released(head, owner));
        const write_count: usize = if (i == 1) 5 else 3;
        for (1..write_count + 1) |fail_write| for ([_]bool{ false, true }) |after| {
            var write = try store.beginWriteTxn();
            var failing: FaultTxn = .{ .base = &write, .fail_write = fail_write, .after = after };
            try std.testing.expectError(error.InjectedStorageFailure, acknowledge(&failing, lookup, owner.ordinal, head.revision, signed, &context));
            write.abort();
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualDeep(head, try readHead(&read, lookup));
            try verifyGroup(a, &read, lookup.group, &context);
        };
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        head = try acknowledge(&write, lookup, owner.ordinal, head.revision, signed, &context);
        try write.commit();
    }
    try std.testing.expectEqual(barriers.Phase.succeeded, head.phase);
    store.close();
    open = false;
    store = try Store.open(a, path, .{});
    open = true;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        var recovery = try recover(a, &read, lookup);
        defer recovery.deinit();
        try std.testing.expectEqualDeep(head, recovery.head);
        try std.testing.expectEqual(@as(usize, 6), recovery.proof_count);
        try verifyGroup(a, &read, lookup.group, &context);
        try std.testing.expectEqual(@as(?lifecycle.Lock, null), try lifecycle.read(&read, lookup.group, lookup.table));
        var counted: FaultTxn = .{ .base = &read };
        const control = try readOwnerControl(&counted, lookup, 0, .release, &context);
        try std.testing.expectEqual(barriers.OwnerPermission.acknowledged, control.disposition);
        try std.testing.expectEqual(@as(usize, 2), counted.reads);
        try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        try std.testing.expectError(error.GenerationPublicationChanged, readOwnerInputs(&read, lookup, 0, &context));
        context.reject = true;
        try std.testing.expectError(error.Forbidden, readOwnerControl(&read, lookup, 0, .release, &context));
        const old_ack = try TestProofs.ackRequest(lookup, 0, try TestProofs.signed(head, prepared.owners[0], TestProofs.ready(prepared.owners[0])));
        try std.testing.expect(try (try observeReceipt(&read, old_ack)).isAcknowledged(old_ack));
        const installation: @import("schema_validation_receipt.zig").Request = .{ .lookup = lookup, .expected = .{ .installation = .{ .revision = head.install_revision - 1, .digest = try head.installDecision() } } };
        const publication: @import("schema_validation_receipt.zig").Request = .{ .lookup = lookup, .expected = .{ .publication = .{ .revision = head.publication_revision - 1, .digest = try head.publicationDecision() } } };
        try std.testing.expect(try (try observeReceipt(&read, installation)).isAcknowledged(installation));
        try std.testing.expect(try (try observeReceipt(&read, publication)).isAcknowledged(publication));
        context.reject = false;
    }
    // Terminal source retirement must not race a late new parent grant. This
    // uses the cold-recovered real ledger, not a coordinator-side terminal bit.
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        const owner = try readOwner(&write, head, 1);
        const late = try TestProofs.parentRequest(head, owner, null);
        const checks = context.parent_verifies;
        var counted: FaultTxn = .{ .base = &write };
        try std.testing.expectError(error.GenerationPublicationChanged, authorizeParent(&counted, late, &context));
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        try std.testing.expectEqual(checks, context.parent_verifies);
    }
    // Lost terminal reply: the exact old accepted phase is still a no-op even
    // after unlock, without current enrollment becoming publication permission.
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        context.reject = true;
        var counted: FaultTxn = .{ .base = &write };
        const prior = try TestProofs.signed(head, prepared.owners[0], TestProofs.ready(prepared.owners[0]));
        try std.testing.expectEqualDeep(head, try acknowledge(&counted, lookup, 0, 1, prior, &context));
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        context.reject = false;
    }
    // Missing evidence, spliced heads and orphan rows fail recovery. Never
    // commit these corruptions: each check uses an independently aborted cut.
    for ([_]Kind{ .preparation, .proof, .owner, .input }) |kind| {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var buf: [192]u8 = undefined;
        try write.delete(try storageKey(lookup, kind, 0, .ready).encode(&buf));
        if (recover(a, &write, lookup)) |value| {
            var unexpected = value;
            unexpected.deinit();
            return error.TestExpectedError;
        } else |_| {}
    }
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var buf: [192]u8 = undefined;
        try write.put(try storageKey(lookup, .owner, 2, .ready).encode(&buf), "orphan");
        try std.testing.expectError(error.InvalidCatalogRecord, verifyGroup(a, &write, lookup.group, &context));
    }
}

test "schema validation publication control commands reject oversized JSON before allocation" {
    for ([_][]const u8{ "{", "[]", "{\"unknown\":{}}" }) |malformed|
        try std.testing.expectError(error.InvalidGenerationPublication, parseCommand(std.testing.allocator, malformed));
    const bytes = try std.testing.allocator.alloc(u8, max_control_command_bytes + 1);
    defer std.testing.allocator.free(bytes);
    @memset(bytes, ' ');
    var workspace: [0]u8 = .{};
    var empty = std.heap.FixedBufferAllocator.init(&workspace);
    try std.testing.expectError(error.InvalidGenerationPublication, parseCommand(empty.allocator(), bytes));
}

test "schema validation publication durable key codec rejects ambiguous and foreign fields" {
    var buf: [192]u8 = undefined;
    const key: Key = .{ .group = 21, .table = 7, .id = @splat(1), .kind = .proof, .ordinal = 4095, .stage = .failed };
    const encoded = try key.encode(&buf);
    try std.testing.expectEqualDeep(key, (try Key.parse(encoded)).?);
    try std.testing.expectEqual(@as(?Key, null), try Key.parse("unrelated"));
    var malformed: [192]u8 = undefined;
    @memcpy(malformed[0..encoded.len], encoded);
    malformed[all_prefix.len] = 'A';
    try std.testing.expectError(error.InvalidCatalogRecord, Key.parse(malformed[0..encoded.len]));
    var invalid = key;
    invalid.kind = .plan;
    try std.testing.expectError(error.InvalidGenerationPublication, invalid.encode(&malformed));
    const head: barriers.Head = .{ .metadata_group_id = 21, .metadata_incarnation = @splat('1'), .id = @splat(1), .plan_digest = @splat(2), .table_id = 7, .owner_count = 2 };
    const lookup = Lookup.fromHead(head);
    const root_key = try rootKey(lookup, 1, std.math.maxInt(u64));
    try std.testing.expectEqualDeep(root_key, (try Key.parse(try root_key.encode(&buf))).?);
    try std.testing.expectError(error.InvalidGenerationPublication, rootKey(lookup, 1, 0));
    var invalid_root = root_key;
    invalid_root.grant.?[0] = 1;
    try std.testing.expectError(error.InvalidGenerationPublication, invalid_root.encode(&buf));
    invalid_root = root_key;
    invalid_root.stage = .installed;
    try std.testing.expectError(error.InvalidGenerationPublication, invalid_root.encode(&buf));
    const history_key = historyKey(lookup, 1, .installed, @splat(3));
    try std.testing.expectEqualDeep(history_key, (try Key.parse(try history_key.encode(&buf))).?);
}

test "schema validation publication admission frame retains raw deduplicated inputs without JSON expansion" {
    const a = std.testing.allocator;
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    var plan = try plan_mod.parse(a, prepared.encoded);
    defer plan.deinit();
    const preps = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    const request: Begin = .{ .plan = plan.value, .preparations = &preps, .inputs = &.{input} };
    const frame = try encodeBegin(a, request);
    defer a.free(frame);
    try std.testing.expectEqual(@import("topology_protocol.zig").schema_validation_publication_version, requiredDecoderVersion(frame));
    try std.testing.expectEqual(@import("topology_protocol.zig").sql_sequence_retirement_version, requiredDecoderVersion(frame[0..15]));
    var parsed = try parseCommand(a, frame);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.begin.inputs.len);
    try std.testing.expectEqualSlices(u8, input, parsed.value.begin.inputs[0]);
    const raw = parsed.value.begin.inputs[0];
    try std.testing.expect(@intFromPtr(raw.ptr) >= @intFromPtr(frame.ptr) and
        @intFromPtr(raw.ptr) + raw.len <= @intFromPtr(frame.ptr) + frame.len);
    const plain = try std.json.Stringify.valueAlloc(a, Command{ .begin = request }, .{});
    defer a.free(plain);
    try std.testing.expect(std.mem.indexOf(u8, plain, "\"inputs\":[[") == null);
    try std.testing.expectEqual(plain.len + 52 + input.len, frame.len);
    try std.testing.expectError(error.InvalidGenerationPublication, parseCommand(a, plain));
    frame[frame.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, parseCommand(a, frame));
    frame[frame.len - 1] ^= 1;
    const saved_count = frame[12..16].*;
    std.mem.writeInt(u32, frame[12..16], barriers.max_owners + 1, .little);
    @memcpy(frame[frame.len - 32 ..], &contract.hash(frame[0 .. frame.len - 32]));
    try std.testing.expectError(error.InvalidGenerationPublication, parseCommand(a, frame));
    @memcpy(frame[12..16], &saved_count);
    @memcpy(frame[frame.len - 32 ..], &contract.hash(frame[0 .. frame.len - 32]));
    const Check = struct {
        fn run(alloc: A, value: Begin) !void {
            var table = alloc.vtable.*;
            table.resize = std.mem.Allocator.noResize;
            table.remap = std.mem.Allocator.noRemap;
            const stable: A = .{ .ptr = alloc.ptr, .vtable = &table };
            const bytes = try encodeBegin(stable, value);
            defer stable.free(bytes);
            var decoded = try parseCommand(stable, bytes);
            defer decoded.deinit();
            try std.testing.expectEqualSlices(u8, value.inputs[0], decoded.value.begin.inputs[0]);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{request});
}

test "schema validation publication cancellation is durable before owner admission and releases only the full cohort" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/validation-cancel", .{tmp.sub_path}, 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    const input = try contract.encodeInputs(a, .{ fixture.plan().before.schema_json, fixture.plan().after.schema_json, "fixture-catalog", "[\"id\"]" });
    defer a.free(input);
    for (&fixture.intents) |*intent| {
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    var prepared = try plan_mod.Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preps = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    try store.put(TestContext.table_key, fixture.plan().before.schema_json);
    var head = prepared.head;
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try begin(a, &write, &prepared, &preps, &.{input}, &context);
        var counted: FaultTxn = .{ .base = &write };
        const candidate = try checkCancellation(&counted, lookup, head.revision, .requested);
        try std.testing.expect(candidate.changed);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        head = try cancel(&write, lookup, head.revision, .requested);
        counted = .{ .base = &write };
        try std.testing.expectEqualDeep(head, try cancel(&counted, lookup, head.cancellation_revision - 1, .requested));
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        try write.commit();
    }
    for (prepared.owners, 0..) |owner, i| {
        // Physical cancellation can precede local begin. Its actual clock is
        // still signed, but no admitted/ready/install position is fabricated.
        const header: contract.Header = .{ .binding = owner.binding, .phase = .canceled, .admitted = null, .updated = TestProofs.position(owner, 2), .revision = 1, .cursor_digest = contract.hash(""), .chain = @splat(21), .last_effect = @splat(22) };
        const signed = try TestProofs.signed(head, owner, header);
        const write_count: usize = if (i == 1) 5 else 3;
        for (1..write_count + 1) |fail_write| for ([_]bool{ false, true }) |after| {
            var write = try store.beginWriteTxn();
            var failing: FaultTxn = .{ .base = &write, .fail_write = fail_write, .after = after };
            try std.testing.expectError(error.InjectedStorageFailure, acknowledge(&failing, lookup, owner.ordinal, head.revision, signed, &context));
            write.abort();
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualDeep(head, try readHead(&read, lookup));
            try verifyGroup(a, &read, lookup.group, &context);
        };
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        head = try acknowledge(&write, lookup, owner.ordinal, head.revision, signed, &context);
        try write.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    try verifyGroup(a, &read, lookup.group, &context);
    try std.testing.expectEqual(barriers.Phase.canceled, head.phase);
    try std.testing.expectEqual(@as(u16, 0), head.installed);
    try std.testing.expectEqual(@as(?lifecycle.Lock, null), try lifecycle.read(&read, lookup.group, lookup.table));
    try std.testing.expectEqualStrings(fixture.plan().before.schema_json, try read.get(TestContext.table_key));
    try std.testing.expectError(error.SchemaValidationPublicationRequired, head.installDecision());
    const cancellation: @import("schema_validation_receipt.zig").Request = .{ .lookup = lookup, .expected = .{ .cancellation = .{ .decision = .{ .revision = head.cancellation_revision - 1, .digest = try head.cancellationDecision() }, .failure = .requested } } };
    context.reject = true;
    var counted: FaultTxn = .{ .base = &read };
    try std.testing.expect(try (try observeReceipt(&counted, cancellation)).isAcknowledged(cancellation));
    try std.testing.expectEqual(@as(usize, 1), counted.reads);
    try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
    try std.testing.expectEqual(@as(usize, 0), counted.writes);
    const late = try TestProofs.parentRequest(head, try readOwner(&read, head, 1), null);
    try std.testing.expectError(error.GenerationPublicationChanged, prepareParentAuthorization(&read, late, &context));
}

test "schema validation publication FK dependency admission and final cancellation ACK are atomic across cold faults" {
    // Real metadata LSM/fault campaign with fixture catalog and signer
    // callbacks. Real FK row/parent preparation is tested by the native
    // transaction campaign; this test qualifies ledger atomicity only.
    const a = std.testing.allocator;
    const deps = @import("schema_validation_dependencies.zig");
    var directory = try local.common_test_directory.TestDirectory.init("fk-validation-ledger");
    defer directory.cleanup();
    const path = try a.dupeSentinel(u8, directory.path(), 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    var plan = fixture.plan();
    plan.before.schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"fk","child_columns":["parent_id"],"parent_table":"rows","parent_columns":["id"],"not_valid":true}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    plan.after = try @import("../api/tables.zig").prepareSchemaValidationPublicationRecord(a, &plan.before, .{ .foreign_keys = &.{"fk"} });
    defer @import("table_manager.zig").freeTable(a, plan.after);
    var before = try local.schema_mod.PreparedSchemaDefinition.init(a, plan.before.schema_json);
    defer before.deinit(a);
    var after = try local.schema_mod.PreparedSchemaDefinition.init(a, plan.after.schema_json);
    defer after.deinit(a);
    const before_native = try local.storage_schema.serializeSchema(a, before.runtime_schema);
    defer a.free(before_native);
    const after_native = try local.storage_schema.serializeSchema(a, after.runtime_schema);
    defer a.free(after_native);
    const source: contract.ForeignKeySource = .{ .table_name = "rows", .parents = &.{.{ .constraint_name = "fk", .declared_parent = "rows", .parent_table = "rows", .parent_table_id = 7, .parent_schema_version = 1, .parent_schema = contract.hash(before_native), .parent_public = contract.hash(plan.before.schema_json), .parent_catalog = contract.hash("fixture-catalog"), .parent_unique = "pk", .parent_generation = @splat(4), .parent_definition = @splat(41) }} };
    plan.selection = .{ .foreign_keys = source };
    const payload = try plan.selection.encodePayload(a);
    defer a.free(payload);
    const input = try contract.encodeInputs(a, .{ plan.before.schema_json, plan.after.schema_json, "fixture-catalog", payload });
    defer a.free(input);
    var intents = fixture.intents;
    for (&intents) |*intent| {
        intent.binding.purpose = .foreign_key_source;
        intent.binding.before_schema = contract.hash(before_native);
        intent.binding.after_schema = contract.hash(after_native);
        intent.binding.before_public = contract.hash(plan.before.schema_json);
        intent.binding.after_public = contract.hash(plan.after.schema_json);
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    plan.owners = &intents;
    var prepared = try plan_mod.Prepared.init(a, plan);
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preps = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    const Store = local.storage_docstore.DocStore;
    var store = try Store.open(a, path, .{});
    var live = true;
    defer if (live) store.close();
    try store.put(TestContext.table_key, plan.before.schema_json);
    for (1..14) |fail_write| for ([_]bool{ false, true }) |after_write| {
        var write = try store.beginWriteTxn();
        var failing: FaultTxn = .{ .base = &write, .fail_write = fail_write, .after = after_write };
        try std.testing.expectError(error.InjectedStorageFailure, begin(a, &failing, &prepared, &preps, &.{input}, &context));
        write.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        try deps.requireUnleased(&read, lookup.group, 7);
        try std.testing.expectError(error.GenerationPublicationNotFound, readHead(&read, lookup));
    };
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try begin(a, &write, &prepared, &preps, &.{input}, &context);
        var head = try readHead(&write, lookup);
        const initial_owner = try readOwner(&write, head, 0);
        head = try adoptRoot(&write, try TestProofs.bootstrapRequest(head, initial_owner, 97), &context);
        const adopted_owner = try readOwner(&write, head, 0);
        try std.testing.expectEqual(@as(u128, 97), adopted_owner.physical_root_incarnation);
        try std.testing.expectEqual(@as(u64, 1), adopted_owner.root_epoch);
        try std.testing.expectEqual(@as(u16, 0), head.ready);
        try deps.requireSource(a, &write, head, source, false);
        var ready = TestProofs.ready(adopted_owner);
        ready.transaction_coverage = .{ .record = @splat(43), .physical_root = adopted_owner.physical_root_incarnation };
        head = try acknowledge(&write, lookup, adopted_owner.ordinal, head.revision, try TestProofs.signed(head, adopted_owner, ready), &context);
        try std.testing.expectEqual(@as(u16, 1), head.ready);
        head = try cancel(&write, lookup, head.revision, .requested);
        const previous_owner = try readOwner(&write, head, 0);
        var canceled = ready;
        canceled.phase = .canceled;
        canceled.revision += 1;
        canceled.updated = TestProofs.position(previous_owner, 4);
        // Fixture proof for the ledger reducer: the real physical cancellation
        // and composite audits are exercised by the native FK transaction gate.
        const adoption = try TestProofs.nativeAdoptionRequestForHeader(head, previous_owner, 98, canceled);
        head = try adoptRoot(&write, adoption, &context);
        const owner = try readOwner(&write, head, 0);
        try std.testing.expectEqual(@as(u64, 2), owner.root_epoch);
        try std.testing.expectEqual(@as(u128, 98), owner.physical_root_incarnation);
        try std.testing.expectEqualDeep(previous_owner.headers, owner.headers);
        try std.testing.expectEqual(@as(u16, 1), head.ready);
        try deps.requireSource(a, &write, head, source, false);
        _ = try acknowledge(&write, lookup, owner.ordinal, head.revision, try TestProofs.signed(head, owner, canceled), &context);
        try write.commit();
    }
    store.close();
    live = false;
    store = try Store.open(a, path, .{});
    live = true;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try deps.requireSource(a, &read, try readHead(&read, lookup), source, false);
        var recovered = try recover(a, &read, lookup);
        defer recovered.deinit();
        try std.testing.expectEqual(@as(u16, 1), recovered.head.canceled);
        try std.testing.expectEqual(@as(u64, 2), recovered.owners[0].root_epoch);
        try std.testing.expectEqual(@as(u128, 98), recovered.owners[0].physical_root_incarnation);
        try verifyGroup(a, &read, lookup.group, &context);
    }
    for (1..8) |fail_write| for ([_]bool{ false, true }) |after_write| {
        var write = try store.beginWriteTxn();
        var failing: FaultTxn = .{ .base = &write, .fail_write = fail_write, .after = after_write };
        const head = try readHead(&failing, lookup);
        const owner = try readOwner(&failing, head, 1);
        var canceled = TestProofs.ready(owner);
        canceled.reference_coverage = .{};
        canceled.phase = .canceled;
        try std.testing.expectError(error.InjectedStorageFailure, acknowledge(&failing, lookup, 1, head.revision, try TestProofs.signed(head, owner, canceled), &context));
        write.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        const retained = try readHead(&read, lookup);
        try std.testing.expectEqual(@as(u16, 1), retained.canceled);
        try deps.requireSource(a, &read, retained, source, false);
    };
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        const head = try readHead(&write, lookup);
        const owner = try readOwner(&write, head, 1);
        var canceled = TestProofs.ready(owner);
        canceled.reference_coverage = .{};
        canceled.phase = .canceled;
        _ = try acknowledge(&write, lookup, 1, head.revision, try TestProofs.signed(head, owner, canceled), &context);
        try write.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    const terminal_head = try readHead(&read, lookup);
    try std.testing.expectEqual(barriers.Phase.canceled, terminal_head.phase);
    try deps.requireSource(a, &read, terminal_head, source, true);
    try deps.requireUnleased(&read, lookup.group, 7);
    try verifyGroup(a, &read, lookup.group, &context);
}

test "schema validation publication FK released root adoption retains history and leases through terminal ACK faults" {
    // Real metadata LSM with fixture catalog/enrollment callbacks. The native
    // journal/restore campaign independently qualifies physical history proofs.
    const a = std.testing.allocator;
    const deps = @import("schema_validation_dependencies.zig");
    var directory = try local.common_test_directory.TestDirectory.init("fk-validation-release-root");
    defer directory.cleanup();
    const path = try a.dupeSentinel(u8, directory.path(), 0);
    defer a.free(path);
    var fixture = try plan_mod.test_support.PlanFixture.init(a);
    defer fixture.deinit();
    var plan = fixture.plan();
    plan.before.schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"fk","child_columns":["parent_id"],"parent_table":"rows","parent_columns":["id"],"not_valid":true}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    plan.after = try @import("../api/tables.zig").prepareSchemaValidationPublicationRecord(a, &plan.before, .{ .foreign_keys = &.{"fk"} });
    defer @import("table_manager.zig").freeTable(a, plan.after);
    var before = try local.schema_mod.PreparedSchemaDefinition.init(a, plan.before.schema_json);
    defer before.deinit(a);
    var after = try local.schema_mod.PreparedSchemaDefinition.init(a, plan.after.schema_json);
    defer after.deinit(a);
    const before_native = try local.storage_schema.serializeSchema(a, before.runtime_schema);
    defer a.free(before_native);
    const after_native = try local.storage_schema.serializeSchema(a, after.runtime_schema);
    defer a.free(after_native);
    const source: contract.ForeignKeySource = .{ .table_name = "rows", .parents = &.{.{ .constraint_name = "fk", .declared_parent = "rows", .parent_table = "rows", .parent_table_id = 7, .parent_schema_version = 1, .parent_schema = contract.hash(before_native), .parent_public = contract.hash(plan.before.schema_json), .parent_catalog = contract.hash("fixture-catalog"), .parent_unique = "pk", .parent_generation = @splat(4), .parent_definition = @splat(41) }} };
    plan.selection = .{ .foreign_keys = source };
    const payload = try plan.selection.encodePayload(a);
    defer a.free(payload);
    const input = try contract.encodeInputs(a, .{ plan.before.schema_json, plan.after.schema_json, "fixture-catalog", payload });
    defer a.free(input);
    var intents = fixture.intents;
    for (&intents) |*intent| {
        intent.binding.purpose = .foreign_key_source;
        intent.binding.before_schema = contract.hash(before_native);
        intent.binding.after_schema = contract.hash(after_native);
        intent.binding.before_public = contract.hash(plan.before.schema_json);
        intent.binding.after_public = contract.hash(plan.after.schema_json);
        intent.binding.inputs = contract.hash(input);
        intent.binding.fence.catalog_digest = contract.hash("fixture-catalog");
    }
    plan.owners = &intents;
    var prepared = try plan_mod.Prepared.init(a, plan);
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const preps = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var context: TestContext = .{};
    const Store = local.storage_docstore.DocStore;
    var store = try Store.open(a, path, .{});
    var live = true;
    defer if (live) store.close();
    try store.put(TestContext.table_key, plan.before.schema_json);
    var installed: [2]contract.Header = undefined;
    var released: [2]contract.Header = undefined;
    var request: roots.Request = undefined;
    var previous: barriers.Owner = undefined;
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try begin(a, &write, &prepared, &preps, &.{input}, &context);
        var head = try readHead(&write, lookup);
        var ready: [2]contract.Header = undefined;
        for (prepared.owners, 0..) |owner, i| {
            ready[i] = TestProofs.ready(owner);
            ready[i].transaction_coverage = .{ .record = @splat(43), .physical_root = if (owner.binding.authority == .native) owner.physical_root_incarnation else 0 };
            head = try acknowledge(&write, lookup, owner.ordinal, head.revision, try TestProofs.signed(head, owner, ready[i]), &context);
        }
        head = try decideInstall(&write, lookup, head.revision);
        for (prepared.owners, 0..) |owner, i| {
            installed[i] = ready[i];
            installed[i].installation = .{ .decision = try head.installDecision(), .ready_digest = try ready[i].digest(), .ready_position = ready[i].updated, .ready_last_effect = ready[i].last_effect, .physical_catalog = @splat(16), .activation = @splat(17), .position = TestProofs.position(owner, 4) };
            installed[i].phase = .installed;
            installed[i].revision += 1;
            installed[i].updated = TestProofs.position(owner, 4);
            installed[i].last_effect = @splat(18);
            head = try acknowledge(&write, lookup, owner.ordinal, head.revision, try TestProofs.signed(head, owner, installed[i]), &context);
        }
        head = try publish(a, &write, lookup, head.revision, &context);
        for (prepared.owners, 0..) |owner, i| {
            released[i] = installed[i];
            released[i].release = .{ .publication = try head.publicationDecision(), .installed_digest = try installed[i].digest(), .installed_last_effect = installed[i].last_effect };
            released[i].phase = .released;
            released[i].revision += 1;
            released[i].updated = TestProofs.position(owner, 5);
            released[i].last_effect = @splat(19);
        }
        previous = try readOwner(&write, head, 0);
        request = try TestProofs.nativeAdoptionRequestForHeader(head, previous, 97, released[0]);
        try write.commit();
    }
    for ([_]bool{ false, true }) |after_write| for (1..5) |fail_write| {
        var write = try store.beginWriteTxn();
        var failing: FaultTxn = .{ .base = &write, .fail_write = fail_write, .after = after_write };
        try std.testing.expectError(error.InjectedStorageFailure, adoptRoot(&failing, request, &context));
        write.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        const head = try readHead(&read, lookup);
        try std.testing.expectEqualDeep(previous, try readOwner(&read, head, 0));
        try deps.requireSource(a, &read, head, source, false);
    };
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        _ = try adoptRoot(&write, request, &context);
        try write.commit();
    }
    store.close();
    live = false;
    store = try Store.open(a, path, .{});
    live = true;
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        // A lost adoption reply can be observed after cold restart even if
        // the previous reporter is no longer authorized to create new proofs.
        context.reject = true;
        var counted: FaultTxn = .{ .base = &write };
        var head = try adoptRoot(&counted, request, &context);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        context.reject = false;
        const owner = try readOwner(&write, head, 0);
        try std.testing.expectEqual(@as(u128, 97), owner.physical_root_incarnation);
        try std.testing.expectEqual(@as(u64, 1), owner.root_epoch);
        try std.testing.expectEqualDeep(previous.headers, owner.headers);
        try std.testing.expectEqualDeep(previous.proofs, owner.proofs);
        head = try acknowledge(&write, lookup, 0, head.revision, try TestProofs.signed(head, owner, released[0]), &context);
        try std.testing.expectEqual(@as(u16, 1), head.released);
        try deps.requireSource(a, &write, head, source, false);
        try verifyGroup(a, &write, lookup.group, &context);
        try write.commit();
    }
    // Discover the complete final write set without committing it, then inject
    // failure before and after every write, including lease/lock retirement.
    const writes = counted: {
        var write = try store.beginWriteTxn();
        defer write.abort();
        var counted: FaultTxn = .{ .base = &write };
        const head = try readHead(&counted, lookup);
        const owner = try readOwner(&counted, head, 1);
        _ = try acknowledge(&counted, lookup, 1, head.revision, try TestProofs.signed(head, owner, released[1]), &context);
        break :counted counted.writes;
    };
    try std.testing.expect(writes > 0);
    for ([_]bool{ false, true }) |after_write| for (1..writes + 1) |fail_write| {
        var write = try store.beginWriteTxn();
        var failing: FaultTxn = .{ .base = &write, .fail_write = fail_write, .after = after_write };
        const head = try readHead(&failing, lookup);
        const owner = try readOwner(&failing, head, 1);
        try std.testing.expectError(error.InjectedStorageFailure, acknowledge(&failing, lookup, 1, head.revision, try TestProofs.signed(head, owner, released[1]), &context));
        write.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        const retained = try readHead(&read, lookup);
        try std.testing.expectEqual(@as(u16, 1), retained.released);
        try deps.requireSource(a, &read, retained, source, false);
    };
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        const head = try readHead(&write, lookup);
        const owner = try readOwner(&write, head, 1);
        _ = try acknowledge(&write, lookup, 1, head.revision, try TestProofs.signed(head, owner, released[1]), &context);
        try write.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    const completed_head = try readHead(&read, lookup);
    try std.testing.expectEqual(barriers.Phase.succeeded, completed_head.phase);
    try deps.requireSource(a, &read, completed_head, source, true);
    try deps.requireUnleased(&read, lookup.group, 7);
    try verifyGroup(a, &read, lookup.group, &context);
}

test "schema validation publication type cohort atomically retains all locks aliases schemas and type through faults cold recovery and release" {
    try typeCohortFaults(false);
}

test "schema validation publication domain NOT NULL cohort atomically retains all locks aliases schemas and type through faults cold recovery and release" {
    try typeCohortFaults(true);
}

fn typeCohortFaults(is_domain: bool) !void {
    const a = std.testing.allocator;
    const work = @import("schema_validation_work.zig");
    var fixture = try plan_mod.test_support.TypeCohort.initKind(a, is_domain);
    defer fixture.deinit();
    const plan = fixture.value;
    var prepared = try plan_mod.Prepared.init(a, plan);
    defer prepared.deinit();
    {
        var decoded = try plan_mod.parse(a, prepared.encoded);
        defer decoded.deinit();
        const signed = try a.alloc(evidence.SignedPreparation, prepared.owners.len);
        defer a.free(signed);
        for (prepared.owners, signed) |owner, *proof| proof.* = try TestProofs.preparation(prepared.head, owner);
        const frame = try encodeBegin(a, .{ .plan = decoded.value, .preparations = signed, .inputs = fixture.inputs });
        defer a.free(frame);
        try std.testing.expectEqual(if (is_domain) @import("topology_protocol.zig").sql_domain_epoch_version else @import("topology_protocol.zig").sql_type_epoch_version, requiredDecoderVersion(frame));
    }
    const lookup = Lookup.fromHead(prepared.head);
    const preps = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    const before_type = try plan.type_change.?.before.encode(a);
    defer a.free(before_type);
    const after_type = try plan.type_change.?.after.encode(a);
    defer a.free(after_type);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/type-cohort", .{tmp.sub_path}, 0);
    defer a.free(path);
    const Store = local.storage_docstore.DocStore;
    var store = try Store.open(a, path, .{});
    var live = true;
    defer if (live) store.close();
    var context: TestContext = .{};
    var buffer: [192]u8 = undefined;
    try store.put(TestContext.table_key, plan.before.schema_json);
    try store.put(try TestContext.participantKey(&buffer, 8), plan.additional_tables[0].before.schema_json);
    try store.put(TestContext.type_key, before_type);
    const admission_writes = block: {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var counted: FaultTxn = .{ .base = &txn };
        try begin(a, &counted, &prepared, &preps, fixture.inputs, &context);
        break :block counted.writes;
    };
    for (1..admission_writes + 1) |cut| for ([_]bool{ false, true }) |after_write| {
        var txn = try store.beginWriteTxn();
        var failing: FaultTxn = .{ .base = &txn, .fail_write = cut, .after = after_write };
        try std.testing.expectError(error.InjectedStorageFailure, begin(a, &failing, &prepared, &preps, fixture.inputs, &context));
        txn.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        for ([_]u64{ 7, 8 }) |table| try std.testing.expect((try discoverActive(&read, .{ .group = lookup.group, .table = table, .metadata_incarnation = lookup.metadata_incarnation })) == null);
        try std.testing.expectError(error.GenerationPublicationNotFound, readHead(&read, lookup));
    };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try begin(a, &txn, &prepared, &preps, fixture.inputs, &context);
        try txn.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        for ([_]u64{ 7, 8 }) |table| try std.testing.expectEqualDeep(lookup, (try discoverActive(&read, .{ .group = lookup.group, .table = table, .metadata_incarnation = lookup.metadata_incarnation })).?);
        try std.testing.expectEqualDeep(lookup, (try activeType(&read, lookup.group, fixture.value.type_change.?.before.identity.id)).?);
        for (prepared.owners) |owner| {
            try std.testing.expectEqualDeep(prepared.head, try activeHeadForBinding(&read, lookup.group, lookup.metadata_incarnation, owner.binding));
            var forged = owner.binding;
            forged.after_catalog[0] ^= 1;
            try std.testing.expectError(error.SchemaValidationJobChanged, activeHeadForBinding(&read, lookup.group, lookup.metadata_incarnation, forged));
            forged = owner.binding;
            forged.plan_digest[0] ^= 1;
            try std.testing.expectError(error.SchemaValidationJobChanged, activeHeadForBinding(&read, lookup.group, lookup.metadata_incarnation, forged));
        }
        var counted: FaultTxn = .{ .base = &read };
        const page = try work.read(&counted, .{ .group = lookup.group, .metadata_incarnation = lookup.metadata_incarnation });
        try std.testing.expectEqual(@as(u8, 2), page.len);
        try std.testing.expectEqual(@as(u64, 7), page.tasks[0].?.physicalTable());
        try std.testing.expectEqual(@as(u64, 8), page.tasks[1].?.physicalTable());
        try std.testing.expectEqual(@as(usize, 0), counted.plan_reads);
        _ = try readOwnerControl(&read, lookup, 1, .begin, &context);
        try verifyGroup(a, &read, lookup.group, &context);
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var wrong = lookup;
        wrong.plan_digest[0] ^= 1;
        const alias = try ParticipantAlias.init(wrong);
        try txn.put(try (Key{ .group = lookup.group, .table = 8, .id = @splat(0), .kind = .active }).encode(&buffer), alias.slice());
        const reads_before = context.permission_reads;
        try std.testing.expectError(error.InvalidCatalogRecord, readOwnerControl(&txn, lookup, 1, .begin, &context));
        try std.testing.expectEqual(reads_before, context.permission_reads);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var head = try readHead(&txn, lookup);
        for (0..2) |ordinal| {
            const owner = try readOwner(&txn, head, @intCast(ordinal));
            head = try acknowledge(&txn, lookup, @intCast(ordinal), head.revision, try TestProofs.signed(head, owner, TestProofs.ready(owner)), &context);
        }
        _ = try decideInstall(&txn, lookup, head.revision);
        try txn.commit();
    }
    store.close();
    live = false;
    store = try Store.open(a, path, .{});
    live = true;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var head = try readHead(&txn, lookup);
        for (0..2) |ordinal| {
            const owner = try readOwner(&txn, head, @intCast(ordinal));
            head = try acknowledge(&txn, lookup, @intCast(ordinal), head.revision, try TestProofs.signed(head, owner, try TestProofs.installed(head, owner)), &context);
        }
        try txn.commit();
    }
    const publication_writes = block: {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const head = try readHead(&txn, lookup);
        var counted: FaultTxn = .{ .base = &txn };
        _ = try publish(a, &counted, lookup, head.revision, &context);
        break :block counted.writes;
    };
    for (1..publication_writes + 1) |cut| for ([_]bool{ false, true }) |after_write| {
        var txn = try store.beginWriteTxn();
        const head = try readHead(&txn, lookup);
        var failing: FaultTxn = .{ .base = &txn, .fail_write = cut, .after = after_write };
        try std.testing.expectError(error.InjectedStorageFailure, publish(a, &failing, lookup, head.revision, &context));
        txn.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        var recovered = try recover(a, &read, lookup);
        defer recovered.deinit();
        try context.restore(&read, &recovered);
        try std.testing.expectEqualStrings(before_type, try read.get(TestContext.type_key));
        try std.testing.expectEqualStrings(plan.before.schema_json, try read.get(TestContext.table_key));
        try std.testing.expectEqualStrings(plan.additional_tables[0].before.schema_json, try read.get(try TestContext.participantKey(&buffer, 8)));
    };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const head = try readHead(&txn, lookup);
        _ = try publish(a, &txn, lookup, head.revision, &context);
        try txn.commit();
    }
    store.close();
    live = false;
    store = try Store.open(a, path, .{});
    live = true;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var head = try readHead(&txn, lookup);
        try std.testing.expectEqualStrings(after_type, try txn.get(TestContext.type_key));
        for (0..2) |ordinal| {
            const owner = try readOwner(&txn, head, @intCast(ordinal));
            head = try acknowledge(&txn, lookup, @intCast(ordinal), head.revision, try TestProofs.signed(head, owner, try TestProofs.released(head, owner)), &context);
        }
        try std.testing.expectEqual(.succeeded, head.phase);
        try txn.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try activeType(&read, lookup.group, prepared.head.type_id)) == null);
        for ([_]u64{ 7, 8 }) |table| try std.testing.expect((try discoverActive(&read, .{ .group = lookup.group, .table = table, .metadata_incarnation = lookup.metadata_incarnation })) == null);
        var history = try recover(a, &read, lookup);
        defer history.deinit();
        try std.testing.expectEqual(.succeeded, history.head.phase);
        try verifyGroup(a, &read, lookup.group, &context);
    }
}

test "schema validation publication sequence retirement journals complete cohorts and container locks through every admission publication terminal write and cold recovery" {
    const a = std.testing.allocator;
    const sequences = @import("../system_catalog/sequence_catalog.zig");
    var fixture = try plan_mod.test_support.SequenceCohort.init(a);
    defer fixture.deinit();
    const plan = fixture.value;
    const request = plan.sequence_change.?.request;
    const digest = try sequences.commandDigest(a, request);
    var prepared = try plan_mod.Prepared.init(a, plan);
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const proofs = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var decoded = try plan_mod.parse(a, prepared.encoded);
    defer decoded.deinit();
    const frame = try encodeBegin(a, .{ .plan = decoded.value, .preparations = &proofs, .inputs = fixture.inputs });
    defer a.free(frame);
    try std.testing.expectEqual(@import("topology_protocol.zig").sql_sequence_retirement_version, requiredDecoderVersion(frame));
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/sequence-cohort", .{tmp.sub_path}, 0);
    defer a.free(path);
    const Store = local.storage_docstore.DocStore;
    var store = try Store.open(a, path, .{});
    var live = true;
    defer if (live) store.close();
    var context: TestContext = .{};
    var buffer: [192]u8 = undefined;
    const descriptor = try plan.sequence_change.?.before.encode(a);
    defer a.free(descriptor);
    const meta = try std.json.Stringify.valueAlloc(a, local.system_catalog_domain.Meta{ .revision = 1, .next_id = 100 }, .{});
    defer a.free(meta);
    try store.put(TestContext.sequence_meta_key, meta);
    try store.put(TestContext.table_key, plan.before.schema_json);
    try store.put(try TestContext.participantKey(&buffer, 8), plan.additional_tables[0].before.schema_json);
    try store.put(TestContext.sequence_key, descriptor);
    const admission_writes = block: {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var counted: FaultTxn = .{ .base = &txn };
        try begin(a, &counted, &prepared, &proofs, fixture.inputs, &context);
        break :block counted.writes;
    };
    for (1..admission_writes + 1) |cut| for ([_]bool{ false, true }) |after_write| {
        var txn = try store.beginWriteTxn();
        var failing: FaultTxn = .{ .base = &txn, .fail_write = cut, .after = after_write };
        try std.testing.expectError(error.InjectedStorageFailure, begin(a, &failing, &prepared, &proofs, fixture.inputs, &context));
        txn.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.GenerationPublicationNotFound, readHead(&read, lookup));
        try std.testing.expect(try activeSequence(&read, 1, 17) == null);
        try requireSequenceContainerFree(&read, 1, .namespace, 2);
        try requireSequenceContainerFree(&read, 1, .database, 1);
        const journal: sequences.outcomes.Store(@TypeOf(read)) = .{ .txn = &read, .group = 1, .incarnation = request.incarnation };
        try std.testing.expect(try journal.lookup(a, request.admission, digest) == null);
    };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try begin(a, &txn, &prepared, &proofs, fixture.inputs, &context);
        try txn.commit();
    }
    for (0..@import("../system_catalog/sequence_outcomes.zig").retained + 1) |ordinal| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const journal: sequences.outcomes.Store(@TypeOf(txn)) = .{ .txn = &txn, .group = 1, .incarnation = request.incarnation };
        const admission: sequences.Admission = .{ .head = try journal.head(a), .nonce = @splat(@as(u8, @intCast(ordinal + 40))) };
        const next = switch (try journal.prepare(a, admission, @splat(23))) {
            .append => |value| value,
            .replay => return error.UnexpectedOutcomeReplay,
        };
        try journal.append(a, next, admission, @splat(23), .{ .success = .{ .revision = 1, .identity = null, .removed = 0 } });
        try txn.commit();
    }
    store.close();
    live = false;
    store = try Store.open(a, path, .{});
    live = true;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(lookup, (try activeSequence(&read, 1, 17)).?);
        try std.testing.expectError(error.CatalogPublicationProofPending, requireSequenceContainerFree(&read, 1, .namespace, 2));
        try std.testing.expectError(error.CatalogPublicationProofPending, requireSequenceContainerFree(&read, 1, .database, 1));
        try requireSequenceContainerFree(&read, 1, .namespace, 3);
        const journal: sequences.outcomes.Store(@TypeOf(read)) = .{ .txn = &read, .group = 1, .incarnation = request.incarnation };
        try std.testing.expect(try journal.lookup(a, request.admission, digest) == null);
        try std.testing.expect((try retainedSequenceOutcome(&read, 1, request, digest, 3)).? == .pending);
        try verifyGroup(a, &read, 1, &context);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var head = try readHead(&txn, lookup);
        for (0..2) |ordinal| {
            const owner = try readOwner(&txn, head, @intCast(ordinal));
            head = try acknowledge(&txn, lookup, @intCast(ordinal), head.revision, try TestProofs.signed(head, owner, TestProofs.ready(owner)), &context);
        }
        head = try decideInstall(&txn, lookup, head.revision);
        for (0..2) |ordinal| {
            const owner = try readOwner(&txn, head, @intCast(ordinal));
            head = try acknowledge(&txn, lookup, @intCast(ordinal), head.revision, try TestProofs.signed(head, owner, try TestProofs.installed(head, owner)), &context);
        }
        try txn.commit();
    }
    const publication_writes = block: {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const head = try readHead(&txn, lookup);
        var counted: FaultTxn = .{ .base = &txn };
        _ = try publish(a, &counted, lookup, head.revision, &context);
        break :block counted.writes;
    };
    for (1..publication_writes + 1) |cut| for ([_]bool{ false, true }) |after_write| {
        var txn = try store.beginWriteTxn();
        const head = try readHead(&txn, lookup);
        var failing: FaultTxn = .{ .base = &txn, .fail_write = cut, .after = after_write };
        try std.testing.expectError(error.InjectedStorageFailure, publish(a, &failing, lookup, head.revision, &context));
        txn.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        try TestContext.requireCohort(&read, plan, false);
        try std.testing.expectEqualStrings(plan.before.schema_json, try read.get(TestContext.table_key));
        try verifyGroup(a, &read, 1, &context);
    };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const head = try readHead(&txn, lookup);
        _ = try publish(a, &txn, lookup, head.revision, &context);
        try txn.commit();
    }
    store.close();
    live = false;
    store = try Store.open(a, path, .{});
    live = true;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const head = try readHead(&txn, lookup);
        try std.testing.expectEqual(.releasing, head.phase);
        try TestContext.requireCohort(&txn, plan, true);
        const journal: sequences.outcomes.Store(@TypeOf(txn)) = .{ .txn = &txn, .group = 1, .incarnation = request.incarnation };
        try std.testing.expect(try journal.lookup(a, request.admission, digest) == null);
        try std.testing.expect((try retainedSequenceOutcome(&txn, 1, request, digest, 3)).? == .pending);
        const owner = try readOwner(&txn, head, 0);
        _ = try acknowledge(&txn, lookup, 0, head.revision, try TestProofs.signed(head, owner, try TestProofs.released(head, owner)), &context);
        try txn.commit();
    }
    const terminal_writes = block: {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const head = try readHead(&txn, lookup);
        const owner = try readOwner(&txn, head, 1);
        var counted: FaultTxn = .{ .base = &txn };
        _ = try acknowledge(&counted, lookup, 1, head.revision, try TestProofs.signed(head, owner, try TestProofs.released(head, owner)), &context);
        break :block counted.writes;
    };
    for (1..terminal_writes + 1) |cut| for ([_]bool{ false, true }) |after_write| {
        var txn = try store.beginWriteTxn();
        const head = try readHead(&txn, lookup);
        const owner = try readOwner(&txn, head, 1);
        var failing: FaultTxn = .{ .base = &txn, .fail_write = cut, .after = after_write };
        try std.testing.expectError(error.InjectedStorageFailure, acknowledge(&failing, lookup, 1, head.revision, try TestProofs.signed(head, owner, try TestProofs.released(head, owner)), &context));
        txn.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(.releasing, (try readHead(&read, lookup)).phase);
        try std.testing.expect(try activeSequence(&read, 1, 17) != null);
        try std.testing.expectError(error.CatalogPublicationProofPending, requireSequenceContainerFree(&read, 1, .namespace, 2));
        const journal: sequences.outcomes.Store(@TypeOf(read)) = .{ .txn = &read, .group = 1, .incarnation = request.incarnation };
        try std.testing.expect(try journal.lookup(a, request.admission, digest) == null);
        try std.testing.expect((try retainedSequenceOutcome(&read, 1, request, digest, 3)).? == .pending);
        try verifyGroup(a, &read, 1, &context);
    };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const head = try readHead(&txn, lookup);
        const owner = try readOwner(&txn, head, 1);
        _ = try acknowledge(&txn, lookup, 1, head.revision, try TestProofs.signed(head, owner, try TestProofs.released(head, owner)), &context);
        try txn.commit();
    }
    store.close();
    live = false;
    store = try Store.open(a, path, .{});
    live = true;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(.succeeded, (try readHead(&read, lookup)).phase);
        try std.testing.expect(try activeSequence(&read, 1, 17) == null);
        try requireSequenceContainerFree(&read, 1, .namespace, 2);
        try requireSequenceContainerFree(&read, 1, .database, 1);
        const journal: sequences.outcomes.Store(@TypeOf(read)) = .{ .txn = &read, .group = 1, .incarnation = request.incarnation };
        try std.testing.expect(try journal.lookup(a, request.admission, digest) == null);
        const outcome = (try retainedSequenceOutcome(&read, 1, request, digest, 3)).?;
        const result = try outcome.result();
        try std.testing.expectEqual(@as(u64, 1), result.removed);
        try std.testing.expect(result.identity == null);
        try std.testing.expectEqual(@as(u64, 3), result.revision);
        try journal.validateCut(a);
        try std.testing.expectError(error.SequenceAdmissionExpired, journal.prepare(a, request.admission, digest));
        var conflicting = request;
        conflicting.target.table = "forged";
        try std.testing.expectError(error.SequenceAdmissionConflict, retainedSequenceOutcome(&read, 1, conflicting, try sequences.commandDigest(a, conflicting), 3));
        try verifyGroup(a, &read, 1, &context);
    }
}

test "schema validation publication sequence cancellation retains exact outcome and all preimages across final ACK faults and cold recovery" {
    const a = std.testing.allocator;
    const sequences = @import("../system_catalog/sequence_catalog.zig");
    var fixture = try plan_mod.test_support.SequenceCohort.init(a);
    defer fixture.deinit();
    const plan = fixture.value;
    const request = plan.sequence_change.?.request;
    const digest = try sequences.commandDigest(a, request);
    var prepared = try plan_mod.Prepared.init(a, plan);
    defer prepared.deinit();
    const lookup = Lookup.fromHead(prepared.head);
    const proofs = [_]evidence.SignedPreparation{ try TestProofs.preparation(prepared.head, prepared.owners[0]), try TestProofs.preparation(prepared.head, prepared.owners[1]) };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/sequence-cancel", .{tmp.sub_path}, 0);
    defer a.free(path);
    const Store = local.storage_docstore.DocStore;
    var store = try Store.open(a, path, .{});
    var live = true;
    defer if (live) store.close();
    var context: TestContext = .{};
    var buffer: [192]u8 = undefined;
    const descriptor = try plan.sequence_change.?.before.encode(a);
    defer a.free(descriptor);
    const meta = try std.json.Stringify.valueAlloc(a, local.system_catalog_domain.Meta{ .revision = 1, .next_id = 100 }, .{});
    defer a.free(meta);
    try store.put(TestContext.sequence_meta_key, meta);
    try store.put(TestContext.table_key, plan.before.schema_json);
    try store.put(try TestContext.participantKey(&buffer, 8), plan.additional_tables[0].before.schema_json);
    try store.put(TestContext.sequence_key, descriptor);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try begin(a, &txn, &prepared, &proofs, fixture.inputs, &context);
        var head = try readHead(&txn, lookup);
        head = try cancel(&txn, lookup, head.revision, .requested);
        const owner = try readOwner(&txn, head, 0);
        var canceled = TestProofs.ready(owner);
        canceled.phase = .canceled;
        _ = try acknowledge(&txn, lookup, 0, head.revision, try TestProofs.signed(head, owner, canceled), &context);
        try txn.commit();
    }
    store.close();
    live = false;
    store = try Store.open(a, path, .{});
    live = true;
    const terminal_writes = block: {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const head = try readHead(&txn, lookup);
        const owner = try readOwner(&txn, head, 1);
        var canceled = TestProofs.ready(owner);
        canceled.phase = .canceled;
        var counted: FaultTxn = .{ .base = &txn };
        _ = try acknowledge(&counted, lookup, 1, head.revision, try TestProofs.signed(head, owner, canceled), &context);
        break :block counted.writes;
    };
    for (1..terminal_writes + 1) |cut| for ([_]bool{ false, true }) |after_write| {
        var txn = try store.beginWriteTxn();
        const head = try readHead(&txn, lookup);
        const owner = try readOwner(&txn, head, 1);
        var canceled = TestProofs.ready(owner);
        canceled.phase = .canceled;
        var failing: FaultTxn = .{ .base = &txn, .fail_write = cut, .after = after_write };
        try std.testing.expectError(error.InjectedStorageFailure, acknowledge(&failing, lookup, 1, head.revision, try TestProofs.signed(head, owner, canceled), &context));
        txn.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(.canceling, (try readHead(&read, lookup)).phase);
        try std.testing.expect(try activeSequence(&read, 1, 17) != null);
        try std.testing.expectError(error.CatalogPublicationProofPending, requireSequenceContainerFree(&read, 1, .namespace, 2));
        try std.testing.expect((try retainedSequenceOutcome(&read, 1, request, digest, 1)).? == .pending);
        try TestContext.requireCohort(&read, plan, false);
        try verifyGroup(a, &read, 1, &context);
    };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const head = try readHead(&txn, lookup);
        const owner = try readOwner(&txn, head, 1);
        var canceled = TestProofs.ready(owner);
        canceled.phase = .canceled;
        _ = try acknowledge(&txn, lookup, 1, head.revision, try TestProofs.signed(head, owner, canceled), &context);
        try txn.commit();
    }
    store.close();
    live = false;
    store = try Store.open(a, path, .{});
    live = true;
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(.canceled, (try readHead(&read, lookup)).phase);
    try std.testing.expect(try activeSequence(&read, 1, 17) == null);
    try requireSequenceContainerFree(&read, 1, .namespace, 2);
    try requireSequenceContainerFree(&read, 1, .database, 1);
    try TestContext.requireCohort(&read, plan, false);
    try std.testing.expectEqual(.Canceled, (try retainedSequenceOutcome(&read, 1, request, digest, 1)).?.rejected);
    try verifyGroup(a, &read, 1, &context);
}
