// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Metadata source retirement is separate from native image loss. A surviving
//! native image still traverses the ordinary physical retirement state machine.
const std = @import("std");
const scopes = @import("scope_retirement_ledger.zig");
const sources = @import("../system_catalog/temporary_source_binding.zig");
const records = @import("schema_validation_publication.zig");
const A = std.mem.Allocator;
const local = @import("antfly_local_sources");
const locks = local.storage_pessimistic_lock_authority;
const native = local.storage_pessimistic_lock_control;
const table = @import("table_retirement_ledger.zig");
const prepared = @import("../system_catalog/prepared_transactions.zig");
const Participant = struct { lookup: scopes.Lookup, value: locks.Data.Participant };
const ParticipantRecord = records.Record(Participant, "STAP", 8192);
const Completion = struct { lookup: scopes.Lookup, child: table.Lookup, generation: u64, digest: [32]u8 };
const CompletionRecord = records.Record(Completion, "STAC", 2048);
pub const Selection = struct {
    child: table.Lookup,
    generation: ?u64 = null,
    decision: ?locks.DecisionRef = null,
    pub fn validate(self: Selection) !void {
        try self.child.validate();
        if (self.generation) |generation| if (generation == 0) return error.InvalidScopeRetirement;
        if (self.decision) |decision| {
            try decision.validate();
            if (self.generation == null) return error.InvalidScopeRetirement;
        }
    }
};
pub const Work = struct {
    control: native.RetirementControl,
    prepared_control: ?native.NativeControl = null,
    prepared_admission: ?@FieldType(native.Command, "admission") = null,
    completed: bool = false,
};
pub const Checkpoint = struct {
    lookup: scopes.Lookup,
    captured: sources.Abandonment,
    participants: u8,
    participants_digest: [32]u8,
    pub fn digest(self: Checkpoint) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/abandoned-temporary-archive/v1\x00");
        hash.update(&try self.captured.digest());
        hash.update(&self.participants_digest);
        hash.update(&.{self.participants});
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn validate(self: Checkpoint) !void {
        try self.lookup.validate();
        if (!try self.captured.eligible() or self.lookup.group != self.captured.binding.group or
            self.lookup.reference.kind != .namespace or self.lookup.reference.root != self.captured.binding.namespace.namespace_id or
            self.lookup.reference.database != self.captured.binding.namespace.database_id or
            self.participants != (if (self.captured.source) |source| if (source.data) |directory| directory.count() else 0 else 0) or
            std.mem.allEqual(u8, &self.participants_digest, 0)) return error.InvalidScopeRetirement;
    }
};
// One bounded source record, namespace binding and immutable scope lookup.
pub const Record = records.Record(Checkpoint, "STAB", 16 * 1024);

pub fn stage(a: A, txn: anytype, before: scopes.Head, captured: sources.Abandonment) !scopes.Head {
    try before.validate();
    if (before.creator.kind != .bootstrap or before.temporary_abandonment != null or before.progress.phase != .draining_connections or
        !std.meta.eql((try scopes.head(txn, before.lookup)) orelse return error.ScopeRetirementChanged, before)) return error.ScopeRetirementChanged;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/abandoned-temporary-participants/v1\x00");
    var count: u8 = 0;
    if (captured.source) |source| if (source.data) |directory| {
        const ns: locks.ledger.Namespace = .{ .home = .metadata, .group = before.lookup.group, .incarnation = captured.binding.backend.incarnation };
        for (directory.slots) |generation| {
            if (generation == 0) continue;
            const item = (try locks.Data.readParticipant(a, txn, ns, captured.binding.backend, generation)) orelse return error.InvalidScopeRetirement;
            try item.validate(ns, captured.binding.backend);
            const record = try ParticipantRecord.init(.{ .lookup = before.lookup, .value = item });
            var key: [512]u8 = undefined;
            try txn.put(try participantKey(&key, before.lookup, generation), record.slice());
            hash.update(record.slice());
            count += 1;
        }
    };
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    const checkpoint: Checkpoint = .{ .lookup = before.lookup, .captured = captured, .participants = count, .participants_digest = digest };
    try checkpoint.validate();
    var after = before;
    after.temporary_abandonment = try checkpoint.digest();
    try after.validate();
    const encoded = try Record.init(checkpoint);
    const head = try scopes.HeadRecord.init(after);
    var key: [320]u8 = undefined;
    try txn.put(try scopes.key(&key, before.lookup, .temporary_abandonment), encoded.slice());
    try txn.put(try scopes.key(&key, before.lookup, .head), head.slice());
    return after;
}

/// Archive validation never requires the ended source to still exist. The
/// admission transaction retained its exact registration/command/state cut.
pub fn verifyHead(a: A, reader: anytype, head: scopes.Head) !void {
    const expected = head.temporary_abandonment orelse return;
    var key: [320]u8 = undefined;
    const checkpoint = try Record.decode(try reader.get(try scopes.key(&key, head.lookup, .temporary_abandonment)));
    try checkpoint.validate();
    if (!std.meta.eql(checkpoint.lookup, head.lookup) or head.creator.kind != .bootstrap or
        head.temporary_owner == null or !head.temporary_owner.?.eql(checkpoint.captured.binding.namespace.owner) or
        !std.mem.eql(u8, &head.metadata_incarnation, &checkpoint.captured.binding.backend.incarnation) or
        !std.mem.eql(u8, &expected, &try checkpoint.digest())) return error.InvalidScopeRetirement;
    try verifyParticipants(a, reader, checkpoint);
}

fn participantKey(buffer: []u8, lookup: scopes.Lookup, generation: u64) ![]const u8 {
    var key: [320]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}:participant:{x:0>16}", .{ try scopes.key(&key, lookup, .temporary_abandonment), generation });
}
fn completionKey(buffer: []u8, lookup: scopes.Lookup, child: table.Lookup, generation: u64) ![]const u8 {
    var key: [320]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}:closed:{x:0>16}:{x:0>16}", .{ try scopes.key(&key, lookup, .temporary_abandonment), child.table, generation });
}
pub fn readCheckpoint(a: A, reader: anytype, head: scopes.Head) !Checkpoint {
    try verifyHead(a, reader, head);
    var key: [320]u8 = undefined;
    return Record.decode(try reader.get(try scopes.key(&key, head.lookup, .temporary_abandonment)));
}
pub fn readParticipant(a: A, reader: anytype, archive: Checkpoint, generation: u64) !locks.Data.Participant {
    const source = archive.captured.source orelse return error.InvalidScopeRetirement;
    const directory = source.data orelse return error.InvalidScopeRetirement;
    if (std.mem.indexOfScalar(u64, &directory.slots, generation) == null) return error.InvalidScopeRetirement;
    var key: [512]u8 = undefined;
    const result = try decodeParticipant(a, try reader.get(try participantKey(&key, archive.lookup, generation)));
    if (!std.meta.eql(result.lookup, archive.lookup) or result.value.admission.generation != generation) return error.InvalidScopeRetirement;
    try result.value.validate(.{ .home = .metadata, .group = archive.lookup.group, .incarnation = archive.captured.binding.backend.incarnation }, archive.captured.binding.backend);
    return result.value;
}
pub fn verifyParticipants(a: A, reader: anytype, archive: Checkpoint) !void {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/abandoned-temporary-participants/v1\x00");
    var count: u8 = 0;
    if (archive.captured.source) |source| if (source.data) |directory| for (directory.slots) |generation| {
        if (generation == 0) continue;
        const record = try ParticipantRecord.init(.{ .lookup = archive.lookup, .value = try readParticipant(a, reader, archive, generation) });
        hash.update(record.slice());
        count += 1;
    };
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    if (count != archive.participants or !std.mem.eql(u8, &digest, &archive.participants_digest)) return error.InvalidScopeRetirement;
}

/// All authorities are rejoined from retained parent/child records and the
/// actual immutable physical plan in the caller's one metadata read cut.
pub fn readWork(a: A, reader: anytype, selection: Selection) !?Work {
    try selection.validate();
    const child = (try table.head(reader, selection.child)) orelse return error.ScopeRetirementChanged;
    const parent_ref = child.scope.parent orelse return null;
    const parent = (try scopes.head(reader, parent_ref.lookup)) orelse return error.InvalidScopeRetirement;
    if (parent.temporary_abandonment == null) return null;
    if (parent.progress.phase == .draining_connections or parent.progress.phase == .canceled) return error.ScopeRetirementChanged;
    const registered = (try scopes.child(reader, parent_ref.lookup, parent_ref.ordinal)) orelse return error.InvalidScopeRetirement;
    if (!std.meta.eql(parent_ref, registered) or parent_ref.table != child.lookup.table or !std.mem.eql(u8, &parent_ref.id, &child.lookup.id)) return error.InvalidScopeRetirement;
    const archive = try readCheckpoint(a, reader, parent);
    if (child.phase == .reserved) return null;
    if (child.phase == .canceled or child.native_loss != null or child.physical == null) return error.ScopeRetirementChanged;
    var key: [512]u8 = undefined;
    const plan_bytes = try reader.get(try table.physical.key(&key, child.lookup, .plan, 0, .source_fenced));
    if (plan_bytes.len > @import("fk_generation_publication.zig").max_bytes) return error.InvalidScopeRetirement;
    const plan = try std.json.parseFromSliceLeaky(@import("fk_generation_publication.zig").Plan, a, plan_bytes, .{ .allocate = .alloc_always });
    try plan.validate(a);
    if (plan.table_retirement == null or !std.meta.eql(plan.table_retirement.?, child.effects) or !std.mem.eql(u8, &try plan.digest(a), &child.physical.?.summary.plan_digest)) return error.InvalidScopeRetirement;
    const source = archive.captured.source orelse return null;
    const directory = source.data orelse return null;
    for (directory.slots) |generation| {
        if (generation == 0 or (selection.generation != null and generation != selection.generation.?)) continue;
        const item = try readParticipant(a, reader, archive, generation);
        if (item.spec.route.table_id != child.lookup.table) continue;
        if (item.spec.route.route.execution != .native_local or item.spec.route.route.execution.native_local.kind != .temporary or
            !item.spec.route.route.execution.native_local.owner.?.eql(archive.captured.binding.namespace.owner)) return error.InvalidScopeRetirement;
        const descriptor = plan.child_before.sql_persistence orelse return error.InvalidScopeRetirement;
        if (!descriptor.eql(item.spec.route.route.execution.native_local)) return error.InvalidScopeRetirement;
        const fence = for (plan.child_fences) |candidate| {
            if (candidate.owner_group_id == item.spec.route.route.group_id and candidate.namespace.eql(item.admission.binding.identity)) break candidate;
        } else return error.InvalidScopeRetirement;
        const done = reader.get(try completionKey(&key, archive.lookup, child.lookup, generation)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (done) |bytes| {
            const completed = try CompletionRecord.decode(bytes);
            if (!std.meta.eql(completed.lookup, archive.lookup) or !std.meta.eql(completed.child, child.lookup) or completed.generation != generation or std.mem.allEqual(u8, &completed.digest, 0)) return error.InvalidScopeRetirement;
            if (selection.generation == null) continue;
        }
        var result: Work = .{ .control = .{ .route = item.spec.route, .scope_id = child.lookup.id, .scope_revision = child.revision, .abandonment_digest = parent.temporary_abandonment.?, .physical_fence = fence, .participant = item }, .completed = done != null };
        try result.control.validate(item.admission);
        if (selection.decision) |decision| {
            const record = (try prepared.readOperation(a, reader, child.lookup.group, decision.operation)) orelse return error.CatalogPublicationProofPending;
            if (record.decision == null or !std.mem.eql(u8, &record.identity.txn_id, &item.admission.transaction)) return error.CatalogPublicationProofPending;
            const owner = try record.identity.owner(a);
            if (!std.mem.eql(u8, &owner.admission_digest, &decision.admission)) return error.ExternalTransactionDecisionChanged;
            for (record.participants) |part| if (try part.lockReference(record.identity, a)) |reference| {
                if (reference.admission.generation != generation or !reference.admission.binding.eql(item.admission.binding)) continue;
                if (!std.mem.eql(u8, part.table_name, item.spec.table_name) or !std.meta.eql(part.route.route, item.spec.route.route) or
                    !std.meta.eql(part.route.native_participant, item.spec.route.native_participant)) return error.InvalidScopeRetirement;
                var old = reference.admission;
                old.purpose = item.admission.purpose;
                old.parent_command = item.admission.parent_command;
                old.serial = item.admission.serial;
                old.source.lease.expires_at_ms = item.admission.source.lease.expires_at_ms;
                if (!std.meta.eql(old, item.admission)) return error.InvalidScopeRetirement;
                result.prepared_control = .{ .route = item.spec.route, .owner = owner, .decision = decision };
                // The existing immutable prepared reference has the resolution
                // command identity; the original execution admission stays in
                // the archive and is never rewritten to resemble it.
                result.prepared_admission = reference.admission;
                break;
            };
            if (result.prepared_control == null) return error.InvalidScopeRetirement;
        }
        return result;
    }
    if (selection.generation != null) return error.InvalidScopeRetirement;
    return null;
}

pub fn acknowledge(a: A, txn: anytype, selection: Selection, observation: native.Snapshot) !void {
    var lookup = selection;
    lookup.decision = null;
    const work = (try readWork(a, txn, lookup)) orelse return error.InvalidScopeRetirement;
    if (work.completed) return;
    const item = work.control.participant;
    const scope = observation.scope orelse return error.LockOwnerBusy;
    const owner = observation.owner orelse return error.LockOwnerBusy;
    try scope.validate(item.admission.binding.namespace);
    try owner.validate(item.admission.binding.namespace);
    var expected = try item.admission.registration();
    expected.lease.expires_at_ms = scope.registration.lease.expires_at_ms;
    if (!std.meta.eql(expected, scope.registration) or !std.meta.eql(owner.lease, scope.registration.lease)) return error.LockOwnerChanged;
    if (scope.state != .retired or scope.transaction != null or owner.state != .retired or owner.resources != 0 or owner.waiting != null or owner.job != null) return error.LockOwnerBusy;
    if (observation.frozen) |frozen| {
        if (!frozen.retired or !std.mem.eql(u8, &frozen.ref.admission.transaction, &item.admission.transaction)) return error.LockOwnerBusy;
        var checked = selection;
        checked.decision = frozen.ref.decision;
        if (try prepared.readOperation(a, txn, selection.child.group, frozen.ref.decision.operation) != null) {
            _ = try readWork(a, txn, checked);
        } else {
            // Completed metadata decisions can compact their manifest only
            // after real native ACKs. A retained receipt plus the ACTUAL retired
            // native Ref proves this late completion; absence alone does not.
            const receipt = (try prepared.readReceipt(a, txn, selection.child.group, frozen.ref.decision.operation)) orelse return error.CatalogPublicationProofPending;
            const ref = receipt.lock_scope orelse return error.InvalidScopeRetirement;
            try frozen.ref.validate(item.admission.transaction, receipt.owner);
            if (receipt.kind != .session_commit or receipt.decision == null or receipt.progress != .ready or
                (receipt.status != .committed and receipt.status != .aborted) or !ref.backend.eql(item.admission.source.lease.backend) or
                !std.meta.eql(ref.command, frozen.ref.admission.parent_command) or
                !std.mem.eql(u8, &receipt.txn_id, &item.admission.transaction)) return error.InvalidScopeRetirement;
            var original = frozen.ref.admission;
            original.purpose = item.admission.purpose;
            original.parent_command = item.admission.parent_command;
            original.serial = item.admission.serial;
            original.source.lease.expires_at_ms = item.admission.source.lease.expires_at_ms;
            if (!std.meta.eql(original, item.admission)) return error.InvalidScopeRetirement;
        }
    }
    const child = (try table.head(txn, selection.child)).?;
    const parent = (try scopes.head(txn, child.scope.parent.?.lookup)).?;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/abandoned-private-group-completion/v1\x00");
    const encoded = try std.json.Stringify.valueAlloc(a, observation, .{});
    hash.update(encoded);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    var key: [512]u8 = undefined;
    const completed = try CompletionRecord.init(.{ .lookup = parent.lookup, .child = child.lookup, .generation = item.admission.generation, .digest = digest });
    try txn.put(try completionKey(&key, parent.lookup, child.lookup, item.admission.generation), completed.slice());
}

/// Auxiliary archive rows live until their existing parent scope tombstone.
/// Cold validation checks canonical keys and cross-links; missing participant
/// bodies are detected by verifyParticipants from the retained source slots.
pub fn verifyAuxiliary(backing: A, reader: anytype, address: []const u8, bytes: []const u8) !bool {
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    const stable_address = try a.dupe(u8, address);
    var key: [512]u8 = undefined;
    if (std.mem.indexOf(u8, stable_address, ":temporary_abandonment:participant:") != null) {
        const saved = try decodeParticipant(a, bytes);
        const parent = (try scopes.head(reader, saved.lookup)) orelse return error.InvalidScopeRetirement;
        const archive = try readCheckpoint(a, reader, parent);
        if (!std.mem.eql(u8, stable_address, try participantKey(&key, saved.lookup, saved.value.admission.generation))) return error.InvalidScopeRetirement;
        _ = try readParticipant(a, reader, archive, saved.value.admission.generation);
        return true;
    }
    if (std.mem.indexOf(u8, stable_address, ":temporary_abandonment:closed:") != null) {
        const saved = try CompletionRecord.decode(bytes);
        const parent = (try scopes.head(reader, saved.lookup)) orelse return error.InvalidScopeRetirement;
        const archive = try readCheckpoint(a, reader, parent);
        const item = try readParticipant(a, reader, archive, saved.generation);
        const child = (try table.head(reader, saved.child)) orelse return error.InvalidScopeRetirement;
        if (child.scope.parent == null or !std.meta.eql(child.scope.parent.?.lookup, parent.lookup) or item.spec.route.table_id != child.lookup.table or
            std.mem.allEqual(u8, &saved.digest, 0) or !std.mem.eql(u8, stable_address, try completionKey(&key, saved.lookup, saved.child, saved.generation))) return error.InvalidScopeRetirement;
        return true;
    }
    return false;
}

fn decodeParticipant(a: A, bytes: []const u8) !Participant {
    if (bytes.len < 44 or bytes.len > ParticipantRecord.max_wire_bytes or !std.mem.eql(u8, bytes[0..4], "STAP") or
        !std.mem.eql(u8, bytes[4..8], "\x01\x00\x00\x00") or std.mem.readInt(u32, bytes[8..12], .little) != bytes.len - 44) return error.InvalidScopeRetirement;
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidScopeRetirement;
    // table_name is variable-length. Generic fixed-record decoders cannot
    // return an escaped JSON string allocated in their temporary stack arena.
    const result = try std.json.parseFromSliceLeaky(Participant, a, bytes[12 .. bytes.len - 32], .{ .allocate = .alloc_always });
    const canonical = try ParticipantRecord.init(result);
    if (!std.mem.eql(u8, canonical.slice(), bytes)) return error.InvalidScopeRetirement;
    return result;
}

test {
    _ = @import("abandoned_private_retirement_test.zig");
}
