// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Immutable regular UNIQUE-build plan and owned metadata admission postimage.
//! No public operation is enabled here. Admission must independently verify
//! current catalog/name authority, the complete live owner cut, capabilities
//! and conflicting lifecycle jobs before staging these records in ONE txn.
const std = @import("std");
const local = @import("antfly_local_sources");
const records = local.common_topology_records;
const names = local.system_catalog_relation_names;
const domain = local.system_catalog_domain;
const manager = @import("table_manager.zig");
const progress = local.storage_db_relational_index_publication_progress;
const candidate_mod = @import("relational_index_candidate.zig");
const shape = @import("schema_publication_shape.zig");
const Allocator = std.mem.Allocator;
pub const Id = progress.Id;
pub const Digest = progress.Digest;
pub const max_bytes = 8 * 1024 * 1024;

fn digest(bytes: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &out, .{});
    return out;
}

/// Counting writer rejects oversized/escaped plans before the first frame
/// allocation. It also bounds work for callers constructing plans in memory,
/// not just for a network decoder which already checked the incoming frame.
const FrameBudget = struct {
    remaining: usize = max_bytes,
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *FrameBudget = @alignCast(@fieldParentPtr("writer", w));
        var count = std.math.mul(usize, data[data.len - 1].len, splat) catch return error.WriteFailed;
        for (data[0 .. data.len - 1]) |bytes| count = std.math.add(usize, count, bytes.len) catch return error.WriteFailed;
        if (count > self.remaining) return error.WriteFailed;
        self.remaining -= count;
        return count;
    }
};

fn encodeBounded(alloc: Allocator, value: anytype) ![]const u8 {
    var budget: FrameBudget = .{};
    std.json.Stringify.value(value, .{}, &budget.writer) catch return error.IndexPublicationTooLarge;
    const bytes = try alloc.alloc(u8, max_bytes - budget.remaining);
    errdefer alloc.free(bytes);
    var writer: std.Io.Writer = .fixed(bytes);
    std.json.Stringify.value(value, .{}, &writer) catch return error.InvalidIndexPublicationCandidate;
    if (writer.end != bytes.len) return error.InvalidIndexPublicationCandidate;
    return bytes;
}

pub const Owner = struct {
    range: records.RangeRecord,
    namespace: local.storage_db_doc_identity.Namespace,
    admission_epoch: u64,
    catalog_digest: Digest,
};

/// Call only for an owner from a bounded, validated frame. Hash directly into
/// the writer, without allocating another serialized copy per owner.
pub fn ownerFenceDigest(owner: Owner) !Digest {
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("antfly:index-build-owner:v1");
    var hashing = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hasher, &.{});
    try std.json.Stringify.value(owner, .{}, &hashing.writer);
    var out: Digest = undefined;
    hashing.hasher.final(&out);
    return out;
}

pub const Plan = struct {
    format_version: u16 = 1,
    id: Id,
    namespace_id: u64,
    index_name: []const u8,
    table_guard: domain.RelationMutationGuard,
    before: records.TableRecord,
    after: records.TableRecord,
    catalog_before_b64: []const u8,
    /// Keyspace order, not arrival order. Full range descriptors bind restore,
    /// placement and physical identity fields, not only routing boundaries.
    owners: []const Owner,

    pub fn prepare(self: Plan, alloc: Allocator) !Prepared {
        if (self.format_version != 1 or std.mem.allEqual(u8, &self.id, 0) or
            self.namespace_id == 0 or self.index_name.len == 0 or self.index_name.len > names.max_name_bytes or
            self.owners.len == 0 or self.owners.len > progress.max_owners)
            return error.InvalidIndexPublicationCandidate;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const frame = try encodeBounded(a, self);
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const s = scratch.allocator();
        try self.table_guard.validate();
        if (self.table_guard.owner.kind != .table or self.table_guard.constraint_name_admission != null or
            self.table_guard.owner.table_id != self.before.table_id or
            !std.mem.eql(u8, self.table_guard.target.name, self.table_guard.logical_table) or
            self.table_guard.owner.schema_version != try @import("../api/tables.zig").schemaVersion(self.before.schema_json) or
            !std.mem.eql(u8, &self.table_guard.owner.schema_digest, &digest(self.before.schema_json)))
            return error.InvalidIndexPublicationCandidate;
        try shape.requireTableImages(s, self.before, self.after);
        const length = std.base64.standard.Decoder.calcSizeForSlice(self.catalog_before_b64) catch return error.InvalidIndexPublicationCandidate;
        if (length == 0 or length > local.storage_db_relational_integrity_catalog.max_catalog_bytes) return error.InvalidIndexPublicationCandidate;
        const predecessor = try s.alloc(u8, length);
        std.base64.standard.Decoder.decode(predecessor, self.catalog_before_b64) catch return error.InvalidIndexPublicationCandidate;
        // Nested catalogs outlive this stack frame. Their allocator must not
        // point at the local arena object which is moved into Prepared.
        var candidate = try candidate_mod.Candidate.prepare(alloc, self.before.table_id, self.before.schema_json, self.after.schema_json, predecessor, self.index_name);
        errdefer candidate.deinit();
        if (!try @import("../api/tables.zig").foreignKeyPublicationIndexesValid(s, self.before.name, self.before.indexes_json, self.after.indexes_json, self.after.schema_json, candidate.next.catalog.schema_version))
            return error.InvalidIndexPublicationCandidate;
        const ranges = try s.alloc(records.RangeRecord, self.owners.len);
        // Membership checks are linear even for the maximum owner cohort.
        var groups: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer groups.deinit(s);
        const before_digest = digest(predecessor);
        for (self.owners, ranges) |owner, *range| {
            range.* = owner.range;
            if (owner.range.table_id != self.before.table_id or owner.range.group_id == 0 or owner.admission_epoch == 0 or
                owner.namespace.table_id != self.before.table_id or
                owner.namespace.shard_id != manager.rangeDocIdentityShardId(owner.range) or
                owner.namespace.range_id != manager.rangeDocIdentityRangeId(owner.range) or
                !std.mem.eql(u8, &owner.catalog_digest, &before_digest)) return error.InvalidIndexPublicationCandidate;
            if ((try groups.getOrPut(s, owner.range.group_id)).found_existing) return error.InvalidIndexPublicationCandidate;
        }
        manager.validateCompleteKeyspaceRanges(ranges) catch return error.InvalidIndexPublicationCandidate;
        const plan_digest = digest(frame);
        const owners = try a.alloc(progress.OwnerState, self.owners.len);
        for (self.owners, owners, 0..) |owner, *state, ordinal| {
            // The head's plan digest binds namespace, exact before/after images
            // and all owners. This owner digest also binds its full local cut.
            state.* = .{ .owner = .{
                .plan_id = self.id,
                .plan_digest = plan_digest,
                .ordinal = @intCast(ordinal),
                .group_id = owner.range.group_id,
                .range_id = owner.namespace.range_id,
                .fence_digest = try ownerFenceDigest(owner),
            } };
            try state.validate();
        }
        const head: progress.Head = .{
            .plan_id = self.id,
            .plan_digest = plan_digest,
            .candidate_digest = digest(candidate.next.value),
            .reservation_digest = digest(candidate.reservation.value),
            .table_id = self.before.table_id,
            .owner_count = @intCast(self.owners.len),
        };
        try head.validate();
        const reservation: names.Entry.Reservation = .{ .successor = .{
            .table_id = self.before.table_id,
            .schema_version = candidate.next.catalog.schema_version,
            .schema_digest = digest(self.after.schema_json),
            .publication_id = self.id,
            .kind = .index,
            .phase = .reserved,
        } };
        try reservation.validate();
        return .{
            .arena = arena,
            .frame = frame,
            .candidate = candidate,
            .head = head,
            .owners = owners,
            .name_key = .{ .namespace_id = self.namespace_id, .name = try a.dupe(u8, self.index_name) },
            .name_reservation = reservation,
        };
    }
};

/// All returned slices are owned. Progress application does not retain or
/// reserialize the immutable frame; it uses head + one owner record per ACK.
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    frame: []const u8,
    candidate: candidate_mod.Candidate,
    head: progress.Head,
    owners: []const progress.OwnerState,
    name_key: names.Key,
    name_reservation: names.Entry.Reservation,

    pub fn deinit(self: *Prepared) void {
        self.candidate.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

test "relational index publication frame preflight bounds escaping before allocation" {
    const a = std.testing.allocator;
    const huge = try a.alloc(u8, max_bytes / 6 + 1);
    defer a.free(huge);
    @memset(huge, 0);
    try std.testing.expectError(error.IndexPublicationTooLarge, encodeBounded(std.testing.failing_allocator, .{ .text = huge }));
    const small = try encodeBounded(a, .{ .value = "quoted \" value" });
    defer a.free(small);
    try std.testing.expectEqualStrings("{\"value\":\"quoted \\\" value\"}", small);
}

fn testPlan(a: Allocator) !Plan {
    const before_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}}}
    ;
    const after_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}},"relational_indexes":[{"name":"email_idx","keys":[{"column":"email"}]}],"unique_constraints":[{"name":"email_idx","origin":"index","columns":["email"]}]}
    ;
    const before: records.TableRecord = .{ .table_id = 101, .name = "physical:101", .schema_json = before_json };
    const after = try @import("../api/tables.zig").prepareForeignKeyPublicationRecord(a, &before, after_json);
    var schema = try local.schema_mod.parseValidatedTableSchema(a, before_json);
    defer schema.deinit(a);
    var compiled = try @import("fk_generation_publication.zig").compileCatalog(a, schema, 101, null);
    defer compiled.deinit();
    const b64 = try a.alloc(u8, std.base64.standard.Encoder.calcSize(compiled.value.len));
    _ = std.base64.standard.Encoder.encode(b64, compiled.value);
    const owners = try a.alloc(Owner, 1);
    owners[0] = .{
        .range = .{ .table_id = 101, .group_id = 301, .range_id = 301, .doc_identity_shard_id = 301, .doc_identity_range_id = 301, .start_key = "" },
        .namespace = .{ .table_id = 101, .shard_id = 301, .range_id = 301 },
        .admission_epoch = 7,
        .catalog_digest = digest(compiled.value),
    };
    return .{
        .id = @splat(1),
        .namespace_id = domain.default_namespace_id,
        .index_name = "email_idx",
        .table_guard = .{ .target = .{ .name = "items" }, .logical_table = "items", .owner = .{ .table_id = 101, .schema_version = 1, .schema_digest = digest(before_json), .kind = .table }, .incarnation = @splat(1) },
        .before = before,
        .after = after,
        .catalog_before_b64 = b64,
        .owners = owners,
    };
}

test "relational index publication prepares owned immutable head owner and reserved name postimages" {
    const a = std.testing.allocator;
    var prepared = blk: {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const plan = try testPlan(scratch.allocator());
        break :blk try plan.prepare(a);
    };
    defer prepared.deinit();
    try std.testing.expectEqual(progress.Phase.fencing, prepared.head.phase);
    try std.testing.expectEqual(@as(u16, 1), prepared.head.owner_count);
    try std.testing.expectEqualSlices(u8, &digest(prepared.frame), &prepared.head.plan_digest);
    try std.testing.expectEqualStrings("email_idx", prepared.name_key.name);
    const pending = try (names.Entry{}).reserve(prepared.name_reservation);
    try std.testing.expect(pending.active == null);
    try std.testing.expectEqual(names.Phase.reserved, pending.pending.?.phase);
    try std.testing.expectEqual(@as(u32, 1), prepared.candidate.reservation.catalog.schema_version);
    try std.testing.expect(prepared.candidate.reservation.catalog.find(.unique, "email_idx") == null);
    // Decode the immutable frame after preparation; progress is not embedded
    // in the plan, and every typed owner/schema identity survives persistence.
    var decoded = try std.json.parseFromSlice(Plan, a, prepared.frame, .{});
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &try ownerFenceDigest(decoded.value.owners[0]), &prepared.owners[0].owner.fence_digest);
    var replay = try decoded.value.prepare(a);
    defer replay.deinit();
    try std.testing.expectEqualSlices(u8, prepared.frame, replay.frame);
    try std.testing.expectEqualSlices(u8, &prepared.head.plan_digest, &replay.head.plan_digest);
}

test "relational index publication rejects stale guards partial cuts and substituted owner identities" {
    const a = std.testing.allocator;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const plan = try testPlan(scratch.allocator());
    var invalid = plan;
    invalid.after.description = "unrelated mutation";
    try std.testing.expectError(error.InvalidGenerationPublication, invalid.prepare(a));
    invalid = plan;
    invalid.table_guard.owner.schema_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
    var owner = plan.owners[0];
    owner.catalog_digest[0] ^= 1;
    invalid = plan;
    invalid.owners = &.{owner};
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
    owner = plan.owners[0];
    owner.range.end_key = "m";
    invalid.owners = &.{owner};
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
    owner = plan.owners[0];
    owner.namespace.range_id += 1;
    invalid.owners = &.{owner};
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
    invalid = plan;
    invalid.owners = &.{};
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
}

test "relational index publication binds complete disjoint multiowner cuts and rejects duplicate groups" {
    const a = std.testing.allocator;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var plan = try testPlan(scratch.allocator());
    var owners = [_]Owner{ plan.owners[0], plan.owners[0] };
    owners[0].range.end_key = "m";
    owners[1].range.start_key = "m";
    owners[1].range.group_id = 302;
    owners[1].range.range_id = 302;
    owners[1].range.doc_identity_shard_id = 302;
    owners[1].range.doc_identity_range_id = 302;
    owners[1].namespace = .{ .table_id = 101, .shard_id = 302, .range_id = 302 };
    plan.owners = &owners;
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(u16, 2), prepared.head.owner_count);
    try std.testing.expectEqual(@as(u16, 1), prepared.owners[1].owner.ordinal);
    owners[1].range.group_id = owners[0].range.group_id;
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, plan.prepare(a));
    owners[1].range.group_id = 302;
    owners[1].range.start_key = "n";
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, plan.prepare(a));
}
