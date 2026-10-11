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
const lifecycle = @import("table_lifecycle_lock.zig");
const Allocator = std.mem.Allocator;
pub const Id = progress.Id;
pub const Digest = progress.Digest;
pub const max_bytes = 8 * 1024 * 1024;
pub const max_command_bytes = max_bytes + 16 * 1024;
const prefix = "\x00\x00__metadata__:relational_index_publication:v1:";
pub fn allGroupsPrefix() []const u8 {
    return prefix;
}
pub fn groupFromKey(key: []const u8) !?u64 {
    if (!std.mem.startsWith(u8, key, prefix)) return null;
    var parts = std.mem.splitScalar(u8, key[prefix.len..], ':');
    const kind = parts.next() orelse return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, kind, "plan") and !std.mem.eql(u8, kind, "head") and
        !std.mem.eql(u8, kind, "active") and !std.mem.eql(u8, kind, "owner")) return error.InvalidCatalogRecord;
    const number = parts.next() orelse return error.InvalidCatalogRecord;
    const group = std.fmt.parseInt(u64, number, 10) catch return error.InvalidCatalogRecord;
    var buf: [20]u8 = undefined;
    if (group == 0 or !std.mem.eql(u8, number, try std.fmt.bufPrint(&buf, "{d}", .{group}))) return error.InvalidCatalogRecord;
    _ = try tableSuffix(parts.next() orelse return error.InvalidCatalogRecord);
    if (std.mem.eql(u8, kind, "owner")) {
        const ordinal = parts.next() orelse return error.InvalidCatalogRecord;
        if (ordinal.len != 4) return error.InvalidCatalogRecord;
        const value = std.fmt.parseInt(u16, ordinal, 16) catch return error.InvalidCatalogRecord;
        var ordinal_buf: [4]u8 = undefined;
        if (value >= progress.max_owners or !std.mem.eql(u8, ordinal, try std.fmt.bufPrint(&ordinal_buf, "{x:0>4}", .{value}))) return error.InvalidCatalogRecord;
    }
    if (parts.next() != null) return error.InvalidCatalogRecord;
    return group;
}

pub fn planPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}plan:{d}:", .{ prefix, group });
}
pub fn activePrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}active:{d}:", .{ prefix, group });
}
pub fn headPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}head:{d}:", .{ prefix, group });
}
pub fn ownersPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}owner:{d}:", .{ prefix, group });
}
pub fn planKey(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}plan:{d}:{x:0>16}", .{ prefix, group, table });
}
pub fn activeKey(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}active:{d}:{x:0>16}", .{ prefix, group, table });
}
pub fn headKey(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}head:{d}:{x:0>16}", .{ prefix, group, table });
}
pub fn ownerKey(buf: []u8, group: u64, table: u64, ordinal: u16) ![]const u8 {
    if (ordinal >= progress.max_owners) return error.InvalidIndexPublicationCandidate;
    return std.fmt.bufPrint(buf, "{s}owner:{d}:{x:0>16}:{x:0>4}", .{ prefix, group, table, ordinal });
}

pub const SourceKey = union(enum) { plan: u64, active: u64 };
pub fn sourceKey(key: []const u8, group: u64) !?SourceKey {
    var buf: [160]u8 = undefined;
    const plans = try planPrefix(&buf, group);
    if (std.mem.startsWith(u8, key, plans)) return .{ .plan = try tableSuffix(key[plans.len..]) };
    const active = try activePrefix(&buf, group);
    if (std.mem.startsWith(u8, key, active)) return .{ .active = try tableSuffix(key[active.len..]) };
    return null;
}
fn tableSuffix(bytes: []const u8) !u64 {
    if (bytes.len != 16) return error.InvalidCatalogRecord;
    for (bytes) |c| if (!(c >= '0' and c <= '9') and !(c >= 'a' and c <= 'f')) return error.InvalidCatalogRecord;
    const id = std.fmt.parseInt(u64, bytes, 16) catch return error.InvalidCatalogRecord;
    if (id == 0) return error.InvalidCatalogRecord;
    return id;
}

/// A fresh begin CASes the exact terminal predecessor head (or true absence).
/// Random job IDs alone cannot prevent replay after multiple canceled attempts
/// against an unchanged public schema. Old owner rows are replaced/trimmed in
/// the same transaction, keeping retained storage bounded after DDL churn.
pub const Begin = struct {
    expected_head: ?[progress.Head.encoded_len]u8 = null,
    plan: Plan,
};

/// Namespace readers need definitions, not owner receipt arrays. Allocate the
/// projected strings independently: a subsequent transactional point read may
/// invalidate a borrowed store value.
pub const Projection = struct {
    format_version: u16 = 1,
    id: Id,
    namespace_id: u64,
    before: Table,
    after: Table,
    const Table = struct { table_id: u64, name: []const u8, schema_json: []const u8 };

    pub fn decode(a: Allocator, bytes: []const u8, table_id: u64) !std.json.Parsed(Projection) {
        if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidCatalogRecord;
        var parsed = try std.json.parseFromSlice(Projection, a, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        errdefer parsed.deinit();
        const p = parsed.value;
        if (p.format_version != 1 or std.mem.allEqual(u8, &p.id, 0) or p.namespace_id == 0 or table_id == 0 or
            p.before.table_id != table_id or p.after.table_id != table_id or
            !std.mem.eql(u8, p.before.name, p.after.name) or p.before.schema_json.len == 0 or p.after.schema_json.len == 0)
            return error.InvalidCatalogRecord;
        return parsed;
    }

    pub fn sourceDigest(self: Projection) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.relation-index-source.v1");
        hash.update(&self.id);
        var number: [8]u8 = undefined;
        for ([_]u64{ self.namespace_id, self.before.table_id }) |n| {
            std.mem.writeInt(u64, &number, n, .big);
            hash.update(&number);
        }
        for ([_][]const u8{ self.before.name, self.before.schema_json, self.after.schema_json }) |bytes| {
            std.mem.writeInt(u64, &number, bytes.len, .big);
            hash.update(&number);
            hash.update(bytes);
        }
        var out: Digest = undefined;
        hash.final(&out);
        return out;
    }
};

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

    /// The caller must first revalidate the live table/name guard, complete
    /// range cut and protocol admission in this SAME transaction. Any error
    /// during staging requires abort; this helper never independently commits.
    pub fn stageBegin(self: *const Prepared, txn: anytype, group: u64, expected_head: ?[progress.Head.encoded_len]u8) !void {
        var buf: [160]u8 = undefined;
        const prior_bytes = txn.get(try headKey(&buf, group, self.head.table_id)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var old_count: u16 = 0;
        if (expected_head) |expected| {
            if (prior_bytes == null or !std.mem.eql(u8, prior_bytes.?, &expected)) return error.GenerationPublicationChanged;
            const prior = try progress.Head.decode(&expected);
            if (prior.table_id != self.head.table_id or (prior.phase != .published and prior.phase != .canceled) or
                std.mem.eql(u8, &prior.plan_id, &self.head.plan_id)) return error.GenerationPublicationChanged;
            old_count = prior.owner_count;
        } else {
            if (prior_bytes != null) return error.GenerationPublicationChanged;
            if (try optionalGet(txn, try planKey(&buf, group, self.head.table_id)) != null) return error.InvalidCatalogRecord;
            const owner_prefix = try std.fmt.bufPrint(&buf, "{s}owner:{d}:{x:0>16}:", .{ prefix, group, self.head.table_id });
            if (try txn.hasPrefix(owner_prefix)) return error.InvalidCatalogRecord;
        }
        if (try optionalGet(txn, try activeKey(&buf, group, self.head.table_id)) != null) return error.GenerationPublicationChanged;
        try lifecycle.acquire(txn, group, self.head.table_id, .{ .kind = .index_build, .id = self.head.plan_id });
        try txn.put(try planKey(&buf, group, self.head.table_id), self.frame);
        try txn.put(try headKey(&buf, group, self.head.table_id), &try self.head.encode());
        for (self.owners, 0..) |owner, i| try txn.put(try ownerKey(&buf, group, self.head.table_id, @intCast(i)), &try owner.encode());
        for (self.owners.len..@max(self.owners.len, old_count)) |i| try txn.delete(try ownerKey(&buf, group, self.head.table_id, @intCast(i)));
        try txn.put(try activeKey(&buf, group, self.head.table_id), &self.head.plan_id);
    }

    pub fn deinit(self: *Prepared) void {
        self.candidate.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

fn optionalGet(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

/// DROP removes the one retained terminal attempt, not an unbounded history.
/// Active jobs must finish their owner cleanup before ordinary table deletion.
pub fn deleteTerminal(txn: anytype, group: u64, table: u64) !bool {
    var buf: [160]u8 = undefined;
    const bytes = (try optionalGet(txn, try headKey(&buf, group, table))) orelse return false;
    const head = try progress.Head.decode(bytes);
    if (head.table_id != table or (head.phase != .published and head.phase != .canceled) or
        try optionalGet(txn, try activeKey(&buf, group, table)) != null) return error.GenerationPublicationChanged;
    for (0..head.owner_count) |i| try txn.delete(try ownerKey(&buf, group, table, @intCast(i)));
    try txn.delete(try headKey(&buf, group, table));
    try txn.delete(try planKey(&buf, group, table));
    return true;
}

/// A checkpoint verifies the complete immutable cohort once. Normal owner ACKs
/// use only their fixed-size head/owner CAS and never invoke this full verifier.
pub fn verifyJob(a: Allocator, txn: anytype, group: u64, table: u64) !progress.Head {
    var verified = try verifyOwned(a, txn, group, table);
    defer verified.deinit();
    return verified.head;
}

pub const Verified = struct {
    plan: std.json.Parsed(Plan),
    prepared: Prepared,
    head: progress.Head,

    pub fn deinit(self: *Verified) void {
        self.prepared.deinit();
        self.plan.deinit();
        self.* = undefined;
    }
};

/// Keep the independently decoded plan alive for the metadata adapter's
/// current table/range/namespace checks, without parsing the frame a second time.
pub fn verifyOwned(a: Allocator, txn: anytype, group: u64, table: u64) !Verified {
    var buf: [160]u8 = undefined;
    const frame = try txn.get(try planKey(&buf, group, table));
    if (frame.len == 0 or frame.len > max_bytes) return error.InvalidCatalogRecord;
    const frame_digest = digest(frame);
    var parsed = std.json.parseFromSlice(Plan, a, frame, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCatalogRecord,
    };
    errdefer parsed.deinit();
    var prepared = parsed.value.prepare(a) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCatalogRecord,
    };
    errdefer prepared.deinit();
    const head = progress.Head.decode(try txn.get(try headKey(&buf, group, table))) catch return error.InvalidCatalogRecord;
    if (table != prepared.head.table_id or head.table_id != table or
        !std.mem.eql(u8, &head.plan_id, &prepared.head.plan_id) or
        !std.mem.eql(u8, &head.plan_digest, &frame_digest) or !std.mem.eql(u8, &head.plan_digest, &prepared.head.plan_digest) or
        !std.mem.eql(u8, &head.candidate_digest, &prepared.head.candidate_digest) or
        !std.mem.eql(u8, &head.reservation_digest, &prepared.head.reservation_digest) or head.owner_count != prepared.head.owner_count)
        return error.InvalidCatalogRecord;
    var counts: [7]u16 = @splat(0);
    for (prepared.owners, 0..) |expected, i| {
        const owner = progress.OwnerState.decode(try txn.get(try ownerKey(&buf, group, table, @intCast(i)))) catch return error.InvalidCatalogRecord;
        if (!std.meta.eql(owner.owner, expected.owner)) return error.InvalidCatalogRecord;
        for (&counts, 0..) |*count, flag| if (owner.flags & (@as(u8, 1) << @as(u3, @intCast(flag))) != 0) {
            count.* += 1;
        };
    }
    if (!std.meta.eql(counts, head.counts)) return error.InvalidCatalogRecord;
    const pending = switch (head.phase) {
        .installing, .releasing, .published, .canceled => false,
        else => true,
    };
    const active = try optionalGet(txn, try activeKey(&buf, group, table));
    if (pending) {
        if (active == null or !std.mem.eql(u8, active.?, &head.plan_id)) return error.InvalidCatalogRecord;
    } else if (active != null) return error.InvalidCatalogRecord;
    const lock = try lifecycle.read(txn, group, table);
    if (head.phase != .published and head.phase != .canceled) {
        if (lock == null or lock.?.kind != .index_build or !std.mem.eql(u8, &lock.?.id, &head.plan_id)) return error.InvalidCatalogRecord;
    } else if (lock) |held| {
        // A terminal attempt may coexist with a newer, unrelated lifecycle job.
        if (held.kind == .index_build) return error.InvalidCatalogRecord;
    }
    return .{ .plan = parsed, .prepared = prepared, .head = head };
}

test "relational index publication durable keys are canonical group scoped and exclude owner progress from namespace sources" {
    var buf: [160]u8 = undefined;
    const Kind = enum { plan, active, head, owner };
    inline for (std.meta.tags(Kind)) |kind| {
        const key = switch (kind) {
            .plan => try planKey(&buf, 13, 101),
            .active => try activeKey(&buf, 13, 101),
            .head => try headKey(&buf, 13, 101),
            .owner => try ownerKey(&buf, 13, 101, 0),
        };
        try std.testing.expectEqual(@as(?u64, 13), try groupFromKey(key));
        try std.testing.expectEqual(null, try sourceKey(key, 14));
        try std.testing.expectEqual(kind == .plan or kind == .active, try sourceKey(key, 13) != null);
    }
    for ([_][]const u8{
        "plan:013:0000000000000065",        "plan:13:00000000000000Af",       "plan:13:0000000000000000",
        "head:0:0000000000000065",          "owner:13:0000000000000065:1000", "owner:13:0000000000000065:000A",
        "active:13:0000000000000065:extra", "unknown:13:0000000000000065",
    }) |suffix| {
        const key = try std.fmt.bufPrint(&buf, "{s}{s}", .{ prefix, suffix });
        try std.testing.expectError(error.InvalidCatalogRecord, groupFromKey(key));
    }
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, ownerKey(&buf, 13, 101, progress.max_owners));
}

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

pub const testing = if (@import("builtin").is_test) struct {
    pub const plan = testPlan;
} else struct {};

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
