// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
pub const RootProof = @import("owner_root_attestation.zig").Proof("antfly/index-build-owner-receipt/v1");

/// Signature verification alone is not authority. The metadata apply adapter
/// must also check the enrolled current root/reporter and exact live placement
/// in the SAME transaction as its progress CAS. Never accept a service ACK or
/// caller-supplied verification boolean in place of those reads.
pub const SignedReceipt = struct {
    receipt: progress.Receipt,
    proof: RootProof,

    pub fn verifySignature(self: SignedReceipt) !void {
        const logical_digest = self.receipt.digest() catch return error.InvalidIndexOwnerReceipt;
        self.proof.verify(logical_digest) catch return error.InvalidIndexOwnerReceipt;
    }
};
pub const max_bytes = 8 * 1024 * 1024;
pub const max_command_bytes = max_bytes + 16 * 1024;
pub const max_ack_bytes = 4096;
/// A planless, bounded owner acknowledgement. The admitted head and ordinal
/// directory, not request fields, determine the records that may change.
pub const Ack = struct {
    table_id: u64,
    expected_revision: u64,
    signed: SignedReceipt,
};

pub const OwnerOperation = enum { fence, reserve, build, validate, install, release, cancel, cancel_release };
pub const OwnerRequest = struct {
    table_id: u64,
    group_id: u64,
    id: Id,
    operation: OwnerOperation,
    projection: enum { permission, admission } = .permission,

    pub fn validate(self: OwnerRequest) !void {
        if (self.table_id == 0 or self.group_id == 0 or std.mem.allEqual(u8, &self.id, 0) or
            (self.projection == .admission and self.operation != .fence and self.operation != .cancel))
            return error.InvalidGenerationPublication;
    }
};
pub const OwnerDecision = struct {
    metadata_group_id: u64,
    metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    head: progress.Head,
    owner: progress.OwnerState,
    disposition: enum { execute, acknowledged },
    dispatch: ?Dispatch = null,
    inputs: ?Inputs = null,
};

/// Pure phase permission, not remote authentication or a lease. Data apply
/// rechecks its local job/fence/cursor, and metadata ACK rechecks the live head.
pub fn ownerPermission(head: progress.Head, owner: progress.OwnerState, operation: OwnerOperation) !@FieldType(OwnerDecision, "disposition") {
    try head.validate();
    try owner.validate();
    if (owner.owner.namespace.table_id != head.table_id or owner.owner.ordinal >= head.owner_count or
        !std.mem.eql(u8, &owner.owner.plan_id, &head.plan_id) or !std.mem.eql(u8, &owner.owner.plan_digest, &head.plan_digest))
        return error.InvalidCatalogRecord;
    for (head.counts, 0..) |count, index| {
        const present = owner.flags & (@as(u8, 1) << @as(u3, @intCast(index))) != 0;
        if ((count == 0 and present) or (count == head.owner_count and !present)) return error.InvalidCatalogRecord;
    }
    const action: progress.Action = switch (operation) {
        .fence => .fenced,
        .reserve => .reserved,
        .build, .validate => .validated,
        .install => .installed,
        .release => .released,
        .cancel => .canceled,
        .cancel_release => .cancel_released,
    };
    if (owner.flags & (@as(u8, 1) << @as(u3, @intCast(@backingInt(action)))) != 0) return .acknowledged;
    const allowed = switch (operation) {
        .fence => head.phase == .fencing,
        .reserve => head.phase == .reserving,
        .build, .validate => head.phase == .building,
        .install => head.phase == .installing,
        .release => head.phase == .releasing,
        .cancel => head.phase == .canceling,
        .cancel_release => head.phase == .cancel_releasing,
    };
    if (!allowed) return error.GenerationPublicationChanged;
    return .execute;
}

/// The namespace's pending-name source disappears at the publication decision,
/// while the index lifecycle lock remains through install/release. ACK and
/// dispatch must agree on this distinction; absence is not blanket authority.
pub fn requireActivePointer(txn: anytype, group: u64, head: progress.Head) !void {
    var buf: [160]u8 = undefined;
    const active = try optionalGet(txn, try activeKey(&buf, group, head.table_id));
    const pending = switch (head.phase) {
        .installing, .releasing, .published, .canceled => false,
        else => true,
    };
    if (pending) {
        if (active == null or !std.mem.eql(u8, active.?, &head.plan_id)) return error.GenerationPublicationChanged;
    } else if (active != null) return error.GenerationPublicationChanged;
}
const prefix = "\x00\x00__metadata__:relational_index_publication:v1:";
pub fn allGroupsPrefix() []const u8 {
    return prefix;
}
pub fn groupFromKey(key: []const u8) !?u64 {
    if (!std.mem.startsWith(u8, key, prefix)) return null;
    var parts = std.mem.splitScalar(u8, key[prefix.len..], ':');
    const kind = parts.next() orelse return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, kind, "plan") and !std.mem.eql(u8, kind, "head") and
        !std.mem.eql(u8, kind, "active") and !std.mem.eql(u8, kind, "owner") and !std.mem.eql(u8, kind, "group") and
        !std.mem.eql(u8, kind, "inputs") and !std.mem.eql(u8, kind, "dispatch")) return error.InvalidCatalogRecord;
    const number = parts.next() orelse return error.InvalidCatalogRecord;
    const group = std.fmt.parseInt(u64, number, 10) catch return error.InvalidCatalogRecord;
    var buf: [20]u8 = undefined;
    if (group == 0 or !std.mem.eql(u8, number, try std.fmt.bufPrint(&buf, "{d}", .{group}))) return error.InvalidCatalogRecord;
    _ = try tableSuffix(parts.next() orelse return error.InvalidCatalogRecord);
    if (std.mem.eql(u8, kind, "owner") or std.mem.eql(u8, kind, "dispatch")) {
        const ordinal = parts.next() orelse return error.InvalidCatalogRecord;
        if (ordinal.len != 4) return error.InvalidCatalogRecord;
        const value = std.fmt.parseInt(u16, ordinal, 16) catch return error.InvalidCatalogRecord;
        var ordinal_buf: [4]u8 = undefined;
        if (value >= progress.max_owners or !std.mem.eql(u8, ordinal, try std.fmt.bufPrint(&ordinal_buf, "{x:0>4}", .{value}))) return error.InvalidCatalogRecord;
    } else if (std.mem.eql(u8, kind, "group")) {
        _ = try tableSuffix(parts.next() orelse return error.InvalidCatalogRecord);
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
pub fn ownerGroupsPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}group:{d}:", .{ prefix, group });
}
pub fn inputsPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}inputs:{d}:", .{ prefix, group });
}
pub fn dispatchPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}dispatch:{d}:", .{ prefix, group });
}
pub fn inputsKey(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}inputs:{d}:{x:0>16}", .{ prefix, group, table });
}
pub fn dispatchKey(buf: []u8, group: u64, table: u64, ordinal: u16) ![]const u8 {
    if (ordinal >= progress.max_owners) return error.InvalidIndexPublicationCandidate;
    return std.fmt.bufPrint(buf, "{s}dispatch:{d}:{x:0>16}:{x:0>4}", .{ prefix, group, table, ordinal });
}
pub fn ownerGroupKey(buf: []u8, group: u64, table: u64, owner_group: u64) ![]const u8 {
    if (table == 0 or owner_group == 0) return error.InvalidCatalogRecord;
    return std.fmt.bufPrint(buf, "{s}group:{d}:{x:0>16}:{x:0>16}", .{ prefix, group, table, owner_group });
}

/// Fixed-size point directory, shared by owner dispatch and reverse cohort
/// verification. It is not a namespace source or independent job authority.
pub fn ownerOrdinal(reader: anytype, group: u64, table: u64, owner_group: u64) !u16 {
    var buf: [160]u8 = undefined;
    const bytes = try reader.get(try ownerGroupKey(&buf, group, table, owner_group));
    if (bytes.len != 2) return error.InvalidCatalogRecord;
    const ordinal = std.mem.readInt(u16, bytes[0..2], .big);
    if (ordinal >= progress.max_owners) return error.InvalidCatalogRecord;
    return ordinal;
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
    receipt_authority: progress.Authority,
};

/// Shared, immutable worker input. Decode once at admission, not once per page
/// and not once per owner inside the durable metadata representation.
pub const Inputs = struct {
    format_version: u16 = 1,
    id: Id,
    plan_digest: Digest,
    namespace_id: u64,
    index_name: []const u8,
    table_guard: domain.RelationMutationGuard,
    before: records.TableRecord,
    after: records.TableRecord,
    catalog_before_b64: []const u8,

    /// Admission rebuilds the typed logical candidate and verifies BOTH admitted
    /// fingerprints. The projected plan ID/checksum is not a substitute for
    /// schema, index-pair and catalog validation. No owner-cohort decode occurs.
    pub fn prepareCandidate(self: Inputs, a: Allocator, head: progress.Head) !candidate_mod.Candidate {
        try self.requireHead(head);
        var budget: FrameBudget = .{};
        std.json.Stringify.value(self, .{}, &budget.writer) catch return error.IndexPublicationTooLarge;
        var candidate = try prepareInputCandidate(a, self);
        errdefer candidate.deinit();
        if (!std.mem.eql(u8, &digest(candidate.next.value), &head.candidate_digest) or
            !std.mem.eql(u8, &digest(candidate.reservation.value), &head.reservation_digest)) return error.InvalidIndexPublicationCandidate;
        return candidate;
    }

    fn requireHead(self: Inputs, head: progress.Head) !void {
        try head.validate();
        if (self.format_version != 1 or !std.mem.eql(u8, &self.id, &head.plan_id) or
            !std.mem.eql(u8, &self.plan_digest, &head.plan_digest) or self.namespace_id == 0 or
            self.before.table_id != head.table_id or self.after.table_id != head.table_id or
            self.index_name.len == 0) return error.InvalidCatalogRecord;
    }
};

/// The hot owner projection contains no schema/catalog JSON or owner cohort.
/// The table guard and successor identity support indexed live-catalog checks;
/// the complete owner descriptor is independently bound by fence_digest.
pub const Dispatch = struct {
    format_version: u16 = 1,
    id: Id,
    plan_digest: Digest,
    ordinal: u16,
    namespace_id: u64,
    table_guard: domain.RelationMutationGuard,
    table_name: []const u8,
    successor_schema_version: u32,
    successor_schema_digest: Digest,
    owner: Owner,
};

/// These reads own every decoded string before another transactional get can
/// invalidate borrowed bytes. A projection is not authority by itself: callers
/// must independently recheck the live phase, lifecycle lock and catalog cut.
pub fn readInputs(a: Allocator, txn: anytype, group: u64, head: progress.Head) !std.json.Parsed(Inputs) {
    try head.validate();
    var buf: [160]u8 = undefined;
    const bytes = try txn.get(try inputsKey(&buf, group, head.table_id));
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidCatalogRecord;
    var parsed = try std.json.parseFromSlice(Inputs, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.requireHead(head);
    try parsed.value.table_guard.validate();
    return parsed;
}

pub fn readDispatch(a: Allocator, txn: anytype, group: u64, expected: progress.Owner) !std.json.Parsed(Dispatch) {
    try (progress.OwnerState{ .owner = expected }).validate();
    var buf: [160]u8 = undefined;
    const bytes = try txn.get(try dispatchKey(&buf, group, expected.namespace.table_id, expected.ordinal));
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidCatalogRecord;
    var parsed = try std.json.parseFromSlice(Dispatch, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    const d = parsed.value;
    if (d.format_version != 1 or !std.mem.eql(u8, &d.id, &expected.plan_id) or
        !std.mem.eql(u8, &d.plan_digest, &expected.plan_digest) or d.ordinal != expected.ordinal or
        d.namespace_id == 0 or d.table_name.len == 0 or d.successor_schema_version == 0 or
        d.owner.range.group_id != expected.group_id or !d.owner.namespace.eql(expected.namespace) or
        d.owner.receipt_authority != expected.authority or
        !std.mem.eql(u8, &try ownerFenceDigest(d.owner), &expected.fence_digest)) return error.InvalidCatalogRecord;
    try d.table_guard.validate();
    if (d.table_guard.owner.kind != .table or d.table_guard.owner.table_id != expected.namespace.table_id or
        d.table_guard.constraint_name_admission != null or
        !std.mem.eql(u8, d.table_guard.target.name, d.table_guard.logical_table)) return error.InvalidCatalogRecord;
    return parsed;
}

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

/// Shared validation for original admission and its independently fetched
/// worker input. Candidate allocations cannot retain this temporary arena.
fn prepareInputCandidate(alloc: Allocator, input: anytype) !candidate_mod.Candidate {
    if (input.format_version != 1 or input.index_name.len == 0 or input.index_name.len > names.max_name_bytes)
        return error.InvalidIndexPublicationCandidate;
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const s = scratch.allocator();
    try input.table_guard.validate();
    if (input.table_guard.owner.kind != .table or input.table_guard.constraint_name_admission != null or
        input.table_guard.owner.table_id != input.before.table_id or
        !std.mem.eql(u8, input.table_guard.target.name, input.table_guard.logical_table) or
        input.table_guard.owner.schema_version != try @import("../api/tables.zig").schemaVersion(input.before.schema_json) or
        !std.mem.eql(u8, &input.table_guard.owner.schema_digest, &digest(input.before.schema_json)))
        return error.InvalidIndexPublicationCandidate;
    try shape.requireTableImages(s, input.before, input.after);
    const length = std.base64.standard.Decoder.calcSizeForSlice(input.catalog_before_b64) catch return error.InvalidIndexPublicationCandidate;
    if (length == 0 or length > local.storage_db_relational_integrity_catalog.max_catalog_bytes) return error.InvalidIndexPublicationCandidate;
    const predecessor = try s.alloc(u8, length);
    std.base64.standard.Decoder.decode(predecessor, input.catalog_before_b64) catch return error.InvalidIndexPublicationCandidate;
    var candidate = try candidate_mod.Candidate.prepare(alloc, input.before.table_id, input.before.schema_json, input.after.schema_json, predecessor, input.index_name);
    errdefer candidate.deinit();
    if (!try @import("../api/tables.zig").foreignKeyPublicationIndexesValid(s, input.before.name, input.before.indexes_json, input.after.indexes_json, input.after.schema_json, candidate.next.catalog.schema_version))
        return error.InvalidIndexPublicationCandidate;
    return candidate;
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
        var candidate = try prepareInputCandidate(alloc, self);
        errdefer candidate.deinit();
        const ranges = try s.alloc(records.RangeRecord, self.owners.len);
        // Membership checks are linear even for the maximum owner cohort.
        var groups: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer groups.deinit(s);
        const before_digest = digest(candidate.reservation.expected.?);
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
                .namespace = owner.namespace,
                .authority = owner.receipt_authority,
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
        const inputs = try encodeBounded(a, Inputs{
            .id = self.id,
            .plan_digest = plan_digest,
            .namespace_id = self.namespace_id,
            .index_name = self.index_name,
            .table_guard = self.table_guard,
            .before = self.before,
            .after = self.after,
            .catalog_before_b64 = self.catalog_before_b64,
        });
        const dispatch = try a.alloc([]const u8, self.owners.len);
        const successor_version = try @import("../api/tables.zig").schemaVersion(self.after.schema_json);
        const successor_digest = digest(self.after.schema_json);
        for (self.owners, dispatch, 0..) |owner, *bytes, ordinal| bytes.* = try encodeBounded(a, Dispatch{
            .id = self.id,
            .plan_digest = plan_digest,
            .ordinal = @intCast(ordinal),
            .namespace_id = self.namespace_id,
            .table_guard = self.table_guard,
            .table_name = self.before.name,
            .successor_schema_version = successor_version,
            .successor_schema_digest = successor_digest,
            .owner = owner,
        });
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
            .inputs = inputs,
            .dispatch = dispatch,
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
    inputs: []const u8,
    dispatch: []const []const u8,
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
        var old_id: Id = @splat(0);
        if (expected_head) |expected| {
            if (prior_bytes == null or !std.mem.eql(u8, prior_bytes.?, &expected)) return error.GenerationPublicationChanged;
            const prior = try progress.Head.decode(&expected);
            if (prior.table_id != self.head.table_id or (prior.phase != .published and prior.phase != .canceled) or
                std.mem.eql(u8, &prior.plan_id, &self.head.plan_id)) return error.GenerationPublicationChanged;
            old_count = prior.owner_count;
            old_id = prior.plan_id;
        } else {
            if (prior_bytes != null) return error.GenerationPublicationChanged;
            if (try optionalGet(txn, try planKey(&buf, group, self.head.table_id)) != null) return error.InvalidCatalogRecord;
            if (try optionalGet(txn, try inputsKey(&buf, group, self.head.table_id)) != null) return error.InvalidCatalogRecord;
            const owner_prefix = try std.fmt.bufPrint(&buf, "{s}owner:{d}:{x:0>16}:", .{ prefix, group, self.head.table_id });
            if (try txn.hasPrefix(owner_prefix)) return error.InvalidCatalogRecord;
            const group_prefix = try std.fmt.bufPrint(&buf, "{s}group:{d}:{x:0>16}:", .{ prefix, group, self.head.table_id });
            if (try txn.hasPrefix(group_prefix)) return error.InvalidCatalogRecord;
            const dispatch_prefix = try std.fmt.bufPrint(&buf, "{s}dispatch:{d}:{x:0>16}:", .{ prefix, group, self.head.table_id });
            if (try txn.hasPrefix(dispatch_prefix)) return error.InvalidCatalogRecord;
        }
        if (try optionalGet(txn, try activeKey(&buf, group, self.head.table_id)) != null) return error.GenerationPublicationChanged;
        try lifecycle.acquire(txn, group, self.head.table_id, .{ .kind = .index_build, .id = self.head.plan_id });
        // Remove the predecessor's group directory before overwriting ordinal
        // rows. Its cohort may have changed completely during terminal DDL.
        for (0..old_count) |i| {
            const old = try progress.OwnerState.decode(try txn.get(try ownerKey(&buf, group, self.head.table_id, @intCast(i))));
            if (old.owner.ordinal != i or !std.mem.eql(u8, &old.owner.plan_id, &old_id)) return error.InvalidCatalogRecord;
            try txn.delete(try ownerGroupKey(&buf, group, self.head.table_id, old.owner.group_id));
        }
        try txn.put(try planKey(&buf, group, self.head.table_id), self.frame);
        try txn.put(try inputsKey(&buf, group, self.head.table_id), self.inputs);
        try txn.put(try headKey(&buf, group, self.head.table_id), &try self.head.encode());
        for (self.owners, 0..) |owner, i| {
            try txn.put(try ownerKey(&buf, group, self.head.table_id, @intCast(i)), &try owner.encode());
            try txn.put(try dispatchKey(&buf, group, self.head.table_id, @intCast(i)), self.dispatch[i]);
            var ordinal: [2]u8 = undefined;
            std.mem.writeInt(u16, &ordinal, @intCast(i), .big);
            try txn.put(try ownerGroupKey(&buf, group, self.head.table_id, owner.owner.group_id), &ordinal);
        }
        for (self.owners.len..@max(self.owners.len, old_count)) |i| {
            try txn.delete(try ownerKey(&buf, group, self.head.table_id, @intCast(i)));
            try txn.delete(try dispatchKey(&buf, group, self.head.table_id, @intCast(i)));
        }
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
    for (0..head.owner_count) |i| {
        const owner = try progress.OwnerState.decode(try txn.get(try ownerKey(&buf, group, table, @intCast(i))));
        if (owner.owner.ordinal != i or !std.mem.eql(u8, &owner.owner.plan_id, &head.plan_id)) return error.InvalidCatalogRecord;
        try txn.delete(try ownerGroupKey(&buf, group, table, owner.owner.group_id));
        try txn.delete(try ownerKey(&buf, group, table, @intCast(i)));
        try txn.delete(try dispatchKey(&buf, group, table, @intCast(i)));
    }
    try txn.delete(try headKey(&buf, group, table));
    try txn.delete(try planKey(&buf, group, table));
    try txn.delete(try inputsKey(&buf, group, table));
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
    if (!std.mem.eql(u8, try txn.get(try inputsKey(&buf, group, table)), prepared.inputs)) return error.InvalidCatalogRecord;
    var counts: [7]u16 = @splat(0);
    for (prepared.owners, 0..) |expected, i| {
        const owner = progress.OwnerState.decode(try txn.get(try ownerKey(&buf, group, table, @intCast(i)))) catch return error.InvalidCatalogRecord;
        if (!std.meta.eql(owner.owner, expected.owner)) return error.InvalidCatalogRecord;
        if (try ownerOrdinal(txn, group, table, owner.owner.group_id) != i) return error.InvalidCatalogRecord;
        if (!std.mem.eql(u8, try txn.get(try dispatchKey(&buf, group, table, @intCast(i))), prepared.dispatch[i])) return error.InvalidCatalogRecord;
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
    const Kind = enum { plan, active, head, owner, group, inputs, dispatch };
    inline for (std.meta.tags(Kind)) |kind| {
        const key = switch (kind) {
            .plan => try planKey(&buf, 13, 101),
            .active => try activeKey(&buf, 13, 101),
            .head => try headKey(&buf, 13, 101),
            .owner => try ownerKey(&buf, 13, 101, 0),
            .group => try ownerGroupKey(&buf, 13, 101, 301),
            .inputs => try inputsKey(&buf, 13, 101),
            .dispatch => try dispatchKey(&buf, 13, 101, 0),
        };
        try std.testing.expectEqual(@as(?u64, 13), try groupFromKey(key));
        try std.testing.expectEqual(null, try sourceKey(key, 14));
        try std.testing.expectEqual(kind == .plan or kind == .active, try sourceKey(key, 13) != null);
    }
    for ([_][]const u8{
        "plan:013:0000000000000065",        "plan:13:00000000000000Af",          "plan:13:0000000000000000",
        "head:0:0000000000000065",          "owner:13:0000000000000065:1000",    "owner:13:0000000000000065:000A",
        "active:13:0000000000000065:extra", "unknown:13:0000000000000065",       "group:13:0000000000000065:0000000000000000",
        "group:13:0000000000000065:000A",   "dispatch:13:0000000000000065:1000", "inputs:13:0000000000000065:extra",
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
        .receipt_authority = .raft,
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

test "relational index publication signatures bind owner authority position and root generation" {
    const seed: [32]u8 = @splat(9);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .seed = seed, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
    inline for (.{ progress.Authority.raft, progress.Authority.native }) |authority| {
        const namespace: local.storage_db_doc_identity.Namespace = .{ .table_id = 101, .shard_id = 301, .range_id = 401 };
        const receipt: progress.Receipt = .{
            .owner = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .ordinal = 0, .group_id = 301, .namespace = namespace, .authority = authority, .fence_digest = @splat(3) },
            .action = .fenced,
            .position = if (authority == .native) .{ .native = .{ .namespace = namespace, .sequence = 5 } } else .{ .raft = .{ .term = 2, .index = 5 } },
            .candidate_digest = @splat(4),
            .reservation_digest = @splat(5),
            .evidence_digest = @splat(6),
        };
        const signed: SignedReceipt = .{ .receipt = receipt, .proof = try (RootProof{
            .identity = .{ .metadata_incarnation = @splat('1'), .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key },
            .reporter_incarnation = 4,
            .replica_id = 5,
            .root_generation = 6,
            .signature = undefined,
        }).sign(root, try receipt.digest()) };
        try signed.verifySignature();
        var altered = signed;
        altered.receipt.owner.namespace.shard_id += 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        if (authority == .native) altered.receipt.position.native.sequence += 1 else altered.receipt.position.raft.index += 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.receipt.candidate_digest[0] ^= 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.receipt.action = .reserved;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.proof.root_generation += 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.proof.reporter_incarnation += 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        const foreign = try (@import("initial_child_root_attestation.zig").Attestation{
            .identity = signed.proof.identity,
            .reporter_incarnation = signed.proof.reporter_incarnation,
            .replica_id = signed.proof.replica_id,
            .root_generation = signed.proof.root_generation,
            .signature = undefined,
        }).sign(root, try receipt.digest());
        altered = signed;
        altered.proof.signature = foreign.signature;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
    }
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

test "relational index publication worker projections use two point reads own borrowed bytes and clean every allocation failure" {
    const Reader = struct {
        prepared: *const Prepared,
        scratch: []u8,
        calls: usize = 0,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.calls += 1;
            var buf: [160]u8 = undefined;
            const source = if (std.mem.eql(u8, key, try inputsKey(&buf, 13, self.prepared.head.table_id)))
                self.prepared.inputs
            else if (std.mem.eql(u8, key, try dispatchKey(&buf, 13, self.prepared.head.table_id, 0)))
                self.prepared.dispatch[0]
            else
                return error.UnexpectedRead;
            @memset(self.scratch, 0);
            @memcpy(self.scratch[0..source.len], source);
            return self.scratch[0..source.len];
        }
        fn exercise(a: Allocator, p: *const Prepared) !void {
            const scratch = try a.alloc(u8, @max(p.inputs.len, p.dispatch[0].len));
            defer a.free(scratch);
            var reader: @This() = .{ .prepared = p, .scratch = scratch };
            var inputs = try readInputs(a, &reader, 13, p.head);
            defer inputs.deinit();
            var dispatch = try readDispatch(a, &reader, 13, p.owners[0].owner);
            defer dispatch.deinit();
            @memset(scratch, 0);
            try std.testing.expectEqual(@as(usize, 2), reader.calls);
            try std.testing.expectEqualSlices(u8, &dispatch.value.table_guard.owner.schema_digest, &digest(inputs.value.before.schema_json));
            var candidate = try inputs.value.prepareCandidate(a, p.head);
            defer candidate.deinit();
            try std.testing.expectEqualSlices(u8, p.candidate.next.value, candidate.next.value);
            try std.testing.expectEqualStrings(inputs.value.before.name, dispatch.value.table_name);
            try std.testing.expectEqualSlices(u8, &digest(inputs.value.after.schema_json), &dispatch.value.successor_schema_digest);
        }
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const plan = try testPlan(arena.allocator());
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    try Reader.exercise(a, &prepared);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Reader.exercise, .{&prepared});
}

test "relational index publication dispatch omits schema sized inputs and immutable projections replay exactly" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var plan = try testPlan(arena.allocator());
    const description = try arena.allocator().alloc(u8, 64 * 1024);
    @memset(description, 'x');
    plan.before.description = description;
    plan.after.description = description;
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    try std.testing.expect(prepared.inputs.len > 128 * 1024);
    try std.testing.expect(prepared.dispatch[0].len < 4096);
    try std.testing.expect(std.mem.indexOf(u8, prepared.dispatch[0], description[0..32]) == null);
    var decoded = try std.json.parseFromSlice(Plan, a, prepared.frame, .{});
    defer decoded.deinit();
    var replay = try decoded.value.prepare(a);
    defer replay.deinit();
    try std.testing.expectEqualSlices(u8, prepared.inputs, replay.inputs);
    try std.testing.expectEqualSlices(u8, prepared.dispatch[0], replay.dispatch[0]);
}

test "relational index publication projected candidate rejects changed admitted fingerprints" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const plan = try testPlan(arena.allocator());
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    var inputs = try std.json.parseFromSlice(Inputs, a, prepared.inputs, .{});
    defer inputs.deinit();
    var candidate = try inputs.value.prepareCandidate(a, prepared.head);
    defer candidate.deinit();
    var changed = prepared.head;
    changed.candidate_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, inputs.value.prepareCandidate(a, changed));
    changed = prepared.head;
    changed.reservation_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, inputs.value.prepareCandidate(a, changed));
    changed = prepared.head;
    changed.plan_id[0] ^= 1;
    try std.testing.expectError(error.InvalidCatalogRecord, inputs.value.prepareCandidate(a, changed));
}

test "relational index publication permission separates phase execution acknowledged retries and cancellation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const plan = try testPlan(arena.allocator());
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    var head = prepared.head;
    var owner = prepared.owners[0];
    try std.testing.expectEqual(.execute, try ownerPermission(head, owner, .fence));
    try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(head, owner, .reserve));
    const canceled = try head.cancel(head.revision, .requested);
    try std.testing.expectEqual(.execute, try ownerPermission(canceled, owner, .cancel));
    try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(canceled, owner, .fence));
    for ([_]progress.Action{ .fenced, .reserved, .validated }, 0..) |action, i| {
        const update = try progress.acknowledge(head, owner, head.revision, .{
            .owner = owner.owner,
            .action = action,
            .position = .{ .raft = .{ .term = 1, .index = i + 1 } },
            .candidate_digest = head.candidate_digest,
            .reservation_digest = head.reservation_digest,
            .evidence_digest = @splat(9),
        });
        head = update.head;
        owner = update.owner;
        try std.testing.expectEqual(.acknowledged, try ownerPermission(head, owner, .fence));
    }
    try std.testing.expectEqual(.acknowledged, try ownerPermission(head, owner, .build));
    try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(head, owner, .install));
    head = try head.decidePublish(head.revision);
    try std.testing.expectEqual(.execute, try ownerPermission(head, owner, .install));
    const Reader = struct {
        value: ?[]const u8,
        pub fn get(self: *@This(), _: []const u8) anyerror![]const u8 {
            return self.value orelse error.NotFound;
        }
    };
    var reader: Reader = .{ .value = null };
    try requireActivePointer(&reader, 13, head);
    reader.value = &head.plan_id;
    try std.testing.expectError(error.GenerationPublicationChanged, requireActivePointer(&reader, 13, head));
    try requireActivePointer(&reader, 13, prepared.head);
    reader.value = null;
    try std.testing.expectError(error.GenerationPublicationChanged, requireActivePointer(&reader, 13, prepared.head));
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
