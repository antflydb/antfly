// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Image attempts belong to an existing immutable empty-generation job. A
//! process restart never rewrites that plan or its generation's birth. Before
//! replacing an unpublished empty image, metadata records a successor attempt
//! and invalidates its old image-bound acceptance in that same transaction.
const std = @import("std");
const attestation = @import("sql_relation_attestation.zig");
const boots = @import("sql_relation_boots.zig");
const staging = @import("restore_staging.zig");
const record = @import("schema_validation_publication.zig").Record;
const p = @import("antfly_local_sources").common_topology_records.RelationPersistence;
const A = std.mem.Allocator;
const Origin = @import("antfly_local_sources").storage_db_native_empty_origin.Origin;
pub const Effective = struct { number: u64, digest: [32]u8, initialized: ?attestation.SignedBinding };

pub fn originalAttemptDigest(scope: attestation.CreationScope, before: attestation.SignedBinding) ![32]u8 {
    try scope.validate();
    try before.verify();
    if (!std.meta.eql(before.creation_scope, @as(?attestation.CreationScope, scope))) return error.InvalidSqlRelationPersistence;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/native-empty-target-original-image/v1\x00");
    hash.update(&scope.plan_id);
    hash.update(&scope.plan_digest);
    hash.update(&try (try p.PhysicalPin.fromBinding(before.binding)).encode());
    hash.update(&try before.binding.descriptor.fingerprint());
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

pub const Attempt = struct {
    version: u8 = 1,
    scope: attestation.CreationScope,
    number: u64,
    previous_attempt_digest: [32]u8,
    previous_initialized: attestation.SignedBinding,
    previous_attempt_boot_generation: u64,
    previous_attempt_uninitialized: bool = false,
    previous_acceptance_digest: ?[32]u8 = null,
    origin: Origin,
    boot: boots.Record,
    image: ?attestation.SignedBinding = null,
    initialized: ?attestation.SignedBinding = null,

    pub fn validate(self: Attempt) !void {
        try self.scope.validate();
        try self.origin.validate();
        try self.previous_initialized.verify();
        try self.boot.validate();
        const before = self.previous_initialized;
        if (self.version != 1 or self.number < 2 or self.boot.phase != .recovering or !self.boot.unclean_restart or
            self.boot.generation <= self.previous_attempt_boot_generation or self.previous_attempt_boot_generation < before.boot_generation or !std.meta.eql(before.creation_scope, @as(?attestation.CreationScope, self.scope)) or
            before.binding.descriptor.kind != .unlogged or before.binding.has_user_data or before.binding.has_transaction_state or
            std.mem.eql(u8, &before.binding.native_instance, &self.boot.instance) or
            before.binding.descriptor.owner_node_id != self.boot.identity.node_id or before.binding.descriptor.owner_store_id != self.boot.identity.store_id or
            !std.mem.eql(u8, &before.identity.metadata_incarnation, &self.boot.identity.metadata_incarnation) or
            std.mem.allEqual(u8, &self.previous_attempt_digest, 0)) return error.InvalidSqlRelationPersistence;
        if (self.image) |image| {
            try self.requireInitialized(image);
            const current = self.initialized orelse return error.InvalidSqlRelationPersistence;
            try current.verify();
            if (!std.meta.eql(current.creation_scope, image.creation_scope) or current.metadata_group_id != image.metadata_group_id or
                !current.binding.descriptor.eql(image.binding.descriptor) or !(try p.PhysicalPin.fromBinding(current.binding)).eql(try p.PhysicalPin.fromBinding(image.binding)) or
                !std.mem.eql(u8, &try current.identity.digest(), &try image.identity.digest()) or current.boot_generation < image.boot_generation)
                return error.InvalidSqlRelationPersistence;
        } else if (self.initialized != null) return error.InvalidSqlRelationPersistence;
    }
    pub fn requireInitialized(self: Attempt, proof: attestation.SignedBinding) !void {
        try proof.verify();
        const before = self.previous_initialized;
        if (proof.kind != .initialized or proof.metadata_group_id != before.metadata_group_id or
            !std.meta.eql(proof.creation_scope, @as(?attestation.CreationScope, self.scope)) or
            !proof.binding.descriptor.eql(before.binding.descriptor) or proof.binding.table_id != before.binding.table_id or
            proof.binding.shard_id != before.binding.shard_id or proof.binding.range_id != before.binding.range_id or
            proof.binding.physical_root_incarnation == before.binding.physical_root_incarnation or
            proof.binding.has_user_data or proof.binding.has_transaction_state or proof.boot_generation != self.boot.generation or
            proof.reporter_incarnation != self.boot.reporter_incarnation or
            !std.mem.eql(u8, &proof.binding.native_instance, &self.boot.instance) or
            !std.mem.eql(u8, &try proof.identity.digest(), &try self.boot.identity.digest()) or
            !std.mem.eql(u8, &proof.binding.schema_digest, &before.binding.schema_digest)) return error.SqlRelationOwnerChanged;
    }
    /// Stable reservation identity. The current process receipt may be adopted
    /// cleanly without changing which physical image this attempt represents.
    pub fn digest(self: Attempt) ![32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/native-empty-target-image-attempt/v1\x00");
        hash.update(&self.scope.plan_id);
        hash.update(&self.scope.plan_digest);
        hash.update(&self.previous_attempt_digest);
        var previous_generation: [8]u8 = undefined;
        std.mem.writeInt(u64, &previous_generation, self.previous_attempt_boot_generation, .little);
        hash.update(&previous_generation);
        hash.update(&.{@intFromBool(self.previous_attempt_uninitialized)});
        hash.update(&try self.previous_initialized.digest());
        hash.update(&try self.origin.digest());
        hash.update(&try self.boot.fingerprint());
        var number: [8]u8 = undefined;
        std.mem.writeInt(u64, &number, self.number, .little);
        hash.update(&number);
        hash.update(if (self.previous_acceptance_digest) |digest_value| &digest_value else &(@as([32]u8, @splat(0))));
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};
pub const AttemptRecord = record(Attempt, "NTIA", 16384);
const Head = struct { number: u64, digest: [32]u8, attempt_records: u64, acceptance_records: u64, image_records: u64 };
const HeadRecord = record(Head, "NTIH", 256);
pub const Image = struct { number: u64, digest: [32]u8 };
const ImageRecord = record(Image, "NTII", 256);

pub fn headKey(buffer: []u8, group: u64, scope: attestation.CreationScope, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:restore_staging:{d}:native-image:{s}:{x:0>16}:head", .{ group, std.fmt.bytesToHex(scope.plan_id, .lower), table });
}
pub fn attemptKey(buffer: []u8, group: u64, scope: attestation.CreationScope, table: u64, number: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:restore_staging:{d}:native-image:{s}:{x:0>16}:attempt:{x:0>16}", .{ group, std.fmt.bytesToHex(scope.plan_id, .lower), table, number });
}
pub fn acceptanceHistoryKey(buffer: []u8, group: u64, scope: attestation.CreationScope, table: u64, number: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:restore_staging:{d}:native-image:{s}:{x:0>16}:acceptance:{x:0>16}", .{ group, std.fmt.bytesToHex(scope.plan_id, .lower), table, number });
}
pub fn imageKey(buffer: []u8, group: u64, scope: attestation.CreationScope, table: u64, uuid: u128) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:restore_staging:{d}:native-image:{s}:{x:0>16}:uuid:{x:0>32}", .{ group, std.fmt.bytesToHex(scope.plan_id, .lower), table, uuid });
}
fn readHead(reader: anytype, group: u64, scope: attestation.CreationScope, table: u64) !?Head {
    var key: [320]u8 = undefined;
    const raw = (try optional(reader, try headKey(&key, group, scope, table))) orelse return null;
    const value = try HeadRecord.decode(raw);
    if (value.number < 2 or value.attempt_records != value.number - 1 or value.acceptance_records > value.attempt_records or
        value.image_records == 0 or value.image_records > value.number or std.mem.allEqual(u8, &value.digest, 0)) return error.InvalidSqlRelationPersistence;
    return value;
}
pub fn imageFor(reader: anytype, group: u64, scope: attestation.CreationScope, proof: attestation.SignedBinding) !?Image {
    var key: [320]u8 = undefined;
    const raw = (try optional(reader, try imageKey(&key, group, scope, proof.binding.table_id, proof.binding.physical_root_incarnation))) orelse return null;
    const image = try ImageRecord.decode(raw);
    if (image.number == 0 or std.mem.allEqual(u8, &image.digest, 0)) return error.InvalidSqlRelationPersistence;
    return image;
}
/// Acknowledged physical UUIDs are never reused by a distinct image attempt.
/// This point index avoids an unbounded history walk on every attestation.
pub fn stageImage(txn: anytype, group: u64, scope: attestation.CreationScope, proof: attestation.SignedBinding, number: u64, digest: [32]u8) !bool {
    try proof.verify();
    if (try imageFor(txn, group, scope, proof)) |before| {
        if (before.number != number or !std.mem.eql(u8, &before.digest, &digest)) return error.SqlRelationOwnerChanged;
        return false;
    }
    const value = try ImageRecord.init(.{ .number = number, .digest = digest });
    var key: [320]u8 = undefined;
    try txn.put(try imageKey(&key, group, scope, proof.binding.table_id, proof.binding.physical_root_incarnation), value.slice());
    return true;
}
fn requireImage(reader: anytype, group: u64, scope: attestation.CreationScope, proof: attestation.SignedBinding, number: u64, digest: [32]u8) !void {
    const stored = (try imageFor(reader, group, scope, proof)) orelse return error.InvalidSqlRelationPersistence;
    if (stored.number != number or !std.mem.eql(u8, &stored.digest, &digest)) return error.InvalidSqlRelationPersistence;
}
fn optional(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| return if (err == error.NotFound) null else err;
}
pub fn currentAttempt(reader: anytype, group: u64, scope: attestation.CreationScope, table: u64) !?Attempt {
    var key: [320]u8 = undefined;
    const head = (try readHead(reader, group, scope, table)) orelse return null;
    const raw = (try optional(reader, try attemptKey(&key, group, scope, table, head.number))) orelse return error.InvalidSqlRelationPersistence;
    const attempt = try AttemptRecord.decode(raw);
    try attempt.validate();
    if (attempt.number != head.number or !std.meta.eql(attempt.scope, scope) or attempt.previous_initialized.metadata_group_id != group or attempt.previous_initialized.binding.table_id != table or
        !std.mem.eql(u8, &try attempt.digest(), &head.digest)) return error.InvalidSqlRelationPersistence;
    if (attempt.image) |image| try requireImage(reader, group, scope, image, attempt.number, head.digest);
    return attempt;
}
pub fn effectiveAttempt(a: A, reader: anytype, group: u64, scope: attestation.CreationScope, table: u64) !?Effective {
    if (try currentAttempt(reader, group, scope, table)) |attempt| return .{ .number = attempt.number, .digest = try attempt.digest(), .initialized = attempt.initialized };
    var key: [320]u8 = undefined;
    const raw = (try optional(reader, try staging.nativeBindingKey(&key, group, scope.plan_id, table))) orelse return null;
    const before = try std.json.parseFromSliceLeaky(attestation.SignedBinding, a, raw, .{});
    if (before.metadata_group_id != group or before.binding.table_id != table) return error.InvalidSqlRelationPersistence;
    const digest = try originalAttemptDigest(scope, before);
    try requireImage(reader, group, scope, before, 1, digest);
    return .{ .number = 1, .digest = digest, .initialized = before };
}
pub fn effectiveInitialized(a: A, reader: anytype, group: u64, scope: attestation.CreationScope, table: u64) !?attestation.SignedBinding {
    const value = (try effectiveAttempt(a, reader, group, scope, table)) orelse return null;
    return value.initialized;
}

/// The owning metadata receiver validates the actual immutable job's reset
/// guard, unpublished-empty state, current registration and source LossFence
/// in this same transaction before calling reserve. It atomically removes the
/// old activating receipt/decrements its count after archival below.
pub fn reserve(a: A, txn: anytype, group: u64, scope: attestation.CreationScope, table: u64, boot: boots.Record, origin: Origin, acceptance: ?[]const u8) !Attempt {
    const current = (try effectiveAttempt(a, txn, group, scope, table)) orelse return error.SqlRelationRecoveryIncomplete;
    const prior_attempt = try currentAttempt(txn, group, scope, table);
    const before = current.initialized orelse if (prior_attempt) |prior| prior.previous_initialized else return error.SqlRelationRecoveryIncomplete;
    const prior_boot_generation = if (prior_attempt) |prior| @max(prior.boot.generation, before.boot_generation) else before.boot_generation;
    // An unacknowledged attempt has no physical Pin to synthesize. Its exact
    // reservation is fenced by the newer Boot while retaining the last real
    // initialized proof solely as provenance. The physical producer must use
    // the distinct no-ACK scope/empty-image check for this case.
    if (current.initialized == null and acceptance != null) return error.InvalidSqlRelationPersistence;
    if (prior_attempt) |prior| if (boot.generation <= prior.boot.generation or std.mem.eql(u8, &boot.instance, &prior.boot.instance)) return error.SqlRelationOwnerChanged;
    var acceptance_digest: ?[32]u8 = null;
    if (acceptance) |bytes| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes, &digest, .{});
        acceptance_digest = digest;
    }
    const next: Attempt = .{ .scope = scope, .number = std.math.add(u64, current.number, 1) catch return error.CatalogGenerationExhausted, .previous_attempt_digest = current.digest, .previous_initialized = before, .previous_attempt_boot_generation = prior_boot_generation, .previous_attempt_uninitialized = current.initialized == null, .previous_acceptance_digest = acceptance_digest, .origin = origin, .boot = boot };
    try next.validate();
    var key: [320]u8 = undefined;
    if (try optional(txn, try attemptKey(&key, group, scope, table, next.number)) != null) return error.InvalidSqlRelationPersistence;
    if (try optional(txn, try acceptanceHistoryKey(&key, group, scope, table, current.number)) != null) return error.InvalidSqlRelationPersistence;
    const previous_head = try readHead(txn, group, scope, table);
    const acceptance_records = std.math.add(u64, if (previous_head) |value| value.acceptance_records else 0, @intFromBool(acceptance != null)) catch return error.CatalogGenerationExhausted;
    if (acceptance) |bytes| try txn.put(try acceptanceHistoryKey(&key, group, scope, table, current.number), bytes);
    const encoded = try AttemptRecord.init(next);
    const head = try HeadRecord.init(.{ .number = next.number, .digest = try next.digest(), .attempt_records = next.number - 1, .acceptance_records = acceptance_records, .image_records = if (previous_head) |value| value.image_records else 1 });
    try txn.put(try attemptKey(&key, group, scope, table, next.number), encoded.slice());
    try txn.put(try headKey(&key, group, scope, table), head.slice());
    return next;
}

pub fn stageInitialized(txn: anytype, group: u64, scope: attestation.CreationScope, expected_number: u64, expected_digest: [32]u8, proof: attestation.SignedBinding) !void {
    var attempt = (try currentAttempt(txn, group, scope, proof.binding.table_id)) orelse return error.SqlRelationRecoveryIncomplete;
    if (attempt.number != expected_number or !std.mem.eql(u8, &try attempt.digest(), &expected_digest)) return error.SqlRelationOwnerChanged;
    if (attempt.initialized) |before| {
        if (!std.mem.eql(u8, &try before.digest(), &try proof.digest())) return error.SqlRelationOwnerChanged;
        return;
    }
    try attempt.requireInitialized(proof);
    const inserted = try stageImage(txn, group, scope, proof, attempt.number, expected_digest);
    if (!inserted) return error.InvalidSqlRelationPersistence;
    attempt.image = proof;
    attempt.initialized = proof;
    const encoded = try AttemptRecord.init(attempt);
    var key: [320]u8 = undefined;
    try txn.put(try attemptKey(&key, group, scope, proof.binding.table_id, attempt.number), encoded.slice());
    var head = (try readHead(txn, group, scope, proof.binding.table_id)) orelse return error.InvalidSqlRelationPersistence;
    head.image_records = std.math.add(u64, head.image_records, 1) catch return error.CatalogGenerationExhausted;
    const encoded_head = try HeadRecord.init(head);
    try txn.put(try headKey(&key, group, scope, proof.binding.table_id), encoded_head.slice());
}

/// Registration/current Boot and exact job state are rejoined by the owning
/// receiver. Clean adoption updates only this attempt's process receipt while
/// retaining its first initialized image proof and immutable attempt digest.
pub fn stageAdopted(txn: anytype, group: u64, scope: attestation.CreationScope, proof: attestation.SignedBinding) !bool {
    var attempt = (try currentAttempt(txn, group, scope, proof.binding.table_id)) orelse return false;
    const before = attempt.initialized orelse return error.SqlRelationRecoveryIncomplete;
    if (std.mem.eql(u8, &try before.digest(), &try proof.digest())) return true;
    try @import("sql_relation_lifetimes.zig").requireAdoption(before, proof);
    attempt.initialized = proof;
    try attempt.validate();
    const encoded = try AttemptRecord.init(attempt);
    var key: [320]u8 = undefined;
    try txn.put(try attemptKey(&key, group, scope, proof.binding.table_id, attempt.number), encoded.slice());
    return true;
}

fn directoryPrefix(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:restore_staging:{d}:native-image:", .{group});
}
const Address = struct {
    plan_id: staging.Id,
    table: u64,
    kind: enum { head, attempt, acceptance, uuid },
    ordinal: u64 = 0,
    uuid: u128 = 0,
};
fn address(prefix: []const u8, raw: []const u8) !Address {
    if (!std.mem.startsWith(u8, raw, prefix)) return error.InvalidSqlRelationPersistence;
    var parts = std.mem.splitScalar(u8, raw[prefix.len..], ':');
    const id = parts.next() orelse return error.InvalidSqlRelationPersistence;
    const table = parts.next() orelse return error.InvalidSqlRelationPersistence;
    const kind = parts.next() orelse return error.InvalidSqlRelationPersistence;
    if (id.len != 32 or table.len != 16) return error.InvalidSqlRelationPersistence;
    var parsed: Address = .{ .plan_id = undefined, .table = std.fmt.parseInt(u64, table, 16) catch return error.InvalidSqlRelationPersistence, .kind = undefined };
    _ = std.fmt.hexToBytes(&parsed.plan_id, id) catch return error.InvalidSqlRelationPersistence;
    if (parsed.table == 0) return error.InvalidSqlRelationPersistence;
    if (std.mem.eql(u8, kind, "head")) {
        parsed.kind = .head;
        if (parts.next() != null) return error.InvalidSqlRelationPersistence;
        return parsed;
    }
    const number = parts.next() orelse return error.InvalidSqlRelationPersistence;
    if (parts.next() != null) return error.InvalidSqlRelationPersistence;
    if (std.mem.eql(u8, kind, "uuid")) {
        parsed.kind = .uuid;
        if (number.len != 32) return error.InvalidSqlRelationPersistence;
        parsed.uuid = std.fmt.parseInt(u128, number, 16) catch return error.InvalidSqlRelationPersistence;
        if (parsed.uuid == 0) return error.InvalidSqlRelationPersistence;
    } else {
        if (number.len != 16) return error.InvalidSqlRelationPersistence;
        parsed.ordinal = std.fmt.parseInt(u64, number, 16) catch return error.InvalidSqlRelationPersistence;
        parsed.kind = if (std.mem.eql(u8, kind, "attempt")) .attempt else if (std.mem.eql(u8, kind, "acceptance")) .acceptance else return error.InvalidSqlRelationPersistence;
    }
    return parsed;
}
const Counts = struct { heads: u64 = 0, attempts: u64 = 0, acceptances: u64 = 0, images: u64 = 0 };
fn requireCounts(actual: Counts, head: ?Head, original: ?attestation.SignedBinding) !void {
    const expected: Counts = if (head) |value| .{ .heads = 1, .attempts = value.attempt_records, .acceptances = value.acceptance_records, .images = value.image_records } else .{ .images = @intFromBool(original != null) };
    if (!std.meta.eql(actual, expected)) return error.InvalidSqlRelationPersistence;
}

/// Cold/snapshot validation streams one directory once. Entries are grouped
/// by immutable plan/table key, so at most one bounded plan decode is retained
/// and each attempt compares only its immediate predecessor. Serving reads
/// use the point checks above and never walk this history.
pub fn verifyDirectory(backing: A, reader: anytype, group: u64, rows: anytype, cancelled: ?*const std.atomic.Value(bool)) !void {
    var prefix_buffer: [160]u8 = undefined;
    const prefix = try directoryPrefix(&prefix_buffer, group);
    try rows.reset();
    var plan_region = std.heap.ArenaAllocator.init(backing);
    defer plan_region.deinit();
    var row_region = std.heap.ArenaAllocator.init(backing);
    defer row_region.deinit();
    var current_plan: ?staging.Job = null;
    var current_table: u64 = 0;
    var current_head: ?Head = null;
    var original: ?attestation.SignedBinding = null;
    var counts: Counts = .{};
    var scope: attestation.CreationScope = undefined;
    var target: staging.Target = undefined;
    while (try rows.next()) |item| {
        if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, item.key, prefix)) continue;
        _ = row_region.reset(.retain_capacity);
        const row_alloc = row_region.allocator();
        const selected = try address(prefix, item.key);
        var key: [320]u8 = undefined;
        const new_plan = current_plan == null or !std.mem.eql(u8, &current_plan.?.plan.id, &selected.plan_id);
        if (new_plan or current_table != selected.table) {
            if (current_table != 0) try requireCounts(counts, current_head, original);
            counts = .{};
            if (new_plan) {
                _ = plan_region.reset(.retain_capacity);
                const a = plan_region.allocator();
                const raw = (try optional(reader, try staging.jobKey(&key, group, selected.plan_id))) orelse return error.InvalidSqlRelationPersistence;
                if (raw.len > staging.max_encoded_bytes) return error.InvalidSqlRelationPersistence;
                current_plan = try std.json.parseFromSliceLeaky(staging.Job, a, raw, .{ .allocate = .alloc_always });
                if (!std.mem.eql(u8, &current_plan.?.plan.id, &selected.plan_id) or !std.mem.eql(u8, &try current_plan.?.plan.digest(a), &current_plan.?.plan_digest)) return error.InvalidSqlRelationPersistence;
            }
            const job = current_plan.?;
            scope = .{ .plan_id = job.plan.id, .plan_digest = job.plan_digest };
            current_table = selected.table;
            target = for (job.plan.targets) |value| {
                if (value.table.table_id == current_table) break value;
            } else return error.InvalidSqlRelationPersistence;
            if (target.table.sql_persistence == null or target.ranges.len != 1) return error.InvalidSqlRelationPersistence;
            current_head = try readHead(reader, group, scope, current_table);
            const raw_original = (try optional(reader, try staging.nativeBindingKey(&key, group, scope.plan_id, current_table))) orelse return error.InvalidSqlRelationPersistence;
            original = try std.json.parseFromSliceLeaky(attestation.SignedBinding, row_alloc, raw_original, .{});
            try original.?.verify();
            if (original.?.metadata_group_id != group or original.?.binding.table_id != current_table or !original.?.binding.descriptor.eql(target.table.sql_persistence.?)) return error.InvalidSqlRelationPersistence;
            try requireImage(reader, group, scope, original.?, 1, try originalAttemptDigest(scope, original.?));
            if (current_head != null and ((try staging.nativeEmptyOriginForTarget(current_plan.?.plan, scope.plan_digest, target)) == null or !target.empty_generation)) return error.InvalidSqlRelationPersistence;
            _ = try currentAttempt(reader, group, scope, current_table);
        }
        const a = row_alloc;
        switch (selected.kind) {
            .head => {
                counts.heads += 1;
                if (current_head == null or !std.mem.eql(u8, item.key, try headKey(&key, group, scope, current_table))) return error.InvalidSqlRelationPersistence;
            },
            .attempt => {
                counts.attempts += 1;
                const head = current_head orelse return error.InvalidSqlRelationPersistence;
                const attempt = try AttemptRecord.decode(item.value);
                try attempt.validate();
                if (selected.ordinal < 2 or selected.ordinal > head.number or attempt.number != selected.ordinal or !std.meta.eql(attempt.scope, scope) or
                    attempt.previous_initialized.metadata_group_id != group or attempt.previous_initialized.binding.table_id != current_table or
                    !attempt.previous_initialized.binding.descriptor.eql(target.table.sql_persistence.?) or !std.mem.eql(u8, item.key, try attemptKey(&key, group, scope, current_table, selected.ordinal))) return error.InvalidSqlRelationPersistence;
                const install = try staging.nativeEmptyInstallForRangeAlloc(a, current_plan.?.plan, scope.plan_digest, target, target.ranges[0]);
                if (!std.mem.eql(u8, &try install.origin.digest(), &try attempt.origin.digest())) return error.InvalidSqlRelationPersistence;
                if (selected.ordinal == 2) {
                    if (attempt.previous_attempt_uninitialized or !std.mem.eql(u8, &attempt.previous_attempt_digest, &try originalAttemptDigest(scope, original.?)) or
                        !std.mem.eql(u8, &try attempt.previous_initialized.digest(), &try original.?.digest())) return error.InvalidSqlRelationPersistence;
                } else {
                    const previous_raw = (try optional(reader, try attemptKey(&key, group, scope, current_table, selected.ordinal - 1))) orelse return error.InvalidSqlRelationPersistence;
                    const previous = try AttemptRecord.decode(previous_raw);
                    try previous.validate();
                    if (!std.mem.eql(u8, &attempt.previous_attempt_digest, &try previous.digest()) or attempt.boot.generation <= previous.boot.generation or
                        std.mem.eql(u8, &attempt.boot.instance, &previous.boot.instance) or attempt.previous_attempt_uninitialized != (previous.initialized == null)) return error.InvalidSqlRelationPersistence;
                    const prior_proof = previous.initialized orelse previous.previous_initialized;
                    if (!std.mem.eql(u8, &try prior_proof.digest(), &try attempt.previous_initialized.digest())) return error.InvalidSqlRelationPersistence;
                }
                if (attempt.image) |image| try requireImage(reader, group, scope, image, attempt.number, try attempt.digest());
                if (attempt.previous_acceptance_digest) |digest| {
                    const raw = (try optional(reader, try acceptanceHistoryKey(&key, group, scope, current_table, attempt.number - 1))) orelse return error.InvalidSqlRelationPersistence;
                    var actual: [32]u8 = undefined;
                    std.crypto.hash.Blake3.hash(raw, &actual, .{});
                    if (!std.mem.eql(u8, &digest, &actual)) return error.InvalidSqlRelationPersistence;
                }
            },
            .acceptance => {
                counts.acceptances += 1;
                const head = current_head orelse return error.InvalidSqlRelationPersistence;
                if (selected.ordinal == 0 or selected.ordinal >= head.number or !std.mem.eql(u8, item.key, try acceptanceHistoryKey(&key, group, scope, current_table, selected.ordinal))) return error.InvalidSqlRelationPersistence;
                const next_raw = (try optional(reader, try attemptKey(&key, group, scope, current_table, selected.ordinal + 1))) orelse return error.InvalidSqlRelationPersistence;
                const next = try AttemptRecord.decode(next_raw);
                const expected = next.previous_acceptance_digest orelse return error.InvalidSqlRelationPersistence;
                var digest: [32]u8 = undefined;
                std.crypto.hash.Blake3.hash(item.value, &digest, .{});
                if (!std.mem.eql(u8, &digest, &expected) or next.previous_attempt_uninitialized) return error.InvalidSqlRelationPersistence;
                const receipt = try staging.OwnerReceipt.decodeNative(a, item.value);
                const accepted = receipt.native_empty_acceptance orelse return error.InvalidSqlRelationPersistence;
                try accepted.requireImage(next.previous_initialized.binding, .{ .number = selected.ordinal, .digest = next.previous_attempt_digest });
            },
            .uuid => {
                counts.images += 1;
                const image = try ImageRecord.decode(item.value);
                if (!std.mem.eql(u8, item.key, try imageKey(&key, group, scope, current_table, selected.uuid))) return error.InvalidSqlRelationPersistence;
                if (image.number == 1) {
                    if (selected.uuid != original.?.binding.physical_root_incarnation or !std.mem.eql(u8, &image.digest, &try originalAttemptDigest(scope, original.?))) return error.InvalidSqlRelationPersistence;
                } else {
                    const head = current_head orelse return error.InvalidSqlRelationPersistence;
                    if (image.number < 2 or image.number > head.number) return error.InvalidSqlRelationPersistence;
                    const raw = (try optional(reader, try attemptKey(&key, group, scope, current_table, image.number))) orelse return error.InvalidSqlRelationPersistence;
                    const attempt = try AttemptRecord.decode(raw);
                    const initialized = attempt.image orelse return error.InvalidSqlRelationPersistence;
                    if (initialized.binding.physical_root_incarnation != selected.uuid or !std.mem.eql(u8, &image.digest, &try attempt.digest())) return error.InvalidSqlRelationPersistence;
                }
            },
        }
    }
    if (current_table != 0) try requireCounts(counts, current_head, original);
}

test "SQL native image attempts durably supersede an uninitialized reservation and reject prior UUID reuse" {
    const a = std.testing.allocator;
    const docstore = @import("antfly_local_sources").storage_docstore;
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: @import("antfly_local_sources").storage_db_root_signing_identity.State = .{ .root_incarnation = 11, .seed = seed, .public_key = pair.public_key.toBytes() };
    const scope: attestation.CreationScope = .{ .plan_id = @splat(1), .plan_digest = @splat(2) };
    const before = try attestation.SignedBinding.sign(.{ .kind = .initialized, .metadata_group_id = 1, .boot_generation = 1, .identity = .{ .metadata_incarnation = @splat('a'), .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key }, .reporter_incarnation = 1, .creation_scope = scope, .binding = .{ .descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 1 }, .native_instance = @splat(1), .table_id = 201, .shard_id = 202, .range_id = 202, .physical_root_incarnation = std.math.maxInt(u128) - 1, .schema_version = 1, .schema_digest = @splat(5), .catalog_generation = 1, .catalog_fingerprint = @splat(6) } }, root);
    const origin: Origin = .{ .schema_only_snapshot = .{ .manifest_digest = @splat(3), .schema_digest = before.binding.schema_digest, .source_table_id = 7 } };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/image-attempt-metadata", .{tmp.sub_path}, 0);
    defer a.free(path);
    var store = try docstore.DocStore.open(a, path.ptr, .{});
    var open = true;
    defer if (open) store.close();
    var key_buffer: [320]u8 = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const encoded = try std.json.Stringify.valueAlloc(a, before, .{});
        defer a.free(encoded);
        try txn.put(try staging.nativeBindingKey(&key_buffer, 1, scope.plan_id, before.binding.table_id), encoded);
        _ = try stageImage(&txn, 1, scope, before, 1, try originalAttemptDigest(scope, before));
        try txn.commit();
    }
    var boot: boots.Record = .{ .node_id = before.identity.node_id, .identity = before.identity, .reporter_incarnation = 2, .generation = 2, .instance = @splat(2), .claim_digest = @splat(8), .unclean_restart = true, .phase = .recovering };
    var second: Attempt = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        second = try reserve(a, &txn, 1, scope, before.binding.table_id, boot, origin, null);
        try txn.commit();
    }
    try store.sync(true);
    store.close();
    open = false;
    store = try docstore.DocStore.open(a, path.ptr, .{});
    open = true;
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        const current = (try effectiveAttempt(a, &txn, 1, scope, before.binding.table_id)).?;
        try std.testing.expectEqual(@as(u64, 2), current.number);
        try std.testing.expect(current.initialized == null);
        try std.testing.expectEqualDeep(second, (try currentAttempt(&txn, 1, scope, before.binding.table_id)).?);
    }
    // Metadata committed attempt2, but no initializer replied. A genuinely
    // superseding process epoch must reserve attempt3 without inventing an
    // attempt2 Pin; the last genuine proof remains explicit provenance.
    boot.generation = 3;
    boot.reporter_incarnation = 3;
    boot.instance = @splat(3);
    var third: Attempt = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        third = try reserve(a, &txn, 1, scope, before.binding.table_id, boot, origin, null);
        try std.testing.expectEqual(@as(u64, 3), third.number);
        try std.testing.expect(third.previous_attempt_uninitialized);
        try std.testing.expectEqualDeep(before, third.previous_initialized);
        try txn.commit();
    }
    var successor = before;
    successor.boot_generation = boot.generation;
    successor.reporter_incarnation = boot.reporter_incarnation;
    successor.binding.native_instance = boot.instance;
    successor.binding.physical_root_incarnation = 123;
    successor = try attestation.SignedBinding.sign(successor, root);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.SqlRelationOwnerChanged, stageInitialized(&txn, 1, scope, second.number, try second.digest(), successor));
        try stageInitialized(&txn, 1, scope, third.number, try third.digest(), successor);
        try stageInitialized(&txn, 1, scope, third.number, try third.digest(), successor);
        try txn.commit();
    }
    boot.generation = 4;
    boot.reporter_incarnation = 4;
    boot.instance = @splat(4);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const next = try reserve(a, &txn, 1, scope, before.binding.table_id, boot, origin, null);
        var reused = successor;
        reused.boot_generation = boot.generation;
        reused.reporter_incarnation = boot.reporter_incarnation;
        reused.binding.native_instance = boot.instance;
        reused.binding.physical_root_incarnation = before.binding.physical_root_incarnation;
        reused = try attestation.SignedBinding.sign(reused, root);
        // The immediately prior UUID differs, so only the persistent inverse
        // image index catches accidental reuse of the older original image.
        try std.testing.expectError(error.SqlRelationOwnerChanged, stageInitialized(&txn, 1, scope, next.number, try next.digest(), reused));
    }
    try store.sync(true);
    store.close();
    open = false;
    store = try docstore.DocStore.open(a, path.ptr, .{});
    open = true;
    var txn = try store.beginReadTxn();
    defer txn.abort();
    const accepted = (try effectiveAttempt(a, &txn, 1, scope, before.binding.table_id)).?;
    try std.testing.expectEqual(@as(u64, 3), accepted.number);
    try std.testing.expectEqual(@as(u128, 123), accepted.initialized.?.binding.physical_root_incarnation);
}
