// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Durable process admission for native TEMP/UNLOGGED owners. This contains
//! no row payloads. The caller authenticates the registered native context
//! instance before the same metadata transaction changes this boot record.
const std = @import("std");
const persistence = @import("antfly_local_sources").common_topology_records.RelationPersistence;
const enrollment = @import("store_root_enrollment.zig");
const A = std.mem.Allocator;
pub const Phase = enum { recovering, ready, draining, clean };
pub const Record = struct {
    version: u8 = 1,
    node_id: u64,
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    generation: u64,
    instance: [16]u8,
    claim_digest: [32]u8,
    unclean_restart: bool,
    phase: Phase,
    recovery_after: ?u64 = null,

    pub fn validate(self: Record) !void {
        try self.identity.validate();
        if (self.version != 1 or self.node_id != self.identity.node_id or self.reporter_incarnation == 0 or self.generation == 0 or
            std.mem.allEqual(u8, &self.instance, 0) or std.mem.allEqual(u8, &self.claim_digest, 0) or
            (self.recovery_after != null and self.phase != .recovering)) return error.InvalidSqlRelationPersistence;
    }
    pub fn boot(self: Record) persistence.Boot {
        return .{ .instance = self.instance, .unclean_restart = self.unclean_restart };
    }
    pub fn requireRegistration(self: Record, current: Registration) !void {
        try self.validate();
        if (self.reporter_incarnation != current.reporter_incarnation or
            !std.mem.eql(u8, &self.instance, &current.instance) or
            !std.mem.eql(u8, &try self.identity.digest(), &try current.identity.digest()))
            return error.SqlRelationOwnerChanged;
    }
    pub fn fingerprint(self: Record) ![32]u8 {
        try self.validate();
        var bytes: [2048]u8 = undefined;
        var writer = std.Io.Writer.fixed(&bytes);
        try std.json.Stringify.value(self, .{}, &writer);
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(writer.buffered(), &digest, .{});
        return digest;
    }
};

pub const Registration = struct {
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    instance: [16]u8,
};

/// Fixed metadata-only maintenance scope. It carries no row key, projection,
/// SQL principal or caller-authored Owner grant.
pub const MaintenanceRequest = struct {
    owner_group_id: u64,
    namespace: @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace,
    descriptor: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor,
    schema_digest: [32]u8,
    indexes_digest: [32]u8,

    pub fn validate(self: @This()) !void {
        try self.descriptor.validate();
        if (self.descriptor.kind != .temporary or self.owner_group_id == 0 or self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            std.mem.allEqual(u8, &self.schema_digest, 0) or std.mem.allEqual(u8, &self.indexes_digest, 0)) return error.InvalidSqlRelationPersistence;
    }
    pub fn requireTable(self: @This(), table: @import("table_manager.zig").TableRecord) !void {
        try self.validate();
        var digest: [32]u8 = undefined;
        if (table.table_id != self.namespace.table_id or table.sql_persistence == null or !table.sql_persistence.?.eql(self.descriptor)) return error.SqlRelationOwnerChanged;
        std.crypto.hash.Blake3.hash(table.schema_json, &digest, .{});
        if (!std.mem.eql(u8, &digest, &self.schema_digest)) return error.CatalogGenerationChanged;
        std.crypto.hash.Blake3.hash(table.indexes_json, &digest, .{});
        if (!std.mem.eql(u8, &digest, &self.indexes_digest)) return error.CatalogGenerationChanged;
    }
};
pub const MaintenanceLease = struct {
    request: MaintenanceRequest,
    metadata_group_id: u64,
    metadata_incarnation: [32]u8,
    binding: @import("../system_catalog/temporary_source_binding.zig").Binding,
    expires_at_ms: u64,
    native_pin: @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin,
    boot_generation: u64,
    execution_instance: [16]u8,

    pub fn requireFor(self: @This(), request: MaintenanceRequest, now_ms: u64) !void {
        try request.validate();
        try self.binding.validate();
        try self.native_pin.validate();
        if (self.boot_generation == 0 or std.mem.allEqual(u8, &self.execution_instance, 0) or
            self.native_pin.table_id != request.namespace.table_id or self.native_pin.shard_id != request.namespace.shard_id or self.native_pin.range_id != request.namespace.range_id or self.native_pin.data_generation != request.descriptor.data_generation or
            !std.mem.eql(u8, &self.execution_instance, &request.descriptor.generation_instance)) return error.SqlTemporaryRelationNotOwned;
        if (!std.meta.eql(self.request, request) or self.metadata_group_id == 0 or self.binding.group != self.metadata_group_id or
            !std.mem.eql(u8, &std.fmt.bytesToHex(self.binding.backend.incarnation, .lower), &self.metadata_incarnation) or
            !self.binding.namespace.owner.eql(request.descriptor.owner.?) or self.expires_at_ms <= now_ms) return error.SqlTemporaryRelationNotOwned;
    }
};

pub const Observation = struct {
    maintenance: ?MaintenanceLease = null,
    restore_terminal: ?@import("sql_relation_restore_terminal.zig").Proof = null,
    data_version_ready: ?bool = null,
    record: ?Record,
    registration_current: bool,
    after: ?u64 = null,
    initialized: ?@import("sql_relation_attestation.zig").SignedBinding = null,
    uninitialized: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.UninitializedAdmission = null,
    report_digest: ?[32]u8 = null,
    attempt: ?@import("sql_relation_empty_target.zig").Attempt = null,
    empty_target: ?@import("sql_relation_empty_target.zig").Admission = null,
    image_attempt: ?@import("sql_relation_target_images.zig").Attempt = null,
    unpublished_retirement: ?@import("sql_relation_unpublished_retirement.zig").Proof = null,
    unpublished_retirement_committed: bool = false,
    retired_group: ?@import("native_group_retirement.zig").Record = null,
    native_group_active: bool = false,
    native_retired_image: ?struct { metadata_incarnation: [32]u8, namespace: @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace, descriptor_fingerprint: [32]u8 } = null,
    lost_generation: ?@import("sql_relation_recovery.zig").RetiredGeneration = null,

    pub fn jsonStringify(self: Observation, writer: anytype) !void {
        if (self.maintenance != null) return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .initialized = self.initialized, .maintenance = self.maintenance });
        if (self.restore_terminal != null) return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .restore_terminal = self.restore_terminal });
        if (self.data_version_ready) |ready| return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .data_version_ready = ready });
        if (self.retired_group != null or self.native_retired_image != null or self.native_group_active) return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .after = self.after, .retired_group = self.retired_group, .native_group_active = self.native_group_active, .native_retired_image = self.native_retired_image });
        if (self.unpublished_retirement != null) return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .after = self.after, .unpublished_retirement = self.unpublished_retirement, .unpublished_retirement_committed = self.unpublished_retirement_committed });
        if (self.attempt != null or self.empty_target != null or self.image_attempt != null) return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .after = self.after, .initialized = self.initialized, .attempt = self.attempt, .empty_target = self.empty_target, .image_attempt = self.image_attempt });
        if (self.uninitialized != null) return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .after = self.after, .uninitialized = self.uninitialized });
        if (self.lost_generation != null) return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .after = self.after, .lost_generation = self.lost_generation });
        if (self.report_digest != null) return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .after = self.after, .report_digest = self.report_digest });
        if (self.initialized != null) return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .after = self.after, .initialized = self.initialized });
        return writer.write(.{ .record = self.record, .registration_current = self.registration_current, .after = self.after });
    }

    pub fn requireReady(self: Observation) !Record {
        const value = self.record orelse return error.SqlRelationOwnerUnavailable;
        try value.validate();
        if (!self.registration_current) return error.SqlRelationOwnerChanged;
        if (value.phase != .ready) return error.SqlRelationRecoveryIncomplete;
        return value;
    }
};

/// Immutable source selected by the Boot recovery feed, never by SQL name.
pub const LostTemporary = struct {
    entry: @import("sql_relation_lifetimes.zig").Entry,
    boot: Record,
};
pub const LostTemporaryTarget = struct {
    retired: bool = false,
    namespace: ?@import("../system_catalog/temporary_namespace_index.zig").Target = null,
    held: ?struct { head: @import("scope_retirement_ledger.zig").Head, ordinal: u16 } = null,
};

pub const AbandonedTemporaryPage = struct {
    next: ?u64 = null,
    target: ?struct {
        namespace: @import("../system_catalog/temporary_namespace_index.zig").Target,
        guard: @import("../system_catalog/temporary_source_binding.zig").AbandonmentGuard,
    } = null,
};

pub const Query = struct {
    maintenance: ?MaintenanceRequest = null,
    restore_terminal: ?@import("sql_relation_restore_terminal.zig").Query = null,
    required_data_version: ?u16 = null,
    retired_group: ?u64 = null,
    recovery_capture: ?@import("sql_relation_recovery_capture.zig").Request = null,
    node: ?u64 = null,
    preferred_node: ?u64 = null,
    after: ?u64 = null,
    limit: u16 = 128,
    binding_table: ?u64 = null,
    unpublished_retirement: ?struct { scope: @import("sql_relation_attestation.zig").CreationScope, table_id: u64 } = null,
    unpublished_group: ?u64 = null,
    lost_generation: ?@import("sql_relation_recovery.zig").Query = null,
    lost_temporary: ?LostTemporary = null,
    temporary_abandonment: ?struct { after: ?u64 = null } = null,
    report_group: ?u64 = null,
    temporary_namespaces: ?@import("../system_catalog/temporary_namespace_index.zig").Selection = null,
    temporary_retirement: ?@import("scope_retirement_wire.zig").TemporaryProbe = null,
    creation_scope: ?@import("sql_relation_attestation.zig").CreationScope = null,

    pub fn jsonStringify(self: Query, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Query).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "maintenance") or std.mem.eql(u8, field, "restore_terminal") or std.mem.eql(u8, field, "required_data_version") or std.mem.eql(u8, field, "binding_table") or std.mem.eql(u8, field, "report_group") or std.mem.eql(u8, field, "creation_scope") or std.mem.eql(u8, field, "temporary_namespaces") or std.mem.eql(u8, field, "temporary_retirement") or std.mem.eql(u8, field, "lost_generation") or std.mem.eql(u8, field, "lost_temporary") or std.mem.eql(u8, field, "temporary_abandonment") or std.mem.eql(u8, field, "unpublished_retirement") or std.mem.eql(u8, field, "unpublished_group") or std.mem.eql(u8, field, "recovery_capture") or std.mem.eql(u8, field, "retired_group")) {
                if (@field(self, field) != null) {
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
};

pub const Claim = struct {
    node_id: u64,
    instance: [16]u8,
    expected_generation: u64,
    expected_instance: ?[16]u8,
    pub fn digest(self: Claim) ![32]u8 {
        if (std.mem.allEqual(u8, &self.instance, 0) or
            ((self.expected_generation == 0) != (self.expected_instance == null))) return error.InvalidSqlRelationPersistence;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-relation-boot-claim/v1\x00");
        hash.update(&self.instance);
        if (self.expected_instance) |value| hash.update(&value);
        var number: [8]u8 = undefined;
        inline for (.{ self.node_id, self.expected_generation }) |value| {
            std.mem.writeInt(u64, &number, value, .little);
            hash.update(&number);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};

pub fn prefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidSqlRelationPersistence;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sql_relation_boots:", .{group});
}
pub fn key(buffer: []u8, group: u64, node: u64) ![]const u8 {
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try prefix(&base, group), node });
}
pub fn readyPrefix(buffer: []u8, group: u64) ![]const u8 {
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}ready:", .{try prefix(&base, group)});
}
pub fn readyKey(buffer: []u8, group: u64, node: u64) ![]const u8 {
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try readyPrefix(&base, group), node });
}
pub fn read(a: A, reader: anytype, group: u64, node: u64) !?Record {
    var buffer: [192]u8 = undefined;
    const raw = reader.get(try key(&buffer, group, node)) catch |err| return if (err == error.NotFound) null else err;
    if (raw.len > 2048) return error.InvalidSqlRelationPersistence;
    var parsed = try std.json.parseFromSlice(Record, a, raw, .{});
    defer parsed.deinit();
    try parsed.value.validate();
    if (parsed.value.node_id != node) return error.InvalidSqlRelationPersistence;
    return parsed.value;
}
fn write(txn: anytype, group: u64, value: Record) !void {
    try value.validate();
    var encoded: [2048]u8 = undefined;
    var writer = std.Io.Writer.fixed(&encoded);
    try std.json.Stringify.value(value, .{}, &writer);
    var buffer: [192]u8 = undefined;
    try txn.put(try key(&buffer, group, value.node_id), writer.buffered());
    if (value.phase == .ready) {
        try txn.put(try readyKey(&buffer, group, value.node_id), &try value.fingerprint());
    } else txn.delete(try readyKey(&buffer, group, value.node_id)) catch |err| if (err != error.NotFound) return err;
}

/// Hidden generation IDs are hashes and can sort before an already verified
/// prefix. Rewind in the SAME table/job admission transaction so a new target
/// cannot be permanently skipped. The process generation itself never changes.
pub fn rewindForInsertion(txn: anytype, group: u64, node: u64, table: u64) !void {
    if (node == 0 or table == 0) return error.InvalidSqlRelationPersistence;
    var storage: [16 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    var before = (try read(fixed.allocator(), txn, group, node)) orelse return;
    if (before.phase != .recovering or before.recovery_after == null or table > before.recovery_after.?) return;
    before.recovery_after = null;
    try write(txn, group, before);
}

pub const ReadyPage = struct { records: []const Record, after: ?u64 };
pub fn listReady(a: A, reader: anytype, group: u64, after: ?u64, limit: u16) !ReadyPage {
    if (limit == 0 or limit > 128) return error.InvalidSqlRelationPersistence;
    var buffer: [192]u8 = undefined;
    const selected = try readyPrefix(&buffer, group);
    var seek_buffer: [192]u8 = undefined;
    const seek = if (after) |node| try readyKey(&seek_buffer, group, node) else selected;
    var cursor = try reader.openCursor();
    defer cursor.close();
    var records: std.ArrayList(Record) = .empty;
    errdefer records.deinit(a);
    var row = try cursor.seekAtOrAfter(seek);
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, selected)) break;
        if (item.key.len != selected.len + 16 or item.value.len != 32) return error.InvalidSqlRelationPersistence;
        const node = std.fmt.parseInt(u64, item.key[selected.len..], 16) catch return error.InvalidSqlRelationPersistence;
        if (after != null and node <= after.?) continue;
        if (records.items.len == limit) {
            const next = records.items[records.items.len - 1].node_id;
            return .{ .records = try records.toOwnedSlice(a), .after = next };
        }
        const expected = item.value[0..32].*;
        const record = (try read(a, reader, group, node)) orelse return error.InvalidSqlRelationPersistence;
        if (record.phase != .ready or !std.mem.eql(u8, &expected, &try record.fingerprint())) return error.InvalidSqlRelationPersistence;
        try records.append(a, record);
    }
    return .{ .records = try records.toOwnedSlice(a), .after = null };
}

/// Registration is independently rejoined to current metadata enrollment and
/// reporter, then bound to an actual compiled-context signed instance proof.
pub fn claim(a: A, txn: anytype, group: u64, request: Claim, registration: Registration) !Record {
    try registration.identity.validate();
    if (registration.identity.node_id != request.node_id or registration.reporter_incarnation == 0 or
        !std.mem.eql(u8, &request.instance, &registration.instance)) return error.SqlRelationOwnerChanged;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/sql-relation-boot/registered-claim/v1\x00");
    hash.update(&try request.digest());
    hash.update(&try registration.identity.digest());
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, registration.reporter_incarnation, .little);
    hash.update(&number);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    const previous = try read(a, txn, group, request.node_id);
    if (previous) |old| {
        if (std.mem.eql(u8, &old.instance, &request.instance)) {
            if (!std.mem.eql(u8, &old.claim_digest, &digest)) return error.SqlRelationOwnerChanged;
            return old;
        }
        if (old.generation != request.expected_generation or request.expected_instance == null or
            !std.mem.eql(u8, &old.instance, &request.expected_instance.?)) return error.SqlRelationOwnerChanged;
    } else if (request.expected_generation != 0 or request.expected_instance != null)
        return error.SqlRelationOwnerChanged;
    const unclean = if (previous) |old| old.phase != .clean or
        old.identity.root_incarnation != registration.identity.root_incarnation or
        !std.mem.eql(u8, &old.identity.public_key, &registration.identity.public_key) else false;
    const value: Record = .{
        .node_id = request.node_id,
        .identity = registration.identity,
        .reporter_incarnation = registration.reporter_incarnation,
        .instance = request.instance,
        .generation = std.math.add(u64, request.expected_generation, 1) catch return error.CatalogGenerationExhausted,
        .claim_digest = digest,
        .unclean_restart = unclean,
        // Even clean restarts must retire prior-process TEMP owners before
        // serving. The worker advances a bounded lifetime owner directory.
        .phase = .recovering,
    };
    try write(txn, group, value);
    return value;
}

pub fn requireCurrent(a: A, reader: anytype, group: u64, node: u64, instance: [16]u8, allow_recovery: bool) !Record {
    const value = (try read(a, reader, group, node)) orelse return error.SqlRelationOwnerChanged;
    if (!std.mem.eql(u8, &value.instance, &instance) or
        (value.phase != .ready and !(allow_recovery and value.phase == .recovering))) return error.SqlRelationOwnerChanged;
    return value;
}

/// The caller supplies a transaction-local verifier over actual native root
/// receipts. Advancing the cursor without verifying the whole emitted page
/// would permanently skip crash-left owners and is therefore never allowed.
pub fn advanceRecovery(a: A, txn: anytype, group: u64, expected: Record, after: ?u64, verifier: anytype) !Record {
    const current = (try read(a, txn, group, expected.node_id)) orelse return error.SqlRelationOwnerChanged;
    var next = expected;
    next.recovery_after = after;
    if (after == null) next.phase = .ready;
    // A lost reply may be retried after readiness publication. Exact durable
    // postimage recognition must precede the ordinary recovering-only CAS.
    if (std.meta.eql(current, next)) return current;
    if (!std.meta.eql(current, expected) or current.phase != .recovering or
        (after != null and current.recovery_after != null and after.? <= current.recovery_after.?)) return error.SqlRelationOwnerChanged;
    try verifier.requireRecoveryCut(txn, expected, after);
    try write(txn, group, next);
    return next;
}

pub fn beginDrain(a: A, txn: anytype, group: u64, node: u64, instance: [16]u8) !Record {
    var value = (try read(a, txn, group, node)) orelse return error.SqlRelationOwnerChanged;
    if (!std.mem.eql(u8, &value.instance, &instance) or (value.phase != .ready and value.phase != .draining)) return error.SqlRelationOwnerChanged;
    value.phase = .draining;
    try write(txn, group, value);
    return value;
}

/// A clean shutdown is a native flush/closed-owner proof plus a closed
/// admission record, never a side effect of DB.close's infallible destructor.
pub fn finishDrain(a: A, txn: anytype, group: u64, expected: Record, verifier: anytype) !Record {
    const current = (try read(a, txn, group, expected.node_id)) orelse return error.SqlRelationOwnerChanged;
    var next = expected;
    next.phase = .clean;
    if (std.meta.eql(current, next)) return current;
    if (!std.meta.eql(current, expected) or current.phase != .draining) return error.SqlRelationOwnerChanged;
    try verifier.requireCleanShutdown(txn, expected);
    try write(txn, group, next);
    return next;
}

test "SQL relation boot requires complete recovery and fallible clean shutdown proof" {
    const Store = struct {
        bytes: [2048]u8 = undefined,
        len: usize = 0,
        ready: [32]u8 = undefined,
        has_ready: bool = false,
        fn get(self: *@This(), key_bytes: []const u8) ![]const u8 {
            if (std.mem.indexOf(u8, key_bytes, ":ready:") != null) return if (self.has_ready) &self.ready else error.NotFound;
            if (self.len == 0) return error.NotFound;
            return self.bytes[0..self.len];
        }
        fn put(self: *@This(), key_bytes: []const u8, value: []const u8) !void {
            if (std.mem.indexOf(u8, key_bytes, ":ready:") != null) {
                if (value.len != 32) return error.ValueTooLarge;
                self.ready = value[0..32].*;
                self.has_ready = true;
                return;
            }
            if (value.len > self.bytes.len) return error.ValueTooLarge;
            @memcpy(self.bytes[0..value.len], value);
            self.len = value.len;
        }
        fn delete(self: *@This(), key_bytes: []const u8) !void {
            if (std.mem.indexOf(u8, key_bytes, ":ready:") != null) self.has_ready = false else self.len = 0;
        }
    };
    const Proof = struct {
        ready: bool,
        fn requireRecoveryCut(self: @This(), _: *Store, _: Record, _: ?u64) !void {
            if (!self.ready) return error.NativeRecoveryIncomplete;
        }
        fn requireCleanShutdown(self: @This(), _: *Store, _: Record) !void {
            if (!self.ready) return error.NativeFlushIncomplete;
        }
    };
    const a = std.testing.allocator;
    var store: Store = .{};
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    var registration: Registration = .{
        .identity = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 11, .root_incarnation = 19, .public_key = pair.public_key.toBytes() },
        .reporter_incarnation = 1,
        .instance = @splat(1),
    };
    const first: Claim = .{ .node_id = 9, .instance = @splat(1), .expected_generation = 0, .expected_instance = null };
    const initial = try claim(a, &store, 1, first, registration);
    try initial.requireRegistration(registration);
    var stale_registration = registration;
    stale_registration.reporter_incarnation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, initial.requireRegistration(stale_registration));
    try std.testing.expectEqual(Phase.recovering, initial.phase);
    try std.testing.expect(!initial.unclean_restart);
    try std.testing.expectEqualDeep(initial, try claim(a, &store, 1, first, registration));
    var partial = initial;
    partial.recovery_after = 100;
    try write(&store, 1, partial);
    try rewindForInsertion(&store, 1, first.node_id, 101);
    try std.testing.expectEqualDeep(partial, (try read(a, &store, 1, first.node_id)).?);
    try rewindForInsertion(&store, 1, first.node_id, 100);
    try std.testing.expectEqualDeep(initial, (try read(a, &store, 1, first.node_id)).?);
    try std.testing.expectError(error.SqlRelationOwnerChanged, advanceRecovery(a, &store, 1, partial, 200, Proof{ .ready = true }));
    try std.testing.expectError(error.SqlRelationOwnerChanged, requireCurrent(a, &store, 1, 9, first.instance, false));
    try std.testing.expectError(error.NativeRecoveryIncomplete, advanceRecovery(a, &store, 1, initial, null, Proof{ .ready = false }));
    try std.testing.expectEqualDeep(initial, (try read(a, &store, 1, 9)).?);
    const ready = try advanceRecovery(a, &store, 1, initial, null, Proof{ .ready = true });
    try std.testing.expectEqual(Phase.ready, ready.phase);
    try std.testing.expectEqualDeep(ready, try advanceRecovery(a, &store, 1, initial, null, Proof{ .ready = false }));
    const draining = try beginDrain(a, &store, 1, 9, first.instance);
    try std.testing.expectError(error.NativeFlushIncomplete, finishDrain(a, &store, 1, draining, Proof{ .ready = false }));
    try std.testing.expectEqualDeep(draining, (try read(a, &store, 1, 9)).?);
    const clean = try finishDrain(a, &store, 1, draining, Proof{ .ready = true });
    try std.testing.expectEqual(Phase.clean, clean.phase);
    try std.testing.expectEqualDeep(clean, try finishDrain(a, &store, 1, draining, Proof{ .ready = false }));
    const second: Claim = .{ .node_id = 9, .instance = @splat(2), .expected_generation = 1, .expected_instance = first.instance };
    registration.instance = second.instance;
    registration.reporter_incarnation += 1;
    const restart = try claim(a, &store, 1, second, registration);
    try std.testing.expect(!restart.unclean_restart);
    try std.testing.expectEqual(@as(u64, 2), restart.generation);
    try std.testing.expectError(error.SqlRelationOwnerChanged, requireCurrent(a, &store, 1, 9, first.instance, true));
    const third: Claim = .{ .node_id = 9, .instance = @splat(3), .expected_generation = 2, .expected_instance = second.instance };
    registration.instance = third.instance;
    registration.reporter_incarnation += 1;
    const crashed = try claim(a, &store, 1, third, registration);
    try std.testing.expect(crashed.unclean_restart);
    try std.testing.expectEqual(@as(u64, 3), crashed.generation);
}
