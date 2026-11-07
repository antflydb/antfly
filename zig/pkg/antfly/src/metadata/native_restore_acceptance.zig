// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Image-bound completion of native empty-target acceptance installation.
//! This proves only a committed target effect, never a source retirement or
//! transaction decision. The metadata writer independently rejoins the current
//! target attempt, initialized image, Boot and root enrollment before admission.
const std = @import("std");
const local = @import("antfly_local_sources");
const lifetime = local.common_topology_records.RelationPersistence;
const native = local.storage_db_native_empty_generation;
const identity = local.storage_db_doc_identity_namespace;
const Digest = [32]u8;
pub const Proof = @import("store_root_attestation.zig").ProofFor("antfly/native-restore-acceptance/signature/v1", "antfly/native-restore-acceptance/accepted/v1");
pub const Attempt = struct {
    number: u64,
    digest: Digest,
    pub fn validate(self: Attempt) !void {
        if (self.number == 0 or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidRestoreStaging;
    }
};
pub const Acceptance = struct {
    metadata_group_id: u64,
    group_id: u64,
    range_id: u64,
    plan_digest: Digest,
    attempt: Attempt,
    initialized_digest: Digest,
    descriptor_fingerprint: Digest,
    boot_generation: u64,
    native_instance: [16]u8,
    installed: native.Receipt,
    proof: Proof,

    fn validateFacts(self: Acceptance) !void {
        try self.attempt.validate();
        _ = try self.installed.encode();
        const pin = self.proof.native_pin orelse return error.InvalidRestoreStaging;
        try pin.validate();
        if (self.metadata_group_id == 0 or self.group_id == 0 or self.range_id == 0 or self.boot_generation == 0 or
            std.mem.allEqual(u8, &self.native_instance, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.initialized_digest, 0) or std.mem.allEqual(u8, &self.descriptor_fingerprint, 0) or
            self.proof.authority != .native or self.range_id != pin.range_id or
            !self.installed.position.namespace.eql(.{ .table_id = pin.table_id, .shard_id = pin.shard_id, .range_id = pin.range_id })) return error.InvalidRestoreStaging;
    }
    pub fn digest(self: Acceptance) !Digest {
        try self.validateFacts();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/native-restore-acceptance/logical/v1\x00");
        var number: [8]u8 = undefined;
        for ([_]u64{ self.metadata_group_id, self.group_id, self.range_id, self.attempt.number, self.boot_generation }) |value| {
            std.mem.writeInt(u64, &number, value, .little);
            hash.update(&number);
        }
        hash.update(&self.plan_digest);
        hash.update(&self.attempt.digest);
        hash.update(&self.initialized_digest);
        hash.update(&self.descriptor_fingerprint);
        hash.update(&self.native_instance);
        hash.update(&try self.installed.encode());
        hash.update(&try self.proof.native_pin.?.encode());
        var out: Digest = undefined;
        hash.final(&out);
        return out;
    }
    pub fn verify(self: Acceptance) !void {
        try self.proof.verify(try self.digest());
    }
    pub fn requireLogical(self: Acceptance, group: u64, plan: Digest, scope: Digest, install: native.Install, descriptor: lifetime.Descriptor, namespace: identity.Namespace) !void {
        try self.verify();
        if (descriptor.kind != .unlogged or group != self.group_id or self.range_id != namespace.range_id or
            !std.mem.eql(u8, &plan, &self.plan_digest) or !std.mem.eql(u8, &scope, &self.installed.scope) or
            !std.mem.eql(u8, &install.scope, &scope) or self.installed.origin_kind != install.origin.kind() or !std.mem.eql(u8, &try install.origin.digest(), &self.installed.origin_digest) or
            !std.mem.eql(u8, &try install.digest(), &self.installed.install_digest) or
            !std.mem.eql(u8, &try descriptor.fingerprint(), &self.descriptor_fingerprint)) return error.InvalidRestoreStaging;
        const pin = self.proof.native_pin.?;
        if (pin.data_generation != descriptor.data_generation or pin.table_id != namespace.table_id or pin.shard_id != namespace.shard_id or pin.range_id != namespace.range_id or
            self.proof.identity.node_id != descriptor.owner_node_id or self.proof.identity.store_id != descriptor.owner_store_id) return error.InvalidRestoreStaging;
    }
    /// Historical accepted evidence survives clean process adoption, but never
    /// an attempt/root replacement. The current PID is checked separately at
    /// admission; it is not a permanent restriction on an accepted receipt.
    pub fn requireImage(self: Acceptance, initialized: lifetime.NativeBinding, attempt: Attempt) !void {
        try self.verify();
        try initialized.validate();
        try attempt.validate();
        if (!std.meta.eql(self.attempt, attempt) or !self.proof.native_pin.?.eql(try lifetime.PhysicalPin.fromBinding(initialized)) or
            self.proof.identity.node_id != initialized.descriptor.owner_node_id or self.proof.identity.store_id != initialized.descriptor.owner_store_id or
            !std.mem.eql(u8, &self.descriptor_fingerprint, &try initialized.descriptor.fingerprint())) return error.InvalidRestoreStaging;
    }
    /// Accepting-writer check after current initialized/Boot enrollment has
    /// been authenticated in the same metadata transaction. Stored historical
    /// signatures remain verifiable, but cannot acknowledge a replacement image.
    pub fn requireInitialized(self: Acceptance, initialized: lifetime.NativeBinding, generation: u64, instance: [16]u8, attempt: Attempt) !void {
        try self.requireImage(initialized, attempt);
        if (initialized.has_user_data or initialized.has_transaction_state or
            !std.mem.eql(u8, &self.initialized_digest, &try initialized.digest()) or
            !std.mem.eql(u8, &self.descriptor_fingerprint, &try initialized.descriptor.fingerprint()) or
            self.boot_generation != generation or !std.mem.eql(u8, &self.native_instance, &instance) or
            !std.mem.eql(u8, &initialized.native_instance, &instance)) return error.InvalidRestoreStaging;
    }
};

test "SQL relation lifetime native restore acceptance binds actual image position attempt and current initialized Boot" {
    const a = std.testing.allocator;
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .root_incarnation = 11, .seed = seed, .public_key = pair.public_key.toBytes() };
    const descriptor: lifetime.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 3 };
    const initialized: lifetime.NativeBinding = .{
        .descriptor = descriptor,
        .native_instance = @splat(1),
        .table_id = 7,
        .shard_id = 17,
        .range_id = 27,
        .physical_root_incarnation = std.math.maxInt(u128) - 9,
        .schema_version = 1,
        .schema_digest = @splat(4),
        .catalog_generation = 1,
        .catalog_fingerprint = @splat(5),
    };
    const pin = try lifetime.PhysicalPin.fromBinding(initialized);
    const enrolled: @import("store_root_enrollment.zig").Identity = .{ .metadata_incarnation = @splat('a'), .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key };
    const install: native.Install = .{ .scope = @splat(2), .origin = .{ .lost_source = .{ .checkpoint_digest = @splat(3), .source = .{ .physical_root_incarnation = 55, .table_id = 6, .shard_id = 16, .range_id = 26, .data_generation = 2 } } }, .transitions = &.{} };
    var receipt: Acceptance = .{
        .metadata_group_id = 1,
        .group_id = 17,
        .range_id = 27,
        .plan_digest = @splat(6),
        .attempt = .{ .number = 1, .digest = @splat(8) },
        .initialized_digest = try initialized.digest(),
        .descriptor_fingerprint = try descriptor.fingerprint(),
        .boot_generation = 5,
        .native_instance = initialized.native_instance,
        .installed = .{ .scope = install.scope, .origin_kind = install.origin.kind(), .origin_digest = try install.origin.digest(), .install_digest = try install.digest(), .position = .{ .namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 27 }, .sequence = 2 } },
        .proof = .{ .identity = enrolled, .reporter_incarnation = 3, .authority = .{ .native = try @import("sql_relation_attestation.zig").nativeAuthorityDigest(enrolled, pin) }, .native_pin = pin, .root_generation = 1, .signature = @splat(0) },
    };
    receipt.proof = try receipt.proof.sign(root, try receipt.digest());
    try receipt.requireLogical(17, receipt.plan_digest, install.scope, install, descriptor, receipt.installed.position.namespace);
    try receipt.requireInitialized(initialized, 5, initialized.native_instance, receipt.attempt);
    const schema_install: native.Install = .{ .scope = install.scope, .origin = .{ .schema_only_snapshot = .{ .manifest_digest = @splat(41), .schema_digest = @splat(42), .source_table_id = 6 } }, .transitions = &.{} };
    var schema_receipt = receipt;
    schema_receipt.installed.origin_kind = schema_install.origin.kind();
    schema_receipt.installed.origin_digest = try schema_install.origin.digest();
    schema_receipt.installed.install_digest = try schema_install.digest();
    schema_receipt.proof = try schema_receipt.proof.sign(root, try schema_receipt.digest());
    try schema_receipt.requireLogical(17, schema_receipt.plan_digest, schema_install.scope, schema_install, descriptor, schema_receipt.installed.position.namespace);
    try std.testing.expectError(error.InvalidRestoreStaging, schema_receipt.requireLogical(17, schema_receipt.plan_digest, install.scope, install, descriptor, schema_receipt.installed.position.namespace));
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireLogical(17, receipt.plan_digest, schema_install.scope, schema_install, descriptor, receipt.installed.position.namespace));
    const bytes = try std.json.Stringify.valueAlloc(a, receipt, .{});
    defer a.free(bytes);
    var dynamic = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer dynamic.deinit();
    const through_value = try std.json.Stringify.valueAlloc(a, dynamic.value, .{});
    defer a.free(through_value);
    var decoded = try std.json.parseFromSlice(Acceptance, a, through_value, .{});
    defer decoded.deinit();
    try decoded.value.verify();
    try std.testing.expectEqualDeep(receipt, decoded.value);
    const owner: @import("restore_staging.zig").OwnerReceipt = .{ .group_id = receipt.group_id, .range_id = receipt.range_id, .plan_digest = receipt.plan_digest, .completion_digest = try receipt.digest(), .native_empty_acceptance = receipt };
    const durable = try owner.encodeDurable(a);
    defer a.free(durable);
    try std.testing.expectEqualDeep(owner, try @import("restore_staging.zig").OwnerReceipt.decodeNative(a, durable));
    var ordinary = owner;
    ordinary.native_empty_acceptance = null;
    const old_bytes = try ordinary.encodeDurable(a);
    defer a.free(old_bytes);
    try std.testing.expectEqualSlices(u8, &ordinary.completion_digest, old_bytes);
    // Preserve the actual historical OwnerReceipt serializer: integrity
    // identities have always used arrays, unlike std.json's UTF-8 heuristic.
    const LegacyOwnerReceipt = struct {
        pub fn nativeJsonSkipField(self: @This(), comptime name: []const u8) bool {
            return std.mem.eql(u8, name, "population") and self.population == null;
        }
        pub fn jsonStringify(self: @This(), writer: anytype) @TypeOf(writer.*).Error!void {
            try @import("antfly_local_sources").storage_db_relational_integrity_json.write(self, writer);
        }
        population: ?@import("antfly_local_sources").storage_db_relational_population_contract.Cursor = null,
        group_id: u64,
        range_id: u64,
        plan_digest: [32]u8,
        completion_digest: [32]u8,
    };
    const old_wire = try std.json.Stringify.valueAlloc(a, LegacyOwnerReceipt{ .group_id = ordinary.group_id, .range_id = ordinary.range_id, .plan_digest = ordinary.plan_digest, .completion_digest = ordinary.completion_digest }, .{});
    defer a.free(old_wire);
    const current_wire = try std.json.Stringify.valueAlloc(a, ordinary, .{});
    defer a.free(current_wire);
    try std.testing.expectEqualSlices(u8, old_wire, current_wire);
    try std.testing.expectError(error.InvalidRestoreStaging, @import("restore_staging.zig").OwnerReceipt.decodeNative(a, current_wire));
    var adopted = initialized;
    adopted.native_instance = @splat(98);
    try receipt.requireImage(adopted, receipt.attempt);
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireInitialized(adopted, 6, adopted.native_instance, receipt.attempt));
    var reattested = receipt;
    reattested.boot_generation = 6;
    reattested.native_instance = adopted.native_instance;
    reattested.initialized_digest = try adopted.digest();
    reattested.proof = try reattested.proof.sign(root, try reattested.digest());
    try reattested.requireInitialized(adopted, 6, adopted.native_instance, reattested.attempt);
    try std.testing.expectEqualDeep(receipt.installed, reattested.installed);
    var dirty = initialized;
    dirty.has_user_data = true;
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireInitialized(dirty, 5, initialized.native_instance, receipt.attempt));
    dirty = initialized;
    dirty.has_transaction_state = true;
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireInitialized(dirty, 5, initialized.native_instance, receipt.attempt));
    var other_image = initialized;
    other_image.physical_root_incarnation -= 1;
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireInitialized(other_image, 5, initialized.native_instance, receipt.attempt));
    var other_attempt = receipt.attempt;
    other_attempt.number += 1;
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireInitialized(initialized, 5, initialized.native_instance, other_attempt));
    other_attempt = receipt.attempt;
    other_attempt.digest[0] ^= 1;
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireInitialized(initialized, 5, initialized.native_instance, other_attempt));
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireInitialized(initialized, 6, initialized.native_instance, receipt.attempt));
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireInitialized(initialized, 5, @splat(99), receipt.attempt));
    var altered = receipt;
    altered.installed.position.sequence += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, altered.verify());
    altered = receipt;
    altered.proof.signature[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, altered.verify());
    altered = receipt;
    altered.installed.origin_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, altered.verify());
    altered = receipt;
    altered.proof.native_pin.?.physical_root_incarnation -= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, altered.verify());
    var wrong_descriptor = descriptor;
    wrong_descriptor.data_generation += 1;
    try std.testing.expectError(error.InvalidRestoreStaging, receipt.requireLogical(17, receipt.plan_digest, install.scope, install, wrong_descriptor, receipt.installed.position.namespace));
    // Equal logical installation bytes on another physical target are not an
    // equal accepted completion. A new receipt still requires a real effect.
    altered = receipt;
    const other_pin = try lifetime.PhysicalPin.fromBinding(other_image);
    altered.proof.native_pin = other_pin;
    altered.proof.authority = .{ .native = try @import("sql_relation_attestation.zig").nativeAuthorityDigest(enrolled, other_pin) };
    altered.initialized_digest = try other_image.digest();
    altered.proof = try altered.proof.sign(root, try altered.digest());
    try altered.requireInitialized(other_image, 5, initialized.native_instance, altered.attempt);
    try std.testing.expect(!std.mem.eql(u8, &try receipt.digest(), &try altered.digest()));
}
