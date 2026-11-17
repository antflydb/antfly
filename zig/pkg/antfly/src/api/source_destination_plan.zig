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

//! Retained source plan handoff. A manifest is recovery input, not proof of
//! reservation, native seal, owner registration, commitment or source ACK.
const std = @import("std");
const local = @import("antfly_local_sources");
const plans = @import("completion_plan.zig");
const authority = @import("../metadata/source_checkpoint_authority.zig");
const delivery = @import("../metadata/source_delivery.zig");
const entry = @import("../data/source_plan_entry.zig");
const control = local.storage_source_plan_control;
const storage = local.storage_source_plan_storage;
const A = std.mem.Allocator;
const Digest = [32]u8;
pub const max_manifest_bytes = @import("../metadata/source_control.zig").max_pending_bytes;
fn sha(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
pub fn manifestDigest(bytes: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-destination-plan:v1");
    hash.update(bytes);
    return hash.finalResult();
}
pub const Ports = struct {
    controls: @import("source_plan_operations.zig").ControlPort,
    observations: @import("source_plan_operations.zig").ObservePort,
    chunks: @import("source_plan_operations.zig").ChunkPort,
};
pub const Manifest = struct {
    version: u8 = 1,
    checkpoint_authority: authority.Spec,
    owner: plans.completion.RoutedOwner,
    source_scope: local.storage_source_scope.Reference,
    checkpoint: local.storage_source_checkpoint.Command,
    transaction_id: [16]u8,
    admission_digest: Digest,
    begin_timestamp: u64,
    commit_version: u64,
    artifact: storage.Spec,

    pub fn validate(self: Manifest) !void {
        if (self.version != 1 or self.begin_timestamp == 0 or self.commit_version <= self.begin_timestamp or std.mem.allEqual(u8, &self.transaction_id, 0) or std.mem.allEqual(u8, &self.admission_digest, 0)) return error.InvalidSourceAdmission;
        try self.checkpoint_authority.validate();
        try self.checkpoint_authority.requireOwner(self.owner);
        try self.source_scope.validate();
        try self.checkpoint.validate();
        try self.artifact.validate(try entry.rootFor(self.owner));
        if (self.checkpoint_authority.retirement != null or self.checkpoint.receipt_window == 0 or
            !std.mem.eql(u8, &self.source_scope.binding, &self.checkpoint_authority.binding) or
            !std.mem.eql(u8, &self.source_scope.binding, &self.checkpoint.binding) or
            !std.mem.eql(u8, &self.source_scope.binding, &self.artifact.binding) or
            self.source_scope.sequence != self.checkpoint.sequence or self.source_scope.token != std.mem.readInt(u64, self.artifact.attempt[0..8], .big)) return error.SourceTransactionIdentityConflict;
    }
    /// Payload integrity and complete frozen admission are checked together.
    /// No current provider rows, schema or routing enter this reconstruction.
    pub fn restore(self: Manifest, a: A, bytes: []const u8) !plans.Plan {
        try self.validate();
        if (bytes.len != self.artifact.bytes or !std.mem.eql(u8, &sha(bytes), &self.artifact.checksum)) return error.SourcePayloadConflict;
        for (self.artifact.chunk_hashes, 0..) |expected, ordinal| {
            const start = ordinal * storage.chunk_bytes;
            if (!std.mem.eql(u8, &storage.chunkDigest(ordinal, bytes[start..@min(bytes.len, start + storage.chunk_bytes)]), &expected)) return error.SourcePayloadConflict;
        }
        var plan = try plans.Plan.restoreFrozen(a, bytes, self.artifact.plan_digest);
        errdefer plan.deinit();
        if (!std.meta.eql(plan.source_scope, @as(?local.storage_source_scope.Reference, self.source_scope)) or
            !std.mem.eql(u8, &plan.admission.digest(), &self.admission_digest) or
            !std.mem.eql(u8, &plan.admission.transaction_id, &self.transaction_id) or plan.begin_timestamp != self.begin_timestamp) return error.SourceTransactionIdentityConflict;
        const owner = try plan.admission.recoveryOwner(plan.admission.coordinator_namespace);
        if (!std.meta.eql(owner, self.owner)) return error.SourceTransactionIdentityConflict;
        const member = try plan.admission.member(owner.namespace);
        const locator = local.api_local_transaction_contract.parseParticipantRef(member.locator) orelse return error.InvalidParticipant;
        if (!std.mem.eql(u8, locator.table_name, self.checkpoint_authority.table_name)) return error.SourceBindingConflict;
        for (plan.participants, 0..) |participant, index| if (std.mem.eql(u8, &participant.owner.namespace, &owner.namespace)) {
            var input = try plan.prepare(a, index);
            defer input.deinit(a);
            const checkpoint = input.req.transaction.?.prepare.source_checkpoint orelse return error.SourceTransactionIdentityConflict;
            if (!std.mem.eql(u8, &checkpoint.digest(), &self.checkpoint.digest())) return error.SourceTransactionIdentityConflict;
        };
        return plan;
    }
    pub fn transfer(self: Manifest, fence: @import("../metadata/api.zig").CatalogRouteFence, ports: Ports) !@import("source_plan_delivery.zig").Transfer {
        try self.validate();
        try @import("source_owner_scope.zig").requireFence(self.owner, fence);
        return .{ .owner = self.owner, .fence = fence, .table = self.checkpoint_authority.table_name, .spec = self.artifact, .controls = ports.controls, .observations = ports.observations, .chunks = ports.chunks };
    }
    /// Metadata's exact sealed ledger grant authorizes loading retained input.
    /// Native seal evidence and the manifest independently bind the same bytes.
    pub fn load(self: Manifest, a: A, record: @import("../metadata/source_artifact_control.zig").Record, fence: @import("../metadata/api.zig").CatalogRouteFence, ports: Ports, context: local.api_operation.RequestContext) !plans.Plan {
        const target = try self.transfer(fence, ports);
        const bytes = try target.load(a, record, context);
        defer a.free(bytes);
        return self.restore(a, bytes);
    }
    /// Returned metadata input borrows this manifest and its encoded bytes.
    pub fn deliveryPlan(self: Manifest, a: A, encoded: []const u8) !delivery.Plan {
        try self.validate();
        if (encoded.len == 0 or encoded.len > max_manifest_bytes) return error.InvalidSourceAdmission;
        const canonical = try std.json.Stringify.valueAlloc(a, self, .{});
        defer a.free(canonical);
        if (!std.mem.eql(u8, canonical, encoded)) return error.SourcePayloadConflict;
        return .{ .digest = manifestDigest(encoded), .transaction_id = self.transaction_id, .manifest = encoded, .source_scope = self.source_scope, .checkpoint_authority = self.checkpoint_authority, .artifact = .{ .namespace = self.artifact.namespace, .binding = self.artifact.binding, .domain = self.artifact.domain, .attempt = self.artifact.attempt, .bytes = self.artifact.bytes, .checksum = self.artifact.checksum, .plan_digest = self.artifact.plan_digest } };
    }
};
pub fn parse(a: A, encoded: []const u8, expected: Digest) !std.json.Parsed(Manifest) {
    if (encoded.len == 0 or encoded.len > max_manifest_bytes or !std.mem.eql(u8, &manifestDigest(encoded), &expected)) return error.SourcePayloadConflict;
    var result = std.json.parseFromSlice(Manifest, a, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    errdefer result.deinit();
    try result.value.validate();
    return result;
}
pub const Frozen = struct {
    arena: std.heap.ArenaAllocator,
    manifest: Manifest,
    encoded_manifest: []const u8,
    bytes: []const u8,
    pub fn deinit(self: *Frozen) void {
        self.arena.deinit();
        self.* = undefined;
    }
    /// The observation must be qualified for this exact captured coordinator.
    /// Capacity remains provisional until native artifact BEGIN commits.
    pub fn init(a: A, plan: *const plans.Plan, checkpoint_authority: authority.Spec, quota: control.Observation, commit_version: u64) !Frozen {
        const owner = try plan.admission.recoveryOwner(plan.admission.coordinator_namespace);
        try checkpoint_authority.requireOwner(owner);
        try quota.validate(.{ .root = try entry.rootFor(owner), .minimum_applied_index = 1 });
        if (quota.quota.active == storage.max_active) return error.SourceAdmissionBackpressure;
        const scope = plan.source_scope orelse return error.UnsupportedOperation;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const bytes = try plan.encodeFrozen(owned);
        const digest = try plans.frozenDigest(bytes);
        const chunks = try owned.alloc(Digest, (bytes.len + storage.chunk_bytes - 1) / storage.chunk_bytes);
        for (chunks, 0..) |*chunk, ordinal| {
            const start = ordinal * storage.chunk_bytes;
            chunk.* = storage.chunkDigest(ordinal, bytes[start..@min(bytes.len, start + storage.chunk_bytes)]);
        }
        var attempt: [16]u8 = undefined;
        std.mem.writeInt(u64, attempt[0..8], scope.token, .big);
        @memcpy(attempt[8..16], digest[0..8]);
        if (std.mem.allEqual(u8, attempt[8..16], 0)) attempt[15] = 1;
        var checkpoint: ?local.storage_source_checkpoint.Command = null;
        for (plan.participants, 0..) |participant, index| if (std.mem.eql(u8, &participant.owner.namespace, &owner.namespace)) {
            var input = try plan.prepare(owned, index);
            defer input.deinit(owned);
            checkpoint = input.req.transaction.?.prepare.source_checkpoint;
        };
        const spec: storage.Spec = .{ .namespace = storage.namespaceFor(try entry.rootFor(owner), quota.quota.epoch), .storage_epoch = quota.quota.epoch, .binding = scope.binding, .domain = storage.domainFor(scope.binding), .attempt = attempt, .bytes = bytes.len, .checksum = sha(bytes), .plan_digest = digest, .chunk_hashes = chunks };
        if (spec.reservationBytes() > storage.max_reserved_bytes - quota.quota.reserved) return error.SourceAdmissionBackpressure;
        const manifest: Manifest = .{ .checkpoint_authority = checkpoint_authority, .owner = owner, .source_scope = scope, .checkpoint = checkpoint orelse return error.InvalidSourceAdmission, .transaction_id = plan.admission.transaction_id, .admission_digest = plan.admission.digest(), .begin_timestamp = plan.begin_timestamp, .commit_version = commit_version, .artifact = spec };
        try manifest.validate();
        const encoded = try std.json.Stringify.valueAlloc(owned, manifest, .{});
        if (encoded.len > max_manifest_bytes) return error.HistoryTooLarge;
        const retained = try std.json.parseFromSliceLeaky(Manifest, owned, encoded, .{ .allocate = .alloc_always });
        return .{ .arena = arena, .manifest = retained, .encoded_manifest = encoded, .bytes = bytes };
    }
};

test "completion frozen plan source destination manifest owns exact admission and artifact identity" {
    const a = std.testing.allocator;
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 9, .big);
    std.mem.writeInt(u64, namespace[16..24], 9, .big);
    const owner: plans.completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = namespace, .table_binding = @splat(2), .root_generation = 0, .group_id = 9, .topology_epoch = 1 };
    const scope: local.storage_source_scope.Reference = .{ .domain = @splat(3), .binding = @splat(4), .generation = 1, .token = 7, .sequence = 1 };
    const checkpoint: local.storage_source_checkpoint.Command = .{ .binding = scope.binding, .purpose = @splat(5), .sequence = 1, .checkpoint = "start", .accepted_at_ns = 11, .receipt_window = 3 };
    const location: authority.Spec = .{ .binding = scope.binding, .table_name = "docs", .owner = owner };
    var position: [32]u8 = @splat(0);
    std.mem.writeInt(u64, position[0..8], 1, .little);
    std.mem.writeInt(u64, position[8..16], 8, .little);
    position[31] = 1;
    const observation: control.Observation = .{ .root = try entry.rootFor(owner), .cut = position, .quota = .{ .epoch = 2 } };
    const value = try a.alloc(u8, storage.chunk_bytes + 2);
    defer a.free(value);
    @memset(value, 'a');
    value[0] = '"';
    value[value.len - 1] = '"';
    var frozen = blk: {
        const request = try (local.storage_db_selected_root.db.types.TransactionIntentRequest{ .completion_owner = owner, .completion_source_scope = scope, .completion_source_purpose = checkpoint.purpose, .completion_source_checkpoint = checkpoint, .writes = &.{.{ .key = "row", .value = value }} }).completionBatch(@splat(6), 10, owner.topology_epoch);
        var plan = try plans.Plan.freezeDerived(a, @splat(6), owner, &.{.{ .owner = owner, .locator = "table2:00000004:docs:9", .roles = plans.completion.Roles.checkpoint | plans.completion.Roles.effects, .prepare = request }}, 11);
        defer plan.deinit();
        var wrong_quota = observation;
        wrong_quota.root.generation += 1;
        try std.testing.expectError(error.InvalidSourcePlanWire, Frozen.init(a, &plan, location, wrong_quota, 20));
        var full = observation;
        full.quota.active = storage.max_active;
        full.quota.reserved = storage.max_reserved_bytes;
        try std.testing.expectError(error.SourceAdmissionBackpressure, Frozen.init(a, &plan, location, full, 20));
        try std.testing.expectError(error.InvalidSourceAdmission, Frozen.init(a, &plan, location, observation, 10));
        break :blk try Frozen.init(a, &plan, location, observation, 20);
    };
    defer frozen.deinit();
    try std.testing.expect(frozen.manifest.artifact.chunk_hashes.len > 1);
    var parsed = try parse(a, frozen.encoded_manifest, manifestDigest(frozen.encoded_manifest));
    defer parsed.deinit();
    var restored = try parsed.value.restore(a, frozen.bytes);
    defer restored.deinit();
    try std.testing.expectEqualDeep(scope, restored.source_scope.?);
    try std.testing.expectEqualDeep(frozen.manifest.admission_digest, restored.admission.digest());
    try std.testing.expectEqual(@as(u64, 2), parsed.value.artifact.storage_epoch);
    try std.testing.expectEqual(@as(u64, 7), std.mem.readInt(u64, parsed.value.artifact.attempt[0..8], .big));
    const delivery_plan = try parsed.value.deliveryPlan(a, frozen.encoded_manifest);
    try std.testing.expectEqualDeep(scope, delivery_plan.source_scope.?);
    try std.testing.expectEqualDeep(location.owner, delivery_plan.checkpoint_authority.?.owner);
    try std.testing.expectEqualDeep(frozen.manifest.artifact.plan_digest, delivery_plan.artifact.?.plan_digest);
    const Native = struct {
        txn: storage.test_support.Txn = .{},
        index: u64 = 8,
        mutations: usize = 0,
        chunk_reads: usize = 0,
        lose_seal: bool = true,
        fn cut(self: *@This()) [32]u8 {
            var result: [32]u8 = @splat(0);
            std.mem.writeInt(u64, result[0..8], 1, .little);
            std.mem.writeInt(u64, result[8..16], self.index, .little);
            result[31] = 1;
            return result;
        }
        fn observe(ptr: *anyopaque, alloc: A, _: @import("../metadata/api.zig").CatalogRouteFence, _: []const u8, request: @import("source_plan_operations.zig").wire.ObserveRequest, _: local.api_operation.RequestContext) !control.Observation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .root = request.query.root, .cut = self.cut(), .quota = try storage.readQuota(&self.txn, request.query.root), .status = if (request.query.spec) |spec| try storage.inspect(alloc, &self.txn, request.query.root, spec) else null };
        }
        fn execute(ptr: *anyopaque, alloc: A, _: @import("../metadata/api.zig").CatalogRouteFence, _: []const u8, request: @import("source_plan_operations.zig").wire.ControlRequest, _: local.api_operation.RequestContext) !@import("source_plan_operations.zig").wire.ControlReply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.mutations += 1;
            self.index += 1;
            var decoded = try @import("source_plan_operations.zig").wire.native.fromCommand(alloc, request.command);
            defer decoded.deinit();
            const result = try control.apply(alloc, &self.txn, decoded.request, self.cut());
            var reply: @import("source_plan_operations.zig").wire.ControlReply = .{ .request_digest = try request.digest(alloc), .receipt = .{ .term = 1, .index = self.index }, .outcome = .{ .applied = result } };
            if (request.command.action == .seal and self.lose_seal) {
                self.lose_seal = false;
                reply.outcome = null;
            }
            return reply;
        }
        fn chunk(ptr: *anyopaque, alloc: A, fence: @import("../metadata/api.zig").CatalogRouteFence, table: []const u8, request: @import("source_plan_operations.zig").wire.ChunkRequest, context: local.api_operation.RequestContext) !control.Read {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.chunk_reads += 1;
            return .{ .observation = try observe(ptr, alloc, fence, table, .{ .version = 1, .source_owner = request.source_owner, .query = request.request.query }, context), .bytes = try storage.readSealedChunkAlloc(alloc, &self.txn, request.request.query.root, request.request.query.spec.?, request.request.seal, request.request.ordinal) };
        }
    };
    var native: Native = .{};
    defer native.txn.deinit();
    // An earlier fenced upload advanced the physical artifact epoch to two.
    var earlier = parsed.value.artifact;
    earlier.storage_epoch = 1;
    earlier.namespace = storage.namespaceFor(observation.root, 1);
    _ = try storage.stageFence(a, &native.txn, observation.root, earlier, native.cut());
    const ports: Ports = .{ .controls = .{ .ptr = &native, .execute_fn = Native.execute }, .observations = .{ .ptr = &native, .read_fn = Native.observe }, .chunks = .{ .ptr = &native, .read_fn = Native.chunk } };
    const fence: @import("../metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 9, .range_id = 9, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 9 } } };
    const transfer = try parsed.value.transfer(fence, ports);
    var record: @import("../metadata/source_artifact_control.zig").Record = .{ .reservation = @splat(10), .spec = delivery_plan.artifact.? };
    try std.testing.expectError(error.SourceOutcomeUnresolved, transfer.seal(a, record, frozen.bytes, .{}));
    const mutations_after_seal = native.mutations;
    const seal = (try transfer.recoverSeal(a, record, .{})).?;
    try std.testing.expectEqual(mutations_after_seal, native.mutations);
    try std.testing.expectError(error.SourceControlConflict, parsed.value.load(a, record, fence, ports, .{}));
    try std.testing.expectEqual(@as(usize, 0), native.chunk_reads);
    record.phase = .sealed;
    record.stamp = seal;
    var loaded = try parsed.value.load(a, record, fence, ports, .{});
    defer loaded.deinit();
    try std.testing.expectEqualDeep(scope, loaded.source_scope.?);
    try std.testing.expectEqualDeep(restored.admission.digest(), loaded.admission.digest());
    try std.testing.expectEqual(parsed.value.artifact.chunk_hashes.len, native.chunk_reads);
    try std.testing.expectEqual(mutations_after_seal, native.mutations);
    var wrong_fence = fence;
    wrong_fence.route.group_id += 1;
    try std.testing.expectError(error.InvalidCatalogRouteFence, parsed.value.load(a, record, wrong_fence, ports, .{}));
    try std.testing.expectEqual(parsed.value.artifact.chunk_hashes.len, native.chunk_reads);
    var changed = parsed.value;
    changed.commit_version += 1;
    try std.testing.expectError(error.SourcePayloadConflict, changed.deliveryPlan(a, frozen.encoded_manifest));
    changed = parsed.value;
    changed.source_scope.token += 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, changed.validate());
    changed = parsed.value;
    changed.transaction_id[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, changed.restore(a, frozen.bytes));
    changed = parsed.value;
    changed.checkpoint.accepted_at_ns += 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, changed.restore(a, frozen.bytes));
    const damaged = try a.dupe(u8, frozen.bytes);
    defer a.free(damaged);
    damaged[damaged.len - 1] ^= 1;
    try std.testing.expectError(error.SourcePayloadConflict, parsed.value.restore(a, damaged));
    var wrong_digest = manifestDigest(frozen.encoded_manifest);
    wrong_digest[0] ^= 1;
    try std.testing.expectError(error.SourcePayloadConflict, parse(a, frozen.encoded_manifest, wrong_digest));
    const unknown_field = try std.fmt.allocPrint(a, "{{\"unexpected\":1,{s}", .{frozen.encoded_manifest[1..]});
    defer a.free(unknown_field);
    try std.testing.expectError(error.InvalidSourceAdmission, parse(a, unknown_field, manifestDigest(unknown_field)));
}
