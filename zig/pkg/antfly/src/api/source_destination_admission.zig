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

//! Source destination admission from a metadata-owned retained grant. Accepted
//! checkpoint evidence is not an all-owner ACK, retention release or index barrier.
const std = @import("std");
const local = @import("antfly_local_sources");
const source = @import("../metadata/source_control.zig");
const artifact = @import("../metadata/source_artifact_control.zig");
const destination = @import("source_destination_plan.zig");
const checkpoints = @import("source_checkpoint_operations.zig");
const metadata = @import("../metadata/api.zig");
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;

pub const Grant = struct {
    manifest: std.json.Parsed(destination.Manifest),
    record: artifact.Record,
    pub fn deinit(self: *Grant) void {
        self.manifest.deinit();
        self.* = undefined;
    }
    /// The actor supplies one applied metadata snapshot's pending reservation
    /// and ledger record. Neither a manifest alone nor native absence grants it.
    pub fn init(a: A, scope: source.Scope, pending: source.Reservation, record: artifact.Record) !Grant {
        try scope.validate();
        try pending.validate();
        try (artifact.State{ .next_token = record.spec.token(), .current = record }).validate();
        if (record.phase == .garbage) return error.SourceReceiptExpired;
        if (scope.destination != .native or pending.destination != .native or pending.artifact == null or !std.meta.eql(pending.artifact.?, record.spec) or !std.mem.eql(u8, &pending.identity(scope), &record.reservation)) return error.SourceTransactionIdentityConflict;
        var manifest = try destination.parse(a, pending.delivery_manifest, pending.plan_digest);
        errdefer manifest.deinit();
        const retained = record.recovery orelse return error.SourcePendingWork;
        if (!pending.retain_manifest or !std.mem.eql(u8, &retained.digest, &pending.plan_digest) or !std.mem.eql(u8, retained.bytes, pending.delivery_manifest)) return error.SourceTransactionIdentityConflict;
        const value = manifest.value;
        const plan = try value.deliveryPlan(a, pending.delivery_manifest);
        const purpose = try pending.inputFingerprint(scope);
        const checkpoint: local.storage_source_checkpoint.Command = .{ .binding = scope.identity(), .purpose = purpose, .sequence = pending.sequence, .checkpoint = pending.checkpoint, .expected_checkpoint = pending.expected_checkpoint, .accepted_at_ns = pending.accepted_at_ns, .receipt_window = value.checkpoint.receipt_window };
        const binding: local.storage_source_scope.Spec = .{ .actor_table_id = scope.table_id, .source_id = scope.source_id, .generation = scope.generation, .binding = scope.identity(), .checkpoint_owner = try local.storage_source_scope.Basis.fromOwner(value.checkpoint_authority.owner) };
        try binding.validate();
        if (!std.meta.eql(plan.artifact.?, record.spec) or !std.mem.eql(u8, &pending.transaction_id, &value.transaction_id) or
            !std.mem.eql(u8, &binding.domain(), &value.source_scope.domain) or binding.generation != value.source_scope.generation or
            !std.mem.eql(u8, &checkpoint.digest(), &value.checkpoint.digest())) return error.SourceTransactionIdentityConflict;
        var owned_record = record;
        owned_record.recovery.?.bytes = try manifest.arena.allocator().dupe(u8, retained.bytes);
        return .{ .manifest = manifest, .record = owned_record };
    }
};
/// Fixed owned native evidence. It proves acceptance at the original checkpoint
/// root; the caller retains separate completion and reclamation obligations.
pub const Accepted = struct { reservation_digest: [32]u8, checkpoint: checkpoints.wire.Reply };
pub const Admission = struct {
    coordinator: @import("completion_admission.zig").Coordinator,
    artifacts: destination.Ports,
    checkpoints: checkpoints.Port,

    pub fn resolve(self: Admission, a: A, grant: *const Grant, fence: metadata.CatalogRouteFence, original: Context) !?Accepted {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        const manifest = grant.manifest.value;
        _ = try manifest.transfer(fence, self.artifacts);
        const query: checkpoints.wire.Request = .{ .version = 1, .source_owner = manifest.owner, .transaction_id = manifest.transaction_id, .command = manifest.checkpoint, .minimum_applied_index = 1 };
        const observed = try self.checkpoints.read(a, fence, manifest.checkpoint_authority.table_name, query, context);
        try observed.validate(query);
        try context.ensureActive();
        return switch (observed.disposition) {
            .absent => null,
            .expired => error.SourceReceiptExpired,
            .committed => .{ .reservation_digest = grant.record.reservation, .checkpoint = observed },
        };
    }
    pub fn admit(self: Admission, a: A, grant: *const Grant, fence: metadata.CatalogRouteFence, original: Context) !Accepted {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        if (grant.record.phase != .sealed or grant.record.stamp == null) return error.SourcePendingWork;
        if (try self.resolve(a, grant, fence, context)) |accepted| return accepted;
        const manifest = grant.manifest.value;
        const retained = try manifest.load(a, grant.record, fence, self.artifacts, context);
        var prepared = try self.coordinator.qualifyRetained(a, retained, context);
        defer prepared.deinit();
        _ = self.coordinator.commitSourcePrepared(a, &prepared, manifest.commit_version, context) catch |err| switch (err) {
            // Read-only checkpoint resolution may prove that the original
            // COMMIT survived an unknown mutation response. Never resend here.
            error.CommitDecisionUnknown => null,
            else => return err,
        };
        return (try self.resolve(a, grant, fence, context)) orelse error.SourceOutcomeUnresolved;
    }
};

test "completion frozen plan source admission binds metadata grant before checkpoint proof" {
    const a = std.testing.allocator;
    const plans = @import("completion_plan.zig");
    const scope: source.Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 2, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 9, .big);
    std.mem.writeInt(u64, namespace[16..24], 9, .big);
    const owner: plans.completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = namespace, .table_binding = @splat(5), .root_generation = 0, .group_id = 9, .topology_epoch = 1 };
    var pending: source.Reservation = .{ .sequence = 1, .batch_id = "one", .checkpoint = "next", .accepted_at_ns = 11, .payload_digest = @splat(6), .plan_digest = @splat(7), .transaction_id = @splat(8), .destination = .native, .delivery_manifest = "{}" };
    const binding: local.storage_source_scope.Spec = .{ .actor_table_id = scope.table_id, .source_id = scope.source_id, .generation = scope.generation, .binding = scope.identity(), .checkpoint_owner = try local.storage_source_scope.Basis.fromOwner(owner) };
    const reference: local.storage_source_scope.Reference = .{ .domain = binding.domain(), .binding = binding.binding, .generation = scope.generation, .token = 3, .sequence = 1 };
    const command: local.storage_source_checkpoint.Command = .{ .binding = scope.identity(), .purpose = try pending.inputFingerprint(scope), .sequence = 1, .checkpoint = pending.checkpoint, .accepted_at_ns = pending.accepted_at_ns, .receipt_window = 3 };
    const request = try (local.storage_db_selected_root.db.types.TransactionIntentRequest{ .completion_owner = owner, .completion_source_scope = reference, .completion_source_purpose = command.purpose, .completion_source_checkpoint = command }).completionBatch(pending.transaction_id, 10, 1);
    var plan = try plans.Plan.freezeDerived(a, pending.transaction_id, owner, &.{.{ .owner = owner, .locator = "table2:00000004:docs:9", .roles = plans.completion.Roles.checkpoint, .prepare = request }}, 11);
    defer plan.deinit();
    var cut: [32]u8 = @splat(0);
    std.mem.writeInt(u64, cut[0..8], 1, .little);
    std.mem.writeInt(u64, cut[8..16], 1, .little);
    cut[31] = 1;
    var frozen = try destination.Frozen.init(a, &plan, .{ .binding = binding.binding, .table_name = "docs", .owner = owner }, .{ .root = try @import("../data/source_plan_entry.zig").rootFor(owner), .cut = cut, .quota = .{} }, 20);
    defer frozen.deinit();
    const compiled = try frozen.manifest.deliveryPlan(a, frozen.encoded_manifest);
    pending.delivery_manifest = compiled.manifest;
    pending.plan_digest = compiled.digest;
    pending.artifact = compiled.artifact;
    pending.retain_manifest = compiled.retain_manifest;
    const record: artifact.Record = .{ .reservation = pending.identity(scope), .spec = compiled.artifact.?, .recovery = .{ .digest = pending.plan_digest, .bytes = pending.delivery_manifest } };
    var grant = try Grant.init(a, scope, pending, record);
    defer grant.deinit();
    var transient_pending = pending;
    transient_pending.delivery_manifest = try a.dupe(u8, pending.delivery_manifest);
    var transient_record = record;
    transient_record.recovery.?.bytes = transient_pending.delivery_manifest;
    var independent = try Grant.init(a, scope, transient_pending, transient_record);
    defer independent.deinit();
    a.free(transient_pending.delivery_manifest);
    try std.testing.expectEqualStrings(pending.delivery_manifest, independent.record.recovery.?.bytes);
    const Fake = struct {
        committed: bool = false,
        forge: bool = false,
        calls: usize = 0,
        fn read(ptr: *anyopaque, alloc: A, _: metadata.CatalogRouteFence, _: []const u8, query: checkpoints.wire.Request, _: Context) !checkpoints.wire.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            var result: checkpoints.wire.Reply = .{ .request_digest = query.digest(), .cut = .{ .term = 1, .index = 1 }, .disposition = .absent };
            if (self.committed) {
                const head: local.storage_source_checkpoint.Head = .{ .binding = query.command.binding, .purpose = query.command.purpose, .transaction_id = query.transaction_id, .sequence = query.command.sequence, .checkpoint = query.command.checkpoint, .accepted_at_ns = query.command.accepted_at_ns, .receipt_window = query.command.receipt_window, .request_digest = query.command.digest() };
                const encoded = try head.encode(alloc);
                defer alloc.free(encoded);
                result.head = try checkpoints.wire.Evidence.init(encoded);
                result.receipt = result.head;
                result.disposition = .committed;
            }
            if (self.forge) result.request_digest[0] ^= 1;
            return result;
        }
    };
    var fake: Fake = .{};
    const admission: Admission = .{ .coordinator = undefined, .artifacts = undefined, .checkpoints = .{ .ptr = &fake, .read_fn = Fake.read } };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 9, .range_id = 9, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 9 } } };
    try std.testing.expect(try admission.resolve(a, &grant, fence, .{}) == null);
    try std.testing.expectError(error.SourcePendingWork, admission.admit(a, &grant, fence, .{}));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    grant.record.phase = .sealed;
    grant.record.stamp = @splat(9);
    fake.committed = true;
    const accepted = try admission.admit(a, &grant, fence, .{});
    try std.testing.expectEqualDeep(record.reservation, accepted.reservation_digest);
    try std.testing.expectEqual(.committed, accepted.checkpoint.disposition);
    fake.forge = true;
    try std.testing.expectError(error.SourceAdmissionCorrupt, admission.resolve(a, &grant, fence, .{}));
    var wrong = pending;
    wrong.payload_digest[0] ^= 1;
    var wrong_record = record;
    wrong_record.reservation = wrong.identity(scope);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, Grant.init(a, scope, wrong, wrong_record));
    wrong_record = record;
    wrong_record.reservation[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, Grant.init(a, scope, pending, wrong_record));
}
