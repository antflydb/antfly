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

const std = @import("std");
const local = @import("antfly_local_sources");
const source = @import("../metadata/source_control.zig");
const plans = @import("completion_plan.zig");
const destination = @import("source_destination_plan.zig");
const receipts = @import("source_destination_receipt.zig");
const checkpoints = @import("source_checkpoint_operations.zig");
const A = std.mem.Allocator;
const Fixture = struct {
    arena: std.heap.ArenaAllocator,
    frozen: destination.Frozen,
    state: source.State,
    native: @import("source_destination_admission.zig").Accepted,
    fn deinit(self: *Fixture) void {
        self.frozen.deinit();
        self.arena.deinit();
    }
    fn init(a: A, checkpoint_bytes: []const u8) !Fixture {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const checkpoint_value = try owned.dupe(u8, checkpoint_bytes);
        const scope: source.Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
        var namespace: [24]u8 = undefined;
        std.mem.writeInt(u64, namespace[0..8], 7, .big);
        std.mem.writeInt(u64, namespace[8..16], 9, .big);
        std.mem.writeInt(u64, namespace[16..24], 9, .big);
        const owner: plans.completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = namespace, .table_binding = @splat(5), .root_generation = 0, .group_id = 9, .topology_epoch = 1 };
        const authority: source.checkpoint_authority.Spec = .{ .binding = scope.identity(), .table_name = "docs", .owner = owner };
        var state = try (source.Mutation{ .configure = scope }).apply(owned, .{});
        state = try (source.Mutation{ .bind_checkpoint = authority }).apply(owned, state);
        state = try (source.Mutation{ .enable_scope_fencing = {} }).apply(owned, state);
        var pending: source.Reservation = .{ .sequence = 1, .batch_id = "one", .checkpoint = checkpoint_value, .accepted_at_ns = 1, .payload_digest = @splat(6), .plan_digest = @splat(7), .transaction_id = @splat(8), .destination = .native, .delivery_manifest = "{}" };
        const reference = (try @import("../metadata/source_scope_binding.zig").reference(state, 1, 1)).?;
        const command: local.storage_source_checkpoint.Command = .{ .binding = scope.identity(), .purpose = try pending.inputFingerprint(scope), .sequence = 1, .checkpoint = checkpoint_value, .accepted_at_ns = 1, .receipt_window = 3 };
        const input = try (local.storage_db_selected_root.db.types.TransactionIntentRequest{ .completion_owner = owner, .completion_source_scope = reference, .completion_source_purpose = command.purpose, .completion_source_checkpoint = command }).completionBatch(pending.transaction_id, 10, 1);
        var plan = try plans.Plan.freezeDerived(a, pending.transaction_id, owner, &.{.{ .owner = owner, .locator = "table2:00000004:docs:9", .roles = plans.completion.Roles.checkpoint, .prepare = input }}, 11);
        defer plan.deinit();
        var position: [32]u8 = @splat(0);
        std.mem.writeInt(u64, position[0..8], 1, .little);
        std.mem.writeInt(u64, position[8..16], 1, .little);
        position[31] = 1;
        var frozen = try destination.Frozen.init(a, &plan, authority, .{ .root = try @import("../data/source_plan_entry.zig").rootFor(owner), .cut = position, .quota = .{} }, 20);
        errdefer frozen.deinit();
        const compiled = try frozen.manifest.deliveryPlan(owned, frozen.encoded_manifest);
        pending.delivery_manifest = compiled.manifest;
        pending.plan_digest = compiled.digest;
        pending.artifact = compiled.artifact;
        pending.retain_manifest = compiled.retain_manifest;
        state = try (source.Mutation{ .reserve = pending }).apply(owned, state);
        const id = pending.identity(scope);
        state = try (source.Mutation{ .seal_artifact = .{ .reservation_digest = id, .stamp = @splat(9) } }).apply(owned, state);
        const head: local.storage_source_checkpoint.Head = .{ .binding = command.binding, .purpose = command.purpose, .transaction_id = pending.transaction_id, .sequence = 1, .checkpoint = checkpoint_value, .accepted_at_ns = 1, .receipt_window = 3, .request_digest = command.digest() };
        const encoded_head = try head.encode(owned);
        const evidence = try checkpoints.wire.Evidence.init(encoded_head);
        const query: checkpoints.wire.Request = .{ .version = 1, .source_owner = owner, .transaction_id = pending.transaction_id, .command = command, .minimum_applied_index = 1 };
        return .{ .arena = arena, .frozen = frozen, .state = state, .native = .{ .reservation_digest = id, .checkpoint = .{ .request_digest = query.digest(), .cut = .{ .term = 1, .index = 2 }, .disposition = .committed, .head = evidence, .receipt = evidence } } };
    }
};

test "completion frozen plan acceptance receipts stay bounded and recover cleanup authority" {
    const a = std.testing.allocator;
    const checkpoint: [1024]u8 = @splat(1);
    var fixture = try Fixture.init(a, &checkpoint);
    defer fixture.deinit();
    const Fake = struct {
        native: @import("source_destination_admission.zig").Accepted,
        calls: usize = 0,
        fn read(ptr: *anyopaque, _: A, _: @import("../metadata/api.zig").CatalogRouteFence, _: []const u8, _: checkpoints.wire.Request, _: local.api_operation.RequestContext) !checkpoints.wire.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return self.native.checkpoint;
        }
    };
    var fake: Fake = .{ .native = fixture.native };
    const bridge: @import("source_destination_admission.zig").Admission = .{ .coordinator = undefined, .artifacts = undefined, .checkpoints = .{ .ptr = &fake, .read_fn = Fake.read } };
    const owner = fixture.frozen.manifest.owner;
    const fence: @import("../metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 9, .range_id = 9, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 9 } } };
    const resolved = (try bridge.resolveDelivery(a, fixture.state, fence, .{})).?;
    defer a.free(resolved.receipt);
    const admitted = try bridge.admitDelivery(a, fixture.state, fence, .{});
    defer a.free(admitted.receipt);
    try std.testing.expect(admitted.commit_reference == null);
    var closing = fixture.state;
    closing.closing = true;
    const calls_before_closing = fake.calls;
    try std.testing.expectError(error.SourcePendingWork, bridge.admitDelivery(a, closing, fence, .{}));
    try std.testing.expectEqual(calls_before_closing, fake.calls);
    const draining = (try bridge.resolveDelivery(a, closing, fence, .{})).?;
    defer a.free(draining.receipt);
    const accepted = try receipts.accept(a, fixture.state, fixture.native);
    defer a.free(accepted.receipt);
    try std.testing.expect(accepted.receipt.len <= receipts.max_bytes);
    try std.testing.expect(accepted.commit_reference == null);
    const completion: @import("source_destination_completion.zig").Completion = .{ .recovery = undefined };
    try std.testing.expectError(error.SourcePendingWork, completion.run(a, fixture.state, accepted.receipt, fence, .{}));
    var before = try receipts.decode(a, fixture.state, accepted.receipt);
    defer before.deinit();
    try std.testing.expect(before == .retained);
    try std.testing.expectEqualDeep(fixture.frozen.manifest.artifact.plan_digest, before.retained.manifest.value.artifact.plan_digest);
    var wrong = fixture.native;
    wrong.checkpoint.request_digest[0] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, receipts.accept(a, fixture.state, wrong));
    const owned = fixture.arena.allocator();
    fixture.state = try (source.Mutation{ .accept = .{ .reservation_digest = accepted.reservation_digest, .receipt = accepted.receipt } }).apply(owned, fixture.state);
    try std.testing.expect(fixture.state.pending == null);
    try std.testing.expectEqual(@as(u64, 1), fixture.state.accepted.sequence);
    try std.testing.expectEqual(@as(u64, 0), fixture.state.committed.sequence);
    var restored = try source.parse(a, try source.encode(owned, fixture.state));
    defer restored.deinit();
    var after = try receipts.decode(a, restored.value, accepted.receipt);
    defer after.deinit();
    try std.testing.expect(after == .retained);
    var wrong_fence = fence;
    wrong_fence.route.group_id += 1;
    try std.testing.expectError(error.InvalidCatalogRouteFence, completion.run(a, restored.value, accepted.receipt, wrong_fence, .{}));
    var substituted = after.retained.receipt;
    substituted.checkpoint.cut.index += 1;
    const different = try std.json.Stringify.valueAlloc(a, substituted, .{});
    defer a.free(different);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, receipts.decode(a, restored.value, different));
    const unknown = try std.fmt.allocPrint(a, "{{\"extra\":1,{s}", .{accepted.receipt[1..]});
    defer a.free(unknown);
    try std.testing.expectError(error.InvalidSourceControl, receipts.decode(a, restored.value, unknown));
    fixture.state = try (source.Mutation{ .commit = .{ .sequence = 1, .receipt_digest = fixture.state.accepted.receipt_digest, .reference = "all-owners-complete" } }).apply(owned, fixture.state);
    fixture.state = try (source.Mutation{ .retire_artifact = .{ .reservation_digest = accepted.reservation_digest, .proof = @splat(10), .reason = .committed } }).apply(owned, fixture.state);
    var garbage = try receipts.decode(a, fixture.state, accepted.receipt);
    defer garbage.deinit();
    try std.testing.expect(garbage == .retained);
    fixture.state = try (source.Mutation{ .reclaim_artifact = .{ .reservation_digest = accepted.reservation_digest, .proof = @splat(11) } }).apply(owned, fixture.state);
    var reclaimed = try receipts.decode(a, fixture.state, accepted.receipt);
    defer reclaimed.deinit();
    try std.testing.expect(reclaimed == .reclaimed);
    try std.testing.expectEqual(.reclaimed, try completion.run(a, fixture.state, accepted.receipt, fence, .{}));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, receipts.decode(a, fixture.state, different));
    // Even a self-consistent generic metadata row cannot promote a receipt
    // whose source token is beyond the proven cumulative cleanup floor.
    var unproven = after.retained.receipt;
    unproven.source_scope.token += 1;
    const beyond_floor = try std.json.Stringify.valueAlloc(a, unproven, .{});
    defer a.free(beyond_floor);
    var inconsistent = fixture.state;
    inconsistent.accepted.reference = beyond_floor;
    inconsistent.accepted.receipt_digest = source.digest(beyond_floor);
    inconsistent.committed.receipt_digest = inconsistent.accepted.receipt_digest;
    try inconsistent.validate();
    try std.testing.expectError(error.SourceTransactionIdentityConflict, receipts.decode(a, inconsistent, beyond_floor));
    var next_spec = fixture.state.artifact_control.?.last_reclaimed.?.spec;
    std.mem.writeInt(u64, next_spec.attempt[0..8], 2, .big);
    const next: source.Reservation = .{ .sequence = 2, .batch_id = "two", .checkpoint = "cp2", .expected_checkpoint = fixture.state.accepted.checkpoint, .accepted_at_ns = 2, .payload_digest = @splat(12), .plan_digest = @splat(13), .transaction_id = @splat(14), .destination = .native, .delivery_manifest = "next-plan", .artifact = next_spec, .retain_manifest = true };
    fixture.state = try (source.Mutation{ .reserve = next }).apply(owned, fixture.state);
    var while_next_pending = try receipts.decode(a, fixture.state, accepted.receipt);
    defer while_next_pending.deinit();
    try std.testing.expect(while_next_pending == .reclaimed);
    const next_id = next.identity(fixture.state.scope.?);
    fixture.state = try (source.Mutation{ .retire_artifact = .{ .reservation_digest = next_id, .proof = @splat(15), .reason = .unsealed } }).apply(owned, fixture.state);
    fixture.state = try (source.Mutation{ .reclaim_artifact = .{ .reservation_digest = next_id, .proof = @splat(16) } }).apply(owned, fixture.state);
    var after_later_reclamation = try receipts.decode(a, fixture.state, accepted.receipt);
    defer after_later_reclamation.deinit();
    try std.testing.expect(after_later_reclamation == .reclaimed);
}
