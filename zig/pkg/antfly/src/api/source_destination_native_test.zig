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

//! Qualification through the production routed HTTP ports and native owner.
//! This fixture grants an artifact ledger locally; it does not grant source
//! lifecycle registration, metadata acceptance or transaction admission.
const std = @import("std");
const local = @import("antfly_local_sources");
const plans = @import("completion_plan.zig");
const destination = @import("source_destination_plan.zig");
const artifact = @import("../metadata/source_artifact_control.zig");
const admission_mod = @import("source_destination_admission.zig");
const source = @import("../metadata/source_control.zig");
const A = std.mem.Allocator;
pub fn verify(a: A, origin: anytype, fence: @import("../metadata/api.zig").CatalogRouteFence, owner: plans.completion.RoutedOwner) !void {
    const actor: source.Scope = .{ .table_id = std.mem.readInt(u64, owner.namespace[0..8], .big), .source_id = @splat(221), .source_ordinal = 0, .generation = 1, .definition = @splat(222), .provider = @splat(223), .destination = .native, .destination_binding = @splat(224) };
    const binding: local.storage_source_scope.Spec = .{ .actor_table_id = actor.table_id, .source_id = actor.source_id, .generation = actor.generation, .binding = actor.identity(), .checkpoint_owner = try local.storage_source_scope.Basis.fromOwner(owner) };
    const scope: local.storage_source_scope.Reference = .{ .domain = binding.domain(), .binding = binding.binding, .generation = 1, .token = 3, .sequence = 1 };
    const id: [16]u8 = @splat(225);
    var pending: source.Reservation = .{ .sequence = 1, .batch_id = "native-manifest", .checkpoint = "manifest-start", .accepted_at_ns = 5011, .payload_digest = @splat(226), .plan_digest = @splat(227), .transaction_id = id, .destination = .native, .delivery_manifest = "{}" };
    const checkpoint: local.storage_source_checkpoint.Command = .{ .binding = scope.binding, .purpose = try pending.inputFingerprint(actor), .sequence = 1, .checkpoint = pending.checkpoint, .accepted_at_ns = pending.accepted_at_ns, .receipt_window = 3 };
    const locator = try local.api_local_transaction_contract.participantIdForGroup(a, "docs", owner.group_id);
    defer a.free(locator);
    const request = try (local.storage_db_selected_root.db.types.TransactionIntentRequest{ .completion_owner = owner, .completion_source_scope = scope, .completion_source_purpose = checkpoint.purpose, .completion_source_checkpoint = checkpoint }).completionBatch(id, 5010, owner.topology_epoch);
    var plan = try plans.Plan.freezeDerived(a, id, owner, &.{.{ .owner = owner, .locator = locator, .roles = plans.completion.Roles.checkpoint, .prepare = request }}, 5011);
    defer plan.deinit();
    const ports: destination.Ports = .{ .controls = origin.sourcePlanControlPort(), .observations = origin.sourcePlanObservePort(), .chunks = origin.sourcePlanChunkPort() };
    const quota_input: @import("source_plan_operations.zig").wire.ObserveRequest = .{ .version = 1, .source_owner = owner, .query = .{ .root = try @import("../data/source_plan_entry.zig").rootFor(owner), .minimum_applied_index = 1 } };
    const quota = try ports.observations.read(a, fence, "docs", quota_input, .{});
    var frozen = try destination.Frozen.init(a, &plan, .{ .binding = scope.binding, .table_name = "docs", .owner = owner }, quota, 5020);
    defer frozen.deinit();
    const delivery = try frozen.manifest.deliveryPlan(a, frozen.encoded_manifest);
    var manifest = try destination.parse(a, delivery.manifest, delivery.digest);
    defer manifest.deinit();
    const transfer = try manifest.value.transfer(fence, ports);
    pending.delivery_manifest = delivery.manifest;
    pending.plan_digest = delivery.digest;
    pending.artifact = delivery.artifact;
    var record: artifact.Record = .{ .reservation = pending.identity(actor), .spec = delivery.artifact.? };
    record.stamp = try transfer.seal(a, record, frozen.bytes, .{});
    const sealed = try ports.observations.read(a, fence, "docs", quota_input, .{});
    // Native seal alone never substitutes for the metadata seal transition.
    try std.testing.expectError(error.SourceControlConflict, manifest.value.load(a, .{ .reservation = record.reservation, .spec = record.spec }, fence, ports, .{}));
    record.phase = .sealed;
    var restored = try manifest.value.load(a, record, fence, ports, .{});
    defer restored.deinit();
    try std.testing.expectEqualDeep(plan.admission.digest(), restored.admission.digest());
    try std.testing.expectEqualDeep(scope, restored.source_scope.?);
    var grant = try admission_mod.Grant.init(a, actor, pending, record);
    defer grant.deinit();
    const admission: admission_mod.Admission = .{ .coordinator = undefined, .artifacts = ports, .checkpoints = origin.sourceCheckpointObservationPort() };
    // A real qualified absent checkpoint remains unproven; resolving it cannot
    // submit any admission or change the native cut.
    try std.testing.expect(try admission.resolve(a, &grant, fence, .{}) == null);
    const after_load = try ports.observations.read(a, fence, "docs", quota_input, .{});
    try std.testing.expectEqualDeep(sealed, after_load);
    record.phase = .garbage;
    record.retirement = @splat(226);
    try std.testing.expect((try transfer.reclaim(a, record, .{})) != null);
}
