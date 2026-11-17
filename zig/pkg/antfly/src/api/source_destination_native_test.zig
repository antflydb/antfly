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
const A = std.mem.Allocator;
pub fn verify(a: A, origin: anytype, fence: @import("../metadata/api.zig").CatalogRouteFence, owner: plans.completion.RoutedOwner) !void {
    const scope: local.storage_source_scope.Reference = .{ .domain = @splat(221), .binding = @splat(222), .generation = 1, .token = 3, .sequence = 1 };
    const checkpoint: local.storage_source_checkpoint.Command = .{ .binding = scope.binding, .purpose = @splat(223), .sequence = 1, .checkpoint = "manifest-start", .accepted_at_ns = 5011, .receipt_window = 3 };
    const id: [16]u8 = @splat(224);
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
    var record: artifact.Record = .{ .reservation = @splat(225), .spec = delivery.artifact.? };
    record.stamp = try transfer.seal(a, record, frozen.bytes, .{});
    const sealed = try ports.observations.read(a, fence, "docs", quota_input, .{});
    // Native seal alone never substitutes for the metadata seal transition.
    try std.testing.expectError(error.SourceControlConflict, manifest.value.load(a, .{ .reservation = record.reservation, .spec = record.spec }, fence, ports, .{}));
    record.phase = .sealed;
    var restored = try manifest.value.load(a, record, fence, ports, .{});
    defer restored.deinit();
    try std.testing.expectEqualDeep(plan.admission.digest(), restored.admission.digest());
    try std.testing.expectEqualDeep(scope, restored.source_scope.?);
    const after_load = try ports.observations.read(a, fence, "docs", quota_input, .{});
    try std.testing.expectEqualDeep(sealed, after_load);
    record.phase = .garbage;
    record.retirement = @splat(226);
    try std.testing.expect((try transfer.reclaim(a, record, .{})) != null);
}
