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
const operation = @import("antfly_local_sources").api_operation;
const protocol = @import("../../topology_protocol.zig");
const wire = @import("boot_wire.zig");

pub fn submitStandalone(store: anytype, alloc: std.mem.Allocator, group: u64, context: operation.RequestContext, bytes: []const u8) ![]u8 {
    if (!context.setting_admin or context.sql_command_fence != null) return error.Forbidden;
    var request = try wire.Request.parse(alloc, bytes);
    defer request.deinit();
    const preview = try store.sqlRelationBootPreflightJson(alloc, group, bytes);
    defer alloc.free(preview);
    try context.ensureActive();
    try store.applyStandaloneCommand(group, .{ .apply_sql_relation_boot = bytes });
    const result = try store.sqlRelationBootJson(alloc, group, request.value.observationQuery());
    errdefer alloc.free(result);
    var observed = try std.json.parseFromSlice(@import("boots.zig").Observation, alloc, result, .{});
    defer observed.deinit();
    _ = try request.value.requireApplied(observed.value);
    return result;
}

pub fn submit(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, bytes: []const u8) ![]u8 {
    if (!context.setting_admin or context.sql_command_fence != null) return error.Forbidden;
    var request = try wire.Request.parse(alloc, bytes);
    defer request.deinit();
    if (request.value.metadata_group_id != svc.metadata_group_id) return error.SqlRelationOwnerChanged;
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_relation_persistence_version);
    svc.lockCatalogMutation();
    var locked = true;
    defer if (locked) svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const preview = try store.sqlRelationBootPreflightJson(alloc, svc.metadata_group_id, bytes);
    defer alloc.free(preview);
    try context.ensureActive();
    const receipt = try @import("../../service.zig").proposeCatalogWithContext(svc, context, .{ .apply_sql_relation_boot = bytes });
    svc.unlockCatalogMutation();
    locked = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const result = store.sqlRelationBootJson(alloc, svc.metadata_group_id, request.value.observationQuery()) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(result);
    var observed = try std.json.parseFromSlice(@import("boots.zig").Observation, alloc, result, .{});
    defer observed.deinit();
    _ = try request.value.requireApplied(observed.value);
    return result;
}
