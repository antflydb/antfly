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

//! Real metadata Raft -> service ReadIndex -> authenticated HTTP capture.
const std = @import("std");
const local = @import("antfly_local_sources");
const control = @import("source_control.zig");
const scopes = @import("../api/source_scope_metadata.zig");
const client_mod = @import("http_client.zig");
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;
fn mutate(a: A, svc: anytype, scope: control.Scope, sources: []const u8, mutation: control.Mutation, context: Context) !void {
    const bytes = try control.readOnService(svc, a, scope.table_id, scope.source_id, context);
    defer a.free(bytes);
    var state = try control.parse(a, bytes);
    defer state.deinit();
    try control.mutateOnService(svc, a, .{ .table_id = scope.table_id, .source_id = scope.source_id, .expected_revision = state.value.revision, .expected_binding = scope.identity(), .expected_catalog_digest = control.digest(sources), .mutation = mutation }, context);
}
pub fn verify(a: A, svc: anytype, client: *client_mod.MetadataHttpClient, uri: []const u8) !void {
    const sources = "[{\"type\":\"postgres\",\"postgres_table\":\"users\"}]";
    const context: Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 30 * std.time.ns_per_s };
    try svc.upsertTable(.{ .table_id = 77, .name = "docs", .replication_sources_json = sources });
    const identity = try svc.catalogIdentity();
    const scope: control.Scope = .{ .table_id = 77, .source_id = @splat(71), .source_ordinal = 0, .generation = 1, .definition = try control.definitionDigest(a, sources, 0), .provider = @splat(72), .destination = .native, .destination_binding = @splat(73) };
    try mutate(a, svc, scope, sources, .{ .configure = scope }, context);
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 77, .big);
    std.mem.writeInt(u64, namespace[8..16], 17, .big);
    std.mem.writeInt(u64, namespace[16..24], 19, .big);
    try mutate(a, svc, scope, sources, .{ .bind_checkpoint = .{ .binding = scope.identity(), .table_name = "docs", .owner = .{ .metadata_group_id = identity.metadata_group_id, .metadata_incarnation = identity.metadata_incarnation, .namespace = namespace, .table_binding = @splat(74), .root_generation = 0, .group_id = 601, .topology_epoch = 1 } } }, context);
    try mutate(a, svc, scope, sources, .{ .enable_scope_fencing = {} }, context);
    var input: scopes.wire.Request = .{ .version = 1, .query = .{ .metadata_group_id = identity.metadata_group_id, .metadata_incarnation = identity.metadata_incarnation, .actor_table_id = scope.table_id, .source_id = scope.source_id, .minimum_applied_index = 1 } };
    var active = try client.captureSourceScope(uri, input, context);
    defer active.deinit(a);
    try active.requireBind();
    try std.testing.expectEqual(scope.identity(), active.spec.binding);
    try std.testing.expectEqualStrings("docs", active.checkpoint_table);
    try mutate(a, svc, scope, sources, .{ .close = {} }, context);
    input.query.minimum_applied_index = active.cut.index + 1;
    var closing = try client.captureSourceScope(uri, input, context);
    defer closing.deinit(a);
    try closing.requireClose();
    try closing.requireBinding(active.spec);
    try std.testing.expect(closing.cut.index > active.cut.index);
    try std.testing.expectError(error.SourceBindingConflict, closing.requireBind());
    try mutate(a, svc, scope, sources, .{ .retire = .{ .receipt = "metadata-only-empty-actor-retirement" } }, context);
    input.query.minimum_applied_index = closing.cut.index + 1;
    var retired = try client.captureSourceScope(uri, input, context);
    defer retired.deinit(a);
    try retired.requireDrop();
    try retired.requireBinding(active.spec);
    try std.testing.expect(retired.cut.index > closing.cut.index);
    // Metadata retirement does not fabricate a physical fencing certificate.
    const raw = try control.readOnService(svc, a, scope.table_id, scope.source_id, context);
    defer a.free(raw);
    var state = try control.parse(a, raw);
    defer state.deinit();
    try std.testing.expect(control.hasWork(state.value));
}
