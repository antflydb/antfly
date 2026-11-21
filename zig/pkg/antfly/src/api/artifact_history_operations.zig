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

//! Fenced native artifact-publication history reads.
const std = @import("std");
const local = @import("antfly_local_sources");
const metadata = @import("../metadata/api.zig");
pub const wire = local.storage_db_artifact_publication;
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;
pub const max_bytes = wire.max_history_read_bytes;

pub fn validate(input: wire.HistoryRead, fence: metadata.CatalogRouteFence, table: []const u8) !void {
    try fence.validate();
    const identity = fence.route.identity_namespace;
    if (table.len == 0 or table.len > 1024 or
        std.mem.readInt(u64, input.namespace[0..8], .big) != identity.table_id or
        std.mem.readInt(u64, input.namespace[8..16], .big) != identity.shard_id or
        std.mem.readInt(u64, input.namespace[16..24], .big) != identity.range_id) return error.InvalidHistoryAdmission;
}

pub fn execute(port: ?Port, a: A, group: u64, table: []const u8, body: []const u8, context: Context) !wire.HistoryObservation {
    try context.ensureActive();
    if (group == 0 or body.len > max_bytes or context.catalog_route_fence_json.len == 0 or context.catalog_route_fence_json.len > 16384)
        return error.InvalidCatalogRouteFence;
    var decoded_fence = std.json.parseFromSlice(metadata.CatalogRouteFence, a, context.catalog_route_fence_json, .{}) catch |err|
        return if (err == error.OutOfMemory) err else error.InvalidCatalogRouteFence;
    defer decoded_fence.deinit();
    const fence = decoded_fence.value;
    if (group != fence.route.group_id) return error.InvalidCatalogRouteFence;
    var parsed = std.json.parseFromSlice(wire.HistoryRead, a, body, .{}) catch |err|
        return if (err == error.OutOfMemory) err else error.InvalidHistoryAdmission;
    defer parsed.deinit();
    try validate(parsed.value, fence, table);
    return (port orelse return error.ChangefeedCapabilityUnsupported).read(a, fence, table, parsed.value, context);
}

pub const Port = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, metadata.CatalogRouteFence, []const u8, wire.HistoryRead, Context) anyerror!wire.HistoryObservation,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,

    pub const VTable = struct { read: @FieldType(Port, "read_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);

    pub fn read(self: Port, a: A, fence: metadata.CatalogRouteFence, table: []const u8, input: wire.HistoryRead, original: Context) !wire.HistoryObservation {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const request = scope.context();
        try request.ensureActive();
        try validate(input, fence, table);
        const result = try Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, input, request });
        try request.ensureActive();
        try input.validateObservation(result, 1);
        return result;
    }
};

pub const artifact_event = control.artifact_event;
const control = @import("../metadata/changefeed_control.zig");
const delivery = @import("../metadata/changefeed_delivery.zig");
const hex = @import("../metadata/native_changefeed_delivery.zig").hex;

/// Render a page from an already authorized and generation-qualified read.
pub fn render(a: A, state: control.State, table: control.Table, input: wire.HistoryRead, observation: wire.HistoryObservation, before: control.Digest, original: ?control.Delivery, limits: delivery.Limits) !delivery.Prepared {
    try limits.validate();
    const definition = state.definition orelse return error.ChangefeedResynchronizationRequired;
    if (state.phase != .streaming or definition.events != artifact_event or input.after == null or
        std.mem.readInt(u64, input.namespace[0..8], .big) != table.table_id)
        return error.ChangefeedCapabilityUnsupported;
    try input.validateObservation(observation, 1);
    const next = if (original != null and original.?.advances.len == 0) null else observation.event;
    if (next) |event| {
        const position: control.Position = .{ .sequence = event.sequence };
        if (original) |receipt| {
            if (receipt.advances.len != 1 or !std.mem.eql(u8, &receipt.advances[0].namespace, &input.namespace) or
                receipt.advances[0].position.order(position) != .eq or receipt.advances[0].snapshot != null)
                return error.ChangefeedResynchronizationRequired;
        }
        const identity = control.hash(.{
            .domain = "antfly:artifact-publication-feed-event:v1",
            .consumer = state.consumerId(),
            .binding = table.binding,
            .root_generation = input.root_generation,
            .event = event,
        });
        const body = try std.json.Stringify.valueAlloc(a, .{
            .version = @as(u8, 1),
            .events = .{.{
                .kind = "artifact_published",
                .event_id = try hex(a, identity),
                .table_id = try std.fmt.allocPrint(a, "{d}", .{table.table_id}),
                .producer_generation = try std.fmt.allocPrint(a, "{d}", .{event.producer_generation}),
                .producer_configuration = try hex(a, event.catalog_digest),
                .source_coverage = try hex(a, event.source_digest),
            }},
            .transaction = @as(?u8, null),
        }, .{});
        if (body.len > limits.bytes) return error.ChangefeedBackpressure;
        return .{
            .body = body,
            .events = 1,
            .proof = control.hash(.{ .domain = "antfly:artifact-publication-feed-receipt:v1", .position = position, .event = identity }),
            .advances = try a.dupe(control.Advance, &.{.{ .namespace = input.namespace, .position = position }}),
            .provider_context = try a.dupe(u8, try state.contextAt(before)),
        };
    }
    if (original) |receipt| if (receipt.advances.len != 0) return error.ChangefeedResynchronizationRequired;
    const body = "{\"version\":1,\"events\":[],\"transaction\":null}";
    if (body.len > limits.bytes) return error.ChangefeedBackpressure;
    return .{
        .body = body,
        .events = 0,
        .proof = control.hash(.{ .domain = "antfly:artifact-publication-feed-heartbeat:v1", .consumer = state.consumerId(), .before = before }),
        .advances = &.{},
        .provider_context = try a.dupe(u8, try state.contextAt(before)),
    };
}

test "httpx changefeed artifact history retains an exact publication across replay" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var namespace: control.Namespace = @splat(1);
    std.mem.writeInt(u64, namespace[0..8], 9, .big);
    const table: control.Table = .{ .table_id = 9, .binding = @splat(2), .policy_revision = 1, .contract = .native_document };
    const cursor: control.Digest = @splat(3);
    const state: control.State = .{
        .definition = .{ .tables = &.{table}, .owner = @splat(4), .authorization = @splat(5), .events = artifact_event },
        .phase = .streaming,
        .acknowledged_cursor = cursor,
    };
    const input: wire.HistoryRead = .{ .namespace = namespace, .root_generation = 0, .after = 0 };
    const event: wire.HistoryEvent = .{
        .namespace = namespace,
        .sequence = 1,
        .authority_epoch = 1,
        .producer_generation = 2,
        .source_count = 1,
        .catalog_digest = @splat(6),
        .publication_digest = @splat(7),
        .source_digest = @splat(8),
    };
    var observation: wire.HistoryObservation = .{ .namespace = namespace, .root_generation = 0, .latest = 1, .applied_index = 1, .event = event };
    const prepared = try render(a, state, table, input, observation, cursor, null, .{});
    const receipt: control.Delivery = .{ .sequence = 1, .before = cursor, .cursor = @splat(9), .authority = prepared.proof, .issued_at = 2, .advances = prepared.advances };
    observation.latest = 2;
    observation.applied_index = 3;
    const replay = try render(a, state, table, input, observation, cursor, receipt, .{});
    try std.testing.expectEqualStrings(prepared.body, replay.body);
    try std.testing.expectEqual(prepared.proof, replay.proof);
    observation.root_generation = 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, render(a, state, table, input, observation, cursor, receipt, .{}));
    observation.root_generation = 0;
    observation.latest = wire.history_limit + 1;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, render(a, state, table, input, observation, cursor, receipt, .{}));
}
