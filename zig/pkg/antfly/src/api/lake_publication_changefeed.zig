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

//! Delivery from the metadata that committed the lake publication.
const std = @import("std");
const local = @import("antfly_local_sources");
const control = @import("../metadata/changefeed_control.zig");
const delivery = @import("../metadata/changefeed_delivery.zig");
const lifecycle = @import("../metadata/lake_index_lifecycle.zig");
const hex = @import("../metadata/native_changefeed_delivery.zig").hex;
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;

pub const activation_event: u8 = 4;

pub fn render(a: A, state: control.State, table: control.Table, after: control.Position, observed: lifecycle.State, before: control.Digest, original: ?control.Delivery, limits: delivery.Limits) !delivery.Prepared {
    try limits.validate();
    const definition = state.definition orelse return error.ChangefeedResynchronizationRequired;
    if (state.phase != .streaming or table.contract != .external_snapshot_lifecycle or definition.events != activation_event or after.event != 0)
        return error.ChangefeedCapabilityUnsupported;
    const namespace = control.catalogRootNamespace(table);
    const next = if (original != null and original.?.advances.len == 0) null else try observed.activationAfter(after.sequence);
    if (next) |event| {
        const position: control.Position = .{ .sequence = event.sequence };
        if (original) |receipt| {
            if (receipt.advances.len != 1 or !std.mem.eql(u8, &receipt.advances[0].namespace, &namespace) or
                receipt.advances[0].position.order(position) != .eq or receipt.advances[0].snapshot != null)
                return error.ChangefeedResynchronizationRequired;
        }
        const identity = control.hash(.{ .domain = "antfly:publication-activation-feed-event:v1", .consumer = state.consumerId(), .binding = table.binding, .event = event });
        const body = try std.json.Stringify.valueAlloc(a, .{
            .version = @as(u8, 1),
            .events = .{.{
                .kind = "publication_activated",
                .event_id = try hex(a, identity),
                .table_id = try std.fmt.allocPrint(a, "{d}", .{table.table_id}),
                .generation = try std.fmt.allocPrint(a, "{d}", .{event.generation}),
                .published_at_ms = event.published_at_ms,
                .publication = try hex(a, event.publication_digest),
            }},
            .transaction = @as(?u8, null),
        }, .{});
        if (body.len > limits.bytes) return error.ChangefeedBackpressure;
        return .{
            .body = body,
            .events = 1,
            .proof = control.hash(.{ .domain = "antfly:publication-activation-feed-receipt:v1", .position = position, .event = identity }),
            .advances = try a.dupe(control.Advance, &.{.{ .namespace = namespace, .position = position }}),
            .provider_context = try a.dupe(u8, try state.contextAt(before)),
        };
    }
    if (original) |receipt| if (receipt.advances.len != 0) return error.ChangefeedResynchronizationRequired;
    const body = "{\"version\":1,\"events\":[],\"transaction\":null}";
    if (body.len > limits.bytes) return error.ChangefeedBackpressure;
    return .{
        .body = body,
        .events = 0,
        .proof = control.hash(.{ .domain = "antfly:publication-activation-feed-heartbeat:v1", .consumer = state.consumerId(), .before = before }),
        .advances = &.{},
        .provider_context = try a.dupe(u8, try state.contextAt(before)),
    };
}

test "external lake publication lifecycle replays committed activation after a later publication" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const table: control.Table = .{ .table_id = 9, .binding = @splat(3), .policy_revision = 1, .contract = .external_snapshot_lifecycle };
    const cursor: control.Digest = @splat(1);
    const state: control.State = .{
        .definition = .{ .tables = &.{table}, .owner = @splat(2), .authorization = @splat(3), .events = activation_event },
        .phase = .streaming,
        .acknowledged_cursor = cursor,
    };
    const first: lifecycle.Activation = .{ .sequence = 1, .generation = 4, .published_at_ms = 102, .publication_digest = @splat(4) };
    const later: lifecycle.Activation = .{ .sequence = 2, .generation = 5, .published_at_ms = 103, .publication_digest = @splat(5) };
    const page = try render(a, state, table, .{}, .{ .activation_history = &.{first} }, cursor, null, .{});
    const receipt: control.Delivery = .{ .sequence = 1, .before = cursor, .cursor = @splat(3), .authority = page.proof, .issued_at = 104, .advances = page.advances };
    const replay = try render(a, state, table, .{}, .{ .activation_history = &.{ first, later } }, cursor, receipt, .{});
    try std.testing.expectEqualStrings(page.body, replay.body);
    try std.testing.expectEqual(page.proof, replay.proof);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, render(a, state, table, .{}, .{ .activation_history = &.{later} }, cursor, receipt, .{}));
}
