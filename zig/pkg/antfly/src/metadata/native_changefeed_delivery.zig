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

//! Native JSON document provider over an authoritative root-local commit.
//! The policy adapter must authorize the principal/replication privilege, live
//! table incarnation, COMPLETE selected root set and exact policy revision, and
//! authorize/redact BOTH historical images. This provider cannot substitute for
//! that authority. Coordinated/legacy frames require separate completion proof;
//! relational tuples, snapshots and lifecycle sources have distinct providers.
const std = @import("std");
const local = @import("antfly_local_sources").api_local_table_writes;
const page_types = local.table_write_source.history_page;
const projection = local.table_write_source.changefeed_projection;
const native_root = @import("native_changefeed_root.zig");
const control = @import("changefeed_control.zig");
const delivery = @import("changefeed_delivery.zig");
const continuation = @import("changefeed_projection_cursor.zig");
const A = std.mem.Allocator;
const Digest = control.Digest;
const Value = std.json.Value;
pub const Images = struct { before: projection.Image, after: projection.Image };
fn equal(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
fn hex(a: A, digest: Digest) ![]const u8 {
    const text = std.fmt.bytesToHex(digest, .lower);
    return a.dupe(u8, &text);
}
fn parseImage(a: A, raw: ?[]const u8) !?Value {
    const bytes = raw orelse return null;
    return std.json.parseFromSliceLeaky(Value, a, bytes, .{ .allocate = .alloc_always, .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ChangefeedCapabilityUnsupported,
    };
}
const json = @import("changefeed_json.zig");
const Json = json.Value;
const RawJson = json.Raw;
const Key = struct { encoding: enum { utf8, base64url }, value: []const u8 };
fn key(a: A, bytes: []const u8) !Key {
    if (std.unicode.utf8ValidateSlice(bytes)) return .{ .encoding = .utf8, .value = bytes };
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const value = try a.alloc(u8, encoder.calcSize(bytes.len));
    _ = encoder.encode(value, bytes);
    return .{ .encoding = .base64url, .value = value };
}
const Event = struct {
    kind: enum { document_committed } = .document_committed,
    event_id: []const u8,
    transaction_id: []const u8,
    table_id: []const u8,
    key: Key,
    operation: projection.Operation,
    /// A feed-scoped committed version; not a document conditional-write token.
    version: []const u8,
    before: ?Json,
    after: ?Json,
};
const Transaction = struct { transaction_id: []const u8, complete: bool };
const Envelope = struct { version: u8 = 1, events: []const RawJson, transaction: ?Transaction };
fn size(a: A, value: anytype) !u64 {
    _ = a;
    return json.size(value);
}

/// Policy methods: authorize(state, request), nowSeconds(request), and
/// images(arena, state, header, authored_key, before_value, after_value, request).
/// images must use these historical values, never a current-row lookup.
pub fn Provider(comptime Policy: type) type {
    return struct {
        root: *native_root.Adapter,
        policy: *Policy,
        raw_bytes_limit: u32 = page_types.max_source_bytes,
        const Self = @This();
        pub fn authorize(self: *Self, state: control.State, request: anytype) !void {
            try self.policy.authorize(state, request);
            _ = try self.root.validateScope(state);
            if (state.definition.?.tables.len != 1 or state.roots.len != 1 or !equal(state.roots[0].namespace, self.root.namespace)) return error.ChangefeedCapabilityUnsupported;
            if (self.raw_bytes_limit == 0 or self.raw_bytes_limit > page_types.max_source_bytes) return error.InvalidChangefeed;
        }
        pub fn nowSeconds(self: *Self, request: anytype) !u64 {
            return self.policy.nowSeconds(request);
        }
        fn scope(state: control.State) Digest {
            return control.hash(.{ .domain = "antfly:native-document-projection:v1", .consumer = state.consumerId(), .definition = state.definition.?.identity() });
        }
        pub fn prepare(self: *Self, a: A, state: control.State, before: Digest, limits: delivery.Limits, request: anytype) !delivery.Prepared {
            return self.render(a, state, before, null, limits, request);
        }
        pub fn replay(self: *Self, a: A, state: control.State, receipt: control.Delivery, limits: delivery.Limits, request: anytype) !delivery.Prepared {
            return self.render(a, state, receipt.before, receipt, limits, request);
        }
        fn render(self: *Self, a: A, state: control.State, before: Digest, original: ?control.Delivery, limits: delivery.Limits, request: anytype) !delivery.Prepared {
            try limits.validate();
            try self.authorize(state, request);
            const roots = try state.rootsAt(a, before);
            const context = try state.contextAt(before);
            const seen = try continuation.decode(a, scope(state), roots, context);
            if (original) |receipt| if (receipt.advances.len == 0) {
                if (!std.mem.eql(u8, receipt.provider_context, context)) return error.ChangefeedResynchronizationRequired;
                return heartbeat(a, state, before, context, limits);
            };
            const start = roots[0].delivered;
            var raw_events = limits.events;
            var target: ?control.Position = null;
            if (original) |receipt| {
                if (receipt.advances.len != 1 or !equal(receipt.advances[0].namespace, self.root.namespace) or receipt.advances[0].snapshot != null) return error.ChangefeedResynchronizationRequired;
                target = receipt.advances[0].position;
                if (target.?.event != 0) {
                    if (target.?.sequence != start.sequence or target.?.event <= start.event) return error.ChangefeedResynchronizationRequired;
                    raw_events = target.?.event - start.event;
                } else {
                    if (target.?.sequence != (std.math.add(u64, start.sequence, 1) catch return error.ChangefeedResynchronizationRequired)) return error.ChangefeedResynchronizationRequired;
                    raw_events = 1024;
                }
                if (raw_events > 1024) return error.ChangefeedResynchronizationRequired;
            }
            var page = try self.root.read(a, state, before, try self.nowSeconds(request), raw_events, self.raw_bytes_limit);
            defer page.deinit();
            const header = page.header orelse {
                if (original != null) return error.ChangefeedResynchronizationRequired;
                return heartbeat(a, state, before, context, limits);
            };
            if (header.transaction_scope != .root_local) return error.ChangefeedCapabilityUnsupported;
            const transaction_id = try hex(a, control.hash(.{ .domain = "antfly:feed-transaction:v1", .scope = scope(state), .namespace = header.namespace, .transaction = header.transaction_id, .sequence = header.sequence }));
            const table_id = try std.fmt.allocPrint(a, "{d}", .{state.definition.?.tables[0].table_id});
            var events: std.ArrayList(RawJson) = .empty;
            var event_bytes: u64 = 0;
            var consumed: u32 = 0;
            const projected: projection.Projection = switch (state.definition.?.projection) {
                .rows => .rows,
                .keys_and_versions => .keys_and_versions,
            };
            for (page.events, 0..) |raw, index| {
                try request.ensureActive();
                const document = try page_types.documentId(a, raw.key);
                const images = try self.policy.images(a, state, header, document, try parseImage(a, raw.before), try parseImage(a, raw.after), request);
                const effect = try projection.project(a, state.definition.?.tables[0], projected, images.before, images.after);
                if (effect) |value| {
                    const ordinal = page.first_ordinal + @as(u32, @intCast(index));
                    const event: Event = .{
                        .event_id = try hex(a, control.hash(.{ .domain = "antfly:feed-event:v1", .scope = scope(state), .namespace = header.namespace, .sequence = header.sequence, .ordinal = ordinal })),
                        .transaction_id = transaction_id,
                        .table_id = table_id,
                        .key = try key(a, document),
                        .operation = value.operation,
                        .version = try hex(a, control.hash(.{ .domain = "antfly:feed-version:v1", .scope = scope(state), .namespace = header.namespace, .sequence = header.sequence, .ordinal = ordinal, .key = document })),
                        .before = if (value.before) |v| .{ .a = a, .value = v } else null,
                        .after = if (value.after) |v| .{ .a = a, .value = v } else null,
                    };
                    const bytes = try size(a, event);
                    const complete = page.first_ordinal + consumed + 1 == page.frame_events;
                    const envelope_bytes = try size(a, Envelope{ .events = &.{}, .transaction = .{ .transaction_id = transaction_id, .complete = complete } });
                    const total = envelope_bytes + event_bytes + bytes + events.items.len;
                    if (total > limits.bytes or events.items.len >= limits.events) {
                        if (original != null or consumed == 0) return error.ChangefeedBackpressure;
                        break;
                    }
                    try events.append(a, .{ .value = try std.json.Stringify.valueAlloc(a, event, .{}) });
                    event_bytes += bytes;
                }
                consumed += 1;
            }
            const complete = page.first_ordinal + consumed == page.frame_events;
            const next: control.Position = if (complete) .{ .sequence = header.sequence } else .{ .sequence = start.sequence, .event = page.first_ordinal + consumed };
            if (target) |expected| if (next.order(expected) != .eq) return error.ChangefeedResynchronizationRequired;
            const relevant = seen[0] or events.items.len != 0;
            seen[0] = relevant and !complete;
            roots[0].delivered = next;
            const next_context = try continuation.encode(a, scope(state), roots, seen);
            const envelope: Envelope = .{ .events = events.items, .transaction = if (relevant) .{ .transaction_id = transaction_id, .complete = complete } else null };
            if (try size(a, envelope) > limits.bytes) return error.ChangefeedBackpressure;
            const advances = try a.dupe(control.Advance, &.{.{ .namespace = self.root.namespace, .position = next }});
            return .{ .body = try std.json.Stringify.valueAlloc(a, envelope, .{}), .events = @intCast(events.items.len), .proof = control.hash(.{ .domain = "antfly:native-document-page:v1", .scope = scope(state), .header = header, .start = start, .next = next, .before_context = context, .after_context = next_context }), .advances = advances, .provider_context = next_context };
        }
        fn heartbeat(a: A, state: control.State, before: Digest, context: []const u8, limits: delivery.Limits) !delivery.Prepared {
            const envelope: Envelope = .{ .events = &.{}, .transaction = null };
            if (try size(a, envelope) > limits.bytes) return error.ChangefeedBackpressure;
            return .{ .body = try std.json.Stringify.valueAlloc(a, envelope, .{}), .events = 0, .advances = &.{}, .provider_context = context, .proof = control.hash(.{ .domain = "antfly:native-document-heartbeat:v1", .scope = scope(state), .before = before, .context = context }) };
        }
    };
}
