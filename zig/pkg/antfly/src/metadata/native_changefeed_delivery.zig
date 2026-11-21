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

//! Native row provider over an authoritative committed history root.
//! The policy adapter must authorize the principal/replication privilege, live
//! table incarnation, COMPLETE selected root set and exact policy revision, and
//! authorize/redact BOTH historical images. This provider cannot substitute for
//! that authority. Coordinated/legacy frames require separate completion proof;
//! derived lifecycle sources have distinct providers.
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
pub fn hex(a: A, digest: Digest) ![]const u8 {
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
const HistoryRows = @import("antfly_local_sources").storage_db_algebraic_relational_row_codec;

const RowImages = union(enum) {
    document,
    relational: ?struct {
        schema: @import("antfly_local_sources").storage_schema.TableSchema,
        layout: HistoryRows.PhysicalLayout,
    },

    fn init(a: A, contract: control.Contract, schema_json: ?[]const u8, rows: usize) !RowImages {
        if (contract == .native_document) return .document;
        if (contract != .native_relational) return error.ChangefeedCapabilityUnsupported;
        const encoded = schema_json orelse return .{ .relational = null };
        const schema_api = @import("antfly_local_sources").schema_mod;
        const schema = try schema_api.deriveRuntimeTableSchema(a, try schema_api.parseValidatedTableSchema(a, encoded));
        const width = schema.relational_columns.len;
        if (schema.storage_mode != .relational or schema.external_base_source != null or width == 0 or width > 1024)
            return error.ChangefeedCapabilityUnsupported;
        if (rows > 65536 / width) return error.HistoryTooLarge;
        return .{ .relational = .{ .schema = schema, .layout = try HistoryRows.PhysicalLayout.init(a, schema) } };
    }

    fn decode(self: *const RowImages, a: A, physical: ?[]const u8) !?Value {
        const image = physical orelse return null;
        return switch (self.*) {
            .document => parseImage(a, image),
            .relational => if (self.relational) |*view| blk: {
                const row = try HistoryRows.ordinalRowView(image, view.schema, &view.layout);
                const json_image = try row.reconstructValueAlloc(a);
                break :blk parseImage(a, json_image);
            } else error.HistoryCorrupt,
        };
    }
};
const json = @import("changefeed_json.zig");
const Json = json.Value;
const RawJson = json.Raw;
const Key = struct { encoding: enum { utf8, base64url }, value: []const u8 };
pub fn key(a: A, bytes: []const u8) !Key {
    if (std.unicode.utf8ValidateSlice(bytes)) return .{ .encoding = .utf8, .value = bytes };
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const value = try a.alloc(u8, encoder.calcSize(bytes.len));
    _ = encoder.encode(value, bytes);
    return .{ .encoding = .base64url, .value = value };
}
pub const Event = struct {
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
pub const SnapshotEvent = struct {
    kind: enum { document_snapshot } = .document_snapshot,
    event_id: []const u8,
    table_id: []const u8,
    key: Key,
    version: []const u8,
    after: ?Json,
};
pub const SnapshotEnvelope = struct {
    version: u8 = 1,
    events: []const RawJson,
    transaction: ?Transaction = null,
    snapshot: struct { complete: bool },
};
const Transaction = struct { transaction_id: []const u8, complete: bool };
pub const Envelope = struct { version: u8 = 1, events: []const RawJson, transaction: ?Transaction };
fn size(a: A, value: anytype) !u64 {
    _ = a;
    return json.size(value);
}

/// Policy methods: authorize(state, request), nowSeconds(request), and
/// images(arena, state, header, authored_key, before_value, after_value, request).
/// images must use these historical values, never a current-row lookup.
pub fn Provider(comptime Policy: type) type {
    return ProviderFor(native_root.Adapter, Policy);
}

/// The factory binds a selected namespace to a route in the request's
/// captured generation. A replay dispatches only to the recorded root.
pub fn ScheduledProvider(comptime Root: type, comptime Factory: type, comptime Policy: type) type {
    return struct {
        factory: Factory,
        policy: *Policy,
        const Self = @This();

        pub fn authorize(self: *Self, state: control.State, request: anytype) !void {
            if (state.roots.len == 0) return error.ChangefeedResynchronizationRequired;
            var root: Root = try self.factory.open(state.roots[0].namespace);
            var provider: ProviderFor(Root, Policy) = .{ .root = &root, .policy = self.policy };
            try provider.authorize(state, request);
        }

        pub fn nowSeconds(self: *Self, request: anytype) !u64 {
            return self.policy.nowSeconds(request);
        }

        pub fn prepare(self: *Self, a: A, state: control.State, before: Digest, limits: delivery.Limits, request: anytype) !delivery.Prepared {
            try self.authorize(state, request);
            if (state.roots.len == 1) {
                var root: Root = try self.factory.open(state.roots[0].namespace);
                var provider: ProviderFor(Root, Policy) = .{ .root = &root, .policy = self.policy };
                return provider.prepare(a, state, before, limits, request);
            }
            const bootstrap_positions = if (state.phase == .bootstrap) try state.rootsAt(a, before) else null;
            const start = state.latest_delivery % state.roots.len;
            var heartbeat: ?delivery.Prepared = null;
            var unavailable = false;
            for (0..state.roots.len) |offset| {
                try request.ensureActive();
                const index = (start + offset) % state.roots.len;
                if (bootstrap_positions) |positions| if (positions[index].snapshot_delivered.done) continue;
                const selected = state.roots[index];
                var root: Root = try self.factory.open(selected.namespace);
                var provider: ProviderFor(Root, Policy) = .{ .root = &root, .policy = self.policy };
                var attempt = std.heap.ArenaAllocator.init(a);
                defer attempt.deinit();
                const page = provider.prepare(attempt.allocator(), state, before, limits, request) catch |err| switch (err) {
                    error.ReadUnavailable => {
                        unavailable = true;
                        continue;
                    },
                    else => return err,
                };
                if (page.advances.len != 0) return copyPage(a, page);
                if (heartbeat == null) heartbeat = try copyPage(a, page);
            }
            if (unavailable) return error.ReadUnavailable;
            return heartbeat orelse if (state.phase == .bootstrap) error.ChangefeedBackpressure else error.ChangefeedResynchronizationRequired;
        }

        fn copyPage(a: A, page: delivery.Prepared) !delivery.Prepared {
            var result = page;
            result.body = try a.dupe(u8, page.body);
            result.provider_context = try a.dupe(u8, page.provider_context);
            const advances = try a.dupe(control.Advance, page.advances);
            for (advances) |*advance| if (advance.snapshot) |*position| {
                position.continuation = try a.dupe(u8, position.continuation);
            };
            result.advances = advances;
            return result;
        }

        pub fn replay(self: *Self, a: A, state: control.State, receipt: control.Delivery, limits: delivery.Limits, request: anytype) !delivery.Prepared {
            try self.authorize(state, request);
            const namespace = switch (receipt.advances.len) {
                0 => state.roots[0].namespace,
                1 => receipt.advances[0].namespace,
                else => return error.ChangefeedResynchronizationRequired,
            };
            _ = try state.rootIndex(namespace);
            var root: Root = try self.factory.open(namespace);
            var provider: ProviderFor(Root, Policy) = .{ .root = &root, .policy = self.policy };
            return provider.replay(a, state, receipt, limits, request);
        }
    };
}

pub fn ProviderFor(comptime Root: type, comptime Policy: type) type {
    return struct {
        root: *Root,
        policy: *Policy,
        raw_bytes_limit: u32 = page_types.max_source_bytes,
        provider_context_override: ?[]const u8 = null,
        const Self = @This();
        pub fn authorize(self: *Self, state: control.State, request: anytype) !void {
            try self.policy.authorize(state, request);
            _ = try self.root.validateScope(state);
            _ = try state.rootIndex(self.root.namespace);
            if (state.roots.len == 1 and state.definition.?.tables.len != 1) return error.ChangefeedCapabilityUnsupported;
            if (state.roots.len != 1 and !@hasDecl(Root, "readProjectedWithContext")) return error.ChangefeedCapabilityUnsupported;
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
        fn snapshotOrdinal(position: control.BootstrapPosition) !u32 {
            if (position.page == 0) return 0;
            const ordinal = std.fmt.parseInt(u32, position.continuation, 10) catch return error.ChangefeedResynchronizationRequired;
            if (ordinal == 0 and !position.done) return error.ChangefeedResynchronizationRequired;
            return ordinal;
        }

        fn renderSnapshot(self: *Self, a: A, state: control.State, root: control.Root, other_roots_done: bool, original: ?control.Delivery, limits: delivery.Limits, request: anytype) !delivery.Prepared {
            if (comptime !@hasDecl(Root, "readBootstrap") or (!@hasDecl(Policy, "snapshotImage") and !@hasDecl(Policy, "snapshotImageFor"))) return error.ChangefeedCapabilityUnsupported;
            const pin = root.snapshot orelse return error.ChangefeedResynchronizationRequired;
            const position = root.snapshot_delivered;
            if (position.done) return error.ChangefeedBackpressure;
            const start = try snapshotOrdinal(position);
            var target: ?control.BootstrapPosition = null;
            var max_rows = limits.events;
            if (original) |receipt| {
                if (receipt.advances.len != 1 or !equal(receipt.advances[0].namespace, root.namespace) or
                    receipt.advances[0].position.order(.{ .sequence = root.start }) != .eq) return error.ChangefeedResynchronizationRequired;
                target = receipt.advances[0].snapshot orelse return error.ChangefeedResynchronizationRequired;
                const stop = try snapshotOrdinal(target.?);
                if (stop < start or stop - start > 1024 or (stop == start and !target.?.done)) return error.ChangefeedResynchronizationRequired;
                max_rows = @max(stop - start, 1);
            }
            const raw_limit = @min(self.raw_bytes_limit, local.table_write_source.history_consumer.max_bootstrap_bytes);
            var page = try self.root.readBootstrap(a, state, start, try self.nowSeconds(request), max_rows, raw_limit, request);
            defer page.deinit();
            if (page.rows.len > max_rows or page.next_row != start + @as(u32, @intCast(page.rows.len)) or (page.rows.len == 0 and !page.done))
                return error.ChangefeedResynchronizationRequired;

            const table = try self.root.validateScope(state);
            const table_id = try std.fmt.allocPrint(a, "{d}", .{table.table_id});
            const scope_id = scope(state);
            const projected: projection.Projection = switch (state.definition.?.projection) {
                .rows => .rows,
                .keys_and_versions => .keys_and_versions,
            };
            const envelope_budget = @max(
                try size(a, SnapshotEnvelope{ .events = &.{}, .snapshot = .{ .complete = true } }),
                try size(a, SnapshotEnvelope{ .events = &.{}, .snapshot = .{ .complete = false } }),
            );
            var events: std.ArrayList(RawJson) = .empty;
            var event_bytes: u64 = 0;
            var consumed: u32 = 0;
            const decoder = try RowImages.init(a, table.contract, page.schema, page.rows.len);
            for (page.rows) |row| {
                try request.ensureActive();
                const document = try page_types.documentId(a, row.key);
                const snapshot_value = try decoder.decode(a, row.value);
                const image = if (@hasDecl(Policy, "snapshotImageFor"))
                    try self.policy.snapshotImageFor(a, state, table.table_id, document, snapshot_value, request)
                else
                    try self.policy.snapshotImage(a, state, document, snapshot_value, request);
                if (try projection.project(a, table, projected, .{}, image)) |effect| {
                    const ordinal = start + consumed;
                    const event: SnapshotEvent = .{
                        .event_id = try hex(a, control.hash(.{ .domain = "antfly:feed-snapshot-event:v1", .scope = scope_id, .pin = pin.digest, .ordinal = ordinal })),
                        .table_id = table_id,
                        .key = try key(a, document),
                        .version = try hex(a, control.hash(.{ .domain = "antfly:feed-snapshot-version:v1", .scope = scope_id, .pin = pin.digest, .ordinal = ordinal, .key = document })),
                        .after = if (effect.after) |value| .{ .a = a, .value = value } else null,
                    };
                    const bytes = try size(a, event);
                    if (envelope_budget + event_bytes + bytes + events.items.len > limits.bytes or events.items.len >= limits.events) {
                        if (original != null or consumed == 0) return error.ChangefeedBackpressure;
                        break;
                    }
                    try events.append(a, .{ .value = try std.json.Stringify.valueAlloc(a, event, .{}) });
                    event_bytes += bytes;
                }
                consumed += 1;
            }
            const next: control.BootstrapPosition = .{
                .page = std.math.add(u64, position.page, 1) catch return error.ChangefeedBackpressure,
                .continuation = try std.fmt.allocPrint(a, "{d}", .{start + consumed}),
                .done = page.done and consumed == page.rows.len,
            };
            if (target) |expected| {
                if (expected.page != next.page or expected.done != next.done or !std.mem.eql(u8, expected.continuation, next.continuation))
                    return error.ChangefeedResynchronizationRequired;
            }
            const body = try std.json.Stringify.valueAlloc(a, SnapshotEnvelope{ .events = events.items, .snapshot = .{ .complete = next.done and other_roots_done } }, .{});
            if (body.len > limits.bytes) return error.ChangefeedBackpressure;
            return .{
                .body = body,
                .events = @intCast(events.items.len),
                .proof = control.hash(.{ .domain = "antfly:feed-snapshot-page:v1", .scope = scope_id, .pin = pin.digest, .from = position, .to = next }),
                .advances = try a.dupe(control.Advance, &.{.{ .namespace = root.namespace, .position = .{ .sequence = root.start }, .snapshot = next }}),
            };
        }

        fn render(self: *Self, a: A, state: control.State, before: Digest, original: ?control.Delivery, limits: delivery.Limits, request: anytype) !delivery.Prepared {
            try limits.validate();
            try self.authorize(state, request);
            const roots = try state.rootsAt(a, before);
            const index = try state.rootIndex(self.root.namespace);
            if (state.phase == .bootstrap) {
                const other_roots_done = for (roots, 0..) |root, i| {
                    if (i != index and !root.snapshot_delivered.done) break false;
                } else true;
                return self.renderSnapshot(a, state, roots[index], other_roots_done, original, limits, request);
            }
            const context = self.provider_context_override orelse try state.contextAt(before);
            const coordinated = try continuation.Coordinated.decode(scope(state), roots, context);
            const seen = if (coordinated == null) try continuation.decode(a, scope(state), roots, context) else try a.alloc(bool, roots.len);
            if (coordinated != null) @memset(seen, false);
            if (original) |receipt| if (receipt.advances.len == 0) {
                if (!std.mem.eql(u8, receipt.provider_context, context)) return error.ChangefeedResynchronizationRequired;
                return heartbeat(a, state, before, context, limits);
            };
            const start = roots[index].delivered;
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
            const now = try self.nowSeconds(request);
            var page = if (@hasDecl(Root, "readProjectedWithContext") and roots.len > 1)
                try self.root.readProjectedWithContext(a, state, before, now, raw_events, self.raw_bytes_limit, request)
            else
                try self.root.readWithContext(a, state, before, now, raw_events, self.raw_bytes_limit, request);
            defer page.deinit();
            const header = page.header orelse {
                if (original != null) return error.ChangefeedResynchronizationRequired;
                if (coordinated != null) return error.ReadUnavailable;
                return heartbeat(a, state, before, context, limits);
            };
            const completion = switch (header.transaction_scope) {
                .root_local => null,
                .coordinated => page.completion orelse return error.ChangefeedCapabilityUnsupported,
                .unknown => return error.ChangefeedCapabilityUnsupported,
            };
            const transaction_identity = if (completion != null)
                control.hash(.{ .domain = "antfly:feed-coordinated-transaction:v1", .scope = scope(state), .transaction = header.transaction_id })
            else
                control.hash(.{ .domain = "antfly:feed-transaction:v1", .scope = scope(state), .namespace = header.namespace, .transaction = header.transaction_id, .sequence = header.sequence });
            const transaction_id = try hex(a, transaction_identity);
            const distributed = completion != null and roots.len > 1;
            if (coordinated) |active| {
                if (!distributed or !equal(active.transaction, transaction_identity) or !equal(active.certificate, completion.?.certificate_digest))
                    return error.ReadUnavailable;
            }
            if (distributed) try continuation.requireCoordinatedHead(roots, page.completion_effects, index, header.sequence, coordinated != null);
            const other_effects_consumed = !distributed or continuation.otherCoordinatedEffectsConsumed(roots, page.completion_effects, index);
            const table = try self.root.validateScope(state);
            const table_id = try std.fmt.allocPrint(a, "{d}", .{table.table_id});
            var events: std.ArrayList(RawJson) = .empty;
            var event_bytes: u64 = 0;
            var consumed: u32 = 0;
            const projected: projection.Projection = switch (state.definition.?.projection) {
                .rows => .rows,
                .keys_and_versions => .keys_and_versions,
            };
            const before_decoder = try RowImages.init(a, table.contract, header.schema_before, page.events.len);
            const after_decoder = try RowImages.init(a, table.contract, header.schema_after, page.events.len);
            for (page.events, 0..) |raw, ordinal_in_page| {
                try request.ensureActive();
                const document = try page_types.documentId(a, raw.key);
                const images = try self.policy.images(a, state, header, document, try before_decoder.decode(a, raw.before), try after_decoder.decode(a, raw.after), request);
                const effect = try projection.project(a, table, projected, images.before, images.after);
                if (effect) |value| {
                    const ordinal = page.first_ordinal + @as(u32, @intCast(ordinal_in_page));
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
                    const complete = other_effects_consumed and page.first_ordinal + consumed + 1 == page.frame_events;
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
            const relevant = (if (coordinated) |active| active.visible else seen[index]) or events.items.len != 0;
            const transaction_complete = complete and other_effects_consumed;
            roots[index].delivered = next;
            seen[index] = relevant and !complete and !distributed;
            const next_context = if (distributed and !transaction_complete)
                try (continuation.Coordinated{ .transaction = transaction_identity, .certificate = completion.?.certificate_digest, .visible = relevant }).encode(a, scope(state), roots)
            else
                try continuation.encode(a, scope(state), roots, seen);
            const envelope: Envelope = .{ .events = events.items, .transaction = if (relevant) .{ .transaction_id = transaction_id, .complete = transaction_complete } else null };
            if (try size(a, envelope) > limits.bytes) return error.ChangefeedBackpressure;
            const advances = try a.dupe(control.Advance, &.{.{ .namespace = self.root.namespace, .position = next }});
            const position_proof = control.hash(.{ .domain = "antfly:native-document-page:v1", .scope = scope(state), .header = header, .start = start, .next = next, .before_context = context, .after_context = next_context });
            const proof = if (completion) |certificate|
                control.hash(.{ .domain = "antfly:native-document-coordinated-page:v1", .position = position_proof, .certificate = certificate })
            else
                position_proof;
            return .{ .body = try std.json.Stringify.valueAlloc(a, envelope, .{}), .events = @intCast(events.items.len), .proof = proof, .advances = advances, .provider_context = next_context };
        }
        fn heartbeat(a: A, state: control.State, before: Digest, context: []const u8, limits: delivery.Limits) !delivery.Prepared {
            const envelope: Envelope = .{ .events = &.{}, .transaction = null };
            if (try size(a, envelope) > limits.bytes) return error.ChangefeedBackpressure;
            return .{ .body = try std.json.Stringify.valueAlloc(a, envelope, .{}), .events = 0, .advances = &.{}, .provider_context = context, .proof = control.hash(.{ .domain = "antfly:native-document-heartbeat:v1", .scope = scope(state), .before = before, .context = context }) };
        }
    };
}
