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

//! Resolve logical images for a catalog-committed Iceberg WAL group.
const std = @import("std");
const local = @import("antfly_local_sources");
const ingestion = @import("../serverless/lake_ingestion.zig");
const lookup = @import("lake_sql_cursor.zig").lookupSnapshotRow;
const A = std.mem.Allocator;
const Value = std.json.Value;
const CatalogTable = local.serverless_external_source_mod.lake_catalog.types.Table;
const Options = @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions;
const Context = local.api_operation.RequestContext;
const Table = local.sql_catalog.Table;
const projection = local.api_local_table_writes.table_write_source.changefeed_projection;

pub const Images = struct { key: []const u8, before: ?Value, after: ?Value };

/// The caller supplies schema views bound to the two exact snapshots and an
/// arena for the resulting images. A finalized source bootstrap is a row
/// commit here. The consumer's own snapshot bootstrap uses a separate scan.
pub fn read(a: A, committed: ingestion.CommittedWalGroup, metadata: CatalogTable, before_table: ?Table, after_table: Table, context: Context, options: Options) ![]const Images {
    const batch = committed.group.batch;
    const interval = committed.interval;
    if ((interval.parent != null) != (before_table != null)) return error.ChangefeedResynchronizationRequired;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var seen: std.StringHashMap(void) = .init(scratch);
    var output: std.ArrayList(Images) = .empty;
    for (batch.changes) |change| {
        try context.ensureActive();
        const identity = try ingestion.rowId(scratch, batch.key_fields, change.row);
        const entry = try seen.getOrPut(identity);
        if (entry.found_existing) continue;
        const before = if (interval.parent) |parent|
            try lookup(a, before_table.?, parent, metadata, batch.key_fields, change.row, context, options)
        else
            null;
        const after = try lookup(a, after_table, interval.snapshot, metadata, batch.key_fields, change.row, context, options);
        if (before == null and after == null) continue;
        try output.append(a, .{ .key = try a.dupe(u8, identity), .before = before, .after = after });
    }
    return output.toOwnedSlice(a);
}

pub const Effect = struct { key: []const u8, change: projection.Effect };

/// Public delivery applies policy to both full images before evaluating the
/// feed selector. The policy and selector are bound to the same table.
pub fn project(a: A, selector: anytype, mode: projection.Projection, images: []const Images, policy: anytype, context: Context) ![]const Effect {
    var output: std.ArrayList(Effect) = .empty;
    errdefer output.deinit(a);
    for (images) |image| {
        try context.ensureActive();
        const authorized = try policy.imagesFor(a, selector.table_id, image.key, image.before, image.after, context);
        const effect = (try projection.project(a, selector, mode, authorized.before, authorized.after)) orelse continue;
        try output.append(a, .{ .key = image.key, .change = effect });
    }
    return output.toOwnedSlice(a);
}

pub const Next = struct {
    position: ingestion.CommittedWalPosition,
    images: []const Images,
};

/// Resolve one logical commit from a durable position. The schema resolver
/// supplies the relation view for each physical snapshot selected by the
/// catalog. The returned position is suitable for acknowledgment after the
/// whole transaction is delivered.
pub fn readNext(a: A, binding: local.serverless_external_source_catalog_binding.Binding, options: Options, metadata: CatalogTable, after: ingestion.CommittedWalPosition, schemas: anytype, context: Context) !?Next {
    const interval = (try ingestion.nextCommittedWalInterval(a, metadata, after)) orelse return null;
    const normalized = try context.platformDeadline();
    const catalog_context: local.serverless_external_source_mod.lake_catalog.types.Context = .{
        .io = options.io,
        .deadline_ns = normalized.deadline_ns,
        .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn),
    };
    const committed = try ingestion.readCommittedWalGroup(a, binding, options, catalog_context, metadata, interval.snapshot);
    const before_table = if (interval.parent) |parent| try schemas.tableAt(a, parent, metadata, context) else null;
    const after_table = try schemas.tableAt(a, interval.snapshot, metadata, context);
    const images = try read(a, committed, metadata, before_table, after_table, context, options);
    return .{
        .position = .{ .table_uuid = try a.dupe(u8, after.table_uuid), .snapshot = interval.snapshot, .through = interval.through },
        .images = images,
    };
}

const StreamPosition = struct {
    version: u8 = 1,
    scope: [32]u8,
    table_id: u64,
    position: ingestion.CommittedWalPosition,
};

fn validatePosition(position: ingestion.CommittedWalPosition) !void {
    if (position.table_uuid.len == 0 or position.table_uuid.len > 128 or !std.unicode.utf8ValidateSlice(position.table_uuid))
        return error.ChangefeedResynchronizationRequired;
    if (position.snapshot) |id| {
        if (id <= 0) return error.ChangefeedResynchronizationRequired;
    } else if (position.through != 0) return error.ChangefeedResynchronizationRequired;
}

/// Private continuation for the delivery receipt. The caller derives scope
/// from the enrolled feed identity and epoch and obtains the value from
/// metadata, so it carries no authorization on its own.
pub fn encodePosition(a: A, scope: [32]u8, table_id: u64, position: ingestion.CommittedWalPosition) ![]const u8 {
    if (table_id == 0) return error.ChangefeedResynchronizationRequired;
    try validatePosition(position);
    const json = try std.json.Stringify.valueAlloc(a, StreamPosition{ .scope = scope, .table_id = table_id, .position = position }, .{});
    defer a.free(json);
    const codec = std.base64.url_safe_no_pad.Encoder;
    return codec.encode(try a.alloc(u8, codec.calcSize(json.len)), json);
}

pub const TablePosition = struct {
    table_id: u64,
    position: ingestion.CommittedWalPosition,
};

const StreamVector = struct {
    version: u8 = 2,
    scope: [32]u8,
    tables: []const TablePosition,
};

fn validateVector(tables: []const TablePosition) !void {
    if (tables.len == 0 or tables.len > control.max_tables) return error.ChangefeedResynchronizationRequired;
    for (tables, 0..) |entry, index| {
        if (entry.table_id == 0 or (index != 0 and tables[index - 1].table_id >= entry.table_id))
            return error.ChangefeedResynchronizationRequired;
        try validatePosition(entry.position);
    }
}

pub fn encodeVector(a: A, scope: [32]u8, tables: []const TablePosition) ![]const u8 {
    try validateVector(tables);
    const bytes = try std.json.Stringify.valueAlloc(a, StreamVector{ .scope = scope, .tables = tables }, .{});
    defer a.free(bytes);
    const codec = std.base64.url_safe_no_pad.Encoder;
    const len = codec.calcSize(bytes.len);
    if (len > control.max_provider_context_bytes) return error.ChangefeedBackpressure;
    return codec.encode(try a.alloc(u8, len), bytes);
}

fn parseContext(comptime T: type, a: A, bytes: []const u8) !T {
    return std.json.parseFromSliceLeaky(T, a, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ChangefeedResynchronizationRequired,
    };
}

/// Return a sorted vector for both the original one-table encoding and the
/// multi-table successor. Persisted cursor bytes remain valid unchanged.
fn decodeVector(a: A, scope: [32]u8, text: []const u8) ![]const TablePosition {
    if (text.len == 0 or text.len > control.max_provider_context_bytes) return error.ChangefeedResynchronizationRequired;
    const codec = std.base64.url_safe_no_pad.Decoder;
    const len = codec.calcSizeForSlice(text) catch return error.ChangefeedResynchronizationRequired;
    const bytes = try a.alloc(u8, len);
    codec.decode(bytes, text) catch return error.ChangefeedResynchronizationRequired;
    const raw = try parseContext(Value, a, bytes);
    if (raw != .object) return error.ChangefeedResynchronizationRequired;
    const version = raw.object.get("version") orelse return error.ChangefeedResynchronizationRequired;
    if (version != .integer) return error.ChangefeedResynchronizationRequired;
    const tables = switch (version.integer) {
        1 => blk: {
            const single = try parseContext(StreamPosition, a, bytes);
            if (!std.mem.eql(u8, &single.scope, &scope)) return error.ChangefeedResynchronizationRequired;
            break :blk try a.dupe(TablePosition, &.{.{ .table_id = single.table_id, .position = single.position }});
        },
        2 => blk: {
            const vector = try parseContext(StreamVector, a, bytes);
            if (!std.mem.eql(u8, &vector.scope, &scope)) return error.ChangefeedResynchronizationRequired;
            break :blk vector.tables;
        },
        else => return error.ChangefeedResynchronizationRequired,
    };
    try validateVector(tables);
    return tables;
}

pub fn decodePosition(a: A, scope: [32]u8, table_id: u64, text: []const u8) !ingestion.CommittedWalPosition {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for (try decodeVector(arena.allocator(), scope, text)) |entry| {
        if (entry.table_id != table_id) continue;
        var position = entry.position;
        position.table_uuid = try a.dupe(u8, position.table_uuid);
        return position;
    }
    return error.ChangefeedResynchronizationRequired;
}

fn advancePosition(a: A, scope: [32]u8, table_id: u64, text: []const u8, position: ingestion.CommittedWalPosition) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const tables = try scratch.dupe(TablePosition, try decodeVector(scratch, scope, text));
    for (tables) |*entry| {
        if (entry.table_id != table_id) continue;
        if (!std.mem.eql(u8, entry.position.table_uuid, position.table_uuid) or position.through <= entry.position.through)
            return error.ChangefeedResynchronizationRequired;
        entry.position = position;
        if (tables.len == 1) return encodePosition(a, scope, table_id, position);
        return encodeVector(a, scope, tables);
    }
    return error.ChangefeedResynchronizationRequired;
}

const control = @import("../metadata/changefeed_control.zig");
const native = @import("../metadata/native_changefeed_delivery.zig");
const delivery = @import("../metadata/changefeed_delivery.zig");
const wire_json = @import("../metadata/changefeed_json.zig");

pub const Page = struct { body: []const u8, events: u32, next_ordinal: u32, complete: bool, proof: control.Digest };

/// Encode a bounded fragment of one committed transaction. A replay supplies
/// the original ending ordinal, independent of the current request limits.
pub fn encodePage(a: A, scope: control.Digest, table_id: u64, position: ingestion.CommittedWalPosition, effects: []const Effect, start: u32, replay_end: ?u32, limits: delivery.Limits) !Page {
    try limits.validate();
    const end = replay_end orelse std.math.cast(u32, effects.len) orelse return error.ChangefeedBackpressure;
    if (start > end or end > effects.len) return error.ChangefeedResynchronizationRequired;
    const transaction = control.hash(.{ .domain = "antfly:iceberg-feed-transaction:v1", .scope = scope, .table_id = table_id, .position = position });
    const transaction_id = try native.hex(a, transaction);
    const table = try std.fmt.allocPrint(a, "{d}", .{table_id});
    const max_overhead = try wire_json.size(native.Envelope{ .events = &.{}, .transaction = .{ .transaction_id = transaction_id, .complete = false } });
    var events: std.ArrayList(wire_json.Raw) = .empty;
    var total = max_overhead;
    var ordinal = start;
    while (ordinal < end) : (ordinal += 1) {
        const effect = effects[ordinal];
        const event: native.Event = .{
            .event_id = try native.hex(a, control.hash(.{ .domain = "antfly:iceberg-feed-event:v1", .transaction = transaction, .key = effect.key })),
            .transaction_id = transaction_id,
            .table_id = table,
            .key = try native.key(a, effect.key),
            .operation = effect.change.operation,
            .version = try native.hex(a, control.hash(.{ .domain = "antfly:iceberg-feed-version:v1", .transaction = transaction, .key = effect.key })),
            .before = if (effect.change.before) |value| .{ .a = a, .value = value } else null,
            .after = if (effect.change.after) |value| .{ .a = a, .value = value } else null,
        };
        const bytes = try wire_json.size(event);
        if (events.items.len == limits.events or total +| bytes +| @as(u64, @intFromBool(events.items.len != 0)) > limits.bytes) {
            if (replay_end != null or ordinal == start) return error.ChangefeedBackpressure;
            break;
        }
        total += bytes + @intFromBool(events.items.len != 0);
        try events.append(a, .{ .value = try std.json.Stringify.valueAlloc(a, event, .{}) });
    }
    const complete = ordinal == effects.len;
    const envelope: native.Envelope = .{
        .events = events.items,
        .transaction = if (effects.len != 0) .{ .transaction_id = transaction_id, .complete = complete } else null,
    };
    if (try wire_json.size(envelope) > limits.bytes) return error.ChangefeedBackpressure;
    return .{
        .body = try std.json.Stringify.valueAlloc(a, envelope, .{}),
        .events = @intCast(events.items.len),
        .next_ordinal = ordinal,
        .complete = complete,
        .proof = control.hash(.{ .domain = "antfly:iceberg-feed-page:v1", .transaction = transaction, .start = start, .end = ordinal, .complete = complete }),
    };
}

pub fn feedScope(state: control.State) control.Digest {
    return control.hash(.{ .domain = "antfly:iceberg-feed-scope:v1", .consumer = state.consumerId(), .definition = state.definition.?.identity() });
}

/// One catalog-backed root. The delivery service persists the receipt before
/// exposing the body and reconstructs the same fragment on retry.
pub fn Provider(comptime Schemas: type, comptime Policy: type) type {
    return struct {
        namespace: control.Namespace,
        binding: local.serverless_external_source_catalog_binding.Binding,
        options: Options,
        metadata: CatalogTable,
        schemas: Schemas,
        policy: *Policy,
        provider_context_override: ?[]const u8 = null,
        const Self = @This();

        pub fn authorize(self: *Self, state: control.State, request: Context) !void {
            try self.policy.authorize(state, request);
        }

        pub fn nowSeconds(self: *Self, request: Context) !u64 {
            return self.policy.nowSeconds(request);
        }

        pub fn prepare(self: *Self, a: A, state: control.State, before: control.Digest, limits: delivery.Limits, request: Context) !delivery.Prepared {
            return self.page(a, state, before, null, limits, request);
        }

        pub fn replay(self: *Self, a: A, state: control.State, receipt: control.Delivery, limits: delivery.Limits, request: Context) !delivery.Prepared {
            return self.page(a, state, receipt.before, receipt, limits, request);
        }

        fn heartbeat(a: A, feed: control.Digest, before: control.Digest, saved: []const u8) !delivery.Prepared {
            return .{
                .body = try std.json.Stringify.valueAlloc(a, native.Envelope{ .events = &.{}, .transaction = null }, .{}),
                .events = 0,
                .proof = control.hash(.{ .domain = "antfly:iceberg-feed-heartbeat:v1", .scope = feed, .before = before, .position = saved }),
                .advances = &.{},
                .provider_context = try a.dupe(u8, saved),
            };
        }

        fn bootstrap(self: *Self, a: A, state: control.State, root: control.Root, selector: control.Table, position: ingestion.CommittedWalPosition, saved: []const u8, other_roots_done: bool, target: ?control.BootstrapPosition, limits: delivery.Limits, request: Context) !delivery.Prepared {
            try limits.validate();
            const previous = root.snapshot_delivered;
            if (previous.done) return error.ChangefeedBackpressure;
            const keys = (self.binding.row_identity orelse return error.ChangefeedCapabilityUnsupported).key_columns;
            const cut = root.snapshot orelse return error.ChangefeedResynchronizationRequired;
            const feed = feedScope(state);
            const table_id = try std.fmt.allocPrint(a, "{d}", .{selector.table_id});
            const mode: projection.Projection = if (state.definition.?.projection == .rows) .rows else .keys_and_versions;
            var continuation = previous.continuation;
            var done = position.snapshot == null;
            var events: std.ArrayList(wire_json.Raw) = .empty;
            var occupied = try wire_json.size(native.SnapshotEnvelope{ .events = &.{}, .snapshot = .{ .complete = false } });
            if (position.snapshot) |id| {
                const table = try self.schemas.tableAt(a, id, self.metadata, request);
                const fields = try a.alloc([]const u8, table.columns.len);
                for (table.columns, fields) |column, *field| field.* = column.name;
                const scan = @import("lake_sql_cursor.zig");
                const cursor = try scan.openSnapshot(a, table, id, self.metadata, .{ .fields = fields, .after = if (previous.continuation.len == 0) null else previous.continuation, .limit = 1 }, request, self.options);
                defer cursor.close(cursor.ptr);
                for (0..1024) |_| {
                    try request.ensureActive();
                    if (target) |end| if (!end.done and std.mem.eql(u8, end.continuation, continuation)) break;
                    const page_result = try cursor.next(cursor.ptr, a, 1);
                    defer page_result.deinit();
                    if (page_result.rows.len > 1) return error.ChangefeedResynchronizationRequired;
                    if (page_result.rows.len == 1) {
                        const row = page_result.rows[0];
                        const identity = try ingestion.rowId(a, keys, row.value);
                        const visible = try self.policy.snapshotImageFor(a, state, selector.table_id, identity, row.value, request);
                        if (try projection.project(a, selector, mode, .{}, visible)) |effect| {
                            const event: native.SnapshotEvent = .{
                                .event_id = try native.hex(a, control.hash(.{ .domain = "antfly:iceberg-bootstrap-event:v1", .scope = feed, .pin = cut.digest, .row = row.id })),
                                .table_id = table_id,
                                .key = try native.key(a, identity),
                                .version = try native.hex(a, control.hash(.{ .domain = "antfly:iceberg-bootstrap-version:v1", .scope = feed, .pin = cut.digest, .row = row.id })),
                                .after = if (effect.after) |image| .{ .a = a, .value = image } else null,
                            };
                            const bytes = try wire_json.size(event);
                            if (events.items.len == limits.events or occupied +| bytes +| @as(u64, @intFromBool(events.items.len != 0)) > limits.bytes) {
                                if (target != null or std.mem.eql(u8, continuation, previous.continuation)) return error.ChangefeedBackpressure;
                                break;
                            }
                            occupied += bytes + @intFromBool(events.items.len != 0);
                            try events.append(a, .{ .value = try std.json.Stringify.valueAlloc(a, event, .{}) });
                        }
                    }
                    if (page_result.after) |next| {
                        if (next.len > 4096 or std.mem.eql(u8, next, continuation)) return error.ChangefeedResynchronizationRequired;
                        continuation = try a.dupe(u8, next);
                    } else {
                        done = true;
                        break;
                    }
                }
            }
            const next: control.BootstrapPosition = .{ .page = try std.math.add(u64, previous.page, 1), .continuation = continuation, .done = done };
            if (target) |expected| if (expected.page != next.page or expected.done != next.done or !std.mem.eql(u8, expected.continuation, next.continuation))
                return error.ChangefeedResynchronizationRequired;
            return .{
                .body = try std.json.Stringify.valueAlloc(a, native.SnapshotEnvelope{ .events = events.items, .snapshot = .{ .complete = done and other_roots_done } }, .{}),
                .events = @intCast(events.items.len),
                .proof = control.hash(.{ .domain = "antfly:iceberg-bootstrap-page:v1", .scope = feed, .pin = cut.digest, .previous = previous, .next = next }),
                .advances = try a.dupe(control.Advance, &.{.{ .namespace = root.namespace, .position = .{ .sequence = root.start }, .snapshot = next }}),
                .provider_context = try a.dupe(u8, saved),
            };
        }

        fn page(self: *Self, a: A, state: control.State, before: control.Digest, original: ?control.Delivery, limits: delivery.Limits, request: Context) !delivery.Prepared {
            try request.ensureActive();
            if (state.phase != .streaming and state.phase != .bootstrap) return error.ChangefeedCapabilityUnsupported;
            const definition = state.definition orelse return error.ChangefeedResynchronizationRequired;
            const table_id = std.mem.readInt(u64, self.namespace[0..8], .big);
            const selector = for (definition.tables) |value| {
                if (value.table_id == table_id) break value;
            } else return error.ChangefeedResynchronizationRequired;
            if (selector.contract != .writable_iceberg_rows) return error.ChangefeedResynchronizationRequired;
            const feed = feedScope(state);
            const saved = self.provider_context_override orelse try state.contextAt(before);
            const position = try decodePosition(a, feed, table_id, saved);
            const roots = try state.rootsAt(a, before);
            const root = for (roots) |root| {
                if (std.mem.eql(u8, &root.namespace, &self.namespace)) break root;
            } else return error.ChangefeedResynchronizationRequired;
            if (state.phase == .bootstrap) {
                const target = if (original) |receipt| blk: {
                    if (receipt.advances.len != 1 or !std.mem.eql(u8, &receipt.advances[0].namespace, &root.namespace)) return error.ChangefeedResynchronizationRequired;
                    break :blk receipt.advances[0].snapshot orelse return error.ChangefeedResynchronizationRequired;
                } else null;
                const other_roots_done = for (roots) |item| {
                    if (!std.mem.eql(u8, &item.namespace, &root.namespace) and !item.snapshot_delivered.done) break false;
                } else true;
                return self.bootstrap(a, state, root, selector, position, saved, other_roots_done, target, limits, request);
            }
            const start = root.delivered;
            if (start.sequence != position.through) return error.ChangefeedResynchronizationRequired;
            const target = if (original) |receipt| blk: {
                if (receipt.advances.len == 0) return heartbeat(a, feed, before, saved);
                if (receipt.advances.len != 1 or !std.mem.eql(u8, &receipt.advances[0].namespace, &self.namespace) or receipt.advances[0].snapshot != null) return error.ChangefeedResynchronizationRequired;
                break :blk receipt.advances[0].position;
            } else null;
            const next = (try readNext(a, self.binding, self.options, self.metadata, position, self.schemas, request)) orelse {
                if (original != null or start.event != 0) return error.ChangefeedResynchronizationRequired;
                return heartbeat(a, feed, before, saved);
            };
            const mode: projection.Projection = switch (definition.projection) {
                .rows => .rows,
                .keys_and_versions => .keys_and_versions,
            };
            const effects = try project(a, selector, mode, next.images, self.policy, request);
            const end = if (target) |bound| if (bound.sequence == start.sequence) bound.event else std.math.cast(u32, effects.len) orelse return error.ChangefeedBackpressure else null;
            const encoded = try encodePage(a, feed, table_id, next.position, effects, start.event, end, limits);
            const advance: control.Position = if (encoded.complete) .{ .sequence = next.position.through } else .{ .sequence = start.sequence, .event = encoded.next_ordinal };
            if (target) |bound| if (advance.order(bound) != .eq) return error.ChangefeedResynchronizationRequired;
            const continuation = if (encoded.complete) try advancePosition(a, feed, table_id, saved, next.position) else try a.dupe(u8, saved);
            return .{
                .body = encoded.body,
                .events = encoded.events,
                .proof = encoded.proof,
                .advances = try a.dupe(control.Advance, &.{.{ .namespace = self.namespace, .position = advance }}),
                .provider_context = continuation,
            };
        }
    };
}

test "external lake filesystem WAL feed context advances one table independently" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const scope: control.Digest = @splat(9);
    const first: ingestion.CommittedWalPosition = .{ .table_uuid = "table-one", .snapshot = 10, .through = 3 };
    const next: ingestion.CommittedWalPosition = .{ .table_uuid = "table-one", .snapshot = 11, .through = 5 };
    const other: ingestion.CommittedWalPosition = .{ .table_uuid = "table-two", .snapshot = 21, .through = 7 };
    const saved = try encodeVector(a, scope, &.{ .{ .table_id = 1, .position = first }, .{ .table_id = 2, .position = other } });
    const advanced = try advancePosition(a, scope, 1, saved, next);
    try std.testing.expectEqual(@as(u64, 5), (try decodePosition(a, scope, 1, advanced)).through);
    try std.testing.expectEqual(@as(u64, 7), (try decodePosition(a, scope, 2, advanced)).through);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, advancePosition(a, scope, 1, advanced, first));
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, encodeVector(a, scope, &.{ .{ .table_id = 1, .position = first }, .{ .table_id = 1, .position = other } }));
}
