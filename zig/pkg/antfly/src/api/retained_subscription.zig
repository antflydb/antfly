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

//! Snapshot plus committed replay for bounded retained-summary subscriptions.
//! Per-owner state is retained by the serving session, never by a TTL hold.
const std = @import("std");
const local = @import("antfly_local_sources");
const dataset = local.retained_dataset_types;
const series = local.storage_retained_series;
const codec = local.storage_retained_series_archive;
const wire = local.storage_retained_series_query;
const read = @import("retained_dataset_read.zig");
const registry = @import("retained_subscription_registry.zig");
const owners = @import("../metadata/retained_dataset_owners.zig");
const archive_api = @import("../serverless/datasets/configured_archive.zig");
const archive_read = @import("../serverless/datasets/summary_read.zig");
const closure = @import("../serverless/datasets/closure_manifest.zig");
const operation = local.api_operation;
const platform = @import("antfly_platform").time;
const A = std.mem.Allocator;
const Target = local.system_catalog_domain.Target;
const max_owners = 128;
const max_floors = 32;
const max_rows = 4096;

pub const Create = struct { start_ns: ?[]const u8 = null, end_ns: ?[]const u8 = null, window_ns: ?[]const u8 = null };
pub const Poll = struct { cursor: []const u8 };
const Bounds = struct { start: i128, end: i128 };
const Floor = struct { bounds: Bounds, through: u64 };
const Part = struct {
    group: u64,
    owner_revision: u64,
    root: []const u8 = "",
    cursor: ?series.Cursor = null,
    rows: []const series.Bucket = &.{},
    floors: []const Floor = &.{},
};
const State = struct {
    resource: u64,
    plan: dataset.Id,
    definition: dataset.Id,
    window_ns: ?u64,
    bounds: Bounds,
    clock_ns: i128,
    parts: []Part,
};
const Event = struct {
    type: enum { upsert, remove },
    key: []const u8,
    bucket_start_ns: ?[]const u8 = null,
    dimensions: ?[]const std.json.Value = null,
    values: ?[]const std.json.Value = null,
};

pub fn authorityHash(a: A, principal: []const u8, target: Target) !registry.Digest {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .principal = principal, .target = target }, .{});
    defer a.free(bytes);
    return digest(bytes);
}
fn digest(bytes: []const u8) registry.Digest {
    var value: registry.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &value, .{});
    return value;
}
fn specFor(a: A, definition: dataset.Definition, owner: owners.Owner) !series.Spec {
    return @import("retained_owner_binding.zig").specification(a, definition, owner.partition, owner.incarnation);
}
fn scopeFor(snapshot: read.Snapshot, owner: owners.Owner) owners.ArchiveScope {
    return .{ .resource = snapshot.plan.resource, .generation = snapshot.plan.generation, .plan = snapshot.plan.id, .group = owner.group, .revision = owner.revision };
}
fn requestFor(a: A, bounds: Bounds) !read.Request {
    return .{ .start_ns = try std.fmt.allocPrint(a, "{d}", .{bounds.start}), .end_ns = try std.fmt.allocPrint(a, "{d}", .{bounds.end}) };
}
fn inRange(bounds: Bounds, bucket: i128) bool {
    return bucket >= bounds.start and bucket < bounds.end;
}
fn movingBounds(spec: series.Spec, window: u64, now: i128) !Bounds {
    if (window == 0 or window > spec.retention_ns or window % spec.interval_ns != 0) return error.InvalidDatasetCoverage;
    const end = std.math.add(i128, try spec.bucket(now), spec.interval_ns) catch return error.RetainedSeriesTimeOverflow;
    return .{ .start = std.math.sub(i128, end, window) catch return error.RetainedSeriesTimeOverflow, .end = end };
}
fn parseBound(text: []const u8) !i128 {
    if (text.len == 0 or text.len > 40) return error.InvalidDatasetCoverage;
    return std.fmt.parseInt(i128, text, 10) catch error.InvalidDatasetCoverage;
}
fn cutFor(owner: owners.Owner) !closure.Cut {
    const cut = owner.cut orelse return error.InvalidRetainedOwnerPlan;
    return .{ .closure = cut.closure, .sequence = cut.sequence, .complete_from_ns = try parseBound(cut.complete_from_ns), .admitted_ns = try parseBound(cut.admitted_ns) };
}
fn openedArchive(server: anytype, a: A, snapshot: read.Snapshot, descriptor: local.system_catalog_retained_datasets.Descriptor) !?archive_api.Opened {
    if (snapshot.plan.independent == 0) return null;
    const config = server.cfg.node_config orelse return error.InvalidDatasetBinding;
    return try archive_api.Opened.init(a, config, server.cfg.secret_store, descriptor, false);
}
const ArchiveResolver = struct {
    value: @import("../serverless/datasets/catalog.zig").Location,
    pub fn location(self: @This(), id: dataset.Id) !@TypeOf(self.value) {
        if (!std.mem.eql(u8, &id, &self.value.id)) return error.DatasetLocationUnavailable;
        return self.value;
    }
};
fn capture(server: anytype, a: A, snapshot: read.Snapshot, definition: dataset.Definition, owner: owners.Owner, bounds: Bounds, clock_ns: i128, archive: ?*const archive_api.Opened, ctx: operation.RequestContext, budget: *usize) !Part {
    const input = try requestFor(a, bounds);
    if (owner.independent_receipt) |receipt| {
        const opened = archive orelse return error.InvalidDatasetBinding;
        const io = server.sharedApiIo() orelse return error.BackendRuntimeUnavailable;
        var part = try archive_read.readClosed(a, opened.store(io, &ctx), definition, try specFor(a, definition, owner), .{ .start_ns = input.start_ns, .end_ns = input.end_ns }, .{ .rows = max_rows, .read_clock_ns = clock_ns }, try cutFor(owner), receipt, ArchiveResolver{ .value = opened.location() });
        defer part.deinit();
        const rows = try a.alloc(series.Bucket, part.rows.len);
        for (part.rows, rows) |row, *out| {
            try spend(budget, row.dimensions.len + series.metricBytes(row.metrics) + @sizeOf(series.Bucket));
            out.* = .{ .start_ns = row.start_ns, .rows = row.rows, .dimensions = try a.dupe(u8, row.dimensions), .metrics = try series.ownMetrics(a, row.metrics) };
        }
        return .{ .group = owner.group, .owner_revision = owner.revision, .rows = rows };
    }
    const source = snapshot.source_table orelse return error.RetainedOwnerPlanChanged;
    const raw = try server.readRetainedOwnerRows(a, source.name, owner.group, .{ .scope = scopeFor(snapshot, owner), .start_ns = input.start_ns, .end_ns = input.end_ns, .limit = max_rows, .clock_ns = clock_ns }, ctx);
    try spend(budget, raw.len);
    const reply = try std.json.parseFromSliceLeaky(read.NativeReply, a, raw, .{});
    if (reply.rows.len > max_rows or !std.mem.eql(u8, &reply.cursor.partition, &owner.partition) or !std.mem.eql(u8, &reply.cursor.definition, &snapshot.plan.definition) or reply.cursor.incarnation != owner.incarnation) return error.RetainedSeriesResetRequired;
    return .{ .group = owner.group, .owner_revision = owner.revision, .root = reply.root_incarnation, .cursor = reply.cursor, .rows = reply.rows };
}
fn spend(remaining: *usize, cost: usize) !void {
    if (cost > remaining.*) return error.DatasetPageBudgetExceeded;
    remaining.* -= cost;
}
fn aggregate(a: A, snapshot: read.Snapshot, state: State, ctx: operation.RequestContext) !read.Result {
    const Reader = struct {
        parts: []const Part,
        current: usize = 0,
        pub fn readPartition(self: *@This(), alloc: A, owner: owners.Owner, _: read.Authority, _: dataset.Range, _: operation.RequestContext) !archive_read.Result {
            const part = self.parts[self.current];
            self.current += 1;
            if (part.group != owner.group) return error.RetainedSeriesResetRequired;
            return .{ .arena = std.heap.ArenaAllocator.init(alloc), .rows = part.rows };
        }
    };
    var reader: Reader = .{ .parts = state.parts };
    return read.read(a, snapshot, try requestFor(a, state.bounds), ctx, &reader);
}
fn event(a: A, summary: dataset.Summary, row: series.Bucket) !Event {
    const values = try a.alloc(std.json.Value, summary.measures.len);
    for (values, summary.measures, row.metrics) |*cell, measure, metric| cell.* = switch (try metric.finalize(measure.law)) {
        .null => .null,
        .integer => |value| .{ .string = try std.fmt.allocPrint(a, "{d}", .{value}) },
        .number => |value| .{ .float = value },
    };
    return .{ .type = .upsert, .key = try a.dupe(u8, &codec.key(row)), .bucket_start_ns = try std.fmt.allocPrint(a, "{d}", .{row.start_ns}), .dimensions = try std.json.parseFromSliceLeaky([]std.json.Value, a, row.dimensions, .{ .parse_numbers = false }), .values = values };
}
fn equal(left: series.Bucket, right: series.Bucket) bool {
    if (left.rows != right.rows or left.metrics.len != right.metrics.len or !std.mem.eql(u8, left.dimensions, right.dimensions)) return false;
    for (left.metrics, right.metrics) |l, r| if (l.count != r.count or l.sum != r.sum or l.minimum != r.minimum or l.maximum != r.maximum or (l.exact_sum == null) != (r.exact_sum == null)) return false;
    for (left.metrics, right.metrics) |l, r| if (l.exact_sum) |bytes| {
        if (!std.mem.eql(u8, bytes, r.exact_sum.?)) return false;
    };
    return true;
}
fn diff(a: A, summary: dataset.Summary, before: []const series.Bucket, after: []const series.Bucket) ![]const Event {
    var previous: std.AutoHashMapUnmanaged(dataset.RowKey, series.Bucket) = .empty;
    defer previous.deinit(a);
    for (before) |row| try previous.put(a, codec.key(row), row);
    var events: std.ArrayList(Event) = .empty;
    for (after) |row| {
        if (previous.fetchRemove(codec.key(row))) |old| if (equal(old.value, row)) continue;
        try events.append(a, try event(a, summary, row));
    }
    for (before) |row| {
        const key = codec.key(row);
        if (previous.contains(key)) try events.append(a, .{ .type = .remove, .key = try a.dupe(u8, &key) });
    }
    return events.items;
}
fn replayKey(raw: []const u8) !dataset.RowKey {
    if (raw.len < 97 or raw[raw.len - 65] != ':') return error.RetainedSeriesCorrupt;
    var key: dataset.RowKey = undefined;
    @memcpy(key[0..32], raw[raw.len - 97 .. raw.len - 65]);
    @memcpy(key[32..], raw[raw.len - 64 ..]);
    for (key) |digit| if (!std.ascii.isDigit(digit) and (digit < 'a' or digit > 'f')) return error.RetainedSeriesCorrupt;
    return key;
}

fn admitArchiveClock(server: anytype, a: A, snapshot: read.Snapshot, definition: dataset.Definition, archive: ?*const archive_api.Opened, proposed: i128, ctx: operation.RequestContext) !i128 {
    const opened = archive orelse return proposed;
    var clock = proposed;
    for (snapshot.owners) |owner| if (owner.independent_receipt != null) {
        clock = @max(clock, (try cutFor(owner)).admitted_ns);
    };
    const io = server.sharedApiIo() orelse return error.BackendRuntimeUnavailable;
    return opened.store(io, &ctx).admitRetentionClock(try specFor(a, definition, snapshot.owners[0]), clock);
}

pub fn open(server: anytype, alloc: A, sessions: *registry.Registry, secret: []const u8, principal: registry.Digest, target: Target, input: Create, ctx: operation.RequestContext) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const snapshot = try read.fetchReadCut(server, a, target, ctx);
    if (snapshot.owners.len == 0 or snapshot.owners.len > max_owners) return error.DatasetReaderBudgetExceeded;
    var descriptor = try local.system_catalog_retained_datasets.parse(a, snapshot.resource.source_json);
    defer descriptor.deinit();
    const definition = descriptor.value.definition;
    const summary = definition.summary orelse return error.InvalidDatasetDefinition;
    var archive = try openedArchive(server, a, snapshot, descriptor.value);
    defer if (archive) |*opened| opened.deinit();
    var observed_clock: i128 = platform.realtimeNs();
    for (snapshot.owners) |owner| if (owner.independent_receipt == null) {
        const source = snapshot.source_table orelse return error.RetainedOwnerPlanChanged;
        const observed = try read.observeNative(server, a, source.name, scopeFor(snapshot, owner), observed_clock, ctx);
        observed_clock = @max(observed_clock, observed.admitted_ns);
    };
    const now = try admitArchiveClock(server, a, snapshot, definition, if (archive) |*opened| opened else null, observed_clock, ctx);
    const window = if (input.window_ns) |text| std.math.cast(u64, try parseBound(text)) orelse return error.InvalidDatasetCoverage else null;
    const bounds: Bounds = if (window) |width| range: {
        if (input.start_ns != null or input.end_ns != null) return error.InvalidDatasetCoverage;
        break :range try movingBounds(try specFor(a, definition, snapshot.owners[0]), width, now);
    } else .{ .start = try parseBound(input.start_ns orelse return error.InvalidDatasetCoverage), .end = try parseBound(input.end_ns orelse return error.InvalidDatasetCoverage) };
    if (bounds.start >= bounds.end) return error.InvalidDatasetCoverage;
    const state: State = .{ .resource = snapshot.resource.id, .plan = snapshot.plan.id, .definition = snapshot.plan.definition, .window_ns = window, .bounds = bounds, .clock_ns = now, .parts = try a.alloc(Part, snapshot.owners.len) };
    var budget: usize = registry.max_state_bytes;
    for (state.parts, snapshot.owners) |*part, owner| part.* = try capture(server, a, snapshot, definition, owner, bounds, now, if (archive) |*opened| opened else null, ctx, &budget);
    var rows = try aggregate(a, snapshot, state, ctx);
    defer rows.deinit();
    const payload = try std.json.Stringify.valueAlloc(a, state, .{});
    var binding = std.crypto.hash.sha2.Sha256.init(.{});
    binding.update(&principal);
    binding.update(payload);
    var position: registry.Position = .{ .id = undefined, .sequence = 0, .binding = binding.finalResult() };
    const io = server.sharedApiIo() orelse return error.BackendRuntimeUnavailable;
    io.random(&position.id);
    const cursor = registry.token(secret, position);
    const response = try std.json.Stringify.valueAlloc(alloc, .{ .kind = "snapshot", .cursor = cursor[0..], .initial_history = summary.initial_history, .dimensions = summary.dimensions, .measures = summary.measures, .events = try diff(a, summary, &.{}, rows.rows) }, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(response);
    if (response.len > registry.max_reply_bytes) return error.DatasetPageBudgetExceeded;
    try ctx.ensureActive();
    try sessions.create(server.alloc, position, principal, platform.monotonicNs(), payload);
    return response;
}

pub fn poll(server: anytype, alloc: A, sessions: *registry.Registry, secret: []const u8, principal: registry.Digest, target: Target, input: Poll, ctx: operation.RequestContext) ![]u8 {
    const position = try registry.verify(secret, input.cursor);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const loaded = try sessions.load(server.alloc, a, position, principal, platform.monotonicNs());
    if (loaded == .retry_reply) return alloc.dupe(u8, loaded.retry_reply);
    const original = try std.json.parseFromSliceLeaky(State, a, loaded.state, .{});
    const snapshot = try read.fetchReadCut(server, a, target, ctx);
    if (snapshot.resource.id != original.resource or !std.mem.eql(u8, &snapshot.plan.id, &original.plan) or !std.mem.eql(u8, &snapshot.plan.definition, &original.definition) or snapshot.owners.len != original.parts.len) return error.RetainedSeriesResetRequired;
    var descriptor = try local.system_catalog_retained_datasets.parse(a, snapshot.resource.source_json);
    defer descriptor.deinit();
    const definition = descriptor.value.definition;
    const summary = definition.summary orelse return error.InvalidDatasetDefinition;
    var archive = try openedArchive(server, a, snapshot, descriptor.value);
    defer if (archive) |*opened| opened.deinit();
    var observed_clock: i128 = @max(platform.realtimeNs(), original.clock_ns);
    const replays = try a.alloc(?wire.Replay, snapshot.owners.len);
    @memset(replays, null);
    const refreshes = try a.alloc(?read.NativeReply, snapshot.owners.len);
    @memset(refreshes, null);
    var resynchronized = false;
    var budget: usize = registry.max_state_bytes;
    for (snapshot.owners, original.parts, replays, refreshes) |owner, old, *replay, *refresh| {
        if (old.group != owner.group or old.owner_revision != owner.revision or (old.cursor == null) != (owner.independent_receipt != null)) return error.RetainedSeriesResetRequired;
        if (old.cursor) |cursor| {
            const source = snapshot.source_table orelse return error.RetainedSeriesResetRequired;
            const raw = server.pollRetainedOwnerReplay(a, source.name, owner.group, .{ .scope = scopeFor(snapshot, owner), .root_incarnation = old.root, .cursor = cursor, .clock_ns = observed_clock }, ctx) catch |err| switch (err) {
                error.RetainedSeriesResetRequired => null,
                else => return err,
            };
            if (raw) |bytes| {
                try spend(&budget, bytes.len);
                replay.* = try std.json.parseFromSliceLeaky(wire.Replay, a, bytes, .{});
                try validateReplay(old, replay.*.?);
                observed_clock = @max(observed_clock, replay.*.?.admitted_ns);
            }
            // This API subscribes to result state, not every intermediate
            // transaction. Catch up through a certified snapshot when replay
            // is truncated or cannot reach the captured head in one batch.
            if (replay.* == null or replay.*.?.latest_sequence > replay.*.?.cursor.sequence) {
                const observed = try read.observeNative(server, a, source.name, scopeFor(snapshot, owner), observed_clock, ctx);
                try requireSameProducer(old, observed);
                refresh.* = observed;
                observed_clock = @max(observed_clock, observed.admitted_ns);
                resynchronized = true;
            }
        }
    }
    const now = try admitArchiveClock(server, a, snapshot, definition, if (archive) |*opened| opened else null, observed_clock, ctx);
    const bounds = if (original.window_ns) |width| try movingBounds(try specFor(a, definition, snapshot.owners[0]), width, now) else original.bounds;
    var next = original;
    next.parts = try a.alloc(Part, original.parts.len);
    next.bounds = bounds;
    next.clock_ns = now;
    for (snapshot.owners, original.parts, next.parts, replays, refreshes) |owner, old, *part, replay, refresh| {
        if (old.group != owner.group or old.owner_revision != owner.revision or (old.cursor == null) != (owner.independent_receipt != null)) return error.RetainedSeriesResetRequired;
        if (refresh) |observed| {
            const spec = try specFor(a, definition, owner);
            const visible: Bounds = .{ .start = @max(bounds.start, try spec.bucket(try std.math.sub(i128, now, spec.retention_ns))), .end = bounds.end };
            if (visible.start < visible.end) {
                part.* = try capture(server, a, snapshot, definition, owner, visible, now, if (archive) |*opened| opened else null, ctx, &budget);
                try requireSameProducer(old, .{ .rows = &.{}, .root_incarnation = part.root, .cursor = part.cursor.? });
            } else part.* = .{ .group = old.group, .owner_revision = old.owner_revision, .root = observed.root_incarnation, .cursor = observed.cursor };
            continue;
        }
        part.* = try advancePart(server, a, snapshot, definition, owner, old, original.bounds, bounds, now, if (archive) |*opened| opened else null, ctx, &budget, replay);
    }
    var before = try aggregate(a, snapshot, original, ctx);
    defer before.deinit();
    var after = try aggregate(a, snapshot, next, ctx);
    defer after.deinit();
    const payload = try std.json.Stringify.valueAlloc(a, next, .{});
    var successor = position;
    successor.sequence = std.math.add(u64, position.sequence, 1) catch return error.RetainedSeriesResetRequired;
    const cursor = registry.token(secret, successor);
    const response = try std.json.Stringify.valueAlloc(a, .{ .kind = "changes", .cursor = cursor[0..], .resynchronized = resynchronized, .events = try diff(a, summary, before.rows, after.rows) }, .{ .emit_null_optional_fields = false });
    try ctx.ensureActive();
    return sessions.commit(server.alloc, alloc, position, principal, platform.monotonicNs(), payload, response);
}

fn applyFrames(a: A, rows: *std.AutoHashMapUnmanaged(dataset.RowKey, series.Bucket), floors: []const Floor, visible: Bounds, initial: u64, replay: wire.Replay) !u64 {
    var sequence = initial;
    for (replay.frames) |frame| {
        sequence = std.math.add(u64, sequence, 1) catch return error.RetainedSeriesResetRequired;
        if (frame.sequence != sequence) return error.RetainedSeriesResetRequired;
        edit_loop: for (frame.edits) |edit| {
            const key = try replayKey(edit.key);
            const timestamp = try dataset.rowKeyTimestamp(key);
            if (!inRange(visible, timestamp)) continue;
            for (floors) |fence| if (sequence <= fence.through and inRange(fence.bounds, timestamp)) continue :edit_loop;
            if (edit.row) |row| {
                if (!std.mem.eql(u8, &codec.key(row), &key)) return error.RetainedSeriesCorrupt;
                try rows.put(a, key, row);
            } else _ = rows.remove(key);
        }
    }
    if (sequence != replay.cursor.sequence) return error.RetainedSeriesResetRequired;
    return sequence;
}

fn requireSameProducer(old: Part, fresh: read.NativeReply) !void {
    const previous = old.cursor orelse return error.RetainedSeriesResetRequired;
    if (!std.mem.eql(u8, old.root, fresh.root_incarnation) or !std.mem.eql(u8, &previous.definition, &fresh.cursor.definition) or !std.mem.eql(u8, &previous.partition, &fresh.cursor.partition) or previous.incarnation != fresh.cursor.incarnation or fresh.cursor.sequence < previous.sequence) return error.RetainedSeriesResetRequired;
}

fn validateReplay(old: Part, replay: wire.Replay) !void {
    const cursor = old.cursor orelse return error.RetainedSeriesResetRequired;
    if (!std.mem.eql(u8, replay.root_incarnation, old.root) or !std.mem.eql(u8, &replay.cursor.definition, &cursor.definition) or !std.mem.eql(u8, &replay.cursor.partition, &cursor.partition) or replay.cursor.incarnation != cursor.incarnation or replay.frames.len > 8) return error.RetainedSeriesResetRequired;
}

fn advancePart(server: anytype, a: A, snapshot: read.Snapshot, definition: dataset.Definition, owner: owners.Owner, old: Part, prior: Bounds, current: Bounds, now: i128, archive: ?*const archive_api.Opened, ctx: operation.RequestContext, budget: *usize, prefetched: ?wire.Replay) !Part {
    try ctx.ensureActive();
    const replay = if (old.cursor) |cursor| prefetched orelse replay: {
        const source = snapshot.source_table orelse return error.RetainedSeriesResetRequired;
        const raw = try server.pollRetainedOwnerReplay(a, source.name, owner.group, .{ .scope = scopeFor(snapshot, owner), .root_incarnation = old.root, .cursor = cursor, .clock_ns = now }, ctx);
        try spend(budget, raw.len);
        const value = try std.json.parseFromSliceLeaky(wire.Replay, a, raw, .{});
        try validateReplay(old, value);
        break :replay value;
    } else null;
    const spec = try specFor(a, definition, owner);
    const floor_clock = std.math.sub(i128, @max(now, if (replay) |value| value.admitted_ns else now), spec.retention_ns) catch return error.RetainedSeriesTimeOverflow;
    var floor = @max(current.start, try spec.bucket(floor_clock));
    if (archive) |opened| if (owner.independent_receipt) |receipt| {
        const io = server.sharedApiIo() orelse return error.BackendRuntimeUnavailable;
        const manifest = (try opened.store(io, &ctx).getClosedExport(spec, try cutFor(owner))) orelse return error.RetainedSeriesResetRequired;
        if (!std.mem.eql(u8, &try manifest.receipt(), &receipt)) return error.RetainedSeriesResetRequired;
        const clock = try opened.store(io, &ctx).admitRetentionClock(spec, @max(now, manifest.cut.admitted_ns));
        floor = @max(floor, try spec.bucket(try std.math.sub(i128, clock, spec.retention_ns)));
        if (manifest.expired_before_ns) |expired| floor = @max(floor, expired);
    };
    const visible: Bounds = .{ .start = floor, .end = current.end };
    var rows: std.AutoHashMapUnmanaged(dataset.RowKey, series.Bucket) = .empty;
    defer rows.deinit(a);
    for (old.rows) |row| if (inRange(visible, row.start_ns)) try rows.put(a, codec.key(row), row);
    var floors: std.ArrayList(Floor) = .empty;
    for (old.floors) |fence| if (fence.bounds.end > floor) try floors.append(a, fence);
    if (current.end > prior.end and visible.start < visible.end) {
        const entering: Bounds = .{ .start = @max(prior.end, visible.start), .end = current.end };
        const fresh = try capture(server, a, snapshot, definition, owner, entering, @max(now, if (replay) |value| value.admitted_ns else now), archive, ctx, budget);
        if (fresh.cursor) |cursor| {
            if (!std.mem.eql(u8, fresh.root, old.root) or cursor.sequence < old.cursor.?.sequence) return error.RetainedSeriesResetRequired;
            try floors.append(a, .{ .bounds = entering, .through = cursor.sequence });
        }
        for (fresh.rows) |row| try rows.put(a, codec.key(row), row);
    }
    var next = old;
    if (old.cursor) |cursor| {
        const frames = replay orelse return error.RetainedSeriesResetRequired;
        const sequence = try applyFrames(a, &rows, floors.items, visible, cursor.sequence, frames);
        next.cursor = frames.cursor;
        var write: usize = 0;
        for (floors.items) |fence| if (fence.through > sequence) {
            floors.items[write] = fence;
            write += 1;
        };
        floors.items.len = write;
    }
    if (floors.items.len > max_floors or rows.count() > max_rows) return error.RetainedSeriesResetRequired;
    next.floors = floors.items;
    const ordered = try a.alloc(series.Bucket, rows.count());
    const keys = try a.alloc(dataset.RowKey, rows.count());
    var iterator = rows.keyIterator();
    for (keys) |*key| key.* = iterator.next().?.*;
    std.mem.sort(dataset.RowKey, keys, {}, struct {
        fn less(_: void, l: dataset.RowKey, r: dataset.RowKey) bool {
            return std.mem.order(u8, &l, &r) == .lt;
        }
    }.less);
    for (ordered, keys) |*row, key| row.* = rows.get(key).?;
    next.rows = ordered;
    return next;
}

test "retained subscription replay preserves entering buckets until their read position is passed" {
    const a = std.testing.allocator;
    var latest_metrics = [_]series.Metric{.{ .count = 3 }};
    var stale_metrics = [_]series.Metric{.{ .count = 1 }};
    var current_metrics = [_]series.Metric{.{ .count = 4 }};
    const fresh: series.Bucket = .{ .start_ns = 10, .dimensions = "[]", .rows = 3, .metrics = &latest_metrics };
    const stale: series.Bucket = .{ .start_ns = 10, .dimensions = "[]", .rows = 1, .metrics = &stale_metrics };
    const newer: series.Bucket = .{ .start_ns = 10, .dimensions = "[]", .rows = 4, .metrics = &current_metrics };
    const key = codec.key(fresh);
    var native: [97]u8 = undefined;
    const raw = try std.fmt.bufPrint(&native, "{s}:{s}", .{ key[0..32], key[32..] });
    var rows: std.AutoHashMapUnmanaged(dataset.RowKey, series.Bucket) = .empty;
    defer rows.deinit(a);
    try rows.put(a, key, fresh);
    const bounds: Bounds = .{ .start = 0, .end = 20 };
    const fence: Floor = .{ .bounds = .{ .start = 10, .end = 20 }, .through = 1 };
    var replay: wire.Replay = .{ .root_incarnation = "7", .cursor = .{ .definition = @splat(1), .partition = @splat(2), .incarnation = 1, .sequence = 1 }, .frames = &.{.{ .sequence = 1, .source_sequence = 5, .edits = &.{.{ .key = raw, .row = stale }} }} };
    try std.testing.expectEqual(@as(u64, 1), try applyFrames(a, &rows, &.{fence}, bounds, 0, replay));
    try std.testing.expectEqual(@as(i64, 3), rows.get(key).?.rows);
    replay.cursor.sequence = 2;
    replay.frames = &.{.{ .sequence = 2, .source_sequence = 6, .edits = &.{.{ .key = raw, .row = newer }} }};
    try std.testing.expectEqual(@as(u64, 2), try applyFrames(a, &rows, &.{fence}, bounds, 1, replay));
    try std.testing.expectEqual(@as(i64, 4), rows.get(key).?.rows);
    try std.testing.expectError(error.RetainedSeriesResetRequired, applyFrames(a, &rows, &.{}, bounds, 0, replay));
}

test "retained subscription archived polling observes durable retention after clock rollback" {
    const objects = @import("objectstore");
    const catalog_api = @import("../serverless/datasets/catalog.zig");
    const export_page = @import("../serverless/datasets/closure_export.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objects.memory.MemoryClient.init(std.testing.allocator);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("meta");
    const namespace = std.fmt.bytesToHex(@as(dataset.Id, @splat(1)), .lower);
    const Clock = struct {
        now: u64 = 1000,
        fn unix(ptr: *const anyopaque) u64 {
            return @as(*const @This(), @ptrCast(@alignCast(ptr))).now;
        }
    };
    var clock: Clock = .{};
    const store: catalog_api.Catalog = .{ .context = .{ .alloc = a, .client = client, .bucket = "meta", .prefix = "catalog", .namespace = &namespace, .io = std.testing.io }, .dataset = @splat(1), .owner = @splat(1), .clock = .{ .ptr = &clock, .unix_fn = Clock.unix } };
    var hash: dataset.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &hash, .{});
    const def: dataset.Definition = .{ .id = @splat(1), .generation = 1, .kind = .summary, .schema = hash, .schema_json = "{}", .authorization = @splat(3), .retention_ns = 1000, .local_residence_ns = 0, .summary = .{ .source = @splat(9), .timestamp = "ts", .interval_ns = 60, .mutable_for_ns = 100, .measures = &.{.{ .name = "n", .law = .count }} } };
    try store.create(def);
    const spec = try @import("retained_owner_binding.zig").specification(a, def, @splat(5), 1);
    const cut: closure.Cut = .{ .closure = @splat(8), .sequence = 20, .complete_from_ns = 0, .admitted_ns = 1000 };
    const loc: catalog_api.Location = .{ .id = @splat(6), .storage_identity = @splat(16), .client = client, .bucket = "meta", .prefix = "catalog", .access = .queryable };
    var metrics = [_]series.Metric{.{ .count = 1 }};
    const rows = [_]series.Bucket{ .{ .start_ns = 0, .dimensions = "[]", .rows = 1, .metrics = &metrics }, .{ .start_ns = 120, .dimensions = "[]", .rows = 1, .metrics = &metrics } };
    const page = (try export_page.encode(a, def, spec, cut, dataset.rowKey(0, @splat(0)), &rows, false, loc)).?;
    try export_page.publish(store, spec, cut, page, loc);
    const manifest = try store.sealClosedExport(spec, cut, (try page.segment.span()).end);
    const receipt = try manifest.receipt();
    _ = try store.admitRetentionClock(spec, 1100);
    clock.now = 1050;
    const Resolver = struct {
        value: catalog_api.Location,
        pub fn location(self: @This(), _: dataset.Id) !catalog_api.Location {
            return self.value;
        }
    };
    try std.testing.expectError(error.RetainedSeriesHistoryExpired, archive_read.readClosed(a, store, def, spec, .{ .start_ns = "0", .end_ns = "180" }, .{}, cut, receipt, Resolver{ .value = loc }));
    var opened: archive_api.Opened = undefined;
    opened.alloc = a;
    opened.prefix = "catalog";
    opened.namespace = namespace;
    opened.dataset = @splat(1);
    opened.writer = @splat(1);
    opened.location_id = loc.id;
    opened.storage.opened.client = client;
    opened.storage.opened.bucket = try a.dupe(u8, "meta");
    opened.storage.opened.prefix = try a.dupe(u8, "catalog");
    opened.storage.identity = loc.storage_identity;
    const Server = struct {
        pub fn sharedApiIo(_: *@This()) ?std.Io {
            return std.testing.io;
        }
        pub fn readRetainedOwnerRows(_: *@This(), _: A, _: []const u8, _: u64, _: read.NativeQuery, _: operation.RequestContext) ![]u8 {
            return error.UnexpectedStorageRead;
        }
        pub fn pollRetainedOwnerReplay(_: *@This(), _: A, _: []const u8, _: u64, _: read.NativeReplay, _: operation.RequestContext) ![]u8 {
            return error.UnexpectedStorageRead;
        }
    };
    var server: Server = .{};
    const owner: owners.Owner = .{ .plan = @splat(7), .group = 11, .partition = @splat(5), .incarnation = 1, .mode = .closed, .native_receipt = @splat(4), .cut = .{ .closure = cut.closure, .sequence = 20, .complete_from_ns = "0", .admitted_ns = "1000" }, .independent_receipt = receipt };
    var budget: usize = registry.max_state_bytes;
    const bounds: Bounds = .{ .start = 0, .end = 180 };
    const next = try advancePart(&server, a, undefined, def, owner, .{ .group = 11, .owner_revision = 1, .rows = &rows }, bounds, bounds, 1050, &opened, .{}, &budget, null);

    try std.testing.expectEqual(@as(usize, 1), next.rows.len);
}

test "retained subscription native polling applies the durable retention clock without a removal frame" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var schema: dataset.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &schema, .{});
    const def: dataset.Definition = .{ .id = @splat(1), .generation = 1, .kind = .summary, .schema = schema, .schema_json = "{}", .authorization = @splat(3), .retention_ns = 1000, .local_residence_ns = 0, .summary = .{ .source = @splat(9), .timestamp = "ts", .interval_ns = 60, .mutable_for_ns = 100, .measures = &.{.{ .name = "n", .law = .count }} } };
    const owner: owners.Owner = .{ .plan = @splat(7), .group = 11, .partition = @splat(5), .incarnation = 1, .mode = .ready, .native_receipt = @splat(4) };
    const spec = try specFor(a, def, owner);
    const cursor: series.Cursor = .{ .definition = spec.definition, .partition = owner.partition, .incarnation = 1, .sequence = 0 };
    const Server = struct {
        cursor: series.Cursor,
        admitted_ns: i128 = 1100,
        pub fn sharedApiIo(_: *@This()) ?std.Io {
            return std.testing.io;
        }
        pub fn readRetainedOwnerRows(_: *@This(), _: A, _: []const u8, _: u64, _: read.NativeQuery, _: operation.RequestContext) ![]u8 {
            return error.UnexpectedStorageRead;
        }
        pub fn pollRetainedOwnerReplay(self: *@This(), alloc: A, _: []const u8, _: u64, _: read.NativeReplay, _: operation.RequestContext) ![]u8 {
            return std.json.Stringify.valueAlloc(alloc, wire.Replay{ .root_incarnation = "7", .cursor = self.cursor, .frames = &.{}, .admitted_ns = self.admitted_ns }, .{});
        }
    };
    var server: Server = .{ .cursor = cursor };
    const snapshot: read.Snapshot = .{ .resource = .{ .id = 41, .kind = .dataset, .name = "summary" }, .plan = .{ .id = @splat(7), .resource = 41, .dataset = def.id, .generation = 1, .definition = spec.definition, .source = @splat(9), .source_table = 7, .topology_generation = 1, .membership = @splat(6) }, .owners = &.{owner}, .source_table = .{ .table_id = 7, .name = "raw" } };
    var metrics = [_]series.Metric{.{ .count = 1 }};
    const rows = [_]series.Bucket{.{ .start_ns = 0, .dimensions = "[]", .rows = 1, .metrics = &metrics }};
    const prior: Part = .{ .group = 11, .owner_revision = 1, .root = "7", .cursor = cursor, .rows = &rows };
    const bounds: Bounds = .{ .start = 0, .end = 180 };
    var budget: usize = registry.max_state_bytes;
    const next = try advancePart(&server, a, snapshot, def, owner, prior, bounds, bounds, 1050, null, .{}, &budget, null);
    try std.testing.expect(try spec.expired(0, server.admitted_ns));
    try std.testing.expectEqual(@as(usize, 0), next.rows.len);
}

test "retained subscription snapshot catchup preserves physical producer identity" {
    const cursor: series.Cursor = .{ .definition = @splat(1), .partition = @splat(2), .incarnation = 3, .sequence = 100 };
    const old: Part = .{ .group = 7, .owner_revision = 1, .root = "42", .cursor = cursor };
    var fresh: read.NativeReply = .{ .root_incarnation = "42", .cursor = cursor, .rows = &.{} };
    fresh.cursor.sequence = 10000;
    try requireSameProducer(old, fresh);
    fresh.root_incarnation = "43";
    try std.testing.expectError(error.RetainedSeriesResetRequired, requireSameProducer(old, fresh));
    fresh.root_incarnation = "42";
    fresh.cursor.incarnation += 1;
    try std.testing.expectError(error.RetainedSeriesResetRequired, requireSameProducer(old, fresh));
    fresh.cursor = cursor;
    fresh.cursor.sequence -= 1;
    try std.testing.expectError(error.RetainedSeriesResetRequired, requireSameProducer(old, fresh));
}
