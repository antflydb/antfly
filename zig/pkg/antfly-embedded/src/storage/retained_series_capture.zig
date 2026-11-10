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

//! Coalesces authoritative source before/after images inside a DocStore commit.
//! The private context is part of the replicated physical input batch, then
//! consumed before commit. A replica therefore never reads a local clock.
const std = @import("std");
const series = @import("retained_series.zig");
const keys = @import("internal_keys.zig");
const schema = @import("schema.zig");
const rows = @import("db/algebraic/relational_row_codec.zig");
const datetime = @import("../datetime.zig");
const A = std.mem.Allocator;
pub const registry_key = "\x00\x00__metadata__:retained_series_bindings_v1";
pub const sequence_key = "\x00\x00__metadata__:retained_series_sequence_v1";
pub const Cache = std.atomic.Value(u64);
const uncached_tag = std.math.maxInt(u64) - 1;

/// Even tags require a read; odd tags certify absence at that exact epoch.
/// Saturation disables negative caching instead of allowing epoch wrap/ABA.
pub fn invalidateCache(cache: *Cache) void {
    var observed = cache.load(.acquire);
    while (true) {
        const next = ((observed & ~@as(u64, 1)) +| 2) & ~@as(u64, 1);
        if (cache.cmpxchgWeak(observed, next, .acq_rel, .acquire)) |changed| observed = changed else return;
    }
}
pub fn cacheAbsent(cache: *Cache, observed: u64) void {
    if (observed != uncached_tag) _ = cache.cmpxchgStrong(observed, observed | 1, .acq_rel, .acquire);
}

pub const context_key = "\x00\x00__metadata__:retained_series_context_v1";
pub const max_bindings = 8;
pub const max_capture_bytes = 64 * 1024 * 1024;
pub const Resolution = struct { transaction: [16]u8, decision: enum { committed, aborted } };
pub const Context = struct { sequence: u64, admitted_ns: i128, cause: series.Cause = .mutation, source_replay_sequence: u64 = 0, resolution: ?Resolution = null };
pub const DimensionType = enum { integer, number, boolean, string, datetime };
pub const Binding = struct {
    spec: series.Spec,
    timestamp: []const u8,
    dimensions: []const []const u8 = &.{},
    dimension_types: []const DimensionType = &.{},
    /// Null names denote count(*); other inputs are numeric nullable fields.
    fields: []const ?[]const u8,

    pub fn validate(self: Binding) !void {
        try self.spec.validate();
        if (self.timestamp.len == 0 or self.timestamp.len > 256 or self.fields.len != self.spec.laws.len or self.dimensions.len > 32 or self.dimension_types.len != self.dimensions.len) return error.InvalidRetainedSeries;
        for (self.spec.laws, self.fields, 0..) |law, field, i| {
            if (law == .count) {
                if ((field != null) != self.spec.countsNonNull(i)) return error.InvalidRetainedSeries;
                if (field) |name| if (name.len == 0 or name.len > 256) return error.InvalidRetainedSeries;
            } else if (field == null or field.?.len == 0 or field.?.len > 256) return error.InvalidRetainedSeries;
        }
        for (self.dimensions, 0..) |name, i| {
            if (name.len == 0 or name.len > 256) return error.InvalidRetainedSeries;
            for (self.dimensions[0..i]) |previous| if (std.mem.eql(u8, name, previous)) return error.InvalidRetainedSeries;
        }
    }
};

pub fn load(a: A, txn: anytype) ![]const Binding {
    const raw = (try optional(txn, registry_key)) orelse return &.{};
    if (raw.len > 32768) return error.RetainedSeriesCorrupt;
    const bindings = (try std.json.parseFromSlice([]const Binding, a, raw, .{ .allocate = .alloc_always })).value;
    if (bindings.len > max_bindings) return error.RetainedSeriesCorrupt;
    for (bindings, 0..) |b, i| {
        try b.validate();
        for (bindings[0..i]) |old| if (std.mem.eql(u8, &old.spec.id, &b.spec.id)) return error.RetainedSeriesCorrupt;
    }
    return bindings;
}

/// Called only by the replicated control path under the source writer fence.
/// The caller derives complete_from_ns from certified existing source coverage.
pub fn install(a: A, txn: anytype, binding: Binding, context: Context, complete_from_ns: i128) !void {
    try binding.validate();
    const previous = try load(a, txn);
    if (previous.len == max_bindings) return error.RetainedSeriesBatchTooLarge;
    for (previous) |old| if (std.mem.eql(u8, &old.spec.id, &binding.spec.id)) return error.RetainedSeriesAlreadyExists;
    const next = try a.alloc(Binding, previous.len + 1);
    @memcpy(next[0..previous.len], previous);
    next[previous.len] = binding;
    const raw = try std.json.Stringify.valueAlloc(a, next, .{});
    if (raw.len > 32768) return error.RetainedSeriesBatchTooLarge;
    const sequence = try sourceSequence(txn, context.sequence);
    try saveSequence(txn, sequence);
    try (series.Engine{ .alloc = a, .spec = binding.spec }).install(txn, sequence, context.admitted_ns, complete_from_ns);
    try txn.put(registry_key, raw);
}

/// Physical inputs have already been schema-normalized by native intent
/// preparation. Metadata intents are excluded by that caller before planning.
pub const PreparedInput = struct { key: []const u8, logical_key: []const u8, after: ?[]const u8 };
pub const AdmissionCost = series.AdmissionCost;
pub const PrepareEffects = struct { writes: []const @import("docstore.zig").KVPair, deletes: []const []const u8, costs: []const AdmissionCost };
const PreparedBudget = struct { count: usize = 0, before: usize = 0, after: usize = 0 };
const PreparedMember = struct { key: []const u8, before: usize, after: usize };
const prepared_prefix = "\x00\x00__metadata__:retained_series_prepared_v1:";
fn preparedKeysPrefix(a: A, transaction: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}:keys:", .{ prepared_prefix, std.fmt.bytesToHex(transaction, .lower) });
}
fn preparedCountKey(a: A, transaction: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}:count", .{ prepared_prefix, std.fmt.bytesToHex(transaction, .lower) });
}
/// A point-only overlay plans metadata under the DB apply fence without
/// opening a second writer or changing state before the intent vote commits.
/// All returned bytes belong to the caller's preparation arena.
const PrepareOverlay = struct {
    a: A,
    read: *@import("docstore.zig").DocStore.Txn,
    pending: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
        if (self.pending.get(key)) |value| return value orelse error.NotFound;
        return self.read.get(key);
    }
    pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
        const entry = try self.pending.getOrPut(self.a, key);
        if (!entry.found_existing) entry.key_ptr.* = try self.a.dupe(u8, key);
        entry.value_ptr.* = try self.a.dupe(u8, value);
    }
    pub fn delete(self: *@This(), key: []const u8) !void {
        const entry = try self.pending.getOrPut(self.a, key);
        if (!entry.found_existing) entry.key_ptr.* = try self.a.dupe(u8, key);
        entry.value_ptr.* = null;
    }
};

pub fn planPrepare(a: A, read: *@import("docstore.zig").DocStore.Txn, transaction: [16]u8, admitted_ns: i128, inputs: []const PreparedInput) !PrepareEffects {
    if (inputs.len == 0) return .{ .writes = &.{}, .deletes = &.{}, .costs = &.{} };
    if (inputs.len > series.max_changes) return error.RetainedSeriesBatchTooLarge;
    const bindings = try load(a, read);
    if (bindings.len == 0) return .{ .writes = &.{}, .deletes = &.{}, .costs = &.{} };
    var overlay = PrepareOverlay{ .a = a, .read = read };
    var selected = std.StringHashMapUnmanaged(PreparedInput).empty;
    for (inputs) |input| {
        if (!keys.isStoredDocumentRowKey(input.key)) return error.InvalidRetainedSeriesRow;
        // Match native last-intent-wins semantics within a prepare request.
        try selected.put(a, input.key, input);
    }
    const Decoded = struct { input: PreparedInput, before: ?Image, after: ?Image, member_bytes: usize };
    const decoded = try a.alloc(Decoded, selected.count());
    var iterator = selected.iterator();
    var count: usize = 0;
    var decoder = try Decoder.init(a, bindings);
    const key_prefix = try preparedKeysPrefix(a, transaction);
    const count_key = try preparedCountKey(a, transaction);
    var total = if (try optional(&overlay, count_key)) |raw| try std.json.parseFromSliceLeaky(PreparedBudget, a, raw, .{}) else PreparedBudget{};
    if (total.count > series.max_changes or total.before > max_capture_bytes or total.after > max_capture_bytes) return error.RetainedSeriesCorrupt;
    while (iterator.next()) |entry| {
        const input = entry.value_ptr.*;
        const borrowed_before = try optional(&overlay, input.key);
        const before = if (borrowed_before) |value| try a.dupe(u8, value) else null;
        const before_size = std.math.add(usize, input.key.len, if (before) |value| value.len else 0) catch return error.RetainedSeriesBatchTooLarge;
        // AROW metadata finalization preserves length. Document source bytes
        // use the same bounded encoding proof as retained intent admission.
        const after_size = if (input.after) |value| if (rows.looksLikeRow(value) or std.mem.startsWith(u8, value, "AJV1")) value.len else try @import("document_value_bound.zig").maximumStoredBytes(a, value) else 0;
        var digest: series.Id = undefined;
        std.crypto.hash.sha2.Sha256.hash(input.key, &digest, .{});
        const member = try std.fmt.allocPrint(a, "{s}{s}", .{ key_prefix, std.fmt.bytesToHex(digest, .lower) });
        if (try optional(&overlay, member)) |existing| {
            const prior = try std.json.parseFromSliceLeaky(PreparedMember, a, existing, .{ .allocate = .alloc_always });
            if (!std.mem.eql(u8, prior.key, input.key) or prior.before != before_size) return error.RetainedSeriesCorrupt;
            total.before = std.math.sub(usize, total.before, prior.before) catch return error.RetainedSeriesCorrupt;
            total.after = std.math.sub(usize, total.after, prior.after) catch return error.RetainedSeriesCorrupt;
        } else {
            if (total.count >= series.max_changes) return error.RetainedSeriesBatchTooLarge;
            total.count += 1;
        }
        if (before_size > max_capture_bytes - total.before or after_size > max_capture_bytes - total.after) return error.RetainedSeriesBatchTooLarge;
        total.before += before_size;
        total.after += after_size;
        // Decoder materializes/copies the before image before another schema
        // lookup can invalidate the backend's borrowed point-read scratch.
        const old = if (before) |value| try decoder.read(&overlay, value) else null;
        const encoded_member = try std.json.Stringify.valueAlloc(a, PreparedMember{ .key = input.key, .before = before_size, .after = after_size }, .{});
        if (encoded_member.len > 32768) return error.RetainedSeriesBatchTooLarge;
        try overlay.put(member, encoded_member);
        decoded[count] = .{ .input = input, .before = old, .after = if (input.after) |value| try decoder.read(&overlay, value) else null, .member_bytes = member.len + encoded_member.len };
        count += 1;
    }
    const encoded_budget = try std.json.Stringify.valueAlloc(a, total, .{});
    try overlay.put(count_key, encoded_budget);
    const changes = try a.alloc(series.Change, decoded.len);
    var costs = std.StringHashMap(u64).init(a);
    for (bindings) |binding| {
        for (decoded, changes) |entry, *change| change.* = .{ .key = entry.input.key, .before = if (entry.before) |image| try normalizeImage(a, binding, image) else null, .after = if (entry.after) |image| try normalizeImage(a, binding, image) else null };
        std.mem.sort(series.Change, changes, {}, struct {
            fn less(_: void, left: series.Change, right: series.Change) bool {
                return std.mem.order(u8, left.key, right.key) == .lt;
            }
        }.less);
        try (series.Engine{ .alloc = a, .spec = binding.spec }).prepareWithCosts(&overlay, transaction, admitted_ns, changes, &costs);
    }
    var writes: std.ArrayList(@import("docstore.zig").KVPair) = .empty;
    var deletes: std.ArrayList([]const u8) = .empty;
    var pending = overlay.pending.iterator();
    while (pending.next()) |entry| {
        if (entry.value_ptr.*) |value| try writes.append(a, .{ .key = entry.key_ptr.*, .value = value }) else try deletes.append(a, entry.key_ptr.*);
    }
    std.mem.sort(@import("docstore.zig").KVPair, writes.items, {}, struct {
        fn less(_: void, left: @import("docstore.zig").KVPair, right: @import("docstore.zig").KVPair) bool {
            return std.mem.order(u8, left.key, right.key) == .lt;
        }
    }.less);
    const charges = try a.alloc(AdmissionCost, decoded.len);
    for (decoded, charges) |entry, *charge| charge.* = .{ .key = entry.input.logical_key, .bytes = std.math.add(u64, costs.get(entry.input.key) orelse 0, entry.member_bytes + count_key.len + encoded_budget.len + 128) catch return error.RetainedSeriesBatchTooLarge };
    return .{ .writes = writes.items, .deletes = deletes.items, .costs = charges };
}

fn releasePreparedKeys(a: A, txn: anytype, transaction: [16]u8) !void {
    const p = try preparedKeysPrefix(a, transaction);
    var remove: std.ArrayList([]const u8) = .empty;
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        var next = try cursor.seekAtOrAfter(p);
        while (next) |entry| {
            if (!std.mem.startsWith(u8, entry.key, p)) break;
            if (remove.items.len == series.max_changes) return error.RetainedSeriesCorrupt;
            try remove.append(a, try a.dupe(u8, entry.key));
            next = try cursor.next();
        }
    }
    for (remove.items) |key| try txn.delete(key);
    try txn.delete(try preparedCountKey(a, transaction));
}

pub const Capture = struct {
    before: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    staging: bool = false,
    poisoned: bool = false,
    context_touched: bool = false,
    registry_touched: bool = false,
    retained_bytes: usize = 0,

    pub fn deinit(self: *Capture, a: A) void {
        var it = self.before.iterator();
        while (it.next()) |entry| {
            a.free(entry.key_ptr.*);
            if (entry.value_ptr.*) |bytes| a.free(bytes);
        }
        self.before.deinit(a);
        self.* = .{};
    }
    pub fn completed(self: *const Capture, cache: ?*Cache) void {
        if (self.registry_touched) if (cache) |c| invalidateCache(c);
    }
    pub fn touch(self: *Capture, a: A, txn: anytype, key: []const u8, cache: ?*Cache) !void {
        if (std.mem.eql(u8, key, context_key)) {
            self.context_touched = true;
            return;
        }
        if (std.mem.eql(u8, key, registry_key)) {
            self.registry_touched = true;
            if (cache) |c| invalidateCache(c);
            return;
        }
        if (self.staging or !keys.isStoredDocumentRowKey(key)) return;
        if (self.poisoned) return error.RetainedSeriesTransactionPoisoned;
        errdefer self.poisoned = true;
        const observed = if (cache) |c| c.load(.acquire) else 0;
        if (!self.registry_touched and observed & 1 != 0) return;
        const registry = try optional(txn, registry_key);
        if (registry == null) {
            if (!self.registry_touched) if (cache) |c| cacheAbsent(c, observed);
            return;
        }
        // Active state is always reread from this transaction. Only durable
        // absence is cached, and touching the registry invalidates that cache.
        if (self.before.contains(key)) return;
        if (self.before.count() == series.max_changes) return error.RetainedSeriesBatchTooLarge;
        const original = try optional(txn, key);
        const retained_size = std.math.add(usize, key.len, if (original) |v| v.len else 0) catch return error.RetainedSeriesBatchTooLarge;
        if (retained_size > max_capture_bytes - self.retained_bytes) return error.RetainedSeriesBatchTooLarge;
        const owned = try a.dupe(u8, key);
        errdefer a.free(owned);
        const value = if (original) |raw| try a.dupe(u8, raw) else null;
        errdefer if (value) |bytes| a.free(bytes);
        try self.before.put(a, owned, value);
        self.retained_bytes += retained_size;
    }
    pub fn stage(self: *Capture, a: A, txn: anytype) !void {
        if (self.poisoned) return error.RetainedSeriesTransactionPoisoned;
        errdefer self.poisoned = true;
        if (self.before.count() == 0) {
            // Context-only maintenance batches do not leave a reusable marker.
            if (self.context_touched) {
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const temp = arena.allocator();
                if (try optional(txn, context_key)) |raw| {
                    const context = try std.json.parseFromSliceLeaky(Context, temp, raw, .{});
                    if (context.resolution) |resolution| {
                        const bindings = try load(temp, txn);
                        for (bindings) |binding| _ = try (series.Engine{ .alloc = a, .spec = binding.spec }).releasePrepared(txn, resolution.transaction);
                        try releasePreparedKeys(temp, txn, resolution.transaction);
                    }
                }
                try txn.delete(context_key);
            }
            return;
        }
        self.staging = true;
        defer self.staging = false;
        errdefer self.poisoned = true;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const bindings = try load(alloc, txn);
        if (bindings.len == 0) return error.RetainedSeriesDefinitionMismatch;
        const raw_context = (try optional(txn, context_key)) orelse return error.RetainedSeriesCommitContextMissing;
        const context = (try std.json.parseFromSlice(Context, alloc, raw_context, .{})).value;
        if (context.sequence == 0) return error.RetainedSeriesSequenceMismatch;
        if (context.resolution) |resolution| if (resolution.decision != .committed) return error.RetainedSeriesReservationMismatch;
        const Decoded = struct { key: []const u8, before: ?Image, after: ?Image };
        const decoded = try alloc.alloc(Decoded, self.before.count());
        var decoder = try Decoder.init(alloc, bindings);
        var after_bytes: usize = 0;
        var iterator = self.before.iterator();
        var count: usize = 0;
        while (iterator.next()) |entry| {
            const key = entry.key_ptr.*;
            const after = try optional(txn, key);
            if (entry.value_ptr.* == null and after == null) continue;
            if (entry.value_ptr.* != null and after != null and std.mem.eql(u8, entry.value_ptr.*.?, after.?)) continue;
            if (after) |raw| {
                if (raw.len > max_capture_bytes - after_bytes) return error.RetainedSeriesBatchTooLarge;
                after_bytes += raw.len;
            }
            // A subsequent schema/point read may invalidate the backend's
            // borrowed result. Own it before decoding the before image.
            const held_after = if (after) |raw| try alloc.dupe(u8, raw) else null;
            decoded[count] = .{ .key = key, .before = if (entry.value_ptr.*) |raw| try decoder.read(txn, raw) else null, .after = if (held_after) |raw| try decoder.read(txn, raw) else null };
            count += 1;
        }
        if (count != 0) {
            const expected = std.math.add(u64, try sourceSequence(txn, null), 1) catch return error.RetainedSeriesSequenceMismatch;
            if (context.sequence != expected) return error.RetainedSeriesSequenceMismatch;
        }
        // Parse each physical source image once, independently of how many
        // definitions reference it. Binding-specific projections stay bounded.
        const changes = try alloc.alloc(series.Change, count);
        for (bindings) |binding| {
            for (decoded[0..count], changes) |entry, *change| {
                change.* = .{ .key = entry.key, .before = if (entry.before) |value| try normalizeImage(alloc, binding, value) else null, .after = if (entry.after) |value| try normalizeImage(alloc, binding, value) else null };
            }
            std.mem.sort(series.Change, changes, {}, struct {
                fn less(_: void, l: series.Change, r: series.Change) bool {
                    return std.mem.order(u8, l.key, r.key) == .lt;
                }
            }.less);
            const engine = series.Engine{ .alloc = alloc, .spec = binding.spec };
            if (context.resolution) |resolution| {
                if (count == 0) _ = try engine.releasePrepared(txn, resolution.transaction) else try engine.applyPrepared(txn, resolution.transaction, context.sequence, context.admitted_ns, context.cause, changes);
            } else if (count != 0) try engine.apply(txn, context.sequence, context.admitted_ns, context.cause, changes);
        }
        if (context.resolution) |resolution| try releasePreparedKeys(alloc, txn, resolution.transaction);
        if (count != 0) try saveSequence(txn, context.sequence);
        try txn.delete(context_key);
    }
};

/// Decode every physical row against its own immutable schema version; never
/// reinterpret a saved before image using the current table layout.
pub fn normalize(a: A, txn: anytype, binding: Binding, physical: []const u8) !series.Row {
    var decoder = try Decoder.init(a, &.{binding});
    return normalizeImage(a, binding, try decoder.read(txn, physical));
}

/// Project packed rows directly, compiling each immutable layout once per
/// source command. Unreferenced wide columns never become JSON/DOM objects.
const Image = struct { value: std.json.Value, presence: std.StringHashMapUnmanaged(bool) = .empty };
const Decoder = struct {
    a: A,
    wanted: std.StringHashMapUnmanaged(bool) = .empty,
    layouts: std.AutoHashMapUnmanaged(u32, Layout) = .empty,
    const Layout = struct { table: schema.TableSchema, physical: rows.PhysicalLayout };

    fn init(a: A, bindings: []const Binding) !Decoder {
        var self = Decoder{ .a = a };
        for (bindings) |binding| {
            try self.want(binding.timestamp, true);
            for (binding.dimensions) |field| try self.want(field, true);
            for (binding.spec.laws, binding.fields) |law, field| if (field) |name| try self.want(name, law != .count);
        }
        return self;
    }
    fn want(self: *Decoder, path: []const u8, value_needed: bool) !void {
        const whole = try self.wanted.getOrPut(self.a, path);
        if (!whole.found_existing) whole.value_ptr.* = false;
        whole.value_ptr.* = whole.value_ptr.* or value_needed;
        if (std.mem.indexOfScalar(u8, path, '.')) |dot| {
            const parent = try self.wanted.getOrPut(self.a, path[0..dot]);
            parent.value_ptr.* = true;
        }
    }
    fn read(self: *Decoder, txn: anytype, physical: []const u8) !Image {
        if (physical.len > max_capture_bytes) return error.RetainedSeriesBatchTooLarge;
        const a = self.a;
        if (rows.looksLikeRow(physical)) {
            // Schema lookup can reuse backend point-read scratch too.
            const held = try a.dupe(u8, physical);
            const version = try rows.rowSchemaVersion(held);
            if (!self.layouts.contains(version)) {
                const table = (try schema.loadSchemaVersionTxn(txn, a, version)) orelse return error.RetainedSeriesSchemaMissing;
                const layout = try rows.PhysicalLayout.init(a, table);
                try self.layouts.put(a, version, .{ .table = table, .physical = layout });
            }
            const layout = self.layouts.getPtr(version).?;
            var cursor = try rows.ordinalCellIteratorWithLayout(held, layout.table, &layout.physical);
            var root = std.json.Value{ .object = .empty };
            var presence = std.StringHashMapUnmanaged(bool).empty;
            while (try cursor.next()) |cell| {
                const column = layout.table.relational_columns[cell.ordinal];
                const needs_value = self.wanted.get(column.name) orelse continue;
                const value: std.json.Value = if (!needs_value) (if (cell.is_null) .null else .{ .bool = true }) else if (column.column_type == .json and !cell.is_null)
                    (try std.json.parseFromSlice(std.json.Value, a, cell.value.bytes_val, .{ .allocate = .alloc_always, .parse_numbers = false })).value
                else
                    try rows.borrowedJsonValueFromCellAlloc(a, column, cell);
                try root.object.put(a, column.name, value);
                try presence.put(a, column.name, !cell.is_null);
            }
            return .{ .value = root, .presence = presence };
        }
        if (std.mem.startsWith(u8, physical, "AJV1")) return .{ .value = try @import("typed_json.zig").decodeBinary(a, physical) };
        // Keep unused numeric lexemes opaque; referenced fields alone choose
        // numeric domains. This also preserves integer grouping above 2^53.
        return .{ .value = (try std.json.parseFromSlice(std.json.Value, a, physical, .{ .allocate = .alloc_always, .parse_numbers = false })).value };
    }
};

pub fn normalizeValue(a: A, binding: Binding, value: std.json.Value) !series.Row {
    return normalizeValueWithPresence(a, binding, value, .empty);
}
fn normalizeImage(a: A, binding: Binding, image: Image) !series.Row {
    return normalizeValueWithPresence(a, binding, image.value, image.presence);
}
fn normalizeValueWithPresence(a: A, binding: Binding, value: std.json.Value, presence: std.StringHashMapUnmanaged(bool)) !series.Row {
    if (value != .object) return error.InvalidRetainedSeriesRow;
    const timestamp = lookup(value, binding.timestamp);
    const ns = datetime.rangeNanoseconds(timestamp) orelse return error.InvalidRetainedSeriesRow;
    const dimensions = try a.alloc(std.json.Value, binding.dimensions.len);
    for (binding.dimensions, binding.dimension_types, dimensions) |field, declared, *out| out.* = try normalizeDimension(a, declared, lookup(value, field));
    const inputs = try a.alloc(?f64, binding.fields.len);
    for (binding.spec.laws, binding.fields, inputs) |law, field, *input| {
        input.* = if (field) |name| projected: {
            const cell = lookup(value, name);
            if (law == .count) {
                // SQL JSONB literal null is a present, non-SQL-null column.
                const nonnull = presence.get(name) orelse (cell != .null);
                break :projected if (nonnull) @as(f64, 1) else null;
            }
            break :projected switch (cell) {
                .null => null,
                .integer => |n| @as(f64, @floatFromInt(n)),
                .float => |n| if (std.math.isFinite(n)) n else return error.InvalidRetainedSeriesRow,
                .number_string => |text| try finiteNumber(text),
                else => return error.InvalidRetainedSeriesRow,
            };
        } else null;
    }
    return .{ .timestamp_ns = ns, .dimensions = try std.json.Stringify.valueAlloc(a, dimensions, .{}), .values = inputs };
}
fn normalizeDimension(a: A, declared: DimensionType, cell: std.json.Value) !std.json.Value {
    if (cell == .null) return .null;
    return switch (declared) {
        .integer => if (cell == .integer) cell else if (cell == .number_string) .{ .integer = std.fmt.parseInt(i64, cell.number_string, 10) catch return error.InvalidRetainedSeriesRow } else error.InvalidRetainedSeriesRow,
        .number => number: {
            const n: f64 = switch (cell) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                .number_string => |text| try finiteNumber(text),
                else => return error.InvalidRetainedSeriesRow,
            };
            if (!std.math.isFinite(n)) return error.InvalidRetainedSeriesRow;
            break :number .{ .float = if (n == 0) 0 else n };
        },
        .boolean => if (cell == .bool) cell else error.InvalidRetainedSeriesRow,
        .string => if (cell == .string) cell else error.InvalidRetainedSeriesRow,
        .datetime => .{ .string = try datetime.formatDateTimeSignedNsAlloc(a, datetime.rangeNanoseconds(cell) orelse return error.InvalidRetainedSeriesRow) },
    };
}
fn finiteNumber(text: []const u8) !f64 {
    const number = std.fmt.parseFloat(f64, text) catch return error.InvalidRetainedSeriesRow;
    if (!std.math.isFinite(number)) return error.InvalidRetainedSeriesRow;
    return number;
}
fn lookup(value: std.json.Value, path: []const u8) std.json.Value {
    if (value == .object) if (value.object.get(path)) |exact| return exact;
    var current = value;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| current = if (current == .object) current.object.get(part) orelse return .null else return .null;
    return current;
}
fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

/// Append canonical context before primary replication captures its effects.
/// Inactive tables avoid both the metadata write and capture allocations.
pub fn appendContext(a: A, store: anytype, source_replay_sequence: u64, admitted_ns: i128, cause: series.Cause, writes: anytype, owned: anytype) !void {
    return appendContextForResolution(a, store, source_replay_sequence, admitted_ns, cause, null, writes, owned);
}
pub fn appendContextForResolution(a: A, store: anytype, source_replay_sequence: u64, admitted_ns: i128, cause: series.Cause, resolution: ?Resolution, writes: anytype, owned: anytype) !void {
    if (!try store.retainedSeriesActive()) return;
    var read = try store.beginReadTxn();
    defer read.abort();
    const sequence = std.math.add(u64, try sourceSequence(&read, null), 1) catch return error.RetainedSeriesSequenceMismatch;
    const context = Context{ .sequence = sequence, .admitted_ns = admitted_ns, .cause = cause, .source_replay_sequence = source_replay_sequence, .resolution = resolution };
    const bytes = try std.json.Stringify.valueAlloc(a, context, .{});
    owned.append(a, bytes) catch |err| {
        a.free(bytes);
        return err;
    };
    try writes.append(a, .{ .key = context_key, .value = bytes });
}

/// Scan current authoritative source rows under the control command's writer
/// fence. Bound PHYSICAL entries as well as logical rows; derived keys cannot
/// cause one maintenance command to scan an unbounded user namespace.
pub fn backfillPage(a: A, txn: anytype, id: series.Id, expected_cursor: []const u8, limit: usize) !bool {
    if (limit == 0 or limit > series.max_changes) return error.RetainedSeriesBatchTooLarge;
    const bindings = try load(a, txn);
    var found: ?Binding = null;
    for (bindings) |b| if (std.mem.eql(u8, &b.spec.id, &id)) {
        found = b;
        break;
    };
    const binding = found orelse return error.RetainedSeriesDefinitionMismatch;
    const changes = try a.alloc(series.Change, limit);
    var decoder = try Decoder.init(a, &.{binding});
    var input_bytes: usize = 0;
    var count: usize = 0;
    var scanned: usize = 0;
    var through = expected_cursor;
    var complete = false;
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        const seek = if (expected_cursor.len == 0) &[_]u8{keys.user_namespace} else try std.mem.concat(a, u8, &.{ expected_cursor, "\x00" });
        var next = try cursor.seekAtOrAfter(seek);
        while (next) |entry| {
            if (entry.key.len == 0 or entry.key[0] != keys.user_namespace) {
                complete = true;
                break;
            }
            if (scanned == limit) break;
            if (keys.isStoredDocumentRowKey(entry.key)) {
                if (entry.value.len > max_capture_bytes) return error.RetainedSeriesBatchTooLarge;
                if (entry.value.len > max_capture_bytes - input_bytes) break;
                input_bytes += entry.value.len;
            }
            scanned += 1;
            through = try a.dupe(u8, entry.key);
            if (keys.isStoredDocumentRowKey(entry.key)) {
                changes[count] = .{ .key = through, .after = try normalizeImage(a, binding, try decoder.read(txn, try a.dupe(u8, entry.value))) };
                count += 1;
            }
            next = try cursor.next();
        }
        if (next == null) complete = true;
    }
    try (series.Engine{ .alloc = a, .spec = binding.spec }).backfillScanned(txn, expected_cursor, changes[0..count], through, complete);
    return complete;
}

test "retained series native source commit coalesces corrections and preserves TTL history" {
    const a = std.testing.allocator;
    const mem = @import("mem_backend.zig");
    var backend = mem.Backend.init(a, .{});
    defer backend.close();
    var store = try @import("docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const binding = Binding{ .spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .retention_ns = 200, .laws = &.{ .count, .sum, .avg, .min, .max } }, .timestamp = "ts", .fields = &.{ null, "v", "v", "v", "v" } };
    const engine = series.Engine{ .alloc = a, .spec = binding.spec };
    const key = try keys.documentKeyAlloc(alloc, "a");
    const first = "{\"ts\":\"1970-01-01T00:00:00.000000001Z\",\"v\":3}";
    const second = "{\"ts\":\"1970-01-01T00:00:00.000000001Z\",\"v\":9}";
    try store.put(key, first);
    try std.testing.expect(!try store.retainedSeriesActive());
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try install(alloc, &txn, binding, .{ .sequence = 0, .admitted_ns = 0 }, 0);
        const unready = try ExpirationGuard.init(alloc, &txn);
        try std.testing.expect(!try unready.allowed(alloc, &txn, first, 109));
        try std.testing.expect(try unready.allowed(alloc, &txn, first, 210));
        try std.testing.expect(try backfillPage(alloc, &txn, binding.spec.id, "", 1024));
        const prepared = try ExpirationGuard.init(alloc, &txn);
        try std.testing.expect(!try prepared.allowed(alloc, &txn, first, 109));
        try std.testing.expect(try prepared.allowed(alloc, &txn, first, 110));
        try txn.commit();
    }
    try std.testing.expect(try store.retainedSeriesActive());
    {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(context_key, try std.json.Stringify.valueAlloc(alloc, Context{ .sequence = 1, .admitted_ns = 5 }, .{}));
        batch.asTxn().appendPut(key, second) catch |err| switch (err) {
            error.Unsupported => try batch.put(key, second),
            else => return err,
        };
        // The first before-image and final after-image define the transaction.
        try batch.put(key, first);
        try batch.put(key, second);
        try batch.commit();
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        const page = try engine.scan(alloc, &txn, 0, 10, 5, null, 10);
        try std.testing.expectEqual(@as(i64, 1), page.rows[0].rows);
        try std.testing.expectEqual(@as(?f64, 9), page.rows[0].metrics[2].value(.avg));
        try std.testing.expectError(error.NotFound, txn.get(context_key));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(context_key, try std.json.Stringify.valueAlloc(alloc, Context{ .sequence = 2, .admitted_ns = 110, .cause = .expiration }, .{}));
        try txn.delete(key);
        try txn.commit();
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get(key));
        const page = try engine.scan(alloc, &txn, 0, 10, 110, null, 10);
        try std.testing.expectEqual(@as(?f64, 9), page.rows[0].metrics[1].value(.sum));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.put(key, first);
        try std.testing.expectError(error.RetainedSeriesCommitContextMissing, txn.commit());
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get(key));
    }
}

test "retained series private lifecycle codec is fenced and rejects public or mixed batches" {
    const a = std.testing.allocator;
    const batch = @import("../api/batch_internal.zig");
    const db_types = @import("db/types.zig");
    const command = @import("retained_series_control.zig").Command{ .action = .tick, .fence = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .partition = @splat(3), .incarnation = 1 } };
    const req = db_types.BatchRequest{ .retained_series = command, .timestamp_ns = 1, .document_version_scheme = .legacy };
    const encoded = try batch.encodeBatchRequest(a, req);
    defer a.free(encoded);
    try std.testing.expectError(error.InvalidBatchRequest, batch.parseBatchRequest(a, encoded));
    var decoded = try batch.parseInternalBatchRequest(a, encoded);
    defer decoded.deinit(a);
    try std.testing.expectEqualDeep(command, decoded.req.retained_series.?);
    var mixed = req;
    mixed.deletes = &.{"a"};
    try std.testing.expectError(error.InvalidRetainedSeries, batch.encodeBatchRequest(a, mixed));
    try std.testing.expectError(error.InvalidBatchRequest, batch.parseInternalBatchRequest(a, "{\"_merge_checkpoint\":{\"kind\":\"retained_series_v1\"}}"));
}

test "retained series native DB lifecycle and ordinary writes share replay progress" {
    const a = std.testing.allocator;
    const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/series", .{tmp.sub_path}, 0);
    defer a.free(path);
    const namespace = @import("db/doc_identity.zig").Namespace{ .table_id = 1, .shard_id = 2, .range_id = 3 };
    var db = try DB.open(a, path, .{ .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false, .start_resolver_workers = false, .start_independent_maintenance = false });
    defer db.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const first = "{\"ts\":\"1970-01-01T00:00:00.000000001Z\",\"v\":3}";
    const second = "{\"ts\":\"1970-01-01T00:00:00.000000001Z\",\"v\":9}";
    try db.batch(.{ .timestamp_ns = 1, .writes = &.{.{ .key = "a", .value = first }} });
    var partition: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&@import("db/online_source_contract.zig").namespaceBytes(namespace), &partition, .{});
    const binding = Binding{ .spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = partition, .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .laws = &.{ .count, .sum } }, .timestamp = "ts", .fields = &.{ null, "v" } };
    const control = @import("retained_series_control.zig");
    const fence = control.Fence{ .id = binding.spec.id, .generation = 1, .definition = binding.spec.definition, .partition = partition, .incarnation = 1 };
    try db.batch(.{ .timestamp_ns = 2, .retained_series = .{ .action = .install, .fence = fence, .binding = binding } });
    try db.batch(.{ .timestamp_ns = 3, .retained_series = .{ .action = .backfill, .fence = fence, .work_limit = 1024 } });
    try db.batch(.{ .timestamp_ns = 4, .writes = &.{.{ .key = "a", .value = second }} });
    const before_sequence = value: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        break :value try sourceSequence(&read, null);
    };
    try db.batch(.{ .timestamp_ns = 5, .writes = &.{.{ .key = "a", .value = "{\"ts\":\"1970-01-01T00:00:00.000000001Z\",\"v\":9,\"unrelated\":1}" }} });
    try db.batch(.{ .timestamp_ns = 6, .writes = &.{.{ .key = "a", .value = "{\"ts\":\"1970-01-01T00:00:00.000000001Z\",\"v\":9,\"unrelated\":2}" }} });
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const progress = try (series.Engine{ .alloc = a, .spec = binding.spec }).status(alloc, &read);
        try std.testing.expectEqual(before_sequence + 2, progress.source_sequence);
        try std.testing.expectEqual(progress.source_sequence, try sourceSequence(&read, null));
    }
    const pending = try db.beginTransaction(5);
    defer db.abortTransaction(pending, 6) catch {};
    try std.testing.expectError(error.RetainedSeriesCorrectionWindow, db.writeIntents(pending, &.{.{ .key = "a", .value = first }}, &.{}));
    try std.testing.expectError(error.RetainedSeriesCorrectionWindow, db.writeTransaction(pending, .{ .writes = &.{.{ .key = "a", .value = first }} }));
    try db.batch(.{ .timestamp_ns = 6, .transaction = .{ .resolve = .{ .txn_id = pending, .status = .aborted, .commit_version = 6 } } });
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    const page = try (series.Engine{ .alloc = a, .spec = binding.spec }).scan(alloc, &txn, 0, 10, 4, null, 10);
    try std.testing.expectEqual(@as(i64, 1), page.rows[0].rows);
    try std.testing.expectEqual(@as(?f64, 9), page.rows[0].metrics[1].value(.sum));
    try std.testing.expectError(error.NotFound, txn.get(context_key));
    try db.batch(.{ .timestamp_ns = 7, .writes = &.{.{ .key = "b", .value = "{\"ts\":\"1970-01-01T00:00:00.000000011Z\",\"v\":3}" }} });
    const reader = try db.openRetainedSeriesRead(a, .{ .fence = fence, .start_ns = 0, .end_ns = 20, .admitted_ns = 7 });
    defer reader.close();
    const first_page = try reader.next(1);
    const cut = first_page.cursor;
    try std.testing.expectEqual(@as(usize, 1), first_page.rows.len);
    try std.testing.expect(first_page.after != null);
    try std.testing.expectEqual(db.root_incarnation, reader.root_incarnation);
    try db.batch(.{ .timestamp_ns = 8, .writes = &.{.{ .key = "b", .value = "{\"ts\":\"1970-01-01T00:00:00.000000011Z\",\"v\":9}" }} });
    const second_page = try reader.next(1);
    try std.testing.expectEqualDeep(cut, second_page.cursor);
    try std.testing.expectEqual(@as(?f64, 3), second_page.rows[0].metrics[1].value(.sum));
    try std.testing.expect(second_page.after == null);
    try std.testing.expectEqual(@as(usize, 0), (try reader.next(1)).rows.len);
    reader.expires_at_ns = 0;
    try std.testing.expectError(error.RetainedSeriesSnapshotExpired, reader.next(1));
    const status_json = try db.queryRetainedSeriesJson(a, .status);
    defer a.free(status_json);
    const status = try std.json.parseFromSliceLeaky(@import("retained_series_query.zig").Status, alloc, status_json, .{});
    try std.testing.expectEqual(@as(usize, 1), status.entries.len);
    try std.testing.expectEqualDeep(binding.spec, status.entries[0].binding.spec);
    const replay_json = try db.queryRetainedSeriesJson(a, .{ .poll = .{ .fence = fence, .root_incarnation = db.root_incarnation, .cursor = cut } });
    defer a.free(replay_json);
    const replay = try std.json.parseFromSliceLeaky(@import("retained_series_query.zig").Replay, alloc, replay_json, .{});
    try std.testing.expectEqual(@as(usize, 1), replay.frames.len);
    try std.testing.expectEqual(@as(?f64, 9), replay.frames[0].edits[0].row.?.metrics[1].value(.sum));
    try std.testing.expectEqual(cut.sequence + 1, replay.cursor.sequence);
    try std.testing.expectError(error.RetainedSeriesResetRequired, db.queryRetainedSeriesJson(a, .{ .poll = .{ .fence = fence, .root_incarnation = db.root_incarnation ^ 1, .cursor = cut } }));
    const previous_quota = db.core.table_catalog.transaction_admission_bytes;
    const quota_txn = try db.beginTransaction(50);
    db.core.table_catalog.transaction_admission_bytes = try @import("transactions.zig").intentAdmissionBytes(.{ .key = "a", .value = first }) + 256;
    try std.testing.expectError(error.TransactionTooLarge, db.writeTransaction(quota_txn, .{ .admitted_timestamp_ns = 50, .writes = &.{.{ .key = "a", .value = first }} }));
    {
        var current = try db.core.store.beginReadTxn();
        defer current.abort();
        try std.testing.expectError(error.NotFound, current.get(try preparedCountKey(alloc, quota_txn)));
    }
    try db.abortTransaction(quota_txn, 51);
    db.core.table_catalog.transaction_admission_bytes = previous_quota;
    const prepared_txn = try db.beginTransaction(109);
    try db.writeTransaction(prepared_txn, .{ .admitted_timestamp_ns = 109, .writes = &.{.{ .key = "a", .value = first }} });
    try db.batch(.{ .timestamp_ns = 120, .retained_series = .{ .action = .tick, .fence = fence } });
    try db.writeTransaction(prepared_txn, .{ .admitted_timestamp_ns = 120, .writes = &.{.{ .key = "a", .value = first }} });
    {
        var current = try db.core.store.beginReadTxn();
        defer current.abort();
        const budget = try std.json.parseFromSliceLeaky(PreparedBudget, alloc, try current.get(try preparedCountKey(alloc, prepared_txn)), .{});
        try std.testing.expectEqual(@as(usize, 1), budget.count);
    }
    try db.resolveTransactionIntents(prepared_txn, .committed, 120);
    {
        var current = try db.core.store.beginReadTxn();
        defer current.abort();
        const committed = try (series.Engine{ .alloc = a, .spec = binding.spec }).scan(alloc, &current, 0, 10, 120, null, 10);
        try std.testing.expectEqual(@as(?f64, 3), committed.rows[0].metrics[1].value(.sum));
        try std.testing.expectError(error.NotFound, current.get(try preparedCountKey(alloc, prepared_txn)));
    }
    const aborted_txn = try db.beginTransaction(121);
    try db.writeTransaction(aborted_txn, .{ .admitted_timestamp_ns = 121, .writes = &.{.{ .key = "c", .value = "{\"ts\":\"1970-01-01T00:00:00.000000121Z\",\"v\":7}" }} });
    try db.abortTransaction(aborted_txn, 250);
    {
        var current = try db.core.store.beginReadTxn();
        defer current.abort();
        try std.testing.expectError(error.NotFound, current.get(try preparedCountKey(alloc, aborted_txn)));
        try std.testing.expectError(error.NotFound, current.get(try keys.documentKeyAlloc(alloc, "c")));
    }
    const noop_txn = try db.beginTransaction(251);
    try db.writeTransaction(noop_txn, .{ .admitted_timestamp_ns = 251, .deletes = &.{"absent"} });
    try db.resolveTransactionIntents(noop_txn, .committed, 260);
    {
        var current = try db.core.store.beginReadTxn();
        defer current.abort();
        try std.testing.expectError(error.NotFound, current.get(try preparedCountKey(alloc, noop_txn)));
    }
}

/// Prepared once under the native writer fence, then used for each TTL
/// candidate. A blocked row must not abort expiration of unrelated rows.
pub const ExpirationGuard = struct {
    bindings: []const Binding,
    progress: []const series.Progress,
    decoder: *Decoder,

    pub fn init(a: A, txn: anytype) !ExpirationGuard {
        const bindings = try load(a, txn);
        const progress = try a.alloc(series.Progress, bindings.len);
        for (bindings, progress) |binding, *p| p.* = try (series.Engine{ .alloc = a, .spec = binding.spec }).status(a, txn);
        const decoder = try a.create(Decoder);
        decoder.* = try Decoder.init(a, bindings);
        return .{ .bindings = bindings, .progress = progress, .decoder = decoder };
    }
    pub fn allowed(self: ExpirationGuard, a: A, txn: anytype, physical: []const u8, admitted_ns: i128) !bool {
        if (self.bindings.len == 0) return true;
        const now = admitted_ns;
        const value = try self.decoder.read(txn, physical);
        for (self.bindings, self.progress) |binding, p| {
            const row = try normalizeImage(a, binding, value);
            if (!try (series.Engine{ .alloc = a, .spec = binding.spec }).expirationReady(a, txn, p, row, now)) return false;
        }
        return true;
    }
};

test "retained series typed dimensions canonicalize numeric spellings offsets and integer identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const binding = Binding{ .spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .laws = &.{.count}, .count_nonnull = &.{true} }, .timestamp = "ts", .dimensions = &.{ "n", "at", "id" }, .dimension_types = &.{ .number, .datetime, .integer }, .fields = &.{"value"} };
    try binding.validate();
    const first = (try std.json.parseFromSlice(std.json.Value, a, "{\"ts\":\"1970-01-01T00:00:00Z\",\"n\":1,\"at\":\"1970-01-01T01:00:00+01:00\",\"id\":9007199254740993,\"value\":null}", .{})).value;
    const second = (try std.json.parseFromSlice(std.json.Value, a, "{\"ts\":\"1970-01-01T00:00:00Z\",\"n\":1.0,\"at\":\"1970-01-01T00:00:00Z\",\"id\":9007199254740993,\"value\":\"nonnull\"}", .{})).value;
    const left = try normalizeValue(a, binding, first);
    const right = try normalizeValue(a, binding, second);
    try std.testing.expectEqualStrings(left.dimensions, right.dimensions);
    try std.testing.expect(std.mem.indexOf(u8, left.dimensions, "9007199254740993") != null);
    try std.testing.expectEqual(@as(?f64, null), left.values[0]);
    try std.testing.expectEqual(@as(?f64, 1), right.values[0]);
}

/// This clock counts committed primary changes while bindings exist. Derived
/// replay may omit no-ops or advance for artifacts, so it is never this clock.
pub fn sourceSequence(txn: anytype, fallback: ?u64) !u64 {
    const raw = (try optional(txn, sequence_key)) orelse return fallback orelse error.RetainedSeriesCorrupt;
    if (raw.len != 8) return error.RetainedSeriesCorrupt;
    return std.mem.readInt(u64, raw[0..8], .little);
}
fn saveSequence(txn: anytype, sequence: u64) !void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, sequence, .little);
    try txn.put(sequence_key, &bytes);
}

test "retained series activation fences stale negative cache readers and same transaction writes" {
    const a = std.testing.allocator;
    var backend = @import("mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try @import("docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    try std.testing.expect(!try store.retainedSeriesActive());
    const binding = Binding{ .spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .laws = &.{.count} }, .timestamp = "ts", .fields = &.{null} };
    const key = try keys.documentKeyAlloc(alloc, "row");
    var stale_epoch: u64 = 0;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try install(alloc, &txn, binding, .{ .sequence = 0, .admitted_ns = 0 }, 0);
        stale_epoch = store.retained_series_cache.load(.acquire);
        // A reader that opened the old committed root while installation is
        // in flight can still observe absence. It must not hide these writes.
        cacheAbsent(&store.retained_series_cache, stale_epoch);
        try std.testing.expect(store.retained_series_cache.load(.acquire) & 1 != 0);
        try txn.put(context_key, try std.json.Stringify.valueAlloc(alloc, Context{ .sequence = 1, .admitted_ns = 1 }, .{}));
        try txn.put(key, "{\"ts\":\"1970-01-01T00:00:00.000000001Z\"}");
        try std.testing.expectEqual(@as(u32, 1), txn.retained_series.before.count());
        try txn.commit();
    }
    try std.testing.expect(store.retained_series_cache.load(.acquire) & 1 == 0);
    // Completing a stale read after publication cannot reinstall its result.
    cacheAbsent(&store.retained_series_cache, stale_epoch);
    try std.testing.expect(store.retained_series_cache.load(.acquire) & 1 == 0);
    try std.testing.expect(try store.retainedSeriesActive());
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try backfillPage(alloc, &txn, binding.spec.id, "", 1024));
        try txn.commit();
    }
    var txn = try store.beginReadTxn();
    defer txn.abort();
    const engine = series.Engine{ .alloc = a, .spec = binding.spec };
    const page = try engine.scan(alloc, &txn, 0, 10, 1, null, 10);
    try std.testing.expectEqual(@as(i64, 1), page.rows[0].rows);
    try std.testing.expectEqual(@as(u64, 1), (try engine.status(alloc, &txn)).source_sequence);
}

test "retained series negative cache saturation disables caching without epoch wrap" {
    var cache = Cache.init(uncached_tag - 2);
    const stale = cache.load(.acquire);
    invalidateCache(&cache);
    try std.testing.expectEqual(uncached_tag, cache.load(.acquire));
    cacheAbsent(&cache, stale);
    cacheAbsent(&cache, uncached_tag);
    invalidateCache(&cache);
    try std.testing.expectEqual(uncached_tag, cache.load(.acquire));
}

test "retained series packed row projection caches immutable layouts and skips unrelated wide columns" {
    const a = std.testing.allocator;
    var backend = @import("mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try @import("docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const columns = [_]schema.RelationalColumn{
        .{ .name = "ts", .path = "ts", .column_type = .datetime },
        .{ .name = "v", .path = "v", .column_type = .number },
        .{ .name = "payload", .path = "payload", .column_type = .string },
        .{ .name = "ignored", .path = "ignored", .column_type = .string },
    };
    const version1 = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    const reversed = [_]schema.RelationalColumn{ columns[1], columns[0], columns[2], columns[3] };
    const version2 = schema.TableSchema{ .version = 2, .storage_mode = .relational, .relational_columns = &reversed };
    const wide = try alloc.alloc(u8, 128 * 1024);
    @memset(wide, 'x');
    const cells = [_]rows.Cell{
        .{ .ordinal = 0, .path = "ts", .value_type = .u64_val, .value = .{ .u64_val = 1 } },
        .{ .ordinal = 1, .path = "v", .value_type = .f64_val, .value = .{ .f64_val = 3 } },
        .{ .ordinal = 2, .path = "payload", .value_type = .bytes_val, .value = .{ .bytes_val = wide } },
        .{ .ordinal = 3, .path = "ignored", .value_type = .bytes_val, .value = .{ .bytes_val = wide } },
    };
    const first = try rows.serializeOrdinal(alloc, 1, &columns, &cells, @splat(0));
    var reordered = cells;
    reordered[0] = cells[1];
    reordered[0].ordinal = 0;
    reordered[1] = cells[0];
    reordered[1].ordinal = 1;
    const second = try rows.serializeOrdinal(alloc, 2, &reversed, &reordered, @splat(0));
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(try schema.schemaVersionKeyAlloc(alloc, 1), try schema.serializeSchema(alloc, version1));
        try txn.put(try schema.schemaVersionKeyAlloc(alloc, 2), try schema.serializeSchema(alloc, version2));
        try txn.commit();
    }
    const binding = Binding{ .spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .laws = &.{ .count, .count, .sum }, .count_nonnull = &.{ false, true, false } }, .timestamp = "ts", .dimensions = &.{"ts"}, .dimension_types = &.{.datetime}, .fields = &.{ null, "payload", "v" } };
    var decoder = try Decoder.init(alloc, &.{binding});
    var txn = try store.beginReadTxn();
    defer txn.abort();
    const old = try decoder.read(&txn, first);
    const current = try decoder.read(&txn, second);
    _ = try decoder.read(&txn, first);
    try std.testing.expectEqual(@as(u32, 2), decoder.layouts.count());
    try std.testing.expectEqual(@as(usize, 3), old.value.object.count());
    try std.testing.expect(old.value.object.get("ignored") == null);
    try std.testing.expectEqual(@as(bool, true), old.value.object.get("payload").?.bool);
    const left = try normalizeImage(alloc, binding, old);
    const right = try normalizeImage(alloc, binding, current);
    try std.testing.expectEqual(@as(i128, 1), left.timestamp_ns);
    try std.testing.expectEqualStrings(left.dimensions, right.dimensions);
    try std.testing.expectEqual(@as(?f64, 1), right.values[1]);
    try std.testing.expectEqual(@as(?f64, 3), right.values[2]);
}

test "retained series packed JSON literal null remains distinct from SQL null when projecting nested fields" {
    const a = std.testing.allocator;
    var backend = @import("mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try @import("docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const columns = [_]schema.RelationalColumn{
        .{ .name = "ts", .path = "ts", .column_type = .datetime },
        .{ .name = "doc", .path = "doc", .column_type = .json, .is_json = true, .json_kind = .any, .allows_null = true },
    };
    const table = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(try schema.schemaVersionKeyAlloc(alloc, 1), try schema.serializeSchema(alloc, table));
        try txn.commit();
    }
    var cells = [_]rows.Cell{
        .{ .ordinal = 0, .path = "ts", .value_type = .u64_val, .value = .{ .u64_val = 1 } },
        .{ .ordinal = 1, .path = "doc", .value_type = .bytes_val, .is_json = true, .value = .{ .bytes_val = "null" } },
    };
    const json_null = try rows.serializeOrdinal(alloc, 1, &columns, &cells, @splat(0));
    cells[1].is_null = true;
    const sql_null = try rows.serializeOrdinal(alloc, 1, &columns, &cells, @splat(0));
    cells[1].is_null = false;
    cells[1].value.bytes_val = "{\"x\":null}";
    const nested_null = try rows.serializeOrdinal(alloc, 1, &columns, &cells, @splat(0));
    const binding = Binding{ .spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .laws = &.{ .count, .count }, .count_nonnull = &.{ true, true } }, .timestamp = "ts", .fields = &.{ "doc", "doc.x" } };
    var decoder = try Decoder.init(alloc, &.{binding});
    var txn = try store.beginReadTxn();
    defer txn.abort();
    const literal = try normalizeImage(alloc, binding, try decoder.read(&txn, json_null));
    const sql = try normalizeImage(alloc, binding, try decoder.read(&txn, sql_null));
    const nested = try normalizeImage(alloc, binding, try decoder.read(&txn, nested_null));
    try std.testing.expectEqual(@as(?f64, 1), literal.values[0]);
    try std.testing.expectEqual(@as(?f64, null), literal.values[1]);
    try std.testing.expectEqual(@as(?f64, null), sql.values[0]);
    try std.testing.expectEqual(@as(?f64, null), sql.values[1]);
    try std.testing.expectEqual(@as(?f64, 1), nested.values[0]);
    try std.testing.expectEqual(@as(?f64, null), nested.values[1]);
}

test "retained series native prepare plans vote atomically and resolution markers release commit and abort evidence" {
    const a = std.testing.allocator;
    var backend = @import("mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try @import("docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const primary = try keys.documentKeyAlloc(temp, "a");
    const first = "{\"ts\":\"1970-01-01T00:00:00.000000001Z\",\"v\":3}";
    const second = "{\"ts\":\"1970-01-01T00:00:00.000000001Z\",\"v\":9}";
    try store.put(primary, first);
    const binding = Binding{ .spec = .{ .id = @splat(1), .definition = @splat(2), .generation = 1, .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .retention_ns = 200, .laws = &.{ .count, .min, .max } }, .timestamp = "ts", .fields = &.{ null, "v", "v" } };
    const engine = series.Engine{ .alloc = a, .spec = binding.spec };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try install(temp, &txn, binding, .{ .sequence = 0, .admitted_ns = 1 }, 0);
        try std.testing.expect(try backfillPage(temp, &txn, binding.spec.id, "", 1024));
        try txn.commit();
    }
    const effects = planned: {
        var read = try store.beginReadTxn();
        defer read.abort();
        break :planned try planPrepare(temp, &read, @splat(1), 109, &.{.{ .key = primary, .logical_key = "a", .after = second }});
    };
    try std.testing.expectEqual(@as(usize, 1), effects.costs.len);
    try std.testing.expectEqualStrings("a", effects.costs[0].key);
    try std.testing.expect(effects.costs[0].bytes != 0);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        for (effects.writes) |write| try txn.put(write.key, write.value);
        for (effects.deletes) |key| try txn.delete(key);
        // A vote publishes only intent metadata; the primary is unchanged.
        try std.testing.expectEqualStrings(first, try txn.get(primary));
        try txn.commit();
    }
    const original_budget = budget: {
        var read = try store.beginReadTxn();
        defer read.abort();
        break :budget try temp.dupe(u8, try read.get(try preparedCountKey(temp, @splat(1))));
    };
    for ([_]PreparedBudget{ .{ .count = series.max_changes }, .{ .count = 1, .after = max_capture_bytes } }) |full| {
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(try preparedCountKey(temp, @splat(1)), try std.json.Stringify.valueAlloc(temp, full, .{}));
            try txn.commit();
        }
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.RetainedSeriesBatchTooLarge, planPrepare(temp, &read, @splat(1), 109, &.{.{ .key = try keys.documentKeyAlloc(temp, "another"), .logical_key = "another", .after = second }}));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(try preparedCountKey(temp, @splat(1)), original_budget);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(@as(usize, 0), try engine.tick(&txn, 120, 1024));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(context_key, try std.json.Stringify.valueAlloc(temp, Context{ .sequence = 1, .admitted_ns = 120, .resolution = .{ .transaction = @splat(1), .decision = .committed } }, .{}));
        try txn.put(primary, second);
        try txn.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const page = try engine.scan(temp, &read, 0, 10, 120, null, 10);
        try std.testing.expectEqual(@as(?f64, 9), page.rows[0].metrics[1].value(.min));
        try std.testing.expectEqual(@as(u64, 1), try sourceSequence(&read, null));
        try std.testing.expectError(error.NotFound, read.get(try preparedCountKey(temp, @splat(1))));
    }
    const fresh = try keys.documentKeyAlloc(temp, "fresh");
    const fresh_value = "{\"ts\":\"1970-01-01T00:00:00.000000121Z\",\"v\":7}";
    const aborted = planned: {
        var read = try store.beginReadTxn();
        defer read.abort();
        break :planned try planPrepare(temp, &read, @splat(2), 120, &.{.{ .key = fresh, .logical_key = "fresh", .after = fresh_value }});
    };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        for (aborted.writes) |write| try txn.put(write.key, write.value);
        for (aborted.deletes) |key| try txn.delete(key);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(context_key, try std.json.Stringify.valueAlloc(temp, Context{ .sequence = 2, .admitted_ns = 250, .resolution = .{ .transaction = @splat(2), .decision = .aborted } }, .{}));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(@as(u64, 1), try sourceSequence(&txn, null));
        try std.testing.expectEqual(@as(usize, 0), try engine.releasePrepared(&txn, @splat(2)));
        try std.testing.expectError(error.NotFound, txn.get(try preparedCountKey(temp, @splat(2))));
        _ = try engine.tick(&txn, 250, 1024);
        _ = try engine.requireClosedSpan(temp, &txn, .{ .start_ns = "120", .end_ns = "130" }, null);
        try txn.commit();
    }
}
