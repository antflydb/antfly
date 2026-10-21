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

//! Durable, receipt-bound semantic read predicates. Only explicitly classified
//! data domains may be fenced; mandatory progress namespaces stay outside them.
const std = @import("std");
const admission = @import("../common/read_admission.zig");
const reads = @import("db/prepared_read_set.zig");
const effects = @import("db/prepared_transaction_effects.zig");
const legacy = @import("transaction_read_guards.zig");
const A = std.mem.Allocator;
const versions = @import("db/catalog/reader_versions.zig");
const keys = @import("internal_keys.zig");
const transactions = @import("transactions.zig");
const backlog = @import("publication_backlog.zig");
pub fn requireFloors(txn: anytype) !void {
    const Floors = struct {
        txn: @TypeOf(txn),
        pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
            const raw = try self.txn.get(selected);
            if (raw.len > 10) return error.InvalidTxnRecord;
            return raw;
        }
    };
    var floors: Floors = .{ .txn = txn };
    const catalog = @import("db/catalog/resolver_catalog.zig");
    const minimum = try catalog.readMinimumReaderVersion(&floors, versions.supported_reader_version);
    const authoritative = try catalog.readAuthoritativeReaderVersion(&floors, versions.supported_reader_version);
    if (minimum < versions.prepared_read_guards_reader_version or authoritative < versions.prepared_read_guards_reader_version) return error.PreparedTransactionProtocolDisabled;
}
fn namespaceMatches(txn: anytype, namespace: [24]u8) !void {
    const raw = txn.get(&keys.identity_namespace_key) catch |cause| return if (cause == error.NotFound) error.PreparedEffectsMismatch else cause;
    if (!std.mem.eql(u8, raw, &namespace)) return error.PreparedEffectsMismatch;
}
fn bundleMatches(txn: anytype, record: *const Record, control: ?admission.Port) !void {
    try namespaceMatches(txn, record.namespace);
    const raw = try txn.get(&transactions.makePreparedEffectsKey(record.receipt.txn_id));
    const inspection = try effects.inspect(raw, control);
    if (!std.meta.eql(inspection.fingerprint, record.receipt.frame_digest) or
        !std.meta.eql(inspection.contract.txn_id, record.receipt.txn_id) or
        inspection.contract.mode != record.receipt.requirement.mode or
        inspection.contract.intent_revision != record.receipt.intent_revision or
        !std.meta.eql(inspection.contract.intent_digest, record.receipt.intent_digest) or
        !std.meta.eql(inspection.contract.catalog_digest, record.receipt.catalog_digest)) return error.PreparedEffectsMismatch;
    const observation = try txn.get(&transactions.makePreparedReadObservationsKey(record.receipt.txn_id));
    if (!std.meta.eql(try digest(observation, control), record.observations_digest)) return error.PreparedEffectsMismatch;
    const requirement = try effects.Requirement.decode(try txn.get(&transactions.makePreparationRequirementKey(record.receipt.txn_id)));
    if (!std.meta.eql(requirement, record.receipt.requirement)) return error.PreparedEffectsMismatch;
    try transactions.validatePreparedReadGuardRecord(txn, record.receipt, false);
}
fn bundleKey(selected: []const u8) bool {
    for ([_][]const u8{ backlog.prepared_effects_prefix, backlog.prepared_observations_prefix, backlog.preparation_requirement_prefix, transactions.records_prefix, "\x00\x00__txn_schema_leases__:" }) |pfx| {
        if (std.mem.startsWith(u8, selected, pfx)) return true;
    }
    return false;
}
fn globalKey(selected: []const u8) bool {
    return @import("db/artifact_inventory.zig").isPreparedCatalogKey(selected) or
        std.mem.eql(u8, selected, &keys.identity_namespace_key) or
        std.mem.eql(u8, selected, versions.minimum_reader_key) or std.mem.eql(u8, selected, versions.replacement_reader_base_key);
}
pub const prefix = "\x00\x00__txn_prepared_read_guards_v1__:";
pub const state_key = "\x00\x00__metadata__:prepared_read_guards_v1";
pub const max_records = 64;
pub const max_bytes = 16 * 1024 * 1024;
pub const max_predicates = 65_536;
pub fn key(id: [16]u8) [prefix.len + 16]u8 {
    return prefix.* ++ id;
}
fn checkpoint(control: ?admission.Port) !void {
    if (control) |port| try port.checkpoint();
}
fn copy(a: A, source: []const u8, control: ?admission.Port) ![]u8 {
    try checkpoint(control);
    const out = try a.alloc(u8, source.len);
    errdefer a.free(out);
    var offset: usize = 0;
    while (offset < source.len) {
        try checkpoint(control);
        const end = offset + @min(source.len - offset, 4096);
        @memcpy(out[offset..end], source[offset..end]);
        offset = end;
    }
    return out;
}
pub fn observationsDigest(source: []const u8, control: ?admission.Port) ![32]u8 {
    return digest(source, control);
}
fn digest(source: []const u8, control: ?admission.Port) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < source.len) {
        try checkpoint(control);
        const end = offset + @min(source.len - offset, 4096);
        hash.update(source[offset..end]);
        offset = end;
    }
    try checkpoint(control);
    return hash.finalResult();
}
fn compare(left: []const u8, right: []const u8, control: ?admission.Port) !std.math.Order {
    var offset: usize = 0;
    const length = @min(left.len, right.len);
    while (offset < length) {
        try checkpoint(control);
        const end = offset + @min(length - offset, 4096);
        const order = std.mem.order(u8, left[offset..end], right[offset..end]);
        if (order != .eq) return order;
        offset = end;
    }
    try checkpoint(control);
    return std.math.order(left.len, right.len);
}
const domains = [_]struct { lower: []const u8, upper: []const u8 }{
    .{ .lower = "\x01", .upper = "\x02" },
    // Forward/ownership relational index records are data, despite sharing the
    // private metadata prefix. Job heads/counters/leases are outside this span.
    .{ .lower = "\x00\x00R\x01", .upper = "\x00\x00R\x03" },
};
pub fn dataKey(selected: []const u8) bool {
    for (domains) |domain| if (std.mem.order(u8, selected, domain.lower) != .lt and std.mem.order(u8, selected, domain.upper) == .lt) return true;
    return false;
}
fn validSpan(span: reads.Footprint.Span, control: ?admission.Port) !void {
    const upper = span.upper orelse return error.Unsupported;
    const order = try compare(span.lower, upper, control);
    if (order == .gt or (order == .eq and (!span.lower_inclusive or !span.upper_inclusive))) return error.InvalidTxnRecord;
    for (domains) |domain| {
        if (try compare(span.lower, domain.lower, control) == .lt) continue;
        const end = try compare(upper, domain.upper, control);
        if (end == .lt or (end == .eq and !span.upper_inclusive)) return;
    }
    return error.Unsupported;
}
pub fn requireDataPredicates(footprint: *const reads.Footprint, control: ?admission.Port) !void {
    for ([_][]const reads.Footprint.Span{ footprint.spans, footprint.membership_spans }) |spans| {
        var previous: ?reads.Footprint.Span = null;
        for (spans) |span| {
            try validSpan(span, control);
            if (previous) |last| {
                const order = try compare(last.upper.?, span.lower, control);
                if (order == .gt or (order == .eq and (last.upper_inclusive or span.lower_inclusive))) return error.InvalidTxnRecord;
            }
            previous = span;
        }
    }
}

pub const State = struct {
    records: u64 = 0,
    bytes: u64 = 0,
    predicates: u64 = 0,
    pub fn admit(self: State, bytes: u64, predicates: u64) !State {
        const next: State = .{
            .records = std.math.add(u64, self.records, 1) catch return error.Backpressured,
            .bytes = std.math.add(u64, self.bytes, bytes) catch return error.Backpressured,
            .predicates = std.math.add(u64, self.predicates, predicates) catch return error.Backpressured,
        };
        if (next.records > max_records or next.bytes > max_bytes or next.predicates > max_predicates) return error.Backpressured;
        return next;
    }
    pub fn encode(self: State) [60]u8 {
        var raw: [60]u8 = undefined;
        @memcpy(raw[0..4], "PRGS");
        std.mem.writeInt(u64, raw[4..12], self.records, .little);
        std.mem.writeInt(u64, raw[12..20], self.bytes, .little);
        std.mem.writeInt(u64, raw[20..28], self.predicates, .little);
        std.crypto.hash.sha2.Sha256.hash(raw[0..28], raw[28..60], .{});
        return raw;
    }
    fn decode(raw: []const u8) !State {
        if (raw.len != 60 or !std.mem.eql(u8, raw[0..4], "PRGS")) return error.InvalidTxnRecord;
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..28], &hash, .{});
        if (!std.mem.eql(u8, &hash, raw[28..60])) return error.InvalidTxnRecord;
        const result: State = .{ .records = std.mem.readInt(u64, raw[4..12], .little), .bytes = std.mem.readInt(u64, raw[12..20], .little), .predicates = std.mem.readInt(u64, raw[20..28], .little) };
        if (result.records == 0 or result.records > max_records or result.bytes > max_bytes or result.predicates > max_predicates) return error.InvalidTxnRecord;
        return result;
    }
};
pub fn loadState(txn: anytype) !State {
    const raw = txn.get(state_key) catch |err| return if (err == error.NotFound) .{} else err;
    return State.decode(raw);
}
pub const Record = struct {
    receipt: effects.BoundReceipt,
    namespace: [24]u8,
    observations_digest: [32]u8,
    predicates: reads.Footprint,
    encoded_bytes: usize = 0,
    pub fn deinit(self: *Record) void {
        self.predicates.deinit();
        self.* = undefined;
    }
    pub fn count(self: *const Record) usize {
        return self.predicates.spans.len + self.predicates.membership_spans.len;
    }
};
const Writer = struct {
    data: ?[]u8 = null,
    offset: usize = 0,
    control: ?admission.Port,
    fn bytes(self: *@This(), value: []const u8) !void {
        try checkpoint(self.control);
        const end = std.math.add(usize, self.offset, value.len) catch return error.TransactionTooLarge;
        if (end > max_bytes - 32) return error.TransactionTooLarge;
        if (self.data) |raw| {
            if (end > raw.len - 32) return error.InvalidTxnRecord;
            var start: usize = 0;
            while (start < value.len) {
                try checkpoint(self.control);
                const next = start + @min(value.len - start, 4096);
                @memcpy(raw[self.offset + start .. self.offset + next], value[start..next]);
                start = next;
            }
        }
        self.offset = end;
    }
    fn int(self: *@This(), comptime T: type, value: T) !void {
        var raw: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &raw, value, .little);
        try self.bytes(&raw);
    }
    fn slice(self: *@This(), value: []const u8) !void {
        try self.int(u32, std.math.cast(u32, value.len) orelse return error.TransactionTooLarge);
        try self.bytes(value);
    }
};
fn write(writer: *Writer, record: *const Record, size: u32) !void {
    try requireDataPredicates(&record.predicates, writer.control);
    if (record.count() > max_predicates or record.receipt.intent_revision == 0) return error.TransactionTooLarge;
    try writer.bytes("PRGR");
    try writer.int(u16, 1);
    try writer.int(u16, 0);
    try writer.int(u32, size);
    try writer.bytes(&record.namespace);
    try writer.bytes(&record.receipt.txn_id);
    try writer.int(u8, @backingInt(record.receipt.requirement.mode));
    try writer.bytes(&(try record.receipt.binding.encode()));
    try writer.int(u64, record.receipt.intent_revision);
    try writer.bytes(&record.receipt.intent_digest);
    try writer.bytes(&record.receipt.catalog_digest);
    try writer.bytes(&record.receipt.frame_digest);
    try writer.bytes(&record.observations_digest);
    try writer.int(u32, @intCast(record.predicates.spans.len));
    try writer.int(u32, @intCast(record.predicates.membership_spans.len));
    for ([_][]const reads.Footprint.Span{ record.predicates.spans, record.predicates.membership_spans }) |spans| for (spans) |span| {
        try writer.int(u8, @as(u8, @intFromBool(span.lower_inclusive)) | (@as(u8, @intFromBool(span.upper_inclusive)) << 1));
        try writer.slice(span.lower);
        try writer.slice(span.upper.?);
    };
}
pub fn encode(a: A, record: *const Record, original: ?admission.Port) ![]u8 {
    var trace: admission.RejectionTrace = .{};
    return encodeInner(a, record, trace.bind(original)) catch |err| return trace.failure orelse err;
}
fn encodeInner(a: A, record: *const Record, control: ?admission.Port) ![]u8 {
    var size: Writer = .{ .control = control };
    try write(&size, record, 0);
    const raw = try a.alloc(u8, size.offset + 32);
    errdefer a.free(raw);
    var writer: Writer = .{ .data = raw, .control = control };
    try write(&writer, record, @intCast(raw.len));
    raw[writer.offset..][0..32].* = try digest(raw[0..writer.offset], control);
    return raw;
}
const Reader = struct {
    raw: []const u8,
    offset: usize = 0,
    control: ?admission.Port,
    fn bytes(self: *@This(), length: usize) ![]const u8 {
        try checkpoint(self.control);
        if (length > self.raw.len - self.offset) return error.InvalidTxnRecord;
        const value = self.raw[self.offset..][0..length];
        self.offset += length;
        return value;
    }
    fn int(self: *@This(), comptime T: type) !T {
        return std.mem.readInt(T, (try self.bytes(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    fn slice(self: *@This()) ![]const u8 {
        const length = try self.int(u32);
        return self.bytes(length);
    }
    fn spans(self: *@This(), count_: u32, output: ?[]reads.Footprint.Span, a: A) !void {
        var previous: ?reads.Footprint.Span = null;
        for (0..count_) |i| {
            const flags = try self.int(u8);
            if (flags & 0xfc != 0) return error.InvalidTxnRecord;
            const span: reads.Footprint.Span = .{ .lower = try self.slice(), .upper = try self.slice(), .lower_inclusive = flags & 1 != 0, .upper_inclusive = flags & 2 != 0 };
            validSpan(span, self.control) catch |err| return if (err == error.Unsupported) error.InvalidTxnRecord else err;
            if (previous) |last| {
                const order = try compare(last.upper.?, span.lower, self.control);
                if (order == .gt or (order == .eq and (last.upper_inclusive or span.lower_inclusive))) return error.InvalidTxnRecord;
            }
            previous = span;
            if (output) |out| out[i] = .{ .lower = try copy(a, span.lower, self.control), .upper = try copy(a, span.upper.?, self.control), .lower_inclusive = span.lower_inclusive, .upper_inclusive = span.upper_inclusive };
        }
    }
};
pub fn decode(a: A, raw: []const u8, original: ?admission.Port) !Record {
    var trace: admission.RejectionTrace = .{};
    return decodeInner(a, raw, trace.bind(original)) catch |err| return trace.failure orelse err;
}
fn decodeInner(a: A, raw: []const u8, control: ?admission.Port) !Record {
    if (raw.len < 309 or raw.len > max_bytes) return error.InvalidTxnRecord;
    const end = raw.len - 32;
    if (!std.mem.eql(u8, &(try digest(raw[0..end], control)), raw[end..])) return error.InvalidTxnRecord;
    var reader: Reader = .{ .raw = raw[0..end], .control = control };
    if (!std.mem.eql(u8, try reader.bytes(4), "PRGR")) return error.InvalidTxnRecord;
    if (try reader.int(u16) != 1) return error.UnsupportedVersion;
    if (try reader.int(u16) != 0 or try reader.int(u32) != raw.len) return error.InvalidTxnRecord;
    const namespace = (try reader.bytes(24))[0..24].*;
    var receipt: effects.BoundReceipt = undefined;
    receipt.txn_id = (try reader.bytes(16))[0..16].*;
    receipt.requirement.mode = std.enums.fromInt(effects.Mode, try reader.int(u8)) orelse return error.InvalidTxnRecord;
    receipt.binding = try effects.Binding.decode(try reader.bytes(80));
    receipt.intent_revision = try reader.int(u64);
    receipt.intent_digest = (try reader.bytes(32))[0..32].*;
    receipt.catalog_digest = (try reader.bytes(32))[0..32].*;
    receipt.frame_digest = (try reader.bytes(32))[0..32].*;
    const observations_digest = (try reader.bytes(32))[0..32].*;
    const values = try reader.int(u32);
    const memberships = try reader.int(u32);
    if (receipt.intent_revision == 0 or values > max_predicates or memberships > max_predicates or @as(u64, values) + memberships > max_predicates or (@as(u64, values) + memberships) * 9 > reader.raw.len - reader.offset) return error.InvalidTxnRecord;
    const body = reader.offset;
    try reader.spans(values, null, a);
    try reader.spans(memberships, null, a);
    if (reader.offset != reader.raw.len) return error.InvalidTxnRecord;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const value_spans = try arena.allocator().alloc(reads.Footprint.Span, values);
    const membership_spans = try arena.allocator().alloc(reads.Footprint.Span, memberships);
    reader.offset = body;
    try reader.spans(values, value_spans, arena.allocator());
    try reader.spans(memberships, membership_spans, arena.allocator());
    return .{ .receipt = receipt, .namespace = namespace, .observations_digest = observations_digest, .predicates = .{ .arena = arena, .spans = value_spans, .membership_spans = membership_spans }, .encoded_bytes = raw.len };
}

/// Per-writer immutable registry view. Metadata cannot remove another owner
/// from this view. Only a verified terminal resolution can skip its own entry.
pub const Capture = struct {
    scope: ?legacy.Scope = null,
    failure: ?anyerror = null,
    loaded: bool = false,
    empty_audited: bool = false,
    state: State = .{},
    records: std.ArrayListUnmanaged(Record) = .empty,
    mutations: std.StringHashMapUnmanaged(bool) = .empty,
    installing: ?[16]u8 = null,
    install_digest: ?[32]u8 = null,
    install_bytes: usize = 0,
    install_predicates: usize = 0,
    retiring: ?[16]u8 = null,
    metadata_touched: bool = false,
    bundle_touched: bool = false,
    unguarded: bool = false,
    pub fn deinit(self: *Capture, a: A) void {
        for (self.records.items) |*record| record.deinit();
        self.records.deinit(a);
        var it = self.mutations.keyIterator();
        while (it.next()) |selected| a.free(selected.*);
        self.mutations.deinit(a);
    }
    pub fn check(self: *const Capture) !void {
        if (self.failure) |cause| return cause;
    }
    pub fn setScope(self: *Capture, scope: legacy.Scope) !void {
        try self.check();
        if (self.scope) |old| if (!std.meta.eql(old, scope)) {
            self.failure = error.InvalidTxnRecord;
            return error.InvalidTxnRecord;
        };
        self.scope = scope;
    }
    pub fn ensureLoaded(self: *Capture, a: A, txn: anytype, debt: anytype, gate: ?*std.atomic.Mutex, control: ?admission.Port) !void {
        try self.check();
        self.loadInner(a, txn, debt, gate, control) catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn loadInner(self: *Capture, a: A, txn: anytype, debt: anytype, gate: ?*std.atomic.Mutex, control: ?admission.Port) !void {
        try checkpoint(control);
        try debt.acquireMutationGate(gate);
        if (self.loaded) return;
        self.state = try loadState(txn);
        // Ordinary unguarded writes must not rotate/pin a physical cursor.
        // Cold registration separately audits for imported orphan entries.
        if (self.state.records == 0) {
            self.loaded = true;
            return;
        }
        var cursor = try txn.openPhysicalCursorAdapter();
        defer cursor.close();
        var next = try cursor.seekAtOrAfter(prefix);
        var bytes: u64 = 0;
        var predicates: u64 = 0;
        while (next) |row| : (next = try cursor.next()) {
            try checkpoint(control);
            if (!std.mem.startsWith(u8, row.key, prefix)) break;
            if (row.key.len != prefix.len + 16 or self.records.items.len >= self.state.records or self.records.items.len == max_records or row.value.len > max_bytes - bytes) return error.InvalidTxnRecord;
            var record = try decode(a, row.value, control);
            errdefer record.deinit();
            if (!std.mem.eql(u8, &record.receipt.txn_id, row.key[prefix.len..])) return error.InvalidTxnRecord;
            bytes += record.encoded_bytes;
            try namespaceMatches(txn, record.namespace);
            predicates += record.count();
            if (predicates > max_predicates) return error.InvalidTxnRecord;
            try self.records.append(a, record);
        }
        if (self.state.records != self.records.items.len or self.state.bytes != bytes or self.state.predicates != predicates) return error.InvalidTxnRecord;
        self.loaded = true;
    }
    pub fn auditEmpty(self: *Capture, txn: anytype, control: ?admission.Port) !void {
        try self.check();
        self.auditEmptyInner(txn, control) catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn auditEmptyInner(self: *Capture, txn: anytype, control: ?admission.Port) !void {
        if (!self.loaded or self.installing != null or self.retiring != null) return error.InvalidTxnRecord;
        if (self.state.records != 0 or self.empty_audited) return;
        try checkpoint(control);
        var cursor = try txn.openPhysicalCursorAdapter();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(prefix)) |entry| if (std.mem.startsWith(u8, entry.key, prefix)) return error.InvalidTxnRecord;
        self.empty_audited = true;
    }
    pub fn touch(self: *Capture, a: A, txn: anytype, debt: anytype, gate: ?*std.atomic.Mutex, selected: []const u8, candidate: ?[]const u8) !void {
        try self.check();
        self.touchInner(a, txn, debt, gate, selected, candidate) catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn touchInner(self: *Capture, a: A, txn: anytype, debt: anytype, gate: ?*std.atomic.Mutex, selected: []const u8, candidate: ?[]const u8) !void {
        const metadata = std.mem.startsWith(u8, selected, prefix) or std.mem.eql(u8, selected, state_key);
        const global = globalKey(selected);
        const bundle = bundleKey(selected);
        if (!metadata and !dataKey(selected) and !global and !bundle) return;
        if (metadata) {
            const actor = self.installing orelse self.retiring orelse return error.InvalidTxnRecord;
            if (!std.mem.eql(u8, selected, state_key) and !std.mem.eql(u8, selected, &key(actor))) return error.InvalidTxnRecord;
        }
        try self.ensureLoaded(a, txn, debt, gate, null);
        if (metadata) {
            self.metadata_touched = true;
            return;
        }
        if (global) {
            // Exact retries may republish the same catalog/floor/namespace.
            // A new registration cannot inherit staged, unvalidated globals.
            if (self.state.records == 0 and self.installing == null) {
                self.unguarded = true;
                return;
            }
            const previous: ?[]const u8 = txn.get(selected) catch |err| if (err == error.NotFound) null else return err;
            if (previous == null and candidate == null) return;
            if (previous) |before| if (candidate) |after| if (std.mem.eql(u8, before, after)) return;
            return error.IntentConflict;
        }
        if (global or bundle) {
            self.bundle_touched = true;
            if (bundle) for (self.records.items) |record| {
                const id = record.receipt.txn_id;
                if (std.mem.endsWith(u8, selected, &id)) {
                    if (self.retiring) |actor| if (std.meta.eql(actor, id)) return;
                    if (std.mem.startsWith(u8, selected, transactions.records_prefix)) return;
                    return error.IntentConflict;
                }
            };
            return;
        }
        if (self.state.records == 0) {
            self.unguarded = true;
            return;
        }
        if (!self.mutations.contains(selected)) {
            const owned = try a.dupe(u8, selected);
            errdefer a.free(owned);
            try self.mutations.put(a, owned, try txn.contains(selected));
        }
    }
    /// The caller acquires the capture's gate before compiling observations.
    /// Admission is separate from data mutations; only owned bytes persist.
    pub fn install(self: *Capture, a: A, txn: anytype, record: *const Record, control: ?admission.Port) !void {
        try self.check();
        self.installInner(a, txn, record, control) catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn installInner(self: *Capture, a: A, txn: anytype, record: *const Record, control: ?admission.Port) !void {
        if (!self.loaded or self.unguarded or self.mutations.count() != 0 or self.retiring != null or self.installing != null) return error.InvalidTxnRecord;
        if (self.state.records == 0 and !self.empty_audited) return error.InvalidTxnRecord;
        const scope = self.scope orelse return error.InvalidTxnRecord;
        if (scope.phase != .admission or !std.meta.eql(scope.txn_id, record.receipt.txn_id)) return error.InvalidTxnRecord;
        try requireFloors(txn);
        try bundleMatches(txn, record, control);
        try transactions.validateReadGuardOwner(txn, scope);
        const raw = try encode(a, record, control);
        defer a.free(raw);
        for (self.records.items) |old| if (std.meta.eql(old.receipt.txn_id, record.receipt.txn_id)) {
            const previous = try txn.get(&key(record.receipt.txn_id));
            if (!std.mem.eql(u8, raw, previous)) return error.PreparedEffectsConflict;
            return;
        };
        const next = try self.state.admit(raw.len, record.count());
        self.installing = record.receipt.txn_id;
        self.install_digest = try digest(raw, control);
        self.install_bytes = raw.len;
        self.install_predicates = record.count();
        try txn.put(&key(record.receipt.txn_id), raw);
        try txn.put(state_key, &next.encode());
    }
    pub fn retire(self: *Capture, a: A, txn: anytype, debt: anytype, gate: ?*std.atomic.Mutex, id: [16]u8) !void {
        try self.check();
        self.retireInner(a, txn, debt, gate, id) catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn retireInner(self: *Capture, a: A, txn: anytype, debt: anytype, gate: ?*std.atomic.Mutex, id: [16]u8) !void {
        const scope = self.scope orelse return error.InvalidTxnRecord;
        if (self.retiring) |old| return if (std.mem.eql(u8, &old, &id)) {} else error.InvalidTxnRecord;
        if (scope.phase != .resolution or !std.mem.eql(u8, &scope.txn_id, &id) or self.installing != null) return error.InvalidTxnRecord;
        try self.ensureLoaded(a, txn, debt, gate, null);
        for (self.records.items) |record| if (std.mem.eql(u8, &record.receipt.txn_id, &id)) {
            self.retiring = id;
            var state = self.state;
            state.records -= 1;
            state.bytes -= record.encoded_bytes;
            state.predicates -= record.count();
            try txn.delete(&key(id));
            if (state.records == 0) try txn.delete(state_key) else try txn.put(state_key, &state.encode());
            return;
        };
    }
    pub fn validate(self: *Capture, a: A, txn: anytype) !void {
        try self.check();
        self.validateInner(a, txn) catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn validateInner(self: *Capture, a: A, txn: anytype) !void {
        if (!self.loaded) return;
        if (self.installing) |id| {
            if (self.unguarded or self.mutations.count() != 0 or self.retiring != null) return error.InvalidTxnRecord;
            const raw = try txn.get(&key(id));
            if (!std.mem.eql(u8, &(try digest(raw, null)), &(self.install_digest orelse return error.InvalidTxnRecord))) return error.InvalidTxnRecord;
            try requireFloors(txn);
            var installed = try decode(a, raw, null);
            defer installed.deinit();
            try bundleMatches(txn, &installed, null);
            try transactions.validateReadGuardOwner(txn, .{ .txn_id = id, .phase = .admission });
        }
        for (self.records.items) |*record| {
            if (self.retiring) |id| if (std.mem.eql(u8, &id, &record.receipt.txn_id)) {
                if (try txn.contains(&key(id))) return error.InvalidTxnRecord;
                try transactions.validatePreparedReadGuardRecord(txn, record.receipt, true);
                continue;
            };
            if (self.bundle_touched) try bundleMatches(txn, record, null);
            var it = self.mutations.iterator();
            while (it.next()) |mutation| if (try record.predicates.conflictsMutation(mutation.key_ptr.*, mutation.value_ptr.*, try txn.contains(mutation.key_ptr.*), null)) return error.IntentConflict;
        }
        if (self.metadata_touched) {
            var expected = self.state;
            if (self.retiring) |id| for (self.records.items) |record| if (std.mem.eql(u8, &id, &record.receipt.txn_id)) {
                expected.records -= 1;
                expected.bytes -= record.encoded_bytes;
                expected.predicates -= record.count();
            };
            if (self.installing != null) {
                expected.records += 1;
                expected.bytes += self.install_bytes;
                expected.predicates += self.install_predicates;
            }
            if (!std.meta.eql(expected, try loadState(txn))) return error.InvalidTxnRecord;
        }
    }
};

test {
    _ = @import("prepared_read_guards_test.zig");
}

test "prepared transaction effects semantic registry quotas reject every dimension and corrupt aggregate" {
    const t = std.testing;
    const boundary: State = .{ .records = max_records - 1, .bytes = max_bytes - 309, .predicates = max_predicates - 1 };
    const full = try boundary.admit(309, 1);
    try t.expectEqualDeep(State{ .records = max_records, .bytes = max_bytes, .predicates = max_predicates }, full);
    try t.expectError(error.Backpressured, full.admit(0, 0));
    try t.expectError(error.Backpressured, (State{ .bytes = max_bytes }).admit(1, 0));
    try t.expectError(error.Backpressured, (State{ .predicates = max_predicates }).admit(309, 1));
    try t.expectError(error.Backpressured, (State{ .bytes = std.math.maxInt(u64) }).admit(1, 0));
    const encoded = full.encode();
    try t.expectEqualDeep(full, try State.decode(&encoded));
    for (0..encoded.len) |offset| {
        var damaged = encoded;
        damaged[offset] ^= 1;
        try t.expectError(error.InvalidTxnRecord, State.decode(&damaged));
        try t.expectError(error.InvalidTxnRecord, State.decode(encoded[0..offset]));
    }
    try t.expectError(error.InvalidTxnRecord, State.decode(&(State{}).encode()));
}

test "prepared transaction effects semantic registry empty writer avoids physical cursor and cold admission audits it" {
    const View = struct {
        gets: usize = 0,
        cursors: usize = 0,
        pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
            _ = selected;
            self.gets += 1;
            return error.NotFound;
        }
        pub fn contains(_: *@This(), _: []const u8) !bool {
            return false;
        }
        pub fn openPhysicalCursorAdapter(self: *@This()) anyerror!@import("backend_erased.zig").Cursor {
            self.cursors += 1;
            return error.Unsupported;
        }
    };
    const Debt = struct {
        acquires: usize = 0,
        pub fn acquireMutationGate(self: *@This(), _: ?*std.atomic.Mutex) !void {
            self.acquires += 1;
        }
    };
    var view: View = .{};
    var debt: Debt = .{};
    var capture: Capture = .{};
    defer capture.deinit(std.testing.allocator);
    try capture.touch(std.testing.allocator, &view, &debt, null, "\x01ordinary", "value");
    try std.testing.expectEqual(@as(usize, 1), view.gets);
    try std.testing.expectEqual(@as(usize, 0), view.cursors);
    try std.testing.expectEqual(@as(usize, 0), capture.mutations.count());
    try std.testing.expectEqual(@as(usize, 1), debt.acquires);
    try std.testing.expectError(error.Unsupported, capture.auditEmpty(&view, null));
    try std.testing.expectEqual(@as(usize, 1), view.cursors);
    try std.testing.expectError(error.Unsupported, capture.check());
}
