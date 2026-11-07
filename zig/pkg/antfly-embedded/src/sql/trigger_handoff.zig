// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Data-only receipts for the existing authenticated private mutation boundary.
//! A digest authenticates nothing by itself. The receiver separately checks the
//! trusted producer's origin, its real pinned schema and atomic old-row guards.
//! Replay verifies these receipts and applies prepared rows without executing
//! trigger programs again. This is distinct from typed-domain row admission.
const std = @import("std");
const manifest = @import("trigger_manifest.zig");
const syntax = @import("trigger_definition.zig");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const max_rows = 16_384;
pub const max_bytes = 8 * 1024 * 1024;
pub const max_receipts = 1024;

pub const Header = struct {
    format: u8 = 1,
    statement_id: [16]u8,
    /// Ordered per-table wave when one logical command revisits a row.
    sequence: u16 = 0,
    metadata_incarnation: [16]u8,
    table: manifest.Table,
    program_digest: Digest,
    events: syntax.Events,
    /// Both phases finish inside the still-unpublished statement transaction.
    /// AFTER is never a post-commit callback or a receiver/shard-local phase.
    phases: enum { complete } = .complete,
    pub fn validate(self: Header) !void {
        if (self.format != 1 or std.mem.allEqual(u8, &self.statement_id, 0) or std.mem.allEqual(u8, &self.metadata_incarnation, 0) or std.mem.allEqual(u8, &self.program_digest, 0)) return error.InvalidSqlTriggerManifest;
        try self.table.validate();
        try self.events.validate(.statement);
    }
    pub fn requireFor(self: Header, table: manifest.Table, incarnation: [16]u8) !void {
        try self.validate();
        if (!self.table.eql(table) or !std.mem.eql(u8, &self.metadata_incarnation, &incarnation)) return error.CatalogGenerationChanged;
    }
};
pub const Preimage = union(enum) {
    absent,
    present: struct { version: u64, digest: Digest },
};
pub const Row = struct {
    key: []const u8,
    event: syntax.Event,
    preimage: Preimage,
    /// Hash of the final canonical wire row and SQL-null provenance, not a
    /// user-visible semantic content hash or a storage-format checksum.
    after: ?Digest,
    /// Explicit original UPDATE targets; trigger-changed cells do not add IDs.
    update_columns: []const u64 = &.{},
    pub fn validate(self: Row) !void {
        if (self.key.len == 0 or self.key.len > 4096 or self.key[0] == 0 or self.key[0] == 1 or self.event == .truncate or self.update_columns.len > 1024) return error.InvalidSqlTriggerManifest;
        if (((self.event == .delete) != (self.after == null)) or (self.event != .update and self.update_columns.len != 0)) return error.InvalidSqlTriggerManifest;
        for (self.update_columns, 0..) |id, i| if (id == 0 or (i != 0 and self.update_columns[i - 1] >= id)) return error.InvalidSqlTriggerManifest;
    }
    /// Must be joined to the same atomic predicate collection used by the
    /// eventual transaction vote, never checked with a fresh standalone read.
    pub fn requirePreimage(self: Row, version: ?u64, digest: ?Digest) !void {
        switch (self.preimage) {
            .absent => if (version != null or digest != null) return error.VersionConflict,
            .present => |expected| if (version == null or digest == null or version.? != expected.version or !std.mem.eql(u8, &digest.?, &expected.digest)) return error.VersionConflict,
        }
    }
};
pub const Envelope = struct {
    header: Header,
    /// Sorted unique user keys. Shard fanout retains only its actual subset;
    /// row indices cannot accidentally refer to another participant's writes.
    rows: []const Row,
    pub fn validate(self: Envelope) !void {
        try self.header.validate();
        if (self.rows.len > max_rows) return error.SqlProgramLimitExceeded;
        for (self.rows, 0..) |row, i| {
            try row.validate();
            if (!self.header.events.contains(row.event) or (i != 0 and std.mem.order(u8, self.rows[i - 1].key, row.key) != .lt)) return error.InvalidSqlTriggerManifest;
        }
    }
    pub fn find(self: Envelope, key: []const u8) ?Row {
        var low: usize = 0;
        var high = self.rows.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, key, self.rows[mid].key)) {
                .lt => high = mid,
                .gt => low = mid + 1,
                .eq => return self.rows[mid],
            }
        }
        return null;
    }
    pub fn encode(self: Envelope, a: A) ![]u8 {
        try self.validate();
        var counter = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(self, .{}, &counter.writer);
        if (counter.fullCount() > max_bytes) return error.SqlProgramLimitExceeded;
        return std.json.Stringify.valueAlloc(a, self, .{});
    }
    pub fn decode(a: A, bytes: []const u8) !std.json.Parsed(Envelope) {
        if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidSqlTriggerManifest;
        var parsed = std.json.parseFromSlice(Envelope, a, bytes, .{ .allocate = .alloc_always }) catch |failure| return if (failure == error.OutOfMemory) failure else error.InvalidSqlTriggerManifest;
        errdefer parsed.deinit();
        const canonical = try parsed.value.encode(a);
        defer a.free(canonical);
        if (!std.mem.eql(u8, canonical, bytes)) return error.InvalidSqlTriggerManifest;
        return parsed;
    }
};

pub const Receipt = struct {
    program: manifest.Manifest,
    processed: Envelope,

    pub fn validate(self: Receipt, a: A) !void {
        try self.program.validate(a);
        try self.processed.validate();
        try self.processed.header.requireFor(self.program.table, self.program.metadata_incarnation);
        if (!std.mem.eql(u8, &try self.program.digest(), &self.processed.header.program_digest)) return error.InvalidSqlTriggerManifest;
    }
};

/// One bounded canonical representation is shared by transport signing,
/// durable staging and receiver admission. Object member order supplied by a
/// caller never becomes a second authority or an unbounded allocation.
pub fn encodeReceipts(a: A, receipts: []const Receipt) ![]u8 {
    if (receipts.len > max_receipts) return error.SqlProgramLimitExceeded;
    for (receipts) |receipt| try receipt.validate(a);
    var counter = std.Io.Writer.Discarding.init(&.{});
    try std.json.Stringify.value(receipts, .{}, &counter.writer);
    if (counter.fullCount() > max_bytes) return error.SqlProgramLimitExceeded;
    return std.json.Stringify.valueAlloc(a, receipts, .{});
}
pub fn decodeReceipts(a: A, bytes: []const u8) !std.json.Parsed([]const Receipt) {
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidSqlTriggerManifest;
    var parsed = std.json.parseFromSlice([]const Receipt, a, bytes, .{ .allocate = .alloc_always }) catch |failure| return if (failure == error.OutOfMemory) failure else error.InvalidSqlTriggerManifest;
    errdefer parsed.deinit();
    const canonical = try encodeReceipts(a, parsed.value);
    defer a.free(canonical);
    if (!std.mem.eql(u8, canonical, bytes)) return error.InvalidSqlTriggerManifest;
    return parsed;
}
pub fn receiptsDigest(a: A, receipts: []const Receipt) !Digest {
    const encoded = try encodeReceipts(a, receipts);
    defer a.free(encoded);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/sql-trigger-preparation/receipts/v1\x00");
    hash.update(encoded);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}
/// A participant receives only rows it will actually mutate. Retain the
/// complete immutable program fence while filtering already sorted row keys;
/// no row in another owner range can be admitted by an index into a shared list.
pub fn forParticipant(a: A, receipts: []const Receipt, writes: anytype, deletes: []const []const u8) ![]const Receipt {
    return forParticipantWithStatements(a, receipts, writes, deletes, false);
}
/// One deterministic native owner retains zero-row statement completion.
/// Other participants receive only their own logical row effects.
pub fn forParticipantWithStatements(a: A, receipts: []const Receipt, writes: anytype, deletes: []const []const u8, include_statements: bool) ![]const Receipt {
    var keys: std.StringHashMapUnmanaged(void) = .empty;
    defer keys.deinit(a);
    for (writes) |write| try keys.put(a, write.key, {});
    for (deletes) |key| try keys.put(a, key, {});
    var result: std.ArrayList(Receipt) = .empty;
    errdefer {
        for (result.items) |receipt| a.free(receipt.processed.rows);
        result.deinit(a);
    }
    for (receipts) |receipt| {
        var rows: std.ArrayList(Row) = .empty;
        errdefer rows.deinit(a);
        for (receipt.processed.rows) |row| if (keys.contains(row.key)) try rows.append(a, row);
        if (rows.items.len == 0 and !(include_statements and receipt.processed.rows.len == 0)) {
            rows.deinit(a);
            continue;
        }
        var filtered = receipt;
        filtered.processed.rows = try rows.toOwnedSlice(a);
        result.append(a, filtered) catch |failure| {
            a.free(filtered.processed.rows);
            return failure;
        };
    }
    return result.toOwnedSlice(a);
}

/// Validate prepared effects against the actual immutable receiver epoch and
/// atomic row predicates. Authentication is a separate native ingress grant;
/// neither a caller-provided manifest nor its own checksum supplies authority.
/// Ordered replay consumes admitted receipts without executing any function.
pub fn verifyPrepared(a: A, view: anytype, receipts: []const Receipt, authenticated: bool, writes: anytype, deletes: []const []const u8, predicates: anytype) !void {
    return verifyPreparedSource(a, view, false, receipts, authenticated, writes, deletes, predicates);
}
pub fn verifyPreparedNoSchema(a: A, receipts: []const Receipt, authenticated: bool, writes: anytype, deletes: []const []const u8, predicates: anytype) !void {
    // Explicit native absence is a distinct branch; no dummy schema/view is
    // constructed and no schema accessor can execute in this specialization.
    return verifyPreparedSource(a, {}, true, receipts, authenticated, writes, deletes, predicates);
}
fn verifyPreparedSource(a: A, view: anytype, comptime schema_absent: bool, receipts: []const Receipt, authenticated: bool, writes: anytype, deletes: []const []const u8, predicates: anytype) !void {
    const state: ?*const @import("../system_catalog/triggers.zig").State = if (comptime schema_absent) null else view.triggerState();
    const required = if (state) |value| value.entries.len != 0 else false;
    if (receipts.len == 0) {
        if (required and (writes.len != 0 or deletes.len != 0)) return error.SqlTriggerCoordinationRequired;
        return;
    }
    if (!authenticated) return error.SqlTriggerCoordinationRequired;
    if (receipts.len > max_receipts) return error.SqlProgramLimitExceeded;
    var counter = std.Io.Writer.Discarding.init(&.{});
    try std.json.Stringify.value(receipts, .{}, &counter.writer);
    if (counter.fullCount() > max_bytes) return error.SqlProgramLimitExceeded;
    if (comptime !schema_absent) if (view.validator() == null) return error.CatalogGenerationChanged;
    const expected_present: ?manifest.Table = if (comptime schema_absent) null else if (state) |actual| .{ .id = actual.table_id, .schema_version = view.version(), .schema_digest = view.validator().?.source_digest orelse return error.CatalogGenerationChanged, .attachment_generation = actual.generation, .attachment_digest = try manifest.attachmentDigest(actual.*) } else null;
    var first: std.StringHashMapUnmanaged(Row) = .empty;
    defer first.deinit(a);
    var last: std.StringHashMapUnmanaged(Row) = .empty;
    defer last.deinit(a);
    var waves: std.AutoHashMapUnmanaged([16]u8, struct { sequence: u16, program: Digest }) = .empty;
    defer waves.deinit(a);
    var count: usize = 0;
    for (receipts) |receipt| {
        try receipt.validate(a);
        const expected: manifest.Table = if (expected_present) |present| present else absent: {
            const proof = receipt.program.absence orelse return error.CatalogGenerationChanged;
            if (comptime schema_absent) try proof.requireNoSchema() else try proof.requireView(view);
            break :absent try proof.table();
        };
        const incarnation = if (state) |actual| actual.metadata_incarnation else receipt.program.absence.?.incarnation;
        if ((state == null) != (receipt.program.absence != null)) return error.CatalogGenerationChanged;
        try receipt.processed.header.requireFor(expected, incarnation);
        const header = receipt.processed.header;
        const wave = try waves.getOrPut(a, header.statement_id);
        if (wave.found_existing) {
            const expected_sequence = std.math.add(u16, wave.value_ptr.sequence, 1) catch return error.InvalidSqlTriggerManifest;
            if (header.sequence != expected_sequence) return error.InvalidSqlTriggerManifest;
            if (!std.mem.eql(u8, &wave.value_ptr.program, &header.program_digest)) return error.InvalidSqlTriggerManifest;
        } else if (header.sequence != 0) return error.InvalidSqlTriggerManifest;
        wave.value_ptr.* = .{ .sequence = header.sequence, .program = header.program_digest };
        if (receipt.processed.rows.len > max_rows -| count) return error.SqlProgramLimitExceeded;
        count += receipt.processed.rows.len;
        for (receipt.processed.rows) |row| {
            const original = try first.getOrPut(a, row.key);
            // OLD describes the preceding logical statement image; the
            // predicate still fences the physical row before this transaction.
            // An UPDATE of a staged INSERT therefore has an absent preimage.
            const existed = if (last.get(row.key)) |previous| previous.after != null else row.preimage == .present;
            if ((row.event == .insert) == existed) return error.InvalidSqlTriggerManifest;
            if (original.found_existing) {
                if (!std.meta.eql(original.value_ptr.preimage, row.preimage)) return error.InvalidSqlTriggerManifest;
            } else original.value_ptr.* = row;
            try last.put(a, row.key, row);
        }
    }
    var guards: std.StringHashMapUnmanaged(usize) = .empty;
    defer guards.deinit(a);
    for (predicates, 0..) |predicate, index| {
        const entry = try guards.getOrPut(a, predicate.key);
        if (entry.found_existing) return error.InvalidSqlTriggerManifest;
        entry.value_ptr.* = index;
    }
    var originals = first.iterator();
    while (originals.next()) |item| {
        const row = item.value_ptr.*;
        const guard = predicates[guards.get(row.key) orelse return error.SqlTriggerCoordinationRequired];
        try row.requirePreimage(if (guard.expected_version == 0) null else guard.expected_version, guard.expected_content_digest);
    }
    for (writes) |write| {
        const row = last.get(write.key) orelse return error.SqlTriggerCoordinationRequired;
        const digest = row.after orelse return error.InvalidSqlTriggerManifest;
        if (!std.mem.eql(u8, &digest, &try rowDigest(write.key, write.value, write.json_null_fields))) return error.InvalidSqlTriggerManifest;
    }
    for (deletes) |key| {
        const row = last.get(key) orelse return error.SqlTriggerCoordinationRequired;
        if (row.after != null) return error.InvalidSqlTriggerManifest;
    }
}

/// Inputs are the single prepared-row pipeline's final canonical JSON bytes.
/// This performs no JSON parse, row allocation, callback or catalog read.
pub fn rowDigest(key: []const u8, canonical_json: []const u8, json_null_fields: []const []const u8) !Digest {
    if (key.len == 0 or key.len > 4096 or canonical_json.len > 16 * 1024 * 1024 or json_null_fields.len > 4096) return error.SqlProgramLimitExceeded;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/trigger-prepared-row/v1\x00");
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, key.len, .little);
    hash.update(&length);
    hash.update(key);
    std.mem.writeInt(u64, &length, canonical_json.len, .little);
    hash.update(&length);
    hash.update(canonical_json);
    std.mem.writeInt(u64, &length, json_null_fields.len, .little);
    hash.update(&length);
    for (json_null_fields) |name| {
        std.mem.writeInt(u64, &length, name.len, .little);
        hash.update(&length);
        hash.update(name);
    }
    return hash.finalResult();
}

test "trigger handoff binds exact postimages null provenance and original-row predicates" {
    const original: Row = .{ .key = "row", .event = .update, .preimage = .{ .present = .{ .version = 7, .digest = @splat(3) } }, .after = try rowDigest("row", "{\"n\":null}", &.{"n"}), .update_columns = &.{ 2, 5 } };
    try original.validate();
    try original.requirePreimage(7, @splat(3));
    try std.testing.expectError(error.VersionConflict, original.requirePreimage(7, @splat(4)));
    try std.testing.expectError(error.VersionConflict, original.requirePreimage(null, null));
    try std.testing.expect(!std.mem.eql(u8, &original.after.?, &try rowDigest("row", "{\"n\":null}", &.{})));
    try std.testing.expect(!std.mem.eql(u8, &original.after.?, &try rowDigest("other", "{\"n\":null}", &.{"n"})));
    var malformed = original;
    malformed.update_columns = &.{ 5, 2 };
    try std.testing.expectError(error.InvalidSqlTriggerManifest, malformed.validate());
}
