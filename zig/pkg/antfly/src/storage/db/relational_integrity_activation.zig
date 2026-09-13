// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Coordinated constraint activation. Coverage binds the immutable active
//! generation set and exact owned range, not mutable schema declaration state.
//! New primary mutations stay gated until ALL table owners report coverage.
//! A page's source-row guards, claims/references and progress transition join
//! the same durable distributed transaction. No progress-only best effort ack.
const std = @import("std");
const integrity = @import("relational_integrity.zig");
const catalog_mod = @import("relational_integrity_catalog.zig");
const rows = @import("relational_rows.zig");
const ranges = @import("range_state.zig");
const transactions = @import("../transactions.zig");
const Allocator = std.mem.Allocator;
pub const key = "\x00\x00__metadata__:relational_integrity_activation";
const header_len = 88;
const max_cursor_bytes = 1024 * 1024;
const full_range = [_]u8{0} ** 8;

fn digest(bytes: []const u8) integrity.Digest {
    var result: integrity.Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

pub fn generationSet(catalog: catalog_mod.Catalog) integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly active constraint coverage v1");
    state.update(&catalog.incarnation);
    for (catalog.bindings) |binding| if (!binding.retired) {
        state.update(&binding.generation);
        state.update(&binding.definition.fingerprint);
    };
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

pub fn hasActive(catalog: catalog_mod.Catalog) bool {
    for (catalog.bindings) |binding| if (!binding.retired) return true;
    return false;
}

fn hasKind(catalog: catalog_mod.Catalog, kind: catalog_mod.Kind) bool {
    for (catalog.bindings) |binding| if (!binding.retired and binding.definition.kind == kind) return true;
    return false;
}

fn optional(txn: anytype, physical_key: []const u8) !?[]const u8 {
    return txn.get(physical_key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

pub fn ownership(txn: anytype) !integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly constraint coverage owner v1");
    const namespace = try optional(txn, &@import("../internal_keys.zig").identity_namespace_key) orelse return error.CoordinatedConstraintsRequireTableIdentity;
    state.update(namespace);
    state.update((try optional(txn, ranges.range_key)) orelse &full_range);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

pub fn routingKey(alloc: Allocator, txn: anytype) ![]const u8 {
    const range = try ranges.decodeRangeAlloc(alloc, (try optional(txn, ranges.range_key)) orelse &full_range);
    defer alloc.free(range.end);
    return range.start;
}

pub const State = enum(u8) { validating = 0, enforced = 1, invalid = 2 };
pub const Phase = enum(u8) { unique = 0, foreign_key = 1 };
pub const Progress = struct {
    generation_set: integrity.Digest,
    owner: integrity.Digest,
    schema_version: u32,
    state: State = .validating,
    phase: Phase = .unique,
    rows_scanned: u64 = 0,
    /// Physical primary-namespace continuation includes skipped auxiliary
    /// records, preventing empty projected pages from repeatedly rescanning.
    cursor: []const u8 = "",
    failure: []const u8 = "",

    pub fn readyForReferences(self: Progress) bool {
        return self.state == .enforced or self.phase == .foreign_key;
    }

    pub fn retry(self: Progress, catalog: catalog_mod.Catalog) !Progress {
        if (self.state != .invalid) return error.InvalidConstraintActivation;
        return .{ .generation_set = generationSet(catalog), .owner = self.owner, .schema_version = catalog.schema_version, .phase = if (hasKind(catalog, .unique)) .unique else .foreign_key };
    }

    pub fn encode(self: Progress, alloc: Allocator) ![]u8 {
        if (self.cursor.len > max_cursor_bytes or self.failure.len > 4096 or
            (self.state == .enforced and self.cursor.len != 0) or (self.state != .invalid and self.failure.len != 0)) return error.InvalidConstraintActivation;
        const out = try alloc.alloc(u8, header_len + self.cursor.len + self.failure.len + 32);
        @memcpy(out[0..4], "AIA1");
        @memcpy(out[4..36], &self.generation_set);
        @memcpy(out[36..68], &self.owner);
        std.mem.writeInt(u32, out[68..72], self.schema_version, .little);
        out[72] = @intFromEnum(self.state);
        out[73] = @intFromEnum(self.phase);
        @memset(out[74..76], 0);
        std.mem.writeInt(u64, out[76..84], self.rows_scanned, .little);
        std.mem.writeInt(u32, out[84..88], @intCast(self.cursor.len), .little);
        // The bounded footer remainder is the diagnostic; no duplicate size.
        @memcpy(out[header_len..][0..self.cursor.len], self.cursor);
        @memcpy(out[header_len + self.cursor.len ..][0..self.failure.len], self.failure);
        @memcpy(out[out.len - 32 ..], &digest(out[0 .. out.len - 32]));
        return out;
    }

    pub fn decode(bytes: []const u8) !Progress {
        if (bytes.len < header_len + 32 or bytes.len > header_len + max_cursor_bytes + 4096 + 32 or
            !std.mem.eql(u8, bytes[0..4], "AIA1") or !std.mem.allEqual(u8, bytes[74..76], 0) or bytes[72] > 2 or bytes[73] > 1 or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidConstraintActivation;
        const cursor_len = std.mem.readInt(u32, bytes[84..88], .little);
        const payload = bytes[header_len .. bytes.len - 32];
        if (cursor_len > max_cursor_bytes or cursor_len > payload.len or payload.len - cursor_len > 4096) return error.InvalidConstraintActivation;
        const result: Progress = .{
            .generation_set = bytes[4..36].*,
            .owner = bytes[36..68].*,
            .schema_version = std.mem.readInt(u32, bytes[68..72], .little),
            .state = switch (bytes[72]) {
                0 => .validating,
                1 => .enforced,
                2 => .invalid,
                else => unreachable,
            },
            .phase = if (bytes[73] == 0) .unique else .foreign_key,
            .rows_scanned = std.mem.readInt(u64, bytes[76..84], .little),
            .cursor = payload[0..cursor_len],
            .failure = payload[cursor_len..],
        };
        if ((result.state == .enforced and result.cursor.len != 0) or (result.state != .invalid and result.failure.len != 0)) return error.InvalidConstraintActivation;
        return result;
    }

    pub fn matches(self: Progress, catalog: catalog_mod.Catalog, owner: integrity.Digest) bool {
        return std.mem.eql(u8, &self.generation_set, &generationSet(catalog)) and std.mem.eql(u8, &self.owner, &owner);
    }
};

pub fn status(txn: anytype, catalog: catalog_mod.Catalog) !Progress {
    const owner = try ownership(txn);
    if (try optional(txn, key)) |bytes| {
        var progress = try Progress.decode(bytes);
        if (progress.matches(catalog, owner)) {
            progress.schema_version = catalog.schema_version;
            return progress;
        }
    }
    return .{ .generation_set = generationSet(catalog), .owner = owner, .schema_version = catalog.schema_version, .phase = if (hasKind(catalog, .unique)) .unique else .foreign_key };
}

/// Must stage in the schema/catalog/outbox commit using the durable table
/// catalog's authoritative row count/has-data bit. A missing proof is NEVER
/// considered ready outside this atomic empty-table bootstrap.
pub fn stageSchema(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog, row_count: u64) !void {
    if (!hasActive(catalog)) {
        try txn.delete(key);
        return;
    }
    var progress = try status(txn, catalog);
    const previous = try optional(txn, key);
    const matched = if (previous) |bytes| (try Progress.decode(bytes)).matches(catalog, progress.owner) else false;
    if (!matched and row_count == 0) progress.state = .enforced;
    const encoded = try progress.encode(alloc);
    defer alloc.free(encoded);
    if (previous == null or !std.mem.eql(u8, previous.?, encoded)) try txn.put(key, encoded);
}

pub fn requireReady(txn: anytype, catalog: catalog_mod.Catalog) !void {
    if (!hasActive(catalog)) return;
    switch ((try status(txn, catalog)).state) {
        .enforced => {},
        .validating => return error.ConstraintActivationInProgress,
        .invalid => return error.ConstraintActivationFailed,
    }
}

pub const Command = struct {
    routing_key: []const u8,
    expected: ?[]const u8,
    next: []const u8,
    retry: bool = false,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};

pub const Prepared = struct { intent: transactions.WriteIntent, predicate: transactions.VersionPredicate };

pub fn prepareCommand(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog, command: Command) !Prepared {
    if (!hasActive(catalog)) return error.ConstraintNotFound;
    const current = try optional(txn, key);
    if (command.expected) |expected| {
        if (!std.mem.eql(u8, current orelse return error.ConstraintActivationChanged, expected)) return error.ConstraintActivationChanged;
    } else if (current != null) return error.ConstraintActivationChanged;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    if (!std.mem.eql(u8, command.routing_key, try routingKey(arena.allocator(), txn))) return error.ConstraintActivationOwnerChanged;
    const before = try status(txn, catalog);
    const after = try Progress.decode(command.next);
    if (!after.matches(catalog, before.owner) or after.schema_version != catalog.schema_version) return error.ConstraintActivationChanged;
    if (command.retry) {
        const initial_phase: Phase = if (hasKind(catalog, .unique)) .unique else .foreign_key;
        if (before.state != .invalid or after.state != .validating or after.phase != initial_phase or after.cursor.len != 0 or after.rows_scanned != 0) return error.InvalidConstraintActivation;
    } else {
        if (before.state != .validating or after.rows_scanned < before.rows_scanned) return error.InvalidConstraintActivation;
        if (after.phase != before.phase) {
            if (before.phase != .unique or after.phase != .foreign_key or after.state != .validating or after.cursor.len != 0 or !hasKind(catalog, .foreign_key)) return error.InvalidConstraintActivation;
        } else if (after.state == .validating and std.mem.order(u8, after.cursor, before.cursor) != .gt) return error.InvalidConstraintActivation;
        if (after.state == .enforced and before.phase == .unique and hasKind(catalog, .foreign_key)) return error.InvalidConstraintActivation;
    }
    return .{ .intent = .{ .key = key, .value = command.next }, .predicate = .{ .key = key, .comparison = .exact_value, .expected_value = command.expected } };
}

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    rows: rows.Page,
    command: Command,
    progress: Progress,
    phase: Phase,
    pub fn deinit(self: *Page) void {
        self.rows.deinit();
        self.arena.deinit();
        self.* = undefined;
    }

    /// A deterministic failure is persisted only with the page's original
    /// source-version guards. Abort its failed claim/reference transaction
    /// first; never acknowledge successful coverage for its rejected rows.
    pub fn markFailed(self: *Page, reason: []const u8) !void {
        if (reason.len == 0 or reason.len > 4096) return error.InvalidConstraintActivation;
        self.progress.state = .invalid;
        self.progress.phase = self.phase;
        self.progress.failure = try self.arena.allocator().dupe(u8, reason);
        self.command.next = try self.progress.encode(self.arena.allocator());
    }

    /// Opens one immutable source snapshot and releases the apply fence before
    /// projection. The coordinator enlists every returned row's version guard
    /// with its generated integrity commands AND this page's CAS command.
    pub fn prepare(alloc: Allocator, io: ?std.Io, core: anytype, fields: []const []const u8, budget: rows.Budget) !?Page {
        var reader = blk: {
            core.lockApplyShared();
            defer core.unlockApplyShared();
            var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
            defer view.release();
            // Validate all physically retained rows, including TTL candidates;
            // only coordinated expiration may retire constrained parents.
            break :blk try rows.Reader.open(alloc, core.store, view, null, .{ .fields = fields }, 0);
        };
        defer reader.deinit();
        const catalog_raw = try optional(&reader.read, catalog_mod.key) orelse return error.ConstraintNotFound;
        var catalog = try catalog_mod.decode(alloc, catalog_raw);
        defer catalog.deinit();
        if (!hasActive(catalog)) return null;
        if (catalog.schema_version != reader.active.version()) return error.ConstraintActivationChanged;
        var progress = try status(&reader.read, catalog);
        const phase = progress.phase;
        if (progress.state != .validating) return null;
        if (progress.cursor.len != 0 and (std.mem.order(u8, progress.cursor, reader.lower) == .lt or std.mem.order(u8, progress.cursor, reader.upper) != .lt)) return error.InvalidConstraintActivation;
        try reader.after.appendSlice(alloc, progress.cursor);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const expected = if (try optional(&reader.read, key)) |bytes| try owned.dupe(u8, bytes) else null;
        const routing = try routingKey(owned, &reader.read);
        var page_rows = reader.nextPage(alloc, io, budget) catch |err| {
            if (err != error.RelationalRowResultTooLarge) return err;
            // A single unprojectable row must not leave an owner retrying
            // forever. Publish a conservative failure CAS with NO source
            // coverage or cursor advancement; ordinary mutations remain
            // fenced until explicit repair/retry resolves the diagnostic.
            progress.state = .invalid;
            progress.failure = "RelationalRowResultTooLarge";
            progress.cursor = try owned.dupe(u8, progress.cursor);
            const next = try progress.encode(owned);
            return .{
                .arena = arena,
                .rows = .{ .arena = std.heap.ArenaAllocator.init(alloc), .rows = &.{}, .more = true, .records_examined = 0, .output_bytes = 0 },
                .progress = progress,
                .phase = phase,
                .command = .{ .routing_key = routing, .expected = expected, .next = next },
            };
        };
        errdefer page_rows.deinit();
        progress.rows_scanned = std.math.add(u64, progress.rows_scanned, page_rows.rows.len) catch return error.InvalidConstraintActivation;
        progress.cursor = if (page_rows.more) try owned.dupe(u8, reader.after.items) else "";
        if (!page_rows.more) {
            if (phase == .unique and hasKind(catalog, .foreign_key)) progress.phase = .foreign_key else progress.state = .enforced;
        }
        const next = try progress.encode(owned);
        return .{ .arena = arena, .rows = page_rows, .progress = progress, .phase = phase, .command = .{ .routing_key = routing, .expected = expected, .next = next } };
    }
};

test "relational integrity activation coverage binds generation owner phase and checksum" {
    const alloc = std.testing.allocator;
    var catalog = try catalog_mod.prepare(alloc, null, @splat(1), 1, @splat(2), &.{ .{ .kind = .unique, .name = "id", .fingerprint = @splat(3) }, .{ .kind = .foreign_key, .name = "parent", .fingerprint = @splat(4) } });
    defer catalog.deinit();
    var progress: Progress = .{ .generation_set = generationSet(catalog.catalog), .owner = @splat(5), .schema_version = 1 };
    try std.testing.expect(!progress.readyForReferences());
    progress.phase = .foreign_key;
    try std.testing.expect(progress.readyForReferences());
    const bytes = try progress.encode(alloc);
    defer alloc.free(bytes);
    const decoded = try Progress.decode(bytes);
    try std.testing.expect(decoded.matches(catalog.catalog, @splat(5)));
    try std.testing.expect(!decoded.matches(catalog.catalog, @splat(6)));
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidConstraintActivation, Progress.decode(bytes));
}
