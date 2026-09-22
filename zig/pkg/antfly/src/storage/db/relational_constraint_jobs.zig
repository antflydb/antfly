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

//! Local CHECK coverage is disposable proof, not part of the declaration.
//! All writes enforce the active epoch before a bounded scan starts. Publication
//! fences the epoch, namespace, owner and progress CAS. A bad source is rechecked
//! at publication, so a concurrent repair cannot publish a stale failure.
const std = @import("std");
const time = @import("antfly_platform").time;
const registry = @import("schema_registry.zig");
const schema = @import("../schema.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const internal = @import("../internal_keys.zig");
const docstore = @import("../docstore.zig");
const ranges = @import("range_state.zig");
const jobs = @import("relational_index_jobs.zig");
const Allocator = std.mem.Allocator;
const Digest = [32]u8;
pub const progress_key = "\x00\x00__metadata__:relational_check_progress";
const header_len = 56;
const max_cursor_bytes = 1024 * 1024;

fn digest(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

pub const State = enum(u8) { validating = 0, enforced = 1, invalid = 2 };
pub const Progress = struct {
    schema_version: u32,
    owner: Digest,
    state: State = .validating,
    rows_scanned: u64 = 0,
    /// Set only for invalid state. The name is resolved in the same schema epoch.
    failed_check: u16 = 0,
    cursor: []const u8 = "",

    pub fn encode(self: Progress, alloc: Allocator) ![]u8 {
        if (self.cursor.len > max_cursor_bytes or (self.state == .enforced and self.cursor.len != 0) or
            (self.state != .invalid and self.failed_check != 0)) return error.InvalidConstraintProgress;
        const out = try alloc.alloc(u8, header_len + self.cursor.len + 32);
        @memcpy(out[0..4], "ACV1");
        std.mem.writeInt(u32, out[4..8], self.schema_version, .little);
        @memcpy(out[8..40], &self.owner);
        out[40] = @intFromEnum(self.state);
        out[41] = 0;
        std.mem.writeInt(u16, out[42..44], self.failed_check, .little);
        std.mem.writeInt(u64, out[44..52], self.rows_scanned, .little);
        std.mem.writeInt(u32, out[52..56], @intCast(self.cursor.len), .little);
        @memcpy(out[header_len..][0..self.cursor.len], self.cursor);
        @memcpy(out[out.len - 32 ..], &digest(out[0 .. out.len - 32]));
        return out;
    }

    pub fn decode(bytes: []const u8) !Progress {
        if (bytes.len < header_len + 32 or bytes.len > header_len + max_cursor_bytes + 32 or
            !std.mem.eql(u8, bytes[0..4], "ACV1") or bytes[41] != 0 or
            std.mem.readInt(u32, bytes[52..56], .little) != bytes.len - header_len - 32 or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidConstraintProgress;
        const state: State = switch (bytes[40]) {
            0 => .validating,
            1 => .enforced,
            2 => .invalid,
            else => return error.InvalidConstraintProgress,
        };
        const failed_check = std.mem.readInt(u16, bytes[42..44], .little);
        const cursor = bytes[header_len .. bytes.len - 32];
        if ((state == .enforced and cursor.len != 0) or (state != .invalid and failed_check != 0)) return error.InvalidConstraintProgress;
        return .{ .schema_version = std.mem.readInt(u32, bytes[4..8], .little), .owner = bytes[8..40].*, .state = state, .rows_scanned = std.mem.readInt(u64, bytes[44..52], .little), .failed_check = failed_check, .cursor = cursor };
    }
};

fn optional(txn: *docstore.DocStore.Txn, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

pub fn status(txn: *docstore.DocStore.Txn, view: registry.SchemaView) !Progress {
    const owner = try jobs.ownership(txn);
    const validator = view.validator() orelse return error.ConstraintNotFound;
    const checks = validator.execution.checks orelse return error.ConstraintNotFound;
    if (try optional(txn, progress_key)) |raw| {
        const current = try Progress.decode(raw);
        if (current.schema_version == view.version() and std.mem.eql(u8, &current.owner, &owner)) {
            if (current.state == .invalid and current.failed_check >= checks.definitions.len) return error.InvalidConstraintProgress;
            return current;
        }
    }
    return .{ .schema_version = view.version(), .owner = owner };
}

pub const Budget = struct {
    records: usize = 256,
    bytes: usize = 1024 * 1024,
    time_ns: u64 = 5 * std.time.ns_per_ms,
};

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    view: registry.SchemaView,
    namespace_generation: u64,
    expected: ?[]const u8,
    next: Progress,
    failed_hash: ?Digest,
    records_examined: usize = 0,
    consumed: bool = false,

    pub fn deinit(self: *Page) void {
        self.view.release();
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn prepare(alloc: Allocator, io: ?std.Io, core: anytype, budget: Budget) !?Page {
        if (budget.records == 0 or budget.records > 4096 or budget.bytes == 0 or budget.bytes > 16 * 1024 * 1024 or
            budget.time_ns == 0 or budget.time_ns > std.time.ns_per_s) return error.InvalidConstraintBudget;
        if (io) |runtime_io| try runtime_io.checkCancel();
        const namespace_generation = core.schemaNamespaceGeneration();
        var view = core.acquireSchemaView() orelse return null;
        var transferred = false;
        defer if (!transferred) view.release();
        const validator = view.validator() orelse return null;
        const checks = validator.execution.checks orelse return null;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer if (!transferred) arena.deinit();
        const page_alloc = arena.allocator();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        var progress = try status(&read, view);
        if (progress.state != .validating) return null;
        const expected = if (try optional(&read, progress_key)) |raw| try page_alloc.dupe(u8, raw) else null;
        const range = try ranges.decodeRangeAlloc(page_alloc, (try optional(&read, ranges.range_key)) orelse &([_]u8{0} ** 8));
        const lower = try internal.documentExactPrefixAlloc(page_alloc, range.start);
        const upper: []const u8 = if (range.end.len != 0) try internal.documentExactPrefixAlloc(page_alloc, range.end) else &.{internal.user_namespace + 1};
        if (progress.cursor.len != 0 and (std.mem.order(u8, progress.cursor, lower) == .lt or std.mem.order(u8, progress.cursor, upper) != .lt)) return error.InvalidConstraintProgress;
        var cursor = try read.openCursor();
        defer cursor.close();
        cursor.setUpperBound(upper);
        var source: ?registry.SchemaView = null;
        defer if (source) |*old| old.release();
        var inspected: usize = 0;
        var bytes: usize = 0;
        var after = std.ArrayList(u8).empty;
        defer after.deinit(alloc);
        try after.appendSlice(alloc, progress.cursor);
        var successor = std.ArrayList(u8).empty;
        defer successor.deinit(alloc);
        var primary = std.ArrayList(u8).empty;
        defer primary.deinit(alloc);
        var failed_hash: ?Digest = null;
        var exhausted = true;
        const started = time.monotonicNs();
        var entry = try cursor.seekAtOrAfter(if (progress.cursor.len == 0) lower else try internal.documentPrefixSuccessor(alloc, &successor, progress.cursor));
        while (entry) |kv| : (entry = try cursor.seekAtOrAfter(try internal.documentPrefixSuccessor(alloc, &successor, after.items))) {
            if (io) |runtime_io| try runtime_io.checkCancel();
            if (progress.cursor.len != 0 and std.mem.order(u8, kv.key, progress.cursor) != .gt) continue;
            if (std.mem.order(u8, kv.key, upper) != .lt) break;
            inspected += 1;
            bytes +|= kv.key.len;
            after.clearRetainingCapacity();
            try after.appendSlice(alloc, kv.key);
            const end = (internal.findComponentTerminator(kv.key, 1) orelse return error.InvalidInternalUserKey) + 2;
            try primary.resize(alloc, end + 1);
            @memcpy(primary.items[0..end], kv.key[0..end]);
            primary.items[end] = internal.relational_row_kind;
            const value = if (std.mem.eql(u8, primary.items, kv.key)) kv.value else try optional(&read, primary.items);
            if (value) |raw| {
                bytes +|= raw.len;
                const version = try codec.rowSchemaVersion(raw);
                if (source == null or source.?.version() != version) {
                    if (source) |*old| old.release();
                    source = null;
                    source = if (version == view.version()) view.clone() else historical: {
                        const key = try schema.schemaVersionKeyAlloc(alloc, version);
                        defer alloc.free(key);
                        const table = try schema.deserializeSchema(alloc, (try optional(&read, key)) orelse return error.UnknownSchemaVersion);
                        errdefer schema.freeSchema(alloc, table);
                        if (table.version != version) return error.RelationalRowSchemaMismatch;
                        break :historical registry.SchemaView{ .epoch = try registry.Epoch.createOwned(alloc, table) };
                    };
                }
                const row = try codec.ordinalRowView(raw, source.?.tableSchema().*, source.?.physicalLayout());
                progress.rows_scanned = try std.math.add(u64, progress.rows_scanned, 1);
                if (try checks.firstFailureRow(alloc, row)) |failure| {
                    progress.state = .invalid;
                    progress.failed_check = @intCast(failure.index);
                    failed_hash = digest(raw);
                    // Failure publication guards the primary, not whichever
                    // companion happened to be first in physical order.
                    after.clearRetainingCapacity();
                    try after.appendSlice(alloc, primary.items);
                    break;
                }
            }
            if (inspected >= budget.records or bytes >= budget.bytes or time.monotonicNs() -| started >= budget.time_ns) {
                exhausted = false;
                break;
            }
        }
        if (failed_hash == null) progress.state = if (exhausted) .enforced else .validating;
        progress.cursor = if (progress.state == .enforced) "" else try page_alloc.dupe(u8, after.items);
        transferred = true;
        return .{ .arena = arena, .view = view, .namespace_generation = namespace_generation, .expected = expected, .next = progress, .failed_hash = failed_hash, .records_examined = inspected };
    }

    /// Caller holds apply-exclusive and snapshot/HA mutation admission.
    pub fn commit(self: *Page, core: anytype) !void {
        if (self.consumed) return error.ConstraintPageConsumed;
        self.consumed = true;
        if (core.schemaNamespaceGeneration() != self.namespace_generation or !core.schema_registry.isCurrent(self.view)) return error.PreparedGenerationChanged;
        var txn = try core.store.beginWriteTxn();
        errdefer txn.abort();
        if (!std.mem.eql(u8, &self.next.owner, &(try jobs.ownership(&txn)))) return error.PreparedGenerationChanged;
        const actual = try optional(&txn, progress_key);
        if ((actual == null) != (self.expected == null) or (actual != null and !std.mem.eql(u8, actual.?, self.expected.?))) return error.PreparedGenerationChanged;
        if (self.failed_hash) |hash| {
            const raw = (try optional(&txn, self.next.cursor)) orelse return error.PreparedGenerationChanged;
            if (!std.mem.eql(u8, &hash, &digest(raw))) return error.PreparedGenerationChanged;
        }
        try txn.put(progress_key, try self.next.encode(self.arena.allocator()));
        try txn.commit();
    }
};

test "relational constraint progress rejects corrupt state and framing" {
    const alloc = std.testing.allocator;
    const raw = try (Progress{ .schema_version = 9, .owner = @splat(3), .cursor = "row", .rows_scanned = 7 }).encode(alloc);
    defer alloc.free(raw);
    const decoded = try Progress.decode(raw);
    try std.testing.expectEqual(@as(u64, 7), decoded.rows_scanned);
    try std.testing.expectEqualStrings("row", decoded.cursor);
    raw[12] ^= 1;
    try std.testing.expectError(error.InvalidConstraintProgress, Progress.decode(raw));
    try std.testing.expectError(error.InvalidConstraintProgress, (Progress{ .schema_version = 9, .owner = @splat(3), .state = .enforced, .cursor = "row" }).encode(alloc));
}
