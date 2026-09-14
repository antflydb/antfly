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

//! Durable owner-local retirement admission and resumable coverage. The
//! metadata coordinator freezes the complete owner set before it advances any
//! owner beyond `fenced`. A checkpoint joins the same 2PC as outgoing reference
//! detaches / unique claim releases; ordinary mutations remain fenced until
//! the target schema has been durably published.
const std = @import("std");
const integrity = @import("relational_integrity.zig");
const catalog_mod = @import("relational_integrity_catalog.zig");
const activation = @import("relational_integrity_activation.zig");
const transactions = @import("../transactions.zig");
const Allocator = std.mem.Allocator;
pub const key = "\x00\x00__metadata__:relational_integrity_retirement";
pub const Phase = enum(u8) { fenced, foreign_keys, unique, ready };

pub const Progress = struct {
    job_id: integrity.Generation,
    generation_set: integrity.Digest,
    owner: integrity.Digest,
    target_schema_digest: integrity.Digest,
    schema_version: u32,
    phase: Phase = .fenced,
    rows_scanned: u64 = 0,
    generations: []const integrity.Generation,
    cursor: []const u8 = "",

    pub fn includes(self: Progress, generation: integrity.Generation) bool {
        for (self.generations) |item| if (std.mem.eql(u8, &item, &generation)) return true;
        return false;
    }

    pub fn encode(self: Progress, alloc: Allocator) ![]u8 {
        if (std.mem.allEqual(u8, &self.job_id, 0) or self.generations.len == 0 or self.generations.len > 1024 or self.cursor.len > 1024 * 1024 or
            ((self.phase == .fenced or self.phase == .ready) and self.cursor.len != 0)) return error.InvalidConstraintRetirement;
        for (self.generations, 0..) |generation, index| {
            if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidConstraintRetirement;
            for (self.generations[0..index]) |previous| if (std.mem.eql(u8, &previous, &generation)) return error.InvalidConstraintRetirement;
        }
        const out = try alloc.alloc(u8, 136 + self.generations.len * 16 + self.cursor.len + 32);
        @memset(out[0..136], 0);
        @memcpy(out[0..4], "AIR1");
        out[4] = @intFromEnum(self.phase);
        @memcpy(out[8..24], &self.job_id);
        @memcpy(out[24..56], &self.generation_set);
        @memcpy(out[56..88], &self.owner);
        @memcpy(out[88..120], &self.target_schema_digest);
        std.mem.writeInt(u32, out[120..124], self.schema_version, .little);
        std.mem.writeInt(u32, out[124..128], @intCast(self.generations.len), .little);
        std.mem.writeInt(u64, out[128..136], self.rows_scanned, .little);
        for (self.generations, 0..) |generation, index| @memcpy(out[136 + index * 16 ..][0..16], &generation);
        @memcpy(out[136 + self.generations.len * 16 ..][0..self.cursor.len], self.cursor);
        std.crypto.hash.Blake3.hash(out[0 .. out.len - 32], out[out.len - 32 ..][0..32], .{});
        return out;
    }

    /// Returned generations and cursor borrow the checksummed input.
    pub fn decode(bytes: []const u8) !Progress {
        if (bytes.len < 184 or bytes.len > 136 + 1024 * 16 + 1024 * 1024 + 32 or !std.mem.eql(u8, bytes[0..4], "AIR1") or
            !std.mem.allEqual(u8, bytes[5..8], 0)) return error.InvalidConstraintRetirement;
        var checksum: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[bytes.len - 32 ..])) return error.InvalidConstraintRetirement;
        const count = std.mem.readInt(u32, bytes[124..128], .little);
        if (count == 0 or count > 1024 or 136 + @as(usize, count) * 16 > bytes.len - 32) return error.InvalidConstraintRetirement;
        const generations: []const integrity.Generation = std.mem.bytesAsSlice(integrity.Generation, bytes[136 .. 136 + @as(usize, count) * 16]);
        const result: Progress = .{
            .job_id = bytes[8..24].*,
            .generation_set = bytes[24..56].*,
            .owner = bytes[56..88].*,
            .target_schema_digest = bytes[88..120].*,
            .schema_version = std.mem.readInt(u32, bytes[120..124], .little),
            .phase = std.enums.fromInt(Phase, bytes[4]) orelse return error.InvalidConstraintRetirement,
            .rows_scanned = std.mem.readInt(u64, bytes[128..136], .little),
            .generations = generations,
            .cursor = bytes[136 + @as(usize, count) * 16 .. bytes.len - 32],
        };
        if (std.mem.allEqual(u8, &result.job_id, 0) or result.cursor.len > 1024 * 1024 or
            ((result.phase == .fenced or result.phase == .ready) and result.cursor.len != 0)) return error.InvalidConstraintRetirement;
        for (generations, 0..) |generation, index| {
            if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidConstraintRetirement;
            for (generations[0..index]) |previous| if (std.mem.eql(u8, &previous, &generation)) return error.InvalidConstraintRetirement;
        }
        return result;
    }
};

fn optional(txn: anytype, physical_key: []const u8) !?[]const u8 {
    return txn.get(physical_key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

pub fn current(txn: anytype) !?Progress {
    return if (try optional(txn, key)) |bytes| try Progress.decode(bytes) else null;
}

pub fn active(txn: anytype) !bool {
    return try current(txn) != null;
}
pub fn requireMutable(txn: anytype) !void {
    if (try active(txn)) return error.ConstraintRetirementInProgress;
}

/// Incoming dependency admission is fenced on the claim owner as well as on
/// the primary owner. Existing detach/release operations are allowed to drain.
pub fn admitCommands(txn: anytype, commands: []const integrity.Command) !void {
    const progress = (try current(txn)) orelse return;
    for (commands) |command| if (progress.includes(command.address.generation)) switch (command.operation) {
        .detach, .repair_detach, .release, .repair_release => {},
        else => return error.ConstraintRetirementInProgress,
    };
}

pub const Command = struct {
    routing_key: []const u8,
    expected: ?[]const u8,
    next: []const u8,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};
pub const Prepared = struct { intent: transactions.WriteIntent, predicate: transactions.VersionPredicate };

pub fn prepareCommand(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog, command: Command) !Prepared {
    try @import("relational_integrity_topology.zig").requireUnfenced(txn);
    const previous = try optional(txn, key);
    if (command.expected) |expected| {
        if (!std.mem.eql(u8, expected, previous orelse return error.ConstraintRetirementChanged)) return error.ConstraintRetirementChanged;
    } else if (previous != null) return error.ConstraintRetirementChanged;
    const next = Progress.decode(command.next) catch return error.InvalidConstraintRetirementCommand;
    const routing = try activation.routingKey(alloc, txn);
    defer alloc.free(routing);
    if (!std.mem.eql(u8, routing, command.routing_key) or !std.mem.eql(u8, &next.owner, &try activation.ownership(txn)) or
        !std.mem.eql(u8, &next.generation_set, &activation.generationSet(catalog)) or next.schema_version != catalog.schema_version) return error.ConstraintRetirementChanged;
    for (next.generations) |generation| {
        const binding = catalog.findGeneration(generation) orelse return error.ConstraintRetirementChanged;
        if (binding.retired) return error.ConstraintRetirementChanged;
    }
    if (previous) |bytes| {
        const before = try Progress.decode(bytes);
        if (!std.mem.eql(u8, &before.job_id, &next.job_id) or !std.mem.eql(u8, &before.owner, &next.owner) or !std.mem.eql(u8, &before.generation_set, &next.generation_set) or before.schema_version != next.schema_version or !std.mem.eql(u8, &before.target_schema_digest, &next.target_schema_digest) or
            !std.mem.eql(u8, std.mem.sliceAsBytes(before.generations), std.mem.sliceAsBytes(next.generations)) or next.rows_scanned < before.rows_scanned) return error.InvalidConstraintRetirementCommand;
        if (next.phase == before.phase) {
            if (before.phase == .fenced or before.phase == .ready or std.mem.order(u8, next.cursor, before.cursor) != .gt) return error.InvalidConstraintRetirementCommand;
        } else if (@intFromEnum(next.phase) != @intFromEnum(before.phase) + 1 or next.cursor.len != 0) return error.InvalidConstraintRetirementCommand;
    } else if (next.phase != .fenced or next.rows_scanned != 0 or next.cursor.len != 0) return error.InvalidConstraintRetirementCommand;
    return .{ .intent = .{ .key = key, .value = command.next }, .predicate = .{ .key = key, .comparison = .exact_value, .expected_value = command.expected } };
}

/// Schema removal consumes the proof in the same catalog/outbox transaction.
/// A proof cannot authorize another target schema or a reincarnated generation.
pub fn permits(txn: anytype, generation: integrity.Generation, target_schema_digest: integrity.Digest) !bool {
    const progress = (try current(txn)) orelse return false;
    return progress.phase == .ready and progress.includes(generation) and
        std.mem.eql(u8, &progress.target_schema_digest, &target_schema_digest);
}

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    rows: @import("relational_rows.zig").Page,
    command: Command,
    phase: Phase,
    pub fn deinit(self: *Page) void {
        self.rows.deinit();
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn prepare(alloc: Allocator, io: ?std.Io, core: anytype, fields: []const []const u8, expected_progress: []const u8, budget: @import("relational_rows.zig").Budget) !?Page {
        var reader = blk: {
            core.lockApplyShared();
            defer core.unlockApplyShared();
            var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
            defer view.release();
            break :blk try @import("relational_rows.zig").Reader.open(alloc, core.store, view, null, .{ .fields = fields, .include_primary_digest = true }, 0);
        };
        defer reader.deinit();
        const previous = (try optional(&reader.read, key)) orelse return null;
        // Projection selection and its phase must refer to this exact cut;
        // another worker may have crossed a barrier before snapshot opening.
        if (!std.mem.eql(u8, previous, expected_progress)) return error.ConstraintRetirementChanged;
        var progress = try Progress.decode(previous);
        if (progress.phase == .fenced or progress.phase == .ready) return null;
        if (!std.mem.eql(u8, &progress.owner, &try activation.ownership(&reader.read)) or progress.schema_version != reader.active.version()) return error.ConstraintRetirementChanged;
        if (progress.cursor.len != 0 and (std.mem.order(u8, progress.cursor, reader.lower) == .lt or std.mem.order(u8, progress.cursor, reader.upper) != .lt)) return error.InvalidConstraintRetirement;
        try reader.after.appendSlice(alloc, progress.cursor);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const expected = try owned.dupe(u8, previous);
        const phase = progress.phase;
        var page_rows: @import("relational_rows.zig").Page = if (fields.len == 0)
            .{ .arena = std.heap.ArenaAllocator.init(alloc), .rows = &.{}, .more = false, .records_examined = 0, .output_bytes = 0 }
        else
            try reader.nextPage(alloc, io, budget);
        errdefer page_rows.deinit();
        progress.rows_scanned = std.math.add(u64, progress.rows_scanned, page_rows.rows.len) catch return error.InvalidConstraintRetirement;
        progress.cursor = if (page_rows.more) try owned.dupe(u8, reader.after.items) else "";
        if (!page_rows.more) progress.phase = if (phase == .foreign_keys) .unique else .ready;
        return .{ .arena = arena, .rows = page_rows, .phase = phase, .command = .{
            .routing_key = try activation.routingKey(owned, &reader.read),
            .expected = expected,
            .next = try progress.encode(owned),
        } };
    }
};

test "relational integrity retirement checksum and generation fence" {
    const alloc = std.testing.allocator;
    const progress: Progress = .{ .job_id = @splat(1), .generation_set = @splat(2), .owner = @splat(3), .target_schema_digest = @splat(4), .schema_version = 7, .generations = &.{@splat(5)} };
    const bytes = try progress.encode(alloc);
    defer alloc.free(bytes);
    const decoded = try Progress.decode(bytes);
    try std.testing.expect(decoded.includes(@splat(5)));
    try std.testing.expect(!decoded.includes(@splat(6)));
    bytes[24] ^= 1;
    try std.testing.expectError(error.InvalidConstraintRetirement, Progress.decode(bytes));
}
