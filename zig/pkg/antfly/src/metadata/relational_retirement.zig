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

//! Internal metadata retirement barrier. Jobs are immutable except for their
//! monotonic phase; the exact source and replacement schema remain pinned.
const std = @import("std");
const native = @import("../storage/db/relational_integrity_retirement.zig");
const topology = @import("../common/topology_records.zig");
pub const Phase = enum { fencing, foreign_keys, unique, ready, published };
pub const Job = struct {
    id: [16]u8,
    source_schema_digest: [32]u8,
    target_schema_digest: [32]u8,
    generation_set: [32]u8,
    generations: []const [16]u8,
    target_schema_json: []const u8,
    drop: bool = false,
    phase: Phase = .fencing,
    failure: []const u8 = "",
    /// Exact source routing topology; all-owner proofs are never transferable
    /// across split/merge, restore or a newly allocated range identity.
    owners: []const Owner,
    pub const Owner = struct { group_id: u64, range_id: u64, start: []const u8, end: []const u8 };

    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try jw.beginObject();
        inline for (.{ "id", "source_schema_digest", "target_schema_digest", "generation_set", "generations", "owners" }) |field| {
            try jw.objectField(field);
            try @import("../storage/db/relational_integrity_json.zig").write(@field(self, field), jw);
        }
        // JSON is already valid UTF-8; byte-array encoding would amplify the
        // pinned schema severalfold on every metadata phase checkpoint.
        try jw.objectField("target_schema_json");
        try jw.write(self.target_schema_json);
        try jw.objectField("drop");
        try jw.write(self.drop);
        try jw.objectField("phase");
        try jw.write(@tagName(self.phase));
        try jw.objectField("failure");
        try jw.write(self.failure);
        try jw.endObject();
    }

    pub fn validate(self: Job) !void {
        if (self.failure.len > 4096 or std.mem.allEqual(u8, &self.id, 0) or self.generations.len == 0 or self.generations.len > 1024 or
            self.owners.len == 0 or self.owners.len > 4096 or self.target_schema_json.len == 0 or self.target_schema_json.len > 1024 * 1024) return error.InvalidConstraintRetirement;
        for (self.generations, 0..) |generation, index| {
            if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidConstraintRetirement;
            for (self.generations[0..index]) |previous| if (std.mem.eql(u8, &generation, &previous)) return error.InvalidConstraintRetirement;
        }
        for (self.owners, 0..) |owner, index| {
            if (owner.group_id == 0 or owner.range_id == 0) return error.InvalidConstraintRetirement;
            if (index == 0 and owner.start.len != 0) return error.InvalidConstraintRetirement;
            if (index > 0 and !std.mem.eql(u8, self.owners[index - 1].end, owner.start)) return error.InvalidConstraintRetirement;
            if (owner.end.len != 0 and std.mem.order(u8, owner.start, owner.end) != .lt) return error.InvalidConstraintRetirement;
            if ((owner.end.len == 0) != (index + 1 == self.owners.len)) return error.InvalidConstraintRetirement;
        }
    }
};

pub fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

pub fn parse(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Job) {
    if (bytes.len == 0 or bytes.len > 2 * 1024 * 1024) return error.InvalidConstraintRetirement;
    var result = try std.json.parseFromSlice(Job, alloc, bytes, .{ .allocate = .alloc_always });
    errdefer result.deinit();
    try result.value.validate();
    return result;
}

pub fn transitionAllowed(alloc: std.mem.Allocator, before: topology.TableRecord, after: topology.TableRecord) !bool {
    if (std.mem.eql(u8, before.relational_retirement_json, after.relational_retirement_json)) {
        return before.relational_retirement_json.len == 0 or
            (std.mem.eql(u8, before.schema_json, after.schema_json) and std.mem.eql(u8, before.read_schema_json, after.read_schema_json));
    }
    if (before.relational_retirement_json.len == 0) {
        if (after.relational_retirement_json.len == 0) return true;
        var job = try parse(alloc, after.relational_retirement_json);
        defer job.deinit();
        return job.value.phase == .fencing and job.value.failure.len == 0 and std.mem.eql(u8, &job.value.source_schema_digest, &digest(before.schema_json)) and
            std.mem.eql(u8, before.schema_json, after.schema_json) and std.mem.eql(u8, before.read_schema_json, after.read_schema_json);
    }
    var old = try parse(alloc, before.relational_retirement_json);
    defer old.deinit();
    if (after.relational_retirement_json.len == 0) return !old.value.drop and old.value.phase == .published and before.read_schema_json.len == 0 and
        std.mem.eql(u8, before.schema_json, after.schema_json) and std.mem.eql(u8, after.schema_json, old.value.target_schema_json);
    var next = try parse(alloc, after.relational_retirement_json);
    defer next.deinit();
    const phase = next.value.phase;
    const failure_changed = !std.mem.eql(u8, next.value.failure, old.value.failure);
    next.value.phase = old.value.phase;
    next.value.failure = old.value.failure;
    const old_bytes = try std.json.Stringify.valueAlloc(alloc, old.value, .{});
    defer alloc.free(old_bytes);
    const next_bytes = try std.json.Stringify.valueAlloc(alloc, next.value, .{});
    defer alloc.free(next_bytes);
    return std.mem.eql(u8, old_bytes, next_bytes) and
        ((failure_changed and phase == old.value.phase) or (!failure_changed and old.value.failure.len == 0 and @intFromEnum(phase) == @intFromEnum(old.value.phase) + 1)) and
        ((old.value.phase == .ready and phase == .published and !old.value.drop and std.mem.eql(u8, after.schema_json, old.value.target_schema_json)) or
            (std.mem.eql(u8, before.schema_json, after.schema_json) and std.mem.eql(u8, before.read_schema_json, after.read_schema_json)));
}

pub fn permitsSchema(alloc: std.mem.Allocator, before: topology.TableRecord, after: topology.TableRecord) !bool {
    if (before.relational_retirement_json.len == 0) return false;
    var job = try parse(alloc, before.relational_retirement_json);
    defer job.deinit();
    if (job.value.phase != .ready or job.value.drop or job.value.failure.len != 0 or !std.mem.eql(u8, job.value.target_schema_json, after.schema_json) or after.relational_retirement_json.len == 0) return false;
    var next = try parse(alloc, after.relational_retirement_json);
    defer next.deinit();
    return next.value.phase == .published and std.mem.eql(u8, &next.value.id, &job.value.id) and try transitionAllowed(alloc, before, after);
}

pub fn permitsMigrationCleanup(alloc: std.mem.Allocator, before: topology.TableRecord, after: topology.TableRecord) !bool {
    if (before.relational_retirement_json.len == 0 or !std.mem.eql(u8, before.relational_retirement_json, after.relational_retirement_json) or
        !std.mem.eql(u8, before.schema_json, after.schema_json) or after.read_schema_json.len != 0 or before.table_id != after.table_id or !std.mem.eql(u8, before.name, after.name)) return false;
    var job = try parse(alloc, before.relational_retirement_json);
    defer job.deinit();
    return job.value.phase == .published and !job.value.drop and std.mem.eql(u8, job.value.target_schema_json, before.schema_json);
}

pub fn permitsDrop(alloc: std.mem.Allocator, table: topology.TableRecord) !bool {
    if (table.relational_retirement_json.len == 0) return false;
    var job = try parse(alloc, table.relational_retirement_json);
    defer job.deinit();
    return job.value.phase == .ready and job.value.drop;
}

/// Run again in the authoritative metadata transaction that installs the
/// barrier: an incoming FK may have been published after API preflight.
pub fn incomingAllowed(alloc: std.mem.Allocator, parent_name: []const u8, source_json: []const u8, job: Job, child_json: []const u8, external: bool) !bool {
    if (child_json.len == 0) return true;
    const Declaration = struct {
        unique_constraints: ?[]const struct { name: []const u8, columns: []const []const u8 } = null,
        foreign_keys: ?[]const struct { parent_table: []const u8, parent_columns: []const []const u8 } = null,
    };
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const child = try std.json.parseFromSlice(Declaration, owned, child_json, .{ .ignore_unknown_fields = true });
    const source = try std.json.parseFromSlice(Declaration, owned, source_json, .{ .ignore_unknown_fields = true });
    const target = try std.json.parseFromSlice(Declaration, owned, job.target_schema_json, .{ .ignore_unknown_fields = true });
    for (child.value.foreign_keys orelse &.{}) |fk| {
        if (!std.mem.eql(u8, fk.parent_table, parent_name)) continue;
        if (external and job.drop) return false;
        for (source.value.unique_constraints orelse &.{}) |unique| {
            const retained = for (target.value.unique_constraints orelse &.{}) |next| {
                if (std.mem.eql(u8, unique.name, next.name)) break true;
            } else false;
            if (retained or unique.columns.len != fk.parent_columns.len) continue;
            const matching_tuple = for (unique.columns, fk.parent_columns) |left, right| {
                if (!std.mem.eql(u8, left, right)) break false;
            } else true;
            // Equivalent UNIQUE definitions do not retarget existing references
            // from the concrete immutable generation being retired.
            if (matching_tuple) return false;
        }
    }
    return true;
}

pub fn phaseForOwner(phase: Phase) native.Phase {
    return switch (phase) {
        .fencing => .fenced,
        .foreign_keys => .foreign_keys,
        .unique => .unique,
        .ready => .ready,
        .published => .ready,
    };
}
