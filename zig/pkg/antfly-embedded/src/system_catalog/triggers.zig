// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical table-local trigger attachment identities. Function bodies remain
//! in the shared routine catalog; an invocation pins the referenced ID and its
//! current executable generation, rather than resolving a reused spelling.
const std = @import("std");
const syntax = @import("../sql/trigger_definition.zig");
const routines = @import("routines.zig");
const A = std.mem.Allocator;
pub const max_entries = 256;
pub const max_bytes = 1024 * 1024;

pub const Declaration = struct {
    name: []const u8,
    timing: syntax.Timing,
    events: syntax.Events,
    level: syntax.Level,
    function: routines.RoutineReference,
    arguments: []const []const u8 = &.{},
    update_columns: []const u64 = &.{},
    predicate: ?[]const u8 = null,

    pub fn bind(self: Declaration, id: u64, generation: u32) Entry {
        var result: Entry = undefined;
        result.id = id;
        result.generation = generation;
        inline for (@typeInfo(Declaration).@"struct".field_names) |field| @field(result, field) = @field(self, field);
        return result;
    }
};

pub const Entry = struct {
    id: u64,
    generation: u32,
    name: []const u8,
    timing: syntax.Timing,
    events: syntax.Events,
    level: syntax.Level,
    function: routines.RoutineReference,
    arguments: []const []const u8 = &.{},
    /// Durable attribute identities, never creation-time column positions.
    update_columns: []const u64 = &.{},
    /// Canonical SQL predicate text, bound against the immutable OLD/NEW domain
    /// at publication and once per statement at execution.
    predicate: ?[]const u8 = null,

    pub fn validate(self: Entry) !void {
        if (self.id == 0 or self.generation == 0 or !syntax.validName(self.name) or self.arguments.len > 32 or self.update_columns.len > 256 or
            (self.update_columns.len != 0 and !self.events.update)) return error.InvalidSqlTriggerDefinition;
        try self.events.validate(self.level);
        try self.function.validate();
        var bytes: usize = 0;
        for (self.arguments) |value| {
            if (!std.unicode.utf8ValidateSlice(value) or std.mem.indexOfScalar(u8, value, 0) != null) return error.InvalidSqlTriggerDefinition;
            bytes = std.math.add(usize, bytes, value.len) catch return error.InvalidSqlTriggerDefinition;
        }
        if (bytes > 64 * 1024) return error.SqlProgramLimitExceeded;
        for (self.update_columns, 0..) |id, i| {
            if (id == 0) return error.InvalidSqlTriggerDefinition;
            for (self.update_columns[0..i]) |previous| if (previous == id) return error.InvalidSqlTriggerDefinition;
        }
        if (self.predicate) |value| if (value.len == 0 or value.len > 65536 or !std.unicode.utf8ValidateSlice(value) or std.mem.indexOfScalar(u8, value, 0) != null) return error.InvalidSqlTriggerDefinition;
    }
    pub fn matches(self: Entry, timing: syntax.Timing, level: syntax.Level, event: syntax.Event, updated: []const u64) bool {
        if (self.timing != timing or self.level != level or !self.events.contains(event)) return false;
        if (event != .update or self.update_columns.len == 0) return true;
        for (self.update_columns) |id| for (updated) |changed| {
            if (id == changed) return true;
        };
        return false;
    }
};
pub const State = struct {
    logical_relation_id: ?u64 = null,
    format: u8 = 1,
    metadata_incarnation: [16]u8,
    table_id: u64,
    generation: u64,
    next_id: u64 = 1,
    /// Canonical name order is also PostgreSQL's firing order.
    entries: []const Entry = &.{},

    pub fn effectiveLogicalRelationId(self: State) u64 {
        return self.logical_relation_id orelse self.table_id;
    }
    pub fn nativeJsonSkipField(self: State, comptime name: []const u8) bool {
        return std.mem.eql(u8, name, "logical_relation_id") and self.logical_relation_id == null;
    }
    pub fn jsonStringify(self: State, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(State).@"struct".field_names) |field| if (!self.nativeJsonSkipField(field)) {
            try writer.objectField(field);
            try writer.write(@field(self, field));
        };
        try writer.endObject();
    }
    pub fn validate(self: State) !void {
        if (self.format != 1 or std.mem.allEqual(u8, &self.metadata_incarnation, 0) or self.table_id == 0 or self.generation == 0 or self.next_id == 0 or self.entries.len > max_entries) return error.InvalidSqlTriggerDefinition;
        if (self.logical_relation_id) |id| if (id == 0 or id == self.table_id) return error.InvalidSqlTriggerDefinition;
        for (self.entries, 0..) |entry, i| {
            try entry.validate();
            if (entry.id >= self.next_id or (i != 0 and std.mem.order(u8, self.entries[i - 1].name, entry.name) != .lt)) return error.InvalidSqlTriggerDefinition;
            for (self.entries[0..i]) |previous| if (previous.id == entry.id) return error.InvalidSqlTriggerDefinition;
        }
    }
    pub fn find(self: State, name: []const u8) ?Entry {
        var low: usize = 0;
        var high: usize = self.entries.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, name, self.entries[mid].name)) {
                .eq => return self.entries[mid],
                .lt => high = mid,
                .gt => low = mid + 1,
            }
        }
        return null;
    }
    pub fn encode(self: State, a: A) ![]u8 {
        try self.validate();
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(self, .{}, &count.writer);
        if (count.fullCount() > max_bytes) return error.SqlProgramLimitExceeded;
        return std.json.Stringify.valueAlloc(a, self, .{});
    }
    pub fn decode(a: A, bytes: []const u8) !std.json.Parsed(State) {
        if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidSqlTriggerDefinition;
        var parsed = std.json.parseFromSlice(State, a, bytes, .{ .allocate = .alloc_always }) catch |failure| return if (failure == error.OutOfMemory) failure else error.InvalidSqlTriggerDefinition;
        errdefer parsed.deinit();
        try parsed.value.validate();
        const encoded = try parsed.value.encode(a);
        defer a.free(encoded);
        if (!std.mem.eql(u8, encoded, bytes)) return error.InvalidSqlTriggerDefinition;
        return parsed;
    }
};

/// Mutations borrow declarations until canonical encoding; the caller's native
/// catalog transaction owns the immutable before/after state and reverse edges.
pub fn prepare(a: A, before: State, replacement: Declaration, replace: bool) !State {
    try before.validate();
    try replacement.bind(1, 1).validate();
    const previous = before.find(replacement.name);
    if (previous != null and !replace) return error.CatalogAlreadyExists;
    const count = before.entries.len + @as(usize, @intFromBool(previous == null));
    if (count > max_entries) return error.SqlProgramLimitExceeded;
    const entries = try a.alloc(Entry, count);
    errdefer a.free(entries);
    @memcpy(entries[0..before.entries.len], before.entries);
    var entry = replacement.bind(1, 1);
    if (previous) |old| {
        entry.id = old.id;
        entry.generation = std.math.add(u32, old.generation, 1) catch return error.CatalogGenerationExhausted;
        for (entries) |*value| if (value.id == old.id) {
            value.* = entry;
            break;
        };
    } else {
        if (before.next_id == std.math.maxInt(u64)) return error.CatalogIdExhausted;
        entry.id = before.next_id;
        entry.generation = 1;
        entries[count - 1] = entry;
    }
    std.mem.sort(Entry, entries, {}, struct {
        fn less(_: void, left: Entry, right: Entry) bool {
            return syntax.nameLess({}, left.name, right.name);
        }
    }.less);
    const result: State = .{ .logical_relation_id = before.logical_relation_id, .metadata_incarnation = before.metadata_incarnation, .table_id = before.table_id, .generation = std.math.add(u64, before.generation, 1) catch return error.CatalogGenerationExhausted, .next_id = before.next_id + @as(u64, @intFromBool(previous == null)), .entries = entries };
    try result.validate();
    return result;
}

pub fn drop(a: A, before: State, name: []const u8, if_exists: bool) !State {
    try before.validate();
    if (before.find(name) == null) return if (if_exists) before else error.CatalogNotFound;
    const generation = std.math.add(u64, before.generation, 1) catch return error.CatalogGenerationExhausted;
    const entries = try a.alloc(Entry, before.entries.len - 1);
    var position: usize = 0;
    for (before.entries) |entry| if (!std.mem.eql(u8, entry.name, name)) {
        entries[position] = entry;
        position += 1;
    };
    return .{ .logical_relation_id = before.logical_relation_id, .metadata_incarnation = before.metadata_incarnation, .table_id = before.table_id, .generation = generation, .next_id = before.next_id, .entries = entries };
}

test "SQL trigger logical identity survives attachment mutations and remains signed data" {
    const a = std.testing.allocator;
    const before: State = .{ .metadata_incarnation = @splat(1), .table_id = 11, .logical_relation_id = 7, .generation = 1 };
    const reference: routines.RoutineReference = .{ .database = "antfly", .namespace = "public", .name = "touch", .namespace_id = 2, .id = 9 };
    const added = try prepare(a, before, .{ .name = "touch", .timing = .before, .events = .{ .update = true }, .level = .row, .function = reference }, false);
    defer a.free(added.entries);
    try std.testing.expectEqual(@as(u64, 7), added.effectiveLogicalRelationId());
    const dropped = try drop(a, added, "touch", false);
    defer a.free(dropped.entries);
    try std.testing.expectEqual(@as(u64, 7), dropped.effectiveLogicalRelationId());
    const bytes = try before.encode(a);
    defer a.free(bytes);
    var parsed = try State.decode(a, bytes);
    defer parsed.deinit();
    try std.testing.expectEqualDeep(before, parsed.value);
    var ordinary = before;
    ordinary.logical_relation_id = null;
    const legacy = try ordinary.encode(a);
    defer a.free(legacy);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "logical_relation_id") == null);
    var forged = before;
    forged.logical_relation_id = 11;
    try std.testing.expectError(error.InvalidSqlTriggerDefinition, forged.validate());
}
