// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Bounded retroactive structured logging for VOPR histories.
//!
//! Every history may keep this ring in memory. Verbose details become an owned
//! artifact only for a failure, novelty, or an explicit debugger request.
//! Filtering and window selection are diagnostic operations and never enter
//! canonical replay bytes.

const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");

pub const format = "vopr-flight-recorder-v2";

pub const Trigger = enum { failure, novelty, debugger };

pub const Record = struct {
    index: u64,
    ordinal: u64,
    id: ids.StableId,
    name: []const u8,
    kind: event.Kind,
    actor_id: ?ids.StableId = null,
    resource_id: ?ids.StableId = null,
    payload_digest: u64 = 0,
    details: []const u8 = "",
    fields: []const event.Field = &.{},
};

pub const FieldFilter = struct {
    name: []const u8,
    value: ?[]const u8 = null,
    value_contains: ?[]const u8 = null,

    fn matches(self: FieldFilter, fields: []const event.Field) bool {
        for (fields) |field| {
            if (!std.mem.eql(u8, field.name, self.name)) continue;
            if (self.value) |expected| if (!std.mem.eql(u8, field.value, expected)) continue;
            if (self.value_contains) |needle| if (std.mem.indexOf(u8, field.value, needle) == null) continue;
            return true;
        }
        return false;
    }
};

/// All configured predicates are conjunctive. An empty filter selects every
/// retained record. `text_contains` searches the event name, detail text, and
/// every structured field name/value.
pub const Filter = struct {
    id: ?ids.StableId = null,
    name: ?[]const u8 = null,
    kind: ?event.Kind = null,
    actor_id: ?ids.StableId = null,
    resource_id: ?ids.StableId = null,
    payload_digest: ?u64 = null,
    first_index: u64 = 0,
    last_index: u64 = std.math.maxInt(u64),
    text_contains: ?[]const u8 = null,
    details_contains: ?[]const u8 = null,
    fields: []const FieldFilter = &.{},

    pub fn validate(self: Filter) !void {
        if (self.first_index > self.last_index) return error.InvalidFlightRecorderFilterWindow;
        for (self.fields) |field| {
            if (field.name.len == 0) return error.EmptyFlightRecorderFieldFilterName;
            if (field.value != null and field.value_contains != null) return error.AmbiguousFlightRecorderFieldFilter;
        }
    }

    pub fn matches(self: Filter, record: Record) bool {
        if (record.index < self.first_index or record.index > self.last_index) return false;
        if (self.id) |value| if (record.id != value) return false;
        if (self.name) |value| if (!std.mem.eql(u8, record.name, value)) return false;
        if (self.kind) |value| if (record.kind != value) return false;
        if (self.actor_id) |value| if (record.actor_id != value) return false;
        if (self.resource_id) |value| if (record.resource_id != value) return false;
        if (self.payload_digest) |value| if (record.payload_digest != value) return false;
        if (self.details_contains) |needle| if (std.mem.indexOf(u8, record.details, needle) == null) return false;
        if (self.text_contains) |needle| {
            var found = std.mem.indexOf(u8, record.name, needle) != null or
                std.mem.indexOf(u8, record.details, needle) != null;
            if (!found) for (record.fields) |field| {
                if (std.mem.indexOf(u8, field.name, needle) != null or
                    std.mem.indexOf(u8, field.value, needle) != null)
                {
                    found = true;
                    break;
                }
            };
            if (!found) return false;
        }
        for (self.fields) |field| if (!field.matches(record.fields)) return false;
        return true;
    }
};

pub const Window = struct {
    filter: Filter = .{},
    before_records: usize = 0,
    after_records: usize = 0,
    max_records: usize = std.math.maxInt(usize),

    pub fn validate(self: Window) !void {
        try self.filter.validate();
        if (self.max_records == 0) return error.InvalidFlightRecorderWindowLimit;
    }
};

const Slot = struct {
    occupied: bool = false,
    record: Record = undefined,
};

pub const Snapshot = struct {
    allocator: std.mem.Allocator,
    trigger: Trigger,
    dropped: u64,
    matched: u64,
    filtered_out: u64,
    records: []Record,

    pub fn deinit(self: *Snapshot) void {
        for (self.records) |record| freeRecord(self.allocator, record);
        self.allocator.free(self.records);
        self.* = undefined;
    }

    pub fn renderJsonAlloc(self: *const Snapshot, allocator: std.mem.Allocator) ![]u8 {
        return std.json.Stringify.valueAlloc(allocator, .{
            .format = format,
            .trigger = self.trigger,
            .dropped = self.dropped,
            .matched = self.matched,
            .filtered_out = self.filtered_out,
            .records = self.records,
        }, .{ .whitespace = .indent_2 });
    }
};

pub const Recorder = struct {
    allocator: std.mem.Allocator,
    slots: []Slot,
    start: usize = 0,
    len: usize = 0,
    dropped: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Recorder {
        if (capacity == 0) return error.InvalidFlightRecorderCapacity;
        const slots = try allocator.alloc(Slot, capacity);
        @memset(slots, .{});
        return .{ .allocator = allocator, .slots = slots };
    }

    pub fn deinit(self: *Recorder) void {
        for (self.slots) |*slot| self.clearSlot(slot);
        self.allocator.free(self.slots);
        self.* = undefined;
    }

    pub fn record(self: *Recorder, value: Record) !void {
        if (value.id == 0 or value.name.len == 0) return error.InvalidFlightRecord;
        const owned = try cloneRecord(self.allocator, value);
        errdefer freeRecord(self.allocator, owned);

        const position = if (self.len < self.slots.len)
            (self.start + self.len) % self.slots.len
        else blk: {
            const oldest = self.start;
            self.clearSlot(&self.slots[oldest]);
            self.start = (self.start + 1) % self.slots.len;
            self.dropped +|= 1;
            break :blk oldest;
        };
        self.slots[position] = .{ .occupied = true, .record = owned };
        if (self.len < self.slots.len) self.len += 1;
    }

    pub fn recordEvent(self: *Recorder, index: u64, ordinal: u64, value: event.Event) !void {
        try self.record(.{
            .index = index,
            .ordinal = ordinal,
            .id = value.id,
            .name = value.name,
            .kind = value.kind,
            .actor_id = value.actor_id,
            .resource_id = value.resource_id,
            .payload_digest = value.payload_digest,
            .details = value.details,
            .fields = value.fields,
        });
    }

    pub fn materialize(self: *const Recorder, allocator: std.mem.Allocator, trigger: Trigger) !Snapshot {
        return self.materializeWindow(allocator, trigger, .{});
    }

    pub fn materializeWindow(
        self: *const Recorder,
        allocator: std.mem.Allocator,
        trigger: Trigger,
        window: Window,
    ) !Snapshot {
        try window.validate();
        const selected = try allocator.alloc(bool, self.len);
        defer allocator.free(selected);
        @memset(selected, false);
        var matched: u64 = 0;
        for (0..self.len) |offset| {
            const record_value = self.slots[(self.start + offset) % self.slots.len].record;
            if (!window.filter.matches(record_value)) continue;
            matched +|= 1;
            const first = offset -| window.before_records;
            const last = @min(self.len - 1, offset +| window.after_records);
            for (first..last + 1) |position| selected[position] = true;
        }
        var selected_count: usize = 0;
        for (selected) |is_selected| selected_count += @intFromBool(is_selected);
        var skip: usize = selected_count -| window.max_records;
        const output_count = selected_count - skip;
        const records = try allocator.alloc(Record, output_count);
        errdefer allocator.free(records);
        var initialized: usize = 0;
        errdefer for (records[0..initialized]) |record_value| freeRecord(allocator, record_value);
        for (selected, 0..) |is_selected, offset| {
            if (!is_selected) continue;
            if (skip > 0) {
                skip -= 1;
                continue;
            }
            const slot = self.slots[(self.start + offset) % self.slots.len];
            std.debug.assert(slot.occupied);
            records[initialized] = try cloneRecord(allocator, slot.record);
            initialized += 1;
        }
        return .{
            .allocator = allocator,
            .trigger = trigger,
            .dropped = self.dropped,
            .matched = matched,
            .filtered_out = @intCast(self.len - output_count),
            .records = records,
        };
    }

    fn clearSlot(self: *Recorder, slot: *Slot) void {
        if (!slot.occupied) return;
        freeRecord(self.allocator, slot.record);
        slot.* = .{};
    }
};

fn cloneRecord(allocator: std.mem.Allocator, value: Record) !Record {
    const name = try allocator.dupe(u8, value.name);
    errdefer allocator.free(name);
    const details = try allocator.dupe(u8, value.details);
    errdefer allocator.free(details);
    const fields = try allocator.alloc(event.Field, value.fields.len);
    errdefer allocator.free(fields);
    var initialized: usize = 0;
    errdefer for (fields[0..initialized]) |field| {
        allocator.free(field.name);
        allocator.free(field.value);
    };
    for (value.fields, 0..) |field, index| {
        if (field.name.len == 0) return error.EmptyFlightRecordFieldName;
        const field_name = try allocator.dupe(u8, field.name);
        errdefer allocator.free(field_name);
        const field_value = try allocator.dupe(u8, field.value);
        fields[index] = .{ .name = field_name, .value = field_value };
        initialized += 1;
    }
    var result = value;
    result.name = name;
    result.details = details;
    result.fields = fields;
    return result;
}

fn freeRecord(allocator: std.mem.Allocator, value: Record) void {
    allocator.free(value.name);
    allocator.free(value.details);
    for (value.fields) |field| {
        allocator.free(field.name);
        allocator.free(field.value);
    }
    allocator.free(value.fields);
}

test "flight recorder retains a bounded retroactive window" {
    var recorder = try Recorder.init(std.testing.allocator, 3);
    defer recorder.deinit();
    try recorder.record(.{ .index = 1, .ordinal = 0, .id = 1, .name = "one", .kind = .domain, .details = "verbose-one" });
    try recorder.record(.{ .index = 2, .ordinal = 0, .id = 2, .name = "two", .kind = .state_change, .details = "verbose-two" });
    try recorder.record(.{
        .index = 3,
        .ordinal = 0,
        .id = 3,
        .name = "three",
        .kind = .injected_error,
        .details = "verbose-three",
        .fields = &.{ .{ .name = "node", .value = "n2" }, .{ .name = "phase", .value = "commit" } },
    });
    try recorder.record(.{ .index = 4, .ordinal = 0, .id = 4, .name = "four", .kind = .domain, .details = "tail" });
    var snapshot = try recorder.materializeWindow(std.testing.allocator, .failure, .{
        .filter = .{ .text_contains = "commit", .fields = &.{.{ .name = "node", .value = "n2" }} },
        .before_records = 1,
        .after_records = 1,
    });
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(u64, 1), snapshot.dropped);
    try std.testing.expectEqual(@as(u64, 1), snapshot.matched);
    try std.testing.expectEqual(@as(usize, 3), snapshot.records.len);
    try std.testing.expectEqualStrings("two", snapshot.records[0].name);
    try std.testing.expectEqualStrings("verbose-three", snapshot.records[1].details);
    try std.testing.expectEqualStrings("n2", snapshot.records[1].fields[0].value);
    try std.testing.expectEqualStrings("four", snapshot.records[2].name);
}

test "flight recorder limits filtered output to the newest selected records" {
    var recorder = try Recorder.init(std.testing.allocator, 4);
    defer recorder.deinit();
    for (1..5) |index| try recorder.record(.{
        .index = index,
        .ordinal = 0,
        .id = index,
        .name = "event",
        .kind = .domain,
    });
    var snapshot = try recorder.materializeWindow(std.testing.allocator, .debugger, .{ .max_records = 2 });
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 2), snapshot.records.len);
    try std.testing.expectEqual(@as(u64, 3), snapshot.records[0].index);
    try std.testing.expectEqual(@as(u64, 4), snapshot.records[1].index);
}
