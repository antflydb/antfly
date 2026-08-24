// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Bounded retroactive structured logging for VOPR histories.
//!
//! Every history may keep this ring in memory. Verbose details become an owned
//! artifact only when `materialize` is called for a failure, novelty, or an
//! explicit debugger request.

const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");

pub const format = "vopr-flight-recorder-v1";

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
};

const Slot = struct {
    occupied: bool = false,
    record: Record = undefined,
};

pub const Snapshot = struct {
    allocator: std.mem.Allocator,
    trigger: Trigger,
    dropped: u64,
    records: []Record,

    pub fn deinit(self: *Snapshot) void {
        for (self.records) |record| {
            self.allocator.free(record.name);
            self.allocator.free(record.details);
        }
        self.allocator.free(self.records);
        self.* = undefined;
    }

    pub fn renderJsonAlloc(self: *const Snapshot, allocator: std.mem.Allocator) ![]u8 {
        return std.json.Stringify.valueAlloc(allocator, .{
            .format = format,
            .trigger = self.trigger,
            .dropped = self.dropped,
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
        const name = try self.allocator.dupe(u8, value.name);
        errdefer self.allocator.free(name);
        const details = try self.allocator.dupe(u8, value.details);
        errdefer self.allocator.free(details);

        const position = if (self.len < self.slots.len)
            (self.start + self.len) % self.slots.len
        else blk: {
            const oldest = self.start;
            self.clearSlot(&self.slots[oldest]);
            self.start = (self.start + 1) % self.slots.len;
            self.dropped +|= 1;
            break :blk oldest;
        };
        self.slots[position] = .{ .occupied = true, .record = .{
            .index = value.index,
            .ordinal = value.ordinal,
            .id = value.id,
            .name = name,
            .kind = value.kind,
            .actor_id = value.actor_id,
            .resource_id = value.resource_id,
            .payload_digest = value.payload_digest,
            .details = details,
        } };
        if (self.len < self.slots.len) self.len += 1;
    }

    pub fn recordEvent(self: *Recorder, index: u64, ordinal: u64, value: event.Event, details: []const u8) !void {
        try self.record(.{
            .index = index,
            .ordinal = ordinal,
            .id = value.id,
            .name = value.name,
            .kind = value.kind,
            .actor_id = value.actor_id,
            .resource_id = value.resource_id,
            .payload_digest = value.payload_digest,
            .details = details,
        });
    }

    pub fn materialize(self: *const Recorder, allocator: std.mem.Allocator, trigger: Trigger) !Snapshot {
        const records = try allocator.alloc(Record, self.len);
        errdefer allocator.free(records);
        var initialized: usize = 0;
        errdefer for (records[0..initialized]) |item| {
            allocator.free(item.name);
            allocator.free(item.details);
        };
        for (0..self.len) |offset| {
            const slot = self.slots[(self.start + offset) % self.slots.len];
            std.debug.assert(slot.occupied);
            records[offset] = slot.record;
            records[offset].name = try allocator.dupe(u8, slot.record.name);
            errdefer allocator.free(records[offset].name);
            records[offset].details = try allocator.dupe(u8, slot.record.details);
            initialized += 1;
        }
        return .{ .allocator = allocator, .trigger = trigger, .dropped = self.dropped, .records = records };
    }

    fn clearSlot(self: *Recorder, slot: *Slot) void {
        if (!slot.occupied) return;
        self.allocator.free(slot.record.name);
        self.allocator.free(slot.record.details);
        slot.* = .{};
    }
};

test "flight recorder retains a bounded retroactive window" {
    var recorder = try Recorder.init(std.testing.allocator, 2);
    defer recorder.deinit();
    try recorder.record(.{ .index = 1, .ordinal = 0, .id = 1, .name = "one", .kind = .domain, .details = "verbose-one" });
    try recorder.record(.{ .index = 2, .ordinal = 0, .id = 2, .name = "two", .kind = .state_change, .details = "verbose-two" });
    try recorder.record(.{ .index = 3, .ordinal = 0, .id = 3, .name = "three", .kind = .injected_error, .details = "verbose-three" });
    var snapshot = try recorder.materialize(std.testing.allocator, .failure);
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(u64, 1), snapshot.dropped);
    try std.testing.expectEqual(@as(usize, 2), snapshot.records.len);
    try std.testing.expectEqualStrings("two", snapshot.records[0].name);
    try std.testing.expectEqualStrings("verbose-three", snapshot.records[1].details);
}
