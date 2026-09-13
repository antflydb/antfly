// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const ids = @import("id.zig");

pub const Kind = enum { message_enqueued, state_change, client_response, injected_error, fault_started, fault_stopped, domain };

/// Diagnostic-only field attached to an event. Names and values are retained
/// by the flight recorder but deliberately excluded from canonical traces.
pub const Field = struct {
    name: []const u8,
    value: []const u8,
};

pub const Event = struct {
    id: ids.StableId,
    name: []const u8,
    kind: Kind,
    actor_id: ?ids.StableId = null,
    resource_id: ?ids.StableId = null,
    payload_digest: u64 = 0,
    /// Diagnostic-only structured text retained by the bounded flight
    /// recorder. Canonical traces continue to use `payload_digest` as replay
    /// truth, so adding detail cannot alter a history.
    details: []const u8 = "",
    fields: []const Field = &.{},

    pub fn named(kind: Kind, name: []const u8, payload_digest: u64) Event {
        return .{ .id = ids.stable("event", name), .name = name, .kind = kind, .payload_digest = payload_digest };
    }
};

pub const Sink = struct {
    events: std.ArrayListUnmanaged(Event) = .empty,

    pub fn deinit(self: *Sink, allocator: std.mem.Allocator) void {
        for (self.events.items) |value| {
            allocator.free(value.name);
            allocator.free(value.details);
            for (value.fields) |field| {
                allocator.free(field.name);
                allocator.free(field.value);
            }
            allocator.free(value.fields);
        }
        self.events.deinit(allocator);
        self.* = .{};
    }

    pub fn emit(self: *Sink, allocator: std.mem.Allocator, value: Event) !void {
        if (value.id == 0) return error.InvalidEventId;
        if (value.name.len == 0) return error.EmptyEventName;
        const name = try allocator.dupe(u8, value.name);
        errdefer allocator.free(name);
        const details = try allocator.dupe(u8, value.details);
        errdefer allocator.free(details);
        const fields = try allocator.alloc(Field, value.fields.len);
        errdefer allocator.free(fields);
        var fields_initialized: usize = 0;
        errdefer for (fields[0..fields_initialized]) |field| {
            allocator.free(field.name);
            allocator.free(field.value);
        };
        for (value.fields, 0..) |field, index| {
            if (field.name.len == 0) return error.EmptyEventFieldName;
            const field_name = try allocator.dupe(u8, field.name);
            errdefer allocator.free(field_name);
            const field_value = try allocator.dupe(u8, field.value);
            fields[index] = .{ .name = field_name, .value = field_value };
            fields_initialized += 1;
        }
        var owned = value;
        owned.name = name;
        owned.details = details;
        owned.fields = fields;
        try self.events.append(allocator, owned);
    }

    pub fn emitNamed(self: *Sink, allocator: std.mem.Allocator, kind: Kind, name: []const u8, payload_digest: u64) !void {
        try self.emit(allocator, Event.named(kind, name, payload_digest));
    }

    pub fn emitDetailed(self: *Sink, allocator: std.mem.Allocator, value: Event, details: []const u8) !void {
        var detailed = value;
        detailed.details = details;
        try self.emit(allocator, detailed);
    }
};

test "event IDs are stable and namespaced" {
    const value = Event.named(.state_change, "toy.changed", 4);
    try std.testing.expectEqual(ids.stable("event", "toy.changed"), value.id);
    try std.testing.expect(value.id != ids.stable("transition", "toy.changed"));
}

test "event sink owns emitted names until deinit" {
    var sink = Sink{};
    defer sink.deinit(std.testing.allocator);
    var name = [_]u8{ 'n', 'o', 'd', 'e' };
    try sink.emitNamed(std.testing.allocator, .fault_stopped, &name, 0);
    @memset(&name, 'x');
    try std.testing.expectEqualStrings("node", sink.events.items[0].name);
}

test "event sink owns diagnostic details independently of replay fields" {
    var sink = Sink{};
    defer sink.deinit(std.testing.allocator);
    var details = [_]u8{ 'p', 'h', 'a', 's', 'e' };
    var field_name = [_]u8{ 'p', 'h', 'a', 's', 'e' };
    var field_value = [_]u8{ 'a', 'p', 'p', 'l', 'y' };
    var value = Event.named(.domain, "operation", 9);
    value.fields = &.{.{ .name = &field_name, .value = &field_value }};
    try sink.emitDetailed(std.testing.allocator, value, &details);
    @memset(&details, 'x');
    @memset(&field_name, 'x');
    @memset(&field_value, 'x');
    try std.testing.expectEqualStrings("phase", sink.events.items[0].details);
    try std.testing.expectEqualStrings("phase", sink.events.items[0].fields[0].name);
    try std.testing.expectEqualStrings("apply", sink.events.items[0].fields[0].value);
    try std.testing.expectEqual(@as(u64, 9), sink.events.items[0].payload_digest);
}
